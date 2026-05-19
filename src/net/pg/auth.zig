//! Server-side SCRAM-SHA-256 (RFC 5802 / RFC 7677) for the Postgres
//! wire frontend.
//!
//! Five-message SASL exchange:
//!   client  → SASLInitialResponse:  client-first-message
//!                                   (n,,n=user,r=<client-nonce>)
//!   server  → AuthenticationSASLContinue: server-first-message
//!                                   (r=<combined-nonce>,s=<salt>,i=<iter>)
//!   client  → SASLResponse:         client-final-message
//!                                   (c=biws,r=<combined-nonce>,p=<proof>)
//!   server  → AuthenticationSASLFinal: server-final-message
//!                                   (v=<server-signature>)
//!   server  → AuthenticationOk
//!
//! Server-stored credentials (computed once at startup from password):
//!   SaltedPassword = PBKDF2-HMAC-SHA-256(password, salt, iter, 32)
//!   ClientKey      = HMAC-SHA-256(SaltedPassword, "Client Key")
//!   StoredKey      = SHA-256(ClientKey)
//!   ServerKey      = HMAC-SHA-256(SaltedPassword, "Server Key")
//!
//! On the wire we only need StoredKey + ServerKey + salt + iter — never
//! the cleartext password.
//!
//! Verification at step 4:
//!   AuthMessage     = client-first-bare + "," + server-first-message +
//!                     "," + client-final-without-proof
//!   ClientSignature = HMAC-SHA-256(StoredKey, AuthMessage)
//!   ClientKey'      = ClientProof XOR ClientSignature
//!   verified        = SHA-256(ClientKey') == StoredKey
//! On success the server replies with v = HMAC-SHA-256(ServerKey, AuthMessage).

const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;

pub const HASH_LEN: usize = 32;
pub const SALT_LEN: usize = 16;
pub const DEFAULT_ITER_COUNT: u32 = 4096;

pub const Error = error{
    MalformedSaslMessage,
    UnsupportedChannelBinding,
    NonceMismatch,
    ProofMismatch,
    InvalidBase64,
};

/// What the server retains for a given password. Re-derived once at
/// startup from the cleartext password + a stable salt; never derived
/// again per-connection.
pub const Credentials = struct {
    salt: [SALT_LEN]u8,
    iter_count: u32,
    stored_key: [HASH_LEN]u8,
    server_key: [HASH_LEN]u8,
};

/// PBKDF2-HMAC-SHA-256, output length fixed at 32 bytes (matches the
/// SCRAM-SHA-256 SaltedPassword spec).
pub fn pbkdf2HmacSha256(password: []const u8, salt: []const u8, iter: u32, out: *[HASH_LEN]u8) void {
    // U_1 = HMAC(P, salt || INT(1))
    var int_be: [4]u8 = .{ 0, 0, 0, 1 };
    var ctx = HmacSha256.init(password);
    ctx.update(salt);
    ctx.update(&int_be);
    var u: [HASH_LEN]u8 = undefined;
    ctx.final(&u);
    @memcpy(out, &u);

    var i: u32 = 1;
    while (i < iter) : (i += 1) {
        var inner = HmacSha256.init(password);
        inner.update(&u);
        inner.final(&u);
        for (0..HASH_LEN) |k| out[k] ^= u[k];
    }
}

/// Deterministically derive a salt from the password so a returning
/// client gets the same StoredKey. SCRAM doesn't require this — the
/// salt can be random and persisted alongside the credential — but
/// thinDB stores only the cleartext password in CLI flags, so a
/// password-derived salt keeps the credential reproducible without
/// extra state.
pub fn deriveSalt(password: []const u8, out: *[SALT_LEN]u8) void {
    var digest: [Sha256.digest_length]u8 = undefined;
    var ctx = Sha256.init(.{});
    ctx.update("thindb-scram-salt:");
    ctx.update(password);
    ctx.final(&digest);
    @memcpy(out, digest[0..SALT_LEN]);
}

/// Build the (salt, StoredKey, ServerKey) tuple for a cleartext
/// password. Called once at server startup.
pub fn deriveCredentials(password: []const u8) Credentials {
    var creds: Credentials = .{
        .salt = undefined,
        .iter_count = DEFAULT_ITER_COUNT,
        .stored_key = undefined,
        .server_key = undefined,
    };
    deriveSalt(password, &creds.salt);

    var salted: [HASH_LEN]u8 = undefined;
    pbkdf2HmacSha256(password, &creds.salt, creds.iter_count, &salted);

    var client_key: [HASH_LEN]u8 = undefined;
    HmacSha256.create(&client_key, "Client Key", &salted);

    Sha256.hash(&client_key, &creds.stored_key, .{});

    HmacSha256.create(&creds.server_key, "Server Key", &salted);
    return creds;
}

/// Verify the client's proof. AuthMessage is constructed by the caller
/// from the three SCRAM messages exchanged so far.
pub fn verifyClientProof(
    creds: Credentials,
    auth_message: []const u8,
    client_proof: [HASH_LEN]u8,
) bool {
    var client_signature: [HASH_LEN]u8 = undefined;
    HmacSha256.create(&client_signature, auth_message, &creds.stored_key);

    var client_key: [HASH_LEN]u8 = undefined;
    for (0..HASH_LEN) |i| client_key[i] = client_proof[i] ^ client_signature[i];

    var derived: [HASH_LEN]u8 = undefined;
    Sha256.hash(&client_key, &derived, .{});
    return std.crypto.timing_safe.eql([HASH_LEN]u8, derived, creds.stored_key);
}

/// HMAC-SHA-256(ServerKey, AuthMessage) — emitted to the client as
/// the SCRAM server-final-message's `v` attribute.
pub fn serverSignature(creds: Credentials, auth_message: []const u8) [HASH_LEN]u8 {
    var sig: [HASH_LEN]u8 = undefined;
    HmacSha256.create(&sig, auth_message, &creds.server_key);
    return sig;
}

/// Find the value of attribute `key=` in a comma-separated SCRAM
/// attribute list. Returns the slice up to the next comma or end.
pub fn findAttr(message: []const u8, key: []const u8) ?[]const u8 {
    var rest = message;
    while (rest.len > 0) {
        const end = std.mem.indexOfScalar(u8, rest, ',') orelse rest.len;
        const attr = rest[0..end];
        if (attr.len > key.len and std.mem.eql(u8, attr[0..key.len], key) and attr[key.len] == '=') {
            return attr[key.len + 1 ..];
        }
        if (end == rest.len) break;
        rest = rest[end + 1 ..];
    }
    return null;
}

/// Strip the GS2 header (`n,,` / `y,,` / `p=...,...,`) from the
/// client-first-message and return the "bare" body used in
/// AuthMessage. Channel binding (`y,,` or `p=...`) is rejected — we
/// don't advertise SCRAM-SHA-256-PLUS.
pub fn parseClientFirst(allocator: Allocator, message: []const u8) !struct {
    bare: []const u8,
    client_nonce: []const u8,
} {
    _ = allocator;
    if (message.len < 3) return Error.MalformedSaslMessage;
    // Channel-binding flag: 'n' (no support), 'y' (client supports
    // but server doesn't advertise), 'p' (in use — rejected).
    if (message[0] == 'p') return Error.UnsupportedChannelBinding;
    if (message[0] != 'n' and message[0] != 'y') return Error.MalformedSaslMessage;
    if (message[1] != ',') return Error.MalformedSaslMessage;
    // Optional authzid between the two commas; ignore.
    var i: usize = 2;
    while (i < message.len and message[i] != ',') i += 1;
    if (i >= message.len) return Error.MalformedSaslMessage;
    const bare = message[i + 1 ..];
    const nonce = findAttr(bare, "r") orelse return Error.MalformedSaslMessage;
    return .{ .bare = bare, .client_nonce = nonce };
}

/// Pull the combined-nonce + client-proof + channel-binding from the
/// client-final-message. The `without_proof` slice is what gets fed
/// into AuthMessage.
pub fn parseClientFinal(message: []const u8) !struct {
    without_proof: []const u8,
    combined_nonce: []const u8,
    client_proof_b64: []const u8,
} {
    const combined_nonce = findAttr(message, "r") orelse return Error.MalformedSaslMessage;
    const proof = findAttr(message, "p") orelse return Error.MalformedSaslMessage;
    // without-proof is the message up to ",p="
    const proof_start = std.mem.lastIndexOf(u8, message, ",p=") orelse return Error.MalformedSaslMessage;
    return .{
        .without_proof = message[0..proof_start],
        .combined_nonce = combined_nonce,
        .client_proof_b64 = proof,
    };
}

/// Same predictable-counter-mix used by the MySQL wire's salt: we
/// only need each connection's server-nonce to be unpredictable to a
/// passive observer, not cryptographically random. 18 raw bytes ->
/// 24 base64 chars per RFC convention.
var server_nonce_counter: std.atomic.Value(u64) = .{ .raw = 0 };
var server_nonce_anchor: std.atomic.Value(u64) = .{ .raw = 0 };

pub fn randomServerNonce(out: *[18]u8) void {
    var anchor = server_nonce_anchor.load(.monotonic);
    if (anchor == 0) {
        anchor = @as(u64, @truncate(@intFromPtr(out))) ^ 0x5A5A_A5A5_C0FF_EE17;
        if (anchor == 0) anchor = 1;
        server_nonce_anchor.store(anchor, .monotonic);
    }
    const ctr = server_nonce_counter.fetchAdd(1, .monotonic);
    var seed: [24]u8 = undefined;
    std.mem.writeInt(u64, seed[0..8], anchor, .little);
    std.mem.writeInt(u64, seed[8..16], ctr, .little);
    const sp: usize = @intFromPtr(out);
    std.mem.writeInt(u64, seed[16..24], @as(u64, @truncate(sp)), .little);
    var digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(&seed, &digest, .{});
    @memcpy(out, digest[0..18]);
}

test "deriveCredentials + verifyClientProof round-trip" {
    const password = "hunter2";
    const creds = deriveCredentials(password);

    // Simulate the client side: re-derive SaltedPassword from the
    // same salt/iter, compute ClientProof, hand it back.
    var salted: [HASH_LEN]u8 = undefined;
    pbkdf2HmacSha256(password, &creds.salt, creds.iter_count, &salted);

    var client_key: [HASH_LEN]u8 = undefined;
    HmacSha256.create(&client_key, "Client Key", &salted);
    var stored_key: [HASH_LEN]u8 = undefined;
    Sha256.hash(&client_key, &stored_key, .{});

    const auth_message = "n=user,r=cnoncesnonce,s=salt,i=4096,c=biws,r=cnoncesnonce";
    var client_signature: [HASH_LEN]u8 = undefined;
    HmacSha256.create(&client_signature, auth_message, &stored_key);
    var proof: [HASH_LEN]u8 = undefined;
    for (0..HASH_LEN) |i| proof[i] = client_key[i] ^ client_signature[i];

    try std.testing.expect(verifyClientProof(creds, auth_message, proof));

    var wrong_proof = proof;
    wrong_proof[0] ^= 0xFF;
    try std.testing.expect(!verifyClientProof(creds, auth_message, wrong_proof));
}

test "findAttr finds keys in attribute list" {
    const m = "n=jeremy,r=abc123,p=xyz";
    try std.testing.expectEqualStrings("jeremy", findAttr(m, "n").?);
    try std.testing.expectEqualStrings("abc123", findAttr(m, "r").?);
    try std.testing.expectEqualStrings("xyz", findAttr(m, "p").?);
    try std.testing.expect(findAttr(m, "missing") == null);
}

test "parseClientFirst extracts bare body + nonce" {
    const r = try parseClientFirst(std.testing.allocator, "n,,n=jeremy,r=mynonce");
    try std.testing.expectEqualStrings("n=jeremy,r=mynonce", r.bare);
    try std.testing.expectEqualStrings("mynonce", r.client_nonce);
}

test "parseClientFirst rejects channel binding 'p'" {
    try std.testing.expectError(
        Error.UnsupportedChannelBinding,
        parseClientFirst(std.testing.allocator, "p=tls-server-end-point,,n=u,r=x"),
    );
}

test "parseClientFinal splits without-proof from proof" {
    const r = try parseClientFinal("c=biws,r=combined,p=base64proof");
    try std.testing.expectEqualStrings("c=biws,r=combined", r.without_proof);
    try std.testing.expectEqualStrings("combined", r.combined_nonce);
    try std.testing.expectEqualStrings("base64proof", r.client_proof_b64);
}
