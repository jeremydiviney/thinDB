//! Server-side `mysql_native_password` plugin implementation.
//!
//! The handshake protocol is:
//!   server → client: salt (20 random bytes) embedded in HandshakeV10's
//!                    auth-plugin-data fields
//!   client → server: 20-byte hash =
//!       SHA1(password) XOR SHA1(salt || SHA1(SHA1(password)))
//!
//! The double-SHA1 of the password is what's stored in mysql.user;
//! checking the response doesn't require the cleartext password on
//! disk (though thinDB currently stores cleartext in its server
//! config — when we add a real user table we'll store the double-SHA1).

const std = @import("std");
const Sha1 = std.crypto.hash.Sha1;
const random_seed = @import("../random_seed.zig");

pub const SALT_LEN: usize = 20;
pub const HASH_LEN: usize = 20;

var salt_state: random_seed.State = .{ .anchor_mix = 0xA5A5_5A5A_DEAD_BEEF };

/// Fill `out` with 20 unpredictable bytes in the printable-ASCII range
/// (0x21..0x7e). The salt is sent in cleartext in the HandshakeV10 packet,
/// so the only security requirement is unpredictability across connections.
///
/// The printable-ASCII constraint matches what real MySQL and StarRocks send,
/// and it is REQUIRED for compatibility: MySQL Connector/J (Java) mishandles
/// salt bytes > 0x7f (signed-byte / charset decoding), computing its scramble
/// against a corrupted salt and failing auth with "Access denied" — even
/// though the C client (libmysqlclient) and Node mysql2 handle raw bytes and
/// authenticate fine. Constraining to printable ASCII also keeps NUL out (the
/// salt is framed with a NUL terminator in the packet).
pub fn randomSalt(out: *[SALT_LEN]u8) void {
    random_seed.fill(&salt_state, out);
    for (out) |*b| b.* = 0x21 + (b.* % 94);
}

/// Compute the expected 20-byte hash for a given password + salt.
/// Server-side: matches what a correctly-configured client should
/// send.
pub fn nativeHash(password: []const u8, salt: [SALT_LEN]u8) [HASH_LEN]u8 {
    var stage1: [HASH_LEN]u8 = undefined;
    Sha1.hash(password, &stage1, .{});

    var stage2: [HASH_LEN]u8 = undefined;
    Sha1.hash(&stage1, &stage2, .{});

    var h = Sha1.init(.{});
    h.update(&salt);
    h.update(&stage2);
    var stage3: [HASH_LEN]u8 = undefined;
    h.final(&stage3);

    var out: [HASH_LEN]u8 = undefined;
    for (0..HASH_LEN) |i| out[i] = stage1[i] ^ stage3[i];
    return out;
}

/// Returns true iff the client's response matches the expected hash
/// for the given password + salt. Empty client_response with empty
/// password is treated as a valid no-password auth (matches mysql's
/// "user with empty password" semantics).
pub fn verify(password: []const u8, salt: [SALT_LEN]u8, client_response: []const u8) bool {
    if (password.len == 0 and client_response.len == 0) return true;
    if (client_response.len != HASH_LEN) return false;
    const expected = nativeHash(password, salt);
    return std.crypto.timing_safe.eql([HASH_LEN]u8, expected, client_response[0..HASH_LEN].*);
}

// -----------------------------------------------------------------------
// caching_sha2_password (MySQL 8.0+ default plugin)
// -----------------------------------------------------------------------
//
// Server stores `double_sha256 = SHA256(SHA256(password))`.
// Per-connection, the client sends a 32-byte response computed as:
//   mask     = SHA256(double_sha256 || salt)         // 32 bytes
//   response = SHA256(password)        XOR mask      // 32 bytes
//
// Server verifies by recovering SHA256(password):
//   recovered_sha_pw = response XOR mask
//   accept iff SHA256(recovered_sha_pw) == double_sha256
//
// On success the server sends a "fast_auth_success" indicator —
// AuthMoreData packet with body byte 0x03 — before the OK packet.
// The "full path" (cache miss → cleartext password over RSA-encrypted
// channel) is not implemented; we always treat a correct response as
// a fast-path success.

const Sha256 = std.crypto.hash.sha2.Sha256;
pub const SHA256_LEN: usize = 32;

pub const CachingSha2Credentials = struct {
    double_sha256: [SHA256_LEN]u8,
};

pub fn deriveCachingSha2Credentials(password: []const u8) CachingSha2Credentials {
    var stage1: [SHA256_LEN]u8 = undefined;
    Sha256.hash(password, &stage1, .{});
    var out: CachingSha2Credentials = .{ .double_sha256 = undefined };
    Sha256.hash(&stage1, &out.double_sha256, .{});
    return out;
}

/// Client-side hash computation — useful for tests that need to
/// simulate a real caching_sha2 client.
pub fn cachingSha2ClientHash(
    password: []const u8,
    salt: [SALT_LEN]u8,
) [SHA256_LEN]u8 {
    var stage1: [SHA256_LEN]u8 = undefined;
    Sha256.hash(password, &stage1, .{});
    var stage2: [SHA256_LEN]u8 = undefined;
    Sha256.hash(&stage1, &stage2, .{});

    var mask: [SHA256_LEN]u8 = undefined;
    var ctx = Sha256.init(.{});
    ctx.update(&stage2);
    ctx.update(&salt);
    ctx.final(&mask);

    var out: [SHA256_LEN]u8 = undefined;
    for (0..SHA256_LEN) |i| out[i] = stage1[i] ^ mask[i];
    return out;
}

pub fn verifyCachingSha2(
    creds: CachingSha2Credentials,
    salt: [SALT_LEN]u8,
    client_response: []const u8,
) bool {
    if (client_response.len != SHA256_LEN) return false;

    var mask: [SHA256_LEN]u8 = undefined;
    var ctx = Sha256.init(.{});
    ctx.update(&creds.double_sha256);
    ctx.update(&salt);
    ctx.final(&mask);

    var recovered: [SHA256_LEN]u8 = undefined;
    for (0..SHA256_LEN) |i| recovered[i] = client_response[i] ^ mask[i];

    var derived: [SHA256_LEN]u8 = undefined;
    Sha256.hash(&recovered, &derived, .{});
    return std.crypto.timing_safe.eql([SHA256_LEN]u8, derived, creds.double_sha256);
}

test "caching_sha2 round-trip: client hash verifies against derived credentials" {
    var salt: [SALT_LEN]u8 = undefined;
    @memcpy(&salt, "abcdefghijklmnopqrst");
    const password = "hunter2";

    const creds = deriveCachingSha2Credentials(password);
    const hash = cachingSha2ClientHash(password, salt);
    try std.testing.expect(verifyCachingSha2(creds, salt, &hash));

    // Wrong password fails.
    const wrong = cachingSha2ClientHash("wrong", salt);
    try std.testing.expect(!verifyCachingSha2(creds, salt, &wrong));

    // Wrong length fails.
    try std.testing.expect(!verifyCachingSha2(creds, salt, "short"));
}

test "nativeHash + verify round-trip" {
    var salt: [SALT_LEN]u8 = undefined;
    @memcpy(&salt, "abcdefghijklmnopqrst");
    const password = "hunter2";

    const hash = nativeHash(password, salt);
    try std.testing.expect(verify(password, salt, &hash));
    try std.testing.expect(!verify("wrong", salt, &hash));
    try std.testing.expect(!verify(password, salt, "short"));
}

test "verify accepts empty client_response when password is empty" {
    var salt: [SALT_LEN]u8 = undefined;
    @memcpy(&salt, "abcdefghijklmnopqrst");
    try std.testing.expect(verify("", salt, ""));
    try std.testing.expect(!verify("", salt, "anything"));
}

test "randomSalt produces only printable-ASCII bytes" {
    // MySQL Connector/J mishandles salt bytes > 0x7f, so the salt must stay in
    // the printable-ASCII range (0x21..0x7e) — also NUL-free for packet framing.
    var s: [SALT_LEN]u8 = undefined;
    var iter: usize = 0;
    while (iter < 64) : (iter += 1) {
        randomSalt(&s);
        for (s[0..]) |b| try std.testing.expect(b >= 0x21 and b <= 0x7e);
    }
}
