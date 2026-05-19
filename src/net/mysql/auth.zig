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

pub const SALT_LEN: usize = 20;
pub const HASH_LEN: usize = 20;

var salt_counter: std.atomic.Value(u64) = .{ .raw = 0 };
/// Mutable process-start seed, written once on the first randomSalt
/// call and mixed into every subsequent salt. Seeded from the stack
/// pointer of the first caller, which is essentially "address-space
/// randomization gave us this much entropy at startup."
var salt_anchor: std.atomic.Value(u64) = .{ .raw = 0 };

/// Fill `out` with 20 unpredictable non-zero bytes. The salt is sent
/// in cleartext as part of the HandshakeV10 packet, so the security
/// requirement is only that a passive observer can't predict the
/// next connection's salt from THIS one. SHA1 of (anchor || counter
/// || stack-pointer) gives us that, deterministic-but-distinct per
/// call, without depending on a crypto RNG. Zig 0.16's stdlib has
/// no cross-platform crypto-random surface; the dep added when we
/// implement TLS can be reused here.
pub fn randomSalt(out: *[SALT_LEN]u8) void {
    var anchor = salt_anchor.load(.monotonic);
    if (anchor == 0) {
        anchor = @as(u64, @truncate(@intFromPtr(out))) ^ 0xA5A5_5A5A_DEAD_BEEF;
        if (anchor == 0) anchor = 1;
        salt_anchor.store(anchor, .monotonic);
    }
    const ctr = salt_counter.fetchAdd(1, .monotonic);
    var seed: [24]u8 = undefined;
    std.mem.writeInt(u64, seed[0..8], anchor, .little);
    std.mem.writeInt(u64, seed[8..16], ctr, .little);
    const sp: usize = @intFromPtr(out);
    std.mem.writeInt(u64, seed[16..24], @as(u64, @truncate(sp)), .little);
    Sha1.hash(&seed, out, .{});
    for (out[0..]) |*b| {
        if (b.* == 0) b.* = 1;
    }
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

test "randomSalt produces non-zero bytes" {
    var s: [SALT_LEN]u8 = undefined;
    randomSalt(&s);
    for (s[0..]) |b| try std.testing.expect(b != 0);
}
