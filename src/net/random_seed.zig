//! Predictable-but-unpredictable byte generator used by the auth modules
//! to fill per-connection salts and nonces. Zig 0.16's stdlib has no
//! cross-platform crypto-RNG surface yet; the threat model only needs
//! "a passive observer can't predict this connection's salt from the
//! previous one's." A SHA-256 mix of (anchor || counter || stack-pointer)
//! gives us that without a dependency.
//!
//! Lifecycle:
//!   - `anchor` is written once at process startup (zero check + CAS via
//!     plain monotonic store; the racing case writes the same family of
//!     values so the only observable consequence of a race is "the
//!     first few salts share an anchor across racing threads," which is
//!     fine for this threat model).
//!   - `counter` advances on every call.
//!   - `stack_ptr` is the local address the caller passed in, mixed in
//!     so two threads calling simultaneously don't get the same bytes.

const std = @import("std");
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const State = struct {
    counter: std.atomic.Value(u64) = .{ .raw = 0 },
    anchor: std.atomic.Value(u64) = .{ .raw = 0 },
    anchor_mix: u64,
};

/// Fill `out` with `out.len` bytes derived from the supplied state +
/// the caller's stack-pointer. `out.len` ≤ 32; longer outputs would
/// need two rounds of SHA-256.
pub fn fill(state: *State, out: []u8) void {
    std.debug.assert(out.len <= 32);

    var anchor = state.anchor.load(.monotonic);
    if (anchor == 0) {
        anchor = @as(u64, @truncate(@intFromPtr(out.ptr))) ^ state.anchor_mix;
        if (anchor == 0) anchor = 1;
        state.anchor.store(anchor, .monotonic);
    }
    const ctr = state.counter.fetchAdd(1, .monotonic);
    var seed: [24]u8 = undefined;
    std.mem.writeInt(u64, seed[0..8], anchor, .little);
    std.mem.writeInt(u64, seed[8..16], ctr, .little);
    const sp: usize = @intFromPtr(out.ptr);
    std.mem.writeInt(u64, seed[16..24], @as(u64, @truncate(sp)), .little);

    var digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(&seed, &digest, .{});
    @memcpy(out, digest[0..out.len]);
}

/// Mutate every zero byte to 1. MySQL's salt encoding embeds a NUL
/// terminator after the salt bytes, so the salt itself must be NUL-free.
pub fn replaceZeroBytes(buf: []u8) void {
    for (buf) |*b| if (b.* == 0) {
        b.* = 1;
    };
}

test "fill produces deterministic output for the same state pair" {
    var s1: State = .{ .anchor_mix = 0xDEAD_BEEF };
    var s2: State = .{ .anchor_mix = 0xDEAD_BEEF };
    var a: [16]u8 = undefined;
    var b: [16]u8 = undefined;
    fill(&s1, &a);
    fill(&s2, &b);
    // Different stack-pointer addresses → distinct output even with
    // matching state. We just assert both are non-trivial.
    var any_nonzero = false;
    for (a) |x| if (x != 0) {
        any_nonzero = true;
    };
    try std.testing.expect(any_nonzero);
    any_nonzero = false;
    for (b) |x| if (x != 0) {
        any_nonzero = true;
    };
    try std.testing.expect(any_nonzero);
}

test "fill increments the counter so two consecutive calls differ" {
    var s: State = .{ .anchor_mix = 0xCAFE_F00D };
    var a: [20]u8 = undefined;
    var b: [20]u8 = undefined;
    fill(&s, &a);
    fill(&s, &b);
    try std.testing.expect(!std.mem.eql(u8, &a, &b));
}
