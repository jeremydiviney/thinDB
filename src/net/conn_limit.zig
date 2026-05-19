//! Server-wide cap on concurrent client connections.
//!
//! Shared across every wire (MySQL, PG, native) when the binary spins
//! up a single Catalog: one limiter, three listeners. Each per-
//! connection handler tries `acquire()` first; on success it owns one
//! slot until `release()`. On failure the wire emits its
//! protocol-specific "too many connections" error and closes the
//! socket.
//!
//! The counter is atomic + lock-free; a CAS loop guards against the
//! race window between read-cap and increment.

const std = @import("std");

pub const ConnectionLimiter = struct {
    active: std.atomic.Value(u32) = .{ .raw = 0 },
    cap: u32,

    pub fn init(cap: u32) ConnectionLimiter {
        return .{ .cap = cap };
    }

    /// Try to reserve a slot. Returns true on success (caller must
    /// `release()` later); false if the cap is already saturated.
    pub fn acquire(self: *ConnectionLimiter) bool {
        var current = self.active.load(.acquire);
        while (true) {
            if (current >= self.cap) return false;
            const next = current + 1;
            const witnessed = self.active.cmpxchgWeak(current, next, .acq_rel, .acquire) orelse return true;
            current = witnessed;
        }
    }

    pub fn release(self: *ConnectionLimiter) void {
        _ = self.active.fetchSub(1, .acq_rel);
    }

    /// Snapshot the current in-use count. Strictly informational —
    /// callers must not use it for admission decisions.
    pub fn count(self: *const ConnectionLimiter) u32 {
        return self.active.load(.acquire);
    }
};

test "limiter rejects acquisition past the cap" {
    var lim = ConnectionLimiter.init(2);
    try std.testing.expect(lim.acquire());
    try std.testing.expect(lim.acquire());
    try std.testing.expect(!lim.acquire());
    lim.release();
    try std.testing.expect(lim.acquire());
    try std.testing.expectEqual(@as(u32, 2), lim.count());
    lim.release();
    lim.release();
    try std.testing.expectEqual(@as(u32, 0), lim.count());
}

test "limiter with cap=0 always rejects" {
    var lim = ConnectionLimiter.init(0);
    try std.testing.expect(!lim.acquire());
}
