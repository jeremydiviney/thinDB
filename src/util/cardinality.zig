//! Bounded distinct-value counter with an "ejection seat".
//!
//! Tracks the exact number of distinct values it has seen, up to a limit.
//! The moment the distinct count would reach `limit`, it ejects: it drops
//! the tracking set, marks itself `big`, and ignores all further values.
//! So the cost is bounded — a near-unique field stops costing anything
//! after the first `limit` distinct values, and a low-cardinality field
//! keeps a small set.
//!
//! Used at flush time to record a per-field cardinality stat: an exact
//! count when the field has fewer than `limit` distinct values, or `big`
//! otherwise. `big` is a sound one-sided signal — a `big` field is
//! guaranteed to have at least `limit` distinct values.
//!
//! Keys are arbitrary encoded value bytes; the counter owns copies of the
//! distinct ones (in an arena) until ejection or deinit.

const std = @import("std");

pub const Cardinality = union(enum) {
    /// Fewer than `limit` distinct values — the exact count.
    exact: u64,
    /// At least `limit` distinct values (tracking was ejected).
    big,
};

pub const CardinalityCounter = struct {
    arena: std.heap.ArenaAllocator,
    set: std.StringHashMapUnmanaged(void) = .empty,
    limit: usize,
    big: bool = false,

    pub fn init(child_allocator: std.mem.Allocator, limit: usize) CardinalityCounter {
        return .{ .arena = std.heap.ArenaAllocator.init(child_allocator), .limit = limit };
    }

    pub fn deinit(self: *CardinalityCounter) void {
        self.arena.deinit();
    }

    /// Observe one value. `key` is the encoded value bytes; only borrowed
    /// for the duration of the call (a copy is kept iff the value is new
    /// and we haven't ejected).
    pub fn add(self: *CardinalityCounter, key: []const u8) !void {
        if (self.big) return;
        const a = self.arena.allocator();
        const gop = try self.set.getOrPut(a, key);
        if (!gop.found_existing) {
            // getOrPut already counted this entry. If that reaches the
            // limit, eject: we've proven cardinality ≥ limit, so drop the
            // set and reclaim its memory — no point tracking further.
            if (self.set.count() >= self.limit) {
                self.big = true;
                self.set = .empty;
                _ = self.arena.reset(.free_all);
                return;
            }
            gop.key_ptr.* = try a.dupe(u8, key);
        }
    }

    pub fn result(self: *const CardinalityCounter) Cardinality {
        if (self.big) return .big;
        return .{ .exact = self.set.count() };
    }
};

test "cardinality: counts distinct values exactly below the limit" {
    var c = CardinalityCounter.init(std.testing.allocator, 100);
    defer c.deinit();
    try c.add("a");
    try c.add("b");
    try c.add("a"); // dup
    try c.add("c");
    try c.add("b"); // dup
    try std.testing.expectEqual(Cardinality{ .exact = 3 }, c.result());
}

test "cardinality: empty counter is exact zero" {
    var c = CardinalityCounter.init(std.testing.allocator, 8);
    defer c.deinit();
    try std.testing.expectEqual(Cardinality{ .exact = 0 }, c.result());
}

test "cardinality: ejects to big at the limit and stays big" {
    var c = CardinalityCounter.init(std.testing.allocator, 4);
    defer c.deinit();
    var buf: [16]u8 = undefined;
    // 3 distinct → still exact.
    for (0..3) |i| try c.add(try std.fmt.bufPrint(&buf, "v{d}", .{i}));
    try std.testing.expectEqual(Cardinality{ .exact = 3 }, c.result());
    // 4th distinct hits the limit → big.
    try c.add("v3");
    try std.testing.expectEqual(Cardinality.big, c.result());
    // More values (new or dup) stay big and are no-ops.
    try c.add("v3");
    try c.add("v999");
    try std.testing.expectEqual(Cardinality.big, c.result());
}

test "cardinality: high-cardinality near-unique input ejects cheaply" {
    var c = CardinalityCounter.init(std.testing.allocator, 1024);
    defer c.deinit();
    var buf: [16]u8 = undefined;
    var i: usize = 0;
    while (i < 5000) : (i += 1) {
        try c.add(try std.fmt.bufPrint(&buf, "{d}", .{i}));
    }
    try std.testing.expectEqual(Cardinality.big, c.result());
}
