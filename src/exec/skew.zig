//! Misra-Gries top-K frequency estimator. Used during hash join's
//! build phase to detect heavy key skew — when one key dominates
//! the build side, the hash bucket grows large and probe-time bucket
//! walks hurt cache locality. SMJ has bounded scratch + sequential
//! emit, so it's the preferred algorithm under skew.
//!
//! The algorithm: track K counters. For each observed key:
//!   - If the key is already in the counters → increment.
//!   - Else if any counter is empty (count = 0) → assign the key.
//!   - Else → decrement all counters by 1.
//!
//! After N observations, any key with frequency > N/(K+1) is
//! guaranteed to appear in the counters with a (possibly under-
//! counted but never over-counted) frequency. We use that to
//! gate "is there a heavy hitter?"
//!
//! Memory: O(K). One pass. No false positives for the heavy hitters
//! we report (they're real); we may UNDER-report frequency.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Number of Misra-Gries counters. Smaller K → cheaper per-observe
/// (fewer compares + decrements) but a higher minimum-detectable
/// skew threshold. With K=16, any key with frequency > N/17 (≈6%)
/// is guaranteed to be tracked — comfortably below typical skew
/// thresholds (50%+).
pub const K: usize = 16;

pub const MisraGries = struct {
    // Fixed-size array of counters. Keys are owned byte slices
    // allocated from the user's allocator on first assignment.
    // A separately-tracked u64 hash per slot is used as a fast-
    // reject filter (most non-matches differ in the hash too).
    counters: [K]Counter,
    hashes: [K]u64,
    allocator: Allocator,
    observed_total: u64 = 0,

    const Counter = struct {
        key: ?[]u8 = null,
        count: u32 = 0,
    };

    pub fn init(allocator: Allocator) MisraGries {
        var self: MisraGries = .{
            .counters = undefined,
            .hashes = undefined,
            .allocator = allocator,
        };
        for (&self.counters) |*c| c.* = .{};
        for (&self.hashes) |*h| h.* = 0;
        return self;
    }

    pub fn deinit(self: *MisraGries) void {
        for (&self.counters) |*c| {
            if (c.key) |k| self.allocator.free(k);
        }
        self.* = undefined;
    }

    pub fn observe(self: *MisraGries, key: []const u8) !void {
        self.observed_total += 1;
        const key_hash = std.hash.Wyhash.hash(0, key);
        // Existing counter for this key? Increment. Hash reject first
        // — only a full eql when hashes match (rare for distinct keys).
        for (&self.counters, &self.hashes) |*c, h| {
            if (c.count == 0) continue;
            if (h != key_hash) continue;
            if (std.mem.eql(u8, c.key.?, key)) {
                c.count += 1;
                return;
            }
        }
        // Empty slot? Assign.
        for (&self.counters, &self.hashes) |*c, *h| {
            if (c.count == 0) {
                c.key = try self.allocator.dupe(u8, key);
                c.count = 1;
                h.* = key_hash;
                return;
            }
        }
        // All counters in use and none matched — decrement all.
        for (&self.counters) |*c| {
            c.count -= 1;
            if (c.count == 0) {
                if (c.key) |k| {
                    self.allocator.free(k);
                    c.key = null;
                }
            }
        }
    }

    /// Highest counter value across all slots. An UNDER-estimate of
    /// the true top-key frequency (true count >= reported count).
    pub fn topFrequency(self: MisraGries) u32 {
        var max: u32 = 0;
        for (self.counters) |c| {
            if (c.count > max) max = c.count;
        }
        return max;
    }
};

// Tests

test "MisraGries: spots a heavy hitter in a small stream" {
    var mg = MisraGries.init(std.testing.allocator);
    defer mg.deinit();

    // 100 observations: 80 of "X", 20 spread across "A".."T".
    var i: usize = 0;
    while (i < 80) : (i += 1) try mg.observe("X");
    const others = [_][]const u8{ "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T" };
    for (others) |k| try mg.observe(k);

    // Heavy hitter X has count >= ceiling threshold.
    try std.testing.expect(mg.topFrequency() >= 60);
}

test "MisraGries: uniform distribution stays below threshold" {
    var mg = MisraGries.init(std.testing.allocator);
    defer mg.deinit();

    // 1000 observations across 1000 distinct keys — no heavy hitter.
    var buf: [16]u8 = undefined;
    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        const key = try std.fmt.bufPrint(&buf, "key_{d:0>10}", .{i});
        try mg.observe(key);
    }
    // No single key has high frequency.
    try std.testing.expect(mg.topFrequency() <= 5);
}

test "MisraGries: moderate skew is bounded" {
    var mg = MisraGries.init(std.testing.allocator);
    defer mg.deinit();

    // 100 observations: 20 of "HOT", 80 of unique keys.
    var i: usize = 0;
    while (i < 20) : (i += 1) try mg.observe("HOT");
    var buf: [16]u8 = undefined;
    while (i < 100) : (i += 1) {
        const key = try std.fmt.bufPrint(&buf, "k{d}", .{i});
        try mg.observe(key);
    }
    // Top is "HOT" with count <= 20 (true value is 20).
    try std.testing.expect(mg.topFrequency() <= 20);
}
