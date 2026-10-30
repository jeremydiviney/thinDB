//! Two-pass exact top-K for `GROUP BY ... ORDER BY COUNT(*) DESC LIMIT k`.
//!
//! The tally pass counts every input row into one of 2^bits counters by a
//! hash of its group identity, so a counter holds the summed row count of the
//! groups hashing to it and bounds each of them from above. The filter pass
//! groups only the rows whose counter reaches `threshold`: a group's rows
//! share one counter, so every kept group is complete, and every dropped
//! group has at most `threshold - 1` rows. Once the k-th best kept group has
//! `threshold - 1` rows or more, no dropped group can outrank it (one may tie
//! it, and SQL leaves ties at the cut unordered), so the kept groups' top k
//! is the answer.
//!
//! It pays when a few groups hold far more rows than the rest (URLs, users,
//! search phrases): the filter pass then groups a few percent of the rows.
//! Near-unique keys spread their rows evenly over the counters, keep nearly
//! every row, and `settle` declines.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// 2^18 u32 counters are 1 MiB per worker, and 100M rows leave ~380 rows of
/// collision noise per counter: far below the counts a skewed key's top
/// groups reach (thousands to hundreds of thousands on ClickBench).
pub const DEFAULT_BITS: u6 = 18;

/// The filter pass must keep at most 1/this of the rows to beat grouping
/// them all.
const MAX_KEPT_FRACTION_DENOM: u64 = 4;

/// Longest LIMIT + OFFSET the sketch serves; past it the threshold search
/// over the counters stops being cheap.
pub const MAX_NEED: usize = 1024;

/// Identities whose counter slots `tally` computes before incrementing them.
const TALLY_LANE: usize = 1024;

/// How many increments ahead `tally` prefetches. A worker's counters (1 MiB
/// at DEFAULT_BITS) overflow L2, and each random increment that waits on its
/// line holds up the stores queued behind it.
const TALLY_PREFETCH_DISTANCE: usize = 16;

pub const CountSketch = struct {
    bits: u6,
    /// `workers` slices of 2^bits counters, one per grid worker. A worker
    /// zeroes its own slice on first use, so idle workers cost nothing.
    counts: []u32,
    touched: []bool,
    /// Bit c set: the filter pass keeps the rows of counter c.
    keep: []u64 = &.{},
    threshold: u64 = 0,
    rows: u64 = 0,
    kept_rows: u64 = 0,

    pub fn init(allocator: Allocator, bits: u6, workers: usize) !CountSketch {
        std.debug.assert(bits >= 6);
        const counts = try allocator.alloc(u32, workers << bits);
        errdefer allocator.free(counts);
        const touched = try allocator.alloc(bool, workers);
        @memset(touched, false);
        return .{ .bits = bits, .counts = counts, .touched = touched };
    }

    pub fn deinit(self: *CountSketch, allocator: Allocator) void {
        allocator.free(self.counts);
        allocator.free(self.touched);
        if (self.keep.len > 0) allocator.free(self.keep);
        self.* = undefined;
    }

    pub fn counter(self: *const CountSketch, identity: u128) usize {
        var x: u64 = @as(u64, @truncate(identity)) ^ (@as(u64, @truncate(identity >> 64)) *% 0x9e3779b97f4a7c15);
        x ^= x >> 33;
        x *%= 0xff51afd7ed558ccd;
        x ^= x >> 33;
        x *%= 0xc4ceb9fe1a85ec53;
        x ^= x >> 33;
        return @intCast(x >> @intCast(64 - @as(u7, self.bits)));
    }

    /// Counts each identity's rows (`weights[i]` of them, else one) into
    /// `counts`, a worker's counters.
    pub fn tally(self: *const CountSketch, counts: []u32, ids: []const u128, weights: ?[]const u32) void {
        var slots: [TALLY_LANE]u32 = undefined;
        var lo: usize = 0;
        while (lo < ids.len) : (lo += TALLY_LANE) {
            const lane = ids[lo..@min(ids.len, lo + TALLY_LANE)];
            const lane_slots = slots[0..lane.len];
            for (lane, lane_slots) |id, *s| s.* = @intCast(self.counter(id));
            if (weights) |w| {
                for (lane_slots, w[lo..][0..lane.len], 0..) |s, n, i| {
                    if (i + TALLY_PREFETCH_DISTANCE < lane_slots.len) @prefetch(&counts[lane_slots[i + TALLY_PREFETCH_DISTANCE]], .{ .rw = .write });
                    counts[s] += n;
                }
            } else {
                for (lane_slots, 0..) |s, i| {
                    if (i + TALLY_PREFETCH_DISTANCE < lane_slots.len) @prefetch(&counts[lane_slots[i + TALLY_PREFETCH_DISTANCE]], .{ .rw = .write });
                    counts[s] += 1;
                }
            }
        }
    }

    /// Worker `w`'s counters. Only worker `w` may call this during a tally.
    pub fn workerCounts(self: *CountSketch, w: usize) []u32 {
        const n = @as(usize, 1) << self.bits;
        const slice = self.counts[w * n ..][0..n];
        if (!self.touched[w]) {
            @memset(slice, 0);
            self.touched[w] = true;
        }
        return slice;
    }

    /// Sum the workers' counters and pick the threshold for the `need` best
    /// groups: half the need-th largest counter. False when the filter pass
    /// would keep too many rows to pay.
    pub fn settle(self: *CountSketch, allocator: Allocator, need: usize) !bool {
        std.debug.assert(need > 0 and need <= MAX_NEED);
        const n = @as(usize, 1) << self.bits;
        var merged: ?[]u32 = null;
        for (self.touched, 0..) |t, w| {
            if (!t) continue;
            const slice = self.counts[w * n ..][0..n];
            if (merged) |m| {
                for (m, slice) |*d, s| d.* += s;
            } else merged = slice;
        }
        const totals = merged orelse return false;
        self.rows = 0;
        for (totals) |c| self.rows += c;

        var best: [MAX_NEED]u32 = undefined;
        const kth = kthLargest(totals, best[0..need]);
        if (@as(f64, @floatFromInt(kth)) <= collisionCeiling(self.rows, self.bits)) return false;
        self.threshold = @max(1, kth / 2);

        self.keep = try allocator.alloc(u64, n / 64);
        return self.fillKeep(totals);
    }

    /// Lower the threshold to `kth_count + 1` after a filter pass whose k-th
    /// best group held `kth_count` rows: that pass's top groups all survive,
    /// so the next filter pass is exact. False when it would keep too many
    /// rows to pay.
    pub fn retarget(self: *CountSketch, kth_count: u64) bool {
        const n = @as(usize, 1) << self.bits;
        const w = std.mem.indexOfScalar(bool, self.touched, true) orelse return false;
        self.threshold = kth_count + 1;
        return self.fillKeep(self.counts[w * n ..][0..n]);
    }

    fn fillKeep(self: *CountSketch, totals: []const u32) bool {
        @memset(self.keep, 0);
        self.kept_rows = 0;
        for (totals, 0..) |c, i| {
            if (c < self.threshold) continue;
            self.keep[i >> 6] |= @as(u64, 1) << @intCast(i & 63);
            self.kept_rows += c;
        }
        return self.kept_rows * MAX_KEPT_FRACTION_DENOM <= self.rows;
    }

    pub fn keeps(self: *const CountSketch, identity: u128) bool {
        const c = self.counter(identity);
        return (self.keep[c >> 6] >> @intCast(c & 63)) & 1 != 0;
    }

    /// Whether a filter pass that found `found` groups, the need-th best
    /// holding `kth_count` rows, found the top `need` of every group.
    pub fn exact(self: *const CountSketch, need: usize, found: usize, kth_count: u64) bool {
        if (self.kept_rows == self.rows) return true;
        return found >= need and kth_count + 1 >= self.threshold;
    }
};

/// About the largest counter `rows` rows of distinct groups reach by
/// collisions alone: each of the 2^bits counters draws ~Poisson(rows/2^bits)
/// rows, and the most loaded lands near mean + sqrt(2·mean·ln n), with a
/// ln n tail when the mean is small. A need-th largest counter at or below
/// it may hold no group bigger than one row, and a filter pass under it
/// keeps a slice of noise that rarely proves exact.
fn collisionCeiling(rows: u64, bits: u6) f64 {
    const counters: f64 = @floatFromInt(@as(u64, 1) << bits);
    const mean = @as(f64, @floatFromInt(rows)) / counters;
    const log_n = @log(counters);
    return mean + @sqrt(2 * mean * log_n) + log_n;
}

/// The `best.len`-th largest of `values` (0 when there are fewer), using
/// `best` as scratch kept in descending order.
fn kthLargest(values: []const u32, best: []u32) u32 {
    const k = best.len;
    var len: usize = 0;
    for (values) |v| {
        if (len == k and v <= best[k - 1]) continue;
        var i = if (len < k) len else k - 1;
        while (i > 0 and best[i - 1] < v) : (i -= 1) best[i] = best[i - 1];
        best[i] = v;
        if (len < k) len += 1;
    }
    return if (len == k) best[k - 1] else 0;
}

test "kthLargest picks the k-th largest value" {
    var best: [3]u32 = undefined;
    try std.testing.expectEqual(@as(u32, 7), kthLargest(&.{ 5, 9, 1, 7, 8, 2 }, &best));
    try std.testing.expectEqual(@as(u32, 0), kthLargest(&.{ 4, 6 }, &best));
    var one: [1]u32 = undefined;
    try std.testing.expectEqual(@as(u32, 9), kthLargest(&.{ 5, 9, 1 }, &one));
}

test "count sketch keeps the heavy groups and proves the top-k exact" {
    const allocator = std.testing.allocator;
    var sketch = try CountSketch.init(allocator, 10, 2);
    defer sketch.deinit(allocator);

    // Groups 0..3 hold 1000, 900, 800, 700 rows; groups 4.. hold one row each.
    var expected = std.AutoHashMap(u128, u64).init(allocator);
    defer expected.deinit();
    const heavy = [_]u64{ 1000, 900, 800, 700 };
    for (heavy, 0..) |n, g| {
        const counts = sketch.workerCounts(g % 2);
        counts[sketch.counter(g)] += @intCast(n);
        try expected.put(g, n);
    }
    var g: u128 = 4;
    while (g < 20_004) : (g += 1) {
        const counts = sketch.workerCounts(@intCast(g % 2));
        counts[sketch.counter(g)] += 1;
        try expected.put(g, 1);
    }

    try std.testing.expect(try sketch.settle(allocator, 3));
    try std.testing.expectEqual(@as(u64, 23_400), sketch.rows);
    for (0..4) |h| try std.testing.expect(sketch.keeps(h));
    try std.testing.expect(sketch.kept_rows < sketch.rows / 4);

    // The kept groups' 3rd best has 800 rows: well over the threshold.
    try std.testing.expect(sketch.exact(3, 4, 800));
    try std.testing.expect(!sketch.exact(3, 2, 900));
    try std.testing.expect(!sketch.exact(3, 4, sketch.threshold - 2));
}

test "tally counts like one increment per identity, by weight when given" {
    const allocator = std.testing.allocator;
    var sketch = try CountSketch.init(allocator, 9, 3);
    defer sketch.deinit(allocator);
    var prng = std.Random.DefaultPrng.init(0x7a11);
    const random = prng.random();

    const n = 2 * TALLY_LANE + 37;
    const ids = try allocator.alloc(u128, n);
    defer allocator.free(ids);
    const weights = try allocator.alloc(u32, n);
    defer allocator.free(weights);
    for (ids, weights) |*id, *w| {
        id.* = random.uintLessThan(u128, 3000);
        w.* = random.intRangeAtMost(u32, 1, 9);
    }

    sketch.tally(sketch.workerCounts(0), ids, null);
    sketch.tally(sketch.workerCounts(1), ids, weights);
    const expected_rows = try allocator.alloc(u32, @as(usize, 1) << sketch.bits);
    defer allocator.free(expected_rows);
    const expected_weighted = try allocator.alloc(u32, expected_rows.len);
    defer allocator.free(expected_weighted);
    @memset(expected_rows, 0);
    @memset(expected_weighted, 0);
    for (ids, weights) |id, w| {
        expected_rows[sketch.counter(id)] += 1;
        expected_weighted[sketch.counter(id)] += w;
    }
    try std.testing.expectEqualSlices(u32, expected_rows, sketch.workerCounts(0));
    try std.testing.expectEqualSlices(u32, expected_weighted, sketch.workerCounts(1));

    sketch.tally(sketch.workerCounts(2), ids[0..0], null);
    for (sketch.workerCounts(2)) |c| try std.testing.expectEqual(@as(u32, 0), c);
}

test "count sketch declines near-unique keys" {
    const allocator = std.testing.allocator;
    var sketch = try CountSketch.init(allocator, 8, 1);
    defer sketch.deinit(allocator);
    const counts = sketch.workerCounts(0);
    var g: u128 = 0;
    while (g < 100_000) : (g += 1) counts[sketch.counter(g)] += 1;
    try std.testing.expect(!try sketch.settle(allocator, 10));
}

test "count sketch declines unique keys spread thinner than the counters" {
    // ~1.2 rows per counter: a filter pass at half the 5th-largest counter
    // would keep a small slice of rows, all of them one-row groups.
    const allocator = std.testing.allocator;
    var sketch = try CountSketch.init(allocator, 12, 1);
    defer sketch.deinit(allocator);
    const counts = sketch.workerCounts(0);
    var g: u128 = 0;
    while (g < 5_000) : (g += 1) counts[sketch.counter(g)] += 1;
    try std.testing.expect(!try sketch.settle(allocator, 5));
}

test "retarget lowers the threshold to one past the found k-th count" {
    const allocator = std.testing.allocator;
    var sketch = try CountSketch.init(allocator, 8, 1);
    defer sketch.deinit(allocator);
    const counts = sketch.workerCounts(0);
    counts[sketch.counter(1)] += 500;
    counts[sketch.counter(2)] += 400;
    var g: u128 = 10;
    while (g < 5_000) : (g += 1) counts[sketch.counter(g)] += 1;
    try std.testing.expect(try sketch.settle(allocator, 2));
    try std.testing.expect(sketch.retarget(399));
    try std.testing.expectEqual(@as(u64, 400), sketch.threshold);
    try std.testing.expect(sketch.keeps(1) and sketch.keeps(2));
    try std.testing.expect(sketch.exact(2, 2, 399));
}
