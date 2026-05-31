//! Concurrent open-addressing hash table for packed integer keys.
//!
//! The basis for parallel high-cardinality GROUP BY / COUNT(DISTINCT): one shared,
//! pre-sized table that every worker thread `bump`s into directly — no per-silo
//! tables, no merge. Correctness rests on two facts:
//!   * a new key is claimed with a single CAS on the slot (the winner publishes
//!     the key; losers re-read and either find their key or probe on), and
//!   * the per-group count is an atomic add, so concurrent updates to the same
//!     group never lose an increment.
//! It never grows — the caller pre-sizes to a cardinality upper bound, so the
//! linear probe always terminates. Contention is rare exactly when this path is
//! chosen (high card ⇒ a large table ⇒ two threads almost never touch the same
//! slot at once), so the atomics seldom actually contend and it scales nearly
//! linearly. Key `0` is the empty sentinel; a real key `0` is held in side fields.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const ConcurrentIntTable = struct {
    keys: []u64, // 0 = empty slot; every access is atomic
    counts: []u64, // per-slot running count; every access is atomic
    mask: u64,
    n_groups: std.atomic.Value(u64), // distinct keys claimed (incl. the key-0 group)
    zero_present: std.atomic.Value(u32),
    zero_count: std.atomic.Value(u64),
    allocator: Allocator,

    pub fn init(allocator: Allocator, expected_groups: usize) !ConcurrentIntTable {
        var cap: usize = 1024;
        const want = (expected_groups *| 10) / 7 + 1; // ~0.7 load factor headroom
        while (cap < want) cap <<= 1;
        const keys = try allocator.alloc(u64, cap);
        errdefer allocator.free(keys);
        @memset(keys, 0);
        const counts = try allocator.alloc(u64, cap);
        @memset(counts, 0);
        return .{
            .keys = keys,
            .counts = counts,
            .mask = cap - 1,
            .n_groups = .init(0),
            .zero_present = .init(0),
            .zero_count = .init(0),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ConcurrentIntTable) void {
        self.allocator.free(self.keys);
        self.allocator.free(self.counts);
    }

    inline fn hashKey(key: u64) u64 {
        var z = key +% 0x9e3779b97f4a7c15;
        z = (z ^ (z >> 30)) *% 0xbf58476d1ce4e5b9;
        z = (z ^ (z >> 27)) *% 0x94d049bb133111eb;
        return z ^ (z >> 31);
    }

    /// Add one occurrence of `key`. Thread-safe across any number of callers.
    pub fn bump(self: *ConcurrentIntTable, key: u64) void {
        if (key == 0) {
            if (self.zero_present.swap(1, .monotonic) == 0) _ = self.n_groups.fetchAdd(1, .monotonic);
            _ = self.zero_count.fetchAdd(1, .monotonic);
            return;
        }
        var i = hashKey(key) & self.mask;
        while (true) {
            const k = @atomicLoad(u64, &self.keys[i], .acquire);
            if (k == key) {
                _ = @atomicRmw(u64, &self.counts[i], .Add, 1, .monotonic);
                return;
            }
            if (k == 0) {
                if (@cmpxchgStrong(u64, &self.keys[i], 0, key, .acq_rel, .acquire) == null) {
                    _ = self.n_groups.fetchAdd(1, .monotonic);
                    _ = @atomicRmw(u64, &self.counts[i], .Add, 1, .monotonic);
                    return;
                }
                continue; // lost the claim — re-read this slot (now non-empty)
            }
            i = (i + 1) & self.mask; // linear probe
        }
    }

    /// Number of distinct keys (= group count). Call after all `bump`s.
    pub fn groupCount(self: *const ConcurrentIntTable) u64 {
        return self.n_groups.load(.monotonic);
    }
};

test "ConcurrentIntTable: concurrent bump is exact" {
    const allocator = std.testing.allocator;
    var t = try ConcurrentIntTable.init(allocator, 100_000);
    defer t.deinit();

    const Ctx = struct {
        tbl: *ConcurrentIntTable,
        k: u64,
        r: usize,
        fn run(self: @This()) void {
            var rr: usize = 0;
            while (rr < self.r) : (rr += 1) {
                var key: u64 = 0; // include the key-0 group
                while (key <= self.k) : (key += 1) self.tbl.bump(key);
            }
        }
    };
    const T = 8;
    const K: u64 = 5000;
    const R = 20;
    var threads: [T]std.Thread = undefined;
    for (&threads) |*th| th.* = try std.Thread.spawn(.{}, Ctx.run, .{Ctx{ .tbl = &t, .k = K, .r = R }});
    for (&threads) |th| th.join();

    try std.testing.expectEqual(@as(u64, K + 1), t.groupCount()); // keys 0..K
    try std.testing.expectEqual(@as(u64, T * R), t.zero_count.load(.monotonic));
    var key: u64 = 1;
    while (key <= K) : (key += 1) {
        var i = ConcurrentIntTable.hashKey(key) & t.mask;
        while (true) {
            const k = t.keys[i];
            if (k == key) {
                try std.testing.expectEqual(@as(u64, T * R), t.counts[i]);
                break;
            }
            if (k == 0) return error.KeyMissing;
            i = (i + 1) & t.mask;
        }
    }
}
