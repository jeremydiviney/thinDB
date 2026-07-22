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
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

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

test "racy memory-pattern scaling (isolates atomics vs DRAM)" {
    if (getenv("THINDB_BENCH") == null) return error.SkipZigTest;
    const allocator = std.heap.page_allocator;
    var cap: usize = 1;
    while (cap < 33_000_000) cap <<= 1;
    const arr = try allocator.alloc(u64, cap);
    defer allocator.free(arr);
    @memset(arr, 0);
    var T: usize = 12;
    if (getenv("THINDB_BENCH_T")) |s| T = std.fmt.parseInt(usize, std.mem.span(s), 10) catch 12;
    const total: usize = 100_000_000;
    const per = total / T;
    const Ctx = struct {
        a: []u64,
        m: u64,
        per: usize,
        seed: u64,
        fn run(self: @This()) void {
            var x = self.seed;
            var j: usize = 0;
            while (j < self.per) : (j += 1) {
                x = (x *% 6364136223846793005) +% 1442695040888963407;
                self.a[(x >> 16) & self.m] +%= 1; // plain RMW: same memory pattern, NO atomics
            }
        }
    };
    const win = std.os.windows;
    const qpc = struct {
        fn now() i64 {
            switch (builtin.os.tag) {
                .windows => {
                    var c: win.LARGE_INTEGER = undefined;
                    _ = win.ntdll.RtlQueryPerformanceCounter(&c);
                    return c;
                },
                .linux => {
                    var ts: std.os.linux.timespec = undefined;
                    _ = std.os.linux.clock_gettime(std.os.linux.CLOCK.MONOTONIC, &ts);
                    return @as(i64, ts.sec) * 1_000_000_000 + ts.nsec;
                },
                else => return 0,
            }
        }
        fn freq() i64 {
            switch (builtin.os.tag) {
                .windows => {
                    var f: win.LARGE_INTEGER = undefined;
                    _ = win.ntdll.RtlQueryPerformanceFrequency(&f);
                    return f;
                },
                .linux => return 1_000_000_000,
                else => return 1,
            }
        }
    };
    var threads: [64]std.Thread = undefined;
    const t0 = qpc.now();
    for (threads[0..T], 0..) |*th, i| th.* = try std.Thread.spawn(.{}, Ctx.run, .{Ctx{ .a = arr, .m = cap - 1, .per = per, .seed = @as(u64, i) *% 0x9e3779b97f4a7c15 +% 1 }});
    for (threads[0..T]) |th| th.join();
    const ms = @as(f64, @floatFromInt(qpc.now() - t0)) / @as(f64, @floatFromInt(qpc.freq())) * 1000.0;
    var chk: u64 = 0;
    for (arr) |v| chk +%= v;
    std.debug.print("\n[bench-racy] {d} plain RMW (no atomics), {d} threads: {d:.1} ms (chk={d})\n", .{ total, T, ms, chk });
}

test "ConcurrentIntTable: throughput (Q9-shaped: 100M bumps, ~17M distinct, 12 threads)" {
    if (getenv("THINDB_BENCH") == null) return error.SkipZigTest;
    const allocator = std.heap.page_allocator;
    const distinct: u64 = 17_000_000;
    var t = try ConcurrentIntTable.init(allocator, distinct);
    defer t.deinit();
    const Ctx = struct {
        tbl: *ConcurrentIntTable,
        per: usize,
        seed: u64,
        d: u64,
        fn run(self: @This()) void {
            var x = self.seed;
            var j: usize = 0;
            while (j < self.per) : (j += 1) {
                x = (x *% 6364136223846793005) +% 1442695040888963407; // LCG
                self.tbl.bump((x >> 16) % self.d +% 1);
            }
        }
    };
    var T: usize = 12;
    if (getenv("THINDB_BENCH_T")) |s| T = std.fmt.parseInt(usize, std.mem.span(s), 10) catch 12;
    const total: usize = 100_000_000;
    const per = total / T;
    const win = std.os.windows;
    const qpc = struct {
        fn now() i64 {
            switch (builtin.os.tag) {
                .windows => {
                    var c: win.LARGE_INTEGER = undefined;
                    _ = win.ntdll.RtlQueryPerformanceCounter(&c);
                    return c;
                },
                .linux => {
                    var ts: std.os.linux.timespec = undefined;
                    _ = std.os.linux.clock_gettime(std.os.linux.CLOCK.MONOTONIC, &ts);
                    return @as(i64, ts.sec) * 1_000_000_000 + ts.nsec;
                },
                else => return 0,
            }
        }
        fn freq() i64 {
            switch (builtin.os.tag) {
                .windows => {
                    var f: win.LARGE_INTEGER = undefined;
                    _ = win.ntdll.RtlQueryPerformanceFrequency(&f);
                    return f;
                },
                .linux => return 1_000_000_000,
                else => return 1,
            }
        }
    };
    var threads: [64]std.Thread = undefined;
    const t0 = qpc.now();
    for (threads[0..T], 0..) |*th, i| th.* = try std.Thread.spawn(.{}, Ctx.run, .{Ctx{ .tbl = &t, .per = per, .seed = @as(u64, i) *% 0x9e3779b97f4a7c15 +% 1, .d = distinct }});
    for (threads[0..T]) |th| th.join();
    const ms = @as(f64, @floatFromInt(qpc.now() - t0)) / @as(f64, @floatFromInt(qpc.freq())) * 1000.0;
    std.debug.print("\n[bench] {d} bumps, {d} distinct, {d} threads: {d:.1} ms ({d:.0} M bumps/s)\n", .{ total, t.groupCount(), T, ms, @as(f64, @floatFromInt(total)) / (ms / 1000.0) / 1e6 });
}

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
