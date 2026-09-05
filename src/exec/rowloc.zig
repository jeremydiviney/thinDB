//! Physical row-location packing for late materialization.
//!
//! The `LateScan` operator decodes only the columns a filter + ORDER BY need,
//! plus a hidden `__rowloc` BIGINT per row that encodes WHERE the row lives so
//! the wide output columns can be fetched for just the survivors after a
//! TopN. A location is one of two shapes packed into an i64:
//!
//!   - segment row:  seg_idx | rg_idx | local_offset-within-row-group
//!   - memtable row: a sentinel marker + the memtable row index
//!
//! Bit layout (i64, sign bit kept clear so the value is always non-negative):
//!
//!   bits [0,  OFFSET_BITS)            local offset within the row group
//!   bits [OFFSET_BITS, +RG_BITS)      row-group index within the segment
//!   bits [.., +SEG_BITS)              segment index in the manifest snapshot
//!
//! The segment field's all-ones value (`seg_sentinel`) flags a memtable row;
//! its lower (OFFSET_BITS + RG_BITS) bits then hold the memtable row index.
//! The field widths comfortably exceed the documented engine limits
//! (DESIGN.md §11: 2^32 segments, 2^32 rows/segment, 65,536-row row groups),
//! and `packSegment` asserts every component fits before shifting.

const std = @import("std");
const parallel = @import("../util/parallel.zig");

pub const col_name = "__rowloc";

const OFFSET_BITS: u6 = 24;
const RG_BITS: u6 = 18;
const SEG_BITS: u6 = 21;

const offset_mask: u64 = (@as(u64, 1) << OFFSET_BITS) - 1;
const rg_mask: u64 = (@as(u64, 1) << RG_BITS) - 1;
const seg_mask: u64 = (@as(u64, 1) << SEG_BITS) - 1;

const rg_shift: u6 = OFFSET_BITS;
const seg_shift: u6 = OFFSET_BITS + RG_BITS;

/// All-ones in the segment field — distinguishes a memtable location from any
/// real segment (segment indices are dense from 0 and never reach this).
const seg_sentinel: u64 = seg_mask;

pub const Location = union(enum) {
    segment: struct { seg_idx: usize, rg_idx: usize, offset: usize },
    memtable: struct { row: usize },
};

pub fn packSegment(seg_idx: usize, rg_idx: usize, offset: usize) i64 {
    std.debug.assert(offset <= offset_mask);
    std.debug.assert(rg_idx <= rg_mask);
    // Segment indices must stay below the sentinel; the documented limit is
    // 2^32 segments but a single manifest snapshot far smaller in practice.
    std.debug.assert(seg_idx < seg_sentinel);
    const packed_bits = (@as(u64, @intCast(seg_idx)) << seg_shift) |
        (@as(u64, @intCast(rg_idx)) << rg_shift) |
        @as(u64, @intCast(offset));
    return @bitCast(packed_bits);
}

pub fn packMemtable(row: usize) i64 {
    const lower: u64 = @intCast(row);
    std.debug.assert(lower <= ((@as(u64, 1) << seg_shift) - 1));
    const packed_bits = (seg_sentinel << seg_shift) | lower;
    return @bitCast(packed_bits);
}

pub fn unpack(value: i64) Location {
    const bits: u64 = @bitCast(value);
    const seg = (bits >> seg_shift) & seg_mask;
    if (seg == seg_sentinel) {
        return .{ .memtable = .{ .row = @intCast(bits & ((@as(u64, 1) << seg_shift) - 1)) } };
    }
    return .{ .segment = .{
        .seg_idx = @intCast(seg),
        .rg_idx = @intCast((bits >> rg_shift) & rg_mask),
        .offset = @intCast(bits & offset_mask),
    } };
}

test "rowloc round-trips segment locations" {
    const cases = .{
        .{ .s = 0, .r = 0, .o = 0 },
        .{ .s = 1, .r = 0, .o = 5 },
        .{ .s = 7, .r = 3, .o = 65535 },
        .{ .s = 1234, .r = 99, .o = 16777215 },
    };
    inline for (cases) |c| {
        const packed_bits = packSegment(c.s, c.r, c.o);
        try std.testing.expect(packed_bits >= 0);
        const loc = unpack(packed_bits);
        try std.testing.expectEqual(@as(usize, c.s), loc.segment.seg_idx);
        try std.testing.expectEqual(@as(usize, c.r), loc.segment.rg_idx);
        try std.testing.expectEqual(@as(usize, c.o), loc.segment.offset);
    }
}

test "rowloc round-trips memtable locations and is distinct from segment 0" {
    const seg0 = unpack(packSegment(0, 0, 0));
    try std.testing.expect(seg0 == .segment);
    inline for (.{ 0, 1, 42, 1_000_000 }) |row| {
        const loc = unpack(packMemtable(row));
        try std.testing.expect(loc == .memtable);
        try std.testing.expectEqual(@as(usize, row), loc.memtable.row);
    }
}

const COUNTING_MIN_ROWS: usize = 1024;
const COUNTING_MAX_RUNS: u64 = 1 << 20;
const DENSE_MAX_KEY: u64 = 1 << 22;
/// Below this many locations the passes aren't worth spreading over threads.
const STRIPE_MIN_ROWS: usize = 1 << 18;
/// Per-thread run histograms stay small: a wider run space orders serially.
const STRIPE_MAX_RUNS: usize = 1 << 16;

/// Permutation ordering `locs` ascending — segment rows by (segment, row
/// group, offset), memtable rows after them by row: the location order that
/// late materialization batches per row group. Survivors cluster into a few
/// row groups, so this counts them per row group and orders each row-group
/// run by offset (a bitmap pass when the run is dense, a small sort
/// otherwise): linear in the survivor count where the comparison sort it
/// replaces was n·log n over an indirect, cache-hostile comparator. A wide
/// row-group space (many segments × many row groups) or a small input falls
/// back to that comparison sort. Duplicate locations keep an arbitrary
/// relative order, as before.
pub fn sortedOrder(allocator: std.mem.Allocator, locs: []const i64, order: []u32) !void {
    return sortedOrderOn(allocator, locs, order, 1);
}

/// `sortedOrder` with its passes striped over `threads` workers: the
/// extents scan, per-thread run histograms, the scatter through each
/// thread's private run offsets (a thread's rows land after the earlier
/// threads' rows of the same run, so the pairs come out exactly as the
/// serial scatter's), and the runs ordered under a dynamic claim with
/// per-thread scratch. The permutation is identical to the serial one.
/// Small inputs and wide run spaces stay serial.
pub fn sortedOrderOn(allocator: std.mem.Allocator, locs: []const i64, order: []u32, threads: usize) !void {
    std.debug.assert(order.len == locs.len);
    const n = locs.len;
    if (n < COUNTING_MIN_ROWS) return comparisonOrder(locs, order);
    const nt: usize = if (threads > 1 and n >= STRIPE_MIN_ROWS) @min(threads, parallel.MAX_THREADS) else 1;

    var extents: [parallel.MAX_THREADS]Extent = undefined;
    for (extents[0..nt]) |*e| e.* = .{};
    const extent_pass = ExtentPass{ .locs = locs, .out = &extents };
    parallel.forRanges(nt, n, &extent_pass, ExtentPass.run);
    var max_seg: u64 = 0;
    var max_rg: u64 = 0;
    for (extents[0..nt]) |e| {
        max_seg = @max(max_seg, e.max_seg);
        max_rg = @max(max_rg, e.max_rg);
    }
    const rg_stride = max_rg + 1;
    const memtable_run = (max_seg + 1) * rg_stride;
    if (memtable_run > COUNTING_MAX_RUNS) return comparisonOrder(locs, order);
    const run_count: usize = @intCast(memtable_run + 1);
    const nt_runs: usize = if (run_count <= STRIPE_MAX_RUNS) nt else 1;
    const layout = RunLayout{ .rg_stride = rg_stride, .memtable_run = memtable_run, .run_count = run_count };

    // counts[t][r]: thread t's rows in run r — then, in place, the offset
    // its scatter starts at (the run's start plus the earlier threads' rows).
    const counts = try allocator.alloc(u32, nt_runs * run_count);
    defer allocator.free(counts);
    @memset(counts, 0);
    const histogram_pass = HistogramPass{ .locs = locs, .counts = counts, .layout = layout };
    parallel.forRanges(nt_runs, n, &histogram_pass, HistogramPass.run);
    const starts = try allocator.alloc(u32, run_count + 1);
    defer allocator.free(starts);
    starts[0] = 0;
    for (0..run_count) |r| {
        var acc: u32 = starts[r];
        for (0..nt_runs) |t| {
            const c = counts[t * run_count + r];
            counts[t * run_count + r] = acc;
            acc += c;
        }
        starts[r + 1] = acc;
    }

    const pairs = try allocator.alloc(RunEntry, n);
    defer allocator.free(pairs);
    const scatter_pass = ScatterPass{ .locs = locs, .fill = counts, .pairs = pairs, .layout = layout };
    parallel.forRanges(nt_runs, n, &scatter_pass, ScatterPass.run);

    // Worker scratch can't come from the caller's allocator (not thread-safe).
    const scratch_alloc = if (nt_runs == 1) allocator else std.heap.c_allocator;
    var scratch: [parallel.MAX_THREADS]RunScratch = undefined;
    for (scratch[0..nt_runs]) |*sc| sc.* = .{};
    defer for (scratch[0..nt_runs]) |*sc| sc.deinit(scratch_alloc);
    const run_pass = RunPass{ .pairs = pairs, .starts = starts, .order = order, .scratch = &scratch, .allocator = scratch_alloc };
    parallel.forJobs(nt_runs, run_count, &run_pass, RunPass.run);
    for (scratch[0..nt_runs]) |sc| if (sc.err) |e| return e;
}

const Extent = struct { max_seg: u64 = 0, max_rg: u64 = 0 };

const ExtentPass = struct {
    locs: []const i64,
    out: *[parallel.MAX_THREADS]Extent,

    fn run(p: *const ExtentPass, t: usize, lo: usize, hi: usize) void {
        var e = Extent{};
        for (p.locs[lo..hi]) |l| {
            const bits: u64 = @bitCast(l);
            const seg = (bits >> seg_shift) & seg_mask;
            if (seg == seg_sentinel) continue;
            e.max_seg = @max(e.max_seg, seg);
            e.max_rg = @max(e.max_rg, (bits >> rg_shift) & rg_mask);
        }
        p.out[t] = e;
    }
};

const RunLayout = struct { rg_stride: u64, memtable_run: u64, run_count: usize };

const HistogramPass = struct {
    locs: []const i64,
    counts: []u32,
    layout: RunLayout,

    fn run(p: *const HistogramPass, t: usize, lo: usize, hi: usize) void {
        const mine = p.counts[t * p.layout.run_count ..][0..p.layout.run_count];
        for (p.locs[lo..hi]) |l| mine[runOf(l, p.layout.rg_stride, p.layout.memtable_run)] += 1;
    }
};

const ScatterPass = struct {
    locs: []const i64,
    fill: []u32,
    pairs: []RunEntry,
    layout: RunLayout,

    fn run(p: *const ScatterPass, t: usize, lo: usize, hi: usize) void {
        const mine = p.fill[t * p.layout.run_count ..][0..p.layout.run_count];
        for (p.locs[lo..hi], lo..) |l, i| {
            const r = runOf(l, p.layout.rg_stride, p.layout.memtable_run);
            p.pairs[mine[r]] = .{ .key = keyOf(l), .idx = @intCast(i) };
            mine[r] += 1;
        }
    }
};

const RunScratch = struct {
    bitmap: std.ArrayListUnmanaged(u64) = .empty,
    idx_by_key: std.ArrayListUnmanaged(u32) = .empty,
    err: ?anyerror = null,

    fn deinit(self: *RunScratch, allocator: std.mem.Allocator) void {
        self.bitmap.deinit(allocator);
        self.idx_by_key.deinit(allocator);
    }
};

const RunPass = struct {
    pairs: []RunEntry,
    starts: []const u32,
    order: []u32,
    scratch: *[parallel.MAX_THREADS]RunScratch,
    allocator: std.mem.Allocator,

    fn run(p: *const RunPass, t: usize, r: usize) void {
        const sc = &p.scratch[t];
        if (sc.err != null) return;
        orderRun(p.allocator, sc, p.pairs[p.starts[r]..p.starts[r + 1]], p.order[p.starts[r]..p.starts[r + 1]]) catch |e| {
            sc.err = e;
        };
    }
};

fn orderRun(allocator: std.mem.Allocator, sc: *RunScratch, run: []RunEntry, out: []u32) !void {
    if (run.len <= 1) {
        for (run, out) |p, *o| o.* = p.idx;
        return;
    }
    var max_key: u64 = 0;
    for (run) |p| max_key = @max(max_key, p.key);
    const dense = max_key < DENSE_MAX_KEY and run.len >= (max_key + 1) / 64;
    if (dense and try bitmapOrder(allocator, &sc.bitmap, &sc.idx_by_key, run, max_key, out)) return;
    std.mem.sortUnstable(RunEntry, run, {}, RunEntry.less);
    for (run, out) |p, *o| o.* = p.idx;
}

const RunEntry = struct {
    key: u64,
    idx: u32,

    fn less(_: void, a: RunEntry, b: RunEntry) bool {
        return a.key < b.key;
    }
};

inline fn runOf(l: i64, rg_stride: u64, memtable_run: u64) usize {
    const bits: u64 = @bitCast(l);
    const seg = (bits >> seg_shift) & seg_mask;
    if (seg == seg_sentinel) return @intCast(memtable_run);
    return @intCast(seg * rg_stride + ((bits >> rg_shift) & rg_mask));
}

inline fn keyOf(l: i64) u64 {
    const bits: u64 = @bitCast(l);
    const seg = (bits >> seg_shift) & seg_mask;
    if (seg == seg_sentinel) return bits & ((@as(u64, 1) << seg_shift) - 1);
    return bits & offset_mask;
}

/// Order one run by key through a bitmap over the key range: set each
/// key's bit, then walk the words in order. Returns false (nothing written)
/// on a duplicate key — the bitmap can't hold two entries — so the caller
/// sorts that run instead.
fn bitmapOrder(
    allocator: std.mem.Allocator,
    bitmap: *std.ArrayListUnmanaged(u64),
    idx_by_key: *std.ArrayListUnmanaged(u32),
    run: []const RunEntry,
    max_key: u64,
    out: []u32,
) !bool {
    const words: usize = @intCast(max_key / 64 + 1);
    try bitmap.resize(allocator, words);
    @memset(bitmap.items, 0);
    try idx_by_key.resize(allocator, @intCast(max_key + 1));
    for (run) |p| {
        const w: usize = @intCast(p.key >> 6);
        const bit = @as(u64, 1) << @intCast(p.key & 63);
        if (bitmap.items[w] & bit != 0) return false;
        bitmap.items[w] |= bit;
        idx_by_key.items[@intCast(p.key)] = p.idx;
    }
    var pos: usize = 0;
    for (bitmap.items, 0..) |word, w| {
        var m = word;
        while (m != 0) : (m &= m - 1) {
            out[pos] = idx_by_key.items[w * 64 + @ctz(m)];
            pos += 1;
        }
    }
    std.debug.assert(pos == out.len);
    return true;
}

fn comparisonOrder(locs: []const i64, order: []u32) void {
    for (order, 0..) |*o, i| o.* = @intCast(i);
    std.mem.sortUnstable(u32, order, locs, struct {
        fn less(ls: []const i64, a: u32, b: u32) bool {
            return ls[a] < ls[b];
        }
    }.less);
}

test "sortedOrder matches the comparison order across dense, sparse, duplicate and memtable runs" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const rand = prng.random();
    const n: usize = 20_000;
    const locs = try allocator.alloc(i64, n);
    defer allocator.free(locs);
    for (locs, 0..) |*l, i| {
        l.* = switch (i % 5) {
            // dense run: most offsets of one row group present (some twice)
            0, 1 => packSegment(0, 0, rand.uintLessThan(usize, 4096)),
            // sparse run: a few rows scattered over a wide offset range
            2 => packSegment(1, rand.uintLessThan(usize, 3), rand.uintLessThan(usize, 1 << 20)),
            // a later segment, later row group
            3 => packSegment(2, 7, rand.uintLessThan(usize, 2048)),
            else => packMemtable(rand.uintLessThan(usize, 5000)),
        };
    }
    const order = try allocator.alloc(u32, n);
    defer allocator.free(order);
    try sortedOrder(allocator, locs, order);

    const seen = try allocator.alloc(bool, n);
    defer allocator.free(seen);
    @memset(seen, false);
    var prev: i64 = std.math.minInt(i64);
    for (order) |idx| {
        try std.testing.expect(!seen[idx]);
        seen[idx] = true;
        try std.testing.expect(locs[idx] >= prev);
        prev = locs[idx];
    }
    // The small-input path is the comparison sort itself.
    const small = locs[0..100];
    const small_order = try allocator.alloc(u32, small.len);
    defer allocator.free(small_order);
    try sortedOrder(allocator, small, small_order);
    prev = std.math.minInt(i64);
    for (small_order) |idx| {
        try std.testing.expect(small[idx] >= prev);
        prev = small[idx];
    }
}

test "sortedOrderOn stripes the passes over threads and matches the serial permutation" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0xace);
    const rand = prng.random();
    const n: usize = STRIPE_MIN_ROWS + 12_345;
    const locs = try allocator.alloc(i64, n);
    defer allocator.free(locs);
    for (locs, 0..) |*l, i| {
        l.* = switch (i % 4) {
            // dense runs with duplicates (the bitmap declines, the run sorts)
            0, 1 => packSegment(rand.uintLessThan(usize, 3), rand.uintLessThan(usize, 4), rand.uintLessThan(usize, 40_000)),
            // one sparse run over a wide offset range
            2 => packSegment(5, 2, rand.uintLessThan(usize, 1 << 20)),
            else => packMemtable(rand.uintLessThan(usize, 9000)),
        };
    }
    const serial = try allocator.alloc(u32, n);
    defer allocator.free(serial);
    try sortedOrder(allocator, locs, serial);
    const striped = try allocator.alloc(u32, n);
    defer allocator.free(striped);
    try sortedOrderOn(allocator, locs, striped, 4);
    try std.testing.expectEqualSlices(u32, serial, striped);
    var prev: i64 = std.math.minInt(i64);
    for (striped) |idx| {
        try std.testing.expect(locs[idx] >= prev);
        prev = locs[idx];
    }
}
