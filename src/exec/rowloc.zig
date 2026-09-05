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
    std.debug.assert(order.len == locs.len);
    const n = locs.len;
    if (n < COUNTING_MIN_ROWS) return comparisonOrder(locs, order);

    var max_seg: u64 = 0;
    var max_rg: u64 = 0;
    for (locs) |l| {
        const bits: u64 = @bitCast(l);
        const seg = (bits >> seg_shift) & seg_mask;
        if (seg == seg_sentinel) continue;
        const rg = (bits >> rg_shift) & rg_mask;
        if (seg > max_seg) max_seg = seg;
        if (rg > max_rg) max_rg = rg;
    }
    const rg_stride = max_rg + 1;
    const memtable_run = (max_seg + 1) * rg_stride;
    if (memtable_run > COUNTING_MAX_RUNS) return comparisonOrder(locs, order);
    const run_count: usize = @intCast(memtable_run + 1);

    const starts = try allocator.alloc(u32, run_count + 1);
    defer allocator.free(starts);
    @memset(starts, 0);
    for (locs) |l| starts[runOf(l, rg_stride, memtable_run) + 1] += 1;
    for (1..starts.len) |k| starts[k] += starts[k - 1];

    const pairs = try allocator.alloc(RunEntry, n);
    defer allocator.free(pairs);
    const fill = try allocator.alloc(u32, run_count);
    defer allocator.free(fill);
    @memcpy(fill, starts[0..run_count]);
    for (locs, 0..) |l, i| {
        const r = runOf(l, rg_stride, memtable_run);
        pairs[fill[r]] = .{ .key = keyOf(l), .idx = @intCast(i) };
        fill[r] += 1;
    }

    var bitmap: std.ArrayListUnmanaged(u64) = .empty;
    defer bitmap.deinit(allocator);
    var idx_by_key: std.ArrayListUnmanaged(u32) = .empty;
    defer idx_by_key.deinit(allocator);
    for (0..run_count) |r| {
        const run = pairs[starts[r]..starts[r + 1]];
        const out = order[starts[r]..starts[r + 1]];
        if (run.len <= 1) {
            for (run, out) |p, *o| o.* = p.idx;
            continue;
        }
        var max_key: u64 = 0;
        for (run) |p| max_key = @max(max_key, p.key);
        const dense = max_key < DENSE_MAX_KEY and run.len >= (max_key + 1) / 64;
        if (dense and try bitmapOrder(allocator, &bitmap, &idx_by_key, run, max_key, out)) continue;
        std.mem.sortUnstable(RunEntry, run, {}, RunEntry.less);
        for (run, out) |p, *o| o.* = p.idx;
    }
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
