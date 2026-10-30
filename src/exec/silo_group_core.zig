//! Silo grouped-aggregation core: the parallel range-scan -> hash/radix
//! partition -> bucket-owned GROUP BY -> top-N machinery behind the V2
//! grouped engines (v2_group_topn_engine, v2_global_aggregate,
//! v2_lowcard_group, v2_shape_group_topn, zonemap_topn). Each worker owns
//! its partition's groups outright ("silo"), so the hot loops run without
//! sharing or locks; per-chunk validity constants monomorphize the inner
//! loops. Born as a ClickBench Q32 harness and promoted to the production
//! path — the workspace/profile plumbing doubles as the gbmicro bench
//! surface (bench/clientip_pipeline.zig).

const std = @import("std");
const platform = @import("../util/platform.zig");
const exec_mod = @import("exec.zig");
const api_mod = @import("../api/api.zig");
const storage_mod = @import("../storage/storage.zig");
const types_mod = @import("../types.zig");
const rowloc = @import("rowloc.zig");
const json_binary = @import("json_binary.zig");
const core_scheduler = @import("../util/core_scheduler.zig");
const StringBank = @import("../util/string_bank.zig").StringBank;
const build_options = @import("build_options");
const udf_mod = @import("../udf.zig");
const ColumnView = storage_mod.ColumnView;
const ColumnStore = @import("../engine/store.zig").ColumnStore;
const RowGroupTiles = @import("rg_tiles.zig").RowGroupTiles;
const CountSketch = @import("count_topk_sketch.zig").CountSketch;
const appendMaskedColumn = @import("../engine/transform.zig").appendMaskedColumn;

// Comptime master switch for the developer execution-trace profilers. False in
// every production build (default), which makes every `PROFILING and …` gate
// below comptime-false so the trace code — branches, prints, and globals — is
// never emitted. Flip on with `-Dprofiling=true`.
const PROFILING = build_options.profiling;

const thindb = struct {
    pub const exec = exec_mod;
    pub const api = api_mod;
    pub const storage = storage_mod;
    pub const types = types_mod;
    pub const Batch = exec_mod.Batch;
    pub const Predicate = exec_mod.Predicate;
};

const Allocator = std.mem.Allocator;
const StringView = storage_mod.StringView;
const Scan = thindb.exec.Scan;
const group_table = thindb.exec.group_table;
const GroupTable = group_table.IntKeyMemsetTable(96);
const WideGroupTable = group_table.IntKeyMemsetTable(128);

/// A bucket's group table, at the slot width its keys need. A key wider than
/// 96 bits, packed or a hashed key's digest, compares on all 128.
const BucketTable = union(enum) {
    narrow: GroupTable,
    wide: WideGroupTable,

    /// Switches an empty table to the width `layout` needs. A bucket's layout
    /// changes only between queries, after a reset empties its table.
    fn fit(self: *BucketTable, allocator: Allocator, layout: GroupRowsLayout, expected_groups: usize) !void {
        const wide = layout.key_width == .u128;
        if (wide == (self.* == .wide)) return;
        if (self.len() != 0) return error.UnsupportedOperatorForType;
        const initial_groups: usize = @min(expected_groups, PIPE_CHUNK_ROWS);
        const next: BucketTable = if (wide)
            .{ .wide = try WideGroupTable.init(allocator, initial_groups) }
        else
            .{ .narrow = try GroupTable.init(allocator, initial_groups) };
        self.deinit(allocator);
        self.* = next;
    }

    /// Grows ahead of `n` more rows toward the bucket's observed group
    /// reservation; true when it grew.
    fn growFor(self: *BucketTable, allocator: Allocator, n: usize, expected_groups: usize, input_rows: u64, observed_rows: u64) !bool {
        switch (self.*) {
            inline else => |*t| {
                if (!t.needsGrow(n)) return false;
                t.grow_target = group_table.capacityFor(group_table.observed_group_reservation(expected_groups, input_rows, t.len, observed_rows, n));
                try t.grow(allocator, n);
                return true;
            },
        }
    }

    /// The gid of `key`'s group, if it has one.
    fn find(self: *BucketTable, key: u128) ?u32 {
        switch (self.*) {
            inline else => |*t| {
                const probe = t.getOrPut(@TypeOf(t.*).hashKey(key), key);
                return if (probe.found) probe.gid else null;
            },
        }
    }

    fn len(self: BucketTable) usize {
        return switch (self) {
            inline else => |t| t.len,
        };
    }

    fn slotCount(self: BucketTable) usize {
        return switch (self) {
            inline else => |t| t.slots.len,
        };
    }

    fn slotBytes(self: BucketTable) usize {
        return switch (self) {
            inline else => |t| std.mem.sliceAsBytes(t.slots).len,
        };
    }

    fn clearRetainingCapacity(self: *BucketTable) void {
        switch (self.*) {
            inline else => |*t| t.clearRetainingCapacity(),
        }
    }

    /// Empties the table for a fold of `rows` rows. A fold reserves a slot per
    /// row, so a part of a few heavy groups grows the table to its row count;
    /// a table past twice what `rows` need is replaced by one sized for them,
    /// so emptying never costs more than the rows it serves.
    fn resetFor(self: *BucketTable, allocator: Allocator, rows: usize) !void {
        const groups = @max(rows, DEFER_PART_ROWS);
        if (self.slotCount() <= 2 * group_table.capacityFor(groups)) return self.clearRetainingCapacity();
        const next: BucketTable = switch (self.*) {
            .narrow => .{ .narrow = try GroupTable.init(allocator, groups) },
            .wide => .{ .wide = try WideGroupTable.init(allocator, groups) },
        };
        self.deinit(allocator);
        self.* = next;
    }

    fn deinit(self: *BucketTable, allocator: Allocator) void {
        switch (self.*) {
            inline else => |*t| t.deinit(allocator),
        }
    }
};
const TOP_K: usize = 10;

test {
    std.testing.refAllDecls(PipeBucket);
}

test "a part table a heavy part grew shrinks back before a small part's fold" {
    const allocator = std.testing.allocator;
    var table: BucketTable = .{ .wide = try WideGroupTable.init(allocator, DEFER_PART_ROWS) };
    defer table.deinit(allocator);
    const part_slots = table.slotCount();

    _ = try table.growFor(allocator, 100_000, 100_000, 100_000, 0);
    const heavy_slots = table.slotCount();
    try std.testing.expect(heavy_slots >= 100_000);
    try table.resetFor(allocator, 100_000);
    try std.testing.expectEqual(heavy_slots, table.slotCount());

    try table.resetFor(allocator, 100);
    try std.testing.expectEqual(part_slots, table.slotCount());
    try std.testing.expectEqual(@as(usize, 0), table.len());
    try std.testing.expect(table == .wide);
}

/// Selective-query right-sizing: one grid worker per this many surviving
/// (post-zone-map) row groups, so a filter that touches a handful of row
/// groups doesn't pay full-DOP worker setup (scans, staging, bucket scratch).
const RGS_PER_GRID_WORKER: usize = 2;
const PREFETCH_DIST_BUCKET: usize = 32;
// A table smaller than this stays in the core's cache, so a look-ahead
// prefetch would cost a second key hash per row and hide no miss.
const PREFETCH_MIN_TABLE_BYTES: usize = 256 * 1024;

inline fn prefetchesSlots(table: anytype) bool {
    return std.mem.sliceAsBytes(table.slots).len >= PREFETCH_MIN_TABLE_BYTES;
}
// Look-ahead distance for the grouped COUNT(DISTINCT) fold: the second pass
// prefetches the membership-set slot this many rows ahead of the insert, so the
// (cache-miss-bound) probe latency of independent rows overlaps.
const PREFETCH_DIST_DISTINCT: usize = 24;
const PIPE_CHUNK_ROWS: usize = 8192;
const GRID_CHUNK_ROWS: usize = 1024;
const GRID_SCAN_TILE_RGS: usize = 16;
const GRID_SCAN_COALESCE_TILES: usize = 1;
const GRID_SCAN_YIELD_CHUNKS: usize = 16384;
// An idle grid worker spins, then yields, then parks on `PipeShared.work_epoch`.
// Parking bounds an idle worker's CPU to its wakeups; spinning burned a core
// per worker for the whole statement. The timeout only bounds how long a
// parked worker takes to notice a cancel.
const GRID_IDLE_SPINS: usize = 256;
const GRID_IDLE_YIELDS: usize = 8;
// A bucket expected to hold this many groups outgrows the core's cache, so
// folding rows as they arrive takes a miss per row on its table and states.
// Such a grid stages every row before folding any; each bucket then folds at
// once, split by key hash into parts whose tables stay cache-resident.
const DEFER_FOLD_MIN_GROUPS: usize = 16 * 1024;
const DEFER_PART_ROWS: usize = 2048;
const MAX_DEFER_PARTS: usize = 256;
// Staging every row must fit in this share of the memory budget.
const DEFER_FOLD_BUDGET_DIVISOR: usize = 4;
const GRID_PARK_TIMEOUT_NS: u64 = std.time.ns_per_ms;
const DEFAULT_ROUTE_BLOCK_ROWS: usize = 2048;
const MAX_ROUTE_BLOCK_ROWS: usize = 2048;
const AUTO_ROUTE_BLOCK_ROWS: usize = 0;
const MAX_WORKSPACE_PROFILE_WORKERS: usize = 128;
const MAX_RAW_BATCH_CHUNKS: usize = 64;
const MAX_GENERIC_GROUP_KEYS: usize = 8;
const MAX_STRING_RECYCLE_BYTES: usize = 2 * 1024 * 1024 * 1024;

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

// Chunk-flow profiler (THINDB_V2_CHUNK_PROFILE): quantifies what the staging
// layer actually pushes through the queues per scan→group hop — row counts, the
// numeric slab volume, and the variable-length string payload (string MIN/MAX).
// A string MIN over a wide column materializes the whole column into these
// chunks; this makes that visible. Counters are process-wide and reset at the
// start of each `runSiloGrid`.
var g_cp_on: bool = false;
var g_cp_chunks: u64 = 0;
var g_cp_rows: u64 = 0;
var g_cp_str_bytes: u64 = 0;
var g_cp_slab_bytes: u64 = 0;
var g_cp_sampled: u32 = 0;

fn chunkProfileReset() void {
    g_cp_chunks = 0;
    g_cp_rows = 0;
    g_cp_str_bytes = 0;
    g_cp_slab_bytes = 0;
    g_cp_sampled = 0;
}

fn chunkProfileRecord(rows: RawRows) void {
    if (comptime !PROFILING) return;
    if (!g_cp_on) return;
    const n = rows.len_rows;
    const str_bytes = rows.str.bytes.items.len;
    _ = @atomicRmw(u64, &g_cp_chunks, .Add, 1, .monotonic);
    _ = @atomicRmw(u64, &g_cp_rows, .Add, @intCast(n), .monotonic);
    _ = @atomicRmw(u64, &g_cp_str_bytes, .Add, @intCast(str_bytes), .monotonic);
    _ = @atomicRmw(u64, &g_cp_slab_bytes, .Add, @intCast(rows.slab.len), .monotonic);
    // One-shot dump of a representative chunk's shape + a few sample strings.
    if (@cmpxchgStrong(u32, &g_cp_sampled, 0, 1, .acq_rel, .monotonic) == null) {
        const k = rows.layout.str_columns.len;
        std.debug.print("[chunk-sample] rows={d} slab_bytes={d} str_cols={d} str_payload_bytes={d} avg_str_bytes_per_row={d:.1}\n", .{
            n, rows.slab.len, k, str_bytes, if (n > 0) @as(f64, @floatFromInt(str_bytes)) / @as(f64, @floatFromInt(n)) else 0,
        });
        if (k > 0 and n > 0) {
            const sample_rows = @min(n, @as(usize, 3));
            var r: usize = 0;
            while (r < sample_rows) : (r += 1) {
                const v = rows.str.get(k, r, 0);
                const show = v[0..@min(v.len, @as(usize, 60))];
                std.debug.print("[chunk-sample]   row{d} str0.len={d} \"{s}\"\n", .{ r, v.len, show });
            }
        }
    }
}

fn chunkProfileDump() void {
    if (comptime !PROFILING) return;
    if (!g_cp_on) return;
    const chunks = @atomicLoad(u64, &g_cp_chunks, .monotonic);
    const rows = @atomicLoad(u64, &g_cp_rows, .monotonic);
    const str_bytes = @atomicLoad(u64, &g_cp_str_bytes, .monotonic);
    const slab_bytes = @atomicLoad(u64, &g_cp_slab_bytes, .monotonic);
    const mb: f64 = 1024.0 * 1024.0;
    std.debug.print("[chunk-profile] chunks={d} rows={d} str_payload_MB={d:.1} slab_MB={d:.1} avg_rows_per_chunk={d:.0} avg_str_bytes_per_row={d:.1}\n", .{
        chunks,
        rows,
        @as(f64, @floatFromInt(str_bytes)) / mb,
        @as(f64, @floatFromInt(slab_bytes)) / mb,
        if (chunks > 0) @as(f64, @floatFromInt(rows)) / @as(f64, @floatFromInt(chunks)) else 0,
        if (rows > 0) @as(f64, @floatFromInt(str_bytes)) / @as(f64, @floatFromInt(rows)) else 0,
    });
}

const Coord = struct { seg: usize, rg: usize };
const ScanTile = struct { lo: usize, hi: usize };

fn flatToCoord(f: usize, seg_start: []const usize, segment_count: usize, total: usize) Coord {
    if (f >= total) return .{ .seg = segment_count, .rg = 0 };
    var seg: usize = 0;
    while (seg + 1 < segment_count and seg_start[seg + 1] <= f) seg += 1;
    return .{ .seg = seg, .rg = f - seg_start[seg] };
}

const RawRows = struct {
    slab: []align(16) u8 = &.{},
    len_rows: usize = 0,
    capacity_rows: usize = 0,
    layout: GroupRowsLayout = .{},
    // Variable-length string payload (string MIN/MAX only). Empty/unallocated
    // unless `layout.has_str_payload`, so numeric staging is unaffected.
    str: StrStore = .{},

    inline fn len(self: RawRows) usize {
        return self.len_rows;
    }

    inline fn capacity(self: RawRows) usize {
        return self.capacity_rows;
    }

    fn slabBytes(layout: GroupRowsLayout, capacity_rows: usize) usize {
        var bytes: usize = 0;
        bytes = alignForward(bytes, groupKeyAlign(layout.key_width));
        bytes += groupKeySize(layout.key_width) * capacity_rows;
        for (layout.columns) |column| {
            bytes = alignForward(bytes, groupColumnAlign(column.physical_type));
            bytes += groupColumnSize(column.physical_type) * capacity_rows;
        }
        if (layout.has_rowref) {
            bytes = alignForward(bytes, @alignOf(i64));
            bytes += @sizeOf(i64) * capacity_rows;
        }
        if (layout.has_weight) {
            bytes = alignForward(bytes, @alignOf(u32));
            bytes += @sizeOf(u32) * capacity_rows;
        }
        bytes += nullableInputCount(layout) * capacity_rows;
        return bytes;
    }

    inline fn keyOffset(_: RawRows) usize {
        return 0;
    }

    // Must mirror slabBytes exactly (see weightOffset).
    fn validOffset(self: RawRows) usize {
        var offset = groupKeySize(self.layout.key_width) * self.capacity_rows;
        for (self.layout.columns) |column| {
            offset = alignForward(offset, groupColumnAlign(column.physical_type));
            offset += groupColumnSize(column.physical_type) * self.capacity_rows;
        }
        if (self.layout.has_rowref) {
            offset = alignForward(offset, @alignOf(i64));
            offset += @sizeOf(i64) * self.capacity_rows;
        }
        if (self.layout.has_weight) {
            offset = alignForward(offset, @alignOf(u32));
            offset += @sizeOf(u32) * self.capacity_rows;
        }
        return offset;
    }

    inline fn validAll(self: RawRows, valid_index: usize) []u8 {
        return (self.slab.ptr + self.validOffset() + valid_index * self.capacity_rows)[0..self.capacity_rows];
    }

    fn rowrefOffset(self: RawRows) usize {
        var offset = groupKeySize(self.layout.key_width) * self.capacity_rows;
        for (self.layout.columns) |column| {
            offset = alignForward(offset, groupColumnAlign(column.physical_type));
            offset += groupColumnSize(column.physical_type) * self.capacity_rows;
        }
        return alignForward(offset, @alignOf(i64));
    }

    inline fn rowrefAll(self: RawRows) []i64 {
        std.debug.assert(self.layout.has_rowref);
        return @as([*]i64, @ptrCast(@alignCast(self.slab.ptr + self.rowrefOffset())))[0..self.capacity_rows];
    }

    // Must mirror slabBytes exactly: the rowref alignment step only happens
    // when the region exists.
    fn weightOffset(self: RawRows) usize {
        var offset = groupKeySize(self.layout.key_width) * self.capacity_rows;
        for (self.layout.columns) |column| {
            offset = alignForward(offset, groupColumnAlign(column.physical_type));
            offset += groupColumnSize(column.physical_type) * self.capacity_rows;
        }
        if (self.layout.has_rowref) {
            offset = alignForward(offset, @alignOf(i64));
            offset += @sizeOf(i64) * self.capacity_rows;
        }
        return alignForward(offset, @alignOf(u32));
    }

    inline fn weightAll(self: RawRows) []u32 {
        std.debug.assert(self.layout.has_weight);
        return @as([*]u32, @ptrCast(@alignCast(self.slab.ptr + self.weightOffset())))[0..self.capacity_rows];
    }

    fn columnOffset(self: RawRows, column_index: usize) usize {
        var offset = groupKeySize(self.layout.key_width) * self.capacity_rows;
        var i: usize = 0;
        while (i < column_index) : (i += 1) {
            offset = alignForward(offset, groupColumnAlign(self.layout.columns[i].physical_type));
            offset += groupColumnSize(self.layout.columns[i].physical_type) * self.capacity_rows;
        }
        return alignForward(offset, groupColumnAlign(self.layout.columns[column_index].physical_type));
    }

    inline fn keyU32All(self: RawRows) []u32 {
        std.debug.assert(self.layout.key_width == .u32);
        return @as([*]u32, @ptrCast(@alignCast(self.slab.ptr + self.keyOffset())))[0..self.capacity_rows];
    }

    inline fn keyU64All(self: RawRows) []u64 {
        std.debug.assert(self.layout.key_width == .u64);
        return @as([*]u64, @ptrCast(@alignCast(self.slab.ptr + self.keyOffset())))[0..self.capacity_rows];
    }

    inline fn keyU96LoAll(self: RawRows) []u64 {
        std.debug.assert(self.layout.key_width == .u96);
        return @as([*]u64, @ptrCast(@alignCast(self.slab.ptr + self.keyOffset())))[0..self.capacity_rows];
    }

    inline fn keyU96HiAll(self: RawRows) []u32 {
        std.debug.assert(self.layout.key_width == .u96);
        const offset = self.keyOffset() + self.capacity_rows * @sizeOf(u64);
        return @as([*]u32, @ptrCast(@alignCast(self.slab.ptr + offset)))[0..self.capacity_rows];
    }

    inline fn keyU128All(self: RawRows) []u128 {
        std.debug.assert(self.layout.key_width == .u128);
        return @as([*]u128, @ptrCast(@alignCast(self.slab.ptr + self.keyOffset())))[0..self.capacity_rows];
    }

    inline fn columnByteSlab(self: RawRows, column_index: usize) []u8 {
        const sz = groupColumnSize(self.layout.columns[column_index].physical_type);
        return (self.slab.ptr + self.columnOffset(column_index))[0 .. sz * self.capacity_rows];
    }

    fn ensureTotalCapacity(self: *RawRows, allocator: Allocator, layout: GroupRowsLayout, capacity_rows: usize) !void {
        if (!sameRowsLayout(self.layout, layout)) {
            self.deinit(allocator);
            self.layout = layout;
        }
        if (capacity_rows <= self.capacity_rows) return;
        var next: RawRows = .{
            .slab = try allocator.alignedAlloc(u8, .@"16", slabBytes(layout, capacity_rows)),
            .len_rows = self.len_rows,
            .capacity_rows = capacity_rows,
            .layout = layout,
        };
        errdefer allocator.free(next.slab);
        if (self.len_rows != 0) {
            var i: usize = 0;
            while (i < self.len_rows) : (i += 1) next.setKey(i, self.keyAt(i));
            var col: usize = 0;
            while (col < layout.columns.len) : (col += 1) {
                const sz = groupColumnSize(layout.columns[col].physical_type);
                @memcpy(next.columnByteSlab(col)[0 .. sz * self.len_rows], self.columnByteSlab(col)[0 .. sz * self.len_rows]);
            }
            if (layout.has_rowref) @memcpy(next.rowrefAll()[0..self.len_rows], self.rowrefAll()[0..self.len_rows]);
            if (layout.has_weight) @memcpy(next.weightAll()[0..self.len_rows], self.weightAll()[0..self.len_rows]);
            var vi: usize = 0;
            const nv = nullableInputCount(layout);
            while (vi < nv) : (vi += 1) @memcpy(next.validAll(vi)[0..self.len_rows], self.validAll(vi)[0..self.len_rows]);
        }
        // The string side store lives outside the slab; grow its ref array in
        // place (preserving the live prefix), then move it into `next`.
        if (layout.has_str_payload) try self.str.ensure(allocator, layout.str_columns.len, capacity_rows, self.len_rows);
        next.str = self.str;
        allocator.free(self.slab);
        self.* = next;
    }

    fn resize(self: *RawRows, allocator: Allocator, layout: GroupRowsLayout, new_len: usize) !void {
        try self.ensureTotalCapacity(allocator, layout, new_len);
        self.len_rows = new_len;
    }

    fn clearRetainingCapacity(self: *RawRows) void {
        self.len_rows = 0;
        self.str.clear();
    }

    fn deinit(self: *RawRows, allocator: Allocator) void {
        self.str.deinit(allocator);
        allocator.free(self.slab);
        self.* = .{};
    }

    inline fn keyAt(self: RawRows, idx: usize) u128 {
        return switch (self.layout.key_width) {
            .u32 => @as(u128, self.keyU32All()[idx]),
            .u64 => @as(u128, self.keyU64All()[idx]),
            .u96 => @as(u128, self.keyU96LoAll()[idx]) | (@as(u128, self.keyU96HiAll()[idx]) << 64),
            .u128 => self.keyU128All()[idx],
        };
    }

    inline fn setKey(self: RawRows, idx: usize, key: u128) void {
        switch (self.layout.key_width) {
            .u32 => self.keyU32All()[idx] = @truncate(key),
            .u64 => self.keyU64All()[idx] = @truncate(key),
            .u96 => {
                self.keyU96LoAll()[idx] = @truncate(key);
                self.keyU96HiAll()[idx] = @truncate(key >> 64);
            },
            .u128 => self.keyU128All()[idx] = key,
        }
    }

    inline fn copyKeyFrom(dst: RawRows, comptime key_width: GroupKeyWidth, dst_idx: usize, src: RawRows, src_idx: usize) void {
        switch (key_width) {
            .u32 => dst.keyU32All()[dst_idx] = src.keyU32All()[src_idx],
            .u64 => dst.keyU64All()[dst_idx] = src.keyU64All()[src_idx],
            .u96 => {
                dst.keyU96LoAll()[dst_idx] = src.keyU96LoAll()[src_idx];
                dst.keyU96HiAll()[dst_idx] = src.keyU96HiAll()[src_idx];
            },
            .u128 => dst.keyU128All()[dst_idx] = src.keyU128All()[src_idx],
        }
    }
};

pub const GroupKeyWidth = enum {
    u32,
    u64,
    u96,
    u128,
};

pub const GroupColumnType = enum {
    i8,
    i16,
    i32,
    i64,
    f32,
    f64,
};

pub const GroupColumnSource = enum {
    is_refresh,
    resolution_width,
};

pub const GroupColumnSpec = struct {
    physical_type: GroupColumnType,
    source: GroupColumnSource = .is_refresh,
    source_name: []const u8 = "",
    // Nullable agg-input columns carry a per-row validity byte in the staged
    // slab's validity region (one byte-per-row lane per nullable column, after
    // the weight region). `valid_index` is this column's lane ordinal.
    nullable: bool = false,
    valid_index: u16 = 0,
};

pub const GroupKeyColumnSpec = struct {
    name: []const u8,
    typ: thindb.types.Type,
    offset_bits: u8,
    width_bits: u8,
    // Nullable key columns carry a validity bit as the TOP bit of their packed
    // slice (width_bits includes it: payload bits + 1). A NULL row contributes
    // all-zero bits — distinct from any valid value, whose validity bit is set.
    // In the hashed lane the validity tag is mixed into the digest instead and
    // a NULL row's payload is skipped (its decoded bytes are an encoding
    // artifact that may differ across batch sources).
    nullable: bool = false,
};

pub const GroupAggregateOp = enum {
    count_star,
    count_col,
    sum,
    avg,
    min,
    max,
    count_distinct,
    // Variance/stddev family (all four SQL variants share one fold): Welford's
    // online algorithm over THREE consecutive slots — non-null count (i64) at
    // `state_index`, running mean (f64 bits) at +1, M2 (f64 bits) at +2. The
    // emit layer picks the pop/samp divisor and the optional sqrt.
    welford,
    // GROUP_CONCAT — folded into the side concat_states (see is_concat).
    concat,
    // User-defined aggregate — opaque per-group state in the side udf_states
    // lane (see is_udf). Folded by the registry callbacks over reconstructed
    // arg ColumnViews; the finalized scalar lands in the numeric value slot.
    udf,
};

// A grouped query may carry up to this many UDAF aggregates; each gets one
// gid-indexed opaque-state cell per bucket. 0/unused for the common case.
pub const MAX_GROUP_UDF_SLOTS: usize = 4;
// Maximum numeric arg columns a silo-hosted UDAF accepts (its args stage as
// ordinary numeric aggregate-input columns). String/exotic-arg UDAFs decline
// to the UdfAggregate operator instead.
pub const MAX_GROUP_UDF_ARGS: usize = 8;

pub const GroupAggregateSpec = struct {
    op: GroupAggregateOp,
    input_column_index: ?u16 = null,
    state_index: u16,
    // String MIN/MAX: reads `layout.str_columns[str_input_index]` and keeps its
    // running extreme in the parallel string-state slot `str_state_index` (not
    // the numeric `slots[]`). `is_string` is false for every numeric aggregate.
    is_string: bool = false,
    // String MIN/MAX over JSON, which orders by MySQL's JSON rules.
    is_json: bool = false,
    str_input_index: u16 = 0,
    str_state_index: u16 = 0,
    // COUNT(DISTINCT col): reads the carried integer payload `input_column_index`
    // and tracks membership in the per-bucket combined set `distinct_state_index`
    // (keyed by (gid,value)); `state_index` is the running distinct-count slot,
    // bumped only on a never-before-seen (gid,value). `is_distinct` is false for
    // every other aggregate.
    is_distinct: bool = false,
    distinct_state_index: u16 = 0,
    // COUNT(DISTINCT) over a string input: reads `str_columns[str_input_index]`
    // instead of a numeric payload and keeps membership in the exact
    // (gid, bytes) set `DistinctSlots.strs[distinct_state_index]`.
    distinct_str: bool = false,
    // SUM/AVG over a 64-bit integer input accumulates in i128: the aggregate
    // owns TWO consecutive slots — lo u64 bits at `state_index - 1`, hi i64
    // at `state_index`. The fused count/sum/avg and weight-fold kernels
    // decline it.
    wide: bool = false,
    // The input column is nullable: NULL rows are skipped in the fold (a
    // nullable COUNT(col) accumulates its own slot instead of mirroring the
    // group row count). `valid_count_index` (0 = none) is the companion slot
    // holding the group's non-null input count — the AVG denominator and the
    // all-NULL → NULL finalize signal for SUM/AVG/MIN/MAX.
    nullable: bool = false,
    valid_count_index: u16 = 0,
    // GROUP_CONCAT: reads `str_columns[str_input_index]` (like string MIN/MAX)
    // and collects (rowloc, bytes) items in side cell `concat_state_index`;
    // the emit sorts + joins with `separator` into the TopRow.str slot
    // `str_state_index`. No numeric slots; forces the rowref region.
    is_concat: bool = false,
    concat_state_index: u16 = 0,
    separator: []const u8 = ",",
    // User-defined aggregate. The opaque per-group state lives in the bucket's
    // side `udf_states` lane at `udf_state_index`; the fold reconstructs
    // ColumnViews over the staged arg columns `udf_arg_input_indices[0..
    // udf_arg_count]` and drives `udf_entry`'s callbacks. The finalized scalar
    // lands in numeric value slot `state_index` (slot `state_index - 1`); its
    // presence (non-NULL) rides the companion slot at `state_index` (one past
    // the value), so a UDAF reserves two numeric slots. `udf_entry` is null for
    // every non-UDAF aggregate.
    is_udf: bool = false,
    udf_entry: ?udf_mod.AggregateEntry = null,
    udf_state_index: u16 = 0,
    udf_arg_count: u8 = 0,
    udf_arg_input_indices: [MAX_GROUP_UDF_ARGS]u16 = [_]u16{0} ** MAX_GROUP_UDF_ARGS,
};

// Per-group numeric accumulators are stored in a runtime variable-stride slab
// (StateSlab) sized to exactly the aggregate program's slot count, so the
// per-group memory never carries unused slots and a wide-aggregate query is not
// declined. MAX_GROUP_AGG_SLOTS is only the inline ceiling on the transient
// result row (TopRow) and the validation cap — generous, not the per-group cost.
const MAX_GROUP_AGG_SLOTS: usize = 16;
const MAX_GROUP_AGG_STATES: usize = MAX_GROUP_AGG_SLOTS + 1;
const MAX_GROUP_PAYLOAD_COLUMNS: usize = 16;

// String MIN/MAX support. A query may carry up to this many distinct string
// agg-input columns (Q23 needs 2: MIN(URL), MIN(Title)) and that many string
// aggregate slots. Both are 0/unused for the numeric-only common case.
const MAX_GROUP_STR_COLUMNS: usize = 4;
const MAX_GROUP_STR_SLOTS: usize = 2;

// COUNT(DISTINCT) support. A query may carry up to this many distinct aggregates;
// each gets one combined per-bucket membership set. 0/unused for the common case.
const MAX_GROUP_DISTINCT_SLOTS: usize = 8;

// One COUNT(DISTINCT) field's combined membership set. Keys are pack(gid, value)
// (gid in the high 64 bits, the integer value's bit pattern in the low 64) —
// exact, so no value is stored beyond the u128 key and the distinct count falls
// out as the number of never-before-seen inserts. The grouped silo folds it under
// the bucket agg_lock (no synchronization needed); the serial global aggregate
// reuses it with a configured tier.
// COUNT(DISTINCT) membership set, tiered by key width over the lean
// open-addressing sets in `group_table`. The fold is cache-miss-bound on a
// multi-million entry table, so the slot must be no wider than the key actually
// needs: a narrow distinct (e.g. a 32-bit column) moves a quarter of the bytes
// per probe versus a full u128. Tiers:
//   .u32  — global distinct over a ≤32-bit value
//   .u64  — global distinct over a ≤64-bit value (int family, f64-bitcast float)
//   .u96  — grouped distinct: the composite `gid<<64 | value64` is exactly 96 bits
//   .u128 — global distinct over a 128-bit value / 128-bit string hash
// The grouped path never configures a tier, so the default is `.u96`; the global
// path calls `configure` per distinct field. All tiers are keys-only lean sets:
// open-addressing, cheap multiply-mix hash, insert-and-count only.
pub const DistinctSet = struct {
    pub const Tier = enum { u32, u64, u96, u128 };

    const Store = union(Tier) {
        u32: group_table.DistinctU32Set,
        u64: group_table.DistinctU64Set,
        u96: group_table.DistinctU96Set,
        u128: group_table.DistinctU128Set,
    };

    store: Store = .{ .u96 = group_table.DistinctU96Set.empty },

    pub inline fn key(gid: u32, value_bits: u64) u128 {
        return (@as(u128, gid) << 64) | @as(u128, value_bits);
    }

    // Select the key-width tier. Valid only while the set is empty (re-tiering a
    // populated set drops its slots). The grouped path relies on the `.u96`
    // default and never calls this.
    pub fn configure(self: *DistinctSet, tier: Tier) void {
        self.store = switch (tier) {
            .u32 => .{ .u32 = group_table.DistinctU32Set.empty },
            .u64 => .{ .u64 = group_table.DistinctU64Set.empty },
            .u96 => .{ .u96 = group_table.DistinctU96Set.empty },
            .u128 => .{ .u128 = group_table.DistinctU128Set.empty },
        };
    }

    // Insert `composite`, truncated to the tier's width (the caller picks a tier
    // wide enough that the truncation is lossless). Returns whether it was newly
    // added — the grouped fold bumps the owning group's counter on a first
    // sighting. `ensureFor(1)` keeps the load factor bounded (a cheap branch
    // when no grow is due) since these lean sets don't grow inside `insert`.
    pub fn insertIsNew(self: *DistinctSet, allocator: Allocator, composite: u128) !bool {
        switch (self.store) {
            .u32 => |*s| {
                try s.ensureFor(allocator, 1);
                return s.insertNew(@truncate(composite));
            },
            .u64 => |*s| {
                try s.ensureFor(allocator, 1);
                return s.insertNew(@truncate(composite));
            },
            .u96 => |*s| {
                try s.ensureFor(allocator, 1);
                return s.insertNew(group_table.Key96.fromU128(composite));
            },
            .u128 => |*s| {
                try s.ensureFor(allocator, 1);
                return s.insertNew(composite);
            },
        }
    }

    // Software-prefetch batch pipeline (the grouped distinct fold). Reserve once
    // with `ensureForBatch(n)` so no grow fires across the batch — keeping the
    // slot addresses prefetched by `prefetchKey(look-ahead)` valid until
    // `insertNewBatch(current)` probes them. Splits insertIsNew's per-call grow
    // check out of the hot loop so the cache-miss latency of independent probes
    // overlaps.
    pub fn ensureForBatch(self: *DistinctSet, allocator: Allocator, additional: usize) !void {
        switch (self.store) {
            inline else => |*s| try s.ensureFor(allocator, additional),
        }
    }

    pub inline fn prefetchKey(self: *DistinctSet, composite: u128) void {
        switch (self.store) {
            // The empty-set guard makes look-ahead prefetch safe against the
            // lazily-allocated per-element insert path (`slots.len - 1` would
            // wrap on an unallocated set).
            .u32 => |*s| if (s.slots.len != 0) s.prefetch(@truncate(composite)),
            .u64 => |*s| if (s.slots.len != 0) s.prefetch(@truncate(composite)),
            .u96 => |*s| if (s.slots.len != 0) s.prefetch(group_table.Key96.fromU128(composite)),
            .u128 => |*s| if (s.slots.len != 0) s.prefetch(composite),
        }
    }

    pub inline fn insertNewBatch(self: *DistinctSet, composite: u128) bool {
        switch (self.store) {
            .u32 => |*s| return s.insertNew(@truncate(composite)),
            .u64 => |*s| return s.insertNew(@truncate(composite)),
            .u96 => |*s| return s.insertNew(group_table.Key96.fromU128(composite)),
            .u128 => |*s| return s.insertNew(composite),
        }
    }

    pub fn count(self: *const DistinctSet) u64 {
        return switch (self.store) {
            inline else => |*s| @intCast(s.count()),
        };
    }

    // Reset for workspace reuse: release the (possibly large) table rather than
    // memset it, so a pooled bucket doesn't carry a multi-hundred-MB set into an
    // unrelated next query. Re-grows lazily on the next distinct query.
    pub fn clear(self: *DistinctSet, allocator: Allocator) void {
        switch (self.store) {
            inline else => |*s, tag| {
                s.deinit(allocator);
                self.store = @unionInit(Store, @tagName(tag), @TypeOf(s.*).empty);
            },
        }
    }

    pub fn deinit(self: *DistinctSet, allocator: Allocator) void {
        switch (self.store) {
            inline else => |*s| s.deinit(allocator),
        }
        self.* = .{};
    }
};

// Per-bucket COUNT(DISTINCT) membership, indexed by an aggregate's
// distinct_state_index: integer and float inputs key (gid, value bits) in
// `ints`, string inputs key (gid, bytes) in `strs`. An index uses one of them.
const DistinctSlots = struct {
    ints: [MAX_GROUP_DISTINCT_SLOTS]DistinctSet = [_]DistinctSet{.{}} ** MAX_GROUP_DISTINCT_SLOTS,
    strs: [MAX_GROUP_DISTINCT_SLOTS]group_table.DistinctStrSet = [_]group_table.DistinctStrSet{.{}} ** MAX_GROUP_DISTINCT_SLOTS,

    // Releases the sets rather than keeping capacity, like `DistinctSet.clear`.
    fn clear(self: *DistinctSlots, allocator: Allocator) void {
        for (&self.ints) |*d| d.clear(allocator);
        for (&self.strs) |*d| d.deinit(allocator);
    }

    fn deinit(self: *DistinctSlots, allocator: Allocator) void {
        for (&self.ints) |*d| d.deinit(allocator);
        for (&self.strs) |*d| d.deinit(allocator);
    }
};

pub const GroupStrColumnSpec = struct {
    source_name: []const u8,
};

const StrRef = struct { off: u32, len: u32 };

// Variable-length string payload carried alongside a fixed-width slab
// (RawRows / GroupRows). Used ONLY when a query has a string MIN/MAX aggregate
// (`layout.has_str_payload`); otherwise nothing here allocates or runs, so the
// numeric path is byte-for-byte unchanged. `refs` is row-major — row r, column
// c at `r*str_cols + c` — so the live prefix stays contiguous across capacity
// growth. `bytes` is the single growing value buffer the refs index into.
const StrStore = struct {
    refs: []StrRef = &.{},
    bytes: std.ArrayListUnmanaged(u8) = .empty,

    fn deinit(self: *StrStore, allocator: Allocator) void {
        if (self.refs.len > 0) allocator.free(self.refs);
        self.bytes.deinit(allocator);
        self.* = .{};
    }

    fn clear(self: *StrStore) void {
        self.bytes.clearRetainingCapacity();
    }

    fn ensure(self: *StrStore, allocator: Allocator, str_cols: usize, capacity_rows: usize, len_rows: usize) !void {
        const need = str_cols * capacity_rows;
        if (self.refs.len >= need) return;
        const next = try allocator.alloc(StrRef, need);
        if (len_rows > 0) @memcpy(next[0 .. str_cols * len_rows], self.refs[0 .. str_cols * len_rows]);
        if (self.refs.len > 0) allocator.free(self.refs);
        self.refs = next;
    }

    // A NULL string value is a sentinel ref (no bytes appended): distinct from
    // a valid empty string, whose ref has a real offset and len 0.
    const NULL_OFF: u32 = std.math.maxInt(u32);

    fn append(self: *StrStore, allocator: Allocator, str_cols: usize, row: usize, col: usize, b: []const u8) !void {
        const off: u32 = @intCast(self.bytes.items.len);
        try self.bytes.appendSlice(allocator, b);
        self.refs[row * str_cols + col] = .{ .off = off, .len = @intCast(b.len) };
    }

    fn appendNull(self: *StrStore, str_cols: usize, row: usize, col: usize) void {
        self.refs[row * str_cols + col] = .{ .off = NULL_OFF, .len = 0 };
    }

    fn isNull(self: StrStore, str_cols: usize, row: usize, col: usize) bool {
        return self.refs[row * str_cols + col].off == NULL_OFF;
    }

    fn get(self: StrStore, str_cols: usize, row: usize, col: usize) []const u8 {
        const ref = self.refs[row * str_cols + col];
        if (ref.off == NULL_OFF) return &.{};
        return self.bytes.items[ref.off..][0..ref.len];
    }
};

const DEFAULT_GROUP_AGGREGATES = [_]GroupAggregateSpec{
    .{ .op = .count_star, .input_column_index = null, .state_index = 0 },
    .{ .op = .sum, .input_column_index = 0, .state_index = 1 },
    .{ .op = .avg, .input_column_index = 1, .state_index = 2 },
};

const CountSumAvgProgram = struct {
    sum_input_index: usize,
    avg_input_index: usize,
};

pub const GroupRowsLayout = struct {
    key_width: GroupKeyWidth = .u128,
    key_columns: []const GroupKeyColumnSpec = &.{},
    columns: []const GroupColumnSpec = &DEFAULT_GROUP_COLUMNS,
    aggregates: []const GroupAggregateSpec = &DEFAULT_GROUP_AGGREGATES,
    // String agg-input columns (for string MIN/MAX), carried as variable-length
    // values in the slab structs' side `StrStore`. Empty for numeric-only
    // queries, in which case `has_str_payload` is false and no string path runs.
    str_columns: []const GroupStrColumnSpec = &.{},
    has_str_payload: bool = false,
    // COUNT(DISTINCT): number of distinct membership sets the query needs (one per
    // distinct aggregate). 0 for the common case, in which case no distinct path
    // runs and the buckets allocate no sets.
    distinct_slot_count: u16 = 0,
    // Each staged row also carries the source row's packed __rowloc (see
    // rowloc.zig) in a dedicated i64 region after the payload columns. Set
    // for hashed keys (captured into the group State on first insert so the
    // key values can be late-materialized at emit) and for GROUP_CONCAT
    // (per-item emission order). Dormant (region absent) otherwise.
    has_rowref: bool = false,
    // The staged key is a 128-bit hash of the key columns rather than a
    // lossless bit-pack (string / >128-bit keys). Implies has_rowref; the
    // reverse does not hold (concat carries rowrefs under packed keys).
    hashed_key: bool = false,
    // The program has GROUP_CONCAT aggregates: buckets maintain concat_states.
    has_concat: bool = false,
    // COUNT-only programs (no payload/str columns, no distinct): each staged
    // row carries a u32 weight, and the scan emitter collapses a run of
    // adjacent-equal keys into ONE weighted row — route/stage/lane traffic
    // shrinks by the run factor. The lane folds `count += weight`.
    has_weight: bool = false,
    // The program has UDAF aggregates: buckets maintain a gid-indexed
    // udf_states lane and the fold runs the per-row registry callbacks.
    has_udf: bool = false,

    const DEFAULT_GROUP_COLUMNS = [_]GroupColumnSpec{
        .{ .physical_type = .i16, .source = .is_refresh },
        .{ .physical_type = .i16, .source = .resolution_width },
    };
};

fn sameRowsLayout(a: GroupRowsLayout, b: GroupRowsLayout) bool {
    if (a.key_width != b.key_width or a.has_rowref != b.has_rowref or a.hashed_key != b.hashed_key or a.has_concat != b.has_concat or a.has_udf != b.has_udf or a.has_weight != b.has_weight or a.has_str_payload != b.has_str_payload or a.distinct_slot_count != b.distinct_slot_count or a.key_columns.len != b.key_columns.len or a.columns.len != b.columns.len or a.str_columns.len != b.str_columns.len or a.aggregates.len != b.aggregates.len) return false;
    var key_i: usize = 0;
    while (key_i < a.key_columns.len) : (key_i += 1) {
        if (!thindb.types.columnNameEql(a.key_columns[key_i].name, b.key_columns[key_i].name) or
            !std.meta.eql(a.key_columns[key_i].typ, b.key_columns[key_i].typ) or
            a.key_columns[key_i].offset_bits != b.key_columns[key_i].offset_bits or
            a.key_columns[key_i].width_bits != b.key_columns[key_i].width_bits or
            a.key_columns[key_i].nullable != b.key_columns[key_i].nullable)
        {
            return false;
        }
    }
    var i: usize = 0;
    while (i < a.columns.len) : (i += 1) {
        if (a.columns[i].physical_type != b.columns[i].physical_type or
            a.columns[i].source != b.columns[i].source or
            a.columns[i].nullable != b.columns[i].nullable or
            a.columns[i].valid_index != b.columns[i].valid_index or
            !std.mem.eql(u8, a.columns[i].source_name, b.columns[i].source_name))
        {
            return false;
        }
    }
    i = 0;
    while (i < a.aggregates.len) : (i += 1) {
        if (a.aggregates[i].op != b.aggregates[i].op or
            a.aggregates[i].input_column_index != b.aggregates[i].input_column_index or
            a.aggregates[i].state_index != b.aggregates[i].state_index or
            a.aggregates[i].is_string != b.aggregates[i].is_string or
            a.aggregates[i].is_json != b.aggregates[i].is_json or
            a.aggregates[i].str_input_index != b.aggregates[i].str_input_index or
            a.aggregates[i].str_state_index != b.aggregates[i].str_state_index or
            a.aggregates[i].is_distinct != b.aggregates[i].is_distinct or
            a.aggregates[i].distinct_str != b.aggregates[i].distinct_str or
            a.aggregates[i].distinct_state_index != b.aggregates[i].distinct_state_index or
            a.aggregates[i].wide != b.aggregates[i].wide or
            a.aggregates[i].nullable != b.aggregates[i].nullable or
            a.aggregates[i].valid_count_index != b.aggregates[i].valid_count_index)
        {
            return false;
        }
    }
    i = 0;
    while (i < a.str_columns.len) : (i += 1) {
        if (!std.mem.eql(u8, a.str_columns[i].source_name, b.str_columns[i].source_name)) return false;
    }
    return true;
}

fn alignForward(value: usize, alignment: usize) usize {
    std.debug.assert(alignment != 0);
    std.debug.assert((alignment & (alignment - 1)) == 0);
    return (value + alignment - 1) & ~(alignment - 1);
}

// Number of byte-per-row validity lanes the staged slab carries — one per
// nullable agg-input column. 0 for the common all-non-nullable case, in which
// case the validity region is absent and every layout/offset is unchanged.
fn nullableInputCount(layout: GroupRowsLayout) usize {
    var n: usize = 0;
    for (layout.columns) |c| {
        if (c.nullable) n += 1;
    }
    return n;
}

fn groupKeySize(width: GroupKeyWidth) usize {
    return switch (width) {
        .u32 => @sizeOf(u32),
        .u64 => @sizeOf(u64),
        .u96 => @sizeOf(u64) + @sizeOf(u32),
        .u128 => @sizeOf(u128),
    };
}

fn groupKeyAlign(width: GroupKeyWidth) usize {
    return switch (width) {
        .u32 => @alignOf(u32),
        .u64 => @alignOf(u64),
        .u96 => @alignOf(u64),
        .u128 => @alignOf(u128),
    };
}

fn groupColumnSize(typ: GroupColumnType) usize {
    return switch (typ) {
        .i8 => @sizeOf(i8),
        .i16 => @sizeOf(i16),
        .i32 => @sizeOf(i32),
        .i64 => @sizeOf(i64),
        .f32 => @sizeOf(f32),
        .f64 => @sizeOf(f64),
    };
}

fn groupColumnAlign(typ: GroupColumnType) usize {
    return switch (typ) {
        .i8 => @alignOf(i8),
        .i16 => @alignOf(i16),
        .i32 => @alignOf(i32),
        .i64 => @alignOf(i64),
        .f32 => @alignOf(f32),
        .f64 => @alignOf(f64),
    };
}

const GroupRows = struct {
    slab: []align(16) u8 = &.{},
    len_rows: usize = 0,
    capacity_rows: usize = 0,
    layout: GroupRowsLayout = .{},
    // Variable-length string payload (string MIN/MAX only); see RawRows.str.
    str: StrStore = .{},

    inline fn len(self: GroupRows) usize {
        return self.len_rows;
    }

    inline fn capacity(self: GroupRows) usize {
        return self.capacity_rows;
    }

    fn slabBytes(layout: GroupRowsLayout, capacity_rows: usize) usize {
        var bytes: usize = 0;
        bytes = alignForward(bytes, groupKeyAlign(layout.key_width));
        bytes += groupKeySize(layout.key_width) * capacity_rows;
        for (layout.columns) |column| {
            bytes = alignForward(bytes, groupColumnAlign(column.physical_type));
            bytes += groupColumnSize(column.physical_type) * capacity_rows;
        }
        if (layout.has_rowref) {
            bytes = alignForward(bytes, @alignOf(i64));
            bytes += @sizeOf(i64) * capacity_rows;
        }
        if (layout.has_weight) {
            bytes = alignForward(bytes, @alignOf(u32));
            bytes += @sizeOf(u32) * capacity_rows;
        }
        bytes += nullableInputCount(layout) * capacity_rows;
        return bytes;
    }

    inline fn keyOffset(_: GroupRows) usize {
        return 0;
    }

    // Must mirror slabBytes exactly (see RawRows.validOffset).
    fn validOffset(self: GroupRows) usize {
        var offset = groupKeySize(self.layout.key_width) * self.capacity_rows;
        for (self.layout.columns) |column| {
            offset = alignForward(offset, groupColumnAlign(column.physical_type));
            offset += groupColumnSize(column.physical_type) * self.capacity_rows;
        }
        if (self.layout.has_rowref) {
            offset = alignForward(offset, @alignOf(i64));
            offset += @sizeOf(i64) * self.capacity_rows;
        }
        if (self.layout.has_weight) {
            offset = alignForward(offset, @alignOf(u32));
            offset += @sizeOf(u32) * self.capacity_rows;
        }
        return offset;
    }

    inline fn validAll(self: GroupRows, valid_index: usize) []u8 {
        return (self.slab.ptr + self.validOffset() + valid_index * self.capacity_rows)[0..self.capacity_rows];
    }

    fn rowrefOffset(self: GroupRows) usize {
        var offset = groupKeySize(self.layout.key_width) * self.capacity_rows;
        for (self.layout.columns) |column| {
            offset = alignForward(offset, groupColumnAlign(column.physical_type));
            offset += groupColumnSize(column.physical_type) * self.capacity_rows;
        }
        return alignForward(offset, @alignOf(i64));
    }

    inline fn rowrefAll(self: GroupRows) []i64 {
        std.debug.assert(self.layout.has_rowref);
        return @as([*]i64, @ptrCast(@alignCast(self.slab.ptr + self.rowrefOffset())))[0..self.capacity_rows];
    }

    // Must mirror slabBytes exactly (see RawRows.weightOffset).
    fn weightOffset(self: GroupRows) usize {
        var offset = groupKeySize(self.layout.key_width) * self.capacity_rows;
        for (self.layout.columns) |column| {
            offset = alignForward(offset, groupColumnAlign(column.physical_type));
            offset += groupColumnSize(column.physical_type) * self.capacity_rows;
        }
        if (self.layout.has_rowref) {
            offset = alignForward(offset, @alignOf(i64));
            offset += @sizeOf(i64) * self.capacity_rows;
        }
        return alignForward(offset, @alignOf(u32));
    }

    inline fn weightAll(self: GroupRows) []u32 {
        std.debug.assert(self.layout.has_weight);
        return @as([*]u32, @ptrCast(@alignCast(self.slab.ptr + self.weightOffset())))[0..self.capacity_rows];
    }

    fn columnOffset(self: GroupRows, column_index: usize) usize {
        var offset = groupKeySize(self.layout.key_width) * self.capacity_rows;
        var i: usize = 0;
        while (i < column_index) : (i += 1) {
            offset = alignForward(offset, groupColumnAlign(self.layout.columns[i].physical_type));
            offset += groupColumnSize(self.layout.columns[i].physical_type) * self.capacity_rows;
        }
        return alignForward(offset, groupColumnAlign(self.layout.columns[column_index].physical_type));
    }

    inline fn keyU32All(self: GroupRows) []u32 {
        std.debug.assert(self.layout.key_width == .u32);
        return @as([*]u32, @ptrCast(@alignCast(self.slab.ptr + self.keyOffset())))[0..self.capacity_rows];
    }

    inline fn keyU64All(self: GroupRows) []u64 {
        std.debug.assert(self.layout.key_width == .u64);
        return @as([*]u64, @ptrCast(@alignCast(self.slab.ptr + self.keyOffset())))[0..self.capacity_rows];
    }

    inline fn keyU96LoAll(self: GroupRows) []u64 {
        std.debug.assert(self.layout.key_width == .u96);
        return @as([*]u64, @ptrCast(@alignCast(self.slab.ptr + self.keyOffset())))[0..self.capacity_rows];
    }

    inline fn keyU96HiAll(self: GroupRows) []u32 {
        std.debug.assert(self.layout.key_width == .u96);
        const offset = self.keyOffset() + self.capacity_rows * @sizeOf(u64);
        return @as([*]u32, @ptrCast(@alignCast(self.slab.ptr + offset)))[0..self.capacity_rows];
    }

    inline fn keyU128All(self: GroupRows) []u128 {
        std.debug.assert(self.layout.key_width == .u128);
        return @as([*]u128, @ptrCast(@alignCast(self.slab.ptr + self.keyOffset())))[0..self.capacity_rows];
    }

    inline fn columnTypedAll(self: GroupRows, comptime T: type, column_index: usize) []T {
        return @as([*]T, @ptrCast(@alignCast(self.slab.ptr + self.columnOffset(column_index))))[0..self.capacity_rows];
    }

    // Read aggregate-input column `column_index` at `row` as i64 (integer
    // physical types, sign-extended) — the generic native-width group read.
    inline fn columnIntAt(self: GroupRows, column_index: usize, row: usize) i64 {
        return switch (self.layout.columns[column_index].physical_type) {
            .i8 => self.columnTypedAll(i8, column_index)[row],
            .i16 => self.columnTypedAll(i16, column_index)[row],
            .i32 => self.columnTypedAll(i32, column_index)[row],
            .i64 => self.columnTypedAll(i64, column_index)[row],
            .f32, .f64 => 0,
        };
    }

    // Read aggregate-input column `column_index` at `row` as f64 (float
    // physical types direct/widened; integer physical types converted) — the
    // float counterpart to columnIntAt for SUM/AVG/MIN/MAX over float inputs.
    inline fn columnFloatAt(self: GroupRows, column_index: usize, row: usize) f64 {
        return switch (self.layout.columns[column_index].physical_type) {
            .i8 => @floatFromInt(self.columnTypedAll(i8, column_index)[row]),
            .i16 => @floatFromInt(self.columnTypedAll(i16, column_index)[row]),
            .i32 => @floatFromInt(self.columnTypedAll(i32, column_index)[row]),
            .i64 => @floatFromInt(self.columnTypedAll(i64, column_index)[row]),
            .f32 => @floatCast(self.columnTypedAll(f32, column_index)[row]),
            .f64 => self.columnTypedAll(f64, column_index)[row],
        };
    }

    inline fn columnByteSlab(self: GroupRows, column_index: usize) []u8 {
        const sz = groupColumnSize(self.layout.columns[column_index].physical_type);
        return (self.slab.ptr + self.columnOffset(column_index))[0 .. sz * self.capacity_rows];
    }

    fn ensureTotalCapacity(self: *GroupRows, allocator: Allocator, layout: GroupRowsLayout, capacity_rows: usize) !void {
        if (!sameRowsLayout(self.layout, layout)) {
            self.deinit(allocator);
            self.layout = layout;
        }
        if (capacity_rows <= self.capacity_rows) return;
        var next: GroupRows = .{
            .slab = try allocator.alignedAlloc(u8, .@"16", slabBytes(layout, capacity_rows)),
            .len_rows = self.len_rows,
            .capacity_rows = capacity_rows,
            .layout = layout,
        };
        errdefer allocator.free(next.slab);
        if (self.len_rows != 0) {
            var i: usize = 0;
            while (i < self.len_rows) : (i += 1) next.setKey(i, self.keyAt(i));
            var col: usize = 0;
            while (col < layout.columns.len) : (col += 1) {
                const sz = groupColumnSize(layout.columns[col].physical_type);
                @memcpy(next.columnByteSlab(col)[0 .. sz * self.len_rows], self.columnByteSlab(col)[0 .. sz * self.len_rows]);
            }
            if (layout.has_rowref) @memcpy(next.rowrefAll()[0..self.len_rows], self.rowrefAll()[0..self.len_rows]);
            if (layout.has_weight) @memcpy(next.weightAll()[0..self.len_rows], self.weightAll()[0..self.len_rows]);
            var vi: usize = 0;
            const nv = nullableInputCount(layout);
            while (vi < nv) : (vi += 1) @memcpy(next.validAll(vi)[0..self.len_rows], self.validAll(vi)[0..self.len_rows]);
        }
        if (layout.has_str_payload) try self.str.ensure(allocator, layout.str_columns.len, capacity_rows, self.len_rows);
        next.str = self.str;
        allocator.free(self.slab);
        self.* = next;
    }

    fn resize(self: *GroupRows, allocator: Allocator, layout: GroupRowsLayout, new_len: usize) !void {
        try self.ensureTotalCapacity(allocator, layout, new_len);
        self.len_rows = new_len;
    }

    fn clearRetainingCapacity(self: *GroupRows) void {
        self.len_rows = 0;
        self.str.clear();
    }

    fn deinit(self: *GroupRows, allocator: Allocator) void {
        self.str.deinit(allocator);
        allocator.free(self.slab);
        self.* = .{};
    }

    fn appendRawRowsSlice(self: *GroupRows, allocator: Allocator, layout: GroupRowsLayout, rows: RawRows, start: usize, count: usize) !void {
        if (count == 0) return;
        const old_len = self.len();
        const new_len = old_len + count;
        try self.resize(allocator, layout, new_len);

        switch (self.layout.key_width) {
            .u32 => @memcpy(self.keyU32All()[old_len..new_len], rows.keyU32All()[start .. start + count]),
            .u64 => @memcpy(self.keyU64All()[old_len..new_len], rows.keyU64All()[start .. start + count]),
            .u96 => {
                @memcpy(self.keyU96LoAll()[old_len..new_len], rows.keyU96LoAll()[start .. start + count]);
                @memcpy(self.keyU96HiAll()[old_len..new_len], rows.keyU96HiAll()[start .. start + count]);
            },
            .u128 => @memcpy(self.keyU128All()[old_len..new_len], rows.keyU128All()[start .. start + count]),
        }
        var col: usize = 0;
        while (col < self.layout.columns.len) : (col += 1) {
            const sz = groupColumnSize(self.layout.columns[col].physical_type);
            @memcpy(self.columnByteSlab(col)[old_len * sz .. new_len * sz], rows.columnByteSlab(col)[start * sz .. (start + count) * sz]);
        }
        if (self.layout.has_rowref) @memcpy(self.rowrefAll()[old_len..new_len], rows.rowrefAll()[start .. start + count]);
        if (self.layout.has_weight) @memcpy(self.weightAll()[old_len..new_len], rows.weightAll()[start .. start + count]);
        {
            var vi: usize = 0;
            const nv = nullableInputCount(self.layout);
            while (vi < nv) : (vi += 1) @memcpy(self.validAll(vi)[old_len..new_len], rows.validAll(vi)[start .. start + count]);
        }
        // Variable-length string payload: copy each scattered row's string
        // values from the source RawRows store into this bucket's store. Owned
        // bytes, so the bucket survives the source chunk's recycle.
        if (self.layout.has_str_payload) {
            const k = self.layout.str_columns.len;
            var i: usize = 0;
            while (i < count) : (i += 1) {
                var c: usize = 0;
                while (c < k) : (c += 1) {
                    if (rows.str.isNull(k, start + i, c)) {
                        self.str.appendNull(k, old_len + i, c);
                    } else {
                        try self.str.append(allocator, k, old_len + i, c, rows.str.get(k, start + i, c));
                    }
                }
            }
        }
    }

    inline fn keyAt(self: GroupRows, idx: usize) u128 {
        return switch (self.layout.key_width) {
            .u32 => @as(u128, self.keyU32All()[idx]),
            .u64 => @as(u128, self.keyU64All()[idx]),
            .u96 => @as(u128, self.keyU96LoAll()[idx]) | (@as(u128, self.keyU96HiAll()[idx]) << 64),
            .u128 => self.keyU128All()[idx],
        };
    }

    inline fn setKey(self: GroupRows, idx: usize, key: u128) void {
        switch (self.layout.key_width) {
            .u32 => self.keyU32All()[idx] = @truncate(key),
            .u64 => self.keyU64All()[idx] = @truncate(key),
            .u96 => {
                self.keyU96LoAll()[idx] = @truncate(key);
                self.keyU96HiAll()[idx] = @truncate(key >> 64);
            },
            .u128 => self.keyU128All()[idx] = key,
        }
    }
};

const PartBucket = struct {
    fn deinit(self: *PartBucket, _: Allocator) void {
        self.* = .{};
    }
};

const SharedScanBucket = struct {
    lock: std.atomic.Mutex = .unlocked,

    fn deinit(self: *SharedScanBucket, _: Allocator) void {
        self.* = .{};
    }
};

const SharedScanBuffers = struct {
    buckets: []SharedScanBucket = &.{},
    bucket_count: usize = 0,
    bank_count: usize = 0,

    fn init(allocator: Allocator, bucket_count: usize, bank_count_raw: usize, _: usize) !SharedScanBuffers {
        const bank_count = @max(@as(usize, 1), bank_count_raw);
        const buckets = try allocator.alloc(SharedScanBucket, bucket_count * bank_count);
        errdefer allocator.free(buckets);
        for (buckets) |*bucket| {
            bucket.* = .{};
        }
        return .{
            .buckets = buckets,
            .bucket_count = bucket_count,
            .bank_count = bank_count,
        };
    }

    fn deinit(self: *SharedScanBuffers, allocator: Allocator) void {
        for (self.buckets) |*bucket| bucket.deinit(allocator);
        if (self.buckets.len > 0) allocator.free(self.buckets);
        self.* = .{};
    }

    inline fn at(self: *SharedScanBuffers, bank_idx: usize, bucket_idx: usize) *SharedScanBucket {
        return &self.buckets[bank_idx * self.bucket_count + bucket_idx];
    }
};

const WorkerParts = struct {
    buckets: []PartBucket = &.{},
    shared_buffers: ?*SharedScanBuffers = null,
    shared_bank_index: usize = 0,
    flat_scan_partitions: bool = false,
    flat_partitioned: bool = false,
    flat_raw_rows: RawRows = .{},
    flat_bucket_ids: std.ArrayListUnmanaged(u16) = .empty,
    flat_counts: []u32 = &.{},
    flat_offsets: []u32 = &.{},
    flat_next: []u32 = &.{},
    /// A deferred bucket fold's chunks, each row's part, the parts, and the
    /// cache-sized table and transient states each part folds into.
    defer_chunks: std.ArrayListUnmanaged(GroupChunk) = .empty,
    defer_part_ids: std.ArrayListUnmanaged(u8) = .empty,
    defer_parts: std.ArrayListUnmanaged(GroupRows) = .empty,
    defer_table: ?BucketTable = null,
    defer_states: StateSlab = .{},
    raw_active_rows: RawRows = .{},
    /// Weighted layouts: staged-row index of the active raw chunk by key
    /// hash, so a key that repeats anywhere in the chunk folds into its one
    /// staged row instead of only an adjacent repeat. Only a hint: an entry
    /// is used when it is below the chunk's length and its key matches, so
    /// entries left from earlier chunks are harmless and a miss just stages
    /// a new row.
    fold_slots: []u32 = &.{},
    fold_shift: u6 = 63,
    /// Fold-index probes and hits since the active chunk was last published;
    /// a chunk whose probes mostly missed turns the probe off for the rest of
    /// the query.
    fold_probes: u64 = 0,
    fold_hits: u64 = 0,
    fold_off: bool = false,
    worker_index: usize = 0,
    raw_scan_lane: usize = 0,
    scanned_count: u64 = 0,
    row_count: u64 = 0,
    local_buffered_rows: u64 = 0,
    published_chunks: u64 = 0,
    scan_ticks: i64 = 0,
    scan_reset_ticks: i64 = 0,
    scan_tiles: u64 = 0,
    scan_quanta: u64 = 0,
    scan_batches: u64 = 0,
    partition_ticks: i64 = 0,
    publish_ticks: i64 = 0,
    group_ticks: i64 = 0,
    sched_decision_ticks: i64 = 0,
    sched_scan_claim_ticks: i64 = 0,
    sched_group_pick_ticks: i64 = 0,
    sched_group_lock_ticks: i64 = 0,
    raw_queue_lock_ticks: i64 = 0,
    raw_scan_queue_lock_ticks: i64 = 0,
    raw_group_queue_lock_ticks: i64 = 0,
    raw_recycle_lock_ticks: i64 = 0,
    raw_agg_lock_ticks: i64 = 0,
    raw_stage_builder_lock_ticks: i64 = 0,
    raw_stage_ticks: i64 = 0,
    raw_stage_pop_ticks: i64 = 0,
    raw_stage_slice_ticks: i64 = 0,
    raw_stage_publish_ticks: i64 = 0,
    raw_stage_recycle_ticks: i64 = 0,
    raw_stage_input_chunks: u64 = 0,
    raw_stage_buffered_rows: u64 = 0,
    sched_loops: u64 = 0,
    sched_scan_jobs: u64 = 0,
    sched_stage_jobs: u64 = 0,
    sched_group_jobs: u64 = 0,
    sched_group_misses: u64 = 0,
    sched_idle_loops: u64 = 0,
    dirty_buckets: std.ArrayListUnmanaged(usize) = .empty,
    dirty_marks: []bool = &.{},
    route_counts: []u16 = &.{},
    route_offsets: []u16 = &.{},
    route_touched: std.ArrayListUnmanaged(u16) = .empty,
    recycle_lock: std.atomic.Mutex = .unlocked,

    fn init(allocator: Allocator, bucket_count: usize, _: usize) !WorkerParts {
        const buckets = try allocator.alloc(PartBucket, bucket_count);
        for (buckets) |*b| b.* = .{};
        errdefer allocator.free(buckets);
        const dirty_marks = try allocator.alloc(bool, bucket_count);
        errdefer allocator.free(dirty_marks);
        @memset(dirty_marks, false);
        const route_counts = try allocator.alloc(u16, bucket_count);
        errdefer allocator.free(route_counts);
        @memset(route_counts, 0);
        const route_offsets = try allocator.alloc(u16, bucket_count);
        errdefer allocator.free(route_offsets);
        @memset(route_offsets, 0);
        var route_touched: std.ArrayListUnmanaged(u16) = .empty;
        errdefer route_touched.deinit(allocator);
        try route_touched.ensureTotalCapacity(allocator, @min(bucket_count, MAX_ROUTE_BLOCK_ROWS));
        return .{
            .buckets = buckets,
            .dirty_marks = dirty_marks,
            .route_counts = route_counts,
            .route_offsets = route_offsets,
            .route_touched = route_touched,
        };
    }

    fn deinit(self: *WorkerParts, allocator: Allocator) void {
        for (self.buckets) |*b| b.deinit(allocator);
        if (self.buckets.len > 0) allocator.free(self.buckets);
        self.flat_raw_rows.deinit(allocator);
        self.flat_bucket_ids.deinit(allocator);
        self.raw_active_rows.deinit(allocator);
        if (self.fold_slots.len > 0) allocator.free(self.fold_slots);
        if (self.flat_counts.len > 0) allocator.free(self.flat_counts);
        if (self.flat_offsets.len > 0) allocator.free(self.flat_offsets);
        if (self.flat_next.len > 0) allocator.free(self.flat_next);
        self.dirty_buckets.deinit(allocator);
        if (self.dirty_marks.len > 0) allocator.free(self.dirty_marks);
        if (self.route_counts.len > 0) allocator.free(self.route_counts);
        if (self.route_offsets.len > 0) allocator.free(self.route_offsets);
        self.route_touched.deinit(allocator);
        self.releaseDeferScratch(allocator);
        self.* = .{};
    }

    fn enableFlatScanPartitions(self: *WorkerParts, allocator: Allocator, bucket_count: usize, reserve_rows: usize) !void {
        self.flat_scan_partitions = true;
        self.flat_partitioned = false;
        try self.ensureWideBucketScratch(allocator, bucket_count);
        if (reserve_rows > 0) {
            try self.flat_bucket_ids.ensureTotalCapacity(allocator, reserve_rows);
        }
    }

    fn releaseDeferScratch(self: *WorkerParts, allocator: Allocator) void {
        self.defer_chunks.deinit(allocator);
        self.defer_chunks = .empty;
        self.defer_part_ids.deinit(allocator);
        self.defer_part_ids = .empty;
        for (self.defer_parts.items) |*part| part.deinit(allocator);
        self.defer_parts.deinit(allocator);
        self.defer_parts = .empty;
        if (self.defer_table) |*t| t.deinit(allocator);
        self.defer_table = null;
        self.defer_states.deinit(allocator);
        self.defer_states = .{};
    }

    fn ensureWideBucketScratch(self: *WorkerParts, allocator: Allocator, bucket_count: usize) !void {
        if (self.flat_counts.len == 0) {
            self.flat_counts = try allocator.alloc(u32, bucket_count);
            @memset(self.flat_counts, 0);
        }
        if (self.flat_offsets.len == 0) {
            self.flat_offsets = try allocator.alloc(u32, bucket_count);
            @memset(self.flat_offsets, 0);
        }
        if (self.flat_next.len == 0) {
            self.flat_next = try allocator.alloc(u32, bucket_count);
            @memset(self.flat_next, 0);
        }
    }

    // Twice the chunk's rows in slots, so a full chunk's distinct keys rarely
    // collide. A reused workspace keeps its slots but not the last query's
    // on/off verdict.
    fn prepareFoldIndex(self: *WorkerParts, allocator: Allocator, raw_chunk_rows: usize) !void {
        const want = try std.math.ceilPowerOfTwo(usize, @max(raw_chunk_rows * 2, 64));
        if (self.fold_slots.len != want) {
            const slots = try allocator.alloc(u32, want);
            if (self.fold_slots.len > 0) allocator.free(self.fold_slots);
            self.fold_slots = slots;
            self.fold_shift = @intCast(64 - @as(u7, std.math.log2_int(usize, want)));
        }
        @memset(self.fold_slots, std.math.maxInt(u32));
        self.fold_probes = 0;
        self.fold_hits = 0;
        self.fold_off = false;
    }
};

// Fixed head of one group's record in a StateSlab. The variable number of
// numeric accumulator slots (i64 each) follows inline, immediately after the
// head, so a group's whole record is one contiguous, cache-friendly run.
// `key` is u128 (16-align) → the head is 32 bytes and the slab stride is kept a
// multiple of 16 so every record's key stays aligned.
const StateHead = extern struct {
    key: u128 = 0,
    // Row counter for the group (also the COUNT(*) result, state_index 0).
    count: u64 = 0,
    // Packed __rowloc of the first row that created this group; only meaningful
    // when the layout has a hashed key (has_rowref). Used at emit to
    // late-materialize the real key column values.
    rowref: i64 = 0,
};

const STATE_HEAD_BYTES: usize = @sizeOf(StateHead);

// A borrowed view of one group's record: its head plus a slice over its
// inline accumulator slots. The aggregate program indexes `slots` by
// `state_index - 1`; each slot holds an i64 (int sum/min/max) or an f64
// bit-pattern (float sum/min/max, avg) decided solely by the aggregate.
const StateRef = struct {
    head: *StateHead,
    slots: []i64,
};

const PendingCountUpdates = struct {
    const CAPACITY = 16;
    // Other buckets fold between a bucket's rounds, so its records only stay
    // cached from round to round while they fit a core's private cache. At
    // 2 MiB, buckets of 10K-60K groups took the immediate loop and missed on
    // most updates (16 threads, 512 KiB L2: GROUP BY ClientIP 0.145 -> 0.131 s,
    // nothing slower).
    const MIN_STATE_BYTES = 128 * 1024;
    heads: [CAPACITY]*StateHead = undefined,
    counts: [CAPACITY]u64 = undefined,
    len: usize = 0,
    next: usize = 0,

    fn append(self: *PendingCountUpdates, head: *StateHead, count: u64) void {
        @prefetch(&head.count, .{ .rw = .write, .locality = 1 });
        if (self.len == CAPACITY) {
            self.heads[self.next].count += self.counts[self.next];
        } else {
            self.len += 1;
        }
        self.heads[self.next] = head;
        self.counts[self.next] = count;
        self.next = (self.next + 1) % CAPACITY;
    }

    fn flush(self: *PendingCountUpdates) void {
        for (self.heads[0..self.len], self.counts[0..self.len]) |head, count| head.count += count;
        self.len = 0;
        self.next = 0;
    }
};

test "count update pipeline preserves repeated keys weights and partial batches" {
    var heads = [_]StateHead{.{}} ** 127;
    var expected = [_]u64{0} ** heads.len;
    var pending: PendingCountUpdates = .{};
    for (0..1003) |i| {
        const index = if (i % 3 == 0) 7 else (i * 37) % heads.len;
        const count: u64 = if (i % 11 == 0) @as(u64, std.math.maxInt(u32)) + 7 else i % 11 + 1;
        pending.append(&heads[index], count);
        expected[index] += count;
        if (i % 257 == 0) pending.flush();
    }
    pending.flush();
    pending.flush();
    for (heads, expected) |head, count| try std.testing.expectEqual(count, head.count);
}

test "count update pipeline folds exact results below and above the live state threshold" {
    const allocator = std.testing.allocator;
    inline for (.{ @as(usize, 1024), @as(usize, 70_000) }) |group_count| {
        var bucket = try PipeBucket.init(allocator, 16);
        defer bucket.deinit(allocator);
        var scratch: GroupScratch = .{};
        defer scratch.deinit(allocator);
        var rows: GroupRows = .{};
        defer rows.deinit(allocator);
        const layout = GroupRowsLayout{ .key_width = .u64, .columns = &.{}, .aggregates = &.{.{ .op = .count_star, .state_index = 0 }}, .has_weight = true };
        for (1..4) |weight| {
            var offset: usize = 0;
            while (offset < group_count) {
                const count = @min(4096, group_count - offset);
                try rows.resize(allocator, layout, count);
                for (0..count) |i| {
                    rows.setKey(i, (@as(u128, 1) << 40) + offset + i);
                    rows.weightAll()[i] = @intCast(weight);
                }
                try bucket.fold_rows(allocator, &scratch, rows, group_count * 6);
                offset += count;
            }
        }
        try std.testing.expectEqual(group_count, bucket.states.len);
        for (0..group_count) |i| {
            try std.testing.expectEqual((@as(u128, 1) << 40) + i, bucket.states.head(i).key);
            try std.testing.expectEqual(@as(u64, 6), bucket.states.head(i).count);
        }
    }
}

test "hashed key digests that differ only past bit 96 fold into separate groups" {
    const allocator = std.testing.allocator;
    var bucket = try PipeBucket.init(allocator, 16);
    defer bucket.deinit(allocator);
    var scratch: GroupScratch = .{};
    defer scratch.deinit(allocator);
    var rows: GroupRows = .{};
    defer rows.deinit(allocator);
    const layout = GroupRowsLayout{ .key_width = .u128, .hashed_key = true, .has_rowref = true, .columns = &.{}, .aggregates = &.{.{ .op = .count_star, .state_index = 0 }} };
    const low: u128 = 0x0123_4567_89ab_cdef_0011_2233;
    try rows.resize(allocator, layout, 6);
    for (0..6) |i| {
        rows.setKey(i, low | (@as(u128, i % 2) << 100));
        rows.rowrefAll()[i] = @intCast(i);
    }
    try bucket.fold_rows(allocator, &scratch, rows, 6);
    try std.testing.expectEqual(@as(usize, 2), bucket.states.len);
    for (0..2) |i| {
        const gid = bucket.table.find(low | (@as(u128, i) << 100)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u64, 3), bucket.states.head(gid).count);
    }
}

// Record stride follows the aggregate program; key and state widths do not
// change. Fixed-size pages avoid relocating live groups during growth.
const StateSlab = struct {
    pages: std.ArrayListUnmanaged([]align(16) u8) = .empty,
    len: usize = 0,
    cap: usize = 0,
    n_slots: usize = 0,
    stride: usize = 0,
    reserve_hint: usize = 0,

    const PAGE_ROWS = 8192;

    fn strideFor(n_slots: usize) usize {
        return std.mem.alignForward(usize, STATE_HEAD_BYTES + n_slots * @sizeOf(i64), 16);
    }

    // A layout change invalidates the old records even when capacity suffices.
    fn prepare(self: *StateSlab, allocator: Allocator, n_slots: usize) !void {
        if (self.stride != 0 and self.n_slots == n_slots) return;
        const reserve_hint = self.reserve_hint;
        self.deinit(allocator);
        self.reserve_hint = reserve_hint;
        self.n_slots = n_slots;
        self.stride = strideFor(n_slots);
        if (self.reserve_hint > 0) try self.growTo(allocator, @min(self.reserve_hint, PAGE_ROWS));
    }

    fn growTo(self: *StateSlab, allocator: Allocator, records: usize) !void {
        if (records <= self.cap) return;
        if (self.pages.items.len == 0) {
            const first_rows = try std.math.ceilPowerOfTwo(usize, @max(8, @min(records, PAGE_ROWS)));
            try self.pages.ensureUnusedCapacity(allocator, 1);
            const page = try allocator.alignedAlloc(u8, .@"16", first_rows * self.stride);
            errdefer allocator.free(page);
            self.pages.appendAssumeCapacity(page);
            self.cap = first_rows;
        }
        // Only the bounded first page can move; established full pages keep
        // their records in place as the group count grows.
        if (records > self.cap and self.cap < PAGE_ROWS) {
            const page = try allocator.alignedAlloc(u8, .@"16", PAGE_ROWS * self.stride);
            errdefer allocator.free(page);
            @memcpy(page[0 .. self.len * self.stride], self.pages.items[0][0 .. self.len * self.stride]);
            allocator.free(self.pages.items[0]);
            self.pages.items[0] = page;
            self.cap = PAGE_ROWS;
        }
        while (self.cap < records) {
            try self.pages.ensureUnusedCapacity(allocator, 1);
            const page = try allocator.alignedAlloc(u8, .@"16", PAGE_ROWS * self.stride);
            errdefer allocator.free(page);
            self.pages.appendAssumeCapacity(page);
            self.cap += PAGE_ROWS;
        }
    }

    fn ensureUnusedCapacity(self: *StateSlab, allocator: Allocator, additional: usize) !void {
        const need = self.len + additional;
        if (need <= self.cap) return;
        try self.growTo(allocator, need);
    }

    inline fn record_ptr(self: StateSlab, gid: usize) [*]u8 {
        return self.pages.items[gid / PAGE_ROWS].ptr + (gid % PAGE_ROWS) * self.stride;
    }

    inline fn head(self: StateSlab, gid: usize) *StateHead {
        return @ptrCast(@alignCast(self.record_ptr(gid)));
    }

    inline fn slotsOf(self: StateSlab, gid: usize) []i64 {
        const p = self.record_ptr(gid) + STATE_HEAD_BYTES;
        return @as([*]i64, @ptrCast(@alignCast(p)))[0..self.n_slots];
    }

    inline fn ref(self: StateSlab, gid: usize) StateRef {
        return .{ .head = self.head(gid), .slots = self.slotsOf(gid) };
    }

    // Append a fresh zeroed record (key/count/rowref/slots all 0) and return its
    // gid. Caller has ensured capacity.
    inline fn pushAssumeCapacity(self: *StateSlab) u32 {
        const gid: u32 = @intCast(self.len);
        const base = self.record_ptr(self.len);
        @memset(base[0..self.stride], 0);
        self.len += 1;
        return gid;
    }

    fn clearRetainingCapacity(self: *StateSlab) void {
        self.len = 0;
    }

    fn deinit(self: *StateSlab, allocator: Allocator) void {
        for (self.pages.items) |page| allocator.free(page);
        self.pages.deinit(allocator);
        self.* = .{};
    }
};

test "encoded machinery paged group state preserves records and clears reused slots" {
    const allocator = std.testing.allocator;
    var states: StateSlab = .{};
    defer states.deinit(allocator);
    try states.prepare(allocator, 2);
    try states.ensureUnusedCapacity(allocator, StateSlab.PAGE_ROWS);
    for (0..StateSlab.PAGE_ROWS) |i| {
        const gid = states.pushAssumeCapacity();
        states.head(gid).key = @as(u128, i) << 70;
        states.head(gid).count = i + 1;
        states.slotsOf(gid)[0] = -@as(i64, @intCast(i));
    }
    const first = states.head(0);
    try states.ensureUnusedCapacity(allocator, StateSlab.PAGE_ROWS + 1);
    try std.testing.expect(first == states.head(0));
    for (0..StateSlab.PAGE_ROWS) |i| {
        try std.testing.expectEqual(@as(u128, i) << 70, states.head(i).key);
        try std.testing.expectEqual(@as(u64, i + 1), states.head(i).count);
        try std.testing.expectEqual(-@as(i64, @intCast(i)), states.slotsOf(i)[0]);
    }
    const next = states.pushAssumeCapacity();
    try std.testing.expectEqual(@as(u64, 0), states.head(next).count);
    states.clearRetainingCapacity();
    const reused = states.pushAssumeCapacity();
    try std.testing.expectEqual(@as(u128, 0), states.head(reused).key);
    try std.testing.expectEqualSlices(i64, &.{ 0, 0 }, states.slotsOf(reused));
    try states.prepare(allocator, 1);
    try std.testing.expectEqual(@as(usize, 0), states.cap);
    try states.ensureUnusedCapacity(allocator, 1);
    try std.testing.expect(states.cap <= StateSlab.PAGE_ROWS);
}

fn test_state_growth_failure(allocator: Allocator) !void {
    var states: StateSlab = .{};
    defer states.deinit(allocator);
    try states.prepare(allocator, 1);
    try states.ensureUnusedCapacity(allocator, 3);
    _ = states.pushAssumeCapacity();
    try states.ensureUnusedCapacity(allocator, StateSlab.PAGE_ROWS * 2);
}

test "encoded machinery paged group state allocation failures release ownership" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, test_state_growth_failure, .{});
}

// Per-group running MIN/MAX bytes for a string aggregate. Kept in a side array
// parallel to a bucket's `states` (NOT in `State`, which would bloat the
// million-group numeric hot path). `bytes` is owned by the bucket allocator
// until emit, where survivors are re-dup'd into the result allocator.
const StrAcc = struct { bytes: []const u8 = &.{}, present: bool = false };
const StrAccRow = [MAX_GROUP_STR_SLOTS]StrAcc;

// Per-group GROUP_CONCAT collection: each non-null input row appends one
// (order, bytes) item; the emit sorts by `order` (the row's __rowloc, so the
// joined string follows physical row order — deterministic despite parallel
// staging, matching the legacy scan-order fold) and joins with the
// aggregate's separator into the str_arena. Item bytes and list growth both
// live in the bucket's str_arena (freed wholesale at teardown); the outer
// per-gid array uses the bucket allocator like str_states.
const ConcatItem = struct { order: i64, bytes: []const u8 };
const ConcatCell = std.ArrayListUnmanaged(ConcatItem);
const MAX_GROUP_CONCAT_SLOTS: usize = 2;
const ConcatRow = [MAX_GROUP_CONCAT_SLOTS]ConcatCell;

// Per-group opaque UDAF state. `bytes` (16-aligned, sized to the UDAF's
// state_size) is bump-allocated from the bucket's udf_arena; `initialized`
// gates the matching `destroy` at teardown (the arena reclaims the bytes,
// `destroy` releases any heap the UDAF's state itself owns).
const UdfCell = struct { bytes: []align(16) u8 = &.{}, initialized: bool = false };
const UdfStateRow = [MAX_GROUP_UDF_SLOTS]UdfCell;

const GroupScratch = struct {
    gids: std.ArrayListUnmanaged(u32) = .empty,
    row_idxs: std.ArrayListUnmanaged(u32) = .empty,

    fn deinit(self: *GroupScratch, allocator: Allocator) void {
        self.gids.deinit(allocator);
        self.row_idxs.deinit(allocator);
        self.* = .{};
    }
};

const BucketResult = struct {
    top: TopSet = .{},
    row_count: u64 = 0,
    group_count: u32 = 0,

    fn deinit(self: *BucketResult, allocator: Allocator) void {
        _ = allocator;
        self.* = .{};
    }
};

const PipeChunk = struct {
    owner_worker: usize,
};

const RawChunk = struct {
    rows: RawRows,
    owner_worker: usize,
};

const GroupChunk = struct {
    rows: GroupRows,
    owner_worker: usize,
    bucket_idx: usize,
};

const RawQueue = struct {
    lock: std.atomic.Mutex = .unlocked,
    lease: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    scan_lease: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    chunks: std.ArrayListUnmanaged(RawChunk) = .empty,
    queued_rows: u64 = 0,
    queued_rows_atomic: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    queued_chunks_atomic: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    fn deinit(self: *RawQueue, allocator: Allocator) void {
        for (self.chunks.items) |*chunk| chunk.rows.deinit(allocator);
        self.chunks.deinit(allocator);
        self.* = .{};
    }
};

const GroupQueue = struct {
    lock: std.atomic.Mutex = .unlocked,
    lease: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    scan_lease: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    chunks: std.ArrayListUnmanaged(GroupChunk) = .empty,
    queued_rows: u64 = 0,
    queued_rows_atomic: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    queued_chunks_atomic: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    fn deinit(self: *GroupQueue, allocator: Allocator) void {
        for (self.chunks.items) |*chunk| chunk.rows.deinit(allocator);
        self.chunks.deinit(allocator);
        self.* = .{};
    }
};

const StageBucketBuilder = struct {
    lock: std.atomic.Mutex = .unlocked,
    rows: GroupRows = .{},

    fn deinit(self: *StageBucketBuilder, allocator: Allocator) void {
        self.rows.deinit(allocator);
        self.* = .{};
    }
};

const GroupAllocationProfile = struct {
    ticks: i64 = 0,
    table_growths: usize = 0,
};

const PipeBucket = struct {
    queue_lock: std.atomic.Mutex = .unlocked,
    agg_lock: std.atomic.Mutex = .unlocked,
    chunks: std.ArrayListUnmanaged(PipeChunk) = .empty,
    queued_rows: u64 = 0,
    table: BucketTable,
    expected_groups: usize,
    observed_input_rows: u64 = 0,
    allocation_profile: GroupAllocationProfile = .{},
    states: StateSlab = .{},
    // Parallel to `states` (gid-indexed); populated only for string MIN/MAX
    // queries. Empty for the numeric common case.
    str_states: std.ArrayListUnmanaged(StrAccRow) = .empty,
    // Parallel to `states` (gid-indexed); populated only for GROUP_CONCAT
    // queries. Cell item lists grow in the str_arena (no per-cell deinit).
    concat_states: std.ArrayListUnmanaged(ConcatRow) = .empty,
    // Parallel to `states` (gid-indexed); populated only for UDAF queries.
    // Each cell's opaque state blob is bump-allocated from `udf_arena`.
    udf_states: std.ArrayListUnmanaged(UdfStateRow) = .empty,
    // Bump arena for the per-group UDAF state blobs (freed wholesale at
    // reset/teardown; `destroyUdfStates` runs the UDAFs' own `destroy` first).
    udf_arena: std.heap.ArenaAllocator,
    // Bump arena for the running-MIN/MAX bytes and the GROUP_CONCAT cells. A
    // per-improvement `dupe` from a shared allocator scatters each group's
    // current value across the heap, so reading it back for the next row's
    // compare is a main-memory miss — the dominant cost of string MIN over
    // millions of groups. Allocating from a contiguous arena keeps those
    // values L2/L3-resident.
    str_arena: std.heap.ArenaAllocator,
    // The running MIN/MAX copies, in `str_arena`: an improvement reuses the
    // block of the value it replaces, so the bytes stay bounded by the groups.
    str_bank: StringBank = .{},
    // One combined membership set per COUNT(DISTINCT) field. Indexed by the
    // aggregate's distinct_state_index; only the first `layout.distinct_slot_count`
    // are touched. Each holds (gid,value) keys for this bucket's groups.
    distinct_sets: DistinctSlots = .{},
    row_count: u64 = 0,

    fn init(allocator: Allocator, expected_groups: usize) !PipeBucket {
        // One batch bounds speculative memory while keeping small tables'
        // page faults out of the scan/route/group workers' allocation traffic.
        const initial_groups = @min(expected_groups, PIPE_CHUNK_ROWS);
        return .{
            .table = .{ .narrow = try GroupTable.init(allocator, initial_groups) },
            .expected_groups = expected_groups,
            .states = .{ .reserve_hint = initial_groups },
            .str_arena = std.heap.ArenaAllocator.init(allocator),
            .udf_arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    fn fold_rows(self: *PipeBucket, allocator: Allocator, scratch: *GroupScratch, rows: GroupRows, input_rows: u64) !void {
        try groupChunkRowsDirect(&self.table, &self.states, &self.str_states, &self.concat_states, &self.udf_states, self.udf_arena.allocator(), &self.distinct_sets, scratch, allocator, self.str_arena.allocator(), &self.str_bank, rows, self.expected_groups, input_rows, self.observed_input_rows, if (PROFILING) &self.allocation_profile else null);
        self.row_count += rows.len();
        if (rows.layout.has_weight) {
            var represented_rows: u64 = 0;
            for (rows.weightAll()[0..rows.len()]) |weight| represented_rows += weight;
            self.observed_input_rows += represented_rows;
        } else {
            self.observed_input_rows += rows.len();
        }
    }

    test "group reservation empty bucket does not reserve the row-count hint" {
        const allocator = std.testing.allocator;
        var bucket = try PipeBucket.init(allocator, 1_000_000);
        defer bucket.deinit(allocator);
        var scratch: GroupScratch = .{};
        defer scratch.deinit(allocator);
        try bucket.fold_rows(allocator, &scratch, .{}, 1_000_000);
        try std.testing.expectEqual(group_table.capacityFor(PIPE_CHUNK_ROWS), bucket.table.slotCount());
        try std.testing.expectEqual(@as(usize, 0), bucket.states.cap);
        try std.testing.expectEqual(@as(u64, 0), bucket.observed_input_rows);
    }

    fn allocation_failure_fixture(allocator: Allocator) !void {
        var bucket = try PipeBucket.init(allocator, 16);
        defer bucket.deinit(allocator);
        var scratch: GroupScratch = .{};
        defer scratch.deinit(allocator);
        var rows: GroupRows = .{};
        defer rows.deinit(allocator);
        try rows.ensureTotalCapacity(allocator, .{
            .key_width = .u64,
            .columns = &.{},
            .aggregates = &.{.{ .op = .count_star, .state_index = 0 }},
        }, 256);
        rows.len_rows = 256;
        for (0..4) |batch| {
            for (0..256) |i| rows.setKey(i, batch * 256 + i);
            try bucket.fold_rows(allocator, &scratch, rows, 1024);
        }
        try std.testing.expectEqual(@as(usize, 1024), bucket.states.len);
    }

    test "group reservation releases allocations after a failed grow" {
        try std.testing.checkAllAllocationFailures(std.testing.allocator, allocation_failure_fixture, .{});
    }

    test "group reservation keeps geometric state growth when the hint is too small" {
        const allocator = std.testing.allocator;
        var bucket = try PipeBucket.init(allocator, 8192);
        defer bucket.deinit(allocator);
        var scratch: GroupScratch = .{};
        defer scratch.deinit(allocator);
        var rows: GroupRows = .{};
        defer rows.deinit(allocator);
        try rows.ensureTotalCapacity(allocator, .{
            .key_width = .u64,
            .columns = &.{},
            .aggregates = &.{.{ .op = .count_star, .state_index = 0 }},
        }, 8192);
        rows.len_rows = 8192;
        for (0..8) |batch| {
            for (0..8192) |i| rows.setKey(i, batch * 8192 + i);
            try bucket.fold_rows(allocator, &scratch, rows, 65536);
        }
        try std.testing.expectEqual(@as(usize, 65536), bucket.states.len);
        try std.testing.expectEqual(@as(usize, 65536), bucket.states.cap);
        for (0..65536) |key| {
            const gid = bucket.table.find(key) orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(@as(u64, 1), bucket.states.head(gid).count);
        }
    }

    fn queued_allocation_failure_fixture(allocator: Allocator) !void {
        var buckets = [_]PipeBucket{try PipeBucket.init(allocator, 16)};
        defer buckets[0].deinit(allocator);
        var scratch: GroupScratch = .{};
        defer scratch.deinit(allocator);
        var local: WorkerParts = .{};
        defer local.deinit(allocator);
        var shared = PipeShared{
            .allocator = allocator,
            .buckets = &buckets,
            .bucket_count = 1,
            .input_rows_per_bucket = 1024,
            .scan_threads = 1,
            .group_rows_layout = .{
                .key_width = .u64,
                .columns = &.{},
                .aggregates = &.{.{ .op = .count_star, .state_index = 0 }},
            },
        };
        defer deinitRawQueues(&shared);
        shared.raw_group_queues = try allocator.alloc(GroupQueue, 1);
        shared.raw_group_queues[0] = .{};
        const queue = &shared.raw_group_queues[0];
        for (0..4) |batch| {
            var rows: GroupRows = .{};
            errdefer rows.deinit(allocator);
            try rows.ensureTotalCapacity(allocator, shared.group_rows_layout, 256);
            rows.len_rows = 256;
            for (0..256) |i| rows.setKey(i, batch * 256 + i);
            try queue.chunks.append(allocator, .{ .rows = rows, .owner_worker = 0, .bucket_idx = 0 });
        }
        queue.queued_rows = 1024;
        queue.queued_rows_atomic.store(1024, .monotonic);
        queue.queued_chunks_atomic.store(4, .monotonic);
        shared.stage_outstanding_rows.store(1024, .monotonic);
        shared.stage_outstanding_chunks.store(4, .monotonic);
        shared.pending_group_rows.store(1024, .monotonic);
        var ticks: i64 = 0;
        var chunks: u64 = 0;
        const drained = drainRawDedicatedGroupLane(allocator, &shared, &local, &scratch, 0, 512, 4, &ticks, &chunks, false);
        try std.testing.expect(buckets[0].agg_lock.tryLock());
        buckets[0].agg_lock.unlock();
        try std.testing.expectEqual(@as(u64, 0), shared.stage_outstanding_rows.load(.monotonic));
        try std.testing.expectEqual(@as(usize, 0), shared.stage_outstanding_chunks.load(.monotonic));
        try std.testing.expect(try drained);
        try std.testing.expectEqual(@as(usize, 1024), buckets[0].states.len);
    }

    test "group reservation cleans up queued chunks and unlocks on allocation failure" {
        try std.testing.checkAllAllocationFailures(std.testing.allocator, queued_allocation_failure_fixture, .{});
    }

    test "group reservation preserves all counts through skew growth and workspace reuse" {
        const allocator = std.testing.allocator;
        const Distribution = enum { correlated, unique, late_unique };
        inline for (.{ GroupKeyWidth.u32, .u64, .u96, .u128 }) |width| {
            inline for (.{ false, true }) |weighted| {
                var bucket = try PipeBucket.init(allocator, 1_000_000);
                defer bucket.deinit(allocator);
                var scratch: GroupScratch = .{};
                defer scratch.deinit(allocator);
                var rows: GroupRows = .{};
                defer rows.deinit(allocator);
                const batch_rows = 256;
                const total_rows = 16_384;
                const weight: u32 = if (weighted) 3 else 1;
                try rows.ensureTotalCapacity(allocator, .{
                    .key_width = width,
                    .columns = &.{},
                    .aggregates = &.{.{ .op = .count_star, .state_index = 0 }},
                    .has_weight = weighted,
                }, batch_rows);
                inline for (.{ Distribution.correlated, Distribution.unique, Distribution.late_unique }) |distribution| {
                    resetPipeBucket(&bucket, allocator);
                    // A stale, too-small hint must not truncate later groups.
                    bucket.expected_groups = if (distribution == .late_unique) 16 else 1_000_000;
                    var expected = [_]u64{0} ** total_rows;
                    for (0..total_rows / batch_rows) |batch| {
                        rows.len_rows = batch_rows;
                        for (0..batch_rows) |r| {
                            const i = batch * batch_rows + r;
                            const key: usize = switch (distribution) {
                                .correlated => i % 64,
                                .unique => i,
                                .late_unique => if (i < total_rows / 2) i % 64 else i,
                            };
                            const packed_key: u128 = if (width == .u96 or width == .u128) @as(u128, key) | (@as(u128, key) << 64) else key;
                            rows.setKey(r, packed_key);
                            if (weighted) rows.weightAll()[r] = weight;
                            expected[key] += weight;
                        }
                        try bucket.fold_rows(allocator, &scratch, rows, total_rows * weight);
                    }
                    var groups: usize = 0;
                    for (&expected, 0..) |count, key| {
                        if (count == 0) continue;
                        groups += 1;
                        const packed_key: u128 = if (width == .u96 or width == .u128) @as(u128, key) | (@as(u128, key) << 64) else key;
                        const gid = bucket.table.find(packed_key) orelse return error.TestUnexpectedResult;
                        try std.testing.expectEqual(count, bucket.states.head(gid).count);
                    }
                    try std.testing.expectEqual(groups, bucket.table.len());
                    try std.testing.expectEqual(groups, bucket.states.len);
                    try std.testing.expectEqual(@as(u64, total_rows * weight), bucket.observed_input_rows);
                    if (distribution == .correlated) {
                        try std.testing.expect(bucket.table.slotCount() <= group_table.capacityFor(PIPE_CHUNK_ROWS));
                        try std.testing.expect(bucket.states.cap <= PIPE_CHUNK_ROWS);
                    }
                }
            }
        }
    }

    fn freeStrBytes(self: *PipeBucket) void {
        _ = self.str_arena.reset(.free_all);
        self.str_bank.reset();
    }

    // Run each initialized UDAF state's `destroy` (releasing heap the state
    // owns) before the udf_arena reclaims the blobs. Idempotent: clears the
    // initialized flag so a later reset/teardown can't double-destroy.
    fn destroyUdfStates(self: *PipeBucket, aggregates: []const GroupAggregateSpec) void {
        if (self.udf_states.items.len == 0) return;
        for (self.udf_states.items) |*row| {
            for (aggregates) |agg| {
                if (!agg.is_udf) continue;
                const cell = &row[agg.udf_state_index];
                if (!cell.initialized) continue;
                if (agg.udf_entry) |entry| {
                    if (entry.destroy) |destroy| {
                        const ctx = udf_mod.AggregateContext{ .allocator = self.udf_arena.allocator(), .user_data = entry.user_data };
                        destroy(&ctx, @ptrCast(cell.bytes.ptr));
                    }
                }
                cell.initialized = false;
            }
        }
    }

    fn deinit(self: *PipeBucket, allocator: Allocator) void {
        self.str_arena.deinit();
        self.udf_arena.deinit();
        self.str_states.deinit(allocator);
        self.concat_states.deinit(allocator);
        self.udf_states.deinit(allocator);
        self.distinct_sets.deinit(allocator);
        self.chunks.deinit(allocator);
        self.table.deinit(allocator);
        self.states.deinit(allocator);
        self.* = undefined;
    }
};

const PipeShared = struct {
    resources: ?*thindb.exec.memory.MemoryAccountant = null,
    allocator: Allocator,
    buckets: []PipeBucket,
    bucket_count: usize,
    input_rows_per_bucket: u64 = 0,
    raw_queue_lock: std.atomic.Mutex = .unlocked,
    raw_chunks: std.ArrayListUnmanaged(RawChunk) = .empty,
    raw_recycle_lock: std.atomic.Mutex = .unlocked,
    raw_recycled_rows: std.ArrayListUnmanaged(RawRows) = .empty,
    group_recycled_rows: std.ArrayListUnmanaged(GroupRows) = .empty,
    recycle_byte_limit: ?usize = null,
    recycled_bytes: usize = 0,
    raw_merge_lock: std.atomic.Mutex = .unlocked,
    raw_scan_queues: []RawQueue = &.{},
    raw_group_queues: []GroupQueue = &.{},
    stage_builders: []StageBucketBuilder = &.{},
    group_rows_layout: GroupRowsLayout = .{},
    generic_filter_required: bool = false,
    count_sketch: ?CountSketchPass = null,
    stage_outstanding_chunks: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    stage_outstanding_rows: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    stage_builder_rows: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    active_stage_jobs: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    scan_threads: usize,
    scans_done: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    outstanding_chunks: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    // Published staged rows not yet folded, including unpublished stage tails.
    // A weighted row counts once here, regardless of its aggregate weight.
    pending_group_rows: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    scan_buffered_rows: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    active_scan_jobs: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    active_group_jobs: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    // Set by any worker that fails. Peers poll it at the top of their schedule
    // loop and break immediately, so one worker's error tears the run down
    // cleanly (the orchestrator joins, sees the error, and returns it) instead
    // of the survivors spinning forever waiting on a `scans_done` that the
    // failed worker never signalled.
    aborted: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    scan_tiles: RowGroupTiles = .{},
    next_final_local_bucket: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    total_scan_rgs: usize = 0,
    local_reserve_per_bucket: usize = 0,
    route_block_rows: usize = AUTO_ROUTE_BLOCK_ROWS,
    route_block_rows_set: bool = false,
    direct_final_local: bool = false,
    local_parts: []WorkerParts = &.{},
    shared_scan_buffers: ?*SharedScanBuffers = null,
    // Set when one worker runs the whole grid over a single bucket: with no
    // peer to hand work to, a filled staging chunk folds straight into that
    // bucket rather than queueing for a stage pass that only copies it again.
    serial_fold_scratch: ?*GroupScratch = null,
    // Without an Io, idle grid workers yield instead of parking.
    io: ?std.Io = null,
    // Bumped by anything that can hand a parked grid worker work: a queue
    // publish (wakes one), or a transition every worker must observe — scan
    // exhausted, all scans done, grouping drained, abort (wakes all). A
    // worker that releases a lane needs no signal: it re-checks before parking.
    work_epoch: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    parked_workers: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    /// No bucket folds until every row is staged (see DEFER_FOLD_MIN_GROUPS).
    defer_fold: bool = false,
    defer_released: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Every row sits in a group queue: scans are done, no raw chunk awaits
    /// a stage pass, and no stage builder holds a tail.
    fn stage_complete(self: *const PipeShared) bool {
        // Once every scan is done, nothing new enters the stage, so each
        // count below stays zero once read as zero.
        return self.scans_done.load(.acquire) == self.scan_threads and
            self.outstanding_chunks.load(.acquire) == 0 and
            self.active_stage_jobs.load(.acquire) == 0 and
            self.stage_builder_rows.load(.acquire) == 0;
    }

    /// Whether a deferred grid's buckets may fold. The first worker to see
    /// staging complete wakes the parked rest.
    fn deferred_folds_released(self: *PipeShared) bool {
        if (self.defer_released.load(.acquire)) return true;
        if (!self.stage_complete()) return false;
        if (!self.defer_released.swap(true, .acq_rel)) self.signalWork(std.math.maxInt(u32));
        return true;
    }

    fn grouping_complete(self: *const PipeShared) bool {
        // Closing every producer makes the remaining row count monotone.
        // Its credits survive queue/builder hand-offs until group writes finish.
        return self.scans_done.load(.acquire) == self.scan_threads and
            self.pending_group_rows.load(.acquire) == 0;
    }

    fn signalWork(self: *PipeShared, waiters: u32) void {
        _ = self.work_epoch.fetchAdd(1, .seq_cst);
        if (self.parked_workers.load(.seq_cst) == 0) return;
        const io = self.io orelse return;
        io.futexWake(u32, &self.work_epoch.raw, waiters);
    }

    // `seen_epoch` must be read before the caller's last look for work, so a
    // signal sent after that look makes the wait return at once.
    fn parkUntilSignal(self: *PipeShared, seen_epoch: u32) void {
        const io = self.io orelse {
            std.Thread.yield() catch {};
            return;
        };
        _ = self.parked_workers.fetchAdd(1, .seq_cst);
        defer _ = self.parked_workers.fetchSub(1, .seq_cst);
        io.futexWaitTimeout(u32, &self.work_epoch.raw, seen_epoch, .{ .duration = .{
            .raw = .fromNanoseconds(GRID_PARK_TIMEOUT_NS),
            .clock = .awake,
        } }) catch {};
    }
};

test "group completion waits for a delayed partial before collecting top rows and OFFSET" {
    const allocator = std.testing.allocator;
    const group_count = 10_020;
    const offset = 10_000;
    var buckets = [_]PipeBucket{try PipeBucket.init(allocator, 16)};
    defer buckets[0].deinit(allocator);
    var local: WorkerParts = .{};
    defer local.deinit(allocator);
    var scratch: GroupScratch = .{};
    defer scratch.deinit(allocator);
    var shared = PipeShared{
        .allocator = allocator,
        .buckets = &buckets,
        .bucket_count = 1,
        .input_rows_per_bucket = group_count * (group_count + 1) / 2,
        .scan_threads = 1,
        .group_rows_layout = .{
            .key_width = .u32,
            .columns = &.{},
            .aggregates = &.{.{ .op = .count_star, .state_index = 0 }},
            .has_weight = true,
        },
    };
    defer deinitRawQueues(&shared);
    shared.raw_scan_queues = try allocator.alloc(RawQueue, 1);
    shared.raw_scan_queues[0] = .{};
    shared.raw_group_queues = try allocator.alloc(GroupQueue, 1);
    shared.raw_group_queues[0] = .{};
    shared.stage_builders = try allocator.alloc(StageBucketBuilder, 1);
    shared.stage_builders[0] = .{};
    try std.testing.expect(!shared.grouping_complete());

    var source: RawRows = .{};
    defer source.deinit(allocator);
    try source.resize(allocator, shared.group_rows_layout, group_count);
    for (0..group_count) |i| {
        source.setKey(i, i);
        source.weightAll()[i] = @intCast(i + 1);
    }
    try publishRawRowsToQueue(&shared, &shared.raw_scan_queues[0], 0, &source, 16384, &shared.outstanding_chunks, &shared.pending_group_rows, null, null);
    shared.scans_done.store(1, .release);
    try std.testing.expect(!shared.grouping_complete());

    // An idle worker can sample empty builders, then resume its completion
    // check after a peer has staged the final input into an unpublished tail.
    const earlier_has_partials = shared.stage_builder_rows.load(.acquire) > 0;
    try std.testing.expect(!earlier_has_partials);
    try std.testing.expect(claimRawQueueLaneExact(shared.raw_scan_queues, 0));
    try std.testing.expect(try drainRawDedicatedStageSharedBuilders(allocator, &shared, &local, 0, 16384, 32768, 1, false));
    try std.testing.expectEqual(@as(u64, group_count), shared.stage_builder_rows.load(.acquire));
    try std.testing.expectEqual(@as(u64, group_count), shared.pending_group_rows.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), shared.stage_outstanding_chunks.load(.acquire));
    try std.testing.expect(!shared.grouping_complete());

    try std.testing.expect(try flushSharedStageBuilders(&shared, &local, false));
    try std.testing.expect(!shared.grouping_complete());
    var ticks: i64 = 0;
    var chunks: u64 = 0;
    try std.testing.expect(claimRawQueueLaneExact(shared.raw_group_queues, 0));
    try std.testing.expect(try drainRawDedicatedGroupLane(allocator, &shared, &local, &scratch, 0, 32768, 1, &ticks, &chunks, false));
    try std.testing.expect(shared.grouping_complete());
    try std.testing.expectEqual(@as(usize, group_count), buckets[0].states.len);

    var top = try TopSet.init(allocator, offset + 10);
    defer top.deinit(allocator);
    try collectOwnedTop(&shared, 0, 1, &top, &ticks, false);
    try top.sortBestFirst(allocator);
    try std.testing.expectEqual(@as(usize, offset + 10), top.len);
    for (offset..top.len, 0..) |rank, i| {
        const row = top.rowAt(rank);
        try std.testing.expectEqual(@as(u128, group_count - 1 - offset - i), row.key);
        try std.testing.expectEqual(@as(u64, group_count - offset - i), row.count);
    }
}

test "a deferred bucket fold matches each group's rows, split into parts or not" {
    const allocator = std.testing.allocator;
    // A group's id sits in the key bits a narrower key width would drop.
    const cases = .{
        .{ .rows = 1_500, .top_k = 0, .key_width = .u96, .group_shift = 64 },
        .{ .rows = 1_500, .top_k = 10, .key_width = .u96, .group_shift = 64 },
        .{ .rows = 40_000, .top_k = 0, .key_width = .u96, .group_shift = 64 },
        .{ .rows = 40_000, .top_k = 10, .key_width = .u96, .group_shift = 64 },
        .{ .rows = 40_000, .top_k = 0, .key_width = .u128, .group_shift = 96 },
        .{ .rows = 40_000, .top_k = 10, .key_width = .u128, .group_shift = 96 },
    };
    inline for (cases) |c| {
        const group_count = c.rows / 3;
        var buckets = [_]PipeBucket{try PipeBucket.init(allocator, 16)};
        defer buckets[0].deinit(allocator);
        var local: WorkerParts = .{};
        defer local.deinit(allocator);
        var scratch: GroupScratch = .{};
        defer scratch.deinit(allocator);
        var shared = PipeShared{
            .allocator = allocator,
            .buckets = &buckets,
            .bucket_count = 1,
            .input_rows_per_bucket = c.rows,
            .scan_threads = 1,
            .group_rows_layout = .{
                .key_width = c.key_width,
                .columns = &.{},
                .aggregates = &.{.{ .op = .count_star, .state_index = 0 }},
                .has_weight = true,
            },
            .defer_fold = true,
        };
        defer deinitRawQueues(&shared);
        shared.raw_scan_queues = try allocator.alloc(RawQueue, 1);
        shared.raw_scan_queues[0] = .{};
        shared.raw_group_queues = try allocator.alloc(GroupQueue, 1);
        shared.raw_group_queues[0] = .{};
        shared.stage_builders = try allocator.alloc(StageBucketBuilder, 1);
        shared.stage_builders[0] = .{};

        // Groups whose keys differ only in their high 32 bits, rows spread over
        // the input, and weights so a group's count is not its row count.
        const expected = try allocator.alloc(u64, group_count);
        defer allocator.free(expected);
        @memset(expected, 0);
        var source: RawRows = .{};
        defer source.deinit(allocator);
        try source.resize(allocator, shared.group_rows_layout, c.rows);
        for (0..c.rows) |i| {
            const g = (i * 7919) % group_count;
            const weight: u32 = @intCast(i % 3 + 1);
            source.setKey(i, 5 | (@as(u128, g) << c.group_shift));
            source.weightAll()[i] = weight;
            expected[g] += weight;
        }
        try publishRawRowsToQueue(&shared, &shared.raw_scan_queues[0], 0, &source, 4096, &shared.outstanding_chunks, &shared.pending_group_rows, null, null);
        shared.scans_done.store(1, .release);
        try std.testing.expect(claimRawQueueLaneExact(shared.raw_scan_queues, 0));
        try std.testing.expect(try drainRawDedicatedStageSharedBuilders(allocator, &shared, &local, 0, 4096, 4096, 4, false));
        try std.testing.expect(!shared.stage_complete());
        _ = try flushSharedStageBuilders(&shared, &local, false);
        try std.testing.expect(shared.stage_complete());

        var top = if (c.top_k == 0) TopSet{} else try TopSet.init(allocator, c.top_k);
        defer top.deinit(allocator);
        var ticks: i64 = 0;
        var chunks: u64 = 0;
        try std.testing.expect(try foldDeferredBucket(&shared, &local, &scratch, 0, &top, &ticks, &chunks, false));
        try std.testing.expect(shared.grouping_complete());
        try std.testing.expectEqual(@as(u64, c.rows), buckets[0].row_count);

        if (c.top_k == 0) {
            try std.testing.expectEqual(@as(usize, group_count), buckets[0].states.len);
            for (0..group_count) |gid| {
                const head = buckets[0].states.head(gid);
                try std.testing.expectEqual(@as(u128, 5), head.key & std.math.maxInt(u64));
                try std.testing.expectEqual(expected[@intCast(head.key >> c.group_shift)], head.count);
            }
        } else {
            // Top-N states are transient: each group goes to the top set as it folds.
            try std.testing.expectEqual(@as(usize, 0), buckets[0].states.len);
            try top.sortBestFirst(allocator);
            try std.testing.expectEqual(@as(usize, c.top_k), top.len);
            var prev: ?TopRow = null;
            for (0..top.len) |rank| {
                const row = top.rowAt(rank);
                try std.testing.expectEqual(expected[@intCast(row.key >> c.group_shift)], row.count);
                if (prev) |p| try std.testing.expect(p.count > row.count or (p.count == row.count and p.key < row.key));
                prev = row;
            }
            // Nothing outside the top set beats its last row.
            const last = top.rowAt(top.len - 1);
            for (expected, 0..) |count, g| {
                const key = 5 | (@as(u128, g) << c.group_shift);
                if (count > last.count or (count == last.count and key < last.key)) {
                    var found = false;
                    for (0..top.len) |rank| found = found or top.rowAt(rank).key == key;
                    try std.testing.expect(found);
                }
            }
        }
    }
}

test "a grid defers its folds only for many groups per bucket and staging within the budget" {
    // 8-byte staged rows against a 64 MiB budget: up to 2M rows may stage.
    var pool = thindb.exec.memory.MemoryPool.init(64 * 1024 * 1024);
    var resources = thindb.exec.memory.MemoryAccountant.init(&pool);
    const cases = .{
        .{ .want = true, .staged = true, .workers = 4, .groups = DEFER_FOLD_MIN_GROUPS, .rows = 2 * 1024 * 1024, .budget = true, .shape = .plain },
        .{ .want = false, .staged = false, .workers = 4, .groups = DEFER_FOLD_MIN_GROUPS, .rows = 1_000_000, .budget = true, .shape = .plain },
        .{ .want = false, .staged = true, .workers = 1, .groups = DEFER_FOLD_MIN_GROUPS, .rows = 1_000_000, .budget = true, .shape = .plain },
        .{ .want = false, .staged = true, .workers = 4, .groups = DEFER_FOLD_MIN_GROUPS - 1, .rows = 1_000_000, .budget = true, .shape = .plain },
        .{ .want = false, .staged = true, .workers = 4, .groups = DEFER_FOLD_MIN_GROUPS, .rows = 2 * 1024 * 1024 + 1, .budget = true, .shape = .plain },
        .{ .want = false, .staged = true, .workers = 4, .groups = DEFER_FOLD_MIN_GROUPS, .rows = 1_000_000, .budget = false, .shape = .plain },
        .{ .want = false, .staged = true, .workers = 4, .groups = DEFER_FOLD_MIN_GROUPS, .rows = 1_000_000, .budget = true, .shape = .str_payload },
        .{ .want = false, .staged = true, .workers = 4, .groups = DEFER_FOLD_MIN_GROUPS, .rows = 1_000_000, .budget = true, .shape = .distinct },
    };
    inline for (cases) |c| {
        const layout = GroupRowsLayout{
            .key_width = .u64,
            .columns = &.{},
            .aggregates = &.{.{ .op = .count_star, .state_index = 0 }},
            .has_str_payload = c.shape == .str_payload,
            .distinct_slot_count = if (c.shape == .distinct) 1 else 0,
        };
        try std.testing.expectEqual(c.want, deferBucketFolds(layout, c.workers, c.staged, c.groups, c.rows, if (c.budget) &resources else null));
    }
}

test "group completion: an empty stream waits until every scan producer is closed" {
    var shared = PipeShared{
        .allocator = std.testing.allocator,
        .buckets = &.{},
        .bucket_count = 0,
        .scan_threads = 2,
    };
    try std.testing.expect(!shared.grouping_complete());
    shared.scans_done.store(1, .release);
    try std.testing.expect(!shared.grouping_complete());
    shared.scans_done.store(2, .release);
    try std.testing.expect(shared.grouping_complete());
}

fn partial_flush_failure_fixture(allocator: Allocator) !void {
    var buckets = [_]PipeBucket{try PipeBucket.init(allocator, 16)};
    defer buckets[0].deinit(allocator);
    var local: WorkerParts = .{};
    defer local.deinit(allocator);
    var shared = PipeShared{
        .allocator = allocator,
        .buckets = &buckets,
        .bucket_count = 1,
        .scan_threads = 1,
        .group_rows_layout = .{
            .key_width = .u32,
            .columns = &.{},
            .aggregates = &.{.{ .op = .count_star, .state_index = 0 }},
        },
    };
    defer deinitRawQueues(&shared);
    shared.raw_group_queues = try allocator.alloc(GroupQueue, 1);
    shared.raw_group_queues[0] = .{};
    shared.stage_builders = try allocator.alloc(StageBucketBuilder, 1);
    shared.stage_builders[0] = .{};
    const builder = &shared.stage_builders[0];
    try builder.rows.ensureTotalCapacity(allocator, shared.group_rows_layout, 32);
    builder.rows.len_rows = 32;
    for (0..32) |i| builder.rows.setKey(i, i);
    shared.stage_builder_rows.store(32, .release);
    shared.pending_group_rows.store(32, .release);
    shared.scans_done.store(1, .release);

    const flushed = flushSharedStageBuilders(&shared, &local, false);
    try std.testing.expect(builder.lock.tryLock());
    builder.lock.unlock();
    try std.testing.expect(!shared.grouping_complete());
    try std.testing.expectEqual(@as(u64, 32), shared.pending_group_rows.load(.acquire));
    if (flushed) |did_flush| {
        try std.testing.expect(did_flush);
        try std.testing.expectEqual(@as(u64, 0), shared.stage_builder_rows.load(.acquire));
        try std.testing.expectEqual(@as(usize, 1), shared.stage_outstanding_chunks.load(.acquire));
    } else |err| {
        try std.testing.expectEqual(@as(u64, 32), shared.stage_builder_rows.load(.acquire));
        try std.testing.expectEqual(@as(usize, 32), builder.rows.len());
        try std.testing.expectEqual(@as(usize, 0), shared.stage_outstanding_chunks.load(.acquire));
        return err;
    }
}

test "group completion: failed partial publication restores ownership and unlocks the builder" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, partial_flush_failure_fixture, .{});
}

const STRING_MEMORY_TEST_LAYOUT = GroupRowsLayout{
    .key_width = .u32,
    .columns = &.{},
    .aggregates = &.{.{ .op = .min, .is_string = true, .str_input_index = 0, .str_state_index = 0, .state_index = 0 }},
    .str_columns = &.{.{ .source_name = "value" }},
    .has_str_payload = true,
};

fn string_recycle_failure_fixture(allocator: Allocator) !void {
    var shared = PipeShared{
        .allocator = allocator,
        .buckets = &.{},
        .bucket_count = 0,
        .scan_threads = 1,
        .group_rows_layout = STRING_MEMORY_TEST_LAYOUT,
        .recycle_byte_limit = 4096,
    };
    defer deinitRawQueues(&shared);
    for (0..12) |_| {
        inline for (.{ RawRows, GroupRows }) |Rows| {
            var rows: Rows = .{};
            defer rows.deinit(allocator);
            try rows.resize(allocator, STRING_MEMORY_TEST_LAYOUT, 4);
            try rows.str.bytes.ensureTotalCapacity(allocator, 1024);
            try rows.str.append(allocator, 1, 0, 0, "x");
            const owned = rows;
            rows = .{};
            if (Rows == RawRows) {
                try recycleRawRows(&shared, owned, 4, null);
            } else {
                try recycleGroupRows(&shared, owned, 4, null);
            }
            try std.testing.expect(shared.recycled_bytes <= 4096);
        }
    }
    var counted: usize = 0;
    for (shared.raw_recycled_rows.items) |rows| counted += rowBufferBytes(rows);
    for (shared.group_recycled_rows.items) |rows| counted += rowBufferBytes(rows);
    try std.testing.expectEqual(counted, shared.recycled_bytes);
    try std.testing.expect(counted > 0);

    inline for (.{ RawRows, GroupRows }) |Rows| {
        var rows = if (Rows == RawRows) try acquireRawRows(&shared, 512, null) else try acquireGroupRows(&shared, 512, null);
        defer rows.deinit(allocator);
        try std.testing.expectEqual(@as(usize, 0), rows.len());
        try std.testing.expectEqual(@as(usize, 0), rows.str.bytes.items.len);
        try rows.str.bytes.ensureTotalCapacity(allocator, 8192);
        const before = shared.recycled_bytes;
        const owned = rows;
        rows = .{};
        if (Rows == RawRows) {
            try recycleRawRows(&shared, owned, 512, null);
        } else {
            try recycleGroupRows(&shared, owned, 512, null);
        }
        try std.testing.expectEqual(before, shared.recycled_bytes);
    }
    try std.testing.expect(shared.raw_recycle_lock.tryLock());
    shared.raw_recycle_lock.unlock();
}

test "string pipeline memory: shared recycle cap includes slack and releases rejected buffers on failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, string_recycle_failure_fixture, .{});
}

test "string MIN keeps one copy per group however many rows improve it" {
    const allocator = std.testing.allocator;
    const layout = STRING_MEMORY_TEST_LAYOUT;
    const groups = 38;
    const n = 200_000;
    const pad = "x" ** 48;
    var buf: [64]u8 = undefined;
    var rows: GroupRows = .{};
    defer rows.deinit(allocator);
    try rows.resize(allocator, layout, n);
    // Descending values of wandering lengths: every row improves its group.
    for (0..n) |i| try rows.str.append(allocator, 1, i, 0, try std.fmt.bufPrint(&buf, "{d:0>9}{s}", .{ n - i, pad[0 .. (i * 7) % 49] }));

    var str_states: std.ArrayListUnmanaged(StrAccRow) = .empty;
    defer str_states.deinit(allocator);
    try str_states.appendNTimes(allocator, [_]StrAcc{.{}} ** MAX_GROUP_STR_SLOTS, groups);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var bank: StringBank = .{};
    var warm_capacity: usize = 0;
    for (0..n) |i| {
        try foldGroupStr(&str_states, &bank, arena.allocator(), i % groups, layout.aggregates, rows, i);
        if (i == n / 4) warm_capacity = arena.queryCapacity();
    }
    try std.testing.expectEqual(warm_capacity, arena.queryCapacity());
    for (str_states.items, 0..) |acc_row, g| {
        const last = n - 1 - (n - 1 + groups - g) % groups;
        try std.testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "{d:0>9}{s}", .{ n - last, pad[0 .. (last * 7) % 49] }), acc_row[0].bytes);
    }
}

test "string pipeline memory: recycle allowance leaves room in the memory budget" {
    var small = thindb.exec.memory.MemoryPool.init(32 * 1024 * 1024);
    var resources = thindb.exec.memory.MemoryAccountant.init(&small);
    try std.testing.expectEqual(@as(usize, 4 * 1024 * 1024), stringRecycleLimit(&resources));
    var large = thindb.exec.memory.MemoryPool.init(64 * 1024 * 1024 * 1024);
    resources = thindb.exec.memory.MemoryAccountant.init(&large);
    try std.testing.expectEqual(MAX_STRING_RECYCLE_BYTES, stringRecycleLimit(&resources));
    try std.testing.expectEqual(MAX_STRING_RECYCLE_BYTES, stringRecycleLimit(null));
}

fn string_stage_failure_fixture(allocator: Allocator, cancelled: bool) !void {
    var cancel = std.atomic.Value(bool).init(cancelled);
    var memory_pool = thindb.exec.memory.MemoryPool.init(1024 * 1024);
    var resources = thindb.exec.memory.MemoryAccountant.init(&memory_pool);
    resources.cancel_flag = &cancel;
    var shared = PipeShared{
        .allocator = allocator,
        .resources = &resources,
        .buckets = &.{},
        .bucket_count = 1,
        .scan_threads = 1,
        .group_rows_layout = STRING_MEMORY_TEST_LAYOUT,
        .recycle_byte_limit = 0,
    };
    defer deinitRawQueues(&shared);
    var local: WorkerParts = .{};
    defer local.deinit(allocator);
    shared.raw_scan_queues = try allocator.alloc(RawQueue, 1);
    shared.raw_scan_queues[0] = .{};
    shared.raw_group_queues = try allocator.alloc(GroupQueue, 1);
    shared.raw_group_queues[0] = .{};
    shared.stage_builders = try allocator.alloc(StageBucketBuilder, 1);
    shared.stage_builders[0] = .{};
    var source: RawRows = .{};
    defer source.deinit(allocator);
    for (0..3) |_| {
        try source.resize(allocator, STRING_MEMORY_TEST_LAYOUT, 2);
        for (0..2) |row| {
            source.setKey(row, 7);
            try source.str.append(allocator, 1, row, 0, "value");
        }
        try publishRawRowsToQueue(&shared, &shared.raw_scan_queues[0], 0, &source, 2, &shared.outstanding_chunks, &shared.pending_group_rows, null, null);
    }
    shared.scans_done.store(1, .release);
    try std.testing.expect(claimRawQueueLaneExact(shared.raw_scan_queues, 0));
    const staged = drainRawDedicatedStageSharedBuilders(allocator, &shared, &local, 0, 2, 4, 1, false);
    try std.testing.expect(!shared.raw_scan_queues[0].lease.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), shared.active_stage_jobs.load(.acquire));
    try std.testing.expect(!shared.grouping_complete());
    if (cancelled) {
        try std.testing.expectError(error.QueryCancelled, staged);
        return;
    }
    try std.testing.expect(try staged);
    try std.testing.expectEqual(@as(usize, 2), shared.raw_scan_queues[0].chunks.items.len);
    try std.testing.expectEqual(@as(u64, 2), shared.stage_builder_rows.load(.acquire));
    try std.testing.expectEqual(@as(u64, 6), shared.pending_group_rows.load(.acquire));
    while (shared.raw_scan_queues[0].chunks.items.len > 0) {
        try std.testing.expect(claimRawQueueLaneExact(shared.raw_scan_queues, 0));
        _ = try drainRawDedicatedStageSharedBuilders(allocator, &shared, &local, 0, 2, 4, 1, false);
    }
    _ = try flushSharedStageBuilders(&shared, &local, false);
    try std.testing.expectEqual(@as(u64, 6), shared.stage_outstanding_rows.load(.acquire));
    for (shared.raw_group_queues[0].chunks.items) |chunk| {
        for (0..chunk.rows.len()) |row| try std.testing.expectEqualStrings("value", chunk.rows.str.get(1, row, 0));
    }
}

test "string pipeline memory: bounded stage preserves partials and frees popped input on allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, string_stage_failure_fixture, .{false});
}

test "string pipeline memory: cancelled stage releases its lease and popped input" {
    try string_stage_failure_fixture(std.testing.allocator, true);
}

pub const RawGroupMode = enum {
    off,
    staged_final,
};

pub const WorkspaceProfile = struct {
    workers: usize = 0,

    setup_total_ticks: i64 = 0,
    setup_parts_alloc_ticks: i64 = 0,
    setup_parts_zero_ticks: i64 = 0,
    setup_buckets_alloc_ticks: i64 = 0,
    setup_flags_alloc_ticks: i64 = 0,
    setup_thread_alloc_ticks: i64 = 0,
    setup_job_alloc_ticks: i64 = 0,
    setup_spawn_ticks: i64 = 0,
    setup_join_ticks: i64 = 0,
    setup_assign_ticks: i64 = 0,
    setup_worker_part_ticks: [MAX_WORKSPACE_PROFILE_WORKERS]i64 = [_]i64{0} ** MAX_WORKSPACE_PROFILE_WORKERS,
    setup_worker_bucket_ticks: [MAX_WORKSPACE_PROFILE_WORKERS]i64 = [_]i64{0} ** MAX_WORKSPACE_PROFILE_WORKERS,
    setup_worker_total_ticks: [MAX_WORKSPACE_PROFILE_WORKERS]i64 = [_]i64{0} ** MAX_WORKSPACE_PROFILE_WORKERS,

    teardown_total_ticks: i64 = 0,
    teardown_thread_alloc_ticks: i64 = 0,
    teardown_job_alloc_ticks: i64 = 0,
    teardown_spawn_ticks: i64 = 0,
    teardown_join_ticks: i64 = 0,
    teardown_outer_free_ticks: i64 = 0,
    teardown_worker_part_ticks: [MAX_WORKSPACE_PROFILE_WORKERS]i64 = [_]i64{0} ** MAX_WORKSPACE_PROFILE_WORKERS,
    teardown_worker_bucket_ticks: [MAX_WORKSPACE_PROFILE_WORKERS]i64 = [_]i64{0} ** MAX_WORKSPACE_PROFILE_WORKERS,
    teardown_worker_total_ticks: [MAX_WORKSPACE_PROFILE_WORKERS]i64 = [_]i64{0} ** MAX_WORKSPACE_PROFILE_WORKERS,

    pub fn printSetup(self: *const WorkspaceProfile, query: []const u8, bucket_count: usize, local_reserve_per_bucket: usize, expected_groups_per_bucket: usize) void {
        const freq = platform.perfFreq();
        std.debug.print(
            "[workspace-setup-detail] query={s} total={d:.1}ms parts_alloc={d:.3}ms parts_zero={d:.3}ms buckets_alloc={d:.3}ms flags_alloc={d:.3}ms thread_alloc={d:.3}ms job_alloc={d:.3}ms spawn={d:.3}ms join={d:.1}ms assign={d:.3}ms workers={} buckets={} local_reserve={} expected_groups_per_bucket={}\n",
            .{
                query,
                platform.ticksToMs(self.setup_total_ticks, freq),
                platform.ticksToMs(self.setup_parts_alloc_ticks, freq),
                platform.ticksToMs(self.setup_parts_zero_ticks, freq),
                platform.ticksToMs(self.setup_buckets_alloc_ticks, freq),
                platform.ticksToMs(self.setup_flags_alloc_ticks, freq),
                platform.ticksToMs(self.setup_thread_alloc_ticks, freq),
                platform.ticksToMs(self.setup_job_alloc_ticks, freq),
                platform.ticksToMs(self.setup_spawn_ticks, freq),
                platform.ticksToMs(self.setup_join_ticks, freq),
                platform.ticksToMs(self.setup_assign_ticks, freq),
                self.workers,
                bucket_count,
                local_reserve_per_bucket,
                expected_groups_per_bucket,
            },
        );
        std.debug.print(
            "[workspace-setup-workers] query={s} part_sum={d:.1}ms part_max={d:.1}ms bucket_sum={d:.1}ms bucket_max={d:.1}ms total_sum={d:.1}ms total_max={d:.1}ms\n",
            .{
                query,
                platform.ticksToMs(profileSum(self.setup_worker_part_ticks[0..profileWorkerCount(self.workers)]), freq),
                platform.ticksToMs(profileMax(self.setup_worker_part_ticks[0..profileWorkerCount(self.workers)]), freq),
                platform.ticksToMs(profileSum(self.setup_worker_bucket_ticks[0..profileWorkerCount(self.workers)]), freq),
                platform.ticksToMs(profileMax(self.setup_worker_bucket_ticks[0..profileWorkerCount(self.workers)]), freq),
                platform.ticksToMs(profileSum(self.setup_worker_total_ticks[0..profileWorkerCount(self.workers)]), freq),
                platform.ticksToMs(profileMax(self.setup_worker_total_ticks[0..profileWorkerCount(self.workers)]), freq),
            },
        );
    }

    pub fn printTeardown(self: *const WorkspaceProfile, query: []const u8, bucket_count: usize) void {
        const freq = platform.perfFreq();
        std.debug.print(
            "[workspace-teardown-detail] query={s} total={d:.1}ms thread_alloc={d:.3}ms job_alloc={d:.3}ms spawn={d:.3}ms join={d:.1}ms outer_free={d:.3}ms workers={} buckets={}\n",
            .{
                query,
                platform.ticksToMs(self.teardown_total_ticks, freq),
                platform.ticksToMs(self.teardown_thread_alloc_ticks, freq),
                platform.ticksToMs(self.teardown_job_alloc_ticks, freq),
                platform.ticksToMs(self.teardown_spawn_ticks, freq),
                platform.ticksToMs(self.teardown_join_ticks, freq),
                platform.ticksToMs(self.teardown_outer_free_ticks, freq),
                self.workers,
                bucket_count,
            },
        );
        std.debug.print(
            "[workspace-teardown-workers] query={s} part_sum={d:.1}ms part_max={d:.1}ms bucket_sum={d:.1}ms bucket_max={d:.1}ms total_sum={d:.1}ms total_max={d:.1}ms\n",
            .{
                query,
                platform.ticksToMs(profileSum(self.teardown_worker_part_ticks[0..profileWorkerCount(self.workers)]), freq),
                platform.ticksToMs(profileMax(self.teardown_worker_part_ticks[0..profileWorkerCount(self.workers)]), freq),
                platform.ticksToMs(profileSum(self.teardown_worker_bucket_ticks[0..profileWorkerCount(self.workers)]), freq),
                platform.ticksToMs(profileMax(self.teardown_worker_bucket_ticks[0..profileWorkerCount(self.workers)]), freq),
                platform.ticksToMs(profileSum(self.teardown_worker_total_ticks[0..profileWorkerCount(self.workers)]), freq),
                platform.ticksToMs(profileMax(self.teardown_worker_total_ticks[0..profileWorkerCount(self.workers)]), freq),
            },
        );
    }
};

fn profileWorkerCount(workers: usize) usize {
    return @min(workers, MAX_WORKSPACE_PROFILE_WORKERS);
}

fn profileSum(values: []const i64) i64 {
    var sum: i64 = 0;
    for (values) |value| sum += value;
    return sum;
}

fn profileMax(values: []const i64) i64 {
    var max_value: i64 = 0;
    for (values) |value| {
        if (value > max_value) max_value = value;
    }
    return max_value;
}

pub const SiloGridWorkspace = struct {
    parts: []WorkerParts = &.{},
    buckets: []PipeBucket = &.{},
    n_workers: usize = 0,
    bucket_count: usize = 0,
    local_reserve_per_bucket: usize = 0,
    expected_groups_per_bucket: usize = 0,

    pub fn ensure(
        self: *SiloGridWorkspace,
        allocator: Allocator,
        n_workers: usize,
        bucket_count: usize,
        local_reserve_per_bucket: usize,
        expected_groups_per_bucket: usize,
        cpus: []const usize,
        profile: ?*WorkspaceProfile,
    ) !void {
        if (self.parts.len != n_workers or
            self.buckets.len != bucket_count or
            self.local_reserve_per_bucket != local_reserve_per_bucket or
            self.expected_groups_per_bucket != expected_groups_per_bucket)
        {
            self.deinit(allocator);
            try self.initFresh(allocator, n_workers, bucket_count, local_reserve_per_bucket, expected_groups_per_bucket, cpus, profile);
            return;
        }
        try self.resetParallel(allocator, n_workers, cpus);
    }

    pub fn deinit(self: *SiloGridWorkspace, allocator: Allocator) void {
        for (self.parts) |*p| p.deinit(allocator);
        if (self.parts.len > 0) allocator.free(self.parts);
        for (self.buckets) |*b| b.deinit(allocator);
        if (self.buckets.len > 0) allocator.free(self.buckets);
        self.* = .{};
    }

    pub fn deinitParallel(self: *SiloGridWorkspace, allocator: Allocator, n_workers: usize, cpus: []const usize, profile: ?*WorkspaceProfile) void {
        const total_t0 = if (profile != null) platform.nowTicks() else 0;
        const workers = @max(@as(usize, 1), @min(n_workers, @max(cpus.len, 1)));
        if (profile) |p| p.workers = profileWorkerCount(workers);
        if (workers == 1 or (self.parts.len + self.buckets.len) < 128) {
            self.deinit(allocator);
            if (profile) |p| p.teardown_total_ticks = platform.nowTicks() - total_t0;
            return;
        }
        const threads_alloc_t0 = if (profile != null) platform.nowTicks() else 0;
        const threads = allocator.alloc(std.Thread, workers) catch {
            self.deinit(allocator);
            if (profile) |p| p.teardown_total_ticks = platform.nowTicks() - total_t0;
            return;
        };
        if (profile) |p| p.teardown_thread_alloc_ticks = platform.nowTicks() - threads_alloc_t0;
        defer allocator.free(threads);
        const jobs_alloc_t0 = if (profile != null) platform.nowTicks() else 0;
        const jobs = allocator.alloc(WorkspaceDeinitJob, workers) catch {
            self.deinit(allocator);
            if (profile) |p| p.teardown_total_ticks = platform.nowTicks() - total_t0;
            return;
        };
        if (profile) |p| p.teardown_job_alloc_ticks = platform.nowTicks() - jobs_alloc_t0;
        defer allocator.free(jobs);

        var i: usize = 0;
        const spawn_t0 = if (profile != null) platform.nowTicks() else 0;
        while (i < workers) : (i += 1) {
            jobs[i] = .{
                .workspace = self,
                .allocator = allocator,
                .worker_index = i,
                .worker_count = workers,
                .profile = profile,
            };
            threads[i] = std.Thread.spawn(.{}, workspaceDeinitWorker, .{&jobs[i]}) catch unreachable;
        }
        if (profile) |p| p.teardown_spawn_ticks = platform.nowTicks() - spawn_t0;
        const join_t0 = if (profile != null) platform.nowTicks() else 0;
        for (threads) |thread| thread.join();
        if (profile) |p| p.teardown_join_ticks = platform.nowTicks() - join_t0;
        const free_t0 = if (profile != null) platform.nowTicks() else 0;
        if (self.parts.len > 0) allocator.free(self.parts);
        if (self.buckets.len > 0) allocator.free(self.buckets);
        if (profile) |p| {
            p.teardown_outer_free_ticks = platform.nowTicks() - free_t0;
            p.teardown_total_ticks = platform.nowTicks() - total_t0;
        }
        self.* = .{};
    }

    fn initFresh(
        self: *SiloGridWorkspace,
        allocator: Allocator,
        n_workers: usize,
        bucket_count: usize,
        local_reserve_per_bucket: usize,
        expected_groups_per_bucket: usize,
        cpus: []const usize,
        profile: ?*WorkspaceProfile,
    ) !void {
        const total_t0 = if (profile != null) platform.nowTicks() else 0;
        const parts_alloc_t0 = if (profile != null) platform.nowTicks() else 0;
        const parts = try allocator.alloc(WorkerParts, n_workers);
        if (profile) |p| p.setup_parts_alloc_ticks = platform.nowTicks() - parts_alloc_t0;
        errdefer allocator.free(parts);
        const parts_zero_t0 = if (profile != null) platform.nowTicks() else 0;
        for (parts) |*p| p.* = .{};
        if (profile) |p| p.setup_parts_zero_ticks = platform.nowTicks() - parts_zero_t0;
        errdefer {
            for (parts) |*p| p.deinit(allocator);
        }

        const buckets_alloc_t0 = if (profile != null) platform.nowTicks() else 0;
        const buckets = try allocator.alloc(PipeBucket, bucket_count);
        if (profile) |p| p.setup_buckets_alloc_ticks = platform.nowTicks() - buckets_alloc_t0;
        errdefer allocator.free(buckets);
        const flags_alloc_t0 = if (profile != null) platform.nowTicks() else 0;
        const bucket_inited = try allocator.alloc(bool, bucket_count);
        defer allocator.free(bucket_inited);
        @memset(bucket_inited, false);
        if (profile) |p| p.setup_flags_alloc_ticks = platform.nowTicks() - flags_alloc_t0;
        errdefer {
            for (buckets, bucket_inited) |*bucket, inited| {
                if (inited) bucket.deinit(allocator);
            }
        }

        try initWorkspaceFreshParallel(allocator, parts, buckets, bucket_inited, bucket_count, local_reserve_per_bucket, expected_groups_per_bucket, n_workers, cpus, profile);

        const assign_t0 = if (profile != null) platform.nowTicks() else 0;
        self.* = .{
            .parts = parts,
            .buckets = buckets,
            .n_workers = n_workers,
            .bucket_count = bucket_count,
            .local_reserve_per_bucket = local_reserve_per_bucket,
            .expected_groups_per_bucket = expected_groups_per_bucket,
        };
        if (profile) |p| {
            p.setup_assign_ticks = platform.nowTicks() - assign_t0;
            p.setup_total_ticks = platform.nowTicks() - total_t0;
        }
    }

    fn reset(self: *SiloGridWorkspace, allocator: Allocator) !void {
        for (self.parts, 0..) |*p, worker_idx| {
            resetWorkerParts(p, worker_idx);
        }
        for (self.buckets) |*bucket| {
            resetPipeBucket(bucket, allocator);
        }
    }

    fn resetParallel(self: *SiloGridWorkspace, allocator: Allocator, n_workers: usize, cpus: []const usize) !void {
        const workers = @max(@as(usize, 1), @min(n_workers, @max(cpus.len, 1)));
        if (workers == 1 or (self.parts.len + self.buckets.len) < 128) return self.reset(allocator);

        const threads = try allocator.alloc(std.Thread, workers);
        defer allocator.free(threads);
        var jobs = try allocator.alloc(WorkspaceResetJob, workers);
        defer allocator.free(jobs);

        var i: usize = 0;
        while (i < workers) : (i += 1) {
            jobs[i] = .{
                .workspace = self,
                .allocator = allocator,
                .worker_index = i,
                .worker_count = workers,
            };
            threads[i] = try std.Thread.spawn(.{}, workspaceResetWorker, .{&jobs[i]});
        }
        for (threads) |thread| thread.join();
    }
};

const WorkspaceResetJob = struct {
    workspace: *SiloGridWorkspace,
    allocator: Allocator,
    worker_index: usize,
    worker_count: usize,
};

const WorkspaceDeinitJob = struct {
    workspace: *SiloGridWorkspace,
    allocator: Allocator,
    worker_index: usize,
    worker_count: usize,
    profile: ?*WorkspaceProfile = null,
};

fn workspaceDeinitWorker(job: *WorkspaceDeinitJob) void {
    const total_t0 = if (job.profile != null) platform.nowTicks() else 0;
    const part_t0 = if (job.profile != null) platform.nowTicks() else 0;
    var part_i = job.worker_index;
    while (part_i < job.workspace.parts.len) : (part_i += job.worker_count) {
        job.workspace.parts[part_i].deinit(job.allocator);
    }
    const part_ticks = if (job.profile != null) platform.nowTicks() - part_t0 else 0;

    const bucket_t0 = if (job.profile != null) platform.nowTicks() else 0;
    const bucket_count = job.workspace.buckets.len;
    const start = job.worker_index * bucket_count / job.worker_count;
    const end = (job.worker_index + 1) * bucket_count / job.worker_count;
    var b = start;
    while (b < end) : (b += 1) {
        job.workspace.buckets[b].deinit(job.allocator);
    }
    if (job.profile) |profile| {
        if (job.worker_index < MAX_WORKSPACE_PROFILE_WORKERS) {
            profile.teardown_worker_part_ticks[job.worker_index] = part_ticks;
            profile.teardown_worker_bucket_ticks[job.worker_index] = platform.nowTicks() - bucket_t0;
            profile.teardown_worker_total_ticks[job.worker_index] = platform.nowTicks() - total_t0;
        }
    }
}

fn workspaceResetWorker(job: *WorkspaceResetJob) void {
    var part_i = job.worker_index;
    while (part_i < job.workspace.parts.len) : (part_i += job.worker_count) {
        resetWorkerParts(&job.workspace.parts[part_i], part_i);
    }

    const bucket_count = job.workspace.buckets.len;
    const start = job.worker_index * bucket_count / job.worker_count;
    const end = (job.worker_index + 1) * bucket_count / job.worker_count;
    var b = start;
    while (b < end) : (b += 1) {
        resetPipeBucket(&job.workspace.buckets[b], job.allocator);
    }
}

const WorkspaceFreshInitJob = struct {
    allocator: Allocator,
    parts: []WorkerParts,
    buckets: []PipeBucket,
    bucket_inited: []bool,
    bucket_count: usize,
    local_reserve_per_bucket: usize,
    expected_groups_per_bucket: usize,
    worker_index: usize,
    worker_count: usize,
    profile: ?*WorkspaceProfile = null,
    err: ?anyerror = null,
};

fn initWorkspaceFreshParallel(
    allocator: Allocator,
    parts: []WorkerParts,
    buckets: []PipeBucket,
    bucket_inited: []bool,
    bucket_count: usize,
    local_reserve_per_bucket: usize,
    expected_groups_per_bucket: usize,
    n_workers: usize,
    cpus: []const usize,
    profile: ?*WorkspaceProfile,
) !void {
    const workers = @max(@as(usize, 1), @min(n_workers, @max(cpus.len, 1)));
    if (profile) |p| p.workers = profileWorkerCount(workers);
    if (workers == 1) {
        const total_t0 = if (profile != null) platform.nowTicks() else 0;
        const part_t0 = if (profile != null) platform.nowTicks() else 0;
        for (parts, 0..) |*p, i| {
            p.* = try WorkerParts.init(allocator, bucket_count, local_reserve_per_bucket);
            p.worker_index = i;
        }
        const part_ticks = if (profile != null) platform.nowTicks() - part_t0 else 0;
        const bucket_t0 = if (profile != null) platform.nowTicks() else 0;
        for (buckets, 0..) |*bucket, i| {
            bucket.* = try PipeBucket.init(allocator, expected_groups_per_bucket);
            bucket_inited[i] = true;
            try bucket.chunks.ensureTotalCapacity(allocator, 8);
        }
        if (profile) |p| {
            p.setup_worker_part_ticks[0] = part_ticks;
            p.setup_worker_bucket_ticks[0] = platform.nowTicks() - bucket_t0;
            p.setup_worker_total_ticks[0] = platform.nowTicks() - total_t0;
        }
        return;
    }

    const threads_alloc_t0 = if (profile != null) platform.nowTicks() else 0;
    const threads = try allocator.alloc(std.Thread, workers);
    if (profile) |p| p.setup_thread_alloc_ticks = platform.nowTicks() - threads_alloc_t0;
    defer allocator.free(threads);
    const jobs_alloc_t0 = if (profile != null) platform.nowTicks() else 0;
    var jobs = try allocator.alloc(WorkspaceFreshInitJob, workers);
    if (profile) |p| p.setup_job_alloc_ticks = platform.nowTicks() - jobs_alloc_t0;
    defer allocator.free(jobs);

    var i: usize = 0;
    const spawn_t0 = if (profile != null) platform.nowTicks() else 0;
    while (i < workers) : (i += 1) {
        jobs[i] = .{
            .allocator = allocator,
            .parts = parts,
            .buckets = buckets,
            .bucket_inited = bucket_inited,
            .bucket_count = bucket_count,
            .local_reserve_per_bucket = local_reserve_per_bucket,
            .expected_groups_per_bucket = expected_groups_per_bucket,
            .worker_index = i,
            .worker_count = workers,
            .profile = profile,
        };
        threads[i] = try std.Thread.spawn(.{}, workspaceFreshInitWorker, .{&jobs[i]});
    }
    if (profile) |p| p.setup_spawn_ticks = platform.nowTicks() - spawn_t0;
    const join_t0 = if (profile != null) platform.nowTicks() else 0;
    for (threads) |thread| thread.join();
    if (profile) |p| p.setup_join_ticks = platform.nowTicks() - join_t0;
    for (jobs) |job| if (job.err) |err| return err;
}

fn workspaceFreshInitWorker(job: *WorkspaceFreshInitJob) void {
    const total_t0 = if (job.profile != null) platform.nowTicks() else 0;

    const part_t0 = if (job.profile != null) platform.nowTicks() else 0;
    if (job.worker_index < job.parts.len) {
        job.parts[job.worker_index] = WorkerParts.init(job.allocator, job.bucket_count, job.local_reserve_per_bucket) catch |err| {
            job.err = err;
            return;
        };
        job.parts[job.worker_index].worker_index = job.worker_index;
    }
    const part_ticks = if (job.profile != null) platform.nowTicks() - part_t0 else 0;

    const bucket_t0 = if (job.profile != null) platform.nowTicks() else 0;
    const bucket_total = job.buckets.len;
    const start = job.worker_index * bucket_total / job.worker_count;
    const end = (job.worker_index + 1) * bucket_total / job.worker_count;
    var b = start;
    while (b < end) : (b += 1) {
        job.buckets[b] = PipeBucket.init(job.allocator, job.expected_groups_per_bucket) catch |err| {
            job.err = err;
            return;
        };
        job.bucket_inited[b] = true;
        job.buckets[b].chunks.ensureTotalCapacity(job.allocator, 8) catch |err| {
            job.err = err;
            return;
        };
    }
    if (job.profile) |profile| {
        if (job.worker_index < MAX_WORKSPACE_PROFILE_WORKERS) {
            profile.setup_worker_part_ticks[job.worker_index] = part_ticks;
            profile.setup_worker_bucket_ticks[job.worker_index] = platform.nowTicks() - bucket_t0;
            profile.setup_worker_total_ticks[job.worker_index] = platform.nowTicks() - total_t0;
        }
    }
}

fn resetWorkerParts(parts: *WorkerParts, worker_index: usize) void {
    parts.shared_buffers = null;
    parts.shared_bank_index = 0;
    parts.flat_scan_partitions = false;
    parts.flat_partitioned = false;
    parts.flat_raw_rows.clearRetainingCapacity();
    parts.flat_bucket_ids.clearRetainingCapacity();
    parts.raw_active_rows.clearRetainingCapacity();
    if (parts.flat_counts.len > 0) @memset(parts.flat_counts, 0);
    if (parts.flat_offsets.len > 0) @memset(parts.flat_offsets, 0);
    if (parts.flat_next.len > 0) @memset(parts.flat_next, 0);
    parts.worker_index = worker_index;
    parts.raw_scan_lane = worker_index;
    parts.scanned_count = 0;
    parts.row_count = 0;
    parts.local_buffered_rows = 0;
    parts.published_chunks = 0;
    parts.scan_ticks = 0;
    parts.scan_reset_ticks = 0;
    parts.scan_tiles = 0;
    parts.scan_quanta = 0;
    parts.scan_batches = 0;
    parts.partition_ticks = 0;
    parts.publish_ticks = 0;
    parts.group_ticks = 0;
    parts.sched_decision_ticks = 0;
    parts.sched_scan_claim_ticks = 0;
    parts.sched_group_pick_ticks = 0;
    parts.sched_group_lock_ticks = 0;
    parts.raw_queue_lock_ticks = 0;
    parts.raw_scan_queue_lock_ticks = 0;
    parts.raw_group_queue_lock_ticks = 0;
    parts.raw_recycle_lock_ticks = 0;
    parts.raw_agg_lock_ticks = 0;
    parts.raw_stage_builder_lock_ticks = 0;
    parts.raw_stage_ticks = 0;
    parts.raw_stage_pop_ticks = 0;
    parts.raw_stage_slice_ticks = 0;
    parts.raw_stage_publish_ticks = 0;
    parts.raw_stage_recycle_ticks = 0;
    parts.raw_stage_input_chunks = 0;
    parts.raw_stage_buffered_rows = 0;
    parts.sched_loops = 0;
    parts.sched_scan_jobs = 0;
    parts.sched_stage_jobs = 0;
    parts.sched_group_jobs = 0;
    parts.sched_group_misses = 0;
    parts.sched_idle_loops = 0;
    parts.dirty_buckets.clearRetainingCapacity();
    if (parts.dirty_marks.len > 0) @memset(parts.dirty_marks, false);
    if (parts.route_counts.len > 0) @memset(parts.route_counts, 0);
    if (parts.route_offsets.len > 0) @memset(parts.route_offsets, 0);
    parts.route_touched.clearRetainingCapacity();
}

fn resetPipeBucket(bucket: *PipeBucket, allocator: Allocator) void {
    bucket.chunks.clearRetainingCapacity();
    bucket.queued_rows = 0;
    bucket.row_count = 0;
    bucket.observed_input_rows = 0;
    bucket.allocation_profile = .{};
    bucket.queue_lock = .unlocked;
    bucket.agg_lock = .unlocked;
    bucket.table.clearRetainingCapacity();
    bucket.states.clearRetainingCapacity();
    bucket.freeStrBytes();
    bucket.str_states.clearRetainingCapacity();
    bucket.concat_states.clearRetainingCapacity();
    // UDAF states are destroyed synchronously after emit (while the layout is
    // live); here the blobs are stale, so just drop them and reclaim the arena.
    bucket.udf_states.clearRetainingCapacity();
    _ = bucket.udf_arena.reset(.retain_capacity);
    bucket.distinct_sets.clear(allocator);
}

fn deinitRawQueues(shared: *PipeShared) void {
    var task = HeavyTeardownTask{
        .allocator = shared.allocator,
        .raw_chunks = shared.raw_chunks,
        .raw_scan_queues = shared.raw_scan_queues,
        .raw_group_queues = shared.raw_group_queues,
        .stage_builders = shared.stage_builders,
        .raw_recycled_rows = shared.raw_recycled_rows,
        .group_recycled_rows = shared.group_recycled_rows,
    };
    task.run();
}

// The heavy post-result frees of a silo-grid run: every staging chunk slab the
// run touched. Moved out of `PipeShared` so they can be released on a detached
// thread after the result has shipped (~hundreds of MB to GB of slab frees,
// ~200ms on a 100M-row high-card query).
const HeavyTeardownTask = struct {
    allocator: Allocator,
    raw_chunks: std.ArrayListUnmanaged(RawChunk),
    raw_scan_queues: []RawQueue,
    raw_group_queues: []GroupQueue,
    stage_builders: []StageBucketBuilder,
    raw_recycled_rows: std.ArrayListUnmanaged(RawRows),
    group_recycled_rows: std.ArrayListUnmanaged(GroupRows),

    fn run(self: *HeavyTeardownTask) void {
        const allocator = self.allocator;
        for (self.raw_chunks.items) |*chunk| chunk.rows.deinit(allocator);
        self.raw_chunks.deinit(allocator);
        for (self.raw_scan_queues) |*queue| queue.deinit(allocator);
        if (self.raw_scan_queues.len > 0) allocator.free(self.raw_scan_queues);
        for (self.raw_group_queues) |*queue| queue.deinit(allocator);
        if (self.raw_group_queues.len > 0) allocator.free(self.raw_group_queues);
        for (self.stage_builders) |*builder| builder.deinit(allocator);
        if (self.stage_builders.len > 0) allocator.free(self.stage_builders);
        for (self.raw_recycled_rows.items) |*rows| rows.deinit(allocator);
        self.raw_recycled_rows.deinit(allocator);
        for (self.group_recycled_rows.items) |*rows| rows.deinit(allocator);
        self.group_recycled_rows.deinit(allocator);
    }
};

fn heavyTeardownMain(task: *HeavyTeardownTask) void {
    task.run();
    const allocator = task.allocator;
    allocator.destroy(task);
}

// Take the queue/recycle state off the result path and zero it in `shared`
// so the unwind path has nothing left to free. A tracked allocator frees it
// here with the frees collected, so none of it counts against the budget once
// this returns, and one detached thread returns the memory (spread over the
// workers, the releases slowed the next query in A/B runs). Otherwise the
// whole teardown moves to a detached thread, and a failed spawn falls back to
// freeing synchronously via the regular defer (returns false).
fn scheduleHeavyTeardown(shared: *PipeShared) bool {
    if (thindb.exec.memory.accountantOf(shared.allocator) != null) {
        var frees: thindb.exec.memory.DeferredFrees = .{};
        frees.collect();
        deinitRawQueues(shared);
        frees.stop();
        frees.releaseDetached();
        forgetRawQueues(shared);
        return true;
    }
    const task = shared.allocator.create(HeavyTeardownTask) catch return false;
    task.* = .{
        .allocator = shared.allocator,
        .raw_chunks = shared.raw_chunks,
        .raw_scan_queues = shared.raw_scan_queues,
        .raw_group_queues = shared.raw_group_queues,
        .stage_builders = shared.stage_builders,
        .raw_recycled_rows = shared.raw_recycled_rows,
        .group_recycled_rows = shared.group_recycled_rows,
    };
    const thread = std.Thread.spawn(.{}, heavyTeardownMain, .{task}) catch {
        shared.allocator.destroy(task);
        return false;
    };
    thread.detach();
    forgetRawQueues(shared);
    return true;
}

fn forgetRawQueues(shared: *PipeShared) void {
    shared.raw_chunks = .empty;
    shared.raw_scan_queues = &.{};
    shared.raw_group_queues = &.{};
    shared.stage_builders = &.{};
    shared.raw_recycled_rows = .empty;
    shared.group_recycled_rows = .empty;
    shared.recycled_bytes = 0;
}

test "heavy teardown stops charging the query before its memory is back" {
    const a = std.testing.allocator;
    const memory = thindb.exec.memory;
    var gate = @import("../util/statement_gate.zig").StatementGate.init(a, std.testing.io);
    defer gate.deinit();
    var pool = memory.MemoryPool.init(64 << 20);
    const account = try a.create(memory.MemoryAccountant);
    account.* = memory.MemoryAccountant.init(&pool);
    account.trackAllocations(a);
    try account.retainGate(&gate);
    const allocator = try account.executionAllocator();
    const layout: GroupRowsLayout = .{
        .key_width = .u32,
        .columns = &.{},
        .aggregates = &.{.{ .op = .count_star, .state_index = 0 }},
        .has_weight = true,
    };
    var shared = PipeShared{
        .allocator = allocator,
        .buckets = &.{},
        .bucket_count = 0,
        .scan_threads = 4,
        .group_rows_layout = layout,
    };
    shared.raw_scan_queues = try allocator.alloc(RawQueue, 4);
    for (shared.raw_scan_queues) |*queue| queue.* = .{};
    shared.raw_group_queues = try allocator.alloc(GroupQueue, 4);
    for (shared.raw_group_queues) |*queue| queue.* = .{};
    shared.stage_builders = try allocator.alloc(StageBucketBuilder, 4);
    for (shared.stage_builders) |*builder| builder.* = .{};
    for (0..32) |i| {
        const chunk = try shared.raw_chunks.addOne(allocator);
        chunk.* = .{ .rows = .{}, .owner_worker = i % 4 };
        try chunk.rows.resize(allocator, layout, 4096);
    }
    for (0..8) |_| {
        const rows = try shared.raw_recycled_rows.addOne(allocator);
        rows.* = .{};
        try rows.resize(allocator, layout, 4096);
    }

    try std.testing.expect(scheduleHeavyTeardown(&shared));
    const charged_on_return = account.current_bytes;
    account.releaseOwner(a);
    gate.beginClose();
    try std.testing.expectEqual(@as(usize, 0), charged_on_return);
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());
    try std.testing.expectEqual(@as(usize, 0), shared.raw_chunks.items.len);
}

fn lockSpin(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn rowBufferBytes(rows: anytype) usize {
    return rows.slab.len + rows.str.refs.len * @sizeOf(StrRef) + rows.str.bytes.capacity;
}

fn stringRecycleLimit(resources: ?*const thindb.exec.memory.MemoryAccountant) usize {
    var limit = MAX_STRING_RECYCLE_BYTES;
    if (resources) |a| limit = @min(limit, a.pool.budget / 8);
    return limit;
}

fn acquireRawRows(shared: *PipeShared, reserve_rows: usize, lock_ticks: ?*i64) !RawRows {
    const lock_t0 = if (lock_ticks != null) platform.nowTicks() else 0;
    lockSpin(&shared.raw_recycle_lock);
    if (lock_ticks) |ticks| ticks.* += platform.nowTicks() - lock_t0;
    if (shared.raw_recycled_rows.items.len > 0) {
        const idx = shared.raw_recycled_rows.items.len - 1;
        var rows = shared.raw_recycled_rows.items[idx];
        shared.raw_recycled_rows.items.len = idx;
        if (shared.recycle_byte_limit != null) shared.recycled_bytes -= rowBufferBytes(rows);
        shared.raw_recycle_lock.unlock();
        errdefer rows.deinit(shared.allocator);
        rows.clearRetainingCapacity();
        if (rows.capacity() < reserve_rows or !sameRowsLayout(rows.layout, shared.group_rows_layout)) {
            try rows.ensureTotalCapacity(shared.allocator, shared.group_rows_layout, reserve_rows);
        }
        return rows;
    }
    shared.raw_recycle_lock.unlock();
    var rows = RawRows{ .layout = shared.group_rows_layout };
    errdefer rows.deinit(shared.allocator);
    if (reserve_rows > 0) try rows.ensureTotalCapacity(shared.allocator, shared.group_rows_layout, reserve_rows);
    return rows;
}

fn recycleRawRows(shared: *PipeShared, rows_raw: RawRows, reserve_rows: usize, lock_ticks: ?*i64) !void {
    var rows = rows_raw;
    errdefer rows.deinit(shared.allocator);
    rows.clearRetainingCapacity();
    if (rows.capacity() < reserve_rows or !sameRowsLayout(rows.layout, shared.group_rows_layout)) {
        try rows.ensureTotalCapacity(shared.allocator, shared.group_rows_layout, reserve_rows);
    }
    const lock_t0 = if (lock_ticks != null) platform.nowTicks() else 0;
    lockSpin(&shared.raw_recycle_lock);
    if (lock_ticks) |ticks| ticks.* += platform.nowTicks() - lock_t0;
    if (shared.recycle_byte_limit) |limit| {
        if (rowBufferBytes(rows) > limit - shared.recycled_bytes) {
            shared.raw_recycle_lock.unlock();
            rows.deinit(shared.allocator);
            return;
        }
    }
    defer shared.raw_recycle_lock.unlock();
    try shared.raw_recycled_rows.append(shared.allocator, rows);
    if (shared.recycle_byte_limit != null) shared.recycled_bytes += rowBufferBytes(rows);
}

fn acquireGroupRows(shared: *PipeShared, reserve_rows: usize, lock_ticks: ?*i64) !GroupRows {
    const lock_t0 = if (lock_ticks != null) platform.nowTicks() else 0;
    lockSpin(&shared.raw_recycle_lock);
    if (lock_ticks) |ticks| ticks.* += platform.nowTicks() - lock_t0;
    if (shared.group_recycled_rows.items.len > 0) {
        const idx = shared.group_recycled_rows.items.len - 1;
        var rows = shared.group_recycled_rows.items[idx];
        shared.group_recycled_rows.items.len = idx;
        if (shared.recycle_byte_limit != null) shared.recycled_bytes -= rowBufferBytes(rows);
        shared.raw_recycle_lock.unlock();
        errdefer rows.deinit(shared.allocator);
        rows.clearRetainingCapacity();
        if (rows.capacity() < reserve_rows) try rows.ensureTotalCapacity(shared.allocator, shared.group_rows_layout, reserve_rows);
        return rows;
    }
    shared.raw_recycle_lock.unlock();
    var rows = GroupRows{ .layout = shared.group_rows_layout };
    errdefer rows.deinit(shared.allocator);
    if (reserve_rows > 0) try rows.ensureTotalCapacity(shared.allocator, shared.group_rows_layout, reserve_rows);
    return rows;
}

fn recycleGroupRows(shared: *PipeShared, rows_group: GroupRows, reserve_rows: usize, lock_ticks: ?*i64) !void {
    var rows = rows_group;
    errdefer rows.deinit(shared.allocator);
    rows.clearRetainingCapacity();
    if (rows.capacity() < reserve_rows) try rows.ensureTotalCapacity(shared.allocator, shared.group_rows_layout, reserve_rows);
    const lock_t0 = if (lock_ticks != null) platform.nowTicks() else 0;
    lockSpin(&shared.raw_recycle_lock);
    if (lock_ticks) |ticks| ticks.* += platform.nowTicks() - lock_t0;
    if (shared.recycle_byte_limit) |limit| {
        if (rowBufferBytes(rows) > limit - shared.recycled_bytes) {
            shared.raw_recycle_lock.unlock();
            rows.deinit(shared.allocator);
            return;
        }
    }
    defer shared.raw_recycle_lock.unlock();
    try shared.group_recycled_rows.append(shared.allocator, rows);
    if (shared.recycle_byte_limit != null) shared.recycled_bytes += rowBufferBytes(rows);
}

fn publishRawRows(shared: *PipeShared, owner_worker: usize, rows_ptr: *RawRows, reserve_rows: usize, queue_lock_ticks: ?*i64, recycle_lock_ticks: ?*i64) !void {
    if (rows_ptr.len() == 0) return;
    const rows = rows_ptr.*;
    chunkProfileRecord(rows);
    const row_count: u64 = @intCast(rows.len());
    rows_ptr.* = try acquireRawRows(shared, reserve_rows, recycle_lock_ticks);
    errdefer {
        rows_ptr.deinit(shared.allocator);
        rows_ptr.* = rows;
    }

    _ = shared.outstanding_chunks.fetchAdd(1, .release);
    _ = shared.pending_group_rows.fetchAdd(row_count, .release);
    const lock_t0 = if (queue_lock_ticks != null) platform.nowTicks() else 0;
    lockSpin(&shared.raw_queue_lock);
    if (queue_lock_ticks) |ticks| ticks.* += platform.nowTicks() - lock_t0;
    errdefer {
        _ = shared.outstanding_chunks.fetchSub(1, .release);
        _ = shared.pending_group_rows.fetchSub(row_count, .release);
        shared.raw_queue_lock.unlock();
    }
    try shared.raw_chunks.append(shared.allocator, .{ .rows = rows, .owner_worker = owner_worker });
    shared.raw_queue_lock.unlock();
}

fn publishRawRowsToQueue(
    shared: *PipeShared,
    queue: *RawQueue,
    owner_worker: usize,
    rows_ptr: *RawRows,
    reserve_rows: usize,
    chunks_counter: *std.atomic.Value(usize),
    rows_counter: *std.atomic.Value(u64),
    queue_lock_ticks: ?*i64,
    recycle_lock_ticks: ?*i64,
) !void {
    if (rows_ptr.len() == 0) return;
    const rows = rows_ptr.*;
    chunkProfileRecord(rows);
    const row_count: u64 = @intCast(rows.len());
    rows_ptr.* = try acquireRawRows(shared, reserve_rows, recycle_lock_ticks);
    errdefer {
        rows_ptr.deinit(shared.allocator);
        rows_ptr.* = rows;
    }

    _ = chunks_counter.fetchAdd(1, .release);
    _ = rows_counter.fetchAdd(row_count, .release);
    const lock_t0 = if (queue_lock_ticks != null) platform.nowTicks() else 0;
    lockSpin(&queue.lock);
    if (queue_lock_ticks) |ticks| ticks.* += platform.nowTicks() - lock_t0;
    errdefer {
        _ = chunks_counter.fetchSub(1, .release);
        _ = rows_counter.fetchSub(row_count, .release);
        queue.lock.unlock();
    }
    try queue.chunks.append(shared.allocator, .{ .rows = rows, .owner_worker = owner_worker });
    queue.queued_rows += row_count;
    _ = queue.queued_rows_atomic.fetchAdd(row_count, .release);
    _ = queue.queued_chunks_atomic.fetchAdd(1, .release);
    queue.lock.unlock();
    shared.signalWork(1);
}

fn publishGroupChunkToQueue(
    shared: *PipeShared,
    queue: *GroupQueue,
    chunk: GroupChunk,
    chunks_counter: *std.atomic.Value(usize),
    rows_counter: *std.atomic.Value(u64),
    queue_lock_ticks: ?*i64,
) !void {
    const row_count: u64 = @intCast(chunk.rows.len());
    _ = chunks_counter.fetchAdd(1, .release);
    _ = rows_counter.fetchAdd(row_count, .release);
    const lock_t0 = if (queue_lock_ticks != null) platform.nowTicks() else 0;
    lockSpin(&queue.lock);
    if (queue_lock_ticks) |ticks| ticks.* += platform.nowTicks() - lock_t0;
    errdefer {
        _ = chunks_counter.fetchSub(1, .release);
        _ = rows_counter.fetchSub(row_count, .release);
        queue.lock.unlock();
    }
    try queue.chunks.append(shared.allocator, chunk);
    queue.queued_rows += row_count;
    _ = queue.queued_rows_atomic.fetchAdd(row_count, .release);
    _ = queue.queued_chunks_atomic.fetchAdd(1, .release);
    queue.lock.unlock();
    shared.signalWork(1);
}

fn popRawChunkBatchFromQueue(queue: *RawQueue, out: *[MAX_RAW_BATCH_CHUNKS]RawChunk, max_chunks_raw: usize, queue_lock_ticks: ?*i64) usize {
    const max_chunks = @max(@as(usize, 1), @min(max_chunks_raw, MAX_RAW_BATCH_CHUNKS));
    const lock_t0 = if (queue_lock_ticks != null) platform.nowTicks() else 0;
    lockSpin(&queue.lock);
    if (queue_lock_ticks) |ticks| ticks.* += platform.nowTicks() - lock_t0;
    defer queue.lock.unlock();

    var popped: usize = 0;
    while (popped < max_chunks and queue.chunks.items.len > 0) : (popped += 1) {
        const idx = queue.chunks.items.len - 1;
        const chunk = queue.chunks.items[idx];
        out[popped] = chunk;
        queue.chunks.items.len = idx;
        queue.queued_rows -= chunk.rows.len();
        _ = queue.queued_rows_atomic.fetchSub(@intCast(chunk.rows.len()), .release);
        _ = queue.queued_chunks_atomic.fetchSub(1, .release);
    }
    return popped;
}

fn popGroupChunkBatchFromQueue(queue: *GroupQueue, out: *[MAX_RAW_BATCH_CHUNKS]GroupChunk, max_chunks_raw: usize, queue_lock_ticks: ?*i64) usize {
    const max_chunks = @max(@as(usize, 1), @min(max_chunks_raw, MAX_RAW_BATCH_CHUNKS));
    const lock_t0 = if (queue_lock_ticks != null) platform.nowTicks() else 0;
    lockSpin(&queue.lock);
    if (queue_lock_ticks) |ticks| ticks.* += platform.nowTicks() - lock_t0;
    defer queue.lock.unlock();

    var popped: usize = 0;
    while (popped < max_chunks and queue.chunks.items.len > 0) : (popped += 1) {
        const idx = queue.chunks.items.len - 1;
        const chunk = queue.chunks.items[idx];
        out[popped] = chunk;
        queue.chunks.items.len = idx;
        queue.queued_rows -= chunk.rows.len();
        _ = queue.queued_rows_atomic.fetchSub(@intCast(chunk.rows.len()), .release);
        _ = queue.queued_chunks_atomic.fetchSub(1, .release);
    }
    return popped;
}

const RawLaneChoice = struct {
    lane: usize,
    rows: u64,
};

fn chooseRawScanPublishLane(shared: *PipeShared, preferred_raw: usize, close_rows_raw: usize) usize {
    if (shared.raw_scan_queues.len == 0) return 0;
    const preferred = preferred_raw % shared.raw_scan_queues.len;
    const preferred_rows = shared.raw_scan_queues[preferred].queued_rows_atomic.load(.acquire);
    const close_rows: u64 = @intCast(close_rows_raw);

    var best_lane = preferred;
    var best_rows = preferred_rows;
    var lane: usize = 0;
    while (lane < shared.raw_scan_queues.len) : (lane += 1) {
        const rows = shared.raw_scan_queues[lane].queued_rows_atomic.load(.acquire);
        if (rows < best_rows) {
            best_rows = rows;
            best_lane = lane;
        }
    }

    if (preferred_rows <= best_rows + close_rows) return preferred;
    return best_lane;
}

fn minUnlockedScanLane(queues: []RawQueue, start_index: usize) ?RawLaneChoice {
    if (queues.len == 0) return null;
    var best: ?RawLaneChoice = null;
    var checked: usize = 0;
    var cursor = start_index % queues.len;
    while (checked < queues.len) : (checked += 1) {
        const lane = cursor;
        cursor += 1;
        if (cursor == queues.len) cursor = 0;
        if (queues[lane].scan_lease.load(.acquire)) continue;
        const rows = queues[lane].queued_rows_atomic.load(.acquire);
        if (best == null or rows < best.?.rows) best = .{ .lane = lane, .rows = rows };
    }
    return best;
}

fn maxUnlockedQueueLaneRows(queues: anytype, start_index: usize) ?RawLaneChoice {
    if (queues.len == 0) return null;
    var best: ?RawLaneChoice = null;
    var checked: usize = 0;
    var cursor = start_index % queues.len;
    while (checked < queues.len) : (checked += 1) {
        const lane = cursor;
        cursor += 1;
        if (cursor == queues.len) cursor = 0;
        if (queues[lane].lease.load(.acquire)) continue;
        const rows = queues[lane].queued_rows_atomic.load(.acquire);
        if (rows == 0) continue;
        if (best == null or rows > best.?.rows) best = .{ .lane = lane, .rows = rows };
    }
    return best;
}

fn claimRawQueueLaneExact(queues: anytype, lane: usize) bool {
    if (lane >= queues.len) return false;
    if (queues[lane].queued_rows_atomic.load(.acquire) == 0) return false;
    return queues[lane].lease.cmpxchgWeak(false, true, .acq_rel, .acquire) == null;
}

fn claimRawScanLaneExact(queues: []RawQueue, lane: usize) bool {
    if (lane >= queues.len) return false;
    return queues[lane].scan_lease.cmpxchgWeak(false, true, .acq_rel, .acquire) == null;
}

fn releaseRawScanLane(queues: []RawQueue, lane: usize) void {
    if (lane < queues.len) queues[lane].scan_lease.store(false, .release);
}

fn releaseRawQueueLane(queues: anytype, lane: usize) void {
    if (lane < queues.len) queues[lane].lease.store(false, .release);
}

pub const TopRow = struct {
    key: u128,
    count: u64 = 0,
    // Generic per-aggregate accumulators, indexed by `state_index - 1`.
    // Interpreted by the aggregate program at emit. This is the transient
    // result row (bounded by the top-N heap or the all-groups emit), so it
    // carries the slots inline up to the generous MAX_GROUP_AGG_SLOTS ceiling
    // rather than allocating per row — the per-group store (StateSlab) is the
    // one sized to exactly the program's slot count.
    slots: [MAX_GROUP_AGG_SLOTS]i64 = [_]i64{0} ** MAX_GROUP_AGG_SLOTS,
    rowref: i64 = 0,
    // String MIN/MAX results, indexed by the aggregate's str_state_index. Empty
    // for numeric queries. Borrows the bucket's str_states bytes during the
    // top-N merge, then re-dup'd into the result allocator at the final emit.
    str: [MAX_GROUP_STR_SLOTS][]const u8 = [_][]const u8{&.{}} ** MAX_GROUP_STR_SLOTS,
    // False when the group's string inputs were all NULL (the accumulator
    // never saw a value) — the emit outputs NULL, distinct from a valid ''.
    str_present: [MAX_GROUP_STR_SLOTS]bool = [_]bool{true} ** MAX_GROUP_STR_SLOTS,
};

// Optional per-group predicate applied during the all-groups emit (a HAVING
// filter for a capped unordered LIMIT). Evaluated on the numeric TopRow before
// the string key is dup'd, so failing groups cost nothing; only passing groups
// count toward the emit cap. The caller (the pipeline) owns the context.
pub const EmitFilter = struct {
    ctx: ?*anyopaque,
    pass: *const fn (?*anyopaque, TopRow) bool,
};

// The caller's final ORDER BY for an all-groups emit followed by a LIMIT the
// in-core count-desc top-N can't serve. Unless `ties` is set, `before` must be
// a total order (ties broken on the group key), so the overall best `keep`
// groups are among the best `keep` of any partition of the groups, and the
// emit returns only those of each fill worker's bucket range. The caller (the
// pipeline) owns the context and still takes its final top rows from what the
// emit returns.
pub const EmitRank = struct {
    ctx: ?*anyopaque,
    before: *const fn (?*anyopaque, TopRow, TopRow) bool,
    keep: usize,
    // `before` orders only a prefix of the result order (the rest needs key
    // bytes the core doesn't hold), so groups may tie on it. The emit then
    // returns every group no worse than the keep-th best, ties at the cut
    // included, for the caller to finish the order on.
    ties: bool = false,
};

const ALL_GROUPS_FILL_ROWS_PER_WORKER: usize = 65536;

fn bucketGroupOffsets(allocator: Allocator, buckets: []PipeBucket) ![]usize {
    const cum = try allocator.alloc(usize, buckets.len + 1);
    cum[0] = 0;
    for (buckets, 0..) |*b, i| cum[i + 1] = cum[i] + b.states.len;
    return cum;
}

fn fillWorkerCount(n_workers: usize, total_groups: usize) usize {
    return @max(1, @min(n_workers, total_groups / ALL_GROUPS_FILL_ROWS_PER_WORKER));
}

// Ends fill worker `w` (1-based) of `workers` where the running group total
// `cum` reaches its equal share, so each worker's bucket range holds about the
// same number of groups.
fn bucketRangeEnd(cum: []const usize, b_start: usize, w: usize, workers: usize) usize {
    const bucket_count = cum.len - 1;
    if (w == workers) return bucket_count;
    const target = cum[bucket_count] * w / workers;
    var b_end = b_start;
    while (b_end < bucket_count and cum[b_end] < target) b_end += 1;
    return b_end;
}

/// Numeric all-groups result (no string/UDAF payload, no cap, no HAVING
/// filter): one append sized to the group total, filled from the bucket
/// records by up to `n_workers` threads over disjoint bucket ranges. The
/// per-row `append` this replaces spent most of a multi-million-group emit
/// on one thread, growing the list and copying 200-byte rows.
fn emitAllGroupsNumeric(allocator: Allocator, buckets: []PipeBucket, out: *std.ArrayListUnmanaged(TopRow), n_workers: usize) !void {
    const cum = try bucketGroupOffsets(allocator, buckets);
    defer allocator.free(cum);
    const total = cum[buckets.len];
    if (total == 0) return;
    try out.ensureTotalCapacityPrecise(allocator, out.items.len + total);
    const dst = out.items.ptr[out.items.len .. out.items.len + total];
    out.items.len += total;
    const workers = fillWorkerCount(n_workers, total);
    if (workers <= 1) {
        fillAllGroupRows(buckets, dst);
        return;
    }
    const jobs = try allocator.alloc(AllGroupsFillJob, workers);
    defer allocator.free(jobs);
    const threads = try allocator.alloc(?std.Thread, workers);
    defer allocator.free(threads);
    var b_start: usize = 0;
    for (jobs, 1..) |*job, w| {
        const b_end = bucketRangeEnd(cum, b_start, w, workers);
        job.* = .{ .buckets = buckets[b_start..b_end], .dst = dst[cum[b_start]..cum[b_end]] };
        b_start = b_end;
    }
    for (jobs, threads) |*job, *t| {
        t.* = std.Thread.spawn(.{}, allGroupsFillWorker, .{job}) catch null;
        if (t.* == null) allGroupsFillWorker(job);
    }
    for (threads) |t| if (t) |th| th.join();
}

const AllGroupsFillJob = struct { buckets: []PipeBucket, dst: []TopRow };

fn allGroupsFillWorker(job: *AllGroupsFillJob) void {
    fillAllGroupRows(job.buckets, job.dst);
}

fn fillAllGroupRows(buckets: []PipeBucket, dst: []TopRow) void {
    var pos: usize = 0;
    for (buckets) |*bucket| {
        var gid: usize = 0;
        while (gid < bucket.states.len) : (gid += 1) {
            dst[pos] = topRowFromState(bucket.states.ref(gid));
            pos += 1;
        }
    }
    std.debug.assert(pos == dst.len);
}

/// Ranked result (no UDAF or GROUP_CONCAT payload, no cap): up to `n_workers`
/// threads each keep the best `rank.keep` groups of a disjoint bucket range
/// that pass `filter`, so only workers × keep rows reach the caller's final
/// selection, which would otherwise walk every group on one thread. With
/// `rank.ties` those rows only fix the cut, and a second sweep returns every
/// group no worse than it.
fn emitRankedGroups(allocator: Allocator, buckets: []PipeBucket, out: *std.ArrayListUnmanaged(TopRow), n_workers: usize, filter: ?EmitFilter, rank: EmitRank, comptime with_str: bool) !void {
    const cum = try bucketGroupOffsets(allocator, buckets);
    defer allocator.free(cum);
    const total = cum[buckets.len];
    if (total == 0 or rank.keep == 0) return;
    const workers = fillWorkerCount(n_workers, total);
    const jobs = try allocator.alloc(RankedFillJob, workers);
    defer allocator.free(jobs);
    var jobs_ready: usize = 0;
    defer for (jobs[0..jobs_ready]) |*job| allocator.free(job.set.items);
    var b_start: usize = 0;
    for (jobs, 1..) |*job, w| {
        const b_end = bucketRangeEnd(cum, b_start, w, workers);
        const items = try allocator.alloc(TopRow, @min(rank.keep, cum[b_end] - cum[b_start]));
        job.* = .{ .buckets = buckets[b_start..b_end], .filter = filter, .set = .{ .items = items, .ctx = rank.ctx, .before = rank.before } };
        jobs_ready += 1;
        b_start = b_end;
    }
    try runRankedJobs(allocator, jobs, rankedFillWorker, with_str);
    var kept: usize = 0;
    for (jobs) |job| kept += job.set.len;
    // Under `keep` rows kept means every worker kept all of its groups.
    if (!rank.ties or kept < rank.keep) {
        try out.ensureUnusedCapacity(allocator, kept);
        for (jobs) |job| {
            for (job.set.items[0..job.set.len]) |row| try appendRankedRow(allocator, out, row, with_str);
        }
        return;
    }

    // The cut is the keep-th best kept row. A group strictly before it was
    // kept by its worker (else that worker alone kept `keep` rows before it,
    // and the cut would be better still); a group tied with it may have lost
    // its worker's tie, so the sweep below takes every group no worse.
    const cut_items = try allocator.alloc(TopRow, rank.keep);
    defer allocator.free(cut_items);
    var best: RankedSet = .{ .items = cut_items, .ctx = rank.ctx, .before = rank.before };
    for (jobs) |job| {
        for (job.set.items[0..job.set.len]) |row| best.consider(row);
    }
    for (jobs) |*job| job.cut = best.items[0];
    try runRankedJobs(allocator, jobs, rankedCutCountWorker, with_str);
    var cut_total: usize = 0;
    for (jobs) |job| cut_total += job.cut_count;
    const cut_rows = try allocator.alloc(TopRow, cut_total);
    defer allocator.free(cut_rows);
    var at: usize = 0;
    for (jobs) |*job| {
        job.cut_dst = cut_rows[at .. at + job.cut_count];
        at += job.cut_count;
    }
    try runRankedJobs(allocator, jobs, rankedCutFillWorker, with_str);
    try out.ensureUnusedCapacity(allocator, cut_total);
    for (cut_rows) |row| try appendRankedRow(allocator, out, row, with_str);
}

// Ranked rows borrow their bucket's string bytes; the result owns its copies.
fn appendRankedRow(allocator: Allocator, out: *std.ArrayListUnmanaged(TopRow), row: TopRow, comptime with_str: bool) !void {
    var owned = row;
    if (with_str) try ownTopRowStr(&owned, allocator);
    out.appendAssumeCapacity(owned);
}

fn runRankedJobs(allocator: Allocator, jobs: []RankedFillJob, comptime work: anytype, comptime with_str: bool) !void {
    const run = struct {
        fn run(job: *RankedFillJob) void {
            work(job, with_str);
        }
    }.run;
    if (jobs.len <= 1) {
        for (jobs) |*job| run(job);
        return;
    }
    const threads = try allocator.alloc(?std.Thread, jobs.len);
    defer allocator.free(threads);
    for (jobs, threads) |*job, *t| {
        t.* = std.Thread.spawn(.{}, run, .{job}) catch null;
        if (t.* == null) run(job);
    }
    for (threads) |t| if (t) |th| th.join();
}

// Ranking pays while each fill worker keeps under half the groups of its
// bucket range. Past that its set holds nearly every group it walks, the rows
// returned are about all the groups, and the sets only add a second copy.
fn rankPays(keep: usize, total_groups: u64, n_workers: usize) bool {
    const total: usize = @intCast(total_groups);
    return (keep *| 2) *| fillWorkerCount(n_workers, total) < total;
}

const RankedFillJob = struct {
    buckets: []PipeBucket,
    filter: ?EmitFilter,
    set: RankedSet,
    // The tie sweep: groups no worse than `cut`, counted, then copied.
    cut: TopRow = .{ .key = 0 },
    cut_count: usize = 0,
    cut_dst: []TopRow = &.{},

    // Inline and unwrapped: these run once per group, and a TopRow is a few
    // hundred bytes to copy.
    inline fn rowAt(bucket: *PipeBucket, gid: usize, comptime with_str: bool) TopRow {
        const ref = bucket.states.ref(gid);
        return if (with_str) topRowFromStateStr(ref, bucket.str_states.items[gid]) else topRowFromState(ref);
    }

    inline fn passes(job: *const RankedFillJob, r: TopRow) bool {
        const f = job.filter orelse return true;
        return f.pass(f.ctx, r);
    }

    inline fn withinCut(job: *const RankedFillJob, r: TopRow) bool {
        return job.passes(r) and !job.set.before(job.set.ctx, job.cut, r);
    }
};

fn rankedFillWorker(job: *RankedFillJob, comptime with_str: bool) void {
    for (job.buckets) |*bucket| {
        var gid: usize = 0;
        while (gid < bucket.states.len) : (gid += 1) {
            const r = RankedFillJob.rowAt(bucket, gid, with_str);
            if (job.passes(r)) job.set.consider(r);
        }
    }
}

fn rankedCutCountWorker(job: *RankedFillJob, comptime with_str: bool) void {
    for (job.buckets) |*bucket| {
        var gid: usize = 0;
        while (gid < bucket.states.len) : (gid += 1) {
            if (job.withinCut(RankedFillJob.rowAt(bucket, gid, with_str))) job.cut_count += 1;
        }
    }
}

fn rankedCutFillWorker(job: *RankedFillJob, comptime with_str: bool) void {
    var pos: usize = 0;
    for (job.buckets) |*bucket| {
        var gid: usize = 0;
        while (gid < bucket.states.len) : (gid += 1) {
            const r = RankedFillJob.rowAt(bucket, gid, with_str);
            if (!job.withinCut(r)) continue;
            job.cut_dst[pos] = r;
            pos += 1;
        }
    }
    std.debug.assert(pos == job.cut_dst.len);
}

/// The best `items.len` rows under `before` (see EmitRank), kept like TopSet
/// as a heap rooted at the worst retained row: a row that misses the cut costs
/// one comparison, a replacement O(log items.len).
pub const RankedSet = struct {
    items: []TopRow,
    len: usize = 0,
    ctx: ?*anyopaque,
    before: *const fn (?*anyopaque, TopRow, TopRow) bool,

    fn worse(self: *const RankedSet, a: TopRow, b: TopRow) bool {
        return self.before(self.ctx, b, a);
    }

    pub fn consider(self: *RankedSet, cand: TopRow) void {
        if (self.len < self.items.len) {
            var i = self.len;
            self.items[i] = cand;
            self.len += 1;
            while (i > 0) {
                const parent = (i - 1) / 2;
                if (!self.worse(self.items[i], self.items[parent])) break;
                std.mem.swap(TopRow, &self.items[i], &self.items[parent]);
                i = parent;
            }
            return;
        }
        if (self.items.len == 0 or !self.before(self.ctx, cand, self.items[0])) return;
        self.items[0] = cand;
        var i: usize = 0;
        while (true) {
            const left = 2 * i + 1;
            const right = left + 1;
            var w = i;
            if (left < self.len and self.worse(self.items[left], self.items[w])) w = left;
            if (right < self.len and self.worse(self.items[right], self.items[w])) w = right;
            if (w == i) break;
            std.mem.swap(TopRow, &self.items[i], &self.items[w]);
            i = w;
        }
    }
};

inline fn topRowFromState(ref: StateRef) TopRow {
    var row: TopRow = .{
        .key = ref.head.key,
        .count = ref.head.count,
        .rowref = ref.head.rowref,
    };
    for (ref.slots, 0..) |v, i| row.slots[i] = v;
    return row;
}

// Build a TopRow carrying its group's string MIN/MAX results (borrowed from the
// bucket's str_states; re-dup'd into the result allocator at final emit).
inline fn topRowFromStateStr(ref: StateRef, str_row: StrAccRow) TopRow {
    var row = topRowFromState(ref);
    for (str_row, 0..) |acc, i| {
        row.str[i] = acc.bytes;
        row.str_present[i] = acc.present;
    }
    return row;
}

// Re-dup a TopRow's (borrowed) string results into `allocator` so they outlive
// the bucket teardown. No-op for numeric rows (all slices empty).
fn ownTopRowStr(row: *TopRow, allocator: Allocator) !void {
    for (&row.str) |*s| {
        if (s.len > 0) s.* = try allocator.dupe(u8, s.*);
    }
}

// Bounded top-K set, sized at runtime from the query's LIMIT+OFFSET. Kept as
// a binary heap rooted at the WORST retained row, so the common case — a
// candidate that doesn't make the cut — is a single comparison against the
// root, and a replacement costs O(log k) instead of the O(k) worst-rescan the
// old fixed-10 array paid. The heap and the final sort move 32-byte ranks,
// not rows: a TopRow carries every aggregate slot inline (over 200 bytes),
// and at OFFSET-sized k moving rows took most of a small query's time.
const TopSet = struct {
    items: []TopRow = &.{},
    ranks: []Rank = &.{},
    len: usize = 0,

    const Rank = struct {
        key: u128,
        count: u64,
        slot: u32,

        fn of(row: TopRow, slot: usize) Rank {
            return .{ .key = row.key, .count = row.count, .slot = @intCast(slot) };
        }

        fn better(a: Rank, b: Rank) bool {
            if (a.count != b.count) return a.count > b.count;
            return a.key < b.key;
        }

        fn before(_: void, a: Rank, b: Rank) bool {
            return better(a, b);
        }
    };

    fn init(allocator: Allocator, k: usize) !TopSet {
        const items = try allocator.alloc(TopRow, k);
        errdefer allocator.free(items);
        return .{ .items = items, .ranks = try allocator.alloc(Rank, k) };
    }

    fn deinit(self: *TopSet, allocator: Allocator) void {
        allocator.free(self.items);
        allocator.free(self.ranks);
        self.* = .{};
    }

    fn consider_state(self: *TopSet, state: StateRef) void {
        if (self.items.len == 0) return;
        if (self.len == self.items.len) {
            const worst = self.ranks[0];
            if (state.head.count < worst.count or (state.head.count == worst.count and state.head.key >= worst.key)) return;
        }
        self.consider(topRowFromState(state));
    }

    fn consider(self: *TopSet, cand: TopRow) void {
        if (self.len < self.items.len) {
            var i = self.len;
            self.items[i] = cand;
            self.ranks[i] = Rank.of(cand, i);
            self.len += 1;
            while (i > 0) {
                const parent = (i - 1) / 2;
                if (!Rank.better(self.ranks[parent], self.ranks[i])) break;
                std.mem.swap(Rank, &self.ranks[i], &self.ranks[parent]);
                i = parent;
            }
            return;
        }
        if (self.items.len == 0) return;
        const rank = Rank.of(cand, self.ranks[0].slot);
        if (!Rank.better(rank, self.ranks[0])) return;
        self.items[rank.slot] = cand;
        self.ranks[0] = rank;
        var i: usize = 0;
        while (true) {
            const left = 2 * i + 1;
            const right = left + 1;
            var w = i;
            if (left < self.len and Rank.better(self.ranks[w], self.ranks[left])) w = left;
            if (right < self.len and Rank.better(self.ranks[w], self.ranks[right])) w = right;
            if (w == i) break;
            std.mem.swap(Rank, &self.ranks[i], &self.ranks[w]);
            i = w;
        }
    }

    /// Order the ranks best first. Call it after the last `consider`: the set
    /// is no longer a heap. `rowAt(i)` is then the i-th best row.
    fn sortBestFirst(self: *TopSet, allocator: Allocator) !void {
        try thindb.exec.memory.sort(Rank, self.ranks[0..self.len], {}, allocator, Rank.before);
    }

    fn rowAt(self: *const TopSet, i: usize) TopRow {
        return self.items[self.ranks[i].slot];
    }
};

fn totalRows(snap: Scan.Snapshot) u64 {
    var n: u64 = snap.memtable_row_count;
    for (snap.segments) |s| n += s.row_count;
    return n;
}

inline fn routeHashKey(key: u128) u64 {
    const lo: u64 = @truncate(key);
    const hi: u64 = @truncate(key >> 64);
    var x = lo ^ (hi *% 0x9e3779b97f4a7c15);
    x ^= x >> 32;
    return x;
}

inline fn routeHashRowBits(lo: u64, hi: u32) u64 {
    var x = lo ^ (@as(u64, hi) *% 0x9e3779b97f4a7c15);
    x ^= x >> 32;
    return x;
}

inline fn bucketIndexHash(hash: u64, bucket_count: usize) usize {
    if (std.math.isPowerOfTwo(bucket_count)) return @as(usize, @truncate(hash)) & (bucket_count - 1);
    return @as(usize, @truncate(hash % bucket_count));
}

inline fn readGenericKeyBits(view: thindb.storage.ColumnView, typ: thindb.types.Type, row: usize) !u128 {
    return switch (typ) {
        .boolean => switch (view.data) {
            .boolean => |v| @as(u128, v[row]),
            else => error.TypeMismatch,
        },
        .tinyint => switch (view.data) {
            .tinyint => |v| @as(u128, @as(u8, @bitCast(v[row]))),
            else => error.TypeMismatch,
        },
        .smallint => switch (view.data) {
            .smallint => |v| @as(u128, @as(u16, @bitCast(v[row]))),
            else => error.TypeMismatch,
        },
        .int, .date => switch (view.data) {
            .int => |v| @as(u128, @as(u32, @bitCast(v[row]))),
            .date => |v| @as(u128, @as(u32, @bitCast(v[row]))),
            else => error.TypeMismatch,
        },
        .bigint, .datetime, .decimal64 => switch (view.data) {
            .bigint => |v| @as(u128, @as(u64, @bitCast(v[row]))),
            .datetime => |v| @as(u128, @as(u64, @bitCast(v[row]))),
            .decimal64 => |v| @as(u128, @as(u64, @bitCast(v[row]))),
            else => error.TypeMismatch,
        },
        else => error.TypeMismatch,
    };
}

fn genericKeyFromViews(layout: GroupRowsLayout, key_views: []const thindb.storage.ColumnView, row: usize) !u128 {
    var key: u128 = 0;
    for (layout.key_columns, 0..) |part, i| {
        if (part.nullable) {
            // NULL → all-zero slice (validity bit clear); a valid row sets the
            // top bit above its payload so value 0 and NULL stay distinct.
            if (!key_views[i].isValid(row)) continue;
            const raw = try readGenericKeyBits(key_views[i], part.typ, row);
            const valid_bit = @as(u128, 1) << @intCast(part.width_bits - 1);
            key |= (raw | valid_bit) << @intCast(part.offset_bits);
            continue;
        }
        const raw = try readGenericKeyBits(key_views[i], part.typ, row);
        key |= raw << @intCast(part.offset_bits);
    }
    return key;
}

// Hashed group identity for string / >128-bit keys: a 128-bit Wyhash digest
// (two independent seeds → hi/lo) over the key columns. A STRING column
// contributes its fixed 16-byte `stringKeyDigest` — taken from the scan's
// `Batch.hashed` sidecar when present (the string was never materialized),
// recomputed from bytes otherwise — so per-column digests compose identically
// across mixed batch sources and with native int key columns. The actual key
// values are recovered at emit via the row's carried __rowloc (late
// materialization), so collisions — astronomically unlikely at 128 bits — are
// the only correctness caveat.
//
// The per-column parts are packed into one buffer and digested in a single
// pass (`stringKeyDigest` over the concatenation): streaming Wyhash fed the
// same parts one by one produces the identical digest, but its per-update
// buffering costs several times the hash itself on short parts — this runs
// once per scanned row.
fn hashGenericKeyFromViews(layout: GroupRowsLayout, key_views: []const thindb.storage.ColumnView, key_digests: []const ?[]const u128, row: usize) u128 {
    // A lone non-NULL string key's digest already identifies it; digesting
    // it again would only cost two more hashes per row.
    if (layout.key_columns.len == 1 and !layout.key_columns[0].nullable) switch (key_views[0].data) {
        .string, .varchar, .char, .json, .nested, .bitmap => |sv| return if (key_digests[0]) |ds| ds[row] else thindb.exec.stringKeyDigest(sv.rowBytes(row)),
        else => {},
    };
    var buf: [MAX_GENERIC_GROUP_KEYS * (1 + @sizeOf(u128))]u8 = undefined;
    var len: usize = 0;
    for (layout.key_columns, 0..) |part, i| {
        if (part.nullable) {
            // The validity tag joins the digest; a NULL row's payload is
            // SKIPPED — its decoded bytes are an encoding artifact (FOR base /
            // dict entry 0) that may differ across batch sources, and all NULL
            // rows must land in one group.
            const valid = key_views[i].isValid(row);
            buf[len] = @intFromBool(valid);
            len += 1;
            if (!valid) continue;
        }
        const d: u128 = switch (key_views[i].data) {
            .string, .varchar, .char, .json, .nested, .bitmap => |sv| if (key_digests[i]) |ds| ds[row] else thindb.exec.stringKeyDigest(sv.rowBytes(row)),
            else => readGenericKeyBits(key_views[i], part.typ, row) catch 0,
        };
        buf[len..][0..@sizeOf(u128)].* = @bitCast(d);
        len += @sizeOf(u128);
    }
    return thindb.exec.stringKeyDigest(buf[0..len]);
}

fn applyScanFilterExpr(scan: *Scan, expr: thindb.exec.PredicateExpr) !bool {
    // The scan owns all filter handling: literal coercion, zone-map prune-hint
    // extraction (incl. IN), and block-sourced evaluation of unprojected
    // columns. The core pipeline just hands it the predicate.
    return try scan.tryFuseFilter(expr);
}

fn normalizeRouteBlockRows(route_block_rows: usize) usize {
    return @max(@as(usize, 1), @min(route_block_rows, MAX_ROUTE_BLOCK_ROWS));
}

fn chooseRouteBlockRows(bucket_count: usize, cfg_route_block_rows: usize, route_block_rows_set: bool) usize {
    _ = bucket_count;
    if (route_block_rows_set) return normalizeRouteBlockRows(cfg_route_block_rows);
    return DEFAULT_ROUTE_BLOCK_ROWS;
}

fn aggInputViewBytes(view: thindb.storage.ColumnView, pt: GroupColumnType) ?[]const u8 {
    return switch (view.data) {
        .boolean => |v| if (pt == .i8) std.mem.sliceAsBytes(v) else null,
        .tinyint => |v| if (pt == .i8) std.mem.sliceAsBytes(v) else null,
        .smallint => |v| if (pt == .i16) std.mem.sliceAsBytes(v) else null,
        .int => |v| if (pt == .i32) std.mem.sliceAsBytes(v) else null,
        .date => |v| if (pt == .i32) std.mem.sliceAsBytes(v) else null,
        .bigint => |v| if (pt == .i64) std.mem.sliceAsBytes(v) else null,
        .datetime => |v| if (pt == .i64) std.mem.sliceAsBytes(v) else null,
        .decimal64 => |v| if (pt == .i64) std.mem.sliceAsBytes(v) else null,
        .float => |v| if (pt == .f32) std.mem.sliceAsBytes(v) else null,
        .double => |v| if (pt == .f64) std.mem.sliceAsBytes(v) else null,
        else => null,
    };
}

/// `hashed_ids` carries every row's identity (batchIdentities) when the
/// layout's key is hashed.
fn appendBatchRawChunksGeneric(parts: *WorkerParts, shared: *PipeShared, batch: thindb.Batch, row_weights: ?RowWeights, hashed_ids: ?[]const u128, raw_chunk_rows: usize, profile: bool, skip_filter_check: bool) !void {
    if (shared.generic_filter_required and !skip_filter_check) return error.UnsupportedOperatorForType;
    const layout = shared.group_rows_layout;
    if (layout.hashed_key != (hashed_ids != null)) return error.UnsupportedOperatorForType;
    if (layout.key_columns.len == 0 or layout.key_columns.len > MAX_GENERIC_GROUP_KEYS) return error.UnsupportedOperatorForType;
    if (parts.raw_active_rows.capacity() == 0) parts.raw_active_rows = try acquireRawRows(shared, raw_chunk_rows, &parts.raw_recycle_lock_ticks);

    var key_views_buf: [MAX_GENERIC_GROUP_KEYS]thindb.storage.ColumnView = undefined;
    for (layout.key_columns, 0..) |part, i| {
        const ci = batch.columnIndex(part.name) orelse return error.ColumnNotFound;
        key_views_buf[i] = batch.values[ci];
    }
    if (layout.columns.len > MAX_GROUP_PAYLOAD_COLUMNS) return error.UnsupportedOperatorForType;

    // Per-column native source bytes + element size — one typed (copyElem) path
    // covering every int/float agg-input width. Weighted layouts bind fold
    // specs instead: their staged columns are pre-widened partials (i64/f64),
    // so the source view is read typed and reduced, never byte-copied.
    var payload_bytes: [MAX_GROUP_PAYLOAD_COLUMNS][]const u8 = undefined;
    var payload_elt: [MAX_GROUP_PAYLOAD_COLUMNS]usize = undefined;
    var payload_valid: [MAX_GROUP_PAYLOAD_COLUMNS]?[]const u8 = undefined;
    var inputs_nullable = false;
    var weight_specs_buf: [MAX_GROUP_PAYLOAD_COLUMNS]WeightFoldSpec = undefined;
    var weight_specs: []const WeightFoldSpec = &.{};
    if (layout.has_weight) {
        try buildWeightFoldSpecs(layout, batch, if (row_weights) |rw| rw.partials else &.{}, weight_specs_buf[0..layout.columns.len]);
        weight_specs = weight_specs_buf[0..layout.columns.len];
    } else {
        for (layout.columns, 0..) |column, i| {
            if (column.source_name.len == 0) return error.UnsupportedOperatorForType;
            const view = batch.columnView(column.source_name) orelse return error.ColumnNotFound;
            payload_bytes[i] = aggInputViewBytes(view, column.physical_type) orelse return error.TypeMismatch;
            payload_elt[i] = groupColumnSize(column.physical_type);
            payload_valid[i] = view.nulls;
            if (column.nullable) inputs_nullable = true;
        }
    }

    // String agg-input columns (string MIN/MAX). Bound once; carried into the
    // active chunk's StrStore per row. Empty for numeric-only queries, so every
    // append path's `if (str_views.len != 0)` guard is a free no-op there.
    var str_views_buf: [MAX_GROUP_STR_COLUMNS]StringView = undefined;
    var str_nulls_buf: [MAX_GROUP_STR_COLUMNS]?[]const u8 = undefined;
    var str_views: []const StringView = &.{};
    var str_nulls: []const ?[]const u8 = &.{};
    // String columns whose values ascend down the batch: an extreme staged
    // from this batch is already settled against its later rows.
    var ascending_str: StrColumnMask = 0;
    if (layout.has_str_payload) {
        if (layout.str_columns.len > MAX_GROUP_STR_COLUMNS) return error.UnsupportedOperatorForType;
        const ascending_column = if (row_weights) |rw| rw.ascending_column else null;
        for (layout.str_columns, 0..) |sc, i| {
            const ci = batch.columnIndex(sc.source_name) orelse return error.ColumnNotFound;
            const view = batch.values[ci];
            str_views_buf[i] = switch (view.data) {
                .varchar, .string, .char, .json, .nested, .bitmap => |sv| sv,
                else => return error.TypeMismatch,
            };
            str_nulls_buf[i] = view.nulls;
            if (ascending_column == ci) ascending_str |= @as(StrColumnMask, 1) << @intCast(i);
        }
        str_views = str_views_buf[0..layout.str_columns.len];
        str_nulls = str_nulls_buf[0..layout.str_columns.len];
    }

    // Key-shape fast variants (no hashed key). Payload is type-generic; the
    // string lane (if any) rides along via appendStrPayload inside each kernel.
    // Run-native weighted emission: when every key column arrives with an RLE
    // run sidecar, merge the columns' runs into composite spans and emit one
    // staged row per span — key packing and the collapse compare happen per
    // SPAN, never per row. Any missing sidecar falls through to the per-row
    // weighted paths below (memtable, tombstoned/filtered batches, non-RLE
    // blocks), which produce identical keys and partials.
    // Nullable key columns take the per-row paths: run sidecars and the
    // monomorphic key kernels read raw values with no validity lane.
    var keys_nullable = false;
    for (layout.key_columns) |part| {
        if (part.nullable) keys_nullable = true;
    }

    if (row_weights == null and layout.has_weight and !layout.has_rowref and !keys_nullable and !layout.has_str_payload) runs_blk: {
        const rb = batch.runs orelse break :runs_blk;
        var key_runs_buf: [MAX_GENERIC_GROUP_KEYS]thindb.exec.RunsColumn = undefined;
        for (layout.key_columns, 0..) |part, i| {
            switch (part.typ) {
                .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => {},
                else => break :runs_blk,
            }
            const ci = batch.columnIndex(part.name) orelse break :runs_blk;
            if (ci >= rb.len) break :runs_blk;
            key_runs_buf[i] = rb[ci] orelse break :runs_blk;
        }
        // An aggregate input that also arrived run-encoded folds value×count
        // off its header — never reading the expanded rows.
        for (layout.columns, 0..) |column, c| {
            weight_specs_buf[c].runs = null;
            if (weight_specs_buf[c].is_float) continue;
            const ci = batch.columnIndex(column.source_name) orelse continue;
            if (ci < rb.len) weight_specs_buf[c].runs = rb[ci];
        }
        try appendBatchRunsWeighted(parts, shared, batch.row_count, layout, key_runs_buf[0..layout.key_columns.len], weight_specs, raw_chunk_rows, profile);
        return;
    }

    // Nullable numeric inputs also stay per-row: the fast key kernels append
    // payload bytes with no validity-lane write. Their weighted forms collapse
    // numeric runs only; string extremes fold in the per-row loop.
    if (row_weights == null and !layout.has_rowref and !keys_nullable and !inputs_nullable and !(layout.has_weight and layout.has_str_payload)) {
        if (try appendBatchRawChunksGenericFast(parts, shared, batch, layout, key_views_buf[0..layout.key_columns.len], payload_bytes[0..layout.columns.len], payload_elt[0..layout.columns.len], weight_specs, str_views, str_nulls, raw_chunk_rows, profile)) return;
    }

    const rowref_col: []const i64 = if (layout.has_rowref) blk: {
        const view = batch.columnView(rowloc.col_name) orelse return error.ColumnNotFound;
        break :blk switch (view.data) {
            .bigint => |v| v,
            else => return error.TypeMismatch,
        };
    } else &.{};

    const route_t0 = if (profile) platform.nowTicks() else 0;
    var accepted: u64 = 0;
    var probes: u64 = 0;
    var hits: u64 = 0;
    // Staged rows from here on came from this batch.
    var batch_first = parts.raw_active_rows.len();
    var r: usize = 0;
    while (r < batch.row_count) {
        var active = &parts.raw_active_rows;
        const use_fold = layout.has_weight and !parts.fold_off and parts.fold_slots.len != 0;
        // The row loop is duplicated on the (per-batch constant) validity
        // flag so the non-nullable copy carries zero per-row validity code —
        // this loop runs once per scanned row on every staged GROUP BY.
        if (inputs_nullable) {
            // The weight gate (buildWeightFoldSpecs) declines nullable inputs,
            // so the weighted-collapse branch lives only in the other copy.
            std.debug.assert(!layout.has_weight);
            while (r < batch.row_count and active.len() < raw_chunk_rows) : (r += 1) {
                const key = if (hashed_ids) |ids| ids[r] else try genericKeyFromViews(layout, key_views_buf[0..layout.key_columns.len], r);
                const idx = active.len();
                active.setKey(idx, key);
                if (layout.has_rowref) active.rowrefAll()[idx] = rowref_col[r];
                appendGenericPayload(active, payload_bytes[0..layout.columns.len], payload_elt[0..layout.columns.len], idx, r);
                appendPayloadValidity(active, layout, payload_valid[0..layout.columns.len], idx, r);
                if (str_views.len != 0) try appendStrPayload(active, shared.allocator, str_views, str_nulls, idx, r);
                active.len_rows = idx + 1;
                accepted += 1;
            }
        } else while (r < batch.row_count and active.len() < raw_chunk_rows) : (r += 1) {
            const key = if (hashed_ids) |ids| ids[r] else try genericKeyFromViews(layout, key_views_buf[0..layout.key_columns.len], r);
            const idx = active.len();
            // Weighted collapse (decomposable programs): a key already staged
            // in the chunk — the adjacent row, or one the fold index finds —
            // folds into that row instead of appending, so the chunk ships
            // ONE (key, weight, partials) row per key. The creating row's
            // rowref is kept (any row of the key has the same key values).
            // (The weight gate declines nullable inputs, so this branch only
            // lives in the non-nullable copy.)
            if (layout.has_weight) {
                // A collapsed batch's row stands for `w` equal rows.
                const w: u32 = if (row_weights) |rw| rw.counts[r] else 1;
                if (idx > 0 and active.keyAt(idx - 1) == key and active.weightAll()[idx - 1] <= std.math.maxInt(u32) - w) {
                    const settled = if (idx - 1 >= batch_first) ascending_str else 0;
                    try weightFoldRow(active, shared.allocator, layout.aggregates, weight_specs, str_views, str_nulls, settled, idx - 1, r, w);
                    accepted += w;
                    continue;
                }
                if (use_fold) {
                    const held = foldIndexProbe(parts, active, key, idx);
                    probes += 1;
                    if (held < idx and active.weightAll()[held] <= std.math.maxInt(u32) - w) {
                        const settled = if (held >= batch_first) ascending_str else 0;
                        try weightFoldRow(active, shared.allocator, layout.aggregates, weight_specs, str_views, str_nulls, settled, held, r, w);
                        hits += 1;
                        accepted += w;
                        continue;
                    }
                }
                active.weightAll()[idx] = w;
                weightFoldScaled(active, weight_specs, idx, r, w, true);
                if (str_views.len != 0) try appendStrPayload(active, shared.allocator, str_views, str_nulls, idx, r);
                active.setKey(idx, key);
                if (layout.has_rowref) active.rowrefAll()[idx] = rowref_col[r];
                active.len_rows = idx + 1;
                accepted += w;
                continue;
            }
            active.setKey(idx, key);
            if (layout.has_rowref) active.rowrefAll()[idx] = rowref_col[r];
            appendGenericPayload(active, payload_bytes[0..layout.columns.len], payload_elt[0..layout.columns.len], idx, r);
            if (str_views.len != 0) try appendStrPayload(active, shared.allocator, str_views, str_nulls, idx, r);
            active.len_rows = idx + 1;
            accepted += 1;
        }
        // A folding chunk can take rows indefinitely; a string extreme that
        // keeps improving keeps appending bytes, so those bytes also close it.
        const full = active.len() == raw_chunk_rows or
            (layout.has_weight and str_views.len != 0 and active.str.bytes.items.len >= FOLD_CHUNK_STR_BYTES);
        if (full) {
            noteFoldChunk(parts, probes, hits);
            probes = 0;
            hits = 0;
            try publishActiveRawRows(parts, shared, raw_chunk_rows);
            batch_first = 0;
        }
    }
    parts.fold_probes += probes;
    parts.fold_hits += hits;
    parts.scanned_count += if (row_weights) |rw| rw.input_rows else batch.row_count;
    parts.row_count += accepted;
    if (profile) parts.partition_ticks += platform.nowTicks() - route_t0;
}

// Native-width key bits from a run value (sign-extended i64), matching
// `readGenericKeyBits` exactly: the value's bits at the type's own width,
// zero-extended. Caller pre-gates the type set.
inline fn runKeyBits(typ: thindb.types.Type, v: i64) u128 {
    const raw: u64 = @bitCast(v);
    return switch (typ) {
        .boolean, .tinyint => @as(u128, raw & 0xFF),
        .smallint => @as(u128, raw & 0xFFFF),
        .int, .date => @as(u128, raw & 0xFFFF_FFFF),
        else => @as(u128, raw),
    };
}

// Run-native weighted emitter: iterate COMPOSITE key spans (a span ends where
// any key column's run ends — a K-pointer merge over the run sidecars), pack
// one key and emit one (key, weight, partials) staged row per span. For a
// CounterID-class block this is ~tens of spans instead of 64K rows.
fn appendBatchRunsWeighted(
    parts: *WorkerParts,
    shared: *PipeShared,
    row_count: usize,
    layout: GroupRowsLayout,
    key_runs: []const thindb.exec.RunsColumn,
    weight_specs: []const WeightFoldSpec,
    raw_chunk_rows: usize,
    profile: bool,
) !void {
    if (row_count == 0) return;
    const route_t0 = if (profile) platform.nowTicks() else 0;
    var accepted: u64 = 0;
    var run_idx = [_]usize{0} ** MAX_GENERIC_GROUP_KEYS;
    var run_left: [MAX_GENERIC_GROUP_KEYS]usize = undefined;
    for (key_runs, 0..) |kr, i| {
        if (kr.lengths.len == 0) return error.UnsupportedOperatorForType;
        run_left[i] = kr.lengths[0];
    }
    var spec_idx = [_]usize{0} ** MAX_GROUP_PAYLOAD_COLUMNS;
    var spec_left = [_]usize{0} ** MAX_GROUP_PAYLOAD_COLUMNS;
    for (weight_specs, 0..) |spec, c| {
        if (spec.runs) |ir| {
            if (ir.lengths.len == 0) return error.UnsupportedOperatorForType;
            spec_left[c] = ir.lengths[0];
        }
    }
    var r: usize = 0;
    while (r < row_count) {
        var active = &parts.raw_active_rows;
        const weights = active.weightAll();
        while (r < row_count and active.len() < raw_chunk_rows) {
            var span: usize = row_count - r;
            for (key_runs, 0..) |_, i| span = @min(span, run_left[i]);
            // Lengths shorter than the row count would spin forever; a corrupt
            // block surfaces as a query error, not a hang.
            if (span == 0) return error.UnsupportedOperatorForType;
            var key: u128 = 0;
            for (layout.key_columns, 0..) |part, i| {
                key |= runKeyBits(part.typ, key_runs[i].values_i64[run_idx[i]]) << @intCast(part.offset_bits);
            }
            const wlen: u32 = @intCast(span);
            const idx = active.len();
            if (idx > 0 and active.keyAt(idx - 1) == key) {
                weights[idx - 1] += wlen;
                try weightFoldSpan(active, weight_specs, &spec_idx, &spec_left, idx - 1, r, span, false);
            } else {
                active.setKey(idx, key);
                weights[idx] = wlen;
                try weightFoldSpan(active, weight_specs, &spec_idx, &spec_left, idx, r, span, true);
                active.len_rows = idx + 1;
            }
            accepted += span;
            r += span;
            for (key_runs, 0..) |kr, i| {
                run_left[i] -= span;
                if (run_left[i] == 0) {
                    run_idx[i] += 1;
                    if (run_idx[i] < kr.lengths.len) run_left[i] = kr.lengths[run_idx[i]];
                }
            }
        }
        if (active.len() == raw_chunk_rows) try publishActiveRawRows(parts, shared, raw_chunk_rows);
    }
    parts.scanned_count += row_count;
    parts.row_count += accepted;
    if (profile) parts.partition_ticks += platform.nowTicks() - route_t0;
}

fn appendBatchRawChunksGenericFast(
    parts: *WorkerParts,
    shared: *PipeShared,
    batch: thindb.Batch,
    layout: GroupRowsLayout,
    key_views: []const thindb.storage.ColumnView,
    payload_bytes: []const []const u8,
    payload_elt: []const usize,
    weight_specs: []const WeightFoldSpec,
    str_views: []const StringView,
    str_nulls: []const ?[]const u8,
    raw_chunk_rows: usize,
    profile: bool,
) !bool {
    if (layout.key_columns.len == 1) {
        const p0 = layout.key_columns[0];
        if (p0.offset_bits == 0 and p0.width_bits == 64 and p0.typ == .bigint) {
            const k0 = switch (key_views[0].data) {
                .bigint => |v| v,
                else => return false,
            };
            try appendBatchRawChunksGenericKey1I64(parts, shared, batch.row_count, k0, payload_bytes, payload_elt, weight_specs, str_views, str_nulls, raw_chunk_rows, profile);
            return true;
        }
    }

    if (layout.key_columns.len == 2) {
        const p0 = layout.key_columns[0];
        const p1 = layout.key_columns[1];
        if (p0.offset_bits == 0 and p0.width_bits == 16 and p0.typ == .smallint and
            p1.offset_bits == 16 and p1.width_bits == 32 and (p1.typ == .int or p1.typ == .date))
        {
            const k0 = switch (key_views[0].data) {
                .smallint => |v| v,
                else => return false,
            };
            const k1 = switch (key_views[1].data) {
                .int => |v| v,
                .date => |v| v,
                else => return false,
            };
            try appendBatchRawChunksGenericKeyI16I32(parts, shared, batch.row_count, k0, k1, payload_bytes, payload_elt, weight_specs, str_views, str_nulls, raw_chunk_rows, profile);
            return true;
        }

        if (p0.offset_bits == 0 and p0.width_bits == 64 and (p0.typ == .bigint or p0.typ == .datetime or p0.typ == .decimal64) and
            p1.offset_bits == 64 and p1.width_bits == 32 and (p1.typ == .int or p1.typ == .date))
        {
            const k0 = switch (key_views[0].data) {
                .bigint => |v| v,
                .datetime => |v| v,
                .decimal64 => |v| v,
                else => return false,
            };
            const k1 = switch (key_views[1].data) {
                .int => |v| v,
                .date => |v| v,
                else => return false,
            };
            try appendBatchRawChunksGenericKeyI64I32(parts, shared, batch.row_count, k0, k1, payload_bytes, payload_elt, weight_specs, str_views, str_nulls, raw_chunk_rows, profile);
            return true;
        }
    }

    return false;
}

fn appendBatchRawChunksGenericKey1I64(
    parts: *WorkerParts,
    shared: *PipeShared,
    row_count: usize,
    key0: []const i64,
    payload_bytes: []const []const u8,
    payload_elt: []const usize,
    weight_specs: []const WeightFoldSpec,
    str_views: []const StringView,
    str_nulls: []const ?[]const u8,
    raw_chunk_rows: usize,
    profile: bool,
) !void {
    const route_t0 = if (profile) platform.nowTicks() else 0;
    var accepted: u64 = 0;
    var r: usize = 0;
    if (parts.raw_active_rows.layout.has_weight) {
        // Decomposable programs: one (key, weight, partials) staged row per
        // source run — the run's inputs reduce in registers via the fold
        // specs. The weight gate guarantees no string payload rides along.
        std.debug.assert(str_views.len == 0);
        while (r < row_count) {
            var active = &parts.raw_active_rows;
            const keys = active.keyU64All();
            const weights = active.weightAll();
            while (r < row_count and active.len() < raw_chunk_rows) {
                var run_end = r + 1;
                while (run_end < row_count and key0[run_end] == key0[r]) run_end += 1;
                const wlen: u32 = @intCast(run_end - r);
                const k = @as(u64, @bitCast(key0[r]));
                const idx = active.len();
                if (idx > 0 and keys[idx - 1] == k) {
                    weights[idx - 1] += wlen;
                    weightFoldRange(active, weight_specs, idx - 1, r, run_end, false);
                } else {
                    keys[idx] = k;
                    weights[idx] = wlen;
                    weightFoldRange(active, weight_specs, idx, r, run_end, true);
                    active.len_rows = idx + 1;
                }
                accepted += wlen;
                r = run_end;
            }
            if (active.len() == raw_chunk_rows) try publishActiveRawRows(parts, shared, raw_chunk_rows);
        }
        parts.scanned_count += row_count;
        parts.row_count += accepted;
        if (profile) parts.partition_ticks += platform.nowTicks() - route_t0;
        return;
    }
    while (r < row_count) {
        var active = &parts.raw_active_rows;
        const keys = active.keyU64All();
        while (r < row_count and active.len() < raw_chunk_rows) : (r += 1) {
            const idx = active.len();
            keys[idx] = @as(u64, @bitCast(key0[r]));
            appendGenericPayload(active, payload_bytes, payload_elt, idx, r);
            if (str_views.len != 0) try appendStrPayload(active, shared.allocator, str_views, str_nulls, idx, r);
            active.len_rows = idx + 1;
            accepted += 1;
        }
        if (active.len() == raw_chunk_rows) try publishActiveRawRows(parts, shared, raw_chunk_rows);
    }
    parts.scanned_count += row_count;
    parts.row_count += accepted;
    if (profile) parts.partition_ticks += platform.nowTicks() - route_t0;
}

fn appendBatchRawChunksGenericKeyI16I32(
    parts: *WorkerParts,
    shared: *PipeShared,
    row_count: usize,
    key0: []const i16,
    key1: []const i32,
    payload_bytes: []const []const u8,
    payload_elt: []const usize,
    weight_specs: []const WeightFoldSpec,
    str_views: []const StringView,
    str_nulls: []const ?[]const u8,
    raw_chunk_rows: usize,
    profile: bool,
) !void {
    const route_t0 = if (profile) platform.nowTicks() else 0;
    var accepted: u64 = 0;
    var r: usize = 0;
    if (parts.raw_active_rows.layout.has_weight) {
        std.debug.assert(str_views.len == 0);
        while (r < row_count) {
            var active = &parts.raw_active_rows;
            const keys = active.keyU64All();
            const weights = active.weightAll();
            while (r < row_count and active.len() < raw_chunk_rows) {
                var run_end = r + 1;
                while (run_end < row_count and key0[run_end] == key0[r] and key1[run_end] == key1[r]) run_end += 1;
                const wlen: u32 = @intCast(run_end - r);
                const k = @as(u16, @bitCast(key0[r])) |
                    (@as(u64, @as(u32, @bitCast(key1[r]))) << 16);
                const idx = active.len();
                if (idx > 0 and keys[idx - 1] == k) {
                    weights[idx - 1] += wlen;
                    weightFoldRange(active, weight_specs, idx - 1, r, run_end, false);
                } else {
                    keys[idx] = k;
                    weights[idx] = wlen;
                    weightFoldRange(active, weight_specs, idx, r, run_end, true);
                    active.len_rows = idx + 1;
                }
                accepted += wlen;
                r = run_end;
            }
            if (active.len() == raw_chunk_rows) try publishActiveRawRows(parts, shared, raw_chunk_rows);
        }
        parts.scanned_count += row_count;
        parts.row_count += accepted;
        if (profile) parts.partition_ticks += platform.nowTicks() - route_t0;
        return;
    }
    while (r < row_count) {
        var active = &parts.raw_active_rows;
        const keys = active.keyU64All();
        while (r < row_count and active.len() < raw_chunk_rows) : (r += 1) {
            const idx = active.len();
            keys[idx] = @as(u16, @bitCast(key0[r])) |
                (@as(u64, @as(u32, @bitCast(key1[r]))) << 16);
            appendGenericPayload(active, payload_bytes, payload_elt, idx, r);
            if (str_views.len != 0) try appendStrPayload(active, shared.allocator, str_views, str_nulls, idx, r);
            active.len_rows = idx + 1;
            accepted += 1;
        }
        if (active.len() == raw_chunk_rows) try publishActiveRawRows(parts, shared, raw_chunk_rows);
    }
    parts.scanned_count += row_count;
    parts.row_count += accepted;
    if (profile) parts.partition_ticks += platform.nowTicks() - route_t0;
}

fn appendBatchRawChunksGenericKeyI64I32(
    parts: *WorkerParts,
    shared: *PipeShared,
    row_count: usize,
    key0: []const i64,
    key1: []const i32,
    payload_bytes: []const []const u8,
    payload_elt: []const usize,
    weight_specs: []const WeightFoldSpec,
    str_views: []const StringView,
    str_nulls: []const ?[]const u8,
    raw_chunk_rows: usize,
    profile: bool,
) !void {
    const route_t0 = if (profile) platform.nowTicks() else 0;
    var accepted: u64 = 0;
    var r: usize = 0;
    if (parts.raw_active_rows.layout.has_weight) {
        std.debug.assert(str_views.len == 0);
        while (r < row_count) {
            var active = &parts.raw_active_rows;
            const key_lo = active.keyU96LoAll();
            const key_hi = active.keyU96HiAll();
            const weights = active.weightAll();
            while (r < row_count and active.len() < raw_chunk_rows) {
                var run_end = r + 1;
                while (run_end < row_count and key0[run_end] == key0[r] and key1[run_end] == key1[r]) run_end += 1;
                const wlen: u32 = @intCast(run_end - r);
                const lo = @as(u64, @bitCast(key0[r]));
                const hi = @as(u32, @bitCast(key1[r]));
                const idx = active.len();
                if (idx > 0 and key_lo[idx - 1] == lo and key_hi[idx - 1] == hi) {
                    weights[idx - 1] += wlen;
                    weightFoldRange(active, weight_specs, idx - 1, r, run_end, false);
                } else {
                    key_lo[idx] = lo;
                    key_hi[idx] = hi;
                    weights[idx] = wlen;
                    weightFoldRange(active, weight_specs, idx, r, run_end, true);
                    active.len_rows = idx + 1;
                }
                accepted += wlen;
                r = run_end;
            }
            if (active.len() == raw_chunk_rows) try publishActiveRawRows(parts, shared, raw_chunk_rows);
        }
        parts.scanned_count += row_count;
        parts.row_count += accepted;
        if (profile) parts.partition_ticks += platform.nowTicks() - route_t0;
        return;
    }
    while (r < row_count) {
        var active = &parts.raw_active_rows;
        const key_lo = active.keyU96LoAll();
        const key_hi = active.keyU96HiAll();
        while (r < row_count and active.len() < raw_chunk_rows) : (r += 1) {
            const idx = active.len();
            key_lo[idx] = @as(u64, @bitCast(key0[r]));
            key_hi[idx] = @as(u32, @bitCast(key1[r]));
            appendGenericPayload(active, payload_bytes, payload_elt, idx, r);
            if (str_views.len != 0) try appendStrPayload(active, shared.allocator, str_views, str_nulls, idx, r);
            active.len_rows = idx + 1;
            accepted += 1;
        }
        if (active.len() == raw_chunk_rows) try publishActiveRawRows(parts, shared, raw_chunk_rows);
    }
    parts.scanned_count += row_count;
    parts.row_count += accepted;
    if (profile) parts.partition_ticks += platform.nowTicks() - route_t0;
}

// Copy one payload element (1/2/4/8-byte typed move per element size — so each
// int/float width gets a real typed path, not a byte loop). Element width is
// the only thing that matters for the copy; int-vs-float is decided later, at
// aggregation.
inline fn copyElem(dst: []u8, src: []const u8, sz: usize, dst_idx: usize, src_idx: usize) void {
    switch (sz) {
        1 => dst[dst_idx] = src[src_idx],
        2 => @as([*]u16, @ptrCast(@alignCast(dst.ptr)))[dst_idx] = @as([*]const u16, @ptrCast(@alignCast(src.ptr)))[src_idx],
        4 => @as([*]u32, @ptrCast(@alignCast(dst.ptr)))[dst_idx] = @as([*]const u32, @ptrCast(@alignCast(src.ptr)))[src_idx],
        8 => @as([*]u64, @ptrCast(@alignCast(dst.ptr)))[dst_idx] = @as([*]const u64, @ptrCast(@alignCast(src.ptr)))[src_idx],
        else => @memcpy(dst[dst_idx * sz ..][0..sz], src[src_idx * sz ..][0..sz]),
    }
}

inline fn appendGenericPayload(active: *RawRows, payload_bytes: []const []const u8, payload_elt: []const usize, dst: usize, src: usize) void {
    var col: usize = 0;
    while (col < payload_bytes.len) : (col += 1) {
        copyElem(active.columnByteSlab(col), payload_bytes[col], payload_elt[col], dst, src);
    }
}

// Stage the source row's validity bits into the chunk's validity lanes — one
// byte per nullable payload column. Only called when the layout has nullable
// inputs (the caller's `inputs_nullable` guard keeps it off the hot path).
inline fn appendPayloadValidity(active: *RawRows, layout: GroupRowsLayout, payload_valid: []const ?[]const u8, dst: usize, src: usize) void {
    for (layout.columns, 0..) |column, col| {
        if (!column.nullable) continue;
        active.validAll(column.valid_index)[dst] = @intFromBool(thindb.storage.column.isValidBit(payload_valid[col], src));
    }
}

// Weighted-collapse payload folding: each staged column belongs to exactly
// one SUM/AVG/MIN/MAX aggregate (the engine's weight gate), is pre-widened to
// i64/f64, and carries the RUN'S PARTIAL (sum or extreme), not a row value.
const WeightFoldOp = enum { sum, min, max };

const WeightFoldSpec = struct {
    op: WeightFoldOp,
    is_float: bool,
    view: thindb.storage.ColumnView,
    /// When the input column itself arrived with an RLE run sidecar, the
    /// run-native emitter folds `value × covered` off the runs instead of
    /// reading rows. Null → per-row fold. Only set on the run-native path.
    runs: ?thindb.exec.RunsColumn = null,
    /// The source rows already sum the rows their weights count.
    summed: bool = false,
};

fn buildWeightFoldSpecs(layout: GroupRowsLayout, batch: thindb.Batch, partials: []const ?ColumnView, out: []WeightFoldSpec) !void {
    for (layout.columns, 0..) |column, c| {
        if (column.source_name.len == 0) return error.UnsupportedOperatorForType;
        // Run partials are NULL-blind; the engine's weight gate already
        // declines nullable inputs.
        if (column.nullable) return error.UnsupportedOperatorForType;
        const partial = if (c < partials.len) partials[c] else null;
        const view = partial orelse batch.values[batch.columnIndex(column.source_name) orelse return error.ColumnNotFound];
        var op: ?WeightFoldOp = null;
        for (layout.aggregates) |agg| {
            if (agg.is_string or agg.is_distinct) continue;
            // Weight folds accumulate in single i64 cells — a wide aggregate
            // disqualifies the whole run-collapse program.
            if (agg.wide) return error.UnsupportedOperatorForType;
            const ic = agg.input_column_index orelse continue;
            if (ic != c or op != null) continue;
            op = switch (agg.op) {
                .sum, .avg => .sum,
                .min => .min,
                .max => .max,
                else => return error.UnsupportedOperatorForType,
            };
        }
        const fold_op = op orelse return error.UnsupportedOperatorForType;
        out[c] = .{
            .op = fold_op,
            .is_float = physicalIsFloat(column.physical_type),
            .view = view,
            .summed = partial != null and fold_op == .sum,
        };
    }
}

inline fn weightSrcInt(view: thindb.storage.ColumnView, r: usize) i64 {
    return switch (view.data) {
        inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |s| @intCast(s[r]),
        else => 0,
    };
}

inline fn weightSrcFloat(view: thindb.storage.ColumnView, r: usize) f64 {
    return switch (view.data) {
        inline .float, .double => |s| @floatCast(s[r]),
        else => 0,
    };
}

// Bytes past which a weighted chunk with string extremes publishes before it
// fills: an unfilled chunk of few keys would otherwise keep every improving
// extreme's bytes until the scan ends.
const FOLD_CHUNK_STR_BYTES: usize = 8 << 20;

// The staged row the fold index holds for `key`, or `idx` after pointing the
// key's slot at `idx`, the row the caller stages next.
inline fn foldIndexProbe(parts: *WorkerParts, active: *const RawRows, key: u128, idx: usize) usize {
    const mixed: u64 = @truncate(key ^ (key >> 64));
    const slot = &parts.fold_slots[@intCast((mixed *% 0x9E3779B97F4A7C15) >> parts.fold_shift)];
    const held = slot.*;
    if (held < idx and active.keyAt(held) == key) return held;
    slot.* = @intCast(idx);
    return idx;
}

// A hit saves staging, shipping and grouping a row, several times a probe's
// cost; a chunk whose probes hit under 1/8 of the time didn't earn them back
// (adjacent repeats fold without probing), so the worker stops probing.
fn noteFoldChunk(parts: *WorkerParts, probes: u64, hits: u64) void {
    const chunk_probes = parts.fold_probes + probes;
    const chunk_hits = parts.fold_hits + hits;
    parts.fold_probes = 0;
    parts.fold_hits = 0;
    if (chunk_hits * 8 < chunk_probes) parts.fold_off = true;
}

inline fn weightFoldRow(active: *RawRows, allocator: Allocator, aggregates: []const GroupAggregateSpec, specs: []const WeightFoldSpec, str_views: []const StringView, str_nulls: []const ?[]const u8, settled: StrColumnMask, dst: usize, src: usize, w: u32) !void {
    active.weightAll()[dst] += w;
    weightFoldScaled(active, specs, dst, src, w, false);
    if (str_views.len != 0) try weightFoldStr(active, allocator, aggregates, str_views, str_nulls, settled, dst, src);
}

// Fold source row `src`, standing for `w` equal rows, into staged row `idx`:
// a sum takes its value `w` times over (unless the value sums them already),
// an extreme takes it once.
fn weightFoldScaled(active: *RawRows, specs: []const WeightFoldSpec, idx: usize, src: usize, w: u32, init: bool) void {
    if (w == 1) return weightFoldRange(active, specs, idx, src, src + 1, init);
    for (specs, 0..) |spec, c| {
        if (spec.op != .sum or spec.summed) {
            weightFoldOne(active, spec, c, idx, src, src + 1, init);
        } else if (spec.is_float) {
            const col = @as([*]f64, @ptrCast(@alignCast(active.columnByteSlab(c).ptr)))[0..active.capacity_rows];
            const v = weightSrcFloat(spec.view, src) * @as(f64, @floatFromInt(w));
            if (init) col[idx] = v else col[idx] += v;
        } else {
            const col = @as([*]i64, @ptrCast(@alignCast(active.columnByteSlab(c).ptr)))[0..active.capacity_rows];
            const v = weightSrcInt(spec.view, src) * @as(i64, w);
            if (init) col[idx] = v else col[idx] += v;
        }
    }
}

/// One bit per string agg-input column.
const StrColumnMask = std.meta.Int(.unsigned, MAX_GROUP_STR_COLUMNS);

// Fold source row `src`'s string extremes into staged row `dst`. A NULL input
// changes nothing and a staged NULL takes the first valid value; a replaced
// value's bytes stay in the chunk's store until the chunk recycles. A
// `settled` column's staged value came from an earlier row of the same
// ascending run, so a MIN keeps it and a MAX takes the source unchecked.
fn weightFoldStr(active: *RawRows, allocator: Allocator, aggregates: []const GroupAggregateSpec, str_views: []const StringView, str_nulls: []const ?[]const u8, settled: StrColumnMask, dst: usize, src: usize) !void {
    const k = str_views.len;
    for (aggregates) |agg| {
        if (!agg.is_string) continue;
        const c = agg.str_input_index;
        if (!thindb.storage.column.isValidBit(str_nulls[c], src)) continue;
        if (!active.str.isNull(k, dst, c)) {
            const ordered = !agg.is_json and settled & (@as(StrColumnMask, 1) << @intCast(c)) != 0;
            if (ordered and agg.op == .min) continue;
            if (!ordered) {
                const cmp = json_binary.columnOrder(agg.is_json, str_views[c].rowBytes(src), active.str.get(k, dst, c));
                const is_better = if (agg.op == .min) cmp == .lt else cmp == .gt;
                if (!is_better) continue;
            }
        }
        try active.str.append(allocator, k, dst, c, str_views[c].rowBytes(src));
    }
}

// Fold source rows [r0, r1) into staged row `idx`: reduce the range in a
// register, then either initialize the staged partial (`init`, a freshly
// appended row) or merge into it (the run continues a prior staged row).
fn weightFoldRange(active: *RawRows, specs: []const WeightFoldSpec, idx: usize, r0: usize, r1: usize, init: bool) void {
    for (specs, 0..) |spec, c| weightFoldOne(active, spec, c, idx, r0, r1, init);
}

fn weightFoldOne(active: *RawRows, spec: WeightFoldSpec, c: usize, idx: usize, r0: usize, r1: usize, init: bool) void {
    {
        switch (spec.op) {
            inline else => |op| {
                if (spec.is_float) {
                    var acc: f64 = weightSrcFloat(spec.view, r0);
                    var r = r0 + 1;
                    while (r < r1) : (r += 1) {
                        const v = weightSrcFloat(spec.view, r);
                        switch (op) {
                            .sum => acc += v,
                            .min => if (v < acc) {
                                acc = v;
                            },
                            .max => if (v > acc) {
                                acc = v;
                            },
                        }
                    }
                    const col = @as([*]f64, @ptrCast(@alignCast(active.columnByteSlab(c).ptr)))[0..active.capacity_rows];
                    if (init) {
                        col[idx] = acc;
                    } else switch (op) {
                        .sum => col[idx] += acc,
                        .min => if (acc < col[idx]) {
                            col[idx] = acc;
                        },
                        .max => if (acc > col[idx]) {
                            col[idx] = acc;
                        },
                    }
                } else {
                    var acc: i64 = weightSrcInt(spec.view, r0);
                    var r = r0 + 1;
                    while (r < r1) : (r += 1) {
                        const v = weightSrcInt(spec.view, r);
                        switch (op) {
                            .sum => acc += v,
                            .min => if (v < acc) {
                                acc = v;
                            },
                            .max => if (v > acc) {
                                acc = v;
                            },
                        }
                    }
                    const col = @as([*]i64, @ptrCast(@alignCast(active.columnByteSlab(c).ptr)))[0..active.capacity_rows];
                    if (init) {
                        col[idx] = acc;
                    } else switch (op) {
                        .sum => col[idx] += acc,
                        .min => if (acc < col[idx]) {
                            col[idx] = acc;
                        },
                        .max => if (acc > col[idx]) {
                            col[idx] = acc;
                        },
                    }
                }
            },
        }
    }
}

// Span fold for the run-native emitter: a spec whose input column carries its
// own run sidecar folds `value × covered` straight off the run cursor (never
// reading expanded rows); a flat input falls back to the per-row reduce. The
// cursors advance monotonically with the caller's span walk.
fn weightFoldSpan(
    active: *RawRows,
    specs: []const WeightFoldSpec,
    spec_idx: *[MAX_GROUP_PAYLOAD_COLUMNS]usize,
    spec_left: *[MAX_GROUP_PAYLOAD_COLUMNS]usize,
    idx: usize,
    r0: usize,
    span: usize,
    init: bool,
) !void {
    for (specs, 0..) |spec, c| {
        const ir = spec.runs orelse {
            weightFoldOne(active, spec, c, idx, r0, r0 + span, init);
            continue;
        };
        switch (spec.op) {
            inline else => |op| {
                var acc: i64 = 0;
                var first = true;
                var remaining = span;
                while (remaining > 0) {
                    const take = @min(remaining, spec_left[c]);
                    if (take == 0) return error.UnsupportedOperatorForType;
                    const v = ir.values_i64[spec_idx[c]];
                    switch (op) {
                        .sum => acc += v * @as(i64, @intCast(take)),
                        .min => if (first or v < acc) {
                            acc = v;
                        },
                        .max => if (first or v > acc) {
                            acc = v;
                        },
                    }
                    first = false;
                    remaining -= take;
                    spec_left[c] -= take;
                    if (spec_left[c] == 0 and spec_idx[c] + 1 < ir.lengths.len) {
                        spec_idx[c] += 1;
                        spec_left[c] = ir.lengths[spec_idx[c]];
                    }
                }
                const col = @as([*]i64, @ptrCast(@alignCast(active.columnByteSlab(c).ptr)))[0..active.capacity_rows];
                if (init) {
                    col[idx] = acc;
                } else switch (op) {
                    .sum => col[idx] += acc,
                    .min => if (acc < col[idx]) {
                        col[idx] = acc;
                    },
                    .max => if (acc > col[idx]) {
                        col[idx] = acc;
                    },
                }
            },
        }
    }
}

// Append the string agg-input values for source row `src` into the active
// chunk's variable-length StrStore at staged index `dst`. Key-independent, so
// every append path (generic + fast key kernels) calls it identically. A no-op
// for numeric queries (`str_views.len == 0`), so the caller's guard keeps it off
// the hot path entirely.
inline fn appendStrPayload(active: *RawRows, allocator: Allocator, str_views: []const StringView, str_nulls: []const ?[]const u8, dst: usize, src: usize) !void {
    for (str_views, 0..) |sv, col| {
        if (!thindb.storage.column.isValidBit(str_nulls[col], src)) {
            active.str.appendNull(str_views.len, dst, col);
            continue;
        }
        try active.str.append(allocator, str_views.len, dst, col, sv.rowBytes(src));
    }
}

fn publishActiveRawRows(parts: *WorkerParts, shared: *PipeShared, raw_chunk_rows: usize) !void {
    if (shared.serial_fold_scratch) |scratch| return foldActiveRawRows(parts, shared, scratch);
    if (shared.raw_scan_queues.len > 0) {
        const qidx = chooseRawScanPublishLane(shared, parts.raw_scan_lane, @max(@as(usize, 1), raw_chunk_rows / 2));
        try publishRawRowsToQueue(shared, &shared.raw_scan_queues[qidx], parts.worker_index, &parts.raw_active_rows, raw_chunk_rows, &shared.outstanding_chunks, &shared.pending_group_rows, &parts.raw_scan_queue_lock_ticks, &parts.raw_recycle_lock_ticks);
    } else {
        try publishRawRows(shared, parts.worker_index, &parts.raw_active_rows, raw_chunk_rows, &parts.raw_queue_lock_ticks, &parts.raw_recycle_lock_ticks);
    }
    parts.published_chunks += 1;
}

fn foldActiveRawRows(parts: *WorkerParts, shared: *PipeShared, scratch: *GroupScratch) !void {
    const staged = parts.raw_active_rows;
    if (staged.len() == 0) return;
    const rows = GroupRows{
        .slab = staged.slab,
        .len_rows = staged.len_rows,
        .capacity_rows = staged.capacity_rows,
        .layout = staged.layout,
        .str = staged.str,
    };
    try shared.buckets[0].fold_rows(shared.allocator, scratch, rows, shared.input_rows_per_bucket);
    parts.raw_active_rows.clearRetainingCapacity();
    parts.published_chunks += 1;
}

fn appendBatchRawChunks(parts: *WorkerParts, shared: *PipeShared, batch: thindb.Batch, weights: ?RowWeights, hashed_ids: ?[]const u128, raw_chunk_rows: usize, profile: bool, skip_filter_check: bool) !void {
    return appendBatchRawChunksGeneric(parts, shared, batch, weights, hashed_ids, raw_chunk_rows, profile, skip_filter_check);
}

fn initGroupStateCountSumAvg(states: *StateSlab, key: u128, sum_acc: i64, avg_acc: i64, run_len: u64) u32 {
    const gid = states.pushAssumeCapacity();
    const head = states.head(gid);
    head.key = key;
    head.count = run_len;
    const slots = states.slotsOf(gid);
    slots[0] = sum_acc;
    slots[1] = avg_acc;
    return gid;
}

inline fn updateGroupStateCountSumAvg(ref: StateRef, sum_acc: i64, avg_acc: i64, run_len: u64) void {
    ref.head.count += run_len;
    ref.slots[0] += sum_acc;
    ref.slots[1] += avg_acc;
}

fn initGroupStateProgram(
    states: *StateSlab,
    key: u128,
    aggregates: []const GroupAggregateSpec,
    rows: GroupRows,
    row_idx: usize,
) !u32 {
    const gid = states.pushAssumeCapacity();
    states.head(gid).key = key;
    try updateGroupStateProgram(states.ref(gid), aggregates, rows, row_idx);
    return gid;
}

fn updateGroupStateProgram(ref: StateRef, aggregates: []const GroupAggregateSpec, rows: GroupRows, row_idx: usize) !void {
    ref.head.count += 1;
    for (aggregates) |agg| {
        // String MIN/MAX is folded separately into the side str_states array,
        // GROUP_CONCAT into the side concat_states, COUNT(DISTINCT) into the
        // side distinct_sets, and UDAFs into the side udf_states (the numeric
        // value/present slots are written at finalize, not here).
        if (agg.is_string or agg.is_distinct or agg.is_concat or agg.is_udf) continue;
        if (agg.state_index >= MAX_GROUP_AGG_STATES) return error.UnsupportedOperatorForType;
        const state_index: usize = agg.state_index;
        // NULL inputs don't fold (SQL: aggregates skip NULLs). The companion
        // valid-count slot carries the group's non-null input count — the AVG
        // denominator, the MIN/MAX first-value test, and the all-NULL → NULL
        // finalize signal at emit.
        const input_ok = aggInputValid(rows, agg, row_idx);
        switch (agg.op) {
            .count_star => {
                if (state_index != 0) try setAggregateStateValue(ref, state_index, ref.head.count);
            },
            .count_col => {
                _ = aggregateInputValue(agg, rows, row_idx) catch return error.UnsupportedOperatorForType;
                if (agg.nullable) {
                    if (state_index == 0) return error.UnsupportedOperatorForType;
                    if (input_ok) try addAggregateStateValue(ref, state_index, 1);
                } else if (state_index != 0) {
                    try setAggregateStateValue(ref, state_index, ref.head.count);
                }
            },
            .sum, .avg => {
                if (!input_ok) continue;
                if (aggInputIsFloat(rows, agg)) {
                    const value = try aggregateInputFloat(agg, rows, row_idx);
                    try addFloatStateValue(ref, state_index, value);
                } else if (agg.wide) {
                    const value = try aggregateInputValue(agg, rows, row_idx);
                    try addWideStateValue(ref, state_index, value);
                } else {
                    const value = try aggregateInputValue(agg, rows, row_idx);
                    try addAggregateStateValue(ref, state_index, value);
                }
                if (agg.valid_count_index != 0) try addAggregateStateValue(ref, agg.valid_count_index, 1);
            },
            .min => {
                if (!input_ok) continue;
                const first = if (agg.valid_count_index != 0)
                    aggregateStateValue(ref, agg.valid_count_index) == 0
                else
                    ref.head.count == 1;
                if (aggInputIsFloat(rows, agg)) {
                    const value = try aggregateInputFloat(agg, rows, row_idx);
                    if (first or value < floatStateValue(ref, state_index)) try setFloatStateValue(ref, state_index, value);
                } else {
                    const value = try aggregateInputValue(agg, rows, row_idx);
                    if (first or value < aggregateStateValue(ref, state_index)) try setAggregateStateValue(ref, state_index, value);
                }
                if (agg.valid_count_index != 0) try addAggregateStateValue(ref, agg.valid_count_index, 1);
            },
            .max => {
                if (!input_ok) continue;
                const first = if (agg.valid_count_index != 0)
                    aggregateStateValue(ref, agg.valid_count_index) == 0
                else
                    ref.head.count == 1;
                if (aggInputIsFloat(rows, agg)) {
                    const value = try aggregateInputFloat(agg, rows, row_idx);
                    if (first or value > floatStateValue(ref, state_index)) try setFloatStateValue(ref, state_index, value);
                } else {
                    const value = try aggregateInputValue(agg, rows, row_idx);
                    if (first or value > aggregateStateValue(ref, state_index)) try setAggregateStateValue(ref, state_index, value);
                }
                if (agg.valid_count_index != 0) try addAggregateStateValue(ref, agg.valid_count_index, 1);
            },
            .welford => {
                if (!input_ok) continue;
                const x: f64 = if (aggInputIsFloat(rows, agg))
                    try aggregateInputFloat(agg, rows, row_idx)
                else
                    @floatFromInt(try aggregateInputValue(agg, rows, row_idx));
                const n = aggregateStateValue(ref, state_index) + 1;
                try setAggregateStateValue(ref, state_index, n);
                const mean_prev = floatStateValue(ref, state_index + 1);
                const delta = x - mean_prev;
                const mean = mean_prev + delta / @as(f64, @floatFromInt(@as(i64, @intCast(n))));
                try setFloatStateValue(ref, state_index + 1, mean);
                try setFloatStateValue(ref, state_index + 2, floatStateValue(ref, state_index + 2) + delta * (x - mean));
            },
            // Folded into the side distinct_sets / concat_states / udf_states;
            // the guards above mean control never reaches here.
            .count_distinct, .concat, .udf => unreachable,
        }
    }
}

inline fn aggInputValid(rows: GroupRows, agg: GroupAggregateSpec, row_idx: usize) bool {
    if (!agg.nullable) return true;
    const ic = agg.input_column_index orelse return true;
    const spec = rows.layout.columns[ic];
    if (!spec.nullable) return true;
    return rows.validAll(spec.valid_index)[row_idx] != 0;
}

fn aggregateInputValue(agg: GroupAggregateSpec, rows: GroupRows, row_idx: usize) !i128 {
    const input_index = agg.input_column_index orelse return error.UnsupportedOperatorForType;
    if (input_index >= rows.layout.columns.len) return error.UnsupportedOperatorForType;
    return rows.columnIntAt(input_index, row_idx);
}

/// An integer aggregate-input column, resolved once ahead of a row loop so
/// the loop reads it without recomputing the slab's column offsets per row.
const IntColumn = union(enum) {
    i8: []const i8,
    i16: []const i16,
    i32: []const i32,
    i64: []const i64,

    fn of(rows: GroupRows, column_index: usize) !IntColumn {
        return switch (rows.layout.columns[column_index].physical_type) {
            .i8 => .{ .i8 = rows.columnTypedAll(i8, column_index) },
            .i16 => .{ .i16 = rows.columnTypedAll(i16, column_index) },
            .i32 => .{ .i32 = rows.columnTypedAll(i32, column_index) },
            .i64 => .{ .i64 = rows.columnTypedAll(i64, column_index) },
            .f32, .f64 => error.UnsupportedOperatorForType,
        };
    }

    inline fn at(self: IntColumn, row: usize) i64 {
        return switch (self) {
            inline else => |values| values[row],
        };
    }
};

inline fn physicalIsFloat(pt: GroupColumnType) bool {
    return pt == .f32 or pt == .f64;
}

inline fn aggInputIsFloat(rows: GroupRows, agg: GroupAggregateSpec) bool {
    const input_index = agg.input_column_index orelse return false;
    if (input_index >= rows.layout.columns.len) return false;
    return physicalIsFloat(rows.layout.columns[input_index].physical_type);
}

fn aggregateInputFloat(agg: GroupAggregateSpec, rows: GroupRows, row_idx: usize) !f64 {
    const input_index = agg.input_column_index orelse return error.UnsupportedOperatorForType;
    if (input_index >= rows.layout.columns.len) return error.UnsupportedOperatorForType;
    return rows.columnFloatAt(input_index, row_idx);
}

// Float aggregates store an f64 accumulator in the i64 slot via @bitCast
// (state_index 0 is the integer counter and is never used for a float agg).
inline fn floatStateValue(ref: StateRef, state_index: usize) f64 {
    const bits: i64 = if (state_index >= 1 and state_index - 1 < ref.slots.len) ref.slots[state_index - 1] else 0;
    return @bitCast(bits);
}

inline fn addFloatStateValue(ref: StateRef, state_index: usize, value: f64) !void {
    try setFloatStateValue(ref, state_index, floatStateValue(ref, state_index) + value);
}

inline fn setFloatStateValue(ref: StateRef, state_index: usize, value: f64) !void {
    if (state_index == 0 or state_index - 1 >= ref.slots.len) return error.UnsupportedOperatorForType;
    ref.slots[state_index - 1] = @bitCast(value);
}

inline fn aggregateStateValue(ref: StateRef, state_index: usize) i128 {
    if (state_index == 0) return @intCast(ref.head.count);
    return ref.slots[state_index - 1];
}

/// Wide (i128) accumulator spanning two slots: lo u64 bits at
/// `state_index - 1`, hi i64 at `state_index`. Reassembly is the inverse of
/// `addWideStateValue`'s split.
pub inline fn wideStateValue(slots: []const i64, state_index: usize) i128 {
    const lo: u64 = @bitCast(slots[state_index - 1]);
    const hi: i64 = slots[state_index];
    return (@as(i128, hi) << 64) | @as(i128, lo);
}

inline fn addWideStateValue(ref: StateRef, state_index: usize, value: i128) !void {
    if (state_index == 0 or state_index >= ref.slots.len) return error.UnsupportedOperatorForType;
    const nv = wideStateValue(ref.slots, state_index) + value;
    ref.slots[state_index - 1] = @bitCast(@as(u64, @truncate(@as(u128, @bitCast(nv)))));
    ref.slots[state_index] = @intCast(nv >> 64);
}

inline fn addAggregateStateValue(ref: StateRef, state_index: usize, value: i128) !void {
    if (state_index == 0) {
        ref.head.count += @intCast(value);
        return;
    }
    if (state_index - 1 >= ref.slots.len) return error.UnsupportedOperatorForType;
    ref.slots[state_index - 1] += @intCast(value);
}

inline fn setAggregateStateValue(ref: StateRef, state_index: usize, value: i128) !void {
    if (state_index == 0) {
        ref.head.count = @intCast(value);
        return;
    }
    if (state_index - 1 >= ref.slots.len) return error.UnsupportedOperatorForType;
    ref.slots[state_index - 1] = @intCast(value);
}

fn validateGroupAggregateProgram(aggregates: []const GroupAggregateSpec, column_count: usize) !void {
    if (aggregates.len == 0) return error.UnsupportedOperatorForType;
    for (aggregates) |agg| {
        // String MIN/MAX and GROUP_CONCAT have no numeric slot/payload column;
        // their inputs are str_columns entries validated by the layout. UDAF
        // args/slots are validated by the planner (the silo just folds them).
        if (agg.is_string or agg.is_concat or agg.is_udf) continue;
        // A wide aggregate's hi half occupies the NEXT slot too.
        if (agg.state_index + @as(u16, @intFromBool(agg.wide)) >= MAX_GROUP_AGG_STATES) return error.UnsupportedOperatorForType;
        if (agg.wide and (agg.state_index == 0 or (agg.op != .sum and agg.op != .avg))) return error.UnsupportedOperatorForType;
        if (agg.valid_count_index >= MAX_GROUP_AGG_STATES) return error.UnsupportedOperatorForType;
        if (agg.is_distinct and agg.distinct_state_index >= MAX_GROUP_DISTINCT_SLOTS) return error.UnsupportedOperatorForType;
        switch (agg.op) {
            .count_star => {},
            .count_col, .sum, .avg, .min, .max, .count_distinct => {
                // A string COUNT(DISTINCT) reads a str_columns lane, not a payload.
                if (agg.distinct_str) continue;
                const input_index = agg.input_column_index orelse return error.UnsupportedOperatorForType;
                if (input_index >= column_count) return error.UnsupportedOperatorForType;
            },
            .welford => {
                const input_index = agg.input_column_index orelse return error.UnsupportedOperatorForType;
                if (input_index >= column_count) return error.UnsupportedOperatorForType;
                if (agg.state_index == 0 or agg.state_index + 2 >= MAX_GROUP_AGG_STATES) return error.UnsupportedOperatorForType;
            },
            .concat, .udf => unreachable,
        }
    }
}

fn countSumAvgProgram(aggregates: []const GroupAggregateSpec, column_count: usize) ?CountSumAvgProgram {
    if (aggregates.len != 3) return null;
    // The fused kernel accumulates in single i64 slots with no validity
    // checks — wide and nullable state stay on the generic per-row program.
    for (aggregates) |a| if (a.wide or a.nullable) return null;
    if (aggregates[0].op != .count_star or aggregates[0].state_index != 0 or aggregates[0].input_column_index != null) return null;
    if (aggregates[1].op != .sum or aggregates[1].state_index != 1) return null;
    if (aggregates[2].op != .avg or aggregates[2].state_index != 2) return null;
    const sum_input_index = aggregates[1].input_column_index orelse return null;
    const avg_input_index = aggregates[2].input_column_index orelse return null;
    if (sum_input_index >= column_count or avg_input_index >= column_count) return null;
    return .{ .sum_input_index = sum_input_index, .avg_input_index = avg_input_index };
}

// The number of runtime accumulator slots the program needs: the max numeric
// state_index (slot `state_index - 1`), so count-only is 0, count+sum+avg is 2.
// A wide aggregate's hi half lives at slot `state_index`, one past its lo.
fn aggSlotCount(layout: GroupRowsLayout) usize {
    var n: usize = 0;
    for (layout.aggregates) |agg| {
        if (agg.is_string or agg.is_concat) continue;
        var top = agg.state_index + @as(u16, @intFromBool(agg.wide));
        if (agg.op == .welford) top = agg.state_index + 2;
        // A UDAF writes its finalized value at slot `state_index - 1` and a
        // companion present bit at slot `state_index` (the next one).
        if (agg.is_udf) top = agg.state_index + 1;
        if (top > n) n = top;
        if (agg.valid_count_index > n) n = agg.valid_count_index;
    }
    return n;
}

fn groupChunkRowsDirect(
    table: *BucketTable,
    states: *StateSlab,
    str_states: *std.ArrayListUnmanaged(StrAccRow),
    concat_states: *std.ArrayListUnmanaged(ConcatRow),
    udf_states: *std.ArrayListUnmanaged(UdfStateRow),
    udf_arena: Allocator,
    distinct_sets: *DistinctSlots,
    scratch: *GroupScratch,
    allocator: Allocator,
    str_arena: Allocator,
    str_bank: *StringBank,
    rows: GroupRows,
    expected_groups: usize,
    input_rows: u64,
    observed_rows: u64,
    allocation_profile: ?*GroupAllocationProfile,
) !void {
    const n = rows.len();
    if (n == 0) return;
    const allocation_t0 = if (allocation_profile != null) platform.nowTicks() else 0;
    try states.prepare(allocator, aggSlotCount(rows.layout));
    try table.fit(allocator, rows.layout, expected_groups);
    if (try table.growFor(allocator, n, expected_groups, input_rows, observed_rows)) {
        if (allocation_profile) |profile| profile.table_growths += 1;
    }
    try states.ensureUnusedCapacity(allocator, n);
    if (hasStringResults(rows.layout)) try str_states.ensureUnusedCapacity(allocator, n);
    if (rows.layout.has_concat) try concat_states.ensureUnusedCapacity(allocator, n);
    if (rows.layout.has_udf) try udf_states.ensureUnusedCapacity(allocator, n);
    if (allocation_profile) |profile| profile.ticks += platform.nowTicks() - allocation_t0;
    if (rows.layout.distinct_slot_count > MAX_GROUP_DISTINCT_SLOTS) return error.UnsupportedOperatorForType;

    if (rows.layout.columns.len > MAX_GROUP_PAYLOAD_COLUMNS) return error.UnsupportedOperatorForType;
    try validateGroupAggregateProgram(rows.layout.aggregates, rows.layout.columns.len);
    const rowrefs: []const i64 = if (rows.layout.has_rowref) rows.rowrefAll()[0..n] else &.{};
    if (countSumAvgProgram(rows.layout.aggregates, rows.layout.columns.len)) |program| blk: {
        // The CountSumAvg fast path reads inputs as i64; float inputs route to
        // the generic per-aggregate program path instead.
        if (physicalIsFloat(rows.layout.columns[program.sum_input_index].physical_type) or
            physicalIsFloat(rows.layout.columns[program.avg_input_index].physical_type)) break :blk;
        switch (rows.layout.key_width) {
            .u32 => try groupChunkRowsDirectKeysCountSumAvg(.u32, &table.narrow, states, rows.keyU32All()[0..n], &.{}, n, rows, program, rowrefs),
            .u64 => try groupChunkRowsDirectKeysCountSumAvg(.u64, &table.narrow, states, rows.keyU64All()[0..n], &.{}, n, rows, program, rowrefs),
            .u96 => try groupChunkRowsDirectKeysCountSumAvg(.u96, &table.narrow, states, rows.keyU96LoAll()[0..n], rows.keyU96HiAll()[0..n], n, rows, program, rowrefs),
            .u128 => switch (table.*) {
                inline else => |*t| try groupChunkRowsDirectKeysCountSumAvg(.u128, t, states, rows.keyU128All()[0..n], &.{}, n, rows, program, rowrefs),
            },
        }
        return;
    }
    switch (rows.layout.key_width) {
        .u32 => try groupChunkRowsDirectKeys(.u32, &table.narrow, states, str_states, concat_states, udf_states, udf_arena, str_arena, str_bank, distinct_sets, &scratch.gids, allocator, rows.keyU32All()[0..n], &.{}, n, rows.layout.aggregates, rows, rowrefs),
        .u64 => try groupChunkRowsDirectKeys(.u64, &table.narrow, states, str_states, concat_states, udf_states, udf_arena, str_arena, str_bank, distinct_sets, &scratch.gids, allocator, rows.keyU64All()[0..n], &.{}, n, rows.layout.aggregates, rows, rowrefs),
        .u96 => try groupChunkRowsDirectKeys(.u96, &table.narrow, states, str_states, concat_states, udf_states, udf_arena, str_arena, str_bank, distinct_sets, &scratch.gids, allocator, rows.keyU96LoAll()[0..n], rows.keyU96HiAll()[0..n], n, rows.layout.aggregates, rows, rowrefs),
        .u128 => switch (table.*) {
            inline else => |*t| try groupChunkRowsDirectKeys(.u128, t, states, str_states, concat_states, udf_states, udf_arena, str_arena, str_bank, distinct_sets, &scratch.gids, allocator, rows.keyU128All()[0..n], &.{}, n, rows.layout.aggregates, rows, rowrefs),
        },
    }
}

fn hasStringExtreme(aggregates: []const GroupAggregateSpec) bool {
    for (aggregates) |agg| if (agg.is_string) return true;
    return false;
}

// Whether each group carries string results (string MIN/MAX, GROUP_CONCAT) in
// the side `str_states` row. A string COUNT(DISTINCT) also stages the string
// lane but emits a number, so a layout whose only string reader is a distinct
// keeps the numeric per-group state, top-N and all-groups emit.
fn hasStringResults(layout: GroupRowsLayout) bool {
    return layout.has_str_payload and (layout.has_concat or hasStringExtreme(layout.aggregates));
}

// Fold one row's string MIN/MAX values into group `gid`'s side accumulators.
// A new extreme is copied into `str_bank`, over the block of the value it
// replaces when that fits, so the compare reads of the current value stay
// cache-local. Called only when the layout has string aggs.
fn foldGroupStr(str_states: *std.ArrayListUnmanaged(StrAccRow), str_bank: *StringBank, str_arena: Allocator, gid: usize, aggregates: []const GroupAggregateSpec, rows: GroupRows, row_idx: usize) !void {
    const k = rows.layout.str_columns.len;
    for (aggregates) |agg| {
        if (!agg.is_string) continue;
        // NULL string inputs don't fold; a group whose inputs were all NULL
        // keeps `present == false` and emits NULL.
        if (rows.str.isNull(k, row_idx, agg.str_input_index)) continue;
        const b = rows.str.get(k, row_idx, agg.str_input_index);
        const acc = &str_states.items[gid][agg.str_state_index];
        if (!acc.present) {
            acc.bytes = try str_bank.replace(str_arena, "", b);
            acc.present = true;
        } else {
            const cmp = json_binary.columnOrder(agg.is_json, b, acc.bytes);
            const is_better = if (agg.op == .min) cmp == .lt else cmp == .gt;
            if (is_better) acc.bytes = try str_bank.replace(str_arena, acc.bytes, b);
        }
    }
}

// Collect one row's GROUP_CONCAT inputs into group `gid`'s side cells: NULL
// inputs skip; a kept value records (rowloc, arena-dup'd bytes) so the emit
// can join in physical row order. Called only when the layout has concat aggs.
fn foldGroupConcat(concat_states: *std.ArrayListUnmanaged(ConcatRow), str_arena: Allocator, gid: usize, aggregates: []const GroupAggregateSpec, rows: GroupRows, row_idx: usize) !void {
    const k = rows.layout.str_columns.len;
    const rowrefs = rows.rowrefAll();
    for (aggregates) |agg| {
        if (!agg.is_concat) continue;
        if (rows.str.isNull(k, row_idx, agg.str_input_index)) continue;
        const b = rows.str.get(k, row_idx, agg.str_input_index);
        const cell = &concat_states.items[gid][agg.concat_state_index];
        try cell.append(str_arena, .{ .order = rowrefs[row_idx], .bytes = try str_arena.dupe(u8, b) });
    }
}

// Build a ColumnView over staged aggregate-input column `ic`'s slab for the
// whole chunk. The arg is a non-nullable numeric (gated in the planner), so the
// view carries no validity bitmap and the union tag follows the UDAF's declared
// arg type. `update_one` indexes it by the chunk-relative row.
fn reconstructUdfArgView(rows: GroupRows, ic: u16, arg_type: types_mod.Type, n: usize) ColumnView {
    const data: storage_mod.column.ValueView = switch (arg_type) {
        .double => .{ .double = rows.columnTypedAll(f64, ic)[0..n] },
        .float => .{ .float = rows.columnTypedAll(f32, ic)[0..n] },
        .bigint => .{ .bigint = rows.columnTypedAll(i64, ic)[0..n] },
        .int => .{ .int = rows.columnTypedAll(i32, ic)[0..n] },
        .smallint => .{ .smallint = rows.columnTypedAll(i16, ic)[0..n] },
        .tinyint => .{ .tinyint = rows.columnTypedAll(i8, ic)[0..n] },
        .boolean => .{ .boolean = @ptrCast(rows.columnTypedAll(i8, ic)[0..n]) },
        .date => .{ .date = rows.columnTypedAll(i32, ic)[0..n] },
        .datetime => .{ .datetime = rows.columnTypedAll(i64, ic)[0..n] },
        .decimal64 => .{ .decimal64 = rows.columnTypedAll(i64, ic)[0..n] },
        // Unreachable: the planner declines any UDAF arg whose type isn't one
        // of the above before this layout is ever built.
        else => unreachable,
    };
    return .{ .data = data, .nulls = null };
}

// Initialize group `gid`'s opaque UDAF state cells: bump a state_size blob from
// the bucket's udf_arena and run the UDAF's `init`. Called when a new group is
// created. The freshly-appended UdfStateRow starts all-uninitialized.
fn initUdfStatesRow(
    udf_states: *std.ArrayListUnmanaged(UdfStateRow),
    udf_arena: Allocator,
    gid: usize,
    aggregates: []const GroupAggregateSpec,
) !void {
    const row = &udf_states.items[gid];
    for (aggregates) |agg| {
        if (!agg.is_udf) continue;
        const entry = agg.udf_entry orelse return error.UnsupportedOperatorForType;
        const cell = &row[agg.udf_state_index];
        const bytes = try udf_arena.alignedAlloc(u8, .@"16", entry.state_size);
        const ctx = udf_mod.AggregateContext{ .allocator = udf_arena, .user_data = entry.user_data };
        try entry.init(&ctx, @ptrCast(bytes.ptr));
        cell.bytes = bytes;
        cell.initialized = true;
    }
}

// Fold one staged row into group `gid`'s UDAF states. Reconstructs each arg's
// ColumnView over the chunk slab and drives `update_one` at the row index.
fn foldGroupUdf(
    udf_states: *std.ArrayListUnmanaged(UdfStateRow),
    udf_arena: Allocator,
    gid: usize,
    aggregates: []const GroupAggregateSpec,
    rows: GroupRows,
    row_idx: usize,
    chunk_rows: usize,
) !void {
    const cells = &udf_states.items[gid];
    for (aggregates) |agg| {
        if (!agg.is_udf) continue;
        const entry = agg.udf_entry orelse return error.UnsupportedOperatorForType;
        var arg_buf: [MAX_GROUP_UDF_ARGS]ColumnView = undefined;
        const args = arg_buf[0..agg.udf_arg_count];
        for (0..agg.udf_arg_count) |k| {
            args[k] = reconstructUdfArgView(rows, agg.udf_arg_input_indices[k], entry.arg_types[k], chunk_rows);
        }
        const ctx = udf_mod.AggregateContext{ .allocator = udf_arena, .user_data = entry.user_data };
        try entry.update_one(&ctx, @ptrCast(cells[agg.udf_state_index].bytes.ptr), args, row_idx);
    }
}

// Fold one row's COUNT(DISTINCT) inputs into group `gid`. Each distinct field's
// combined set is probed with the exact (gid,value) composite; a never-before-seen
// pair bumps that aggregate's running count slot. Called under the bucket agg_lock.
fn groupChunkRowsDirectKeysCountSumAvg(
    comptime key_width: GroupKeyWidth,
    table: anytype,
    states: *StateSlab,
    keys: anytype,
    key_hi: []const u32,
    row_count: usize,
    rows: GroupRows,
    program: CountSumAvgProgram,
    rowrefs: []const i64,
) !void {
    // Weighted staged rows carry pre-summed run partials in their i64-widened
    // columns and the run's row count in the weight — the register
    // accumulation below is identical; only run_len comes from the weights.
    const weights: []const u32 = if (rows.layout.has_weight) rows.weightAll()[0..row_count] else &.{};
    const sum_input = try IntColumn.of(rows, program.sum_input_index);
    const avg_input = try IntColumn.of(rows, program.avg_input_index);
    const prefetch = prefetchesSlots(table);
    var r: usize = 0;
    while (r < row_count) {
        const pf = r + PREFETCH_DIST_BUCKET;
        if (prefetch and pf < row_count) {
            const pf_key = groupKeyAt(key_width, keys, key_hi, pf);
            @prefetch(table.slotAddr(table.bucketOf(@TypeOf(table.*).hashKey(pf_key))), .{ .rw = .write, .locality = 1 });
        }

        // The table is physically ordered, so clustered group keys arrive in
        // adjacent-equal runs: accumulate the run in registers and touch the
        // hash table + state record ONCE per run, not once per row.
        const key = groupKeyAt(key_width, keys, key_hi, r);
        var run_end = r + 1;
        while (run_end < row_count and groupKeyAt(key_width, keys, key_hi, run_end) == key) run_end += 1;
        var sum_acc: i64 = 0;
        var avg_acc: i64 = 0;
        var rr = r;
        while (rr < run_end) : (rr += 1) {
            sum_acc += sum_input.at(rr);
            avg_acc += avg_input.at(rr);
        }
        const run_len: u64 = if (weights.len != 0) blk: {
            var s: u64 = 0;
            for (weights[r..run_end]) |w| s += w;
            break :blk s;
        } else @intCast(run_end - r);
        const probe = table.getOrPut(@TypeOf(table.*).hashKey(key), key);
        if (!probe.found) {
            const new_gid = initGroupStateCountSumAvg(states, key, sum_acc, avg_acc, run_len);
            if (rowrefs.len != 0) states.head(new_gid).rowref = rowrefs[r];
            table.commit(probe.slot, key, new_gid);
        } else {
            updateGroupStateCountSumAvg(states.ref(probe.gid), sum_acc, avg_acc, run_len);
        }
        r = run_end;
    }
}

fn group_count_prefetched(comptime width: GroupKeyWidth, table: anytype, states: *StateSlab, keys: anytype, key_hi: []const u32, n: usize, weights: []const u32, rowrefs: []const i64) void {
    var pending: PendingCountUpdates = .{};
    const prefetch = prefetchesSlots(table);
    var r: usize = 0;
    while (r < n) {
        const pf = r + PREFETCH_DIST_BUCKET;
        if (prefetch and pf < n) {
            const key = groupKeyAt(width, keys, key_hi, pf);
            @prefetch(table.slotAddr(table.bucketOf(@TypeOf(table.*).hashKey(key))), .{ .rw = .write, .locality = 1 });
        }
        const key = groupKeyAt(width, keys, key_hi, r);
        var end = r + 1;
        while (end < n and groupKeyAt(width, keys, key_hi, end) == key) end += 1;
        const count: u64 = if (weights.len == 0) end - r else blk: {
            var sum: u64 = 0;
            for (weights[r..end]) |weight| sum += weight;
            break :blk sum;
        };
        const probe = table.getOrPut(@TypeOf(table.*).hashKey(key), key);
        if (probe.found) {
            pending.append(states.head(probe.gid), count);
        } else {
            const gid = states.pushAssumeCapacity();
            const head = states.head(gid);
            head.key = key;
            head.count = count;
            if (rowrefs.len != 0) head.rowref = rowrefs[r];
            table.commit(probe.slot, key, gid);
        }
        r = end;
    }
    pending.flush();
}

fn groupChunkRowsDirectKeys(
    comptime key_width: GroupKeyWidth,
    table: anytype,
    states: *StateSlab,
    str_states: *std.ArrayListUnmanaged(StrAccRow),
    concat_states: *std.ArrayListUnmanaged(ConcatRow),
    udf_states: *std.ArrayListUnmanaged(UdfStateRow),
    udf_arena: Allocator,
    str_arena: Allocator,
    str_bank: *StringBank,
    distinct_sets: *DistinctSlots,
    gids_buf: *std.ArrayListUnmanaged(u32),
    allocator: Allocator,
    keys: anytype,
    key_hi: []const u32,
    row_count: usize,
    aggregates: []const GroupAggregateSpec,
    rows: GroupRows,
    rowrefs: []const i64,
) !void {
    const has_str = hasStringResults(rows.layout);
    const has_str_fold = rows.layout.has_str_payload and hasStringExtreme(aggregates);
    const has_concat = rows.layout.has_concat;
    const has_distinct = rows.layout.distinct_slot_count > 0;
    const n = row_count;

    // Classify the program so the row loop carries only what it must: bare
    // COUNT bumps fold inline in the probe pass; SUM/AVG/MIN/MAX run as
    // per-aggregate monomorphic kernels over the recorded gid array (pass 2) —
    // the op dispatch and the physical-type switch hoisted out of the row
    // loop, with the state-record misses overlapped by a look-ahead prefetch.
    // A shape the kernels can't express (a non-count aggregate aimed at state
    // slot 0) keeps the original per-row program.
    var kernelizable = true;
    var needs_kernels = false;
    var has_extreme = false;
    var has_mirror = false;
    // UDAFs fold their opaque state per row in the generic program; the
    // monomorphic kernels have no form for it.
    if (rows.layout.has_udf) kernelizable = false;
    for (aggregates) |agg| {
        if (agg.is_string or agg.is_distinct or agg.is_concat or agg.is_udf) continue;
        if (agg.state_index >= MAX_GROUP_AGG_STATES) return error.UnsupportedOperatorForType;
        switch (agg.op) {
            .count_star, .count_col => {
                if (agg.state_index != 0) has_mirror = true;
                // A nullable COUNT(col) accumulates its own slot (it is NOT a
                // mirror of the group row count); per-row program only.
                if (agg.nullable) kernelizable = false;
            },
            .sum, .avg, .min, .max => {
                needs_kernels = true;
                if (agg.op == .min or agg.op == .max) has_extreme = true;
                if (agg.state_index == 0) kernelizable = false;
                // A nullable input's first-touch and AVG denominator live in
                // its valid-count slot; without one only the program folds it.
                if (agg.nullable and agg.valid_count_index == 0) kernelizable = false;
            },
            // Welford's three-slot sequential update has no kernel form.
            .welford => kernelizable = false,
            .count_distinct, .concat, .udf => unreachable,
        }
    }
    if (!kernelizable) {
        return groupChunkRowsDirectKeysProgram(key_width, table, states, str_states, concat_states, udf_states, udf_arena, str_arena, str_bank, distinct_sets, gids_buf, allocator, keys, key_hi, n, aggregates, rows, rowrefs);
    }

    // Keep the small-state and mixed-aggregate loops independent of the
    // deferred count buffer's branches and live registers.
    if (!needs_kernels and !has_mirror and !has_distinct and !has_str and !has_concat and !rows.layout.has_udf and
        states.len * states.stride >= PendingCountUpdates.MIN_STATE_BYTES)
    {
        group_count_prefetched(key_width, table, states, keys, key_hi, n, if (rows.layout.has_weight) rows.weightAll()[0..n] else &.{}, rowrefs);
        return;
    }

    // The gid array feeds the aggregate kernels and the COUNT(DISTINCT) pass 2
    // (the set probe is the cache-miss bottleneck; the group table is small
    // and stays cache-resident). MIN/MAX first-touch is carried as a tag bit
    // on the creating row's gid (the chunk is the group's first ever — its
    // record was zero-initialized — exactly when the bit is set).
    const want_gids = has_distinct or needs_kernels;
    if (want_gids) try gids_buf.resize(allocator, n);
    const gids: []u32 = if (want_gids) gids_buf.items[0..n] else &.{};
    const mark_new = has_extreme;
    // Weighted staged rows (COUNT-only layouts): each row is a collapsed run
    // from the scan emitter; its weight is the row count it stands for.
    const weights: []const u32 = if (rows.layout.has_weight) rows.weightAll()[0..n] else &.{};

    const prefetch = prefetchesSlots(table);
    var r: usize = 0;
    while (r < n) {
        const pf = r + PREFETCH_DIST_BUCKET;
        if (prefetch and pf < n) {
            const pf_key = groupKeyAt(key_width, keys, key_hi, pf);
            @prefetch(table.slotAddr(table.bucketOf(@TypeOf(table.*).hashKey(pf_key))), .{ .rw = .write, .locality = 1 });
        }

        // Adjacent-equal run: probe the table once for the whole run; only the
        // creating row carries NEW_GID_BIT (the extreme kernels' first-touch
        // contract), the rest of the run records the bare gid.
        const key = groupKeyAt(key_width, keys, key_hi, r);
        var run_end = r + 1;
        while (run_end < n and groupKeyAt(key_width, keys, key_hi, run_end) == key) run_end += 1;
        const run_len: u64 = if (weights.len != 0) blk: {
            var s: u64 = 0;
            for (weights[r..run_end]) |w| s += w;
            break :blk s;
        } else @intCast(run_end - r);

        const probe = table.getOrPut(@TypeOf(table.*).hashKey(key), key);
        var gid: u32 = probe.gid;
        var tagged: u32 = probe.gid;
        if (!probe.found) {
            gid = states.pushAssumeCapacity();
            const head = states.head(gid);
            head.key = key;
            head.count = run_len;
            if (rowrefs.len != 0) head.rowref = rowrefs[r];
            table.commit(probe.slot, key, gid);
            if (has_str) {
                str_states.appendAssumeCapacity([_]StrAcc{.{}} ** MAX_GROUP_STR_SLOTS);
            }
            if (has_concat) {
                concat_states.appendAssumeCapacity([_]ConcatCell{.empty} ** MAX_GROUP_CONCAT_SLOTS);
            }
            tagged = if (mark_new) gid | NEW_GID_BIT else gid;
        } else {
            states.head(gid).count += run_len;
        }
        if (has_str_fold) {
            var rr = r;
            while (rr < run_end) : (rr += 1) try foldGroupStr(str_states, str_bank, str_arena, gid, aggregates, rows, rr);
        }
        if (has_concat) {
            var rr = r;
            while (rr < run_end) : (rr += 1) try foldGroupConcat(concat_states, str_arena, gid, aggregates, rows, rr);
        }
        if (has_mirror) mirrorCountSlots(states, gid, aggregates);
        if (want_gids) {
            gids[r] = tagged;
            if (run_end > r + 1) @memset(gids[r + 1 .. run_end], gid);
        }
        r = run_end;
    }

    if (needs_kernels) {
        for (aggregates) |agg| {
            if (agg.is_string or agg.is_distinct or agg.is_concat or agg.is_udf) continue;
            if (aggInputHasValidity(rows, agg))
                foldAggregateKernel(true, states, gids, rows, agg)
            else
                foldAggregateKernel(false, states, gids, rows, agg);
        }
    }

    if (has_distinct) {
        if (mark_new) for (gids) |*g| {
            g.* &= ~NEW_GID_BIT;
        };
        try foldGroupDistinctChunk(distinct_sets, allocator, states, gids, aggregates, rows);
    }
}

// The original per-row program loop, kept for aggregate shapes the kernel
// pass can't express.
fn groupChunkRowsDirectKeysProgram(
    comptime key_width: GroupKeyWidth,
    table: anytype,
    states: *StateSlab,
    str_states: *std.ArrayListUnmanaged(StrAccRow),
    concat_states: *std.ArrayListUnmanaged(ConcatRow),
    udf_states: *std.ArrayListUnmanaged(UdfStateRow),
    udf_arena: Allocator,
    str_arena: Allocator,
    str_bank: *StringBank,
    distinct_sets: *DistinctSlots,
    gids_buf: *std.ArrayListUnmanaged(u32),
    allocator: Allocator,
    keys: anytype,
    key_hi: []const u32,
    row_count: usize,
    aggregates: []const GroupAggregateSpec,
    rows: GroupRows,
    rowrefs: []const i64,
) !void {
    // Weighted rows would silently undercount here (per-row +1 program); the
    // COUNT-only weight gate guarantees this path never sees them.
    std.debug.assert(!rows.layout.has_weight);
    const has_str = hasStringResults(rows.layout);
    const has_str_fold = rows.layout.has_str_payload and hasStringExtreme(aggregates);
    const has_concat = rows.layout.has_concat;
    const has_udf = rows.layout.has_udf;
    const has_distinct = rows.layout.distinct_slot_count > 0;
    const n = row_count;
    if (has_distinct) try gids_buf.resize(allocator, n);
    const gids: []u32 = if (has_distinct) gids_buf.items[0..n] else &.{};

    var prev_key: u128 = 0;
    var prev_gid: u32 = 0;
    var have_prev = false;
    const prefetch = prefetchesSlots(table);
    var r: usize = 0;
    while (r < n) : (r += 1) {
        const pf = r + PREFETCH_DIST_BUCKET;
        if (prefetch and pf < n) {
            const pf_key = groupKeyAt(key_width, keys, key_hi, pf);
            @prefetch(table.slotAddr(table.bucketOf(@TypeOf(table.*).hashKey(pf_key))), .{ .rw = .write, .locality = 1 });
        }

        const key = groupKeyAt(key_width, keys, key_hi, r);
        // Adjacent-equal run: reuse the previous row's resolved gid, skipping
        // the table probe (the state program still folds per row — its inputs
        // vary within the run).
        if (have_prev and key == prev_key) {
            try updateGroupStateProgram(states.ref(prev_gid), aggregates, rows, r);
            if (has_str_fold) try foldGroupStr(str_states, str_bank, str_arena, prev_gid, aggregates, rows, r);
            if (has_concat) try foldGroupConcat(concat_states, str_arena, prev_gid, aggregates, rows, r);
            if (has_udf) try foldGroupUdf(udf_states, udf_arena, prev_gid, aggregates, rows, r, n);
            if (has_distinct) gids[r] = prev_gid;
            continue;
        }
        const probe = table.getOrPut(@TypeOf(table.*).hashKey(key), key);
        if (!probe.found) {
            const new_gid = try initGroupStateProgram(states, key, aggregates, rows, r);
            if (rowrefs.len != 0) states.head(new_gid).rowref = rowrefs[r];
            table.commit(probe.slot, key, new_gid);
            if (has_str) {
                str_states.appendAssumeCapacity([_]StrAcc{.{}} ** MAX_GROUP_STR_SLOTS);
                if (has_str_fold) try foldGroupStr(str_states, str_bank, str_arena, new_gid, aggregates, rows, r);
            }
            if (has_concat) {
                concat_states.appendAssumeCapacity([_]ConcatCell{.empty} ** MAX_GROUP_CONCAT_SLOTS);
                try foldGroupConcat(concat_states, str_arena, new_gid, aggregates, rows, r);
            }
            if (has_udf) {
                udf_states.appendAssumeCapacity([_]UdfCell{.{}} ** MAX_GROUP_UDF_SLOTS);
                try initUdfStatesRow(udf_states, udf_arena, new_gid, aggregates);
                try foldGroupUdf(udf_states, udf_arena, new_gid, aggregates, rows, r, n);
            }
            if (has_distinct) gids[r] = new_gid;
            prev_key = key;
            prev_gid = new_gid;
            have_prev = true;
            continue;
        }
        try updateGroupStateProgram(states.ref(probe.gid), aggregates, rows, r);
        if (has_str_fold) try foldGroupStr(str_states, str_bank, str_arena, probe.gid, aggregates, rows, r);
        if (has_concat) try foldGroupConcat(concat_states, str_arena, probe.gid, aggregates, rows, r);
        if (has_udf) try foldGroupUdf(udf_states, udf_arena, probe.gid, aggregates, rows, r, n);
        if (has_distinct) gids[r] = probe.gid;
        prev_key = key;
        prev_gid = probe.gid;
        have_prev = true;
    }

    if (has_distinct) try foldGroupDistinctChunk(distinct_sets, allocator, states, gids, aggregates, rows);
}

// Tags the gid of the row that CREATED its group, so the MIN/MAX kernels know
// "set unconditionally" vs "compare" without re-deriving first-touch. Group
// counts are dense u32 indexes far below 2^31, so the bit is free.
const NEW_GID_BIT: u32 = 1 << 31;
const PREFETCH_DIST_STATES: usize = 24;

inline fn prefetchStateSlots(states: *const StateSlab, gid: u32) void {
    @prefetch(states.record_ptr(gid) + STATE_HEAD_BYTES, .{ .rw = .write, .locality = 1 });
}

// COUNT aggregates aimed at a numeric slot (state_index != 0) mirror the
// group's running count there; rare, so the probe pass calls this only when
// the program has one.
inline fn mirrorCountSlots(states: *StateSlab, gid: u32, aggregates: []const GroupAggregateSpec) void {
    const count = states.head(gid).count;
    const slots = states.slotsOf(gid);
    for (aggregates) |agg| {
        if (agg.is_string or agg.is_distinct) continue;
        if ((agg.op == .count_star or agg.op == .count_col) and agg.state_index != 0) {
            slots[agg.state_index - 1] = @intCast(count);
        }
    }
}

// Per-aggregate fold kernels (pass 2 of the kernelized program): one
// monomorphic row loop per aggregate over the recorded gid array, the staged
// column read through its typed slice and the per-group state record
// prefetched ahead — group records are DRAM-resident at silo cardinalities,
// and the look-ahead overlaps those independent misses (the per-row program
// chained them).
//
// `nullable` kernels read the input's validity bytes: a NULL row folds
// nothing, and each run adds its non-null count to the aggregate's
// valid-count slot, which also decides MIN/MAX first-touch (the group may
// have seen only NULLs so far, so the creating row's tag doesn't).
fn foldAggregateKernel(comptime nullable: bool, states: *StateSlab, gids: []const u32, rows: GroupRows, agg: GroupAggregateSpec) void {
    switch (agg.op) {
        .count_star, .count_col => {},
        .sum, .avg => if (aggInputIsFloat(rows, agg))
            foldKernelSumFloat(nullable, states, gids, rows, agg)
        else if (agg.wide)
            foldKernelSumInt(i128, nullable, states, gids, rows, agg)
        else
            foldKernelSumInt(i64, nullable, states, gids, rows, agg),
        .min => if (aggInputIsFloat(rows, agg))
            foldKernelExtreme(f64, true, nullable, states, gids, rows, agg)
        else
            foldKernelExtreme(i64, true, nullable, states, gids, rows, agg),
        .max => if (aggInputIsFloat(rows, agg))
            foldKernelExtreme(f64, false, nullable, states, gids, rows, agg)
        else
            foldKernelExtreme(i64, false, nullable, states, gids, rows, agg),
        // All force or skip the kernel pass.
        .count_distinct, .welford, .concat, .udf => unreachable,
    }
}

inline fn aggInputHasValidity(rows: GroupRows, agg: GroupAggregateSpec) bool {
    if (!agg.nullable) return false;
    const ic = agg.input_column_index orelse return false;
    return rows.layout.columns[ic].nullable;
}

inline fn aggInputValidBytes(comptime nullable: bool, rows: GroupRows, agg: GroupAggregateSpec) []const u8 {
    if (!nullable) return &.{};
    return rows.validAll(rows.layout.columns[agg.input_column_index.?].valid_index);
}

// `Acc` is i64 for narrow state and i128 for the two-slot wide state (lo u64
// bits at `state_index - 1`, hi at `state_index`).
fn foldKernelSumInt(comptime Acc: type, comptime nullable: bool, states: *StateSlab, gids: []const u32, rows: GroupRows, agg: GroupAggregateSpec) void {
    const si: usize = agg.state_index;
    const vci: usize = agg.valid_count_index;
    const valid = aggInputValidBytes(nullable, rows, agg);
    switch (rows.layout.columns[agg.input_column_index.?].physical_type) {
        inline .i8, .i16, .i32, .i64 => |pt| {
            const T = groupPhysicalT(pt);
            const vals = rows.columnTypedAll(T, agg.input_column_index.?);
            var r: usize = 0;
            while (r < gids.len) {
                const pf = r + PREFETCH_DIST_STATES;
                if (pf < gids.len) prefetchStateSlots(states, gids[pf] & ~NEW_GID_BIT);
                // Adjacent-equal gid run: accumulate in a register, store once.
                const g = gids[r] & ~NEW_GID_BIT;
                var acc: Acc = 0;
                var n_valid: i64 = 0;
                var rr = r;
                while (rr < gids.len and (gids[rr] & ~NEW_GID_BIT) == g) : (rr += 1) {
                    if (nullable) {
                        const ok = valid[rr] != 0;
                        acc += if (ok) vals[rr] else 0;
                        n_valid += @intFromBool(ok);
                    } else acc += vals[rr];
                }
                if (!nullable) n_valid = @intCast(rr - r);
                const slots = states.slotsOf(g);
                if (Acc == i128) {
                    const sum = wideStateValue(slots, si) + acc;
                    slots[si - 1] = @bitCast(@as(u64, @truncate(@as(u128, @bitCast(sum)))));
                    slots[si] = @intCast(sum >> 64);
                } else slots[si - 1] += acc;
                if (vci != 0) slots[vci - 1] += n_valid;
                r = rr;
            }
        },
        .f32, .f64 => {},
    }
}

fn foldKernelSumFloat(comptime nullable: bool, states: *StateSlab, gids: []const u32, rows: GroupRows, agg: GroupAggregateSpec) void {
    const si: usize = agg.state_index;
    const vci: usize = agg.valid_count_index;
    const valid = aggInputValidBytes(nullable, rows, agg);
    switch (rows.layout.columns[agg.input_column_index.?].physical_type) {
        inline .f32, .f64 => |pt| {
            const T = groupPhysicalT(pt);
            const vals = rows.columnTypedAll(T, agg.input_column_index.?);
            var r: usize = 0;
            while (r < gids.len) {
                const pf = r + PREFETCH_DIST_STATES;
                if (pf < gids.len) prefetchStateSlots(states, gids[pf] & ~NEW_GID_BIT);
                const g = gids[r] & ~NEW_GID_BIT;
                var acc: f64 = 0;
                var n_valid: i64 = 0;
                var rr = r;
                while (rr < gids.len and (gids[rr] & ~NEW_GID_BIT) == g) : (rr += 1) {
                    const v: f64 = @floatCast(vals[rr]);
                    if (nullable) {
                        const ok = valid[rr] != 0;
                        acc += if (ok) v else 0;
                        n_valid += @intFromBool(ok);
                    } else acc += v;
                }
                if (!nullable) n_valid = @intCast(rr - r);
                const slots = states.slotsOf(g);
                const cur: f64 = @bitCast(slots[si - 1]);
                slots[si - 1] = @bitCast(cur + acc);
                if (vci != 0) slots[vci - 1] += n_valid;
                r = rr;
            }
        },
        else => {},
    }
}

// `V` is i64 for integer inputs and f64 (bit-cast into the slot) for floats.
fn foldKernelExtreme(comptime V: type, comptime is_min: bool, comptime nullable: bool, states: *StateSlab, gids: []const u32, rows: GroupRows, agg: GroupAggregateSpec) void {
    const si: usize = agg.state_index;
    const vci: usize = agg.valid_count_index;
    const valid = aggInputValidBytes(nullable, rows, agg);
    switch (rows.layout.columns[agg.input_column_index.?].physical_type) {
        inline else => |pt| if (comptime (V == f64) == physicalIsFloat(pt)) {
            const T = groupPhysicalT(pt);
            const vals = rows.columnTypedAll(T, agg.input_column_index.?);
            var r: usize = 0;
            while (r < gids.len) {
                const pf = r + PREFETCH_DIST_STATES;
                if (pf < gids.len) prefetchStateSlots(states, gids[pf] & ~NEW_GID_BIT);
                // A run's creating row is its first, so the run inherits its
                // NEW_GID_BIT: reduce the run in a register, one tagged store.
                const tag_new = gids[r] & NEW_GID_BIT != 0;
                const g = gids[r] & ~NEW_GID_BIT;
                var best: V = undefined;
                var n_valid: i64 = 0;
                var rr = r;
                if (!nullable) {
                    best = if (V == f64) @floatCast(vals[r]) else vals[r];
                    n_valid = 1;
                    rr = r + 1;
                }
                while (rr < gids.len and (gids[rr] & ~NEW_GID_BIT) == g) : (rr += 1) {
                    if (nullable and valid[rr] == 0) continue;
                    const v: V = if (V == f64) @floatCast(vals[rr]) else vals[rr];
                    if ((nullable and n_valid == 0) or (if (is_min) v < best else v > best)) best = v;
                    n_valid += 1;
                }
                r = rr;
                if (n_valid == 0) continue;
                const slots = states.slotsOf(g);
                const first = if (nullable) slots[vci - 1] == 0 else tag_new;
                const cur: V = if (V == f64) @bitCast(slots[si - 1]) else slots[si - 1];
                if (first or (if (is_min) best < cur else best > cur)) slots[si - 1] = if (V == f64) @bitCast(best) else best;
                if (vci != 0) slots[vci - 1] += n_valid;
            }
        },
    }
}

inline fn groupPhysicalT(comptime pt: GroupColumnType) type {
    return switch (pt) {
        .i8 => i8,
        .i16 => i16,
        .i32 => i32,
        .i64 => i64,
        .f32 => f32,
        .f64 => f64,
    };
}

// Pass 2 of the grouped COUNT(DISTINCT) fold: per distinct aggregate, scatter
// each (gid, value) composite into its membership set with a software-prefetch
// pipeline. `ensureForBatch(n)` reserves the whole chunk up front so no grow
// fires mid-loop (slot addresses stay stable for the look-ahead prefetch), then
// each iteration prefetches the slot `PREFETCH_DIST_DISTINCT` rows ahead while
// inserting the current row, overlapping the independent cache misses. A
// first-ever (gid, value) bumps the group's distinct-count slot.
fn foldGroupDistinctChunk(
    distinct_sets: *DistinctSlots,
    allocator: Allocator,
    states: *StateSlab,
    gids: []const u32,
    aggregates: []const GroupAggregateSpec,
    rows: GroupRows,
) !void {
    const n = gids.len;
    for (aggregates) |agg| {
        if (!agg.is_distinct) continue;
        if (agg.distinct_state_index >= MAX_GROUP_DISTINCT_SLOTS) return error.UnsupportedOperatorForType;
        if (agg.distinct_str) {
            try foldGroupDistinctStr(&distinct_sets.strs[agg.distinct_state_index], allocator, states, gids, rows, agg.str_input_index, agg.state_index);
            continue;
        }
        const dset = &distinct_sets.ints[agg.distinct_state_index];
        try dset.ensureForBatch(allocator, n);
        const input_index = agg.input_column_index orelse return error.UnsupportedOperatorForType;
        if (input_index >= rows.layout.columns.len) return error.UnsupportedOperatorForType;
        // COUNT(DISTINCT) over a nullable input counts distinct NON-NULL
        // values: NULL rows skip the set probe entirely.
        const col_spec = rows.layout.columns[input_index];
        const valid: ?[]const u8 = if (col_spec.nullable) rows.validAll(col_spec.valid_index) else null;
        // Hoist the physical-type switch out of the row loop: the kernel runs
        // over the staged column's typed slice directly.
        switch (col_spec.physical_type) {
            inline .i8, .i16, .i32, .i64 => |pt| {
                const T = switch (pt) {
                    .i8 => i8,
                    .i16 => i16,
                    .i32 => i32,
                    .i64 => i64,
                    else => unreachable,
                };
                try foldGroupDistinctTyped(T, dset, states, gids, rows.columnTypedAll(T, input_index), valid, agg.state_index);
            },
            inline .f32, .f64 => |pt| {
                const T = if (pt == .f32) f32 else f64;
                try foldGroupDistinctTyped(T, dset, states, gids, rows.columnTypedAll(T, input_index), valid, agg.state_index);
            },
        }
    }
}

// The 64 bits a value contributes to the (gid, value) composite: an integer's
// two's-complement pattern, a float's canonical bits (-0.0 and 0.0 are one
// value, and so is every NaN — #82). A float widens to f64 first so both float
// widths share one encoding.
inline fn distinctValueBits(comptime T: type, v: T) u64 {
    return switch (@typeInfo(T)) {
        .float => types_mod.canonicalFloatBits(@as(f64, v)),
        else => @bitCast(@as(i64, v)),
    };
}

// String COUNT(DISTINCT) over the staged StrStore lane. Each row's digest is
// computed once, PREFETCH_DIST_DISTINCT rows ahead of its insert, and parked in
// a ring so the prefetch and the insert share it. NULL refs never count; an
// adjacent repeat of the same (gid, bytes) skips the probe.
fn foldGroupDistinctStr(
    set: *group_table.DistinctStrSet,
    allocator: Allocator,
    states: *StateSlab,
    gids: []const u32,
    rows: GroupRows,
    str_input_index: u16,
    state_index: u16,
) !void {
    const k = rows.layout.str_columns.len;
    if (str_input_index >= k) return error.UnsupportedOperatorForType;
    const n = gids.len;
    const RING = 32;
    comptime std.debug.assert(PREFETCH_DIST_DISTINCT < RING);
    var ring: [RING]u64 = undefined;
    for (0..@min(n, PREFETCH_DIST_DISTINCT)) |r| {
        ring[r % RING] = group_table.distinctStrDigest(rows.str.get(k, r, str_input_index));
    }
    var prev_gid: u32 = 0;
    var prev_bytes: ?[]const u8 = null;
    var r: usize = 0;
    while (r < n) : (r += 1) {
        const pf = r + PREFETCH_DIST_DISTINCT;
        if (pf < n) {
            const d_pf = group_table.distinctStrDigest(rows.str.get(k, pf, str_input_index));
            ring[pf % RING] = d_pf;
            set.prefetch(gids[pf], d_pf);
        }
        if (rows.str.isNull(k, r, str_input_index)) continue;
        const bytes = rows.str.get(k, r, str_input_index);
        const gid = gids[r];
        if (prev_bytes) |pb| {
            if (gid == prev_gid and std.mem.eql(u8, pb, bytes)) continue;
        }
        prev_gid = gid;
        prev_bytes = bytes;
        // Reserved per insert, not per chunk: a chunk of repeats would
        // otherwise size the set for rows that are never new.
        try set.ensureFor(allocator, 1);
        if (try set.insertNew(allocator, gid, ring[r % RING], bytes)) {
            try addAggregateStateValue(states.ref(gid), state_index, 1);
        }
    }
}

fn foldGroupDistinctTyped(
    comptime T: type,
    dset: *DistinctSet,
    states: *StateSlab,
    gids: []const u32,
    vals: []const T,
    valid: ?[]const u8,
    state_index: u16,
) !void {
    // Monomorphize on the validity lane: the non-nullable loop carries zero
    // per-row validity branches.
    if (valid) |vb| {
        return foldGroupDistinctTypedImpl(T, true, dset, states, gids, vals, vb, state_index);
    }
    return foldGroupDistinctTypedImpl(T, false, dset, states, gids, vals, &.{}, state_index);
}

fn foldGroupDistinctTypedImpl(
    comptime T: type,
    comptime has_valid: bool,
    dset: *DistinctSet,
    states: *StateSlab,
    gids: []const u32,
    vals: []const T,
    vb: []const u8,
    state_index: u16,
) !void {
    const n = gids.len;
    var r: usize = 0;
    while (r < n) : (r += 1) {
        const pf = r + PREFETCH_DIST_DISTINCT;
        if (pf < n) {
            dset.prefetchKey(DistinctSet.key(gids[pf], distinctValueBits(T, vals[pf])));
        }
        if (has_valid) {
            if (vb[r] == 0) continue;
        }
        const v = distinctValueBits(T, vals[r]);
        // The table's physical order clusters repeated values (UserID etc.)
        // into adjacent runs: an identical (gid, value) pair can't be new, so
        // skip the cache-missing set probe entirely. With a validity lane the
        // previous row may have been a skipped NULL carrying an artifact value
        // equal to this one — disable the shortcut there.
        if (!has_valid and r > 0 and gids[r] == gids[r - 1] and v == distinctValueBits(T, vals[r - 1])) continue;
        const composite = DistinctSet.key(gids[r], v);
        if (dset.insertNewBatch(composite)) {
            try addAggregateStateValue(states.ref(gids[r]), state_index, 1);
        }
    }
}

inline fn groupKeyAt(comptime key_width: GroupKeyWidth, keys: anytype, key_hi: []const u32, idx: usize) u128 {
    return switch (key_width) {
        .u32 => @as(u128, keys[idx]),
        .u64 => @as(u128, keys[idx]),
        .u96 => @as(u128, keys[idx]) | (@as(u128, key_hi[idx]) << 64),
        .u128 => keys[idx],
    };
}

inline fn rawRowBucketIndex(rows: RawRows, row_idx: usize, bucket_count: usize) usize {
    const key = rows.keyAt(row_idx);
    const h = routeHashRowBits(@truncate(key), @truncate(key >> 64));
    return if (std.math.isPowerOfTwo(bucket_count))
        (@as(usize, @truncate(h)) & (bucket_count - 1))
    else
        bucketIndexHash(h, bucket_count);
}

/// A collapsed batch's repeat counts, one per row, and the rows they cover.
const RowWeights = struct {
    counts: []const u32,
    input_rows: usize,
    /// The batch column whose values strictly ascend row by row (a sorted
    /// row-group dictionary's used values), when there is one.
    ascending_column: ?usize = null,
    /// Per staged column, when its input is a scan column: that input
    /// folded over the rows each row stands for, by the column's aggregate.
    /// A batch column holds one fold, and MIN(x) and MAX(x) need two.
    partials: []const ?ColumnView = &.{},
};

/// A numeric aggregate input riding along with a collapsed column: each
/// collapsed row carries its value's rows folded by `op`, the aggregate of
/// staged column `layout_column`.
const DictPartial = struct {
    column: usize,
    layout_column: usize,
    op: WeightFoldOp,
    /// Per dictionary code, then per collapsed value; the column's type
    /// picks the integer or the float lists.
    code_ints: std.ArrayListUnmanaged(i64) = .empty,
    code_floats: std.ArrayListUnmanaged(f64) = .empty,
    code_seen: std.ArrayListUnmanaged(bool) = .empty,
    ints: std.ArrayListUnmanaged(i64) = .empty,
    floats: std.ArrayListUnmanaged(f64) = .empty,
    is_float: bool = false,

    fn deinit(self: *DictPartial, allocator: Allocator) void {
        self.code_ints.deinit(allocator);
        self.code_floats.deinit(allocator);
        self.code_seen.deinit(allocator);
        self.ints.deinit(allocator);
        self.floats.deinit(allocator);
    }

    fn accumulate(self: *DictPartial, allocator: Allocator, view: ColumnView, codes: []const u32, value_count: usize) !void {
        switch (view.data) {
            inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |values| {
                self.is_float = false;
                try self.code_ints.resize(allocator, value_count);
                const acc = self.code_ints.items;
                switch (self.op) {
                    .sum => {
                        @memset(acc, 0);
                        for (codes, values[0..codes.len]) |c, v| acc[c] += v;
                    },
                    .min => {
                        @memset(acc, std.math.maxInt(i64));
                        for (codes, values[0..codes.len]) |c, v| acc[c] = @min(acc[c], v);
                    },
                    .max => {
                        @memset(acc, std.math.minInt(i64));
                        for (codes, values[0..codes.len]) |c, v| acc[c] = @max(acc[c], v);
                    },
                }
            },
            inline .float, .double => |values| {
                self.is_float = true;
                try self.code_floats.resize(allocator, value_count);
                const acc = self.code_floats.items;
                if (self.op == .sum) {
                    @memset(acc, 0);
                    for (codes, values[0..codes.len]) |c, v| acc[c] += v;
                    return;
                }
                // A row-by-row extreme starts from the first row (a NaN
                // there sticks), so each code's first row seeds it.
                try self.code_seen.resize(allocator, value_count);
                const seen = self.code_seen.items;
                @memset(seen, false);
                for (codes, values[0..codes.len]) |c, v| {
                    const x: f64 = v;
                    const better = if (self.op == .min) x < acc[c] else x > acc[c];
                    if (!seen[c] or better) acc[c] = x;
                    seen[c] = true;
                }
            },
            else => return error.TypeMismatch,
        }
    }

    // The folded values of codes [lo, hi) that some row used, in code order.
    fn emit(self: *DictPartial, allocator: Allocator, code_rows: []const u32, lo: usize, hi: usize, used: usize) !ColumnView {
        if (self.is_float) {
            self.floats.clearRetainingCapacity();
            try self.floats.ensureTotalCapacity(allocator, used);
            for (code_rows[lo..hi], self.code_floats.items[lo..hi]) |n, v| {
                if (n != 0) self.floats.appendAssumeCapacity(v);
            }
            return .{ .data = .{ .double = self.floats.items } };
        }
        self.ints.clearRetainingCapacity();
        try self.ints.ensureTotalCapacity(allocator, used);
        for (code_rows[lo..hi], self.code_ints.items[lo..hi]) |n, v| {
            if (n != 0) self.ints.appendAssumeCapacity(v);
        }
        return .{ .data = .{ .bigint = self.ints.items } };
    }
};

/// One worker's per-distinct evaluation. A batch whose string column arrives
/// as row-group dictionary codes (`Batch.block_dicts`) collapses to the
/// values its rows use, in dictionary order; the derived columns evaluate
/// once per value (when there are any), and the grid folds each value by its
/// row count. Any other batch (a raw row group, the memtable) evaluates per row.
const GridDictEval = struct {
    compute: ?*thindb.exec.Compute,
    column: usize,
    loc_column: ?usize,
    partials: []DictPartial = &.{},
    /// Per staged column, its partial's folded values in the collapsed batch.
    partial_views: []?ColumnView = &.{},
    /// Per dictionary code: rows using it, and one such row's location.
    code_rows: std.ArrayListUnmanaged(u32) = .empty,
    code_loc: std.ArrayListUnmanaged(i64) = .empty,
    /// Per collapsed value: its row count and a row's location, its digest
    /// when the column is a hashed key, else the value bytes when the used
    /// codes aren't one contiguous run.
    weights: std.ArrayListUnmanaged(u32) = .empty,
    locs: std.ArrayListUnmanaged(i64) = .empty,
    digests: std.ArrayListUnmanaged(u128) = .empty,
    offsets: std.ArrayListUnmanaged(u32) = .empty,
    bytes: std.ArrayListUnmanaged(u8) = .empty,
    views: []ColumnView = &.{},
    hashed: []?[]const u128 = &.{},
    input_rows: usize = 0,
    collapsed: bool = false,
    /// The collapsed column reached the evaluated batch as the dictionary's
    /// values in code order, so they ascend.
    ascending: bool = false,

    fn deinit(self: *GridDictEval, allocator: Allocator) void {
        self.code_rows.deinit(allocator);
        self.code_loc.deinit(allocator);
        self.weights.deinit(allocator);
        self.locs.deinit(allocator);
        self.digests.deinit(allocator);
        self.offsets.deinit(allocator);
        self.bytes.deinit(allocator);
        if (self.views.len > 0) allocator.free(self.views);
        if (self.hashed.len > 0) allocator.free(self.hashed);
        for (self.partials) |*p| p.deinit(allocator);
        if (self.partials.len > 0) allocator.free(self.partials);
        if (self.partial_views.len > 0) allocator.free(self.partial_views);
    }

    fn next(self: *GridDictEval, scan: *Scan, allocator: Allocator) !?thindb.Batch {
        self.collapsed = false;
        const raw = (try scan.next()) orelse return null;
        self.input_rows = raw.row_count;
        const dicts = raw.block_dicts orelse return try self.eval(raw);
        const column = dicts[self.column] orelse return try self.eval(raw);
        const batch = try self.collapse(allocator, raw, column);
        self.collapsed = true;
        const out = try self.eval(batch);
        // A derived column may take the collapsed column's name and slot;
        // hashed values are the scan's placeholders.
        self.ascending = column.digests == null and sameStringView(out.values[self.column], batch.values[self.column]);
        return out;
    }

    fn sameStringView(a: ColumnView, b: ColumnView) bool {
        const sa = switch (a.data) {
            .varchar, .string => |sv| sv,
            else => return false,
        };
        const sb = switch (b.data) {
            .varchar, .string => |sv| sv,
            else => return false,
        };
        return sa.offsets.ptr == sb.offsets.ptr and sa.offsets.len == sb.offsets.len and sa.bytes.ptr == sb.bytes.ptr;
    }

    fn eval(self: *GridDictEval, batch: thindb.Batch) !thindb.Batch {
        const compute = self.compute orelse return batch;
        return try compute.evalBatch(batch);
    }

    fn collapse(self: *GridDictEval, allocator: Allocator, raw: thindb.Batch, column: thindb.exec.BlockDictColumn) !thindb.Batch {
        try self.code_rows.resize(allocator, column.valueCount());
        const code_rows = self.code_rows.items;
        @memset(code_rows, 0);
        const row_locs: ?[]const i64 = if (self.loc_column) |lc| raw.values[lc].data.bigint else null;
        if (row_locs) |rl| {
            try self.code_loc.resize(allocator, code_rows.len);
            const code_loc = self.code_loc.items;
            for (column.codes, rl[0..column.codes.len]) |c, loc| {
                code_loc[c] = loc;
                code_rows[c] += 1;
            }
        } else {
            for (column.codes) |c| code_rows[c] += 1;
        }
        for (self.partials) |*p| try p.accumulate(allocator, raw.values[p.column], column.codes, code_rows.len);

        var lo: usize = code_rows.len;
        var hi: usize = 0;
        var used: usize = 0;
        for (code_rows, 0..) |n, c| {
            if (n == 0) continue;
            lo = @min(lo, c);
            hi = c + 1;
            used += 1;
        }
        self.weights.clearRetainingCapacity();
        self.locs.clearRetainingCapacity();
        self.digests.clearRetainingCapacity();
        try self.weights.ensureTotalCapacity(allocator, used);
        if (row_locs != null) try self.locs.ensureTotalCapacity(allocator, used);
        if (column.digests != null) try self.digests.ensureTotalCapacity(allocator, used);
        for (code_rows[lo..hi], lo..) |n, c| {
            if (n == 0) continue;
            self.weights.appendAssumeCapacity(n);
            if (row_locs != null) self.locs.appendAssumeCapacity(self.code_loc.items[c]);
            if (column.digests) |ds| self.digests.appendAssumeCapacity(ds[c]);
        }

        const raw_values = switch (raw.values[self.column].data) {
            .varchar, .string => |sv| sv,
            else => return error.TypeMismatch,
        };
        // A hashed key's value column is the scan's empty placeholder; else
        // a contiguous run of used codes is a slice of the dictionary itself,
        // and a gapped one copies just the used values.
        const dict = column.dict;
        var values: storage_mod.StringView = undefined;
        if (column.digests != null) {
            values = .{ .offsets = raw_values.offsets[0 .. used + 1], .bytes = raw_values.bytes };
        } else if (used == hi - lo) {
            values = .{ .offsets = dict.offsets[lo .. hi + 1], .bytes = dict.bytes };
        } else {
            self.offsets.clearRetainingCapacity();
            self.bytes.clearRetainingCapacity();
            try self.offsets.ensureTotalCapacity(allocator, used + 1);
            self.offsets.appendAssumeCapacity(0);
            for (code_rows[lo..hi], lo..) |n, c| {
                if (n == 0) continue;
                try self.bytes.appendSlice(allocator, dict.rowBytes(c));
                self.offsets.appendAssumeCapacity(@intCast(self.bytes.items.len));
            }
            values = .{ .offsets = self.offsets.items, .bytes = self.bytes.items };
        }

        if (self.views.len != raw.values.len or self.hashed.len != raw.values.len) {
            if (self.views.len > 0) allocator.free(self.views);
            if (self.hashed.len > 0) allocator.free(self.hashed);
            self.views = &.{};
            self.hashed = &.{};
            self.views = try allocator.alloc(ColumnView, raw.values.len);
            self.hashed = try allocator.alloc(?[]const u128, raw.values.len);
        }
        @memcpy(self.views, raw.values);
        self.views[self.column] = .{ .data = switch (raw.values[self.column].data) {
            .varchar => .{ .varchar = values },
            .string => .{ .string = values },
            else => return error.TypeMismatch,
        } };
        if (self.loc_column) |lc| self.views[lc] = .{ .data = .{ .bigint = self.locs.items } };
        // The batch column takes one of its partials only so that every
        // view spans the collapsed rows; the staging reads `partial_views`.
        for (self.partials) |*p| {
            const view = try p.emit(allocator, code_rows, lo, hi, used);
            self.views[p.column] = view;
            self.partial_views[p.layout_column] = view;
        }
        const hashed: ?[]const ?[]const u128 = if (column.digests != null) blk: {
            @memset(self.hashed, null);
            self.hashed[self.column] = self.digests.items;
            break :blk self.hashed;
        } else null;
        return .{ .schema = raw.schema, .values = self.views, .row_count = used, .hashed = hashed };
    }

    fn rowWeights(self: *const GridDictEval) ?RowWeights {
        if (!self.collapsed) return null;
        return .{ .counts = self.weights.items, .input_rows = self.input_rows, .ascending_column = if (self.ascending) self.column else null, .partials = self.partial_views };
    }
};

/// The scan column whose row-group dictionary codes drive per-distinct
/// evaluation, or null when the program can't fold by count. It takes a
/// weighted program (every aggregate folds a repeated row by its count) with
/// deterministic derived columns, if any, over a scan reading one non-nullable
/// string column and numeric aggregate inputs (`dictEvalSource`); the row
/// locations ride along, as any row of a value recovers it. String expressions, and the string keys they feed, cost far
/// more per row than counting a row's code. The column may be a hashed key:
/// the scan then digests each dictionary value once, and the collapsed rows
/// carry those digests.
fn dictEvalColumn(allocator: Allocator, cfg: RunConfig, scan_schema: []const thindb.types.Column) !?usize {
    if (!cfg.distinct_eval) return null;
    const layout = cfg.group_rows_layout;
    if (!layout.has_weight or layout.has_concat or layout.has_udf) return null;
    if (cfg.byte_length_columns.len != 0) return null;
    if (layout.columns.len > MAX_GROUP_PAYLOAD_COLUMNS) return null;
    var foldable_buf: [MAX_GROUP_PAYLOAD_COLUMNS][]const u8 = undefined;
    var n_foldable: usize = 0;
    outer: for (layout.columns) |c| {
        for (layout.key_columns) |k| {
            if (types_mod.columnNameEql(k.name, c.source_name)) continue :outer;
        }
        foldable_buf[n_foldable] = c.source_name;
        n_foldable += 1;
    }
    return dictEvalSource(allocator, scan_schema, cfg.derived, cfg.udf_registry, foldable_buf[0..n_foldable]);
}

/// The one non-nullable string column `scan_schema` reads besides row
/// locations, when `derived` is deterministic: its row-group dictionaries
/// can stand in for its rows under a weighted program. Any other column must
/// be a non-nullable number in `foldable` (aggregate inputs) that no derived
/// column reads: a collapsed row carries it folded over the rows the row
/// stands for, once per aggregate over it.
pub fn dictEvalSource(
    allocator: Allocator,
    scan_schema: []const thindb.types.Column,
    derived: []const thindb.exec.Derived,
    udf_registry: ?*const udf_mod.UdfRegistry,
    foldable: []const []const u8,
) !?usize {
    for (derived) |d| {
        if (thindb.exec.compute_op.mayVary(d.expr, udf_registry)) return null;
    }
    var refs: std.ArrayListUnmanaged([]const u8) = .empty;
    defer refs.deinit(allocator);
    for (derived) |d| try thindb.exec.compute_op.collectColumnRefs(allocator, &refs, d.expr);
    var column: ?usize = null;
    for (scan_schema, 0..) |col, j| {
        if (types_mod.columnNameEql(col.name, rowloc.col_name)) continue;
        if (col.nullable) return null;
        switch (col.type) {
            .varchar, .string => {
                if (column != null) return null;
                column = j;
            },
            .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64, .float, .double => {
                for (refs.items) |r| {
                    if (types_mod.columnNameEql(r, col.name)) return null;
                }
                for (foldable) |f| {
                    if (types_mod.columnNameEql(f, col.name)) break;
                } else return null;
            },
            else => return null,
        }
    }
    return column;
}

const SiloGridJob = struct {
    scan: *Scan,
    // Batch source driven each tile: a Compute-wrapped scan when the shape has
    // derived columns, else a bare scan. `scan` is the same underlying object,
    // retained for range resets; `drive` produces the batches (with derived
    // columns appended). Points into the harness-owned `drives` array.
    drive: *thindb.exec.Query,
    // Set when dictionary-coded batches collapse to their distinct values
    // ahead of the derived columns; it then pulls the scan and evaluates
    // them itself.
    distinct: ?*GridDictEval = null,
    local: *WorkerParts,
    shared: *PipeShared,
    seg_start: []const usize,
    segment_count: usize,
    worker_index: usize,
    worker_count: usize,
    chunk_rows: usize,
    group_lease_buckets: usize,
    group_lease_rows: u64,
    raw_group_mode: RawGroupMode,
    raw_chunk_rows: usize,
    raw_group_chunk_rows: usize,
    raw_batch_chunks: usize,
    filter_fused: bool,
    profile: bool,
    cpu: usize,
    group_ticks: *i64,
    idle_ticks: *i64,
    top_ticks: *i64,
    chunks: *u64,
    top: *TopSet,
    err: *?anyerror,
    sketch_scratch: *SketchScratch,
};

fn siloGridWorker(job: SiloGridJob) void {
    // Lease a core from the process-global CoreScheduler rather than pinning to
    // a per-query `cpus[worker_index]`. A fixed per-query assignment makes every
    // query's worker 0 pin to the SAME core (cpus[0]), so N concurrent grouped
    // queries (e.g. order-key range shards, each a single-worker grid) all
    // serialize on one core. The scheduler round-robins distinct cores across
    // ALL in-flight queries and caps at the per-core thread budget globally, so
    // concurrent grouped queries spread across the machine. NO_PIN / no-cores
    // makes acquire a no-op (unpinned); a thread already holding a core reuses
    // it (no-hold-and-wait). tryAcquire (not acquire): the grid's workers
    // coordinate at barriers, so a worker must NEVER block waiting for a core
    // slot — under oversubscription (N concurrent grids > core budget) that
    // would deadlock a barrier-waiting peer. Excess workers run unpinned.
    var lease = core_scheduler.global().tryAcquire();
    defer lease.release();
    siloGridWorkerErr(job) catch |e| {
        job.err.* = e;
        // Wake the peers so they stop waiting on coordination counters this
        // worker will never advance (e.g. its `scans_done` increment).
        job.shared.aborted.store(true, .release);
        job.shared.signalWork(std.math.maxInt(u32));
    };
}

fn queuedBucketRows(bucket: *PipeBucket) ?u64 {
    if (!bucket.queue_lock.tryLock()) return null;
    const rows = bucket.queued_rows;
    bucket.queue_lock.unlock();
    return rows;
}

fn publishSharedStageBuilderLocked(
    shared: *PipeShared,
    builder: *StageBucketBuilder,
    bucket_idx: usize,
    queue_lock_ticks: ?*i64,
) !void {
    if (bucket_idx >= shared.raw_group_queues.len) return;
    const row_count = builder.rows.len();
    if (row_count == 0) return;

    const rows = builder.rows;
    // A replacement is acquired by the next append. Final publication must
    // not allocate an empty chunk that will only be torn down.
    builder.rows = .{};
    errdefer builder.rows = rows;
    try publishGroupChunkToQueue(
        shared,
        &shared.raw_group_queues[bucket_idx],
        .{ .rows = rows, .owner_worker = 0, .bucket_idx = bucket_idx },
        &shared.stage_outstanding_chunks,
        &shared.stage_outstanding_rows,
        queue_lock_ticks,
    );
    _ = shared.stage_builder_rows.fetchSub(@intCast(row_count), .release);
}

fn appendSharedStageBuilderRawSlice(
    shared: *PipeShared,
    local: *WorkerParts,
    bucket_idx: usize,
    rows: RawRows,
    start: usize,
    count: usize,
    raw_group_chunk_rows: usize,
    profile: bool,
) !void {
    if (count == 0 or bucket_idx >= shared.stage_builders.len) return;
    const builder = &shared.stage_builders[bucket_idx];
    const lock_t0 = if (profile) platform.nowTicks() else 0;
    lockSpin(&builder.lock);
    if (profile) local.raw_stage_builder_lock_ticks += platform.nowTicks() - lock_t0;
    defer builder.lock.unlock();

    var pos: usize = 0;
    while (pos < count) {
        if (builder.rows.capacity() == 0) {
            builder.rows = try acquireGroupRows(shared, raw_group_chunk_rows, &local.raw_recycle_lock_ticks);
        }
        if (builder.rows.len() >= raw_group_chunk_rows) {
            try publishSharedStageBuilderLocked(shared, builder, bucket_idx, &local.raw_group_queue_lock_ticks);
            continue;
        }

        const free = raw_group_chunk_rows - builder.rows.len();
        const take = @min(free, count - pos);
        try builder.rows.appendRawRowsSlice(shared.allocator, shared.group_rows_layout, rows, start + pos, take);
        _ = shared.stage_builder_rows.fetchAdd(@intCast(take), .release);
        pos += take;

        if (builder.rows.len() >= raw_group_chunk_rows) {
            try publishSharedStageBuilderLocked(shared, builder, bucket_idx, &local.raw_group_queue_lock_ticks);
        }
    }
}

fn flushSharedStageBuilders(
    shared: *PipeShared,
    local: *WorkerParts,
    profile: bool,
) !bool {
    if (shared.stage_builder_rows.load(.acquire) == 0) return false;
    const publish_t0 = if (profile) platform.nowTicks() else 0;
    var flushed = false;
    var b: usize = 0;
    while (b < shared.stage_builders.len) : (b += 1) {
        const builder = &shared.stage_builders[b];
        const lock_t0 = if (profile) platform.nowTicks() else 0;
        lockSpin(&builder.lock);
        defer builder.lock.unlock();
        if (profile) local.raw_stage_builder_lock_ticks += platform.nowTicks() - lock_t0;
        if (builder.rows.len() > 0) {
            try publishSharedStageBuilderLocked(shared, builder, b, &local.raw_group_queue_lock_ticks);
            flushed = true;
        }
    }
    if (profile) local.raw_stage_publish_ticks += platform.nowTicks() - publish_t0;
    return flushed;
}

/// Each staged row's bucket, and the rows each bucket receives.
fn stageBucketIds(comptime key_width: GroupKeyWidth, chunks: []const RawChunk, bucket_ids: []u16, counts: []u32) void {
    @memset(counts, 0);
    switch (std.math.isPowerOfTwo(counts.len)) {
        inline else => |pow2| {
            var row_idx: usize = 0;
            for (chunks) |chunk| {
                const rows = chunk.rows;
                const n = rows.len();
                const keys = switch (key_width) {
                    .u32 => rows.keyU32All(),
                    .u64 => rows.keyU64All(),
                    .u96 => rows.keyU96LoAll(),
                    .u128 => rows.keyU128All(),
                };
                const key_hi: []const u32 = if (key_width == .u96) rows.keyU96HiAll() else &.{};
                for (bucket_ids[row_idx..][0..n], 0..) |*id, r| {
                    const key = groupKeyAt(key_width, keys, key_hi, r);
                    const h = routeHashRowBits(@truncate(key), @truncate(key >> 64));
                    const b = if (pow2) @as(usize, @truncate(h)) & (counts.len - 1) else bucketIndexHash(h, counts.len);
                    id.* = @intCast(b);
                    counts[b] += 1;
                }
                row_idx += n;
            }
        },
    }
}

const MAX_STAGE_LANES: usize = 4 + 2 * MAX_GROUP_PAYLOAD_COLUMNS;

/// One fixed-width lane of a staged row slab.
const StageLane = union(enum) {
    /// The whole key, or a 96-bit key's low 64 bits.
    key_lo,
    key_hi,
    column: usize,
    rowref,
    weight,
    valid: usize,

    /// Every fixed-width lane of `layout`'s rows, in `buf`.
    fn all(layout: GroupRowsLayout, buf: *[MAX_STAGE_LANES]StageLane) []const StageLane {
        var n: usize = 0;
        buf[n] = .key_lo;
        n += 1;
        if (layout.key_width == .u96) {
            buf[n] = .key_hi;
            n += 1;
        }
        for (0..layout.columns.len) |c| {
            buf[n] = .{ .column = c };
            n += 1;
        }
        if (layout.has_rowref) {
            buf[n] = .rowref;
            n += 1;
        }
        if (layout.has_weight) {
            buf[n] = .weight;
            n += 1;
        }
        for (0..nullableInputCount(layout)) |v| {
            buf[n] = .{ .valid = v };
            n += 1;
        }
        return buf[0..n];
    }

    fn width(self: StageLane, layout: GroupRowsLayout) usize {
        return switch (self) {
            .key_lo => switch (layout.key_width) {
                .u32 => @sizeOf(u32),
                .u64, .u96 => @sizeOf(u64),
                .u128 => @sizeOf(u128),
            },
            .key_hi => @sizeOf(u32),
            .column => |c| groupColumnSize(layout.columns[c].physical_type),
            .rowref => @sizeOf(i64),
            .weight => @sizeOf(u32),
            .valid => 1,
        };
    }

    /// The lane's bytes in `rows`, a RawRows or GroupRows slab.
    fn bytes(self: StageLane, rows: anytype) []u8 {
        return switch (self) {
            .key_lo => switch (rows.layout.key_width) {
                .u32 => std.mem.sliceAsBytes(rows.keyU32All()),
                .u64 => std.mem.sliceAsBytes(rows.keyU64All()),
                .u96 => std.mem.sliceAsBytes(rows.keyU96LoAll()),
                .u128 => std.mem.sliceAsBytes(rows.keyU128All()),
            },
            .key_hi => std.mem.sliceAsBytes(rows.keyU96HiAll()),
            .column => |c| rows.columnByteSlab(c),
            .rowref => std.mem.sliceAsBytes(rows.rowrefAll()),
            .weight => std.mem.sliceAsBytes(rows.weightAll()),
            .valid => |v| rows.validAll(v),
        };
    }
};

/// Scatters one lane of the staged chunks into `flat`, each row to the next
/// slot of its bucket's run. A typed loop per lane keeps one write stream per
/// bucket and no per-row dispatch on the key width or a column's size.
fn scatterStageLane(flat: RawRows, chunks: []const RawChunk, bucket_ids: []const u16, offsets: []const u32, next: []u32, lane: StageLane) !void {
    switch (lane.width(flat.layout)) {
        1 => scatterStageRuns(u8, flat, chunks, bucket_ids, offsets, next, lane),
        2 => scatterStageRuns(u16, flat, chunks, bucket_ids, offsets, next, lane),
        4 => scatterStageRuns(u32, flat, chunks, bucket_ids, offsets, next, lane),
        8 => scatterStageRuns(u64, flat, chunks, bucket_ids, offsets, next, lane),
        16 => scatterStageRuns(u128, flat, chunks, bucket_ids, offsets, next, lane),
        else => return error.UnsupportedOperatorForType,
    }
}

fn scatterStageRuns(comptime T: type, flat: RawRows, chunks: []const RawChunk, bucket_ids: []const u16, offsets: []const u32, next: []u32, lane: StageLane) void {
    const dst: []T = @alignCast(std.mem.bytesAsSlice(T, lane.bytes(flat)));
    @memcpy(next, offsets);
    var row_idx: usize = 0;
    for (chunks) |chunk| {
        const n = chunk.rows.len();
        const src: []const T = @alignCast(std.mem.bytesAsSlice(T, lane.bytes(chunk.rows)));
        for (src[0..n], bucket_ids[row_idx..][0..n]) |v, b| {
            dst[next[b]] = v;
            next[b] += 1;
        }
        row_idx += n;
    }
}

fn drainRawDedicatedStageSharedBuilders(
    allocator: Allocator,
    shared: *PipeShared,
    local: *WorkerParts,
    scan_lane: usize,
    raw_chunk_rows: usize,
    raw_group_chunk_rows: usize,
    raw_batch_chunks: usize,
    profile: bool,
) !bool {
    var raw_chunks: [MAX_RAW_BATCH_CHUNKS]RawChunk = undefined;
    if (scan_lane >= shared.raw_scan_queues.len) return false;
    defer releaseRawQueueLane(shared.raw_scan_queues, scan_lane);

    var pop_t0 = if (profile) platform.nowTicks() else 0;
    var popped_total = popRawChunkBatchFromQueue(&shared.raw_scan_queues[scan_lane], &raw_chunks, raw_batch_chunks, &local.raw_scan_queue_lock_ticks);
    if (profile) local.raw_stage_pop_ticks += platform.nowTicks() - pop_t0;
    if (popped_total == 0) return false;
    var recycled: usize = 0;
    errdefer for (raw_chunks[recycled..popped_total]) |*chunk| chunk.rows.deinit(allocator);

    _ = shared.active_stage_jobs.fetchAdd(1, .release);
    defer _ = shared.active_stage_jobs.fetchSub(1, .release);

    const bucket_count = @min(shared.bucket_count, shared.raw_group_queues.len);
    std.debug.assert(bucket_count <= @as(usize, std.math.maxInt(u16)) + 1);
    try local.ensureWideBucketScratch(allocator, bucket_count);

    while (popped_total > 0) {
        if (shared.resources) |a| try a.checkCancelled();
        local.raw_stage_input_chunks += @intCast(popped_total);

        var total_rows: usize = 0;
        var i: usize = 0;
        while (i < popped_total) : (i += 1) total_rows += raw_chunks[i].rows.len();
        _ = shared.outstanding_chunks.fetchSub(popped_total, .release);
        if (total_rows == 0) {
            i = 0;
            const recycle_t0 = if (profile) platform.nowTicks() else 0;
            while (i < popped_total) : (i += 1) {
                recycled = i + 1;
                try recycleRawRows(shared, raw_chunks[i].rows, raw_chunk_rows, &local.raw_recycle_lock_ticks);
            }
            if (profile) local.raw_stage_recycle_ticks += platform.nowTicks() - recycle_t0;
            if (shared.group_rows_layout.has_str_payload) break;
            pop_t0 = if (profile) platform.nowTicks() else 0;
            popped_total = popRawChunkBatchFromQueue(&shared.raw_scan_queues[scan_lane], &raw_chunks, raw_batch_chunks, &local.raw_scan_queue_lock_ticks);
            recycled = 0;
            if (profile) local.raw_stage_pop_ticks += platform.nowTicks() - pop_t0;
            continue;
        }

        try local.flat_raw_rows.resize(allocator, shared.group_rows_layout, total_rows);
        try local.flat_bucket_ids.resize(allocator, total_rows);
        const layout = shared.group_rows_layout;
        if (layout.has_str_payload) local.flat_raw_rows.str.clear();

        const stage_t0 = if (profile) platform.nowTicks() else 0;
        const chunks = raw_chunks[0..popped_total];
        const bucket_ids = local.flat_bucket_ids.items[0..total_rows];
        const counts = local.flat_counts[0..bucket_count];
        const offsets = local.flat_offsets[0..bucket_count];
        const next = local.flat_next[0..bucket_count];
        switch (layout.key_width) {
            inline else => |kw| stageBucketIds(kw, chunks, bucket_ids, counts),
        }
        var offset: u32 = 0;
        for (counts, offsets) |n, *run_start| {
            run_start.* = offset;
            offset += n;
        }

        var lanes: [MAX_STAGE_LANES]StageLane = undefined;
        for (StageLane.all(layout, &lanes)) |lane| try scatterStageLane(local.flat_raw_rows, chunks, bucket_ids, offsets, next, lane);
        if (layout.has_str_payload) {
            const k = layout.str_columns.len;
            @memcpy(next, offsets);
            var row_idx: usize = 0;
            for (chunks) |chunk| {
                const src = chunk.rows;
                for (bucket_ids[row_idx..][0..src.len()], 0..) |b, r| {
                    const pos = next[b];
                    next[b] = pos + 1;
                    for (0..k) |sc| {
                        if (src.str.isNull(k, r, sc)) {
                            local.flat_raw_rows.str.appendNull(k, pos, sc);
                        } else {
                            try local.flat_raw_rows.str.append(allocator, k, pos, sc, src.str.get(k, r, sc));
                        }
                    }
                }
                row_idx += src.len();
            }
        }
        if (profile) local.raw_stage_ticks += platform.nowTicks() - stage_t0;

        const append_t0 = if (profile) platform.nowTicks() else 0;
        var b: usize = 0;
        while (b < bucket_count) : (b += 1) {
            const count: usize = local.flat_counts[b];
            if (count == 0) continue;
            const start: usize = local.flat_offsets[b];
            try appendSharedStageBuilderRawSlice(shared, local, b, local.flat_raw_rows, start, count, raw_group_chunk_rows, profile);
        }
        if (profile) local.raw_stage_slice_ticks += platform.nowTicks() - append_t0;

        i = 0;
        const recycle_t0 = if (profile) platform.nowTicks() else 0;
        while (i < popped_total) : (i += 1) {
            recycled = i + 1;
            try recycleRawRows(shared, raw_chunks[i].rows, raw_chunk_rows, &local.raw_recycle_lock_ticks);
        }
        if (profile) local.raw_stage_recycle_ticks += platform.nowTicks() - recycle_t0;

        // Return the lane so ready aggregation can consume its payload before
        // a producer fills more buffers, even while this queue has input left.
        if (shared.group_rows_layout.has_str_payload) break;
        pop_t0 = if (profile) platform.nowTicks() else 0;
        popped_total = popRawChunkBatchFromQueue(&shared.raw_scan_queues[scan_lane], &raw_chunks, raw_batch_chunks, &local.raw_scan_queue_lock_ticks);
        recycled = 0;
        if (profile) local.raw_stage_pop_ticks += platform.nowTicks() - pop_t0;
    }
    return true;
}

fn drainRawDedicatedGroupLane(
    allocator: Allocator,
    shared: *PipeShared,
    local: *WorkerParts,
    scratch: *GroupScratch,
    group_lane: usize,
    raw_group_chunk_rows: usize,
    raw_batch_chunks: usize,
    group_ticks: *i64,
    chunks: *u64,
    profile: bool,
) !bool {
    if (group_lane >= shared.raw_group_queues.len) return false;
    var group_chunks: [MAX_RAW_BATCH_CHUNKS]GroupChunk = undefined;
    const popped_total = popGroupChunkBatchFromQueue(&shared.raw_group_queues[group_lane], &group_chunks, raw_batch_chunks, &local.raw_group_queue_lock_ticks);
    releaseRawQueueLane(shared.raw_group_queues, group_lane);
    if (popped_total == 0) return false;
    _ = shared.active_group_jobs.fetchAdd(1, .release);
    defer _ = shared.active_group_jobs.fetchSub(1, .release);

    const bucket = &shared.buckets[group_lane % shared.bucket_count];
    var total_rows: u64 = 0;
    for (group_chunks[0..popped_total]) |chunk| total_rows += chunk.rows.len();
    defer {
        _ = shared.stage_outstanding_rows.fetchSub(total_rows, .release);
        _ = shared.stage_outstanding_chunks.fetchSub(popped_total, .release);
    }
    var transferred: usize = 0;
    errdefer for (group_chunks[transferred..popped_total]) |*chunk| chunk.rows.deinit(allocator);
    var i: usize = 0;
    while (i < popped_total) : (i += 1) {
        const rows = group_chunks[i].rows;
        const lock_t0 = if (profile) platform.nowTicks() else 0;
        lockSpin(&bucket.agg_lock);
        if (profile) local.raw_agg_lock_ticks += platform.nowTicks() - lock_t0;
        const g0 = if (profile) platform.nowTicks() else 0;
        bucket.fold_rows(allocator, scratch, rows, shared.input_rows_per_bucket) catch |err| {
            bucket.agg_lock.unlock();
            return err;
        };
        if (profile) group_ticks.* += platform.nowTicks() - g0;
        bucket.agg_lock.unlock();
        chunks.* += 1;
        // Recycling consumes this slab even if growing the recycle pool fails.
        transferred = i + 1;
        try recycleGroupRows(shared, group_chunks[i].rows, raw_group_chunk_rows, &local.raw_recycle_lock_ticks);
    }
    // Failed folds retain their credit until the query aborts, so peers cannot
    // read partial aggregate states in the interval before abort is signalled.
    if (shared.pending_group_rows.fetchSub(total_rows, .release) == total_rows) shared.signalWork(std.math.maxInt(u32));
    return true;
}

fn collectOwnedTop(shared: *PipeShared, worker_index: usize, worker_count: usize, top_out: *TopSet, top_ticks: *i64, profile: bool) !void {
    const top_t0 = if (profile) platform.nowTicks() else 0;
    if (top_out.items.len == 0) return;
    var b = worker_index;
    const has_str = hasStringResults(shared.group_rows_layout);
    const has_concat = shared.group_rows_layout.has_concat;
    const has_udf = shared.group_rows_layout.has_udf;
    var udf_scratch = UdfEmitScratch{ .allocator = shared.allocator };
    defer udf_scratch.deinit();
    while (b < shared.buckets.len) : (b += worker_count) {
        const bkt = &shared.buckets[b];
        if (has_str or has_udf) {
            var gid: usize = 0;
            while (gid < bkt.states.len) : (gid += 1) {
                var row = if (has_str) topRowFromStateStr(bkt.states.ref(gid), bkt.str_states.items[gid]) else topRowFromState(bkt.states.ref(gid));
                if (has_concat) try finalizeConcatIntoRow(&row, shared.group_rows_layout.aggregates, &bkt.concat_states.items[gid], bkt.str_arena.allocator());
                if (has_udf) try finalizeUdfIntoRow(&udf_scratch, &row, bkt, gid, shared.group_rows_layout.aggregates);
                top_out.consider(row);
            }
        } else {
            var gid: usize = 0;
            while (gid < bkt.states.len) : (gid += 1) top_out.consider_state(bkt.states.ref(gid));
        }
    }
    if (profile) top_ticks.* += platform.nowTicks() - top_t0;
}

// Join group `gid`'s collected GROUP_CONCAT items into the TopRow's str slot:
// sorted by rowloc (physical row order — the legacy fold's scan order), values
// separated by the aggregate's separator, built in the bucket's str_arena (the
// emit re-dups survivors via ownTopRowStr). No items → NULL (str_present off).
fn finalizeConcatIntoRow(row: *TopRow, aggregates: []const GroupAggregateSpec, cells: *ConcatRow, str_arena: Allocator) !void {
    for (aggregates) |agg| {
        if (!agg.is_concat) continue;
        const cell = &cells[agg.concat_state_index];
        if (cell.items.len == 0) {
            row.str[agg.str_state_index] = &.{};
            row.str_present[agg.str_state_index] = false;
            continue;
        }
        std.mem.sort(ConcatItem, cell.items, {}, concatItemLess);
        var total: usize = agg.separator.len * (cell.items.len - 1);
        for (cell.items) |it| total += it.bytes.len;
        const buf = try str_arena.alloc(u8, total);
        var off: usize = 0;
        for (cell.items, 0..) |it, i| {
            if (i != 0) {
                @memcpy(buf[off..][0..agg.separator.len], agg.separator);
                off += agg.separator.len;
            }
            @memcpy(buf[off..][0..it.bytes.len], it.bytes);
            off += it.bytes.len;
        }
        row.str[agg.str_state_index] = buf;
        row.str_present[agg.str_state_index] = true;
    }
}

fn concatItemLess(_: void, a: ConcatItem, b: ConcatItem) bool {
    return a.order < b.order;
}

// Reusable per-UDAF scratch ColumnStores for the emit's finalize calls. One
// store per udf_state_index, created lazily and cleared between groups so a
// million-group emit doesn't allocate a store per group.
const UdfEmitScratch = struct {
    allocator: Allocator,
    stores: [MAX_GROUP_UDF_SLOTS]?ColumnStore = [_]?ColumnStore{null} ** MAX_GROUP_UDF_SLOTS,

    fn deinit(self: *UdfEmitScratch) void {
        for (&self.stores) |*s| if (s.*) |*cs| cs.deinit(self.allocator);
    }

    fn store(self: *UdfEmitScratch, slot: usize, return_type: types_mod.Type) !*ColumnStore {
        if (self.stores[slot] == null) self.stores[slot] = try ColumnStore.init(self.allocator, return_type, true);
        return &self.stores[slot].?;
    }
};

// Decode the single value a UDAF's finalize appended to its scratch store into
// the i64 bit pattern the value slot carries (float results bit-cast through
// f64, integer results sign-extended) — the inverse the v2 emit reads back.
fn udfResultBits(view: ColumnView, return_type: types_mod.Type) i64 {
    return switch (return_type) {
        .double => @bitCast(view.data.double[0]),
        .float => @bitCast(@as(f64, view.data.float[0])),
        .bigint => view.data.bigint[0],
        .int => @as(i64, view.data.int[0]),
        .smallint => @as(i64, view.data.smallint[0]),
        .tinyint => @as(i64, view.data.tinyint[0]),
        .boolean => @as(i64, view.data.boolean[0]),
        .date => @as(i64, view.data.date[0]),
        .datetime => view.data.datetime[0],
        .decimal64 => view.data.decimal64[0],
        // Unreachable: the planner declines any UDAF whose return type isn't one
        // of the above (no value slot can hold a string / 128-bit result).
        else => 0,
    };
}

// Finalize group `gid`'s UDAF states into `row`: each UDAF's scalar lands in
// value slot `state_index - 1` and its presence (non-NULL) in the companion
// slot `state_index`. The v2 emit reads those back via aggIsNull + the .udf
// branch of appendAggregateValue.
fn finalizeUdfIntoRow(scratch: *UdfEmitScratch, row: *TopRow, bucket: *PipeBucket, gid: usize, aggregates: []const GroupAggregateSpec) !void {
    for (aggregates) |agg| {
        if (!agg.is_udf) continue;
        const entry = agg.udf_entry orelse return error.UnsupportedOperatorForType;
        const present_slot: usize = agg.state_index;
        const cell = &bucket.udf_states.items[gid][agg.udf_state_index];
        if (!cell.initialized) {
            row.slots[present_slot] = 0;
            continue;
        }
        const cs = try scratch.store(agg.udf_state_index, entry.return_type);
        cs.clear();
        const ctx = udf_mod.AggregateContext{ .allocator = scratch.allocator, .user_data = entry.user_data };
        try entry.finalize(&ctx, @ptrCast(cell.bytes.ptr), cs);
        if (cs.rowCount() == 0 or !cs.view().isValid(0)) {
            row.slots[present_slot] = 0;
            continue;
        }
        row.slots[agg.state_index - 1] = udfResultBits(cs.view(), entry.return_type);
        row.slots[present_slot] = 1;
    }
}

fn claimScanTile(job: SiloGridJob) ?ScanTile {
    const tile = job.shared.scan_tiles.claim() orelse return null;
    _ = job.shared.active_scan_jobs.fetchAdd(1, .release);
    return .{ .lo = tile.lo, .hi = tile.hi };
}

fn openGridScanTile(job: SiloGridJob, tile: ScanTile) void {
    const start = flatToCoord(tile.lo, job.seg_start, job.segment_count, job.shared.total_scan_rgs);
    const end = flatToCoord(tile.hi, job.seg_start, job.segment_count, job.shared.total_scan_rgs);
    const reset_t0 = if (job.profile) platform.nowTicks() else 0;
    job.scan.resetRange(start.seg, start.rg, end.seg, end.rg, tile.hi == job.shared.total_scan_rgs);
    if (job.profile) job.local.scan_reset_ticks += platform.nowTicks() - reset_t0;
    job.local.scan_tiles += 1;
}

const EncodedCountGroups = struct {
    job: SiloGridJob,
    route_ticks: i64 = 0,

    pub fn consume(self: *EncodedCountGroups, batch: @import("scan.zig").EncodedBatch) !bool {
        const layout = self.job.shared.group_rows_layout;
        for (layout.key_columns) |part| {
            const idx = batch.column_index(part.name) orelse return false;
            if (batch.schema[idx].nullable or batch.blocks[idx].encoding != .rle) return false;
            switch (part.typ) {
                .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => {},
                else => return false,
            }
        }
        const runs = try self.job.scan.encoded_runs(batch);
        var key_runs: [MAX_GENERIC_GROUP_KEYS]thindb.exec.RunsColumn = undefined;
        for (layout.key_columns, 0..) |part, i| key_runs[i] = runs[batch.column_index(part.name).?].?;
        const t0 = if (self.job.profile) platform.nowTicks() else 0;
        if (self.job.shared.count_sketch) |pass| {
            try sketchRuns(self.job, pass, batch.row_count, key_runs[0..layout.key_columns.len]);
        } else {
            if (self.job.local.raw_active_rows.capacity() == 0) self.job.local.raw_active_rows = try acquireRawRows(self.job.shared, self.job.raw_chunk_rows, &self.job.local.raw_recycle_lock_ticks);
            try appendBatchRunsWeighted(self.job.local, self.job.shared, batch.row_count, layout, key_runs[0..layout.key_columns.len], &.{}, self.job.raw_chunk_rows, self.job.profile);
        }
        if (self.job.profile) self.route_ticks += platform.nowTicks() - t0;
        return true;
    }
};

/// One pass of the two-pass COUNT top-K (see count_topk_sketch.zig): a
/// tally counts every input row into the sketch and groups none; a filter
/// pass groups only the rows of the groups the sketch keeps.
pub const CountSketchPass = struct {
    sketch: *CountSketch,
    mode: enum { tally, filter },
};

/// Below this many estimated groups the single pass groups in cache, and a
/// tally can't save enough to pay for itself.
const COUNT_SKETCH_MIN_GROUPS: u64 = 65536;

/// A batch's group-key columns as the core keys them.
const BatchKeys = struct {
    views: [MAX_GENERIC_GROUP_KEYS]ColumnView = undefined,
    digests: [MAX_GENERIC_GROUP_KEYS]?[]const u128 = undefined,
    len: usize,

    fn init(layout: GroupRowsLayout, batch: thindb.Batch) !BatchKeys {
        if (layout.key_columns.len == 0 or layout.key_columns.len > MAX_GENERIC_GROUP_KEYS) return error.UnsupportedOperatorForType;
        var keys: BatchKeys = .{ .len = layout.key_columns.len };
        for (layout.key_columns, 0..) |part, i| {
            const ci = batch.columnIndex(part.name) orelse return error.ColumnNotFound;
            keys.views[i] = batch.values[ci];
            keys.digests[i] = if (batch.hashed) |hs| (if (ci < hs.len) hs[ci] else null) else null;
        }
        return keys;
    }
};

/// One grid worker's buffers for a batch's group identities (and their key
/// lanes), and for a count-sketch pass: the kept spans of run-encoded keys,
/// and the rows (and identities) a filter pass keeps.
const SketchScratch = struct {
    ids: std.ArrayListUnmanaged(u128) = .empty,
    lanes: std.ArrayListUnmanaged(u128) = .empty,
    kept_ids: std.ArrayListUnmanaged(u128) = .empty,
    mask: std.ArrayListUnmanaged(bool) = .empty,
    weights: std.ArrayListUnmanaged(u32) = .empty,
    span_lengths: std.ArrayListUnmanaged(u32) = .empty,
    span_values: [MAX_GENERIC_GROUP_KEYS]std.ArrayListUnmanaged(i64) = @splat(.empty),
    stores: []ColumnStore = &.{},
    views: []ColumnView = &.{},
    digests: []std.ArrayListUnmanaged(u128) = &.{},
    hashed: []?[]const u128 = &.{},
    /// A collapsed batch's per-staged-column partials, compacted.
    partial_ints: [MAX_GROUP_PAYLOAD_COLUMNS]std.ArrayListUnmanaged(i64) = @splat(.empty),
    partial_floats: [MAX_GROUP_PAYLOAD_COLUMNS]std.ArrayListUnmanaged(f64) = @splat(.empty),
    partials: [MAX_GROUP_PAYLOAD_COLUMNS]?ColumnView = @splat(null),

    fn deinit(self: *SketchScratch, allocator: Allocator) void {
        self.ids.deinit(allocator);
        self.lanes.deinit(allocator);
        self.kept_ids.deinit(allocator);
        self.mask.deinit(allocator);
        self.weights.deinit(allocator);
        self.span_lengths.deinit(allocator);
        for (&self.span_values) |*v| v.deinit(allocator);
        for (&self.partial_ints) |*v| v.deinit(allocator);
        for (&self.partial_floats) |*v| v.deinit(allocator);
        self.deinitColumns(allocator);
    }

    fn deinitColumns(self: *SketchScratch, allocator: Allocator) void {
        for (self.stores) |*st| st.deinit(allocator);
        if (self.stores.len > 0) allocator.free(self.stores);
        if (self.views.len > 0) allocator.free(self.views);
        for (self.digests) |*d| d.deinit(allocator);
        if (self.digests.len > 0) allocator.free(self.digests);
        if (self.hashed.len > 0) allocator.free(self.hashed);
        self.stores = &.{};
        self.views = &.{};
        self.digests = &.{};
        self.hashed = &.{};
    }

    /// One store per column of `batch`, typed like its views. A worker's
    /// batches share their columns, but a memtable batch and a segment
    /// batch may type or null one differently.
    fn ensureColumns(self: *SketchScratch, allocator: Allocator, batch: thindb.Batch) !void {
        if (self.holds(batch)) return;
        self.deinitColumns(allocator);
        const n = batch.values.len;
        const stores = try allocator.alloc(ColumnStore, n);
        var built: usize = 0;
        errdefer {
            for (stores[0..built]) |*st| st.deinit(allocator);
            allocator.free(stores);
        }
        for (batch.values, batch.schema, 0..) |view, col, i| {
            stores[i] = try ColumnStore.initLike(allocator, view, col.nullable or view.nulls != null);
            built += 1;
        }
        const views = try allocator.alloc(ColumnView, n);
        errdefer allocator.free(views);
        const digests = try allocator.alloc(std.ArrayListUnmanaged(u128), n);
        errdefer allocator.free(digests);
        for (digests) |*d| d.* = .empty;
        const hashed = try allocator.alloc(?[]const u128, n);
        self.stores = stores;
        self.views = views;
        self.digests = digests;
        self.hashed = hashed;
    }

    fn holds(self: *const SketchScratch, batch: thindb.Batch) bool {
        if (self.stores.len != batch.values.len) return false;
        for (self.stores, batch.values) |st, view| {
            if (std.meta.activeTag(st.data) != std.meta.activeTag(view.data)) return false;
            if (view.nulls != null and st.nulls == null) return false;
        }
        return true;
    }
};

/// The composite spans of run-encoded key columns: row ranges over which
/// every key column holds one value, as appendBatchRunsWeighted merges them.
const KeySpans = struct {
    layout: GroupRowsLayout,
    key_runs: []const thindb.exec.RunsColumn,
    row_count: usize,
    row: usize = 0,
    rows: usize = 0,
    run_idx: [MAX_GENERIC_GROUP_KEYS]usize = @splat(0),
    run_left: [MAX_GENERIC_GROUP_KEYS]usize = undefined,

    fn init(layout: GroupRowsLayout, key_runs: []const thindb.exec.RunsColumn, row_count: usize) !KeySpans {
        var spans: KeySpans = .{ .layout = layout, .key_runs = key_runs, .row_count = row_count };
        for (key_runs, 0..) |kr, i| {
            if (kr.lengths.len == 0) return error.UnsupportedOperatorForType;
            spans.run_left[i] = kr.lengths[0];
        }
        return spans;
    }

    /// The next span's packed group key; `rows` and `value` then describe it.
    fn next(self: *KeySpans) !?u128 {
        self.advance();
        if (self.row == self.row_count) return null;
        var rows = self.row_count - self.row;
        for (self.key_runs, 0..) |_, i| rows = @min(rows, self.run_left[i]);
        // Lengths short of the row count would spin forever; a corrupt block
        // surfaces as a query error, not a hang.
        if (rows == 0) return error.UnsupportedOperatorForType;
        self.rows = rows;
        var key: u128 = 0;
        for (self.layout.key_columns, 0..) |part, i| key |= runKeyBits(part.typ, self.value(i)) << @intCast(part.offset_bits);
        return key;
    }

    fn value(self: *const KeySpans, column: usize) i64 {
        return self.key_runs[column].values_i64[self.run_idx[column]];
    }

    fn advance(self: *KeySpans) void {
        if (self.rows == 0) return;
        self.row += self.rows;
        for (self.key_runs, 0..) |kr, i| {
            self.run_left[i] -= self.rows;
            if (self.run_left[i] == 0) {
                self.run_idx[i] += 1;
                if (self.run_idx[i] < kr.lengths.len) self.run_left[i] = kr.lengths[self.run_idx[i]];
            }
        }
        self.rows = 0;
    }
};

/// A count-sketch pass over a batch whose key columns arrived run-encoded:
/// one counter update or keep test per span, never per row. A filter pass
/// forwards the kept spans as runs, so the grid groups them run-natively.
fn sketchRuns(job: SiloGridJob, pass: CountSketchPass, row_count: usize, key_runs: []const thindb.exec.RunsColumn) !void {
    const layout = job.shared.group_rows_layout;
    var spans = try KeySpans.init(layout, key_runs, row_count);
    switch (pass.mode) {
        .tally => {
            const counts = pass.sketch.workerCounts(job.worker_index);
            while (try spans.next()) |key| counts[pass.sketch.counter(key)] += @intCast(spans.rows);
        },
        .filter => {
            const allocator = job.scan.allocator;
            const scratch = job.sketch_scratch;
            const values = scratch.span_values[0..key_runs.len];
            scratch.span_lengths.clearRetainingCapacity();
            for (values) |*v| v.clearRetainingCapacity();
            var kept: usize = 0;
            while (try spans.next()) |key| {
                if (!pass.sketch.keeps(key)) continue;
                try scratch.span_lengths.append(allocator, @intCast(spans.rows));
                for (values, 0..) |*v, i| try v.append(allocator, spans.value(i));
                kept += spans.rows;
            }
            if (kept == 0) return;
            var kept_runs: [MAX_GENERIC_GROUP_KEYS]thindb.exec.RunsColumn = undefined;
            for (values, 0..) |v, i| kept_runs[i] = .{ .values_i64 = v.items, .lengths = scratch.span_lengths.items };
            if (job.local.raw_active_rows.capacity() == 0) job.local.raw_active_rows = try acquireRawRows(job.shared, job.raw_chunk_rows, &job.local.raw_recycle_lock_ticks);
            try appendBatchRunsWeighted(job.local, job.shared, kept, layout, kept_runs[0..key_runs.len], &.{}, job.raw_chunk_rows, job.profile);
        },
    }
}

/// Rows per chunk of hashedIdentities' key lanes, so a chunk's lanes stay
/// cache-resident between the fill and the digest pass.
const IDENTITY_LANE_ROWS: usize = 1024;

/// Rows `lo..lo + lane.len` of a non-NULL key column as
/// hashGenericKeyFromViews' parts: a string's digest (the sidecar's, when
/// the scan sent one), an integer's bits, or 0 for a type pairing
/// readGenericKeyBits rejects.
fn keyLane(view: ColumnView, digests: ?[]const u128, typ: thindb.types.Type, lo: usize, lane: []u128) []const u128 {
    switch (view.data) {
        .string, .varchar, .char, .json, .nested, .bitmap => |sv| {
            if (digests) |ds| return ds[lo..][0..lane.len];
            for (lane, lo..) |*p, r| p.* = thindb.exec.stringKeyDigest(sv.rowBytes(r));
        },
        inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |vals| {
            if (readGenericKeyBits(view, typ, lo)) |_| {
                const Bits = std.meta.Int(.unsigned, @bitSizeOf(@TypeOf(vals[0])));
                for (lane, vals[lo..][0..lane.len]) |*p, v| p.* = @as(Bits, @bitCast(v));
            } else |_| @memset(lane, 0);
        },
        else => @memset(lane, 0),
    }
    return lane;
}

/// Every row's hashed group identity, as hashGenericKeyFromViews digests it
/// row by row: each key column fills a lane of parts with its type switch
/// out of the row loop, then each row digests its fixed-width parts. A
/// nullable key column's validity tags vary the parts' layout per row, so
/// those layouts digest row by row.
fn hashedIdentities(allocator: Allocator, layout: GroupRowsLayout, keys: *const BatchKeys, lanes: *std.ArrayListUnmanaged(u128), out: []u128) !void {
    const k = keys.len;
    for (layout.key_columns) |part| {
        if (!part.nullable) continue;
        for (out, 0..) |*id, r| id.* = hashGenericKeyFromViews(layout, keys.views[0..k], keys.digests[0..k], r);
        return;
    }
    // A lone string key's digest is its identity, not a part to digest.
    if (k == 1) switch (keys.views[0].data) {
        .string, .varchar, .char, .json, .nested, .bitmap => {
            const ids = keyLane(keys.views[0], keys.digests[0], layout.key_columns[0].typ, 0, out);
            if (ids.ptr != out.ptr) @memcpy(out, ids);
            return;
        },
        else => {},
    };
    try lanes.resize(allocator, k * IDENTITY_LANE_ROWS);
    var src: [MAX_GENERIC_GROUP_KEYS][]const u128 = undefined;
    var lo: usize = 0;
    while (lo < out.len) : (lo += IDENTITY_LANE_ROWS) {
        const chunk = out[lo..@min(out.len, lo + IDENTITY_LANE_ROWS)];
        for (layout.key_columns, 0..) |part, i| {
            src[i] = keyLane(keys.views[i], keys.digests[i], part.typ, lo, lanes.items[i * IDENTITY_LANE_ROWS ..][0..chunk.len]);
        }
        switch (k) {
            inline 1...MAX_GENERIC_GROUP_KEYS => |kk| for (chunk, 0..) |*id, j| {
                var parts: [kk]u128 = undefined;
                inline for (0..kk) |i| parts[i] = src[i][j];
                id.* = thindb.exec.keyPartsDigest(kk, parts);
            },
            else => return error.UnsupportedOperatorForType,
        }
    }
}

test "hashed identities filled by column match hashGenericKeyFromViews row by row" {
    const allocator = std.testing.allocator;
    const n = 2 * IDENTITY_LANE_ROWS + 452;
    var prng = std.Random.DefaultPrng.init(0x1d3a);
    const random = prng.random();

    const users = try allocator.alloc(i64, n);
    defer allocator.free(users);
    const minutes = try allocator.alloc(i32, n);
    defer allocator.free(minutes);
    const ratios = try allocator.alloc(f64, n);
    defer allocator.free(ratios);
    const offsets = try allocator.alloc(u32, n + 1);
    defer allocator.free(offsets);
    var bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer bytes.deinit(allocator);
    const phrases = [_][]const u8{ "", "a", "thin", "columnar analytics", "x" ** 40 };
    offsets[0] = 0;
    for (0..n) |r| {
        users[r] = random.intRangeAtMost(i64, -3, 3);
        minutes[r] = random.intRangeAtMost(i32, 0, 59);
        ratios[r] = random.float(f64);
        try bytes.appendSlice(allocator, phrases[random.uintLessThan(usize, phrases.len)]);
        offsets[r + 1] = @intCast(bytes.items.len);
    }
    const sv: StringView = .{ .offsets = offsets, .bytes = bytes.items };
    const digests = try allocator.alloc(u128, n);
    defer allocator.free(digests);
    for (digests, 0..) |*d, r| d.* = thindb.exec.stringKeyDigest(sv.rowBytes(r));
    var nulls: [(n + 7) / 8]u8 = undefined;
    random.bytes(&nulls);

    const user: ColumnView = .{ .data = .{ .bigint = users } };
    const minute: ColumnView = .{ .data = .{ .int = minutes } };
    const phrase: ColumnView = .{ .data = .{ .string = sv } };
    const Key = struct { view: ColumnView, typ: thindb.types.Type, nullable: bool = false, digests: bool = false };
    const cases = .{
        .{Key{ .view = phrase, .typ = .string }},
        .{Key{ .view = phrase, .typ = .string, .digests = true }},
        .{ Key{ .view = user, .typ = .bigint }, Key{ .view = phrase, .typ = .string } },
        .{ Key{ .view = user, .typ = .bigint }, Key{ .view = phrase, .typ = .string, .digests = true } },
        .{ Key{ .view = user, .typ = .bigint }, Key{ .view = minute, .typ = .int }, Key{ .view = phrase, .typ = .string, .digests = true } },
        // Pairings readGenericKeyBits rejects key as part 0.
        .{ Key{ .view = minute, .typ = .bigint }, Key{ .view = .{ .data = .{ .double = ratios } }, .typ = .double }, Key{ .view = phrase, .typ = .string } },
        .{ Key{ .view = phrase, .typ = .string }, Key{ .view = .{ .data = .{ .bigint = users }, .nulls = &nulls }, .typ = .bigint, .nullable = true } },
    };
    var lanes: std.ArrayListUnmanaged(u128) = .empty;
    defer lanes.deinit(allocator);
    const out = try allocator.alloc(u128, n);
    defer allocator.free(out);
    inline for (cases) |case| {
        var specs: [case.len]GroupKeyColumnSpec = undefined;
        var keys: BatchKeys = .{ .len = case.len };
        inline for (case, 0..) |key, i| {
            specs[i] = .{ .name = "", .typ = key.typ, .offset_bits = 0, .width_bits = 0, .nullable = key.nullable };
            keys.views[i] = key.view;
            keys.digests[i] = if (key.digests) digests else null;
        }
        const layout: GroupRowsLayout = .{ .key_columns = &specs, .hashed_key = true };
        try hashedIdentities(allocator, layout, &keys, &lanes, out);
        for (out, 0..) |id, r| {
            try std.testing.expectEqual(hashGenericKeyFromViews(layout, keys.views[0..case.len], keys.digests[0..case.len], r), id);
        }
    }
}

/// Every row's group identity, as genericKeyFromViews and
/// hashGenericKeyFromViews key it row by row, filled column by column so
/// each loop reads one typed slice.
fn batchIdentities(job: SiloGridJob, allocator: Allocator, batch: thindb.Batch) ![]const u128 {
    const layout = job.shared.group_rows_layout;
    const keys = try BatchKeys.init(layout, batch);
    const n = batch.row_count;
    if (layout.hashed_key and keys.len == 1 and !layout.key_columns[0].nullable) {
        if (keys.digests[0]) |ds| return ds[0..n];
    }
    const ids = &job.sketch_scratch.ids;
    try ids.resize(allocator, n);
    const out = ids.items;
    if (layout.hashed_key) {
        try hashedIdentities(allocator, layout, &keys, &job.sketch_scratch.lanes, out);
        return out;
    }
    @memset(out, 0);
    for (layout.key_columns, keys.views[0..keys.len]) |part, view| {
        const shift: u7 = @intCast(part.offset_bits);
        if (part.nullable) {
            const valid_bit = @as(u128, 1) << @intCast(part.width_bits - 1);
            for (out, 0..) |*id, r| {
                if (view.isValid(r)) id.* |= (try readGenericKeyBits(view, part.typ, r) | valid_bit) << shift;
            }
            continue;
        }
        // Rejects the type pairings readGenericKeyBits rejects; the loop
        // below then reads the view's own type.
        if (n > 0) _ = try readGenericKeyBits(view, part.typ, 0);
        switch (view.data) {
            inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |vals| {
                const Bits = std.meta.Int(.unsigned, @bitSizeOf(@TypeOf(vals[0])));
                for (out, vals[0..n]) |*id, v| id.* |= @as(u128, @as(Bits, @bitCast(v))) << shift;
            },
            else => return error.TypeMismatch,
        }
    }
    return out;
}

/// A collapsed batch's rows stand for `weights` rows each, so the tally
/// counts them by weight: the sketch must see the counts the grid will.
fn tallyRows(job: SiloGridJob, sketch: *CountSketch, batch: thindb.Batch, weights: ?RowWeights) !void {
    const ids = try batchIdentities(job, job.scan.allocator, batch);
    const counts = sketch.workerCounts(job.worker_index);
    if (weights) |w| {
        for (ids, w.counts[0..ids.len]) |id, n| counts[sketch.counter(id)] += n;
    } else {
        for (ids) |id| counts[sketch.counter(id)] += 1;
    }
}

/// A batch bound for the grid, with its rows' repeat counts when collapsed
/// and, for a hashed key, the identities the sketch already digested.
const KeptRows = struct {
    batch: thindb.Batch,
    weights: ?RowWeights,
    ids: ?[]const u128 = null,
};

/// The rows of `batch` whose groups the sketch keeps, compacted (with their
/// weights) into the worker's scratch when only some are; null when none are.
fn keptRows(job: SiloGridJob, allocator: Allocator, sketch: *const CountSketch, batch: thindb.Batch, weights: ?RowWeights) !?KeptRows {
    const ids = try batchIdentities(job, allocator, batch);
    const scratch = job.sketch_scratch;
    try scratch.mask.resize(allocator, batch.row_count);
    const mask = scratch.mask.items;
    var kept: usize = 0;
    for (mask, ids) |*m, id| {
        m.* = sketch.keeps(id);
        kept += @intFromBool(m.*);
    }
    const hashed = job.shared.group_rows_layout.hashed_key;
    if (kept == batch.row_count) return .{ .batch = batch, .weights = weights, .ids = if (hashed) ids else null };
    if (kept == 0) return null;

    const kept_ids: ?[]const u128 = if (hashed) blk: {
        try scratch.kept_ids.resize(allocator, kept);
        var j: usize = 0;
        for (mask, ids) |m, id| {
            if (!m) continue;
            scratch.kept_ids.items[j] = id;
            j += 1;
        }
        break :blk scratch.kept_ids.items;
    } else null;

    const kept_weights: ?RowWeights = if (weights) |w| blk: {
        try scratch.weights.resize(allocator, kept);
        var j: usize = 0;
        var rows: usize = 0;
        for (mask, w.counts[0..batch.row_count]) |m, n| {
            if (!m) continue;
            scratch.weights.items[j] = n;
            rows += n;
            j += 1;
        }
        if (w.partials.len > scratch.partials.len) return error.UnsupportedOperatorForType;
        for (w.partials, 0..) |partial, c| {
            const view = partial orelse {
                scratch.partials[c] = null;
                continue;
            };
            scratch.partials[c] = switch (view.data) {
                .bigint => |values| .{ .data = .{ .bigint = try keepMasked(i64, allocator, values, mask, kept, &scratch.partial_ints[c]) } },
                .double => |values| .{ .data = .{ .double = try keepMasked(f64, allocator, values, mask, kept, &scratch.partial_floats[c]) } },
                else => return error.TypeMismatch,
            };
        }
        break :blk .{ .counts = scratch.weights.items, .input_rows = rows, .ascending_column = w.ascending_column, .partials = scratch.partials[0..w.partials.len] };
    } else null;

    try scratch.ensureColumns(allocator, batch);
    for (batch.values, 0..) |view, ci| {
        scratch.stores[ci].clear();
        try appendMaskedColumn(allocator, view, mask, &scratch.stores[ci]);
        scratch.views[ci] = scratch.stores[ci].view();
        scratch.hashed[ci] = null;
        // A sidecar can be shorter than `values` (a drive appends derived
        // columns past the scan's).
        const hs = batch.hashed orelse continue;
        const digests = (if (ci < hs.len) hs[ci] else null) orelse continue;
        const out = &scratch.digests[ci];
        try out.resize(allocator, kept);
        var j: usize = 0;
        for (mask, digests[0..batch.row_count]) |m, d| {
            if (!m) continue;
            out.items[j] = d;
            j += 1;
        }
        scratch.hashed[ci] = out.items;
    }
    // The run sidecar describes the uncompacted rows; consumers fall back
    // to the views without it.
    return .{
        .batch = .{
            .schema = batch.schema,
            .values = scratch.views,
            .row_count = kept,
            .hashed = if (batch.hashed != null) scratch.hashed else null,
        },
        .weights = kept_weights,
        .ids = kept_ids,
    };
}

fn keepMasked(comptime T: type, allocator: Allocator, values: []const T, mask: []const bool, kept: usize, out: *std.ArrayListUnmanaged(T)) ![]const T {
    try out.resize(allocator, kept);
    var j: usize = 0;
    for (mask, values[0..mask.len]) |m, v| {
        if (!m) continue;
        out.items[j] = v;
        j += 1;
    }
    return out.items;
}

fn runGridScanBurst(job: SiloGridJob, scan_exhausted: *bool, marked_scan_done: *bool) !void {
    const claim_t0 = if (job.profile) platform.nowTicks() else 0;
    const tile = claimScanTile(job) orelse {
        if (job.profile) job.local.sched_scan_claim_ticks += platform.nowTicks() - claim_t0;
        scan_exhausted.* = true;
        try markGridScanDone(job, marked_scan_done);
        return;
    };
    if (job.profile) job.local.sched_scan_claim_ticks += platform.nowTicks() - claim_t0;
    job.local.sched_scan_jobs += 1;
    errdefer _ = job.shared.active_scan_jobs.fetchSub(1, .release);
    openGridScanTile(job, tile);

    job.local.scan_quanta += 1;
    const layout = job.shared.group_rows_layout;
    const encoded = job.drive.ptr == @as(*anyopaque, job.scan) and layout.has_weight and !layout.has_rowref and
        !layout.hashed_key and !layout.has_str_payload and layout.columns.len == 0 and
        layout.key_columns.len > 0 and layout.key_columns.len <= MAX_GENERIC_GROUP_KEYS and !job.shared.generic_filter_required;
    while (true) {
        const t0 = if (job.profile) platform.nowTicks() else 0;
        if (encoded) {
            var consumer = EncodedCountGroups{ .job = job };
            if (try job.scan.consume_encoded(&consumer)) {
                if (job.profile) job.local.scan_ticks += platform.nowTicks() - t0 - consumer.route_ticks;
                job.local.scan_batches += 1;
                continue;
            }
        }
        const maybe_batch = if (job.distinct) |gd| try gd.next(job.scan, job.shared.allocator) else try job.drive.next();
        if (job.profile) job.local.scan_ticks += platform.nowTicks() - t0;
        const scanned = maybe_batch orelse break;
        job.local.scan_batches += 1;
        const weights = if (job.distinct) |gd| gd.rowWeights() else null;
        const kept: KeptRows = if (job.shared.count_sketch) |pass| switch (pass.mode) {
            .tally => {
                try tallyRows(job, pass.sketch, scanned, weights);
                continue;
            },
            .filter => (try keptRows(job, job.scan.allocator, pass.sketch, scanned, weights)) orelse continue,
        } else .{ .batch = scanned, .weights = weights };
        const hashed_ids = if (layout.hashed_key) kept.ids orelse try batchIdentities(job, job.scan.allocator, kept.batch) else null;
        try appendBatchRawChunks(job.local, job.shared, kept.batch, kept.weights, hashed_ids, job.raw_chunk_rows, job.profile, job.filter_fused);
    }

    if (job.raw_group_mode != .off and job.shared.raw_scan_queues.len > 0) {
        const publish_t0 = if (job.profile) platform.nowTicks() else 0;
        try publishActiveRawRows(job.local, job.shared, job.raw_chunk_rows);
        if (job.profile) job.local.publish_ticks += platform.nowTicks() - publish_t0;
    }

    if (tile.hi == job.shared.total_scan_rgs) {
        try markGridScanDone(job, marked_scan_done);
    }
    _ = job.shared.active_scan_jobs.fetchSub(1, .release);
    if (job.shared.scan_tiles.exhausted()) job.shared.signalWork(std.math.maxInt(u32));
}

fn markGridScanDone(job: SiloGridJob, marked_scan_done: *bool) !void {
    if (marked_scan_done.*) return;
    if (job.raw_group_mode != .off) {
        const publish_t0 = if (job.profile) platform.nowTicks() else 0;
        if (job.shared.raw_scan_queues.len > 0) {
            try publishActiveRawRows(job.local, job.shared, job.raw_chunk_rows);
        }
        if (job.profile) job.local.publish_ticks += platform.nowTicks() - publish_t0;
    }
    if (job.shared.scans_done.fetchAdd(1, .release) + 1 == job.shared.scan_threads) job.shared.signalWork(std.math.maxInt(u32));
    marked_scan_done.* = true;
}

/// The parts a deferred bucket fold of `rows` rows splits into: a power of
/// two, so a part is the top bits of a key's table hash.
fn deferPartCount(rows: usize) usize {
    const want = (rows + DEFER_PART_ROWS - 1) / DEFER_PART_ROWS;
    return @min(MAX_DEFER_PARTS, std.math.ceilPowerOfTwoAssert(usize, @max(1, want)));
}

/// Each row's part, and the rows each part receives.
fn deferPartIds(comptime key_width: GroupKeyWidth, chunks: []const GroupChunk, part_ids: []u8, counts: []u32, part_bits: u6) void {
    @memset(counts, 0);
    const Table = if (key_width == .u128) WideGroupTable else GroupTable;
    const shift: u6 = @intCast(@as(u7, 64) - part_bits);
    var row_idx: usize = 0;
    for (chunks) |chunk| {
        const rows = chunk.rows;
        const n = rows.len();
        const keys = switch (key_width) {
            .u32 => rows.keyU32All(),
            .u64 => rows.keyU64All(),
            .u96 => rows.keyU96LoAll(),
            .u128 => rows.keyU128All(),
        };
        const key_hi: []const u32 = if (key_width == .u96) rows.keyU96HiAll() else &.{};
        for (part_ids[row_idx..][0..n], 0..) |*id, r| {
            const part: u8 = @intCast(Table.hashKey(groupKeyAt(key_width, keys, key_hi, r)) >> shift);
            id.* = part;
            counts[part] += 1;
        }
        row_idx += n;
    }
}

/// Scatters one lane of a bucket's chunks into its parts, each row to the
/// next slot of its part.
fn scatterPartLane(layout: GroupRowsLayout, parts: []GroupRows, chunks: []const GroupChunk, part_ids: []const u8, lane: StageLane) !void {
    switch (lane.width(layout)) {
        1 => scatterPartRuns(u8, parts, chunks, part_ids, lane),
        2 => scatterPartRuns(u16, parts, chunks, part_ids, lane),
        4 => scatterPartRuns(u32, parts, chunks, part_ids, lane),
        8 => scatterPartRuns(u64, parts, chunks, part_ids, lane),
        16 => scatterPartRuns(u128, parts, chunks, part_ids, lane),
        else => return error.UnsupportedOperatorForType,
    }
}

fn scatterPartRuns(comptime T: type, parts: []GroupRows, chunks: []const GroupChunk, part_ids: []const u8, lane: StageLane) void {
    var dst: [MAX_DEFER_PARTS][*]T = undefined;
    var next = [_]u32{0} ** MAX_DEFER_PARTS;
    for (parts, 0..) |part, p| {
        // A part no row maps to has no slab to point into.
        if (part.len() == 0) continue;
        const lane_dst: []T = @alignCast(std.mem.bytesAsSlice(T, lane.bytes(part)));
        dst[p] = lane_dst.ptr;
    }
    var row_idx: usize = 0;
    for (chunks) |chunk| {
        const n = chunk.rows.len();
        const src: []const T = @alignCast(std.mem.bytesAsSlice(T, lane.bytes(chunk.rows)));
        for (src[0..n], part_ids[row_idx..][0..n]) |v, p| {
            dst[p][next[p]] = v;
            next[p] += 1;
        }
        row_idx += n;
    }
}

/// Folds every row of one bucket at once, after staging completes (see
/// DEFER_FOLD_MIN_GROUPS). Each part's groups fold together into the worker's
/// cache-sized table. A top-N grid takes each group into its top set while
/// hot; with no COUNT(DISTINCT) sets keyed by group id, the group then needs
/// no lasting state at all.
fn foldDeferredBucket(
    shared: *PipeShared,
    local: *WorkerParts,
    scratch: *GroupScratch,
    lane: usize,
    top: *TopSet,
    group_ticks: *i64,
    chunk_count: *u64,
    profile: bool,
) !bool {
    const allocator = shared.allocator;
    if (!claimRawQueueLaneExact(shared.raw_group_queues, lane)) return false;
    local.defer_chunks.clearRetainingCapacity();
    errdefer for (local.defer_chunks.items) |*chunk| chunk.rows.deinit(allocator);
    {
        defer releaseRawQueueLane(shared.raw_group_queues, lane);
        var popped: [MAX_RAW_BATCH_CHUNKS]GroupChunk = undefined;
        while (true) {
            const n = popGroupChunkBatchFromQueue(&shared.raw_group_queues[lane], &popped, MAX_RAW_BATCH_CHUNKS, &local.raw_group_queue_lock_ticks);
            if (n == 0) break;
            local.defer_chunks.appendSlice(allocator, popped[0..n]) catch |err| {
                for (popped[0..n]) |*chunk| chunk.rows.deinit(allocator);
                return err;
            };
        }
    }
    const chunks = local.defer_chunks.items;
    if (chunks.len == 0) {
        local.sched_group_misses += 1;
        return false;
    }
    _ = shared.active_group_jobs.fetchAdd(1, .release);
    defer _ = shared.active_group_jobs.fetchSub(1, .release);
    var total_rows: usize = 0;
    for (chunks) |chunk| total_rows += chunk.rows.len();
    defer {
        _ = shared.stage_outstanding_rows.fetchSub(total_rows, .release);
        _ = shared.stage_outstanding_chunks.fetchSub(chunks.len, .release);
    }

    const bucket = &shared.buckets[lane % shared.bucket_count];
    const lock_t0 = if (profile) platform.nowTicks() else 0;
    lockSpin(&bucket.agg_lock);
    defer bucket.agg_lock.unlock();
    if (profile) local.raw_agg_lock_ticks += platform.nowTicks() - lock_t0;
    const g0 = if (profile) platform.nowTicks() else 0;

    const layout = shared.group_rows_layout;
    if (local.defer_table == null) local.defer_table = .{ .narrow = try GroupTable.init(allocator, DEFER_PART_ROWS) };
    const table = &local.defer_table.?;
    const transient = top.items.len != 0 and layout.distinct_slot_count == 0;
    const states = if (transient) &local.defer_states else &bucket.states;
    const part_count = deferPartCount(total_rows);
    if (part_count == 1) {
        try foldDeferredPart(allocator, scratch, bucket, table, states, transient, top, chunks, total_rows);
    } else {
        try local.defer_part_ids.resize(allocator, total_rows);
        const part_ids = local.defer_part_ids.items;
        var counts: [MAX_DEFER_PARTS]u32 = undefined;
        switch (layout.key_width) {
            inline else => |kw| deferPartIds(kw, chunks, part_ids, counts[0..part_count], std.math.log2_int(usize, part_count)),
        }
        while (local.defer_parts.items.len < part_count) try local.defer_parts.append(allocator, .{});
        const parts = local.defer_parts.items[0..part_count];
        for (parts, counts[0..part_count]) |*part, n| try part.resize(allocator, layout, n);
        var lanes: [MAX_STAGE_LANES]StageLane = undefined;
        for (StageLane.all(layout, &lanes)) |stage_lane| try scatterPartLane(layout, parts, chunks, part_ids, stage_lane);
        for (parts) |part| {
            if (part.len() == 0) continue;
            try foldDeferredPart(allocator, scratch, bucket, table, states, transient, top, &.{.{ .rows = part, .owner_worker = 0, .bucket_idx = lane }}, part.len());
        }
    }
    bucket.row_count += total_rows;
    if (profile) group_ticks.* += platform.nowTicks() - g0;

    // Staging is complete, so nothing will take a recycled slab.
    for (chunks) |*chunk| chunk.rows.deinit(allocator);
    chunk_count.* += chunks.len;
    local.defer_chunks.clearRetainingCapacity();
    local.sched_group_jobs += 1;
    if (shared.pending_group_rows.fetchSub(total_rows, .release) == total_rows) shared.signalWork(std.math.maxInt(u32));
    return true;
}

/// Folds one part's rows, all of its groups' rows, into the emptied table.
fn foldDeferredPart(
    allocator: Allocator,
    scratch: *GroupScratch,
    bucket: *PipeBucket,
    table: *BucketTable,
    states: *StateSlab,
    transient: bool,
    top: *TopSet,
    chunks: []const GroupChunk,
    part_rows: usize,
) !void {
    try table.resetFor(allocator, part_rows);
    if (transient) states.clearRetainingCapacity();
    const first_new = states.len;
    for (chunks) |chunk| {
        try groupChunkRowsDirect(table, states, &bucket.str_states, &bucket.concat_states, &bucket.udf_states, bucket.udf_arena.allocator(), &bucket.distinct_sets, scratch, allocator, bucket.str_arena.allocator(), &bucket.str_bank, chunk.rows, part_rows, part_rows, 0, null);
    }
    if (top.items.len == 0) return;
    var gid = first_new;
    while (gid < states.len) : (gid += 1) top.consider_state(states.ref(gid));
}

fn runReadyGroup(job: SiloGridJob, scratch: *GroupScratch, lane: usize) !bool {
    if (job.shared.defer_fold) return foldDeferredBucket(job.shared, job.local, scratch, lane, job.top, job.group_ticks, job.chunks, job.profile);
    if (!claimRawQueueLaneExact(job.shared.raw_group_queues, lane)) return false;
    if (try drainRawDedicatedGroupLane(
        job.shared.allocator,
        job.shared,
        job.local,
        scratch,
        lane,
        job.raw_group_chunk_rows,
        job.raw_batch_chunks,
        job.group_ticks,
        job.chunks,
        job.profile,
    )) {
        job.local.sched_group_jobs += 1;
        return true;
    }
    job.local.sched_group_misses += 1;
    return false;
}

fn siloGridWorkerErr(job: SiloGridJob) !void {
    if (job.shared.group_rows_layout.has_str_payload) return runSiloGridWorker(true, job);
    return runSiloGridWorker(false, job);
}

fn runSiloGridWorker(comptime downstream_first: bool, job: SiloGridJob) !void {
    var scratch: GroupScratch = .{};
    defer scratch.deinit(job.shared.allocator);

    var marked_scan_done = false;
    var scan_exhausted = false;
    var idle_spins: usize = 0;

    while (true) {
        const seen_epoch = job.shared.work_epoch.load(.acquire);
        // A peer failed: stop scheduling and tear down (the failing worker's
        // error is already recorded; ours would just race it).
        if (job.shared.aborted.load(.acquire)) return;
        if (job.shared.resources) |a| try a.checkCancelled();
        job.local.sched_loops += 1;
        const decision_t0 = if (job.profile) platform.nowTicks() else 0;
        const scan_claims_available = !scan_exhausted and !job.shared.scan_tiles.exhausted();
        const global_scan_finished = job.shared.scan_tiles.exhausted() and
            job.shared.active_scan_jobs.load(.acquire) == 0;
        if (global_scan_finished and !marked_scan_done) {
            if (job.profile) job.local.sched_decision_ticks += platform.nowTicks() - decision_t0;
            scan_exhausted = true;
            try markGridScanDone(job, &marked_scan_done);
            idle_spins = 0;
            continue;
        }
        if (job.profile) job.local.sched_decision_ticks += platform.nowTicks() - decision_t0;

        if (job.raw_group_mode == .staged_final) {
            const scan_choice = if (scan_claims_available) minUnlockedScanLane(job.shared.raw_scan_queues, job.worker_index) else null;
            const stage_choice = maxUnlockedQueueLaneRows(job.shared.raw_scan_queues, job.worker_index);
            const folds_held = job.shared.defer_fold and !job.shared.deferred_folds_released();
            const group_choice = if (folds_held) null else maxUnlockedQueueLaneRows(job.shared.raw_group_queues, job.worker_index);
            const max_stage_rows = if (stage_choice) |choice| choice.rows else 0;
            const max_group_rows = if (group_choice) |choice| choice.rows else 0;
            const downstream_max_rows = @max(max_stage_rows, max_group_rows);

            // Queue counts differ by stage; comparing their largest row counts
            // can starve consumers while variable-width payloads accumulate.
            if (downstream_first) {
                if (group_choice) |choice| {
                    if (try runReadyGroup(job, &scratch, choice.lane)) {
                        idle_spins = 0;
                        continue;
                    }
                }
            }

            if (!downstream_first) if (scan_choice) |choice| {
                if (scan_claims_available and (downstream_max_rows == 0 or choice.rows < downstream_max_rows) and claimRawScanLaneExact(job.shared.raw_scan_queues, choice.lane)) {
                    var release_scan = true;
                    errdefer if (release_scan) releaseRawScanLane(job.shared.raw_scan_queues, choice.lane);
                    job.local.raw_scan_lane = choice.lane;
                    try runGridScanBurst(job, &scan_exhausted, &marked_scan_done);
                    releaseRawScanLane(job.shared.raw_scan_queues, choice.lane);
                    release_scan = false;
                    idle_spins = 0;
                    continue;
                }
            };

            if (max_stage_rows > (if (downstream_first) @as(u64, 0) else max_group_rows)) {
                if (stage_choice) |choice| {
                    if (claimRawQueueLaneExact(job.shared.raw_scan_queues, choice.lane)) {
                        const did_stage = try drainRawDedicatedStageSharedBuilders(
                            job.shared.allocator,
                            job.shared,
                            job.local,
                            choice.lane,
                            job.raw_chunk_rows,
                            job.raw_group_chunk_rows,
                            job.raw_batch_chunks,
                            job.profile,
                        );
                        if (did_stage) {
                            job.local.sched_stage_jobs += 1;
                            idle_spins = 0;
                            continue;
                        }
                        job.local.sched_group_misses += 1;
                    }
                }
            }

            if (!downstream_first and max_group_rows > 0) {
                if (group_choice) |choice| {
                    if (try runReadyGroup(job, &scratch, choice.lane)) {
                        idle_spins = 0;
                        continue;
                    }
                }
            }

            const no_more_stage_input = global_scan_finished and
                job.shared.outstanding_chunks.load(.acquire) == 0 and
                job.shared.active_stage_jobs.load(.acquire) == 0;
            const has_stage_partials = job.shared.stage_builder_rows.load(.acquire) > 0;
            if (no_more_stage_input and has_stage_partials) {
                const flushed = try flushSharedStageBuilders(job.shared, job.local, job.profile);
                if (flushed) {
                    idle_spins = 0;
                    continue;
                }
            }

            if (scan_choice) |choice| {
                if (scan_claims_available and claimRawScanLaneExact(job.shared.raw_scan_queues, choice.lane)) {
                    var release_scan = true;
                    errdefer if (release_scan) releaseRawScanLane(job.shared.raw_scan_queues, choice.lane);
                    job.local.raw_scan_lane = choice.lane;
                    try runGridScanBurst(job, &scan_exhausted, &marked_scan_done);
                    releaseRawScanLane(job.shared.raw_scan_queues, choice.lane);
                    release_scan = false;
                    idle_spins = 0;
                    continue;
                }
            }

            if (job.shared.grouping_complete()) {
                break;
            }

            const idle_t0 = if (job.profile) platform.nowTicks() else 0;
            job.local.sched_idle_loops += 1;
            idle_spins += 1;
            if (idle_spins < GRID_IDLE_SPINS) {
                std.atomic.spinLoopHint();
            } else if (idle_spins < GRID_IDLE_SPINS + GRID_IDLE_YIELDS) {
                std.Thread.yield() catch {};
            } else {
                job.shared.parkUntilSignal(seen_epoch);
                // Back from a park, a miss parks again without re-spinning.
                idle_spins -= 1;
            }
            if (job.profile) job.idle_ticks.* += platform.nowTicks() - idle_t0;
            continue;
        }
    }

    if (!marked_scan_done) {
        try markGridScanDone(job, &marked_scan_done);
    }
    // A deferred fold took each group into the top set as it folded.
    if (job.shared.defer_fold) {
        job.local.releaseDeferScratch(job.shared.allocator);
        return;
    }
    try collectOwnedTop(job.shared, job.worker_index, job.worker_count, job.top, job.top_ticks, job.profile);
}

/// Whether the grid defers every bucket's fold until all rows are staged (see
/// DEFER_FOLD_MIN_GROUPS). Staging then holds every input row at once, so it
/// must fit well within the memory budget. String payloads and per-group
/// finalized aggregates keep folding as rows arrive. So does COUNT(DISTINCT):
/// a bucket's distinct members share one (gid, value) set that parts don't
/// split, so deferring would add the staging copy and keep the set's misses.
fn deferBucketFolds(layout: GroupRowsLayout, n_workers: usize, staged: bool, expected_groups_per_bucket: usize, input_rows: u64, resources: ?*thindb.exec.memory.MemoryAccountant) bool {
    if (!staged or n_workers < 2 or expected_groups_per_bucket < DEFER_FOLD_MIN_GROUPS) return false;
    if (layout.has_str_payload or hasStringResults(layout) or layout.has_concat or layout.has_udf or layout.distinct_slot_count != 0) return false;
    const accountant = resources orelse return false;
    const staged_bytes = std.math.mul(u64, input_rows, GroupRows.slabBytes(layout, 1)) catch return false;
    return staged_bytes <= accountant.pool.budget / DEFER_FOLD_BUDGET_DIVISOR;
}

pub const RunConfig = struct {
    dop: usize,
    // The default bucket count, which a run expecting many groups raises (see
    // bucketCountForGroups) unless `bucket_count_set` pins it.
    bucket_count: usize,
    bucket_count_set: bool = false,
    // The view of the table the run reads. The caller captures it under the
    // table's shared ddl_lock and keeps both until it has resolved the rowrefs
    // the run returns, which index this snapshot's segments and memtable.
    snapshot: Scan.Snapshot,
    // Free the staging-chunk pools (gigabytes of recycled RawRows slabs) on a
    // detached thread after the result is built, instead of on the wire path.
    // Requires `allocator` to be thread-safe. A tracked one stops charging the
    // query before the run returns and keeps its accountant alive until the
    // memory is back; an untracked one must outlive the query. The engine sets
    // this only for the fresh-workspace path (never the arena).
    defer_heavy_teardown: bool = false,
    // String group-key columns the worker scans emit as key digests
    // (`Batch.hashed`) instead of materialized strings — hashed-key shapes
    // where the key bytes only return at emit via rowref late-mat. A scan
    // decline (or a sidecar-less batch) falls back to digesting bytes.
    hash_key_columns: []const []const u8 = &.{},
    // Columns the derived expressions read only through `octet_length`,
    // which the worker scans emit as byte lengths (`Batch.byte_lengths`).
    byte_length_columns: []const []const u8 = &.{},
    stream: bool = false,
    pipe: bool = false,
    pipe_tail: bool = false,
    silo: bool = false,
    silo_grid: bool = false,
    local_preagg: bool = false,
    q30_inline: bool = false,
    late_filter: bool = false,
    scan_filter: bool = false,
    pipe_scan_threads: usize = 0,
    chunk_rows: usize = PIPE_CHUNK_ROWS,
    chunk_rows_set: bool = false,
    scan_tile_rgs: usize = GRID_SCAN_TILE_RGS,
    scan_tile_rgs_set: bool = false,
    scan_coalesce_tiles: usize = GRID_SCAN_COALESCE_TILES,
    route_block_rows: usize = AUTO_ROUTE_BLOCK_ROWS,
    route_block_rows_set: bool = false,
    scan_yield_chunks: usize = GRID_SCAN_YIELD_CHUNKS,
    scan_resume_chunks: usize = 0,
    elastic_backlog: usize = 0,
    group_lease_buckets: usize = 1,
    group_lease_rows: u64 = 0,
    group_init_cap: usize = 0,
    shared_scan_buffers: bool = false,
    shared_scan_banks: usize = 1,
    force_queue_publish: bool = false,
    flat_scan_partitions: bool = false,
    raw_group_mode: RawGroupMode = .off,
    raw_chunk_rows: usize = 32768,
    raw_group_chunk_rows: usize = 0,
    raw_batch_chunks: usize = 4,
    group_rows_layout: GroupRowsLayout = .{},
    scan_columns: ?[]const []const u8 = null,
    derived: []const thindb.exec.Derived = &.{},
    udf_registry: ?*const udf_mod.UdfRegistry = null,
    resources: ?*thindb.exec.memory.MemoryAccountant = null,
    filter_expr: ?thindb.exec.PredicateExpr = null,
    /// Hints the consumer offered on scan columns, after the filter's own.
    offered_prunes: Scan.OfferedPrunes = .{},
    key_bound: ?thindb.exec.KeyBoundSpec = null,
    count_sketch: ?CountSketchPass = null,
    shared_stage_builders: bool = false,
    // Collapse each scanned batch to its distinct rows before the derived
    // columns evaluate, where `dictEvalColumn` allows it.
    distinct_eval: bool = false,
    no_profile: bool = false,
    quiet: bool = false,
    result_out: ?*std.ArrayListUnmanaged(TopRow) = null,
    // Rows retained by the in-core count-desc top-N (the query's LIMIT+OFFSET).
    // Ignored when `result_all_groups` is set.
    top_k: usize = TOP_K,
    result_all_groups: bool = false,
    // Stop the all-groups emit after this many groups (an unordered `LIMIT N`
    // needs only any N+offset groups). 0 means emit every group.
    result_all_groups_cap: usize = 0,
    // Per-group predicate applied during the all-groups emit (HAVING for a
    // capped unordered LIMIT). Only passing groups count toward the cap.
    result_emit_filter: ?EmitFilter = null,
    // The caller's ORDER BY ... LIMIT over the all-groups emit; see EmitRank.
    result_rank: ?EmitRank = null,
    trace_timing: bool = false,
    workspace: ?*SiloGridWorkspace = null,
};

fn localReservePerBucket(total_rows: u64, dop: usize, bucket_count: usize, chunk_rows: usize, route_block_rows: usize) usize {
    const denom = @as(u64, @intCast(@max(@as(usize, 1), dop) * @max(@as(usize, 1), bucket_count)));
    const estimated_per_worker_bucket = (total_rows + denom - 1) / denom;
    const chunk_u64: u64 = @intCast(chunk_rows);
    const route_slack: u64 = @intCast(@min(chunk_rows, route_block_rows));
    const reserve = if (estimated_per_worker_bucket * 2 + route_slack >= chunk_u64)
        chunk_u64
    else
        estimated_per_worker_bucket;
    return @intCast(@min(chunk_u64, @max(@as(u64, 16), reserve)));
}

// Combined-key allocation hint: saturating product of the KEY
// columns' NDVs, resolved BY NAME against the stats scan's output schema —
// the projection orders filter columns ahead of keys, so positional indexing
// would read the wrong columns' NDVs. A derived key has no schema entry →
// null (caller falls back to the conservative estimate).
fn estimateGroupCountFromStats(stats: thindb.exec.PipelineStats, schema: []const thindb.types.Column, key_columns: []const GroupKeyColumnSpec) ?u64 {
    if (key_columns.len == 0) return null;
    var est: u64 = 1;
    for (key_columns) |kc| {
        const idx = thindb.types.findColumn(schema, kc.name) orelse return null;
        if (idx >= stats.column_stats.len) return null;
        switch (stats.column_stats[idx].ndv) {
            .exact => |ndv| est *|= @max(@as(u64, 1), ndv),
            .unknown => return null,
        }
    }
    return @min(est, @max(stats.upper_rows, 1));
}

/// Whether one key column alone is essentially unique by its stats: with
/// NDV ≥ 90% of the rows, at least 80% of them are one-row groups, so the
/// top groups rarely clear a count sketch's collision floor and a tally is a
/// wasted scan. The segment sketches' NDV runs ~3% off, so a unique column
/// can read a few percent short; stats can't see skew, so a looser bound
/// would turn away mostly-unique keys with a heavy head, which the sketch
/// serves best. Composite estimates multiply marginals and saturate at the
/// row count, so only a single column's NDV says this.
fn nearUniqueKeyColumn(total_rows: u64, stats: thindb.exec.PipelineStats, schema: []const thindb.types.Column, key_columns: []const GroupKeyColumnSpec) bool {
    for (key_columns) |kc| {
        const idx = thindb.types.findColumn(schema, kc.name) orelse continue;
        if (idx >= stats.column_stats.len) continue;
        switch (stats.column_stats[idx].ndv) {
            .exact => |ndv| if (ndv * 10 >= total_rows * 9) return true,
            .unknown => {},
        }
    }
    return false;
}

fn expectedTotalGroups(total_rows: u64, stats: thindb.exec.PipelineStats, schema: []const thindb.types.Column, key_columns: []const GroupKeyColumnSpec, generic_key_width: GroupKeyWidth, generic_has_filter: bool) u64 {
    const conservative_total = @max(@as(u64, 16), total_rows / 4);
    const generic_key_count = key_columns.len;
    const estimated_total = estimateGroupCountFromStats(stats, schema, key_columns) orelse conservative_total;
    const has_filter = generic_has_filter;
    const generic_wide_no_filter = generic_key_count != 0 and !has_filter and (generic_key_width == .u96 or generic_key_width == .u128);
    const no_filter_near_unique = !has_filter and estimated_total * 4 >= total_rows * 3;
    // These are allocation hints, not exact cardinalities: column sketches
    // estimate marginal NDVs and compound keys may be correlated. Buckets
    // start small and use observed joint-key density when growing. A filtered
    // input also bounds the speculative reservation; real groups can grow
    // beyond every hint without changing the aggregation algorithm.
    const filtered_init_cap: u64 = 2 * 1024 * 1024;
    return if (generic_wide_no_filter)
        total_rows
    else if (no_filter_near_unique)
        estimated_total
    else if (has_filter)
        @min(@min(estimated_total, conservative_total), filtered_init_cap)
    else
        @min(estimated_total, conservative_total);
}

fn expectedGroupsPerBucket(total_groups: u64, bucket_count: usize) usize {
    const per_bucket = (total_groups + @as(u64, @intCast(bucket_count)) - 1) / @as(u64, @intCast(bucket_count));
    return @intCast(@max(@as(u64, 16), per_bucket));
}

// Each bucket's group table is probed at random, so past roughly
// GROUPS_PER_BUCKET groups its probes leave the core's cache. More buckets keep
// the tables small, but each adds a route block per worker to the scatter, so
// the count grows from the default only as the expected groups demand.
// Measured on 100M ClickBench rows at 16 workers: WatchID,ClientIP (100M
// groups) 1.25 s at 256 buckets, 1.06 s at 1024; URL (18M groups) lost 3% at
// 1024 and 12% at 2048.
const GROUPS_PER_BUCKET: u64 = 100_000;
const MAX_AUTO_BUCKETS: usize = 1024;

fn bucketCountForGroups(default_count: usize, total_groups: u64) usize {
    if (default_count >= MAX_AUTO_BUCKETS) return default_count;
    const wanted = std.math.ceilPowerOfTwo(u64, @max(@as(u64, 1), total_groups / GROUPS_PER_BUCKET)) catch MAX_AUTO_BUCKETS;
    return @intCast(std.math.clamp(wanted, default_count, MAX_AUTO_BUCKETS));
}

fn chooseGridChunkRows(cfg_chunk_rows: usize, chunk_rows_set: bool) usize {
    return if (chunk_rows_set) cfg_chunk_rows else GRID_CHUNK_ROWS;
}

fn chooseGridScanTileRgs(cfg_scan_tile_rgs: usize, scan_tile_rgs_set: bool) usize {
    if (scan_tile_rgs_set) return cfg_scan_tile_rgs;
    return GRID_SCAN_TILE_RGS;
}

pub fn runSiloGrid(allocator: Allocator, table: *thindb.api.Table, cpus: []const usize, cfg: RunConfig) !void {
    const freq = platform.perfFreq();
    const function_t0 = platform.nowTicks();
    g_cp_on = PROFILING and (getenv("THINDB_V2_CHUNK_PROFILE") != null);
    if (comptime PROFILING) chunkProfileReset();
    defer chunkProfileDump();
    var pre_return_ticks: i64 = 0;
    defer {
        if ((PROFILING and cfg.trace_timing) and pre_return_ticks != 0) {
            std.debug.print("[harness-core-cleanup] query={s} total_cleanup_after_return={d:.1}ms\n", .{
                "generic",
                platform.ticksToMs(platform.nowTicks() - pre_return_ticks, freq),
            });
        }
    }
    // The stats scan and every worker scan read this snapshot: merge its
    // column stats once for all of them.
    var snap = cfg.snapshot;
    const column_stats: []thindb.exec.ColStat = if (snap.column_stats.len == 0) try Scan.snapshotColumnStats(allocator, table, snap) else &.{};
    defer if (column_stats.len > 0) allocator.free(column_stats);
    if (column_stats.len > 0) snap.column_stats = column_stats;
    const total = totalRows(snap);
    const dop = @max(@as(usize, 1), cfg.dop);

    const chunk_rows = chooseGridChunkRows(cfg.chunk_rows, cfg.chunk_rows_set);
    const raw_chunk_rows = @max(@as(usize, 1), cfg.raw_chunk_rows);
    const raw_group_chunk_rows = if (cfg.raw_group_chunk_rows == 0) raw_chunk_rows else @max(@as(usize, 1), cfg.raw_group_chunk_rows);
    const raw_batch_chunks = @max(@as(usize, 1), cfg.raw_batch_chunks);
    const group_rows_layout = cfg.group_rows_layout;
    const use_raw_group = cfg.raw_group_mode != .off;
    const scan_tile_rgs = chooseGridScanTileRgs(cfg.scan_tile_rgs, cfg.scan_tile_rgs_set);
    const scan_coalesce_tiles = cfg.scan_coalesce_tiles;

    const snapshot_setup_t0 = if ((PROFILING and cfg.trace_timing)) platform.nowTicks() else 0;
    const seg_start = try allocator.alloc(usize, snap.segment_count + 1);
    defer allocator.free(seg_start);
    var total_rgs: usize = 0;
    for (snap.segments, 0..) |entry, i| {
        seg_start[i] = total_rgs;
        total_rgs += entry.row_group_count;
    }
    seg_start[snap.segment_count] = total_rgs;
    // The memtable rides whichever scan tile ends at `total_scan_rgs`
    // (`openGridScanTile` sets scan_memtable on tile.hi == total). With ZERO
    // segment row groups no tile is ever claimable (lo=0 >= total=0), so a
    // memtable-only table silently aggregated to nothing — give the claim
    // space one synthetic unit so exactly one worker opens the empty-range,
    // memtable-bearing tile. (Mirrors v2_lowcard_group.workerRun's
    // total_rgs == 0 arm; flatToCoord maps both ends to (0, 0).)
    const claim_total_rgs = if (total_rgs == 0 and snap.memtable_snap.row_count > 0) 1 else total_rgs;

    const scan_columns = cfg.scan_columns orelse &[_][]const u8{};
    const resources = cfg.resources orelse thindb.exec.memory.accountantOf(allocator);
    // Worker scans' decode and digest buffers recycle through the process
    // buffer pool, as ParallelScan's do: fresh ones each query cost page
    // faults to fill and an OS release at teardown, ahead of the response.
    const scan_allocator = try thindb.exec.memory.workerAllocator(resources, table.allocator);
    var stats_scan = try Scan.allocWithProjectionLoc(scan_allocator, table, null, scan_columns, false, snap);
    defer {
        const cleanup_t0 = if ((PROFILING and cfg.trace_timing)) platform.nowTicks() else 0;
        stats_scan.deinit();
        if ((PROFILING and cfg.trace_timing)) std.debug.print("[harness-core-cleanup] query={s} stats_scan={d:.3}ms\n", .{
            "generic",
            platform.ticksToMs(platform.nowTicks() - cleanup_t0, freq),
        });
    }
    // Right-size the worker fleet to the work that survives zone-map pruning:
    // fuse the filter into the stats scan (the same hint set every worker scan
    // gets) and count the surviving row groups. A selective filter then spins
    // ceil(surviving/RGS_PER_GRID_WORKER) workers instead of the full DOP; an
    // unfusable or absent filter keeps full DOP. The same survival mask cuts
    // the scan tiles, so every sized worker finds surviving work to claim.
    const memtable_units = (@as(usize, @intCast(snap.memtable_snap.row_count)) + 65535) / 65536;
    var work_rgs: usize = total_rgs + memtable_units;
    var hinted = false;
    if (cfg.filter_expr) |expr| hinted = applyScanFilterExpr(stats_scan, expr) catch false;
    if (try cfg.offered_prunes.apply(stats_scan)) hinted = true;
    const surviving_mask: ?[]bool = if (hinted) stats_scan.survivingMask(allocator) else null;
    defer if (surviving_mask) |m| allocator.free(m);
    if (surviving_mask) |mask| {
        work_rgs = memtable_units;
        for (mask) |m| work_rgs += @intFromBool(m);
    } else if (hinted) {
        if (stats_scan.survivingWorkUnits()) |surviving| work_rgs = surviving;
    }
    const sized_workers = (work_rgs + RGS_PER_GRID_WORKER - 1) / RGS_PER_GRID_WORKER;
    var grant = core_scheduler.reserveWorkers(@min(dop, @min(cpus.len, @max(sized_workers, 1))));
    defer grant.release();
    const n_workers = grant.count;

    // Narrow the scatter for small filtered inputs. Route blocks hold
    // `route_block_rows` rows PER BUCKET, so at the default 256 buckets a
    // selective query's chunks scatter a few dozen rows into each block and
    // the flush machinery costs more than the aggregation it feeds (Q37:
    // 13ms routing for 5.4ms of aggregate work). A handful of buckets keeps
    // the blocks dense while the group stage stays n_workers-parallel.
    // Measured on the 660K-row Title GROUP BY: 256 buckets ≈ 28-32ms,
    // 4-64 ≈ 22ms, 1 ≈ 29ms (serial group lane overshoots).
    const small_input = work_rgs <= 64;
    // Partitioning only pays when workers fold buckets in parallel; a lone
    // worker folds one table directly (see PipeShared.serial_fold_scratch).
    const serial_fold = n_workers == 1 and cfg.raw_group_mode == .staged_final;
    const estimated_groups = expectedTotalGroups(total, stats_scan.stats(), stats_scan.outputSchema(), group_rows_layout.key_columns, group_rows_layout.key_width, cfg.filter_expr != null);
    // A tally declines (counting nothing, so the sketch never settles) where
    // the single pass is already cheap or the stats say it can't settle. It
    // groups nothing itself, and a filter pass groups at most the rows it
    // keeps.
    if (cfg.count_sketch) |pass| {
        if (pass.mode == .tally and (small_input or estimated_groups < COUNT_SKETCH_MIN_GROUPS or
            nearUniqueKeyColumn(total, stats_scan.stats(), stats_scan.outputSchema(), group_rows_layout.key_columns))) return;
    }
    const total_groups: u64 = if (cfg.count_sketch) |pass| switch (pass.mode) {
        .tally => 1,
        .filter => @max(1, @min(estimated_groups, pass.sketch.kept_rows)),
    } else estimated_groups;
    const bucket_count = if (serial_fold)
        1
    else if (small_input)
        @min(cfg.bucket_count, @max(@as(usize, 4), n_workers))
    else if (cfg.bucket_count_set)
        cfg.bucket_count
    else
        bucketCountForGroups(cfg.bucket_count, total_groups);
    const route_block_rows = chooseRouteBlockRows(bucket_count, cfg.route_block_rows, cfg.route_block_rows_set);

    const expected_groups_per_bucket = expectedGroupsPerBucket(total_groups, bucket_count);
    // A filter's selectivity is unknown until the scan, so deferring folds is
    // judged on the stats' unfiltered bound on the groups. A filter that keeps
    // few rows leaves a deferred grid little to stage or fold.
    const defer_groups: u64 = if (cfg.count_sketch != null)
        total_groups
    else
        @max(total_groups, estimateGroupCountFromStats(stats_scan.stats(), stats_scan.outputSchema(), group_rows_layout.key_columns) orelse total);
    const init_groups_per_bucket = if (cfg.group_init_cap > 0)
        @max(@as(usize, 16), @min(expected_groups_per_bucket, cfg.group_init_cap))
    else
        expected_groups_per_bucket;

    const direct_final_local = !use_raw_group and !cfg.force_queue_publish and bucket_count >= n_workers * 8;
    const local_reserve_per_bucket = localReservePerBucket(total, n_workers, bucket_count, chunk_rows, route_block_rows);
    const use_flat_scan_partitions = cfg.flat_scan_partitions and direct_final_local;
    const use_shared_scan_buffers = cfg.shared_scan_buffers and direct_final_local and !use_flat_scan_partitions;
    const shared_scan_bank_count = if (use_shared_scan_buffers)
        @max(@as(usize, 1), @min(cfg.shared_scan_banks, n_workers))
    else
        @as(usize, 0);
    const worker_local_reserve_per_bucket: usize = if (use_shared_scan_buffers or use_flat_scan_partitions or use_raw_group) 0 else local_reserve_per_bucket;
    const shared_scan_reserve_per_bucket: usize = if (use_shared_scan_buffers)
        @max(@as(usize, 16), chunk_rows / shared_scan_bank_count)
    else
        0;
    const flat_reserve_per_worker: usize = if (use_flat_scan_partitions)
        @intCast(@min((total + @as(u64, @intCast(n_workers)) - 1) / @as(u64, @intCast(n_workers)), @as(u64, @intCast(chunk_rows * 16))))
    else
        0;
    const snapshot_setup_ticks = if ((PROFILING and cfg.trace_timing)) platform.nowTicks() - snapshot_setup_t0 else 0;

    if (PROFILING and !cfg.quiet) {
        std.debug.print(
            "[clientip] query={s} DOP={d} buckets={d} rows={d} mode=silo-grid workers={d} chunk_rows={d} raw_chunk_rows={d} raw_group_chunk_rows={d} raw_batch_chunks={d} scan_tile_rgs={d} scan_coalesce_tiles={d} route_block_rows={d} group_lease_buckets={d} group_lease_rows={d} local_reserve_per_bucket={d} worker_local_reserve_per_bucket={d} shared_scan_banks={d} shared_scan_reserve_per_bucket={d} flat_reserve_per_worker={d} expected_groups_per_bucket={d} init_groups_per_bucket={d} direct_final_local={s} force_queue_publish={s} shared_scan_buffers={s} flat_scan_partitions={s} raw_group_mode={s} shared_stage_builders={s} scheduler=group_rows_vs_scan_buffer_rows\n",
            .{ "generic", dop, bucket_count, total, n_workers, chunk_rows, raw_chunk_rows, raw_group_chunk_rows, raw_batch_chunks, scan_tile_rgs, scan_coalesce_tiles, route_block_rows, cfg.group_lease_buckets, cfg.group_lease_rows, local_reserve_per_bucket, worker_local_reserve_per_bucket, shared_scan_bank_count, shared_scan_reserve_per_bucket, flat_reserve_per_worker, expected_groups_per_bucket, init_groups_per_bucket, if (direct_final_local) "true" else "false", if (cfg.force_queue_publish) "true" else "false", if (use_shared_scan_buffers) "true" else "false", if (use_flat_scan_partitions) "true" else "false", @tagName(cfg.raw_group_mode), if (cfg.shared_stage_builders) "true" else "false" },
        );
        std.debug.print("[clientip-cpu-assign] mode=silo-grid workers=", .{});
        for (cpus[0..@min(n_workers, cpus.len)], 0..) |cpu, ci| {
            if (ci != 0) std.debug.print(",", .{});
            std.debug.print("{d}", .{cpu});
        }
        std.debug.print("\n", .{});
        std.debug.print("[clientip-affinity] mode=silo-grid total_threads={d} dynamic_scan_tiles=true dynamic_group_leases=true physical_cores_distinct={s}\n", .{ n_workers, if (n_workers <= cpus.len) "true" else "false" });
    }

    var scans = try allocator.alloc(*Scan, n_workers);
    defer allocator.free(scans);
    // Each worker's batch source: a Compute-wrapped scan (derived shapes) or the
    // bare scan. The drive owns its underlying scan, so teardown frees through
    // `drives`, not `scans`.
    var drives = try allocator.alloc(thindb.exec.Query, n_workers);
    defer allocator.free(drives);
    var built_scans: usize = 0;
    defer {
        const cleanup_t0 = if ((PROFILING and cfg.trace_timing)) platform.nowTicks() else 0;
        for (drives[0..built_scans]) |*d| d.deinit();
        if ((PROFILING and cfg.trace_timing)) std.debug.print("[harness-core-cleanup] query={s} worker_scans={d:.1}ms count={d}\n", .{
            "generic",
            platform.ticksToMs(platform.nowTicks() - cleanup_t0, freq),
            built_scans,
        });
    }

    const bucket_setup_t0 = if ((PROFILING and cfg.trace_timing)) platform.nowTicks() else 0;
    const using_workspace = cfg.workspace != null;
    var workspace_profile: WorkspaceProfile = .{};
    const workspace_profile_ptr: ?*WorkspaceProfile = if ((PROFILING and cfg.trace_timing) and using_workspace) &workspace_profile else null;
    var owned_parts: []WorkerParts = &.{};
    defer if (!using_workspace and owned_parts.len > 0) allocator.free(owned_parts);
    var built_parts: usize = 0;
    defer if (!using_workspace) {
        const cleanup_t0 = if ((PROFILING and cfg.trace_timing)) platform.nowTicks() else 0;
        for (owned_parts[0..built_parts]) |*p| p.deinit(allocator);
        if ((PROFILING and cfg.trace_timing)) std.debug.print("[harness-core-cleanup] query={s} worker_parts={d:.1}ms count={d}\n", .{
            "generic",
            platform.ticksToMs(platform.nowTicks() - cleanup_t0, freq),
            built_parts,
        });
    };
    var owned_buckets: []PipeBucket = &.{};
    defer if (!using_workspace and owned_buckets.len > 0) allocator.free(owned_buckets);
    var built_buckets: usize = 0;
    defer if (!using_workspace) {
        const cleanup_t0 = if ((PROFILING and cfg.trace_timing)) platform.nowTicks() else 0;
        for (owned_buckets[0..built_buckets]) |*bkt| bkt.deinit(allocator);
        if ((PROFILING and cfg.trace_timing)) std.debug.print("[harness-core-cleanup] query={s} pipe_buckets={d:.1}ms count={d}\n", .{
            "generic",
            platform.ticksToMs(platform.nowTicks() - cleanup_t0, freq),
            built_buckets,
        });
    };

    var parts: []WorkerParts = undefined;
    var buckets: []PipeBucket = undefined;
    if (cfg.workspace) |workspace| {
        try workspace.ensure(allocator, n_workers, bucket_count, worker_local_reserve_per_bucket, init_groups_per_bucket, cpus, workspace_profile_ptr);
        parts = workspace.parts;
        buckets = workspace.buckets;
        built_parts = parts.len;
        built_buckets = buckets.len;
    } else {
        owned_parts = try allocator.alloc(WorkerParts, n_workers);
        parts = owned_parts;
        owned_buckets = try allocator.alloc(PipeBucket, bucket_count);
        buckets = owned_buckets;
        var b: usize = 0;
        while (b < bucket_count) : (b += 1) {
            buckets[b] = try PipeBucket.init(allocator, init_groups_per_bucket);
            built_buckets += 1;
            try buckets[b].chunks.ensureTotalCapacity(allocator, 8);
        }
    }
    const bucket_setup_ticks = if ((PROFILING and cfg.trace_timing)) platform.nowTicks() - bucket_setup_t0 else 0;
    if (workspace_profile_ptr) |profile| {
        profile.printSetup("generic", bucket_count, worker_local_reserve_per_bucket, init_groups_per_bucket);
    }

    var shared_scan_buffers_storage: SharedScanBuffers = .{};
    var shared_scan_buffers_ptr: ?*SharedScanBuffers = null;
    if (use_shared_scan_buffers) {
        shared_scan_buffers_storage = try SharedScanBuffers.init(allocator, bucket_count, shared_scan_bank_count, shared_scan_reserve_per_bucket);
        shared_scan_buffers_ptr = &shared_scan_buffers_storage;
    }
    defer if (shared_scan_buffers_ptr != null) shared_scan_buffers_storage.deinit(allocator);

    const use_dedicated_raw_stage = cfg.raw_group_mode == .staged_final;
    var raw_scan_queues: []RawQueue = &.{};
    var raw_group_queues: []GroupQueue = &.{};
    var stage_builders: []StageBucketBuilder = &.{};
    var raw_queues_moved_to_shared = false;
    errdefer if (!raw_queues_moved_to_shared) {
        if (raw_scan_queues.len > 0) allocator.free(raw_scan_queues);
        if (raw_group_queues.len > 0) allocator.free(raw_group_queues);
        if (stage_builders.len > 0) allocator.free(stage_builders);
    };
    if (use_dedicated_raw_stage) {
        raw_scan_queues = try allocator.alloc(RawQueue, n_workers);
        raw_group_queues = try allocator.alloc(GroupQueue, bucket_count);
        for (raw_scan_queues) |*queue| queue.* = .{};
        for (raw_group_queues) |*queue| queue.* = .{};
        stage_builders = try allocator.alloc(StageBucketBuilder, bucket_count);
        for (stage_builders) |*builder| builder.* = .{};
    }

    var shared = PipeShared{
        .resources = resources,
        .allocator = allocator,
        .io = table.io,
        .buckets = buckets,
        .bucket_count = bucket_count,
        .raw_scan_queues = raw_scan_queues,
        .input_rows_per_bucket = total / bucket_count + @intFromBool(total % bucket_count != 0),
        .raw_group_queues = raw_group_queues,
        .stage_builders = stage_builders,
        .group_rows_layout = group_rows_layout,
        .recycle_byte_limit = if (group_rows_layout.has_str_payload) stringRecycleLimit(resources) else null,
        .generic_filter_required = cfg.filter_expr != null,
        .count_sketch = cfg.count_sketch,
        .scan_threads = n_workers,
        .total_scan_rgs = claim_total_rgs,
        .scan_tiles = try RowGroupTiles.init(allocator, claim_total_rgs, surviving_mask, n_workers, scan_tile_rgs * scan_coalesce_tiles),
        .local_reserve_per_bucket = local_reserve_per_bucket,
        .route_block_rows = route_block_rows,
        .direct_final_local = direct_final_local,
        .local_parts = parts,
        .shared_scan_buffers = shared_scan_buffers_ptr,
        .defer_fold = deferBucketFolds(group_rows_layout, n_workers, use_dedicated_raw_stage, expectedGroupsPerBucket(defer_groups, bucket_count), total, resources),
    };
    defer shared.scan_tiles.deinit(allocator);
    raw_queues_moved_to_shared = true;
    var serial_fold_scratch: GroupScratch = .{};
    defer serial_fold_scratch.deinit(allocator);
    if (serial_fold) shared.serial_fold_scratch = &serial_fold_scratch;
    var heavy_teardown_scheduled = false;
    defer if (!heavy_teardown_scheduled) deinitRawQueues(&shared);
    if (use_dedicated_raw_stage) {
        for (shared.raw_scan_queues) |*queue| try queue.chunks.ensureTotalCapacity(allocator, 8);
        for (shared.raw_group_queues) |*queue| try queue.chunks.ensureTotalCapacity(allocator, 8);
    }
    var i: usize = 0;
    const worker_setup_t0 = if ((PROFILING and cfg.trace_timing)) platform.nowTicks() else 0;
    while (i < n_workers) : (i += 1) {
        if (!using_workspace) {
            parts[i] = try WorkerParts.init(allocator, bucket_count, worker_local_reserve_per_bucket);
            parts[i].worker_index = i;
            parts[i].raw_scan_lane = i;
            built_parts += 1;
        }
        parts[i].shared_buffers = shared_scan_buffers_ptr;
        parts[i].shared_bank_index = if (shared_scan_bank_count > 0) i % shared_scan_bank_count else 0;
        if (use_flat_scan_partitions) try parts[i].enableFlatScanPartitions(allocator, bucket_count, flat_reserve_per_worker);
        if (use_raw_group) {
            try parts[i].ensureWideBucketScratch(allocator, bucket_count);
            try parts[i].raw_active_rows.ensureTotalCapacity(allocator, cfg.group_rows_layout, raw_chunk_rows);
            if (cfg.group_rows_layout.has_weight) try parts[i].prepareFoldIndex(allocator, raw_chunk_rows);
            const stage_scratch_rows = if (cfg.shared_stage_builders) raw_chunk_rows * @max(@as(usize, 1), @min(raw_batch_chunks, MAX_RAW_BATCH_CHUNKS)) else raw_chunk_rows;
            try parts[i].flat_raw_rows.ensureTotalCapacity(allocator, cfg.group_rows_layout, stage_scratch_rows);
            try parts[i].flat_bucket_ids.ensureTotalCapacity(allocator, stage_scratch_rows);
        }
        scans[i] = try Scan.allocWithProjectionLoc(scan_allocator, table, null, scan_columns, cfg.group_rows_layout.has_rowref, snap);
        // Weighted integer-key programs consume RLE run headers directly (the
        // run-native emitter); hashed-key layouts pack from digests and string
        // extremes fold per row, so neither has a consumer for the sidecar.
        scans[i].emit_runs = cfg.group_rows_layout.has_weight and !cfg.group_rows_layout.has_rowref and !cfg.group_rows_layout.has_str_payload;
        {
            var sq = thindb.exec.makeQuery(scan_allocator, scans[i]);
            drives[i] = if (cfg.derived.len == 0) sq else sq.computeWithRegistry(cfg.derived, cfg.udf_registry) catch |e| {
                sq.deinit();
                return e;
            };
        }
        built_scans += 1;
        if (cfg.scan_filter) {
            if (cfg.filter_expr) |expr| {
                _ = try applyScanFilterExpr(scans[i], expr);
            }
        }
        _ = try cfg.offered_prunes.apply(scans[i]);
        for (cfg.hash_key_columns) |hc| _ = scans[i].setHashKeyColumn(hc);
        for (cfg.byte_length_columns) |lc| _ = scans[i].setByteLengthColumn(lc);
        if (cfg.key_bound) |kb| _ = try scans[i].setKeyBound(kb);
        scans[i].setRange(0, 0, 0, 0, false);
    }
    const worker_setup_ticks = if ((PROFILING and cfg.trace_timing)) platform.nowTicks() - worker_setup_t0 else 0;

    var grid_distinct: []GridDictEval = &.{};
    defer {
        for (grid_distinct) |*gd| gd.deinit(allocator);
        if (grid_distinct.len > 0) allocator.free(grid_distinct);
    }
    if (n_workers > 0) distinct: {
        const scan_schema = scans[0].outputSchema();
        const column = (try dictEvalColumn(allocator, cfg, scan_schema)) orelse break :distinct;
        if (cfg.derived.len != 0) {
            for (drives) |d| {
                const compute = thindb.exec.queryAs(thindb.exec.Compute, d) orelse break :distinct;
                if (compute.chain != null) break :distinct;
            }
        }
        // All or none: a marked scan's string rows arrive as placeholders
        // that only the collapse below reads past.
        for (scans[0..n_workers]) |sc| {
            if (sc.blockDictColumnIndex(scan_schema[column].name) == null) break :distinct;
        }
        const loc_column = for (scan_schema, 0..) |col, j| {
            if (types_mod.columnNameEql(col.name, rowloc.col_name)) break j;
        } else null;
        // The scan's other columns are aggregate inputs. Each staged column
        // over one folds it per dictionary code by its aggregate; the
        // engine stages a copy of an input for each aggregate over it.
        const layout = cfg.group_rows_layout;
        for (scan_schema, 0..) |col, j| {
            if (j == column or (loc_column != null and j == loc_column.?)) continue;
            for (layout.columns) |lc| {
                if (types_mod.columnNameEql(lc.source_name, col.name)) break;
            } else break :distinct;
        }
        var partials_buf: [MAX_GROUP_PAYLOAD_COLUMNS]DictPartial = undefined;
        var n_partials: usize = 0;
        for (layout.columns, 0..) |lc, c| {
            const j = for (scan_schema, 0..) |col, j| {
                if (j == column or (loc_column != null and j == loc_column.?)) continue;
                if (types_mod.columnNameEql(col.name, lc.source_name)) break j;
            } else continue;
            const op: WeightFoldOp = for (layout.aggregates) |agg| {
                if (agg.is_string or agg.input_column_index == null or agg.input_column_index.? != c) continue;
                break switch (agg.op) {
                    .sum, .avg => .sum,
                    .min => .min,
                    .max => .max,
                    else => break :distinct,
                };
            } else break :distinct;
            partials_buf[n_partials] = .{ .column = j, .layout_column = c, .op = op };
            n_partials += 1;
        }
        for (scans[0..n_workers]) |sc| try sc.setBlockDictColumn(sc.blockDictColumnIndex(scan_schema[column].name).?);
        grid_distinct = try allocator.alloc(GridDictEval, n_workers);
        for (grid_distinct, drives) |*gd, d| gd.* = .{
            .compute = if (cfg.derived.len != 0) thindb.exec.queryAs(thindb.exec.Compute, d).? else null,
            .column = column,
            .loc_column = loc_column,
        };
        if (n_partials > 0) {
            for (grid_distinct) |*gd| {
                gd.partials = try allocator.dupe(DictPartial, partials_buf[0..n_partials]);
                gd.partial_views = try allocator.alloc(?ColumnView, layout.columns.len);
                @memset(gd.partial_views, null);
            }
        }
    }

    var group_ticks = try allocator.alloc(i64, n_workers);
    defer allocator.free(group_ticks);
    @memset(group_ticks, 0);
    var idle_ticks = try allocator.alloc(i64, n_workers);
    defer allocator.free(idle_ticks);
    @memset(idle_ticks, 0);
    var top_ticks = try allocator.alloc(i64, n_workers);
    defer allocator.free(top_ticks);
    @memset(top_ticks, 0);
    var chunks = try allocator.alloc(u64, n_workers);
    defer allocator.free(chunks);
    @memset(chunks, 0);
    var worker_tops = try allocator.alloc(TopSet, n_workers);
    defer allocator.free(worker_tops);
    for (worker_tops) |*t| t.* = .{};
    defer for (worker_tops) |*t| t.deinit(allocator);
    // The all-groups emit never reads the top sets; size them to zero so
    // collectOwnedTop degenerates to a no-op walk.
    const top_k_eff: usize = if (cfg.result_all_groups) 0 else cfg.top_k;
    for (worker_tops) |*t| t.* = try TopSet.init(allocator, top_k_eff);

    const setup_ticks = platform.nowTicks() - function_t0;
    if ((PROFILING and cfg.trace_timing)) {
        std.debug.print("[harness-core-setup] query={s} total={d:.1}ms snapshot_stats={d:.1}ms pipe_buckets={d:.1}ms worker_parts_scans={d:.1}ms other={d:.1}ms\n", .{
            "generic",
            platform.ticksToMs(setup_ticks, freq),
            platform.ticksToMs(snapshot_setup_ticks, freq),
            platform.ticksToMs(bucket_setup_ticks, freq),
            platform.ticksToMs(worker_setup_ticks, freq),
            platform.ticksToMs(setup_ticks - snapshot_setup_ticks - bucket_setup_ticks - worker_setup_ticks, freq),
        });
    }
    const total_t0 = platform.nowTicks();
    var threads = try allocator.alloc(std.Thread, n_workers);
    defer allocator.free(threads);
    var errs = try allocator.alloc(?anyerror, n_workers);
    defer allocator.free(errs);
    @memset(errs, null);
    const sketch_scratch = try allocator.alloc(SketchScratch, n_workers);
    defer allocator.free(sketch_scratch);
    for (sketch_scratch) |*sc| sc.* = .{};
    defer for (sketch_scratch) |*sc| sc.deinit(scan_allocator);

    i = 0;
    while (i < n_workers) : (i += 1) {
        const job = SiloGridJob{
            .scan = scans[i],
            .drive = &drives[i],
            .distinct = if (grid_distinct.len > 0) &grid_distinct[i] else null,
            .local = &parts[i],
            .shared = &shared,
            .seg_start = seg_start,
            .segment_count = snap.segment_count,
            .worker_index = i,
            .worker_count = n_workers,
            .chunk_rows = chunk_rows,
            .group_lease_buckets = cfg.group_lease_buckets,
            .group_lease_rows = cfg.group_lease_rows,
            .raw_group_mode = cfg.raw_group_mode,
            .raw_chunk_rows = raw_chunk_rows,
            .raw_group_chunk_rows = raw_group_chunk_rows,
            .raw_batch_chunks = raw_batch_chunks,
            .filter_fused = scans[i].fusedActive(),
            .profile = !cfg.no_profile,
            .cpu = cpus[i % cpus.len],
            .group_ticks = &group_ticks[i],
            .idle_ticks = &idle_ticks[i],
            .top_ticks = &top_ticks[i],
            .chunks = &chunks[i],
            .top = &worker_tops[i],
            .err = &errs[i],
            .sketch_scratch = &sketch_scratch[i],
        };
        threads[i] = try std.Thread.spawn(.{}, siloGridWorker, .{job});
    }
    i = 0;
    while (i < n_workers) : (i += 1) threads[i].join();
    const worker_wall_ticks = platform.nowTicks() - total_t0;
    for (errs) |maybe_err| if (maybe_err) |e| return e;

    var scan_cpu_ticks: i64 = 0;
    var partition_cpu_ticks: i64 = 0;
    var publish_cpu_ticks: i64 = 0;
    var group_cpu_ticks: i64 = 0;
    var idle_cpu_ticks: i64 = 0;
    var top_cpu_ticks: i64 = 0;
    var total_chunks: u64 = 0;
    var scan_reset_ticks: i64 = 0;
    var total_scan_tiles: u64 = 0;
    var total_scan_quanta: u64 = 0;
    var total_scan_batches: u64 = 0;
    var total_segments_opened: u64 = 0;
    var sched_decision_ticks: i64 = 0;
    var sched_scan_claim_ticks: i64 = 0;
    var sched_group_pick_ticks: i64 = 0;
    var sched_group_lock_ticks: i64 = 0;
    var raw_queue_lock_ticks: i64 = 0;
    var raw_scan_queue_lock_ticks: i64 = 0;
    var raw_group_queue_lock_ticks: i64 = 0;
    var raw_recycle_lock_ticks: i64 = 0;
    var raw_agg_lock_ticks: i64 = 0;
    var raw_stage_builder_lock_ticks: i64 = 0;
    var raw_stage_ticks: i64 = 0;
    var raw_stage_pop_ticks: i64 = 0;
    var raw_stage_slice_ticks: i64 = 0;
    var raw_stage_publish_ticks: i64 = 0;
    var raw_stage_recycle_ticks: i64 = 0;
    var raw_stage_input_chunks: u64 = 0;
    var sched_loops: u64 = 0;
    var sched_scan_jobs: u64 = 0;
    var sched_stage_jobs: u64 = 0;
    var sched_group_jobs: u64 = 0;
    var sched_group_misses: u64 = 0;
    var sched_idle_loops: u64 = 0;
    var fused_scan_count: usize = 0;
    var scanned_rows: u64 = 0;
    var filtered_rows: u64 = 0;
    for (parts) |p| {
        scan_cpu_ticks += p.scan_ticks;
        scan_reset_ticks += p.scan_reset_ticks;
        partition_cpu_ticks += p.partition_ticks;
        publish_cpu_ticks += p.publish_ticks;
        total_scan_tiles += p.scan_tiles;
        total_scan_quanta += p.scan_quanta;
        total_scan_batches += p.scan_batches;
        sched_decision_ticks += p.sched_decision_ticks;
        sched_scan_claim_ticks += p.sched_scan_claim_ticks;
        sched_group_pick_ticks += p.sched_group_pick_ticks;
        sched_group_lock_ticks += p.sched_group_lock_ticks;
        raw_queue_lock_ticks += p.raw_queue_lock_ticks;
        raw_scan_queue_lock_ticks += p.raw_scan_queue_lock_ticks;
        raw_group_queue_lock_ticks += p.raw_group_queue_lock_ticks;
        raw_recycle_lock_ticks += p.raw_recycle_lock_ticks;
        raw_agg_lock_ticks += p.raw_agg_lock_ticks;
        raw_stage_builder_lock_ticks += p.raw_stage_builder_lock_ticks;
        raw_stage_ticks += p.raw_stage_ticks;
        raw_stage_pop_ticks += p.raw_stage_pop_ticks;
        raw_stage_slice_ticks += p.raw_stage_slice_ticks;
        raw_stage_publish_ticks += p.raw_stage_publish_ticks;
        raw_stage_recycle_ticks += p.raw_stage_recycle_ticks;
        raw_stage_input_chunks += p.raw_stage_input_chunks;
        sched_loops += p.sched_loops;
        sched_scan_jobs += p.sched_scan_jobs;
        sched_stage_jobs += p.sched_stage_jobs;
        sched_group_jobs += p.sched_group_jobs;
        sched_group_misses += p.sched_group_misses;
        sched_idle_loops += p.sched_idle_loops;
        scanned_rows += p.scanned_count;
        filtered_rows += p.row_count;
    }
    for (scans) |s| {
        total_segments_opened += s.segments_opened;
        if (s.fusedActive()) fused_scan_count += 1;
    }
    for (group_ticks) |ticks| group_cpu_ticks += ticks;
    for (idle_ticks) |ticks| idle_cpu_ticks += ticks;
    for (top_ticks) |ticks| top_cpu_ticks += ticks;
    for (chunks) |chunk_count| total_chunks += chunk_count;

    const final_t0 = platform.nowTicks();
    var top = try TopSet.init(allocator, top_k_eff);
    defer top.deinit(allocator);
    var group_count: u64 = 0;
    var grouped_rows: u64 = 0;
    for (buckets) |*bucket| {
        group_count += bucket.states.len;
        grouped_rows += bucket.row_count;
    }
    for (worker_tops) |worker_top| {
        for (worker_top.items[0..worker_top.len], 0..) |candidate, candidate_index| {
            if (candidate_index % 1024 == 0) try thindb.exec.memory.checkCancelled(allocator);
            top.consider(candidate);
        }
    }
    try top.sortBestFirst(allocator);
    const final_merge_ticks = platform.nowTicks() - final_t0;
    const total_ticks = platform.nowTicks() - total_t0;
    const function_ticks = platform.nowTicks() - function_t0;
    if (cfg.result_out) |out| {
        const has_str_out = hasStringResults(cfg.group_rows_layout);
        const has_udf_out = cfg.group_rows_layout.has_udf;
        var udf_scratch = UdfEmitScratch{ .allocator = allocator };
        defer udf_scratch.deinit();
        if (cfg.result_all_groups) {
            const cap = cfg.result_all_groups_cap;
            const filter = cfg.result_emit_filter;
            const numeric = !has_str_out and !has_udf_out and cap == 0;
            // String MIN/MAX results rank from the bucket's bytes; UDAF and
            // GROUP_CONCAT results finalize per group, so they emit every group.
            const rankable = !has_udf_out and !cfg.group_rows_layout.has_concat and cap == 0;
            const rank: ?EmitRank = if (cfg.result_rank) |r| (if (rankable and rankPays(r.keep, group_count, n_workers)) r else null) else null;
            const numeric_uncapped = numeric and filter == null and rank == null;
            if (rank) |r| {
                if (has_str_out) try emitRankedGroups(allocator, buckets, out, n_workers, filter, r, true) else try emitRankedGroups(allocator, buckets, out, n_workers, filter, r, false);
            }
            if (numeric_uncapped) try emitAllGroupsNumeric(allocator, buckets, out, n_workers);
            emit_all: for (buckets) |*bucket| {
                if (numeric_uncapped or rank != null) break;
                if (cap != 0 and out.items.len >= cap) break;
                if (has_str_out or has_udf_out) {
                    const has_concat_out = cfg.group_rows_layout.has_concat;
                    var gid: usize = 0;
                    while (gid < bucket.states.len) : (gid += 1) {
                        if (gid % 1024 == 0) try thindb.exec.memory.checkCancelled(allocator);
                        if (cap != 0 and out.items.len >= cap) break :emit_all;
                        var row = if (has_str_out) topRowFromStateStr(bucket.states.ref(gid), bucket.str_states.items[gid]) else topRowFromState(bucket.states.ref(gid));
                        if (has_concat_out) try finalizeConcatIntoRow(&row, cfg.group_rows_layout.aggregates, &bucket.concat_states.items[gid], bucket.str_arena.allocator());
                        if (has_udf_out) try finalizeUdfIntoRow(&udf_scratch, &row, bucket, gid, cfg.group_rows_layout.aggregates);
                        if (filter) |f| if (!f.pass(f.ctx, row)) continue;
                        if (has_str_out) try ownTopRowStr(&row, allocator);
                        try out.append(allocator, row);
                    }
                } else {
                    var gid: usize = 0;
                    while (gid < bucket.states.len) : (gid += 1) {
                        if (gid % 1024 == 0) try thindb.exec.memory.checkCancelled(allocator);
                        if (cap != 0 and out.items.len >= cap) break :emit_all;
                        const row = topRowFromState(bucket.states.ref(gid));
                        if (filter) |f| if (!f.pass(f.ctx, row)) continue;
                        try out.append(allocator, row);
                    }
                }
            }
        } else {
            // The merged top-N rows borrow each survivor's bucket str bytes (and
            // already carry finalized UDAF slots from collectOwnedTop); re-dup
            // the str bytes into the result allocator so they survive teardown.
            for (0..top.len) |rank_index| {
                var row = top.rowAt(rank_index);
                if (has_str_out) try ownTopRowStr(&row, allocator);
                try out.append(allocator, row);
            }
        }
    }
    // Run each UDAF's `destroy` while the layout (the agg specs) is still live —
    // bucket teardown may be deferred to another thread, where the stack-built
    // spec slice would dangle.
    if (cfg.group_rows_layout.has_udf) {
        for (buckets) |*bucket| bucket.destroyUdfStates(cfg.group_rows_layout.aggregates);
    }
    if ((PROFILING and cfg.trace_timing)) {
        var hash_bytes: usize = 0;
        var state_capacity: usize = 0;
        var state_bytes: usize = 0;
        var allocation_ticks: i64 = 0;
        var table_growths: usize = 0;
        for (buckets) |bucket| {
            hash_bytes += bucket.table.slotBytes();
            state_capacity += bucket.states.cap;
            state_bytes += bucket.states.cap * bucket.states.stride;
            allocation_ticks += bucket.allocation_profile.ticks;
            table_growths += bucket.allocation_profile.table_growths;
        }
        std.debug.print("[group-allocation] groups={d} hash_bytes={d} state_capacity={d} state_bytes={d} table_growths={d} allocation_worker_sum={d:.3}ms\n", .{ group_count, hash_bytes, state_capacity, state_bytes, table_growths, platform.ticksToMs(allocation_ticks, freq) });
        std.debug.print(
            "[harness-core-timing] query={s} full={d:.1}ms setup_before_workers={d:.1}ms worker_and_final={d:.1}ms final_merge={d:.3}ms result_rows={d}\n",
            .{
                "generic",
                platform.ticksToMs(function_ticks, freq),
                platform.ticksToMs(setup_ticks, freq),
                platform.ticksToMs(total_ticks, freq),
                platform.ticksToMs(final_merge_ticks, freq),
                top.len,
            },
        );
    }
    pre_return_ticks = platform.nowTicks();
    if (cfg.defer_heavy_teardown) heavy_teardown_scheduled = scheduleHeavyTeardown(&shared);

    if (PROFILING and !cfg.quiet and cfg.no_profile) {
        std.debug.print(
            "[clientip-result] query={s} DOP={d} mode=silo-grid workers={d} chunk_rows={d} scan_tile_rgs={d} scan_coalesce_tiles={d} group_lease_buckets={d} group_lease_rows={d} scheduler=group_rows_vs_scan_buffer_rows scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} total={d:.1}ms worker_wall={d:.1}ms final_merge={d:.3}ms no_profile=true\n",
            .{
                "generic",
                n_workers,
                n_workers,
                chunk_rows,
                scan_tile_rgs,
                scan_coalesce_tiles,
                cfg.group_lease_buckets,
                cfg.group_lease_rows,
                scanned_rows,
                total,
                filtered_rows,
                grouped_rows,
                group_count,
                platform.ticksToMs(total_ticks, freq),
                platform.ticksToMs(worker_wall_ticks, freq),
                platform.ticksToMs(final_merge_ticks, freq),
            },
        );
    } else if (PROFILING and !cfg.quiet) {
        std.debug.print(
            "[clientip-prof] query={s} DOP={d} scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} total={d:.1}ms mode=silo-grid workers={d} worker_wall={d:.1}ms final_merge={d:.3}ms group_lease_buckets={d} group_lease_rows={d}\n",
            .{
                "generic",
                n_workers,
                scanned_rows,
                total,
                filtered_rows,
                grouped_rows,
                group_count,
                platform.ticksToMs(total_ticks, freq),
                n_workers,
                platform.ticksToMs(worker_wall_ticks, freq),
                platform.ticksToMs(final_merge_ticks, freq),
                cfg.group_lease_buckets,
                cfg.group_lease_rows,
            },
        );
        std.debug.print(
            "[clientip-silo-grid-stages] query={s} scan_decode_cpu={d:.1}ms scan_reset_cpu={d:.3}ms route_partition_cpu={d:.1}ms raw_stage_cpu={d:.1}ms publish_queue_cpu={d:.1}ms aggregate_cpu={d:.1}ms idle_cpu={d:.1}ms local_topn_cpu={d:.1}ms final_merge_wall={d:.3}ms chunks={d} rows_per_chunk={d:.1} scan_ranges={d} scan_quanta={d} scan_batches={d} segments_opened={d} fused_scans={d}/{d} scan_tile_rgs={d} scan_coalesce_tiles={d} route_block_rows={d} group_lease_buckets={d} group_lease_rows={d} final_pending_group_rows={d} final_stage_queue_rows={d} final_scan_buffered_rows={d} active_scan_jobs={d} active_stage_jobs={d} active_group_jobs={d}\n",
            .{
                "generic",
                platform.ticksToMs(scan_cpu_ticks, freq),
                platform.ticksToMs(scan_reset_ticks, freq),
                platform.ticksToMs(partition_cpu_ticks, freq),
                platform.ticksToMs(raw_stage_ticks, freq),
                platform.ticksToMs(publish_cpu_ticks, freq),
                platform.ticksToMs(group_cpu_ticks, freq),
                platform.ticksToMs(idle_cpu_ticks, freq),
                platform.ticksToMs(top_cpu_ticks, freq),
                platform.ticksToMs(final_merge_ticks, freq),
                total_chunks,
                if (total_chunks == 0) 0.0 else @as(f64, @floatFromInt(grouped_rows)) / @as(f64, @floatFromInt(total_chunks)),
                total_scan_tiles,
                total_scan_quanta,
                total_scan_batches,
                total_segments_opened,
                fused_scan_count,
                n_workers,
                scan_tile_rgs,
                scan_coalesce_tiles,
                route_block_rows,
                cfg.group_lease_buckets,
                cfg.group_lease_rows,
                shared.pending_group_rows.load(.acquire),
                shared.stage_outstanding_rows.load(.acquire),
                shared.scan_buffered_rows.load(.acquire),
                shared.active_scan_jobs.load(.acquire),
                shared.active_stage_jobs.load(.acquire),
                shared.active_group_jobs.load(.acquire),
            },
        );
        const sched_cpu_ticks = sched_decision_ticks + sched_scan_claim_ticks + sched_group_pick_ticks + sched_group_lock_ticks;
        std.debug.print(
            "[clientip-silo-grid-scheduler] query={s} scheduler_cpu={d:.3}ms decision={d:.3}ms scan_claim={d:.3}ms group_pick={d:.3}ms group_lock={d:.3}ms loops={d} scan_jobs={d} stage_jobs={d} group_jobs={d} group_misses={d} idle_loops={d}\n",
            .{
                "generic",
                platform.ticksToMs(sched_cpu_ticks, freq),
                platform.ticksToMs(sched_decision_ticks, freq),
                platform.ticksToMs(sched_scan_claim_ticks, freq),
                platform.ticksToMs(sched_group_pick_ticks, freq),
                platform.ticksToMs(sched_group_lock_ticks, freq),
                sched_loops,
                sched_scan_jobs,
                sched_stage_jobs,
                sched_group_jobs,
                sched_group_misses,
                sched_idle_loops,
            },
        );
        std.debug.print(
            "[clientip-raw-stage-breakdown] query={s} pop_cpu={d:.3}ms cluster_cpu={d:.3}ms slice_copy_alloc_cpu={d:.3}ms publish_group_queue_cpu={d:.3}ms recycle_input_cpu={d:.3}ms total_stage_measured={d:.3}ms input_chunks={d} avg_input_chunks_per_stage_job={d:.2}\n",
            .{
                "generic",
                platform.ticksToMs(raw_stage_pop_ticks, freq),
                platform.ticksToMs(raw_stage_ticks, freq),
                platform.ticksToMs(raw_stage_slice_ticks, freq),
                platform.ticksToMs(raw_stage_publish_ticks, freq),
                platform.ticksToMs(raw_stage_recycle_ticks, freq),
                platform.ticksToMs(raw_stage_pop_ticks + raw_stage_ticks + raw_stage_slice_ticks + raw_stage_publish_ticks + raw_stage_recycle_ticks, freq),
                raw_stage_input_chunks,
                if (sched_stage_jobs == 0) 0.0 else @as(f64, @floatFromInt(raw_stage_input_chunks)) / @as(f64, @floatFromInt(sched_stage_jobs)),
            },
        );
        std.debug.print(
            "[clientip-raw-contention] query={s} raw_queue_lock={d:.3}ms raw_scan_queue_lock={d:.3}ms raw_group_queue_lock={d:.3}ms raw_recycle_lock={d:.3}ms stage_builder_lock={d:.3}ms central_group_lock={d:.3}ms total_raw_lock={d:.3}ms\n",
            .{
                "generic",
                platform.ticksToMs(raw_queue_lock_ticks, freq),
                platform.ticksToMs(raw_scan_queue_lock_ticks, freq),
                platform.ticksToMs(raw_group_queue_lock_ticks, freq),
                platform.ticksToMs(raw_recycle_lock_ticks, freq),
                platform.ticksToMs(raw_stage_builder_lock_ticks, freq),
                platform.ticksToMs(raw_agg_lock_ticks, freq),
                platform.ticksToMs(raw_queue_lock_ticks + raw_scan_queue_lock_ticks + raw_group_queue_lock_ticks + raw_recycle_lock_ticks + raw_stage_builder_lock_ticks + raw_agg_lock_ticks, freq),
            },
        );
        std.debug.print("[clientip-silo-grid-workers] query={s} aggregate_cpu_by_worker_ms=", .{"generic"});
        for (group_ticks, 0..) |ticks, worker_idx| {
            if (worker_idx != 0) std.debug.print(",", .{});
            std.debug.print("{d:.1}", .{platform.ticksToMs(ticks, freq)});
        }
        std.debug.print(" chunks_by_worker=", .{});
        for (chunks, 0..) |chunk_count, worker_idx| {
            if (worker_idx != 0) std.debug.print(",", .{});
            std.debug.print("{d}", .{chunk_count});
        }
        std.debug.print("\n", .{});
    }
}
