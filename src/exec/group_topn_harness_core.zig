//! Simplified real-data pipeline for:
//!   SELECT ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth)
//!   FROM hits
//!   GROUP BY ClientIP
//!   ORDER BY c DESC
//!   LIMIT 10;
//!
//! This intentionally bypasses the SQL planner and production GROUP BY
//! operators. It still uses the real table scan/decode path, then runs a small
//! fixed pipeline: range scan -> hash/radix partition -> bucket-owned GROUP BY
//! -> top-N.

const std = @import("std");
const win = std.os.windows;
const exec_mod = @import("exec.zig");
const api_mod = @import("../api/api.zig");
const storage_mod = @import("../storage/storage.zig");
const types_mod = @import("../types.zig");
const rowloc = @import("rowloc.zig");
const build_options = @import("build_options");

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
const TOP_K: usize = 10;

/// Selective-query right-sizing: one grid worker per this many surviving
/// (post-zone-map) row groups, so a filter that touches a handful of row
/// groups doesn't pay full-DOP worker setup (scans, staging, bucket scratch).
const RGS_PER_GRID_WORKER: usize = 2;
const PREFETCH_DIST_BUCKET: usize = 32;
// Look-ahead distance for the grouped COUNT(DISTINCT) fold: the second pass
// prefetches the membership-set slot this many rows ahead of the insert, so the
// (cache-miss-bound) probe latency of independent rows overlaps.
const PREFETCH_DIST_DISTINCT: usize = 24;
const PIPE_CHUNK_ROWS: usize = 8192;
const GRID_CHUNK_ROWS: usize = 1024;
const GRID_SCAN_TILE_RGS: usize = 16;
const GRID_SCAN_COALESCE_TILES: usize = 1;
const GRID_SCAN_YIELD_CHUNKS: usize = 16384;
const DEFAULT_ROUTE_BLOCK_ROWS: usize = 2048;
const MAX_ROUTE_BLOCK_ROWS: usize = 2048;
const AUTO_ROUTE_BLOCK_ROWS: usize = 0;
const MAX_GROUP_LEASE_BUCKETS: usize = 64;
const MAX_WORKSPACE_PROFILE_WORKERS: usize = 128;
const MAX_RAW_BATCH_CHUNKS: usize = 64;
const MAX_GENERIC_GROUP_KEYS: usize = 8;

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

fn nowTicks() i64 {
    var c: win.LARGE_INTEGER = 0;
    _ = win.ntdll.RtlQueryPerformanceCounter(&c);
    return c;
}

fn perfFreq() i64 {
    var f: win.LARGE_INTEGER = 0;
    _ = win.ntdll.RtlQueryPerformanceFrequency(&f);
    return if (f == 0) 1 else f;
}

fn ticksToMs(ticks: i64, freq: i64) f64 {
    return @as(f64, @floatFromInt(ticks)) * 1000.0 / @as(f64, @floatFromInt(freq));
}

extern "kernel32" fn GetCurrentThread() callconv(.winapi) win.HANDLE;
extern "kernel32" fn SetThreadAffinityMask(hThread: win.HANDLE, mask: usize) callconv(.winapi) usize;
extern "kernel32" fn GetLogicalProcessorInformation(buf: ?[*]LogicalProcInfo, len: *u32) callconv(.winapi) win.BOOL;

const RelationProcessorCore: u32 = 0;
const LogicalProcInfo = extern struct {
    processor_mask: usize,
    relationship: u32,
    _pad: u32 = 0,
    _union: [16]u8 = [_]u8{0} ** 16,
};

pub const CpuLayout = struct {
    order: []usize,
    physical_count: usize,

    pub fn deinit(self: CpuLayout, allocator: Allocator) void {
        allocator.free(self.order);
    }
};

pub fn cpuLayout(allocator: Allocator) !CpuLayout {
    var buf: [256]LogicalProcInfo = undefined;
    var len: u32 = @intCast(@sizeOf(LogicalProcInfo) * buf.len);
    if (!GetLogicalProcessorInformation(&buf, &len).toBool()) return error.QueryFailed;
    const n = len / @sizeOf(LogicalProcInfo);

    var primaries: std.ArrayListUnmanaged(usize) = .empty;
    var siblings: std.ArrayListUnmanaged(usize) = .empty;
    for (buf[0..n]) |info| {
        if (info.relationship != RelationProcessorCore) continue;
        var mask = info.processor_mask;
        var first = true;
        while (mask != 0) {
            const bit: usize = @ctz(mask);
            mask &= mask - 1;
            if (first) {
                try primaries.append(allocator, bit);
                first = false;
            } else {
                try siblings.append(allocator, bit);
            }
        }
    }
    const physical_count = primaries.items.len;
    try primaries.appendSlice(allocator, siblings.items);
    siblings.deinit(allocator);
    return .{
        .order = try primaries.toOwnedSlice(allocator),
        .physical_count = physical_count,
    };
}

pub fn pinToCpu(cpu: usize) void {
    if (cpu < @bitSizeOf(usize)) {
        _ = SetThreadAffinityMask(GetCurrentThread(), @as(usize, 1) << @intCast(cpu));
    }
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
        return bytes;
    }

    inline fn keyOffset(_: RawRows) usize {
        return 0;
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
};

pub const GroupKeyColumnSpec = struct {
    name: []const u8,
    typ: thindb.types.Type,
    offset_bits: u8,
    width_bits: u8,
};

pub const GroupAggregateOp = enum {
    count_star,
    count_col,
    sum,
    avg,
    min,
    max,
    count_distinct,
};

pub const GroupAggregateSpec = struct {
    op: GroupAggregateOp,
    input_column_index: ?u16 = null,
    state_index: u16,
    // String MIN/MAX: reads `layout.str_columns[str_input_index]` and keeps its
    // running extreme in the parallel string-state slot `str_state_index` (not
    // the numeric `slots[]`). `is_string` is false for every numeric aggregate.
    is_string: bool = false,
    str_input_index: u16 = 0,
    str_state_index: u16 = 0,
    // COUNT(DISTINCT col): reads the carried integer payload `input_column_index`
    // and tracks membership in the per-bucket combined set `distinct_state_index`
    // (keyed by (gid,value)); `state_index` is the running distinct-count slot,
    // bumped only on a never-before-seen (gid,value). `is_distinct` is false for
    // every other aggregate.
    is_distinct: bool = false,
    distinct_state_index: u16 = 0,
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
const MAX_GROUP_STR_COLUMNS: usize = 2;
const MAX_GROUP_STR_SLOTS: usize = 2;

// COUNT(DISTINCT) support. A query may carry up to this many distinct aggregates;
// each gets one combined per-bucket membership set. 0/unused for the common case.
const MAX_GROUP_DISTINCT_SLOTS: usize = 8;

// One COUNT(DISTINCT) field's combined membership set. Keys are pack(gid, value)
// (gid in the high 64 bits, the integer value's bit pattern in the low 64) —
// exact, so no value is stored beyond the u128 key and the distinct count falls
// out as the number of never-before-seen inserts. The grouped silo folds it under
// the bucket agg_lock (no synchronization needed); the global aggregate reuses it
// as a per-lane partial (gid 0) and unions lanes via `mergeInto` at the single-
// threaded merge layer.
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

    // Fold another lane's partial set into this one (global aggregate merge).
    // Both sets share the same tier (configured from the same plan). Reserve the
    // union upper bound once so no grow fires per inserted key.
    pub fn mergeInto(self: *DistinctSet, allocator: Allocator, other: *const DistinctSet) !void {
        switch (self.store) {
            .u32 => |*s| {
                const o = &other.store.u32;
                if (o.has_sentinel) s.has_sentinel = true;
                try s.ensureFor(allocator, o.count());
                for (o.slots) |k| if (k != group_table.DistinctU32Set.SENTINEL) s.insert(k);
            },
            .u64 => |*s| {
                const o = &other.store.u64;
                if (o.has_sentinel) s.has_sentinel = true;
                try s.ensureFor(allocator, o.count());
                for (o.slots) |k| if (k != group_table.DistinctU64Set.SENTINEL) s.insert(k);
            },
            .u96 => |*s| {
                const o = &other.store.u96;
                if (o.has_sentinel) s.has_sentinel = true;
                try s.ensureFor(allocator, o.count());
                for (o.slots) |k| if (!k.eql(group_table.DistinctU96Set.SENTINEL)) s.insert(k);
            },
            .u128 => |*s| {
                const o = &other.store.u128;
                if (o.has_sentinel) s.has_sentinel = true;
                try s.ensureFor(allocator, o.count());
                for (o.slots) |k| if (k != group_table.DistinctU128Set.SENTINEL) s.insert(k);
            },
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

    fn append(self: *StrStore, allocator: Allocator, str_cols: usize, row: usize, col: usize, b: []const u8) !void {
        const off: u32 = @intCast(self.bytes.items.len);
        try self.bytes.appendSlice(allocator, b);
        self.refs[row * str_cols + col] = .{ .off = off, .len = @intCast(b.len) };
    }

    fn get(self: StrStore, str_cols: usize, row: usize, col: usize) []const u8 {
        const ref = self.refs[row * str_cols + col];
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
    // When the group key is a hash (string / >128-bit keys), each staged row
    // also carries the source row's packed __rowloc (see rowloc.zig) in a
    // dedicated i64 region after the payload columns, captured into the group
    // State on first insert so the actual key values can be late-materialized
    // at emit. Dormant (region absent) for the integer-packed-key queries.
    has_rowref: bool = false,
    // COUNT-only programs (no payload/str columns, no distinct): each staged
    // row carries a u32 weight, and the scan emitter collapses a run of
    // adjacent-equal keys into ONE weighted row — route/stage/lane traffic
    // shrinks by the run factor. The lane folds `count += weight`.
    has_weight: bool = false,

    const DEFAULT_GROUP_COLUMNS = [_]GroupColumnSpec{
        .{ .physical_type = .i16, .source = .is_refresh },
        .{ .physical_type = .i16, .source = .resolution_width },
    };
};

fn sameRowsLayout(a: GroupRowsLayout, b: GroupRowsLayout) bool {
    if (a.key_width != b.key_width or a.has_rowref != b.has_rowref or a.has_weight != b.has_weight or a.has_str_payload != b.has_str_payload or a.distinct_slot_count != b.distinct_slot_count or a.key_columns.len != b.key_columns.len or a.columns.len != b.columns.len or a.str_columns.len != b.str_columns.len or a.aggregates.len != b.aggregates.len) return false;
    var key_i: usize = 0;
    while (key_i < a.key_columns.len) : (key_i += 1) {
        if (!thindb.types.columnNameEql(a.key_columns[key_i].name, b.key_columns[key_i].name) or
            !std.meta.eql(a.key_columns[key_i].typ, b.key_columns[key_i].typ) or
            a.key_columns[key_i].offset_bits != b.key_columns[key_i].offset_bits or
            a.key_columns[key_i].width_bits != b.key_columns[key_i].width_bits)
        {
            return false;
        }
    }
    var i: usize = 0;
    while (i < a.columns.len) : (i += 1) {
        if (a.columns[i].physical_type != b.columns[i].physical_type or
            a.columns[i].source != b.columns[i].source or
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
            a.aggregates[i].str_input_index != b.aggregates[i].str_input_index or
            a.aggregates[i].str_state_index != b.aggregates[i].str_state_index)
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
        return bytes;
    }

    inline fn keyOffset(_: GroupRows) usize {
        return 0;
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
        // Variable-length string payload: copy each scattered row's string
        // values from the source RawRows store into this bucket's store. Owned
        // bytes, so the bucket survives the source chunk's recycle.
        if (self.layout.has_str_payload) {
            const k = self.layout.str_columns.len;
            var i: usize = 0;
            while (i < count) : (i += 1) {
                var c: usize = 0;
                while (c < k) : (c += 1) {
                    try self.str.append(allocator, k, old_len + i, c, rows.str.get(k, start + i, c));
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
    raw_active_rows: RawRows = .{},
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
        if (self.flat_counts.len > 0) allocator.free(self.flat_counts);
        if (self.flat_offsets.len > 0) allocator.free(self.flat_offsets);
        if (self.flat_next.len > 0) allocator.free(self.flat_next);
        self.dirty_buckets.deinit(allocator);
        if (self.dirty_marks.len > 0) allocator.free(self.dirty_marks);
        if (self.route_counts.len > 0) allocator.free(self.route_counts);
        if (self.route_offsets.len > 0) allocator.free(self.route_offsets);
        self.route_touched.deinit(allocator);
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

// Per-bucket group accumulator store. Replaces ArrayListUnmanaged(State): the
// per-group slot count is a runtime value (`n_slots`), so the record stride is
// computed once per query from the aggregate program rather than baked into a
// fixed array. gid is a dense 0..len index (the GroupTable maps key→gid).
const StateSlab = struct {
    bytes: []align(16) u8 = &.{},
    len: usize = 0,
    cap: usize = 0,
    n_slots: usize = 0,
    stride: usize = 0,
    // Group-count reservation requested before the layout (hence stride) is
    // known; applied on the first prepare().
    reserve_hint: usize = 0,

    fn strideFor(n_slots: usize) usize {
        return std.mem.alignForward(usize, STATE_HEAD_BYTES + n_slots * @sizeOf(i64), 16);
    }

    fn reserve(self: *StateSlab, records: usize) void {
        if (records > self.reserve_hint) self.reserve_hint = records;
    }

    // Bind the slab to the query's slot count before folding. First call (or a
    // layout change) sets the stride and honours any pending reservation.
    fn prepare(self: *StateSlab, allocator: Allocator, n_slots: usize) !void {
        if (self.stride != 0 and self.n_slots == n_slots) return;
        if (self.bytes.len > 0) allocator.free(self.bytes);
        self.bytes = &.{};
        self.len = 0;
        self.cap = 0;
        self.n_slots = n_slots;
        self.stride = strideFor(n_slots);
        if (self.reserve_hint > 0) try self.growTo(allocator, self.reserve_hint);
    }

    fn growTo(self: *StateSlab, allocator: Allocator, records: usize) !void {
        if (records <= self.cap) return;
        const next = try allocator.alignedAlloc(u8, .@"16", records * self.stride);
        if (self.len > 0) @memcpy(next[0 .. self.len * self.stride], self.bytes[0 .. self.len * self.stride]);
        if (self.bytes.len > 0) allocator.free(self.bytes);
        self.bytes = next;
        self.cap = records;
    }

    fn ensureUnusedCapacity(self: *StateSlab, allocator: Allocator, additional: usize) !void {
        const need = self.len + additional;
        if (need <= self.cap) return;
        var new_cap = if (self.cap == 0) @max(self.reserve_hint, 8) else self.cap * 2;
        while (new_cap < need) new_cap *= 2;
        try self.growTo(allocator, new_cap);
    }

    inline fn head(self: StateSlab, gid: usize) *StateHead {
        return @ptrCast(@alignCast(self.bytes.ptr + gid * self.stride));
    }

    inline fn slotsOf(self: StateSlab, gid: usize) []i64 {
        const p = self.bytes.ptr + gid * self.stride + STATE_HEAD_BYTES;
        return @as([*]i64, @ptrCast(@alignCast(p)))[0..self.n_slots];
    }

    inline fn ref(self: StateSlab, gid: usize) StateRef {
        return .{ .head = self.head(gid), .slots = self.slotsOf(gid) };
    }

    // Append a fresh zeroed record (key/count/rowref/slots all 0) and return its
    // gid. Caller has ensured capacity.
    inline fn pushAssumeCapacity(self: *StateSlab) u32 {
        const gid: u32 = @intCast(self.len);
        const base = self.bytes.ptr + self.len * self.stride;
        @memset(base[0..self.stride], 0);
        self.len += 1;
        return gid;
    }

    fn clearRetainingCapacity(self: *StateSlab) void {
        self.len = 0;
    }

    fn deinit(self: *StateSlab, allocator: Allocator) void {
        if (self.bytes.len > 0) allocator.free(self.bytes);
        self.* = .{};
    }
};

// Per-group running MIN/MAX bytes for a string aggregate. Kept in a side array
// parallel to a bucket's `states` (NOT in `State`, which would bloat the
// million-group numeric hot path). `bytes` is owned by the bucket allocator
// until emit, where survivors are re-dup'd into the result allocator.
const StrAcc = struct { bytes: []const u8 = &.{}, present: bool = false };
const StrAccRow = [MAX_GROUP_STR_SLOTS]StrAcc;

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

const PipeBucket = struct {
    queue_lock: std.atomic.Mutex = .unlocked,
    agg_lock: std.atomic.Mutex = .unlocked,
    chunks: std.ArrayListUnmanaged(PipeChunk) = .empty,
    queued_rows: u64 = 0,
    table: GroupTable,
    states: StateSlab = .{},
    // Parallel to `states` (gid-indexed); populated only for string MIN/MAX
    // queries. Empty for the numeric common case.
    str_states: std.ArrayListUnmanaged(StrAccRow) = .empty,
    // Bump arena for the running-MIN/MAX bytes. A per-improvement `dupe` from a
    // shared allocator scatters each group's current value across the heap, so
    // reading it back for the next row's compare is a main-memory miss — the
    // dominant cost of string MIN over millions of groups. Allocating from a
    // contiguous arena keeps those values L2/L3-resident and drops the
    // per-improvement `free` (superseded values are arena garbage, reclaimed
    // wholesale at reset/teardown).
    str_arena: std.heap.ArenaAllocator,
    // One combined membership set per COUNT(DISTINCT) field. Indexed by the
    // aggregate's distinct_state_index; only the first `layout.distinct_slot_count`
    // are touched. Each holds (gid,value) keys for this bucket's groups.
    distinct_sets: [MAX_GROUP_DISTINCT_SLOTS]DistinctSet = [_]DistinctSet{.{}} ** MAX_GROUP_DISTINCT_SLOTS,
    row_count: u64 = 0,

    fn init(allocator: Allocator, expected_groups: usize) !PipeBucket {
        return .{
            .table = try GroupTable.init(allocator, expected_groups),
            .str_arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    fn freeStrBytes(self: *PipeBucket) void {
        _ = self.str_arena.reset(.free_all);
    }

    fn deinit(self: *PipeBucket, allocator: Allocator) void {
        self.str_arena.deinit();
        self.str_states.deinit(allocator);
        for (&self.distinct_sets) |*d| d.deinit(allocator);
        self.chunks.deinit(allocator);
        self.table.deinit(allocator);
        self.states.deinit(allocator);
        self.* = undefined;
    }
};

const PipeShared = struct {
    allocator: Allocator,
    buckets: []PipeBucket,
    bucket_count: usize,
    raw_queue_lock: std.atomic.Mutex = .unlocked,
    raw_chunks: std.ArrayListUnmanaged(RawChunk) = .empty,
    raw_recycle_lock: std.atomic.Mutex = .unlocked,
    raw_recycled_rows: std.ArrayListUnmanaged(RawRows) = .empty,
    group_recycled_rows: std.ArrayListUnmanaged(GroupRows) = .empty,
    raw_merge_lock: std.atomic.Mutex = .unlocked,
    raw_scan_queues: []RawQueue = &.{},
    raw_group_queues: []GroupQueue = &.{},
    stage_builders: []StageBucketBuilder = &.{},
    group_rows_layout: GroupRowsLayout = .{},
    generic_filter_required: bool = false,
    stage_outstanding_chunks: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    stage_outstanding_rows: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    stage_builder_rows: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    active_stage_jobs: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    scan_threads: usize,
    scans_done: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    outstanding_chunks: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    outstanding_rows: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    scan_buffered_rows: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    active_scan_jobs: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    active_group_jobs: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    // Set by any worker that fails. Peers poll it at the top of their schedule
    // loop and break immediately, so one worker's error tears the run down
    // cleanly (the orchestrator joins, sees the error, and returns it) instead
    // of the survivors spinning forever waiting on a `scans_done` that the
    // failed worker never signalled.
    aborted: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    next_scan_rg: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    next_final_local_bucket: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    total_scan_rgs: usize = 0,
    local_reserve_per_bucket: usize = 0,
    route_block_rows: usize = AUTO_ROUTE_BLOCK_ROWS,
    route_block_rows_set: bool = false,
    direct_final_local: bool = false,
    local_parts: []WorkerParts = &.{},
    shared_scan_buffers: ?*SharedScanBuffers = null,
};

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
        const freq = perfFreq();
        std.debug.print(
            "[workspace-setup-detail] query={s} total={d:.1}ms parts_alloc={d:.3}ms parts_zero={d:.3}ms buckets_alloc={d:.3}ms flags_alloc={d:.3}ms thread_alloc={d:.3}ms job_alloc={d:.3}ms spawn={d:.3}ms join={d:.1}ms assign={d:.3}ms workers={} buckets={} local_reserve={} expected_groups_per_bucket={}\n",
            .{
                query,
                ticksToMs(self.setup_total_ticks, freq),
                ticksToMs(self.setup_parts_alloc_ticks, freq),
                ticksToMs(self.setup_parts_zero_ticks, freq),
                ticksToMs(self.setup_buckets_alloc_ticks, freq),
                ticksToMs(self.setup_flags_alloc_ticks, freq),
                ticksToMs(self.setup_thread_alloc_ticks, freq),
                ticksToMs(self.setup_job_alloc_ticks, freq),
                ticksToMs(self.setup_spawn_ticks, freq),
                ticksToMs(self.setup_join_ticks, freq),
                ticksToMs(self.setup_assign_ticks, freq),
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
                ticksToMs(profileSum(self.setup_worker_part_ticks[0..profileWorkerCount(self.workers)]), freq),
                ticksToMs(profileMax(self.setup_worker_part_ticks[0..profileWorkerCount(self.workers)]), freq),
                ticksToMs(profileSum(self.setup_worker_bucket_ticks[0..profileWorkerCount(self.workers)]), freq),
                ticksToMs(profileMax(self.setup_worker_bucket_ticks[0..profileWorkerCount(self.workers)]), freq),
                ticksToMs(profileSum(self.setup_worker_total_ticks[0..profileWorkerCount(self.workers)]), freq),
                ticksToMs(profileMax(self.setup_worker_total_ticks[0..profileWorkerCount(self.workers)]), freq),
            },
        );
    }

    pub fn printTeardown(self: *const WorkspaceProfile, query: []const u8, bucket_count: usize) void {
        const freq = perfFreq();
        std.debug.print(
            "[workspace-teardown-detail] query={s} total={d:.1}ms thread_alloc={d:.3}ms job_alloc={d:.3}ms spawn={d:.3}ms join={d:.1}ms outer_free={d:.3}ms workers={} buckets={}\n",
            .{
                query,
                ticksToMs(self.teardown_total_ticks, freq),
                ticksToMs(self.teardown_thread_alloc_ticks, freq),
                ticksToMs(self.teardown_job_alloc_ticks, freq),
                ticksToMs(self.teardown_spawn_ticks, freq),
                ticksToMs(self.teardown_join_ticks, freq),
                ticksToMs(self.teardown_outer_free_ticks, freq),
                self.workers,
                bucket_count,
            },
        );
        std.debug.print(
            "[workspace-teardown-workers] query={s} part_sum={d:.1}ms part_max={d:.1}ms bucket_sum={d:.1}ms bucket_max={d:.1}ms total_sum={d:.1}ms total_max={d:.1}ms\n",
            .{
                query,
                ticksToMs(profileSum(self.teardown_worker_part_ticks[0..profileWorkerCount(self.workers)]), freq),
                ticksToMs(profileMax(self.teardown_worker_part_ticks[0..profileWorkerCount(self.workers)]), freq),
                ticksToMs(profileSum(self.teardown_worker_bucket_ticks[0..profileWorkerCount(self.workers)]), freq),
                ticksToMs(profileMax(self.teardown_worker_bucket_ticks[0..profileWorkerCount(self.workers)]), freq),
                ticksToMs(profileSum(self.teardown_worker_total_ticks[0..profileWorkerCount(self.workers)]), freq),
                ticksToMs(profileMax(self.teardown_worker_total_ticks[0..profileWorkerCount(self.workers)]), freq),
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
        const total_t0 = if (profile != null) nowTicks() else 0;
        const workers = @max(@as(usize, 1), @min(n_workers, @max(cpus.len, 1)));
        if (profile) |p| p.workers = profileWorkerCount(workers);
        if (workers == 1 or (self.parts.len + self.buckets.len) < 128) {
            self.deinit(allocator);
            if (profile) |p| p.teardown_total_ticks = nowTicks() - total_t0;
            return;
        }
        const threads_alloc_t0 = if (profile != null) nowTicks() else 0;
        const threads = allocator.alloc(std.Thread, workers) catch {
            self.deinit(allocator);
            if (profile) |p| p.teardown_total_ticks = nowTicks() - total_t0;
            return;
        };
        if (profile) |p| p.teardown_thread_alloc_ticks = nowTicks() - threads_alloc_t0;
        defer allocator.free(threads);
        const jobs_alloc_t0 = if (profile != null) nowTicks() else 0;
        const jobs = allocator.alloc(WorkspaceDeinitJob, workers) catch {
            self.deinit(allocator);
            if (profile) |p| p.teardown_total_ticks = nowTicks() - total_t0;
            return;
        };
        if (profile) |p| p.teardown_job_alloc_ticks = nowTicks() - jobs_alloc_t0;
        defer allocator.free(jobs);

        var i: usize = 0;
        const spawn_t0 = if (profile != null) nowTicks() else 0;
        while (i < workers) : (i += 1) {
            jobs[i] = .{
                .workspace = self,
                .allocator = allocator,
                .worker_index = i,
                .worker_count = workers,
                .cpu = if (cpus.len == 0) null else cpus[i % cpus.len],
                .profile = profile,
            };
            threads[i] = std.Thread.spawn(.{}, workspaceDeinitWorker, .{&jobs[i]}) catch unreachable;
        }
        if (profile) |p| p.teardown_spawn_ticks = nowTicks() - spawn_t0;
        const join_t0 = if (profile != null) nowTicks() else 0;
        for (threads) |thread| thread.join();
        if (profile) |p| p.teardown_join_ticks = nowTicks() - join_t0;
        const free_t0 = if (profile != null) nowTicks() else 0;
        if (self.parts.len > 0) allocator.free(self.parts);
        if (self.buckets.len > 0) allocator.free(self.buckets);
        if (profile) |p| {
            p.teardown_outer_free_ticks = nowTicks() - free_t0;
            p.teardown_total_ticks = nowTicks() - total_t0;
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
        const total_t0 = if (profile != null) nowTicks() else 0;
        const parts_alloc_t0 = if (profile != null) nowTicks() else 0;
        const parts = try allocator.alloc(WorkerParts, n_workers);
        if (profile) |p| p.setup_parts_alloc_ticks = nowTicks() - parts_alloc_t0;
        errdefer allocator.free(parts);
        const parts_zero_t0 = if (profile != null) nowTicks() else 0;
        for (parts) |*p| p.* = .{};
        if (profile) |p| p.setup_parts_zero_ticks = nowTicks() - parts_zero_t0;
        errdefer {
            for (parts) |*p| p.deinit(allocator);
        }

        const buckets_alloc_t0 = if (profile != null) nowTicks() else 0;
        const buckets = try allocator.alloc(PipeBucket, bucket_count);
        if (profile) |p| p.setup_buckets_alloc_ticks = nowTicks() - buckets_alloc_t0;
        errdefer allocator.free(buckets);
        const flags_alloc_t0 = if (profile != null) nowTicks() else 0;
        const bucket_inited = try allocator.alloc(bool, bucket_count);
        defer allocator.free(bucket_inited);
        @memset(bucket_inited, false);
        if (profile) |p| p.setup_flags_alloc_ticks = nowTicks() - flags_alloc_t0;
        errdefer {
            for (buckets, bucket_inited) |*bucket, inited| {
                if (inited) bucket.deinit(allocator);
            }
        }

        try initWorkspaceFreshParallel(allocator, parts, buckets, bucket_inited, bucket_count, local_reserve_per_bucket, expected_groups_per_bucket, n_workers, cpus, profile);

        const assign_t0 = if (profile != null) nowTicks() else 0;
        self.* = .{
            .parts = parts,
            .buckets = buckets,
            .n_workers = n_workers,
            .bucket_count = bucket_count,
            .local_reserve_per_bucket = local_reserve_per_bucket,
            .expected_groups_per_bucket = expected_groups_per_bucket,
        };
        if (profile) |p| {
            p.setup_assign_ticks = nowTicks() - assign_t0;
            p.setup_total_ticks = nowTicks() - total_t0;
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
                .cpu = if (cpus.len == 0) null else cpus[i % cpus.len],
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
    cpu: ?usize,
};

const WorkspaceDeinitJob = struct {
    workspace: *SiloGridWorkspace,
    allocator: Allocator,
    worker_index: usize,
    worker_count: usize,
    cpu: ?usize,
    profile: ?*WorkspaceProfile = null,
};

fn workspaceDeinitWorker(job: *WorkspaceDeinitJob) void {
    const total_t0 = if (job.profile != null) nowTicks() else 0;
    if (job.cpu) |cpu| pinToCpu(cpu);
    const part_t0 = if (job.profile != null) nowTicks() else 0;
    var part_i = job.worker_index;
    while (part_i < job.workspace.parts.len) : (part_i += job.worker_count) {
        job.workspace.parts[part_i].deinit(job.allocator);
    }
    const part_ticks = if (job.profile != null) nowTicks() - part_t0 else 0;

    const bucket_t0 = if (job.profile != null) nowTicks() else 0;
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
            profile.teardown_worker_bucket_ticks[job.worker_index] = nowTicks() - bucket_t0;
            profile.teardown_worker_total_ticks[job.worker_index] = nowTicks() - total_t0;
        }
    }
}

fn workspaceResetWorker(job: *WorkspaceResetJob) void {
    if (job.cpu) |cpu| pinToCpu(cpu);
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
    cpu: ?usize,
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
        const total_t0 = if (profile != null) nowTicks() else 0;
        const part_t0 = if (profile != null) nowTicks() else 0;
        for (parts, 0..) |*p, i| {
            p.* = try WorkerParts.init(allocator, bucket_count, local_reserve_per_bucket);
            p.worker_index = i;
        }
        const part_ticks = if (profile != null) nowTicks() - part_t0 else 0;
        const bucket_t0 = if (profile != null) nowTicks() else 0;
        for (buckets, 0..) |*bucket, i| {
            bucket.* = try PipeBucket.init(allocator, expected_groups_per_bucket);
            bucket_inited[i] = true;
            bucket.states.reserve(expected_groups_per_bucket);
            try bucket.chunks.ensureTotalCapacity(allocator, 8);
        }
        if (profile) |p| {
            p.setup_worker_part_ticks[0] = part_ticks;
            p.setup_worker_bucket_ticks[0] = nowTicks() - bucket_t0;
            p.setup_worker_total_ticks[0] = nowTicks() - total_t0;
        }
        return;
    }

    const threads_alloc_t0 = if (profile != null) nowTicks() else 0;
    const threads = try allocator.alloc(std.Thread, workers);
    if (profile) |p| p.setup_thread_alloc_ticks = nowTicks() - threads_alloc_t0;
    defer allocator.free(threads);
    const jobs_alloc_t0 = if (profile != null) nowTicks() else 0;
    var jobs = try allocator.alloc(WorkspaceFreshInitJob, workers);
    if (profile) |p| p.setup_job_alloc_ticks = nowTicks() - jobs_alloc_t0;
    defer allocator.free(jobs);

    var i: usize = 0;
    const spawn_t0 = if (profile != null) nowTicks() else 0;
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
            .cpu = if (cpus.len == 0) null else cpus[i % cpus.len],
            .profile = profile,
        };
        threads[i] = try std.Thread.spawn(.{}, workspaceFreshInitWorker, .{&jobs[i]});
    }
    if (profile) |p| p.setup_spawn_ticks = nowTicks() - spawn_t0;
    const join_t0 = if (profile != null) nowTicks() else 0;
    for (threads) |thread| thread.join();
    if (profile) |p| p.setup_join_ticks = nowTicks() - join_t0;
    for (jobs) |job| if (job.err) |err| return err;
}

fn workspaceFreshInitWorker(job: *WorkspaceFreshInitJob) void {
    const total_t0 = if (job.profile != null) nowTicks() else 0;
    if (job.cpu) |cpu| pinToCpu(cpu);

    const part_t0 = if (job.profile != null) nowTicks() else 0;
    if (job.worker_index < job.parts.len) {
        job.parts[job.worker_index] = WorkerParts.init(job.allocator, job.bucket_count, job.local_reserve_per_bucket) catch |err| {
            job.err = err;
            return;
        };
        job.parts[job.worker_index].worker_index = job.worker_index;
    }
    const part_ticks = if (job.profile != null) nowTicks() - part_t0 else 0;

    const bucket_t0 = if (job.profile != null) nowTicks() else 0;
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
        job.buckets[b].states.reserve(job.expected_groups_per_bucket);
        job.buckets[b].chunks.ensureTotalCapacity(job.allocator, 8) catch |err| {
            job.err = err;
            return;
        };
    }
    if (job.profile) |profile| {
        if (job.worker_index < MAX_WORKSPACE_PROFILE_WORKERS) {
            profile.setup_worker_part_ticks[job.worker_index] = part_ticks;
            profile.setup_worker_bucket_ticks[job.worker_index] = nowTicks() - bucket_t0;
            profile.setup_worker_total_ticks[job.worker_index] = nowTicks() - total_t0;
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
    bucket.queue_lock = .unlocked;
    bucket.agg_lock = .unlocked;
    bucket.table.clearRetainingCapacity();
    bucket.states.clearRetainingCapacity();
    bucket.freeStrBytes();
    bucket.str_states.clearRetainingCapacity();
    for (&bucket.distinct_sets) |*d| d.clear(allocator);
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

// Move the queue/recycle state onto a detached thread and zero it in `shared`
// so the unwind path has nothing left to free. Any failure falls back to
// freeing synchronously via the regular defer (returns false).
fn scheduleHeavyTeardown(shared: *PipeShared) bool {
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
    shared.raw_chunks = .empty;
    shared.raw_scan_queues = &.{};
    shared.raw_group_queues = &.{};
    shared.stage_builders = &.{};
    shared.raw_recycled_rows = .empty;
    shared.group_recycled_rows = .empty;
    return true;
}

fn lockSpin(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn acquireRawRows(shared: *PipeShared, reserve_rows: usize, lock_ticks: ?*i64) !RawRows {
    const lock_t0 = if (lock_ticks != null) nowTicks() else 0;
    lockSpin(&shared.raw_recycle_lock);
    if (lock_ticks) |ticks| ticks.* += nowTicks() - lock_t0;
    if (shared.raw_recycled_rows.items.len > 0) {
        const idx = shared.raw_recycled_rows.items.len - 1;
        var rows = shared.raw_recycled_rows.items[idx];
        shared.raw_recycled_rows.items.len = idx;
        shared.raw_recycle_lock.unlock();
        rows.clearRetainingCapacity();
        if (rows.capacity() < reserve_rows or !sameRowsLayout(rows.layout, shared.group_rows_layout)) {
            try rows.ensureTotalCapacity(shared.allocator, shared.group_rows_layout, reserve_rows);
        }
        return rows;
    }
    shared.raw_recycle_lock.unlock();
    var rows = RawRows{ .layout = shared.group_rows_layout };
    if (reserve_rows > 0) try rows.ensureTotalCapacity(shared.allocator, shared.group_rows_layout, reserve_rows);
    return rows;
}

fn recycleRawRows(shared: *PipeShared, rows_raw: RawRows, reserve_rows: usize, lock_ticks: ?*i64) !void {
    var rows = rows_raw;
    rows.clearRetainingCapacity();
    if (rows.capacity() < reserve_rows or !sameRowsLayout(rows.layout, shared.group_rows_layout)) {
        try rows.ensureTotalCapacity(shared.allocator, shared.group_rows_layout, reserve_rows);
    }
    const lock_t0 = if (lock_ticks != null) nowTicks() else 0;
    lockSpin(&shared.raw_recycle_lock);
    if (lock_ticks) |ticks| ticks.* += nowTicks() - lock_t0;
    defer shared.raw_recycle_lock.unlock();
    try shared.raw_recycled_rows.append(shared.allocator, rows);
}

fn acquireGroupRows(shared: *PipeShared, reserve_rows: usize, lock_ticks: ?*i64) !GroupRows {
    const lock_t0 = if (lock_ticks != null) nowTicks() else 0;
    lockSpin(&shared.raw_recycle_lock);
    if (lock_ticks) |ticks| ticks.* += nowTicks() - lock_t0;
    if (shared.group_recycled_rows.items.len > 0) {
        const idx = shared.group_recycled_rows.items.len - 1;
        var rows = shared.group_recycled_rows.items[idx];
        shared.group_recycled_rows.items.len = idx;
        shared.raw_recycle_lock.unlock();
        rows.clearRetainingCapacity();
        if (rows.capacity() < reserve_rows) try rows.ensureTotalCapacity(shared.allocator, shared.group_rows_layout, reserve_rows);
        return rows;
    }
    shared.raw_recycle_lock.unlock();
    var rows = GroupRows{ .layout = shared.group_rows_layout };
    if (reserve_rows > 0) try rows.ensureTotalCapacity(shared.allocator, shared.group_rows_layout, reserve_rows);
    return rows;
}

fn recycleGroupRows(shared: *PipeShared, rows_group: GroupRows, reserve_rows: usize, lock_ticks: ?*i64) !void {
    var rows = rows_group;
    rows.clearRetainingCapacity();
    if (rows.capacity() < reserve_rows) try rows.ensureTotalCapacity(shared.allocator, shared.group_rows_layout, reserve_rows);
    const lock_t0 = if (lock_ticks != null) nowTicks() else 0;
    lockSpin(&shared.raw_recycle_lock);
    if (lock_ticks) |ticks| ticks.* += nowTicks() - lock_t0;
    defer shared.raw_recycle_lock.unlock();
    try shared.group_recycled_rows.append(shared.allocator, rows);
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
    _ = shared.outstanding_rows.fetchAdd(row_count, .release);
    const lock_t0 = if (queue_lock_ticks != null) nowTicks() else 0;
    lockSpin(&shared.raw_queue_lock);
    if (queue_lock_ticks) |ticks| ticks.* += nowTicks() - lock_t0;
    errdefer {
        _ = shared.outstanding_chunks.fetchSub(1, .release);
        _ = shared.outstanding_rows.fetchSub(row_count, .release);
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
    const lock_t0 = if (queue_lock_ticks != null) nowTicks() else 0;
    lockSpin(&queue.lock);
    if (queue_lock_ticks) |ticks| ticks.* += nowTicks() - lock_t0;
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
    const lock_t0 = if (queue_lock_ticks != null) nowTicks() else 0;
    lockSpin(&queue.lock);
    if (queue_lock_ticks) |ticks| ticks.* += nowTicks() - lock_t0;
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
}

fn popRawChunkBatchFromQueue(queue: *RawQueue, out: *[MAX_RAW_BATCH_CHUNKS]RawChunk, max_chunks_raw: usize, queue_lock_ticks: ?*i64) usize {
    const max_chunks = @max(@as(usize, 1), @min(max_chunks_raw, MAX_RAW_BATCH_CHUNKS));
    const lock_t0 = if (queue_lock_ticks != null) nowTicks() else 0;
    lockSpin(&queue.lock);
    if (queue_lock_ticks) |ticks| ticks.* += nowTicks() - lock_t0;
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
    const lock_t0 = if (queue_lock_ticks != null) nowTicks() else 0;
    lockSpin(&queue.lock);
    if (queue_lock_ticks) |ticks| ticks.* += nowTicks() - lock_t0;
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
};

// Optional per-group predicate applied during the all-groups emit (a HAVING
// filter for a capped unordered LIMIT). Evaluated on the numeric TopRow before
// the string key is dup'd, so failing groups cost nothing; only passing groups
// count toward the emit cap. The caller (the pipeline) owns the context.
pub const EmitFilter = struct {
    ctx: ?*anyopaque,
    pass: *const fn (?*anyopaque, TopRow) bool,
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
    for (str_row, 0..) |acc, i| row.str[i] = acc.bytes;
    return row;
}

// Re-dup a TopRow's (borrowed) string results into `allocator` so they outlive
// the bucket teardown. No-op for numeric rows (all slices empty).
fn ownTopRowStr(row: *TopRow, allocator: Allocator) !void {
    for (&row.str) |*s| {
        if (s.len > 0) s.* = try allocator.dupe(u8, s.*);
    }
}

fn better(a: TopRow, b: TopRow) bool {
    if (a.count != b.count) return a.count > b.count;
    return a.key < b.key;
}

fn worse(a: TopRow, b: TopRow) bool {
    if (a.count != b.count) return a.count < b.count;
    return a.key > b.key;
}

// Bounded top-K set, sized at runtime from the query's LIMIT+OFFSET. Kept as
// a binary heap rooted at the WORST retained row, so the common case — a
// candidate that doesn't make the cut — is a single comparison against the
// root, and a replacement costs O(log k) instead of the O(k) worst-rescan the
// old fixed-10 array paid.
const TopSet = struct {
    items: []TopRow = &.{},
    len: usize = 0,

    fn init(allocator: Allocator, k: usize) !TopSet {
        return .{ .items = try allocator.alloc(TopRow, k) };
    }

    fn deinit(self: *TopSet, allocator: Allocator) void {
        allocator.free(self.items);
        self.* = .{};
    }

    fn consider(self: *TopSet, cand: TopRow) void {
        if (self.len < self.items.len) {
            var i = self.len;
            self.items[i] = cand;
            self.len += 1;
            while (i > 0) {
                const parent = (i - 1) / 2;
                if (!worse(self.items[i], self.items[parent])) break;
                std.mem.swap(TopRow, &self.items[i], &self.items[parent]);
                i = parent;
            }
            return;
        }
        if (self.items.len == 0 or !better(cand, self.items[0])) return;
        self.items[0] = cand;
        var i: usize = 0;
        while (true) {
            const left = 2 * i + 1;
            const right = left + 1;
            var w = i;
            if (left < self.len and worse(self.items[left], self.items[w])) w = left;
            if (right < self.len and worse(self.items[right], self.items[w])) w = right;
            if (w == i) break;
            std.mem.swap(TopRow, &self.items[i], &self.items[w]);
            i = w;
        }
    }
};

fn topLess(_: void, a: TopRow, b: TopRow) bool {
    return better(a, b);
}

fn totalRows(table: *thindb.api.Table) u64 {
    var n: u64 = table.memtable.row_count;
    for (table.manifest.segments.items) |s| n += s.row_count;
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
        const raw = try readGenericKeyBits(key_views[i], part.typ, row);
        key |= raw << @intCast(part.offset_bits);
    }
    return key;
}

inline fn updateKeyHash(h: *std.hash.Wyhash, view: thindb.storage.ColumnView, typ: thindb.types.Type, row: usize) void {
    switch (view.data) {
        .string, .varchar, .char => |sv| h.update(sv.rowBytes(row)),
        else => {
            const bits = readGenericKeyBits(view, typ, row) catch 0;
            h.update(std.mem.asBytes(&bits));
        },
    }
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
fn hashGenericKeyFromViews(layout: GroupRowsLayout, key_views: []const thindb.storage.ColumnView, key_digests: []const ?[]const u128, row: usize) u128 {
    var lo = std.hash.Wyhash.init(0x9E3779B97F4A7C15);
    var hi = std.hash.Wyhash.init(0xD1B54A32D192ED03);
    for (layout.key_columns, 0..) |part, i| {
        switch (key_views[i].data) {
            .string, .varchar, .char => |sv| {
                const d: u128 = if (key_digests[i]) |ds| ds[row] else thindb.exec.stringKeyDigest(sv.rowBytes(row));
                lo.update(std.mem.asBytes(&d));
                hi.update(std.mem.asBytes(&d));
            },
            else => {
                updateKeyHash(&lo, key_views[i], part.typ, row);
                updateKeyHash(&hi, key_views[i], part.typ, row);
            },
        }
    }
    return (@as(u128, hi.final()) << 64) | @as(u128, lo.final());
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

fn appendBatchRawChunksGeneric(parts: *WorkerParts, shared: *PipeShared, batch: thindb.Batch, raw_chunk_rows: usize, profile: bool, skip_filter_check: bool) !void {
    if (shared.generic_filter_required and !skip_filter_check) return error.UnsupportedOperatorForType;
    const layout = shared.group_rows_layout;
    if (layout.key_columns.len == 0 or layout.key_columns.len > MAX_GENERIC_GROUP_KEYS) return error.UnsupportedOperatorForType;
    if (parts.raw_active_rows.capacity() == 0) parts.raw_active_rows = try acquireRawRows(shared, raw_chunk_rows, &parts.raw_recycle_lock_ticks);

    var key_views_buf: [MAX_GENERIC_GROUP_KEYS]thindb.storage.ColumnView = undefined;
    var key_digests_buf: [MAX_GENERIC_GROUP_KEYS]?[]const u128 = undefined;
    for (layout.key_columns, 0..) |part, i| {
        const ci = batch.columnIndex(part.name) orelse return error.ColumnNotFound;
        key_views_buf[i] = batch.values[ci];
        key_digests_buf[i] = if (batch.hashed) |hs| hs[ci] else null;
    }
    if (layout.columns.len > MAX_GROUP_PAYLOAD_COLUMNS) return error.UnsupportedOperatorForType;

    // Per-column native source bytes + element size — one typed (copyElem) path
    // covering every int/float agg-input width. Weighted layouts bind fold
    // specs instead: their staged columns are pre-widened partials (i64/f64),
    // so the source view is read typed and reduced, never byte-copied.
    var payload_bytes: [MAX_GROUP_PAYLOAD_COLUMNS][]const u8 = undefined;
    var payload_elt: [MAX_GROUP_PAYLOAD_COLUMNS]usize = undefined;
    var weight_specs_buf: [MAX_GROUP_PAYLOAD_COLUMNS]WeightFoldSpec = undefined;
    var weight_specs: []const WeightFoldSpec = &.{};
    if (layout.has_weight) {
        try buildWeightFoldSpecs(layout, batch, weight_specs_buf[0..layout.columns.len]);
        weight_specs = weight_specs_buf[0..layout.columns.len];
    } else {
        for (layout.columns, 0..) |column, i| {
            if (column.source_name.len == 0) return error.UnsupportedOperatorForType;
            const view = batch.columnView(column.source_name) orelse return error.ColumnNotFound;
            payload_bytes[i] = aggInputViewBytes(view, column.physical_type) orelse return error.TypeMismatch;
            payload_elt[i] = groupColumnSize(column.physical_type);
        }
    }

    // String agg-input columns (string MIN/MAX). Bound once; carried into the
    // active chunk's StrStore per row. Empty for numeric-only queries, so every
    // append path's `if (str_views.len != 0)` guard is a free no-op there.
    var str_views_buf: [MAX_GROUP_STR_COLUMNS]StringView = undefined;
    var str_views: []const StringView = &.{};
    if (layout.has_str_payload) {
        if (layout.str_columns.len > MAX_GROUP_STR_COLUMNS) return error.UnsupportedOperatorForType;
        for (layout.str_columns, 0..) |sc, i| {
            const view = batch.columnView(sc.source_name) orelse return error.ColumnNotFound;
            str_views_buf[i] = switch (view.data) {
                .varchar, .string, .char => |sv| sv,
                else => return error.TypeMismatch,
            };
        }
        str_views = str_views_buf[0..layout.str_columns.len];
    }

    // Key-shape fast variants (no hashed key). Payload is type-generic; the
    // string lane (if any) rides along via appendStrPayload inside each kernel.
    // Run-native weighted emission: when every key column arrives with an RLE
    // run sidecar, merge the columns' runs into composite spans and emit one
    // staged row per span — key packing and the collapse compare happen per
    // SPAN, never per row. Any missing sidecar falls through to the per-row
    // weighted paths below (memtable, tombstoned/filtered batches, non-RLE
    // blocks), which produce identical keys and partials.
    if (layout.has_weight and !layout.has_rowref) runs_blk: {
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

    if (!layout.has_rowref) {
        if (try appendBatchRawChunksGenericFast(parts, shared, batch, layout, key_views_buf[0..layout.key_columns.len], payload_bytes[0..layout.columns.len], payload_elt[0..layout.columns.len], weight_specs, str_views, raw_chunk_rows, profile)) return;
    }

    const rowref_col: []const i64 = if (layout.has_rowref) blk: {
        const view = batch.columnView(rowloc.col_name) orelse return error.ColumnNotFound;
        break :blk switch (view.data) {
            .bigint => |v| v,
            else => return error.TypeMismatch,
        };
    } else &.{};

    const route_t0 = if (profile) nowTicks() else 0;
    var accepted: u64 = 0;
    var r: usize = 0;
    while (r < batch.row_count) {
        var active = &parts.raw_active_rows;
        while (r < batch.row_count and active.len() < raw_chunk_rows) : (r += 1) {
            const key = if (layout.has_rowref)
                hashGenericKeyFromViews(layout, key_views_buf[0..layout.key_columns.len], key_digests_buf[0..layout.key_columns.len], r)
            else
                try genericKeyFromViews(layout, key_views_buf[0..layout.key_columns.len], r);
            const idx = active.len();
            // Weighted collapse (decomposable programs): an adjacent-equal key
            // folds into the last staged row instead of appending — the run
            // ships as ONE (key, weight, partials) row. The creating row's
            // rowref is kept (any row of the run has the same key values).
            if (layout.has_weight) {
                if (idx > 0 and active.keyAt(idx - 1) == key) {
                    active.weightAll()[idx - 1] += 1;
                    weightFoldRange(active, weight_specs, idx - 1, r, r + 1, false);
                    accepted += 1;
                    continue;
                }
                active.weightAll()[idx] = 1;
                weightFoldRange(active, weight_specs, idx, r, r + 1, true);
                active.setKey(idx, key);
                if (layout.has_rowref) active.rowrefAll()[idx] = rowref_col[r];
                active.len_rows = idx + 1;
                accepted += 1;
                continue;
            }
            active.setKey(idx, key);
            if (layout.has_rowref) active.rowrefAll()[idx] = rowref_col[r];
            appendGenericPayload(active, payload_bytes[0..layout.columns.len], payload_elt[0..layout.columns.len], idx, r);
            if (str_views.len != 0) try appendStrPayload(active, shared.allocator, str_views, idx, r);
            active.len_rows = idx + 1;
            accepted += 1;
        }
        if (active.len() == raw_chunk_rows) {
            if (shared.raw_scan_queues.len > 0) {
                const qidx = chooseRawScanPublishLane(shared, parts.raw_scan_lane, @max(@as(usize, 1), raw_chunk_rows / 2));
                try publishRawRowsToQueue(shared, &shared.raw_scan_queues[qidx], parts.worker_index, active, raw_chunk_rows, &shared.outstanding_chunks, &shared.outstanding_rows, &parts.raw_scan_queue_lock_ticks, &parts.raw_recycle_lock_ticks);
            } else {
                try publishRawRows(shared, parts.worker_index, active, raw_chunk_rows, &parts.raw_queue_lock_ticks, &parts.raw_recycle_lock_ticks);
            }
            parts.published_chunks += 1;
        }
    }
    parts.scanned_count += batch.row_count;
    parts.row_count += accepted;
    if (profile) parts.partition_ticks += nowTicks() - route_t0;
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
    const route_t0 = if (profile) nowTicks() else 0;
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
    if (profile) parts.partition_ticks += nowTicks() - route_t0;
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
            try appendBatchRawChunksGenericKey1I64(parts, shared, batch.row_count, k0, payload_bytes, payload_elt, weight_specs, str_views, raw_chunk_rows, profile);
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
            try appendBatchRawChunksGenericKeyI16I32(parts, shared, batch.row_count, k0, k1, payload_bytes, payload_elt, weight_specs, str_views, raw_chunk_rows, profile);
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
            try appendBatchRawChunksGenericKeyI64I32(parts, shared, batch.row_count, k0, k1, payload_bytes, payload_elt, weight_specs, str_views, raw_chunk_rows, profile);
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
    raw_chunk_rows: usize,
    profile: bool,
) !void {
    const route_t0 = if (profile) nowTicks() else 0;
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
        if (profile) parts.partition_ticks += nowTicks() - route_t0;
        return;
    }
    while (r < row_count) {
        var active = &parts.raw_active_rows;
        const keys = active.keyU64All();
        while (r < row_count and active.len() < raw_chunk_rows) : (r += 1) {
            const idx = active.len();
            keys[idx] = @as(u64, @bitCast(key0[r]));
            appendGenericPayload(active, payload_bytes, payload_elt, idx, r);
            if (str_views.len != 0) try appendStrPayload(active, shared.allocator, str_views, idx, r);
            active.len_rows = idx + 1;
            accepted += 1;
        }
        if (active.len() == raw_chunk_rows) try publishActiveRawRows(parts, shared, raw_chunk_rows);
    }
    parts.scanned_count += row_count;
    parts.row_count += accepted;
    if (profile) parts.partition_ticks += nowTicks() - route_t0;
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
    raw_chunk_rows: usize,
    profile: bool,
) !void {
    const route_t0 = if (profile) nowTicks() else 0;
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
        if (profile) parts.partition_ticks += nowTicks() - route_t0;
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
            if (str_views.len != 0) try appendStrPayload(active, shared.allocator, str_views, idx, r);
            active.len_rows = idx + 1;
            accepted += 1;
        }
        if (active.len() == raw_chunk_rows) try publishActiveRawRows(parts, shared, raw_chunk_rows);
    }
    parts.scanned_count += row_count;
    parts.row_count += accepted;
    if (profile) parts.partition_ticks += nowTicks() - route_t0;
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
    raw_chunk_rows: usize,
    profile: bool,
) !void {
    const route_t0 = if (profile) nowTicks() else 0;
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
        if (profile) parts.partition_ticks += nowTicks() - route_t0;
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
            if (str_views.len != 0) try appendStrPayload(active, shared.allocator, str_views, idx, r);
            active.len_rows = idx + 1;
            accepted += 1;
        }
        if (active.len() == raw_chunk_rows) try publishActiveRawRows(parts, shared, raw_chunk_rows);
    }
    parts.scanned_count += row_count;
    parts.row_count += accepted;
    if (profile) parts.partition_ticks += nowTicks() - route_t0;
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
};

fn buildWeightFoldSpecs(layout: GroupRowsLayout, batch: thindb.Batch, out: []WeightFoldSpec) !void {
    for (layout.columns, 0..) |column, c| {
        if (column.source_name.len == 0) return error.UnsupportedOperatorForType;
        const view = batch.columnView(column.source_name) orelse return error.ColumnNotFound;
        var op: ?WeightFoldOp = null;
        for (layout.aggregates) |agg| {
            if (agg.is_string or agg.is_distinct) continue;
            const ic = agg.input_column_index orelse continue;
            if (ic != c or op != null) continue;
            op = switch (agg.op) {
                .sum, .avg => .sum,
                .min => .min,
                .max => .max,
                else => return error.UnsupportedOperatorForType,
            };
        }
        out[c] = .{
            .op = op orelse return error.UnsupportedOperatorForType,
            .is_float = physicalIsFloat(column.physical_type),
            .view = view,
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
inline fn appendStrPayload(active: *RawRows, allocator: Allocator, str_views: []const StringView, dst: usize, src: usize) !void {
    for (str_views, 0..) |sv, col| {
        try active.str.append(allocator, str_views.len, dst, col, sv.rowBytes(src));
    }
}

fn publishActiveRawRows(parts: *WorkerParts, shared: *PipeShared, raw_chunk_rows: usize) !void {
    if (shared.raw_scan_queues.len > 0) {
        const qidx = chooseRawScanPublishLane(shared, parts.raw_scan_lane, @max(@as(usize, 1), raw_chunk_rows / 2));
        try publishRawRowsToQueue(shared, &shared.raw_scan_queues[qidx], parts.worker_index, &parts.raw_active_rows, raw_chunk_rows, &shared.outstanding_chunks, &shared.outstanding_rows, &parts.raw_scan_queue_lock_ticks, &parts.raw_recycle_lock_ticks);
    } else {
        try publishRawRows(shared, parts.worker_index, &parts.raw_active_rows, raw_chunk_rows, &parts.raw_queue_lock_ticks, &parts.raw_recycle_lock_ticks);
    }
    parts.published_chunks += 1;
}

fn appendBatchRawChunks(parts: *WorkerParts, shared: *PipeShared, batch: thindb.Batch, raw_chunk_rows: usize, profile: bool, skip_filter_check: bool) !void {
    return appendBatchRawChunksGeneric(parts, shared, batch, raw_chunk_rows, profile, skip_filter_check);
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
        // String MIN/MAX is folded separately into the side str_states array, and
        // COUNT(DISTINCT) into the side distinct_sets (its count slot is bumped
        // there, not here).
        if (agg.is_string or agg.is_distinct) continue;
        if (agg.state_index >= MAX_GROUP_AGG_STATES) return error.UnsupportedOperatorForType;
        const state_index: usize = agg.state_index;
        switch (agg.op) {
            .count_star => {
                if (state_index != 0) try setAggregateStateValue(ref, state_index, ref.head.count);
            },
            .count_col => {
                _ = aggregateInputValue(agg, rows, row_idx) catch return error.UnsupportedOperatorForType;
                if (state_index != 0) try setAggregateStateValue(ref, state_index, ref.head.count);
            },
            .sum, .avg => {
                if (aggInputIsFloat(rows, agg)) {
                    const value = try aggregateInputFloat(agg, rows, row_idx);
                    try addFloatStateValue(ref, state_index, value);
                } else {
                    const value = try aggregateInputValue(agg, rows, row_idx);
                    try addAggregateStateValue(ref, state_index, value);
                }
            },
            .min => {
                if (aggInputIsFloat(rows, agg)) {
                    const value = try aggregateInputFloat(agg, rows, row_idx);
                    if (ref.head.count == 1 or value < floatStateValue(ref, state_index)) try setFloatStateValue(ref, state_index, value);
                } else {
                    const value = try aggregateInputValue(agg, rows, row_idx);
                    if (ref.head.count == 1 or value < aggregateStateValue(ref, state_index)) try setAggregateStateValue(ref, state_index, value);
                }
            },
            .max => {
                if (aggInputIsFloat(rows, agg)) {
                    const value = try aggregateInputFloat(agg, rows, row_idx);
                    if (ref.head.count == 1 or value > floatStateValue(ref, state_index)) try setFloatStateValue(ref, state_index, value);
                } else {
                    const value = try aggregateInputValue(agg, rows, row_idx);
                    if (ref.head.count == 1 or value > aggregateStateValue(ref, state_index)) try setAggregateStateValue(ref, state_index, value);
                }
            },
            // Folded into the side distinct_sets by foldGroupDistinct; the
            // `is_distinct` guard above means control never reaches here.
            .count_distinct => unreachable,
        }
    }
}

fn aggregateInputValue(agg: GroupAggregateSpec, rows: GroupRows, row_idx: usize) !i128 {
    const input_index = agg.input_column_index orelse return error.UnsupportedOperatorForType;
    if (input_index >= rows.layout.columns.len) return error.UnsupportedOperatorForType;
    return rows.columnIntAt(input_index, row_idx);
}

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
        // String MIN/MAX has no numeric slot/payload column; its input is a
        // str_columns entry validated by the layout, not here.
        if (agg.is_string) continue;
        if (agg.state_index >= MAX_GROUP_AGG_STATES) return error.UnsupportedOperatorForType;
        if (agg.is_distinct and agg.distinct_state_index >= MAX_GROUP_DISTINCT_SLOTS) return error.UnsupportedOperatorForType;
        switch (agg.op) {
            .count_star => {},
            .count_col, .sum, .avg, .min, .max, .count_distinct => {
                const input_index = agg.input_column_index orelse return error.UnsupportedOperatorForType;
                if (input_index >= column_count) return error.UnsupportedOperatorForType;
            },
        }
    }
}

fn countSumAvgProgram(aggregates: []const GroupAggregateSpec, column_count: usize) ?CountSumAvgProgram {
    if (aggregates.len != 3) return null;
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
fn aggSlotCount(layout: GroupRowsLayout) usize {
    var n: usize = 0;
    for (layout.aggregates) |agg| {
        if (agg.is_string) continue;
        if (agg.state_index > n) n = agg.state_index;
    }
    return n;
}

fn groupChunkRowsDirect(
    table: *GroupTable,
    states: *StateSlab,
    str_states: *std.ArrayListUnmanaged(StrAccRow),
    distinct_sets: []DistinctSet,
    scratch: *GroupScratch,
    allocator: Allocator,
    str_arena: Allocator,
    rows: GroupRows,
) !void {
    const n = rows.len();
    if (n == 0) return;
    if (table.needsGrow(n)) try table.grow(allocator, n);
    try states.prepare(allocator, aggSlotCount(rows.layout));
    try states.ensureUnusedCapacity(allocator, n);
    if (rows.layout.has_str_payload) try str_states.ensureUnusedCapacity(allocator, n);
    if (rows.layout.distinct_slot_count > distinct_sets.len) return error.UnsupportedOperatorForType;

    if (rows.layout.columns.len > MAX_GROUP_PAYLOAD_COLUMNS) return error.UnsupportedOperatorForType;
    try validateGroupAggregateProgram(rows.layout.aggregates, rows.layout.columns.len);
    const rowrefs: []const i64 = if (rows.layout.has_rowref) rows.rowrefAll()[0..n] else &.{};
    if (countSumAvgProgram(rows.layout.aggregates, rows.layout.columns.len)) |program| blk: {
        // The CountSumAvg fast path reads inputs as i64; float inputs route to
        // the generic per-aggregate program path instead.
        if (physicalIsFloat(rows.layout.columns[program.sum_input_index].physical_type) or
            physicalIsFloat(rows.layout.columns[program.avg_input_index].physical_type)) break :blk;
        switch (rows.layout.key_width) {
            .u32 => try groupChunkRowsDirectKeysCountSumAvg(.u32, table, states, rows.keyU32All()[0..n], &.{}, n, rows, program, rowrefs),
            .u64 => try groupChunkRowsDirectKeysCountSumAvg(.u64, table, states, rows.keyU64All()[0..n], &.{}, n, rows, program, rowrefs),
            .u96 => try groupChunkRowsDirectKeysCountSumAvg(.u96, table, states, rows.keyU96LoAll()[0..n], rows.keyU96HiAll()[0..n], n, rows, program, rowrefs),
            .u128 => try groupChunkRowsDirectKeysCountSumAvg(.u128, table, states, rows.keyU128All()[0..n], &.{}, n, rows, program, rowrefs),
        }
        return;
    }
    switch (rows.layout.key_width) {
        .u32 => try groupChunkRowsDirectKeys(.u32, table, states, str_states, str_arena, distinct_sets, &scratch.gids, allocator, rows.keyU32All()[0..n], &.{}, n, rows.layout.aggregates, rows, rowrefs),
        .u64 => try groupChunkRowsDirectKeys(.u64, table, states, str_states, str_arena, distinct_sets, &scratch.gids, allocator, rows.keyU64All()[0..n], &.{}, n, rows.layout.aggregates, rows, rowrefs),
        .u96 => try groupChunkRowsDirectKeys(.u96, table, states, str_states, str_arena, distinct_sets, &scratch.gids, allocator, rows.keyU96LoAll()[0..n], rows.keyU96HiAll()[0..n], n, rows.layout.aggregates, rows, rowrefs),
        .u128 => try groupChunkRowsDirectKeys(.u128, table, states, str_states, str_arena, distinct_sets, &scratch.gids, allocator, rows.keyU128All()[0..n], &.{}, n, rows.layout.aggregates, rows, rowrefs),
    }
}

// Fold one row's string MIN/MAX values into group `gid`'s side accumulators.
// New extremes are dup'd from `str_arena` (a contiguous bump arena); a superseded
// value is left as arena garbage rather than freed, so the compare reads of the
// current value stay cache-local. Called only when the layout has string aggs.
fn foldGroupStr(str_states: *std.ArrayListUnmanaged(StrAccRow), str_arena: Allocator, gid: usize, aggregates: []const GroupAggregateSpec, rows: GroupRows, row_idx: usize) !void {
    const k = rows.layout.str_columns.len;
    for (aggregates) |agg| {
        if (!agg.is_string) continue;
        const b = rows.str.get(k, row_idx, agg.str_input_index);
        const acc = &str_states.items[gid][agg.str_state_index];
        if (!acc.present) {
            acc.bytes = try str_arena.dupe(u8, b);
            acc.present = true;
        } else {
            const cmp = std.mem.order(u8, b, acc.bytes);
            const is_better = if (agg.op == .min) cmp == .lt else cmp == .gt;
            if (is_better) acc.bytes = try str_arena.dupe(u8, b);
        }
    }
}

// Fold one row's COUNT(DISTINCT) inputs into group `gid`. Each distinct field's
// combined set is probed with the exact (gid,value) composite; a never-before-seen
// pair bumps that aggregate's running count slot. Called under the bucket agg_lock.
fn groupChunkRowsDirectKeysCountSumAvg(
    comptime key_width: GroupKeyWidth,
    table: *GroupTable,
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
    var r: usize = 0;
    while (r < row_count) {
        const pf = r + PREFETCH_DIST_BUCKET;
        if (pf < row_count) {
            const pf_key = groupKeyAt(key_width, keys, key_hi, pf);
            @prefetch(table.slotAddr(table.bucketOf(GroupTable.hashKey(pf_key))), .{ .rw = .write, .locality = 1 });
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
            sum_acc += rows.columnIntAt(program.sum_input_index, rr);
            avg_acc += rows.columnIntAt(program.avg_input_index, rr);
        }
        const run_len: u64 = if (weights.len != 0) blk: {
            var s: u64 = 0;
            for (weights[r..run_end]) |w| s += w;
            break :blk s;
        } else @intCast(run_end - r);
        const probe = table.getOrPut(GroupTable.hashKey(key), key);
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

fn groupChunkRowsDirectKeys(
    comptime key_width: GroupKeyWidth,
    table: *GroupTable,
    states: *StateSlab,
    str_states: *std.ArrayListUnmanaged(StrAccRow),
    str_arena: Allocator,
    distinct_sets: []DistinctSet,
    gids_buf: *std.ArrayListUnmanaged(u32),
    allocator: Allocator,
    keys: anytype,
    key_hi: []const u32,
    row_count: usize,
    aggregates: []const GroupAggregateSpec,
    rows: GroupRows,
    rowrefs: []const i64,
) !void {
    const has_str = rows.layout.has_str_payload;
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
    for (aggregates) |agg| {
        if (agg.is_string or agg.is_distinct) continue;
        if (agg.state_index >= MAX_GROUP_AGG_STATES) return error.UnsupportedOperatorForType;
        switch (agg.op) {
            .count_star, .count_col => {
                if (agg.state_index != 0) has_mirror = true;
            },
            .sum, .avg, .min, .max => {
                needs_kernels = true;
                if (agg.op == .min or agg.op == .max) has_extreme = true;
                if (agg.state_index == 0) kernelizable = false;
            },
            .count_distinct => unreachable,
        }
    }
    if (!kernelizable) {
        return groupChunkRowsDirectKeysProgram(key_width, table, states, str_states, str_arena, distinct_sets, gids_buf, allocator, keys, key_hi, n, aggregates, rows, rowrefs);
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

    var r: usize = 0;
    while (r < n) {
        const pf = r + PREFETCH_DIST_BUCKET;
        if (pf < n) {
            const pf_key = groupKeyAt(key_width, keys, key_hi, pf);
            @prefetch(table.slotAddr(table.bucketOf(GroupTable.hashKey(pf_key))), .{ .rw = .write, .locality = 1 });
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

        const probe = table.getOrPut(GroupTable.hashKey(key), key);
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
            tagged = if (mark_new) gid | NEW_GID_BIT else gid;
        } else {
            states.head(gid).count += run_len;
        }
        if (has_str) {
            var rr = r;
            while (rr < run_end) : (rr += 1) try foldGroupStr(str_states, str_arena, gid, aggregates, rows, rr);
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
            if (agg.is_string or agg.is_distinct) continue;
            switch (agg.op) {
                .count_star, .count_col => {},
                .sum, .avg => if (aggInputIsFloat(rows, agg))
                    foldKernelSumFloat(states, gids, rows, agg)
                else
                    foldKernelSumInt(states, gids, rows, agg),
                .min => if (aggInputIsFloat(rows, agg))
                    foldKernelExtremeFloat(true, states, gids, rows, agg)
                else
                    foldKernelExtremeInt(true, states, gids, rows, agg),
                .max => if (aggInputIsFloat(rows, agg))
                    foldKernelExtremeFloat(false, states, gids, rows, agg)
                else
                    foldKernelExtremeInt(false, states, gids, rows, agg),
                .count_distinct => unreachable,
            }
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
    table: *GroupTable,
    states: *StateSlab,
    str_states: *std.ArrayListUnmanaged(StrAccRow),
    str_arena: Allocator,
    distinct_sets: []DistinctSet,
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
    const has_str = rows.layout.has_str_payload;
    const has_distinct = rows.layout.distinct_slot_count > 0;
    const n = row_count;
    if (has_distinct) try gids_buf.resize(allocator, n);
    const gids: []u32 = if (has_distinct) gids_buf.items[0..n] else &.{};

    var prev_key: u128 = 0;
    var prev_gid: u32 = 0;
    var have_prev = false;
    var r: usize = 0;
    while (r < n) : (r += 1) {
        const pf = r + PREFETCH_DIST_BUCKET;
        if (pf < n) {
            const pf_key = groupKeyAt(key_width, keys, key_hi, pf);
            @prefetch(table.slotAddr(table.bucketOf(GroupTable.hashKey(pf_key))), .{ .rw = .write, .locality = 1 });
        }

        const key = groupKeyAt(key_width, keys, key_hi, r);
        // Adjacent-equal run: reuse the previous row's resolved gid, skipping
        // the table probe (the state program still folds per row — its inputs
        // vary within the run).
        if (have_prev and key == prev_key) {
            try updateGroupStateProgram(states.ref(prev_gid), aggregates, rows, r);
            if (has_str) try foldGroupStr(str_states, str_arena, prev_gid, aggregates, rows, r);
            if (has_distinct) gids[r] = prev_gid;
            continue;
        }
        const probe = table.getOrPut(GroupTable.hashKey(key), key);
        if (!probe.found) {
            const new_gid = try initGroupStateProgram(states, key, aggregates, rows, r);
            if (rowrefs.len != 0) states.head(new_gid).rowref = rowrefs[r];
            table.commit(probe.slot, key, new_gid);
            if (has_str) {
                str_states.appendAssumeCapacity([_]StrAcc{.{}} ** MAX_GROUP_STR_SLOTS);
                try foldGroupStr(str_states, str_arena, new_gid, aggregates, rows, r);
            }
            if (has_distinct) gids[r] = new_gid;
            prev_key = key;
            prev_gid = new_gid;
            have_prev = true;
            continue;
        }
        try updateGroupStateProgram(states.ref(probe.gid), aggregates, rows, r);
        if (has_str) try foldGroupStr(str_states, str_arena, probe.gid, aggregates, rows, r);
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
    @prefetch(states.bytes.ptr + @as(usize, gid) * states.stride + STATE_HEAD_BYTES, .{ .rw = .write, .locality = 1 });
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
fn foldKernelSumInt(states: *StateSlab, gids: []const u32, rows: GroupRows, agg: GroupAggregateSpec) void {
    const si: usize = agg.state_index;
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
                var acc: i64 = vals[r];
                var rr = r + 1;
                while (rr < gids.len and (gids[rr] & ~NEW_GID_BIT) == g) : (rr += 1) acc += vals[rr];
                states.slotsOf(g)[si - 1] += acc;
                r = rr;
            }
        },
        .f32, .f64 => {},
    }
}

fn foldKernelSumFloat(states: *StateSlab, gids: []const u32, rows: GroupRows, agg: GroupAggregateSpec) void {
    const si: usize = agg.state_index;
    switch (rows.layout.columns[agg.input_column_index.?].physical_type) {
        inline .f32, .f64 => |pt| {
            const T = groupPhysicalT(pt);
            const vals = rows.columnTypedAll(T, agg.input_column_index.?);
            var r: usize = 0;
            while (r < gids.len) {
                const pf = r + PREFETCH_DIST_STATES;
                if (pf < gids.len) prefetchStateSlots(states, gids[pf] & ~NEW_GID_BIT);
                const g = gids[r] & ~NEW_GID_BIT;
                var acc: f64 = @floatCast(vals[r]);
                var rr = r + 1;
                while (rr < gids.len and (gids[rr] & ~NEW_GID_BIT) == g) : (rr += 1) acc += @as(f64, @floatCast(vals[rr]));
                const slot = &states.slotsOf(g)[si - 1];
                const cur: f64 = @bitCast(slot.*);
                slot.* = @bitCast(cur + acc);
                r = rr;
            }
        },
        else => {},
    }
}

fn foldKernelExtremeInt(comptime is_min: bool, states: *StateSlab, gids: []const u32, rows: GroupRows, agg: GroupAggregateSpec) void {
    const si: usize = agg.state_index;
    switch (rows.layout.columns[agg.input_column_index.?].physical_type) {
        inline .i8, .i16, .i32, .i64 => |pt| {
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
                var best: i64 = vals[r];
                var rr = r + 1;
                while (rr < gids.len and (gids[rr] & ~NEW_GID_BIT) == g) : (rr += 1) {
                    const v: i64 = vals[rr];
                    if (if (is_min) v < best else v > best) best = v;
                }
                const slot = &states.slotsOf(g)[si - 1];
                const improves = if (is_min) best < slot.* else best > slot.*;
                if (tag_new or improves) slot.* = best;
                r = rr;
            }
        },
        .f32, .f64 => {},
    }
}

fn foldKernelExtremeFloat(comptime is_min: bool, states: *StateSlab, gids: []const u32, rows: GroupRows, agg: GroupAggregateSpec) void {
    const si: usize = agg.state_index;
    switch (rows.layout.columns[agg.input_column_index.?].physical_type) {
        inline .f32, .f64 => |pt| {
            const T = groupPhysicalT(pt);
            const vals = rows.columnTypedAll(T, agg.input_column_index.?);
            var r: usize = 0;
            while (r < gids.len) {
                const pf = r + PREFETCH_DIST_STATES;
                if (pf < gids.len) prefetchStateSlots(states, gids[pf] & ~NEW_GID_BIT);
                const tag_new = gids[r] & NEW_GID_BIT != 0;
                const g = gids[r] & ~NEW_GID_BIT;
                var best: f64 = @floatCast(vals[r]);
                var rr = r + 1;
                while (rr < gids.len and (gids[rr] & ~NEW_GID_BIT) == g) : (rr += 1) {
                    const v: f64 = @floatCast(vals[rr]);
                    if (if (is_min) v < best else v > best) best = v;
                }
                const slot = &states.slotsOf(g)[si - 1];
                const cur: f64 = @bitCast(slot.*);
                const improves = if (is_min) best < cur else best > cur;
                if (tag_new or improves) slot.* = @bitCast(best);
                r = rr;
            }
        },
        else => {},
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
    distinct_sets: []DistinctSet,
    allocator: Allocator,
    states: *StateSlab,
    gids: []const u32,
    aggregates: []const GroupAggregateSpec,
    rows: GroupRows,
) !void {
    const n = gids.len;
    for (aggregates) |agg| {
        if (!agg.is_distinct) continue;
        const dset = &distinct_sets[agg.distinct_state_index];
        try dset.ensureForBatch(allocator, n);
        const input_index = agg.input_column_index orelse return error.UnsupportedOperatorForType;
        if (input_index >= rows.layout.columns.len) return error.UnsupportedOperatorForType;
        // Hoist the physical-type switch out of the row loop: the kernel runs
        // over the staged column's typed slice directly.
        switch (rows.layout.columns[input_index].physical_type) {
            inline .i8, .i16, .i32, .i64 => |pt| {
                const T = switch (pt) {
                    .i8 => i8,
                    .i16 => i16,
                    .i32 => i32,
                    .i64 => i64,
                    else => unreachable,
                };
                try foldGroupDistinctTyped(T, dset, states, gids, rows.columnTypedAll(T, input_index), agg.state_index);
            },
            // The planner scopes distinct inputs to the integer family.
            .f32, .f64 => return error.UnsupportedOperatorForType,
        }
    }
}

fn foldGroupDistinctTyped(
    comptime T: type,
    dset: *DistinctSet,
    states: *StateSlab,
    gids: []const u32,
    vals: []const T,
    state_index: u16,
) !void {
    const n = gids.len;
    var r: usize = 0;
    while (r < n) : (r += 1) {
        const pf = r + PREFETCH_DIST_DISTINCT;
        if (pf < n) {
            const v_pf: i64 = vals[pf];
            dset.prefetchKey(DistinctSet.key(gids[pf], @bitCast(v_pf)));
        }
        const v: i64 = vals[r];
        // The table's physical order clusters repeated values (UserID etc.)
        // into adjacent runs: an identical (gid, value) pair can't be new, so
        // skip the cache-missing set probe entirely.
        if (r > 0 and gids[r] == gids[r - 1] and v == @as(i64, vals[r - 1])) continue;
        const composite = DistinctSet.key(gids[r], @bitCast(v));
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

const SiloGridJob = struct {
    scan: *Scan,
    // Batch source driven each tile: a Compute-wrapped scan when the shape has
    // derived columns, else a bare scan. `scan` is the same underlying object,
    // retained for range resets; `drive` produces the batches (with derived
    // columns appended). Points into the harness-owned `drives` array.
    drive: *thindb.exec.Query,
    local: *WorkerParts,
    shared: *PipeShared,
    seg_start: []const usize,
    segment_count: usize,
    worker_index: usize,
    worker_count: usize,
    chunk_rows: usize,
    scan_tile_rgs: usize,
    scan_coalesce_tiles: usize,
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
};

fn siloGridWorker(job: SiloGridJob) void {
    pinToCpu(job.cpu);
    siloGridWorkerErr(job) catch |e| {
        job.err.* = e;
        // Wake the peers so they stop waiting on coordination counters this
        // worker will never advance (e.g. its `scans_done` increment).
        job.shared.aborted.store(true, .release);
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
    raw_group_chunk_rows: usize,
    queue_lock_ticks: ?*i64,
    recycle_lock_ticks: ?*i64,
) !void {
    if (bucket_idx >= shared.raw_group_queues.len) return;
    const row_count = builder.rows.len();
    if (row_count == 0) return;

    const rows = builder.rows;
    builder.rows = try acquireGroupRows(shared, raw_group_chunk_rows, recycle_lock_ticks);
    errdefer {
        builder.rows.deinit(shared.allocator);
        builder.rows = rows;
    }
    _ = shared.stage_builder_rows.fetchSub(@intCast(row_count), .release);

    try publishGroupChunkToQueue(
        shared,
        &shared.raw_group_queues[bucket_idx],
        .{ .rows = rows, .owner_worker = 0, .bucket_idx = bucket_idx },
        &shared.stage_outstanding_chunks,
        &shared.stage_outstanding_rows,
        queue_lock_ticks,
    );
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
    const lock_t0 = if (profile) nowTicks() else 0;
    lockSpin(&builder.lock);
    if (profile) local.raw_stage_builder_lock_ticks += nowTicks() - lock_t0;
    defer builder.lock.unlock();

    var pos: usize = 0;
    while (pos < count) {
        if (builder.rows.capacity() == 0) {
            builder.rows = try acquireGroupRows(shared, raw_group_chunk_rows, &local.raw_recycle_lock_ticks);
        }
        if (builder.rows.len() >= raw_group_chunk_rows) {
            try publishSharedStageBuilderLocked(shared, builder, bucket_idx, raw_group_chunk_rows, &local.raw_group_queue_lock_ticks, &local.raw_recycle_lock_ticks);
            continue;
        }

        const free = raw_group_chunk_rows - builder.rows.len();
        const take = @min(free, count - pos);
        try builder.rows.appendRawRowsSlice(shared.allocator, shared.group_rows_layout, rows, start + pos, take);
        _ = shared.stage_builder_rows.fetchAdd(@intCast(take), .release);
        pos += take;

        if (builder.rows.len() >= raw_group_chunk_rows) {
            try publishSharedStageBuilderLocked(shared, builder, bucket_idx, raw_group_chunk_rows, &local.raw_group_queue_lock_ticks, &local.raw_recycle_lock_ticks);
        }
    }
}

fn flushSharedStageBuilders(
    shared: *PipeShared,
    local: *WorkerParts,
    raw_group_chunk_rows: usize,
    profile: bool,
) !bool {
    if (shared.stage_builder_rows.load(.acquire) == 0) return false;
    const publish_t0 = if (profile) nowTicks() else 0;
    var flushed = false;
    var b: usize = 0;
    while (b < shared.stage_builders.len) : (b += 1) {
        const builder = &shared.stage_builders[b];
        const lock_t0 = if (profile) nowTicks() else 0;
        lockSpin(&builder.lock);
        if (profile) local.raw_stage_builder_lock_ticks += nowTicks() - lock_t0;
        if (builder.rows.len() > 0) {
            try publishSharedStageBuilderLocked(shared, builder, b, raw_group_chunk_rows, &local.raw_group_queue_lock_ticks, &local.raw_recycle_lock_ticks);
            flushed = true;
        }
        builder.lock.unlock();
    }
    if (profile) local.raw_stage_publish_ticks += nowTicks() - publish_t0;
    return flushed;
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

    var pop_t0 = if (profile) nowTicks() else 0;
    var popped_total = popRawChunkBatchFromQueue(&shared.raw_scan_queues[scan_lane], &raw_chunks, raw_batch_chunks, &local.raw_scan_queue_lock_ticks);
    if (profile) local.raw_stage_pop_ticks += nowTicks() - pop_t0;
    if (popped_total == 0) return false;

    _ = shared.active_stage_jobs.fetchAdd(1, .release);
    defer _ = shared.active_stage_jobs.fetchSub(1, .release);

    const bucket_count = @min(shared.bucket_count, shared.raw_group_queues.len);
    std.debug.assert(bucket_count <= @as(usize, std.math.maxInt(u16)) + 1);
    try local.ensureWideBucketScratch(allocator, bucket_count);

    while (popped_total > 0) {
        local.raw_stage_input_chunks += @intCast(popped_total);

        var total_rows: usize = 0;
        var i: usize = 0;
        while (i < popped_total) : (i += 1) total_rows += raw_chunks[i].rows.len();
        _ = shared.outstanding_rows.fetchSub(@intCast(total_rows), .release);
        _ = shared.outstanding_chunks.fetchSub(popped_total, .release);
        if (total_rows == 0) {
            i = 0;
            const recycle_t0 = if (profile) nowTicks() else 0;
            while (i < popped_total) : (i += 1) try recycleRawRows(shared, raw_chunks[i].rows, raw_chunk_rows, &local.raw_recycle_lock_ticks);
            if (profile) local.raw_stage_recycle_ticks += nowTicks() - recycle_t0;
            pop_t0 = if (profile) nowTicks() else 0;
            popped_total = popRawChunkBatchFromQueue(&shared.raw_scan_queues[scan_lane], &raw_chunks, raw_batch_chunks, &local.raw_scan_queue_lock_ticks);
            if (profile) local.raw_stage_pop_ticks += nowTicks() - pop_t0;
            continue;
        }

        try local.flat_raw_rows.resize(allocator, shared.group_rows_layout, total_rows);
        try local.flat_bucket_ids.resize(allocator, total_rows);
        const flat_has_str = shared.group_rows_layout.has_str_payload;
        const flat_str_cols = shared.group_rows_layout.str_columns.len;
        if (flat_has_str) local.flat_raw_rows.str.clear();

        const stage_t0 = if (profile) nowTicks() else 0;
        @memset(local.flat_counts[0..bucket_count], 0);
        var row_idx: usize = 0;
        i = 0;
        while (i < popped_total) : (i += 1) {
            var r: usize = 0;
            while (r < raw_chunks[i].rows.len()) : (r += 1) {
                const b = rawRowBucketIndex(raw_chunks[i].rows, r, bucket_count);
                local.flat_bucket_ids.items[row_idx] = @intCast(b);
                local.flat_counts[b] += 1;
                row_idx += 1;
            }
        }

        var offset: u32 = 0;
        var b: usize = 0;
        while (b < bucket_count) : (b += 1) {
            local.flat_offsets[b] = offset;
            local.flat_next[b] = offset;
            offset += local.flat_counts[b];
        }

        row_idx = 0;
        const ncols = shared.group_rows_layout.columns.len;
        var flat_slabs: [MAX_GROUP_PAYLOAD_COLUMNS][]u8 = undefined;
        var col_elt: [MAX_GROUP_PAYLOAD_COLUMNS]usize = undefined;
        for (0..ncols) |c| {
            flat_slabs[c] = local.flat_raw_rows.columnByteSlab(c);
            col_elt[c] = groupColumnSize(shared.group_rows_layout.columns[c].physical_type);
        }
        const has_rowref = shared.group_rows_layout.has_rowref;
        const has_weight = shared.group_rows_layout.has_weight;
        const flat_rowref: []i64 = if (has_rowref) local.flat_raw_rows.rowrefAll() else &.{};
        const flat_weight: []u32 = if (has_weight) local.flat_raw_rows.weightAll() else &.{};
        switch (local.flat_raw_rows.layout.key_width) {
            inline else => |kw| {
                const flat = local.flat_raw_rows;
                i = 0;
                while (i < popped_total) : (i += 1) {
                    const src = raw_chunks[i].rows;
                    var src_slabs: [MAX_GROUP_PAYLOAD_COLUMNS][]const u8 = undefined;
                    for (0..ncols) |c| src_slabs[c] = src.columnByteSlab(c);
                    const src_rowref: []const i64 = if (has_rowref) src.rowrefAll() else &.{};
                    const src_weight: []const u32 = if (has_weight) src.weightAll() else &.{};
                    const chunk_rows_n = src.len();
                    var r: usize = 0;
                    while (r < chunk_rows_n) : (r += 1) {
                        const bucket_idx: usize = local.flat_bucket_ids.items[row_idx];
                        const pos = local.flat_next[bucket_idx];
                        flat.copyKeyFrom(kw, pos, src, r);
                        var c: usize = 0;
                        while (c < ncols) : (c += 1) copyElem(flat_slabs[c], src_slabs[c], col_elt[c], pos, r);
                        if (has_rowref) flat_rowref[pos] = src_rowref[r];
                        if (has_weight) flat_weight[pos] = src_weight[r];
                        if (flat_has_str) {
                            var sc: usize = 0;
                            while (sc < flat_str_cols) : (sc += 1)
                                try local.flat_raw_rows.str.append(allocator, flat_str_cols, pos, sc, src.str.get(flat_str_cols, r, sc));
                        }
                        local.flat_next[bucket_idx] = pos + 1;
                        row_idx += 1;
                    }
                }
            },
        }
        if (profile) local.raw_stage_ticks += nowTicks() - stage_t0;

        const append_t0 = if (profile) nowTicks() else 0;
        b = 0;
        while (b < bucket_count) : (b += 1) {
            const count: usize = local.flat_counts[b];
            if (count == 0) continue;
            const start: usize = local.flat_offsets[b];
            try appendSharedStageBuilderRawSlice(shared, local, b, local.flat_raw_rows, start, count, raw_group_chunk_rows, profile);
        }
        if (profile) local.raw_stage_slice_ticks += nowTicks() - append_t0;

        i = 0;
        const recycle_t0 = if (profile) nowTicks() else 0;
        while (i < popped_total) : (i += 1) try recycleRawRows(shared, raw_chunks[i].rows, raw_chunk_rows, &local.raw_recycle_lock_ticks);
        if (profile) local.raw_stage_recycle_ticks += nowTicks() - recycle_t0;

        pop_t0 = if (profile) nowTicks() else 0;
        popped_total = popRawChunkBatchFromQueue(&shared.raw_scan_queues[scan_lane], &raw_chunks, raw_batch_chunks, &local.raw_scan_queue_lock_ticks);
        if (profile) local.raw_stage_pop_ticks += nowTicks() - pop_t0;
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
    var i: usize = 0;
    while (i < popped_total) : (i += 1) {
        const rows = group_chunks[i].rows;
        total_rows += @intCast(rows.len());
        const lock_t0 = if (profile) nowTicks() else 0;
        lockSpin(&bucket.agg_lock);
        if (profile) local.raw_agg_lock_ticks += nowTicks() - lock_t0;
        const g0 = if (profile) nowTicks() else 0;
        try groupChunkRowsDirect(&bucket.table, &bucket.states, &bucket.str_states, &bucket.distinct_sets, scratch, allocator, bucket.str_arena.allocator(), rows);
        bucket.row_count += rows.len();
        if (profile) group_ticks.* += nowTicks() - g0;
        bucket.agg_lock.unlock();
        chunks.* += 1;
        try recycleGroupRows(shared, group_chunks[i].rows, raw_group_chunk_rows, &local.raw_recycle_lock_ticks);
    }
    _ = shared.stage_outstanding_rows.fetchSub(total_rows, .release);
    _ = shared.stage_outstanding_chunks.fetchSub(popped_total, .release);
    return true;
}

fn collectOwnedTop(shared: *PipeShared, worker_index: usize, worker_count: usize, top_out: *TopSet, top_ticks: *i64, profile: bool) void {
    const top_t0 = if (profile) nowTicks() else 0;
    if (top_out.items.len == 0) return;
    var b = worker_index;
    const has_str = shared.group_rows_layout.has_str_payload;
    while (b < shared.buckets.len) : (b += worker_count) {
        const bkt = &shared.buckets[b];
        if (has_str) {
            var gid: usize = 0;
            while (gid < bkt.states.len) : (gid += 1) top_out.consider(topRowFromStateStr(bkt.states.ref(gid), bkt.str_states.items[gid]));
        } else {
            var gid: usize = 0;
            while (gid < bkt.states.len) : (gid += 1) top_out.consider(topRowFromState(bkt.states.ref(gid)));
        }
    }
    if (profile) top_ticks.* += nowTicks() - top_t0;
}

fn claimScanTile(job: SiloGridJob) ?ScanTile {
    const claim_rgs = job.scan_tile_rgs * job.scan_coalesce_tiles;
    const lo = job.shared.next_scan_rg.fetchAdd(claim_rgs, .monotonic);
    if (lo >= job.shared.total_scan_rgs) return null;
    _ = job.shared.active_scan_jobs.fetchAdd(1, .release);
    return .{ .lo = lo, .hi = @min(lo + claim_rgs, job.shared.total_scan_rgs) };
}

fn openGridScanTile(job: SiloGridJob, tile: ScanTile) void {
    const start = flatToCoord(tile.lo, job.seg_start, job.segment_count, job.shared.total_scan_rgs);
    const end = flatToCoord(tile.hi, job.seg_start, job.segment_count, job.shared.total_scan_rgs);
    const reset_t0 = if (job.profile) nowTicks() else 0;
    job.scan.resetRange(start.seg, start.rg, end.seg, end.rg, tile.hi == job.shared.total_scan_rgs);
    if (job.profile) job.local.scan_reset_ticks += nowTicks() - reset_t0;
    job.local.scan_tiles += 1;
}

fn runGridScanBurst(job: SiloGridJob, scan_exhausted: *bool, marked_scan_done: *bool) !void {
    const claim_t0 = if (job.profile) nowTicks() else 0;
    const tile = claimScanTile(job) orelse {
        if (job.profile) job.local.sched_scan_claim_ticks += nowTicks() - claim_t0;
        scan_exhausted.* = true;
        try markGridScanDone(job, marked_scan_done);
        return;
    };
    if (job.profile) job.local.sched_scan_claim_ticks += nowTicks() - claim_t0;
    job.local.sched_scan_jobs += 1;
    errdefer _ = job.shared.active_scan_jobs.fetchSub(1, .release);
    openGridScanTile(job, tile);

    job.local.scan_quanta += 1;
    while (true) {
        const t0 = if (job.profile) nowTicks() else 0;
        const maybe_batch = try job.drive.next();
        if (job.profile) job.local.scan_ticks += nowTicks() - t0;
        const batch = maybe_batch orelse break;
        job.local.scan_batches += 1;
        try appendBatchRawChunks(job.local, job.shared, batch, job.raw_chunk_rows, job.profile, job.filter_fused);
    }

    if (job.raw_group_mode != .off and job.shared.raw_scan_queues.len > 0) {
        const publish_t0 = if (job.profile) nowTicks() else 0;
        const qidx = chooseRawScanPublishLane(job.shared, job.local.raw_scan_lane, @max(@as(usize, 1), job.raw_chunk_rows / 2));
        try publishRawRowsToQueue(job.shared, &job.shared.raw_scan_queues[qidx], job.worker_index, &job.local.raw_active_rows, job.raw_chunk_rows, &job.shared.outstanding_chunks, &job.shared.outstanding_rows, &job.local.raw_scan_queue_lock_ticks, &job.local.raw_recycle_lock_ticks);
        if (job.profile) job.local.publish_ticks += nowTicks() - publish_t0;
    }

    if (tile.hi == job.shared.total_scan_rgs) {
        try markGridScanDone(job, marked_scan_done);
    }
    _ = job.shared.active_scan_jobs.fetchSub(1, .release);
}

fn markGridScanDone(job: SiloGridJob, marked_scan_done: *bool) !void {
    if (marked_scan_done.*) return;
    if (job.raw_group_mode != .off) {
        const publish_t0 = if (job.profile) nowTicks() else 0;
        if (job.shared.raw_scan_queues.len > 0) {
            const qidx = chooseRawScanPublishLane(job.shared, job.local.raw_scan_lane, @max(@as(usize, 1), job.raw_chunk_rows / 2));
            try publishRawRowsToQueue(job.shared, &job.shared.raw_scan_queues[qidx], job.worker_index, &job.local.raw_active_rows, job.raw_chunk_rows, &job.shared.outstanding_chunks, &job.shared.outstanding_rows, &job.local.raw_scan_queue_lock_ticks, &job.local.raw_recycle_lock_ticks);
        }
        if (job.profile) job.local.publish_ticks += nowTicks() - publish_t0;
    }
    _ = job.shared.scans_done.fetchAdd(1, .release);
    marked_scan_done.* = true;
}

fn siloGridWorkerErr(job: SiloGridJob) !void {
    var scratch: GroupScratch = .{};
    defer scratch.deinit(job.shared.allocator);

    var marked_scan_done = false;
    var scan_exhausted = false;
    var idle_spins: usize = 0;

    while (true) {
        // A peer failed: stop scheduling and tear down (the failing worker's
        // error is already recorded; ours would just race it).
        if (job.shared.aborted.load(.acquire)) break;
        job.local.sched_loops += 1;
        const decision_t0 = if (job.profile) nowTicks() else 0;
        const scan_claims_available = !scan_exhausted and job.shared.next_scan_rg.load(.acquire) < job.shared.total_scan_rgs;
        const global_scan_finished = job.shared.next_scan_rg.load(.acquire) >= job.shared.total_scan_rgs and
            job.shared.active_scan_jobs.load(.acquire) == 0;
        if (global_scan_finished and !marked_scan_done) {
            if (job.profile) job.local.sched_decision_ticks += nowTicks() - decision_t0;
            scan_exhausted = true;
            try markGridScanDone(job, &marked_scan_done);
            idle_spins = 0;
            continue;
        }
        if (job.profile) job.local.sched_decision_ticks += nowTicks() - decision_t0;

        if (job.raw_group_mode == .staged_final) {
        const scan_choice = if (scan_claims_available) minUnlockedScanLane(job.shared.raw_scan_queues, job.worker_index) else null;
        const stage_choice = maxUnlockedQueueLaneRows(job.shared.raw_scan_queues, job.worker_index);
        const group_choice = maxUnlockedQueueLaneRows(job.shared.raw_group_queues, job.worker_index);
            const max_stage_rows = if (stage_choice) |choice| choice.rows else 0;
            const max_group_rows = if (group_choice) |choice| choice.rows else 0;
            const downstream_max_rows = @max(max_stage_rows, max_group_rows);

            if (scan_choice) |choice| {
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
            }

            if (max_stage_rows > max_group_rows) {
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

            if (max_group_rows > 0) {
                if (group_choice) |choice| {
                    if (claimRawQueueLaneExact(job.shared.raw_group_queues, choice.lane)) {
                        if (try drainRawDedicatedGroupLane(
                            job.shared.allocator,
                            job.shared,
                            job.local,
                            &scratch,
                            choice.lane,
                            job.raw_group_chunk_rows,
                            job.raw_batch_chunks,
                            job.group_ticks,
                            job.chunks,
                            job.profile,
                        )) {
                            job.local.sched_group_jobs += 1;
                            idle_spins = 0;
                            continue;
                        }
                        job.local.sched_group_misses += 1;
                    }
                }
            }

            const no_more_stage_input = global_scan_finished and
                job.shared.outstanding_chunks.load(.acquire) == 0 and
                job.shared.active_stage_jobs.load(.acquire) == 0;
            const has_stage_partials = job.shared.stage_builder_rows.load(.acquire) > 0;
            if (no_more_stage_input and has_stage_partials) {
                const flushed = try flushSharedStageBuilders(job.shared, job.local, job.raw_group_chunk_rows, job.profile);
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

            if (job.shared.scans_done.load(.acquire) == job.shared.scan_threads and
                job.shared.outstanding_chunks.load(.acquire) == 0 and
                job.shared.stage_outstanding_chunks.load(.acquire) == 0 and
                job.shared.active_stage_jobs.load(.acquire) == 0 and
                job.shared.active_group_jobs.load(.acquire) == 0)
            {
                break;
            }

            const idle_t0 = if (job.profile) nowTicks() else 0;
            job.local.sched_idle_loops += 1;
            idle_spins += 1;
            if (idle_spins < 256) {
                std.atomic.spinLoopHint();
            } else {
                std.Thread.yield() catch {};
                idle_spins = 0;
            }
            if (job.profile) job.idle_ticks.* += nowTicks() - idle_t0;
            continue;
        }
    }

    if (!marked_scan_done) {
        try markGridScanDone(job, &marked_scan_done);
    }
    collectOwnedTop(job.shared, job.worker_index, job.worker_count, job.top, job.top_ticks, job.profile);
}

pub const RunConfig = struct {
    dop: usize,
    bucket_count: usize,
    // Free the staging-chunk pools (gigabytes of recycled RawRows slabs) on a
    // detached thread after the result is built, instead of on the wire path.
    // Requires `allocator` to be thread-safe and to outlive the query — the
    // engine sets this only for the fresh-workspace path (never the arena).
    defer_heavy_teardown: bool = false,
    // String group-key columns the worker scans emit as key digests
    // (`Batch.hashed`) instead of materialized strings — hashed-key shapes
    // where the key bytes only return at emit via rowref late-mat. A scan
    // decline (or a sidecar-less batch) falls back to digesting bytes.
    hash_key_columns: []const []const u8 = &.{},
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
    filter_expr: ?thindb.exec.PredicateExpr = null,
    shared_stage_builders: bool = false,
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

// Combined-key cardinality upper bound: saturating product of the KEY
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

fn expectedGroupsPerBucket(total_rows: u64, bucket_count: usize, stats: thindb.exec.PipelineStats, schema: []const thindb.types.Column, key_columns: []const GroupKeyColumnSpec, generic_key_width: GroupKeyWidth, generic_has_filter: bool) usize {
    const conservative_total = @max(@as(u64, 16), total_rows / 4);
    const generic_key_count = key_columns.len;
    const estimated_total = estimateGroupCountFromStats(stats, schema, key_columns) orelse conservative_total;
    const has_filter = generic_has_filter;
    const generic_wide_no_filter = generic_key_count != 0 and !has_filter and (generic_key_width == .u96 or generic_key_width == .u128);
    const no_filter_near_unique = !has_filter and estimated_total * 4 >= total_rows * 3;
    // A filter can only REDUCE the distinct key combinations, so the no-filter
    // NDV-product estimate stays a sound upper bound — but a correlated
    // compound key defeats it (WindowClientWidth × Height ≈ 25M product for
    // ~11K real combos), so under a filter the INITIAL presize is also capped
    // outright: zeroing rows/4-group tables costs ~20ms of setup on a query
    // whose whole runtime is ~40ms, while a rare filtered query that really
    // produces millions of groups just grows (amortized ~2× insert cost on a
    // query that runs seconds anyway). Sizing only — never correctness.
    const filtered_init_cap: u64 = 2 * 1024 * 1024;
    const total_groups = if (generic_wide_no_filter)
        total_rows
    else if (no_filter_near_unique)
        estimated_total
    else if (has_filter)
        @min(@min(estimated_total, conservative_total), filtered_init_cap)
    else
        @min(estimated_total, conservative_total);
    const per_bucket = (total_groups + @as(u64, @intCast(bucket_count)) - 1) / @as(u64, @intCast(bucket_count));
    return @intCast(@max(@as(u64, 16), per_bucket));
}

fn chooseGridChunkRows(cfg_chunk_rows: usize, chunk_rows_set: bool) usize {
    return if (chunk_rows_set) cfg_chunk_rows else GRID_CHUNK_ROWS;
}

fn chooseGridScanTileRgs(cfg_scan_tile_rgs: usize, scan_tile_rgs_set: bool) usize {
    if (scan_tile_rgs_set) return cfg_scan_tile_rgs;
    return GRID_SCAN_TILE_RGS;
}

pub fn runSiloGrid(allocator: Allocator, table: *thindb.api.Table, cpus: []const usize, cfg: RunConfig) !void {
    const freq = perfFreq();
    const function_t0 = nowTicks();
    g_cp_on = PROFILING and (getenv("THINDB_V2_CHUNK_PROFILE") != null);
    if (comptime PROFILING) chunkProfileReset();
    defer chunkProfileDump();
    var pre_return_ticks: i64 = 0;
    defer {
        if ((PROFILING and cfg.trace_timing) and pre_return_ticks != 0) {
            std.debug.print("[harness-core-cleanup] query={s} total_cleanup_after_return={d:.1}ms\n", .{
                "generic",
                ticksToMs(nowTicks() - pre_return_ticks, freq),
            });
        }
    }
    const total = totalRows(table);
    const dop = @max(@as(usize, 1), cfg.dop);

    const chunk_rows = chooseGridChunkRows(cfg.chunk_rows, cfg.chunk_rows_set);
    const raw_chunk_rows = @max(@as(usize, 1), cfg.raw_chunk_rows);
    const raw_group_chunk_rows = if (cfg.raw_group_chunk_rows == 0) raw_chunk_rows else @max(@as(usize, 1), cfg.raw_group_chunk_rows);
    const raw_batch_chunks = @max(@as(usize, 1), cfg.raw_batch_chunks);
    const group_rows_layout = cfg.group_rows_layout;
    const use_raw_group = cfg.raw_group_mode != .off;
    const scan_tile_rgs = chooseGridScanTileRgs(cfg.scan_tile_rgs, cfg.scan_tile_rgs_set);
    const scan_coalesce_tiles = cfg.scan_coalesce_tiles;

    const snapshot_setup_t0 = if ((PROFILING and cfg.trace_timing)) nowTicks() else 0;
    table.ddl_lock.lockSharedUncancelable(table.io);
    defer table.ddl_lock.unlockShared(table.io);

    const snap = Scan.captureSnapshot(table);
    var pin_held = true;
    defer if (pin_held) snap.memtable_snap.release();

    const seg_start = try allocator.alloc(usize, snap.segment_count + 1);
    defer allocator.free(seg_start);
    var total_rgs: usize = 0;
    for (table.manifest.segments.items[0..snap.segment_count], 0..) |entry, i| {
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
    var stats_scan = try Scan.allocWithProjectionLoc(table.allocator, table, null, scan_columns, false, snap);
    defer {
        const cleanup_t0 = if ((PROFILING and cfg.trace_timing)) nowTicks() else 0;
        stats_scan.deinit();
        if ((PROFILING and cfg.trace_timing)) std.debug.print("[harness-core-cleanup] query={s} stats_scan={d:.3}ms\n", .{
            "generic",
            ticksToMs(nowTicks() - cleanup_t0, freq),
        });
    }
    // Right-size the worker fleet to the work that survives zone-map pruning:
    // fuse the filter into the stats scan (the same hint set every worker scan
    // gets) and count via the shared `Scan.survivingWorkUnits`. A selective
    // filter then spins ceil(surviving/RGS_PER_GRID_WORKER) workers instead of
    // the full DOP; an unfusable or absent filter keeps full DOP.
    var work_rgs: usize = total_rgs + (@as(usize, @intCast(snap.memtable_snap.row_count)) + 65535) / 65536;
    if (cfg.filter_expr) |expr| {
        if (applyScanFilterExpr(stats_scan, expr) catch false) {
            if (stats_scan.survivingWorkUnits()) |surviving| work_rgs = surviving;
        }
    }
    const sized_workers = (work_rgs + RGS_PER_GRID_WORKER - 1) / RGS_PER_GRID_WORKER;
    const n_workers = @max(@as(usize, 1), @min(dop, @min(cpus.len, @max(sized_workers, 1))));

    // Narrow the scatter for small filtered inputs. Route blocks hold
    // `route_block_rows` rows PER BUCKET, so at the default 256 buckets a
    // selective query's chunks scatter a few dozen rows into each block and
    // the flush machinery costs more than the aggregation it feeds (Q37:
    // 13ms routing for 5.4ms of aggregate work). A handful of buckets keeps
    // the blocks dense while the group stage stays n_workers-parallel.
    // Measured on the 660K-row Title GROUP BY: 256 buckets ≈ 28-32ms,
    // 4-64 ≈ 22ms, 1 ≈ 29ms (serial group lane overshoots).
    const small_input = work_rgs <= 64;
    const bucket_count = if (small_input) @min(cfg.bucket_count, @max(@as(usize, 4), n_workers)) else cfg.bucket_count;
    const route_block_rows = chooseRouteBlockRows(bucket_count, cfg.route_block_rows, cfg.route_block_rows_set);

    const expected_groups_per_bucket = expectedGroupsPerBucket(total, bucket_count, stats_scan.stats(), stats_scan.outputSchema(), group_rows_layout.key_columns, group_rows_layout.key_width, cfg.filter_expr != null);
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
    const snapshot_setup_ticks = if ((PROFILING and cfg.trace_timing)) nowTicks() - snapshot_setup_t0 else 0;

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
        const cleanup_t0 = if ((PROFILING and cfg.trace_timing)) nowTicks() else 0;
        for (drives[0..built_scans]) |*d| d.deinit();
        if ((PROFILING and cfg.trace_timing)) std.debug.print("[harness-core-cleanup] query={s} worker_scans={d:.1}ms count={d}\n", .{
            "generic",
            ticksToMs(nowTicks() - cleanup_t0, freq),
            built_scans,
        });
    }

    const bucket_setup_t0 = if ((PROFILING and cfg.trace_timing)) nowTicks() else 0;
    const using_workspace = cfg.workspace != null;
    var workspace_profile: WorkspaceProfile = .{};
    const workspace_profile_ptr: ?*WorkspaceProfile = if ((PROFILING and cfg.trace_timing) and using_workspace) &workspace_profile else null;
    var owned_parts: []WorkerParts = &.{};
    defer if (!using_workspace and owned_parts.len > 0) allocator.free(owned_parts);
    var built_parts: usize = 0;
    defer if (!using_workspace) {
        const cleanup_t0 = if ((PROFILING and cfg.trace_timing)) nowTicks() else 0;
        for (owned_parts[0..built_parts]) |*p| p.deinit(allocator);
        if ((PROFILING and cfg.trace_timing)) std.debug.print("[harness-core-cleanup] query={s} worker_parts={d:.1}ms count={d}\n", .{
            "generic",
            ticksToMs(nowTicks() - cleanup_t0, freq),
            built_parts,
        });
    };
    var owned_buckets: []PipeBucket = &.{};
    defer if (!using_workspace and owned_buckets.len > 0) allocator.free(owned_buckets);
    var built_buckets: usize = 0;
    defer if (!using_workspace) {
        const cleanup_t0 = if ((PROFILING and cfg.trace_timing)) nowTicks() else 0;
        for (owned_buckets[0..built_buckets]) |*bkt| bkt.deinit(allocator);
        if ((PROFILING and cfg.trace_timing)) std.debug.print("[harness-core-cleanup] query={s} pipe_buckets={d:.1}ms count={d}\n", .{
            "generic",
            ticksToMs(nowTicks() - cleanup_t0, freq),
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
            buckets[b].states.reserve(init_groups_per_bucket);
            try buckets[b].chunks.ensureTotalCapacity(allocator, 8);
        }
    }
    const bucket_setup_ticks = if ((PROFILING and cfg.trace_timing)) nowTicks() - bucket_setup_t0 else 0;
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
        .allocator = allocator,
        .buckets = buckets,
        .bucket_count = bucket_count,
        .raw_scan_queues = raw_scan_queues,
        .raw_group_queues = raw_group_queues,
        .stage_builders = stage_builders,
        .group_rows_layout = group_rows_layout,
        .generic_filter_required = cfg.filter_expr != null,
        .scan_threads = n_workers,
        .total_scan_rgs = claim_total_rgs,
        .local_reserve_per_bucket = local_reserve_per_bucket,
        .route_block_rows = route_block_rows,
        .direct_final_local = direct_final_local,
        .local_parts = parts,
        .shared_scan_buffers = shared_scan_buffers_ptr,
    };
    raw_queues_moved_to_shared = true;
    var heavy_teardown_scheduled = false;
    defer if (!heavy_teardown_scheduled) deinitRawQueues(&shared);
    if (use_dedicated_raw_stage) {
        for (shared.raw_scan_queues) |*queue| try queue.chunks.ensureTotalCapacity(allocator, 8);
        for (shared.raw_group_queues) |*queue| try queue.chunks.ensureTotalCapacity(allocator, 8);
    }
    var i: usize = 0;
    const worker_setup_t0 = if ((PROFILING and cfg.trace_timing)) nowTicks() else 0;
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
            const stage_scratch_rows = if (cfg.shared_stage_builders) raw_chunk_rows * @max(@as(usize, 1), @min(raw_batch_chunks, MAX_RAW_BATCH_CHUNKS)) else raw_chunk_rows;
            try parts[i].flat_raw_rows.ensureTotalCapacity(allocator, cfg.group_rows_layout, stage_scratch_rows);
            try parts[i].flat_bucket_ids.ensureTotalCapacity(allocator, stage_scratch_rows);
        }
        scans[i] = try Scan.allocWithProjectionLoc(table.allocator, table, null, scan_columns, cfg.group_rows_layout.has_rowref, snap);
        // Weighted integer-key programs consume RLE run headers directly (the
        // run-native emitter); hashed-key layouts pack from digests, where the
        // sidecar has no consumer.
        scans[i].emit_runs = cfg.group_rows_layout.has_weight and !cfg.group_rows_layout.has_rowref;
        {
            var sq = thindb.exec.makeQuery(table.allocator, scans[i]);
            drives[i] = if (cfg.derived.len == 0) sq else sq.compute(cfg.derived) catch |e| {
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
        for (cfg.hash_key_columns) |hc| _ = scans[i].setHashKeyColumn(hc);
        scans[i].setRange(0, 0, 0, 0, false);
    }
    const worker_setup_ticks = if ((PROFILING and cfg.trace_timing)) nowTicks() - worker_setup_t0 else 0;
    snap.memtable_snap.release();
    pin_held = false;

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

    const setup_ticks = nowTicks() - function_t0;
    if ((PROFILING and cfg.trace_timing)) {
        std.debug.print("[harness-core-setup] query={s} total={d:.1}ms snapshot_stats={d:.1}ms pipe_buckets={d:.1}ms worker_parts_scans={d:.1}ms other={d:.1}ms\n", .{
            "generic",
            ticksToMs(setup_ticks, freq),
            ticksToMs(snapshot_setup_ticks, freq),
            ticksToMs(bucket_setup_ticks, freq),
            ticksToMs(worker_setup_ticks, freq),
            ticksToMs(setup_ticks - snapshot_setup_ticks - bucket_setup_ticks - worker_setup_ticks, freq),
        });
    }
    const total_t0 = nowTicks();
    var threads = try allocator.alloc(std.Thread, n_workers);
    defer allocator.free(threads);
    var errs = try allocator.alloc(?anyerror, n_workers);
    defer allocator.free(errs);
    @memset(errs, null);

    i = 0;
    while (i < n_workers) : (i += 1) {
        threads[i] = try std.Thread.spawn(.{}, siloGridWorker, .{SiloGridJob{
            .scan = scans[i],
            .drive = &drives[i],
            .local = &parts[i],
            .shared = &shared,
            .seg_start = seg_start,
            .segment_count = snap.segment_count,
            .worker_index = i,
            .worker_count = n_workers,
            .chunk_rows = chunk_rows,
            .scan_tile_rgs = scan_tile_rgs,
            .scan_coalesce_tiles = scan_coalesce_tiles,
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
        }});
    }
    i = 0;
    while (i < n_workers) : (i += 1) threads[i].join();
    const worker_wall_ticks = nowTicks() - total_t0;
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

    const final_t0 = nowTicks();
    var top = try TopSet.init(allocator, top_k_eff);
    defer top.deinit(allocator);
    var group_count: u64 = 0;
    var grouped_rows: u64 = 0;
    for (buckets) |*bucket| {
        group_count += bucket.states.len;
        grouped_rows += bucket.row_count;
    }
    for (worker_tops) |worker_top| {
        for (worker_top.items[0..worker_top.len]) |candidate| top.consider(candidate);
    }
    std.mem.sort(TopRow, top.items[0..top.len], {}, topLess);
    const final_merge_ticks = nowTicks() - final_t0;
    const total_ticks = nowTicks() - total_t0;
    const function_ticks = nowTicks() - function_t0;
    if (cfg.result_out) |out| {
        const has_str_out = cfg.group_rows_layout.has_str_payload;
        if (cfg.result_all_groups) {
            const cap = cfg.result_all_groups_cap;
            const filter = cfg.result_emit_filter;
            emit_all: for (buckets) |*bucket| {
                if (cap != 0 and out.items.len >= cap) break;
                if (has_str_out) {
                    var gid: usize = 0;
                    while (gid < bucket.states.len) : (gid += 1) {
                        if (cap != 0 and out.items.len >= cap) break :emit_all;
                        var row = topRowFromStateStr(bucket.states.ref(gid), bucket.str_states.items[gid]);
                        if (filter) |f| if (!f.pass(f.ctx, row)) continue;
                        try ownTopRowStr(&row, allocator);
                        try out.append(allocator, row);
                    }
                } else {
                    var gid: usize = 0;
                    while (gid < bucket.states.len) : (gid += 1) {
                        if (cap != 0 and out.items.len >= cap) break :emit_all;
                        const row = topRowFromState(bucket.states.ref(gid));
                        if (filter) |f| if (!f.pass(f.ctx, row)) continue;
                        try out.append(allocator, row);
                    }
                }
            }
        } else {
            // The merged top-N rows borrow each survivor's bucket str bytes;
            // re-dup into the result allocator so they survive bucket teardown.
            for (top.items[0..top.len]) |cand| {
                var row = cand;
                if (has_str_out) try ownTopRowStr(&row, allocator);
                try out.append(allocator, row);
            }
        }
    }
    if ((PROFILING and cfg.trace_timing)) {
        std.debug.print(
            "[harness-core-timing] query={s} full={d:.1}ms setup_before_workers={d:.1}ms worker_and_final={d:.1}ms final_merge={d:.3}ms result_rows={d}\n",
            .{
                "generic",
                ticksToMs(function_ticks, freq),
                ticksToMs(setup_ticks, freq),
                ticksToMs(total_ticks, freq),
                ticksToMs(final_merge_ticks, freq),
                top.len,
            },
        );
    }
    pre_return_ticks = nowTicks();
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
                ticksToMs(total_ticks, freq),
                ticksToMs(worker_wall_ticks, freq),
                ticksToMs(final_merge_ticks, freq),
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
                ticksToMs(total_ticks, freq),
                n_workers,
                ticksToMs(worker_wall_ticks, freq),
                ticksToMs(final_merge_ticks, freq),
                cfg.group_lease_buckets,
                cfg.group_lease_rows,
            },
        );
        std.debug.print(
            "[clientip-silo-grid-stages] query={s} scan_decode_cpu={d:.1}ms scan_reset_cpu={d:.3}ms route_partition_cpu={d:.1}ms raw_stage_cpu={d:.1}ms publish_queue_cpu={d:.1}ms aggregate_cpu={d:.1}ms idle_cpu={d:.1}ms local_topn_cpu={d:.1}ms final_merge_wall={d:.3}ms chunks={d} rows_per_chunk={d:.1} scan_ranges={d} scan_quanta={d} scan_batches={d} segments_opened={d} fused_scans={d}/{d} scan_tile_rgs={d} scan_coalesce_tiles={d} route_block_rows={d} group_lease_buckets={d} group_lease_rows={d} final_scan_queue_rows={d} final_stage_queue_rows={d} final_scan_buffered_rows={d} active_scan_jobs={d} active_stage_jobs={d} active_group_jobs={d}\n",
            .{
                "generic",
                ticksToMs(scan_cpu_ticks, freq),
                ticksToMs(scan_reset_ticks, freq),
                ticksToMs(partition_cpu_ticks, freq),
                ticksToMs(raw_stage_ticks, freq),
                ticksToMs(publish_cpu_ticks, freq),
                ticksToMs(group_cpu_ticks, freq),
                ticksToMs(idle_cpu_ticks, freq),
                ticksToMs(top_cpu_ticks, freq),
                ticksToMs(final_merge_ticks, freq),
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
                shared.outstanding_rows.load(.acquire),
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
                ticksToMs(sched_cpu_ticks, freq),
                ticksToMs(sched_decision_ticks, freq),
                ticksToMs(sched_scan_claim_ticks, freq),
                ticksToMs(sched_group_pick_ticks, freq),
                ticksToMs(sched_group_lock_ticks, freq),
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
                ticksToMs(raw_stage_pop_ticks, freq),
                ticksToMs(raw_stage_ticks, freq),
                ticksToMs(raw_stage_slice_ticks, freq),
                ticksToMs(raw_stage_publish_ticks, freq),
                ticksToMs(raw_stage_recycle_ticks, freq),
                ticksToMs(raw_stage_pop_ticks + raw_stage_ticks + raw_stage_slice_ticks + raw_stage_publish_ticks + raw_stage_recycle_ticks, freq),
                raw_stage_input_chunks,
                if (sched_stage_jobs == 0) 0.0 else @as(f64, @floatFromInt(raw_stage_input_chunks)) / @as(f64, @floatFromInt(sched_stage_jobs)),
            },
        );
        std.debug.print(
            "[clientip-raw-contention] query={s} raw_queue_lock={d:.3}ms raw_scan_queue_lock={d:.3}ms raw_group_queue_lock={d:.3}ms raw_recycle_lock={d:.3}ms stage_builder_lock={d:.3}ms central_group_lock={d:.3}ms total_raw_lock={d:.3}ms\n",
            .{
                "generic",
                ticksToMs(raw_queue_lock_ticks, freq),
                ticksToMs(raw_scan_queue_lock_ticks, freq),
                ticksToMs(raw_group_queue_lock_ticks, freq),
                ticksToMs(raw_recycle_lock_ticks, freq),
                ticksToMs(raw_stage_builder_lock_ticks, freq),
                ticksToMs(raw_agg_lock_ticks, freq),
                ticksToMs(raw_queue_lock_ticks + raw_scan_queue_lock_ticks + raw_group_queue_lock_ticks + raw_recycle_lock_ticks + raw_stage_builder_lock_ticks + raw_agg_lock_ticks, freq),
            },
        );
        std.debug.print("[clientip-silo-grid-workers] query={s} aggregate_cpu_by_worker_ms=", .{"generic"});
        for (group_ticks, 0..) |ticks, worker_idx| {
            if (worker_idx != 0) std.debug.print(",", .{});
            std.debug.print("{d:.1}", .{ticksToMs(ticks, freq)});
        }
        std.debug.print(" chunks_by_worker=", .{});
        for (chunks, 0..) |chunk_count, worker_idx| {
            if (worker_idx != 0) std.debug.print(",", .{});
            std.debug.print("{d}", .{chunk_count});
        }
        std.debug.print("\n", .{});
    }
}
