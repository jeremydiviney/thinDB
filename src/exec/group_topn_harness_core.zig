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

const thindb = struct {
    pub const exec = exec_mod;
    pub const api = api_mod;
    pub const storage = storage_mod;
    pub const types = types_mod;
    pub const Batch = exec_mod.Batch;
    pub const Predicate = exec_mod.Predicate;
};

const Allocator = std.mem.Allocator;
const Scan = thindb.exec.Scan;
const GroupTable = thindb.exec.group_table.IntKeyMemsetTable(96);
const Q30InlineTable = thindb.exec.group_table.InlineSlotTable(u64, Q30InlineState);

const COLS_CLIENTIP = [_][]const u8{ "ClientIP", "IsRefresh", "ResolutionWidth" };
const COLS_USERID = [_][]const u8{"UserID"};
const COLS_Q30 = [_][]const u8{ "SearchEngineID", "ClientIP", "IsRefresh", "ResolutionWidth", "SearchPhrase" };
const COLS_Q31 = [_][]const u8{ "WatchID", "ClientIP", "IsRefresh", "ResolutionWidth", "SearchPhrase" };
const COLS_Q32 = [_][]const u8{ "WatchID", "ClientIP", "IsRefresh", "ResolutionWidth" };
const COLS_PHRASE = [_][]const u8{"SearchPhrase"};
const COLS_Q30_PAYLOAD = [_][]const u8{ "SearchEngineID", "ClientIP", "IsRefresh", "ResolutionWidth" };
const COLS_Q31_PAYLOAD = [_][]const u8{ "WatchID", "ClientIP", "IsRefresh", "ResolutionWidth" };
const TOP_K: usize = 10;
const PREFETCH_DIST_DIRECT: usize = 48;
const PREFETCH_DIST_BUCKET: usize = 32;
const PIPE_CHUNK_ROWS: usize = 8192;
const ELASTIC_BACKLOG_CHUNKS: usize = 2048;
const GRID_CHUNK_ROWS: usize = 1024;
const GRID_SCAN_TILE_RGS: usize = 16;
const GRID_SCAN_COALESCE_TILES: usize = 1;
const GRID_SCAN_YIELD_CHUNKS: usize = 16384;
const LOCAL_PREAGG_FLUSH_GROUPS: usize = 131072;
const DEFAULT_ROUTE_BLOCK_ROWS: usize = 2048;
const MAX_ROUTE_BLOCK_ROWS: usize = 2048;
const AUTO_ROUTE_BLOCK_ROWS: usize = 0;
const MAX_GROUP_LEASE_BUCKETS: usize = 64;
const MAX_WORKSPACE_PROFILE_WORKERS: usize = 128;
const MAX_RAW_BATCH_CHUNKS: usize = 64;
const MAX_GENERIC_GROUP_KEYS: usize = 8;

pub const QueryKind = enum {
    clientip,
    q15,
    q30,
    q31,
    q32,
    generic,

    pub fn label(self: QueryKind) []const u8 {
        return switch (self) {
            .clientip => "clientip",
            .q15 => "q15",
            .q30 => "q30",
            .q31 => "q31",
            .q32 => "q32",
            .generic => "generic",
        };
    }

    fn columns(self: QueryKind) []const []const u8 {
        return querySpec(self).columns;
    }

    fn payloadColumns(self: QueryKind) []const []const u8 {
        return querySpec(self).payload_columns orelse querySpec(self).columns;
    }

    fn groupKeyColumnCount(self: QueryKind) usize {
        return querySpec(self).group_key_columns;
    }

    fn hasFilter(self: QueryKind) bool {
        return querySpec(self).filter != null;
    }
};

const QuerySpec = struct {
    columns: []const []const u8,
    payload_columns: ?[]const []const u8 = null,
    group_key_columns: usize,
    filter: ?thindb.Predicate = null,
};

fn querySpec(kind: QueryKind) QuerySpec {
    const phrase_non_empty = searchPhrasePred();
    return switch (kind) {
        .clientip => .{ .columns = &COLS_CLIENTIP, .group_key_columns = 1 },
        .q15 => .{ .columns = &COLS_USERID, .group_key_columns = 1 },
        .q30 => .{ .columns = &COLS_Q30, .payload_columns = &COLS_Q30_PAYLOAD, .group_key_columns = 2, .filter = phrase_non_empty },
        .q31 => .{ .columns = &COLS_Q31, .payload_columns = &COLS_Q31_PAYLOAD, .group_key_columns = 2, .filter = phrase_non_empty },
        .q32 => .{ .columns = &COLS_Q32, .group_key_columns = 2 },
        .generic => .{ .columns = &.{}, .group_key_columns = 0 },
    };
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

fn pinToCpu(cpu: usize) void {
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

const Row = extern struct {
    key_lo: u64,
    key_hi: u32,
    refresh: i16,
    width: i16,
};

inline fn makeRow(key: u128, refresh: i16, width: i16) Row {
    return .{
        .key_lo = @truncate(key),
        .key_hi = @truncate(key >> 64),
        .refresh = refresh,
        .width = width,
    };
}

inline fn stagedRowKey(row: Row) u128 {
    return @as(u128, row.key_lo) | (@as(u128, row.key_hi) << 64);
}

const RawRows = struct {
    slab: []align(16) u8 = &.{},
    len_rows: usize = 0,
    capacity_rows: usize = 0,
    layout: GroupRowsLayout = .{},

    inline fn len(self: RawRows) usize {
        return self.len_rows;
    }

    inline fn capacity(self: RawRows) usize {
        return self.capacity_rows;
    }

    fn slabBytes(layout: GroupRowsLayout, capacity_rows: usize) usize {
        var bytes: usize = 0;
        bytes = alignForward(bytes, @alignOf(u64));
        bytes += @sizeOf(u64) * capacity_rows;
        bytes = alignForward(bytes, @alignOf(u32));
        bytes += @sizeOf(u32) * capacity_rows;
        for (layout.columns) |column| {
            bytes = alignForward(bytes, groupColumnAlign(column.physical_type));
            bytes += groupColumnSize(column.physical_type) * capacity_rows;
        }
        return bytes;
    }

    inline fn keyLoOffset(_: RawRows) usize {
        return 0;
    }

    inline fn keyHiOffset(self: RawRows) usize {
        return self.capacity_rows * @sizeOf(u64);
    }

    fn columnOffset(self: RawRows, column_index: usize) usize {
        var offset = self.keyHiOffset() + self.capacity_rows * @sizeOf(u32);
        var i: usize = 0;
        while (i < column_index) : (i += 1) {
            offset = alignForward(offset, groupColumnAlign(self.layout.columns[i].physical_type));
            offset += groupColumnSize(self.layout.columns[i].physical_type) * self.capacity_rows;
        }
        return alignForward(offset, groupColumnAlign(self.layout.columns[column_index].physical_type));
    }

    inline fn keyLoAll(self: RawRows) []u64 {
        return @as([*]u64, @ptrCast(@alignCast(self.slab.ptr + self.keyLoOffset())))[0..self.capacity_rows];
    }

    inline fn keyHiAll(self: RawRows) []u32 {
        return @as([*]u32, @ptrCast(@alignCast(self.slab.ptr + self.keyHiOffset())))[0..self.capacity_rows];
    }

    inline fn columnI16All(self: RawRows, column_index: usize) []i16 {
        std.debug.assert(self.layout.columns[column_index].physical_type == .i16);
        return @as([*]i16, @ptrCast(@alignCast(self.slab.ptr + self.columnOffset(column_index))))[0..self.capacity_rows];
    }

    inline fn keyLoItems(self: RawRows) []u64 {
        return self.keyLoAll()[0..self.len_rows];
    }

    inline fn keyHiItems(self: RawRows) []u32 {
        return self.keyHiAll()[0..self.len_rows];
    }

    inline fn columnI16Items(self: RawRows, column_index: usize) []i16 {
        return self.columnI16All(column_index)[0..self.len_rows];
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
            @memcpy(next.keyLoItems(), self.keyLoItems());
            @memcpy(next.keyHiItems(), self.keyHiItems());
            var col: usize = 0;
            while (col < layout.columns.len) : (col += 1) {
                switch (layout.columns[col].physical_type) {
                    .i16 => @memcpy(next.columnI16Items(col), self.columnI16Items(col)),
                }
            }
        }
        allocator.free(self.slab);
        self.* = next;
    }

    fn resize(self: *RawRows, allocator: Allocator, layout: GroupRowsLayout, new_len: usize) !void {
        try self.ensureTotalCapacity(allocator, layout, new_len);
        self.len_rows = new_len;
    }

    fn clearRetainingCapacity(self: *RawRows) void {
        self.len_rows = 0;
    }

    fn deinit(self: *RawRows, allocator: Allocator) void {
        allocator.free(self.slab);
        self.* = .{};
    }

    inline fn appendAssumeCapacity(self: *RawRows, key: u128, refresh: i16, width: i16) void {
        const idx = self.len_rows;
        self.keyLoAll()[idx] = @truncate(key);
        self.keyHiAll()[idx] = @truncate(key >> 64);
        self.columnI16All(0)[idx] = refresh;
        self.columnI16All(1)[idx] = width;
        self.len_rows = idx + 1;
    }

    fn appendRow(self: *RawRows, allocator: Allocator, row: Row) !void {
        if (self.len_rows == self.capacity_rows) {
            const next_capacity = if (self.capacity_rows == 0) 8 else self.capacity_rows * 2;
            try self.ensureTotalCapacity(allocator, self.layout, next_capacity);
        }
        self.appendAssumeCapacity(stagedRowKey(row), row.refresh, row.width);
    }

    fn appendRows(self: *RawRows, allocator: Allocator, rows: []const Row) !void {
        const old_len = self.len();
        const new_len = old_len + rows.len;
        try self.resize(allocator, self.layout, new_len);
        const key_lo = self.keyLoAll();
        const key_hi = self.keyHiAll();
        const refresh = self.columnI16All(0);
        const width = self.columnI16All(1);
        var i: usize = 0;
        while (i < rows.len) : (i += 1) {
            key_lo[old_len + i] = rows[i].key_lo;
            key_hi[old_len + i] = rows[i].key_hi;
            refresh[old_len + i] = rows[i].refresh;
            width[old_len + i] = rows[i].width;
        }
    }

    inline fn rowAt(self: RawRows, idx: usize) Row {
        return .{
            .key_lo = self.keyLoAll()[idx],
            .key_hi = self.keyHiAll()[idx],
            .refresh = self.columnI16All(0)[idx],
            .width = self.columnI16All(1)[idx],
        };
    }

    inline fn keyAt(self: RawRows, idx: usize) u128 {
        return @as(u128, self.keyLoAll()[idx]) | (@as(u128, self.keyHiAll()[idx]) << 64);
    }

    inline fn refreshAt(self: RawRows, idx: usize) i16 {
        return self.columnI16All(0)[idx];
    }

    inline fn widthAt(self: RawRows, idx: usize) i16 {
        return self.columnI16All(1)[idx];
    }
};

pub const GroupKeyWidth = enum {
    u32,
    u64,
    u96,
    u128,
};

pub const GroupColumnType = enum {
    i16,
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
};

pub const GroupAggregateSpec = struct {
    op: GroupAggregateOp,
    input_column_index: ?u16 = null,
    state_index: u16,
};

const MAX_GROUP_AGG_STATES: usize = 4;
const MAX_GROUP_PAYLOAD_COLUMNS: usize = 8;

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

    const DEFAULT_GROUP_COLUMNS = [_]GroupColumnSpec{
        .{ .physical_type = .i16, .source = .is_refresh },
        .{ .physical_type = .i16, .source = .resolution_width },
    };
};

fn sameRowsLayout(a: GroupRowsLayout, b: GroupRowsLayout) bool {
    if (a.key_width != b.key_width or a.key_columns.len != b.key_columns.len or a.columns.len != b.columns.len or a.aggregates.len != b.aggregates.len) return false;
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
            a.aggregates[i].state_index != b.aggregates[i].state_index)
        {
            return false;
        }
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
        .i16 => @sizeOf(i16),
    };
}

fn groupColumnAlign(typ: GroupColumnType) usize {
    return switch (typ) {
        .i16 => @alignOf(i16),
    };
}

const GroupRows = struct {
    slab: []align(16) u8 = &.{},
    len_rows: usize = 0,
    capacity_rows: usize = 0,
    layout: GroupRowsLayout = .{},

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
        return bytes;
    }

    inline fn keyOffset(_: GroupRows) usize {
        return 0;
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

    inline fn columnI16All(self: GroupRows, column_index: usize) []i16 {
        std.debug.assert(self.layout.columns[column_index].physical_type == .i16);
        return @as([*]i16, @ptrCast(@alignCast(self.slab.ptr + self.columnOffset(column_index))))[0..self.capacity_rows];
    }

    inline fn columnI16Items(self: GroupRows, column_index: usize) []i16 {
        return self.columnI16All(column_index)[0..self.len_rows];
    }

    inline fn refreshAll(self: GroupRows) []i16 {
        return self.columnI16All(0);
    }

    inline fn widthAll(self: GroupRows) []i16 {
        return self.columnI16All(1);
    }

    inline fn refreshItems(self: GroupRows) []i16 {
        return self.refreshAll()[0..self.len_rows];
    }

    inline fn widthItems(self: GroupRows) []i16 {
        return self.widthAll()[0..self.len_rows];
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
                switch (layout.columns[col].physical_type) {
                    .i16 => @memcpy(next.columnI16Items(col), self.columnI16Items(col)),
                }
            }
        }
        allocator.free(self.slab);
        self.* = next;
    }

    fn resize(self: *GroupRows, allocator: Allocator, layout: GroupRowsLayout, new_len: usize) !void {
        try self.ensureTotalCapacity(allocator, layout, new_len);
        self.len_rows = new_len;
    }

    fn clearRetainingCapacity(self: *GroupRows) void {
        self.len_rows = 0;
    }

    fn deinit(self: *GroupRows, allocator: Allocator) void {
        allocator.free(self.slab);
        self.* = .{};
    }

    inline fn appendAssumeCapacity(self: *GroupRows, key: u128, refresh: i16, width: i16) void {
        const idx = self.len_rows;
        self.setKey(idx, key);
        self.refreshAll()[idx] = refresh;
        self.widthAll()[idx] = width;
        self.len_rows = idx + 1;
    }

    fn appendRows(self: *GroupRows, allocator: Allocator, layout: GroupRowsLayout, rows: []const Row) !void {
        const old_len = self.len();
        const new_len = old_len + rows.len;
        try self.resize(allocator, layout, new_len);
        const refresh = self.refreshAll();
        const width = self.widthAll();
        switch (self.layout.key_width) {
            .u32 => {
                const key_dst = self.keyU32All();
                var i: usize = 0;
                while (i < rows.len) : (i += 1) {
                    key_dst[old_len + i] = @truncate(rows[i].key_lo);
                    refresh[old_len + i] = rows[i].refresh;
                    width[old_len + i] = rows[i].width;
                }
            },
            .u64 => {
                const key_dst = self.keyU64All();
                var i: usize = 0;
                while (i < rows.len) : (i += 1) {
                    key_dst[old_len + i] = rows[i].key_lo;
                    refresh[old_len + i] = rows[i].refresh;
                    width[old_len + i] = rows[i].width;
                }
            },
            .u96 => {
                const key_lo_dst = self.keyU96LoAll();
                const key_hi_dst = self.keyU96HiAll();
                var i: usize = 0;
                while (i < rows.len) : (i += 1) {
                    key_lo_dst[old_len + i] = rows[i].key_lo;
                    key_hi_dst[old_len + i] = rows[i].key_hi;
                    refresh[old_len + i] = rows[i].refresh;
                    width[old_len + i] = rows[i].width;
                }
            },
            .u128 => {
                const key_dst = self.keyU128All();
                var i: usize = 0;
                while (i < rows.len) : (i += 1) {
                    key_dst[old_len + i] = stagedRowKey(rows[i]);
                    refresh[old_len + i] = rows[i].refresh;
                    width[old_len + i] = rows[i].width;
                }
            },
        }
    }

    fn appendRawRowsSlice(self: *GroupRows, allocator: Allocator, layout: GroupRowsLayout, rows: RawRows, start: usize, count: usize) !void {
        if (count == 0) return;
        const old_len = self.len();
        const new_len = old_len + count;
        try self.resize(allocator, layout, new_len);

        const src_key_lo = rows.keyLoAll()[start .. start + count];
        const src_key_hi = rows.keyHiAll()[start .. start + count];
        switch (self.layout.key_width) {
            .u32 => {
                const dst = self.keyU32All()[old_len..new_len];
                var i: usize = 0;
                while (i < count) : (i += 1) dst[i] = @truncate(src_key_lo[i]);
            },
            .u64 => @memcpy(self.keyU64All()[old_len..new_len], src_key_lo),
            .u96 => {
                @memcpy(self.keyU96LoAll()[old_len..new_len], src_key_lo);
                @memcpy(self.keyU96HiAll()[old_len..new_len], src_key_hi);
            },
            .u128 => {
                const dst = self.keyU128All()[old_len..new_len];
                var i: usize = 0;
                while (i < count) : (i += 1) dst[i] = @as(u128, src_key_lo[i]) | (@as(u128, src_key_hi[i]) << 64);
            },
        }
        var col: usize = 0;
        while (col < self.layout.columns.len) : (col += 1) {
            switch (self.layout.columns[col].physical_type) {
                .i16 => @memcpy(self.columnI16All(col)[old_len..new_len], rows.columnI16All(col)[start .. start + count]),
            }
        }
    }

    inline fn rowAt(self: GroupRows, idx: usize) Row {
        const key = self.keyAt(idx);
        return .{
            .key_lo = @truncate(key),
            .key_hi = @truncate(key >> 64),
            .refresh = self.refreshAll()[idx],
            .width = self.widthAll()[idx],
        };
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

    inline fn refreshAt(self: GroupRows, idx: usize) i16 {
        return self.refreshAll()[idx];
    }

    inline fn widthAt(self: GroupRows, idx: usize) i16 {
        return self.widthAll()[idx];
    }
};

const PartBucket = struct {
    rows: std.ArrayListUnmanaged(Row) = .empty,

    fn deinit(self: *PartBucket, allocator: Allocator) void {
        self.rows.deinit(allocator);
        self.* = .{};
    }
};

const SharedScanBucket = struct {
    lock: std.atomic.Mutex = .unlocked,
    rows: std.ArrayListUnmanaged(Row) = .empty,

    fn deinit(self: *SharedScanBucket, allocator: Allocator) void {
        self.rows.deinit(allocator);
        self.* = .{};
    }
};

const SharedScanBuffers = struct {
    buckets: []SharedScanBucket = &.{},
    bucket_count: usize = 0,
    bank_count: usize = 0,

    fn init(allocator: Allocator, bucket_count: usize, bank_count_raw: usize, reserve_per_bucket: usize) !SharedScanBuffers {
        const bank_count = @max(@as(usize, 1), bank_count_raw);
        const buckets = try allocator.alloc(SharedScanBucket, bucket_count * bank_count);
        errdefer allocator.free(buckets);
        for (buckets) |*bucket| {
            bucket.* = .{};
            if (reserve_per_bucket > 0) try bucket.rows.ensureTotalCapacity(allocator, reserve_per_bucket);
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
    flat_rows: std.ArrayListUnmanaged(Row) = .empty,
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
    recycled_rows: std.ArrayListUnmanaged(std.ArrayListUnmanaged(Row)) = .empty,

    fn init(allocator: Allocator, bucket_count: usize, reserve_per_bucket: usize) !WorkerParts {
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
        if (reserve_per_bucket > 0) {
            for (buckets) |*b| try b.rows.ensureTotalCapacity(allocator, reserve_per_bucket);
        }
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
        self.flat_rows.deinit(allocator);
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
        for (self.recycled_rows.items) |*rows| rows.deinit(allocator);
        self.recycled_rows.deinit(allocator);
        self.* = .{};
    }

    fn enableFlatScanPartitions(self: *WorkerParts, allocator: Allocator, bucket_count: usize, reserve_rows: usize) !void {
        self.flat_scan_partitions = true;
        self.flat_partitioned = false;
        try self.ensureWideBucketScratch(allocator, bucket_count);
        if (reserve_rows > 0) {
            try self.flat_rows.ensureTotalCapacity(allocator, reserve_rows);
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

const State = struct {
    key: u128,
    count: u64,
    refresh_sum: i64,
    width_sum: i64,
    extra_sum: i64 = 0,
};

const Q30InlineState = struct {
    count: u64,
    refresh_sum: i64,
    width_sum: i64,
};

const GroupScratch = struct {
    gids: std.ArrayListUnmanaged(u32) = .empty,
    row_idxs: std.ArrayListUnmanaged(u32) = .empty,
    rows: std.ArrayListUnmanaged(Row) = .empty,
    states: std.ArrayListUnmanaged(State) = .empty,

    fn deinit(self: *GroupScratch, allocator: Allocator) void {
        self.gids.deinit(allocator);
        self.row_idxs.deinit(allocator);
        self.rows.deinit(allocator);
        self.states.deinit(allocator);
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

const StateBucket = struct {
    states: std.ArrayListUnmanaged(State) = .empty,

    fn deinit(self: *StateBucket, allocator: Allocator) void {
        self.states.deinit(allocator);
        self.* = .{};
    }
};

const WorkerAgg = struct {
    table: GroupTable,
    states: std.ArrayListUnmanaged(State) = .empty,
    buckets: []StateBucket = &.{},
    scratch: GroupScratch = .{},
    scanned_count: u64 = 0,
    row_count: u64 = 0,
    scan_ticks: i64 = 0,
    group_ticks: i64 = 0,
    partition_ticks: i64 = 0,
    partitioned_count: u64 = 0,

    fn init(allocator: Allocator, bucket_count: usize, expected_groups: usize) !WorkerAgg {
        const buckets = try allocator.alloc(StateBucket, bucket_count);
        for (buckets) |*b| b.* = .{};
        errdefer allocator.free(buckets);
        return .{
            .table = try GroupTable.init(allocator, expected_groups),
            .buckets = buckets,
        };
    }

    fn deinit(self: *WorkerAgg, allocator: Allocator) void {
        self.table.deinit(allocator);
        self.states.deinit(allocator);
        self.scratch.deinit(allocator);
        for (self.buckets) |*b| b.deinit(allocator);
        if (self.buckets.len > 0) allocator.free(self.buckets);
        self.* = undefined;
    }
};

const CentralBucket = struct {
    mutex: std.atomic.Mutex = .unlocked,
    table: GroupTable,
    states: std.ArrayListUnmanaged(State) = .empty,

    fn init(allocator: Allocator, expected_groups: usize) !CentralBucket {
        return .{
            .table = try GroupTable.init(allocator, expected_groups),
        };
    }

    fn deinit(self: *CentralBucket, allocator: Allocator) void {
        self.table.deinit(allocator);
        self.states.deinit(allocator);
        self.* = undefined;
    }
};

const PipeChunk = struct {
    rows: std.ArrayListUnmanaged(Row),
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
    states: std.ArrayListUnmanaged(State) = .empty,
    row_count: u64 = 0,

    fn init(allocator: Allocator, expected_groups: usize) !PipeBucket {
        return .{ .table = try GroupTable.init(allocator, expected_groups) };
    }

    fn deinit(self: *PipeBucket, allocator: Allocator) void {
        for (self.chunks.items) |*chunk| chunk.rows.deinit(allocator);
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
            try bucket.states.ensureTotalCapacity(allocator, expected_groups_per_bucket);
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
        job.buckets[b].states.ensureTotalCapacity(job.allocator, job.expected_groups_per_bucket) catch |err| {
            job.err = err;
            return;
        };
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

const WorkspacePartsInitJob = struct {
    allocator: Allocator,
    parts: []WorkerParts,
    bucket_count: usize,
    local_reserve_per_bucket: usize,
    worker_index: usize,
    worker_count: usize,
    cpu: ?usize,
    err: ?anyerror = null,
};

fn initWorkspacePartsParallel(
    allocator: Allocator,
    parts: []WorkerParts,
    bucket_count: usize,
    local_reserve_per_bucket: usize,
    n_workers: usize,
    cpus: []const usize,
) !void {
    const workers = @max(@as(usize, 1), @min(n_workers, @max(cpus.len, 1)));
    if (workers == 1 or parts.len < 2) {
        for (parts, 0..) |*p, i| {
            p.* = try WorkerParts.init(allocator, bucket_count, local_reserve_per_bucket);
            p.worker_index = i;
        }
        return;
    }

    const threads = try allocator.alloc(std.Thread, workers);
    defer allocator.free(threads);
    var jobs = try allocator.alloc(WorkspacePartsInitJob, workers);
    defer allocator.free(jobs);

    var i: usize = 0;
    while (i < workers) : (i += 1) {
        jobs[i] = .{
            .allocator = allocator,
            .parts = parts,
            .bucket_count = bucket_count,
            .local_reserve_per_bucket = local_reserve_per_bucket,
            .worker_index = i,
            .worker_count = workers,
            .cpu = if (cpus.len == 0) null else cpus[i % cpus.len],
        };
        threads[i] = try std.Thread.spawn(.{}, workspacePartsInitWorker, .{&jobs[i]});
    }
    for (threads) |thread| thread.join();
    for (jobs) |job| if (job.err) |err| return err;
}

fn workspacePartsInitWorker(job: *WorkspacePartsInitJob) void {
    if (job.cpu) |cpu| pinToCpu(cpu);
    var i = job.worker_index;
    while (i < job.parts.len) : (i += job.worker_count) {
        job.parts[i] = WorkerParts.init(job.allocator, job.bucket_count, job.local_reserve_per_bucket) catch |err| {
            job.err = err;
            return;
        };
        job.parts[i].worker_index = i;
    }
}

const WorkspaceBucketsInitJob = struct {
    allocator: Allocator,
    buckets: []PipeBucket,
    inited: []bool,
    expected_groups_per_bucket: usize,
    worker_index: usize,
    worker_count: usize,
    cpu: ?usize,
    err: ?anyerror = null,
};

fn initWorkspaceBucketsParallel(
    allocator: Allocator,
    buckets: []PipeBucket,
    inited: []bool,
    expected_groups_per_bucket: usize,
    n_workers: usize,
    cpus: []const usize,
) !void {
    const workers = @max(@as(usize, 1), @min(n_workers, @max(cpus.len, 1)));
    if (workers == 1 or buckets.len < 128) {
        for (buckets, 0..) |*bucket, i| {
            bucket.* = try PipeBucket.init(allocator, expected_groups_per_bucket);
            inited[i] = true;
            try bucket.states.ensureTotalCapacity(allocator, expected_groups_per_bucket);
            try bucket.chunks.ensureTotalCapacity(allocator, 8);
        }
        return;
    }

    const threads = try allocator.alloc(std.Thread, workers);
    defer allocator.free(threads);
    var jobs = try allocator.alloc(WorkspaceBucketsInitJob, workers);
    defer allocator.free(jobs);

    var i: usize = 0;
    while (i < workers) : (i += 1) {
        jobs[i] = .{
            .allocator = allocator,
            .buckets = buckets,
            .inited = inited,
            .expected_groups_per_bucket = expected_groups_per_bucket,
            .worker_index = i,
            .worker_count = workers,
            .cpu = if (cpus.len == 0) null else cpus[i % cpus.len],
        };
        threads[i] = try std.Thread.spawn(.{}, workspaceBucketsInitWorker, .{&jobs[i]});
    }
    for (threads) |thread| thread.join();
    for (jobs) |job| if (job.err) |err| return err;
}

fn workspaceBucketsInitWorker(job: *WorkspaceBucketsInitJob) void {
    if (job.cpu) |cpu| pinToCpu(cpu);
    const bucket_count = job.buckets.len;
    const start = job.worker_index * bucket_count / job.worker_count;
    const end = (job.worker_index + 1) * bucket_count / job.worker_count;
    var i = start;
    while (i < end) : (i += 1) {
        job.buckets[i] = PipeBucket.init(job.allocator, job.expected_groups_per_bucket) catch |err| {
            job.err = err;
            return;
        };
        job.inited[i] = true;
        job.buckets[i].states.ensureTotalCapacity(job.allocator, job.expected_groups_per_bucket) catch |err| {
            job.err = err;
            return;
        };
        job.buckets[i].chunks.ensureTotalCapacity(job.allocator, 8) catch |err| {
            job.err = err;
            return;
        };
    }
}

fn resetWorkerParts(parts: *WorkerParts, worker_index: usize) void {
    for (parts.buckets) |*bucket| bucket.rows.clearRetainingCapacity();
    parts.shared_buffers = null;
    parts.shared_bank_index = 0;
    parts.flat_scan_partitions = false;
    parts.flat_partitioned = false;
    parts.flat_rows.clearRetainingCapacity();
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
    for (parts.recycled_rows.items) |*rows| rows.clearRetainingCapacity();
}

fn resetPipeBucket(bucket: *PipeBucket, allocator: Allocator) void {
    for (bucket.chunks.items) |*chunk| chunk.rows.deinit(allocator);
    bucket.chunks.clearRetainingCapacity();
    bucket.queued_rows = 0;
    bucket.row_count = 0;
    bucket.queue_lock = .unlocked;
    bucket.agg_lock = .unlocked;
    bucket.table.clearRetainingCapacity();
    bucket.states.clearRetainingCapacity();
}

fn resetRawQueues(shared: *PipeShared) void {
    shared.raw_chunks.clearRetainingCapacity();
    for (shared.raw_scan_queues) |*queue| {
        queue.chunks.clearRetainingCapacity();
        queue.queued_rows = 0;
        queue.queued_rows_atomic.store(0, .release);
        queue.queued_chunks_atomic.store(0, .release);
        queue.lease.store(false, .release);
        queue.scan_lease.store(false, .release);
        queue.lock = .unlocked;
    }
    for (shared.raw_group_queues) |*queue| {
        queue.chunks.clearRetainingCapacity();
        queue.queued_rows = 0;
        queue.queued_rows_atomic.store(0, .release);
        queue.queued_chunks_atomic.store(0, .release);
        queue.lease.store(false, .release);
        queue.scan_lease.store(false, .release);
        queue.lock = .unlocked;
    }
    for (shared.stage_builders) |*builder| {
        builder.rows.clearRetainingCapacity();
        builder.lock = .unlocked;
    }
    for (shared.raw_recycled_rows.items) |*rows| rows.clearRetainingCapacity();
    for (shared.group_recycled_rows.items) |*rows| rows.clearRetainingCapacity();
    shared.raw_queue_lock = .unlocked;
    shared.raw_recycle_lock = .unlocked;
    shared.stage_outstanding_chunks.store(0, .release);
    shared.stage_outstanding_rows.store(0, .release);
    shared.stage_builder_rows.store(0, .release);
    shared.active_stage_jobs.store(0, .release);
}

fn deinitRawQueues(shared: *PipeShared) void {
    for (shared.raw_chunks.items) |*chunk| chunk.rows.deinit(shared.allocator);
    shared.raw_chunks.deinit(shared.allocator);
    for (shared.raw_scan_queues) |*queue| queue.deinit(shared.allocator);
    if (shared.raw_scan_queues.len > 0) shared.allocator.free(shared.raw_scan_queues);
    for (shared.raw_group_queues) |*queue| queue.deinit(shared.allocator);
    if (shared.raw_group_queues.len > 0) shared.allocator.free(shared.raw_group_queues);
    for (shared.stage_builders) |*builder| builder.deinit(shared.allocator);
    if (shared.stage_builders.len > 0) shared.allocator.free(shared.stage_builders);
    for (shared.raw_recycled_rows.items) |*rows| rows.deinit(shared.allocator);
    shared.raw_recycled_rows.deinit(shared.allocator);
    for (shared.group_recycled_rows.items) |*rows| rows.deinit(shared.allocator);
    shared.group_recycled_rows.deinit(shared.allocator);
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

fn publishRawChunkToQueue(
    shared: *PipeShared,
    queue: *RawQueue,
    chunk: RawChunk,
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

fn popRawChunk(shared: *PipeShared, queue_lock_ticks: ?*i64) ?RawChunk {
    const lock_t0 = if (queue_lock_ticks != null) nowTicks() else 0;
    lockSpin(&shared.raw_queue_lock);
    if (queue_lock_ticks) |ticks| ticks.* += nowTicks() - lock_t0;
    defer shared.raw_queue_lock.unlock();
    const len = shared.raw_chunks.items.len;
    if (len == 0) return null;
    const chunk = shared.raw_chunks.items[len - 1];
    shared.raw_chunks.items.len = len - 1;
    return chunk;
}

fn popRawChunkBatch(shared: *PipeShared, out: *[MAX_RAW_BATCH_CHUNKS]RawChunk, max_chunks_raw: usize, queue_lock_ticks: ?*i64) usize {
    const max_chunks = @max(@as(usize, 1), @min(max_chunks_raw, MAX_RAW_BATCH_CHUNKS));
    const lock_t0 = if (queue_lock_ticks != null) nowTicks() else 0;
    lockSpin(&shared.raw_queue_lock);
    if (queue_lock_ticks) |ticks| ticks.* += nowTicks() - lock_t0;
    defer shared.raw_queue_lock.unlock();

    var popped: usize = 0;
    while (popped < max_chunks and shared.raw_chunks.items.len > 0) : (popped += 1) {
        const idx = shared.raw_chunks.items.len - 1;
        out[popped] = shared.raw_chunks.items[idx];
        shared.raw_chunks.items.len = idx;
    }
    return popped;
}

fn popRawChunkFromQueue(queue: *RawQueue, queue_lock_ticks: ?*i64) ?RawChunk {
    const lock_t0 = if (queue_lock_ticks != null) nowTicks() else 0;
    lockSpin(&queue.lock);
    if (queue_lock_ticks) |ticks| ticks.* += nowTicks() - lock_t0;
    defer queue.lock.unlock();
    const len = queue.chunks.items.len;
    if (len == 0) return null;
    const chunk = queue.chunks.items[len - 1];
    queue.chunks.items.len = len - 1;
    queue.queued_rows -= chunk.rows.len();
    _ = queue.queued_rows_atomic.fetchSub(@intCast(chunk.rows.len()), .release);
    _ = queue.queued_chunks_atomic.fetchSub(1, .release);
    return chunk;
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

fn popRawScanBatch(shared: *PipeShared, worker_index: usize, out: *[MAX_RAW_BATCH_CHUNKS]RawChunk, max_chunks_raw: usize, queue_lock_ticks: ?*i64) usize {
    if (shared.raw_scan_queues.len == 0) return 0;
    const max_chunks = @max(@as(usize, 1), @min(max_chunks_raw, MAX_RAW_BATCH_CHUNKS));
    var popped: usize = 0;
    var checked: usize = 0;
    var cursor = worker_index % shared.raw_scan_queues.len;
    while (checked < shared.raw_scan_queues.len and popped < max_chunks) : (checked += 1) {
        const qidx = cursor;
        cursor += 1;
        if (cursor == shared.raw_scan_queues.len) cursor = 0;
        if (popRawChunkFromQueue(&shared.raw_scan_queues[qidx], queue_lock_ticks)) |chunk| {
            out[popped] = chunk;
            popped += 1;
        }
    }
    return popped;
}

fn claimRawQueueLane(queues: []RawQueue, start_index: usize) ?usize {
    if (queues.len == 0) return null;
    var checked: usize = 0;
    var cursor = start_index % queues.len;
    while (checked < queues.len) : (checked += 1) {
        const lane = cursor;
        cursor += 1;
        if (cursor == queues.len) cursor = 0;
        if (queues[lane].queued_rows_atomic.load(.acquire) == 0) continue;
        if (queues[lane].lease.cmpxchgWeak(false, true, .acq_rel, .acquire) == null) return lane;
    }
    return null;
}

fn claimRawQueueLaneMostChunks(queues: []RawQueue, start_index: usize) ?usize {
    if (queues.len == 0) return null;
    var attempts: usize = 0;
    while (attempts < 2) : (attempts += 1) {
        var best_lane: usize = 0;
        var best_chunks: usize = 0;
        var checked: usize = 0;
        var cursor = start_index % queues.len;
        while (checked < queues.len) : (checked += 1) {
            const lane = cursor;
            cursor += 1;
            if (cursor == queues.len) cursor = 0;
            if (queues[lane].lease.load(.acquire)) continue;
            const chunk_count = queues[lane].queued_chunks_atomic.load(.acquire);
            if (chunk_count > best_chunks) {
                best_chunks = chunk_count;
                best_lane = lane;
            }
        }
        if (best_chunks == 0) return null;
        if (queues[best_lane].lease.cmpxchgWeak(false, true, .acq_rel, .acquire) == null) return best_lane;
    }
    return null;
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

fn acquireRecycledRows(shared: *PipeShared, owner_worker: usize) ?std.ArrayListUnmanaged(Row) {
    if (shared.local_parts.len == 0 or owner_worker >= shared.local_parts.len) return null;
    const owner = &shared.local_parts[owner_worker];
    lockSpin(&owner.recycle_lock);
    defer owner.recycle_lock.unlock();
    const len = owner.recycled_rows.items.len;
    if (len == 0) return null;
    const rows = owner.recycled_rows.items[len - 1];
    owner.recycled_rows.items.len = len - 1;
    return rows;
}

fn prepareEmptyLocalRows(shared: *PipeShared, owner_worker: usize) !std.ArrayListUnmanaged(Row) {
    var rows = acquireRecycledRows(shared, owner_worker) orelse std.ArrayListUnmanaged(Row).empty;
    rows.clearRetainingCapacity();
    if (shared.local_reserve_per_bucket > 0 and rows.capacity < shared.local_reserve_per_bucket) {
        try rows.ensureTotalCapacity(shared.allocator, shared.local_reserve_per_bucket);
    }
    return rows;
}

fn recycleChunkRows(shared: *PipeShared, owner_worker: usize, rows: std.ArrayListUnmanaged(Row)) !void {
    if (shared.local_reserve_per_bucket == 0 or shared.local_parts.len == 0 or owner_worker >= shared.local_parts.len) {
        var disposable = rows;
        disposable.deinit(shared.allocator);
        return;
    }
    const owner = &shared.local_parts[owner_worker];
    var reusable = rows;
    reusable.clearRetainingCapacity();
    lockSpin(&owner.recycle_lock);
    errdefer owner.recycle_lock.unlock();
    try owner.recycled_rows.append(shared.allocator, reusable);
    owner.recycle_lock.unlock();
}

pub const TopRow = struct {
    key: u128,
    count: u64,
    refresh_sum: i64,
    width_sum: i64,
    extra_sum: i64 = 0,
};

inline fn topRowFromState(s: State) TopRow {
    return .{
        .key = s.key,
        .count = s.count,
        .refresh_sum = s.refresh_sum,
        .width_sum = s.width_sum,
        .extra_sum = s.extra_sum,
    };
}

fn better(a: TopRow, b: TopRow) bool {
    if (a.count != b.count) return a.count > b.count;
    return a.key < b.key;
}

fn worse(a: TopRow, b: TopRow) bool {
    if (a.count != b.count) return a.count < b.count;
    return a.key > b.key;
}

const TopSet = struct {
    items: [TOP_K]TopRow = undefined,
    len: usize = 0,
    worst_i: usize = 0,

    fn recomputeWorst(self: *TopSet) void {
        var w: usize = 0;
        var i: usize = 1;
        while (i < self.len) : (i += 1) {
            if (worse(self.items[i], self.items[w])) w = i;
        }
        self.worst_i = w;
    }

    fn consider(self: *TopSet, cand: TopRow) void {
        if (self.len < TOP_K) {
            self.items[self.len] = cand;
            if (self.len == 0 or worse(cand, self.items[self.worst_i])) self.worst_i = self.len;
            self.len += 1;
            return;
        }
        if (!better(cand, self.items[self.worst_i])) return;
        self.items[self.worst_i] = cand;
        self.recomputeWorst();
    }
};

fn topLess(_: void, a: TopRow, b: TopRow) bool {
    return better(a, b);
}

fn tableHasColumns(table: *thindb.api.Table) bool {
    const required = [_][]const u8{ "WatchID", "SearchEngineID", "ClientIP", "IsRefresh", "ResolutionWidth", "SearchPhrase" };
    for (required) |want| {
        var found = false;
        for (table.schema.columns) |c| {
            if (std.mem.eql(u8, c.name, want)) {
                found = true;
                break;
            }
        }
        if (!found) return false;
    }
    return true;
}

fn findHitsTable(allocator: Allocator, catalog: anytype) !*thindb.api.Table {
    const db_names = try catalog.listDatabases(allocator);
    defer {
        for (db_names) |n| allocator.free(n);
        allocator.free(db_names);
    }
    for (db_names) |dn| {
        const db = catalog.database(dn).?;
        const schema_names = try db.listSchemas(allocator);
        defer {
            for (schema_names) |n| allocator.free(n);
            allocator.free(schema_names);
        }
        for (schema_names) |sn| {
            const schema = db.schema(sn).?;
            const table_names = try schema.listTables(allocator);
            defer {
                for (table_names) |n| allocator.free(n);
                allocator.free(table_names);
            }
            for (table_names) |tn| {
                const table = schema.openTable(tn, .{}) catch continue;
                if (tableHasColumns(table)) {
                    std.debug.print("[clientip] table {s}.{s}.{s}\n", .{ dn, sn, tn });
                    return table;
                }
            }
        }
    }
    return error.NotFound;
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

fn bucketIndex(key: u128, bucket_count: usize) usize {
    const h = routeHashKey(key);
    return bucketIndexHash(h, bucket_count);
}

inline fn bucketIndexHash(hash: u64, bucket_count: usize) usize {
    if (std.math.isPowerOfTwo(bucket_count)) return @as(usize, @truncate(hash)) & (bucket_count - 1);
    return @as(usize, @truncate(hash % bucket_count));
}

inline fn bucketIdsFitU8(bucket_count: usize) bool {
    return bucket_count <= @as(usize, std.math.maxInt(u8)) + 1;
}

inline fn packClientIP(client_ip: i32) u128 {
    return @as(u128, @as(u32, @bitCast(client_ip)));
}

inline fn packQ30(search_engine_id: i16, client_ip: i32) u128 {
    return @as(u128, @as(u16, @bitCast(search_engine_id))) |
        (@as(u128, @as(u32, @bitCast(client_ip))) << 16);
}

inline fn packWatchClient(watch_id: i64, client_ip: i32) u128 {
    return @as(u128, @as(u64, @bitCast(watch_id))) |
        (@as(u128, @as(u32, @bitCast(client_ip))) << 64);
}

fn searchPhraseNonEmpty(batch: thindb.Batch, idx: usize, row: usize) bool {
    const sv = switch (batch.values[idx].data) {
        .varchar, .string, .char => |s| s,
        else => return false,
    };
    return sv.rowBytes(row).len != 0;
}

const BatchViews = struct {
    kind: QueryKind,
    client_ip: []const i32 = &.{},
    user_id: ?[]const i64 = null,
    watch_id: ?[]const i64 = null,
    search_engine: ?thindb.storage.ColumnView = null,
    refresh: []const i16 = &.{},
    width: []const i16 = &.{},
    phrase: ?thindb.storage.StringView = null,
};

const PayloadViews = struct {
    client_ip: []const i32,
    watch_id: ?[]const i64 = null,
    search_engine: ?thindb.storage.ColumnView = null,
    refresh: []const i16,
    width: []const i16,
};

fn loadPayloadViews(kind: QueryKind, batch: thindb.Batch) !PayloadViews {
    const client_ip = switch ((batch.columnView("ClientIP") orelse return error.ColumnNotFound).data) {
        .int => |v| v,
        else => return error.UnexpectedClientIPType,
    };
    const refresh = switch ((batch.columnView("IsRefresh") orelse return error.ColumnNotFound).data) {
        .smallint => |v| v,
        else => return error.UnexpectedIsRefreshType,
    };
    const width = switch ((batch.columnView("ResolutionWidth") orelse return error.ColumnNotFound).data) {
        .smallint => |v| v,
        else => return error.UnexpectedResolutionWidthType,
    };
    var out = PayloadViews{ .client_ip = client_ip, .refresh = refresh, .width = width };
    if (kind == .q30) out.search_engine = batch.columnView("SearchEngineID") orelse return error.ColumnNotFound;
    if (kind == .q31) {
        out.watch_id = switch ((batch.columnView("WatchID") orelse return error.ColumnNotFound).data) {
            .bigint => |v| v,
            else => return error.UnexpectedWatchIDType,
        };
    }
    return out;
}

fn loadViewsMaybePhrase(kind: QueryKind, batch: thindb.Batch, need_phrase: bool) !BatchViews {
    if (kind == .q15) {
        return .{
            .kind = kind,
            .user_id = switch ((batch.columnView("UserID") orelse return error.ColumnNotFound).data) {
                .bigint => |v| v,
                else => return error.UnexpectedUserIDType,
            },
        };
    }
    const client_ip = switch ((batch.columnView("ClientIP") orelse return error.ColumnNotFound).data) {
        .int => |v| v,
        else => return error.UnexpectedClientIPType,
    };
    const refresh = switch ((batch.columnView("IsRefresh") orelse return error.ColumnNotFound).data) {
        .smallint => |v| v,
        else => return error.UnexpectedIsRefreshType,
    };
    const width = switch ((batch.columnView("ResolutionWidth") orelse return error.ColumnNotFound).data) {
        .smallint => |v| v,
        else => return error.UnexpectedResolutionWidthType,
    };
    var out = BatchViews{ .kind = kind, .client_ip = client_ip, .refresh = refresh, .width = width };
    if (kind == .q30) out.search_engine = batch.columnView("SearchEngineID") orelse return error.ColumnNotFound;
    if (kind == .q31 or kind == .q32) {
        out.watch_id = switch ((batch.columnView("WatchID") orelse return error.ColumnNotFound).data) {
            .bigint => |v| v,
            else => return error.UnexpectedWatchIDType,
        };
    }
    if (kind.hasFilter() and need_phrase) {
        out.phrase = switch ((batch.columnView("SearchPhrase") orelse return error.ColumnNotFound).data) {
            .varchar, .string, .char => |v| v,
            else => return error.UnexpectedSearchPhraseType,
        };
    }
    return out;
}

fn loadViews(kind: QueryKind, batch: thindb.Batch) !BatchViews {
    return loadViewsMaybePhrase(kind, batch, true);
}

inline fn readSearchEngine(v: thindb.storage.ColumnView, row: usize) !i16 {
    return switch (v.data) {
        .smallint => |s| s[row],
        .tinyint => |s| s[row],
        .int => |s| @intCast(s[row]),
        else => error.UnexpectedSearchEngineIDType,
    };
}

inline fn keyFromViews(v: BatchViews, row: usize) !u128 {
    return switch (v.kind) {
        .clientip => packClientIP(v.client_ip[row]),
        .q15 => @as(u128, @as(u64, @bitCast(v.user_id.?[row]))),
        .q30 => packQ30(try readSearchEngine(v.search_engine.?, row), v.client_ip[row]),
        .q31, .q32 => packWatchClient(v.watch_id.?[row], v.client_ip[row]),
        .generic => error.UnsupportedOperatorForType,
    };
}

inline fn rowPasses(v: BatchViews, row: usize) bool {
    return if (v.phrase) |p| p.offsets[row] != p.offsets[row + 1] else true;
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

fn searchPhrasePred() thindb.Predicate {
    return .{ .col = "SearchPhrase", .op = .neq, .val = .{ .text = "" } };
}

fn applyScanFilter(scan: *Scan, kind: QueryKind) !bool {
    const pred = querySpec(kind).filter orelse return false;
    try scan.addPrune(pred);
    return try scan.tryFuseFilter(.{ .leaf = pred });
}

fn applyScanFilterExpr(scan: *Scan, expr: thindb.exec.PredicateExpr) !bool {
    // The scan owns all filter handling: literal coercion, zone-map prune-hint
    // extraction (incl. IN), and block-sourced evaluation of unprojected
    // columns. The core pipeline just hands it the predicate.
    return try scan.tryFuseFilter(expr);
}

fn rowKey(kind: QueryKind, batch: thindb.Batch, row: usize) !u128 {
    return switch (kind) {
        .clientip => blk: {
            const ip = switch (batch.values[0].data) {
                .int => |v| v,
                else => return error.UnexpectedClientIPType,
            };
            break :blk packClientIP(ip[row]);
        },
        .q15 => blk: {
            const user = switch (batch.values[0].data) {
                .bigint => |v| v,
                else => return error.UnexpectedUserIDType,
            };
            break :blk @as(u128, @as(u64, @bitCast(user[row])));
        },
        .q30 => blk: {
            const ip = switch (batch.values[1].data) {
                .int => |v| v,
                else => return error.UnexpectedClientIPType,
            };
            const search_engine: i16 = switch (batch.values[0].data) {
                .smallint => |v| v[row],
                .tinyint => |v| v[row],
                .int => |v| @intCast(v[row]),
                else => return error.UnexpectedSearchEngineIDType,
            };
            break :blk packQ30(search_engine, ip[row]);
        },
        .q31, .q32 => blk: {
            const watch = switch (batch.values[0].data) {
                .bigint => |v| v,
                else => return error.UnexpectedWatchIDType,
            };
            const ip = switch (batch.values[1].data) {
                .int => |v| v,
                else => return error.UnexpectedClientIPType,
            };
            break :blk packWatchClient(watch[row], ip[row]);
        },
    };
}

fn aggViews(kind: QueryKind, batch: thindb.Batch) !struct { refresh: []const i16, width: []const i16 } {
    const refresh_idx: usize = switch (kind) {
        .clientip => 1,
        .q15 => return error.UnsupportedOperatorForType,
        .q30, .q31, .q32 => 2,
    };
    const width_idx: usize = switch (kind) {
        .clientip => 2,
        .q30, .q31, .q32 => 3,
    };
    const refresh = switch (batch.values[refresh_idx].data) {
        .smallint => |v| v,
        else => return error.UnexpectedIsRefreshType,
    };
    const width = switch (batch.values[width_idx].data) {
        .smallint => |v| v,
        else => return error.UnexpectedResolutionWidthType,
    };
    return .{ .refresh = refresh, .width = width };
}

inline fn markDirtyBucket(parts: *WorkerParts, allocator: Allocator, bucket_idx: usize) !void {
    if (!parts.dirty_marks[bucket_idx]) {
        parts.dirty_marks[bucket_idx] = true;
        try parts.dirty_buckets.append(allocator, bucket_idx);
    }
}

inline fn appendFlatPartitionRow(parts: *WorkerParts, allocator: Allocator, bucket_idx: usize, row: Row) !void {
    try parts.flat_rows.append(allocator, row);
    try parts.flat_bucket_ids.append(allocator, @intCast(bucket_idx));
}

inline fn appendPartitionRow(parts: *WorkerParts, bucket_idx: usize, allocator: Allocator, row: Row) !void {
    if (parts.flat_scan_partitions) {
        return appendFlatPartitionRow(parts, allocator, bucket_idx, row);
    }
    if (parts.shared_buffers) |shared_buffers| {
        var bucket = shared_buffers.at(parts.shared_bank_index, bucket_idx);
        lockSpin(&bucket.lock);
        defer bucket.lock.unlock();
        if (bucket.rows.items.len < bucket.rows.capacity) {
            bucket.rows.appendAssumeCapacity(row);
        } else {
            try bucket.rows.append(allocator, row);
        }
        return;
    }
    var bucket = &parts.buckets[bucket_idx];
    if (bucket.rows.items.len == 0) try markDirtyBucket(parts, allocator, bucket_idx);
    if (bucket.rows.items.len < bucket.rows.capacity) {
        bucket.rows.appendAssumeCapacity(row);
    } else {
        try bucket.rows.append(allocator, row);
    }
}

inline fn appendPartitionRows(parts: *WorkerParts, bucket_idx: usize, allocator: Allocator, rows: []const Row) !void {
    if (rows.len == 0) return;
    if (parts.flat_scan_partitions) {
        try parts.flat_rows.appendSlice(allocator, rows);
        const old_len = parts.flat_bucket_ids.items.len;
        try parts.flat_bucket_ids.resize(allocator, old_len + rows.len);
        @memset(parts.flat_bucket_ids.items[old_len..], @as(u16, @intCast(bucket_idx)));
        return;
    }
    if (parts.shared_buffers) |shared_buffers| {
        var bucket = shared_buffers.at(parts.shared_bank_index, bucket_idx);
        lockSpin(&bucket.lock);
        defer bucket.lock.unlock();
        const old_len = bucket.rows.items.len;
        const new_len = old_len + rows.len;
        if (new_len <= bucket.rows.capacity) {
            bucket.rows.items.len = new_len;
            @memcpy(bucket.rows.items[old_len..new_len], rows);
        } else {
            try bucket.rows.appendSlice(allocator, rows);
        }
        return;
    }
    var bucket = &parts.buckets[bucket_idx];
    if (bucket.rows.items.len == 0) try markDirtyBucket(parts, allocator, bucket_idx);
    const old_len = bucket.rows.items.len;
    const new_len = old_len + rows.len;
    if (new_len <= bucket.rows.capacity) {
        bucket.rows.items.len = new_len;
        @memcpy(bucket.rows.items[old_len..new_len], rows);
    } else {
        try bucket.rows.appendSlice(allocator, rows);
    }
}

fn flushRouteBlockTyped(
    comptime BucketT: type,
    parts: *WorkerParts,
    allocator: Allocator,
    rows: []const Row,
    buckets: []const BucketT,
) !void {
    if (rows.len == 0) return;
    if (parts.flat_scan_partitions) {
        try parts.flat_rows.appendSlice(allocator, rows);
        const old_len = parts.flat_bucket_ids.items.len;
        try parts.flat_bucket_ids.resize(allocator, old_len + rows.len);
        var i: usize = 0;
        while (i < rows.len) : (i += 1) {
            parts.flat_bucket_ids.items[old_len + i] = @intCast(buckets[i]);
        }
        return;
    }
    if (parts.shared_buffers == null and parts.buckets.len >= rows.len / 2) {
        var direct_i: usize = 0;
        while (direct_i < rows.len) : (direct_i += 1) {
            try appendPartitionRow(parts, @intCast(buckets[direct_i]), allocator, rows[direct_i]);
        }
        return;
    }

    if (parts.buckets.len * 4 <= rows.len) {
        var i: usize = 0;
        while (i < rows.len) : (i += 1) {
            parts.route_counts[@intCast(buckets[i])] += 1;
        }

        var offset: u16 = 0;
        var b: usize = 0;
        while (b < parts.buckets.len) : (b += 1) {
            parts.route_offsets[b] = offset;
            offset += parts.route_counts[b];
            parts.route_counts[b] = 0;
        }

        var sorted: [MAX_ROUTE_BLOCK_ROWS]Row = undefined;
        i = 0;
        while (i < rows.len) : (i += 1) {
            const bucket_idx: usize = @intCast(buckets[i]);
            const pos = parts.route_offsets[bucket_idx] + parts.route_counts[bucket_idx];
            sorted[pos] = rows[i];
            parts.route_counts[bucket_idx] += 1;
        }

        b = 0;
        while (b < parts.buckets.len) : (b += 1) {
            const count: usize = parts.route_counts[b];
            if (count != 0) {
                const start: usize = parts.route_offsets[b];
                try appendPartitionRows(parts, b, allocator, sorted[start .. start + count]);
                parts.route_counts[b] = 0;
            }
        }
        return;
    }

    parts.route_touched.clearRetainingCapacity();

    var i: usize = 0;
    while (i < rows.len) : (i += 1) {
        const b: usize = @intCast(buckets[i]);
        if (parts.route_counts[b] == 0) parts.route_touched.appendAssumeCapacity(@intCast(b));
        parts.route_counts[b] += 1;
    }

    var offset: u16 = 0;
    for (parts.route_touched.items) |b| {
        parts.route_offsets[b] = offset;
        offset += parts.route_counts[b];
        parts.route_counts[b] = 0;
    }

    var sorted: [MAX_ROUTE_BLOCK_ROWS]Row = undefined;
    i = 0;
    while (i < rows.len) : (i += 1) {
        const b: usize = @intCast(buckets[i]);
        const pos = parts.route_offsets[b] + parts.route_counts[b];
        sorted[pos] = rows[i];
        parts.route_counts[b] += 1;
    }

    for (parts.route_touched.items) |b| {
        const start: usize = parts.route_offsets[b];
        const count: usize = parts.route_counts[b];
        try appendPartitionRows(parts, b, allocator, sorted[start .. start + count]);
        parts.route_counts[b] = 0;
    }
}

fn flushRouteBlock(
    parts: *WorkerParts,
    allocator: Allocator,
    rows: []const Row,
    buckets: []const u16,
) !void {
    return flushRouteBlockTyped(u16, parts, allocator, rows, buckets);
}

fn partitionFlatWorkerParts(parts: *WorkerParts, bucket_count: usize) !void {
    if (!parts.flat_scan_partitions or parts.flat_partitioned) return;
    const row_len = parts.flat_rows.items.len;
    if (row_len != parts.flat_bucket_ids.items.len) return error.FlatPartitionLengthMismatch;
    if (row_len > std.math.maxInt(u32)) return error.FlatPartitionTooLarge;
    if (parts.flat_counts.len < bucket_count or parts.flat_offsets.len < bucket_count or parts.flat_next.len < bucket_count) {
        return error.FlatPartitionNotInitialized;
    }

    @memset(parts.flat_counts[0..bucket_count], 0);
    var i: usize = 0;
    while (i < row_len) : (i += 1) {
        parts.flat_counts[parts.flat_bucket_ids.items[i]] += 1;
    }

    var offset: u32 = 0;
    var b: usize = 0;
    while (b < bucket_count) : (b += 1) {
        parts.flat_offsets[b] = offset;
        parts.flat_next[b] = offset;
        offset += parts.flat_counts[b];
    }

    b = 0;
    while (b < bucket_count) : (b += 1) {
        const end = parts.flat_offsets[b] + parts.flat_counts[b];
        var pos = parts.flat_next[b];
        while (pos < end) {
            const current_bucket: usize = parts.flat_bucket_ids.items[pos];
            if (current_bucket == b) {
                pos += 1;
                parts.flat_next[b] = pos;
                continue;
            }
            const dest = parts.flat_next[current_bucket];
            parts.flat_next[current_bucket] = dest + 1;
            std.mem.swap(Row, &parts.flat_rows.items[pos], &parts.flat_rows.items[dest]);
            std.mem.swap(u16, &parts.flat_bucket_ids.items[pos], &parts.flat_bucket_ids.items[dest]);
        }
    }

    parts.flat_partitioned = true;
}

inline fn routeBlockAdd(
    parts: *WorkerParts,
    allocator: Allocator,
    bucket_count: usize,
    bucket_mask: usize,
    route_block_rows: usize,
    block_rows: *[MAX_ROUTE_BLOCK_ROWS]Row,
    block_buckets: *[MAX_ROUTE_BLOCK_ROWS]u16,
    block_len: *usize,
    key: u128,
    refresh: i16,
    width: i16,
) !void {
    return routeBlockAddRow(parts, allocator, bucket_count, bucket_mask, route_block_rows, block_rows, block_buckets, block_len, makeRow(key, refresh, width));
}

inline fn routeBlockAddTyped(
    comptime BucketT: type,
    parts: *WorkerParts,
    allocator: Allocator,
    bucket_count: usize,
    bucket_mask: usize,
    route_block_rows: usize,
    block_rows: *[MAX_ROUTE_BLOCK_ROWS]Row,
    block_buckets: *[MAX_ROUTE_BLOCK_ROWS]BucketT,
    block_len: *usize,
    key: u128,
    refresh: i16,
    width: i16,
) !void {
    return routeBlockAddRowTyped(BucketT, parts, allocator, bucket_count, bucket_mask, route_block_rows, block_rows, block_buckets, block_len, makeRow(key, refresh, width));
}

inline fn routeBlockAddPow2Typed(
    comptime BucketT: type,
    parts: *WorkerParts,
    allocator: Allocator,
    bucket_mask: usize,
    route_block_rows: usize,
    block_rows: *[MAX_ROUTE_BLOCK_ROWS]Row,
    block_buckets: *[MAX_ROUTE_BLOCK_ROWS]BucketT,
    block_len: *usize,
    key: u128,
    refresh: i16,
    width: i16,
) !void {
    return routeBlockAddRowPow2Typed(BucketT, parts, allocator, bucket_mask, route_block_rows, block_rows, block_buckets, block_len, makeRow(key, refresh, width));
}

inline fn routeBlockAddRowPow2Typed(
    comptime BucketT: type,
    parts: *WorkerParts,
    allocator: Allocator,
    bucket_mask: usize,
    route_block_rows: usize,
    block_rows: *[MAX_ROUTE_BLOCK_ROWS]Row,
    block_buckets: *[MAX_ROUTE_BLOCK_ROWS]BucketT,
    block_len: *usize,
    row: Row,
) !void {
    const h = routeHashRowBits(row.key_lo, row.key_hi);
    const b = @as(usize, @truncate(h)) & bucket_mask;
    block_rows[block_len.*] = row;
    block_buckets[block_len.*] = @intCast(b);
    block_len.* += 1;
    if (block_len.* == route_block_rows) {
        try flushRouteBlockTyped(BucketT, parts, allocator, block_rows[0..block_len.*], block_buckets[0..block_len.*]);
        block_len.* = 0;
    }
}

inline fn routeBlockAddRowTyped(
    comptime BucketT: type,
    parts: *WorkerParts,
    allocator: Allocator,
    bucket_count: usize,
    bucket_mask: usize,
    route_block_rows: usize,
    block_rows: *[MAX_ROUTE_BLOCK_ROWS]Row,
    block_buckets: *[MAX_ROUTE_BLOCK_ROWS]BucketT,
    block_len: *usize,
    row: Row,
) !void {
    const h = routeHashRowBits(row.key_lo, row.key_hi);
    const b = if (bucket_mask != 0) (@as(usize, @truncate(h)) & bucket_mask) else bucketIndexHash(h, bucket_count);
    block_rows[block_len.*] = row;
    block_buckets[block_len.*] = @intCast(b);
    block_len.* += 1;
    if (block_len.* == route_block_rows) {
        try flushRouteBlockTyped(BucketT, parts, allocator, block_rows[0..block_len.*], block_buckets[0..block_len.*]);
        block_len.* = 0;
    }
}

inline fn routeBlockAddRow(
    parts: *WorkerParts,
    allocator: Allocator,
    bucket_count: usize,
    bucket_mask: usize,
    route_block_rows: usize,
    block_rows: *[MAX_ROUTE_BLOCK_ROWS]Row,
    block_buckets: *[MAX_ROUTE_BLOCK_ROWS]u16,
    block_len: *usize,
    row: Row,
) !void {
    return routeBlockAddRowTyped(u16, parts, allocator, bucket_count, bucket_mask, route_block_rows, block_rows, block_buckets, block_len, row);
}

inline fn appendHashedRow(parts: *WorkerParts, allocator: Allocator, bucket_count: usize, key: u128, refresh: i16, width: i16) !void {
    const h = routeHashKey(key);
    const b = bucketIndexHash(h, bucket_count);
    try appendPartitionRow(parts, b, allocator, makeRow(key, refresh, width));
    parts.row_count += 1;
}

fn appendBatchClientIPTyped(comptime BucketT: type, parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: BatchViews, row_count: usize, route_block_rows: usize) !void {
    const use_mask = std.math.isPowerOfTwo(bucket_count);
    const bucket_mask: usize = if (use_mask) bucket_count - 1 else 0;
    var block_rows: [MAX_ROUTE_BLOCK_ROWS]Row = undefined;
    var block_buckets: [MAX_ROUTE_BLOCK_ROWS]BucketT = undefined;
    var block_len: usize = 0;
    var r: usize = 0;
    if (use_mask) {
        while (r < row_count) : (r += 1) {
            try routeBlockAddPow2Typed(BucketT, parts, allocator, bucket_mask, route_block_rows, &block_rows, &block_buckets, &block_len, packClientIP(views.client_ip[r]), views.refresh[r], views.width[r]);
        }
    } else {
        while (r < row_count) : (r += 1) {
            try routeBlockAddTyped(BucketT, parts, allocator, bucket_count, bucket_mask, route_block_rows, &block_rows, &block_buckets, &block_len, packClientIP(views.client_ip[r]), views.refresh[r], views.width[r]);
        }
    }
    try flushRouteBlockTyped(BucketT, parts, allocator, block_rows[0..block_len], block_buckets[0..block_len]);
    parts.scanned_count += row_count;
    parts.row_count += row_count;
}

fn appendBatchClientIP(parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: BatchViews, row_count: usize, route_block_rows: usize) !void {
    if (bucketIdsFitU8(bucket_count)) {
        return appendBatchClientIPTyped(u8, parts, allocator, bucket_count, views, row_count, route_block_rows);
    }
    return appendBatchClientIPTyped(u16, parts, allocator, bucket_count, views, row_count, route_block_rows);
}

fn appendBatchQ30WithSETyped(comptime T: type, comptime BucketT: type, parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: BatchViews, se: []const T, row_count: usize, route_block_rows: usize, skip_filter_check: bool) !void {
    const offsets = if (skip_filter_check) null else views.phrase.?.offsets;
    const use_mask = std.math.isPowerOfTwo(bucket_count);
    const bucket_mask: usize = if (use_mask) bucket_count - 1 else 0;
    var block_rows: [MAX_ROUTE_BLOCK_ROWS]Row = undefined;
    var block_buckets: [MAX_ROUTE_BLOCK_ROWS]BucketT = undefined;
    var block_len: usize = 0;
    var accepted: u64 = 0;
    var r: usize = 0;
    while (r < row_count) : (r += 1) {
        if (offsets) |o| {
            if (o[r] == o[r + 1]) continue;
        }
        const search_engine: i16 = switch (T) {
            i8 => se[r],
            i16 => se[r],
            i32 => @intCast(se[r]),
            else => unreachable,
        };
        const key = packQ30(search_engine, views.client_ip[r]);
        if (use_mask) {
            try routeBlockAddPow2Typed(BucketT, parts, allocator, bucket_mask, route_block_rows, &block_rows, &block_buckets, &block_len, key, views.refresh[r], views.width[r]);
        } else {
            try routeBlockAddTyped(BucketT, parts, allocator, bucket_count, bucket_mask, route_block_rows, &block_rows, &block_buckets, &block_len, key, views.refresh[r], views.width[r]);
        }
        accepted += 1;
    }
    try flushRouteBlockTyped(BucketT, parts, allocator, block_rows[0..block_len], block_buckets[0..block_len]);
    parts.scanned_count += row_count;
    parts.row_count += accepted;
}

fn appendBatchQ30WithSE(comptime T: type, parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: BatchViews, se: []const T, row_count: usize, route_block_rows: usize, skip_filter_check: bool) !void {
    if (bucketIdsFitU8(bucket_count)) {
        return appendBatchQ30WithSETyped(T, u8, parts, allocator, bucket_count, views, se, row_count, route_block_rows, skip_filter_check);
    }
    return appendBatchQ30WithSETyped(T, u16, parts, allocator, bucket_count, views, se, row_count, route_block_rows, skip_filter_check);
}

fn appendBatchQ30(parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: BatchViews, row_count: usize, route_block_rows: usize, skip_filter_check: bool) !void {
    switch (views.search_engine.?.data) {
        .tinyint => |se| try appendBatchQ30WithSE(i8, parts, allocator, bucket_count, views, se, row_count, route_block_rows, skip_filter_check),
        .smallint => |se| try appendBatchQ30WithSE(i16, parts, allocator, bucket_count, views, se, row_count, route_block_rows, skip_filter_check),
        .int => |se| try appendBatchQ30WithSE(i32, parts, allocator, bucket_count, views, se, row_count, route_block_rows, skip_filter_check),
        else => return error.UnexpectedSearchEngineIDType,
    }
}

fn appendBatchQ31Typed(comptime BucketT: type, parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: BatchViews, row_count: usize, route_block_rows: usize, skip_filter_check: bool) !void {
    const offsets = if (skip_filter_check) null else views.phrase.?.offsets;
    const watch = views.watch_id.?;
    const use_mask = std.math.isPowerOfTwo(bucket_count);
    const bucket_mask: usize = if (use_mask) bucket_count - 1 else 0;
    var block_rows: [MAX_ROUTE_BLOCK_ROWS]Row = undefined;
    var block_buckets: [MAX_ROUTE_BLOCK_ROWS]BucketT = undefined;
    var block_len: usize = 0;
    var accepted: u64 = 0;
    var r: usize = 0;
    while (r < row_count) : (r += 1) {
        if (offsets) |o| {
            if (o[r] == o[r + 1]) continue;
        }
        const key = packWatchClient(watch[r], views.client_ip[r]);
        if (use_mask) {
            try routeBlockAddPow2Typed(BucketT, parts, allocator, bucket_mask, route_block_rows, &block_rows, &block_buckets, &block_len, key, views.refresh[r], views.width[r]);
        } else {
            try routeBlockAddTyped(BucketT, parts, allocator, bucket_count, bucket_mask, route_block_rows, &block_rows, &block_buckets, &block_len, key, views.refresh[r], views.width[r]);
        }
        accepted += 1;
    }
    try flushRouteBlockTyped(BucketT, parts, allocator, block_rows[0..block_len], block_buckets[0..block_len]);
    parts.scanned_count += row_count;
    parts.row_count += accepted;
}

fn appendBatchQ31(parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: BatchViews, row_count: usize, route_block_rows: usize, skip_filter_check: bool) !void {
    if (bucketIdsFitU8(bucket_count)) {
        return appendBatchQ31Typed(u8, parts, allocator, bucket_count, views, row_count, route_block_rows, skip_filter_check);
    }
    return appendBatchQ31Typed(u16, parts, allocator, bucket_count, views, row_count, route_block_rows, skip_filter_check);
}

fn appendBatchQ32Typed(comptime BucketT: type, parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: BatchViews, row_count: usize, route_block_rows: usize) !void {
    const watch = views.watch_id.?;
    const use_mask = std.math.isPowerOfTwo(bucket_count);
    const bucket_mask: usize = if (use_mask) bucket_count - 1 else 0;
    var block_rows: [MAX_ROUTE_BLOCK_ROWS]Row = undefined;
    var block_buckets: [MAX_ROUTE_BLOCK_ROWS]BucketT = undefined;
    var block_len: usize = 0;
    var r: usize = 0;
    if (use_mask) {
        while (r < row_count) : (r += 1) {
            const key = packWatchClient(watch[r], views.client_ip[r]);
            try routeBlockAddPow2Typed(BucketT, parts, allocator, bucket_mask, route_block_rows, &block_rows, &block_buckets, &block_len, key, views.refresh[r], views.width[r]);
        }
    } else {
        while (r < row_count) : (r += 1) {
            const key = packWatchClient(watch[r], views.client_ip[r]);
            try routeBlockAddTyped(BucketT, parts, allocator, bucket_count, bucket_mask, route_block_rows, &block_rows, &block_buckets, &block_len, key, views.refresh[r], views.width[r]);
        }
    }
    try flushRouteBlockTyped(BucketT, parts, allocator, block_rows[0..block_len], block_buckets[0..block_len]);
    parts.scanned_count += row_count;
    parts.row_count += row_count;
}

fn appendBatchQ32(parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: BatchViews, row_count: usize, route_block_rows: usize) !void {
    if (bucketIdsFitU8(bucket_count)) {
        return appendBatchQ32Typed(u8, parts, allocator, bucket_count, views, row_count, route_block_rows);
    }
    return appendBatchQ32Typed(u16, parts, allocator, bucket_count, views, row_count, route_block_rows);
}

fn appendSurvivorsQ30WithSE(comptime T: type, parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: PayloadViews, se: []const T, survivor_rows: []const u32) !void {
    for (survivor_rows) |row_u32| {
        const r: usize = row_u32;
        const search_engine: i16 = switch (T) {
            i8 => se[r],
            i16 => se[r],
            i32 => @intCast(se[r]),
            else => unreachable,
        };
        const key = packQ30(search_engine, views.client_ip[r]);
        try appendHashedRow(parts, allocator, bucket_count, key, views.refresh[r], views.width[r]);
    }
}

fn appendSurvivorsQ30(parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: PayloadViews, survivor_rows: []const u32) !void {
    switch (views.search_engine.?.data) {
        .tinyint => |se| try appendSurvivorsQ30WithSE(i8, parts, allocator, bucket_count, views, se, survivor_rows),
        .smallint => |se| try appendSurvivorsQ30WithSE(i16, parts, allocator, bucket_count, views, se, survivor_rows),
        .int => |se| try appendSurvivorsQ30WithSE(i32, parts, allocator, bucket_count, views, se, survivor_rows),
        else => return error.UnexpectedSearchEngineIDType,
    }
}

fn appendSurvivorsQ31(parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: PayloadViews, survivor_rows: []const u32) !void {
    const watch = views.watch_id.?;
    for (survivor_rows) |row_u32| {
        const r: usize = row_u32;
        const key = packWatchClient(watch[r], views.client_ip[r]);
        try appendHashedRow(parts, allocator, bucket_count, key, views.refresh[r], views.width[r]);
    }
}

fn appendLateQ30WithSE(comptime T: type, parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: PayloadViews, se: []const T, offsets: []const u32, phrase_base: usize, payload_base: usize, len: usize) !void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        const pr = phrase_base + i;
        if (offsets[pr] == offsets[pr + 1]) continue;
        const r = payload_base + i;
        const search_engine: i16 = switch (T) {
            i8 => se[r],
            i16 => se[r],
            i32 => @intCast(se[r]),
            else => unreachable,
        };
        try appendHashedRow(parts, allocator, bucket_count, packQ30(search_engine, views.client_ip[r]), views.refresh[r], views.width[r]);
    }
}

fn appendLateQ30(parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: PayloadViews, offsets: []const u32, phrase_base: usize, payload_base: usize, len: usize) !void {
    switch (views.search_engine.?.data) {
        .tinyint => |se| try appendLateQ30WithSE(i8, parts, allocator, bucket_count, views, se, offsets, phrase_base, payload_base, len),
        .smallint => |se| try appendLateQ30WithSE(i16, parts, allocator, bucket_count, views, se, offsets, phrase_base, payload_base, len),
        .int => |se| try appendLateQ30WithSE(i32, parts, allocator, bucket_count, views, se, offsets, phrase_base, payload_base, len),
        else => return error.UnexpectedSearchEngineIDType,
    }
}

fn appendLateQ31(parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: PayloadViews, offsets: []const u32, phrase_base: usize, payload_base: usize, len: usize) !void {
    const watch = views.watch_id.?;
    var i: usize = 0;
    while (i < len) : (i += 1) {
        const pr = phrase_base + i;
        if (offsets[pr] == offsets[pr + 1]) continue;
        const r = payload_base + i;
        try appendHashedRow(parts, allocator, bucket_count, packWatchClient(watch[r], views.client_ip[r]), views.refresh[r], views.width[r]);
    }
}

fn normalizeRouteBlockRows(route_block_rows: usize) usize {
    return @max(@as(usize, 1), @min(route_block_rows, MAX_ROUTE_BLOCK_ROWS));
}

fn chooseRouteBlockRows(bucket_count: usize, cfg_route_block_rows: usize, route_block_rows_set: bool) usize {
    _ = bucket_count;
    if (route_block_rows_set) return normalizeRouteBlockRows(cfg_route_block_rows);
    return DEFAULT_ROUTE_BLOCK_ROWS;
}

fn appendBatchRoutedEx(parts: *WorkerParts, allocator: Allocator, bucket_count: usize, kind: QueryKind, batch: thindb.Batch, route_block_rows_raw: usize, skip_filter_check: bool) !void {
    const views = try loadViewsMaybePhrase(kind, batch, !(skip_filter_check and kind.hasFilter()));
    const route_block_rows = normalizeRouteBlockRows(route_block_rows_raw);
    return switch (kind) {
        .clientip => appendBatchClientIP(parts, allocator, bucket_count, views, batch.row_count, route_block_rows),
        .q15 => appendBatchCountOnly(parts, allocator, bucket_count, views, batch.row_count),
        .q30 => appendBatchQ30(parts, allocator, bucket_count, views, batch.row_count, route_block_rows, skip_filter_check),
        .q31 => appendBatchQ31(parts, allocator, bucket_count, views, batch.row_count, route_block_rows, skip_filter_check),
        .q32 => appendBatchQ32(parts, allocator, bucket_count, views, batch.row_count, route_block_rows),
        .generic => error.UnsupportedOperatorForType,
    };
}

fn appendBatchCountOnly(parts: *WorkerParts, allocator: Allocator, bucket_count: usize, views: BatchViews, row_count: usize) !void {
    var r: usize = 0;
    while (r < row_count) : (r += 1) {
        const key = try keyFromViews(views, r);
        try appendHashedRow(parts, allocator, bucket_count, key, 0, 0);
    }
}

fn appendBatchRouted(parts: *WorkerParts, allocator: Allocator, bucket_count: usize, kind: QueryKind, batch: thindb.Batch, route_block_rows_raw: usize) !void {
    return appendBatchRoutedEx(parts, allocator, bucket_count, kind, batch, route_block_rows_raw, false);
}

fn appendBatch(parts: *WorkerParts, allocator: Allocator, bucket_count: usize, kind: QueryKind, batch: thindb.Batch) !void {
    return appendBatchRouted(parts, allocator, bucket_count, kind, batch, DEFAULT_ROUTE_BLOCK_ROWS);
}

fn publishPipeBucket(shared: *PipeShared, bucket_idx: usize, owner_worker: usize, bucket: *PartBucket) !void {
    if (bucket.rows.items.len == 0) return;
    const rows = bucket.rows;
    const row_count: u64 = @intCast(rows.items.len);
    bucket.rows = try prepareEmptyLocalRows(shared, owner_worker);
    errdefer {
        bucket.rows.deinit(shared.allocator);
        bucket.rows = rows;
    }

    _ = shared.outstanding_chunks.fetchAdd(1, .release);
    _ = shared.outstanding_rows.fetchAdd(row_count, .release);
    lockSpin(&shared.buckets[bucket_idx].queue_lock);
    errdefer {
        _ = shared.outstanding_chunks.fetchSub(1, .release);
        _ = shared.outstanding_rows.fetchSub(row_count, .release);
        shared.buckets[bucket_idx].queue_lock.unlock();
    }
    try shared.buckets[bucket_idx].chunks.append(shared.allocator, .{ .rows = rows, .owner_worker = owner_worker });
    shared.buckets[bucket_idx].queued_rows += row_count;
    shared.buckets[bucket_idx].queue_lock.unlock();
}

fn flushPipeParts(shared: *PipeShared, parts: *WorkerParts) !void {
    for (parts.dirty_buckets.items) |b| {
        if (!parts.dirty_marks[b]) continue;
        try publishPipeBucket(shared, b, parts.worker_index, &parts.buckets[b]);
        parts.dirty_marks[b] = false;
    }
    parts.dirty_buckets.clearRetainingCapacity();
}

fn appendBatchPipe(parts: *WorkerParts, shared: *PipeShared, kind: QueryKind, batch: thindb.Batch, chunk_rows: usize) !void {
    const views = try loadViews(kind, batch);

    var r: usize = 0;
    if (std.math.isPowerOfTwo(shared.bucket_count)) {
        const mask = shared.bucket_count - 1;
        while (r < batch.row_count) : (r += 1) {
            parts.scanned_count += 1;
            if (!rowPasses(views, r)) continue;
            const key = try keyFromViews(views, r);
            const h = routeHashKey(key);
            const b = @as(usize, @truncate(h)) & mask;
            try appendPartitionRow(parts, b, shared.allocator, makeRow(key, views.refresh[r], views.width[r]));
            parts.row_count += 1;
            if (parts.buckets[b].rows.items.len >= chunk_rows) {
                try publishPipeBucket(shared, b, parts.worker_index, &parts.buckets[b]);
                parts.dirty_marks[b] = false;
            }
        }
    } else {
        while (r < batch.row_count) : (r += 1) {
            parts.scanned_count += 1;
            if (!rowPasses(views, r)) continue;
            const key = try keyFromViews(views, r);
            const h = routeHashKey(key);
            const b = bucketIndexHash(h, shared.bucket_count);
            try appendPartitionRow(parts, b, shared.allocator, makeRow(key, views.refresh[r], views.width[r]));
            parts.row_count += 1;
            if (parts.buckets[b].rows.items.len >= chunk_rows) {
                try publishPipeBucket(shared, b, parts.worker_index, &parts.buckets[b]);
                parts.dirty_marks[b] = false;
            }
        }
    }
}

fn appendBatchSilo(parts: *WorkerParts, shared: *PipeShared, kind: QueryKind, batch: thindb.Batch, chunk_rows: usize, profile: bool) !void {
    const route_t0 = if (profile) nowTicks() else 0;
    try appendBatchRouted(parts, shared.allocator, shared.bucket_count, kind, batch, shared.route_block_rows);
    if (profile) parts.partition_ticks += nowTicks() - route_t0;

    const publish_t0 = if (profile) nowTicks() else 0;
    var kept: usize = 0;
    for (parts.dirty_buckets.items) |b| {
        if (!parts.dirty_marks[b]) continue;
        if (parts.buckets[b].rows.items.len >= chunk_rows) {
            try publishPipeBucket(shared, b, parts.worker_index, &parts.buckets[b]);
            parts.published_chunks += 1;
            parts.dirty_marks[b] = false;
        } else {
            parts.dirty_buckets.items[kept] = b;
            kept += 1;
        }
    }
    parts.dirty_buckets.items.len = kept;
    if (profile) parts.publish_ticks += nowTicks() - publish_t0;
}

fn appendBatchSiloLocal(parts: *WorkerParts, shared: *PipeShared, kind: QueryKind, batch: thindb.Batch, profile: bool, skip_filter_check: bool) !void {
    const route_t0 = if (profile) nowTicks() else 0;
    const before_rows = parts.row_count;
    try appendBatchRoutedEx(parts, shared.allocator, shared.bucket_count, kind, batch, shared.route_block_rows, skip_filter_check);
    const added_rows = parts.row_count - before_rows;
    parts.local_buffered_rows += added_rows;
    if (added_rows > 0) _ = shared.scan_buffered_rows.fetchAdd(added_rows, .release);
    if (profile) parts.partition_ticks += nowTicks() - route_t0;
}

inline fn appendRawPacked(parts: *WorkerParts, shared: *PipeShared, raw_chunk_rows: usize, key: u128, refresh: i16, width: i16) !void {
    parts.raw_active_rows.appendAssumeCapacity(key, refresh, width);
    if (parts.raw_active_rows.len() == raw_chunk_rows) {
        if (shared.raw_scan_queues.len > 0) {
            const qidx = chooseRawScanPublishLane(shared, parts.raw_scan_lane, @max(@as(usize, 1), raw_chunk_rows / 2));
            try publishRawRowsToQueue(shared, &shared.raw_scan_queues[qidx], parts.worker_index, &parts.raw_active_rows, raw_chunk_rows, &shared.outstanding_chunks, &shared.outstanding_rows, &parts.raw_scan_queue_lock_ticks, &parts.raw_recycle_lock_ticks);
        } else {
            try publishRawRows(shared, parts.worker_index, &parts.raw_active_rows, raw_chunk_rows, &parts.raw_queue_lock_ticks, &parts.raw_recycle_lock_ticks);
        }
        parts.published_chunks += 1;
    }
}

fn appendBatchRawChunksGeneric(parts: *WorkerParts, shared: *PipeShared, batch: thindb.Batch, raw_chunk_rows: usize, profile: bool, skip_filter_check: bool) !void {
    if (shared.generic_filter_required and !skip_filter_check) return error.UnsupportedOperatorForType;
    const layout = shared.group_rows_layout;
    if (layout.key_columns.len == 0 or layout.key_columns.len > MAX_GENERIC_GROUP_KEYS) return error.UnsupportedOperatorForType;
    if (parts.raw_active_rows.capacity() == 0) parts.raw_active_rows = try acquireRawRows(shared, raw_chunk_rows, &parts.raw_recycle_lock_ticks);

    var key_views_buf: [MAX_GENERIC_GROUP_KEYS]thindb.storage.ColumnView = undefined;
    for (layout.key_columns, 0..) |part, i| {
        key_views_buf[i] = batch.columnView(part.name) orelse return error.ColumnNotFound;
    }
    if (layout.columns.len > MAX_GROUP_PAYLOAD_COLUMNS) return error.UnsupportedOperatorForType;
    var payload_views_buf: [MAX_GROUP_PAYLOAD_COLUMNS][]const i16 = undefined;
    for (layout.columns, 0..) |column, i| {
        if (column.source_name.len == 0) return error.UnsupportedOperatorForType;
        const view = batch.columnView(column.source_name) orelse return error.ColumnNotFound;
        payload_views_buf[i] = switch (view.data) {
            .smallint => |v| v,
            .tinyint => return error.UnsupportedOperatorForType,
            .int => return error.UnsupportedOperatorForType,
            else => return error.TypeMismatch,
        };
    }

    if (try appendBatchRawChunksGenericFast(parts, shared, batch, layout, key_views_buf[0..layout.key_columns.len], payload_views_buf[0..layout.columns.len], raw_chunk_rows, profile)) return;

    const route_t0 = if (profile) nowTicks() else 0;
    var accepted: u64 = 0;
    var r: usize = 0;
    while (r < batch.row_count) {
        var active = &parts.raw_active_rows;
        const key_lo = active.keyLoAll();
        const key_hi = active.keyHiAll();
        while (r < batch.row_count and active.len() < raw_chunk_rows) : (r += 1) {
            const key = try genericKeyFromViews(layout, key_views_buf[0..layout.key_columns.len], r);
            const idx = active.len();
            key_lo[idx] = @truncate(key);
            key_hi[idx] = @truncate(key >> 64);
            var col: usize = 0;
            while (col < layout.columns.len) : (col += 1) {
                active.columnI16All(col)[idx] = payload_views_buf[col][r];
            }
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

fn appendBatchRawChunksGenericFast(
    parts: *WorkerParts,
    shared: *PipeShared,
    batch: thindb.Batch,
    layout: GroupRowsLayout,
    key_views: []const thindb.storage.ColumnView,
    payload_views: []const []const i16,
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
            try appendBatchRawChunksGenericKey1I64(parts, shared, batch.row_count, k0, payload_views, raw_chunk_rows, profile);
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
            try appendBatchRawChunksGenericKeyI16I32(parts, shared, batch.row_count, k0, k1, payload_views, raw_chunk_rows, profile);
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
            try appendBatchRawChunksGenericKeyI64I32(parts, shared, batch.row_count, k0, k1, payload_views, raw_chunk_rows, profile);
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
    payload_views: []const []const i16,
    raw_chunk_rows: usize,
    profile: bool,
) !void {
    const route_t0 = if (profile) nowTicks() else 0;
    var accepted: u64 = 0;
    var r: usize = 0;
    while (r < row_count) {
        var active = &parts.raw_active_rows;
        const key_lo = active.keyLoAll();
        const key_hi = active.keyHiAll();
        while (r < row_count and active.len() < raw_chunk_rows) : (r += 1) {
            const idx = active.len();
            key_lo[idx] = @as(u64, @bitCast(key0[r]));
            key_hi[idx] = 0;
            appendGenericPayload(active, payload_views, idx, r);
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
    payload_views: []const []const i16,
    raw_chunk_rows: usize,
    profile: bool,
) !void {
    const route_t0 = if (profile) nowTicks() else 0;
    var accepted: u64 = 0;
    var r: usize = 0;
    while (r < row_count) {
        var active = &parts.raw_active_rows;
        const key_lo = active.keyLoAll();
        const key_hi = active.keyHiAll();
        while (r < row_count and active.len() < raw_chunk_rows) : (r += 1) {
            const idx = active.len();
            key_lo[idx] = @as(u16, @bitCast(key0[r])) |
                (@as(u64, @as(u32, @bitCast(key1[r]))) << 16);
            key_hi[idx] = 0;
            appendGenericPayload(active, payload_views, idx, r);
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
    payload_views: []const []const i16,
    raw_chunk_rows: usize,
    profile: bool,
) !void {
    const route_t0 = if (profile) nowTicks() else 0;
    var accepted: u64 = 0;
    var r: usize = 0;
    while (r < row_count) {
        var active = &parts.raw_active_rows;
        const key_lo = active.keyLoAll();
        const key_hi = active.keyHiAll();
        while (r < row_count and active.len() < raw_chunk_rows) : (r += 1) {
            const idx = active.len();
            key_lo[idx] = @as(u64, @bitCast(key0[r]));
            key_hi[idx] = @as(u32, @bitCast(key1[r]));
            appendGenericPayload(active, payload_views, idx, r);
            active.len_rows = idx + 1;
            accepted += 1;
        }
        if (active.len() == raw_chunk_rows) try publishActiveRawRows(parts, shared, raw_chunk_rows);
    }
    parts.scanned_count += row_count;
    parts.row_count += accepted;
    if (profile) parts.partition_ticks += nowTicks() - route_t0;
}

inline fn appendGenericPayload(active: *RawRows, payload_views: []const []const i16, dst: usize, src: usize) void {
    if (payload_views.len == 2) {
        active.columnI16All(0)[dst] = payload_views[0][src];
        active.columnI16All(1)[dst] = payload_views[1][src];
        return;
    }
    var col: usize = 0;
    while (col < payload_views.len) : (col += 1) {
        active.columnI16All(col)[dst] = payload_views[col][src];
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

fn appendBatchRawChunks(parts: *WorkerParts, shared: *PipeShared, kind: QueryKind, batch: thindb.Batch, raw_chunk_rows: usize, profile: bool, skip_filter_check: bool) !void {
    if (shared.group_rows_layout.key_columns.len != 0) {
        return appendBatchRawChunksGeneric(parts, shared, batch, raw_chunk_rows, profile, skip_filter_check);
    }
    const route_t0 = if (profile) nowTicks() else 0;
    const views = try loadViewsMaybePhrase(kind, batch, !(skip_filter_check and kind.hasFilter()));
    if (parts.raw_active_rows.capacity() == 0) parts.raw_active_rows = try acquireRawRows(shared, raw_chunk_rows, &parts.raw_recycle_lock_ticks);

    var accepted: u64 = 0;
    var r: usize = 0;
    var empty_i16_buf: [0]i16 = .{};
    while (r < batch.row_count) {
        var active = &parts.raw_active_rows;
        const key_lo = active.keyLoAll();
        const key_hi = active.keyHiAll();
        const columns = active.layout.columns;
        const fast_payload = columns.len == 2 and
            columns[0].physical_type == .i16 and columns[0].source == .is_refresh and
            columns[1].physical_type == .i16 and columns[1].source == .resolution_width;
        const col0: []i16 = if (fast_payload) active.columnI16All(0) else empty_i16_buf[0..0];
        const col1: []i16 = if (fast_payload) active.columnI16All(1) else empty_i16_buf[0..0];

        while (r < batch.row_count and active.len() < raw_chunk_rows) : (r += 1) {
            if (!rowPasses(views, r)) continue;
            const key = try keyFromViews(views, r);
            const idx = active.len();
            key_lo[idx] = @truncate(key);
            key_hi[idx] = @truncate(key >> 64);
            if (fast_payload) {
                col0[idx] = views.refresh[r];
                col1[idx] = views.width[r];
            } else {
                var col: usize = 0;
                while (col < columns.len) : (col += 1) {
                    switch (columns[col].physical_type) {
                        .i16 => active.columnI16All(col)[idx] = payloadValueI16(views, columns[col].source, r),
                    }
                }
            }
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

inline fn payloadValueI16(views: BatchViews, source: GroupColumnSource, row: usize) i16 {
    return switch (source) {
        .is_refresh => views.refresh[row],
        .resolution_width => views.width[row],
    };
}

fn publishFullLocalBuckets(shared: *PipeShared, parts: *WorkerParts, chunk_rows: usize, profile: bool) !void {
    const publish_t0 = if (profile) nowTicks() else 0;
    var kept: usize = 0;
    for (parts.dirty_buckets.items) |b| {
        if (!parts.dirty_marks[b]) continue;
        if (parts.buckets[b].rows.items.len >= chunk_rows) {
            const rows_to_publish: u64 = @intCast(parts.buckets[b].rows.items.len);
            try publishPipeBucket(shared, b, parts.worker_index, &parts.buckets[b]);
            parts.local_buffered_rows -= rows_to_publish;
            _ = shared.scan_buffered_rows.fetchSub(rows_to_publish, .release);
            parts.published_chunks += 1;
            parts.dirty_marks[b] = false;
        } else {
            parts.dirty_buckets.items[kept] = b;
            kept += 1;
        }
    }
    parts.dirty_buckets.items.len = kept;
    if (profile) parts.publish_ticks += nowTicks() - publish_t0;
}

fn flushPipePartsTracked(shared: *PipeShared, parts: *WorkerParts) !void {
    for (parts.dirty_buckets.items) |b| {
        if (!parts.dirty_marks[b]) continue;
        if (parts.buckets[b].rows.items.len == 0) continue;
        const rows_to_publish: u64 = @intCast(parts.buckets[b].rows.items.len);
        try publishPipeBucket(shared, b, parts.worker_index, &parts.buckets[b]);
        parts.local_buffered_rows -= rows_to_publish;
        _ = shared.scan_buffered_rows.fetchSub(rows_to_publish, .release);
        parts.dirty_marks[b] = false;
    }
    parts.dirty_buckets.clearRetainingCapacity();
}

fn initGroupState(states: *std.ArrayListUnmanaged(State), key: u128) u32 {
    const gid: u32 = @intCast(states.items.len);
    states.appendAssumeCapacity(.{
        .key = key,
        .count = 0,
        .refresh_sum = 0,
        .width_sum = 0,
    });
    return gid;
}

fn initGroupStateFromRow(states: *std.ArrayListUnmanaged(State), key: u128, row: Row) u32 {
    return initGroupStateValues(states, key, row.refresh, row.width);
}

fn initGroupStateValues(states: *std.ArrayListUnmanaged(State), key: u128, refresh: i16, width: i16) u32 {
    const gid: u32 = @intCast(states.items.len);
    states.appendAssumeCapacity(.{
        .key = key,
        .count = 1,
        .refresh_sum = refresh,
        .width_sum = width,
    });
    return gid;
}

fn initGroupStateCountSumAvg(states: *std.ArrayListUnmanaged(State), key: u128, sum_value: i16, avg_value: i16) u32 {
    const gid: u32 = @intCast(states.items.len);
    states.appendAssumeCapacity(.{
        .key = key,
        .count = 1,
        .refresh_sum = sum_value,
        .width_sum = avg_value,
    });
    return gid;
}

inline fn updateGroupStateCountSumAvg(st: *State, sum_value: i16, avg_value: i16) void {
    st.count += 1;
    st.refresh_sum += sum_value;
    st.width_sum += avg_value;
}

fn initGroupStateProgram(
    states: *std.ArrayListUnmanaged(State),
    key: u128,
    aggregates: []const GroupAggregateSpec,
    columns: []const []const i16,
    row_idx: usize,
) !u32 {
    const gid: u32 = @intCast(states.items.len);
    states.appendAssumeCapacity(.{
        .key = key,
        .count = 0,
        .refresh_sum = 0,
        .width_sum = 0,
    });
    const st = &states.items[gid];
    try updateGroupStateProgram(st, aggregates, columns, row_idx);
    return gid;
}

fn updateGroupStateProgram(st: *State, aggregates: []const GroupAggregateSpec, columns: []const []const i16, row_idx: usize) !void {
    st.count += 1;
    for (aggregates) |agg| {
        if (agg.state_index >= MAX_GROUP_AGG_STATES) return error.UnsupportedOperatorForType;
        const state_index: usize = agg.state_index;
        switch (agg.op) {
            .count_star => {
                if (state_index != 0) try setAggregateStateValue(st, state_index, st.count);
            },
            .count_col => {
                _ = aggregateInputValue(agg, columns, row_idx) catch return error.UnsupportedOperatorForType;
                if (state_index != 0) try setAggregateStateValue(st, state_index, st.count);
            },
            .sum => {
                const value = try aggregateInputValue(agg, columns, row_idx);
                try addAggregateStateValue(st, state_index, value);
            },
            .avg => {
                const value = try aggregateInputValue(agg, columns, row_idx);
                try addAggregateStateValue(st, state_index, value);
            },
            .min => {
                const value = try aggregateInputValue(agg, columns, row_idx);
                if (st.count == 1 or value < aggregateStateValue(st, state_index)) try setAggregateStateValue(st, state_index, value);
            },
            .max => {
                const value = try aggregateInputValue(agg, columns, row_idx);
                if (st.count == 1 or value > aggregateStateValue(st, state_index)) try setAggregateStateValue(st, state_index, value);
            },
        }
    }
}

fn aggregateInputValue(agg: GroupAggregateSpec, columns: []const []const i16, row_idx: usize) !i128 {
    const input_index = agg.input_column_index orelse return error.UnsupportedOperatorForType;
    if (input_index >= columns.len) return error.UnsupportedOperatorForType;
    return columns[input_index][row_idx];
}

inline fn aggregateStateValue(st: *const State, state_index: usize) i128 {
    return switch (state_index) {
        0 => @intCast(st.count),
        1 => st.refresh_sum,
        2 => st.width_sum,
        3 => st.extra_sum,
        else => unreachable,
    };
}

inline fn addAggregateStateValue(st: *State, state_index: usize, value: i128) !void {
    switch (state_index) {
        0 => st.count += @intCast(value),
        1 => st.refresh_sum += @intCast(value),
        2 => st.width_sum += @intCast(value),
        3 => st.extra_sum += @intCast(value),
        else => return error.UnsupportedOperatorForType,
    }
}

inline fn setAggregateStateValue(st: *State, state_index: usize, value: i128) !void {
    switch (state_index) {
        0 => st.count = @intCast(value),
        1 => st.refresh_sum = @intCast(value),
        2 => st.width_sum = @intCast(value),
        3 => st.extra_sum = @intCast(value),
        else => return error.UnsupportedOperatorForType,
    }
}

fn validateGroupAggregateProgram(aggregates: []const GroupAggregateSpec, column_count: usize) !void {
    if (aggregates.len == 0) return error.UnsupportedOperatorForType;
    for (aggregates) |agg| {
        if (agg.state_index >= MAX_GROUP_AGG_STATES) return error.UnsupportedOperatorForType;
        switch (agg.op) {
            .count_star => {},
            .count_col, .sum, .avg, .min, .max => {
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

fn groupBatchDirect(
    table: *GroupTable,
    states: *std.ArrayListUnmanaged(State),
    scratch: *GroupScratch,
    allocator: Allocator,
    kind: QueryKind,
    batch: thindb.Batch,
) !void {
    const views = try loadViews(kind, batch);
    const n = batch.row_count;

    if (table.needsGrow(n)) try table.grow(allocator, n);
    try states.ensureUnusedCapacity(allocator, n);
    scratch.gids.clearRetainingCapacity();
    scratch.row_idxs.clearRetainingCapacity();
    try scratch.gids.ensureTotalCapacity(allocator, n);
    try scratch.row_idxs.ensureTotalCapacity(allocator, n);

    var r: usize = 0;
    while (r < n) : (r += 1) {
        const pf = r + PREFETCH_DIST_DIRECT;
        if (pf < n) {
            const pf_key = try keyFromViews(views, pf);
            @prefetch(table.slotAddr(table.bucketOf(GroupTable.hashKey(pf_key))), .{ .rw = .write, .locality = 1 });
        }

        if (!rowPasses(views, r)) continue;
        const key = try keyFromViews(views, r);
        const probe = table.getOrPut(GroupTable.hashKey(key), key);
        const gid = if (probe.found) probe.gid else blk: {
            const new_gid = initGroupState(states, key);
            table.commit(probe.slot, key, new_gid);
            break :blk new_gid;
        };
        scratch.gids.appendAssumeCapacity(gid);
        scratch.row_idxs.appendAssumeCapacity(@intCast(r));
    }

    const gids = scratch.gids.items;
    const row_idxs = scratch.row_idxs.items;
    var j: usize = 0;
    while (j < gids.len) : (j += 1) {
        const pf = j + PREFETCH_DIST_DIRECT;
        if (pf < gids.len) @prefetch(&states.items[gids[pf]], .{ .rw = .write, .locality = 1 });
        const src = row_idxs[j];
        var st = &states.items[gids[j]];
        st.count += 1;
        st.refresh_sum += views.refresh[src];
        st.width_sum += views.width[src];
    }
}

fn groupRowsDirect(
    table: *GroupTable,
    states: *std.ArrayListUnmanaged(State),
    scratch: *GroupScratch,
    allocator: Allocator,
    rows: []const Row,
) !void {
    const n = rows.len;
    if (n == 0) return;
    if (table.needsGrow(n)) try table.grow(allocator, n);
    try states.ensureUnusedCapacity(allocator, n);
    _ = scratch;

    var r: usize = 0;
    while (r < n) : (r += 1) {
        const pf = r + PREFETCH_DIST_BUCKET;
        if (pf < n) {
            const pf_key = stagedRowKey(rows[pf]);
            @prefetch(table.slotAddr(table.bucketOf(GroupTable.hashKey(pf_key))), .{ .rw = .write, .locality = 1 });
        }

        const row = rows[r];
        const key = stagedRowKey(row);
        const probe = table.getOrPut(GroupTable.hashKey(key), key);
        if (!probe.found) {
            const new_gid = initGroupStateFromRow(states, key, row);
            table.commit(probe.slot, key, new_gid);
            continue;
        }
        const st = &states.items[probe.gid];
        st.count += 1;
        st.refresh_sum += row.refresh;
        st.width_sum += row.width;
    }
}

fn groupChunkRowsDirect(
    table: *GroupTable,
    states: *std.ArrayListUnmanaged(State),
    scratch: *GroupScratch,
    allocator: Allocator,
    rows: GroupRows,
) !void {
    const n = rows.len();
    if (n == 0) return;
    if (table.needsGrow(n)) try table.grow(allocator, n);
    try states.ensureUnusedCapacity(allocator, n);
    _ = scratch;

    if (rows.layout.columns.len > MAX_GROUP_PAYLOAD_COLUMNS) return error.UnsupportedOperatorForType;
    try validateGroupAggregateProgram(rows.layout.aggregates, rows.layout.columns.len);
    var column_slices_buf: [MAX_GROUP_PAYLOAD_COLUMNS][]const i16 = undefined;
    var col: usize = 0;
    while (col < rows.layout.columns.len) : (col += 1) {
        column_slices_buf[col] = switch (rows.layout.columns[col].physical_type) {
            .i16 => rows.columnI16Items(col),
        };
    }
    const column_slices = column_slices_buf[0..rows.layout.columns.len];
    if (countSumAvgProgram(rows.layout.aggregates, rows.layout.columns.len)) |program| {
        const sum_values = column_slices[program.sum_input_index];
        const avg_values = column_slices[program.avg_input_index];
        switch (rows.layout.key_width) {
            .u32 => try groupChunkRowsDirectKeysCountSumAvg(.u32, table, states, rows.keyU32All()[0..n], &.{}, n, sum_values, avg_values),
            .u64 => try groupChunkRowsDirectKeysCountSumAvg(.u64, table, states, rows.keyU64All()[0..n], &.{}, n, sum_values, avg_values),
            .u96 => try groupChunkRowsDirectKeysCountSumAvg(.u96, table, states, rows.keyU96LoAll()[0..n], rows.keyU96HiAll()[0..n], n, sum_values, avg_values),
            .u128 => try groupChunkRowsDirectKeysCountSumAvg(.u128, table, states, rows.keyU128All()[0..n], &.{}, n, sum_values, avg_values),
        }
        return;
    }
    switch (rows.layout.key_width) {
        .u32 => try groupChunkRowsDirectKeys(.u32, table, states, rows.keyU32All()[0..n], &.{}, n, rows.layout.aggregates, column_slices),
        .u64 => try groupChunkRowsDirectKeys(.u64, table, states, rows.keyU64All()[0..n], &.{}, n, rows.layout.aggregates, column_slices),
        .u96 => try groupChunkRowsDirectKeys(.u96, table, states, rows.keyU96LoAll()[0..n], rows.keyU96HiAll()[0..n], n, rows.layout.aggregates, column_slices),
        .u128 => try groupChunkRowsDirectKeys(.u128, table, states, rows.keyU128All()[0..n], &.{}, n, rows.layout.aggregates, column_slices),
    }
}

fn groupChunkRowsDirectKeysCountSumAvg(
    comptime key_width: GroupKeyWidth,
    table: *GroupTable,
    states: *std.ArrayListUnmanaged(State),
    keys: anytype,
    key_hi: []const u32,
    row_count: usize,
    sum_values: []const i16,
    avg_values: []const i16,
) !void {
    var r: usize = 0;
    while (r < row_count) : (r += 1) {
        const pf = r + PREFETCH_DIST_BUCKET;
        if (pf < row_count) {
            const pf_key = groupKeyAt(key_width, keys, key_hi, pf);
            @prefetch(table.slotAddr(table.bucketOf(GroupTable.hashKey(pf_key))), .{ .rw = .write, .locality = 1 });
        }

        const key = groupKeyAt(key_width, keys, key_hi, r);
        const probe = table.getOrPut(GroupTable.hashKey(key), key);
        if (!probe.found) {
            const new_gid = initGroupStateCountSumAvg(states, key, sum_values[r], avg_values[r]);
            table.commit(probe.slot, key, new_gid);
            continue;
        }
        updateGroupStateCountSumAvg(&states.items[probe.gid], sum_values[r], avg_values[r]);
    }
}

fn groupChunkRowsDirectKeys(
    comptime key_width: GroupKeyWidth,
    table: *GroupTable,
    states: *std.ArrayListUnmanaged(State),
    keys: anytype,
    key_hi: []const u32,
    row_count: usize,
    aggregates: []const GroupAggregateSpec,
    columns: []const []const i16,
) !void {
    const n = row_count;
    var r: usize = 0;
    while (r < n) : (r += 1) {
        const pf = r + PREFETCH_DIST_BUCKET;
        if (pf < n) {
            const pf_key = groupKeyAt(key_width, keys, key_hi, pf);
            @prefetch(table.slotAddr(table.bucketOf(GroupTable.hashKey(pf_key))), .{ .rw = .write, .locality = 1 });
        }

        const key = groupKeyAt(key_width, keys, key_hi, r);
        const probe = table.getOrPut(GroupTable.hashKey(key), key);
        if (!probe.found) {
            const new_gid = try initGroupStateProgram(states, key, aggregates, columns, r);
            table.commit(probe.slot, key, new_gid);
            continue;
        }
        const st = &states.items[probe.gid];
        try updateGroupStateProgram(st, aggregates, columns, r);
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

fn mergeStatesDirect(
    table: *GroupTable,
    states: *std.ArrayListUnmanaged(State),
    scratch: *GroupScratch,
    allocator: Allocator,
    input: []const State,
) !void {
    const n = input.len;
    if (n == 0) return;
    if (table.needsGrow(n)) try table.grow(allocator, n);
    try states.ensureUnusedCapacity(allocator, n);
    scratch.gids.clearRetainingCapacity();
    try scratch.gids.ensureTotalCapacity(allocator, n);

    var r: usize = 0;
    while (r < n) : (r += 1) {
        const pf = r + PREFETCH_DIST_BUCKET;
        if (pf < n) {
            const pf_key = input[pf].key;
            @prefetch(table.slotAddr(table.bucketOf(GroupTable.hashKey(pf_key))), .{ .rw = .write, .locality = 1 });
        }

        const key = input[r].key;
        const probe = table.getOrPut(GroupTable.hashKey(key), key);
        const gid = if (probe.found) probe.gid else blk: {
            const new_gid = initGroupState(states, key);
            table.commit(probe.slot, key, new_gid);
            break :blk new_gid;
        };
        scratch.gids.appendAssumeCapacity(gid);
    }

    const gids = scratch.gids.items;
    r = 0;
    while (r < n) : (r += 1) {
        const pf = r + PREFETCH_DIST_BUCKET;
        if (pf < n) @prefetch(&states.items[gids[pf]], .{ .rw = .write, .locality = 1 });
        const src = input[r];
        var st = &states.items[gids[r]];
        st.count += src.count;
        st.refresh_sum += src.refresh_sum;
        st.width_sum += src.width_sum;
        st.extra_sum += src.extra_sum;
    }
}

inline fn rowBucketIndex(row: Row, bucket_count: usize) usize {
    const h = routeHashRowBits(row.key_lo, row.key_hi);
    return if (std.math.isPowerOfTwo(bucket_count))
        (@as(usize, @truncate(h)) & (bucket_count - 1))
        else
        bucketIndexHash(h, bucket_count);
}

inline fn rawRowBucketIndex(rows: RawRows, row_idx: usize, bucket_count: usize) usize {
    const h = routeHashRowBits(rows.keyLoAll()[row_idx], rows.keyHiAll()[row_idx]);
    return if (std.math.isPowerOfTwo(bucket_count))
        (@as(usize, @truncate(h)) & (bucket_count - 1))
        else
        bucketIndexHash(h, bucket_count);
}

const ScanJob = struct {
    scan: *Scan,
    parts: *WorkerParts,
    allocator: Allocator,
    bucket_count: usize,
    kind: QueryKind,
    cpu: usize,
    err: *?anyerror,
};

fn scanWorker(job: ScanJob) void {
    pinToCpu(job.cpu);
    scanWorkerErr(job) catch |e| {
        job.err.* = e;
    };
}

fn scanWorkerErr(job: ScanJob) !void {
    while (true) {
        const t0 = nowTicks();
        const maybe_batch = try job.scan.next();
        job.parts.scan_ticks += nowTicks() - t0;
        const batch = maybe_batch orelse break;
        const p0 = nowTicks();
        try appendBatch(job.parts, job.allocator, job.bucket_count, job.kind, batch);
        job.parts.partition_ticks += nowTicks() - p0;
    }
}

const LateFilterJob = struct {
    filter_scan: *Scan,
    payload_scan: *Scan,
    parts: *WorkerParts,
    allocator: Allocator,
    bucket_count: usize,
    kind: QueryKind,
    cpu: usize,
    err: *?anyerror,
};

fn lateFilterWorker(job: LateFilterJob) void {
    pinToCpu(job.cpu);
    lateFilterWorkerErr(job) catch |e| {
        job.err.* = e;
    };
}

fn collectPhraseSurvivors(allocator: Allocator, phrase_batch: thindb.Batch, survivor_rows: *std.ArrayListUnmanaged(u32)) !void {
    survivor_rows.clearRetainingCapacity();
    try survivor_rows.ensureTotalCapacity(allocator, phrase_batch.row_count);
    const phrase = switch ((phrase_batch.columnView("SearchPhrase") orelse return error.ColumnNotFound).data) {
        .varchar, .string, .char => |v| v,
        else => return error.UnexpectedSearchPhraseType,
    };
    const offsets = phrase.offsets;
    var r: usize = 0;
    while (r < phrase_batch.row_count) : (r += 1) {
        if (offsets[r] != offsets[r + 1]) survivor_rows.appendAssumeCapacity(@intCast(r));
    }
}

fn lateFilterWorkerErr(job: LateFilterJob) !void {
    var payload_batch: ?thindb.Batch = null;
    var payload_pos: usize = 0;
    while (true) {
        const t0 = nowTicks();
        const maybe_filter = try job.filter_scan.next();
        job.parts.scan_ticks += nowTicks() - t0;
        if (maybe_filter == null) break;
        const phrase_batch = maybe_filter.?;
        job.parts.scanned_count += phrase_batch.row_count;
        const p0 = nowTicks();
        const phrase = switch ((phrase_batch.columnView("SearchPhrase") orelse return error.ColumnNotFound).data) {
            .varchar, .string, .char => |v| v,
            else => return error.UnexpectedSearchPhraseType,
        };
        const offsets = phrase.offsets;

        var phrase_pos: usize = 0;
        while (phrase_pos < phrase_batch.row_count) {
            if (payload_batch == null or payload_pos >= payload_batch.?.row_count) {
                const t_payload = nowTicks();
                payload_batch = try job.payload_scan.next();
                job.parts.scan_ticks += nowTicks() - t_payload;
                if (payload_batch == null) return error.QueryFailed;
                payload_pos = 0;
            }
            const pb = payload_batch.?;
            const payload = try loadPayloadViews(job.kind, pb);
            const take = @min(phrase_batch.row_count - phrase_pos, pb.row_count - payload_pos);
            switch (job.kind) {
                .q30 => try appendLateQ30(job.parts, job.allocator, job.bucket_count, payload, offsets, phrase_pos, payload_pos, take),
                .q31 => try appendLateQ31(job.parts, job.allocator, job.bucket_count, payload, offsets, phrase_pos, payload_pos, take),
                else => return error.InvalidArgument,
            }
            phrase_pos += take;
            payload_pos += take;
        }
        job.parts.partition_ticks += nowTicks() - p0;
    }
}

const PreAggScanJob = struct {
    scan: *Scan,
    agg: *WorkerAgg,
    allocator: Allocator,
    bucket_count: usize,
    table_expected: usize,
    kind: QueryKind,
    cpu: usize,
    err: *?anyerror,
};

fn preAggScanWorker(job: PreAggScanJob) void {
    pinToCpu(job.cpu);
    preAggScanWorkerErr(job) catch |e| {
        job.err.* = e;
    };
}

fn preAggScanWorkerErr(job: PreAggScanJob) !void {
    while (true) {
        const t0 = nowTicks();
        const maybe_batch = try job.scan.next();
        job.agg.scan_ticks += nowTicks() - t0;
        const batch = maybe_batch orelse break;
        job.agg.scanned_count += batch.row_count;
        const g0 = nowTicks();
        try groupBatchDirect(&job.agg.table, &job.agg.states, &job.agg.scratch, job.allocator, job.kind, batch);
        job.agg.row_count += job.agg.scratch.gids.items.len;
        job.agg.group_ticks += nowTicks() - g0;
        if (job.agg.states.items.len >= LOCAL_PREAGG_FLUSH_GROUPS) {
            try flushLocalPreAgg(job.allocator, job.agg, job.bucket_count, true, job.table_expected);
        }
    }
    try flushLocalPreAgg(job.allocator, job.agg, job.bucket_count, false, job.table_expected);
}

fn flushLocalPreAgg(allocator: Allocator, agg: *WorkerAgg, bucket_count: usize, reset_table: bool, table_expected: usize) !void {
    if (agg.states.items.len == 0) return;
    const p0 = nowTicks();
    if (std.math.isPowerOfTwo(bucket_count)) {
        const mask = bucket_count - 1;
        for (agg.states.items) |s| {
            const b = @as(usize, @truncate(GroupTable.hashKey(s.key))) & mask;
            try agg.buckets[b].states.append(allocator, s);
        }
    } else {
        for (agg.states.items) |s| {
            const b = bucketIndex(s.key, bucket_count);
            try agg.buckets[b].states.append(allocator, s);
        }
    }
    agg.partitioned_count += agg.states.items.len;
    agg.states.clearRetainingCapacity();
    agg.partition_ticks += nowTicks() - p0;
    if (reset_table) {
        agg.table.deinit(allocator);
        agg.table = try GroupTable.init(allocator, table_expected);
    }
}

const StreamJob = struct {
    scan: *Scan,
    local: *WorkerParts,
    central: []CentralBucket,
    allocator: Allocator,
    bucket_count: usize,
    kind: QueryKind,
    cpu: usize,
    err: *?anyerror,
};

fn streamWorker(job: StreamJob) void {
    pinToCpu(job.cpu);
    streamWorkerErr(job) catch |e| {
        job.err.* = e;
    };
}

fn flushLocalBuckets(job: StreamJob, scratch: *GroupScratch) !void {
    const offset = if (std.math.isPowerOfTwo(job.bucket_count)) job.cpu & (job.bucket_count - 1) else job.cpu % job.bucket_count;
    var k: usize = 0;
    while (k < job.bucket_count) : (k += 1) {
        const b = if (std.math.isPowerOfTwo(job.bucket_count)) (k + offset) & (job.bucket_count - 1) else (k + offset) % job.bucket_count;
        const rows = job.local.buckets[b].rows.items;
        if (rows.len == 0) continue;
        const central = &job.central[b];
        lockSpin(&central.mutex);
        groupRowsDirect(&central.table, &central.states, scratch, job.allocator, rows) catch |e| {
            central.mutex.unlock();
            return e;
        };
        central.mutex.unlock();
        job.local.buckets[b].rows.clearRetainingCapacity();
    }
}

fn streamWorkerErr(job: StreamJob) !void {
    var scratch: GroupScratch = .{};
    defer scratch.deinit(job.allocator);
    while (true) {
        const t0 = nowTicks();
        const maybe_batch = try job.scan.next();
        job.local.scan_ticks += nowTicks() - t0;
        const batch = maybe_batch orelse break;
        const p0 = nowTicks();
        try appendBatch(job.local, job.allocator, job.bucket_count, job.kind, batch);
        job.local.partition_ticks += nowTicks() - p0;
        const g0 = nowTicks();
        try flushLocalBuckets(job, &scratch);
        job.local.group_ticks += nowTicks() - g0;
    }
}

fn groupBucket(allocator: Allocator, parts: []WorkerParts, bucket_idx: usize) !BucketResult {
    var rows: usize = 0;
    for (parts) |*p| rows += p.buckets[bucket_idx].rows.items.len;
    if (rows == 0) return .{};

    var table = try GroupTable.init(allocator, @max(@as(usize, 16), rows / 6));
    defer table.deinit(allocator);

    var states: std.ArrayListUnmanaged(State) = .empty;
    defer states.deinit(allocator);
    try states.ensureTotalCapacity(allocator, @max(@as(usize, 16), rows / 8));
    var scratch: GroupScratch = .{};
    defer scratch.deinit(allocator);

    for (parts) |*p| {
        try groupRowsDirect(&table, &states, &scratch, allocator, p.buckets[bucket_idx].rows.items);
    }

    var result = BucketResult{
        .row_count = @intCast(rows),
        .group_count = @intCast(states.items.len),
    };
    for (states.items) |s| result.top.consider(topRowFromState(s));
    return result;
}

fn groupBucketTimed(allocator: Allocator, parts: []WorkerParts, bucket_idx: usize, preferred_part: usize, agg_ticks: *i64, local_top_ticks: *i64) !BucketResult {
    var rows: usize = 0;
    for (parts) |*p| rows += p.buckets[bucket_idx].rows.items.len;
    if (rows == 0) return .{};

    var table = try GroupTable.init(allocator, @max(@as(usize, 16), rows / 6));
    defer table.deinit(allocator);

    var states: std.ArrayListUnmanaged(State) = .empty;
    defer states.deinit(allocator);
    try states.ensureTotalCapacity(allocator, @max(@as(usize, 16), rows / 8));
    var scratch: GroupScratch = .{};
    defer scratch.deinit(allocator);

    const agg_t0 = nowTicks();
    var offset: usize = 0;
    while (offset < parts.len) : (offset += 1) {
        const part_idx = (preferred_part + offset) % parts.len;
        try groupRowsDirect(&table, &states, &scratch, allocator, parts[part_idx].buckets[bucket_idx].rows.items);
    }
    agg_ticks.* += nowTicks() - agg_t0;

    var result = BucketResult{
        .row_count = @intCast(rows),
        .group_count = @intCast(states.items.len),
    };
    const top_t0 = nowTicks();
    for (states.items) |s| result.top.consider(topRowFromState(s));
    local_top_ticks.* += nowTicks() - top_t0;
    return result;
}

fn groupBucketQ30Inline(allocator: Allocator, parts: []WorkerParts, bucket_idx: usize) !BucketResult {
    var rows: usize = 0;
    for (parts) |*p| rows += p.buckets[bucket_idx].rows.items.len;
    if (rows == 0) return .{};

    var table = try Q30InlineTable.init(allocator, @max(@as(usize, 16), rows / 6));
    defer table.deinit(allocator);

    var sentinel_seen = false;
    var sentinel_state: Q30InlineState = undefined;

    for (parts) |*p| {
        const items = p.buckets[bucket_idx].rows.items;
        if (items.len == 0) continue;
        try table.ensureFor(allocator, items.len);
        var i: usize = 0;
        while (i < items.len) : (i += 1) {
            const pf = i + PREFETCH_DIST_BUCKET;
            if (pf < items.len) table.prefetch(@truncate(stagedRowKey(items[pf])));

            const row = items[i];
            const key64: u64 = @truncate(stagedRowKey(row));
            if (key64 == Q30InlineTable.SENTINEL) {
                if (!sentinel_seen) {
                    sentinel_state = .{ .count = 0, .refresh_sum = 0, .width_sum = 0 };
                    sentinel_seen = true;
                }
                sentinel_state.count += 1;
                sentinel_state.refresh_sum += row.refresh;
                sentinel_state.width_sum += row.width;
                continue;
            }
            const entry = table.getOrPut(key64);
            if (!entry.found) entry.state.* = .{ .count = 0, .refresh_sum = 0, .width_sum = 0 };
            entry.state.count += 1;
            entry.state.refresh_sum += row.refresh;
            entry.state.width_sum += row.width;
        }
    }

    var result = BucketResult{
        .row_count = @intCast(rows),
        .group_count = @as(u32, @intCast(table.count())) + @intFromBool(sentinel_seen),
    };
    for (table.slots) |slot| {
        if (slot.key == Q30InlineTable.SENTINEL) continue;
        result.top.consider(.{
            .key = @as(u128, slot.key),
            .count = slot.state.count,
            .refresh_sum = slot.state.refresh_sum,
            .width_sum = slot.state.width_sum,
        });
    }
    if (sentinel_seen) {
        result.top.consider(.{
            .key = @as(u128, Q30InlineTable.SENTINEL),
            .count = sentinel_state.count,
            .refresh_sum = sentinel_state.refresh_sum,
            .width_sum = sentinel_state.width_sum,
        });
    }
    return result;
}

const GroupJob = struct {
    allocator: Allocator,
    parts: []WorkerParts,
    results: []BucketResult,
    next_bucket: *std.atomic.Value(usize),
    bucket_count: usize,
    q30_inline: bool,
    cpu: usize,
    err: *?anyerror,
};

const SiloStagedJob = struct {
    allocator: Allocator,
    parts: []WorkerParts,
    results: []BucketResult,
    worker_index: usize,
    worker_count: usize,
    bucket_count: usize,
    cpu: usize,
    agg_ticks: *i64,
    local_top_ticks: *i64,
    chunks: *u64,
    err: *?anyerror,
};

fn groupWorker(job: GroupJob) void {
    pinToCpu(job.cpu);
    groupWorkerErr(job) catch |e| {
        job.err.* = e;
    };
}

fn siloStagedWorker(job: SiloStagedJob) void {
    pinToCpu(job.cpu);
    siloStagedWorkerErr(job) catch |e| {
        job.err.* = e;
    };
}

fn groupWorkerErr(job: GroupJob) !void {
    while (true) {
        const b = job.next_bucket.fetchAdd(1, .monotonic);
        if (b >= job.bucket_count) break;
        job.results[b] = if (job.q30_inline)
            try groupBucketQ30Inline(job.allocator, job.parts, b)
        else
            try groupBucket(job.allocator, job.parts, b);
    }
}

fn siloStagedWorkerErr(job: SiloStagedJob) !void {
    var b = job.worker_index;
    while (b < job.bucket_count) : (b += job.worker_count) {
        var chunk_count: u64 = 0;
        for (job.parts) |*p| {
            if (p.buckets[b].rows.items.len != 0) chunk_count += 1;
        }
        job.chunks.* += chunk_count;
        job.results[b] = try groupBucketTimed(job.allocator, job.parts, b, job.worker_index % job.parts.len, job.agg_ticks, job.local_top_ticks);
    }
}

fn mergeStateBucket(allocator: Allocator, aggs: []WorkerAgg, bucket_idx: usize) !BucketResult {
    var rows: usize = 0;
    for (aggs) |*a| rows += a.buckets[bucket_idx].states.items.len;
    if (rows == 0) return .{};

    var table = try GroupTable.init(allocator, @max(@as(usize, 16), rows / 6));
    defer table.deinit(allocator);

    var states: std.ArrayListUnmanaged(State) = .empty;
    defer states.deinit(allocator);
    try states.ensureTotalCapacity(allocator, @max(@as(usize, 16), rows / 8));
    var scratch: GroupScratch = .{};
    defer scratch.deinit(allocator);

    for (aggs) |*a| {
        try mergeStatesDirect(&table, &states, &scratch, allocator, a.buckets[bucket_idx].states.items);
    }

    var result = BucketResult{
        .row_count = @intCast(rows),
        .group_count = @intCast(states.items.len),
    };
    for (states.items) |s| result.top.consider(topRowFromState(s));
    return result;
}

const PreAggMergeJob = struct {
    allocator: Allocator,
    aggs: []WorkerAgg,
    results: []BucketResult,
    next_bucket: *std.atomic.Value(usize),
    bucket_count: usize,
    cpu: usize,
    ticks: *i64,
    err: *?anyerror,
};

fn preAggMergeWorker(job: PreAggMergeJob) void {
    pinToCpu(job.cpu);
    preAggMergeWorkerErr(job) catch |e| {
        job.err.* = e;
    };
}

fn preAggMergeWorkerErr(job: PreAggMergeJob) !void {
    while (true) {
        const b = job.next_bucket.fetchAdd(1, .monotonic);
        if (b >= job.bucket_count) break;
        const t0 = nowTicks();
        job.results[b] = try mergeStateBucket(job.allocator, job.aggs, b);
        job.ticks.* += nowTicks() - t0;
    }
}

const PipeScanJob = struct {
    scan: *Scan,
    parts: *WorkerParts,
    shared: *PipeShared,
    kind: QueryKind,
    silo: bool,
    chunk_rows: usize,
    profile: bool,
    cpu: usize,
    err: *?anyerror,
};

fn pipeScanWorker(job: PipeScanJob) void {
    pinToCpu(job.cpu);
    pipeScanWorkerErr(job) catch |e| {
        job.err.* = e;
    };
}

fn pipeScanWorkerErr(job: PipeScanJob) !void {
    defer _ = job.shared.scans_done.fetchAdd(1, .release);
    defer {
        if (!job.silo) {
            flushPipeParts(job.shared, job.parts) catch |e| {
                job.err.* = e;
            };
        }
    }
    defer {
        if (job.silo) {
            const publish_t0 = if (job.profile) nowTicks() else 0;
            flushPipeParts(job.shared, job.parts) catch |e| {
                job.err.* = e;
            };
            if (job.profile) job.parts.publish_ticks += nowTicks() - publish_t0;
        }
    }

    while (true) {
        const t0 = if (job.profile) nowTicks() else 0;
        const maybe_batch = try job.scan.next();
        if (job.profile) job.parts.scan_ticks += nowTicks() - t0;
        const batch = maybe_batch orelse break;
        if (job.silo) {
            try appendBatchSilo(job.parts, job.shared, job.kind, batch, job.chunk_rows, job.profile);
        } else {
            const p0 = nowTicks();
            try appendBatchPipe(job.parts, job.shared, job.kind, batch, job.chunk_rows);
            job.parts.partition_ticks += nowTicks() - p0;
        }
    }
}

const PipeGroupJob = struct {
    allocator: Allocator,
    shared: *PipeShared,
    worker_index: usize,
    worker_count: usize,
    cpu: usize,
    ticks: *i64,
    err: *?anyerror,
};

const SiloGroupJob = struct {
    allocator: Allocator,
    shared: *PipeShared,
    worker_index: usize,
    worker_count: usize,
    cpu: usize,
    ticks: *i64,
    idle_ticks: *i64,
    top_ticks: *i64,
    chunks: *u64,
    top: *TopSet,
    profile: bool,
    err: *?anyerror,
};

const SiloElevatorJob = struct {
    scan: *Scan,
    local: *WorkerParts,
    shared: *PipeShared,
    worker_index: usize,
    worker_count: usize,
    kind: QueryKind,
    chunk_rows: usize,
    profile: bool,
    cpu: usize,
    group_ticks: *i64,
    idle_ticks: *i64,
    top_ticks: *i64,
    chunks: *u64,
    top: *TopSet,
    err: *?anyerror,
};

const SiloAdaptiveScanJob = struct {
    scan: *Scan,
    local: *WorkerParts,
    shared: *PipeShared,
    worker_index: usize,
    worker_count: usize,
    kind: QueryKind,
    chunk_rows: usize,
    profile: bool,
    cpu: usize,
    group_ticks: *i64,
    idle_ticks: *i64,
    top_ticks: *i64,
    chunks: *u64,
    top: *TopSet,
    err: *?anyerror,
};

const SiloAdaptiveGroupJob = struct {
    shared: *PipeShared,
    worker_index: usize,
    worker_count: usize,
    profile: bool,
    cpu: usize,
    group_ticks: *i64,
    idle_ticks: *i64,
    top_ticks: *i64,
    chunks: *u64,
    top: *TopSet,
    err: *?anyerror,
};

const SiloElasticJob = struct {
    scan: *Scan,
    local: *WorkerParts,
    shared: *PipeShared,
    worker_index: usize,
    worker_count: usize,
    kind: QueryKind,
    chunk_rows: usize,
    group_backlog_target: usize,
    profile: bool,
    cpu: usize,
    group_ticks: *i64,
    idle_ticks: *i64,
    top_ticks: *i64,
    chunks: *u64,
    top: *TopSet,
    err: *?anyerror,
};

const SiloGridJob = struct {
    scan: *Scan,
    local: *WorkerParts,
    shared: *PipeShared,
    seg_start: []const usize,
    segment_count: usize,
    worker_index: usize,
    worker_count: usize,
    kind: QueryKind,
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

fn pipeGroupWorker(job: PipeGroupJob) void {
    pinToCpu(job.cpu);
    pipeGroupWorkerErr(job) catch |e| {
        job.err.* = e;
    };
}

fn siloGroupWorker(job: SiloGroupJob) void {
    pinToCpu(job.cpu);
    siloGroupWorkerErr(job) catch |e| {
        job.err.* = e;
    };
}

fn siloElevatorWorker(job: SiloElevatorJob) void {
    pinToCpu(job.cpu);
    siloElevatorWorkerErr(job) catch |e| {
        job.err.* = e;
    };
}

fn siloAdaptiveScanWorker(job: SiloAdaptiveScanJob) void {
    pinToCpu(job.cpu);
    siloAdaptiveScanWorkerErr(job) catch |e| {
        job.err.* = e;
    };
}

fn siloAdaptiveGroupWorker(job: SiloAdaptiveGroupJob) void {
    pinToCpu(job.cpu);
    siloAdaptiveGroupWorkerErr(job) catch |e| {
        job.err.* = e;
    };
}

fn siloElasticWorker(job: SiloElasticJob) void {
    pinToCpu(job.cpu);
    siloElasticWorkerErr(job) catch |e| {
        job.err.* = e;
    };
}

fn siloGridWorker(job: SiloGridJob) void {
    pinToCpu(job.cpu);
    siloGridWorkerErr(job) catch |e| {
        job.err.* = e;
    };
}

fn popPipeChunk(bucket: *PipeBucket) ?PipeChunk {
    lockSpin(&bucket.queue_lock);
    defer bucket.queue_lock.unlock();
    const len = bucket.chunks.items.len;
    if (len == 0) return null;
    const chunk = bucket.chunks.items[len - 1];
    bucket.chunks.items.len = len - 1;
    bucket.queued_rows -= chunk.rows.items.len;
    return chunk;
}

fn pipeGroupWorkerErr(job: PipeGroupJob) !void {
    var scratch: GroupScratch = .{};
    defer scratch.deinit(job.allocator);

    const bucket_count = job.shared.bucket_count;
    var cursor = (job.worker_index * 17) % bucket_count;
    var idle_spins: usize = 0;

    while (true) {
        if (job.shared.scans_done.load(.acquire) == job.shared.scan_threads and
            job.shared.outstanding_chunks.load(.acquire) == 0)
        {
            break;
        }

        var did_work = false;
        var checked: usize = 0;
        while (checked < bucket_count) : (checked += 1) {
            const b = cursor;
            cursor += 1;
            if (cursor == bucket_count) cursor = 0;

            const bucket = &job.shared.buckets[b];
            if (!bucket.agg_lock.tryLock()) continue;
            var owned_work = false;
            while (popPipeChunk(bucket)) |chunk| {
                owned_work = true;
                const g0 = nowTicks();
                try groupRowsDirect(&bucket.table, &bucket.states, &scratch, job.allocator, chunk.rows.items);
                bucket.row_count += chunk.rows.items.len;
                job.ticks.* += nowTicks() - g0;
                _ = job.shared.outstanding_rows.fetchSub(@intCast(chunk.rows.items.len), .release);
                try recycleChunkRows(job.shared, chunk.owner_worker, chunk.rows);
                _ = job.shared.outstanding_chunks.fetchSub(1, .release);
            }
            bucket.agg_lock.unlock();
            if (owned_work) {
                did_work = true;
                break;
            }
        }

        if (did_work) {
            idle_spins = 0;
        } else {
            idle_spins += 1;
            if (idle_spins < 256) {
                std.atomic.spinLoopHint();
            } else {
                std.Thread.yield() catch {};
                idle_spins = 0;
            }
        }
    }
}

fn siloGroupWorkerErr(job: SiloGroupJob) !void {
    var scratch: GroupScratch = .{};
    defer scratch.deinit(job.allocator);

    const bucket_count = job.shared.bucket_count;
    var idle_spins: usize = 0;

    while (true) {
        var did_work = false;

        var b = job.worker_index;
        while (b < bucket_count) : (b += job.worker_count) {
            const bucket = &job.shared.buckets[b];
            while (popPipeChunk(bucket)) |chunk| {
                did_work = true;
                job.chunks.* += 1;
                const g0 = if (job.profile) nowTicks() else 0;
                try groupRowsDirect(&bucket.table, &bucket.states, &scratch, job.allocator, chunk.rows.items);
                bucket.row_count += chunk.rows.items.len;
                if (job.profile) job.ticks.* += nowTicks() - g0;
                _ = job.shared.outstanding_rows.fetchSub(@intCast(chunk.rows.items.len), .release);
                try recycleChunkRows(job.shared, chunk.owner_worker, chunk.rows);
                _ = job.shared.outstanding_chunks.fetchSub(1, .release);
            }
        }

        if (job.shared.scans_done.load(.acquire) == job.shared.scan_threads and
            job.shared.outstanding_chunks.load(.acquire) == 0)
        {
            break;
        }

        if (did_work) {
            idle_spins = 0;
        } else {
            const idle_t0 = if (job.profile) nowTicks() else 0;
            idle_spins += 1;
            if (idle_spins < 256) {
                std.atomic.spinLoopHint();
            } else {
                std.Thread.yield() catch {};
                idle_spins = 0;
            }
            if (job.profile) job.idle_ticks.* += nowTicks() - idle_t0;
        }
    }

    const top_t0 = if (job.profile) nowTicks() else 0;
    var local_top: TopSet = .{};
    var b = job.worker_index;
    while (b < bucket_count) : (b += job.worker_count) {
        for (job.shared.buckets[b].states.items) |s| local_top.consider(topRowFromState(s));
    }
    job.top.* = local_top;
    if (job.profile) job.top_ticks.* += nowTicks() - top_t0;
}

fn drainOwnedSilos(
    allocator: Allocator,
    shared: *PipeShared,
    worker_index: usize,
    worker_count: usize,
    scratch: *GroupScratch,
    group_ticks: *i64,
    chunks: *u64,
    profile: bool,
) !bool {
    var did_work = false;
    var b = worker_index;
    while (b < shared.bucket_count) : (b += worker_count) {
        const bucket = &shared.buckets[b];
        while (popPipeChunk(bucket)) |chunk| {
            did_work = true;
            chunks.* += 1;
            const g0 = if (profile) nowTicks() else 0;
            try groupRowsDirect(&bucket.table, &bucket.states, scratch, allocator, chunk.rows.items);
            bucket.row_count += chunk.rows.items.len;
            if (profile) group_ticks.* += nowTicks() - g0;
            _ = shared.outstanding_rows.fetchSub(@intCast(chunk.rows.items.len), .release);
            try recycleChunkRows(shared, chunk.owner_worker, chunk.rows);
            _ = shared.outstanding_chunks.fetchSub(1, .release);
        }
    }
    return did_work;
}

fn drainGridLeasedSilos(
    allocator: Allocator,
    shared: *PipeShared,
    scratch: *GroupScratch,
    cursor: *usize,
    max_leases: usize,
    group_ticks: *i64,
    chunks: *u64,
    profile: bool,
) !bool {
    var did_work = false;
    var leases: usize = 0;
    var checked: usize = 0;
    while (checked < shared.bucket_count and leases < max_leases) : (checked += 1) {
        const b = cursor.*;
        cursor.* = b + 1;
        if (cursor.* == shared.bucket_count) cursor.* = 0;

        const bucket = &shared.buckets[b];
        if (!bucket.agg_lock.tryLock()) continue;

        var bucket_work = false;
        while (popPipeChunk(bucket)) |chunk| {
            bucket_work = true;
            did_work = true;
            chunks.* += 1;
            const g0 = if (profile) nowTicks() else 0;
            try groupRowsDirect(&bucket.table, &bucket.states, scratch, allocator, chunk.rows.items);
            bucket.row_count += chunk.rows.items.len;
            if (profile) group_ticks.* += nowTicks() - g0;
            _ = shared.outstanding_rows.fetchSub(@intCast(chunk.rows.items.len), .release);
            try recycleChunkRows(shared, chunk.owner_worker, chunk.rows);
            _ = shared.outstanding_chunks.fetchSub(1, .release);
        }
        bucket.agg_lock.unlock();
        if (bucket_work) leases += 1;
    }
    return did_work;
}

fn queuedBucketRows(bucket: *PipeBucket) ?u64 {
    if (!bucket.queue_lock.tryLock()) return null;
    const rows = bucket.queued_rows;
    bucket.queue_lock.unlock();
    return rows;
}

fn drainLeasedSilos(
    allocator: Allocator,
    shared: *PipeShared,
    scratch: *GroupScratch,
    cursor: *usize,
    group_ticks: *i64,
    chunks: *u64,
    max_buckets_raw: usize,
    target_rows: u64,
    sched_pick_ticks: ?*i64,
    sched_lock_ticks: ?*i64,
    profile: bool,
) !bool {
    const max_buckets = @max(@as(usize, 1), @min(max_buckets_raw, MAX_GROUP_LEASE_BUCKETS));
    const pick_t0 = if (profile) nowTicks() else 0;

    var buckets_buf: [MAX_GROUP_LEASE_BUCKETS]usize = undefined;
    var rows_buf: [MAX_GROUP_LEASE_BUCKETS]u64 = undefined;
    var selected: usize = 0;
    var selected_rows: u64 = 0;
    var checked: usize = 0;

    while (checked < shared.bucket_count) : (checked += 1) {
        const b = (cursor.* + checked) % shared.bucket_count;
        const q = queuedBucketRows(&shared.buckets[b]) orelse continue;
        if (q == 0) continue;

        var insert_at: usize = selected;
        if (selected == max_buckets) {
            var min_i: usize = 0;
            var i: usize = 1;
            while (i < selected) : (i += 1) {
                if (rows_buf[i] < rows_buf[min_i]) min_i = i;
            }
            if (q <= rows_buf[min_i]) continue;
            insert_at = min_i;
        }

        const lock_t0 = if (profile) nowTicks() else 0;
        if (!shared.buckets[b].agg_lock.tryLock()) {
            if (profile) {
                if (sched_lock_ticks) |ticks| ticks.* += nowTicks() - lock_t0;
            }
            continue;
        }
        if (profile) {
            if (sched_lock_ticks) |ticks| ticks.* += nowTicks() - lock_t0;
        }
        if (selected == max_buckets) {
            selected_rows -= rows_buf[insert_at];
            shared.buckets[buckets_buf[insert_at]].agg_lock.unlock();
        } else {
            selected += 1;
        }
        buckets_buf[insert_at] = b;
        rows_buf[insert_at] = q;
        selected_rows += q;
    }

    var sort_i: usize = 1;
    while (sort_i < selected) : (sort_i += 1) {
        const bucket = buckets_buf[sort_i];
        const rows = rows_buf[sort_i];
        var sort_j = sort_i;
        while (sort_j > 0 and rows_buf[sort_j - 1] < rows) : (sort_j -= 1) {
            buckets_buf[sort_j] = buckets_buf[sort_j - 1];
            rows_buf[sort_j] = rows_buf[sort_j - 1];
        }
        buckets_buf[sort_j] = bucket;
        rows_buf[sort_j] = rows;
    }

    if (target_rows > 0 and selected > 0) {
        var keep: usize = 0;
        var keep_rows: u64 = 0;
        while (keep < selected and keep_rows < target_rows) : (keep += 1) {
            keep_rows += rows_buf[keep];
        }
        var unlock_i = keep;
        while (unlock_i < selected) : (unlock_i += 1) {
            shared.buckets[buckets_buf[unlock_i]].agg_lock.unlock();
        }
        selected = keep;
        selected_rows = keep_rows;
    }

    if (profile) {
        if (sched_pick_ticks) |ticks| ticks.* += nowTicks() - pick_t0;
    }
    if (selected == 0) return false;
    cursor.* = (buckets_buf[selected - 1] + 1) % shared.bucket_count;

    _ = shared.active_group_jobs.fetchAdd(1, .release);
    defer _ = shared.active_group_jobs.fetchSub(1, .release);
    var did_work = false;
    var selected_i: usize = 0;
    while (selected_i < selected) : (selected_i += 1) {
        const bucket = &shared.buckets[buckets_buf[selected_i]];
        while (popPipeChunk(bucket)) |chunk| {
            did_work = true;
            chunks.* += 1;
            const g0 = if (profile) nowTicks() else 0;
            try groupRowsDirect(&bucket.table, &bucket.states, scratch, allocator, chunk.rows.items);
            bucket.row_count += chunk.rows.items.len;
            if (profile) group_ticks.* += nowTicks() - g0;
            _ = shared.outstanding_rows.fetchSub(@intCast(chunk.rows.items.len), .release);
            try recycleChunkRows(shared, chunk.owner_worker, chunk.rows);
            _ = shared.outstanding_chunks.fetchSub(1, .release);
        }
        bucket.agg_lock.unlock();
    }
    return did_work;
}

fn drainNextFinalLocalBucket(
    allocator: Allocator,
    shared: *PipeShared,
    scratch: *GroupScratch,
    group_ticks: *i64,
    chunks: *u64,
    profile: bool,
) !bool {
    while (true) {
        _ = shared.active_group_jobs.fetchAdd(1, .release);
        const b = shared.next_final_local_bucket.fetchAdd(1, .monotonic);
        if (b >= shared.bucket_count) {
            _ = shared.active_group_jobs.fetchSub(1, .release);
            return false;
        }

        const bucket = &shared.buckets[b];
        if (shared.shared_scan_buffers) |shared_buffers| {
            var did_work = false;
            var bank: usize = 0;
            while (bank < shared_buffers.bank_count) : (bank += 1) {
                const scan_bucket = shared_buffers.at(bank, b);
                const rows = scan_bucket.rows.items;
                if (rows.len == 0) continue;
                did_work = true;
                chunks.* += 1;
                const g0 = if (profile) nowTicks() else 0;
                try groupRowsDirect(&bucket.table, &bucket.states, scratch, allocator, rows);
                bucket.row_count += rows.len;
                if (profile) group_ticks.* += nowTicks() - g0;
                const row_count: u64 = @intCast(rows.len);
                _ = shared.scan_buffered_rows.fetchSub(row_count, .release);
                scan_bucket.rows.clearRetainingCapacity();
            }
            _ = shared.active_group_jobs.fetchSub(1, .release);
            if (did_work) return true;
            continue;
        }

        var did_work = false;
        for (shared.local_parts) |*part| {
            if (part.flat_scan_partitions) {
                if (!part.flat_partitioned) continue;
                const count: usize = part.flat_counts[b];
                if (count == 0) continue;
                const start: usize = part.flat_offsets[b];
                const rows = part.flat_rows.items[start .. start + count];
                did_work = true;
                chunks.* += 1;
                const g0 = if (profile) nowTicks() else 0;
                try groupRowsDirect(&bucket.table, &bucket.states, scratch, allocator, rows);
                bucket.row_count += rows.len;
                if (profile) group_ticks.* += nowTicks() - g0;
                const row_count: u64 = @intCast(rows.len);
                if (part.local_buffered_rows >= row_count) part.local_buffered_rows -= row_count;
                _ = shared.scan_buffered_rows.fetchSub(row_count, .release);
                continue;
            }
            const rows = part.buckets[b].rows.items;
            if (rows.len == 0) continue;
            did_work = true;
            chunks.* += 1;
            const g0 = if (profile) nowTicks() else 0;
            try groupRowsDirect(&bucket.table, &bucket.states, scratch, allocator, rows);
            bucket.row_count += rows.len;
            if (profile) group_ticks.* += nowTicks() - g0;
            const row_count: u64 = @intCast(rows.len);
            if (part.local_buffered_rows >= row_count) part.local_buffered_rows -= row_count;
            _ = shared.scan_buffered_rows.fetchSub(row_count, .release);
            part.buckets[b].rows.clearRetainingCapacity();
            if (part.dirty_marks.len > b) part.dirty_marks[b] = false;
        }
        _ = shared.active_group_jobs.fetchSub(1, .release);
        if (did_work) return true;
    }
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

fn appendSharedStageBuilderSlice(
    shared: *PipeShared,
    local: *WorkerParts,
    bucket_idx: usize,
    rows: []const Row,
    raw_group_chunk_rows: usize,
    profile: bool,
) !void {
    if (rows.len == 0 or bucket_idx >= shared.stage_builders.len) return;
    const builder = &shared.stage_builders[bucket_idx];
    const lock_t0 = if (profile) nowTicks() else 0;
    lockSpin(&builder.lock);
    if (profile) local.raw_stage_builder_lock_ticks += nowTicks() - lock_t0;
    defer builder.lock.unlock();

    var pos: usize = 0;
    while (pos < rows.len) {
        if (builder.rows.capacity() == 0) {
            builder.rows = try acquireGroupRows(shared, raw_group_chunk_rows, &local.raw_recycle_lock_ticks);
        }
        if (builder.rows.len() >= raw_group_chunk_rows) {
            try publishSharedStageBuilderLocked(shared, builder, bucket_idx, raw_group_chunk_rows, &local.raw_group_queue_lock_ticks, &local.raw_recycle_lock_ticks);
            continue;
        }

        const free = raw_group_chunk_rows - builder.rows.len();
        const take = @min(free, rows.len - pos);
        try builder.rows.appendRows(shared.allocator, shared.group_rows_layout, rows[pos .. pos + take]);
        _ = shared.stage_builder_rows.fetchAdd(@intCast(take), .release);
        pos += take;

        if (builder.rows.len() >= raw_group_chunk_rows) {
            try publishSharedStageBuilderLocked(shared, builder, bucket_idx, raw_group_chunk_rows, &local.raw_group_queue_lock_ticks, &local.raw_recycle_lock_ticks);
        }
    }
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

fn publishRawStageFinalBucket(
    shared: *PipeShared,
    local: *WorkerParts,
    bucket_idx: usize,
    raw_group_chunk_rows: usize,
    queue_lock_ticks: ?*i64,
) !void {
    if (bucket_idx >= local.buckets.len or bucket_idx >= shared.raw_group_queues.len) return;
    const row_count = local.buckets[bucket_idx].rows.items.len;
    if (row_count == 0) return;

    const rows_aos = local.buckets[bucket_idx].rows;
    var rows = try acquireGroupRows(shared, raw_group_chunk_rows, &local.raw_recycle_lock_ticks);
    try rows.appendRows(shared.allocator, shared.group_rows_layout, rows_aos.items);
    local.buckets[bucket_idx].rows.clearRetainingCapacity();
    local.dirty_marks[bucket_idx] = false;
    local.raw_stage_buffered_rows -= row_count;

    try publishGroupChunkToQueue(
        shared,
        &shared.raw_group_queues[bucket_idx],
        .{ .rows = rows, .owner_worker = local.worker_index, .bucket_idx = bucket_idx },
        &shared.stage_outstanding_chunks,
        &shared.stage_outstanding_rows,
        queue_lock_ticks,
    );
}

fn appendRawStageFinalRow(
    shared: *PipeShared,
    local: *WorkerParts,
    bucket_idx: usize,
    row: Row,
    raw_group_chunk_rows: usize,
    queue_lock_ticks: ?*i64,
) !void {
    if (bucket_idx >= local.buckets.len) return;
    var bucket = &local.buckets[bucket_idx];
    if (bucket.rows.capacity == 0) {
        try bucket.rows.ensureTotalCapacity(shared.allocator, raw_group_chunk_rows);
    }
    if (!local.dirty_marks[bucket_idx]) {
        local.dirty_marks[bucket_idx] = true;
        try local.dirty_buckets.append(shared.allocator, bucket_idx);
    }
    if (bucket.rows.items.len < bucket.rows.capacity) {
        bucket.rows.appendAssumeCapacity(row);
    } else {
        try bucket.rows.append(shared.allocator, row);
    }
    local.raw_stage_buffered_rows += 1;
    if (bucket.rows.items.len >= raw_group_chunk_rows) {
        try publishRawStageFinalBucket(shared, local, bucket_idx, raw_group_chunk_rows, queue_lock_ticks);
    }
}

fn flushRawStageFinalBuckets(
    shared: *PipeShared,
    local: *WorkerParts,
    raw_group_chunk_rows: usize,
    profile: bool,
) !bool {
    if (local.raw_stage_buffered_rows == 0) return false;
    const publish_t0 = if (profile) nowTicks() else 0;
    var kept: usize = 0;
    var flushed = false;
    for (local.dirty_buckets.items) |bucket_idx| {
        if (bucket_idx >= local.dirty_marks.len or !local.dirty_marks[bucket_idx]) continue;
        if (bucket_idx < local.buckets.len and local.buckets[bucket_idx].rows.items.len > 0) {
            try publishRawStageFinalBucket(shared, local, bucket_idx, raw_group_chunk_rows, &local.raw_group_queue_lock_ticks);
            flushed = true;
        } else {
            local.dirty_marks[bucket_idx] = false;
        }
        if (bucket_idx < local.dirty_marks.len and local.dirty_marks[bucket_idx]) {
            local.dirty_buckets.items[kept] = bucket_idx;
            kept += 1;
        }
    }
    local.dirty_buckets.items.len = kept;
    if (profile) local.raw_stage_publish_ticks += nowTicks() - publish_t0;
    return flushed;
}

fn drainRawDedicatedStageFinalBuckets(
    allocator: Allocator,
    shared: *PipeShared,
    local: *WorkerParts,
    worker_index: usize,
    scan_lane: usize,
    raw_chunk_rows: usize,
    raw_group_chunk_rows: usize,
    raw_batch_chunks: usize,
    profile: bool,
) !bool {
    _ = allocator;
    _ = worker_index;
    var raw_chunks: [MAX_RAW_BATCH_CHUNKS]RawChunk = undefined;
    if (scan_lane >= shared.raw_scan_queues.len) return false;
    defer releaseRawQueueLane(shared.raw_scan_queues, scan_lane);

    var pop_t0 = if (profile) nowTicks() else 0;
    var popped_total = popRawChunkBatchFromQueue(&shared.raw_scan_queues[scan_lane], &raw_chunks, raw_batch_chunks, &local.raw_scan_queue_lock_ticks);
    if (profile) local.raw_stage_pop_ticks += nowTicks() - pop_t0;
    if (popped_total == 0) return false;

    _ = shared.active_stage_jobs.fetchAdd(1, .release);
    defer _ = shared.active_stage_jobs.fetchSub(1, .release);

    const final_bucket_count = @min(shared.bucket_count, shared.raw_group_queues.len);
    while (popped_total > 0) {
        local.raw_stage_input_chunks += @intCast(popped_total);

        var total_rows: u64 = 0;
        var i: usize = 0;
        while (i < popped_total) : (i += 1) total_rows += @intCast(raw_chunks[i].rows.len());
        _ = shared.outstanding_rows.fetchSub(total_rows, .release);
        _ = shared.outstanding_chunks.fetchSub(popped_total, .release);

        const stage_t0 = if (profile) nowTicks() else 0;
        i = 0;
        while (i < popped_total) : (i += 1) {
            var r: usize = 0;
            while (r < raw_chunks[i].rows.len()) : (r += 1) {
                const row = raw_chunks[i].rows.rowAt(r);
                const b = rowBucketIndex(row, final_bucket_count);
                try appendRawStageFinalRow(shared, local, b, row, raw_group_chunk_rows, &local.raw_group_queue_lock_ticks);
            }
        }
        if (profile) local.raw_stage_ticks += nowTicks() - stage_t0;

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
        i = 0;
        const ncols = shared.group_rows_layout.columns.len;
        const flat_key_lo = local.flat_raw_rows.keyLoAll();
        const flat_key_hi = local.flat_raw_rows.keyHiAll();
        var flat_cols: [MAX_GROUP_PAYLOAD_COLUMNS][]i16 = undefined;
        for (0..ncols) |c| flat_cols[c] = local.flat_raw_rows.columnI16All(c);
        while (i < popped_total) : (i += 1) {
            const src_key_lo = raw_chunks[i].rows.keyLoAll();
            const src_key_hi = raw_chunks[i].rows.keyHiAll();
            var src_cols: [MAX_GROUP_PAYLOAD_COLUMNS][]const i16 = undefined;
            for (0..ncols) |c| src_cols[c] = raw_chunks[i].rows.columnI16All(c);
            const chunk_rows_n = raw_chunks[i].rows.len();
            var r: usize = 0;
            while (r < chunk_rows_n) : (r += 1) {
                const bucket_idx: usize = local.flat_bucket_ids.items[row_idx];
                const pos = local.flat_next[bucket_idx];
                flat_key_lo[pos] = src_key_lo[r];
                flat_key_hi[pos] = src_key_hi[r];
                var c: usize = 0;
                while (c < ncols) : (c += 1) flat_cols[c][pos] = src_cols[c][r];
                local.flat_next[bucket_idx] = pos + 1;
                row_idx += 1;
            }
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
        try groupChunkRowsDirect(&bucket.table, &bucket.states, scratch, allocator, rows);
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

fn waitAndDrainOwnedSilos(
    allocator: Allocator,
    shared: *PipeShared,
    worker_index: usize,
    worker_count: usize,
    profile: bool,
    scratch: *GroupScratch,
    group_ticks: *i64,
    idle_ticks: *i64,
    chunks: *u64,
) !void {
    var idle_spins: usize = 0;
    while (true) {
        const did_work = try drainOwnedSilos(allocator, shared, worker_index, worker_count, scratch, group_ticks, chunks, profile);
        if (shared.scans_done.load(.acquire) == shared.scan_threads and !did_work) break;
        if (did_work) {
            idle_spins = 0;
        } else {
            const idle_t0 = if (profile) nowTicks() else 0;
            idle_spins += 1;
            if (idle_spins < 256) {
                std.atomic.spinLoopHint();
            } else {
                std.Thread.yield() catch {};
                idle_spins = 0;
            }
            if (profile) idle_ticks.* += nowTicks() - idle_t0;
        }
    }
}

fn collectOwnedTop(shared: *PipeShared, worker_index: usize, worker_count: usize, top_out: *TopSet, top_ticks: *i64, profile: bool) void {
    const top_t0 = if (profile) nowTicks() else 0;
    var local_top: TopSet = .{};
    var b = worker_index;
    while (b < shared.buckets.len) : (b += worker_count) {
        for (shared.buckets[b].states.items) |s| local_top.consider(topRowFromState(s));
    }
    top_out.* = local_top;
    if (profile) top_ticks.* += nowTicks() - top_t0;
}

fn siloElevatorWorkerErr(job: SiloElevatorJob) !void {
    var scratch: GroupScratch = .{};
    defer scratch.deinit(job.shared.allocator);

    while (true) {
        const t0 = if (job.profile) nowTicks() else 0;
        const maybe_batch = try job.scan.next();
        if (job.profile) job.local.scan_ticks += nowTicks() - t0;
        const batch = maybe_batch orelse break;

        try appendBatchSilo(job.local, job.shared, job.kind, batch, job.chunk_rows, job.profile);
        _ = try drainOwnedSilos(job.shared.allocator, job.shared, job.worker_index, job.worker_count, &scratch, job.group_ticks, job.chunks, job.profile);
    }

    const publish_t0 = if (job.profile) nowTicks() else 0;
    try flushPipeParts(job.shared, job.local);
    if (job.profile) job.local.publish_ticks += nowTicks() - publish_t0;
    _ = job.shared.scans_done.fetchAdd(1, .release);

    var idle_spins: usize = 0;
    while (true) {
        const did_work = try drainOwnedSilos(job.shared.allocator, job.shared, job.worker_index, job.worker_count, &scratch, job.group_ticks, job.chunks, job.profile);
        if (job.shared.scans_done.load(.acquire) == job.shared.scan_threads and !did_work) break;
        if (did_work) {
            idle_spins = 0;
        } else {
            const idle_t0 = if (job.profile) nowTicks() else 0;
            idle_spins += 1;
            if (idle_spins < 256) {
                std.atomic.spinLoopHint();
            } else {
                std.Thread.yield() catch {};
                idle_spins = 0;
            }
            if (job.profile) job.idle_ticks.* += nowTicks() - idle_t0;
        }
    }

    const top_t0 = if (job.profile) nowTicks() else 0;
    var local_top: TopSet = .{};
    var b = job.worker_index;
    while (b < job.shared.bucket_count) : (b += job.worker_count) {
        for (job.shared.buckets[b].states.items) |s| local_top.consider(topRowFromState(s));
    }
    job.top.* = local_top;
    if (job.profile) job.top_ticks.* += nowTicks() - top_t0;
}

fn siloAdaptiveScanWorkerErr(job: SiloAdaptiveScanJob) !void {
    var scratch: GroupScratch = .{};
    defer scratch.deinit(job.shared.allocator);

    while (true) {
        const t0 = if (job.profile) nowTicks() else 0;
        const maybe_batch = try job.scan.next();
        if (job.profile) job.local.scan_ticks += nowTicks() - t0;
        const batch = maybe_batch orelse break;
        try appendBatchSilo(job.local, job.shared, job.kind, batch, job.chunk_rows, job.profile);
    }

    const publish_t0 = if (job.profile) nowTicks() else 0;
    try flushPipeParts(job.shared, job.local);
    if (job.profile) job.local.publish_ticks += nowTicks() - publish_t0;
    _ = job.shared.scans_done.fetchAdd(1, .release);

    try waitAndDrainOwnedSilos(job.shared.allocator, job.shared, job.worker_index, job.worker_count, job.profile, &scratch, job.group_ticks, job.idle_ticks, job.chunks);
    collectOwnedTop(job.shared, job.worker_index, job.worker_count, job.top, job.top_ticks, job.profile);
}

fn siloAdaptiveGroupWorkerErr(job: SiloAdaptiveGroupJob) !void {
    var scratch: GroupScratch = .{};
    defer scratch.deinit(job.shared.allocator);
    try waitAndDrainOwnedSilos(job.shared.allocator, job.shared, job.worker_index, job.worker_count, job.profile, &scratch, job.group_ticks, job.idle_ticks, job.chunks);
    collectOwnedTop(job.shared, job.worker_index, job.worker_count, job.top, job.top_ticks, job.profile);
}

fn siloElasticWorkerErr(job: SiloElasticJob) !void {
    var scratch: GroupScratch = .{};
    defer scratch.deinit(job.shared.allocator);

    var cursor = (job.worker_index * 17) % job.shared.bucket_count;
    var scan_done = false;
    var scan_marked_done = false;
    var idle_spins: usize = 0;

    while (true) {
        const all_scans_done = job.shared.scans_done.load(.acquire) == job.shared.scan_threads;
        const outstanding = job.shared.outstanding_chunks.load(.acquire);
        if (all_scans_done and outstanding == 0) break;

        const should_group =
            outstanding > 0 and (scan_done or outstanding >= job.group_backlog_target);

        if (should_group) {
            const max_leases: usize = if (scan_done) 8 else 2;
            if (try drainGridLeasedSilos(
                job.shared.allocator,
                job.shared,
                &scratch,
                &cursor,
                max_leases,
                job.group_ticks,
                job.chunks,
                job.profile,
            )) {
                idle_spins = 0;
                continue;
            }
        }

        if (!scan_done) {
            const t0 = if (job.profile) nowTicks() else 0;
            const maybe_batch = try job.scan.next();
            if (job.profile) job.local.scan_ticks += nowTicks() - t0;
            if (maybe_batch) |batch| {
                try appendBatchSilo(job.local, job.shared, job.kind, batch, job.chunk_rows, job.profile);
                idle_spins = 0;
                continue;
            }

            scan_done = true;
            const publish_t0 = if (job.profile) nowTicks() else 0;
            try flushPipeParts(job.shared, job.local);
            if (job.profile) job.local.publish_ticks += nowTicks() - publish_t0;
            _ = job.shared.scans_done.fetchAdd(1, .release);
            scan_marked_done = true;
            continue;
        }

        if (outstanding > 0) {
            if (try drainGridLeasedSilos(
                job.shared.allocator,
                job.shared,
                &scratch,
                &cursor,
                8,
                job.group_ticks,
                job.chunks,
                job.profile,
            )) {
                idle_spins = 0;
                continue;
            }
        }

        const idle_t0 = if (job.profile) nowTicks() else 0;
        idle_spins += 1;
        if (idle_spins < 256) {
            std.atomic.spinLoopHint();
        } else {
            std.Thread.yield() catch {};
            idle_spins = 0;
        }
        if (job.profile) job.idle_ticks.* += nowTicks() - idle_t0;
    }

    if (!scan_marked_done) {
        try flushPipeParts(job.shared, job.local);
        _ = job.shared.scans_done.fetchAdd(1, .release);
    }

    collectOwnedTop(job.shared, job.worker_index, job.worker_count, job.top, job.top_ticks, job.profile);
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
        const maybe_batch = try job.scan.next();
        if (job.profile) job.local.scan_ticks += nowTicks() - t0;
        const batch = maybe_batch orelse break;
        job.local.scan_batches += 1;
        if (job.raw_group_mode != .off) {
            try appendBatchRawChunks(job.local, job.shared, job.kind, batch, job.raw_chunk_rows, job.profile, job.filter_fused);
        } else {
            try appendBatchSiloLocal(job.local, job.shared, job.kind, batch, job.profile, job.filter_fused);
        }
    }

    if (job.raw_group_mode != .off and job.shared.raw_scan_queues.len > 0) {
        const publish_t0 = if (job.profile) nowTicks() else 0;
        const qidx = chooseRawScanPublishLane(job.shared, job.local.raw_scan_lane, @max(@as(usize, 1), job.raw_chunk_rows / 2));
        try publishRawRowsToQueue(job.shared, &job.shared.raw_scan_queues[qidx], job.worker_index, &job.local.raw_active_rows, job.raw_chunk_rows, &job.shared.outstanding_chunks, &job.shared.outstanding_rows, &job.local.raw_scan_queue_lock_ticks, &job.local.raw_recycle_lock_ticks);
        if (job.profile) job.local.publish_ticks += nowTicks() - publish_t0;
    }

    if (tile.hi == job.shared.total_scan_rgs) {
        try markGridScanDone(job, marked_scan_done);
    } else {
        try publishFullLocalBuckets(job.shared, job.local, job.chunk_rows, job.profile);
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
    } else if (job.shared.direct_final_local and job.local.flat_scan_partitions) {
        const publish_t0 = if (job.profile) nowTicks() else 0;
        try partitionFlatWorkerParts(job.local, job.shared.bucket_count);
        if (job.profile) job.local.publish_ticks += nowTicks() - publish_t0;
    } else if (!job.shared.direct_final_local) {
        const publish_t0 = if (job.profile) nowTicks() else 0;
        try flushPipePartsTracked(job.shared, job.local);
        if (job.profile) job.local.publish_ticks += nowTicks() - publish_t0;
    }
    _ = job.shared.scans_done.fetchAdd(1, .release);
    marked_scan_done.* = true;
}

fn siloGridWorkerErr(job: SiloGridJob) !void {
    var scratch: GroupScratch = .{};
    defer scratch.deinit(job.shared.allocator);

    var cursor = (job.worker_index * 17) % job.shared.bucket_count;
    var marked_scan_done = false;
    var scan_exhausted = false;
    var idle_spins: usize = 0;

    while (true) {
        job.local.sched_loops += 1;
        const decision_t0 = if (job.profile) nowTicks() else 0;
        const group_queued_rows = job.shared.outstanding_rows.load(.acquire);
        const scan_buffered_rows = job.shared.scan_buffered_rows.load(.acquire);
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
        const group_first = group_queued_rows > scan_buffered_rows or !scan_claims_available;
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
                    const did_stage = if (job.shared.stage_builders.len > 0)
                        try drainRawDedicatedStageSharedBuilders(
                            job.shared.allocator,
                            job.shared,
                            job.local,
                            choice.lane,
                            job.raw_chunk_rows,
                            job.raw_group_chunk_rows,
                            job.raw_batch_chunks,
                            job.profile,
                        )
                    else
                        try drainRawDedicatedStageFinalBuckets(
                            job.shared.allocator,
                            job.shared,
                            job.local,
                            job.worker_index,
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
            const has_stage_partials = if (job.shared.stage_builders.len > 0)
                job.shared.stage_builder_rows.load(.acquire) > 0
            else
                job.local.raw_stage_buffered_rows > 0;
            if (no_more_stage_input and has_stage_partials) {
                const flushed = if (job.shared.stage_builders.len > 0)
                    try flushSharedStageBuilders(job.shared, job.local, job.raw_group_chunk_rows, job.profile)
                else
                    try flushRawStageFinalBuckets(job.shared, job.local, job.raw_group_chunk_rows, job.profile);
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

        if (group_first and group_queued_rows > 0) {
            const did_group_work = try drainLeasedSilos(
                    job.shared.allocator,
                    job.shared,
                    &scratch,
                    &cursor,
                    job.group_ticks,
                    job.chunks,
                    job.group_lease_buckets,
                    job.group_lease_rows,
                    &job.local.sched_group_pick_ticks,
                    &job.local.sched_group_lock_ticks,
                    job.profile,
                );
            if (did_group_work) {
                job.local.sched_group_jobs += 1;
                idle_spins = 0;
                continue;
            }
            job.local.sched_group_misses += 1;
        }

        if (scan_claims_available and (!group_first or group_queued_rows == 0)) {
            try runGridScanBurst(job, &scan_exhausted, &marked_scan_done);
            idle_spins = 0;
            continue;
        }

        if (!group_first and group_queued_rows > 0 and !scan_claims_available) {
            const did_group_work = try drainLeasedSilos(
                    job.shared.allocator,
                    job.shared,
                    &scratch,
                    &cursor,
                    job.group_ticks,
                    job.chunks,
                    job.group_lease_buckets,
                    job.group_lease_rows,
                    &job.local.sched_group_pick_ticks,
                    &job.local.sched_group_lock_ticks,
                    job.profile,
                );
            if (did_group_work) {
                job.local.sched_group_jobs += 1;
                idle_spins = 0;
                continue;
            }
            job.local.sched_group_misses += 1;
        }

        if (job.shared.scans_done.load(.acquire) == job.shared.scan_threads and
            job.shared.outstanding_chunks.load(.acquire) == 0)
        {
            if (!job.shared.direct_final_local) break;
            if (try drainNextFinalLocalBucket(
                job.shared.allocator,
                job.shared,
                &scratch,
                job.group_ticks,
                job.chunks,
                job.profile,
            )) {
                job.local.sched_group_jobs += 1;
                idle_spins = 0;
                continue;
            }
            if (job.shared.next_final_local_bucket.load(.acquire) >= job.shared.bucket_count and
                job.shared.active_group_jobs.load(.acquire) == 0)
            {
                break;
            }
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
    }

    if (!marked_scan_done) {
        try markGridScanDone(job, &marked_scan_done);
    }
    collectOwnedTop(job.shared, job.worker_index, job.worker_count, job.top, job.top_ticks, job.profile);
}

pub const RunConfig = struct {
    dop: usize,
    bucket_count: usize,
    kind: QueryKind,
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
    filter_expr: ?thindb.exec.PredicateExpr = null,
    shared_stage_builders: bool = false,
    no_profile: bool = false,
    quiet: bool = false,
    result_out: ?*std.ArrayListUnmanaged(TopRow) = null,
    result_all_groups: bool = false,
    trace_timing: bool = false,
    workspace: ?*SiloGridWorkspace = null,
};

fn autoBucketCount(kind: QueryKind, dop: usize, total_rows: u64) usize {
    _ = kind;
    _ = dop;
    _ = total_rows;
    return 2028;
}

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

fn estimateGroupCountFromStats(stats: thindb.exec.PipelineStats, key_cols: usize) ?u64 {
    if (key_cols == 0 or stats.column_stats.len < key_cols) return null;
    var est: u64 = 1;
    var i: usize = 0;
    while (i < key_cols) : (i += 1) {
        switch (stats.column_stats[i].ndv) {
            .exact => |ndv| est *|= @max(@as(u64, 1), ndv),
            .unknown => return null,
        }
    }
    return @min(est, @max(stats.upper_rows, 1));
}

fn expectedGroupsPerBucket(total_rows: u64, bucket_count: usize, stats: thindb.exec.PipelineStats, kind: QueryKind, generic_key_count: usize, generic_key_width: GroupKeyWidth, generic_has_filter: bool) usize {
    const conservative_total = @max(@as(u64, 16), total_rows / 4);
    const key_count = if (generic_key_count != 0) generic_key_count else kind.groupKeyColumnCount();
    const estimated_total = estimateGroupCountFromStats(stats, key_count) orelse conservative_total;
    const has_filter = if (generic_key_count != 0) generic_has_filter else kind.hasFilter();
    const generic_wide_no_filter = generic_key_count != 0 and !has_filter and (generic_key_width == .u96 or generic_key_width == .u128);
    const no_filter_near_unique = !has_filter and estimated_total * 4 >= total_rows * 3;
    const total_groups = if (generic_wide_no_filter) total_rows else if (no_filter_near_unique) estimated_total else conservative_total;
    const per_bucket = (total_groups + @as(u64, @intCast(bucket_count)) - 1) / @as(u64, @intCast(bucket_count));
    return @intCast(@max(@as(u64, 16), per_bucket));
}

fn runSerialDirect(allocator: Allocator, table_ref: *thindb.api.Table, cfg: RunConfig) !void {
    const freq = perfFreq();
    const total = totalRows(table_ref);
    if (!cfg.quiet) std.debug.print("[clientip] query={s} DOP=1 direct rows={d}\n", .{ cfg.kind.label(), total });

    var q = try Scan.allocWithProjectionLoc(table_ref.allocator, table_ref, null, cfg.kind.columns(), false, null);
    defer q.deinit();
    if (cfg.scan_filter) _ = try applyScanFilter(q, cfg.kind);

    var groups = try GroupTable.init(allocator, @intCast(@max(@as(u64, 16), total / 8)));
    defer groups.deinit(allocator);

    var states: std.ArrayListUnmanaged(State) = .empty;
    defer states.deinit(allocator);
    try states.ensureTotalCapacity(allocator, @intCast(@max(@as(u64, 16), total / 10)));
    var scratch: GroupScratch = .{};
    defer scratch.deinit(allocator);

    const total_t0 = nowTicks();
    var scan_ticks: i64 = 0;
    var group_ticks: i64 = 0;
    var scanned_rows: u64 = 0;
    var rows: u64 = 0;
    while (true) {
        const scan_t0 = nowTicks();
        const maybe_batch = try q.next();
        scan_ticks += nowTicks() - scan_t0;
        const batch = maybe_batch orelse break;
        scanned_rows += batch.row_count;
        const group_t0 = nowTicks();
        try groupBatchDirect(&groups, &states, &scratch, allocator, cfg.kind, batch);
        rows += scratch.gids.items.len;
        group_ticks += nowTicks() - group_t0;
    }

    const top_t0 = nowTicks();
    var top: TopSet = .{};
    for (states.items) |s| top.consider(topRowFromState(s));
    std.mem.sort(TopRow, top.items[0..top.len], {}, topLess);
    const top_ticks = nowTicks() - top_t0;
    const total_ticks = nowTicks() - total_t0;

    if (!cfg.quiet) {
        std.debug.print(
            "[clientip-prof] query={s} DOP=1 scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} scan_partition_wall={d:.1}ms group_wall={d:.1}ms topn={d:.1}ms total={d:.1}ms mode=direct\n",
            .{
                cfg.kind.label(),
                scanned_rows,
                total,
                rows,
                rows,
                states.items.len,
                ticksToMs(scan_ticks, freq),
                ticksToMs(group_ticks, freq),
                ticksToMs(top_ticks, freq),
                ticksToMs(total_ticks, freq),
            },
        );
        std.debug.print("[clientip-prof-detail] query={s} scan_cpu={d:.1}ms partition_cpu=0.0ms\n", .{ cfg.kind.label(), ticksToMs(scan_ticks, freq) });
        for (top.items[0..top.len], 0..) |r, rank| {
            const avg = @as(f64, @floatFromInt(r.width_sum)) / @as(f64, @floatFromInt(r.count));
            std.debug.print(
                "[clientip-top] #{d} ClientIP={d} c={d} sum_refresh={d} avg_width={d:.6}\n",
                .{ rank + 1, r.key, r.count, r.refresh_sum, avg },
            );
        }
    }
}

fn runStream(allocator: Allocator, table: *thindb.api.Table, cpus: []const usize, cfg: RunConfig) !void {
    const freq = perfFreq();
    const total = totalRows(table);
    const dop = @max(@as(usize, 1), cfg.dop);
    const bucket_count = cfg.bucket_count;
    const local_reserve_per_bucket: usize = 512;
    const expected_groups_per_bucket: usize = @intCast(@max(@as(u64, 16), total / @as(u64, @intCast(bucket_count * 10))));

    std.debug.print(
        "[clientip] query={s} DOP={d} buckets={d} rows={d} mode=stream local_reserve/bucket={d}\n",
        .{ cfg.kind.label(), dop, bucket_count, total, local_reserve_per_bucket },
    );

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

    const n_threads = @max(@as(usize, 1), @min(dop, @max(total_rgs, 1)));
    var scans = try allocator.alloc(*Scan, n_threads);
    defer allocator.free(scans);
    var built_scans: usize = 0;
    defer {
        for (scans[0..built_scans]) |s| s.deinit();
    }

    var locals = try allocator.alloc(WorkerParts, n_threads);
    defer allocator.free(locals);
    var built_locals: usize = 0;
    defer {
        for (locals[0..built_locals]) |*p| p.deinit(allocator);
    }

    var central = try allocator.alloc(CentralBucket, bucket_count);
    defer allocator.free(central);
    var built_central: usize = 0;
    defer {
        for (central[0..built_central]) |*b| b.deinit(allocator);
    }

    var b: usize = 0;
    while (b < bucket_count) : (b += 1) {
        central[b] = try CentralBucket.init(allocator, expected_groups_per_bucket);
        built_central += 1;
        try central[b].states.ensureTotalCapacity(allocator, expected_groups_per_bucket);
    }

    var i: usize = 0;
    while (i < n_threads) : (i += 1) {
        locals[i] = try WorkerParts.init(allocator, bucket_count, local_reserve_per_bucket);
        built_locals += 1;
        const lo = i * total_rgs / n_threads;
        const hi = if (i == n_threads - 1) total_rgs else (i + 1) * total_rgs / n_threads;
        const start = flatToCoord(lo, seg_start, snap.segment_count, total_rgs);
        const end = flatToCoord(hi, seg_start, snap.segment_count, total_rgs);
        scans[i] = try Scan.allocWithProjectionLoc(table.allocator, table, null, cfg.kind.columns(), false, snap);
        if (cfg.scan_filter) _ = try applyScanFilter(scans[i], cfg.kind);
        built_scans += 1;
        scans[i].setRange(start.seg, start.rg, end.seg, end.rg, i == n_threads - 1);
    }
    snap.memtable_snap.release();
    pin_held = false;

    const total_t0 = nowTicks();
    var threads = try allocator.alloc(std.Thread, n_threads);
    defer allocator.free(threads);
    var errs = try allocator.alloc(?anyerror, n_threads);
    defer allocator.free(errs);
    @memset(errs, null);

    i = 0;
    while (i < n_threads) : (i += 1) {
        threads[i] = try std.Thread.spawn(.{}, streamWorker, .{StreamJob{
            .scan = scans[i],
            .local = &locals[i],
            .central = central,
            .allocator = allocator,
            .bucket_count = bucket_count,
            .kind = cfg.kind,
            .cpu = cpus[i % cpus.len],
            .err = &errs[i],
        }});
    }
    i = 0;
    while (i < n_threads) : (i += 1) threads[i].join();
    const worker_wall_ticks = nowTicks() - total_t0;
    for (errs) |maybe_err| if (maybe_err) |e| return e;

    var scan_cpu_ticks: i64 = 0;
    var partition_cpu_ticks: i64 = 0;
    var group_cpu_ticks: i64 = 0;
    var scanned_rows: u64 = 0;
    var filtered_rows: u64 = 0;
    for (locals) |p| {
        scan_cpu_ticks += p.scan_ticks;
        partition_cpu_ticks += p.partition_ticks;
        group_cpu_ticks += p.group_ticks;
        scanned_rows += p.scanned_count;
        filtered_rows += p.row_count;
    }

    const top_t0 = nowTicks();
    var top: TopSet = .{};
    var group_count: u64 = 0;
    for (central) |*cb| {
        group_count += cb.states.items.len;
        var local_top: TopSet = .{};
        for (cb.states.items) |s| local_top.consider(topRowFromState(s));
        for (local_top.items[0..local_top.len]) |candidate| top.consider(candidate);
    }
    std.mem.sort(TopRow, top.items[0..top.len], {}, topLess);
    const top_ticks = nowTicks() - top_t0;
    const total_ticks = nowTicks() - total_t0;

    std.debug.print(
        "[clientip-prof] query={s} DOP={d} scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} scan_partition_wall={d:.1}ms group_wall=overlapped topn={d:.1}ms total={d:.1}ms mode=stream\n",
        .{
            cfg.kind.label(),
            n_threads,
            scanned_rows,
            total,
            filtered_rows,
            filtered_rows,
            group_count,
            ticksToMs(worker_wall_ticks, freq),
            ticksToMs(top_ticks, freq),
            ticksToMs(total_ticks, freq),
        },
    );
    std.debug.print(
        "[clientip-prof-detail] query={s} scan_cpu={d:.1}ms partition_cpu={d:.1}ms group_cpu={d:.1}ms\n",
        .{ cfg.kind.label(), ticksToMs(scan_cpu_ticks, freq), ticksToMs(partition_cpu_ticks, freq), ticksToMs(group_cpu_ticks, freq) },
    );
    for (top.items[0..top.len], 0..) |r, rank| {
        const avg = @as(f64, @floatFromInt(r.width_sum)) / @as(f64, @floatFromInt(r.count));
        std.debug.print(
            "[clientip-top] #{d} ClientIP={d} c={d} sum_refresh={d} avg_width={d:.6}\n",
            .{ rank + 1, r.key, r.count, r.refresh_sum, avg },
        );
    }
}

fn choosePipeScanThreads(kind: QueryKind, dop: usize, override_scan_threads: usize) usize {
    if (dop <= 1) return 1;
    if (override_scan_threads > 0) return @max(@as(usize, 1), @min(override_scan_threads, dop - 1));
    return switch (kind) {
        .q30, .q31 => @max(@as(usize, 1), @min(dop - 1, (dop * 2 + 2) / 3)),
        .q32, .clientip => @max(@as(usize, 1), @min(dop - 1, dop / 4)),
    };
}

fn chooseSiloScanThreads(kind: QueryKind, dop: usize, override_scan_threads: usize) usize {
    if (dop <= 1) return 1;
    if (override_scan_threads > 0) return @max(@as(usize, 1), @min(override_scan_threads, dop - 1));
    return switch (kind) {
        .q30, .q31 => @max(@as(usize, 1), @min(dop - 1, (dop + 2) / 2)),
        .q32, .clientip => @max(@as(usize, 1), @min(dop - 1, dop / 4)),
    };
}

fn chooseElasticChunkRows(kind: QueryKind, cfg_chunk_rows: usize, chunk_rows_set: bool) usize {
    if (chunk_rows_set) return cfg_chunk_rows;
    return switch (kind) {
        .q32 => 65536,
        .clientip, .q30, .q31 => 16384,
    };
}

fn chooseGridChunkRows(cfg_chunk_rows: usize, chunk_rows_set: bool) usize {
    return if (chunk_rows_set) cfg_chunk_rows else GRID_CHUNK_ROWS;
}

fn chooseGridScanTileRgs(kind: QueryKind, cfg_scan_tile_rgs: usize, scan_tile_rgs_set: bool) usize {
    _ = kind;
    if (scan_tile_rgs_set) return cfg_scan_tile_rgs;
    return GRID_SCAN_TILE_RGS;
}

fn runPipeModeLabel(cfg: RunConfig) []const u8 {
    if (cfg.silo) return "silo";
    if (cfg.pipe_tail) return "pipe-tail";
    return "pipe";
}

fn runPipe(allocator: Allocator, table: *thindb.api.Table, cpus: []const usize, cfg: RunConfig) !void {
    const freq = perfFreq();
    const total = totalRows(table);
    const dop = @max(@as(usize, 1), cfg.dop);
    if (dop == 1) return runSerialDirect(allocator, table, cfg);

    const bucket_count = cfg.bucket_count;
    const dedicated_scan_threads = if (cfg.silo)
        chooseSiloScanThreads(cfg.kind, dop, cfg.pipe_scan_threads)
    else
        choosePipeScanThreads(cfg.kind, dop, cfg.pipe_scan_threads);
    const scan_threads = if (cfg.pipe_tail) dop else dedicated_scan_threads;
    const group_threads = if (cfg.pipe_tail) dop - dedicated_scan_threads else dop - scan_threads;
    const chunk_rows = cfg.chunk_rows;
    const local_reserve_per_bucket: usize = @min(chunk_rows, @as(usize, @intCast(@max(@as(u64, 16), total / @as(u64, @intCast(scan_threads * bucket_count * 8))))));
    const expected_groups_per_bucket: usize = @intCast(@max(@as(u64, 16), total / @as(u64, @intCast(bucket_count * 4))));

    if (!cfg.quiet) {
        std.debug.print(
            "[clientip] query={s} DOP={d} buckets={d} rows={d} mode={s} scan_threads={d} group_threads={d} chunk_rows={d}\n",
            .{ cfg.kind.label(), dop, bucket_count, total, runPipeModeLabel(cfg), scan_threads, group_threads, chunk_rows },
        );
        if (cfg.silo) {
            if (scan_threads + group_threads > cpus.len) {
                std.debug.print(
                    "[clientip-cpu-warning] mode={s} concurrent_threads={d} physical_cpu_slots={d}; physical cores must be reused in this mode\n",
                    .{ runPipeModeLabel(cfg), scan_threads + group_threads, cpus.len },
                );
            }
            std.debug.print("[clientip-cpu-assign] mode={s} scan=", .{runPipeModeLabel(cfg)});
            var ci: usize = 0;
            while (ci < scan_threads) : (ci += 1) {
                if (ci != 0) std.debug.print(",", .{});
                std.debug.print("{d}", .{cpus[ci % cpus.len]});
            }
            std.debug.print(" group=", .{});
            ci = 0;
            while (ci < group_threads) : (ci += 1) {
                if (ci != 0) std.debug.print(",", .{});
                std.debug.print("{d}", .{cpus[(scan_threads + ci) % cpus.len]});
            }
            std.debug.print("\n", .{});
            std.debug.print("[clientip-affinity] mode=silo scan_and_group_overlap=true scan_group_physical_cores_distinct=true same_core_handoff=false\n", .{});
        }
    }

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

    const n_scan_threads = @max(@as(usize, 1), @min(scan_threads, @max(total_rgs, 1)));
    const n_group_threads = @max(@as(usize, 1), group_threads);

    var scans = try allocator.alloc(*Scan, n_scan_threads);
    defer allocator.free(scans);
    var built_scans: usize = 0;
    defer {
        for (scans[0..built_scans]) |s| s.deinit();
    }

    var parts = try allocator.alloc(WorkerParts, n_scan_threads);
    defer allocator.free(parts);
    var built_parts: usize = 0;
    defer {
        for (parts[0..built_parts]) |*p| p.deinit(allocator);
    }

    var buckets = try allocator.alloc(PipeBucket, bucket_count);
    defer allocator.free(buckets);
    var built_buckets: usize = 0;
    defer {
        for (buckets[0..built_buckets]) |*b| b.deinit(allocator);
    }
    var b: usize = 0;
    while (b < bucket_count) : (b += 1) {
        buckets[b] = try PipeBucket.init(allocator, expected_groups_per_bucket);
        built_buckets += 1;
        try buckets[b].states.ensureTotalCapacity(allocator, expected_groups_per_bucket);
        try buckets[b].chunks.ensureTotalCapacity(allocator, 8);
    }

    var shared = PipeShared{
        .allocator = allocator,
        .buckets = buckets,
        .bucket_count = bucket_count,
        .scan_threads = n_scan_threads,
    };

    var i: usize = 0;
    while (i < n_scan_threads) : (i += 1) {
        parts[i] = try WorkerParts.init(allocator, bucket_count, local_reserve_per_bucket);
        built_parts += 1;
        const lo = i * total_rgs / n_scan_threads;
        const hi = if (i == n_scan_threads - 1) total_rgs else (i + 1) * total_rgs / n_scan_threads;
        const start = flatToCoord(lo, seg_start, snap.segment_count, total_rgs);
        const end = flatToCoord(hi, seg_start, snap.segment_count, total_rgs);
        scans[i] = try Scan.allocWithProjectionLoc(table.allocator, table, null, cfg.kind.columns(), false, snap);
        if (cfg.scan_filter) _ = try applyScanFilter(scans[i], cfg.kind);
        built_scans += 1;
        scans[i].setRange(start.seg, start.rg, end.seg, end.rg, i == n_scan_threads - 1);
    }
    snap.memtable_snap.release();
    pin_held = false;

    const total_t0 = nowTicks();

    var scan_os_threads = try allocator.alloc(std.Thread, n_scan_threads);
    defer allocator.free(scan_os_threads);
    var scan_errs = try allocator.alloc(?anyerror, n_scan_threads);
    defer allocator.free(scan_errs);
    @memset(scan_errs, null);

    var group_os_threads = try allocator.alloc(std.Thread, n_group_threads);
    defer allocator.free(group_os_threads);
    var group_errs = try allocator.alloc(?anyerror, n_group_threads);
    defer allocator.free(group_errs);
    @memset(group_errs, null);
    var group_ticks = try allocator.alloc(i64, n_group_threads);
    defer allocator.free(group_ticks);
    @memset(group_ticks, 0);
    var group_idle_ticks = try allocator.alloc(i64, n_group_threads);
    defer allocator.free(group_idle_ticks);
    @memset(group_idle_ticks, 0);
    var group_top_ticks = try allocator.alloc(i64, n_group_threads);
    defer allocator.free(group_top_ticks);
    @memset(group_top_ticks, 0);
    var group_chunks = try allocator.alloc(u64, n_group_threads);
    defer allocator.free(group_chunks);
    @memset(group_chunks, 0);
    var silo_tops: []TopSet = &.{};
    if (cfg.silo) {
        silo_tops = try allocator.alloc(TopSet, n_group_threads);
        for (silo_tops) |*t| t.* = .{};
    }
    defer if (silo_tops.len > 0) allocator.free(silo_tops);

    const workers_t0 = nowTicks();
    i = 0;
    while (i < n_scan_threads) : (i += 1) {
        scan_os_threads[i] = try std.Thread.spawn(.{}, pipeScanWorker, .{PipeScanJob{
            .scan = scans[i],
            .parts = &parts[i],
            .shared = &shared,
            .kind = cfg.kind,
            .silo = cfg.silo,
            .chunk_rows = chunk_rows,
            .profile = !cfg.no_profile,
            .cpu = cpus[i % cpus.len],
            .err = &scan_errs[i],
        }});
    }
    if (cfg.pipe_tail) {
        while (shared.scans_done.load(.acquire) < n_group_threads and shared.scans_done.load(.acquire) < n_scan_threads) {
            std.Thread.yield() catch {};
        }
    }
    const group_launch_t0 = nowTicks();
    i = 0;
    while (i < n_group_threads) : (i += 1) {
        if (cfg.silo) {
            group_os_threads[i] = try std.Thread.spawn(.{}, siloGroupWorker, .{SiloGroupJob{
                .allocator = allocator,
                .shared = &shared,
                .worker_index = i,
                .worker_count = n_group_threads,
                .cpu = cpus[(n_scan_threads + i) % cpus.len],
                .ticks = &group_ticks[i],
                .idle_ticks = &group_idle_ticks[i],
                .top_ticks = &group_top_ticks[i],
                .chunks = &group_chunks[i],
                .top = &silo_tops[i],
                .profile = !cfg.no_profile,
                .err = &group_errs[i],
            }});
        } else {
            group_os_threads[i] = try std.Thread.spawn(.{}, pipeGroupWorker, .{PipeGroupJob{
                .allocator = allocator,
                .shared = &shared,
                .worker_index = i,
                .worker_count = n_group_threads,
                .cpu = cpus[(n_scan_threads + i) % cpus.len],
                .ticks = &group_ticks[i],
                .err = &group_errs[i],
            }});
        }
    }

    i = 0;
    while (i < n_scan_threads) : (i += 1) scan_os_threads[i].join();
    const scan_done_wall_ticks = nowTicks() - workers_t0;
    i = 0;
    while (i < n_group_threads) : (i += 1) group_os_threads[i].join();
    const group_wall_ticks = nowTicks() - group_launch_t0;
    const worker_wall_ticks = nowTicks() - workers_t0;

    for (scan_errs) |maybe_err| if (maybe_err) |e| return e;
    for (group_errs) |maybe_err| if (maybe_err) |e| return e;

    var scan_cpu_ticks: i64 = 0;
    var partition_cpu_ticks: i64 = 0;
    var publish_cpu_ticks: i64 = 0;
    var group_cpu_ticks: i64 = 0;
    var group_idle_cpu_ticks: i64 = 0;
    var group_top_cpu_ticks: i64 = 0;
    var total_chunks: u64 = 0;
    var scanned_rows: u64 = 0;
    var filtered_rows: u64 = 0;
    for (parts) |p| {
        scan_cpu_ticks += p.scan_ticks;
        partition_cpu_ticks += p.partition_ticks;
        publish_cpu_ticks += p.publish_ticks;
        scanned_rows += p.scanned_count;
        filtered_rows += p.row_count;
    }
    for (group_ticks) |ticks| group_cpu_ticks += ticks;
    for (group_idle_ticks) |ticks| group_idle_cpu_ticks += ticks;
    for (group_top_ticks) |ticks| group_top_cpu_ticks += ticks;
    for (group_chunks) |chunks| total_chunks += chunks;

    const top_t0 = nowTicks();
    var top: TopSet = .{};
    var group_count: u64 = 0;
    var grouped_rows: u64 = 0;
    for (buckets) |*bucket| {
        group_count += bucket.states.items.len;
        grouped_rows += bucket.row_count;
        if (!cfg.silo) {
            var local_top: TopSet = .{};
            for (bucket.states.items) |s| local_top.consider(topRowFromState(s));
            for (local_top.items[0..local_top.len]) |candidate| top.consider(candidate);
        }
    }
    if (cfg.silo) {
        for (silo_tops) |worker_top| {
            for (worker_top.items[0..worker_top.len]) |candidate| top.consider(candidate);
        }
    }
    std.mem.sort(TopRow, top.items[0..top.len], {}, topLess);
    const top_ticks = nowTicks() - top_t0;
    const total_ticks = nowTicks() - total_t0;

    if (!cfg.quiet and cfg.no_profile) {
        std.debug.print(
            "[clientip-result] query={s} DOP={d} mode={s} scan_threads={d} group_threads={d} chunk_rows={d} scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} total={d:.1}ms no_profile=true\n",
            .{
                cfg.kind.label(),
                dop,
                runPipeModeLabel(cfg),
                n_scan_threads,
                n_group_threads,
                chunk_rows,
                scanned_rows,
                total,
                filtered_rows,
                grouped_rows,
                group_count,
                ticksToMs(total_ticks, freq),
            },
        );
    } else if (!cfg.quiet) {
        std.debug.print(
            "[clientip-prof] query={s} DOP={d} scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} scan_partition_wall=overlapped group_wall=overlapped topn={d:.1}ms total={d:.1}ms mode={s} scan_threads={d} group_threads={d} worker_wall={d:.1}ms\n",
            .{
                cfg.kind.label(),
                dop,
                scanned_rows,
                total,
                filtered_rows,
                grouped_rows,
                group_count,
                ticksToMs(top_ticks, freq),
                ticksToMs(total_ticks, freq),
                runPipeModeLabel(cfg),
                n_scan_threads,
                n_group_threads,
                ticksToMs(worker_wall_ticks, freq),
            },
        );
        std.debug.print(
            "[clientip-prof-detail] query={s} scan_cpu={d:.1}ms partition_cpu={d:.1}ms group_cpu={d:.1}ms outstanding_chunks={d}\n",
            .{
                cfg.kind.label(),
                ticksToMs(scan_cpu_ticks, freq),
                ticksToMs(partition_cpu_ticks, freq),
                ticksToMs(group_cpu_ticks, freq),
                shared.outstanding_chunks.load(.acquire),
            },
        );
        if (cfg.silo) {
            std.debug.print(
                "[clientip-silo-stages] query={s} scan_stage_wall={d:.1}ms group_stage_wall={d:.1}ms scan_decode_cpu={d:.1}ms route_partition_cpu={d:.1}ms publish_queue_cpu={d:.1}ms aggregate_cpu={d:.1}ms aggregate_idle_cpu={d:.1}ms local_topn_cpu={d:.1}ms final_merge_wall={d:.3}ms chunks={d} rows_per_chunk={d:.1}\n",
                .{
                    cfg.kind.label(),
                    ticksToMs(scan_done_wall_ticks, freq),
                    ticksToMs(group_wall_ticks, freq),
                    ticksToMs(scan_cpu_ticks, freq),
                    ticksToMs(partition_cpu_ticks, freq),
                    ticksToMs(publish_cpu_ticks, freq),
                    ticksToMs(group_cpu_ticks, freq),
                    ticksToMs(group_idle_cpu_ticks, freq),
                    ticksToMs(group_top_cpu_ticks, freq),
                    ticksToMs(top_ticks, freq),
                    total_chunks,
                    if (total_chunks == 0) 0.0 else @as(f64, @floatFromInt(grouped_rows)) / @as(f64, @floatFromInt(total_chunks)),
                },
            );
            std.debug.print(
                "[clientip-silo-workers] query={s} aggregate_cpu_by_worker_ms=",
                .{cfg.kind.label()},
            );
            for (group_ticks, 0..) |ticks, worker_idx| {
                if (worker_idx != 0) std.debug.print(",", .{});
                std.debug.print("{d:.1}", .{ticksToMs(ticks, freq)});
            }
            std.debug.print(" idle_cpu_by_worker_ms=", .{});
            for (group_idle_ticks, 0..) |ticks, worker_idx| {
                if (worker_idx != 0) std.debug.print(",", .{});
                std.debug.print("{d:.1}", .{ticksToMs(ticks, freq)});
            }
            std.debug.print(" chunks_by_worker=", .{});
            for (group_chunks, 0..) |chunks, worker_idx| {
                if (worker_idx != 0) std.debug.print(",", .{});
                std.debug.print("{d}", .{chunks});
            }
            std.debug.print("\n", .{});
        }
        for (top.items[0..top.len], 0..) |r, rank| {
            const avg = @as(f64, @floatFromInt(r.width_sum)) / @as(f64, @floatFromInt(r.count));
            std.debug.print(
                "[clientip-top] #{d} ClientIP={d} c={d} sum_refresh={d} avg_width={d:.6}\n",
                .{ rank + 1, r.key, r.count, r.refresh_sum, avg },
            );
        }
    }
}

fn runSiloElevator(allocator: Allocator, table: *thindb.api.Table, cpus: []const usize, cfg: RunConfig) !void {
    const freq = perfFreq();
    const total = totalRows(table);
    const dop = @max(@as(usize, 1), cfg.dop);
    if (dop == 1) return runSerialDirect(allocator, table, cfg);

    const bucket_count = cfg.bucket_count;
    const expected_groups_per_bucket: usize = @intCast(@max(@as(u64, 16), total / @as(u64, @intCast(bucket_count * 4))));

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

    const n_threads = @max(@as(usize, 1), @min(dop, @max(total_rgs, 1)));
    const chunk_rows = cfg.chunk_rows;
    const local_reserve_per_bucket: usize = @min(chunk_rows, @as(usize, @intCast(@max(@as(u64, 16), total / @as(u64, @intCast(n_threads * bucket_count * 8))))));

    if (!cfg.quiet) {
        std.debug.print(
            "[clientip] query={s} DOP={d} buckets={d} rows={d} mode=silo-elevator workers={d} chunk_rows={d}\n",
            .{ cfg.kind.label(), dop, bucket_count, total, n_threads, chunk_rows },
        );
        std.debug.print("[clientip-cpu-assign] mode=silo-elevator workers=", .{});
        var ci: usize = 0;
        while (ci < n_threads) : (ci += 1) {
            if (ci != 0) std.debug.print(",", .{});
            std.debug.print("{d}", .{cpus[ci % cpus.len]});
        }
        std.debug.print("\n", .{});
        std.debug.print("[clientip-affinity] mode=silo-elevator same_thread_scans_and_drains_owned_silos=true physical_cores_distinct={s}\n", .{if (n_threads <= cpus.len) "true" else "false"});
    }

    var scans = try allocator.alloc(*Scan, n_threads);
    defer allocator.free(scans);
    var built_scans: usize = 0;
    defer {
        for (scans[0..built_scans]) |s| s.deinit();
    }

    var parts = try allocator.alloc(WorkerParts, n_threads);
    defer allocator.free(parts);
    var built_parts: usize = 0;
    defer {
        for (parts[0..built_parts]) |*p| p.deinit(allocator);
    }

    var buckets = try allocator.alloc(PipeBucket, bucket_count);
    defer allocator.free(buckets);
    var built_buckets: usize = 0;
    defer {
        for (buckets[0..built_buckets]) |*b| b.deinit(allocator);
    }
    var b: usize = 0;
    while (b < bucket_count) : (b += 1) {
        buckets[b] = try PipeBucket.init(allocator, expected_groups_per_bucket);
        built_buckets += 1;
        try buckets[b].states.ensureTotalCapacity(allocator, expected_groups_per_bucket);
        try buckets[b].chunks.ensureTotalCapacity(allocator, 8);
    }

    var shared = PipeShared{
        .allocator = allocator,
        .buckets = buckets,
        .bucket_count = bucket_count,
        .scan_threads = n_threads,
    };

    var i: usize = 0;
    while (i < n_threads) : (i += 1) {
        parts[i] = try WorkerParts.init(allocator, bucket_count, local_reserve_per_bucket);
        built_parts += 1;
        const lo = i * total_rgs / n_threads;
        const hi = if (i == n_threads - 1) total_rgs else (i + 1) * total_rgs / n_threads;
        const start = flatToCoord(lo, seg_start, snap.segment_count, total_rgs);
        const end = flatToCoord(hi, seg_start, snap.segment_count, total_rgs);
        scans[i] = try Scan.allocWithProjectionLoc(table.allocator, table, null, cfg.kind.columns(), false, snap);
        if (cfg.scan_filter) _ = try applyScanFilter(scans[i], cfg.kind);
        built_scans += 1;
        scans[i].setRange(start.seg, start.rg, end.seg, end.rg, i == n_threads - 1);
    }
    snap.memtable_snap.release();
    pin_held = false;

    var group_ticks = try allocator.alloc(i64, n_threads);
    defer allocator.free(group_ticks);
    @memset(group_ticks, 0);
    var idle_ticks = try allocator.alloc(i64, n_threads);
    defer allocator.free(idle_ticks);
    @memset(idle_ticks, 0);
    var top_ticks = try allocator.alloc(i64, n_threads);
    defer allocator.free(top_ticks);
    @memset(top_ticks, 0);
    var chunks = try allocator.alloc(u64, n_threads);
    defer allocator.free(chunks);
    @memset(chunks, 0);
    var worker_tops = try allocator.alloc(TopSet, n_threads);
    defer allocator.free(worker_tops);
    for (worker_tops) |*t| t.* = .{};

    const total_t0 = nowTicks();
    var threads = try allocator.alloc(std.Thread, n_threads);
    defer allocator.free(threads);
    var errs = try allocator.alloc(?anyerror, n_threads);
    defer allocator.free(errs);
    @memset(errs, null);

    i = 0;
    while (i < n_threads) : (i += 1) {
        threads[i] = try std.Thread.spawn(.{}, siloElevatorWorker, .{SiloElevatorJob{
            .scan = scans[i],
            .local = &parts[i],
            .shared = &shared,
            .worker_index = i,
            .worker_count = n_threads,
            .kind = cfg.kind,
            .chunk_rows = chunk_rows,
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
    while (i < n_threads) : (i += 1) threads[i].join();
    const worker_wall_ticks = nowTicks() - total_t0;
    for (errs) |maybe_err| if (maybe_err) |e| return e;

    var scan_cpu_ticks: i64 = 0;
    var partition_cpu_ticks: i64 = 0;
    var publish_cpu_ticks: i64 = 0;
    var group_cpu_ticks: i64 = 0;
    var idle_cpu_ticks: i64 = 0;
    var local_top_cpu_ticks: i64 = 0;
    var scanned_rows: u64 = 0;
    var filtered_rows: u64 = 0;
    var total_chunks: u64 = 0;
    for (parts) |p| {
        scan_cpu_ticks += p.scan_ticks;
        partition_cpu_ticks += p.partition_ticks;
        publish_cpu_ticks += p.publish_ticks;
        scanned_rows += p.scanned_count;
        filtered_rows += p.row_count;
    }
    for (group_ticks) |ticks| group_cpu_ticks += ticks;
    for (idle_ticks) |ticks| idle_cpu_ticks += ticks;
    for (top_ticks) |ticks| local_top_cpu_ticks += ticks;
    for (chunks) |chunk_count| total_chunks += chunk_count;

    const final_t0 = nowTicks();
    var top: TopSet = .{};
    var group_count: u64 = 0;
    var grouped_rows: u64 = 0;
    for (buckets) |*bucket| {
        group_count += bucket.states.items.len;
        grouped_rows += bucket.row_count;
    }
    for (worker_tops) |worker_top| {
        for (worker_top.items[0..worker_top.len]) |candidate| top.consider(candidate);
    }
    std.mem.sort(TopRow, top.items[0..top.len], {}, topLess);
    const final_merge_ticks = nowTicks() - final_t0;
    const total_ticks = nowTicks() - total_t0;
    if (cfg.result_out) |out| {
        if (cfg.result_all_groups) {
            for (buckets) |*bucket| {
                for (bucket.states.items) |s| try out.append(allocator, topRowFromState(s));
            }
        } else {
            try out.appendSlice(allocator, top.items[0..top.len]);
        }
    }

    if (!cfg.quiet and cfg.no_profile) {
        std.debug.print(
            "[clientip-result] query={s} DOP={d} mode=silo-elevator chunk_rows={d} scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} total={d:.1}ms no_profile=true\n",
            .{
                cfg.kind.label(),
                n_threads,
                chunk_rows,
                scanned_rows,
                total,
                filtered_rows,
                grouped_rows,
                group_count,
                ticksToMs(total_ticks, freq),
            },
        );
    } else if (!cfg.quiet) {
        std.debug.print(
            "[clientip-prof] query={s} DOP={d} scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} total={d:.1}ms mode=silo-elevator worker_wall={d:.1}ms final_merge={d:.3}ms\n",
            .{
                cfg.kind.label(),
                n_threads,
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
        std.debug.print(
            "[clientip-silo-elevator-stages] query={s} scan_decode_cpu={d:.1}ms route_partition_cpu={d:.1}ms publish_queue_cpu={d:.1}ms aggregate_cpu={d:.1}ms drain_idle_cpu={d:.1}ms local_topn_cpu={d:.1}ms final_merge_wall={d:.3}ms chunks={d} rows_per_chunk={d:.1}\n",
            .{
                cfg.kind.label(),
                ticksToMs(scan_cpu_ticks, freq),
                ticksToMs(partition_cpu_ticks, freq),
                ticksToMs(publish_cpu_ticks, freq),
                ticksToMs(group_cpu_ticks, freq),
                ticksToMs(idle_cpu_ticks, freq),
                ticksToMs(local_top_cpu_ticks, freq),
                ticksToMs(final_merge_ticks, freq),
                total_chunks,
                if (total_chunks == 0) 0.0 else @as(f64, @floatFromInt(grouped_rows)) / @as(f64, @floatFromInt(total_chunks)),
            },
        );
        std.debug.print("[clientip-silo-elevator-workers] query={s} aggregate_cpu_by_worker_ms=", .{cfg.kind.label()});
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
        for (top.items[0..top.len], 0..) |r, rank| {
            const avg = @as(f64, @floatFromInt(r.width_sum)) / @as(f64, @floatFromInt(r.count));
            std.debug.print(
                "[clientip-top] #{d} ClientIP={d} c={d} sum_refresh={d} avg_width={d:.6}\n",
                .{ rank + 1, r.key, r.count, r.refresh_sum, avg },
            );
        }
    }
}

fn runLocalPreAgg(allocator: Allocator, table: *thindb.api.Table, cpus: []const usize, cfg: RunConfig) !void {
    const freq = perfFreq();
    const total = totalRows(table);
    const dop = @max(@as(usize, 1), cfg.dop);
    if (dop == 1) return runSerialDirect(allocator, table, cfg);

    const bucket_count = cfg.bucket_count;
    const expected_groups_per_worker: usize = LOCAL_PREAGG_FLUSH_GROUPS;

    std.debug.print(
        "[clientip] query={s} DOP={d} buckets={d} rows={d} mode=local-preagg flush_groups={d}\n",
        .{ cfg.kind.label(), dop, bucket_count, total, expected_groups_per_worker },
    );

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

    const n_threads = @max(@as(usize, 1), @min(dop, @max(total_rgs, 1)));
    var scans = try allocator.alloc(*Scan, n_threads);
    defer allocator.free(scans);
    var built_scans: usize = 0;
    defer {
        for (scans[0..built_scans]) |s| s.deinit();
    }

    var aggs = try allocator.alloc(WorkerAgg, n_threads);
    defer allocator.free(aggs);
    var built_aggs: usize = 0;
    defer {
        for (aggs[0..built_aggs]) |*a| a.deinit(allocator);
    }

    var i: usize = 0;
    while (i < n_threads) : (i += 1) {
        aggs[i] = try WorkerAgg.init(allocator, bucket_count, expected_groups_per_worker);
        built_aggs += 1;
        const lo = i * total_rgs / n_threads;
        const hi = if (i == n_threads - 1) total_rgs else (i + 1) * total_rgs / n_threads;
        const start = flatToCoord(lo, seg_start, snap.segment_count, total_rgs);
        const end = flatToCoord(hi, seg_start, snap.segment_count, total_rgs);
        scans[i] = try Scan.allocWithProjectionLoc(table.allocator, table, null, cfg.kind.columns(), false, snap);
        if (cfg.scan_filter) _ = try applyScanFilter(scans[i], cfg.kind);
        built_scans += 1;
        scans[i].setRange(start.seg, start.rg, end.seg, end.rg, i == n_threads - 1);
    }
    snap.memtable_snap.release();
    pin_held = false;

    const total_t0 = nowTicks();
    const sink_t0 = nowTicks();
    var scan_threads = try allocator.alloc(std.Thread, n_threads);
    defer allocator.free(scan_threads);
    var scan_errs = try allocator.alloc(?anyerror, n_threads);
    defer allocator.free(scan_errs);
    @memset(scan_errs, null);

    i = 0;
    while (i < n_threads) : (i += 1) {
        scan_threads[i] = try std.Thread.spawn(.{}, preAggScanWorker, .{PreAggScanJob{
            .scan = scans[i],
            .agg = &aggs[i],
            .allocator = allocator,
            .bucket_count = bucket_count,
            .table_expected = expected_groups_per_worker,
            .kind = cfg.kind,
            .cpu = cpus[i % cpus.len],
            .err = &scan_errs[i],
        }});
    }
    i = 0;
    while (i < n_threads) : (i += 1) scan_threads[i].join();
    const sink_wall_ticks = nowTicks() - sink_t0;
    for (scan_errs) |maybe_err| if (maybe_err) |e| return e;

    var scan_cpu_ticks: i64 = 0;
    var local_group_cpu_ticks: i64 = 0;
    var partition_cpu_ticks: i64 = 0;
    var scanned_rows: u64 = 0;
    var filtered_rows: u64 = 0;
    var local_groups: u64 = 0;
    for (aggs) |a| {
        scan_cpu_ticks += a.scan_ticks;
        local_group_cpu_ticks += a.group_ticks;
        partition_cpu_ticks += a.partition_ticks;
        scanned_rows += a.scanned_count;
        filtered_rows += a.row_count;
        local_groups += a.partitioned_count;
    }

    const results = try allocator.alloc(BucketResult, bucket_count);
    defer allocator.free(results);
    for (results) |*r| r.* = .{};
    defer {
        for (results) |*r| r.deinit(allocator);
    }

    const merge_t0 = nowTicks();
    var next_bucket = std.atomic.Value(usize).init(0);
    var merge_threads = try allocator.alloc(std.Thread, n_threads);
    defer allocator.free(merge_threads);
    var merge_errs = try allocator.alloc(?anyerror, n_threads);
    defer allocator.free(merge_errs);
    @memset(merge_errs, null);
    var merge_ticks = try allocator.alloc(i64, n_threads);
    defer allocator.free(merge_ticks);
    @memset(merge_ticks, 0);

    i = 0;
    while (i < n_threads) : (i += 1) {
        merge_threads[i] = try std.Thread.spawn(.{}, preAggMergeWorker, .{PreAggMergeJob{
            .allocator = allocator,
            .aggs = aggs,
            .results = results,
            .next_bucket = &next_bucket,
            .bucket_count = bucket_count,
            .cpu = cpus[i % cpus.len],
            .ticks = &merge_ticks[i],
            .err = &merge_errs[i],
        }});
    }
    i = 0;
    while (i < n_threads) : (i += 1) merge_threads[i].join();
    const merge_wall_ticks = nowTicks() - merge_t0;
    for (merge_errs) |maybe_err| if (maybe_err) |e| return e;
    var merge_cpu_ticks: i64 = 0;
    for (merge_ticks) |ticks| merge_cpu_ticks += ticks;

    const top_t0 = nowTicks();
    var top: TopSet = .{};
    var group_count: u64 = 0;
    var merged_states: u64 = 0;
    for (results) |r| {
        group_count += r.group_count;
        merged_states += r.row_count;
        for (r.top.items[0..r.top.len]) |candidate| top.consider(candidate);
    }
    std.mem.sort(TopRow, top.items[0..top.len], {}, topLess);
    const top_ticks = nowTicks() - top_t0;
    const total_ticks = nowTicks() - total_t0;

    std.debug.print(
        "[clientip-prof] query={s} DOP={d} scanned={d}/{d} filtered={d} local_groups={d} merged_states={d} groups={d} sink_wall={d:.1}ms merge_wall={d:.1}ms topn={d:.1}ms total={d:.1}ms mode=local-preagg\n",
        .{
            cfg.kind.label(),
            n_threads,
            scanned_rows,
            total,
            filtered_rows,
            local_groups,
            merged_states,
            group_count,
            ticksToMs(sink_wall_ticks, freq),
            ticksToMs(merge_wall_ticks, freq),
            ticksToMs(top_ticks, freq),
            ticksToMs(total_ticks, freq),
        },
    );
    std.debug.print(
        "[clientip-prof-detail] query={s} scan_cpu={d:.1}ms local_group_cpu={d:.1}ms partition_cpu={d:.1}ms merge_cpu={d:.1}ms\n",
        .{
            cfg.kind.label(),
            ticksToMs(scan_cpu_ticks, freq),
            ticksToMs(local_group_cpu_ticks, freq),
            ticksToMs(partition_cpu_ticks, freq),
            ticksToMs(merge_cpu_ticks, freq),
        },
    );
    for (top.items[0..top.len], 0..) |r, rank| {
        const avg = @as(f64, @floatFromInt(r.width_sum)) / @as(f64, @floatFromInt(r.count));
        std.debug.print(
            "[clientip-top] #{d} ClientIP={d} c={d} sum_refresh={d} avg_width={d:.6}\n",
            .{ rank + 1, r.key, r.count, r.refresh_sum, avg },
        );
    }
}

fn runLateFilter(allocator: Allocator, table: *thindb.api.Table, cpus: []const usize, cfg: RunConfig) !void {
    if (cfg.kind != .q30 and cfg.kind != .q31) return runOnce(allocator, table, cpus, .{
        .dop = cfg.dop,
        .bucket_count = cfg.bucket_count,
        .kind = cfg.kind,
        .q30_inline = cfg.q30_inline,
    });

    const freq = perfFreq();
    const total = totalRows(table);
    const dop = @max(@as(usize, 1), cfg.dop);
    const bucket_count = cfg.bucket_count;
    const reserve_per_worker_bucket: usize = @intCast(@max(@as(u64, 1), total / @as(u64, @intCast(dop * bucket_count))));

    std.debug.print(
        "[clientip] query={s} DOP={d} buckets={d} rows={d} mode=late-filter reserve/worker/bucket={d}\n",
        .{ cfg.kind.label(), dop, bucket_count, total, reserve_per_worker_bucket },
    );

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

    const n_threads = @max(@as(usize, 1), @min(dop, @max(total_rgs, 1)));
    var filter_scans = try allocator.alloc(*Scan, n_threads);
    defer allocator.free(filter_scans);
    var payload_scans = try allocator.alloc(*Scan, n_threads);
    defer allocator.free(payload_scans);
    var built_filter_scans: usize = 0;
    var built_payload_scans: usize = 0;
    defer {
        for (filter_scans[0..built_filter_scans]) |s| s.deinit();
        for (payload_scans[0..built_payload_scans]) |s| s.deinit();
    }

    var parts = try allocator.alloc(WorkerParts, n_threads);
    defer allocator.free(parts);
    var built_parts: usize = 0;
    defer {
        for (parts[0..built_parts]) |*p| p.deinit(allocator);
    }

    const payload_cols = if (cfg.kind == .q30) &COLS_Q30_PAYLOAD else &COLS_Q31_PAYLOAD;
    var i: usize = 0;
    while (i < n_threads) : (i += 1) {
        parts[i] = try WorkerParts.init(allocator, bucket_count, reserve_per_worker_bucket);
        built_parts += 1;
        const lo = i * total_rgs / n_threads;
        const hi = if (i == n_threads - 1) total_rgs else (i + 1) * total_rgs / n_threads;
        const start = flatToCoord(lo, seg_start, snap.segment_count, total_rgs);
        const end = flatToCoord(hi, seg_start, snap.segment_count, total_rgs);
        filter_scans[i] = try Scan.allocWithProjectionLoc(table.allocator, table, null, &COLS_PHRASE, false, snap);
        built_filter_scans += 1;
        filter_scans[i].setRange(start.seg, start.rg, end.seg, end.rg, i == n_threads - 1);
        payload_scans[i] = try Scan.allocWithProjectionLoc(table.allocator, table, null, payload_cols, false, snap);
        built_payload_scans += 1;
        payload_scans[i].setRange(start.seg, start.rg, end.seg, end.rg, i == n_threads - 1);
    }
    snap.memtable_snap.release();
    pin_held = false;

    const total_t0 = nowTicks();
    const scan_t0 = nowTicks();
    var scan_threads = try allocator.alloc(std.Thread, n_threads);
    defer allocator.free(scan_threads);
    var scan_errs = try allocator.alloc(?anyerror, n_threads);
    defer allocator.free(scan_errs);
    @memset(scan_errs, null);

    i = 0;
    while (i < n_threads) : (i += 1) {
        scan_threads[i] = try std.Thread.spawn(.{}, lateFilterWorker, .{LateFilterJob{
            .filter_scan = filter_scans[i],
            .payload_scan = payload_scans[i],
            .parts = &parts[i],
            .allocator = allocator,
            .bucket_count = bucket_count,
            .kind = cfg.kind,
            .cpu = cpus[i % cpus.len],
            .err = &scan_errs[i],
        }});
    }
    i = 0;
    while (i < n_threads) : (i += 1) scan_threads[i].join();
    const scan_wall_ticks = nowTicks() - scan_t0;
    for (scan_errs) |maybe_err| if (maybe_err) |e| return e;

    var scan_cpu_ticks: i64 = 0;
    var partition_cpu_ticks: i64 = 0;
    var scanned_rows: u64 = 0;
    var filtered_rows: u64 = 0;
    for (parts) |p| {
        scan_cpu_ticks += p.scan_ticks;
        partition_cpu_ticks += p.partition_ticks;
        scanned_rows += p.scanned_count;
        filtered_rows += p.row_count;
    }

    const results = try allocator.alloc(BucketResult, bucket_count);
    defer allocator.free(results);
    for (results) |*r| r.* = .{};
    defer {
        for (results) |*r| r.deinit(allocator);
    }

    const group_t0 = nowTicks();
    var next_bucket = std.atomic.Value(usize).init(0);
    var group_threads = try allocator.alloc(std.Thread, n_threads);
    defer allocator.free(group_threads);
    var group_errs = try allocator.alloc(?anyerror, n_threads);
    defer allocator.free(group_errs);
    @memset(group_errs, null);

    i = 0;
    while (i < n_threads) : (i += 1) {
        group_threads[i] = try std.Thread.spawn(.{}, groupWorker, .{GroupJob{
            .allocator = allocator,
            .parts = parts,
            .results = results,
            .next_bucket = &next_bucket,
            .bucket_count = bucket_count,
            .q30_inline = cfg.q30_inline and cfg.kind == .q30,
            .cpu = cpus[i % cpus.len],
            .err = &group_errs[i],
        }});
    }
    i = 0;
    while (i < n_threads) : (i += 1) group_threads[i].join();
    const group_wall_ticks = nowTicks() - group_t0;
    for (group_errs) |maybe_err| if (maybe_err) |e| return e;

    const top_t0 = nowTicks();
    var top: TopSet = .{};
    var group_count: u64 = 0;
    var grouped_rows: u64 = 0;
    for (results) |r| {
        group_count += r.group_count;
        grouped_rows += r.row_count;
        for (r.top.items[0..r.top.len]) |candidate| top.consider(candidate);
    }
    std.mem.sort(TopRow, top.items[0..top.len], {}, topLess);
    const top_ticks = nowTicks() - top_t0;
    const total_ticks = nowTicks() - total_t0;

    std.debug.print(
        "[clientip-prof] query={s} DOP={d} scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} scan_partition_wall={d:.1}ms group_wall={d:.1}ms topn={d:.1}ms total={d:.1}ms mode=late-filter\n",
        .{
            cfg.kind.label(),
            n_threads,
            scanned_rows,
            total,
            filtered_rows,
            grouped_rows,
            group_count,
            ticksToMs(scan_wall_ticks, freq),
            ticksToMs(group_wall_ticks, freq),
            ticksToMs(top_ticks, freq),
            ticksToMs(total_ticks, freq),
        },
    );
    std.debug.print(
        "[clientip-prof-detail] query={s} scan_cpu={d:.1}ms partition_cpu={d:.1}ms\n",
        .{ cfg.kind.label(), ticksToMs(scan_cpu_ticks, freq), ticksToMs(partition_cpu_ticks, freq) },
    );
    for (top.items[0..top.len], 0..) |r, rank| {
        const avg = @as(f64, @floatFromInt(r.width_sum)) / @as(f64, @floatFromInt(r.count));
        std.debug.print(
            "[clientip-top] #{d} ClientIP={d} c={d} sum_refresh={d} avg_width={d:.6}\n",
            .{ rank + 1, r.key, r.count, r.refresh_sum, avg },
        );
    }
}

fn runSiloStaged(allocator: Allocator, table: *thindb.api.Table, cpus: []const usize, cfg: RunConfig) !void {
    const freq = perfFreq();
    const total = totalRows(table);
    const dop = @max(@as(usize, 1), cfg.dop);
    const bucket_count = cfg.bucket_count;
    const reserve_per_worker_bucket: usize = @intCast(@max(@as(u64, 1), total / @as(u64, @intCast(dop * bucket_count))));

    if (!cfg.quiet) {
        std.debug.print(
            "[clientip] query={s} DOP={d} buckets={d} rows={d} mode=silo-staged scan_threads={d} group_threads={d} reserve/worker/bucket={d}\n",
            .{ cfg.kind.label(), dop, bucket_count, total, dop, dop, reserve_per_worker_bucket },
        );
        std.debug.print("[clientip-cpu-assign] mode=silo-staged scan_phase=", .{});
        var ci: usize = 0;
        while (ci < dop) : (ci += 1) {
            if (ci != 0) std.debug.print(",", .{});
            std.debug.print("{d}", .{cpus[ci % cpus.len]});
        }
        std.debug.print(" group_phase=", .{});
        ci = 0;
        while (ci < dop) : (ci += 1) {
            if (ci != 0) std.debug.print(",", .{});
            std.debug.print("{d}", .{cpus[ci % cpus.len]});
        }
        std.debug.print("\n", .{});
        std.debug.print("[clientip-affinity] mode=silo-staged group_worker_i_runs_on_scan_core_i=true preferred_scan_part_first=true\n", .{});
    }

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

    const n_threads = @max(@as(usize, 1), @min(dop, @max(total_rgs, 1)));
    var scans = try allocator.alloc(*Scan, n_threads);
    defer allocator.free(scans);
    var built_scans: usize = 0;
    defer {
        for (scans[0..built_scans]) |s| s.deinit();
    }

    var parts = try allocator.alloc(WorkerParts, n_threads);
    defer allocator.free(parts);
    var built_parts: usize = 0;
    defer {
        for (parts[0..built_parts]) |*p| p.deinit(allocator);
    }

    var i: usize = 0;
    while (i < n_threads) : (i += 1) {
        parts[i] = try WorkerParts.init(allocator, bucket_count, reserve_per_worker_bucket);
        built_parts += 1;
        const lo = i * total_rgs / n_threads;
        const hi = if (i == n_threads - 1) total_rgs else (i + 1) * total_rgs / n_threads;
        const start = flatToCoord(lo, seg_start, snap.segment_count, total_rgs);
        const end = flatToCoord(hi, seg_start, snap.segment_count, total_rgs);
        scans[i] = try Scan.allocWithProjectionLoc(table.allocator, table, null, cfg.kind.columns(), false, snap);
        if (cfg.scan_filter) _ = try applyScanFilter(scans[i], cfg.kind);
        built_scans += 1;
        scans[i].setRange(start.seg, start.rg, end.seg, end.rg, i == n_threads - 1);
    }
    snap.memtable_snap.release();
    pin_held = false;

    const total_t0 = nowTicks();
    const scan_t0 = nowTicks();
    var scan_threads = try allocator.alloc(std.Thread, n_threads);
    defer allocator.free(scan_threads);
    var scan_errs = try allocator.alloc(?anyerror, n_threads);
    defer allocator.free(scan_errs);
    @memset(scan_errs, null);

    i = 0;
    while (i < n_threads) : (i += 1) {
        scan_threads[i] = try std.Thread.spawn(.{}, scanWorker, .{ScanJob{
            .scan = scans[i],
            .parts = &parts[i],
            .allocator = allocator,
            .bucket_count = bucket_count,
            .kind = cfg.kind,
            .cpu = cpus[i % cpus.len],
            .err = &scan_errs[i],
        }});
    }
    i = 0;
    while (i < n_threads) : (i += 1) scan_threads[i].join();
    const scan_wall_ticks = nowTicks() - scan_t0;
    for (scan_errs) |maybe_err| if (maybe_err) |e| return e;

    var scan_cpu_ticks: i64 = 0;
    var partition_cpu_ticks: i64 = 0;
    var scanned_rows: u64 = 0;
    var filtered_rows: u64 = 0;
    for (parts) |p| {
        scan_cpu_ticks += p.scan_ticks;
        partition_cpu_ticks += p.partition_ticks;
        scanned_rows += p.scanned_count;
        filtered_rows += p.row_count;
    }

    const results = try allocator.alloc(BucketResult, bucket_count);
    defer allocator.free(results);
    for (results) |*r| r.* = .{};
    defer {
        for (results) |*r| r.deinit(allocator);
    }

    var group_agg_ticks = try allocator.alloc(i64, n_threads);
    defer allocator.free(group_agg_ticks);
    @memset(group_agg_ticks, 0);
    var group_top_ticks = try allocator.alloc(i64, n_threads);
    defer allocator.free(group_top_ticks);
    @memset(group_top_ticks, 0);
    var group_chunks = try allocator.alloc(u64, n_threads);
    defer allocator.free(group_chunks);
    @memset(group_chunks, 0);

    const group_t0 = nowTicks();
    var group_threads = try allocator.alloc(std.Thread, n_threads);
    defer allocator.free(group_threads);
    var group_errs = try allocator.alloc(?anyerror, n_threads);
    defer allocator.free(group_errs);
    @memset(group_errs, null);

    i = 0;
    while (i < n_threads) : (i += 1) {
        group_threads[i] = try std.Thread.spawn(.{}, siloStagedWorker, .{SiloStagedJob{
            .allocator = allocator,
            .parts = parts,
            .results = results,
            .worker_index = i,
            .worker_count = n_threads,
            .bucket_count = bucket_count,
            .cpu = cpus[i % cpus.len],
            .agg_ticks = &group_agg_ticks[i],
            .local_top_ticks = &group_top_ticks[i],
            .chunks = &group_chunks[i],
            .err = &group_errs[i],
        }});
    }
    i = 0;
    while (i < n_threads) : (i += 1) group_threads[i].join();
    const group_wall_ticks = nowTicks() - group_t0;
    for (group_errs) |maybe_err| if (maybe_err) |e| return e;

    var aggregate_cpu_ticks: i64 = 0;
    var local_top_cpu_ticks: i64 = 0;
    var total_chunks: u64 = 0;
    for (group_agg_ticks) |ticks| aggregate_cpu_ticks += ticks;
    for (group_top_ticks) |ticks| local_top_cpu_ticks += ticks;
    for (group_chunks) |chunks| total_chunks += chunks;

    const top_t0 = nowTicks();
    var top: TopSet = .{};
    var group_count: u64 = 0;
    var grouped_rows: u64 = 0;
    for (results) |r| {
        group_count += r.group_count;
        grouped_rows += r.row_count;
        for (r.top.items[0..r.top.len]) |candidate| top.consider(candidate);
    }
    std.mem.sort(TopRow, top.items[0..top.len], {}, topLess);
    const final_merge_ticks = nowTicks() - top_t0;
    const total_ticks = nowTicks() - total_t0;

    if (!cfg.quiet) {
        std.debug.print(
            "[clientip-prof] query={s} DOP={d} scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} scan_partition_wall={d:.1}ms group_wall={d:.1}ms topn={d:.1}ms total={d:.1}ms mode=silo-staged\n",
            .{
                cfg.kind.label(),
                n_threads,
                scanned_rows,
                total,
                filtered_rows,
                grouped_rows,
                group_count,
                ticksToMs(scan_wall_ticks, freq),
                ticksToMs(group_wall_ticks, freq),
                ticksToMs(final_merge_ticks, freq),
                ticksToMs(total_ticks, freq),
            },
        );
        std.debug.print(
            "[clientip-silo-staged-stages] query={s} scan_stage_wall={d:.1}ms group_stage_wall={d:.1}ms scan_decode_cpu={d:.1}ms route_partition_cpu={d:.1}ms aggregate_cpu={d:.1}ms local_topn_cpu={d:.1}ms final_merge_wall={d:.3}ms chunks={d} rows_per_chunk={d:.1}\n",
            .{
                cfg.kind.label(),
                ticksToMs(scan_wall_ticks, freq),
                ticksToMs(group_wall_ticks, freq),
                ticksToMs(scan_cpu_ticks, freq),
                ticksToMs(partition_cpu_ticks, freq),
                ticksToMs(aggregate_cpu_ticks, freq),
                ticksToMs(local_top_cpu_ticks, freq),
                ticksToMs(final_merge_ticks, freq),
                total_chunks,
                if (total_chunks == 0) 0.0 else @as(f64, @floatFromInt(grouped_rows)) / @as(f64, @floatFromInt(total_chunks)),
            },
        );
        std.debug.print("[clientip-silo-staged-workers] query={s} aggregate_cpu_by_worker_ms=", .{cfg.kind.label()});
        for (group_agg_ticks, 0..) |ticks, worker_idx| {
            if (worker_idx != 0) std.debug.print(",", .{});
            std.debug.print("{d:.1}", .{ticksToMs(ticks, freq)});
        }
        std.debug.print(" local_topn_cpu_by_worker_ms=", .{});
        for (group_top_ticks, 0..) |ticks, worker_idx| {
            if (worker_idx != 0) std.debug.print(",", .{});
            std.debug.print("{d:.1}", .{ticksToMs(ticks, freq)});
        }
        std.debug.print(" chunks_by_worker=", .{});
        for (group_chunks, 0..) |chunks, worker_idx| {
            if (worker_idx != 0) std.debug.print(",", .{});
            std.debug.print("{d}", .{chunks});
        }
        std.debug.print("\n", .{});
        for (top.items[0..top.len], 0..) |r, rank| {
            const avg = @as(f64, @floatFromInt(r.width_sum)) / @as(f64, @floatFromInt(r.count));
            std.debug.print(
                "[clientip-top] #{d} ClientIP={d} c={d} sum_refresh={d} avg_width={d:.6}\n",
                .{ rank + 1, r.key, r.count, r.refresh_sum, avg },
            );
        }
    }
}

fn runSiloAdaptive(allocator: Allocator, table: *thindb.api.Table, cpus: []const usize, cfg: RunConfig) !void {
    const freq = perfFreq();
    const total = totalRows(table);
    const dop = @max(@as(usize, 1), cfg.dop);
    if (dop == 1) return runSerialDirect(allocator, table, cfg);

    const bucket_count = cfg.bucket_count;
    const total_workers = @max(@as(usize, 1), @min(dop, cpus.len));
    const scan_threads = chooseSiloScanThreads(cfg.kind, total_workers, cfg.pipe_scan_threads);
    const group_threads = total_workers - scan_threads;
    const chunk_rows = cfg.chunk_rows;
    const expected_groups_per_bucket: usize = @intCast(@max(@as(u64, 16), total / @as(u64, @intCast(bucket_count * 4))));
    const local_reserve_per_bucket: usize = @min(chunk_rows, @as(usize, @intCast(@max(@as(u64, 16), total / @as(u64, @intCast(scan_threads * bucket_count * 8))))));

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

    if (!cfg.quiet) {
        std.debug.print(
            "[clientip] query={s} DOP={d} buckets={d} rows={d} mode=silo-adaptive scan_threads={d} group_threads={d} total_workers={d} chunk_rows={d}\n",
            .{ cfg.kind.label(), dop, bucket_count, total, scan_threads, group_threads, total_workers, chunk_rows },
        );
        std.debug.print("[clientip-cpu-assign] mode=silo-adaptive scan_then_group=", .{});
        var ci: usize = 0;
        while (ci < scan_threads) : (ci += 1) {
            if (ci != 0) std.debug.print(",", .{});
            std.debug.print("{d}", .{cpus[ci % cpus.len]});
        }
        std.debug.print(" group_only=", .{});
        ci = scan_threads;
        while (ci < total_workers) : (ci += 1) {
            if (ci != scan_threads) std.debug.print(",", .{});
            std.debug.print("{d}", .{cpus[ci % cpus.len]});
        }
        std.debug.print("\n", .{});
        std.debug.print("[clientip-affinity] mode=silo-adaptive scan_workers_become_group_owners=true owner_count={d} physical_cores_distinct={s}\n", .{ total_workers, if (total_workers <= cpus.len) "true" else "false" });
    }

    var scans = try allocator.alloc(*Scan, scan_threads);
    defer allocator.free(scans);
    var built_scans: usize = 0;
    defer {
        for (scans[0..built_scans]) |s| s.deinit();
    }

    var parts = try allocator.alloc(WorkerParts, scan_threads);
    defer allocator.free(parts);
    var built_parts: usize = 0;
    defer {
        for (parts[0..built_parts]) |*p| p.deinit(allocator);
    }

    var buckets = try allocator.alloc(PipeBucket, bucket_count);
    defer allocator.free(buckets);
    var built_buckets: usize = 0;
    defer {
        for (buckets[0..built_buckets]) |*b| b.deinit(allocator);
    }
    var b: usize = 0;
    while (b < bucket_count) : (b += 1) {
        buckets[b] = try PipeBucket.init(allocator, expected_groups_per_bucket);
        built_buckets += 1;
        try buckets[b].states.ensureTotalCapacity(allocator, expected_groups_per_bucket);
        try buckets[b].chunks.ensureTotalCapacity(allocator, 8);
    }

    var shared = PipeShared{
        .allocator = allocator,
        .buckets = buckets,
        .bucket_count = bucket_count,
        .scan_threads = scan_threads,
    };

    var i: usize = 0;
    while (i < scan_threads) : (i += 1) {
        parts[i] = try WorkerParts.init(allocator, bucket_count, local_reserve_per_bucket);
        built_parts += 1;
        const lo = i * total_rgs / scan_threads;
        const hi = if (i == scan_threads - 1) total_rgs else (i + 1) * total_rgs / scan_threads;
        const start = flatToCoord(lo, seg_start, snap.segment_count, total_rgs);
        const end = flatToCoord(hi, seg_start, snap.segment_count, total_rgs);
        scans[i] = try Scan.allocWithProjectionLoc(table.allocator, table, null, cfg.kind.columns(), false, snap);
        if (cfg.scan_filter) _ = try applyScanFilter(scans[i], cfg.kind);
        built_scans += 1;
        scans[i].setRange(start.seg, start.rg, end.seg, end.rg, i == scan_threads - 1);
    }
    snap.memtable_snap.release();
    pin_held = false;

    var group_ticks = try allocator.alloc(i64, total_workers);
    defer allocator.free(group_ticks);
    @memset(group_ticks, 0);
    var idle_ticks = try allocator.alloc(i64, total_workers);
    defer allocator.free(idle_ticks);
    @memset(idle_ticks, 0);
    var top_ticks = try allocator.alloc(i64, total_workers);
    defer allocator.free(top_ticks);
    @memset(top_ticks, 0);
    var chunks = try allocator.alloc(u64, total_workers);
    defer allocator.free(chunks);
    @memset(chunks, 0);
    var worker_tops = try allocator.alloc(TopSet, total_workers);
    defer allocator.free(worker_tops);
    for (worker_tops) |*t| t.* = .{};

    const total_t0 = nowTicks();
    var threads = try allocator.alloc(std.Thread, total_workers);
    defer allocator.free(threads);
    var errs = try allocator.alloc(?anyerror, total_workers);
    defer allocator.free(errs);
    @memset(errs, null);

    i = 0;
    while (i < scan_threads) : (i += 1) {
        threads[i] = try std.Thread.spawn(.{}, siloAdaptiveScanWorker, .{SiloAdaptiveScanJob{
            .scan = scans[i],
            .local = &parts[i],
            .shared = &shared,
            .worker_index = i,
            .worker_count = total_workers,
            .kind = cfg.kind,
            .chunk_rows = chunk_rows,
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
    while (i < total_workers) : (i += 1) {
        threads[i] = try std.Thread.spawn(.{}, siloAdaptiveGroupWorker, .{SiloAdaptiveGroupJob{
            .shared = &shared,
            .worker_index = i,
            .worker_count = total_workers,
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
    while (i < total_workers) : (i += 1) threads[i].join();
    const worker_wall_ticks = nowTicks() - total_t0;
    for (errs) |maybe_err| if (maybe_err) |e| return e;

    var scanned_rows: u64 = 0;
    var filtered_rows: u64 = 0;
    for (parts) |p| {
        scanned_rows += p.scanned_count;
        filtered_rows += p.row_count;
    }

    var top: TopSet = .{};
    var group_count: u64 = 0;
    var grouped_rows: u64 = 0;
    for (buckets) |*bucket| {
        group_count += bucket.states.items.len;
        grouped_rows += bucket.row_count;
    }
    const final_t0 = nowTicks();
    for (worker_tops) |worker_top| {
        for (worker_top.items[0..worker_top.len]) |candidate| top.consider(candidate);
    }
    std.mem.sort(TopRow, top.items[0..top.len], {}, topLess);
    const final_merge_ticks = nowTicks() - final_t0;
    const total_ticks = nowTicks() - total_t0;

    if (!cfg.quiet) {
        std.debug.print(
            "[clientip-result] query={s} DOP={d} mode=silo-adaptive scan_threads={d} group_threads={d} chunk_rows={d} scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} total={d:.1}ms worker_wall={d:.1}ms final_merge={d:.3}ms no_profile={s}\n",
            .{
                cfg.kind.label(),
                total_workers,
                scan_threads,
                group_threads,
                chunk_rows,
                scanned_rows,
                total,
                filtered_rows,
                grouped_rows,
                group_count,
                ticksToMs(total_ticks, freq),
                ticksToMs(worker_wall_ticks, freq),
                ticksToMs(final_merge_ticks, freq),
                if (cfg.no_profile) "true" else "false",
            },
        );
    }
}

fn runSiloElastic(allocator: Allocator, table: *thindb.api.Table, cpus: []const usize, cfg: RunConfig) !void {
    const freq = perfFreq();
    const total = totalRows(table);
    const dop = @max(@as(usize, 1), cfg.dop);
    if (dop == 1) return runSerialDirect(allocator, table, cfg);

    const bucket_count = cfg.bucket_count;
    const n_workers = @max(@as(usize, 1), @min(dop, cpus.len));
    const chunk_rows = chooseElasticChunkRows(cfg.kind, cfg.chunk_rows, cfg.chunk_rows_set);
    const group_backlog_target = if (cfg.elastic_backlog > 0) cfg.elastic_backlog else ELASTIC_BACKLOG_CHUNKS;
    const expected_groups_per_bucket: usize = @intCast(@max(@as(u64, 16), total / @as(u64, @intCast(bucket_count * 4))));
    const local_reserve_per_bucket: usize = @min(chunk_rows, @as(usize, @intCast(@max(@as(u64, 16), total / @as(u64, @intCast(n_workers * bucket_count * 8))))));

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

    if (!cfg.quiet) {
        std.debug.print(
            "[clientip] query={s} DOP={d} buckets={d} rows={d} mode=silo-elastic workers={d} chunk_rows={d} backlog_target={d}\n",
            .{ cfg.kind.label(), dop, bucket_count, total, n_workers, chunk_rows, group_backlog_target },
        );
        std.debug.print("[clientip-cpu-assign] mode=silo-elastic workers=", .{});
        for (cpus[0..@min(n_workers, cpus.len)], 0..) |cpu, ci| {
            if (ci != 0) std.debug.print(",", .{});
            std.debug.print("{d}", .{cpu});
        }
        std.debug.print("\n", .{});
        std.debug.print("[clientip-affinity] mode=silo-elastic total_threads={d} workers_switch_between_scan_and_leased_group=true physical_cores_distinct={s}\n", .{ n_workers, if (n_workers <= cpus.len) "true" else "false" });
    }

    var scans = try allocator.alloc(*Scan, n_workers);
    defer allocator.free(scans);
    var built_scans: usize = 0;
    defer {
        for (scans[0..built_scans]) |s| s.deinit();
    }

    var parts = try allocator.alloc(WorkerParts, n_workers);
    defer allocator.free(parts);
    var built_parts: usize = 0;
    defer {
        for (parts[0..built_parts]) |*p| p.deinit(allocator);
    }

    var buckets = try allocator.alloc(PipeBucket, bucket_count);
    defer allocator.free(buckets);
    var built_buckets: usize = 0;
    defer {
        for (buckets[0..built_buckets]) |*b| b.deinit(allocator);
    }
    var b: usize = 0;
    while (b < bucket_count) : (b += 1) {
        buckets[b] = try PipeBucket.init(allocator, expected_groups_per_bucket);
        built_buckets += 1;
        try buckets[b].states.ensureTotalCapacity(allocator, expected_groups_per_bucket);
        try buckets[b].chunks.ensureTotalCapacity(allocator, 8);
    }

    var shared = PipeShared{
        .allocator = allocator,
        .buckets = buckets,
        .bucket_count = bucket_count,
        .scan_threads = n_workers,
    };

    var i: usize = 0;
    while (i < n_workers) : (i += 1) {
        parts[i] = try WorkerParts.init(allocator, bucket_count, local_reserve_per_bucket);
        built_parts += 1;
        const lo = i * total_rgs / n_workers;
        const hi = if (i == n_workers - 1) total_rgs else (i + 1) * total_rgs / n_workers;
        const start = flatToCoord(lo, seg_start, snap.segment_count, total_rgs);
        const end = flatToCoord(hi, seg_start, snap.segment_count, total_rgs);
        scans[i] = try Scan.allocWithProjectionLoc(table.allocator, table, null, cfg.kind.columns(), false, snap);
        if (cfg.scan_filter) _ = try applyScanFilter(scans[i], cfg.kind);
        built_scans += 1;
        scans[i].setRange(start.seg, start.rg, end.seg, end.rg, i == n_workers - 1);
    }
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

    const total_t0 = nowTicks();
    var threads = try allocator.alloc(std.Thread, n_workers);
    defer allocator.free(threads);
    var errs = try allocator.alloc(?anyerror, n_workers);
    defer allocator.free(errs);
    @memset(errs, null);

    i = 0;
    while (i < n_workers) : (i += 1) {
        threads[i] = try std.Thread.spawn(.{}, siloElasticWorker, .{SiloElasticJob{
            .scan = scans[i],
            .local = &parts[i],
            .shared = &shared,
            .worker_index = i,
            .worker_count = n_workers,
            .kind = cfg.kind,
            .chunk_rows = chunk_rows,
            .group_backlog_target = group_backlog_target,
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
    var scanned_rows: u64 = 0;
    var filtered_rows: u64 = 0;
    for (parts) |p| {
        scan_cpu_ticks += p.scan_ticks;
        partition_cpu_ticks += p.partition_ticks;
        publish_cpu_ticks += p.publish_ticks;
        scanned_rows += p.scanned_count;
        filtered_rows += p.row_count;
    }
    for (group_ticks) |ticks| group_cpu_ticks += ticks;
    for (idle_ticks) |ticks| idle_cpu_ticks += ticks;
    for (top_ticks) |ticks| top_cpu_ticks += ticks;
    for (chunks) |chunk_count| total_chunks += chunk_count;

    const final_t0 = nowTicks();
    var top: TopSet = .{};
    var group_count: u64 = 0;
    var grouped_rows: u64 = 0;
    for (buckets) |*bucket| {
        group_count += bucket.states.items.len;
        grouped_rows += bucket.row_count;
    }
    for (worker_tops) |worker_top| {
        for (worker_top.items[0..worker_top.len]) |candidate| top.consider(candidate);
    }
    std.mem.sort(TopRow, top.items[0..top.len], {}, topLess);
    const final_merge_ticks = nowTicks() - final_t0;
    const total_ticks = nowTicks() - total_t0;

    if (!cfg.quiet and cfg.no_profile) {
        std.debug.print(
            "[clientip-result] query={s} DOP={d} mode=silo-elastic workers={d} chunk_rows={d} backlog_target={d} scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} total={d:.1}ms worker_wall={d:.1}ms final_merge={d:.3}ms no_profile=true\n",
            .{
                cfg.kind.label(),
                n_workers,
                n_workers,
                chunk_rows,
                group_backlog_target,
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
    } else if (!cfg.quiet) {
        std.debug.print(
            "[clientip-prof] query={s} DOP={d} scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} total={d:.1}ms mode=silo-elastic workers={d} worker_wall={d:.1}ms final_merge={d:.3}ms\n",
            .{
                cfg.kind.label(),
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
            },
        );
        std.debug.print(
            "[clientip-silo-elastic-stages] query={s} scan_decode_cpu={d:.1}ms route_partition_cpu={d:.1}ms publish_queue_cpu={d:.1}ms aggregate_cpu={d:.1}ms idle_cpu={d:.1}ms local_topn_cpu={d:.1}ms final_merge_wall={d:.3}ms chunks={d} rows_per_chunk={d:.1} backlog_target={d}\n",
            .{
                cfg.kind.label(),
                ticksToMs(scan_cpu_ticks, freq),
                ticksToMs(partition_cpu_ticks, freq),
                ticksToMs(publish_cpu_ticks, freq),
                ticksToMs(group_cpu_ticks, freq),
                ticksToMs(idle_cpu_ticks, freq),
                ticksToMs(top_cpu_ticks, freq),
                ticksToMs(final_merge_ticks, freq),
                total_chunks,
                if (total_chunks == 0) 0.0 else @as(f64, @floatFromInt(grouped_rows)) / @as(f64, @floatFromInt(total_chunks)),
                group_backlog_target,
            },
        );
        std.debug.print("[clientip-silo-elastic-workers] query={s} aggregate_cpu_by_worker_ms=", .{cfg.kind.label()});
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

pub fn runSiloGrid(allocator: Allocator, table: *thindb.api.Table, cpus: []const usize, cfg: RunConfig) !void {
    const freq = perfFreq();
    const function_t0 = nowTicks();
    var pre_return_ticks: i64 = 0;
    defer {
        if (cfg.trace_timing and pre_return_ticks != 0) {
            std.debug.print("[harness-core-cleanup] query={s} total_cleanup_after_return={d:.1}ms\n", .{
                cfg.kind.label(),
                ticksToMs(nowTicks() - pre_return_ticks, freq),
            });
        }
    }
    const total = totalRows(table);
    const dop = @max(@as(usize, 1), cfg.dop);

    const bucket_count = cfg.bucket_count;
    const n_workers = @max(@as(usize, 1), @min(dop, cpus.len));
    const chunk_rows = chooseGridChunkRows(cfg.chunk_rows, cfg.chunk_rows_set);
    const raw_chunk_rows = @max(@as(usize, 1), cfg.raw_chunk_rows);
    const raw_group_chunk_rows = if (cfg.raw_group_chunk_rows == 0) raw_chunk_rows else @max(@as(usize, 1), cfg.raw_group_chunk_rows);
    const raw_batch_chunks = @max(@as(usize, 1), cfg.raw_batch_chunks);
    const group_rows_layout = cfg.group_rows_layout;
    const use_raw_group = cfg.raw_group_mode != .off;
    const scan_tile_rgs = chooseGridScanTileRgs(cfg.kind, cfg.scan_tile_rgs, cfg.scan_tile_rgs_set);
    const scan_coalesce_tiles = cfg.scan_coalesce_tiles;
    const route_block_rows = chooseRouteBlockRows(bucket_count, cfg.route_block_rows, cfg.route_block_rows_set);
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

    const snapshot_setup_t0 = if (cfg.trace_timing) nowTicks() else 0;
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

    const scan_columns = cfg.scan_columns orelse if (cfg.scan_filter and cfg.kind.hasFilter() and table.memtable.row_count == 0)
        cfg.kind.payloadColumns()
    else
        cfg.kind.columns();
    var stats_scan = try Scan.allocWithProjectionLoc(table.allocator, table, null, scan_columns, false, snap);
    defer {
        const cleanup_t0 = if (cfg.trace_timing) nowTicks() else 0;
        stats_scan.deinit();
        if (cfg.trace_timing) std.debug.print("[harness-core-cleanup] query={s} stats_scan={d:.3}ms\n", .{
            cfg.kind.label(),
            ticksToMs(nowTicks() - cleanup_t0, freq),
        });
    }
    const expected_groups_per_bucket = expectedGroupsPerBucket(total, bucket_count, stats_scan.stats(), cfg.kind, group_rows_layout.key_columns.len, group_rows_layout.key_width, cfg.filter_expr != null);
    const init_groups_per_bucket = if (cfg.group_init_cap > 0)
        @max(@as(usize, 16), @min(expected_groups_per_bucket, cfg.group_init_cap))
    else
        expected_groups_per_bucket;
    const snapshot_setup_ticks = if (cfg.trace_timing) nowTicks() - snapshot_setup_t0 else 0;

    if (!cfg.quiet) {
        std.debug.print(
            "[clientip] query={s} DOP={d} buckets={d} rows={d} mode=silo-grid workers={d} chunk_rows={d} raw_chunk_rows={d} raw_group_chunk_rows={d} raw_batch_chunks={d} scan_tile_rgs={d} scan_coalesce_tiles={d} route_block_rows={d} group_lease_buckets={d} group_lease_rows={d} local_reserve_per_bucket={d} worker_local_reserve_per_bucket={d} shared_scan_banks={d} shared_scan_reserve_per_bucket={d} flat_reserve_per_worker={d} expected_groups_per_bucket={d} init_groups_per_bucket={d} direct_final_local={s} force_queue_publish={s} shared_scan_buffers={s} flat_scan_partitions={s} raw_group_mode={s} shared_stage_builders={s} scheduler=group_rows_vs_scan_buffer_rows\n",
            .{ cfg.kind.label(), dop, bucket_count, total, n_workers, chunk_rows, raw_chunk_rows, raw_group_chunk_rows, raw_batch_chunks, scan_tile_rgs, scan_coalesce_tiles, route_block_rows, cfg.group_lease_buckets, cfg.group_lease_rows, local_reserve_per_bucket, worker_local_reserve_per_bucket, shared_scan_bank_count, shared_scan_reserve_per_bucket, flat_reserve_per_worker, expected_groups_per_bucket, init_groups_per_bucket, if (direct_final_local) "true" else "false", if (cfg.force_queue_publish) "true" else "false", if (use_shared_scan_buffers) "true" else "false", if (use_flat_scan_partitions) "true" else "false", @tagName(cfg.raw_group_mode), if (cfg.shared_stage_builders) "true" else "false" },
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
    var built_scans: usize = 0;
    defer {
        const cleanup_t0 = if (cfg.trace_timing) nowTicks() else 0;
        for (scans[0..built_scans]) |s| s.deinit();
        if (cfg.trace_timing) std.debug.print("[harness-core-cleanup] query={s} worker_scans={d:.1}ms count={d}\n", .{
            cfg.kind.label(),
            ticksToMs(nowTicks() - cleanup_t0, freq),
            built_scans,
        });
    }

    const bucket_setup_t0 = if (cfg.trace_timing) nowTicks() else 0;
    const using_workspace = cfg.workspace != null;
    var workspace_profile: WorkspaceProfile = .{};
    const workspace_profile_ptr: ?*WorkspaceProfile = if (cfg.trace_timing and using_workspace) &workspace_profile else null;
    var owned_parts: []WorkerParts = &.{};
    defer if (!using_workspace and owned_parts.len > 0) allocator.free(owned_parts);
    var built_parts: usize = 0;
    defer if (!using_workspace) {
        const cleanup_t0 = if (cfg.trace_timing) nowTicks() else 0;
        for (owned_parts[0..built_parts]) |*p| p.deinit(allocator);
        if (cfg.trace_timing) std.debug.print("[harness-core-cleanup] query={s} worker_parts={d:.1}ms count={d}\n", .{
            cfg.kind.label(),
            ticksToMs(nowTicks() - cleanup_t0, freq),
            built_parts,
        });
    };
    var owned_buckets: []PipeBucket = &.{};
    defer if (!using_workspace and owned_buckets.len > 0) allocator.free(owned_buckets);
    var built_buckets: usize = 0;
    defer if (!using_workspace) {
        const cleanup_t0 = if (cfg.trace_timing) nowTicks() else 0;
        for (owned_buckets[0..built_buckets]) |*bkt| bkt.deinit(allocator);
        if (cfg.trace_timing) std.debug.print("[harness-core-cleanup] query={s} pipe_buckets={d:.1}ms count={d}\n", .{
            cfg.kind.label(),
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
            try buckets[b].states.ensureTotalCapacity(allocator, init_groups_per_bucket);
            try buckets[b].chunks.ensureTotalCapacity(allocator, 8);
        }
    }
    const bucket_setup_ticks = if (cfg.trace_timing) nowTicks() - bucket_setup_t0 else 0;
    if (workspace_profile_ptr) |profile| {
        profile.printSetup(cfg.kind.label(), bucket_count, worker_local_reserve_per_bucket, init_groups_per_bucket);
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
        if (cfg.shared_stage_builders) {
            stage_builders = try allocator.alloc(StageBucketBuilder, bucket_count);
            for (stage_builders) |*builder| builder.* = .{};
        }
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
        .total_scan_rgs = total_rgs,
        .local_reserve_per_bucket = local_reserve_per_bucket,
        .route_block_rows = route_block_rows,
        .direct_final_local = direct_final_local,
        .local_parts = parts,
        .shared_scan_buffers = shared_scan_buffers_ptr,
    };
    raw_queues_moved_to_shared = true;
    defer deinitRawQueues(&shared);
    if (use_dedicated_raw_stage) {
        for (shared.raw_scan_queues) |*queue| try queue.chunks.ensureTotalCapacity(allocator, 8);
        for (shared.raw_group_queues) |*queue| try queue.chunks.ensureTotalCapacity(allocator, 8);
    }
    var i: usize = 0;
    const worker_setup_t0 = if (cfg.trace_timing) nowTicks() else 0;
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
            try parts[i].flat_rows.ensureTotalCapacity(allocator, stage_scratch_rows);
            try parts[i].flat_raw_rows.ensureTotalCapacity(allocator, cfg.group_rows_layout, stage_scratch_rows);
            try parts[i].flat_bucket_ids.ensureTotalCapacity(allocator, stage_scratch_rows);
        }
        scans[i] = try Scan.allocWithProjectionLoc(table.allocator, table, null, scan_columns, false, snap);
        if (cfg.scan_filter) {
            if (cfg.filter_expr) |expr| {
                _ = try applyScanFilterExpr(scans[i], expr);
            } else {
                _ = try applyScanFilter(scans[i], cfg.kind);
            }
        }
        built_scans += 1;
        scans[i].setRange(0, 0, 0, 0, false);
    }
    const worker_setup_ticks = if (cfg.trace_timing) nowTicks() - worker_setup_t0 else 0;
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

    const setup_ticks = nowTicks() - function_t0;
    if (cfg.trace_timing) {
        std.debug.print("[harness-core-setup] query={s} total={d:.1}ms snapshot_stats={d:.1}ms pipe_buckets={d:.1}ms worker_parts_scans={d:.1}ms other={d:.1}ms\n", .{
            cfg.kind.label(),
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
            .local = &parts[i],
            .shared = &shared,
            .seg_start = seg_start,
            .segment_count = snap.segment_count,
            .worker_index = i,
            .worker_count = n_workers,
            .kind = cfg.kind,
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
    var top: TopSet = .{};
    var group_count: u64 = 0;
    var grouped_rows: u64 = 0;
    for (buckets) |*bucket| {
        group_count += bucket.states.items.len;
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
        if (cfg.result_all_groups) {
            for (buckets) |*bucket| {
                for (bucket.states.items) |s| try out.append(allocator, topRowFromState(s));
            }
        } else {
            try out.appendSlice(allocator, top.items[0..top.len]);
        }
    }
    if (cfg.trace_timing) {
        std.debug.print(
            "[harness-core-timing] query={s} full={d:.1}ms setup_before_workers={d:.1}ms worker_and_final={d:.1}ms final_merge={d:.3}ms result_rows={d}\n",
            .{
                cfg.kind.label(),
                ticksToMs(function_ticks, freq),
                ticksToMs(setup_ticks, freq),
                ticksToMs(total_ticks, freq),
                ticksToMs(final_merge_ticks, freq),
                top.len,
            },
        );
    }
    pre_return_ticks = nowTicks();

    if (!cfg.quiet and cfg.no_profile) {
        std.debug.print(
            "[clientip-result] query={s} DOP={d} mode=silo-grid workers={d} chunk_rows={d} scan_tile_rgs={d} scan_coalesce_tiles={d} group_lease_buckets={d} group_lease_rows={d} scheduler=group_rows_vs_scan_buffer_rows scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} total={d:.1}ms worker_wall={d:.1}ms final_merge={d:.3}ms no_profile=true\n",
            .{
                cfg.kind.label(),
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
    } else if (!cfg.quiet) {
        std.debug.print(
            "[clientip-prof] query={s} DOP={d} scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} total={d:.1}ms mode=silo-grid workers={d} worker_wall={d:.1}ms final_merge={d:.3}ms group_lease_buckets={d} group_lease_rows={d}\n",
            .{
                cfg.kind.label(),
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
                cfg.kind.label(),
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
                cfg.kind.label(),
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
                cfg.kind.label(),
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
                cfg.kind.label(),
                ticksToMs(raw_queue_lock_ticks, freq),
                ticksToMs(raw_scan_queue_lock_ticks, freq),
                ticksToMs(raw_group_queue_lock_ticks, freq),
                ticksToMs(raw_recycle_lock_ticks, freq),
                ticksToMs(raw_stage_builder_lock_ticks, freq),
                ticksToMs(raw_agg_lock_ticks, freq),
                ticksToMs(raw_queue_lock_ticks + raw_scan_queue_lock_ticks + raw_group_queue_lock_ticks + raw_recycle_lock_ticks + raw_stage_builder_lock_ticks + raw_agg_lock_ticks, freq),
            },
        );
        std.debug.print("[clientip-silo-grid-workers] query={s} aggregate_cpu_by_worker_ms=", .{cfg.kind.label()});
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

fn runOnce(allocator: Allocator, table: *thindb.api.Table, cpus: []const usize, cfg: RunConfig) !void {
    if (cfg.dop == 1) return runSerialDirect(allocator, table, cfg);
    if (cfg.late_filter) return runLateFilter(allocator, table, cpus, cfg);
    if (cfg.local_preagg) return runLocalPreAgg(allocator, table, cpus, cfg);
    if (cfg.silo_grid) return runSiloGrid(allocator, table, cpus, cfg);
    if (cfg.silo) return runPipe(allocator, table, cpus, cfg);
    if (cfg.pipe) return runPipe(allocator, table, cpus, cfg);
    if (cfg.stream) return runStream(allocator, table, cpus, cfg);

    const freq = perfFreq();
    const total = totalRows(table);
    const dop = @max(@as(usize, 1), cfg.dop);
    const bucket_count = cfg.bucket_count;
    const reserve_per_worker_bucket: usize = @intCast(@max(@as(u64, 1), total / @as(u64, @intCast(dop * bucket_count))));

    if (!cfg.quiet) {
        std.debug.print(
            "[clientip] query={s} DOP={d} buckets={d} rows={d} reserve/worker/bucket={d}\n",
            .{ cfg.kind.label(), dop, bucket_count, total, reserve_per_worker_bucket },
        );
    }

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

    const n_threads = @max(@as(usize, 1), @min(dop, @max(total_rgs, 1)));
    var scans = try allocator.alloc(*Scan, n_threads);
    defer allocator.free(scans);
    var built_scans: usize = 0;
    defer {
        for (scans[0..built_scans]) |s| s.deinit();
    }

    var parts = try allocator.alloc(WorkerParts, n_threads);
    defer allocator.free(parts);
    var built_parts: usize = 0;
    defer {
        for (parts[0..built_parts]) |*p| p.deinit(allocator);
    }

    var i: usize = 0;
    while (i < n_threads) : (i += 1) {
        parts[i] = try WorkerParts.init(allocator, bucket_count, reserve_per_worker_bucket);
        built_parts += 1;
        const lo = i * total_rgs / n_threads;
        const hi = if (i == n_threads - 1) total_rgs else (i + 1) * total_rgs / n_threads;
        const start = flatToCoord(lo, seg_start, snap.segment_count, total_rgs);
        const end = flatToCoord(hi, seg_start, snap.segment_count, total_rgs);
        scans[i] = try Scan.allocWithProjectionLoc(table.allocator, table, null, cfg.kind.columns(), false, snap);
        if (cfg.scan_filter) _ = try applyScanFilter(scans[i], cfg.kind);
        built_scans += 1;
        scans[i].setRange(start.seg, start.rg, end.seg, end.rg, i == n_threads - 1);
    }
    snap.memtable_snap.release();
    pin_held = false;

    const total_t0 = nowTicks();
    const scan_t0 = nowTicks();
    var scan_threads = try allocator.alloc(std.Thread, n_threads);
    defer allocator.free(scan_threads);
    var scan_errs = try allocator.alloc(?anyerror, n_threads);
    defer allocator.free(scan_errs);
    @memset(scan_errs, null);

    i = 0;
    while (i < n_threads) : (i += 1) {
        scan_threads[i] = try std.Thread.spawn(.{}, scanWorker, .{ScanJob{
            .scan = scans[i],
            .parts = &parts[i],
            .allocator = allocator,
            .bucket_count = bucket_count,
            .kind = cfg.kind,
            .cpu = cpus[i % cpus.len],
            .err = &scan_errs[i],
        }});
    }
    i = 0;
    while (i < n_threads) : (i += 1) scan_threads[i].join();
    const scan_wall_ticks = nowTicks() - scan_t0;
    for (scan_errs) |maybe_err| if (maybe_err) |e| return e;

    var scan_cpu_ticks: i64 = 0;
    var partition_cpu_ticks: i64 = 0;
    var scanned_rows: u64 = 0;
    var filtered_rows: u64 = 0;
    for (parts) |p| {
        scan_cpu_ticks += p.scan_ticks;
        partition_cpu_ticks += p.partition_ticks;
        scanned_rows += p.scanned_count;
        filtered_rows += p.row_count;
    }

    const results = try allocator.alloc(BucketResult, bucket_count);
    defer allocator.free(results);
    for (results) |*r| r.* = .{};
    defer {
        for (results) |*r| r.deinit(allocator);
    }

    const group_t0 = nowTicks();
    var next_bucket = std.atomic.Value(usize).init(0);
    var group_threads = try allocator.alloc(std.Thread, n_threads);
    defer allocator.free(group_threads);
    var group_errs = try allocator.alloc(?anyerror, n_threads);
    defer allocator.free(group_errs);
    @memset(group_errs, null);

    i = 0;
    while (i < n_threads) : (i += 1) {
        group_threads[i] = try std.Thread.spawn(.{}, groupWorker, .{GroupJob{
            .allocator = allocator,
            .parts = parts,
            .results = results,
            .next_bucket = &next_bucket,
            .bucket_count = bucket_count,
            .q30_inline = cfg.q30_inline and cfg.kind == .q30,
            .cpu = cpus[i % cpus.len],
            .err = &group_errs[i],
        }});
    }
    i = 0;
    while (i < n_threads) : (i += 1) group_threads[i].join();
    const group_wall_ticks = nowTicks() - group_t0;
    for (group_errs) |maybe_err| if (maybe_err) |e| return e;

    const top_t0 = nowTicks();
    var top: TopSet = .{};
    var group_count: u64 = 0;
    var grouped_rows: u64 = 0;
    for (results) |r| {
        group_count += r.group_count;
        grouped_rows += r.row_count;
        for (r.top.items[0..r.top.len]) |candidate| {
            top.consider(candidate);
        }
    }
    std.mem.sort(TopRow, top.items[0..top.len], {}, topLess);
    const top_ticks = nowTicks() - top_t0;
    const total_ticks = nowTicks() - total_t0;

    if (!cfg.quiet) {
        std.debug.print(
            "[clientip-prof] query={s} DOP={d} scanned={d}/{d} filtered={d} grouped_rows={d} groups={d} scan_partition_wall={d:.1}ms group_wall={d:.1}ms topn={d:.1}ms total={d:.1}ms\n",
            .{
                cfg.kind.label(),
                n_threads,
                scanned_rows,
                total,
                filtered_rows,
                grouped_rows,
                group_count,
                ticksToMs(scan_wall_ticks, freq),
                ticksToMs(group_wall_ticks, freq),
                ticksToMs(top_ticks, freq),
                ticksToMs(total_ticks, freq),
            },
        );
        std.debug.print(
            "[clientip-prof-detail] query={s} scan_cpu={d:.1}ms partition_cpu={d:.1}ms\n",
            .{ cfg.kind.label(), ticksToMs(scan_cpu_ticks, freq), ticksToMs(partition_cpu_ticks, freq) },
        );
        for (top.items[0..top.len], 0..) |r, rank| {
            const avg = @as(f64, @floatFromInt(r.width_sum)) / @as(f64, @floatFromInt(r.count));
            std.debug.print(
                "[clientip-top] #{d} ClientIP={d} c={d} sum_refresh={d} avg_width={d:.6}\n",
                .{ rank + 1, r.key, r.count, r.refresh_sum, avg },
            );
        }
    }
}

fn parsePositiveUsize(text: []const u8) !usize {
    const v = try std.fmt.parseUnsigned(usize, text, 10);
    if (v == 0) return error.InvalidArgument;
    return v;
}

fn parseQueryKind(text: []const u8) !QueryKind {
    if (std.mem.eql(u8, text, "clientip")) return .clientip;
    if (std.mem.eql(u8, text, "30") or std.mem.eql(u8, text, "q30")) return .q30;
    if (std.mem.eql(u8, text, "31") or std.mem.eql(u8, text, "q31")) return .q31;
    if (std.mem.eql(u8, text, "32") or std.mem.eql(u8, text, "q32")) return .q32;
    return error.InvalidArgument;
}

pub fn main(init: std.process.Init) !u8 {
    const allocator = init.gpa;

    var dops_buf: [16]usize = undefined;
    var dops_len: usize = 0;
    var queries_buf: [8]QueryKind = undefined;
    var queries_len: usize = 0;
    var bucket_count: usize = 256;
    var bucket_count_set = false;
    var stream = false;
    var pipe = false;
    var pipe_tail = false;
    var silo = false;
    var silo_grid = false;
    var local_preagg = false;
    var q30_inline = false;
    var late_filter = false;
    var scan_filter = true;
    var pipe_scan_threads: usize = 0;
    var chunk_rows: usize = PIPE_CHUNK_ROWS;
    var chunk_rows_set = false;
    var scan_tile_rgs: usize = GRID_SCAN_TILE_RGS;
    var scan_tile_rgs_set = false;
    var scan_coalesce_tiles: usize = GRID_SCAN_COALESCE_TILES;
    var route_block_rows: usize = AUTO_ROUTE_BLOCK_ROWS;
    var route_block_rows_set = false;
    var group_lease_buckets: usize = 1;
    var group_lease_buckets_set = false;
    var group_lease_rows: u64 = 0;
    var scan_yield_chunks: usize = GRID_SCAN_YIELD_CHUNKS;
    var scan_resume_chunks: usize = 0;
    var elastic_backlog: usize = 0;
    var no_profile = false;
    var warmup_runs: usize = 1;
    var repeat_runs: usize = 1;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--dop")) {
            const value = args.next() orelse return error.InvalidArgument;
            if (dops_len >= dops_buf.len) return error.InvalidArgument;
            dops_buf[dops_len] = try parsePositiveUsize(value);
            dops_len += 1;
        } else if (std.mem.eql(u8, arg, "--buckets")) {
            const value = args.next() orelse return error.InvalidArgument;
            bucket_count = try parsePositiveUsize(value);
            bucket_count_set = true;
        } else if (std.mem.eql(u8, arg, "--query")) {
            const value = args.next() orelse return error.InvalidArgument;
            if (queries_len >= queries_buf.len) return error.InvalidArgument;
            queries_buf[queries_len] = try parseQueryKind(value);
            queries_len += 1;
        } else if (std.mem.eql(u8, arg, "--stream")) {
            stream = true;
        } else if (std.mem.eql(u8, arg, "--pipe")) {
            pipe = true;
        } else if (std.mem.eql(u8, arg, "--pipe-tail")) {
            pipe = true;
            pipe_tail = true;
        } else if (std.mem.eql(u8, arg, "--silo")) {
            silo = true;
        } else if (std.mem.eql(u8, arg, "--silo-grid")) {
            silo_grid = true;
        } else if (std.mem.eql(u8, arg, "--local-preagg")) {
            local_preagg = true;
        } else if (std.mem.eql(u8, arg, "--q30-inline")) {
            q30_inline = true;
        } else if (std.mem.eql(u8, arg, "--late-filter")) {
            late_filter = true;
        } else if (std.mem.eql(u8, arg, "--scan-filter")) {
            scan_filter = true;
        } else if (std.mem.eql(u8, arg, "--manual-filter")) {
            scan_filter = false;
        } else if (std.mem.eql(u8, arg, "--pipe-scan")) {
            const value = args.next() orelse return error.InvalidArgument;
            pipe_scan_threads = try parsePositiveUsize(value);
        } else if (std.mem.eql(u8, arg, "--chunk-rows")) {
            const value = args.next() orelse return error.InvalidArgument;
            chunk_rows = try parsePositiveUsize(value);
            chunk_rows_set = true;
        } else if (std.mem.eql(u8, arg, "--elastic-backlog")) {
            const value = args.next() orelse return error.InvalidArgument;
            elastic_backlog = try parsePositiveUsize(value);
        } else if (std.mem.eql(u8, arg, "--scan-yield-chunks")) {
            const value = args.next() orelse return error.InvalidArgument;
            scan_yield_chunks = try parsePositiveUsize(value);
        } else if (std.mem.eql(u8, arg, "--scan-resume-chunks")) {
            const value = args.next() orelse return error.InvalidArgument;
            scan_resume_chunks = try parsePositiveUsize(value);
        } else if (std.mem.eql(u8, arg, "--scan-tile-rgs")) {
            const value = args.next() orelse return error.InvalidArgument;
            scan_tile_rgs = try parsePositiveUsize(value);
            scan_tile_rgs_set = true;
        } else if (std.mem.eql(u8, arg, "--scan-coalesce-tiles")) {
            const value = args.next() orelse return error.InvalidArgument;
            scan_coalesce_tiles = try parsePositiveUsize(value);
        } else if (std.mem.eql(u8, arg, "--route-block-rows")) {
            const value = args.next() orelse return error.InvalidArgument;
            route_block_rows = normalizeRouteBlockRows(try parsePositiveUsize(value));
            route_block_rows_set = true;
        } else if (std.mem.eql(u8, arg, "--group-lease-buckets")) {
            const value = args.next() orelse return error.InvalidArgument;
            group_lease_buckets = @min(try parsePositiveUsize(value), MAX_GROUP_LEASE_BUCKETS);
            group_lease_buckets_set = true;
        } else if (std.mem.eql(u8, arg, "--group-lease-rows")) {
            const value = args.next() orelse return error.InvalidArgument;
            group_lease_rows = @intCast(try parsePositiveUsize(value));
        } else if (std.mem.eql(u8, arg, "--no-profile")) {
            no_profile = true;
        } else if (std.mem.eql(u8, arg, "--warmup")) {
            const value = args.next() orelse return error.InvalidArgument;
            warmup_runs = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--repeat")) {
            const value = args.next() orelse return error.InvalidArgument;
            repeat_runs = try parsePositiveUsize(value);
        } else {
            std.debug.print("usage: zig build clientip -- [--query q30|q31|q32|clientip ...] [--dop N ...] [--buckets N] [--stream|--pipe|--pipe-tail|--silo|--silo-grid|--local-preagg|--q30-inline|--late-filter|--scan-filter|--manual-filter] [--pipe-scan N] [--chunk-rows N] [--elastic-backlog N] [--scan-yield-chunks N] [--scan-resume-chunks N] [--scan-tile-rgs N] [--scan-coalesce-tiles N] [--route-block-rows N] [--group-lease-buckets N] [--group-lease-rows N] [--no-profile] [--warmup N] [--repeat N]\n", .{});
            return error.InvalidArgument;
        }
    }
    if (group_lease_rows > 0 and !group_lease_buckets_set) {
        group_lease_buckets = MAX_GROUP_LEASE_BUCKETS;
    }
    if (dops_len == 0) {
        dops_buf[0] = 1;
        dops_buf[1] = 12;
        dops_len = 2;
    }
    if (queries_len == 0) {
        queries_buf[0] = .clientip;
        queries_len = 1;
    }

    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const cwd = std.Io.Dir.cwd();
    var data_root = try cwd.createDirPathOpen(io, ".clickbench-db", .{});
    defer data_root.close(io);

    const catalog = try thindb.Catalog.open(allocator, io, data_root, .{});
    defer catalog.close();
    const table = try findHitsTable(allocator, catalog);

    const layout = try cpuLayout(allocator);
    defer layout.deinit(allocator);
    const physical_cpus = layout.order[0..layout.physical_count];
    std.debug.print("[clientip] cpu topology physical_cores={d} logical_slots={d} primary_cpu_order=", .{ layout.physical_count, layout.order.len });
    for (physical_cpus, 0..) |cpu, i| {
        if (i != 0) std.debug.print(",", .{});
        std.debug.print("{d}", .{cpu});
    }
    std.debug.print("\n", .{});

    for (queries_buf[0..queries_len]) |kind| {
        for (dops_buf[0..dops_len]) |dop| {
            const effective_buckets = if (bucket_count_set) bucket_count else autoBucketCount(kind, dop, totalRows(table));
            var cfg = RunConfig{
                .dop = dop,
                .bucket_count = effective_buckets,
                .kind = kind,
                .stream = stream,
                .pipe = pipe,
                .pipe_tail = pipe_tail,
                .silo = silo,
                .silo_grid = silo_grid,
                .local_preagg = local_preagg,
                .q30_inline = q30_inline,
                .late_filter = late_filter,
                .scan_filter = scan_filter,
                .pipe_scan_threads = pipe_scan_threads,
                .chunk_rows = chunk_rows,
                .chunk_rows_set = chunk_rows_set,
                .scan_tile_rgs = scan_tile_rgs,
                .scan_tile_rgs_set = scan_tile_rgs_set,
                .scan_coalesce_tiles = scan_coalesce_tiles,
                .route_block_rows = route_block_rows,
                .route_block_rows_set = route_block_rows_set,
                .group_lease_buckets = group_lease_buckets,
                .group_lease_rows = group_lease_rows,
                .scan_yield_chunks = scan_yield_chunks,
                .scan_resume_chunks = scan_resume_chunks,
                .elastic_backlog = elastic_backlog,
                .no_profile = no_profile,
            };
            var w: usize = 0;
            while (w < warmup_runs) : (w += 1) {
                cfg.quiet = true;
                try runOnce(std.heap.smp_allocator, table, physical_cpus, cfg);
            }
            var r: usize = 0;
            while (r < repeat_runs) : (r += 1) {
                cfg.quiet = false;
                if (repeat_runs > 1) {
                    std.debug.print("[clientip-repeat] query={s} DOP={d} iteration={d}/{d} warmup={d}\n", .{ kind.label(), dop, r + 1, repeat_runs, warmup_runs });
                }
                try runOnce(std.heap.smp_allocator, table, physical_cpus, cfg);
            }
        }
    }
    return 0;
}
