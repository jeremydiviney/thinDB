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
const thindb = @import("thindb");

const Allocator = std.mem.Allocator;
const Scan = thindb.exec.Scan;
const GroupTable = thindb.exec.group_table.IntKeyTable(96);
const Q30InlineTable = thindb.exec.group_table.InlineSlotTable(u64, Q30InlineState);

const COLS_CLIENTIP = [_][]const u8{ "ClientIP", "IsRefresh", "ResolutionWidth" };
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

const QueryKind = enum {
    clientip,
    q30,
    q31,
    q32,

    fn label(self: QueryKind) []const u8 {
        return switch (self) {
            .clientip => "clientip",
            .q30 => "q30",
            .q31 => "q31",
            .q32 => "q32",
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
        .q30 => .{ .columns = &COLS_Q30, .payload_columns = &COLS_Q30_PAYLOAD, .group_key_columns = 2, .filter = phrase_non_empty },
        .q31 => .{ .columns = &COLS_Q31, .payload_columns = &COLS_Q31_PAYLOAD, .group_key_columns = 2, .filter = phrase_non_empty },
        .q32 => .{ .columns = &COLS_Q32, .group_key_columns = 2 },
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

const CpuLayout = struct {
    order: []usize,
    physical_count: usize,

    fn deinit(self: CpuLayout, allocator: Allocator) void {
        allocator.free(self.order);
    }
};

fn cpuLayout(allocator: Allocator) !CpuLayout {
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

const PartBucket = struct {
    rows: std.ArrayListUnmanaged(Row) = .empty,

    fn deinit(self: *PartBucket, allocator: Allocator) void {
        self.rows.deinit(allocator);
        self.* = .{};
    }
};

const WorkerParts = struct {
    buckets: []PartBucket = &.{},
    worker_index: usize = 0,
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
    sched_loops: u64 = 0,
    sched_scan_jobs: u64 = 0,
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
        self.dirty_buckets.deinit(allocator);
        if (self.dirty_marks.len > 0) allocator.free(self.dirty_marks);
        if (self.route_counts.len > 0) allocator.free(self.route_counts);
        if (self.route_offsets.len > 0) allocator.free(self.route_offsets);
        self.route_touched.deinit(allocator);
        for (self.recycled_rows.items) |*rows| rows.deinit(allocator);
        self.recycled_rows.deinit(allocator);
        self.* = .{};
    }
};

const State = struct {
    key: u128,
    count: u64,
    refresh_sum: i64,
    width_sum: i64,
};

const Q30InlineState = struct {
    count: u64,
    refresh_sum: i64,
    width_sum: i64,
};

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
};

fn lockSpin(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
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

const TopRow = struct {
    key: u128,
    count: u64,
    refresh_sum: i64,
    width_sum: i64,
};

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
    client_ip: []const i32,
    watch_id: ?[]const i64 = null,
    search_engine: ?thindb.storage.ColumnView = null,
    refresh: []const i16,
    width: []const i16,
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
        .q30 => packQ30(try readSearchEngine(v.search_engine.?, row), v.client_ip[row]),
        .q31, .q32 => packWatchClient(v.watch_id.?[row], v.client_ip[row]),
    };
}

inline fn rowPasses(v: BatchViews, row: usize) bool {
    return if (v.phrase) |p| p.offsets[row] != p.offsets[row + 1] else true;
}

fn searchPhrasePred() thindb.Predicate {
    return .{ .col = "SearchPhrase", .op = .neq, .val = .{ .text = "" } };
}

fn applyScanFilter(scan: *Scan, kind: QueryKind) !bool {
    const pred = querySpec(kind).filter orelse return false;
    try scan.addPrune(pred);
    return try scan.tryFuseFilter(.{ .leaf = pred });
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

inline fn appendPartitionRow(parts: *WorkerParts, bucket_idx: usize, allocator: Allocator, row: Row) !void {
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
    if (parts.buckets.len >= rows.len / 2) {
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
        .q30 => appendBatchQ30(parts, allocator, bucket_count, views, batch.row_count, route_block_rows, skip_filter_check),
        .q31 => appendBatchQ31(parts, allocator, bucket_count, views, batch.row_count, route_block_rows, skip_filter_check),
        .q32 => appendBatchQ32(parts, allocator, bucket_count, views, batch.row_count, route_block_rows),
    };
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
    const gid: u32 = @intCast(states.items.len);
    states.appendAssumeCapacity(.{
        .key = key,
        .count = 1,
        .refresh_sum = row.refresh,
        .width_sum = row.width,
    });
    return gid;
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
        var st = &states.items[probe.gid];
        st.count += 1;
        st.refresh_sum += row.refresh;
        st.width_sum += row.width;
    }
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
    }
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
    for (states.items) |s| {
        result.top.consider(.{
            .key = s.key,
            .count = s.count,
            .refresh_sum = s.refresh_sum,
            .width_sum = s.width_sum,
        });
    }
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
    for (states.items) |s| {
        result.top.consider(.{
            .key = s.key,
            .count = s.count,
            .refresh_sum = s.refresh_sum,
            .width_sum = s.width_sum,
        });
    }
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
    for (states.items) |s| {
        result.top.consider(.{
            .key = s.key,
            .count = s.count,
            .refresh_sum = s.refresh_sum,
            .width_sum = s.width_sum,
        });
    }
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
        for (job.shared.buckets[b].states.items) |s| {
            local_top.consider(.{
                .key = s.key,
                .count = s.count,
                .refresh_sum = s.refresh_sum,
                .width_sum = s.width_sum,
            });
        }
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
        defer _ = shared.active_group_jobs.fetchSub(1, .release);

        const bucket = &shared.buckets[b];
        var did_work = false;
        for (shared.local_parts) |*part| {
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
        if (did_work) return true;
    }
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
    while (b < shared.bucket_count) : (b += worker_count) {
        for (shared.buckets[b].states.items) |s| {
            local_top.consider(.{
                .key = s.key,
                .count = s.count,
                .refresh_sum = s.refresh_sum,
                .width_sum = s.width_sum,
            });
        }
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
        for (job.shared.buckets[b].states.items) |s| {
            local_top.consider(.{
                .key = s.key,
                .count = s.count,
                .refresh_sum = s.refresh_sum,
                .width_sum = s.width_sum,
            });
        }
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
        try appendBatchSiloLocal(job.local, job.shared, job.kind, batch, job.profile, job.filter_fused);
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
    if (!job.shared.direct_final_local) {
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

        if (group_first and group_queued_rows > 0) {
            if (try drainLeasedSilos(
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
            )) {
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
            if (try drainLeasedSilos(
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
            )) {
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

const RunConfig = struct {
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
    no_profile: bool = false,
    quiet: bool = false,
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

fn expectedGroupsPerBucket(total_rows: u64, bucket_count: usize, stats: thindb.exec.PipelineStats, kind: QueryKind) usize {
    const conservative_total = @max(@as(u64, 16), total_rows / 4);
    const estimated_total = estimateGroupCountFromStats(stats, kind.groupKeyColumnCount()) orelse conservative_total;
    const no_filter_near_unique = !kind.hasFilter() and estimated_total * 4 >= total_rows * 3;
    const total_groups = if (no_filter_near_unique) estimated_total else conservative_total;
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
    for (states.items) |s| {
        top.consider(.{
            .key = s.key,
            .count = s.count,
            .refresh_sum = s.refresh_sum,
            .width_sum = s.width_sum,
        });
    }
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
        for (cb.states.items) |s| {
            local_top.consider(.{
                .key = s.key,
                .count = s.count,
                .refresh_sum = s.refresh_sum,
                .width_sum = s.width_sum,
            });
        }
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
            for (bucket.states.items) |s| {
                local_top.consider(.{
                    .key = s.key,
                    .count = s.count,
                    .refresh_sum = s.refresh_sum,
                    .width_sum = s.width_sum,
                });
            }
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

fn runSiloGrid(allocator: Allocator, table: *thindb.api.Table, cpus: []const usize, cfg: RunConfig) !void {
    const freq = perfFreq();
    const total = totalRows(table);
    const dop = @max(@as(usize, 1), cfg.dop);
    if (dop == 1) return runSerialDirect(allocator, table, cfg);

    const bucket_count = cfg.bucket_count;
    const n_workers = @max(@as(usize, 1), @min(dop, cpus.len));
    const chunk_rows = chooseGridChunkRows(cfg.chunk_rows, cfg.chunk_rows_set);
    const scan_tile_rgs = chooseGridScanTileRgs(cfg.kind, cfg.scan_tile_rgs, cfg.scan_tile_rgs_set);
    const scan_coalesce_tiles = cfg.scan_coalesce_tiles;
    const route_block_rows = chooseRouteBlockRows(bucket_count, cfg.route_block_rows, cfg.route_block_rows_set);
    const direct_final_local = bucket_count >= n_workers * 8;
    const local_reserve_per_bucket = localReservePerBucket(total, n_workers, bucket_count, chunk_rows, route_block_rows);

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

    const scan_columns = if (cfg.scan_filter and cfg.kind.hasFilter() and table.memtable.row_count == 0)
        cfg.kind.payloadColumns()
    else
        cfg.kind.columns();
    var stats_scan = try Scan.allocWithProjectionLoc(table.allocator, table, null, scan_columns, false, snap);
    defer stats_scan.deinit();
    const expected_groups_per_bucket = expectedGroupsPerBucket(total, bucket_count, stats_scan.stats(), cfg.kind);

    if (!cfg.quiet) {
        std.debug.print(
            "[clientip] query={s} DOP={d} buckets={d} rows={d} mode=silo-grid workers={d} chunk_rows={d} scan_tile_rgs={d} scan_coalesce_tiles={d} route_block_rows={d} group_lease_buckets={d} group_lease_rows={d} local_reserve_per_bucket={d} expected_groups_per_bucket={d} direct_final_local={s} scheduler=group_rows_vs_scan_buffer_rows\n",
            .{ cfg.kind.label(), dop, bucket_count, total, n_workers, chunk_rows, scan_tile_rgs, scan_coalesce_tiles, route_block_rows, cfg.group_lease_buckets, cfg.group_lease_rows, local_reserve_per_bucket, expected_groups_per_bucket, if (direct_final_local) "true" else "false" },
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
        .total_scan_rgs = total_rgs,
        .local_reserve_per_bucket = local_reserve_per_bucket,
        .route_block_rows = route_block_rows,
        .direct_final_local = direct_final_local,
        .local_parts = parts,
    };
    var i: usize = 0;
    while (i < n_workers) : (i += 1) {
        parts[i] = try WorkerParts.init(allocator, bucket_count, local_reserve_per_bucket);
        parts[i].worker_index = i;
        built_parts += 1;
        scans[i] = try Scan.allocWithProjectionLoc(table.allocator, table, null, scan_columns, false, snap);
        if (cfg.scan_filter) _ = try applyScanFilter(scans[i], cfg.kind);
        built_scans += 1;
        scans[i].setRange(0, 0, 0, 0, false);
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
    var sched_loops: u64 = 0;
    var sched_scan_jobs: u64 = 0;
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
        sched_loops += p.sched_loops;
        sched_scan_jobs += p.sched_scan_jobs;
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
            "[clientip-silo-grid-stages] query={s} scan_decode_cpu={d:.1}ms scan_reset_cpu={d:.3}ms route_partition_cpu={d:.1}ms publish_queue_cpu={d:.1}ms aggregate_cpu={d:.1}ms idle_cpu={d:.1}ms local_topn_cpu={d:.1}ms final_merge_wall={d:.3}ms chunks={d} rows_per_chunk={d:.1} scan_ranges={d} scan_quanta={d} scan_batches={d} segments_opened={d} fused_scans={d}/{d} scan_tile_rgs={d} scan_coalesce_tiles={d} route_block_rows={d} group_lease_buckets={d} group_lease_rows={d} final_group_queued_rows={d} final_scan_buffered_rows={d} active_scan_jobs={d} active_group_jobs={d}\n",
            .{
                cfg.kind.label(),
                ticksToMs(scan_cpu_ticks, freq),
                ticksToMs(scan_reset_ticks, freq),
                ticksToMs(partition_cpu_ticks, freq),
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
                shared.scan_buffered_rows.load(.acquire),
                shared.active_scan_jobs.load(.acquire),
                shared.active_group_jobs.load(.acquire),
            },
        );
        const sched_cpu_ticks = sched_decision_ticks + sched_scan_claim_ticks + sched_group_pick_ticks + sched_group_lock_ticks;
        std.debug.print(
            "[clientip-silo-grid-scheduler] query={s} scheduler_cpu={d:.3}ms decision={d:.3}ms scan_claim={d:.3}ms group_pick={d:.3}ms group_lock={d:.3}ms loops={d} scan_jobs={d} group_jobs={d} group_misses={d} idle_loops={d}\n",
            .{
                cfg.kind.label(),
                ticksToMs(sched_cpu_ticks, freq),
                ticksToMs(sched_decision_ticks, freq),
                ticksToMs(sched_scan_claim_ticks, freq),
                ticksToMs(sched_group_pick_ticks, freq),
                ticksToMs(sched_group_lock_ticks, freq),
                sched_loops,
                sched_scan_jobs,
                sched_group_jobs,
                sched_group_misses,
                sched_idle_loops,
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
