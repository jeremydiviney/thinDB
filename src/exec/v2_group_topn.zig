//! Engine V2 grouped Top-N pipeline.
//!
//! This is the first production-shaped port of the isolated ClickBench harness:
//! workers float between scan slots and group-bucket slots, scan output is
//! routed into local bucket buffers, full buffers are published to shared group
//! buckets, and group workers lease buckets so exactly one worker mutates a
//! bucket's aggregate table at a time.

const std = @import("std");
const Allocator = std.mem.Allocator;

const api = @import("../api/api.zig");
const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const store = @import("../engine/store.zig");
const ColumnStore = store.ColumnStore;

const exec = @import("exec.zig");
const Batch = exec.Batch;
const Query = exec.Query;
const PredicateExpr = exec.PredicateExpr;
const SortSpec = exec.SortSpec;
const AggSpec = exec.AggSpec;
const Scan = exec.Scan;
const GroupTable = exec.group_table.IntKeyTable(96);
const core_scheduler = @import("../util/core_scheduler.zig");
const aggregate = @import("aggregate.zig");

const TOP_MAX: usize = 4096;
const DEFAULT_BUCKET_COUNT: usize = 2028;
const DEFAULT_CHUNK_ROWS: usize = 1024;
const DEFAULT_LOCAL_BUCKET_RESERVE_ROWS: usize = 2048;
const DEFAULT_SCAN_TILE_RGS: usize = 16;
const DEFAULT_ROUTE_BLOCK_ROWS: usize = 2048;
const DEFAULT_GROUP_LEASE_BUCKETS: usize = 8;
const MAX_GROUP_LEASE_BUCKETS: usize = 64;
const MAX_ROUTE_BLOCK_ROWS: usize = 2048;
const PREFETCH_DIST_BUCKET: usize = 32;

pub const Spec = struct {
    group_cols: []const []const u8,
    aggs: []const AggSpec,
    order_specs: []const SortSpec,
    limit: usize,
    offset: usize,
    where_filter: ?PredicateExpr,
    needed: ?[]const []const u8,
    dop: usize,
};

const KeyPart = struct {
    name: []const u8,
    typ: Type,
    offset_bits: u8,
    width_bits: u8,
};

const KeyLayout = struct {
    parts: [2]KeyPart,
    total_bits: u8,
};

const Accepted = struct {
    layout: KeyLayout,
    sum_col: []const u8,
    sum_type: Type,
    sum_out_type: Type,
    avg_col: []const u8,
    avg_type: Type,
    avg_out_type: Type,
    count_name: []const u8,
    sum_name: []const u8,
    avg_name: []const u8,
};

pub fn tryCreate(allocator: Allocator, table: *api.Table, spec: Spec) !?Query {
    const accepted = validateShape(table, spec) orelse return null;
    traceAccepted(spec);
    const self = try allocator.create(V2GroupTopN);
    errdefer allocator.destroy(self);
    self.* = try V2GroupTopN.init(allocator, table, spec, accepted);
    return exec.makeQuery(allocator, self);
}

const V2GroupTopN = struct {
    allocator: Allocator,
    table: *api.Table,
    spec: Spec,
    accepted: Accepted,
    output_schema: []Column,
    output_cols: []ColumnStore,
    views: []ColumnView,
    owned_needed: ?[]const []const u8 = null,
    emitted: bool = false,
    built: bool = false,
    row_count: usize = 0,

    fn init(allocator: Allocator, table: *api.Table, spec: Spec, accepted: Accepted) !V2GroupTopN {
        const owned_needed = if (spec.needed) |needed| try allocator.dupe([]const u8, needed) else null;
        errdefer if (owned_needed) |n| allocator.free(n);
        var owned_spec = spec;
        owned_spec.needed = owned_needed;

        const output_schema = try allocator.alloc(Column, spec.group_cols.len + spec.aggs.len);
        errdefer allocator.free(output_schema);

        for (accepted.layout.parts, 0..) |part, i| {
            output_schema[i] = .{ .name = part.name, .type = part.typ };
        }
        output_schema[2] = .{ .name = accepted.count_name, .type = .bigint };
        output_schema[3] = .{ .name = accepted.sum_name, .type = accepted.sum_out_type };
        output_schema[4] = .{ .name = accepted.avg_name, .type = accepted.avg_out_type };

        const output_cols = try allocator.alloc(ColumnStore, output_schema.len);
        errdefer allocator.free(output_cols);
        var built_cols: usize = 0;
        errdefer for (output_cols[0..built_cols]) |*c| c.deinit(allocator);
        for (output_schema, 0..) |col, i| {
            output_cols[i] = try ColumnStore.initCapacity(allocator, col.type, col.nullable, spec.limit, 0);
            built_cols += 1;
        }
        const views = try allocator.alloc(ColumnView, output_schema.len);
        errdefer allocator.free(views);

        return .{
            .allocator = allocator,
            .table = table,
            .spec = owned_spec,
            .accepted = accepted,
            .output_schema = output_schema,
            .output_cols = output_cols,
            .views = views,
            .owned_needed = owned_needed,
        };
    }

    pub fn deinit(self: *V2GroupTopN) void {
        for (self.output_cols) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_cols);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        if (self.owned_needed) |n| self.allocator.free(n);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *V2GroupTopN) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(_: *V2GroupTopN, _: exec.Predicate) !void {}

    pub fn stats(self: *V2GroupTopN) exec.PipelineStats {
        return .{ .upper_rows = self.spec.limit };
    }

    pub fn accountant(_: *V2GroupTopN) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(_: *V2GroupTopN, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainIndent(out, allocator, depth);
        try out.appendSlice(allocator, "V2GroupTopN(silo-grid scheduler)\n");
    }

    pub fn next(self: *V2GroupTopN) !?Batch {
        const trace_timing = getenv("THINDB_V2_HARNESS_TIMING") != null;
        const next_t0 = if (trace_timing) exec.prof.nowTicks() else 0;
        if (self.emitted) {
            if (trace_timing) std.debug.print("[v2-harness-timing] next_null={d:.3}ms\n", .{exec.prof.ticksToMs(exec.prof.nowTicks() - next_t0)});
            return null;
        }
        if (!self.built) {
            try self.run();
            self.built = true;
        }
        self.emitted = true;
        for (self.output_cols, 0..) |c, i| self.views[i] = c.view();
        if (trace_timing) std.debug.print("[v2-harness-timing] next_first_total={d:.1}ms rows={d}\n", .{
            exec.prof.ticksToMs(exec.prof.nowTicks() - next_t0),
            self.row_count,
        });
        return .{ .schema = self.output_schema, .values = self.views, .row_count = self.row_count };
    }

    fn run(self: *V2GroupTopN) !void {
        const profile_enabled = getenv("THINDB_V2_PROFILE") != null;
        const run_t0 = if (profile_enabled) exec.prof.nowTicks() else 0;
        const dop = @max(@as(usize, 1), self.spec.dop);
        if (getenv("THINDB_V2_RUN_HARNESS_CORE") != null and dop > 1) {
            try self.runHarnessCoreDiagnostic(dop);
        }
        if (getenv("THINDB_V2_USE_HARNESS_CORE") != null and dop > 1) {
            const trace_timing = getenv("THINDB_V2_HARNESS_TIMING") != null;
            const harness_t0 = if (trace_timing) exec.prof.nowTicks() else 0;
            var top = try self.runHarnessCoreRows(dop);
            defer top.deinit(self.allocator);
            const emit_t0 = if (trace_timing) exec.prof.nowTicks() else 0;
            try self.emitHarnessTop(top.items);
            if (trace_timing) std.debug.print("[v2-harness-timing] run_total_before_defer={d:.1}ms emit_top={d:.3}ms top_rows={d}\n", .{
                exec.prof.ticksToMs(exec.prof.nowTicks() - harness_t0),
                exec.prof.ticksToMs(exec.prof.nowTicks() - emit_t0),
                top.items.len,
            });
            if (profile_enabled) std.debug.print("[v2prof] run wrapper total={d:.2}ms harness_core_result=true\n", .{
                exec.prof.ticksToMs(exec.prof.nowTicks() - run_t0),
            });
            return;
        }
        if (dop == 1) {
            var top = try runSerial(self);
            defer top.deinit(self.allocator);
            const emit_t0 = if (profile_enabled) exec.prof.nowTicks() else 0;
            try self.emitTop(top.items);
            if (profile_enabled) std.debug.print("[v2prof] run wrapper total={d:.2}ms emit_top={d:.2}ms\n", .{
                exec.prof.ticksToMs(exec.prof.nowTicks() - run_t0),
                exec.prof.ticksToMs(exec.prof.nowTicks() - emit_t0),
            });
            return;
        }
        var top = try runSiloGrid(self, dop);
        defer top.deinit(self.allocator);
        const emit_t0 = if (profile_enabled) exec.prof.nowTicks() else 0;
        try self.emitTop(top.items);
        if (profile_enabled) std.debug.print("[v2prof] run wrapper total={d:.2}ms emit_top={d:.2}ms\n", .{
            exec.prof.ticksToMs(exec.prof.nowTicks() - run_t0),
            exec.prof.ticksToMs(exec.prof.nowTicks() - emit_t0),
        });
    }

    const HarnessTopRows = struct {
        items: []exec.group_topn_harness_core.TopRow = &.{},

        fn deinit(self: *HarnessTopRows, allocator: Allocator) void {
            if (self.items.len > 0) allocator.free(self.items);
            self.* = .{};
        }
    };

    fn runHarnessCoreRows(self: *V2GroupTopN, dop: usize) !HarnessTopRows {
        const core = exec.group_topn_harness_core;
        const kind = self.harnessKind() orelse return error.UnsupportedOperatorForType;
        const trace_timing = getenv("THINDB_V2_HARNESS_TIMING") != null;
        const total_t0 = if (trace_timing) exec.prof.nowTicks() else 0;
        const layout_t0 = if (trace_timing) exec.prof.nowTicks() else 0;
        var layout = try core.cpuLayout(self.allocator);
        defer layout.deinit(self.allocator);
        const layout_ticks = if (trace_timing) exec.prof.nowTicks() - layout_t0 else 0;
        const n = @min(dop, layout.order.len);
        if (n == 0) return .{};
        var rows: std.ArrayListUnmanaged(core.TopRow) = .empty;
        errdefer rows.deinit(self.allocator);
        const core_t0 = if (trace_timing) exec.prof.nowTicks() else 0;
        try core.runSiloGrid(self.allocator, self.table, layout.order[0..n], .{
            .dop = dop,
            .bucket_count = DEFAULT_BUCKET_COUNT,
            .kind = kind,
            .silo_grid = true,
            .scan_filter = true,
            .chunk_rows = DEFAULT_CHUNK_ROWS,
            .chunk_rows_set = true,
            .scan_tile_rgs = DEFAULT_SCAN_TILE_RGS,
            .scan_tile_rgs_set = true,
            .route_block_rows = DEFAULT_ROUTE_BLOCK_ROWS,
            .route_block_rows_set = true,
            .group_lease_buckets = DEFAULT_GROUP_LEASE_BUCKETS,
            .no_profile = true,
            .quiet = true,
            .result_out = &rows,
            .trace_timing = trace_timing,
        });
        const core_ticks = if (trace_timing) exec.prof.nowTicks() - core_t0 else 0;
        const copy_t0 = if (trace_timing) exec.prof.nowTicks() else 0;
        const owned = try rows.toOwnedSlice(self.allocator);
        if (trace_timing) std.debug.print("[v2-harness-timing] core_rows total={d:.1}ms cpu_layout={d:.3}ms core_call_full={d:.1}ms result_copy={d:.3}ms rows={d}\n", .{
            exec.prof.ticksToMs(exec.prof.nowTicks() - total_t0),
            exec.prof.ticksToMs(layout_ticks),
            exec.prof.ticksToMs(core_ticks),
            exec.prof.ticksToMs(exec.prof.nowTicks() - copy_t0),
            owned.len,
        });
        return .{ .items = owned };
    }

    fn runHarnessCoreDiagnostic(self: *V2GroupTopN, dop: usize) !void {
        const core = exec.group_topn_harness_core;
        const kind = self.harnessKind() orelse return;
        var layout = try core.cpuLayout(self.allocator);
        defer layout.deinit(self.allocator);
        const n = @min(dop, layout.order.len);
        if (n == 0) return;
        try core.runSiloGrid(self.allocator, self.table, layout.order[0..n], .{
            .dop = dop,
            .bucket_count = DEFAULT_BUCKET_COUNT,
            .kind = kind,
            .silo_grid = true,
            .scan_filter = true,
            .chunk_rows = DEFAULT_CHUNK_ROWS,
            .chunk_rows_set = true,
            .scan_tile_rgs = DEFAULT_SCAN_TILE_RGS,
            .scan_tile_rgs_set = true,
            .route_block_rows = DEFAULT_ROUTE_BLOCK_ROWS,
            .route_block_rows_set = true,
            .group_lease_buckets = DEFAULT_GROUP_LEASE_BUCKETS,
            .no_profile = false,
            .quiet = false,
        });
    }

    fn harnessKind(self: *const V2GroupTopN) ?exec.group_topn_harness_core.QueryKind {
        const core = exec.group_topn_harness_core;
        if (self.accepted.layout.parts.len != 2) return null;
        const a = self.accepted.layout.parts[0].name;
        const b = self.accepted.layout.parts[1].name;
        const has_filter = self.spec.where_filter != null;
        if (types.columnNameEql(a, "SearchEngineID") and types.columnNameEql(b, "ClientIP") and has_filter) return core.QueryKind.q30;
        if (types.columnNameEql(a, "WatchID") and types.columnNameEql(b, "ClientIP") and has_filter) return core.QueryKind.q31;
        if (types.columnNameEql(a, "WatchID") and types.columnNameEql(b, "ClientIP") and !has_filter) return core.QueryKind.q32;
        return null;
    }

    fn emitHarnessTop(self: *V2GroupTopN, rows: []const exec.group_topn_harness_core.TopRow) !void {
        const start = @min(self.spec.offset, rows.len);
        const end = @min(rows.len, start + self.spec.limit);
        for (rows[start..end]) |row| {
            try appendKeyPart(self.allocator, &self.output_cols[0], self.accepted.layout.parts[0], row.key);
            try appendKeyPart(self.allocator, &self.output_cols[1], self.accepted.layout.parts[1], row.key);
            try self.output_cols[2].data.bigint.append(self.allocator, @intCast(row.count));
            try appendSum(self.allocator, &self.output_cols[3], self.accepted.sum_out_type, row.refresh_sum);
            try self.output_cols[4].data.double.append(
                self.allocator,
                if (row.count == 0) 0.0 else @as(f64, @floatFromInt(row.width_sum)) / @as(f64, @floatFromInt(row.count)),
            );
            self.row_count += 1;
        }
    }

    fn emitTop(self: *V2GroupTopN, rows: []TopRow) !void {
        const start = @min(self.spec.offset, rows.len);
        const end = @min(rows.len, start + self.spec.limit);
        for (rows[start..end]) |row| {
            try appendKeyPart(self.allocator, &self.output_cols[0], self.accepted.layout.parts[0], row.key);
            try appendKeyPart(self.allocator, &self.output_cols[1], self.accepted.layout.parts[1], row.key);
            try self.output_cols[2].data.bigint.append(self.allocator, @intCast(row.count));
            try appendSum(self.allocator, &self.output_cols[3], self.accepted.sum_out_type, row.sum_total);
            try self.output_cols[4].data.double.append(
                self.allocator,
                if (row.count == 0) 0.0 else @as(f64, @floatFromInt(row.avg_total)) / @as(f64, @floatFromInt(row.count)),
            );
            self.row_count += 1;
        }
    }
};

const Row = extern struct {
    key_lo: u64,
    key_hi: u32,
    sum_val: i16,
    avg_val: i16,
};

inline fn makeRow(key: u128, sum_val: i64, avg_val: i64) Row {
    return .{
        .key_lo = @truncate(key),
        .key_hi = @truncate(key >> 64),
        .sum_val = @intCast(sum_val),
        .avg_val = @intCast(avg_val),
    };
}

inline fn rowKey(row: Row) u128 {
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
    dirty_buckets: std.ArrayListUnmanaged(usize) = .empty,
    dirty_marks: []bool = &.{},
    recycled_rows: std.ArrayListUnmanaged(std.ArrayListUnmanaged(Row)) = .empty,
    recycle_lock: std.atomic.Mutex = .unlocked,
    worker_index: usize = 0,
    scanned_count: u64 = 0,
    row_count: u64 = 0,
    local_buffered_rows: u64 = 0,

    fn init(allocator: Allocator, bucket_count: usize, reserve_per_bucket: usize) !WorkerParts {
        const buckets = try allocator.alloc(PartBucket, bucket_count);
        errdefer allocator.free(buckets);
        for (buckets) |*b| {
            b.* = .{};
            if (reserve_per_bucket > 0) try b.rows.ensureTotalCapacity(allocator, reserve_per_bucket);
        }
        const dirty_marks = try allocator.alloc(bool, bucket_count);
        errdefer allocator.free(dirty_marks);
        @memset(dirty_marks, false);
        return .{ .buckets = buckets, .dirty_marks = dirty_marks };
    }

    fn deinit(self: *WorkerParts, allocator: Allocator) void {
        for (self.buckets) |*b| b.deinit(allocator);
        if (self.buckets.len > 0) allocator.free(self.buckets);
        self.dirty_buckets.deinit(allocator);
        if (self.dirty_marks.len > 0) allocator.free(self.dirty_marks);
        for (self.recycled_rows.items) |*rows| rows.deinit(allocator);
        self.recycled_rows.deinit(allocator);
        self.* = .{};
    }
};

const State = struct {
    key: u128,
    count: u64,
    sum_total: i64,
    avg_total: i64,
};

const GroupScratch = struct {
    gids: std.ArrayListUnmanaged(u32) = .empty,

    fn deinit(self: *GroupScratch, allocator: Allocator) void {
        self.gids.deinit(allocator);
        self.* = .{};
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
    table: ?GroupTable = null,
    expected_groups: usize = 16,
    states: std.ArrayListUnmanaged(State) = .empty,
    row_count: u64 = 0,

    fn init(allocator: Allocator, expected_groups: usize) !PipeBucket {
        _ = allocator;
        return .{ .expected_groups = expected_groups };
    }

    fn deinit(self: *PipeBucket, allocator: Allocator) void {
        for (self.chunks.items) |*chunk| chunk.rows.deinit(allocator);
        self.chunks.deinit(allocator);
        if (self.table) |*table| table.deinit(allocator);
        self.states.deinit(allocator);
        self.* = undefined;
    }
};

const PipeShared = struct {
    allocator: Allocator,
    buckets: []PipeBucket,
    bucket_count: usize,
    scan_threads: usize,
    profile: ?*V2Profile = null,
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
    route_block_rows: usize = DEFAULT_ROUTE_BLOCK_ROWS,
    direct_final_local: bool = false,
    local_parts: []WorkerParts = &.{},
};

const V2Profile = struct {
    scan_tile_ticks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    append_ticks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    publish_ticks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    flush_ticks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    drain_select_ticks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    drain_total_ticks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    group_rows_ticks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    collect_ticks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    final_top_ticks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    scan_jobs: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    scan_batches: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    scan_rows: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    routed_rows: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    published_chunks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    published_rows: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    group_jobs: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    grouped_chunks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    grouped_rows: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    empty_group_tries: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    idle_yields: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

const WorkerProfile = struct {
    scan_tile_ticks: u64 = 0,
    append_ticks: u64 = 0,
    publish_ticks: u64 = 0,
    flush_ticks: u64 = 0,
    drain_total_ticks: u64 = 0,
    drain_select_ticks: u64 = 0,
    group_rows_ticks: u64 = 0,
    collect_ticks: u64 = 0,
    scan_jobs: u64 = 0,
    scan_rows: u64 = 0,
    routed_rows: u64 = 0,
    published_rows: u64 = 0,
    grouped_rows: u64 = 0,
    group_jobs: u64 = 0,
    idle_yields: u64 = 0,
};

const TopRow = struct {
    key: u128,
    count: u64,
    sum_total: i64,
    avg_total: i64,
};

const TopRows = struct {
    items: []TopRow = &.{},

    fn deinit(self: *TopRows, allocator: Allocator) void {
        if (self.items.len > 0) allocator.free(self.items);
        self.* = .{};
    }
};

const TopSet = struct {
    items: []TopRow,
    len: usize = 0,
    worst_i: usize = 0,

    fn init(allocator: Allocator, cap: usize) !TopSet {
        return .{ .items = try allocator.alloc(TopRow, cap) };
    }

    fn deinit(self: *TopSet, allocator: Allocator) void {
        allocator.free(self.items);
        self.* = undefined;
    }

    fn recomputeWorst(self: *TopSet) void {
        var w: usize = 0;
        var i: usize = 1;
        while (i < self.len) : (i += 1) {
            if (worse(self.items[i], self.items[w])) w = i;
        }
        self.worst_i = w;
    }

    fn consider(self: *TopSet, cand: TopRow) void {
        if (self.len < self.items.len) {
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

fn better(a: TopRow, b: TopRow) bool {
    if (a.count != b.count) return a.count > b.count;
    return a.key < b.key;
}

fn worse(a: TopRow, b: TopRow) bool {
    if (a.count != b.count) return a.count < b.count;
    return a.key > b.key;
}

fn topLess(_: void, a: TopRow, b: TopRow) bool {
    return better(a, b);
}

fn runSerial(self: *V2GroupTopN) !TopRows {
    var scan = try Scan.allocWithProjectionLoc(self.table.allocator, self.table, null, self.spec.needed, false, null);
    defer scan.deinit();
    if (self.spec.where_filter) |pred| _ = try scan.tryFuseFilter(pred);

    var table = try GroupTable.init(self.allocator, @max(@as(usize, 16), totalRows(self.table) / 8));
    defer table.deinit(self.allocator);
    var states: std.ArrayListUnmanaged(State) = .empty;
    defer states.deinit(self.allocator);
    var scratch: GroupScratch = .{};
    defer scratch.deinit(self.allocator);

    while (try scan.next()) |batch| {
        try appendBatchDirect(self.allocator, self.accepted, &table, &states, &scratch, batch, self.spec.where_filter == null or scan.fusedActive());
    }

    return topFromStates(self.allocator, states.items, self.spec.limit + self.spec.offset);
}

fn runSiloGrid(self: *V2GroupTopN, dop: usize) !TopRows {
    const profile_enabled = getenv("THINDB_V2_PROFILE") != null;
    const wall_t0 = if (profile_enabled) exec.prof.nowTicks() else 0;
    var setup_snapshot_ticks: i64 = 0;
    var setup_bucket_ticks: i64 = 0;
    var setup_worker_ticks: i64 = 0;
    var worker_wall_ticks: i64 = 0;
    const worker_count = @max(@as(usize, 1), dop);
    const bucket_count = DEFAULT_BUCKET_COUNT;
    const chunk_rows = DEFAULT_CHUNK_ROWS;
    const route_block_rows = DEFAULT_ROUTE_BLOCK_ROWS;
    const scan_tile_rgs = DEFAULT_SCAN_TILE_RGS;
    const group_lease_buckets = DEFAULT_GROUP_LEASE_BUCKETS;
    const total = totalRows(self.table);
    const direct_final_local = bucket_count >= worker_count * 8;

    const snapshot_t0 = if (profile_enabled) exec.prof.nowTicks() else 0;
    self.table.ddl_lock.lockSharedUncancelable(self.table.io);
    defer self.table.ddl_lock.unlockShared(self.table.io);

    const snap = Scan.captureSnapshot(self.table);
    var pin_held = true;
    defer if (pin_held) snap.memtable_snap.release();

    const seg_start = try self.allocator.alloc(usize, snap.segment_count + 1);
    defer self.allocator.free(seg_start);
    var total_rgs: usize = 0;
    for (self.table.manifest.segments.items[0..snap.segment_count], 0..) |entry, i| {
        seg_start[i] = total_rgs;
        total_rgs += entry.row_group_count;
    }
    seg_start[snap.segment_count] = total_rgs;

    var stats_scan = try Scan.allocWithProjectionLoc(self.table.allocator, self.table, null, self.spec.needed, false, snap);
    defer stats_scan.deinit();
    const expected_groups_per_bucket = expectedGroupsPerBucket(total, bucket_count, stats_scan.outputSchema(), stats_scan.stats(), self.spec);
    const local_reserve_per_bucket = localReservePerBucket(total, total_rgs, scan_tile_rgs, bucket_count, chunk_rows);
    if (profile_enabled) setup_snapshot_ticks = exec.prof.nowTicks() - snapshot_t0;

    var scans = try self.allocator.alloc(*Scan, worker_count);
    defer self.allocator.free(scans);
    var built_scans: usize = 0;
    defer for (scans[0..built_scans]) |s| s.deinit();

    var parts = try self.allocator.alloc(WorkerParts, worker_count);
    defer self.allocator.free(parts);
    var built_parts: usize = 0;
    defer for (parts[0..built_parts]) |*p| p.deinit(self.allocator);

    var buckets = try self.allocator.alloc(PipeBucket, bucket_count);
    defer self.allocator.free(buckets);
    var built_buckets: usize = 0;
    defer for (buckets[0..built_buckets]) |*b| b.deinit(self.allocator);
    const bucket_t0 = if (profile_enabled) exec.prof.nowTicks() else 0;
    for (buckets) |*b| {
        b.* = try PipeBucket.init(self.allocator, expected_groups_per_bucket);
        built_buckets += 1;
        try b.chunks.ensureTotalCapacity(self.allocator, 8);
    }
    if (profile_enabled) setup_bucket_ticks = exec.prof.nowTicks() - bucket_t0;

    var profile: V2Profile = .{};
    var worker_profiles = try self.allocator.alloc(WorkerProfile, worker_count);
    defer self.allocator.free(worker_profiles);
    @memset(worker_profiles, .{});
    var shared = PipeShared{
        .allocator = self.allocator,
        .buckets = buckets,
        .bucket_count = bucket_count,
        .scan_threads = worker_count,
        .profile = if (profile_enabled) &profile else null,
        .total_scan_rgs = total_rgs,
        .local_reserve_per_bucket = local_reserve_per_bucket,
        .route_block_rows = route_block_rows,
        .direct_final_local = direct_final_local,
        .local_parts = parts,
    };

    const worker_setup_t0 = if (profile_enabled) exec.prof.nowTicks() else 0;
    for (0..worker_count) |i| {
        parts[i] = try WorkerParts.init(self.allocator, bucket_count, local_reserve_per_bucket);
        parts[i].worker_index = i;
        built_parts += 1;
        scans[i] = try Scan.allocWithProjectionLoc(self.table.allocator, self.table, null, self.spec.needed, false, snap);
        if (self.spec.where_filter) |pred| _ = try scans[i].tryFuseFilter(pred);
        scans[i].setRange(0, 0, 0, 0, false);
        built_scans += 1;
    }
    if (profile_enabled) setup_worker_ticks = exec.prof.nowTicks() - worker_setup_t0;
    snap.memtable_snap.release();
    pin_held = false;

    var tops = try self.allocator.alloc(TopSet, worker_count);
    defer {
        for (tops) |*t| t.deinit(self.allocator);
        self.allocator.free(tops);
    }
    for (tops) |*t| t.* = try TopSet.init(self.allocator, @min(TOP_MAX, self.spec.limit + self.spec.offset));

    var threads = try self.allocator.alloc(std.Thread, worker_count);
    defer self.allocator.free(threads);
    var errs = try self.allocator.alloc(?anyerror, worker_count);
    defer self.allocator.free(errs);
    @memset(errs, null);

    const worker_wall_t0 = if (profile_enabled) exec.prof.nowTicks() else 0;
    for (0..worker_count) |i| {
        threads[i] = try std.Thread.spawn(.{}, workerMain, .{WorkerJob{
            .scan = scans[i],
            .local = &parts[i],
            .shared = &shared,
            .seg_start = seg_start,
            .segment_count = snap.segment_count,
            .worker_index = i,
            .worker_count = worker_count,
            .accepted = self.accepted,
            .scan_tile_rgs = scan_tile_rgs,
            .chunk_rows = chunk_rows,
            .group_lease_buckets = group_lease_buckets,
            .filter_fused = scans[i].fusedActive(),
            .top = &tops[i],
            .err = &errs[i],
            .profile = if (profile_enabled) &worker_profiles[i] else null,
        }});
    }
    for (threads) |thread| thread.join();
    if (profile_enabled) worker_wall_ticks = exec.prof.nowTicks() - worker_wall_t0;
    for (errs) |maybe_err| if (maybe_err) |err| return err;

    const final_t0 = if (profile_enabled) exec.prof.nowTicks() else 0;
    var final_top = try TopSet.init(self.allocator, @min(TOP_MAX, self.spec.limit + self.spec.offset));
    defer final_top.deinit(self.allocator);
    for (tops) |t| {
        for (t.items[0..t.len]) |candidate| final_top.consider(candidate);
    }
    std.mem.sort(TopRow, final_top.items[0..final_top.len], {}, topLess);
    const out = try self.allocator.dupe(TopRow, final_top.items[0..final_top.len]);
    if (profile_enabled) {
        addTicks(&profile.final_top_ticks, exec.prof.nowTicks() - final_t0);
        const wall_ticks = exec.prof.nowTicks() - wall_t0;
        reportProfile(
            &profile,
            wall_ticks,
            worker_count,
            bucket_count,
            chunk_rows,
            route_block_rows,
            scan_tile_rgs,
            group_lease_buckets,
            total_rgs,
            setup_snapshot_ticks,
            setup_bucket_ticks,
            setup_worker_ticks,
            worker_wall_ticks,
            worker_profiles,
        );
    }
    return .{ .items = out };
}

const WorkerJob = struct {
    scan: *Scan,
    local: *WorkerParts,
    shared: *PipeShared,
    seg_start: []const usize,
    segment_count: usize,
    worker_index: usize,
    worker_count: usize,
    accepted: Accepted,
    scan_tile_rgs: usize,
    chunk_rows: usize,
    group_lease_buckets: usize,
    filter_fused: bool,
    top: *TopSet,
    err: *?anyerror,
    profile: ?*WorkerProfile,
};

fn workerMain(job: WorkerJob) void {
    var lease = core_scheduler.global().acquire();
    defer lease.release();
    workerMainErr(job) catch |err| {
        job.err.* = err;
    };
}

fn workerMainErr(job: WorkerJob) !void {
    var scratch: GroupScratch = .{};
    defer scratch.deinit(job.shared.allocator);
    var cursor = (job.worker_index * 17) % job.shared.bucket_count;
    var marked_scan_done = false;
    var scan_exhausted = false;
    var idle_spins: usize = 0;

    while (true) {
        const group_queued_rows = job.shared.outstanding_rows.load(.acquire);
        const scan_buffered_rows = job.shared.scan_buffered_rows.load(.acquire);
        const scan_claims_available = !scan_exhausted and job.shared.next_scan_rg.load(.acquire) < job.shared.total_scan_rgs;
        const global_scan_finished = job.shared.next_scan_rg.load(.acquire) >= job.shared.total_scan_rgs and
            job.shared.active_scan_jobs.load(.acquire) == 0;
        if (global_scan_finished and !marked_scan_done) {
            scan_exhausted = true;
            try markScanDone(job, &marked_scan_done);
            idle_spins = 0;
            continue;
        }

        const group_first = group_queued_rows > scan_buffered_rows or !scan_claims_available;
        if (group_first and group_queued_rows > 0) {
            if (try drainLeasedBuckets(job, &scratch, &cursor, job.group_lease_buckets)) {
                idle_spins = 0;
                continue;
            }
        }

        if (scan_claims_available and (!group_first or group_queued_rows == 0)) {
            try runScanTile(job, &scan_exhausted, &marked_scan_done);
            idle_spins = 0;
            continue;
        }

        if (job.shared.scans_done.load(.acquire) == job.shared.scan_threads and
            job.shared.outstanding_chunks.load(.acquire) == 0)
        {
            if (!job.shared.direct_final_local) break;
            if (try drainNextFinalLocalBucket(job, &scratch)) {
                idle_spins = 0;
                continue;
            }
            if (job.shared.next_final_local_bucket.load(.acquire) >= job.shared.bucket_count and
                job.shared.active_group_jobs.load(.acquire) == 0)
            {
                break;
            }
        }

        idle_spins += 1;
        if (idle_spins < 256) {
            std.atomic.spinLoopHint();
        } else {
            if (job.shared.profile) |p| _ = p.idle_yields.fetchAdd(1, .monotonic);
            if (job.profile) |p| p.idle_yields += 1;
            std.Thread.yield() catch {};
            idle_spins = 0;
        }
    }

    if (!marked_scan_done) try markScanDone(job, &marked_scan_done);
    collectOwnedTop(job.shared, job.worker_index, job.worker_count, job.top, job.profile);
}

fn drainNextFinalLocalBucket(job: WorkerJob, scratch: *GroupScratch) !bool {
    const shared = job.shared;
    const prof = shared.profile;
    while (true) {
        _ = shared.active_group_jobs.fetchAdd(1, .release);
        const bucket_idx = shared.next_final_local_bucket.fetchAdd(1, .monotonic);
        if (bucket_idx >= shared.bucket_count) {
            _ = shared.active_group_jobs.fetchSub(1, .release);
            return false;
        }
        defer _ = shared.active_group_jobs.fetchSub(1, .release);

        const bucket = &shared.buckets[bucket_idx];
        lockSpin(&bucket.agg_lock);
        defer bucket.agg_lock.unlock();
        var did_work = false;
        for (shared.local_parts) |*part| {
            const rows = part.buckets[bucket_idx].rows.items;
            if (rows.len == 0) continue;
            did_work = true;

            const group_t0 = if (prof != null) exec.prof.nowTicks() else 0;
            try groupRows(shared.allocator, bucket, scratch, rows);
            if (prof) |p| {
                addTicks(&p.group_rows_ticks, exec.prof.nowTicks() - group_t0);
                _ = p.grouped_chunks.fetchAdd(1, .monotonic);
                _ = p.grouped_rows.fetchAdd(@intCast(rows.len), .monotonic);
            }
            if (job.profile) |p| {
                addLocalTicks(&p.group_rows_ticks, exec.prof.nowTicks() - group_t0);
                p.grouped_rows += @intCast(rows.len);
            }

            const row_count: u64 = @intCast(rows.len);
            if (part.local_buffered_rows >= row_count) part.local_buffered_rows -= row_count;
            _ = shared.scan_buffered_rows.fetchSub(row_count, .release);
            part.buckets[bucket_idx].rows.clearRetainingCapacity();
            if (part.dirty_marks.len > bucket_idx) part.dirty_marks[bucket_idx] = false;
        }
        if (did_work) {
            if (prof) |p| _ = p.group_jobs.fetchAdd(1, .monotonic);
            if (job.profile) |p| p.group_jobs += 1;
            return true;
        }
    }
}

fn runScanTile(job: WorkerJob, scan_exhausted: *bool, marked_scan_done: *bool) !void {
    const prof = job.shared.profile;
    const tile_t0 = if (prof != null) exec.prof.nowTicks() else 0;
    if (prof) |p| _ = p.scan_jobs.fetchAdd(1, .monotonic);
    if (job.profile) |p| p.scan_jobs += 1;
    defer {
        if (prof) |p| addTicks(&p.scan_tile_ticks, exec.prof.nowTicks() - tile_t0);
        if (job.profile) |p| addLocalTicks(&p.scan_tile_ticks, exec.prof.nowTicks() - tile_t0);
    }

    _ = job.shared.active_scan_jobs.fetchAdd(1, .release);
    defer _ = job.shared.active_scan_jobs.fetchSub(1, .release);

    const tile_width = job.scan_tile_rgs;
    const lo = job.shared.next_scan_rg.fetchAdd(tile_width, .monotonic);
    if (lo >= job.shared.total_scan_rgs) {
        scan_exhausted.* = true;
        return;
    }
    const hi = @min(job.shared.total_scan_rgs, lo + tile_width);
    const start = flatToCoord(lo, job.seg_start, job.segment_count, job.shared.total_scan_rgs);
    const end = flatToCoord(hi, job.seg_start, job.segment_count, job.shared.total_scan_rgs);
    job.scan.resetRange(start.seg, start.rg, end.seg, end.rg, hi >= job.shared.total_scan_rgs);

    while (try job.scan.next()) |batch| {
        if (prof) |p| {
            _ = p.scan_batches.fetchAdd(1, .monotonic);
            _ = p.scan_rows.fetchAdd(@intCast(batch.row_count), .monotonic);
        }
        if (job.profile) |p| p.scan_rows += @intCast(batch.row_count);
        const append_t0 = if (prof != null) exec.prof.nowTicks() else 0;
        const before_routed = job.local.row_count;
        try appendBatch(job.local, job.shared, job.accepted, batch, job.filter_fused);
        if (prof) |p| addTicks(&p.append_ticks, exec.prof.nowTicks() - append_t0);
        if (job.profile) |p| {
            addLocalTicks(&p.append_ticks, exec.prof.nowTicks() - append_t0);
            p.routed_rows += @intCast(job.local.row_count - before_routed);
        }
    }

    if (hi >= job.shared.total_scan_rgs) {
        scan_exhausted.* = true;
        try markScanDone(job, marked_scan_done);
    } else {
        const publish_t0 = if (prof != null) exec.prof.nowTicks() else 0;
        try publishFullLocalBuckets(job.shared, job.local, job.chunk_rows);
        if (prof) |p| addTicks(&p.publish_ticks, exec.prof.nowTicks() - publish_t0);
        if (job.profile) |p| addLocalTicks(&p.publish_ticks, exec.prof.nowTicks() - publish_t0);
    }
}

fn markScanDone(job: WorkerJob, marked: *bool) !void {
    if (marked.*) return;
    if (!job.shared.direct_final_local) {
        const flush_t0 = if (job.shared.profile != null) exec.prof.nowTicks() else 0;
        try flushAllLocalBuckets(job.shared, job.local);
        if (job.shared.profile) |p| addTicks(&p.flush_ticks, exec.prof.nowTicks() - flush_t0);
        if (job.profile) |p| addLocalTicks(&p.flush_ticks, exec.prof.nowTicks() - flush_t0);
    }
    _ = job.shared.scans_done.fetchAdd(1, .release);
    marked.* = true;
}

const Coord = struct { seg: usize, rg: usize };

fn flatToCoord(f: usize, seg_start: []const usize, segment_count: usize, total: usize) Coord {
    if (f >= total) return .{ .seg = segment_count, .rg = 0 };
    var seg: usize = 0;
    while (seg + 1 < segment_count and seg_start[seg + 1] <= f) seg += 1;
    return .{ .seg = seg, .rg = f - seg_start[seg] };
}

fn appendBatch(parts: *WorkerParts, shared: *PipeShared, accepted: Accepted, batch: Batch, filter_fused: bool) !void {
    const views = try BatchViews.init(accepted, batch, !filter_fused);
    const before_count = parts.row_count;
    const use_mask = std.math.isPowerOfTwo(shared.bucket_count);
    const bucket_mask: usize = if (use_mask) shared.bucket_count - 1 else 0;
    var block_rows: [MAX_ROUTE_BLOCK_ROWS]Row = undefined;
    var block_buckets: [MAX_ROUTE_BLOCK_ROWS]u16 = undefined;
    var block_len: usize = 0;
    var r: usize = 0;
    while (r < batch.row_count) : (r += 1) {
        parts.scanned_count += 1;
        if (!views.rowPasses(r)) continue;
        const key = try views.keyAt(r);
        const hash = routeHashKey(key);
        const bucket = if (use_mask) @as(usize, @truncate(hash)) & bucket_mask else @as(usize, @truncate(hash % shared.bucket_count));
        block_rows[block_len] = makeRow(key, try views.sumAt(r), try views.avgAt(r));
        block_buckets[block_len] = @intCast(bucket);
        block_len += 1;
        if (block_len == shared.route_block_rows) {
            try flushRouteBlock(parts, shared.allocator, block_rows[0..block_len], block_buckets[0..block_len]);
            block_len = 0;
        }
        parts.row_count += 1;
        parts.local_buffered_rows += 1;
    }
    try flushRouteBlock(parts, shared.allocator, block_rows[0..block_len], block_buckets[0..block_len]);
    const delta = parts.row_count - before_count;
    if (shared.profile) |p| _ = p.routed_rows.fetchAdd(delta, .monotonic);
    _ = shared.scan_buffered_rows.fetchAdd(delta, .release);
}

fn flushRouteBlock(parts: *WorkerParts, allocator: Allocator, rows: []const Row, buckets: []const u16) !void {
    for (rows, buckets) |row, bucket_u16| {
        const bucket: usize = bucket_u16;
        try appendPartitionRow(parts, bucket, allocator, row);
    }
}

fn appendPartitionRow(parts: *WorkerParts, bucket: usize, allocator: Allocator, row: Row) !void {
    var pb = &parts.buckets[bucket];
    try pb.rows.append(allocator, row);
    if (!parts.dirty_marks[bucket]) {
        parts.dirty_marks[bucket] = true;
        try parts.dirty_buckets.append(allocator, bucket);
    }
}

fn publishFullLocalBuckets(shared: *PipeShared, parts: *WorkerParts, chunk_rows: usize) !void {
    var kept: usize = 0;
    for (parts.dirty_buckets.items) |bucket| {
        if (!parts.dirty_marks[bucket]) continue;
        if (parts.buckets[bucket].rows.items.len >= chunk_rows) {
            const rows_to_publish: u64 = @intCast(parts.buckets[bucket].rows.items.len);
            try publishBucket(shared, bucket, parts.worker_index, &parts.buckets[bucket]);
            parts.local_buffered_rows -= rows_to_publish;
            _ = shared.scan_buffered_rows.fetchSub(rows_to_publish, .release);
            parts.dirty_marks[bucket] = false;
        } else {
            parts.dirty_buckets.items[kept] = bucket;
            kept += 1;
        }
    }
    parts.dirty_buckets.items.len = kept;
}

fn flushAllLocalBuckets(shared: *PipeShared, parts: *WorkerParts) !void {
    for (parts.dirty_buckets.items) |bucket| {
        if (!parts.dirty_marks[bucket]) continue;
        if (parts.buckets[bucket].rows.items.len == 0) continue;
        const rows_to_publish: u64 = @intCast(parts.buckets[bucket].rows.items.len);
        try publishBucket(shared, bucket, parts.worker_index, &parts.buckets[bucket]);
        parts.local_buffered_rows -= rows_to_publish;
        _ = shared.scan_buffered_rows.fetchSub(rows_to_publish, .release);
        parts.dirty_marks[bucket] = false;
    }
    parts.dirty_buckets.clearRetainingCapacity();
}

fn publishBucket(shared: *PipeShared, bucket_idx: usize, owner_worker: usize, bucket: *PartBucket) !void {
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
    if (shared.profile) |p| {
        _ = p.published_chunks.fetchAdd(1, .monotonic);
        _ = p.published_rows.fetchAdd(row_count, .monotonic);
    }
}

fn drainLeasedBuckets(job: WorkerJob, scratch: *GroupScratch, cursor: *usize, max_buckets_raw: usize) !bool {
    const shared = job.shared;
    const prof = shared.profile;
    const drain_t0 = if (prof != null) exec.prof.nowTicks() else 0;
    defer {
        if (prof) |p| addTicks(&p.drain_total_ticks, exec.prof.nowTicks() - drain_t0);
        if (job.profile) |p| addLocalTicks(&p.drain_total_ticks, exec.prof.nowTicks() - drain_t0);
    }
    const select_t0 = if (prof != null) exec.prof.nowTicks() else 0;
    const max_buckets = @max(@as(usize, 1), @min(max_buckets_raw, MAX_GROUP_LEASE_BUCKETS));
    var buckets_buf: [MAX_GROUP_LEASE_BUCKETS]usize = undefined;
    var rows_buf: [MAX_GROUP_LEASE_BUCKETS]u64 = undefined;
    var selected: usize = 0;
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

        if (!shared.buckets[b].agg_lock.tryLock()) continue;
        if (selected == max_buckets) {
            shared.buckets[buckets_buf[insert_at]].agg_lock.unlock();
        } else {
            selected += 1;
        }
        buckets_buf[insert_at] = b;
        rows_buf[insert_at] = q;
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

    if (prof) |p| addTicks(&p.drain_select_ticks, exec.prof.nowTicks() - select_t0);
    if (job.profile) |p| addLocalTicks(&p.drain_select_ticks, exec.prof.nowTicks() - select_t0);

    if (selected == 0) {
        if (prof) |p| _ = p.empty_group_tries.fetchAdd(1, .monotonic);
        return false;
    }
    cursor.* = (buckets_buf[selected - 1] + 1) % shared.bucket_count;

    if (prof) |p| _ = p.group_jobs.fetchAdd(1, .monotonic);
    if (job.profile) |p| p.group_jobs += 1;
    _ = shared.active_group_jobs.fetchAdd(1, .release);
    defer _ = shared.active_group_jobs.fetchSub(1, .release);
    var did_work = false;
    for (buckets_buf[0..selected]) |bucket_idx| {
        const bucket = &shared.buckets[bucket_idx];
        while (popChunk(bucket)) |chunk| {
            did_work = true;
            const group_t0 = if (prof != null) exec.prof.nowTicks() else 0;
            try groupRows(shared.allocator, bucket, scratch, chunk.rows.items);
            if (prof) |p| {
                addTicks(&p.group_rows_ticks, exec.prof.nowTicks() - group_t0);
                _ = p.grouped_chunks.fetchAdd(1, .monotonic);
                _ = p.grouped_rows.fetchAdd(@intCast(chunk.rows.items.len), .monotonic);
            }
            if (job.profile) |p| {
                addLocalTicks(&p.group_rows_ticks, exec.prof.nowTicks() - group_t0);
                p.grouped_rows += @intCast(chunk.rows.items.len);
            }
            _ = shared.outstanding_rows.fetchSub(@intCast(chunk.rows.items.len), .release);
            try recycleChunkRows(shared, chunk.owner_worker, chunk.rows);
            _ = shared.outstanding_chunks.fetchSub(1, .release);
        }
        bucket.agg_lock.unlock();
    }
    return did_work;
}

fn queuedBucketRows(bucket: *PipeBucket) ?u64 {
    if (!bucket.queue_lock.tryLock()) return null;
    const rows = bucket.queued_rows;
    bucket.queue_lock.unlock();
    return rows;
}

fn popChunk(bucket: *PipeBucket) ?PipeChunk {
    lockSpin(&bucket.queue_lock);
    defer bucket.queue_lock.unlock();
    const len = bucket.chunks.items.len;
    if (len == 0) return null;
    const chunk = bucket.chunks.items[len - 1];
    bucket.chunks.items.len = len - 1;
    bucket.queued_rows -= chunk.rows.items.len;
    return chunk;
}

fn groupRows(allocator: Allocator, bucket: *PipeBucket, scratch: *GroupScratch, rows: []const Row) !void {
    const n = rows.len;
    if (n == 0) return;
    if (bucket.table == null) {
        bucket.table = try GroupTable.init(allocator, bucket.expected_groups);
        try bucket.states.ensureTotalCapacity(allocator, bucket.expected_groups);
    }
    var table = &bucket.table.?;
    if (table.needsGrow(n)) try table.grow(allocator, n);
    try bucket.states.ensureUnusedCapacity(allocator, n);
    _ = scratch;

    var r: usize = 0;
    while (r < n) : (r += 1) {
        const pf = r + PREFETCH_DIST_BUCKET;
        if (pf < n) {
            const pf_key = rowKey(rows[pf]);
            @prefetch(table.slotAddr(table.bucketOf(GroupTable.hashKey(pf_key))), .{ .rw = .write, .locality = 1 });
        }

        const row = rows[r];
        const key = rowKey(row);
        const probe = table.getOrPut(GroupTable.hashKey(key), key);
        if (!probe.found) {
            const gid: u32 = @intCast(bucket.states.items.len);
            bucket.states.appendAssumeCapacity(.{
                .key = key,
                .count = 1,
                .sum_total = row.sum_val,
                .avg_total = row.avg_val,
            });
            table.commit(probe.slot, key, gid);
            continue;
        }
        var st = &bucket.states.items[probe.gid];
        st.count += 1;
        st.sum_total += row.sum_val;
        st.avg_total += row.avg_val;
    }
    bucket.row_count += n;
}

fn appendBatchDirect(
    allocator: Allocator,
    accepted: Accepted,
    table: *GroupTable,
    states: *std.ArrayListUnmanaged(State),
    scratch: *GroupScratch,
    batch: Batch,
    skip_filter_check: bool,
) !void {
    const views = try BatchViews.init(accepted, batch, !skip_filter_check);
    const n = batch.row_count;
    if (table.needsGrow(n)) try table.grow(allocator, n);
    try states.ensureUnusedCapacity(allocator, n);
    scratch.gids.clearRetainingCapacity();
    try scratch.gids.ensureTotalCapacity(allocator, n);

    var r: usize = 0;
    while (r < n) : (r += 1) {
        if (!views.rowPasses(r)) continue;
        const key = try views.keyAt(r);
        const probe = table.getOrPut(GroupTable.hashKey(key), key);
        const gid = if (probe.found) probe.gid else blk: {
            const new_gid: u32 = @intCast(states.items.len);
            states.appendAssumeCapacity(.{ .key = key, .count = 0, .sum_total = 0, .avg_total = 0 });
            table.commit(probe.slot, key, new_gid);
            break :blk new_gid;
        };
        var st = &states.items[gid];
        st.count += 1;
        st.sum_total += try views.sumAt(r);
        st.avg_total += try views.avgAt(r);
    }
}

fn collectOwnedTop(shared: *PipeShared, worker_index: usize, worker_count: usize, top: *TopSet, worker_profile: ?*WorkerProfile) void {
    const t0 = if (shared.profile != null) exec.prof.nowTicks() else 0;
    defer {
        if (shared.profile) |p| addTicks(&p.collect_ticks, exec.prof.nowTicks() - t0);
        if (worker_profile) |p| addLocalTicks(&p.collect_ticks, exec.prof.nowTicks() - t0);
    }
    var b = worker_index;
    while (b < shared.bucket_count) : (b += worker_count) {
        for (shared.buckets[b].states.items) |s| {
            top.consider(.{
                .key = s.key,
                .count = s.count,
                .sum_total = s.sum_total,
                .avg_total = s.avg_total,
            });
        }
    }
}

fn topFromBuckets(allocator: Allocator, buckets: []const PipeBucket, cap_raw: usize) !TopRows {
    var top = try TopSet.init(allocator, @min(TOP_MAX, cap_raw));
    defer top.deinit(allocator);
    for (buckets) |bucket| {
        for (bucket.states.items) |s| {
            top.consider(.{ .key = s.key, .count = s.count, .sum_total = s.sum_total, .avg_total = s.avg_total });
        }
    }
    std.mem.sort(TopRow, top.items[0..top.len], {}, topLess);
    return .{ .items = try allocator.dupe(TopRow, top.items[0..top.len]) };
}

fn topFromStates(allocator: Allocator, states: []const State, cap_raw: usize) !TopRows {
    var top = try TopSet.init(allocator, @min(TOP_MAX, cap_raw));
    defer top.deinit(allocator);
    for (states) |s| {
        top.consider(.{ .key = s.key, .count = s.count, .sum_total = s.sum_total, .avg_total = s.avg_total });
    }
    std.mem.sort(TopRow, top.items[0..top.len], {}, topLess);
    return .{ .items = try allocator.dupe(TopRow, top.items[0..top.len]) };
}

const BatchViews = struct {
    accepted: Accepted,
    key_views: [2]ColumnView,
    sum_view: ColumnView,
    avg_view: ColumnView,
    phrase: ?storage.StringView = null,

    fn init(accepted: Accepted, batch: Batch, need_phrase: bool) !BatchViews {
        var out = BatchViews{
            .accepted = accepted,
            .key_views = .{
                batch.columnView(accepted.layout.parts[0].name) orelse return error.ColumnNotFound,
                batch.columnView(accepted.layout.parts[1].name) orelse return error.ColumnNotFound,
            },
            .sum_view = batch.columnView(accepted.sum_col) orelse return error.ColumnNotFound,
            .avg_view = batch.columnView(accepted.avg_col) orelse return error.ColumnNotFound,
        };
        if (need_phrase) {
            if (batch.columnView("SearchPhrase")) |v| {
                out.phrase = switch (v.data) {
                    .varchar, .string, .char => |s| s,
                    else => return error.TypeMismatch,
                };
            }
        }
        return out;
    }

    inline fn rowPasses(self: BatchViews, row: usize) bool {
        return if (self.phrase) |p| p.offsets[row] != p.offsets[row + 1] else true;
    }

    inline fn keyAt(self: BatchViews, row: usize) !u128 {
        const a = try readIntBits(self.key_views[0], self.accepted.layout.parts[0].typ, row);
        const b = try readIntBits(self.key_views[1], self.accepted.layout.parts[1].typ, row);
        return (a << @intCast(self.accepted.layout.parts[0].offset_bits)) |
            (b << @intCast(self.accepted.layout.parts[1].offset_bits));
    }

    inline fn sumAt(self: BatchViews, row: usize) !i64 {
        return readIntValue(self.sum_view, self.accepted.sum_type, row);
    }

    inline fn avgAt(self: BatchViews, row: usize) !i64 {
        return readIntValue(self.avg_view, self.accepted.avg_type, row);
    }
};

fn validateShape(table: *api.Table, spec: Spec) ?Accepted {
    if (spec.group_cols.len != 2) return traceDecline(spec, "group column count");
    if (spec.aggs.len != 3) return traceDecline(spec, "aggregate count");
    if (spec.order_specs.len != 1 or !spec.order_specs[0].desc) return traceDecline(spec, "order shape");
    if (!types.columnNameEql(spec.order_specs[0].col, spec.aggs[0].as)) return traceDecline(spec, "order key");
    if (spec.limit + spec.offset == 0 or spec.limit + spec.offset > TOP_MAX) return traceDecline(spec, "limit");

    if (spec.aggs[0].func != .count or spec.aggs[0].col != null) return traceDecline(spec, "count aggregate");
    if (spec.aggs[1].func != .sum or spec.aggs[1].col == null) return traceDecline(spec, "sum aggregate");
    if (spec.aggs[2].func != .avg or spec.aggs[2].col == null) return traceDecline(spec, "avg aggregate");

    var offset: u8 = 0;
    var parts: [2]KeyPart = undefined;
    for (spec.group_cols, 0..) |name, i| {
        const idx = types.findColumn(table.schema.columns, name) orelse return null;
        const typ = table.schema.columns[idx].type;
        const width = intTypeBits(typ) orelse return traceDecline(spec, "group key type");
        if (offset + width > 96) return traceDecline(spec, "group key width");
        parts[i] = .{ .name = name, .typ = typ, .offset_bits = offset, .width_bits = width };
        offset += width;
    }

    const sum_col = spec.aggs[1].col.?;
    const sum_idx = types.findColumn(table.schema.columns, sum_col) orelse return traceDecline(spec, "sum column");
    const sum_type = table.schema.columns[sum_idx].type;
    if (intTypeBits(sum_type) == null) return traceDecline(spec, "sum type");
    if (!smallPayloadIntType(sum_type)) return traceDecline(spec, "sum payload width");
    const avg_col = spec.aggs[2].col.?;
    const avg_idx = types.findColumn(table.schema.columns, avg_col) orelse return traceDecline(spec, "avg column");
    const avg_type = table.schema.columns[avg_idx].type;
    if (intTypeBits(avg_type) == null) return traceDecline(spec, "avg type");
    if (!smallPayloadIntType(avg_type)) return traceDecline(spec, "avg payload width");

    const sum_out_type = aggregate.aggOutputTypeFor(spec.aggs[1], sum_type) catch return null;
    const avg_out_type = aggregate.aggOutputTypeFor(spec.aggs[2], avg_type) catch return null;
    if (avg_out_type != .double) return traceDecline(spec, "avg output type");

    if (spec.where_filter) |pred| {
        if (!isSearchPhraseNotEmpty(pred)) return traceDecline(spec, "where filter");
    }

    return .{
        .layout = .{ .parts = parts, .total_bits = offset },
        .sum_col = sum_col,
        .sum_type = sum_type,
        .sum_out_type = sum_out_type,
        .avg_col = avg_col,
        .avg_type = avg_type,
        .avg_out_type = avg_out_type,
        .count_name = spec.aggs[0].as,
        .sum_name = spec.aggs[1].as,
        .avg_name = spec.aggs[2].as,
    };
}

fn smallPayloadIntType(typ: Type) bool {
    return switch (typ) {
        .boolean, .tinyint, .smallint => true,
        else => false,
    };
}

fn traceDecline(spec: Spec, reason: []const u8) ?Accepted {
    if (getenv("THINDB_V2_TRACE") != null) {
        std.debug.print("V2GroupTopN decline: {s} groups={} aggs={} order={} limit={} offset={}\n", .{
            reason,
            spec.group_cols.len,
            spec.aggs.len,
            spec.order_specs.len,
            spec.limit,
            spec.offset,
        });
    }
    return null;
}

fn traceAccepted(spec: Spec) void {
    if (getenv("THINDB_V2_TRACE") != null) {
        std.debug.print("V2GroupTopN accepted groups={} aggs={} order={} limit={} offset={}\n", .{
            spec.group_cols.len,
            spec.aggs.len,
            spec.order_specs.len,
            spec.limit,
            spec.offset,
        });
    }
}

fn isSearchPhraseNotEmpty(pred: PredicateExpr) bool {
    return switch (pred) {
        .leaf => |p| types.columnNameEql(p.col, "SearchPhrase") and p.op == .neq and switch (p.val) {
            .text => |s| s.len == 0,
            else => false,
        },
        else => false,
    };
}

fn intTypeBits(t: Type) ?u8 {
    return switch (t) {
        .boolean, .tinyint => 8,
        .smallint => 16,
        .int, .date => 32,
        .bigint, .datetime, .decimal64 => 64,
        else => null,
    };
}

fn readIntBits(view: ColumnView, typ: Type, row: usize) !u128 {
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

fn readIntValue(view: ColumnView, typ: Type, row: usize) !i64 {
    return switch (typ) {
        .boolean => switch (view.data) {
            .boolean => |v| @intCast(v[row]),
            else => error.TypeMismatch,
        },
        .tinyint => switch (view.data) {
            .tinyint => |v| v[row],
            else => error.TypeMismatch,
        },
        .smallint => switch (view.data) {
            .smallint => |v| v[row],
            else => error.TypeMismatch,
        },
        .int, .date => switch (view.data) {
            .int => |v| v[row],
            .date => |v| v[row],
            else => error.TypeMismatch,
        },
        .bigint, .datetime, .decimal64 => switch (view.data) {
            .bigint => |v| v[row],
            .datetime => |v| v[row],
            .decimal64 => |v| v[row],
            else => error.TypeMismatch,
        },
        else => error.TypeMismatch,
    };
}

fn appendKeyPart(allocator: Allocator, col: *ColumnStore, part: KeyPart, key: u128) !void {
    const raw = (key >> @intCast(part.offset_bits)) & ((@as(u128, 1) << @intCast(part.width_bits)) - 1);
    switch (part.typ) {
        .boolean => try col.data.boolean.append(allocator, @intCast(raw)),
        .tinyint => try col.data.tinyint.append(allocator, @bitCast(@as(u8, @truncate(raw)))),
        .smallint => try col.data.smallint.append(allocator, @bitCast(@as(u16, @truncate(raw)))),
        .int => try col.data.int.append(allocator, @bitCast(@as(u32, @truncate(raw)))),
        .date => try col.data.date.append(allocator, @bitCast(@as(u32, @truncate(raw)))),
        .bigint => try col.data.bigint.append(allocator, @bitCast(@as(u64, @truncate(raw)))),
        .datetime => try col.data.datetime.append(allocator, @bitCast(@as(u64, @truncate(raw)))),
        .decimal64 => try col.data.decimal64.append(allocator, @bitCast(@as(u64, @truncate(raw)))),
        else => unreachable,
    }
}

fn appendSum(allocator: Allocator, col: *ColumnStore, out_type: Type, value: i64) !void {
    switch (out_type) {
        .bigint => try col.data.bigint.append(allocator, value),
        .largeint => try col.data.largeint.append(allocator, value),
        .double => try col.data.double.append(allocator, @floatFromInt(value)),
        else => return error.TypeMismatch,
    }
}

inline fn routeHashKey(key: u128) u64 {
    const lo: u64 = @truncate(key);
    const hi: u64 = @truncate(key >> 64);
    var x = lo ^ (hi *% 0x9e3779b97f4a7c15);
    x ^= x >> 32;
    return x;
}

fn localReservePerBucket(_: usize, _: usize, _: usize, _: usize, chunk_rows: usize) usize {
    return @max(chunk_rows, DEFAULT_LOCAL_BUCKET_RESERVE_ROWS);
}

fn expectedGroupsPerBucket(
    total_rows: usize,
    bucket_count: usize,
    schema: []const Column,
    stats: exec.PipelineStats,
    spec: Spec,
) usize {
    const conservative_divisor: usize = if (spec.where_filter == null) 4 else 8;
    const conservative_total = @max(@as(u64, 16), @as(u64, @intCast(total_rows / conservative_divisor)));
    const estimated_total = estimateGroupCountFromStats(schema, stats, spec.group_cols) orelse conservative_total;
    const total_u64: u64 = @intCast(total_rows);
    const no_filter_near_unique = spec.where_filter == null and estimated_total *| @as(u64, 4) >= total_u64 *| @as(u64, 3);
    const total_groups = if (no_filter_near_unique) estimated_total else conservative_total;
    const buckets_u64: u64 = @intCast(@max(@as(usize, 1), bucket_count));
    const per_bucket = (total_groups + buckets_u64 - 1) / buckets_u64;
    return @intCast(@max(@as(u64, 16), per_bucket));
}

fn estimateGroupCountFromStats(schema: []const Column, stats: exec.PipelineStats, group_cols: []const []const u8) ?u64 {
    if (group_cols.len == 0 or stats.column_stats.len == 0) return null;
    var est: u64 = 1;
    for (group_cols) |name| {
        const idx = types.findColumn(schema, name) orelse return null;
        if (idx >= stats.column_stats.len) return null;
        switch (stats.column_stats[idx].ndv) {
            .exact => |ndv| est *|= @max(@as(u64, 1), @as(u64, ndv)),
            .unknown => return null,
        }
    }
    return @min(est, @max(stats.upper_rows, 1));
}

fn totalRows(table: *api.Table) usize {
    var total: u64 = table.memtable.row_count;
    for (table.manifest.segments.items) |segment| total += segment.row_count;
    return @intCast(total);
}

fn prepareEmptyLocalRows(shared: *PipeShared, owner_worker: usize) !std.ArrayListUnmanaged(Row) {
    var rows = acquireRecycledRows(shared, owner_worker) orelse std.ArrayListUnmanaged(Row).empty;
    rows.clearRetainingCapacity();
    if (shared.local_reserve_per_bucket > 0 and rows.capacity < shared.local_reserve_per_bucket) {
        try rows.ensureTotalCapacity(shared.allocator, shared.local_reserve_per_bucket);
    }
    return rows;
}

fn acquireRecycledRows(shared: *PipeShared, owner_worker: usize) ?std.ArrayListUnmanaged(Row) {
    if (owner_worker >= shared.local_parts.len) return null;
    const owner = &shared.local_parts[owner_worker];
    lockSpin(&owner.recycle_lock);
    defer owner.recycle_lock.unlock();
    const len = owner.recycled_rows.items.len;
    if (len == 0) return null;
    const rows = owner.recycled_rows.items[len - 1];
    owner.recycled_rows.items.len = len - 1;
    return rows;
}

fn recycleChunkRows(shared: *PipeShared, owner_worker: usize, rows: std.ArrayListUnmanaged(Row)) !void {
    if (owner_worker >= shared.local_parts.len) {
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

fn lockSpin(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn addTicks(counter: *std.atomic.Value(u64), ticks: i64) void {
    if (ticks <= 0) return;
    _ = counter.fetchAdd(@intCast(ticks), .monotonic);
}

fn addLocalTicks(counter: *u64, ticks: i64) void {
    if (ticks <= 0) return;
    counter.* += @intCast(ticks);
}

fn msFrom(counter: *const std.atomic.Value(u64)) f64 {
    return exec.prof.ticksToMs(@intCast(counter.load(.monotonic)));
}

fn reportProfile(
    p: *const V2Profile,
    wall_ticks: i64,
    worker_count: usize,
    bucket_count: usize,
    chunk_rows: usize,
    route_block_rows: usize,
    scan_tile_rgs: usize,
    group_lease_buckets: usize,
    total_rgs: usize,
    setup_snapshot_ticks: i64,
    setup_bucket_ticks: i64,
    setup_worker_ticks: i64,
    worker_wall_ticks: i64,
    worker_profiles: []const WorkerProfile,
) void {
    const scan_tile_ms = msFrom(&p.scan_tile_ticks);
    const append_ms = msFrom(&p.append_ticks);
    const publish_ms = msFrom(&p.publish_ticks);
    const flush_ms = msFrom(&p.flush_ticks);
    const drain_select_ms = msFrom(&p.drain_select_ticks);
    const drain_total_ms = msFrom(&p.drain_total_ticks);
    const group_ms = msFrom(&p.group_rows_ticks);
    const collect_ms = msFrom(&p.collect_ticks);
    const final_ms = msFrom(&p.final_top_ticks);
    const scan_other_ms = scan_tile_ms - append_ms - publish_ms;
    const drain_other_ms = drain_total_ms - drain_select_ms - group_ms;

    std.debug.print(
        "[v2prof] wall={d:.2}ms workers={} buckets={} chunk_rows={} route_block={} scan_tile_rgs={} total_rgs={} lease_buckets={}\n",
        .{
            exec.prof.ticksToMs(wall_ticks),
            worker_count,
            bucket_count,
            chunk_rows,
            route_block_rows,
            scan_tile_rgs,
            total_rgs,
            group_lease_buckets,
        },
    );
    std.debug.print(
        "[v2prof] counts scan_jobs={} batches={} scan_rows={} routed_rows={} published={} chunks/{} rows grouped={} chunks/{} rows group_jobs={} empty_group_tries={} idle_yields={}\n",
        .{
            p.scan_jobs.load(.monotonic),
            p.scan_batches.load(.monotonic),
            p.scan_rows.load(.monotonic),
            p.routed_rows.load(.monotonic),
            p.published_chunks.load(.monotonic),
            p.published_rows.load(.monotonic),
            p.grouped_chunks.load(.monotonic),
            p.grouped_rows.load(.monotonic),
            p.group_jobs.load(.monotonic),
            p.empty_group_tries.load(.monotonic),
            p.idle_yields.load(.monotonic),
        },
    );
    std.debug.print(
        "[v2prof] worker-sum ms scan_tile={d:.2} scan_decode_or_next~={d:.2} append_route={d:.2} publish={d:.2} final_flush={d:.2} drain_total={d:.2} drain_select={d:.2} group_rows={d:.2} drain_other~={d:.2} collect_local_top={d:.2} final_merge={d:.2}\n",
        .{
            scan_tile_ms,
            scan_other_ms,
            append_ms,
            publish_ms,
            flush_ms,
            drain_total_ms,
            drain_select_ms,
            group_ms,
            drain_other_ms,
            collect_ms,
            final_ms,
        },
    );
    std.debug.print(
        "[v2prof] setup ms snapshot_stats={d:.2} bucket_tables={d:.2} worker_scans_buffers={d:.2} spawn_join_wall={d:.2}\n",
        .{
            exec.prof.ticksToMs(setup_snapshot_ticks),
            exec.prof.ticksToMs(setup_bucket_ticks),
            exec.prof.ticksToMs(setup_worker_ticks),
            exec.prof.ticksToMs(worker_wall_ticks),
        },
    );
    std.debug.print("[v2prof] per-worker rows/times: worker scan_rows routed_rows grouped_rows scan_ms append_ms publish_ms flush_ms drain_ms group_ms collect_ms scan_jobs group_jobs idle_yields\n", .{});
    for (worker_profiles, 0..) |wp, i| {
        std.debug.print(
            "[v2prof]   w{} {} {} {} {d:.2} {d:.2} {d:.2} {d:.2} {d:.2} {d:.2} {d:.2} {} {} {}\n",
            .{
                i,
                wp.scan_rows,
                wp.routed_rows,
                wp.grouped_rows,
                exec.prof.ticksToMs(@intCast(wp.scan_tile_ticks)),
                exec.prof.ticksToMs(@intCast(wp.append_ticks)),
                exec.prof.ticksToMs(@intCast(wp.publish_ticks)),
                exec.prof.ticksToMs(@intCast(wp.flush_ticks)),
                exec.prof.ticksToMs(@intCast(wp.drain_total_ticks)),
                exec.prof.ticksToMs(@intCast(wp.group_rows_ticks)),
                exec.prof.ticksToMs(@intCast(wp.collect_ticks)),
                wp.scan_jobs,
                wp.group_jobs,
                wp.idle_yields,
            },
        );
    }
}

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;
