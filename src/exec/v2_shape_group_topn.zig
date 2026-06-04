//! Minimal V2 shape: table scan -> optional fused filter -> grouped aggregate
//! -> ORDER BY aggregate DESC -> LIMIT/OFFSET.
//!
//! This is deliberately shaped as a pipeline instead of an operator chain. The
//! first implementation reuses the proven ClickBench group-topN core for Q30-Q32
//! while keeping the public structure ready for a native reusable workspace.

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
const aggregate = @import("aggregate.zig");

const Batch = exec.Batch;
const Query = exec.Query;
const PredicateExpr = exec.PredicateExpr;
const SortSpec = exec.SortSpec;
const AggSpec = exec.AggSpec;
const HarnessCore = exec.group_topn_harness_core;

const DEFAULT_DOP: usize = 12;
const DEFAULT_BUCKET_COUNT: usize = 128;
const DEFAULT_CHUNK_ROWS: usize = 8192;
const DEFAULT_SCAN_TILE_RGS: usize = 16;
const DEFAULT_ROUTE_BLOCK_ROWS: usize = 2048;
const DEFAULT_GROUP_LEASE_BUCKETS: usize = 8;
const DEFAULT_GROUP_INIT_CAP: usize = 0;
const DEFAULT_RAW_CHUNK_ROWS: usize = 32768;

pub const Request = struct {
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

const ShapePlan = struct {
    layout: KeyLayout,
    core_kind: HarnessCore.QueryKind,
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

const StageTimes = struct {
    prepare_ticks: i64 = 0,
    run_core_ticks: i64 = 0,
    workspace_teardown_ticks: i64 = 0,
    emit_ticks: i64 = 0,
};

const ExecutionContext = struct {
    allocator: Allocator,
    table: *api.Table,
    request: Request,
    plan: ShapePlan,
    dop: usize,
    bucket_count: usize = DEFAULT_BUCKET_COUNT,
    chunk_rows: usize = DEFAULT_CHUNK_ROWS,
    route_block_rows: usize = DEFAULT_ROUTE_BLOCK_ROWS,
    group_init_cap: usize = DEFAULT_GROUP_INIT_CAP,
    times: StageTimes = .{},
};

const TopRows = struct {
    allocator: Allocator,
    items: []HarnessCore.TopRow = &.{},

    fn deinit(self: *TopRows) void {
        const allocator = self.allocator;
        if (self.items.len > 0) self.allocator.free(self.items);
        self.* = .{ .allocator = allocator };
    }
};

const AsyncWorkspaceTeardownTask = struct {
    allocator: Allocator,
    workspace: HarnessCore.SiloGridWorkspace,
    cpus: []usize,
    n_workers: usize,
    trace_timing: bool,
    query_label: []const u8,
    bucket_count: usize,
};

pub fn tryBuild(allocator: Allocator, table: *api.Table, request: Request) !?Query {
    const plan = validateShape(table, request) orelse return null;
    traceAccepted(request, plan);
    const op = try allocator.create(GroupTopNPipeline);
    errdefer allocator.destroy(op);
    op.* = try GroupTopNPipeline.init(allocator, table, request, plan);
    return exec.makeQuery(allocator, op);
}

const GroupTopNPipeline = struct {
    allocator: Allocator,
    table: *api.Table,
    request: Request,
    plan: ShapePlan,
    output_schema: []Column,
    output_cols: []ColumnStore,
    views: []ColumnView,
    owned_needed: ?[]const []const u8 = null,
    emitted: bool = false,
    built: bool = false,
    row_count: usize = 0,

    fn init(allocator: Allocator, table: *api.Table, request: Request, plan: ShapePlan) !GroupTopNPipeline {
        const owned_needed = if (request.needed) |needed| try allocator.dupe([]const u8, needed) else null;
        errdefer if (owned_needed) |n| allocator.free(n);

        var owned_request = request;
        owned_request.needed = owned_needed;

        const output_schema = try allocator.alloc(Column, request.group_cols.len + request.aggs.len);
        errdefer allocator.free(output_schema);
        for (plan.layout.parts, 0..) |part, i| {
            output_schema[i] = .{ .name = part.name, .type = part.typ };
        }
        output_schema[2] = .{ .name = plan.count_name, .type = .bigint };
        output_schema[3] = .{ .name = plan.sum_name, .type = plan.sum_out_type };
        output_schema[4] = .{ .name = plan.avg_name, .type = plan.avg_out_type };

        const output_cols = try allocator.alloc(ColumnStore, output_schema.len);
        errdefer allocator.free(output_cols);
        var built_cols: usize = 0;
        errdefer for (output_cols[0..built_cols]) |*c| c.deinit(allocator);
        for (output_schema, 0..) |col, i| {
            output_cols[i] = try ColumnStore.initCapacity(allocator, col.type, col.nullable, request.limit, 0);
            built_cols += 1;
        }

        const views = try allocator.alloc(ColumnView, output_schema.len);
        errdefer allocator.free(views);

        return .{
            .allocator = allocator,
            .table = table,
            .request = owned_request,
            .plan = plan,
            .output_schema = output_schema,
            .output_cols = output_cols,
            .views = views,
            .owned_needed = owned_needed,
        };
    }

    pub fn deinit(self: *GroupTopNPipeline) void {
        for (self.output_cols) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_cols);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        if (self.owned_needed) |n| self.allocator.free(n);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *GroupTopNPipeline) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(_: *GroupTopNPipeline, _: exec.Predicate) !void {}

    pub fn stats(self: *GroupTopNPipeline) exec.PipelineStats {
        return .{ .upper_rows = self.request.limit };
    }

    pub fn accountant(_: *GroupTopNPipeline) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(_: *GroupTopNPipeline, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainIndent(out, allocator, depth);
        try out.appendSlice(allocator, "V2Pipeline(group-topN: scan/filter/group/order/limit)\n");
    }

    pub fn next(self: *GroupTopNPipeline) !?Batch {
        if (self.emitted) return null;
        if (!self.built) {
            try self.execute();
            self.built = true;
        }
        self.emitted = true;
        for (self.output_cols, 0..) |c, i| self.views[i] = c.view();
        return .{ .schema = self.output_schema, .values = self.views, .row_count = self.row_count };
    }

    fn execute(self: *GroupTopNPipeline) !void {
        var ctx = try prepareExecution(self.allocator, self.table, self.request, self.plan);
        var rows = try runGroupTopNStage(&ctx);
        defer rows.deinit();
        const emit_t0 = exec.prof.nowTicks();
        try emitResultStage(self, rows.items);
        ctx.times.emit_ticks = exec.prof.nowTicks() - emit_t0;
        traceProfile(ctx);
    }
};

fn prepareExecution(allocator: Allocator, table: *api.Table, request: Request, plan: ShapePlan) !ExecutionContext {
    const t0 = exec.prof.nowTicks();
    var ctx = ExecutionContext{
        .allocator = allocator,
        .table = table,
        .request = request,
        .plan = plan,
        .dop = @max(@as(usize, 1), request.dop),
    };
    if (ctx.dop == 0) ctx.dop = DEFAULT_DOP;
    ctx.times.prepare_ticks = exec.prof.nowTicks() - t0;
    return ctx;
}

fn runGroupTopNStage(ctx: *ExecutionContext) !TopRows {
    const t0 = exec.prof.nowTicks();
    const core_allocator = std.heap.page_allocator;
    var layout = try HarnessCore.cpuLayout(core_allocator);
    defer layout.deinit(core_allocator);
    const n = @min(@max(@as(usize, 1), ctx.dop), layout.order.len);
    if (n == 0) return .{ .allocator = core_allocator };
    const bucket_count = @max(@as(usize, 1), envUsize("THINDB_V2_BUCKET_COUNT", DEFAULT_BUCKET_COUNT));
    const chunk_rows = @max(@as(usize, 1), envUsize("THINDB_V2_CHUNK_ROWS", DEFAULT_CHUNK_ROWS));
    const route_block_rows = @max(@as(usize, 1), envUsize("THINDB_V2_ROUTE_BLOCK_ROWS", DEFAULT_ROUTE_BLOCK_ROWS));
    const group_init_cap = envUsize("THINDB_V2_GROUP_INIT_CAP", DEFAULT_GROUP_INIT_CAP);
    const group_lease_buckets = @max(@as(usize, 1), envUsize("THINDB_V2_GROUP_LEASE_BUCKETS", DEFAULT_GROUP_LEASE_BUCKETS));
    const group_lease_rows = @as(u64, @intCast(envUsize("THINDB_V2_GROUP_LEASE_ROWS", 0)));
    ctx.bucket_count = bucket_count;
    ctx.chunk_rows = chunk_rows;
    ctx.route_block_rows = route_block_rows;
    ctx.group_init_cap = group_init_cap;
    var workspace: HarnessCore.SiloGridWorkspace = .{};
    const trace_timing = getenv("THINDB_V2_PIPELINE_TRACE") != null;
    const arena_workspace = getenv("THINDB_V2_ARENA_WORKSPACE") != null;
    const shared_scan_buffers = getenv("THINDB_V2_SHARED_SCAN_BUFFERS") != null;
    const shared_scan_banks = @max(@as(usize, 1), envUsize("THINDB_V2_SHARED_SCAN_BANKS", 1));
    const force_queue_publish = getenv("THINDB_V2_FORCE_QUEUE_PUBLISH") != null;
    const flat_scan_partitions = getenv("THINDB_V2_FLAT_SCAN_PARTITIONS") != null;
    const raw_chunk_rows = @max(@as(usize, 1), envUsize("THINDB_V2_RAW_CHUNK_ROWS", DEFAULT_RAW_CHUNK_ROWS));
    const raw_batch_chunks = @max(@as(usize, 1), envUsize("THINDB_V2_RAW_BATCH_CHUNKS", 4));
    const raw_stage_lanes = @max(@as(usize, 1), envUsize("THINDB_V2_STAGE_LANES", 16));
    const raw_group_lane_claim = getenv("THINDB_V2_GROUP_LANE_CLAIM") != null;
    const raw_group_mode: HarnessCore.RawGroupMode = if (getenv("THINDB_V2_RAW_GROUP_MODE")) |mode_z| blk: {
        const mode = std.mem.span(mode_z);
        break :blk if (std.mem.eql(u8, mode, "radix"))
            .radix_global
        else if (std.mem.eql(u8, mode, "batch") or std.mem.eql(u8, mode, "radix_batch"))
            .radix_batch
        else if (std.mem.eql(u8, mode, "cluster") or std.mem.eql(u8, mode, "radix_cluster"))
            .radix_cluster
        else if (std.mem.eql(u8, mode, "staged") or std.mem.eql(u8, mode, "radix_staged"))
            .radix_staged
        else if (std.mem.eql(u8, mode, "preagg"))
            .preagg_merge
        else if (std.mem.eql(u8, mode, "sortpreagg"))
            .sort_preagg
        else
            .off;
    } else .radix_global;
    const worker_profile = getenv("THINDB_V2_WORKER_PROFILE") != null;
    if (arena_workspace) {
        var arena = std.heap.ArenaAllocator.init(core_allocator);
        const arena_allocator = arena.allocator();
        errdefer arena.deinit();

        var rows: std.ArrayListUnmanaged(HarnessCore.TopRow) = .empty;
        try HarnessCore.runSiloGrid(arena_allocator, ctx.table, layout.order[0..n], .{
            .dop = ctx.dop,
            .bucket_count = bucket_count,
            .kind = ctx.plan.core_kind,
            .silo_grid = true,
            .scan_filter = true,
            .chunk_rows = chunk_rows,
            .chunk_rows_set = true,
            .scan_tile_rgs = DEFAULT_SCAN_TILE_RGS,
            .scan_tile_rgs_set = true,
            .route_block_rows = route_block_rows,
            .route_block_rows_set = true,
            .group_lease_buckets = group_lease_buckets,
            .group_lease_rows = group_lease_rows,
            .group_init_cap = group_init_cap,
            .shared_scan_buffers = shared_scan_buffers,
            .shared_scan_banks = shared_scan_banks,
            .force_queue_publish = force_queue_publish,
            .flat_scan_partitions = flat_scan_partitions,
            .raw_group_mode = raw_group_mode,
            .raw_chunk_rows = raw_chunk_rows,
            .raw_batch_chunks = raw_batch_chunks,
            .raw_stage_lanes = raw_stage_lanes,
            .raw_group_lane_claim = raw_group_lane_claim,
            .no_profile = !worker_profile,
            .quiet = !worker_profile,
            .result_out = &rows,
            .trace_timing = trace_timing,
            .workspace = &workspace,
        });
        const owned = try core_allocator.dupe(HarnessCore.TopRow, rows.items);
        ctx.times.run_core_ticks = exec.prof.nowTicks() - t0;
        const teardown_t0 = exec.prof.nowTicks();
        arena.deinit();
        ctx.times.workspace_teardown_ticks = exec.prof.nowTicks() - teardown_t0;
        if (trace_timing) {
            std.debug.print("[workspace-arena-teardown] query={s} total={d:.3}ms mode=arena-bulk-free\n", .{
                ctx.plan.core_kind.label(),
                exec.prof.ticksToMs(ctx.times.workspace_teardown_ticks),
            });
        }
        return .{ .allocator = core_allocator, .items = owned };
    }

    errdefer workspace.deinitParallel(core_allocator, n, layout.order[0..n], null);

    var rows: std.ArrayListUnmanaged(HarnessCore.TopRow) = .empty;
    errdefer rows.deinit(core_allocator);
    try HarnessCore.runSiloGrid(core_allocator, ctx.table, layout.order[0..n], .{
        .dop = ctx.dop,
        .bucket_count = bucket_count,
        .kind = ctx.plan.core_kind,
        .silo_grid = true,
        .scan_filter = true,
        .chunk_rows = chunk_rows,
        .chunk_rows_set = true,
        .scan_tile_rgs = DEFAULT_SCAN_TILE_RGS,
        .scan_tile_rgs_set = true,
        .route_block_rows = route_block_rows,
        .route_block_rows_set = true,
        .group_lease_buckets = group_lease_buckets,
        .group_lease_rows = group_lease_rows,
        .group_init_cap = group_init_cap,
        .shared_scan_buffers = shared_scan_buffers,
        .shared_scan_banks = shared_scan_banks,
        .force_queue_publish = force_queue_publish,
        .flat_scan_partitions = flat_scan_partitions,
        .raw_group_mode = raw_group_mode,
        .raw_chunk_rows = raw_chunk_rows,
        .raw_batch_chunks = raw_batch_chunks,
        .raw_stage_lanes = raw_stage_lanes,
        .raw_group_lane_claim = raw_group_lane_claim,
        .no_profile = !worker_profile,
        .quiet = !worker_profile,
        .result_out = &rows,
        .trace_timing = trace_timing,
        .workspace = &workspace,
    });
    const owned = try rows.toOwnedSlice(core_allocator);
    ctx.times.run_core_ticks = exec.prof.nowTicks() - t0;
    ctx.times.workspace_teardown_ticks = scheduleWorkspaceTeardown(
        core_allocator,
        &workspace,
        n,
        layout.order[0..n],
        trace_timing,
        ctx.plan.core_kind.label(),
        bucket_count,
    );
    return .{ .allocator = core_allocator, .items = owned };
}

fn scheduleWorkspaceTeardown(
    allocator: Allocator,
    workspace: *HarnessCore.SiloGridWorkspace,
    n_workers: usize,
    cpus: []const usize,
    trace_timing: bool,
    query_label: []const u8,
    bucket_count: usize,
) i64 {
    const t0 = exec.prof.nowTicks();
    if (getenv("THINDB_V2_SYNC_TEARDOWN") != null) {
        var teardown_profile: HarnessCore.WorkspaceProfile = .{};
        const profile_ptr: ?*HarnessCore.WorkspaceProfile = if (trace_timing) &teardown_profile else null;
        workspace.deinitParallel(allocator, n_workers, cpus, profile_ptr);
        if (profile_ptr) |profile| profile.printTeardown(query_label, bucket_count);
        return exec.prof.nowTicks() - t0;
    }

    const task = allocator.create(AsyncWorkspaceTeardownTask) catch {
        workspace.deinitParallel(allocator, n_workers, cpus, null);
        return exec.prof.nowTicks() - t0;
    };
    const cpus_copy = allocator.dupe(usize, cpus) catch {
        allocator.destroy(task);
        workspace.deinitParallel(allocator, n_workers, cpus, null);
        return exec.prof.nowTicks() - t0;
    };
    task.* = .{
        .allocator = allocator,
        .workspace = workspace.*,
        .cpus = cpus_copy,
        .n_workers = n_workers,
        .trace_timing = trace_timing,
        .query_label = query_label,
        .bucket_count = bucket_count,
    };
    workspace.* = .{};
    const thread = std.Thread.spawn(.{}, asyncWorkspaceTeardown, .{task}) catch {
        var teardown_profile: HarnessCore.WorkspaceProfile = .{};
        const profile_ptr: ?*HarnessCore.WorkspaceProfile = if (trace_timing) &teardown_profile else null;
        task.workspace.deinitParallel(allocator, n_workers, cpus_copy, profile_ptr);
        if (profile_ptr) |profile| profile.printTeardown(query_label, bucket_count);
        allocator.free(cpus_copy);
        allocator.destroy(task);
        return exec.prof.nowTicks() - t0;
    };
    thread.detach();
    return exec.prof.nowTicks() - t0;
}

fn asyncWorkspaceTeardown(task: *AsyncWorkspaceTeardownTask) void {
    var teardown_profile: HarnessCore.WorkspaceProfile = .{};
    const profile_ptr: ?*HarnessCore.WorkspaceProfile = if (task.trace_timing) &teardown_profile else null;
    task.workspace.deinitParallel(task.allocator, task.n_workers, task.cpus, profile_ptr);
    if (profile_ptr) |profile| {
        profile.printTeardown(task.query_label, task.bucket_count);
        std.debug.print("[workspace-teardown-async] query={s} complete=true\n", .{task.query_label});
    }
    task.allocator.free(task.cpus);
    const allocator = task.allocator;
    allocator.destroy(task);
}

fn emitResultStage(op: *GroupTopNPipeline, rows: []const HarnessCore.TopRow) !void {
    const start = @min(op.request.offset, rows.len);
    const end = @min(rows.len, start + op.request.limit);
    for (rows[start..end]) |row| {
        try appendKeyPart(op.allocator, &op.output_cols[0], op.plan.layout.parts[0], row.key);
        try appendKeyPart(op.allocator, &op.output_cols[1], op.plan.layout.parts[1], row.key);
        try op.output_cols[2].data.bigint.append(op.allocator, @intCast(row.count));
        try appendSum(op.allocator, &op.output_cols[3], op.plan.sum_out_type, row.refresh_sum);
        try op.output_cols[4].data.double.append(
            op.allocator,
            if (row.count == 0) 0.0 else @as(f64, @floatFromInt(row.width_sum)) / @as(f64, @floatFromInt(row.count)),
        );
        op.row_count += 1;
    }
}

fn validateShape(table: *api.Table, request: Request) ?ShapePlan {
    if (request.group_cols.len != 2) return traceDecline(request, "group key count");
    if (request.aggs.len != 3) return traceDecline(request, "aggregate count");
    if (request.order_specs.len != 1) return traceDecline(request, "order key count");
    if (request.limit == 0) return traceDecline(request, "zero limit");
    if (request.aggs[0].func != .count or request.aggs[0].col != null) return traceDecline(request, "count agg");
    if (request.aggs[1].func != .sum or request.aggs[1].col == null) return traceDecline(request, "sum agg");
    if (request.aggs[2].func != .avg or request.aggs[2].col == null) return traceDecline(request, "avg agg");
    if (!types.columnNameEql(request.order_specs[0].col, request.aggs[0].as) or !request.order_specs[0].desc) {
        return traceDecline(request, "order key");
    }

    var parts: [2]KeyPart = undefined;
    var offset: u8 = 0;
    for (request.group_cols, 0..) |name, i| {
        const idx = types.findColumn(table.schema.columns, name) orelse return traceDecline(request, "group key column");
        const typ = table.schema.columns[idx].type;
        const width = intTypeBits(typ) orelse return traceDecline(request, "group key type");
        if (offset + width > 96) return traceDecline(request, "group key width");
        parts[i] = .{ .name = name, .typ = typ, .offset_bits = offset, .width_bits = width };
        offset += width;
    }

    const sum_col = request.aggs[1].col.?;
    const sum_idx = types.findColumn(table.schema.columns, sum_col) orelse return traceDecline(request, "sum column");
    const sum_type = table.schema.columns[sum_idx].type;
    if (intTypeBits(sum_type) == null or !smallPayloadIntType(sum_type)) return traceDecline(request, "sum type");

    const avg_col = request.aggs[2].col.?;
    const avg_idx = types.findColumn(table.schema.columns, avg_col) orelse return traceDecline(request, "avg column");
    const avg_type = table.schema.columns[avg_idx].type;
    if (intTypeBits(avg_type) == null or !smallPayloadIntType(avg_type)) return traceDecline(request, "avg type");

    const core_kind = classifyClickBenchCoreKind(request) orelse return traceDecline(request, "core adapter");
    const sum_out_type = aggregate.aggOutputTypeFor(request.aggs[1], sum_type) catch return null;
    const avg_out_type = aggregate.aggOutputTypeFor(request.aggs[2], avg_type) catch return null;
    if (avg_out_type != .double) return traceDecline(request, "avg output type");

    return .{
        .layout = .{ .parts = parts, .total_bits = offset },
        .core_kind = core_kind,
        .sum_col = sum_col,
        .sum_type = sum_type,
        .sum_out_type = sum_out_type,
        .avg_col = avg_col,
        .avg_type = avg_type,
        .avg_out_type = avg_out_type,
        .count_name = request.aggs[0].as,
        .sum_name = request.aggs[1].as,
        .avg_name = request.aggs[2].as,
    };
}

fn classifyClickBenchCoreKind(request: Request) ?HarnessCore.QueryKind {
    const a = request.group_cols[0];
    const b = request.group_cols[1];
    const has_filter = request.where_filter != null;
    if (has_filter) {
        if (!isSearchPhraseNotEmpty(request.where_filter.?)) return null;
        if (types.columnNameEql(a, "SearchEngineID") and types.columnNameEql(b, "ClientIP")) return .q30;
        if (types.columnNameEql(a, "WatchID") and types.columnNameEql(b, "ClientIP")) return .q31;
        return null;
    }
    if (types.columnNameEql(a, "WatchID") and types.columnNameEql(b, "ClientIP")) return .q32;
    return null;
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

fn smallPayloadIntType(typ: Type) bool {
    return switch (typ) {
        .boolean, .tinyint, .smallint => true,
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

fn traceDecline(request: Request, reason: []const u8) ?ShapePlan {
    if (getenv("THINDB_V2_TRACE") != null) {
        std.debug.print("V2Pipeline group-topN decline: {s} groups={} aggs={} order={} limit={} offset={}\n", .{
            reason,
            request.group_cols.len,
            request.aggs.len,
            request.order_specs.len,
            request.limit,
            request.offset,
        });
    }
    return null;
}

fn traceAccepted(request: Request, plan: ShapePlan) void {
    if (getenv("THINDB_V2_TRACE") != null) {
        std.debug.print("V2Pipeline group-topN accepted kind={s} groups={} aggs={} order={} limit={} offset={}\n", .{
            plan.core_kind.label(),
            request.group_cols.len,
            request.aggs.len,
            request.order_specs.len,
            request.limit,
            request.offset,
        });
    }
}

fn traceProfile(ctx: ExecutionContext) void {
    if (getenv("THINDB_V2_PIPELINE_TRACE") == null) return;
    std.debug.print("[v2-pipeline] shape=group-topN kind={s} prepare={d:.3}ms core={d:.1}ms workspace_teardown={d:.1}ms emit={d:.3}ms dop={} buckets={} chunk_rows={} route_block_rows={} group_init_cap={} shared_scan_buffers={s} shared_scan_banks={} force_queue_publish={s} flat_scan_partitions={s} raw_group_mode={s} raw_chunk_rows={} raw_batch_chunks={} raw_stage_lanes={}\n", .{
        ctx.plan.core_kind.label(),
        exec.prof.ticksToMs(ctx.times.prepare_ticks),
        exec.prof.ticksToMs(ctx.times.run_core_ticks),
        exec.prof.ticksToMs(ctx.times.workspace_teardown_ticks),
        exec.prof.ticksToMs(ctx.times.emit_ticks),
        ctx.dop,
        ctx.bucket_count,
        ctx.chunk_rows,
        ctx.route_block_rows,
        ctx.group_init_cap,
        if (getenv("THINDB_V2_SHARED_SCAN_BUFFERS") != null) "true" else "false",
        @max(@as(usize, 1), envUsize("THINDB_V2_SHARED_SCAN_BANKS", 1)),
        if (getenv("THINDB_V2_FORCE_QUEUE_PUBLISH") != null) "true" else "false",
        if (getenv("THINDB_V2_FLAT_SCAN_PARTITIONS") != null) "true" else "false",
        if (getenv("THINDB_V2_RAW_GROUP_MODE")) |mode_z| std.mem.span(mode_z) else "off",
        @max(@as(usize, 1), envUsize("THINDB_V2_RAW_CHUNK_ROWS", DEFAULT_RAW_CHUNK_ROWS)),
        @max(@as(usize, 1), envUsize("THINDB_V2_RAW_BATCH_CHUNKS", 4)),
        @max(@as(usize, 1), envUsize("THINDB_V2_STAGE_LANES", 16)),
    });
}

fn envUsize(comptime name: [:0]const u8, default: usize) usize {
    const raw = getenv(name.ptr) orelse return default;
    const text = std.mem.span(raw);
    if (text.len == 0) return default;
    return std.fmt.parseInt(usize, text, 10) catch default;
}

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;
