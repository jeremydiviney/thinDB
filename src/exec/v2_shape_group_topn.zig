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
const GroupTopNEngine = exec.v2_group_topn_engine;

const DEFAULT_DOP: usize = 12;
const DEFAULT_BUCKET_COUNT: usize = 256;
const DEFAULT_CHUNK_ROWS: usize = 8192;
const DEFAULT_SCAN_TILE_RGS: usize = 16;
const DEFAULT_ROUTE_BLOCK_ROWS: usize = 2048;
const DEFAULT_GROUP_LEASE_BUCKETS: usize = 8;
const DEFAULT_GROUP_INIT_CAP: usize = 0;
const DEFAULT_RAW_CHUNK_ROWS: usize = 8192;
const DEFAULT_RAW_GROUP_CHUNK_ROWS: usize = 8192;
const DEFAULT_RAW_BATCH_CHUNKS: usize = 12;
const MAX_GROUP_KEYS: usize = 8;
const MAX_AGGS: usize = 3;
const MAX_AGG_INPUTS: usize = 8;

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
    parts: [MAX_GROUP_KEYS]KeyPart,
    part_count: usize,
    total_bits: u8,
};

const AggregateInputPlan = struct {
    source_name: []const u8,
    source_type: Type,
    physical_type: GroupTopNEngine.PhysicalType,
};

const AggregatePlan = struct {
    name: []const u8,
    func: exec.AggFunc,
    input_column_index: ?u16,
    input_type: GroupTopNEngine.PhysicalType,
    state_index: u16,
    output_type: Type,
};

const ShapePlan = struct {
    layout: KeyLayout,
    core_kind: HarnessCore.QueryKind,
    aggregate_inputs: [MAX_AGG_INPUTS]AggregateInputPlan,
    aggregate_input_count: usize,
    aggregates: [MAX_AGGS]AggregatePlan,
    aggregate_count: usize,
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
    raw_group_chunk_rows: usize = DEFAULT_RAW_GROUP_CHUNK_ROWS,
    raw_batch_chunks: usize = DEFAULT_RAW_BATCH_CHUNKS,
    shared_stage_builders: bool = true,
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
        for (plan.layout.parts[0..plan.layout.part_count], 0..) |part, i| {
            output_schema[i] = .{ .name = part.name, .type = part.typ };
        }
        for (plan.aggregates[0..plan.aggregate_count], 0..) |agg_plan, i| {
            output_schema[plan.layout.part_count + i] = .{ .name = agg_plan.name, .type = agg_plan.output_type };
        }

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
    const params = GroupTopNEngine.paramsFromEnv(ctx.dop);
    ctx.bucket_count = params.bucket_count;
    ctx.chunk_rows = params.raw_chunk_rows;
    ctx.route_block_rows = params.route_block_rows;
    ctx.group_init_cap = params.group_init_cap;
    ctx.raw_group_chunk_rows = params.raw_group_chunk_rows;
    ctx.raw_batch_chunks = params.raw_batch_chunks;
    ctx.shared_stage_builders = params.shared_stage_builders;

    var aggregate_inputs: [MAX_AGG_INPUTS]GroupTopNEngine.AggregateInput = undefined;
    var i: usize = 0;
    while (i < ctx.plan.aggregate_input_count) : (i += 1) {
        const input = ctx.plan.aggregate_inputs[i];
        aggregate_inputs[i] = .{
            .source_name = input.source_name,
            .source_type = input.source_type,
            .physical_type = input.physical_type,
        };
    }
    var aggregate_program: [MAX_AGGS]GroupTopNEngine.AggregateSpec = undefined;
    i = 0;
    while (i < ctx.plan.aggregate_count) : (i += 1) {
        const agg_plan = ctx.plan.aggregates[i];
        aggregate_program[i] = .{
            .op = switch (agg_plan.func) {
                .count => if (agg_plan.input_column_index == null) .count_star else .count_col,
                .sum => .sum,
                .avg => .avg,
                .min => .min,
                .max => .max,
                else => return error.UnsupportedOperatorForType,
            },
            .input_column_index = agg_plan.input_column_index,
            .input_type = agg_plan.input_type,
            .state_index = agg_plan.state_index,
        };
    }

    var result = try GroupTopNEngine.run(core_allocator, .{
        .table = ctx.table,
        .kind = ctx.plan.core_kind,
        .shape = .{
            .key_width = GroupTopNEngine.KeyWidth.fromBits(ctx.plan.layout.total_bits),
            .key_bits = ctx.plan.layout.total_bits,
            .group_key_count = ctx.plan.layout.part_count,
            .aggregate_inputs = aggregate_inputs[0..ctx.plan.aggregate_input_count],
            .aggregate_program = aggregate_program[0..ctx.plan.aggregate_count],
            .has_filter = ctx.request.where_filter != null,
            .order_by_count_desc = true,
            .limit = ctx.request.limit,
            .offset = ctx.request.offset,
        },
        .params = params,
    });

    ctx.times.run_core_ticks = exec.prof.nowTicks() - t0 - result.times.workspace_teardown_ticks;
    ctx.times.workspace_teardown_ticks = result.times.workspace_teardown_ticks;
    const rows = result.rows;
    result.rows = &.{};
    return .{ .allocator = result.allocator, .items = rows };
}

fn emitResultStage(op: *GroupTopNPipeline, rows: []const HarnessCore.TopRow) !void {
    const start = @min(op.request.offset, rows.len);
    const end = @min(rows.len, start + op.request.limit);
    for (rows[start..end]) |row| {
        for (op.plan.layout.parts[0..op.plan.layout.part_count], 0..) |part, i| {
            try appendKeyPart(op.allocator, &op.output_cols[i], part, row.key);
        }
        for (op.plan.aggregates[0..op.plan.aggregate_count], 0..) |agg_plan, i| {
            try appendAggregateValue(op.allocator, &op.output_cols[op.plan.layout.part_count + i], agg_plan, row);
        }
        op.row_count += 1;
    }
}

fn appendAggregateValue(allocator: Allocator, col: *ColumnStore, agg_plan: AggregatePlan, row: HarnessCore.TopRow) !void {
    switch (agg_plan.func) {
        .count => try col.data.bigint.append(allocator, @intCast(row.count)),
        .sum => try appendIntegerAggregate(allocator, col, agg_plan.output_type, row.refresh_sum),
        .avg => try col.data.double.append(
            allocator,
            if (row.count == 0) 0.0 else @as(f64, @floatFromInt(row.width_sum)) / @as(f64, @floatFromInt(row.count)),
        ),
        else => return error.UnsupportedOperatorForType,
    }
}

fn validateShape(table: *api.Table, request: Request) ?ShapePlan {
    if (request.group_cols.len == 0 or request.group_cols.len > MAX_GROUP_KEYS) return traceDecline(request, "group key count");
    if (request.aggs.len == 0 or request.aggs.len > MAX_AGGS) return traceDecline(request, "aggregate count");
    if (request.order_specs.len != 1) return traceDecline(request, "order key count");
    if (request.limit == 0) return traceDecline(request, "zero limit");

    var count_order_found = false;
    for (request.aggs) |agg| {
        if (agg.func == .count and types.columnNameEql(request.order_specs[0].col, agg.as)) {
            count_order_found = true;
            break;
        }
    }
    if (!count_order_found or !request.order_specs[0].desc) return traceDecline(request, "order key");

    var parts: [MAX_GROUP_KEYS]KeyPart = undefined;
    var offset: u8 = 0;
    for (request.group_cols, 0..) |name, i| {
        const idx = types.findColumn(table.schema.columns, name) orelse return traceDecline(request, "group key column");
        const typ = table.schema.columns[idx].type;
        const width = intTypeBits(typ) orelse return traceDecline(request, "group key type");
        if (offset + width > 96) return traceDecline(request, "group key width");
        parts[i] = .{ .name = name, .typ = typ, .offset_bits = offset, .width_bits = width };
        offset += width;
    }

    const core_kind = classifyClickBenchCoreKind(request) orelse return traceDecline(request, "core adapter");

    var aggregate_inputs: [MAX_AGG_INPUTS]AggregateInputPlan = undefined;
    var aggregate_input_count: usize = 0;
    var aggregates: [MAX_AGGS]AggregatePlan = undefined;
    for (request.aggs, 0..) |agg, agg_i| {
        if (agg_i >= MAX_AGGS) return traceDecline(request, "aggregate count");
        const state_index: u16 = @intCast(agg_i);
        switch (agg.func) {
            .count => {
                const input_column_index = if (agg.col) |col_name| blk: {
                    const input_idx = addAggregateInput(table, &aggregate_inputs, &aggregate_input_count, col_name) orelse return traceDecline(request, "count input");
                    break :blk input_idx;
                } else null;
                aggregates[agg_i] = .{
                    .name = agg.as,
                    .func = agg.func,
                    .input_column_index = input_column_index,
                    .input_type = .u64,
                    .state_index = state_index,
                    .output_type = aggregate.aggOutputTypeFor(agg, if (agg.col) |col_name| columnType(table, col_name) orelse return traceDecline(request, "count input type") else null) catch return null,
                };
            },
            .sum, .avg => {
                const col_name = agg.col orelse return traceDecline(request, "aggregate column");
                const input_idx = addAggregateInput(table, &aggregate_inputs, &aggregate_input_count, col_name) orelse return traceDecline(request, "aggregate input");
                const input_type = columnType(table, col_name) orelse return traceDecline(request, "aggregate type");
                const output_type = aggregate.aggOutputTypeFor(agg, input_type) catch return null;
                if (agg.func == .avg and output_type != .double) return traceDecline(request, "avg output type");
                aggregates[agg_i] = .{
                    .name = agg.as,
                    .func = agg.func,
                    .input_column_index = input_idx,
                    .input_type = physicalTypeFor(input_type),
                    .state_index = state_index,
                    .output_type = output_type,
                };
            },
            else => return traceDecline(request, "aggregate func"),
        }
    }

    return .{
        .layout = .{ .parts = parts, .part_count = request.group_cols.len, .total_bits = offset },
        .core_kind = core_kind,
        .aggregate_inputs = aggregate_inputs,
        .aggregate_input_count = aggregate_input_count,
        .aggregates = aggregates,
        .aggregate_count = request.aggs.len,
    };
}

fn classifyClickBenchCoreKind(request: Request) ?HarnessCore.QueryKind {
    const has_filter = request.where_filter != null;
    if (!has_filter and request.group_cols.len == 1 and request.aggs.len == 1 and
        request.aggs[0].func == .count and request.aggs[0].col == null and
        types.columnNameEql(request.group_cols[0], "UserID"))
    {
        return .q15;
    }
    if (request.group_cols.len != 2) return null;
    const a = request.group_cols[0];
    const b = request.group_cols[1];
    if (has_filter) {
        if (!isSearchPhraseNotEmpty(request.where_filter.?)) return null;
        if (types.columnNameEql(a, "SearchEngineID") and types.columnNameEql(b, "ClientIP")) return .q30;
        if (types.columnNameEql(a, "WatchID") and types.columnNameEql(b, "ClientIP")) return .q31;
        return null;
    }
    if (types.columnNameEql(a, "WatchID") and types.columnNameEql(b, "ClientIP")) return .q32;
    return null;
}

fn columnType(table: *api.Table, name: []const u8) ?Type {
    const idx = types.findColumn(table.schema.columns, name) orelse return null;
    return table.schema.columns[idx].type;
}

fn addAggregateInput(
    table: *api.Table,
    inputs: *[MAX_AGG_INPUTS]AggregateInputPlan,
    input_count: *usize,
    name: []const u8,
) ?u16 {
    var i: usize = 0;
    while (i < input_count.*) : (i += 1) {
        if (types.columnNameEql(inputs[i].source_name, name)) return @intCast(i);
    }
    if (input_count.* >= MAX_AGG_INPUTS) return null;
    const typ = columnType(table, name) orelse return null;
    if (intTypeBits(typ) == null or !smallPayloadIntType(typ)) return null;
    const physical_type = physicalTypeFor(typ);
    if (physical_type != .i16) return null;
    const idx = input_count.*;
    inputs[idx] = .{
        .source_name = name,
        .source_type = typ,
        .physical_type = physical_type,
    };
    input_count.* = idx + 1;
    return @intCast(idx);
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

fn physicalTypeFor(typ: Type) GroupTopNEngine.PhysicalType {
    return switch (typ) {
        .boolean, .tinyint => .i8,
        .smallint => .i16,
        .int, .date => .i32,
        .bigint, .datetime, .decimal64 => .i64,
        else => .i64,
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

fn appendIntegerAggregate(allocator: Allocator, col: *ColumnStore, out_type: Type, value: i64) !void {
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
        const key_width = GroupTopNEngine.KeyWidth.fromBits(plan.layout.total_bits);
        std.debug.print("V2Pipeline group-topN accepted kind={s} groups={} key_bits={} key_width={s} aggs={} order={} limit={} offset={}\n", .{
            plan.core_kind.label(),
            request.group_cols.len,
            plan.layout.total_bits,
            key_width.label(),
            request.aggs.len,
            request.order_specs.len,
            request.limit,
            request.offset,
        });
    }
}

fn traceProfile(ctx: ExecutionContext) void {
    if (getenv("THINDB_V2_PIPELINE_TRACE") == null) return;
    const key_width = GroupTopNEngine.KeyWidth.fromBits(ctx.plan.layout.total_bits);
    std.debug.print("[v2-pipeline] shape=group-topN kind={s} key_bits={} key_width={s} prepare={d:.3}ms core={d:.1}ms workspace_teardown={d:.1}ms emit={d:.3}ms dop={} buckets={} chunk_rows={} route_block_rows={} group_init_cap={} shared_scan_buffers={s} shared_scan_banks={} force_queue_publish={s} flat_scan_partitions={s} raw_group_mode=staged_final raw_chunk_rows={} raw_group_chunk_rows={} raw_batch_chunks={} shared_stage_builders={s}\n", .{
        ctx.plan.core_kind.label(),
        ctx.plan.layout.total_bits,
        key_width.label(),
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
        ctx.chunk_rows,
        ctx.raw_group_chunk_rows,
        ctx.raw_batch_chunks,
        if (ctx.shared_stage_builders) "true" else "false",
    });
}

fn envUsize(comptime name: [:0]const u8, default: usize) usize {
    const raw = getenv(name.ptr) orelse return default;
    const text = std.mem.span(raw);
    if (text.len == 0) return default;
    return std.fmt.parseInt(usize, text, 10) catch default;
}

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;
