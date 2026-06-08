//! Minimal V2 shape: table scan -> optional fused filter -> grouped aggregate
//! -> ORDER BY aggregate DESC -> LIMIT/OFFSET.
//!
//! This is deliberately shaped as a pipeline instead of an operator chain. The
//! first implementation reuses the proven ClickBench group-topN core for Q30-Q32
//! while keeping the public structure ready for a native reusable workspace.

const std = @import("std");
const Allocator = std.mem.Allocator;
const build_options = @import("build_options");

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
const compute = @import("compute.zig");
const Scan = @import("scan.zig").Scan;
const LateScan = @import("latescan.zig").LateScan;
const SingleBatchSource = @import("single_batch.zig").SingleBatchSource;
const transform = @import("../engine/engine.zig").transform;

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
// Aggregate-count ceiling. The per-group accumulator store in the silo core is
// a runtime variable-stride slab, so this is just a generous planner bound (and
// the inline ceiling on the transient result row), not a per-group memory cost.
const MAX_AGGS: usize = 16;
const MAX_AGG_INPUTS: usize = 16;
const MAX_STRING_AGG_INPUTS: usize = 2;
const MAX_STRING_AGG_SLOTS: usize = 2;

pub const Request = struct {
    group_cols: []const []const u8,
    aggs: []const AggSpec,
    order_specs: []const SortSpec,
    limit: usize,
    offset: usize,
    where_filter: ?PredicateExpr,
    having_filter: ?PredicateExpr,
    needed: ?[]const []const u8,
    // Row-local derived columns (e.g. ClientIP - 1, DATE_TRUNC('minute',
    // EventTime), length(URL)). A group key or aggregate input may name one of
    // these; the scan layer wraps in a Compute operator so the derived column
    // appears in each batch by name. Empty for plain column shapes.
    derived: []const compute.Derived = &.{},
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
    // String MIN/MAX: the result is carried in TopRow.str[str_state_index],
    // sourced from the scan-projected string column string_aggregate_inputs[
    // str_input_index]. Numeric state_index/input are unused when is_string.
    is_string: bool = false,
    str_input_index: u16 = 0,
    str_state_index: u16 = 0,
};

const StringAggInputPlan = struct {
    source_name: []const u8,
};

const ShapePlan = struct {
    layout: KeyLayout,
    aggregate_inputs: [MAX_AGG_INPUTS]AggregateInputPlan,
    aggregate_input_count: usize,
    string_aggregate_inputs: [MAX_STRING_AGG_INPUTS]StringAggInputPlan = undefined,
    string_aggregate_input_count: usize = 0,
    aggregates: [MAX_AGGS]AggregatePlan,
    aggregate_count: usize,
    // The key is a hash (string keys, or integer combos wider than 128 bits);
    // real key values are late-materialized from each row's __rowloc at emit.
    hashed: bool = false,
    // Derived columns evaluated by the scan-layer Compute. Borrowed from the IR.
    derived: []const compute.Derived = &.{},
    // Base columns the scan must project to back the keys, aggregate inputs, and
    // derived expressions. Owned by the pipeline. Replaces the by-name key/agg
    // projection so derived names (not real columns) never reach the scan.
    scan_base_columns: []const []const u8 = &.{},
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
        // String MIN/MAX results are re-dup'd into this allocator at emit; free
        // them here. Numeric rows carry only empty slices (free is a no-op).
        for (self.items) |row| {
            for (row.str) |s| if (s.len > 0) allocator.free(s);
        }
        if (self.items.len > 0) allocator.free(self.items);
        self.* = .{ .allocator = allocator };
    }
};

const FinalRows = struct {
    allocator: Allocator,
    items: []HarnessCore.TopRow = &.{},
    owned: bool = false,

    fn deinit(self: *FinalRows) void {
        if (self.owned and self.items.len > 0) self.allocator.free(self.items);
        self.* = .{ .allocator = self.allocator };
    }
};

pub fn tryBuild(allocator: Allocator, table: *api.Table, request: Request) !?Query {
    // Base columns the scan projects to back keys, aggregate inputs, and the
    // derived expressions that feed them. Derived key/agg names are produced by
    // the Compute layer, never projected directly.
    var base_cols: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer base_cols.deinit(allocator);
    if (!try collectScanBaseColumns(allocator, &base_cols, table, request)) {
        base_cols.deinit(allocator);
        return null;
    }

    // Resolve derived key/aggregate-input types from the Compute output schema.
    // The probe fetches no rows — it only exposes the post-Compute schema, then
    // is torn down.
    var probe_q: ?Query = null;
    defer if (probe_q) |*q| q.deinit();
    var probe_schema: ?[]const Column = null;
    if (request.derived.len > 0) {
        const scan = try Scan.allocWithProjectionLoc(allocator, table, null, base_cols.items, false, null);
        var sq = exec.makeQuery(allocator, scan);
        probe_q = sq.compute(request.derived) catch |e| {
            sq.deinit();
            return e;
        };
        probe_schema = probe_q.?.outputSchema();
    }

    var plan = validateShape(table, request, probe_schema) orelse {
        base_cols.deinit(allocator);
        return null;
    };
    plan.derived = request.derived;
    plan.scan_base_columns = try base_cols.toOwnedSlice(allocator);
    errdefer allocator.free(plan.scan_base_columns);

    traceAccepted(request, plan);
    const op = try allocator.create(GroupTopNPipeline);
    errdefer allocator.destroy(op);
    op.* = try GroupTopNPipeline.init(allocator, table, request, plan);
    return exec.makeQuery(allocator, op);
}

fn findDerived(derived: []const compute.Derived, name: []const u8) ?compute.Derived {
    for (derived) |d| if (types.columnNameEql(d.name, name)) return d;
    return null;
}

fn appendUniqueCol(allocator: Allocator, out: *std.ArrayListUnmanaged([]const u8), name: []const u8) !void {
    if (name.len == 0) return;
    for (out.items) |existing| if (types.columnNameEql(existing, name)) return;
    try out.append(allocator, name);
}

// Project the base columns that back every group key and aggregate input: a
// real table column is projected as-is; a derived name expands to the base
// columns its expression reads. Returns false (decline) on a name that is
// neither a table column nor a known derived column.
fn collectScanBaseColumns(allocator: Allocator, out: *std.ArrayListUnmanaged([]const u8), table: *api.Table, request: Request) !bool {
    for (request.group_cols) |name| {
        if (types.findColumn(table.schema.columns, name) != null) {
            try appendUniqueCol(allocator, out, name);
        } else if (findDerived(request.derived, name)) |d| {
            try compute.collectColumnRefs(allocator, out, d.expr);
        } else return false;
    }
    for (request.aggs) |agg| {
        const name = agg.col orelse continue;
        if (types.findColumn(table.schema.columns, name) != null) {
            try appendUniqueCol(allocator, out, name);
        } else if (findDerived(request.derived, name)) |d| {
            try compute.collectColumnRefs(allocator, out, d.expr);
        } else return false;
    }
    return true;
}

fn resolveColumnType(table: *api.Table, schema: ?[]const Column, name: []const u8) ?Type {
    if (types.findColumn(table.schema.columns, name)) |idx| return table.schema.columns[idx].type;
    if (schema) |s| for (s) |c| if (types.columnNameEql(c.name, name)) return c.type;
    return null;
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
        if (self.plan.scan_base_columns.len > 0) self.allocator.free(self.plan.scan_base_columns);
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
        const grouped = if (self.request.having_filter) |hexpr| applyHaving(self, hexpr, rows.items) else rows.items;
        var final_rows = try prepareFinalRows(self, grouped);
        defer final_rows.deinit();
        try emitResultStage(self, final_rows.items);
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
    var string_aggregate_inputs: [MAX_STRING_AGG_INPUTS]GroupTopNEngine.StringAggInput = undefined;
    i = 0;
    while (i < ctx.plan.string_aggregate_input_count) : (i += 1) {
        string_aggregate_inputs[i] = .{ .source_name = ctx.plan.string_aggregate_inputs[i].source_name };
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
            .is_string = agg_plan.is_string,
            .str_input_index = agg_plan.str_input_index,
            .str_state_index = agg_plan.str_state_index,
        };
    }
    var group_key_inputs: [MAX_GROUP_KEYS]GroupTopNEngine.GroupKeyInput = undefined;
    i = 0;
    while (i < ctx.plan.layout.part_count) : (i += 1) {
        const part = ctx.plan.layout.parts[i];
        group_key_inputs[i] = .{
            .name = part.name,
            .source_type = part.typ,
            .offset_bits = part.offset_bits,
            .width_bits = part.width_bits,
        };
    }
    // The scan projects the precomputed base columns (keys, aggregate inputs,
    // and the base columns their derived expressions read). The derived columns
    // themselves are added by the Compute layer the engine wraps the scan in;
    // any filter-only columns are sourced by the fused filter itself.
    const scan_columns = ctx.plan.scan_base_columns;

    var result = try GroupTopNEngine.run(core_allocator, .{
        .table = ctx.table,
        .shape = .{
            .key_width = if (ctx.plan.hashed) .u128 else GroupTopNEngine.KeyWidth.fromBits(ctx.plan.layout.total_bits),
            .key_bits = ctx.plan.layout.total_bits,
            .group_key_count = ctx.plan.layout.part_count,
            .group_key_inputs = group_key_inputs[0..ctx.plan.layout.part_count],
            .aggregate_inputs = aggregate_inputs[0..ctx.plan.aggregate_input_count],
            .string_aggregate_inputs = string_aggregate_inputs[0..ctx.plan.string_aggregate_input_count],
            .aggregate_program = aggregate_program[0..ctx.plan.aggregate_count],
            .has_filter = ctx.request.where_filter != null,
            .order_by_count_desc = true,
            .limit = ctx.request.limit,
            .offset = ctx.request.offset,
            .emit_all_groups = !canUseCoreCountDescTopN(ctx),
            .emit_all_groups_cap = unorderedLimitCap(ctx),
            // When the unordered-LIMIT cap is active and a HAVING is present,
            // the core counts only HAVING survivors toward the cap.
            .emit_filter = if (unorderedLimitCap(ctx) != 0 and ctx.request.having_filter != null)
                HarnessCore.EmitFilter{ .ctx = ctx, .pass = havingEmitPass }
            else
                null,
            .hashed = ctx.plan.hashed,
        },
        .params = params,
        .scan_columns = scan_columns,
        .derived = ctx.plan.derived,
        .filter_expr = ctx.request.where_filter,
    });

    ctx.times.run_core_ticks = exec.prof.nowTicks() - t0 - result.times.workspace_teardown_ticks;
    ctx.times.workspace_teardown_ticks = result.times.workspace_teardown_ticks;
    const rows = result.rows;
    result.rows = &.{};
    return .{ .allocator = result.allocator, .items = rows };
}

fn canUseCoreCountDescTopN(ctx: *const ExecutionContext) bool {
    // HAVING reduces the group set before LIMIT, so the core must emit every
    // group; the top-N is then taken after applyHaving in prepareFinalRows.
    if (ctx.request.having_filter != null) return false;
    if (ctx.request.order_specs.len != 1) return false;
    if (!ctx.request.order_specs[0].desc) return false;
    if (ctx.request.limit == 0) return false;
    if (ctx.request.limit + ctx.request.offset > 10) return false;
    for (ctx.plan.aggregates[0..ctx.plan.aggregate_count]) |agg| {
        if (agg.func == .count and types.columnNameEql(agg.name, ctx.request.order_specs[0].col)) return true;
    }
    return false;
}

// A `LIMIT N` with no ORDER BY accepts any N+offset groups, so the core can
// stop emitting once it has that many instead of materializing every group
// (and string-dup'ing every key) only to slice the first N. With HAVING the
// cap counts only groups that pass the predicate — the core applies HAVING in
// the emit loop via `emit_filter` (see `havingEmitPass`), so any N+offset
// survivors still satisfy the unordered LIMIT.
fn unorderedLimitCap(ctx: *const ExecutionContext) usize {
    if (ctx.request.order_specs.len != 0) return 0;
    if (ctx.request.limit == 0) return 0;
    return ctx.request.limit + ctx.request.offset;
}

fn prepareFinalRows(op: *GroupTopNPipeline, rows: []HarnessCore.TopRow) !FinalRows {
    if (rows.len == 0) return .{ .allocator = op.allocator };
    if (op.request.order_specs.len == 0) {
        const start = @min(op.request.offset, rows.len);
        const end = limitEnd(start, rows.len, op.request.limit);
        return .{ .allocator = op.allocator, .items = rows[start..end] };
    }

    if (op.request.limit == 0) {
        std.mem.sort(HarnessCore.TopRow, rows, op, finalRowLess);
        return .{ .allocator = op.allocator, .items = rows };
    }

    const keep = @min(rows.len, op.request.limit + op.request.offset);
    if (keep == 0) return .{ .allocator = op.allocator };
    var candidates = try op.allocator.alloc(HarnessCore.TopRow, keep);
    errdefer op.allocator.free(candidates);
    var len: usize = 0;
    var worst_i: usize = 0;
    for (rows) |row| {
        if (len < keep) {
            candidates[len] = row;
            if (len == 0 or finalRowLess(op, candidates[worst_i], row)) worst_i = len;
            len += 1;
            continue;
        }
        if (!finalRowLess(op, row, candidates[worst_i])) continue;
        candidates[worst_i] = row;
        worst_i = 0;
        var i: usize = 1;
        while (i < len) : (i += 1) {
            if (finalRowLess(op, candidates[worst_i], candidates[i])) worst_i = i;
        }
    }
    std.mem.sort(HarnessCore.TopRow, candidates[0..len], op, finalRowLess);
    const start = @min(op.request.offset, len);
    const end = limitEnd(start, len, op.request.limit);
    const emit_len = end - start;
    if (start != 0 and emit_len != 0) std.mem.copyForwards(HarnessCore.TopRow, candidates[0..emit_len], candidates[start..end]);
    return .{ .allocator = op.allocator, .items = candidates[0..emit_len], .owned = true };
}

fn limitEnd(start: usize, len: usize, limit: usize) usize {
    if (limit == 0) return len;
    return @min(len, start + limit);
}

fn finalRowLess(op: *GroupTopNPipeline, a: HarnessCore.TopRow, b: HarnessCore.TopRow) bool {
    for (op.request.order_specs) |spec| {
        const cmp = compareOutputValue(op, spec.col, a, b) catch 0;
        if (cmp == 0) continue;
        return if (spec.desc) cmp > 0 else cmp < 0;
    }
    return a.key < b.key;
}

fn compareOutputValue(op: *GroupTopNPipeline, name: []const u8, a: HarnessCore.TopRow, b: HarnessCore.TopRow) !i8 {
    for (op.plan.layout.parts[0..op.plan.layout.part_count]) |part| {
        if (types.columnNameEql(name, part.name)) {
            return compareI128(keyPartValue(part, a.key), keyPartValue(part, b.key));
        }
    }
    for (op.plan.aggregates[0..op.plan.aggregate_count]) |agg_plan| {
        if (types.columnNameEql(name, agg_plan.name)) {
            return compareAggregateValue(agg_plan, a, b);
        }
    }
    return error.UnsupportedOrderBy;
}

fn compareAggregateValue(agg_plan: AggregatePlan, a: HarnessCore.TopRow, b: HarnessCore.TopRow) i8 {
    const float_input = isFloatPhysical(agg_plan.input_type);
    return switch (agg_plan.func) {
        .count => compareU64(a.count, b.count),
        .sum, .min, .max => if (float_input)
            compareF64(rowStateFloat(a, agg_plan.state_index), rowStateFloat(b, agg_plan.state_index))
        else
            compareI128(rowStateValue(a, agg_plan.state_index), rowStateValue(b, agg_plan.state_index)),
        .avg => if (float_input)
            compareF64(avgFloatValue(a, agg_plan.state_index), avgFloatValue(b, agg_plan.state_index))
        else
            compareF64(avgValue(a, agg_plan.state_index), avgValue(b, agg_plan.state_index)),
        else => 0,
    };
}

fn avgValue(row: HarnessCore.TopRow, state_index: u16) f64 {
    return if (row.count == 0) 0.0 else @as(f64, @floatFromInt(rowStateValue(row, state_index))) / @as(f64, @floatFromInt(row.count));
}

fn rowStateValue(row: HarnessCore.TopRow, state_index: u16) i128 {
    if (state_index == 0) return @intCast(row.count);
    if (state_index - 1 >= row.slots.len) return 0;
    return row.slots[state_index - 1];
}

// Float aggregates carry their f64 accumulator in the i64 slot bits.
fn rowStateFloat(row: HarnessCore.TopRow, state_index: u16) f64 {
    const bits: i64 = if (state_index >= 1 and state_index - 1 < row.slots.len) row.slots[state_index - 1] else 0;
    return @bitCast(bits);
}

fn avgFloatValue(row: HarnessCore.TopRow, state_index: u16) f64 {
    return if (row.count == 0) 0.0 else rowStateFloat(row, state_index) / @as(f64, @floatFromInt(row.count));
}

fn keyPartValue(part: KeyPart, key: u128) i128 {
    const raw = (key >> @intCast(part.offset_bits)) & ((@as(u128, 1) << @intCast(part.width_bits)) - 1);
    return switch (part.typ) {
        .boolean, .tinyint => @as(i8, @bitCast(@as(u8, @truncate(raw)))),
        .smallint => @as(i16, @bitCast(@as(u16, @truncate(raw)))),
        .int, .date => @as(i32, @bitCast(@as(u32, @truncate(raw)))),
        .bigint, .datetime, .decimal64 => @as(i64, @bitCast(@as(u64, @truncate(raw)))),
        else => @intCast(raw),
    };
}

fn compareI128(a: i128, b: i128) i8 {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}

fn compareU64(a: u64, b: u64) i8 {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}

fn compareF64(a: f64, b: f64) i8 {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}

fn emitResultStage(op: *GroupTopNPipeline, rows: []const HarnessCore.TopRow) !void {
    if (op.plan.hashed or getenv("THINDB_V2_FORCE_HASH_KEY") != null) return emitResultStageHashed(op, rows);
    for (rows) |row| {
        for (op.plan.layout.parts[0..op.plan.layout.part_count], 0..) |part, i| {
            try appendKeyPart(op.allocator, &op.output_cols[i], part, row.key);
        }
        for (op.plan.aggregates[0..op.plan.aggregate_count], 0..) |agg_plan, i| {
            try appendAggregateValue(op.allocator, &op.output_cols[op.plan.layout.part_count + i], agg_plan, row);
        }
        op.row_count += 1;
    }
}

// Hashed-key emit: the group key is a hash, so the real key column values are
// recovered by late-materializing each survivor's carried __rowloc against the
// base table (reusing LateScan's per-(segment,row-group) single-row reader).
// Aggregates still come straight from the grouped TopRow. Single-threaded, per
// the scan/staging/group-only-parallel policy; survivors are bounded by LIMIT
// for top-N queries.
fn emitResultStageHashed(op: *GroupTopNPipeline, rows: []const HarnessCore.TopRow) !void {
    if (rows.len == 0) return;
    const allocator = op.allocator;
    const part_count = op.plan.layout.part_count;

    // Projection to late-materialize: a plain key is its own base column; a
    // derived key has no stored column, so materialize the columns its
    // expression reads and recompute it below.
    var base_names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer base_names.deinit(allocator);
    var derived_keys: [MAX_GROUP_KEYS]compute.Derived = undefined;
    var n_derived: usize = 0;
    for (op.plan.layout.parts[0..part_count]) |part| {
        if (findDerived(op.request.derived, part.name)) |d| {
            try compute.collectColumnRefs(allocator, &base_names, d.expr);
            derived_keys[n_derived] = d;
            n_derived += 1;
        } else {
            try appendUniqueCol(allocator, &base_names, part.name);
        }
    }
    const names = base_names.items;

    const scan_ptr = try Scan.allocWithProjectionLoc(allocator, op.table, null, names, false, null);
    var inner = exec.makeQuery(allocator, scan_ptr);
    var late_built = false;
    errdefer if (!late_built) inner.deinit();
    var late_q = try LateScan.create(allocator, inner, scan_ptr, op.table, names);
    late_built = true;
    defer late_q.deinit();
    const late: *LateScan = @ptrCast(@alignCast(late_q.ptr));

    const locs = try allocator.alloc(i64, rows.len);
    defer allocator.free(locs);
    for (rows, 0..) |row, i| locs[i] = row.rowref;

    try late.materializeInto(locs, scan_ptr.memtableSnap());
    const materialized = late.outputColumns();

    // Recompute any derived key columns over the materialized source rows with
    // the standard Compute operator; the result batch then carries every key
    // column (plain + derived) by name.
    const mat_views = try allocator.alloc(ColumnView, materialized.columns.len);
    defer allocator.free(mat_views);
    for (materialized.columns, 0..) |*c, i| mat_views[i] = c.view();
    const mat_batch = Batch{ .schema = materialized.schema, .values = mat_views, .row_count = rows.len };

    var compute_q: ?Query = null;
    defer if (compute_q) |*q| q.deinit();
    var result = mat_batch;
    if (n_derived > 0) {
        const src = try SingleBatchSource.create(allocator, mat_batch);
        compute_q = try compute.Compute.create(allocator, src, derived_keys[0..n_derived]);
        result = (try compute_q.?.next()) orelse return error.UnsupportedQueryShape;
    }

    const idx_buf = try allocator.alloc(u32, rows.len);
    defer allocator.free(idx_buf);
    for (0..rows.len) |i| idx_buf[i] = @intCast(i);

    for (op.plan.layout.parts[0..part_count], 0..) |part, j| {
        const ci = types.findColumn(result.schema, part.name) orelse return error.UnsupportedQueryShape;
        try transform.appendByIndices(allocator, result.values[ci], idx_buf, &op.output_cols[j]);
    }
    for (rows) |row| {
        for (op.plan.aggregates[0..op.plan.aggregate_count], 0..) |agg_plan, i| {
            try appendAggregateValue(allocator, &op.output_cols[part_count + i], agg_plan, row);
        }
    }
    op.row_count += rows.len;
}

fn appendStringAggregate(allocator: Allocator, col: *ColumnStore, out_type: Type, bytes: []const u8) !void {
    switch (out_type) {
        .varchar => try col.data.varchar.appendValue(allocator, bytes),
        .string => try col.data.string.appendValue(allocator, bytes),
        .char => try col.data.char.appendValue(allocator, bytes),
        else => return error.TypeMismatch,
    }
}

fn appendAggregateValue(allocator: Allocator, col: *ColumnStore, agg_plan: AggregatePlan, row: HarnessCore.TopRow) !void {
    if (agg_plan.is_string) {
        return appendStringAggregate(allocator, col, agg_plan.output_type, row.str[agg_plan.str_state_index]);
    }
    const float_input = isFloatPhysical(agg_plan.input_type);
    switch (agg_plan.func) {
        .count => try col.data.bigint.append(allocator, @intCast(row.count)),
        .sum, .min, .max => if (float_input)
            try appendFloatAggregate(allocator, col, agg_plan.output_type, rowStateFloat(row, agg_plan.state_index))
        else
            try appendIntegerAggregate(allocator, col, agg_plan.output_type, rowStateValue(row, agg_plan.state_index)),
        .avg => try col.data.double.append(
            allocator,
            if (float_input) avgFloatValue(row, agg_plan.state_index) else avgValue(row, agg_plan.state_index),
        ),
        else => return error.UnsupportedOperatorForType,
    }
}

fn validateShape(table: *api.Table, request: Request, schema: ?[]const Column) ?ShapePlan {
    if (request.group_cols.len == 0 or request.group_cols.len > MAX_GROUP_KEYS) return traceDecline(request, "group key count");
    if (request.aggs.len == 0 or request.aggs.len > MAX_AGGS) return traceDecline(request, "aggregate count");
    // Decide packed vs hashed: integer keys totalling ≤128 bits bit-pack
    // losslessly into the staged key (exact, reversible at emit). A string key
    // or an integer combo wider than 128 bits can't pack, so the key becomes a
    // 128-bit hash and the real values are late-materialized from each row's
    // __rowloc at emit.
    var hashed = false;
    var total_bits: u32 = 0;
    for (request.group_cols) |name| {
        const typ = resolveColumnType(table, schema, name) orelse return traceDecline(request, "group key column");
        if (intTypeBits(typ)) |width| {
            total_bits += width;
        } else if (isStringKeyType(typ)) {
            hashed = true;
        } else {
            return traceDecline(request, "group key type");
        }
    }
    if (total_bits > 128) hashed = true;

    // A derived key in a hashed layout has no stored column to read back at
    // emit. `emitResultStageHashed` recovers it the same way it recovers any
    // key — late-materialize from the survivor's rowref — but materializes the
    // derived expression's SOURCE columns and recomputes the expression over
    // them (only the ≤LIMIT survivors).

    var parts: [MAX_GROUP_KEYS]KeyPart = undefined;
    var offset: u8 = 0;
    for (request.group_cols, 0..) |name, i| {
        const typ = resolveColumnType(table, schema, name).?;
        if (hashed) {
            parts[i] = .{ .name = name, .typ = typ, .offset_bits = 0, .width_bits = 0 };
        } else {
            const width = intTypeBits(typ).?;
            parts[i] = .{ .name = name, .typ = typ, .offset_bits = offset, .width_bits = width };
            offset += width;
        }
    }
    if (hashed) offset = 128;

    var aggregate_inputs: [MAX_AGG_INPUTS]AggregateInputPlan = undefined;
    var aggregate_input_count: usize = 0;
    var string_aggregate_inputs: [MAX_STRING_AGG_INPUTS]StringAggInputPlan = undefined;
    var string_aggregate_input_count: usize = 0;
    var aggregates: [MAX_AGGS]AggregatePlan = undefined;
    var next_numeric_state_index: u16 = 1;
    var next_string_state_index: u16 = 0;
    for (request.aggs, 0..) |agg, agg_i| {
        if (agg_i >= MAX_AGGS) return traceDecline(request, "aggregate count");
        switch (agg.func) {
            .count => {
                const input_column_index = if (agg.col) |col_name| blk: {
                    const input_idx = addAggregateInput(table, schema, &aggregate_inputs, &aggregate_input_count, col_name) orelse return traceDecline(request, "count input");
                    break :blk input_idx;
                } else null;
                aggregates[agg_i] = .{
                    .name = agg.as,
                    .func = agg.func,
                    .input_column_index = input_column_index,
                    .input_type = .u64,
                    .state_index = 0,
                    .output_type = aggregate.aggOutputTypeFor(agg, if (agg.col) |col_name| resolveColumnType(table, schema, col_name) orelse return traceDecline(request, "count input type") else null) catch return null,
                };
            },
            .sum, .avg, .min, .max => {
                const col_name = agg.col orelse return traceDecline(request, "aggregate column");
                const input_type = resolveColumnType(table, schema, col_name) orelse return traceDecline(request, "aggregate type");
                if ((agg.func == .min or agg.func == .max) and isStringKeyType(input_type)) {
                    if (next_string_state_index >= MAX_STRING_AGG_SLOTS) return traceDecline(request, "string aggregate slot count");
                    const str_input_idx = addStringAggInput(&string_aggregate_inputs, &string_aggregate_input_count, col_name) orelse return traceDecline(request, "string aggregate input");
                    aggregates[agg_i] = .{
                        .name = agg.as,
                        .func = agg.func,
                        .input_column_index = null,
                        .input_type = .i64,
                        .state_index = 0,
                        .output_type = input_type,
                        .is_string = true,
                        .str_input_index = str_input_idx,
                        .str_state_index = next_string_state_index,
                    };
                    next_string_state_index += 1;
                    continue;
                }
                // SUM/AVG over a 64-bit integer accumulates into i128 (the result
                // widens to LARGEINT). The silo's per-group slots are i64, so
                // route these to the generic i128 accumulator path instead of
                // overflowing here. MIN/MAX over a 64-bit int is fine — it holds a
                // single value, never grows. Float SUM/AVG (f64) is also fine.
                if ((agg.func == .sum or agg.func == .avg) and physicalTypeFor(input_type) == .i64) {
                    return traceDecline(request, "64-bit sum/avg needs i128 accumulator");
                }
                if (next_numeric_state_index > MAX_AGGS) return traceDecline(request, "aggregate state count");
                const input_idx = addAggregateInput(table, schema, &aggregate_inputs, &aggregate_input_count, col_name) orelse return traceDecline(request, "aggregate input");
                const output_type = aggregate.aggOutputTypeFor(agg, input_type) catch return null;
                if (agg.func == .avg and output_type != .double) return traceDecline(request, "avg output type");
                aggregates[agg_i] = .{
                    .name = agg.as,
                    .func = agg.func,
                    .input_column_index = input_idx,
                    .input_type = physicalTypeFor(input_type),
                    .state_index = next_numeric_state_index,
                    .output_type = output_type,
                };
                next_numeric_state_index += 1;
            },
            else => return traceDecline(request, "aggregate func"),
        }
    }
    for (request.order_specs) |spec| {
        if (!outputColumnExists(parts[0..request.group_cols.len], aggregates[0..request.aggs.len], spec.col)) {
            return traceDecline(request, "order key");
        }
        // Sorting on a string MIN/MAX result isn't wired through the final
        // top-N comparator yet; such queries order by COUNT in practice.
        for (aggregates[0..request.aggs.len]) |agg_plan| {
            if (agg_plan.is_string and types.columnNameEql(agg_plan.name, spec.col)) {
                return traceDecline(request, "order by string aggregate");
            }
        }
    }
    if (request.having_filter) |hexpr| {
        if (!havingExprSupported(parts[0..request.group_cols.len], aggregates[0..request.aggs.len], hexpr)) {
            return traceDecline(request, "having");
        }
    }

    return .{
        .layout = .{ .parts = parts, .part_count = request.group_cols.len, .total_bits = offset },
        .aggregate_inputs = aggregate_inputs,
        .aggregate_input_count = aggregate_input_count,
        .string_aggregate_inputs = string_aggregate_inputs,
        .string_aggregate_input_count = string_aggregate_input_count,
        .aggregates = aggregates,
        .aggregate_count = request.aggs.len,
        .hashed = hashed,
    };
}

fn isStringKeyType(t: Type) bool {
    return switch (t) {
        .varchar, .string, .char => true,
        else => false,
    };
}

fn outputColumnExists(parts: []const KeyPart, aggregates: []const AggregatePlan, name: []const u8) bool {
    for (parts) |part| if (types.columnNameEql(part.name, name)) return true;
    for (aggregates) |agg| if (types.columnNameEql(agg.name, name)) return true;
    return false;
}

// HAVING runs single-threaded after grouping, before ORDER BY / LIMIT: each
// grouped row's output values are tested against the predicate and failing
// groups are dropped. Only column-vs-constant comparisons over output columns
// (group keys + aggregate results) are supported; richer predicate forms are
// declined at validateShape so we never reach the evaluator with one.
const Num = union(enum) { i: i128, f: f64 };

fn valueAsNum(v: types.Value) ?Num {
    return switch (v) {
        .int => |x| .{ .i = x },
        .bigint => |x| .{ .i = x },
        .smallint => |x| .{ .i = x },
        .tinyint => |x| .{ .i = x },
        .largeint => |x| .{ .i = x },
        .date => |x| .{ .i = x },
        .datetime => |x| .{ .i = x },
        .decimal64 => |x| .{ .i = x },
        .decimal128 => |x| .{ .i = x },
        .boolean => |b| .{ .i = @intFromBool(b) },
        .float => |x| .{ .f = x },
        .double => |x| .{ .f = x },
        .text, .uuid => null,
    };
}

fn numOrder(a: Num, b: Num) std.math.Order {
    if (a == .f or b == .f) {
        const af: f64 = switch (a) {
            .i => |v| @floatFromInt(v),
            .f => |v| v,
        };
        const bf: f64 = switch (b) {
            .i => |v| @floatFromInt(v),
            .f => |v| v,
        };
        return std.math.order(af, bf);
    }
    return std.math.order(a.i, b.i);
}

fn havingExprSupported(parts: []const KeyPart, aggregates: []const AggregatePlan, expr: PredicateExpr) bool {
    return switch (expr) {
        .leaf => |p| outputColumnExists(parts, aggregates, p.col) and valueAsNum(p.val) != null,
        .is_null, .is_not_null => |name| outputColumnExists(parts, aggregates, name),
        .not => |child| havingExprSupported(parts, aggregates, child.*),
        .@"and", .@"or" => |children| {
            for (children) |child| if (!havingExprSupported(parts, aggregates, child)) return false;
            return true;
        },
        else => false,
    };
}

fn havingOutputNum(plan: *const ShapePlan, name: []const u8, row: HarnessCore.TopRow) ?Num {
    for (plan.layout.parts[0..plan.layout.part_count]) |part| {
        if (types.columnNameEql(name, part.name)) return .{ .i = keyPartValue(part, row.key) };
    }
    for (plan.aggregates[0..plan.aggregate_count]) |agg_plan| {
        if (types.columnNameEql(name, agg_plan.name)) {
            const float_input = isFloatPhysical(agg_plan.input_type);
            return switch (agg_plan.func) {
                .count => .{ .i = @intCast(row.count) },
                .sum, .min, .max => if (float_input)
                    Num{ .f = rowStateFloat(row, agg_plan.state_index) }
                else
                    Num{ .i = rowStateValue(row, agg_plan.state_index) },
                .avg => .{ .f = if (float_input) avgFloatValue(row, agg_plan.state_index) else avgValue(row, agg_plan.state_index) },
                else => null,
            };
        }
    }
    return null;
}

fn havingPasses(plan: *const ShapePlan, expr: PredicateExpr, row: HarnessCore.TopRow) bool {
    return switch (expr) {
        .leaf => |p| {
            const lhs = havingOutputNum(plan, p.col, row) orelse return false;
            const rhs = valueAsNum(p.val) orelse return false;
            const ord = numOrder(lhs, rhs);
            return switch (p.op) {
                .eq => ord == .eq,
                .neq => ord != .eq,
                .lt => ord == .lt,
                .lte => ord != .gt,
                .gt => ord == .gt,
                .gte => ord != .lt,
            };
        },
        .is_null => false,
        .is_not_null => true,
        .not => |child| !havingPasses(plan, child.*, row),
        .@"and" => |children| {
            for (children) |child| if (!havingPasses(plan, child, row)) return false;
            return true;
        },
        .@"or" => |children| {
            for (children) |child| if (havingPasses(plan, child, row)) return true;
            return false;
        },
        else => true,
    };
}

// Callback bridge so the core's all-groups emit can apply HAVING per group
// (before the string key is materialized) and count only survivors toward the
// cap. `ctx` is the owning pipeline; its plan + request supply the predicate.
fn havingEmitPass(ctx: ?*anyopaque, row: HarnessCore.TopRow) bool {
    const ec: *const ExecutionContext = @ptrCast(@alignCast(ctx.?));
    const expr = ec.request.having_filter orelse return true;
    return havingPasses(&ec.plan, expr, row);
}

fn applyHaving(op: *GroupTopNPipeline, expr: PredicateExpr, rows: []HarnessCore.TopRow) []HarnessCore.TopRow {
    var w: usize = 0;
    for (rows) |row| {
        if (havingPasses(&op.plan, expr, row)) {
            rows[w] = row;
            w += 1;
        }
    }
    return rows[0..w];
}

fn addAggregateInput(
    table: *api.Table,
    schema: ?[]const Column,
    inputs: *[MAX_AGG_INPUTS]AggregateInputPlan,
    input_count: *usize,
    name: []const u8,
) ?u16 {
    var i: usize = 0;
    while (i < input_count.*) : (i += 1) {
        if (types.columnNameEql(inputs[i].source_name, name)) return @intCast(i);
    }
    if (input_count.* >= MAX_AGG_INPUTS) return null;
    const typ = resolveColumnType(table, schema, name) orelse return null;
    if (!aggInputSupported(typ)) return null;
    const physical_type = physicalTypeFor(typ);
    const idx = input_count.*;
    inputs[idx] = .{
        .source_name = name,
        .source_type = typ,
        .physical_type = physical_type,
    };
    input_count.* = idx + 1;
    return @intCast(idx);
}

fn addStringAggInput(
    inputs: *[MAX_STRING_AGG_INPUTS]StringAggInputPlan,
    input_count: *usize,
    name: []const u8,
) ?u16 {
    var i: usize = 0;
    while (i < input_count.*) : (i += 1) {
        if (types.columnNameEql(inputs[i].source_name, name)) return @intCast(i);
    }
    if (input_count.* >= MAX_STRING_AGG_INPUTS) return null;
    const idx = input_count.*;
    inputs[idx] = .{ .source_name = name };
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

fn aggInputSupported(typ: Type) bool {
    return switch (typ) {
        .boolean, .tinyint, .smallint, .int, .bigint, .float, .double => true,
        else => false,
    };
}

fn physicalTypeFor(typ: Type) GroupTopNEngine.PhysicalType {
    return switch (typ) {
        .boolean, .tinyint => .i8,
        .smallint => .i16,
        .int, .date => .i32,
        .bigint, .datetime, .decimal64 => .i64,
        .float => .f32,
        .double => .f64,
        else => .i64,
    };
}

fn isFloatPhysical(pt: GroupTopNEngine.PhysicalType) bool {
    return pt == .f32 or pt == .f64;
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

fn appendFloatAggregate(allocator: Allocator, col: *ColumnStore, out_type: Type, value: f64) !void {
    switch (out_type) {
        .float => try col.data.float.append(allocator, @floatCast(value)),
        .double => try col.data.double.append(allocator, value),
        else => return error.TypeMismatch,
    }
}

fn appendIntegerAggregate(allocator: Allocator, col: *ColumnStore, out_type: Type, value: i128) !void {
    switch (out_type) {
        .boolean => try col.data.boolean.append(allocator, @intCast(value)),
        .tinyint => try col.data.tinyint.append(allocator, @intCast(value)),
        .smallint => try col.data.smallint.append(allocator, @intCast(value)),
        .int => try col.data.int.append(allocator, @intCast(value)),
        .bigint => try col.data.bigint.append(allocator, @intCast(value)),
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
            "generic",
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
    if (comptime !build_options.profiling) return;
    if (getenv("THINDB_V2_PIPELINE_TRACE") == null) return;
    const key_width = GroupTopNEngine.KeyWidth.fromBits(ctx.plan.layout.total_bits);
    std.debug.print("[v2-pipeline] shape=group-topN kind={s} key_bits={} key_width={s} prepare={d:.3}ms core={d:.1}ms workspace_teardown={d:.1}ms emit={d:.3}ms dop={} buckets={} chunk_rows={} route_block_rows={} group_init_cap={} shared_scan_buffers={s} shared_scan_banks={} force_queue_publish={s} flat_scan_partitions={s} raw_group_mode=staged_final raw_chunk_rows={} raw_group_chunk_rows={} raw_batch_chunks={} shared_stage_builders={s}\n", .{
        "generic",
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
