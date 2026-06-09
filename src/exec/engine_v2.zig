//! Engine V2 physical-planning boundary.
//!
//! Parser, SQL lowering, and IR stay unchanged. The local compile path hands
//! the resolved IR block here first; when V2 can build the selected physical
//! shape it returns an executable Query, otherwise the caller receives an
//! unsupported-shape error for that V2-owned path.

const std = @import("std");

const api = @import("../api/api.zig");
const types = @import("../types.zig");
const ir = @import("../ir/ir.zig");
const exec = @import("exec.zig");
const v2_pipeline = @import("v2_pipeline.zig");
const v2_global_aggregate = @import("v2_global_aggregate.zig");

pub const SourceKind = enum {
    table_scan,
    file_scan,
    materialized,
    single_row,
};

pub const Shape = enum {
    stream_scan,
    scan_topn,
    scan_full_sort,
    global_aggregate,
    group_aggregate,
    group_topn,
    group_full_sort,
    materialized_input,
};

pub const SimplePipelineSpec = struct {
    source: SourceKind,
    shape: Shape,
    has_where_filter: bool = false,
    has_having_filter: bool = false,
    has_compute: bool = false,
    has_project: bool = false,
    has_exclude: bool = false,
    has_group: bool = false,
    has_order: bool = false,
    has_limit: bool = false,
    has_alias: bool = false,
    group_key_count: usize = 0,
    aggregate_count: usize = 0,
};

pub const CompileInput = struct {
    allocator: std.mem.Allocator,
    db: *api.Database,
    session: api.Session,
    prune_names: ?[][]const u8 = null,
};

/// Try to compile a whole query block into Engine V2.
///
/// The current implementation is intentionally a no-behavior-change scaffold:
/// it classifies supported simple shapes, then declines until the concrete V2
/// builders are introduced for those shapes.
pub fn tryCompile(input: CompileInput, root: *const ir.Op) !?exec.Query {
    // Engine V2 is the default path for SELECT-shaped queries. Setting
    // THINDB_ENGINE_V1 reverts the whole compile path to the legacy engine.
    if (getenv("THINDB_ENGINE_V1") != null) return null;
    // Side-effecting statements (DDL/DML/SET/SHOW/EXPLAIN/...) are not the V2
    // engine's concern; the legacy compile path owns them.
    if (!isSelectQuery(root)) return null;
    // Every SELECT shape is now V2's responsibility: build it or fail. There is
    // deliberately no fallback to the legacy operator engine for these.
    const spec = classifySimplePipeline(root) orelse return error.UnsupportedQueryShape;
    return switch (spec.shape) {
        // All three grouped shapes share one builder: the group-topN core
        // already emits every group (no LIMIT) and sorts on demand (ORDER BY),
        // so full-sort and bare-aggregate are just the limit/order-optional
        // forms of the same pipeline.
        .group_topn, .group_full_sort, .group_aggregate => (try buildGroupTopN(input, root)) orelse return error.UnsupportedQueryShape,
        .global_aggregate => (try buildGlobalAggregate(input, root)) orelse return error.UnsupportedQueryShape,
        // Non-aggregating SELECT: filter/order/limit/project over a parallel scan.
        .stream_scan, .scan_topn, .scan_full_sort => (try buildScanSelect(input, root)) orelse return error.UnsupportedQueryShape,
        else => error.UnsupportedQueryShape,
    };
}

/// A row-producing SELECT query, as opposed to a side-effecting statement
/// (DDL/DML/SET/SHOW/EXPLAIN). Distinguished by the root op tag: queries are
/// rooted in a pipeline operator, statements in their own dedicated op.
fn isSelectQuery(op: *const ir.Op) bool {
    return switch (op.*) {
        .scan,
        .limit,
        .select,
        .exclude,
        .filter,
        .order_by,
        .group_by,
        .compute,
        .join,
        .materialize,
        .window,
        .set_union,
        .single_row,
        .file_scan,
        .alias,
        => true,
        .ddl,
        .show,
        .insert,
        .batch,
        .copy,
        .create_table_as,
        .insert_select,
        .set_var,
        .delete_op,
        .update_op,
        .explain,
        => false,
    };
}

/// Classify a single-source, no-join/no-window/no-set-op query block.
pub fn classifySimplePipeline(root: *const ir.Op) ?SimplePipelineSpec {
    var spec = SimplePipelineSpec{
        .source = undefined,
        .shape = undefined,
    };
    var saw_source = false;
    var saw_group = false;
    var op = root;

    while (true) {
        switch (op.*) {
            .limit => |l| {
                if (spec.has_limit) return null;
                spec.has_limit = true;
                op = l.upstream;
            },
            .select => |p| {
                spec.has_project = true;
                op = p.upstream;
            },
            .exclude => |p| {
                spec.has_exclude = true;
                op = p.upstream;
            },
            .filter => |f| {
                if (saw_group) {
                    spec.has_where_filter = true;
                } else {
                    spec.has_having_filter = true;
                }
                op = f.upstream;
            },
            .order_by => |o| {
                if (spec.has_order) return null;
                spec.has_order = true;
                op = o.upstream;
            },
            .group_by => |g| {
                if (saw_group) return null;
                saw_group = true;
                spec.has_group = true;
                spec.group_key_count = g.group_cols.len;
                spec.aggregate_count = g.aggs.len;
                op = g.upstream;
            },
            .compute => |c| {
                spec.has_compute = true;
                op = c.upstream;
            },
            .alias => |a| {
                spec.has_alias = true;
                op = a.upstream;
            },
            .materialize => {
                spec.source = .materialized;
                saw_source = true;
                break;
            },
            .scan => {
                spec.source = .table_scan;
                saw_source = true;
                break;
            },
            .file_scan => {
                spec.source = .file_scan;
                saw_source = true;
                break;
            },
            .single_row => {
                spec.source = .single_row;
                saw_source = true;
                break;
            },
            else => return null,
        }
    }

    if (!saw_source) return null;
    spec.shape = chooseShape(spec);
    return spec;
}

fn chooseShape(spec: SimplePipelineSpec) Shape {
    if (spec.source == .materialized) return .materialized_input;
    if (spec.has_group) {
        if (spec.group_key_count == 0) return .global_aggregate;
        if (spec.has_order and spec.has_limit) return .group_topn;
        if (spec.has_order) return .group_full_sort;
        return .group_aggregate;
    }
    if (spec.has_order and spec.has_limit) return .scan_topn;
    if (spec.has_order) return .scan_full_sort;
    return .stream_scan;
}

const GroupTopNPlan = struct {
    scan: ir.Op.Scan,
    where_filter: ?ir.Op.Filter,
    having_filter: ?ir.Op.Filter,
    group_by: ir.Op.GroupBy,
    order_by: ?ir.Op.OrderBy,
    limit: ?ir.Op.Limit,
    derived: []const ir.Derived = &.{},
    // Post-aggregate derived columns: collapsed group keys (a function of a
    // surviving plain-column key, e.g. `ClientIP - 1`) recomputed once per
    // output group above the aggregate. Empty for plain shapes.
    post_agg_derived: []const ir.Derived = &.{},
    // Final output column order (SELECT list) when a reordering Project sits
    // above the post-aggregate Compute. Null when the group output order is the
    // final order.
    output_columns: ?[]const []const u8 = null,
    // Per-output rename from the SELECT-list Project (`URL AS Dst`), parallel to
    // `output_columns`. A null entry keeps the selected column's own name. Null
    // (the whole field) when the Project renames nothing.
    output_names: ?[]const ?[]const u8 = null,
};

fn matchGroupTopN(root: *const ir.Op) ?GroupTopNPlan {
    var op = root;
    // The reordering Project, LIMIT, and ORDER BY decorators can nest in either
    // order above the aggregate. A renamed or computed output column forces a
    // Project that lands *below* the LIMIT (Limit → Project → OrderBy → GroupBy),
    // so peel all three in a single any-order pass rather than a fixed sequence.
    var top_project: ?ir.Op.Project = null;
    var limit: ?ir.Op.Limit = null;
    var order_by: ?ir.Op.OrderBy = null;
    while (true) {
        switch (op.*) {
            .select => {
                if (top_project != null) return null;
                top_project = op.select;
                op = op.select.upstream;
            },
            .limit => {
                if (limit != null) return null;
                limit = op.limit;
                op = op.limit.upstream;
            },
            .order_by => {
                if (order_by != null) return null;
                order_by = op.order_by;
                op = op.order_by.upstream;
            },
            else => break,
        }
    }
    var having_filter: ?ir.Op.Filter = null;
    if (op.* == .filter) {
        having_filter = op.filter;
        op = op.filter.upstream;
    }
    // A post-aggregate Compute (collapsed group keys recomputed above the
    // aggregate) sits directly on the GroupBy, below HAVING/ORDER BY. Peel it;
    // the handler recomputes those columns over the grouped output.
    var post_agg_derived: []const ir.Derived = &.{};
    if (op.* == .compute) {
        post_agg_derived = op.compute.derived;
        op = op.compute.upstream;
    }
    if (op.* != .group_by) return null;
    const group_by = op.group_by;
    if (group_by.group_cols.len == 0) return null;
    // The top Project's column order is the final output order. With a
    // post-aggregate Compute it genuinely reorders (group output is
    // [keys, aggs]; SELECT interleaves the derived columns), so capture the
    // order instead of demanding a pass-through. Only a plain column-name
    // reorder is handled (no nested output expressions at this level).
    var output_columns: ?[]const []const u8 = null;
    var output_names: ?[]const ?[]const u8 = null;
    if (top_project) |p| {
        if (post_agg_derived.len != 0) {
            // The post-agg Compute genuinely reorders [keys, aggs]; trust the
            // SELECT order. A per-column rename (`URL AS Dst`) rides along.
            output_columns = p.columns;
            output_names = p.outputs;
        } else if (!projectMatchesGroupOutput(p, group_by)) {
            return null;
        } else if (p.outputs != null) {
            // Pure rename passthrough of the group output (`SELECT col AS x`):
            // columns already match the group output order, only names change.
            output_columns = p.columns;
            output_names = p.outputs;
        }
    }

    // Peel an optional row-local Compute (derived group keys / aggregate
    // inputs, e.g. ClientIP - 1 or length(URL)) and an optional WHERE filter,
    // in either order, down to the scan. Derived columns are evaluated in the
    // scan layer's Compute wrapper.
    var where_filter: ?ir.Op.Filter = null;
    var derived: []const ir.Derived = &.{};
    var source = group_by.upstream;
    while (true) {
        switch (source.*) {
            .compute => {
                if (derived.len != 0) return null;
                derived = source.compute.derived;
                source = source.compute.upstream;
            },
            .filter => {
                if (where_filter != null) return null;
                where_filter = source.filter;
                source = source.filter.upstream;
            },
            else => break,
        }
    }
    if (source.* != .scan) return null;
    if (source.scan.alias != null) return null;

    return .{
        .scan = source.scan,
        .where_filter = where_filter,
        .having_filter = having_filter,
        .group_by = group_by,
        .order_by = order_by,
        .limit = limit,
        .derived = derived,
        .post_agg_derived = post_agg_derived,
        .output_columns = output_columns,
        .output_names = output_names,
    };
}

// Whether `project`'s selected columns are exactly the group output
// (`[keys, aggs]`) in canonical order. Output RENAMES are tolerated — the
// caller captures them separately and applies them via `applyOutputProjection`.
fn projectMatchesGroupOutput(project: ir.Op.Project, group_by: ir.Op.GroupBy) bool {
    if (project.columns.len != group_by.group_cols.len + group_by.aggs.len) return false;
    var i: usize = 0;
    for (group_by.group_cols) |name| {
        if (!types.columnNameEql(project.columns[i], name)) return false;
        i += 1;
    }
    for (group_by.aggs) |agg| {
        if (!types.columnNameEql(project.columns[i], agg.as)) return false;
        i += 1;
    }
    return true;
}

// Apply the SELECT-list Project captured during shape matching: select
// `output_columns` (reorder) and, when the Project renamed any column
// (`output_names`), rename them via `Project.createNamed`. A null `output_names`
// entry keeps the selected column's own name. No-op when no Project was captured.
fn applyOutputProjection(allocator: std.mem.Allocator, q: exec.Query, plan: GroupTopNPlan) !exec.Query {
    const cols = plan.output_columns orelse return q;
    const renames = plan.output_names orelse return q.project(cols);
    const names = try allocator.alloc([]const u8, cols.len);
    defer allocator.free(names);
    for (cols, renames, names) |col, rename, *out| out.* = rename orelse col;
    return exec.Project.createNamed(allocator, q, cols, names);
}

fn buildGroupTopN(input: CompileInput, root: *const ir.Op) !?exec.Query {
    const plan = matchGroupTopN(root) orelse return null;
    if (hasUdfAgg(plan.group_by.aggs)) return null;

    const table = try resolveTable(input.db, input.session, plan.scan.table);
    const needed = try projectedBaseColumns(input.allocator, table, input.prune_names);
    defer if (needed) |n| input.allocator.free(n);

    const request = v2_pipeline.GroupTopNRequest{
        .group_cols = plan.group_by.group_cols,
        .aggs = plan.group_by.aggs,
        .order_specs = if (plan.order_by) |o| o.specs else &.{},
        .limit = if (plan.limit) |l| @intCast(l.n) else 0,
        .offset = if (plan.limit) |l| @intCast(l.offset) else 0,
        .where_filter = if (plan.where_filter) |f| f.predicate else null,
        .having_filter = if (plan.having_filter) |f| f.predicate else null,
        .needed = needed,
        .derived = plan.derived,
        .dop = input.db.config.max_dop,
    };

    if (try v2_pipeline.tryBuildGroupTopN(input.allocator, table, request)) |silo_q| {
        var q = silo_q;
        // Post-aggregate enrich: the grouped pipeline emits [keys, aggs] in
        // count-/order-ranked order; recompute the collapsed group keys over that
        // small output and reorder to the SELECT list using the generic Compute and
        // Project operators (no bespoke materialization — the pipeline is just a
        // Query like any other). ORDER BY/LIMIT already ran inside the pipeline on
        // the grouped columns, so nothing re-runs here.
        errdefer q.deinit();
        if (plan.post_agg_derived.len > 0) q = try q.compute(plan.post_agg_derived);
        q = try applyOutputProjection(input.allocator, q, plan);
        return q;
    }

    // The silo-grid core carries fixed numeric state slots and string MIN/MAX,
    // but declines variable-state aggregates it can't hold in the group table —
    // COUNT(DISTINCT) (a growing per-group set), percentile, group_concat. There
    // is no single-threaded fallback: a grouped shape the parallel core can't run
    // is unsupported, not silently serialized.
    return error.UnsupportedQueryShape;
}

// Push the fusable subset of derived columns DOWN into the ParallelScan workers
// so row-local scalar fns (e.g. REGEXP_REPLACE, length()) compute in parallel —
// instead of stacking a serial Compute that a single-threaded drain evaluates one
// row at a time (the Q29 40s trap). Any non-fusable remainder (CASE, subqueries)
// is layered as a serial Compute referencing the fused cols. Mirrors
// net/local.zig's fusion split.
fn computeDerivedFused(allocator: std.mem.Allocator, q: exec.Query, derived: []const ir.Derived) !exec.Query {
    var result = q;
    const scan_cols = result.outputSchema();
    var fusable: std.ArrayListUnmanaged(ir.Derived) = .empty;
    defer fusable.deinit(allocator);
    var serial: std.ArrayListUnmanaged(ir.Derived) = .empty;
    defer serial.deinit(allocator);
    for (derived) |d| {
        if (exec.derivedFusable(d, scan_cols)) try fusable.append(allocator, d) else try serial.append(allocator, d);
    }
    if (fusable.items.len == 0) return result.compute(derived);
    if (!try result.tryFuseCompute(fusable.items)) return result.compute(derived);
    if (serial.items.len == 0) return result;
    return result.compute(serial.items);
}

const GlobalAggregatePlan = struct {
    scan: ir.Op.Scan,
    where_filter: ?ir.Op.Filter,
    having_filter: ?ir.Op.Filter,
    group_by: ir.Op.GroupBy,
    derived: []const ir.Derived = &.{},
};

fn matchGlobalAggregate(root: *const ir.Op) ?GlobalAggregatePlan {
    var op = root;
    var top_project: ?ir.Op.Project = null;
    if (op.* == .select) {
        top_project = op.select;
        op = op.select.upstream;
    }
    // ORDER BY / LIMIT over a one-row result are no-ops; accept and ignore.
    if (op.* == .limit) op = op.limit.upstream;
    if (op.* == .order_by) op = op.order_by.upstream;

    var having_filter: ?ir.Op.Filter = null;
    if (op.* == .filter) {
        having_filter = op.filter;
        op = op.filter.upstream;
    }
    if (op.* != .group_by) return null;
    const group_by = op.group_by;
    if (group_by.group_cols.len != 0) return null;
    if (top_project) |p| {
        if (!projectMatchesGroupOutput(p, group_by)) return null;
    }

    // Peel an optional row-local Compute (derived aggregate inputs, e.g.
    // SUM(ResolutionWidth + 1)) and an optional WHERE filter, in either order,
    // down to the scan. The derived columns are computed in the scan layer.
    var where_filter: ?ir.Op.Filter = null;
    var derived: []const ir.Derived = &.{};
    var source = group_by.upstream;
    while (true) {
        switch (source.*) {
            .compute => {
                if (derived.len != 0) return null;
                derived = source.compute.derived;
                source = source.compute.upstream;
            },
            .filter => {
                if (where_filter != null) return null;
                where_filter = source.filter;
                source = source.filter.upstream;
            },
            else => break,
        }
    }
    if (source.* != .scan) return null;
    if (source.scan.alias != null) return null;

    return .{
        .scan = source.scan,
        .where_filter = where_filter,
        .having_filter = having_filter,
        .group_by = group_by,
        .derived = derived,
    };
}

fn buildGlobalAggregate(input: CompileInput, root: *const ir.Op) !?exec.Query {
    const plan = matchGlobalAggregate(root) orelse return null;
    if (hasUdfAgg(plan.group_by.aggs)) return null;

    const table = try resolveTable(input.db, input.session, plan.scan.table);

    // No serial fallback. The parallel reducer (v2_global_aggregate) is the only
    // path for a no-GROUP-BY aggregate: every aggregate — COUNT(DISTINCT) of any
    // type included — folds per-lane and merges. A shape it declines surfaces as
    // UnsupportedQueryShape (the caller maps null → error) so it gets fixed in
    // the parallel handler rather than silently run single-threaded.
    return try v2_global_aggregate.tryBuild(input.allocator, table, .{
        .aggs = plan.group_by.aggs,
        .where_filter = if (plan.where_filter) |f| f.predicate else null,
        .having_filter = if (plan.having_filter) |f| f.predicate else null,
        .derived = plan.derived,
        .dop = input.db.config.max_dop,
    });
}

// A non-aggregating SELECT: scan → optional WHERE → optional row-local derived
// → optional ORDER BY → optional LIMIT → optional projection. Covers the
// stream_scan / scan_topn / scan_full_sort shapes (e.g. ClickBench Q20, Q24-27).
const ScanSelectPlan = struct {
    scan: ir.Op.Scan,
    where_filter: ?ir.Op.Filter = null,
    derived: []const ir.Derived = &.{},
    order_by: ?ir.Op.OrderBy = null,
    limit: ?ir.Op.Limit = null,
    // SELECT-list columns (whitelist projection). Null = emit the scan's columns
    // as-is (e.g. SELECT *).
    project_columns: ?[]const []const u8 = null,
};

fn matchScanSelect(root: *const ir.Op) ?ScanSelectPlan {
    var op = root;
    // Top decorators — Project (SELECT list), LIMIT, ORDER BY — can nest in any
    // order above the scan body; peel each at most once.
    var project_columns: ?[]const []const u8 = null;
    var limit: ?ir.Op.Limit = null;
    var order_by: ?ir.Op.OrderBy = null;
    while (true) {
        switch (op.*) {
            .select => |p| {
                if (project_columns != null) return null;
                // Aliased / computed output expressions aren't handled here.
                if (p.outputs) |outs| for (outs) |o| if (o != null) return null;
                project_columns = p.columns;
                op = p.upstream;
            },
            .limit => |l| {
                if (limit != null) return null;
                limit = l;
                op = l.upstream;
            },
            .order_by => |o| {
                if (order_by != null) return null;
                order_by = o;
                op = o.upstream;
            },
            else => break,
        }
    }
    // The scan body: an optional row-local Compute and an optional WHERE, in
    // either order, down to the table scan.
    var where_filter: ?ir.Op.Filter = null;
    var derived: []const ir.Derived = &.{};
    while (true) {
        switch (op.*) {
            .compute => |c| {
                if (derived.len != 0) return null;
                derived = c.derived;
                op = c.upstream;
            },
            .filter => |f| {
                if (where_filter != null) return null;
                where_filter = f;
                op = f.upstream;
            },
            else => break,
        }
    }
    if (op.* != .scan) return null;
    if (op.scan.alias != null) return null;
    return .{
        .scan = op.scan,
        .where_filter = where_filter,
        .derived = derived,
        .order_by = order_by,
        .limit = limit,
        .project_columns = project_columns,
    };
}

/// Walk a resolved WHERE predicate and collect every base-column name it
/// references (case-insensitive dedup). Returns error.UnsupportedOp for any
/// subquery / correlated / var shape — those never appear under a single
/// base-table late-mat candidate and signal the caller to skip late-mat. Mirror
/// of net/local.collectPredicateNames; kept here so engine_v2 stays free of a
/// net/ dependency.
fn collectPredicateColumns(
    allocator: std.mem.Allocator,
    set: *std.ArrayListUnmanaged([]const u8),
    p: exec.PredicateExpr,
) !void {
    switch (p) {
        .leaf => |lf| try addColumnUnique(allocator, set, lf.col),
        .leaf_col_col => |lc| {
            try addColumnUnique(allocator, set, lc.left);
            try addColumnUnique(allocator, set, lc.right);
        },
        .is_null, .is_not_null => |col| try addColumnUnique(allocator, set, col),
        .like => |lp| try addColumnUnique(allocator, set, lp.col),
        .in_set => |s| try addColumnUnique(allocator, set, s.col),
        .@"and", .@"or" => |children| for (children) |ch| try collectPredicateColumns(allocator, set, ch),
        .not => |child| try collectPredicateColumns(allocator, set, child.*),
        .always => {},
        else => return error.UnsupportedOp,
    }
}

fn addColumnUnique(
    allocator: std.mem.Allocator,
    set: *std.ArrayListUnmanaged([]const u8),
    name: []const u8,
) !void {
    for (set.items) |existing| {
        if (types.columnNameEql(existing, name)) return;
    }
    try set.append(allocator, name);
}

/// True when at least one output column isn't already decoded by the probe set,
/// so fetching the wide output for only the survivors saves decoding work.
fn outputWiderThanProbe(
    table: *api.Table,
    output_names: []const []const u8,
    probe: []const []const u8,
) bool {
    for (output_names) |on| {
        const oi = types.findColumn(table.schema.columns, on) orelse return false;
        var in_probe = false;
        for (probe) |pn| {
            if (types.findColumn(table.schema.columns, pn)) |pi| {
                if (pi == oi) {
                    in_probe = true;
                    break;
                }
            }
        }
        if (!in_probe) return true;
    }
    return false;
}

/// Late-materialization fast path for `SELECT <wide> FROM t WHERE <pred>
/// [ORDER BY <keys>] LIMIT n`. Decodes only the probe columns (filter ∪ ORDER
/// BY) through the bounded top-k, then fetches the wide output columns for just
/// the ≤ n survivors — instead of decoding every output column for every
/// filtered row. Prefers the zonemap block-skipping top-N when an ORDER BY key
/// qualifies. Returns null when the shape doesn't benefit (caller falls back to
/// the naive wide scan), so it's only ever a speedup, never a correctness risk.
fn tryScanSelectLateMat(
    input: CompileInput,
    table: *api.Table,
    plan: ScanSelectPlan,
    limit: ir.Op.Limit,
) !?exec.Query {
    // Derived output/probe columns aren't base columns the late-mat fetch can
    // resolve by location — leave those to the naive path.
    if (plan.derived.len > 0) return null;

    const allocator = input.allocator;

    // Probe set = WHERE columns ∪ ORDER BY columns, all base columns.
    var probe: std.ArrayListUnmanaged([]const u8) = .empty;
    defer probe.deinit(allocator);
    if (plan.where_filter) |f| {
        collectPredicateColumns(allocator, &probe, f.predicate) catch return null;
    }
    if (plan.order_by) |o| {
        for (o.specs) |sp| try addColumnUnique(allocator, &probe, sp.col);
    }
    for (probe.items) |nm| {
        if (types.findColumn(table.schema.columns, nm) == null) return null;
    }

    // Output projection: explicit SELECT-list names, or every base column for
    // SELECT *. Owned only in the SELECT * case.
    var owns_output = false;
    const output_names: []const []const u8 = if (plan.project_columns) |cols| blk: {
        for (cols) |nm| {
            if (types.findColumn(table.schema.columns, nm) == null) return null;
        }
        break :blk cols;
    } else blk: {
        const all = try allocator.alloc([]const u8, table.schema.columns.len);
        for (all, table.schema.columns) |*o, c| o.* = c.name;
        owns_output = true;
        break :blk all;
    };
    defer if (owns_output) allocator.free(@constCast(output_names));

    const n: usize = @intCast(limit.n);
    const offset: usize = @intCast(limit.offset);

    // Zonemap block-skipping top-N: prunes whole row groups via per-RG min/max
    // and is byte-identical to the full scan regardless of projection width or
    // a WHERE — so try it first whenever there's an ORDER BY.
    if (plan.order_by) |o| {
        if (try exec.zonemapTopN(allocator, table, null, probe.items, predicateOrAlways(plan.where_filter), o.specs, output_names, n, offset)) |q| {
            return q;
        }
    }

    // LateScan only pays off with a real WHERE that decodes fewer columns than
    // the wide output; otherwise the naive bounded scan is as good or better.
    if (plan.where_filter == null) return null;
    if (!outputWiderThanProbe(table, output_names, probe.items)) return null;

    const specs: ?[]const exec.SortSpec = if (plan.order_by) |o| o.specs else null;
    return try exec.lateScan(allocator, table, null, probe.items, plan.where_filter.?.predicate, specs, output_names, n, offset);
}

fn predicateOrAlways(filter: ?ir.Op.Filter) exec.PredicateExpr {
    return if (filter) |f| f.predicate else .{ .always = true };
}

// Build the non-aggregating SELECT pipeline: parallel scan with the WHERE fused
// into its workers (Filter.create auto-fuses via tryFuseFilter), row-local
// derived columns fused too, then ORDER BY + LIMIT as a single heap top-N (or
// either alone), then the SELECT-list projection. When a LIMIT bounds the
// output, late materialization (zonemap top-N / LateScan) is tried first — it
// decodes only the probe columns through the top-k and fetches the wide output
// for just the survivors.
fn buildScanSelect(input: CompileInput, root: *const ir.Op) !?exec.Query {
    const plan = matchScanSelect(root) orelse return null;
    const table = try resolveTable(input.db, input.session, plan.scan.table);

    if (plan.limit) |l| {
        if (try tryScanSelectLateMat(input, table, plan, l)) |q| return q;
    }

    const needed = try projectedBaseColumns(input.allocator, table, input.prune_names);
    defer if (needed) |n| input.allocator.free(n);
    const allocator = input.allocator;
    const max_dop = input.db.config.max_dop;

    var q = if (max_dop > 1)
        try exec.ParallelScan.create(allocator, table, null, needed, max_dop)
    else
        try exec.scanWithProjection(allocator, table, null, needed);
    errdefer q.deinit();

    if (plan.where_filter) |f| q = try q.filter(f.predicate);
    if (plan.derived.len > 0) q = try computeDerivedFused(allocator, q, plan.derived);
    if (plan.order_by) |o| {
        if (plan.limit) |l| {
            q = try q.topN(o.specs, @intCast(l.n), @intCast(l.offset));
        } else {
            q = try q.orderBy(o.specs);
        }
    } else if (plan.limit) |l| {
        q = try q.limitOffset(@intCast(l.n), @intCast(l.offset));
    }
    if (plan.project_columns) |cols| q = try q.project(cols);
    return q;
}

fn projectedBaseColumns(
    allocator: std.mem.Allocator,
    table: *api.Table,
    prune_names: ?[][]const u8,
) !?[]const []const u8 {
    const raw = prune_names orelse return null;
    var keep: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer keep.deinit(allocator);
    for (raw) |name| {
        if (types.findColumn(table.schema.columns, name) != null) {
            try keep.append(allocator, name);
        }
    }
    return try keep.toOwnedSlice(allocator);
}

fn hasUdfAgg(aggs: []const exec.AggSpec) bool {
    for (aggs) |a| if (a.func == .udf) return true;
    return false;
}

fn resolveTable(db: *api.Database, session: api.Session, ref: ir.TableRef) !*api.Table {
    if (ref.database == null and ref.schema == null) {
        if (session.temp_namespace) |ns| {
            if (ns.findTable(ref.name)) |t| return t;
        }
    }

    const catalog = catalogFor(db) orelse return error.DatabaseNotFound;
    var db_name: []const u8 = ref.database orelse session.current_db;
    var schema_name: []const u8 = ref.schema orelse session.current_schema;
    if (ref.database == null and ref.schema != null) {
        if (splitDoubleUnderscore(ref.schema.?)) |parts| {
            db_name = parts.db;
            schema_name = parts.schema;
        }
    }
    const resolved_db = catalog.database(db_name) orelse return error.DatabaseNotFound;
    const schema = resolved_db.schema(schema_name) orelse return error.SchemaNotFound;
    {
        schema.tables_mutex.lockUncancelable(schema.io);
        defer schema.tables_mutex.unlock(schema.io);
        if (schema.tables.get(ref.name)) |t| return t;
    }
    return schema.openTable(ref.name, .{});
}

fn catalogFor(db: *api.Database) ?*api.Catalog {
    if (db.catalog) |catalog| return catalog;
    if (db.owned_catalog) |catalog| return catalog;
    return null;
}

fn splitDoubleUnderscore(s: []const u8) ?struct { db: []const u8, schema: []const u8 } {
    const pos = std.mem.indexOf(u8, s, "__") orelse return null;
    if (pos == 0 or pos + 2 >= s.len) return null;
    return .{ .db = s[0..pos], .schema = s[pos + 2 ..] };
}

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

test "engine v2 classifies scan filter group order limit as grouped topn" {
    var scan: ir.Op = .{ .scan = .{ .table = .{ .name = "hits" } } };
    var filter: ir.Op = .{ .filter = .{
        .predicate = .{ .leaf = .{ .col = "SearchPhrase", .op = .neq, .val = .{ .text = "" } } },
        .upstream = &scan,
    } };
    const group_cols = [_][]const u8{"ClientIP"};
    const aggs = [_]ir.AggSpec{.{ .func = .count, .col = null, .as = "c" }};
    var group: ir.Op = .{ .group_by = .{ .group_cols = &group_cols, .aggs = &aggs, .upstream = &filter } };
    const order_specs = [_]ir.SortSpec{.{ .col = "c", .desc = true }};
    var order: ir.Op = .{ .order_by = .{ .specs = &order_specs, .upstream = &group } };
    var limit: ir.Op = .{ .limit = .{ .n = 10, .upstream = &order } };

    const spec = classifySimplePipeline(&limit).?;
    try std.testing.expectEqual(Shape.group_topn, spec.shape);
    try std.testing.expect(spec.has_where_filter);
    try std.testing.expect(!spec.has_having_filter);
}

test "engine v2 treats filter above group as having" {
    var scan: ir.Op = .{ .scan = .{ .table = .{ .name = "hits" } } };
    const group_cols = [_][]const u8{"ClientIP"};
    const aggs = [_]ir.AggSpec{.{ .func = .count, .col = null, .as = "c" }};
    var group: ir.Op = .{ .group_by = .{ .group_cols = &group_cols, .aggs = &aggs, .upstream = &scan } };
    var having: ir.Op = .{ .filter = .{
        .predicate = .{ .leaf = .{ .col = "c", .op = .gt, .val = .{ .int = 1 } } },
        .upstream = &group,
    } };

    const spec = classifySimplePipeline(&having).?;
    try std.testing.expectEqual(Shape.group_aggregate, spec.shape);
    try std.testing.expect(!spec.has_where_filter);
    try std.testing.expect(spec.has_having_filter);
}
