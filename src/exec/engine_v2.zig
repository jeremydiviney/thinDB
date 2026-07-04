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
const affine_agg = @import("affine_agg.zig");

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
    // Query-lifetime arena for plan-node data the operator tree borrows but
    // doesn't own (the affine reduction's derived names / expr trees). Freed
    // by the CompileCtx after the operator tree is torn down, so the borrows
    // outlive the operators that hold them.
    node_arena: std.mem.Allocator,
    /// Process-local scalar UDF registry (null when the db has no catalog).
    /// Serial Computes resolve user-defined functions through it — same as
    /// the legacy path's CompileCtx.udf_registry.
    udf_registry: ?*const @import("../udf.zig").UdfRegistry = null,
    /// Query-scoped memory accountant (CompileCtx-owned), injected into the
    /// scans and stages this compile creates so the whole query shares one
    /// budget. Leaves created without it (silo-internal scans) self-mint
    /// from the table's budget + shared pool, so enforcement still holds —
    /// only the per-query attribution fragments.
    accountant: ?*exec.memory.MemoryAccountant = null,
    /// Ride-the-order: this subtree must preserve its source's row order
    /// (a sorted window stage feeding a same-key rider), so order-scrambling
    /// parallel seams (round buffer scans interleave stripes) are suppressed
    /// for its chains. Scoped: callers set it on a COPY of the input.
    force_ordered: bool = false,
    /// Cap on parallelism for this compile, below the server's max_dop.
    /// A SEPARABLE slice pipeline compiles with dop_cap = 1 — the slices
    /// themselves are the parallelism. Read via `effectiveDop()`.
    dop_cap: ?usize = null,
    /// SEPARABLE slice predicate: applied (as a fused Filter) on top of
    /// every LEAF this compile produces whose schema carries all of
    /// `slice_cols` — base-table scans and shared-stage reads alike. The
    /// body IR is shared read-only across slice compiles; the predicate is
    /// a compile parameter, never an IR mutation.
    slice_pred: ?exec.predicate.PredicateExpr = null,
    slice_cols: []const []const u8 = &.{},

    pub fn effectiveDop(self: *const CompileInput) usize {
        const base = self.db.config.max_dop;
        if (self.dop_cap) |cap| return @min(base, cap);
        return base;
    }
};

/// Compile ONE single-source query block (no materialize boundaries inside —
/// staged plans route each block here or to the materialized-source builder
/// in net/cte_stages.zig). Every SELECT shape is V2's responsibility: build
/// it or fail. There is deliberately no fallback to the legacy operator
/// engine for these.
pub fn compileSelectBlock(input: CompileInput, root: *const ir.Op) anyerror!exec.Query {
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
pub fn isSelectQuery(op: *const ir.Op) bool {
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
            // A strict SUBSET / reorder of the group output (SELECT DISTINCT's
            // hidden COUNT(*) is grouped but never projected) rides the same
            // applyOutputProjection seam as the post-agg-compute case. Any
            // column outside the group output stays unsupported.
            for (p.columns) |c| {
                if (!nameInList(group_by.group_cols, c)) {
                    var is_agg = false;
                    for (group_by.aggs) |agg| {
                        if (types.columnNameEql(agg.as, c)) is_agg = true;
                    }
                    if (!is_agg) return null;
                }
            }
            output_columns = p.columns;
            output_names = p.outputs;
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
fn applyOutputProjection(allocator: std.mem.Allocator, q: exec.Query, output_columns: ?[]const []const u8, output_names: ?[]const ?[]const u8) !exec.Query {
    const cols = output_columns orelse return q;
    const renames = output_names orelse return q.project(cols);
    const names = try allocator.alloc([]const u8, cols.len);
    defer allocator.free(names);
    for (cols, renames, names) |col, rename, *out| out.* = rename orelse col;
    return exec.Project.createNamed(allocator, q, cols, names);
}

fn appendNameUnique(allocator: std.mem.Allocator, set: *std.ArrayListUnmanaged([]const u8), name: []const u8) !void {
    for (set.items) |existing| if (types.columnNameEql(existing, name)) return;
    try set.append(allocator, name);
}

fn collectPredicateNames(allocator: std.mem.Allocator, set: *std.ArrayListUnmanaged([]const u8), p: exec.PredicateExpr) !void {
    switch (p) {
        .leaf => |l| try appendNameUnique(allocator, set, l.col),
        .day_leaf => |l| try appendNameUnique(allocator, set, l.col),
        .leaf_col_col => |c| {
            try appendNameUnique(allocator, set, c.left);
            try appendNameUnique(allocator, set, c.right);
        },
        .is_null, .is_not_null => |nm| try appendNameUnique(allocator, set, nm),
        .like => |lk| try appendNameUnique(allocator, set, lk.col),
        .in_set => |s| try appendNameUnique(allocator, set, s.col),
        .@"and", .@"or" => |kids| for (kids) |k| try collectPredicateNames(allocator, set, k),
        .not => |k| try collectPredicateNames(allocator, set, k.*),
        else => {},
    }
}

fn nameInList(list: []const []const u8, name: []const u8) bool {
    for (list) |n| if (types.columnNameEql(n, name)) return true;
    return false;
}

// Whether the group columns are exactly the table's leading order-key columns
// (as a set — the stream groups on the prefix whatever order GROUP BY lists
// them in), i.e. the shape the legacy router streams in key order.
fn groupColsAreOrderKeyPrefix(table: *api.Table, group_cols: []const []const u8) bool {
    const ok = table.schema.order_key;
    if (group_cols.len == 0 or group_cols.len > ok.len) return false;
    for (ok[0..group_cols.len]) |k| {
        if (!nameInList(group_cols, k)) return false;
    }
    return true;
}

// A HAVING conjunct whose every referenced column is a group-key BASE column
// (not a derived key — those don't exist at scan time under every scan mode)
// admits the per-group ≡ per-row equivalence: all rows of a group share its
// key, so filtering the rows filters exactly the failing groups.
fn havingConjunctPushable(input: CompileInput, table: *api.Table, group_cols: []const []const u8, conjunct: exec.PredicateExpr) !bool {
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer names.deinit(input.allocator);
    try collectPredicateNames(input.allocator, &names, conjunct);
    if (names.items.len == 0) return false;
    for (names.items) |nm| {
        if (!nameInList(group_cols, nm)) return false;
        if (types.findColumn(table.schema.columns, nm) == null) return false;
    }
    return true;
}

const HavingSplit = struct { where: ?exec.PredicateExpr, having: ?exec.PredicateExpr };

// Pre-execution rewrite: move key-only HAVING conjuncts into the WHERE filter.
// Splits only a top-level AND (or the whole predicate); a key/aggregate mix
// under OR stays as HAVING. New predicate nodes live in the query's node arena.
fn pushKeyOnlyHavingIntoWhere(
    input: CompileInput,
    table: *api.Table,
    group_cols: []const []const u8,
    having: exec.PredicateExpr,
    where: ?exec.PredicateExpr,
) !HavingSplit {
    const arena = input.node_arena;
    const conjuncts: []const exec.PredicateExpr = switch (having) {
        .@"and" => |kids| kids,
        else => blk: {
            const one = try arena.alloc(exec.PredicateExpr, 1);
            one[0] = having;
            break :blk one;
        },
    };

    var pushed: std.ArrayListUnmanaged(exec.PredicateExpr) = .empty;
    defer pushed.deinit(input.allocator);
    var kept: std.ArrayListUnmanaged(exec.PredicateExpr) = .empty;
    defer kept.deinit(input.allocator);
    for (conjuncts) |c| {
        if (try havingConjunctPushable(input, table, group_cols, c)) {
            try pushed.append(input.allocator, c);
        } else {
            try kept.append(input.allocator, c);
        }
    }
    if (pushed.items.len == 0) return .{ .where = where, .having = having };

    var where_parts: std.ArrayListUnmanaged(exec.PredicateExpr) = .empty;
    defer where_parts.deinit(input.allocator);
    if (where) |w| try where_parts.append(input.allocator, w);
    try where_parts.appendSlice(input.allocator, pushed.items);
    const new_where: exec.PredicateExpr = if (where_parts.items.len == 1)
        where_parts.items[0]
    else
        .{ .@"and" = try arena.dupe(exec.PredicateExpr, where_parts.items) };

    const new_having: ?exec.PredicateExpr = switch (kept.items.len) {
        0 => null,
        1 => kept.items[0],
        else => .{ .@"and" = try arena.dupe(exec.PredicateExpr, kept.items) },
    };
    return .{ .where = new_where, .having = new_having };
}

// Output aliases the grouped core must keep direct (computed in-core, not
// derived afterward) because ORDER BY / HAVING ranks or filters on them.
fn buildUdafGroupBy(input: CompileInput, table: *api.Table, plan: GroupTopNPlan) !exec.Query {
    const registry = input.udf_registry orelse return error.UnsupportedQueryShape;
    const allocator = input.allocator;
    const needed = try projectedBaseColumns(allocator, table, input.prune_names);
    defer if (needed) |n| allocator.free(n);
    const max_dop = input.effectiveDop();

    var q = if (max_dop > 1)
        try exec.ParallelScan.create(allocator, table, input.accountant, needed, max_dop)
    else
        try exec.scanWithProjection(allocator, table, input.accountant, needed);
    errdefer q.deinit();

    if (plan.where_filter) |f| q = try q.filter(f.predicate);
    if (plan.derived.len > 0) q = try computeDerivedFused(allocator, q, plan.derived, input.udf_registry);
    q = try q.udfGroupBy(plan.group_by.group_cols, plan.group_by.aggs, registry);
    // HAVING runs as a generic filter over the (small) grouped output.
    if (plan.having_filter) |f| q = try q.filter(f.predicate);
    if (plan.order_by) |o| {
        if (plan.limit) |l| {
            q = try q.topN(o.specs, @intCast(l.n), @intCast(l.offset));
        } else {
            q = try q.orderBy(o.specs);
        }
    } else if (plan.limit) |l| {
        q = try q.limitOffset(@intCast(l.n), @intCast(l.offset));
    }
    if (plan.post_agg_derived.len > 0) q = try q.computeWithRegistry(plan.post_agg_derived, input.udf_registry);
    q = try applyOutputProjection(allocator, q, plan.output_columns, plan.output_names);
    return q;
}

fn buildOperatorGroupBy(input: CompileInput, table: *api.Table, plan: GroupTopNPlan) !exec.Query {
    const allocator = input.allocator;
    const needed = try projectedBaseColumns(allocator, table, input.prune_names);
    defer if (needed) |n| allocator.free(n);
    const max_dop = input.effectiveDop();

    var q = if (max_dop > 1)
        try exec.ParallelScan.create(allocator, table, input.accountant, needed, max_dop)
    else
        try exec.scanWithProjection(allocator, table, input.accountant, needed);
    errdefer q.deinit();

    if (plan.where_filter) |f| q = try q.filter(f.predicate);
    if (plan.derived.len > 0) q = try computeDerivedFused(allocator, q, plan.derived, input.udf_registry);
    q = try q.groupBy(plan.group_by.group_cols, plan.group_by.aggs);
    if (plan.having_filter) |f| q = try q.filter(f.predicate);
    if (plan.order_by) |o| {
        if (plan.limit) |l| {
            q = try q.topN(o.specs, @intCast(l.n), @intCast(l.offset));
        } else {
            q = try q.orderBy(o.specs);
        }
    } else if (plan.limit) |l| {
        q = try q.limitOffset(@intCast(l.n), @intCast(l.offset));
    }
    if (plan.post_agg_derived.len > 0) q = try q.computeWithRegistry(plan.post_agg_derived, input.udf_registry);
    q = try applyOutputProjection(allocator, q, plan.output_columns, plan.output_names);
    return q;
}

fn collectProtectedAggNames(allocator: std.mem.Allocator, plan: GroupTopNPlan) ![]const []const u8 {
    var set: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer set.deinit(allocator);
    if (plan.order_by) |o| for (o.specs) |s| try appendNameUnique(allocator, &set, s.col);
    if (plan.having_filter) |f| try collectPredicateNames(allocator, &set, f.predicate);
    return set.toOwnedSlice(allocator);
}

fn buildGroupTopN(input: CompileInput, root: *const ir.Op) !?exec.Query {
    const plan = matchGroupTopN(root) orelse return null;

    const table = try resolveTable(input.db, input.session, plan.scan.table);

    const needed = try projectedBaseColumns(input.allocator, table, input.prune_names);
    defer if (needed) |n| input.allocator.free(n);

    // HAVING conjuncts that reference only group-key base columns filter the
    // same rows whether applied per row (WHERE) or per group (every row of a
    // group shares its key), so push them below the aggregate. The grouped
    // core's HAVING evaluator is numeric-only; this rewrite is what lets
    // `HAVING string_key = '...'` run at all — and it prunes rows earlier.
    var eff_where: ?exec.PredicateExpr = if (plan.where_filter) |f| f.predicate else null;
    var eff_having: ?exec.PredicateExpr = if (plan.having_filter) |f| f.predicate else null;
    if (eff_having) |h| {
        const split = try pushKeyOnlyHavingIntoWhere(input, table, plan.group_by.group_cols, h, eff_where);
        eff_where = split.where;
        eff_having = split.having;
    }

    // The grouped core orders string keys by their dict code / 128-bit digest —
    // neither is lexicographic. When ORDER BY names a string-typed group key,
    // run the core unordered over ALL groups and sort (+ limit) the small
    // grouped output with the generic sort operator instead.
    var order_specs: []const exec.SortSpec = if (plan.order_by) |o| o.specs else &.{};
    var post_sort = false;
    for (order_specs) |s| {
        if (!nameInList(plan.group_by.group_cols, s.col)) continue;
        const idx = types.findColumn(table.schema.columns, s.col) orelse continue;
        if (table.schema.columns[idx].type.isString()) {
            post_sort = true;
            break;
        }
    }
    // No explicit ORDER BY, group columns = a prefix of the table's order key:
    // the legacy engine streams the sorted scan and emits groups in ascending
    // key order — an observable contract tests rely on. The silo emits hash
    // order; restore the streamed order by sorting the (small) grouped output.
    // Always legal: where the legacy router instead hashed (unsorted input),
    // any emission order was permitted.
    if (order_specs.len == 0 and groupColsAreOrderKeyPrefix(table, plan.group_by.group_cols)) {
        const synth = try input.node_arena.alloc(exec.SortSpec, plan.group_by.group_cols.len);
        for (table.schema.order_key[0..plan.group_by.group_cols.len], synth) |k, *s| {
            s.* = .{ .col = k, .desc = false };
        }
        order_specs = synth;
        post_sort = true;
    }
    // ORDER BY a post-aggregate output that the core can't rank on — a
    // collapsed/derived column (`k + 1`, `concat(tag,'')`) computed above the
    // aggregate, not a group key or an aggregate alias. Run the core unordered
    // and sort the (small) grouped output after the post-agg Compute builds it.
    if (!post_sort) {
        for (order_specs) |s| {
            if (nameInList(plan.group_by.group_cols, s.col)) continue;
            var is_agg = false;
            for (plan.group_by.aggs) |a| {
                if (types.columnNameEql(a.as, s.col)) {
                    is_agg = true;
                    break;
                }
            }
            if (!is_agg) {
                post_sort = true;
                break;
            }
        }
    }

    // Algebraic reduction for the grouped core, same as the global path:
    // collapse affine aggregates (SUM/MIN/MAX of a·col+b) onto a shared base set,
    // deriving each original output once over the (small) grouped result.
    // Aggregates consumed by ORDER BY / HAVING stay direct — the core ranks and
    // filters on them in-pass, before the post-core derivation runs.
    var eff_aggs = plan.group_by.aggs;
    var eff_derived = plan.derived;
    var affine_post: []const ir.Derived = &.{};
    var eff_out_cols = plan.output_columns;
    {
        const protected = try collectProtectedAggNames(input.allocator, plan);
        defer input.allocator.free(protected);
        if (try affine_agg.reduce(input.node_arena, table.schema.columns, plan.group_by.group_cols, plan.group_by.aggs, plan.derived, protected)) |red| {
            eff_aggs = red.base_aggs;
            eff_derived = red.pre_derived;
            affine_post = red.post_derived;
            // The core now emits the base set, not the original aggs. When the
            // matcher already captured a SELECT order (collapsed keys / renames),
            // it binds the original aliases the derivations reproduce; otherwise
            // impose the reduced output order so the base columns are dropped.
            if (eff_out_cols == null) eff_out_cols = red.output_names;
        }
    }

    const request = v2_pipeline.GroupTopNRequest{
        .group_cols = plan.group_by.group_cols,
        .aggs = eff_aggs,
        .order_specs = if (post_sort) &.{} else order_specs,
        .limit = if (post_sort) 0 else if (plan.limit) |l| @intCast(l.n) else 0,
        .offset = if (post_sort) 0 else if (plan.limit) |l| @intCast(l.offset) else 0,
        .where_filter = eff_where,
        .having_filter = eff_having,
        .needed = needed,
        .derived = eff_derived,
        .dop = input.effectiveDop(),
        .udf_registry = input.udf_registry,
    };

    // Provably-low-cardinality group keys take the direct (scatter-free)
    // private-table handler; everything else runs the silo grid.
    const built_q = (try v2_pipeline.tryBuildLowCardGroup(input.allocator, table, request)) orelse
        (try v2_pipeline.tryBuildGroupTopN(input.allocator, table, request));
    if (built_q) |silo_q| {
        var q = silo_q;
        // Post-aggregate enrich: the grouped pipeline emits [keys, aggs] in
        // count-/order-ranked order. The affine late-materialization (reduced
        // aggregates) and the collapsed-key recompute are independent Computes
        // over that small output; then reorder to the SELECT list. ORDER BY/LIMIT
        // already ran inside the pipeline on the grouped columns — except in the
        // string-key-order case, which sorts the grouped output here.
        errdefer q.deinit();
        // The post-agg Computes (affine late-materialization, collapsed/derived
        // keys) must run BEFORE the post-sort: ORDER BY may name a column they
        // produce. For the string-group-key post-sort the key is already in the
        // grouped output, so sorting after the Computes is equally correct.
        if (affine_post.len > 0) q = try q.computeWithRegistry(affine_post, input.udf_registry);
        if (plan.post_agg_derived.len > 0) q = try q.computeWithRegistry(plan.post_agg_derived, input.udf_registry);
        if (post_sort) {
            q = try q.orderBy(order_specs);
            if (plan.limit) |l| q = try q.limitOffset(@intCast(l.n), @intCast(l.offset));
        }
        q = try applyOutputProjection(input.allocator, q, eff_out_cols, plan.output_names);
        return q;
    }

    // UDAF aggregates hold opaque registry-driven per-group state. The silo grid
    // hosts them natively when every arg is a non-null fixed-width numeric (the
    // fold reconstructs ColumnViews over the staged slabs); shapes it declines —
    // string/nullable args, or a low-card route that hasn't taught the lowcard
    // handler to combine opaque state yet — fall to the engine-neutral
    // UdfAggregate operator behind the same V2-built (parallel) scan.
    if (hasUdfAgg(plan.group_by.aggs)) return try buildUdafGroupBy(input, table, plan);
    if (hasMaxByAgg(plan.group_by.aggs)) return try buildOperatorGroupBy(input, table, plan);

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
pub fn computeDerivedFused(allocator: std.mem.Allocator, q: exec.Query, derived: []const ir.Derived, udf_registry: ?*const @import("../udf.zig").UdfRegistry) !exec.Query {
    var result = q;
    const scan_cols = result.outputSchema();
    var fusable: std.ArrayListUnmanaged(ir.Derived) = .empty;
    defer fusable.deinit(allocator);
    var serial: std.ArrayListUnmanaged(ir.Derived) = .empty;
    defer serial.deinit(allocator);
    for (derived) |d| {
        if (exec.derivedFusable(d, scan_cols)) try fusable.append(allocator, d) else try serial.append(allocator, d);
    }
    if (fusable.items.len == 0) return computeSelfPushed(result, derived, udf_registry);
    if (!try result.tryFuseCompute(fusable.items)) return computeSelfPushed(result, derived, udf_registry);
    if (serial.items.len == 0) return result;
    return computeSelfPushed(result, serial.items, udf_registry);
}

/// Layer a Compute, then attempt the TERMINAL push: when the upstream ends
/// in a probe-fused parallel pipeline, the derived evaluation moves into
/// its stripe workers as a chained sink and the operator passes the final
/// batches through. Serial pull semantics are unchanged on decline.
pub fn computeSelfPushed(up: exec.Query, derived: []const ir.Derived, udf_registry: ?*const @import("../udf.zig").UdfRegistry) !exec.Query {
    const q = try up.computeWithRegistry(derived, udf_registry);
    if (exec.queryAs(@import("compute.zig").Compute, q)) |c| {
        const pushed = c.tryFuseSelf();
        if (getenv_c("THINDB_TRACE_SELFPUSH") != null and !pushed) {
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(std.heap.page_allocator);
            c.upstream.explain(&buf, std.heap.page_allocator, 0) catch {};
            const line = if (std.mem.indexOfScalar(u8, buf.items, '\n')) |nl| buf.items[0..nl] else buf.items;
            std.debug.print("[selfpush] DECLINED n_derived={d} upstream={s}\n", .{ derived.len, line });
        }
    }
    return q;
}

const getenv_c = @extern(*const fn (name: [*:0]const u8) callconv(.c) ?[*:0]const u8, .{ .name = "getenv", .library_name = "c" });

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

    const table = try resolveTable(input.db, input.session, plan.scan.table);

    // GROUP_CONCAT (no UDAF): a variable-state aggregate the parallel reducer
    // doesn't host — route onto the engine-neutral hash Aggregate operator.
    if ((hasConcatAgg(plan.group_by.aggs) or hasMaxByAgg(plan.group_by.aggs)) and !hasUdfAgg(plan.group_by.aggs)) {
        return try buildGlobalOperatorAggregate(input, table, plan);
    }
    // Global UDAF: the parallel reducer folds each UDAF per-lane and merges them
    // with `combine`. A non-combinable UDAF (or a UDAF mixed with GROUP_CONCAT)
    // declines there → the serial UdfAggregate operator (parallel scan, serial
    // fold) is the correct home, since states that can't merge can't parallelize.
    if (hasUdfAgg(plan.group_by.aggs)) {
        if (try v2_global_aggregate.tryBuild(input.allocator, table, .{
            .aggs = plan.group_by.aggs,
            .where_filter = if (plan.where_filter) |f| f.predicate else null,
            .having_filter = if (plan.having_filter) |f| f.predicate else null,
            .derived = plan.derived,
            .dop = input.effectiveDop(),
            .udf_registry = input.udf_registry,
        })) |q| return q;
        return try buildGlobalOperatorAggregate(input, table, plan);
    }

    // Algebraic reduction: collapse SUM/MIN/MAX(affine(col)) onto a shared base
    // set computed once, then derive every original output. Q29's 90
    // SUM(ResolutionWidth+k) become SUM+COUNT + a post-agg Compute. Only when
    // there's no HAVING (it would bind to the original aliases the base set
    // replaces; global+HAVING is declined regardless). Tried BEFORE the
    // metadata lane: a reduced integer SUM narrows through `__narrow_bigint`'s
    // i64 range check (the documented overflow dialect), which the metadata
    // lane's canonical-type emit would silently skip.
    if (plan.having_filter == null) {
        if (try affine_agg.reduce(input.node_arena, table.schema.columns, &.{}, plan.group_by.aggs, plan.derived, &.{})) |red| {
            return try buildGlobalAggregateReduced(input, table, plan, red);
        }
    }

    // Metadata-only lane: a bare global aggregate (no WHERE/HAVING/derived)
    // mixing COUNT(*) / COUNT(col) / MIN / MAX / SUM / AVG is answerable from
    // the manifest — segment row counts (− tombstones + memtable) for the
    // counts, per-segment column stats (min/max/sum/null_count) for the rest.
    // MetaAggStats.create owns the data-side preconditions (exact-stats
    // types, and for any stats-dependent spec an empty memtable and zero
    // tombstones) and returns null to fall through to the scan pipeline
    // whenever any fails.
    if (plan.where_filter == null and plan.having_filter == null and plan.derived.len == 0) {
        if (try tryMetaAggStats(input.allocator, table, plan.group_by.aggs)) |q| return q;
    }

    // The parallel reducer (v2_global_aggregate) hosts the combinable aggregates —
    // COUNT/SUM/AVG/MIN/MAX and COUNT(DISTINCT) of any type — folding per-lane and
    // merging. It declines the rest (count_if, bool_and/or, bit_or, distinct
    // SUM/AVG, approx_count_distinct, median, percentile, std/variance, first/last):
    // non-combinable or order-dependent states that can't merge across lanes. Those
    // run on the engine-neutral hash Aggregate operator (serial fold over a parallel
    // scan), the same home GROUP_CONCAT and non-combinable UDAFs decline to above.
    if (try v2_global_aggregate.tryBuild(input.allocator, table, .{
        .aggs = plan.group_by.aggs,
        .where_filter = if (plan.where_filter) |f| f.predicate else null,
        .having_filter = if (plan.having_filter) |f| f.predicate else null,
        .derived = plan.derived,
        .dop = input.effectiveDop(),
    })) |q| return q;
    return try buildGlobalOperatorAggregate(input, table, plan);
}

// Shape-side gate for the metadata-only lane: every aggregate must be
// COUNT(*), COUNT(col), MIN/MAX(col), or SUM/AVG(col) — COUNT(DISTINCT) is a
// separate AggFunc and never matches. Data-side preconditions live in
// MetaAggStats.create.
fn tryMetaAggStats(allocator: std.mem.Allocator, table: *api.Table, aggs: []const exec.AggSpec) !?exec.Query {
    if (aggs.len == 0) return null;
    const specs = try allocator.alloc(exec.MetaAggSpec, aggs.len);
    defer allocator.free(specs);
    for (aggs, specs) |a, *sp| {
        // The affine reduction forces output types on its base aggregates;
        // this lane emits canonical types only.
        if (a.out_type_override != null) return null;
        switch (a.func) {
            .count => {
                const col_name = a.col orelse {
                    sp.* = .{ .kind = .count_star, .out_name = a.as };
                    continue;
                };
                const idx = types.findColumn(table.schema.columns, col_name) orelse return null;
                sp.* = .{ .kind = .count_col, .col_idx = idx, .out_name = a.as };
            },
            .min, .max => {
                const col_name = a.col orelse return null;
                const idx = types.findColumn(table.schema.columns, col_name) orelse return null;
                sp.* = .{ .kind = if (a.func == .min) .min else .max, .col_idx = idx, .out_name = a.as };
            },
            .sum, .avg => {
                const col_name = a.col orelse return null;
                const idx = types.findColumn(table.schema.columns, col_name) orelse return null;
                sp.* = .{ .kind = if (a.func == .sum) .sum else .avg, .col_idx = idx, .out_name = a.as };
            },
            else => return null,
        }
    }
    return exec.metaAggStats(allocator, table, specs);
}

// Build the global aggregate over the reduced base set, then layer the late
// materialization: a Compute deriving each original output from the base
// aggregates and a Project to the original SELECT order.
fn buildGlobalAggregateReduced(
    input: CompileInput,
    table: *api.Table,
    plan: GlobalAggregatePlan,
    red: affine_agg.Reduction,
) !exec.Query {
    var q = (try v2_global_aggregate.tryBuild(input.allocator, table, .{
        .aggs = red.base_aggs,
        .where_filter = if (plan.where_filter) |f| f.predicate else null,
        .having_filter = null,
        .derived = red.pre_derived,
        .dop = input.effectiveDop(),
    })) orelse return error.UnsupportedQueryShape;
    errdefer q.deinit();
    if (red.post_derived.len > 0) q = try q.computeWithRegistry(red.post_derived, input.udf_registry);
    return try q.project(red.output_names);
}

// A non-aggregating SELECT: scan → optional WHERE → optional row-local derived
// → optional ORDER BY → optional LIMIT → optional projection. Covers the
// stream_scan / scan_topn / scan_full_sort shapes (e.g. ClickBench Q20, Q24-27).
const ScanSelectPlan = struct {
    scan: ir.Op.Scan,
    where_filter: ?ir.Op.Filter = null,
    compute_layers: [MAX_SCAN_COMPUTES][]const ir.Derived = undefined,
    compute_layer_count: usize = 0,
    order_by: ?ir.Op.OrderBy = null,
    limit: ?ir.Op.Limit = null,
    // SELECT-list columns (whitelist projection). Null = emit the scan's columns
    // as-is (e.g. SELECT *).
    project_columns: ?[]const []const u8 = null,
    // Optional per-item output aliases (`SELECT qty AS amount`). Null entries
    // keep the source name. Star expansion is not handled on this path, so a
    // plan with outputs never contains `*` items.
    project_outputs: ?[]const ?[]const u8 = null,
    // Optional per-item replace-on-collision policy: a flagged item whose
    // final name matches a star-expanded column replaces it in place instead
    // of appending (`SELECT *, qty + 1 AS qty`).
    project_replace: ?[]const bool = null,
    // Stacked `.exclude` ops in the scan body (plan-builder API), outermost
    // first. Applied as complement-projections after the body's Compute so a
    // later SELECT of an excluded name fails with ColumnNotFound.
    excludes: [MAX_SCAN_EXCLUDES][]const []const u8 = undefined,
    exclude_count: usize = 0,
};

const MAX_SCAN_COMPUTES = 4;
const MAX_SCAN_EXCLUDES = 4;

fn matchScanSelect(root: *const ir.Op) ?ScanSelectPlan {
    var op = root;
    // Top decorators — Project (SELECT list), LIMIT, ORDER BY — can nest in any
    // order above the scan body; peel each at most once.
    var project_columns: ?[]const []const u8 = null;
    var project_outputs: ?[]const ?[]const u8 = null;
    var project_replace: ?[]const bool = null;
    var limit: ?ir.Op.Limit = null;
    var order_by: ?ir.Op.OrderBy = null;
    while (true) {
        switch (op.*) {
            .select => |p| {
                if (project_columns != null) return null;
                // Plain column aliases ride along (projectNamed); star items
                // can't mix with aliases on this path — their expansion lives
                // in the net-layer projection compiler.
                if (p.outputs != null) {
                    for (p.columns) |c| {
                        if (std.mem.eql(u8, c, "*") or std.mem.endsWith(u8, c, ".*")) return null;
                    }
                }
                project_columns = p.columns;
                project_outputs = p.outputs;
                project_replace = p.replace_on_collision;
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
    // The scan body: an optional row-local Compute, an optional WHERE, and
    // any `.exclude` decorators (plan-builder API), in any order, down to
    // the table scan.
    var where_filter: ?ir.Op.Filter = null;
    var compute_layers: [MAX_SCAN_COMPUTES][]const ir.Derived = undefined;
    var compute_layer_count: usize = 0;
    var excludes: [MAX_SCAN_EXCLUDES][]const []const u8 = undefined;
    var exclude_count: usize = 0;
    while (true) {
        switch (op.*) {
            .compute => |c| {
                if (compute_layer_count == MAX_SCAN_COMPUTES) return null;
                compute_layers[compute_layer_count] = c.derived;
                compute_layer_count += 1;
                op = c.upstream;
            },
            .filter => |f| {
                if (where_filter != null) return null;
                where_filter = f;
                op = f.upstream;
            },
            .exclude => |p| {
                if (exclude_count == MAX_SCAN_EXCLUDES) return null;
                excludes[exclude_count] = p.columns;
                exclude_count += 1;
                op = p.upstream;
            },
            else => break,
        }
    }
    if (op.* != .scan) return null;
    return .{
        .scan = op.scan,
        .where_filter = where_filter,
        .compute_layers = compute_layers,
        .compute_layer_count = compute_layer_count,
        .order_by = order_by,
        .limit = limit,
        .project_columns = project_columns,
        .project_outputs = project_outputs,
        .project_replace = project_replace,
        .excludes = excludes,
        .exclude_count = exclude_count,
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
        .day_leaf => |lf| try addColumnUnique(allocator, set, lf.col),
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
    if (plan.compute_layer_count > 0) return null;

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
        if (try exec.zonemapTopN(allocator, table, input.accountant, probe.items, predicateOrAlways(plan.where_filter), o.specs, output_names, n, offset, input.effectiveDop())) |q| {
            return q;
        }
    }

    // LateScan only pays off with a real WHERE that decodes fewer columns than
    // the wide output; otherwise the naive bounded scan is as good or better.
    if (plan.where_filter == null) return null;
    if (!outputWiderThanProbe(table, output_names, probe.items)) return null;

    const specs: ?[]const exec.SortSpec = if (plan.order_by) |o| o.specs else null;
    return try exec.lateScan(allocator, table, input.accountant, probe.items, plan.where_filter.?.predicate, specs, output_names, n, offset);
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
        if (plan.project_outputs == null and plan.exclude_count == 0) {
            if (try tryScanSelectLateMat(input, table, plan, l)) |q| return q;
        }
    }

    const needed = try projectedBaseColumns(input.allocator, table, input.prune_names);
    defer if (needed) |n| input.allocator.free(n);
    const allocator = input.allocator;
    const max_dop = input.effectiveDop();

    var q = if (max_dop > 1)
        try exec.ParallelScan.create(allocator, table, input.accountant, needed, max_dop)
    else
        try exec.scanWithProjection(allocator, table, input.accountant, needed);
    errdefer q.deinit();

    if (plan.where_filter) |f| q = try q.filter(f.predicate);
    var compute_i = plan.compute_layer_count;
    while (compute_i > 0) {
        compute_i -= 1;
        const derived = plan.compute_layers[compute_i];
        if (derived.len > 0) q = try computeDerivedFused(allocator, q, derived, input.udf_registry);
    }
    // Innermost exclude first (they were collected outermost-first walking
    // down). Each is a complement-projection over the current schema, so a
    // later SELECT of an excluded name fails with ColumnNotFound.
    var ei: usize = plan.exclude_count;
    while (ei > 0) {
        ei -= 1;
        var remaining: std.ArrayListUnmanaged([]const u8) = .empty;
        defer remaining.deinit(allocator);
        keep: for (q.outputSchema()) |col| {
            for (plan.excludes[ei]) |ex| {
                if (types.columnNameEql(col.name, ex)) continue :keep;
            }
            try remaining.append(allocator, col.name);
        }
        q = try q.project(remaining.items);
    }
    if (plan.order_by) |o| {
        if (plan.limit) |l| {
            q = try q.topN(o.specs, @intCast(l.n), @intCast(l.offset));
        } else {
            q = try q.orderBy(o.specs);
        }
    } else if (plan.limit) |l| {
        q = try q.limitOffset(@intCast(l.n), @intCast(l.offset));
    }
    if (plan.project_columns) |cols| {
        var has_star = false;
        for (cols) |c| {
            if (std.mem.eql(u8, c, "*") or std.mem.endsWith(u8, c, ".*")) has_star = true;
        }
        if (has_star) {
            // `*` / `alias.*` over a single-table block expands to the SOURCE
            // columns — every upstream column except the block's own derived
            // ones, which the SELECT list names explicitly (V1 semantics:
            // star is the table's columns). The alias qualifies nothing in a
            // single-source block, so expansion uses the bare names.
            var names: std.ArrayListUnmanaged([]const u8) = .empty;
            defer names.deinit(allocator);
            for (cols, 0..) |c, ci| {
                if (std.mem.eql(u8, c, "*") or std.mem.endsWith(u8, c, ".*")) {
                    expand: for (q.outputSchema()) |col| {
                        // A derived column appended by the block is named
                        // explicitly by the SELECT list, so the star skips it
                        // — UNLESS it REPLACES a source column of the same
                        // name (Compute merged it in place), in which case it
                        // stays at its source position and the explicit item
                        // below is dropped.
                        for (plan.compute_layers[0..plan.compute_layer_count]) |layer| {
                            for (layer) |d| {
                                if (types.columnNameEql(d.name, col.name) and
                                    !planReplacesName(plan, d.name)) continue :expand;
                            }
                        }
                        try names.append(allocator, col.name);
                    }
                } else {
                    // matchScanSelect declines star lists with output
                    // aliases, so non-star items here carry no rename. A
                    // replacement item whose name the star already emitted
                    // in place is redundant — drop it.
                    if (planItemReplaces(plan, ci) and nameAppended(names.items, c)) continue;
                    try names.append(allocator, c);
                }
            }
            q = try q.project(names.items);
        } else if (plan.project_outputs) |outs| {
            const names = try allocator.alloc([]const u8, cols.len);
            defer allocator.free(names);
            for (cols, 0..) |c, i| names[i] = outs[i] orelse c;
            q = try q.projectNamed(cols, names);
        } else {
            q = try q.project(cols);
        }
    }
    return q;
}

/// Whether projection item `index` is flagged replace-on-collision.
fn planItemReplaces(plan: ScanSelectPlan, index: usize) bool {
    const flags = plan.project_replace orelse return false;
    return index < flags.len and flags[index];
}

/// Whether any replace-flagged projection item targets the output name
/// `name` (its alias, or its source column when unaliased).
fn planReplacesName(plan: ScanSelectPlan, name: []const u8) bool {
    const cols = plan.project_columns orelse return false;
    for (cols, 0..) |c, i| {
        if (!planItemReplaces(plan, i)) continue;
        const target = if (plan.project_outputs) |outs| (outs[i] orelse c) else c;
        if (types.columnNameEql(target, name)) return true;
    }
    return false;
}

fn nameAppended(names: []const []const u8, name: []const u8) bool {
    for (names) |n| if (types.columnNameEql(n, name)) return true;
    return false;
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

fn hasConcatAgg(aggs: []const exec.AggSpec) bool {
    for (aggs) |a| if (a.func == .group_concat) return true;
    return false;
}

fn hasMaxByAgg(aggs: []const exec.AggSpec) bool {
    for (aggs) |a| if (a.func == .max_by) return true;
    return false;
}

// Global (no GROUP BY) UDAF / GROUP_CONCAT: V2-built scan feeding the
// engine-neutral aggregate operators. ORDER BY / LIMIT over the one-row
// result are no-ops the matcher already discarded.
fn buildGlobalOperatorAggregate(input: CompileInput, table: *api.Table, plan: GlobalAggregatePlan) !exec.Query {
    const allocator = input.allocator;
    const needed = try projectedBaseColumns(allocator, table, input.prune_names);
    defer if (needed) |n| allocator.free(n);
    const max_dop = input.effectiveDop();

    var q = if (max_dop > 1)
        try exec.ParallelScan.create(allocator, table, input.accountant, needed, max_dop)
    else
        try exec.scanWithProjection(allocator, table, input.accountant, needed);
    errdefer q.deinit();

    if (plan.where_filter) |f| q = try q.filter(f.predicate);
    if (plan.derived.len > 0) q = try computeDerivedFused(allocator, q, plan.derived, input.udf_registry);
    if (hasUdfAgg(plan.group_by.aggs)) {
        const registry = input.udf_registry orelse return error.UnsupportedQueryShape;
        q = try q.udfGroupBy(&.{}, plan.group_by.aggs, registry);
    } else {
        q = try q.aggregate(plan.group_by.aggs);
    }
    if (plan.having_filter) |f| q = try q.filter(f.predicate);
    return q;
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
