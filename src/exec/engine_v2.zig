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
};

fn matchGroupTopN(root: *const ir.Op) ?GroupTopNPlan {
    var op = root;
    var top_project: ?ir.Op.Project = null;
    if (op.* == .select) {
        top_project = op.select;
        op = op.select.upstream;
    }

    var limit: ?ir.Op.Limit = null;
    var order_by: ?ir.Op.OrderBy = null;
    if (op.* == .limit) {
        limit = op.limit;
        op = op.limit.upstream;
    }
    if (op.* == .order_by) {
        order_by = op.order_by;
        op = op.order_by.upstream;
    }
    var having_filter: ?ir.Op.Filter = null;
    if (op.* == .filter) {
        having_filter = op.filter;
        op = op.filter.upstream;
    }
    if (op.* != .group_by) return null;
    const group_by = op.group_by;
    if (group_by.group_cols.len == 0) return null;
    if (top_project) |p| {
        if (!projectMatchesGroupOutput(p, group_by)) return null;
    }

    var where_filter: ?ir.Op.Filter = null;
    var source = group_by.upstream;
    if (source.* == .filter) {
        where_filter = source.filter;
        source = source.filter.upstream;
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
    };
}

fn projectMatchesGroupOutput(project: ir.Op.Project, group_by: ir.Op.GroupBy) bool {
    if (project.outputs) |outputs| {
        for (outputs) |out| if (out != null) return false;
    }
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
        .dop = input.db.config.max_dop,
    };

    if (try v2_pipeline.tryBuildGroupTopN(input.allocator, table, request)) |q| {
        return q;
    }

    return error.UnsupportedQueryShape;
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

    return v2_global_aggregate.tryBuild(input.allocator, table, .{
        .aggs = plan.group_by.aggs,
        .where_filter = if (plan.where_filter) |f| f.predicate else null,
        .having_filter = if (plan.having_filter) |f| f.predicate else null,
        .derived = plan.derived,
        .dop = input.db.config.max_dop,
    });
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
