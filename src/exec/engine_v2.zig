//! Engine V2 physical-planning boundary.
//!
//! Parser, SQL lowering, and IR stay unchanged. The local compile path hands
//! the resolved IR block here first; when V2 can build the selected physical
//! shape it returns an executable Query, otherwise the existing engine remains
//! the fallback.

const std = @import("std");

const api = @import("../api/api.zig");
const types = @import("../types.zig");
const ir = @import("../ir/ir.zig");
const exec = @import("exec.zig");
const v2_pipeline = @import("v2_pipeline.zig");
const v2_group_topn = @import("v2_group_topn.zig");

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
    if (getenv("THINDB_ENGINE_V2") == null) return null;
    const spec = classifySimplePipeline(root) orelse return null;
    return switch (spec.shape) {
        .group_topn => try buildGroupTopN(input, root),
        else => null,
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
    group_by: ir.Op.GroupBy,
    order_by: ir.Op.OrderBy,
    limit: ir.Op.Limit,
};

fn matchGroupTopN(root: *const ir.Op) ?GroupTopNPlan {
    var op = root;
    var top_project: ?ir.Op.Project = null;
    if (op.* == .select) {
        top_project = op.select;
        op = op.select.upstream;
    }

    if (op.* != .limit) return null;
    const limit = op.limit;
    if (limit.upstream.* != .order_by) return null;
    const order_by = limit.upstream.order_by;
    if (order_by.upstream.* != .group_by) return null;
    const group_by = order_by.upstream.group_by;
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
        .order_specs = plan.order_by.specs,
        .limit = @intCast(plan.limit.n),
        .offset = @intCast(plan.limit.offset),
        .where_filter = if (plan.where_filter) |f| f.predicate else null,
        .needed = needed,
        .dop = input.db.config.max_dop,
    };

    if (try v2_pipeline.tryBuildGroupTopN(input.allocator, table, request)) |q| {
        return q;
    }

    if (try v2_group_topn.tryCreate(input.allocator, table, .{
        .group_cols = request.group_cols,
        .aggs = request.aggs,
        .order_specs = request.order_specs,
        .limit = request.limit,
        .offset = request.offset,
        .where_filter = request.where_filter,
        .needed = request.needed,
        .dop = request.dop,
    })) |q| {
        return q;
    }

    var cur = if (input.db.config.max_dop > 1)
        try exec.ParallelScan.create(input.allocator, table, null, needed, input.db.config.max_dop)
    else
        try exec.scanWithProjection(input.allocator, table, null, needed);
    var owns_cur = true;
    errdefer if (owns_cur) cur.deinit();

    if (plan.where_filter) |f| {
        cur = try cur.filter(f.predicate);
    }

    const top_k_n = std.math.cast(u32, plan.limit.n +| plan.limit.offset) orelse return null;
    const top_k = ir.Op.TopK{
        .k = top_k_n,
        .keys = plan.order_by.specs,
    };
    cur = try cur.groupByTopK(plan.group_by.group_cols, plan.group_by.aggs, top_k, null);
    cur = try cur.topN(plan.order_by.specs, @intCast(plan.limit.n), @intCast(plan.limit.offset));
    owns_cur = false;
    return cur;
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
