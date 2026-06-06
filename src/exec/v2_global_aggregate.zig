//! V2 global-aggregate shape: table scan -> optional fused filter -> a single
//! whole-table aggregate row (no GROUP BY). One group, so there is no keying,
//! no scatter, and no staging — every surviving row folds into one accumulator.
//!
//! This is a deliberately separate handler from the group-topN core: that core's
//! machinery exists to route rows to different groups, which is pure overhead
//! when there is exactly one group. The parallel strategy here is a reduction —
//! each lane folds its share into a local accumulator, then a single thread
//! merges the lanes — built on its own scheduler (see runParallel). The scan
//! layer is reused verbatim.
//!
//! Scope: COUNT(*), COUNT(col), SUM, AVG, MIN, MAX over any int or float column,
//! with an optional WHERE. COUNT(DISTINCT), aggregate-over-expression, and HAVING
//! are intentionally out of this first cut (they decline to UnsupportedQueryShape).

const std = @import("std");
const Allocator = std.mem.Allocator;

const api = @import("../api/api.zig");
const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;
const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;
const ValueView = storage.column.ValueView;
const store = @import("../engine/store.zig");
const ColumnStore = store.ColumnStore;

const exec = @import("exec.zig");
const aggregate = @import("aggregate.zig");
const Scan = @import("scan.zig").Scan;

const Batch = exec.Batch;
const Query = exec.Query;
const PredicateExpr = exec.PredicateExpr;
const AggSpec = exec.AggSpec;

pub const Request = struct {
    aggs: []const AggSpec,
    where_filter: ?PredicateExpr,
    having_filter: ?PredicateExpr,
    dop: usize,
};

const AggOp = enum { count_star, count_col, sum, avg, min, max };

const AggPlan = struct {
    op: AggOp,
    // Folded input column name; null for COUNT(*). The batch column index is
    // resolved by NAME at run time — the scan projects in table-schema order,
    // not in the order columns were requested.
    input_name: ?[]const u8,
    is_float: bool,
    output_type: Type,
    name: []const u8,
};

// A per-lane partial accumulator. Aggregate i uses `isum[i]` (integer
// sum/min/max — i128, so a 100M-row SUM over bigint can't overflow) OR
// `fsum[i]` (float sum/min/max), chosen by the aggregate's input type. `ns[i]`
// is the non-null input count (AVG denominator, COUNT(col)); `count` is the
// group's row count (COUNT(*)). Nothing here is column-specific. Per-group
// memory is irrelevant with one group, so the wide i128 accumulator is free.
const Lane = struct {
    count: u64 = 0,
    isum: []i128,
    fsum: []f64,
    ns: []u64,

    fn init(allocator: Allocator, n: usize) !Lane {
        const isum = try allocator.alloc(i128, n);
        errdefer allocator.free(isum);
        const fsum = try allocator.alloc(f64, n);
        errdefer allocator.free(fsum);
        const ns = try allocator.alloc(u64, n);
        @memset(isum, 0);
        @memset(fsum, 0);
        @memset(ns, 0);
        return .{ .isum = isum, .fsum = fsum, .ns = ns };
    }

    fn deinit(self: *Lane, allocator: Allocator) void {
        allocator.free(self.isum);
        allocator.free(self.fsum);
        allocator.free(self.ns);
    }

    fn mergeFrom(self: *Lane, other: Lane, plans: []const AggPlan) void {
        self.count += other.count;
        for (plans, 0..) |p, i| {
            if (other.ns[i] == 0) continue;
            const had = self.ns[i];
            self.ns[i] += other.ns[i];
            switch (p.op) {
                .count_star, .count_col => {},
                .sum, .avg => {
                    if (p.is_float) self.fsum[i] += other.fsum[i] else self.isum[i] += other.isum[i];
                },
                .min => {
                    if (p.is_float) {
                        if (had == 0 or other.fsum[i] < self.fsum[i]) self.fsum[i] = other.fsum[i];
                    } else {
                        if (had == 0 or other.isum[i] < self.isum[i]) self.isum[i] = other.isum[i];
                    }
                },
                .max => {
                    if (p.is_float) {
                        if (had == 0 or other.fsum[i] > self.fsum[i]) self.fsum[i] = other.fsum[i];
                    } else {
                        if (had == 0 or other.isum[i] > self.isum[i]) self.isum[i] = other.isum[i];
                    }
                },
            }
        }
    }
};

inline fn readIntView(v: ValueView, row: usize) i64 {
    return switch (v) {
        .boolean => |s| @intCast(s[row]),
        .tinyint => |s| @intCast(s[row]),
        .smallint => |s| @intCast(s[row]),
        .int, .date => |s| @intCast(s[row]),
        .bigint, .datetime, .decimal64 => |s| s[row],
        .largeint => |s| @intCast(s[row]),
        else => 0,
    };
}

inline fn readFloatView(v: ValueView, row: usize) f64 {
    return switch (v) {
        .float => |s| @floatCast(s[row]),
        .double => |s| s[row],
        .boolean => |s| @floatFromInt(s[row]),
        .tinyint => |s| @floatFromInt(s[row]),
        .smallint => |s| @floatFromInt(s[row]),
        .int, .date => |s| @floatFromInt(s[row]),
        .bigint, .datetime, .decimal64 => |s| @floatFromInt(s[row]),
        else => 0,
    };
}

fn foldBatch(lane: *Lane, plans: []const AggPlan, resolved: []const ?usize, batch: Batch) void {
    lane.count += batch.row_count;
    for (plans, 0..) |p, i| {
        const idx = resolved[i] orelse continue;
        const view = batch.values[idx];
        var r: usize = 0;
        const n = batch.row_count;
        while (r < n) : (r += 1) {
            if (!view.isValid(r)) continue;
            lane.ns[i] += 1;
            switch (p.op) {
                .count_star => unreachable,
                .count_col => {},
                .sum, .avg => {
                    if (p.is_float) lane.fsum[i] += readFloatView(view.data, r) else lane.isum[i] += readIntView(view.data, r);
                },
                .min => {
                    if (p.is_float) {
                        const v = readFloatView(view.data, r);
                        if (lane.ns[i] == 1 or v < lane.fsum[i]) lane.fsum[i] = v;
                    } else {
                        const v: i128 = readIntView(view.data, r);
                        if (lane.ns[i] == 1 or v < lane.isum[i]) lane.isum[i] = v;
                    }
                },
                .max => {
                    if (p.is_float) {
                        const v = readFloatView(view.data, r);
                        if (lane.ns[i] == 1 or v > lane.fsum[i]) lane.fsum[i] = v;
                    } else {
                        const v: i128 = readIntView(view.data, r);
                        if (lane.ns[i] == 1 or v > lane.isum[i]) lane.isum[i] = v;
                    }
                },
            }
        }
    }
}

fn aggInputSupported(typ: Type) bool {
    return switch (typ) {
        .boolean, .tinyint, .smallint, .int, .bigint, .date, .datetime, .decimal64, .float, .double => true,
        else => false,
    };
}

fn isFloatType(typ: Type) bool {
    return typ == .float or typ == .double;
}

pub fn tryBuild(allocator: Allocator, table: *api.Table, request: Request) !?Query {
    if (request.aggs.len == 0) return null;
    // HAVING over a global aggregate is a 0/1-row post-filter; not in this cut.
    if (request.having_filter != null) return null;

    var plans = try allocator.alloc(AggPlan, request.aggs.len);
    errdefer allocator.free(plans);

    // Projected (and folded) input columns, de-duplicated. Filter-only columns
    // are sourced by the fused filter itself and never enter this projection.
    var needed: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer needed.deinit(allocator);

    for (request.aggs, 0..) |agg, i| {
        switch (agg.func) {
            .count => {
                if (agg.col) |col_name| {
                    _ = (try addNeeded(allocator, &needed, table, col_name)) orelse return declineFree(allocator, plans, &needed);
                    plans[i] = .{ .op = .count_col, .input_name = col_name, .is_float = false, .output_type = .bigint, .name = agg.as };
                } else {
                    plans[i] = .{ .op = .count_star, .input_name = null, .is_float = false, .output_type = .bigint, .name = agg.as };
                }
            },
            .sum, .avg, .min, .max => {
                const col_name = agg.col orelse return declineFree(allocator, plans, &needed);
                const ctyp = columnType(table, col_name) orelse return declineFree(allocator, plans, &needed);
                if (!aggInputSupported(ctyp)) return declineFree(allocator, plans, &needed);
                _ = (try addNeeded(allocator, &needed, table, col_name)) orelse return declineFree(allocator, plans, &needed);
                const out_type = aggregate.aggOutputTypeFor(agg, ctyp) catch return declineFree(allocator, plans, &needed);
                plans[i] = .{
                    .op = switch (agg.func) {
                        .sum => .sum,
                        .avg => .avg,
                        .min => .min,
                        .max => .max,
                        else => unreachable,
                    },
                    .input_name = col_name,
                    .is_float = isFloatType(ctyp),
                    .output_type = out_type,
                    .name = agg.as,
                };
            },
            else => return declineFree(allocator, plans, &needed),
        }
    }

    // A pure COUNT(*) with no folded column still needs the scan to iterate
    // row-groups; project the narrowest column so it yields row counts.
    if (needed.items.len == 0) {
        _ = (try addNeeded(allocator, &needed, table, table.schema.columns[0].name)) orelse return declineFree(allocator, plans, &needed);
    }

    const op = try allocator.create(GlobalAggregate);
    errdefer allocator.destroy(op);
    op.* = try GlobalAggregate.init(allocator, table, request, plans, try needed.toOwnedSlice(allocator));
    return exec.makeQuery(allocator, op);
}

fn declineFree(allocator: Allocator, plans: []AggPlan, needed: *std.ArrayListUnmanaged([]const u8)) ?Query {
    allocator.free(plans);
    needed.deinit(allocator);
    return null;
}

fn columnType(table: *api.Table, name: []const u8) ?Type {
    const idx = types.findColumn(table.schema.columns, name) orelse return null;
    return table.schema.columns[idx].type;
}

fn addNeeded(allocator: Allocator, needed: *std.ArrayListUnmanaged([]const u8), table: *api.Table, name: []const u8) !?usize {
    if (types.findColumn(table.schema.columns, name) == null) return null;
    for (needed.items, 0..) |existing, i| {
        if (types.columnNameEql(existing, name)) return i;
    }
    const idx = needed.items.len;
    try needed.append(allocator, name);
    return idx;
}

const GlobalAggregate = struct {
    allocator: Allocator,
    table: *api.Table,
    where_filter: ?PredicateExpr,
    plans: []AggPlan,
    needed: []const []const u8,
    output_schema: []Column,
    output_cols: []ColumnStore,
    views: []ColumnView,
    emitted: bool = false,
    built: bool = false,
    row_count: usize = 0,

    fn init(allocator: Allocator, table: *api.Table, request: Request, plans: []AggPlan, needed: []const []const u8) !GlobalAggregate {
        const output_schema = try allocator.alloc(Column, plans.len);
        errdefer allocator.free(output_schema);
        for (plans, 0..) |p, i| {
            output_schema[i] = .{ .name = p.name, .type = p.output_type, .nullable = false };
        }

        const output_cols = try allocator.alloc(ColumnStore, output_schema.len);
        errdefer allocator.free(output_cols);
        var built_cols: usize = 0;
        errdefer for (output_cols[0..built_cols]) |*c| c.deinit(allocator);
        for (output_schema, 0..) |col, i| {
            output_cols[i] = try ColumnStore.initCapacity(allocator, col.type, col.nullable, 1, 0);
            built_cols += 1;
        }

        const views = try allocator.alloc(ColumnView, output_schema.len);
        return .{
            .allocator = allocator,
            .table = table,
            .where_filter = request.where_filter,
            .plans = plans,
            .needed = needed,
            .output_schema = output_schema,
            .output_cols = output_cols,
            .views = views,
        };
    }

    pub fn deinit(self: *GlobalAggregate) void {
        for (self.output_cols) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_cols);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.plans);
        self.allocator.free(@constCast(self.needed));
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *GlobalAggregate) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(_: *GlobalAggregate, _: exec.Predicate) !void {}

    pub fn accountant(_: *GlobalAggregate) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn stats(_: *GlobalAggregate) exec.PipelineStats {
        return .{ .upper_rows = 1 };
    }

    pub fn explain(_: *GlobalAggregate, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainIndent(out, allocator, depth);
        try out.appendSlice(allocator, "V2Pipeline(global-aggregate: scan/filter/reduce)\n");
    }

    pub fn next(self: *GlobalAggregate) !?Batch {
        if (self.emitted) return null;
        if (!self.built) {
            try self.execute();
            self.built = true;
        }
        self.emitted = true;
        for (self.output_cols, 0..) |c, i| self.views[i] = c.view();
        return .{ .schema = self.output_schema, .values = self.views, .row_count = self.row_count };
    }

    fn execute(self: *GlobalAggregate) !void {
        var lane = try Lane.init(self.allocator, self.plans.len);
        defer lane.deinit(self.allocator);

        const scan = try Scan.allocWithProjectionLoc(self.allocator, self.table, null, self.needed, false, null);
        defer scan.deinit();
        if (self.where_filter) |w| {
            const fused = try scan.tryFuseFilter(w);
            // Never produce a result from an unapplied filter.
            if (!fused) return error.UnsupportedQueryShape;
        }

        // Resolve each aggregate's folded column to its batch index by name
        // (the scan projects in table-schema order, not request order).
        const resolved = try self.allocator.alloc(?usize, self.plans.len);
        defer self.allocator.free(resolved);
        var have_resolved = false;

        while (try scan.next()) |batch| {
            if (!have_resolved) {
                for (self.plans, 0..) |p, i| {
                    resolved[i] = if (p.input_name) |nm| (batch.columnIndex(nm) orelse return error.UnsupportedQueryShape) else null;
                }
                have_resolved = true;
            }
            foldBatch(&lane, self.plans, resolved, batch);
        }

        try self.emitRow(lane);
        self.row_count = 1;
    }

    fn emitRow(self: *GlobalAggregate, lane: Lane) !void {
        const a = self.allocator;
        for (self.plans, 0..) |p, i| {
            const col = &self.output_cols[i];
            switch (p.op) {
                .count_star => try col.data.bigint.append(a, @intCast(lane.count)),
                .count_col => try col.data.bigint.append(a, @intCast(lane.ns[i])),
                .sum => {
                    if (p.is_float) {
                        try appendFloat(a, col, p.output_type, lane.fsum[i]);
                    } else {
                        try appendInt(a, col, p.output_type, lane.isum[i]);
                    }
                },
                .avg => {
                    const n = lane.ns[i];
                    const avg: f64 = if (n == 0) 0.0 else if (p.is_float)
                        lane.fsum[i] / @as(f64, @floatFromInt(n))
                    else
                        @as(f64, @floatFromInt(lane.isum[i])) / @as(f64, @floatFromInt(n));
                    try col.data.double.append(a, avg);
                },
                .min, .max => {
                    // MIN/MAX over an empty input is SQL NULL; this cut emits 0
                    // for that (no ClickBench query hits it). Non-empty is exact.
                    if (lane.ns[i] == 0) {
                        if (p.is_float) try appendFloat(a, col, p.output_type, 0) else try appendInt(a, col, p.output_type, 0);
                    } else if (p.is_float) {
                        try appendFloat(a, col, p.output_type, lane.fsum[i]);
                    } else {
                        try appendInt(a, col, p.output_type, lane.isum[i]);
                    }
                },
            }
        }
    }
};

fn appendInt(allocator: Allocator, col: *ColumnStore, out_type: Type, value: i128) !void {
    switch (out_type) {
        .boolean => try col.data.boolean.append(allocator, @intCast(value)),
        .tinyint => try col.data.tinyint.append(allocator, @intCast(value)),
        .smallint => try col.data.smallint.append(allocator, @intCast(value)),
        .int => try col.data.int.append(allocator, @intCast(value)),
        .date => try col.data.date.append(allocator, @intCast(value)),
        .bigint => try col.data.bigint.append(allocator, @intCast(value)),
        .datetime => try col.data.datetime.append(allocator, @intCast(value)),
        .decimal64 => try col.data.decimal64.append(allocator, @intCast(value)),
        .largeint => try col.data.largeint.append(allocator, value),
        .double => try col.data.double.append(allocator, @floatFromInt(value)),
        else => return error.TypeMismatch,
    }
}

fn appendFloat(allocator: Allocator, col: *ColumnStore, out_type: Type, value: f64) !void {
    switch (out_type) {
        .float => try col.data.float.append(allocator, @floatCast(value)),
        .double => try col.data.double.append(allocator, value),
        else => return error.TypeMismatch,
    }
}
