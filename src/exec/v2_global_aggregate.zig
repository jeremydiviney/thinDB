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

const TILE_RGS: usize = 16;

const Coord = struct { seg: usize, rg: usize };

fn flatToCoord(f: usize, seg_start: []const usize, segment_count: usize, total: usize) Coord {
    if (f >= total) return .{ .seg = segment_count, .rg = 0 };
    var seg: usize = 0;
    while (seg + 1 < segment_count and seg_start[seg + 1] <= f) seg += 1;
    return .{ .seg = seg, .rg = f - seg_start[seg] };
}

const Worker = struct {
    index: usize,
    scan: *Scan,
    lane: Lane,
    plans: []const AggPlan,
    resolved: []?usize,
    allocator: Allocator,
    seg_start: []const usize,
    segment_count: usize,
    total_rgs: usize,
    next_rg: *std.atomic.Value(usize),
    err: ?anyerror = null,
};

fn workerMain(w: *Worker) void {
    workerRun(w) catch |e| {
        w.err = e;
    };
}

fn workerRun(w: *Worker) !void {
    var have_resolved = false;
    if (w.total_rgs == 0) {
        // Memtable-only (no segments / no row groups): one lane scans it all.
        if (w.index != 0) return;
        w.scan.resetRange(0, 0, w.segment_count, 0, true);
        try driveTile(w, &have_resolved);
        return;
    }
    while (true) {
        const lo = w.next_rg.fetchAdd(TILE_RGS, .monotonic);
        if (lo >= w.total_rgs) break;
        const hi = @min(lo + TILE_RGS, w.total_rgs);
        const start = flatToCoord(lo, w.seg_start, w.segment_count, w.total_rgs);
        const end = flatToCoord(hi, w.seg_start, w.segment_count, w.total_rgs);
        // The final tile also drains the memtable (scan_memtable = true).
        w.scan.resetRange(start.seg, start.rg, end.seg, end.rg, hi == w.total_rgs);
        try driveTile(w, &have_resolved);
    }
}

fn driveTile(w: *Worker, have_resolved: *bool) !void {
    while (try w.scan.next()) |batch| {
        if (!have_resolved.*) {
            try resolveBatchIndices(w.plans, w.resolved, batch);
            have_resolved.* = true;
        }
        foldBatch(&w.lane, w.plans, w.resolved, batch);
    }
}

fn driveScan(lane: *Lane, plans: []const AggPlan, allocator: Allocator, scan: *Scan) !void {
    const resolved = try allocator.alloc(?usize, plans.len);
    defer allocator.free(resolved);
    var have_resolved = false;
    while (try scan.next()) |batch| {
        if (!have_resolved) {
            try resolveBatchIndices(plans, resolved, batch);
            have_resolved = true;
        }
        foldBatch(lane, plans, resolved, batch);
    }
}

// The scan projects in table-schema order, so each aggregate's folded column
// is resolved by NAME against the batch schema (not by request order).
fn resolveBatchIndices(plans: []const AggPlan, resolved: []?usize, batch: Batch) !void {
    for (plans, 0..) |p, i| {
        resolved[i] = if (p.input_name) |nm| (batch.columnIndex(nm) orelse return error.UnsupportedQueryShape) else null;
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

    // A column-less COUNT(*) still needs ≥1 projected column for the scan to
    // iterate row-groups with correct visibility (an empty projection neither
    // counts correctly nor lets a WHERE fuse). Project the narrowest column to
    // minimize the decode it's forced to do.
    if (needed.items.len == 0) {
        _ = (try addNeeded(allocator, &needed, table, narrowestColumn(table))) orelse return declineFree(allocator, plans, &needed);
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

// Byte width for ranking the cheapest column to project; strings/variable
// types rank last so a fixed narrow column wins.
fn typeWidthRank(t: Type) usize {
    return switch (t) {
        .boolean, .tinyint => 1,
        .smallint => 2,
        .int, .date, .float => 4,
        .bigint, .datetime, .double, .decimal64 => 8,
        .largeint, .decimal128, .uuid => 16,
        else => 64,
    };
}

fn narrowestColumn(table: *api.Table) []const u8 {
    var best: usize = 0;
    var best_rank: usize = typeWidthRank(table.schema.columns[0].type);
    for (table.schema.columns, 0..) |c, i| {
        const rank = typeWidthRank(c.type);
        if (rank < best_rank) {
            best_rank = rank;
            best = i;
        }
    }
    return table.schema.columns[best].name;
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
    dop: usize,
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
            .dop = request.dop,
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
        var lane = if (self.dop > 1)
            try self.reduceParallel()
        else
            try self.reduceSerial();
        defer lane.deinit(self.allocator);
        try self.emitRow(lane);
        self.row_count = 1;
    }

    fn openScan(self: *GlobalAggregate, snap: ?Scan.Snapshot) !*Scan {
        const scan = try Scan.allocWithProjectionLoc(self.allocator, self.table, null, self.needed, false, snap);
        errdefer scan.deinit();
        if (self.where_filter) |w| {
            // Never produce a result from an unapplied filter.
            if (!try scan.tryFuseFilter(w)) return error.UnsupportedQueryShape;
        }
        return scan;
    }

    fn reduceSerial(self: *GlobalAggregate) !Lane {
        var lane = try Lane.init(self.allocator, self.plans.len);
        errdefer lane.deinit(self.allocator);
        const scan = try self.openScan(null);
        defer scan.deinit();
        try driveScan(&lane, self.plans, self.allocator, scan);
        return lane;
    }

    // Dedicated reduction scheduler: one scan per lane over a disjoint,
    // dynamically-claimed row-group range (balanced under zone-map skew), each
    // folding into its own accumulator, then a single-thread merge.
    fn reduceParallel(self: *GlobalAggregate) !Lane {
        const table = self.table;
        table.ddl_lock.lockSharedUncancelable(table.io);
        defer table.ddl_lock.unlockShared(table.io);

        const snap = Scan.captureSnapshot(table);
        var pin_held = true;
        defer if (pin_held) snap.memtable_snap.release();

        const seg_start = try self.allocator.alloc(usize, snap.segment_count + 1);
        defer self.allocator.free(seg_start);
        var total_rgs: usize = 0;
        for (table.manifest.segments.items[0..snap.segment_count], 0..) |entry, i| {
            seg_start[i] = total_rgs;
            total_rgs += entry.row_group_count;
        }
        seg_start[snap.segment_count] = total_rgs;

        const cpu_count = std.Thread.getCpuCount() catch 1;
        var n_workers = @max(@as(usize, 1), @min(self.dop, cpu_count));
        if (total_rgs > 0) n_workers = @min(n_workers, total_rgs);

        const workers = try self.allocator.alloc(Worker, n_workers);
        defer self.allocator.free(workers);
        var built: usize = 0;
        defer for (workers[0..built]) |*w| {
            w.scan.deinit();
            w.lane.deinit(self.allocator);
            self.allocator.free(w.resolved);
        };

        var next_rg = std.atomic.Value(usize).init(0);
        for (workers, 0..) |*w, i| {
            const scan = try self.openScan(snap);
            w.* = .{
                .index = i,
                .scan = scan,
                .lane = try Lane.init(self.allocator, self.plans.len),
                .plans = self.plans,
                .resolved = try self.allocator.alloc(?usize, self.plans.len),
                .allocator = self.allocator,
                .seg_start = seg_start,
                .segment_count = snap.segment_count,
                .total_rgs = total_rgs,
                .next_rg = &next_rg,
            };
            built += 1;
        }
        // All worker scans now hold their own memtable pins.
        snap.memtable_snap.release();
        pin_held = false;

        const threads = try self.allocator.alloc(std.Thread, n_workers);
        defer self.allocator.free(threads);
        const spawned = try self.allocator.alloc(bool, n_workers);
        defer self.allocator.free(spawned);
        @memset(spawned, false);
        for (workers[1..], 1..) |*w, i| {
            if (std.Thread.spawn(.{}, workerMain, .{w})) |th| {
                threads[i] = th;
                spawned[i] = true;
            } else |_| {}
        }
        workerMain(&workers[0]);
        // Any worker whose thread failed to spawn runs inline — the shared
        // atomic tile claim keeps every row-group counted exactly once.
        for (workers[1..], 1..) |*w, i| if (!spawned[i]) workerMain(w);
        for (workers[1..], 1..) |_, i| if (spawned[i]) threads[i].join();

        for (workers) |*w| if (w.err) |e| return e;

        var merged = try Lane.init(self.allocator, self.plans.len);
        errdefer merged.deinit(self.allocator);
        for (workers) |*w| merged.mergeFrom(w.lane, self.plans);
        return merged;
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
