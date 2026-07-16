//! Parallel reduce for GLOBAL (no group key) aggregates over a stable-data
//! upstream. The serial path drains the whole input on the connection thread
//! and folds one accumulator row; here `dop` workers share the upstream via a
//! mutex-guarded tap — the pull (a view fill over materialized stage buffers)
//! is cheap and serialized, the fold (the actual per-row aggregate work) runs
//! concurrently. Each worker is a plain global `Aggregate` over its share of
//! batches emitting one partial row; a serial combine `Aggregate` over the
//! ≤dop partials finishes.
//!
//! Safety rests on `Query.stableData()`: batch DATA must outlive further
//! `next()` calls (a worker folds its batch after releasing the tap lock while
//! another worker pulls). Only the view-struct arrays are per-`next()` scratch
//! — the tap copies those under the lock.
//!
//! Combine correctness over "empty" partials: post-#79 a global aggregate
//! over zero rows emits COUNT=0 and NULL for everything else, and every
//! combine function skips NULLs (`max_by` skips a pair when EITHER side is
//! NULL via its hidden ord twin), so a worker that saw no batches cannot
//! poison the result — including the all-empty case, which combines back to
//! the exact serial empty-input row.

const std = @import("std");
const Allocator = std.mem.Allocator;
const exec = @import("exec.zig");
const ir = @import("../ir/ir.zig");
const types = @import("../types.zig");
const aggregate_op = @import("aggregate.zig");
const ColumnView = @import("../storage/storage.zig").ColumnView;

/// The two-phase partial/combine split carries exactly these functions.
/// COUNT/SUM re-sum; MIN/MAX/ANY_VALUE fold representatives (NULL-skipping);
/// MAX_BY rides a hidden `max_by_key` twin carrying the winning ord.
pub fn combinable(aggs: []const ir.AggSpec) bool {
    for (aggs) |a| switch (a.func) {
        .count, .sum, .min, .max, .any_value, .max_by => {},
        else => return false,
    };
    return true;
}

/// Partial-phase specs: the originals plus one hidden `max_by_key` twin per
/// `max_by` so its winning ord rides along for the combine. The twin shares
/// max_by's skip-if-EITHER-NULL pair semantics — a plain MAX(ord) would count
/// NULL-value rows and could carry an ord higher than its partial's value,
/// poisoning the combine. Returns null when an ord column is missing.
pub fn partialSpecs(arena: Allocator, up_schema: []const types.Column, aggs: []const ir.AggSpec) !?[]const ir.AggSpec {
    var n_hidden: usize = 0;
    for (aggs) |a| {
        if (a.func == .max_by) n_hidden += 1;
    }
    if (n_hidden == 0) return aggs;
    const buf = try arena.alloc(ir.AggSpec, aggs.len + n_hidden);
    @memcpy(buf[0..aggs.len], aggs);
    var j: usize = aggs.len;
    for (aggs) |a| {
        if (a.func != .max_by) continue;
        const ord_name = a.arg2_col orelse return null;
        const ord_idx = types.findColumn(up_schema, ord_name) orelse return null;
        buf[j] = .{
            .func = .max_by_key,
            .col = a.col,
            .arg2_col = ord_name,
            .as = try std.fmt.allocPrint(arena, "__mb_ord__{s}", .{a.as}),
            .out_type_override = up_schema[ord_idx].type,
        };
        j += 1;
    }
    return buf;
}

/// Combine-phase specs over the partial rows, read by `.as`, type-forced to
/// the partial output type (COUNT's bigint must not be widened by SUM).
/// Hidden max_by_key columns sit past `aggs.len` in the partial schema —
/// combine inputs only, never re-emitted.
pub fn combineSpecs(
    arena: Allocator,
    aggs: []const ir.AggSpec,
    part_aggs: []const ir.AggSpec,
    part_schema: []const types.Column,
    n_group_cols: usize,
) ![]ir.AggSpec {
    const combine = try arena.alloc(ir.AggSpec, aggs.len);
    var hj: usize = aggs.len;
    for (aggs, 0..) |a, i| {
        combine[i] = .{
            .func = switch (a.func) {
                .count, .sum => .sum,
                .min => .min,
                .max => .max,
                .any_value => .any_value,
                .max_by => .max_by,
                else => unreachable,
            },
            .col = a.as,
            .as = a.as,
            .out_type_override = part_schema[n_group_cols + i].type,
        };
        if (a.func == .max_by) {
            combine[i].arg2_col = part_aggs[hj].as;
            hj += 1;
        }
    }
    return combine;
}

pub const ParallelReduceAggregate = struct {
    allocator: Allocator,
    /// Thread-safe allocator backing the per-worker arenas.
    worker_alloc: Allocator,
    upstream: exec.Query,
    /// Everything plan-shaped (specs, schemas, worker array, partials source)
    /// lives here and dies at deinit.
    spec_arena: std.heap.ArenaAllocator,
    output_schema: []const types.Column,
    part_aggs: []const ir.AggSpec,
    combine_aggs: []const ir.AggSpec,
    part_schema: []const types.Column,
    dop: usize,
    tap: Tap,
    workers: []Worker = &.{},
    n_built: usize = 0,
    combine: ?exec.Query = null,
    emitted: bool = false,

    const Tap = struct {
        mu: std.atomic.Mutex = .unlocked,
        upstream: *exec.Query,
        done: bool = false,
        err: ?anyerror = null,
    };

    /// Per-worker view of the shared tap: pulls one upstream batch under the
    /// lock, copies its view STRUCTS into worker-local scratch (the data
    /// behind them is stable — that's the routing precondition), and hands
    /// the re-pointed batch to this worker's partial Aggregate.
    const TapRef = struct {
        tap: *Tap,
        views: []ColumnView,
        schema: []const types.Column,

        pub fn next(self: *TapRef) !?exec.Batch {
            const t = self.tap;
            // Spinlock (`std.atomic.Mutex`): the hold is one buffer-backed
            // upstream.next() + a view-struct memcpy — microseconds against
            // each worker's ~64K-row fold between pulls.
            while (!t.mu.tryLock()) std.atomic.spinLoopHint();
            defer t.mu.unlock();
            if (t.err) |e| return e;
            if (t.done) return null;
            const maybe = t.upstream.next() catch |e| {
                t.err = e;
                return e;
            };
            const b = maybe orelse {
                t.done = true;
                return null;
            };
            // A digest-sidecar batch's values are placeholders — folding them
            // outside the lock would need the sidecar copied too. No stable
            // source emits one today; decline loudly rather than corrupt.
            if (b.hashed != null) {
                t.err = exec.Error.UnsupportedOperatorForType;
                return t.err.?;
            }
            @memcpy(self.views[0..b.values.len], b.values);
            return .{
                .schema = b.schema,
                .values = self.views[0..b.values.len],
                .row_count = b.row_count,
            };
        }

        pub fn deinit(_: *TapRef) void {}

        pub fn outputSchema(self: *TapRef) []const types.Column {
            return self.schema;
        }

        pub fn addPrune(_: *TapRef, _: exec.Predicate) !void {}

        pub fn stats(_: *TapRef) exec.PipelineStats {
            return .{ .upper_rows = std.math.maxInt(u64) };
        }

        pub fn accountant(_: *TapRef) ?*exec.memory.MemoryAccountant {
            return null;
        }

        pub fn explain(self: *TapRef, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) anyerror!void {
            _ = self;
            try exec.explainLine(out, alloc, depth, "SharedTap");
        }
    };

    const Worker = struct {
        arena: std.heap.ArenaAllocator,
        agg: exec.Query,
        out: ?exec.Batch = null,
        err: ?anyerror = null,
        thread: ?std.Thread = null,

        fn main(w: *Worker) void {
            w.out = w.agg.next() catch |e| blk: {
                w.err = e;
                break :blk null;
            };
        }
    };

    /// One-row batches, one per worker, feeding the serial combine.
    const PartialsSource = struct {
        workers: []Worker,
        schema: []const types.Column,
        i: usize = 0,

        pub fn next(self: *PartialsSource) !?exec.Batch {
            while (self.i < self.workers.len) {
                const b = self.workers[self.i].out;
                self.i += 1;
                if (b) |batch| {
                    if (batch.row_count > 0) return batch;
                }
            }
            return null;
        }

        pub fn deinit(_: *PartialsSource) void {}

        pub fn outputSchema(self: *PartialsSource) []const types.Column {
            return self.schema;
        }

        pub fn addPrune(_: *PartialsSource, _: exec.Predicate) !void {}

        pub fn stats(self: *PartialsSource) exec.PipelineStats {
            return .{ .upper_rows = self.workers.len };
        }

        pub fn accountant(_: *PartialsSource) ?*exec.memory.MemoryAccountant {
            return null;
        }

        pub fn explain(self: *PartialsSource, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) anyerror!void {
            _ = self;
            try exec.explainLine(out, alloc, depth, "ReducePartials");
        }
    };

    /// Returns null (without consuming `upstream`) when the aggregate set
    /// isn't two-phase combinable. The caller gates on `stableData()`, dop,
    /// and row count — this only validates the specs.
    pub fn create(
        allocator: Allocator,
        worker_alloc: Allocator,
        upstream: exec.Query,
        aggs: []const ir.AggSpec,
        dop: usize,
    ) !?exec.Query {
        if (!combinable(aggs)) return null;
        var spec_arena = std.heap.ArenaAllocator.init(allocator);
        errdefer spec_arena.deinit();
        const aa = spec_arena.allocator();

        const up_schema = upstream.outputSchema();
        const part_aggs = (try partialSpecs(aa, up_schema, aggs)) orelse {
            spec_arena.deinit();
            return null;
        };
        const part_schema = try aggregate_op.outputSchemaFor(aa, up_schema, &.{}, part_aggs);
        const combine_aggs = try combineSpecs(aa, aggs, part_aggs, part_schema, 0);
        const output_schema = try aggregate_op.outputSchemaFor(aa, up_schema, &.{}, aggs);

        const self = try allocator.create(ParallelReduceAggregate);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .worker_alloc = worker_alloc,
            .upstream = upstream,
            .spec_arena = spec_arena,
            .output_schema = output_schema,
            .part_aggs = part_aggs,
            .combine_aggs = combine_aggs,
            .part_schema = part_schema,
            .dop = @max(dop, 1),
            .tap = .{ .upstream = undefined },
        };
        self.tap.upstream = &self.upstream;
        return exec.makeQuery(allocator, self);
    }

    fn runWorkers(self: *ParallelReduceAggregate) !void {
        const aa = self.spec_arena.allocator();
        self.workers = try aa.alloc(Worker, self.dop);
        for (self.workers) |*w| {
            w.* = .{ .arena = std.heap.ArenaAllocator.init(self.worker_alloc), .agg = undefined };
            const wa = w.arena.allocator();
            const tap_ref = try wa.create(TapRef);
            tap_ref.* = .{
                .tap = &self.tap,
                .views = try wa.alloc(ColumnView, self.upstream.outputSchema().len),
                .schema = self.upstream.outputSchema(),
            };
            w.agg = try exec.makeQuery(wa, tap_ref).groupBy(&.{}, self.part_aggs);
            self.n_built += 1;
        }
        for (self.workers) |*w| {
            w.thread = std.Thread.spawn(.{}, Worker.main, .{w}) catch blk: {
                Worker.main(w);
                break :blk null;
            };
        }
        for (self.workers) |*w| {
            if (w.thread) |t| t.join();
        }
    }

    pub fn next(self: *ParallelReduceAggregate) !?exec.Batch {
        if (self.emitted) {
            if (self.combine) |*c| return c.next();
            return null;
        }
        self.emitted = true;
        try self.runWorkers();
        if (self.tap.err) |e| return e;
        for (self.workers) |*w| {
            if (w.err) |e| return e;
        }
        const aa = self.spec_arena.allocator();
        const ps = try aa.create(PartialsSource);
        ps.* = .{ .workers = self.workers, .schema = self.part_schema };
        self.combine = try exec.makeQuery(aa, ps).groupBy(&.{}, self.combine_aggs);
        return self.combine.?.next();
    }

    pub fn outputSchema(self: *ParallelReduceAggregate) []const types.Column {
        return self.output_schema;
    }

    pub fn addPrune(_: *ParallelReduceAggregate, _: exec.Predicate) !void {}

    pub fn stats(_: *ParallelReduceAggregate) exec.PipelineStats {
        return .{ .upper_rows = 1 };
    }

    pub fn accountant(self: *ParallelReduceAggregate) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *ParallelReduceAggregate, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) anyerror!void {
        var buf: [64]u8 = undefined;
        const line = std.fmt.bufPrint(&buf, "Aggregate (global: parallel reduce, dop={d})", .{self.dop}) catch "Aggregate (global: parallel reduce)";
        try exec.explainLine(out, alloc, depth, line);
        try self.upstream.explain(out, alloc, depth + 1);
    }

    pub fn deinit(self: *ParallelReduceAggregate) void {
        if (self.combine) |*c| c.deinit();
        for (self.workers[0..self.n_built]) |*w| w.agg.deinit();
        for (self.workers[0..self.n_built]) |*w| w.arena.deinit();
        self.upstream.deinit();
        self.spec_arena.deinit();
        self.allocator.destroy(self);
    }
};

// ---------------------------------------------------------------------------

const testing = std.testing;

/// A deliberately stable test source: batches view into operator-owned
/// column stores that live until deinit.
const StableRows = struct {
    allocator: Allocator,
    schema: []const types.Column,
    ids: []const i64,
    vals: []const i64,
    valid: []const bool,
    batch_rows: usize,
    cursor: usize = 0,
    views: []ColumnView,

    fn create(allocator: Allocator, schema: []const types.Column, ids: []const i64, vals: []const i64, valid: []const bool, batch_rows: usize) !exec.Query {
        const self = try allocator.create(StableRows);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .schema = schema,
            .ids = ids,
            .vals = vals,
            .valid = valid,
            .batch_rows = batch_rows,
            .views = try allocator.alloc(ColumnView, 2),
        };
        return exec.makeQuery(allocator, self);
    }

    pub fn next(self: *StableRows) !?exec.Batch {
        if (self.cursor >= self.ids.len) return null;
        const lo = self.cursor;
        const hi = @min(lo + self.batch_rows, self.ids.len);
        self.cursor = hi;
        self.views[0] = .{ .data = .{ .bigint = self.ids[lo..hi] }, .nulls = null };
        self.views[1] = .{ .data = .{ .bigint = self.vals[lo..hi] }, .nulls = null };
        return .{ .schema = self.schema, .values = self.views, .row_count = hi - lo };
    }

    pub fn deinit(self: *StableRows) void {
        self.allocator.free(self.views);
        self.allocator.destroy(self);
    }

    pub fn outputSchema(self: *StableRows) []const types.Column {
        return self.schema;
    }

    pub fn addPrune(_: *StableRows, _: exec.Predicate) !void {}

    pub fn stats(self: *StableRows) exec.PipelineStats {
        return .{ .upper_rows = self.ids.len };
    }

    pub fn accountant(_: *StableRows) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(_: *StableRows, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) anyerror!void {
        try exec.explainLine(out, alloc, depth, "StableRows");
    }

    pub fn stableData(_: *StableRows) bool {
        return true;
    }
};

test "parallel reduce matches serial global aggregate" {
    const allocator = testing.allocator;
    const schema = [_]types.Column{
        .{ .name = "id", .type = .bigint },
        .{ .name = "v", .type = .bigint },
    };
    const n = 10_000;
    const ids = try allocator.alloc(i64, n);
    defer allocator.free(ids);
    const vals = try allocator.alloc(i64, n);
    defer allocator.free(vals);
    for (ids, vals, 0..) |*id, *v, i| {
        id.* = @intCast(i);
        v.* = @intCast((i * 7) % 1000);
    }
    const aggs = [_]ir.AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "v", .as = "s" },
        .{ .func = .min, .col = "id", .as = "mn" },
        .{ .func = .max, .col = "id", .as = "mx" },
        .{ .func = .max_by, .col = "v", .arg2_col = "id", .as = "mb" },
    };

    const up = try StableRows.create(allocator, &schema, ids, vals, &.{}, 777);
    var q = (try ParallelReduceAggregate.create(allocator, std.heap.c_allocator, up, &aggs, 4)).?;
    defer q.deinit();
    const b = (try q.next()).?;
    try testing.expectEqual(@as(usize, 1), b.row_count);
    try testing.expectEqual(@as(i64, n), b.values[0].data.bigint[0]);
    var expect_sum: i128 = 0;
    for (vals) |v| expect_sum += v;
    try testing.expectEqual(expect_sum, b.values[1].data.largeint[0]);
    try testing.expectEqual(@as(i64, 0), b.values[2].data.bigint[0]);
    try testing.expectEqual(@as(i64, n - 1), b.values[3].data.bigint[0]);
    // max_by(v, id): value at the highest id.
    try testing.expectEqual(vals[n - 1], b.values[4].data.bigint[0]);
    try testing.expectEqual(@as(?exec.Batch, null), try q.next());
}

test "parallel reduce empty input keeps the serial empty dialect" {
    const allocator = testing.allocator;
    const schema = [_]types.Column{
        .{ .name = "id", .type = .bigint },
        .{ .name = "v", .type = .bigint },
    };
    const aggs = [_]ir.AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "v", .as = "s" },
        .{ .func = .min, .col = "id", .as = "mn" },
    };
    const up = try StableRows.create(allocator, &schema, &.{}, &.{}, &.{}, 64);
    var q = (try ParallelReduceAggregate.create(allocator, std.heap.c_allocator, up, &aggs, 3)).?;
    defer q.deinit();
    const b = (try q.next()).?;
    try testing.expectEqual(@as(usize, 1), b.row_count);
    // COUNT over empty = 0; SUM/MIN = NULL.
    try testing.expectEqual(@as(i64, 0), b.values[0].data.bigint[0]);
    try testing.expect(!b.values[1].isValid(0));
    try testing.expect(!b.values[2].isValid(0));
}
