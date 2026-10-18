//! Series — the row source behind `generate_series(start, stop[, step])`.
//! For each upstream row it emits the integers from that row's start column
//! to its stop column inclusive, step apart, as one BIGINT column. A NULL
//! argument gives the row no values, as does a step pointing away from stop
//! (PostgreSQL, DuckDB and StarRocks agree on both).

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

const BATCH_ROWS = 4096;

const Error = exec.Error;

pub const Series = struct {
    allocator: Allocator,
    upstream: Query,
    upstream_done: bool = false,
    start_col: usize,
    stop_col: usize,
    step_col: ?usize,
    schema: [1]Column,
    values: []i64,
    view: [1]ColumnView = undefined,
    /// The upstream batch being expanded and its next unread row. Its views
    /// stay valid until the next upstream pull, which comes only after every
    /// row has been read into a `Run`.
    batch: ?Batch = null,
    row: usize = 0,
    run: ?Run = null,

    /// One row's sequence still to emit. i128 so stepping past an i64 bound
    /// ends the sequence instead of wrapping.
    const Run = struct {
        next: i128,
        stop: i128,
        step: i128,

        fn more(self: Run) bool {
            return if (self.step > 0) self.next <= self.stop else self.next >= self.stop;
        }
    };

    /// `start`, `stop` and `step` name upstream columns; `column` names the
    /// output column.
    pub fn create(allocator: Allocator, upstream: Query, start: []const u8, stop: []const u8, step: ?[]const u8, column: []const u8) !Query {
        const up_schema = upstream.outputSchema();
        const start_col = try integerColumn(up_schema, start);
        const stop_col = try integerColumn(up_schema, stop);
        const step_col = if (step) |name| try integerColumn(up_schema, name) else null;

        const name = try allocator.dupe(u8, column);
        errdefer allocator.free(name);
        const values = try allocator.alloc(i64, BATCH_ROWS);
        errdefer allocator.free(values);

        const self = try allocator.create(Series);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .start_col = start_col,
            .stop_col = stop_col,
            .step_col = step_col,
            .schema = .{.{ .name = name, .type = .bigint }},
            .values = values,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Series) void {
        var up = self.upstream;
        up.deinit();
        self.allocator.free(self.values);
        self.allocator.free(self.schema[0].name);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn next(self: *Series) !?Batch {
        var n: usize = 0;
        while (n < BATCH_ROWS) {
            if (self.run) |*run| {
                while (n < BATCH_ROWS and run.more()) : (n += 1) {
                    self.values[n] = @intCast(run.next);
                    run.next += run.step;
                }
                if (!run.more()) self.run = null;
            } else {
                self.run = (try self.nextRun()) orelse break;
            }
        }
        if (n == 0) return null;
        self.view[0] = .{ .data = .{ .bigint = self.values[0..n] } };
        return .{ .schema = &self.schema, .values = &self.view, .row_count = n };
    }

    fn nextRun(self: *Series) !?Run {
        while (true) {
            if (self.batch) |b| {
                while (self.row < b.row_count) {
                    const row = self.row;
                    self.row += 1;
                    if (try self.runAt(b, row)) |run| return run;
                }
                self.batch = null;
            }
            if (self.upstream_done) return null;
            self.batch = (try self.upstream.next()) orelse {
                self.upstream_done = true;
                return null;
            };
            self.row = 0;
        }
    }

    fn runAt(self: *Series, b: Batch, row: usize) !?Run {
        const start = (try integerAt(b.values[self.start_col], row)) orelse return null;
        const stop = (try integerAt(b.values[self.stop_col], row)) orelse return null;
        var step: i64 = 1;
        if (self.step_col) |c| step = (try integerAt(b.values[c], row)) orelse return null;
        if (step == 0) return Error.SeriesStepZero;
        return .{ .next = start, .stop = stop, .step = step };
    }

    pub fn outputSchema(self: *Series) []const Column {
        return &self.schema;
    }

    // A hint on the output column says nothing about which argument rows
    // produce matching values.
    pub fn addPrune(_: *Series, _: Predicate) !void {}

    /// The arguments are only known once the upstream runs, so the row count
    /// has no bound here.
    pub fn stats(_: *Series) exec.PipelineStats {
        return .{ .upper_rows = std.math.maxInt(u64) };
    }

    pub fn accountant(self: *Series) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *Series, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "Series");
        try self.upstream.explain(out, allocator, depth + 1);
    }
};

fn integerColumn(schema: []const Column, name: []const u8) !usize {
    for (schema, 0..) |c, i| {
        if (!std.mem.eql(u8, c.name, name)) continue;
        return switch (c.type) {
            .tinyint, .smallint, .int, .bigint, .largeint => i,
            else => Error.SeriesNonIntegerArgument,
        };
    }
    return error.ColumnNotFound;
}

/// Row `row` of an integer column, null when NULL. A LARGEINT outside the
/// BIGINT range can't bound a BIGINT sequence.
fn integerAt(view: ColumnView, row: usize) !?i64 {
    if (!view.isValid(row)) return null;
    return switch (view.data) {
        .tinyint => |v| v[row],
        .smallint => |v| v[row],
        .int => |v| v[row],
        .bigint => |v| v[row],
        .largeint => |v| std.math.cast(i64, v[row]) orelse error.ValueOutOfRange,
        else => Error.SeriesNonIntegerArgument,
    };
}

const SingleBatchSource = @import("single_batch.zig").SingleBatchSource;

fn drainSeries(allocator: Allocator, q: *Query) ![]i64 {
    var out: std.ArrayList(i64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        try std.testing.expect(b.row_count <= BATCH_ROWS);
        try out.appendSlice(allocator, b.values[0].data.bigint);
    }
    return out.toOwnedSlice(allocator);
}

fn argBatch(schema: []const Column, views: []const ColumnView) Batch {
    return .{ .schema = schema, .values = views, .row_count = views[0].rowCount() };
}

test "series expands each argument row, skipping NULL arguments and steps away from stop" {
    const allocator = std.testing.allocator;
    const schema = [_]Column{
        .{ .name = "a", .type = .int, .nullable = true },
        .{ .name = "b", .type = .int },
        .{ .name = "s", .type = .smallint },
    };
    // Rows: 1..3 by 1; NULL start; 5 down to 2 by -1; 0..-2 by 1 (empty);
    // 10..20 by 4.
    const starts = [_]i32{ 1, 0, 5, 0, 10 };
    const valid = [_]u8{0b1_1101};
    const stops = [_]i32{ 3, 9, 2, -2, 20 };
    const steps = [_]i16{ 1, 1, -1, 1, 4 };
    const views = [_]ColumnView{
        .{ .data = .{ .int = &starts }, .nulls = &valid },
        .{ .data = .{ .int = &stops } },
        .{ .data = .{ .smallint = &steps } },
    };
    const up = try SingleBatchSource.create(allocator, argBatch(&schema, &views));
    var q = try Series.create(allocator, up, "a", "b", "s", "generate_series");
    defer q.deinit();
    try std.testing.expectEqualStrings("generate_series", q.outputSchema()[0].name);
    const got = try drainSeries(allocator, &q);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 5, 4, 3, 2, 10, 14, 18 }, got);
}

test "series stops at the BIGINT bounds without wrapping and splits long runs into batches" {
    const allocator = std.testing.allocator;
    const max = std.math.maxInt(i64);
    const min = std.math.minInt(i64);
    const cases = .{
        .{ .start = max - 2, .stop = max, .step = 2, .want = &[_]i64{ max - 2, max } },
        .{ .start = max, .stop = max, .step = max, .want = &[_]i64{max} },
        .{ .start = min, .stop = min, .step = min, .want = &[_]i64{min} },
        .{ .start = min + 1, .stop = min, .step = -1, .want = &[_]i64{ min + 1, min } },
    };
    const schema = [_]Column{ .{ .name = "a", .type = .bigint }, .{ .name = "b", .type = .bigint }, .{ .name = "s", .type = .bigint } };
    inline for (cases) |c| {
        const starts = [_]i64{c.start};
        const stops = [_]i64{c.stop};
        const steps = [_]i64{c.step};
        const views = [_]ColumnView{ .{ .data = .{ .bigint = &starts } }, .{ .data = .{ .bigint = &stops } }, .{ .data = .{ .bigint = &steps } } };
        const up = try SingleBatchSource.create(allocator, argBatch(&schema, &views));
        var q = try Series.create(allocator, up, "a", "b", "s", "x");
        defer q.deinit();
        const got = try drainSeries(allocator, &q);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, c.want, got);
    }

    const starts = [_]i64{1};
    const stops = [_]i64{3 * BATCH_ROWS + 7};
    const views = [_]ColumnView{ .{ .data = .{ .bigint = &starts } }, .{ .data = .{ .bigint = &stops } } };
    const up = try SingleBatchSource.create(allocator, argBatch(schema[0..2], &views));
    var q = try Series.create(allocator, up, "a", "b", null, "x");
    defer q.deinit();
    const got = try drainSeries(allocator, &q);
    defer allocator.free(got);
    try std.testing.expectEqual(@as(usize, 3 * BATCH_ROWS + 7), got.len);
    for (got, 1..) |v, want| try std.testing.expectEqual(@as(i64, @intCast(want)), v);
}

test "series rejects a zero step and non-integer arguments" {
    const allocator = std.testing.allocator;
    const schema = [_]Column{ .{ .name = "a", .type = .bigint }, .{ .name = "b", .type = .bigint }, .{ .name = "s", .type = .bigint } };
    const starts = [_]i64{1};
    const stops = [_]i64{2};
    const steps = [_]i64{0};
    const views = [_]ColumnView{ .{ .data = .{ .bigint = &starts } }, .{ .data = .{ .bigint = &stops } }, .{ .data = .{ .bigint = &steps } } };
    const up = try SingleBatchSource.create(allocator, argBatch(&schema, &views));
    var q = try Series.create(allocator, up, "a", "b", "s", "x");
    defer q.deinit();
    try std.testing.expectError(Error.SeriesStepZero, q.next());

    const dschema = [_]Column{ .{ .name = "a", .type = .double }, .{ .name = "b", .type = .bigint } };
    const dstarts = [_]f64{1.5};
    const dviews = [_]ColumnView{ .{ .data = .{ .double = &dstarts } }, .{ .data = .{ .bigint = &stops } } };
    var dup = try SingleBatchSource.create(allocator, argBatch(&dschema, &dviews));
    defer dup.deinit();
    try std.testing.expectError(Error.SeriesNonIntegerArgument, Series.create(allocator, dup, "a", "b", null, "x"));
}
