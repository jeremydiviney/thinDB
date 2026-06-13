const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

const schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "g", .type = .int },
        .{ .name = "x", .type = .double },
        .{ .name = "w", .type = .double },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const ok = [_][]const u8{"id"};
const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 8 };

fn seed(db: *thindb.Database) !void {
    const t = try db.table("t", schema, opts);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .g = @as(i32, 1), .x = @as(f64, 2.0), .w = @as(f64, 1.0) },
        .{ .id = @as(i64, 2), .g = @as(i32, 1), .x = @as(f64, 3.0), .w = @as(f64, 2.0) },
        .{ .id = @as(i64, 3), .g = @as(i32, 2), .x = @as(f64, 4.0), .w = @as(f64, 1.0) },
    });
    try t.flush();
}

fn registryFor(db: *thindb.Database) *const thindb.UdfRegistry {
    if (db.catalog) |catalog| return &catalog.udfs;
    return &db.owned_catalog.?.udfs;
}

fn runSqlWithUdfs(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) !helpers.RunResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const root = try thindb.sql.parseDialectWithUdfs(arena.allocator(), sql, .neutral, registryFor(db));
    const cq = try thindb.net.compile(allocator, db, root);
    return .{
        .arena = arena,
        .cq = cq,
        .owned_vars = cq.sessionValue().vars,
        .backing_allocator = allocator,
    };
}

fn scoreBucketKernel(
    ctx: *const thindb.udf.ScalarContext,
    args: []const thindb.storage.ColumnView,
    out: *thindb.engine.ColumnStore,
    row_count: usize,
) !void {
    const xs = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const bucket: i32 = if (xs[i] < 3.0) 0 else 1;
        try out.data.int.append(ctx.allocator, bucket);
    }
}

const SumSquaresState = struct { total: f64 = 0.0 };

fn sumSquaresInit(ctx: *const thindb.udf.AggregateContext, state: *anyopaque) !void {
    _ = ctx;
    const s: *SumSquaresState = @ptrCast(@alignCast(state));
    s.* = .{};
}

fn sumSquaresUpdate(
    ctx: *const thindb.udf.AggregateContext,
    state: *anyopaque,
    args: []const thindb.storage.ColumnView,
    row: usize,
) !void {
    _ = ctx;
    const s: *SumSquaresState = @ptrCast(@alignCast(state));
    if (!args[0].isValid(row)) return;
    const x = args[0].data.double[row];
    s.total += x * x;
}

fn sumSquaresFinalize(
    ctx: *const thindb.udf.AggregateContext,
    state: *anyopaque,
    out: *thindb.engine.ColumnStore,
) !void {
    const s: *SumSquaresState = @ptrCast(@alignCast(state));
    try out.data.double.append(ctx.allocator, s.total);
    try out.appendValidBit(ctx.allocator, out.rowCount() - 1, true);
}

const WeightedAvgState = struct {
    weighted_sum: f64 = 0.0,
    weight_sum: f64 = 0.0,
};

fn weightedAvgInit(ctx: *const thindb.udf.AggregateContext, state: *anyopaque) !void {
    _ = ctx;
    const s: *WeightedAvgState = @ptrCast(@alignCast(state));
    s.* = .{};
}

fn weightedAvgUpdate(
    ctx: *const thindb.udf.AggregateContext,
    state: *anyopaque,
    args: []const thindb.storage.ColumnView,
    row: usize,
) !void {
    _ = ctx;
    if (!args[0].isValid(row) or !args[1].isValid(row)) return;
    const s: *WeightedAvgState = @ptrCast(@alignCast(state));
    const x = args[0].data.double[row];
    const w = args[1].data.double[row];
    s.weighted_sum += x * w;
    s.weight_sum += w;
}

fn weightedAvgFinalize(
    ctx: *const thindb.udf.AggregateContext,
    state: *anyopaque,
    out: *thindb.engine.ColumnStore,
) !void {
    const s: *WeightedAvgState = @ptrCast(@alignCast(state));
    const value = if (s.weight_sum == 0.0) 0.0 else s.weighted_sum / s.weight_sum;
    try out.data.double.append(ctx.allocator, value);
    try out.appendValidBit(ctx.allocator, out.rowCount() - 1, true);
}

fn registerTestUdfs(db: *thindb.Database) !void {
    try db.registerScalarUdf(.{
        .name = "score_bucket",
        .arg_types = &.{.double},
        .return_type = .int,
        .volatility = .immutable,
        .kernel = scoreBucketKernel,
    });
    try db.registerAggregateUdf(.{
        .name = "sum_squares",
        .arg_types = &.{.double},
        .return_type = .double,
        .state_size = @sizeOf(SumSquaresState),
        .state_align = @alignOf(SumSquaresState),
        .init = sumSquaresInit,
        .update_one = sumSquaresUpdate,
        .finalize = sumSquaresFinalize,
    });
    try db.registerAggregateUdf(.{
        .name = "weighted_avg",
        .arg_types = &.{ .double, .double },
        .return_type = .double,
        .state_size = @sizeOf(WeightedAvgState),
        .state_align = @alignOf(WeightedAvgState),
        .init = weightedAvgInit,
        .update_one = weightedAvgUpdate,
        .finalize = weightedAvgFinalize,
    });
}

test "scalar UDF resolves through SQL compute" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try registerTestUdfs(db);
    try seed(db);

    var q = try helpers.runSql(allocator, db, "SELECT score_bucket(x) AS b FROM t ORDER BY id");
    defer q.deinit();

    var got: std.ArrayList(i32) = .empty;
    defer got.deinit(allocator);
    while (try q.next()) |batch| {
        try got.appendSlice(allocator, batch.values[0].data.int[0..batch.row_count]);
    }
    try std.testing.expectEqualSlices(i32, &.{ 0, 1, 1 }, got.items);
}

test "aggregate UDF groups and finalizes state" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try registerTestUdfs(db);
    try seed(db);

    var q = try runSqlWithUdfs(allocator, db, "SELECT g, sum_squares(x) AS ss FROM t GROUP BY g ORDER BY g");
    defer q.deinit();

    const batch = (try q.next()).?;
    try std.testing.expectEqualSlices(i32, &.{ 1, 2 }, batch.values[0].data.int[0..batch.row_count]);
    try std.testing.expectApproxEqAbs(@as(f64, 13.0), batch.values[1].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 16.0), batch.values[1].data.double[1], 1e-9);
}

test "aggregate UDF accepts multiple column arguments" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try registerTestUdfs(db);
    try seed(db);

    var q = try runSqlWithUdfs(allocator, db, "SELECT weighted_avg(x, w) AS wa FROM t");
    defer q.deinit();

    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), batch.values[0].data.double[0], 1e-9);
}

test "aggregate UDF can mix with grouped built-in aggregates" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try registerTestUdfs(db);
    try seed(db);

    var q = try runSqlWithUdfs(allocator, db,
        \\SELECT g, sum(x) AS sx, sum_squares(x) AS ss, count(*) AS n
        \\FROM t
        \\GROUP BY g
        \\ORDER BY g
    );
    defer q.deinit();

    const batch = (try q.next()).?;
    try std.testing.expectEqualSlices(i32, &.{ 1, 2 }, batch.values[0].data.int[0..batch.row_count]);
    try std.testing.expectApproxEqAbs(@as(f64, 5.0), batch.values[1].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 4.0), batch.values[1].data.double[1], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 13.0), batch.values[2].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 16.0), batch.values[2].data.double[1], 1e-9);
    try std.testing.expectEqual(@as(i64, 2), batch.values[3].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 1), batch.values[3].data.bigint[1]);
}

test "aggregate UDF can mix with global built-in aggregates" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try registerTestUdfs(db);
    try seed(db);

    var q = try runSqlWithUdfs(allocator, db, "SELECT count(*) AS n, weighted_avg(x, w) AS wa FROM t");
    defer q.deinit();

    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectEqual(@as(i64, 3), batch.values[0].data.bigint[0]);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), batch.values[1].data.double[0], 1e-9);
}

test "multiple aggregate UDFs can mix with multiple built-ins" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try registerTestUdfs(db);
    try seed(db);

    var q = try runSqlWithUdfs(allocator, db,
        \\SELECT g, max(x) AS mx, sum_squares(x) AS ss, weighted_avg(x, w) AS wa, count(*) AS n
        \\FROM t
        \\GROUP BY g
        \\ORDER BY g
    );
    defer q.deinit();

    const batch = (try q.next()).?;
    try std.testing.expectEqualSlices(i32, &.{ 1, 2 }, batch.values[0].data.int[0..batch.row_count]);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), batch.values[1].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 4.0), batch.values[1].data.double[1], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 13.0), batch.values[2].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 16.0), batch.values[2].data.double[1], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 8.0 / 3.0), batch.values[3].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 4.0), batch.values[3].data.double[1], 1e-9);
    try std.testing.expectEqual(@as(i64, 2), batch.values[4].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 1), batch.values[4].data.bigint[1]);
}

test "UDF definitions are process-local and not persisted" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        try registerTestUdfs(db);
        var q = try helpers.runSql(allocator, db, "SELECT score_bucket(2.0) AS b");
        defer q.deinit();
        const batch = (try q.next()).?;
        try std.testing.expectEqual(@as(i32, 0), batch.values[0].data.int[0]);
    }

    var reopened = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer reopened.close();
    try helpers.expectRunError(allocator, reopened, "SELECT score_bucket(2.0) AS b", thindb.exec.Error.ComputeNoSuchOverload);
}
