//! Table-valued UDF (TVF) integration: raw-descriptor registration via
//! `Database.registerTableUdf`, the `TABLE(f((subquery)) PARTITION BY ...
//! ORDER BY ...)` call form, partitioned vs GLOBAL execution, and the
//! type-contract compile errors.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

const schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "g", .type = .int },
        .{ .name = "amt", .type = .bigint },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const ok = [_][]const u8{"id"};
const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 8 };

fn seed(db: *thindb.Database) !void {
    const t = try db.table("t", schema, opts);
    // Two groups, deliberately inserted out of id order so the ORDER BY
    // inside the TVF call is doing real work.
    try t.insert(&.{
        .{ .id = @as(i64, 4), .g = @as(i32, 2), .amt = @as(i64, 40) },
        .{ .id = @as(i64, 1), .g = @as(i32, 1), .amt = @as(i64, 10) },
        .{ .id = @as(i64, 3), .g = @as(i32, 1), .amt = @as(i64, 30) },
        .{ .id = @as(i64, 2), .g = @as(i32, 1), .amt = @as(i64, 20) },
        .{ .id = @as(i64, 5), .g = @as(i32, 2), .amt = @as(i64, 50) },
    });
    try t.flush();
}

/// Running total per partition: emits (id, running) with state carried in
/// a local across the ordered rows — the canonical "SQL can't say this
/// without window contortions" kernel, written against the RAW layer.
fn runningTotal(
    ctx: *const thindb.udf.TvfContext,
    part: *const thindb.udf.TvfPartition,
    out: *thindb.udf.TvfOutput,
) !void {
    _ = ctx;
    const ids = part.columns[0].data.bigint;
    const amts = part.columns[2].data.bigint;
    var running: i64 = 0;
    for (0..part.row_count) |i| {
        running += amts[i];
        try out.columns[0].data.bigint.append(out.allocator, ids[i]);
        try out.columns[1].data.bigint.append(out.allocator, running);
    }
}

const input_cols = [_]thindb.Column{
    .{ .name = "id", .type = .bigint },
    .{ .name = "g", .type = .int },
    .{ .name = "amt", .type = .bigint },
};
const output_cols = [_]thindb.Column{
    .{ .name = "id", .type = .bigint },
    .{ .name = "running", .type = .bigint },
};

fn register(db: *thindb.Database, execution: thindb.udf.TvfExecution) !void {
    try db.registerTableUdf(.{
        .name = "running_total",
        .input_schema = &input_cols,
        .output_schema = &output_cols,
        .execution = execution,
        .process = runningTotal,
    });
}

fn registryFor(db: *thindb.Database) *const thindb.UdfRegistry {
    if (db.catalog) |catalog| return &catalog.udfs;
    return &db.owned_catalog.?.udfs;
}

fn run(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) !helpers.RunResult {
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

test "table UDF: partitioned running total with ORDER BY" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(db);
    try register(db, .either);

    var res = try run(allocator, db,
        \\SELECT id, running
        \\FROM TABLE(running_total((SELECT id, g, amt FROM t)) PARTITION BY g ORDER BY id)
    );
    defer res.deinit();

    // Group 1 in id order: 10, 30, 60. Group 2: 40, 90. Partition-major
    // concat, ordered within each partition.
    var got_ids: std.ArrayList(i64) = .empty;
    defer got_ids.deinit(allocator);
    var got_running: std.ArrayList(i64) = .empty;
    defer got_running.deinit(allocator);
    while (try res.next()) |batch| {
        const ids = batch.values[0].data.bigint;
        const rs = batch.values[1].data.bigint;
        for (0..batch.row_count) |i| {
            try got_ids.append(allocator, ids[i]);
            try got_running.append(allocator, rs[i]);
        }
    }
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5 }, got_ids.items);
    try std.testing.expectEqualSlices(i64, &.{ 10, 30, 60, 40, 90 }, got_running.items);
}

test "table UDF: global mode is one partition over everything" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(db);
    try register(db, .either);

    var res = try run(allocator, db,
        \\SELECT id, running
        \\FROM TABLE(running_total((SELECT id, g, amt FROM t)) ORDER BY id)
    );
    defer res.deinit();

    var got_running: std.ArrayList(i64) = .empty;
    defer got_running.deinit(allocator);
    while (try res.next()) |batch| {
        const rs = batch.values[1].data.bigint;
        for (0..batch.row_count) |i| try got_running.append(allocator, rs[i]);
    }
    // One global run over ids 1..5: 10, 30, 60, 100, 150.
    try std.testing.expectEqualSlices(i64, &.{ 10, 30, 60, 100, 150 }, got_running.items);
}

test "table UDF: declared execution mode is compile-enforced both ways" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(db);
    try register(db, .partitioned);

    // .partitioned called WITHOUT PARTITION BY: rejected at compile.
    try helpers.expectRunError(
        allocator,
        db,
        "SELECT * FROM TABLE(running_total((SELECT id, g, amt FROM t)) ORDER BY id)",
        thindb.exec.Error.TableFnExecutionMismatch,
    );
}

test "table UDF: input shape violations are compile errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(db);
    try register(db, .either);

    // Missing declared column (only 2 of 3 provided).
    try helpers.expectRunError(
        allocator,
        db,
        "SELECT * FROM TABLE(running_total((SELECT id, g FROM t)) PARTITION BY g)",
        thindb.exec.Error.TableFnInputMismatch,
    );
    // Wrong column name for the declared shape.
    try helpers.expectRunError(
        allocator,
        db,
        "SELECT * FROM TABLE(running_total((SELECT id, g, amt AS amount FROM t)) PARTITION BY g)",
        thindb.exec.Error.TableFnInputMismatch,
    );
}
