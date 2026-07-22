//! Grouped MIN/MAX over temporal columns (date / datetime).
//!
//! The silo grouped core folds temporals as their day/µs ints; nullable
//! datetime inputs exercise the validity slot. Regression for the native-
//! datetime wayroll resync: `SELECT projectId, MAX(updatedAt) ... GROUP BY
//! projectId` declined with UnsupportedQueryShape because the silo's
//! aggInputSupported omitted .date/.datetime (the global lane had them).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(
        allocator,
        db,
        "CREATE TABLE ev (id BIGINT PRIMARY KEY, pid INT NOT NULL, ts DATETIME, d DATE NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO ev (id, pid, ts, d) VALUES " ++
            "(1, 10, '2026-07-01 08:00:00.000001', '2026-07-01'), " ++
            "(2, 10, '2026-07-10 05:56:45.455833', '2026-07-10'), " ++
            "(3, 20, '2026-06-15 12:00:00', '2026-06-15'), " ++
            "(4, 20, NULL, '2026-06-16'), " ++
            "(5, 30, NULL, '2026-05-01')",
    );
    const t = try db.openTable("ev", .{});
    try t.flush();
    return db;
}

test "parse probe: grouped temporal shapes parse under neutral dialect" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const shapes = [_][]const u8{
        "SELECT pid, MAX(ts) AS mx FROM ev GROUP BY pid ORDER BY pid ASC",
        "SELECT pid, MAX(ts) AS mx, MIN(ts) AS mn FROM ev GROUP BY pid ORDER BY pid",
    };
    for (shapes) |s| {
        _ = thindb.sql.parse(arena.allocator(), s) catch |err| {
            std.debug.print("PARSE FAILED [{s}]: {t}\n", .{ s, err });
            return err;
        };
    }
}

test "grouped MAX/MIN over nullable datetime (silo path)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(
        allocator,
        db,
        "SELECT pid, MAX(ts) AS mx, MIN(ts) AS mn FROM ev GROUP BY pid ORDER BY pid",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), batch.row_count);
    const mx = batch.values[1];
    const mn = batch.values[2];
    // pid 10: full range; pid 20: single non-null value; pid 30: all-NULL group.
    try std.testing.expectEqual(@as(i64, 1783663005455833), mx.data.datetime[0]);
    try std.testing.expectEqual(@as(i64, 1782892800000001), mn.data.datetime[0]);
    // pid 20's single non-null value is both extremes.
    try std.testing.expectEqual(@as(i64, 1781524800000000), mx.data.datetime[1]);
    try std.testing.expectEqual(mx.data.datetime[1], mn.data.datetime[1]);
    // pid 30 is an all-NULL group.
    try std.testing.expect(!mx.isValid(2));
    try std.testing.expect(!mn.isValid(2));
}

test "grouped MAX over date + bare MAX agrees with grouped fold" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT pid, MAX(d) AS mx FROM ev GROUP BY pid ORDER BY pid");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), batch.row_count);
    // days since epoch for 2026-07-10 / 2026-06-16 / 2026-05-01
    try std.testing.expectEqual(@as(i32, 20644), batch.values[1].data.date[0]);
    try std.testing.expectEqual(@as(i32, 20620), batch.values[1].data.date[1]);
    try std.testing.expectEqual(@as(i32, 20574), batch.values[1].data.date[2]);
}

test "grouped SUM over datetime stays a dialect error" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try std.testing.expectError(
        error.UnsupportedQueryShape,
        runSql(allocator, db, "SELECT pid, SUM(ts) AS s FROM ev GROUP BY pid"),
    );
}
