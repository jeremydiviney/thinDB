//! Views + materialized views (manual REFRESH). A plain view expands its
//! defining query at each reference; a materialized view is a real backing
//! table refreshed on demand. Both survive a catalog close/reopen.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSqlCtx = helpers.runSqlCtx;
const runSql = helpers.runSql;

fn firstInt(allocator: std.mem.Allocator, db: anytype, sql: []const u8, ctx: bool) !i64 {
    var q = if (ctx) try runSqlCtx(allocator, db, sql) else try runSql(allocator, db, sql);
    defer q.deinit();
    const batch = (try q.next()) orelse return error.NoRows;
    return switch (batch.values[0].data) {
        .int => |s| s[0],
        .bigint => |s| s[0],
        else => error.NotInt,
    };
}

fn seed(allocator: std.mem.Allocator, db: anytype) !void {
    try exec(allocator, db, "CREATE TABLE sales (id INT, region STRING, amt INT, PRIMARY KEY (id))");
    try exec(allocator, db, "INSERT INTO sales (id, region, amt) VALUES (1,'east',10),(2,'west',20),(3,'east',30)");
    const t = try db.openTable("sales", .{});
    try t.flush();
    try exec(allocator, db, "CREATE VIEW east AS SELECT id, amt FROM sales WHERE region = 'east'");
    try exec(allocator, db, "CREATE MATERIALIZED VIEW totals AS SELECT region, SUM(amt) AS s FROM sales GROUP BY region");
}

test "view expands; materialized view snapshots + refreshes" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(allocator, db);

    // Plain view expands and reflects the base table.
    try std.testing.expectEqual(@as(i64, 40), try firstInt(allocator, db, "SELECT SUM(amt) FROM east", true));

    // Materialized view is a snapshot: adding a row is invisible until REFRESH.
    try std.testing.expectEqual(@as(i64, 40), try firstInt(allocator, db, "SELECT s FROM totals WHERE region = 'east'", false));
    try exec(allocator, db, "INSERT INTO sales (id, region, amt) VALUES (4,'east',100)");
    try std.testing.expectEqual(@as(i64, 40), try firstInt(allocator, db, "SELECT s FROM totals WHERE region = 'east'", false));
    // ...but the plain view sees it live.
    try std.testing.expectEqual(@as(i64, 140), try firstInt(allocator, db, "SELECT SUM(amt) FROM east", true));

    try exec(allocator, db, "REFRESH MATERIALIZED VIEW totals");
    try std.testing.expectEqual(@as(i64, 140), try firstInt(allocator, db, "SELECT s FROM totals WHERE region = 'east'", false));
}

test "views + materialized views survive a reopen" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        try seed(allocator, db);
        try exec(allocator, db, "REFRESH MATERIALIZED VIEW totals");
    }

    // Reopen: the view registry reloads from `_views/`, the MV backing table
    // from the normal table path.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // Plain view still expands after reload.
    try std.testing.expectEqual(@as(i64, 40), try firstInt(allocator, db, "SELECT SUM(amt) FROM east", true));
    // Materialized view backing table still holds its snapshot.
    try std.testing.expectEqual(@as(i64, 40), try firstInt(allocator, db, "SELECT s FROM totals WHERE region = 'east'", false));
    // REFRESH works after reload (defining query re-parsed from persisted text).
    try exec(allocator, db, "INSERT INTO sales (id, region, amt) VALUES (9,'east',5)");
    try exec(allocator, db, "REFRESH MATERIALIZED VIEW totals");
    try std.testing.expectEqual(@as(i64, 45), try firstInt(allocator, db, "SELECT s FROM totals WHERE region = 'east'", false));
}
