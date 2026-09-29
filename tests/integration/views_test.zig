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

fn runIn(allocator: std.mem.Allocator, db: *thindb.Database, database: []const u8, sql: []const u8) !helpers.RunResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const cat = db.catalog.?;
    const session: thindb.Session = .{ .current_db = database };
    const tables: thindb.net.SessionTables = .{ .catalog = cat, .session = session };
    const root = try thindb.sql.parseWithContext(
        arena.allocator(),
        sql,
        .neutral,
        &cat.udfs,
        .{ .registry = &cat.sql_fns, .db = database, .views = &cat.views, .tables = tables.columns() },
    );
    const cq = try thindb.net.compileWithSession(allocator, db, session, root);
    return .{ .arena = arena, .cq = cq, .owned_vars = cq.sessionValue().vars, .backing_allocator = allocator };
}

fn execIn(allocator: std.mem.Allocator, db: *thindb.Database, database: []const u8, sql: []const u8) !void {
    var q = try runIn(allocator, db, database, sql);
    defer q.deinit();
    while (try q.next()) |_| {}
}

fn firstIntIn(allocator: std.mem.Allocator, db: *thindb.Database, database: []const u8, sql: []const u8) !i64 {
    var q = try runIn(allocator, db, database, sql);
    defer q.deinit();
    const batch = (try q.next()) orelse return error.NoRows;
    return switch (batch.values[0].data) {
        .int => |s| s[0],
        .bigint => |s| s[0],
        else => error.NotInt,
    };
}

test "DROP DATABASE takes its views and functions with it (#370)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const expectEqual = std.testing.expectEqual;
    const expectError = std.testing.expectError;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        try exec(allocator, db, "CREATE DATABASE probe");
        try exec(allocator, db, "CREATE DATABASE probe_v");
        for ([_][]const u8{ "probe", "probe_v" }) |database| {
            try execIn(allocator, db, database, "CREATE VIEW ghost AS SELECT 1 AS one");
            try execIn(allocator, db, database, "CREATE MATERIALIZED VIEW mv AS SELECT 5 AS five");
            try execIn(allocator, db, database, "CREATE FUNCTION gf(x BIGINT) RETURNS TABLE AS (SELECT x AS one)");
        }
        try expectEqual(@as(i64, 1), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM ghost"));
        try expectEqual(@as(i64, 7), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM gf(7)"));

        try exec(allocator, db, "DROP DATABASE probe_v");
        // A session still using the dropped database defines nothing there.
        try expectError(error.DatabaseNotFound, execIn(allocator, db, "probe_v", "CREATE VIEW late AS SELECT 3 AS three"));
        try expectError(error.DatabaseNotFound, execIn(allocator, db, "probe_v", "CREATE FUNCTION lf(x BIGINT) RETURNS TABLE AS (SELECT x AS three)"));
        try exec(allocator, db, "CREATE DATABASE probe_v");

        try expectError(error.TableNotFound, firstIntIn(allocator, db, "probe_v", "SELECT * FROM ghost"));
        try expectError(error.TableNotFound, firstIntIn(allocator, db, "probe_v", "SELECT * FROM late"));
        try expectError(error.TableNotFound, firstIntIn(allocator, db, "probe_v", "SELECT * FROM mv"));
        try expectError(error.SqlUnsupportedFileFunction, firstIntIn(allocator, db, "probe_v", "SELECT * FROM gf(7)"));
        try expectError(error.FunctionNotFound, execIn(allocator, db, "probe_v", "SHOW CREATE FUNCTION gf"));
        try expectError(error.FunctionNotFound, execIn(allocator, db, "probe_v", "SHOW CREATE FUNCTION lf"));

        try execIn(allocator, db, "probe_v", "CREATE VIEW ghost AS SELECT 2 AS two");
        try execIn(allocator, db, "probe_v", "CREATE MATERIALIZED VIEW mv AS SELECT 6 AS six");
        try execIn(allocator, db, "probe_v", "CREATE FUNCTION gf(x BIGINT) RETURNS TABLE AS (SELECT 8 AS eight, x AS one)");
        try expectEqual(@as(i64, 2), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM ghost"));
        try expectEqual(@as(i64, 6), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM mv"));
        try expectEqual(@as(i64, 8), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM gf(7)"));
        try expectEqual(@as(i64, 1), try firstIntIn(allocator, db, "probe", "SELECT * FROM ghost"));
        try expectEqual(@as(i64, 7), try firstIntIn(allocator, db, "probe", "SELECT * FROM gf(7)"));
    }

    // A restart loads exactly what the running catalog answered.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try expectEqual(@as(i64, 2), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM ghost"));
    try expectEqual(@as(i64, 6), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM mv"));
    try expectEqual(@as(i64, 8), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM gf(7)"));
    try expectError(error.TableNotFound, firstIntIn(allocator, db, "probe_v", "SELECT * FROM late"));
    try expectEqual(@as(i64, 1), try firstIntIn(allocator, db, "probe", "SELECT * FROM ghost"));
    try expectEqual(@as(i64, 5), try firstIntIn(allocator, db, "probe", "SELECT * FROM mv"));
}
