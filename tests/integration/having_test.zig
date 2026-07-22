//! HAVING — post-aggregate filter. References either grouped columns
//! or aggregate aliases. Validated against the GroupBy output schema.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, region VARCHAR(8) NOT NULL, qty INT NOT NULL)");
    try exec(
        allocator,
        db,
        "INSERT INTO t (id, region, qty) VALUES (1, 'east', 10), (2, 'east', 20), (3, 'west', 5), (4, 'north', 100), (5, 'east', 30)",
    );
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

test "HAVING: filters by aggregate alias" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(
        allocator,
        db,
        "SELECT region, COUNT(id) AS cnt FROM t GROUP BY region HAVING cnt > 1 ORDER BY region ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectEqualStrings("east", batch.values[0].data.varchar.rowBytes(0));
    try std.testing.expectEqual(@as(i64, 3), batch.values[1].data.bigint[0]);
}

test "HAVING: filters by grouped column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(
        allocator,
        db,
        "SELECT region, SUM(qty) AS total FROM t GROUP BY region HAVING region = 'east'",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectEqualStrings("east", batch.values[0].data.varchar.rowBytes(0));
    try std.testing.expectEqual(@as(i64, 60), batch.values[1].data.bigint[0]);
}

test "HAVING: rejected without GROUP BY / aggregates" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const err = thindb.sql.parse(arena.allocator(), "SELECT id FROM t HAVING id > 1");
    try std.testing.expectError(thindb.sql.ParseError.SqlInvalidProjection, err);
}
