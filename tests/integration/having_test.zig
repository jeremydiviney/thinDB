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

fn setupTags(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE tags (id BIGINT PRIMARY KEY, region VARCHAR(16) NOT NULL, def INT NOT NULL)");
    // east spans defs {1,2}; west only {1} (twice, so COUNT(*) can't stand in
    // for the distinct count); north only {3}.
    try exec(
        allocator,
        db,
        "INSERT INTO tags (id, region, def) VALUES (1, 'east', 1), (2, 'east', 2), (3, 'west', 1), (4, 'west', 1), (5, 'north', 3)",
    );
    const t = try db.openTable("tags", .{});
    try t.flush();
    return db;
}

// Regression: the shape lane's plan gate accepted a COUNT(DISTINCT) alias in
// HAVING by name, but the emit-filter evaluator had no arm for the func and
// treated every leaf as false — silently returning zero rows while the
// projected count itself was correct.
test "HAVING: filters by COUNT(DISTINCT) alias" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupTags(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(
        allocator,
        db,
        "SELECT region, COUNT(DISTINCT def) AS dc FROM tags GROUP BY region HAVING dc = 2",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectEqualStrings("east", batch.values[0].data.varchar.rowBytes(0));
    try std.testing.expectEqual(@as(i64, 2), batch.values[1].data.bigint[0]);
}

test "HAVING: COUNT(DISTINCT) alias with >= over an int group key" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupTags(allocator, io, tmp.dir);
    defer db.close();

    // The distinct column must be an integer (this lane declines string
    // distinct columns loudly) — id is the distinct payload here.
    var q = try runSql(
        allocator,
        db,
        "SELECT def, COUNT(DISTINCT id) AS rc FROM tags GROUP BY def HAVING rc >= 2 ORDER BY def ASC",
    );
    defer q.deinit();
    // def 1 covers ids {1,3,4} (rc=3); defs 2 and 3 cover one id each.
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectEqual(@as(i64, 1), batch.values[0].data.int[0]);
    try std.testing.expectEqual(@as(i64, 3), batch.values[1].data.bigint[0]);
}

test "HAVING: rejected without GROUP BY / aggregates" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const err = thindb.sql.parse(arena.allocator(), "SELECT id FROM t HAVING id > 1");
    try std.testing.expectError(thindb.sql.ParseError.SqlInvalidProjection, err);
}
