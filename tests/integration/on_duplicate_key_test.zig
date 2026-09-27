//! `INSERT IGNORE` and `INSERT ... ON DUPLICATE KEY UPDATE` on unique and
//! non-unique tables, plus the MySQL INSERT spellings that feed them
//! (`SET`, `VALUE`, `ROW(...)`, a row alias). An update that sets every
//! non-key column from the new row takes the plain upsert path the Flink
//! JDBC sink relies on; any other update merges into the stored row.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSql = helpers.runSql;
const expectRunError = helpers.expectRunError;

fn collectInts(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]i64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(i64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        for (0..b.row_count) |i| try out.append(allocator, switch (b.values[0].data) {
            .int => |s| s[i],
            .bigint => |s| s[i],
            else => return error.NotInt,
        });
    }
    return out.toOwnedSlice(allocator);
}

fn expectInts(allocator: std.mem.Allocator, db: anytype, sql: []const u8, expected: []const i64) !void {
    const got = try collectInts(allocator, db, sql);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, expected, got);
}

fn affectedRows(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !u64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    while (try q.next()) |_| {}
    return q.affectedRows();
}

fn openDim(allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE dim (id INT NOT NULL, a INT NOT NULL, b INT, PRIMARY KEY (id))");
    try exec(allocator, db, "INSERT INTO dim (id, a, b) VALUES (1, 10, 100), (2, 20, 200)");
    return db;
}

test "ON DUPLICATE KEY UPDATE from every new value replaces the row" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openDim(allocator, std.testing.io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "INSERT INTO dim (id, a, b) VALUES (1, 11, 111), (3, 30, 300) ON DUPLICATE KEY UPDATE a = VALUES(a), b = VALUES(b)");
    try expectInts(allocator, db, "SELECT id FROM dim ORDER BY id", &.{ 1, 2, 3 });
    try expectInts(allocator, db, "SELECT a FROM dim ORDER BY id", &.{ 11, 20, 30 });
    try expectInts(allocator, db, "SELECT b FROM dim ORDER BY id", &.{ 111, 200, 300 });
}

test "ON DUPLICATE KEY UPDATE of some columns keeps the others" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openDim(allocator, std.testing.io, tmp.dir);
    defer db.close();

    try std.testing.expectEqual(@as(u64, 3), try affectedRows(allocator, db, "INSERT INTO dim (id, a, b) VALUES (1, 11, 111), (4, 40, 400) ON DUPLICATE KEY UPDATE a = VALUES(a)"));
    try expectInts(allocator, db, "SELECT a FROM dim ORDER BY id", &.{ 11, 20, 40 });
    try expectInts(allocator, db, "SELECT b FROM dim ORDER BY id", &.{ 100, 200, 400 });

    try exec(allocator, db, "INSERT INTO dim (id, a) VALUES (2, 0) ON DUPLICATE KEY UPDATE b = 7");
    try expectInts(allocator, db, "SELECT a FROM dim WHERE id = 2", &.{20});
    try expectInts(allocator, db, "SELECT b FROM dim WHERE id = 2", &.{7});
}

test "ON DUPLICATE KEY UPDATE accumulates, repeated keys included" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openDim(allocator, std.testing.io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "INSERT INTO dim (id, a) VALUES (1, 5), (1, 7), (5, 50), (5, 1) ON DUPLICATE KEY UPDATE a = a + VALUES(a)");
    try expectInts(allocator, db, "SELECT id FROM dim ORDER BY id", &.{ 1, 2, 5 });
    try expectInts(allocator, db, "SELECT a FROM dim ORDER BY id", &.{ 22, 20, 51 });
    try expectInts(allocator, db, "SELECT COALESCE(b, -1) FROM dim ORDER BY id", &.{ 100, 200, -1 });

    try exec(allocator, db, "INSERT INTO dim (id, a) VALUES (2, 3), (2, 90) ON DUPLICATE KEY UPDATE a = IF(VALUES(a) > a, VALUES(a), a)");
    try expectInts(allocator, db, "SELECT a FROM dim WHERE id = 2", &.{90});
}

test "ON DUPLICATE KEY UPDATE merges into flushed rows" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openDim(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try (try db.openTable("dim", .{})).flush();

    try exec(allocator, db, "INSERT INTO dim (id, a) VALUES (2, 1), (6, 60) ON DUPLICATE KEY UPDATE a = a + VALUES(a)");
    try expectInts(allocator, db, "SELECT a FROM dim ORDER BY id", &.{ 10, 21, 60 });
    try expectInts(allocator, db, "SELECT COALESCE(b, -1) FROM dim ORDER BY id", &.{ 100, 200, -1 });
    try expectInts(allocator, db, "SELECT COUNT(*) FROM dim", &.{3});
}

test "ON DUPLICATE KEY UPDATE through a row alias" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openDim(allocator, std.testing.io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "INSERT INTO dim (id, a) VALUES (1, 5) AS new ON DUPLICATE KEY UPDATE a = dim.a + new.a");
    try expectInts(allocator, db, "SELECT a FROM dim WHERE id = 1", &.{15});
    try exec(allocator, db, "INSERT INTO dim (id, a) VALUES (1, 3) AS new (i, x) ON DUPLICATE KEY UPDATE a = a * x");
    try expectInts(allocator, db, "SELECT a FROM dim WHERE id = 1", &.{45});
}

test "ON DUPLICATE KEY UPDATE after INSERT ... SELECT names the source" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openDim(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE src (k INT NOT NULL, v INT NOT NULL) ORDER BY (k)");
    try exec(allocator, db, "INSERT INTO src (k, v) VALUES (1, 4), (7, 70)");

    try exec(allocator, db, "INSERT INTO dim (id, a) SELECT k, v FROM src ON DUPLICATE KEY UPDATE a = a + v");
    try expectInts(allocator, db, "SELECT id FROM dim ORDER BY id", &.{ 1, 2, 7 });
    try expectInts(allocator, db, "SELECT a FROM dim ORDER BY id", &.{ 14, 20, 70 });

    try exec(allocator, db, "INSERT INTO dim (id, a) VALUES (2, 2 + 3) ON DUPLICATE KEY UPDATE a = a - VALUES(a)");
    try expectInts(allocator, db, "SELECT a FROM dim WHERE id = 2", &.{15});
}

test "ON DUPLICATE KEY UPDATE binds subqueries and user variables" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openDim(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE other (w INT NOT NULL, PRIMARY KEY (w))");
    try exec(allocator, db, "INSERT INTO other (w) VALUES (7), (70)");

    try exec(allocator, db, "INSERT INTO dim (id, a) VALUES (1, 5) ON DUPLICATE KEY UPDATE a = (SELECT MAX(w) FROM other)");
    try exec(allocator, db, "SET @x = 3; INSERT INTO dim (id, a) VALUES (2, 5) ON DUPLICATE KEY UPDATE a = a + @x");
    try expectInts(allocator, db, "SELECT a FROM dim ORDER BY id", &.{ 70, 23 });

    try exec(allocator, db, "SET @y = 100; INSERT INTO dim (id, a) SELECT w, w FROM other ON DUPLICATE KEY UPDATE a = @y");
    try exec(allocator, db, "SET @y = 100; INSERT INTO dim (id, a) SELECT w, w FROM other ON DUPLICATE KEY UPDATE a = @y");
    try expectInts(allocator, db, "SELECT id FROM dim ORDER BY id", &.{ 1, 2, 7, 70 });
    try expectInts(allocator, db, "SELECT a FROM dim ORDER BY id", &.{ 70, 23, 100, 100 });
}

test "ON DUPLICATE KEY UPDATE rejects a moved key and unknown names" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openDim(allocator, std.testing.io, tmp.dir);
    defer db.close();

    try expectRunError(allocator, db, "INSERT INTO dim (id, a) VALUES (1, 1) ON DUPLICATE KEY UPDATE id = id + 1", error.UnsupportedOp);
    try expectRunError(allocator, db, "INSERT INTO dim (id, a) VALUES (1, 1) ON DUPLICATE KEY UPDATE a = nope", error.ColumnNotFound);
    try expectRunError(allocator, db, "INSERT INTO dim (id, a) VALUES (1, 1) ON DUPLICATE KEY UPDATE nope = 1", error.ColumnNotFound);
    try expectInts(allocator, db, "SELECT a FROM dim ORDER BY id", &.{ 10, 20 });
}

test "ON DUPLICATE KEY UPDATE on a key-only table is a no-op" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE k (id INT NOT NULL, PRIMARY KEY (id))");
    try exec(allocator, db, "INSERT INTO k (id) VALUES (1), (2) ON DUPLICATE KEY UPDATE id = id");
    try exec(allocator, db, "INSERT INTO k (id) VALUES (1), (3) ON DUPLICATE KEY UPDATE id = id");
    try expectInts(allocator, db, "SELECT id FROM k ORDER BY id", &.{ 1, 2, 3 });
}

test "INSERT IGNORE keeps the stored row and the first of a repeated key" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openDim(allocator, std.testing.io, tmp.dir);
    defer db.close();

    try std.testing.expectEqual(@as(u64, 1), try affectedRows(allocator, db, "INSERT IGNORE INTO dim (id, a, b) VALUES (1, 0, 0), (3, 30, 300), (3, 31, 301)"));
    try expectInts(allocator, db, "SELECT id FROM dim ORDER BY id", &.{ 1, 2, 3 });
    try expectInts(allocator, db, "SELECT a FROM dim ORDER BY id", &.{ 10, 20, 30 });

    try (try db.openTable("dim", .{})).flush();
    try exec(allocator, db, "INSERT IGNORE dim (id, a) VALUES (2, 0), (4, 40)");
    try expectInts(allocator, db, "SELECT a FROM dim ORDER BY id", &.{ 10, 20, 30, 40 });
}

test "INSERT IGNORE on a non-unique table appends" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE evt (ts INT NOT NULL, val INT NOT NULL) ORDER BY (ts)");
    try exec(allocator, db, "INSERT INTO evt (ts, val) VALUES (1, 10)");
    try exec(allocator, db, "INSERT IGNORE INTO evt (ts, val) VALUES (1, 20)");
    try exec(allocator, db, "INSERT INTO evt (ts, val) VALUES (1, 30) ON DUPLICATE KEY UPDATE val = val + 1");
    try expectInts(allocator, db, "SELECT val FROM evt ORDER BY val", &.{ 10, 20, 30 });
}

test "INSERT SET, VALUE and ROW spellings" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openDim(allocator, std.testing.io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "INSERT INTO dim SET id = 3, a = 30, b = 300");
    try exec(allocator, db, "INSERT INTO dim (id, a) VALUE (4, 40)");
    try exec(allocator, db, "INSERT INTO dim (id, a) VALUES ROW(5, 50), ROW(6, 60)");
    try exec(allocator, db, "INSERT LOW_PRIORITY INTO dim SET id = 1, a = 1 ON DUPLICATE KEY UPDATE a = a + VALUES(a)");
    try expectInts(allocator, db, "SELECT id FROM dim ORDER BY id", &.{ 1, 2, 3, 4, 5, 6 });
    try expectInts(allocator, db, "SELECT a FROM dim ORDER BY id", &.{ 11, 20, 30, 40, 50, 60 });
    try expectRunError(allocator, db, "INSERT INTO dim (id) SET id = 9", error.SqlExpectedKeyword);
}
