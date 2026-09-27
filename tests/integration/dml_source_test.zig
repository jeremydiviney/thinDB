//! MySQL's UPDATE / DELETE forms beyond a filtered single-table scan:
//! ORDER BY / LIMIT, a table alias, qualified SET targets, joins, and
//! multi-table DELETE. Each runs as a SELECT of the rows it touches, then
//! writes the target by key.

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

/// `t`: ids 1..6 with v = id * 10; `u`: a second keyed table.
fn openTables(allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir, flush: bool) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL, tag VARCHAR(8), PRIMARY KEY (id))");
    try exec(allocator, db, "INSERT INTO t (id, v, tag) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'a'), (4, 40, 'b'), (5, 50, 'a'), (6, 60, 'b')");
    try exec(allocator, db, "CREATE TABLE u (k INT NOT NULL, w INT NOT NULL, PRIMARY KEY (k))");
    try exec(allocator, db, "INSERT INTO u (k, w) VALUES (2, 200), (4, 400), (9, 900)");
    if (flush) {
        try (try db.openTable("t", .{})).flush();
        try (try db.openTable("u", .{})).flush();
    }
    return db;
}

test "UPDATE ... ORDER BY ... LIMIT rewrites only the first rows" {
    const allocator = std.testing.allocator;
    inline for (.{ false, true }) |flush| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try openTables(allocator, std.testing.io, tmp.dir, flush);
        defer db.close();

        try std.testing.expectEqual(@as(u64, 1), try affectedRows(allocator, db, "UPDATE t SET v = v + 1 ORDER BY id DESC LIMIT 1"));
        try expectInts(allocator, db, "SELECT v FROM t ORDER BY id", &.{ 10, 20, 30, 40, 50, 61 });
        try std.testing.expectEqual(@as(u64, 2), try affectedRows(allocator, db, "UPDATE t SET v = 0 WHERE tag = 'a' ORDER BY v LIMIT 2"));
        try expectInts(allocator, db, "SELECT v FROM t ORDER BY id", &.{ 0, 20, 0, 40, 50, 61 });
        try exec(allocator, db, "UPDATE t SET v = 7 WHERE id = 2 LIMIT 5");
        try expectInts(allocator, db, "SELECT v FROM t WHERE id = 2", &.{7});
        try expectInts(allocator, db, "SELECT COUNT(*) FROM t", &.{6});
    }
}

test "DELETE ... ORDER BY ... LIMIT removes only the first rows" {
    const allocator = std.testing.allocator;
    inline for (.{ false, true }) |flush| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try openTables(allocator, std.testing.io, tmp.dir, flush);
        defer db.close();

        try std.testing.expectEqual(@as(u64, 1), try affectedRows(allocator, db, "DELETE FROM t WHERE id = 4 LIMIT 1"));
        try std.testing.expectEqual(@as(u64, 2), try affectedRows(allocator, db, "DELETE FROM t WHERE tag = 'a' ORDER BY id DESC LIMIT 2"));
        try expectInts(allocator, db, "SELECT id FROM t ORDER BY id", &.{ 1, 2, 6 });
        try exec(allocator, db, "DELETE LOW_PRIORITY QUICK FROM t ORDER BY v LIMIT 1");
        try expectInts(allocator, db, "SELECT id FROM t ORDER BY id", &.{ 2, 6 });
    }
}

test "UPDATE and DELETE through an alias and qualified names" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openTables(allocator, std.testing.io, tmp.dir, false);
    defer db.close();

    try exec(allocator, db, "UPDATE t AS x SET x.v = x.v * 2 WHERE x.id <= 2");
    try exec(allocator, db, "UPDATE t SET t.tag = 'z' WHERE t.id = 3");
    try expectInts(allocator, db, "SELECT v FROM t ORDER BY id", &.{ 20, 40, 30, 40, 50, 60 });
    try expectInts(allocator, db, "SELECT id FROM t WHERE tag = 'z'", &.{3});
    try exec(allocator, db, "DELETE FROM t AS x WHERE x.v = 40");
    try expectInts(allocator, db, "SELECT id FROM t ORDER BY id", &.{ 1, 3, 5, 6 });
}

test "multi-table UPDATE writes the joined values" {
    const allocator = std.testing.allocator;
    inline for (.{ false, true }) |flush| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try openTables(allocator, std.testing.io, tmp.dir, flush);
        defer db.close();

        try std.testing.expectEqual(@as(u64, 2), try affectedRows(allocator, db, "UPDATE t JOIN u ON t.id = u.k SET t.v = u.w"));
        try expectInts(allocator, db, "SELECT v FROM t ORDER BY id", &.{ 10, 200, 30, 400, 50, 60 });

        // A self-join reads the rows as they were before the statement.
        try exec(allocator, db, "UPDATE t a JOIN t b ON a.id = b.id + 1 SET a.v = b.v");
        try expectInts(allocator, db, "SELECT v FROM t ORDER BY id", &.{ 10, 10, 200, 30, 400, 50 });

        // Unqualified targets settle on the table that holds them.
        try exec(allocator, db, "UPDATE t, u SET w = w + 1 WHERE u.k = t.id AND t.tag = 'b'");
        try expectInts(allocator, db, "SELECT w FROM u ORDER BY k", &.{ 201, 401, 900 });
        try expectInts(allocator, db, "SELECT COUNT(*) FROM t", &.{6});
    }
}

test "multi-table UPDATE assigns each table it names" {
    const allocator = std.testing.allocator;
    inline for (.{ false, true }) |flush| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try openTables(allocator, std.testing.io, tmp.dir, flush);
        defer db.close();

        try std.testing.expectEqual(@as(u64, 4), try affectedRows(allocator, db, "UPDATE t JOIN u ON t.id = u.k SET t.v = u.w, u.w = u.w + 1"));
        try expectInts(allocator, db, "SELECT v FROM t ORDER BY id", &.{ 10, 200, 30, 400, 50, 60 });
        try expectInts(allocator, db, "SELECT w FROM u ORDER BY k", &.{ 201, 401, 900 });

        // Unqualified targets each settle on the table that holds them.
        try std.testing.expectEqual(@as(u64, 2), try affectedRows(allocator, db, "UPDATE t, u SET v = 0, w = 0 WHERE t.id = u.k AND u.k = 2"));
        try expectInts(allocator, db, "SELECT v FROM t ORDER BY id", &.{ 10, 0, 30, 400, 50, 60 });
        try expectInts(allocator, db, "SELECT w FROM u ORDER BY k", &.{ 0, 401, 900 });
        try expectInts(allocator, db, "SELECT COUNT(*) FROM t", &.{6});
        try expectInts(allocator, db, "SELECT COUNT(*) FROM u", &.{3});
    }
}

test "UPDATE over an outer join leaves the NULL-extended side alone" {
    const allocator = std.testing.allocator;
    inline for (.{ false, true }) |flush| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try openTables(allocator, std.testing.io, tmp.dir, flush);
        defer db.close();

        try std.testing.expectEqual(@as(u64, 2), try affectedRows(allocator, db, "UPDATE t LEFT JOIN u ON t.id = u.k SET u.w = u.w + 1"));
        try expectInts(allocator, db, "SELECT w FROM u ORDER BY k", &.{ 201, 401, 900 });

        try std.testing.expectEqual(@as(u64, 8), try affectedRows(allocator, db, "UPDATE t LEFT JOIN u ON t.id = u.k SET t.v = COALESCE(u.w, -1), u.w = 0"));
        try expectInts(allocator, db, "SELECT v FROM t ORDER BY id", &.{ -1, 201, -1, 401, -1, -1 });
        try expectInts(allocator, db, "SELECT w FROM u ORDER BY k", &.{ 0, 0, 900 });

        try std.testing.expectEqual(@as(u64, 2), try affectedRows(allocator, db, "UPDATE t RIGHT JOIN u ON t.id = u.k SET t.v = 7"));
        try expectInts(allocator, db, "SELECT v FROM t ORDER BY id", &.{ -1, 7, -1, 7, -1, -1 });
        try std.testing.expectEqual(@as(u64, 0), try affectedRows(allocator, db, "UPDATE t LEFT JOIN u ON t.id = u.k + 100 SET u.w = 5"));
        try expectInts(allocator, db, "SELECT COUNT(*) FROM t", &.{6});
        try expectInts(allocator, db, "SELECT COUNT(*) FROM u", &.{3});
    }
}

test "multi-table DELETE removes each target's joined rows" {
    const allocator = std.testing.allocator;
    inline for (.{ false, true }) |flush| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try openTables(allocator, std.testing.io, tmp.dir, flush);
        defer db.close();

        try std.testing.expectEqual(@as(u64, 1), try affectedRows(allocator, db, "DELETE a FROM t a JOIN t b ON a.id = b.id + 5"));
        try expectInts(allocator, db, "SELECT id FROM t ORDER BY id", &.{ 1, 2, 3, 4, 5 });
        try exec(allocator, db, "DELETE t, u FROM t JOIN u ON t.id = u.k WHERE u.w > 300");
        try expectInts(allocator, db, "SELECT id FROM t ORDER BY id", &.{ 1, 2, 3, 5 });
        try expectInts(allocator, db, "SELECT k FROM u ORDER BY k", &.{ 2, 9 });
        try exec(allocator, db, "DELETE FROM u USING u LEFT JOIN t ON t.id = u.k WHERE t.id IS NULL");
        try expectInts(allocator, db, "SELECT k FROM u ORDER BY k", &.{2});
    }
}

test "UPDATE and DELETE over a SELECT reject what a key can't resolve" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openTables(allocator, std.testing.io, tmp.dir, false);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE evt (ts INT NOT NULL, val INT NOT NULL) ORDER BY (ts)");
    try exec(allocator, db, "INSERT INTO evt (ts, val) VALUES (1, 10), (1, 20)");

    try expectRunError(allocator, db, "UPDATE t SET id = id + 10 ORDER BY id LIMIT 1", error.UnsupportedOp);
    try expectRunError(allocator, db, "UPDATE evt SET val = 0 ORDER BY ts LIMIT 1", error.UnsupportedOp);
    try expectRunError(allocator, db, "DELETE FROM evt ORDER BY ts LIMIT 1", error.UnsupportedOp);
    try expectRunError(allocator, db, "UPDATE t a JOIN t b ON a.id = b.id SET v = 1", error.BadRequest);
    try expectRunError(allocator, db, "UPDATE t JOIN u ON t.id = u.k SET nope = 1", error.ColumnNotFound);
    try expectInts(allocator, db, "SELECT v FROM t ORDER BY id", &.{ 10, 20, 30, 40, 50, 60 });
    try expectInts(allocator, db, "SELECT COUNT(*) FROM evt", &.{2});
}
