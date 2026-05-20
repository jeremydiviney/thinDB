//! Uncorrelated EXISTS / NOT EXISTS — `WHERE EXISTS (SELECT ...)` and
//! `SELECT EXISTS(...) AS has`. Pre-compile pass drains the inner once
//! and rewrites the predicate to `.always = true|false` or the
//! projection to `.lit = boolean`.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "CREATE TABLE other (k BIGINT PRIMARY KEY)");
    try exec(allocator, db, "INSERT INTO t (id, qty) VALUES (1, 10), (2, 20), (3, 30)");
    try exec(allocator, db, "INSERT INTO other (k) VALUES (100)");
    const t = try db.openTable("t", .{});
    try t.flush();
    const o = try db.openTable("other", .{});
    try o.flush();
    return db;
}

test "EXISTS: non-empty inner makes WHERE pass every row" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SELECT id FROM t WHERE EXISTS (SELECT k FROM other) ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, ids);
}

test "EXISTS: empty inner makes WHERE filter out everything" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SELECT id FROM t WHERE EXISTS (SELECT k FROM other WHERE k = 999)",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{}, ids);
}

test "NOT EXISTS: empty inner passes all rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SELECT id FROM t WHERE NOT EXISTS (SELECT k FROM other WHERE k = 999) ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, ids);
}

test "EXISTS: composes with AND" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // EXISTS is TRUE (other has rows); AND qty > 15 → ids 2, 3.
    const ids = try collectBigints(allocator, db,
        "SELECT id FROM t WHERE EXISTS (SELECT k FROM other) AND qty > 15 ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, ids);
}

test "EXISTS in projection emits boolean column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db,
        "SELECT id, EXISTS(SELECT k FROM other) AS has FROM t ORDER BY id ASC LIMIT 2",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), batch.row_count);
    // EXISTS(...) over non-empty inner → TRUE for every row.
    try std.testing.expectEqual(@as(u8, 1), batch.values[1].data.boolean[0]);
    try std.testing.expectEqual(@as(u8, 1), batch.values[1].data.boolean[1]);
}

test "EXISTS in projection — empty inner gives FALSE" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db,
        "SELECT EXISTS(SELECT k FROM other WHERE k = 999) AS has FROM t LIMIT 1",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(u8, 0), batch.values[0].data.boolean[0]);
}
