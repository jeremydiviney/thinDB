//! SQL `UPDATE t SET col = expr [, ...] [WHERE ...]` — modeled as
//! atomic DELETE-old + INSERT-new under the table mutex. Assignment
//! RHS can reference the original row's columns (`x = x + 1`) and
//! can use any scalar subquery / session var (resolved by the
//! pre-compile pass).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSql = helpers.runSql;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, label VARCHAR(8) NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO t (id, qty, label) VALUES " ++
            "(1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, 40, 'd')",
    );
    const tt = try db.openTable("t", .{});
    try tt.flush();
    return db;
}

test "UPDATE: literal RHS, predicate matches subset" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "UPDATE t SET qty = 999 WHERE id = 2");

    var q = try runSql(allocator, db, "SELECT qty FROM t WHERE id = 2");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(i32, 999), batch.values[0].data.int[0]);
}

test "UPDATE: untouched rows unchanged" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "UPDATE t SET qty = 999 WHERE id = 2");

    const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4 }, ids);
}

test "UPDATE: self-ref RHS (qty = qty + 1)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "UPDATE t SET qty = qty + 1 WHERE id > 2");

    // id 3: 30→31. id 4: 40→41.
    var q = try runSql(allocator, db, "SELECT id, qty FROM t WHERE id > 2 ORDER BY id ASC");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), batch.row_count);
    try std.testing.expectEqual(@as(i32, 31), batch.values[1].data.int[0]);
    try std.testing.expectEqual(@as(i32, 41), batch.values[1].data.int[1]);
}

test "UPDATE: multiple SET assignments" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "UPDATE t SET qty = 100, label = 'X' WHERE id = 1");

    var q = try runSql(allocator, db, "SELECT qty, label FROM t WHERE id = 1");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(i32, 100), batch.values[0].data.int[0]);
    try std.testing.expectEqualStrings("X", batch.values[1].data.varchar.rowBytes(0));
}

test "UPDATE: no WHERE updates every row" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "UPDATE t SET qty = 0");

    var q = try runSql(allocator, db, "SELECT id, qty FROM t ORDER BY id ASC");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 4), batch.row_count);
    for (0..4) |i| try std.testing.expectEqual(@as(i32, 0), batch.values[1].data.int[i]);
}

test "UPDATE: WHERE using session var" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "SET @cutoff = 25; UPDATE t SET qty = -1 WHERE qty > @cutoff");

    // qty > 25 → ids 3 (30) and 4 (40) set to -1.
    var q = try runSql(allocator, db, "SELECT id, qty FROM t ORDER BY id ASC");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqualSlices(i32, &.{ 10, 20, -1, -1 }, batch.values[1].data.int[0..batch.row_count]);
}

test "UPDATE: affected_rows reported" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "UPDATE t SET qty = 0 WHERE id > 2");
    defer q.deinit();
    while (try q.next()) |_| {}
    try std.testing.expectEqual(@as(u64, 2), q.affectedRows());
}

test "UPDATE: predicate matching nothing leaves table unchanged" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "UPDATE t SET qty = 99 WHERE id > 100");

    var q = try runSql(allocator, db, "SELECT id, qty FROM t ORDER BY id ASC");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqualSlices(i32, &.{ 10, 20, 30, 40 }, batch.values[1].data.int[0..batch.row_count]);
}
