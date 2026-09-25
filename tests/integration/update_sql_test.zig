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
    try exec(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, label VARCHAR(8) NOT NULL)",
    );
    try exec(
        allocator,
        db,
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

/// Nullable columns of every storage family plus a NOT NULL sibling, with
/// rows split across flushed segments and the memtable so both UPDATE
/// phases see a `SET col = NULL` assignment.
fn setupNullable(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(
        allocator,
        db,
        "CREATE TABLE u (id BIGINT PRIMARY KEY, n BIGINT NOT NULL, amt DECIMAL(10,2), " ++
            "ts DATETIME, d DATE, s VARCHAR(8), f DOUBLE, k BIGINT, i INT)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO u VALUES " ++
            "(1, 10, 1.25, '2026-01-01 00:00:01', '2026-01-01', 'a', 1.5, 100, 7), " ++
            "(2, 20, 2.25, '2026-01-01 00:00:02', '2026-01-02', 'b', 2.5, 200, 14), " ++
            "(3, 30, 3.25, '2026-01-01 00:00:03', '2026-01-03', 'c', 3.5, 300, 21), " ++
            "(4, 40, 4.25, '2026-01-01 00:00:04', '2026-01-04', 'd', 4.5, 400, 28)",
    );
    const tt = try db.openTable("u", .{});
    try tt.flush();
    try exec(
        allocator,
        db,
        "INSERT INTO u VALUES " ++
            "(5, 50, 5.25, '2026-01-01 00:00:05', '2026-01-05', 'e', 5.5, 500, 35), " ++
            "(6, 60, 6.25, '2026-01-01 00:00:06', '2026-01-06', 'f', 6.5, 600, 42)",
    );
    return db;
}

test "UPDATE: SET col = NULL on every nullable type, segment and memtable rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupNullable(allocator, io, tmp.dir);
    defer db.close();

    try exec(
        allocator,
        db,
        "UPDATE u SET amt = NULL, ts = NULL, d = NULL, s = NULL, f = NULL, k = NULL, i = NULL WHERE id IN (2, 6)",
    );

    const nulled = try collectBigints(
        allocator,
        db,
        "SELECT id FROM u WHERE amt IS NULL AND ts IS NULL AND d IS NULL AND s IS NULL " ++
            "AND f IS NULL AND k IS NULL AND i IS NULL ORDER BY id ASC",
    );
    defer allocator.free(nulled);
    try std.testing.expectEqualSlices(i64, &.{ 2, 6 }, nulled);

    const intact = try collectBigints(
        allocator,
        db,
        "SELECT id FROM u WHERE amt IS NOT NULL AND ts IS NOT NULL AND d IS NOT NULL " ++
            "AND s IS NOT NULL AND f IS NOT NULL AND k IS NOT NULL AND i IS NOT NULL ORDER BY id ASC",
    );
    defer allocator.free(intact);
    try std.testing.expectEqualSlices(i64, &.{ 1, 3, 4, 5 }, intact);

    const untouched = try collectBigints(allocator, db, "SELECT n FROM u ORDER BY id ASC");
    defer allocator.free(untouched);
    try std.testing.expectEqualSlices(i64, &.{ 10, 20, 30, 40, 50, 60 }, untouched);
}

test "UPDATE: NULL literal mixed with a self-referencing assignment" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupNullable(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "UPDATE u SET amt = NULL, k = k + 1 WHERE id = 3 OR id = 5");

    const ks = try collectBigints(allocator, db, "SELECT k FROM u WHERE amt IS NULL ORDER BY id ASC");
    defer allocator.free(ks);
    try std.testing.expectEqualSlices(i64, &.{ 301, 501 }, ks);

    var q = try runSql(allocator, db, "SELECT COUNT(*) FROM u WHERE amt IS NULL");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 2), batch.values[0].data.bigint[0]);
}

test "UPDATE: NULL through CASE adopts the sibling branch type" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupNullable(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "UPDATE u SET k = CASE WHEN id = 1 THEN NULL ELSE k END");

    const nulled = try collectBigints(allocator, db, "SELECT id FROM u WHERE k IS NULL ORDER BY id ASC");
    defer allocator.free(nulled);
    try std.testing.expectEqualSlices(i64, &.{1}, nulled);

    const kept = try collectBigints(allocator, db, "SELECT k FROM u WHERE k IS NOT NULL ORDER BY id ASC");
    defer allocator.free(kept);
    try std.testing.expectEqualSlices(i64, &.{ 200, 300, 400, 500, 600 }, kept);
}

test "UPDATE: SET NULL on a NOT NULL column is rejected and changes nothing" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupNullable(allocator, io, tmp.dir);
    defer db.close();

    try helpers.expectRunError(allocator, db, "UPDATE u SET n = NULL WHERE id = 2", error.TypeMismatch);
    try helpers.expectRunError(allocator, db, "UPDATE u SET amt = NULL, n = NULL WHERE id = 6", error.TypeMismatch);

    const ns = try collectBigints(allocator, db, "SELECT n FROM u ORDER BY id ASC");
    defer allocator.free(ns);
    try std.testing.expectEqualSlices(i64, &.{ 10, 20, 30, 40, 50, 60 }, ns);

    var q = try runSql(allocator, db, "SELECT COUNT(*) FROM u WHERE amt IS NULL");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 0), batch.values[0].data.bigint[0]);
}

test "UPDATE: an expression that yields NULL for a NOT NULL column is rejected, not stored as a placeholder" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupNullable(allocator, io, tmp.dir);
    defer db.close();

    // Memtable row (id 6) and flushed-segment row (id 2), each through its own phase.
    try helpers.expectRunError(
        allocator,
        db,
        "UPDATE u SET n = CASE WHEN id = 6 THEN NULL ELSE n END WHERE id IN (2, 6)",
        error.TypeMismatch,
    );
    try helpers.expectRunError(
        allocator,
        db,
        "UPDATE u SET n = CASE WHEN id = 2 THEN NULL ELSE n END WHERE id = 2",
        error.TypeMismatch,
    );

    const ns = try collectBigints(allocator, db, "SELECT n FROM u ORDER BY id ASC");
    defer allocator.free(ns);
    try std.testing.expectEqualSlices(i64, &.{ 10, 20, 30, 40, 50, 60 }, ns);
}

test "UPDATE: WHERE with a computed operand — segment and memtable rows (#94)" {
    // Row 5 stays in the memtable; 1-4 are in a flushed segment.
    const cases = .{
        .{ .sql = "UPDATE t SET qty = 0 WHERE id % 2 = 0", .qtys = &[_]i64{ 10, 0, 30, 0, 50 }, .affected = 2 },
        .{ .sql = "UPDATE t SET qty = 0 WHERE id * 2 = 4", .qtys = &[_]i64{ 10, 0, 30, 40, 50 }, .affected = 1 },
        .{ .sql = "UPDATE t SET qty = qty + 1 WHERE MOD(qty, 20) = 10", .qtys = &[_]i64{ 11, 20, 31, 40, 51 }, .affected = 3 },
        .{ .sql = "UPDATE t SET qty = 0 WHERE UPPER(label) = 'E'", .qtys = &[_]i64{ 10, 20, 30, 40, 0 }, .affected = 1 },
        .{ .sql = "UPDATE t SET qty = 0 WHERE id = 3 AND qty % 2 = 0", .qtys = &[_]i64{ 10, 20, 0, 40, 50 }, .affected = 1 },
        .{ .sql = "UPDATE t SET qty = 0 WHERE id = 3 AND qty % 2 = 1", .qtys = &[_]i64{ 10, 20, 30, 40, 50 }, .affected = 0 },
    };
    inline for (cases) |c| {
        const allocator = std.testing.allocator;
        const io = std.testing.io;
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try setup(allocator, io, tmp.dir);
        defer db.close();
        try exec(allocator, db, "INSERT INTO t (id, qty, label) VALUES (5, 50, 'e')");

        var q = try runSql(allocator, db, c.sql);
        defer q.deinit();
        while (try q.next()) |_| {}
        try std.testing.expectEqual(@as(u64, c.affected), q.affectedRows());

        const qtys = try collectBigints(allocator, db, "SELECT CAST(qty AS BIGINT) FROM t ORDER BY id ASC");
        defer allocator.free(qtys);
        try std.testing.expectEqualSlices(i64, c.qtys, qtys);
    }
}
