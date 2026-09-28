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

fn affectedRows(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !u64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    while (try q.next()) |_| {}
    return q.affectedRows();
}

const DeadRowsMode = enum {
    /// No segment: every row stays in the memtable.
    memtable,
    /// The load is flushed; later statements work over one segment.
    flushed,
    /// A flush after every statement spreads each row's dead and live
    /// copies over several segments.
    flush_each,
    /// A flush and a compaction after every statement: the dead copies are
    /// dropped as they appear.
    compacted,
};

/// Runs `sql`, checks its affected-row count, then moves the rows as `mode` says.
fn dmlStep(allocator: std.mem.Allocator, db: *thindb.Database, mode: DeadRowsMode, sql: []const u8, affected: ?u64) !void {
    const got = try affectedRows(allocator, db, sql);
    if (affected) |want| std.testing.expectEqual(want, got) catch |err| {
        std.debug.print("{s} ({s})\n", .{ sql, @tagName(mode) });
        return err;
    };
    const t = try db.openTable("t", .{});
    switch (mode) {
        .memtable, .flushed => {},
        .flush_each => try t.flush(),
        .compacted => {
            try t.flush();
            try t.compact();
        },
    }
}

test "UPDATE, DELETE and ON DUPLICATE KEY UPDATE match only live rows (#343)" {
    const allocator = std.testing.allocator;
    inline for (.{ true, false }) |keyed| {
        inline for (.{ DeadRowsMode.memtable, .flushed, .flush_each, .compacted }) |mode| {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
            defer db.close();
            try exec(allocator, db, if (keyed)
                "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)"
            else
                "CREATE TABLE t (id BIGINT NOT NULL, qty INT NOT NULL)");
            try exec(allocator, db, "CREATE TABLE s (k BIGINT PRIMARY KEY)");
            try exec(allocator, db, "INSERT INTO s (k) VALUES (3), (4)");
            try exec(allocator, db, "INSERT INTO t (id, qty) VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            if (mode != .memtable) try (try db.openTable("t", .{})).flush();

            // A second UPDATE builds on the first, not on the copy it replaced.
            try dmlStep(allocator, db, mode, "UPDATE t SET qty = qty + 1 WHERE id = 1", 1);
            try dmlStep(allocator, db, mode, "UPDATE t SET qty = qty + 1 WHERE id = 1", 1);
            // A deleted row stays deleted.
            try dmlStep(allocator, db, mode, "DELETE FROM t WHERE id = 2", 1);
            try dmlStep(allocator, db, mode, "UPDATE t SET qty = 0 WHERE id = 2", 0);
            try dmlStep(allocator, db, mode, "DELETE FROM t WHERE id = 2", 0);
            try dmlStep(allocator, db, mode, "UPDATE t SET qty = qty + 100 WHERE id IN (SELECT k FROM s)", 2);
            try dmlStep(allocator, db, mode, "UPDATE t SET qty = qty + 1000", 3);
            try std.testing.expectEqual(@as(usize, 3), try countOf(allocator, db, "SELECT COUNT(*) FROM t"));
            try expectRows(allocator, db, &.{ 1, 3, 4 }, &.{ 1012, 1130, 1140 });

            if (keyed) {
                // The forms that run a SELECT and write back by key, and the
                // upsert's own lookup of the stored row.
                try dmlStep(allocator, db, mode, "UPDATE t SET qty = qty + 1 ORDER BY id LIMIT 1", 1);
                try dmlStep(allocator, db, mode, "UPDATE t JOIN s ON t.id = s.k SET t.qty = t.qty + 1 WHERE s.k = 3", 1);
                try dmlStep(allocator, db, mode, "INSERT INTO t (id, qty) VALUES (2, 7) ON DUPLICATE KEY UPDATE qty = qty + 1", null);
                try dmlStep(allocator, db, mode, "INSERT INTO t (id, qty) VALUES (4, 0) ON DUPLICATE KEY UPDATE qty = qty + 1", null);
                try dmlStep(allocator, db, mode, "INSERT INTO t (id, qty) VALUES (4, 0) ON DUPLICATE KEY UPDATE qty = qty + 1", null);
                try dmlStep(allocator, db, mode, "DELETE FROM t ORDER BY id DESC LIMIT 1", 1);
                try dmlStep(allocator, db, mode, "INSERT INTO t (id, qty) VALUES (4, 5) ON DUPLICATE KEY UPDATE qty = qty + 1", null);
                try expectRows(allocator, db, &.{ 1, 2, 3, 4 }, &.{ 1013, 7, 1131, 5 });
            } else {
                try dmlStep(allocator, db, mode, "DELETE FROM t WHERE qty > 1100", 2);
                try expectRows(allocator, db, &.{1}, &.{1012});
            }
        }
    }
}

fn countOf(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !usize {
    const got = try collectBigints(allocator, db, sql);
    defer allocator.free(got);
    return @intCast(got[0]);
}

fn expectRows(allocator: std.mem.Allocator, db: anytype, ids: []const i64, qtys: []const i64) !void {
    const got_ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(got_ids);
    try std.testing.expectEqualSlices(i64, ids, got_ids);
    const got_qtys = try collectBigints(allocator, db, "SELECT CAST(qty AS BIGINT) FROM t ORDER BY id ASC");
    defer allocator.free(got_qtys);
    try std.testing.expectEqualSlices(i64, qtys, got_qtys);
}

const old_ts = "'2001-01-01 00:00:00'";
const recent = "'2020-01-01 00:00:00'";

fn expectIds(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, want: []const i64) !void {
    const got = try collectBigints(allocator, db, sql);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, want, got);
}

/// Every value of the single DATETIME column `sql` returns.
fn collectDatetimes(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) ![]i64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(i64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |batch| {
        for (batch.values[0].data.datetime[0..batch.row_count]) |v| try out.append(allocator, v);
    }
    return out.toOwnedSlice(allocator);
}

test "ON UPDATE CURRENT_TIMESTAMP stamps the rows an UPDATE changes, with one timestamp (#251)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE ou (id BIGINT PRIMARY KEY, v INT, note VARCHAR(8), " ++
        "ts DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, also DATETIME ON UPDATE NOW())");
    try exec(allocator, db, "INSERT INTO ou VALUES (1, 10, 'a', " ++ old_ts ++ ", NULL), (2, 20, 'b', " ++ old_ts ++ ", NULL), " ++
        "(3, 30, NULL, " ++ old_ts ++ ", NULL)");
    const t = try db.openTable("ou", .{});
    try t.flush();
    try exec(allocator, db, "INSERT INTO ou VALUES (4, 40, 'd', " ++ old_ts ++ ", NULL)");

    // Row 2 already holds 20, so it doesn't change and isn't stamped.
    try exec(allocator, db, "UPDATE ou SET v = 20 WHERE id <= 2");
    try expectIds(allocator, db, "SELECT id FROM ou WHERE ts > " ++ recent ++ " AND ts = also ORDER BY id", &.{1});
    try expectIds(allocator, db, "SELECT id FROM ou WHERE ts = " ++ old_ts ++ " AND also IS NULL ORDER BY id", &.{ 2, 3, 4 });

    // NULL to NULL is no change either. Segment and memtable rows take the
    // statement's one timestamp.
    try exec(allocator, db, "UPDATE ou SET note = NULL");
    try expectIds(allocator, db, "SELECT id FROM ou WHERE ts = " ++ old_ts ++ " AND also IS NULL ORDER BY id", &.{3});
    const stamps = try collectDatetimes(allocator, db, "SELECT ts FROM ou WHERE id <> 3 AND ts = also ORDER BY id");
    defer allocator.free(stamps);
    try std.testing.expectEqual(@as(usize, 3), stamps.len);
    for (stamps) |s| try std.testing.expectEqual(stamps[0], s);

    // An explicit assignment wins, even of the value the row holds.
    try exec(allocator, db, "UPDATE ou SET v = 31, ts = ts WHERE id = 3");
    try expectIds(allocator, db, "SELECT id FROM ou WHERE ts = " ++ old_ts ++ " AND also > " ++ recent, &.{3});
    try exec(allocator, db, "UPDATE ou SET also = NULL, v = 32 WHERE id = 3");
    try expectIds(allocator, db, "SELECT id FROM ou WHERE ts > " ++ recent ++ " AND also IS NULL", &.{3});
}

test "ON DUPLICATE KEY UPDATE stamps as UPDATE does, and the CDC upsert keeps its source timestamp (#251)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE cdc (id BIGINT PRIMARY KEY, m INT, n VARCHAR(8), " ++
        "updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");
    try exec(allocator, db, "INSERT INTO cdc VALUES (1, 1, 'a', " ++ old_ts ++ "), (2, 2, 'b', " ++ old_ts ++ "), (3, 3, 'c', " ++ old_ts ++ ")");
    const t = try db.openTable("cdc", .{});
    try t.flush();

    // The update branch leaves updated_at out; row 2's m doesn't change.
    try exec(allocator, db, "INSERT INTO cdc (id, m) VALUES (1, 10), (2, 2) ON DUPLICATE KEY UPDATE m = VALUES(m)");
    try expectIds(allocator, db, "SELECT id FROM cdc WHERE updated_at > " ++ recent ++ " ORDER BY id", &.{1});
    try exec(allocator, db, "INSERT INTO cdc (id, m, updated_at) VALUES (3, 30, " ++ old_ts ++ ") " ++
        "ON DUPLICATE KEY UPDATE m = VALUES(m) + 3, updated_at = VALUES(updated_at)");
    try expectIds(allocator, db, "SELECT m FROM cdc WHERE id = 3 AND updated_at = " ++ old_ts, &.{33});

    // The Flink sink's shape: every column from the new row, updated_at included.
    const rows = "(1, 100, 'x', '2005-05-05 05:05:05'), (2, 2, 'b', '2006-06-06 06:06:06'), (3, 3, 'c', " ++ old_ts ++ "), " ++
        "(5, 5, 'e', '2007-07-07 07:07:07')";
    try exec(allocator, db, "INSERT INTO cdc (id, m, n, updated_at) VALUES " ++ rows ++ " ON DUPLICATE KEY UPDATE " ++
        "id = VALUES(id), m = VALUES(m), n = VALUES(n), updated_at = VALUES(updated_at)");
    try exec(allocator, db, "CREATE TABLE plain (id BIGINT PRIMARY KEY, m INT, n VARCHAR(8), updated_at DATETIME NOT NULL)");
    try exec(allocator, db, "INSERT INTO plain VALUES " ++ rows);
    const want = try collectDatetimes(allocator, db, "SELECT updated_at FROM plain ORDER BY id");
    defer allocator.free(want);
    const got = try collectDatetimes(allocator, db, "SELECT updated_at FROM cdc ORDER BY id");
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, want, got);
}

test "UPDATE ... JOIN stamps the changed rows of each table it writes (#251)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE ju (id BIGINT PRIMARY KEY, v INT, ts DATETIME ON UPDATE CURRENT_TIMESTAMP)");
    try exec(allocator, db, "CREATE TABLE js (id BIGINT PRIMARY KEY, v INT, ts DATETIME ON UPDATE CURRENT_TIMESTAMP)");
    try exec(allocator, db, "INSERT INTO ju VALUES (1, 1, NULL), (2, 2, NULL), (3, 3, NULL)");
    try exec(allocator, db, "INSERT INTO js VALUES (1, 10, NULL), (2, 2, NULL)");

    try exec(allocator, db, "UPDATE ju JOIN js ON ju.id = js.id SET ju.v = js.v");
    try expectIds(allocator, db, "SELECT id FROM ju WHERE ts > " ++ recent ++ " ORDER BY id", &.{1});
    try expectIds(allocator, db, "SELECT id FROM js WHERE ts IS NOT NULL", &.{});

    // An explicit assignment wins, even of the value the row holds.
    try exec(allocator, db, "UPDATE ju JOIN js ON ju.id = js.id SET ju.v = js.v + 1, ju.ts = ju.ts WHERE ju.id = 2");
    try expectIds(allocator, db, "SELECT id FROM ju WHERE ts IS NULL ORDER BY id", &.{ 2, 3 });

    // Both tables written: js row 2 already holds 2.
    try exec(allocator, db, "UPDATE ju JOIN js ON ju.id = js.id SET ju.v = 0, js.v = 2 WHERE ju.id = 2");
    try expectIds(allocator, db, "SELECT id FROM ju WHERE ts > " ++ recent ++ " ORDER BY id", &.{ 1, 2 });
    try expectIds(allocator, db, "SELECT id FROM js WHERE ts IS NOT NULL", &.{});
    try exec(allocator, db, "UPDATE ju JOIN js ON ju.id = js.id SET ju.v = 0, js.v = 20 WHERE ju.id = 2");
    try expectIds(allocator, db, "SELECT id FROM js WHERE ts > " ++ recent, &.{2});
}

test "ON UPDATE timestamps replay from the WAL as they were stored (#251)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const config: thindb.Config = .{ .wal_enabled = true, .auto_flush_secs = 0 };
    var before: []i64 = undefined;
    var manifest: []u8 = undefined;
    var wal: []u8 = undefined;
    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, config);
        defer db.close();
        try exec(allocator, db, "CREATE TABLE wu (id BIGINT PRIMARY KEY, v INT, ts DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");
        try exec(allocator, db, "INSERT INTO wu (id, v) VALUES (1, 1), (2, 2)");
        const t = try db.openTable("wu", .{});
        try t.flush();
        try exec(allocator, db, "INSERT INTO wu (id, v) VALUES (3, 3)");
        try exec(allocator, db, "UPDATE wu SET v = v + 1 WHERE id >= 2");
        try exec(allocator, db, "INSERT INTO wu (id, v) VALUES (1, 10) ON DUPLICATE KEY UPDATE v = VALUES(v)");
        before = try collectDatetimes(arena.allocator(), db, "SELECT ts FROM wu ORDER BY id");
        manifest = try t.table_dir.readFileAlloc(io, "manifest", arena.allocator(), .unlimited);
        wal = try t.table_dir.readFileAlloc(io, "wal", arena.allocator(), .unlimited);
    }
    try std.testing.expectEqual(@as(usize, 3), before.len);
    // As if the process had died right after the upsert.
    try tmp.dir.writeFile(io, .{ .sub_path = "main/public/wu/manifest", .data = manifest });
    try tmp.dir.writeFile(io, .{ .sub_path = "main/public/wu/wal", .data = wal });
    var db = try thindb.Database.open(allocator, io, tmp.dir, config);
    defer db.close();
    const after = try collectDatetimes(allocator, db, "SELECT ts FROM wu ORDER BY id");
    defer allocator.free(after);
    try std.testing.expectEqualSlices(i64, before, after);
}
