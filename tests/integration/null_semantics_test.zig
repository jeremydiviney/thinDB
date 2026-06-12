//! NULL semantics battery — pins standard SQL NULL behavior across the
//! predicate evaluator, aggregates, grouping, and ordering. Grown one
//! bucket at a time as the 2026-06-12 NULL audit fixes land; every case
//! here was probed against MySQL/PG/DuckDB semantics first.
//!
//! Fixture: six rows over (id, v BIGINT NULL, s VARCHAR NULL, grp NOT NULL)
//!   (1, 10, 'a', 'x') (2, NULL, NULL, 'x') (3, 20, 'b', 'x')
//!   (4, NULL, NULL, 'y') (5, NULL, NULL, 'y') (6, 30, NULL, 'z')

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db,
        "CREATE TABLE nt (id BIGINT PRIMARY KEY, v BIGINT, s VARCHAR(16), grp VARCHAR(8) NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO nt (id, v, s, grp) VALUES " ++
            "(1, 10, 'a', 'x'), (2, NULL, NULL, 'x'), (3, 20, 'b', 'x'), " ++
            "(4, NULL, NULL, 'y'), (5, NULL, NULL, 'y'), (6, 30, NULL, 'z')",
    );
    const t = try db.openTable("nt", .{});
    try t.flush();
    return db;
}

fn collectIds(allocator: std.mem.Allocator, q: anytype) !std.ArrayList(i64) {
    var ids: std.ArrayList(i64) = .empty;
    errdefer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |x| try ids.append(allocator, x);
    }
    return ids;
}

test "null 3VL: NOT over a comparison keeps excluding NULL rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // v > 15 → {3, 6}. NOT (v > 15) → {1} ONLY: rows 2/4/5 are UNKNOWN
    // either way and must never pass.
    var q = try runSql(allocator, db, "SELECT id FROM nt WHERE NOT (v > 15) ORDER BY id");
    defer q.deinit();
    var ids = try collectIds(allocator, &q);
    defer ids.deinit(allocator);
    try std.testing.expectEqualSlices(i64, &[_]i64{1}, ids.items);
}

test "null 3VL: NOT over AND/OR (De Morgan) keeps excluding NULL rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // NOT (v < 15 OR v > 25) ≡ v >= 15 AND v <= 25 → {3}.
    var q = try runSql(allocator, db, "SELECT id FROM nt WHERE NOT (v < 15 OR v > 25) ORDER BY id");
    defer q.deinit();
    var ids = try collectIds(allocator, &q);
    defer ids.deinit(allocator);
    try std.testing.expectEqualSlices(i64, &[_]i64{3}, ids.items);
}

test "null 3VL: NOT (... = ...) excludes NULLs; double NOT round-trips" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q1 = try runSql(allocator, db, "SELECT id FROM nt WHERE NOT (v = 10) ORDER BY id");
    defer q1.deinit();
    var ids1 = try collectIds(allocator, &q1);
    defer ids1.deinit(allocator);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 6 }, ids1.items);

    var q2 = try runSql(allocator, db, "SELECT id FROM nt WHERE NOT (NOT (v = 10)) ORDER BY id");
    defer q2.deinit();
    var ids2 = try collectIds(allocator, &q2);
    defer ids2.deinit(allocator);
    try std.testing.expectEqualSlices(i64, &[_]i64{1}, ids2.items);
}

test "null 3VL: NOT IN literal list excludes NULL probe rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // v NOT IN (10, 20) → {6}: NULL v is UNKNOWN, excluded (standard).
    var q = try runSql(allocator, db, "SELECT id FROM nt WHERE v NOT IN (10, 20) ORDER BY id");
    defer q.deinit();
    var ids = try collectIds(allocator, &q);
    defer ids.deinit(allocator);
    try std.testing.expectEqualSlices(i64, &[_]i64{6}, ids.items);
}

test "null 3VL: NOT LIKE excludes NULL rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // s NOT LIKE 'a%' → {3} only — rows with NULL s are UNKNOWN.
    var q = try runSql(allocator, db, "SELECT id FROM nt WHERE s NOT LIKE 'a%' ORDER BY id");
    defer q.deinit();
    var ids = try collectIds(allocator, &q);
    defer ids.deinit(allocator);
    try std.testing.expectEqualSlices(i64, &[_]i64{3}, ids.items);
}

test "null 3VL: NOT (IS NULL) is IS NOT NULL" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT id FROM nt WHERE NOT (v IS NULL) ORDER BY id");
    defer q.deinit();
    var ids = try collectIds(allocator, &q);
    defer ids.deinit(allocator);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 3, 6 }, ids.items);
}

test "null literal: comparison against NULL is UNKNOWN — empty either polarity" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q1 = try runSql(allocator, db, "SELECT id FROM nt WHERE v = NULL");
    defer q1.deinit();
    try std.testing.expectEqual(@as(?thindb.exec.Batch, null), try q1.next());

    // NOT UNKNOWN is still UNKNOWN — the negated form is empty too.
    var q2 = try runSql(allocator, db, "SELECT id FROM nt WHERE NOT (v = NULL)");
    defer q2.deinit();
    try std.testing.expectEqual(@as(?thindb.exec.Batch, null), try q2.next());

    var q3 = try runSql(allocator, db, "SELECT id FROM nt WHERE v <> NULL");
    defer q3.deinit();
    try std.testing.expectEqual(@as(?thindb.exec.Batch, null), try q3.next());
}

test "null literal: NULLs drop from IN lists (dialect: both polarities)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // IN (10, NULL) ≡ IN (10): row 1 only (standard outcome).
    var q1 = try runSql(allocator, db, "SELECT id FROM nt WHERE v IN (10, NULL) ORDER BY id");
    defer q1.deinit();
    var ids1 = try collectIds(allocator, &q1);
    defer ids1.deinit(allocator);
    try std.testing.expectEqualSlices(i64, &[_]i64{1}, ids1.items);

    // NOT IN (10, NULL) ≡ NOT IN (10) per the drop-NULLs dialect: non-NULL
    // non-10 rows pass; NULL probe rows stay excluded (3VL probe side).
    var q2 = try runSql(allocator, db, "SELECT id FROM nt WHERE v NOT IN (10, NULL) ORDER BY id");
    defer q2.deinit();
    var ids2 = try collectIds(allocator, &q2);
    defer ids2.deinit(allocator);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 6 }, ids2.items);

    // All-NULL list: IN (NULL) matches nothing; NOT IN (NULL) is vacuously
    // true for every row (dialect: the set is empty after the drop).
    var q3 = try runSql(allocator, db, "SELECT id FROM nt WHERE v IN (NULL)");
    defer q3.deinit();
    try std.testing.expectEqual(@as(?thindb.exec.Batch, null), try q3.next());

    var q4 = try runSql(allocator, db, "SELECT COUNT(*) AS n FROM nt WHERE v NOT IN (NULL)");
    defer q4.deinit();
    const b4 = (try q4.next()).?;
    try std.testing.expectEqual(@as(i64, 6), b4.values[0].data.bigint[0]);
}

test "null basics: COUNT variants, aggregate NULL skipping, DISTINCT exclusion" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db,
        "SELECT COUNT(*) AS a, COUNT(v) AS b, COUNT(s) AS c, COUNT(DISTINCT v) AS d FROM nt",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 6), batch.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 3), batch.values[1].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 2), batch.values[2].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 3), batch.values[3].data.bigint[0]);
}

test "null basics: arithmetic propagates NULL" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT v + 1 AS w FROM nt WHERE id = 2");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expect(!batch.values[0].isValid(0));
}
