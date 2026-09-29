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
    try exec(
        allocator,
        db,
        "CREATE TABLE nt (id BIGINT PRIMARY KEY, v BIGINT, s VARCHAR(16), grp VARCHAR(8) NOT NULL)",
    );
    try exec(
        allocator,
        db,
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

    var q = try runSql(
        allocator,
        db,
        "SELECT COUNT(*) AS a, COUNT(v) AS b, COUNT(s) AS c, COUNT(DISTINCT v) AS d FROM nt",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 6), batch.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 3), batch.values[1].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 2), batch.values[2].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 3), batch.values[3].data.bigint[0]);
}

test "null aggregates: zero qualifying rows finalize to NULL (COUNT stays 0)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Empty input: SUM/AVG/MIN/MAX are NULL, COUNTs are 0.
    var q = try runSql(
        allocator,
        db,
        "SELECT SUM(v) AS s, AVG(v) AS a, MIN(v) AS lo, MAX(v) AS hi, COUNT(v) AS cv, COUNT(*) AS cs FROM nt WHERE id > 100",
    );
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expect(!b.values[0].isValid(0));
    try std.testing.expect(!b.values[1].isValid(0));
    try std.testing.expect(!b.values[2].isValid(0));
    try std.testing.expect(!b.values[3].isValid(0));
    try std.testing.expectEqual(@as(i64, 0), b.values[4].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 0), b.values[5].data.bigint[0]);
}

test "null aggregates: an all-NULL input column finalizes to NULL" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Rows EXIST (4 and 5) but every v is NULL.
    var q = try runSql(
        allocator,
        db,
        "SELECT SUM(v) AS s, AVG(v) AS a, MIN(v) AS lo, MAX(v) AS hi, COUNT(v) AS cv, COUNT(*) AS cs FROM nt WHERE grp = 'y'",
    );
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expect(!b.values[0].isValid(0));
    try std.testing.expect(!b.values[1].isValid(0));
    try std.testing.expect(!b.values[2].isValid(0));
    try std.testing.expect(!b.values[3].isValid(0));
    try std.testing.expectEqual(@as(i64, 0), b.values[4].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 2), b.values[5].data.bigint[0]);
}

test "null aggregates: grouped all-NULL group emits NULL aggregates" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // grp y = two rows, both v NULL → SUM/AVG/MIN/MAX NULL, COUNT(v) 0,
    // COUNT(*) 2. grp x mixes (10, NULL, 20) → SUM 30, AVG 15 (non-NULL
    // denominator), COUNT(v) 2, COUNT(*) 3.
    var q = try runSql(
        allocator,
        db,
        "SELECT grp, SUM(v) AS s, AVG(v) AS a, MIN(v) AS lo, MAX(v) AS hi, COUNT(v) AS cv, COUNT(*) AS cs " ++
            "FROM nt GROUP BY grp ORDER BY grp",
    );
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), b.row_count);
    // row 0 = x
    try std.testing.expectEqualStrings("x", b.values[0].data.varchar.rowBytes(0));
    try std.testing.expect(b.values[1].isValid(0));
    try std.testing.expectEqual(@as(f64, 15.0), b.values[2].data.double[0]);
    try std.testing.expectEqual(@as(i64, 2), b.values[5].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 3), b.values[6].data.bigint[0]);
    // row 1 = y: all aggregates NULL, counts 0 / 2
    try std.testing.expectEqualStrings("y", b.values[0].data.varchar.rowBytes(1));
    try std.testing.expect(!b.values[1].isValid(1));
    try std.testing.expect(!b.values[2].isValid(1));
    try std.testing.expect(!b.values[3].isValid(1));
    try std.testing.expect(!b.values[4].isValid(1));
    try std.testing.expectEqual(@as(i64, 0), b.values[5].data.bigint[1]);
    try std.testing.expectEqual(@as(i64, 2), b.values[6].data.bigint[1]);
}

test "null aggregates: metadata lane (bare global, flushed, no WHERE) emits NULL for all-NULL column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // A flushed table whose nullable column is entirely NULL, queried with no
    // WHERE / GROUP BY — the stats-only MetaAggStats lane answers this from
    // segment footers and must surface NULL, not the 0 fold identity.
    try exec(allocator, db, "CREATE TABLE allnull (id BIGINT PRIMARY KEY, v BIGINT)");
    try exec(allocator, db, "INSERT INTO allnull (id, v) VALUES (1, NULL), (2, NULL), (3, NULL)");
    const t = try db.openTable("allnull", .{});
    try t.flush();

    var q = try runSql(
        allocator,
        db,
        "SELECT SUM(v) AS s, AVG(v) AS a, MIN(v) AS lo, MAX(v) AS hi, COUNT(v) AS cv, COUNT(*) AS cs FROM allnull",
    );
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expect(!b.values[0].isValid(0));
    try std.testing.expect(!b.values[1].isValid(0));
    try std.testing.expect(!b.values[2].isValid(0));
    try std.testing.expect(!b.values[3].isValid(0));
    try std.testing.expectEqual(@as(i64, 0), b.values[4].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 3), b.values[5].data.bigint[0]);
}

test "null group keys: int-key NULLs form one group, emitted as NULL" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // v = 10, NULL, 20, NULL, NULL, 30 → groups NULL(3), 10, 20, 30.
    // NULLs sort first ascending (MySQL convention).
    var q = try runSql(allocator, db, "SELECT v, COUNT(*) AS c FROM nt GROUP BY v ORDER BY v");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 4), b.row_count);
    try std.testing.expect(!b.values[0].isValid(0));
    try std.testing.expectEqual(@as(i64, 3), b.values[1].data.bigint[0]);
    inline for (.{ .{ 1, 10 }, .{ 2, 20 }, .{ 3, 30 } }) |row| {
        try std.testing.expect(b.values[0].isValid(row[0]));
        try std.testing.expectEqual(@as(i64, row[1]), b.values[0].data.bigint[row[0]]);
        try std.testing.expectEqual(@as(i64, 1), b.values[1].data.bigint[row[0]]);
    }
}

test "null group keys: string-key NULL group stays distinct from every value (incl. '')" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // s = 'a', NULL, 'b', NULL, NULL, NULL → groups NULL(4), a(1), b(1).
    var q = try runSql(allocator, db, "SELECT s, COUNT(*) AS c FROM nt GROUP BY s ORDER BY s");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), b.row_count);
    try std.testing.expect(!b.values[0].isValid(0));
    try std.testing.expectEqual(@as(i64, 4), b.values[1].data.bigint[0]);
    try std.testing.expect(b.values[0].isValid(1));
    try std.testing.expectEqualStrings("a", b.values[0].data.varchar.rowBytes(1));
    try std.testing.expectEqual(@as(i64, 1), b.values[1].data.bigint[1]);
    try std.testing.expect(b.values[0].isValid(2));
    try std.testing.expectEqualStrings("b", b.values[0].data.varchar.rowBytes(2));
    try std.testing.expectEqual(@as(i64, 1), b.values[1].data.bigint[2]);
}

test "null group keys: multi-key combos with per-column NULLs" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // (v, s): (10,'a'), (NULL,NULL)×3, (20,'b'), (30,NULL)
    // → groups (NULL,NULL,3), (10,a,1), (20,b,1), (30,NULL,1).
    var q = try runSql(allocator, db, "SELECT v, s, COUNT(*) AS c FROM nt GROUP BY v, s ORDER BY v, s");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 4), b.row_count);
    try std.testing.expect(!b.values[0].isValid(0));
    try std.testing.expect(!b.values[1].isValid(0));
    try std.testing.expectEqual(@as(i64, 3), b.values[2].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 10), b.values[0].data.bigint[1]);
    try std.testing.expectEqualStrings("a", b.values[1].data.varchar.rowBytes(1));
    try std.testing.expectEqual(@as(i64, 20), b.values[0].data.bigint[2]);
    try std.testing.expectEqualStrings("b", b.values[1].data.varchar.rowBytes(2));
    try std.testing.expectEqual(@as(i64, 30), b.values[0].data.bigint[3]);
    try std.testing.expect(!b.values[1].isValid(3));
    try std.testing.expectEqual(@as(i64, 1), b.values[2].data.bigint[3]);
}

test "null group keys: memtable-only rows (unflushed) group correctly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // Same fixture, never flushed — the V1 legacy hash path segfaulted on
    // exactly this shape (memtable string-key GROUP BY with NULL rows).
    try exec(
        allocator,
        db,
        "CREATE TABLE mt (id BIGINT PRIMARY KEY, v BIGINT, s VARCHAR(16))",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO mt (id, v, s) VALUES (1, 10, 'a'), (2, NULL, NULL), (3, 20, 'b'), (4, NULL, NULL)",
    );

    var q = try runSql(allocator, db, "SELECT v, COUNT(*) AS c FROM mt GROUP BY v ORDER BY v");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), b.row_count);
    try std.testing.expect(!b.values[0].isValid(0));
    try std.testing.expectEqual(@as(i64, 2), b.values[1].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 10), b.values[0].data.bigint[1]);
    try std.testing.expectEqual(@as(i64, 20), b.values[0].data.bigint[2]);

    var qs = try runSql(allocator, db, "SELECT s, COUNT(*) AS c FROM mt GROUP BY s ORDER BY s");
    defer qs.deinit();
    const bs = (try qs.next()).?;
    try std.testing.expectEqual(@as(usize, 3), bs.row_count);
    try std.testing.expect(!bs.values[0].isValid(0));
    try std.testing.expectEqual(@as(i64, 2), bs.values[1].data.bigint[0]);
    try std.testing.expectEqualStrings("a", bs.values[0].data.varchar.rowBytes(1));
    try std.testing.expectEqualStrings("b", bs.values[0].data.varchar.rowBytes(2));
}

test "null group keys: NULL-keyed group aggregates its values normally" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // GROUP BY s: the NULL group holds ids {2,4,5,6} with v = NULL,NULL,NULL,30
    // → SUM(v)=30, COUNT(v)=1, COUNT(*)=4.
    var q = try runSql(
        allocator,
        db,
        "SELECT s, SUM(v) AS sv, COUNT(v) AS cv, COUNT(*) AS cs FROM nt GROUP BY s ORDER BY s",
    );
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), b.row_count);
    try std.testing.expect(!b.values[0].isValid(0));
    // SUM(BIGINT) is BIGINT (DESIGN.md §3.4).
    try std.testing.expectEqual(@as(i64, 30), b.values[1].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 1), b.values[2].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 4), b.values[3].data.bigint[0]);
}

test "null ordering: NULLs first ascending, last descending, through Sort and TopN" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT id FROM nt ORDER BY v DESC, id");
    defer q.deinit();
    var ids = try collectIds(allocator, &q);
    defer ids.deinit(allocator);
    // 30, 20, 10, then the NULL rows (2,4,5) last, id-tiebroken.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 6, 3, 1, 2, 4, 5 }, ids.items);

    // LIMIT routes through TopN — NULLs are the smallest ascending keys.
    var qt = try runSql(allocator, db, "SELECT id FROM nt ORDER BY v, id LIMIT 4");
    defer qt.deinit();
    var tids = try collectIds(allocator, &qt);
    defer tids.deinit(allocator);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 4, 5, 1 }, tids.items);
}

test "null window: PARTITION BY nullable key — NULLs form one partition" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // v partitions: {1}(v=10), {2,4,5}(NULL), {3}(20), {6}(30).
    var q = try runSql(
        allocator,
        db,
        "SELECT id, COUNT(*) OVER (PARTITION BY v) AS c, SUM(id) OVER (PARTITION BY v) AS si FROM nt ORDER BY id",
    );
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 6), b.row_count);
    const expect_c = [_]i64{ 1, 3, 1, 3, 3, 1 };
    const expect_si = [_]i64{ 1, 11, 3, 11, 11, 6 };
    for (expect_c, expect_si, 0..) |ec, es, row| {
        try std.testing.expectEqual(ec, b.values[1].data.bigint[row]);
        try std.testing.expectEqual(es, b.values[2].data.bigint[row]);
    }
}

test "null distinct: SELECT DISTINCT collapses NULLs into one row" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // v = 10, NULL, 20, NULL, NULL, 30 → DISTINCT yields NULL, 10, 20, 30.
    var q = try runSql(allocator, db, "SELECT DISTINCT v FROM nt ORDER BY v");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 4), b.row_count);
    try std.testing.expect(!b.values[0].isValid(0));
    try std.testing.expectEqual(@as(i64, 10), b.values[0].data.bigint[1]);
    try std.testing.expectEqual(@as(i64, 20), b.values[0].data.bigint[2]);
    try std.testing.expectEqual(@as(i64, 30), b.values[0].data.bigint[3]);

    // s = 'a', NULL, 'b', NULL, NULL, NULL → DISTINCT yields NULL, a, b.
    var qs = try runSql(allocator, db, "SELECT DISTINCT s FROM nt ORDER BY s");
    defer qs.deinit();
    const bs = (try qs.next()).?;
    try std.testing.expectEqual(@as(usize, 3), bs.row_count);
    try std.testing.expect(!bs.values[0].isValid(0));
    try std.testing.expectEqualStrings("a", bs.values[0].data.varchar.rowBytes(1));
    try std.testing.expectEqualStrings("b", bs.values[0].data.varchar.rowBytes(2));
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

test "null agg inputs: all-NULL group emits NULL for SUM/AVG/MIN/MAX, 0 for counts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // grp 'a': v = 10, NULL, 30 / s = 'm', NULL, ''   (mixed)
    // grp 'b': v = NULL, NULL  / s = NULL, NULL       (all-NULL)
    try exec(
        allocator,
        db,
        "CREATE TABLE na (id BIGINT PRIMARY KEY, grp VARCHAR(8) NOT NULL, v BIGINT, s VARCHAR(16))",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO na (id, grp, v, s) VALUES " ++
            "(1, 'a', 10, 'm'), (2, 'a', NULL, NULL), (3, 'a', 30, ''), " ++
            "(4, 'b', NULL, NULL), (5, 'b', NULL, NULL)",
    );
    const t = try db.openTable("na", .{});
    try t.flush();

    var q = try runSql(
        allocator,
        db,
        "SELECT grp, SUM(v) AS sv, AVG(v) AS av, MIN(v) AS lo, MAX(v) AS hi, " ++
            "COUNT(v) AS cv, COUNT(*) AS cs, COUNT(DISTINCT v) AS dv, MIN(s) AS ms " ++
            "FROM na GROUP BY grp ORDER BY grp",
    );
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), b.row_count);

    // grp 'a': NULLs skipped — SUM 40, AVG 20 (÷2 non-null, not ÷3),
    // MIN 10, MAX 30, COUNT(v) 2, COUNT(*) 3, MIN(s) = '' (valid, not NULL).
    try std.testing.expectEqualStrings("a", b.values[0].data.varchar.rowBytes(0));
    try std.testing.expect(b.values[1].isValid(0));
    try std.testing.expectEqual(@as(i64, 40), b.values[1].data.bigint[0]);
    try std.testing.expectApproxEqAbs(@as(f64, 20.0), b.values[2].data.double[0], 1e-9);
    try std.testing.expectEqual(@as(i64, 10), b.values[3].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 30), b.values[4].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 2), b.values[5].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 3), b.values[6].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 2), b.values[7].data.bigint[0]);
    try std.testing.expect(b.values[8].isValid(0));
    try std.testing.expectEqualStrings("", b.values[8].data.varchar.rowBytes(0));

    // grp 'b': all inputs NULL — SUM/AVG/MIN/MAX/MIN(s) are NULL; the COUNT
    // family is 0 (and COUNT(*) still 2).
    try std.testing.expectEqualStrings("b", b.values[0].data.varchar.rowBytes(1));
    for ([_]usize{ 1, 2, 3, 4, 8 }) |ci| try std.testing.expect(!b.values[ci].isValid(1));
    try std.testing.expectEqual(@as(i64, 0), b.values[5].data.bigint[1]);
    try std.testing.expectEqual(@as(i64, 2), b.values[6].data.bigint[1]);
    try std.testing.expectEqual(@as(i64, 0), b.values[7].data.bigint[1]);
}

test "grouped variance/stddev: welford per group, NULL skips, 1-row samp is NULL" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // grp 'a': x = 1..5         → var_pop 2.0, var_samp 2.5
    // grp 'b': x = 42           → var_pop 0, var_samp NULL (n < 2)
    // grp 'c': x = NULL, 10, 20 → NULL skipped: var_pop 25, var_samp 50
    try exec(
        allocator,
        db,
        "CREATE TABLE wv (id BIGINT PRIMARY KEY, grp VARCHAR(8) NOT NULL, x DOUBLE)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO wv (id, grp, x) VALUES " ++
            "(1,'a',1), (2,'a',2), (3,'a',3), (4,'a',4), (5,'a',5), " ++
            "(6,'b',42), (7,'c',NULL), (8,'c',10), (9,'c',20)",
    );
    const t = try db.openTable("wv", .{});
    try t.flush();

    var q = try runSql(
        allocator,
        db,
        "SELECT grp, VAR_POP(x) AS vp, VAR_SAMP(x) AS vs, STDDEV_POP(x) AS sp, STDDEV_SAMP(x) AS ss " ++
            "FROM wv GROUP BY grp ORDER BY grp",
    );
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), b.row_count);

    try std.testing.expectEqualStrings("a", b.values[0].data.varchar.rowBytes(0));
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), b.values[1].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 2.5), b.values[2].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@sqrt(@as(f64, 2.0)), b.values[3].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@sqrt(@as(f64, 2.5)), b.values[4].data.double[0], 1e-9);

    try std.testing.expectEqualStrings("b", b.values[0].data.varchar.rowBytes(1));
    try std.testing.expect(b.values[1].isValid(1));
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), b.values[1].data.double[1], 1e-9);
    try std.testing.expect(!b.values[2].isValid(1));
    try std.testing.expect(!b.values[4].isValid(1));

    try std.testing.expectEqualStrings("c", b.values[0].data.varchar.rowBytes(2));
    try std.testing.expectApproxEqAbs(@as(f64, 25.0), b.values[1].data.double[2], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 50.0), b.values[2].data.double[2], 1e-9);
}

test "group_concat: skips NULLs, all-NULL group emits NULL, custom separator, row order" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(
        allocator,
        db,
        "CREATE TABLE gc (id BIGINT PRIMARY KEY, grp VARCHAR(8) NOT NULL, s VARCHAR(16))",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO gc (id, grp, s) VALUES " ++
            "(1,'a','x'), (2,'a',NULL), (3,'a','y'), (4,'a',''), " ++
            "(5,'b',NULL), (6,'b',NULL)",
    );
    const t = try db.openTable("gc", .{});
    try t.flush();

    var q = try runSql(
        allocator,
        db,
        "SELECT grp, GROUP_CONCAT(s, '|') AS cs, COUNT(*) AS c FROM gc GROUP BY grp ORDER BY grp",
    );
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), b.row_count);
    // grp 'a': NULL skipped, '' kept, physical row order → "x|y|"
    try std.testing.expectEqualStrings("a", b.values[0].data.varchar.rowBytes(0));
    try std.testing.expect(b.values[1].isValid(0));
    try std.testing.expectEqualStrings("x|y|", b.values[1].data.string.rowBytes(0));
    try std.testing.expectEqual(@as(i64, 4), b.values[2].data.bigint[0]);
    // grp 'b': all inputs NULL → NULL
    try std.testing.expectEqualStrings("b", b.values[0].data.varchar.rowBytes(1));
    try std.testing.expect(!b.values[1].isValid(1));
    try std.testing.expectEqual(@as(i64, 2), b.values[2].data.bigint[1]);
}

test "null literal arguments take a sibling argument's type" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE nd (id BIGINT PRIMARY KEY, amt DOUBLE, d DATE)");
    try exec(allocator, db, "INSERT INTO nd (id, amt, d) VALUES (1, 1.5, '2024-01-01'), (2, NULL, NULL), (3, 0, '2024-02-01')");

    const cases = .{
        .{ "SELECT COUNT(COALESCE(NULL, amt)) FROM nd", 2 },
        .{ "SELECT COUNT(COALESCE(amt, NULL)) FROM nd", 2 },
        .{ "SELECT COUNT(COALESCE(NULL, NULL, id)) FROM nd", 3 },
        .{ "SELECT COUNT(IFNULL(NULL, amt)) FROM nd", 2 },
        .{ "SELECT COUNT(COALESCE(NULL, d)) FROM nd", 2 },
        .{ "SELECT COUNT(amt + NULL) FROM nd", 0 },
        .{ "SELECT COUNT(NULL - id) FROM nd", 0 },
        .{ "SELECT COUNT(GREATEST(amt, NULL)) FROM nd", 0 },
        .{ "SELECT COUNT(*) FROM nd WHERE COALESCE(amt, NULL) > 1", 1 },
        .{ "SELECT COUNT(NULLIF(amt, 0)) FROM nd", 1 },
        .{ "SELECT COUNT(NULLIF(amt, 1.5)) FROM nd", 1 },
        .{ "SELECT COUNT(1 / NULLIF(amt, 0)) FROM nd", 1 },
        .{ "SELECT COUNT(NULLIF(d, '2024-01-01')) FROM nd", 1 },
        .{ "SELECT COUNT(NULLIF(amt > 1, true)) FROM nd", 1 },
    };
    inline for (cases) |c| {
        const got = try helpers.collectBigints(allocator, db, c[0]);
        defer allocator.free(got);
        std.testing.expectEqualSlices(i64, &.{c[1]}, got) catch |err| {
            std.debug.print("query: {s}\n", .{c[0]});
            return err;
        };
    }

    var q = try runSql(allocator, db, "SELECT COALESCE(NULL, amt) AS x, NULL + 1 AS y FROM nd");
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqual(thindb.types.TypeTag.double, std.meta.activeTag(schema[0].type));
    try std.testing.expectEqual(thindb.types.TypeTag.bigint, std.meta.activeTag(schema[1].type));
}

test "null-safe equality, IS [NOT] TRUE / FALSE / UNKNOWN and XOR under NULLs" {
    // Probed against MySQL 8.4. <=>, IS DISTINCT FROM and the IS tests are
    // never UNKNOWN, so NOT keeps the rows a NULL decided; XOR is UNKNOWN
    // when either side is.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE ns (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, f BOOLEAN)");
    try exec(allocator, db, "INSERT INTO ns VALUES (1, 1, 1, true), (2, 1, 2, false), (3, NULL, 1, NULL), (4, NULL, NULL, true), (5, 0, NULL, false)");

    const cases = .{
        .{ "a <=> b", &[_]i64{ 1, 4 } },
        .{ "NOT (a <=> b)", &[_]i64{ 2, 3, 5 } },
        .{ "a IS NOT DISTINCT FROM b", &[_]i64{ 1, 4 } },
        .{ "a IS DISTINCT FROM b", &[_]i64{ 2, 3, 5 } },
        .{ "a <=> 1", &[_]i64{ 1, 2 } },
        .{ "NOT (a <=> 1)", &[_]i64{ 3, 4, 5 } },
        .{ "a <=> NULL", &[_]i64{ 3, 4 } },
        .{ "NULL <=> b", &[_]i64{ 4, 5 } },
        .{ "a + 0 <=> b * 1", &[_]i64{ 1, 4 } },
        .{ "(a, b) <=> (NULL, 1)", &[_]i64{3} },
        .{ "a IS TRUE", &[_]i64{ 1, 2 } },
        .{ "a IS NOT TRUE", &[_]i64{ 3, 4, 5 } },
        .{ "NOT (a IS TRUE)", &[_]i64{ 3, 4, 5 } },
        .{ "a IS FALSE", &[_]i64{5} },
        .{ "a IS NOT FALSE", &[_]i64{ 1, 2, 3, 4 } },
        .{ "a IS UNKNOWN", &[_]i64{ 3, 4 } },
        .{ "f IS FALSE", &[_]i64{ 2, 5 } },
        .{ "(a > 0) IS NOT TRUE", &[_]i64{ 3, 4, 5 } },
        .{ "(b > a) IS UNKNOWN", &[_]i64{ 3, 4, 5 } },
        .{ "NULL IS UNKNOWN AND id < 3", &[_]i64{ 1, 2 } },
        .{ "1 IS TRUE AND 0 IS NOT TRUE AND id = 1", &[_]i64{1} },
        .{ "a XOR b > 1", &[_]i64{1} },
        .{ "NOT (a XOR b > 1)", &[_]i64{2} },
        .{ "NULL XOR a", &[_]i64{} },
    };
    inline for (cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        const got = try helpers.collectBigints(allocator, db, "SELECT id FROM ns WHERE " ++ c[0] ++ " ORDER BY id");
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, c[1], got);
    }

    const value_cases = .{
        .{ "CAST(a <=> b AS BIGINT)", &[_]i64{ 1, 0, 0, 1, 0 } },
        .{ "CAST(a IS NOT TRUE AS BIGINT)", &[_]i64{ 0, 0, 1, 1, 1 } },
        .{ "CAST((b > a) IS UNKNOWN AS BIGINT)", &[_]i64{ 0, 0, 1, 1, 1 } },
    };
    inline for (value_cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        const got = try helpers.collectBigints(allocator, db, "SELECT " ++ c[0] ++ " FROM ns ORDER BY id");
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, c[1], got);
    }
}

test "null-safe equality joins NULL keys, inner and outer" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE ns (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, s VARCHAR(8), d DOUBLE, t VARCHAR(8))");
    try exec(allocator, db, "INSERT INTO ns VALUES (1, 1, 1, 'p', 1.5, '1'), (2, 1, 2, NULL, NULL, 'x'), (3, NULL, 1, 'p', 1.5, NULL), " ++
        "(4, NULL, NULL, NULL, NULL, '2'), (5, 0, NULL, 'q', 2.5, NULL)");
    try exec(allocator, db, "CREATE TABLE nn (id BIGINT PRIMARY KEY, k BIGINT)");
    try exec(allocator, db, "INSERT INTO nn VALUES (1, NULL), (2, NULL)");

    // Expected values from DuckDB (IS NOT DISTINCT FROM); the text key
    // against a BIGINT key from MySQL 8.4, which DuckDB won't compare.
    const cases = .{
        .{ "SELECT x.id * 10 + y.id FROM ns x JOIN ns y ON x.a <=> y.b ORDER BY 1", &[_]i64{ 11, 13, 21, 23, 34, 35, 44, 45 } },
        .{ "SELECT x.id * 10 + COALESCE(y.id, 0) FROM ns x LEFT JOIN ns y ON x.a <=> y.b AND y.id > 2 ORDER BY 1", &[_]i64{ 13, 23, 34, 35, 44, 45, 50 } },
        .{ "SELECT COALESCE(x.id, 0) * 10 + y.id FROM ns x RIGHT JOIN ns y ON x.a <=> y.b ORDER BY 1", &[_]i64{ 2, 11, 13, 21, 23, 34, 35, 44, 45 } },
        .{ "SELECT COALESCE(x.id, 0) * 10 + COALESCE(y.id, 0) FROM ns x FULL JOIN ns y ON x.a <=> y.b AND x.id < 4 ORDER BY 1", &[_]i64{ 2, 11, 13, 21, 23, 34, 35, 40, 50 } },
        .{ "SELECT x.id * 10 + y.id FROM ns x JOIN ns y ON x.a <=> y.b AND x.s = y.s ORDER BY 1", &[_]i64{ 11, 13 } },
        .{ "SELECT x.id * 10 + y.id FROM ns x JOIN ns y ON x.a <=> y.b AND x.s <=> y.s ORDER BY 1", &[_]i64{ 11, 13, 44 } },
        .{ "SELECT x.id * 10 + y.id FROM ns x JOIN ns y ON x.s <=> y.s ORDER BY 1", &[_]i64{ 11, 13, 22, 24, 31, 33, 42, 44, 55 } },
        .{ "SELECT x.id * 10 + COALESCE(y.id, 0) FROM ns x LEFT JOIN ns y ON x.d <=> y.d AND y.id <> 3 ORDER BY 1", &[_]i64{ 11, 22, 24, 31, 42, 44, 55 } },
        .{ "SELECT x.id * 10 + y.id FROM ns x JOIN ns y ON x.t <=> y.b ORDER BY 1", &[_]i64{ 11, 13, 34, 35, 42, 54, 55 } },
        .{ "SELECT x.id * 10 + COALESCE(y.id, 0) FROM ns x LEFT JOIN ns y ON x.t <=> y.b ORDER BY 1", &[_]i64{ 11, 13, 20, 34, 35, 42, 54, 55 } },
        .{ "SELECT x.id * 10 + y.id FROM ns x JOIN ns y ON CAST(x.a AS CHAR) <=> y.b ORDER BY 1", &[_]i64{ 11, 13, 21, 23, 34, 35, 44, 45 } },
        .{ "SELECT x.id * 10 + COALESCE(y.id, 0) FROM ns x LEFT JOIN ns y ON x.a <=> y.b AND (x.id < 3 OR y.id < 3) ORDER BY 1", &[_]i64{ 11, 13, 21, 23, 30, 40, 50 } },
        .{ "SELECT x.id * 10 + COALESCE(y.id, 0) FROM ns x LEFT JOIN ns y ON x.a <=> y.b AND x.id > 2 ORDER BY 1", &[_]i64{ 10, 20, 34, 35, 44, 45, 50 } },
        .{ "SELECT x.id * 10 + COALESCE(y.id, 0) FROM ns x LEFT JOIN ns y ON x.a <=> y.b AND x.a IS NOT NULL ORDER BY 1", &[_]i64{ 11, 13, 21, 23, 30, 40, 50 } },
        .{ "SELECT x.id * 10 + COALESCE(z.id, 0) FROM ns x LEFT JOIN nn z ON x.a <=> z.k ORDER BY 1", &[_]i64{ 10, 20, 31, 32, 41, 42, 50 } },
        .{ "SELECT COALESCE(x.id, 0) * 10 + z.id FROM ns x RIGHT JOIN nn z ON x.b <=> z.k ORDER BY 1", &[_]i64{ 41, 42, 51, 52 } },
        .{ "SELECT COALESCE(x.id, 0) * 10 + COALESCE(y.id, 0) FROM ns x FULL JOIN ns y ON x.b <=> y.a AND x.s = y.s ORDER BY 1", &[_]i64{ 2, 3, 4, 5, 11, 20, 31, 40, 50 } },
    };
    inline for (cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        const got = try helpers.collectBigints(allocator, db, c[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, c[1], got);
        // Parsed and compiled as a MySQL connection does.
        var session = try helpers.runSqlMysqlCtx(allocator, db, c[0]);
        defer session.deinit();
        const got_session = try helpers.collectIntCells(allocator, &session);
        defer allocator.free(got_session);
        try std.testing.expectEqual(c[1].len, got_session.len);
        for (c[1], got_session) |want, cell| try std.testing.expectEqual(@as(?i64, want), cell);
    }

    var pg = try helpers.runSqlDialect(allocator, db, "SELECT x.id * 10 + y.id FROM ns x JOIN ns y ON x.a IS NOT DISTINCT FROM y.b ORDER BY 1", .postgres);
    defer pg.deinit();
    const pg_got = try helpers.collectIntCells(allocator, &pg);
    defer allocator.free(pg_got);
    const pg_want = [_]?i64{ 11, 13, 21, 23, 34, 35, 44, 45 };
    try std.testing.expectEqualSlices(?i64, &pg_want, pg_got);

    var plan = try helpers.runSql(allocator, db, "EXPLAIN SELECT x.id FROM ns x JOIN ns y ON x.a <=> y.b");
    defer plan.deinit();
    var keyed = false;
    while (try plan.next()) |b| {
        for (0..b.row_count) |i| {
            if (std.mem.indexOf(u8, b.values[0].data.string.rowBytes(i), "HashJoin on=[x.a<=>") != null) keyed = true;
        }
    }
    try std.testing.expect(keyed);
}

test "null literal arguments with no typed sibling take an overload's parameter type" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE nf (id BIGINT PRIMARY KEY, d DATE)");
    try exec(allocator, db, "INSERT INTO nf (id, d) VALUES (1, '2024-01-01'), (2, NULL)");

    const cases = .{
        .{ "SELECT COUNT(ABS(NULL)) FROM nf", 0 },
        .{ "SELECT COUNT(ROUND(NULL)) FROM nf", 0 },
        .{ "SELECT COUNT(ROUND(NULL, 2)) FROM nf", 0 },
        .{ "SELECT COUNT(DAY(NULL)) FROM nf", 0 },
        .{ "SELECT COUNT(YEAR(NULL)) FROM nf", 0 },
        .{ "SELECT COUNT(DATE_FORMAT(NULL, '%Y')) FROM nf", 0 },
        .{ "SELECT COUNT(DATE_FORMAT(d, NULL)) FROM nf", 0 },
        .{ "SELECT COUNT(*) FROM nf WHERE YEAR(NULL) = 2024", 0 },
        .{ "SELECT COUNT(*) FROM nf WHERE ABS(NULL) IS NULL", 2 },
        .{ "SELECT COUNT(COALESCE(ABS(NULL), id)) FROM nf", 2 },
    };
    inline for (cases) |c| {
        const got = try helpers.collectBigints(allocator, db, c[0]);
        defer allocator.free(got);
        std.testing.expectEqualSlices(i64, &.{c[1]}, got) catch |err| {
            std.debug.print("query: {s}\n", .{c[0]});
            return err;
        };
    }
}

/// The first column of `sql`, run in `dialect`, as the MySQL wire prints it.
fn expectTexts(allocator: std.mem.Allocator, db: *thindb.Database, dialect: thindb.types.Dialect, sql: []const u8, expected: []const ?[]const u8) !void {
    var q = try helpers.runSqlDialect(allocator, db, sql, dialect);
    defer q.deinit();
    const got = try helpers.columnText(allocator, &q);
    defer helpers.freeStrings(allocator, got);
    errdefer std.debug.print("query: {s}\n", .{sql});
    try std.testing.expectEqual(expected.len, got.len);
    for (expected, got) |want, have| {
        if (want) |text| {
            try std.testing.expect(have != null);
            try std.testing.expectEqualStrings(text, have.?);
        } else try std.testing.expect(have == null);
    }
}

test "a value where a condition stands reads as CAST(x AS BOOLEAN) (issue #391)" {
    // Expected values probed against StarRocks: a condition reads its
    // operand as CAST(x AS BOOLEAN), so text is `true`, `false` or an INT,
    // and other text is UNKNOWN. MySQL instead reads the number text starts
    // with (`'abc' OR 0` is 0 there); the StarRocks dialect is the one the
    // workload speaks. XOR and IS TRUE, which StarRocks lacks, read the same
    // truth, and IS TRUE / IS FALSE are never UNKNOWN.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE bc (id BIGINT PRIMARY KEY, s VARCHAR(16), n DOUBLE)");
    try exec(
        allocator,
        db,
        "INSERT INTO bc VALUES (1, 'abc', 1.5), (2, '1', 0), (3, '0', 0), (4, NULL, -0.25), (5, '0.5', 2), " ++
            "(6, 'true', 0), (7, ' FALSE ', NULL), (8, '2147483648', 0), (9, '-3', NULL)",
    );

    const N: ?[]const u8 = null;
    const truth_of_s: []const ?[]const u8 = &.{ N, "1", "0", N, N, "1", "0", N, "1" };
    const not_s: []const ?[]const u8 = &.{ N, "0", "1", N, N, "0", "1", N, "0" };
    // A select-list operand of OR, AND or XOR is followed by an alias, an
    // alias without AS, or FROM, all of which close it.
    const value_cases = .{
        .{ "SELECT s OR 0 AS c FROM bc ORDER BY id", truth_of_s },
        .{ "SELECT s AND 1 c FROM bc ORDER BY id", truth_of_s },
        .{ "SELECT s XOR 0 FROM bc ORDER BY id", truth_of_s },
        .{ "SELECT CAST(s AS BOOLEAN) FROM bc ORDER BY id", truth_of_s },
        .{ "SELECT NOT s FROM bc ORDER BY id", not_s },
        .{ "SELECT s OR 1 FROM bc ORDER BY id", &[_]?[]const u8{ "1", "1", "1", "1", "1", "1", "1", "1", "1" } },
        .{ "SELECT s AND 0 FROM bc ORDER BY id", &[_]?[]const u8{ "0", "0", "0", "0", "0", "0", "0", "0", "0" } },
        .{ "SELECT s OR NULL FROM bc ORDER BY id", &[_]?[]const u8{ N, "1", N, N, N, "1", N, N, "1" } },
        .{ "SELECT NULL OR s FROM bc ORDER BY id", &[_]?[]const u8{ N, "1", N, N, N, "1", N, N, "1" } },
        .{ "SELECT s AND NULL FROM bc ORDER BY id", &[_]?[]const u8{ N, N, "0", N, N, N, "0", N, N } },
        .{ "SELECT NULL AND s FROM bc ORDER BY id", &[_]?[]const u8{ N, N, "0", N, N, N, "0", N, N } },
        .{ "SELECT n OR 0 FROM bc ORDER BY id", &[_]?[]const u8{ "1", "0", "0", "1", "1", "0", N, "0", N } },
        .{ "SELECT s OR n FROM bc ORDER BY id", &[_]?[]const u8{ "1", "1", "0", "1", "1", "1", N, N, "1" } },
        .{ "SELECT NOT (s OR n) FROM bc ORDER BY id", &[_]?[]const u8{ "0", "0", "1", "0", "0", "0", N, N, "0" } },
        .{ "SELECT s AND NOT n FROM bc ORDER BY id", &[_]?[]const u8{ "0", "1", "0", "0", "0", "1", "0", N, N } },
        .{ "SELECT s XOR n FROM bc ORDER BY id", &[_]?[]const u8{ N, "1", "0", N, N, "1", N, N, N } },
        .{ "SELECT IF(s, 'y', 'n') FROM bc ORDER BY id", &[_]?[]const u8{ "n", "y", "n", "n", "n", "y", "n", "n", "y" } },
        .{ "SELECT CASE WHEN s THEN 'y' ELSE 'n' END FROM bc ORDER BY id", &[_]?[]const u8{ "n", "y", "n", "n", "n", "y", "n", "n", "y" } },
        .{ "SELECT s IS TRUE FROM bc ORDER BY id", &[_]?[]const u8{ "0", "1", "0", "0", "0", "1", "0", "0", "1" } },
        .{ "SELECT s IS NOT TRUE FROM bc ORDER BY id", &[_]?[]const u8{ "1", "0", "1", "1", "1", "0", "1", "1", "0" } },
        .{ "SELECT s IS FALSE FROM bc ORDER BY id", &[_]?[]const u8{ "0", "0", "1", "0", "0", "0", "1", "0", "0" } },
        .{ "SELECT s IS NOT FALSE FROM bc ORDER BY id", &[_]?[]const u8{ "1", "1", "0", "1", "1", "1", "0", "1", "1" } },
        .{ "SELECT (s OR n) IS UNKNOWN FROM bc ORDER BY id", &[_]?[]const u8{ "0", "0", "0", "0", "0", "0", "1", "1", "0" } },
        .{ "SELECT MIN(s OR 'x') FROM bc", &[_]?[]const u8{"1"} },
        .{ "SELECT MAX(s OR 'x') FROM bc", &[_]?[]const u8{"1"} },
        .{ "SELECT COUNT(s OR 'x') FROM bc", &[_]?[]const u8{"3"} },
        .{ "SELECT SUM(s OR 0) FROM bc", &[_]?[]const u8{"3"} },
        .{ "SELECT COUNT(s AND 1) FROM bc", &[_]?[]const u8{"5"} },
        .{ "SELECT MIN(NOT s) FROM bc", &[_]?[]const u8{"0"} },
        .{ "SELECT SUM(IF(s, 1, 0)) FROM bc", &[_]?[]const u8{"3"} },
        .{ "SELECT MIN(s OR 'x') FROM bc WHERE id IN (1, 5)", &[_]?[]const u8{N} },
        .{ "SELECT COUNT(s OR 'x') FROM bc WHERE id IN (1, 5)", &[_]?[]const u8{"0"} },
        .{ "SELECT MIN(s OR 0), id % 2 FROM bc GROUP BY id % 2 ORDER BY 1", &[_]?[]const u8{ "0", "1" } },
        .{ "SELECT id % 2 FROM bc GROUP BY id % 2 HAVING MIN(s OR 0) ORDER BY 1", &[_]?[]const u8{"0"} },
        .{ "SELECT 'abc' OR 'x' AS c FROM bc WHERE id = 1", &[_]?[]const u8{N} },
        .{ "SELECT ' TRUE ' AND '-3' AS c FROM bc WHERE id = 1", &[_]?[]const u8{"1"} },
        .{ "SELECT '0.5' OR 0 FROM bc WHERE id = 1", &[_]?[]const u8{N} },
        .{ "SELECT NOT 'false' FROM bc WHERE id = 1", &[_]?[]const u8{"1"} },
        .{ "SELECT NULL OR 'true' FROM bc WHERE id = 1", &[_]?[]const u8{"1"} },
        .{ "SELECT 'false' AND NULL FROM bc WHERE id = 1", &[_]?[]const u8{"0"} },
        .{ "SELECT 'abc' IS FALSE FROM bc WHERE id = 1", &[_]?[]const u8{"0"} },
        .{ "SELECT '2147483648' IS NOT TRUE FROM bc WHERE id = 1", &[_]?[]const u8{"1"} },
    };
    const where_cases = .{
        .{ "s", &[_]i64{ 2, 6, 9 } },
        .{ "NOT s", &[_]i64{ 3, 7 } },
        .{ "s OR n", &[_]i64{ 1, 2, 4, 5, 6, 9 } },
        .{ "NOT (s OR n)", &[_]i64{3} },
        .{ "s AND NOT n", &[_]i64{ 2, 6 } },
        .{ "NULL OR s", &[_]i64{ 2, 6, 9 } },
        .{ "s XOR n", &[_]i64{ 2, 6 } },
        .{ "NOT (s XOR n)", &[_]i64{3} },
        .{ "s IS NOT TRUE", &[_]i64{ 1, 3, 4, 5, 7, 8 } },
        .{ "NOT (s IS TRUE)", &[_]i64{ 1, 3, 4, 5, 7, 8 } },
        .{ "s IS FALSE", &[_]i64{ 3, 7 } },
        .{ "CASE WHEN s THEN 1 END", &[_]i64{ 2, 6, 9 } },
    };
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("bc", .{})).flush();
        inline for (value_cases) |c| try expectTexts(allocator, db, .neutral, c[0], c[1]);
        inline for (where_cases) |c| {
            const got = try helpers.collectBigints(allocator, db, "SELECT id FROM bc WHERE " ++ c[0] ++ " ORDER BY id");
            defer allocator.free(got);
            std.testing.expectEqualSlices(i64, c[1], got) catch |err| {
                std.debug.print("WHERE {s} (pass {d})\n", .{ c[0], pass });
                return err;
            };
        }
    }

    // `||` is OR and `!` is NOT on the MySQL wire, as in StarRocks without
    // PIPES_AS_CONCAT; the other dialects concatenate.
    try expectTexts(allocator, db, .mysql, "SELECT s || 0 AS c FROM bc ORDER BY id", truth_of_s);
    try expectTexts(allocator, db, .mysql, "SELECT !s FROM bc ORDER BY id", not_s);
    try expectTexts(allocator, db, .mysql, "SELECT id FROM bc WHERE s || n ORDER BY id", &.{ "1", "2", "4", "5", "6", "9" });
    try expectTexts(allocator, db, .neutral, "SELECT s || 'x' FROM bc WHERE id = 2", &.{"1x"});
}
