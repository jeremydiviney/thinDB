//! The result-type rule (`cast.commonType`) wherever one result may hold a
//! value of several types: CASE/IF branches, the arguments GREATEST, LEAST
//! and COALESCE return, UNION arms, and a string parameter given a number
//! or date. Each result is rendered as text, so a decimal read at the wrong
//! scale shows in the expected string.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

const Case = struct { sql: []const u8, expected: []const []const u8 };

/// Rendering of a NULL cell in `Case.expected`.
const NULL = "NULL";

fn expectTexts(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, expected: []const []const u8) !void {
    var q = try helpers.runSqlCtx(allocator, db, sql);
    defer q.deinit();
    var row: usize = 0;
    while (try q.next()) |batch| {
        const view = batch.values[0];
        for (0..batch.row_count) |i| {
            if (row >= expected.len) return error.TestUnexpectedResult;
            const text = if (view.isValid(i)) view.data.string.rowBytes(i) else NULL;
            try std.testing.expectEqualStrings(expected[row], text);
            row += 1;
        }
    }
    try std.testing.expectEqual(expected.len, row);
}

fn expectCases(allocator: std.mem.Allocator, db: *thindb.Database, cases: []const Case) !void {
    for (cases) |c| {
        expectTexts(allocator, db, c.sql, c.expected) catch |err| {
            std.debug.print("case failed ({s}): {s}\n", .{ @errorName(err), c.sql });
            return err;
        };
    }
}

fn expectCasesBeforeAndAfterFlush(allocator: std.mem.Allocator, db: *thindb.Database, cases: []const Case) !void {
    try expectCases(allocator, db, cases);
    try (try db.openTable("rt", .{})).flush();
    try expectCases(allocator, db, cases);
}

fn setup(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    try helpers.exec(allocator, db,
        \\CREATE TABLE rt (id BIGINT PRIMARY KEY, a DECIMAL(10,2), b DECIMAL(12,4), i INT, x DOUBLE,
        \\  d DATE, ts DATETIME, s VARCHAR(20))
    );
    try helpers.exec(allocator, db,
        \\INSERT INTO rt VALUES
        \\  (1, 1.50, 1.5000, 2, 1.5, '2024-03-05', '2024-03-05 00:00:00', 'apple'),
        \\  (2, 3.25, 3.2400, 3, 3.25, '2024-03-06', '2024-03-05 10:00:00', 'pear'),
        \\  (3, 2.00, 2.0000, NULL, 2.5, NULL, '2024-03-07 00:00:00', 'kiwi')
    );
}

test "result type: CASE and IF branches meet at one type" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setup(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{
        .{ .sql = "SELECT CAST(CASE WHEN id = 1 THEN a ELSE b END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5000", "3.2400", "2.0000" } },
        .{ .sql = "SELECT CAST(CASE WHEN id = 1 THEN b ELSE a END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5000", "3.2500", "2.0000" } },
        .{ .sql = "SELECT CAST(IF(id = 1, a, b) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5000", "3.2400", "2.0000" } },
        .{ .sql = "SELECT CAST(CASE WHEN id = 2 THEN a ELSE 0 END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "0.00", "3.25", "0.00" } },
        .{ .sql = "SELECT CAST(CASE WHEN id = 1 THEN a ELSE x END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5", "3.25", "2.5" } },
        .{ .sql = "SELECT CAST(CASE WHEN id = 1 THEN d ELSE ts END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2024-03-05 00:00:00", "2024-03-05 10:00:00", "2024-03-07 00:00:00" } },
        .{ .sql = "SELECT CASE WHEN id = 1 THEN i ELSE s END FROM rt ORDER BY id", .expected = &.{ "2", "pear", "kiwi" } },
        .{ .sql = "SELECT CAST(CASE WHEN id = 3 THEN NULL ELSE b END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5000", "3.2400", NULL } },
    });
}

test "result type: GREATEST, LEAST and COALESCE return their arguments' common type" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setup(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{
        .{ .sql = "SELECT CAST(GREATEST(a, b) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5000", "3.2500", "2.0000" } },
        .{ .sql = "SELECT CAST(GREATEST(a, x) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5", "3.25", "2.5" } },
        .{ .sql = "SELECT CAST(LEAST(d, ts) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2024-03-05 00:00:00", "2024-03-05 10:00:00", NULL } },
        .{ .sql = "SELECT CAST(GREATEST(d, '2024-03-06') AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2024-03-06", "2024-03-06", NULL } },
        .{ .sql = "SELECT CAST(GREATEST(i, s) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "apple", "pear", NULL } },
        .{ .sql = "SELECT CAST(COALESCE(d, ts) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2024-03-05 00:00:00", "2024-03-06 00:00:00", "2024-03-07 00:00:00" } },
        .{ .sql = "SELECT CAST(COALESCE(i, a) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2.00", "3.00", "2.00" } },
        .{ .sql = "SELECT CAST(COALESCE(i, x) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2", "3", "2.5" } },
        .{ .sql = "SELECT CAST(GREATEST(i, x, 2.75) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2.75", "3.25", NULL } },
        .{ .sql = "SELECT CAST(LEAST(id, i, 3) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1", "2", NULL } },
        .{ .sql = "SELECT CAST(GREATEST(a, b, 2) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2.0000", "3.2500", "2.0000" } },
        // Text among them makes the common type text, as in StarRocks.
        .{ .sql = "SELECT CAST(LEAST(d, ts, '2024-03-05 12:00:00') AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2024-03-05", "2024-03-05 10:00:00", NULL } },
        .{ .sql = "SELECT GREATEST(s, 'banana', 'cherry') FROM rt ORDER BY id", .expected = &.{ "cherry", "pear", "kiwi" } },
        .{ .sql = "SELECT LEAST(s, 'banana', 'cherry', s) FROM rt ORDER BY id", .expected = &.{ "apple", "banana", "banana" } },
    });
}

test "result type: a string parameter takes a number or date as its text" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setup(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{
        .{ .sql = "SELECT CONCAT(s, i) FROM rt ORDER BY id", .expected = &.{ "apple2", "pear3", NULL } },
        .{ .sql = "SELECT CONCAT('Q', i, '-', d) FROM rt ORDER BY id", .expected = &.{ "Q2-2024-03-05", "Q3-2024-03-06", NULL } },
        .{ .sql = "SELECT CONCAT(a, '') FROM rt ORDER BY id", .expected = &.{ "1.50", "3.25", "2.00" } },
        .{ .sql = "SELECT CONCAT(s, ts) FROM rt ORDER BY id", .expected = &.{ "apple2024-03-05 00:00:00", "pear2024-03-05 10:00:00", "kiwi2024-03-07 00:00:00" } },
        .{ .sql = "SELECT CONCAT(s, x) FROM rt ORDER BY id", .expected = &.{ "apple1.5", "pear3.25", "kiwi2.5" } },
        .{ .sql = "SELECT CAST(LENGTH(b) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "6", "6", "6" } },
        .{ .sql = "SELECT UPPER(i) FROM rt ORDER BY id", .expected = &.{ "2", "3", NULL } },
        .{ .sql = "SELECT CAST(d AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2024-03-05", "2024-03-06", NULL } },
        .{ .sql = "SELECT CONCAT('a', 'b', 'c', 'd', 'e') FROM rt WHERE id = 1", .expected = &.{"abcde"} },
    });
}

test "result type: UNION arms meet at one type" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setup(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{
        .{ .sql = "SELECT CAST(v AS CHAR) AS t FROM (SELECT a AS v FROM rt UNION ALL SELECT b FROM rt) u ORDER BY t", .expected = &.{ "1.5000", "1.5000", "2.0000", "2.0000", "3.2400", "3.2500" } },
        .{ .sql = "SELECT CAST(v AS CHAR) AS t FROM (SELECT b AS v FROM rt UNION SELECT a FROM rt) u ORDER BY t", .expected = &.{ "1.5000", "2.0000", "3.2400", "3.2500" } },
        .{ .sql = "SELECT CAST(v AS CHAR) AS t FROM (SELECT a AS v FROM rt UNION ALL SELECT x FROM rt) u ORDER BY t", .expected = &.{ "1.5", "1.5", "2", "2.5", "3.25", "3.25" } },
        .{ .sql = "SELECT v FROM (SELECT id AS v FROM rt UNION ALL SELECT s FROM rt) u ORDER BY v", .expected = &.{ "1", "2", "3", "apple", "kiwi", "pear" } },
    });
}

test "result type: a number meets a DATE as its YYYYMMDD number and a DATETIME as its YYYYMMDDhhmmss number (issue #430)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db,
        \\CREATE TABLE rt (id BIGINT PRIMARY KEY, ti TINYINT, i INT, li LARGEINT, bo BOOLEAN, f FLOAT,
        \\  x DOUBLE, a DECIMAL(10,2), d DATE, ts DATETIME)
    );
    try helpers.exec(allocator, db,
        \\INSERT INTO rt VALUES
        \\  (1, 5, 20260131, 20260131, true, 1.5, 20260131, 1.50, '2026-01-01', '2026-01-01 10:20:30'),
        \\  (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-02-03', '2026-02-03 04:05:06'),
        \\  (3, 7, 20260131, 20260131, false, 1.5, 20260131, 1.50, NULL, NULL)
    );

    // StarRocks' results. An integer meets a DATE as INT and a DATETIME as
    // BIGINT, a decimal meets either as DOUBLE, a float meets a DATETIME as
    // DOUBLE and a DATE as text.
    try expectCasesBeforeAndAfterFlush(allocator, db, &.{
        .{ .sql = "SELECT CAST(CAST(d AS INT) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "20260101", "20260203", NULL } },
        .{ .sql = "SELECT CAST(COALESCE(i, d) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "20260131", "20260203", "20260131" } },
        .{ .sql = "SELECT CAST(IFNULL(i, ts) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "20260131", "20260203040506", "20260131" } },
        .{ .sql = "SELECT CAST(COALESCE(i, d, ts) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "20260131", "20260203", "20260131" } },
        .{ .sql = "SELECT CAST(GREATEST(i, d) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "20260131", NULL, NULL } },
        .{ .sql = "SELECT CAST(LEAST(i, d) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "20260101", NULL, NULL } },
        .{ .sql = "SELECT CAST(GREATEST(i, ts) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "20260101102030", NULL, NULL } },
        .{ .sql = "SELECT CAST(IF(id = 1, ti, d) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "5", "20260203", NULL } },
        .{ .sql = "SELECT CAST(CASE WHEN id = 1 THEN bo ELSE d END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1", "20260203", NULL } },
        .{ .sql = "SELECT CAST(CASE WHEN id = 1 THEN ts ELSE ti END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "20260101102030", NULL, "7" } },
        .{ .sql = "SELECT CAST(COALESCE(li, ts) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "20260131", "20260203040506", "20260131" } },
        .{ .sql = "SELECT CAST(COALESCE(a, d) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5", "20260203", "1.5" } },
        .{ .sql = "SELECT CAST(COALESCE(x, ts) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "20260131", "20260203040506", "20260131" } },
        .{ .sql = "SELECT COALESCE(x, d) FROM rt ORDER BY id", .expected = &.{ "20260131", "2026-02-03", "20260131" } },
        .{ .sql = "SELECT GREATEST(f, d) FROM rt ORDER BY id", .expected = &.{ "2026-01-01", NULL, NULL } },
        .{ .sql = "SELECT LEAST(f, d) FROM rt ORDER BY id", .expected = &.{ "1.5", NULL, NULL } },
        .{ .sql = "SELECT CAST(IF(id = 1, 20260131, DATE '2026-01-01') AS CHAR) FROM rt ORDER BY id", .expected = &.{ "20260131", "20260101", "20260101" } },
        .{ .sql = "SELECT CAST(CASE WHEN id = 1 THEN 20260131 ELSE DATE '2026-01-01' END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "20260131", "20260101", "20260101" } },
        .{ .sql = "SELECT CAST(GREATEST(20260131, DATE '2026-01-01') AS CHAR) FROM rt WHERE id = 1", .expected = &.{"20260131"} },
        .{ .sql = "SELECT CAST(COALESCE(20260131, TIMESTAMP '2026-01-01 10:00:00') AS CHAR) FROM rt WHERE id = 1", .expected = &.{"20260131"} },
        .{ .sql = "SELECT CAST(v AS CHAR) FROM (SELECT i AS v FROM rt WHERE id = 1 UNION ALL SELECT d FROM rt WHERE id <= 2) u ORDER BY v", .expected = &.{ "20260101", "20260131", "20260203" } },
        .{ .sql = "WITH u AS (SELECT x AS v FROM rt WHERE id = 1 UNION ALL SELECT ts FROM rt WHERE id <= 2) SELECT CAST(v AS CHAR) FROM u ORDER BY v", .expected = &.{ "20260131", "20260101102030", "20260203040506" } },
    });
}

test "result type: kinds that never meet are rejected" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE rt (id BIGINT PRIMARY KEY, u UUID, i INT, d DATE)");

    try helpers.expectRunError(allocator, db, "SELECT CASE WHEN id = 1 THEN i ELSE u END FROM rt", error.ComputeUnsupportedExpr);
    try helpers.expectRunError(allocator, db, "SELECT i FROM rt UNION ALL SELECT u FROM rt", error.TypeMismatch);
    try helpers.expectRunError(allocator, db, "SELECT d FROM rt UNION ALL SELECT u FROM rt", error.TypeMismatch);
}

test "result type: a LARGEINT meets a decimal with a fraction as DOUBLE, and one without as LARGEINT (issue #426)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE rt (id BIGINT PRIMARY KEY, li LARGEINT, a DECIMAL(10,2), z DECIMAL(10,0))");
    try helpers.exec(allocator, db,
        \\INSERT INTO rt VALUES
        \\  (1, 170141183460469231731687303715884105727, 1.50, 1),
        \\  (2, 7, 1.25, 99999),
        \\  (3, NULL, 2.00, NULL),
        \\  (4, -170141183460469231731687303715884105728, NULL, 5)
    );

    // Expected values are StarRocks': DOUBLE beside a fraction, and every
    // digit beside DECIMAL(p,0), which StarRocks types DECIMAL(38,0). A
    // double is written as thinDB writes one, with no '+' in its exponent.
    const big = "170141183460469231731687303715884105727";
    const min = "-170141183460469231731687303715884105728";
    const big_double = "1.7014118346046923e38";
    const min_double = "-1.7014118346046923e38";
    try expectCasesBeforeAndAfterFlush(allocator, db, &.{
        .{ .sql = "SELECT CAST(IF(id <= 2, li, a) AS CHAR) FROM rt ORDER BY id", .expected = &.{ big_double, "7", "2", NULL } },
        .{ .sql = "SELECT CAST(CASE WHEN id <= 2 THEN li ELSE a END AS CHAR) FROM rt ORDER BY id", .expected = &.{ big_double, "7", "2", NULL } },
        .{ .sql = "SELECT CAST(COALESCE(li, a) AS CHAR) FROM rt ORDER BY id", .expected = &.{ big_double, "7", "2", min_double } },
        .{ .sql = "SELECT CAST(IFNULL(li, a) AS CHAR) FROM rt ORDER BY id", .expected = &.{ big_double, "7", "2", min_double } },
        .{ .sql = "SELECT CAST(GREATEST(li, a) AS CHAR) FROM rt ORDER BY id", .expected = &.{ big_double, "7", NULL, NULL } },
        .{ .sql = "SELECT CAST(LEAST(li, a) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5", "1.25", NULL, NULL } },
        .{ .sql = "SELECT CAST(NULLIF(li, a) AS CHAR) FROM rt ORDER BY id", .expected = &.{ big_double, "7", NULL, min_double } },
        .{ .sql = "SELECT CAST(IF(id <= 2, li, z) AS CHAR) FROM rt ORDER BY id", .expected = &.{ big, "7", NULL, "5" } },
        .{ .sql = "SELECT CAST(CASE WHEN id <= 2 THEN li ELSE z END AS CHAR) FROM rt ORDER BY id", .expected = &.{ big, "7", NULL, "5" } },
        .{ .sql = "SELECT CAST(COALESCE(li, z) AS CHAR) FROM rt ORDER BY id", .expected = &.{ big, "7", NULL, min } },
        .{ .sql = "SELECT CAST(GREATEST(li, z) AS CHAR) FROM rt ORDER BY id", .expected = &.{ big, "99999", NULL, "5" } },
        .{ .sql = "SELECT CAST(LEAST(li, z) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1", "7", NULL, min } },
        .{ .sql = "SELECT CAST(IF(id = 1, " ++ big ++ ", 1.5) AS CHAR) FROM rt ORDER BY id", .expected = &.{ big_double, "1.5", "1.5", "1.5" } },
        .{ .sql = "SELECT CAST(CASE WHEN id = 1 THEN a ELSE " ++ big ++ " END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5", big_double, big_double, big_double } },
        .{ .sql = "SELECT CAST(CASE WHEN id = 1 THEN z ELSE " ++ big ++ " END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1", big, big, big } },
        .{ .sql = "SELECT CAST(v AS CHAR) AS t FROM (SELECT li AS v FROM rt WHERE id <= 2 UNION ALL SELECT a FROM rt WHERE id = 2) u ORDER BY v", .expected = &.{ "1.25", "7", big_double } },
        .{ .sql = "SELECT CAST(v AS CHAR) AS t FROM (SELECT li AS v FROM rt WHERE id <= 2 UNION ALL SELECT z FROM rt WHERE id = 2) u ORDER BY v", .expected = &.{ "7", "99999", big } },
        .{ .sql = "WITH u AS (SELECT li AS v FROM rt WHERE id <= 2 UNION ALL SELECT a FROM rt WHERE id = 2) SELECT CAST(v AS CHAR) FROM u ORDER BY v", .expected = &.{ "1.25", "7", big_double } },
        .{ .sql = "WITH u AS (SELECT li AS v FROM rt WHERE id <= 2 UNION ALL SELECT z FROM rt WHERE id = 2) SELECT CAST(v AS CHAR) FROM u ORDER BY v", .expected = &.{ "7", "99999", big } },
    });

    // A VALUES list with an expression cell meets its rows at the common
    // type too, before each lands in its column.
    try helpers.exec(allocator, db, "CREATE TABLE lv (id BIGINT PRIMARY KEY, v LARGEINT)");
    try helpers.exec(allocator, db, "INSERT INTO lv VALUES (1, " ++ big ++ "), (2, 99999999999999999999999999999999999999), (3, 1 + 1)");
    try expectCases(allocator, db, &.{
        .{ .sql = "SELECT CAST(v AS CHAR) FROM lv ORDER BY id", .expected = &.{ big, "99999999999999999999999999999999999999", "2" } },
    });
}
