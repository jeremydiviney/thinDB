//! The comparison rule (`predicate.typesComparable`) across every place a
//! comparison is formed: WHERE literals and column pairs, IN lists,
//! subquery results, correlated keys and join keys. Each case runs against
//! the memtable and again against flushed segments, whose encoded kernels
//! and zonemaps see the coerced literals.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

const Case = struct { sql: []const u8, expected: []const i64 };

fn expectCases(allocator: std.mem.Allocator, db: *thindb.Database, cases: []const Case) !void {
    for (cases) |c| {
        const ids = helpers.collectBigintsCtx(allocator, db, c.sql) catch |err| {
            std.debug.print("query failed ({s}): {s}\n", .{ @errorName(err), c.sql });
            return err;
        };
        defer allocator.free(ids);
        std.testing.expectEqualSlices(i64, c.expected, ids) catch |err| {
            std.debug.print("wrong rows: {s}\n", .{c.sql});
            return err;
        };
    }
}

fn expectCasesBeforeAndAfterFlush(allocator: std.mem.Allocator, db: *thindb.Database, tables: []const []const u8, cases: []const Case) !void {
    try expectCases(allocator, db, cases);
    for (tables) |name| try (try db.openTable(name, .{})).flush();
    try expectCases(allocator, db, cases);
}

fn setupMixed(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    try helpers.exec(allocator, db,
        \\CREATE TABLE cm (id BIGINT PRIMARY KEY, d DATE, ts DATETIME, a DECIMAL(10,2), b DECIMAL(12,4),
        \\  c DECIMAL(30,3), i INT, sm SMALLINT, x DOUBLE, s VARCHAR(20), s2 VARCHAR(20), ds VARCHAR(20), ns VARCHAR(20))
    );
    try helpers.exec(allocator, db,
        \\INSERT INTO cm VALUES
        \\  (1, '2024-03-05', '2024-03-05 00:00:00', 1.50, 1.5000, 2.000, 2, 1, 1.5, 'apple', 'banana', '2024-03-05', '12'),
        \\  (2, '2024-03-06', '2024-03-05 10:00:00', 3.25, 3.2400, 3.250, 3, 2, 3.25, 'pear', 'fig', '2024-03-07', '2.5'),
        \\  (3, NULL, '2024-03-07 00:00:00', 2.00, 2.0000, 1.000, 2, NULL, 2.5, 'kiwi', 'kiwi', 'junk', 'x')
    );
}

test "comparison: column pairs of different types compare by value" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setupMixed(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{"cm"}, &.{
        .{ .sql = "SELECT id FROM cm WHERE d = ts ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE ts >= d ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE d < ts ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM cm WHERE NOT (d = ts) ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE a = b ORDER BY id", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE a > b ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE a = i ORDER BY id", .expected = &.{3} },
        .{ .sql = "SELECT id FROM cm WHERE i < a ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE a = x ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM cm WHERE c > a ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE c = i ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE s < s2 ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE s >= s2 ORDER BY id", .expected = &.{ 2, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE ds = d ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE d < ds ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE ns > x ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE a * 2 > b ORDER BY id", .expected = &.{ 1, 2, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE LOWER('KIWI') = s ORDER BY id", .expected = &.{3} },
        .{ .sql = "SELECT id FROM cm WHERE CASE WHEN d < ts THEN 0 ELSE 1 END = 1 ORDER BY id", .expected = &.{ 1, 2, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE CASE WHEN a > b THEN 1 ELSE 0 END ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE DATE(ts) = d ORDER BY id", .expected = &.{1} },
    });
}

test "comparison: literals of another type take the column's type" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setupMixed(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{"cm"}, &.{
        .{ .sql = "SELECT id FROM cm WHERE ts >= DATE '2024-03-06' ORDER BY id", .expected = &.{3} },
        .{ .sql = "SELECT id FROM cm WHERE ts >= CAST('2024-03-06' AS DATE) ORDER BY id", .expected = &.{3} },
        .{ .sql = "SELECT id FROM cm WHERE d < TIMESTAMP '2024-03-06 10:00:00' ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM cm WHERE d = '2024-03-05 10:00:00' ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM cm WHERE ts = '2024-03-05' ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE ts IN (DATE '2024-03-05') ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE d IN ('2024-03-05', '2024-03-06') ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM cm WHERE a = CAST(1.5 AS DECIMAL(12,4)) ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE a > 1.5e0 ORDER BY id", .expected = &.{ 2, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE i = 1.5 + 0.5 ORDER BY id", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE i = '2' ORDER BY id", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE x = '1.5' ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE a = ' 1.5 ' ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE i IN ('2', '3') ORDER BY id", .expected = &.{ 1, 2, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE i = 'abc' ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM cm WHERE i <> 'abc' ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM cm WHERE s > 'b' ORDER BY id", .expected = &.{ 2, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE s BETWEEN 'a' AND 'l' ORDER BY id", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE sm = 100000 ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM cm WHERE sm <> 100000 ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM cm WHERE sm < 100000 ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM cm WHERE sm >= 100000 ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM cm WHERE sm > -100000 ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM cm WHERE sm < 32767.5 ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM cm WHERE sm IN (100000, 2) ORDER BY id", .expected = &.{2} },
    });
}

test "comparison: subquery results compare by value" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setupMixed(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{"cm"}, &.{
        .{ .sql = "SELECT id FROM cm WHERE a = (SELECT b FROM cm WHERE id = 1) ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE b = (SELECT a FROM cm WHERE id = 1) ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE i < (SELECT MAX(a) FROM cm) ORDER BY id", .expected = &.{ 1, 2, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE a IN (SELECT b FROM cm) ORDER BY id", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE a > (SELECT AVG(x) FROM cm) ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE ts > (SELECT MAX(d) FROM cm) ORDER BY id", .expected = &.{3} },
        .{ .sql = "SELECT id FROM cm WHERE a + (SELECT MIN(b) FROM cm) > 4 ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE a > (SELECT AVG(x) FROM cm) - 0.5 ORDER BY id", .expected = &.{ 2, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE x IN (SELECT x FROM cm WHERE id < 3) ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM cm WHERE x IN (1.5, 2.5) ORDER BY id", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE x NOT IN (1.5, 2.5) ORDER BY id", .expected = &.{2} },
    });
}

test "comparison: a decimal scalar subquery projects at its scale" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setupMixed(allocator, db);

    var q = try helpers.runSqlCtx(allocator, db, "SELECT (SELECT MAX(a) FROM cm) AS m");
    defer q.deinit();
    const spec = q.outputSchema()[0].type.decimalSpec() orelse return error.TestUnexpectedResult;
    const batch = (try q.next()) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    const mantissa: i128 = switch (batch.values[0].data) {
        .decimal64 => |m| m[0],
        .decimal128 => |m| m[0],
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(i128, 325), @divExact(mantissa * 100, std.math.pow(i128, 10, spec.s)));
}

test "comparison: join, IN and correlated keys of different types match by value" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE ja (id BIGINT PRIMARY KEY, k INT, a DECIMAL(10,2), d DATE, s VARCHAR(10))");
    try helpers.exec(allocator, db, "CREATE TABLE jb (id BIGINT PRIMARY KEY, k BIGINT, b DECIMAL(12,4), ts DATETIME, n INT)");
    try helpers.exec(allocator, db, "INSERT INTO ja VALUES (1, 1, 1.50, '2024-03-05', '1'), (2, 2, 2.25, '2024-03-06', '2')");
    try helpers.exec(allocator, db, "INSERT INTO jb VALUES (10, 1, 1.5000, '2024-03-05 00:00:00', 1), (20, 2, 2.2500, '2024-03-06 10:00:00', 2)");

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{ "ja", "jb" }, &.{
        .{ .sql = "SELECT jb.id FROM ja JOIN jb ON ja.k = jb.k ORDER BY jb.id", .expected = &.{ 10, 20 } },
        .{ .sql = "SELECT jb.id FROM ja JOIN jb ON ja.a = jb.b ORDER BY jb.id", .expected = &.{ 10, 20 } },
        .{ .sql = "SELECT jb.id FROM ja JOIN jb ON ja.d = jb.ts ORDER BY jb.id", .expected = &.{10} },
        .{ .sql = "SELECT jb.id FROM ja JOIN jb ON ja.s = jb.n ORDER BY jb.id", .expected = &.{ 10, 20 } },
        .{ .sql = "SELECT jb.id FROM ja LEFT JOIN jb ON ja.k = jb.k AND ja.a = jb.b ORDER BY jb.id", .expected = &.{ 10, 20 } },
        .{ .sql = "SELECT ja.id FROM ja WHERE ja.k IN (SELECT k FROM jb) ORDER BY ja.id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT ja.id FROM ja WHERE ja.a IN (SELECT b FROM jb) ORDER BY ja.id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT ja.id FROM ja WHERE EXISTS (SELECT 1 FROM jb WHERE jb.k = ja.k AND jb.b = ja.a) ORDER BY ja.id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT ja.id FROM ja WHERE ja.a = (SELECT MAX(b) FROM jb WHERE jb.k = ja.k) ORDER BY ja.id", .expected = &.{ 1, 2 } },
    });
}

test "comparison: kinds that never compare are rejected" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setupMixed(allocator, db);

    try helpers.expectRunError(allocator, db, "SELECT id FROM cm WHERE i = d", error.PredicateTypeMismatch);
    try helpers.expectRunError(allocator, db, "SELECT id FROM cm WHERE ts > 5", error.PredicateTypeMismatch);
    try helpers.expectRunError(allocator, db, "SELECT id FROM cm WHERE i IN (DATE '2024-03-05', DATE '2024-03-06')", error.PredicateTypeMismatch);

    // A subquery's column meets the rule by its type, whatever rows it returns.
    try helpers.exec(allocator, db, "CREATE TABLE ck (id BIGINT PRIMARY KEY, d DATE, i INT)");
    try helpers.exec(allocator, db, "INSERT INTO ck VALUES (1, '2024-03-05', 2), (2, '2024-03-06', 3)");
    const statements = [_][]const u8{
        "SELECT id FROM cm WHERE i IN (SELECT d FROM ck)",
        "SELECT id FROM cm WHERE i NOT IN (SELECT d FROM ck)",
        "SELECT id FROM cm WHERE i IN (SELECT d FROM ck WHERE id < 0)",
        "SELECT id FROM cm WHERE i IN (SELECT ck.d FROM ck WHERE ck.id = cm.id)",
        "SELECT id FROM cm WHERE EXISTS (SELECT 1 FROM ck WHERE ck.d = cm.i)",
        "SELECT id FROM cm WHERE NOT EXISTS (SELECT 1 FROM ck WHERE ck.d = cm.i)",
        "SELECT id FROM cm WHERE EXISTS (SELECT 1 FROM ck WHERE ck.d > cm.i)",
    };
    for (statements) |sql| try helpers.expectRunError(allocator, db, sql, error.PredicateTypeMismatch);
    // A correlated scalar subquery runs as a join on its correlation keys.
    try helpers.expectRunError(allocator, db, "SELECT id FROM cm WHERE i = (SELECT MAX(ck.i) FROM ck WHERE ck.d = cm.i)", error.JoinKeyTypeMismatch);
}

fn setupTextNumbers(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    try helpers.exec(allocator, db, "CREATE TABLE tn (id BIGINT PRIMARY KEY, code VARCHAR(10), n INT)");
    try helpers.exec(allocator, db,
        \\INSERT INTO tn VALUES
        \\  (1, '12', 12), (2, '12.0', 7), (3, ' 7 ', NULL), (4, '007', NULL), (5, 'x', NULL),
        \\  (6, '', NULL), (7, NULL, NULL), (8, '1e1', NULL), (9, '-3.5', NULL), (10, '12abc', NULL)
    );
    try helpers.exec(allocator, db, "CREATE TABLE tnk (id BIGINT PRIMARY KEY, k BIGINT, n INT)");
    try helpers.exec(allocator, db, "INSERT INTO tnk VALUES (100, 1, 12), (200, 2, 12), (300, 3, 7), (400, 4, 8), (500, 5, 0), (600, 8, 10), (700, 9, -3)");
}

test "comparison: a text column against a number reads each row as a number" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setupTextNumbers(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{ "tn", "tnk" }, &.{
        .{ .sql = "SELECT id FROM tn WHERE code = 12 ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM tn WHERE code = 7 ORDER BY id", .expected = &.{ 3, 4 } },
        .{ .sql = "SELECT id FROM tn WHERE code > 5 ORDER BY id", .expected = &.{ 1, 2, 3, 4, 8 } },
        .{ .sql = "SELECT id FROM tn WHERE code < 0 ORDER BY id", .expected = &.{9} },
        .{ .sql = "SELECT id FROM tn WHERE code >= 1.5 ORDER BY id", .expected = &.{ 1, 2, 3, 4, 8 } },
        .{ .sql = "SELECT id FROM tn WHERE code = 10.0 ORDER BY id", .expected = &.{8} },
        .{ .sql = "SELECT id FROM tn WHERE code <> 12 ORDER BY id", .expected = &.{ 3, 4, 8, 9 } },
        .{ .sql = "SELECT id FROM tn WHERE NOT (code = 12) ORDER BY id", .expected = &.{ 3, 4, 8, 9 } },
        .{ .sql = "SELECT id FROM tn WHERE code BETWEEN 5 AND 11 ORDER BY id", .expected = &.{ 3, 4, 8 } },
        .{ .sql = "SELECT id FROM tn WHERE code IN (7, 12) ORDER BY id", .expected = &.{ 1, 2, 3, 4 } },
        .{ .sql = "SELECT id FROM tn WHERE code NOT IN (7, 12) ORDER BY id", .expected = &.{ 8, 9 } },
        .{ .sql = "SELECT id FROM tn WHERE code IN ('x', 12) ORDER BY id", .expected = &.{ 1, 2, 5 } },
        .{ .sql = "SELECT id FROM tn WHERE code = 12 OR code LIKE 'x%' ORDER BY id", .expected = &.{ 1, 2, 5 } },
        .{ .sql = "SELECT id FROM tn WHERE code = 12 AND id > 1 ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM tn WHERE CASE WHEN code = 12 THEN 1 ELSE 0 END = 1 ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT COUNT(*) FROM tn WHERE code > 5", .expected = &.{5} },
        .{ .sql = "SELECT id FROM tn WHERE code = (SELECT MAX(n) FROM tn) ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM tn WHERE code IN (SELECT n FROM tn WHERE n IS NOT NULL) ORDER BY id", .expected = &.{ 1, 2, 3, 4 } },
        .{ .sql = "SELECT id FROM tn WHERE code NOT IN (SELECT n FROM tn WHERE n IS NOT NULL) ORDER BY id", .expected = &.{ 8, 9 } },
        .{ .sql = "SELECT id FROM tn WHERE code IN (SELECT n FROM tnk WHERE tnk.k = tn.id) ORDER BY id", .expected = &.{ 1, 2, 3, 8 } },
        .{ .sql = "SELECT id FROM tn WHERE EXISTS (SELECT 1 FROM tnk WHERE tnk.k = tn.id AND tnk.n = tn.code) ORDER BY id", .expected = &.{ 1, 2, 3, 8 } },
        .{ .sql = "SELECT id FROM tn WHERE code = (SELECT MAX(n) FROM tnk WHERE tnk.k = tn.id) ORDER BY id", .expected = &.{ 1, 2, 3, 8 } },
        .{ .sql = "SELECT id FROM tn WHERE EXISTS (SELECT 1 FROM tnk WHERE tnk.n = tn.code) ORDER BY id", .expected = &.{ 1, 2, 3, 4, 8 } },
    });
}

test "comparison: DELETE by a text column against a number" {
    const allocator = std.testing.allocator;
    for (0..2) |flush_first| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
        defer db.close();
        try setupTextNumbers(allocator, db);
        if (flush_first == 1) try (try db.openTable("tn", .{})).flush();

        try helpers.exec(allocator, db, "DELETE FROM tn WHERE code = 7 OR code < 0");
        try expectCases(allocator, db, &.{
            .{ .sql = "SELECT id FROM tn ORDER BY id", .expected = &.{ 1, 2, 5, 6, 7, 8, 10 } },
        });
    }
}

test "comparison: a text order key against a number reads each key as a number" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE tk (code VARCHAR(10) PRIMARY KEY, v BIGINT)");
    try helpers.exec(allocator, db, "INSERT INTO tk VALUES ('007', 1), ('7', 2), ('12', 3), ('x', 4)");

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{"tk"}, &.{
        .{ .sql = "SELECT v FROM tk WHERE code = 7 ORDER BY v", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT v FROM tk WHERE code IN (7, 12) ORDER BY v", .expected = &.{ 1, 2, 3 } },
        .{ .sql = "SELECT v FROM tk WHERE code IN (SELECT v + 11 FROM tk WHERE v = 1) ORDER BY v", .expected = &.{3} },
        .{ .sql = "SELECT v FROM tk WHERE code = '7' ORDER BY v", .expected = &.{2} },
    });
    try helpers.exec(allocator, db, "DELETE FROM tk WHERE code = 7");
    try expectCases(allocator, db, &.{
        .{ .sql = "SELECT v FROM tk ORDER BY v", .expected = &.{ 3, 4 } },
    });
}
