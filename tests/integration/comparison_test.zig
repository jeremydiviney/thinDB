//! The comparison rule (`predicate.typesComparable`) across every place a
//! comparison is formed: WHERE literals and column pairs, IN lists,
//! subquery results, correlated keys, join keys and NULLIF. Each case runs against
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

test "comparison: a typed temporal literal on the left compares as on the right (issue #325)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setupMixed(allocator, db);
    try helpers.exec(allocator, db, "CREATE TABLE kd (id BIGINT PRIMARY KEY, date DATE, timestamp DATETIME)");
    try helpers.exec(allocator, db, "INSERT INTO kd VALUES (1, '2024-03-05', '2024-03-05 10:00:00'), (2, '2024-03-06', '2024-03-06 00:00:00')");

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{ "cm", "kd" }, &.{
        .{ .sql = "SELECT id FROM cm WHERE DATE '2024-03-05' = d ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE DATE '2024-03-05' < d ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE DATE '2024-03-06' >= d ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM cm WHERE DATE '2024-03-05' <=> d ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE TIMESTAMP '2024-03-05 10:00:00' = ts ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE DATETIME '2024-03-05 10:00:00' <= ts ORDER BY id", .expected = &.{ 2, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE NOT DATE '2024-03-05' = d ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE DATE '2024-03-05' BETWEEN d AND ts ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE DATE '2024-03-05' NOT BETWEEN d AND ts ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE DATE '2024-03-05' IN (d, ts) ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE DATE '2024-03-06' NOT IN (d) ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE DATE '2024-03-05' + INTERVAL 1 DAY = d ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE DATE '2024-03-05' = '2024-3-5' ORDER BY id", .expected = &.{ 1, 2, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE DATE '2024-03-05' < DATE '2024-03-06' ORDER BY id", .expected = &.{ 1, 2, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE IF(DATE '2024-03-05' IN (d), 1, 0) = 1 ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE CASE WHEN DATE '2024-03-05' < d THEN 1 ELSE 0 END = 1 ORDER BY id", .expected = &.{2} },
        // An IN list entry no literal leads compares with its value.
        .{ .sql = "SELECT id FROM cm WHERE 2 IN (i, sm) ORDER BY id", .expected = &.{ 1, 2, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE 1 IN (sm, id + 1) ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE 3 NOT IN (i, id) ORDER BY id", .expected = &.{1} },
        // A column named for the keyword stays a column.
        .{ .sql = "SELECT id FROM kd WHERE date = '2024-03-05' ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM kd WHERE date IN (DATE '2024-03-06') ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM kd WHERE DATE '2024-03-05' IN (date) ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM kd WHERE TIMESTAMP '2024-03-05 12:00:00' < timestamp ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM kd WHERE date = timestamp ORDER BY id", .expected = &.{2} },
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

test "comparison: row values compare element by element" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE rv (id BIGINT PRIMARY KEY, a INT, b INT, s VARCHAR(8))");
    try helpers.exec(allocator, db, "CREATE TABLE rw (n BIGINT PRIMARY KEY, a BIGINT, s VARCHAR(8))");
    try helpers.exec(allocator, db, "INSERT INTO rv VALUES (1, 1, 1, 'x'), (2, 1, 2, 'y'), (3, 2, 1, 'x'), (4, 2, NULL, 'z'), (5, NULL, 1, 'x')");
    try helpers.exec(allocator, db, "INSERT INTO rw VALUES (1, 1, 'x'), (2, 2, 'y'), (3, 2, 'z'), (4, NULL, 'x'), (5, 5, NULL)");

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{ "rv", "rw" }, &.{
        .{ .sql = "SELECT id FROM rv WHERE (a, b) = (1, 2) ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM rv WHERE (1, 2) = (a, b) ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM rv WHERE (a, b) <> (1, 1) ORDER BY id", .expected = &.{ 2, 3, 4 } },
        .{ .sql = "SELECT id FROM rv WHERE (a, b) < (2, 1) ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM rv WHERE (a, b) <= (2, 1) ORDER BY id", .expected = &.{ 1, 2, 3 } },
        .{ .sql = "SELECT id FROM rv WHERE (a, b) > (1, 1) ORDER BY id", .expected = &.{ 2, 3, 4 } },
        .{ .sql = "SELECT id FROM rv WHERE (a, b) >= (2, 0) ORDER BY id", .expected = &.{3} },
        .{ .sql = "SELECT id FROM rv WHERE (a, 1) < (2, b) ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM rv WHERE (a, b, s) > (1, 1, 'x') ORDER BY id", .expected = &.{ 2, 3, 4 } },
        .{ .sql = "SELECT id FROM rv WHERE NOT ((a, b) = (2, 1)) ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM rv WHERE (b, a) = (1, NULL) ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM rv WHERE (a + 1, s) = (2, 'x') ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM rv WHERE (1, 2) = (1, 2) AND id < 3 ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM rv WHERE (a, b) IN ((1, 2), (2, 1)) ORDER BY id", .expected = &.{ 2, 3 } },
        .{ .sql = "SELECT id FROM rv WHERE (a, b) NOT IN ((1, 2), (2, 1)) ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM rv WHERE (s, a) IN (('x', 1), ('z', 2)) ORDER BY id", .expected = &.{ 1, 4 } },
        .{ .sql = "SELECT id FROM rv WHERE (a, b) IN ((1, 1)) ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM rv WHERE CASE WHEN (a, s) = (2, 'x') THEN 1 ELSE 0 END = 1 ORDER BY id", .expected = &.{3} },
        .{ .sql = "SELECT id FROM rv WHERE (a, s) IN (SELECT a, s FROM rw) ORDER BY id", .expected = &.{ 1, 4 } },
        .{ .sql = "SELECT id FROM rv WHERE (a, s) IN (SELECT a, s FROM rw WHERE n > 1) ORDER BY id", .expected = &.{4} },
        .{ .sql = "SELECT id FROM rv WHERE (a, s) NOT IN (SELECT a, s FROM rw) ORDER BY id", .expected = &.{ 2, 3 } },
        .{ .sql = "SELECT id FROM rv WHERE NOT ((a, s) IN (SELECT a, s FROM rw)) ORDER BY id", .expected = &.{ 2, 3 } },
        .{ .sql = "SELECT id FROM rv WHERE (a * 1, UPPER(s)) IN (SELECT a, UPPER(s) FROM rw) ORDER BY id", .expected = &.{ 1, 4 } },
        .{ .sql = "SELECT id FROM rv WHERE (a, s) IN (SELECT a, s FROM rw WHERE rw.n = rv.id) ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM rv WHERE a * 1 IN (SELECT a FROM rw WHERE rw.n = rv.id) ORDER BY id", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT id FROM rv WHERE (a, s) NOT IN (SELECT a, s FROM rw WHERE rw.n = rv.id) ORDER BY id", .expected = &.{ 2, 3, 4 } },
        .{ .sql = "SELECT n FROM rw WHERE (a, s) IN (SELECT a, s FROM rv) ORDER BY n", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT n FROM rw WHERE (a, s) NOT IN (SELECT a, s FROM rv) ORDER BY n", .expected = &.{2} },
    });

    try helpers.expectRunError(allocator, db, "SELECT id FROM rv WHERE (a, b) = (1, 2, 3)", error.SqlRowValueWidthMismatch);
    try helpers.expectRunError(allocator, db, "SELECT id FROM rv WHERE (a, b) IN ((1, 2), (3))", error.SqlRowValueWidthMismatch);
    try helpers.expectRunError(allocator, db, "SELECT id FROM rv WHERE (a, s) IN (SELECT a FROM rw)", error.BadRequest);
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

test "comparison: a join converts its keys for matching only" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE ka (id BIGINT PRIMARY KEY, k INT, a DECIMAL(10,2), d DATE, s VARCHAR(10))");
    try helpers.exec(allocator, db, "CREATE TABLE kb (id BIGINT PRIMARY KEY, k BIGINT, b DECIMAL(12,4), ts DATETIME, n INT)");
    try helpers.exec(allocator, db, "INSERT INTO ka VALUES (1, 1, 1.50, '2024-03-05', '1')");
    try helpers.exec(allocator, db, "INSERT INTO kb VALUES (10, 1, 1.5000, '2024-03-05 00:00:00', 1)");

    // Each joined left key keeps the type a plain SELECT of it has.
    const cases = [_]struct { plain: []const u8, joined: []const u8 }{
        .{ .plain = "SELECT ka.k FROM ka", .joined = "SELECT ka.k FROM ka JOIN kb ON ka.k = kb.k" },
        .{ .plain = "SELECT ka.a FROM ka", .joined = "SELECT ka.a FROM ka JOIN kb ON ka.a = kb.b" },
        .{ .plain = "SELECT ka.d FROM ka", .joined = "SELECT ka.d FROM ka LEFT JOIN kb ON ka.d = kb.ts" },
        .{ .plain = "SELECT ka.s FROM ka", .joined = "SELECT ka.s FROM ka JOIN kb ON ka.s = kb.n" },
        .{ .plain = "SELECT ka.a FROM ka", .joined = "SELECT ka.a FROM ka JOIN kb ON ka.a < kb.b + 1" },
    };
    for (cases) |c| {
        var plain = try helpers.runSql(allocator, db, c.plain);
        defer plain.deinit();
        var joined = try helpers.runSql(allocator, db, c.joined);
        defer joined.deinit();
        std.testing.expect(std.meta.eql(plain.outputSchema()[0].type, joined.outputSchema()[0].type)) catch |err| {
            std.debug.print("key type changed: {s}\n", .{c.joined});
            return err;
        };
        var rows: usize = 0;
        while (try joined.next()) |b| rows += b.row_count;
        try std.testing.expectEqual(@as(usize, 1), rows);
    }
}

test "comparison: a text join key meets a number or temporal key by value" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE jt (id BIGINT PRIMARY KEY, code VARCHAR(20), day VARCHAR(30))");
    try helpers.exec(allocator, db,
        \\INSERT INTO jt VALUES (1, '007', '2024-03-05'), (2, '12.0', '2024-03-05 00:00:00'), (3, '5', ' 2024-03-06 '),
        \\  (4, 'x', '2024-03-05 10:00:00'), (5, '1.5', 'junk'), (6, '1e1', NULL), (7, NULL, '2024-03-07'),
        \\  (8, ' 7 ', '2024-03-06'), (9, '12.5', '2024-03-05')
    );
    try helpers.exec(allocator, db, "CREATE TABLE jn (id BIGINT PRIMARY KEY, n INT, t TINYINT, d DECIMAL(10,2), f DOUBLE, dt DATE, ts DATETIME)");
    try helpers.exec(allocator, db,
        \\INSERT INTO jn VALUES (10, 7, 7, 7.00, 7.0, '2024-03-05', '2024-03-05 00:00:00'),
        \\  (20, 12, 12, 12.00, 12.0, '2024-03-06', '2024-03-05 10:00:00'),
        \\  (30, 5, 5, 1.50, 1.5, '2024-03-07', '2024-03-06 00:00:00'),
        \\  (40, 10, 10, 10.00, 10.0, '2024-03-08', '2024-03-09 00:00:00')
    );

    // Each pair reads as jt.id * 100 + jn.id.
    try expectCasesBeforeAndAfterFlush(allocator, db, &.{ "jt", "jn" }, &.{
        .{ .sql = "SELECT jt.id * 100 + jn.id AS p FROM jt JOIN jn ON jt.code = jn.n ORDER BY p", .expected = &.{ 110, 220, 330, 640, 810 } },
        .{ .sql = "SELECT jt.id * 100 + jn.id AS p FROM jn JOIN jt ON jn.n = jt.code ORDER BY p", .expected = &.{ 110, 220, 330, 640, 810 } },
        .{ .sql = "SELECT jt.id * 100 + jn.id AS p FROM jt JOIN jn ON jt.code = jn.t ORDER BY p", .expected = &.{ 110, 220, 330, 640, 810 } },
        .{ .sql = "SELECT jt.id * 100 + jn.id AS p FROM jt JOIN jn ON jt.code = jn.d ORDER BY p", .expected = &.{ 110, 220, 530, 640, 810 } },
        .{ .sql = "SELECT jt.id * 100 + jn.id AS p FROM jt JOIN jn ON jt.code = jn.f ORDER BY p", .expected = &.{ 110, 220, 530, 640, 810 } },
        .{ .sql = "SELECT jt.id * 100 + COALESCE(jn.id, 0) AS p FROM jt LEFT JOIN jn ON jt.code = jn.n ORDER BY p", .expected = &.{ 110, 220, 330, 400, 500, 640, 700, 810, 900 } },
        .{ .sql = "SELECT jt.id * 100 + jn.id AS p FROM jt JOIN jn ON jt.code = jn.n AND jt.id < 5 ORDER BY p", .expected = &.{ 110, 220, 330 } },
        .{ .sql = "SELECT jt.id * 100 + jn.id AS p FROM jt JOIN jn ON jt.code < jn.n ORDER BY p", .expected = &.{ 120, 140, 310, 320, 340, 510, 520, 530, 540, 620, 820, 840 } },
        .{ .sql = "SELECT jt.id * 100 + jn.id AS p FROM jt JOIN jn ON jt.day = jn.dt ORDER BY p", .expected = &.{ 110, 210, 320, 730, 820, 910 } },
        .{ .sql = "SELECT jt.id * 100 + jn.id AS p FROM jt JOIN jn ON jt.day = jn.ts ORDER BY p", .expected = &.{ 110, 210, 330, 420, 830, 910 } },
        // The key is converted for the join only; the text column keeps its text.
        .{ .sql = "SELECT jt.id * 10 + LENGTH(jt.code) AS p FROM jt JOIN jn ON jt.code = jn.n ORDER BY p", .expected = &.{ 13, 24, 31, 63, 83 } },
        .{ .sql = "SELECT jt.id * 10 + LENGTH(jt.code) AS p FROM jt JOIN jn ON jt.code < jn.n ORDER BY p", .expected = &.{ 13, 13, 31, 31, 31, 53, 53, 53, 53, 63, 83, 83 } },
        .{ .sql = "SELECT jt.id * 10 + LENGTH(jt.code) AS p FROM jt JOIN jn ON jt.code <= jn.n AND jt.code >= jn.t ORDER BY p", .expected = &.{ 13, 24, 31, 63, 83 } },
    });
}

test "comparison: a join key no common decimal holds matches by value (issue #433)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE wk (id BIGINT PRIMARY KEY, li LARGEINT, bi BIGINT, d0 DECIMAL(38,0), d5 DECIMAL(30,5))");
    try helpers.exec(allocator, db,
        \\INSERT INTO wk VALUES
        \\  (1, 170141183460469231731687303715884105727, 9223372036854775807, 99999999999999999999999999999999999999, 9999999999999999999999999.99999),
        \\  (2, 2, 2, 2, 2.5),
        \\  (3, -170141183460469231731687303715884105727, -9223372036854775808, -99999999999999999999999999999999999999, -2.5),
        \\  (4, 1000000000000000000000000001, 1, 1000000000000000000000000001, 0.00001),
        \\  (5, NULL, NULL, NULL, NULL)
    );
    try helpers.exec(allocator, db, "CREATE TABLE nk (id BIGINT PRIMARY KEY, d1 DECIMAL(2,1), d10 DECIMAL(38,10), d20 DECIMAL(38,20))");
    try helpers.exec(allocator, db,
        \\INSERT INTO nk VALUES
        \\  (1, 1.5, 1.5, 1.5),
        \\  (2, 2.0, 2, 2),
        \\  (3, -9.9, 9999999999999999999999999999.9999999999, 999999999999999999.99999999999999999999),
        \\  (4, 9.9, 1000000000000000000000000001, -999999999999999999.99999999999999999999),
        \\  (5, NULL, -9999999999999999999999999999.9999999999, 2.5),
        \\  (6, 2.5, NULL, NULL)
    );
    try helpers.exec(allocator, db, "CREATE TABLE ek (id BIGINT PRIMARY KEY, d10 DECIMAL(38,10), d20 DECIMAL(38,20))");
    try helpers.exec(allocator, db,
        \\INSERT INTO ek VALUES
        \\  (1, 1000000000000000000000000000.5, 2.50000000000000000001),
        \\  (2, 1000000000000000000000000001.5, 2.49999999999999999999),
        \\  (3, 1000000000000000000000000001, 0.00001)
    );

    // Each pair reads as wk.id * 10 + nk.id (or ek.id). No decimal of 38
    // digits holds both keys of any pair, so a key past the common decimal
    // meets nothing and still orders by its value.
    try expectCasesBeforeAndAfterFlush(allocator, db, &.{ "wk", "nk", "ek" }, &.{
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.li = nk.d1 ORDER BY p", .expected = &.{22} },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.li < nk.d1 ORDER BY p", .expected = &.{ 24, 26, 31, 32, 33, 34, 36 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.li >= nk.d1 ORDER BY p", .expected = &.{ 11, 12, 13, 14, 16, 21, 22, 23, 41, 42, 43, 44, 46 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.li <=> nk.d1 ORDER BY p", .expected = &.{ 22, 55 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM nk JOIN wk ON nk.d1 = wk.li ORDER BY p", .expected = &.{22} },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM nk JOIN wk ON nk.d1 > wk.li ORDER BY p", .expected = &.{ 24, 26, 31, 32, 33, 34, 36 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM nk JOIN wk ON nk.d1 <= wk.li ORDER BY p", .expected = &.{ 11, 12, 13, 14, 16, 21, 22, 23, 41, 42, 43, 44, 46 } },
        .{ .sql = "SELECT wk.id * 10 + COALESCE(nk.id, 0) AS p FROM wk LEFT JOIN nk ON wk.li = nk.d1 ORDER BY p", .expected = &.{ 10, 22, 30, 40, 50 } },
        .{ .sql = "SELECT wk.id FROM wk WHERE wk.li IN (SELECT d1 FROM nk) ORDER BY wk.id", .expected = &.{2} },
        .{ .sql = "SELECT nk.id FROM nk WHERE nk.d1 IN (SELECT li FROM wk) ORDER BY nk.id", .expected = &.{2} },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.li = nk.d10 ORDER BY p", .expected = &.{ 22, 44 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.li < nk.d10 ORDER BY p", .expected = &.{ 23, 24, 31, 32, 33, 34, 35, 43 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.li >= nk.d10 ORDER BY p", .expected = &.{ 11, 12, 13, 14, 15, 21, 22, 25, 41, 42, 44, 45 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.li <=> nk.d10 ORDER BY p", .expected = &.{ 22, 44, 56 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM nk JOIN wk ON nk.d10 = wk.li ORDER BY p", .expected = &.{ 22, 44 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM nk JOIN wk ON nk.d10 > wk.li ORDER BY p", .expected = &.{ 23, 24, 31, 32, 33, 34, 35, 43 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM nk JOIN wk ON nk.d10 <= wk.li ORDER BY p", .expected = &.{ 11, 12, 13, 14, 15, 21, 22, 25, 41, 42, 44, 45 } },
        .{ .sql = "SELECT wk.id * 10 + COALESCE(nk.id, 0) AS p FROM wk LEFT JOIN nk ON wk.li = nk.d10 ORDER BY p", .expected = &.{ 10, 22, 30, 44, 50 } },
        .{ .sql = "SELECT wk.id FROM wk WHERE wk.li IN (SELECT d10 FROM nk) ORDER BY wk.id", .expected = &.{ 2, 4 } },
        .{ .sql = "SELECT nk.id FROM nk WHERE nk.d10 IN (SELECT li FROM wk) ORDER BY nk.id", .expected = &.{ 2, 4 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.bi = nk.d20 ORDER BY p", .expected = &.{22} },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.bi < nk.d20 ORDER BY p", .expected = &.{ 23, 25, 31, 32, 33, 34, 35, 41, 42, 43, 45 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.bi >= nk.d20 ORDER BY p", .expected = &.{ 11, 12, 13, 14, 15, 21, 22, 24, 44 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.bi <=> nk.d20 ORDER BY p", .expected = &.{ 22, 56 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM nk JOIN wk ON nk.d20 = wk.bi ORDER BY p", .expected = &.{22} },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM nk JOIN wk ON nk.d20 > wk.bi ORDER BY p", .expected = &.{ 23, 25, 31, 32, 33, 34, 35, 41, 42, 43, 45 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM nk JOIN wk ON nk.d20 <= wk.bi ORDER BY p", .expected = &.{ 11, 12, 13, 14, 15, 21, 22, 24, 44 } },
        .{ .sql = "SELECT wk.id * 10 + COALESCE(nk.id, 0) AS p FROM wk LEFT JOIN nk ON wk.bi = nk.d20 ORDER BY p", .expected = &.{ 10, 22, 30, 40, 50 } },
        .{ .sql = "SELECT wk.id FROM wk WHERE wk.bi IN (SELECT d20 FROM nk) ORDER BY wk.id", .expected = &.{2} },
        .{ .sql = "SELECT nk.id FROM nk WHERE nk.d20 IN (SELECT bi FROM wk) ORDER BY nk.id", .expected = &.{2} },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.d0 = nk.d10 ORDER BY p", .expected = &.{ 22, 44 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.d0 < nk.d10 ORDER BY p", .expected = &.{ 23, 24, 31, 32, 33, 34, 35, 43 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.d0 >= nk.d10 ORDER BY p", .expected = &.{ 11, 12, 13, 14, 15, 21, 22, 25, 41, 42, 44, 45 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.d0 <=> nk.d10 ORDER BY p", .expected = &.{ 22, 44, 56 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM nk JOIN wk ON nk.d10 = wk.d0 ORDER BY p", .expected = &.{ 22, 44 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM nk JOIN wk ON nk.d10 > wk.d0 ORDER BY p", .expected = &.{ 23, 24, 31, 32, 33, 34, 35, 43 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM nk JOIN wk ON nk.d10 <= wk.d0 ORDER BY p", .expected = &.{ 11, 12, 13, 14, 15, 21, 22, 25, 41, 42, 44, 45 } },
        .{ .sql = "SELECT wk.id * 10 + COALESCE(nk.id, 0) AS p FROM wk LEFT JOIN nk ON wk.d0 = nk.d10 ORDER BY p", .expected = &.{ 10, 22, 30, 44, 50 } },
        .{ .sql = "SELECT wk.id FROM wk WHERE wk.d0 IN (SELECT d10 FROM nk) ORDER BY wk.id", .expected = &.{ 2, 4 } },
        .{ .sql = "SELECT nk.id FROM nk WHERE nk.d10 IN (SELECT d0 FROM wk) ORDER BY nk.id", .expected = &.{ 2, 4 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.d5 = nk.d20 ORDER BY p", .expected = &.{25} },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.d5 < nk.d20 ORDER BY p", .expected = &.{ 23, 31, 32, 33, 35, 41, 42, 43, 45 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.d5 >= nk.d20 ORDER BY p", .expected = &.{ 11, 12, 13, 14, 15, 21, 22, 24, 25, 34, 44 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM wk JOIN nk ON wk.d5 <=> nk.d20 ORDER BY p", .expected = &.{ 25, 56 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM nk JOIN wk ON nk.d20 = wk.d5 ORDER BY p", .expected = &.{25} },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM nk JOIN wk ON nk.d20 > wk.d5 ORDER BY p", .expected = &.{ 23, 31, 32, 33, 35, 41, 42, 43, 45 } },
        .{ .sql = "SELECT wk.id * 10 + nk.id AS p FROM nk JOIN wk ON nk.d20 <= wk.d5 ORDER BY p", .expected = &.{ 11, 12, 13, 14, 15, 21, 22, 24, 25, 34, 44 } },
        .{ .sql = "SELECT wk.id * 10 + COALESCE(nk.id, 0) AS p FROM wk LEFT JOIN nk ON wk.d5 = nk.d20 ORDER BY p", .expected = &.{ 10, 25, 30, 40, 50 } },
        .{ .sql = "SELECT wk.id FROM wk WHERE wk.d5 IN (SELECT d20 FROM nk) ORDER BY wk.id", .expected = &.{2} },
        .{ .sql = "SELECT nk.id FROM nk WHERE nk.d20 IN (SELECT d5 FROM wk) ORDER BY nk.id", .expected = &.{5} },
        // StarRocks reads these pairs as DOUBLE, where 2.5 meets
        // 2.50000000000000000001; thinDB compares them exactly, in a join as
        // in WHERE.
        .{ .sql = "SELECT wk.id * 10 + ek.id AS p FROM wk JOIN ek ON wk.li = ek.d10 ORDER BY p", .expected = &.{43} },
        .{ .sql = "SELECT wk.id * 10 + ek.id AS p FROM wk JOIN ek ON wk.li < ek.d10 ORDER BY p", .expected = &.{ 21, 22, 23, 31, 32, 33, 42 } },
        .{ .sql = "SELECT wk.id * 10 + ek.id AS p FROM wk JOIN ek ON wk.li <= ek.d10 ORDER BY p", .expected = &.{ 21, 22, 23, 31, 32, 33, 42, 43 } },
        .{ .sql = "SELECT wk.id * 10 + ek.id AS p FROM wk JOIN ek ON wk.li > ek.d10 ORDER BY p", .expected = &.{ 11, 12, 13, 41 } },
        .{ .sql = "SELECT wk.id * 10 + ek.id AS p FROM wk JOIN ek ON wk.li >= ek.d10 ORDER BY p", .expected = &.{ 11, 12, 13, 41, 43 } },
        .{ .sql = "SELECT wk.id * 10 + ek.id AS p FROM wk JOIN ek ON wk.d0 = ek.d10 ORDER BY p", .expected = &.{43} },
        .{ .sql = "SELECT wk.id * 10 + ek.id AS p FROM wk JOIN ek ON wk.d0 < ek.d10 ORDER BY p", .expected = &.{ 21, 22, 23, 31, 32, 33, 42 } },
        .{ .sql = "SELECT wk.id * 10 + ek.id AS p FROM wk JOIN ek ON wk.d0 <= ek.d10 ORDER BY p", .expected = &.{ 21, 22, 23, 31, 32, 33, 42, 43 } },
        .{ .sql = "SELECT wk.id * 10 + ek.id AS p FROM wk JOIN ek ON wk.d0 > ek.d10 ORDER BY p", .expected = &.{ 11, 12, 13, 41 } },
        .{ .sql = "SELECT wk.id * 10 + ek.id AS p FROM wk JOIN ek ON wk.d0 >= ek.d10 ORDER BY p", .expected = &.{ 11, 12, 13, 41, 43 } },
        .{ .sql = "SELECT wk.id * 10 + ek.id AS p FROM wk JOIN ek ON wk.d5 = ek.d20 ORDER BY p", .expected = &.{43} },
        .{ .sql = "SELECT wk.id * 10 + ek.id AS p FROM wk JOIN ek ON wk.d5 < ek.d20 ORDER BY p", .expected = &.{ 21, 31, 32, 33, 41, 42 } },
        .{ .sql = "SELECT wk.id * 10 + ek.id AS p FROM wk JOIN ek ON wk.d5 <= ek.d20 ORDER BY p", .expected = &.{ 21, 31, 32, 33, 41, 42, 43 } },
        .{ .sql = "SELECT wk.id * 10 + ek.id AS p FROM wk JOIN ek ON wk.d5 > ek.d20 ORDER BY p", .expected = &.{ 11, 12, 13, 22, 23 } },
        .{ .sql = "SELECT wk.id * 10 + ek.id AS p FROM wk JOIN ek ON wk.d5 >= ek.d20 ORDER BY p", .expected = &.{ 11, 12, 13, 22, 23, 43 } },
    });
}

test "comparison: a subquery's DATE never meets a number" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setupMixed(allocator, db);

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

fn expectInvalidTemporal(allocator: std.mem.Allocator, db: *thindb.Database, statements: []const []const u8) !void {
    for (statements) |sql| {
        std.testing.expectError(error.InvalidTemporalLiteral, helpers.execCtx(allocator, db, sql)) catch |err| {
            std.debug.print("expected InvalidTemporalLiteral: {s}\n", .{sql});
            return err;
        };
    }
}

test "comparison: a string constant no DATE or DATETIME reads fails the statement" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setupMixed(allocator, db);

    // MySQL raises 1525 for each of these; BETWEEN and IN lists of several
    // values only warn there, and fail here too.
    const statements = [_][]const u8{
        "SELECT id FROM cm WHERE d = ''",
        "SELECT id FROM cm WHERE d = '   '",
        "SELECT id FROM cm WHERE d = 'abc'",
        "SELECT id FROM cm WHERE ts = 'abc'",
        "SELECT id FROM cm WHERE d <> 'abc'",
        "SELECT id FROM cm WHERE d != '2024-02-30'",
        "SELECT id FROM cm WHERE d < '2024-09-31'",
        "SELECT id FROM cm WHERE d <= '2024-13-01'",
        "SELECT id FROM cm WHERE d > '2024-03'",
        "SELECT id FROM cm WHERE d >= '0000-00-00'",
        "SELECT id FROM cm WHERE d = '2024'",
        "SELECT id FROM cm WHERE ts > '2024-03-05 25:00:00'",
        "SELECT id FROM cm WHERE ts < '2024-03-05 10:60:00'",
        "SELECT id FROM cm WHERE d <=> 'abc'",
        "SELECT id FROM cm WHERE 'abc' = d",
        "SELECT id FROM cm WHERE NOT (d = 'abc')",
        "SELECT id FROM cm WHERE d = 'abc' OR id = 1",
        "SELECT id FROM cm WHERE d = 'abc' AND id < 0",
        "SELECT id FROM cm WHERE d BETWEEN '2024-03-01' AND '2024-03-32'",
        "SELECT id FROM cm WHERE d NOT BETWEEN 'abc' AND '2024-03-31'",
        "SELECT id FROM cm WHERE d IN ('abc')",
        "SELECT id FROM cm WHERE d IN ('2024-03-05', 'abc')",
        "SELECT id FROM cm WHERE d NOT IN ('2024-03-05', 'abc')",
        "SELECT id FROM cm WHERE (d, id) = ('abc', 1)",
        "SELECT id, CASE WHEN d = 'abc' THEN 1 ELSE 0 END AS c FROM cm",
        "SELECT id, CASE d WHEN 'abc' THEN 1 ELSE 0 END AS c FROM cm",
        "SELECT d FROM cm GROUP BY d HAVING d = 'abc'",
        "SELECT MAX(d) AS m FROM cm HAVING MAX(d) < '2024-02-30'",
        "SELECT cm.id FROM cm JOIN cm AS o ON o.id = cm.id AND cm.d = 'abc'",
        "SELECT cm.id FROM cm LEFT JOIN cm AS o ON o.id = cm.id AND o.ts = 'abc'",
        "SELECT id FROM cm WHERE DATE(ts) = 'abc'",
        "SELECT id FROM cm WHERE d = CONCAT('ab', 'c')",
        "SELECT id FROM cm WHERE d = (SELECT 'abc')",
        "SELECT id FROM cm WHERE d = (SELECT ds FROM cm WHERE id = 3)",
        "SELECT COUNT(*) FROM cm WHERE d = 'abc'",
        "DELETE FROM cm WHERE d = 'abc'",
        "UPDATE cm SET i = 0 WHERE ts < '2024-02-30'",
    };
    try expectInvalidTemporal(allocator, db, &statements);
    try (try db.openTable("cm", .{})).flush();
    try expectInvalidTemporal(allocator, db, &statements);
    try expectCases(allocator, db, &.{
        .{ .sql = "SELECT id FROM cm WHERE i <> 0 ORDER BY id", .expected = &.{ 1, 2, 3 } },
    });
}

fn countApiRows(allocator: std.mem.Allocator, t: *thindb.Table, expr: thindb.PredicateExpr) !usize {
    var base = try thindb.scan(allocator, t);
    var q = try base.filter(expr);
    defer q.deinit();
    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    return rows;
}

test "comparison: an API value no DATE or DATETIME reads matches nothing" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setupMixed(allocator, db);
    const t = try db.openTable("cm", .{});

    // A value the caller passes is a parameter, not a constant the statement
    // spells: like a prepared statement's, it never fails the query.
    const ops = [_]thindb.exec.PredicateOp{ .eq, .neq, .lt, .gte };
    for (0..2) |_| {
        for ([_][]const u8{ "d", "ts" }) |col| {
            for ([_][]const u8{ "", "abc", "2024-02-30" }) |text| {
                for (ops) |op| {
                    try std.testing.expectEqual(@as(usize, 0), try countApiRows(allocator, t, thindb.leafExpr(col, op, .{ .text = text })));
                }
            }
        }
        try std.testing.expectEqual(@as(usize, 2), try countApiRows(allocator, t, thindb.leafExpr("d", .gte, .{ .text = "2024-3-1" })));
        try t.flush();
    }
}

test "comparison: text meets a DATE or DATETIME the way MySQL reads it" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setupMixed(allocator, db);
    try helpers.exec(allocator, db, "CREATE TABLE tx (id BIGINT PRIMARY KEY, s VARCHAR(30))");
    try helpers.exec(allocator, db,
        \\INSERT INTO tx VALUES (1, '2024-3-5'), (2, '20240306'), (3, 'junk'), (4, ''),
        \\  (5, '2024-03-05 25:00:00'), (6, '2024-03-05 10:00'), (7, NULL)
    );

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{ "cm", "tx" }, &.{
        .{ .sql = "SELECT id FROM cm WHERE d = '20240305' ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE d = '240305' ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE d = '2024-3-5' ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE d = '2024/03/05' ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE d = ' 2024-03-05 ' ORDER BY id", .expected = &.{1} },
        // MySQL reads the date and drops the rest with a warning.
        .{ .sql = "SELECT id FROM cm WHERE d = '2024-03-05x' ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE ts = '2024-03-05 10:00' ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE ts = '2024-03-05 10' ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE ts = '2024-03-05T10:00:00' ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE ts > '2024-03-05 9:5:3' ORDER BY id", .expected = &.{ 2, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE d < '2024-03-05 10:00' ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE d BETWEEN '2024-3-1' AND '2024-03-31' ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM cm WHERE d IN ('2024-3-5', '20240306') ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM cm WHERE d NOT IN ('2024-3-5') ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE CASE WHEN d = '2024-3-6' THEN 1 ELSE 0 END = 1 ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE d = NULL ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM cm WHERE d <=> NULL ORDER BY id", .expected = &.{3} },
        .{ .sql = "SELECT id FROM cm WHERE d IN ('2024-03-05', NULL) ORDER BY id", .expected = &.{1} },
        // Only a constant raises: rows of a text column, a subquery's rows and
        // a number never do.
        .{ .sql = "SELECT id FROM cm WHERE s = 'abc' ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM cm WHERE i = 'abc' ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM cm WHERE ds = d ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE ds > '2024-03-06' ORDER BY id", .expected = &.{ 2, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE d IN (SELECT ds FROM cm) ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE d NOT IN (SELECT ds FROM cm) ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE d = CAST('abc' AS DATE) ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT cm.id * 10 + tx.id AS p FROM cm, tx WHERE tx.s = cm.d ORDER BY p", .expected = &.{ 11, 22 } },
        .{ .sql = "SELECT cm.id * 10 + tx.id AS p FROM cm, tx WHERE tx.s = cm.ts ORDER BY p", .expected = &.{ 11, 26 } },
        .{ .sql = "SELECT cm.id * 10 + tx.id AS p FROM cm JOIN tx ON tx.s = cm.d ORDER BY p", .expected = &.{ 11, 22 } },
        .{ .sql = "SELECT cm.id * 10 + tx.id AS p FROM cm JOIN tx ON tx.s = cm.ts ORDER BY p", .expected = &.{ 11, 26 } },
    });
}

test "comparison: a number meets a DATE or DATETIME the way MySQL reads it" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE nd (id BIGINT PRIMARY KEY, d DATE, ts DATETIME(6), t0 DATETIME, g INT, s SMALLINT)");
    try helpers.exec(allocator, db,
        \\INSERT INTO nd VALUES
        \\  (1, '2026-09-26', '2026-09-26 10:05:03.5', '2026-09-26 10:05:03', 1, 5),
        \\  (2, '2026-09-27', '2026-09-27 00:00:00', '2026-09-27 23:59:59', 1, 6),
        \\  (3, NULL, NULL, NULL, 2, NULL),
        \\  (4, '2001-01-01', '2001-01-01 00:00:00', '2001-01-01 00:00:00', 2, 7),
        \\  (5, '1999-12-31', '1999-12-31 23:59:59', '1999-12-31 23:59:59', 2, 8)
    );

    // Each answer is MySQL 8.4's.
    try expectCasesBeforeAndAfterFlush(allocator, db, &.{"nd"}, &.{
        .{ .sql = "SELECT id FROM nd WHERE d = 20260926 ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM nd WHERE d = 260926 ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM nd WHERE d = 991231 ORDER BY id", .expected = &.{5} },
        .{ .sql = "SELECT id FROM nd WHERE d = 10101 ORDER BY id", .expected = &.{4} },
        // A number no datetime reads compares with each row's own number.
        .{ .sql = "SELECT id FROM nd WHERE d = 2026 ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM nd WHERE d > 2026 ORDER BY id", .expected = &.{ 1, 2, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE d <> 2026 ORDER BY id", .expected = &.{ 1, 2, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE d > 0 ORDER BY id", .expected = &.{ 1, 2, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE d > -20260926 ORDER BY id", .expected = &.{ 1, 2, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE d = TRUE ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM nd WHERE d > 20260926.5 ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM nd WHERE d = 20260926.9 ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM nd WHERE d = 2.0260926e7 ORDER BY id", .expected = &.{1} },
        // A DATE column meets a datetime number by its day.
        .{ .sql = "SELECT id FROM nd WHERE d = 20260926000001 ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM nd WHERE d < 20260926000001 ORDER BY id", .expected = &.{ 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE d > 20260926000001 ORDER BY id", .expected = &.{2} },
        // A zero month or day, or a day past its month's end, lies before the next real day.
        .{ .sql = "SELECT id FROM nd WHERE d = 20260900 ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM nd WHERE d > 20260900 ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM nd WHERE d > 260900 ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM nd WHERE d < 20260230 ORDER BY id", .expected = &.{ 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE d > 20260931 ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM nd WHERE d >= 20010229 ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM nd WHERE d >= 20260000 ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM nd WHERE d < 20261399 ORDER BY id", .expected = &.{ 1, 2, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE d = 20261399 ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM nd WHERE d > 10000100 ORDER BY id", .expected = &.{ 1, 2, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE d > 1e10 ORDER BY id", .expected = &.{ 1, 2, 4 } },
        .{ .sql = "SELECT id FROM nd WHERE d < 99991232 ORDER BY id", .expected = &.{ 1, 2, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE d > 99991231240000 ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM nd WHERE t0 = 20260926100503 ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM nd WHERE t0 = 260926100503 ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM nd WHERE t0 > 20260926 ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM nd WHERE t0 = 20010101 ORDER BY id", .expected = &.{4} },
        .{ .sql = "SELECT id FROM nd WHERE t0 > 20260926250000 ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM nd WHERE t0 >= 20260926100560 ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM nd WHERE t0 > 20260900000000 ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM nd WHERE t0 < 20270000000000 ORDER BY id", .expected = &.{ 1, 2, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE t0 > 2026092610050 ORDER BY id", .expected = &.{ 1, 2, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE t0 < 691231235959 ORDER BY id", .expected = &.{ 1, 2, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE ts = 20260926100503.5 ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM nd WHERE ts > 20260926100503.4 ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM nd WHERE ts < 20260926100503.6 ORDER BY id", .expected = &.{ 1, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE ts = 20260926100503 ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM nd WHERE t0 IN (20260926100503, 20010101) ORDER BY id", .expected = &.{ 1, 4 } },
        .{ .sql = "SELECT id FROM nd WHERE d IN (20260900, 20260927) ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM nd WHERE d NOT IN (20260926, 20260927) ORDER BY id", .expected = &.{ 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE t0 NOT IN (20260926100503, 19991231235959) ORDER BY id", .expected = &.{ 2, 4 } },
        .{ .sql = "SELECT id FROM nd WHERE d BETWEEN 20010101 AND 20260926 ORDER BY id", .expected = &.{ 1, 4 } },
        .{ .sql = "SELECT id FROM nd WHERE d BETWEEN 20260926000001 AND 20270101 ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM nd WHERE d <=> 20260926 ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM nd WHERE 20260926 < d ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM nd WHERE NOT (d > 20260900) ORDER BY id", .expected = &.{ 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE d = 20260926 + 1 ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM nd WHERE d = 20260926 OR t0 < 20000101 ORDER BY id", .expected = &.{ 1, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE IF(d = 260926, 1, 0) = 1 ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM nd WHERE CASE WHEN d > 20260230 THEN 1 ELSE 0 END = 1 ORDER BY id", .expected = &.{ 1, 2 } },
        // A number column meets a DATE or DATETIME as its number.
        .{ .sql = "SELECT id FROM nd WHERE d = g ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM nd WHERE d > g ORDER BY id", .expected = &.{ 1, 2, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE g < t0 ORDER BY id", .expected = &.{ 1, 2, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE s < d ORDER BY id", .expected = &.{ 1, 2, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE g < DATE '2026-09-26' ORDER BY id", .expected = &.{ 1, 2, 3, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE s = DATE '2026-09-26' ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM nd WHERE g < TIMESTAMP '2026-09-26 10:05:03.5' ORDER BY id", .expected = &.{ 1, 2, 3, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE g IN (DATE '2026-09-26', 1) ORDER BY id", .expected = &.{ 1, 2 } },
        // NULLIF compares as = does.
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(d, 20260926) IS NULL ORDER BY id", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(d, 260926) IS NULL ORDER BY id", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(d, 20260926000001) IS NULL ORDER BY id", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(d, 20260926.0) IS NULL ORDER BY id", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(d, 20260900) IS NULL ORDER BY id", .expected = &.{3} },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(d, 2026) IS NULL ORDER BY id", .expected = &.{3} },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(d, 991231) IS NULL ORDER BY id", .expected = &.{ 3, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(ts, 20260926100503.5) IS NULL ORDER BY id", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(ts, 20260926100503) IS NULL ORDER BY id", .expected = &.{3} },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(ts, 20260927) IS NULL ORDER BY id", .expected = &.{ 2, 3 } },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(t0, 20260927235959) IS NULL ORDER BY id", .expected = &.{ 2, 3 } },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(t0, 1e13) IS NULL ORDER BY id", .expected = &.{3} },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(d, 20260926) = '2026-09-27' ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(g + 20260925, DATE '2026-09-26') IS NULL ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(s + 20260920, DATE '2026-09-26') IS NULL ORDER BY id", .expected = &.{ 2, 3 } },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(g * 1000000 + 20260926100502, TIMESTAMP '2026-09-26 10:05:03') IS NULL ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(DATE '2026-09-26', 20260926) IS NULL ORDER BY id", .expected = &.{ 1, 2, 3, 4, 5 } },
        .{ .sql = "SELECT id FROM nd WHERE NULLIF(20260926, DATE '2026-09-26') IS NULL ORDER BY id", .expected = &.{ 1, 2, 3, 4, 5 } },
    });
}

test "comparison: NULLIF reads a text constant against a DATE or DATETIME as = does (issue #326)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setupMixed(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{"cm"}, &.{
        .{ .sql = "SELECT id FROM cm WHERE NULLIF(d, '2024-03-05') IS NULL ORDER BY id", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE NULLIF(d, '2024-3-5') IS NULL ORDER BY id", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE NULLIF(d, '20240305') IS NULL ORDER BY id", .expected = &.{ 1, 3 } },
        .{ .sql = "SELECT id FROM cm WHERE NULLIF(d, '2024-03-06 00:00:00') IS NULL ORDER BY id", .expected = &.{ 2, 3 } },
        // A time of day no DATE has never equals one.
        .{ .sql = "SELECT id FROM cm WHERE NULLIF(d, '2024-03-05 10:00:00') IS NULL ORDER BY id", .expected = &.{3} },
        .{ .sql = "SELECT id FROM cm WHERE NULLIF(ts, '2024-3-5 10:00') IS NULL ORDER BY id", .expected = &.{2} },
        .{ .sql = "SELECT id FROM cm WHERE NULLIF(ts, '2024-03-05') IS NULL ORDER BY id", .expected = &.{1} },
        .{ .sql = "SELECT id FROM cm WHERE NULLIF(DATE(ts), '2024-3-5') IS NULL ORDER BY id", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT id FROM cm WHERE NULLIF('2024-3-5', d) IS NULL ORDER BY id", .expected = &.{1} },
        // The result is the DATE, so text compared with it reads as a date.
        .{ .sql = "SELECT id FROM cm WHERE NULLIF(d, '2024-3-5') = '2024-3-6' ORDER BY id", .expected = &.{2} },
        // Text meets text as text.
        .{ .sql = "SELECT id FROM cm WHERE NULLIF(ds, '2024-3-5') IS NULL ORDER BY id", .expected = &.{} },
        .{ .sql = "SELECT id FROM cm WHERE NULLIF(s, 'apple') IS NULL ORDER BY id", .expected = &.{1} },
    });

    const statements = [_][]const u8{
        "SELECT id, NULLIF(d, 'abc') AS x FROM cm",
        "SELECT id, NULLIF(d, '') AS x FROM cm",
        "SELECT id, NULLIF(ts, '2024-02-30') AS x FROM cm",
        "SELECT id, NULLIF('abc', d) AS x FROM cm",
        "SELECT id FROM cm WHERE NULLIF(d, 'abc') IS NULL",
        "SELECT NULLIF(DATE '2024-03-05', 'abc') AS x",
    };
    try expectInvalidTemporal(allocator, db, &statements);
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
