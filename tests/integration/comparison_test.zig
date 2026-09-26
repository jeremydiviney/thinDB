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

test "comparison: kinds that never compare are rejected" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setupMixed(allocator, db);

    try helpers.expectRunError(allocator, db, "SELECT id FROM cm WHERE i = d", error.PredicateTypeMismatch);
    try helpers.expectRunError(allocator, db, "SELECT id FROM cm WHERE ts > 5", error.PredicateTypeMismatch);
}
