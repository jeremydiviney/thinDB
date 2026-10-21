//! STRUCT type end-to-end: STRUCT<...> columns store, flush and reopen; the
//! row / struct / named_struct constructors, field access and CAST, each
//! checked by its result's StarRocks text.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSql = helpers.runSql;
const expectTexts = helpers.expectTexts;
const expectText = helpers.expectText;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE sc (id INT PRIMARY KEY, s STRUCT<a INT, b STRING>, n STRUCT<id INT, tags ARRAY<INT>, m MAP<STRING, INT>, `inner` STRUCT<x DOUBLE, y STRING>>)");
    try exec(allocator, db,
        \\INSERT INTO sc VALUES
        \\ (1, row(1, 'x'), named_struct('id', 10, 'tags', [1, 2], 'm', map{'k': 1}, 'inner', row(1.5, 'p'))),
        \\ (2, row(NULL, 'y'), NULL),
        \\ (3, NULL, row(30, [], map(), NULL)),
        \\ (4, row(2, NULL), row(40, NULL, NULL, row(NULL, 'q')))
    );
    return db;
}

const S_TEXTS = [_]?[]const u8{ "{\"a\":1,\"b\":\"x\"}", "{\"a\":null,\"b\":\"y\"}", null, "{\"a\":2,\"b\":null}" };
const N_TEXTS = [_]?[]const u8{
    "{\"id\":10,\"tags\":[1,2],\"m\":{\"k\":1},\"inner\":{\"x\":1.5,\"y\":\"p\"}}",
    null,
    "{\"id\":30,\"tags\":[],\"m\":{},\"inner\":null}",
    "{\"id\":40,\"tags\":null,\"m\":null,\"inner\":{\"x\":null,\"y\":\"q\"}}",
};

test "STRUCT: columns store, flush and reopen" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        var db = try setup(allocator, io, tmp.dir);
        defer db.close();
        try expectTexts(allocator, db, "SELECT s FROM sc ORDER BY id", &S_TEXTS);
        try expectTexts(allocator, db, "SELECT n FROM sc ORDER BY id", &N_TEXTS);
        const t = try db.openTable("sc", .{});
        try t.flush();
    }
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try expectTexts(allocator, db, "SELECT s FROM sc ORDER BY id", &S_TEXTS);
    try expectTexts(allocator, db, "SELECT n FROM sc ORDER BY id", &N_TEXTS);
}

test "STRUCT: constructors and CAST" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectText(allocator, db, "SELECT row(1, 'a', NULL)", "{\"col1\":1,\"col2\":\"a\",\"col3\":null}");
    try expectText(allocator, db, "SELECT struct(1.5, [1, 2])", "{\"col1\":1.5,\"col2\":[1,2]}");
    try expectText(allocator, db, "SELECT named_struct('x', 1, 'Y z', map{'k': 'v'})", "{\"x\":1,\"Y z\":{\"k\":\"v\"}}");
    try expectText(allocator, db, "SELECT [row(1, 'a'), row(2, 'b')]", "[{\"col1\":1,\"col2\":\"a\"},{\"col1\":2,\"col2\":\"b\"}]");
    try expectText(allocator, db, "SELECT map{'k': row(1)}", "{\"k\":{\"col1\":1}}");
    try expectText(allocator, db, "SELECT row(row(1, 2), NULL)", "{\"col1\":{\"col1\":1,\"col2\":2},\"col2\":null}");
    try expectText(allocator, db, "SELECT CAST('{\"a\": 1, \"b\": \"x\"}' AS STRUCT<a INT, b STRING>)", "{\"a\":1,\"b\":\"x\"}");
    try expectText(allocator, db, "SELECT CAST('{\"b\": [1]}' AS STRUCT<a INT, b ARRAY<INT>>)", "{\"a\":null,\"b\":[1]}");
    try expectText(allocator, db, "SELECT CAST('[1]' AS STRUCT<a INT>)", null);
    try expectText(allocator, db, "SELECT CAST(row(1, 2) AS STRUCT<a STRING, b DOUBLE>)", "{\"a\":\"1\",\"b\":2}");
    // A BOOLEAN field casts to a nested one as NULL, as in StarRocks.
    try expectText(allocator, db, "SELECT CAST(row(1, TRUE) AS STRUCT<a INT, b MAP<INT, INT>>)", "{\"a\":1,\"b\":null}");
    try expectTexts(allocator, db, "SELECT CAST(s AS STRING) FROM sc ORDER BY id", &S_TEXTS);
    try expectTexts(allocator, db, "SELECT CAST(s AS STRUCT<c BIGINT, d VARCHAR(4)>) FROM sc ORDER BY id", &.{ "{\"c\":1,\"d\":\"x\"}", "{\"c\":null,\"d\":\"y\"}", null, "{\"c\":2,\"d\":null}" });
    inline for (.{ "SELECT named_struct('a')", "SELECT named_struct('a', 1, 'A', 2)", "SELECT named_struct(1, 2)" }) |sql| {
        try std.testing.expectError(error.SqlExpectedValue, runSql(allocator, db, sql));
    }
}

test "STRUCT: field access" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectTexts(allocator, db, "SELECT s.a FROM sc ORDER BY id", &.{ "1", null, null, "2" });
    try expectTexts(allocator, db, "SELECT S.B FROM sc ORDER BY id", &.{ "x", "y", null, null });
    try expectTexts(allocator, db, "SELECT sc.s.b FROM sc ORDER BY id", &.{ "x", "y", null, null });
    try expectTexts(allocator, db, "SELECT x.s.a FROM sc x ORDER BY id", &.{ "1", null, null, "2" });
    try expectTexts(allocator, db, "SELECT n.inner.y FROM sc ORDER BY id", &.{ "p", null, null, "q" });
    try expectTexts(allocator, db, "SELECT sc.n.inner.x FROM sc ORDER BY id", &.{ "1.5", null, null, null });
    // The database and schema a bare table name resolves into qualify it.
    try expectTexts(allocator, db, "SELECT main.sc.s.b FROM sc ORDER BY id", &.{ "x", "y", null, null });
    try expectTexts(allocator, db, "SELECT MAIN.SC.n.inner.y FROM sc ORDER BY id", &.{ "p", null, null, "q" });
    try expectTexts(allocator, db, "SELECT main.public.sc.s.a FROM sc ORDER BY id", &.{ "1", null, null, "2" });
    try expectTexts(allocator, db, "SELECT main.x.s.a FROM sc x ORDER BY id", &.{ "1", null, null, "2" });
    // Ahead of no table name it is the struct column it names.
    try exec(allocator, db, "CREATE TABLE nm (id INT PRIMARY KEY, main STRUCT<a INT, public STRUCT<c STRUCT<d INT>>>, o STRUCT<public STRUCT<c STRUCT<d INT>>>)");
    try exec(allocator, db, "INSERT INTO nm VALUES (1, row(5, row(row(6))), row(row(row(7))))");
    try expectText(allocator, db, "SELECT main.a FROM nm", "5");
    try expectText(allocator, db, "SELECT main.public.c.d FROM nm", "6");
    try expectText(allocator, db, "SELECT o.public.c.d FROM nm", "7");
    try expectTexts(allocator, db, "SELECT n.tags[2] FROM sc ORDER BY id", &.{ "2", null, null, null });
    try expectTexts(allocator, db, "SELECT n.m['k'] FROM sc ORDER BY id", &.{ "1", null, null, null });
    try expectTexts(allocator, db, "SELECT n.inner FROM sc ORDER BY id", &.{ "{\"x\":1.5,\"y\":\"p\"}", null, null, "{\"x\":null,\"y\":\"q\"}" });
    try expectTexts(allocator, db, "SELECT s.a + 10 FROM sc ORDER BY id", &.{ "11", null, null, "12" });
    try expectTexts(allocator, db, "SELECT id FROM sc WHERE s.a > 1", &.{"4"});
    try expectTexts(allocator, db, "SELECT id FROM sc WHERE s.b = 'y' OR n.id = 30 ORDER BY id", &.{ "2", "3" });
    // A path's Compute sits under both halves of a split AND.
    try expectTexts(allocator, db, "SELECT s.b FROM sc WHERE id > 1 AND abs(id) < 4 ORDER BY id", &.{ "y", null });
    try expectTexts(allocator, db, "SELECT id FROM sc WHERE s.a >= 1 AND abs(id) < 9 ORDER BY id", &.{ "1", "4" });
    try expectTexts(allocator, db, "SELECT n.inner.y FROM sc WHERE id > 1 AND abs(id) < 9 AND n.id > 30", &.{"q"});
    try expectTexts(allocator, db, "SELECT map_size(n.m), n.inner.x FROM sc WHERE id < 4 AND n.tags[1] > 0 ORDER BY id LIMIT 2", &.{"1"});
    try expectTexts(allocator, db, "SELECT max(s.a) FROM sc WHERE id > 1 AND abs(id) < 9", &.{"2"});
    try expectTexts(allocator, db, "SELECT upper(s.b) FROM sc WHERE s.b IS NOT NULL AND abs(id) > 0 GROUP BY upper(s.b) ORDER BY 1", &.{ "X", "Y" });
    try expectTexts(allocator, db, "SELECT id FROM sc WHERE s.a IS NOT NULL ORDER BY s.a DESC", &.{ "4", "1" });
    try expectTexts(allocator, db, "SELECT s.b FROM sc WHERE s.b IS NOT NULL GROUP BY s.b ORDER BY s.b", &.{ "x", "y" });
    try expectTexts(allocator, db, "SELECT max(n.id) FROM sc", &.{"40"});
    try expectTexts(allocator, db, "SELECT t.inner.x FROM (SELECT n FROM sc) t ORDER BY t.inner.x", &.{ null, null, null, "1.5" });
    try expectTexts(allocator, db, "SELECT row(id, 'k').col1 FROM sc ORDER BY id", &.{ "1", "2", "3", "4" });
    try expectText(allocator, db, "SELECT named_struct('a', 1, 'b', row(2, 3)).b.col2", "3");
    try expectText(allocator, db, "SELECT [row(1, 'a'), row(2, 'b')][2].col2", "b");
    // A constant subscript picks a field by position, from 1.
    try expectTexts(allocator, db, "SELECT s[2] FROM sc ORDER BY id", &.{ "x", "y", null, null });
    try expectText(allocator, db, "SELECT named_struct('a', 1, 'b', 'x')[1]", "1");
    try expectText(allocator, db, "SELECT named_struct('a', row(5, 6))[1].col2", "6");
    inline for (.{ "SELECT named_struct('a', 1, 'b', 'x')[3]", "SELECT named_struct('a', 1)[0]", "SELECT named_struct('a', 1).zz" }) |sql| {
        try std.testing.expectError(error.ComputeNoSuchOverload, runSql(allocator, db, sql));
    }

    var star = try runSql(allocator, db, "SELECT * FROM sc WHERE s.a = 1");
    defer star.deinit();
    try std.testing.expectEqual(@as(usize, 3), star.outputSchema().len);
    const ids = try helpers.columnText(allocator, &star);
    defer helpers.freeStrings(allocator, ids);
    try std.testing.expectEqual(@as(usize, 1), ids.len);
}

test "STRUCT: comparison, ordering, grouping and conditionals" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectTexts(allocator, db, "SELECT id FROM sc WHERE s = row(1, 'x')", &.{"1"});
    // NULL fields compare equal, as NULL array elements do.
    try expectTexts(allocator, db, "SELECT id FROM sc WHERE s = named_struct('a', 2, 'b', NULL)", &.{"4"});
    // A NULL field orders first, as a NULL array element does.
    try expectTexts(allocator, db, "SELECT id FROM sc WHERE s < row(2, 'a') ORDER BY id", &.{ "1", "2", "4" });
    try expectTexts(allocator, db, "SELECT count(*) FROM (SELECT row(id % 2) AS k FROM sc) t GROUP BY k ORDER BY 1", &.{ "2", "2" });
    // A field of a grouping key reads above the GROUP BY.
    try expectTexts(allocator, db, "SELECT s FROM sc WHERE s.a IS NOT NULL GROUP BY s ORDER BY s.a DESC", &.{ "{\"a\":2,\"b\":null}", "{\"a\":1,\"b\":\"x\"}" });
    try expectTexts(allocator, db, "SELECT max(id) FROM sc GROUP BY s ORDER BY s.b, max(id)", &.{ "3", "4", "1", "2" });
    try expectTexts(allocator, db, "SELECT max(id) FROM sc GROUP BY sc.s ORDER BY sc.s.b DESC, 1", &.{ "2", "1", "3", "4" });
    try expectTexts(allocator, db, "SELECT max(id) FROM sc GROUP BY s HAVING s.a > 1", &.{"4"});
    try expectTexts(allocator, db, "SELECT COALESCE(s, row(0, 'z')) FROM sc ORDER BY id", &.{ "{\"a\":1,\"b\":\"x\"}", "{\"a\":null,\"b\":\"y\"}", "{\"a\":0,\"b\":\"z\"}", "{\"a\":2,\"b\":null}" });
    try expectTexts(allocator, db, "SELECT IF(id > 2, s, NULL) FROM sc ORDER BY id", &.{ null, null, null, "{\"a\":2,\"b\":null}" });
}

test "STRUCT: two struct columns of different types compare and join at their common type" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE st (id INT PRIMARY KEY, s1 STRUCT<a INT, b VARCHAR(10)>, s2 STRUCT<x BIGINT, y STRING>, s3 STRUCT<a INT>, s4 STRUCT<a INT, b ARRAY<INT>>)");
    try exec(allocator, db,
        \\INSERT INTO st VALUES
        \\ (1, row(1, 'x'), row(1, 'x'), row(1), row(1, [1])),
        \\ (2, row(2, 'y'), row(3, 'y'), row(2), row(2, [2])),
        \\ (3, NULL, row(1, NULL), NULL, row(3, NULL))
    );

    try expectTexts(allocator, db, "SELECT s1 = s2 FROM st ORDER BY id", &.{ "1", "0", null });
    try expectTexts(allocator, db, "SELECT s1 < s2 FROM st ORDER BY id", &.{ "0", "1", null });
    try expectTexts(allocator, db, "SELECT u.id FROM st t JOIN st u ON t.s1 = u.s2 ORDER BY t.id, u.id", &.{"1"});
    // A NULL literal's field reads as the other side's array.
    try expectTexts(allocator, db, "SELECT s4 <=> row(3, NULL) FROM st ORDER BY id", &.{ "0", "0", "1" });
    try helpers.expectRunError(allocator, db, "SELECT id FROM st WHERE s1 = s3", error.PredicateTypeMismatch);
    try helpers.expectRunError(allocator, db, "SELECT id FROM st WHERE s1 = s4", error.PredicateTypeMismatch);
    try helpers.expectRunError(allocator, db, "SELECT t.id FROM st t JOIN st u ON t.s1 = u.s4", error.JoinKeyTypeMismatch);
}

test "STRUCT: nested paths through joins, unions and derived tables" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE sc2 (v1 BIGINT, map1 MAP<INT, MAP<INT, MAP<INT, INT>>>, st1 STRUCT<s1 INT, sm2 MAP<INT, INT>, sm3 MAP<INT, MAP<INT, INT>>>, st2 STRUCT<s1 INT, sa2 ARRAY<INT>, ss3 STRUCT<sss1 INT, sss2 STRUCT<ssss1 INT, ssss2 INT>>>)");
    try exec(allocator, db, "CREATE TABLE t1 (v1 BIGINT, v2 BIGINT, v3 BIGINT)");
    try exec(allocator, db, "INSERT INTO t1 VALUES (1, 2, 3), (2, 2, 2), (3, 3, 3)");
    try exec(allocator, db,
        \\INSERT INTO sc2 VALUES
        \\ (1, map{1: map{10: map{100: 101}}}, row(1, map{10: 101}, map{10: map{100: 101}}), row(1, [1, 10, 101], row(1, row(10, 11)))),
        \\ (2, map{2: map{20: map{200: 202}}}, row(2, map{20: 202}, map{20: map{200: 202}}), row(2, [2, 20, 202], row(2, row(20, 22)))),
        \\ (3, map{3: map{30: map{300: 303}}}, row(3, map{30: 303}, map{30: map{300: 303}}), row(3, [3, 30, 303], row(3, row(30, 33))))
    );
    try expectTexts(allocator, db, "SELECT st2.sa2 FROM sc2 ORDER BY st1.s1", &.{ "[1,10,101]", "[2,20,202]", "[3,30,303]" });
    try expectTexts(allocator, db, "SELECT st1.sm2[10] FROM sc2 ORDER BY v1", &.{ "101", null, null });
    try expectTexts(allocator, db, "SELECT map_size(map1[3][30]) FROM sc2 ORDER BY v1", &.{ null, null, "1" });
    try expectTexts(allocator, db, "SELECT st2.sa2[1] FROM sc2 ORDER BY v1", &.{ "1", "2", "3" });
    try expectTexts(allocator, db, "SELECT st2.ss3.sss1 FROM sc2 ORDER BY v1", &.{ "1", "2", "3" });
    try expectTexts(allocator, db, "SELECT st2.ss3.sss2.ssss2 FROM sc2 ORDER BY v1", &.{ "11", "22", "33" });
    try expectTexts(allocator, db, "SELECT st2.ss3 FROM sc2 ORDER BY st2.ss3.sss2.ssss2 DESC", &.{
        "{\"sss1\":3,\"sss2\":{\"ssss1\":30,\"ssss2\":33}}",
        "{\"sss1\":2,\"sss2\":{\"ssss1\":20,\"ssss2\":22}}",
        "{\"sss1\":1,\"sss2\":{\"ssss1\":10,\"ssss2\":11}}",
    });
    try expectTexts(allocator, db, "SELECT st1.sm2 FROM t1 JOIN sc2 ON t1.v2 = sc2.v1 ORDER BY t1.v1", &.{ "{20:202}", "{20:202}", "{30:303}" });
    try expectTexts(allocator, db, "SELECT m FROM (SELECT 1 AS side, x1.v1 AS k, x1.st1.sm2 AS m FROM sc2 x1 UNION ALL SELECT 2, x2.v1, x2.st1.sm3[10] FROM sc2 x2) u ORDER BY side, k", &.{ "{10:101}", "{20:202}", "{30:303}", "{100:101}", null, null });
    try expectTexts(allocator, db, "SELECT x.st2.ss3.sss1 FROM (SELECT * FROM sc2 UNION ALL SELECT * FROM sc2) x ORDER BY 1", &.{ "1", "1", "2", "2", "3", "3" });
    try expectTexts(allocator, db, "SELECT row(map1).col1 FROM sc2 ORDER BY v1", &.{ "{1:{10:{100:101}}}", "{2:{20:{200:202}}}", "{3:{30:{300:303}}}" });
    try expectTexts(allocator, db, "SELECT named_struct('a', st1, 'b', st2).b.ss3.sss1 FROM sc2 ORDER BY v1", &.{ "1", "2", "3" });
    try expectText(allocator, db, "SELECT CAST(row(1, 2, 3) AS STRUCT<a DOUBLE, b STRING, c BIGINT>)", "{\"a\":1,\"b\":\"2\",\"c\":3}");
    try expectTexts(allocator, db, "WITH input AS (SELECT struct([1, 2, 3], [4, 5, 6]) AS s UNION ALL SELECT struct([5, 6, 7], [6, 7]) AS s) SELECT s.col1 FROM input ORDER BY 1", &.{ "[1,2,3]", "[5,6,7]" });
    try expectTexts(allocator, db, "WITH input AS (SELECT struct([1, 2, 3], [4, 5, 6]) AS s UNION ALL SELECT NULL AS s UNION ALL SELECT struct([5, 6, 7], [6, 7]) AS s) SELECT s.col1 FROM input ORDER BY 1", &.{ null, "[1,2,3]", "[5,6,7]" });
    try expectTexts(allocator, db, "SELECT s.col2 FROM (SELECT struct(1, 'a') AS s) x", &.{"a"});
}
