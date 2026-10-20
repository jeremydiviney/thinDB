//! ARRAY type end-to-end: ARRAY<T> columns store, flush and reopen; array
//! literals, subscripts and CAST; and the array functions, each checked by
//! its result's StarRocks text.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const expectTexts = helpers.expectTexts;
const expectText = helpers.expectText;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, a ARRAY<INT>, s ARRAY<VARCHAR(10)>, n ARRAY<ARRAY<BIGINT>>, d ARRAY<DECIMAL(10,2)>)");
    try exec(allocator, db,
        \\INSERT INTO t VALUES
        \\ (1, [3, 1, 2], ['b', 'a', NULL], [[1, 2], [3]], [1.5, 2.25]),
        \\ (2, [], [], [], []),
        \\ (3, NULL, NULL, NULL, NULL),
        \\ (4, [5, NULL, 5, 4], ['x'], [NULL, [4]], [-1])
    );
    return db;
}

test "ARRAY: columns store, flush and reopen" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        var db = try setup(allocator, io, tmp.dir);
        defer db.close();
        try expectTexts(allocator, db, "SELECT a FROM t ORDER BY id", &.{ "[3,1,2]", "[]", null, "[5,null,5,4]" });
        const t = try db.openTable("t", .{});
        try t.flush();
    }
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try expectTexts(allocator, db, "SELECT a FROM t ORDER BY id", &.{ "[3,1,2]", "[]", null, "[5,null,5,4]" });
    try expectTexts(allocator, db, "SELECT s FROM t ORDER BY id", &.{ "[\"b\",\"a\",null]", "[]", null, "[\"x\"]" });
    try expectTexts(allocator, db, "SELECT n FROM t ORDER BY id", &.{ "[[1,2],[3]]", "[]", null, "[null,[4]]" });
    try expectTexts(allocator, db, "SELECT d FROM t ORDER BY id", &.{ "[1.50,2.25]", "[]", null, "[-1.00]" });
}

test "ARRAY: literals, subscripts and CAST" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectText(allocator, db, "SELECT [1, 2, NULL]", "[1,2,null]");
    try expectText(allocator, db, "SELECT ['a', 'bc']", "[\"a\",\"bc\"]");
    try expectText(allocator, db, "SELECT [1, 2.5]", "[1.0,2.5]");
    try expectText(allocator, db, "SELECT ARRAY[1, 2]", "[1,2]");
    try expectText(allocator, db, "SELECT ARRAY<INT>[]", "[]");
    try expectText(allocator, db, "SELECT [[1], [2, 3]]", "[[1],[2,3]]");
    try expectText(allocator, db, "SELECT [1, 2, 3][2]", "2");
    try expectText(allocator, db, "SELECT [1, 2, 3][4]", null);
    try expectText(allocator, db, "SELECT [[1], [2, 3]][2][1]", "2");
    try expectTexts(allocator, db, "SELECT a[1] FROM t ORDER BY id", &.{ "3", null, null, "5" });
    try expectText(allocator, db, "SELECT CAST('[1, 2, 3]' AS ARRAY<INT>)", "[1,2,3]");
    try expectText(allocator, db, "SELECT CAST('[\"a\", null]' AS ARRAY<STRING>)", "[\"a\",null]");
    try expectText(allocator, db, "SELECT CAST('null' AS ARRAY<INT>)", null);
    try expectText(allocator, db, "SELECT CAST([1, 2] AS ARRAY<STRING>)", "[\"1\",\"2\"]");
    try expectText(allocator, db, "SELECT CAST(['1', 'x'] AS ARRAY<INT>)", "[1,null]");
    try expectText(allocator, db, "SELECT CAST([1, 2] AS STRING)", "[1,2]");
    try expectText(allocator, db, "SELECT CAST(CAST('[[1],[2,3]]' AS ARRAY<ARRAY<INT>>) AS ARRAY<ARRAY<STRING>>)", "[[\"1\"],[\"2\",\"3\"]]");
}

test "ARRAY: functions" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectTexts(allocator, db, "SELECT array_length(a) FROM t ORDER BY id", &.{ "3", "0", null, "4" });
    try expectTexts(allocator, db, "SELECT cardinality(n) FROM t ORDER BY id", &.{ "2", "0", null, "2" });
    try expectText(allocator, db, "SELECT array_length(NULL)", null);
    try expectTexts(allocator, db, "SELECT array_contains(a, 5) FROM t ORDER BY id", &.{ "0", "0", null, "1" });
    try expectTexts(allocator, db, "SELECT array_contains(a, NULL) FROM t ORDER BY id", &.{ "0", "0", null, "1" });
    try expectTexts(allocator, db, "SELECT array_position(a, 2) FROM t ORDER BY id", &.{ "3", "0", null, "0" });
    try expectTexts(allocator, db, "SELECT array_sort(a) FROM t ORDER BY id", &.{ "[1,2,3]", "[]", null, "[null,4,5,5]" });
    try expectTexts(allocator, db, "SELECT array_distinct(a) FROM t ORDER BY id", &.{ "[3,1,2]", "[]", null, "[5,null,4]" });
    try expectTexts(allocator, db, "SELECT reverse(s) FROM t ORDER BY id", &.{ "[null,\"a\",\"b\"]", "[]", null, "[\"x\"]" });
    try expectTexts(allocator, db, "SELECT array_slice(a, 2) FROM t ORDER BY id", &.{ "[1,2]", "[]", null, "[null,5,4]" });
    try expectText(allocator, db, "SELECT array_slice([1, 2, 3, 4], -2, 1)", "[3]");
    try expectText(allocator, db, "SELECT array_slice([1, 2, 3, 4], 10)", "[]");
    try expectTexts(allocator, db, "SELECT array_concat(a, [9]) FROM t ORDER BY id", &.{ "[3,1,2,9]", "[9]", null, "[5,null,5,4,9]" });
    try expectText(allocator, db, "SELECT array_concat(['a'], [1])", "[\"a\",\"1\"]");
    try expectTexts(allocator, db, "SELECT array_append(a, NULL) FROM t ORDER BY id", &.{ "[3,1,2,null]", "[null]", null, "[5,null,5,4,null]" });
    try expectTexts(allocator, db, "SELECT array_remove(a, 5) FROM t ORDER BY id", &.{ "[3,1,2]", "[]", null, "[null,4]" });
    try expectTexts(allocator, db, "SELECT array_join(s, '-') FROM t ORDER BY id", &.{ "b-a", "", null, "x" });
    try expectText(allocator, db, "SELECT array_join(['a', NULL, 'c'], ',', '?')", "a,?,c");
    try expectTexts(allocator, db, "SELECT array_min(a) FROM t ORDER BY id", &.{ "1", null, null, "4" });
    try expectTexts(allocator, db, "SELECT array_max(a) FROM t ORDER BY id", &.{ "3", null, null, "5" });
    try expectTexts(allocator, db, "SELECT array_sum(a) FROM t ORDER BY id", &.{ "6", null, null, "14" });
    try expectText(allocator, db, "SELECT array_avg([1, 2])", "1.5");
    try expectText(allocator, db, "SELECT CAST(array_avg([1.50, 2.25]) AS STRING)", "1.87500000");
    try expectText(allocator, db, "SELECT array_difference([1, 4, NULL, 9, 10])", "[0,3,null,null,1]");
    try expectText(allocator, db, "SELECT split('a,b,,c', ',')", "[\"a\",\"b\",\"\",\"c\"]");
    try expectText(allocator, db, "SELECT array_generate(1, 4)", "[1,2,3,4]");
    try expectText(allocator, db, "SELECT array_generate(4, 1)", "[4,3,2,1]");
    try expectText(allocator, db, "SELECT array_generate(1, 7, 3)", "[1,4,7]");
    try expectText(allocator, db, "SELECT array_generate(1, NULL)", null);
    try expectText(allocator, db, "SELECT array_repeat('a', 3)", "[\"a\",\"a\",\"a\"]");
    try expectTexts(allocator, db, "SELECT array_flatten(n) FROM t ORDER BY id", &.{ "[1,2,3]", "[]", null, "[4]" });
    try expectTexts(allocator, db, "SELECT arrays_overlap(a, [2, 9]) FROM t ORDER BY id", &.{ "1", "0", null, "0" });
    try expectText(allocator, db, "SELECT array_contains_all([1, 2, 3], [3, 1])", "1");
    try expectText(allocator, db, "SELECT array_contains_all([1, 2, 3], [4])", "0");
    try expectText(allocator, db, "SELECT array_contains_seq([1, 2, 3], [2, 3])", "1");
    try expectText(allocator, db, "SELECT array_contains_seq([1, 2, 3], [3, 2])", "0");
    try expectText(allocator, db, "SELECT array_sort(array_intersect([1, 2, 2, 3], [2, 3, 4]))", "[2,3]");
}

test "ARRAY: cosine_similarity, cosine_similarity_norm and l2_distance, and their APPROX_ forms" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE v (id BIGINT PRIMARY KEY, data ARRAY<FLOAT>, i ARRAY<INT>)");
    try exec(allocator, db, "INSERT INTO v VALUES (1, [0.1, 0.2, 0.3], [1, 2]), (2, [0.2, 0.1, 0.3], [0, 0]), (3, [0.3, 0.2, 0.1], [3, NULL]), (4, NULL, NULL)");
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("v", .{})).flush();
        // StarRocks' answers.
        try expectTexts(allocator, db, "SELECT ROUND(cosine_similarity(ARRAY<FLOAT>[0.1, 0.2, 0.3], data), 3) FROM v ORDER BY id", &.{ "1", "0.929", "0.714", null });
        try expectTexts(allocator, db, "SELECT ROUND(l2_distance(ARRAY<FLOAT>[0.1, 0.2, 0.3], data), 3) FROM v ORDER BY id", &.{ "0", "0.02", "0.08", null });
        try expectTexts(allocator, db, "SELECT ROUND(cosine_similarity_norm(data, data), 3) FROM v ORDER BY id", &.{ "0.14", "0.14", "0.14", null });
        try expectTexts(allocator, db, "SELECT cosine_similarity(i, [3, 4]) FROM v ORDER BY id", &.{ "0.9838699100999074", null, null, null });
        try expectTexts(allocator, db, "SELECT l2_distance(i, [3, 4]) FROM v ORDER BY id", &.{ "8", "25", null, null });
        try expectTexts(allocator, db, "SELECT approx_l2_distance([3, 4], i) FROM v ORDER BY approx_l2_distance([3, 4], i), id", &.{ null, null, "8", "25" });
        try expectTexts(allocator, db, "SELECT APPROX_COSINE_SIMILARITY(i, [3, 4]) FROM v ORDER BY id", &.{ "0.9838699100999074", null, null, null });
    }
    // A FLOAT vector's measure is a FLOAT, summed as one: StarRocks' answer,
    // where the exact squared distance is 500100015.
    try expectText(allocator, db, "SELECT l2_distance(ARRAY<FLOAT>[10000, 10001, 10002, 10003, 10004], [1, 1, 1, 1, 1])", "500100000");
    try expectText(allocator, db, "SELECT l2_distance(ARRAY<DOUBLE>[10000, 10001, 10002, 10003, 10004], [1, 1, 1, 1, 1])", "500100015");
    try expectText(allocator, db, "SELECT typeof(l2_distance(ARRAY<FLOAT>[1], [2]))", "float");
    try expectText(allocator, db, "SELECT l2_distance([], [])", "0");
    try std.testing.expectError(error.ArrayLengthMismatch, exec(allocator, db, "SELECT cosine_similarity([1, 2], [1, 2, 3])"));
}

test "ARRAY: IF, IFNULL, COALESCE and CASE choose whole arrays" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectTexts(allocator, db, "SELECT IF(id > 2, a, [0]) FROM t ORDER BY id", &.{ "[0]", "[0]", null, "[5,null,5,4]" });
    try expectTexts(allocator, db, "SELECT IF(id > 2, NULL, s) FROM t ORDER BY id", &.{ "[\"b\",\"a\",null]", "[]", null, null });
    try expectTexts(allocator, db, "SELECT IFNULL(a, [9]) FROM t ORDER BY id", &.{ "[3,1,2]", "[]", "[9]", "[5,null,5,4]" });
    try expectTexts(allocator, db, "SELECT COALESCE(n, NULL, [[7]]) FROM t ORDER BY id", &.{ "[[1,2],[3]]", "[]", "[[7]]", "[null,[4]]" });
    try expectTexts(allocator, db, "SELECT COALESCE(a, [2.5]) FROM t ORDER BY id", &.{ "[3.0,1.0,2.0]", "[]", "[2.5]", "[5.0,null,5.0,4.0]" });
    try expectTexts(allocator, db, "SELECT CASE WHEN id = 1 THEN a WHEN id = 2 THEN [8] END FROM t ORDER BY id", &.{ "[3,1,2]", "[8]", null, null });
}

test "ARRAY: grouping, ordering and equality compare whole arrays" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE g (id BIGINT PRIMARY KEY, a ARRAY<DOUBLE>)");
    try exec(allocator, db, "INSERT INTO g VALUES (1, [1, 2]), (2, [1]), (3, [1, 2]), (4, [0.0]), (5, [-0.0]), (6, [1, NULL])");

    try expectTexts(allocator, db, "SELECT a FROM g GROUP BY a ORDER BY a", &.{ "[0]", "[1]", "[1,null]", "[1,2]" });
    try expectTexts(allocator, db, "SELECT COUNT(*) FROM g GROUP BY a ORDER BY a", &.{ "2", "1", "1", "2" });
    try expectTexts(allocator, db, "SELECT id FROM g WHERE a = [1, 2] ORDER BY id", &.{ "1", "3" });
    try expectTexts(allocator, db, "SELECT id FROM g ORDER BY a DESC, id", &.{ "1", "3", "6", "2", "4", "5" });
}

test "ARRAY: comparisons meet at the common array type" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectTexts(allocator, db, "SELECT id FROM t WHERE a = [3, 1, 2] ORDER BY id", &.{"1"});
    try expectTexts(allocator, db, "SELECT id FROM t WHERE a != [3, 1, 2] ORDER BY id", &.{ "2", "4" });
    try expectTexts(allocator, db, "SELECT id FROM t WHERE a > [3] ORDER BY id", &.{ "1", "4" });
    try expectTexts(allocator, db, "SELECT id FROM t WHERE a < [1] ORDER BY id", &.{"2"});
    try expectTexts(allocator, db, "SELECT id FROM t WHERE s = ['x'] ORDER BY id", &.{"4"});
    try expectTexts(allocator, db, "SELECT id FROM t WHERE d = [1.5, 2.25] ORDER BY id", &.{"1"});
    try expectTexts(allocator, db, "SELECT id FROM t WHERE n = [[1, 2], [3]] ORDER BY id", &.{"1"});
    try expectTexts(allocator, db, "SELECT a = [3, 1, 2] FROM t ORDER BY id", &.{ "1", "0", null, "0" });
    try expectText(allocator, db, "SELECT [1, 2] = [1.0, 2.0]", "1");
    try expectText(allocator, db, "SELECT [1, 2] < [1, 3]", "1");
    try expectText(allocator, db, "SELECT [2] > [1, 5]", "1");
    try expectText(allocator, db, "SELECT [1, NULL] = [1, NULL]", "1");

    try exec(allocator, db, "INSERT INTO t VALUES (5, '[7, 8]', '[\"p\"]', '[[9]]', '[0.5]')");
    try expectTexts(allocator, db, "SELECT a FROM t WHERE id = 5", &.{"[7,8]"});
    try expectTexts(allocator, db, "SELECT n FROM t WHERE id = 5", &.{"[[9]]"});
    try expectTexts(allocator, db, "SELECT d FROM t WHERE id = 5", &.{"[0.50]"});
}

test "ARRAY: two array columns of different types compare and join at their common type" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE c (id BIGINT PRIMARY KEY, s10 ARRAY<VARCHAR(10)>, s20 ARRAY<VARCHAR(20)>, f ARRAY<FLOAT>, i ARRAY<INT>, n ARRAY<ARRAY<INT>>)");
    try exec(allocator, db,
        \\INSERT INTO c VALUES
        \\ (1, ['a', 'b'], ['a', 'b'], [1.5], [1], [[1]]),
        \\ (2, ['aa', NULL], ['ssss', NULL], [2.0], [2], [[2]]),
        \\ (3, NULL, NULL, NULL, NULL, NULL)
    );

    try expectTexts(allocator, db, "SELECT id FROM c WHERE s10 = s20 ORDER BY id", &.{"1"});
    try expectTexts(allocator, db, "SELECT s10 < s20 FROM c ORDER BY id", &.{ "0", "1", null });
    try expectTexts(allocator, db, "SELECT f = i FROM c ORDER BY id", &.{ "0", "1", null });
    try expectTexts(allocator, db, "SELECT id FROM c WHERE f >= i ORDER BY id", &.{ "1", "2" });
    try expectTexts(allocator, db, "SELECT id FROM c WHERE f <> i ORDER BY id", &.{"1"});
    try expectTexts(allocator, db, "SELECT b.id FROM c a LEFT JOIN c b ON a.f = b.i ORDER BY a.id", &.{ null, "2", null });
    try expectTexts(allocator, db, "SELECT b.id FROM c a JOIN c b ON a.i = b.f ORDER BY a.id", &.{"2"});
    try helpers.expectRunError(allocator, db, "SELECT id FROM c WHERE i = n", error.PredicateTypeMismatch);
    // Text never reads as an array between columns, as a NULL literal does.
    try helpers.expectRunError(allocator, db, "SELECT id FROM c WHERE s10 = n", error.PredicateTypeMismatch);
    try helpers.expectRunError(allocator, db, "SELECT a.id FROM c a JOIN c b ON a.s10 = b.n", error.JoinKeyTypeMismatch);
}

test "ARRAY: an empty, all-NULL or BOOLEAN array meets a nested sibling, as in StarRocks" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectText(allocator, db, "SELECT array_concat([], [[1]])", "[[1]]");
    try expectText(allocator, db, "SELECT array_concat([NULL], [[1]])", "[null,[1]]");
    try expectText(allocator, db, "SELECT array_concat([true], [[1]])", "[null,[1]]");
    try expectText(allocator, db,
        \\WITH t0 AS (SELECT c1 FROM (VALUES ([])) AS v(c1))
        \\SELECT array_concat(c1, [map{'a': 1}]) FROM t0
    , "[{\"a\":1}]");
    try expectText(allocator, db, "SELECT array_contains_seq([named_struct('a', 1)], [])", "1");
    try expectText(allocator, db, "SELECT array_contains_seq([], [named_struct('a', 1)])", "0");
    try expectText(allocator, db, "SELECT array_contains(NULL, NULL)", null);
    try expectText(allocator, db, "SELECT array_position(NULL, NULL)", null);
}

test "ARRAY: NULLIF compares whole arrays at their common type" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectTexts(allocator, db, "SELECT NULLIF(a, [3, 1, 2]) FROM t ORDER BY id", &.{ null, "[]", null, "[5,null,5,4]" });
    try expectTexts(allocator, db, "SELECT NULLIF(n, [[1, 2], [3]]) FROM t ORDER BY id", &.{ null, "[]", null, "[null,[4]]" });
    try expectText(allocator, db, "SELECT NULLIF([1, 2], ['1', '2'])", null);
    try expectText(allocator, db, "SELECT NULLIF([1, 2], ['1', '3'])", "[\"1\",\"2\"]");
    try expectText(allocator, db, "SELECT NULLIF([1, NULL], [1, NULL])", null);
}

test "ARRAY: ARRAY_AGG keeps NULLs and follows ORDER BY" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE g (id BIGINT PRIMARY KEY, k INT, v INT, s VARCHAR(10), a ARRAY<INT>)");
    try exec(allocator, db, "INSERT INTO g VALUES (1, 1, 10, 'x', [1]), (2, 1, NULL, 'y', [2, 3]), (3, 2, 30, 'x', NULL), (4, 2, 20, NULL, []), (5, 1, 10, 'x', [1])");

    try expectText(allocator, db, "SELECT array_agg(v ORDER BY id) FROM g", "[10,null,30,20,10]");
    try expectTexts(allocator, db, "SELECT array_agg(v ORDER BY id) FROM g GROUP BY k ORDER BY k", &.{ "[10,null,10]", "[30,20]" });
    try expectTexts(allocator, db, "SELECT array_sort(array_agg(v)) FROM g GROUP BY s ORDER BY s", &.{ "[20]", "[10,10,30]", "[null]" });
    try expectText(allocator, db, "SELECT array_agg(DISTINCT v) FROM g", "[null,10,20,30]");
    try expectText(allocator, db, "SELECT array_agg(s ORDER BY s DESC, id) FROM g", "[\"y\",\"x\",\"x\",\"x\",null]");
    try expectText(allocator, db, "SELECT array_agg(a ORDER BY id) FROM g", "[[1],[2,3],null,[],[1]]");
    try expectText(allocator, db, "SELECT array_agg(v) FROM g WHERE id > 100", null);
    try expectText(allocator, db, "SELECT array_length(array_agg(v)) FROM g", "5");
    try expectText(allocator, db, "SELECT array_sort(array_agg(v)) FROM g", "[null,10,10,20,30]");
}

test "ARRAY: ARRAY_AGG and GROUP_CONCAT ORDER BY take NULLS FIRST / NULLS LAST" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE g (id BIGINT PRIMARY KEY, k INT, v INT, s VARCHAR(10))");
    try exec(allocator, db, "INSERT INTO g VALUES (1, 1, 10, 'x'), (2, 1, NULL, 'y'), (3, 2, 30, 'x'), (4, 2, 20, NULL), (5, 1, 10, 'x')");

    const cases = .{
        .{ "SELECT array_agg(v ORDER BY v NULLS LAST, id) FROM g", "[10,10,20,30,null]" },
        .{ "SELECT array_agg(v ORDER BY v ASC NULLS FIRST, id) FROM g", "[null,10,10,20,30]" },
        .{ "SELECT array_agg(v ORDER BY v DESC NULLS FIRST, id) FROM g", "[null,30,20,10,10]" },
        .{ "SELECT array_agg(v ORDER BY v DESC NULLS LAST, id) FROM g", "[30,20,10,10,null]" },
        .{ "SELECT array_agg(s ORDER BY s NULLS LAST, id DESC) FROM g", "[\"x\",\"x\",\"x\",\"y\",null]" },
        .{ "SELECT array_agg(DISTINCT v ORDER BY v NULLS LAST) FROM g", "[10,20,30,null]" },
        .{ "SELECT group_concat(id ORDER BY s DESC NULLS FIRST, id) FROM g", "4,2,1,3,5" },
    };
    inline for (cases) |c| try expectText(allocator, db, c[0], c[1]);
    try expectTexts(allocator, db, "SELECT array_agg(v ORDER BY v DESC NULLS FIRST) FROM g GROUP BY k ORDER BY k", &.{ "[null,10,10]", "[30,20]" });

    // MySQL's ORDER BY <n> names the n-th argument ahead of NULLS too.
    var q = try helpers.runSqlMysql(allocator, db, "SELECT array_agg(v ORDER BY 1 DESC NULLS FIRST) FROM g");
    defer q.deinit();
    const got = try helpers.columnText(allocator, &q);
    defer helpers.freeStrings(allocator, got);
    try std.testing.expectEqualStrings("[null,30,20,10,10]", got[0].?);
}

test "ARRAY: ARRAY_UNIQUE_AGG is the distinct elements of a group's arrays" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE ua (id BIGINT PRIMARY KEY, k INT, i ARRAY<BIGINT>, s ARRAY<VARCHAR(10)>, d ARRAY<DECIMAL(26, 2)>, ai ARRAY<ARRAY<BIGINT>>)");
    try exec(allocator, db,
        \\INSERT INTO ua VALUES (1, 1, [1], ['a', 'a'], [1.3], [[1]]), (2, 2, [2], ['1'], [100.0], [[2]]), (3, 1, [1], ['a'], [1.3], [[1]]),
        \\  (4, 2, [3, NULL, 2], NULL, NULL, [[2], [3]]), (5, 3, NULL, NULL, NULL, NULL)
    );

    try expectTexts(allocator, db, "SELECT array_unique_agg(i ORDER BY id) FROM ua GROUP BY k ORDER BY k", &.{ "[1]", "[2,3,null]", null });
    try expectTexts(allocator, db, "SELECT array_unique_agg(s) FROM ua GROUP BY k ORDER BY k", &.{ "[\"a\"]", "[\"1\"]", null });
    try expectTexts(allocator, db, "SELECT array_unique_agg(d) FROM ua GROUP BY k ORDER BY k", &.{ "[1.30]", "[100.00]", null });
    try expectTexts(allocator, db, "SELECT array_unique_agg(ai ORDER BY id) FROM ua GROUP BY k ORDER BY k", &.{ "[[1]]", "[[2],[3]]", null });
    try expectText(allocator, db, "SELECT array_sort(array_unique_agg(i)) FROM ua", "[null,1,2,3]");
    try expectTexts(allocator, db, "SELECT array_max(array_unique_agg(i)) FROM ua GROUP BY k ORDER BY k", &.{ "1", "3", null });
    try expectText(allocator, db, "SELECT array_unique_agg(i) FROM ua WHERE id > 100", null);
}

test "ARRAY: subscripts and array functions inside predicates" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectTexts(allocator, db, "SELECT id FROM t WHERE a[1] = 3 ORDER BY id", &.{"1"});
    try expectTexts(allocator, db, "SELECT id FROM t WHERE a[2] IS NULL ORDER BY id", &.{ "2", "3", "4" });
    try expectTexts(allocator, db, "SELECT id FROM t WHERE array_contains(a, 5) ORDER BY id", &.{"4"});
    try expectTexts(allocator, db, "SELECT id FROM t WHERE array_length(a) > 2 ORDER BY id", &.{ "1", "4" });
    try expectTexts(allocator, db, "SELECT id FROM t WHERE split('p,q', ',')[2] = 'q' ORDER BY id", &.{ "1", "2", "3", "4" });
    try expectTexts(allocator, db, "SELECT id FROM t WHERE array_sort(a) = [1, 2, 3] ORDER BY id", &.{"1"});
}
