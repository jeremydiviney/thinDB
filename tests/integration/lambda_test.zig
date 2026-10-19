//! Lambdas end-to-end: `array_map` and the StarRocks functions that read a
//! lambda's values (`array_filter`, `any_match`, `all_match`,
//! `array_sortby`, `map_filter`, `transform_values`, `transform_keys`), plus
//! the plain-array forms those lower to. Results are checked by their
//! StarRocks text.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const expectTexts = helpers.expectTexts;
const expectText = helpers.expectText;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (k BIGINT PRIMARY KEY, arr_0 ARRAY<BIGINT> NOT NULL, arr_1 ARRAY<BIGINT>, arr_2 ARRAY<BIGINT>, s ARRAY<VARCHAR(10)>)");
    try exec(allocator, db,
        \\INSERT INTO t VALUES
        \\ (1, [1, 2], [1, 2], [2, 3], ['b', 'a']),
        \\ (2, [1, 2], NULL, [2, 3], NULL),
        \\ (3, [1, 2], [1, 2], NULL, ['x', NULL, 'y']),
        \\ (4, [1, 2], [NULL, NULL], [2, 3], [])
    );
    return db;
}

test "LAMBDA: array_map pairs its arrays element by element" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectTexts(allocator, db, "SELECT array_map((x, y, z) -> x + y + z, arr_0, arr_1, arr_2) FROM t ORDER BY k", &.{ "[4,7]", null, null, "[null,null]" });
    try expectTexts(allocator, db, "SELECT array_map((x, y, z, d) -> x + y + z + d, arr_0, arr_1, arr_2, [1, 2]) FROM t ORDER BY k", &.{ "[5,9]", null, null, "[null,null]" });
    try expectTexts(allocator, db, "SELECT array_map(x -> x * x, arr_0) FROM t ORDER BY k", &.{ "[1,4]", "[1,4]", "[1,4]", "[1,4]" });
    try expectTexts(allocator, db, "SELECT array_map(x -> x, arr_0) FROM t ORDER BY k", &.{ "[1,2]", "[1,2]", "[1,2]", "[1,2]" });
    try expectTexts(allocator, db, "SELECT array_map((x, y, z) -> 10, arr_0, arr_1, arr_2) FROM t ORDER BY k", &.{ "[10,10]", null, null, "[10,10]" });
    try expectText(allocator, db, "SELECT array_map((x, y, z) -> x + y + z, [1, 2], [2, NULL], [3, 4])", "[6,null]");
    try expectText(allocator, db, "SELECT array_map((x, y, z) -> x + y + z, [1, 2], [2, NULL], NULL)", null);
    try expectText(allocator, db, "SELECT array_map(x -> x + 1, [])", "[]");
    try expectText(allocator, db, "SELECT array_map(x -> concat(x, '!'), ['a', NULL])", "[\"a!\",null]");
    try expectText(allocator, db, "SELECT transform([1, 2], x -> x * 10)", "[10,20]");
    try expectTexts(allocator, db, "SELECT array_map(x -> upper(x), s) FROM t ORDER BY k", &.{ "[\"B\",\"A\"]", null, "[\"X\",null,\"Y\"]", "[]" });
    try std.testing.expectError(error.ArrayLengthMismatch, exec(allocator, db, "SELECT array_map((x, y) -> x + y, [1, 2], [1])"));
    try std.testing.expectError(error.ArrayLengthMismatch, exec(allocator, db, "SELECT array_map((x, y) -> x + y, arr_0, [1]) FROM t"));
}

test "LAMBDA: bodies read outer columns, nest, and shadow" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectTexts(allocator, db, "SELECT array_map((x, y) -> k, arr_0, arr_1) FROM t ORDER BY k", &.{ "[1,1]", null, "[3,3]", "[4,4]" });
    try expectTexts(allocator, db, "SELECT array_map((x, y) -> k, [1, 2], [2, 3]) FROM t ORDER BY k", &.{ "[1,1]", "[2,2]", "[3,3]", "[4,4]" });
    try expectTexts(allocator, db, "SELECT array_map(arr_0, x -> x + k) FROM t ORDER BY k", &.{ "[2,3]", "[3,4]", "[4,5]", "[5,6]" });
    try expectTexts(allocator, db, "SELECT array_map(x -> array_length(arr_2) + x, arr_0) FROM t ORDER BY k", &.{ "[3,4]", "[3,4]", "[null,null]", "[3,4]" });
    try expectText(allocator, db, "SELECT array_map(x -> array_map(x -> x + 100, x), [[1, 2, 3], [NULL]])", "[[101,102,103],[null]]");
    try expectText(allocator, db, "SELECT array_map(x -> array_map(x -> array_map(x -> x + 100, x), x), [[[1, 2, 3]]])", "[[[101,102,103]]]");
    try expectText(allocator, db, "SELECT array_map(a0 -> array_map(a1 -> array_map(a2 -> array_map(a3 -> array_length(a1) + a3, a2), a1), a0), [[[[1, 2]]]])", "[[[[2,3]]]]");
    try expectText(allocator, db, "SELECT array_map(a0 -> array_map(a1 -> array_map(a2 -> array_map(a3 -> array_length(a2) + a3, a2), a1), a0), [[[[1, 2]]]])", "[[[[3,4]]]]");
    try expectText(allocator, db, "SELECT array_map(x -> array_map(y -> x * 10 + y, [1, 2]), [1, 2])", "[[11,12],[21,22]]");
    try expectTexts(allocator, db, "SELECT array_map(x -> x + any_match(array_map(y -> y < 10, arr_1)), arr_0) FROM t ORDER BY k", &.{ "[2,3]", "[null,null]", "[2,3]", "[null,null]" });
    try expectTexts(allocator, db, "SELECT k FROM t WHERE element_at(array_map(x -> x + k, arr_0), 1) > 3 ORDER BY k", &.{ "3", "4" });
    try expectTexts(allocator, db, "SELECT k FROM t WHERE any_match(arr_0, x -> x = k) ORDER BY k", &.{ "1", "2" });
    try expectTexts(allocator, db, "SELECT array_map(x -> x * 2, v) FROM (SELECT k, arr_0 AS v FROM t) d ORDER BY k", &.{ "[2,4]", "[2,4]", "[2,4]", "[2,4]" });
    try expectTexts(allocator, db, "SELECT sum(array_sum(array_map(x -> x + k, arr_0))) FROM t", &.{"32"});
    // The parameter `k` hides the column, so the second `k + 1` is the
    // element's, though the first is anchored as the column's.
    try expectTexts(allocator, db, "SELECT k FROM t WHERE k + 1 > 3 AND any_match(arr_0, k -> k + 1 > 3) ORDER BY k", &.{});
    try expectTexts(allocator, db, "SELECT k FROM t WHERE k + 1 > 3 AND any_match(arr_0, k -> k + 1 > 2) ORDER BY k", &.{ "3", "4" });
}

test "LAMBDA: bodies read struct fields" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE st (id INT PRIMARY KEY, s STRUCT<a INT, b STRING>, arr ARRAY<INT>, ss ARRAY<STRUCT<a INT, b STRING>>)");
    try exec(allocator, db,
        \\INSERT INTO st VALUES
        \\ (1, row(10, 'x'), [1, 2], [row(1, 'p'), row(2, 'q')]),
        \\ (2, NULL, [3], [row(NULL, 'r')]),
        \\ (3, row(30, 'z'), [], NULL)
    );

    try expectTexts(allocator, db, "SELECT array_map(x -> x + s.a, arr) FROM st ORDER BY id", &.{ "[11,12]", "[null]", "[]" });
    try expectTexts(allocator, db, "SELECT array_map(x -> x.a, ss) FROM st ORDER BY id", &.{ "[1,2]", "[null]", null });
    try expectTexts(allocator, db, "SELECT array_map(x -> concat(x.b, s.b), ss) FROM st ORDER BY id", &.{ "[\"px\",\"qx\"]", "[null]", null });
    try expectTexts(allocator, db, "SELECT array_filter(ss, x -> x.a > 1) FROM st ORDER BY id", &.{ "[{\"a\":2,\"b\":\"q\"}]", "[]", null });
    try expectTexts(allocator, db, "SELECT array_map(x -> row(x, s.a), arr) FROM st ORDER BY id", &.{ "[{\"col1\":1,\"col2\":10},{\"col1\":2,\"col2\":10}]", "[{\"col1\":3,\"col2\":null}]", "[]" });
    // The parameter `s` hides the column, fields and all.
    try expectTexts(allocator, db, "SELECT array_map(s -> s.a + 100, ss) FROM st ORDER BY id", &.{ "[101,102]", "[null]", null });
    try expectText(allocator, db, "SELECT array_map(x -> x.n.a * 2, [named_struct('n', named_struct('a', 1)), named_struct('n', named_struct('a', 5))])", "[2,10]");
    try expectText(allocator, db, "SELECT array_map((x, y) -> x.col1 + y.col2, [row(1, 2)], [row(10, 20)])", "[21]");
}

test "LAMBDA: predicates in bodies" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectText(allocator, db, "SELECT array_map(x -> x > 1, [1, 2, NULL])", "[0,1,null]");
    try expectText(allocator, db, "SELECT array_map(x -> x IS NULL, [1, NULL])", "[0,1]");
    try expectText(allocator, db, "SELECT array_map((x, y) -> x > y, [1, 5], [2, 3])", "[0,1]");
    try expectText(allocator, db, "SELECT array_map(x -> CASE WHEN x > 1 THEN 'big' ELSE 'small' END, [1, 2])", "[\"small\",\"big\"]");
    try expectText(allocator, db, "SELECT array_map(x -> x BETWEEN 2 AND 3 AND x <> 3, [1, 2, 3])", "[0,1,0]");
    try expectTexts(allocator, db, "SELECT array_map(x -> x IS NULL, s) FROM t ORDER BY k", &.{ "[0,0]", null, "[0,1,0]", "[]" });
}

test "LAMBDA: the functions that read a lambda" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectText(allocator, db, "SELECT array_filter(x -> x > 1, [1, 2, 3, NULL])", "[2,3]");
    try expectText(allocator, db, "SELECT array_filter([1, 2, 3], x -> x % 2 = 1)", "[1,3]");
    try expectText(allocator, db, "SELECT array_filter((x, y) -> y, [1, 2, 3], [true, false, true])", "[1,3]");
    try expectTexts(allocator, db, "SELECT array_filter(x -> x > k, arr_2) FROM t ORDER BY k", &.{ "[2,3]", "[3]", null, "[]" });

    try expectText(allocator, db, "SELECT any_match((x, y) -> x < y, [1, 2, 8], [4, 5, 6])", "1");
    try expectText(allocator, db, "SELECT any_match((x, y) -> x < y, [11, 12, 8], [4, 5, 6])", "0");
    try expectText(allocator, db, "SELECT any_match((x, y) -> x < y, [11, 12, NULL], [4, 5, 6])", null);
    try expectText(allocator, db, "SELECT all_match((x, y) -> x < y, [1, 2, NULL], [4, 5, 6])", null);
    try expectText(allocator, db, "SELECT all_match((x, y) -> x < y, [1, 2, 3], [4, 5, 6])", "1");
    try expectText(allocator, db, "SELECT all_match((x, y) -> x < y, [], [])", "1");
    try expectText(allocator, db, "SELECT any_match((x, y) -> x < y, [], [])", "0");
    try expectText(allocator, db, "SELECT any_match((x, y) -> x < y, NULL, [4, 5, 6])", null);
    try expectText(allocator, db, "SELECT all_match([0], x -> 1)", "1");
    try expectText(allocator, db, "SELECT any_match([], x -> 1)", "0");
    try expectTexts(allocator, db, "SELECT all_match(s, x -> x IS NULL) FROM t ORDER BY k", &.{ "0", null, "0", "1" });
    try expectTexts(allocator, db, "SELECT any_match(s, x -> x IS NULL) FROM t ORDER BY k", &.{ "0", null, "1", "0" });

    try expectText(allocator, db, "SELECT array_sortby(x -> -x, [1, 3, 2])", "[3,2,1]");
    try expectText(allocator, db, "SELECT array_sortby((x, y) -> y, ['a', 'b', 'c'], [3, 1, 2])", "[\"b\",\"c\",\"a\"]");
    try expectTexts(allocator, db, "SELECT array_sortby(x -> x IS NULL, s) FROM t ORDER BY k", &.{ "[\"b\",\"a\"]", null, "[\"x\",\"y\",null]", "[]" });

    try expectText(allocator, db, "SELECT map_filter((k, v) -> v > 1, map{1: 1, 2: 2, 3: 3})", "{2:2,3:3}");
    try expectText(allocator, db, "SELECT map_filter(map{'a': 1, 'b': NULL}, (k, v) -> v IS NULL)", "{\"b\":null}");
    try expectText(allocator, db, "SELECT transform_values((k, v) -> v * 10, map{1: 1, 2: 2})", "{1:10,2:20}");
    try expectText(allocator, db, "SELECT transform_keys((k, v) -> k + v, map{1: 1, 2: 2})", "{2:1,4:2}");
    try expectText(allocator, db, "SELECT transform_values((k, v) -> v, NULL)", null);
}

test "LAMBDA: the plain-array forms" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectTexts(allocator, db, "SELECT array_filter(arr_0, arr_2) FROM t ORDER BY k", &.{ "[1,2]", "[1,2]", "[]", "[1,2]" });
    try expectText(allocator, db, "SELECT array_filter([1, 2, 3, 4], [0, 0, 1, 1])", "[3,4]");
    try expectText(allocator, db, "SELECT array_filter([1, 2], [1, 0, 1])", "[1]");
    try expectText(allocator, db, "SELECT array_filter([1, 2, 3], [true, NULL])", "[1]");
    try expectText(allocator, db, "SELECT array_filter([1, 2, 3, 4], NULL)", "[]");
    try expectText(allocator, db, "SELECT array_filter(NULL, [1, 0])", null);

    try expectText(allocator, db, "SELECT any_match([false, NULL, true])", "1");
    try expectText(allocator, db, "SELECT any_match([false, NULL])", null);
    try expectText(allocator, db, "SELECT any_match([])", "0");
    try expectText(allocator, db, "SELECT any_match(NULL)", null);
    try expectText(allocator, db, "SELECT all_match([true, NULL, false])", "0");
    try expectText(allocator, db, "SELECT all_match([true, NULL])", null);
    try expectText(allocator, db, "SELECT all_match([])", "1");

    try expectText(allocator, db, "SELECT array_sortby([1, 2, 3, 4, 5, 6], ['a', 'b', 'c', 'c', 'b', 'a'], [11, 22, 32, 31, 21, 12])", "[1,6,5,2,4,3]");
    try expectText(allocator, db, "SELECT array_sortby([1, 2, NULL, 4, 5, 6], ['a', 'b', 'c', 'c', 'b', 'a'], [11, 22, 32, 31, 21, 12])", "[1,6,5,2,4,null]");
    try expectText(allocator, db, "SELECT array_sortby([1, 2, 3, 4, 5, 6], ['a', 'b', NULL, NULL, 'b', 'a'], [11, 22, 32, 31, 21, 12])", "[4,3,1,6,5,2]");
    try expectText(allocator, db, "SELECT array_sortby([1, 2, 3, 4, 5, 6], CAST(NULL AS ARRAY<INT>), ['a', 'b', 'c', 'c', 'b', 'a'])", "[1,6,2,5,3,4]");
    try expectText(allocator, db, "SELECT array_sortby(CAST(NULL AS ARRAY<INT>), [1])", null);
    try expectTexts(allocator, db, "SELECT array_sortby(arr_0, arr_1) FROM t ORDER BY k", &.{ "[1,2]", "[1,2]", "[1,2]", "[1,2]" });
    try std.testing.expectError(error.ArrayLengthMismatch, exec(allocator, db, "SELECT array_sortby([1, 2], [1, 2, 3])"));

    try expectText(allocator, db, "SELECT map_filter(map{1: 'a', 2: 'b', 3: 'c'}, [true, false, true])", "{1:\"a\",3:\"c\"}");
    try expectText(allocator, db, "SELECT map_filter(map{1: 'a', 2: 'b'}, NULL)", "{}");
}

test "LAMBDA: over flushed row groups, in parallel" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .auto_flush_secs = 0, .max_dop = 4, .row_group_size = 1024 });
    defer db.close();
    try exec(allocator, db, "CREATE TABLE p (id BIGINT PRIMARY KEY, g BIGINT, a ARRAY<BIGINT>)");
    try exec(allocator, db, "INSERT INTO p SELECT generate_series, generate_series % 7, [generate_series, generate_series * 2] FROM table(generate_series(1, 20000))");
    const t = try db.openTable("p", .{});
    try t.flush();

    try expectTexts(allocator, db, "SELECT sum(array_sum(array_map(x -> x + g, a))) FROM p", &.{"600149996"});
    try expectTexts(allocator, db, "SELECT sum(array_sum(array_map(x -> x + g, a))) FROM p WHERE g = 3", &.{"85718571"});
    try expectTexts(allocator, db, "SELECT count(*) FROM p WHERE any_match(a, x -> x > 39990)", &.{"5"});
    try expectTexts(allocator, db, "SELECT array_map(x -> x * g, a) FROM p ORDER BY id LIMIT 3", &.{ "[1,2]", "[4,8]", "[9,18]" });
    try expectTexts(allocator, db, "WITH c AS (SELECT id, array_map(x -> x + 1, a) AS b FROM p) SELECT sum(array_sum(b)) FROM c", &.{"600070000"});
    try expectTexts(allocator, db, "SELECT g, sum(array_length(array_filter(a, x -> x % 4 = 0))) FROM p GROUP BY g ORDER BY g LIMIT 2", &.{ "0", "1" });
}
