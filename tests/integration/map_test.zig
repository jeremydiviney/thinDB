//! MAP type end-to-end: MAP<K, V> columns store, flush and reopen; map
//! literals, subscripts and CAST; the map functions and MAP_AGG, each checked
//! by its result's StarRocks text.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const expectTexts = helpers.expectTexts;
const expectText = helpers.expectText;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE m (id BIGINT PRIMARY KEY, c2 MAP<VARCHAR(8), INT>, s MAP<INT, STRING>, n MAP<STRING, ARRAY<INT>>)");
    try exec(allocator, db,
        \\INSERT INTO m VALUES
        \\ (1, map{'key1': 1}, map{1: 'a', 2: NULL}, map{'x': [1, 2]}),
        \\ (2, map{'key1': 5, 'key2': 6}, map(), map{'y': [], 'z': NULL}),
        \\ (3, map{'key1': 2, 'key2': 3, 'key4': 4}, NULL, NULL),
        \\ (4, map{'key1': 12, 'key2': 13, 'key3': 14, 'key4': 15}, map(3, 'c'), map()),
        \\ (5, map{'key1': 7, 'key2': 8, 'key3': 9, 'key4': 10, 'key5': 11}, map{5: 'e', 5: 'f'}, map{'x': [NULL]})
    );
    return db;
}

test "MAP: columns store, flush and reopen" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const c2 = [_]?[]const u8{
        "{\"key1\":1}",
        "{\"key1\":5,\"key2\":6}",
        "{\"key1\":2,\"key2\":3,\"key4\":4}",
        "{\"key1\":12,\"key2\":13,\"key3\":14,\"key4\":15}",
        "{\"key1\":7,\"key2\":8,\"key3\":9,\"key4\":10,\"key5\":11}",
    };
    {
        var db = try setup(allocator, io, tmp.dir);
        defer db.close();
        try expectTexts(allocator, db, "SELECT c2 FROM m ORDER BY id", &c2);
        const t = try db.openTable("m", .{});
        try t.flush();
    }
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try expectTexts(allocator, db, "SELECT c2 FROM m ORDER BY id", &c2);
    try expectTexts(allocator, db, "SELECT s FROM m ORDER BY id", &.{ "{1:\"a\",2:null}", "{}", null, "{3:\"c\"}", "{5:\"f\"}" });
    try expectTexts(allocator, db, "SELECT n FROM m ORDER BY id", &.{ "{\"x\":[1,2]}", "{\"y\":[],\"z\":null}", null, "{}", "{\"x\":[null]}" });
}

test "MAP: literals, subscripts and CAST" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectText(allocator, db, "SELECT map{'a': 1, 'b': 2}", "{\"a\":1,\"b\":2}");
    try expectText(allocator, db, "SELECT map('a', 1, 'b', 2)", "{\"a\":1,\"b\":2}");
    try expectText(allocator, db, "SELECT map{'a': 1, 'b': 2, 'a': 3}", "{\"b\":2,\"a\":3}");
    try expectText(allocator, db, "SELECT map{1: 'a', 2: 2.5}", "{1:\"a\",2:\"2.5\"}");
    try expectText(allocator, db, "SELECT map{}", "{}");
    try expectText(allocator, db, "SELECT map{'a': [1], 'b': NULL}", "{\"a\":[1],\"b\":null}");
    try expectText(allocator, db, "SELECT [map{'a': 1}, map()]", "[{\"a\":1},{}]");
    try expectText(allocator, db, "SELECT element_at(map{'a': 1, 'b': 2}, 'a')", "1");
    try expectText(allocator, db, "SELECT map{'a': 1, 'b': 2}['a']", "1");
    try expectText(allocator, db, "SELECT map{'a': 1, 'b': 2}['c']", null);
    try expectText(allocator, db, "SELECT map{'a': NULL, 'b': 2}['a']", null);
    try expectText(allocator, db, "SELECT map{}['a']", null);
    try expectText(allocator, db, "SELECT map{NULL: 'a', 2: 'b'}[NULL]", "a");
    try expectText(allocator, db, "SELECT map{1: 'a'}[1.0]", "a");
    try expectText(allocator, db, "SELECT map{'a': [1, 2]}['a'][2]", "2");
    try expectTexts(allocator, db, "SELECT c2['key2'] FROM m ORDER BY id", &.{ null, "6", "3", "13", "8" });
    try expectTexts(allocator, db, "SELECT map{1: 'a', 2: 'b'}[id] FROM m ORDER BY id", &.{ "a", "b", null, null, null });
    try expectText(allocator, db, "SELECT CAST('{\"a\": 1, \"b\": null}' AS MAP<STRING, INT>)", "{\"a\":1,\"b\":null}");
    try expectText(allocator, db, "SELECT CAST('{1: [2], 3: []}' AS MAP<INT, ARRAY<INT>>)", "{1:[2],3:[]}");
    try expectText(allocator, db, "SELECT CAST('null' AS MAP<STRING, INT>)", null);
    try expectText(allocator, db, "SELECT CAST(map{'a': 1} AS MAP<STRING, STRING>)", "{\"a\":\"1\"}");
    try expectText(allocator, db, "SELECT CAST(map{'1': 'x', '01': 'y'} AS MAP<INT, STRING>)", "{1:\"y\"}");
    try expectText(allocator, db, "SELECT CAST(map{1: 2} AS STRING)", "{1:2}");

    var q = try helpers.runSqlMysql(allocator, db, "SELECT map{\"key1\": 1, \"key2\": NULL}");
    defer q.deinit();
    const got = try helpers.columnText(allocator, &q);
    defer helpers.freeStrings(allocator, got);
    try std.testing.expectEqualStrings("{\"key1\":1,\"key2\":null}", got[0].?);
}

test "MAP: functions" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectTexts(allocator, db, "SELECT map_size(s) FROM m ORDER BY id", &.{ "2", "0", null, "1", "1" });
    try expectTexts(allocator, db, "SELECT cardinality(c2) FROM m ORDER BY id", &.{ "1", "2", "3", "4", "5" });
    try expectText(allocator, db, "SELECT map_size(NULL)", null);
    try expectTexts(allocator, db, "SELECT map_keys(s) FROM m ORDER BY id", &.{ "[1,2]", "[]", null, "[3]", "[5]" });
    try expectTexts(allocator, db, "SELECT map_values(n) FROM m ORDER BY id", &.{ "[[1,2]]", "[[],null]", null, "[]", "[[null]]" });
    try expectTexts(allocator, db, "SELECT map_concat(s, map{2: 'b', 9: 'z'}) FROM m ORDER BY id", &.{
        "{1:\"a\",2:\"b\",9:\"z\"}",
        "{2:\"b\",9:\"z\"}",
        "{2:\"b\",9:\"z\"}",
        "{3:\"c\",2:\"b\",9:\"z\"}",
        "{5:\"f\",2:\"b\",9:\"z\"}",
    });
    try expectText(allocator, db, "SELECT map_concat(map('a', 1, 'b', 2), map())", "{\"a\":1,\"b\":2}");
    try expectText(allocator, db, "SELECT map_concat(map('a', 1), map('a', 2.5), map('c', 3))", "{\"a\":2.5,\"c\":3.0}");
    try expectText(allocator, db, "SELECT map_concat(NULL, map(1, 2))", "{1:2}");
    try expectText(allocator, db, "SELECT map_from_arrays([1, 2], ['a', 'b'])", "{1:\"a\",2:\"b\"}");
    try expectText(allocator, db, "SELECT map_from_arrays([1, 1], ['a', 'b'])", "{1:\"b\"}");
    try expectText(allocator, db, "SELECT map_from_arrays([], [])", "{}");
    try expectText(allocator, db, "SELECT map_from_arrays(NULL, [1])", null);
    try std.testing.expectError(error.MapArrayLengthMismatch, exec(allocator, db, "SELECT map_from_arrays([1, 2], [1])"));
    try expectTexts(allocator, db, "SELECT distinct_map_keys(c2) = c2 FROM m ORDER BY id", &.{ "1", "1", "1", "1", "1" });
    try expectTexts(allocator, db, "SELECT IFNULL(s, map{0: 'none'}) FROM m ORDER BY id", &.{ "{1:\"a\",2:null}", "{}", "{0:\"none\"}", "{3:\"c\"}", "{5:\"f\"}" });
    try expectTexts(allocator, db, "SELECT IF(id = 1, map{'k': 0}, c2) FROM m WHERE id <= 2 ORDER BY id", &.{ "{\"k\":0}", "{\"key1\":5,\"key2\":6}" });
}

test "MAP: ordering by a subscript, grouping and equality" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectTexts(allocator, db, "SELECT id FROM m ORDER BY c2['key1']", &.{ "1", "3", "2", "5", "4" });
    try expectTexts(allocator, db, "SELECT id FROM m ORDER BY c2['key1'] LIMIT 2, 10", &.{ "2", "5", "4" });
    try expectTexts(allocator, db, "SELECT id FROM m ORDER BY c2['key1'] LIMIT 5, 10", &.{});
    try expectTexts(allocator, db, "SELECT id FROM m WHERE c2['key2'] > 5 ORDER BY id", &.{ "2", "4", "5" });
    try expectTexts(allocator, db, "SELECT id FROM m WHERE c2 = map{'key1': 5, 'key2': 6} ORDER BY id", &.{"2"});
    try expectTexts(allocator, db, "SELECT id FROM m WHERE s = map() ORDER BY id", &.{"2"});
    try expectTexts(allocator, db, "SELECT id FROM m WHERE map_size(c2) >= 4 ORDER BY id", &.{ "4", "5" });
    try expectText(allocator, db, "SELECT map{'a': 1, 'b': 2} = map{'b': 2, 'a': 1}", "0");
    try expectText(allocator, db, "SELECT map{'a': 1} = map{'a': 1.0}", "1");

    try exec(allocator, db, "CREATE TABLE g (id BIGINT PRIMARY KEY, v MAP<STRING, INT>)");
    try exec(allocator, db, "INSERT INTO g VALUES (1, map{'a': 1}), (2, map{'b': 1}), (3, map{'a': 1}), (4, map()), (5, NULL)");
    try expectTexts(allocator, db, "SELECT v FROM g GROUP BY v ORDER BY v", &.{ null, "{}", "{\"a\":1}", "{\"b\":1}" });
    try expectTexts(allocator, db, "SELECT COUNT(*) FROM g GROUP BY v ORDER BY v", &.{ "1", "1", "2", "1" });
}

test "MAP: MAP_AGG skips NULL keys and keeps NULL values" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE g (id BIGINT PRIMARY KEY, k INT, v DOUBLE, s VARCHAR(10))");
    try exec(allocator, db, "INSERT INTO g VALUES (1, 1, 1.5, 'x'), (2, 2, NULL, 'y'), (3, NULL, 3.5, 'x'), (4, 1, 1.5, NULL), (5, 3, 4.5, 'y')");

    try expectText(allocator, db, "SELECT map_size(map_agg(k, v)) FROM g", "3");
    try expectText(allocator, db, "SELECT map_agg(k, v)[1] FROM g", "1.5");
    try expectText(allocator, db, "SELECT map_agg(k, v)[2] FROM g", null);
    try expectText(allocator, db, "SELECT map_size(map_agg(s, id)) FROM g", "2");
    try expectText(allocator, db, "SELECT map_agg(id, s) FROM g WHERE id < 3", "{1:\"x\",2:\"y\"}");
    try expectTexts(allocator, db, "SELECT map_size(map_agg(id, v)) FROM g GROUP BY s ORDER BY s", &.{ "1", "2", "2" });
    try expectText(allocator, db, "SELECT map_agg(k, v) FROM g WHERE id > 100", null);
}

test "MAP: text and empty maps beside typed maps" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "INSERT INTO m VALUES (6, '{\"key1\": 3}', '{7: \"g\"}', '{\"w\": [7]}')");
    try expectTexts(allocator, db, "SELECT c2 FROM m WHERE id = 6", &.{"{\"key1\":3}"});
    try expectTexts(allocator, db, "SELECT s FROM m WHERE id = 6", &.{"{7:\"g\"}"});
    try expectTexts(allocator, db, "SELECT n FROM m WHERE id = 6", &.{"{\"w\":[7]}"});
    try exec(allocator, db, "INSERT INTO m VALUES (7, map(), map{}, map{}), (8, map{'k': 1}, map(1, 'a'), map{'q': [1]})");
    try expectTexts(allocator, db, "SELECT c2 FROM m WHERE id >= 7 ORDER BY id", &.{ "{}", "{\"k\":1}" });
    try expectText(allocator, db,
        \\WITH t0 AS (SELECT c1 FROM (VALUES (map())) AS t(c1))
        \\SELECT map_concat(map('a', 1, 'b', 2), c1) FROM t0
    , "{\"a\":1,\"b\":2}");
    try expectText(allocator, db,
        \\WITH t0 AS (SELECT c1 FROM (VALUES (map())) AS t(c1))
        \\SELECT map_concat(c1, map()) FROM t0
    , "{}");
}

test "MAP: STR_TO_MAP cuts text into entries and each entry into a key and a value" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE sm (id BIGINT PRIMARY KEY, s VARCHAR(40), d VARCHAR(4))");
    try exec(allocator, db, "INSERT INTO sm VALUES (1, 'a:1,b:2', ','), (2, 'a:1;a:2;c', ';'), (3, '7', ','), (4, '', ','), (5, NULL, ','), (6, 'x:1', NULL)");
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("sm", .{})).flush();
        try expectTexts(allocator, db, "SELECT STR_TO_MAP(s, d, ':') FROM sm ORDER BY id", &.{ "{\"a\":\"1\",\"b\":\"2\"}", "{\"a\":\"2\",\"c\":null}", "{\"7\":null}", "{\"\":null}", null, null });
        try expectTexts(allocator, db, "SELECT CAST(CARDINALITY(STR_TO_MAP(s)) AS CHAR) FROM sm ORDER BY id", &.{ "2", "1", "1", "1", null, "1" });
    }
    try expectText(allocator, db, "SELECT STR_TO_MAP('k=v&x=y=z', '&', '=')", "{\"k\":\"v\",\"x\":\"y=z\"}");
    try expectText(allocator, db, "SELECT STR_TO_MAP('ab', '', ':')", "{\"a\":null,\"b\":null}");
    try expectText(allocator, db, "SELECT STR_TO_MAP('a:1', ',', '')", "{\"a:1\":null}");
    try expectText(allocator, db, "SELECT STR_TO_MAP('a:1,b:2')['b']", "2");
}
