//! ARRAY type end-to-end: ARRAY<T> columns store, flush and reopen; array
//! literals, subscripts and CAST; and the array functions, each checked by
//! its result's StarRocks text.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;
const freeStrings = helpers.freeStrings;

/// The first column of `sql`'s result as text, NULL as null: an array as
/// StarRocks prints it, anything else as `helpers.columnText` does.
fn texts(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]?[]u8 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    const t = q.outputSchema()[0].type;
    if (!t.isNested()) return helpers.columnText(allocator, &q);
    var out: std.ArrayList(?[]u8) = .empty;
    errdefer {
        for (out.items) |v| if (v) |x| allocator.free(x);
        out.deinit(allocator);
    }
    while (try q.next()) |batch| {
        const col = batch.values[0];
        for (0..batch.row_count) |row| {
            const text: ?[]u8 = if (!col.isValid(row)) null else blk: {
                var buf: std.ArrayList(u8) = .empty;
                errdefer buf.deinit(allocator);
                try thindb.exec.nested.appendText(allocator, &buf, t, col.data.nested.rowBytes(row), .starrocks);
                break :blk try buf.toOwnedSlice(allocator);
            };
            errdefer if (text) |x| allocator.free(x);
            try out.append(allocator, text);
        }
    }
    return out.toOwnedSlice(allocator);
}

fn expectTexts(allocator: std.mem.Allocator, db: anytype, sql: []const u8, expected: []const ?[]const u8) !void {
    const got = try texts(allocator, db, sql);
    defer freeStrings(allocator, got);
    errdefer std.debug.print("query: {s}\n", .{sql});
    try std.testing.expectEqual(expected.len, got.len);
    for (expected, got) |e, g| {
        if (e == null or g == null) {
            if (e != null or g != null) std.debug.print("expected {?s}, got {?s}\n", .{ e, g });
            try std.testing.expect(e == null and g == null);
            continue;
        }
        try std.testing.expectEqualStrings(e.?, g.?);
    }
}

fn expectText(allocator: std.mem.Allocator, db: anytype, sql: []const u8, expected: ?[]const u8) !void {
    try expectTexts(allocator, db, sql, &.{expected});
}

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
    try expectText(allocator, db, "SELECT array_agg(DISTINCT v) FROM g", "[null,10,20,30]");
    try expectText(allocator, db, "SELECT array_agg(s ORDER BY s DESC, id) FROM g", "[\"y\",\"x\",\"x\",\"x\",null]");
    try expectText(allocator, db, "SELECT array_agg(a ORDER BY id) FROM g", "[[1],[2,3],null,[],[1]]");
    try expectText(allocator, db, "SELECT array_agg(v) FROM g WHERE id > 100", null);
    try expectText(allocator, db, "SELECT array_length(array_agg(v)) FROM g", "5");
    try expectText(allocator, db, "SELECT array_sort(array_agg(v)) FROM g", "[null,10,10,20,30]");
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
