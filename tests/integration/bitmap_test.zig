//! BITMAP end-to-end: BITMAP columns store, flush and reopen; the bitmap
//! functions and aggregates, checked against StarRocks' answers (its
//! `test_bitmap_functions` and `test_bitmap_agg` suites) and its bytes.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const expectTexts = helpers.expectTexts;
const expectText = helpers.expectText;

const ONE_TO_40 = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40";

test "BITMAP: columns store, flush and reopen" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const want = [_]?[]const u8{ "1", "1,2,3", null, "", "0,17179869184,18446744073709551615", ONE_TO_40 };
    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        try exec(allocator, db, "CREATE TABLE b (id BIGINT PRIMARY KEY, bm BITMAP)");
        try exec(allocator, db, "INSERT INTO b VALUES (1, to_bitmap(1)), (2, bitmap_from_string('3,1,2')), (3, NULL), (4, bitmap_empty()), (5, bitmap_from_string('18446744073709551615,0,17179869184'))");
        try exec(allocator, db, "INSERT INTO b SELECT 6, bitmap_agg(x) FROM generate_series(1, 40) AS t(x)");
        try expectTexts(allocator, db, "SELECT bitmap_to_string(bm) FROM b ORDER BY id", &want);
        const t = try db.openTable("b", .{});
        try t.flush();
    }
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try expectTexts(allocator, db, "SELECT bitmap_to_string(bm) FROM b ORDER BY id", &want);
    try expectTexts(allocator, db, "SELECT bitmap_count(bm) FROM b ORDER BY id", &.{ "1", "3", "0", "0", "3", "40" });
    try expectText(allocator, db, "SELECT typeof(bm) FROM b WHERE id = 1", "bitmap");
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_union(bm)) FROM b", "0," ++ ONE_TO_40 ++ ",17179869184,18446744073709551615");
    try expectText(allocator, db, "SELECT bitmap_union_count(bm) FROM b", "43");
    // A bitmap from outside is stored in the one layout its set has, so
    // equal sets group together.
    try exec(allocator, db, "INSERT INTO b VALUES (7, bitmap_from_binary(unhex('023A300000010000000000020010000000010002000300')))");
    try expectText(allocator, db, "SELECT CAST(COUNT(DISTINCT bm) AS STRING) FROM b WHERE id IN (2, 7)", "1");
}

test "BITMAP: DDL refuses a default and nesting" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try std.testing.expectError(error.SqlInvalidDefault, exec(allocator, db, "CREATE TABLE d (id INT, bm BITMAP DEFAULT '')"));
    try std.testing.expectError(error.SqlExpectedKeyword, exec(allocator, db, "CREATE TABLE d (id INT, a ARRAY<BITMAP>)"));
    try std.testing.expectError(error.SqlExpectedKeyword, exec(allocator, db, "CREATE TABLE d (id INT, m MAP<INT, BITMAP>)"));
    try std.testing.expectError(error.SqlExpectedKeyword, exec(allocator, db, "CREATE TABLE d (id INT, s STRUCT<a BITMAP>)"));
    try std.testing.expectError(error.SqlExpectedKeyword, exec(allocator, db, "CREATE TABLE d (id INT, a BITMAP[])"));
    try exec(allocator, db, "CREATE TABLE d (id INT, bm BITMAP NOT NULL)");
    try std.testing.expectError(error.SqlInvalidDefault, exec(allocator, db, "ALTER TABLE d ADD COLUMN bm2 BITMAP DEFAULT ''"));
}

test "BITMAP: building, reading and set algebra" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try expectText(allocator, db, "SELECT bitmap_to_string(to_bitmap('42'))", "42");
    try expectText(allocator, db, "SELECT bitmap_to_string(to_bitmap('abc'))", null);
    try expectText(allocator, db, "SELECT bitmap_to_string(to_bitmap(-1))", null);
    try expectText(allocator, db, "SELECT bitmap_to_string(to_bitmap(true))", "1");
    try expectText(allocator, db, "SELECT bitmap_to_string(to_bitmap(CAST('18446744073709551615' AS LARGEINT)))", "18446744073709551615");
    try expectText(allocator, db, "SELECT bitmap_to_string(to_bitmap(CAST('18446744073709551616' AS LARGEINT)))", null);
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_from_string('1,,2, 3'))", "1,2,3");
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_from_string('1,a'))", null);
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_from_string('-1'))", null);
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_from_string(''))", "");
    try expectText(allocator, db, "SELECT bitmap_to_string(NULL)", null);
    try expectText(allocator, db, "SELECT typeof(to_bitmap(1))", "bitmap");

    try expectText(allocator, db, "SELECT bitmap_count(bitmap_from_string('1,2,3,1'))", "3");
    try expectText(allocator, db, "SELECT bitmap_count(NULL)", "0");
    try expectText(allocator, db, "SELECT bitmap_min(bitmap_from_string('5,3,18446744073709551615'))", "3");
    try expectText(allocator, db, "SELECT bitmap_max(bitmap_from_string('5,3,18446744073709551615'))", "18446744073709551615");
    try expectText(allocator, db, "SELECT bitmap_min(bitmap_empty())", null);
    try expectText(allocator, db, "SELECT bitmap_contains(bitmap_from_string('1,2'), 2)", "1");
    try expectText(allocator, db, "SELECT bitmap_contains(bitmap_from_string('1,2'), 3)", "0");
    try expectText(allocator, db, "SELECT bitmap_contains(bitmap_from_string('1,2'), -1)", "0");
    try expectText(allocator, db, "SELECT bitmap_contains(NULL, 1)", null);
    try expectText(allocator, db, "SELECT bitmap_has_any(bitmap_from_string('1,2'), bitmap_from_string('2,3'))", "1");
    try expectText(allocator, db, "SELECT bitmap_has_any(bitmap_from_string('1,2'), bitmap_from_string('3'))", "0");

    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_or(bitmap_from_string('1,2,3'), bitmap_from_string('2,3,4')))", "1,2,3,4");
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_and(bitmap_from_string('1,2,3'), bitmap_from_string('2,3,4')))", "2,3");
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_xor(bitmap_from_string('1,2,3'), bitmap_from_string('2,3,4')))", "1,4");
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_andnot(bitmap_from_string('1,2,3'), bitmap_from_string('2,3,4')))", "1");
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_and(bitmap_from_string('1'), bitmap_from_string('2')))", "");
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_or(NULL, bitmap_from_string('1')))", null);
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_remove(bitmap_from_string('1,2,3'), 2))", "1,3");
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_remove(bitmap_from_string('1,2,3'), -5))", "1,2,3");

    try expectText(allocator, db, "SELECT bitmap_to_array(bitmap_from_string('3,1,2'))", "[1,2,3]");
    try expectText(allocator, db, "SELECT bitmap_to_array(bitmap_empty())", "[]");
    try expectText(allocator, db, "SELECT bitmap_to_array(NULL)", null);
    try std.testing.expectError(error.ValueOutOfRange, exec(allocator, db, "SELECT bitmap_to_array(bitmap_from_string('18446744073709551615'))"));
    try expectText(allocator, db, "SELECT bitmap_to_string(array_to_bitmap([3, 1, NULL, -2, 1]))", "1,3");
    try expectText(allocator, db, "SELECT bitmap_to_array(array_to_bitmap(CAST('[40360,40361]' AS ARRAY<INT>)))", "[40360,40361]");
    try expectText(allocator, db, "SELECT bitmap_to_string(array_to_bitmap(NULL))", null);

    try std.testing.expectError(error.PredicateTypeMismatch, exec(allocator, db, "SELECT to_bitmap(1) = to_bitmap(1)"));
}

test "BITMAP: subsets by value and by rank" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const cases = .{
        .{ "sub_bitmap(bitmap_from_string(''), 0, 3)", null },
        .{ "sub_bitmap(bitmap_from_string('1'), 0, 3)", "1" },
        .{ "sub_bitmap(bitmap_from_string('1'), -1, 3)", "1" },
        .{ "sub_bitmap(bitmap_from_string('1'), 1, 3)", null },
        .{ "sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), 0, 3)", "0,1,2" },
        .{ "sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), 3, 6)", "3,5" },
        .{ "sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), -2, 1)", "3" },
        .{ "sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), -9, 1)", null },
        .{ "sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), 0, 0)", null },
        .{ "bitmap_subset_limit(bitmap_from_string(''), 0, 3)", null },
        .{ "bitmap_subset_limit(bitmap_from_string('1'), -1, 3)", "1" },
        .{ "bitmap_subset_limit(bitmap_from_string('1'), 1, 3)", "1" },
        .{ "bitmap_subset_limit(bitmap_from_string('1,0,1,2,3,1,5'), 0, 3)", "0,1,2" },
        .{ "bitmap_subset_limit(bitmap_from_string('1,0,1,2,3,1,5'), 3, 6)", "3,5" },
        .{ "bitmap_subset_limit(bitmap_from_string('1,0,1,2,3,1,5'), 0, -3)", "0" },
        .{ "bitmap_subset_limit(bitmap_from_string('1,0,1,2,3,1,5'), 3, -6)", "0,1,2,3" },
        .{ "bitmap_subset_limit(bitmap_from_string('1,0,1,2,3,1,5'), 4, -2)", "2,3" },
        .{ "bitmap_subset_in_range(bitmap_from_string(''), 0, 3)", null },
        .{ "bitmap_subset_in_range(bitmap_from_string('1'), -1, 3)", "1" },
        .{ "bitmap_subset_in_range(bitmap_from_string('1'), 1, 3)", "1" },
        .{ "bitmap_subset_in_range(bitmap_from_string('1,0,1,2,3,1,5'), 0, 3)", "0,1,2" },
        .{ "bitmap_subset_in_range(bitmap_from_string('1,0,1,2,3,1,5'), 3, 6)", "3,5" },
        .{ "bitmap_subset_in_range(bitmap_from_string('1,0,1,2,3,1,5'), 3, 0)", null },
        .{ "bitmap_subset_in_range(bitmap_from_string('1,0,1,2,3,1,5'), 4, 5)", null },
    };
    inline for (cases) |c| try expectText(allocator, db, "SELECT bitmap_to_string(" ++ c[0] ++ ")", c[1]);
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_subset_in_range(bitmap_from_string(group_concat(CAST(x AS STRING), ',')), 0, 3)) FROM generate_series(1, 64, 1) AS t(x)", "1,2");
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_subset_in_range(bitmap_from_string(group_concat(CAST(x AS STRING), ',')), 10, 3)) FROM generate_series(1, 64, 1) AS t(x)", null);
}

test "BITMAP: StarRocks' bytes and base64" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try expectText(allocator, db, "SELECT hex(bitmap_to_binary(bitmap_empty()))", "00");
    try expectText(allocator, db, "SELECT hex(bitmap_to_binary(to_bitmap(1)))", "0101000000");
    try expectText(allocator, db, "SELECT hex(bitmap_to_binary(to_bitmap(17179869184)))", "030000000004000000");
    try expectText(allocator, db, "SELECT hex(bitmap_to_binary(bitmap_agg(x))) FROM generate_series(1, 5) AS t(x)", "0A0500000001000000000000000200000000000000030000000000000004000000000000000500000000000000");
    try expectText(allocator, db, "SELECT hex(bitmap_to_binary(bitmap_agg(x))) FROM generate_series(1, 40) AS t(x)", "023B3000000100002700010001002700");
    try expectText(allocator, db, "SELECT hex(bitmap_to_binary(bitmap_agg(x))) FROM generate_series(1, 80) AS t(x)", "023B3000000100004F00010001004F00");
    try expectText(allocator, db, "SELECT hex(bitmap_to_binary(bitmap_agg(x))) FROM (SELECT x FROM generate_series(1, 200) AS a(x) UNION ALL SELECT x FROM generate_series(900, 910) AS b(x)) u", "023B300000010000D20002000100C70084030A00");
    try expectText(allocator, db, "SELECT hex(bitmap_to_binary(bitmap_agg(x))) FROM (SELECT x FROM generate_series(1, 20) AS a(x) UNION ALL SELECT x FROM generate_series(17179869184, 17179869284) AS b(x)) u", "0402000000003B3000000100001300010001001300040000003B3000000100006400010000006400");
    // StarRocks writes this union with its first bucket unencoded.
    try expectText(allocator, db, "SELECT bitmap_count(bitmap_from_binary(unhex('0402000000003A3000000100000000001300100000000100020003000400050006000700080009000A000B000C000D000E000F0010001100120013001400040000003B3000000100006400010000006400')))", "121");
    try expectText(allocator, db, "SELECT hex(bitmap_to_binary(bitmap_from_binary(unhex('023A300000010000000000020010000000010002000300'))))", "0A03000000010000000000000002000000000000000300000000000000");
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_from_binary('1234'))", null);
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_from_binary(''))", null);
    try expectText(allocator, db, "SELECT bitmap_to_binary(NULL)", null);

    try expectText(allocator, db, "SELECT bitmap_to_string(base64_to_bitmap('AjowAAABAAAAAAACABAAAAABAAIAAwA='))", "1,2,3");
    try expectText(allocator, db, "SELECT bitmap_to_base64(bitmap_from_string('1,2,3'))", "CgMAAAABAAAAAAAAAAIAAAAAAAAAAwAAAAAAAAA=");
    try expectText(allocator, db, "SELECT bitmap_to_base64(bitmap_from_string('abc'))", null);
    try expectText(allocator, db, "SELECT bitmap_to_string(base64_to_bitmap('not base64'))", null);
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_hash('abc'))", "3409700625");
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_hash(42))", "3939580941");
}

test "BITMAP: aggregates" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t1 (c1 INT, c2 BOOLEAN, c3 TINYINT, c4 INT, c5 BIGINT, c6 LARGEINT, c7 STRING)");
    try exec(allocator, db,
        \\INSERT INTO t1 VALUES
        \\ (1, true, 11, 111, 1111, 11111, '111111'),
        \\ (2, false, 22, 222, 2222, 22222, '222222'),
        \\ (3, true, 33, 333, 3333, 33333, '333333'),
        \\ (4, NULL, NULL, NULL, NULL, NULL, NULL),
        \\ (5, true, -11, -111, -1111, -11111, '-111111'),
        \\ (6, NULL, NULL, NULL, NULL, CAST('36893488147419103232' AS LARGEINT), '680564733841876926926749214863536422912')
    );
    const by_column = .{
        .{ "c2", "0,1" },
        .{ "c3", "11,22,33" },
        .{ "c4", "111,222,333" },
        .{ "c5", "1111,2222,3333" },
        .{ "c6", "11111,22222,33333" },
        .{ "c7", "111111,222222,333333" },
    };
    inline for (by_column) |c| {
        try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_agg(" ++ c[0] ++ ")) FROM t1", c[1]);
        try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_union(to_bitmap(" ++ c[0] ++ "))) FROM t1", c[1]);
    }
    try expectTexts(allocator, db, "SELECT bitmap_to_string(bitmap_union(to_bitmap(c3))) FROM t1 GROUP BY c1 ORDER BY c1", &.{ "11", "22", "33", null, null, null });
    try expectTexts(allocator, db, "SELECT bitmap_to_string(bitmap_agg(c4)) FROM t1 GROUP BY c1 ORDER BY c1", &.{ "111", "222", "333", null, null, null });
    try expectTexts(allocator, db, "SELECT bitmap_union_count(to_bitmap(c4)) FROM t1 GROUP BY c1 ORDER BY c1", &.{ "1", "1", "1", "0", "0", "0" });
    try expectText(allocator, db, "SELECT bitmap_union_count(to_bitmap(c4)) FROM t1 WHERE c1 > 100", "0");
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_agg(c4)) FROM t1 WHERE c1 > 100", null);

    try exec(allocator, db, "CREATE TABLE g (k INT, bm BITMAP)");
    try exec(allocator, db, "INSERT INTO g VALUES (1, bitmap_from_string('1,2,3')), (1, bitmap_from_string('2,3,4')), (1, NULL), (2, to_bitmap(5)), (3, NULL)");
    try expectTexts(allocator, db, "SELECT bitmap_to_string(bitmap_intersect(bm)) FROM g GROUP BY k ORDER BY k", &.{ "2,3", "5", null });
    try expectText(allocator, db, "SELECT bitmap_to_string(bitmap_intersect(bm)) FROM g", "");
    try expectTexts(allocator, db, "SELECT bitmap_to_string(bitmap_union(bm)) FROM g GROUP BY k ORDER BY k", &.{ "1,2,3,4", "5", null });

    try expectText(allocator, db, "SELECT bitmap_count(bitmap_agg(x)) FROM generate_series(1, 100000) AS t(x)", "100000");
    try expectText(allocator, db, "SELECT bitmap_count(bitmap_agg(x * 65537)) FROM generate_series(1, 100000) AS t(x)", "100000");
    try expectText(allocator, db, "SELECT bitmap_union_count(to_bitmap(x % 1000)) FROM generate_series(1, 100000) AS t(x)", "1000");
    try expectText(allocator, db, "SELECT bitmap_count(bitmap_union(bitmap_hash(CAST(x AS STRING)))) FROM generate_series(1, 20000) AS t(x)", "20000");
    try expectText(allocator, db, "SELECT bitmap_union_int(x % 7) FROM generate_series(1, 100) AS t(x)", "7");
}
