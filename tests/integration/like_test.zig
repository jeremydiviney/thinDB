//! LIKE / NOT LIKE pattern matching. `%` matches zero-or-more, `_`
//! matches one, and a backslash (or the ESCAPE character) makes the next
//! character literal. NULL never matches (two-valued logic).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(32) NOT NULL)");
    try exec(
        allocator,
        db,
        "INSERT INTO t (id, name) VALUES (1, 'alpha'), (2, 'alphabet'), (3, 'beta'), (4, 'gamma'), (5, 'al')",
    );
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

test "LIKE: % suffix wildcard" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE name LIKE 'alpha%' ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, ids);
}

test "LIKE: % prefix and middle wildcards" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE name LIKE '%a%a%' ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 4 }, ids);
}

test "LIKE: _ single-char wildcard" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE name LIKE 'al___' ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{1}, ids);
}

test "LIKE: NOT LIKE inverts the match" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE name NOT LIKE 'al%' ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 3, 4 }, ids);
}

test "LIKE: literal pattern with no wildcards = exact match" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE name LIKE 'beta' ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{3}, ids);
}

test "LIKE: matcher unit tests" {
    try std.testing.expect(thindb.exec.predicate.likeMatch("foo", "f%"));
    try std.testing.expect(thindb.exec.predicate.likeMatch("foo", "%o"));
    try std.testing.expect(thindb.exec.predicate.likeMatch("foo", "%"));
    try std.testing.expect(thindb.exec.predicate.likeMatch("foo", "f_o"));
    try std.testing.expect(!thindb.exec.predicate.likeMatch("foo", "f_oo"));
    try std.testing.expect(!thindb.exec.predicate.likeMatch("", "_"));
    try std.testing.expect(thindb.exec.predicate.likeMatch("", "%"));
    try std.testing.expect(thindb.exec.predicate.likeMatch("ababc", "a%c"));
    try std.testing.expect(!thindb.exec.predicate.likeMatch("ababc", "a%d"));
}

test "REGEXP and RLIKE match anywhere in the string" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT id FROM t WHERE name REGEXP '^al' ORDER BY id", &[_]i64{ 1, 2, 5 } },
        .{ "SELECT id FROM t WHERE name RLIKE 'ta$' ORDER BY id", &[_]i64{3} },
        .{ "SELECT id FROM t WHERE name regexp 'mm' ORDER BY id", &[_]i64{4} },
        .{ "SELECT id FROM t WHERE name NOT REGEXP 'a$' ORDER BY id", &[_]i64{ 2, 5 } },
        .{ "SELECT id FROM t WHERE NOT name RLIKE '^al' ORDER BY id", &[_]i64{ 3, 4 } },
        .{ "SELECT id FROM t WHERE UPPER(name) REGEXP '^AL.+T$' ORDER BY id", &[_]i64{2} },
        .{ "SELECT id FROM t WHERE name REGEXP CONCAT('^', 'g') OR id = 5 ORDER BY id", &[_]i64{ 4, 5 } },
        .{ "SELECT id FROM t WHERE id > 1 AND name REGEXP 'b' ORDER BY id", &[_]i64{ 2, 3 } },
    };
    inline for (cases) |case| {
        const ids = try collectBigints(allocator, db, case[0]);
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(i64, case[1], ids);
    }
}

test "REGEXP functions read each row's own pattern, replacement and match type" {
    // They read row 0's and applied it to the whole batch, so a pattern
    // column matched every row against its first pattern.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE rp (id BIGINT PRIMARY KEY, s VARCHAR(16), p VARCHAR(16), r VARCHAR(8), m VARCHAR(4))");
    try exec(allocator, db,
        \\INSERT INTO rp VALUES (1, 'abc', 'b', '-', 'c'), (2, 'xyz', 'y', '+', 'c'), (3, 'xyz', 'q', '-', 'c'),
        \\  (4, 'XYZ', 'y', '*', 'i'), (5, 'abc', NULL, '-', 'c'), (6, 'aBc', 'b', NULL, NULL), (7, 'abcb', 'b', '=', 'c')
    );
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("rp", .{})).flush();
        try helpers.expectTexts(allocator, db, "SELECT CAST(REGEXP_LIKE(s, p, m) AS CHAR) FROM rp ORDER BY id", &.{ "1", "1", "0", "1", null, null, "1" });
        try helpers.expectTexts(allocator, db, "SELECT CAST(REGEXP_LIKE(s, p) AS CHAR) FROM rp ORDER BY id", &.{ "1", "1", "0", "0", null, "0", "1" });
        try helpers.expectTexts(allocator, db, "SELECT REGEXP_REPLACE(s, p, r) FROM rp ORDER BY id", &.{ "a-c", "x+z", "xyz", "XYZ", null, null, "a=c=" });
        try helpers.expectTexts(allocator, db, "SELECT CAST(REGEXP_INSTR(s, p) AS CHAR) FROM rp ORDER BY id", &.{ "2", "2", "0", "0", null, "0", "2" });
        try helpers.expectTexts(allocator, db, "SELECT REGEXP_SUBSTR(s, p, 1, 1, m) FROM rp ORDER BY id", &.{ "b", "y", null, "Y", null, null, "b" });
        try helpers.expectTexts(allocator, db, "SELECT CAST(id AS CHAR) FROM rp WHERE s REGEXP p ORDER BY id", &.{ "1", "2", "7" });
    }
}

test "REGEXP functions read a constant argument once for every row" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE rc (id BIGINT PRIMARY KEY, s VARCHAR(16), p VARCHAR(16), r VARCHAR(8), m VARCHAR(4))");
    try exec(allocator, db,
        \\INSERT INTO rc VALUES (1, 'abc', 'b', '-', 'c'), (2, 'xyz', 'y', '+', 'c'), (3, 'xyz', 'q', '-', 'c'),
        \\  (4, 'XYZ', 'y', '*', 'i'), (5, 'abc', NULL, '-', 'c'), (6, 'aBc', 'b', NULL, NULL), (7, 'abcb', 'b', '=', 'c')
    );
    const all_null = [_]?[]const u8{null} ** 7;
    try helpers.expectTexts(allocator, db, "SELECT REGEXP_REPLACE(s, 'b', '-') FROM rc ORDER BY id", &.{ "a-c", "xyz", "xyz", "XYZ", "a-c", "aBc", "a-c-" });
    try helpers.expectTexts(allocator, db, "SELECT REGEXP_REPLACE(s, 'b', r) FROM rc ORDER BY id", &.{ "a-c", "xyz", "xyz", "XYZ", "a-c", null, "a=c=" });
    try helpers.expectTexts(allocator, db, "SELECT REGEXP_REPLACE('abc', p, r) FROM rc ORDER BY id", &.{ "a-c", "abc", "abc", "abc", null, null, "a=c" });
    try helpers.expectTexts(allocator, db, "SELECT REGEXP_REPLACE(s, 'B', '_', 1, 0, 'i') FROM rc ORDER BY id", &.{ "a_c", "xyz", "xyz", "XYZ", "a_c", "a_c", "a_c_" });
    try helpers.expectTexts(allocator, db, "SELECT REGEXP_REPLACE(s, '^(a)', '<\\1>') FROM rc ORDER BY id", &.{ "<a>bc", "xyz", "xyz", "XYZ", "<a>bc", "<a>Bc", "<a>bcb" });
    try helpers.expectTexts(allocator, db, "SELECT REGEXP_REPLACE(s, CONCAT('b', ''), LOWER('X')) FROM rc ORDER BY id", &.{ "axc", "xyz", "xyz", "XYZ", "axc", "aBc", "axcx" });
    try helpers.expectTexts(allocator, db, "SELECT REGEXP_REPLACE(s, NULL, '-') FROM rc ORDER BY id", &all_null);
    try helpers.expectTexts(allocator, db, "SELECT CAST(REGEXP_LIKE(s, 'y') AS CHAR) FROM rc ORDER BY id", &.{ "0", "1", "1", "0", "0", "0", "0" });
    try helpers.expectTexts(allocator, db, "SELECT CAST(REGEXP_LIKE(s, 'Y', m) AS CHAR) FROM rc ORDER BY id", &.{ "0", "0", "0", "1", "0", null, "0" });
    try helpers.expectTexts(allocator, db, "SELECT CAST(REGEXP_LIKE(s, p, 'i') AS CHAR) FROM rc ORDER BY id", &.{ "1", "1", "0", "1", null, "1", "1" });
    try helpers.expectTexts(allocator, db, "SELECT CAST(REGEXP_INSTR(s, 'b', 1, 2) AS CHAR) FROM rc ORDER BY id", &.{ "0", "0", "0", "0", "0", "0", "4" });
    try helpers.expectTexts(allocator, db, "SELECT REGEXP_SUBSTR(s, '[a-z]', 2) FROM rc ORDER BY id", &.{ "b", "y", "y", null, "b", "c", "b" });
    try helpers.expectTexts(allocator, db, "SELECT REGEXP_SUBSTR(s, 'b', NULL) FROM rc ORDER BY id", &all_null);
    try helpers.expectTexts(allocator, db, "SELECT CAST(REGEXP_COUNT(s, 'b') AS CHAR) FROM rc ORDER BY id", &.{ "1", "0", "0", "0", "1", "0", "2" });
    try helpers.expectTexts(allocator, db, "SELECT REGEXP_EXTRACT(s, '([a-z])([a-z])', 2) FROM rc ORDER BY id", &.{ "b", "y", "y", "", "b", "", "b" });
    try std.testing.expectError(error.RegexInvalidPattern, exec(allocator, db, "SELECT REGEXP_REPLACE(s, '(a', '-') FROM rc"));
    // An anchored pattern matches only once, at the start or, multiline,
    // at each line's start.
    try helpers.expectText(allocator, db, "SELECT REGEXP_REPLACE('aaa', '^a', 'X')", "Xaa");
    try helpers.expectText(allocator, db, "SELECT REGEXP_REPLACE('aaa', '^a', 'X', 1, 2)", "aaa");
    try helpers.expectText(allocator, db, "SELECT REGEXP_REPLACE('a\na', '^a', 'X', 1, 0, 'm')", "X\nX");
}

test "REGEXP_REPLACE over a text column alone keeps NULL texts NULL and repeats adjacent results" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE rn (id BIGINT PRIMARY KEY, s VARCHAR(16))");
    try exec(allocator, db, "INSERT INTO rn VALUES (1, 'abcb'), (2, NULL), (3, 'abcb'), (4, 'abcb'), (5, NULL), (6, 'b'), (7, '')");
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("rn", .{})).flush();
        try helpers.expectTexts(allocator, db, "SELECT REGEXP_REPLACE(s, 'b', '-') FROM rn ORDER BY id", &.{ "a-c-", null, "a-c-", "a-c-", null, "-", "" });
        try helpers.expectTexts(allocator, db, "SELECT REGEXP_REPLACE(s, 'b', '-', 1, 2) FROM rn ORDER BY id", &.{ "abc-", null, "abc-", "abc-", null, "b", "" });
        try helpers.expectTexts(allocator, db, "SELECT REGEXP_REPLACE(s, '^a(b)', '<\\1\\1>', 1) FROM rn ORDER BY id", &.{ "<bb>cb", null, "<bb>cb", "<bb>cb", null, "b", "" });
    }
}

test "REGEXP_COUNT, REGEXP_EXTRACT and REGEXP_EXTRACT_ALL count and cut out every match" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE rx (id BIGINT PRIMARY KEY, s VARCHAR(20), p VARCHAR(40), g BIGINT)");
    try exec(allocator, db,
        \\INSERT INTO rx VALUES (1, 'AbCdExCeF', '([[:lower:]]+)C([[:lower:]]+)', 1), (2, 'a1b22c333', '[0-9]+', 0),
        \\  (3, 'abc', 'z', 0), (4, 'abc', '', 0), (5, NULL, 'a', 0), (6, 'abc', NULL, 0), (7, 'AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 3),
        \\  (8, 'ab', 'a|(x)', 1)
    );
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("rx", .{})).flush();
        try helpers.expectTexts(allocator, db, "SELECT CAST(REGEXP_COUNT(s, p) AS CHAR) FROM rx ORDER BY id", &.{ "2", "3", "0", "4", null, null, "1", "1" });
        try helpers.expectTexts(allocator, db, "SELECT REGEXP_EXTRACT(s, p, g) FROM rx ORDER BY id", &.{ "b", "1", "", "", null, null, "", "" });
        try helpers.expectTexts(allocator, db, "SELECT REGEXP_EXTRACT_ALL(s, p, g) FROM rx ORDER BY id", &.{ "[\"b\",\"x\"]", "[\"1\",\"22\",\"333\"]", "[]", "[\"\",\"\",\"\",\"\"]", null, null, "[]", "[\"\"]" });
    }
    try helpers.expectText(allocator, db, "SELECT REGEXP_EXTRACT_ALL('AbCdExCeF', '([[:lower:]]+)C([[:lower:]]+)', 2)", "[\"d\",\"e\"]");
    try helpers.expectText(allocator, db, "SELECT REGEXP_EXTRACT('AbCdExCeF', '([[:lower:]]+)C([[:lower:]]+)', 0)", "bCd");
    try std.testing.expectError(error.RegexInvalidPattern, exec(allocator, db, "SELECT REGEXP_COUNT('abc', '(a')"));
    try std.testing.expectError(error.RegexInvalidPattern, exec(allocator, db, "SELECT REGEXP_REPLACE('abc中文def', '[\\p{Han}]+', 'xx')"));
}

test "LIKE: backslash escapes by default, ESCAPE names another character" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    // Neutral-dialect strings keep a backslash as written: row 4 is a\b.
    try exec(allocator, db, "CREATE TABLE lk (id BIGINT PRIMARY KEY, s VARCHAR(16))");
    try exec(allocator, db, "INSERT INTO lk VALUES (1, 'a%b'), (2, 'axb'), (3, 'a_b'), (4, 'a\\b')");

    const cases = .{
        .{ "s LIKE 'a|%b' ESCAPE '|'", &[_]i64{1} },
        .{ "s LIKE 'a\\%b'", &[_]i64{1} },
        .{ "s LIKE 'a\\_b'", &[_]i64{3} },
        .{ "s LIKE 'a_b'", &[_]i64{ 1, 2, 3, 4 } },
        .{ "s LIKE 'a\\\\b'", &[_]i64{4} },
        .{ "s LIKE 'a\\b' ESCAPE ''", &[_]i64{4} },
        .{ "s LIKE 'a\\b' ESCAPE '|'", &[_]i64{4} },
        .{ "s NOT LIKE 'a!_b' ESCAPE '!'", &[_]i64{ 1, 2, 4 } },
        .{ "s LIKE '%\\%%'", &[_]i64{1} },
        .{ "'a%b' LIKE 'a|%b' ESCAPE '|' AND id = 2", &[_]i64{2} },
    };
    inline for (cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        const ids = try collectBigints(allocator, db, "SELECT id FROM lk WHERE " ++ c[0] ++ " ORDER BY id");
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(i64, c[1], ids);
    }

    // MySQL string literals keep \% and \_ for LIKE, and \\ is one
    // backslash, so 'a\\\\b' is the pattern a\\b.
    const mysql_cases = .{
        .{ "SELECT id FROM lk WHERE s LIKE 'a\\%b'", &[_]i64{1} },
        .{ "SELECT id FROM lk WHERE s LIKE 'a\\_b'", &[_]i64{3} },
        .{ "SELECT id FROM lk WHERE s LIKE 'a\\\\\\\\b'", &[_]i64{4} },
    };
    inline for (mysql_cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        var q = try helpers.runSqlMysql(allocator, db, c[0]);
        defer q.deinit();
        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint[0..batch.row_count]);
        try std.testing.expectEqualSlices(i64, c[1], ids.items);
    }
}

test "LIKE: a pattern expression matches per row" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE pt (id BIGINT PRIMARY KEY, s VARCHAR(16), pat VARCHAR(16))");
    try exec(
        allocator,
        db,
        "INSERT INTO pt VALUES (1, 'apple', 'a%'), (2, 'banana', '%nan%'), (3, 'cherry', 'ch_rry'), (4, 'date', NULL), (5, NULL, '%'), (6, 'fig', 'g%')",
    );

    const cases = .{
        .{ "SELECT id FROM t WHERE LOWER(name) LIKE LOWER('%ALPHA%') ORDER BY id", &[_]i64{ 1, 2 } },
        .{ "SELECT id FROM t WHERE name LIKE CONCAT('%', 'mm', '%') ORDER BY id", &[_]i64{4} },
        .{ "SELECT id FROM t WHERE name NOT LIKE LOWER('AL%') ORDER BY id", &[_]i64{ 3, 4 } },
        .{ "SELECT id FROM t WHERE UPPER(name) LIKE UPPER('%ta') OR id = 5 ORDER BY id", &[_]i64{ 3, 5 } },
        .{ "SELECT id FROM t WHERE name LIKE NULL ORDER BY id", &[_]i64{} },
        .{ "SELECT id FROM t WHERE name NOT LIKE NULL ORDER BY id", &[_]i64{} },
        .{
            "WITH c AS (SELECT id, name FROM t WHERE id > 0 AND (LOWER(name) LIKE LOWER('%BET%') OR LOWER(name) LIKE LOWER('%MM%'))) SELECT id FROM c ORDER BY id",
            &[_]i64{ 2, 3, 4 },
        },
        .{ "SELECT id FROM pt WHERE s LIKE pat ORDER BY id", &[_]i64{ 1, 2, 3 } },
        .{ "SELECT id FROM pt WHERE s NOT LIKE pat ORDER BY id", &[_]i64{6} },
        .{ "SELECT id FROM t WHERE name LIKE LOWER('AL%') ESCAPE '!' ORDER BY id", &[_]i64{ 1, 2, 5 } },
        .{ "SELECT id FROM t WHERE name LIKE LOWER('AL!%') ESCAPE '!' ORDER BY id", &[_]i64{} },
    };
    inline for (cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        const ids = try collectBigints(allocator, db, c[0]);
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(i64, c[1], ids);
    }
    try helpers.expectRunError(allocator, db, "SELECT id FROM pt WHERE s LIKE pat ESCAPE '!'", error.SqlExpectedValue);
}

test "LIKE: like(s, pattern) is the operator's function form" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    try helpers.expectText(allocator, db, "SELECT like('abc', 'a%')", "1");
    try helpers.expectText(allocator, db, "SELECT like('abc', 'b%')", "0");
    try helpers.expectText(allocator, db, "SELECT COUNT(*) FROM (SELECT 'abc' AS s) t WHERE like(s, '%c')", "1");
    // After a name, LIKE is still the operator.
    try helpers.expectText(allocator, db, "SELECT mod LIKE 'a%' FROM (SELECT 'abc' AS mod) t", "1");
}
