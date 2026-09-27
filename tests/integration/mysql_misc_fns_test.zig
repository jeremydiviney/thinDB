//! MySQL miscellaneous scalar functions (issue #245): ELT, INSERT, QUOTE,
//! SOUNDEX / SOUNDS LIKE, the INET family, INTERVAL(N, ...), SLEEP, the
//! REGEXP_* match-type and position arguments, the JSON constructors and
//! aggregates, LAST_INSERT_ID() and ROW_COUNT(). Expected values are MySQL
//! 8.4 output for the same statements.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

const Case = struct { sql: []const u8, want: ?[]const u8 };

const scalar_cases = [_]Case{
    .{ .sql = "ELT(1, 'a', 'b')", .want = "a" },
    .{ .sql = "ELT(2, 'a', 'b')", .want = "b" },
    .{ .sql = "ELT(3, 'a', 'b')", .want = null },
    .{ .sql = "ELT(0, 'a')", .want = null },
    .{ .sql = "ELT(-1, 'a')", .want = null },
    .{ .sql = "ELT(NULL, 'a')", .want = null },
    .{ .sql = "ELT(1.5, 'a', 'b')", .want = "b" },
    .{ .sql = "ELT(2, 'a', NULL)", .want = null },
    .{ .sql = "ELT(1, 'héllo', 'x')", .want = "héllo" },
    .{ .sql = "INSERT('abcdef', 2, 3, 'XY')", .want = "aXYef" },
    .{ .sql = "INSERT('abcdef', 0, 3, 'XY')", .want = "abcdef" },
    .{ .sql = "INSERT('abcdef', 7, 3, 'XY')", .want = "abcdef" },
    .{ .sql = "INSERT('abcdef', 6, 3, 'XY')", .want = "abcdeXY" },
    .{ .sql = "INSERT('abcdef', 2, -1, 'XY')", .want = "aXY" },
    .{ .sql = "INSERT('abcdef', 1, 100, '')", .want = "" },
    .{ .sql = "INSERT('héllo', 2, 2, 'ü')", .want = "hülo" },
    .{ .sql = "INSERT('', 1, 1, 'x')", .want = "" },
    .{ .sql = "INSERT(NULL, 1, 1, 'x')", .want = null },
    .{ .sql = "INSERT('abc', NULL, 1, 'x')", .want = null },
    .{ .sql = "INSERT('abc', 1, 1, NULL)", .want = null },
    .{ .sql = "QUOTE('it''s')", .want = "'it\\'s'" },
    .{ .sql = "CONCAT('[', QUOTE(NULL), ']')", .want = "[NULL]" },
    .{ .sql = "QUOTE('')", .want = "''" },
    .{ .sql = "QUOTE('a\\\\b')", .want = "'a\\\\b'" },
    .{ .sql = "QUOTE(CONCAT('a', CHAR(0), 'b', CHAR(26)))", .want = "'a\\0b\\Z'" },
    .{ .sql = "QUOTE('héllo')", .want = "'héllo'" },
    .{ .sql = "SOUNDEX('Tymczak')", .want = "T520" },
    .{ .sql = "SOUNDEX('Robert')", .want = "R163" },
    .{ .sql = "SOUNDEX('Ashcraft')", .want = "A2613" },
    .{ .sql = "SOUNDEX('Quadratically')", .want = "Q36324" },
    .{ .sql = "SOUNDEX('Hello World')", .want = "H4643" },
    .{ .sql = "SOUNDEX('')", .want = "" },
    .{ .sql = "SOUNDEX('123')", .want = "" },
    .{ .sql = "SOUNDEX('éab')", .want = "é100" },
    .{ .sql = "SOUNDEX(NULL)", .want = null },
    .{ .sql = "'Robert' SOUNDS LIKE 'Rupert'", .want = "1" },
    .{ .sql = "'Robert' SOUNDS LIKE 'Tymczak'", .want = "0" },
    .{ .sql = "INET_ATON('10.0.5.9')", .want = "167773449" },
    .{ .sql = "INET_ATON('127.1')", .want = "2130706433" },
    .{ .sql = "INET_ATON('255.255.255.255')", .want = "4294967295" },
    .{ .sql = "INET_ATON('1.2.3.256')", .want = null },
    .{ .sql = "INET_ATON('1.2.3.4.5')", .want = null },
    .{ .sql = "INET_ATON('')", .want = null },
    .{ .sql = "INET_ATON(NULL)", .want = null },
    .{ .sql = "INET_NTOA(167773449)", .want = "10.0.5.9" },
    .{ .sql = "INET_NTOA(0)", .want = "0.0.0.0" },
    .{ .sql = "INET_NTOA(4294967295)", .want = "255.255.255.255" },
    .{ .sql = "INET_NTOA(4294967296)", .want = null },
    .{ .sql = "INET_NTOA(-1)", .want = null },
    .{ .sql = "INET_NTOA(1.5e0)", .want = "0.0.0.2" },
    .{ .sql = "INET_NTOA(NULL)", .want = null },
    .{ .sql = "LOWER(HEX(INET6_ATON('fdfe::5a55:caff:fefa:9089')))", .want = "fdfe0000000000005a55cafffefa9089" },
    .{ .sql = "LOWER(HEX(INET6_ATON('10.0.5.9')))", .want = "0a000509" },
    .{ .sql = "LOWER(HEX(INET6_ATON('::ffff:10.0.5.9')))", .want = "00000000000000000000ffff0a000509" },
    .{ .sql = "INET6_ATON('x')", .want = null },
    .{ .sql = "INET6_ATON(NULL)", .want = null },
    .{ .sql = "INET6_NTOA(INET6_ATON('fdfe::5a55:caff:fefa:9089'))", .want = "fdfe::5a55:caff:fefa:9089" },
    .{ .sql = "INET6_NTOA(INET6_ATON('::ffff:10.0.5.9'))", .want = "::ffff:10.0.5.9" },
    .{ .sql = "INET6_NTOA(INET6_ATON('10.0.5.9'))", .want = "10.0.5.9" },
    .{ .sql = "INET6_NTOA(INET6_ATON('::'))", .want = "::" },
    .{ .sql = "INET6_NTOA(INET6_ATON('1:0:0:2:0:0:0:3'))", .want = "1:0:0:2::3" },
    .{ .sql = "INET6_NTOA(INET6_ATON('::10.0.5.9'))", .want = "::10.0.5.9" },
    .{ .sql = "INET6_NTOA('abc')", .want = null },
    .{ .sql = "INET6_NTOA(NULL)", .want = null },
    .{ .sql = "IS_IPV4('10.0.5.9')", .want = "1" },
    .{ .sql = "IS_IPV4('10.0.5.256')", .want = "0" },
    .{ .sql = "IS_IPV4('::1')", .want = "0" },
    .{ .sql = "IS_IPV4(NULL)", .want = null },
    .{ .sql = "IS_IPV6('::1')", .want = "1" },
    .{ .sql = "IS_IPV6('10.0.5.9')", .want = "0" },
    .{ .sql = "IS_IPV6('fdfe::5a55:caff:fefa:9089')", .want = "1" },
    .{ .sql = "IS_IPV4_COMPAT(INET6_ATON('::10.0.5.9'))", .want = "1" },
    .{ .sql = "IS_IPV4_COMPAT(INET6_ATON('::ffff:10.0.5.9'))", .want = "0" },
    .{ .sql = "IS_IPV4_MAPPED(INET6_ATON('::ffff:10.0.5.9'))", .want = "1" },
    .{ .sql = "IS_IPV4_MAPPED(INET6_ATON('::10.0.5.9'))", .want = "0" },
    .{ .sql = "INTERVAL(5, 1, 3, 7)", .want = "2" },
    .{ .sql = "INTERVAL(0, 1, 2)", .want = "0" },
    .{ .sql = "INTERVAL(99, 1, 2)", .want = "2" },
    .{ .sql = "INTERVAL(3, 1, 3, 7)", .want = "2" },
    .{ .sql = "INTERVAL(NULL, 1, 2)", .want = "-1" },
    .{ .sql = "INTERVAL(5, NULL, 3, NULL, 7)", .want = "3" },
    .{ .sql = "INTERVAL(2.5, 1, 2.5, 3)", .want = "2" },
    .{ .sql = "1 + INTERVAL(5, 1, 3, 7)", .want = "3" },
    .{ .sql = "SLEEP(0)", .want = "0" },
    .{ .sql = "SLEEP(0.01)", .want = "0" },
    .{ .sql = "REGEXP_INSTR('dog cat dog', 'dog')", .want = "1" },
    .{ .sql = "REGEXP_INSTR('dog cat dog', 'dog', 2)", .want = "9" },
    .{ .sql = "REGEXP_INSTR('dog cat dog', 'dog', 1, 2)", .want = "9" },
    .{ .sql = "REGEXP_INSTR('dog cat dog', 'dog', 1, 3)", .want = "0" },
    .{ .sql = "REGEXP_INSTR('dog cat dog', 'dog', 1, 1, 1)", .want = "4" },
    .{ .sql = "REGEXP_INSTR('dog cat dog', 'dog', 1, 2, 1)", .want = "12" },
    .{ .sql = "REGEXP_INSTR('héllo wörld', 'w')", .want = "7" },
    .{ .sql = "REGEXP_INSTR('héllo wörld', 'ö', 1, 1, 1)", .want = "9" },
    .{ .sql = "REGEXP_INSTR('abc', 'x')", .want = "0" },
    .{ .sql = "REGEXP_INSTR(NULL, 'a')", .want = null },
    .{ .sql = "REGEXP_INSTR('a', 'a', NULL)", .want = null },
    .{ .sql = "REGEXP_INSTR('abc', 'c', 3)", .want = "3" },
    .{ .sql = "REGEXP_INSTR('abc', 'c', 1, 0)", .want = "3" },
    .{ .sql = "REGEXP_INSTR('aaa', 'a', 1, 4)", .want = "0" },
    .{ .sql = "REGEXP_INSTR('', 'a')", .want = "0" },
    .{ .sql = "REGEXP_INSTR('', 'a', 2)", .want = "0" },
    .{ .sql = "REGEXP_INSTR('abc', 'x*', 1, 2)", .want = "2" },
    .{ .sql = "REGEXP_INSTR('ABC', 'b', 1, 1, 0, 'i')", .want = "2" },
    .{ .sql = "REGEXP_INSTR('ABC', 'b', 1, 1, 0, 'c')", .want = "0" },
    .{ .sql = "REGEXP_INSTR('ABC', 'b', 1, 1, 0, 'ci')", .want = "2" },
    .{ .sql = "REGEXP_INSTR('ABC', 'b', 1, 1, 0, 'ic')", .want = "0" },
    .{ .sql = "REGEXP_LIKE('abc', 'b')", .want = "1" },
    .{ .sql = "REGEXP_LIKE('ABC', 'b', 'c')", .want = "0" },
    .{ .sql = "REGEXP_LIKE('ABC', 'b', 'i')", .want = "1" },
    .{ .sql = "REGEXP_LIKE('a\\nb', 'a.b')", .want = "0" },
    .{ .sql = "REGEXP_LIKE('a\\nb', 'a.b', 'n')", .want = "1" },
    .{ .sql = "REGEXP_LIKE('B', '[^b]', 'i')", .want = "0" },
    .{ .sql = "REGEXP_LIKE('B', '[a-c]', 'i')", .want = "1" },
    .{ .sql = "REGEXP_LIKE('a', 'a', NULL)", .want = null },
    .{ .sql = "REGEXP_REPLACE('a b c', 'b', 'X')", .want = "a X c" },
    .{ .sql = "REGEXP_REPLACE('abc abc', 'b', 'X', 5)", .want = "abc aXc" },
    .{ .sql = "REGEXP_REPLACE('abc abc abc', 'b', 'X', 1, 2)", .want = "abc aXc abc" },
    .{ .sql = "REGEXP_REPLACE('abc abc abc', 'b', 'X', 1, 0)", .want = "aXc aXc aXc" },
    .{ .sql = "REGEXP_REPLACE('abc abc abc', 'b', 'X', 3, 1)", .want = "abc aXc abc" },
    .{ .sql = "REGEXP_REPLACE('ABC', 'b', 'X', 1, 0, 'i')", .want = "AXC" },
    .{ .sql = "REGEXP_REPLACE('abc abc', 'b', 'X', 1, 5)", .want = "abc abc" },
    .{ .sql = "REGEXP_REPLACE('héllo wörld', 'l', 'L', 4)", .want = "hélLo wörLd" },
    .{ .sql = "REGEXP_REPLACE('aaa', 'a*', 'X')", .want = "XX" },
    .{ .sql = "REGEXP_REPLACE('abc', 'x*', '-')", .want = "-a-b-c-" },
    .{ .sql = "REGEXP_REPLACE('héllo', 'x*', '-')", .want = "-h-é-l-l-o-" },
    .{ .sql = "REGEXP_REPLACE('abc', 'b', 'X', 1, NULL)", .want = null },
    .{ .sql = "REGEXP_SUBSTR('abc abd abe', 'ab.')", .want = "abc" },
    .{ .sql = "REGEXP_SUBSTR('abc abd abe', 'ab.', 2)", .want = "abd" },
    .{ .sql = "REGEXP_SUBSTR('abc abd abe', 'ab.', 1, 3)", .want = "abe" },
    .{ .sql = "REGEXP_SUBSTR('abc abd abe', 'ab.', 1, 4)", .want = null },
    .{ .sql = "REGEXP_SUBSTR('ABC', 'b', 1, 1, 'i')", .want = "B" },
    .{ .sql = "REGEXP_SUBSTR('héllo', 'l+', 2)", .want = "ll" },
    .{ .sql = "REGEXP_SUBSTR('abc', 'b', 4)", .want = null },
    .{ .sql = "REGEXP_SUBSTR('abc', 'b', 1, NULL)", .want = null },
};

/// Each is wrapped in `CAST(... AS CHAR)`, which MySQL renders the same as
/// its JSON result.
const json_cases = [_]Case{
    .{ .sql = "JSON_ARRAY()", .want = "[]" },
    .{ .sql = "JSON_ARRAY(NULL)", .want = "[null]" },
    .{ .sql = "JSON_ARRAY(1, 'a', NULL, TRUE, FALSE, 1.50, -3)", .want = "[1, \"a\", null, true, false, 1.50, -3]" },
    .{ .sql = "JSON_ARRAY(1.5e-15, 1.5e-16, 1e15, 100.0e0, 0.1, 2.5)", .want = "[0.0000000000000015, 1.5e-16, 1e15, 100.0, 0.1, 2.5]" },
    .{ .sql = "JSON_ARRAY('a\"b', 'c\\\\d', 'e\\nf', 'héllo', '[1,2]')", .want = "[\"a\\\"b\", \"c\\\\d\", \"e\\nf\", \"héllo\", \"[1,2]\"]" },
    .{ .sql = "JSON_ARRAY(CAST('[1,2]' AS JSON), CAST(1.25 AS DECIMAL(10,4)))", .want = "[[1, 2], 1.2500]" },
    .{ .sql = "JSON_ARRAY(DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05', TIMESTAMP '2024-01-02 03:04:05.123456')", .want = "[\"2024-01-02\", \"2024-01-02 03:04:05.000000\", \"2024-01-02 03:04:05.123456\"]" },
    .{ .sql = "JSON_OBJECT()", .want = "{}" },
    .{ .sql = "JSON_OBJECT('b', 1, 'a', 'x', 'aa', 2, 'c', NULL)", .want = "{\"a\": \"x\", \"b\": 1, \"c\": null, \"aa\": 2}" },
    .{ .sql = "JSON_OBJECT('a', 1, 'a', 2)", .want = "{\"a\": 2}" },
    .{ .sql = "JSON_OBJECT(1, 2, 1.5, 3)", .want = "{\"1\": 2, \"1.5\": 3}" },
    .{ .sql = "JSON_OBJECT('k', JSON_ARRAY(1, 2), 'o', JSON_OBJECT('z', 1, 'y', 2))", .want = "{\"k\": [1, 2], \"o\": {\"y\": 2, \"z\": 1}}" },
    .{ .sql = "JSON_KEYS(JSON_OBJECT('bb', 1, 'a', 2, 'ccc', 3, 'b', 4))", .want = "[\"a\", \"b\", \"bb\", \"ccc\"]" },
    .{ .sql = "JSON_TYPE(JSON_EXTRACT(JSON_ARRAY(CAST(1.5 AS DECIMAL(4,2))), '$[0]'))", .want = "DECIMAL" },
    .{ .sql = "JSON_EXTRACT(JSON_OBJECT('a', CAST(2.50 AS DECIMAL(5,2))), '$.a')", .want = "2.50" },
    .{ .sql = "JSON_UNQUOTE(JSON_EXTRACT(JSON_OBJECT('a', CAST(2.50 AS DECIMAL(5,2))), '$.a'))", .want = "2.50" },
    .{ .sql = "CAST(JSON_ARRAY(1, 2) AS CHAR)", .want = "[1, 2]" },
    .{ .sql = "CAST('{\"b\": [1, 2.5], \"a\": {\"y\": null}}' AS JSON)", .want = "{\"a\": {\"y\": null}, \"b\": [1, 2.5]}" },
};

const error_cases = [_][]const u8{
    "SELECT SLEEP(NULL)",
    "SELECT SLEEP(-1)",
    "SELECT REGEXP_INSTR('abc', 'c', 4)",
    "SELECT REGEXP_INSTR('abc', 'c', 0)",
    "SELECT REGEXP_INSTR('abc', 'c', 1, 1, 2)",
    "SELECT REGEXP_INSTR('ABC', 'b', 1, 1, 0, 'x')",
    "SELECT REGEXP_LIKE('abc', 'b', 'q')",
    "SELECT REGEXP_REPLACE('abc', 'b', 'X', 5)",
    "SELECT REGEXP_REPLACE('abc', 'b', 'X', 0)",
    "SELECT REGEXP_SUBSTR('abc', 'b', 5)",
    "SELECT JSON_OBJECT(NULL, 1)",
};

fn renderCell(allocator: std.mem.Allocator, col: thindb.storage.ColumnView, row: usize) !?[]u8 {
    if (!col.isValid(row)) return null;
    return switch (col.data) {
        .boolean => |s| try allocator.dupe(u8, if (s[row] != 0) "1" else "0"),
        inline .tinyint, .smallint, .int, .bigint, .largeint => |s| try std.fmt.allocPrint(allocator, "{d}", .{s[row]}),
        .varchar, .string, .char => |sv| try allocator.dupe(u8, sv.rowBytes(row)),
        else => error.UnexpectedResultType,
    };
}

/// Every row of column `column` in `sql`'s result, rendered as MySQL text.
fn collectCells(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, column: usize) ![]?[]u8 {
    var q = try helpers.runSqlMysql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(?[]u8) = .empty;
    errdefer {
        for (out.items) |v| if (v) |x| allocator.free(x);
        out.deinit(allocator);
    }
    while (try q.next()) |batch| {
        for (0..batch.row_count) |row| {
            const cell = try renderCell(allocator, batch.values[column], row);
            errdefer if (cell) |x| allocator.free(x);
            try out.append(allocator, cell);
        }
    }
    return out.toOwnedSlice(allocator);
}

fn expectCells(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, column: usize, want: []const ?[]const u8) !void {
    errdefer std.debug.print("query: {s}\n", .{sql});
    const got = try collectCells(allocator, db, sql, column);
    defer helpers.freeStrings(allocator, got);
    try std.testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| {
        if (w) |text| {
            try std.testing.expect(g != null);
            try std.testing.expectEqualStrings(text, g.?);
        } else try std.testing.expect(g == null);
    }
}

fn expectFailure(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) !void {
    const got = collectCells(allocator, db, sql, 0) catch return;
    helpers.freeStrings(allocator, got);
    std.debug.print("query succeeded but MySQL rejects it: {s}\n", .{sql});
    return error.TestUnexpectedSuccess;
}

fn openDb(allocator: std.mem.Allocator, dir: std.Io.Dir) !*thindb.Database {
    return thindb.Database.open(allocator, std.testing.io, dir, .{});
}

test "MySQL misc functions: scalar values match MySQL 8.4" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    var sql_buf: std.ArrayList(u8) = .empty;
    defer sql_buf.deinit(allocator);
    for (scalar_cases) |c| {
        sql_buf.clearRetainingCapacity();
        try sql_buf.print(allocator, "SELECT {s}", .{c.sql});
        try expectCells(allocator, db, sql_buf.items, 0, &.{c.want});
    }
    for (json_cases) |c| {
        sql_buf.clearRetainingCapacity();
        try sql_buf.print(allocator, "SELECT CAST({s} AS CHAR)", .{c.sql});
        try expectCells(allocator, db, sql_buf.items, 0, &.{c.want});
    }
}

test "MySQL misc functions: invalid arguments raise errors as in MySQL" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();
    for (error_cases) |sql| try expectFailure(allocator, db, sql);
}

test "MySQL misc functions: SLEEP runs once per row and returns 0" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE s (id BIGINT PRIMARY KEY)");
    try helpers.exec(allocator, db, "INSERT INTO s (id) VALUES (1), (2), (3)");
    try expectCells(allocator, db, "SELECT SLEEP(0) FROM s ORDER BY id", 0, &.{ "0", "0", "0" });
}

test "MySQL misc functions: JSON_ARRAYAGG and JSON_OBJECTAGG" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE ja (id BIGINT PRIMARY KEY, g BIGINT, k VARCHAR(8), v BIGINT, d DECIMAL(6,2))");
    try helpers.exec(allocator, db,
        \\INSERT INTO ja (id, g, k, v, d) VALUES
        \\ (1, 1, 'x', 10, 1.50), (2, 1, 'y', NULL, 2.00), (3, 2, 'z', 30, NULL), (4, 1, 'x', 40, 0.25)
    );

    const grouped = "SELECT g, CAST(JSON_ARRAYAGG(v) AS CHAR) a, CAST(JSON_OBJECTAGG(k, v) AS CHAR) o FROM ja GROUP BY g ORDER BY g";
    try expectCells(allocator, db, grouped, 1, &.{ "[10, null, 40]", "[30]" });
    try expectCells(allocator, db, grouped, 2, &.{ "{\"x\": 40, \"y\": null}", "{\"z\": 30}" });
    try expectCells(allocator, db, "SELECT CAST(JSON_ARRAYAGG(d) AS CHAR) FROM ja", 0, &.{"[1.50, 2.00, null, 0.25]"});
    try expectCells(allocator, db, "SELECT CAST(JSON_ARRAYAGG(JSON_OBJECT('a', v)) AS CHAR) FROM ja WHERE id < 3", 0, &.{"[{\"a\": 10}, {\"a\": null}]"});
    try expectCells(allocator, db, "SELECT CAST(JSON_ARRAYAGG(v) AS CHAR) FROM ja WHERE id > 10", 0, &.{null});
    try expectCells(allocator, db, "SELECT CAST(JSON_OBJECTAGG(k, v) AS CHAR) FROM ja WHERE id > 10", 0, &.{null});
    try expectCells(allocator, db, "SELECT g FROM ja GROUP BY g HAVING JSON_LENGTH(JSON_ARRAYAGG(v)) > 1", 0, &.{"1"});
    try expectFailure(allocator, db, "SELECT JSON_OBJECTAGG(CASE WHEN id = 3 THEN NULL ELSE k END, v) FROM ja");

    var q = try helpers.runSqlMysql(allocator, db, "SELECT JSON_ARRAYAGG(v), JSON_OBJECTAGG(k, v) FROM ja");
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqualStrings("JSON_ARRAYAGG(v)", schema[0].name);
    try std.testing.expectEqualStrings("JSON_OBJECTAGG(k, v)", schema[1].name);
    while (try q.next()) |_| {}
}

fn expectSessionCells(allocator: std.mem.Allocator, db: *thindb.Database, session: thindb.Session, sql: []const u8, want: []const ?[]const u8) !void {
    errdefer std.debug.print("query: {s}\n", .{sql});
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parseDialect(arena.allocator(), sql, .mysql);
    var cq = try thindb.net.compileWithSession(allocator, db, session, root);
    defer cq.deinit();
    var got: usize = 0;
    while (try cq.next()) |batch| {
        for (0..batch.row_count) |row| {
            try std.testing.expect(got < want.len);
            const cell = try renderCell(allocator, batch.values[0], row);
            defer if (cell) |x| allocator.free(x);
            if (want[got]) |text| {
                try std.testing.expect(cell != null);
                try std.testing.expectEqualStrings(text, cell.?);
            } else try std.testing.expect(cell == null);
            got += 1;
        }
    }
    try std.testing.expectEqual(want.len, got);
}

test "MySQL misc functions: LAST_INSERT_ID and ROW_COUNT read the session" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE ai (id BIGINT PRIMARY KEY AUTO_INCREMENT, s VARCHAR(10))");

    try expectSessionCells(allocator, db, .{}, "SELECT LAST_INSERT_ID()", &.{"0"});
    try expectSessionCells(allocator, db, .{}, "SELECT ROW_COUNT()", &.{"-1"});
    try expectSessionCells(allocator, db, .{ .last_insert_id = 2, .row_count = 3 }, "SELECT LAST_INSERT_ID()", &.{"2"});
    try expectSessionCells(allocator, db, .{ .last_insert_id = 2, .row_count = 3 }, "SELECT ROW_COUNT()", &.{"3"});

    {
        var q = try helpers.runSqlMysql(allocator, db, "INSERT INTO ai (s) VALUES ('a'), ('b'), ('c')");
        defer q.deinit();
        while (try q.next()) |_| {}
        try std.testing.expectEqual(@as(?u64, 1), q.cq.lastInsertId());
        try std.testing.expectEqual(@as(u64, 3), q.affectedRows());
    }
    {
        var q = try helpers.runSqlMysql(allocator, db, "INSERT INTO ai (id, s) VALUES (100, 'x')");
        defer q.deinit();
        while (try q.next()) |_| {}
        try std.testing.expectEqual(@as(?u64, null), q.cq.lastInsertId());
    }
    {
        var q = try helpers.runSqlMysql(allocator, db, "INSERT INTO ai (s) VALUES ('d')");
        defer q.deinit();
        while (try q.next()) |_| {}
        try std.testing.expectEqual(@as(?u64, 101), q.cq.lastInsertId());
    }
    try expectSessionCells(allocator, db, .{ .last_insert_id = 2 }, "SELECT s FROM ai WHERE id = LAST_INSERT_ID()", &.{"b"});
}
