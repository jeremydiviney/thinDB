//! MySQL spellings a client or dump sends that carry no new semantics:
//! literal forms (integers past BIGINT, hex, bit, national, charset
//! introducers, adjacent strings), SELECT hints, and administrative
//! statements that are accepted without effect or answered with MySQL's
//! result shape. Everything runs under the MySQL dialect, as the wire
//! parses it.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

const ir = thindb.ir;

/// Every cell of a result whose columns are all text, row by row.
fn expectCells(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, expected: []const []const u8) !void {
    var q = try helpers.runSqlMysql(allocator, db, sql);
    defer q.deinit();
    var cell: usize = 0;
    while (try q.next()) |batch| {
        for (0..batch.row_count) |row| {
            for (batch.values) |view| {
                if (cell >= expected.len) return error.TestUnexpectedResult;
                const text = switch (view.data) {
                    .varchar, .string, .char => |sv| sv.rowBytes(row),
                    else => return error.TestUnexpectedResult,
                };
                try std.testing.expectEqualStrings(expected[cell], text);
                cell += 1;
            }
        }
    }
    try std.testing.expectEqual(expected.len, cell);
}

fn expectText(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, expected: []const u8) !void {
    expectCells(allocator, db, sql, &.{expected}) catch |err| {
        std.debug.print("case failed ({s}): {s}\n", .{ @errorName(err), sql });
        return err;
    };
}

fn expectBigint(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, expected: i64) !void {
    var q = try helpers.runSqlMysql(allocator, db, sql);
    defer q.deinit();
    const batch = (try q.next()) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    const actual: i64 = switch (batch.values[0].data) {
        .bigint => |v| v[0],
        .int => |v| v[0],
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(expected, actual);
}

fn expectType(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, expected: thindb.types.Type) !void {
    var q = try helpers.runSqlMysql(allocator, db, sql);
    defer q.deinit();
    try std.testing.expectEqual(expected, q.outputSchema()[0].type);
}

fn run(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) !void {
    var q = helpers.runSqlMysql(allocator, db, sql) catch |err| {
        std.debug.print("statement failed ({s}): {s}\n", .{ @errorName(err), sql });
        return err;
    };
    defer q.deinit();
    while (try q.next()) |_| {}
}

fn expectMysqlError(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, expected: anyerror) !void {
    if (helpers.runSqlMysql(allocator, db, sql)) |ok| {
        var q = ok;
        q.deinit();
        std.debug.print("expected {s}: {s}\n", .{ @errorName(expected), sql });
        return error.TestUnexpectedSuccess;
    } else |err| try std.testing.expectEqual(expected, err);
}

fn parseMysql(arena: std.mem.Allocator, sql: []const u8) !*ir.Op {
    return thindb.sql.parseDialect(arena, sql, .mysql);
}

fn openDb(allocator: std.mem.Allocator, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, std.testing.io, dir, .{});
    errdefer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, s VARCHAR(20), high_priority INT)");
    try helpers.exec(allocator, db, "INSERT INTO t VALUES (1, 'kiwi', 10), (2, 'pear', 20), (3, 'AB', 30)");
    try helpers.exec(allocator, db, "CREATE TABLE u (id BIGINT PRIMARY KEY, w INT)");
    try helpers.exec(allocator, db, "INSERT INTO u VALUES (1, 100), (3, 300)");
    return db;
}

test "mysql literals: an integer past BIGINT is DECIMAL, then DOUBLE" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    try expectType(allocator, db, "SELECT 12345678901234567890", .{ .decimal128 = .{ .p = 20, .s = 0 } });
    try expectText(allocator, db, "SELECT CAST(12345678901234567890 AS CHAR)", "12345678901234567890");
    try expectText(allocator, db, "SELECT CAST(12345678901234567890 + 1 AS CHAR)", "12345678901234567891");
    try expectText(allocator, db, "SELECT CAST(-12345678901234567890 AS CHAR)", "-12345678901234567890");
    try expectType(allocator, db, "SELECT 9223372036854775808", .{ .decimal128 = .{ .p = 19, .s = 0 } });
    try expectType(allocator, db, "SELECT 1234567890123456789012345678901234567890", .double);
    try expectType(allocator, db, "SELECT -9223372036854775808", .bigint);
    try expectBigint(allocator, db, "SELECT -9223372036854775808", std.math.minInt(i64));
    try expectBigint(allocator, db, "SELECT 9223372036854775807", std.math.maxInt(i64));

    try helpers.exec(allocator, db, "CREATE TABLE big (id BIGINT PRIMARY KEY, d DECIMAL(20,0))");
    try run(allocator, db, "INSERT INTO big VALUES (1, 12345678901234567890), (2, -9223372036854775808)");
    try expectText(allocator, db, "SELECT CAST(d AS CHAR) FROM big WHERE d = 12345678901234567890", "12345678901234567890");
    try expectText(allocator, db, "SELECT CAST(d AS CHAR) FROM big WHERE d IN (12345678901234567890)", "12345678901234567890");
    try expectText(allocator, db, "SELECT CAST(d AS CHAR) FROM big WHERE d = -9223372036854775808", "-9223372036854775808");
    try expectText(allocator, db, "SELECT CAST(d AS CHAR) FROM big WHERE 12345678901234567890 = d", "12345678901234567890");
    try expectText(allocator, db, "SELECT CAST(d AS CHAR) FROM big WHERE d + 0 = 12345678901234567890", "12345678901234567890");
    try expectText(allocator, db, "SELECT CAST(d AS CHAR) FROM big WHERE d < 12345678901234567890", "-9223372036854775808");
    try expectText(allocator, db, "SELECT CAST(COUNT(*) AS CHAR) FROM big WHERE id = 12345678901234567890", "0");
    try expectText(allocator, db, "SELECT CAST(COUNT(*) AS CHAR) FROM big WHERE id < 12345678901234567890 AND id > -12345678901234567890", "2");
    try expectText(allocator, db, "SELECT CAST(COUNT(*) AS CHAR) FROM big WHERE CAST(id AS DOUBLE) < 12345678901234567890", "2");
}

test "mysql literals: hex, bit, national, introducers and adjacent strings" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    const texts = .{
        .{ "SELECT 0x41", "A" },
        .{ "SELECT X'4142'", "AB" },
        .{ "SELECT x'4142'", "AB" },
        .{ "SELECT X''", "" },
        .{ "SELECT 0x123", "\x01\x23" },
        .{ "SELECT N'abc'", "abc" },
        .{ "SELECT n'abc'", "abc" },
        .{ "SELECT _utf8mb4'abc'", "abc" },
        .{ "SELECT _utf8mb4 'abc'", "abc" },
        .{ "SELECT _latin1'x'", "x" },
        .{ "SELECT _binary'x'", "x" },
        .{ "SELECT _binary X'41'", "A" },
        .{ "SELECT _utf8mb4 0x41", "A" },
        .{ "SELECT 'a' 'b' \"c\"", "abc" },
        .{ "SELECT 'a'\n  'b'", "ab" },
        .{ "SELECT N'a' 'b'", "ab" },
        .{ "SELECT \"a\\nb\"", "a\nb" },
        .{ "SELECT \"say \\\"hi\\\"\"", "say \"hi\"" },
        .{ "SELECT s FROM t WHERE s = 0x6B697769", "kiwi" },
        .{ "SELECT s FROM t WHERE s = X'4142'", "AB" },
        .{ "SELECT CONCAT(0x41, 'b')", "Ab" },
    };
    inline for (texts) |c| try expectText(allocator, db, c[0], c[1]);

    try expectBigint(allocator, db, "SELECT b'1000001'", 65);
    try expectBigint(allocator, db, "SELECT B'11'", 3);
    try expectBigint(allocator, db, "SELECT 0b11", 3);
    try expectBigint(allocator, db, "SELECT b''", 0);
    try expectBigint(allocator, db, "SELECT id FROM t WHERE id = b'11'", 3);

    try expectMysqlError(allocator, db, "SELECT X'414'", error.LexInvalidNumber);
    try expectMysqlError(allocator, db, "SELECT X'4G'", error.LexInvalidNumber);
    try expectMysqlError(allocator, db, "SELECT 0x41g", error.LexInvalidNumber);
    try expectMysqlError(allocator, db, "SELECT b'102'", error.LexInvalidNumber);
    try expectMysqlError(allocator, db, "SELECT _latin1'\xc3\xa9'", error.LexCharsetUnsupported);
    try expectMysqlError(allocator, db, "SELECT _utf16'a'", error.LexCharsetUnsupported);
}

test "mysql select hints: modifiers, index hints and STRAIGHT_JOIN are ignored" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    const three_ids = [_][]const u8{ "1", "2", "3" };
    const queries = [_][]const u8{
        "SELECT SQL_NO_CACHE CAST(id AS CHAR) FROM t ORDER BY id",
        "SELECT SQL_CACHE HIGH_PRIORITY STRAIGHT_JOIN SQL_SMALL_RESULT SQL_BIG_RESULT SQL_BUFFER_RESULT SQL_CALC_FOUND_ROWS CAST(id AS CHAR) FROM t ORDER BY id",
        "SELECT ALL SQL_NO_CACHE CAST(id AS CHAR) FROM t ORDER BY id",
        "SELECT /*+ MAX_EXECUTION_TIME(1000) */ CAST(id AS CHAR) FROM t ORDER BY id",
        "SELECT CAST(id AS CHAR) FROM t USE INDEX (PRIMARY) ORDER BY id",
        "SELECT CAST(id AS CHAR) FROM t USE INDEX () ORDER BY id",
        "SELECT CAST(id AS CHAR) FROM t AS x FORCE INDEX FOR ORDER BY (PRIMARY) ORDER BY id",
        "SELECT CAST(id AS CHAR) FROM t x IGNORE KEY FOR GROUP BY (a, b), USE KEY FOR JOIN (c) ORDER BY id",
        "SELECT CAST(id AS CHAR) FROM t FORCE KEY (PRIMARY) WHERE id > 0 ORDER BY id",
    };
    for (queries) |sql| expectCells(allocator, db, sql, &three_ids) catch |err| {
        std.debug.print("case failed ({s}): {s}\n", .{ @errorName(err), sql });
        return err;
    };
    try expectCells(allocator, db, "SELECT DISTINCTROW s FROM t WHERE id < 3 ORDER BY s", &.{ "kiwi", "pear" });
    {
        var q = try helpers.runSqlMysql(allocator, db, "SELECT high_priority FROM t WHERE id = 2");
        defer q.deinit();
        try std.testing.expectEqualStrings("high_priority", q.outputSchema()[0].name);
        const batch = (try q.next()) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(i32, 20), batch.values[0].data.int[0]);
    }

    try expectBigint(allocator, db, "SELECT SUM(u.w) FROM t STRAIGHT_JOIN u ON t.id = u.id", 400);
    try expectBigint(allocator, db, "SELECT SUM(u.w) FROM t USE INDEX (PRIMARY) STRAIGHT_JOIN u IGNORE INDEX (i) ON t.id = u.id", 400);
    try expectBigint(allocator, db, "SELECT COUNT(*) FROM t STRAIGHT_JOIN u", 6);
    try expectBigint(allocator, db, "SELECT STRAIGHT_JOIN SUM(u.w) FROM t JOIN u ON t.id = u.id", 400);

    try expectMysqlError(allocator, db, "SELECT FOUND_ROWS()", error.SqlFoundRowsUnsupported);
    try expectMysqlError(allocator, db, "SELECT SQL_CALC_FOUND_ROWS id FROM t LIMIT 1; SELECT FOUND_ROWS()", error.SqlFoundRowsUnsupported);
}

test "mysql predicates: a parenthesized operand takes the operator after it" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "WHERE (id) = 1", &[_][]const u8{"1"} },
        .{ "WHERE ((id)) = 1", &[_][]const u8{"1"} },
        .{ "WHERE ((id) = 2)", &[_][]const u8{"2"} },
        .{ "WHERE (id) = 1 OR (id) = 3", &[_][]const u8{ "1", "3" } },
        .{ "WHERE (id) <=> 2", &[_][]const u8{"2"} },
        .{ "WHERE (s) LIKE 'k%'", &[_][]const u8{"1"} },
        .{ "WHERE (UPPER(s)) LIKE 'P%'", &[_][]const u8{"2"} },
        .{ "WHERE (s) IN ('pear')", &[_][]const u8{"2"} },
        .{ "WHERE (s) NOT IN ('pear', 'kiwi')", &[_][]const u8{"3"} },
        .{ "WHERE (id) BETWEEN 2 AND 3", &[_][]const u8{ "2", "3" } },
        .{ "WHERE (s) REGEXP '^p'", &[_][]const u8{"2"} },
        .{ "WHERE (s) SOUNDS LIKE 'pair'", &[_][]const u8{"2"} },
        .{ "WHERE (id) IS NOT NULL", &[_][]const u8{ "1", "2", "3" } },
        .{ "WHERE (id) + 1 = 3", &[_][]const u8{"2"} },
        .{ "WHERE (id) MOD 2 = 0", &[_][]const u8{"2"} },
        .{ "WHERE (id + 1) * 2 = 6", &[_][]const u8{"2"} },
        .{ "WHERE (1) = 1 AND id = 3", &[_][]const u8{"3"} },
        .{ "WHERE (id = 1) = 1", &[_][]const u8{"1"} },
        .{ "WHERE (id > 1) IS TRUE", &[_][]const u8{ "2", "3" } },
        .{ "WHERE (id) IN (SELECT id FROM u)", &[_][]const u8{ "1", "3" } },
        .{ "WHERE (SELECT MAX(id) FROM u) = id", &[_][]const u8{"3"} },
    };
    inline for (cases) |c| {
        const sql = "SELECT CAST(id AS CHAR) FROM t " ++ c[0] ++ " ORDER BY id";
        expectCells(allocator, db, sql, c[1]) catch |err| {
            std.debug.print("case failed ({s}): {s}\n", .{ @errorName(err), sql });
            return err;
        };
    }
    try expectText(allocator, db, "SELECT IF((s) = 'kiwi', 'y', 'n') FROM t WHERE id = 1", "y");
    try expectText(allocator, db, "SELECT CASE WHEN (id) = 2 THEN 'two' ELSE 'other' END FROM t WHERE id = 2", "two");
    try expectText(allocator, db, "SELECT CAST(COUNT(*) AS CHAR) FROM t HAVING (COUNT(*)) = 3", "3");
}

test "mysql joins: an inner join's condition is optional, and CROSS JOIN takes one" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    const counts = .{
        .{ "SELECT COUNT(*) FROM t JOIN u", 6 },
        .{ "SELECT COUNT(*) FROM t INNER JOIN u", 6 },
        .{ "SELECT COUNT(*) FROM t JOIN u WHERE t.id = u.id", 2 },
        .{ "SELECT COUNT(*) FROM t JOIN u JOIN u AS v ON u.id = v.id", 6 },
        .{ "SELECT COUNT(*) FROM (SELECT 1 AS a) x JOIN (SELECT 2 AS b) y", 1 },
        .{ "SELECT COUNT(*) FROM t CROSS JOIN u ON t.id = u.id", 2 },
        .{ "SELECT SUM(u.w) FROM t CROSS JOIN u ON t.id = u.id", 400 },
        .{ "SELECT COUNT(*) FROM t CROSS JOIN u USING (id)", 2 },
        .{ "SELECT COUNT(*) FROM t CROSS JOIN u", 6 },
    };
    inline for (counts) |c| expectBigint(allocator, db, c[0], c[1]) catch |err| {
        std.debug.print("case failed ({s}): {s}\n", .{ @errorName(err), c[0] });
        return err;
    };

    try expectMysqlError(allocator, db, "SELECT COUNT(*) FROM t LEFT JOIN u", error.SqlExpectedJoinOn);
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    try std.testing.expectError(error.SqlExpectedJoinOn, thindb.sql.parseDialect(arena, "SELECT COUNT(*) FROM t JOIN u", .neutral));
    try std.testing.expectError(error.SqlTrailingTokens, thindb.sql.parseDialect(arena, "SELECT COUNT(*) FROM t CROSS JOIN u ON t.id = u.id", .postgres));
}

test "mysql literals: a fraction past a double's digits stays an exact decimal" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    try helpers.exec(allocator, db, "CREATE TABLE dd (id BIGINT PRIMARY KEY, v DECIMAL(20,1), f DOUBLE, s VARCHAR(30))");
    try run(allocator, db, "INSERT INTO dd VALUES (1, 123456789012345678.5, 1.5, 'a'), (2, 123456789012345680.0, 2.5, 'b')");
    try run(allocator, db, "INSERT INTO dd VALUES (3, -123456789012345678.5, 0.1, 'c')");
    try expectCells(allocator, db, "SELECT CAST(v AS CHAR), s FROM dd ORDER BY id", &.{
        "123456789012345678.5",  "a",
        "123456789012345680.0",  "b",
        "-123456789012345678.5", "c",
    });

    const cases = .{
        .{ "WHERE v = 123456789012345678.5", &[_][]const u8{"1"} },
        .{ "WHERE 123456789012345678.5 = v", &[_][]const u8{"1"} },
        .{ "WHERE v = -123456789012345678.5", &[_][]const u8{"3"} },
        .{ "WHERE v IN (123456789012345678.5, 1.5)", &[_][]const u8{"1"} },
        .{ "WHERE v NOT IN (123456789012345678.5)", &[_][]const u8{ "2", "3" } },
        .{ "WHERE v > 123456789012345678.5", &[_][]const u8{"2"} },
        .{ "WHERE v BETWEEN 123456789012345678.4 AND 123456789012345678.6", &[_][]const u8{"1"} },
        .{ "WHERE (v) = 123456789012345678.5", &[_][]const u8{"1"} },
        .{ "WHERE v = CAST('123456789012345678.5' AS DECIMAL(20,1))", &[_][]const u8{"1"} },
        .{ "WHERE f = 1.5", &[_][]const u8{"1"} },
        .{ "WHERE f IN (2.5, 0.1)", &[_][]const u8{ "2", "3" } },
        .{ "WHERE v = 1.5", &[_][]const u8{} },
    };
    inline for (cases) |c| {
        const sql = "SELECT CAST(id AS CHAR) FROM dd " ++ c[0] ++ " ORDER BY id";
        expectCells(allocator, db, sql, c[1]) catch |err| {
            std.debug.print("case failed ({s}): {s}\n", .{ @errorName(err), sql });
            return err;
        };
    }
    try expectCells(allocator, db, "SELECT CASE WHEN v = 123456789012345678.5 THEN 'hit' ELSE 'miss' END FROM dd ORDER BY id", &.{ "hit", "miss", "miss" });
    try expectText(allocator, db, "SELECT CAST(e.id AS CHAR) FROM dd JOIN dd AS e ON dd.id = e.id AND e.v = 123456789012345678.5", "1");
    try expectText(allocator, db, "SELECT IF(123456789012345678 = 123456789012345678.0, 'y', 'n')", "y");
    try expectText(allocator, db, "SELECT IF(123456789012345678.5 = 123456789012345678.5, 'y', 'n')", "y");
    try expectText(allocator, db, "SELECT IF(123456789012345678.5 > 123456789012345678.4, 'y', 'n')", "y");

    // Rows with an expression are read as expressions, fractions as the
    // decimals they spell, whichever row the expression is in.
    try helpers.exec(allocator, db, "CREATE TABLE de (id BIGINT PRIMARY KEY, v DECIMAL(20,1), f DOUBLE, n BIGINT)");
    try run(allocator, db, "INSERT INTO de VALUES (1, 123456789012345678.5, 0.1, 2.5), (2, 1 + 1, 2.5, -123456789012345678.5)");
    try run(allocator, db, "INSERT INTO de VALUES (3, 1.5, 1e1, 7), (4, -123456789012345678.5, 0.25, 1)");
    try run(allocator, db, "INSERT INTO de SET id = 5, v = 123456789012345678.5, f = 123456789012345678.5, n = 3");
    try run(allocator, db, "INSERT INTO de SET id = 6, v = 123456789012345678.5 * 1, f = 0.5, n = 123456789012345.5");
    try expectCells(allocator, db, "SELECT CAST(v AS CHAR), CAST(f AS CHAR), CAST(n AS CHAR) FROM de ORDER BY id", &.{
        "123456789012345678.5",  "0.1",                   "3",
        "2.0",                   "2.5",                   "-123456789012345679",
        "1.5",                   "10",                    "7",
        "-123456789012345678.5", "0.25",                  "1",
        "123456789012345678.5",  "1.2345678901234568e17", "3",
        "123456789012345678.5",  "0.5",                   "123456789012346",
    });
}

fn expectNames(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, names: []const []const u8) !void {
    var q = helpers.runSqlMysql(allocator, db, sql) catch |err| {
        std.debug.print("case failed ({s}): {s}\n", .{ @errorName(err), sql });
        return err;
    };
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqual(names.len, schema.len);
    for (names, schema) |name, col| try std.testing.expectEqualStrings(name, col.name);
    while (try q.next()) |_| {}
}

test "mysql aliases: a string names a select-list item" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    try expectNames(allocator, db, "SELECT 1 'a'", &.{"a"});
    try expectNames(allocator, db, "SELECT 1 AS 'a'", &.{"a"});
    try expectNames(allocator, db, "SELECT 1 AS \"a\"", &.{"a"});
    try expectNames(allocator, db, "SELECT 1 \"a b\", 2 'c'", &.{ "a b", "c" });
    try expectNames(allocator, db, "SELECT COUNT(*) AS 'Total' FROM t", &.{"Total"});
    try expectNames(allocator, db, "SELECT id = 1 'first' FROM t", &.{"first"});
    try expectNames(allocator, db, "SELECT d.a FROM (SELECT 1 AS 'a') d", &.{"a"});
    try expectCells(allocator, db, "SELECT s 'name' FROM t WHERE id = 1", &.{"kiwi"});
    try expectNames(allocator, db, "SELECT s 'name' FROM t WHERE id = 1", &.{"name"});
    try expectCells(allocator, db, "SELECT s AS 'k' FROM t ORDER BY k", &.{ "AB", "kiwi", "pear" });
    try expectText(allocator, db, "SELECT 'x' 'y'", "xy");

    try expectMysqlError(allocator, db, "SELECT 1 AS ''", error.SqlExpectedIdent);
    try expectMysqlError(allocator, db, "SELECT x.id FROM t AS 'x'", error.SqlExpectedIdent);
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    try std.testing.expectError(error.SqlExpectedIdent, thindb.sql.parseDialect(arena_state.allocator(), "SELECT 1 AS 'a'", .neutral));
}

test "mysql admin: transaction, lock, flush, savepoint and DO statements are no-ops" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    const statements = [_][]const u8{
        "BEGIN",
        "BEGIN WORK",
        "START TRANSACTION",
        "START TRANSACTION READ ONLY",
        "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ WRITE",
        "COMMIT",
        "COMMIT WORK AND NO CHAIN NO RELEASE",
        "ROLLBACK AND CHAIN",
        "ROLLBACK RELEASE",
        "SAVEPOINT s1",
        "RELEASE SAVEPOINT s1",
        "ROLLBACK TO SAVEPOINT s1",
        "ROLLBACK WORK TO s1",
        "LOCK TABLES t READ, u AS x WRITE",
        "LOCK TABLE t READ LOCAL",
        "UNLOCK TABLES",
        "LOCK INSTANCE FOR BACKUP",
        "UNLOCK INSTANCE",
        "FLUSH TABLES",
        "FLUSH TABLES t, u WITH READ LOCK",
        "FLUSH PRIVILEGES",
        "FLUSH NO_WRITE_TO_BINLOG LOGS",
        "DO 1",
        "DO 1 + 1, CONCAT('a', 'b')",
        "BEGIN; SELECT 1; COMMIT",
    };
    for (statements) |sql| try run(allocator, db, sql);

    try expectCells(allocator, db, "SELECT CAST(COUNT(*) AS CHAR) FROM t", &.{"3"});

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const verbs = .{
        .{ "BEGIN", @as(ir.AdminOp, .begin_transaction) },
        .{ "START TRANSACTION READ ONLY", @as(ir.AdminOp, .begin_transaction) },
        .{ "COMMIT", @as(ir.AdminOp, .end_transaction) },
        .{ "ROLLBACK WORK", @as(ir.AdminOp, .end_transaction) },
        .{ "COMMIT AND CHAIN", @as(ir.AdminOp, .begin_transaction) },
        .{ "ROLLBACK AND NO CHAIN", @as(ir.AdminOp, .end_transaction) },
        .{ "ROLLBACK TO s1", @as(ir.AdminOp, .ignored) },
        .{ "UNLOCK TABLES", @as(ir.AdminOp, .ignored) },
    };
    inline for (verbs) |c| {
        const op = try parseMysql(aa, c[0]);
        try std.testing.expectEqual(c[1], op.admin);
    }

    try std.testing.expectError(error.SqlExpectedKeyword, parseMysql(aa, "START"));
    try std.testing.expectError(error.SqlExpectedKeyword, parseMysql(aa, "LOCK t"));
    try std.testing.expectError(error.SqlExpectedKeyword, parseMysql(aa, "FLUSH"));
    try std.testing.expectError(error.SqlExpectedKeyword, parseMysql(aa, "RELEASE s1"));
    try std.testing.expectError(error.SqlExpectedKeyword, parseMysql(aa, "COMMIT AND"));
    try std.testing.expect(std.meta.isError(thindb.sql.parseDialect(aa, "SAVEPOINT s1", .postgres)));
    try std.testing.expect(std.meta.isError(thindb.sql.parseDialect(aa, "LOCK TABLES t READ", .postgres)));
}

test "mysql admin: PREPARE and EXECUTE are rejected clearly" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    const statements = [_][]const u8{
        "PREPARE s FROM 'SELECT 1'",
        "EXECUTE s",
        "EXECUTE s USING @a",
        "DEALLOCATE PREPARE s",
        "DROP PREPARE s",
    };
    for (statements) |sql| try expectMysqlError(allocator, db, sql, error.SqlPrepareExecuteUnsupported);
}

test "mysql admin: table maintenance answers MySQL's status rows" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    try expectCells(allocator, db, "ANALYZE TABLE t", &.{ "main__public.t", "analyze", "status", "OK" });
    try expectCells(allocator, db, "ANALYZE NO_WRITE_TO_BINLOG TABLE t UPDATE HISTOGRAM ON s", &.{ "main__public.t", "analyze", "status", "OK" });
    try expectCells(allocator, db, "OPTIMIZE LOCAL TABLES t, u", &.{
        "main__public.t", "optimize", "status", "OK",
        "main__public.u", "optimize", "status", "OK",
    });
    try expectCells(allocator, db, "CHECK TABLE t QUICK", &.{ "main__public.t", "check", "status", "OK" });
    try expectCells(allocator, db, "REPAIR TABLE public.t", &.{ "main__public.t", "repair", "note", "The storage engine for the table doesn't support repair" });
    try expectCells(allocator, db, "ANALYZE TABLE nope, t", &.{
        "main__public.nope", "analyze", "Error",  "Table 'main__public.nope' doesn't exist",
        "main__public.nope", "analyze", "status", "Operation failed",
        "main__public.t",    "analyze", "status", "OK",
    });

    var q = try helpers.runSqlMysql(allocator, db, "CHECK TABLE t");
    defer q.deinit();
    const names = [_][]const u8{ "Table", "Op", "Msg_type", "Msg_text" };
    const schema = q.outputSchema();
    try std.testing.expectEqual(names.len, schema.len);
    for (names, schema) |name, col| try std.testing.expectEqualStrings(name, col.name);

    try expectCells(allocator, db, "SELECT CAST(COUNT(*) AS CHAR) FROM t", &.{"3"});
}

test "mysql admin: SHOW CREATE DATABASE" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    try expectCells(allocator, db, "SHOW CREATE DATABASE public", &.{
        "public",
        "CREATE DATABASE `public` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci */",
    });
    try expectCells(allocator, db, "SHOW CREATE SCHEMA IF NOT EXISTS public", &.{
        "public",
        "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `public` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci */",
    });
    var q = try helpers.runSqlMysql(allocator, db, "SHOW CREATE DATABASE public");
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqualStrings("Database", schema[0].name);
    try std.testing.expectEqualStrings("Create Database", schema[1].name);

    try expectMysqlError(allocator, db, "SHOW CREATE DATABASE nope", error.DatabaseNotFound);
}
