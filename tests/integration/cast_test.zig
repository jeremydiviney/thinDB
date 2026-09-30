//! CAST where the value has no answer in the target type, StarRocks
//! semantics: numbers truncate toward zero into an integer, a value outside
//! the target's range is NULL, and text converts only when it is a number of
//! the target's kind. Each result is rendered as text, so NULL and the exact
//! value both show in the expected strings.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

const Case = struct { sql: []const u8, expected: []const []const u8 };

/// Rendering of a NULL cell in `Case.expected`.
const NULL = "NULL";

fn expectTexts(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, expected: []const []const u8) !void {
    var q = try helpers.runSqlCtx(allocator, db, sql);
    defer q.deinit();
    var row: usize = 0;
    while (try q.next()) |batch| {
        const view = batch.values[0];
        for (0..batch.row_count) |i| {
            if (row >= expected.len) return error.TestUnexpectedResult;
            const text = if (!view.isValid(i)) NULL else switch (view.data) {
                .string, .varchar, .char => |sv| sv.rowBytes(i),
                else => return error.TestUnexpectedType,
            };
            try std.testing.expectEqualStrings(expected[row], text);
            row += 1;
        }
    }
    try std.testing.expectEqual(expected.len, row);
}

fn expectCasesBeforeAndAfterFlush(allocator: std.mem.Allocator, db: *thindb.Database, cases: []const Case) !void {
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("ct", .{})).flush();
        for (cases) |c| {
            expectTexts(allocator, db, c.sql, c.expected) catch |err| {
                std.debug.print("case failed ({s}): {s}\n", .{ @errorName(err), c.sql });
                return err;
            };
        }
    }
}

fn expectExecError(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, expected: anyerror) !void {
    var q = helpers.runSqlCtx(allocator, db, sql) catch |err| return std.testing.expectEqual(expected, err);
    defer q.deinit();
    while (q.next()) |batch| {
        if (batch == null) return error.TestUnexpectedSuccess;
    } else |err| return std.testing.expectEqual(expected, err);
}

fn setup(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    try helpers.exec(allocator, db, "CREATE TABLE ct (id BIGINT PRIMARY KEY, s VARCHAR(40), d DECIMAL(10,2), x DOUBLE, b BIGINT)");
    try helpers.exec(allocator, db,
        \\INSERT INTO ct VALUES
        \\  (1, ' 12 ', 2.50, 2.5, 12),
        \\  (2, '+5', -2.50, -2.5, 9999999999),
        \\  (3, '1.7', 1.99, 1e20, -9999999999),
        \\  (4, '12abc', NULL, NULL, NULL),
        \\  (5, '', 0.00, 0, 9007199254740993),
        \\  (6, '1e3', 0.00, 0, 0),
        \\  (7, '9999999999', 0.00, 0, 0),
        \\  (8, ' -1.005 ', 0.00, 0, 0),
        \\  (9, 'False', 0.00, 0, 0),
        \\  (10, NULL, 0.00, 0, 0)
    );
}

test "cast: a number into an integer truncates, and out of range is NULL" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setup(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{
        .{ .sql = "SELECT CAST(CAST(d AS INT) AS CHAR) FROM ct WHERE id <= 5 ORDER BY id", .expected = &.{ "2", "-2", "1", NULL, "0" } },
        .{ .sql = "SELECT CAST(CAST(d AS BIGINT) AS CHAR) FROM ct WHERE id <= 5 ORDER BY id", .expected = &.{ "2", "-2", "1", NULL, "0" } },
        .{ .sql = "SELECT CAST(CAST(x AS INT) AS CHAR) FROM ct WHERE id <= 5 ORDER BY id", .expected = &.{ "2", "-2", NULL, NULL, "0" } },
        .{ .sql = "SELECT CAST(CAST(x AS BIGINT) AS CHAR) FROM ct WHERE id <= 5 ORDER BY id", .expected = &.{ "2", "-2", NULL, NULL, "0" } },
        .{ .sql = "SELECT CAST(CAST(b AS INT) AS CHAR) FROM ct WHERE id <= 5 ORDER BY id", .expected = &.{ "12", NULL, NULL, NULL, NULL } },
        .{ .sql = "SELECT CAST(CAST(b AS SMALLINT) AS CHAR) FROM ct WHERE id <= 5 ORDER BY id", .expected = &.{ "12", NULL, NULL, NULL, NULL } },
        .{ .sql = "SELECT CAST(CAST(b AS TINYINT) AS CHAR) FROM ct WHERE id <= 5 ORDER BY id", .expected = &.{ "12", NULL, NULL, NULL, NULL } },
        .{ .sql = "SELECT CAST(CAST(b AS BIGINT) AS CHAR) FROM ct WHERE id <= 5 ORDER BY id", .expected = &.{ "12", "9999999999", "-9999999999", NULL, "9007199254740993" } },
    });
}

test "cast: text converts only when it is a number of the target's kind" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setup(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{
        .{ .sql = "SELECT CAST(CAST(s AS INT) AS CHAR) FROM ct ORDER BY id", .expected = &.{ "12", "5", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL } },
        .{ .sql = "SELECT CAST(CAST(s AS BIGINT) AS CHAR) FROM ct ORDER BY id", .expected = &.{ "12", "5", NULL, NULL, NULL, NULL, "9999999999", NULL, NULL, NULL } },
        .{ .sql = "SELECT CAST(CAST(s AS DOUBLE) AS CHAR) FROM ct ORDER BY id", .expected = &.{ "12", "5", "1.7", NULL, NULL, "1000", "9999999999", "-1.005", NULL, NULL } },
        .{ .sql = "SELECT CAST(CAST(s AS DECIMAL(18,2)) AS CHAR) FROM ct ORDER BY id", .expected = &.{ "12.00", "5.00", "1.70", NULL, NULL, "1000.00", "9999999999.00", "-1.01", NULL, NULL } },
        .{ .sql = "SELECT CAST(CAST(s AS BOOLEAN) AS CHAR) FROM ct ORDER BY id", .expected = &.{ "1", "1", NULL, NULL, NULL, NULL, NULL, NULL, "0", NULL } },
        .{ .sql = "SELECT CAST(CAST(' 7 ' AS INT) AS CHAR) FROM ct WHERE id = 1", .expected = &.{"7"} },
        .{ .sql = "SELECT CAST(CAST('7x' AS DOUBLE) AS CHAR) FROM ct WHERE id = 1", .expected = &.{NULL} },
        .{ .sql = "SELECT CAST(id AS CHAR) FROM ct WHERE b = CAST(' 12 ' AS BIGINT)", .expected = &.{"1"} },
        .{ .sql = "SELECT CAST(COUNT(*) AS CHAR) FROM ct WHERE CAST(s AS DOUBLE) IS NULL", .expected = &.{"4"} },
    });
}

test "cast: a decimal past the target's precision raises" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setup(allocator, db);

    try expectExecError(allocator, db, "SELECT CAST(x AS DECIMAL(10,2)) FROM ct WHERE id = 3", error.ArithmeticOverflow);
    try expectExecError(allocator, db, "SELECT CAST(s AS DECIMAL(10,2)) FROM ct WHERE id = 7", error.ArithmeticOverflow);
}

test "cast: a boolean becomes text as 1 or 0, except through PostgreSQL's cast" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const cases = .{
        .{ "SELECT CAST(TRUE AS VARCHAR)", thindb.types.Dialect.neutral, "1" },
        .{ "SELECT CONCAT(1 = 2, 'x')", thindb.types.Dialect.neutral, "0x" },
        .{ "SELECT CAST(TRUE AS TEXT)", thindb.types.Dialect.postgres, "true" },
        .{ "SELECT CAST(1 = 2 AS TEXT)", thindb.types.Dialect.postgres, "false" },
        .{ "SELECT (1 = 1)::text", thindb.types.Dialect.postgres, "true" },
        .{ "SELECT CAST(12 AS TEXT)", thindb.types.Dialect.postgres, "12" },
    };
    inline for (cases) |c| {
        var q = try helpers.runSqlDialect(allocator, db, c[0], c[1]);
        defer q.deinit();
        const got = try helpers.columnText(allocator, &q);
        defer helpers.freeStrings(allocator, got);
        try std.testing.expectEqual(@as(usize, 1), got.len);
        try std.testing.expectEqualStrings(c[2], got[0].?);
    }
}

/// How a statement is parsed and compiled: in a dialect, or as a MySQL wire
/// connection runs it.
const Run = enum { neutral, mysql, postgres, mysql_session };

fn expectRunTexts(allocator: std.mem.Allocator, db: *thindb.Database, run: Run, c: Case) !void {
    var q = try switch (run) {
        .neutral => helpers.runSqlDialect(allocator, db, c.sql, .neutral),
        .mysql => helpers.runSqlDialect(allocator, db, c.sql, .mysql),
        .postgres => helpers.runSqlDialect(allocator, db, c.sql, .postgres),
        .mysql_session => helpers.runSqlMysqlSession(allocator, db, c.sql),
    };
    defer q.deinit();
    const got = try helpers.columnText(allocator, &q);
    defer helpers.freeStrings(allocator, got);
    try std.testing.expectEqual(c.expected.len, got.len);
    for (c.expected, got) |want, cell| try std.testing.expectEqualStrings(want, cell orelse NULL);
}

test "cast: an integer that doesn't fit a narrower integer is NULL, cast or passed to a narrower parameter (issue #450)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    // NOT NULL columns, so only the narrowing can make a result NULL.
    try helpers.exec(allocator, db, "CREATE TABLE nw (id INT NOT NULL, b BIGINT NOT NULL, l LARGEINT NOT NULL)");
    try helpers.exec(allocator, db,
        \\INSERT INTO nw VALUES
        \\  (1, 7, 7),
        \\  (2, 2147483648, 9223372036854775808),
        \\  (3, -2147483649, 18446744073709551615),
        \\  (4, 9223372036854775807, -9223372036854775809)
    );

    // Each expected value is StarRocks 4.0's, from its backend: the
    // `IF(RAND() < 2, x, NULL)` wrapper keeps its frontend from folding x.
    const cases = [_]Case{
        .{ .sql = "SELECT CAST(l AS BIGINT) FROM nw ORDER BY id", .expected = &.{ "7", NULL, NULL, NULL } },
        .{ .sql = "SELECT CAST(l AS INT) FROM nw ORDER BY id", .expected = &.{ "7", NULL, NULL, NULL } },
        .{ .sql = "SELECT CAST(b AS INT) FROM nw ORDER BY id", .expected = &.{ "7", NULL, NULL, NULL } },
        .{ .sql = "SELECT LEFT('abcdefghi', b) FROM nw ORDER BY id", .expected = &.{ "abcdefg", NULL, NULL, NULL } },
        .{ .sql = "SELECT LEFT('abcdefghi', l) FROM nw ORDER BY id", .expected = &.{ "abcdefg", NULL, NULL, NULL } },
        .{ .sql = "SELECT SUBSTR('abcdefghi', 2, b) FROM nw ORDER BY id", .expected = &.{ "bcdefgh", NULL, NULL, NULL } },
        .{ .sql = "SELECT LEFT('abcdefghi', b - 2147483641) FROM nw ORDER BY id", .expected = &.{ "", "abcdefg", NULL, NULL } },
        .{ .sql = "SELECT CAST(CAST('9223372036854775807' AS LARGEINT) AS BIGINT)", .expected = &.{"9223372036854775807"} },
        .{ .sql = "SELECT CAST(CAST('-9223372036854775808' AS LARGEINT) AS BIGINT)", .expected = &.{"-9223372036854775808"} },
        .{ .sql = "SELECT CAST(IF(RAND() < 2, CAST('9223372036854775808' AS LARGEINT), NULL) AS BIGINT)", .expected = &.{NULL} },
        .{ .sql = "SELECT CAST(IF(RAND() < 2, CAST('18446744073709551615' AS LARGEINT), NULL) AS BIGINT)", .expected = &.{NULL} },
        .{ .sql = "SELECT LEFT('abcdef', 4294967298)", .expected = &.{NULL} },
        .{ .sql = "SELECT LEFT('abcdef', IF(RAND() < 2, 4294967298, NULL))", .expected = &.{NULL} },
        .{ .sql = "SELECT LEFT('abcdef', IF(RAND() < 2, 2147483647, NULL))", .expected = &.{"abcdef"} },
        .{ .sql = "SELECT RIGHT('abcdef', IF(RAND() < 2, 4294967298, NULL))", .expected = &.{NULL} },
        .{ .sql = "SELECT SUBSTR('abcdef', 2, IF(RAND() < 2, 4294967298, NULL))", .expected = &.{NULL} },
        .{ .sql = "SELECT SUBSTRING_INDEX('a.b.c', '.', IF(RAND() < 2, 4294967297, NULL))", .expected = &.{NULL} },
        .{ .sql = "SELECT ROUND(1.25e0, IF(RAND() < 2, 4294967297, NULL))", .expected = &.{NULL} },
        .{ .sql = "SELECT LEFT('abcdef', IF(RAND() < 2, 1e15, NULL))", .expected = &.{NULL} },
        .{ .sql = "SELECT LEFT('abcdef', IF(RAND() < 2, 1e100, NULL))", .expected = &.{NULL} },
        .{ .sql = "SELECT LEFT('abcdef', IF(RAND() < 2, 2.5e0, NULL))", .expected = &.{"ab"} },
        .{ .sql = "SELECT LEFT('abcdef', IF(RAND() < 2, 1000000000000000.0, NULL))", .expected = &.{NULL} },
        .{ .sql = "SELECT bitnot(IF(RAND() < 2, 1e100, NULL))", .expected = &.{NULL} },
        .{ .sql = "SELECT bit_shift_left(1, IF(RAND() < 2, 1e100, NULL))", .expected = &.{NULL} },
    };
    // MySQL's spellings, where they parse: the BIGINT cast's names and
    // INTERVAL arithmetic.
    const mysql_cases = [_]Case{
        .{ .sql = "SELECT CAST(l AS SIGNED) FROM nw ORDER BY id", .expected = &.{ "7", NULL, NULL, NULL } },
        .{ .sql = "SELECT CAST(IF(RAND() < 2, CAST('18446744073709551615' AS LARGEINT), NULL) AS SIGNED)", .expected = &.{NULL} },
        .{ .sql = "SELECT CAST(IF(RAND() < 2, CAST('18446744073709551615' AS LARGEINT), NULL) AS UNSIGNED)", .expected = &.{NULL} },
        // Text read as a date beside a count that narrows to INT.
        .{ .sql = "SELECT CAST(DATE_ADD('2020-01-31', INTERVAL id + 1 MONTH) AS CHAR) FROM nw ORDER BY id", .expected = &.{ "2020-03-31 00:00:00", "2020-04-30 00:00:00", "2020-05-31 00:00:00", "2020-06-30 00:00:00" } },
        .{ .sql = "SELECT CAST(DATE_ADD('2020-01-01', INTERVAL b DAY) AS CHAR) FROM nw ORDER BY id", .expected = &.{ "2020-01-08 00:00:00", NULL, NULL, NULL } },
    };
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("nw", .{})).flush();
        for ([_]Run{ .neutral, .mysql, .postgres, .mysql_session }) |run| {
            const all = [_][]const Case{ &cases, if (run == .postgres) &.{} else &mysql_cases };
            for (all) |list| for (list) |c| {
                expectRunTexts(allocator, db, run, c) catch |err| {
                    std.debug.print("case failed ({s}, {t}): {s}\n", .{ @errorName(err), run, c.sql });
                    return err;
                };
            };
        }
    }
}

fn expectLargeintCases(allocator: std.mem.Allocator, db: *thindb.Database, cases: anytype) !void {
    inline for (cases) |c| {
        var q = try helpers.runSqlMysql(allocator, db, c[0]);
        defer q.deinit();
        const is_largeint = q.outputSchema()[0].type == .largeint;
        const got = try helpers.columnText(allocator, &q);
        defer helpers.freeStrings(allocator, got);
        std.testing.expectEqual(@as(usize, 1), got.len) catch |err| {
            std.debug.print("case failed: {s}\n", .{c[0]});
            return err;
        };
        std.testing.expectEqualStrings(c[1], got[0] orelse NULL) catch |err| {
            std.debug.print("case failed: {s}\n", .{c[0]});
            return err;
        };
        std.testing.expectEqual(c[2], is_largeint) catch |err| {
            std.debug.print("case failed on its type: {s}\n", .{c[0]});
            return err;
        };
    }
}

test "cast: LARGEINT reads text, numbers, dates and datetimes as StarRocks does (issue #410)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    // Each expected value is StarRocks 4.0's, from its backend: the
    // `IF(RAND() < 2, x, NULL)` wrapper keeps its frontend from folding x.
    // The third field is whether the result is a LARGEINT.
    try expectLargeintCases(allocator, db, .{
        .{ "SELECT CAST(1 AS LARGEINT)", "1", true },
        .{ "SELECT CAST(IF(RAND() < 2, 1, NULL) AS LARGEINT)", "1", true },
        .{ "SELECT CAST(170141183460469231731687303715884105727 AS LARGEINT)", "170141183460469231731687303715884105727", true },
        .{ "SELECT CAST(-170141183460469231731687303715884105728 AS LARGEINT)", "-170141183460469231731687303715884105728", true },
        .{ "SELECT CAST(IF(RAND() < 2, '170141183460469231731687303715884105727', NULL) AS LARGEINT)", "170141183460469231731687303715884105727", true },
        .{ "SELECT CAST(IF(RAND() < 2, '-170141183460469231731687303715884105728', NULL) AS LARGEINT)", "-170141183460469231731687303715884105728", true },
        .{ "SELECT CAST(IF(RAND() < 2, '170141183460469231731687303715884105728', NULL) AS LARGEINT)", NULL, true },
        .{ "SELECT CAST(IF(RAND() < 2, '12abc', NULL) AS LARGEINT)", NULL, true },
        .{ "SELECT CAST(IF(RAND() < 2, ' 12 ', NULL) AS LARGEINT)", "12", true },
        .{ "SELECT CAST(IF(RAND() < 2, '+5', NULL) AS LARGEINT)", "5", true },
        .{ "SELECT CAST(IF(RAND() < 2, '12.5', NULL) AS LARGEINT)", NULL, true },
        .{ "SELECT CAST(IF(RAND() < 2, '1e3', NULL) AS LARGEINT)", NULL, true },
        .{ "SELECT CAST(IF(RAND() < 2, '0x10', NULL) AS LARGEINT)", NULL, true },
        .{ "SELECT CAST(IF(RAND() < 2, '', NULL) AS LARGEINT)", NULL, true },
        .{ "SELECT CAST(IF(RAND() < 2, 12.7, NULL) AS LARGEINT)", "12", true },
        .{ "SELECT CAST(IF(RAND() < 2, -12.7, NULL) AS LARGEINT)", "-12", true },
        .{ "SELECT CAST(IF(RAND() < 2, 12.7e0, NULL) AS LARGEINT)", "12", true },
        .{ "SELECT CAST(IF(RAND() < 2, 16777217.0e0, NULL) AS LARGEINT)", "16777217", true },
        .{ "SELECT CAST(IF(RAND() < 2, 1e20, NULL) AS LARGEINT)", "100000000000000000000", true },
        .{ "SELECT CAST(IF(RAND() < 2, 1e39, NULL) AS LARGEINT)", NULL, true },
        .{ "SELECT CAST(IF(RAND() < 2, 99999999999999999999999999999999999999, NULL) AS LARGEINT)", "99999999999999999999999999999999999999", true },
        .{ "SELECT CAST(IF(RAND() < 2, TRUE, NULL) AS LARGEINT)", "1", true },
        .{ "SELECT CAST(IF(RAND() < 2, DATE '2026-01-01', NULL) AS LARGEINT)", "20260101", true },
        .{ "SELECT CAST(IF(RAND() < 2, DATE '0000-01-01', NULL) AS LARGEINT)", "101", true },
        .{ "SELECT CAST(IF(RAND() < 2, CAST('2026-01-01 10:30:00' AS DATETIME), NULL) AS LARGEINT)", "20260101103000", true },
        .{ "SELECT CAST(IF(RAND() < 2, CAST('2026-01-01 10:30:00.4' AS DATETIME), NULL) AS LARGEINT)", "20260101103000", true },
        .{ "SELECT CAST(IF(RAND() < 2, CAST('2026-01-01 23:59:59.5' AS DATETIME), NULL) AS LARGEINT)", "20260101235959", true },
        .{ "SELECT CAST(IF(RAND() < 2, CAST('9999-12-31 23:59:59' AS DATETIME), NULL) AS LARGEINT)", "99991231235959", true },
        .{ "SELECT CAST(CAST(CAST(IF(RAND() < 2, 20260101, NULL) AS LARGEINT) AS DATE) AS CHAR)", "2026-01-01", false },
        .{ "SELECT CAST(CAST(IF(RAND() < 2, '170141183460469231731687303715884105727', NULL) AS LARGEINT) AS BIGINT)", NULL, false },
        .{ "SELECT CAST(CAST(IF(RAND() < 2, '100', NULL) AS LARGEINT) AS DOUBLE)", "100", false },
        .{ "SELECT CAST(CAST(IF(RAND() < 2, '170141183460469231731687303715884105727', NULL) AS LARGEINT) AS VARCHAR)", "170141183460469231731687303715884105727", false },
        .{ "SELECT CAST(CAST(CAST(IF(RAND() < 2, '123456789012345678901234567890', NULL) AS LARGEINT) AS DECIMAL(38, 0)) AS CHAR)", "123456789012345678901234567890", false },
        .{ "SELECT CAST(IF(RAND() < 2, '170141183460469231731687303715884105727', NULL) AS LARGEINT) + 1", "-170141183460469231731687303715884105728", true },
        .{ "SELECT CAST(IF(RAND() < 2, 9223372036854775807, NULL) AS LARGEINT) * 2", "18446744073709551614", true },
        .{ "SELECT CAST(IF(RAND() < 2, '123456789012345678901234567890', NULL) AS LARGEINT) DIV 7", "17636684144620811271604938270", true },
        .{ "SELECT CAST(IF(RAND() < 2, '123456789012345678901234567890', NULL) AS LARGEINT) % 7", "0", true },
        .{ "SELECT CAST(IF(RAND() < 2, 1, NULL) AS LARGEINT) = 1", "1", false },
        .{ "SELECT ABS(CAST(IF(RAND() < 2, '-123456789012345678901234567890', NULL) AS LARGEINT))", "123456789012345678901234567890", true },
        .{ "SELECT ABS(CAST(IF(RAND() < 2, '-5', NULL) AS LARGEINT))", "5", true },
        .{ "SELECT ABS(CAST(IF(RAND() < 2, '-170141183460469231731687303715884105728', NULL) AS LARGEINT))", "-170141183460469231731687303715884105728", true },
        .{ "SELECT -CAST(IF(RAND() < 2, '-170141183460469231731687303715884105728', NULL) AS LARGEINT)", "-170141183460469231731687303715884105728", true },
        // An integer literal past DECIMAL's 38 digits is a LARGEINT while
        // it fits, LARGEINT's minimum included.
        .{ "SELECT IF(RAND() < 2, 170141183460469231731687303715884105727, NULL)", "170141183460469231731687303715884105727", true },
        .{ "SELECT IF(RAND() < 2, 170141183460469231731687303715884105727, NULL) + 1", "-170141183460469231731687303715884105728", true },
        .{ "SELECT IF(RAND() < 2, 150000000000000000000000000000000000000, NULL)", "150000000000000000000000000000000000000", true },
        .{ "SELECT IF(RAND() < 2, 150000000000000000000000000000000000000, NULL) + 1", "150000000000000000000000000000000000001", true },
        .{ "SELECT IF(RAND() < 2, 150000000000000000000000000000000000000, NULL) = 150000000000000000000000000000000000000", "1", false },
        .{ "SELECT -IF(RAND() < 2, 150000000000000000000000000000000000000, NULL)", "-150000000000000000000000000000000000000", true },
        .{ "SELECT IF(RAND() < 2, -150000000000000000000000000000000000000, NULL)", "-150000000000000000000000000000000000000", true },
        .{ "SELECT -170141183460469231731687303715884105728", "-170141183460469231731687303715884105728", true },
        .{ "SELECT - 170141183460469231731687303715884105728", "-170141183460469231731687303715884105728", true },
        .{ "SELECT IF(RAND() < 2, -170141183460469231731687303715884105728, NULL)", "-170141183460469231731687303715884105728", true },
        .{ "SELECT -170141183460469231731687303715884105728 + 1", "-170141183460469231731687303715884105727", true },
    });
}

test "LARGEINT columns keep every digit through keys, aggregates, joins, ALTER and a reopen (issue #410)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        try helpers.exec(allocator, db, "CREATE TABLE lgt (id LARGEINT PRIMARY KEY, v LARGEINT, g INT)");
        // Integer literals past BIGINT land in their column exactly, on
        // either side of DECIMAL's 38 digits, in one VALUES list.
        try helpers.exec(allocator, db,
            \\INSERT INTO lgt VALUES
            \\  (170141183460469231731687303715884105727, 1, 1),
            \\  (-170141183460469231731687303715884105728, -150000000000000000000000000000000000000, 1),
            \\  (1, 99999999999999999999999999999999999999, 2),
            \\  (2, NULL, 2),
            \\  (3, 9223372036854775808, 3)
        );
        try helpers.exec(allocator, db, "CREATE TABLE lgj (k LARGEINT, name VARCHAR(10))");
        try helpers.exec(allocator, db, "INSERT INTO lgj VALUES (170141183460469231731687303715884105727, 'max'), (3, 'three'), (-170141183460469231731687303715884105728, 'min')");

        for (0..2) |pass| {
            if (pass == 1) try (try db.openTable("lgt", .{})).flush();
            const cases = [_]Case{
                .{ .sql = "SELECT CAST(v AS CHAR) FROM lgt ORDER BY id", .expected = &.{ "-150000000000000000000000000000000000000", "99999999999999999999999999999999999999", NULL, "9223372036854775808", "1" } },
                .{ .sql = "SELECT CAST(id AS CHAR) FROM lgt WHERE id = 170141183460469231731687303715884105727", .expected = &.{"170141183460469231731687303715884105727"} },
                .{ .sql = "SELECT CAST(id AS CHAR) FROM lgt WHERE id = -170141183460469231731687303715884105728", .expected = &.{"-170141183460469231731687303715884105728"} },
                .{ .sql = "SELECT CAST(id AS CHAR) FROM lgt WHERE v > 9223372036854775807 ORDER BY id", .expected = &.{ "1", "3" } },
                .{ .sql = "SELECT CAST(v AS CHAR) FROM lgt WHERE v IN (1, 99999999999999999999999999999999999999) ORDER BY id", .expected = &.{ "99999999999999999999999999999999999999", "1" } },
                .{ .sql = "SELECT CAST(v AS CHAR) FROM lgt WHERE v BETWEEN 0 AND 100000000000000000000000000000000000000 ORDER BY id", .expected = &.{ "99999999999999999999999999999999999999", "9223372036854775808", "1" } },
                .{ .sql = "SELECT CAST(SUM(v) AS CHAR) FROM lgt GROUP BY g ORDER BY g", .expected = &.{ "-149999999999999999999999999999999999999", "99999999999999999999999999999999999999", "9223372036854775808" } },
                .{ .sql = "SELECT CAST(MIN(v) AS CHAR) FROM lgt GROUP BY g ORDER BY g", .expected = &.{ "-150000000000000000000000000000000000000", "99999999999999999999999999999999999999", "9223372036854775808" } },
                .{ .sql = "SELECT CAST(MAX(v) AS CHAR) FROM lgt GROUP BY g ORDER BY g", .expected = &.{ "1", "99999999999999999999999999999999999999", "9223372036854775808" } },
                .{ .sql = "SELECT CAST(v AS CHAR) FROM lgt GROUP BY v ORDER BY v", .expected = &.{ NULL, "-150000000000000000000000000000000000000", "1", "9223372036854775808", "99999999999999999999999999999999999999" } },
                .{ .sql = "SELECT CAST(COUNT(DISTINCT v) AS CHAR) FROM lgt", .expected = &.{"4"} },
                .{ .sql = "SELECT j.name FROM lgt t JOIN lgj j ON t.id = j.k ORDER BY j.name", .expected = &.{ "max", "min", "three" } },
                .{ .sql = "SELECT CAST(ABS(v) AS CHAR) FROM lgt ORDER BY id", .expected = &.{ "150000000000000000000000000000000000000", "99999999999999999999999999999999999999", NULL, "9223372036854775808", "1" } },
            };
            for (cases) |c| {
                expectTexts(allocator, db, c.sql, c.expected) catch |err| {
                    std.debug.print("case failed ({s}): {s}\n", .{ @errorName(err), c.sql });
                    return err;
                };
            }
        }

        try helpers.exec(allocator, db, "UPDATE lgt SET v = v + 1 WHERE id = 1");
        try helpers.exec(allocator, db, "DELETE FROM lgt WHERE id = 2");
        try helpers.exec(allocator, db, "ALTER TABLE lgt ADD COLUMN w LARGEINT DEFAULT -170141183460469231731687303715884105728");
        try helpers.exec(allocator, db, "INSERT INTO lgt (id, v, g, w) VALUES (10, '12345678901234567890123456789', 4, 170141183460469231731687303715884105727)");
        try std.testing.expectError(error.ValueOutOfRange, helpers.exec(allocator, db, "INSERT INTO lgt (id, v, g) VALUES (170141183460469231731687303715884105728, 1, 1)"));
        try (try db.openTable("lgt", .{})).flush();
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try expectTexts(allocator, db, "SELECT CONCAT(CAST(id AS CHAR), ' ', CAST(v AS CHAR), ' ', CAST(w AS CHAR)) FROM lgt ORDER BY id", &.{
        "-170141183460469231731687303715884105728 -150000000000000000000000000000000000000 -170141183460469231731687303715884105728",
        "1 100000000000000000000000000000000000000 -170141183460469231731687303715884105728",
        "3 9223372036854775808 -170141183460469231731687303715884105728",
        "10 12345678901234567890123456789 170141183460469231731687303715884105727",
        "170141183460469231731687303715884105727 1 -170141183460469231731687303715884105728",
    });
}
