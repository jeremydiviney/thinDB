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
    // MySQL's INTERVAL arithmetic, where it parses. Its SIGNED and UNSIGNED
    // spellings have a test of their own.
    const mysql_cases = [_]Case{
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

/// A statement whose result in the MySQL dialect (a MySQL wire connection
/// too) differs from the neutral and PG dialects'.
const DialectCase = struct { sql: []const u8, mysql: []const []const u8, other: []const []const u8 };

test "cast: MySQL's SIGNED and UNSIGNED keep an integer's 64 bits and clamp a fraction in the MySQL dialect alone (issue #479)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    // l and s hold 2^63-1, 2^63, 2^64-1, 2^64, -1, -2^63 and -2^63-1.
    try helpers.exec(allocator, db, "CREATE TABLE sb (id INT NOT NULL, l LARGEINT NOT NULL, s VARCHAR(30) NOT NULL, b BIGINT NOT NULL, x DOUBLE NOT NULL, d DECIMAL(21,1) NOT NULL)");
    try helpers.exec(allocator, db,
        \\INSERT INTO sb VALUES
        \\  (1, 9223372036854775807, '9223372036854775807', 5, 9.3e18, 18446744073709551615.5),
        \\  (2, 9223372036854775808, '9223372036854775808', 0, 1e19, 9223372036854775808.4),
        \\  (3, 18446744073709551615, '18446744073709551615', -1, 1.8446744073709552e19, 18446744073709551616.5),
        \\  (4, 18446744073709551616, '18446744073709551616', 9223372036854775807, 2e19, -9223372036854775809.5),
        \\  (5, -1, '-1', -9223372036854775808, -1e0, -0.4),
        \\  (6, -9223372036854775808, '-9223372036854775808', 1, -9.3e18, 9223372036854775807.4),
        \\  (7, -9223372036854775809, '-9223372036854775809', 2, 2.5e0, 18446744073709551614.4)
    );
    // MySQL 8.4.11 gives these for x and d, each column read from a derived
    // table of the same values: a double or a decimal with a fraction clamps
    // (with a warning) rather than wrapping.
    const x_signed = &[_][]const u8{ "9223372036854775807", "9223372036854775807", "9223372036854775807", "9223372036854775807", "-1", "-9223372036854775808", "2" };
    const x_unsigned = &[_][]const u8{ "9223372036854775807", "9223372036854775807", "9223372036854775807", "9223372036854775807", "18446744073709551615", "9223372036854775808", "2" };
    const x_bigint = &[_][]const u8{ NULL, NULL, NULL, NULL, "-1", NULL, "2" };
    const d_signed = &[_][]const u8{ "9223372036854775807", "9223372036854775807", "9223372036854775807", "-9223372036854775808", "0", "9223372036854775807", "9223372036854775807" };
    const d_unsigned = &[_][]const u8{ "18446744073709551615", "9223372036854775808", "18446744073709551615", "9223372036854775808", "0", "9223372036854775807", "18446744073709551614" };
    const d_bigint = &[_][]const u8{ NULL, NULL, NULL, NULL, "0", "9223372036854775807", NULL };
    const signed_bits = &[_][]const u8{ "9223372036854775807", "-9223372036854775808", "-1", NULL, "-1", "-9223372036854775808", NULL };
    const unsigned_bits = &[_][]const u8{ "9223372036854775807", "9223372036854775808", "18446744073709551615", NULL, "18446744073709551615", "9223372036854775808", NULL };
    const in_bigint = &[_][]const u8{ "9223372036854775807", NULL, NULL, NULL, "-1", "-9223372036854775808", NULL };
    const b_signed = &[_][]const u8{ "5", "0", "-1", "9223372036854775807", "-9223372036854775808", "1", "2" };
    const b_unsigned = &[_][]const u8{ "5", "0", "18446744073709551615", "9223372036854775807", "9223372036854775808", "1", "2" };
    const not_b_signed = &[_][]const u8{ "-6", "-1", "0", "-9223372036854775808", "9223372036854775807", "-2", "-3" };
    const not_b_unsigned = &[_][]const u8{ "18446744073709551610", "18446744073709551615", "0", "9223372036854775808", "9223372036854775807", "18446744073709551614", "18446744073709551613" };
    const cases = [_]DialectCase{
        .{ .sql = "SELECT CAST(l AS SIGNED) FROM sb ORDER BY id", .mysql = signed_bits, .other = in_bigint },
        .{ .sql = "SELECT CAST(l AS SIGNED INTEGER) FROM sb ORDER BY id", .mysql = signed_bits, .other = in_bigint },
        .{ .sql = "SELECT CAST(s AS SIGNED) FROM sb ORDER BY id", .mysql = signed_bits, .other = in_bigint },
        .{ .sql = "SELECT CAST(l AS UNSIGNED) FROM sb ORDER BY id", .mysql = unsigned_bits, .other = in_bigint },
        .{ .sql = "SELECT CAST(l AS UNSIGNED INT) FROM sb ORDER BY id", .mysql = unsigned_bits, .other = in_bigint },
        .{ .sql = "SELECT CAST(s AS UNSIGNED) FROM sb ORDER BY id", .mysql = unsigned_bits, .other = in_bigint },
        .{ .sql = "SELECT CAST(l AS BIGINT) FROM sb ORDER BY id", .mysql = in_bigint, .other = in_bigint },
        .{ .sql = "SELECT CAST(s AS BIGINT) FROM sb ORDER BY id", .mysql = in_bigint, .other = in_bigint },
        .{ .sql = "SELECT CAST(b AS SIGNED) FROM sb ORDER BY id", .mysql = b_signed, .other = b_signed },
        .{ .sql = "SELECT CAST(b AS UNSIGNED) FROM sb ORDER BY id", .mysql = b_unsigned, .other = b_signed },
        .{ .sql = "SELECT CAST(~b AS SIGNED) FROM sb ORDER BY id", .mysql = not_b_signed, .other = not_b_signed },
        .{ .sql = "SELECT CAST(~b AS UNSIGNED) FROM sb ORDER BY id", .mysql = not_b_unsigned, .other = not_b_signed },
        // MySQL 8.4.11 gives each MySQL value from here through CONVERT, but
        // for 2^64 and -2^63-1, which it clamps with a warning (past 64 bits
        // stays StarRocks' NULL), and the LARGEINT casts it has no type for.
        .{ .sql = "SELECT CAST(9223372036854775807 AS SIGNED)", .mysql = &.{"9223372036854775807"}, .other = &.{"9223372036854775807"} },
        .{ .sql = "SELECT CAST(9223372036854775808 AS SIGNED)", .mysql = &.{"-9223372036854775808"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(18446744073709551615 AS SIGNED)", .mysql = &.{"-1"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(18446744073709551616 AS SIGNED)", .mysql = &.{NULL}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(-1 AS SIGNED)", .mysql = &.{"-1"}, .other = &.{"-1"} },
        .{ .sql = "SELECT CAST(-9223372036854775808 AS SIGNED)", .mysql = &.{"-9223372036854775808"}, .other = &.{"-9223372036854775808"} },
        .{ .sql = "SELECT CAST(-9223372036854775809 AS SIGNED)", .mysql = &.{NULL}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(9223372036854775807 AS UNSIGNED)", .mysql = &.{"9223372036854775807"}, .other = &.{"9223372036854775807"} },
        .{ .sql = "SELECT CAST(9223372036854775808 AS UNSIGNED)", .mysql = &.{"9223372036854775808"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(18446744073709551615 AS UNSIGNED)", .mysql = &.{"18446744073709551615"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(18446744073709551616 AS UNSIGNED)", .mysql = &.{NULL}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(-1 AS UNSIGNED)", .mysql = &.{"18446744073709551615"}, .other = &.{"-1"} },
        .{ .sql = "SELECT CAST(-9223372036854775808 AS UNSIGNED)", .mysql = &.{"9223372036854775808"}, .other = &.{"-9223372036854775808"} },
        .{ .sql = "SELECT CAST(-9223372036854775809 AS UNSIGNED)", .mysql = &.{NULL}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(IF(RAND() < 2, CAST('18446744073709551615' AS LARGEINT), NULL) AS SIGNED)", .mysql = &.{"-1"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(IF(RAND() < 2, CAST('18446744073709551615' AS LARGEINT), NULL) AS UNSIGNED)", .mysql = &.{"18446744073709551615"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(~5 AS SIGNED)", .mysql = &.{"-6"}, .other = &.{"-6"} },
        .{ .sql = "SELECT CAST(~5 AS UNSIGNED)", .mysql = &.{"18446744073709551610"}, .other = &.{"-6"} },
        .{ .sql = "SELECT CAST(~0 AS SIGNED)", .mysql = &.{"-1"}, .other = &.{"-1"} },
        .{ .sql = "SELECT CAST(~0 AS UNSIGNED)", .mysql = &.{"18446744073709551615"}, .other = &.{"-1"} },
        .{ .sql = "SELECT CAST(~9223372036854775807 AS SIGNED)", .mysql = &.{"-9223372036854775808"}, .other = &.{"-9223372036854775808"} },
        .{ .sql = "SELECT CAST(~9223372036854775807 AS UNSIGNED)", .mysql = &.{"9223372036854775808"}, .other = &.{"-9223372036854775808"} },
        .{ .sql = "SELECT CONVERT(~5, SIGNED)", .mysql = &.{"-6"}, .other = &.{"-6"} },
        // The BIGINT spelling keeps StarRocks' NULL past BIGINT's range.
        .{ .sql = "SELECT CAST(~5 AS BIGINT)", .mysql = &.{NULL}, .other = &.{"-6"} },
        .{ .sql = "SELECT CAST(NULL AS UNSIGNED)", .mysql = &.{NULL}, .other = &.{NULL} },
        // A DECIMAL with scale 0 wraps like the integer literal it stands for
        // (MySQL clamps a DECIMAL(20,0) value, but its literal is BIGINT
        // UNSIGNED).
        .{ .sql = "SELECT CAST(CAST(18446744073709551615 AS DECIMAL(20,0)) AS SIGNED)", .mysql = &.{"-1"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(CAST(-1 AS DECIMAL(20,0)) AS UNSIGNED)", .mysql = &.{"18446744073709551615"}, .other = &.{"-1"} },
        .{ .sql = "SELECT CAST(x AS SIGNED) FROM sb ORDER BY id", .mysql = x_signed, .other = x_bigint },
        .{ .sql = "SELECT CAST(x AS UNSIGNED) FROM sb ORDER BY id", .mysql = x_unsigned, .other = x_bigint },
        .{ .sql = "SELECT CAST(d AS SIGNED) FROM sb ORDER BY id", .mysql = d_signed, .other = d_bigint },
        .{ .sql = "SELECT CAST(d AS UNSIGNED) FROM sb ORDER BY id", .mysql = d_unsigned, .other = d_bigint },
        // Each MySQL value below was checked against MySQL 8.4.11. Its fraction
        // is one MySQL's rounding and thinDB's truncation agree on.
        .{ .sql = "SELECT CAST(9.3e18 AS SIGNED)", .mysql = &.{"9223372036854775807"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(1e19 AS SIGNED)", .mysql = &.{"9223372036854775807"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(1.8446744073709552e19 AS SIGNED)", .mysql = &.{"9223372036854775807"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(2e19 AS SIGNED)", .mysql = &.{"9223372036854775807"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(2.5e0 AS SIGNED)", .mysql = &.{"2"}, .other = &.{"2"} },
        .{ .sql = "SELECT CAST(-2.5e0 AS SIGNED)", .mysql = &.{"-2"}, .other = &.{"-2"} },
        .{ .sql = "SELECT CAST(9.3e18 AS UNSIGNED)", .mysql = &.{"9223372036854775807"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(1e19 AS UNSIGNED)", .mysql = &.{"9223372036854775807"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(2e19 AS UNSIGNED)", .mysql = &.{"9223372036854775807"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(-1e0 AS UNSIGNED)", .mysql = &.{"18446744073709551615"}, .other = &.{"-1"} },
        .{ .sql = "SELECT CAST(-2.5e0 AS UNSIGNED)", .mysql = &.{"18446744073709551614"}, .other = &.{"-2"} },
        .{ .sql = "SELECT CAST(-0.4e0 AS UNSIGNED)", .mysql = &.{"0"}, .other = &.{"0"} },
        .{ .sql = "SELECT CAST(x AS SIGNED) FROM (SELECT -1e19 AS x) t", .mysql = &.{"-9223372036854775808"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(x AS UNSIGNED) FROM (SELECT -1e19 AS x) t", .mysql = &.{"9223372036854775808"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(x AS SIGNED) FROM (SELECT 1e300 AS x) t", .mysql = &.{"9223372036854775807"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(x AS UNSIGNED) FROM (SELECT -1e300 AS x) t", .mysql = &.{"9223372036854775808"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(18446744073709551615.5 AS SIGNED)", .mysql = &.{"9223372036854775807"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(9223372036854775808.5 AS SIGNED)", .mysql = &.{"9223372036854775807"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(9223372036854775807.5 AS SIGNED)", .mysql = &.{"9223372036854775807"}, .other = &.{"9223372036854775807"} },
        .{ .sql = "SELECT CAST(18446744073709551616.5 AS SIGNED)", .mysql = &.{"9223372036854775807"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(-9223372036854775808.5 AS SIGNED)", .mysql = &.{"-9223372036854775808"}, .other = &.{"-9223372036854775808"} },
        .{ .sql = "SELECT CAST(-9223372036854775809.5 AS SIGNED)", .mysql = &.{"-9223372036854775808"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(-1.4 AS SIGNED)", .mysql = &.{"-1"}, .other = &.{"-1"} },
        .{ .sql = "SELECT CAST(18446744073709551615.5 AS UNSIGNED)", .mysql = &.{"18446744073709551615"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(18446744073709551614.4 AS UNSIGNED)", .mysql = &.{"18446744073709551614"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(18446744073709551616.5 AS UNSIGNED)", .mysql = &.{"18446744073709551615"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(9223372036854775808.4 AS UNSIGNED)", .mysql = &.{"9223372036854775808"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(-9223372036854775809.5 AS UNSIGNED)", .mysql = &.{"9223372036854775808"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST(-1.0 AS UNSIGNED)", .mysql = &.{"18446744073709551615"}, .other = &.{"-1"} },
        .{ .sql = "SELECT CAST(-0.4 AS UNSIGNED)", .mysql = &.{"0"}, .other = &.{"0"} },
        .{ .sql = "SELECT CAST('9223372036854775808' AS SIGNED)", .mysql = &.{"-9223372036854775808"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST('18446744073709551615' AS SIGNED)", .mysql = &.{"-1"}, .other = &.{NULL} },
        .{ .sql = "SELECT CAST('-1' AS UNSIGNED)", .mysql = &.{"18446744073709551615"}, .other = &.{"-1"} },
        .{ .sql = "SELECT CAST('18446744073709551615' AS UNSIGNED)", .mysql = &.{"18446744073709551615"}, .other = &.{NULL} },
    };
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("sb", .{})).flush();
        for ([_]Run{ .neutral, .mysql, .postgres, .mysql_session }) |run| {
            for (cases) |c| {
                const expected = switch (run) {
                    .mysql, .mysql_session => c.mysql,
                    .neutral, .postgres => c.other,
                };
                expectRunTexts(allocator, db, run, .{ .sql = c.sql, .expected = expected }) catch |err| {
                    std.debug.print("case failed ({s}, {t}): {s}\n", .{ @errorName(err), run, c.sql });
                    return err;
                };
            }
        }
    }

    const result_types = .{
        .{ "SELECT CAST(b AS SIGNED) FROM sb", thindb.types.Dialect.mysql, thindb.types.Type.bigint },
        .{ "SELECT CAST(b AS UNSIGNED) FROM sb", thindb.types.Dialect.mysql, thindb.types.Type.largeint },
        .{ "SELECT CAST(NULL AS UNSIGNED)", thindb.types.Dialect.mysql, thindb.types.Type.largeint },
        .{ "SELECT CAST(b AS UNSIGNED) FROM sb", thindb.types.Dialect.neutral, thindb.types.Type.bigint },
        .{ "SELECT CAST(b AS UNSIGNED) FROM sb", thindb.types.Dialect.postgres, thindb.types.Type.bigint },
    };
    inline for (result_types) |c| {
        var q = try helpers.runSqlDialect(allocator, db, c[0], c[1]);
        defer q.deinit();
        try std.testing.expectEqual(c[2], q.outputSchema()[0].type);
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

test "cast: FLOAT is 32-bit: the nearest f32, NULL past its range (issue #551)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setup(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{
        .{ .sql = "SELECT CAST(CAST(x AS FLOAT) AS CHAR) FROM ct WHERE id <= 5 ORDER BY id", .expected = &.{ "2.5", "-2.5", "1e20", NULL, "0" } },
        .{ .sql = "SELECT CAST(CAST(d AS FLOAT) AS CHAR) FROM ct WHERE id <= 5 ORDER BY id", .expected = &.{ "2.5", "-2.5", "1.99", NULL, "0" } },
        .{ .sql = "SELECT CAST(CAST(s AS FLOAT) AS CHAR) FROM ct ORDER BY id", .expected = &.{ "12", "5", "1.7", NULL, NULL, "1000", "10000000000", "-1.005", NULL, NULL } },
        // A DECIMAL target reads an integer's f32; text would read the integer.
        .{ .sql = "SELECT CAST(CAST(CAST(b AS FLOAT) AS DECIMAL(20,0)) AS CHAR) FROM ct WHERE id <= 4 ORDER BY id", .expected = &.{ "12", "10000000000", "-10000000000", NULL } },
        .{ .sql = "SELECT CAST(CAST(CAST(16777217 AS FLOAT) AS DECIMAL(10,0)) AS CHAR) FROM ct WHERE id = 1", .expected = &.{"16777216"} },
        .{ .sql = "SELECT CAST(CAST('1.1691079661731327' AS FLOAT) AS CHAR) FROM ct WHERE id = 1", .expected = &.{"1.1691079"} },
        .{ .sql = "SELECT CAST(CAST(CAST('1e39' AS DOUBLE) AS FLOAT) AS CHAR) FROM ct WHERE id = 1", .expected = &.{NULL} },
        // Arithmetic reads the f32 value as a DOUBLE, as StarRocks does.
        .{ .sql = "SELECT CAST(SUM(CAST(x AS FLOAT) + 0.2) AS CHAR) FROM ct WHERE id = 1", .expected = &.{"2.7"} },
        .{ .sql = "SELECT CAST(SUM(f) AS CHAR) FROM (SELECT CAST(1.1 AS FLOAT) AS f FROM ct WHERE id = 1 UNION ALL SELECT CAST(2.2 AS FLOAT) FROM ct WHERE id = 1) t", .expected = &.{"3.3000000715255737"} },
        // 1.000000055 rounds to 1 as an f32, so the product is 10000000, not 10000000.55.
        .{ .sql = "SELECT CAST(ROUND(10000000 * CAST(1.000000055 AS FLOAT)) AS CHAR) FROM ct WHERE id = 1", .expected = &.{"10000000"} },
    });

    var q = try helpers.runSqlCtx(allocator, db, "SELECT CAST(x AS FLOAT) AS f, IF(id = 1, CAST(x AS FLOAT), CAST(d AS FLOAT)) AS g FROM ct WHERE id = 1");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(f32, 2.5), batch.values[0].data.float[0]);
    try std.testing.expectEqual(@as(f32, 2.5), batch.values[1].data.float[0]);
}

test "cast: an integer's FLOAT converted again converts the integer, as in StarRocks" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setup(allocator, db);

    // b + 16777205 is 16777217 on row 1, which FLOAT holds as 16777216.
    try expectCasesBeforeAndAfterFlush(allocator, db, &.{
        .{ .sql = "SELECT CAST(33554434 / CAST(b + 16777205 AS FLOAT) AS CHAR) FROM ct WHERE id = 1", .expected = &.{"2"} },
        .{ .sql = "SELECT CAST(CAST(b + 16777205 AS FLOAT) * 1 AS CHAR) FROM ct WHERE id = 1", .expected = &.{"16777217"} },
        .{ .sql = "SELECT CAST(CAST(CAST(b + 16777205 AS FLOAT) AS DOUBLE) AS CHAR) FROM ct WHERE id = 1", .expected = &.{"16777217"} },
        .{ .sql = "SELECT CAST(CAST(b + 16777205 AS FLOAT) AS CHAR) FROM ct WHERE id = 1", .expected = &.{"16777217"} },
        .{ .sql = "SELECT CAST(CASE WHEN id = 1 THEN CAST(b + 16777205 AS FLOAT) ELSE x END AS CHAR) FROM ct WHERE id = 1", .expected = &.{"16777217"} },
        .{ .sql = "SELECT CAST(IF(id = 1, CAST(b + 16777205 AS FLOAT), x) AS CHAR) FROM ct WHERE id = 1", .expected = &.{"16777217"} },
        // A DOUBLE source, an aggregate, a decimal target and a FLOAT column keep the f32.
        .{ .sql = "SELECT CAST(CAST(x / 3 AS FLOAT) * 3 AS CHAR) FROM ct WHERE id = 1", .expected = &.{"2.4999999403953552"} },
        .{ .sql = "SELECT CAST(SUM(CAST(b + 16777205 AS FLOAT)) AS CHAR) FROM ct WHERE id = 1", .expected = &.{"16777216"} },
        .{ .sql = "SELECT CAST(CAST(CAST(b + 16777205 AS FLOAT) AS DECIMAL(10,0)) AS CHAR) FROM ct WHERE id = 1", .expected = &.{"16777216"} },
        .{ .sql = "SELECT CAST(MAX(f * 1) AS CHAR) FROM (SELECT CAST(b + 16777205 AS FLOAT) AS f FROM ct WHERE id = 1) t", .expected = &.{"16777216"} },
        .{ .sql = "WITH t AS (SELECT CAST(b + 16777205 AS FLOAT) AS f FROM ct WHERE id = 1) SELECT CAST(MAX(f * 1) AS CHAR) FROM t", .expected = &.{"16777216"} },
    });
}
