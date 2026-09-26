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
            const text = if (view.isValid(i)) view.data.string.rowBytes(i) else NULL;
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
        .{ .sql = "SELECT CAST(CAST(s AS BOOLEAN) AS CHAR) FROM ct ORDER BY id", .expected = &.{ "true", "true", "true", NULL, NULL, "true", "true", "true", "false", NULL } },
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
