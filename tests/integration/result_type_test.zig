//! The result-type rule (`cast.commonType`) wherever one result may hold a
//! value of several types: CASE/IF branches, the arguments GREATEST, LEAST
//! and COALESCE return, UNION arms, and a string parameter given a number
//! or date. Each result is rendered as text, so a decimal read at the wrong
//! scale shows in the expected string.

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

fn expectCases(allocator: std.mem.Allocator, db: *thindb.Database, cases: []const Case) !void {
    for (cases) |c| {
        expectTexts(allocator, db, c.sql, c.expected) catch |err| {
            std.debug.print("case failed ({s}): {s}\n", .{ @errorName(err), c.sql });
            return err;
        };
    }
}

fn expectCasesBeforeAndAfterFlush(allocator: std.mem.Allocator, db: *thindb.Database, cases: []const Case) !void {
    try expectCases(allocator, db, cases);
    try (try db.openTable("rt", .{})).flush();
    try expectCases(allocator, db, cases);
}

fn setup(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    try helpers.exec(allocator, db,
        \\CREATE TABLE rt (id BIGINT PRIMARY KEY, a DECIMAL(10,2), b DECIMAL(12,4), i INT, x DOUBLE,
        \\  d DATE, ts DATETIME, s VARCHAR(20))
    );
    try helpers.exec(allocator, db,
        \\INSERT INTO rt VALUES
        \\  (1, 1.50, 1.5000, 2, 1.5, '2024-03-05', '2024-03-05 00:00:00', 'apple'),
        \\  (2, 3.25, 3.2400, 3, 3.25, '2024-03-06', '2024-03-05 10:00:00', 'pear'),
        \\  (3, 2.00, 2.0000, NULL, 2.5, NULL, '2024-03-07 00:00:00', 'kiwi')
    );
}

test "result type: CASE and IF branches meet at one type" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setup(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{
        .{ .sql = "SELECT CAST(CASE WHEN id = 1 THEN a ELSE b END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5000", "3.2400", "2.0000" } },
        .{ .sql = "SELECT CAST(CASE WHEN id = 1 THEN b ELSE a END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5000", "3.2500", "2.0000" } },
        .{ .sql = "SELECT CAST(IF(id = 1, a, b) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5000", "3.2400", "2.0000" } },
        .{ .sql = "SELECT CAST(CASE WHEN id = 2 THEN a ELSE 0 END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "0.00", "3.25", "0.00" } },
        .{ .sql = "SELECT CAST(CASE WHEN id = 1 THEN a ELSE x END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5", "3.25", "2.5" } },
        .{ .sql = "SELECT CAST(CASE WHEN id = 1 THEN d ELSE ts END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2024-03-05 00:00:00", "2024-03-05 10:00:00", "2024-03-07 00:00:00" } },
        .{ .sql = "SELECT CASE WHEN id = 1 THEN i ELSE s END FROM rt ORDER BY id", .expected = &.{ "2", "pear", "kiwi" } },
        .{ .sql = "SELECT CAST(CASE WHEN id = 3 THEN NULL ELSE b END AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5000", "3.2400", NULL } },
    });
}

test "result type: GREATEST, LEAST and COALESCE return their arguments' common type" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setup(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{
        .{ .sql = "SELECT CAST(GREATEST(a, b) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5000", "3.2500", "2.0000" } },
        .{ .sql = "SELECT CAST(GREATEST(a, x) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "1.5", "3.25", "2.5" } },
        .{ .sql = "SELECT CAST(LEAST(d, ts) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2024-03-05 00:00:00", "2024-03-05 10:00:00", NULL } },
        .{ .sql = "SELECT CAST(GREATEST(d, '2024-03-06') AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2024-03-06", "2024-03-06", NULL } },
        .{ .sql = "SELECT CAST(GREATEST(i, s) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "apple", "pear", NULL } },
        .{ .sql = "SELECT CAST(COALESCE(d, ts) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2024-03-05 00:00:00", "2024-03-06 00:00:00", "2024-03-07 00:00:00" } },
        .{ .sql = "SELECT CAST(COALESCE(i, a) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2.00", "3.00", "2.00" } },
        .{ .sql = "SELECT CAST(COALESCE(i, x) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2", "3", "2.5" } },
    });
}

test "result type: a string parameter takes a number or date as its text" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setup(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{
        .{ .sql = "SELECT CONCAT(s, i) FROM rt ORDER BY id", .expected = &.{ "apple2", "pear3", NULL } },
        .{ .sql = "SELECT CONCAT('Q', i, '-', d) FROM rt ORDER BY id", .expected = &.{ "Q2-2024-03-05", "Q3-2024-03-06", NULL } },
        .{ .sql = "SELECT CONCAT(a, '') FROM rt ORDER BY id", .expected = &.{ "1.50", "3.25", "2.00" } },
        .{ .sql = "SELECT CONCAT(s, ts) FROM rt ORDER BY id", .expected = &.{ "apple2024-03-05 00:00:00", "pear2024-03-05 10:00:00", "kiwi2024-03-07 00:00:00" } },
        .{ .sql = "SELECT CONCAT(s, x) FROM rt ORDER BY id", .expected = &.{ "apple1.5", "pear3.25", "kiwi2.5" } },
        .{ .sql = "SELECT CAST(LENGTH(b) AS CHAR) FROM rt ORDER BY id", .expected = &.{ "6", "6", "6" } },
        .{ .sql = "SELECT UPPER(i) FROM rt ORDER BY id", .expected = &.{ "2", "3", NULL } },
        .{ .sql = "SELECT CAST(d AS CHAR) FROM rt ORDER BY id", .expected = &.{ "2024-03-05", "2024-03-06", NULL } },
        .{ .sql = "SELECT CONCAT('a', 'b', 'c', 'd', 'e') FROM rt WHERE id = 1", .expected = &.{"abcde"} },
    });
}

test "result type: UNION arms meet at one type" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setup(allocator, db);

    try expectCasesBeforeAndAfterFlush(allocator, db, &.{
        .{ .sql = "SELECT CAST(v AS CHAR) AS t FROM (SELECT a AS v FROM rt UNION ALL SELECT b FROM rt) u ORDER BY t", .expected = &.{ "1.5000", "1.5000", "2.0000", "2.0000", "3.2400", "3.2500" } },
        .{ .sql = "SELECT CAST(v AS CHAR) AS t FROM (SELECT b AS v FROM rt UNION SELECT a FROM rt) u ORDER BY t", .expected = &.{ "1.5000", "2.0000", "3.2400", "3.2500" } },
        .{ .sql = "SELECT CAST(v AS CHAR) AS t FROM (SELECT a AS v FROM rt UNION ALL SELECT x FROM rt) u ORDER BY t", .expected = &.{ "1.5", "1.5", "2", "2.5", "3.25", "3.25" } },
        .{ .sql = "SELECT v FROM (SELECT id AS v FROM rt UNION ALL SELECT s FROM rt) u ORDER BY v", .expected = &.{ "1", "2", "3", "apple", "kiwi", "pear" } },
    });
}

test "result type: kinds that never meet are rejected" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try setup(allocator, db);

    try helpers.expectRunError(allocator, db, "SELECT CASE WHEN id = 1 THEN i ELSE d END FROM rt", error.ComputeUnsupportedExpr);
    try helpers.expectRunError(allocator, db, "SELECT i FROM rt UNION ALL SELECT d FROM rt", error.TypeMismatch);
    try helpers.expectRunError(allocator, db, "SELECT SQRT(s) FROM rt", error.ComputeNoSuchOverload);
}
