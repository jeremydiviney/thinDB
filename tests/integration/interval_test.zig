//! INTERVAL '<integer>' <unit> — calendar-aware date and datetime
//! arithmetic. Lowered at parse time to one kernel per unit (`date_add`,
//! `date_add_weeks`, ... `date_add_micros`). Month/year add clamps the day
//! on short destination months: `2024-01-31 + 1 month → 2024-02-29`. A DATE
//! moved by a sub-day unit becomes a DATETIME, as in MySQL. A result outside
//! years 0-9999 is NULL, as in StarRocks.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

fn collectDates(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]i32 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(i32) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |batch| {
        for (batch.values[0].data.date[0..batch.row_count]) |v| try out.append(allocator, v);
    }
    return out.toOwnedSlice(allocator);
}

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, d DATE NOT NULL)");
    try exec(
        allocator,
        db,
        "INSERT INTO t (id, d) VALUES (1, '2024-01-15'), (2, '2024-01-31'), (3, '2024-02-29')",
    );
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

test "INTERVAL: DAY add and subtract" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const plus = try collectDates(allocator, db, "SELECT d + INTERVAL '10' DAY AS r FROM t WHERE id = 1");
    defer allocator.free(plus);
    try std.testing.expectEqual(@as(usize, 1), plus.len);
    // 2024-01-15 + 10 days = 2024-01-25; daysToYmd-roundtrip checks below.

    const minus = try collectDates(allocator, db, "SELECT d - INTERVAL '5' DAY AS r FROM t WHERE id = 1");
    defer allocator.free(minus);
    try std.testing.expectEqual(plus[0] - 15, minus[0]); // plus - 15 = minus
}

test "INTERVAL: MONTH add with day-clamp on short month" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // 2024-01-31 + 1 month → 2024-02-29 (leap year: Feb has 29 days)
    const r = try collectDates(allocator, db, "SELECT d + INTERVAL '1' MONTH AS r FROM t WHERE id = 2");
    defer allocator.free(r);
    // expected days = ymdToDays(2024, 2, 29) — assert via reverse.
    var q = try runSql(allocator, db, "SELECT EXTRACT(YEAR FROM d + INTERVAL '1' MONTH) AS y, EXTRACT(MONTH FROM d + INTERVAL '1' MONTH) AS m, EXTRACT(DAY FROM d + INTERVAL '1' MONTH) AS dd FROM t WHERE id = 2");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(i32, 2024), batch.values[0].data.int[0]);
    try std.testing.expectEqual(@as(i32, 2), batch.values[1].data.int[0]);
    try std.testing.expectEqual(@as(i32, 29), batch.values[2].data.int[0]);
}

test "INTERVAL: YEAR add clamps Feb-29 in non-leap year" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // 2024-02-29 + 1 year → 2025-02-28
    var q = try runSql(
        allocator,
        db,
        "SELECT EXTRACT(YEAR FROM d + INTERVAL '1' YEAR) AS y, EXTRACT(MONTH FROM d + INTERVAL '1' YEAR) AS m, EXTRACT(DAY FROM d + INTERVAL '1' YEAR) AS dd FROM t WHERE id = 3",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(i32, 2025), batch.values[0].data.int[0]);
    try std.testing.expectEqual(@as(i32, 2), batch.values[1].data.int[0]);
    try std.testing.expectEqual(@as(i32, 28), batch.values[2].data.int[0]);
}

test "INTERVAL: bare integer accepted (PG-style)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // INTERVAL 7 DAY  vs  INTERVAL '7' DAY — both should work.
    var q = try runSql(
        allocator,
        db,
        "SELECT EXTRACT(DAY FROM d + INTERVAL 7 DAY) AS dd FROM t WHERE id = 1",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(i32, 22), batch.values[0].data.int[0]); // 15 + 7
}

test "INTERVAL: unknown unit rejected at parse time" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const err = thindb.sql.parse(arena.allocator(), "SELECT d + INTERVAL '1' FORTNIGHT FROM t");
    try std.testing.expectError(thindb.sql.ParseError.SqlExpectedKeyword, err);
}

test "INTERVAL: DATETIME keeps its time of day, and hours, minutes and seconds move either type" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE dt (id BIGINT PRIMARY KEY, ts DATETIME, d DATE)");
    try exec(allocator, db, "INSERT INTO dt VALUES (1, '2024-01-31 10:30:00', '2024-01-31'), (2, '2024-02-29 23:59:59', '2024-02-29')");

    const text_cases = .{
        .{ "ts + INTERVAL 1 DAY", .{ "2024-02-01 10:30:00", "2024-03-01 23:59:59" } },
        .{ "ts - INTERVAL 1 MONTH", .{ "2023-12-31 10:30:00", "2024-01-29 23:59:59" } },
        .{ "DATE_ADD(ts, INTERVAL 1 MONTH)", .{ "2024-02-29 10:30:00", "2024-03-29 23:59:59" } },
        .{ "ts + INTERVAL 1 YEAR", .{ "2025-01-31 10:30:00", "2025-02-28 23:59:59" } },
        .{ "DATE_SUB(ts, 1)", .{ "2024-01-30 10:30:00", "2024-02-28 23:59:59" } },
        .{ "ts + INTERVAL 2 HOUR", .{ "2024-01-31 12:30:00", "2024-03-01 01:59:59" } },
        .{ "ts - INTERVAL 90 MINUTE", .{ "2024-01-31 09:00:00", "2024-02-29 22:29:59" } },
        .{ "DATE_ADD(ts, INTERVAL 1 SECOND)", .{ "2024-01-31 10:30:01", "2024-03-01 00:00:00" } },
        .{ "d + INTERVAL 30 MINUTE", .{ "2024-01-31 00:30:00", "2024-02-29 00:30:00" } },
        .{ "d + INTERVAL 1 DAY", .{ "2024-02-01", "2024-03-01" } },
        .{ "TIMESTAMPADD(HOUR, 1, d)", .{ "2024-01-31 01:00:00", "2024-02-29 01:00:00" } },
        .{ "TIMESTAMPADD(MONTH, 1, d)", .{ "2024-02-29", "2024-03-29" } },
    };
    inline for (text_cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        const got = try helpers.collectStrings(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS CHAR) FROM dt ORDER BY id");
        defer helpers.freeStrings(allocator, got);
        try std.testing.expectEqual(@as(usize, 2), got.len);
        try std.testing.expectEqualStrings(c[1][0], got[0].?);
        try std.testing.expectEqualStrings(c[1][1], got[1].?);
    }

    const filter_cases = .{
        .{ "SELECT id FROM dt WHERE ts - INTERVAL 1 DAY < '2024-02-01 00:00:00' ORDER BY id", &[_]i64{1} },
        .{ "SELECT id FROM dt WHERE NOW() > ts - INTERVAL 1 DAY ORDER BY id", &[_]i64{ 1, 2 } },
        .{ "SELECT id FROM dt WHERE DATE_ADD(ts, INTERVAL 1 HOUR) >= '2024-03-01' ORDER BY id", &[_]i64{2} },
        .{ "SELECT id FROM dt WHERE ts + INTERVAL 1500 MICROSECOND > ts ORDER BY id", &[_]i64{ 1, 2 } },
    };
    inline for (filter_cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        const got = try helpers.collectBigints(allocator, db, c[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, c[1], got);
    }
}

test "INTERVAL: a result outside years 0-9999, or a count past INT, is NULL (issue #398)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE edge (id BIGINT PRIMARY KEY, d DATE NOT NULL, ts DATETIME NOT NULL, m INT, n BIGINT NOT NULL)");
    try exec(allocator, db, "INSERT INTO edge VALUES (1, '9999-12-31', '9999-12-31 23:59:59', NULL, 1), " ++
        "(2, '0000-01-01', '0000-01-01 00:00:00', 1, -1), (3, '2026-01-31', '2026-01-31 10:00:00', -31, 3000000000)");

    // Every expected value is StarRocks 4.0's, except that a DATE moved by
    // days or more stays a DATE. A count past INT is NULL even where the
    // move would land in range (row 3's 3000000000 microseconds).
    const cases = .{
        .{ "d + INTERVAL 1 DAY", .{ null, "0000-01-02", "2026-02-01" } },
        .{ "d - INTERVAL 1 DAY", .{ "9999-12-30", null, "2026-01-30" } },
        .{ "DATE_SUB(d, n)", .{ "9999-12-30", "0000-01-02", null } },
        .{ "ADDDATE(d, m)", .{ null, "0000-01-02", "2025-12-31" } },
        .{ "d + INTERVAL 1 WEEK", .{ null, "0000-01-08", "2026-02-07" } },
        .{ "d + INTERVAL 1 MONTH", .{ null, "0000-02-01", "2026-02-28" } },
        .{ "d - INTERVAL 1 QUARTER", .{ "9999-09-30", null, "2025-10-31" } },
        .{ "d + INTERVAL 1 YEAR", .{ null, "0001-01-01", "2027-01-31" } },
        .{ "d + INTERVAL 2147483647 YEAR", .{ null, null, null } },
        .{ "d - INTERVAL '-2147483648' DAY", .{ null, null, null } },
        .{ "d + INTERVAL 24 HOUR", .{ null, "0000-01-02 00:00:00", "2026-02-01 00:00:00" } },
        .{ "d + INTERVAL 9223372036854775807 HOUR", .{ null, null, null } },
        .{ "ts + INTERVAL 1 SECOND", .{ null, "0000-01-01 00:00:01", "2026-01-31 10:00:01" } },
        .{ "ts - INTERVAL n MICROSECOND", .{ "9999-12-31 23:59:58.999999", "0000-01-01 00:00:00.000001", null } },
        .{ "ts + INTERVAL '3000000000' MICROSECOND", .{ null, null, null } },
        .{ "ts + INTERVAL m HOUR", .{ null, "0000-01-01 01:00:00", "2026-01-30 03:00:00" } },
        .{ "ts + INTERVAL 1 DAY", .{ null, "0000-01-02 00:00:00", "2026-02-01 10:00:00" } },
        .{ "DATE_SUB(ts, 1)", .{ "9999-12-30 23:59:59", null, "2026-01-30 10:00:00" } },
        .{ "ts + INTERVAL 1 MONTH", .{ null, "0000-02-01 00:00:00", "2026-02-28 10:00:00" } },
        .{ "TIMESTAMPADD(YEAR, -1, ts)", .{ "9998-12-31 23:59:59", null, "2025-01-31 10:00:00" } },
    };
    inline for (cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        const got = try helpers.collectStrings(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS CHAR) FROM edge ORDER BY id");
        defer helpers.freeStrings(allocator, got);
        try std.testing.expectEqual(@as(usize, 3), got.len);
        inline for (c[1], 0..) |want, row| {
            if (@TypeOf(want) == @TypeOf(null)) try std.testing.expect(got[row] == null) else try std.testing.expectEqualStrings(want, got[row].?);
        }
    }
}

test "ADDDATE / SUBDATE are MySQL spellings of DATE_ADD / DATE_SUB" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT ADDDATE(d, INTERVAL 10 DAY) AS r FROM t WHERE id = 1", "SELECT DATE_ADD(d, INTERVAL 10 DAY) AS r FROM t WHERE id = 1" },
        .{ "SELECT ADDDATE(d, 10) AS r FROM t WHERE id = 1", "SELECT DATE_ADD(d, INTERVAL 10 DAY) AS r FROM t WHERE id = 1" },
        .{ "SELECT SUBDATE(d, INTERVAL 1 MONTH) AS r FROM t WHERE id = 2", "SELECT DATE_SUB(d, INTERVAL 1 MONTH) AS r FROM t WHERE id = 2" },
        .{ "SELECT ADDDATE(LAST_DAY(SUBDATE(d, INTERVAL 1 MONTH)), 1) AS r FROM t WHERE id = 1", "SELECT DATE_ADD(LAST_DAY(DATE_SUB(d, INTERVAL 1 MONTH)), 1) AS r FROM t WHERE id = 1" },
    };
    inline for (cases) |c| {
        const alias = try collectDates(allocator, db, c[0]);
        defer allocator.free(alias);
        const canonical = try collectDates(allocator, db, c[1]);
        defer allocator.free(canonical);
        try std.testing.expectEqualSlices(i32, canonical, alias);
    }
}

test "date-add spellings and unit-first calls parse as a predicate's left side" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT d FROM t WHERE ADDDATE(d, 1) >= '2024-02-01' ORDER BY d", "SELECT d FROM t WHERE d >= '2024-01-31' ORDER BY d" },
        .{ "SELECT d FROM t WHERE DATE_ADD(d, INTERVAL 1 DAY) >= '2024-02-01' ORDER BY d", "SELECT d FROM t WHERE d >= '2024-01-31' ORDER BY d" },
        .{ "SELECT d FROM t WHERE SUBDATE(d, 1) BETWEEN '2024-01-30' AND '2024-02-28' ORDER BY d", "SELECT d FROM t WHERE d >= '2024-01-31' ORDER BY d" },
        .{
            "SELECT d FROM t WHERE (ADDDATE(LAST_DAY(SUBDATE(d, INTERVAL 1 MONTH)), 1) >= '2024-02-01' AND ADDDATE(LAST_DAY(SUBDATE(d, INTERVAL 1 MONTH)), 1) < '2024-03-01') ORDER BY d",
            "SELECT d FROM t WHERE d >= '2024-02-01' ORDER BY d",
        },
        .{ "SELECT d FROM t WHERE TIMESTAMPDIFF(DAY, d, DATE '2024-03-01') < 10 ORDER BY d", "SELECT d FROM t WHERE d >= '2024-02-01' ORDER BY d" },
    };
    inline for (cases) |c| {
        const got = try collectDates(allocator, db, c[0]);
        defer allocator.free(got);
        const want = try collectDates(allocator, db, c[1]);
        defer allocator.free(want);
        try std.testing.expect(want.len > 0);
        try std.testing.expectEqualSlices(i32, want, got);
    }
}

test "INTERVAL: QUARTER is three months and WEEK is seven days" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT d + INTERVAL 1 QUARTER AS r FROM t ORDER BY id", "SELECT d + INTERVAL 3 MONTH AS r FROM t ORDER BY id" },
        .{ "SELECT d - INTERVAL '2' QUARTERS AS r FROM t ORDER BY id", "SELECT d - INTERVAL 6 MONTH AS r FROM t ORDER BY id" },
        .{ "SELECT DATE_SUB(d, INTERVAL 2 WEEK) AS r FROM t ORDER BY id", "SELECT DATE_SUB(d, INTERVAL 14 DAY) AS r FROM t ORDER BY id" },
        .{ "SELECT ADDDATE(d, INTERVAL 1 QUARTER) AS r FROM t ORDER BY id", "SELECT DATE_ADD(d, INTERVAL 3 MONTH) AS r FROM t ORDER BY id" },
        .{
            "SELECT MAKEDATE(YEAR(d), 1) + INTERVAL QUARTER(d) QUARTER - INTERVAL 1 QUARTER AS r FROM t ORDER BY id",
            "SELECT CAST(date_trunc('quarter', d) AS DATE) AS r FROM t ORDER BY id",
        },
    };
    inline for (cases) |c| {
        const got = try collectDates(allocator, db, c[0]);
        defer allocator.free(got);
        const want = try collectDates(allocator, db, c[1]);
        defer allocator.free(want);
        try std.testing.expectEqual(@as(usize, 3), want.len);
        try std.testing.expectEqualSlices(i32, want, got);
    }
}

test "date unit functions know WEEK and QUARTER and reject unknown units" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Expected values are DuckDB's, as days since 1970-01-01.
    const date_cases = .{
        .{ "SELECT CAST(date_trunc('week', d) AS DATE) AS r FROM t ORDER BY id", [_]i32{ 19737, 19751, 19779 } },
        .{ "SELECT CAST(date_trunc('QUARTER', d) AS DATE) AS r FROM t ORDER BY id", [_]i32{ 19723, 19723, 19723 } },
        .{ "SELECT TIMESTAMPADD(WEEK, 2, d) AS r FROM t ORDER BY id", [_]i32{ 19751, 19767, 19796 } },
        .{ "SELECT TIMESTAMPADD(QUARTER, 1, d) AS r FROM t ORDER BY id", [_]i32{ 19828, 19843, 19872 } },
    };
    inline for (date_cases) |c| {
        const got = try collectDates(allocator, db, c[0]);
        defer allocator.free(got);
        const want: [3]i32 = c[1];
        try std.testing.expectEqualSlices(i32, &want, got);
    }
    const int_cases = .{
        .{ "SELECT TIMESTAMPDIFF(WEEK, d, DATE '2024-06-30') AS r FROM t ORDER BY id", [_]i64{ 23, 21, 17 } },
        .{ "SELECT TIMESTAMPDIFF(QUARTER, d, DATE '2024-07-15') AS r FROM t ORDER BY id", [_]i64{ 2, 1, 1 } },
    };
    inline for (int_cases) |c| {
        const got = try helpers.collectBigints(allocator, db, c[0]);
        defer allocator.free(got);
        const want: [3]i64 = c[1];
        try std.testing.expectEqualSlices(i64, &want, got);
    }

    // The unit is read when the kernel runs, so the error surfaces on the
    // first batch.
    const unknown_units = .{
        "SELECT date_trunc('fortnight', d) AS r FROM t",
        "SELECT TIMESTAMPDIFF(FORTNIGHT, d, DATE '2024-06-30') AS r FROM t",
        "SELECT DATE_DIFF('fortnight', d, DATE '2024-06-30') AS r FROM t",
    };
    inline for (unknown_units) |sql| {
        var q = try runSql(allocator, db, sql);
        defer q.deinit();
        try std.testing.expectError(error.ComputeUnsupportedExpr, q.next());
    }
}

fn expectTexts(allocator: std.mem.Allocator, db: anytype, sql: []const u8, want: []const ?[]const u8) !void {
    const got = try helpers.collectStrings(allocator, db, sql);
    defer helpers.freeStrings(allocator, got);
    errdefer std.debug.print("sql: {s}\n", .{sql});
    try std.testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| {
        if (w) |text| try std.testing.expectEqualStrings(text, g orelse return error.TestUnexpectedResult) else try std.testing.expect(g == null);
    }
}

test "DATE_DIFF(unit, a, b) is a - b in whole units, and MONTHS_DIFF(a, b) too, as in StarRocks (issue #413)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    // Every value is StarRocks 4.0's. DATE_DIFF counts a month once the
    // later month's last day is reached, where MONTHS_DIFF and TIMESTAMPDIFF
    // wait for the earlier value's day.
    const cases = .{
        .{ "DATE_DIFF('day', DATE '2026-01-31', DATE '2026-01-01')", "30" },
        .{ "DATE_DIFF('day', DATE '2026-01-01', DATE '2026-01-31')", "-30" },
        .{ "DATE_DIFF('millisecond', DATE '2026-01-31', DATE '2026-01-01')", "2592000000" },
        .{ "DATE_DIFF('second', DATE '2026-03-01', DATE '2026-01-31')", "2505600" },
        .{ "DATE_DIFF('minute', DATE '2026-03-01', DATE '2026-01-31')", "41760" },
        .{ "DATE_DIFF('hour', DATE '2026-03-01', DATE '2026-01-31')", "696" },
        .{ "DATE_DIFF('week', DATE '2026-01-31', DATE '2026-01-01')", "4" },
        .{ "DATE_DIFF('week', DATE '2026-01-01', DATE '2026-01-31')", "-4" },
        .{ "DATE_DIFF('month', DATE '2026-01-31', DATE '2026-01-01')", "0" },
        .{ "DATE_DIFF('month', DATE '2026-01-01', DATE '2026-01-31')", "0" },
        .{ "DATE_DIFF('month', DATE '2026-03-01', DATE '2026-01-31')", "1" },
        .{ "DATE_DIFF('month', DATE '2026-03-28', DATE '2026-01-31')", "1" },
        .{ "DATE_DIFF('month', DATE '2024-02-29', DATE '2024-01-31')", "1" },
        .{ "DATE_DIFF('month', DATE '2024-01-31', DATE '2024-02-29')", "-1" },
        .{ "DATE_DIFF('month', DATE '2024-02-28', DATE '2024-01-31')", "0" },
        .{ "DATE_DIFF('month', DATE '2024-04-30', DATE '2024-03-31')", "1" },
        .{ "DATE_DIFF('quarter', DATE '2024-02-29', DATE '2023-11-30')", "1" },
        .{ "DATE_DIFF('quarter', DATE '2023-11-30', DATE '2024-02-29')", "-1" },
        .{ "DATE_DIFF('year', DATE '2023-02-28', DATE '2020-02-29')", "3" },
        .{ "DATE_DIFF('year', DATE '2024-02-29', DATE '2025-02-28')", "-1" },
        .{ "DATE_DIFF('year', DATE '2026-01-01', DATE '2026-06-01')", "0" },
        .{ "DATE_DIFF('day', DATETIME '2026-01-02 01:00:00', DATETIME '2026-01-01 23:00:00')", "0" },
        .{ "DATE_DIFF('hour', DATETIME '2026-01-02 01:00:00', DATETIME '2026-01-01 23:00:00')", "2" },
        .{ "DATE_DIFF('minute', DATETIME '2026-01-02 01:00:00', DATETIME '2026-01-01 23:00:00')", "120" },
        .{ "DATE_DIFF('millisecond', DATETIME '2026-01-02 01:00:00', DATETIME '2026-01-01 23:00:00')", "7200000" },
        .{ "DATE_DIFF('hour', DATETIME '2026-01-01 23:00:00', DATETIME '2026-01-02 01:00:00')", "-2" },
        .{ "DATE_DIFF('day', DATETIME '2026-03-31 10:00:00', DATETIME '2026-02-28 11:00:00')", "30" },
        .{ "DATE_DIFF('day', DATETIME '2026-02-28 11:00:00', DATETIME '2026-03-31 10:00:00')", "-30" },
        .{ "DATE_DIFF('hour', DATETIME '2026-03-31 10:00:00', DATETIME '2026-02-28 11:00:00')", "743" },
        .{ "DATE_DIFF('month', DATETIME '2026-03-31 10:00:00', DATETIME '2026-02-28 11:00:00')", "1" },
        .{ "DATE_DIFF('month', DATETIME '2026-02-28 11:00:00', DATETIME '2026-03-31 10:00:00')", "-1" },
        .{ "DATE_DIFF('day', DATETIME '2027-01-01 00:00:00', DATETIME '2026-01-01 00:00:01')", "364" },
        .{ "DATE_DIFF('month', DATETIME '2027-01-01 00:00:00', DATETIME '2026-01-01 00:00:01')", "11" },
        .{ "DATE_DIFF('quarter', DATETIME '2027-01-01 00:00:00', DATETIME '2026-01-01 00:00:01')", "3" },
        .{ "DATE_DIFF('year', DATETIME '2027-01-01 00:00:00', DATETIME '2026-01-01 00:00:01')", "0" },
        .{ "DATE_DIFF('year', DATETIME '2025-02-28 00:00:00', DATETIME '2024-02-29 00:00:00')", "1" },
        .{ "DATE_DIFF('week', DATETIME '2026-01-08 00:00:00', DATETIME '2026-01-01 00:00:01')", "0" },
        .{ "DATE_DIFF('minute', DATETIME '2026-01-08 00:00:00', DATETIME '2026-01-01 00:00:01')", "10079" },
        .{ "DATE_DIFF('month', DATETIME '2024-02-29 10:00:00', DATETIME '2024-01-31 11:00:00')", "0" },
        .{ "DATE_DIFF('month', DATETIME '2024-02-29 12:00:00', DATETIME '2024-01-31 11:00:00')", "1" },
        .{ "DATE_DIFF('month', DATETIME '2024-01-31 11:00:00', DATETIME '2024-02-29 12:00:00')", "-1" },
        .{ "DATE_DIFF('month', DATETIME '2026-02-01 00:00:00', DATETIME '2026-01-01 00:00:00.5')", "0" },
        .{ "DATE_DIFF('millisecond', DATETIME '2026-01-01 00:00:01.2', DATETIME '2026-01-01 00:00:00.8')", "400" },
        .{ "DATE_DIFF('second', DATETIME '2026-01-01 00:00:01.2', DATETIME '2026-01-01 00:00:00.8')", "0" },
        .{ "DATE_DIFF('second', DATETIME '2026-01-01 00:00:00', DATETIME '2026-01-01 00:00:01.5')", "-1" },
        .{ "DATE_DIFF('millisecond', DATETIME '2026-01-01 00:00:00', DATETIME '2026-01-01 00:00:00.0015')", "-1" },
        .{ "DATE_DIFF('hour', DATETIME '2026-01-02 00:00:00', DATETIME '2026-01-01 00:00:00.5')", "23" },
        .{ "DATE_DIFF('day', DATETIME '2026-01-02 00:00:00', DATETIME '2026-01-01 00:00:00.5')", "0" },
        .{ "DATE_DIFF('second', DATETIME '1969-12-31 23:59:59.5', DATETIME '1970-01-01 00:00:00.25')", "0" },
        .{ "DATE_DIFF('month', DATETIME '1969-03-31 12:00:00', DATETIME '1969-02-28 12:00:00.000001')", "1" },
        .{ "DATE_DIFF('second', DATETIME '9999-12-31 23:59:59', DATETIME '0000-01-01 00:00:00')", "315569519999" },
        .{ "DATE_DIFF('month', DATETIME '9999-12-31 23:59:59', DATETIME '0000-01-01 00:00:00')", "119999" },
        .{ "DATE_DIFF('year', DATETIME '0000-01-01 00:00:00', DATETIME '9999-12-31 23:59:59')", "-9999" },
        .{ "DATE_DIFF('second', DATE '2026-01-01', DATETIME '2025-12-31 23:59:59')", "1" },
        .{ "DATE_DIFF('day', DATE '2026-01-01', DATETIME '2025-12-31 23:59:59')", "0" },
        .{ "DATE_DIFF('month', DATETIME '2026-03-15 08:00:00', DATE '2026-01-15')", "2" },
        .{ "DATE_DIFF('month', DATE '2026-01-15', DATETIME '2026-03-15 08:00:00')", "-2" },
        .{ "DATE_DIFF('day', '2026-01-02 01:00:00', '2026-01-01 23:00:00')", "0" },
        .{ "DATE_DIFF('hour', '2026-01-02 01:00:00', '2026-01-01 23:00:00')", "2" },
        .{ "DATE_DIFF('millisecond', '2026-01-02 01:00:00', '2026-01-01 23:00:00')", "7200000" },
        .{ "DATE_DIFF('day', '2026-01-31', '2026-01-01')", "30" },
        .{ "DATE_DIFF('day', '2026/01/31', '20260101')", "30" },
        .{ "DATE_DIFF('day', 'abc', '2026-01-01')", null },
        .{ "DATE_DIFF('DAY', DATE '2026-01-31', DATE '2026-01-01')", "30" },
        .{ "DATE_DIFF('Hour', DATE '2026-01-31', DATE '2026-01-01')", "720" },
        .{ "DATE_DIFF('day', DATE '2026-01-31', NULL)", null },
        .{ "DATE_DIFF(NULL, DATE '2026-01-31', DATE '2026-01-01')", null },
        .{ "DATE_DIFF(NULL, NULL, NULL)", null },
        .{ "MONTHS_DIFF(DATE '2024-02-29', DATE '2024-01-31')", "0" },
        .{ "MONTHS_DIFF(DATE '2024-02-29', DATE '2023-11-30')", "2" },
        .{ "MONTHS_DIFF(DATE '2023-02-28', DATE '2020-02-29')", "35" },
        .{ "MONTHS_DIFF(DATETIME '2026-02-15 10:00:00', DATETIME '2026-01-15 11:00:00')", "0" },
        .{ "MONTHS_DIFF(DATETIME '2026-01-15 11:00:00', DATETIME '2026-02-15 10:00:00')", "0" },
        .{ "MONTHS_DIFF(DATETIME '2026-04-15 00:00:00', DATE '2026-01-15')", "3" },
        .{ "MONTHS_DIFF(DATE '2026-01-20', DATE '2026-04-15')", "-2" },
        .{ "MONTHS_DIFF('2026-04-15', '2026-01-20')", "2" },
        // StarRocks gives -1 for an earlier value in the same month, where
        // MySQL's TIMESTAMPDIFF(MONTH, b, a) gives 0.
        .{ "MONTHS_DIFF(DATE '2026-01-01', DATE '2026-01-31')", "-1" },
        .{ "MONTHS_DIFF(DATE '2026-01-31', DATE '2026-01-01')", "0" },
        .{ "MONTHS_DIFF(DATE '2026-01-01', NULL)", null },
        .{ "TIMESTAMPDIFF(MILLISECOND, DATETIME '2026-01-01 00:00:00', DATETIME '2026-01-01 00:00:01.5')", "1500" },
    };
    inline for (cases) |c| try expectTexts(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS CHAR)", &.{c[1]});

    // A unit may differ by row, and a NULL unit makes its row NULL. A bare
    // name first is a unit word, as in `DATE_DIFF(day, a, b)`, so the column
    // goes through LOWER.
    try exec(allocator, db, "CREATE TABLE dd (id BIGINT PRIMARY KEY, u VARCHAR(20), a DATETIME, b DATE)");
    try exec(allocator, db,
        \\INSERT INTO dd VALUES (1, 'day', '2026-01-02 01:00:00', '2026-01-01'), (2, 'month', '2024-02-29 00:00:00', '2024-01-31'),
        \\  (3, NULL, '2026-01-02 00:00:00', '2026-01-01'), (4, 'week', '2026-01-01 00:00:00', '2026-01-31'),
        \\  (5, 'millisecond', '2026-01-01 00:00:00.5', '2026-01-01'), (6, 'month', NULL, '2026-01-01')
    );
    try expectTexts(allocator, db, "SELECT CAST(DATE_DIFF(LOWER(u), a, b) AS CHAR) FROM dd ORDER BY id", &.{ "1", "1", null, "-4", "500", null });
    try expectTexts(allocator, db, "SELECT CAST(MONTHS_DIFF(a, b) AS CHAR) FROM dd ORDER BY id", &.{ "0", "0", "0", "-1", "0", null });
}

test "calendar functions work before 1970" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE h (id BIGINT PRIMARY KEY, d DATE NOT NULL, ts DATETIME NOT NULL)");
    try exec(allocator, db, "INSERT INTO h (id, d, ts) VALUES (1, '1965-03-05', '1965-03-05 10:07:09.25'), (2, '1964-02-10', '1900-02-10 23:59:58'), (3, '0001-01-01', '1969-12-31 23:00:01')");

    const cases = .{
        .{ "YEAR(d)", [_]i64{ 1965, 1964, 1 } },
        .{ "MONTH(d)", [_]i64{ 3, 2, 1 } },
        .{ "DAY(d)", [_]i64{ 5, 10, 1 } },
        .{ "QUARTER(d)", [_]i64{ 1, 1, 1 } },
        .{ "DAYOFYEAR(d)", [_]i64{ 64, 41, 1 } },
        .{ "DAYOFWEEK(d)", [_]i64{ 6, 2, 2 } },
        .{ "YEAR(ts)", [_]i64{ 1965, 1900, 1969 } },
        .{ "HOUR(ts)", [_]i64{ 10, 23, 23 } },
        .{ "MINUTE(ts)", [_]i64{ 7, 59, 0 } },
        .{ "SECOND(ts)", [_]i64{ 9, 58, 1 } },
        .{ "DATEDIFF(LAST_DAY(d), DATE '1964-01-01')", [_]i64{ 455, 59, -716940 } },
        .{ "DATEDIFF(LAST_DAY(ts), DATE '1900-01-01')", [_]i64{ 23830, 58, 25566 } },
        .{ "DATEDIFF(d + INTERVAL 1 MONTH, d)", [_]i64{ 31, 29, 31 } },
        .{ "TIMESTAMPDIFF(MONTH, d, DATE '1965-03-05')", [_]i64{ 0, 12, 23570 } },
        .{ "DATEDIFF(DATE_TRUNC('month', ts), DATE '1900-01-01')", [_]i64{ 23800, 31, 25536 } },
        .{ "DATEDIFF(DATE_TRUNC('quarter', ts), DATE '1900-01-01')", [_]i64{ 23741, 0, 25475 } },
        .{ "CASE WHEN DATE_FORMAT(ts, '%Y/%m/%d %H:%i:%s') IN ('1965/03/05 10:07:09', '1900/02/10 23:59:58', '1969/12/31 23:00:01') THEN 1 ELSE 0 END", [_]i64{ 1, 1, 1 } },
        .{ "CASE WHEN MONTHNAME(d) IN ('March', 'February', 'January') THEN 1 ELSE 0 END", [_]i64{ 1, 1, 1 } },
    };
    inline for (cases) |c| {
        const got = helpers.collectBigints(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS BIGINT) FROM h ORDER BY id") catch |err| {
            std.debug.print("expr: {s}\n", .{c[0]});
            return err;
        };
        defer allocator.free(got);
        const want: [3]i64 = c[1];
        std.testing.expectEqualSlices(i64, &want, got) catch |err| {
            std.debug.print("expr: {s}\n", .{c[0]});
            return err;
        };
    }

    const day_match = try helpers.collectBigints(allocator, db, "SELECT id FROM h WHERE DAY(d) = 10");
    defer allocator.free(day_match);
    try std.testing.expectEqualSlices(i64, &.{2}, day_match);
}

test "UNIX_TIMESTAMP before 1970 is 0, and FROM_UNIXTIME of a negative count is NULL (issue #400)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE ep (id BIGINT PRIMARY KEY, ts DATETIME NOT NULL, d DATE NOT NULL, n BIGINT)");
    try exec(allocator, db, "INSERT INTO ep VALUES (1, '1969-12-31 23:59:59', '1969-12-31', -1), " ++
        "(2, '1970-01-01 00:00:01', '1970-01-02', 0), (3, '9999-12-31 23:59:59', '9999-12-31', 253402300799), " ++
        "(4, '0000-01-01 00:00:00', '0000-01-01', 253402300800), (5, '2026-01-01 00:00:00.5', '2026-01-01', NULL)");

    // StarRocks 4.0 and MySQL 8.4 give 0 before 1970 and NULL for a negative
    // count. Both stop earlier at the top: StarRocks after 9999-12-31
    // 07:59:59, MySQL after 3001-01-18 23:59:59. thinDB stops where DATETIME
    // does.
    const seconds = try helpers.collectBigints(allocator, db, "SELECT UNIX_TIMESTAMP(ts) FROM ep ORDER BY id");
    defer allocator.free(seconds);
    try std.testing.expectEqualSlices(i64, &.{ 0, 1, 253402300799, 0, 1767225600 }, seconds);
    const day_seconds = try helpers.collectBigints(allocator, db, "SELECT UNIX_TIMESTAMP(d) FROM ep ORDER BY id");
    defer allocator.free(day_seconds);
    try std.testing.expectEqualSlices(i64, &.{ 0, 86400, 253402214400, 0, 1767225600 }, day_seconds);

    const moments = try helpers.collectStrings(allocator, db, "SELECT CAST(FROM_UNIXTIME(n) AS CHAR) FROM ep ORDER BY id");
    defer helpers.freeStrings(allocator, moments);
    const want = [_]?[]const u8{ null, "1970-01-01 00:00:00", "9999-12-31 23:59:59", null, null };
    try std.testing.expectEqual(want.len, moments.len);
    for (want, moments) |w, m| {
        if (w) |text| try std.testing.expectEqualStrings(text, m.?) else try std.testing.expect(m == null);
    }
    inline for (.{ "9223372036854775807", "-9223372036854775808" }) |extreme| {
        const got = try helpers.collectStrings(allocator, db, "SELECT CAST(FROM_UNIXTIME(" ++ extreme ++ ") AS CHAR)");
        defer helpers.freeStrings(allocator, got);
        try std.testing.expect(got.len == 1 and got[0] == null);
    }
}

test "FROM_UNIXTIME(n, format) renders as DATE_FORMAT does and reads its count as CAST does, as in StarRocks (issues #408, #409)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    // StarRocks 4.0's values, from constant-only SELECTs with the backend
    // forced (`IF(RAND() < 2, x, NULL)`), except the last two.
    const cases = [_]struct { []const u8, ?[]const u8 }{
        .{ "FROM_UNIXTIME(0, '%Y-%m-%d')", "1970-01-01" },
        .{ "FROM_UNIXTIME(1767263400, '%Y-%m-%d %H:%i:%s')", "2026-01-01 10:30:00" },
        .{ "FROM_UNIXTIME(1767263400, '%W %M %D %y %j %a %b %c %e %f %p %r %T %U %u %V %v %X %x %%')", "Thursday January 1st 26 001 Thu Jan 1 1 000000 AM 10:30:00 AM 10:30:00 00 01 52 01 2025 2026 %" },
        .{ "FROM_UNIXTIME(1767263405, '%Y-%m-%d %T.%f')", "2026-01-01 10:30:05.000000" },
        .{ "FROM_UNIXTIME(-1, '%Y')", null },
        .{ "FROM_UNIXTIME(0, NULL)", null },
        .{ "FROM_UNIXTIME(NULL, '%Y')", null },
        .{ "FROM_UNIXTIME(0, '')", null },
        .{ "FROM_UNIXTIME(1.5, '')", null },
        .{ "FROM_UNIXTIME(0, 'abc')", "abc" },
        .{ "FROM_UNIXTIME(0, '%Q')", "Q" },
        .{ "FROM_UNIXTIME(1767263400, ' ')", " " },
        .{ "FROM_UNIXTIME(1767263400, '%')", "%" },
        .{ "FROM_UNIXTIME(1767263400, '%Y%')", "2026%" },
        .{ "FROM_UNIXTIME(0, 'yyyy-MM-dd')", "1970-01-01" },
        .{ "FROM_UNIXTIME(0, 'yyyy-MM-dd HH:mm:ss')", "1970-01-01 00:00:00" },
        .{ "FROM_UNIXTIME(0, 'yyyyMMdd')", "19700101" },
        .{ "FROM_UNIXTIME(1767263400, 'yyyy/MM/dd')", "yyyy/MM/dd" },
        .{ "FROM_UNIXTIME(1767263400, 'yyyy-MM-dd HH:mm')", "yyyy-MM-dd HH:mm" },
        .{ "FROM_UNIXTIME(1767263400, 'HH:mm:ss')", "HH:mm:ss" },
        .{ "FROM_UNIXTIME(1.5, '%Y-%m-%d %H:%i:%s')", "1970-01-01 00:00:01" },
        .{ "FROM_UNIXTIME(1767263400.999999, '%f')", "000000" },
        .{ "FROM_UNIXTIME(12, 34)", "34" },
        .{ "FROM_UNIXTIME(12, 5.5)", "5.5" },
        .{ "FROM_UNIXTIME(CAST(12 AS TINYINT), '%s')", "12" },
        .{ "CAST(FROM_UNIXTIME(1.5) AS CHAR)", "1970-01-01 00:00:01" },
        .{ "CAST(FROM_UNIXTIME(1.9) AS CHAR)", "1970-01-01 00:00:01" },
        .{ "CAST(FROM_UNIXTIME(-0.5) AS CHAR)", "1970-01-01 00:00:00" },
        .{ "CAST(FROM_UNIXTIME(-1.5) AS CHAR)", null },
        .{ "CAST(FROM_UNIXTIME(1.5e0) AS CHAR)", "1970-01-01 00:00:01" },
        .{ "CAST(FROM_UNIXTIME(CAST(1.5 AS DOUBLE)) AS CHAR)", "1970-01-01 00:00:01" },
        .{ "CAST(FROM_UNIXTIME(CAST(1.9 AS FLOAT)) AS CHAR)", "1970-01-01 00:00:01" },
        .{ "CAST(FROM_UNIXTIME(1767263400.999) AS CHAR)", "2026-01-01 10:30:00" },
        .{ "CAST(FROM_UNIXTIME(1767263400.9999999999) AS CHAR)", "2026-01-01 10:30:00" },
        .{ "CAST(FROM_UNIXTIME(1e20) AS CHAR)", null },
        .{ "CAST(FROM_UNIXTIME(9223372036854775807.5) AS CHAR)", null },
        .{ "CAST(FROM_UNIXTIME(TRUE) AS CHAR)", "1970-01-01 00:00:01" },
        .{ "CAST(FROM_UNIXTIME('12') AS CHAR)", "1970-01-01 00:00:12" },
        .{ "CAST(FROM_UNIXTIME(' 12') AS CHAR)", "1970-01-01 00:00:12" },
        .{ "CAST(FROM_UNIXTIME('+12') AS CHAR)", "1970-01-01 00:00:12" },
        .{ "CAST(FROM_UNIXTIME('-0') AS CHAR)", "1970-01-01 00:00:00" },
        .{ "CAST(FROM_UNIXTIME('1.5') AS CHAR)", null },
        .{ "CAST(FROM_UNIXTIME('1e3') AS CHAR)", null },
        .{ "CAST(FROM_UNIXTIME('12abc') AS CHAR)", null },
        .{ "CAST(FROM_UNIXTIME('abc') AS CHAR)", null },
        .{ "CAST(FROM_UNIXTIME('') AS CHAR)", null },
        .{ "DATE_FORMAT(DATETIME '2026-01-01 10:30:00', '')", null },
        .{ "DATE_FORMAT(DATE '2026-01-01', '')", null },
        .{ "DATE_FORMAT(DATETIME '2026-01-01 10:30:00', 'yyyy-MM-dd')", "2026-01-01" },
        .{ "DATE_FORMAT(DATETIME '2026-01-01 10:30:05.123456', 'yyyy-MM-dd HH:mm:ss')", "2026-01-01 10:30:05" },
        .{ "DATE_FORMAT(DATETIME '0001-02-03 04:05:06', 'yyyy-MM-dd HH:mm:ss')", "0001-02-03 04:05:06" },
        .{ "DATE_FORMAT(DATETIME '2026-01-01 10:30:00', 'yyyyMMdd')", "20260101" },
        .{ "DATE_FORMAT(DATE '2026-01-01', 'yyyy-MM-dd HH:mm:ss')", "2026-01-01 00:00:00" },
        .{ "DATE_FORMAT('1970-01-01', 'yyyy-MM-dd')", "1970-01-01" },
        .{ "DATE_FORMAT(DATETIME '2026-01-01 10:30:05', 'yyyy-MM-dd HH:mm:ss.SSS')", "yyyy-MM-dd HH:mm:ss.SSS" },
        .{ "DATE_FORMAT(DATETIME '2026-01-01 10:30:05', 'yyyyMMddHHmmss')", "yyyyMMddHHmmss" },
        .{ "DATE_FORMAT(DATETIME '2026-01-01 10:30:05', ' yyyy-MM-dd')", " yyyy-MM-dd" },
        .{ "DATE_FORMAT(DATETIME '2026-01-01 10:30:05', 'YYYY-MM-dd')", "YYYY-MM-dd" },
        .{ "DATE_FORMAT(DATETIME '2026-01-01 10:30:05', 'yyyy-mm-dd')", "yyyy-mm-dd" },
        // thinDB's DATETIME range ends at 9999-12-31 23:59:59. StarRocks gives
        // NULL after 9999-12-31 07:59:59, a time-zone guard band.
        .{ "FROM_UNIXTIME(253402300799, '%Y-%m-%d %H:%i:%s')", "9999-12-31 23:59:59" },
        .{ "CAST(FROM_UNIXTIME(253402300799.5) AS CHAR)", "9999-12-31 23:59:59" },
    };
    for (cases) |c| {
        const sql = try std.fmt.allocPrint(allocator, "SELECT {s}", .{c[0]});
        defer allocator.free(sql);
        errdefer std.debug.print("failed: {s}\n", .{sql});
        const got = try helpers.collectStrings(allocator, db, sql);
        defer helpers.freeStrings(allocator, got);
        try std.testing.expectEqual(@as(usize, 1), got.len);
        if (c[1]) |want| {
            try std.testing.expect(got[0] != null);
            try std.testing.expectEqualStrings(want, got[0].?);
        } else try std.testing.expect(got[0] == null);
    }

    // The same readings over columns, a format per row.
    try exec(allocator, db, "CREATE TABLE fu (id BIGINT PRIMARY KEY, x DOUBLE, d DECIMAL(20, 10), s VARCHAR(20), f VARCHAR(30))");
    try exec(allocator, db, "INSERT INTO fu VALUES (1, 1.5, 1767263400.9999999999, '12', '%Y-%m-%d %H:%i:%s'), " ++
        "(2, -0.5, -0.5, ' 12', ''), (3, -1.5, -1.5, '1.5', NULL), (4, 1e20, 0.9, 'abc', 'yyyyMMdd'), (5, NULL, NULL, NULL, 'yyyy/MM/dd')");
    const column_cases = [_]struct { []const u8, [5]?[]const u8 }{
        .{ "CAST(FROM_UNIXTIME(x) AS CHAR)", .{ "1970-01-01 00:00:01", "1970-01-01 00:00:00", null, null, null } },
        .{ "CAST(FROM_UNIXTIME(d) AS CHAR)", .{ "2026-01-01 10:30:00", "1970-01-01 00:00:00", null, "1970-01-01 00:00:00", null } },
        .{ "CAST(FROM_UNIXTIME(s) AS CHAR)", .{ "1970-01-01 00:00:12", "1970-01-01 00:00:12", null, null, null } },
        .{ "FROM_UNIXTIME(1767263405, f)", .{ "2026-01-01 10:30:05", null, null, "20260101", "yyyy/MM/dd" } },
        .{ "FROM_UNIXTIME(x, f)", .{ "1970-01-01 00:00:01", null, null, null, null } },
        .{ "DATE_FORMAT(DATETIME '2026-01-01 10:30:05', f)", .{ "2026-01-01 10:30:05", null, null, "20260101", "yyyy/MM/dd" } },
        .{ "DATE_FORMAT(DATE '2026-01-01', f)", .{ "2026-01-01 00:00:00", null, null, "20260101", "yyyy/MM/dd" } },
    };
    for (column_cases) |c| {
        const sql = try std.fmt.allocPrint(allocator, "SELECT {s} FROM fu ORDER BY id", .{c[0]});
        defer allocator.free(sql);
        errdefer std.debug.print("failed: {s}\n", .{sql});
        const got = try helpers.collectStrings(allocator, db, sql);
        defer helpers.freeStrings(allocator, got);
        try std.testing.expectEqual(c[1].len, got.len);
        for (c[1], got) |want, g| {
            if (want) |text| {
                try std.testing.expect(g != null);
                try std.testing.expectEqualStrings(text, g.?);
            } else try std.testing.expect(g == null);
        }
    }
}

test "DATE_FORMAT, STR_TO_DATE and the week functions match MySQL" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE w (id BIGINT PRIMARY KEY, ts DATETIME NOT NULL)");
    try exec(allocator, db, "INSERT INTO w (id, ts) VALUES (1, '2024-03-05 14:07:09.25'), (2, '1965-12-31 00:30:00'), (3, '2021-01-03 12:00:59.000001')");

    // Every value below is MySQL 8.4's.
    const formatted = try helpers.collectStrings(allocator, db, "SELECT DATE_FORMAT(ts, '%M %e, %Y %r %D %j %a %W %b %c %f %h %I %l %k %p %T %U %u %V %v %w %X %x %y %%') FROM w ORDER BY id");
    defer helpers.freeStrings(allocator, formatted);
    const want_formatted = [_][]const u8{
        "March 5, 2024 02:07:09 PM 5th 065 Tue Tuesday Mar 3 250000 02 02 2 14 PM 14:07:09 09 10 09 10 2 2024 2024 24 %",
        "December 31, 1965 12:30:00 AM 31st 365 Fri Friday Dec 12 000000 12 12 12 0 AM 00:30:00 52 52 52 52 5 1965 1965 65 %",
        "January 3, 2021 12:00:59 PM 3rd 003 Sun Sunday Jan 1 000001 12 12 12 12 PM 12:00:59 01 00 01 53 0 2021 2020 21 %",
    };
    try std.testing.expectEqual(want_formatted.len, formatted.len);
    for (want_formatted, formatted) |want, got| try std.testing.expectEqualStrings(want, got.?);

    const int_cases = .{
        .{ "WEEK(ts)", [_]i64{ 9, 52, 1 } },
        .{ "WEEK(ts, 3)", [_]i64{ 10, 52, 53 } },
        .{ "YEARWEEK(ts)", [_]i64{ 202409, 196552, 202101 } },
        .{ "YEARWEEK(ts, 1)", [_]i64{ 202410, 196552, 202053 } },
        .{ "WEEKOFYEAR(ts)", [_]i64{ 10, 52, 53 } },
        .{ "WEEKDAY(ts)", [_]i64{ 1, 4, 6 } },
        .{ "MICROSECOND(ts)", [_]i64{ 250000, 0, 1 } },
        .{ "EXTRACT(QUARTER FROM ts)", [_]i64{ 1, 4, 1 } },
        .{ "EXTRACT(WEEK FROM ts)", [_]i64{ 9, 52, 1 } },
        .{ "EXTRACT(DAYOFYEAR FROM ts)", [_]i64{ 65, 365, 3 } },
        .{ "EXTRACT(MICROSECOND FROM ts)", [_]i64{ 250000, 0, 1 } },
    };
    inline for (int_cases) |c| {
        const got = try helpers.collectBigints(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS BIGINT) FROM w ORDER BY id");
        defer allocator.free(got);
        const want: [3]i64 = c[1];
        std.testing.expectEqualSlices(i64, &want, got) catch |err| {
            std.debug.print("expr: {s}\n", .{c[0]});
            return err;
        };
    }

    // A format that names no time of day yields a DATE; one that does, a
    // DATETIME. Text that does not match, or names no real date, is NULL.
    const parse_cases = .{
        .{ "STR_TO_DATE('March 5, 2024', '%M %e, %Y')", "2024-03-05" },
        .{ "STR_TO_DATE('Tue 5th Mar 2024', '%a %D %b %Y')", "2024-03-05" },
        .{ "STR_TO_DATE(' 2024- 3-5', '%Y-%m-%d')", "2024-03-05" },
        .{ "STR_TO_DATE('2024-03-05trailing', '%Y-%m-%d')", "2024-03-05" },
        .{ "STR_TO_DATE('200442 Monday', '%X%V %W')", "2004-10-18" },
        .{ "STR_TO_DATE('2024 065', '%Y %j')", "2024-03-05" },
        .{ "STR_TO_DATE('Sept 5 2024', '%M %e %Y')", "2024-09-05" },
        .{ "STR_TO_DATE('05/03/24 2:07:09 PM', '%d/%m/%y %r')", "2024-03-05 14:07:09" },
        .{ "STR_TO_DATE('12:30 AM 2024-01-02', '%h:%i %p %Y-%m-%d')", "2024-01-02 00:30:00" },
        .{ "STR_TO_DATE('2024-3-5 7', '%Y-%m-%d %H')", "2024-03-05 07:00:00" },
        .{ "DATE_FORMAT(STR_TO_DATE('2024-03-05 14:07:09.25', '%Y-%m-%d %H:%i:%s.%f'), '%Y-%m-%d %T.%f')", "2024-03-05 14:07:09.250000" },
        .{ "STR_TO_DATE('2024-02-30', '%Y-%m-%d')", null },
        .{ "STR_TO_DATE('abc', '%Y')", null },
        .{ "STR_TO_DATE('Ma 5 2024', '%M %e %Y')", null },
        .{ "STR_TO_DATE('13:00 PM 2024-01-02', '%h:%i %p %Y-%m-%d')", null },
    };
    inline for (parse_cases) |c| {
        const got = try helpers.collectStrings(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS CHAR) FROM w WHERE id = 1");
        defer helpers.freeStrings(allocator, got);
        try std.testing.expectEqual(@as(usize, 1), got.len);
        const want: ?[]const u8 = c[1];
        (if (want) |w| std.testing.expectEqualStrings(w, got[0] orelse "NULL") else std.testing.expectEqual(@as(?[]u8, null), got[0])) catch |err| {
            std.debug.print("expr: {s}\n", .{c[0]});
            return err;
        };
    }

    const uuids = try helpers.collectStrings(allocator, db, "SELECT UUID() FROM w ORDER BY id");
    defer helpers.freeStrings(allocator, uuids);
    try std.testing.expectEqual(@as(usize, 3), uuids.len);
    for (uuids, 0..) |u, i| {
        const text = u.?;
        try std.testing.expectEqual(@as(usize, 36), text.len);
        try std.testing.expectEqual(@as(u8, '4'), text[14]);
        for (uuids[0..i]) |earlier| try std.testing.expect(!std.mem.eql(u8, earlier.?, text));
    }
}

test "INTERVAL: a fractional amount rounds to whole units before the unit's factor" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // MySQL 8.4: half away from zero, and a text amount reads its leading
    // integer.
    const cases = .{
        .{ "d + INTERVAL 1.5 DAY", "d + INTERVAL 2 DAY" },
        .{ "d + INTERVAL 1.4 DAY", "d + INTERVAL 1 DAY" },
        .{ "d + INTERVAL -1.5 DAY", "d + INTERVAL -2 DAY" },
        .{ "d - INTERVAL 2.5 DAY", "d - INTERVAL 3 DAY" },
        .{ "d + INTERVAL 1.5 WEEK", "d + INTERVAL 2 WEEK" },
        .{ "d + INTERVAL 1.5 QUARTER", "d + INTERVAL 2 QUARTER" },
        .{ "DATE_ADD(d, INTERVAL 1.49 MONTH)", "DATE_ADD(d, INTERVAL 1 MONTH)" },
        .{ "DATE_ADD(d, INTERVAL 1.5e0 DAY)", "DATE_ADD(d, INTERVAL 2 DAY)" },
        .{ "DATE_SUB(d, INTERVAL 2.5 DAY)", "DATE_SUB(d, INTERVAL 3 DAY)" },
        .{ "d + INTERVAL '1.5' DAY", "d + INTERVAL 1 DAY" },
    };
    inline for (cases) |c| {
        const got = try collectDates(allocator, db, "SELECT " ++ c[0] ++ " FROM t ORDER BY id");
        defer allocator.free(got);
        const want = try collectDates(allocator, db, "SELECT " ++ c[1] ++ " FROM t ORDER BY id");
        defer allocator.free(want);
        errdefer std.debug.print("case: {s}\n", .{c[0]});
        try std.testing.expectEqualSlices(i32, want, got);
    }
}

test "INTERVAL: an interval may lead a sum, but not a difference (issue #322)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE dt (id BIGINT PRIMARY KEY, ts DATETIME, d DATE)");
    try exec(allocator, db, "INSERT INTO dt VALUES (1, '2024-01-31 10:30:00', '2024-01-31'), (2, '2024-02-29 23:59:59', '2024-02-29')");

    const text_cases = .{
        .{ "INTERVAL 1 DAY + d", .{ "2024-02-01", "2024-03-01" } },
        .{ "INTERVAL 1 MONTH + ts", .{ "2024-02-29 10:30:00", "2024-03-29 23:59:59" } },
        .{ "INTERVAL 30 MINUTE + d", .{ "2024-01-31 00:30:00", "2024-02-29 00:30:00" } },
        .{ "INTERVAL 1 DAY + d + INTERVAL 1 HOUR", .{ "2024-02-01 01:00:00", "2024-03-01 01:00:00" } },
        .{ "INTERVAL 1 DAY + DATE '2024-01-02'", .{ "2024-01-03", "2024-01-03" } },
    };
    inline for (text_cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        const got = try helpers.collectStrings(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS CHAR) FROM dt ORDER BY id");
        defer helpers.freeStrings(allocator, got);
        try std.testing.expectEqual(@as(usize, 2), got.len);
        try std.testing.expectEqualStrings(c[1][0], got[0].?);
        try std.testing.expectEqualStrings(c[1][1], got[1].?);
    }

    const dates = try collectDates(allocator, db, "SELECT INTERVAL 1 DAY + DATE '2024-01-02'");
    defer allocator.free(dates);
    const moved = try collectDates(allocator, db, "SELECT DATE '2024-01-02' + INTERVAL 1 DAY");
    defer allocator.free(moved);
    try std.testing.expectEqualSlices(i32, moved, dates);

    const ids = try helpers.collectBigints(allocator, db, "SELECT id FROM dt WHERE INTERVAL 1 DAY + d = '2024-03-01'");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{2}, ids);

    try helpers.expectRunError(allocator, db, "SELECT INTERVAL 1 DAY - d FROM dt", error.SqlExpectedToken);
    const interval_fn = try helpers.collectBigints(allocator, db, "SELECT INTERVAL(5, 1, 10)");
    defer allocator.free(interval_fn);
    try std.testing.expectEqualSlices(i64, &.{1}, interval_fn);
}
