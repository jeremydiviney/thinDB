//! MySQL's date and TIME functions (#244): day numbers, periods, TIME text,
//! CONVERT_TZ, compound EXTRACT units, TIMESTAMPDIFF units and the wall
//! clock. thinDB has no TIME type, so a TIME is its text as MySQL prints it.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;

fn expectText(allocator: std.mem.Allocator, db: anytype, comptime sql: []const u8, want: []const ?[]const u8) !void {
    const got = helpers.collectStrings(allocator, db, sql) catch |err| {
        std.debug.print("sql: {s}\n", .{sql});
        return err;
    };
    defer helpers.freeStrings(allocator, got);
    errdefer std.debug.print("sql: {s}\n", .{sql});
    try std.testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| {
        if (w) |text| {
            try std.testing.expect(g != null);
            try std.testing.expectEqualStrings(text, g.?);
        } else try std.testing.expect(g == null);
    }
}

test "MySQL date and TIME functions over constants" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // Every value below is MySQL 8.4's unless its comment says otherwise.
    const cases = .{
        .{ "TO_DAYS('2026-09-26')", "740250" },
        .{ "TO_DAYS('2026-09-26 10:05:03')", "740250" },
        .{ "TO_DAYS(DATE '0000-01-01')", "1" },
        .{ "TO_DAYS('0000-03-01')", "60" },
        .{ "TO_SECONDS('2026-09-26 10:05:03')", "63957636303" },
        .{ "FROM_DAYS(740250)", "2026-09-26" },
        .{ "FROM_DAYS(366)", "0001-01-01" },
        // MySQL gives its zero date, which a DATE can't hold.
        .{ "FROM_DAYS(365)", null },
        .{ "PERIOD_ADD(202601, 13)", "202702" },
        .{ "PERIOD_ADD(6901, 1)", "206902" },
        .{ "PERIOD_DIFF(202601, 199912)", "313" },
        .{ "PERIOD_DIFF(6901, 7001)", "1188" },
        // MySQL raises an error for a period whose month isn't 1-12.
        .{ "PERIOD_ADD(202613, 1)", null },
        .{ "MAKEDATE(2026, 269)", "2026-09-26" },
        .{ "MAKEDATE(26, 1)", "2026-01-01" },
        .{ "MAKEDATE(2026, 400)", "2027-02-04" },
        .{ "MAKEDATE(2026, 0)", null },
        .{ "MAKEDATE(9999, 366)", null },
        .{ "TIME_TO_SEC('10:05:03')", "36303" },
        .{ "TIME_TO_SEC('-838:59:59')", "-3020399" },
        .{ "TIME_TO_SEC('2026-09-26 10:05:03')", "36303" },
        .{ "TIME_TO_SEC(TIMESTAMP '2026-09-26 10:05:03')", "36303" },
        .{ "TIME_TO_SEC(100503)", "36303" },
        .{ "TIME_TO_SEC('abc')", null },
        .{ "SEC_TO_TIME(3661)", "01:01:01" },
        .{ "SEC_TO_TIME(-3661)", "-01:01:01" },
        .{ "SEC_TO_TIME(3661.5)", "01:01:01.5" },
        .{ "SEC_TO_TIME(3661.123456789)", "01:01:01.123457" },
        .{ "SEC_TO_TIME(99999999)", "838:59:59" },
        .{ "SEC_TO_TIME('3661')", "01:01:01.000000" },
        .{ "SEC_TO_TIME(1e3)", "00:16:40.000000" },
        .{ "MAKETIME(12, 15, 30)", "12:15:30" },
        .{ "MAKETIME(-12, 15, 30)", "-12:15:30" },
        .{ "MAKETIME(12, 15, 30.5)", "12:15:30.5" },
        .{ "MAKETIME(12, 15, 59.9999999)", "12:16:00.000000" },
        .{ "MAKETIME(900, 0, 0)", "838:59:59" },
        .{ "MAKETIME(12, 60, 30)", null },
        .{ "HOUR('838:59:59')", "838" },
        .{ "HOUR('-10:05:03')", "10" },
        .{ "MINUTE('2026-09-26 10:05:03')", "5" },
        .{ "MINUTE('2026-09-26')", "20" },
        .{ "SECOND('10:05:03')", "3" },
        .{ "MICROSECOND('10:00:00.5')", "500000" },
        .{ "MICROSECOND('2026-09-26 10:00:00.123')", "123000" },
        .{ "TIME('2026-09-26 10:05:03.25')", "10:05:03.25" },
        .{ "TIME('10:05:03.5')", "10:05:03.5" },
        .{ "TIME('-838:59:59')", "-838:59:59" },
        .{ "TIME(TIMESTAMP '2026-09-26 10:05:03')", "10:05:03" },
        .{ "ADDTIME('2007-12-31 23:59:59.999999', '1 1:1:1.000002')", "2008-01-02 01:01:01.000001" },
        .{ "ADDTIME('01:00:00.999999', '02:00:00.999998')", "03:00:01.999997" },
        .{ "ADDTIME('10:00:00', '-11:00:00')", "-01:00:00" },
        .{ "SUBTIME('2007-12-31 23:59:59.999999', '1 1:1:1.000002')", "2007-12-30 22:58:58.999997" },
        .{ "SUBTIME('01:00:00.999999', '02:00:00.999998')", "-00:59:59.999999" },
        .{ "TIMEDIFF('2000-01-01 00:00:00', '2000-01-01 00:00:00.000001')", "-00:00:00.000001" },
        .{ "TIMEDIFF('10:00:00', '12:30:15.5')", "-02:30:15.5" },
        .{ "TIMEDIFF('2026-01-01 00:00:00', '2025-01-01 00:00:00')", "838:59:59" },
        .{ "TIMEDIFF('10:00:00', '2026-01-01 00:00:00')", null },
        .{ "TIMEDIFF(TIMESTAMP '2026-01-02 00:00:00', TIMESTAMP '2026-01-01 00:00:00')", "24:00:00" },
        .{ "TIMEDIFF(DATE '2026-01-02', DATE '2026-01-01')", "24:00:00" },
        .{ "CONVERT_TZ('2026-01-01 00:00:00', '+00:00', '+05:30')", "2026-01-01 05:30:00" },
        .{ "CONVERT_TZ('2026-01-01 00:00:00', '+05:30', '-08:00')", "2025-12-31 10:30:00" },
        .{ "CONVERT_TZ('2026-01-01 00:00:00', '+00:00', '+14:01')", null },
        .{ "CONVERT_TZ('1970-01-01 00:00:00', '+00:00', '+01:00')", "1970-01-01 00:00:00" },
        // A MySQL without time zone tables knows no named zone; thinDB also
        // knows UTC.
        .{ "CONVERT_TZ('2026-01-01 00:00:00', 'Europe/Paris', '+00:00')", null },
        .{ "CONVERT_TZ('2026-01-01 00:00:00', 'UTC', '+01:00')", "2026-01-01 01:00:00" },
        .{ "EXTRACT(YEAR_MONTH FROM '2026-09-26 10:05:03')", "202609" },
        .{ "EXTRACT(YEAR_MONTH FROM DATE '2026-09-26')", "202609" },
        .{ "EXTRACT(DAY_HOUR FROM '2026-09-26 10:05:03')", "2610" },
        .{ "EXTRACT(DAY_MINUTE FROM '2026-09-26 10:05:03')", "261005" },
        .{ "EXTRACT(DAY_SECOND FROM '2026-09-26 10:05:03')", "26100503" },
        .{ "EXTRACT(DAY_MICROSECOND FROM '2026-09-26 10:05:03.123456')", "26100503123456" },
        .{ "EXTRACT(HOUR_MINUTE FROM '2026-09-26 10:05:03')", "1005" },
        .{ "EXTRACT(HOUR_SECOND FROM '10:05:03')", "100503" },
        .{ "EXTRACT(HOUR_MICROSECOND FROM '10:05:03.5')", "100503500000" },
        .{ "EXTRACT(MINUTE_SECOND FROM '10:05:03')", "503" },
        .{ "EXTRACT(MINUTE_MICROSECOND FROM '10:05:03.25')", "503250000" },
        .{ "EXTRACT(SECOND_MICROSECOND FROM '10:05:03.25')", "3250000" },
        .{ "EXTRACT(HOUR_SECOND FROM '-10:05:03')", "-100503" },
        .{ "EXTRACT(DAY_HOUR FROM '1 10:05:03')", "34" },
        .{ "EXTRACT(MICROSECOND FROM '10:00:00.5')", "500000" },
        .{ "TIMESTAMPADD(MICROSECOND, 1, '2026-01-01')", "2026-01-01 00:00:00.000001" },
        .{ "TIMESTAMPADD(QUARTER, 1, '2026-01-31')", "2026-04-30" },
        .{ "TIMESTAMPADD(WEEK, 1, '2026-01-31')", "2026-02-07" },
        .{ "TIMESTAMPADD(SQL_TSI_MONTH, 1, '2026-01-31')", "2026-02-28" },
        .{ "TIMESTAMPDIFF(QUARTER, '2026-01-01', '2026-12-31')", "3" },
        .{ "TIMESTAMPDIFF(WEEK, '2026-01-01', '2026-12-31')", "52" },
        .{ "TIMESTAMPDIFF(MICROSECOND, '2026-01-01', '2026-01-01 00:00:01.5')", "1500000" },
        .{ "TIMESTAMPDIFF(SECOND, '1970-01-01', '2100-01-01')", "4102444800" },
        .{ "TIMESTAMPDIFF(MONTH, '2026-01-31 10:00:00', '2026-02-28 09:00:00')", "0" },
        .{ "TIMESTAMPDIFF(SQL_TSI_DAY, '2026-01-01 12:00:00', '2026-01-03 11:59:59')", "1" },
        .{ "TIMESTAMPDIFF(YEAR, DATE '2024-02-29', DATE '2025-02-28')", "0" },
        .{ "TIMESTAMPDIFF(DAY, NULL, '2026-01-01')", null },
        .{ "STR_TO_DATE('10:05:03', '%H:%i:%s')", "10:05:03" },
        .{ "STR_TO_DATE('10:05:03.5', '%H:%i:%s.%f')", "10:05:03.500000" },
        .{ "STR_TO_DATE('10:05 PM', '%h:%i %p')", "22:05:00" },
        .{ "STR_TO_DATE('5 10', '%d %H')", "130:00:00" },
        .{ "STR_TO_DATE('24', '%H')", null },
        .{ "STR_TO_DATE('01.02.2026', GET_FORMAT(DATE, 'EUR'))", "2026-02-01" },
        .{ "GET_FORMAT(DATE, 'USA')", "%m.%d.%Y" },
        .{ "GET_FORMAT(TIME, 'USA')", "%h:%i:%s %p" },
        .{ "GET_FORMAT(DATETIME, 'JIS')", "%Y-%m-%d %H:%i:%s" },
        .{ "GET_FORMAT(TIMESTAMP, 'INTERNAL')", "%Y%m%d%H%i%s" },
        .{ "GET_FORMAT(DATE, 'XYZ')", null },
    };
    inline for (cases) |c| try expectText(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS CHAR)", &.{c[1]});
}

test "MySQL date and TIME functions over columns" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE tm (id BIGINT PRIMARY KEY, d DATE, ts DATETIME, t VARCHAR(20), n INT, x DECIMAL(10,3))");
    try exec(allocator, db,
        \\INSERT INTO tm VALUES (1, '2026-09-26', '2026-09-26 10:05:03.25', '10:05:03', 3661, 3661.5),
        \\  (2, '2024-02-29', '2024-02-29 23:59:59', '-838:59:59', -1, 0.001), (3, NULL, NULL, NULL, NULL, NULL)
    );

    // MySQL 8.4's values, with each column replaced by a constant of its
    // type; a DATETIME shows six fraction digits when it has a fraction.
    const cases = .{
        .{ "TO_DAYS(d)", .{ "740250", "739310", null } },
        .{ "TO_DAYS(ts)", .{ "740250", "739310", null } },
        .{ "TO_SECONDS(ts)", .{ "63957636303", "63876470399", null } },
        .{ "FROM_DAYS(TO_DAYS(d) + 1)", .{ "2026-09-27", "2024-03-01", null } },
        .{ "TIME_TO_SEC(t)", .{ "36303", "-3020399", null } },
        .{ "TIME_TO_SEC(ts)", .{ "36303", "86399", null } },
        .{ "SEC_TO_TIME(n)", .{ "01:01:01", "-00:00:01", null } },
        .{ "SEC_TO_TIME(x)", .{ "01:01:01.500", "00:00:00.001", null } },
        .{ "MAKETIME(n, 0, 0)", .{ "838:59:59", "-01:00:00", null } },
        .{ "MAKEDATE(YEAR(d), n)", .{ "2036-01-09", null, null } },
        .{ "HOUR(t)", .{ "10", "838", null } },
        .{ "MINUTE(t)", .{ "5", "59", null } },
        .{ "SECOND(t)", .{ "3", "59", null } },
        .{ "MICROSECOND(ts)", .{ "250000", "0", null } },
        .{ "TIME(ts)", .{ "10:05:03.250000", "23:59:59", null } },
        .{ "TIME(t)", .{ "10:05:03", "-838:59:59", null } },
        .{ "TIMEDIFF(ts, d)", .{ null, null, null } },
        .{ "TIMEDIFF(t, '01:00:00')", .{ "09:05:03", "-838:59:59", null } },
        .{ "ADDTIME(ts, t)", .{ "2026-09-26 20:10:06.250000", "2024-01-26 01:00:00", null } },
        .{ "SUBTIME(t, '00:00:01')", .{ "10:05:02", "-838:59:59", null } },
        .{ "CONVERT_TZ(ts, '+00:00', '-05:00')", .{ "2026-09-26 05:05:03.250000", "2024-02-29 18:59:59", null } },
        .{ "EXTRACT(DAY_SECOND FROM t)", .{ "100503", "-8385959", null } },
        .{ "EXTRACT(HOUR_MICROSECOND FROM t)", .{ "100503000000", "-8385959000000", null } },
        .{ "EXTRACT(YEAR_MONTH FROM d)", .{ "202609", "202402", null } },
        .{ "EXTRACT(YEAR_MONTH FROM ts)", .{ "202609", "202402", null } },
        .{ "TIMESTAMPDIFF(MONTH, d, ts)", .{ "0", "0", null } },
        .{ "TIMESTAMPDIFF(SECOND, d, ts)", .{ "36303", "86399", null } },
        .{ "TIMESTAMPDIFF(MICROSECOND, d, ts)", .{ "36303250000", "86399000000", null } },
        .{ "TIMESTAMPDIFF(QUARTER, ts, '2027-01-01')", .{ "1", "11", null } },
        .{ "STR_TO_DATE(t, '%H:%i:%s')", .{ "10:05:03", null, null } },
        .{ "PERIOD_ADD(202601, n)", .{ "233102", "202512", null } },
    };
    inline for (cases) |c| {
        const want: [3]?[]const u8 = c[1];
        try expectText(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS CHAR) FROM tm ORDER BY id", &want);
    }
}

test "CURTIME and UTC_TIME are the statement's time of day as TIME text" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // The clock is read once per statement and runs in UTC.
    const agree = .{
        "TIME_TO_SEC(CURTIME()) = TIME_TO_SEC(NOW())",
        "CURRENT_TIME = UTC_TIME()",
        "CURTIME(0) = CURTIME()",
        "LEFT(CURTIME(6), 8) = CURTIME()",
        "SYSDATE() = NOW()",
        "UTC_DATE() = CURDATE()",
        "UTC_TIMESTAMP() = CURRENT_TIMESTAMP",
        "LENGTH(CURRENT_TIME(3)) = 12",
        "LENGTH(UTC_TIME) = 8",
    };
    inline for (agree) |pred| try expectText(allocator, db, "SELECT CAST(CASE WHEN " ++ pred ++ " THEN 'yes' ELSE 'no' END AS CHAR)", &.{"yes"});
}
