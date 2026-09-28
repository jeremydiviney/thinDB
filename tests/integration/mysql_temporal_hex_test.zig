//! MySQL's implicit conversions around temporals and hex literals: CAST AS
//! TIME (#261), dates whose day is past the month's end (#262), dates and
//! datetimes read as numbers (#263) and summed as numbers (#305), and hex
//! literals read as integers in a numeric context (#269). Every expected
//! value is MySQL 8.4's unless a comment says otherwise.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;

/// The first column of `sql`'s rows as text, the way the MySQL wire prints
/// it: a boolean is 1 or 0.
fn firstColumnText(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]?[]u8 {
    var q = try helpers.runSql(allocator, db, sql);
    defer q.deinit();
    return columnText(allocator, &q);
}

fn mysqlFirstColumnText(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]?[]u8 {
    var q = try helpers.runSqlMysqlSession(allocator, db, sql);
    defer q.deinit();
    return columnText(allocator, &q);
}

fn columnText(allocator: std.mem.Allocator, q: *helpers.RunResult) ![]?[]u8 {
    var out: std.ArrayList(?[]u8) = .empty;
    errdefer {
        for (out.items) |v| if (v) |x| allocator.free(x);
        out.deinit(allocator);
    }
    while (try q.next()) |batch| {
        const col = batch.values[0];
        for (0..batch.row_count) |row| {
            const text: ?[]u8 = if (!col.isValid(row)) null else switch (col.data) {
                .string, .varchar, .char => |sv| try allocator.dupe(u8, sv.rowBytes(row)),
                inline .boolean, .tinyint, .smallint, .int, .bigint, .largeint, .double => |s| try std.fmt.allocPrint(allocator, "{d}", .{s[row]}),
                else => return error.TestUnexpectedType,
            };
            errdefer if (text) |x| allocator.free(x);
            try out.append(allocator, text);
        }
    }
    return out.toOwnedSlice(allocator);
}

fn expectRows(allocator: std.mem.Allocator, db: anytype, sql: []const u8, want: []const ?[]const u8) !void {
    const got = firstColumnText(allocator, db, sql) catch |err| {
        std.debug.print("sql: {s}\n", .{sql});
        return err;
    };
    try expectText(allocator, got, sql, want);
}

fn expectMysqlRows(allocator: std.mem.Allocator, db: anytype, sql: []const u8, want: []const ?[]const u8) !void {
    const got = mysqlFirstColumnText(allocator, db, sql) catch |err| {
        std.debug.print("sql: {s}\n", .{sql});
        return err;
    };
    try expectText(allocator, got, sql, want);
}

/// Checks `got` against `want` and frees it.
fn expectText(allocator: std.mem.Allocator, got: []?[]u8, sql: []const u8, want: []const ?[]const u8) !void {
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

test "CAST AS TIME reads text, dates, datetimes and HHMMSS numbers" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const cases = .{
        .{ "CAST('2026-09-26 10:05:03' AS TIME)", "10:05:03" },
        .{ "CAST('10:05:03.5' AS TIME)", "10:05:04" },
        .{ "CAST('10:05:03.5' AS TIME(3))", "10:05:03.500" },
        .{ "CAST('-10:05:03.5' AS TIME)", "-10:05:04" },
        .{ "CAST('838:59:59.5' AS TIME)", "838:59:59" },
        .{ "CAST('23:59:59.5' AS TIME)", "24:00:00" },
        .{ "CAST('2026-09-26 23:59:59.6' AS TIME)", "24:00:00" },
        .{ "CAST('10:05' AS TIME)", "10:05:00" },
        .{ "CAST('1 10:05:03' AS TIME(2))", "34:05:03.00" },
        .{ "CAST('abc' AS TIME)", null },
        .{ "CAST(NULL AS TIME)", null },
        .{ "CAST(DATE '2026-09-26' AS TIME)", "00:00:00" },
        .{ "CAST(TIMESTAMP '2026-09-26 10:05:03.123456' AS TIME)", "10:05:03" },
        .{ "CAST(TIMESTAMP '2026-09-26 10:05:03.123456' AS TIME(6))", "10:05:03.123456" },
        .{ "CAST(100503 AS TIME)", "10:05:03" },
        .{ "CAST(100503.5 AS TIME)", "10:05:04" },
        .{ "CAST(-100503 AS TIME)", "-10:05:03" },
        .{ "CAST(8390000 AS TIME)", null },
        .{ "CAST(106000 AS TIME)", null },
        .{ "CAST(20260926100503 AS TIME)", "10:05:03" },
        .{ "CAST(20260926100503.5 AS TIME)", "10:05:04" },
        .{ "CAST(20260230100503 AS TIME)", null },
        .{ "CAST(1e3 AS TIME)", "00:10:00" },
        .{ "CAST(CAST(100503.25 AS DECIMAL(10,2)) AS TIME(1))", "10:05:03.3" },
        .{ "CAST(8385959 AS TIME)", "838:59:59" },
        .{ "CAST(8385959.5 AS TIME)", "838:59:59" },
        .{ "CAST(123456789 AS TIME)", null },
        .{ "CAST(1000000 AS TIME)", "100:00:00" },
        .{ "CAST(-8390000 AS TIME)", null },
        .{ "CAST(99991231235959 AS TIME)", "23:59:59" },
        .{ "CAST(1234567890 AS TIME)", null },
        .{ "CAST(-0.5 AS TIME)", "-00:00:01" },
        .{ "CAST(0.5 AS TIME)", "00:00:01" },
        .{ "CAST(59.9999999 AS TIME(6))", "00:01:00.000000" },
        .{ "CAST(TRUE AS TIME)", "00:00:01" },
    };
    inline for (cases) |c| try expectRows(allocator, db, "SELECT " ++ c[0], &.{c[1]});
}

test "a date whose day is past its month's end is invalid everywhere a date is read" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const cases = .{
        .{ "DATE('2026-02-30')", null },
        .{ "CAST('2026-02-30' AS DATE)", null },
        .{ "CAST('2026-02-30' AS DATETIME)", null },
        .{ "CAST('2026-04-31 10:00:00' AS DATETIME)", null },
        .{ "DATE('0000-02-29')", null },
        .{ "DATE('2024-02-29')", "2024-02-29" },
        .{ "DATE('2026-02-28')", "2026-02-28" },
    };
    inline for (cases) |c| try expectRows(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS CHAR)", &.{c[1]});

    try exec(allocator, db, "CREATE TABLE dd (id INT, d DATE, ts DATETIME)");
    try exec(allocator, db, "INSERT INTO dd VALUES (1, '2026-02-28', '2026-02-28 10:00:00')");
    // MySQL's strict mode rejects the row (error 1292).
    try helpers.expectRunError(allocator, db, "INSERT INTO dd VALUES (2, '2026-02-30', NULL)", error.TypeMismatch);
    try helpers.expectRunError(allocator, db, "INSERT INTO dd VALUES (2, NULL, '2026-04-31 10:00:00')", error.TypeMismatch);
    try helpers.expectRunError(allocator, db, "SELECT DATE '2026-02-30'", error.SqlExpectedValue);
    // MySQL raises error 1525 comparing with an impossible date.
    try helpers.expectRunError(allocator, db, "SELECT id FROM dd WHERE d = '2026-02-30'", error.InvalidTemporalLiteral);
    try expectRows(allocator, db, "SELECT id FROM dd WHERE d = '2026-02-28'", &.{"1"});
}

test "a date or datetime in a numeric context is its YYYYMMDD[HHMMSS] number" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const cases = .{
        .{ "DATE '2026-09-27' + 0", "20260927" },
        .{ "TIMESTAMP '2026-09-27 12:34:56' + 0", "20260927123456" },
        .{ "TIMESTAMP '2026-09-27 12:34:56' - 1", "20260927123455" },
        .{ "DATE '2026-09-27' - 1", "20260926" },
        .{ "DATE '2026-09-27' * 2", "40521854" },
        .{ "DATE '2026-09-27' + DATE '2026-09-27'", "40521854" },
        .{ "ABS(DATE '2026-09-27')", "20260927" },
        .{ "DATE '2026-09-27' DIV 100", "202609" },
        .{ "DATE '2026-09-27' % 100", "27" },
        .{ "TIMESTAMP '2026-09-27 12:34:56' DIV 1000000", "20260927" },
        .{ "CAST(TIMESTAMP '2026-09-27 23:59:59.5' AS SIGNED)", "20260928000000" },
        .{ "CAST(DATE '2026-09-27' AS UNSIGNED)", "20260927" },
        .{ "CAST(DATE '2026-09-27' AS DOUBLE)", "20260927" },
        .{ "FLOOR(TIMESTAMP '2026-09-27 12:34:56')", "20260927123456" },
        // MySQL types this DECIMAL (10130463.5000); the value is the same.
        .{ "DATE '2026-09-27' / 2", "10130463.5" },
    };
    inline for (cases) |c| try expectRows(allocator, db, "SELECT " ++ c[0], &.{c[1]});

    const decimal_cases = .{
        .{ "DATE '2026-09-27' + 1.5", "20260928.5" },
        .{ "DATE '2026-09-27' + 0.0", "20260927.0" },
        .{ "CAST(TIMESTAMP '2026-09-27 23:59:59.95' AS DECIMAL(20,1))", "20260927235960.0" },
        .{ "CAST(DATE '2026-09-27' AS DECIMAL(10,2))", "20260927.00" },
    };
    inline for (decimal_cases) |c| try expectRows(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS CHAR)", &.{c[1]});

    try expectRows(allocator, db, "SELECT CAST(CURDATE() + 0 AS CHAR) = DATE_FORMAT(CURDATE(), '%Y%m%d')", &.{"1"});

    try exec(allocator, db, "CREATE TABLE dn (id INT, d DATE, ts DATETIME)");
    try exec(allocator, db, "INSERT INTO dn VALUES (1, '2026-09-26', '2026-09-26 10:05:03'), (2, '2026-02-28', '2026-02-28 23:59:59'), (3, NULL, NULL)");
    try expectRows(allocator, db, "SELECT d + 0 FROM dn ORDER BY id", &.{ "20260926", "20260228", null });
    try expectRows(allocator, db, "SELECT ts + 0 FROM dn ORDER BY id", &.{ "20260926100503", "20260228235959", null });
    try expectRows(allocator, db, "SELECT CAST(d AS SIGNED) FROM dn ORDER BY id", &.{ "20260926", "20260228", null });
    try expectRows(allocator, db, "SELECT ROUND(SQRT(d), 2) FROM dn ORDER BY id", &.{ "4501.21", "4501.14", null });
    // `d`'s zone maps hold day numbers, which must not prune `d + 0`.
    try expectRows(allocator, db, "SELECT id FROM dn WHERE d + 0 = 20260926", &.{"1"});
    try expectRows(allocator, db, "SELECT id FROM dn WHERE d + 0 > 20260300 ORDER BY id", &.{"1"});
    try expectRows(allocator, db, "SELECT SUM(d + 0) FROM dn", &.{"40521154"});
}

test "an aggregate over a DATE or DATETIME sums its number in MySQL" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE ag (id BIGINT PRIMARY KEY, d DATE, ts DATETIME, g INT, k VARCHAR(5))");
    try exec(allocator, db,
        \\INSERT INTO ag VALUES (1, '2026-09-26', '2026-09-26 10:05:03', 1, 'a'),
        \\  (2, '2026-09-27', '2026-09-27 23:59:59', 1, 'a'), (3, NULL, NULL, 2, 'b'),
        \\  (4, '2001-01-01', '2001-01-01 00:00:00', 2, 'b'), (5, '1999-12-31', '1999-12-31 23:59:59', 2, 'b')
    );

    // MySQL types SUM and AVG here DECIMAL (20130796.2500); the values are
    // the same.
    const cases = .{
        .{ "SELECT SUM(d) FROM ag", &[_]?[]const u8{"80523185"} },
        .{ "SELECT AVG(d) FROM ag", &[_]?[]const u8{"20130796.25"} },
        .{ "SELECT SUM(ts) FROM ag", &[_]?[]const u8{"80523185572421"} },
        .{ "SELECT AVG(ts) FROM ag", &[_]?[]const u8{"20130796393105.25"} },
        .{ "SELECT SUM(DISTINCT d) FROM ag", &[_]?[]const u8{"80523185"} },
        .{ "SELECT AVG(DISTINCT d) FROM ag", &[_]?[]const u8{"20130796.25"} },
        .{ "SELECT SUM(d + INTERVAL 1 DAY) FROM ag", &[_]?[]const u8{"80532058"} },
        .{ "SELECT BIT_XOR(d) FROM ag", &[_]?[]const u8{"24267"} },
        .{ "SELECT STDDEV_POP(d) FROM ag", &[_]?[]const u8{"130301.15723848158"} },
        .{ "SELECT SUM(d) FROM ag WHERE d > 20000000", &[_]?[]const u8{"60531954"} },
        .{ "SELECT MIN(d) + 0 FROM ag", &[_]?[]const u8{"19991231"} },
        .{ "SELECT SUM(d) FROM ag GROUP BY g ORDER BY g", &[_]?[]const u8{ "40521853", "40001332" } },
        .{ "SELECT AVG(d) FROM ag GROUP BY g ORDER BY g", &[_]?[]const u8{ "20260926.5", "20000666" } },
        .{ "SELECT SUM(d) FROM ag GROUP BY k ORDER BY k", &[_]?[]const u8{ "40521853", "40001332" } },
        .{ "SELECT STDDEV_POP(d) FROM ag GROUP BY g ORDER BY g", &[_]?[]const u8{ "0.5", "9435" } },
        .{ "SELECT VAR_SAMP(d) FROM ag WHERE g = 1", &[_]?[]const u8{"0.5"} },
        .{ "SELECT BIT_OR(d) FROM ag GROUP BY g ORDER BY g", &[_]?[]const u8{ "20260927", "20012799" } },
        .{ "SELECT BIT_AND(ts) FROM ag GROUP BY g ORDER BY g", &[_]?[]const u8{ "20260926030871", "19939691071296" } },
        .{ "SELECT SUM(ts) FROM ag GROUP BY g HAVING SUM(ts) > 40000000000000 ORDER BY g", &[_]?[]const u8{ "40521853336462", "40001332235959" } },
        .{ "SELECT SUM(d) + 1 FROM ag GROUP BY g ORDER BY g", &[_]?[]const u8{ "40521854", "40001333" } },
        .{ "WITH c AS (SELECT d, g FROM ag WHERE id < 4) SELECT SUM(d) FROM c GROUP BY g ORDER BY g", &[_]?[]const u8{ "40521853", null } },
        .{ "SELECT SUM(d) FROM (SELECT d FROM ag ORDER BY id LIMIT 2) s", &[_]?[]const u8{"40521853"} },
        // MySQL can't join a TEMPORARY table with itself; each row matches once.
        .{ "SELECT SUM(x.d) FROM ag x JOIN ag y ON x.id = y.id", &[_]?[]const u8{"80523185"} },
    };
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("ag", .{})).flush();
        inline for (cases) |c| try expectMysqlRows(allocator, db, c[0], c[1]);
    }

    // The other dialects reject a number-only aggregate over a temporal.
    try helpers.expectRunError(allocator, db, "SELECT SUM(d) FROM ag", error.AggregateUnsupportedType);
    try helpers.expectRunError(allocator, db, "SELECT g, STDDEV_POP(d) FROM ag GROUP BY g", error.AggregateUnsupportedType);
}

test "a hex literal is its integer in a numeric context and its bytes elsewhere" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const cases = .{
        .{ "0x41 = 65", "1" },
        .{ "65 = 0x41", "1" },
        .{ "0x41 = 65.0", "1" },
        .{ "0x41 < 66", "1" },
        .{ "0x41 <=> 65", "1" },
        .{ "0x41 = 'A'", "1" },
        .{ "0x41 > 'B'", "0" },
        .{ "0x41 = TRUE", "0" },
        .{ "0x01 = TRUE", "1" },
        .{ "0x3132 + 0", "12594" },
        .{ "X'41' + 0", "65" },
        .{ "X'' + 0", "0" },
        .{ "0x00 + 0", "0" },
        .{ "0x0102030405060708 + 0", "72623859790382856" },
        .{ "0xFFFFFFFFFFFFFFFF + 0", "18446744073709551615" },
        // Past eight bytes MySQL reads 0 and warns.
        .{ "0x414243444546474849 + 0", "0" },
        .{ "-0x41", "-65" },
        .{ "ABS(0x41)", "65" },
        .{ "FLOOR(0x41)", "65" },
        .{ "CAST(0x41 AS SIGNED)", "65" },
        .{ "CAST(0x41 AS DOUBLE)", "65" },
        .{ "0x41 DIV 2", "32" },
        .{ "0x41 % 10", "5" },
        // MySQL types this DECIMAL (32.5000); the value is the same.
        .{ "0x41 / 2", "32.5" },
        .{ "0x41 | 2", "67" },
        .{ "0x41 & 0x40", "64" },
        .{ "0x41", "A" },
        .{ "CONCAT(0x41, 1)", "A1" },
        .{ "LENGTH(0x4142)", "2" },
        .{ "HEX(0x41)", "41" },
        .{ "UPPER(0x61)", "A" },
        .{ "COALESCE(0x41, 1)", "A" },
        .{ "IF(1, 0x41, 2)", "A" },
        .{ "GREATEST(0x41, 1)", "A" },
    };
    inline for (cases) |c| try expectRows(allocator, db, "SELECT " ++ c[0], &.{c[1]});
    try expectRows(allocator, db, "SELECT CAST(0x41 * 1.5 AS CHAR)", &.{"97.5"});
    try expectRows(allocator, db, "SELECT CAST(CAST(0x41 AS DECIMAL(5,1)) AS CHAR)", &.{"65.0"});

    try exec(allocator, db, "CREATE TABLE hx (id INT, v INT, s VARCHAR(10), d DOUBLE, m DECIMAL(10,2))");
    try exec(allocator, db, "INSERT INTO hx VALUES (1, 65, 'A', 1.5, 2.5), (2, 0x3132, 0x3132, 0x41, 0x41)");
    try exec(allocator, db, "INSERT INTO hx (s, id, v) VALUES (X'42', 3, X'42'), (0x43, 4, 0x43)");
    try expectRows(allocator, db, "SELECT v FROM hx ORDER BY id", &.{ "65", "12594", "66", "67" });
    try expectRows(allocator, db, "SELECT s FROM hx ORDER BY id", &.{ "A", "12", "B", "C" });
    try expectRows(allocator, db, "SELECT d FROM hx ORDER BY id", &.{ "1.5", "65", null, null });
    try expectRows(allocator, db, "SELECT CAST(m AS CHAR) FROM hx ORDER BY id", &.{ "2.50", "65.00", null, null });

    const filters = .{
        .{ "v = 0x41", &[_][]const u8{"1"} },
        .{ "0x41 = v", &[_][]const u8{"1"} },
        .{ "s = 0x41", &[_][]const u8{"1"} },
        .{ "d = 0x41", &[_][]const u8{"2"} },
        .{ "m = 0x41", &[_][]const u8{"2"} },
        .{ "v + 0 = 0x41", &[_][]const u8{"1"} },
        .{ "v <=> 0x41", &[_][]const u8{"1"} },
        .{ "v > 0x42", &[_][]const u8{ "2", "4" } },
        .{ "v IN (0x41, 0x42)", &[_][]const u8{ "1", "3" } },
        .{ "v IN (0x41, 67)", &[_][]const u8{ "1", "4" } },
        .{ "s IN (0x41, 0x42)", &[_][]const u8{ "1", "3" } },
        .{ "v NOT IN (0x41, 0x42)", &[_][]const u8{ "2", "4" } },
        .{ "v BETWEEN 0x41 AND 0x43", &[_][]const u8{ "1", "3", "4" } },
    };
    inline for (filters) |f| {
        const want: []const []const u8 = f[1];
        var opt: [4]?[]const u8 = undefined;
        for (want, 0..) |w, i| opt[i] = w;
        try expectRows(allocator, db, "SELECT id FROM hx WHERE " ++ f[0] ++ " ORDER BY id", opt[0..want.len]);
    }

    try exec(allocator, db, "UPDATE hx SET v = 0x3133, s = 0x3133 WHERE id = 1");
    try expectRows(allocator, db, "SELECT v FROM hx WHERE id = 1", &.{"12595"});
    try expectRows(allocator, db, "SELECT s FROM hx WHERE id = 1", &.{"13"});
}
