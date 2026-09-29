//! DATE '2024-01-15' and DATETIME '2024-01-15 12:34:56' typed literals
//! — SQL-standard temporal literal syntax. Parsed at SQL-parse time
//! into Value.date / Value.datetime so they compare type-cleanly
//! against DATE / DATETIME columns without going through text
//! coercion. TIMESTAMP is an alias of DATETIME.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(
        allocator,
        db,
        "CREATE TABLE shipments (id BIGINT PRIMARY KEY, ship_date DATE NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO shipments (id, ship_date) VALUES (1, '2024-01-15'), (2, '2024-03-01'), (3, '2024-06-10'), (4, '2024-12-31')",
    );
    const t = try db.openTable("shipments", .{});
    try t.flush();
    return db;
}

test "DATE typed literal: filter by exact match" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM shipments WHERE ship_date = DATE '2024-03-01'",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{2}, ids);
}

test "DATE typed literal: range filter" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM shipments WHERE ship_date > DATE '2024-03-01' ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 3, 4 }, ids);
}

test "DATE typed literal: case-insensitive keyword" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM shipments WHERE ship_date < date '2024-04-01' ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, ids);
}

test "DATETIME typed literal: round trip" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(
        allocator,
        db,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, ts DATETIME NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO events (id, ts) VALUES (1, '2024-01-15 09:30:00'), (2, '2024-01-15 18:45:00')",
    );
    const t = try db.openTable("events", .{});
    try t.flush();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM events WHERE ts > DATETIME '2024-01-15 12:00:00'",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{2}, ids);
}

test "TIMESTAMP alias for DATETIME" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(
        allocator,
        db,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, ts DATETIME NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO events (id, ts) VALUES (1, '2024-01-15 09:30:00')",
    );
    const t = try db.openTable("events", .{});
    try t.flush();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM events WHERE ts = TIMESTAMP '2024-01-15 09:30:00'",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{1}, ids);
}

test "CAST text AS DATE / DATETIME: a column parses per row, a literal stays a constant" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE raw (id BIGINT PRIMARY KEY, s VARCHAR(32))");
    try exec(
        allocator,
        db,
        "INSERT INTO raw (id, s) VALUES (1, '2024-03-01'), (2, '2024-03-01 18:30:00'), (3, 'soon'), (4, NULL), (5, '2024-12-31')",
    );
    const t = try db.openTable("raw", .{});
    try t.flush();

    const cases = .{
        .{ .sql = "SELECT id FROM raw WHERE CAST(s AS DATE) = DATE '2024-03-01' ORDER BY id", .ids = &[_]i64{ 1, 2 } },
        .{ .sql = "SELECT id FROM raw WHERE CAST(s AS DATE) IS NULL ORDER BY id", .ids = &[_]i64{ 3, 4 } },
        .{ .sql = "SELECT id FROM raw WHERE CAST(s AS DATETIME) = DATETIME '2024-03-01 18:30:00'", .ids = &[_]i64{2} },
        .{ .sql = "SELECT id FROM raw WHERE CAST(s AS DATETIME) = DATETIME '2024-03-01 00:00:00'", .ids = &[_]i64{1} },
        .{ .sql = "SELECT id FROM raw WHERE DATEDIFF(CAST(s AS DATE), CAST('2024-03-01' AS DATE)) > 0", .ids = &[_]i64{5} },
        .{ .sql = "SELECT id FROM raw WHERE EXTRACT(MONTH FROM CAST(s AS DATE)) = 12", .ids = &[_]i64{5} },
    };
    inline for (cases) |c| {
        const ids = try collectBigints(allocator, db, c.sql);
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(i64, c.ids, ids);
    }

    // A literal parses once while planning: the column is a non-null
    // constant, not a per-row parse that could yield NULL.
    var constant = try runSql(allocator, db, "SELECT CAST('2024-03-01' AS DATE) AS d, CAST('2024-03-01 18:30:00' AS DATETIME) AS ts FROM raw WHERE id = 1");
    defer constant.deinit();
    try std.testing.expectEqual(false, constant.outputSchema()[0].nullable);
    try std.testing.expectEqual(false, constant.outputSchema()[1].nullable);
    const row = (try constant.next()).?;
    try std.testing.expectEqual(@as(i32, 19783), row.values[0].data.date[0]);
    try std.testing.expectEqual(@as(i64, 19783 * std.time.us_per_day + (18 * 3600 + 30 * 60) * std.time.us_per_s), row.values[1].data.datetime[0]);

    var invalid = try runSql(allocator, db, "SELECT CAST('soon' AS DATE) AS d FROM raw WHERE id = 1");
    defer invalid.deinit();
    const invalid_row = (try invalid.next()).?;
    try std.testing.expectEqual(@as(usize, 1), invalid_row.row_count);
    try std.testing.expect(!invalid_row.values[0].isValid(0));
}

/// The first column of `sql` as text, NULL as null.
fn expectStrings(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, expected: []const ?[]const u8) !void {
    const got = try helpers.collectStrings(allocator, db, sql);
    defer helpers.freeStrings(allocator, got);
    errdefer std.debug.print("query: {s}\n", .{sql});
    try std.testing.expectEqual(expected.len, got.len);
    for (expected, got) |want, have| {
        if (want) |text| {
            try std.testing.expect(have != null);
            try std.testing.expectEqualStrings(text, have.?);
        } else try std.testing.expect(have == null);
    }
}

test "year 0 dates print, store and compute as the days they are (issue #393)" {
    // One proleptic Gregorian calendar, where year 0 is a leap year: the
    // day arithmetic StarRocks does (DATEDIFF, TO_DAYS, DAYOFWEEK,
    // DAYOFYEAR, DATE_SUB). MySQL counts year 0 as 365 days, so its
    // weekdays and day numbers before 0000-03-01 differ by one, and it has
    // no 0000-02-29. StarRocks disagrees with itself on that day: a
    // constant CAST accepts it and a column CAST doesn't.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE y0 (id BIGINT PRIMARY KEY, d DATE, ts DATETIME)");
    try exec(
        allocator,
        db,
        "INSERT INTO y0 VALUES (1, '0000-01-01', '0000-01-01 00:00:00'), (2, '0000-01-31', '0000-01-31 12:34:56'), " ++
            "(3, '0000-02-28', '0000-02-28 23:59:59.5'), (4, DATE '0000-02-29', DATETIME '0000-02-29 00:00:01'), " ++
            "(5, '0000-03-01', '0000-03-01 00:00:00'), (6, '0000-12-31', '0000-12-31 23:59:59'), (7, '0001-01-01', '0001-01-01 00:00:00')",
    );
    try exec(allocator, db, "CREATE TABLE y0_text (id BIGINT PRIMARY KEY, d DATE)");
    try exec(allocator, db, "INSERT INTO y0_text SELECT id, CAST(CAST(d AS CHAR) AS DATE) FROM y0");

    const dates = &[_]?[]const u8{ "0000-01-01", "0000-01-31", "0000-02-28", "0000-02-29", "0000-03-01", "0000-12-31", "0001-01-01" };
    const cases = .{
        .{ "CAST(d AS CHAR)", dates },
        .{ "CAST(ts AS CHAR)", &[_]?[]const u8{ "0000-01-01 00:00:00", "0000-01-31 12:34:56", "0000-02-28 23:59:59.500000", "0000-02-29 00:00:01", "0000-03-01 00:00:00", "0000-12-31 23:59:59", "0001-01-01 00:00:00" } },
        .{ "CAST(DATE(ts) AS CHAR)", dates },
        .{ "CAST(FROM_DAYS(TO_DAYS(d)) AS CHAR)", dates },
        .{ "CAST(YEAR(d) AS CHAR)", &[_]?[]const u8{ "0", "0", "0", "0", "0", "0", "1" } },
        .{ "CAST(MONTH(d) AS CHAR)", &[_]?[]const u8{ "1", "1", "2", "2", "3", "12", "1" } },
        .{ "CAST(DAY(d) AS CHAR)", &[_]?[]const u8{ "1", "31", "28", "29", "1", "31", "1" } },
        .{ "CAST(DAYOFWEEK(d) AS CHAR)", &[_]?[]const u8{ "7", "2", "2", "3", "4", "1", "2" } },
        .{ "DAYNAME(d)", &[_]?[]const u8{ "Saturday", "Monday", "Monday", "Tuesday", "Wednesday", "Sunday", "Monday" } },
        .{ "CAST(DAYOFYEAR(d) AS CHAR)", &[_]?[]const u8{ "1", "31", "59", "60", "61", "366", "1" } },
        .{ "CAST(DATEDIFF(d, '0000-01-01') AS CHAR)", &[_]?[]const u8{ "0", "30", "58", "59", "60", "365", "366" } },
        .{ "CAST(TO_DAYS(d) AS CHAR)", &[_]?[]const u8{ "0", "30", "58", "59", "60", "365", "366" } },
        .{ "CAST(TIMESTAMPDIFF(SECOND, '0000-01-01 00:00:00', ts) AS CHAR)", &[_]?[]const u8{ "0", "2637296", "5097599", "5097601", "5184000", "31622399", "31622400" } },
        .{ "CAST(DATE_ADD(d, INTERVAL 1 DAY) AS CHAR)", &[_]?[]const u8{ "0000-01-02", "0000-02-01", "0000-02-29", "0000-03-01", "0000-03-02", "0001-01-01", "0001-01-02" } },
        .{ "CAST(DATE_ADD(d, INTERVAL 1 YEAR) AS CHAR)", &[_]?[]const u8{ "0001-01-01", "0001-01-31", "0001-02-28", "0001-02-28", "0001-03-01", "0001-12-31", "0002-01-01" } },
        .{ "CAST(DATE_ADD(d, INTERVAL 1 MONTH) AS CHAR)", &[_]?[]const u8{ "0000-02-01", "0000-02-29", "0000-03-28", "0000-03-29", "0000-04-01", "0001-01-31", "0001-02-01" } },
        .{ "CAST(LAST_DAY(d) AS CHAR)", &[_]?[]const u8{ "0000-01-31", "0000-01-31", "0000-02-29", "0000-02-29", "0000-03-31", "0000-12-31", "0001-01-31" } },
    };
    const where_cases = .{
        .{ "d = '0000-02-29'", &[_]i64{4} },
        .{ "d = DATE '0000-01-01'", &[_]i64{1} },
        .{ "d < '0000-03-01'", &[_]i64{ 1, 2, 3, 4 } },
        .{ "ts BETWEEN '0000-02-28 00:00:00' AND '0000-02-29 23:59:59'", &[_]i64{ 3, 4 } },
        .{ "YEAR(d) = 0 AND MONTH(d) = 2", &[_]i64{ 3, 4 } },
        .{ "CAST(CAST(d AS CHAR) AS DATE) = d", &[_]i64{ 1, 2, 3, 4, 5, 6, 7 } },
    };
    for (0..2) |pass| {
        if (pass == 1) {
            try (try db.openTable("y0", .{})).flush();
            try (try db.openTable("y0_text", .{})).flush();
        }
        inline for (cases) |c| try expectStrings(allocator, db, "SELECT " ++ c[0] ++ " FROM y0 ORDER BY id", c[1]);
        try expectStrings(allocator, db, "SELECT CAST(d AS CHAR) FROM y0_text ORDER BY id", dates);
        // Row 1 minus a day leaves the calendar, which is #398.
        try expectStrings(
            allocator,
            db,
            "SELECT CAST(DATE_SUB(d, INTERVAL 1 DAY) AS CHAR) FROM y0 WHERE id > 1 ORDER BY id",
            &[_]?[]const u8{ "0000-01-30", "0000-02-27", "0000-02-28", "0000-02-29", "0000-12-30", "0000-12-31" },
        );
        inline for (where_cases) |c| {
            const got = try collectBigints(allocator, db, "SELECT id FROM y0 WHERE " ++ c[0] ++ " ORDER BY id");
            defer allocator.free(got);
            std.testing.expectEqualSlices(i64, c[1], got) catch |err| {
                std.debug.print("WHERE {s} (pass {d})\n", .{ c[0], pass });
                return err;
            };
        }
    }

    // A fraction before the epoch printed a second early.
    try expectStrings(allocator, db, "SELECT CAST(CAST('1969-12-31 23:59:59.5' AS DATETIME) AS CHAR) FROM y0 WHERE id = 1", &.{"1969-12-31 23:59:59.500000"});
}

test "CAST and DATE() read text and numbers as StarRocks does (issue #399)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE spell (id BIGINT PRIMARY KEY, s VARCHAR(40), n BIGINT, x DOUBLE)");
    try exec(allocator, db, "INSERT INTO spell (id, s, n, x) VALUES " ++
        "(1, '2026/1/1', 20260101, 20260101.9), " ++
        "(2, '20260101103000', 260101103000, 260101.5), " ++
        "(3, '2026-01-01 25:00:00', 20260101240000, -1), " ++
        "(4, ' 26-1-1 1:2:3.5 ', 1231, 991231), " ++
        "(5, '2026-01-01 10 30 00', 2026, 1e20), " ++
        "(6, 'garbage', NULL, NULL)");
    const t = try db.openTable("spell", .{});
    try t.flush();

    // The same text read per row from a column and folded from a literal.
    const texts = [_][]const u8{ "2026/1/1", "20260101103000", "2026-01-01 25:00:00", " 26-1-1 1:2:3.5 ", "2026-01-01 10 30 00", "garbage" };
    const cast_date = [_]?[]const u8{ "2026-01-01", "2026-01-01", "2026-01-01", "2026-01-01", null, null };
    const date_fn = [_]?[]const u8{ "2026-01-01", "2026-01-01", null, "2026-01-01", "2026-01-01", null };
    const cast_datetime = [_]?[]const u8{ "2026-01-01 00:00:00", "2026-01-01 10:30:00", null, "2026-01-01 01:02:03.500000", "2026-01-01 10:30:00", null };
    try expectStrings(allocator, db, "SELECT CAST(CAST(s AS DATE) AS CHAR) FROM spell ORDER BY id", &cast_date);
    try expectStrings(allocator, db, "SELECT CAST(DATE(s) AS CHAR) FROM spell ORDER BY id", &date_fn);
    try expectStrings(allocator, db, "SELECT CAST(CAST(s AS DATETIME) AS CHAR) FROM spell ORDER BY id", &cast_datetime);
    for (texts, cast_date, date_fn, cast_datetime) |text, want_date, want_date_fn, want_datetime| {
        var buf: [160]u8 = undefined;
        try expectStrings(allocator, db, try std.fmt.bufPrint(&buf, "SELECT CAST(CAST('{s}' AS DATE) AS CHAR) FROM spell WHERE id = 1", .{text}), &.{want_date});
        try expectStrings(allocator, db, try std.fmt.bufPrint(&buf, "SELECT CAST(DATE('{s}') AS CHAR) FROM spell WHERE id = 1", .{text}), &.{want_date_fn});
        try expectStrings(allocator, db, try std.fmt.bufPrint(&buf, "SELECT CAST(CAST('{s}' AS DATETIME) AS CHAR) FROM spell WHERE id = 1", .{text}), &.{want_datetime});
    }

    try expectStrings(allocator, db, "SELECT CAST(CAST(n AS DATE) AS CHAR) FROM spell ORDER BY id", &.{ "2026-01-01", "2026-01-01", null, "2000-12-31", null, null });
    try expectStrings(allocator, db, "SELECT CAST(DATE(n) AS CHAR) FROM spell ORDER BY id", &.{ "2026-01-01", "2026-01-01", null, "2000-12-31", null, null });
    try expectStrings(allocator, db, "SELECT CAST(CAST(n AS DATETIME) AS CHAR) FROM spell ORDER BY id", &.{ "2026-01-01 00:00:00", "2026-01-01 10:30:00", null, "2000-12-31 00:00:00", null, null });
    try expectStrings(allocator, db, "SELECT CAST(CAST(x AS DATE) AS CHAR) FROM spell ORDER BY id", &.{ "2026-01-01", "2026-01-01", null, "1999-12-31", null, null });
    try expectStrings(allocator, db, "SELECT CAST(CAST(20260101 AS DATE) AS CHAR) FROM spell WHERE id = 1", &.{"2026-01-01"});
    // StarRocks rejects a DECIMAL; thinDB reads it as a double, truncated.
    try expectStrings(allocator, db, "SELECT CAST(CAST(20260101103000.5 AS DATETIME) AS CHAR) FROM spell WHERE id = 1", &.{"2026-01-01 10:30:00"});
    try expectStrings(allocator, db, "SELECT CAST(DATE(20260101.5) AS CHAR) FROM spell WHERE id = 1", &.{"2026-01-01"});

    // A literal that reads is a non-null constant.
    var constant = try runSql(allocator, db, "SELECT CAST('2026/1/1' AS DATE), DATE('2026-01-01T10:30:00Z') FROM spell WHERE id = 1");
    defer constant.deinit();
    try std.testing.expectEqual(false, constant.outputSchema()[0].nullable);
    try std.testing.expectEqual(false, constant.outputSchema()[1].nullable);

    // Text a function wants as a date reads as a CAST would read it, and
    // text that doesn't read is NULL, as in StarRocks.
    try expectStrings(allocator, db, "SELECT CAST(UNIX_TIMESTAMP('2026/1/1 10:30') AS CHAR) FROM spell WHERE id = 1", &.{"1767263400"});
    try expectStrings(allocator, db, "SELECT CAST(UNIX_TIMESTAMP('garbage') AS CHAR) FROM spell WHERE id = 1", &.{null});
    try expectStrings(allocator, db, "SELECT CAST(YEAR('garbage') AS CHAR) FROM spell WHERE id = 1", &.{null});

    // INSERT ... SELECT reads text into a DATE column as CAST does.
    try exec(allocator, db, "CREATE TABLE spelled (id BIGINT PRIMARY KEY, d DATE)");
    try exec(allocator, db, "INSERT INTO spelled SELECT id, s FROM spell WHERE id IN (1, 2, 4)");
    try expectStrings(allocator, db, "SELECT CAST(d AS CHAR) FROM spelled ORDER BY id", &.{ "2026-01-01", "2026-01-01", "2026-01-01" });
}

test "a text literal keeps its time of day in date functions and stays text where text fits, as in StarRocks" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE one (id BIGINT PRIMARY KEY)");
    try exec(allocator, db, "INSERT INTO one VALUES (1)");

    // Expected values are StarRocks'. Text takes a DATETIME parameter
    // before a DATE one; a function that takes only a DATE takes the day.
    const cases = .{
        .{ "DATE_ADD('2026-01-01 10:30:00', INTERVAL 1 DAY)", "2026-01-02 10:30:00" },
        .{ "DATE_SUB('2026-01-01 10:30:00', INTERVAL 1 DAY)", "2025-12-31 10:30:00" },
        .{ "DATE_SUB('2026-01-01 10:30:00', INTERVAL 1 HOUR)", "2026-01-01 09:30:00" },
        .{ "DATE_ADD('2026-01-01 10:30:00', INTERVAL 1 WEEK)", "2026-01-08 10:30:00" },
        .{ "DATE_ADD('2026-01-01 10:30:00', INTERVAL 1 QUARTER)", "2026-04-01 10:30:00" },
        .{ "DATE_ADD('2026-01-01 10:30:00', INTERVAL 1 YEAR)", "2027-01-01 10:30:00" },
        .{ "ADDDATE('2026-01-01 10:30:00', 1)", "2026-01-02 10:30:00" },
        .{ "SUBDATE('2026-01-01 10:30:00', 1)", "2025-12-31 10:30:00" },
        .{ "'2026-01-01 10:30:00' + INTERVAL 1 MONTH", "2026-02-01 10:30:00" },
        .{ "INTERVAL 1 DAY + '2026-01-01 10:30:00'", "2026-01-02 10:30:00" },
        .{ "TIMESTAMPADD(DAY, 1, '2026-01-01 10:30:00')", "2026-01-02 10:30:00" },
        .{ "TIMESTAMPADD(MONTH, 1, '2026-01-01 10:30:00')", "2026-02-01 10:30:00" },
        .{ "DATE_ADD('2026-01-31', INTERVAL 1 DAY)", "2026-02-01 00:00:00" },
        .{ "'2026-01-31' + INTERVAL 1 MONTH", "2026-02-28 00:00:00" },
        .{ "DATE_ADD('2026/1/31', INTERVAL 1 DAY)", "2026-02-01 00:00:00" },
        .{ "LAST_DAY('2026/2/1 10:00')", "2026-02-28" },
        .{ "DAYNAME('2026-01-01 10:30:00')", "Thursday" },
        .{ "WEEK('2026-01-01 10:30:00', 1)", "1" },
        .{ "TO_DAYS('2026-01-01 10:30:00')", "739982" },
        .{ "DATEDIFF('2026-01-02 01:00:00', '2026-01-01 23:00:00')", "1" },
        .{ "DATEDIFF('2026-01-02 01:00:00', DATE '2026-01-01')", "1" },
        .{ "TIMESTAMPDIFF(HOUR, '2026-01-01 23:00:00', '2026-01-02 01:00:00')", "2" },
        // Text beside a DATE where text fits stays text.
        .{ "COALESCE('2026-01-01 10:30:00', DATE '2026-01-01')", "2026-01-01 10:30:00" },
        .{ "COALESCE(NULL, '2026-01-01 10:30:00', DATE '2026-01-01')", "2026-01-01 10:30:00" },
        .{ "COALESCE('2026/1/1', DATE '2026-01-02')", "2026/1/1" },
        .{ "IFNULL('2026-01-01 10:30:00', DATE '2026-01-01')", "2026-01-01 10:30:00" },
        .{ "GREATEST('2026-01-01 10:30:00', DATE '2026-01-01')", "2026-01-01 10:30:00" },
        .{ "LEAST('2026/1/1', DATE '2026-01-02')", "2026-01-02" },
    };
    inline for (cases) |c| {
        try expectStrings(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS CHAR) FROM one", &.{c[1]});
    }
    try expectStrings(allocator, db, "SELECT CAST(DATE_ADD('abc', INTERVAL 1 DAY) AS CHAR) FROM one", &.{null});
}

test "text that isn't a literal reads as CAST reads it where a date function wants a date or datetime, as in StarRocks (issue #412)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE s (id BIGINT PRIMARY KEY, s VARCHAR(40))");
    try exec(allocator, db, "INSERT INTO s VALUES (1, '2026-01-01 10:30:00'), (2, '2026-01-31'), (3, '2026/1/31 10:00'), " ++
        "(4, '2026-02-29'), (5, '20260131'), (6, 'abc'), (7, ''), (8, NULL)");

    // Expected values are StarRocks', one per row of `s`: text takes a
    // DATETIME parameter before a DATE one, and text that doesn't read is
    // NULL.
    const cases = .{
        .{ "DATE_ADD(s, INTERVAL 1 DAY)", [_]?[]const u8{ "2026-01-02 10:30:00", "2026-02-01 00:00:00", "2026-02-01 10:00:00", null, "2026-02-01 00:00:00", null, null, null } },
        .{ "s + INTERVAL 1 MONTH", [_]?[]const u8{ "2026-02-01 10:30:00", "2026-02-28 00:00:00", "2026-02-28 10:00:00", null, "2026-02-28 00:00:00", null, null, null } },
        .{ "DATE_SUB(s, INTERVAL 1 HOUR)", [_]?[]const u8{ "2026-01-01 09:30:00", "2026-01-30 23:00:00", "2026-01-31 09:00:00", null, "2026-01-30 23:00:00", null, null, null } },
        .{ "TIMESTAMPADD(MINUTE, 90, s)", [_]?[]const u8{ "2026-01-01 12:00:00", "2026-01-31 01:30:00", "2026-01-31 11:30:00", null, "2026-01-31 01:30:00", null, null, null } },
        .{ "YEAR(s)", [_]?[]const u8{ "2026", "2026", "2026", null, "2026", null, null, null } },
        .{ "MONTH(s)", [_]?[]const u8{ "1", "1", "1", null, "1", null, null, null } },
        .{ "DAY(s)", [_]?[]const u8{ "1", "31", "31", null, "31", null, null, null } },
        .{ "QUARTER(s)", [_]?[]const u8{ "1", "1", "1", null, "1", null, null, null } },
        .{ "DAYOFWEEK(s)", [_]?[]const u8{ "5", "7", "7", null, "7", null, null, null } },
        .{ "DAYOFYEAR(s)", [_]?[]const u8{ "1", "31", "31", null, "31", null, null, null } },
        .{ "WEEK(s, 1)", [_]?[]const u8{ "1", "5", "5", null, "5", null, null, null } },
        .{ "YEARWEEK(s)", [_]?[]const u8{ "202552", "202604", "202604", null, "202604", null, null, null } },
        .{ "WEEKOFYEAR(s)", [_]?[]const u8{ "1", "5", "5", null, "5", null, null, null } },
        .{ "LAST_DAY(s)", [_]?[]const u8{ "2026-01-31", "2026-01-31", "2026-01-31", null, "2026-01-31", null, null, null } },
        .{ "DAYNAME(s)", [_]?[]const u8{ "Thursday", "Saturday", "Saturday", null, "Saturday", null, null, null } },
        .{ "MONTHNAME(s)", [_]?[]const u8{ "January", "January", "January", null, "January", null, null, null } },
        .{ "TO_DAYS(s)", [_]?[]const u8{ "739982", "740012", "740012", null, "740012", null, null, null } },
        .{ "DATEDIFF(s, '2026-01-01')", [_]?[]const u8{ "0", "30", "30", null, "30", null, null, null } },
        .{ "DATEDIFF('2026-03-01', s)", [_]?[]const u8{ "59", "29", "29", null, "29", null, null, null } },
        .{ "DATE_DIFF('hour', s, '2026-01-01')", [_]?[]const u8{ "10", "720", "730", null, "720", null, null, null } },
        .{ "TIMESTAMPDIFF(HOUR, '2026-01-01', s)", [_]?[]const u8{ "10", "720", "730", null, "720", null, null, null } },
        .{ "TIMESTAMPDIFF(MONTH, s, '2026-03-01')", [_]?[]const u8{ "1", "1", "1", null, "1", null, null, null } },
        .{ "MONTHS_DIFF(s, '2025-11-15')", [_]?[]const u8{ "1", "2", "2", null, "2", null, null, null } },
        .{ "DATE_FORMAT(s, '%Y-%m-%d %H:%i')", [_]?[]const u8{ "2026-01-01 10:30", "2026-01-31 00:00", "2026-01-31 10:00", null, "2026-01-31 00:00", null, null, null } },
        .{ "DATE_TRUNC('month', s)", [_]?[]const u8{ "2026-01-01 00:00:00", "2026-01-01 00:00:00", "2026-01-01 00:00:00", null, "2026-01-01 00:00:00", null, null, null } },
        .{ "UNIX_TIMESTAMP(s)", [_]?[]const u8{ "1767263400", "1769817600", "1769853600", null, "1769817600", null, null, null } },
    };
    inline for (cases) |c| {
        const want: [8]?[]const u8 = c[1];
        try expectStrings(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS CHAR) FROM s ORDER BY id", &want);
    }

    const in_2026 = try collectBigints(allocator, db, "SELECT id FROM s WHERE YEAR(s) = 2026 ORDER BY id");
    defer allocator.free(in_2026);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 5 }, in_2026);

    // Any text expression reads so, beside literals of either kind; where
    // text fits the call, it stays text.
    const expr = "IF(id = 1, '2026-01-01 10:30:00', NULL)";
    const later = "IF(id = 1, '2026-01-02 01:00:00', NULL)";
    const expr_cases = .{
        .{ "DATE_ADD(" ++ expr ++ ", INTERVAL 1 DAY)", "2026-01-02 10:30:00" },
        .{ "DATE_ADD(CONCAT('2026-01-', id), INTERVAL 1 DAY)", "2026-01-02 00:00:00" },
        .{ "DATEDIFF(" ++ later ++ ", " ++ expr ++ ")", "1" },
        .{ "DATEDIFF(" ++ later ++ ", '2026-01-01')", "1" },
        .{ "DATEDIFF(" ++ later ++ ", DATE '2026-01-01')", "1" },
        .{ "DATE_DIFF('hour', " ++ later ++ ", " ++ expr ++ ")", "14" },
        .{ "TIMESTAMPDIFF(MINUTE, " ++ expr ++ ", " ++ later ++ ")", "870" },
        .{ "DATE_TRUNC('hour', " ++ expr ++ ")", "2026-01-01 10:00:00" },
        .{ "TO_DATE(" ++ expr ++ ")", "2026-01-01" },
        .{ "DATE(" ++ expr ++ ")", "2026-01-01" },
        .{ "COALESCE(" ++ expr ++ ", DATE '2026-01-01')", "2026-01-01 10:30:00" },
        .{ "GREATEST(" ++ expr ++ ", DATE '2026-01-01')", "2026-01-01 10:30:00" },
        .{ "IFNULL(" ++ expr ++ ", DATE '2026-01-01')", "2026-01-01 10:30:00" },
    };
    try exec(allocator, db, "CREATE TABLE one (id BIGINT PRIMARY KEY)");
    try exec(allocator, db, "INSERT INTO one VALUES (1)");
    inline for (expr_cases) |c| {
        try expectStrings(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS CHAR) FROM one", &.{c[1]});
    }
}

test "a number reads as CAST(n AS DATETIME) reads it where a date function wants a date or datetime, as in StarRocks (issue #424)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE nums (id BIGINT PRIMARY KEY, n BIGINT)");
    try exec(allocator, db, "INSERT INTO nums VALUES (1, 20260131), (2, 20260131103000), (3, 260131), (4, 2026), " ++
        "(5, 20261301), (6, 20260131240000), (7, -20260131), (8, NULL)");

    // Expected values are StarRocks', one per row of `nums`: YYYYMMDD,
    // YYYYMMDDhhmmss and YYMMDD read, and any other number is NULL.
    const cases = .{
        .{ "DATE_ADD(n, INTERVAL 1 DAY)", [_]?[]const u8{ "2026-02-01 00:00:00", "2026-02-01 10:30:00", "2026-02-01 00:00:00", null, null, null, null, null } },
        .{ "n + INTERVAL 1 MONTH", [_]?[]const u8{ "2026-02-28 00:00:00", "2026-02-28 10:30:00", "2026-02-28 00:00:00", null, null, null, null, null } },
        .{ "DATE_SUB(n, INTERVAL 1 HOUR)", [_]?[]const u8{ "2026-01-30 23:00:00", "2026-01-31 09:30:00", "2026-01-30 23:00:00", null, null, null, null, null } },
        .{ "TIMESTAMPADD(MINUTE, 90, n)", [_]?[]const u8{ "2026-01-31 01:30:00", "2026-01-31 12:00:00", "2026-01-31 01:30:00", null, null, null, null, null } },
        .{ "YEAR(n)", [_]?[]const u8{ "2026", "2026", "2026", null, null, null, null, null } },
        .{ "MONTH(n)", [_]?[]const u8{ "1", "1", "1", null, null, null, null, null } },
        .{ "DAY(n)", [_]?[]const u8{ "31", "31", "31", null, null, null, null, null } },
        .{ "QUARTER(n)", [_]?[]const u8{ "1", "1", "1", null, null, null, null, null } },
        .{ "DAYOFWEEK(n)", [_]?[]const u8{ "7", "7", "7", null, null, null, null, null } },
        .{ "DAYOFYEAR(n)", [_]?[]const u8{ "31", "31", "31", null, null, null, null, null } },
        .{ "WEEK(n, 1)", [_]?[]const u8{ "5", "5", "5", null, null, null, null, null } },
        .{ "YEARWEEK(n)", [_]?[]const u8{ "202604", "202604", "202604", null, null, null, null, null } },
        .{ "WEEKOFYEAR(n)", [_]?[]const u8{ "5", "5", "5", null, null, null, null, null } },
        .{ "LAST_DAY(n)", [_]?[]const u8{ "2026-01-31", "2026-01-31", "2026-01-31", null, null, null, null, null } },
        .{ "DAYNAME(n)", [_]?[]const u8{ "Saturday", "Saturday", "Saturday", null, null, null, null, null } },
        .{ "MONTHNAME(n)", [_]?[]const u8{ "January", "January", "January", null, null, null, null, null } },
        .{ "TO_DAYS(n)", [_]?[]const u8{ "740012", "740012", "740012", null, null, null, null, null } },
        .{ "DATEDIFF(n, '2026-01-01')", [_]?[]const u8{ "30", "30", "30", null, null, null, null, null } },
        .{ "DATEDIFF(20260301, n)", [_]?[]const u8{ "29", "29", "29", null, null, null, null, null } },
        .{ "DATE_DIFF('hour', n, '2026-01-01')", [_]?[]const u8{ "720", "730", "720", null, null, null, null, null } },
        .{ "TIMESTAMPDIFF(HOUR, '2026-01-01', n)", [_]?[]const u8{ "720", "730", "720", null, null, null, null, null } },
        .{ "MONTHS_DIFF(n, 20251115)", [_]?[]const u8{ "2", "2", "2", null, null, null, null, null } },
        .{ "DAYS_DIFF(n, DATE '2026-01-01')", [_]?[]const u8{ "30", "30", "30", null, null, null, null, null } },
        .{ "DATE_FORMAT(n, '%Y-%m-%d %H:%i')", [_]?[]const u8{ "2026-01-31 00:00", "2026-01-31 10:30", "2026-01-31 00:00", null, null, null, null, null } },
        .{ "DATE_TRUNC('month', n)", [_]?[]const u8{ "2026-01-01 00:00:00", "2026-01-01 00:00:00", "2026-01-01 00:00:00", null, null, null, null, null } },
        .{ "UNIX_TIMESTAMP(n)", [_]?[]const u8{ "1769817600", "1769855400", "1769817600", null, null, null, null, null } },
    };
    inline for (cases) |c| {
        const want: [8]?[]const u8 = c[1];
        try expectStrings(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS CHAR) FROM nums ORDER BY id", &want);
    }

    const in_2026 = try collectBigints(allocator, db, "SELECT id FROM nums WHERE YEAR(n) = 2026 ORDER BY id");
    defer allocator.free(in_2026);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, in_2026);

    // Every integer width, a float, a boolean, a literal and any other
    // expression reads so. StarRocks takes no decimal here; thinDB reads
    // one as CAST does.
    try exec(allocator, db, "CREATE TABLE typed (id BIGINT PRIMARY KEY, i INT, si SMALLINT, x DOUBLE, f FLOAT, b BOOLEAN, d DECIMAL(16,1))");
    try exec(allocator, db, "INSERT INTO typed VALUES (1, 20260131, 101, 20260131103000.9, 20260131, true, 20260131.7)");
    const typed_cases = .{
        .{ "YEAR(i)", "2026" },
        .{ "YEAR(si)", "2000" },
        .{ "DATE_FORMAT(x, '%Y-%m-%d %H:%i:%s')", "2026-01-31 10:30:00" },
        .{ "YEAR(f)", null },
        .{ "YEAR(b)", null },
        .{ "YEAR(d)", "2026" },
        .{ "YEAR(20260131.7)", "2026" },
        .{ "DATE_FORMAT(20260131103000.9, '%Y-%m-%d %H:%i:%s')", "2026-01-31 10:30:00" },
        .{ "YEAR(id + 20260130)", "2026" },
        .{ "YEAR(20260131)", "2026" },
        .{ "MONTH(260131)", "1" },
        .{ "YEAR(2026)", null },
        .{ "DATE_ADD(20260131, INTERVAL 1 DAY)", "2026-02-01 00:00:00" },
        .{ "MONTHS_DIFF(20260131, 20251231)", "1" },
        .{ "DATEDIFF(20260131, '2026-01-01')", "30" },
        .{ "DATEDIFF(20260131, DATE '2026-01-01')", "30" },
        .{ "DATEDIFF(20260131, CONCAT('2026-01-0', id))", "30" },
        .{ "DATEDIFF(20260131, NULL)", null },
        .{ "DATE_FORMAT(20260131103000, '%Y-%m-%d %H:%i:%s')", "2026-01-31 10:30:00" },
    };
    inline for (typed_cases) |c| {
        try expectStrings(allocator, db, "SELECT CAST(" ++ c[0] ++ " AS CHAR) FROM typed", &.{c[1]});
    }

    // A function that returns one of its arguments meets a number and a
    // date at their common type, which StarRocks makes the number (#430),
    // so the number isn't read as a date there.
    try std.testing.expectError(error.ComputeNoSuchOverload, helpers.collectStrings(allocator, db, "SELECT COALESCE(i, DATE '2026-01-01') FROM typed"));
    try std.testing.expectError(error.ComputeNoSuchOverload, helpers.collectStrings(allocator, db, "SELECT GREATEST(i, DATE '2026-01-01') FROM typed"));
}
