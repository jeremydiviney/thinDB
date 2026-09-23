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
