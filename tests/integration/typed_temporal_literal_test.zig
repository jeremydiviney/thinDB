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
    try exec(allocator, db,
        "CREATE TABLE shipments (id BIGINT PRIMARY KEY, ship_date DATE NOT NULL)",
    );
    try exec(allocator, db,
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

    const ids = try collectBigints(allocator, db,
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

    const ids = try collectBigints(allocator, db,
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

    const ids = try collectBigints(allocator, db,
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

    try exec(allocator, db,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, ts DATETIME NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO events (id, ts) VALUES (1, '2024-01-15 09:30:00'), (2, '2024-01-15 18:45:00')",
    );
    const t = try db.openTable("events", .{});
    try t.flush();

    const ids = try collectBigints(allocator, db,
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

    try exec(allocator, db,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, ts DATETIME NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO events (id, ts) VALUES (1, '2024-01-15 09:30:00')",
    );
    const t = try db.openTable("events", .{});
    try t.flush();

    const ids = try collectBigints(allocator, db,
        "SELECT id FROM events WHERE ts = TIMESTAMP '2024-01-15 09:30:00'",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{1}, ids);
}
