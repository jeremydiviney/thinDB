//! Range-correlated EXISTS / NOT EXISTS — the inner WHERE includes
//! a `inner.x op outer.y` conjunct with `op` ∈ {<, <=, >, >=}.
//!
//! Implementation strategy: the resolver materializes the inner once
//! (after applying all non-correlated filters), buckets rows by any
//! equi-correlation keys, and stores each bucket's range-column
//! values sorted with min/max cached. Per outer row evaluation
//! collapses to a single min/max compare for open-ended ops, made under
//! the comparison rule, so the inner and outer columns needn't share a
//! type.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(
        allocator,
        db,
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, threshold INT NOT NULL, region VARCHAR(8) NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO orders (id, threshold, region) VALUES " ++
            "(1, 50, 'east'), (2, 100, 'east'), (3, 200, 'west'), (4, 500, 'west')",
    );
    try exec(
        allocator,
        db,
        "CREATE TABLE payments (id BIGINT PRIMARY KEY, amount INT NOT NULL, region VARCHAR(8) NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO payments (id, amount, region) VALUES " ++
            "(1, 75, 'east'), (2, 150, 'east'), (3, 250, 'west')",
    );
    const t1 = try db.openTable("orders", .{});
    try t1.flush();
    const t2 = try db.openTable("payments", .{});
    try t2.flush();
    return db;
}

test "range EXISTS: amount > o.threshold (any payment beats this order's threshold)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Inner: SELECT p.id FROM payments p WHERE p.amount > o.threshold.
    // payments.amount values: {75, 150, 250}, max = 250.
    // Orders with threshold < 250: ids 1 (50), 2 (100), 3 (200).
    const ids = try collectBigints(
        allocator,
        db,
        "SELECT o.id FROM orders AS o " ++
            "WHERE EXISTS (SELECT p.id FROM payments AS p WHERE p.amount > o.threshold) " ++
            "ORDER BY o.id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, ids);
}

test "range NOT EXISTS: amount > o.threshold (no payment beats threshold)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // NOT EXISTS — orders where no payment beats their threshold.
    // max(amount) = 250. Order with threshold >= 250: id 4 (500).
    const ids = try collectBigints(
        allocator,
        db,
        "SELECT o.id FROM orders AS o " ++
            "WHERE NOT EXISTS (SELECT p.id FROM payments AS p WHERE p.amount > o.threshold) " ++
            "ORDER BY o.id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{4}, ids);
}

test "range EXISTS: <= op (some payment <= threshold)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // min(amount) = 75. Orders with threshold >= 75: ids 1 (50? no),
    // 2 (100), 3 (200), 4 (500). Wait, 50 < 75, so id=1 fails.
    const ids = try collectBigints(
        allocator,
        db,
        "SELECT o.id FROM orders AS o " ++
            "WHERE EXISTS (SELECT p.id FROM payments AS p WHERE p.amount <= o.threshold) " ++
            "ORDER BY o.id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3, 4 }, ids);
}

test "range EXISTS: mixed equi + range correlation" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Inner restricted to same region AND payment.amount > threshold.
    // east payments: 75, 150. west payments: 250.
    // east orders (id 1, 2) thresholds (50, 100): both < 150 → both match.
    // west orders (id 3, 4) thresholds (200, 500): 200 < 250 → id 3 matches; 500 >= 250 → id 4 no.
    const ids = try collectBigints(
        allocator,
        db,
        "SELECT o.id FROM orders AS o " ++
            "WHERE EXISTS (SELECT p.id FROM payments AS p WHERE p.region = o.region AND p.amount > o.threshold) " ++
            "ORDER BY o.id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, ids);
}

test "range EXISTS: BETWEEN-against-outer (closed range)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // Customers with subscription windows; events have timestamps.
    // Question: which customers had any event during their subscription window?
    try exec(
        allocator,
        db,
        "CREATE TABLE customers (id BIGINT PRIMARY KEY, sub_start INT NOT NULL, sub_end INT NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO customers (id, sub_start, sub_end) VALUES " ++
            "(1, 100, 200), (2, 300, 400), (3, 500, 600)",
    );
    try exec(
        allocator,
        db,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, ts INT NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO events (id, ts) VALUES (1, 150), (2, 350), (3, 700)",
    );
    const t1 = try db.openTable("customers", .{});
    try t1.flush();
    const t2 = try db.openTable("events", .{});
    try t2.flush();

    // EXISTS event with ts between c.sub_start and c.sub_end.
    // c1 (100-200) → event at 150 matches.
    // c2 (300-400) → event at 350 matches.
    // c3 (500-600) → no event in window.
    const ids = try collectBigints(
        allocator,
        db,
        "SELECT c.id FROM customers AS c " ++
            "WHERE EXISTS (SELECT e.id FROM events AS e WHERE e.ts >= c.sub_start AND e.ts <= c.sub_end) " ++
            "ORDER BY c.id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, ids);
}

test "range NOT EXISTS: BETWEEN-against-outer (closed range)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(
        allocator,
        db,
        "CREATE TABLE customers (id BIGINT PRIMARY KEY, sub_start INT NOT NULL, sub_end INT NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO customers (id, sub_start, sub_end) VALUES " ++
            "(1, 100, 200), (2, 300, 400), (3, 500, 600)",
    );
    try exec(
        allocator,
        db,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, ts INT NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO events (id, ts) VALUES (1, 150), (2, 350), (3, 700)",
    );
    const t1 = try db.openTable("customers", .{});
    try t1.flush();
    const t2 = try db.openTable("events", .{});
    try t2.flush();

    // Customers with NO event in their window: only c3.
    const ids = try collectBigints(
        allocator,
        db,
        "SELECT c.id FROM customers AS c " ++
            "WHERE NOT EXISTS (SELECT e.id FROM events AS e WHERE e.ts >= c.sub_start AND e.ts <= c.sub_end) " ++
            "ORDER BY c.id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{3}, ids);
}

test "range EXISTS: equi + closed range (per-user date window)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(
        allocator,
        db,
        "CREATE TABLE customers (id BIGINT PRIMARY KEY, sub_start INT NOT NULL, sub_end INT NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO customers (id, sub_start, sub_end) VALUES " ++
            "(1, 100, 200), (2, 300, 400), (3, 500, 600)",
    );
    try exec(
        allocator,
        db,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, ts INT NOT NULL)",
    );
    // c1 has an event at 150 (in window). c2 has an event at 500 (NOT in c2's
    // window 300-400; it's in c3's window but the user_id is 2). c3 has none.
    try exec(
        allocator,
        db,
        "INSERT INTO events (id, user_id, ts) VALUES (1, 1, 150), (2, 2, 500), (3, 1, 350)",
    );
    const t1 = try db.openTable("customers", .{});
    try t1.flush();
    const t2 = try db.openTable("events", .{});
    try t2.flush();

    // For each customer, EXISTS an event whose user_id matches AND ts is in window.
    // c1: event (id=1, user_id=1, ts=150) in window [100,200] → match.
    // c2: event (id=2, user_id=2, ts=500) NOT in window [300,400] → no.
    // c3: no events for user_id=3 → no.
    const ids = try collectBigints(
        allocator,
        db,
        "SELECT c.id FROM customers AS c " ++
            "WHERE EXISTS (SELECT e.id FROM events AS e WHERE e.user_id = c.id " ++
            "AND e.ts >= c.sub_start AND e.ts <= c.sub_end) " ++
            "ORDER BY c.id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{1}, ids);
}

test "range EXISTS: with inner-local filter applied before materialization" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Restrict the inner to payments.id >= 2 (drops the 75-amount row).
    // Effective inner amounts: {150, 250}, max = 250.
    // Same as the first test but with max bumped from 75-relevant to 150.
    // Orders with threshold < 250: ids 1, 2, 3.
    const ids = try collectBigints(
        allocator,
        db,
        "SELECT o.id FROM orders AS o " ++
            "WHERE EXISTS (SELECT p.id FROM payments AS p WHERE p.id >= 2 AND p.amount > o.threshold) " ++
            "ORDER BY o.id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, ids);
}

test "range EXISTS / IN: the inner range column compares by value with an outer column of another type" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE mx_a (id BIGINT PRIMARY KEY, lo INT, hi BIGINT, d DATE, dc DECIMAL(10,2), f DOUBLE)");
    try exec(allocator, db, "CREATE TABLE mx_b (id BIGINT PRIMARY KEY, n BIGINT, m DECIMAL(12,3), dt DATETIME, g DOUBLE, s SMALLINT, t VARCHAR(4))");
    try exec(allocator, db, "INSERT INTO mx_a VALUES (1, 5, 12, '2024-01-02', 5.50, 1.5), (2, 20, 25, '2024-03-01', 20.00, 30.0), " ++
        "(3, NULL, 40, '2023-12-31', NULL, NULL), (4, 0, 3, '2024-02-15', 0.01, 0.0), (5, 12, 12, '2024-01-01', 12.25, 12.25), (6, 7, 9, NULL, 7.10, 9.5)");
    try exec(allocator, db, "INSERT INTO mx_b VALUES (1, 3, 5.500, '2024-01-01 12:00:00', 1.5, 2, '9'), (2, 12, 12.250, '2024-01-02 00:00:00', 12.25, 12, '12'), " ++
        "(3, 8, 7.105, '2024-02-15 23:59:59', 9.5, 8, '100'), (4, NULL, NULL, NULL, NULL, NULL, NULL), (5, 18, 19.999, '2023-12-31 00:00:00', 30.0, 18, '18')");
    const t1 = try db.openTable("mx_a", .{});
    try t1.flush();
    const t2 = try db.openTable("mx_b", .{});
    try t2.flush();

    // Rows are DuckDB's, except the VARCHAR-against-INT cases, which DuckDB
    // refuses to compare; those follow the numeric reading of the text.
    const cases = .{
        .{ "WHERE EXISTS (SELECT 1 FROM mx_b b WHERE b.n > a.lo)", &[_]i64{ 1, 4, 5, 6 } },
        .{ "WHERE EXISTS (SELECT 1 FROM mx_b b WHERE b.n < a.lo)", &[_]i64{ 1, 2, 5, 6 } },
        .{ "WHERE NOT EXISTS (SELECT 1 FROM mx_b b WHERE b.n >= a.lo)", &[_]i64{ 2, 3 } },
        .{ "WHERE EXISTS (SELECT 1 FROM mx_b b WHERE b.m <= a.dc)", &[_]i64{ 1, 2, 5, 6 } },
        .{ "WHERE EXISTS (SELECT 1 FROM mx_b b WHERE b.n >= a.lo AND b.n < a.hi)", &[_]i64{ 1, 6 } },
        .{ "WHERE EXISTS (SELECT 1 FROM mx_b b WHERE b.m BETWEEN a.lo AND a.hi)", &[_]i64{ 1, 6 } },
        .{ "WHERE NOT EXISTS (SELECT 1 FROM mx_b b WHERE b.m > a.lo AND b.m <= a.hi)", &[_]i64{ 2, 3, 4, 5 } },
        .{ "WHERE EXISTS (SELECT 1 FROM mx_b b WHERE b.dt < a.d)", &[_]i64{ 1, 2, 4, 5 } },
        .{ "WHERE EXISTS (SELECT 1 FROM mx_b b WHERE b.dt >= a.d)", &[_]i64{ 1, 3, 4, 5 } },
        .{ "WHERE EXISTS (SELECT 1 FROM mx_b b WHERE b.g > a.dc)", &[_]i64{ 1, 2, 4, 5, 6 } },
        .{ "WHERE EXISTS (SELECT 1 FROM mx_b b WHERE b.m > a.f)", &[_]i64{ 1, 4, 5, 6 } },
        .{ "WHERE EXISTS (SELECT 1 FROM mx_b b WHERE b.id = a.id AND b.s <= a.lo)", &[_]i64{ 1, 2 } },
        .{ "WHERE a.lo IN (SELECT b.s FROM mx_b b WHERE b.n <= a.hi)", &[_]i64{5} },
        .{ "WHERE a.lo NOT IN (SELECT b.s FROM mx_b b WHERE b.n <= a.hi AND b.s IS NOT NULL)", &[_]i64{ 1, 2, 4, 6 } },
        .{ "WHERE EXISTS (SELECT 1 FROM mx_b b WHERE b.t > a.lo)", &[_]i64{ 1, 2, 4, 5, 6 } },
        .{ "WHERE EXISTS (SELECT 1 FROM mx_b b WHERE b.t < a.lo)", &[_]i64{ 2, 5 } },
        .{ "WHERE EXISTS (SELECT 1 FROM mx_b b WHERE b.t >= a.lo AND b.t < a.hi)", &[_]i64{1} },
    };
    inline for (cases) |case| {
        const ids = try collectBigints(allocator, db, "SELECT a.id FROM mx_a a " ++ case[0] ++ " ORDER BY a.id");
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(i64, case[1], ids);
    }
}
