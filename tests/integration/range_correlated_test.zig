//! Range-correlated EXISTS / NOT EXISTS — the inner WHERE includes
//! a `inner.x op outer.y` conjunct with `op` ∈ {<, <=, >, >=}.
//!
//! Implementation strategy: the resolver materializes the inner once
//! (after applying all non-correlated filters), buckets rows by any
//! equi-correlation keys, and stores each bucket's range-column
//! values sorted with min/max cached. Per outer row evaluation
//! collapses to a single min/max compare for open-ended ops.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db,
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, threshold INT NOT NULL, region VARCHAR(8) NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO orders (id, threshold, region) VALUES " ++
            "(1, 50, 'east'), (2, 100, 'east'), (3, 200, 'west'), (4, 500, 'west')",
    );
    try exec(allocator, db,
        "CREATE TABLE payments (id BIGINT PRIMARY KEY, amount INT NOT NULL, region VARCHAR(8) NOT NULL)",
    );
    try exec(allocator, db,
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
    const ids = try collectBigints(allocator, db,
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
    const ids = try collectBigints(allocator, db,
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
    const ids = try collectBigints(allocator, db,
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
    const ids = try collectBigints(allocator, db,
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
    try exec(allocator, db,
        "CREATE TABLE customers (id BIGINT PRIMARY KEY, sub_start INT NOT NULL, sub_end INT NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO customers (id, sub_start, sub_end) VALUES " ++
            "(1, 100, 200), (2, 300, 400), (3, 500, 600)",
    );
    try exec(allocator, db,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, ts INT NOT NULL)",
    );
    try exec(allocator, db,
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
    const ids = try collectBigints(allocator, db,
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

    try exec(allocator, db,
        "CREATE TABLE customers (id BIGINT PRIMARY KEY, sub_start INT NOT NULL, sub_end INT NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO customers (id, sub_start, sub_end) VALUES " ++
            "(1, 100, 200), (2, 300, 400), (3, 500, 600)",
    );
    try exec(allocator, db,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, ts INT NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO events (id, ts) VALUES (1, 150), (2, 350), (3, 700)",
    );
    const t1 = try db.openTable("customers", .{});
    try t1.flush();
    const t2 = try db.openTable("events", .{});
    try t2.flush();

    // Customers with NO event in their window: only c3.
    const ids = try collectBigints(allocator, db,
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

    try exec(allocator, db,
        "CREATE TABLE customers (id BIGINT PRIMARY KEY, sub_start INT NOT NULL, sub_end INT NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO customers (id, sub_start, sub_end) VALUES " ++
            "(1, 100, 200), (2, 300, 400), (3, 500, 600)",
    );
    try exec(allocator, db,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, ts INT NOT NULL)",
    );
    // c1 has an event at 150 (in window). c2 has an event at 500 (NOT in c2's
    // window 300-400; it's in c3's window but the user_id is 2). c3 has none.
    try exec(allocator, db,
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
    const ids = try collectBigints(allocator, db,
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
    const ids = try collectBigints(allocator, db,
        "SELECT o.id FROM orders AS o " ++
            "WHERE EXISTS (SELECT p.id FROM payments AS p WHERE p.id >= 2 AND p.amount > o.threshold) " ++
            "ORDER BY o.id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, ids);
}
