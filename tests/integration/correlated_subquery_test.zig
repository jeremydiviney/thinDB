//! Correlated EXISTS / NOT EXISTS / IN / NOT IN — Tier 3a.
//!
//! thinDB decorrelates via "materialize then filter" (Approach B): the
//! pre-compile pass detects correlation in the inner's WHERE clause,
//! rewrites the inner to project the correlation keys, drains the
//! rewritten inner into a tuple set, and replaces the predicate with
//! a per-row tuple lookup. Equi-correlations only; inner shape is
//! Filter(AND-conjunction, Scan(T)).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE orders (o_id BIGINT PRIMARY KEY, o_total INT NOT NULL)");
    try exec(allocator, db, "CREATE TABLE lineitem (l_id BIGINT PRIMARY KEY, l_orderid BIGINT NOT NULL, l_qty INT NOT NULL)");
    try exec(
        allocator,
        db,
        "INSERT INTO orders (o_id, o_total) VALUES (1, 100), (2, 200), (3, 300), (4, 400)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO lineitem (l_id, l_orderid, l_qty) VALUES (1, 1, 5), (2, 1, 10), (3, 2, 50), (4, 4, 200)",
    );
    const t1 = try db.openTable("orders", .{});
    try t1.flush();
    const t2 = try db.openTable("lineitem", .{});
    try t2.flush();
    return db;
}

test "correlated EXISTS: orders with any line item" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // orders 1, 2, 4 have line items; 3 does not.
    const ids = try collectBigints(allocator, db,
        \\SELECT o_id FROM orders
        \\WHERE EXISTS (SELECT l_id FROM lineitem WHERE l_orderid = o_id)
        \\ORDER BY o_id ASC
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 4 }, ids);
}

test "correlated NOT EXISTS: orders with no line items" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        \\SELECT o_id FROM orders
        \\WHERE NOT EXISTS (SELECT l_id FROM lineitem WHERE l_orderid = o_id)
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{3}, ids);
}

test "correlated EXISTS: with extra inner predicate" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Only orders where SOME line item has qty > 30 → order 2 (qty=50)
    // and order 4 (qty=200) qualify.
    const ids = try collectBigints(allocator, db,
        \\SELECT o_id FROM orders
        \\WHERE EXISTS (
        \\  SELECT l_id FROM lineitem
        \\  WHERE l_orderid = o_id AND l_qty > 30
        \\)
        \\ORDER BY o_id ASC
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 4 }, ids);
}

test "correlated IN: order ids that have a line item with qty=10" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Find orders whose o_id is referenced by some lineitem with qty=10.
    // lineitem (l_orderid, l_qty): (1,5), (1,10), (2,50), (4,200). qty=10 → orderid 1.
    const ids = try collectBigints(allocator, db,
        \\SELECT o_id FROM orders
        \\WHERE o_id IN (SELECT l_orderid FROM lineitem WHERE l_qty = 10)
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{1}, ids);
}

test "correlated NOT IN: orders whose id isn't referenced by any line item over 100" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // lineitem with qty>100: only (l_orderid=4, qty=200). So orders
    // whose id is NOT in {4} → 1, 2, 3.
    const ids = try collectBigints(allocator, db,
        \\SELECT o_id FROM orders
        \\WHERE o_id NOT IN (SELECT l_orderid FROM lineitem WHERE l_qty > 100)
        \\ORDER BY o_id ASC
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, ids);
}

test "correlated EXISTS: multi-key correlation" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(
        allocator,
        db,
        "CREATE TABLE ps (ps_part BIGINT NOT NULL, ps_supp BIGINT NOT NULL, PRIMARY KEY (ps_part, ps_supp))",
    );
    try exec(
        allocator,
        db,
        "CREATE TABLE li (li_id BIGINT PRIMARY KEY, li_part BIGINT NOT NULL, li_supp BIGINT NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO ps (ps_part, ps_supp) VALUES (1, 10), (1, 20), (2, 10), (3, 30)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO li (li_id, li_part, li_supp) VALUES (1, 1, 10), (2, 2, 10), (3, 3, 99)",
    );
    const t1 = try db.openTable("ps", .{});
    try t1.flush();
    const t2 = try db.openTable("li", .{});
    try t2.flush();

    // partsupp rows that have a matching lineitem on BOTH part and supp:
    //   (1, 10) → li has (1, 10) ✓
    //   (1, 20) → li has no (1, 20) ✗
    //   (2, 10) → li has (2, 10) ✓
    //   (3, 30) → li has (3, 99), no (3, 30) ✗
    // → (1,10), (2,10). Test by counting matches via a projection.
    var q = try runSql(allocator, db,
        \\SELECT COUNT(ps_part) AS n FROM ps
        \\WHERE EXISTS (
        \\  SELECT li_id FROM li
        \\  WHERE li_part = ps_part AND li_supp = ps_supp
        \\)
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 2), batch.values[0].data.bigint[0]);
}

test "correlated subqueries: SELECT 1 / SELECT * and refs qualified by the inner table's name" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT o_id FROM orders WHERE EXISTS (SELECT 1 FROM lineitem WHERE l_orderid = o_id) ORDER BY o_id", &[_]i64{ 1, 2, 4 } },
        .{ "SELECT x.o_id FROM orders x WHERE EXISTS (SELECT 1 FROM lineitem AS p WHERE p.l_orderid = x.o_id) ORDER BY x.o_id", &[_]i64{ 1, 2, 4 } },
        .{ "SELECT o_id FROM orders WHERE EXISTS (SELECT * FROM lineitem WHERE lineitem.l_orderid = orders.o_id) ORDER BY o_id", &[_]i64{ 1, 2, 4 } },
        .{ "SELECT x.o_id FROM orders x WHERE EXISTS (SELECT lineitem.l_id FROM lineitem WHERE lineitem.l_orderid = x.o_id) ORDER BY x.o_id", &[_]i64{ 1, 2, 4 } },
        .{ "SELECT o_id FROM orders WHERE EXISTS (SELECT 1 FROM LineItem WHERE LineItem.l_orderid = orders.o_id) ORDER BY o_id", &[_]i64{ 1, 2, 4 } },
        .{ "SELECT orders.o_id FROM orders WHERE NOT EXISTS (SELECT 1 FROM lineitem WHERE lineitem.l_orderid = orders.o_id)", &[_]i64{3} },
        .{ "SELECT o_id FROM orders WHERE EXISTS (SELECT 1 FROM lineitem WHERE lineitem.l_orderid = orders.o_id AND lineitem.l_qty >= 50) ORDER BY o_id", &[_]i64{ 2, 4 } },
        .{ "SELECT o_id FROM orders WHERE EXISTS (SELECT 1 FROM lineitem WHERE lineitem.l_qty > orders.o_total) ORDER BY o_id", &[_]i64{1} },
        .{ "SELECT o_id FROM orders WHERE EXISTS (SELECT 1 FROM lineitem WHERE l_qty > 100) ORDER BY o_id", &[_]i64{ 1, 2, 3, 4 } },
        .{ "SELECT o_id FROM orders WHERE EXISTS (SELECT 1 FROM lineitem WHERE l_qty > 1000) ORDER BY o_id", &[_]i64{} },
        .{ "SELECT o_id FROM orders WHERE o_id IN (SELECT l_orderid FROM lineitem WHERE l_id = o_id) ORDER BY o_id", &[_]i64{ 1, 4 } },
        .{ "SELECT x.o_id FROM orders x WHERE x.o_id IN (SELECT p.l_orderid FROM lineitem p WHERE p.l_id = x.o_id) ORDER BY x.o_id", &[_]i64{ 1, 4 } },
        .{ "SELECT o_id FROM orders WHERE o_id IN (SELECT l_orderid FROM lineitem WHERE lineitem.l_id = orders.o_id) ORDER BY o_id", &[_]i64{ 1, 4 } },
        .{ "SELECT o_id FROM orders WHERE o_id IN (SELECT lineitem.l_orderid FROM lineitem WHERE lineitem.l_id = orders.o_id) ORDER BY o_id", &[_]i64{ 1, 4 } },
        .{ "SELECT o_id FROM orders WHERE o_id NOT IN (SELECT l_orderid FROM lineitem WHERE l_id = o_id) ORDER BY o_id", &[_]i64{ 2, 3 } },
        .{ "SELECT CASE WHEN EXISTS (SELECT 1 FROM lineitem WHERE lineitem.l_orderid = orders.o_id) THEN o_id ELSE 0 END AS f FROM orders ORDER BY o_id", &[_]i64{ 1, 2, 0, 4 } },
        .{ "SELECT o_id FROM orders WHERE o_id = (SELECT MIN(lineitem.l_id) FROM lineitem WHERE lineitem.l_orderid = orders.o_id) ORDER BY o_id", &[_]i64{ 1, 4 } },
    };
    inline for (cases) |case| {
        const got = try collectBigints(allocator, db, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, case[1], got);
    }
}
