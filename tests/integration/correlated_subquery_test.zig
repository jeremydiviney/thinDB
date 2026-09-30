//! Correlated EXISTS / NOT EXISTS / IN / NOT IN — Tier 3a.
//!
//! thinDB decorrelates via "materialize then filter" (Approach B): the
//! pre-compile pass detects correlation in the inner's WHERE clause,
//! rewrites the inner to project the correlation keys, drains the
//! rewritten inner into a tuple set, and replaces the predicate with
//! a per-row tuple lookup. The inner FROM may be a table, CTE, view or
//! derived table, and a column reference binds in the innermost query
//! whose FROM has it, as SQL scopes names.
//! A subquery correlated any other way joins its domain, the distinct
//! outer values it reads (DESIGN.md §6.7).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;
const collectIntCells = helpers.collectIntCells;

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

fn setupScopes(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE ex_t (id BIGINT PRIMARY KEY, v INT, k INT)");
    try exec(allocator, db, "CREATE TABLE ex_u (id BIGINT PRIMARY KEY, v INT, w VARCHAR(4))");
    try exec(allocator, db, "INSERT INTO ex_t VALUES (1, 5, 10), (2, 2, 20), (3, NULL, 30), (4, 7, 40), (5, 2, 50)");
    try exec(allocator, db, "INSERT INTO ex_u VALUES (10, 2, 'a'), (20, 7, 'b'), (30, NULL, 'c'), (40, 2, 'd'), (50, 9, 'e')");
    const t1 = try db.openTable("ex_t", .{});
    try t1.flush();
    const t2 = try db.openTable("ex_u", .{});
    try t2.flush();
    try exec(allocator, db, "CREATE VIEW vu AS SELECT id, v, w FROM ex_u");
    return db;
}

fn expectCells(allocator: std.mem.Allocator, db: anytype, sql: []const u8, expected: []const ?i64) !void {
    var q = try helpers.runSqlCtx(allocator, db, sql);
    defer q.deinit();
    const cells = try collectIntCells(allocator, &q);
    defer allocator.free(cells);
    try std.testing.expectEqualSlices(?i64, expected, cells);
}

// Each form names the inner relation `y`; it has an `ex_u` column list the
// outer `ex_t x` shares `id` and `v` with, so a reference bound in the
// wrong scope changes the rows. Expected rows are DuckDB's.
const scope_forms = .{
    .{ "", "ex_u y" },
    .{ "", "(SELECT id, v, w FROM ex_u) y" },
    .{ "WITH y AS (SELECT id, v, w FROM ex_u) ", "y" },
    .{ "WITH c AS (SELECT id, v, w FROM ex_u) ", "c y" },
    .{ "WITH y AS (SELECT d.id, d.v, d.w FROM (SELECT id, v, w FROM ex_u) d) ", "y" },
    .{ "", "vu y" },
};

const scope_queries = .{
    .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ", " WHERE y.v = x.v) ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
    .{ "SELECT x.id FROM ex_t x WHERE NOT EXISTS (SELECT 1 FROM ", " WHERE y.v = x.v) ORDER BY x.id", &[_]?i64{ 1, 3 } },
    .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ", " WHERE v = x.v) ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
    .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ", " WHERE y.id = k AND y.v > 5) ORDER BY x.id", &[_]?i64{ 2, 5 } },
    .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ", " WHERE y.v > x.v) ORDER BY x.id", &[_]?i64{ 1, 2, 4, 5 } },
    .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ", " WHERE y.v = x.v AND y.w <> 'a') ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
    .{ "SELECT x.id FROM ex_t x WHERE x.v IN (SELECT y.v FROM ", " WHERE y.id >= x.k) ORDER BY x.id", &[_]?i64{2} },
    .{ "SELECT x.id FROM ex_t x WHERE x.v NOT IN (SELECT y.v FROM ", " WHERE y.id >= x.k AND y.v IS NOT NULL) ORDER BY x.id", &[_]?i64{ 1, 4, 5 } },
    .{ "SELECT x.id FROM ex_t x WHERE (SELECT COUNT(*) FROM ", " WHERE y.v = x.v) > 0 ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
    .{ "SELECT x.id, (SELECT MAX(y.id) FROM ", " WHERE y.v = x.v) AS m FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, null, 2, 40, 3, null, 4, 20, 5, 40 } },
    .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ex_u z WHERE z.v = x.v AND EXISTS (SELECT 1 FROM ", " WHERE y.id = z.id AND y.w <> 'd')) ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
    .{ "SELECT ex_t.id FROM ex_t WHERE EXISTS (SELECT 1 FROM ", " WHERE y.v = ex_t.v) ORDER BY ex_t.id", &[_]?i64{ 2, 4, 5 } },
    .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT y.v FROM ", " WHERE y.v = x.v GROUP BY y.v) ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
    .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ", " JOIN ex_u z ON z.id = y.id WHERE y.v = x.v AND z.w <> 'a') ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
    .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT DISTINCT y.v FROM ", " WHERE y.v = x.v) ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
    .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ", " WHERE y.v = x.v LIMIT 1) ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
    .{ "SELECT x.id, CASE WHEN NOT EXISTS (SELECT 1 FROM ", " WHERE y.v = x.v) THEN 1 ELSE 0 END AS f FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 1, 2, 0, 3, 1, 4, 0, 5, 0 } },
    .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ", " WHERE y.id = x.k + 10) ORDER BY x.id", &[_]?i64{ 1, 2, 3, 4 } },
    .{ "SELECT x.id FROM ex_t x WHERE NOT EXISTS (SELECT 1 FROM ", " WHERE y.id = x.k + 10 AND y.v IS NOT NULL) ORDER BY x.id", &[_]?i64{ 2, 5 } },
    .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ", " WHERE y.v > x.v + 3) ORDER BY x.id", &[_]?i64{ 1, 2, 5 } },
    .{ "SELECT x.id FROM ex_t x WHERE x.v IN (SELECT y.v FROM ", " WHERE y.id = x.k + 20) ORDER BY x.id", &[_]?i64{2} },
    .{ "SELECT x.id FROM ex_t x WHERE x.v NOT IN (SELECT y.v FROM ", " WHERE y.id >= x.k - 10 AND y.v IS NOT NULL) ORDER BY x.id", &[_]?i64{ 1, 4 } },
    .{ "SELECT x.id, (SELECT MAX(y.v) FROM ", " WHERE y.id = x.k - 10) AS m FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, null, 2, 2, 3, 7, 4, null, 5, 2 } },
    .{ "SELECT x.id, (SELECT y.v FROM ", " WHERE y.id = x.id * 10) AS v FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 2, 2, 7, 3, null, 4, 2, 5, 9 } },
    .{ "SELECT x.id, CASE WHEN EXISTS (SELECT 1 FROM ", " WHERE y.id = x.k + 10) THEN 1 ELSE 0 END AS f FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 1, 2, 1, 3, 1, 4, 1, 5, 0 } }, // Lifted onto the domain of outer values: a reference two levels out,
    // correlation under OR, over both rows or by <>, a range-correlated
    // scalar, and a correlated LIMIT.
    .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ex_u z WHERE z.id > 15 AND EXISTS (SELECT 1 FROM ", " WHERE y.id = z.id AND y.v = x.v)) ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
    .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ", " WHERE y.id > 15 AND EXISTS (SELECT 1 FROM ex_u z WHERE z.id = y.id AND z.v = x.v)) ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
    .{ "SELECT x.id, (SELECT COUNT(*) FROM ", " WHERE y.id = x.k + 10 OR y.id = x.k) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 2, 2, 2, 3, 2, 4, 2, 5, 1 } },
    .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ", " WHERE y.id + x.k = 50) ORDER BY x.id", &[_]?i64{ 1, 2, 3, 4 } },
    .{ "SELECT x.id, (SELECT COUNT(*) FROM ", " WHERE y.id <> x.k AND y.v > x.v) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 2, 2, 1, 3, 0, 4, 1, 5, 1 } },
    .{ "SELECT x.id FROM ex_t x WHERE x.v = (SELECT MIN(y.v) FROM ", " WHERE y.id > x.k) ORDER BY x.id", &[_]?i64{2} },
    .{ "SELECT x.id, (SELECT MAX(y.v) FROM ", " WHERE y.id <= x.k) AS m FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 2, 2, 7, 3, 7, 4, 7, 5, 9 } },
    .{ "SELECT x.id FROM ex_t x WHERE x.v NOT IN (SELECT y.v FROM ", " WHERE y.id <= x.k ORDER BY y.id DESC LIMIT 1) ORDER BY x.id", &[_]?i64{ 1, 2, 4, 5 } },
    .{ "SELECT x.id FROM ex_t x WHERE x.v IN (SELECT y.v FROM ", " WHERE y.id <> x.k ORDER BY y.id LIMIT 2) ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
    .{ "SELECT x.id, (SELECT y.v FROM ", " WHERE y.id <> x.k ORDER BY y.id LIMIT 1 OFFSET 1) AS s FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, null, 2, null, 3, 7, 4, 7, 5, 7 } },
    .{ "SELECT x.id, (SELECT y.v FROM ", " WHERE y.id <> x.k AND y.v = 9) AS s FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 9, 2, 9, 3, 9, 4, 9, 5, null } },
    .{ "SELECT x.id FROM ex_t x WHERE x.id < 3 AND x.v NOT IN (SELECT y.v FROM ", " WHERE y.id <> x.k AND y.v IS NOT NULL) ORDER BY x.id", &[_]?i64{1} },
    .{ "SELECT x.id FROM ex_t x WHERE x.k >= 30 AND x.v < (SELECT MAX(y.v) FROM ", " WHERE y.id <> x.k) ORDER BY x.id", &[_]?i64{ 4, 5 } },
    // A NULL outer value matches no inner row, yet its outer row still
    // finds its domain row: a count of 0, or what the outer-only terms give.
    .{ "SELECT x.id, (SELECT COUNT(*) FROM ", " WHERE y.v = x.v OR y.id < 0) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 0, 2, 2, 3, 0, 4, 1, 5, 2 } },
    .{ "SELECT x.id, (SELECT COUNT(*) FROM ", " WHERE y.id > 25 AND x.v IS NULL) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 0, 2, 0, 3, 3, 4, 0, 5, 0 } },
};

test "correlated subqueries over a CTE, view or derived table bind outer references to the outer query" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupScopes(allocator, std.testing.io, tmp.dir);
    defer db.close();

    inline for (scope_forms) |form| {
        inline for (scope_queries) |query| {
            expectCells(allocator, db, form[0] ++ query[0] ++ form[1] ++ query[1], query[2]) catch |err| {
                std.debug.print("failed: {s}{s}{s}{s}\n", .{ form[0], query[0], form[1], query[1] });
                return err;
            };
        }
    }
}

test "correlated subqueries: an inner relation shadows the outer name, and an unaliased CTE or view qualifies its columns" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupScopes(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ex_u x WHERE x.v = 9) ORDER BY x.id", &[_]?i64{ 1, 2, 3, 4, 5 } },
        .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM (SELECT id, v, w FROM ex_u) x WHERE x.v = 9) ORDER BY x.id", &[_]?i64{ 1, 2, 3, 4, 5 } },
        .{ "WITH y AS (SELECT id, v, w FROM ex_u) SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM y x WHERE x.v = 9) ORDER BY x.id", &[_]?i64{ 1, 2, 3, 4, 5 } },
        .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM vu WHERE vu.v = x.v) ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
        .{ "SELECT x.id FROM ex_t x WHERE NOT EXISTS (SELECT 1 FROM vu WHERE vu.v = x.v) ORDER BY x.id", &[_]?i64{ 1, 3 } },
        .{ "SELECT x.id FROM ex_t x WHERE x.v IN (SELECT vu.v FROM vu WHERE vu.id >= x.k) ORDER BY x.id", &[_]?i64{2} },
        .{ "SELECT x.id, (SELECT MAX(vu.id) FROM vu WHERE vu.v = x.v) AS m FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, null, 2, 40, 3, null, 4, 20, 5, 40 } },
        .{ "WITH y AS (SELECT id, v FROM ex_u) SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM y WHERE y.v = x.v) ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
    };
    inline for (cases) |case| try expectCells(allocator, db, case[0], case[1]);
}

test "correlated subqueries the domain can't carry are rejected rather than bound to an inner column" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupScopes(allocator, std.testing.io, tmp.dir);
    defer db.close();

    // An outer aggregate beside an ungrouped outer column has no group to
    // aggregate in, and nested in another aggregate it has no query to; a
    // FULL JOIN would need every domain row on its dependent side, and a
    // star over a join names columns the lift can't spell out.
    const cases = .{
        "SELECT x.id, (SELECT SUM(x.v) FROM ex_u y WHERE y.id <> x.k) AS s FROM ex_t x ORDER BY x.id",
        "SELECT SUM((SELECT SUM(x.v) FROM ex_u y WHERE y.id = 10)) AS s FROM ex_t x",
        "SELECT x.id, (SELECT COUNT(*) FROM ex_u a FULL JOIN (SELECT id FROM ex_u WHERE v = x.v) b ON a.id = b.id) AS n FROM ex_t x ORDER BY x.id",
        "SELECT x.id, (SELECT COUNT(*) FROM (SELECT * FROM ex_u a JOIN ex_u b ON a.id = b.id WHERE a.id > x.k) d) AS n FROM ex_t x ORDER BY x.id",
    };
    inline for (cases) |sql| try helpers.expectRunError(allocator, db, sql, error.UnsupportedCorrelatedSubquery);
}

test "correlated subqueries with an outer column in the FROM, a correlated UNION or a star lift onto the domain" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupScopes(allocator, std.testing.io, tmp.dir);
    defer db.close();

    // Rows are DuckDB's.
    const cases = .{
        .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM (SELECT id FROM ex_u WHERE ex_u.id > x.k) y) ORDER BY x.id", &[_]?i64{ 1, 2, 3, 4 } },
        .{ "SELECT x.id, (SELECT COUNT(*) FROM (SELECT id FROM ex_u WHERE ex_u.id > x.k) y) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 4, 2, 3, 3, 2, 4, 1, 5, 0 } },
        .{ "SELECT x.id, (SELECT MAX(y.v) FROM (SELECT * FROM ex_u WHERE ex_u.id > x.k) y) AS m FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 9, 2, 9, 3, 9, 4, 9, 5, null } },
        .{ "SELECT x.id, (SELECT COUNT(*) FROM (SELECT id FROM ex_u WHERE id > x.k UNION ALL SELECT id FROM vu WHERE v = x.v) d) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 4, 2, 5, 3, 2, 4, 2, 5, 2 } },
        .{ "SELECT x.id, (SELECT COUNT(*) FROM ex_u a LEFT JOIN (SELECT id FROM ex_u WHERE v = x.v) b ON a.id = b.id WHERE b.id IS NULL) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 5, 2, 3, 3, 5, 4, 4, 5, 3 } },
        .{ "SELECT x.id, (SELECT COUNT(b.id) FROM (SELECT id FROM ex_u WHERE v = x.v) b RIGHT JOIN ex_u a ON b.id = a.id) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 0, 2, 2, 3, 0, 4, 1, 5, 2 } },
        .{ "SELECT x.id, (SELECT SUM(d.c) FROM (SELECT COUNT(*) AS c FROM ex_u WHERE id > x.k) d) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 4, 2, 3, 3, 2, 4, 1, 5, 0 } },
        .{ "SELECT x.id, (SELECT SUM(d.id) FROM (SELECT id FROM ex_u WHERE id > x.k ORDER BY id LIMIT 2) d) AS s FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 50, 2, 70, 3, 90, 4, 50, 5, null } },
        .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ex_u y WHERE y.v = x.v UNION ALL SELECT 1 FROM ex_u z WHERE z.id = x.k + 20) ORDER BY x.id", &[_]?i64{ 1, 2, 3, 4, 5 } },
        .{ "SELECT x.id FROM ex_t x WHERE x.v IN (SELECT y.v FROM ex_u y WHERE y.id > x.k UNION SELECT 5) ORDER BY x.id", &[_]?i64{ 1, 2 } },
        .{ "SELECT x.id FROM ex_t x WHERE x.v NOT IN (SELECT y.v FROM ex_u y WHERE y.id > x.k AND y.v IS NOT NULL UNION SELECT 9) ORDER BY x.id", &[_]?i64{ 1, 4, 5 } },
        .{ "SELECT x.id, (SELECT y.v FROM ex_u y WHERE y.id = x.k UNION ALL SELECT z.v FROM ex_u z WHERE z.id = x.k + 1000) AS v FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 2, 2, 7, 3, null, 4, 2, 5, 9 } },
        .{ "SELECT x.id FROM ex_t x WHERE x.v IN (SELECT * FROM (SELECT v FROM ex_u) y WHERE y.v <> x.v) ORDER BY x.id", &[_]?i64{} },
        .{ "SELECT x.id FROM ex_t x WHERE x.v IN (SELECT * FROM (SELECT v FROM ex_u) y WHERE y.v < x.v + 3) ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
        .{ "SELECT x.id, (SELECT * FROM (SELECT v FROM ex_u WHERE id = x.k + 10) y) AS v FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 7, 2, null, 3, 2, 4, 9, 5, null } },
        .{ "SELECT x.id FROM ex_t x WHERE x.v IN (SELECT MAX(y.v) FROM ex_u y WHERE y.id <> x.k) ORDER BY x.id", &[_]?i64{} },
        .{ "SELECT x.id FROM ex_t x WHERE x.v IN (SELECT MIN(y.v) FROM ex_u y WHERE y.id > x.k) ORDER BY x.id", &[_]?i64{2} },
        .{ "SELECT x.id FROM ex_t x WHERE x.id - 1 IN (SELECT * FROM (SELECT COUNT(*) AS c FROM ex_u WHERE id > x.k) d) ORDER BY x.id", &[_]?i64{3} },
        .{ "SELECT x.id FROM ex_t x WHERE x.v IN (SELECT COUNT(*) FROM ex_u y WHERE y.id > x.k OR y.v = x.v) ORDER BY x.id", &[_]?i64{5} },
    };
    inline for (cases) |case| {
        expectCells(allocator, db, case[0], case[1]) catch |err| {
            std.debug.print("failed: {s}\n", .{case[0]});
            return err;
        };
    }
}

test "a subquery's aggregate over only outer columns aggregates in the outer query" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupScopes(allocator, std.testing.io, tmp.dir);
    defer db.close();

    // Rows are DuckDB's. The outer group's value holds whatever rows the
    // subquery sees, and none leaves it NULL.
    const cases = .{
        .{ "SELECT x.k, (SELECT SUM(x.v) FROM ex_u y WHERE y.id = 10) AS s FROM ex_t x GROUP BY x.k ORDER BY x.k", &[_]?i64{ 10, 5, 20, 2, 30, null, 40, 7, 50, 2 } },
        .{ "SELECT x.k, (SELECT SUM(x.v) FROM ex_u y WHERE y.id = 99) AS s FROM ex_t x GROUP BY x.k ORDER BY x.k", &[_]?i64{ 10, null, 20, null, 30, null, 40, null, 50, null } },
        .{ "SELECT x.k, (SELECT SUM(x.v) + COUNT(*) FROM ex_u y WHERE y.v > 2) AS s FROM ex_t x GROUP BY x.k ORDER BY x.k", &[_]?i64{ 10, 7, 20, 4, 30, null, 40, 9, 50, 4 } },
        .{ "SELECT x.k, (SELECT MIN(x.v) + MAX(y.v) FROM ex_u y WHERE y.id < 35) AS s FROM ex_t x GROUP BY x.k ORDER BY x.k", &[_]?i64{ 10, 12, 20, 9, 30, null, 40, 14, 50, 9 } },
        .{ "SELECT x.k, (SELECT MAX(x.v) FROM ex_u y WHERE y.id = x.k + 10) AS s FROM ex_t x GROUP BY x.k ORDER BY x.k", &[_]?i64{ 10, 5, 20, 2, 30, null, 40, 7, 50, null } },
        .{ "SELECT x.k FROM ex_t x GROUP BY x.k HAVING EXISTS (SELECT 1 FROM ex_u y WHERE y.v = SUM(x.v)) ORDER BY x.k", &[_]?i64{ 20, 40, 50 } },
        .{ "SELECT x.k FROM ex_t x GROUP BY x.k HAVING (SELECT COUNT(*) FROM ex_u y WHERE y.v < SUM(x.v)) > 1 ORDER BY x.k", &[_]?i64{ 10, 40 } },
        .{ "SELECT (SELECT SUM(x.v) FROM ex_u y WHERE y.id = 10) AS s FROM ex_t x", &[_]?i64{16} },
        .{ "SELECT MAX(x.id) AS m, (SELECT SUM(x.v) FROM ex_u y WHERE y.id = 10) AS s FROM ex_t x WHERE x.id > 1", &[_]?i64{ 5, 11 } },
    };
    inline for (cases) |case| {
        expectCells(allocator, db, case[0], case[1]) catch |err| {
            std.debug.print("failed: {s}\n", .{case[0]});
            return err;
        };
    }
}

test "correlated NOT IN lifted onto its domain skips NULLs in the set as the keyed paths do" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupScopes(allocator, std.testing.io, tmp.dir);
    defer db.close();

    // Each pair asks one question through a keyed path and through the
    // domain. id 2's set is {NULL} and id 1's holds a NULL: a skipped NULL
    // keeps the row. A NULL outer value is never NOT IN, even an empty set.
    const pairs = .{
        .{ "y.id = x.k + 10", "y.id - x.k = 10", &[_]?i64{ 1, 2, 4, 5 } },
        .{ "y.id >= x.k", "y.id - x.k >= 0", &[_]?i64{ 1, 4, 5 } },
        .{ "y.id = x.k + 30", "y.id - x.k = 30", &[_]?i64{ 1, 2, 4, 5 } },
    };
    inline for (pairs) |pair| {
        inline for (.{ pair[0], pair[1] }) |corr| {
            try expectCells(allocator, db, "SELECT x.id FROM ex_t x WHERE x.v NOT IN (SELECT y.v FROM ex_u y WHERE " ++ corr ++ ") ORDER BY x.id", pair[2]);
        }
    }
}

test "correlated scalar lifted onto its domain fails an outer row whose value matches several rows" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupScopes(allocator, std.testing.io, tmp.dir);
    defer db.close();

    // Only id 5 passes the other conjunct, and its value matches one row.
    try expectCells(allocator, db, "SELECT x.id FROM ex_t x WHERE x.id = 5 AND x.v = (SELECT y.v FROM ex_u y WHERE y.id <> x.k AND y.id > 30)", &.{5});

    var q = try runSql(allocator, db, "SELECT x.id, (SELECT y.v FROM ex_u y WHERE y.id <> x.k) AS s FROM ex_t x");
    defer q.deinit();
    while (q.next()) |batch| {
        if (batch == null) return error.TestUnexpectedSuccess;
    } else |err| try std.testing.expectEqual(error.SubqueryMultipleRows, err);
}

test "correlated subqueries keyed by an outer expression in DELETE, UPDATE and a join's ON" {
    const allocator = std.testing.allocator;
    const cases = .{
        .{ "DELETE FROM ex_t WHERE EXISTS (SELECT 1 FROM ex_u y WHERE y.id = ex_t.k + 10)", &[_]?i64{ 5, 2 } },
        .{ "UPDATE ex_t SET v = 0 WHERE EXISTS (SELECT 1 FROM ex_u y WHERE y.id = ex_t.k + 10 AND y.v > 2)", &[_]?i64{ 1, 0, 2, 2, 3, null, 4, 0, 5, 2 } },
        .{ "DELETE FROM ex_t WHERE ex_t.v IN (SELECT y.v FROM (SELECT id, v FROM ex_u) y WHERE y.id = ex_t.k * 2)", &[_]?i64{ 1, 5, 3, null, 4, 7, 5, 2 } },
    };
    inline for (cases) |case| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try setupScopes(allocator, std.testing.io, tmp.dir);
        defer db.close();
        try exec(allocator, db, case[0]);
        try expectCells(allocator, db, "SELECT id, v FROM ex_t ORDER BY id", case[1]);
    }

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupScopes(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try expectCells(allocator, db,
        \\SELECT x.id, z.id AS zid FROM ex_t x LEFT JOIN ex_u z
        \\  ON z.v = x.v AND EXISTS (SELECT 1 FROM ex_u y WHERE y.id = x.k + 10) ORDER BY x.id, zid
    , &.{ 1, null, 2, 10, 2, 40, 3, null, 4, 20, 5, null });
}

test "correlated DELETE and UPDATE no keyed path takes write the rows found by the target's primary key" {
    const allocator = std.testing.allocator;
    // Rows are DuckDB's.
    const cases = .{
        .{ "DELETE FROM ex_t WHERE EXISTS (SELECT 1 FROM ex_u y WHERE y.id = ex_t.k + 30 OR y.v = ex_t.v)", &[_]?i64{ 3, null } },
        .{ "UPDATE ex_t SET v = v + 100 WHERE EXISTS (SELECT 1 FROM ex_u y WHERE y.id <> ex_t.k AND y.v = ex_t.v)", &[_]?i64{ 1, 5, 2, 102, 3, null, 4, 107, 5, 102 } },
        .{ "DELETE FROM ex_t WHERE ex_t.v IN (SELECT y.v FROM ex_u y WHERE y.id <> ex_t.k)", &[_]?i64{ 1, 5, 3, null } },
        .{ "DELETE FROM ex_t WHERE EXISTS (SELECT 1 FROM ex_u y WHERE y.id - ex_t.k = 10 AND y.v > ex_t.v)", &[_]?i64{ 2, 2, 3, null, 5, 2 } },
        .{ "UPDATE ex_t SET v = -1 WHERE NOT EXISTS (SELECT 1 FROM ex_u y WHERE y.id = ex_t.k + 30 OR y.v = ex_t.v + 1)", &[_]?i64{ 1, 5, 2, 2, 3, -1, 4, -1, 5, -1 } },
        // A key no inner row has still counts zero rows.
        .{ "DELETE FROM ex_t WHERE ex_t.v * 0 = (SELECT COUNT(*) FROM ex_u y WHERE y.id = ex_t.k + 10)", &[_]?i64{ 1, 5, 2, 2, 3, null, 4, 7 } },
    };
    inline for (cases) |case| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try setupScopes(allocator, std.testing.io, tmp.dir);
        defer db.close();
        try exec(allocator, db, case[0]);
        expectCells(allocator, db, "SELECT id, v FROM ex_t ORDER BY id", case[1]) catch |err| {
            std.debug.print("failed: {s}\n", .{case[0]});
            return err;
        };
    }
}

test "correlated DELETE and UPDATE on a target without a primary key fail when no keyed path takes them" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupScopes(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE np (id BIGINT, v INT, k INT)");
    try exec(allocator, db, "INSERT INTO np SELECT id, v, k FROM ex_t");

    // Without a key there's no sound row identity to write the found rows
    // by, so these fail, and a keyed one still runs.
    try helpers.expectRunError(allocator, db, "DELETE FROM np WHERE EXISTS (SELECT 1 FROM ex_u y WHERE y.id = np.k OR y.v = np.v)", error.UnsupportedCorrelatedSubquery);
    try helpers.expectRunError(allocator, db, "UPDATE np SET v = 0 WHERE np.v IN (SELECT y.v FROM ex_u y WHERE y.id <> np.k)", error.UnsupportedCorrelatedSubquery);
    try helpers.expectRunError(allocator, db, "UPDATE np SET v = 0 WHERE (SELECT COUNT(*) FROM ex_u y WHERE y.v = np.v) + 1 = 2", error.UnsupportedCorrelatedSubquery);
    try helpers.expectRunError(allocator, db, "DELETE FROM np WHERE (SELECT COUNT(*) FROM ex_u y WHERE y.v <> np.v) = 2", error.UnsupportedCorrelatedSubquery);
    try helpers.expectRunError(allocator, db, "UPDATE np SET v = (SELECT COUNT(*) FROM ex_u y WHERE y.v = np.v)", error.UnsupportedCorrelatedSubquery);
    try expectCells(allocator, db, "SELECT id, v FROM np ORDER BY id", &.{ 1, 5, 2, 2, 3, null, 4, 7, 5, 2 });
    try exec(allocator, db, "DELETE FROM np WHERE EXISTS (SELECT 1 FROM ex_u y WHERE y.id = np.k + 10)");
    try expectCells(allocator, db, "SELECT id, v FROM np ORDER BY id", &.{ 5, 2 });
}

test "an outer join's ON correlated by OR, <> or terms over both rows matches the pairs it keeps" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupScopes(allocator, std.testing.io, tmp.dir);
    defer db.close();

    // Rows are DuckDB's, from the inner join matched back on each side's id.
    const cases = .{
        .{ "SELECT x.id, z.id AS zid FROM ex_t x LEFT JOIN ex_u z ON z.v = x.v AND EXISTS (SELECT 1 FROM ex_u y WHERE y.id = x.k + 20 OR y.v = z.v + 5) ORDER BY x.id, zid", &[_]?i64{ 1, null, 2, 10, 2, 40, 3, null, 4, null, 5, 10, 5, 40 } },
        .{ "SELECT x.id, z.id AS zid FROM ex_t x RIGHT JOIN ex_u z ON z.v = x.v AND EXISTS (SELECT 1 FROM ex_u y WHERE y.id <> x.k AND y.v = z.v) ORDER BY x.id, zid", &[_]?i64{ null, 30, null, 50, 2, 10, 2, 40, 4, 20, 5, 10, 5, 40 } },
        .{ "SELECT x.id, z.id AS zid FROM ex_t x FULL JOIN ex_u z ON z.v >= x.v AND EXISTS (SELECT 1 FROM ex_u y WHERE y.id + z.id = x.k + 50) ORDER BY x.id, zid", &[_]?i64{ null, 10, null, 30, 1, 20, 1, 50, 2, 20, 2, 40, 2, 50, 3, null, 4, 50, 5, 50 } },
        .{ "SELECT x.id, z.id AS zid FROM ex_t x LEFT JOIN ex_u z ON z.v = x.v AND (SELECT COUNT(*) FROM ex_u y WHERE y.id <> x.k AND y.v >= z.v) > 2 ORDER BY x.id, zid", &[_]?i64{ 1, null, 2, 10, 2, 40, 3, null, 4, null, 5, 10, 5, 40 } },
    };
    inline for (cases) |case| {
        expectCells(allocator, db, case[0], case[1]) catch |err| {
            std.debug.print("failed: {s}\n", .{case[0]});
            return err;
        };
    }
}

test "a subquery's join ON reads an enclosing query's columns" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupScopes(allocator, std.testing.io, tmp.dir);
    defer db.close();

    // Rows are DuckDB's. It rejects an enclosing column in an outer join's
    // ON, so those rows read the column projected onto the preserved input.
    const cases = .{
        .{ "SELECT x.id, (SELECT COUNT(*) FROM ex_u a JOIN ex_u b ON a.id = b.id AND b.v = x.v) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 0, 2, 2, 3, 0, 4, 1, 5, 2 } },
        .{ "SELECT x.id, (SELECT COUNT(*) FROM ex_u a JOIN vu b ON a.v = b.v AND a.id + b.id > x.k + 30) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 4, 2, 2, 3, 2, 4, 2, 5, 1 } },
        .{ "SELECT x.id, (SELECT COUNT(*) FROM ex_u a JOIN ex_u b ON a.id = b.id AND x.v > 3) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 5, 2, 0, 3, 0, 4, 5, 5, 0 } },
        .{ "SELECT x.id, (SELECT COUNT(*) FROM ex_u a JOIN ex_u b ON a.id = b.id AND b.id = k) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 1, 2, 1, 3, 1, 4, 1, 5, 1 } },
        .{ "SELECT x.id, (SELECT COUNT(*) FROM ex_u a JOIN ex_u b ON a.id = b.id JOIN ex_u c ON c.v = b.v AND c.id <> x.k) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 4, 2, 5, 3, 6, 4, 4, 5, 5 } },
        .{ "SELECT x.id, (SELECT COUNT(*) FROM (SELECT id, v FROM ex_u) a JOIN ex_u b ON a.v = b.v AND b.id <> x.k WHERE a.id > x.k) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 3, 2, 3, 3, 3, 4, 1, 5, 0 } },
        .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM ex_u a JOIN ex_u b ON a.id = b.id AND b.v = x.v WHERE a.w <> 'a') ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
        .{ "SELECT x.id FROM ex_t x WHERE x.v IN (SELECT a.v FROM ex_u a JOIN ex_u b ON a.v = b.v AND b.id > x.k) ORDER BY x.id", &[_]?i64{2} },
        .{ "SELECT x.id FROM ex_t x WHERE x.v NOT IN (SELECT a.v FROM ex_u a JOIN ex_u b ON a.v = b.v AND b.id > x.k WHERE a.v IS NOT NULL) ORDER BY x.id", &[_]?i64{ 1, 4, 5 } },
        .{ "SELECT x.id, (SELECT COUNT(*) FROM ex_u z WHERE EXISTS (SELECT 1 FROM ex_u a JOIN ex_u b ON a.id = b.id AND b.v = x.v AND a.id = z.id)) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 0, 2, 2, 3, 0, 4, 1, 5, 2 } },
        .{ "SELECT x.v FROM ex_t x GROUP BY x.v HAVING EXISTS (SELECT 1 FROM ex_u a JOIN ex_u b ON a.id = b.id AND b.v = x.v) ORDER BY x.v", &[_]?i64{ 2, 7 } },
        .{ "SELECT x.id, z.id AS zid FROM ex_t x JOIN ex_u z ON z.v = x.v AND EXISTS (SELECT 1 FROM ex_u a JOIN ex_u b ON a.id = b.id AND b.v = z.v AND a.id <> x.k) ORDER BY x.id, zid", &[_]?i64{ 2, 10, 2, 40, 4, 20, 5, 10, 5, 40 } },
        .{ "SELECT x.id, (SELECT COUNT(b.id) FROM ex_u a LEFT JOIN ex_u b ON a.id = b.id AND b.v = x.v) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 0, 2, 2, 3, 0, 4, 1, 5, 2 } },
        .{ "SELECT x.id, (SELECT COUNT(b.id) FROM ex_u a LEFT JOIN ex_u b ON a.id = b.id AND a.v = x.v) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 0, 2, 2, 3, 0, 4, 1, 5, 2 } },
        .{ "SELECT x.id, (SELECT COUNT(b.id) FROM ex_u a LEFT JOIN ex_u b ON a.v = b.v AND a.id + b.id > x.k + 30) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 4, 2, 2, 3, 2, 4, 2, 5, 1 } },
        .{ "SELECT x.id, (SELECT COUNT(*) + SUM(b.id) FROM ex_u a LEFT JOIN ex_u b ON a.v = b.v AND b.id > x.k) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 155, 2, 135, 3, 135, 4, 55, 5, null } },
        .{ "SELECT x.id, (SELECT COUNT(*) FROM ex_u a LEFT JOIN ex_u b ON a.id = b.id AND b.v = x.v WHERE b.id IS NULL) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 5, 2, 3, 3, 5, 4, 4, 5, 3 } },
        .{ "SELECT x.id FROM ex_t x WHERE NOT EXISTS (SELECT 1 FROM ex_u a LEFT JOIN ex_u b ON a.id = b.id AND b.v = x.v WHERE b.id IS NOT NULL) ORDER BY x.id", &[_]?i64{ 1, 3 } },
        .{ "SELECT x.id, (SELECT COUNT(a.id) FROM ex_u a RIGHT JOIN ex_u b ON a.id = b.id AND a.v = x.v) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 0, 2, 2, 3, 0, 4, 1, 5, 2 } },
        .{ "SELECT x.id, (SELECT COUNT(a.id) FROM ex_u a RIGHT JOIN ex_u b ON a.v = b.v AND a.id + b.id > x.k + 30) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 4, 2, 2, 3, 2, 4, 2, 5, 1 } },
    };
    inline for (cases) |case| {
        expectCells(allocator, db, case[0], case[1]) catch |err| {
            std.debug.print("failed: {s}\n", .{case[0]});
            return err;
        };
    }

    // A FULL JOIN would need every domain row on both inputs. An ON can't
    // see a relation of its own FROM outside its join, and that name is the
    // FROM's before it is an enclosing query's.
    try helpers.expectRunError(allocator, db, "SELECT x.id, (SELECT COUNT(*) FROM ex_u a FULL JOIN ex_u b ON a.id = b.id AND b.v = x.v) AS n FROM ex_t x", error.UnsupportedCorrelatedSubquery);
    try helpers.expectRunError(allocator, db, "SELECT x.id, (SELECT COUNT(*) FROM ex_u a JOIN ex_u b ON a.id = b.id AND zz.v = 1) AS n FROM ex_t x", error.UnsupportedCorrelatedSubquery);
    const unseen = .{
        "SELECT a.id FROM ex_u a JOIN ex_u b ON a.id = b.id AND zz.v = 1",
        "SELECT a.id, (SELECT COUNT(*) FROM ex_u a, ex_u b JOIN ex_u c ON a.id = c.id) AS n FROM ex_t a",
        "SELECT a.id, (SELECT COUNT(*) FROM ex_u b JOIN ex_u c ON a.k = c.id, ex_u a) AS n FROM ex_t a",
        "SELECT a.id, (SELECT COUNT(*) FROM ex_u b LEFT JOIN ex_u c ON c.v = a.v JOIN ex_u a ON a.id = b.id) AS n FROM ex_t a",
        "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT 1 FROM (SELECT b.id FROM ex_u b JOIN ex_u c ON b.id = c.id AND c.v = d.v) d)",
    };
    inline for (unseen) |sql| try helpers.expectRunError(allocator, db, sql, error.SqlOnRefsUnknownTable);
}

test "correlated DELETE and UPDATE whose subquery's join ON reads the target row" {
    const allocator = std.testing.allocator;
    // Rows are DuckDB's. `np` is a copy of `ex_t` without a key, which the
    // keyed statements filter in place.
    const cases = .{
        .{ "ex_t", "DELETE FROM ex_t WHERE EXISTS (SELECT 1 FROM ex_u a JOIN ex_u b ON a.id = b.id AND b.v = ex_t.v)", &[_]?i64{ 1, 5, 10, 3, null, 30 } },
        .{ "np", "DELETE FROM np WHERE EXISTS (SELECT 1 FROM ex_u a JOIN ex_u b ON a.id = b.id AND b.v = np.v)", &[_]?i64{ 1, 5, 10, 3, null, 30 } },
        .{ "ex_t", "UPDATE ex_t SET v = -2 WHERE ex_t.id < (SELECT COUNT(*) FROM ex_u a JOIN ex_u b ON a.v = b.v AND b.v = ex_t.v)", &[_]?i64{ 1, 5, 10, 2, -2, 20, 3, null, 30, 4, 7, 40, 5, 2, 50 } },
        .{ "np", "UPDATE np SET v = -2 WHERE np.id < (SELECT COUNT(*) FROM ex_u a JOIN ex_u b ON a.v = b.v AND b.v = np.v)", &[_]?i64{ 1, 5, 10, 2, -2, 20, 3, null, 30, 4, 7, 40, 5, 2, 50 } },
        .{ "ex_t", "DELETE FROM ex_t WHERE ex_t.id * 0 = (SELECT COUNT(b.id) FROM ex_u a LEFT JOIN ex_u b ON a.v = b.v AND b.id > ex_t.k)", &[_]?i64{ 1, 5, 10, 2, 2, 20, 3, null, 30, 4, 7, 40 } },
        .{ "ex_t", "UPDATE ex_t SET v = -3 WHERE EXISTS (SELECT 1 FROM ex_u a LEFT JOIN ex_u b ON a.id = b.id AND b.v = ex_t.v WHERE b.id IS NULL AND a.v = 7)", &[_]?i64{ 1, -3, 10, 2, -3, 20, 3, -3, 30, 4, 7, 40, 5, -3, 50 } },
    };
    inline for (cases) |case| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try setupScopes(allocator, std.testing.io, tmp.dir);
        defer db.close();
        try exec(allocator, db, "CREATE TABLE np (id BIGINT, v INT, k INT)");
        try exec(allocator, db, "INSERT INTO np SELECT id, v, k FROM ex_t");
        try exec(allocator, db, case[1]);
        expectCells(allocator, db, "SELECT id, v, k FROM " ++ case[0] ++ " ORDER BY id", case[2]) catch |err| {
            std.debug.print("failed: {s}\n", .{case[1]});
            return err;
        };
    }
}

test "correlated scalar over a CTE that shadows its table, keyed by an expression over the outer row" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE rr (id BIGINT PRIMARY KEY, p INT, c VARCHAR(4), d DATE, amount BIGINT)");
    try exec(allocator, db, "INSERT INTO rr VALUES (1, 1, 'c1', '2024-01-01', 100), (2, 1, 'c1', '2024-04-01', 0), (3, 1, 'c2', '2024-01-01', 50), " ++
        "(4, 1, 'c2', '2024-04-01', 70), (5, 1, 'c3', '2024-04-01', 30), (6, 2, 'c1', '2024-01-01', 999), (7, 1, 'c1', '2023-10-01', 5)");
    const t = try db.openTable("rr", .{});
    try t.flush();

    // Each row's amount three months earlier, as a quarterly cohort report
    // reads it. Rows are DuckDB's.
    var q = try helpers.runSqlMysqlCtx(allocator, db,
        \\WITH rr AS (SELECT id, p, c, d, amount FROM rr WHERE p = 1)
        \\SELECT i.id, CAST(COALESCE((SELECT SUM(CAST(COALESCE(c1.amount, 0) AS SIGNED)) FROM rr c1
        \\  WHERE c1.p = i.p AND c1.c = i.c AND c1.d = ADDDATE(i.d, INTERVAL -3 MONTH)), 0) AS SIGNED) AS last_amount
        \\FROM rr i ORDER BY i.c, i.d
    );
    defer q.deinit();
    const cells = try collectIntCells(allocator, &q);
    defer allocator.free(cells);
    try std.testing.expectEqualSlices(?i64, &.{ 7, 0, 1, 5, 2, 100, 3, 0, 4, 50, 5, 0 }, cells);
}

test "a correlated subquery's item that returns an outer column, or opens with its qualifier, keeps its name" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupScopes(allocator, std.testing.io, tmp.dir);
    defer db.close();

    // `x.k + y.id` reads like column `k + y.id` of `x`, and `x.k` reads
    // the domain's column once lifted, yet each is the item's own value.
    // Rows are DuckDB's.
    const cases = .{
        .{ "SELECT x.id, (SELECT x.k + y.id FROM ex_u y WHERE y.id <> x.k AND y.v = 9) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 60, 2, 70, 3, 80, 4, 90, 5, null } },
        .{ "SELECT x.id, (SELECT y.id + x.k FROM ex_u y WHERE y.id <> x.k AND y.v = 9) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 60, 2, 70, 3, 80, 4, 90, 5, null } },
        .{ "SELECT x.id, (SELECT x.k FROM ex_u y WHERE y.id <> x.k AND y.v = 9) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 10, 2, 20, 3, 30, 4, 40, 5, null } },
        .{ "SELECT x.id, (SELECT x.k FROM ex_u y WHERE y.id > x.k LIMIT 1) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 10, 2, 20, 3, 30, 4, 40, 5, null } },
        .{ "SELECT x.id, (SELECT DISTINCT x.k FROM ex_u y WHERE y.id > x.k) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 10, 2, 20, 3, 30, 4, 40, 5, null } },
        .{ "SELECT x.id, (SELECT x.k + y.id AS s FROM ex_u y WHERE y.id > x.k ORDER BY s DESC LIMIT 1) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 60, 2, 70, 3, 80, 4, 90, 5, null } },
        .{ "SELECT x.id, (SELECT x.k + y.id FROM ex_u y WHERE y.id > x.k ORDER BY x.k + y.id DESC LIMIT 1) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 60, 2, 70, 3, 80, 4, 90, 5, null } },
        .{ "SELECT x.id FROM ex_t x WHERE (SELECT x.v FROM ex_u y WHERE y.id > x.k AND y.v = 9) = 5 ORDER BY x.id", &[_]?i64{1} },
        .{ "SELECT x.id, (SELECT SUM(d.k) FROM (SELECT x.k FROM ex_u y WHERE y.id > x.k) d) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 40, 2, 60, 3, 60, 4, 40, 5, null } },
        .{ "SELECT x.id, (SELECT MAX(d.q) FROM (SELECT x.k + y.id AS q FROM ex_u y WHERE y.id > x.k) d) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 60, 2, 70, 3, 80, 4, 90, 5, null } },
        .{ "SELECT x.id FROM ex_t x WHERE x.id * 10 IN (SELECT x.k FROM ex_u y WHERE y.id <> x.k AND y.v = 2) ORDER BY x.id", &[_]?i64{ 1, 2, 3, 4, 5 } },
        .{ "SELECT x.id FROM ex_t x WHERE x.k + 10 IN (SELECT x.k + y.id FROM ex_u y WHERE y.id <> x.k) ORDER BY x.id", &[_]?i64{ 2, 3, 4, 5 } },
        .{ "SELECT x.id FROM ex_t x WHERE x.id * 10 NOT IN (SELECT x.k FROM ex_u y WHERE y.id <> x.k AND y.v = 2) ORDER BY x.id", &[_]?i64{} },
        .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT x.k FROM ex_u y WHERE y.id <> x.k AND y.v = x.v) ORDER BY x.id", &[_]?i64{ 2, 4, 5 } },
        .{ "SELECT x.id FROM ex_t x WHERE EXISTS (SELECT x.v, y.v FROM ex_u y WHERE y.id > x.k AND y.v = x.v) ORDER BY x.id", &[_]?i64{2} },
        .{ "SELECT x.id, (SELECT x.k + y.id FROM ex_u y WHERE y.id = x.k UNION ALL SELECT x.k + z.id FROM ex_u z WHERE z.id = x.k + 1000) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 20, 2, 40, 3, 60, 4, 80, 5, 100 } },
        .{ "SELECT x.id FROM ex_t x WHERE x.v IN (SELECT x.v FROM ex_u y WHERE y.id = x.k + 10 UNION SELECT y.v FROM ex_u y WHERE y.id > x.k) ORDER BY x.id", &[_]?i64{ 1, 2, 4 } },
        .{ "SELECT x.id, (SELECT x.k FROM (SELECT id, v FROM ex_u) y WHERE y.id <> x.k AND y.v = 9) AS n FROM ex_t x ORDER BY x.id", &[_]?i64{ 1, 10, 2, 20, 3, 30, 4, 40, 5, null } },
        .{ "WITH c AS (SELECT id, v, k FROM ex_t) SELECT x.id, (SELECT x.k + y.id FROM vu y WHERE y.id <> x.k AND y.v = 9) AS n FROM c x ORDER BY x.id", &[_]?i64{ 1, 60, 2, 70, 3, 80, 4, 90, 5, null } },
    };
    inline for (cases) |case| {
        expectCells(allocator, db, case[0], case[1]) catch |err| {
            std.debug.print("failed: {s}\n", .{case[0]});
            return err;
        };
    }
}

/// `np` is a copy of `ex_t` without a key.
fn expectRowsAfter(allocator: std.mem.Allocator, statement: []const u8, table: []const u8, expected: []const ?i64) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupScopes(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE np (id BIGINT, v INT, k INT)");
    try exec(allocator, db, "INSERT INTO np SELECT id, v, k FROM ex_t");
    const t = try db.openTable("np", .{});
    try t.flush();
    try exec(allocator, db, statement);
    const select = try std.fmt.allocPrint(allocator, "SELECT id, v, k FROM {s} ORDER BY id", .{table});
    defer allocator.free(select);
    expectCells(allocator, db, select, expected) catch |err| {
        std.debug.print("failed: {s}\n", .{statement});
        return err;
    };
}

test "correlated DELETE and UPDATE comparing a keyed correlated scalar with a literal filter in place on any target" {
    const allocator = std.testing.allocator;
    // The subquery on either side of the comparison, a literal or a value
    // over the target's row on the other. Each runs on the keyed target and
    // on its key-less copy. Rows are DuckDB's.
    const cases = .{
        .{ "UPDATE ex_t SET v = -1 WHERE (SELECT COUNT(*) FROM ex_u y WHERE y.v = ex_t.v) = 1", "UPDATE np SET v = -1 WHERE (SELECT COUNT(*) FROM ex_u y WHERE y.v = np.v) = 1", &[_]?i64{ 1, 5, 10, 2, 2, 20, 3, null, 30, 4, -1, 40, 5, 2, 50 } },
        .{ "UPDATE ex_t SET v = -2 WHERE 1 = (SELECT COUNT(*) FROM ex_u y WHERE y.v = ex_t.v AND y.id <> 40)", "UPDATE np SET v = -2 WHERE 1 = (SELECT COUNT(*) FROM ex_u y WHERE y.v = np.v AND y.id <> 40)", &[_]?i64{ 1, 5, 10, 2, -2, 20, 3, null, 30, 4, -2, 40, 5, -2, 50 } },
        .{ "UPDATE ex_t SET v = -3 WHERE (SELECT COUNT(*) FROM ex_u y WHERE y.v = ex_t.v AND y.id <> 40) = 0", "UPDATE np SET v = -3 WHERE (SELECT COUNT(*) FROM ex_u y WHERE y.v = np.v AND y.id <> 40) = 0", &[_]?i64{ 1, -3, 10, 2, 2, 20, 3, -3, 30, 4, 7, 40, 5, 2, 50 } },
        .{ "UPDATE ex_t SET v = -4 WHERE (SELECT MAX(y.id) FROM ex_u y WHERE y.v = ex_t.v AND y.id <> 40) = 10", "UPDATE np SET v = -4 WHERE (SELECT MAX(y.id) FROM ex_u y WHERE y.v = np.v AND y.id <> 40) = 10", &[_]?i64{ 1, 5, 10, 2, -4, 20, 3, null, 30, 4, 7, 40, 5, -4, 50 } },
        .{ "UPDATE ex_t SET v = -5 WHERE (SELECT COUNT(*) FROM ex_u y WHERE y.v = ex_t.v) = ex_t.id - 3", "UPDATE np SET v = -5 WHERE (SELECT COUNT(*) FROM ex_u y WHERE y.v = np.v) = np.id - 3", &[_]?i64{ 1, 5, 10, 2, 2, 20, 3, -5, 30, 4, -5, 40, 5, -5, 50 } },
        .{ "UPDATE ex_t SET v = -6 WHERE NOT ((SELECT MAX(y.id) FROM ex_u y WHERE y.v = ex_t.v) > 30)", "UPDATE np SET v = -6 WHERE NOT ((SELECT MAX(y.id) FROM ex_u y WHERE y.v = np.v) > 30)", &[_]?i64{ 1, 5, 10, 2, 2, 20, 3, null, 30, 4, -6, 40, 5, 2, 50 } },
        .{ "UPDATE ex_t SET v = -7 WHERE (SELECT COUNT(*) FROM ex_u y WHERE y.v = ex_t.v) = 1 OR ex_t.id = 3", "UPDATE np SET v = -7 WHERE (SELECT COUNT(*) FROM ex_u y WHERE y.v = np.v) = 1 OR np.id = 3", &[_]?i64{ 1, 5, 10, 2, 2, 20, 3, -7, 30, 4, -7, 40, 5, 2, 50 } },
        .{ "DELETE FROM ex_t WHERE (SELECT COUNT(*) FROM ex_u y WHERE y.v = ex_t.v) = 1", "DELETE FROM np WHERE (SELECT COUNT(*) FROM ex_u y WHERE y.v = np.v) = 1", &[_]?i64{ 1, 5, 10, 2, 2, 20, 3, null, 30, 5, 2, 50 } },
        .{ "DELETE FROM ex_t WHERE (SELECT MAX(y.id) FROM ex_u y WHERE y.v = ex_t.v) > 15", "DELETE FROM np WHERE (SELECT MAX(y.id) FROM ex_u y WHERE y.v = np.v) > 15", &[_]?i64{ 1, 5, 10, 3, null, 30 } },
        .{ "DELETE FROM ex_t WHERE 15 < (SELECT MIN(y.id) FROM ex_u y WHERE y.v = ex_t.v)", "DELETE FROM np WHERE 15 < (SELECT MIN(y.id) FROM ex_u y WHERE y.v = np.v)", &[_]?i64{ 1, 5, 10, 2, 2, 20, 3, null, 30, 5, 2, 50 } },
    };
    inline for (cases) |case| {
        try expectRowsAfter(allocator, case[0], "ex_t", case[2]);
        try expectRowsAfter(allocator, case[1], "np", case[2]);
    }
}

test "correlated DELETE and UPDATE computing a correlated scalar no keyed path takes, or assigning one, write the rows found by the primary key" {
    const allocator = std.testing.allocator;
    // Rows are DuckDB's.
    const cases = .{
        .{ "UPDATE ex_t SET v = -8 WHERE (SELECT COUNT(*) FROM ex_u y WHERE y.v = ex_t.v) + 1 = 2", &[_]?i64{ 1, 5, 10, 2, 2, 20, 3, null, 30, 4, -8, 40, 5, 2, 50 } },
        .{ "UPDATE ex_t SET v = -9 WHERE (SELECT MAX(y.id) FROM ex_u y WHERE y.v = ex_t.v) IS NULL", &[_]?i64{ 1, -9, 10, 2, 2, 20, 3, -9, 30, 4, 7, 40, 5, 2, 50 } },
        .{ "DELETE FROM ex_t WHERE (SELECT COUNT(*) FROM ex_u y WHERE y.v <> ex_t.v) = 2", &[_]?i64{ 1, 5, 10, 3, null, 30, 4, 7, 40 } },
        .{ "UPDATE ex_t SET v = -10 WHERE COALESCE((SELECT SUM(y.id) FROM ex_u y WHERE y.v = ex_t.v), 0) > 20", &[_]?i64{ 1, 5, 10, 2, -10, 20, 3, null, 30, 4, 7, 40, 5, -10, 50 } },
        .{ "UPDATE ex_t SET v = (SELECT COUNT(*) FROM ex_u y WHERE y.v = ex_t.v)", &[_]?i64{ 1, 0, 10, 2, 2, 20, 3, 0, 30, 4, 1, 40, 5, 2, 50 } },
        .{ "UPDATE ex_t SET k = (SELECT MAX(y.id) FROM ex_u y WHERE y.v = ex_t.v) WHERE ex_t.id < 4", &[_]?i64{ 1, 5, null, 2, 2, 40, 3, null, null, 4, 7, 40, 5, 2, 50 } },
        .{ "UPDATE ex_t SET v = 100 + (SELECT COUNT(*) FROM ex_u y WHERE y.v <> ex_t.v)", &[_]?i64{ 1, 104, 10, 2, 102, 20, 3, 100, 30, 4, 103, 40, 5, 102, 50 } },
        .{ "UPDATE ex_t SET k = (SELECT MIN(y.id) FROM ex_u y WHERE y.v = ex_t.v) WHERE (SELECT COUNT(*) FROM ex_u y WHERE y.v = ex_t.v) = 2", &[_]?i64{ 1, 5, 10, 2, 2, 10, 3, null, 30, 4, 7, 40, 5, 2, 10 } },
        .{ "DELETE FROM ex_t WHERE ex_t.id * 10 IN (SELECT ex_t.k FROM ex_u y WHERE y.id <> ex_t.k AND y.v = 2)", &[_]?i64{} },
        .{ "UPDATE ex_t SET v = 0 WHERE (SELECT ex_t.v FROM ex_u y WHERE y.id > ex_t.k AND y.v = 9) = 5", &[_]?i64{ 1, 0, 10, 2, 2, 20, 3, null, 30, 4, 7, 40, 5, 2, 50 } },
    };
    inline for (cases) |case| try expectRowsAfter(allocator, case[0], "ex_t", case[1]);
}

fn setupTextKeys(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE tk_t (id BIGINT PRIMARY KEY, n INT, d DATE, f DOUBLE, m DECIMAL(6,2), s VARCHAR(8))");
    try exec(allocator, db, "CREATE TABLE tk_u (id BIGINT PRIMARY KEY, s VARCHAR(12), v INT)");
    try exec(allocator, db, "CREATE TABLE tk_np (id BIGINT, n INT)");
    try exec(allocator, db, "INSERT INTO tk_t VALUES (1, 7, '2024-01-07', 7.0, 7.00, '7'), (2, 8, '2024-01-08', 8.5, 8.50, '07'), (3, 9, '2024-01-09', 9.0, 9.00, 'x')");
    try exec(allocator, db,
        \\INSERT INTO tk_u VALUES (1, '7', 10), (2, '07', 20), (3, '7.0', 30), (4, '8', 40), (5, '7x', 50),
        \\  (6, '2024-01-07', 60), (7, '2024-1-7', 70), (8, '8.5', 80), (9, '8.50', 90), (10, NULL, 100)
    );
    try exec(allocator, db, "INSERT INTO tk_np VALUES (1, 7), (2, 8), (3, 9)");
    inline for (.{ "tk_t", "tk_u", "tk_np" }) |name| {
        const t = try db.openTable(name, .{});
        try t.flush();
    }
    return db;
}

test "a correlated scalar keyed by text against a number or a date meets one group per outer row" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupTextKeys(allocator, std.testing.io, tmp.dir);
    defer db.close();

    // '7', '07' and '7.0' all equal the outer 7, so they count as one group
    // under the comparison's type. Rows are DuckDB's, comparing TRY_CAST of
    // the text to the outer key's type.
    const cases = .{
        .{ "SELECT t.id, (SELECT COUNT(*) FROM tk_u u WHERE u.s = t.n) AS c FROM tk_t t ORDER BY t.id", &[_]?i64{ 1, 3, 2, 1, 3, 0 } },
        .{ "SELECT t.id, (SELECT SUM(u.v) FROM tk_u u WHERE u.s = t.n) AS c FROM tk_t t ORDER BY t.id", &[_]?i64{ 1, 60, 2, 40, 3, null } },
        .{ "SELECT t.id FROM tk_t t WHERE (SELECT COUNT(*) FROM tk_u u WHERE u.s = t.n) = 3 ORDER BY t.id", &[_]?i64{1} },
        .{ "SELECT t.id FROM tk_t t WHERE t.id < (SELECT COUNT(*) FROM tk_u u WHERE u.s = t.n) ORDER BY t.id", &[_]?i64{1} },
        .{ "SELECT t.id, (SELECT COUNT(*) FROM tk_u u WHERE u.s = t.n + 1) AS c FROM tk_t t ORDER BY t.id", &[_]?i64{ 1, 1, 2, 0, 3, 0 } },
        .{ "SELECT t.id, (SELECT COUNT(*) FROM tk_u u WHERE u.s = t.d) AS c FROM tk_t t ORDER BY t.id", &[_]?i64{ 1, 2, 2, 0, 3, 0 } },
        .{ "SELECT t.id, (SELECT COUNT(*) FROM tk_u u WHERE u.s = t.f) AS c FROM tk_t t ORDER BY t.id", &[_]?i64{ 1, 3, 2, 2, 3, 0 } },
        .{ "SELECT t.id, (SELECT COUNT(*) FROM tk_u u WHERE u.s = t.m) AS c FROM tk_t t ORDER BY t.id", &[_]?i64{ 1, 3, 2, 2, 3, 0 } },
        .{ "SELECT t.id, (SELECT COUNT(*) FROM tk_u u WHERE u.s = t.s) AS c FROM tk_t t ORDER BY t.id", &[_]?i64{ 1, 1, 2, 1, 3, 0 } },
        .{ "SELECT t.id, (SELECT u.v FROM tk_u u WHERE u.s = t.n ORDER BY u.v LIMIT 1) AS c FROM tk_t t ORDER BY t.id", &[_]?i64{ 1, 10, 2, 40, 3, null } },
        .{ "SELECT t.id, (SELECT u.v FROM tk_u u WHERE u.s = t.n ORDER BY u.v DESC LIMIT 1 OFFSET 1) AS c FROM tk_t t ORDER BY t.id", &[_]?i64{ 1, 20, 2, null, 3, null } },
        .{ "SELECT t.id, (SELECT u.v FROM tk_u u WHERE u.s = t.n) AS c FROM tk_t t WHERE t.id = 2", &[_]?i64{ 2, 40 } },
    };
    inline for (cases) |case| {
        expectCells(allocator, db, case[0], case[1]) catch |err| {
            std.debug.print("failed: {s}\n", .{case[0]});
            return err;
        };
    }

    var q = try runSql(allocator, db, "SELECT t.id, (SELECT u.v FROM tk_u u WHERE u.s = t.n) AS c FROM tk_t t");
    defer q.deinit();
    while (q.next()) |batch| {
        if (batch == null) return error.TestUnexpectedSuccess;
    } else |err| try std.testing.expectEqual(error.SubqueryMultipleRows, err);
}

test "correlated DELETE and UPDATE by a scalar keyed by text against a number write one group per target row" {
    const allocator = std.testing.allocator;
    // Rows are DuckDB's.
    const cases = .{
        .{ "UPDATE tk_t SET n = n + 100 WHERE (SELECT COUNT(*) FROM tk_u u WHERE u.s = tk_t.n) = 3", "SELECT id, n FROM tk_t ORDER BY id", &[_]?i64{ 1, 107, 2, 8, 3, 9 } },
        .{ "UPDATE tk_np SET n = 0 WHERE 1 = (SELECT COUNT(*) FROM tk_u u WHERE u.s = tk_np.n)", "SELECT id, n FROM tk_np ORDER BY id", &[_]?i64{ 1, 7, 2, 0, 3, 9 } },
        .{ "DELETE FROM tk_np WHERE (SELECT COUNT(*) FROM tk_u u WHERE u.s = tk_np.n) = 3", "SELECT id, n FROM tk_np ORDER BY id", &[_]?i64{ 2, 8, 3, 9 } },
        .{ "UPDATE tk_t SET n = (SELECT SUM(u.v) FROM tk_u u WHERE u.s = tk_t.n)", "SELECT id, n FROM tk_t ORDER BY id", &[_]?i64{ 1, 60, 2, 40, 3, null } },
    };
    inline for (cases) |case| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try setupTextKeys(allocator, std.testing.io, tmp.dir);
        defer db.close();
        try exec(allocator, db, case[0]);
        expectCells(allocator, db, case[1], case[2]) catch |err| {
            std.debug.print("failed: {s}\n", .{case[0]});
            return err;
        };
    }
}
