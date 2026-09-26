//! Correlated scalar subquery — `WHERE outer.x op (SELECT agg(y)
//! FROM B WHERE B.k = outer.k)`. Tier 3b.
//!
//! thinDB decorrelates by promoting the correlation keys into the
//! inner's GROUP BY, draining the rewritten inner into a per-key
//! map, and rewriting the outer predicate as "per row, look up
//! `outer.k` in the map and compare against the aggregate result."
//! No matching key → predicate fails (row filtered).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE part (p_id BIGINT PRIMARY KEY)");
    try exec(
        allocator,
        db,
        "CREATE TABLE li (l_id BIGINT PRIMARY KEY, l_partid BIGINT NOT NULL, l_qty INT NOT NULL)",
    );
    try exec(allocator, db, "INSERT INTO part (p_id) VALUES (1), (2), (3)");
    // part 1: qty values {10, 30, 50}, avg = 30
    // part 2: qty values {100, 200},   avg = 150
    // part 3: qty values {5},          avg = 5
    try exec(
        allocator,
        db,
        "INSERT INTO li (l_id, l_partid, l_qty) VALUES (1,1,10), (2,1,30), (3,1,50), (4,2,100), (5,2,200), (6,3,5)",
    );
    const t1 = try db.openTable("part", .{});
    try t1.flush();
    const t2 = try db.openTable("li", .{});
    try t2.flush();
    return db;
}

test "correlated scalar: SUM per key compared to literal" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // For each part, find l_id where l_qty > (SUM(l_qty) per partid) / 1000.
    // Simpler: SUM per part — 1:90, 2:300, 3:5. Select li rows where qty > 100 (its part's sum / 3).
    // But correlated scalar's RHS is the subquery; the outer is the *parent* row.
    //
    // Better test: "for each part, is the part's total qty > 50?" → parts 1 (90), 2 (300).
    const ids = try collectBigints(allocator, db,
        \\SELECT p_id FROM part
        \\WHERE p_id < (SELECT SUM(l_qty) FROM li WHERE l_partid = p_id)
        \\ORDER BY p_id ASC
    );
    defer allocator.free(ids);
    // p_id < SUM(l_qty per p_id):
    //   p_id=1: 1 < 90  → TRUE
    //   p_id=2: 2 < 300 → TRUE
    //   p_id=3: 3 < 5   → TRUE
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, ids);
}

test "correlated scalar: missing inner key → row filtered" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE outer_t (k BIGINT PRIMARY KEY)");
    try exec(
        allocator,
        db,
        "CREATE TABLE inner_t (i_id BIGINT PRIMARY KEY, i_k BIGINT NOT NULL, i_v INT NOT NULL)",
    );
    try exec(allocator, db, "INSERT INTO outer_t (k) VALUES (1), (2), (3)");
    // Only key 1 and 2 have inner rows; key 3 has none.
    try exec(allocator, db, "INSERT INTO inner_t (i_id, i_k, i_v) VALUES (1, 1, 50), (2, 2, 100)");
    const t1 = try db.openTable("outer_t", .{});
    try t1.flush();
    const t2 = try db.openTable("inner_t", .{});
    try t2.flush();

    // Outer rows whose k has an inner with COUNT > 0 and k < that count's sum.
    // The materialized map: {1 → 50, 2 → 100}. Key 3 has no entry.
    // Predicate: k < (SUM ...)
    //   k=1: 1 < 50 → TRUE
    //   k=2: 2 < 100 → TRUE
    //   k=3: no key → FALSE
    const ids = try collectBigints(allocator, db,
        \\SELECT k FROM outer_t
        \\WHERE k < (SELECT SUM(i_v) FROM inner_t WHERE i_k = k)
        \\ORDER BY k ASC
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, ids);
}

test "correlated scalar: equality comparison" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // p_id = COUNT(li per partid):
    //   p_id=1: COUNT=3 → 1=3 → FALSE
    //   p_id=2: COUNT=2 → 2=2 → TRUE
    //   p_id=3: COUNT=1 → 3=1 → FALSE
    const ids = try collectBigints(allocator, db,
        \\SELECT p_id FROM part
        \\WHERE p_id = (SELECT COUNT(l_id) FROM li WHERE l_partid = p_id)
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{2}, ids);
}

test "correlated scalar: compared in WHERE and read in the SELECT list, by one LEFT JOIN" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, big BIGINT NOT NULL)");
    try exec(allocator, db, "INSERT INTO t (id, qty, big) VALUES (1, 1, 1), (2, 0, 0), (3, 5, 5), (4, 0, 0), (5, 2, 2)");
    try exec(allocator, db, "CREATE TABLE o (oid BIGINT PRIMARY KEY, tid BIGINT NOT NULL, amount INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO o (oid, tid, amount) VALUES (1, 1, 5), (2, 1, 7), (3, 3, 9)");

    // Per t.id: COUNT over o = {2, 0, 1, 0, 0}; MAX(amount) = {7, NULL, 9, NULL, NULL}.
    const cases = .{
        // An INT or BIGINT outer column against a BIGINT COUNT, where a key
        // with no inner rows counts 0.
        .{ "SELECT id FROM t WHERE qty > (SELECT COUNT(*) FROM o WHERE tid = id) ORDER BY id", &[_]i64{ 3, 5 } },
        .{ "SELECT id FROM t WHERE big > (SELECT COUNT(*) FROM o WHERE tid = id) ORDER BY id", &[_]i64{ 3, 5 } },
        .{ "SELECT id FROM t WHERE big = (SELECT COUNT(*) FROM o WHERE tid = id) ORDER BY id", &[_]i64{ 2, 4 } },
        // A key with no inner rows reads MAX as NULL, which matches nothing.
        .{ "SELECT id FROM t WHERE qty < (SELECT MAX(amount) FROM o WHERE tid = id) ORDER BY id", &[_]i64{ 1, 3 } },
        .{ "SELECT id FROM t WHERE big < (SELECT MAX(amount) FROM o WHERE tid = id) ORDER BY id", &[_]i64{ 1, 3 } },
        .{ "SELECT id FROM t WHERE id <= 3 AND big >= (SELECT COUNT(*) FROM o WHERE tid = id) ORDER BY id", &[_]i64{ 2, 3 } },
        .{ "SELECT id FROM t WHERE big = (SELECT COUNT(*) FROM o WHERE tid = id AND amount > 6) ORDER BY id", &[_]i64{ 1, 2, 4 } },
        .{ "SELECT (SELECT COUNT(*) FROM o WHERE tid = id) AS n FROM t ORDER BY id", &[_]i64{ 2, 0, 1, 0, 0 } },
        .{ "SELECT id * 100 + (SELECT COUNT(*) FROM o WHERE tid = id) AS v FROM t ORDER BY id", &[_]i64{ 102, 200, 301, 400, 500 } },
        .{ "SELECT x.id * 100 + (SELECT COUNT(*) FROM o p WHERE p.tid = x.id) AS v FROM t x ORDER BY x.id", &[_]i64{ 102, 200, 301, 400, 500 } },
        .{ "SELECT COALESCE((SELECT MAX(big) FROM t t2 WHERE t2.id = o.tid), 0) AS m FROM o ORDER BY oid", &[_]i64{ 1, 1, 5 } },
        .{ "SELECT id + (SELECT COUNT(*) FROM o WHERE tid = id) + (SELECT COUNT(*) FROM o WHERE tid = id AND amount > 6) AS n FROM t ORDER BY id", &[_]i64{ 4, 2, 5, 4, 5 } },
        .{ "SELECT CASE WHEN big > (SELECT COUNT(*) FROM o WHERE tid = id) THEN id ELSE 0 END AS c FROM t ORDER BY id", &[_]i64{ 0, 0, 3, 0, 5 } },
        .{ "SELECT (SELECT COUNT(*) FROM o WHERE tid = id) AS n FROM t GROUP BY id ORDER BY n", &[_]i64{ 0, 0, 0, 1, 2 } },
        .{ "SELECT id FROM t WHERE (SELECT COUNT(*) FROM o WHERE tid = id) = 0 ORDER BY id", &[_]i64{ 2, 4, 5 } },
        .{ "SELECT id FROM t WHERE (SELECT COUNT(*) FROM o WHERE tid = id) > 0 ORDER BY id", &[_]i64{ 1, 3 } },
        .{ "SELECT id FROM t WHERE (SELECT MAX(amount) FROM o) > big * 2 ORDER BY id", &[_]i64{ 1, 2, 4, 5 } },
        // Correlation refs qualified by the table names rather than aliases.
        .{ "SELECT t.id FROM t WHERE t.big = (SELECT COUNT(*) FROM o WHERE o.tid = t.id) ORDER BY t.id", &[_]i64{ 2, 4 } },
        .{ "SELECT (SELECT COUNT(*) FROM o WHERE o.tid = t.id) AS n FROM t ORDER BY t.id", &[_]i64{ 2, 0, 1, 0, 0 } },
    };
    inline for (cases) |case| {
        const got = try collectBigints(allocator, db, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, case[1], got);
    }

    // The join's key and value columns never reach the output.
    var q = try runSql(allocator, db, "SELECT *, (SELECT COUNT(*) FROM o WHERE tid = id) AS n FROM t ORDER BY id");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 4), batch.values.len);
    try std.testing.expectEqualSlices(i64, &.{ 2, 0, 1, 0, 0 }, batch.values[3].data.bigint[0..batch.row_count]);
}
