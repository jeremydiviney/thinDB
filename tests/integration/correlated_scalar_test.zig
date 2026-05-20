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
    try exec(allocator, db,
        "CREATE TABLE li (l_id BIGINT PRIMARY KEY, l_partid BIGINT NOT NULL, l_qty INT NOT NULL)",
    );
    try exec(allocator, db, "INSERT INTO part (p_id) VALUES (1), (2), (3)");
    // part 1: qty values {10, 30, 50}, avg = 30
    // part 2: qty values {100, 200},   avg = 150
    // part 3: qty values {5},          avg = 5
    try exec(allocator, db,
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
    try exec(allocator, db,
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
