//! Window function operator — end-to-end SQL tests for Tier 1.
//!
//! Driven through the SQL parser + compile path (the same one the
//! MySQL/PG wires use). Each test seeds a table, runs an OVER query,
//! and validates the projected window output column row by row.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const RunResult = helpers.RunResult;
const runSql = helpers.runSql;

fn collectRows(comptime T: type, allocator: std.mem.Allocator, q: *RunResult, col_idx: usize) ![]T {
    var out: std.ArrayList(T) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        switch (T) {
            i64 => for (b.values[col_idx].data.bigint[0..b.row_count]) |v|
                try out.append(allocator, v),
            i32 => for (b.values[col_idx].data.int[0..b.row_count]) |v|
                try out.append(allocator, v),
            f64 => for (b.values[col_idx].data.double[0..b.row_count]) |v|
                try out.append(allocator, v),
            else => unreachable,
        }
    }
    return try out.toOwnedSlice(allocator);
}

/// Validity bitmap reader — returns one bool per row (true = valid).
fn collectValidity(allocator: std.mem.Allocator, q: *RunResult, col_idx: usize) ![]bool {
    var out: std.ArrayList(bool) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        const nulls_opt = b.values[col_idx].nulls;
        var r: usize = 0;
        while (r < b.row_count) : (r += 1) {
            const valid = if (nulls_opt) |nb| (nb[r >> 3] & (@as(u8, 1) << @intCast(r & 7))) != 0 else true;
            try out.append(allocator, valid);
        }
    }
    return try out.toOwnedSlice(allocator);
}

fn seedSimple(allocator: std.mem.Allocator, db: anytype) !void {
    var q1 = try runSql(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, grp BIGINT, qty BIGINT)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(
        allocator,
        db,
        "INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 2, 100), (5, 2, 200)",
    );
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("t", .{});
    try t.flush();
}

test "window: ROW_NUMBER over single partition" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(
        allocator,
        db,
        "SELECT id, row_number() OVER (ORDER BY id ASC) AS rn FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const rns = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(rns);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4, 5 }, rns);
}

test "window: output named after an input column replaces it" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    // `... AS qty` must resolve to the window output, not the source column
    // it shadows (the rollforward name broadcast relies on this).
    var q = try runSql(
        allocator,
        db,
        "SELECT id, LAST_VALUE(qty) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS qty FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    try std.testing.expectEqual(@as(usize, 2), q.outputSchema().len);
    const qtys = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(qtys);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 30, 30, 30, 200, 200 }, qtys);
}

test "window: ROW_NUMBER with PARTITION BY" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(
        allocator,
        db,
        "SELECT id, row_number() OVER (PARTITION BY grp ORDER BY id ASC) AS rn FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const rns = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(rns);
    // grp=1: ids 1,2,3 → rn 1,2,3.  grp=2: ids 4,5 → rn 1,2.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 1, 2 }, rns);
}

test "window: RANK and DENSE_RANK with ties" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, score BIGINT)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(
        allocator,
        db,
        "INSERT INTO t VALUES (1, 90), (2, 90), (3, 80), (4, 70), (5, 70)",
    );
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(
        allocator,
        db,
        "SELECT id, rank() OVER (ORDER BY score DESC) AS rk, dense_rank() OVER (ORDER BY score DESC) AS drk FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const rks = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(rks);
    // q.next() consumed the batch; re-run for dense
    var q3 = try runSql(
        allocator,
        db,
        "SELECT id, dense_rank() OVER (ORDER BY score DESC) AS drk FROM t ORDER BY id ASC",
    );
    defer q3.deinit();
    const drks = try collectRows(i64, allocator, &q3, 1);
    defer allocator.free(drks);

    // scores sorted DESC: 90(id=1), 90(id=2), 80(id=3), 70(id=4), 70(id=5)
    // RANK:        1, 1, 3, 4, 4
    // DENSE_RANK:  1, 1, 2, 3, 3
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 3, 4, 4 }, rks);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 2, 3, 3 }, drks);
}

test "window: LAG with default 1-row offset and NULL fallback" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(
        allocator,
        db,
        "SELECT id, lag(qty) OVER (PARTITION BY grp ORDER BY id ASC) AS prev_qty FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    var qq = try runSql(
        allocator,
        db,
        "SELECT id, lag(qty) OVER (PARTITION BY grp ORDER BY id ASC) AS prev_qty FROM t ORDER BY id ASC",
    );
    defer qq.deinit();
    const valids = try collectValidity(allocator, &q, 1);
    defer allocator.free(valids);
    const vals = try collectRows(i64, allocator, &qq, 1);
    defer allocator.free(vals);

    // grp=1: id 1 (NULL), id 2 (10), id 3 (20)
    // grp=2: id 4 (NULL), id 5 (100)
    try std.testing.expectEqualSlices(bool, &[_]bool{ false, true, true, false, true }, valids);
    // valid positions carry the prev qty:
    try std.testing.expectEqual(@as(i64, 10), vals[1]);
    try std.testing.expectEqual(@as(i64, 20), vals[2]);
    try std.testing.expectEqual(@as(i64, 100), vals[4]);
}

test "window: LAG with literal default" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(
        allocator,
        db,
        "SELECT id, lag(qty, 1, 0) OVER (PARTITION BY grp ORDER BY id ASC) AS prev_qty FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const vals = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(vals);
    // grp=1: 0, 10, 20.  grp=2: 0, 100.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 0, 10, 20, 0, 100 }, vals);
}

test "window: LAG with column-ref default (StarRocks v4 extension)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    // LAG(qty, 1, qty) — at the first row of each partition, falls
    // back to the current row's qty (so a delta calc gives 0).
    var q = try runSql(
        allocator,
        db,
        "SELECT id, lag(qty, 1, qty) OVER (PARTITION BY grp ORDER BY id ASC) AS prev_qty FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const vals = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(vals);
    // grp=1: id1 → qty=10 (self), id2 → 10 (prev), id3 → 20 (prev)
    // grp=2: id4 → qty=100 (self), id5 → 100 (prev)
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 10, 20, 100, 100 }, vals);
}

test "window: LEAD with default offset" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(
        allocator,
        db,
        "SELECT id, lead(qty, 1, 0) OVER (PARTITION BY grp ORDER BY id ASC) AS next_qty FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const vals = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(vals);
    // grp=1: id1 → 20, id2 → 30, id3 → 0
    // grp=2: id4 → 200, id5 → 0
    try std.testing.expectEqualSlices(i64, &[_]i64{ 20, 30, 0, 200, 0 }, vals);
}

test "window: FIRST_VALUE per partition" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(
        allocator,
        db,
        "SELECT id, first_value(qty) OVER (PARTITION BY grp ORDER BY id ASC) AS first_q FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const vals = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(vals);
    // grp=1: all → 10.  grp=2: all → 100.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 10, 10, 100, 100 }, vals);
}

test "window: SUM as running total (default frame)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(
        allocator,
        db,
        "SELECT id, sum(qty) OVER (PARTITION BY grp ORDER BY id ASC) AS running_sum FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const vals = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(vals);
    // grp=1: 10, 30, 60.  grp=2: 100, 300.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 30, 60, 100, 300 }, vals);
}

test "window: SUM over whole partition (no ORDER BY)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(
        allocator,
        db,
        "SELECT id, sum(qty) OVER (PARTITION BY grp) AS total FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const vals = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(vals);
    // grp=1: all → 60.  grp=2: all → 300.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 60, 60, 60, 300, 300 }, vals);
}

test "window: COUNT(*) over partition" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(
        allocator,
        db,
        "SELECT id, count(*) OVER (PARTITION BY grp) AS n FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const vals = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(vals);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 3, 3, 2, 2 }, vals);
}

test "window: AVG as running average" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(
        allocator,
        db,
        "SELECT id, avg(qty) OVER (PARTITION BY grp ORDER BY id ASC) AS avg_so_far FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const vals = try collectRows(f64, allocator, &q, 1);
    defer allocator.free(vals);
    // grp=1: 10, 15, 20.  grp=2: 100, 150.
    const expected = [_]f64{ 10, 15, 20, 100, 150 };
    for (vals, expected) |got, want| try std.testing.expectApproxEqAbs(want, got, 1e-9);
}

test "window: MIN/MAX over running frame" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var qa = try runSql(
        allocator,
        db,
        "SELECT id, min(qty) OVER (PARTITION BY grp ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer qa.deinit();
    const mins = try collectRows(i64, allocator, &qa, 1);
    defer allocator.free(mins);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 10, 10, 100, 100 }, mins);

    var qb = try runSql(
        allocator,
        db,
        "SELECT id, max(qty) OVER (PARTITION BY grp ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer qb.deinit();
    const maxs = try collectRows(i64, allocator, &qb, 1);
    defer allocator.free(maxs);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20, 30, 100, 200 }, maxs);
}

test "window: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW (trailing window)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(
        allocator,
        db,
        "SELECT id, sum(qty) OVER (PARTITION BY grp ORDER BY id ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trailing FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const vals = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(vals);
    // grp=1: [10], [10+20]=30, [20+30]=50
    // grp=2: [100], [100+200]=300
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 30, 50, 100, 300 }, vals);
}

test "window: two calls sharing the same spec collapse to one sort" {
    // We can't observe sort count directly, but we can verify both
    // columns are correctly computed for the same (PARTITION, ORDER)
    // when the parser deduplicates them.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(allocator, db,
        \\SELECT id,
        \\  row_number() OVER (PARTITION BY grp ORDER BY id ASC) AS rn,
        \\  sum(qty)     OVER (PARTITION BY grp ORDER BY id ASC) AS rs
        \\FROM t ORDER BY id ASC
    );
    defer q.deinit();
    var q2 = try runSql(allocator, db,
        \\SELECT id,
        \\  row_number() OVER (PARTITION BY grp ORDER BY id ASC) AS rn,
        \\  sum(qty)     OVER (PARTITION BY grp ORDER BY id ASC) AS rs
        \\FROM t ORDER BY id ASC
    );
    defer q2.deinit();
    const rns = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(rns);
    const rss = try collectRows(i64, allocator, &q2, 2);
    defer allocator.free(rss);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 1, 2 }, rns);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 30, 60, 100, 300 }, rss);
}

test "window: LAG IGNORE NULLS skips null source rows when computing offset" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty BIGINT)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(
        allocator,
        db,
        "INSERT INTO t VALUES (1, 10), (2, NULL), (3, NULL), (4, 40), (5, 50)",
    );
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(
        allocator,
        db,
        "SELECT id, lag(qty) IGNORE NULLS OVER (ORDER BY id ASC) AS prev FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    var qv = try runSql(
        allocator,
        db,
        "SELECT id, lag(qty) IGNORE NULLS OVER (ORDER BY id ASC) AS prev FROM t ORDER BY id ASC",
    );
    defer qv.deinit();
    const valids = try collectValidity(allocator, &q, 1);
    defer allocator.free(valids);
    const vals = try collectRows(i64, allocator, &qv, 1);
    defer allocator.free(vals);

    // id=1: no prior → NULL
    // id=2: prior non-null is id=1 → 10
    // id=3: prior non-null is id=1 → 10 (NULL at id=2 skipped)
    // id=4: prior non-null is id=1 → 10
    // id=5: prior non-null is id=4 → 40
    try std.testing.expectEqualSlices(bool, &[_]bool{ false, true, true, true, true }, valids);
    try std.testing.expectEqual(@as(i64, 10), vals[1]);
    try std.testing.expectEqual(@as(i64, 10), vals[2]);
    try std.testing.expectEqual(@as(i64, 10), vals[3]);
    try std.testing.expectEqual(@as(i64, 40), vals[4]);
}

test "window: FIRST_VALUE IGNORE NULLS finds first non-null in partition" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty BIGINT)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(
        allocator,
        db,
        "INSERT INTO t VALUES (1, NULL), (2, NULL), (3, 30), (4, 40)",
    );
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(
        allocator,
        db,
        "SELECT id, first_value(qty) IGNORE NULLS OVER (ORDER BY id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fv FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const vals = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(vals);
    // All rows see the first non-null in the partition: 30.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 30, 30, 30, 30 }, vals);
}

test "window: IGNORE NULLS / RESPECT NULLS inside the call parentheses" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty BIGINT)");
    try helpers.exec(allocator, db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, NULL), (4, 40), (5, 50)");

    const cases = .{
        .{ "SELECT lag(qty IGNORE NULLS) OVER (ORDER BY id) FROM t ORDER BY id", &[_]?[]const u8{ null, "10", "10", "10", "40" } },
        .{ "SELECT lag(qty, 1) IGNORE NULLS OVER (ORDER BY id) FROM t ORDER BY id", &[_]?[]const u8{ null, "10", "10", "10", "40" } },
        .{ "SELECT lag(qty IGNORE NULLS, 2) OVER (ORDER BY id) FROM t ORDER BY id", &[_]?[]const u8{ null, null, null, null, "10" } },
        .{ "SELECT lag(qty IGNORE NULLS, 2, -1) OVER (ORDER BY id) FROM t ORDER BY id", &[_]?[]const u8{ "-1", "-1", "-1", "-1", "10" } },
        .{ "SELECT lead(qty IGNORE NULLS, 1, -1) OVER (ORDER BY id) FROM t ORDER BY id", &[_]?[]const u8{ "40", "40", "40", "50", "-1" } },
        .{ "SELECT lag(qty RESPECT NULLS, 1) OVER (ORDER BY id) FROM t ORDER BY id", &[_]?[]const u8{ null, "10", null, null, "40" } },
        .{ "SELECT last_value(qty IGNORE NULLS) OVER (ORDER BY id) FROM t ORDER BY id", &[_]?[]const u8{ "10", "10", "10", "40", "50" } },
        .{ "SELECT first_value(qty IGNORE NULLS) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) FROM t ORDER BY id", &[_]?[]const u8{ "40", "40", "40", "50", null } },
        .{ "SELECT coalesce(lag(qty IGNORE NULLS) OVER (ORDER BY id), 0) FROM t ORDER BY id", &[_]?[]const u8{ "0", "10", "10", "10", "40" } },
        .{ "SELECT CASE WHEN lag(qty IGNORE NULLS) OVER (ORDER BY id) > 10 THEN 1 ELSE 0 END FROM t ORDER BY id", &[_]?[]const u8{ "0", "0", "0", "0", "1" } },
    };
    inline for (cases) |c| try helpers.expectTexts(allocator, db, c[0], c[1]);

    const rejected = .{
        "SELECT lag(qty IGNORE NULLS) IGNORE NULLS OVER (ORDER BY id) FROM t",
        "SELECT lag(qty IGNORE NULLS, 1 IGNORE NULLS) OVER (ORDER BY id) FROM t",
        "SELECT sum(qty IGNORE NULLS) OVER (ORDER BY id) FROM t",
        "SELECT lag(qty IGNORE NULLS) FROM t",
        "SELECT abs(qty IGNORE NULLS) FROM t",
    };
    inline for (rejected) |sql| try helpers.expectRunError(allocator, db, sql, error.SqlInvalidProjection);
}

test "window: SUM / AVG / COUNT (DISTINCT) over a DECIMAL past 38 digits, and SUM / AVG over a BOOLEAN" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE w (id BIGINT PRIMARY KEY, g INT, b BOOLEAN, d DECIMAL(40, 2))");
    try helpers.exec(allocator, db, "INSERT INTO w VALUES (1, 1, true, 1.50), (2, 1, false, 2.25), (3, 1, true, 1.50), (4, 1, NULL, NULL), (5, 2, true, 10.00), (6, 2, true, 3.00)");

    const cases = .{
        .{ "SELECT SUM(DISTINCT d) OVER (PARTITION BY g ORDER BY id) FROM w ORDER BY id", &[_]?[]const u8{ "1.50", "3.75", "3.75", "3.75", "10.00", "13.00" } },
        .{ "SELECT SUM(DISTINCT d) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w ORDER BY id", &[_]?[]const u8{ "1.50", "3.75", "3.75", "1.50", "10.00", "13.00" } },
        .{ "SELECT SUM(DISTINCT d) OVER (PARTITION BY g) FROM w ORDER BY id", &[_]?[]const u8{ "3.75", "3.75", "3.75", "3.75", "13.00", "13.00" } },
        .{ "SELECT AVG(DISTINCT d) OVER (PARTITION BY g ORDER BY id) FROM w ORDER BY id", &[_]?[]const u8{ "1.5", "1.875", "1.875", "1.875", "10", "6.5" } },
        .{ "SELECT COUNT(DISTINCT d) OVER (PARTITION BY g ORDER BY id) FROM w ORDER BY id", &[_]?[]const u8{ "1", "2", "2", "2", "1", "2" } },
        .{ "SELECT SUM(b) OVER (PARTITION BY g ORDER BY id) FROM w ORDER BY id", &[_]?[]const u8{ "1", "1", "2", "2", "1", "2" } },
        .{ "SELECT SUM(b) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w ORDER BY id", &[_]?[]const u8{ "1", "1", "1", "1", "1", "2" } },
        .{ "SELECT AVG(b) OVER (PARTITION BY g ORDER BY id) FROM w ORDER BY id", &[_]?[]const u8{ "1", "0.5", "0.6666666666666666", "0.6666666666666666", "1", "1" } },
        .{ "SELECT AVG(b) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w ORDER BY id", &[_]?[]const u8{ "1", "0.5", "0.5", "1", "1", "1" } },
        .{ "SELECT SUM(DISTINCT b) OVER (PARTITION BY g ORDER BY id) FROM w ORDER BY id", &[_]?[]const u8{ "1", "1", "1", "1", "1", "1" } },
        .{ "SELECT AVG(DISTINCT b) OVER (PARTITION BY g ORDER BY id) FROM w ORDER BY id", &[_]?[]const u8{ "1", "0.5", "0.5", "0.5", "1", "1" } },
    };
    inline for (cases) |c| try helpers.expectTexts(allocator, db, c[0], c[1]);
}

test "window: NTILE(4) divides 10 rows into 4 buckets of 3,3,2,2" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(
        allocator,
        db,
        "INSERT INTO t VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)",
    );
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(
        allocator,
        db,
        "SELECT id, ntile(4) OVER (ORDER BY id ASC) AS bucket FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const buckets = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(buckets);
    // 10 rows, 4 buckets. N=10, n=4 → 10/4 = 2 small, 10%4 = 2 large.
    // Buckets sized: 3, 3, 2, 2.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 1, 2, 2, 2, 3, 3, 4, 4 }, buckets);
}

test "window: PERCENT_RANK + CUME_DIST on ties" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, score BIGINT)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(
        allocator,
        db,
        "INSERT INTO t VALUES (1, 10), (2, 20), (3, 20), (4, 30)",
    );
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("t", .{});
    try t.flush();

    // Sorted by score: 10, 20, 20, 30. Ranks (with-gaps): 1, 2, 2, 4.
    // PERCENT_RANK = (rank - 1) / (N - 1):
    //   id=1: 0/3 = 0.0
    //   id=2: 1/3 ≈ 0.333
    //   id=3: 1/3 ≈ 0.333  (peer)
    //   id=4: 3/3 = 1.0
    // CUME_DIST = rows_<=_current / N:
    //   id=1: 1/4 = 0.25
    //   id=2: 3/4 = 0.75 (peer)
    //   id=3: 3/4 = 0.75
    //   id=4: 4/4 = 1.0
    var qa = try runSql(
        allocator,
        db,
        "SELECT id, percent_rank() OVER (ORDER BY score ASC) AS pr FROM t ORDER BY id ASC",
    );
    defer qa.deinit();
    const pr = try collectRows(f64, allocator, &qa, 1);
    defer allocator.free(pr);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), pr[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0 / 3.0), pr[1], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0 / 3.0), pr[2], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), pr[3], 1e-9);

    var qb = try runSql(
        allocator,
        db,
        "SELECT id, cume_dist() OVER (ORDER BY score ASC) AS cd FROM t ORDER BY id ASC",
    );
    defer qb.deinit();
    const cd = try collectRows(f64, allocator, &qb, 1);
    defer allocator.free(cd);
    try std.testing.expectApproxEqAbs(@as(f64, 0.25), cd[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 0.75), cd[1], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 0.75), cd[2], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), cd[3], 1e-9);
}

test "window: NTH_VALUE returns NULL when n exceeds frame size, value otherwise" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    // NTH_VALUE(qty, 2) OVER (PARTITION BY grp ORDER BY id) with default
    // frame (UNBOUNDED PRECEDING TO CURRENT ROW). Row 1 of each partition
    // has only 1 row in its frame → NULL. Row 2+ sees the second value.
    var q = try runSql(
        allocator,
        db,
        "SELECT id, nth_value(qty, 2) OVER (PARTITION BY grp ORDER BY id ASC) AS second FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    var qv = try runSql(
        allocator,
        db,
        "SELECT id, nth_value(qty, 2) OVER (PARTITION BY grp ORDER BY id ASC) AS second FROM t ORDER BY id ASC",
    );
    defer qv.deinit();
    const valids = try collectValidity(allocator, &q, 1);
    defer allocator.free(valids);
    const vals = try collectRows(i64, allocator, &qv, 1);
    defer allocator.free(vals);
    // grp=1: id1 → NULL, id2 → 20 (second row's qty), id3 → 20
    // grp=2: id4 → NULL, id5 → 200
    try std.testing.expectEqualSlices(bool, &[_]bool{ false, true, true, false, true }, valids);
    try std.testing.expectEqual(@as(i64, 20), vals[1]);
    try std.testing.expectEqual(@as(i64, 20), vals[2]);
    try std.testing.expectEqual(@as(i64, 200), vals[4]);
}

test "window: LAG on string column round-trips bytes" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(
        allocator,
        db,
        "INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')",
    );
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(
        allocator,
        db,
        "SELECT id, lag(name) OVER (ORDER BY id ASC) AS prev FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    var batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), batch.row_count);
    const nulls = batch.values[1].nulls.?;
    // id=1 → NULL; id=2 → "alice"; id=3 → "bob"
    try std.testing.expectEqual(false, (nulls[0] & 1) != 0);
    try std.testing.expectEqual(true, (nulls[0] & 2) != 0);
    try std.testing.expectEqual(true, (nulls[0] & 4) != 0);
    try std.testing.expectEqualStrings("alice", batch.values[1].data.string.rowBytes(1));
    try std.testing.expectEqualStrings("bob", batch.values[1].data.string.rowBytes(2));
}

test "window: MIN over string column finds lexicographically smallest" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, grp BIGINT, name TEXT NOT NULL)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(
        allocator,
        db,
        "INSERT INTO t VALUES (1, 1, 'zeta'), (2, 1, 'alpha'), (3, 1, 'mu'), (4, 2, 'omega'), (5, 2, 'beta')",
    );
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(
        allocator,
        db,
        "SELECT id, min(name) OVER (PARTITION BY grp) AS smallest FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    var batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 5), batch.row_count);
    // grp=1: min lex = "alpha". grp=2: "beta".
    try std.testing.expectEqualStrings("alpha", batch.values[1].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("alpha", batch.values[1].data.string.rowBytes(1));
    try std.testing.expectEqualStrings("alpha", batch.values[1].data.string.rowBytes(2));
    try std.testing.expectEqualStrings("beta", batch.values[1].data.string.rowBytes(3));
    try std.testing.expectEqualStrings("beta", batch.values[1].data.string.rowBytes(4));
}

test "window: QUALIFY filters on window output alias" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    // Per-partition rank by qty DESC, keep only the top row of each.
    var q = try runSql(allocator, db,
        \\SELECT id, rank() OVER (PARTITION BY grp ORDER BY qty DESC) AS rk
        \\FROM t
        \\QUALIFY rk = 1
        \\ORDER BY id ASC
    );
    defer q.deinit();
    var rows: std.ArrayList(i64) = .empty;
    defer rows.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try rows.append(allocator, v);
    }
    // grp=1: top qty is 30 → id=3.  grp=2: top is 200 → id=5.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 5 }, rows.items);
}

test "window: named window via WINDOW clause" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(allocator, db,
        \\SELECT id, row_number() OVER w AS rn, sum(qty) OVER w AS rs
        \\FROM t
        \\WINDOW w AS (PARTITION BY grp ORDER BY id ASC)
        \\ORDER BY id ASC
    );
    defer q.deinit();
    var q2 = try runSql(allocator, db,
        \\SELECT id, row_number() OVER w AS rn, sum(qty) OVER w AS rs
        \\FROM t
        \\WINDOW w AS (PARTITION BY grp ORDER BY id ASC)
        \\ORDER BY id ASC
    );
    defer q2.deinit();
    const rns = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(rns);
    const rss = try collectRows(i64, allocator, &q2, 2);
    defer allocator.free(rss);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 1, 2 }, rns);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 30, 60, 100, 300 }, rss);
}

test "window: leading window call continued by arithmetic hoists to hidden column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(allocator, db,
        \\SELECT id, SUM(qty) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / 2 AS h
        \\FROM t
        \\ORDER BY id ASC
    );
    defer q.deinit();
    var rows: std.ArrayList(f64) = .empty;
    defer rows.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[1].data.double[0..b.row_count]) |v| try rows.append(allocator, v);
    }
    // Running sums 10,30,60 | 100,300 halved; `/` is true division (double).
    try std.testing.expectEqualSlices(f64, &[_]f64{ 5, 15, 30, 50, 150 }, rows.items);
}

test "window: SUM and AVG over DECIMAL inputs (running and framed)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const helpers2 = @import("sql_helpers.zig");
    try helpers2.exec(allocator, db, "CREATE TABLE dts (id BIGINT PRIMARY KEY, grp BIGINT, amt DECIMAL(16,6))");
    try helpers2.exec(allocator, db, "INSERT INTO dts VALUES (1, 1, '1.500000'), (2, 1, '2.250000'), (3, 1, '3.000000')");

    var q = try runSql(allocator, db, "SELECT id, SUM(amt) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rs, AVG(amt) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS fa FROM dts ORDER BY id ASC");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), b.row_count);
    // Running sums at scale 6: 1.5, 3.75, 6.75.
    try std.testing.expectEqual(@as(i128, 1_500_000), b.values[1].data.decimal128[0]);
    try std.testing.expectEqual(@as(i128, 3_750_000), b.values[1].data.decimal128[1]);
    try std.testing.expectEqual(@as(i128, 6_750_000), b.values[1].data.decimal128[2]);
    // Framed averages: 1.5, 1.875, 2.625.
    try std.testing.expectApproxEqAbs(@as(f64, 1.5), b.values[2].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 1.875), b.values[2].data.double[1], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 2.625), b.values[2].data.double[2], 1e-9);
}

test "window: LAG over DECIMAL with integer default, and CASE mixing decimal with int literal" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const helpers3 = @import("sql_helpers.zig");
    try helpers3.exec(allocator, db, "CREATE TABLE dl (id BIGINT PRIMARY KEY, grp BIGINT, amt DECIMAL(10,2))");
    try helpers3.exec(allocator, db, "INSERT INTO dl VALUES (1, 1, '1.50'), (2, 1, '2.25'), (3, 1, '3.75')");

    var q = try runSql(allocator, db, "SELECT id, LAG(amt, 1, 0) OVER (PARTITION BY grp ORDER BY id) AS prev, LAG(CASE WHEN id > 1 THEN amt ELSE 0 END, 1, 0) OVER (PARTITION BY grp ORDER BY id) AS prev_gated FROM dl ORDER BY id ASC");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), b.row_count);
    // prev at scale 2: 0.00, 1.50, 2.25
    try std.testing.expectEqual(@as(i64, 0), b.values[1].data.decimal64[0]);
    try std.testing.expectEqual(@as(i64, 150), b.values[1].data.decimal64[1]);
    try std.testing.expectEqual(@as(i64, 225), b.values[1].data.decimal64[2]);
    // prev_gated: 0.00 (default), 0.00 (id=1 gated to 0), 2.25
    try std.testing.expectEqual(@as(i64, 0), b.values[2].data.decimal64[0]);
    try std.testing.expectEqual(@as(i64, 0), b.values[2].data.decimal64[1]);
    try std.testing.expectEqual(@as(i64, 225), b.values[2].data.decimal64[2]);
}

test "window: a narrowed CTE keeps a window column when every derived sibling dies" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(
        allocator,
        db,
        "WITH c AS (SELECT id, IF(qty = 0, 0, 1) AS h, IF(qty = 1, 0, 1) AS h2, IF(qty = 2, 0, 1) AS h3, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY qty) AS rn2 FROM t) SELECT MAX(rn) AS n FROM c",
    );
    defer q.deinit();
    const ns = try collectRows(i64, allocator, &q, 0);
    defer allocator.free(ns);
    try std.testing.expectEqualSlices(i64, &[_]i64{3}, ns);
}

test "window: ORDER BY keys whose predicates compare computed operands" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(
        allocator,
        db,
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY CASE WHEN qty % 20 = 0 THEN 0 ELSE 1 END, id DESC) AS rn, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY CASE WHEN CASE WHEN qty > 15 THEN qty ELSE 0 END > 0 THEN 0 ELSE 1 END, id) AS rn2 FROM t ORDER BY id",
    );
    defer q.deinit();
    var rn: std.ArrayList(i64) = .empty;
    defer rn.deinit(allocator);
    var rn2: std.ArrayList(i64) = .empty;
    defer rn2.deinit(allocator);
    while (try q.next()) |b| {
        try rn.appendSlice(allocator, b.values[1].data.bigint[0..b.row_count]);
        try rn2.appendSlice(allocator, b.values[2].data.bigint[0..b.row_count]);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 1, 2, 2, 1 }, rn.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 1, 2, 1, 2 }, rn2.items);
}

test "window: VAR_*, STDDEV_*, COVAR_* and CORR, over every frame shape, as their aggregates" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE st (k BIGINT PRIMARY KEY, g BIGINT, v DOUBLE, y INT, d DECIMAL(10,2))");
    try helpers.exec(allocator, db, "INSERT INTO st VALUES (1, 1, 1.5, 2, 1.25), (2, 1, 2.5, 4, 2.50), (3, 1, NULL, 5, NULL), (4, 2, 10, 1, 10.00), (5, 2, 11, 3, 11.10), (6, 2, 14, 2, 14.75)");
    const expectTexts = helpers.expectTexts;

    // A whole partition gives its GROUP BY value.
    try expectTexts(allocator, db, "SELECT ROUND(VAR_SAMP(v) OVER (PARTITION BY g), 3) FROM st ORDER BY k", &.{ "0.5", "0.5", "0.5", "4.333", "4.333", "4.333" });
    try expectTexts(allocator, db, "SELECT ROUND(VAR_SAMP(v), 3) FROM st GROUP BY g ORDER BY g", &.{ "0.5", "4.333" });
    try expectTexts(allocator, db, "SELECT ROUND(VARIANCE(v) OVER (PARTITION BY g), 3) FROM st ORDER BY k", &.{ "0.25", "0.25", "0.25", "2.889", "2.889", "2.889" });
    try expectTexts(allocator, db, "SELECT ROUND(STDDEV(v) OVER (PARTITION BY g), 3) FROM st ORDER BY k", &.{ "0.5", "0.5", "0.5", "1.7", "1.7", "1.7" });
    try expectTexts(allocator, db, "SELECT ROUND(STDDEV_SAMP(v) OVER (PARTITION BY g), 3) FROM st ORDER BY k", &.{ "0.707", "0.707", "0.707", "2.082", "2.082", "2.082" });
    try expectTexts(allocator, db, "SELECT ROUND(COVAR_POP(v, y) OVER (PARTITION BY g), 3) FROM st ORDER BY k", &.{ "0.5", "0.5", "0.5", "0.333", "0.333", "0.333" });
    try expectTexts(allocator, db, "SELECT ROUND(COVAR_SAMP(v, y) OVER (PARTITION BY g), 3) FROM st ORDER BY k", &.{ "1", "1", "1", "0.5", "0.5", "0.5" });
    try expectTexts(allocator, db, "SELECT ROUND(CORR(v, y) OVER (PARTITION BY g), 3) FROM st ORDER BY k", &.{ "1", "1", "1", "0.24", "0.24", "0.24" });
    // A DECIMAL reads at its scale.
    try expectTexts(allocator, db, "SELECT VAR_SAMP(d) OVER (PARTITION BY g) FROM st ORDER BY k", &.{ "0.78125", "0.78125", "0.78125", "6.1825", "6.1825", "6.1825" });
    try expectTexts(allocator, db, "SELECT ROUND(c, 6) FROM (SELECT k, CORR(d, y) OVER () AS c FROM st) x WHERE k = 1", &.{"-0.367677"});

    // A running frame, and RANGE peers, which share their last row's value.
    try expectTexts(allocator, db, "SELECT ROUND(VAR_POP(v) OVER (PARTITION BY g ORDER BY k), 3) FROM st ORDER BY k", &.{ "0", "0.25", "0.25", "0", "0.25", "2.889" });
    try expectTexts(allocator, db, "SELECT ROUND(VAR_SAMP(v) OVER (PARTITION BY g ORDER BY k), 3) FROM st ORDER BY k", &.{ null, "0.5", "0.5", null, "0.5", "4.333" });
    try expectTexts(allocator, db, "SELECT ROUND(STDDEV_POP(y) OVER (ORDER BY g RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 3) FROM st ORDER BY k", &.{ "1.247", "1.247", "1.247", "1.344", "1.344", "1.344" });
    // Sliding frames, where NULLs and too few values give NULL.
    try expectTexts(allocator, db, "SELECT ROUND(VAR_SAMP(v) OVER (ORDER BY k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 3) FROM st ORDER BY k", &.{ null, "0.5", "0.5", "28.125", "0.5", "4.333" });
    try expectTexts(allocator, db, "SELECT ROUND(CORR(v, y) OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), 3) FROM st ORDER BY k", &.{ "1", "1", "-1", "1", "0.24", "-1" });
    try expectTexts(allocator, db, "SELECT VAR_SAMP(k) OVER (PARTITION BY g ORDER BY k DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM st ORDER BY k", &.{ null, "0.5", "1", null, "0.5", "1" });

    // Over an expression, in an expression, and through a named window.
    try expectTexts(allocator, db, "SELECT ROUND(VAR_SAMP(v * 2) OVER (PARTITION BY g), 3) FROM st ORDER BY k", &.{ "2", "2", "2", "17.333", "17.333", "17.333" });
    try expectTexts(allocator, db, "SELECT ROUND(VAR_SAMP(v) OVER (PARTITION BY g) / 2, 3) FROM st ORDER BY k", &.{ "0.25", "0.25", "0.25", "2.167", "2.167", "2.167" });
    try expectTexts(allocator, db, "SELECT CASE WHEN VAR_SAMP(v) OVER w > 1 THEN 'wide' ELSE 'narrow' END FROM st WINDOW w AS (PARTITION BY g) ORDER BY k", &.{ "narrow", "narrow", "narrow", "wide", "wide", "wide" });

    try std.testing.expectError(error.AggregateUnsupportedType, helpers.exec(allocator, db, "SELECT VAR_SAMP(s) OVER () FROM (SELECT CAST(k AS VARCHAR(5)) AS s FROM st) x"));
    try std.testing.expectError(error.SqlInvalidProjection, helpers.exec(allocator, db, "SELECT CORR(v) OVER () FROM st"));
    try std.testing.expectError(error.SqlInvalidProjection, helpers.exec(allocator, db, "SELECT VAR_SAMP(v, y) OVER () FROM st"));
}

test "window: a call over an expression needs no alias" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    // Its default name, `sum(add(...))`, holds dots, which read as a struct
    // path unless the window owns the name.
    var q = try runSql(allocator, db, "SELECT id, sum(qty + 1) OVER (PARTITION BY grp) FROM t ORDER BY id ASC");
    defer q.deinit();
    const sums = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(sums);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 63, 63, 63, 302, 302 }, sums);
}

test "window: SESSION_NUMBER starts a session where x jumps by more than delta" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE sn (k BIGINT PRIMARY KEY, g BIGINT, ts BIGINT, x TINYINT, s VARCHAR(5))");
    try helpers.exec(allocator, db, "INSERT INTO sn VALUES (1, 1, 10, 1, 'a'), (2, 1, 12, 2, 'b'), (3, 1, 20, NULL, 'c'), (4, 1, 21, 9, 'd'), (5, 2, 5, 3, 'e'), (6, 2, 100, 3, 'f'), (7, 2, NULL, 4, 'g'), (8, 2, 101, 4, 'h')");
    const expectTexts = helpers.expectTexts;

    // A gap of exactly delta stays in the session, and a NULL x stays in it
    // without moving the last x.
    try expectTexts(allocator, db, "SELECT SESSION_NUMBER(ts, 2) OVER (PARTITION BY g ORDER BY k) FROM sn ORDER BY k", &.{ "1", "1", "2", "2", "1", "2", "2", "2" });
    try expectTexts(allocator, db, "SELECT SESSION_NUMBER(ts, 0) OVER (ORDER BY k) FROM sn ORDER BY k", &.{ "1", "2", "3", "4", "4", "5", "5", "6" });
    // The gap is signed: a fall never starts a session, unless delta is negative.
    try expectTexts(allocator, db, "SELECT SESSION_NUMBER(x, 1) OVER (PARTITION BY g ORDER BY k DESC) FROM sn ORDER BY k", &.{ "1", "1", "1", "1", "1", "1", "1", "1" });
    try expectTexts(allocator, db, "SELECT SESSION_NUMBER(ts, -1) OVER (ORDER BY ts) FROM sn ORDER BY k", &.{ "2", "3", "4", "5", "1", "6", "1", "7" });
    try expectTexts(allocator, db, "SELECT SESSION_NUMBER(ts, 9223372036854775807) OVER (ORDER BY k) FROM sn ORDER BY k", &.{ "1", "1", "1", "1", "1", "1", "1", "1" });
    // Over an expression, ignoring a frame, and NULL for a NULL argument.
    try expectTexts(allocator, db, "SELECT SESSION_NUMBER(ts + 1, 5) OVER (PARTITION BY g ORDER BY k) FROM sn ORDER BY k", &.{ "1", "1", "2", "2", "1", "2", "2", "2" });
    try expectTexts(allocator, db, "SELECT SESSION_NUMBER(ts, 2) OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM sn ORDER BY k", &.{ "1", "1", "2", "2", "2", "3", "3", "3" });
    try expectTexts(allocator, db, "SELECT SESSION_NUMBER(NULL, NULL) OVER (ORDER BY k) FROM sn WHERE k < 3 ORDER BY k", &.{ null, null });
    try expectTexts(allocator, db, "SELECT SESSION_NUMBER(ts, NULL) OVER (ORDER BY k) FROM sn WHERE k < 3 ORDER BY k", &.{ null, null });
    try expectTexts(allocator, db, "SELECT SUM(n) FROM (SELECT SESSION_NUMBER(ts, 2) OVER (PARTITION BY g ORDER BY k) AS n FROM sn) x", &.{"13"});

    try std.testing.expectError(error.AggregateUnsupportedType, helpers.exec(allocator, db, "SELECT SESSION_NUMBER(s, 2) OVER (ORDER BY k) FROM sn"));
    try std.testing.expectError(error.WindowUnsupported, helpers.exec(allocator, db, "SELECT SESSION_NUMBER(ts, g) OVER (ORDER BY k) FROM sn"));
    try std.testing.expectError(error.SqlInvalidProjection, helpers.exec(allocator, db, "SELECT SESSION_NUMBER(ts) OVER (ORDER BY k) FROM sn"));
}

test "window: COUNT, SUM and AVG of DISTINCT values over every frame shape" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE dw (k BIGINT PRIMARY KEY, g BIGINT, v INT, d DECIMAL(6,2), f DOUBLE, s VARCHAR(5))");
    try helpers.exec(allocator, db, "INSERT INTO dw VALUES (1, 1, 5, 1.50, 0.0, 'a'), (2, 1, 5, 1.50, -0.0, 'a'), (3, 1, 7, 2.25, NULL, 'b'), (4, 2, 1, NULL, 2.5, NULL), (5, 2, 1, 3.00, 2.5, 'c'), (6, 2, NULL, 3.00, 1.0, 'c'), (7, 2, 4, 1.00, 1.0, 'd')");
    const expectTexts = helpers.expectTexts;

    // A whole partition, beside the plain aggregate that counts repeats.
    try expectTexts(allocator, db, "SELECT COUNT(DISTINCT v) OVER (PARTITION BY g) FROM dw ORDER BY k", &.{ "2", "2", "2", "2", "2", "2", "2" });
    try expectTexts(allocator, db, "SELECT COUNT(v) OVER (PARTITION BY g) FROM dw ORDER BY k", &.{ "3", "3", "3", "3", "3", "3", "3" });
    try expectTexts(allocator, db, "SELECT SUM(DISTINCT v) OVER (PARTITION BY g) FROM dw ORDER BY k", &.{ "12", "12", "12", "5", "5", "5", "5" });
    try expectTexts(allocator, db, "SELECT AVG(DISTINCT v) OVER (PARTITION BY g) FROM dw ORDER BY k", &.{ "6", "6", "6", "2.5", "2.5", "2.5", "2.5" });
    // Running frames, by rows and by peers.
    try expectTexts(allocator, db, "SELECT COUNT(DISTINCT v) OVER (PARTITION BY g ORDER BY k) FROM dw ORDER BY k", &.{ "1", "1", "2", "1", "1", "1", "2" });
    try expectTexts(allocator, db, "SELECT SUM(DISTINCT v) OVER (PARTITION BY g ORDER BY k) FROM dw ORDER BY k", &.{ "5", "5", "12", "1", "1", "1", "5" });
    try expectTexts(allocator, db, "SELECT COUNT(DISTINCT v) OVER (ORDER BY g RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM dw ORDER BY k", &.{ "2", "2", "2", "4", "4", "4", "4" });
    // Sliding frames, and frames past the partition's end.
    try expectTexts(allocator, db, "SELECT COUNT(DISTINCT v) OVER (PARTITION BY g ORDER BY k ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM dw ORDER BY k", &.{ "1", "2", "2", "1", "1", "2", "1" });
    try expectTexts(allocator, db, "SELECT SUM(DISTINCT v) OVER (ORDER BY k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM dw ORDER BY k", &.{ "5", "5", "12", "13", "8", "1", "5" });
    try expectTexts(allocator, db, "SELECT COUNT(DISTINCT v) OVER (ORDER BY k ROWS BETWEEN 3 FOLLOWING AND 4 FOLLOWING) FROM dw ORDER BY k", &.{ "1", "1", "1", "1", "0", "0", "0" });
    // DECIMAL sums keep their scale; -0 and 0 are one value; text counts.
    try expectTexts(allocator, db, "SELECT CAST(SUM(DISTINCT d) OVER (PARTITION BY g) AS VARCHAR(10)) FROM dw ORDER BY k", &.{ "3.75", "3.75", "3.75", "4.00", "4.00", "4.00", "4.00" });
    try expectTexts(allocator, db, "SELECT AVG(DISTINCT d) OVER (PARTITION BY g) FROM dw ORDER BY k", &.{ "1.875", "1.875", "1.875", "2", "2", "2", "2" });
    try expectTexts(allocator, db, "SELECT COUNT(DISTINCT f) OVER (PARTITION BY g) FROM dw ORDER BY k", &.{ "1", "1", "1", "2", "2", "2", "2" });
    try expectTexts(allocator, db, "SELECT SUM(DISTINCT f) OVER (PARTITION BY g) FROM dw ORDER BY k", &.{ "0", "0", "0", "3.5", "3.5", "3.5", "3.5" });
    try expectTexts(allocator, db, "SELECT COUNT(DISTINCT s) OVER () FROM dw ORDER BY k", &.{ "4", "4", "4", "4", "4", "4", "4" });
    // MIN and MAX ignore DISTINCT; an expression, and one in an expression.
    try expectTexts(allocator, db, "SELECT MIN(DISTINCT v) OVER (PARTITION BY g) FROM dw ORDER BY k", &.{ "5", "5", "5", "1", "1", "1", "1" });
    try expectTexts(allocator, db, "SELECT COUNT(DISTINCT v + 1) OVER (PARTITION BY g) FROM dw ORDER BY k", &.{ "2", "2", "2", "2", "2", "2", "2" });
    try expectTexts(allocator, db, "SELECT 10 + COUNT(DISTINCT v) OVER (PARTITION BY g) FROM dw ORDER BY k", &.{ "12", "12", "12", "12", "12", "12", "12" });
    try expectTexts(allocator, db, "SELECT CASE WHEN COUNT(DISTINCT v) OVER (ORDER BY k) > 2 THEN 'many' ELSE 'few' END FROM dw ORDER BY k", &.{ "few", "few", "few", "many", "many", "many", "many" });

    try std.testing.expectError(error.WindowUnsupported, helpers.exec(allocator, db, "SELECT SUM(DISTINCT s) OVER () FROM dw"));
    try std.testing.expectError(error.WindowUnsupported, helpers.exec(allocator, db, "SELECT AVG(DISTINCT s) OVER () FROM dw"));
    try std.testing.expectError(error.SqlInvalidProjection, helpers.exec(allocator, db, "SELECT COUNT(DISTINCT *) OVER () FROM dw"));
    try std.testing.expectError(error.SqlInvalidProjection, helpers.exec(allocator, db, "SELECT VAR_SAMP(DISTINCT v) OVER () FROM dw"));
    // The distinct (v, g) pairs with no NULL, as GROUP BY counts them.
    try expectTexts(allocator, db, "SELECT COUNT(DISTINCT v, g) OVER () FROM dw ORDER BY k", &.{ "4", "4", "4", "4", "4", "4", "4" });
}

test "window: SELECT DISTINCT dedups the rows the windows annotate" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE sdw (k BIGINT PRIMARY KEY, g BIGINT, v INT)");
    try helpers.exec(allocator, db, "INSERT INTO sdw VALUES (1, 1, 5), (2, 1, 5), (3, 1, 7), (4, 2, 1), (5, 2, 1), (6, 2, NULL), (7, 2, 4)");

    const cases = .{
        .{ "SELECT DISTINCT g, COUNT(v) OVER (PARTITION BY g) AS c FROM sdw ORDER BY g", &[_]?i64{ 1, 3, 2, 3 } },
        .{ "SELECT DISTINCT v, COUNT(*) OVER (PARTITION BY v) AS c FROM sdw ORDER BY v", &[_]?i64{ null, 1, 1, 2, 4, 1, 5, 2, 7, 1 } },
        .{ "SELECT DISTINCT g, MAX(v) OVER (PARTITION BY g) - MIN(v) OVER (PARTITION BY g) AS spread FROM sdw ORDER BY g", &[_]?i64{ 1, 2, 2, 3 } },
        .{ "SELECT DISTINCT COUNT(v) OVER (PARTITION BY g) AS c FROM sdw ORDER BY c", &[_]?i64{3} },
        .{ "SELECT DISTINCT g, SUM(v) OVER (PARTITION BY g) AS s FROM sdw ORDER BY g DESC LIMIT 1", &[_]?i64{ 2, 6 } },
        .{ "SELECT COUNT(*) FROM (SELECT DISTINCT g, ROW_NUMBER() OVER (ORDER BY k) AS r FROM sdw) t", &[_]?i64{7} },
        .{ "WITH d AS (SELECT DISTINCT g, COUNT(*) OVER (PARTITION BY g) AS c FROM sdw) SELECT g, c FROM d ORDER BY g", &[_]?i64{ 1, 3, 2, 4 } },
    };
    inline for (cases) |c| {
        var q = try helpers.runSql(allocator, db, c[0]);
        defer q.deinit();
        const got = try helpers.collectIntCells(allocator, &q);
        defer allocator.free(got);
        errdefer std.debug.print("query: {s}\n", .{c[0]});
        try std.testing.expectEqualSlices(?i64, c[1], got);
    }
    try std.testing.expectError(error.SqlInvalidProjection, helpers.exec(allocator, db, "SELECT DISTINCT *, ROW_NUMBER() OVER () FROM sdw"));
}

test "window: a LAG/LEAD default of another type or an expression reads as the value's type" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE cd (c0 INT PRIMARY KEY, c1 INT, c2 VARCHAR(20), c5 DECIMAL(10, 2), c6 DATE)");
    try helpers.exec(allocator, db, "INSERT INTO cd VALUES (1, 10, 'a', 3.33, '2021-11-11'), (2, 20, 'aa', 4.33, '2022-12-11'), (3, 30, 'aaa', 5.33, '2022-06-11')");

    const cases = .{
        .{ "SELECT lag(c1, 1, 1 + 2 + 3) OVER (ORDER BY c0) FROM cd ORDER BY c0", &[_]?[]const u8{ "6", "10", "20" } },
        .{ "SELECT lead(c2, 1, 1 + 2 + 3) OVER (ORDER BY c0) FROM cd ORDER BY c0", &[_]?[]const u8{ "aa", "aaa", "6" } },
        .{ "SELECT lead(c2, 5, day('2021-01-02')) OVER (ORDER BY c0) FROM cd ORDER BY c0", &[_]?[]const u8{ "2", "2", "2" } },
        .{ "SELECT CAST(lag(c6, 1, '2020-01-01') OVER (ORDER BY c0) AS STRING) FROM cd ORDER BY c0", &[_]?[]const u8{ "2020-01-01", "2021-11-11", "2022-12-11" } },
        .{ "SELECT CAST(lead(c5, 1, '12345') OVER (ORDER BY c0) AS STRING) FROM cd ORDER BY c0", &[_]?[]const u8{ "4.33", "5.33", "12345.00" } },
        .{ "SELECT lag(c1, 1, c0 * 100) OVER (ORDER BY c0) FROM cd ORDER BY c0", &[_]?[]const u8{ "100", "10", "20" } },
        .{ "SELECT lag(c2, 1, c0) OVER (ORDER BY c0) FROM cd ORDER BY c0", &[_]?[]const u8{ "1", "a", "aa" } },
        .{ "SELECT lag(c1, 1, c5) OVER (ORDER BY c0) FROM cd ORDER BY c0", &[_]?[]const u8{ "3", "10", "20" } },
        .{ "SELECT lag(c1, 1, NULL) OVER (ORDER BY c0) FROM cd ORDER BY c0", &[_]?[]const u8{ null, "10", "20" } },
    };
    inline for (cases) |c| try helpers.expectTexts(allocator, db, c[0], c[1]);

    var star = try runSql(allocator, db, "SELECT *, lag(c1, 1, 1 + 2 + 3) OVER (ORDER BY c0) AS l FROM cd");
    defer star.deinit();
    try std.testing.expectEqual(@as(usize, 6), star.outputSchema().len);
}

test "window: ROWS from UNBOUNDED PRECEDING to n PRECEDING or FOLLOWING folds each row's frame" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE ws (k BIGINT PRIMARY KEY, g INT, v INT, d DECIMAL(10, 2))");
    try helpers.exec(allocator, db, "INSERT INTO ws SELECT x, x % 3, IF(x % 7 = 0, NULL, (x * 37) % 23), CAST((x * 13) % 11 AS DECIMAL(10, 2)) / 4 FROM generate_series(1, 60) t(x)");

    try helpers.expectTexts(allocator, db, "SELECT SUM(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) FROM ws WHERE k <= 5 ORDER BY k", &.{ null, null, "14", "19", "38" });
    try helpers.expectTexts(allocator, db, "SELECT COUNT(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) FROM ws WHERE k <= 5 ORDER BY k", &.{ "0", "0", "1", "2", "3" });
    try helpers.expectTexts(allocator, db, "SELECT MAX(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM ws WHERE k <= 5 ORDER BY k", &.{ "14", "19", "19", "19", "19" });

    // Each answer matches the same frame with a bounded start, which folds
    // the frame row by row.
    const aggs = .{ "SUM(v)", "AVG(v)", "COUNT(*)", "COUNT(v)", "MIN(v)", "MAX(v)", "STDDEV_SAMP(v)", "COUNT(DISTINCT v)", "SUM(DISTINCT v)", "SUM(d)", "AVG(d)" };
    const ends = .{ "1 PRECEDING", "2 PRECEDING", "100 PRECEDING", "3 FOLLOWING", "100 FOLLOWING" };
    const partitions = .{ "ORDER BY k", "PARTITION BY g ORDER BY k" };
    inline for (aggs) |agg| inline for (ends) |end| inline for (partitions) |partition| {
        const shifted = "SELECT CAST(" ++ agg ++ " OVER (" ++ partition ++ " ROWS BETWEEN UNBOUNDED PRECEDING AND " ++ end ++ ") AS STRING) FROM ws ORDER BY k";
        const bounded = "SELECT CAST(" ++ agg ++ " OVER (" ++ partition ++ " ROWS BETWEEN 1000 PRECEDING AND " ++ end ++ ") AS STRING) FROM ws ORDER BY k";
        const got = try helpers.collectStrings(allocator, db, shifted);
        defer helpers.freeStrings(allocator, got);
        const want = try helpers.collectStrings(allocator, db, bounded);
        defer helpers.freeStrings(allocator, want);
        try std.testing.expectEqual(want.len, got.len);
        for (want, got) |w, g| {
            if (w == null or g == null) {
                std.testing.expect(w == null and g == null) catch |err| {
                    std.debug.print("query: {s}\n", .{shifted});
                    return err;
                };
            } else std.testing.expectEqualStrings(w.?, g.?) catch |err| {
                std.debug.print("query: {s}\n", .{shifted});
                return err;
            };
        }
    };
}

test "window: any other aggregate runs over each row's frame" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE aw (k BIGINT PRIMARY KEY, g BIGINT, v INT, s VARCHAR(5), b BOOLEAN)");
    try helpers.exec(allocator, db, "INSERT INTO aw VALUES (1, 1, 30, 'c', false), (2, 1, 10, 'a', NULL), (3, 1, 20, 'b', true), (4, 2, NULL, NULL, false), (5, 2, 50, 'x', false), (6, 2, 40, 'e', true), (7, 2, 45, 'f', NULL)");
    const expectTexts = helpers.expectTexts;

    // A whole partition, a running frame, and a row whose key is NULL.
    try expectTexts(allocator, db, "SELECT max_by(s, v) OVER (PARTITION BY g) FROM aw ORDER BY k", &.{ "c", "c", "c", "x", "x", "x", "x" });
    try expectTexts(allocator, db, "SELECT min_by(s, v) OVER (PARTITION BY g) FROM aw ORDER BY k", &.{ "a", "a", "a", "e", "e", "e", "e" });
    try expectTexts(allocator, db, "SELECT min_by(s, v) OVER (PARTITION BY g ORDER BY k) FROM aw ORDER BY k", &.{ "c", "a", "a", null, "x", "e", "e" });
    // Sliding frames start afresh; an empty one is the aggregate of no rows.
    try expectTexts(allocator, db, "SELECT max_by(s, v) OVER (ORDER BY k ROWS BETWEEN 3 FOLLOWING AND 4 FOLLOWING) FROM aw ORDER BY k", &.{ "x", "x", "f", "f", null, null, null });
    try expectTexts(allocator, db, "SELECT count_if(v > 25) OVER (PARTITION BY g ORDER BY k ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM aw ORDER BY k", &.{ "1", "1", "0", "1", "2", "3", "2" });
    try expectTexts(allocator, db, "SELECT count_if(v > 25) OVER (ORDER BY k ROWS BETWEEN 9 FOLLOWING AND 10 FOLLOWING) FROM aw ORDER BY k", &.{ "0", "0", "0", "0", "0", "0", "0" });
    try expectTexts(allocator, db, "SELECT bool_or(b) OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM aw ORDER BY k", &.{ "0", "0", "1", "1", "0", "1", "1" });
    try expectTexts(allocator, db, "SELECT bool_and(b) OVER (ORDER BY k) FROM aw ORDER BY k", &.{ "0", "0", "0", "0", "0", "0", "0" });

    // ARRAY_AGG and GROUP_CONCAT, ordered, DISTINCT, and over RANGE peers.
    try expectTexts(allocator, db, "SELECT CAST(array_agg(v) OVER (PARTITION BY g ORDER BY k) AS STRING) FROM aw ORDER BY k", &.{ "[30]", "[30,10]", "[30,10,20]", "[null]", "[null,50]", "[null,50,40]", "[null,50,40,45]" });
    try expectTexts(allocator, db, "SELECT CAST(array_agg(s ORDER BY v DESC) OVER (PARTITION BY g) AS STRING) FROM aw ORDER BY k", &.{ "[\"c\",\"b\",\"a\"]", "[\"c\",\"b\",\"a\"]", "[\"c\",\"b\",\"a\"]", "[\"x\",\"f\",\"e\",null]", "[\"x\",\"f\",\"e\",null]", "[\"x\",\"f\",\"e\",null]", "[\"x\",\"f\",\"e\",null]" });
    try expectTexts(allocator, db, "SELECT CAST(array_agg(DISTINCT g) OVER (ORDER BY k) AS STRING) FROM aw ORDER BY k", &.{ "[1]", "[1]", "[1]", "[1,2]", "[1,2]", "[1,2]", "[1,2]" });
    try expectTexts(allocator, db, "SELECT CAST(array_agg_distinct(g) OVER (ORDER BY k) AS STRING) FROM aw ORDER BY k", &.{ "[1]", "[1]", "[1]", "[1,2]", "[1,2]", "[1,2]", "[1,2]" });
    try expectTexts(allocator, db, "SELECT group_concat(s ORDER BY k) OVER (ORDER BY g) FROM aw ORDER BY k", &.{ "c,a,b", "c,a,b", "c,a,b", "c,a,b,x,e,f", "c,a,b,x,e,f", "c,a,b,x,e,f", "c,a,b,x,e,f" });
    try expectTexts(allocator, db, "SELECT group_concat(DISTINCT g ORDER BY g SEPARATOR '-') OVER () FROM aw ORDER BY k", &.{ "1-2", "1-2", "1-2", "1-2", "1-2", "1-2", "1-2" });
    try expectTexts(allocator, db, "SELECT CAST(approx_top_k(g) OVER () AS STRING) FROM aw WHERE k < 3", &.{ "[{\"item\":1,\"count\":2}]", "[{\"item\":1,\"count\":2}]" });

    // Inside an expression, a CASE condition, under a GROUP BY, and read
    // from a subquery.
    try expectTexts(allocator, db, "SELECT concat('<', max_by(s, v) OVER (PARTITION BY g), '>') FROM aw ORDER BY k", &.{ "<c>", "<c>", "<c>", "<x>", "<x>", "<x>", "<x>" });
    try expectTexts(allocator, db, "SELECT CASE WHEN max_by(s, v) OVER (PARTITION BY g) = 'c' THEN 'one' ELSE 'two' END FROM aw ORDER BY k", &.{ "one", "one", "one", "two", "two", "two", "two" });
    try expectTexts(allocator, db, "SELECT max_by(g, SUM(v)) OVER () FROM aw GROUP BY g ORDER BY g", &.{ "2", "2" });
    try expectTexts(allocator, db, "SELECT k FROM (SELECT k, max_by(s, v + k) OVER (PARTITION BY g) AS m FROM aw) x WHERE m = 'x' ORDER BY k", &.{ "4", "5", "6", "7" });

    try std.testing.expectError(error.SqlInvalidProjection, helpers.exec(allocator, db, "SELECT max_by(s) OVER () FROM aw"));
    try std.testing.expectError(error.SqlInvalidProjection, helpers.exec(allocator, db, "SELECT max_by(s, v) IGNORE NULLS OVER () FROM aw"));
    try std.testing.expectError(error.SqlInvalidProjection, helpers.exec(allocator, db, "SELECT array_unique_agg(v) OVER () FROM aw"));
}

test "window: an aggregate over a large partitioned input gives what it gives on one thread" {
    const allocator = std.testing.allocator;
    const Row = struct { id: i64, g: i64, amt: f64 };
    // Past the row count where a partitioned window sorts and evaluates on
    // several threads.
    const rows: usize = 70_000;
    const seed = try allocator.alloc(Row, rows);
    defer allocator.free(seed);
    for (seed, 0..) |*row, i| row.* = .{ .id = @intCast(i), .g = @intCast(i % 7), .amt = @floatFromInt(i % 1000) };

    var expected: [7]?[]const u8 = undefined;
    var texts: [7][64]u8 = undefined;
    for (0..7) |g| {
        var count_above: i64 = 0;
        var running_sum: i64 = 0;
        var n: i64 = 0;
        var last_amt: f64 = 0;
        for (seed) |row| {
            if (row.g != g) continue;
            n += 1;
            if (row.amt > 500) count_above += 1;
            running_sum += count_above;
            last_amt = row.amt;
        }
        expected[g] = try std.fmt.bufPrint(&texts[g], "{d},{d},{d},{d}", .{ g, last_amt, running_sum, @divExact(n * (n + 1), 2) });
    }

    inline for (.{ 1, 4 }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .auto_flush_secs = 0, .max_dop = dop });
        defer db.close();
        const facts = try db.table("facts", .{
            .columns = &.{
                .{ .name = "id", .type = .bigint },
                .{ .name = "g", .type = .bigint },
                .{ .name = "amt", .type = .double },
            },
            .order_key = &.{"id"},
            .unique = false,
        }, .{ .order_key = &.{"id"}, .unique = false });
        try facts.insert(seed);
        try facts.flush();
        try helpers.expectTexts(allocator, db,
            \\SELECT CONCAT_WS(',', g, MAX(m), SUM(c), SUM(r)) FROM (
            \\  SELECT g, max_by(amt, id) OVER (PARTITION BY g) AS m,
            \\    count_if(amt > 500) OVER (PARTITION BY g ORDER BY id) AS c,
            \\    ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS r
            \\  FROM facts) x GROUP BY g ORDER BY g
        , &expected);
    }
}
