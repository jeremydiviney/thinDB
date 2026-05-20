//! Window function operator — end-to-end SQL tests for Tier 1.
//!
//! Driven through the SQL parser + compile path (the same one the
//! MySQL/PG wires use). Each test seeds a table, runs an OVER query,
//! and validates the projected window output column row by row.

const std = @import("std");
const thindb = @import("thindb");

const RunResult = struct {
    arena: std.heap.ArenaAllocator,
    cq: thindb.net.CompiledQuery,

    pub fn deinit(self: *RunResult) void {
        self.cq.deinit();
        self.arena.deinit();
    }

    pub fn next(self: *RunResult) !?thindb.Batch {
        return self.cq.next();
    }
};

fn runSql(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !RunResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), sql);
    const cq = try thindb.net.compile(allocator, db, root);
    return .{ .arena = arena, .cq = cq };
}

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
    var q1 = try runSql(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, grp BIGINT, qty BIGINT)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db,
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

    var q = try runSql(allocator, db,
        "SELECT id, row_number() OVER (ORDER BY id ASC) AS rn FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const rns = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(rns);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4, 5 }, rns);
}

test "window: ROW_NUMBER with PARTITION BY" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(allocator, db,
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

    var q1 = try runSql(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, score BIGINT)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db,
        "INSERT INTO t VALUES (1, 90), (2, 90), (3, 80), (4, 70), (5, 70)",
    );
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db,
        "SELECT id, rank() OVER (ORDER BY score DESC) AS rk, dense_rank() OVER (ORDER BY score DESC) AS drk FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const rks = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(rks);
    // q.next() consumed the batch; re-run for dense
    var q3 = try runSql(allocator, db,
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

    var q = try runSql(allocator, db,
        "SELECT id, lag(qty) OVER (PARTITION BY grp ORDER BY id ASC) AS prev_qty FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    var qq = try runSql(allocator, db,
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

    var q = try runSql(allocator, db,
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
    var q = try runSql(allocator, db,
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

    var q = try runSql(allocator, db,
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

    var q = try runSql(allocator, db,
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

    var q = try runSql(allocator, db,
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

    var q = try runSql(allocator, db,
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

    var q = try runSql(allocator, db,
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

    var q = try runSql(allocator, db,
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

    var qa = try runSql(allocator, db,
        "SELECT id, min(qty) OVER (PARTITION BY grp ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer qa.deinit();
    const mins = try collectRows(i64, allocator, &qa, 1);
    defer allocator.free(mins);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 10, 10, 100, 100 }, mins);

    var qb = try runSql(allocator, db,
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

    var q = try runSql(allocator, db,
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

    var q1 = try runSql(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty BIGINT)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db,
        "INSERT INTO t VALUES (1, 10), (2, NULL), (3, NULL), (4, 40), (5, 50)",
    );
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db,
        "SELECT id, lag(qty) IGNORE NULLS OVER (ORDER BY id ASC) AS prev FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    var qv = try runSql(allocator, db,
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

    var q1 = try runSql(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty BIGINT)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db,
        "INSERT INTO t VALUES (1, NULL), (2, NULL), (3, 30), (4, 40)",
    );
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db,
        "SELECT id, first_value(qty) IGNORE NULLS OVER (ORDER BY id ASC) AS fv FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const vals = try collectRows(i64, allocator, &q, 1);
    defer allocator.free(vals);
    // All rows see the first non-null in the partition: 30.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 30, 30, 30, 30 }, vals);
}

test "window: LAG on string column round-trips bytes" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db,
        "INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')",
    );
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db,
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

    var q1 = try runSql(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, grp BIGINT, name TEXT NOT NULL)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db,
        "INSERT INTO t VALUES (1, 1, 'zeta'), (2, 1, 'alpha'), (3, 1, 'mu'), (4, 2, 'omega'), (5, 2, 'beta')",
    );
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db,
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
