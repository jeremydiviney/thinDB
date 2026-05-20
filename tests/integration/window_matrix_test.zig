//! Comprehensive correctness matrix for window functions.
//!
//! Organized one section per function. Each section covers:
//!   - basic shape
//!   - empty / single-row partition edge cases
//!   - NULL handling where applicable (IGNORE NULLS variants)
//!   - tie behavior for ranking-family functions
//!   - frame variants for frame-aware functions
//!
//! Complements `window_test.zig` (which has the "narrative" / first-time
//! coverage); this file is the dense matrix for regression coverage.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const RunResult = helpers.RunResult;
const runSql = helpers.runSql;
const exec = helpers.exec;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn collectI64(allocator: std.mem.Allocator, q: *RunResult, col_idx: usize) ![]i64 {
    var out: std.ArrayList(i64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[col_idx].data.bigint[0..b.row_count]) |v| try out.append(allocator, v);
    }
    return try out.toOwnedSlice(allocator);
}

fn collectF64(allocator: std.mem.Allocator, q: *RunResult, col_idx: usize) ![]f64 {
    var out: std.ArrayList(f64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[col_idx].data.double[0..b.row_count]) |v| try out.append(allocator, v);
    }
    return try out.toOwnedSlice(allocator);
}

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

fn approxSlice(actual: []const f64, expected: []const f64, tol: f64) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (actual, expected) |a, e| try std.testing.expectApproxEqAbs(e, a, tol);
}

const Fixture = struct {
    tmp: std.testing.TmpDir,
    db: *thindb.Database,

    fn open(allocator: std.mem.Allocator) !Fixture {
        const io = std.testing.io;
        var tmp = std.testing.tmpDir(.{});
        errdefer tmp.cleanup();
        const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        return .{ .tmp = tmp, .db = db };
    }

    fn close(self: *Fixture) void {
        self.db.close();
        self.tmp.cleanup();
    }

    fn exec(self: *Fixture, allocator: std.mem.Allocator, sql: []const u8) !void {
        var q = try runSql(allocator, self.db, sql);
        defer q.deinit();
        while (try q.next()) |_| {}
    }
};

// ---------------------------------------------------------------------------
// ROW_NUMBER
// ---------------------------------------------------------------------------

test "row_number: single partition basics" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    try f.exec(allocator, "INSERT INTO t VALUES (1), (2), (3), (4)");
    var q = try runSql(allocator, f.db,
        "SELECT row_number() OVER (ORDER BY id ASC) AS rn FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4 }, got);
}

test "row_number: PARTITION BY restarts per group" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 2)");
    var q = try runSql(allocator, f.db,
        "SELECT row_number() OVER (PARTITION BY g ORDER BY id ASC) AS rn FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 1, 2, 3 }, got);
}

test "row_number: singleton partition emits just 1" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)");
    var q = try runSql(allocator, f.db,
        "SELECT row_number() OVER (PARTITION BY g ORDER BY id ASC) AS rn FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 1 }, got);
}

// ---------------------------------------------------------------------------
// RANK / DENSE_RANK
// ---------------------------------------------------------------------------

test "rank: all-tied partition gets rank 1 everywhere" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, score BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 100), (2, 100), (3, 100)");
    var q = try runSql(allocator, f.db,
        "SELECT rank() OVER (ORDER BY score ASC) AS rk FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 1 }, got);
}

test "rank: gaps after ties" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, score BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 90), (2, 90), (3, 80), (4, 70), (5, 70)");
    var q = try runSql(allocator, f.db,
        "SELECT rank() OVER (ORDER BY score DESC) AS rk FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 3, 4, 4 }, got);
}

test "dense_rank: no gaps after ties" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, score BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 90), (2, 90), (3, 80), (4, 70), (5, 70)");
    var q = try runSql(allocator, f.db,
        "SELECT dense_rank() OVER (ORDER BY score DESC) AS drk FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 2, 3, 3 }, got);
}

// ---------------------------------------------------------------------------
// LAG / LEAD
// ---------------------------------------------------------------------------

test "lag: default offset=1 with NULL fallback" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    var q = try runSql(allocator, f.db,
        "SELECT lag(v) OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const valids = try collectValidity(allocator, &q, 0);
    defer allocator.free(valids);
    try std.testing.expectEqualSlices(bool, &[_]bool{ false, true, true }, valids);
}

test "lag: offset=2" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
    var q = try runSql(allocator, f.db,
        "SELECT lag(v, 2, 0) OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 0, 0, 10, 20 }, got);
}

test "lag: offset larger than partition uses default" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 10), (2, 20)");
    var q = try runSql(allocator, f.db,
        "SELECT lag(v, 5, 999) OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 999, 999 }, got);
}

test "lead: symmetric to lag, forward offset" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    var q = try runSql(allocator, f.db,
        "SELECT lead(v, 1, 0) OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 20, 30, 0 }, got);
}

test "lag: column-ref default returns current row's value" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    var q = try runSql(allocator, f.db,
        "SELECT lag(v, 1, v) OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 10, 20 }, got);
}

// ---------------------------------------------------------------------------
// FIRST_VALUE / LAST_VALUE / NTH_VALUE
// ---------------------------------------------------------------------------

test "first_value: returns partition's first value" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 1, 100), (2, 1, 200), (3, 2, 999)");
    var q = try runSql(allocator, f.db,
        "SELECT first_value(v) OVER (PARTITION BY g ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 100, 100, 999 }, got);
}

test "last_value: with default frame returns current row's value" {
    // Default frame with ORDER BY is RANGE UNBOUNDED PRECEDING TO
    // CURRENT ROW — surprises many users, but it's the SQL standard.
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    var q = try runSql(allocator, f.db,
        "SELECT last_value(v) OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20, 30 }, got);
}

test "last_value: with ROWS UNBOUNDED FOLLOWING returns partition's last" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    var q = try runSql(allocator, f.db,
        "SELECT last_value(v) OVER (ORDER BY id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 30, 30, 30 }, got);
}

test "nth_value: returns NULL when n exceeds frame, value otherwise" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    var q = try runSql(allocator, f.db,
        "SELECT nth_value(v, 2) OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    var qv = try runSql(allocator, f.db,
        "SELECT nth_value(v, 2) OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer qv.deinit();
    const valids = try collectValidity(allocator, &q, 0);
    defer allocator.free(valids);
    const vals = try collectI64(allocator, &qv, 0);
    defer allocator.free(vals);
    try std.testing.expectEqualSlices(bool, &[_]bool{ false, true, true }, valids);
    try std.testing.expectEqual(@as(i64, 20), vals[1]);
    try std.testing.expectEqual(@as(i64, 20), vals[2]);
}

// ---------------------------------------------------------------------------
// SUM / COUNT / AVG / MIN / MAX
// ---------------------------------------------------------------------------

test "sum: running, single partition" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 1), (2, 2), (3, 3), (4, 4)");
    var q = try runSql(allocator, f.db,
        "SELECT sum(v) OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 3, 6, 10 }, got);
}

test "sum: whole-partition broadcast" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 2, 100)");
    var q = try runSql(allocator, f.db,
        "SELECT sum(v) OVER (PARTITION BY g) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 30, 30, 100 }, got);
}

test "sum: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
    var q = try runSql(allocator, f.db,
        "SELECT sum(v) OVER (ORDER BY id ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 30, 50, 70 }, got);
}

test "count(*): partition size broadcast" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 1), (2, 1), (3, 1), (4, 2)");
    var q = try runSql(allocator, f.db,
        "SELECT count(*) OVER (PARTITION BY g) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 3, 3, 1 }, got);
}

test "avg: running average over running frame" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
    var q = try runSql(allocator, f.db,
        "SELECT avg(v) OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectF64(allocator, &q, 0);
    defer allocator.free(got);
    try approxSlice(got, &[_]f64{ 10, 15, 20, 25 }, 1e-9);
}

test "min: running" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 30), (2, 10), (3, 20), (4, 5)");
    var q = try runSql(allocator, f.db,
        "SELECT min(v) OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 30, 10, 10, 5 }, got);
}

test "max: running" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 30), (2, 10), (3, 20), (4, 50)");
    var q = try runSql(allocator, f.db,
        "SELECT max(v) OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 30, 30, 30, 50 }, got);
}

test "min: string column lexicographic" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT NOT NULL)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 'zeta'), (2, 'alpha'), (3, 'mu')");
    var q = try runSql(allocator, f.db,
        "SELECT min(name) OVER () FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqualStrings("alpha", batch.values[0].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("alpha", batch.values[0].data.string.rowBytes(1));
    try std.testing.expectEqualStrings("alpha", batch.values[0].data.string.rowBytes(2));
}

// ---------------------------------------------------------------------------
// NTILE
// ---------------------------------------------------------------------------

test "ntile: 10 rows / 4 buckets → 3,3,2,2 distribution" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    try f.exec(allocator, "INSERT INTO t VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)");
    var q = try runSql(allocator, f.db,
        "SELECT ntile(4) OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 1, 2, 2, 2, 3, 3, 4, 4 }, got);
}

test "ntile: n > N gives each row its own bucket" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    try f.exec(allocator, "INSERT INTO t VALUES (1), (2), (3)");
    var q = try runSql(allocator, f.db,
        "SELECT ntile(10) OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    // N=3, n=10 → small=0, large=3, large_size=1 → buckets 1,2,3
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, got);
}

test "ntile: n = 1 puts every row in bucket 1" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    try f.exec(allocator, "INSERT INTO t VALUES (1), (2), (3), (4)");
    var q = try runSql(allocator, f.db,
        "SELECT ntile(1) OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 1, 1 }, got);
}

// ---------------------------------------------------------------------------
// PERCENT_RANK / CUME_DIST
// ---------------------------------------------------------------------------

test "percent_rank: singleton partition returns 0" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    try f.exec(allocator, "INSERT INTO t VALUES (1)");
    var q = try runSql(allocator, f.db,
        "SELECT percent_rank() OVER (ORDER BY id ASC) FROM t",
    );
    defer q.deinit();
    const got = try collectF64(allocator, &q, 0);
    defer allocator.free(got);
    try approxSlice(got, &[_]f64{0}, 1e-9);
}

test "percent_rank: distinct values, distributed evenly" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    try f.exec(allocator, "INSERT INTO t VALUES (1), (2), (3), (4), (5)");
    var q = try runSql(allocator, f.db,
        "SELECT percent_rank() OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectF64(allocator, &q, 0);
    defer allocator.free(got);
    try approxSlice(got, &[_]f64{ 0.0, 0.25, 0.5, 0.75, 1.0 }, 1e-9);
}

test "cume_dist: distinct values give 1/N, 2/N, ..." {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    try f.exec(allocator, "INSERT INTO t VALUES (1), (2), (3), (4)");
    var q = try runSql(allocator, f.db,
        "SELECT cume_dist() OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectF64(allocator, &q, 0);
    defer allocator.free(got);
    try approxSlice(got, &[_]f64{ 0.25, 0.5, 0.75, 1.0 }, 1e-9);
}

test "cume_dist: peer rows share the value" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, score BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 20), (4, 30)");
    var q = try runSql(allocator, f.db,
        "SELECT cume_dist() OVER (ORDER BY score ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectF64(allocator, &q, 0);
    defer allocator.free(got);
    try approxSlice(got, &[_]f64{ 0.25, 0.75, 0.75, 1.0 }, 1e-9);
}

// ---------------------------------------------------------------------------
// IGNORE NULLS
// ---------------------------------------------------------------------------

test "lag IGNORE NULLS: skips null source rows" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, NULL), (4, 40)");
    var q = try runSql(allocator, f.db,
        "SELECT lag(v) IGNORE NULLS OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    var qv = try runSql(allocator, f.db,
        "SELECT lag(v) IGNORE NULLS OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer qv.deinit();
    const valids = try collectValidity(allocator, &q, 0);
    defer allocator.free(valids);
    const vals = try collectI64(allocator, &qv, 0);
    defer allocator.free(vals);
    // id=1 → no prior (NULL); id=2,3,4 all see id=1 as the previous non-null.
    try std.testing.expectEqualSlices(bool, &[_]bool{ false, true, true, true }, valids);
    try std.testing.expectEqual(@as(i64, 10), vals[1]);
    try std.testing.expectEqual(@as(i64, 10), vals[2]);
    try std.testing.expectEqual(@as(i64, 10), vals[3]);
}

test "first_value IGNORE NULLS: skips leading nulls" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, NULL), (2, NULL), (3, 30)");
    var q = try runSql(allocator, f.db,
        "SELECT first_value(v) IGNORE NULLS OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const got = try collectI64(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 30, 30, 30 }, got);
}

test "first_value IGNORE NULLS: all-null partition returns NULL" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, NULL), (2, NULL)");
    var q = try runSql(allocator, f.db,
        "SELECT first_value(v) IGNORE NULLS OVER (ORDER BY id ASC) FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const valids = try collectValidity(allocator, &q, 0);
    defer allocator.free(valids);
    try std.testing.expectEqualSlices(bool, &[_]bool{ false, false }, valids);
}

// ---------------------------------------------------------------------------
// Named windows + QUALIFY
// ---------------------------------------------------------------------------

test "named window: WINDOW w AS (...) shared across two calls" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 2, 100)");
    var q = try runSql(allocator, f.db,
        \\SELECT row_number() OVER w AS rn, sum(v) OVER w AS rs
        \\FROM t
        \\WINDOW w AS (PARTITION BY g ORDER BY id ASC)
        \\ORDER BY id ASC
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), batch.row_count);
    try std.testing.expectEqual(@as(i64, 1), batch.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 10), batch.values[1].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 30), batch.values[1].data.bigint[1]);
    try std.testing.expectEqual(@as(i64, 100), batch.values[1].data.bigint[2]);
}

test "QUALIFY: filters on rank()" {
    const allocator = std.testing.allocator;
    var f = try Fixture.open(allocator);
    defer f.close();
    try f.exec(allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT, v BIGINT)");
    try f.exec(allocator, "INSERT INTO t VALUES (1, 1, 10), (2, 1, 30), (3, 1, 20), (4, 2, 50), (5, 2, 60)");
    var q = try runSql(allocator, f.db,
        \\SELECT id, rank() OVER (PARTITION BY g ORDER BY v DESC) AS rk
        \\FROM t QUALIFY rk = 1 ORDER BY id ASC
    );
    defer q.deinit();
    var rows: std.ArrayList(i64) = .empty;
    defer rows.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try rows.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 5 }, rows.items);
}
