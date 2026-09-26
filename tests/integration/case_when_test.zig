//! Searched CASE WHEN expressions in SELECT projections.
//!
//!   CASE WHEN cond THEN expr [WHEN cond THEN expr]* [ELSE expr] END
//!
//! v1 scope:
//!   - Searched form only (no `CASE col WHEN val THEN ...`).
//!   - All branches' THEN (+ optional ELSE) must unify to one type.
//!   - THEN/ELSE: column ref, literal, or scalar function call.
//!     Nested CASE in a branch is rejected at compile time.
//!   - Conditions can reference upstream columns (the predicates run
//!     against the input batch's schema, same as WHERE).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

test "CASE WHEN: literal branches, ELSE present" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)",
    );
    try exec(allocator, db, "INSERT INTO t (id, qty) VALUES (1, 5), (2, 50), (3, 500)");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(
        allocator,
        db,
        "SELECT CASE WHEN qty < 10 THEN 'small' WHEN qty < 100 THEN 'medium' ELSE 'large' END AS bucket FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), batch.row_count);
    try std.testing.expectEqualStrings("small", batch.values[0].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("medium", batch.values[0].data.string.rowBytes(1));
    try std.testing.expectEqualStrings("large", batch.values[0].data.string.rowBytes(2));
}

test "simple CASE compares its operand to each WHEN value" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT, s STRING)");
    try exec(allocator, db, "INSERT INTO t (id, qty, s) VALUES (1, 1, 'a'), (2, 0, 'b'), (3, 5, NULL), (4, NULL, 'a')");

    // A NULL operand matches no WHEN and takes the ELSE.
    const cases = .{
        .{ "SELECT CASE qty WHEN 0 THEN id WHEN 1 THEN id * 10 ELSE 0 - id END AS v FROM t ORDER BY id", &[_]i64{ 10, 2, -3, -4 } },
        .{ "SELECT CASE id % 2 WHEN 0 THEN 100 + id ELSE id END AS v FROM t ORDER BY id", &[_]i64{ 1, 102, 3, 104 } },
        .{ "SELECT CASE s WHEN 'a' THEN id ELSE 0 - id END AS v FROM t ORDER BY id", &[_]i64{ 1, -2, -3, 4 } },
        .{ "SELECT COALESCE(CASE qty WHEN 5 THEN id END, 0) AS v FROM t ORDER BY id", &[_]i64{ 0, 0, 3, 0 } },
        .{ "SELECT SUM(CASE s WHEN 'a' THEN id ELSE 0 END) AS n FROM t", &[_]i64{5} },
    };
    inline for (cases) |case| {
        const got = try helpers.collectBigints(allocator, db, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, case[1], got);
    }

    var q = try runSql(allocator, db, "SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END AS w FROM t ORDER BY id");
    defer q.deinit();
    const batch = (try q.next()).?;
    const col = batch.values[0];
    for ([_][]const u8{ "one", "two", "many", "many" }, 0..) |want, i| {
        try std.testing.expectEqualStrings(want, col.data.string.rowBytes(i));
    }
}

test "CASE WHEN: ELSE-less form emits NULL for unmatched rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)",
    );
    try exec(allocator, db, "INSERT INTO t (id, qty) VALUES (1, 5), (2, 50)");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(
        allocator,
        db,
        "SELECT CASE WHEN qty < 10 THEN 'small' END AS bucket FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), batch.row_count);
    try std.testing.expect(batch.values[0].isValid(0));
    try std.testing.expectEqualStrings("small", batch.values[0].data.string.rowBytes(0));
    try std.testing.expect(!batch.values[0].isValid(1));
}

test "CASE WHEN: integer branches via column ref" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL)",
    );
    try exec(allocator, db, "INSERT INTO t (id, a, b) VALUES (1, 10, 20), (2, 30, 5)");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(
        allocator,
        db,
        "SELECT CASE WHEN a < 25 THEN a ELSE b END AS picked FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), batch.row_count);
    try std.testing.expectEqual(@as(i32, 10), batch.values[0].data.int[0]);
    try std.testing.expectEqual(@as(i32, 5), batch.values[0].data.int[1]);
}

test "CASE WHEN: first-true wins across overlapping conditions" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)",
    );
    try exec(allocator, db, "INSERT INTO t (id, qty) VALUES (1, 5)");
    const t = try db.openTable("t", .{});
    try t.flush();

    // qty=5 matches BOTH conditions; first one wins.
    var q = try runSql(
        allocator,
        db,
        "SELECT CASE WHEN qty < 100 THEN 'small' WHEN qty < 1000 THEN 'medium' ELSE 'large' END AS bucket FROM t",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqualStrings("small", batch.values[0].data.string.rowBytes(0));
}

test "CASE WHEN: AND/OR conditions in WHEN" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, region VARCHAR(8) NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO t (id, qty, region) VALUES (1, 5, 'east'), (2, 50, 'west'), (3, 5, 'west')",
    );
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(
        allocator,
        db,
        "SELECT CASE WHEN qty < 10 AND region = 'west' THEN 'small-west' ELSE 'other' END AS label FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), batch.row_count);
    try std.testing.expectEqualStrings("other", batch.values[0].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("other", batch.values[0].data.string.rowBytes(1));
    try std.testing.expectEqualStrings("small-west", batch.values[0].data.string.rowBytes(2));
}

test "CASE WHEN: rejects branches with mismatched types" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(
        arena.allocator(),
        "SELECT CASE WHEN qty < 10 THEN 1 ELSE 'big' END AS mix FROM t",
    );
    const cq = thindb.net.compile(allocator, db, root);
    if (cq) |ok| {
        var c = ok;
        c.deinit();
        return error.TestUnexpectedSuccess;
    } else |err| {
        try std.testing.expectEqual(thindb.exec.Error.ComputeUnsupportedExpr, err);
    }
}

test "case: parenthesized CASE as a WHEN comparison operand" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO t (id, qty) VALUES (1, 5), (2, 50), (3, 500)");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db, "SELECT id, CASE WHEN (CASE WHEN qty > 40 THEN qty ELSE 0 END) > 0 THEN 1 ELSE 0 END AS flag FROM t ORDER BY id ASC");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), batch.row_count);
    try std.testing.expectEqual(@as(i32, 0), batch.values[1].data.int[0]);
    try std.testing.expectEqual(@as(i32, 1), batch.values[1].data.int[1]);
    try std.testing.expectEqual(@as(i32, 1), batch.values[1].data.int[2]);
}

test "case: CASE expression continued by arithmetic over a window call" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO t (id, qty) VALUES (1, 5), (2, 50), (3, 500)");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db, "SELECT id, CASE WHEN qty > 40 THEN 1 ELSE 0 END - LAG(CASE WHEN qty > 40 THEN 1 ELSE 0 END, 1, 0) OVER (ORDER BY id) AS chg FROM t ORDER BY id ASC");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), batch.row_count);
    try std.testing.expectEqual(@as(i64, 0), batch.values[1].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 1), batch.values[1].data.bigint[1]);
    try std.testing.expectEqual(@as(i64, 0), batch.values[1].data.bigint[2]);
}

test "case: bare NULL branch adopts the typed branches' type" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, d DATE NOT NULL)");
    try exec(allocator, db, "INSERT INTO t (id, qty, d) VALUES (1, 5, '2026-01-05'), (2, 50, '2026-02-07')");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db, "SELECT id, CASE WHEN qty > 10 THEN d ELSE NULL END AS gated FROM t ORDER BY id ASC");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), b.row_count);
    const nulls = b.values[1].nulls.?;
    try std.testing.expectEqual(@as(u8, 0), nulls[0] & 1);
    try std.testing.expectEqual(@as(u8, 2), nulls[0] & 2);
}

test "case: window call inside a WHEN condition hoists in projection context" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE wt (id BIGINT PRIMARY KEY, g INT, qty INT)");
    try exec(allocator, db, "INSERT INTO wt VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)");
    const t = try db.openTable("wt", .{});
    try t.flush();

    var q = try runSql(allocator, db, "SELECT id, CASE WHEN ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) < 2 THEN qty ELSE 0 END AS v FROM wt ORDER BY id ASC");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), b.row_count);
    try std.testing.expectEqual(@as(i32, 10), b.values[1].data.int[0]);
    try std.testing.expectEqual(@as(i32, 0), b.values[1].data.int[1]);
    try std.testing.expectEqual(@as(i32, 30), b.values[1].data.int[2]);

    // Windows stay illegal in WHERE.
    const bad = runSql(allocator, db, "SELECT id FROM wt WHERE ROW_NUMBER() OVER (ORDER BY id) < 2");
    try std.testing.expectError(error.SqlTrailingTokens, bad);
}

test "CASE WHEN: one CASE repeated across derived flags evaluates once and agrees" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO t (id, qty) VALUES (1, 5), (2, 50), (3, 500), (4, 7)");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(
        allocator,
        db,
        "SELECT CASE WHEN qty < 10 THEN 'small' WHEN qty < 100 THEN 'medium' ELSE 'large' END AS bucket, " ++
            "CASE WHEN (CASE WHEN qty < 10 THEN 'small' WHEN qty < 100 THEN 'medium' ELSE 'large' END) = 'small' THEN 1 ELSE 0 END AS is_small, " ++
            "CASE WHEN (CASE WHEN qty < 10 THEN 'small' WHEN qty < 100 THEN 'medium' ELSE 'large' END) = 'large' THEN qty ELSE 0 END AS large_qty " ++
            "FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 4), batch.row_count);
    try std.testing.expectEqual(@as(usize, 3), batch.values.len);
    const buckets = [_][]const u8{ "small", "medium", "large", "small" };
    for (buckets, 0..) |want, i| try std.testing.expectEqualStrings(want, batch.values[0].data.string.rowBytes(i));
    try std.testing.expectEqualSlices(i32, &.{ 1, 0, 0, 1 }, batch.values[1].data.int[0..4]);
    try std.testing.expectEqualSlices(i32, &.{ 0, 0, 500, 0 }, batch.values[2].data.int[0..4]);
}

test "CASE WHEN: nullable column branches beside literal branches" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, note STRING, n2 INT)");
    try exec(allocator, db, "INSERT INTO t (id, qty, note, n2) VALUES (1, 5, 'a', 3), (2, 50, NULL, 4), (3, 500, 'c', NULL), (4, 7, NULL, NULL)");
    const t = try db.openTable("t", .{});
    try t.flush();

    {
        var q = try runSql(allocator, db, "SELECT CASE WHEN qty < 10 THEN note WHEN qty < 100 THEN 'mid' END AS s FROM t ORDER BY id ASC");
        defer q.deinit();
        const batch = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 4), batch.row_count);
        const col = batch.values[0];
        try std.testing.expect(col.isValid(0));
        try std.testing.expectEqualStrings("a", col.data.string.rowBytes(0));
        try std.testing.expect(col.isValid(1));
        try std.testing.expectEqualStrings("mid", col.data.string.rowBytes(1));
        // Unmatched row and a NULL source row are both NULL.
        try std.testing.expect(!col.isValid(2));
        try std.testing.expect(!col.isValid(3));
    }
    {
        var q = try runSql(allocator, db, "SELECT CASE WHEN qty < 10 THEN n2 ELSE 0 END AS v FROM t ORDER BY id ASC");
        defer q.deinit();
        const batch = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 4), batch.row_count);
        const col = batch.values[0];
        try std.testing.expect(col.isValid(0));
        try std.testing.expectEqual(@as(i32, 3), col.data.int[0]);
        try std.testing.expect(col.isValid(1));
        try std.testing.expectEqual(@as(i32, 0), col.data.int[1]);
        try std.testing.expect(col.isValid(2));
        try std.testing.expectEqual(@as(i32, 0), col.data.int[2]);
        try std.testing.expect(!col.isValid(3));
    }
}

/// Each row's first column as 1 or 0, or -1 where it is NULL.
fn collectTruth(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]i64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(i64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |batch| {
        const col = batch.values[0];
        for (0..batch.row_count) |i| {
            try out.append(allocator, if (col.isValid(i)) col.data.boolean[i] else -1);
        }
    }
    return out.toOwnedSlice(allocator);
}

test "predicates read as values: TRUE, FALSE, or NULL where unknown" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT, big BIGINT NOT NULL, s STRING)");
    try exec(allocator, db, "INSERT INTO t (id, qty, big, s) VALUES (1, 1, 1, 'apple'), (2, 0, 0, 'banana'), (3, 5, 5, NULL), (4, NULL, 0, 'avocado'), (5, 2, 2, 'cherry')");
    try exec(allocator, db, "CREATE TABLE o (oid BIGINT PRIMARY KEY, tid BIGINT NOT NULL, amount INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO o (oid, tid, amount) VALUES (1, 1, 5), (2, 1, 7), (3, 3, 9)");

    const truth_cases = .{
        .{ "SELECT qty > 1 AS f FROM t ORDER BY id", &[_]i64{ 0, 0, 1, -1, 1 } },
        .{ "SELECT qty > 1 FROM t ORDER BY id", &[_]i64{ 0, 0, 1, -1, 1 } },
        .{ "SELECT (qty > 1) AS f FROM t ORDER BY id", &[_]i64{ 0, 0, 1, -1, 1 } },
        .{ "SELECT id = big AS f FROM t ORDER BY id", &[_]i64{ 1, 0, 0, 0, 0 } },
        .{ "SELECT id IN (1, 3) AS f FROM t ORDER BY id", &[_]i64{ 1, 0, 1, 0, 0 } },
        .{ "SELECT id NOT IN (1, 3) AS f FROM t ORDER BY id", &[_]i64{ 0, 1, 0, 1, 1 } },
        .{ "SELECT qty BETWEEN 1 AND 2 AS f FROM t ORDER BY id", &[_]i64{ 1, 0, 0, -1, 1 } },
        .{ "SELECT qty IS NULL AS f FROM t ORDER BY id", &[_]i64{ 0, 0, 0, 1, 0 } },
        .{ "SELECT qty > 0 AND big < 5 AS f FROM t ORDER BY id", &[_]i64{ 1, 0, 0, -1, 1 } },
        .{ "SELECT qty > 0 OR big = 0 AS f FROM t ORDER BY id", &[_]i64{ 1, 1, 1, 1, 1 } },
        .{ "SELECT NOT (qty > 0) AS f FROM t ORDER BY id", &[_]i64{ 0, 1, 0, -1, 0 } },
        .{ "SELECT s LIKE 'a%' AS f FROM t ORDER BY id", &[_]i64{ 1, 0, -1, 1, 0 } },
        .{ "SELECT COALESCE(qty > 1, FALSE) AS f FROM t ORDER BY id", &[_]i64{ 0, 0, 1, 0, 1 } },
        .{ "SELECT id IN (SELECT tid FROM o) AS f FROM t ORDER BY id", &[_]i64{ 1, 0, 1, 0, 0 } },
        .{ "SELECT EXISTS (SELECT 1 FROM o WHERE o.tid = t.id) AS f FROM t ORDER BY id", &[_]i64{ 1, 0, 1, 0, 0 } },
        .{ "SELECT NOT EXISTS (SELECT 1 FROM o WHERE o.tid = t.id) AS f FROM t ORDER BY id", &[_]i64{ 0, 1, 0, 1, 1 } },
        .{ "SELECT COUNT(*) > 4 AS f FROM t", &[_]i64{1} },
        .{ "SELECT SUM(amount) > 10 AS f FROM o GROUP BY tid ORDER BY tid", &[_]i64{ 1, 0 } },
    };
    inline for (truth_cases) |case| {
        const got = try collectTruth(allocator, db, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, case[1], got);
    }

    const bigint_cases = .{
        .{ "SELECT SUM(qty > 0) AS n FROM t", &[_]i64{3} },
        .{ "SELECT SUM(qty > 0 AND big > 1) AS n FROM t", &[_]i64{2} },
        // Call arguments and BETWEEN bounds in WHERE keep parsing as values.
        .{ "SELECT id FROM t WHERE COALESCE(qty, 0) > 1 ORDER BY id", &[_]i64{ 3, 5 } },
        .{ "SELECT id FROM t WHERE big BETWEEN ABS(0 - 1) AND 5 ORDER BY id", &[_]i64{ 1, 3, 5 } },
    };
    inline for (bigint_cases) |case| {
        const got = try collectBigints(allocator, db, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, case[1], got);
    }

    // An unaliased predicate is named by its text, as MySQL names it.
    {
        var q = try runSql(allocator, db, "SELECT qty > 1, id IN (1, 3) FROM t");
        defer q.deinit();
        const schema = q.outputSchema();
        try std.testing.expectEqualStrings("qty > 1", schema[0].name);
        try std.testing.expectEqualStrings("id IN (1, 3)", schema[1].name);
    }
    // On the MySQL wire `||` is OR.
    {
        var q = try helpers.runSqlMysql(allocator, db, "SELECT qty > 1 || big = 0 AS f FROM t ORDER BY id");
        defer q.deinit();
        const batch = (try q.next()).?;
        try std.testing.expectEqualSlices(u8, &.{ 0, 1, 1, 1, 1 }, batch.values[0].data.boolean[0..batch.row_count]);
    }
}
