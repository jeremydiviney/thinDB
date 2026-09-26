//! Uncorrelated scalar subquery — `(SELECT single_value FROM ...)` in
//! WHERE / projection / arithmetic position. Pre-compile pass runs the
//! inner once and rewrites the marker into a `.leaf` / `.lit`.
//!
//! v1 (Tier 1) covers:
//!   - WHERE col cmp (SELECT ...)
//!   - SELECT (SELECT ...) AS alias FROM ...
//!   - Inside arithmetic / function calls / CASE branches
//!   - multi-row → error, multi-col → error, zero rows → NULL.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(
        allocator,
        db,
        "INSERT INTO t (id, qty) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
    );
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

test "scalar subquery: WHERE col > (SELECT MIN(...))" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // MIN(qty) = 10 → rows with qty > 10 are ids 2, 3, 4, 5.
    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM t WHERE qty > (SELECT MIN(qty) FROM t) ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3, 4, 5 }, ids);
}

test "scalar subquery: WHERE col = (SELECT MAX(...))" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM t WHERE qty = (SELECT MAX(qty) FROM t)",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{5}, ids);
}

test "scalar subquery: projection — (SELECT ...) AS alias" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(
        allocator,
        db,
        "SELECT id, (SELECT MAX(qty) FROM t) AS top FROM t ORDER BY id ASC LIMIT 2",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), batch.row_count);
    // Every row carries the same broadcast value 50 in the `top` column.
    // MAX(qty) on an INT column → INT.
    try std.testing.expectEqual(@as(i32, 50), batch.values[1].data.int[0]);
    try std.testing.expectEqual(@as(i32, 50), batch.values[1].data.int[1]);
}

test "scalar subquery: composes with HAVING" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, region VARCHAR(8) NOT NULL, qty INT NOT NULL)");
    try exec(
        allocator,
        db,
        "INSERT INTO t (id, region, qty) VALUES (1, 'east', 5), (2, 'east', 100), (3, 'west', 10), (4, 'west', 50)",
    );
    const t = try db.openTable("t", .{});
    try t.flush();

    // total_qty per region > (SELECT MIN(qty) FROM t) = 5
    var q = try runSql(
        allocator,
        db,
        "SELECT region, SUM(qty) AS total FROM t GROUP BY region HAVING total > (SELECT MIN(qty) FROM t) ORDER BY region ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), batch.row_count);
}

test "scalar subquery: multi-row error" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(
        arena.allocator(),
        "SELECT id FROM t WHERE qty > (SELECT qty FROM t)",
    );
    const cq = thindb.net.compile(allocator, db, root);
    if (cq) |ok| {
        var c = ok;
        c.deinit();
        return error.TestUnexpectedSuccess;
    } else |err| {
        try std.testing.expectEqual(thindb.net.Error.BadRequest, err);
    }
}

test "scalar subquery: multi-column error" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(
        arena.allocator(),
        "SELECT id FROM t WHERE qty > (SELECT id, qty FROM t LIMIT 1)",
    );
    const cq = thindb.net.compile(allocator, db, root);
    if (cq) |ok| {
        var c = ok;
        c.deinit();
        return error.TestUnexpectedSuccess;
    } else |err| {
        try std.testing.expectEqual(thindb.net.Error.BadRequest, err);
    }
}

test "scalar subquery: a statement constant beside an aggregate" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE o (oid BIGINT PRIMARY KEY, tid BIGINT NOT NULL, amount INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO o (oid, tid, amount) VALUES (1, 1, 5), (2, 1, 7), (3, 3, 9)");

    const cases = .{
        .{ "SELECT COUNT(*) + (SELECT COUNT(*) FROM o) AS n FROM t", &[_]i64{8} },
        .{ "SELECT (SELECT COUNT(*) FROM o) * COUNT(*) AS n FROM t", &[_]i64{15} },
        .{ "SELECT COUNT(*) AS n, (SELECT MAX(amount) FROM o) AS m FROM t", &[_]i64{5} },
        .{ "SELECT SUM(qty) - (SELECT MAX(amount) FROM o) AS d FROM t GROUP BY id ORDER BY d", &[_]i64{ 1, 11, 21, 31, 41 } },
        .{ "SELECT COUNT(*) + COALESCE(@never_set, 7) AS n FROM t", &[_]i64{12} },
        .{ "SELECT CASE WHEN EXISTS (SELECT 1 FROM o WHERE amount > 8) THEN COUNT(*) ELSE 0 END AS n FROM t", &[_]i64{5} },
        .{ "SELECT CASE WHEN id IN (SELECT tid FROM o) THEN SUM(qty) ELSE 0 END AS s FROM t GROUP BY id ORDER BY s", &[_]i64{ 0, 0, 0, 10, 30 } },
    };
    inline for (cases) |case| {
        const got = try collectBigints(allocator, db, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, case[1], got);
    }
}

test "scalar subquery: a NULL result or no rows reads as NULL" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE n (id BIGINT PRIMARY KEY, v INT)");
    try exec(allocator, db, "INSERT INTO n (id, v) VALUES (1, NULL), (2, 3)");

    const cases = .{
        // A comparison with NULL keeps no row, whichever way it is negated.
        .{ "SELECT id FROM t WHERE qty > (SELECT v FROM n WHERE id = 1)", &[_]i64{} },
        .{ "SELECT id FROM t WHERE NOT (qty > (SELECT v FROM n WHERE id = 1))", &[_]i64{} },
        .{ "SELECT id FROM t WHERE qty > (SELECT v FROM n WHERE id = 99)", &[_]i64{} },
        .{ "SELECT id FROM t WHERE qty > (SELECT MAX(id) FROM n WHERE id = 99)", &[_]i64{} },
        .{ "SELECT id FROM t WHERE qty > (SELECT v FROM n WHERE id = 2) ORDER BY id", &[_]i64{ 1, 2, 3, 4, 5 } },
        .{ "SELECT COALESCE((SELECT MAX(id) FROM n WHERE id = 99), -1) AS x FROM t", &[_]i64{ -1, -1, -1, -1, -1 } },
        .{ "SELECT COALESCE((SELECT id FROM n WHERE id = 99), -1) AS x FROM t", &[_]i64{ -1, -1, -1, -1, -1 } },
    };
    inline for (cases) |case| {
        const got = try collectBigints(allocator, db, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, case[1], got);
    }

    var q = try runSql(allocator, db, "SELECT id + (SELECT MAX(id) FROM n WHERE id = 99) AS s FROM t");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 5), batch.row_count);
    for (0..batch.row_count) |i| try std.testing.expect(!batch.values[0].isValid(i));
}
