//! Uncorrelated scalar subquery — `(SELECT single_value FROM ...)` in
//! WHERE / projection / arithmetic position. Pre-compile pass runs the
//! inner once and rewrites the marker into a `.leaf` / `.lit`.
//!
//! v1 (Tier 1) covers:
//!   - WHERE col cmp (SELECT ...)
//!   - SELECT (SELECT ...) AS alias FROM ...
//!   - Inside arithmetic / function calls / CASE branches
//!   - Postgres semantics: multi-row → error, multi-col → error,
//!     zero-row → error (Tier 2 may revisit zero-row → NULL).

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
    try exec(allocator, db,
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
    const ids = try collectBigints(allocator, db,
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

    const ids = try collectBigints(allocator, db,
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

    var q = try runSql(allocator, db,
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
    try exec(allocator, db,
        "INSERT INTO t (id, region, qty) VALUES (1, 'east', 5), (2, 'east', 100), (3, 'west', 10), (4, 'west', 50)",
    );
    const t = try db.openTable("t", .{});
    try t.flush();

    // total_qty per region > (SELECT MIN(qty) FROM t) = 5
    var q = try runSql(allocator, db,
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
    const root = try thindb.sql.parse(arena.allocator(),
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
    const root = try thindb.sql.parse(arena.allocator(),
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
