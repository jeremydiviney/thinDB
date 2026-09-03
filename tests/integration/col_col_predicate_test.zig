//! Col-vs-col comparisons in WHERE — `WHERE c1 op c2`.
//!
//! Required for TPC-H Q12 (`l_commitdate < l_receiptdate`) and as the
//! foundation for correlated-subquery detection (where one side of
//! a comparison binds to the outer scope instead of the inner).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL)");
    try exec(
        allocator,
        db,
        "INSERT INTO t (id, a, b) VALUES (1, 10, 20), (2, 30, 30), (3, 50, 40), (4, 5, 5), (5, 100, 99)",
    );
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

test "col op col: a < b" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM t WHERE a < b ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{1}, ids);
}

test "col op col: a = b" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM t WHERE a = b ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 4 }, ids);
}

test "col op col: a > b" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM t WHERE a > b ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 3, 5 }, ids);
}

test "col op col: composes with AND on a col-vs-literal predicate" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM t WHERE a > b AND id > 3 ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{5}, ids);
}

test "col op col: rejected when type tags differ" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(
        allocator,
        db,
        "CREATE TABLE m (id BIGINT PRIMARY KEY, name VARCHAR(8) NOT NULL, qty INT NOT NULL)",
    );

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(
        arena.allocator(),
        "SELECT id FROM m WHERE qty = name",
    );
    const cq = thindb.net.compile(allocator, db, root);
    if (cq) |ok| {
        var c = ok;
        c.deinit();
        return error.TestUnexpectedSuccess;
    } else |err| {
        try std.testing.expectEqual(thindb.exec.Error.PredicateTypeMismatch, err);
    }
}

test "col-col: mixed numeric types widen (double vs int)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const h = @import("sql_helpers.zig");
    try h.exec(allocator, db, "CREATE TABLE mx (id BIGINT PRIMARY KEY, a DOUBLE, b INT)");
    try h.exec(allocator, db, "INSERT INTO mx VALUES (1, 1.5, 1), (2, 2.0, 3), (3, 4.0, 4)");

    const ids = try h.collectBigints(allocator, db, "SELECT id FROM mx WHERE a > b ORDER BY id");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{1}, ids);

    const ids2 = try h.collectBigints(allocator, db, "SELECT id FROM mx WHERE b >= a ORDER BY id");
    defer allocator.free(ids2);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, ids2);
}

test "predicate: expression-led LHS anchors to a hidden computed column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const h = @import("sql_helpers.zig");
    try h.exec(allocator, db, "CREATE TABLE ex (id BIGINT PRIMARY KEY, i INT, d DOUBLE)");
    try h.exec(allocator, db, "INSERT INTO ex VALUES (1, 3, 1.5), (2, 12, 3.5), (3, -4, -2.5)");

    const a = try h.collectBigints(allocator, db, "SELECT id FROM ex WHERE i + 1 > 3 ORDER BY id");
    defer allocator.free(a);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, a);

    const b = try h.collectBigints(allocator, db, "SELECT id FROM ex WHERE (i * 2) BETWEEN 1 AND 20 ORDER BY id");
    defer allocator.free(b);
    try std.testing.expectEqualSlices(i64, &.{1}, b);

    const c = try h.collectBigints(allocator, db, "SELECT id FROM ex WHERE i + 1 IN (4, 8, 13) ORDER BY id");
    defer allocator.free(c);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, c);

    const e = try h.collectBigints(allocator, db, "SELECT id FROM ex WHERE ABS(d) BETWEEN 2 AND 3 ORDER BY id");
    defer allocator.free(e);
    try std.testing.expectEqualSlices(i64, &.{3}, e);

    const f = try h.collectBigints(allocator, db, "SELECT id FROM ex WHERE d * 2 NOT IN (3.0, 9.0) AND id < 3 ORDER BY id");
    defer allocator.free(f);
    try std.testing.expectEqualSlices(i64, &.{2}, f);
}
