//! IN / NOT IN against a subquery — pre-compile pass drains the inner,
//! materializes the column into a Value slice, evaluator does linear
//! scan per row.
//!
//! thinDB dialect: NULLs in the materialized set are dropped (both IN
//! and NOT IN). This deliberately deviates from SQL-standard 3VL on
//! NOT IN. See memory thindb_not_in_nonstandard for the rationale.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE customer (id BIGINT PRIMARY KEY, region VARCHAR(8) NOT NULL)");
    try exec(allocator, db, "CREATE TABLE premium (cust_id BIGINT PRIMARY KEY)");
    try exec(allocator, db, "CREATE TABLE blocked (cust_id BIGINT NULL, note VARCHAR(8) NULL, PRIMARY KEY (note))");
    try exec(allocator, db,
        "INSERT INTO customer (id, region) VALUES (1, 'east'), (2, 'east'), (3, 'west'), (4, 'north'), (5, 'west')",
    );
    try exec(allocator, db, "INSERT INTO premium (cust_id) VALUES (2), (4)");
    // blocked has a non-null and a null cust_id; the NULL gets dropped from the set per dialect.
    try exec(allocator, db, "INSERT INTO blocked (cust_id, note) VALUES (3, 'a'), (NULL, 'b')");
    const t1 = try db.openTable("customer", .{});
    try t1.flush();
    const t2 = try db.openTable("premium", .{});
    try t2.flush();
    const t3 = try db.openTable("blocked", .{});
    try t3.flush();
    return db;
}

test "IN (subquery): basic set membership" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SELECT id FROM customer WHERE id IN (SELECT cust_id FROM premium) ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 4 }, ids);
}

test "NOT IN (subquery) with no NULLs in the set" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SELECT id FROM customer WHERE id NOT IN (SELECT cust_id FROM premium) ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 3, 5 }, ids);
}

test "NOT IN (subquery) with NULL in set — thinDB dialect drops the NULL" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // blocked.cust_id = [3, NULL]. Dialect: NULL dropped → set is {3}.
    // SQL-standard would filter out every row (NULL contamination).
    // thinDB returns all customers whose id != 3.
    const ids = try collectBigints(allocator, db,
        "SELECT id FROM customer WHERE id NOT IN (SELECT cust_id FROM blocked) ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 4, 5 }, ids);
}

test "IN (subquery): text column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE products (id BIGINT PRIMARY KEY, region VARCHAR(8) NOT NULL)");
    try exec(allocator, db, "CREATE TABLE active_regions (name VARCHAR(8) PRIMARY KEY)");
    try exec(allocator, db,
        "INSERT INTO products (id, region) VALUES (1, 'east'), (2, 'west'), (3, 'south'), (4, 'east')",
    );
    try exec(allocator, db, "INSERT INTO active_regions (name) VALUES ('east'), ('west')");
    const t1 = try db.openTable("products", .{});
    try t1.flush();
    const t2 = try db.openTable("active_regions", .{});
    try t2.flush();

    const ids = try collectBigints(allocator, db,
        "SELECT id FROM products WHERE region IN (SELECT name FROM active_regions) ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 4 }, ids);
}

test "IN (subquery): empty inner makes IN always-false" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SELECT id FROM customer WHERE id IN (SELECT cust_id FROM premium WHERE cust_id = 999)",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{}, ids);
}

test "NOT IN (subquery): empty inner passes every row" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SELECT id FROM customer WHERE id NOT IN (SELECT cust_id FROM premium WHERE cust_id = 999) ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5 }, ids);
}

test "IN (subquery): multi-column inner rejected" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(),
        "SELECT id FROM customer WHERE id IN (SELECT cust_id, 1 AS marker FROM premium)",
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
