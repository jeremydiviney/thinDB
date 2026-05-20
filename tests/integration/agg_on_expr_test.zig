//! Aggregate-on-expression — `SUM(a * b)`, `AVG(qty * 1.05)`, etc.
//!
//! The parser detects an aggregate whose argument is an expression
//! (call / case / lit, not a plain col-ref or `*`) and hoists it
//! into a synthetic Compute column inserted BEFORE the GroupBy.
//! The AggSpec then references that synthetic column.
//!
//! Unblocks TPC-H Q1's
//!   `SUM(l_extendedprice * (1 - l_discount))` style aggregates.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db,
        "CREATE TABLE li (id BIGINT PRIMARY KEY, price BIGINT NOT NULL, qty BIGINT NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO li (id, price, qty) VALUES (1, 100, 2), (2, 200, 3), (3, 50, 5)",
    );
    const t = try db.openTable("li", .{});
    try t.flush();
    return db;
}

test "agg on expr: SUM(price * qty)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT SUM(price * qty) AS rev FROM li");
    defer q.deinit();
    const batch = (try q.next()).?;
    // 100*2 + 200*3 + 50*5 = 200 + 600 + 250 = 1050
    try std.testing.expectEqual(@as(i64, 1050), batch.values[0].data.bigint[0]);
}

test "agg on expr: AVG(price + qty)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT AVG(price + qty) AS m FROM li");
    defer q.deinit();
    const batch = (try q.next()).?;
    // (102 + 203 + 55) / 3 = 360 / 3 = 120
    try std.testing.expectEqual(@as(f64, 120.0), batch.values[0].data.double[0]);
}

test "agg on expr: mixed with plain col agg" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db,
        "SELECT SUM(price) AS total_price, SUM(price * qty) AS rev FROM li",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    // SUM(price) = 100+200+50 = 350
    // SUM(price * qty) = 1050
    try std.testing.expectEqual(@as(i64, 350), batch.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 1050), batch.values[1].data.bigint[0]);
}

test "agg on expr: GROUP BY column with agg-on-expr" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE sales (id BIGINT PRIMARY KEY, region VARCHAR(8) NOT NULL, price BIGINT NOT NULL, qty BIGINT NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO sales (id, region, price, qty) VALUES (1, 'east', 10, 2), (2, 'east', 20, 3), (3, 'west', 30, 1)",
    );
    const t = try db.openTable("sales", .{});
    try t.flush();

    var q = try runSql(allocator, db,
        "SELECT region, SUM(price * qty) AS rev FROM sales GROUP BY region ORDER BY region ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), batch.row_count);
    // east: 10*2 + 20*3 = 80; west: 30*1 = 30
    try std.testing.expectEqualStrings("east", batch.values[0].data.varchar.rowBytes(0));
    try std.testing.expectEqual(@as(i64, 80), batch.values[1].data.bigint[0]);
    try std.testing.expectEqualStrings("west", batch.values[0].data.varchar.rowBytes(1));
    try std.testing.expectEqual(@as(i64, 30), batch.values[1].data.bigint[1]);
}

// NOTE: SUM(CASE WHEN ...) currently crashes — the CASE branches
// produce INT but SUM widens to BIGINT and the type-marshaling
// between Compute and Aggregate has a gap. Captured as TODO; the
// pure-arithmetic agg-on-expr forms above all work.
