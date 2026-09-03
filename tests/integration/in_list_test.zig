//! IN (literal list) / NOT IN (literal list). v1 desugars at parse
//! time to an OR-chain of equality leaves; NOT IN wraps the chain in
//! a logical NOT. IN against a subquery is deferred to the subquery
//! batch.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, region VARCHAR(8) NOT NULL)");
    try exec(
        allocator,
        db,
        "INSERT INTO t (id, region) VALUES (1, 'east'), (2, 'west'), (3, 'north'), (4, 'south'), (5, 'east')",
    );
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

test "IN: numeric list" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE id IN (1, 3, 5) ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 3, 5 }, ids);
}

test "IN: text list" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM t WHERE region IN ('east', 'west') ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 5 }, ids);
}

test "IN: NOT IN inverts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM t WHERE region NOT IN ('east', 'west') ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 3, 4 }, ids);
}

test "IN: single-element list still works" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE id IN (4)");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{4}, ids);
}

test "in-list: fractional literals against an integer column follow MySQL semantics" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const h = @import("sql_helpers.zig");
    try h.exec(allocator, db, "CREATE TABLE fx (id BIGINT PRIMARY KEY, x INT)");
    try h.exec(allocator, db, "INSERT INTO fx VALUES (1, 2), (2, 5), (3, NULL)");

    const in_ids = try h.collectBigints(allocator, db, "SELECT id FROM fx WHERE x IN (2.5, 5) ORDER BY id");
    defer allocator.free(in_ids);
    try std.testing.expectEqualSlices(i64, &.{2}, in_ids);

    const eq_ids = try h.collectBigints(allocator, db, "SELECT id FROM fx WHERE x = 2.5 ORDER BY id");
    defer allocator.free(eq_ids);
    try std.testing.expectEqual(@as(usize, 0), eq_ids.len);

    const whole_ids = try h.collectBigints(allocator, db, "SELECT id FROM fx WHERE x = 2.0 ORDER BY id");
    defer allocator.free(whole_ids);
    try std.testing.expectEqualSlices(i64, &.{1}, whole_ids);

    const neq_ids = try h.collectBigints(allocator, db, "SELECT id FROM fx WHERE x <> 2.5 ORDER BY id");
    defer allocator.free(neq_ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, neq_ids);

    const lt_ids = try h.collectBigints(allocator, db, "SELECT id FROM fx WHERE x < 2.5 ORDER BY id");
    defer allocator.free(lt_ids);
    try std.testing.expectEqualSlices(i64, &.{1}, lt_ids);

    const gt_ids = try h.collectBigints(allocator, db, "SELECT id FROM fx WHERE x > 2.5 ORDER BY id");
    defer allocator.free(gt_ids);
    try std.testing.expectEqualSlices(i64, &.{2}, gt_ids);
}
