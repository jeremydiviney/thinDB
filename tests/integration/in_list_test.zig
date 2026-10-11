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

// An IN list prunes row groups by its values' zone-map order; literals of
// another type than the column still match the rows they equal.
test "in-list: literals of another type prune row groups without losing rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .auto_flush_secs = 0 });
    defer db.close();

    const schema = thindb.TableSchema{
        .columns = &.{ .{ .name = "k", .type = .bigint }, .{ .name = "x", .type = .int } },
        .order_key = &.{"k"},
        .unique = false,
    };
    const t = try db.table("t", schema, .{ .order_key = &.{"k"}, .unique = false, .row_group_size = 4 });
    const Row = struct { k: i64, x: i32 };
    var rows: [40]Row = undefined;
    for (&rows, 0..) |*row, k| row.* = .{ .k = @intCast(k), .x = @intCast(k) };
    try t.insert(&rows);
    try t.flush();

    const cases = .{
        .{ "SELECT k FROM t WHERE k IN (1, 22.0) ORDER BY k", &[_]i64{ 1, 22 } },
        .{ "SELECT k FROM t WHERE k IN (1, '22') ORDER BY k", &[_]i64{ 1, 22 } },
        .{ "SELECT k FROM t WHERE k IN (1.5, 22) ORDER BY k", &[_]i64{22} },
        .{ "SELECT k FROM t WHERE x IN (1, 22.0) ORDER BY k", &[_]i64{ 1, 22 } },
        .{ "SELECT k FROM t WHERE x IN (1, '22') ORDER BY k", &[_]i64{ 1, 22 } },
        .{ "SELECT k FROM t WHERE x IN (1.5, 22) ORDER BY k", &[_]i64{22} },
    };
    inline for (cases) |case| {
        errdefer std.debug.print("case: {s}\n", .{case[0]});
        const got = try collectBigints(allocator, db, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, case[1], got);
    }
}

test "in-list: equal-length lists on different columns each test their own literals" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE s (id BIGINT PRIMARY KEY, a VARCHAR(8) NOT NULL, b VARCHAR(8) NOT NULL)");
    var insert: std.ArrayList(u8) = .empty;
    defer insert.deinit(allocator);
    try insert.appendSlice(allocator, "INSERT INTO s VALUES ");
    for (0..60) |i| {
        if (i > 0) try insert.appendSlice(allocator, ", ");
        try insert.print(allocator, "({d}, 'v{d}', 'v{d}')", .{ i, i % 20, (i * 7) % 20 });
    }
    try exec(allocator, db, insert.items);
    try (try db.openTable("s", .{})).flush();

    // Nine literals per column: past the length compared one by one, so
    // each column's list is hashed in turn from the same scratch buffer.
    var not_in: i64 = 0;
    var in: i64 = 0;
    for (0..60) |i| {
        const a_listed = i % 20 <= 8;
        const b_listed = (i * 7) % 20 >= 10 and (i * 7) % 20 <= 18;
        if (!a_listed and !b_listed) not_in += 1;
        if (a_listed and b_listed) in += 1;
    }
    const cases = .{
        .{ "SELECT id FROM s WHERE a NOT IN ('v0', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8') " ++
            "AND b NOT IN ('v10', 'v11', 'v12', 'v13', 'v14', 'v15', 'v16', 'v17', 'v18')", not_in },
        .{ "SELECT id FROM s WHERE (a = 'v0' OR a = 'v1' OR a = 'v2' OR a = 'v3' OR a = 'v4' OR a = 'v5' OR a = 'v6' OR a = 'v7' OR a = 'v8') " ++
            "AND (b = 'v10' OR b = 'v11' OR b = 'v12' OR b = 'v13' OR b = 'v14' OR b = 'v15' OR b = 'v16' OR b = 'v17' OR b = 'v18')", in },
    };
    inline for (cases) |c| {
        const ids = try collectBigints(allocator, db, c[0]);
        defer allocator.free(ids);
        try std.testing.expectEqual(@as(usize, @intCast(c[1])), ids.len);
    }
}
