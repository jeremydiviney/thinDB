//! Join operator integration tests. v1 covers inner equi-join via
//! hash algorithm with automatic build-side selection.
//!
//! Future tests (as features land): SMJ, INLJ, NLJ, outer joins,
//! semi/anti, multi-column keys, type-mismatch errors, etc.

const std = @import("std");
const thindb = @import("thindb");

const users_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "uid", .type = .bigint },
        .{ .name = "name", .type = .string },
    },
    .order_key = &.{"uid"},
    .unique = true,
};
const users_ok = [_][]const u8{"uid"};
const users_opts = thindb.TableOptions{
    .order_key = &users_ok,
    .unique = true,
    .row_group_size = 4,
};

// orders shares no column names with users → no collision on join.
// Join key is `uid` on both sides — right-side `uid` is dropped from
// output per USING-semantic.
const orders_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "oid", .type = .bigint },
        .{ .name = "uid", .type = .bigint },
        .{ .name = "qty", .type = .int },
    },
    .order_key = &.{"oid"},
    .unique = true,
};
const orders_ok = [_][]const u8{"oid"};
const orders_opts = thindb.TableOptions{
    .order_key = &orders_ok,
    .unique = true,
    .row_group_size = 4,
};

test "join: inner equi-join with single key returns matching rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const users = try db.table("users", users_schema, users_opts);
    try users.insert(&.{
        .{ .uid = @as(i64, 1), .name = "alice" },
        .{ .uid = @as(i64, 2), .name = "bob" },
        .{ .uid = @as(i64, 3), .name = "carol" },
    });
    try users.flush();

    const orders = try db.table("orders", orders_schema, orders_opts);
    try orders.insert(&.{
        .{ .oid = @as(i64, 100), .uid = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .oid = @as(i64, 101), .uid = @as(i64, 1), .qty = @as(i32, 20) },
        .{ .oid = @as(i64, 102), .uid = @as(i64, 2), .qty = @as(i32, 30) },
        .{ .oid = @as(i64, 103), .uid = @as(i64, 99), .qty = @as(i32, 40) }, // no matching user
    });
    try orders.flush();

    const left = try thindb.scan(allocator, users);
    const right = try thindb.scan(allocator, orders);
    var q = try left.join(right, .{
        .join_type = .inner,
        .on = &.{.{ .left = "uid", .right = "uid" }},
    });
    defer q.deinit();

    // Output schema: users.uid, users.name, orders.oid, orders.qty
    // (orders.uid dropped per USING-clause semantics)
    const schema = q.outputSchema();
    try std.testing.expectEqual(@as(usize, 4), schema.len);
    try std.testing.expectEqualStrings("uid", schema[0].name);
    try std.testing.expectEqualStrings("name", schema[1].name);
    try std.testing.expectEqualStrings("oid", schema[2].name);
    try std.testing.expectEqualStrings("qty", schema[3].name);

    // Collect output rows.
    var uids: std.ArrayList(i64) = .empty;
    defer uids.deinit(allocator);
    var oids: std.ArrayList(i64) = .empty;
    defer oids.deinit(allocator);
    var qtys: std.ArrayList(i32) = .empty;
    defer qtys.deinit(allocator);

    while (try q.next()) |b| {
        try uids.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
        try oids.appendSlice(allocator, b.values[2].data.bigint[0..b.row_count]);
        try qtys.appendSlice(allocator, b.values[3].data.int[0..b.row_count]);
    }

    // Expected output (any order — sort to verify):
    //   (uid=1, alice, oid=100, qty=10)
    //   (uid=1, alice, oid=101, qty=20)
    //   (uid=2, bob,   oid=102, qty=30)
    // orders.uid=99 has no matching user → dropped.
    try std.testing.expectEqual(@as(usize, 3), uids.items.len);

    // Sort the three parallel arrays by oid (the order is non-
    // deterministic depending on hash iteration). Bubble sort is
    // fine for n=3.
    var i: usize = 0;
    while (i < uids.items.len) : (i += 1) {
        var j: usize = i + 1;
        while (j < uids.items.len) : (j += 1) {
            if (oids.items[j] < oids.items[i]) {
                std.mem.swap(i64, &uids.items[i], &uids.items[j]);
                std.mem.swap(i64, &oids.items[i], &oids.items[j]);
                std.mem.swap(i32, &qtys.items[i], &qtys.items[j]);
            }
        }
    }

    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 2 }, uids.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 100, 101, 102 }, oids.items);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 10, 20, 30 }, qtys.items);
}

test "join: NULL join key never matches" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // Use nullable join key on the orders side.
    const orders_nullable = thindb.Schema{
        .columns = &.{
            .{ .name = "oid", .type = .bigint },
            .{ .name = "uid", .type = .bigint, .nullable = true },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"oid"},
        .unique = true,
    };
    const ok = [_][]const u8{"oid"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 8 };

    const users = try db.table("users", users_schema, users_opts);
    try users.insert(&.{
        .{ .uid = @as(i64, 1), .name = "alice" },
    });
    try users.flush();

    const orders = try db.table("orders", orders_nullable, opts);
    try orders.insert(&[_]struct { oid: i64, uid: ?i64, qty: i32 }{
        .{ .oid = 100, .uid = 1, .qty = 10 },
        .{ .oid = 101, .uid = null, .qty = 20 }, // null uid → no match
    });
    try orders.flush();

    const left = try thindb.scan(allocator, users);
    const right = try thindb.scan(allocator, orders);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "uid", .right = "uid" }},
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    // Only oid=100 matches; oid=101 has null uid which doesn't match.
    try std.testing.expectEqual(@as(usize, 1), rows);
}

test "join: empty build side produces empty output" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const users = try db.table("users", users_schema, users_opts);
    // No users inserted.

    const orders = try db.table("orders", orders_schema, orders_opts);
    try orders.insert(&.{
        .{ .oid = @as(i64, 100), .uid = @as(i64, 1), .qty = @as(i32, 10) },
    });
    try orders.flush();

    const left = try thindb.scan(allocator, users);
    const right = try thindb.scan(allocator, orders);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "uid", .right = "uid" }},
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 0), rows);
}

test "join: type mismatch on join key errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_a = thindb.Schema{
        .columns = &.{
            .{ .name = "a_id", .type = .bigint },
            .{ .name = "a_val", .type = .string },
        },
        .order_key = &.{"a_id"},
        .unique = true,
    };
    const schema_b = thindb.Schema{
        .columns = &.{
            .{ .name = "b_id", .type = .int }, // i32, not i64
            .{ .name = "b_val", .type = .string },
        },
        .order_key = &.{"b_id"},
        .unique = true,
    };
    const ok_a = [_][]const u8{"a_id"};
    const ok_b = [_][]const u8{"b_id"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const a = try db.table("a", schema_a, .{ .order_key = &ok_a, .unique = true });
    const b = try db.table("b", schema_b, .{ .order_key = &ok_b, .unique = true });

    var left = try thindb.scan(allocator, a);
    var right = try thindb.scan(allocator, b);
    try std.testing.expectError(
        thindb.exec.Error.JoinKeyTypeMismatch,
        left.join(right, .{
            .on = &.{.{ .left = "a_id", .right = "b_id" }},
        }),
    );

    // Clean up the queries that didn't get consumed by join.
    left.deinit();
    right.deinit();
}
