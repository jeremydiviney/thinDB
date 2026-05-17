//! Walking-skeleton tests for the new client/server surface.
//!
//! Verifies that `thindb.local() → conn.scan(name).limit(n) → q.next()`
//! round-trips through the operator IR (encoded + decoded) and produces
//! the right batches. Setup of table data uses the existing library API
//! via `thindb.net.underlyingDb(conn)` since we don't yet have a client-
//! facing write path.

const std = @import("std");
const thindb = @import("thindb");

const schema_v1 = thindb.Schema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "qty", .type = .int },
        .{ .name = "active", .type = .boolean },
        .{ .name = "tag", .type = .string },
    },
    .order_key = &.{"id"},
    .unique = true,
};

const order_key = [_][]const u8{"id"};
const opts_v1 = thindb.TableOptions{
    .order_key = &order_key,
    .unique = true,
    .row_group_size = 4,
};

test "ir roundtrip: conn.scan returns all rows when no limit" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    // Seed via existing library API (no client-facing write path yet).
    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "c" },
    });
    try orders.flush();

    // Client-side: build, dispatch, drain.
    var q = try conn.scan("orders");
    defer q.deinit();

    var total: usize = 0;
    while (try q.next()) |batch| total += batch.row_count;
    try std.testing.expectEqual(@as(usize, 3), total);
}

test "ir roundtrip: limit truncates the stream at N rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "c" },
        .{ .id = @as(i64, 4), .qty = @as(i32, 40), .active = true, .tag = "d" },
        .{ .id = @as(i64, 5), .qty = @as(i32, 50), .active = false, .tag = "e" },
    });
    try orders.flush();

    var base = try conn.scan("orders");
    var q = try base.limit(2);
    defer q.deinit();

    var total: usize = 0;
    while (try q.next()) |batch| total += batch.row_count;
    try std.testing.expectEqual(@as(usize, 2), total);
}

test "ir error: scanning a table that doesn't exist returns TableNotFound" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    var q = try conn.scan("does_not_exist");
    defer q.deinit();

    try std.testing.expectError(thindb.net.Error.TableNotFound, q.next());
}

test "select: keeps only the named columns, in the listed order" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
    });
    try orders.flush();

    var base = try conn.scan("orders");
    var q = try base.select(&.{ "tag", "id" });
    defer q.deinit();

    var saw_batches: usize = 0;
    var total_rows: usize = 0;
    while (try q.next()) |batch| {
        saw_batches += 1;
        total_rows += batch.row_count;
        // Schema must be exactly the selected columns, in order.
        try std.testing.expectEqual(@as(usize, 2), batch.schema.len);
        try std.testing.expectEqualStrings("tag", batch.schema[0].name);
        try std.testing.expectEqualStrings("id", batch.schema[1].name);
    }
    try std.testing.expect(saw_batches > 0);
    try std.testing.expectEqual(@as(usize, 2), total_rows);
}

test "exclude: drops named columns and downstream cannot see them" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
    });
    try orders.flush();

    // schema_v1 columns: id, qty, active, tag.  Exclude active + tag.
    var base = try conn.scan("orders");
    var q = try base.exclude(&.{ "active", "tag" });
    defer q.deinit();

    while (try q.next()) |batch| {
        try std.testing.expectEqual(@as(usize, 2), batch.schema.len);
        try std.testing.expectEqualStrings("id", batch.schema[0].name);
        try std.testing.expectEqualStrings("qty", batch.schema[1].name);
    }
}

test "strict semantic: selecting an excluded column fails" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
    });
    try orders.flush();

    // Exclude "tag", then try to select it back. Server-side project
    // can't find the column → builds error from the existing exec layer.
    var base = try conn.scan("orders");
    var excluded = try base.exclude(&.{"tag"});
    var q = try excluded.select(&.{ "id", "tag" });
    defer q.deinit();

    // The build of the downstream project should fail because "tag" is
    // no longer in upstream's schema. exec.Project returns a ColumnNotFound
    // error from the exec error set.
    try std.testing.expectError(error.ColumnNotFound, q.next());
}

test "select then limit chain works" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "c" },
    });
    try orders.flush();

    var base = try conn.scan("orders");
    var projected = try base.select(&.{"id"});
    var q = try projected.limit(2);
    defer q.deinit();

    var total: usize = 0;
    while (try q.next()) |batch| {
        total += batch.row_count;
        try std.testing.expectEqual(@as(usize, 1), batch.schema.len);
        try std.testing.expectEqualStrings("id", batch.schema[0].name);
    }
    try std.testing.expectEqual(@as(usize, 2), total);
}
