//! UUID type — end-to-end coverage. A new type tag has to flow through
//! every layer (memtable append, segment write, segment read, predicate
//! eval, sort, group key, wire encoding). These tests prove all of
//! those land for `.uuid`.

const std = @import("std");
const thindb = @import("thindb");

const uuid_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "id", .type = .uuid },
        .{ .name = "name", .type = .string },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const uuid_order_key = [_][]const u8{"id"};
const uuid_opts = thindb.TableOptions{
    .order_key = &uuid_order_key,
    .unique = true,
    .row_group_size = 4,
};

test "uuid: insert, flush, scan returns the same u128 values" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const t = try db.table("users", uuid_schema, uuid_opts);

    const id_a: u128 = 0x550e8400e29b41d4a716446655440000;
    const id_b: u128 = 0x6ba7b8109dad11d180b400c04fd430c8;
    const id_c: u128 = 0xfedcba9876543210fedcba9876543210;

    try t.insert(&[_]struct { id: u128, name: []const u8 }{
        .{ .id = id_a, .name = "alice" },
        .{ .id = id_b, .name = "bob" },
        .{ .id = id_c, .name = "carol" },
    });
    try t.flush();

    var q = try thindb.scan(allocator, t);
    defer q.deinit();

    var ids: std.ArrayList(u128) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| try ids.appendSlice(allocator, b.values[0].data.uuid);

    // Sorted by order key (id), unsigned.
    //   0x550e... < 0x6ba7... < 0xfedc...
    try std.testing.expectEqualSlices(u128, &[_]u128{ id_a, id_b, id_c }, ids.items);
}

test "uuid: equality predicate filters to the matching row" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("users", uuid_schema, uuid_opts);

    const id_a: u128 = 0x11112222_33334444_55556666_77778888;
    const id_b: u128 = 0x99990000_11112222_33334444_55556666;

    try t.insert(&[_]struct { id: u128, name: []const u8 }{
        .{ .id = id_a, .name = "first" },
        .{ .id = id_b, .name = "second" },
    });
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var q = try base.filter(thindb.leafExpr("id", .eq, .{ .uuid = id_b }));
    defer q.deinit();

    var rows: usize = 0;
    var matched_id: u128 = 0;
    while (try q.next()) |b| {
        rows += b.row_count;
        if (b.row_count > 0) matched_id = b.values[0].data.uuid[0];
    }
    try std.testing.expectEqual(@as(usize, 1), rows);
    try std.testing.expectEqual(id_b, matched_id);
}

test "uuid: round-trips through the wire (in-process Connection)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    try conn.createTable("widgets", uuid_schema, uuid_opts);

    const u_a: u128 = 0xaaaaaaaa_bbbbbbbb_cccccccc_dddddddd;
    try conn.insert("widgets", &[_]struct { id: u128, name: []const u8 }{
        .{ .id = u_a, .name = "only" },
    });
    try conn.flush("widgets");

    var q = try conn.scan("widgets");
    defer q.deinit();

    var got: ?u128 = null;
    while (try q.next()) |b| {
        if (b.row_count > 0) got = b.values[0].data.uuid[0];
    }
    try std.testing.expectEqual(u_a, got.?);
}
