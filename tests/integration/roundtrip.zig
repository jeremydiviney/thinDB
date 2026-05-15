//! End-to-end v0.1 round-trip:
//!   open → create table → insert (segments + memtable) → reopen → piped query
//!
//! Uses only the public `thindb.*` API.

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

test "roundtrip: insert, flush, reopen, piped query" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // --- session 1: create + insert + flush --------------------------------
    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
        defer db.close();

        const orders = try db.table("orders", schema_v1, opts_v1);

        // Batch 1 → flushed as segment 1
        try orders.insert(&.{
            .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "alpha" },
            .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "beta" },
            .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "gamma" },
            .{ .id = @as(i64, 4), .qty = @as(i32, 40), .active = true, .tag = "delta" },
            .{ .id = @as(i64, 5), .qty = @as(i32, 50), .active = false, .tag = "epsilon" },
        });
        try orders.flush();

        // Batch 2 → flushed as segment 2
        try orders.insert(&.{
            .{ .id = @as(i64, 6), .qty = @as(i32, 60), .active = true, .tag = "zeta" },
            .{ .id = @as(i64, 7), .qty = @as(i32, 70), .active = true, .tag = "eta" },
        });
        try orders.flush();

        // Batch 3 stays in memtable until close
        try orders.insert(&.{
            .{ .id = @as(i64, 8), .qty = @as(i32, 80), .active = true, .tag = "theta" },
            .{ .id = @as(i64, 9), .qty = @as(i32, 90), .active = false, .tag = "iota" },
        });
        // Flush so memtable rows survive reopen — v0.1 doesn't durable-write memtable.
        try orders.flush();

        try std.testing.expectEqual(@as(usize, 3), orders.segmentCount());
    }

    // --- session 2: reopen + piped query -----------------------------------
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();

    const orders = try db.table("orders", schema_v1, opts_v1);
    try std.testing.expectEqual(@as(usize, 3), orders.segmentCount());

    // A reusable transform expressed as a function over Query.
    const onlyActive = struct {
        fn apply(q: thindb.Query) anyerror!thindb.Query {
            return q.filter(thindb.leafExpr("active", .eq, .{ .boolean = true }));
        }
    }.apply;

    // scan → only_active → qty > 25 → project(id, tag) → limit(3)
    var base = try thindb.scan(allocator, orders);
    var piped = try base.pipe(onlyActive);
    var filtered = try piped.filter(thindb.leafExpr("qty", .gt, .{ .int = 25 }));
    var projected = try filtered.project(&.{ "id", "tag" });
    var q = try projected.limit(3);
    defer q.deinit();

    // Expected matches: id ∈ {3, 4, 6, 7, 8}; first 3 are {3, 4, 6} with tags
    // {"gamma", "delta", "zeta"}.
    var collected_ids: std.ArrayList(i64) = .empty;
    defer collected_ids.deinit(allocator);
    var collected_tags: std.ArrayList(u8) = .empty;
    defer collected_tags.deinit(allocator);

    while (try q.next()) |batch| {
        try std.testing.expectEqual(@as(usize, 2), batch.schema.len);
        try std.testing.expectEqualStrings("id", batch.schema[0].name);
        try std.testing.expectEqualStrings("tag", batch.schema[1].name);
        try collected_ids.appendSlice(allocator, batch.values[0].bigint);
        for (0..batch.row_count) |i| {
            try collected_tags.append(allocator, '|');
            try collected_tags.appendSlice(allocator, batch.values[1].string.rowBytes(i));
        }
    }

    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 4, 6 }, collected_ids.items);
    try std.testing.expectEqualStrings("|gamma|delta|zeta", collected_tags.items);
}

test "roundtrip: empty table scan yields no batches" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const orders = try db.table("orders", schema_v1, opts_v1);
    var q = try thindb.scan(allocator, orders);
    defer q.deinit();

    try std.testing.expect((try q.next()) == null);
}

test "roundtrip: scan reads memtable without a flush" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
    });

    var base = try thindb.scan(allocator, orders);
    var q = try base.filter(thindb.leafExpr("qty", .gte, .{ .int = 15 }));
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expectEqualSlices(i64, &[_]i64{2}, b.values[0].bigint);
}
