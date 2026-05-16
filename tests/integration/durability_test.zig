//! Config.sync_mode tests — fsync round-trips for segments, manifest,
//! tombstones; verifies the default is .none.

const std = @import("std");
const thindb = @import("thindb");
const common = @import("common.zig");
const schema_v1 = common.schema_v1;
const opts_v1 = common.opts_v1;

test "sync_mode .per_flush round-trips through flush + delete + compact" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 4,
        .sync_mode = .per_flush,
        .compact_min_segments = 100,
        .compact_tombstone_threshold = 2.0,
        .auto_flush_secs = 0,
        .auto_flush_rows = 1_000_000,
        .auto_flush_bytes = 64 * 1024 * 1024,
    });
    defer db.close();

    const t = try db.table("orders", schema_v1, opts_v1);

    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "c" },
    });
    try t.flush();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    // Trigger a tombstone write (fsync'd) under sync_mode = .per_flush.
    _ = try t.delete(.{ .col = "qty", .op = .lt, .val = .{ .int = 15 } });

    // Compact (segment fsync + manifest fsync) — flip the threshold low.
    try t.compact();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    // Read back the survivors.
    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3 }, ids.items);
}

test "sync_mode .none is the default" {
    const db_cfg: thindb.Config = .{};
    try std.testing.expect(db_cfg.sync_mode == .none);
}
