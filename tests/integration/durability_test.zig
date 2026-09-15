//! Config.sync_mode tests — fsync round-trips for segments, manifest,
//! tombstones; verifies the default is .none.

const std = @import("std");
const thindb = @import("thindb");
const common = @import("common.zig");
const schema_v1 = common.schema_v1;
const opts_v1 = common.opts_v1;

test "durability: truncate preserves rows on publication failure and survives WAL cleanup failure" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        const db = try thindb.Database.open(a, io, tmp.dir, .{ .auto_flush_secs = 0 });
        defer db.close();
        const t = try db.table("t", .{
            .columns = &.{.{ .name = "id", .type = .bigint }},
            .order_key = &.{"id"},
            .unique = false,
        }, .{ .order_key = &.{"id"} });
        try t.insert(&.{.{ .id = @as(i64, 1) }});
        try t.flush();
        try t.insert(&.{.{ .id = @as(i64, 2) }});
        try t.table_dir.createDir(io, "manifest.tmp", .default_dir);
        var publication_failed = false;
        t.truncate() catch {
            publication_failed = true;
        };
        try std.testing.expect(publication_failed);
        {
            var q = try thindb.scan(a, t);
            defer q.deinit();
            var sum: i64 = 0;
            var rows: usize = 0;
            while (try q.next()) |batch| {
                rows += batch.row_count;
                for (batch.values[0].data.bigint[0..batch.row_count]) |id| sum += id;
            }
            try std.testing.expectEqual(@as(usize, 2), rows);
            try std.testing.expectEqual(@as(i64, 3), sum);
        }
        try t.table_dir.deleteDir(io, "manifest.tmp");
        try t.table_dir.createDir(io, "wal.tmp", .default_dir);
        var cleanup_failed = false;
        t.truncate() catch {
            cleanup_failed = true;
        };
        try std.testing.expect(cleanup_failed);
        {
            var q = try thindb.scan(a, t);
            defer q.deinit();
            try std.testing.expectEqual(@as(?thindb.Batch, null), try q.next());
        }
        try t.table_dir.deleteDir(io, "wal.tmp");
    }
    const db = try thindb.Database.open(a, io, tmp.dir, .{});
    defer db.close();
    const t = try db.openTable("t", .{});
    {
        var q = try thindb.scan(a, t);
        defer q.deinit();
        try std.testing.expectEqual(@as(?thindb.Batch, null), try q.next());
    }
    try t.insert(&.{.{ .id = @as(i64, 3) }});
    try t.flush();
    var q = try thindb.scan(a, t);
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqualSlices(i64, &.{3}, batch.values[0].data.bigint[0..batch.row_count]);
    try std.testing.expectEqual(@as(?thindb.Batch, null), try q.next());
}

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

test "durability: failed flush publication and WAL cleanup never duplicate rows" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const schema: thindb.TableSchema = .{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    inline for (.{ "manifest.tmp", "wal.tmp" }) |blocked_path| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        {
            const db = try thindb.Database.open(a, io, tmp.dir, .{ .auto_flush_secs = 0, .sync_mode = .per_flush });
            defer db.close();
            const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
            try t.insert(&.{.{ .id = @as(i64, 7) }});
            try t.table_dir.createDir(io, blocked_path, .default_dir);
            var failed = false;
            t.flush() catch {
                failed = true;
            };
            try std.testing.expect(failed);
            {
                var q = try thindb.scan(a, t);
                defer q.deinit();
                var rows: usize = 0;
                while (try q.next()) |b| {
                    for (b.values[0].data.bigint) |v| try std.testing.expectEqual(@as(i64, 7), v);
                    rows += b.row_count;
                }
                try std.testing.expectEqual(@as(usize, 1), rows);
            }
            try t.table_dir.deleteDir(io, blocked_path);
            if (comptime std.mem.eql(u8, blocked_path, "manifest.tmp")) try t.flush();
        }
        const reopened = try thindb.Database.open(a, io, tmp.dir, .{ .auto_flush_secs = 0 });
        defer reopened.close();
        const t = try reopened.openTable("t", .{});
        try t.insert(&.{.{ .id = @as(i64, 8) }});
        try t.flush();
        var q = try thindb.scan(a, t);
        defer q.deinit();
        var rows: usize = 0;
        var sum: i64 = 0;
        while (try q.next()) |b| {
            rows += b.row_count;
            for (b.values[0].data.bigint) |v| sum += v;
        }
        try std.testing.expectEqual(@as(usize, 2), rows);
        try std.testing.expectEqual(@as(i64, 15), sum);
    }
}

test "durability: published manifest skips its untruncated WAL generation" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const schema: thindb.TableSchema = .{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    var before_flush: []u8 = undefined;
    {
        const db = try thindb.Database.open(a, io, tmp.dir, .{ .auto_flush_secs = 0 });
        defer db.close();
        const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
        try t.insert(&.{.{ .id = @as(i64, 9) }});
        before_flush = try t.table_dir.readFileAlloc(io, "wal", a, .unlimited);
        errdefer a.free(before_flush);
        try t.flush();
    }
    defer a.free(before_flush);
    try tmp.dir.writeFile(io, .{ .sub_path = "main/public/t/wal", .data = before_flush });
    const db = try thindb.Database.open(a, io, tmp.dir, .{ .auto_flush_secs = 0 });
    defer db.close();
    const t = try db.openTable("t", .{});
    var q = try thindb.scan(a, t);
    defer q.deinit();
    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 1), rows);
}
