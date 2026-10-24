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

fn expectIds(a: std.mem.Allocator, t: *thindb.Table, expected: []const i64) !void {
    var q = try thindb.scan(a, t);
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(a);
    while (try q.next()) |b| try ids.appendSlice(a, b.values[0].data.bigint[0..b.row_count]);
    std.mem.sort(i64, ids.items, {}, std.sort.asc(i64));
    try std.testing.expectEqualSlices(i64, expected, ids.items);
}

test "durability: a log whose records a crash zeroed still opens" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const schema: thindb.TableSchema = .{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    {
        const db = try thindb.Database.open(a, io, tmp.dir, .{ .auto_flush_secs = 0, .sync_mode = .per_flush });
        defer db.close();
        const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
        try t.insert(&.{ .{ .id = @as(i64, 1) }, .{ .id = @as(i64, 2) } });
    }
    // The 2026-10-24 crash left logs that kept their synced header and read
    // back zeros for the rest of the block; opening one failed with
    // WalUnknownRecord, and every read of the table with it.
    const log = try tmp.dir.readFileAlloc(io, "main/public/t/wal", a, .unlimited);
    defer a.free(log);
    var torn: [4096]u8 = @splat(0);
    @memcpy(torn[0..32], log[0..32]);
    try tmp.dir.writeFile(io, .{ .sub_path = "main/public/t/wal", .data = &torn });
    {
        const db = try thindb.Database.open(a, io, tmp.dir, .{ .auto_flush_secs = 0, .sync_mode = .per_flush });
        defer db.close();
        const t = try db.openTable("t", .{});
        try expectIds(a, t, &.{ 1, 2 });
        try t.insert(&.{.{ .id = @as(i64, 3) }});
    }
    const db = try thindb.Database.open(a, io, tmp.dir, .{ .auto_flush_secs = 0 });
    defer db.close();
    try expectIds(a, try db.openTable("t", .{}), &.{ 1, 2, 3 });
}

test "durability: a segment a crash lost fails the open until the table is dropped" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const schema: thindb.TableSchema = .{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    inline for (.{ true, false }) |emptied| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        {
            const db = try thindb.Database.open(a, io, tmp.dir, .{ .auto_flush_secs = 0, .sync_mode = .per_flush });
            defer db.close();
            const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
            try t.insert(&.{.{ .id = @as(i64, 1) }});
            try t.flush();
            try t.insert(&.{.{ .id = @as(i64, 2) }});
            try t.flush();
        }
        var segments = try tmp.dir.openDir(io, "main/public/t/segments", .{ .iterate = true });
        defer segments.close(io);
        var name_buf: [64]u8 = undefined;
        const lost = blk: {
            var it = segments.iterate();
            while (try it.next(io)) |entry| {
                if (std.mem.endsWith(u8, entry.name, ".dat")) {
                    @memcpy(name_buf[0..entry.name.len], entry.name);
                    break :blk name_buf[0..entry.name.len];
                }
            }
            return error.TestUnexpectedResult;
        };
        // An OS crash keeps the new file's name and loses its unsynced
        // data, or loses the file outright.
        if (emptied) {
            try segments.writeFile(io, .{ .sub_path = lost, .data = "" });
        } else {
            try segments.deleteFile(io, lost);
        }
        // A file no manifest entry names: what a lost compaction's inputs
        // would be, and the sweep at open must not reclaim it now.
        try segments.writeFile(io, .{ .sub_path = "00000000000000099999.dat", .data = "kept" });

        const db = try thindb.Database.open(a, io, tmp.dir, .{ .auto_flush_secs = 0, .sync_mode = .per_flush });
        defer db.close();
        try std.testing.expectError(error.SegmentDamaged, db.openTable("t", .{}));
        try std.testing.expectError(error.SegmentDamaged, db.openTable("t", .{}));
        try segments.access(io, "00000000000000099999.dat", .{});
        try db.dropTable("t");
        const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
        try t.insert(&.{.{ .id = @as(i64, 5) }});
        try t.flush();
        try expectIds(a, t, &.{5});
    }
}
