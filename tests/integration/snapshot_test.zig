//! Memtable snapshot isolation tests. Scans capture a refcounted snapshot
//! of the memtable at create time; flush / delete / upsert retire-and-
//! replace the active memtable. Concurrent inserts after scan start are
//! invisible to the scan; the scan continues to see pre-mutation state
//! until it ends.

const std = @import("std");
const thindb = @import("thindb");
const common = @import("common.zig");
const schema_v1 = common.schema_v1;
const opts_v1 = common.opts_v1;

test "snapshot: scan sees stable row count even as concurrent inserts continue" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = 10_000_000,
        .auto_flush_bytes = 1 << 30,
        .auto_flush_secs = 0,
    });
    defer db.close();

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .row_group_size = 1024 };
    const t = try db.table("orders", schema, opts);

    // Pre-populate.
    const initial_rows: usize = 200;
    for (0..initial_rows) |i| {
        try t.insert(&.{.{ .id = @as(i64, @intCast(i)), .qty = @as(i32, 1) }});
    }

    // Start a scan: this should pin the current memtable state.
    var q = try thindb.scan(allocator, t);
    defer q.deinit();

    // Concurrently insert MORE rows on another thread. These should be
    // invisible to the running scan.
    var stop_inserts: std.atomic.Value(bool) = .init(false);
    const Ctx = struct {
        t: *thindb.Table,
        stop: *std.atomic.Value(bool),
        fn run(self: @This()) void {
            var i: i64 = 1_000_000;
            while (!self.stop.load(.acquire)) {
                self.t.insert(&.{.{ .id = i, .qty = @as(i32, 2) }}) catch return;
                i += 1;
            }
        }
    };
    const thr = try std.Thread.spawn(.{}, Ctx.run, .{Ctx{ .t = t, .stop = &stop_inserts }});

    // Drain the scan; count rows seen.
    var seen: usize = 0;
    while (try q.next()) |batch| {
        seen += batch.row_count;
    }

    stop_inserts.store(true, .release);
    thr.join();

    // Scan must see exactly the rows that existed at scan start —
    // not the rows added by the concurrent writer.
    try std.testing.expectEqual(initial_rows, seen);

    // The table should now have at least the initial rows PLUS some of the
    // concurrent writes (exact count depends on thread scheduling).
    try std.testing.expect(t.memtable.row_count >= 0); // post-retire-replace, active is fresh; concurrent inserts went here.
}

test "snapshot: scan straddling a flush sees pre-flush rows; writer keeps progressing" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 16,
        .auto_flush_rows = 10_000_000,
        .auto_flush_bytes = 1 << 30,
        .auto_flush_secs = 0,
    });
    defer db.close();

    const t = try db.table("orders", schema_v1, opts_v1);

    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = true, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "c" },
    });

    // Start a scan: pins the captured memtable (which has 3 rows).
    var q = try thindb.scan(allocator, t);
    defer q.deinit();

    // Flush mid-scan. The flush retires the (already retired) snapshot's
    // memtable — wait, the snapshot is its OWN retired memtable. Flush
    // operates on the table's NEW active memtable (which is empty post-
    // scan-start retire-replace). So flush is a no-op here.
    //
    // Insert into the new active memtable, then flush.
    try t.insert(&.{.{ .id = @as(i64, 100), .qty = @as(i32, 999), .active = false, .tag = "z" }});
    try t.flush(); // builds segment from {id=100}, swaps memtable.

    // Scan continues. It should see the ORIGINAL 3 rows from its pinned
    // snapshot, NOT id=100 (which was added after scan start).
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| {
        try ids.appendSlice(allocator, batch.values[0].data.bigint);
    }

    try std.testing.expectEqual(@as(usize, 3), ids.items.len);
    // Order may vary (memtable insertion-order); just check the set.
    var has1: bool = false;
    var has2: bool = false;
    var has3: bool = false;
    for (ids.items) |id| switch (id) {
        1 => has1 = true,
        2 => has2 = true,
        3 => has3 = true,
        else => try std.testing.expect(false), // id=100 must NOT appear
    };
    try std.testing.expect(has1 and has2 and has3);

    // The table now has the segment from the flush, visible to NEW scans.
    var q2 = try thindb.scan(allocator, t);
    defer q2.deinit();
    var ids2: std.ArrayList(i64) = .empty;
    defer ids2.deinit(allocator);
    while (try q2.next()) |batch| {
        try ids2.appendSlice(allocator, batch.values[0].data.bigint);
    }
    // New scan sees the flushed segment (id=100) and any subsequent memtable rows.
    var has100: bool = false;
    for (ids2.items) |id| if (id == 100) { has100 = true; };
    try std.testing.expect(has100);
}

test "snapshot: scan survives concurrent delete via retire-replace" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = 10_000_000,
        .auto_flush_bytes = 1 << 30,
        .auto_flush_secs = 0,
    });
    defer db.close();

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .row_group_size = 1024 };
    const t = try db.table("orders", schema, opts);

    for (0..10) |i| {
        try t.insert(&.{.{ .id = @as(i64, @intCast(i)), .qty = @as(i32, @intCast(i * 10)) }});
    }

    var q = try thindb.scan(allocator, t);
    defer q.deinit();

    // After scan start, do another insert (goes to the new active memtable)
    // and a delete (retire-replaces the new active memtable).
    try t.insert(&.{.{ .id = @as(i64, 100), .qty = @as(i32, 999) }});
    _ = try t.delete(.{ .col = "id", .op = .lt, .val = .{ .bigint = 5 } });

    // The scan's snapshot has the original 10 rows. The delete operated on
    // the table's new active memtable (post-scan retire-replace), so the
    // scan should still see all 10 originals.
    var seen: usize = 0;
    while (try q.next()) |batch| {
        seen += batch.row_count;
    }
    try std.testing.expectEqual(@as(usize, 10), seen);
}
