//! Tests for `src/api/api.zig`. Brought in via the parent file's `test`
//! block, so `zig build test` discovers them.

const std = @import("std");
const api = @import("api.zig");
const Database = api.Database;
const Table = api.Table;
const Error = api.Error;

const types = @import("../types.zig");
const TableSchema = types.TableSchema;

const storage = @import("../storage/storage.zig");
const exec = @import("../exec/exec.zig");

const thindb_scan = exec.scan;

test "Database open/close with no tables" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
}

test "Table insert + flush writes a segment, manifest reflects it" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = true,
    };

    {
        var db = try Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
        defer db.close();

        const orders = try db.table("orders", schema, .{ .order_key = &.{"id"}, .unique = true });

        try orders.insert(&.{
            .{ .id = @as(i64, 1), .qty = @as(i32, 10), .tag = "a" },
            .{ .id = @as(i64, 2), .qty = @as(i32, 20), .tag = "bb" },
            .{ .id = @as(i64, 3), .qty = @as(i32, 30), .tag = "ccc" },
            .{ .id = @as(i64, 4), .qty = @as(i32, 40), .tag = "dddd" },
            .{ .id = @as(i64, 5), .qty = @as(i32, 50), .tag = "" },
        });
        try std.testing.expectEqual(@as(u64, 5), orders.memtable.row_count);

        try orders.flush();
        try std.testing.expectEqual(@as(u64, 0), orders.memtable.row_count);
        try std.testing.expectEqual(@as(usize, 1), orders.segmentCount());
    }

    // Reopen — manifest should show the segment, and segment file should be readable.
    {
        var db = try Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
        defer db.close();

        const orders = try db.table("orders", schema, .{ .order_key = &.{"id"}, .unique = true });
        try std.testing.expectEqual(@as(usize, 1), orders.segmentCount());

        const seg_id = orders.manifest.segments.items[0].segment_id;
        var name_buf: [32]u8 = undefined;
        const file_name = try Table.segmentFileName(&name_buf, seg_id);

        var seg = try storage.readSegment(allocator, io, orders.segments_dir, file_name, schema);
        defer seg.deinit();

        try std.testing.expectEqual(@as(u64, 5), seg.info.row_count);
        // 5 rows / 4-row groups → 2 row groups
        try std.testing.expectEqual(@as(usize, 2), seg.info.row_groups.len);

        // Decode the id column across both row groups, verify the full sequence.
        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        for (seg.info.row_groups, 0..) |_, rg_idx| {
            var c = try seg.decodeColumn(allocator, schema, rg_idx, 0);
            defer c.deinit(allocator);
            try ids.appendSlice(allocator, c.data.bigint);
        }
        try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4, 5 }, ids.items);
    }
}

test "Flush writes segment sorted by order key" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = false,
    };

    var db = try Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });

    // Insert deliberately out of order.
    try t.insert(&.{
        .{ .id = @as(i64, 5), .tag = "e" },
        .{ .id = @as(i64, 2), .tag = "b" },
        .{ .id = @as(i64, 9), .tag = "i" },
        .{ .id = @as(i64, 1), .tag = "a" },
        .{ .id = @as(i64, 7), .tag = "g" },
    });
    try t.flush();

    // Read the segment back and verify ids are sorted.
    const seg_id = t.manifest.segments.items[0].segment_id;
    var name_buf: [32]u8 = undefined;
    const file_name = try Table.segmentFileName(&name_buf, seg_id);

    var seg = try storage.readSegment(allocator, io, t.segments_dir, file_name, schema);
    defer seg.deinit();

    var col = try seg.decodeColumn(allocator, schema, 0, 0);
    defer col.deinit(allocator);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 5, 7, 9 }, col.data.bigint);

    var tag_col = try seg.decodeColumn(allocator, schema, 0, 1);
    defer tag_col.deinit(allocator);
    const sv = tag_col.data.string;
    try std.testing.expectEqualStrings("a", sv.view().rowBytes(0));
    try std.testing.expectEqualStrings("b", sv.view().rowBytes(1));
    try std.testing.expectEqualStrings("e", sv.view().rowBytes(2));
    try std.testing.expectEqualStrings("g", sv.view().rowBytes(3));
    try std.testing.expectEqualStrings("i", sv.view().rowBytes(4));
}

test "Flush sorts by composite order key (lexicographic)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "user_id", .type = .bigint },
            .{ .name = "ts", .type = .bigint },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{ "user_id", "ts" },
        .unique = false,
    };

    var db = try Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{ "user_id", "ts" } });

    try t.insert(&.{
        .{ .user_id = @as(i64, 1), .ts = @as(i64, 200), .tag = "a" },
        .{ .user_id = @as(i64, 2), .ts = @as(i64, 100), .tag = "b" },
        .{ .user_id = @as(i64, 1), .ts = @as(i64, 100), .tag = "c" },
        .{ .user_id = @as(i64, 2), .ts = @as(i64, 50), .tag = "d" },
    });
    try t.flush();

    const seg_id = t.manifest.segments.items[0].segment_id;
    var name_buf: [32]u8 = undefined;
    const file_name = try Table.segmentFileName(&name_buf, seg_id);
    var seg = try storage.readSegment(allocator, io, t.segments_dir, file_name, schema);
    defer seg.deinit();

    var u_col = try seg.decodeColumn(allocator, schema, 0, 0);
    defer u_col.deinit(allocator);
    var ts_col = try seg.decodeColumn(allocator, schema, 0, 1);
    defer ts_col.deinit(allocator);

    // Expected order: (1,100,c), (1,200,a), (2,50,d), (2,100,b)
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 2, 2 }, u_col.data.bigint);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 100, 200, 50, 100 }, ts_col.data.bigint);
}

test "Table reopen with different schema returns mismatch" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try Database.open(allocator, io, tmp.dir, .{});
        defer db.close();

        const schema_a = TableSchema{
            .columns = &.{.{ .name = "id", .type = .bigint }},
            .order_key = &.{"id"},
            .unique = false,
        };
        const t = try db.table("t", schema_a, .{ .order_key = &.{"id"} });
        try t.insert(&[_]struct { id: i64 }{.{ .id = 1 }});
        try t.flush();
    }

    {
        var db = try Database.open(allocator, io, tmp.dir, .{});
        defer db.close();

        const schema_b = TableSchema{
            .columns = &.{.{ .name = "id", .type = .int }}, // <-- changed type
            .order_key = &.{"id"},
            .unique = false,
        };
        const result = db.table("t", schema_b, .{ .order_key = &.{"id"} });
        try std.testing.expectError(storage.schema_file.Error.SchemaMismatch, result);
    }
}

test "openTable reads persisted schema without caller supplying it" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = false,
    };

    {
        var db = try Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
        try t.insert(&.{
            .{ .id = @as(i64, 1), .tag = "a" },
            .{ .id = @as(i64, 2), .tag = "b" },
        });
        try t.flush();
    }

    {
        var db = try Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        // Re-open without supplying the schema.
        const t = try db.openTable("t", .{});
        try std.testing.expect(storage.schema_file.schemasEqual(schema, t.schema));
        try std.testing.expectEqual(@as(usize, 1), t.segmentCount());
    }
}

test "delete tombstones matching rows in flushed segments" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "status", .type = .{ .varchar = 16 } },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });

    try t.insert(&.{
        .{ .id = @as(i64, 1), .status = "paid" },
        .{ .id = @as(i64, 2), .status = "pending" },
        .{ .id = @as(i64, 3), .status = "paid" },
        .{ .id = @as(i64, 4), .status = "cancelled" },
        .{ .id = @as(i64, 5), .status = "paid" },
    });
    try t.flush();

    const deleted = try t.delete(.{
        .col = "status",
        .op = .eq,
        .val = .{ .text = "paid" },
    });
    try std.testing.expectEqual(@as(usize, 3), deleted);

    // Scan should now see only id=2 and id=4
    var q = try thindb_scan(allocator, t);
    defer q.deinit();

    var collected: std.ArrayList(i64) = .empty;
    defer collected.deinit(allocator);
    while (try q.next()) |b| {
        try collected.appendSlice(allocator, b.values[0].data.bigint);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 4 }, collected.items);
}

test "upsert works with compound (bigint, bigint) order key" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "user_id", .type = .bigint },
            .{ .name = "ts", .type = .bigint },
            .{ .name = "val", .type = .int },
        },
        .order_key = &.{ "user_id", "ts" },
        .unique = true,
    };
    var db = try Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{ "user_id", "ts" }, .unique = true });

    try t.insert(&.{
        .{ .user_id = @as(i64, 1), .ts = @as(i64, 100), .val = @as(i32, 10) },
        .{ .user_id = @as(i64, 1), .ts = @as(i64, 200), .val = @as(i32, 20) },
        .{ .user_id = @as(i64, 2), .ts = @as(i64, 100), .val = @as(i32, 30) },
    });
    try t.flush();

    // Overwrite (user=1, ts=100); (user=1, ts=200) is unchanged.
    try t.insert(&.{
        .{ .user_id = @as(i64, 1), .ts = @as(i64, 100), .val = @as(i32, 999) },
    });

    var q = try thindb_scan(allocator, t);
    defer q.deinit();

    var users: std.ArrayList(i64) = .empty;
    defer users.deinit(allocator);
    var tss: std.ArrayList(i64) = .empty;
    defer tss.deinit(allocator);
    var vals: std.ArrayList(i32) = .empty;
    defer vals.deinit(allocator);
    while (try q.next()) |b| {
        try users.appendSlice(allocator, b.values[0].data.bigint);
        try tss.appendSlice(allocator, b.values[1].data.bigint);
        try vals.appendSlice(allocator, b.values[2].data.int);
    }
    // Segment after upsert: (1,200,20), (2,100,30). Memtable: (1,100,999).
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 1 }, users.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 200, 100, 100 }, tss.items);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 20, 30, 999 }, vals.items);
}

test "upsert works with single STRING order key" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "code", .type = .string },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"code"},
        .unique = true,
    };
    var db = try Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"code"}, .unique = true });

    try t.insert(&.{
        .{ .code = "alpha", .qty = @as(i32, 1) },
        .{ .code = "beta", .qty = @as(i32, 2) },
        .{ .code = "gamma", .qty = @as(i32, 3) },
    });
    try t.flush();

    try t.insert(&.{
        .{ .code = "beta", .qty = @as(i32, 999) },
    });

    var q = try thindb_scan(allocator, t);
    defer q.deinit();
    var qtys: std.ArrayList(i32) = .empty;
    defer qtys.deinit(allocator);
    var codes: std.ArrayList(u8) = .empty;
    defer codes.deinit(allocator);
    while (try q.next()) |b| {
        try qtys.appendSlice(allocator, b.values[1].data.int);
        for (0..b.row_count) |i| {
            try codes.append(allocator, '|');
            try codes.appendSlice(allocator, b.values[0].data.string.rowBytes(i));
        }
    }
    // Segment after upsert: alpha=1, gamma=3 (sorted lex). Memtable: beta=999.
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 3, 999 }, qtys.items);
    try std.testing.expectEqualStrings("|alpha|gamma|beta", codes.items);
}

test "upsert works with mixed (string, bigint) compound key" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "tenant", .type = .string },
            .{ .name = "event_id", .type = .bigint },
            .{ .name = "payload", .type = .int },
        },
        .order_key = &.{ "tenant", "event_id" },
        .unique = true,
    };
    var db = try Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{ "tenant", "event_id" }, .unique = true });

    try t.insert(&.{
        .{ .tenant = "acme", .event_id = @as(i64, 1), .payload = @as(i32, 10) },
        .{ .tenant = "acme", .event_id = @as(i64, 2), .payload = @as(i32, 20) },
        .{ .tenant = "globex", .event_id = @as(i64, 1), .payload = @as(i32, 30) },
    });
    try t.flush();

    try t.insert(&.{
        .{ .tenant = "acme", .event_id = @as(i64, 1), .payload = @as(i32, 111) },
    });

    var q = try thindb_scan(allocator, t);
    defer q.deinit();
    var payloads: std.ArrayList(i32) = .empty;
    defer payloads.deinit(allocator);
    while (try q.next()) |b| {
        try payloads.appendSlice(allocator, b.values[2].data.int);
    }
    // Segment kept (acme,2,20) + (globex,1,30); upserted (acme,1) is now 111 in memtable.
    try std.testing.expectEqualSlices(i32, &[_]i32{ 20, 30, 111 }, payloads.items);
}

test "compact merges segments and absorbs tombstones" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });

    // 3 flushes → 3 segments.
    try t.insert(&.{
        .{ .id = @as(i64, 1), .tag = "a" },
        .{ .id = @as(i64, 2), .tag = "b" },
    });
    try t.flush();

    try t.insert(&.{
        .{ .id = @as(i64, 3), .tag = "c" },
        .{ .id = @as(i64, 4), .tag = "d" },
    });
    try t.flush();

    try t.insert(&.{
        .{ .id = @as(i64, 5), .tag = "e" },
        .{ .id = @as(i64, 6), .tag = "f" },
    });
    try t.flush();

    try std.testing.expectEqual(@as(usize, 3), t.segmentCount());

    // Delete a row across the segments, then compact.
    _ = try t.delete(.{ .col = "tag", .op = .eq, .val = .{ .text = "c" } });
    try t.compact();

    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    var q = try thindb_scan(allocator, t);
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        try ids.appendSlice(allocator, b.values[0].data.bigint);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 4, 5, 6 }, ids.items);
}

/// Decode the table's single (post-compaction) segment, concatenating the
/// named column's values across every row-group in physical (= order-key) order.
/// Caller frees the returned slice.
fn collectMergedBigint(allocator: std.mem.Allocator, io: anytype, t: *Table, schema: TableSchema, col_idx: usize) ![]i64 {
    const seg_id = t.manifest.segments.items[0].segment_id;
    var name_buf: [32]u8 = undefined;
    const file_name = try Table.segmentFileName(&name_buf, seg_id);
    var seg = try storage.readSegment(allocator, io, t.segments_dir, file_name, schema);
    defer seg.deinit();

    var out: std.ArrayList(i64) = .empty;
    errdefer out.deinit(allocator);
    for (seg.info.row_groups, 0..) |_, rg_idx| {
        var c = try seg.decodeColumn(allocator, schema, rg_idx, col_idx);
        defer c.deinit(allocator);
        try out.appendSlice(allocator, c.data.bigint);
    }
    return out.toOwnedSlice(allocator);
}

test "streaming merge: sorted union across many small segments (multi-row-group output)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    // row_group_size = 4 forces several output row-groups, exercising the flush
    // boundary in the streaming writer.
    var db = try Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });

    // Five segments whose key ranges interleave (each individually sorted, as
    // flush guarantees). The merge must interleave them into one sorted run.
    const segs = [_][]const i64{
        &.{ 3, 9, 21 },
        &.{ 1, 8, 30 },
        &.{ 5, 6, 7, 22 },
        &.{ 2, 4, 25 },
        &.{ 10, 11, 12, 13, 14 },
    };
    for (segs) |s| {
        for (s) |id| try t.insert(&.{.{ .id = id, .v = @as(i32, @intCast(id * 10)) }});
        try t.flush();
    }
    try std.testing.expectEqual(@as(usize, 5), t.segmentCount());

    try t.compact();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    // Brute-force expected: sorted union of all ids.
    var want: std.ArrayList(i64) = .empty;
    defer want.deinit(allocator);
    for (segs) |s| try want.appendSlice(allocator, s);
    std.mem.sort(i64, want.items, {}, std.sort.asc(i64));

    const ids = try collectMergedBigint(allocator, io, t, schema, 0);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, want.items, ids);

    // The paired `v` column must travel with its row (v == id*10), proving the
    // non-key columns are emitted in lockstep with the key.
    const seg_id = t.manifest.segments.items[0].segment_id;
    var name_buf: [32]u8 = undefined;
    const file_name = try Table.segmentFileName(&name_buf, seg_id);
    var seg = try storage.readSegment(allocator, io, t.segments_dir, file_name, schema);
    defer seg.deinit();
    var ri: usize = 0;
    for (seg.info.row_groups, 0..) |_, rg_idx| {
        var vcol = try seg.decodeColumn(allocator, schema, rg_idx, 1);
        defer vcol.deinit(allocator);
        for (vcol.data.int) |v| {
            try std.testing.expectEqual(@as(i32, @intCast(ids[ri] * 10)), v);
            ri += 1;
        }
    }
}

test "streaming merge: tombstoned rows are dropped" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try Database.open(allocator, io, tmp.dir, .{ .row_group_size = 3 });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });

    // Three segments; we'll delete rows spread across all of them.
    for ([_][]const i64{ &.{ 1, 2, 3, 4 }, &.{ 5, 6, 7, 8 }, &.{ 9, 10, 11, 12 } }) |s| {
        for (s) |id| try t.insert(&.{.{ .id = id }});
        try t.flush();
    }
    // Delete first-of-group, last-of-group, and an interior row across segments.
    for ([_]i64{ 1, 4, 6, 8, 9, 12 }) |id| {
        _ = try t.delete(.{ .col = "id", .op = .eq, .val = .{ .bigint = id } });
    }

    try t.compact();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    const ids = try collectMergedBigint(allocator, io, t, schema, 0);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3, 5, 7, 10, 11 }, ids);
}

test "streaming merge: dict-eligible string column round-trips" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "color", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try Database.open(allocator, io, tmp.dir, .{ .row_group_size = 64 });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });

    const palette = [_][]const u8{ "red", "green", "blue", "magenta" };
    // Two segments, each ~150 rows of a 4-value palette → low global NDV → the
    // merged block must be dict-encoded, and every value must round-trip.
    var next_id: i64 = 0;
    var expected_color = std.AutoHashMap(i64, []const u8).init(allocator);
    defer expected_color.deinit();
    for (0..2) |_| {
        var k: usize = 0;
        while (k < 150) : (k += 1) {
            const color = palette[@as(usize, @intCast(next_id)) % palette.len];
            try t.insert(&.{.{ .id = next_id, .color = color }});
            try expected_color.put(next_id, color);
            next_id += 1;
        }
        try t.flush();
    }
    try std.testing.expectEqual(@as(usize, 2), t.segmentCount());

    try t.compact();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    const seg_id = t.manifest.segments.items[0].segment_id;
    var name_buf: [32]u8 = undefined;
    const file_name = try Table.segmentFileName(&name_buf, seg_id);
    var seg = try storage.readSegment(allocator, io, t.segments_dir, file_name, schema);
    defer seg.deinit();

    // The color column's first block is dict-encoded (low global NDV).
    var cache = storage.cache.Cache.init(allocator, 1 << 20);
    defer cache.deinit();
    var block = try seg.borrowColumnBlock(allocator, 0, 1, &cache);
    const enc = block.encoding;
    block.release(allocator, &cache);
    try std.testing.expectEqual(storage.format.Encoding.dict, enc);

    // Every row's (id, color) survives the merge intact, in id order.
    var prev_id: i64 = -1;
    for (seg.info.row_groups, 0..) |_, rg_idx| {
        var idc = try seg.decodeColumn(allocator, schema, rg_idx, 0);
        defer idc.deinit(allocator);
        var cc = try seg.decodeColumn(allocator, schema, rg_idx, 1);
        defer cc.deinit(allocator);
        const sv = cc.view().data.string;
        for (idc.data.bigint, 0..) |id, i| {
            try std.testing.expect(id > prev_id); // strictly ascending key
            prev_id = id;
            try std.testing.expectEqualStrings(expected_color.get(id).?, sv.rowBytes(i));
        }
    }
}

test "streaming merge: NULL ordering matches the single-flush sort path" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "k", .type = .bigint, .nullable = true },
            .{ .name = "id", .type = .bigint },
        },
        .order_key = &.{"k"},
        .unique = false,
    };

    // Same rows two ways: (1) inserted across several segments then merged,
    // (2) inserted once and flushed (the buildSortedSnapshot path). The merged
    // segment's row order — including where NULLs (stored as a 0 placeholder in
    // the key column, so they sort with the zeros) land — must match the
    // single-flush order exactly.
    const Row = struct { k: ?i64, id: i64 };
    const rows = [_]Row{
        .{ .k = 5, .id = 1 },   .{ .k = null, .id = 2 }, .{ .k = -3, .id = 3 },
        .{ .k = 0, .id = 4 },   .{ .k = 5, .id = 5 },    .{ .k = null, .id = 6 },
        .{ .k = 100, .id = 7 }, .{ .k = -3, .id = 8 },   .{ .k = 2, .id = 9 },
    };

    // (2) reference: one flush.
    var ref_ids: []i64 = undefined;
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try Database.open(allocator, io, tmp.dir, .{ .row_group_size = 64 });
        defer db.close();
        const t = try db.table("t", schema, .{ .order_key = &.{"k"} });
        for (rows) |r| try t.insert(&.{.{ .k = r.k, .id = r.id }});
        try t.flush();
        ref_ids = try collectMergedBigint(allocator, io, t, schema, 1);
    }
    defer allocator.free(ref_ids);

    // (1) merged: one row per segment, then compact.
    var tmp2 = std.testing.tmpDir(.{});
    defer tmp2.cleanup();
    var db2 = try Database.open(allocator, io, tmp2.dir, .{ .row_group_size = 4 });
    defer db2.close();
    const t2 = try db2.table("t", schema, .{ .order_key = &.{"k"} });
    for (rows) |r| {
        try t2.insert(&.{.{ .k = r.k, .id = r.id }});
        try t2.flush();
    }
    try t2.compact();
    try std.testing.expectEqual(@as(usize, 1), t2.segmentCount());

    const merged_ids = try collectMergedBigint(allocator, io, t2, schema, 1);
    defer allocator.free(merged_ids);

    // The merged row order (carried by the unique `id`) must equal the
    // single-flush sort's order, NULL placement included.
    try std.testing.expectEqualSlices(i64, ref_ids, merged_ids);
}

test "auto-flush fires when row count threshold is hit" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    // Tiny threshold so 3 inserts of 1 row each force 3 flushes.
    var db = try Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = 1,
        .auto_flush_secs = 0, // disable time trigger
    });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });

    try t.insert(&.{.{ .id = @as(i64, 1), .qty = @as(i32, 10) }});
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());
    try std.testing.expectEqual(@as(u64, 0), t.memtable.row_count);

    try t.insert(&.{.{ .id = @as(i64, 2), .qty = @as(i32, 20) }});
    try std.testing.expectEqual(@as(usize, 2), t.segmentCount());

    try t.insert(&.{.{ .id = @as(i64, 3), .qty = @as(i32, 30) }});
    try std.testing.expectEqual(@as(usize, 3), t.segmentCount());
}

test "auto-flush does NOT fire below thresholds" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = 1_000_000,
        .auto_flush_bytes = 1024 * 1024 * 1024,
        .auto_flush_secs = 0, // disable time trigger
    });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });

    try t.insert(&.{
        .{ .id = @as(i64, 1) },
        .{ .id = @as(i64, 2) },
        .{ .id = @as(i64, 3) },
    });
    try std.testing.expectEqual(@as(usize, 0), t.segmentCount());
    try std.testing.expectEqual(@as(u64, 3), t.memtable.row_count);
}

test "compact on empty table is a no-op" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });

    try t.compact();
    try std.testing.expectEqual(@as(usize, 0), t.segmentCount());
}

test "upsert: re-inserting the same key tombstones the old row" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "val", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .unique = true });

    try t.insert(&.{
        .{ .id = @as(i64, 1), .val = @as(i32, 10) },
        .{ .id = @as(i64, 2), .val = @as(i32, 20) },
        .{ .id = @as(i64, 3), .val = @as(i32, 30) },
    });
    try t.flush();

    // Overwrite id=1 and id=3 with new values.
    try t.insert(&.{
        .{ .id = @as(i64, 1), .val = @as(i32, 100) },
        .{ .id = @as(i64, 3), .val = @as(i32, 300) },
    });

    var q = try thindb_scan(allocator, t);
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    var vals: std.ArrayList(i32) = .empty;
    defer vals.deinit(allocator);
    while (try q.next()) |b| {
        try ids.appendSlice(allocator, b.values[0].data.bigint);
        try vals.appendSlice(allocator, b.values[1].data.int);
    }
    // Expected: id=2 from segment (untouched), id=1 and id=3 from memtable.
    // Order: scan returns segment (id=2 only — id=1 and id=3 are tombstoned)
    // followed by memtable (id=1, id=3).
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 1, 3 }, ids.items);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 20, 100, 300 }, vals.items);
}

test "upsert: duplicate keys within a single insert keep the last one" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "val", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .unique = true });

    try t.insert(&.{
        .{ .id = @as(i64, 1), .val = @as(i32, 10) },
        .{ .id = @as(i64, 1), .val = @as(i32, 20) },
        .{ .id = @as(i64, 1), .val = @as(i32, 30) }, // wins
        .{ .id = @as(i64, 2), .val = @as(i32, 50) },
    });

    try std.testing.expectEqual(@as(u64, 2), t.memtable.row_count);
    var q = try thindb_scan(allocator, t);
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2 }, b.values[0].data.bigint);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 30, 50 }, b.values[1].data.int);
}

test "delete removes matching rows from memtable" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });

    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20) },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30) },
        .{ .id = @as(i64, 4), .qty = @as(i32, 40) },
    });
    // No flush — everything in memtable.

    const deleted = try t.delete(.{
        .col = "qty",
        .op = .gte,
        .val = .{ .int = 25 },
    });
    try std.testing.expectEqual(@as(usize, 2), deleted);
    try std.testing.expectEqual(@as(u64, 2), t.memtable.row_count);

    var q = try thindb_scan(allocator, t);
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2 }, b.values[0].data.bigint);
}

test "openTable on missing table returns TableNotFound" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const result = db.openTable("missing", .{});
    try std.testing.expectError(Error.TableNotFound, result);
}
