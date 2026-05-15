//! Storage subsystem aggregate. Public storage names re-exported through
//! `thindb.storage.*`.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Schema = types.Schema;

pub const format = @import("format.zig");
pub const column = @import("column.zig");
pub const segment_writer = @import("segment_writer.zig");
pub const segment_reader = @import("segment_reader.zig");
pub const manifest = @import("manifest.zig");
pub const schema_file = @import("schema_file.zig");
pub const tombstone = @import("tombstone.zig");
pub const compression = @import("compression.zig");
pub const cache = @import("cache.zig");

pub const ColumnView = column.ColumnView;
pub const StringView = column.StringView;
pub const OwnedColumn = column.OwnedColumn;
pub const OwnedStringColumn = column.OwnedStringColumn;

pub const SegmentInfo = format.SegmentInfo;
pub const RowGroupMeta = format.RowGroupMeta;
pub const ReadSegment = segment_reader.ReadSegment;

pub const Manifest = manifest.Manifest;
pub const ManifestEntry = manifest.ManifestEntry;
pub const writeManifest = manifest.writeManifest;
pub const readManifest = manifest.readManifest;

pub const writeSegment = segment_writer.writeSegment;
pub const readSegment = segment_reader.readSegment;

// ---------------------------------------------------------------------------
// Round-trip test — write some columns out, read them back, verify.
// ---------------------------------------------------------------------------

test "round-trip a single row group with all v0.1 types" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
            .{ .name = "active", .type = .boolean },
            .{ .name = "tag", .type = .{ .varchar = 16 } },
            .{ .name = "note", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    try schema.validate();

    // Build column data for 5 rows.
    const ids = [_]i64{ 100, 101, 102, 103, 104 };
    const qtys = [_]i32{ 1, 2, 3, 4, 5 };
    const actives = [_]u8{ 1, 0, 1, 1, 0 };
    const tag_bytes = "AAA" ++ "BB" ++ "C" ++ "DDDD" ++ "EE";
    const tag_offsets = [_]u32{ 0, 3, 5, 6, 10, 12 };
    const note_bytes = "first" ++ "second" ++ "" ++ "fourth" ++ "fifth";
    const note_offsets = [_]u32{ 0, 5, 11, 11, 17, 22 };

    const columns = [_]ColumnView{
        .{ .data = .{ .bigint = &ids } },
        .{ .data = .{ .int = &qtys } },
        .{ .data = .{ .boolean = &actives } },
        .{ .data = .{ .varchar = .{ .offsets = &tag_offsets, .bytes = tag_bytes } } },
        .{ .data = .{ .string = .{ .offsets = &note_offsets, .bytes = note_bytes } } },
    };

    var info = try writeSegment(
        allocator,
        io,
        tmp.dir,
        "seg.dat",
        schema,
        42, // segment_id
        0xCAFEBABE_DEADBEEF, // fingerprint
        16, // row_group_size — small to force one row group of 5
        &columns,
    );
    defer info.deinit(allocator);

    try std.testing.expectEqual(@as(u64, 42), info.segment_id);
    try std.testing.expectEqual(@as(u64, 5), info.row_count);
    try std.testing.expectEqual(@as(usize, 1), info.row_groups.len);

    var seg = try readSegment(allocator, io, tmp.dir, "seg.dat", schema);
    defer seg.deinit();

    try std.testing.expectEqual(@as(u64, 42), seg.info.segment_id);
    try std.testing.expectEqual(@as(u64, 5), seg.info.row_count);
    try std.testing.expectEqual(@as(u64, 0xCAFEBABE_DEADBEEF), seg.info.schema_fingerprint);
    try std.testing.expectEqual(@as(usize, 1), seg.info.row_groups.len);

    var col0 = try seg.decodeColumn(allocator, schema, 0, 0);
    defer col0.deinit(allocator);
    try std.testing.expectEqualSlices(i64, &ids, col0.data.bigint);

    var col1 = try seg.decodeColumn(allocator, schema, 0, 1);
    defer col1.deinit(allocator);
    try std.testing.expectEqualSlices(i32, &qtys, col1.data.int);

    var col2 = try seg.decodeColumn(allocator, schema, 0, 2);
    defer col2.deinit(allocator);
    try std.testing.expectEqualSlices(u8, &actives, col2.data.boolean);

    var col3 = try seg.decodeColumn(allocator, schema, 0, 3);
    defer col3.deinit(allocator);
    const tag_view = col3.view().data.varchar;
    try std.testing.expectEqualStrings("AAA", tag_view.rowBytes(0));
    try std.testing.expectEqualStrings("BB", tag_view.rowBytes(1));
    try std.testing.expectEqualStrings("C", tag_view.rowBytes(2));
    try std.testing.expectEqualStrings("DDDD", tag_view.rowBytes(3));
    try std.testing.expectEqualStrings("EE", tag_view.rowBytes(4));

    var col4 = try seg.decodeColumn(allocator, schema, 0, 4);
    defer col4.deinit(allocator);
    const note_view = col4.view().data.string;
    try std.testing.expectEqualStrings("first", note_view.rowBytes(0));
    try std.testing.expectEqualStrings("second", note_view.rowBytes(1));
    try std.testing.expectEqualStrings("", note_view.rowBytes(2));
    try std.testing.expectEqualStrings("fourth", note_view.rowBytes(3));
    try std.testing.expectEqualStrings("fifth", note_view.rowBytes(4));
}

test "round-trip with multiple row groups" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };

    // 7 rows, row_group_size = 3 → 3 row groups (3, 3, 1)
    const ids = [_]i64{ 1, 2, 3, 4, 5, 6, 7 };
    const qtys = [_]i32{ 10, 20, 30, 40, 50, 60, 70 };

    const columns = [_]ColumnView{
        .{ .data = .{ .bigint = &ids } },
        .{ .data = .{ .int = &qtys } },
    };

    var info = try writeSegment(allocator, io, tmp.dir, "multi.dat", schema, 1, 0, 3, &columns);
    defer info.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), info.row_groups.len);
    try std.testing.expectEqual(@as(u32, 3), info.row_groups[0].row_count);
    try std.testing.expectEqual(@as(u32, 3), info.row_groups[1].row_count);
    try std.testing.expectEqual(@as(u32, 1), info.row_groups[2].row_count);

    var seg = try readSegment(allocator, io, tmp.dir, "multi.dat", schema);
    defer seg.deinit();

    // Concatenate all decoded id values across the three row groups; should equal ids[].
    var collected: std.ArrayList(i64) = .empty;
    defer collected.deinit(allocator);
    for (seg.info.row_groups, 0..) |_, rg_idx| {
        var col = try seg.decodeColumn(allocator, schema, rg_idx, 0);
        defer col.deinit(allocator);
        try collected.appendSlice(allocator, col.data.bigint);
    }
    try std.testing.expectEqualSlices(i64, &ids, collected.items);
}

test {
    _ = column;
    _ = format;
    _ = segment_writer;
    _ = segment_reader;
    _ = manifest;
    _ = schema_file;
    _ = tombstone;
    _ = compression;
    _ = cache;
}
