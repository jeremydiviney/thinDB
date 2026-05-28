//! Storage subsystem aggregate. Public storage names re-exported through
//! `thindb.storage.*`.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const TableSchema = types.TableSchema;

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
pub const MergedSegmentWriter = segment_writer.MergedSegmentWriter;
pub const dictEligibleFromSketches = segment_writer.dictEligibleFromSketches;
pub const readSegment = segment_reader.readSegment;

/// Write `data` to `dir/sub_path`. When `sync_after_write` is true, also
/// `fsync` the resulting file before closing — guarantees the bytes are on
/// physical media (not just OS page cache) by the time we return. Used for
/// segments, tombstones, and the manifest's write-tmp step.
pub fn writeFileSynced(
    io: Io,
    dir: Io.Dir,
    sub_path: []const u8,
    data: []const u8,
    sync_after_write: bool,
) !void {
    var file = try dir.createFile(io, sub_path, .{});
    defer file.close(io);
    try file.writeStreamingAll(io, data);
    if (sync_after_write) try file.sync(io);
}

// ---------------------------------------------------------------------------
// Round-trip test — write some columns out, read them back, verify.
// ---------------------------------------------------------------------------

test "round-trip a single row group with all v0.1 types" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
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
        &.{},
        false, // sync not needed for round-trip test
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

    const schema = TableSchema{
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

    var info = try writeSegment(allocator, io, tmp.dir, "multi.dat", schema, 1, 0, 3, &columns, &.{}, false);
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

test "borrowed view matches owned decode byte-for-byte (fixed + string)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
            .{ .name = "ratio", .type = .double },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = true,
    };

    const ids = [_]i64{ 100, 101, 102, 103, 104 };
    const qtys = [_]i32{ 1, 2, 3, 4, 5 };
    const ratios = [_]f64{ 1.5, 2.5, 3.5, 4.5, 5.5 };
    const tag_bytes = "AAA" ++ "BB" ++ "C" ++ "DDDD" ++ "EE";
    const tag_offsets = [_]u32{ 0, 3, 5, 6, 10, 12 };

    const columns = [_]ColumnView{
        .{ .data = .{ .bigint = &ids } },
        .{ .data = .{ .int = &qtys } },
        .{ .data = .{ .double = &ratios } },
        .{ .data = .{ .string = .{ .offsets = &tag_offsets, .bytes = tag_bytes } } },
    };

    var info = try writeSegment(allocator, io, tmp.dir, "seg.dat", schema, 7, 0, 16, &columns, &.{}, false);
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "seg.dat", schema);
    defer seg.deinit();

    var c = cache.Cache.init(allocator, 1 << 20);
    defer c.deinit();

    const rg_count = seg.info.row_groups[0].row_count;
    inline for (.{ 0, 1, 2, 3 }) |col_idx| {
        var owned = try seg.decodeColumn(allocator, schema, 0, col_idx);
        defer owned.deinit(allocator);

        var block = try seg.borrowColumnBlock(allocator, 0, col_idx, &c);
        defer block.release(allocator, &c);

        const col_type = schema.columns[col_idx].type;
        const flags = format.ColumnBlockFlags{ .has_nulls = schema.columns[col_idx].nullable };
        const maybe_view = segment_reader.viewRawColumn(col_type, block.bytes, rg_count, flags, block.encoding);

        // A FOR-encoded block (the narrow-range int/bigint columns here) has no
        // in-place native view, so the borrow declines — the owned decode above
        // is the correctness check for those. Raw blocks must match byte-for-byte.
        if (block.encoding != .raw) {
            try std.testing.expect(maybe_view == null);
        } else {
            const view = maybe_view.?;
            const ov = owned.view();
            try std.testing.expectEqual(ov.data.rowCount(), view.data.rowCount());
            switch (ov.data) {
                .bigint => |s| try std.testing.expectEqualSlices(i64, s, view.data.bigint),
                .int => |s| try std.testing.expectEqualSlices(i32, s, view.data.int),
                .double => |s| try std.testing.expectEqualSlices(f64, s, view.data.double),
                .string => |sv| {
                    for (0..sv.rowCount()) |r| {
                        try std.testing.expectEqualStrings(sv.rowBytes(r), view.data.string.rowBytes(r));
                    }
                },
                else => unreachable,
            }
        }
    }
}

test "FOR encoding round-trips narrow-range int/bigint incl. negatives + nulls" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "k", .type = .int }, // narrow positive range → FOR
            .{ .name = "n", .type = .bigint, .nullable = true }, // negatives + NULLs → FOR
        },
        .order_key = &.{"k"},
        .unique = false,
    };

    const n_rows = 50;
    var ks: [n_rows]i32 = undefined;
    var ns: [n_rows]i64 = undefined;
    var bm: [column.bitmapBytes(n_rows)]u8 = .{0} ** column.bitmapBytes(n_rows);
    for (0..n_rows) |i| {
        ks[i] = 1000 + @as(i32, @intCast(i % 7)); // [1000, 1006] → u8 deltas
        ns[i] = -500 + @as(i64, @intCast(i)); // negative base, [-500, -451]
        // Every 5th row of `n` is NULL.
        if (i % 5 != 0) column.setValidBit(&bm, i, true);
    }

    const columns = [_]ColumnView{
        .{ .data = .{ .int = &ks } },
        .{ .data = .{ .bigint = &ns }, .nulls = &bm },
    };

    var info = try writeSegment(allocator, io, tmp.dir, "for.dat", schema, 9, 0, 64, &columns, &.{}, false);
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "for.dat", schema);
    defer seg.deinit();

    var c0 = try seg.decodeColumn(allocator, schema, 0, 0);
    defer c0.deinit(allocator);
    try std.testing.expectEqualSlices(i32, &ks, c0.data.int);

    var c1 = try seg.decodeColumn(allocator, schema, 0, 1);
    defer c1.deinit(allocator);
    const v1 = c1.view();
    for (0..n_rows) |i| {
        const want_valid = (i % 5 != 0);
        try std.testing.expectEqual(want_valid, v1.isValid(i));
        if (want_valid) try std.testing.expectEqual(ns[i], c1.data.bigint[i]);
    }
}

test "forBlockOf parses base/width/codes (2B narrow accessor)" {
    // A FOR block over [10, 13]: base = 10, width = 1, deltas {0,3,1}.
    const row_count: u32 = 3;
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(std.testing.allocator);
    var b16: [16]u8 = undefined;
    format.writeI128(&b16, 10);
    try payload.appendSlice(std.testing.allocator, &b16);
    try payload.appendSlice(std.testing.allocator, &[_]u8{ 1, 0, 0, 0 }); // width=1 + 3 pad
    try payload.appendSlice(std.testing.allocator, &[_]u8{ 0, 3, 1 });

    const fb = segment_reader.forBlockOf(payload.items, row_count);
    try std.testing.expectEqual(@as(i128, 10), fb.base);
    try std.testing.expectEqual(@as(u8, 1), fb.width);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 3, 1 }, fb.codes);
}

// ---------------------------------------------------------------------------
// dict (segment-local string dictionary) encoding
// ---------------------------------------------------------------------------

const DictCols = struct {
    offsets: []u32,
    bytes: []u8,
    fn deinit(self: *DictCols, a: Allocator) void {
        a.free(self.offsets);
        a.free(self.bytes);
    }
    fn view(self: DictCols, nulls: ?[]const u8) ColumnView {
        return .{ .data = .{ .string = .{ .offsets = self.offsets, .bytes = self.bytes } }, .nulls = nulls };
    }
};

fn buildStringCol(a: Allocator, vals: []const []const u8) !DictCols {
    const offsets = try a.alloc(u32, vals.len + 1);
    errdefer a.free(offsets);
    var total: usize = 0;
    offsets[0] = 0;
    for (vals, 0..) |v, i| {
        total += v.len;
        offsets[i + 1] = @intCast(total);
    }
    const bytes = try a.alloc(u8, total);
    errdefer a.free(bytes);
    for (vals, 0..) |v, i| @memcpy(bytes[offsets[i]..offsets[i + 1]], v);
    return .{ .offsets = offsets, .bytes = bytes };
}

/// A column of `n` distinct decimal strings "0".."n-1" — high-cardinality by
/// construction, used to exercise the stays-raw + mid-build-abandon gates
/// without a huge stack array of slices.
fn buildSeqStringCol(a: Allocator, n: usize) !DictCols {
    var bytes: std.ArrayList(u8) = .empty;
    errdefer bytes.deinit(a);
    const offsets = try a.alloc(u32, n + 1);
    errdefer a.free(offsets);
    offsets[0] = 0;
    var tmp: [24]u8 = undefined;
    for (0..n) |i| {
        const s = std.fmt.bufPrint(&tmp, "{d}", .{i}) catch unreachable;
        try bytes.appendSlice(a, s);
        offsets[i + 1] = @intCast(bytes.items.len);
    }
    return .{ .offsets = offsets, .bytes = try bytes.toOwnedSlice(a) };
}

fn blockEncodingOf(allocator: Allocator, seg: *ReadSegment, rg_idx: usize, col_idx: usize) !format.Encoding {
    var c = cache.Cache.init(allocator, 1 << 20);
    defer c.deinit();
    var block = try seg.borrowColumnBlock(allocator, rg_idx, col_idx, &c);
    defer block.release(allocator, &c);
    return block.encoding;
}

test "dict encoding: low-card string → dict, high-card → raw, both round-trip" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "color", .type = .string }, // 4 distinct over 300 rows → dict
            .{ .name = "uniq", .type = .string }, // 300 distinct → NDV > n/2 → raw
        },
        .order_key = &.{"color"},
        .unique = false,
    };

    const n = 300;
    const palette = [_][]const u8{ "red", "green", "blue", "magenta" };
    var color_vals: [n][]const u8 = undefined;
    for (0..n) |i| color_vals[i] = palette[i % palette.len];

    var color = try buildStringCol(allocator, &color_vals);
    defer color.deinit(allocator);
    var uniq = try buildSeqStringCol(allocator, n);
    defer uniq.deinit(allocator);

    const columns = [_]ColumnView{ color.view(null), uniq.view(null) };

    var info = try writeSegment(allocator, io, tmp.dir, "dict.dat", schema, 11, 0, n, &columns, &.{}, false);
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "dict.dat", schema);
    defer seg.deinit();

    try std.testing.expectEqual(format.Encoding.dict, try blockEncodingOf(allocator, &seg, 0, 0));
    try std.testing.expectEqual(format.Encoding.raw, try blockEncodingOf(allocator, &seg, 0, 1));

    var c0 = try seg.decodeColumn(allocator, schema, 0, 0);
    defer c0.deinit(allocator);
    var c1 = try seg.decodeColumn(allocator, schema, 0, 1);
    defer c1.deinit(allocator);
    const v0 = c0.view();
    const v1 = c1.view();
    var line: [24]u8 = undefined;
    for (0..n) |i| {
        try std.testing.expectEqualStrings(color_vals[i], v0.data.string.rowBytes(i));
        const want = std.fmt.bufPrint(&line, "{d}", .{i}) catch unreachable;
        try std.testing.expectEqualStrings(want, v1.data.string.rowBytes(i));
    }
}

test "dict encoding: NULL and empty string are distinct under codes" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{.{ .name = "s", .type = .string, .nullable = true }},
        .order_key = &.{"s"},
        .unique = false,
    };

    // Pattern of length 6: a NULL, an empty string, then low-card values. The
    // empty string is a real distinct dict value; the NULL is masked.
    const n = 120;
    var vals: [n][]const u8 = undefined;
    var bm: [column.bitmapBytes(n)]u8 = .{0} ** column.bitmapBytes(n);
    for (0..n) |i| {
        switch (i % 6) {
            0 => vals[i] = "", // NULL row (bytes irrelevant; validity bit left clear)
            1 => {
                vals[i] = ""; // empty string, NOT null
                column.setValidBit(&bm, i, true);
            },
            2, 3 => {
                vals[i] = "alpha";
                column.setValidBit(&bm, i, true);
            },
            else => {
                vals[i] = "beta";
                column.setValidBit(&bm, i, true);
            },
        }
    }

    var col = try buildStringCol(allocator, &vals);
    defer col.deinit(allocator);
    const columns = [_]ColumnView{col.view(&bm)};

    var info = try writeSegment(allocator, io, tmp.dir, "dnull.dat", schema, 12, 0, n, &columns, &.{}, false);
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "dnull.dat", schema);
    defer seg.deinit();

    try std.testing.expectEqual(format.Encoding.dict, try blockEncodingOf(allocator, &seg, 0, 0));

    var c0 = try seg.decodeColumn(allocator, schema, 0, 0);
    defer c0.deinit(allocator);
    const v = c0.view();
    for (0..n) |i| {
        const want_null = (i % 6 == 0);
        try std.testing.expectEqual(!want_null, v.isValid(i));
        if (!want_null) try std.testing.expectEqualStrings(vals[i], v.data.string.rowBytes(i));
    }
}

test "dict encoding: blob-like (avg len > 256) stays raw" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{.{ .name = "blob", .type = .string }},
        .order_key = &.{"blob"},
        .unique = false,
    };

    // Only 3 distinct values (low card) but each ~300 bytes → avg-len gate trips.
    const big_a = "A" ** 300;
    const big_b = "B" ** 300;
    const big_c = "C" ** 300;
    const n = 60;
    var vals: [n][]const u8 = undefined;
    for (0..n) |i| vals[i] = switch (i % 3) {
        0 => big_a,
        1 => big_b,
        else => big_c,
    };

    var col = try buildStringCol(allocator, &vals);
    defer col.deinit(allocator);
    const columns = [_]ColumnView{col.view(null)};

    var info = try writeSegment(allocator, io, tmp.dir, "blob.dat", schema, 13, 0, n, &columns, &.{}, false);
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "blob.dat", schema);
    defer seg.deinit();

    try std.testing.expectEqual(format.Encoding.raw, try blockEncodingOf(allocator, &seg, 0, 0));

    var c0 = try seg.decodeColumn(allocator, schema, 0, 0);
    defer c0.deinit(allocator);
    const v = c0.view();
    for (0..n) |i| try std.testing.expectEqualStrings(vals[i], v.data.string.rowBytes(i));
}

test "dict encoding: NDV beyond the cap abandons mid-build → raw, round-trips" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{.{ .name = "s", .type = .string }},
        .order_key = &.{"s"},
        .unique = false,
    };

    // 70_000 distinct values in a single row group: the distinct map crosses the
    // 65_536 cap mid-build, so the encoder abandons and the block stays raw.
    const n = 70_000;
    var col = try buildSeqStringCol(allocator, n);
    defer col.deinit(allocator);
    const columns = [_]ColumnView{col.view(null)};

    var info = try writeSegment(allocator, io, tmp.dir, "abandon.dat", schema, 14, 0, n, &columns, &.{}, false);
    defer info.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), info.row_groups.len);

    var seg = try readSegment(allocator, io, tmp.dir, "abandon.dat", schema);
    defer seg.deinit();

    try std.testing.expectEqual(format.Encoding.raw, try blockEncodingOf(allocator, &seg, 0, 0));

    var c0 = try seg.decodeColumn(allocator, schema, 0, 0);
    defer c0.deinit(allocator);
    const v = c0.view();
    var line: [24]u8 = undefined;
    for (0..n) |i| {
        const want = std.fmt.bufPrint(&line, "{d}", .{i}) catch unreachable;
        try std.testing.expectEqualStrings(want, v.data.string.rowBytes(i));
    }
}

test "dict block stores a lexicographically sorted dictionary" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{.{ .name = "s", .type = .string }},
        .order_key = &.{"s"},
        .unique = false,
    };

    // Distinct values first SEEN in non-sorted order; the on-disk dict must be
    // sorted, and per-row codes must still resolve to the original value.
    const seen = [_][]const u8{ "delta", "alpha", "charlie", "bravo" };
    const n = 40;
    var vals: [n][]const u8 = undefined;
    for (0..n) |i| vals[i] = seen[i % seen.len];

    var col = try buildStringCol(allocator, &vals);
    defer col.deinit(allocator);
    const columns = [_]ColumnView{col.view(null)};

    var info = try writeSegment(allocator, io, tmp.dir, "sorted.dat", schema, 15, 0, n, &columns, &.{}, false);
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "sorted.dat", schema);
    defer seg.deinit();

    var c = cache.Cache.init(allocator, 1 << 20);
    defer c.deinit();
    var block = try seg.borrowColumnBlock(allocator, 0, 0, &c);
    defer block.release(allocator, &c);
    try std.testing.expectEqual(format.Encoding.dict, block.encoding);

    // Non-nullable column → no validity bitmap → the payload is the dict body.
    const db = segment_reader.dictBlockOf(block.bytes, n);
    try std.testing.expectEqual(@as(u32, 4), db.ndv);

    // Dictionary entries are in non-decreasing lexicographic order.
    var k: u32 = 1;
    while (k < db.ndv) : (k += 1) {
        try std.testing.expect(!std.mem.lessThan(u8, db.dictValue(k), db.dictValue(k - 1)));
    }
    // Each row's code resolves back to the original value.
    for (0..n) |i| {
        try std.testing.expectEqualStrings(vals[i], db.dictValue(db.rowCode(i)));
    }
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
