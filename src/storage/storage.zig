//! Storage subsystem aggregate. Public storage names re-exported through
//! `thindb.storage.*`.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const memory = @import("../memory.zig");
const TableSchema = types.TableSchema;

pub const format = @import("format.zig");
pub const column = @import("column.zig");
pub const segment_writer = @import("segment_writer.zig");
pub const segment_reader = @import("segment_reader.zig");
pub const manifest = @import("manifest.zig");
pub const schema_file = @import("schema_file.zig");
pub const tombstone = @import("tombstone.zig");
pub const compression = @import("compression.zig");
pub const snappy = @import("snappy.zig");
pub const fsst = @import("fsst.zig");
pub const segment_strings = @import("segment_strings.zig");
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

/// POSIX file sync does not persist the directory entry selecting that file.
/// Windows does not support fsync on a directory handle.
pub fn syncDirectory(io: Io, dir: Io.Dir) !void {
    if (@import("builtin").os.tag == .windows) return;
    const readable = try dir.openDir(io, ".", .{ .iterate = true });
    defer readable.close(io);
    const file: Io.File = .{ .handle = readable.handle, .flags = .{ .nonblocking = false } };
    try file.sync(io);
}

pub fn writeFileAtomic(
    io: Io,
    dir: Io.Dir,
    temporary_path: []const u8,
    path: []const u8,
    data: []const u8,
    durable: bool,
) !void {
    try writeFileSynced(io, dir, temporary_path, data, durable);
    errdefer dir.deleteFile(io, temporary_path) catch {};
    // Retry only before publication; a persistent denial must preserve the
    // old file and reach the caller.
    try retryTransientWindowsRefusal(io, Io.Dir.rename, .{ dir, temporary_path, dir, path, io });
    if (durable) syncDirectory(io, dir) catch return error.DurabilityUncertain;
}

/// Delete directory `name` in `parent` so that a crash never leaves it half
/// deleted under its name: rename it to `aside_name` in `aside_dir`, sync
/// both directories when `durable`, then delete it there. A crash between
/// the steps leaves the whole directory aside, for its owner to sweep at
/// open. Once the rename stands, a failed delete costs only disk until that
/// sweep, so it isn't reported.
pub fn deleteTreeAside(io: Io, parent: Io.Dir, name: []const u8, aside_dir: Io.Dir, aside_name: []const u8, durable: bool) !void {
    try retryTransientWindowsRefusal(io, Io.Dir.rename, .{ parent, name, aside_dir, aside_name, io });
    if (durable) {
        syncDirectory(io, parent) catch return error.DurabilityUncertain;
        syncDirectory(io, aside_dir) catch return error.DurabilityUncertain;
    }
    retryTransientWindowsRefusal(io, Io.Dir.deleteTree, .{ aside_dir, io, aside_name }) catch {};
}

/// `prefix` followed by 16 random hex digits: a name no other directory set
/// aside under `prefix` takes.
pub fn randomAsideName(io: Io, buf: []u8, prefix: []const u8) ![]u8 {
    var bytes: [8]u8 = undefined;
    io.random(&bytes);
    return std.fmt.bufPrint(buf, "{s}{x:0>16}", .{ prefix, std.mem.readInt(u64, &bytes, .little) });
}

/// Windows can temporarily refuse to rename or delete a directory entry while
/// another handle or a filesystem filter still holds it. Runs `op(args...)`,
/// retrying AccessDenied with exponential backoff (1..32 ms) on Windows only.
/// Any other error, or a refusal that outlives the backoff, reaches the caller
/// unchanged.
pub fn retryTransientWindowsRefusal(io: Io, comptime op: anytype, args: anytype) !void {
    var retry_delay_ms: i64 = 1;
    while (true) {
        @call(.auto, op, args) catch |err| {
            if (@import("builtin").os.tag != .windows or err != error.AccessDenied or retry_delay_ms > 32) return err;
            try Io.sleep(io, .fromMilliseconds(retry_delay_ms), .awake);
            retry_delay_ms *= 2;
            continue;
        };
        return;
    }
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
        false, // sync not needed for round-trip test
        1, // encode_threads
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

test "footer stats carry sum / null_count / blank-excluded string min" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "v", .type = .bigint, .nullable = true },
            .{ .name = "f", .type = .double },
            .{ .name = "s", .type = .string },
        },
        .order_key = &.{"f"},
        .unique = false,
    };
    try schema.validate();

    // 6 rows, row_group_size 4 → two row groups (4 + 2).
    const vs = [_]i64{ 1, 2, 3, 4, 5, 6 };
    // Rows 1 and 4 are NULL (bit clear).
    const v_nulls = [_]u8{0b101101};
    const fs = [_]f64{ 0.5, 1.5, 2.5, 3.5, 4.5, 5.5 };
    const s_bytes = "" ++ "beta" ++ "alpha" ++ "" ++ "zz" ++ "";
    const s_offsets = [_]u32{ 0, 0, 4, 9, 9, 11, 11 };

    const columns = [_]ColumnView{
        .{ .data = .{ .bigint = &vs }, .nulls = &v_nulls },
        .{ .data = .{ .double = &fs } },
        .{ .data = .{ .string = .{ .offsets = &s_offsets, .bytes = s_bytes } } },
    };

    var info = try writeSegment(allocator, io, tmp.dir, "seg.dat", schema, 7, 0xFEED, 4, &columns, false, 1);
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "seg.dat", schema);
    defer seg.deinit();
    try std.testing.expectEqual(@as(usize, 2), seg.info.row_groups.len);

    // Row group 0 (rows 0-3): v null at row 1 → sum 1+3+4, one null.
    const rg0 = seg.info.row_groups[0];
    try std.testing.expectEqual(@as(i128, 8), rg0.stats[0].sum);
    try std.testing.expectEqual(@as(u64, 1), rg0.stats[0].null_count);
    // f: 0.5+1.5+2.5+3.5 = 8.0, bit-stored in the low 64 bits.
    try std.testing.expectEqual(@as(f64, 8.0), format.f64FromSumSlot(rg0.stats[1].sum));
    try std.testing.expectEqual(@as(u64, 0), rg0.stats[1].null_count);
    // s: plain min is '' but the sum slot excludes blanks → "alpha".
    try std.testing.expectEqual(format.encodeStringPrefix(""), rg0.stats[2].min);
    try std.testing.expectEqual(format.encodeStringPrefix("alpha"), rg0.stats[2].sum);

    // Row group 1 (rows 4-5): v null at row 4 → sum 6; s nonblank min "zz".
    const rg1 = seg.info.row_groups[1];
    try std.testing.expectEqual(@as(i128, 6), rg1.stats[0].sum);
    try std.testing.expectEqual(@as(u64, 1), rg1.stats[0].null_count);
    try std.testing.expectEqual(format.encodeStringPrefix("zz"), rg1.stats[2].sum);

    // Segment-level fold into a manifest entry.
    const entry = try manifest.entryFromSegmentInfo(allocator, seg.info, null, schema.columns);
    defer allocator.free(entry.column_stats);
    try std.testing.expectEqual(@as(i128, 14), entry.column_stats[0].sum);
    try std.testing.expectEqual(@as(u64, 2), entry.column_stats[0].null_count);
    try std.testing.expectEqual(@as(f64, 18.0), format.f64FromSumSlot(entry.column_stats[1].sum));
    try std.testing.expectEqual(format.encodeStringPrefix("alpha"), entry.column_stats[2].sum);
    try std.testing.expectEqual(@as(u64, 0), entry.column_stats[2].null_count);
}

test "lz4 string blocks: large raw block is cached decompressed and borrowed in place" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "url", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    try schema.validate();

    // 2000 distinct ~70B URLs ≈ 140KB raw — NDV == n so dict declines (and
    // FSST is off), leaving a raw LZ4-compressed string block.
    const n: usize = 2000;
    const ids = try allocator.alloc(i64, n);
    defer allocator.free(ids);
    const offsets = try allocator.alloc(u32, n + 1);
    defer allocator.free(offsets);
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(allocator);
    offsets[0] = 0;
    var buf: [128]u8 = undefined;
    for (0..n) |i| {
        ids[i] = @intCast(i);
        const s = try std.fmt.bufPrint(&buf, "https://example.com/products/category-{d}/item?id={d}&ref=search", .{ i % 13, i });
        try bytes.appendSlice(allocator, s);
        offsets[i + 1] = @intCast(bytes.items.len);
    }

    const columns = [_]ColumnView{
        .{ .data = .{ .bigint = ids } },
        .{ .data = .{ .string = .{ .offsets = offsets, .bytes = bytes.items } } },
    };
    var info = try segment_writer.writeSegmentWithStringCap(allocator, io, tmp.dir, "seg.dat", schema, 3, 0x124, 4096, &columns, false, 1, 0);
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "seg.dat", schema);
    defer seg.deinit();

    var controller = memory.MemoryPool.init(1 << 24);

    var c = cache.Cache.init(allocator, &controller, 0);

    controller.shareCache(&c, controller.budget);

    defer c.deinit();

    // Borrow through the cache: the block is decompressed once at fill and
    // every borrow views the pinned cache bytes in place — no owned copy, and
    // a warm re-borrow hands back the very same bytes.
    var bb = try seg.borrowColumnBlock(allocator, 0, 1, .{ .cache = &c, .table_uid = 0 });
    try std.testing.expectEqual(format.Encoding.raw, bb.encoding);
    try std.testing.expect(bb.owned == null);
    try std.testing.expect(bb.entry != null);
    const first_ptr = bb.bytes.ptr;
    bb.release(allocator, .{ .cache = &c, .table_uid = 0 });

    var again = try seg.borrowColumnBlock(allocator, 0, 1, .{ .cache = &c, .table_uid = 0 });
    try std.testing.expectEqual(first_ptr, again.bytes.ptr);
    try std.testing.expect(again.bytes.len > bytes.items.len);
    again.release(allocator, .{ .cache = &c, .table_uid = 0 });
    try std.testing.expectEqual(@as(u64, 1), c.misses);

    var col = try seg.decodeColumnMaybeCached(allocator, schema, 0, 1, .{ .cache = &c, .table_uid = 0 });
    defer col.deinit(allocator);
    const sv = col.view().data.string;
    for (0..n) |i| {
        try std.testing.expectEqualStrings(bytes.items[offsets[i]..offsets[i + 1]], sv.rowBytes(i));
    }

    // The plain-int sibling block stays on the decompressed-at-fill path.
    var bb2 = try seg.borrowColumnBlock(allocator, 0, 0, .{ .cache = &c, .table_uid = 0 });
    try std.testing.expect(bb2.entry != null);
    bb2.release(allocator, .{ .cache = &c, .table_uid = 0 });
}

test "MergedSegmentWriter: parallel encode output is byte-identical to serial" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "url", .type = .string },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    try schema.validate();

    // Three row groups of 1200 rows: ascending ids (FOR), ~70B distinct URLs
    // (≈84KB raw per block → the LZ4 string path), pseudo-random ints (raw +
    // zstd). Same data written with 1 and with 8 encoder threads must produce
    // identical files — the parallel path only reorders WORK, never bytes.
    const rg_rows: usize = 1200;
    const n_rgs: usize = 3;

    const file_names = [_][]const u8{ "serial.dat", "parallel.dat" };
    const thread_counts = [_]usize{ 1, 8 };
    for (file_names, thread_counts) |file_name, n_threads| {
        var w = try MergedSegmentWriter.begin(allocator, io, tmp.dir, file_name, schema, 7, 0x99, rg_rows, &.{}, n_threads);
        errdefer w.deinit();

        var prng = std.Random.DefaultPrng.init(42);
        const rand = prng.random();
        var sbuf: [128]u8 = undefined;
        for (0..n_rgs) |rg| {
            const ids = try allocator.alloc(i64, rg_rows);
            defer allocator.free(ids);
            const vals = try allocator.alloc(i32, rg_rows);
            defer allocator.free(vals);
            const offsets = try allocator.alloc(u32, rg_rows + 1);
            defer allocator.free(offsets);
            var bytes: std.ArrayList(u8) = .empty;
            defer bytes.deinit(allocator);
            offsets[0] = 0;
            for (0..rg_rows) |i| {
                const row = rg * rg_rows + i;
                ids[i] = @intCast(row);
                vals[i] = rand.int(i32);
                const s = try std.fmt.bufPrint(&sbuf, "https://example.com/products/category-{d}/item?id={d}&ref=search", .{ row % 13, row });
                try bytes.appendSlice(allocator, s);
                offsets[i + 1] = @intCast(bytes.items.len);
            }
            const columns = [_]ColumnView{
                .{ .data = .{ .bigint = ids } },
                .{ .data = .{ .string = .{ .offsets = offsets, .bytes = bytes.items } } },
                .{ .data = .{ .int = vals } },
            };
            try w.writeRowGroup(&columns);
        }

        var info = try w.finish(false);
        info.deinit(allocator);
    }

    const serial_bytes = try tmp.dir.readFileAlloc(io, "serial.dat", allocator, .unlimited);
    defer allocator.free(serial_bytes);
    const parallel_bytes = try tmp.dir.readFileAlloc(io, "parallel.dat", allocator, .unlimited);
    defer allocator.free(parallel_bytes);
    try std.testing.expectEqualSlices(u8, serial_bytes, parallel_bytes);

    // And the parallel-written file round-trips through the reader.
    var seg = try readSegment(allocator, io, tmp.dir, "parallel.dat", schema);
    defer seg.deinit();
    try std.testing.expectEqual(@as(u64, rg_rows * n_rgs), seg.info.row_count);
    var col = try seg.decodeColumn(allocator, schema, 2, 1);
    defer col.deinit(allocator);
    const sv = col.view().data.string;
    var expect_buf: [128]u8 = undefined;
    const row: usize = 2 * rg_rows + 5;
    const expected = try std.fmt.bufPrint(&expect_buf, "https://example.com/products/category-{d}/item?id={d}&ref=search", .{ row % 13, row });
    try std.testing.expectEqualStrings(expected, sv.rowBytes(5));
}

test "fsst encoding: high-NDV string block round-trips, dict-sized stays dict" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "url", .type = .string, .nullable = true },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    try schema.validate();

    // 600 distinct URL-ish strings (NDV == n → dict declines via k*2 > n),
    // sharing long substrings so FSST clears its acceptance ratio. Rows
    // divisible by 7 are NULL; row 100 is the empty string.
    const n: usize = 600;
    var ids = try allocator.alloc(i64, n);
    defer allocator.free(ids);
    var offsets = try allocator.alloc(u32, n + 1);
    defer allocator.free(offsets);
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(allocator);
    const nulls = try allocator.alloc(u8, column.bitmapBytes(n));
    defer allocator.free(nulls);
    @memset(nulls, 0);

    offsets[0] = 0;
    var buf: [128]u8 = undefined;
    for (0..n) |i| {
        ids[i] = @intCast(i);
        const valid = i % 7 != 0;
        column.setValidBit(nulls, i, valid);
        if (valid and i != 100) {
            const s = try std.fmt.bufPrint(&buf, "https://example.com/products/category-{d}/item?id={d}&ref=search", .{ i % 13, i });
            try bytes.appendSlice(allocator, s);
        }
        offsets[i + 1] = @intCast(bytes.items.len);
    }

    const columns = [_]ColumnView{
        .{ .data = .{ .bigint = ids } },
        .{ .data = .{ .string = .{ .offsets = offsets, .bytes = bytes.items } }, .nulls = nulls },
    };

    var info = try segment_writer.writeSegmentWithStringCap(allocator, io, tmp.dir, "seg.dat", schema, 9, 0xF557, 1024, &columns, false, 1, 0);
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "seg.dat", schema);
    defer seg.deinit();

    // FSST is currently disabled (`fsst_enabled = false` in the writer —
    // LZ4-cached raw blocks won that bake-off), so the high-NDV block falls
    // through to raw. The kernel + block parser stay covered by fsst.zig's
    // own tests; flip the writer gate to re-enable end-to-end.
    var bb = try seg.borrowColumnBlock(allocator, 0, 1, null);
    defer bb.release(allocator, null);
    try std.testing.expectEqual(format.Encoding.raw, bb.encoding);

    var col = try seg.decodeColumn(allocator, schema, 0, 1);
    defer col.deinit(allocator);
    const sv = col.view().data.string;
    for (0..n) |i| {
        try std.testing.expectEqualStrings(
            bytes.items[offsets[i]..offsets[i + 1]],
            sv.rowBytes(i),
        );
        try std.testing.expectEqual(column.isValidBit(nulls, i), col.view().isValid(i));
    }
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

    var info = try writeSegment(allocator, io, tmp.dir, "multi.dat", schema, 1, 0, 3, &columns, false, 1);
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

    var info = try writeSegment(allocator, io, tmp.dir, "seg.dat", schema, 7, 0, 16, &columns, false, 1);
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "seg.dat", schema);
    defer seg.deinit();

    var controller = memory.MemoryPool.init(1 << 20);

    var c = cache.Cache.init(allocator, &controller, 0);

    controller.shareCache(&c, controller.budget);

    defer c.deinit();

    const rg_count = seg.info.row_groups[0].row_count;
    inline for (.{ 0, 1, 2, 3 }) |col_idx| {
        var owned = try seg.decodeColumn(allocator, schema, 0, col_idx);
        defer owned.deinit(allocator);

        var block = try seg.borrowColumnBlock(allocator, 0, col_idx, .{ .cache = &c, .table_uid = 0 });
        defer block.release(allocator, .{ .cache = &c, .table_uid = 0 });

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
    var bm: [column.bitmapBytes(n_rows)]u8 = @splat(0);
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

    var info = try writeSegment(allocator, io, tmp.dir, "for.dat", schema, 9, 0, 64, &columns, false, 1);
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

test "integer-stream transforms: a wide datetime block stores rewritten and reads back exactly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "ts", .type = .datetime },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"ts"},
        .unique = false,
    };

    const n_rows = 4096;
    var ts: [n_rows]i64 = undefined;
    var tag_offsets: [n_rows + 1]u32 = undefined;
    var tag_bytes: std.ArrayList(u8) = .empty;
    defer tag_bytes.deinit(allocator);
    tag_offsets[0] = 0;
    for (0..n_rows) |i| {
        // A span past u32 keeps the block raw (FOR declines), and a common
        // step leaves the rewrite plenty to save.
        ts[i] = 1_700_000_000_000_000 + @as(i64, @intCast(i)) * 2_000_003 + @as(i64, @intCast(i % 3));
        try tag_bytes.print(allocator, "tag-{d}", .{i % 37});
        tag_offsets[i + 1] = @intCast(tag_bytes.items.len);
    }
    const columns = [_]ColumnView{
        .{ .data = .{ .datetime = &ts } },
        .{ .data = .{ .string = .{ .offsets = &tag_offsets, .bytes = tag_bytes.items } } },
    };

    var info = try writeSegment(allocator, io, tmp.dir, "ints.dat", schema, 3, 0, n_rows, &columns, false, 1);
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "ints.dat", schema);
    defer seg.deinit();

    const file = try tmp.dir.readFileAlloc(io, "ints.dat", allocator, .unlimited);
    defer allocator.free(file);
    const ts_block = seg.info.row_groups[0].col_offsets[0];
    try std.testing.expect(format.ColumnBlockFlags.fromByte(file[ts_block + 1]).int_streams);

    var owned_ts = try seg.decodeColumn(allocator, schema, 0, 0);
    defer owned_ts.deinit(allocator);
    try std.testing.expectEqualSlices(i64, &ts, owned_ts.data.datetime);

    var owned_tag = try seg.decodeColumn(allocator, schema, 0, 1);
    defer owned_tag.deinit(allocator);
    for (0..n_rows) |i| {
        try std.testing.expectEqualStrings(tag_bytes.items[tag_offsets[i]..tag_offsets[i + 1]], owned_tag.view().data.string.rowBytes(i));
    }

    var block = try seg.borrowColumnBlock(allocator, 0, 0, null);
    defer block.release(allocator, null);
    try std.testing.expectEqual(format.Encoding.raw, block.encoding);
    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(&ts), block.bytes);
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

test "forInSetInto matches each code against the set at every width and alignment" {
    const n = 300;
    var prng = std.Random.DefaultPrng.init(0xf0f);
    const rand = prng.random();
    // Sets whose top code ends a bitmap byte (code 7, 65535), sets past the
    // bitmap (binary search) and a set no code reaches.
    const sets = [_][]const u64{ &.{7}, &.{ 0, 15, 200 }, &.{ 3, 65535 }, &.{ 1, 70000, 1 << 31 }, &.{ 100_000, 4_000_000_000 } };
    var storage_bytes: [n * 4 + 1]u8 align(4) = undefined;
    var mask: [n]bool = undefined;
    inline for (.{ u8, u16, u32 }) |U| {
        for ([_]usize{ 0, 1 }) |misalign| {
            const codes = storage_bytes[misalign..][0 .. n * @sizeOf(U)];
            var vals: [n]u64 = undefined;
            for (&vals, 0..) |*v, i| {
                // Every set member appears, plus neighbours of each.
                const member = sets[i % sets.len][(i / sets.len) % sets[i % sets.len].len];
                const pick: u64 = switch (i % 3) {
                    0 => member,
                    1 => member +% 1,
                    else => rand.int(u64),
                };
                v.* = @as(U, @truncate(pick));
                std.mem.writeInt(U, codes[i * @sizeOf(U) ..][0..@sizeOf(U)], @intCast(v.*), .little);
            }
            const fb = segment_reader.ForBlock{ .base = 0, .width = @sizeOf(U), .codes = codes };
            for (sets) |set| {
                segment_reader.forInSetInto(fb, set, n, &mask);
                for (vals, mask) |v, got| {
                    const want = std.mem.indexOfScalar(u64, set, v) != null;
                    try std.testing.expectEqual(want, got);
                }
            }
        }
    }
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
    var controller = memory.MemoryPool.init(1 << 20);
    var c = cache.Cache.init(allocator, &controller, 0);
    controller.shareCache(&c, controller.budget);
    defer c.deinit();
    var block = try seg.borrowColumnBlock(allocator, rg_idx, col_idx, .{ .cache = &c, .table_uid = 0 });
    defer block.release(allocator, .{ .cache = &c, .table_uid = 0 });
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

    var info = try segment_writer.writeSegmentWithStringCap(allocator, io, tmp.dir, "dict.dat", schema, 11, 0, n, &columns, false, 1, 0);
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
    var bm: [column.bitmapBytes(n)]u8 = @splat(0);
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

    var info = try writeSegment(allocator, io, tmp.dir, "dnull.dat", schema, 12, 0, n, &columns, false, 1);
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "dnull.dat", schema);
    defer seg.deinit();

    try std.testing.expectEqual(format.Encoding.dict, try blockEncodingOf(allocator, &seg, 0, 0));
    {
        // A nullable column's blocks store no per-value rows: nothing reads
        // them counted.
        var controller = memory.MemoryPool.init(1 << 20);
        var c = cache.Cache.init(allocator, &controller, 0);
        controller.shareCache(&c, controller.budget);
        defer c.deinit();
        var block = try seg.borrowColumnBlock(allocator, 0, 0, .{ .cache = &c, .table_uid = 0 });
        defer block.release(allocator, .{ .cache = &c, .table_uid = 0 });
        try std.testing.expect(!segment_reader.dictBlockOf(block.bytes[column.bitmapBytes(n)..], n).hasEntryStats());
    }

    var c0 = try seg.decodeColumn(allocator, schema, 0, 0);
    defer c0.deinit(allocator);
    const v = c0.view();
    for (0..n) |i| {
        const want_null = (i % 6 == 0);
        try std.testing.expectEqual(!want_null, v.isValid(i));
        if (!want_null) try std.testing.expectEqualStrings(vals[i], v.data.string.rowBytes(i));
    }
}

test "dict encoding: blob-like (avg len > 256) declines dict; stays raw with fsst off" {
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
    const big_a = &@as([300]u8, @splat('A'));
    const big_b = &@as([300]u8, @splat('B'));
    const big_c = &@as([300]u8, @splat('C'));
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

    var info = try segment_writer.writeSegmentWithStringCap(allocator, io, tmp.dir, "blob.dat", schema, 13, 0, n, &columns, false, 1, 0);
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "blob.dat", schema);
    defer seg.deinit();

    // The dict avg-len gate still declines; with FSST disabled the block
    // stays raw (LZ4-compressed at rest above the size threshold).
    try std.testing.expectEqual(format.Encoding.raw, try blockEncodingOf(allocator, &seg, 0, 0));

    var c0 = try seg.decodeColumn(allocator, schema, 0, 0);
    defer c0.deinit(allocator);
    const v = c0.view();
    for (0..n) |i| try std.testing.expectEqualStrings(vals[i], v.data.string.rowBytes(i));
}

test "dict encoding: NDV beyond the cap abandons mid-build → raw fallback, round-trips" {
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

    var info = try segment_writer.writeSegmentWithStringCap(allocator, io, tmp.dir, "abandon.dat", schema, 14, 0, n, &columns, false, 1, 0);
    defer info.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), info.row_groups.len);

    var seg = try readSegment(allocator, io, tmp.dir, "abandon.dat", schema);
    defer seg.deinit();

    // Dict abandons past the cap; with FSST disabled the fall-through is raw
    // (LZ4-compressed at rest, invisible at this API).
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

    var info = try writeSegment(allocator, io, tmp.dir, "sorted.dat", schema, 15, 0, n, &columns, false, 1);
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "sorted.dat", schema);
    defer seg.deinit();

    var controller = memory.MemoryPool.init(1 << 20);

    var c = cache.Cache.init(allocator, &controller, 0);

    controller.shareCache(&c, controller.budget);

    defer c.deinit();
    var block = try seg.borrowColumnBlock(allocator, 0, 0, .{ .cache = &c, .table_uid = 0 });
    defer block.release(allocator, .{ .cache = &c, .table_uid = 0 });
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
    // The non-nullable block stores each value's row count and first row.
    try std.testing.expect(db.hasEntryStats());
    var stored: [4]u32 = undefined;
    var first: [4]u32 = undefined;
    db.entryStatsInto(&stored, &first);
    var counted: [4 * 4]u32 = @splat(0);
    var any_row: [4]u32 = undefined;
    db.countEntries(n, counted[0 .. db.entryCountLanes(n) * 4], &any_row);
    try std.testing.expectEqualSlices(u32, counted[0..4], &stored);
    for (first, 0..) |row, code| {
        try std.testing.expectEqual(@as(u32, @intCast(code)), db.rowCode(row));
        for (0..row) |earlier| try std.testing.expect(db.rowCode(earlier) != code);
    }
    // A cached block's offsets view in place, so the scan's dictionary
    // emit never copies them.
    if (comptime @import("builtin").cpu.arch.endian() == .little) {
        const offsets = db.offsetsInPlace().?;
        for (0..db.ndv) |k2| try std.testing.expectEqualStrings(db.dictValue(@intCast(k2)), db.dict_bytes[offsets[k2]..offsets[k2 + 1]]);
    }
}

/// Rows of the segment-string-table tests: three row groups of four. pear,
/// fig and kiwi recur across row groups (shared); date, apple and the empty
/// string occur in one (local); rows 2 and 9 are NULL.
const sst_vals = [_][]const u8{ "pear", "fig", "", "kiwi", "fig", "date", "apple", "kiwi", "pear", "", "", "fig" };
const sst_nulls = [_]u8{ 0b1111_1011, 0b0000_1101 };
const sst_schema = TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .int },
        .{ .name = "s", .type = .string, .nullable = true },
    },
    .order_key = &.{"id"},
    .unique = false,
};

/// Reads every row group of column 1 back, checks each row against
/// `sst_vals`, and checks that row groups the table took give each distinct
/// value one segment id below the table's `value_count`. Returns how many
/// row groups carried segment ids.
fn expectSegmentStrings(allocator: Allocator, seg: *ReadSegment) !usize {
    var controller = memory.MemoryPool.init(1 << 20);
    var c = cache.Cache.init(allocator, &controller, 0);
    controller.shareCache(&c, controller.budget);
    defer c.deinit();
    const tc: cache.TableCache = .{ .cache = &c, .table_uid = 0 };

    var id_of: std.StringHashMapUnmanaged(u32) = .empty;
    defer id_of.deinit(allocator);
    var value_of: std.AutoHashMapUnmanaged(u32, []const u8) = .empty;
    defer value_of.deinit(allocator);
    var with_ids: usize = 0;
    var row: usize = 0;
    for (seg.info.row_groups, 0..) |rg, rg_idx| {
        var col = try seg.decodeColumn(allocator, sst_schema, rg_idx, 1);
        defer col.deinit(allocator);
        var cached = try seg.decodeColumnMaybeCached(allocator, sst_schema, rg_idx, 1, tc);
        defer cached.deinit(allocator);
        var block = try seg.borrowColumnBlock(allocator, rg_idx, 1, tc);
        defer block.release(allocator, tc);
        const nulls = block.bytes[0..column.bitmapBytes(rg.row_count)];
        const db: ?segment_reader.DictBlock = if (block.encoding == .dict)
            segment_reader.dictBlockOf(block.bytes[nulls.len..], rg.row_count)
        else
            null;
        if (db != null and db.?.hasSegmentIds()) with_ids += 1;
        for (0..rg.row_count) |r| {
            const valid = column.isValidBit(&sst_nulls, row);
            try std.testing.expectEqual(valid, col.view().isValid(r));
            try std.testing.expectEqual(valid, column.isValidBit(nulls, r));
            if (valid) {
                try std.testing.expectEqualStrings(sst_vals[row], col.view().data.string.rowBytes(r));
                try std.testing.expectEqualStrings(sst_vals[row], cached.view().data.string.rowBytes(r));
                if (db) |d| if (d.hasSegmentIds()) {
                    const id = d.segmentId(d.rowCode(r));
                    try std.testing.expect(id < seg.info.stringTable(1).?.value_count);
                    const by_value = try id_of.getOrPut(allocator, sst_vals[row]);
                    if (by_value.found_existing) try std.testing.expectEqual(by_value.value_ptr.*, id) else by_value.value_ptr.* = id;
                    const by_id = try value_of.getOrPut(allocator, id);
                    if (by_id.found_existing) try std.testing.expectEqualStrings(by_id.value_ptr.*, sst_vals[row]) else by_id.value_ptr.* = sst_vals[row];
                };
            }
            row += 1;
        }
    }
    return with_ids;
}

test "segment string tables: one id per distinct value across row groups; blocks round-trip" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var ids: [sst_vals.len]i32 = undefined;
    for (&ids, 0..) |*v, i| v.* = @intCast(i);
    var s = try buildStringCol(allocator, &sst_vals);
    defer s.deinit(allocator);
    const columns = [_]ColumnView{ .{ .data = .{ .int = &ids } }, s.view(&sst_nulls) };

    var info = try writeSegment(allocator, io, tmp.dir, "sst.dat", sst_schema, 1, 0, 4, &columns, false, 2);
    defer info.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), info.string_tables.len);

    var seg = try readSegment(allocator, io, tmp.dir, "sst.dat", sst_schema);
    defer seg.deinit();
    const table = seg.info.stringTable(1).?;
    // pear, fig, kiwi shared; date, apple, "" local.
    try std.testing.expectEqual(@as(u32, 3), table.shared_count);
    try std.testing.expectEqual(@as(u32, 6), table.value_count);
    try std.testing.expectEqual(@as(usize, 3), try expectSegmentStrings(allocator, &seg));
}

test "segment string tables: past the cap a column's later row groups keep their own encodings" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var ids: [sst_vals.len]i32 = undefined;
    for (&ids, 0..) |*v, i| v.* = @intCast(i);
    var s = try buildStringCol(allocator, &sst_vals);
    defer s.deinit(allocator);
    const columns = [_]ColumnView{ .{ .data = .{ .int = &ids } }, s.view(&sst_nulls) };

    // Row group 0 holds three values; row group 1's date and apple pass four.
    var info = try segment_writer.writeSegmentWithStringCap(allocator, io, tmp.dir, "cap.dat", sst_schema, 1, 0, 4, &columns, false, 1, 4);
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "cap.dat", sst_schema);
    defer seg.deinit();
    const table = seg.info.stringTable(1).?;
    try std.testing.expectEqual(@as(u32, 0), table.shared_count);
    try std.testing.expectEqual(@as(u32, 3), table.value_count);
    try std.testing.expectEqual(@as(usize, 1), try expectSegmentStrings(allocator, &seg));
}

test "segment string tables: MergedSegmentWriter writes them too" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var w = try MergedSegmentWriter.begin(allocator, io, tmp.dir, "merged.dat", sst_schema, 2, 0, 4, &.{}, 2);
    var finished = false;
    defer if (!finished) w.deinit();
    for (0..3) |g| {
        var ids: [4]i32 = undefined;
        for (&ids, 0..) |*v, i| v.* = @intCast(g * 4 + i);
        var s = try buildStringCol(allocator, sst_vals[g * 4 ..][0..4]);
        defer s.deinit(allocator);
        var nulls: [1]u8 = .{0};
        for (0..4) |i| column.setValidBit(&nulls, i, column.isValidBit(&sst_nulls, g * 4 + i));
        try w.writeRowGroup(&.{ .{ .data = .{ .int = &ids } }, s.view(&nulls) });
    }
    var info = try w.finish(false);
    finished = true;
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "merged.dat", sst_schema);
    defer seg.deinit();
    try std.testing.expectEqual(@as(u32, 3), seg.info.stringTable(1).?.shared_count);
    try std.testing.expectEqual(@as(usize, 3), try expectSegmentStrings(allocator, &seg));
}

test "MergedSegmentWriter: the step API writes the bytes writeRowGroup writes" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var compressor = try compression.Compressor.init();
    defer compressor.deinit();
    const file_names = [_][]const u8{ "rows.dat", "steps.dat" };
    for (file_names, [_]bool{ false, true }) |file_name, steps| {
        var w = try MergedSegmentWriter.begin(allocator, io, tmp.dir, file_name, sst_schema, 2, 0, 4, &.{}, 2);
        var finished = false;
        defer if (!finished) w.deinit();
        for (0..3) |g| {
            var ids: [4]i32 = undefined;
            for (&ids, 0..) |*v, i| v.* = @intCast(g * 4 + i);
            var s = try buildStringCol(allocator, sst_vals[g * 4 ..][0..4]);
            defer s.deinit(allocator);
            var nulls: [1]u8 = .{0};
            for (0..4) |i| column.setValidBit(&nulls, i, column.isValidBit(&sst_nulls, g * 4 + i));
            const views = [_]ColumnView{ .{ .data = .{ .int = &ids } }, s.view(&nulls) };
            if (!steps) {
                try w.writeRowGroup(&views);
                continue;
            }
            var blocks: [views.len]segment_writer.ColumnBlock = @splat(.{});
            defer for (&blocks) |*b| b.deinit(allocator);
            for (views, &blocks, 0..) |view, *block, ci| {
                try w.encodeColumn(&compressor, ci, view, block);
                try w.commitColumn(&compressor, ci, @intCast(g), view, block);
            }
            try w.appendRowGroup(&blocks, ids.len);
        }
        if (steps) for (0..sst_schema.columns.len) |ci| {
            var build = (try w.beginStringTable(ci)) orelse continue;
            for (0..build.pieces.len) |i| w.encodeStringTablePiece(&compressor, &build, i) catch |e| {
                build.deinit(allocator);
                return e;
            };
            try w.appendStringTable(&build);
        };
        var info = try w.finish(false);
        finished = true;
        info.deinit(allocator);
    }

    const rows_bytes = try tmp.dir.readFileAlloc(io, "rows.dat", allocator, .unlimited);
    defer allocator.free(rows_bytes);
    const steps_bytes = try tmp.dir.readFileAlloc(io, "steps.dat", allocator, .unlimited);
    defer allocator.free(steps_bytes);
    try std.testing.expectEqualSlices(u8, rows_bytes, steps_bytes);
}

test "MergedSegmentWriter: a segment written column by column reads back its rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var compressor = try compression.Compressor.init();
    defer compressor.deinit();
    var w = try MergedSegmentWriter.begin(allocator, io, tmp.dir, "columns.dat", sst_schema, 2, 0, 4, &.{}, 1);
    var finished = false;
    defer if (!finished) w.deinit();
    var blocks: [3][2]segment_writer.ColumnBlock = @splat(@splat(.{}));
    defer for (&blocks) |*row_group| for (row_group) |*b| b.deinit(allocator);
    for (&blocks, 0..) |*row_group, g| {
        var ids: [4]i32 = undefined;
        for (&ids, 0..) |*v, i| v.* = @intCast(g * 4 + i);
        var s = try buildStringCol(allocator, sst_vals[g * 4 ..][0..4]);
        defer s.deinit(allocator);
        var nulls: [1]u8 = .{0};
        for (0..4) |i| column.setValidBit(&nulls, i, column.isValidBit(&sst_nulls, g * 4 + i));
        const views = [_]ColumnView{ .{ .data = .{ .int = &ids } }, s.view(&nulls) };
        for (views, row_group, 0..) |view, *block, ci| {
            try w.encodeColumn(&compressor, ci, view, block);
            try w.commitColumn(&compressor, ci, @intCast(g), view, block);
        }
    }
    try w.beginRowGroups(sst_vals.len);
    for (0..sst_schema.columns.len) |ci| {
        for (&blocks, 0..) |*row_group, g| try w.appendColumnBlock(g, ci, &row_group[ci]);
        var build = (try w.beginStringTable(ci)) orelse continue;
        for (0..build.pieces.len) |i| w.encodeStringTablePiece(&compressor, &build, i) catch |e| {
            build.deinit(allocator);
            return e;
        };
        try w.appendStringTable(&build);
    }
    var info = try w.finish(false);
    finished = true;
    defer info.deinit(allocator);

    var seg = try readSegment(allocator, io, tmp.dir, "columns.dat", sst_schema);
    defer seg.deinit();
    try std.testing.expectEqual(@as(u64, sst_vals.len), seg.info.row_count);
    for (seg.info.row_groups, 0..) |rg, g| {
        try std.testing.expectEqual(@as(u32, 4), rg.row_count);
        var ids = try seg.decodeColumn(allocator, sst_schema, g, 0);
        defer ids.deinit(allocator);
        for (ids.view().data.int, 0..) |id, i| try std.testing.expectEqual(@as(i32, @intCast(g * 4 + i)), id);
    }
    try std.testing.expectEqual(@as(u32, 3), seg.info.stringTable(1).?.shared_count);
    try std.testing.expectEqual(@as(usize, 3), try expectSegmentStrings(allocator, &seg));
}

test "MergedSegmentWriter: an abandoned writer deletes its partial file" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var w = try MergedSegmentWriter.begin(allocator, io, tmp.dir, "abandoned.dat", sst_schema, 2, 0, 4, &.{}, 2);
    var abandoned = false;
    defer if (!abandoned) w.deinit();
    var ids = [_]i32{ 0, 1, 2, 3 };
    var s = try buildStringCol(allocator, sst_vals[0..4]);
    defer s.deinit(allocator);
    var nulls: [1]u8 = .{0};
    for (0..4) |i| column.setValidBit(&nulls, i, column.isValidBit(&sst_nulls, i));
    try w.writeRowGroup(&.{ .{ .data = .{ .int = &ids } }, s.view(&nulls) });
    try tmp.dir.access(io, "abandoned.dat", .{});
    w.deinit();
    abandoned = true;
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "abandoned.dat", .{}));
}

test "DictBlock.offsetsInPlace views u32-aligned offsets and declines misaligned ones" {
    var storage_words: [8]u32 = undefined;
    const bytes = std.mem.sliceAsBytes(&storage_words);
    for (0..4) |i| std.mem.writeInt(u32, bytes[4 + i * 4 ..][0..4], @intCast(i * 3), .little);
    const aligned: segment_reader.DictBlock = .{ .ndv = 3, .code_width = 1, .offsets_region = bytes[4..20], .dict_bytes = &.{}, .codes = &.{} };
    if (comptime @import("builtin").cpu.arch.endian() == .little) {
        try std.testing.expectEqualSlices(u32, &.{ 0, 3, 6, 9 }, aligned.offsetsInPlace().?);
    } else {
        try std.testing.expectEqual(@as(?[]const u32, null), aligned.offsetsInPlace());
    }
    const misaligned: segment_reader.DictBlock = .{ .ndv = 3, .code_width = 1, .offsets_region = bytes[5..21], .dict_bytes = &.{}, .codes = &.{} };
    try std.testing.expectEqual(@as(?[]const u32, null), misaligned.offsetsInPlace());
}

test "DictBlock.rowCode reads each writer code width little-endian" {
    const cases = .{
        .{ .width = 1, .codes = &[_]u8{ 7, 0, 255 }, .want = &[_]u32{ 7, 0, 255 } },
        .{ .width = 2, .codes = &[_]u8{ 0x34, 0x12, 0xff, 0xff, 0, 1 }, .want = &[_]u32{ 0x1234, 0xffff, 0x100 } },
        .{ .width = 4, .codes = &[_]u8{ 0x78, 0x56, 0x34, 0x12, 1, 0, 1, 0 }, .want = &[_]u32{ 0x12345678, 0x10001 } },
    };
    inline for (cases) |c| {
        const db: segment_reader.DictBlock = .{ .ndv = 0, .code_width = c.width, .offsets_region = &.{}, .dict_bytes = &.{}, .codes = c.codes };
        for (c.want, 0..) |want, row| try std.testing.expectEqual(want, db.rowCode(row));
    }
}

test "DictBlock.codesInto and codesAt match rowCode at every width and offset" {
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const random = prng.random();
    var bytes: [4 * 203 + 3]u8 = undefined;
    random.bytes(&bytes);
    const rows = [_]u32{ 0, 1, 9, 63, 64, 100, 150, 201, 202 };
    inline for (.{ 1, 2, 3, 4 }) |width| {
        for (0..3) |skew| {
            const n = 203 - skew;
            const db: segment_reader.DictBlock = .{ .ndv = 0, .code_width = width, .offsets_region = &.{}, .dict_bytes = &.{}, .codes = bytes[skew..][0 .. n * width] };
            var all: [203]u32 = undefined;
            db.codesInto(all[0..n]);
            for (all[0..n], 0..) |code, row| try std.testing.expectEqual(db.rowCode(row), code);
            var picked: [rows.len]u32 = undefined;
            const kept = rows.len - skew;
            db.codesAt(rows[0..kept], picked[0..kept]);
            for (rows[0..kept], picked[0..kept]) |row, code| try std.testing.expectEqual(db.rowCode(row), code);
        }
    }
}

test "DictBlock.codeRangeInto matches the per-row range test at every width and offset" {
    const n = 203;
    var storage_bytes: [n * 4 + 3]u8 = undefined;
    var mask: [n]bool = undefined;
    var prng = std.Random.DefaultPrng.init(0xd1c7);
    const rand = prng.random();
    inline for (.{ 1, 2, 4 }) |width| {
        const T = @Int(.unsigned, width * 8);
        // Code counts a width can hold, up to every code at width 1.
        const ndv: u32 = if (width == 1) 256 else 1000;
        for ([_]usize{ 0, 1, 3 }) |misalign| {
            const codes = storage_bytes[misalign..][0 .. n * width];
            for (0..n) |i| std.mem.writeInt(T, codes[i * width ..][0..width], @intCast(rand.uintLessThan(u32, ndv)), .little);
            const db: segment_reader.DictBlock = .{ .ndv = ndv, .code_width = width, .offsets_region = &.{}, .dict_bytes = &.{}, .codes = codes };
            const ranges = [_][2]u32{ .{ 0, 0 }, .{ 0, ndv }, .{ 0, 1 }, .{ ndv - 1, ndv }, .{ 17, 140 }, .{ ndv, ndv } };
            for (ranges) |r| {
                for ([_]bool{ false, true }) |outside| {
                    db.codeRangeInto(r[0], r[1] - r[0], outside, n, &mask);
                    for (mask, 0..) |got, i| {
                        const code = db.rowCode(i);
                        try std.testing.expectEqual((code >= r[0] and code < r[1]) != outside, got);
                    }
                }
            }
        }
    }
}

test "DictBlock.countEntries counts each code's rows and names one, with one counter lane or several" {
    var storage_bytes: [400 * 4 + 3]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(0xc0de);
    const rand = prng.random();
    inline for (.{ 1, 2, 4 }) |width| {
        const T = @Int(.unsigned, width * 8);
        // Few values per row take separate counter lanes; many take one.
        for ([_]u32{ 5, if (width == 1) 256 else 300 }) |ndv| {
            for ([_]u32{ 0, 1, 3, 400 }) |n| {
                const codes = storage_bytes[n % 4 ..][0 .. n * width];
                // Runs of a repeated code, as clustered columns hold.
                var row: usize = 0;
                while (row < n) {
                    const code: T = @intCast(rand.uintLessThan(u32, ndv));
                    const run = @min(n - row, rand.intRangeAtMost(usize, 1, 6));
                    for (row..row + run) |r| std.mem.writeInt(T, codes[r * width ..][0..width], code, .little);
                    row += run;
                }
                const db: segment_reader.DictBlock = .{ .ndv = ndv, .code_width = width, .offsets_region = &.{}, .dict_bytes = &.{}, .codes = codes };
                var counts: [4 * 300]u32 = @splat(0);
                var rows: [300]u32 = @splat(std.math.maxInt(u32));
                db.countEntries(n, counts[0 .. db.entryCountLanes(n) * ndv], rows[0..ndv]);
                var want: [300]u32 = @splat(0);
                for (0..n) |r| want[db.rowCode(r)] += 1;
                try std.testing.expectEqualSlices(u32, want[0..ndv], counts[0..ndv]);
                for (want[0..ndv], rows[0..ndv], 0..) |w, at, c| {
                    if (w > 0) try std.testing.expectEqual(@as(u32, @intCast(c)), db.rowCode(at));
                }
            }
        }
    }
}

test "stringBlockBytes counts the string bytes raw, dict and FSST blocks decode to" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "num", .type = .string, .nullable = true },
            .{ .name = "tag", .type = .string, .nullable = true },
            .{ .name = "url", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = false,
        .compression = .lz4_fsst,
    };
    try schema.validate();

    // `num` is short and distinct (raw), `tag` low-card (dict) and `url`
    // long and distinct (FSST). A NULL `tag` row keeps a value the dict
    // codes as 0, so counting it would add that value's bytes.
    const n: usize = 2003;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const palette = [_][]const u8{ "red", "green", "blue", "magenta" };
    var ids: [n]i64 = undefined;
    var num_vals: [n][]const u8 = undefined;
    var tag_vals: [n][]const u8 = undefined;
    var url_vals: [n][]const u8 = undefined;
    var bm: [column.bitmapBytes(n)]u8 = @splat(0);
    var tag_bytes: u64 = 0;
    for (0..n) |i| {
        const valid = i % 5 != 0;
        column.setValidBit(&bm, i, valid);
        ids[i] = @intCast(i);
        num_vals[i] = if (valid) try std.fmt.allocPrint(arena.allocator(), "{d}", .{i}) else "";
        tag_vals[i] = palette[i % palette.len];
        if (valid) tag_bytes += tag_vals[i].len;
        url_vals[i] = try std.fmt.allocPrint(arena.allocator(), "https://example.com/products/category-{d}/item?id={d}&ref=search", .{ i % 13, i });
    }
    var num = try buildStringCol(allocator, &num_vals);
    defer num.deinit(allocator);
    var tag = try buildStringCol(allocator, &tag_vals);
    defer tag.deinit(allocator);
    var url = try buildStringCol(allocator, &url_vals);
    defer url.deinit(allocator);
    const columns = [_]ColumnView{ .{ .data = .{ .bigint = &ids } }, num.view(&bm), tag.view(&bm), url.view(null) };

    var info = try writeSegment(allocator, io, tmp.dir, "widths.dat", schema, 13, 0, n, &columns, false, 1);
    defer info.deinit(allocator);
    var seg = try readSegment(allocator, io, tmp.dir, "widths.dat", schema);
    defer seg.deinit();
    try std.testing.expectEqual(format.Encoding.raw, try blockEncodingOf(allocator, &seg, 0, 1));
    try std.testing.expectEqual(format.Encoding.dict, try blockEncodingOf(allocator, &seg, 0, 2));
    try std.testing.expectEqual(format.Encoding.fsst, try blockEncodingOf(allocator, &seg, 0, 3));

    var controller = memory.MemoryPool.init(1 << 24);

    var c = cache.Cache.init(allocator, &controller, 0);

    controller.shareCache(&c, controller.budget);

    defer c.deinit();
    const tc = cache.TableCache{ .cache = &c, .table_uid = 0 };

    // A raw block answers from its header and leaves the cache cold.
    try std.testing.expectEqual(@as(u64, num.offsets[n]), try seg.stringBlockBytes(allocator, 0, 1, true, tc));
    if (c.acquire(.{ .segment_id = 13, .row_group_idx = 0, .column_idx = 1 })) |e| {
        c.release(e);
        return error.TestUnexpectedResult;
    }
    var warm = try seg.borrowColumnBlock(allocator, 0, 1, tc);
    warm.release(allocator, tc);
    try std.testing.expectEqual(@as(u64, num.offsets[n]), try seg.stringBlockBytes(allocator, 0, 1, true, tc));

    try std.testing.expectEqual(tag_bytes, try seg.stringBlockBytes(allocator, 0, 2, true, tc));
    try std.testing.expectEqual(@as(u64, url.offsets[n]), try seg.stringBlockBytes(allocator, 0, 3, false, tc));
}

test {
    _ = column;
    _ = snappy;
    _ = format;
    _ = segment_writer;
    _ = segment_reader;
    _ = manifest;
    _ = schema_file;
    _ = tombstone;
    _ = compression;
    _ = segment_strings;
    _ = cache;
}
