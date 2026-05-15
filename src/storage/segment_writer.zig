//! Build a segment file in memory from a set of ColumnViews, then flush to disk.
//!
//! Single entry point: `writeSegment`. Caller owns the returned SegmentInfo
//! (call `.deinit(allocator)`).

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Schema = types.Schema;

const format = @import("format.zig");
const column = @import("column.zig");
const compression_mod = @import("compression.zig");
const ColumnView = column.ColumnView;
const StringView = column.StringView;
const RowGroupMeta = format.RowGroupMeta;
const SegmentInfo = format.SegmentInfo;

pub fn writeSegment(
    allocator: Allocator,
    io: Io,
    dir: Io.Dir,
    file_name: []const u8,
    schema: Schema,
    segment_id: u64,
    schema_fingerprint: u64,
    row_group_size: usize,
    columns: []const ColumnView,
) !SegmentInfo {
    if (columns.len != schema.columns.len) return format.Error.SchemaMismatch;
    if (row_group_size == 0) return format.Error.InvalidRowGroupSize;

    const row_count: usize = if (columns.len == 0) 0 else columns[0].rowCount();
    for (columns, schema.columns) |view, schema_col| {
        if (view.rowCount() != row_count) return format.Error.UnevenColumns;
        if (std.meta.activeTag(view) != std.meta.activeTag(schema_col.type)) {
            return format.Error.SchemaMismatch;
        }
    }

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    // ---- Header ----
    try buf.appendSlice(allocator, &format.segment_magic);
    try appendU16(allocator, &buf, format.segment_version);
    try appendU16(allocator, &buf, 0); // flags
    try appendU64(allocator, &buf, schema_fingerprint);
    try appendU64(allocator, &buf, segment_id);
    try appendU64(allocator, &buf, @intCast(row_count));
    std.debug.assert(buf.items.len == format.header_size);

    // ---- Row groups ----
    var row_groups: std.ArrayList(RowGroupMeta) = .empty;
    errdefer {
        for (row_groups.items) |rg| allocator.free(rg.stats);
        row_groups.deinit(allocator);
    }

    var row_offset: usize = 0;
    while (row_offset < row_count) {
        const rows_in_group: usize = @min(row_group_size, row_count - row_offset);
        const rg_file_offset: u64 = @intCast(buf.items.len);

        try appendU32(allocator, &buf, @intCast(rows_in_group));
        try appendU32(allocator, &buf, 0); // padding

        for (columns) |view| {
            try writeColumnBlock(allocator, &buf, view, row_offset, row_offset + rows_in_group);
        }

        const rg_length: u32 = @intCast(buf.items.len - rg_file_offset);

        const stats = try allocator.alloc(format.Stats, columns.len);
        errdefer allocator.free(stats);
        for (columns, 0..) |view, ci| {
            stats[ci] = computeStats(view, row_offset, row_offset + rows_in_group);
        }

        try row_groups.append(allocator, .{
            .offset = rg_file_offset,
            .length = rg_length,
            .row_count = @intCast(rows_in_group),
            .stats = stats,
        });

        row_offset += rows_in_group;
    }

    // ---- Footer ----
    const footer_start = buf.items.len;
    try appendU32(allocator, &buf, @intCast(row_groups.items.len));
    for (row_groups.items) |rg| {
        try appendU64(allocator, &buf, rg.offset);
        try appendU32(allocator, &buf, rg.length);
        try appendU32(allocator, &buf, rg.row_count);
        for (rg.stats) |s| {
            try appendI64(allocator, &buf, s.min);
            try appendI64(allocator, &buf, s.max);
        }
    }
    const footer_size: u32 = @intCast(buf.items.len - footer_start + format.footer_trailer_size);
    try appendU32(allocator, &buf, footer_size);
    try buf.appendSlice(allocator, &format.segment_magic);

    // ---- Flush to disk ----
    try dir.writeFile(io, .{ .sub_path = file_name, .data = buf.items });

    return SegmentInfo{
        .segment_id = segment_id,
        .row_count = row_count,
        .schema_fingerprint = schema_fingerprint,
        .row_groups = try row_groups.toOwnedSlice(allocator),
    };
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

const appendU16 = format.appendU16;
const appendU32 = format.appendU32;
const appendU64 = format.appendU64;
const appendI32 = format.appendI32;
const appendI64 = format.appendI64;

/// Build the raw (uncompressed) column-block payload, then try flate-compress
/// it. Keep whichever is smaller. Prepend the on-disk header.
fn writeColumnBlock(
    allocator: Allocator,
    buf: *std.ArrayList(u8),
    view: ColumnView,
    row_start: usize,
    row_end: usize,
) !void {
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    try writeRawColumnBlock(allocator, &scratch, view, row_start, row_end);

    const raw_size: u32 = @intCast(scratch.items.len);

    // Try compressing. If the result is smaller, use it; otherwise keep raw.
    const compressed = try compression_mod.compress(allocator, scratch.items);
    defer allocator.free(compressed);

    const use_compressed = compressed.len < scratch.items.len;
    const kind: format.Compression = if (use_compressed) .flate else .none;
    const payload_size: u32 = if (use_compressed)
        @intCast(compressed.len)
    else
        raw_size;

    // Header: kind (u8) + 3-byte padding + uncompressed_size (u32) + compressed_size (u32)
    try buf.ensureUnusedCapacity(allocator, format.column_block_header_size + payload_size);
    buf.appendAssumeCapacity(@intFromEnum(kind));
    buf.appendSliceAssumeCapacity(&[_]u8{ 0, 0, 0 });
    var b4: [4]u8 = undefined;
    format.writeU32(&b4, raw_size);
    buf.appendSliceAssumeCapacity(&b4);
    format.writeU32(&b4, payload_size);
    buf.appendSliceAssumeCapacity(&b4);

    if (use_compressed) {
        buf.appendSliceAssumeCapacity(compressed);
    } else {
        buf.appendSliceAssumeCapacity(scratch.items);
    }
}

fn writeRawColumnBlock(
    allocator: Allocator,
    buf: *std.ArrayList(u8),
    view: ColumnView,
    row_start: usize,
    row_end: usize,
) !void {
    switch (view) {
        .int => |data| {
            const slice = data[row_start..row_end];
            try buf.ensureUnusedCapacity(allocator, slice.len * 4);
            for (slice) |v| {
                var b: [4]u8 = undefined;
                format.writeI32(&b, v);
                buf.appendSliceAssumeCapacity(&b);
            }
        },
        .bigint => |data| {
            const slice = data[row_start..row_end];
            try buf.ensureUnusedCapacity(allocator, slice.len * 8);
            for (slice) |v| {
                var b: [8]u8 = undefined;
                format.writeI64(&b, v);
                buf.appendSliceAssumeCapacity(&b);
            }
        },
        .boolean => |data| {
            try buf.appendSlice(allocator, data[row_start..row_end]);
        },
        .varchar => |sv| try writeStringBlock(allocator, buf, sv, row_start, row_end),
        .string => |sv| try writeStringBlock(allocator, buf, sv, row_start, row_end),
    }
}

fn computeStats(view: ColumnView, row_start: usize, row_end: usize) format.Stats {
    return switch (view) {
        .int => |data| blk: {
            const slice = data[row_start..row_end];
            var lo: i32 = std.math.maxInt(i32);
            var hi: i32 = std.math.minInt(i32);
            for (slice) |v| {
                if (v < lo) lo = v;
                if (v > hi) hi = v;
            }
            break :blk .{ .min = @intCast(lo), .max = @intCast(hi) };
        },
        .bigint => |data| blk: {
            const slice = data[row_start..row_end];
            var lo: i64 = std.math.maxInt(i64);
            var hi: i64 = std.math.minInt(i64);
            for (slice) |v| {
                if (v < lo) lo = v;
                if (v > hi) hi = v;
            }
            break :blk .{ .min = lo, .max = hi };
        },
        .boolean => |data| blk: {
            const slice = data[row_start..row_end];
            var lo: u8 = 1;
            var hi: u8 = 0;
            for (slice) |v| {
                if (v < lo) lo = v;
                if (v > hi) hi = v;
            }
            break :blk .{ .min = lo, .max = hi };
        },
        // Strings carry no stats in v0.2.
        .varchar, .string => .{ .min = 0, .max = 0 },
    };
}

fn writeStringBlock(
    allocator: Allocator,
    buf: *std.ArrayList(u8),
    sv: StringView,
    row_start: usize,
    row_end: usize,
) !void {
    const n = row_end - row_start;
    const byte_start = sv.offsets[row_start];
    const byte_end = sv.offsets[row_end];
    const byte_count = byte_end - byte_start;

    const total = 4 + (n + 1) * 4 + byte_count;
    try buf.ensureUnusedCapacity(allocator, total);

    var b4: [4]u8 = undefined;

    // byte_count
    format.writeU32(&b4, byte_count);
    buf.appendSliceAssumeCapacity(&b4);

    // offsets (rebased to 0)
    for (sv.offsets[row_start .. row_end + 1]) |off| {
        format.writeU32(&b4, off - byte_start);
        buf.appendSliceAssumeCapacity(&b4);
    }

    // bytes
    buf.appendSliceAssumeCapacity(sv.bytes[byte_start..byte_end]);
}
