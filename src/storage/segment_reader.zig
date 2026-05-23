//! Read a segment file into memory, parse header/footer, and decode columns
//! on demand. Caller owns returned OwnedColumns.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const native_endian = builtin.cpu.arch.endian();

/// Decode a packed fixed-width column block into a typed array. The on-disk
/// layout is little-endian and contiguous, so on a little-endian host it is
/// bit-identical to the in-memory `[]T` — one bulk copy at memory-bandwidth
/// speed, replacing a per-element `readInt` parse that ran ~10-40x slower on
/// wide scans. Big-endian hosts byteswap each element.
fn decodeFixed(comptime T: type, allocator: Allocator, values: []const u8, row_count: u32) ![]T {
    const data = try allocator.alloc(T, row_count);
    errdefer allocator.free(data);
    const n_bytes = @as(usize, row_count) * @sizeOf(T);
    if (native_endian == .little) {
        @memcpy(std.mem.sliceAsBytes(data), values[0..n_bytes]);
    } else {
        const Bits = std.meta.Int(.unsigned, @bitSizeOf(T));
        for (data, 0..) |*slot, i| {
            const bits = std.mem.readInt(Bits, values[i * @sizeOf(T) ..][0..@sizeOf(T)], .little);
            slot.* = @bitCast(bits);
        }
    }
    return data;
}

const types = @import("../types.zig");
const Type = types.Type;
const TableSchema = types.TableSchema;

const format = @import("format.zig");
const column = @import("column.zig");
const compression_mod = @import("compression.zig");
const storage_cache = @import("cache.zig");
const ColumnView = column.ColumnView;
const StringView = column.StringView;
const OwnedColumn = column.OwnedColumn;
const OwnedStringColumn = column.OwnedStringColumn;
const RowGroupMeta = format.RowGroupMeta;
const SegmentInfo = format.SegmentInfo;

pub const ReadSegment = struct {
    allocator: Allocator,
    file: Io.File,
    io: Io,
    info: SegmentInfo,

    pub fn deinit(self: *ReadSegment) void {
        for (self.info.row_groups) |rg| {
            self.allocator.free(rg.stats);
            self.allocator.free(@constCast(rg.col_offsets));
        }
        self.allocator.free(@constCast(self.info.row_groups));
        self.file.close(self.io);
        self.* = undefined;
    }

    /// Decode a single column out of a single row group. Caller frees via `OwnedColumn.deinit`.
    pub fn decodeColumn(
        self: ReadSegment,
        allocator: Allocator,
        schema: TableSchema,
        row_group_idx: usize,
        column_idx: usize,
    ) !OwnedColumn {
        return self.decodeColumnMaybeCached(allocator, schema, row_group_idx, column_idx, null);
    }

    /// Decode one column of one row group, reading only that column's block
    /// from disk (located via the footer's per-column offsets) instead of the
    /// whole segment file. `c` (if non-null) is the decompressed-block buffer
    /// pool: on a hit we touch no I/O at all (`has_nulls` is schema-derived);
    /// on a miss we pread the block, decompress, cache it, and decode.
    pub fn decodeColumnMaybeCached(
        self: ReadSegment,
        allocator: Allocator,
        schema: TableSchema,
        row_group_idx: usize,
        column_idx: usize,
        c: ?*storage_cache.Cache,
    ) !OwnedColumn {
        std.debug.assert(column_idx < schema.columns.len);
        const col_type = schema.columns[column_idx].type;
        const rg = self.info.row_groups[row_group_idx];
        // The writer emits a validity bitmap for every nullable column, so
        // `has_nulls` is fixed by the schema — no need to read the block header.
        const flags = format.ColumnBlockFlags{ .has_nulls = schema.columns[column_idx].nullable };

        if (c) |cc| {
            const key = storage_cache.Key{
                .segment_id = self.info.segment_id,
                .row_group_idx = @intCast(row_group_idx),
                .column_idx = @intCast(column_idx),
            };
            if (cc.acquire(key)) |entry| {
                defer cc.release(entry);
                return decodeRawColumn(allocator, col_type, entry.bytes, rg.row_count, flags);
            }
            // Miss: pread the block, decompress, hand the bytes to the cache.
            // The errdefer is scoped to the block so it fires only if we fail
            // before ownership transfers; once `insertPinned` returns the cache
            // owns the bytes and only the pin needs releasing.
            const entry = blk: {
                const block = try self.readColumnBlock(allocator, rg, column_idx);
                defer allocator.free(block);
                const raw = try getDecompressedBytes(allocator, block, 0);
                errdefer allocator.free(raw);
                break :blk try cc.insertPinned(key, raw);
            };
            defer cc.release(entry);
            return decodeRawColumn(allocator, col_type, entry.bytes, rg.row_count, flags);
        }

        const block = try self.readColumnBlock(allocator, rg, column_idx);
        defer allocator.free(block);
        return decodeBlock(allocator, block, 0, col_type, rg.row_count, flags);
    }

    /// Read the raw on-disk bytes (header + payload) of one column's block in a
    /// row group via a single positioned read. The block ends where the next
    /// column's begins (or at the row group's end for the last column).
    fn readColumnBlock(self: ReadSegment, allocator: Allocator, rg: RowGroupMeta, column_idx: usize) ![]u8 {
        const start = rg.col_offsets[column_idx];
        const end = if (column_idx + 1 < rg.col_offsets.len)
            rg.col_offsets[column_idx + 1]
        else
            rg.offset + rg.length;
        const buf = try allocator.alloc(u8, @intCast(end - start));
        errdefer allocator.free(buf);
        if (try self.file.readPositionalAll(self.io, buf, start) != buf.len) {
            return format.Error.UnexpectedEof;
        }
        return buf;
    }
};

pub fn readSegment(
    allocator: Allocator,
    io: Io,
    dir: Io.Dir,
    file_name: []const u8,
    schema: TableSchema,
) !ReadSegment {
    var file = try dir.openFile(io, file_name, .{});
    errdefer file.close(io);

    const file_size = (try file.stat(io)).size;
    if (file_size < format.header_size + format.footer_trailer_size) {
        return format.Error.SegmentTooSmall;
    }

    // Header (first 32 bytes) — magic, version, fingerprint, ids.
    var header: [format.header_size]u8 = undefined;
    if (try file.readPositionalAll(io, &header, 0) != format.header_size) return format.Error.UnexpectedEof;
    if (!std.mem.eql(u8, header[0..4], &format.segment_magic)) return format.Error.BadMagic;
    const version = format.readU16(header[4..6]);
    if (version != format.segment_version) return format.Error.UnsupportedVersion;
    const schema_fingerprint = format.readU64(header[8..16]);
    const segment_id = format.readU64(header[16..24]);
    const row_count = format.readU64(header[24..32]);

    // Trailer (last 8 bytes) — footer size + magic.
    var trailer: [format.footer_trailer_size]u8 = undefined;
    if (try file.readPositionalAll(io, &trailer, file_size - format.footer_trailer_size) != format.footer_trailer_size) {
        return format.Error.UnexpectedEof;
    }
    if (!std.mem.eql(u8, trailer[4..8], &format.segment_magic)) return format.Error.BadFooterMagic;
    const footer_size = format.readU32(trailer[0..4]);
    if (footer_size < format.footer_trailer_size or footer_size > file_size - format.header_size) {
        return format.Error.CorruptFooter;
    }

    // The footer is the only data region we read in full; column blocks are
    // pread on demand. It carries per-row-group offset/length/row_count, then
    // per-column min/max stats, then per-column block offsets.
    const footer = try allocator.alloc(u8, footer_size);
    defer allocator.free(footer);
    if (try file.readPositionalAll(io, footer, file_size - footer_size) != footer_size) {
        return format.Error.UnexpectedEof;
    }

    const row_group_count = format.readU32(footer[0..4]);
    const ncols = schema.columns.len;
    const per_rg = 16 + ncols * 32 + ncols * 8; // offset/len/rows + stats + col offsets
    const expected_footer = 4 + @as(usize, row_group_count) * per_rg + format.footer_trailer_size;
    if (expected_footer != footer_size) return format.Error.CorruptFooter;

    const row_groups = try allocator.alloc(RowGroupMeta, row_group_count);
    errdefer allocator.free(row_groups);
    var inited_rg: usize = 0;
    errdefer for (row_groups[0..inited_rg]) |rg| {
        allocator.free(rg.stats);
        allocator.free(@constCast(rg.col_offsets));
    };

    var off: usize = 4;
    for (row_groups) |*rg| {
        rg.offset = format.readU64(footer[off .. off + 8]);
        off += 8;
        rg.length = format.readU32(footer[off .. off + 4]);
        off += 4;
        rg.row_count = format.readU32(footer[off .. off + 4]);
        off += 4;

        const stats = try allocator.alloc(format.Stats, ncols);
        errdefer allocator.free(stats);
        for (stats) |*s| {
            s.min = format.readI128(footer[off .. off + 16]);
            off += 16;
            s.max = format.readI128(footer[off .. off + 16]);
            off += 16;
        }
        rg.stats = stats;

        const col_offsets = try allocator.alloc(u64, ncols);
        for (col_offsets) |*co| {
            co.* = format.readU64(footer[off .. off + 8]);
            off += 8;
        }
        rg.col_offsets = col_offsets;
        inited_rg += 1;
    }

    return ReadSegment{
        .allocator = allocator,
        .file = file,
        .io = io,
        .info = .{
            .segment_id = segment_id,
            .row_count = row_count,
            .schema_fingerprint = schema_fingerprint,
            .byte_size = file_size,
            .row_groups = row_groups,
        },
    };
}

// ---------------------------------------------------------------------------
// Block decoding
// ---------------------------------------------------------------------------

fn decodeBlock(
    allocator: Allocator,
    bytes: []const u8,
    offset: usize,
    col_type: Type,
    row_count: u32,
    flags: format.ColumnBlockFlags,
) !OwnedColumn {
    var owned_raw: ?[]u8 = null;
    defer if (owned_raw) |r| allocator.free(r);

    const raw = try readBlockRaw(allocator, bytes, offset, &owned_raw);
    return decodeRawColumn(allocator, col_type, raw, row_count, flags);
}

/// Reads the block at `offset`, returns its decompressed bytes. If the block
/// is stored uncompressed, returns a slice into `bytes` (zero allocation) and
/// leaves `*owned_raw` as null. Otherwise allocates the decompressed buffer,
/// assigns it to `*owned_raw`, and returns it.
fn readBlockRaw(
    allocator: Allocator,
    bytes: []const u8,
    offset: usize,
    owned_raw: *?[]u8,
) ![]const u8 {
    const kind_byte = bytes[offset];
    if (kind_byte > @intFromEnum(format.Compression.zstd)) return format.Error.UnknownCompression;
    const kind: format.Compression = @enumFromInt(kind_byte);
    const uncompressed_size = format.readU32(bytes[offset + 4 .. offset + 8]);
    const compressed_size = format.readU32(bytes[offset + 8 .. offset + 12]);
    const payload_start = offset + format.column_block_header_size;
    const payload = bytes[payload_start .. payload_start + compressed_size];

    switch (kind) {
        .none => return payload,
        .zstd => {
            const r = try compression_mod.decompress(allocator, payload, uncompressed_size);
            owned_raw.* = r;
            return r;
        },
    }
}

/// Like `readBlockRaw` but ALWAYS returns owned bytes (caller frees), so it
/// can be inserted into the cache. For uncompressed blocks, allocates a copy.
fn getDecompressedBytes(
    allocator: Allocator,
    bytes: []const u8,
    offset: usize,
) ![]u8 {
    const kind_byte = bytes[offset];
    if (kind_byte > @intFromEnum(format.Compression.zstd)) return format.Error.UnknownCompression;
    const kind: format.Compression = @enumFromInt(kind_byte);
    const uncompressed_size = format.readU32(bytes[offset + 4 .. offset + 8]);
    const compressed_size = format.readU32(bytes[offset + 8 .. offset + 12]);
    const payload_start = offset + format.column_block_header_size;
    const payload = bytes[payload_start .. payload_start + compressed_size];

    switch (kind) {
        .none => return allocator.dupe(u8, payload),
        .zstd => return compression_mod.decompress(allocator, payload, uncompressed_size),
    }
}

fn decodeRawColumn(
    allocator: Allocator,
    col_type: Type,
    raw: []const u8,
    row_count: u32,
    flags: format.ColumnBlockFlags,
) !OwnedColumn {
    // If has_nulls, the first `bitmapBytes(row_count)` bytes of `raw` are the
    // validity bitmap. Copy them out so OwnedColumn owns its bitmap, then
    // decode the rest as values.
    var nulls: ?[]u8 = null;
    errdefer if (nulls) |n| allocator.free(n);
    var values = raw;
    if (flags.has_nulls) {
        const bm_len = column.bitmapBytes(row_count);
        const bm_copy = try allocator.alloc(u8, bm_len);
        @memcpy(bm_copy, raw[0..bm_len]);
        nulls = bm_copy;
        values = raw[bm_len..];
    }

    switch (col_type) {
        .int => return .{ .data = .{ .int = try decodeFixed(i32, allocator, values, row_count) }, .nulls = nulls },
        .bigint => return .{ .data = .{ .bigint = try decodeFixed(i64, allocator, values, row_count) }, .nulls = nulls },
        .boolean => {
            const data = try allocator.alloc(u8, row_count);
            errdefer allocator.free(data);
            @memcpy(data, values[0..row_count]);
            return .{ .data = .{ .boolean = data }, .nulls = nulls };
        },
        .varchar => return .{ .data = .{ .varchar = try decodeStringRaw(allocator, values, row_count) }, .nulls = nulls },
        .string => return .{ .data = .{ .string = try decodeStringRaw(allocator, values, row_count) }, .nulls = nulls },
        .float => return .{ .data = .{ .float = try decodeFixed(f32, allocator, values, row_count) }, .nulls = nulls },
        .double => return .{ .data = .{ .double = try decodeFixed(f64, allocator, values, row_count) }, .nulls = nulls },
        .date => return .{ .data = .{ .date = try decodeFixed(i32, allocator, values, row_count) }, .nulls = nulls },
        .datetime => return .{ .data = .{ .datetime = try decodeFixed(i64, allocator, values, row_count) }, .nulls = nulls },
        .tinyint => return .{ .data = .{ .tinyint = try decodeFixed(i8, allocator, values, row_count) }, .nulls = nulls },
        .smallint => return .{ .data = .{ .smallint = try decodeFixed(i16, allocator, values, row_count) }, .nulls = nulls },
        .largeint => return .{ .data = .{ .largeint = try decodeFixed(i128, allocator, values, row_count) }, .nulls = nulls },
        .char => return .{ .data = .{ .char = try decodeStringRaw(allocator, values, row_count) }, .nulls = nulls },
        .decimal64 => return .{ .data = .{ .decimal64 = try decodeFixed(i64, allocator, values, row_count) }, .nulls = nulls },
        .decimal128 => return .{ .data = .{ .decimal128 = try decodeFixed(i128, allocator, values, row_count) }, .nulls = nulls },
        .uuid => return .{ .data = .{ .uuid = try decodeFixed(u128, allocator, values, row_count) }, .nulls = nulls },
    }
}

fn decodeStringRaw(allocator: Allocator, raw: []const u8, row_count: u32) !OwnedStringColumn {
    var cursor: usize = 0;
    const byte_count = format.readU32(raw[cursor .. cursor + 4]);
    cursor += 4;

    const offsets = try allocator.alloc(u32, @as(usize, row_count) + 1);
    errdefer allocator.free(offsets);
    const off_bytes = offsets.len * 4;
    if (native_endian == .little) {
        @memcpy(std.mem.sliceAsBytes(offsets), raw[cursor .. cursor + off_bytes]);
    } else {
        for (offsets, 0..) |*slot, i| {
            slot.* = format.readU32(raw[cursor + i * 4 .. cursor + i * 4 + 4]);
        }
    }
    cursor += off_bytes;

    const data = try allocator.alloc(u8, byte_count);
    errdefer allocator.free(data);
    @memcpy(data, raw[cursor .. cursor + byte_count]);

    return .{ .offsets = offsets, .bytes = data };
}
