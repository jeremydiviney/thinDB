//! Read a segment file into memory, parse header/footer, and decode columns
//! on demand. Caller owns returned OwnedColumns.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const Schema = types.Schema;

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
    file_bytes: []u8,
    info: SegmentInfo,

    pub fn deinit(self: *ReadSegment) void {
        for (self.info.row_groups) |rg| self.allocator.free(rg.stats);
        self.allocator.free(@constCast(self.info.row_groups));
        self.allocator.free(self.file_bytes);
        self.* = undefined;
    }

    /// Returns the byte slice for an entire row group (including its header).
    pub fn rowGroupBytes(self: ReadSegment, idx: usize) []const u8 {
        const rg = self.info.row_groups[idx];
        return self.file_bytes[rg.offset .. rg.offset + rg.length];
    }

    /// Decode a single column out of a single row group. Caller frees via `OwnedColumn.deinit`.
    pub fn decodeColumn(
        self: ReadSegment,
        allocator: Allocator,
        schema: Schema,
        row_group_idx: usize,
        column_idx: usize,
    ) !OwnedColumn {
        return self.decodeColumnMaybeCached(allocator, schema, row_group_idx, column_idx, null);
    }

    /// Same as `decodeColumn` but consults `c` (if non-null) for previously-
    /// decompressed block bytes. On hit, skips the flate decompress step. On
    /// miss, decompresses, inserts the bytes into the cache (cache then owns
    /// them), and decodes.
    pub fn decodeColumnMaybeCached(
        self: ReadSegment,
        allocator: Allocator,
        schema: Schema,
        row_group_idx: usize,
        column_idx: usize,
        c: ?*storage_cache.Cache,
    ) !OwnedColumn {
        std.debug.assert(column_idx < schema.columns.len);
        const col_type = schema.columns[column_idx].type;
        const rg = self.info.row_groups[row_group_idx];

        var cursor: usize = rg.offset + format.row_group_header_size;
        var i: usize = 0;
        while (i < column_idx) : (i += 1) {
            cursor += columnBlockSize(self.file_bytes, cursor);
        }

        const flags = format.ColumnBlockFlags.fromByte(self.file_bytes[cursor + 1]);

        // Try cache for the decompressed bytes.
        if (c) |cc| {
            const key = storage_cache.Key{
                .segment_id = self.info.segment_id,
                .row_group_idx = @intCast(row_group_idx),
                .column_idx = @intCast(column_idx),
            };
            if (cc.get(key)) |raw| {
                return decodeRawColumn(allocator, col_type, raw, rg.row_count, flags);
            }
            // Miss: decompress fresh + cache.
            const raw = try getDecompressedBytes(allocator, self.file_bytes, cursor);
            errdefer allocator.free(raw);
            try cc.put(key, raw);
            // After put(), cache OWNS raw — re-fetch a borrowed slice to decode from.
            const cached_raw = cc.get(key) orelse raw;
            return decodeRawColumn(allocator, col_type, cached_raw, rg.row_count, flags);
        }

        // No cache: original path.
        return decodeBlock(allocator, self.file_bytes, cursor, col_type, rg.row_count, flags);
    }
};

pub fn readSegment(
    allocator: Allocator,
    io: Io,
    dir: Io.Dir,
    file_name: []const u8,
    schema: Schema,
) !ReadSegment {
    const bytes = try dir.readFileAlloc(io, file_name, allocator, .unlimited);
    errdefer allocator.free(bytes);

    if (bytes.len < format.header_size + format.footer_trailer_size) {
        return format.Error.SegmentTooSmall;
    }
    if (!std.mem.eql(u8, bytes[0..4], &format.segment_magic)) {
        return format.Error.BadMagic;
    }

    const version = format.readU16(bytes[4..6]);
    if (version != format.segment_version) return format.Error.UnsupportedVersion;

    const schema_fingerprint = format.readU64(bytes[8..16]);
    const segment_id = format.readU64(bytes[16..24]);
    const row_count = format.readU64(bytes[24..32]);

    const trailer_off = bytes.len - format.footer_trailer_size;
    if (!std.mem.eql(u8, bytes[bytes.len - 4 .. bytes.len], &format.segment_magic)) {
        return format.Error.BadFooterMagic;
    }
    const footer_size = format.readU32(bytes[trailer_off .. trailer_off + 4]);
    if (footer_size < format.footer_trailer_size or footer_size > bytes.len - format.header_size) {
        return format.Error.CorruptFooter;
    }

    const footer_start = bytes.len - footer_size;
    const row_group_count = format.readU32(bytes[footer_start .. footer_start + 4]);

    const stats_bytes_per_rg = schema.columns.len * 16; // 8 bytes min + 8 bytes max
    const expected_footer = 4 + @as(usize, row_group_count) * (16 + stats_bytes_per_rg) + format.footer_trailer_size;
    if (expected_footer != footer_size) return format.Error.CorruptFooter;

    const row_groups = try allocator.alloc(RowGroupMeta, row_group_count);
    errdefer allocator.free(row_groups);
    var inited_rg: usize = 0;
    errdefer for (row_groups[0..inited_rg]) |rg| allocator.free(rg.stats);

    var off: usize = footer_start + 4;
    for (row_groups) |*rg| {
        rg.offset = format.readU64(bytes[off .. off + 8]);
        off += 8;
        rg.length = format.readU32(bytes[off .. off + 4]);
        off += 4;
        rg.row_count = format.readU32(bytes[off .. off + 4]);
        off += 4;

        const stats = try allocator.alloc(format.Stats, schema.columns.len);
        for (stats) |*s| {
            s.min = format.readI64(bytes[off .. off + 8]);
            off += 8;
            s.max = format.readI64(bytes[off .. off + 8]);
            off += 8;
        }
        rg.stats = stats;
        inited_rg += 1;
    }

    return ReadSegment{
        .allocator = allocator,
        .file_bytes = bytes,
        .info = .{
            .segment_id = segment_id,
            .row_count = row_count,
            .schema_fingerprint = schema_fingerprint,
            .row_groups = row_groups,
        },
    };
}

// ---------------------------------------------------------------------------
// Block decoding
// ---------------------------------------------------------------------------

fn columnBlockSize(bytes: []const u8, offset: usize) usize {
    // Header layout: u8 kind + 3 pad + u32 uncompressed_size + u32 compressed_size
    const compressed_size = format.readU32(bytes[offset + 8 .. offset + 12]);
    return format.column_block_header_size + compressed_size;
}

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
        .int => {
            const data = try allocator.alloc(i32, row_count);
            errdefer allocator.free(data);
            var i: usize = 0;
            while (i < row_count) : (i += 1) {
                data[i] = format.readI32(values[i * 4 .. i * 4 + 4]);
            }
            return .{ .data = .{ .int = data }, .nulls = nulls };
        },
        .bigint => {
            const data = try allocator.alloc(i64, row_count);
            errdefer allocator.free(data);
            var i: usize = 0;
            while (i < row_count) : (i += 1) {
                data[i] = format.readI64(values[i * 8 .. i * 8 + 8]);
            }
            return .{ .data = .{ .bigint = data }, .nulls = nulls };
        },
        .boolean => {
            const data = try allocator.alloc(u8, row_count);
            errdefer allocator.free(data);
            @memcpy(data, values[0..row_count]);
            return .{ .data = .{ .boolean = data }, .nulls = nulls };
        },
        .varchar => {
            const owned = try decodeStringRaw(allocator, values, row_count);
            return .{ .data = .{ .varchar = owned }, .nulls = nulls };
        },
        .string => {
            const owned = try decodeStringRaw(allocator, values, row_count);
            return .{ .data = .{ .string = owned }, .nulls = nulls };
        },
        .float => {
            const data = try allocator.alloc(f32, row_count);
            errdefer allocator.free(data);
            var i: usize = 0;
            while (i < row_count) : (i += 1) {
                data[i] = format.readF32(values[i * 4 .. i * 4 + 4]);
            }
            return .{ .data = .{ .float = data }, .nulls = nulls };
        },
        .double => {
            const data = try allocator.alloc(f64, row_count);
            errdefer allocator.free(data);
            var i: usize = 0;
            while (i < row_count) : (i += 1) {
                data[i] = format.readF64(values[i * 8 .. i * 8 + 8]);
            }
            return .{ .data = .{ .double = data }, .nulls = nulls };
        },
        .date => {
            const data = try allocator.alloc(i32, row_count);
            errdefer allocator.free(data);
            var i: usize = 0;
            while (i < row_count) : (i += 1) {
                data[i] = format.readI32(values[i * 4 .. i * 4 + 4]);
            }
            return .{ .data = .{ .date = data }, .nulls = nulls };
        },
        .datetime => {
            const data = try allocator.alloc(i64, row_count);
            errdefer allocator.free(data);
            var i: usize = 0;
            while (i < row_count) : (i += 1) {
                data[i] = format.readI64(values[i * 8 .. i * 8 + 8]);
            }
            return .{ .data = .{ .datetime = data }, .nulls = nulls };
        },
        .tinyint => {
            const data = try allocator.alloc(i8, row_count);
            errdefer allocator.free(data);
            for (data, 0..) |*slot, i| slot.* = @bitCast(values[i]);
            return .{ .data = .{ .tinyint = data }, .nulls = nulls };
        },
        .smallint => {
            const data = try allocator.alloc(i16, row_count);
            errdefer allocator.free(data);
            for (data, 0..) |*slot, i| {
                slot.* = std.mem.readInt(i16, values[i * 2 ..][0..2], .little);
            }
            return .{ .data = .{ .smallint = data }, .nulls = nulls };
        },
        .largeint => {
            const data = try allocator.alloc(i128, row_count);
            errdefer allocator.free(data);
            for (data, 0..) |*slot, i| {
                slot.* = std.mem.readInt(i128, values[i * 16 ..][0..16], .little);
            }
            return .{ .data = .{ .largeint = data }, .nulls = nulls };
        },
        .char => {
            const owned = try decodeStringRaw(allocator, values, row_count);
            return .{ .data = .{ .char = owned }, .nulls = nulls };
        },
    }
}

fn decodeStringRaw(allocator: Allocator, raw: []const u8, row_count: u32) !OwnedStringColumn {
    var cursor: usize = 0;
    const byte_count = format.readU32(raw[cursor .. cursor + 4]);
    cursor += 4;

    const offsets = try allocator.alloc(u32, @as(usize, row_count) + 1);
    errdefer allocator.free(offsets);
    var i: usize = 0;
    while (i < offsets.len) : (i += 1) {
        offsets[i] = format.readU32(raw[cursor + i * 4 .. cursor + i * 4 + 4]);
    }
    cursor += offsets.len * 4;

    const data = try allocator.alloc(u8, byte_count);
    errdefer allocator.free(data);
    @memcpy(data, raw[cursor .. cursor + byte_count]);

    return .{ .offsets = offsets, .bytes = data };
}
