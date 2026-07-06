//! Read a segment file into memory, parse header/footer, and decode columns
//! on demand. Caller owns returned OwnedColumns.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const native_endian = builtin.cpu.arch.endian();

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

/// `THINDB_LZ4_CACHE_DECODED=1`: cache at-rest LZ4 string blocks DECODED
/// (trading resident bytes for skipping the per-borrow whole-block
/// decompress). A/B lever for concurrent-scan workloads — SEPARABLE slice
/// pipelines borrow the same blocks N times per query.
fn cacheDecodedOverride() bool {
    return getenv("THINDB_LZ4_CACHE_DECODED") != null;
}

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
const fsst = @import("fsst.zig");
const storage_cache = @import("cache.zig");
const simd_mod = @import("../util/simd.zig");
const prof = @import("../util/prof.zig");
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
            const entry = (cc.acquire(key)) orelse try self.fillCacheEntry(allocator, cc, key, rg, column_idx);
            defer cc.release(entry);
            // LZ4-at-rest entries decompress per use (into recycled scratch —
            // a fresh alloc per access would re-fault freshly-zeroed pages
            // every time); plain entries decode in place.
            if (entry.compression == .lz4) {
                const scratch = try cc.acquireScratch(entry.uncompressed_size);
                defer cc.releaseScratch(scratch);
                const plain = scratch[0..entry.uncompressed_size];
                try compression_mod.lz4DecompressInto(entry.bytes, plain);
                return decodeColumnPayload(allocator, col_type, plain, rg.row_count, flags, entry.encoding);
            }
            return decodeColumnPayload(allocator, col_type, entry.bytes, rg.row_count, flags, entry.encoding);
        }

        const block = try self.readColumnBlock(allocator, rg, column_idx);
        defer allocator.free(block);
        return decodeBlock(allocator, block, 0, col_type, rg.row_count, flags);
    }

    /// Cache-miss fill: pread the block and insert it pinned. Most blocks are
    /// decompressed at fill (a hit costs only the decode); blocks the writer
    /// flagged `at_rest` (large raw string blocks under `.lz4` tables) instead
    /// cache the still-compressed payload — they trade a whole-block LZ4
    /// decompress per access for a much smaller resident set. The persistent
    /// payload lives in the cache's huge-page pool, so it's both allocated and
    /// (on eviction) freed there.
    fn fillCacheEntry(
        self: ReadSegment,
        allocator: Allocator,
        cc: *storage_cache.Cache,
        key: storage_cache.Key,
        rg: RowGroupMeta,
        column_idx: usize,
    ) !*storage_cache.Cache.Entry {
        const block = try self.readColumnBlock(allocator, rg, column_idx);
        defer allocator.free(block);
        const encoding = blockEncoding(block, 0);
        const block_alloc = cc.blockAllocator();

        const kind_byte = block[0];
        const flags = format.ColumnBlockFlags.fromByte(block[1]);
        if (kind_byte == @intFromEnum(format.Compression.lz4) and flags.at_rest and !cacheDecodedOverride()) {
            const uncompressed_size = format.readU32(block[4..8]);
            const compressed_size = format.readU32(block[8..12]);
            const payload_start = format.column_block_header_size;
            const copy = try block_alloc.alignedAlloc(u8, .@"16", compressed_size);
            errdefer block_alloc.free(copy);
            @memcpy(copy, block[payload_start .. payload_start + compressed_size]);
            return cc.insertPinnedCompressed(key, copy, encoding, .lz4, uncompressed_size);
        }

        const raw = try getDecompressedBytes(block_alloc, block, 0);
        errdefer block_alloc.free(raw);
        return cc.insertPinned(key, raw, encoding);
    }

    /// A pinned, decompressed column block borrowed for the duration of one
    /// scan `next()` call. `bytes` are the decompressed block bytes. Exactly
    /// one of `entry` / `owned` is set: `entry` when the bytes live in the
    /// shared cache (release the pin via `release`), `owned` when they were
    /// decompressed without a cache (free via `release`). Callers MUST call
    /// `release` to drop the pin / free the buffer.
    pub const BorrowedBlock = struct {
        bytes: []const u8,
        /// Value encoding of `bytes`. A `.for_` block's `bytes` are narrow
        /// deltas, not native-width values, so `viewRawColumn` declines a direct
        /// in-place view. The scan's borrow path instead expands the FOR codes
        /// ONCE into `expanded` (native width) and views over that buffer —
        /// keeping the rest of the projection zero-copy. Raw blocks view in place.
        encoding: format.Encoding = .raw,
        entry: ?*storage_cache.Cache.Entry = null,
        owned: ?[]align(16) u8 = null,
        /// `owned` came from the cache's scratch pool (LZ4-at-rest decompress)
        /// rather than `allocator` — `release` returns it to the pool. Only set
        /// alongside a non-null cache.
        pooled: bool = false,
        /// Native-width expansion of a FOR-encoded block, allocated by the
        /// borrow path so a single FOR column doesn't force the whole row group
        /// onto the owned-decode path. Freed on `release`. Null for raw blocks.
        expanded: ?OwnedColumn = null,

        pub fn release(self: *BorrowedBlock, allocator: Allocator, c: ?*storage_cache.Cache) void {
            if (self.entry) |e| {
                if (c) |cc| cc.release(e);
            }
            if (self.owned) |b| {
                if (self.pooled) c.?.releaseScratch(b) else allocator.free(b);
            }
            if (self.expanded) |*col| col.deinit(allocator);
            self.* = undefined;
        }
    };

    /// Pin (and, on a miss, decompress + cache) one column's decompressed block
    /// so the caller can build a BORROWED typed view over it. The returned
    /// `BorrowedBlock` keeps the cache pin held until `release` — used by the
    /// scan-side in-place filter, which evaluates the predicate and compacts
    /// survivors out of the borrowed view, then releases, all within one
    /// `next()`. When `c` is null the block is decompressed into an owned
    /// buffer carried by the returned struct (still skips the decode-copy).
    pub fn borrowColumnBlock(
        self: ReadSegment,
        allocator: Allocator,
        row_group_idx: usize,
        column_idx: usize,
        c: ?*storage_cache.Cache,
    ) !BorrowedBlock {
        const rg = self.info.row_groups[row_group_idx];
        if (c) |cc| {
            const key = storage_cache.Key{
                .segment_id = self.info.segment_id,
                .row_group_idx = @intCast(row_group_idx),
                .column_idx = @intCast(column_idx),
            };
            const entry = (cc.acquire(key)) orelse try self.fillCacheEntry(allocator, cc, key, rg, column_idx);
            // LZ4-at-rest: decompress the whole block into recycled scratch
            // and drop the pin immediately — the borrow then behaves exactly
            // like the cacheless owned path, and every downstream consumer
            // sees plain decompressed bytes. Scratch (not a fresh alloc): a
            // per-access alloc re-faults freshly-zeroed pages on every borrow.
            if (entry.compression == .lz4) {
                const encoding = entry.encoding;
                defer cc.release(entry);
                const scratch = try cc.acquireScratch(entry.uncompressed_size);
                errdefer cc.releaseScratch(scratch);
                try compression_mod.lz4DecompressInto(entry.bytes, scratch[0..entry.uncompressed_size]);
                return .{
                    .bytes = scratch[0..entry.uncompressed_size],
                    .encoding = encoding,
                    .owned = scratch,
                    .pooled = true,
                };
            }
            return .{ .bytes = entry.bytes, .encoding = entry.encoding, .entry = entry };
        }
        const block = try self.readColumnBlock(allocator, rg, column_idx);
        defer allocator.free(block);
        const encoding = blockEncoding(block, 0);
        const raw = try getDecompressedBytes(allocator, block, 0);
        return .{ .bytes = raw, .encoding = encoding, .owned = raw };
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
    const per_rg = 16 + ncols * 56 + ncols * 8; // offset/len/rows + stats + col offsets
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
            s.sum = format.readI128(footer[off .. off + 16]);
            off += 16;
            s.null_count = format.readU64(footer[off .. off + 8]);
            off += 8;
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

/// Read the per-block `encoding` byte from a column-block header at `offset`.
fn blockEncoding(bytes: []const u8, offset: usize) format.Encoding {
    const b = bytes[offset + format.column_block_encoding_offset];
    // Unknown encodings degrade to raw; the writer fully controls this byte and
    // an unknown value would be caught by a downstream decode mismatch, but we
    // keep the reader total here rather than introducing a new error path for a
    // byte the writer fully controls.
    return if (b <= @intFromEnum(format.Encoding.fsst)) @enumFromInt(b) else .raw;
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

    const encoding = blockEncoding(bytes, offset);
    const raw = try readBlockRaw(allocator, bytes, offset, &owned_raw);
    return decodeColumnPayload(allocator, col_type, raw, row_count, flags, encoding);
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
    if (kind_byte > @intFromEnum(format.Compression.lz4)) return format.Error.UnknownCompression;
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
        .lz4 => {
            const r = try allocator.alloc(u8, uncompressed_size);
            errdefer allocator.free(r);
            try compression_mod.lz4DecompressInto(payload, r);
            owned_raw.* = r;
            return r;
        },
    }
}

/// Like `readBlockRaw` but ALWAYS returns owned, 16-byte-aligned bytes (caller
/// frees), so it can be inserted into the cache and viewed zero-copy. For
/// uncompressed blocks, allocates an aligned copy.
fn getDecompressedBytes(
    allocator: Allocator,
    bytes: []const u8,
    offset: usize,
) ![]align(16) u8 {
    const kind_byte = bytes[offset];
    if (kind_byte > @intFromEnum(format.Compression.lz4)) return format.Error.UnknownCompression;
    const kind: format.Compression = @enumFromInt(kind_byte);
    const uncompressed_size = format.readU32(bytes[offset + 4 .. offset + 8]);
    const compressed_size = format.readU32(bytes[offset + 8 .. offset + 12]);
    const payload_start = offset + format.column_block_header_size;
    const payload = bytes[payload_start .. payload_start + compressed_size];

    switch (kind) {
        .none => {
            const dst = try allocator.alignedAlloc(u8, .@"16", payload.len);
            @memcpy(dst, payload);
            return dst;
        },
        .zstd => return compression_mod.decompressAligned(allocator, payload, uncompressed_size),
        .lz4 => return compression_mod.lz4DecompressAligned(allocator, payload, uncompressed_size),
    }
}

/// Decode a decompressed block payload into an OwnedColumn, dispatching on the
/// block's value encoding. Both encodings share the validity-bitmap prefix
/// convention (bitmap first when nullable); a `.for_` block's value region is
/// expanded back to the native-width values so the produced column is the SAME
/// native shape every consumer already expects.
pub fn decodeColumnPayload(
    allocator: Allocator,
    col_type: Type,
    raw: []const u8,
    row_count: u32,
    flags: format.ColumnBlockFlags,
    encoding: format.Encoding,
) !OwnedColumn {
    return switch (encoding) {
        .raw => decodeRawColumn(allocator, col_type, raw, row_count, flags),
        .for_ => decodeForColumn(allocator, col_type, raw, row_count, flags),
        .dict => decodeDictColumn(allocator, col_type, raw, row_count, flags),
        .rle => decodeRleColumn(allocator, col_type, raw, row_count, flags),
        .fsst => decodeFsstColumn(allocator, col_type, raw, row_count, flags),
    };
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
        .json => return .{ .data = .{ .json = try decodeStringRaw(allocator, values, row_count) }, .nulls = nulls },
        .decimal64 => return .{ .data = .{ .decimal64 = try decodeFixed(i64, allocator, values, row_count) }, .nulls = nulls },
        .decimal128 => return .{ .data = .{ .decimal128 = try decodeFixed(i128, allocator, values, row_count) }, .nulls = nulls },
        .uuid => return .{ .data = .{ .uuid = try decodeFixed(u128, allocator, values, row_count) }, .nulls = nulls },
    }
}

/// A FOR (Frame-of-Reference) block's value region, parsed in place over the
/// decompressed payload (past any validity bitmap): the `base`, the delta
/// `width` in bytes, and the `codes` (row_count × width narrow deltas). This is
/// the hook Phase 2B's FOR-aware filter consumes — it lets a predicate compare
/// against `const - base` over the narrow codes without expanding to native
/// width. In 2A it is exercised only by a unit test.
pub const ForBlock = struct {
    base: i128,
    width: u8,
    codes: []const u8,
};

/// Parse the FOR header + codes out of a decompressed block payload. `values`
/// is the payload past the validity bitmap (the caller strips the bitmap the
/// same way the raw path does). Layout: `[base i128 LE][width u8][3 pad][codes]`.
pub fn forBlockOf(values: []const u8, row_count: u32) ForBlock {
    const base = format.readI128(values[0..16]);
    const width = values[16];
    const codes_start = 16 + 1 + 3;
    const codes = values[codes_start .. codes_start + @as(usize, row_count) * width];
    return .{ .base = base, .width = width, .codes = codes };
}

/// Split a decompressed FOR block payload into (validity bitmap, ForBlock).
/// Mirrors the bitmap-stripping the raw/expand paths do, but keeps the codes
/// narrow — the Phase 2B FOR-aware filter compares against these directly.
pub const ForView = struct {
    nulls: ?[]const u8,
    block: ForBlock,
};

pub fn forViewOf(raw: []const u8, row_count: u32, flags: format.ColumnBlockFlags) ForView {
    var nulls: ?[]const u8 = null;
    var values = raw;
    if (flags.has_nulls) {
        const bm_len = column.bitmapBytes(row_count);
        nulls = raw[0..bm_len];
        values = raw[bm_len..];
    }
    return .{ .nulls = nulls, .block = forBlockOf(values, row_count) };
}

/// Compare the narrow FOR codes against `want` under `op`, writing the result
/// into `mask` — never expanding to native width. The width tiers a v9 FOR
/// block can carry are u8/u16/u32 (8-byte deltas never beat the 8-byte native
/// width, so the writer never emits width 8). `want` is the constant already
/// translated into the code domain (`const - base`) by `translateForLeaf`,
/// which guarantees it is in `[0, max_code]` for this width — so the unsigned
/// comparison over codes is bit-for-bit identical to comparing the native
/// values. NULLs are not handled here; the caller ANDs the validity bitmap.
///
/// SIMD over an aligned codes slice; an unaligned codes start (the FOR payload
/// begins past a width-misaligned validity bitmap) falls back to a per-row
/// narrow load + scalar compare — still narrow-width, no native expansion.
/// The `@ptrCast`/`@alignCast` stays at this storage boundary.
pub fn forCompareInto(fb: ForBlock, op: simd_mod.CmpOp, want: u64, row_count: u32, mask: []bool) void {
    switch (fb.width) {
        1 => forCompareWidth(u8, fb.codes, @truncate(want), row_count, op, mask),
        2 => forCompareWidth(u16, fb.codes, @truncate(want), row_count, op, mask),
        4 => forCompareWidth(u32, fb.codes, @truncate(want), row_count, op, mask),
        // width 8 is never emitted by the writer; degrade safely by reading
        // each code wide rather than reaching for an unreachable.
        else => for (mask[0..row_count], 0..) |*m, i| {
            const code = readForCode(fb.codes, fb.width, i);
            m.* = cmpScalar(u64, code, want, op);
        },
    }
}

fn forCompareWidth(comptime U: type, codes: []const u8, want: U, row_count: u32, op: simd_mod.CmpOp, mask: []bool) void {
    const n: usize = row_count;
    if (@sizeOf(U) == 1) {
        const typed: []const U = codes[0..n];
        switch (op) {
            inline else => |o| simd_mod.compareInto(U, o, typed, want, mask[0..n]),
        }
        return;
    }
    if (@intFromPtr(codes.ptr) % @alignOf(U) == 0) {
        const typed: []const U = @as([*]const U, @ptrCast(@alignCast(codes.ptr)))[0..n];
        switch (op) {
            inline else => |o| simd_mod.compareInto(U, o, typed, want, mask[0..n]),
        }
        return;
    }
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const code = std.mem.readInt(U, codes[i * @sizeOf(U) ..][0..@sizeOf(U)], .little);
        mask[i] = cmpScalar(U, code, want, op);
    }
}

fn cmpScalar(comptime U: type, a: U, b: U, op: simd_mod.CmpOp) bool {
    return switch (op) {
        .eq => a == b,
        .neq => a != b,
        .lt => a < b,
        .lte => a <= b,
        .gt => a > b,
        .gte => a >= b,
    };
}

/// Reconstruct the native value `base + code` for each row whose `mask` bit is
/// set, appending the survivors in order into `out` (sized to the survivor
/// count). Only survivors are expanded — the bandwidth win is that the full
/// column was already filtered narrow; materialization touches survivors only.
pub fn forExpandSurvivors(comptime T: type, fb: ForBlock, mask: []const bool, out: []T) void {
    const base: T = @intCast(fb.base);
    switch (fb.width) {
        1 => expandSurvivorsW(T, u8, base, fb.codes, mask, out),
        2 => expandSurvivorsW(T, u16, base, fb.codes, mask, out),
        4 => expandSurvivorsW(T, u32, base, fb.codes, mask, out),
        8 => expandSurvivorsW(T, u64, base, fb.codes, mask, out),
        else => unreachable,
    }
}

/// `out[j++] = base + codes[i]` for each set `mask[i]` — native add (no i128),
/// reading the delta directly at its width instead of via a per-row temp copy.
inline fn expandSurvivorsW(comptime T: type, comptime W: type, base: T, codes: []const u8, mask: []const bool, out: []T) void {
    const wsz = @sizeOf(W);
    var j: usize = 0;
    for (mask, 0..) |m, i| {
        if (!m) continue;
        const d = std.mem.readInt(W, codes[i * wsz ..][0..wsz], .little);
        out[j] = base +% @as(T, @intCast(d));
        j += 1;
    }
}

/// Expand a FOR-encoded block back to its native-width values so the produced
/// OwnedColumn has the identical shape a raw block would. Reconstructs each row
/// as `base + delta`. NULL rows (masked by the validity bitmap) still get a
/// delta slot expanded into a placeholder value, which consumers ignore.
pub fn decodeForColumn(
    allocator: Allocator,
    col_type: Type,
    raw: []const u8,
    row_count: u32,
    flags: format.ColumnBlockFlags,
) !OwnedColumn {
    const _pt = if (prof.enabled) prof.nowTicks() else 0;
    defer if (prof.enabled) prof.add("for-decode (expand→native)", @intCast(@max(0, prof.nowTicks() - _pt)));

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

    const fb = forBlockOf(values, row_count);

    // The writer only ever FOR-encodes these integer-family fixed-width types.
    return switch (col_type) {
        .int => .{ .data = .{ .int = try expandFor(i32, allocator, fb, row_count) }, .nulls = nulls },
        .date => .{ .data = .{ .date = try expandFor(i32, allocator, fb, row_count) }, .nulls = nulls },
        .bigint => .{ .data = .{ .bigint = try expandFor(i64, allocator, fb, row_count) }, .nulls = nulls },
        .datetime => .{ .data = .{ .datetime = try expandFor(i64, allocator, fb, row_count) }, .nulls = nulls },
        .decimal64 => .{ .data = .{ .decimal64 = try expandFor(i64, allocator, fb, row_count) }, .nulls = nulls },
        .smallint => .{ .data = .{ .smallint = try expandFor(i16, allocator, fb, row_count) }, .nulls = nulls },
        .tinyint => .{ .data = .{ .tinyint = try expandFor(i8, allocator, fb, row_count) }, .nulls = nulls },
        .boolean => .{ .data = .{ .boolean = try expandFor(u8, allocator, fb, row_count) }, .nulls = nulls },
        else => unreachable,
    };
}

/// One narrow delta from the codes region (little-endian, `width` bytes).
fn readForCode(codes: []const u8, width: u8, row: usize) u64 {
    var b8: [8]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 0 };
    const off = row * width;
    @memcpy(b8[0..width], codes[off .. off + width]);
    return std.mem.readInt(u64, &b8, .little);
}

fn expandFor(comptime T: type, allocator: Allocator, fb: ForBlock, row_count: u32) ![]T {
    const data = try allocator.alloc(T, row_count);
    errdefer allocator.free(data);
    const base: T = @intCast(fb.base);
    switch (fb.width) {
        1 => expandWidth(T, u8, base, fb.codes, data),
        2 => expandWidth(T, u16, base, fb.codes, data),
        4 => expandWidth(T, u32, base, fb.codes, data),
        8 => expandWidth(T, u64, base, fb.codes, data),
        else => unreachable,
    }
    return data;
}

/// Reconstruct native values from the narrow unsigned deltas: `out[i] = base +
/// codes[i]`. FOR is only applied when `value = base + delta` provably fits `T`
/// (the writer narrows only when the span does), so the add runs in `T` — no
/// per-row i128. Vectorized in chunks; the deltas are read unaligned (the codes
/// region is not aligned in the decompressed buffer), then widened + added with
/// `base` broadcast.
inline fn expandWidth(comptime T: type, comptime W: type, base: T, codes: []const u8, out: []T) void {
    const wsz = @sizeOf(W);
    const N = std.simd.suggestVectorLength(T) orelse @max(1, 16 / @sizeOf(T));
    var i: usize = 0;
    if (N > 1) {
        const base_vec: @Vector(N, T) = @splat(base);
        const VecW = @Vector(N, W);
        while (i + N <= out.len) : (i += N) {
            const dv: VecW = @as(*align(1) const VecW, @ptrCast(codes.ptr + i * wsz)).*;
            const wide: @Vector(N, T) = @intCast(dv);
            out[i..][0..N].* = wide +% base_vec;
        }
    }
    while (i < out.len) : (i += 1) {
        const delta = std.mem.readInt(W, codes[i * wsz ..][0..wsz], .little);
        out[i] = base +% @as(T, @intCast(delta));
    }
}

/// An RLE block's value region, parsed in place over the decompressed payload
/// (past any validity bitmap): `n_runs` runs as two parallel arrays — each
/// run's value (native element width) and its row count. This is the hook
/// run-aware kernels consume: a predicate evaluates once per run and a
/// SUM/COUNT folds `value × length` without expanding to row width.
pub const RleBlock = struct {
    n_runs: u32,
    value_width: u8,
    /// n_runs × value_width, native little-endian.
    values: []const u8,
    /// n_runs × 4, u32 little-endian.
    lengths: []const u8,

    pub fn runLength(self: RleBlock, run: usize) u32 {
        return std.mem.readInt(u32, self.lengths[run * 4 ..][0..4], .little);
    }
};

/// Parse the RLE header + run arrays out of a decompressed block payload.
/// `values` is the payload past the validity bitmap. Layout:
/// `[n_runs u32][value_width u8][3 pad][run values][run lengths u32]`.
pub fn rleBlockOf(values: []const u8) RleBlock {
    const n_runs = format.readU32(values[0..4]);
    const width = values[4];
    const vals_start = 4 + 1 + 3;
    const vals_len = @as(usize, n_runs) * width;
    return .{
        .n_runs = n_runs,
        .value_width = width,
        .values = values[vals_start .. vals_start + vals_len],
        .lengths = values[vals_start + vals_len .. vals_start + vals_len + @as(usize, n_runs) * 4],
    };
}

/// Split a decompressed RLE block payload into (validity bitmap, RleBlock),
/// mirroring `forViewOf`.
pub const RleView = struct {
    nulls: ?[]const u8,
    block: RleBlock,
};

pub fn rleViewOf(raw: []const u8, row_count: u32, flags: format.ColumnBlockFlags) RleView {
    var nulls: ?[]const u8 = null;
    var values = raw;
    if (flags.has_nulls) {
        const bm_len = column.bitmapBytes(row_count);
        nulls = raw[0..bm_len];
        values = raw[bm_len..];
    }
    return .{ .nulls = nulls, .block = rleBlockOf(values) };
}

/// Expand an RLE-encoded block back to its native-width values so the produced
/// OwnedColumn has the identical shape a raw block would. The writer encodes
/// the stored value stream as-is (NULL placeholders included), so expansion
/// reproduces it exactly; the validity bitmap stays orthogonal.
pub fn decodeRleColumn(
    allocator: Allocator,
    col_type: Type,
    raw: []const u8,
    row_count: u32,
    flags: format.ColumnBlockFlags,
) !OwnedColumn {
    const _pt = if (prof.enabled) prof.nowTicks() else 0;
    defer if (prof.enabled) prof.add("rle-decode (expand→native)", @intCast(@max(0, prof.nowTicks() - _pt)));

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

    const rb = rleBlockOf(values);

    // The writer only ever RLE-encodes these integer-family fixed-width types.
    return switch (col_type) {
        .int => .{ .data = .{ .int = try expandRle(i32, allocator, rb, row_count) }, .nulls = nulls },
        .date => .{ .data = .{ .date = try expandRle(i32, allocator, rb, row_count) }, .nulls = nulls },
        .bigint => .{ .data = .{ .bigint = try expandRle(i64, allocator, rb, row_count) }, .nulls = nulls },
        .datetime => .{ .data = .{ .datetime = try expandRle(i64, allocator, rb, row_count) }, .nulls = nulls },
        .decimal64 => .{ .data = .{ .decimal64 = try expandRle(i64, allocator, rb, row_count) }, .nulls = nulls },
        .smallint => .{ .data = .{ .smallint = try expandRle(i16, allocator, rb, row_count) }, .nulls = nulls },
        .tinyint => .{ .data = .{ .tinyint = try expandRle(i8, allocator, rb, row_count) }, .nulls = nulls },
        .boolean => .{ .data = .{ .boolean = try expandRle(u8, allocator, rb, row_count) }, .nulls = nulls },
        else => unreachable,
    };
}

fn expandRle(comptime T: type, allocator: Allocator, rb: RleBlock, row_count: u32) ![]T {
    const data = try allocator.alloc(T, row_count);
    errdefer allocator.free(data);
    var pos: usize = 0;
    var run: usize = 0;
    while (run < rb.n_runs) : (run += 1) {
        const v = std.mem.readInt(T, rb.values[run * @sizeOf(T) ..][0..@sizeOf(T)], .little);
        // Clamp against a corrupt length so expansion can never write past the
        // block's row count; a short fill surfaces as a value mismatch, not UB.
        const len = @min(@as(usize, rb.runLength(run)), row_count - pos);
        @memset(data[pos .. pos + len], v);
        pos += len;
        if (pos == row_count) break;
    }
    return data;
}

/// Build a BORROWED `ColumnView` over the raw decompressed block bytes —
/// zero allocation, the view aliases `raw` directly. Used by the scan-side
/// in-place filter fast path: the view is created over PINNED cache bytes,
/// used for predicate evaluation + survivor compaction, and dropped within a
/// single `next()` call. It must never be handed to a downstream operator.
///
/// Returns `null` when a typed view can't be formed safely:
///   - a `.for_` block: its bytes are narrow deltas, not native-width values,
///     so it has no in-place native view — the caller falls back to the
///     owned-decode (expand) path,
///   - a fixed-width element slice would be misaligned (`@alignCast` of
///     misaligned bytes is UB, so we decline and the caller falls back to the
///     owned-copy decode path),
///   - on a big-endian host (the borrowed view assumes the on-disk
///     little-endian layout is bit-identical to the in-memory `[]T`).
///
/// Only this `storage/` boundary performs the `@ptrCast`/`@alignCast`.
pub fn viewRawColumn(
    col_type: Type,
    raw: []const u8,
    row_count: u32,
    flags: format.ColumnBlockFlags,
    encoding: format.Encoding,
) ?ColumnView {
    if (native_endian != .little) return null;
    if (encoding != .raw) return null;

    var nulls: ?[]const u8 = null;
    var values = raw;
    if (flags.has_nulls) {
        const bm_len = column.bitmapBytes(row_count);
        nulls = raw[0..bm_len];
        values = raw[bm_len..];
    }

    switch (col_type) {
        .int => return fixedView(i32, values, row_count, nulls, .int),
        .bigint => return fixedView(i64, values, row_count, nulls, .bigint),
        .boolean => return .{ .data = .{ .boolean = values[0..row_count] }, .nulls = nulls },
        .float => return fixedView(f32, values, row_count, nulls, .float),
        .double => return fixedView(f64, values, row_count, nulls, .double),
        .date => return fixedView(i32, values, row_count, nulls, .date),
        .datetime => return fixedView(i64, values, row_count, nulls, .datetime),
        .tinyint => return .{ .data = .{ .tinyint = @ptrCast(values[0..row_count]) }, .nulls = nulls },
        .smallint => return fixedView(i16, values, row_count, nulls, .smallint),
        .largeint => return fixedView(i128, values, row_count, nulls, .largeint),
        .decimal64 => return fixedView(i64, values, row_count, nulls, .decimal64),
        .decimal128 => return fixedView(i128, values, row_count, nulls, .decimal128),
        .uuid => return fixedView(u128, values, row_count, nulls, .uuid),
        .varchar => return stringView(values, row_count, nulls, .varchar),
        .string => return stringView(values, row_count, nulls, .string),
        .char => return stringView(values, row_count, nulls, .char),
        .json => return stringView(values, row_count, nulls, .json),
    }
}

/// Reinterpret `values` as `[]const T` with a runtime alignment guard. Returns
/// null when the byte slice isn't aligned for `T` (caller falls back to copy).
fn fixedView(
    comptime T: type,
    values: []const u8,
    row_count: u32,
    nulls: ?[]const u8,
    comptime tag: std.meta.Tag(column.ValueView),
) ?ColumnView {
    if (@intFromPtr(values.ptr) % @alignOf(T) != 0) return null;
    const typed: []const T = @as([*]const T, @ptrCast(@alignCast(values.ptr)))[0..row_count];
    return .{ .data = @unionInit(column.ValueView, @tagName(tag), typed), .nulls = nulls };
}

/// Build a borrowed `StringView` over `values` laid out as
/// `[u32 byte_count][(n+1) u32 offsets][bytes]`. Returns null when the offset
/// array isn't u32-aligned in place.
fn stringView(
    values: []const u8,
    row_count: u32,
    nulls: ?[]const u8,
    comptime tag: std.meta.Tag(column.ValueView),
) ?ColumnView {
    const off_start = 4;
    const off_count = @as(usize, row_count) + 1;
    const off_bytes = off_count * 4;
    const offsets_ptr = values.ptr + off_start;
    if (@intFromPtr(offsets_ptr) % @alignOf(u32) != 0) return null;
    const offsets: []const u32 = @as([*]const u32, @ptrCast(@alignCast(offsets_ptr)))[0..off_count];
    const byte_count = offsets[row_count];
    const data_start = off_start + off_bytes;
    const bytes = values[data_start .. data_start + byte_count];
    const sv = StringView{ .offsets = offsets, .bytes = bytes };
    return .{ .data = @unionInit(column.ValueView, @tagName(tag), sv), .nulls = nulls };
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

/// A segment-local string dictionary block's regions, parsed in place over the
/// decompressed payload (past any validity bitmap). `dict_bytes` holds the `ndv`
/// distinct values concatenated in sorted order; `offsets_region` is the
/// `(ndv+1) × u32` rebased offset array (read via `format.readU32` so it tolerates
/// the payload's arbitrary alignment); `codes` is `row_count × code_width`. This
/// is the seam a later dictionary-aware execution layer consumes to operate on
/// codes directly (global-code stitch, predicate-per-distinct); the Phase 3
/// reader only uses it to rebuild the raw string shape.
pub const DictBlock = struct {
    ndv: u32,
    code_width: u8,
    offsets_region: []const u8,
    dict_bytes: []const u8,
    codes: []const u8,

    /// Byte slice of distinct value at sorted index `code` (aliases `dict_bytes`).
    pub fn dictValue(self: DictBlock, code: u32) []const u8 {
        const a = format.readU32(self.offsets_region[@as(usize, code) * 4 ..][0..4]);
        const b = format.readU32(self.offsets_region[(@as(usize, code) + 1) * 4 ..][0..4]);
        return self.dict_bytes[a..b];
    }

    /// Per-row code (little-endian, `code_width` bytes).
    pub fn rowCode(self: DictBlock, row: usize) u32 {
        var b4: [4]u8 = .{ 0, 0, 0, 0 };
        const off = row * self.code_width;
        @memcpy(b4[0..self.code_width], self.codes[off .. off + self.code_width]);
        return std.mem.readInt(u32, &b4, .little);
    }
};

/// Parse the dict header + regions out of a decompressed block payload. `values`
/// is the payload past the validity bitmap (the caller strips the bitmap the
/// same way the raw/FOR paths do). Layout documented on `format.Encoding.dict`.
pub fn dictBlockOf(values: []const u8, row_count: u32) DictBlock {
    const ndv = format.readU32(values[0..4]);
    const code_width = values[4];
    const off_start = 8;
    const off_bytes = (@as(usize, ndv) + 1) * 4;
    const offsets_region = values[off_start .. off_start + off_bytes];
    const dict_byte_count = format.readU32(values[off_start + @as(usize, ndv) * 4 ..][0..4]);
    const dict_start = off_start + off_bytes;
    const dict_bytes = values[dict_start .. dict_start + dict_byte_count];
    const codes_start = dict_start + dict_byte_count;
    const codes = values[codes_start .. codes_start + @as(usize, row_count) * code_width];
    return .{
        .ndv = ndv,
        .code_width = code_width,
        .offsets_region = offsets_region,
        .dict_bytes = dict_bytes,
        .codes = codes,
    };
}

/// Decode a dict-encoded string block into an OwnedColumn whose shape is
/// IDENTICAL to what a raw string block produces — every consumer is unchanged.
/// Reconstructs each row by indexing the sorted dict with the row's code. NULL
/// rows (masked by the validity bitmap) reconstruct their placeholder code's
/// value, which consumers ignore (same convention as the FOR path).
pub fn decodeDictColumn(
    allocator: Allocator,
    col_type: Type,
    raw: []const u8,
    row_count: u32,
    flags: format.ColumnBlockFlags,
) !OwnedColumn {
    const _pt = if (prof.enabled) prof.nowTicks() else 0;
    defer if (prof.enabled) prof.add("dict-decode (expand→strings)", @intCast(@max(0, prof.nowTicks() - _pt)));

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

    const db = dictBlockOf(values, row_count);
    const sc = try expandDict(allocator, db, row_count);

    // The writer only ever dict-encodes string-family columns.
    return switch (col_type) {
        .varchar => .{ .data = .{ .varchar = sc }, .nulls = nulls },
        .string => .{ .data = .{ .string = sc }, .nulls = nulls },
        .char => .{ .data = .{ .char = sc }, .nulls = nulls },
        .json => .{ .data = .{ .json = sc }, .nulls = nulls },
        else => unreachable,
    };
}

/// Rebuild the `[offsets][bytes]` owned string layout from dict codes. Two
/// passes: size the per-row offsets, then copy each row's dict value in.
fn expandDict(allocator: Allocator, db: DictBlock, row_count: u32) !OwnedStringColumn {
    const offsets = try allocator.alloc(u32, @as(usize, row_count) + 1);
    errdefer allocator.free(offsets);

    var total: usize = 0;
    offsets[0] = 0;
    var row: usize = 0;
    while (row < row_count) : (row += 1) {
        total += db.dictValue(db.rowCode(row)).len;
        offsets[row + 1] = @intCast(total);
    }

    const bytes = try allocator.alloc(u8, total);
    errdefer allocator.free(bytes);
    row = 0;
    while (row < row_count) : (row += 1) {
        const v = db.dictValue(db.rowCode(row));
        @memcpy(bytes[offsets[row]..offsets[row + 1]], v);
    }

    return .{ .offsets = offsets, .bytes = bytes };
}

/// An FSST block's regions, parsed in place over the decompressed payload
/// (past any validity bitmap). The symbol table is rebuilt by value (it is a
/// few KB of plain arrays); the per-row compressed slices alias the payload.
/// This is the seam compressed-domain consumers use: digest-while-decode via
/// `table.decodeStream(rowComp(r), sink)`, per-segment equality via
/// memcmp over `rowComp`, and one-row decode for late materialization.
pub const FsstBlock = struct {
    table: fsst.SymbolTable,
    raw_byte_count: u32,
    /// (row_count+1) × u32 little-endian, rebased to 0.
    offsets_region: []const u8,
    comp_bytes: []const u8,

    pub fn rowComp(self: *const FsstBlock, row: usize) []const u8 {
        const a = format.readU32(self.offsets_region[row * 4 ..][0..4]);
        const b = format.readU32(self.offsets_region[(row + 1) * 4 ..][0..4]);
        return self.comp_bytes[a..b];
    }
};

/// Parse the FSST regions out of a decompressed block payload. `values` is the
/// payload past the validity bitmap. Layout documented on
/// `format.Encoding.fsst`. Errors on a corrupt symbol table rather than
/// risking out-of-bounds symbol lengths.
pub fn fsstBlockOf(values: []const u8, row_count: u32) !FsstBlock {
    const table_len = format.readU32(values[0..4]);
    const table = try fsst.SymbolTable.deserialize(values[4 .. 4 + table_len]);
    var cur: usize = 4 + table_len;
    const raw_byte_count = format.readU32(values[cur..][0..4]);
    cur += 4;
    const comp_byte_count = format.readU32(values[cur..][0..4]);
    cur += 4;
    const off_bytes = (@as(usize, row_count) + 1) * 4;
    const offsets_region = values[cur .. cur + off_bytes];
    cur += off_bytes;
    return .{
        .table = table,
        .raw_byte_count = raw_byte_count,
        .offsets_region = offsets_region,
        .comp_bytes = values[cur .. cur + comp_byte_count],
    };
}

/// Split a decompressed FSST block payload into (validity bitmap, FsstBlock),
/// mirroring `rleViewOf` / `forViewOf`.
pub const FsstView = struct {
    nulls: ?[]const u8,
    block: FsstBlock,
};

pub fn fsstViewOf(raw: []const u8, row_count: u32, flags: format.ColumnBlockFlags) !FsstView {
    var nulls: ?[]const u8 = null;
    var values = raw;
    if (flags.has_nulls) {
        const bm_len = column.bitmapBytes(row_count);
        nulls = raw[0..bm_len];
        values = raw[bm_len..];
    }
    return .{ .nulls = nulls, .block = try fsstBlockOf(values, row_count) };
}

/// Expand a borrowed FSST block into the cache's recycled scratch pool —
/// `[offsets (row_count+1 × u32)][bytes]` in one buffer — and return a
/// borrowed string view over it. A fresh allocation per borrow re-faults
/// freshly-zeroed pages on every scan `next()` (the same cost class the
/// LZ4-at-rest scratch pool eliminated). The scratch rides `block.owned` /
/// `block.pooled` and returns to the pool on `block.release`; the cache pin
/// stays held (the nulls bitmap aliases the entry's bytes).
pub fn expandFsstPooled(
    block: *ReadSegment.BorrowedBlock,
    cc: *storage_cache.Cache,
    col_type: Type,
    row_count: u32,
    flags: format.ColumnBlockFlags,
) !ColumnView {
    const fv = try fsstViewOf(block.bytes, row_count, flags);
    const n: usize = row_count;
    const offsets_bytes = (n + 1) * @sizeOf(u32);
    const scratch = try cc.acquireScratch(offsets_bytes + fv.block.raw_byte_count);
    errdefer cc.releaseScratch(scratch);

    const offsets: []u32 = @alignCast(std.mem.bytesAsSlice(u32, scratch[0..offsets_bytes]));
    const bytes = scratch[offsets_bytes..][0..fv.block.raw_byte_count];
    var o: usize = 0;
    offsets[0] = 0;
    for (0..n) |r| {
        // Prefetch a few rows ahead: row starts hop unpredictably enough that
        // the hardware prefetcher misses them (+~10% measured in the micro).
        if (r + 8 < n) @prefetch(fv.block.rowComp(r + 8).ptr, .{ .rw = .read, .locality = 2 });
        o += fv.block.table.decodeInto(fv.block.rowComp(r), bytes[o..]);
        offsets[r + 1] = @intCast(o);
    }
    if (o != fv.block.raw_byte_count) return format.Error.CorruptColumnBlockHeader;

    block.owned = scratch;
    block.pooled = true;
    const sc = column.StringView{ .offsets = offsets, .bytes = bytes };
    return switch (col_type) {
        .varchar => .{ .data = .{ .varchar = sc }, .nulls = fv.nulls },
        .string => .{ .data = .{ .string = sc }, .nulls = fv.nulls },
        .char => .{ .data = .{ .char = sc }, .nulls = fv.nulls },
        .json => .{ .data = .{ .json = sc }, .nulls = fv.nulls },
        else => format.Error.CorruptColumnBlockHeader,
    };
}

/// Expand an FSST block into the owned `[offsets][bytes]` string shape every
/// consumer expects — identical output to a raw string block.
pub fn decodeFsstColumn(
    allocator: Allocator,
    col_type: Type,
    raw: []const u8,
    row_count: u32,
    flags: format.ColumnBlockFlags,
) !OwnedColumn {
    const _pt = if (prof.enabled) prof.nowTicks() else 0;
    defer if (prof.enabled) prof.add("fsst-decode (expand→strings)", @intCast(@max(0, prof.nowTicks() - _pt)));

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

    const fb = try fsstBlockOf(values, row_count);

    const offsets = try allocator.alloc(u32, @as(usize, row_count) + 1);
    errdefer allocator.free(offsets);
    const bytes = try allocator.alloc(u8, fb.raw_byte_count);
    errdefer allocator.free(bytes);

    var o: usize = 0;
    offsets[0] = 0;
    var row: usize = 0;
    while (row < row_count) : (row += 1) {
        if (row + 8 < row_count) @prefetch(fb.rowComp(row + 8).ptr, .{ .rw = .read, .locality = 2 });
        o += fb.table.decodeInto(fb.rowComp(row), bytes[o..]);
        offsets[row + 1] = @intCast(o);
    }
    if (o != fb.raw_byte_count) return format.Error.CorruptColumnBlockHeader;

    const sc = OwnedStringColumn{ .offsets = offsets, .bytes = bytes };
    // The writer only ever FSST-encodes string-family columns.
    return switch (col_type) {
        .varchar => .{ .data = .{ .varchar = sc }, .nulls = nulls },
        .string => .{ .data = .{ .string = sc }, .nulls = nulls },
        .char => .{ .data = .{ .char = sc }, .nulls = nulls },
        .json => .{ .data = .{ .json = sc }, .nulls = nulls },
        else => unreachable,
    };
}
