//! On-disk segment file format constants and metadata structs.
//!
//! Layout (v0.1, plain encoding, no compression):
//!
//!   [Header — 32 bytes]
//!     magic "tDBS"            (4)
//!     version u16             (2)
//!     flags u16               (2 — reserved, written 0)
//!     schema_fingerprint u64  (8)
//!     segment_id u64          (8)
//!     row_count u64           (8)
//!
//!   [Row groups, sequential]
//!     For each row group:
//!       row_count u32         (4)
//!       _padding u32          (4 — reserved, written 0)
//!       For each column in schema order:
//!         column block content (see column.zig)
//!
//!   [Footer]
//!     row_group_count u32     (4)
//!     For each row group:
//!       offset_from_start u64 (8)
//!       length u32            (4)
//!       row_count u32         (4)
//!     footer_size u32         (4 — total footer bytes incl. itself + trailing magic)
//!     magic "tDBS"            (4)

const std = @import("std");

pub const segment_magic: [4]u8 = .{ 't', 'D', 'B', 'S' };
/// v9: per-column-block `encoding` byte (one of the two header reserved bytes)
/// selects `raw` or `for_` (Frame-of-Reference). v8 segments wrote both reserved
/// bytes as 0, which decodes as `.raw`, so v8 and v9 raw blocks are byte-identical
/// on disk — the only on-disk difference is a `.for_` block, which only a v9
/// writer emits. The header size is unchanged.
///
/// v7→v8: per-column stats slot widened from 16 bytes (i64 min + i64 max) to 32
/// bytes (i128 min + i128 max). Unlocked usable stats for largeint / decimal128 /
/// uuid and extended the string prefix from 8 to 16 bytes.
///
/// v9→v10: added the `.rle` block encoding.
pub const segment_version: u16 = 10;

/// Column-block compression algorithm. Stored as a u8 in each block's header.
pub const Compression = enum(u8) {
    none = 0,
    zstd = 1,
};

/// Per-column-block value encoding, stored in the first of the header's two
/// reserved bytes. Orthogonal to `Compression`: a block is first encoded
/// (raw vs FOR), then the encoded bytes are optionally zstd-compressed.
///
/// `.for_` (Frame-of-Reference) stores a fixed-width integer column as a single
/// `base` plus per-row narrow unsigned deltas `value - base`, where `base` is the
/// block's minimum non-null value and the delta width is the smallest of
/// {1,2,4,8} bytes that holds `max - base`. Decoding reconstructs
/// `value = base + delta`. See the FOR payload layout in `segment_writer.zig`.
///
/// `.dict` (segment-local string dictionary) stores a low-cardinality string
/// column as the `k` distinct values once (sorted) plus a narrow per-row `code`
/// (index into the sorted dict). Decoding reconstructs each row by indexing the
/// dict. The post-bitmap payload layout is:
///
///   [ndv: u32]                      number of distinct values `k`
///   [code_width: u8][3 pad]         1 / 2 / 4 bytes, chosen by `k`
///   [dict_offsets: (k+1) × u32]     byte offsets into `dict_bytes`, rebased to 0
///   [dict_bytes: …]                 the `k` distinct values, concatenated, SORTED
///   [codes: row_count × code_width] per-row index into the sorted dict
///
/// The dict is stored sorted so a later execution layer can binary-search it and
/// derive ORDER BY from codes. NULL rows carry a placeholder code, masked by the
/// validity bitmap (orthogonal, same convention as `.raw`/`.for_`).
///
/// `.rle` (run-length) stores a fixed-width integer-family column as runs of
/// equal adjacent values. Chosen by the writer only when the run body beats
/// both raw and FOR for the block, so it only appears on genuinely clustered
/// data (sort keys, session-adjacent ids, flag columns). The post-bitmap
/// payload layout is:
///
///   [n_runs: u32]
///   [value_width: u8][3 pad]          native element width (1/2/4/8 bytes)
///   [values: n_runs × value_width]    each run's value, native LE
///   [lengths: n_runs × u32]           each run's row count
///
/// Runs are computed over the stored value stream as-is (NULL placeholder
/// values included), so decode reproduces the exact stream and the validity
/// bitmap stays orthogonal. The split values/lengths arrays keep both
/// SIMD-friendly for run-aware kernels that scan without expanding.
pub const Encoding = enum(u8) {
    raw = 0,
    for_ = 1,
    dict = 2,
    rle = 3,
};

/// Per-column-block flags (u8). Bit 0 = has_nulls (decompressed payload is
/// prefixed by a validity bitmap). Remaining bits reserved.
pub const ColumnBlockFlags = packed struct(u8) {
    has_nulls: bool = false,
    _reserved: u7 = 0,

    pub fn toByte(self: ColumnBlockFlags) u8 {
        return @bitCast(self);
    }
    pub fn fromByte(b: u8) ColumnBlockFlags {
        return @bitCast(b);
    }
};

/// Header preceding each column block in v4 segments. Always emitted, even
/// for `Compression.none`, so the reader can skip blocks of any kind.
pub const ColumnBlockHeader = struct {
    compression: Compression,
    flags: ColumnBlockFlags,
    encoding: Encoding,
    uncompressed_size: u32,
    compressed_size: u32,
};

/// Size of the in-file column-block header:
///   u8 compression + u8 flags + u8 encoding + 1 reserved + u32 uncompressed_size + u32 compressed_size.
/// (`encoding` occupies the first of v8's two reserved bytes; one reserved byte
/// remains. The size is unchanged from v8, preserving on-disk back-compat.)
pub const column_block_header_size: usize = 12;
/// Byte offset of the `encoding` field within a column-block header.
pub const column_block_encoding_offset: usize = 2;
pub const header_size: usize = 32;
pub const row_group_header_size: usize = 8;
pub const footer_trailer_size: usize = 8; // u32 footer_size + 4-byte magic

/// Per-column min/max in the segment row-group footer. Each value is
/// 16 bytes on disk (two `i128` LE), interpreted per column type:
///   - integer / boolean / date / datetime / decimal64: sign-extended
///     (or zero-extended for boolean) to i128
///   - largeint, decimal128: direct i128
///   - uuid (u128): XOR top bit then cast to i128 — preserves unsigned
///     ordering under signed comparison
///   - varchar / string / char: prefix encoding (see `encodeStringPrefix`)
///   - float / double: order-preserving bit transform (see `encodeFloatOrder`);
///     NaN is skipped when computing the extent
pub const Stats = struct {
    min: i128,
    max: i128,
};

/// Encode a 16-byte prefix of `bytes` (zero-padded if shorter) into an
/// i128 such that signed i128 comparison preserves lex byte order over
/// the prefix. Same trick as the 8-byte version: read big-endian into
/// u128, XOR the top bit to flip unsigned→signed ordering.
///
/// Strings longer than 16 bytes contribute only their first 16 bytes;
/// callers must treat prefix-tied keys as "could match" (no false
/// negatives, only false positives at the row-group/segment level).
pub fn encodeStringPrefix(bytes: []const u8) i128 {
    var buf: [16]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    const n = @min(bytes.len, 16);
    @memcpy(buf[0..n], bytes[0..n]);
    const u = std.mem.readInt(u128, &buf, .big);
    return @bitCast(u ^ (@as(u128, 1) << 127));
}

/// Encode a u128 (used for uuid) so signed i128 comparison preserves
/// unsigned ordering. Same top-bit XOR pattern as the string encoding.
pub fn encodeUnsignedU128(v: u128) i128 {
    return @bitCast(v ^ (@as(u128, 1) << 127));
}

/// Encode an f64 into an i128 such that signed i128 comparison preserves IEEE-754
/// numeric order for every finite value and ±inf. The standard total-order trick:
/// reinterpret the bits as u64, then if the sign bit is set (negative) flip all
/// bits, else flip just the sign bit — making the result monotonic in float order.
/// The u64 key lands in `[0, 2^64)`, so the i128 is always non-negative and signed
/// comparison matches. NaN is never passed here — `computeStats` skips it.
pub fn encodeFloatOrder(f: f64) i128 {
    // NaN sorts last (max key) to match `types.floatOrder`: +inf maps to
    // 0xFFF0… while maxInt(u64) is strictly above it, so NaN > every value.
    if (std.math.isNan(f)) return @as(i128, std.math.maxInt(u64));
    const bits: u64 = @bitCast(f);
    const key: u64 = if (bits >> 63 != 0) ~bits else bits | (@as(u64, 1) << 63);
    return @as(i128, key);
}

pub const Error = error{
    SchemaMismatch,
    UnevenColumns,
    InvalidRowGroupSize,
    SegmentTooSmall,
    BadMagic,
    BadFooterMagic,
    UnsupportedVersion,
    CorruptFooter,
    CorruptColumnBlockHeader,
    UnknownCompression,
    UnexpectedEof,
};

pub const RowGroupMeta = struct {
    offset: u64,
    length: u32,
    row_count: u32,
    /// One entry per schema column, min/max in the per-type i128 encoding
    /// (see `Stats`). Every column type carries usable stats.
    stats: []const Stats,
    /// Absolute file offset of each column's block within this row group, one
    /// per schema column (column-index order). Lets the reader pread just the
    /// columns a query needs instead of the whole segment file. A block's
    /// length is the next column's offset minus this one (the last column ends
    /// at `offset + length`). Empty on the writer-built `SegmentInfo` (only the
    /// footer carries them); populated by `readSegment`.
    col_offsets: []const u64 = &.{},
};

pub const SegmentInfo = struct {
    segment_id: u64,
    row_count: u64,
    schema_fingerprint: u64,
    /// Size of the on-disk `.dat` file in bytes. Populated by both
    /// `writeSegment` (from the buffered output length) and
    /// `readSegment` (from the input slice length).
    byte_size: u64,
    row_groups: []const RowGroupMeta,
    /// Per-column HyperLogLog sketches, concatenated: column `ci` occupies
    /// bytes `[ci*hll.m .. (ci+1)*hll.m]`. Mergeable across segments. Empty
    /// when the writer didn't compute it.
    column_sketches: []const u8 = &.{},

    pub fn deinit(self: SegmentInfo, allocator: std.mem.Allocator) void {
        for (self.row_groups) |rg| {
            allocator.free(rg.stats);
            if (rg.col_offsets.len > 0) allocator.free(rg.col_offsets);
        }
        allocator.free(self.row_groups);
        if (self.column_sketches.len > 0) allocator.free(self.column_sketches);
    }
};

/// True iff a column of this type carries meaningful min/max in the
/// per-row-group `Stats` slot (and in the manifest entry). Stats are
/// always stored as i128 with a per-type encoding (see `Stats`). Every
/// column type carries stats now (float/double via `encodeFloatOrder`,
/// NaN skipped at write time).
pub fn typeHasStats(t: @import("../types.zig").Type) bool {
    return switch (t) {
        .int, .bigint, .smallint, .tinyint, .boolean, .date, .datetime, .decimal64 => true,
        .largeint, .decimal128, .uuid => true,
        .varchar, .string, .char => true,
        .float, .double => true,
    };
}

// ---------- byte helpers -------------------------------------------------

pub fn writeU16(buf: []u8, v: u16) void {
    std.mem.writeInt(u16, buf[0..2], v, .little);
}
pub fn writeU32(buf: []u8, v: u32) void {
    std.mem.writeInt(u32, buf[0..4], v, .little);
}
pub fn writeU64(buf: []u8, v: u64) void {
    std.mem.writeInt(u64, buf[0..8], v, .little);
}
pub fn writeI32(buf: []u8, v: i32) void {
    std.mem.writeInt(i32, buf[0..4], v, .little);
}
pub fn writeI64(buf: []u8, v: i64) void {
    std.mem.writeInt(i64, buf[0..8], v, .little);
}
pub fn writeI128(buf: []u8, v: i128) void {
    std.mem.writeInt(i128, buf[0..16], v, .little);
}
pub fn writeF32(buf: []u8, v: f32) void {
    std.mem.writeInt(u32, buf[0..4], @bitCast(v), .little);
}
pub fn writeF64(buf: []u8, v: f64) void {
    std.mem.writeInt(u64, buf[0..8], @bitCast(v), .little);
}

pub fn readU16(buf: []const u8) u16 {
    return std.mem.readInt(u16, buf[0..2], .little);
}
pub fn readU32(buf: []const u8) u32 {
    return std.mem.readInt(u32, buf[0..4], .little);
}
pub fn readU64(buf: []const u8) u64 {
    return std.mem.readInt(u64, buf[0..8], .little);
}
pub fn readI32(buf: []const u8) i32 {
    return std.mem.readInt(i32, buf[0..4], .little);
}
pub fn readI64(buf: []const u8) i64 {
    return std.mem.readInt(i64, buf[0..8], .little);
}
pub fn readI128(buf: []const u8) i128 {
    return std.mem.readInt(i128, buf[0..16], .little);
}
pub fn readF32(buf: []const u8) f32 {
    return @bitCast(std.mem.readInt(u32, buf[0..4], .little));
}
pub fn readF64(buf: []const u8) f64 {
    return @bitCast(std.mem.readInt(u64, buf[0..8], .little));
}

// ---------- ArrayList(u8) append helpers ----------

const Allocator = std.mem.Allocator;

pub fn appendU16(allocator: Allocator, buf: *std.ArrayList(u8), v: u16) !void {
    var b: [2]u8 = undefined;
    writeU16(&b, v);
    try buf.appendSlice(allocator, &b);
}
pub fn appendU32(allocator: Allocator, buf: *std.ArrayList(u8), v: u32) !void {
    var b: [4]u8 = undefined;
    writeU32(&b, v);
    try buf.appendSlice(allocator, &b);
}
pub fn appendU64(allocator: Allocator, buf: *std.ArrayList(u8), v: u64) !void {
    var b: [8]u8 = undefined;
    writeU64(&b, v);
    try buf.appendSlice(allocator, &b);
}
pub fn appendI32(allocator: Allocator, buf: *std.ArrayList(u8), v: i32) !void {
    var b: [4]u8 = undefined;
    writeI32(&b, v);
    try buf.appendSlice(allocator, &b);
}
pub fn appendI64(allocator: Allocator, buf: *std.ArrayList(u8), v: i64) !void {
    var b: [8]u8 = undefined;
    writeI64(&b, v);
    try buf.appendSlice(allocator, &b);
}
pub fn appendI128(allocator: Allocator, buf: *std.ArrayList(u8), v: i128) !void {
    var b: [16]u8 = undefined;
    writeI128(&b, v);
    try buf.appendSlice(allocator, &b);
}

test "encodeStringPrefix preserves lex byte order across signed i128" {
    const cases = [_]struct { a: []const u8, b: []const u8 }{
        .{ .a = "alice", .b = "bob" },
        .{ .a = "alice", .b = "alicez" },
        .{ .a = "a", .b = "alice" },
        .{ .a = "", .b = "a" },
        // High-bit-set first byte (would be negative under naive signed
        // cast) still orders above ASCII.
        .{ .a = "alice", .b = &[_]u8{ 0x80, 0, 0, 0, 0, 0, 0, 0 } },
        // Strings sharing the first 8 but differing within byte 8..16
        // now correctly compare (16-byte prefix vs 8-byte).
        .{ .a = "abcdefgh_extra1", .b = "abcdefgh_extra2" },
    };
    for (cases) |c| {
        const ea = encodeStringPrefix(c.a);
        const eb = encodeStringPrefix(c.b);
        try std.testing.expect(ea < eb);
    }

    // Tied prefixes (>16 byte strings sharing first 16 bytes) encode equal.
    try std.testing.expectEqual(
        encodeStringPrefix("abcdefghijklmnop_extra1"),
        encodeStringPrefix("abcdefghijklmnop_extra2"),
    );
    try std.testing.expectEqual(@as(i128, std.math.minInt(i128)), encodeStringPrefix(""));
}

test "encodeFloatOrder is monotonic across the f64 range" {
    // Ascending by numeric value; encoded keys must be strictly increasing.
    const ordered = [_]f64{
        -std.math.inf(f64), -1e308, -1.5, -1.0, -0.5, -1e-308, -0.0,
        0.0,               1e-308, 0.5,  1.0,  1.5,  1e308,    std.math.inf(f64),
    };
    var prev = encodeFloatOrder(ordered[0]);
    for (ordered[1..]) |f| {
        const cur = encodeFloatOrder(f);
        // -0.0 and +0.0 are adjacent (encode to consecutive keys); every other
        // step is a strict increase. `<=` covers the ±0 tie soundly for bounds.
        try std.testing.expect(prev <= cur);
        prev = cur;
    }
    // Distinct finite values are strictly ordered.
    try std.testing.expect(encodeFloatOrder(1.0) < encodeFloatOrder(2.0));
    try std.testing.expect(encodeFloatOrder(-2.0) < encodeFloatOrder(-1.0));
    try std.testing.expect(encodeFloatOrder(-1.0) < encodeFloatOrder(1.0));
    // NaN sorts strictly above every value, including +inf.
    try std.testing.expect(encodeFloatOrder(std.math.nan(f64)) > encodeFloatOrder(std.math.inf(f64)));
}

test "byte helpers round-trip primitive types" {
    var buf: [8]u8 = undefined;

    writeU16(&buf, 0xBEEF);
    try std.testing.expectEqual(@as(u16, 0xBEEF), readU16(&buf));

    writeU32(&buf, 0xDEADBEEF);
    try std.testing.expectEqual(@as(u32, 0xDEADBEEF), readU32(&buf));

    writeU64(&buf, 0x0123_4567_89AB_CDEF);
    try std.testing.expectEqual(@as(u64, 0x0123_4567_89AB_CDEF), readU64(&buf));

    writeI32(&buf, -123456);
    try std.testing.expectEqual(@as(i32, -123456), readI32(&buf));

    writeI64(&buf, -9_876_543_210);
    try std.testing.expectEqual(@as(i64, -9_876_543_210), readI64(&buf));
}
