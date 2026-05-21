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
/// v7: per-column stats slot widens from 16 bytes (i64 min + i64 max)
/// to 32 bytes (i128 min + i128 max). Unlocks usable stats for
/// largeint / decimal128 / uuid (whose full range needs 16 bytes per
/// value) and extends the string prefix from 8 bytes to 16. All
/// stats-bearing types now share a uniform i128-based representation.
pub const segment_version: u16 = 7;

/// Column-block compression algorithm. Stored as a u8 in each block's header.
pub const Compression = enum(u8) {
    none = 0,
    zstd = 1,
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
    uncompressed_size: u32,
    compressed_size: u32,
};

/// Size of the in-file column-block header:
///   u8 compression + u8 flags + 2 reserved + u32 uncompressed_size + u32 compressed_size.
pub const column_block_header_size: usize = 12;
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
///   - float / double: `{0, 0}` sentinel (NaN/sign handling deferred)
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
    /// One entry per schema column. For string columns the entry is
    /// `{ min: 0, max: 0 }` (ignored).
    stats: []const Stats,
};

/// Hash-vs-sort GROUP BY cutoff: a group key whose estimated distinct
/// count (or product, for compound keys) is below this fits the hash
/// table, so we hash; otherwise we sort. Conservative on purpose. Not a
/// memory cliff — 8000 groups fit trivially in the budget — so the HLL
/// estimate's error near this value is harmless.
pub const cardinality_limit: u32 = 8000;

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
        for (self.row_groups) |rg| allocator.free(rg.stats);
        allocator.free(self.row_groups);
        if (self.column_sketches.len > 0) allocator.free(self.column_sketches);
    }
};

/// True iff a column of this type carries meaningful min/max in the
/// per-row-group `Stats` slot (and in the manifest entry). Stats are
/// always stored as i128 with a per-type encoding (see `Stats`).
/// Types not listed here store `{0, 0}` and must be treated as "no
/// stats" by pruning code.
pub fn typeHasStats(t: @import("../types.zig").Type) bool {
    return switch (t) {
        .int, .bigint, .smallint, .tinyint, .boolean, .date, .datetime, .decimal64 => true,
        .largeint, .decimal128, .uuid => true,
        .varchar, .string, .char => true,
        .float, .double => false,
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
