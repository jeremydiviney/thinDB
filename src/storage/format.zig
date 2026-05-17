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
/// v6: per-row-group `Stats` slot for string/varchar/char columns now
/// carries the prefix-encoded min/max of the column's first 8 bytes
/// (was `{0, 0}` in v5). On-disk layout is otherwise identical to v5,
/// but a v5 reader would interpret v6's string-stat bytes as garbage
/// i64 values — hence the version bump. See `encodeStringPrefix`.
pub const segment_version: u16 = 6;

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

/// Per-column min/max in the segment row-group footer. The 16-byte slot
/// (two i64) is reinterpreted per column type:
///   - integer / boolean / date / datetime / decimal64: sign-extended i64
///   - varchar / string / char: prefix encoding (see `encodeStringPrefix`)
///   - largeint / decimal128 / uuid / float / double: `{0, 0}` sentinel
///     (no usable stats — these types carry no min/max today)
pub const Stats = struct {
    min: i64,
    max: i64,
};

/// Encode an 8-byte prefix of `bytes` (zero-padded if shorter) into an
/// i64 such that signed i64 comparison preserves lex byte order over
/// the prefix:
///   1. Take the first up-to-8 bytes, zero-pad on the right.
///   2. Read as u64 big-endian → first byte becomes the MSB, so unsigned
///      integer ordering matches lex byte ordering.
///   3. XOR the top bit to flip the unsigned/signed ordering — turns
///      values with high-bit-set bytes (negative in i64) into the upper
///      half of the i64 range instead of the lower half.
///
/// Strings longer than 8 bytes contribute only their first 8 bytes; the
/// resulting min/max is a CONSERVATIVE upper bound (a row's actual full
/// value may differ from another row sharing the same 8-byte prefix).
/// Callers that prune on these stats must treat ties as "could match".
pub fn encodeStringPrefix(bytes: []const u8) i64 {
    var buf: [8]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 0 };
    const n = @min(bytes.len, 8);
    @memcpy(buf[0..n], bytes[0..n]);
    const u = std.mem.readInt(u64, &buf, .big);
    return @bitCast(u ^ (@as(u64, 1) << 63));
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

pub const SegmentInfo = struct {
    segment_id: u64,
    row_count: u64,
    schema_fingerprint: u64,
    /// Size of the on-disk `.dat` file in bytes. Populated by both
    /// `writeSegment` (from the buffered output length) and
    /// `readSegment` (from the input slice length).
    byte_size: u64,
    row_groups: []const RowGroupMeta,

    pub fn deinit(self: SegmentInfo, allocator: std.mem.Allocator) void {
        for (self.row_groups) |rg| allocator.free(rg.stats);
        allocator.free(self.row_groups);
    }
};

/// True iff a column of this type carries meaningful min/max in the
/// per-row-group `Stats` slot (and in the manifest leading-key slot).
/// Numeric types use sign-extended i64; string types use the prefix
/// encoding (`encodeStringPrefix`). Types not listed here store
/// `{0, 0}` and must be treated as "no stats" by pruning code.
pub fn typeHasI64Stats(t: @import("../types.zig").Type) bool {
    return switch (t) {
        .int, .bigint, .smallint, .tinyint, .boolean, .date, .datetime, .decimal64 => true,
        .varchar, .string, .char => true,
        .largeint, .decimal128, .uuid, .float, .double => false,
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

test "encodeStringPrefix preserves lex byte order across signed i64" {
    const cases = [_]struct { a: []const u8, b: []const u8 }{
        // Plain ASCII pairs.
        .{ .a = "alice", .b = "bob" },
        .{ .a = "alice", .b = "alicez" },
        .{ .a = "a", .b = "alice" },
        // Empty < anything non-empty.
        .{ .a = "", .b = "a" },
        // Strings whose first byte has the high bit set (would be negative
        // if we naively reinterpreted as signed i64) must still order above
        // ASCII strings. 0x80 > 'z' (0x7A).
        .{ .a = "alice", .b = &[_]u8{ 0x80, 0, 0, 0, 0, 0, 0, 0 } },
        // Strings sharing the full 8-byte prefix but differing afterward
        // encode to the same i64 — callers must keep these conservatively.
        // Verified separately below via the equality check.
    };
    for (cases) |c| {
        const ea = encodeStringPrefix(c.a);
        const eb = encodeStringPrefix(c.b);
        try std.testing.expect(ea < eb);
    }

    // Tied prefixes (>8 byte strings sharing first 8 bytes) encode equal.
    try std.testing.expectEqual(
        encodeStringPrefix("abcdefgh_extra1"),
        encodeStringPrefix("abcdefgh_extra2"),
    );
    // Empty string encodes to the minimum (all-zero bytes, then top-bit
    // XOR → minInt(i64)) — every non-empty string sorts above it.
    try std.testing.expectEqual(@as(i64, std.math.minInt(i64)), encodeStringPrefix(""));
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
