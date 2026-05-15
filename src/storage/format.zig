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
/// v3: each column block is prefixed by a small header recording compression
/// kind (none vs flate) + compressed/uncompressed sizes. v2 segments aren't
/// readable by v3 code.
pub const segment_version: u16 = 3;

/// Column-block compression algorithm. Stored as a u8 in each block's header.
pub const Compression = enum(u8) {
    none = 0,
    flate = 1,
};

/// Header preceding each column block in v3 segments. Always emitted, even
/// for `Compression.none`, so the reader can skip blocks of any kind.
pub const ColumnBlockHeader = struct {
    compression: Compression,
    uncompressed_size: u32,
    compressed_size: u32,
};

/// Size of the in-file column-block header: u8 + u32 + u32 + 3 padding bytes
/// for 4-byte alignment.
pub const column_block_header_size: usize = 12;
pub const header_size: usize = 32;
pub const row_group_header_size: usize = 8;
pub const footer_trailer_size: usize = 8; // u32 footer_size + 4-byte magic

/// Min/max for fixed-width columns. Stored as i64 (sign-extended) so the
/// footer entry size is the same for INT, BIGINT, and BOOLEAN. Strings carry
/// no stats in v0.2.
pub const Stats = struct {
    min: i64,
    max: i64,
};

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
    row_groups: []const RowGroupMeta,

    pub fn deinit(self: SegmentInfo, allocator: std.mem.Allocator) void {
        for (self.row_groups) |rg| allocator.free(rg.stats);
        allocator.free(self.row_groups);
    }
};

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
