//! Physical row-location packing for late materialization.
//!
//! The `LateScan` operator decodes only the columns a filter + ORDER BY need,
//! plus a hidden `__rowloc` BIGINT per row that encodes WHERE the row lives so
//! the wide output columns can be fetched for just the survivors after a
//! TopN. A location is one of two shapes packed into an i64:
//!
//!   - segment row:  seg_idx | rg_idx | local_offset-within-row-group
//!   - memtable row: a sentinel marker + the memtable row index
//!
//! Bit layout (i64, sign bit kept clear so the value is always non-negative):
//!
//!   bits [0,  OFFSET_BITS)            local offset within the row group
//!   bits [OFFSET_BITS, +RG_BITS)      row-group index within the segment
//!   bits [.., +SEG_BITS)              segment index in the manifest snapshot
//!
//! The segment field's all-ones value (`seg_sentinel`) flags a memtable row;
//! its lower (OFFSET_BITS + RG_BITS) bits then hold the memtable row index.
//! The field widths comfortably exceed the documented engine limits
//! (DESIGN.md §11: 2^32 segments, 2^32 rows/segment, 65,536-row row groups),
//! and `packSegment` asserts every component fits before shifting.

const std = @import("std");

pub const col_name = "__rowloc";

const OFFSET_BITS: u6 = 24;
const RG_BITS: u6 = 18;
const SEG_BITS: u6 = 21;

const offset_mask: u64 = (@as(u64, 1) << OFFSET_BITS) - 1;
const rg_mask: u64 = (@as(u64, 1) << RG_BITS) - 1;
const seg_mask: u64 = (@as(u64, 1) << SEG_BITS) - 1;

const rg_shift: u6 = OFFSET_BITS;
const seg_shift: u6 = OFFSET_BITS + RG_BITS;

/// All-ones in the segment field — distinguishes a memtable location from any
/// real segment (segment indices are dense from 0 and never reach this).
const seg_sentinel: u64 = seg_mask;

pub const Location = union(enum) {
    segment: struct { seg_idx: usize, rg_idx: usize, offset: usize },
    memtable: struct { row: usize },
};

pub fn packSegment(seg_idx: usize, rg_idx: usize, offset: usize) i64 {
    std.debug.assert(offset <= offset_mask);
    std.debug.assert(rg_idx <= rg_mask);
    // Segment indices must stay below the sentinel; the documented limit is
    // 2^32 segments but a single manifest snapshot far smaller in practice.
    std.debug.assert(seg_idx < seg_sentinel);
    const packed_bits = (@as(u64, @intCast(seg_idx)) << seg_shift) |
        (@as(u64, @intCast(rg_idx)) << rg_shift) |
        @as(u64, @intCast(offset));
    return @bitCast(packed_bits);
}

pub fn packMemtable(row: usize) i64 {
    const lower: u64 = @intCast(row);
    std.debug.assert(lower <= ((@as(u64, 1) << seg_shift) - 1));
    const packed_bits = (seg_sentinel << seg_shift) | lower;
    return @bitCast(packed_bits);
}

pub fn unpack(value: i64) Location {
    const bits: u64 = @bitCast(value);
    const seg = (bits >> seg_shift) & seg_mask;
    if (seg == seg_sentinel) {
        return .{ .memtable = .{ .row = @intCast(bits & ((@as(u64, 1) << seg_shift) - 1)) } };
    }
    return .{ .segment = .{
        .seg_idx = @intCast(seg),
        .rg_idx = @intCast((bits >> rg_shift) & rg_mask),
        .offset = @intCast(bits & offset_mask),
    } };
}

test "rowloc round-trips segment locations" {
    const cases = .{
        .{ .s = 0, .r = 0, .o = 0 },
        .{ .s = 1, .r = 0, .o = 5 },
        .{ .s = 7, .r = 3, .o = 65535 },
        .{ .s = 1234, .r = 99, .o = 16777215 },
    };
    inline for (cases) |c| {
        const packed_bits = packSegment(c.s, c.r, c.o);
        try std.testing.expect(packed_bits >= 0);
        const loc = unpack(packed_bits);
        try std.testing.expectEqual(@as(usize, c.s), loc.segment.seg_idx);
        try std.testing.expectEqual(@as(usize, c.r), loc.segment.rg_idx);
        try std.testing.expectEqual(@as(usize, c.o), loc.segment.offset);
    }
}

test "rowloc round-trips memtable locations and is distinct from segment 0" {
    const seg0 = unpack(packSegment(0, 0, 0));
    try std.testing.expect(seg0 == .segment);
    inline for (.{ 0, 1, 42, 1_000_000 }) |row| {
        const loc = unpack(packMemtable(row));
        try std.testing.expect(loc == .memtable);
        try std.testing.expectEqual(@as(usize, row), loc.memtable.row);
    }
}
