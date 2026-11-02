//! The condition cache's record of one row group (§6.4): the stored rows of
//! one row group of an immutable segment that one AND arm of a scan's filter
//! matched, before tombstones. A record lives in the block cache, so it shares
//! the cache's LRU and memory budget, and takes the smallest of three forms:
//! the matched rows' positions, the unmatched rows' positions, or one bit per
//! row. None passes `rows / 8` bytes after its header.

const std = @import("std");
const simd = @import("../util/simd.zig");

pub const Form = enum(u8) { matched, unmatched, bitmap };

/// `[matched u32][rows u32][form u8][position width u8][6 spare]`.
const HEADER = 16;

/// The record of `mask`, the arm's verdict on each of a row group's stored
/// rows, `matched` of them true. Allocated from `allocator`, the block
/// cache's block allocator, as the cache takes ownership on insert.
pub fn encode(allocator: std.mem.Allocator, mask: []const bool, matched: usize) ![]align(16) u8 {
    const rows = mask.len;
    const width: u8 = if (rows > 1 << 16) 4 else 2;
    const listed = @min(matched, rows - matched);
    const bitmap_len = (rows + 7) / 8;
    const form: Form = if (listed * width >= bitmap_len) .bitmap else if (matched <= rows - matched) .matched else .unmatched;
    const body_len = if (form == .bitmap) bitmap_len else listed * width;
    const record = try allocator.alignedAlloc(u8, .@"16", HEADER + body_len);
    std.mem.writeInt(u32, record[0..4], @intCast(matched), .little);
    std.mem.writeInt(u32, record[4..8], @intCast(rows), .little);
    record[8] = @backingInt(form);
    record[9] = width;
    @memset(record[10..HEADER], 0);
    const body = record[HEADER..];
    switch (form) {
        .bitmap => simd.packMask(mask, body),
        .matched, .unmatched => writePositions(mask, form == .matched, width, body),
    }
    return record;
}

/// A record of how many of a row group's `rows` stored rows an arm matched,
/// without which: what a scan that only counts learns. It has no body, so
/// it is kept apart from the full records and never `expand`ed.
pub fn encodeCount(allocator: std.mem.Allocator, matched: usize, rows: usize) ![]align(16) u8 {
    const record = try allocator.alignedAlloc(u8, .@"16", HEADER);
    @memset(record, 0);
    std.mem.writeInt(u32, record[0..4], @intCast(matched), .little);
    std.mem.writeInt(u32, record[4..8], @intCast(rows), .little);
    return record;
}

/// The rows of `mask` equal to `want`, ascending, `width` bytes each.
fn writePositions(mask: []const bool, want: bool, width: u8, body: []u8) void {
    const bytes: []const u8 = @ptrCast(mask);
    const lanes = 64;
    const zero: @Vector(lanes, u8) = @splat(0);
    var k: usize = 0;
    var i: usize = 0;
    while (i < bytes.len) : (i += lanes) {
        const full = i + lanes <= bytes.len;
        var set: u64 = if (full) blk: {
            const v: @Vector(lanes, u8) = bytes[i..][0..lanes].*;
            break :blk @bitCast(v != zero);
        } else blk: {
            var tail: u64 = 0;
            for (bytes[i..], 0..) |b, lane| tail |= @as(u64, b) << @intCast(lane);
            break :blk tail;
        };
        const in_range: u64 = if (full) ~@as(u64, 0) else (@as(u64, 1) << @intCast(bytes.len - i)) - 1;
        if (!want) set = ~set & in_range;
        while (set != 0) : (set &= set - 1) {
            const row: u32 = @intCast(i + @ctz(set));
            if (width == 2) {
                std.mem.writeInt(u16, body[k..][0..2], @intCast(row), .little);
            } else {
                std.mem.writeInt(u32, body[k..][0..4], row, .little);
            }
            k += width;
        }
    }
    std.debug.assert(k == body.len);
}

/// How many stored rows the record's arm matched.
pub fn matchedRows(record: []const u8) u32 {
    return std.mem.readInt(u32, record[0..4], .little);
}

/// The record's verdicts into `mask`, one per stored row of its row group.
pub fn expand(record: []const u8, mask: []bool) void {
    std.debug.assert(std.mem.readInt(u32, record[4..8], .little) == mask.len);
    const form: Form = @fromBackingInt(@intCast(record[8]));
    const width = record[9];
    const body = record[HEADER..];
    switch (form) {
        .bitmap => simd.unpackMask(body, mask),
        .matched, .unmatched => {
            const listed = form == .matched;
            @memset(mask, !listed);
            var k: usize = 0;
            while (k < body.len) : (k += width) {
                const row: usize = if (width == 2) std.mem.readInt(u16, body[k..][0..2], .little) else std.mem.readInt(u32, body[k..][0..4], .little);
                mask[row] = listed;
            }
        },
    }
}

test "condition record: each form gives back the mask it was made from, in at most rows / 8 bytes" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0xc0d1);
    const random = prng.random();
    for ([_]usize{ 1, 7, 64, 65, 1000, 4096, 65536, 65537 }) |rows| {
        const mask = try allocator.alloc(bool, rows);
        defer allocator.free(mask);
        const back = try allocator.alloc(bool, rows);
        defer allocator.free(back);
        var seen: [3]bool = @splat(false);
        // Per mille: none, sparse, dense, all but a few, all.
        for ([_]u32{ 0, 2, 500, 998, 1000 }) |per_mille| {
            for (mask) |*m| m.* = random.uintLessThan(u32, 1000) < per_mille;
            const matched = simd.countTrue(mask);
            const record = try encode(allocator, mask, matched);
            defer allocator.free(record);
            seen[record[8]] = true;
            try std.testing.expect(record.len - HEADER <= (rows + 7) / 8);
            try std.testing.expectEqual(@as(u32, @intCast(matched)), matchedRows(record));
            @memset(back, per_mille % 2 == 0);
            expand(record, back);
            try std.testing.expectEqualSlices(bool, mask, back);
        }
        if (rows >= 4096) for (seen) |s| try std.testing.expect(s);
    }
}
