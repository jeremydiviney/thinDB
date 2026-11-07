//! A segment string table's n-gram index (DESIGN.md §4.7): for each trigram
//! of the column's values (`util/ngram.zig`), the row groups whose values
//! hold it, so a scan skips the row groups a LIKE or a regex can't match in.
//!
//! The index is one column block (on-disk encoding `.raw`) after the
//! table's ids. Its payload:
//!
//!   [row_group_count: u32][gram_count: u32]   row groups 0 .. row_group_count
//!   [grams: gram_count × u32]   ascending
//!   [ends: gram_count × u32]    each gram's posting's end in `postings`
//!   [postings]
//!
//! A posting is empty when every row group the table took holds the gram; a
//! bitmap of `ceil(row_group_count / 8)` bytes; or, when shorter than that,
//! the row groups' indices ascending as LEB128 gaps, the first from 0. A gram
//! with no posting is in no row group the table took. The row groups counted
//! run to the table's last; any after it the table didn't take.

const std = @import("std");
const Allocator = std.mem.Allocator;

const format = @import("format.zig");
const int_streams = @import("int_streams.zig");
const segment_strings = @import("segment_strings.zig");
const ngram = @import("../util/ngram.zig");
const Gram = ngram.Gram;

/// Dictionary bytes a column's row groups must average for the index to
/// pay: a smaller dictionary reads whole in about the time its share of the
/// index does, and its LIKE is decided once per entry anyway.
pub const min_row_group_bytes: u64 = 64 << 10;

/// Longest average value the index covers: a document-length value holds
/// most grams, so nearly every posting says "every row group" and skips
/// nothing, at the cost of reading every byte to build it.
pub const max_value_bytes: u64 = 4 << 10;

/// Pairs of a gram and a row group past which a segment keeps no index: the
/// build holds two u32s per pair.
const max_pairs: usize = 1 << 27;

/// Whether column `cs`'s values, after its row groups have all committed,
/// take an index.
pub fn qualifies(cs: *const segment_strings.ColumnStrings) bool {
    const blocks = cs.blocks.items;
    if (blocks.len == 0 or cs.valueCount() == 0) return false;
    if (cs.valueBytes() > max_value_bytes * cs.valueCount()) return false;
    var dictionary_bytes: u64 = 0;
    for (blocks) |b| for (b.values) |v| {
        dictionary_bytes += cs.value(v).len;
    };
    return dictionary_bytes >= min_row_group_bytes * blocks.len;
}

/// One column's index under construction: each block's grams gather apart,
/// in parallel, then `assemble` lays out the payload.
pub const Build = struct {
    allocator: Allocator,
    strings: *const segment_strings.ColumnStrings,
    /// Each block's distinct grams, by `strings.blocks` index.
    grams: []std.ArrayList(Gram),

    pub fn init(allocator: Allocator, strings: *const segment_strings.ColumnStrings) !Build {
        const grams = try allocator.alloc(std.ArrayList(Gram), strings.blocks.items.len);
        @memset(grams, .empty);
        return .{ .allocator = allocator, .strings = strings, .grams = grams };
    }

    pub fn deinit(self: *Build) void {
        for (self.grams) |*g| g.deinit(self.allocator);
        self.allocator.free(self.grams);
        self.* = undefined;
    }

    /// Gathers blocks `lo..hi`'s grams, apart from other ranges.
    pub fn gather(self: *Build, lo: usize, hi: usize) !void {
        var seen = try std.DynamicBitSetUnmanaged.initEmpty(self.allocator, ngram.gram_space);
        defer seen.deinit(self.allocator);
        for (self.strings.blocks.items[lo..hi], self.grams[lo..hi]) |b, *out| {
            for (b.values) |v| {
                const bytes = self.strings.value(v);
                if (bytes.len < 3) continue;
                for (0..bytes.len - 2) |i| {
                    const g = ngram.gramAt(bytes, i);
                    if (seen.isSet(g)) continue;
                    seen.set(g);
                    try out.append(self.allocator, g);
                }
            }
            for (out.items) |g| seen.unset(g);
        }
    }

    /// The payload, once every block has gathered, onto `out`, and its two
    /// integer arrays; null when it would hold no gram or pass `max_pairs`.
    pub fn assemble(self: *Build, out: *std.ArrayList(u8)) !?[2]int_streams.Stream {
        const allocator = self.allocator;
        const blocks = self.strings.blocks.items;
        const row_group_count = blocks[blocks.len - 1].rg_idx + 1;
        var pairs: usize = 0;
        for (self.grams) |g| pairs += g.items.len;
        if (pairs == 0 or pairs > max_pairs) return null;

        var present = try std.DynamicBitSetUnmanaged.initEmpty(allocator, ngram.gram_space);
        defer present.deinit(allocator);
        for (self.grams) |list| for (list.items) |g| present.set(g);
        const words = present.masks[0 .. ngram.gram_space / @bitSizeOf(usize)];
        const ranks = try allocator.alloc(u32, words.len);
        defer allocator.free(ranks);
        var gram_count: u32 = 0;
        for (words, ranks) |w, *r| {
            r.* = gram_count;
            gram_count += @popCount(w);
        }

        // Each gram's row groups, in row-group order: blocks commit in it.
        const starts = try allocator.alloc(u32, @as(usize, gram_count) + 1);
        defer allocator.free(starts);
        @memset(starts, 0);
        for (self.grams) |list| for (list.items) |g| {
            starts[rank(words, ranks, g) + 1] += 1;
        };
        for (1..starts.len) |k| starts[k] += starts[k - 1];
        const row_groups = try allocator.alloc(u32, pairs);
        defer allocator.free(row_groups);
        const filled = try allocator.alloc(u32, gram_count);
        defer allocator.free(filled);
        @memcpy(filled, starts[0..gram_count]);
        for (self.grams, blocks) |*list, b| {
            for (list.items) |g| {
                const k = rank(words, ranks, g);
                row_groups[filled[k]] = b.rg_idx;
                filled[k] += 1;
            }
            list.clearAndFree(allocator);
        }

        const covered = blocks.len;
        const bitmap_len = bitmapLength(row_group_count);
        const head = 8 + @as(usize, gram_count) * 8;
        try out.ensureUnusedCapacity(allocator, head);
        const base = out.items.len;
        out.appendNTimesAssumeCapacity(0, head);
        format.writeU32(out.items[base..][0..4], row_group_count);
        format.writeU32(out.items[base + 4 ..][0..4], gram_count);
        const postings_at = base + head;
        var k: usize = 0;
        var it = present.iterator(.{});
        while (it.next()) |g| : (k += 1) {
            const rgs = row_groups[starts[k]..starts[k + 1]];
            if (rgs.len != covered) {
                const gaps_len = gapsLength(rgs);
                if (gaps_len < bitmap_len) {
                    try appendGaps(allocator, out, rgs);
                } else {
                    const at = out.items.len;
                    try out.appendNTimes(allocator, 0, bitmap_len);
                    for (rgs) |r| out.items[at + r / 8] |= @as(u8, 1) << @intCast(r % 8);
                }
            }
            format.writeU32(out.items[base + 8 + k * 4 ..][0..4], @intCast(g));
            const end: u32 = @intCast(out.items.len - postings_at);
            format.writeU32(out.items[base + 8 + (@as(usize, gram_count) + k) * 4 ..][0..4], end);
        }
        return .{
            .{ .offset = @intCast(base + 8), .count = gram_count, .width = 4 },
            .{ .offset = @intCast(base + 8 + @as(usize, gram_count) * 4), .count = gram_count, .width = 4 },
        };
    }

    fn rank(words: []const usize, ranks: []const u32, g: Gram) u32 {
        const w = g / @bitSizeOf(usize);
        const below = (@as(usize, 1) << @intCast(g % @bitSizeOf(usize))) - 1;
        return ranks[w] + @popCount(words[w] & below);
    }
};

fn bitmapLength(row_group_count: u32) usize {
    return (@as(usize, row_group_count) + 7) / 8;
}

fn gapsLength(rgs: []const u32) usize {
    var len: usize = 0;
    var prev: u32 = 0;
    for (rgs) |r| {
        len += lebLength(r - prev);
        prev = r;
    }
    return len;
}

fn lebLength(v: u32) usize {
    return @max(1, (@as(usize, 32 - @clz(v)) + 6) / 7);
}

fn appendGaps(allocator: Allocator, out: *std.ArrayList(u8), rgs: []const u32) !void {
    var prev: u32 = 0;
    for (rgs) |r| {
        var v = r - prev;
        prev = r;
        while (v >= 0x80) : (v >>= 7) try out.append(allocator, @as(u8, @truncate(v)) | 0x80);
        try out.append(allocator, @intCast(v));
    }
}

/// A read index payload, over its bytes.
pub const Index = struct {
    row_group_count: u32,
    grams: []align(1) const u32,
    ends: []align(1) const u32,
    postings: []const u8,

    pub fn of(payload: []const u8) !Index {
        if (payload.len < 8) return format.Error.CorruptColumnBlockHeader;
        const row_group_count = format.readU32(payload[0..4]);
        const gram_count: usize = format.readU32(payload[4..8]);
        if (payload.len - 8 < gram_count * 8) return format.Error.CorruptColumnBlockHeader;
        const grams = std.mem.bytesAsSlice(u32, payload[8..][0 .. gram_count * 4]);
        const ends = std.mem.bytesAsSlice(u32, payload[8 + gram_count * 4 ..][0 .. gram_count * 4]);
        const postings = payload[8 + gram_count * 8 ..];
        if (gram_count > 0 and ends[gram_count - 1] != postings.len) return format.Error.CorruptColumnBlockHeader;
        return .{ .row_group_count = row_group_count, .grams = grams, .ends = ends, .postings = postings };
    }

    /// Sets `mask[rg]` (one per row group the index counts) when row group
    /// `rg` may hold what `q` asks, `scratch` holding the masks of its arms.
    pub fn evaluate(self: Index, scratch: Allocator, q: ngram.Query, mask: []bool) !void {
        switch (q) {
            .all => @memset(mask, true),
            .gram => |g| try self.rowGroupsOf(g, mask),
            .@"and" => |arms| {
                @memset(mask, true);
                const arm_mask = try scratch.alloc(bool, mask.len);
                defer scratch.free(arm_mask);
                for (arms) |arm| {
                    try self.evaluate(scratch, arm, arm_mask);
                    var any = false;
                    for (mask, arm_mask) |*m, a| {
                        m.* = m.* and a;
                        any = any or m.*;
                    }
                    if (!any) return;
                }
            },
            .@"or" => |arms| {
                @memset(mask, false);
                const arm_mask = try scratch.alloc(bool, mask.len);
                defer scratch.free(arm_mask);
                for (arms) |arm| {
                    try self.evaluate(scratch, arm, arm_mask);
                    for (mask, arm_mask) |*m, a| m.* = m.* or a;
                }
            },
        }
    }

    fn rowGroupsOf(self: Index, g: Gram, mask: []bool) !void {
        std.debug.assert(mask.len == self.row_group_count);
        const k = self.find(g) orelse {
            @memset(mask, false);
            return;
        };
        const start: usize = if (k == 0) 0 else self.ends[k - 1];
        const end: usize = self.ends[k];
        if (start > end or end > self.postings.len) return format.Error.CorruptColumnBlockHeader;
        const posting = self.postings[start..end];
        if (posting.len == 0) {
            @memset(mask, true);
            return;
        }
        if (posting.len == bitmapLength(self.row_group_count)) {
            for (mask, 0..) |*m, r| m.* = posting[r / 8] & (@as(u8, 1) << @intCast(r % 8)) != 0;
            return;
        }
        @memset(mask, false);
        var rg: u64 = 0;
        var i: usize = 0;
        while (i < posting.len) {
            var gap: u64 = 0;
            var shift: u6 = 0;
            while (true) : (shift += 7) {
                if (i == posting.len or shift > 28) return format.Error.CorruptColumnBlockHeader;
                const byte = posting[i];
                i += 1;
                gap |= @as(u64, byte & 0x7f) << shift;
                if (byte & 0x80 == 0) break;
            }
            rg += gap;
            if (rg >= mask.len) return format.Error.CorruptColumnBlockHeader;
            mask[@intCast(rg)] = true;
        }
    }

    fn find(self: Index, g: Gram) ?usize {
        var lo: usize = 0;
        var hi: usize = self.grams.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const at = self.grams[mid];
            if (at == g) return mid;
            if (at < g) lo = mid + 1 else hi = mid;
        }
        return null;
    }
};

test "ngram index: postings as every row group, gaps and bitmaps read back" {
    const allocator = std.testing.allocator;
    var cs = segment_strings.ColumnStrings.init(allocator, segment_strings.segment_value_cap);
    defer cs.deinit();

    const row_group_count = 20;
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(allocator);
    var offsets: std.ArrayList(u32) = .empty;
    defer offsets.deinit(allocator);
    for (0..row_group_count) |rg| {
        bytes.clearRetainingCapacity();
        offsets.clearRetainingCapacity();
        try offsets.append(allocator, 0);
        var name_buf: [16]u8 = undefined;
        const rows = [_][]const u8{
            "Common",
            try std.fmt.bufPrint(&name_buf, "x{d}y", .{rg}),
            if (rg == 3 or rg == 17) "beta" else "Common",
            if (rg % 2 == 0) "wide" else "Common",
        };
        for (rows) |r| {
            try bytes.appendSlice(allocator, r);
            try offsets.append(allocator, @intCast(bytes.items.len));
        }
        const view: @import("column.zig").ColumnView = .{ .data = .{ .string = .{ .offsets = offsets.items, .bytes = bytes.items } } };
        try std.testing.expect(try cs.absorb(view, 0, rows.len, @intCast(rg), &.{}, true));
    }
    try std.testing.expect(!qualifies(&cs));

    var build = try Build.init(allocator, &cs);
    defer build.deinit();
    try build.gather(0, 7);
    try build.gather(7, row_group_count);
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);
    _ = (try build.assemble(&payload)).?;

    const index = try Index.of(payload.items);
    try std.testing.expectEqual(@as(u32, row_group_count), index.row_group_count);
    var mask: [row_group_count]bool = undefined;
    const gram = ngram.gramAt;

    try index.evaluate(allocator, .{ .gram = gram("com", 0) }, &mask);
    for (mask) |m| try std.testing.expect(m);

    const beta: ngram.Query = .{ .@"and" = &.{ .{ .gram = gram("bet", 0) }, .{ .gram = gram("eta", 0) } } };
    try index.evaluate(allocator, beta, &mask);
    for (mask, 0..) |m, rg| try std.testing.expectEqual(rg == 3 or rg == 17, m);

    try index.evaluate(allocator, .{ .gram = gram("wid", 0) }, &mask);
    for (mask, 0..) |m, rg| try std.testing.expectEqual(rg % 2 == 0, m);

    try index.evaluate(allocator, .{ .gram = gram("x12", 0) }, &mask);
    for (mask, 0..) |m, rg| try std.testing.expectEqual(rg == 12, m);

    try index.evaluate(allocator, .{ .gram = gram("zzz", 0) }, &mask);
    for (mask) |m| try std.testing.expect(!m);

    const either: ngram.Query = .{ .@"or" = &.{ beta, .{ .gram = gram("wid", 0) } } };
    try index.evaluate(allocator, either, &mask);
    for (mask, 0..) |m, rg| try std.testing.expectEqual(rg == 3 or rg == 17 or rg % 2 == 0, m);
}
