//! Query-time global string dictionary — the cross-segment "stitch" that lets
//! GROUP BY / DISTINCT / ORDER BY operate on a single integer code space even
//! though each segment carries its own *local* dictionary (and raw/memtable
//! blocks carry none).
//!
//! Per-segment dict codes are meaningless across segments (local code 0 is
//! "apple" in one segment, "banana" in another). This structure interns every
//! distinct string once into a stable **global code**, and produces a tiny
//! per-segment `local→global` lookup table so a scan can translate a row's
//! narrow local code to the global code with a single array index — no
//! re-hashing. Raw / memtable rows (no local dict) intern their bytes per row.
//!
//! Ownership: the global dict owns a copy of every distinct string's bytes (so
//! callers may free the source segment buffers), and can decode a global code
//! back to its string for result materialization at emit.

const std = @import("std");
const Allocator = std.mem.Allocator;

const storage = @import("../storage/storage.zig");
const DictBlock = storage.segment_reader.DictBlock;

pub const GlobalDict = struct {
    /// string bytes → global code. Keys alias `owned.items` entries (stable).
    map: std.StringHashMapUnmanaged(u32) = .empty,
    /// Interned distinct strings, indexed by global code. Each is an owned copy.
    owned: std.ArrayListUnmanaged([]const u8) = .empty,

    pub fn deinit(self: *GlobalDict, allocator: Allocator) void {
        for (self.owned.items) |s| allocator.free(@constCast(s));
        self.owned.deinit(allocator);
        self.map.deinit(allocator);
        self.* = undefined;
    }

    /// Number of distinct strings interned so far (the global code count).
    pub fn count(self: GlobalDict) u32 {
        return @intCast(self.owned.items.len);
    }

    /// Intern `s`, returning its stable global code (assigning a new one, with
    /// an owned copy of the bytes, on first sight).
    pub fn intern(self: *GlobalDict, allocator: Allocator, s: []const u8) !u32 {
        const gop = try self.map.getOrPut(allocator, s);
        if (gop.found_existing) return gop.value_ptr.*;
        // First sight: own the bytes, point the (just-inserted) key at the copy
        // so the map never references caller memory, and assign the next code.
        errdefer _ = self.map.remove(s);
        const copy = try allocator.dupe(u8, s);
        errdefer allocator.free(copy);
        try self.owned.append(allocator, copy);
        gop.key_ptr.* = copy;
        const code: u32 = @intCast(self.owned.items.len - 1);
        gop.value_ptr.* = code;
        return code;
    }

    /// Build a `local_code → global_code` lookup for one segment dict block:
    /// `lut[c]` is the global code for the block's local code `c`. The caller
    /// owns the returned slice. A row's global code is then `lut[rowCode(row)]`.
    pub fn buildLut(self: *GlobalDict, allocator: Allocator, db: DictBlock) ![]u32 {
        const lut = try allocator.alloc(u32, db.ndv);
        errdefer allocator.free(lut);
        var c: u32 = 0;
        while (c < db.ndv) : (c += 1) lut[c] = try self.intern(allocator, db.dictValue(c));
        return lut;
    }

    /// The global code for `s` if it has been interned, else null. Used by
    /// equality/range translation and ORDER BY without growing the dict.
    pub fn lookup(self: GlobalDict, s: []const u8) ?u32 {
        return self.map.get(s);
    }

    /// Decode a global code back to its string (for group-key materialization at
    /// emit). Aliases the owned copy — valid until `deinit`.
    pub fn decode(self: GlobalDict, code: u32) []const u8 {
        return self.owned.items[code];
    }
};

/// A string column held as global codes instead of materialized bytes — the
/// "coded" form that flows through the batch in Phase 4.2 Option A. Code-aware
/// consumers (the aggregate's group key, DISTINCT, sorted-code ORDER BY) read
/// the narrow `codes` directly; anyone else calls `materialize` to get the
/// ordinary `[offsets][bytes]` string column. `codes[i]` indexes `dict`. NULL
/// rows carry a placeholder code, masked by the column's validity bitmap (kept
/// alongside, same split as `OwnedColumn.data` / `.nulls`).
pub const CodedColumn = struct {
    codes: []const u32,
    dict: *const GlobalDict,

    pub fn rowCount(self: CodedColumn) usize {
        return self.codes.len;
    }

    /// Row `i`'s string, aliasing the dict's owned bytes (valid until the dict's
    /// `deinit`). Caller checks validity separately.
    pub fn rowBytes(self: CodedColumn, i: usize) []const u8 {
        return self.dict.decode(self.codes[i]);
    }

    /// Decode every row into an `OwnedStringColumn` — the single materialize
    /// choke point a non-code-aware consumer goes through. Two passes: size the
    /// per-row offsets, then copy each row's decoded bytes.
    pub fn materialize(self: CodedColumn, allocator: Allocator) !storage.OwnedStringColumn {
        const n = self.codes.len;
        const offsets = try allocator.alloc(u32, n + 1);
        errdefer allocator.free(offsets);
        var total: usize = 0;
        offsets[0] = 0;
        for (0..n) |i| {
            total += self.dict.decode(self.codes[i]).len;
            offsets[i + 1] = @intCast(total);
        }
        const bytes = try allocator.alloc(u8, total);
        errdefer allocator.free(bytes);
        for (0..n) |i| {
            const s = self.dict.decode(self.codes[i]);
            @memcpy(bytes[offsets[i]..offsets[i + 1]], s);
        }
        return .{ .offsets = offsets, .bytes = bytes };
    }
};

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "intern assigns stable, deduplicated codes and decodes back" {
    var gd: GlobalDict = .{};
    defer gd.deinit(testing.allocator);

    const a = try gd.intern(testing.allocator, "apple");
    const b = try gd.intern(testing.allocator, "banana");
    const a2 = try gd.intern(testing.allocator, "apple");

    try testing.expectEqual(a, a2);
    try testing.expect(a != b);
    try testing.expectEqual(@as(u32, 2), gd.count());
    try testing.expectEqualStrings("apple", gd.decode(a));
    try testing.expectEqualStrings("banana", gd.decode(b));
    try testing.expectEqual(@as(?u32, a), gd.lookup("apple"));
    try testing.expectEqual(@as(?u32, null), gd.lookup("cherry"));
}

test "intern owns its bytes (source can be freed)" {
    var gd: GlobalDict = .{};
    defer gd.deinit(testing.allocator);

    const src = try testing.allocator.dupe(u8, "ephemeral");
    const code = try gd.intern(testing.allocator, src);
    testing.allocator.free(src); // global dict must not alias this
    try testing.expectEqualStrings("ephemeral", gd.decode(code));
    try testing.expectEqual(@as(?u32, code), gd.lookup("ephemeral"));
}

/// Build a DictBlock in memory from a set of (already-sorted) distinct values,
/// mirroring the on-disk layout `segment_reader.dictBlockOf` parses.
fn makeDictBlock(allocator: Allocator, values: []const []const u8, row_codes: []const u32) !struct { bytes: []u8, rows: u32 } {
    const k = values.len;
    var dict_bytes: usize = 0;
    for (values) |v| dict_bytes += v.len;
    const code_width: u8 = if (k <= 256) 1 else if (k <= 65536) 2 else 4;
    const total = 4 + 4 + (k + 1) * 4 + dict_bytes + row_codes.len * code_width;
    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);
    std.mem.writeInt(u32, buf[0..4], @intCast(k), .little);
    buf[4] = code_width;
    buf[5] = 0;
    buf[6] = 0;
    buf[7] = 0;
    var off: usize = 8;
    var acc: u32 = 0;
    std.mem.writeInt(u32, buf[off..][0..4], 0, .little);
    off += 4;
    for (values) |v| {
        acc += @intCast(v.len);
        std.mem.writeInt(u32, buf[off..][0..4], acc, .little);
        off += 4;
    }
    for (values) |v| {
        @memcpy(buf[off..][0..v.len], v);
        off += v.len;
    }
    for (row_codes) |rc| {
        var b8: [8]u8 = undefined;
        std.mem.writeInt(u64, &b8, rc, .little);
        @memcpy(buf[off..][0..code_width], b8[0..code_width]);
        off += code_width;
    }
    return .{ .bytes = buf, .rows = @intCast(row_codes.len) };
}

test "buildLut unifies overlapping per-segment dictionaries" {
    var gd: GlobalDict = .{};
    defer gd.deinit(testing.allocator);

    // Segment A dict (sorted): apple, banana, cherry. Segment B: banana, date.
    const seg_a = try makeDictBlock(testing.allocator, &.{ "apple", "banana", "cherry" }, &.{ 0, 2, 1, 0 });
    defer testing.allocator.free(seg_a.bytes);
    const seg_b = try makeDictBlock(testing.allocator, &.{ "banana", "date" }, &.{ 0, 1, 0 });
    defer testing.allocator.free(seg_b.bytes);

    const dba = storage.segment_reader.dictBlockOf(seg_a.bytes, seg_a.rows);
    const dbb = storage.segment_reader.dictBlockOf(seg_b.bytes, seg_b.rows);

    const lut_a = try gd.buildLut(testing.allocator, dba);
    defer testing.allocator.free(lut_a);
    const lut_b = try gd.buildLut(testing.allocator, dbb);
    defer testing.allocator.free(lut_b);

    // "banana" is local 1 in A and local 0 in B → must map to the same global.
    try testing.expectEqual(lut_a[1], lut_b[0]);
    // Four distinct strings total across both segments.
    try testing.expectEqual(@as(u32, 4), gd.count());

    // Translating each segment's row codes yields globally-consistent codes,
    // and decoding recovers the original value.
    const want_a = [_][]const u8{ "apple", "cherry", "banana", "apple" };
    for (0..dba.ndv) |_| {}
    for (0..seg_a.rows) |r| {
        const g = lut_a[dba.rowCode(r)];
        try testing.expectEqualStrings(want_a[r], gd.decode(g));
    }
    const want_b = [_][]const u8{ "banana", "date", "banana" };
    for (0..seg_b.rows) |r| {
        const g = lut_b[dbb.rowCode(r)];
        try testing.expectEqualStrings(want_b[r], gd.decode(g));
    }
}

test "CodedColumn materialize + rowBytes decode through the dict" {
    var gd: GlobalDict = .{};
    defer gd.deinit(testing.allocator);
    const red = try gd.intern(testing.allocator, "red");
    const green = try gd.intern(testing.allocator, "green");
    const blue = try gd.intern(testing.allocator, "blue");

    const codes = [_]u32{ red, green, blue, red, green };
    const cc = CodedColumn{ .codes = &codes, .dict = &gd };
    const want = [_][]const u8{ "red", "green", "blue", "red", "green" };

    // Direct per-row access (the code-aware path).
    for (0..cc.rowCount()) |i| try testing.expectEqualStrings(want[i], cc.rowBytes(i));

    // Materialize (the non-code-aware choke point) → identical string column.
    var sc = try cc.materialize(testing.allocator);
    defer sc.deinit(testing.allocator);
    const sv = sc.view();
    try testing.expectEqual(@as(usize, want.len), sv.rowCount());
    for (0..want.len) |i| try testing.expectEqualStrings(want[i], sv.rowBytes(i));
}
