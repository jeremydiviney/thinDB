//! String-family scalar function kernels: text manipulation, hash digests
//! (which produce hex strings), encoding (hex/base64), and chr (int → 1-byte
//! string). Registered in scalar_fn.zig's `builtins` array.

const std = @import("std");
const Allocator = std.mem.Allocator;

const common = @import("scalar_fn_common.zig");
const ColumnView = common.ColumnView;
const ColumnStore = common.ColumnStore;
const stringViewOf = common.stringViewOf;
const stringStoreOf = common.stringStoreOf;
const Type = @import("../types.zig").Type;

const regex = @import("../util/regex.zig");

// ---------------------------------------------------------------------------
// Core string kernels (upper, lower, length, trims, reverse, concat,
// substring, replace). The lengthKernel is also registered as octet_length
// and char_length aliases — same kernel, different name in builtins[].
// ---------------------------------------------------------------------------

/// REGEXP_REPLACE(haystack, pattern, replacement[, pos[, occurrence[,
/// match_type]]]): the matches from character `pos` on, or only the
/// `occurrence`-th of them when it's positive, replaced. The pattern,
/// replacement and match type are read from row 0 and the regex compiled
/// once per batch — i.e. they must be constant across the batch (the usual
/// case: SQL literals). The replacement may use `\N` capture backrefs.
/// Backed by the linear-time engine in util/regex.zig; unsupported regex
/// features (lookaround, in-pattern backrefs) or malformed patterns
/// surface as RegexInvalidPattern.
pub fn regexpReplaceKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    if (row_count == 0) return;
    const base = out.data.rowCount();
    var call = try RegexCall.init(allocator, args[1], if (args.len > 5) args[5] else null) orelse
        return appendNullStrings(allocator, out, row_count);
    defer call.deinit(allocator);
    const replacement = stringViewOf(args[2]).rowBytes(0);
    const sv = stringViewOf(args[0]);
    for (0..row_count) |i| {
        if (!allValid(args, i)) {
            try stringStoreOf(out).appendValue(allocator, "");
            try out.appendValidBit(allocator, base + i, false);
            continue;
        }
        const s = sv.rowBytes(i);
        const start = if (args.len > 3) try regexStart(s, args[3].data.bigint[i], 1) else 0;
        // Occurrence 0 replaces every match; MySQL reads a negative one as 1.
        const occurrence: usize = if (args.len > 4) switch (args[4].data.bigint[i]) {
            std.math.minInt(i64)...-1 => 1,
            else => |n| @intCast(n),
        } else 0;
        // `replaced` is borrowed from the scratch's reused output buffer;
        // appendValue copies it into the column store, so no free per row.
        const replaced = try call.re.replaceScratch(s, replacement, &call.scratch, start, occurrence);
        try stringStoreOf(out).appendValue(allocator, replaced);
        try out.appendValidBit(allocator, base + i, true);
    }
}

/// The compiled pattern of a REGEXP_* call and the matcher state its rows
/// share: the visited array, thread lists, capture arena, and slot buffers
/// all persist between rows, so applying one pattern to a whole batch
/// allocates ~nothing per row. The pattern and match type come from row 0.
const RegexCall = struct {
    re: regex.Regex,
    scratch: regex.Scratch,
    slots: []?usize,

    /// Null when the pattern or match type is NULL, which makes every row
    /// NULL.
    fn init(allocator: Allocator, pattern: ColumnView, match_type: ?ColumnView) !?RegexCall {
        if (!pattern.isValid(0)) return null;
        var options: regex.Options = .{};
        if (match_type) |m| {
            if (!m.isValid(0)) return null;
            options = try regexMatchOptions(stringViewOf(m).rowBytes(0));
        }
        var re = try regex.Regex.compileWith(allocator, stringViewOf(pattern).rowBytes(0), options);
        errdefer re.deinit();
        const slots = try allocator.alloc(?usize, re.n_slots);
        return .{ .re = re, .scratch = regex.Scratch.init(allocator), .slots = slots };
    }

    fn deinit(self: *RegexCall, allocator: Allocator) void {
        allocator.free(self.slots);
        self.scratch.deinit();
        self.re.deinit();
    }

    /// Byte bounds of the `occurrence`-th match at or after byte `start`.
    fn nth(self: *RegexCall, s: []const u8, start: usize, occurrence: usize) !?[2]usize {
        @memset(self.slots, null);
        if (!try self.re.findNth(&self.scratch, s, start, occurrence, self.slots)) return null;
        return .{ self.slots[0].?, self.slots[1].? };
    }
};

/// MySQL's match_type letters: c case-sensitive, i case-insensitive (the
/// later of the two wins), n `.` matches line ends too, m `^` and `$` match
/// at line ends too, u Unix line ends (the only line end here, so u changes
/// nothing). With no letters a pattern is case-sensitive, as in StarRocks
/// and DuckDB, where MySQL follows the collation.
fn regexMatchOptions(match_type: []const u8) !regex.Options {
    var options: regex.Options = .{};
    for (match_type) |c| switch (c) {
        'c' => options.case_insensitive = false,
        'i' => options.case_insensitive = true,
        'n' => options.dot_all = true,
        'm' => options.multiline = true,
        'u' => {},
        else => return error.RegexInvalidMatchType,
    };
    return options;
}

fn allValid(args: []const ColumnView, row: usize) bool {
    for (args) |a| if (!a.isValid(row)) return false;
    return true;
}

/// The byte offset of 1-based character position `pos` in `s`. MySQL
/// accepts `past_end` positions after the last character.
fn regexStart(s: []const u8, pos: i64, past_end: usize) !usize {
    if (pos < 1 or @as(u64, @intCast(pos)) > charCount(s) + past_end) return error.RegexIndexOutOfBounds;
    return charOffset(s, @intCast(pos - 1));
}

fn appendNullStrings(allocator: Allocator, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    for (0..row_count) |i| {
        try stringStoreOf(out).appendValue(allocator, "");
        try out.appendValidBit(allocator, base + i, false);
    }
}

pub fn orderKeyKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    try appendOrderKeys(allocator, args, out, row_count, false);
}

pub fn orderKeyDescKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    try appendOrderKeys(allocator, args, out, row_count, true);
}

/// Inverting every byte of an ascending key reverses its byte order, and
/// moves the NULL marker from first to last.
fn appendOrderKeys(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize, descending: bool) !void {
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var key: std.ArrayList(u8) = .empty;
    defer key.deinit(allocator);
    for (0..row_count) |row| {
        key.clearRetainingCapacity();
        for (args) |arg| try appendOrderKeyPart(allocator, &key, arg, row);
        if (descending) for (key.items) |*b| {
            b.* = ~b.*;
        };
        try ss.appendValue(allocator, key.items);
        try out.appendValidBit(allocator, base + row, true);
    }
}

fn appendOrderKeyPart(allocator: Allocator, key: *std.ArrayList(u8), arg: ColumnView, row: usize) Allocator.Error!void {
    if (!arg.isValid(row)) return key.append(allocator, 0);
    try key.append(allocator, 1);
    switch (arg.data) {
        // A zero byte escapes to 0x00 0xFF and 0x00 0x00 ends the text, so a
        // prefix sorts before every longer text, and the next key starts clean.
        .varchar, .string, .char, .json => |sv| {
            var rest = sv.rowBytes(row);
            while (std.mem.indexOfScalar(u8, rest, 0)) |zero| {
                try key.appendSlice(allocator, rest[0 .. zero + 1]);
                try key.append(allocator, 0xFF);
                rest = rest[zero + 1 ..];
            }
            try key.appendSlice(allocator, rest);
            try key.appendSlice(allocator, &.{ 0, 0 });
        },
        inline .float, .double => |s| {
            const Bits = std.meta.Int(.unsigned, @bitSizeOf(@TypeOf(s[row])));
            const sign: Bits = @as(Bits, 1) << (@bitSizeOf(Bits) - 1);
            // -0.0 sorts as 0.0. Negative values invert entirely so a larger
            // magnitude sorts lower; positive ones only set the sign bit.
            const bits: Bits = @bitCast(if (s[row] == 0) 0 else s[row]);
            try appendBigEndian(allocator, key, if (bits & sign != 0) ~bits else bits | sign);
        },
        .boolean => |s| try key.append(allocator, @intFromBool(s[row] != 0)),
        .uuid => |s| try appendBigEndian(allocator, key, s[row]),
        // Dates, datetimes and decimals are signed integers too: with the sign
        // bit flipped, big-endian bytes sort like the values.
        inline .int, .bigint, .date, .datetime, .tinyint, .smallint, .largeint, .decimal64, .decimal128 => |s| {
            const Bits = std.meta.Int(.unsigned, @bitSizeOf(@TypeOf(s[row])));
            const bits: Bits = @bitCast(s[row]);
            try appendBigEndian(allocator, key, bits ^ (@as(Bits, 1) << (@bitSizeOf(Bits) - 1)));
        },
    }
}

fn appendBigEndian(allocator: Allocator, key: *std.ArrayList(u8), bits: anytype) Allocator.Error!void {
    var bytes: [@sizeOf(@TypeOf(bits))]u8 = undefined;
    std.mem.writeInt(@TypeOf(bits), &bytes, bits, .big);
    try key.appendSlice(allocator, &bytes);
}

pub fn stringIdentityKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) try ss.appendValue(allocator, sv.rowBytes(i));
}

/// Vectorized in-place ASCII case-fold over a contiguous byte run. Folding is
/// per-byte independent, so the whole run is processed in `@Vector` chunks: a
/// byte in the source-case range gets bit 5 flipped (`|0x20` to lower, `&~0x20`
/// to upper), all other bytes pass through. Scalar tail handles the remainder.
inline fn foldBytes(buf: []u8, comptime to_lower: bool) void {
    const lo: u8 = if (to_lower) 'A' else 'a';
    const hi: u8 = if (to_lower) 'Z' else 'z';
    const W = std.simd.suggestVectorLength(u8) orelse 16;
    const V = @Vector(W, u8);
    var i: usize = 0;
    while (i + W <= buf.len) : (i += W) {
        const v: V = buf[i..][0..W].*;
        const in_range = (v >= @as(V, @splat(lo))) & (v <= @as(V, @splat(hi)));
        const flipped = if (to_lower) v | @as(V, @splat(0x20)) else v & @as(V, @splat(~@as(u8, 0x20)));
        buf[i..][0..W].* = @select(u8, in_range, flipped, v);
    }
    while (i < buf.len) : (i += 1) buf[i] = if (to_lower) std.ascii.toLower(buf[i]) else std.ascii.toUpper(buf[i]);
}

/// ASCII case-fold. Case-folding preserves byte length, so the whole output
/// byte buffer is reserved once (total = sum of source lengths); each row's
/// bytes are bulk-appended (one copy, no per-row scratch alloc), then the whole
/// appended region is folded in a single vectorized pass. The prior code did a
/// malloc+free and two copies per row plus a scalar byte loop, which dominated
/// wide string projections like `LOWER(customerNumber)` over millions of rows.
inline fn caseFoldKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize, comptime to_lower: bool) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var total: usize = 0;
    var i: usize = 0;
    while (i < row_count) : (i += 1) total += sv.rowBytes(i).len;
    try ss.ensureUnusedValueCapacity(allocator, row_count, total);
    const base = ss.bytes.items.len;
    i = 0;
    while (i < row_count) : (i += 1) {
        ss.bytes.appendSliceAssumeCapacity(sv.rowBytes(i));
        ss.offsets.appendAssumeCapacity(@intCast(ss.bytes.items.len));
    }
    foldBytes(ss.bytes.items[base..], to_lower);
}

pub fn upperKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try caseFoldKernel(allocator, args, out, row_count, false);
}

pub fn lowerKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try caseFoldKernel(allocator, args, out, row_count, true);
}

// octet_length / byte length: raw byte count.
pub fn lengthKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.int.append(allocator, @intCast(sv.rowBytes(i).len));
    }
}

// length / char_length: UTF-8 character (codepoint) count, matching DuckDB and
// the SQL standard. Counts the bytes that begin a codepoint (every byte except
// a 0b10xxxxxx continuation byte), so a Cyrillic/multi-byte string measures
// shorter than its byte length.
pub fn charLengthKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.int.append(allocator, @intCast(charCount(sv.rowBytes(i))));
    }
}

// Every function that takes or returns a position counts UTF-8 characters,
// as MySQL, StarRocks and DuckDB do, so it never splits a multi-byte one.

fn isCharStart(b: u8) bool {
    return b & 0xC0 != 0x80;
}

/// Characters in UTF-8 `s`: every byte but a 0b10xxxxxx continuation byte
/// starts one, so a malformed byte still counts once.
fn charCount(s: []const u8) usize {
    var n: usize = 0;
    for (s) |b| n += @intFromBool(isCharStart(b));
    return n;
}

/// Byte offset where character `k` of `s` starts, or `s.len` when `s`
/// has `k` characters or fewer.
fn charOffset(s: []const u8, k: usize) usize {
    if (k == 0) return 0;
    var seen: usize = 0;
    for (s, 0..) |b, j| {
        if (!isCharStart(b)) continue;
        if (seen == k) return j;
        seen += 1;
    }
    return s.len;
}

/// Characters [start, end) of `s`.
fn charSlice(s: []const u8, start: usize, end: usize) []const u8 {
    const rest = s[charOffset(s, start)..];
    return rest[0..charOffset(rest, end - start)];
}

/// The character at byte `j` of `s`, through its continuation bytes.
fn charAt(s: []const u8, j: usize) []const u8 {
    var end = j + 1;
    while (end < s.len and !isCharStart(s[end])) end += 1;
    return s[j..end];
}

/// 1-based character position of the first `needle` in `hay` at or after
/// byte `from`, or 0. An empty needle is found where the search starts.
fn findChars(hay: []const u8, needle: []const u8, from: usize) i32 {
    const idx = std.mem.indexOfPos(u8, hay, from, needle) orelse return 0;
    return @intCast(charCount(hay[0..idx]) + 1);
}

const TrimSide = enum { leading, trailing, both };

/// What one trim step removes: a single character that appears in the given
/// set (the one-argument forms remove only ' ', as in MySQL, StarRocks and
/// DuckDB; the two-argument call form removes any listed character, as in
/// StarRocks and DuckDB), or a whole copy of a string (MySQL's
/// `TRIM(... remstr FROM s)`).
const TrimRemoval = enum { char_set, substring };

fn trimRows(
    allocator: Allocator,
    args: []const ColumnView,
    out: *ColumnStore,
    row_count: usize,
    comptime side: TrimSide,
    comptime removal: TrimRemoval,
) !void {
    const sv = stringViewOf(args[0]);
    const remove_view = if (args.len > 1) stringViewOf(args[1]) else null;
    const ss = stringStoreOf(out);
    for (0..row_count) |i| {
        const remove = if (remove_view) |rv| rv.rowBytes(i) else " ";
        try ss.appendValue(allocator, trimText(sv.rowBytes(i), remove, side, removal));
    }
}

fn trimText(src: []const u8, remove: []const u8, comptime side: TrimSide, comptime removal: TrimRemoval) []const u8 {
    var start: usize = 0;
    var end: usize = src.len;
    if (side != .trailing) {
        while (start < end) {
            const n = leadingTrimLen(src[start..end], remove, removal);
            if (n == 0) break;
            start += n;
        }
    }
    if (side != .leading) {
        while (end > start) {
            const n = trailingTrimLen(src[start..end], remove, removal);
            if (n == 0) break;
            end -= n;
        }
    }
    return src[start..end];
}

fn leadingTrimLen(s: []const u8, remove: []const u8, comptime removal: TrimRemoval) usize {
    switch (removal) {
        .substring => return if (remove.len > 0 and std.mem.startsWith(u8, s, remove)) remove.len else 0,
        .char_set => {
            const ch = charAt(s, 0);
            return if (std.mem.indexOf(u8, remove, ch) != null) ch.len else 0;
        },
    }
}

fn trailingTrimLen(s: []const u8, remove: []const u8, comptime removal: TrimRemoval) usize {
    switch (removal) {
        .substring => return if (remove.len > 0 and std.mem.endsWith(u8, s, remove)) remove.len else 0,
        .char_set => {
            var char_start = s.len - 1;
            while (char_start > 0 and !isCharStart(s[char_start])) char_start -= 1;
            return if (std.mem.indexOf(u8, remove, s[char_start..]) != null) s.len - char_start else 0;
        },
    }
}

pub fn ltrimKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try trimRows(allocator, args, out, row_count, .leading, .char_set);
}

pub fn rtrimKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try trimRows(allocator, args, out, row_count, .trailing, .char_set);
}

pub fn trimKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try trimRows(allocator, args, out, row_count, .both, .char_set);
}

pub fn ltrimSubstringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try trimRows(allocator, args, out, row_count, .leading, .substring);
}

pub fn rtrimSubstringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try trimRows(allocator, args, out, row_count, .trailing, .substring);
}

pub fn trimSubstringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try trimRows(allocator, args, out, row_count, .both, .substring);
}

var uuid_stream_counter = std.atomic.Value(u64).init(0);

/// UUID(seed): random (version 4) UUIDs, as StarRocks and DuckDB return.
/// The compile pass supplies a fresh `seed` per statement; the counter gives
/// every batch its own stream under it.
pub fn uuidKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    if (row_count == 0) return;
    var key: [std.Random.DefaultCsprng.secret_seed_length]u8 = @splat(0);
    std.mem.writeInt(i64, key[0..8], args[0].data.bigint[0], .little);
    std.mem.writeInt(u64, key[8..16], uuid_stream_counter.fetchAdd(1, .monotonic), .little);
    var csprng = std.Random.DefaultCsprng.init(key);
    const ss = stringStoreOf(out);
    for (0..row_count) |_| {
        var bytes: [16]u8 = undefined;
        csprng.fill(&bytes);
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        const hex = std.fmt.bytesToHex(bytes, .lower);
        const text = hex[0..8] ++ "-" ++ hex[8..12] ++ "-" ++ hex[12..16] ++ "-" ++ hex[16..20] ++ "-" ++ hex[20..32];
        try ss.appendValue(allocator, text);
    }
}

pub fn reverseKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const dst = try allocator.alloc(u8, src.len);
        defer allocator.free(dst);
        var end = src.len;
        var written: usize = 0;
        while (end > 0) {
            var start = end - 1;
            while (start > 0 and !isCharStart(src[start])) start -= 1;
            @memcpy(dst[written..][0 .. end - start], src[start..end]);
            written += end - start;
            end = start;
        }
        try ss.appendValue(allocator, dst);
    }
}

pub fn concat2Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = stringViewOf(args[0]);
    const b = stringViewOf(args[1]);
    const ss = stringStoreOf(out);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        scratch.clearRetainingCapacity();
        try scratch.appendSlice(allocator, a.rowBytes(i));
        try scratch.appendSlice(allocator, b.rowBytes(i));
        try ss.appendValue(allocator, scratch.items);
    }
}

pub fn concat3Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = stringViewOf(args[0]);
    const b = stringViewOf(args[1]);
    const c = stringViewOf(args[2]);
    const ss = stringStoreOf(out);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        scratch.clearRetainingCapacity();
        try scratch.appendSlice(allocator, a.rowBytes(i));
        try scratch.appendSlice(allocator, b.rowBytes(i));
        try scratch.appendSlice(allocator, c.rowBytes(i));
        try ss.appendValue(allocator, scratch.items);
    }
}

pub fn concatNKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    for (0..row_count) |i| {
        scratch.clearRetainingCapacity();
        for (args) |arg| try scratch.appendSlice(allocator, stringViewOf(arg).rowBytes(i));
        try ss.appendValue(allocator, scratch.items);
    }
}

/// MySQL-style substring: 1-indexed start; negative start counts from
/// end; length < 0 → empty string. Out-of-range returns empty string
/// rather than erroring (matches MySQL).
/// SUBSTRING(s, pos[, len]), with or without the length.
pub fn substringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const starts = args[1].data.int;
    const lens: ?[]const i32 = if (args.len > 2) args[2].data.int else null;
    const ss = stringStoreOf(out);
    for (0..row_count) |i| {
        try ss.appendValue(allocator, substringChars(sv.rowBytes(i), starts[i], if (lens) |l| l[i] else null));
    }
}

/// MySQL's and StarRocks' SUBSTRING: `pos` counts from 1, or back from the
/// end when negative; position 0, a position outside `s` or a length
/// below 1 gives ''.
fn substringChars(s: []const u8, pos: i32, len: ?i32) []const u8 {
    const n: i64 = @intCast(charCount(s));
    if (pos == 0 or pos > n or -@as(i64, pos) > n) return "";
    const start: i64 = if (pos > 0) pos - 1 else n + pos;
    const count: i64 = len orelse n;
    if (count <= 0) return "";
    return charSlice(s, @intCast(start), @intCast(@min(start + count, n)));
}

/// MySQL REPLACE(haystack, needle, replacement). Empty needle leaves
/// the haystack unchanged (matches MySQL — avoids an infinite loop).
pub fn replaceKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const hay_view = stringViewOf(args[0]);
    const needle_view = stringViewOf(args[1]);
    const repl_view = stringViewOf(args[2]);
    const ss = stringStoreOf(out);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const hay = hay_view.rowBytes(i);
        const needle = needle_view.rowBytes(i);
        const repl = repl_view.rowBytes(i);
        if (needle.len == 0) {
            try ss.appendValue(allocator, hay);
            continue;
        }
        scratch.clearRetainingCapacity();
        var pos: usize = 0;
        while (std.mem.indexOfPos(u8, hay, pos, needle)) |found| {
            try scratch.appendSlice(allocator, hay[pos..found]);
            try scratch.appendSlice(allocator, repl);
            pos = found + needle.len;
        }
        try scratch.appendSlice(allocator, hay[pos..]);
        try ss.appendValue(allocator, scratch.items);
    }
}

// ---------------------------------------------------------------------------
// Hash kernels — produce hex-encoded digest strings (md5/sha1/sha256) or a
// numeric crc32. Tied to the string family because every hash input here is
// a string column.
// ---------------------------------------------------------------------------

pub fn md5Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var digest: [16]u8 = undefined;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        std.crypto.hash.Md5.hash(sv.rowBytes(i), &digest, .{});
        const hex_str = std.fmt.bytesToHex(digest, .lower);
        try ss.appendValue(allocator, &hex_str);
    }
}

pub fn sha1Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var digest: [20]u8 = undefined;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        std.crypto.hash.Sha1.hash(sv.rowBytes(i), &digest, .{});
        const hex_str = std.fmt.bytesToHex(digest, .lower);
        try ss.appendValue(allocator, &hex_str);
    }
}

pub fn sha256Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var digest: [32]u8 = undefined;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        std.crypto.hash.sha2.Sha256.hash(sv.rowBytes(i), &digest, .{});
        const hex_str = std.fmt.bytesToHex(digest, .lower);
        try ss.appendValue(allocator, &hex_str);
    }
}

pub fn crc32Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const c = std.hash.Crc32.hash(sv.rowBytes(i));
        try out.data.bigint.append(allocator, @intCast(c));
    }
}

pub fn sha2Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const bits = args[1].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        switch (bits[i]) {
            224 => {
                var digest: [28]u8 = undefined;
                std.crypto.hash.sha2.Sha224.hash(sv.rowBytes(i), &digest, .{});
                const hex_str = std.fmt.bytesToHex(digest, .lower);
                try ss.appendValue(allocator, &hex_str);
            },
            256, 0 => {
                var digest: [32]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(sv.rowBytes(i), &digest, .{});
                const hex_str = std.fmt.bytesToHex(digest, .lower);
                try ss.appendValue(allocator, &hex_str);
            },
            384 => {
                var digest: [48]u8 = undefined;
                std.crypto.hash.sha2.Sha384.hash(sv.rowBytes(i), &digest, .{});
                const hex_str = std.fmt.bytesToHex(digest, .lower);
                try ss.appendValue(allocator, &hex_str);
            },
            512 => {
                var digest: [64]u8 = undefined;
                std.crypto.hash.sha2.Sha512.hash(sv.rowBytes(i), &digest, .{});
                const hex_str = std.fmt.bytesToHex(digest, .lower);
                try ss.appendValue(allocator, &hex_str);
            },
            else => try ss.appendValue(allocator, ""),
        }
    }
}

pub fn md5sumKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var digest: [16]u8 = undefined;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        scratch.clearRetainingCapacity();
        for (args) |arg| try scratch.appendSlice(allocator, stringViewOf(arg).rowBytes(i));
        std.crypto.hash.Md5.hash(scratch.items, &digest, .{});
        const hex_str = std.fmt.bytesToHex(digest, .lower);
        try ss.appendValue(allocator, &hex_str);
    }
}

pub fn murmurHash3_32Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.bigint.append(allocator, @intCast(std.hash.Murmur3_32.hash(sv.rowBytes(i))));
}

pub fn xxHash3_64Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const bits = std.hash.XxHash3.hash(0, sv.rowBytes(i));
        try out.data.bigint.append(allocator, @bitCast(bits));
    }
}

pub fn xxHash3_128Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var digest: [16]u8 = undefined;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const lo = std.hash.XxHash3.hash(0, sv.rowBytes(i));
        const hi = std.hash.XxHash3.hash(1, sv.rowBytes(i));
        std.mem.writeInt(u64, digest[0..8], hi, .big);
        std.mem.writeInt(u64, digest[8..16], lo, .big);
        const hex_str = std.fmt.bytesToHex(digest, .lower);
        try ss.appendValue(allocator, &hex_str);
    }
}

// ---------------------------------------------------------------------------
// Encoding kernels — hex / base64 round-trips on string columns.
// ---------------------------------------------------------------------------

/// HEX(number): the hex of the value as a BIGINT (`common.integerHex`), a
/// double rounded to one first (`common.doubleAsBigint`).
pub fn hexBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    var buf: [16]u8 = undefined;
    for (args[0].data.bigint[0..row_count]) |v| try ss.appendValue(allocator, common.integerHex(&buf, v));
}

pub fn hexLargeintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    var buf: [16]u8 = undefined;
    for (args[0].data.largeint[0..row_count]) |v| try ss.appendValue(allocator, common.integerHex(&buf, common.wideIntegerAsBigint(v)));
}

pub fn hexDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    var buf: [16]u8 = undefined;
    for (args[0].data.double[0..row_count]) |x| try ss.appendValue(allocator, common.integerHex(&buf, common.doubleAsBigint(x)));
}

pub fn hexEncodeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const dst = try allocator.alloc(u8, src.len * 2);
        defer allocator.free(dst);
        const charset = "0123456789ABCDEF";
        for (src, 0..) |b, j| {
            dst[j * 2] = charset[b >> 4];
            dst[j * 2 + 1] = charset[b & 0x0F];
        }
        try ss.appendValue(allocator, dst);
    }
}

pub fn hexDecodeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        // Odd-length input + invalid chars → produce empty string
        // (MySQL convention is NULL but we'd need .kernel_managed).
        if (src.len % 2 != 0) {
            try ss.appendValue(allocator, "");
            continue;
        }
        const dst = try allocator.alloc(u8, src.len / 2);
        defer allocator.free(dst);
        var ok = true;
        for (dst, 0..) |*b, j| {
            const hi = decodeHexNibble(src[j * 2]) orelse {
                ok = false;
                break;
            };
            const lo = decodeHexNibble(src[j * 2 + 1]) orelse {
                ok = false;
                break;
            };
            b.* = (hi << 4) | lo;
        }
        try ss.appendValue(allocator, if (ok) dst else "");
    }
}

fn decodeHexNibble(c: u8) ?u8 {
    return switch (c) {
        '0'...'9' => c - '0',
        'a'...'f' => 10 + (c - 'a'),
        'A'...'F' => 10 + (c - 'A'),
        else => null,
    };
}

pub fn base64EncodeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    const enc = std.base64.standard.Encoder;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const dst = try allocator.alloc(u8, enc.calcSize(src.len));
        defer allocator.free(dst);
        const written = enc.encode(dst, src);
        try ss.appendValue(allocator, written);
    }
}

pub fn base64DecodeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    const dec = std.base64.standard.Decoder;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const out_len = dec.calcSizeForSlice(src) catch {
            try ss.appendValue(allocator, "");
            continue;
        };
        const dst = try allocator.alloc(u8, out_len);
        defer allocator.free(dst);
        dec.decode(dst, src) catch {
            try ss.appendValue(allocator, "");
            continue;
        };
        try ss.appendValue(allocator, dst);
    }
}

// ---------------------------------------------------------------------------
// Additional string parity functions.
// ---------------------------------------------------------------------------

pub fn concatWsKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sep_sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        if (!args[0].isValid(i)) {
            try ss.appendValue(allocator, "");
            try out.appendValidBit(allocator, base + i, false);
            continue;
        }
        const sep = sep_sv.rowBytes(i);
        scratch.clearRetainingCapacity();
        var appended = false;
        for (args[1..]) |arg| {
            if (!arg.isValid(i)) continue;
            if (appended) try scratch.appendSlice(allocator, sep);
            try scratch.appendSlice(allocator, stringViewOf(arg).rowBytes(i));
            appended = true;
        }
        try ss.appendValue(allocator, scratch.items);
        try out.appendValidBit(allocator, base + i, true);
    }
}

/// The character set MySQL names for a value of the argument's type: text
/// and JSON are utf8mb4, every other type binary. A NULL still has a type,
/// so the result is never NULL.
pub const charsetKernel = typeNamedKernel("utf8mb4", "utf8mb4", "binary");

/// The collation MySQL names for a value of the argument's type, as
/// `charsetKernel` names its character set: text collates as the connection
/// does (`@@collation_connection`), JSON as utf8mb4_bin.
pub const collationKernel = typeNamedKernel("utf8mb4_general_ci", "utf8mb4_bin", "binary");

fn typeNamedKernel(comptime text: []const u8, comptime json: []const u8, comptime other: []const u8) common.TypedKernelFn {
    return struct {
        fn kernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
            _ = out_type;
            _ = args;
            const name: []const u8 = if (arg_types[0] == .json) json else if (arg_types[0].isString()) text else other;
            const ss = stringStoreOf(out);
            const base = out.data.rowCount();
            for (0..row_count) |row| {
                try ss.appendValue(allocator, name);
                try out.appendValidBit(allocator, base + row, true);
            }
        }
    }.kernel;
}

/// `__row_key(a, b, ...)`: the ascending order key of the arguments, which
/// is equal for two rows exactly when every argument is, and NULL when any
/// argument is.
pub fn rowKeyKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var key: std.ArrayList(u8) = .empty;
    defer key.deinit(allocator);
    for (0..row_count) |row| {
        key.clearRetainingCapacity();
        const complete = for (args) |arg| {
            if (!arg.isValid(row)) break false;
            try appendOrderKeyPart(allocator, &key, arg, row);
        } else true;
        try ss.appendValue(allocator, if (complete) key.items else "");
        try out.appendValidBit(allocator, base + row, complete);
    }
}

pub fn leftKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ns = args[1].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const n: usize = if (ns[i] <= 0) 0 else @intCast(ns[i]);
        try ss.appendValue(allocator, src[0..charOffset(src, n)]);
    }
}

pub fn rightKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ns = args[1].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const n: usize = if (ns[i] <= 0) 0 else @intCast(ns[i]);
        const count = charCount(src);
        try ss.appendValue(allocator, src[charOffset(src, count - @min(count, n))..]);
    }
}

pub fn startsWithKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const prefix_sv = stringViewOf(args[1]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.boolean.append(allocator, if (std.mem.startsWith(u8, sv.rowBytes(i), prefix_sv.rowBytes(i))) 1 else 0);
    }
}

pub fn endsWithKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const suffix_sv = stringViewOf(args[1]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.boolean.append(allocator, if (std.mem.endsWith(u8, sv.rowBytes(i), suffix_sv.rowBytes(i))) 1 else 0);
    }
}

pub fn splitPartKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const delim_sv = stringViewOf(args[1]);
    const idxs = args[2].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const delim = delim_sv.rowBytes(i);
        const idx = idxs[i];
        if (idx == 0 or delim.len == 0) {
            try ss.appendValue(allocator, "");
            continue;
        }
        if (idx > 0) {
            var part: i32 = 1;
            var start: usize = 0;
            while (true) {
                const end = std.mem.indexOfPos(u8, src, start, delim) orelse src.len;
                if (part == idx) {
                    try ss.appendValue(allocator, src[start..end]);
                    break;
                }
                if (end == src.len) {
                    try ss.appendValue(allocator, "");
                    break;
                }
                start = end + delim.len;
                part += 1;
            }
        } else {
            var part: i32 = -1;
            var end: usize = src.len;
            while (true) {
                const start = std.mem.lastIndexOf(u8, src[0..end], delim) orelse 0;
                if (part == idx) {
                    const lo = if (start == 0) 0 else start + delim.len;
                    try ss.appendValue(allocator, src[lo..end]);
                    break;
                }
                if (start == 0) {
                    try ss.appendValue(allocator, "");
                    break;
                }
                end = start;
                part -= 1;
            }
        }
    }
}

/// REGEXP_LIKE(s, pattern[, match_type]): whether `pattern` matches
/// anywhere in `s`.
pub fn regexpLikeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    if (row_count == 0) return;
    const base = out.data.rowCount();
    var call = try RegexCall.init(allocator, args[1], if (args.len > 2) args[2] else null) orelse {
        try out.data.boolean.appendNTimes(allocator, 0, row_count);
        for (0..row_count) |i| try out.appendValidBit(allocator, base + i, false);
        return;
    };
    defer call.deinit(allocator);
    const sv = stringViewOf(args[0]);
    for (0..row_count) |i| {
        const valid = allValid(args, i);
        const ok = valid and try call.re.matchesWith(&call.scratch, sv.rowBytes(i), 0);
        try out.data.boolean.append(allocator, @intFromBool(ok));
        try out.appendValidBit(allocator, base + i, valid);
    }
}

/// REGEXP_INSTR(s, pattern[, pos[, occurrence[, return_option[,
/// match_type]]]]): the character position where the `occurrence`-th match
/// from character `pos` on starts, or just past its end when
/// `return_option` is 1; 0 when there's no such match.
pub fn regexpInstrKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    if (row_count == 0) return;
    const base = out.data.rowCount();
    var call = try RegexCall.init(allocator, args[1], if (args.len > 5) args[5] else null) orelse {
        try out.data.bigint.appendNTimes(allocator, 0, row_count);
        for (0..row_count) |i| try out.appendValidBit(allocator, base + i, false);
        return;
    };
    defer call.deinit(allocator);
    const sv = stringViewOf(args[0]);
    for (0..row_count) |i| {
        const valid = allValid(args, i);
        try out.data.bigint.append(allocator, if (valid) try regexpInstrRow(&call, args, sv.rowBytes(i), i) else 0);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

fn regexpInstrRow(call: *RegexCall, args: []const ColumnView, s: []const u8, row: usize) !i64 {
    const return_end = if (args.len > 4) switch (args[4].data.bigint[row]) {
        0 => false,
        1 => true,
        else => return error.RegexInvalidReturnOption,
    } else false;
    // MySQL skips the position check for an empty subject.
    const start = if (args.len > 2 and s.len > 0) try regexStart(s, args[2].data.bigint[row], 0) else 0;
    const occurrence: usize = if (args.len > 3) @intCast(@max(args[3].data.bigint[row], 1)) else 1;
    const bounds = try call.nth(s, start, occurrence) orelse return 0;
    const at = if (return_end) bounds[1] else bounds[0];
    return @intCast(charCount(s[0..at]) + 1);
}

/// REGEXP_SUBSTR(s, pattern[, pos[, occurrence[, match_type]]]): the text of
/// the `occurrence`-th match from character `pos` on; NULL when there's no
/// such match.
pub fn regexpSubstrKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    if (row_count == 0) return;
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var call = try RegexCall.init(allocator, args[1], if (args.len > 4) args[4] else null) orelse
        return appendNullStrings(allocator, out, row_count);
    defer call.deinit(allocator);
    const sv = stringViewOf(args[0]);
    for (0..row_count) |i| {
        const src = sv.rowBytes(i);
        const bounds: ?[2]usize = if (allValid(args, i)) blk: {
            const start = if (args.len > 2) try regexStart(src, args[2].data.bigint[i], 1) else 0;
            const occurrence: usize = if (args.len > 3) @intCast(@max(args[3].data.bigint[i], 1)) else 1;
            break :blk try call.nth(src, start, occurrence);
        } else null;
        try ss.appendValue(allocator, if (bounds) |b| src[b[0]..b[1]] else "");
        try out.appendValidBit(allocator, base + i, bounds != null);
    }
}

pub fn bitLengthKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, @intCast(sv.rowBytes(i).len * 8));
}

pub fn ordKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    return asciiKernel(allocator, args, out, row_count);
}

pub fn fieldKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const probe_sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var found: i32 = 0;
        if (args[0].isValid(i)) {
            const probe = probe_sv.rowBytes(i);
            for (args[1..], 1..) |arg, pos| {
                if (arg.isValid(i) and std.mem.eql(u8, probe, stringViewOf(arg).rowBytes(i))) {
                    found = @intCast(pos);
                    break;
                }
            }
        }
        try out.data.int.append(allocator, found);
    }
}

pub fn findInSetKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const needle_sv = stringViewOf(args[0]);
    const set_sv = stringViewOf(args[1]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const needle = needle_sv.rowBytes(i);
        const set = set_sv.rowBytes(i);
        var idx: i32 = 1;
        var cursor: usize = 0;
        var found: i32 = 0;
        while (cursor <= set.len) : (idx += 1) {
            const end = std.mem.indexOfScalarPos(u8, set, cursor, ',') orelse set.len;
            if (std.mem.eql(u8, needle, set[cursor..end])) {
                found = idx;
                break;
            }
            if (end == set.len) break;
            cursor = end + 1;
        }
        try out.data.int.append(allocator, found);
    }
}

pub fn initcapKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const dst = try allocator.alloc(u8, src.len);
        defer allocator.free(dst);
        var new_word = true;
        for (src, dst) |b, *d| {
            if (std.ascii.isAlphanumeric(b)) {
                d.* = if (new_word) std.ascii.toUpper(b) else std.ascii.toLower(b);
                new_word = false;
            } else {
                d.* = b;
                new_word = true;
            }
        }
        try ss.appendValue(allocator, dst);
    }
}

pub fn translateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const from_sv = stringViewOf(args[1]);
    const to_sv = stringViewOf(args[2]);
    const ss = stringStoreOf(out);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const from = from_sv.rowBytes(i);
        const to = to_sv.rowBytes(i);
        scratch.clearRetainingCapacity();
        var j: usize = 0;
        while (j < src.len) {
            const ch = charAt(src, j);
            j += ch.len;
            if (charIndex(from, ch)) |k| {
                try scratch.appendSlice(allocator, charSlice(to, k, k + 1));
            } else {
                try scratch.appendSlice(allocator, ch);
            }
        }
        try ss.appendValue(allocator, scratch.items);
    }
}

/// Character index of the first `ch` in `s`.
fn charIndex(s: []const u8, ch: []const u8) ?usize {
    var k: usize = 0;
    var j: usize = 0;
    while (j < s.len) : (k += 1) {
        const c = charAt(s, j);
        if (std.mem.eql(u8, c, ch)) return k;
        j += c.len;
    }
    return null;
}

// ---------------------------------------------------------------------------
// Expanded string kernels (DuckDB / MySQL / StarRocks parity additions):
// lpad, rpad, repeat, space, ascii, position, instr, substring_index,
// strcmp, greatest/least over strings, chr.
// ---------------------------------------------------------------------------

pub fn lpadKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try padKernel(allocator, args, out, row_count, .left);
}

pub fn rpadKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try padKernel(allocator, args, out, row_count, .right);
}

/// LPAD / RPAD(s, len, pad): `s` cut or padded to `len` characters, the
/// pad repeating as needed. An empty pad leaves a short `s` as it is.
fn padKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize, side: enum { left, right }) !void {
    const sv = stringViewOf(args[0]);
    const lens = args[1].data.int;
    const pad_sv = stringViewOf(args[2]);
    const ss = stringStoreOf(out);
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    for (0..row_count) |i| {
        const src = sv.rowBytes(i);
        const pad = pad_sv.rowBytes(i);
        const target: usize = @intCast(@max(lens[i], 0));
        const src_chars = charCount(src);
        if (src_chars >= target or pad.len == 0) {
            try ss.appendValue(allocator, charSlice(src, 0, @min(src_chars, target)));
            continue;
        }
        const needed = target - src_chars;
        const pad_chars = charCount(pad);
        buf.clearRetainingCapacity();
        if (side == .right) try buf.appendSlice(allocator, src);
        for (0..needed / pad_chars) |_| try buf.appendSlice(allocator, pad);
        try buf.appendSlice(allocator, charSlice(pad, 0, needed % pad_chars));
        if (side == .left) try buf.appendSlice(allocator, src);
        try ss.appendValue(allocator, buf.items);
    }
}

pub fn repeatKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ns = args[1].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const n = ns[i];
        if (n <= 0 or src.len == 0) {
            try ss.appendValue(allocator, "");
            continue;
        }
        const total: usize = src.len * @as(usize, @intCast(n));
        var buf = try allocator.alloc(u8, total);
        defer allocator.free(buf);
        var k: usize = 0;
        while (k < @as(usize, @intCast(n))) : (k += 1) {
            @memcpy(buf[k * src.len ..][0..src.len], src);
        }
        try ss.appendValue(allocator, buf);
    }
}

pub fn spaceKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ns = args[0].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const n = ns[i];
        if (n <= 0) {
            try ss.appendValue(allocator, "");
            continue;
        }
        const len: usize = @intCast(n);
        const buf = try allocator.alloc(u8, len);
        defer allocator.free(buf);
        @memset(buf, ' ');
        try ss.appendValue(allocator, buf);
    }
}

pub fn asciiKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const v: i32 = if (src.len == 0) 0 else @intCast(src[0]);
        try out.data.int.append(allocator, v);
    }
}

/// POSITION(needle IN haystack) / LOCATE(needle, haystack): the 1-based
/// character position of `needle`, or 0 if absent. An empty needle is at 1.
pub fn positionKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const needle_sv = stringViewOf(args[0]);
    const hay_sv = stringViewOf(args[1]);
    for (0..row_count) |i| {
        try out.data.int.append(allocator, findChars(hay_sv.rowBytes(i), needle_sv.rowBytes(i), 0));
    }
}

/// INSTR(haystack, needle) / STRPOS(haystack, needle): POSITION with the
/// arguments swapped.
pub fn instrKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const hay_sv = stringViewOf(args[0]);
    const needle_sv = stringViewOf(args[1]);
    for (0..row_count) |i| {
        try out.data.int.append(allocator, findChars(hay_sv.rowBytes(i), needle_sv.rowBytes(i), 0));
    }
}

/// LOCATE(needle, haystack, pos): the search starts at character `pos`; a
/// `pos` below 1 or past the end finds nothing, as in MySQL and StarRocks.
pub fn locateFromKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const needle_sv = stringViewOf(args[0]);
    const hay_sv = stringViewOf(args[1]);
    const starts = args[2].data.int;
    for (0..row_count) |i| {
        const hay = hay_sv.rowBytes(i);
        const pos = starts[i];
        const found: i32 = if (pos < 1 or pos > charCount(hay) + 1) 0 else findChars(hay, needle_sv.rowBytes(i), charOffset(hay, @intCast(pos - 1)));
        try out.data.int.append(allocator, found);
    }
}

/// SUBSTRING_INDEX(s, delim, count). Positive count: keep first N parts;
/// negative count: keep last |N| parts. count=0 → empty string. Matches MySQL.
pub fn substringIndexKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const delim_sv = stringViewOf(args[1]);
    const counts = args[2].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const delim = delim_sv.rowBytes(i);
        const n = counts[i];
        if (n == 0 or delim.len == 0) {
            try ss.appendValue(allocator, if (delim.len == 0) src else "");
            continue;
        }
        if (n > 0) {
            var remaining: i32 = n;
            var cursor: usize = 0;
            while (remaining > 0 and cursor < src.len) {
                if (std.mem.indexOfPos(u8, src, cursor, delim)) |idx| {
                    remaining -= 1;
                    if (remaining == 0) {
                        try ss.appendValue(allocator, src[0..idx]);
                        break;
                    }
                    cursor = idx + delim.len;
                } else break;
            } else {
                try ss.appendValue(allocator, src);
                continue;
            }
            if (remaining > 0) try ss.appendValue(allocator, src);
        } else {
            var want: i32 = -n;
            var idx_opt: ?usize = src.len;
            while (want > 0) : (want -= 1) {
                const upper_bound = idx_opt orelse 0;
                if (upper_bound == 0) {
                    idx_opt = null;
                    break;
                }
                idx_opt = std.mem.lastIndexOf(u8, src[0..upper_bound], delim);
                if (idx_opt == null) break;
            }
            if (idx_opt) |idx| {
                try ss.appendValue(allocator, src[idx + delim.len ..]);
            } else {
                try ss.appendValue(allocator, src);
            }
        }
    }
}

pub fn strcmpKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a_sv = stringViewOf(args[0]);
    const b_sv = stringViewOf(args[1]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const a = a_sv.rowBytes(i);
        const b = b_sv.rowBytes(i);
        const v: i32 = switch (std.mem.order(u8, a, b)) {
            .lt => -1,
            .eq => 0,
            .gt => 1,
        };
        try out.data.int.append(allocator, v);
    }
}

/// GREATEST/LEAST over any number of string arguments, by byte order; the
/// first of equal values wins.
fn StringExtremum(comptime replace_when: std.math.Order) type {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const ss = stringStoreOf(out);
            var i: usize = 0;
            while (i < row_count) : (i += 1) {
                var best = stringViewOf(args[0]).rowBytes(i);
                for (args[1..]) |arg| {
                    const x = stringViewOf(arg).rowBytes(i);
                    if (std.mem.order(u8, x, best) == replace_when) best = x;
                }
                try ss.appendValue(allocator, best);
            }
        }
    };
}

pub const greatestStringKernel = StringExtremum(.gt).kernel;
pub const leastStringKernel = StringExtremum(.lt).kernel;

/// chr(int) — inverse of ascii. Out-of-range / negative input → empty string.
pub fn chrKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const codes = args[0].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const c = codes[i];
        if (c < 0 or c > 255) {
            try ss.appendValue(allocator, "");
        } else {
            const b: [1]u8 = .{@intCast(c)};
            try ss.appendValue(allocator, &b);
        }
    }
}

/// ELT(n, s1, s2, ...): the n-th string. NULL when `n` is NULL or outside
/// 1..count, or when the string it picks is NULL, as in MySQL.
pub fn eltKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    for (0..row_count) |i| {
        const picked: ?ColumnView = if (eltIndex(args[0], i, args.len - 1)) |n| args[n] else null;
        const valid = if (picked) |p| p.isValid(i) else false;
        try ss.appendValue(allocator, if (valid) stringViewOf(picked.?).rowBytes(i) else "");
        try out.appendValidBit(allocator, base + i, valid);
    }
}

fn eltIndex(n_arg: ColumnView, row: usize, count: usize) ?usize {
    if (!n_arg.isValid(row)) return null;
    const n = n_arg.data.bigint[row];
    return if (n >= 1 and n <= count) @intCast(n) else null;
}

/// INSERT(s, pos, len, new): `s` with the `len` characters from `pos`
/// replaced by `new`. A position outside `s` leaves it unchanged; a
/// negative length, or one past the end, replaces the rest.
pub fn insertKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const positions = args[1].data.int;
    const lengths = args[2].data.int;
    const new_sv = stringViewOf(args[3]);
    const ss = stringStoreOf(out);
    var spliced: std.ArrayList(u8) = .empty;
    defer spliced.deinit(allocator);
    for (0..row_count) |i| {
        const s = sv.rowBytes(i);
        const pos = positions[i];
        if (pos < 1 or pos > charCount(s)) {
            try ss.appendValue(allocator, s);
            continue;
        }
        const start = charOffset(s, @intCast(pos - 1));
        const end = if (lengths[i] < 0) s.len else start + charOffset(s[start..], @intCast(lengths[i]));
        spliced.clearRetainingCapacity();
        try spliced.appendSlice(allocator, s[0..start]);
        try spliced.appendSlice(allocator, new_sv.rowBytes(i));
        try spliced.appendSlice(allocator, s[end..]);
        try ss.appendValue(allocator, spliced.items);
    }
}

/// QUOTE(s): `s` as a single-quoted SQL literal, with backslash, quote,
/// NUL and Ctrl-Z escaped; the word NULL, unquoted, for NULL.
pub fn quoteKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var quoted: std.ArrayList(u8) = .empty;
    defer quoted.deinit(allocator);
    for (0..row_count) |i| {
        if (!args[0].isValid(i)) {
            try ss.appendValue(allocator, "NULL");
            try out.appendValidBit(allocator, base + i, true);
            continue;
        }
        quoted.clearRetainingCapacity();
        try quoted.append(allocator, '\'');
        for (sv.rowBytes(i)) |c| switch (c) {
            '\\' => try quoted.appendSlice(allocator, "\\\\"),
            '\'' => try quoted.appendSlice(allocator, "\\'"),
            0 => try quoted.appendSlice(allocator, "\\0"),
            0x1a => try quoted.appendSlice(allocator, "\\Z"),
            else => try quoted.append(allocator, c),
        };
        try quoted.append(allocator, '\'');
        try ss.appendValue(allocator, quoted.items);
        try out.appendValidBit(allocator, base + i, true);
    }
}

/// SOUNDEX(s), MySQL's variant: the first letter, then the code of every
/// later letter that differs from the last code written, padded with zeros
/// to four characters but never cut short. Only ASCII letters have codes; a
/// multi-byte character can be the first letter (copied as is) and is
/// skipped anywhere else. Text with no letter gives ''.
pub fn soundexKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var code: std.ArrayList(u8) = .empty;
    defer code.deinit(allocator);
    for (0..row_count) |i| {
        code.clearRetainingCapacity();
        try appendSoundex(allocator, &code, sv.rowBytes(i));
        try ss.appendValue(allocator, code.items);
    }
}

const SOUNDEX_CODES = "01230120022455012623010202";

fn soundexCode(c: u8) u8 {
    const upper = std.ascii.toUpper(c);
    return if (upper >= 'A' and upper <= 'Z') SOUNDEX_CODES[upper - 'A'] else '0';
}

/// Byte length of the valid UTF-8 character starting `s`, or null.
fn utf8CharLen(s: []const u8) ?usize {
    const len = std.unicode.utf8ByteSequenceLength(s[0]) catch return null;
    if (len > s.len) return null;
    _ = std.unicode.utf8Decode(s[0..len]) catch return null;
    return len;
}

fn appendSoundex(allocator: Allocator, code: *std.ArrayList(u8), s: []const u8) !void {
    var i: usize = 0;
    var last: u8 = '0';
    while (i < s.len) {
        if (s[i] >= 0x80) {
            const len = utf8CharLen(s[i..]) orelse return;
            try code.appendSlice(allocator, s[i .. i + len]);
            i += len;
            break;
        }
        const c = s[i];
        i += 1;
        if (std.ascii.isAlphabetic(c)) {
            try code.append(allocator, std.ascii.toUpper(c));
            last = soundexCode(c);
            break;
        }
    } else return;
    var chars: usize = 1;
    while (i < s.len) {
        if (s[i] >= 0x80) {
            i += utf8CharLen(s[i..]) orelse break;
            continue;
        }
        const c = s[i];
        i += 1;
        if (!std.ascii.isAlphabetic(c)) continue;
        const digit = soundexCode(c);
        if (digit != '0' and digit != last) {
            try code.append(allocator, digit);
            last = digit;
            chars += 1;
        }
    }
    while (chars < 4) : (chars += 1) try code.append(allocator, '0');
}

test "foldBytes vectorized case-fold matches scalar across boundaries and non-alpha" {
    const testing = std.testing;
    // Length 67 spans multiple vector chunks plus a non-zero scalar tail; mixes
    // letters, digits, punctuation, and bytes adjacent to the A-Z/a-z range.
    const sample = "Hello, World! 123 @AZ[`az{ ABCxyz... MixedCASE-9 zZaA__~ payment-Hash99";
    inline for (.{ true, false }) |to_lower| {
        var buf = sample.*;
        foldBytes(&buf, to_lower);
        var want: [sample.len]u8 = undefined;
        for (sample, &want) |c, *w| w.* = if (to_lower) std.ascii.toLower(c) else std.ascii.toUpper(c);
        try testing.expectEqualSlices(u8, &want, &buf);
    }
}
