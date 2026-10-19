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
const aes = @import("../util/aes.zig");
const predicate = @import("predicate.zig");

// ---------------------------------------------------------------------------
// Core string kernels (upper, lower, length, trims, reverse, concat,
// substring, replace). The lengthKernel is also registered as octet_length
// and char_length aliases — same kernel, different name in builtins[].
// ---------------------------------------------------------------------------

/// REGEXP_REPLACE(haystack, pattern, replacement[, pos[, occurrence[,
/// match_type]]]): the matches from character `pos` on, or only the
/// `occurrence`-th of them when it's positive, replaced. Each row may give
/// its own pattern, replacement and match type (`RegexCall`). The
/// replacement may use `\N` capture backrefs.
/// Backed by the linear-time engine in util/regex.zig; unsupported regex
/// features (lookaround, in-pattern backrefs) or malformed patterns
/// surface as RegexInvalidPattern.
pub fn regexpReplaceKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    if (row_count == 0) return;
    const base = out.data.rowCount();
    var call = RegexCall.init(allocator, args[1], if (args.len > 5) args[5] else null);
    defer call.deinit();
    const replacements = stringViewOf(args[2]);
    const sv = stringViewOf(args[0]);
    for (0..row_count) |i| {
        const re = try call.at(i);
        if (re == null or !allValid(args, i)) {
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
        const replaced = try re.?.replaceScratch(s, replacements.rowBytes(i), &call.scratch, start, occurrence);
        try stringStoreOf(out).appendValue(allocator, replaced);
        try out.appendValidBit(allocator, base + i, true);
    }
}

/// The patterns of a REGEXP_* call's rows and the matcher state they
/// share: the visited array, thread lists, capture arena, and slot buffers
/// all persist between rows, so applying one pattern to a whole batch
/// allocates ~nothing per row. Each row may give its own pattern and match
/// type. The last pair compiled is kept, so a constant pattern compiles
/// once per batch, and a column of them once per change.
pub const RegexCall = struct {
    allocator: Allocator,
    patterns: ColumnView,
    match_types: ?ColumnView,
    compiled: ?Compiled = null,
    scratch: regex.Scratch,

    const Compiled = struct {
        re: regex.Regex,
        slots: []?usize,
        pattern: []u8,
        match_type: []u8,
    };

    pub fn init(allocator: Allocator, patterns: ColumnView, match_types: ?ColumnView) RegexCall {
        return .{ .allocator = allocator, .patterns = patterns, .match_types = match_types, .scratch = regex.Scratch.init(allocator) };
    }

    pub fn deinit(self: *RegexCall) void {
        self.release();
        self.scratch.deinit();
    }

    fn release(self: *RegexCall) void {
        const c = &(self.compiled orelse return);
        c.re.deinit();
        self.allocator.free(c.slots);
        self.allocator.free(c.pattern);
        self.allocator.free(c.match_type);
        self.compiled = null;
    }

    /// The regex `row` names, or null when its pattern or match type is
    /// NULL. Compiled even when the row's other arguments are NULL, so a
    /// malformed pattern fails the call as it does in MySQL.
    pub fn at(self: *RegexCall, row: usize) !?*regex.Regex {
        if (!self.patterns.isValid(row)) return null;
        const pattern = stringViewOf(self.patterns).rowBytes(row);
        var match_type: []const u8 = "";
        if (self.match_types) |m| {
            if (!m.isValid(row)) return null;
            match_type = stringViewOf(m).rowBytes(row);
        }
        if (self.compiled) |*c| {
            if (std.mem.eql(u8, c.pattern, pattern) and std.mem.eql(u8, c.match_type, match_type)) return &c.re;
        }
        self.release();
        // The scratch keys its lazy DFA by the program's address, which a
        // new compile may reuse, so a new pattern starts a new scratch.
        self.scratch.deinit();
        self.scratch = regex.Scratch.init(self.allocator);
        var re = try regex.Regex.compileWith(self.allocator, pattern, try regexMatchOptions(match_type));
        errdefer re.deinit();
        const slots = try self.allocator.alloc(?usize, re.n_slots);
        errdefer self.allocator.free(slots);
        const pattern_copy = try self.allocator.dupe(u8, pattern);
        errdefer self.allocator.free(pattern_copy);
        self.compiled = .{ .re = re, .slots = slots, .pattern = pattern_copy, .match_type = try self.allocator.dupe(u8, match_type) };
        return &self.compiled.?.re;
    }

    /// Byte bounds of the `occurrence`-th match at or after byte `start`
    /// of the pattern `at` last gave.
    fn nth(self: *RegexCall, s: []const u8, start: usize, occurrence: usize) !?[2]usize {
        const c = &self.compiled.?;
        @memset(c.slots, null);
        if (!try c.re.findNth(&self.scratch, s, start, occurrence, c.slots)) return null;
        return .{ c.slots[0].?, c.slots[1].? };
    }

    /// The capture slots of the first match at or after byte `pos.*` of
    /// the pattern `at` last gave, `pos.*` moving to where the search
    /// after it resumes; null when there is none.
    pub fn next(self: *RegexCall, s: []const u8, pos: *usize) !?[]const ?usize {
        const c = &self.compiled.?;
        if (pos.* > s.len) return null;
        @memset(c.slots, null);
        if (!try c.re.findWith(&self.scratch, s, pos.*, c.slots)) return null;
        pos.* = regex.afterMatch(s, c.slots[0].?, c.slots[1].?);
        return c.slots;
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
        .varchar, .string, .char, .json, .nested => |sv| {
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

/// Process-wide, so databases sharing a process never hand out the same
/// UUID_SHORT.
var uuid_short_counter = std.atomic.Value(u64).init(0);

/// UUID_SHORT(): MySQL's `(startup seconds << 24) + counter`, with the
/// statement's clock seconds (`args[0]`) for the startup time. Both terms
/// only grow, so each value is larger than every earlier one.
pub fn uuidShortKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    if (row_count == 0) return;
    const first = uuid_short_counter.fetchAdd(row_count, .monotonic);
    const seconds: u64 = @intCast(@max(args[0].data.bigint[0], 0));
    for (0..row_count) |k| try out.data.largeint.append(allocator, @as(i128, seconds << 24) + first + k);
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

/// AES_ENCRYPT(text, key[, iv[, mode[, aad]]]), as StarRocks reads it:
/// `mode` is `AES_<bits>_<chaining>`, AES_128_ECB by default, and `aad`
/// is GCM's alone (`aesRow`).
pub fn aesEncryptKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try aesKernel(allocator, args, out, row_count, .encrypt);
}

/// AES_DECRYPT(bytes, key[, iv[, mode[, aad]]]): what AES_ENCRYPT encrypted
/// under the same arguments; NULL for bytes it can't have written.
pub fn aesDecryptKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try aesKernel(allocator, args, out, row_count, .decrypt);
}

const AesDirection = enum { encrypt, decrypt };

fn aesKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize, comptime direction: AesDirection) !void {
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    var keys: AesKeyCache = .{};
    for (0..row_count) |i| {
        buf.clearRetainingCapacity();
        const valid = try aesRow(allocator, args, i, direction, &keys, &buf);
        try ss.appendValue(allocator, buf.items);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

/// The IV StarRocks uses where the IV is empty.
const AES_DEFAULT_IV = "STARROCKS_16BYTE";

/// One row's AES_ENCRYPT or AES_DECRYPT into `buf`; false where it is NULL.
/// A NULL argument makes it NULL, except an IV that ECB has no use for.
/// The IV is zero-padded or cut to 16 bytes, and GCM's nonce is its first
/// 12. A mode that isn't one, or an `aad` beside a mode other than GCM,
/// raises `InvalidEncryptionMode`.
fn aesRow(allocator: Allocator, args: []const ColumnView, row: usize, comptime direction: AesDirection, keys: *AesKeyCache, buf: *std.ArrayList(u8)) !bool {
    const mode: AesMode = if (args.len >= 4) mode: {
        if (!args[3].isValid(row)) return false;
        break :mode AesMode.parse(stringViewOf(args[3]).rowBytes(row)) orelse return error.InvalidEncryptionMode;
    } else .{ .key_len = 16, .chaining = .ecb };
    if (args.len == 5 and mode.chaining != .gcm) return error.InvalidEncryptionMode;
    if (!args[0].isValid(row) or !args[1].isValid(row)) return false;
    if (args.len == 5 and !args[4].isValid(row)) return false;
    const iv_text: []const u8 = if (args.len < 3 or mode.chaining == .ecb)
        ""
    else if (args[2].isValid(row))
        stringViewOf(args[2]).rowBytes(row)
    else
        return false;
    var iv: [aes.BLOCK_LEN]u8 = @splat(0);
    const iv_source = if (iv_text.len == 0) AES_DEFAULT_IV else iv_text;
    const iv_len = @min(iv_source.len, aes.BLOCK_LEN);
    @memcpy(iv[0..iv_len], iv_source[0..iv_len]);
    const cipher = keys.get(mode.key_len, stringViewOf(args[1]).rowBytes(row));
    const input = stringViewOf(args[0]).rowBytes(row);
    const aad = if (args.len == 5) stringViewOf(args[4]).rowBytes(row) else "";
    switch (direction) {
        .encrypt => {
            try aes.encrypt(allocator, cipher, mode.chaining, input, iv, aad, buf);
            return true;
        },
        .decrypt => return aes.decrypt(allocator, cipher, mode.chaining, input, iv, aad, buf),
    }
}

const AesMode = struct {
    key_len: usize,
    chaining: aes.Chaining,

    const CHAININGS = [_]struct { []const u8, aes.Chaining }{
        .{ "ECB", .ecb },   .{ "CBC", .cbc },   .{ "CFB", .cfb128 },
        .{ "CFB1", .cfb1 }, .{ "CFB8", .cfb8 }, .{ "CFB128", .cfb128 },
        .{ "OFB", .ofb },   .{ "CTR", .ctr },   .{ "GCM", .gcm },
    };

    /// `AES_<128|192|256>_<chaining>` in any case, CFB being CFB128.
    fn parse(text: []const u8) ?AesMode {
        if (text.len < 9 or !std.ascii.eqlIgnoreCase(text[0..4], "AES_") or text[7] != '_') return null;
        const key_len: usize = if (std.mem.eql(u8, text[4..7], "128"))
            16
        else if (std.mem.eql(u8, text[4..7], "192"))
            24
        else if (std.mem.eql(u8, text[4..7], "256"))
            32
        else
            return null;
        const chaining = for (CHAININGS) |entry| {
            if (std.ascii.eqlIgnoreCase(text[8..], entry[0])) break entry[1];
        } else return null;
        return .{ .key_len = key_len, .chaining = chaining };
    }
};

/// The cipher for the last key seen, since a column of keys mostly
/// repeats one.
const AesKeyCache = struct {
    key_len: usize = 0,
    folded: [32]u8 = undefined,
    cipher: aes.Cipher = undefined,

    /// The cipher for `key` folded into `key_len` bytes by XOR, as MySQL
    /// folds a key of any length.
    fn get(cache: *AesKeyCache, key_len: usize, key: []const u8) *const aes.Cipher {
        var folded: [32]u8 = @splat(0);
        for (key, 0..) |b, i| folded[i % key_len] ^= b;
        if (cache.key_len != key_len or !std.mem.eql(u8, cache.folded[0..key_len], folded[0..key_len])) {
            cache.* = .{ .key_len = key_len, .folded = folded, .cipher = aes.Cipher.init(folded[0..key_len]) };
        }
        return &cache.cipher;
    }
};

/// Seed StarRocks and Doris start `murmur_hash3_32` from, so checksums
/// written against either engine match.
const MURMUR3_32_SEED: u32 = 104729;

/// `murmur_hash3_32(s1, s2, ...)`: each argument is hashed with the previous
/// hash as its seed, and the result is a signed INT.
pub fn murmurHash3_32Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var hash: u32 = MURMUR3_32_SEED;
        for (args) |arg| hash = std.hash.Murmur3_32.hashWithSeed(stringViewOf(arg).rowBytes(i), hash);
        try out.data.int.append(allocator, @bitCast(hash));
    }
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

/// TYPEOF(x): the name StarRocks gives the argument's type. Like CHARSET it
/// depends on the type alone, so the result is never NULL.
pub fn typeofKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
    _ = out_type;
    _ = args;
    var name: std.ArrayList(u8) = .empty;
    defer name.deinit(allocator);
    try appendStarRocksTypeName(allocator, &name, arg_types[0]);
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    for (0..row_count) |row| {
        try ss.appendValue(allocator, name.items);
        try out.appendValidBit(allocator, base + row, true);
    }
}

/// StarRocks names a decimal by its backing width, which follows from the
/// precision alone, so DECIMAL(19, 2) is a decimal128 though this engine
/// backs it as one too and DECIMAL(9, 2) a decimal32 though this engine
/// backs it with 64 bits.
fn appendStarRocksTypeName(allocator: Allocator, out: *std.ArrayList(u8), t: Type) Allocator.Error!void {
    switch (t) {
        .varchar, .string => try out.appendSlice(allocator, "varchar"),
        .char => try out.appendSlice(allocator, "char"),
        .decimal64, .decimal128 => |spec| {
            const width: u8 = if (spec.p <= 9) 32 else if (spec.p <= 18) 64 else 128;
            try out.print(allocator, "decimal{d}({d}, {d})", .{ width, spec.p, spec.s });
        },
        .nested => |n| switch (n.*) {
            .array => |element| {
                try out.appendSlice(allocator, "array<");
                try appendStarRocksTypeName(allocator, out, element);
                try out.append(allocator, '>');
            },
            .map => |m| {
                try out.appendSlice(allocator, "map<");
                try appendStarRocksTypeName(allocator, out, m.key);
                try out.append(allocator, ',');
                try appendStarRocksTypeName(allocator, out, m.value);
                try out.append(allocator, '>');
            },
            .@"struct" => |fields| {
                try out.appendSlice(allocator, "struct<");
                for (fields, 0..) |f, i| {
                    if (i > 0) try out.appendSlice(allocator, ", ");
                    try out.appendSlice(allocator, f.name);
                    try out.append(allocator, ' ');
                    try appendStarRocksTypeName(allocator, out, f.type);
                }
                try out.append(allocator, '>');
            },
        },
        else => try out.appendSlice(allocator, @tagName(t)),
    }
}

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

/// NULL_OR_EMPTY(s): whether `s` is NULL or the empty string, never NULL.
pub fn nullOrEmptyKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const base = out.data.rowCount();
    for (0..row_count) |i| {
        const empty = !args[0].isValid(i) or sv.rowBytes(i).len == 0;
        try out.data.boolean.append(allocator, @intFromBool(empty));
        try out.appendValidBit(allocator, base + i, true);
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

/// `s LIKE pattern` for a pattern that isn't a literal (the parser keeps a
/// literal pattern on the `.like` predicate). The pattern may differ per row;
/// it compiles again only when it changes, so a constant one compiles once
/// per batch.
pub fn likeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const text = stringViewOf(args[0]);
    const patterns = stringViewOf(args[1]);
    var plan: ?predicate.LikePlan = null;
    for (0..row_count) |i| {
        const pattern = patterns.rowBytes(i);
        if (plan == null or !std.mem.eql(u8, plan.?.pattern, pattern)) plan = predicate.compileLike(pattern);
        try out.data.boolean.append(allocator, @intFromBool(plan.?.match(text.rowBytes(i))));
    }
}

/// REGEXP_LIKE(s, pattern[, match_type]): whether `pattern` matches
/// anywhere in `s`.
pub fn regexpLikeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    if (row_count == 0) return;
    const base = out.data.rowCount();
    var call = RegexCall.init(allocator, args[1], if (args.len > 2) args[2] else null);
    defer call.deinit();
    const sv = stringViewOf(args[0]);
    for (0..row_count) |i| {
        const re = try call.at(i);
        const valid = re != null and allValid(args, i);
        const ok = valid and try re.?.matchesWith(&call.scratch, sv.rowBytes(i), 0);
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
    var call = RegexCall.init(allocator, args[1], if (args.len > 5) args[5] else null);
    defer call.deinit();
    const sv = stringViewOf(args[0]);
    for (0..row_count) |i| {
        const valid = try call.at(i) != null and allValid(args, i);
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
    var call = RegexCall.init(allocator, args[1], if (args.len > 4) args[4] else null);
    defer call.deinit();
    const sv = stringViewOf(args[0]);
    for (0..row_count) |i| {
        const src = sv.rowBytes(i);
        const bounds: ?[2]usize = if (try call.at(i) != null and allValid(args, i)) blk: {
            const start = if (args.len > 2) try regexStart(src, args[2].data.bigint[i], 1) else 0;
            const occurrence: usize = if (args.len > 3) @intCast(@max(args[3].data.bigint[i], 1)) else 1;
            break :blk try call.nth(src, start, occurrence);
        } else null;
        try ss.appendValue(allocator, if (bounds) |b| src[b[0]..b[1]] else "");
        try out.appendValidBit(allocator, base + i, bounds != null);
    }
}

/// REGEXP_COUNT(s, pattern): how many times `pattern` matches `s`, each
/// search resuming where the last match ended.
pub fn regexpCountKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    var call = RegexCall.init(allocator, args[1], null);
    defer call.deinit();
    const sv = stringViewOf(args[0]);
    for (0..row_count) |i| {
        const valid = try call.at(i) != null and args[0].isValid(i);
        var count: i64 = 0;
        if (valid) {
            var pos: usize = 0;
            while (try call.next(sv.rowBytes(i), &pos)) |_| count += 1;
        }
        try out.data.bigint.append(allocator, count);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

/// REGEXP_EXTRACT(s, pattern, group): capture group `group` of the first
/// match (`regexGroup`); '' when nothing matches or there is no such group.
pub fn regexpExtractKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var call = RegexCall.init(allocator, args[1], null);
    defer call.deinit();
    const sv = stringViewOf(args[0]);
    for (0..row_count) |i| {
        const valid = try call.at(i) != null and allValid(args, i);
        var text: []const u8 = "";
        if (valid) {
            const s = sv.rowBytes(i);
            var pos: usize = 0;
            if (try call.next(s, &pos)) |slots| text = regexGroup(s, slots, args[2].data.bigint[i]) orelse "";
        }
        try ss.appendValue(allocator, text);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

/// The text of capture group `group` (0 the whole match) in a match's
/// `slots`: '' when the group took no part in the match, null when the
/// pattern has no such group.
pub fn regexGroup(s: []const u8, slots: []const ?usize, group: i64) ?[]const u8 {
    if (group < 0 or @as(u64, @intCast(group)) >= slots.len / 2) return null;
    const g: usize = @intCast(group);
    const start = slots[2 * g] orelse return "";
    return s[start..slots[2 * g + 1].?];
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

/// The longest string REPEAT, LPAD, RPAD and SPACE build: the
/// max_allowed_packet thinDB reports. MySQL answers a longer result with
/// NULL rather than building it. These kernels own their validity bitmap
/// (`.kernel_managed`).
pub const MAX_STRING_RESULT: usize = 16 * 1024 * 1024;

/// A result `len` bytes long, or null when it's past `MAX_STRING_RESULT`.
fn resultLen(len: anytype) ?usize {
    const n = std.math.cast(usize, len) orelse return null;
    return if (n <= MAX_STRING_RESULT) n else null;
}

/// LPAD / RPAD(s, len[, pad]): `s` cut or padded to `len` characters, the
/// pad repeating as needed, spaces when it's left out. An empty pad leaves a
/// short `s` as it is.
fn padKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize, side: enum { left, right }) !void {
    const sv = stringViewOf(args[0]);
    const lens = args[1].data.int;
    const pad_sv = if (args.len == 3) stringViewOf(args[2]) else null;
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    for (0..row_count) |i| {
        buf.clearRetainingCapacity();
        const pad = if (pad_sv) |pads| pads.rowBytes(i) else " ";
        const valid = allValid(args, i) and try padRow(allocator, &buf, sv.rowBytes(i), lens[i], pad, side == .left);
        try ss.appendValue(allocator, buf.items);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

/// `src` padded or cut to `target` characters into `buf`, or false when
/// `target` is negative, as in MySQL and StarRocks, or the result would be
/// past `MAX_STRING_RESULT`.
fn padRow(allocator: Allocator, buf: *std.ArrayList(u8), src: []const u8, target_in: i32, pad: []const u8, left: bool) !bool {
    if (target_in < 0) return false;
    const target: usize = @intCast(target_in);
    const src_chars = charCount(src);
    if (src_chars >= target or pad.len == 0) {
        try buf.appendSlice(allocator, charSlice(src, 0, @min(src_chars, target)));
        return true;
    }
    const needed = target - src_chars;
    const pad_chars = charCount(pad);
    const partial = charSlice(pad, 0, needed % pad_chars);
    _ = resultLen(@as(u128, needed / pad_chars) * pad.len + partial.len + src.len) orelse return false;
    if (!left) try buf.appendSlice(allocator, src);
    for (0..needed / pad_chars) |_| try buf.appendSlice(allocator, pad);
    try buf.appendSlice(allocator, partial);
    if (left) try buf.appendSlice(allocator, src);
    return true;
}

pub fn repeatKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ns = args[1].data.int;
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    for (0..row_count) |i| {
        const src = sv.rowBytes(i);
        const n: usize = @intCast(@max(ns[i], 0));
        buf.clearRetainingCapacity();
        const valid = allValid(args, i) and resultLen(@as(u128, src.len) * n) != null;
        if (valid) for (0..n) |_| try buf.appendSlice(allocator, src);
        try ss.appendValue(allocator, buf.items);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

pub fn spaceKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ns = args[0].data.int;
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    for (0..row_count) |i| {
        const len = resultLen(@max(ns[i], 0));
        const valid = args[0].isValid(i) and len != null;
        buf.clearRetainingCapacity();
        if (valid) try buf.appendNTimes(allocator, ' ', len.?);
        try ss.appendValue(allocator, buf.items);
        try out.appendValidBit(allocator, base + i, valid);
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

/// STRPOS(haystack, needle, instance), as Trino: the character position of
/// the instance-th occurrence of `needle`, overlapping ones counted, from
/// the end for a negative instance; 0 when there are fewer, or the instance
/// is 0. An empty needle is at 1, as in StarRocks.
pub fn strposInstanceKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const hay_sv = stringViewOf(args[0]);
    const needle_sv = stringViewOf(args[1]);
    const instances = args[2].data.int;
    for (0..row_count) |i| {
        const hay = hay_sv.rowBytes(i);
        const found: i32 = if (nthOccurrence(hay, needle_sv.rowBytes(i), instances[i])) |at| @intCast(charCount(hay[0..at]) + 1) else 0;
        try out.data.int.append(allocator, found);
    }
}

/// The byte offset of the `instance`-th occurrence of `needle` in `hay`,
/// overlapping ones counted, from the end when `instance` is negative.
fn nthOccurrence(hay: []const u8, needle: []const u8, instance: i32) ?usize {
    if (instance == 0) return null;
    if (needle.len == 0) return 0;
    var left = @abs(instance);
    if (instance > 0) {
        var from: usize = 0;
        while (std.mem.indexOfPos(u8, hay, from, needle)) |at| : (from = at + 1) {
            left -= 1;
            if (left == 0) return at;
        }
    } else {
        var end = hay.len;
        while (std.mem.lastIndexOf(u8, hay[0..end], needle)) |at| : (end = at + needle.len - 1) {
            left -= 1;
            if (left == 0) return at;
        }
    }
    return null;
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

/// MAKE_SET(bits, s1, s2, ...): the strings whose bit is set in `bits`
/// (s1 for bit 0), joined by commas. A NULL string is skipped; NULL `bits`
/// is NULL, as in MySQL.
pub fn makeSetKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var joined: std.ArrayList(u8) = .empty;
    defer joined.deinit(allocator);
    for (0..row_count) |i| {
        const valid = args[0].isValid(i);
        joined.clearRetainingCapacity();
        const bits: u64 = @bitCast(args[0].data.bigint[i]);
        var first = true;
        for (args[1..@min(args.len, 65)], 0..) |s, bit| {
            if (!valid or bits >> @intCast(bit) & 1 == 0 or !s.isValid(i)) continue;
            if (!first) try joined.append(allocator, ',');
            try joined.appendSlice(allocator, stringViewOf(s).rowBytes(i));
            first = false;
        }
        try ss.appendValue(allocator, joined.items);
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

// ---------------------------------------------------------------------------
// Byte formats
// ---------------------------------------------------------------------------

/// The text formats TO_BINARY reads and FROM_BINARY writes, named in any case.
const BinaryFormat = enum { hex, encode64, utf8 };

/// Row `row`'s format: the second argument, hex when there is none. Any
/// other name is `InvalidBinaryFormat`, as StarRocks fails it.
fn binaryFormat(args: []const ColumnView, row: usize) !BinaryFormat {
    if (args.len < 2) return .hex;
    const name = stringViewOf(args[1]).rowBytes(row);
    for (std.enums.values(BinaryFormat)) |f| {
        if (std.ascii.eqlIgnoreCase(name, @tagName(f))) return f;
    }
    return error.InvalidBinaryFormat;
}

/// TO_BINARY(s[, format]): the bytes `s` spells in `format`. Text that
/// isn't hex or padded base64 gives '', and empty base64 NULL, as in StarRocks.
pub fn toBinaryKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    for (0..row_count) |i| {
        buf.clearRetainingCapacity();
        var valid = allValid(args, i);
        if (valid) {
            const src = sv.rowBytes(i);
            switch (try binaryFormat(args, i)) {
                .utf8 => try buf.appendSlice(allocator, src),
                .hex => try decodeHexInto(allocator, &buf, src),
                .encode64 => if (src.len == 0) {
                    valid = false;
                } else try decodeBase64Into(allocator, &buf, src),
            }
        }
        try ss.appendValue(allocator, buf.items);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

/// FROM_BINARY(b[, format]): `b` written in `format`, hex in capitals.
pub fn fromBinaryKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    for (0..row_count) |i| {
        buf.clearRetainingCapacity();
        const valid = allValid(args, i);
        if (valid) {
            const src = sv.rowBytes(i);
            switch (try binaryFormat(args, i)) {
                .utf8 => try buf.appendSlice(allocator, src),
                .hex => for (src) |b| try buf.print(allocator, "{X:0>2}", .{b}),
                .encode64 => {
                    const enc = std.base64.standard.Encoder;
                    try buf.resize(allocator, enc.calcSize(src.len));
                    _ = enc.encode(buf.items, src);
                },
            }
        }
        try ss.appendValue(allocator, buf.items);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

/// The bytes hex text spells into `buf`, left empty when it isn't hex.
fn decodeHexInto(allocator: Allocator, buf: *std.ArrayList(u8), src: []const u8) !void {
    if (src.len % 2 != 0) return;
    try buf.resize(allocator, src.len / 2);
    for (buf.items, 0..) |*b, j| {
        const hi = decodeHexNibble(src[j * 2]) orelse return buf.clearRetainingCapacity();
        const lo = decodeHexNibble(src[j * 2 + 1]) orelse return buf.clearRetainingCapacity();
        b.* = (hi << 4) | lo;
    }
}

/// The bytes padded base64 spells into `buf`, left empty when it isn't that.
fn decodeBase64Into(allocator: Allocator, buf: *std.ArrayList(u8), src: []const u8) !void {
    const dec = std.base64.standard.Decoder;
    const n = dec.calcSizeForSlice(src) catch return;
    try buf.resize(allocator, n);
    dec.decode(buf.items, src) catch buf.clearRetainingCapacity();
}

/// FORMAT_BYTES(n): `n` bytes in the largest binary unit it reaches, with two
/// decimals past B (`1.50 KB`); NULL for a negative count, as in StarRocks.
pub fn formatBytesKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const counts = args[0].data.bigint;
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    for (0..row_count) |i| {
        buf.clearRetainingCapacity();
        const valid = args[0].isValid(i) and counts[i] >= 0;
        if (valid) try appendFormattedBytes(allocator, &buf, counts[i]);
        try ss.appendValue(allocator, buf.items);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

fn appendFormattedBytes(allocator: Allocator, buf: *std.ArrayList(u8), n: i64) !void {
    if (n < 1024) return buf.print(allocator, "{d} B", .{n});
    const units = [_][]const u8{ "KB", "MB", "GB", "TB", "PB", "EB" };
    var unit: usize = 0;
    var size: i64 = 1024;
    while (unit + 1 < units.len and @divTrunc(n, 1024) >= size) : (unit += 1) size *= 1024;
    const scaled = @as(f64, @floatFromInt(n)) / @as(f64, @floatFromInt(size));
    try buf.print(allocator, "{d:.2} {s}", .{ scaled, units[unit] });
}

/// BAR(size, min, max, width): `width` ▓ characters scaled by where `size`
/// falls between `min` and `max`, in whole characters. A size outside
/// [min, max], a min not below max or a negative width is
/// `InvalidBarArguments`; StarRocks requires the same.
pub fn barKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sizes = args[0].data.double;
    const mins = args[1].data.double;
    const maxes = args[2].data.double;
    const widths = args[3].data.bigint;
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    for (0..row_count) |i| {
        buf.clearRetainingCapacity();
        const valid = allValid(args, i);
        if (valid) {
            const size = sizes[i];
            const lo = mins[i];
            const hi = maxes[i];
            if (!(lo < hi and size >= lo and size <= hi and widths[i] >= 0)) return error.InvalidBarArguments;
            const width: f64 = @floatFromInt(widths[i]);
            const chars: usize = @intFromFloat(@floor((size - lo) * width / (hi - lo)));
            for (0..chars) |_| try buf.appendSlice(allocator, "▓");
        }
        try ss.appendValue(allocator, buf.items);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

/// EQUIWIDTH_BUCKET(value, min, max, buckets): StarRocks' bucket of `value`
/// among buckets of one whole width laid from `min`, counting from 0. The
/// width is (max - min) / buckets rounded down, or 1 where that is 0, so
/// values near `max` can land in bucket `buckets` or later. A min not below
/// max, a value outside [min, max] or a bucket count below 1 is
/// `InvalidEquiwidthBucketArguments`.
pub fn equiwidthBucketKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const values = args[0].data.bigint;
    const mins = args[1].data.bigint;
    const maxes = args[2].data.bigint;
    const bucket_counts = args[3].data.bigint;
    const base = out.data.rowCount();
    for (0..row_count) |i| {
        const valid = allValid(args, i);
        const bucket = if (valid) try equiwidthBucket(values[i], mins[i], maxes[i], bucket_counts[i]) else 0;
        try out.data.bigint.append(allocator, bucket);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

fn equiwidthBucket(value: i64, min: i64, max: i64, buckets: i64) !i64 {
    if (min >= max or value < min or value > max or buckets < 1) return error.InvalidEquiwidthBucketArguments;
    const width = @max(1, @divFloor(@as(i128, max) - min, buckets));
    // Only over a range wider than half of BIGINT's can the bucket pass
    // BIGINT, and there it wraps as BIGINT arithmetic does.
    return @truncate(@divFloor(@as(i128, value) - min, width));
}

// ---------------------------------------------------------------------------
// URLs
// ---------------------------------------------------------------------------

/// The parts of a URL the URL functions read, or null when `url` is none: it
/// needs a scheme, and RFC 3986 allows no space or control character in it.
const UrlParts = struct {
    /// Null when the URL has no `//` authority.
    host: ?[]const u8,
    /// What follows the `?`, short of any `#` fragment.
    query: ?[]const u8,

    /// A URL, or a relative reference such as `/doc?k=1` (RFC 3986 §4.1).
    /// Empty text is neither.
    fn parse(url: []const u8) ?UrlParts {
        if (url.len == 0) return null;
        for (url) |c| if (c <= ' ' or c == 0x7f) return null;
        const rest = url[afterScheme(url) orelse return null ..];
        const before_fragment = rest[0 .. std.mem.indexOfScalar(u8, rest, '#') orelse rest.len];
        const query_at = std.mem.indexOfScalar(u8, before_fragment, '?');
        const before_query = before_fragment[0 .. query_at orelse before_fragment.len];
        return .{
            .host = if (std.mem.startsWith(u8, before_query, "//")) hostOf(before_query[2..]) else null,
            .query = if (query_at) |q| before_fragment[q + 1 ..] else null,
        };
    }

    /// Where `url` goes on past its `scheme:`, 0 for a relative reference,
    /// whose first segment holds no ':'. Null when what stands before the
    /// first ':' is no scheme.
    fn afterScheme(url: []const u8) ?usize {
        const colon = std.mem.indexOfAny(u8, url, ":/?#") orelse return 0;
        if (url[colon] != ':') return 0;
        if (colon == 0 or !std.ascii.isAlphabetic(url[0])) return null;
        for (url[1..colon]) |c| if (!std.ascii.isAlphanumeric(c) and c != '+' and c != '-' and c != '.') return null;
        return colon + 1;
    }

    fn hostOf(after_slashes: []const u8) []const u8 {
        const authority = after_slashes[0 .. std.mem.indexOfScalar(u8, after_slashes, '/') orelse after_slashes.len];
        const host_port = authority[if (std.mem.lastIndexOfScalar(u8, authority, '@')) |at| at + 1 else 0..];
        if (std.mem.startsWith(u8, host_port, "[")) {
            const close = std.mem.indexOfScalar(u8, host_port, ']') orelse return host_port;
            return host_port[0 .. close + 1];
        }
        return host_port[0 .. std.mem.indexOfScalar(u8, host_port, ':') orelse host_port.len];
    }
};

/// URL_EXTRACT_HOST(url): the host of `url`'s authority, without user or
/// port; NULL when `url` is no URL or has no authority.
pub fn urlExtractHostKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const urls = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    for (0..row_count) |i| {
        const parts = if (args[0].isValid(i)) UrlParts.parse(urls.rowBytes(i)) else null;
        const host = if (parts) |u| u.host else null;
        try ss.appendValue(allocator, host orelse "");
        try out.appendValidBit(allocator, base + i, host != null);
    }
}

/// URL_EXTRACT_PARAMETER(url, name): the value of the first parameter of
/// `url`'s query called `name`, names and values read as a form encodes them
/// (`appendFormDecoded`); '' for a parameter with no `=`, NULL when there is
/// no such parameter or `url` is no URL.
pub fn urlExtractParameterKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const urls = stringViewOf(args[0]);
    const names = stringViewOf(args[1]);
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var name_buf: std.ArrayList(u8) = .empty;
    defer name_buf.deinit(allocator);
    var value_buf: std.ArrayList(u8) = .empty;
    defer value_buf.deinit(allocator);
    for (0..row_count) |i| {
        const value = if (allValid(args, i)) try queryParameter(allocator, urls.rowBytes(i), names.rowBytes(i), &name_buf, &value_buf) else null;
        try ss.appendValue(allocator, value orelse "");
        try out.appendValidBit(allocator, base + i, value != null);
    }
}

fn queryParameter(allocator: Allocator, url: []const u8, name: []const u8, name_buf: *std.ArrayList(u8), value_buf: *std.ArrayList(u8)) !?[]const u8 {
    const query = (UrlParts.parse(url) orelse return null).query orelse return null;
    var params = std.mem.splitScalar(u8, query, '&');
    while (params.next()) |param| {
        if (param.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, param, '=');
        name_buf.clearRetainingCapacity();
        try appendFormDecoded(allocator, name_buf, param[0 .. eq orelse param.len]);
        if (!std.mem.eql(u8, name_buf.items, name)) continue;
        value_buf.clearRetainingCapacity();
        if (eq) |e| try appendFormDecoded(allocator, value_buf, param[e + 1 ..]);
        return value_buf.items;
    }
    return null;
}

/// URL_ENCODE(s): `s` in the application/x-www-form-urlencoded format:
/// letters, digits and `*-._` as they are, a space as `+`, and every other
/// byte as `%XX`.
pub fn urlEncodeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    for (0..row_count) |i| {
        buf.clearRetainingCapacity();
        for (sv.rowBytes(i)) |c| {
            if (std.ascii.isAlphanumeric(c) or c == '*' or c == '-' or c == '.' or c == '_') {
                try buf.append(allocator, c);
            } else if (c == ' ') {
                try buf.append(allocator, '+');
            } else {
                try buf.appendSlice(allocator, &.{ '%', "0123456789ABCDEF"[c >> 4], "0123456789ABCDEF"[c & 0x0F] });
            }
        }
        try ss.appendValue(allocator, buf.items);
    }
}

/// URL_DECODE(s): `s` read back from the application/x-www-form-urlencoded
/// format (`appendFormDecoded`).
pub fn urlDecodeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    for (0..row_count) |i| {
        buf.clearRetainingCapacity();
        try appendFormDecoded(allocator, &buf, sv.rowBytes(i));
        try ss.appendValue(allocator, buf.items);
    }
}

/// `text` with each `%XX` escape read as its byte and `+` as a space, as a
/// form encodes them. A `%` without two hex digits after it stays a `%`.
fn appendFormDecoded(allocator: Allocator, out: *std.ArrayList(u8), text: []const u8) !void {
    var i: usize = 0;
    while (i < text.len) : (i += 1) {
        const escaped: ?u8 = if (text[i] == '%' and i + 2 < text.len) hexByte(text[i + 1], text[i + 2]) else null;
        if (escaped) |b| {
            try out.append(allocator, b);
            i += 2;
        } else {
            try out.append(allocator, if (text[i] == '+') ' ' else text[i]);
        }
    }
}

fn hexByte(hi: u8, lo: u8) ?u8 {
    return (decodeHexNibble(hi) orelse return null) << 4 | (decodeHexNibble(lo) orelse return null);
}

// ---------------------------------------------------------------------------
// N-gram similarity
// ---------------------------------------------------------------------------

/// NGRAM_SEARCH(haystack, needle, n) and its case-insensitive twin: how much
/// of the needle the haystack holds, as the share of the needle's `n`-byte
/// grams the haystack has too, each haystack gram standing for at most one
/// of them. 1 when it has them all; 0 when the needle has none (it's shorter
/// than `n`). Computed in single precision as in StarRocks, so the values
/// match it to the digit. Case folding is ASCII only.
pub fn NgramSearch(comptime fold_case: bool) type {
    return struct {
        pub fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const haystacks = stringViewOf(args[0]);
            const needles = stringViewOf(args[1]);
            const base = out.data.rowCount();
            var needle: NgramNeedle = .{};
            defer needle.deinit(allocator);
            var folded: std.ArrayList(u8) = .empty;
            defer folded.deinit(allocator);
            for (0..row_count) |i| {
                const valid = allValid(args, i);
                var similarity: f32 = 0;
                if (valid) {
                    const n = args[2].data.int[i];
                    if (n <= 0) return error.InvalidNgramLength;
                    try needle.set(allocator, needles.rowBytes(i), @intCast(n), fold_case);
                    var haystack = haystacks.rowBytes(i);
                    if (fold_case) {
                        folded.clearRetainingCapacity();
                        try folded.appendSlice(allocator, haystack);
                        foldBytes(folded.items, true);
                        haystack = folded.items;
                    }
                    similarity = try needle.similarity(allocator, haystack);
                }
                try out.data.double.append(allocator, similarity);
                try out.appendValidBit(allocator, base + i, valid);
            }
        }
    };
}

/// A needle's distinct grams and how often each occurs, kept between rows so
/// a constant needle is cut into grams once per batch.
const NgramNeedle = struct {
    text: std.ArrayList(u8) = .empty,
    n: usize = 0,
    /// Gram → its place in `counts`; the keys point into `text`.
    ids: std.StringHashMapUnmanaged(u32) = .empty,
    counts: std.ArrayList(u32) = .empty,
    left: std.ArrayList(u32) = .empty,
    grams: usize = 0,

    fn deinit(self: *NgramNeedle, allocator: Allocator) void {
        self.text.deinit(allocator);
        self.ids.deinit(allocator);
        self.counts.deinit(allocator);
        self.left.deinit(allocator);
    }

    fn set(self: *NgramNeedle, allocator: Allocator, raw: []const u8, n: usize, fold_case: bool) !void {
        if (self.n == n and self.text.items.len == raw.len and (if (fold_case) std.ascii.eqlIgnoreCase(self.text.items, raw) else std.mem.eql(u8, self.text.items, raw))) return;
        self.ids.clearRetainingCapacity();
        self.counts.clearRetainingCapacity();
        self.text.clearRetainingCapacity();
        try self.text.appendSlice(allocator, raw);
        if (fold_case) foldBytes(self.text.items, true);
        self.n = n;
        self.grams = if (raw.len >= n) raw.len - n + 1 else 0;
        for (0..self.grams) |at| {
            const entry = try self.ids.getOrPut(allocator, self.text.items[at..][0..n]);
            if (!entry.found_existing) {
                entry.value_ptr.* = @intCast(self.counts.items.len);
                try self.counts.append(allocator, 0);
            }
            self.counts.items[entry.value_ptr.*] += 1;
        }
    }

    fn similarity(self: *NgramNeedle, allocator: Allocator, haystack: []const u8) !f32 {
        if (self.grams == 0 or haystack.len < self.n) return 0;
        self.left.clearRetainingCapacity();
        try self.left.appendSlice(allocator, self.counts.items);
        var found: usize = 0;
        for (0..haystack.len - self.n + 1) |at| {
            const id = self.ids.get(haystack[at..][0..self.n]) orelse continue;
            if (self.left.items[id] == 0) continue;
            self.left.items[id] -= 1;
            found += 1;
        }
        const missed: f32 = @floatFromInt(self.grams - found);
        return 1 - missed / @as(f32, @floatFromInt(self.grams));
    }
};

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
