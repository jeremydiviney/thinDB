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

const store = @import("../engine/store.zig");
const regex = @import("../util/regex.zig");

// ---------------------------------------------------------------------------
// Core string kernels (upper, lower, length, trims, reverse, concat,
// substring, replace). The lengthKernel is also registered as octet_length
// and char_length aliases — same kernel, different name in builtins[].
// ---------------------------------------------------------------------------

/// REGEXP_REPLACE(haystack, pattern, replacement). Pattern + replacement
/// are read from row 0 and the regex compiled once per batch — i.e. they
/// must be constant across the batch (the usual case: SQL literals). The
/// replacement may use `\N` capture backrefs. Backed by the linear-time
/// engine in util/regex.zig; unsupported regex features (lookaround,
/// in-pattern backrefs) or malformed patterns surface as
/// RegexInvalidPattern.
pub fn regexpReplaceKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    if (row_count == 0) return;
    const pattern = stringViewOf(args[1]).rowBytes(0);
    const replacement = stringViewOf(args[2]).rowBytes(0);
    var re = try regex.Regex.compile(allocator, pattern);
    defer re.deinit();
    const sv = stringViewOf(args[0]);
    // Matcher scratch reused across every row: the visited array, thread
    // lists, capture arena, and slot buffers all persist between rows, so
    // applying one pattern to a whole batch allocates ~nothing per row.
    var scratch = regex.Scratch.init(allocator);
    defer scratch.deinit();
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        // `replaced` is borrowed from the scratch's reused output buffer;
        // appendValue copies it into the column store, so no free per row.
        const replaced = try re.replaceAllScratch(sv.rowBytes(i), replacement, &scratch);
        try store.StringStore.appendValue(stringStoreOf(out), allocator, replaced);
    }
}

pub fn upperKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const dst = try allocator.alloc(u8, src.len);
        defer allocator.free(dst);
        for (src, dst) |b, *d| d.* = std.ascii.toUpper(b);
        try store.StringStore.appendValue(stringStoreOf(out), allocator, dst);
    }
}

pub fn lowerKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const dst = try allocator.alloc(u8, src.len);
        defer allocator.free(dst);
        for (src, dst) |b, *d| d.* = std.ascii.toLower(b);
        try store.StringStore.appendValue(stringStoreOf(out), allocator, dst);
    }
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
        var n: usize = 0;
        for (sv.rowBytes(i)) |b| {
            if (b & 0xC0 != 0x80) n += 1;
        }
        try out.data.int.append(allocator, @intCast(n));
    }
}

pub fn ltrimKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        var start: usize = 0;
        while (start < src.len and std.ascii.isWhitespace(src[start])) : (start += 1) {}
        try ss.appendValue(allocator, src[start..]);
    }
}

pub fn rtrimKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        var end: usize = src.len;
        while (end > 0 and std.ascii.isWhitespace(src[end - 1])) : (end -= 1) {}
        try ss.appendValue(allocator, src[0..end]);
    }
}

pub fn trimKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        var start: usize = 0;
        while (start < src.len and std.ascii.isWhitespace(src[start])) : (start += 1) {}
        var end: usize = src.len;
        while (end > start and std.ascii.isWhitespace(src[end - 1])) : (end -= 1) {}
        try ss.appendValue(allocator, src[start..end]);
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
        for (src, 0..) |b, j| dst[src.len - 1 - j] = b;
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

/// MySQL-style substring: 1-indexed start; negative start counts from
/// end; length < 0 → empty string. Out-of-range returns empty string
/// rather than erroring (matches MySQL).
pub fn substringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const starts = args[1].data.int;
    const lens = args[2].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const start_raw = starts[i];
        const len_raw = lens[i];
        if (len_raw <= 0 or src.len == 0) {
            try ss.appendValue(allocator, "");
            continue;
        }
        const src_len_i: i64 = @intCast(src.len);
        var start_0: i64 = if (start_raw > 0)
            @as(i64, start_raw) - 1
        else if (start_raw < 0)
            src_len_i + @as(i64, start_raw)
        else
            0;
        if (start_0 < 0) start_0 = 0;
        if (start_0 >= src_len_i) {
            try ss.appendValue(allocator, "");
            continue;
        }
        const end_0 = @min(start_0 + @as(i64, len_raw), src_len_i);
        const start_u: usize = @intCast(start_0);
        const end_u: usize = @intCast(end_0);
        try ss.appendValue(allocator, src[start_u..end_u]);
    }
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

// ---------------------------------------------------------------------------
// Encoding kernels — hex / base64 round-trips on string columns.
// ---------------------------------------------------------------------------

pub fn hexEncodeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const dst = try allocator.alloc(u8, src.len * 2);
        defer allocator.free(dst);
        const charset = "0123456789abcdef";
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
// Expanded string kernels (DuckDB / MySQL / StarRocks parity additions):
// lpad, rpad, repeat, space, ascii, position, instr, substring_index,
// strcmp, greatest/least over strings, chr.
// ---------------------------------------------------------------------------

pub fn lpadKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const lens = args[1].data.int;
    const pad_sv = stringViewOf(args[2]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const target_len_i32 = lens[i];
        const pad = pad_sv.rowBytes(i);
        if (target_len_i32 <= 0 or pad.len == 0) {
            try ss.appendValue(allocator, src[0..@min(src.len, @as(usize, @intCast(@max(target_len_i32, 0))))]);
            continue;
        }
        const target_len: usize = @intCast(target_len_i32);
        if (src.len >= target_len) {
            try ss.appendValue(allocator, src[0..target_len]);
            continue;
        }
        var buf = try allocator.alloc(u8, target_len);
        defer allocator.free(buf);
        const pad_needed = target_len - src.len;
        var written: usize = 0;
        while (written < pad_needed) : (written += 1) buf[written] = pad[written % pad.len];
        @memcpy(buf[pad_needed..], src);
        try ss.appendValue(allocator, buf);
    }
}

pub fn rpadKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const lens = args[1].data.int;
    const pad_sv = stringViewOf(args[2]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const target_len_i32 = lens[i];
        const pad = pad_sv.rowBytes(i);
        if (target_len_i32 <= 0 or pad.len == 0) {
            try ss.appendValue(allocator, src[0..@min(src.len, @as(usize, @intCast(@max(target_len_i32, 0))))]);
            continue;
        }
        const target_len: usize = @intCast(target_len_i32);
        if (src.len >= target_len) {
            try ss.appendValue(allocator, src[0..target_len]);
            continue;
        }
        var buf = try allocator.alloc(u8, target_len);
        defer allocator.free(buf);
        @memcpy(buf[0..src.len], src);
        var j: usize = src.len;
        while (j < target_len) : (j += 1) buf[j] = pad[(j - src.len) % pad.len];
        try ss.appendValue(allocator, buf);
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

/// 1-based offset of `needle` in `haystack`, or 0 if absent. Matches MySQL /
/// StarRocks / DuckDB. An empty needle returns 1 (consistent with most engines).
pub fn positionKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const needle_sv = stringViewOf(args[0]);
    const hay_sv = stringViewOf(args[1]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const needle = needle_sv.rowBytes(i);
        const hay = hay_sv.rowBytes(i);
        const v: i32 = if (needle.len == 0) 1 else if (std.mem.indexOf(u8, hay, needle)) |idx| @intCast(idx + 1) else 0;
        try out.data.int.append(allocator, v);
    }
}

/// MySQL's INSTR(haystack, needle). Same semantics as position; args swapped.
pub fn instrKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const hay_sv = stringViewOf(args[0]);
    const needle_sv = stringViewOf(args[1]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const hay = hay_sv.rowBytes(i);
        const needle = needle_sv.rowBytes(i);
        const v: i32 = if (needle.len == 0) 1 else if (std.mem.indexOf(u8, hay, needle)) |idx| @intCast(idx + 1) else 0;
        try out.data.int.append(allocator, v);
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

pub fn greatestStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a_sv = stringViewOf(args[0]);
    const b_sv = stringViewOf(args[1]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const a = a_sv.rowBytes(i);
        const b = b_sv.rowBytes(i);
        try ss.appendValue(allocator, if (std.mem.order(u8, a, b) == .lt) b else a);
    }
}

pub fn leastStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a_sv = stringViewOf(args[0]);
    const b_sv = stringViewOf(args[1]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const a = a_sv.rowBytes(i);
        const b = b_sv.rowBytes(i);
        try ss.appendValue(allocator, if (std.mem.order(u8, a, b) == .gt) b else a);
    }
}

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
