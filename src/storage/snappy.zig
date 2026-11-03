//! Snappy block decompression: the raw format (no framing) Parquet pages
//! compress with.
//!
//! A block is the uncompressed length as a varint, then elements: a literal
//! run, or a copy of earlier output at a 1-, 2- or 4-byte offset.

const std = @import("std");

pub const Error = error{CorruptSnappy};

/// The uncompressed length `src` declares, and where its elements start.
pub fn header(src: []const u8) Error!struct { len: usize, body: usize } {
    var len: u64 = 0;
    var shift: u6 = 0;
    for (src, 0..) |b, i| {
        if (i == 5) break;
        len |= @as(u64, b & 0x7f) << shift;
        if (b & 0x80 == 0) return .{ .len = @intCast(len), .body = i + 1 };
        shift += 7;
    }
    return error.CorruptSnappy;
}

/// Decompresses `src` into `dst`, which must be exactly the length
/// `header(src)` declares.
pub fn decompress(src: []const u8, dst: []u8) Error!void {
    const h = try header(src);
    if (h.len != dst.len) return error.CorruptSnappy;
    var s = h.body;
    var d: usize = 0;
    while (s < src.len) {
        const tag = src[s];
        s += 1;
        switch (@as(u2, @truncate(tag))) {
            0 => {
                var len: usize = tag >> 2;
                if (len >= 60) {
                    const n = len - 59;
                    if (n > src.len - s) return error.CorruptSnappy;
                    len = 0;
                    for (src[s..][0..n], 0..) |b, i| len |= @as(usize, b) << @intCast(8 * i);
                    s += n;
                }
                len += 1;
                if (len > src.len - s or len > dst.len - d) return error.CorruptSnappy;
                @memcpy(dst[d..][0..len], src[s..][0..len]);
                s += len;
                d += len;
            },
            1 => {
                if (s >= src.len) return error.CorruptSnappy;
                const len: usize = 4 + ((tag >> 2) & 7);
                try copyMatch(dst, d, (@as(usize, tag >> 5) << 8) | src[s], len);
                s += 1;
                d += len;
            },
            2 => {
                if (2 > src.len - s) return error.CorruptSnappy;
                const len: usize = 1 + @as(usize, tag >> 2);
                try copyMatch(dst, d, std.mem.readInt(u16, src[s..][0..2], .little), len);
                s += 2;
                d += len;
            },
            3 => {
                if (4 > src.len - s) return error.CorruptSnappy;
                const len: usize = 1 + @as(usize, tag >> 2);
                try copyMatch(dst, d, std.mem.readInt(u32, src[s..][0..4], .little), len);
                s += 4;
                d += len;
            },
        }
    }
    if (d != dst.len) return error.CorruptSnappy;
}

/// Appends `len` bytes copied from `offset` bytes back. A copy longer than
/// its offset repeats the bytes it has just written, so it goes in steps of
/// at most `offset` bytes, each reading only bytes already in place.
fn copyMatch(dst: []u8, d: usize, offset: usize, len: usize) Error!void {
    if (offset == 0 or offset > d or len > dst.len - d) return error.CorruptSnappy;
    const from = d - offset;
    if (offset >= len) {
        @memcpy(dst[d..][0..len], dst[from..][0..len]);
        return;
    }
    if (offset == 1) {
        @memset(dst[d..][0..len], dst[from]);
        return;
    }
    var i: usize = 0;
    while (i < len) {
        const n = @min(len - i, offset);
        @memcpy(dst[d + i ..][0..n], dst[from + i ..][0..n]);
        i += n;
    }
}

test "literals and every copy form decode" {
    // "abcdabcdabcdX" + 70 x 'z': literal "abcd", a 1-byte-offset copy of 8,
    // literal "X", literal "z", a 1-byte-offset run of 'z' (offset 1), and a
    // 2-byte- and 4-byte-offset copy of "abcd".
    const src = [_]u8{
        91, // varint 91
        (4 - 1) << 2,
        'a',
        'b',
        'c',
        'd',
        1 | ((8 - 4) << 2), 4, // copy len 8, offset 4
        0,                  'X',
        0,                  'z',
        1 | ((11 - 4) << 2), 1, // copy len 11, offset 1
        2 | ((58 - 1) << 2), 1, 0, // copy len 58, offset 1 (2-byte form)
        2 | ((4 - 1) << 2), 83, 0, // copy len 4, offset 83 -> "abcd"
        3 | ((4 - 1) << 2), 4, 0, 0, 0, // copy len 4, offset 4 (4-byte form)
    };
    var want: [91]u8 = undefined;
    @memcpy(want[0..13], "abcdabcdabcdX");
    @memset(want[13..83], 'z');
    @memcpy(want[83..91], "abcdabcd");
    var out: [91]u8 = undefined;
    try decompress(&src, &out);
    try std.testing.expectEqualSlices(u8, &want, &out);
}

test "a long literal takes its length from the following bytes" {
    // Varint 300, then a literal whose length - 1 (299) is in 2 bytes.
    var block: [5 + 300]u8 = undefined;
    block[0..5].* = .{ 0xac, 0x02, 61 << 2, 299 & 0xff, 299 >> 8 };
    for (block[5..], 0..) |*b, i| b.* = @truncate(i);
    var out: [300]u8 = undefined;
    try decompress(&block, &out);
    try std.testing.expectEqualSlices(u8, block[5..], &out);
}

test "corrupt blocks are errors" {
    var out: [8]u8 = undefined;
    // Declared length differs from the buffer.
    try std.testing.expectError(error.CorruptSnappy, decompress(&.{ 9, 0, 'a' }, &out));
    // Copy before any output.
    try std.testing.expectError(error.CorruptSnappy, decompress(&.{ 8, 1 | (4 << 2), 1 }, &out));
    // Literal running past the input.
    try std.testing.expectError(error.CorruptSnappy, decompress(&.{ 8, 7 << 2, 'a' }, &out));
    // Output shorter than declared.
    try std.testing.expectError(error.CorruptSnappy, decompress(&.{ 8, 0, 'a' }, &out));
}
