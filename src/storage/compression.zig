//! In-memory zstd compress/decompress, used by the segment writer/reader for
//! column-block compression. Calls into the vendored libzstd C library via
//! `@cImport`.

const std = @import("std");
const Allocator = std.mem.Allocator;

const c = @cImport({
    @cInclude("zstd.h");
});

/// Default zstd compression level. 3 is the upstream default — Pareto-optimal
/// speed/ratio for analytics-shaped data. Range is 1 (fastest) to 22.
pub const default_level: c_int = 3;

pub const Error = error{
    ZstdEncodeFailed,
    ZstdDecodeFailed,
    OutOfMemory,
};

/// Compress `input` into a freshly-allocated byte slice. Caller owns the
/// returned slice and must `allocator.free` it.
pub fn compress(allocator: Allocator, input: []const u8) ![]u8 {
    const bound = c.ZSTD_compressBound(input.len);
    const dst = try allocator.alloc(u8, bound);
    errdefer allocator.free(dst);

    const written = c.ZSTD_compress(
        dst.ptr,
        dst.len,
        input.ptr,
        input.len,
        default_level,
    );
    if (c.ZSTD_isError(written) != 0) return Error.ZstdEncodeFailed;

    return allocator.realloc(dst, written) catch dst[0..written];
}

/// Decompress `input` and return a freshly-allocated slice of exactly
/// `uncompressed_size` bytes. Caller owns the returned slice.
pub fn decompress(allocator: Allocator, input: []const u8, uncompressed_size: usize) ![]u8 {
    const dst = try allocator.alloc(u8, uncompressed_size);
    errdefer allocator.free(dst);

    const written = c.ZSTD_decompress(
        dst.ptr,
        dst.len,
        input.ptr,
        input.len,
    );
    if (c.ZSTD_isError(written) != 0) return Error.ZstdDecodeFailed;
    if (written != uncompressed_size) return Error.ZstdDecodeFailed;

    return dst;
}

// ---------- tests --------------------------------------------------------

test "compress + decompress round-trip — simple bytes" {
    const allocator = std.testing.allocator;
    const payload = "hello world from thindb's compression layer; the quick brown fox jumps over the lazy dog";

    const compressed = try compress(allocator, payload);
    defer allocator.free(compressed);
    try std.testing.expect(compressed.len > 0);

    const decompressed = try decompress(allocator, compressed, payload.len);
    defer allocator.free(decompressed);
    try std.testing.expectEqualStrings(payload, decompressed);
}

test "compress + decompress on a 1KB redundant buffer ~10x ratio" {
    const allocator = std.testing.allocator;
    const payload = "aaaaaaaaaa" ** 100; // 1000 bytes of 'a' — very compressible

    const compressed = try compress(allocator, payload);
    defer allocator.free(compressed);

    try std.testing.expect(compressed.len < payload.len / 10);

    const decompressed = try decompress(allocator, compressed, payload.len);
    defer allocator.free(decompressed);
    try std.testing.expectEqualSlices(u8, payload, decompressed);
}

test "compress + decompress on column-shaped data (i64 bytes)" {
    const allocator = std.testing.allocator;

    // Monotonic i64 values — strongly compressible.
    var raw: [8192]u8 = undefined;
    var i: usize = 0;
    while (i < 1024) : (i += 1) {
        std.mem.writeInt(i64, raw[i * 8 ..][0..8], @intCast(i), .little);
    }

    const compressed = try compress(allocator, &raw);
    defer allocator.free(compressed);

    // 1024 increasing i64s should compress dramatically.
    try std.testing.expect(compressed.len < raw.len / 3);

    const decompressed = try decompress(allocator, compressed, raw.len);
    defer allocator.free(decompressed);
    try std.testing.expectEqualSlices(u8, &raw, decompressed);
}
