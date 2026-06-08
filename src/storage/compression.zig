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

/// Reusable zstd compression context. A CCtx allocates ~1-2 MB of internal
/// hash tables, FSE/Huffman state, and staging buffers; reusing one across
/// many small compress calls (e.g. one per column block in a flush) avoids
/// paying that setup cost on every call.
///
/// Not thread-safe — give each thread its own Compressor.
pub const Compressor = struct {
    cctx: *c.ZSTD_CCtx,

    pub fn init() !Compressor {
        const cctx = c.ZSTD_createCCtx() orelse return Error.OutOfMemory;
        return .{ .cctx = cctx };
    }

    pub fn deinit(self: *Compressor) void {
        _ = c.ZSTD_freeCCtx(self.cctx);
        self.* = undefined;
    }

    /// Compress `input` into a freshly-allocated byte slice using this
    /// context's persistent state. Caller owns the returned slice.
    pub fn compress(self: *Compressor, allocator: Allocator, input: []const u8) ![]u8 {
        const bound = c.ZSTD_compressBound(input.len);
        const dst = try allocator.alloc(u8, bound);
        errdefer allocator.free(dst);

        const written = c.ZSTD_compressCCtx(
            self.cctx,
            dst.ptr,
            dst.len,
            input.ptr,
            input.len,
            default_level,
        );
        if (c.ZSTD_isError(written) != 0) return Error.ZstdEncodeFailed;

        return allocator.realloc(dst, written) catch dst[0..written];
    }
};

/// One-shot compress: convenience wrapper around `Compressor.compress` that
/// creates a fresh CCtx for a single call. Use `Compressor` directly in hot
/// paths that compress many blocks back-to-back.
pub fn compress(allocator: Allocator, input: []const u8) ![]u8 {
    var ctx = try Compressor.init();
    defer ctx.deinit();
    return ctx.compress(allocator, input);
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

/// Like `decompress` but the result buffer is 16-byte aligned. The row-group
/// cache stores blocks through this so the scan can build zero-copy typed views
/// (string offset arrays, fixed-width columns) directly over the cached bytes —
/// an arbitrary-alignment buffer forces a redundant owned decode per row group.
pub fn decompressAligned(allocator: Allocator, input: []const u8, uncompressed_size: usize) ![]align(16) u8 {
    const dst = try allocator.alignedAlloc(u8, .@"16", uncompressed_size);
    errdefer allocator.free(dst);

    const written = c.ZSTD_decompress(dst.ptr, dst.len, input.ptr, input.len);
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
