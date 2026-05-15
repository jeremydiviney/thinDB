//! In-memory DEFLATE compress/decompress, used by the segment writer/reader
//! for column block compression.
//!
//! We use the raw DEFLATE container (no zlib/gzip header) to save a few bytes
//! per block. zstd isn't available in stdlib 0.16 for compression yet, so
//! flate is the pragmatic choice with no external deps.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const flate = std.compress.flate;

/// Default DEFLATE compression level. Level 6 is the GZIP/zlib default —
/// a good speed/ratio balance.
pub const default_level: flate.Compress.Options = flate.Compress.Options.default;

/// Compress `input` into a freshly-allocated byte slice. Caller owns the
/// returned slice and must `allocator.free` it.
pub fn compress(allocator: Allocator, input: []const u8) ![]u8 {
    // Allocating writer needs a non-empty buffer for flate.Compress.init
    // (which asserts `output.buffer.len > 8`). Seed with a small capacity;
    // it grows as needed.
    var out: Io.Writer.Allocating = try .initCapacity(allocator, @max(64, input.len / 4 + 64));
    defer out.deinit();

    // flate.Compress also needs a working buffer at least `flate.max_window_len`
    // bytes (its lookahead/hash table is built on top of this).
    var lookahead: [flate.max_window_len]u8 = undefined;
    var comp = try flate.Compress.init(&out.writer, &lookahead, .raw, default_level);
    try comp.writer.writeAll(input);
    try comp.finish();

    // Move the bytes out of the Allocating writer.
    var list = out.toArrayList();
    return list.toOwnedSlice(allocator);
}

/// Decompress `input` (raw DEFLATE) and return a freshly-allocated slice of
/// exactly `uncompressed_size` bytes. Caller owns the returned slice.
pub fn decompress(allocator: Allocator, input: []const u8, uncompressed_size: usize) ![]u8 {
    var in_reader = Io.Reader.fixed(input);

    var window: [flate.max_window_len]u8 = undefined;
    var dec = flate.Decompress.init(&in_reader, .raw, &window);

    const out = try allocator.alloc(u8, uncompressed_size);
    errdefer allocator.free(out);

    try dec.reader.readSliceAll(out);
    return out;
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
