//! The `memset` that every binary linking thinDB uses.
//!
//! Zig 0.16's compiler_rt `memset` stores one byte per loop iteration, and
//! being a static definition it also wins the link over libc's, on Linux
//! with glibc as on Windows. Each runtime-length `@memset` in the engine —
//! hash-table clears, validity bitmaps, scratch buffers — ran at a fraction
//! of store bandwidth: 4 KiB took 580 ns instead of 72 ns on a Zen 3 core.
//! This strong definition takes its place.
//!
//! The module is built with `no_builtin`, so LLVM cannot recognize the store
//! loops below as a memset and compile them into a call to this function.

const std = @import("std");

comptime {
    @export(&memset, .{ .name = "memset", .linkage = .strong });
}

const Chunk = @Vector(32, u8);
const Half = @Vector(16, u8);

fn memset(dest: ?[*]u8, c: u8, len: usize) callconv(.c) ?[*]u8 {
    @setRuntimeSafety(false);
    const d = dest orelse return dest;
    // Below one chunk, two overlapping stores of the widest fitting size
    // cover any length without a loop.
    if (len < 4) {
        for (0..len) |i| d[i] = c;
    } else if (len < 8) {
        const word = @as(u32, c) * 0x01010101;
        store(u32, d, word);
        store(u32, d + len - 4, word);
    } else if (len < 16) {
        const word = @as(u64, c) * 0x0101010101010101;
        store(u64, d, word);
        store(u64, d + len - 8, word);
    } else if (len < 32) {
        const half: Half = @splat(c);
        store(Half, d, half);
        store(Half, d + len - 16, half);
    } else {
        const chunk: Chunk = @splat(c);
        var i: usize = 0;
        while (i + 32 <= len) : (i += 32) store(Chunk, d + i, chunk);
        if (i < len) store(Chunk, d + len - 32, chunk);
    }
    return dest;
}

inline fn store(comptime T: type, p: [*]u8, value: T) void {
    @as(*align(1) T, @ptrCast(p)).* = value;
}

test "memset fills every length and alignment exactly" {
    var buf: [200]u8 = undefined;
    for (0..8) |offset| {
        for (0..150) |len| {
            @memset(&buf, 0xAA);
            _ = memset(buf[offset..].ptr, 0x5C, len);
            for (buf, 0..) |b, i| {
                const inside = i >= offset and i < offset + len;
                try std.testing.expectEqual(@as(u8, if (inside) 0x5C else 0xAA), b);
            }
        }
    }
}
