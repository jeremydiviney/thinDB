//! Shared SIMD reduction kernels over contiguous column slices. Used by the
//! aggregate operator's no-null fast paths today, and by the window operator's
//! materialized partitions later (#272). Loops are generic over the `@Vector`
//! width — the compiler picks the lane count for the target — with a scalar
//! tail for the remainder.

const std = @import("std");

fn lanes(comptime T: type) comptime_int {
    return std.simd.suggestVectorLength(T) orelse @max(1, 16 / @sizeOf(T));
}

/// Sum a contiguous slice into `i128`, accumulating through `i64` lanes. The
/// widening is lossless for integer `T` up to 32 bits, and an `i64` lane can't
/// overflow at thinDB's scale (`i32` max × 4e9 rows < `i64` max), so callers
/// restrict `T` to `i8`/`i16`/`i32`/`u8` and keep 64-bit-plus inputs on the
/// scalar `i128` path.
pub fn sumWiden(comptime T: type, data: []const T) i128 {
    comptime std.debug.assert(@bitSizeOf(T) <= 32);
    const N = comptime lanes(i64);
    var acc: @Vector(N, i64) = @splat(0);
    var i: usize = 0;
    while (i + N <= data.len) : (i += N) {
        const chunk: @Vector(N, T) = data[i..][0..N].*;
        acc += @as(@Vector(N, i64), chunk);
    }
    var total: i128 = @reduce(.Add, acc);
    while (i < data.len) : (i += 1) total += data[i];
    return total;
}

/// Sum a contiguous float slice into `f64` via `f64` lanes. Note: SIMD changes
/// the addition order vs a left-to-right scalar sum, so the low bits can differ
/// — fine for SUM/AVG semantics, but callers needing bit-identical reproduction
/// of the scalar order should not use this.
pub fn sumFloat(comptime T: type, data: []const T) f64 {
    const N = comptime lanes(f64);
    var acc: @Vector(N, f64) = @splat(0);
    var i: usize = 0;
    while (i + N <= data.len) : (i += N) {
        const chunk: @Vector(N, T) = data[i..][0..N].*;
        acc += @as(@Vector(N, f64), chunk);
    }
    var total: f64 = @reduce(.Add, acc);
    while (i < data.len) : (i += 1) total += data[i];
    return total;
}

/// SIMD min (`is_min`) or max of a non-empty contiguous slice. Works natively
/// for any integer/float `T` (no widening — min/max can't overflow). 128-bit
/// `T` degenerates to the scalar loop where the target has no wide vector.
fn reduceExtreme(comptime T: type, data: []const T, comptime is_min: bool) T {
    std.debug.assert(data.len > 0);
    const N = comptime lanes(T);
    if (N <= 1 or data.len < N) {
        var m = data[0];
        for (data[1..]) |v| m = if (is_min) @min(m, v) else @max(m, v);
        return m;
    }
    var acc: @Vector(N, T) = data[0..N].*;
    var i: usize = N;
    while (i + N <= data.len) : (i += N) {
        const chunk: @Vector(N, T) = data[i..][0..N].*;
        acc = if (is_min) @min(acc, chunk) else @max(acc, chunk);
    }
    var m: T = if (is_min) @reduce(.Min, acc) else @reduce(.Max, acc);
    while (i < data.len) : (i += 1) m = if (is_min) @min(m, data[i]) else @max(m, data[i]);
    return m;
}

pub fn minOf(comptime T: type, data: []const T) T {
    return reduceExtreme(T, data, true);
}

pub fn maxOf(comptime T: type, data: []const T) T {
    return reduceExtreme(T, data, false);
}

pub const CmpOp = enum { eq, neq, lt, lte, gt, gte };

/// Elementwise `mask[i] = (data[i] <op> want)`, vectorized. `mask` must be
/// sized to `data.len`. `op` is comptime so the vector comparison monomorphizes
/// to a single SIMD compare per chunk; 128-bit `T` (no wide vector) degenerates
/// to the scalar tail. Used by the Filter's per-leaf comparison kernel.
pub fn compareInto(comptime T: type, comptime op: CmpOp, data: []const T, want: T, mask: []bool) void {
    std.debug.assert(data.len == mask.len);
    const N = comptime lanes(T);
    var i: usize = 0;
    if (N > 1) {
        const wv: @Vector(N, T) = @splat(want);
        while (i + N <= data.len) : (i += N) {
            const v: @Vector(N, T) = data[i..][0..N].*;
            const m: @Vector(N, bool) = switch (op) {
                .eq => v == wv,
                .neq => v != wv,
                .lt => v < wv,
                .lte => v <= wv,
                .gt => v > wv,
                .gte => v >= wv,
            };
            mask[i..][0..N].* = m;
        }
    }
    while (i < data.len) : (i += 1) {
        const v = data[i];
        mask[i] = switch (op) {
            .eq => v == want,
            .neq => v != want,
            .lt => v < want,
            .lte => v <= want,
            .gt => v > want,
            .gte => v >= want,
        };
    }
}

pub const BinOp = enum { add, sub, mul };

/// Elementwise `dst[i] = a[i] <op> b[i]` over equal-length slices, vectorized.
/// Integer ops wrap (matching the scalar `+%`/`-%`/`*%` kernels); float ops are
/// plain IEEE. `dst` must already be sized to `a.len`.
pub fn binInto(comptime T: type, comptime op: BinOp, a: []const T, b: []const T, dst: []T) void {
    std.debug.assert(a.len == b.len and a.len == dst.len);
    const is_int = @typeInfo(T) == .int;
    const N = comptime lanes(T);
    var i: usize = 0;
    if (N > 1) {
        while (i + N <= dst.len) : (i += N) {
            const va: @Vector(N, T) = a[i..][0..N].*;
            const vb: @Vector(N, T) = b[i..][0..N].*;
            const vr: @Vector(N, T) = if (is_int) switch (op) {
                .add => va +% vb,
                .sub => va -% vb,
                .mul => va *% vb,
            } else switch (op) {
                .add => va + vb,
                .sub => va - vb,
                .mul => va * vb,
            };
            dst[i..][0..N].* = vr;
        }
    }
    while (i < dst.len) : (i += 1) {
        dst[i] = if (is_int) switch (op) {
            .add => a[i] +% b[i],
            .sub => a[i] -% b[i],
            .mul => a[i] *% b[i],
        } else switch (op) {
            .add => a[i] + b[i],
            .sub => a[i] - b[i],
            .mul => a[i] * b[i],
        };
    }
}

/// Fused `dst[i] = widen(src[i]) <op> scalar` (or `scalar <op> widen(src[i])`
/// when `col_left` is false), in one vectorized pass. `Tsrc` widens to `Tout`
/// (same kind: int→int or float→float); integer ops wrap. Lets `col + const`
/// skip both the smallint→int cast column and the replicated-literal column.
pub fn scalarOp(
    comptime Tsrc: type,
    comptime Tout: type,
    comptime op: BinOp,
    comptime col_left: bool,
    src: []const Tsrc,
    scalar: Tout,
    dst: []Tout,
) void {
    std.debug.assert(src.len == dst.len);
    const is_int = @typeInfo(Tout) == .int;
    const N = comptime lanes(Tout);
    var i: usize = 0;
    if (N > 1) {
        const sv: @Vector(N, Tout) = @splat(scalar);
        while (i + N <= dst.len) : (i += N) {
            const chunk: @Vector(N, Tsrc) = src[i..][0..N].*;
            const w = @as(@Vector(N, Tout), chunk);
            const a = if (col_left) w else sv;
            const b = if (col_left) sv else w;
            dst[i..][0..N].* = if (is_int) switch (op) {
                .add => a +% b,
                .sub => a -% b,
                .mul => a *% b,
            } else switch (op) {
                .add => a + b,
                .sub => a - b,
                .mul => a * b,
            };
        }
    }
    while (i < dst.len) : (i += 1) {
        const w: Tout = src[i];
        const a = if (col_left) w else scalar;
        const b = if (col_left) scalar else w;
        dst[i] = if (is_int) switch (op) {
            .add => a +% b,
            .sub => a -% b,
            .mul => a *% b,
        } else switch (op) {
            .add => a + b,
            .sub => a - b,
            .mul => a * b,
        };
    }
}

test "simd: scalarOp matches widen-then-scalar reference" {
    const lengths = [_]usize{ 0, 1, 9, 16, 33, 777 };
    inline for (.{ .{ i16, i32 }, .{ i32, i32 }, .{ i32, i64 }, .{ f32, f64 }, .{ f64, f64 } }) |pair| {
        const Tsrc = pair[0];
        const Tout = pair[1];
        inline for (.{ BinOp.add, BinOp.sub, BinOp.mul }) |op| {
            inline for (.{ true, false }) |col_left| {
                for (lengths) |len| {
                    const src = try std.testing.allocator.alloc(Tsrc, len);
                    defer std.testing.allocator.free(src);
                    const dst = try std.testing.allocator.alloc(Tout, len);
                    defer std.testing.allocator.free(dst);
                    for (src, 0..) |*s, idx| s.* = if (@typeInfo(Tsrc) == .float) @floatFromInt(idx % 11) else @intCast(idx % 11);
                    const scalar: Tout = if (@typeInfo(Tout) == .float) 3.0 else 3;
                    scalarOp(Tsrc, Tout, op, col_left, src, scalar, dst);
                    for (src, dst) |s, d| {
                        const w: Tout = if (@typeInfo(Tout) == .float and @typeInfo(Tsrc) == .float) @floatCast(s) else s;
                        const a = if (col_left) w else scalar;
                        const b = if (col_left) scalar else w;
                        const want: Tout = if (@typeInfo(Tout) == .int) switch (op) {
                            .add => a +% b,
                            .sub => a -% b,
                            .mul => a *% b,
                        } else switch (op) {
                            .add => a + b,
                            .sub => a - b,
                            .mul => a * b,
                        };
                        try std.testing.expectEqual(want, d);
                    }
                }
            }
        }
    }
}

test "simd: compareInto matches scalar for all ops/types/lengths" {
    const lengths = [_]usize{ 0, 1, 7, 8, 16, 17, 64, 1000, 4097 };
    inline for (.{ i8, i16, i32, i64, i128, u8, u128, f32, f64 }) |T| {
        inline for (.{ CmpOp.eq, CmpOp.neq, CmpOp.lt, CmpOp.lte, CmpOp.gt, CmpOp.gte }) |op| {
            for (lengths) |len| {
                const data = try std.testing.allocator.alloc(T, len);
                defer std.testing.allocator.free(data);
                const mask = try std.testing.allocator.alloc(bool, len);
                defer std.testing.allocator.free(mask);
                for (data, 0..) |*d, idx| {
                    const x: i64 = @intCast((idx *% 31 +% 7) % 13);
                    d.* = if (@typeInfo(T) == .float) @floatFromInt(x) else @intCast(x);
                }
                const want: T = if (@typeInfo(T) == .float) 6.0 else 6;
                compareInto(T, op, data, want, mask);
                for (data, mask) |d, m| {
                    const exp = switch (op) {
                        .eq => d == want,
                        .neq => d != want,
                        .lt => d < want,
                        .lte => d <= want,
                        .gt => d > want,
                        .gte => d >= want,
                    };
                    try std.testing.expectEqual(exp, m);
                }
            }
        }
    }
}

test "simd: binInto matches scalar add/sub/mul" {
    const lengths = [_]usize{ 0, 1, 9, 16, 33, 1000 };
    inline for (.{ i32, i64, f64 }) |T| {
        inline for (.{ BinOp.add, BinOp.sub, BinOp.mul }) |op| {
            for (lengths) |len| {
                const a = try std.testing.allocator.alloc(T, len);
                defer std.testing.allocator.free(a);
                const b = try std.testing.allocator.alloc(T, len);
                defer std.testing.allocator.free(b);
                const dst = try std.testing.allocator.alloc(T, len);
                defer std.testing.allocator.free(dst);
                for (a, b, 0..) |*av, *bv, idx| {
                    av.* = if (@typeInfo(T) == .float) @floatFromInt(idx % 17) else @intCast(idx % 17);
                    bv.* = if (@typeInfo(T) == .float) @floatFromInt((idx % 5) + 1) else @intCast((idx % 5) + 1);
                }
                binInto(T, op, a, b, dst);
                for (a, b, dst) |av, bv, dv| {
                    const want: T = if (@typeInfo(T) == .float) switch (op) {
                        .add => av + bv,
                        .sub => av - bv,
                        .mul => av * bv,
                    } else switch (op) {
                        .add => av +% bv,
                        .sub => av -% bv,
                        .mul => av *% bv,
                    };
                    try std.testing.expectEqual(want, dv);
                }
            }
        }
    }
}

test "simd: minOf/maxOf match scalar over assorted lengths and types" {
    const lengths = [_]usize{ 1, 7, 16, 17, 64, 1000, 4097 };
    inline for (.{ i8, i16, i32, i64, i128, f32, f64 }) |T| {
        for (lengths) |len| {
            const buf = try std.testing.allocator.alloc(T, len);
            defer std.testing.allocator.free(buf);
            for (buf, 0..) |*v, idx| {
                const x: i64 = @intCast((idx *% 131 +% 17) % 251);
                v.* = if (@typeInfo(T) == .float) @floatFromInt(x - 125) else @intCast(x - 125);
            }
            var emin = buf[0];
            var emax = buf[0];
            for (buf[1..]) |v| {
                emin = @min(emin, v);
                emax = @max(emax, v);
            }
            try std.testing.expectEqual(emin, minOf(T, buf));
            try std.testing.expectEqual(emax, maxOf(T, buf));
        }
    }
}

test "simd: sumWiden matches scalar over assorted lengths and types" {
    const lengths = [_]usize{ 0, 1, 7, 16, 17, 64, 1000, 4097 };
    inline for (.{ i8, i16, i32, u8 }) |T| {
        const signed = @typeInfo(T).int.signedness == .signed;
        for (lengths) |len| {
            const buf = try std.testing.allocator.alloc(T, len);
            defer std.testing.allocator.free(buf);
            var scalar: i128 = 0;
            for (buf, 0..) |*v, idx| {
                // Varied values within every T's range; signed types include
                // negatives to exercise i64 sign-extension in the widen.
                const base: i64 = @intCast((idx *% 31 +% 7) % 101); // 0..100
                v.* = @intCast(if (signed) base - 50 else base);
                scalar += v.*;
            }
            try std.testing.expectEqual(scalar, sumWiden(T, buf));
        }
    }
}

test "simd: sumFloat matches scalar within tolerance" {
    const buf = try std.testing.allocator.alloc(f64, 5000);
    defer std.testing.allocator.free(buf);
    var scalar: f64 = 0;
    for (buf, 0..) |*v, i| {
        v.* = @as(f64, @floatFromInt(i)) * 0.5 - 100.0;
        scalar += v.*;
    }
    try std.testing.expectApproxEqRel(scalar, sumFloat(f64, buf), 1e-9);
}
