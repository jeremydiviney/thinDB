//! Math + numeric-conversion scalar kernels. The conversion kernels
//! (to_int / to_bigint / to_double / to_string) live here because they're
//! per-row numeric operations with the same kernel shape as abs/ceil/etc.

const std = @import("std");
const Allocator = std.mem.Allocator;

const common = @import("scalar_fn_common.zig");
const ColumnView = common.ColumnView;
const ColumnStore = common.ColumnStore;
const simd = @import("../util/simd.zig");
const stringViewOf = common.stringViewOf;
const stringStoreOf = common.stringStoreOf;

var random_seed_counter = std.atomic.Value(u64).init(0);

// ---------------------------------------------------------------------------
// Core math: abs / ceil / floor / round / sign / mod / pow / sqrt / exp /
// ln / log10 / log2 / greatest / least.
// ---------------------------------------------------------------------------

pub fn absIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        // INT_MIN's abs overflows; saturate to INT_MAX to avoid trap.
        const v = s[i];
        const r: i32 = if (v == std.math.minInt(i32)) std.math.maxInt(i32) else if (v < 0) -v else v;
        try out.data.int.append(allocator, r);
    }
}

pub fn absBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v = s[i];
        const r: i64 = if (v == std.math.minInt(i64)) std.math.maxInt(i64) else if (v < 0) -v else v;
        try out.data.bigint.append(allocator, r);
    }
}

pub fn absDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @abs(s[i]));
}

pub fn ceilKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @ceil(s[i]));
}

pub fn floorKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @floor(s[i]));
}

pub fn roundKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @round(s[i]));
}

pub fn signKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v = s[i];
        const r: i32 = if (v > 0) 1 else if (v < 0) -1 else 0;
        try out.data.int.append(allocator, r);
    }
}

// ---------------------------------------------------------------------------
// Binary arithmetic (+, -, *, /) — kernel implementations.
// All wrapping for integer types matches the column's declared width
// (i32/i64). Division by zero on integer kernels returns 0 (same
// convention as MOD above); on double-typed kernels Zig propagates
// IEEE NaN/inf naturally.
// ---------------------------------------------------------------------------

// Reserve `n` elements of an unmanaged int/bigint/double list and return the
// freshly-exposed tail slice for a vectorized write. The output list is
// cleared before each kernel call, so this appends `n` new values.
fn reserveInt(allocator: Allocator, out: *ColumnStore, n: usize) ![]i32 {
    try out.data.int.ensureUnusedCapacity(allocator, n);
    const base = out.data.int.items.len;
    out.data.int.items.len = base + n;
    return out.data.int.items[base..];
}
fn reserveBigint(allocator: Allocator, out: *ColumnStore, n: usize) ![]i64 {
    try out.data.bigint.ensureUnusedCapacity(allocator, n);
    const base = out.data.bigint.items.len;
    out.data.bigint.items.len = base + n;
    return out.data.bigint.items[base..];
}
fn reserveDouble(allocator: Allocator, out: *ColumnStore, n: usize) ![]f64 {
    try out.data.double.ensureUnusedCapacity(allocator, n);
    const base = out.data.double.items.len;
    out.data.double.items.len = base + n;
    return out.data.double.items[base..];
}

pub fn addIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    simd.binInto(i32, .add, args[0].data.int[0..row_count], args[1].data.int[0..row_count], try reserveInt(allocator, out, row_count));
}

pub fn addBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    simd.binInto(i64, .add, args[0].data.bigint[0..row_count], args[1].data.bigint[0..row_count], try reserveBigint(allocator, out, row_count));
}

pub fn addDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    simd.binInto(f64, .add, args[0].data.double[0..row_count], args[1].data.double[0..row_count], try reserveDouble(allocator, out, row_count));
}

// i128 (LARGEINT) arithmetic. Wrapping like the narrower int kernels — the
// affine-aggregate reduction (`local.zig`) is the only producer and proves
// the affine transform stays in range, so the i128 result equals the direct
// `SUM(a·col+b)` accumulator exactly. Scalar loops (no SIMD) since i128 lanes
// aren't a vector win.
pub fn addLargeintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.largeint;
    const b = args[1].data.largeint;
    try out.data.largeint.ensureUnusedCapacity(allocator, row_count);
    var i: usize = 0;
    while (i < row_count) : (i += 1) out.data.largeint.appendAssumeCapacity(a[i] +% b[i]);
}

pub fn subLargeintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.largeint;
    const b = args[1].data.largeint;
    try out.data.largeint.ensureUnusedCapacity(allocator, row_count);
    var i: usize = 0;
    while (i < row_count) : (i += 1) out.data.largeint.appendAssumeCapacity(a[i] -% b[i]);
}

pub fn mulLargeintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.largeint;
    const b = args[1].data.largeint;
    try out.data.largeint.ensureUnusedCapacity(allocator, row_count);
    var i: usize = 0;
    while (i < row_count) : (i += 1) out.data.largeint.appendAssumeCapacity(a[i] *% b[i]);
}

/// Checked i128 → i64 narrow used by the affine-aggregate reduction to
/// finalize a derived integer SUM. Errors on out-of-range EXACTLY as the
/// SUM accumulator's finalize does (`appendAccToColumn`: `> maxInt(i64) or
/// < minInt(i64)`), so a derived `SUM(col)+k·COUNT(col)` raises the same
/// `ArithmeticOverflow` the direct `SUM(col+k)` would. Internal-only
/// (`__` name), never reachable from user SQL.
pub fn narrowBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.largeint;
    try out.data.bigint.ensureUnusedCapacity(allocator, row_count);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v = s[i];
        if (v > std.math.maxInt(i64) or v < std.math.minInt(i64)) return error.ArithmeticOverflow;
        out.data.bigint.appendAssumeCapacity(@intCast(v));
    }
}

pub fn subIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    simd.binInto(i32, .sub, args[0].data.int[0..row_count], args[1].data.int[0..row_count], try reserveInt(allocator, out, row_count));
}

pub fn subBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    simd.binInto(i64, .sub, args[0].data.bigint[0..row_count], args[1].data.bigint[0..row_count], try reserveBigint(allocator, out, row_count));
}

pub fn subDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    simd.binInto(f64, .sub, args[0].data.double[0..row_count], args[1].data.double[0..row_count], try reserveDouble(allocator, out, row_count));
}

pub fn mulIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    simd.binInto(i32, .mul, args[0].data.int[0..row_count], args[1].data.int[0..row_count], try reserveInt(allocator, out, row_count));
}

pub fn mulBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    simd.binInto(i64, .mul, args[0].data.bigint[0..row_count], args[1].data.bigint[0..row_count], try reserveBigint(allocator, out, row_count));
}

pub fn mulDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    simd.binInto(f64, .mul, args[0].data.double[0..row_count], args[1].data.double[0..row_count], try reserveDouble(allocator, out, row_count));
}

pub fn divIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.int;
    const b = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const r: i32 = if (b[i] == 0) 0 else @divTrunc(a[i], b[i]);
        try out.data.int.append(allocator, r);
    }
}

pub fn divBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.bigint;
    const b = args[1].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const r: i64 = if (b[i] == 0) 0 else @divTrunc(a[i], b[i]);
        try out.data.bigint.append(allocator, r);
    }
}

pub fn divDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.double;
    const b = args[1].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, a[i] / b[i]);
}

pub fn modIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.int;
    const b = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        // MySQL convention: MOD by 0 returns 0 (we don't surface NULL on
        // the propagates path without going kernel_managed).
        const r: i32 = if (b[i] == 0) 0 else @rem(a[i], b[i]);
        try out.data.int.append(allocator, r);
    }
}

pub fn modBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.bigint;
    const b = args[1].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const r: i64 = if (b[i] == 0) 0 else @rem(a[i], b[i]);
        try out.data.bigint.append(allocator, r);
    }
}

pub fn powKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.double;
    const b = args[1].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, std.math.pow(f64, a[i], b[i]));
}

pub fn sqrtKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @sqrt(s[i]));
}

pub fn expKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @exp(s[i]));
}

pub fn lnKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @log(s[i]));
}

pub fn log10Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @log10(s[i]));
}

pub fn log2Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @log2(s[i]));
}

pub fn greatestIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.int;
    const b = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, @max(a[i], b[i]));
}

pub fn greatestBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.bigint;
    const b = args[1].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.bigint.append(allocator, @max(a[i], b[i]));
}

pub fn greatestDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.double;
    const b = args[1].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @max(a[i], b[i]));
}

pub fn leastIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.int;
    const b = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, @min(a[i], b[i]));
}

pub fn leastBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.bigint;
    const b = args[1].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.bigint.append(allocator, @min(a[i], b[i]));
}

pub fn leastDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.double;
    const b = args[1].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @min(a[i], b[i]));
}

// ---------------------------------------------------------------------------
// Expanded math parity: trig, log(base,x), nullary constants/random, round
// with scale, positive modulo, bit helpers, binary/base conversion.
// ---------------------------------------------------------------------------

pub fn sinKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @sin(s[i]));
}

pub fn cosKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @cos(s[i]));
}

pub fn tanKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @tan(s[i]));
}

pub fn asinKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, std.math.asin(s[i]));
}

pub fn acosKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, std.math.acos(s[i]));
}

pub fn atanKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, std.math.atan(s[i]));
}

pub fn cotKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, 1.0 / @tan(s[i]));
}

pub fn cbrtKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, std.math.cbrt(s[i]));
}

pub fn piKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    _ = args;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, std.math.pi);
}

pub fn randomKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    _ = args;
    var prng = std.Random.DefaultPrng.init(random_seed_counter.fetchAdd(1, .monotonic) +% 1);
    const random = prng.random();
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, random.float(f64));
}

pub fn logBaseKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = args[0].data.double;
    const x = args[1].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @log(x[i]) / @log(base[i]));
}

pub fn roundScaleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const x = args[0].data.double;
    const d = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const scale = std.math.pow(f64, 10.0, @floatFromInt(d[i]));
        try out.data.double.append(allocator, @round(x[i] * scale) / scale);
    }
}

pub fn fmodKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.double;
    const b = args[1].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @mod(a[i], b[i]));
}

pub fn pmodIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.int;
    const b = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const r: i32 = if (b[i] == 0) 0 else @mod(a[i], b[i]);
        try out.data.int.append(allocator, r);
    }
}

pub fn pmodBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.bigint;
    const b = args[1].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const r: i64 = if (b[i] == 0) 0 else @mod(a[i], b[i]);
        try out.data.bigint.append(allocator, r);
    }
}

pub fn squareKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, s[i] * s[i]);
}

pub fn bitCountIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, @intCast(@popCount(@as(u32, @bitCast(s[i])))));
}

pub fn bitCountBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, @intCast(@popCount(@as(u64, @bitCast(s[i])))));
}

fn appendUnsignedBase(allocator: Allocator, ss: anytype, value: u128, base: u8) !void {
    const digits = "0123456789abcdefghijklmnopqrstuvwxyz";
    var buf: [128]u8 = undefined;
    var n: usize = 0;
    var v = value;
    if (v == 0) {
        buf[buf.len - 1] = '0';
        n = 1;
    } else {
        while (v != 0) : (n += 1) {
            const b128: u128 = base;
            const rem: usize = @intCast(v % b128);
            buf[buf.len - 1 - n] = digits[rem];
            v /= b128;
        }
    }
    try ss.appendValue(allocator, buf[buf.len - n ..]);
}

pub fn binIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const value: u128 = if (s[i] < 0) @as(u128, @as(u32, @bitCast(s[i]))) else @intCast(s[i]);
        try appendUnsignedBase(allocator, ss, value, 2);
    }
}

pub fn binBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const value: u128 = if (s[i] < 0) @as(u128, @as(u64, @bitCast(s[i]))) else @intCast(s[i]);
        try appendUnsignedBase(allocator, ss, value, 2);
    }
}

fn digitValue(c: u8) ?u8 {
    return switch (c) {
        '0'...'9' => c - '0',
        'a'...'z' => 10 + (c - 'a'),
        'A'...'Z' => 10 + (c - 'A'),
        else => null,
    };
}

fn parseUnsignedBase(bytes: []const u8, base: u8) ?u128 {
    if (base < 2 or base > 36) return null;
    var v: u128 = 0;
    var saw = false;
    for (bytes) |c| {
        const d = digitValue(c) orelse return null;
        if (d >= base) return null;
        v = v * @as(u128, base) + @as(u128, d);
        saw = true;
    }
    return if (saw) v else null;
}

pub fn convStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const from_bases = args[1].data.int;
    const to_bases = args[2].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const fb = from_bases[i];
        const tb = to_bases[i];
        if (fb < 2 or fb > 36 or tb < 2 or tb > 36) {
            try ss.appendValue(allocator, "");
            continue;
        }
        const v = parseUnsignedBase(sv.rowBytes(i), @intCast(fb)) orelse {
            try ss.appendValue(allocator, "");
            continue;
        };
        try appendUnsignedBase(allocator, ss, v, @intCast(tb));
    }
}

pub fn convBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const values = args[0].data.bigint;
    const _from_bases = args[1].data.int;
    const to_bases = args[2].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        _ = _from_bases[i];
        const tb = to_bases[i];
        if (tb < 2 or tb > 36 or values[i] < 0) {
            try ss.appendValue(allocator, "");
            continue;
        }
        try appendUnsignedBase(allocator, ss, @intCast(values[i]), @intCast(tb));
    }
}

// ---------------------------------------------------------------------------
// Conversion kernels — explicit `to_*` functions. With the implicit cast
// machinery in scalar_fn/cast.zig in place, these are mainly used when the
// caller wants narrowing (which never happens implicitly) or string parsing.
// ---------------------------------------------------------------------------

pub fn intToBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.bigint.append(allocator, s[i]);
}

pub fn intToDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @floatFromInt(s[i]));
}

pub fn bigintToDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @floatFromInt(s[i]));
}

pub fn bigintToIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v = s[i];
        const clamped: i32 = if (v > std.math.maxInt(i32))
            std.math.maxInt(i32)
        else if (v < std.math.minInt(i32))
            std.math.minInt(i32)
        else
            @intCast(v);
        try out.data.int.append(allocator, clamped);
    }
}

pub fn doubleToIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v = s[i];
        const trunc = @trunc(v);
        const clamped: i32 = if (std.math.isNan(v) or trunc > @as(f64, @floatFromInt(std.math.maxInt(i32))))
            std.math.maxInt(i32)
        else if (trunc < @as(f64, @floatFromInt(std.math.minInt(i32))))
            std.math.minInt(i32)
        else
            @intFromFloat(trunc);
        try out.data.int.append(allocator, clamped);
    }
}

pub fn doubleToBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v = s[i];
        const trunc = @trunc(v);
        const clamped: i64 = if (std.math.isNan(v) or trunc > @as(f64, @floatFromInt(std.math.maxInt(i64))))
            std.math.maxInt(i64)
        else if (trunc < @as(f64, @floatFromInt(std.math.minInt(i64))))
            std.math.minInt(i64)
        else
            @intFromFloat(trunc);
        try out.data.bigint.append(allocator, clamped);
    }
}

pub fn stringToIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v = std.fmt.parseInt(i32, sv.rowBytes(i), 10) catch 0;
        try out.data.int.append(allocator, v);
    }
}

pub fn stringToBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v = std.fmt.parseInt(i64, sv.rowBytes(i), 10) catch 0;
        try out.data.bigint.append(allocator, v);
    }
}

pub fn stringToDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v = std.fmt.parseFloat(f64, sv.rowBytes(i)) catch 0.0;
        try out.data.double.append(allocator, v);
    }
}

pub fn intToStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.int;
    const ss = stringStoreOf(out);
    var buf: [16]u8 = undefined;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const text = try std.fmt.bufPrint(&buf, "{d}", .{s[i]});
        try ss.appendValue(allocator, text);
    }
}

pub fn bigintToStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    const ss = stringStoreOf(out);
    var buf: [24]u8 = undefined;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const text = try std.fmt.bufPrint(&buf, "{d}", .{s[i]});
        try ss.appendValue(allocator, text);
    }
}

pub fn doubleToStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    const ss = stringStoreOf(out);
    var buf: [64]u8 = undefined;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const text = try std.fmt.bufPrint(&buf, "{d}", .{s[i]});
        try ss.appendValue(allocator, text);
    }
}

pub fn boolToStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.boolean;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try ss.appendValue(allocator, if (s[i] != 0) "true" else "false");
    }
}

// --- narrowing / boolean / largeint conversions (back CAST targets) ---
// Numeric→numeric uses saturating lossyCast; string parses (0 on failure,
// matching the existing to_int/to_bigint string kernels).

pub fn bigintToSmallintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.smallint.append(allocator, std.math.lossyCast(i16, s[i]));
}
pub fn doubleToSmallintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.smallint.append(allocator, std.math.lossyCast(i16, s[i]));
}
pub fn stringToSmallintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.smallint.append(allocator, std.fmt.parseInt(i16, sv.rowBytes(i), 10) catch 0);
}

pub fn bigintToTinyintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.tinyint.append(allocator, std.math.lossyCast(i8, s[i]));
}
pub fn doubleToTinyintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.tinyint.append(allocator, std.math.lossyCast(i8, s[i]));
}
pub fn stringToTinyintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.tinyint.append(allocator, std.fmt.parseInt(i8, sv.rowBytes(i), 10) catch 0);
}

pub fn bigintToLargeintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.largeint.append(allocator, @as(i128, s[i]));
}
pub fn doubleToLargeintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.largeint.append(allocator, std.math.lossyCast(i128, s[i]));
}
pub fn stringToLargeintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.largeint.append(allocator, std.fmt.parseInt(i128, sv.rowBytes(i), 10) catch 0);
}

pub fn bigintToBoolKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.boolean.append(allocator, if (s[i] != 0) @as(u8, 1) else 0);
}
pub fn doubleToBoolKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.boolean.append(allocator, if (s[i] != 0) @as(u8, 1) else 0);
}

// ---------------------------------------------------------------------------
// Expanded math: truncate(x, d) / degrees / radians / atan2.
// ---------------------------------------------------------------------------

pub fn truncateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const x = args[0].data.double;
    const d = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const scale: f64 = std.math.pow(f64, 10.0, @floatFromInt(d[i]));
        const v = @trunc(x[i] * scale) / scale;
        try out.data.double.append(allocator, v);
    }
}

pub fn degreesKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, s[i] * (180.0 / std.math.pi));
}

pub fn radiansKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, s[i] * (std.math.pi / 180.0));
}

pub fn atan2Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const y = args[0].data.double;
    const x = args[1].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, std.math.atan2(y[i], x[i]));
}
