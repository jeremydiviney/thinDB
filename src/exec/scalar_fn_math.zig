//! Math + numeric-conversion scalar kernels. The conversion kernels
//! (to_int / to_bigint / to_double / to_string) live here because they're
//! per-row numeric operations with the same kernel shape as abs/ceil/etc.

const std = @import("std");
const Allocator = std.mem.Allocator;

const common = @import("scalar_fn_common.zig");
const ColumnView = common.ColumnView;
const ColumnStore = common.ColumnStore;
const simd = @import("../util/simd.zig");
const memory = @import("../memory.zig");
const jb = @import("json_binary.zig");
const dec = @import("scalar_fn_decimal.zig");
const cast = @import("cast.zig");
const Type = @import("../types.zig").Type;
const stringViewOf = common.stringViewOf;
const stringStoreOf = common.stringStoreOf;

var random_seed_counter = std.atomic.Value(u64).init(0);

// ---------------------------------------------------------------------------
// Core math: abs / ceil / floor / round / sign / mod / pow / sqrt / exp /
// ln / log10 / log2 / greatest / least.
// ---------------------------------------------------------------------------

/// ABS widens one level like `+ - *` (TINYINT→SMALLINT→INT→BIGINT), so only
/// BIGINT can overflow: ABS(BIGINT_MIN) wraps to BIGINT_MIN, where StarRocks
/// returns the LARGEINT 9223372036854775808 (DESIGN.md §3.4).
pub fn absIntegerKernel(comptime Src: type, comptime Dst: type) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const src = @field(args[0].data, intField(Src))[0..row_count];
            const dst = try reserveInts(Dst, allocator, out, row_count);
            for (src, dst) |x, *d| d.* = if (@bitSizeOf(Dst) > @bitSizeOf(Src)) @abs(x) else @bitCast(@abs(x));
        }
    }.kernel;
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
// Binary arithmetic (+, -, *, /, DIV, MOD) — kernel implementations.
// `scalar_fn.intArithResultType` picks an integer operation's width
// (DESIGN.md §3.4) and the resolver casts both operands to it, so each integer
// kernel reads two same-width columns. Integer results wrap in two's
// complement, as StarRocks does. Division registers `.zero_divisor`, so
// Compute nulls a zero-divisor row and the kernel writes 0 there. Floating
// operands otherwise follow IEEE.
// ---------------------------------------------------------------------------

const Kernel = *const fn (allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void;

fn intField(comptime T: type) []const u8 {
    return switch (T) {
        i8 => "tinyint",
        i16 => "smallint",
        i32 => "int",
        i64 => "bigint",
        i128 => "largeint",
        else => @compileError("no integer column type for " ++ @typeName(T)),
    };
}

// Reserve `n` elements of an unmanaged output list and return the
// freshly-exposed tail slice for a vectorized write. The output list is
// cleared before each kernel call, so this appends `n` new values.
fn reserveInts(comptime T: type, allocator: Allocator, out: *ColumnStore, n: usize) ![]T {
    const list = &@field(out.data, intField(T));
    try list.ensureUnusedCapacity(allocator, n);
    const base = list.items.len;
    list.items.len = base + n;
    return list.items[base..];
}
fn reserveDouble(allocator: Allocator, out: *ColumnStore, n: usize) ![]f64 {
    try out.data.double.ensureUnusedCapacity(allocator, n);
    const base = out.data.double.items.len;
    out.data.double.items.len = base + n;
    return out.data.double.items[base..];
}

/// MySQL and StarRocks answer a math function's domain error or overflow
/// (SQRT(-1), LN(0), ASIN(2), EXP(1000), MOD(x, 0)) with NULL, never NaN or
/// ±inf, so a wrapped kernel owns its validity bitmap: register it with
/// `null_strategy = .kernel_managed`. A NULL slot holds 0, not the NaN.
fn finiteOrNull(comptime f: anytype) Kernel {
    const Operands = std.meta.ArgsTuple(@TypeOf(f));
    const arity = @typeInfo(Operands).@"struct".fields.len;
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const dst = try reserveDouble(allocator, out, row_count);
            const base = out.data.rowCount() - row_count;
            for (dst, 0..) |*d, row| {
                var operands: Operands = undefined;
                var valid = true;
                inline for (0..arity) |a| {
                    operands[a] = args[a].data.double[row];
                    valid = valid and args[a].isValid(row);
                }
                const r = @call(.auto, f, operands);
                valid = valid and std.math.isFinite(r);
                d.* = if (valid) r else 0;
                try out.appendValidBit(allocator, base + row, valid);
            }
        }
    }.kernel;
}

pub fn wrappingArithKernel(comptime T: type, comptime op: simd.BinOp) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const a = @field(args[0].data, intField(T))[0..row_count];
            const b = @field(args[1].data, intField(T))[0..row_count];
            simd.binInto(T, op, a, b, try reserveInts(T, allocator, out, row_count));
        }
    }.kernel;
}

pub fn addDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    simd.binInto(f64, .add, args[0].data.double[0..row_count], args[1].data.double[0..row_count], try reserveDouble(allocator, out, row_count));
}

pub fn subDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    simd.binInto(f64, .sub, args[0].data.double[0..row_count], args[1].data.double[0..row_count], try reserveDouble(allocator, out, row_count));
}

pub fn mulDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    simd.binInto(f64, .mul, args[0].data.double[0..row_count], args[1].data.double[0..row_count], try reserveDouble(allocator, out, row_count));
}

pub const DivMod = enum { div, mod };

/// Integer DIV (truncating) or MOD (dividend's sign). A -1 divisor is answered
/// without dividing because minInt ÷ -1 traps x86 `idiv`: DIV negates with
/// wrap (minInt DIV -1 = minInt, as in StarRocks) and MOD is 0.
pub fn intDivModKernel(comptime T: type, comptime op: DivMod) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const a = @field(args[0].data, intField(T))[0..row_count];
            const b = @field(args[1].data, intField(T))[0..row_count];
            const dst = try reserveInts(T, allocator, out, row_count);
            for (a, b, dst) |x, y, *d| {
                d.* = switch (y) {
                    0 => 0,
                    -1 => if (op == .div) 0 -% x else 0,
                    else => if (op == .div) @divTrunc(x, y) else @rem(x, y),
                };
            }
        }
    }.kernel;
}

pub fn divDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.double[0..row_count];
    const b = args[1].data.double[0..row_count];
    const dst = try reserveDouble(allocator, out, row_count);
    for (a, b, dst) |x, y, *d| d.* = if (y == 0) 0 else x / y;
}

pub const powKernel = finiteOrNull(struct {
    fn f(a: f64, b: f64) f64 {
        return std.math.pow(f64, a, b);
    }
}.f);

pub const sqrtKernel = finiteOrNull(struct {
    fn f(x: f64) f64 {
        return @sqrt(x);
    }
}.f);

pub const expKernel = finiteOrNull(struct {
    fn f(x: f64) f64 {
        return @exp(x);
    }
}.f);

pub const lnKernel = finiteOrNull(struct {
    fn f(x: f64) f64 {
        return @log(x);
    }
}.f);

pub const log10Kernel = finiteOrNull(struct {
    fn f(x: f64) f64 {
        return @log10(x);
    }
}.f);

pub const log2Kernel = finiteOrNull(struct {
    fn f(x: f64) f64 {
        return @log2(x);
    }
}.f);

/// GREATEST/LEAST over any number of arguments in a column representation
/// they and the output share (`field` names it in the column data union),
/// folded one argument column at a time.
fn Extremum(comptime field: []const u8, comptime take_max: bool) type {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const dst = &@field(out.data, field);
            try dst.appendSlice(allocator, @field(args[0].data, field)[0..row_count]);
            const best = dst.items[dst.items.len - row_count ..];
            for (args[1..]) |arg| {
                for (best, @field(arg.data, field)[0..row_count]) |*b, x| b.* = if (take_max) @max(b.*, x) else @min(b.*, x);
            }
        }
    };
}

pub const greatestIntKernel = Extremum("int", true).kernel;
pub const greatestBigintKernel = Extremum("bigint", true).kernel;
pub const greatestLargeintKernel = Extremum("largeint", true).kernel;
pub const greatestDoubleKernel = Extremum("double", true).kernel;
pub const greatestDateKernel = Extremum("date", true).kernel;
pub const greatestDatetimeKernel = Extremum("datetime", true).kernel;
pub const leastIntKernel = Extremum("int", false).kernel;
pub const leastBigintKernel = Extremum("bigint", false).kernel;
pub const leastLargeintKernel = Extremum("largeint", false).kernel;
pub const leastDoubleKernel = Extremum("double", false).kernel;
pub const leastDateKernel = Extremum("date", false).kernel;
pub const leastDatetimeKernel = Extremum("datetime", false).kernel;

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

pub const asinKernel = finiteOrNull(struct {
    fn f(x: f64) f64 {
        return std.math.asin(x);
    }
}.f);

pub const acosKernel = finiteOrNull(struct {
    fn f(x: f64) f64 {
        return std.math.acos(x);
    }
}.f);

pub fn atanKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, std.math.atan(s[i]));
}

pub const cotKernel = finiteOrNull(struct {
    fn f(x: f64) f64 {
        return 1.0 / @tan(x);
    }
}.f);

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

pub const logBaseKernel = finiteOrNull(struct {
    fn f(base: f64, x: f64) f64 {
        return @log(x) / @log(base);
    }
}.f);

pub fn roundScaleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const x = args[0].data.double;
    const d = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const scale = std.math.pow(f64, 10.0, @floatFromInt(d[i]));
        try out.data.double.append(allocator, @round(x[i] * scale) / scale);
    }
}

pub const fmodKernel = finiteOrNull(struct {
    fn f(a: f64, b: f64) f64 {
        return @mod(a, b);
    }
}.f);

/// `%` / MOD with a floating operand: the remainder keeps the dividend's
/// sign (MySQL, DuckDB), unlike `fmod()` above which floors. MOD by 0 is
/// NULL, as for the integer kernels.
pub const modDoubleKernel = finiteOrNull(struct {
    fn f(a: f64, b: f64) f64 {
        return @rem(a, b);
    }
}.f);

pub fn pmodIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.int;
    const b = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const r: i32 = if (b[i] == 0 or b[i] == -1) 0 else @mod(a[i], b[i]);
        try out.data.int.append(allocator, r);
    }
}

pub fn pmodBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.bigint;
    const b = args[1].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const r: i64 = if (b[i] == 0 or b[i] == -1) 0 else @mod(a[i], b[i]);
        try out.data.bigint.append(allocator, r);
    }
}

pub fn squareKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, s[i] * s[i]);
}

/// A BIGINT or LARGEINT as the BIGINT UNSIGNED MySQL's bit functions read:
/// a negative BIGINT as its two's-complement bits, and a LARGEINT, which
/// holds the BIGINT UNSIGNED values past BIGINT, as
/// `common.wideIntegerAsBigint` keeps it.
fn unsignedBits(x: anytype) u64 {
    return switch (@TypeOf(x)) {
        i64 => @bitCast(x),
        i128 => @bitCast(common.wideIntegerAsBigint(x)),
        else => @compileError("no BIGINT UNSIGNED reading of " ++ @typeName(@TypeOf(x))),
    };
}

/// MySQL's BIT_COUNT: the one bits of the value as BIGINT UNSIGNED, so a
/// negative value of any width has 64.
pub fn bitCountKernel(comptime T: type) Kernel {
    return struct {
        fn f(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const dst = try reserveInts(i64, allocator, out, row_count);
            for (dst, @field(args[0].data, intField(T))[0..row_count]) |*d, x| d.* = @popCount(unsignedBits(x));
        }
    }.f;
}

pub const BitOp = enum { @"and", @"or", xor, shift_left, shift_right };

/// MySQL's bit operators over BIGINT UNSIGNED (`unsignedBits`), the result
/// held in a LARGEINT: `-1 | 0` is 18446744073709551615, and `>>` shifts
/// zeros in. A count outside 0..63, a negative one included, shifts every
/// bit out.
pub fn unsignedBitwiseKernel(comptime op: BitOp, comptime T: type) Kernel {
    return struct {
        fn f(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const field = comptime intField(T);
            const dst = try reserveInts(i128, allocator, out, row_count);
            for (dst, @field(args[0].data, field)[0..row_count], @field(args[1].data, field)[0..row_count]) |*d, a_in, b_in| {
                const a = unsignedBits(a_in);
                const b = unsignedBits(b_in);
                d.* = switch (op) {
                    .@"and" => a & b,
                    .@"or" => a | b,
                    .xor => a ^ b,
                    .shift_left => if (b > 63) 0 else a << @intCast(b),
                    .shift_right => if (b > 63) 0 else a >> @intCast(b),
                };
            }
        }
    }.f;
}

pub fn unsignedBitNotKernel(comptime T: type) Kernel {
    return struct {
        fn f(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const dst = try reserveInts(i128, allocator, out, row_count);
            for (dst, @field(args[0].data, intField(T))[0..row_count]) |*d, x| d.* = ~unsignedBits(x);
        }
    }.f;
}

/// StarRocks' bit functions, which the operators lower to outside MySQL,
/// read a BIGINT as its two's-complement bits, as DuckDB and PG do. A shift
/// by a count outside 0..63 shifts every bit out; `>>` is arithmetic,
/// keeping the sign.
pub fn bitwiseKernel(comptime op: BitOp) Kernel {
    return struct {
        fn f(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const dst = try reserveInts(i64, allocator, out, row_count);
            for (dst, args[0].data.bigint[0..row_count], args[1].data.bigint[0..row_count]) |*d, a, b| d.* = switch (op) {
                .@"and" => a & b,
                .@"or" => a | b,
                .xor => a ^ b,
                .shift_left => if (b < 0 or b > 63) 0 else @bitCast(@as(u64, @bitCast(a)) << @intCast(b)),
                .shift_right => if (b < 0 or b > 63) (if (a < 0) -1 else 0) else a >> @intCast(b),
            };
        }
    }.f;
}

pub fn bitNotKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const dst = try reserveInts(i64, allocator, out, row_count);
    for (dst, args[0].data.bigint[0..row_count]) |*d, a| d.* = ~a;
}

/// MySQL's CONV(n, from_base, to_base), and BIN(n) as CONV(n, 10, 2). `n`
/// is read as its text, so `BIN(2.7)` converts "2.7", which reads as 2; a
/// boolean is MySQL's integer 1 or 0, so its text is that digit.
pub const convKernel = baseConversionKernel(.text, null);
pub const binKernel = baseConversionKernel(.text, .{ 10, 2 });
pub const convBooleanKernel = baseConversionKernel(.boolean, null);
pub const binBooleanKernel = baseConversionKernel(.boolean, .{ 10, 2 });
/// CONV over a hex literal's integer, held in a LARGEINT: MySQL converts the
/// literal's own 64 bits rather than any text, so `from_base` must be a base
/// but names none of its digits (`CONV(X'FF', 16, 10)` is 255).
pub const convBitsKernel = baseConversionKernel(.bits, null);

fn baseConversionKernel(comptime source: enum { text, boolean, bits }, comptime fixed_bases: ?[2]i32) Kernel {
    return struct {
        fn f(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const sv = switch (source) {
                .text => stringViewOf(args[0]),
                .boolean, .bits => {},
            };
            const ss = stringStoreOf(out);
            const first = out.data.rowCount();
            var buf: [65]u8 = undefined;
            for (0..row_count) |i| {
                const text: ?[]const u8 = for (args) |a| {
                    if (!a.isValid(i)) break null;
                } else blk: {
                    const from_base, const to_base = fixed_bases orelse .{ args[1].data.int[i], args[2].data.int[i] };
                    const n = switch (source) {
                        .text => sv.rowBytes(i),
                        .boolean => if (args[0].data.boolean[i] != 0) "1" else "0",
                        .bits => {
                            if (!validBase(from_base) or !validBase(to_base)) break :blk null;
                            const bits: u64 = @truncate(@as(u128, @bitCast(args[0].data.largeint[i])));
                            break :blk baseText(&buf, bits, @intCast(@abs(to_base)), to_base < 0);
                        },
                    };
                    break :blk convText(&buf, n, from_base, to_base);
                };
                try ss.appendValue(allocator, text orelse "");
                try out.appendValidBit(allocator, first + i, text != null);
            }
        }
    }.f;
}

/// `text` read in base |from_base| and written in base |to_base|, each
/// signed when negative; null for a base outside 2..36 or empty text.
fn convText(buf: *[65]u8, text: []const u8, from_base: i32, to_base: i32) ?[]const u8 {
    if (text.len == 0 or !validBase(from_base) or !validBase(to_base)) return null;
    return baseText(buf, baseValue(text, @intCast(@abs(from_base)), from_base < 0), @intCast(@abs(to_base)), to_base < 0);
}

fn validBase(base: i32) bool {
    return @abs(base) >= 2 and @abs(base) <= 36;
}

/// Text as MySQL's my_strntoull reads it, or my_strntoll when `signed`:
/// leading spaces, a sign, then the digits of `base` up to the first byte
/// that isn't one, so no digit at all reads as 0. Unsigned, a value past
/// 2^64 - 1 is 2^64 - 1 and a '-' negates modulo 2^64; signed, the value
/// saturates to BIGINT. Either way the result is the 64 bits.
fn baseValue(text: []const u8, base: u8, signed: bool) u64 {
    var i: usize = 0;
    while (i < text.len and std.ascii.isWhitespace(text[i])) i += 1;
    const negative = i < text.len and text[i] == '-';
    if (i < text.len and (text[i] == '-' or text[i] == '+')) i += 1;
    const past_unsigned: u128 = @as(u128, std.math.maxInt(u64)) + 1;
    var magnitude: u128 = 0;
    for (text[i..]) |c| {
        const digit = std.fmt.charToDigit(c, base) catch break;
        magnitude = @min(magnitude * base + digit, past_unsigned);
    }
    if (signed) {
        const limit: u128 = if (negative) @as(u128, 1) << 63 else std.math.maxInt(i64);
        const m: i128 = @intCast(@min(magnitude, limit));
        return @bitCast(@as(i64, @intCast(if (negative) -m else m)));
    }
    if (magnitude == past_unsigned) return std.math.maxInt(u64);
    const m: u64 = @intCast(magnitude);
    return if (negative) 0 -% m else m;
}

/// `bits` in `base` with uppercase letter digits: a negative BIGINT with a
/// '-' when `signed`, and BIGINT UNSIGNED otherwise.
fn baseText(buf: *[65]u8, bits: u64, base: u8, signed: bool) []const u8 {
    const negative = signed and @as(i64, @bitCast(bits)) < 0;
    var v = if (negative) 0 -% bits else bits;
    var n: usize = buf.len;
    while (true) {
        n -= 1;
        buf[n] = std.fmt.digitToChar(@intCast(v % base), .upper);
        v /= base;
        if (v == 0) break;
    }
    if (negative) {
        n -= 1;
        buf[n] = '-';
    }
    return buf[n..];
}

// ---------------------------------------------------------------------------
// Conversion kernels — explicit `to_*` functions. With the implicit cast
// machinery in scalar_fn/cast.zig in place, these are mainly used when the
// caller wants narrowing (which never happens implicitly) or string parsing.
// ---------------------------------------------------------------------------

pub fn intIdentityKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, s[i]);
}

pub fn bigintIdentityKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.bigint.append(allocator, s[i]);
}

pub fn smallintIdentityKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.smallint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.smallint.append(allocator, s[i]);
}

pub fn tinyintIdentityKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.tinyint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.tinyint.append(allocator, s[i]);
}

pub fn largeintIdentityKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.largeint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.largeint.append(allocator, s[i]);
}

pub fn doubleIdentityKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, s[i]);
}

pub fn floatIdentityKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try out.data.float.appendSlice(allocator, args[0].data.float[0..row_count]);
}

pub fn booleanIdentityKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.boolean;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.boolean.append(allocator, s[i]);
}

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

// ---------------------------------------------------------------------------
// Conversions with no answer for some inputs (CAST targets). StarRocks
// semantics: a number converts to an integer by truncating toward zero, and
// a value outside the target's range, or text that isn't a number of the
// target's kind, is NULL. These kernels own their validity bitmap: register
// them with `null_strategy = .kernel_managed`.
// ---------------------------------------------------------------------------

/// A one-argument conversion kernel: `f` maps an input row to the target
/// value, or null when it has none. An input NULL stays NULL.
fn convertOrNull(comptime dst_field: []const u8, comptime f: anytype) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const base = out.data.rowCount();
            const dst = &@field(out.data, dst_field);
            try dst.ensureUnusedCapacity(allocator, row_count);
            for (0..row_count) |row| {
                const v = if (args[0].isValid(row)) f(args[0], row) else null;
                dst.appendAssumeCapacity(v orelse 0);
                try out.appendValidBit(allocator, base + row, v != null);
            }
        }
    }.kernel;
}

/// `x` truncated toward zero, or null when that is outside `T` or `x` isn't
/// finite.
pub fn truncatedInt(comptime T: type, x: f64) ?T {
    if (!std.math.isFinite(x)) return null;
    const t = @trunc(x);
    // -minInt(T) is 2^(bits-1): a power of two, so exact as an f64.
    const bound = -@as(f64, @floatFromInt(std.math.minInt(T)));
    if (t < -bound or t >= bound) return null;
    return @intFromFloat(t);
}

/// The CASTs to an integer from a double or text. An integer source narrows
/// by `cast.intNarrowKernel`.
fn IntCast(comptime T: type) type {
    return struct {
        const from_double = convertOrNull(intField(T), struct {
            fn f(v: ColumnView, row: usize) ?T {
                return truncatedInt(T, v.data.double[row]);
            }
        }.f);
        const from_text = convertOrNull(intField(T), struct {
            fn f(v: ColumnView, row: usize) ?T {
                return std.math.cast(T, common.textInteger(stringViewOf(v).rowBytes(row)) orelse return null);
            }
        }.f);
    };
}

pub const doubleToTinyintKernel = IntCast(i8).from_double;
pub const stringToTinyintKernel = IntCast(i8).from_text;
pub const doubleToSmallintKernel = IntCast(i16).from_double;
pub const stringToSmallintKernel = IntCast(i16).from_text;
pub const doubleToIntKernel = IntCast(i32).from_double;
pub const stringToIntKernel = IntCast(i32).from_text;
pub const doubleToBigintKernel = IntCast(i64).from_double;
pub const stringToBigintKernel = IntCast(i64).from_text;
pub const doubleToLargeintKernel = IntCast(i128).from_double;
pub const stringToLargeintKernel = IntCast(i128).from_text;

/// The MySQL dialect's `CAST(x AS SIGNED)` and `CAST(x AS UNSIGNED)`
/// (`cast.mysqlSigned`, `cast.mysqlUnsigned`) of a value `whole` reads as
/// an integer, null when it isn't one. UNSIGNED is a LARGEINT.
fn MysqlCast(comptime source: cast.MysqlCastSource, comptime whole: fn (ColumnView, usize) ?i128) type {
    return struct {
        const signed = convertOrNull("bigint", struct {
            fn f(v: ColumnView, row: usize) ?i64 {
                return cast.mysqlSigned(source, whole(v, row) orelse return null);
            }
        }.f);
        const unsigned = convertOrNull("largeint", struct {
            fn f(v: ColumnView, row: usize) ?i128 {
                const bits = cast.mysqlUnsigned(source, whole(v, row) orelse return null) orelse return null;
                return bits;
            }
        }.f);
    };
}

fn largeintWhole(v: ColumnView, row: usize) ?i128 {
    return v.data.largeint[row];
}

fn textWhole(v: ColumnView, row: usize) ?i128 {
    return common.textInteger(stringViewOf(v).rowBytes(row));
}

/// A double truncated toward zero. One past i128 still clamps, so it is
/// brought within it first.
fn doubleWhole(v: ColumnView, row: usize) ?i128 {
    const x = v.data.double[row];
    if (!std.math.isFinite(x)) return null;
    return truncatedInt(i128, std.math.clamp(x, -0x1p100, 0x1p100));
}

pub const largeintToMysqlSignedKernel = MysqlCast(.integer, largeintWhole).signed;
pub const largeintToMysqlUnsignedKernel = MysqlCast(.integer, largeintWhole).unsigned;
pub const stringToMysqlSignedKernel = MysqlCast(.integer, textWhole).signed;
pub const stringToMysqlUnsignedKernel = MysqlCast(.integer, textWhole).unsigned;
pub const doubleToMysqlSignedKernel = MysqlCast(.double, doubleWhole).signed;
pub const doubleToMysqlUnsignedKernel = MysqlCast(.double, doubleWhole).unsigned;

/// The MySQL dialect's `CAST(x AS UNSIGNED)` of a BIGINT: its 64 bits read
/// unsigned (`cast.mysqlUnsigned`), so `CAST(-1 AS UNSIGNED)` is 2^64 - 1.
pub fn bigintToMysqlUnsignedKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const dst = try reserveInts(i128, allocator, out, row_count);
    for (dst, args[0].data.bigint[0..row_count]) |*d, x| d.* = @as(u64, @bitCast(x));
}

pub const stringToDoubleKernel = convertOrNull("double", struct {
    fn f(v: ColumnView, row: usize) ?f64 {
        return common.textDouble(stringViewOf(v).rowBytes(row));
    }
}.f);

/// `x` as the nearest FLOAT, or null when it is past FLOAT's range.
pub fn narrowFloat(x: f64) ?f32 {
    const f: f32 = @floatCast(x);
    if (std.math.isInf(f) and !std.math.isInf(x)) return null;
    return f;
}

pub const doubleToFloatKernel = convertOrNull("float", struct {
    fn f(v: ColumnView, row: usize) ?f32 {
        return narrowFloat(v.data.double[row]);
    }
}.f);

pub const stringToFloatKernel = convertOrNull("float", struct {
    fn f(v: ColumnView, row: usize) ?f32 {
        return narrowFloat(common.textDouble(stringViewOf(v).rowBytes(row)) orelse return null);
    }
}.f);

pub const stringToBoolKernel = convertOrNull("boolean", struct {
    fn f(v: ColumnView, row: usize) ?u8 {
        return @intFromBool(common.textBoolean(stringViewOf(v).rowBytes(row)) orelse return null);
    }
}.f);

// ---------------------------------------------------------------------------
// Text where a number is expected and no CAST was written: MySQL reads the
// number the text starts with, so these always have an answer
// (`common.leadingDouble`, `common.leadingInteger`).
// ---------------------------------------------------------------------------

fn textAsNumber(comptime dst_field: []const u8, comptime read: anytype) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const dst = &@field(out.data, dst_field);
            try dst.ensureUnusedCapacity(allocator, row_count);
            var scratch: std.ArrayList(u8) = .empty;
            defer scratch.deinit(allocator);
            for (0..row_count) |row| {
                dst.appendAssumeCapacity(if (args[0].isValid(row)) read(try numericText(allocator, args[0], row, &scratch)) else 0);
            }
        }
    }.kernel;
}

/// The text a value reads as a number from: a JSON value's unquoted text,
/// with true and false as 1 and 0, as MySQL reads JSON as a number; other
/// text as it is.
fn numericText(allocator: Allocator, v: ColumnView, row: usize, scratch: *std.ArrayList(u8)) ![]const u8 {
    const bytes = stringViewOf(v).rowBytes(row);
    if (v.data != .json) return bytes;
    const norm = jb.normalize(allocator, bytes) catch return bytes;
    defer if (norm.owned) allocator.free(norm.bytes);
    scratch.clearRetainingCapacity();
    switch (jb.tagOf(norm.bytes)) {
        .true => try scratch.append(allocator, '1'),
        .false => try scratch.append(allocator, '0'),
        else => try jb.appendUnquoted(allocator, scratch, norm.bytes),
    }
    return scratch.items;
}

pub const textAsDoubleKernel = textAsNumber("double", common.leadingDouble);
pub const textAsBigintKernel = textAsNumber("bigint", common.leadingInteger);

/// A double passed where a function takes an integer: rounded half to even,
/// as MySQL reads it, and NULL past BIGINT, as a CAST is.
pub const doubleIntegerArgKernel = convertOrNull("bigint", struct {
    fn f(v: ColumnView, row: usize) ?i64 {
        return truncatedInt(i64, common.roundHalfEven(v.data.double[row]));
    }
}.f);

/// An integer of any width as its exact digits: a LARGEINT must not reach
/// text through DOUBLE, which keeps only 17 of its digits.
pub fn integerToStringKernel(comptime T: type) Kernel {
    return struct {
        fn f(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const ss = stringStoreOf(out);
            var buf: [48]u8 = undefined;
            for (@field(args[0].data, intField(T))[0..row_count]) |x| try ss.appendValue(allocator, try std.fmt.bufPrint(&buf, "{d}", .{x}));
        }
    }.f;
}

pub fn doubleToStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try appendFloatTexts(allocator, args[0].data.double[0..row_count], out);
}

pub fn floatToStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try appendFloatTexts(allocator, args[0].data.float[0..row_count], out);
}

fn appendFloatTexts(allocator: Allocator, values: anytype, out: *ColumnStore) !void {
    const ss = stringStoreOf(out);
    var buf: [common.FLOAT_TEXT_MAX]u8 = undefined;
    for (values) |x| try ss.appendValue(allocator, common.floatText(&buf, x, .plain));
}

/// A boolean as the integer it is, `1` or `0`, as MySQL and StarRocks write
/// it wherever a number becomes text.
pub fn boolToStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try appendBoolTexts(allocator, args[0].data.boolean[0..row_count], out, .{ "0", "1" });
}

/// PostgreSQL's CAST(bool AS TEXT): `true` or `false`.
pub fn boolToWordKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try appendBoolTexts(allocator, args[0].data.boolean[0..row_count], out, .{ "false", "true" });
}

fn appendBoolTexts(allocator: Allocator, values: []const u8, out: *ColumnStore, texts: [2][]const u8) !void {
    const ss = stringStoreOf(out);
    for (values) |b| try ss.appendValue(allocator, texts[@intFromBool(b != 0)]);
}

pub fn bigintToLargeintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.largeint.append(allocator, @as(i128, s[i]));
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

// ---------------------------------------------------------------------------
// MySQL misc: interval(n, n1, n2, ...) / sleep(seconds).
// ---------------------------------------------------------------------------

/// INTERVAL(n, n1, n2, ...): the index of the first bound above `n`, so the
/// count of bounds at most `n` when they ascend, as MySQL requires. A NULL
/// bound is passed over and a NULL `n` gives -1; the result is never NULL.
pub fn intervalKernel(comptime field: []const u8) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const base = out.data.rowCount();
            const dst = try reserveInts(i64, allocator, out, row_count);
            for (dst, 0..) |*d, i| {
                d.* = intervalIndex(field, args, i);
                try out.appendValidBit(allocator, base + i, true);
            }
        }
    }.kernel;
}

fn intervalIndex(comptime field: []const u8, args: []const ColumnView, row: usize) i64 {
    if (!args[0].isValid(row)) return -1;
    const n = @field(args[0].data, field)[row];
    for (args[1..], 0..) |bound, i| {
        if (bound.isValid(row) and @field(bound.data, field)[row] > n) return @intCast(i);
    }
    return @intCast(args.len - 1);
}

/// BENCHMARK(count, expr): 0, or NULL when `count` is NULL or negative, as
/// in MySQL. `expr` is evaluated once, as an argument, not `count` times.
pub fn benchmarkKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
    _ = out_type;
    const dst = &out.data.bigint;
    try dst.ensureUnusedCapacity(allocator, row_count);
    const base = out.data.rowCount();
    for (0..row_count) |row| {
        const counted = args[0].isValid(row) and !negativeCount(arg_types[0], args[0], row);
        dst.appendAssumeCapacity(0);
        try out.appendValidBit(allocator, base + row, counted);
    }
}

fn negativeCount(t: Type, v: ColumnView, row: usize) bool {
    return switch (v.data) {
        .float => |s| common.roundHalfEven(s[row]) < 0,
        .double => |s| common.roundHalfEven(s[row]) < 0,
        .string, .varchar, .char => common.leadingInteger(stringViewOf(v).rowBytes(row)) < 0,
        else => dec.integerArgAt(v, t, row) < 0,
    };
}

/// Longest single wait between checks for KILL QUERY or a dropped client.
const SLEEP_SLICE: std.Io.Duration = .fromMilliseconds(100);

/// SLEEP(seconds): waits once for every row it's evaluated on, then gives 0.
/// NULL, negative and NaN waits are errors, as in MySQL.
pub fn sleepKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    // Kernels carry no Io. The stdlib's process-wide instance is stateless
    // for a clock read and a sleep, the only calls made through it, so any
    // worker thread can use it.
    const io = std.Io.Threaded.global_single_threaded.io();
    const seconds = args[0].data.double;
    const base = out.data.rowCount();
    for (0..row_count) |i| {
        if (!args[0].isValid(i) or !(seconds[i] >= 0)) return error.IncorrectArgumentsToSleep;
        const wait_ns = @min(seconds[i] * std.time.ns_per_s, 1e18);
        const deadline = std.Io.Clock.awake.now(io).addDuration(.fromNanoseconds(@intFromFloat(wait_ns)));
        while (true) {
            try memory.checkCancelled(allocator);
            const left = std.Io.Clock.awake.now(io).durationTo(deadline);
            if (left.nanoseconds <= 0) break;
            const slice: std.Io.Duration = if (left.nanoseconds < SLEEP_SLICE.nanoseconds) left else SLEEP_SLICE;
            std.Io.sleep(io, slice, .awake) catch {};
        }
        try out.data.bigint.append(allocator, 0);
        try out.appendValidBit(allocator, base + i, true);
    }
}
