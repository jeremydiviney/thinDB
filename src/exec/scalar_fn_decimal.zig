//! Scale-aware fixed-point DECIMAL kernels.
//!
//! A decimal column stores an integer mantissa (i64 for `decimal64`, i128 for
//! `decimal128`, i256 for `decimal256`); the logical value is
//! `mantissa / 10^scale`. Scale lives only on the schema `Type`
//! (`DecimalSpec`), never on the column data — so every decimal operation
//! needs the operand `Type`s, which is why these run on the `TypedKernel`
//! path (arg types + out type passed in) rather than the plain `Kernel` path.
//!
//! Result precision/scale follow DESIGN.md §3.4. Arithmetic is exact:
//! operands are aligned to a common scale, and a result that overflows its
//! declared precision raises `error.ArithmeticOverflow` (row-level), never
//! silently truncates. Mixed decimal/integer promotes the integer to
//! `DECIMAL(digits, 0)`; mixed decimal/float promotes both to `DOUBLE`.
//!
//! Kernels work on i128 mantissas unless an operand or the result is a
//! `decimal256` (`needsWide`), so the narrower decimals never pay for i256
//! arithmetic.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const DecimalSpec = types.DecimalSpec;

const cast = @import("cast.zig");
const common = @import("scalar_fn_common.zig");
const time = @import("scalar_fn_time.zig");
const ColumnView = common.ColumnView;
const ColumnStore = common.ColumnStore;

const digitsOf = common.decimalDigitsOf;

// ---------------------------------------------------------------------------
// Scale arithmetic primitives
// ---------------------------------------------------------------------------

/// 10^n as i128. `n` must be ≤ 38 (10^38 < i128 max ≈ 1.7e38; 10^39 overflows).
pub fn pow10(n: u8) i128 {
    return pow10Of(i128, n);
}

/// 10^n as a `W`; `n` must be ≤ `digitsOf(W)`.
pub fn pow10Of(comptime W: type, n: u8) W {
    return Pow10(W).table[n];
}

fn Pow10(comptime W: type) type {
    return struct {
        const table = blk: {
            var t: [digitsOf(W) + 1]W = undefined;
            t[0] = 1;
            for (1..t.len) |i| t[i] = t[i - 1] * 10;
            break :blk t;
        };
    };
}

fn pow10f(n: u8) f64 {
    return std.math.pow(f64, 10.0, @floatFromInt(n));
}

/// Checked multiply by 10^n. Null on overflow.
fn mulPow10(comptime W: type, m: W, n: u8) ?W {
    if (n > digitsOf(W)) return if (m == 0) 0 else null;
    return std.math.mul(W, m, pow10Of(W, n)) catch null;
}

/// Round `num / den` to the nearest integer, ties away from zero (the rounding
/// SQL ROUND and decimal rescale/division use). `den` must be non-zero.
/// Avoids doubling the remainder so it can't overflow near the type's ceiling.
fn roundDiv(comptime W: type, num: W, den: W) W {
    const q = @divTrunc(num, den);
    const r = @rem(num, den);
    if (r == 0) return q;
    const abs_r = @abs(r);
    const abs_d = @abs(den);
    // ties away from zero: |r|*2 >= |den|, written without the doubling.
    if (abs_r >= abs_d - abs_r) {
        return if ((num < 0) != (den < 0)) q - 1 else q + 1;
    }
    return q;
}

/// Re-express mantissa `m` from `from_s` digits of scale to `to_s`. Widening
/// pads with zeros (exact); narrowing rounds ties away from zero. Null on the
/// (decimal-impossible) i128 overflow of the widening multiply.
pub fn rescale(m: i128, from_s: u8, to_s: u8) ?i128 {
    return rescaleOf(i128, m, from_s, to_s);
}

/// `rescale` over a `W` mantissa.
pub fn rescaleOf(comptime W: type, m: W, from_s: u8, to_s: u8) ?W {
    if (to_s == from_s) return m;
    if (to_s > from_s) return mulPow10(W, m, to_s - from_s);
    if (from_s - to_s > digitsOf(W)) return 0;
    return roundDiv(W, m, pow10Of(W, from_s - to_s));
}

/// Decimal digits an integer type spans, used to promote an integer operand to
/// `DECIMAL(digits, 0)` before decimal arithmetic.
fn intDigits(t: Type) ?u8 {
    return switch (t) {
        .boolean => 1,
        .tinyint => 3,
        .smallint => 5,
        .int => 10,
        .bigint => 19,
        .largeint => types.max_decimal128_precision,
        else => null,
    };
}

/// `DecimalSpec` an operand contributes to a decimal operation: its own spec if
/// decimal, else the integer-promotion spec. Null for non-numeric/float.
fn operandSpec(t: Type) ?DecimalSpec {
    if (t.decimalSpec()) |sp| return sp;
    if (intDigits(t)) |d| return .{ .p = d, .s = 0 };
    return null;
}

/// `DecimalSpec` an operand contributes to a decimal op (own spec, or the
/// integer-promotion spec). Null for float/string. Public for the resolver.
pub fn promoteSpec(t: Type) ?DecimalSpec {
    return operandSpec(t);
}

/// The most digits a result over operands of at most `widest` digits keeps:
/// 38, unless an operand already holds more, so a result over the narrower
/// decimals stays in an i128 as it did before DECIMAL reached 76 digits.
fn precisionCap(widest: u8) u8 {
    return if (widest > types.max_decimal128_precision) types.max_decimal_precision else types.max_decimal128_precision;
}

/// Common `DecimalSpec` covering every operand (max integer-digits + max
/// scale), for COALESCE/IF/GREATEST/LEAST. Null if any operand is non-numeric.
pub fn commonSpec(arg_types: []const Type) ?DecimalSpec {
    var s: u8 = 0;
    var lead: u8 = 0;
    var widest: u8 = 0;
    var any = false;
    for (arg_types) |t| {
        const sp = operandSpec(t) orelse return null;
        s = @max(s, sp.s);
        lead = @max(lead, sp.p - sp.s);
        widest = @max(widest, sp.p);
        any = true;
    }
    if (!any) return null;
    return .{ .p = @min(precisionCap(widest), lead + s), .s = s };
}

/// Pick the narrowest backing for a `DECIMAL(p, s)`, clamping precision to 76.
pub fn decTypeFor(p_in: u8, s_in: u8) Type {
    const p = @min(p_in, types.max_decimal_precision);
    return types.decimalOf(.{ .p = p, .s = @min(s_in, p) });
}

pub const Op = enum { add, sub, mul, div, mod };

/// Result `DecimalSpec` of `a <op> b` per DESIGN.md §3.4.
fn arithSpec(op: Op, a: DecimalSpec, b: DecimalSpec) DecimalSpec {
    const cap = precisionCap(@max(a.p, b.p));
    return switch (op) {
        .add, .sub, .mod => blk: {
            const s = @max(a.s, b.s);
            const lead = @max(a.p - a.s, b.p - b.s);
            break :blk .{ .p = @min(cap, lead + s + 1), .s = s };
        },
        .mul => .{ .p = @min(cap, a.p + b.p), .s = @min(cap, a.s + b.s) },
        .div => blk: {
            const s = quotientScale(a.s);
            // The quotient's integer digits grow by the divisor's scale.
            break :blk .{ .p = @min(cap, a.p - a.s + b.s + s), .s = s };
        },
    };
}

/// StarRocks' quotient scale, which only the dividend's scale decides: six
/// more digits up to scale 12, then the dividend's own. ROUND over a quotient
/// sees these digits, so `ROUND(int / rate)` matches StarRocks' integer.
pub fn quotientScale(dividend_s: u8) u8 {
    if (dividend_s <= 6) return dividend_s + 6;
    return @max(dividend_s, 12);
}

/// Compute the result `Type` of an arithmetic op over the given arg types.
/// Any float operand makes the whole expression `DOUBLE`; otherwise the
/// decimal §3.4 rules apply (integers promoted to scale-0 decimals).
pub fn arithResultType(op: Op, a: Type, b: Type) Type {
    if (a.isFloat() or b.isFloat()) return .double;
    const sa = operandSpec(a) orelse return .double;
    const sb = operandSpec(b) orelse return .double;
    const r = arithSpec(op, sa, sb);
    return decTypeFor(r.p, r.s);
}

// ---------------------------------------------------------------------------
// Operand readers
// ---------------------------------------------------------------------------

/// Whether a kernel over `arg_types` into `out_type` works on i256
/// mantissas: when any of them is a `decimal256`. Every other decimal and
/// integer fits an i128.
fn needsWide(arg_types: []const Type, out_type: Type) bool {
    if (out_type == .decimal256) return true;
    for (arg_types) |t| if (t == .decimal256) return true;
    return false;
}

/// A typed kernel running `body` on i256 mantissas when `needsWide`, on
/// i128 ones otherwise.
fn widthDispatched(comptime body: anytype) common.TypedKernelFn {
    return &struct {
        fn k(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
            if (needsWide(arg_types, out_type)) return body(i256, allocator, arg_types, out_type, args, out, n);
            return body(i128, allocator, arg_types, out_type, args, out, n);
        }
    }.k;
}

/// Integer/decimal mantissa of row `row`. Non-numeric → 0 (unreachable in
/// practice; resolution gates the operand types). An i128 reading of a
/// `decimal256` past its range saturates, which keeps its sign and leaves it
/// past every narrower target's range.
fn mantissaOf(comptime W: type, v: ColumnView, row: usize) W {
    return switch (v.data) {
        .decimal64 => |s| s[row],
        .decimal128 => |s| s[row],
        .decimal256 => |s| std.math.cast(W, s[row]) orelse if (s[row] < 0) std.math.minInt(W) else std.math.maxInt(W),
        .tinyint => |s| s[row],
        .smallint => |s| s[row],
        .int => |s| s[row],
        .bigint => |s| s[row],
        .largeint => |s| s[row],
        .boolean => |s| s[row],
        else => 0,
    };
}

fn mantissaAt(v: ColumnView, row: usize) i128 {
    return mantissaOf(i128, v, row);
}

fn scaleOf(t: Type) u8 {
    return if (t.decimalSpec()) |sp| sp.s else 0;
}

/// Logical f64 value of row `row` (mantissa scaled down for decimals).
fn f64At(v: ColumnView, t: Type, row: usize) f64 {
    return switch (v.data) {
        .decimal64 => |s| @as(f64, @floatFromInt(s[row])) / pow10f(scaleOf(t)),
        .decimal128 => |s| @as(f64, @floatFromInt(s[row])) / pow10f(scaleOf(t)),
        .decimal256 => |s| types.floatOfInt(s[row]) / pow10f(scaleOf(t)),
        .float => |s| s[row],
        .double => |s| s[row],
        .tinyint => |s| @floatFromInt(s[row]),
        .smallint => |s| @floatFromInt(s[row]),
        .int => |s| @floatFromInt(s[row]),
        .bigint => |s| @floatFromInt(s[row]),
        .largeint => |s| @floatFromInt(s[row]),
        .boolean => |s| @floatFromInt(s[row]),
        else => 0,
    };
}

fn rowValid(args: []const ColumnView, row: usize) bool {
    for (args) |a| if (!a.isValid(row)) return false;
    return true;
}

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

/// Append `m` into a decimal output store; range-checks against the declared
/// precision and raises on a valid-row overflow. Invalid (NULL) rows whose
/// garbage mantissa overflows store 0 — the validity bitmap discards them.
fn appendDec(comptime W: type, allocator: Allocator, out: *ColumnStore, out_type: Type, m: W, valid: bool) !void {
    const sp = out_type.decimalSpec().?;
    var v = m;
    // A precision past what every `W` holds bounds no `W` value.
    if (sp.p <= digitsOf(W)) {
        const limit = pow10Of(W, sp.p);
        if (v <= -limit or v >= limit) {
            if (valid) return error.ArithmeticOverflow;
            v = 0;
        }
    }
    switch (out.data) {
        .decimal64 => try out.data.decimal64.append(allocator, @intCast(v)),
        .decimal128 => try out.data.decimal128.append(allocator, @intCast(v)),
        .decimal256 => try out.data.decimal256.append(allocator, v),
        else => unreachable,
    }
}

// ---------------------------------------------------------------------------
// Arithmetic
// ---------------------------------------------------------------------------

fn arithDecimal(
    comptime W: type,
    op: Op,
    allocator: Allocator,
    arg_types: []const Type,
    out_type: Type,
    args: []const ColumnView,
    n: usize,
    out: *ColumnStore,
) !void {
    const s0 = scaleOf(arg_types[0]);
    const s1 = scaleOf(arg_types[1]);
    const sr = out_type.decimalSpec().?.s;
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const a = mantissaOf(W, args[0], row);
        const b = mantissaOf(W, args[1], row);
        // A zero divisor's row is NULL (`.zero_divisor`), so it must not
        // raise an overflow while aligning its operands.
        const valid = rowValid(args, row) and !((op == .div or op == .mod) and b == 0);
        const mr: W = switch (op) {
            .add, .sub, .mod => blk: {
                const ns = @max(s0, s1);
                const aa = try orErr(W, mulPow10(W, a, ns - s0), valid);
                const bb = try orErr(W, mulPow10(W, b, ns - s1), valid);
                const nat = switch (op) {
                    .add => std.math.add(W, aa, bb) catch (try overflowI(W, valid)),
                    .sub => std.math.sub(W, aa, bb) catch (try overflowI(W, valid)),
                    .mod => if (bb == 0) 0 else @rem(aa, bb),
                    else => unreachable,
                };
                break :blk try orErr(W, rescaleOf(W, nat, ns, sr), valid);
            },
            .mul => blk: {
                const nat = std.math.mul(W, a, b) catch (try overflowI(W, valid));
                break :blk try orErr(W, rescaleOf(W, nat, s0 + s1, sr), valid);
            },
            .div => blk: {
                if (b == 0) break :blk 0;
                // quotientScale(s0) ≥ s0, so the exponent sr+s1-s0 is ≥ 0.
                const scaled = try orErr(W, mulPow10(W, a, sr + s1 - s0), valid);
                break :blk roundDiv(W, scaled, b);
            },
        };
        try appendDec(W, allocator, out, out_type, mr, valid);
    }
}

/// Resolve an overflow-detecting optional: the value if present; otherwise a
/// real error on a valid row, or 0 on a NULL row (validity discards it).
fn orErr(comptime W: type, opt: ?W, valid: bool) error{ArithmeticOverflow}!W {
    return opt orelse (try overflowI(W, valid));
}

fn overflowI(comptime W: type, valid: bool) error{ArithmeticOverflow}!W {
    if (valid) return error.ArithmeticOverflow;
    return 0;
}

fn arithDouble(
    op: Op,
    allocator: Allocator,
    arg_types: []const Type,
    args: []const ColumnView,
    n: usize,
    out: *ColumnStore,
) !void {
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const a = f64At(args[0], arg_types[0], row);
        const b = f64At(args[1], arg_types[1], row);
        const r = switch (op) {
            .add => a + b,
            .sub => a - b,
            .mul => a * b,
            .div => if (b == 0) 0 else a / b,
            .mod => if (b == 0) 0 else @rem(a, b),
        };
        try out.data.double.append(allocator, r);
    }
}

fn arithImpl(
    comptime op: Op,
) common.TypedKernelFn {
    return struct {
        fn k(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
            switch (out_type) {
                .decimal64, .decimal128 => if (needsWide(arg_types, out_type))
                    try arithDecimal(i256, op, allocator, arg_types, out_type, args, n, out)
                else
                    try arithDecimal(i128, op, allocator, arg_types, out_type, args, n, out),
                .decimal256 => try arithDecimal(i256, op, allocator, arg_types, out_type, args, n, out),
                .double => try arithDouble(op, allocator, arg_types, args, n, out),
                else => unreachable,
            }
        }
    }.k;
}

pub const addKernel = arithImpl(.add);
pub const subKernel = arithImpl(.sub);
pub const mulKernel = arithImpl(.mul);
pub const divKernel = arithImpl(.div);
pub const modKernel = arithImpl(.mod);

/// `a DIV b` over a float or decimal operand, as MySQL divides it: exactly,
/// with a double read as its shortest digits, so `0.3e0 DIV 0.1e0` is 3.
/// The quotient truncates toward zero into a BIGINT and raises past its
/// range. `.zero_divisor`.
pub fn intDivKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const divides = rowValid(args, row) and f64At(args[1], arg_types[1], row) != 0;
        const q = if (!divides) 0 else if (needsWide(arg_types, out_type))
            try truncatedQuotient(i256, arg_types, args, row)
        else
            try truncatedQuotient(i128, arg_types, args, row);
        try out.data.bigint.append(allocator, q);
    }
}

/// An operand as an exact scaled integer; null for a double past DECIMAL's
/// range.
pub fn exactAt(v: ColumnView, t: Type, row: usize) ?common.ScaledInt {
    return exactOf(i128, v, t, row);
}

fn exactOf(comptime W: type, v: ColumnView, t: Type, row: usize) ?common.ScaledIntOf(W) {
    if (t.isFloat()) return common.floatDigitsOf(W, f64At(v, t, row));
    return .{ .m = mantissaOf(W, v, row), .s = scaleOf(t) };
}

fn truncatedQuotient(comptime W: type, arg_types: []const Type, args: []const ColumnView, row: usize) error{ArithmeticOverflow}!i64 {
    exact: {
        const a = exactOf(W, args[0], arg_types[0], row) orelse break :exact;
        const b = exactOf(W, args[1], arg_types[1], row) orelse break :exact;
        const s = @max(a.s, b.s);
        const num = mulPow10(W, a.m, s - a.s) orelse break :exact;
        const den = mulPow10(W, b.m, s - b.s) orelse break :exact;
        const q = std.math.divTrunc(W, num, den) catch break :exact;
        return std.math.cast(i64, q) orelse error.ArithmeticOverflow;
    }
    // Operands too wide to align exactly, or a divisor below DECIMAL's
    // smallest step, divide as doubles.
    const q = @trunc(f64At(args[0], arg_types[0], row) / f64At(args[1], arg_types[1], row));
    if (!(q >= -0x1p63 and q < 0x1p63)) return error.ArithmeticOverflow;
    return @intFromFloat(q);
}

// ---------------------------------------------------------------------------
// Casts: decimal -> X and X -> decimal
// ---------------------------------------------------------------------------

pub fn toDoubleKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    _ = out_type;
    var row: usize = 0;
    while (row < n) : (row += 1) try out.data.double.append(allocator, f64At(args[0], arg_types[0], row));
}

/// DECIMAL to FLOAT: the nearest f32 of its value; one past FLOAT's range
/// is an infinity.
pub fn toFloatKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    _ = out_type;
    var row: usize = 0;
    while (row < n) : (row += 1) try out.data.float.append(allocator, @floatCast(f64At(args[0], arg_types[0], row)));
}

/// DECIMAL to an integer type: truncates toward zero, as StarRocks does; a
/// value outside the target's range is NULL. `.kernel_managed`.
pub const toIntKernel = widthDispatched(toInt);

fn toInt(comptime W: type, allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    const factor = pow10Of(W, scaleOf(arg_types[0]));
    const base = out.data.rowCount();
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const whole: ?W = if (args[0].isValid(row)) @divTrunc(mantissaOf(W, args[0], row), factor) else null;
        const stored = try appendIntFamily(W, allocator, out, out_type, whole);
        try out.appendValidBit(allocator, base + row, stored);
    }
}

/// The MySQL dialect's `CAST(x AS SIGNED)` (a BIGINT) and
/// `CAST(x AS UNSIGNED)` (a LARGEINT) of a decimal's whole part
/// (`cast.mysqlSigned`, `cast.mysqlUnsigned`). One with scale 0 reads as an
/// integer, which an integer literal past BIGINT is in MySQL. `.kernel_managed`.
pub const toMysqlSignedKernel = widthDispatched(MysqlCastKernel(.signed).kernel);
pub const toMysqlUnsignedKernel = widthDispatched(MysqlCastKernel(.unsigned).kernel);

fn MysqlCastKernel(comptime spelling: cast.IntegerSpelling) type {
    return struct {
        fn kernel(comptime W: type, allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
            _ = out_type;
            const scale = scaleOf(arg_types[0]);
            const source: cast.MysqlCastSource = if (scale == 0) .integer else .decimal;
            const factor = pow10Of(W, scale);
            const dst = switch (spelling) {
                .signed => &out.data.bigint,
                .unsigned => &out.data.largeint,
            };
            const base = out.data.rowCount();
            try dst.ensureUnusedCapacity(allocator, n);
            for (0..n) |row| {
                const whole = saturate(i128, @divTrunc(mantissaOf(W, args[0], row), factor));
                const v = if (!args[0].isValid(row)) null else switch (spelling) {
                    .signed => cast.mysqlSigned(source, whole),
                    .unsigned => if (cast.mysqlUnsigned(source, whole)) |bits| @as(i128, bits) else null,
                };
                dst.appendAssumeCapacity(v orelse 0);
                try out.appendValidBit(allocator, base + row, v != null);
            }
        }
    };
}

/// `v` as a `T`, clamped to its range.
fn saturate(comptime T: type, v: anytype) T {
    return std.math.cast(T, v) orelse if (v < 0) std.math.minInt(T) else std.math.maxInt(T);
}

/// Append `v` to an integer output, or 0 when it is null or outside the
/// output's range; whether `v` was stored.
fn appendIntFamily(comptime W: type, allocator: Allocator, out: *ColumnStore, out_type: Type, v: ?W) !bool {
    return switch (out_type) {
        .tinyint => appendInRange(i8, W, allocator, &out.data.tinyint, v),
        .smallint => appendInRange(i16, W, allocator, &out.data.smallint, v),
        .int => appendInRange(i32, W, allocator, &out.data.int, v),
        .bigint => appendInRange(i64, W, allocator, &out.data.bigint, v),
        .largeint => appendInRange(i128, W, allocator, &out.data.largeint, v),
        else => unreachable,
    };
}

fn appendInRange(comptime T: type, comptime W: type, allocator: Allocator, list: anytype, v: ?W) !bool {
    const fit: ?T = if (v) |x| std.math.cast(T, x) else null;
    try list.append(allocator, fit orelse 0);
    return fit != null;
}

/// `CAST(x AS DECIMAL(p,s))`. Source may be decimal (rescale), integer
/// (×10^s), float (round) or text (parse). Text that isn't a number and a
/// float that isn't finite are NULL, as in StarRocks; a value past the
/// target's precision raises, as in DuckDB. `.kernel_managed`.
pub const toDecimalKernel = widthDispatched(toDecimal);

fn toDecimal(comptime W: type, allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    const target = out_type.decimalSpec().?;
    const src = arg_types[0];
    const base = out.data.rowCount();
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const m: ?W = if (!args[0].isValid(row)) null else switch (src) {
            .decimal64, .decimal128, .decimal256 => try orErr(W, rescaleOf(W, mantissaOf(W, args[0], row), scaleOf(src), target.s), true),
            .tinyint, .smallint, .int, .bigint, .largeint, .boolean => try orErr(W, mulPow10(W, mantissaOf(W, args[0], row), target.s), true),
            .float, .double => try floatMantissa(W, f64At(args[0], src, row), target.s),
            .varchar, .string, .char, .json, .nested, .bitmap => try textMantissa(W, common.stringViewOf(args[0]).rowBytes(row), target.s),
            .date => try orErr(W, mulPow10(W, common.dateNumber(args[0].data.date[row]), target.s), true),
            .datetime => blk: {
                const number = common.datetimeNumber(args[0].data.datetime[row]);
                break :blk try orErr(W, rescaleOf(W, number.m, number.s, target.s), true);
            },
            else => return error.ComputeNoSuchOverload,
        };
        try appendDec(W, allocator, out, out_type, m orelse 0, m != null);
        try out.appendValidBit(allocator, base + row, m != null);
    }
}

/// A double as a mantissa at scale `s`: its shortest digits
/// (`common.floatDigits`) rounded half away from zero; null when it isn't
/// finite.
fn floatMantissa(comptime W: type, x: f64, s: u8) error{ArithmeticOverflow}!?W {
    if (!std.math.isFinite(x)) return null;
    const d = common.floatDigitsOf(W, x) orelse return error.ArithmeticOverflow;
    return rescaleOf(W, d.m, d.s, s) orelse error.ArithmeticOverflow;
}

/// Text as a mantissa at scale `s`: any number `textNumber` reads, rounded
/// half away from zero; null when the text isn't a number.
fn textMantissa(comptime W: type, text: []const u8, s: u8) error{ArithmeticOverflow}!?W {
    return switch (common.textNumberOf(W, text) orelse return null) {
        .exact => |d| rescaleOf(W, d.m, d.s, s) orelse error.ArithmeticOverflow,
        .float => |f| floatMantissa(W, f, s),
    };
}

/// A decimal as DOUBLE, as the cast and the float kernels read it.
pub fn mantissaToDouble(m: i256, s: u8) f64 {
    return types.floatOfInt(m) / pow10f(s);
}

/// A text constant as `CAST(text AS DECIMAL(p,s))` reads it, for folding
/// the cast at plan time: null where the cast gives NULL or raises. Within
/// `spec`'s precision, so it fits the mantissa type of `spec`'s decimal.
pub fn textConstantMantissa(text: []const u8, spec: DecimalSpec) ?i256 {
    const m = (textMantissa(i256, text, spec.s) catch return null) orelse return null;
    const limit = pow10Of(i256, spec.p);
    return if (m > -limit and m < limit) m else null;
}

/// Text as a mantissa at scale `s` with nothing rounded away: null when the
/// text isn't a number or its value needs more fraction digits than `s`.
/// An exponent form reads as the double's shortest digits.
fn exactTextMantissa(comptime W: type, text: []const u8, s: u8) ?W {
    const d: common.ScaledIntOf(W) = switch (common.textNumberOf(W, text) orelse return null) {
        .exact => |d| d,
        .float => |f| common.floatDigitsOf(W, f) orelse return null,
    };
    return exactRescale(W, d.m, d.s, s);
}

/// A mantissa at scale `from_s` moved to scale `to_s` with nothing rounded
/// away: null when the value needs more fraction digits than `to_s`, or
/// overflows.
fn exactRescale(comptime W: type, m: W, from_s: u8, to_s: u8) ?W {
    if (from_s <= to_s) return mulPow10(W, m, to_s - from_s);
    if (from_s - to_s > digitsOf(W)) return if (m == 0) 0 else null;
    const p = pow10Of(W, from_s - to_s);
    return if (@rem(m, p) == 0) @divExact(m, p) else null;
}

/// A text value as a `ty` value when the comparison rule calls them equal,
/// exactly; null when no `ty` value equals the text. Integer targets are
/// narrowed by the caller.
fn textKeyValue(comptime W: type, ty: Type, text: []const u8) ?W {
    return switch (ty) {
        .tinyint, .smallint, .int, .bigint, .largeint => exactTextMantissa(W, text, 0),
        .boolean => blk: {
            const v = exactTextMantissa(W, text, 0) orelse break :blk null;
            break :blk if (v == 0 or v == 1) v else null;
        },
        .decimal64, .decimal128, .decimal256 => |spec| blk: {
            const m = exactTextMantissa(W, text, spec.s) orelse break :blk null;
            break :blk withinPrecision(W, ty, m);
        },
        .date => blk: {
            const micros = (time.parseDatetime(text) orelse break :blk null).value;
            break :blk if (@mod(micros, std.time.us_per_day) == 0) @divExact(micros, std.time.us_per_day) else null;
        },
        .datetime => if (time.parseDatetime(text)) |m| m.value else null,
        else => null,
    };
}

/// A mantissa at `ty`'s scale as a `ty` value: null when `ty` is a decimal
/// too narrow for it. Integer targets are narrowed by the caller.
fn withinPrecision(comptime W: type, ty: Type, m: W) ?W {
    const spec = ty.decimalSpec() orelse return m;
    if (spec.p > digitsOf(W)) return m;
    return if (@abs(m) < pow10Of(W, spec.p)) m else null;
}

fn textKeyAt(comptime W: type, src: Type, target: Type, arg: ColumnView, row: usize) ?W {
    _ = src;
    return textKeyValue(W, target, common.stringViewOf(arg).rowBytes(row));
}

fn numberKeyAt(comptime W: type, src: Type, target: Type, arg: ColumnView, row: usize) ?W {
    return withinPrecision(W, target, exactRescale(W, mantissaOf(W, arg, row), scaleOf(src), scaleOf(target)) orelse return null);
}

fn RoundedKey(comptime up: bool) type {
    return struct {
        fn at(comptime W: type, src: Type, target: Type, arg: ColumnView, row: usize) ?W {
            const m = mantissaOf(W, arg, row);
            const from_s = scaleOf(src);
            const to_s = scaleOf(target);
            if (from_s <= to_s) return withinPrecision(W, target, mulPow10(W, m, to_s - from_s) orelse return null);
            if (from_s - to_s > digitsOf(W)) return withinPrecision(W, target, if (up and m > 0) 1 else if (!up and m < 0) -1 else 0);
            const p = pow10Of(W, from_s - to_s);
            const down = @divFloor(m, p);
            return withinPrecision(W, target, if (up and @mod(m, p) != 0) down + 1 else down);
        }
    };
}

/// A join key read as the other key's type (`join.zig`): each row is the
/// `out_type` value `valueAt` reads from it, NULL where it reads none.
fn keyKernel(comptime valueAt: anytype) common.TypedKernelFn {
    return widthDispatched(struct {
        fn kernel(comptime W: type, allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
            const base = out.data.rowCount();
            switch (out.data) {
                inline .tinyint, .smallint, .int, .bigint, .largeint, .boolean, .date, .datetime, .decimal64, .decimal128, .decimal256 => |*list| {
                    const T = @typeInfo(@TypeOf(list.items)).pointer.child;
                    try list.ensureUnusedCapacity(allocator, n);
                    for (0..n) |row| {
                        const wide = if (args[0].isValid(row)) valueAt(W, arg_types[0], out_type, args[0], row) else null;
                        const v: ?T = if (wide) |w| std.math.cast(T, w) else null;
                        list.appendAssumeCapacity(v orelse 0);
                        try out.appendValidBit(allocator, base + row, v != null);
                    }
                },
                else => return error.ComputeNoSuchOverload,
            }
        }
    }.kernel);
}

/// A text key as the value it equals under the comparison rule, NULL when
/// there is none, so a hash on the result matches what `=` matches.
pub const textKeyKernel = keyKernel(textKeyAt);
/// A number key as the value it equals exactly, NULL when the target holds
/// none: a key past a decimal's range meets none of its values.
pub const numberKeyKernel = keyKernel(numberKeyAt);
/// A number key rounded down to the target's scale.
pub const floorKeyKernel = keyKernel(RoundedKey(false).at);
/// A number key rounded up to the target's scale.
pub const ceilKeyKernel = keyKernel(RoundedKey(true).at);

/// The longest text `formatDecimal` writes: a sign, the 77 digits of an
/// i256 and a point.
pub const DECIMAL_TEXT_MAX = 80;

pub fn toStringKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    _ = out_type;
    const s = scaleOf(arg_types[0]);
    var buf: [DECIMAL_TEXT_MAX]u8 = undefined;
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const text = switch (args[0].data) {
            .decimal256 => |m| formatDecimal(&buf, m[row], s),
            else => formatDecimal(&buf, mantissaAt(args[0], row), s),
        };
        try common.stringStoreOf(out).appendValue(allocator, text);
    }
}

/// HEX(decimal): the value read as an integer (`integerArgAt`), printed
/// as `common.integerHex` prints one.
///
/// An integer literal past BIGINT is a DECIMAL of scale 0 here, but MySQL
/// reads one up to 2^64 - 1 as BIGINT UNSIGNED, so a scale-0 value in that
/// range keeps its bits (`HEX(18446744073709551615)` is FFFFFFFFFFFFFFFF).
pub fn hexKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    _ = out_type;
    const t = arg_types[0];
    var buf: [16]u8 = undefined;
    for (0..n) |row| {
        const v = if (scaleOf(t) == 0) common.wideIntegerAsBigint(mantissaAt(args[0], row)) else integerArgAt(args[0], t, row);
        try common.stringStoreOf(out).appendValue(allocator, common.integerHex(&buf, v));
    }
}

/// `__iceberg_transform_bucket(d, n)`: Iceberg hashes a DECIMAL as its
/// unscaled value's shortest big-endian two's complement, the bytes Java's
/// `BigInteger.toByteArray` writes.
pub fn icebergBucketKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    var bytes: [32]u8 = undefined;
    for (0..n) |row| {
        const m = mantissaOf(i256, args[0], row);
        std.mem.writeInt(i256, &bytes, m, .big);
        var len: usize = 1;
        while (len < bytes.len and !fitsSignedBytes(m, len)) len += 1;
        const valid = args[0].isValid(row) and args[1].isValid(row);
        try out.data.int.append(allocator, if (valid) try common.icebergBucket(bytes[bytes.len - len ..], nAt(args[1], row)) else 0);
    }
}

fn fitsSignedBytes(m: i256, len: usize) bool {
    const bound = @as(i256, 1) << @intCast(len * 8 - 1);
    return m >= -bound and m < bound;
}

/// `__iceberg_transform_truncate(d, w)`: the unscaled value dropped to the
/// multiple of w at or below it, at d's scale, as Iceberg truncates a
/// DECIMAL.
pub fn icebergTruncateKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    _ = arg_types;
    for (0..n) |row| {
        const valid = args[0].isValid(row) and args[1].isValid(row);
        const width = nAt(args[1], row);
        if (valid and width < 1) return error.InvalidIcebergTransformWidth;
        const m = mantissaOf(i256, args[0], row);
        try appendDec(i256, allocator, out, out_type, if (valid) m - @mod(m, @as(i256, width)) else 0, valid);
    }
}

/// A DECIMAL where MySQL reads it as an integer (`HEX(2.5)`, BENCHMARK's
/// count): rounded half away from zero, clamped to the BIGINT range.
pub fn integerArgAt(v: ColumnView, t: Type, row: usize) i64 {
    if (t == .decimal256) return saturate(i64, roundDiv(i256, mantissaOf(i256, v, row), pow10Of(i256, scaleOf(t))));
    return saturate(i64, roundDiv(i128, mantissaAt(v, row), pow10(scaleOf(t))));
}

/// A DECIMAL passed where a function takes an integer: rounded as
/// `integerArgAt` rounds it, and NULL past BIGINT (`cast.narrowInt`).
pub const integerArgKernel = widthDispatched(integerArg);

fn integerArg(comptime W: type, allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    _ = out_type;
    const dst = &out.data.bigint;
    const base = out.data.rowCount();
    const den = pow10Of(W, scaleOf(arg_types[0]));
    try dst.ensureUnusedCapacity(allocator, n);
    for (0..n) |row| {
        const v = if (args[0].isValid(row)) cast.narrowInt(i64, roundDiv(W, mantissaOf(W, args[0], row), den)) else null;
        dst.appendAssumeCapacity(v orelse 0);
        try out.appendValidBit(allocator, base + row, v != null);
    }
}

/// MySQL's `FORMAT(x, d)`: `x` rounded to `d` places (clamped to 0..30),
/// its integer part grouped by thousands with `,`. A DECIMAL or an integer
/// rounds half away from zero; a float rounds its binary value half to even,
/// as MySQL rounds a double, so `FORMAT(1.005e0, 2)` is `1.00`. A value
/// that rounds to zero drops its sign.
pub fn formatKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    _ = out_type;
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(allocator);
    var row: usize = 0;
    while (row < n) : (row += 1) {
        text.clearRetainingCapacity();
        if (rowValid(args, row)) {
            const places: u8 = @intCast(std.math.clamp(mantissaAt(args[1], row), 0, 30));
            try appendFormatted(allocator, &text, arg_types[0], args[0], row, places);
        }
        try common.stringStoreOf(out).appendValue(allocator, text.items);
    }
}

fn appendFormatted(allocator: Allocator, text: *std.ArrayList(u8), t: Type, v: ColumnView, row: usize, places: u8) !void {
    if (!t.isFloat()) {
        if (rescaleOf(i256, mantissaOf(i256, v, row), scaleOf(t), places)) |m| return appendGroupedMantissa(allocator, text, m, places);
    }
    const x = f64At(v, t, row);
    const scaled = roundHalfEven(x * pow10f(places));
    if (@abs(scaled) < 1e38) return appendGroupedMantissa(allocator, text, @as(i128, @intFromFloat(scaled)), places);
    // Past DECIMAL's range: the double's whole digits, grouped.
    var digits: std.ArrayList(u8) = .empty;
    defer digits.deinit(allocator);
    try digits.print(allocator, "{d:.0}", .{x});
    try appendGrouped(allocator, text, digits.items, "");
}

fn roundHalfEven(x: f64) f64 {
    return if (@abs(x - @trunc(x)) == 0.5) 2 * @round(x / 2) else @round(x);
}

fn appendGroupedMantissa(allocator: Allocator, text: *std.ArrayList(u8), m: i256, places: u8) !void {
    const mag: u256 = @abs(m);
    const factor: u256 = @intCast(pow10Of(i256, places));
    var whole_buf: [DECIMAL_TEXT_MAX]u8 = undefined;
    const whole = std.fmt.bufPrint(&whole_buf, "{s}{d}", .{ @as([]const u8, if (m < 0) "-" else ""), mag / factor }) catch unreachable;
    var frac_buf: [40]u8 = undefined;
    const frac = std.fmt.bufPrint(&frac_buf, "{d:0>[1]}", .{ mag % factor, @as(usize, places) }) catch unreachable;
    try appendGrouped(allocator, text, whole, if (places == 0) "" else frac);
}

/// `whole` (digits after an optional `-`) grouped by thousands, then `frac`.
fn appendGrouped(allocator: Allocator, text: *std.ArrayList(u8), whole: []const u8, frac: []const u8) !void {
    const negative = whole.len > 0 and whole[0] == '-';
    const digits = if (negative) whole[1..] else whole;
    if (negative) try text.append(allocator, '-');
    for (digits, 0..) |c, i| {
        if (i > 0 and (digits.len - i) % 3 == 0) try text.append(allocator, ',');
        try text.append(allocator, c);
    }
    if (frac.len == 0) return;
    try text.append(allocator, '.');
    try text.appendSlice(allocator, frac);
}

/// Render `mantissa / 10^s` as a fixed-point string into `buf`, whose
/// `DECIMAL_TEXT_MAX` bytes hold any mantissa up to i256 at any scale to 76.
pub fn formatDecimal(buf: *[DECIMAL_TEXT_MAX]u8, mantissa: anytype, s: u8) []const u8 {
    const T = @TypeOf(mantissa);
    const W = if (T != comptime_int and @bitSizeOf(T) > 128) i256 else i128;
    const U = std.meta.Int(.unsigned, @bitSizeOf(W));
    const m: W = mantissa;
    if (s == 0) return std.fmt.bufPrint(buf, "{d}", .{m}) catch unreachable;
    const neg = m < 0;
    const mag: U = @abs(m);
    const factor: U = @intCast(pow10Of(W, s));
    const int_part = mag / factor;
    const frac_part = mag % factor;
    return std.fmt.bufPrint(buf, "{[sign]s}{[int]d}.{[frac]d:0>[width]}", .{
        .sign = @as([]const u8, if (neg) "-" else ""),
        .int = int_part,
        .frac = frac_part,
        .width = @as(usize, s),
    }) catch unreachable;
}

// ---------------------------------------------------------------------------
// round / floor / ceil / truncate / abs
// ---------------------------------------------------------------------------

/// ROUND(decimal [, n]) → decimal at scale `target_s` (n clamped to source
/// scale; default 0). out_type carries the target scale.
pub const roundKernel = widthDispatched(RescaleToOut(.round).kernel);
pub const truncateKernel = widthDispatched(RescaleToOut(.trunc).kernel);

/// ROUND(decimal, n) / TRUNCATE(decimal, n): round (or truncate) away the
/// digits below the n-th place; a negative `n` reaches left of the point.
/// `n` is read per row, so the result keeps the source scale unless a
/// literal `n` narrowed out_type to scale max(n, 0).
pub const roundNKernel = widthDispatched(RescaleNInPlace(.round).kernel);
pub const truncateNKernel = widthDispatched(RescaleNInPlace(.trunc).kernel);

fn RescaleNInPlace(comptime mode: RoundMode) type {
    return struct {
        fn kernel(comptime W: type, allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
            const s = scaleOf(arg_types[0]);
            const ts = scaleOf(out_type);
            var row: usize = 0;
            while (row < n) : (row += 1) {
                const places = nAt(args[1], row);
                const m = mantissaOf(W, args[0], row);
                // out_type's scale is at least max(places, 0) whenever places < s.
                const v: W = if (places >= s)
                    m
                else if (s - places > digitsOf(W))
                    0
                else
                    reduceScale(W, m, @intCast(s - places), mode) * pow10Of(W, @intCast(ts - places));
                try appendDec(W, allocator, out, out_type, v, args[0].isValid(row));
            }
        }
    };
}

fn nAt(v: ColumnView, row: usize) i128 {
    return switch (v.data) {
        .tinyint => |s| s[row],
        .smallint => |s| s[row],
        .int => |s| s[row],
        .bigint => |s| s[row],
        .largeint => |s| s[row],
        else => 0,
    };
}

const RoundMode = enum { round, trunc, floor, ceil };

fn RescaleToOut(comptime mode: RoundMode) type {
    return struct {
        fn kernel(comptime W: type, allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
            const s = scaleOf(arg_types[0]);
            const ts = out_type.decimalSpec().?.s;
            var row: usize = 0;
            while (row < n) : (row += 1) {
                const m = mantissaOf(W, args[0], row);
                const v = if (ts >= s) (try orErr(W, mulPow10(W, m, ts - s), args[0].isValid(row))) else reduceScale(W, m, s - ts, mode);
                try appendDec(W, allocator, out, out_type, v, args[0].isValid(row));
            }
        }
    };
}

fn reduceScale(comptime W: type, m: W, drop: u8, mode: RoundMode) W {
    const factor = pow10Of(W, drop);
    return switch (mode) {
        .round => roundDiv(W, m, factor),
        .trunc => @divTrunc(m, factor),
        .floor => @divFloor(m, factor),
        .ceil => -@divFloor(-m, factor),
    };
}

pub const floorKernel = widthDispatched(FloorCeil(.floor).kernel);
pub const ceilKernel = widthDispatched(FloorCeil(.ceil).kernel);

fn FloorCeil(comptime mode: RoundMode) type {
    return struct {
        fn kernel(comptime W: type, allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
            const s = scaleOf(arg_types[0]);
            var row: usize = 0;
            while (row < n) : (row += 1) {
                const v = reduceScale(W, mantissaOf(W, args[0], row), s, mode);
                try appendDec(W, allocator, out, out_type, v, args[0].isValid(row));
            }
        }
    };
}

pub const absKernel = widthDispatched(abs);

fn abs(comptime W: type, allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    _ = arg_types;
    var row: usize = 0;
    while (row < n) : (row += 1) try appendDec(W, allocator, out, out_type, @intCast(@abs(mantissaOf(W, args[0], row))), args[0].isValid(row));
}

// ---------------------------------------------------------------------------
// coalesce / ifnull / nullif / if / greatest / least
// ---------------------------------------------------------------------------

/// COALESCE/IFNULL over decimals — first valid arg, rescaled to out scale.
/// `.absorbs`: Compute fills the validity bitmap from whether all args were
/// null, so a row with no valid arg writes 0 (then gets marked null).
pub const coalesceKernel = widthDispatched(coalesce);

fn coalesce(comptime W: type, allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    const ts = out_type.decimalSpec().?.s;
    var row: usize = 0;
    while (row < n) : (row += 1) {
        var picked: ?W = null;
        for (args, arg_types) |a, t| {
            if (a.isValid(row)) {
                picked = rescaleOf(W, mantissaOf(W, a, row), scaleOf(t), ts) orelse return error.ArithmeticOverflow;
                break;
            }
        }
        try appendDec(W, allocator, out, out_type, picked orelse 0, picked != null);
    }
}

/// NULLIF(a, b) → NULL when a == b (scale-aligned), else a. `.kernel_managed`:
/// writes the validity bitmap directly.
pub const nullifKernel = widthDispatched(nullif);

fn nullif(comptime W: type, allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    const ts = out_type.decimalSpec().?.s;
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const a_valid = args[0].isValid(row);
        const equal = a_valid and args[1].isValid(row) and decCompare(W, args[0], arg_types[0], args[1], arg_types[1], row) == .eq;
        const keep = a_valid and !equal;
        const m = if (keep) rescaleOf(W, mantissaOf(W, args[0], row), scaleOf(arg_types[0]), ts) orelse return error.ArithmeticOverflow else 0;
        try appendDec(W, allocator, out, out_type, m, keep);
        if (out.nulls != null) try out.appendValidBit(allocator, out.data.rowCount() - 1, keep);
    }
}

/// IF(cond, a, b) over decimals. `.kernel_managed`. cond is a boolean column.
pub const ifKernel = widthDispatched(ifChoice);

fn ifChoice(comptime W: type, allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    const ts = out_type.decimalSpec().?.s;
    const cond = args[0].data.boolean;
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const take_then = args[0].isValid(row) and cond[row] != 0;
        const idx: usize = if (take_then) 1 else 2;
        const valid = args[idx].isValid(row);
        const m = if (valid) rescaleOf(W, mantissaOf(W, args[idx], row), scaleOf(arg_types[idx]), ts) orelse return error.ArithmeticOverflow else 0;
        try appendDec(W, allocator, out, out_type, m, valid);
        if (out.nulls != null) try out.appendValidBit(allocator, out.data.rowCount() - 1, valid);
    }
}

fn GreatestLeast(comptime want_max: bool) type {
    return struct {
        fn kernel(comptime W: type, allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
            const ts = out_type.decimalSpec().?.s;
            var row: usize = 0;
            while (row < n) : (row += 1) {
                var best_idx: usize = 0;
                for (1..args.len) |i| {
                    const ord = decCompare(W, args[i], arg_types[i], args[best_idx], arg_types[best_idx], row);
                    if ((want_max and ord == .gt) or (!want_max and ord == .lt)) best_idx = i;
                }
                const m = rescaleOf(W, mantissaOf(W, args[best_idx], row), scaleOf(arg_types[best_idx]), ts) orelse return error.ArithmeticOverflow;
                try appendDec(W, allocator, out, out_type, m, rowValid(args, row));
            }
        }
    };
}

pub const greatestKernel = widthDispatched(GreatestLeast(true).kernel);
pub const leastKernel = widthDispatched(GreatestLeast(false).kernel);

/// Scale-aligned order of two decimal/integer operands at `row`. Aligns to the
/// higher scale (no precision loss).
fn decCompare(comptime W: type, va: ColumnView, ta: Type, vb: ColumnView, tb: Type, row: usize) std.math.Order {
    const sa = scaleOf(ta);
    const sb = scaleOf(tb);
    const ns = @max(sa, sb);
    const a = mulPow10(W, mantissaOf(W, va, row), ns - sa) orelse return compareWide(va, ta, vb, tb, row);
    const b = mulPow10(W, mantissaOf(W, vb, row), ns - sb) orelse return compareWide(va, ta, vb, tb, row);
    return std.math.order(a, b);
}

/// Overflow fallback for `decCompare`: compare as f64 (only reached for
/// pathologically large aligned mantissas, which decimals can't hold anyway).
fn compareWide(va: ColumnView, ta: Type, vb: ColumnView, tb: Type, row: usize) std.math.Order {
    return types.floatOrder(f64At(va, ta, row), f64At(vb, tb, row));
}

// ---------------------------------------------------------------------------
// Tests — pure scale arithmetic
// ---------------------------------------------------------------------------

test "pow10 and roundDiv (ties away from zero)" {
    try std.testing.expectEqual(@as(i128, 1), pow10(0));
    try std.testing.expectEqual(@as(i128, 1_000_000), pow10(6));
    try std.testing.expectEqual(@as(i128, 3), roundDiv(i128, 25, 10)); // 2.5 -> 3
    try std.testing.expectEqual(@as(i128, -3), roundDiv(i128, -25, 10)); // -2.5 -> -3
    try std.testing.expectEqual(@as(i128, 2), roundDiv(i128, 24, 10)); // 2.4 -> 2
    try std.testing.expectEqual(@as(i128, 2), roundDiv(i128, 15, 10)); // 1.5 -> 2 (away from zero)
    try std.testing.expectEqual(std.math.powi(i256, 10, 76) catch unreachable, pow10Of(i256, 76));
    try std.testing.expectEqual(@as(?i256, null), mulPow10(i256, pow10Of(i256, 40), 37));
}

test "rescale widens exactly, narrows with rounding" {
    try std.testing.expectEqual(@as(?i128, 12300), rescale(123, 2, 4)); // 1.23 -> 1.2300
    try std.testing.expectEqual(@as(?i128, 12), rescale(1234, 4, 2)); // 0.1234 -> 0.12
    try std.testing.expectEqual(@as(?i128, 13), rescale(1250, 4, 2)); // 0.1250 -> 0.13 (tie up)
    try std.testing.expectEqual(@as(?i128, 123), rescale(123, 2, 2)); // identity
    try std.testing.expectEqual(@as(?i256, pow10Of(i256, 70)), rescaleOf(i256, 1, 0, 70));
    try std.testing.expectEqual(@as(?i256, 2), rescaleOf(i256, 15 * pow10Of(i256, 59), 60, 0));
}

test "arithResultType follows DESIGN §3.4" {
    const a = types.decimal(10, 2);
    const b = types.decimal(8, 4);
    // add: max(p1-s1,p2-s2)+max(s1,s2)+1 = max(8,4)+4+1 = 13, scale 4
    try std.testing.expectEqual(DecimalSpec{ .p = 13, .s = 4 }, arithResultType(.add, a, b).decimalSpec().?);
    // mul: p1+p2=18, s1+s2=6
    try std.testing.expectEqual(DecimalSpec{ .p = 18, .s = 6 }, arithResultType(.mul, a, b).decimalSpec().?);
    // div: scale s1+6=8, precision p1-s1+s2+8=20
    try std.testing.expectEqual(DecimalSpec{ .p = 20, .s = 8 }, arithResultType(.div, a, b).decimalSpec().?);
    // an int dividend is DECIMAL(10,0): scale 0+6, precision 10+4+6=20
    try std.testing.expectEqual(DecimalSpec{ .p = 20, .s = 6 }, arithResultType(.div, .int, b).decimalSpec().?);
    // mixed with int: int promotes to (10,0)
    try std.testing.expectEqual(DecimalSpec{ .p = 13, .s = 2 }, arithResultType(.add, a, .int).decimalSpec().?);
    // mixed with float collapses to double
    try std.testing.expect(arithResultType(.mul, a, .double) == .double);
}

test "results over operands of at most 38 digits stay at 38; a wider operand reaches 76" {
    const narrow = types.decimal(30, 2);
    const wide = types.decimal(40, 10);
    try std.testing.expectEqual(Type{ .decimal128 = .{ .p = 38, .s = 4 } }, arithResultType(.mul, narrow, narrow));
    try std.testing.expectEqual(Type{ .decimal256 = .{ .p = 70, .s = 12 } }, arithResultType(.mul, narrow, wide));
    try std.testing.expectEqual(Type{ .decimal256 = .{ .p = 41, .s = 10 } }, arithResultType(.add, wide, .bigint));
    try std.testing.expectEqual(Type{ .decimal256 = .{ .p = 76, .s = 10 } }, arithResultType(.add, types.decimal(76, 10), wide));
    try std.testing.expectEqual(DecimalSpec{ .p = 40, .s = 10 }, commonSpec(&.{ wide, .int }).?);
    try std.testing.expectEqual(DecimalSpec{ .p = 38, .s = 2 }, commonSpec(&.{ narrow, .largeint }).?);
}

test "textMantissa and formatDecimal round-trip" {
    try std.testing.expectEqual(@as(?i128, 1234560), try textMantissa(i128, "1.23456", 6));
    try std.testing.expectEqual(@as(?i128, -50000), try textMantissa(i128, "-0.5", 5));
    try std.testing.expectEqual(@as(?i128, 1000000), try textMantissa(i128, " 1 ", 6));
    try std.testing.expectEqual(@as(?i128, -101), try textMantissa(i128, "-1.005", 2));
    try std.testing.expectEqual(@as(?i128, 150), try textMantissa(i128, "1.5e0", 2));
    try std.testing.expectEqual(@as(?i128, null), try textMantissa(i128, "abc", 2));
    try std.testing.expectEqual(@as(?i128, null), try floatMantissa(i128, std.math.inf(f64), 2));
    try std.testing.expectEqual(@as(?i128, 101), try floatMantissa(i128, 1.005, 2));
    try std.testing.expectEqual(@as(?i128, -268), try floatMantissa(i128, -2.675, 2));
    try std.testing.expectEqual(@as(?i128, 30), try floatMantissa(i128, @as(f64, 0.1) + @as(f64, 0.2), 2));
    try std.testing.expectEqual(@as(?i128, 101), try textMantissa(i128, "1.005e0", 2));
    var buf: [DECIMAL_TEXT_MAX]u8 = undefined;
    try std.testing.expectEqualStrings("1.230000", formatDecimal(&buf, 1230000, 6));
    try std.testing.expectEqualStrings("-0.50", formatDecimal(&buf, -50, 2));
    try std.testing.expectEqualStrings("42", formatDecimal(&buf, 42, 0));
}

test "76-digit text reads exactly and prints back" {
    const nines = "9" ** 56 ++ "." ++ "9" ** 20;
    const m = (try textMantissa(i256, nines, 20)).?;
    try std.testing.expectEqual(pow10Of(i256, 76) - 1, m);
    var buf: [DECIMAL_TEXT_MAX]u8 = undefined;
    try std.testing.expectEqualStrings(nines, formatDecimal(&buf, m, 20));
    try std.testing.expectEqualStrings("-" ++ nines, formatDecimal(&buf, -m, 20));
    try std.testing.expectEqual(@as(?i256, null), textConstantMantissa(nines, .{ .p = 75, .s = 20 }));
    try std.testing.expectEqual(@as(?i256, m), textConstantMantissa(nines, .{ .p = 76, .s = 20 }));
    // Past 76 digits the text reads as a double, too wide for any DECIMAL.
    try std.testing.expectError(error.ArithmeticOverflow, textMantissa(i256, "1" ++ "0" ** 76, 0));
    try std.testing.expectEqual(@as(?i256, 15 * pow10Of(i256, 49)), try floatMantissa(i256, 1.5e50, 0));
}

test "quotientScale follows the dividend's scale only" {
    const cases = .{
        .{ .s = 0, .expected = 6 },   .{ .s = 2, .expected = 8 },   .{ .s = 6, .expected = 12 },
        .{ .s = 8, .expected = 12 },  .{ .s = 12, .expected = 12 }, .{ .s = 14, .expected = 14 },
        .{ .s = 30, .expected = 30 },
    };
    inline for (cases) |c| try std.testing.expectEqual(@as(u8, c.expected), quotientScale(c.s));
}
