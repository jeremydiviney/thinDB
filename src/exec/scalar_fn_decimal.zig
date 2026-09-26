//! Scale-aware fixed-point DECIMAL kernels.
//!
//! A decimal column stores an integer mantissa (i64 for `decimal64`, i128 for
//! `decimal128`); the logical value is `mantissa / 10^scale`. Scale lives only
//! on the schema `Type` (`DecimalSpec`), never on the column data — so every
//! decimal operation needs the operand `Type`s, which is why these run on the
//! `TypedKernel` path (arg types + out type passed in) rather than the plain
//! `Kernel` path.
//!
//! Result precision/scale follow DuckDB (DESIGN.md §3.4). Arithmetic is exact:
//! operands are aligned to a common scale in i128, and a result that overflows
//! its declared precision raises `error.ArithmeticOverflow` (row-level), never
//! silently truncates. Mixed decimal/integer promotes the integer to
//! `DECIMAL(digits, 0)`; mixed decimal/float promotes both to `DOUBLE`.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const DecimalSpec = types.DecimalSpec;

const common = @import("scalar_fn_common.zig");
const ColumnView = common.ColumnView;
const ColumnStore = common.ColumnStore;

pub const MAX_PRECISION: u8 = 38;

// ---------------------------------------------------------------------------
// Scale arithmetic primitives
// ---------------------------------------------------------------------------

/// 10^n as i128. `n` must be ≤ 38 (10^38 < i128 max ≈ 1.7e38; 10^39 overflows).
pub fn pow10(n: u8) i128 {
    std.debug.assert(n <= MAX_PRECISION);
    var r: i128 = 1;
    var i: u8 = 0;
    while (i < n) : (i += 1) r *= 10;
    return r;
}

fn pow10f(n: u8) f64 {
    return std.math.pow(f64, 10.0, @floatFromInt(n));
}

/// Checked i128 multiply by 10^n. Null on overflow.
fn mulPow10(m: i128, n: u8) ?i128 {
    return std.math.mul(i128, m, pow10(n)) catch null;
}

/// Round `num / den` to the nearest integer, ties away from zero (the rounding
/// SQL ROUND and decimal rescale/division use). `den` must be non-zero.
/// Avoids doubling the remainder so it can't overflow near the i128 ceiling.
fn roundDiv(num: i128, den: i128) i128 {
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
    if (to_s == from_s) return m;
    if (to_s > from_s) return mulPow10(m, to_s - from_s);
    return roundDiv(m, pow10(from_s - to_s));
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
        .largeint => MAX_PRECISION,
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

/// Common `DecimalSpec` covering every operand (max integer-digits + max
/// scale), for COALESCE/IF/GREATEST/LEAST. Null if any operand is non-numeric.
pub fn commonSpec(arg_types: []const Type) ?DecimalSpec {
    var s: u8 = 0;
    var lead: u8 = 0;
    var any = false;
    for (arg_types) |t| {
        const sp = operandSpec(t) orelse return null;
        s = @max(s, sp.s);
        lead = @max(lead, sp.p - sp.s);
        any = true;
    }
    if (!any) return null;
    return .{ .p = @min(MAX_PRECISION, lead + s), .s = s };
}

/// Pick the narrowest backing for a `DECIMAL(p, s)`, clamping precision to 38.
pub fn decTypeFor(p_in: u8, s_in: u8) Type {
    const p = @min(p_in, MAX_PRECISION);
    const s = @min(s_in, p);
    return if (p <= 18) .{ .decimal64 = .{ .p = p, .s = s } } else .{ .decimal128 = .{ .p = p, .s = s } };
}

pub const Op = enum { add, sub, mul, div, mod };

/// Result `DecimalSpec` of `a <op> b` per DESIGN.md §3.4 (DuckDB rules).
fn arithSpec(op: Op, a: DecimalSpec, b: DecimalSpec) DecimalSpec {
    return switch (op) {
        .add, .sub, .mod => blk: {
            const s = @max(a.s, b.s);
            const lead = @max(a.p - a.s, b.p - b.s);
            break :blk .{ .p = @min(MAX_PRECISION, lead + s + 1), .s = s };
        },
        .mul => .{ .p = @min(MAX_PRECISION, a.p + b.p), .s = @min(MAX_PRECISION, a.s + b.s) },
        .div => .{ .p = @min(MAX_PRECISION, a.p + b.s + 4), .s = a.s + 4 },
    };
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

/// Integer/decimal mantissa of row `row`. Non-numeric → 0 (unreachable in
/// practice; resolution gates the operand types).
fn mantissaAt(v: ColumnView, row: usize) i128 {
    return switch (v.data) {
        .decimal64 => |s| s[row],
        .decimal128 => |s| s[row],
        .tinyint => |s| s[row],
        .smallint => |s| s[row],
        .int => |s| s[row],
        .bigint => |s| s[row],
        .largeint => |s| s[row],
        .boolean => |s| s[row],
        else => 0,
    };
}

fn scaleOf(t: Type) u8 {
    return if (t.decimalSpec()) |sp| sp.s else 0;
}

/// Logical f64 value of row `row` (mantissa scaled down for decimals).
fn f64At(v: ColumnView, t: Type, row: usize) f64 {
    return switch (v.data) {
        .decimal64 => |s| @as(f64, @floatFromInt(s[row])) / pow10f(scaleOf(t)),
        .decimal128 => |s| @as(f64, @floatFromInt(s[row])) / pow10f(scaleOf(t)),
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
fn appendDec(allocator: Allocator, out: *ColumnStore, out_type: Type, m: i128, valid: bool) !void {
    const sp = out_type.decimalSpec().?;
    const limit = pow10(sp.p);
    var v = m;
    if (v <= -limit or v >= limit) {
        if (valid) return error.ArithmeticOverflow;
        v = 0;
    }
    switch (out.data) {
        .decimal64 => try out.data.decimal64.append(allocator, @intCast(v)),
        .decimal128 => try out.data.decimal128.append(allocator, v),
        else => unreachable,
    }
}

// ---------------------------------------------------------------------------
// Arithmetic
// ---------------------------------------------------------------------------

fn arithDecimal(
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
        const a = mantissaAt(args[0], row);
        const b = mantissaAt(args[1], row);
        // A zero divisor's row is NULL (`.zero_divisor`), so it must not
        // raise an overflow while aligning its operands.
        const valid = rowValid(args, row) and !((op == .div or op == .mod) and b == 0);
        const mr: i128 = switch (op) {
            .add, .sub, .mod => blk: {
                const ns = @max(s0, s1);
                const aa = try orErr(mulPow10(a, ns - s0), valid);
                const bb = try orErr(mulPow10(b, ns - s1), valid);
                const nat = switch (op) {
                    .add => std.math.add(i128, aa, bb) catch (try overflowI(valid)),
                    .sub => std.math.sub(i128, aa, bb) catch (try overflowI(valid)),
                    .mod => if (bb == 0) 0 else @rem(aa, bb),
                    else => unreachable,
                };
                break :blk try orErr(rescale(nat, ns, sr), valid);
            },
            .mul => blk: {
                const nat = std.math.mul(i128, a, b) catch (try overflowI(valid));
                break :blk try orErr(rescale(nat, s0 + s1, sr), valid);
            },
            .div => blk: {
                if (b == 0) break :blk 0;
                // result scale sr = s0+4 ⇒ exponent sr+s1-s0 = s1+4 ≥ 0.
                const scaled = try orErr(mulPow10(a, sr + s1 - s0), valid);
                break :blk roundDiv(scaled, b);
            },
        };
        try appendDec(allocator, out, out_type, mr, valid);
    }
}

/// Resolve an overflow-detecting optional: the value if present; otherwise a
/// real error on a valid row, or 0 on a NULL row (validity discards it).
fn orErr(opt: ?i128, valid: bool) error{ArithmeticOverflow}!i128 {
    return opt orelse (try overflowI(valid));
}

fn overflowI(valid: bool) error{ArithmeticOverflow}!i128 {
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
                .decimal64, .decimal128 => try arithDecimal(op, allocator, arg_types, out_type, args, n, out),
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

// ---------------------------------------------------------------------------
// Casts: decimal -> X and X -> decimal
// ---------------------------------------------------------------------------

pub fn toDoubleKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    _ = out_type;
    var row: usize = 0;
    while (row < n) : (row += 1) try out.data.double.append(allocator, f64At(args[0], arg_types[0], row));
}

/// DECIMAL to an integer type: truncates toward zero, as StarRocks does; a
/// value outside the target's range is NULL. `.kernel_managed`.
pub fn toIntKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    const factor = pow10(scaleOf(arg_types[0]));
    const base = out.data.rowCount();
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const whole: ?i128 = if (args[0].isValid(row)) @divTrunc(mantissaAt(args[0], row), factor) else null;
        const stored = try appendIntFamily(allocator, out, out_type, whole);
        try out.appendValidBit(allocator, base + row, stored);
    }
}

/// Append `v` to an integer output, or 0 when it is null or outside the
/// output's range; whether `v` was stored.
fn appendIntFamily(allocator: Allocator, out: *ColumnStore, out_type: Type, v: ?i128) !bool {
    return switch (out_type) {
        .tinyint => appendInRange(i8, allocator, &out.data.tinyint, v),
        .smallint => appendInRange(i16, allocator, &out.data.smallint, v),
        .int => appendInRange(i32, allocator, &out.data.int, v),
        .bigint => appendInRange(i64, allocator, &out.data.bigint, v),
        .largeint => appendInRange(i128, allocator, &out.data.largeint, v),
        else => unreachable,
    };
}

fn appendInRange(comptime T: type, allocator: Allocator, list: anytype, v: ?i128) !bool {
    const fit: ?T = if (v) |x| std.math.cast(T, x) else null;
    try list.append(allocator, fit orelse 0);
    return fit != null;
}

/// `CAST(x AS DECIMAL(p,s))`. Source may be decimal (rescale), integer
/// (×10^s), float (round) or text (parse). Text that isn't a number and a
/// float that isn't finite are NULL, as in StarRocks; a value past the
/// target's precision raises, as in DuckDB. `.kernel_managed`.
pub fn toDecimalKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    const target = out_type.decimalSpec().?;
    const src = arg_types[0];
    const base = out.data.rowCount();
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const m: ?i128 = if (!args[0].isValid(row)) null else switch (src) {
            .decimal64, .decimal128 => try orErr(rescale(mantissaAt(args[0], row), scaleOf(src), target.s), true),
            .tinyint, .smallint, .int, .bigint, .largeint, .boolean => try orErr(mulPow10(mantissaAt(args[0], row), target.s), true),
            .float, .double => try floatMantissa(f64At(args[0], src, row), target.s),
            .varchar, .string, .char, .json => try textMantissa(common.stringViewOf(args[0]).rowBytes(row), target.s),
            else => return error.ComputeNoSuchOverload,
        };
        try appendDec(allocator, out, out_type, m orelse 0, m != null);
        try out.appendValidBit(allocator, base + row, m != null);
    }
}

/// A double as a mantissa at scale `s`, rounded half away from zero; null
/// when it isn't finite.
fn floatMantissa(x: f64, s: u8) error{ArithmeticOverflow}!?i128 {
    if (!std.math.isFinite(x)) return null;
    const scaled = @round(x * pow10f(s));
    if (@abs(scaled) >= 1e38) return error.ArithmeticOverflow;
    return @as(i128, @intFromFloat(scaled));
}

/// Text as a mantissa at scale `s`: any number `textNumber` reads, rounded
/// half away from zero; null when the text isn't a number.
fn textMantissa(text: []const u8, s: u8) error{ArithmeticOverflow}!?i128 {
    return switch (common.textNumber(text) orelse return null) {
        .exact => |d| rescale(d.m, d.s, s) orelse error.ArithmeticOverflow,
        .float => |f| floatMantissa(f, s),
    };
}

/// Text as a mantissa at scale `s` with nothing rounded away: null when the
/// text isn't a number or its value needs more fraction digits than `s`.
/// An exponent form holds when the double it reads is that mantissa's value.
fn exactTextMantissa(text: []const u8, s: u8) ?i128 {
    return switch (common.textNumber(text) orelse return null) {
        .exact => |d| if (d.s <= s) mulPow10(d.m, s - d.s) else blk: {
            const p = pow10(d.s - s);
            break :blk if (@rem(d.m, p) == 0) @divExact(d.m, p) else null;
        },
        .float => |f| blk: {
            const scaled = @round(f * pow10f(s));
            if (!(@abs(scaled) < 1e38)) break :blk null;
            const m: i128 = @intFromFloat(scaled);
            break :blk if (@as(f64, @floatFromInt(m)) / pow10f(s) == f) m else null;
        },
    };
}

/// A text value as a `ty` value when the comparison rule calls them equal,
/// exactly; null when no `ty` value equals the text. Integer targets are
/// narrowed by the caller.
fn textKeyValue(ty: Type, text: []const u8) ?i128 {
    return switch (ty) {
        .tinyint, .smallint, .int, .bigint, .largeint => exactTextMantissa(text, 0),
        .boolean => blk: {
            const v = exactTextMantissa(text, 0) orelse break :blk null;
            break :blk if (v == 0 or v == 1) v else null;
        },
        .decimal64, .decimal128 => |spec| blk: {
            const m = exactTextMantissa(text, spec.s) orelse break :blk null;
            break :blk if (@abs(m) < pow10(spec.p)) m else null;
        },
        .date => blk: {
            const micros = common.textToDatetime(std.mem.trim(u8, text, common.TEXT_SPACE)) orelse break :blk null;
            break :blk if (@mod(micros, std.time.us_per_day) == 0) @divExact(micros, std.time.us_per_day) else null;
        },
        .datetime => common.textToDatetime(std.mem.trim(u8, text, common.TEXT_SPACE)) orelse null,
        else => null,
    };
}

/// A text join key read as the other key's type (`join.zig`): each row is
/// the `out_type` value the text equals under the comparison rule, NULL when
/// there is none, so a hash on the result matches what `=` matches.
pub fn textKeyKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    _ = arg_types;
    const text = common.stringViewOf(args[0]);
    const base = out.data.rowCount();
    switch (out.data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint, .boolean, .date, .datetime, .decimal64, .decimal128 => |*list| {
            const T = @typeInfo(@TypeOf(list.items)).pointer.child;
            try list.ensureUnusedCapacity(allocator, n);
            for (0..n) |row| {
                const wide = if (args[0].isValid(row)) textKeyValue(out_type, text.rowBytes(row)) else null;
                const v: ?T = if (wide) |w| std.math.cast(T, w) else null;
                list.appendAssumeCapacity(v orelse 0);
                try out.appendValidBit(allocator, base + row, v != null);
            }
        },
        else => return error.ComputeNoSuchOverload,
    }
}

pub fn toStringKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    _ = out_type;
    const s = scaleOf(arg_types[0]);
    var buf: [64]u8 = undefined;
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const text = formatDecimal(&buf, mantissaAt(args[0], row), s);
        try common.stringStoreOf(out).appendValue(allocator, text);
    }
}

/// Render `mantissa / 10^s` as a fixed-point string into `buf`.
fn formatDecimal(buf: []u8, mantissa: i128, s: u8) []const u8 {
    if (s == 0) return std.fmt.bufPrint(buf, "{d}", .{mantissa}) catch "0";
    const neg = mantissa < 0;
    const mag: u128 = @abs(mantissa);
    const factor: u128 = @intCast(pow10(s));
    const int_part = mag / factor;
    const frac_part = mag % factor;
    return std.fmt.bufPrint(buf, "{[sign]s}{[int]d}.{[frac]d:0>[width]}", .{
        .sign = @as([]const u8, if (neg) "-" else ""),
        .int = int_part,
        .frac = frac_part,
        .width = @as(usize, s),
    }) catch "0";
}

// ---------------------------------------------------------------------------
// round / floor / ceil / truncate / abs
// ---------------------------------------------------------------------------

/// ROUND(decimal [, n]) → decimal at scale `target_s` (n clamped to source
/// scale; default 0). out_type carries the target scale.
pub fn roundKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    try rescaleToOut(allocator, arg_types, out_type, args, out, n, .round);
}

pub fn truncateKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    try rescaleToOut(allocator, arg_types, out_type, args, out, n, .trunc);
}

/// ROUND(decimal, n) / TRUNCATE(decimal, n): keep the source scale, but zero
/// (round/truncate) the digits below the n-th place. `n` is read per-row off
/// the second (literal) column, so the result type stays decimal(p, s).
pub fn roundNKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    try rescaleNInPlace(allocator, arg_types, out_type, args, out, n, .round);
}

pub fn truncateNKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    try rescaleNInPlace(allocator, arg_types, out_type, args, out, n, .trunc);
}

fn rescaleNInPlace(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize, mode: RoundMode) !void {
    const s = scaleOf(arg_types[0]);
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const places = nAt(args[1], row);
        var v = mantissaAt(args[0], row);
        if (places < s) {
            const drop: u8 = @intCast(@min(@as(i128, s), s - @as(i128, @max(places, 0))));
            const reduced = reduceScale(v, drop, mode);
            v = reduced * pow10(drop);
        }
        try appendDec(allocator, out, out_type, v, args[0].isValid(row));
    }
}

fn nAt(v: ColumnView, row: usize) i128 {
    return switch (v.data) {
        .tinyint => |s| s[row],
        .smallint => |s| s[row],
        .int => |s| s[row],
        .bigint => |s| s[row],
        else => 0,
    };
}

const RoundMode = enum { round, trunc, floor, ceil };

fn rescaleToOut(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize, mode: RoundMode) !void {
    const s = scaleOf(arg_types[0]);
    const ts = out_type.decimalSpec().?.s;
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const m = mantissaAt(args[0], row);
        const v = if (ts >= s) (try orErr(mulPow10(m, ts - s), args[0].isValid(row))) else reduceScale(m, s - ts, mode);
        try appendDec(allocator, out, out_type, v, args[0].isValid(row));
    }
}

fn reduceScale(m: i128, drop: u8, mode: RoundMode) i128 {
    const factor = pow10(drop);
    return switch (mode) {
        .round => roundDiv(m, factor),
        .trunc => @divTrunc(m, factor),
        .floor => @divFloor(m, factor),
        .ceil => -@divFloor(-m, factor),
    };
}

pub fn floorKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    try floorCeil(allocator, arg_types, out_type, args, out, n, .floor);
}

pub fn ceilKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    try floorCeil(allocator, arg_types, out_type, args, out, n, .ceil);
}

fn floorCeil(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize, mode: RoundMode) !void {
    const s = scaleOf(arg_types[0]);
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const v = reduceScale(mantissaAt(args[0], row), s, mode);
        try appendDec(allocator, out, out_type, v, args[0].isValid(row));
    }
}

pub fn absKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    _ = arg_types;
    var row: usize = 0;
    while (row < n) : (row += 1) try appendDec(allocator, out, out_type, @intCast(@abs(mantissaAt(args[0], row))), args[0].isValid(row));
}

// ---------------------------------------------------------------------------
// coalesce / ifnull / nullif / if / greatest / least
// ---------------------------------------------------------------------------

/// COALESCE/IFNULL over decimals — first valid arg, rescaled to out scale.
/// `.absorbs`: Compute fills the validity bitmap from whether all args were
/// null, so a row with no valid arg writes 0 (then gets marked null).
pub fn coalesceKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    const ts = out_type.decimalSpec().?.s;
    var row: usize = 0;
    while (row < n) : (row += 1) {
        var picked: ?i128 = null;
        for (args, arg_types) |a, t| {
            if (a.isValid(row)) {
                picked = rescale(mantissaAt(a, row), scaleOf(t), ts) orelse return error.ArithmeticOverflow;
                break;
            }
        }
        try appendDec(allocator, out, out_type, picked orelse 0, picked != null);
    }
}

/// NULLIF(a, b) → NULL when a == b (scale-aligned), else a. `.kernel_managed`:
/// writes the validity bitmap directly.
pub fn nullifKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    const ts = out_type.decimalSpec().?.s;
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const a_valid = args[0].isValid(row);
        const equal = a_valid and args[1].isValid(row) and decCompare(args[0], arg_types[0], args[1], arg_types[1], row) == .eq;
        const keep = a_valid and !equal;
        const m = if (keep) rescale(mantissaAt(args[0], row), scaleOf(arg_types[0]), ts) orelse return error.ArithmeticOverflow else 0;
        try appendDec(allocator, out, out_type, m, keep);
        if (out.nulls != null) try out.appendValidBit(allocator, out.data.rowCount() - 1, keep);
    }
}

/// IF(cond, a, b) over decimals. `.kernel_managed`. cond is a boolean column.
pub fn ifKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
    const ts = out_type.decimalSpec().?.s;
    const cond = args[0].data.boolean;
    var row: usize = 0;
    while (row < n) : (row += 1) {
        const take_then = args[0].isValid(row) and cond[row] != 0;
        const idx: usize = if (take_then) 1 else 2;
        const valid = args[idx].isValid(row);
        const m = if (valid) rescale(mantissaAt(args[idx], row), scaleOf(arg_types[idx]), ts) orelse return error.ArithmeticOverflow else 0;
        try appendDec(allocator, out, out_type, m, valid);
        if (out.nulls != null) try out.appendValidBit(allocator, out.data.rowCount() - 1, valid);
    }
}

fn greatestLeast(comptime want_max: bool) common.TypedKernelFn {
    return struct {
        fn k(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, n: usize) anyerror!void {
            const ts = out_type.decimalSpec().?.s;
            var row: usize = 0;
            while (row < n) : (row += 1) {
                var best_idx: usize = 0;
                for (args, 0..) |a, i| {
                    _ = a;
                    if (i == 0) continue;
                    const ord = decCompare(args[i], arg_types[i], args[best_idx], arg_types[best_idx], row);
                    if ((want_max and ord == .gt) or (!want_max and ord == .lt)) best_idx = i;
                }
                const m = rescale(mantissaAt(args[best_idx], row), scaleOf(arg_types[best_idx]), ts) orelse return error.ArithmeticOverflow;
                try appendDec(allocator, out, out_type, m, rowValid(args, row));
            }
        }
    }.k;
}

pub const greatestKernel = greatestLeast(true);
pub const leastKernel = greatestLeast(false);

/// Scale-aligned order of two decimal/integer operands at `row`. Aligns to the
/// higher scale in i128 (no precision loss).
pub fn decCompare(va: ColumnView, ta: Type, vb: ColumnView, tb: Type, row: usize) std.math.Order {
    const sa = scaleOf(ta);
    const sb = scaleOf(tb);
    const ns = @max(sa, sb);
    const a = mulPow10(mantissaAt(va, row), ns - sa) orelse return compareWide(va, ta, vb, tb, row);
    const b = mulPow10(mantissaAt(vb, row), ns - sb) orelse return compareWide(va, ta, vb, tb, row);
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
    try std.testing.expectEqual(@as(i128, 3), roundDiv(25, 10)); // 2.5 -> 3
    try std.testing.expectEqual(@as(i128, -3), roundDiv(-25, 10)); // -2.5 -> -3
    try std.testing.expectEqual(@as(i128, 2), roundDiv(24, 10)); // 2.4 -> 2
    try std.testing.expectEqual(@as(i128, 2), roundDiv(15, 10)); // 1.5 -> 2 (away from zero)
}

test "rescale widens exactly, narrows with rounding" {
    try std.testing.expectEqual(@as(?i128, 12300), rescale(123, 2, 4)); // 1.23 -> 1.2300
    try std.testing.expectEqual(@as(?i128, 12), rescale(1234, 4, 2)); // 0.1234 -> 0.12
    try std.testing.expectEqual(@as(?i128, 13), rescale(1250, 4, 2)); // 0.1250 -> 0.13 (tie up)
    try std.testing.expectEqual(@as(?i128, 123), rescale(123, 2, 2)); // identity
}

test "arithResultType follows DESIGN §3.4" {
    const a = types.decimal(10, 2);
    const b = types.decimal(8, 4);
    // add: max(p1-s1,p2-s2)+max(s1,s2)+1 = max(8,4)+4+1 = 13, scale 4
    try std.testing.expectEqual(DecimalSpec{ .p = 13, .s = 4 }, arithResultType(.add, a, b).decimalSpec().?);
    // mul: p1+p2=18, s1+s2=6
    try std.testing.expectEqual(DecimalSpec{ .p = 18, .s = 6 }, arithResultType(.mul, a, b).decimalSpec().?);
    // div: p1+s2+4=18, s1+4=6
    try std.testing.expectEqual(DecimalSpec{ .p = 18, .s = 6 }, arithResultType(.div, a, b).decimalSpec().?);
    // mixed with int: int promotes to (10,0)
    try std.testing.expectEqual(DecimalSpec{ .p = 13, .s = 2 }, arithResultType(.add, a, .int).decimalSpec().?);
    // mixed with float collapses to double
    try std.testing.expect(arithResultType(.mul, a, .double) == .double);
}

test "textMantissa and formatDecimal round-trip" {
    try std.testing.expectEqual(@as(?i128, 1234560), try textMantissa("1.23456", 6));
    try std.testing.expectEqual(@as(?i128, -50000), try textMantissa("-0.5", 5));
    try std.testing.expectEqual(@as(?i128, 1000000), try textMantissa(" 1 ", 6));
    try std.testing.expectEqual(@as(?i128, -101), try textMantissa("-1.005", 2));
    try std.testing.expectEqual(@as(?i128, 150), try textMantissa("1.5e0", 2));
    try std.testing.expectEqual(@as(?i128, null), try textMantissa("abc", 2));
    try std.testing.expectEqual(@as(?i128, null), try floatMantissa(std.math.inf(f64), 2));
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("1.230000", formatDecimal(&buf, 1230000, 6));
    try std.testing.expectEqualStrings("-0.50", formatDecimal(&buf, -50, 2));
    try std.testing.expectEqualStrings("42", formatDecimal(&buf, 42, 0));
}
