//! Scalar function registry + resolver. Per-category kernels live in
//! sibling files (scalar_fn_string.zig, scalar_fn_math.zig, etc.).
//!
//! A `ScalarFn` is the runtime descriptor for one overload: name +
//! expected arg types + return type + kernel. Multiple `ScalarFn`
//! entries can share a name — `resolve` picks an exact-type match if
//! possible; otherwise it falls back to implicit-cast cost ranking
//! via cast.zig.
//!
//! Kernel contract (per file in scalar_fn_*.zig):
//!   - Inputs are `[]const ColumnView` aligned to the function's args.
//!     Each view has `row_count` rows.
//!   - Output is a `*ColumnStore` of the function's declared
//!     `return_type`. The kernel appends exactly `row_count` rows.
//!   - Null propagation: by default, if ANY input row is null, the
//!     output row is null. The Compute operator handles the
//!     bookkeeping — kernels can assume non-null inputs. `coalesce` /
//!     `ifnull` are flagged with `null_strategy = .absorbs`; `nullif`
//!     uses `.kernel_managed` (kernel writes the bitmap itself).
//!     Division (`/`, `DIV`, `%`, `PMOD`) uses `.zero_divisor`: Compute
//!     also nulls each row whose divisor is 0, so those kernels need only
//!     not trap on it.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const TypeTag = types.TypeTag;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const store = @import("../engine/store.zig");
const ColumnStore = store.ColumnStore;

const Expr = @import("expr.zig").Expr;

const cast = @import("cast.zig");
const CastKernel = cast.CastKernel;
const udf_mod = @import("../udf.zig");

// Kernel implementations are split into category files. Imported here for
// the builtins[] registry; the rest of the codebase only sees ScalarFn /
// resolve.
const string = @import("scalar_fn_string.zig");
const math = @import("scalar_fn_math.zig");
const date = @import("scalar_fn_date.zig");
const datefmt = @import("scalar_fn_datefmt.zig");
const time = @import("scalar_fn_time.zig");
const cond = @import("scalar_fn_cond.zig");
const dec = @import("scalar_fn_decimal.zig");
const json = @import("scalar_fn_json.zig");
const inet = @import("scalar_fn_inet.zig");
const common = @import("scalar_fn_common.zig");

pub const NullStrategy = udf_mod.NullStrategy;
pub const TypedKernel = common.TypedKernelFn;

pub const Kernel = *const fn (
    allocator: Allocator,
    args: []const ColumnView,
    out: *ColumnStore,
    row_count: usize,
) anyerror!void;

pub const ScalarFn = struct {
    name: []const u8,
    arg_types: []const Type,
    return_type: Type,
    /// When set, the overload accepts any arity >= this value. `arg_types`
    /// is treated as a repeating prototype and expanded by the resolver into
    /// a call-specific descriptor before Compute plans casts/buffers.
    variadic_min_args: ?usize = null,
    /// How many leading `arg_types` of a variadic overload are fixed
    /// parameters that the prototype doesn't repeat: `ELT(n, s1, s2, ...)`
    /// takes one number, then strings.
    variadic_fixed: usize = 0,
    null_strategy: NullStrategy = .propagates,
    volatility: udf_mod.Volatility = .immutable,
    kernel: ?Kernel = null,
    /// Decimal (scale-aware) kernel. Takes precedence over `kernel`; receives
    /// the call's arg `Type`s and the resolved output `Type` so it can read and
    /// produce the correct scale. Set only by `resolveDecimal`.
    typed_kernel: ?TypedKernel = null,
    udf_kernel: ?udf_mod.ScalarKernel = null,
    user_data: ?*anyopaque = null,
};

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Resolved overload + per-arg coercion plan returned by `resolve`.
///
/// On an EXACT match, `arg_casts` is null — the fast path avoids any
/// per-arg work in the resolver and per-batch work in the executor.
/// On an IMPLICIT-CAST match, `arg_casts[i]` is the cast kernel to
/// apply to arg `i` (or null if that arg didn't need coercion).
pub const ResolvedOverload = struct {
    func: ScalarFn,
    arg_casts: ?[]const ?CastKernel,
};

/// Look up the best-matching overload for `name(arg_types)`.
///
/// Resolution order:
///   1. Exact `TypeTag` match → return immediately (zero overhead).
///   2. Implicit-cast match: for each name-matching overload, check that
///      every arg can implicitly cast to the declared type and sum the
///      per-arg costs (see cast.zig). Pick the lowest-cost overload;
///      ties → first-registered.
///   3. No castable overload → null (caller surfaces ComputeNoSuchOverload).
///
/// Width metadata (varchar length, decimal precision) doesn't affect
/// selection — only the TypeTag matters.
pub fn resolve(
    aa: Allocator,
    name: []const u8,
    arg_types: []const Type,
) !?ResolvedOverload {
    return resolveWithRegistry(aa, null, name, arg_types);
}

pub fn resolveWithRegistry(
    aa: Allocator,
    registry: ?*const udf_mod.UdfRegistry,
    name_in: []const u8,
    arg_types: []const Type,
) !?ResolvedOverload {
    if (try resolvePgBoolText(aa, name_in, arg_types)) |ov| return ov;
    const name = if (std.mem.eql(u8, name_in, PG_TEXT_FN)) "to_string" else name_in;
    // Decimal-involving calls resolve to scale-aware typed kernels (the static
    // builtins table can't express a dynamic output scale). Checked first so a
    // decimal operand never falls into an int/double overload that ignores scale.
    if (try resolveJson(aa, name, arg_types)) |ov| return ov;
    if (try resolveDecimal(aa, name, arg_types)) |ov| return ov;
    if (try resolveIntArith(aa, name, arg_types)) |ov| return ov;
    if (try resolveFractionalIntDiv(aa, name, arg_types)) |ov| return ov;
    if (try resolveFormat(aa, name, arg_types)) |ov| return ov;
    if (try resolveTimeFromNumbers(aa, name, arg_types)) |ov| return ov;
    if (try resolveTimeOfNumber(aa, name, arg_types)) |ov| return ov;
    if (try resolveCastTime(aa, name, arg_types)) |ov| return ov;
    if (try resolveSingleRow(aa, name, arg_types)) |ov| return ov;
    if (try resolveCharset(aa, name, arg_types)) |ov| return ov;
    if (try resolveBenchmark(aa, name, arg_types)) |ov| return ov;
    if (try resolveTextKey(aa, name, arg_types)) |ov| return ov;
    if (try resolveRowKey(aa, name, arg_types)) |ov| return ov;
    if (try resolveOrderKey(aa, name, arg_types)) |ov| return ov;
    if (try resolveHexLiteralAs(aa, name, arg_types)) |ov| return ov;

    // Fast path: exact TypeTag match. No allocation, no cost calc.
    for (builtins) |f| {
        if (!std.ascii.eqlIgnoreCase(f.name, name)) continue;
        if (!scalarArityMatches(f, arg_types.len)) continue;
        var all_match = true;
        for (arg_types, 0..) |given, i| {
            if (!bindsAsIs(given, scalarDeclaredTypeAt(f, i))) {
                all_match = false;
                break;
            }
        }
        if (all_match) return ResolvedOverload{ .func = try expandScalarFn(aa, f, arg_types.len), .arg_casts = null };
    }
    if (registry) |reg| {
        for (reg.scalarEntries()) |entry| {
            if (!std.ascii.eqlIgnoreCase(entry.name, name)) continue;
            if (!udf_mod.sameTypeTags(entry.arg_types, arg_types)) continue;
            return ResolvedOverload{ .func = scalarFromUdf(entry), .arg_casts = null };
        }
    }

    // Slow path: rank by cumulative cast cost. Iterate name-matching
    // overloads, compute cost, keep the cheapest. Integer arguments narrow
    // to a narrower integer parameter only when nothing matched by widening.
    var best: ?ScalarFn = null;
    for ([_]bool{ false, true }) |allow_narrowing| {
        var best_cost: u64 = std.math.maxInt(u64);
        for (builtins) |f| {
            if (!std.ascii.eqlIgnoreCase(f.name, name)) continue;
            const total_cost = scalarCastCost(f, arg_types, allow_narrowing) orelse continue;
            if (total_cost < best_cost) {
                best_cost = total_cost;
                best = f;
            }
        }
        if (registry) |reg| {
            for (reg.scalarEntries()) |entry| {
                if (!std.ascii.eqlIgnoreCase(entry.name, name)) continue;
                if (entry.arg_types.len != arg_types.len) continue;
                var total_cost: u64 = 0;
                var castable = true;
                for (entry.arg_types, arg_types) |declared, given| {
                    const c = argCastCost(given, declared, allow_narrowing) orelse {
                        castable = false;
                        break;
                    };
                    total_cost += c;
                }
                if (!castable) continue;
                if (total_cost < best_cost) {
                    best_cost = total_cost;
                    best = scalarFromUdf(entry);
                }
            }
        }
        if (best != null) break;
    }

    const chosen_proto = best orelse return null;
    const chosen = try expandScalarFn(aa, chosen_proto, arg_types.len);
    // Build per-arg cast plan.
    const arg_casts = try aa.alloc(?CastKernel, arg_types.len);
    for (arg_types, arg_casts, 0..) |given, *slot, i| {
        const declared = chosen.arg_types[i];
        const ft: TypeTag = @as(TypeTag, given);
        const tt: TypeTag = @as(TypeTag, declared);
        slot.* = if (ft == tt) null else cast.kernelFor(ft, tt) orelse cast.argNarrowingKernelFor(ft, tt);
    }
    return ResolvedOverload{ .func = chosen, .arg_casts = arg_casts };
}

// ---------------------------------------------------------------------------
// Decimal resolution
//
// Decimal operations need the operand scales, which the static `builtins` table
// can't carry (its `return_type` is fixed, and a plain `Kernel` never sees the
// arg `Type`s). `resolveDecimal` builds a synthetic overload on demand: it
// computes the DESIGN.md §3.4 result type from the call's arg types and points
// at a `typed_kernel` in `scalar_fn_decimal.zig`. No casts are attached
// (`arg_casts = null`) — the typed kernel reads each operand's declared scale
// and aligns internally, so integer operands flow through unmangled.
// ---------------------------------------------------------------------------

fn anyDecimal(arg_types: []const Type) bool {
    for (arg_types) |t| if (t.isDecimal()) return true;
    return false;
}

/// An operand a decimal op can absorb without a string cast.
fn numericLike(t: Type) bool {
    return t.isInteger() or t.isFloat() or t.isDecimal() or t == .boolean;
}

fn allNumericLike(arg_types: []const Type) bool {
    for (arg_types) |t| if (!numericLike(t)) return false;
    return true;
}

/// Decimal/int-only (no float) — the operands COALESCE/IF/GREATEST/LEAST can
/// fold into a decimal result.
fn allDecimalOrInt(arg_types: []const Type) bool {
    for (arg_types) |t| if (!(t.isInteger() or t.isDecimal() or t == .boolean)) return false;
    return true;
}

fn arithOp(name: []const u8) ?dec.Op {
    if (std.ascii.eqlIgnoreCase(name, "add")) return .add;
    if (std.ascii.eqlIgnoreCase(name, "sub")) return .sub;
    if (std.ascii.eqlIgnoreCase(name, "mul")) return .mul;
    if (std.ascii.eqlIgnoreCase(name, "div")) return .div;
    if (std.ascii.eqlIgnoreCase(name, "mod")) return .mod;
    return null;
}

fn arithKernelFor(op: dec.Op) TypedKernel {
    return switch (op) {
        .add => dec.addKernel,
        .sub => dec.subKernel,
        .mul => dec.mulKernel,
        .div => dec.divKernel,
        .mod => dec.modKernel,
    };
}

fn buildDecFn(
    aa: Allocator,
    name: []const u8,
    arg_types: []const Type,
    return_type: Type,
    kernel: TypedKernel,
    null_strategy: NullStrategy,
) !ResolvedOverload {
    return ResolvedOverload{
        .func = .{
            .name = name,
            .arg_types = try aa.dupe(Type, arg_types),
            .return_type = return_type,
            .typed_kernel = kernel,
            .null_strategy = null_strategy,
        },
        .arg_casts = null,
    };
}

fn resolveDecimal(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    // CAST(x AS DECIMAL(p,s)) lowers to a name-encoded `to_decimal:<p>:<s>`
    // (the target p/s ride in the name since the resolver sees types, not the
    // literal values the generic call form would pass).
    if (std.mem.startsWith(u8, name, "to_decimal")) return resolveToDecimal(aa, name, arg_types);

    if (!anyDecimal(arg_types)) return null;

    // Binary arithmetic.
    if (arg_types.len == 2) {
        if (arithOp(name)) |op| {
            if (!allNumericLike(arg_types)) return null;
            const rt = dec.arithResultType(op, arg_types[0], arg_types[1]);
            const nulls: NullStrategy = switch (op) {
                .add, .sub, .mul => .propagates,
                .div, .mod => .zero_divisor,
            };
            return try buildDecFn(aa, name, arg_types, rt, arithKernelFor(op), nulls);
        }
    }

    // Unary decimal functions (source must be decimal).
    if (arg_types.len == 1 and arg_types[0].isDecimal()) {
        const sp = arg_types[0].decimalSpec().?;
        if (std.ascii.eqlIgnoreCase(name, "to_double") or std.ascii.eqlIgnoreCase(name, "to_float"))
            return try buildDecFn(aa, name, arg_types, .double, dec.toDoubleKernel, .propagates);
        if (intCastTarget(name)) |it|
            return try buildDecFn(aa, name, arg_types, it, dec.toIntKernel, .kernel_managed);
        if (std.ascii.eqlIgnoreCase(name, "to_string"))
            return try buildDecFn(aa, name, arg_types, .string, dec.toStringKernel, .propagates);
        if (std.ascii.eqlIgnoreCase(name, "abs"))
            return try buildDecFn(aa, name, arg_types, arg_types[0], dec.absKernel, .propagates);
        if (std.ascii.eqlIgnoreCase(name, "round"))
            return try buildDecFn(aa, name, arg_types, dec.decTypeFor(sp.p, 0), dec.roundKernel, .propagates);
        if (std.ascii.eqlIgnoreCase(name, "floor"))
            return try buildDecFn(aa, name, arg_types, dec.decTypeFor(sp.p, 0), dec.floorKernel, .propagates);
        if (std.ascii.eqlIgnoreCase(name, "ceil"))
            return try buildDecFn(aa, name, arg_types, dec.decTypeFor(sp.p, 0), dec.ceilKernel, .propagates);
        if (std.ascii.eqlIgnoreCase(name, "truncate"))
            return try buildDecFn(aa, name, arg_types, dec.decTypeFor(sp.p, 0), dec.truncateKernel, .propagates);
        if (std.ascii.eqlIgnoreCase(name, "hex"))
            return try buildDecFn(aa, name, arg_types, .string, dec.hexKernel, .propagates);
        if (std.ascii.eqlIgnoreCase(name, INTEGER_ARG_FN))
            return try buildDecFn(aa, name, arg_types, .bigint, dec.integerArgKernel, .propagates);
        return null;
    }

    // ROUND/TRUNCATE(decimal, n) — keeps the source scale, rounds the value.
    // A literal `n` narrows the scale (`roundedDecimalType`).
    if (arg_types.len == 2 and arg_types[0].isDecimal() and arg_types[1].isInteger()) {
        if (std.ascii.eqlIgnoreCase(name, "round"))
            return try buildDecFn(aa, name, arg_types, arg_types[0], dec.roundNKernel, .propagates);
        if (std.ascii.eqlIgnoreCase(name, "truncate"))
            return try buildDecFn(aa, name, arg_types, arg_types[0], dec.truncateNKernel, .propagates);
    }

    // COALESCE / IFNULL — first non-null, all operands decimal/int.
    if ((std.ascii.eqlIgnoreCase(name, "coalesce") or std.ascii.eqlIgnoreCase(name, "ifnull")) and allDecimalOrInt(arg_types)) {
        const rt = decimalResultOf(arg_types) orelse return null;
        return try buildDecFn(aa, name, arg_types, rt, dec.coalesceKernel, .absorbs);
    }

    if (std.ascii.eqlIgnoreCase(name, "nullif") and arg_types.len == 2 and allDecimalOrInt(arg_types)) {
        const rt = decimalResultOf(arg_types) orelse return null;
        return try buildDecFn(aa, name, arg_types, rt, dec.nullifKernel, .kernel_managed);
    }

    if (std.ascii.eqlIgnoreCase(name, "if") and arg_types.len == 3 and arg_types[0] == .boolean and allDecimalOrInt(arg_types[1..])) {
        const rt = decimalResultOf(arg_types[1..]) orelse return null;
        return try buildDecFn(aa, name, arg_types, rt, dec.ifKernel, .kernel_managed);
    }

    if ((std.ascii.eqlIgnoreCase(name, "greatest") or std.ascii.eqlIgnoreCase(name, "least")) and allDecimalOrInt(arg_types)) {
        const rt = decimalResultOf(arg_types) orelse return null;
        const k = if (std.ascii.eqlIgnoreCase(name, "greatest")) dec.greatestKernel else dec.leastKernel;
        return try buildDecFn(aa, name, arg_types, rt, k, .propagates);
    }

    return null;
}

/// The decimal a function returning one of `args` returns: the type they
/// meet at by the result-type rule (`cast.commonType`). Null when that is
/// no decimal (a LARGEINT beside one), so the call converts its arguments to
/// that type instead.
fn decimalResultOf(args: []const Type) ?Type {
    const t = cast.commonTypeOf(args) orelse return null;
    return if (t.isDecimal()) t else null;
}

// ---------------------------------------------------------------------------
// Integer arithmetic resolution
//
// The one rule for the width of `+ - * DIV %` over integer operands
// (DESIGN.md §3.4), matching StarRocks 4.0: both operands widen to their common
// type; `+ - *` then widen one more level (TINYINT→SMALLINT→INT→BIGINT; BIGINT
// and LARGEINT stay) and wrap on overflow, while DIV and % keep the common
// type and return NULL for a zero divisor. BOOLEAN counts as TINYINT. Integer
// literal operands are typed by `arithOperandLiteral`.
// ---------------------------------------------------------------------------

pub const IntArithOp = enum { add, sub, mul, intdiv, mod };

pub fn intArithOp(name: []const u8) ?IntArithOp {
    inline for (@typeInfo(IntArithOp).@"enum".fields) |f| {
        if (std.ascii.eqlIgnoreCase(name, f.name)) return @field(IntArithOp, f.name);
    }
    return null;
}

fn intWidthRank(t: Type) ?usize {
    return switch (t) {
        .boolean, .tinyint => 0,
        .smallint => 1,
        .int => 2,
        .bigint => 3,
        .largeint => 4,
        else => null,
    };
}

const INT_BY_WIDTH_RANK = [_]Type{ .tinyint, .smallint, .int, .bigint, .largeint };

/// Result type of integer `a <op> b`; null unless both operands are integers
/// (or BOOLEAN). Both operands are cast to this type before the kernel runs.
pub fn intArithResultType(op: IntArithOp, a: Type, b: Type) ?Type {
    const common_rank = @max(intWidthRank(a) orelse return null, intWidthRank(b) orelse return null);
    return INT_BY_WIDTH_RANK[
        switch (op) {
            .add, .sub, .mul => if (common_rank < 3) common_rank + 1 else common_rank,
            .intdiv, .mod => common_rank,
        }
    ];
}

/// A literal operand of `name(arg_types)` (typed as the parser typed it) as
/// the arithmetic sees it. In integer arithmetic an integer literal takes the
/// narrowest of TINYINT/SMALLINT/INT/BIGINT holding it, as StarRocks types
/// literals, so `smallint_col + 1` is SMALLINT + TINYINT → INT rather than
/// INT + INT → BIGINT. A LARGEINT literal, which no integer's digits spell
/// (past BIGINT they are a DECIMAL), is a hex literal's BIGINT UNSIGNED
/// (`expr.hexLiteralNumber`) and stays one, so `0x7FFFFFFFFFFFFFFF + 1` is
/// 2^63, as in MySQL. Every other call keeps the parser's literal.
pub fn arithOperandLiteral(name: []const u8, arg_types: []const Type, lit: types.Value) types.Value {
    const op = intArithOp(name) orelse return lit;
    if (arg_types.len != 2 or intArithResultType(op, arg_types[0], arg_types[1]) == null) return lit;
    const x: i128 = switch (lit) {
        .tinyint => |x| x,
        .smallint => |x| x,
        .int => |x| x,
        .bigint => |x| x,
        else => return lit,
    };
    if (std.math.cast(i8, x)) |n| return .{ .tinyint = n };
    if (std.math.cast(i16, x)) |n| return .{ .smallint = n };
    if (std.math.cast(i32, x)) |n| return .{ .int = n };
    return .{ .bigint = @intCast(x) };
}

/// Whether `name(arg_types)` is arithmetic over a float operand, which is
/// DOUBLE (`dec.arithResultType`): a decimal literal operand then converts
/// once, at plan time, instead of per row.
pub fn arithTakesDouble(name: []const u8, arg_types: []const Type) bool {
    if (arg_types.len != 2 or !(arg_types[0].isFloat() or arg_types[1].isFloat())) return false;
    inline for (@typeInfo(dec.Op).@"enum".fields) |f| {
        if (std.ascii.eqlIgnoreCase(name, f.name)) return true;
    }
    return false;
}

/// ROUND/TRUNCATE(decimal, n) with a literal `n` below the source scale
/// return DECIMAL(p, max(n, 0)), as MySQL and DuckDB do: `ROUND(1.005, 2)`
/// is 1.01, not 1.010. A per-row `n` keeps the source type.
pub fn roundedDecimalType(name: []const u8, arg_types: []const Type, places: i128) ?Type {
    if (arg_types.len != 2 or !arg_types[1].isInteger()) return null;
    if (!std.ascii.eqlIgnoreCase(name, "round") and !std.ascii.eqlIgnoreCase(name, "truncate")) return null;
    const sp = arg_types[0].decimalSpec() orelse return null;
    if (places >= sp.s) return null;
    return dec.decTypeFor(sp.p, @intCast(@max(places, 0)));
}

/// `width` is always one of INT_BY_WIDTH_RANK.
fn intArithKernel(op: IntArithOp, width: Type) Kernel {
    return switch (width) {
        inline .tinyint, .smallint, .int, .bigint, .largeint => |_, tag| blk: {
            const T = switch (tag) {
                .tinyint => i8,
                .smallint => i16,
                .int => i32,
                .bigint => i64,
                .largeint => i128,
                else => unreachable,
            };
            break :blk switch (op) {
                .add => math.wrappingArithKernel(T, .add),
                .sub => math.wrappingArithKernel(T, .sub),
                .mul => math.wrappingArithKernel(T, .mul),
                .intdiv => math.intDivModKernel(T, .div),
                .mod => math.intDivModKernel(T, .mod),
            };
        },
        else => unreachable,
    };
}

fn resolveIntArith(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (arg_types.len != 2) return null;
    const op = intArithOp(name) orelse return null;
    const width = intArithResultType(op, arg_types[0], arg_types[1]) orelse return null;
    const declared = try aa.alloc(Type, 2);
    @memset(declared, width);
    const casts = try aa.alloc(?CastKernel, 2);
    var any_cast = false;
    for (arg_types, casts) |given, *slot| {
        const from: TypeTag = given;
        slot.* = if (from == @as(TypeTag, width)) null else cast.kernelFor(from, width) orelse return null;
        any_cast = any_cast or slot.* != null;
    }
    return ResolvedOverload{
        .func = .{
            .name = name,
            .arg_types = declared,
            .return_type = width,
            .null_strategy = switch (op) {
                .add, .sub, .mul => .propagates,
                .intdiv, .mod => .zero_divisor,
            },
            .kernel = intArithKernel(op, width),
        },
        .arg_casts = if (any_cast) casts else null,
    };
}

/// `a DIV b` with a float or decimal operand: the exact quotient truncated
/// into a BIGINT, as in MySQL (`dec.intDivKernel`). Integer operands resolve
/// first, in `resolveIntArith`.
fn resolveFractionalIntDiv(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (arg_types.len != 2 or !std.ascii.eqlIgnoreCase(name, "intdiv") or !allNumericLike(arg_types)) return null;
    return try buildDecFn(aa, name, arg_types, .bigint, dec.intDivKernel, .zero_divisor);
}

/// MySQL's `FORMAT(x, d)` over any number and an integer `d`
/// (`dec.formatKernel`). The locale form isn't supported.
fn resolveFormat(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (arg_types.len != 2 or !std.ascii.eqlIgnoreCase(name, "format")) return null;
    if (!numericLike(arg_types[0]) or !arg_types[1].isInteger()) return null;
    return try buildDecFn(aa, name, arg_types, .string, dec.formatKernel, .propagates);
}

/// SEC_TO_TIME(n) and MAKETIME(h, m, s) read any number, or text as a
/// number, keeping a decimal's or a double's fraction, so they take each
/// argument's own type rather than a cast.
fn resolveTimeFromNumbers(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    const kernel: TypedKernel = if (arg_types.len == 1 and std.ascii.eqlIgnoreCase(name, "sec_to_time"))
        time.secToTimeKernel
    else if (arg_types.len == 3 and std.ascii.eqlIgnoreCase(name, "maketime"))
        time.makeTimeKernel
    else
        return null;
    for (arg_types) |t| if (!numericLike(t) and !t.isString()) return null;
    return try buildDecFn(aa, name, arg_types, .string, kernel, .kernel_managed);
}

/// HOUR, TIME_TO_SEC, TIMEDIFF, ADDTIME and the other TIME functions when a
/// number is among the arguments: it reads by its digits (`time.numberArgFn`),
/// not as text, so `HOUR(8390000)` is NULL where `HOUR('8390000')` clamps.
fn resolveTimeOfNumber(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    var numbers: usize = 0;
    for (arg_types) |t| {
        if (numericLike(t)) numbers += 1 else if (!(t.isString() or t.isTemporal())) return null;
    }
    if (numbers == 0) return null;
    const f = time.numberArgFn(name, arg_types) orelse return null;
    return try buildDecFn(aa, name, arg_types, f.return_type, f.kernel, .kernel_managed);
}

const CAST_TIME_PREFIX = "to_time:";

/// The function CAST(x AS TIME(fsp)) lowers to. thinDB has no TIME type, so
/// the precision rides in the name, as a DECIMAL target's does.
pub fn castTimeFnName(arena: Allocator, fsp: u8) Allocator.Error![]const u8 {
    return std.fmt.allocPrint(arena, CAST_TIME_PREFIX ++ "{d}", .{fsp});
}

const CAST_TIME_KERNELS = blk: {
    var kernels: [time.MAX_FSP + 1]TypedKernel = undefined;
    for (&kernels, 0..) |*k, fsp| k.* = time.castTimeKernel(fsp);
    break :blk kernels;
};

/// `to_time:<fsp>` over text, a date, a datetime or any number
/// (`time.castTimeKernel`): each reads by its own type, so no cast applies.
fn resolveCastTime(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (arg_types.len != 1 or !std.mem.startsWith(u8, name, CAST_TIME_PREFIX)) return null;
    const fsp = std.fmt.parseInt(u8, name[CAST_TIME_PREFIX.len..], 10) catch return null;
    if (fsp > time.MAX_FSP) return null;
    const t = arg_types[0];
    if (!(numericLike(t) or t.isString() or t.isTemporal())) return null;
    return try buildDecFn(aa, name, arg_types, .string, CAST_TIME_KERNELS[fsp], .kernel_managed);
}

/// Internal: JSON_ARRAYAGG / JSON_OBJECTAGG lower to a GROUP_CONCAT of these
/// per-row packings, which the matching wrapper builds the document from
/// (`scalar_fn_json.zig`).
pub const JSON_AGG_ELEMENT_FN = "__json_agg_element";
pub const JSON_AGG_MEMBER_FN = "__json_agg_member";
pub const JSON_AGG_ARRAY_FN = "__json_agg_array";
pub const JSON_AGG_OBJECT_FN = "__json_agg_object";

/// JSON_ARRAY, JSON_OBJECT and the JSON aggregates' packers take arguments of
/// any type, each becoming the JSON value MySQL makes of it; the JSON value
/// of a DECIMAL needs its scale, so these are typed kernels. CAST(json AS
/// CHAR) prints the document rather than copying its JSONB bytes.
fn resolveJson(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (std.ascii.eqlIgnoreCase(name, "json_array"))
        return try buildDecFn(aa, name, arg_types, .json, json.jsonArrayKernel, .kernel_managed);
    if (std.ascii.eqlIgnoreCase(name, "json_object")) {
        if (arg_types.len % 2 != 0) return null;
        return try buildDecFn(aa, name, arg_types, .json, json.jsonObjectKernel, .kernel_managed);
    }
    if (std.mem.eql(u8, name, JSON_AGG_ELEMENT_FN) and arg_types.len == 1)
        return try buildDecFn(aa, name, arg_types, .string, json.jsonAggElementKernel, .kernel_managed);
    if (std.mem.eql(u8, name, JSON_AGG_MEMBER_FN) and arg_types.len == 2)
        return try buildDecFn(aa, name, arg_types, .string, json.jsonAggMemberKernel, .kernel_managed);
    if (std.ascii.eqlIgnoreCase(name, "to_string") and arg_types.len == 1 and arg_types[0] == .json) {
        return .{
            .func = .{ .name = name, .arg_types = try aa.dupe(Type, arg_types), .return_type = .string, .kernel = json.jsonToTextKernel },
            .arg_casts = null,
        };
    }
    return null;
}

/// Whether ANY overload named `name` exists — builtin, decimal-only, or a
/// registered UDF. Name-only, so it holds before argument types are known.
pub fn nameResolvable(registry: ?*const udf_mod.UdfRegistry, name: []const u8) bool {
    if (std.mem.startsWith(u8, name, "to_decimal")) return true;
    if (std.mem.startsWith(u8, name, CAST_TIME_PREFIX)) return true;
    if (std.ascii.eqlIgnoreCase(name, "json_array") or std.ascii.eqlIgnoreCase(name, "json_object")) return true;
    if (std.mem.eql(u8, name, JSON_AGG_ELEMENT_FN) or std.mem.eql(u8, name, JSON_AGG_MEMBER_FN)) return true;
    if (std.mem.eql(u8, name, SINGLE_ROW_FN)) return true;
    inline for (.{ "charset", "collation", "benchmark" }) |n| if (std.ascii.eqlIgnoreCase(name, n)) return true;
    if (std.mem.eql(u8, name, ROW_KEY_FN)) return true;
    if (std.mem.startsWith(u8, name, TEXT_KEY_PREFIX)) return true;
    if (std.mem.eql(u8, name, ORDER_KEY_FN) or std.mem.eql(u8, name, ORDER_KEY_DESC_FN)) return true;
    if (std.mem.eql(u8, name, expr_mod.HEX_LITERAL_AS_FN) or std.mem.eql(u8, name, PG_TEXT_FN)) return true;
    if (std.ascii.eqlIgnoreCase(name, "to_float")) return true;
    if (intArithOp(name) != null) return true;
    if (std.ascii.eqlIgnoreCase(name, "format")) return true;
    if (std.ascii.eqlIgnoreCase(name, "sec_to_time") or std.ascii.eqlIgnoreCase(name, "maketime")) return true;
    for (builtins) |f| if (std.ascii.eqlIgnoreCase(f.name, name)) return true;
    if (registry) |reg| {
        for (reg.scalarEntries()) |entry| if (std.ascii.eqlIgnoreCase(entry.name, name)) return true;
    }
    return false;
}

/// Whether a scalar UDF may not take `name`: the builtin resolver answers
/// to it, as itself or as the alias the parser rewrites (`ucase`), so a UDF
/// under it would take calls meant for a builtin, or for the function some
/// syntax lowers to, or never be called; or it starts `__`, as the engine's
/// own functions do.
pub fn isReservedScalarUdfName(name: []const u8) bool {
    return std.mem.startsWith(u8, name, "__") or nameResolvable(null, canonicalName(name));
}

/// CHARSET(x) and COLLATION(x) depend on x's type alone, so they take any
/// argument, NULL too.
fn resolveCharset(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (arg_types.len != 1) return null;
    const kernel: TypedKernel = if (std.ascii.eqlIgnoreCase(name, "charset"))
        string.charsetKernel
    else if (std.ascii.eqlIgnoreCase(name, "collation"))
        string.collationKernel
    else
        return null;
    return try buildDecFn(aa, name, arg_types, .string, kernel, .kernel_managed);
}

/// BENCHMARK(count, expr) reads `count` as MySQL reads an integer argument
/// and never looks at `expr`, so it takes any two arguments.
fn resolveBenchmark(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (!std.ascii.eqlIgnoreCase(name, "benchmark") or arg_types.len != 2) return null;
    return try buildDecFn(aa, name, arg_types, .bigint, math.benchmarkKernel, .kernel_managed);
}

/// Internal: a correlated scalar subquery's value for one outer row,
/// `__single_row(matched_rows, value)`, where `matched_rows` counts the inner
/// rows the outer row's correlation key matched.
pub const SINGLE_ROW_FN = "__single_row";

fn resolveSingleRow(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (!std.mem.eql(u8, name, SINGLE_ROW_FN)) return null;
    if (arg_types.len != 2 or arg_types[0] != .bigint) return null;
    return try buildDecFn(aa, name, arg_types, arg_types[1], cond.singleRowKernel, .kernel_managed);
}

/// Internal: one key per row for a tuple of any types, NULL when any element
/// is (`string.rowKeyKernel`). COUNT(DISTINCT a, b) counts these.
pub const ROW_KEY_FN = "__row_key";

fn resolveRowKey(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (!std.mem.eql(u8, name, ROW_KEY_FN) or arg_types.len == 0) return null;
    return try buildDecFn(aa, name, arg_types, .string, string.rowKeyKernel, .kernel_managed);
}

fn intCastTarget(name: []const u8) ?Type {
    if (std.ascii.eqlIgnoreCase(name, "to_int")) return .int;
    if (std.ascii.eqlIgnoreCase(name, "to_bigint")) return .bigint;
    if (std.ascii.eqlIgnoreCase(name, "to_smallint")) return .smallint;
    if (std.ascii.eqlIgnoreCase(name, "to_tinyint")) return .tinyint;
    if (std.ascii.eqlIgnoreCase(name, "to_largeint")) return .largeint;
    return null;
}

/// Resolve a name-encoded `to_decimal:<p>:<s>` cast. Source may be any numeric
/// or string type.
/// The conversion function `CAST(x AS ty)` lowers to (the implicit-cast
/// ranking then coerces the source width, e.g. smallint→bigint before
/// to_int), or null when no kernel converts into `ty`. A DECIMAL target
/// carries its (p,s) in the name — the resolver sees types, not literal
/// values — so every name is allocated in `arena`.
pub fn castFnName(arena: Allocator, ty: Type) Allocator.Error!?[]const u8 {
    if (ty.decimalSpec()) |spec| return try std.fmt.allocPrint(arena, "to_decimal:{d}:{d}", .{ spec.p, spec.s });
    const name: []const u8 = switch (ty) {
        .int => "to_int",
        .bigint => "to_bigint",
        .smallint => "to_smallint",
        .tinyint => "to_tinyint",
        .largeint => "to_largeint",
        .float, .double => "to_double",
        .boolean => "to_boolean",
        .date => "to_date",
        .datetime => "to_datetime",
        .varchar, .char, .string => "to_string",
        .json => "to_json",
        .decimal64, .decimal128, .uuid => return null,
    };
    return try arena.dupe(u8, name);
}

const TEXT_KEY_PREFIX = "text_key:";

/// The function that reads a text join key as `ty`, the other key's type:
/// each row becomes the `ty` value it equals under the comparison rule, or
/// NULL (`dec.textKeyKernel`). The target rides in the name, as CAST's
/// DECIMAL target does; null when no such reading exists.
pub fn textKeyFnName(arena: Allocator, ty: Type) Allocator.Error!?[]const u8 {
    if (ty.decimalSpec()) |spec| return try std.fmt.allocPrint(arena, TEXT_KEY_PREFIX ++ "decimal:{d}:{d}", .{ spec.p, spec.s });
    return switch (ty) {
        .tinyint, .smallint, .int, .bigint, .largeint, .boolean, .date, .datetime => try std.fmt.allocPrint(arena, TEXT_KEY_PREFIX ++ "{s}", .{@tagName(ty)}),
        else => null,
    };
}

/// True for a function a join lays over one key column to convert it:
/// `castFnName`'s and `textKeyFnName`'s.
pub fn isKeyConversionFn(name: []const u8) bool {
    return std.ascii.startsWithIgnoreCase(name, "to_") or std.mem.startsWith(u8, name, TEXT_KEY_PREFIX);
}

fn resolveTextKey(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (!std.mem.startsWith(u8, name, TEXT_KEY_PREFIX)) return null;
    if (arg_types.len != 1 or !arg_types[0].isString()) return null;
    const target = textKeyTarget(name[TEXT_KEY_PREFIX.len..]) orelse return null;
    return try buildDecFn(aa, name, arg_types, target, dec.textKeyKernel, .kernel_managed);
}

/// Internal: a byte string per row that, compared as bytes, sorts like its
/// arguments ascending with NULLs first (`string.orderKeyKernel`). The DESC
/// twin sorts them descending with NULLs last. GROUP_CONCAT's ORDER BY packs
/// its keys into one of these.
pub const ORDER_KEY_FN = "__order_key";
pub const ORDER_KEY_DESC_FN = "__order_key_desc";

fn resolveOrderKey(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (arg_types.len == 0) return null;
    const kernel: TypedKernel = if (std.mem.eql(u8, name, ORDER_KEY_FN))
        string.orderKeyKernel
    else if (std.mem.eql(u8, name, ORDER_KEY_DESC_FN))
        string.orderKeyDescKernel
    else
        return null;
    return try buildDecFn(aa, name, arg_types, .string, kernel, .kernel_managed);
}

/// `expr.HEX_LITERAL_AS_FN`: the literal's integer when the first argument
/// is a number, else its bytes.
/// PostgreSQL's cast to text. It spells a boolean `true` or `false`, where
/// every other dialect writes it as `1` or `0` (`to_string`); any other type
/// casts as `to_string` does.
pub const PG_TEXT_FN = "__pg_text";

/// Internal: UUID_SHORT(), given the statement's clock seconds by the
/// pre-compile pass, as UUID() is given its seed.
pub const UUID_SHORT_FN = "__uuid_short";

/// Internal: `@@name`, which the pre-compile pass replaces with the value
/// thinDB reports for the system variable.
pub const SYSTEM_VARIABLE_FN = "__system_variable";

fn resolvePgBoolText(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (!std.mem.eql(u8, name, PG_TEXT_FN) or arg_types.len != 1 or arg_types[0] != .boolean) return null;
    return .{
        .func = .{ .name = name, .arg_types = try aa.dupe(Type, arg_types), .return_type = .string, .kernel = math.boolToWordKernel },
        .arg_casts = null,
    };
}

fn resolveHexLiteralAs(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (!std.mem.eql(u8, name, expr_mod.HEX_LITERAL_AS_FN)) return null;
    if (arg_types.len != 2 or !arg_types[1].isString()) return null;
    const out: Type = if (numericLike(arg_types[0])) .largeint else .string;
    return try buildDecFn(aa, name, arg_types, out, hexLiteralAsKernel, .propagates);
}

fn hexLiteralAsKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
    _ = arg_types;
    const bytes = common.stringViewOf(args[1]);
    if (out_type == .largeint) {
        for (0..row_count) |row| try out.data.largeint.append(allocator, expr_mod.hexNumber(bytes.rowBytes(row)));
        return;
    }
    return string.stringIdentityKernel(allocator, args[1..2], out, row_count);
}

/// Whether argument `i` of an `arity`-argument `name` call reads a number,
/// so a hex literal there is its integer (`0x41 + 0`, `ABS(0x41)`,
/// `CAST(0x41 AS SIGNED)`) rather than its bytes (`CONCAT(0x41, 1)`,
/// `LENGTH(0x41)`), as in MySQL. A numeric cast reads a number; an argument
/// the call returns (COALESCE, IF's branches) keeps its bytes, as does one
/// any overload takes as text, except BIN's and CONV's number.
pub fn readsNumberAt(registry: ?*const udf_mod.UdfRegistry, name: []const u8, arity: usize, i: usize) bool {
    if (intCastTarget(name) != null or std.mem.startsWith(u8, name, "to_decimal")) return true;
    if (std.ascii.eqlIgnoreCase(name, "to_double") or std.ascii.eqlIgnoreCase(name, "to_float")) return true;
    if (resultValueArgsStart(name)) |start| if (i >= start) return false;
    if (i == 0 and readsNumberAsText(name)) return true;
    for (builtins) |f| {
        if (!std.ascii.eqlIgnoreCase(f.name, name) or !scalarArityMatches(f, arity)) continue;
        if (scalarDeclaredTypeAt(f, i).isString()) return false;
    }
    if (registry) |reg| for (reg.scalarEntries()) |entry| {
        if (!std.ascii.eqlIgnoreCase(entry.name, name) or entry.arg_types.len != arity) continue;
        if (entry.arg_types[i].isString()) return false;
    };
    return true;
}

/// BIN and CONV take their number as its text, yet a hex literal there is
/// the number it spells, as MySQL reads it (`BIN(0x41)` is 1000001).
pub fn readsNumberAsText(name: []const u8) bool {
    return std.ascii.eqlIgnoreCase(name, "bin") or std.ascii.eqlIgnoreCase(name, "conv");
}

/// Internal: CONV over a hex literal's integer (`math.convBitsKernel`).
pub const CONV_BITS_FN = "__conv_bits";

/// The function a call takes once its hex literal first argument reads as
/// its integer, when that is not the call's own: CONV converts the integer
/// itself, whatever `from_base` says, where its decimal text would read in
/// `from_base` (`CONV(X'FF', 16, 10)` is 255, not 597).
pub fn hexNumberFn(name: []const u8, arity: usize) []const u8 {
    return if (arity == 3 and std.ascii.eqlIgnoreCase(name, "conv")) CONV_BITS_FN else name;
}

fn textKeyTarget(spec: []const u8) ?Type {
    var it = std.mem.splitScalar(u8, spec, ':');
    const head = it.next() orelse return null;
    if (std.mem.eql(u8, head, "decimal")) {
        const p = std.fmt.parseInt(u8, it.next() orelse return null, 10) catch return null;
        const s = std.fmt.parseInt(u8, it.next() orelse return null, 10) catch return null;
        return dec.decTypeFor(p, s);
    }
    const tag = std.meta.stringToEnum(TypeTag, head) orelse return null;
    return switch (tag) {
        inline .tinyint, .smallint, .int, .bigint, .largeint, .boolean, .date, .datetime => |t| t,
        else => null,
    };
}

fn resolveToDecimal(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (arg_types.len != 1) return null;
    var it = std.mem.splitScalar(u8, name, ':');
    const head = it.next() orelse return null;
    if (!std.mem.eql(u8, head, "to_decimal")) return null;
    const p = std.fmt.parseInt(u8, it.next() orelse return null, 10) catch return null;
    const s = std.fmt.parseInt(u8, it.next() orelse return null, 10) catch return null;
    const src = arg_types[0];
    if (!(numericLike(src) or src.isString() or src.isTemporal())) return null;
    return try buildDecFn(aa, name, arg_types, dec.decTypeFor(p, s), dec.toDecimalKernel, .kernel_managed);
}

pub fn scalarArityMatches(f: ScalarFn, actual: usize) bool {
    if (f.variadic_min_args) |min_args| return actual >= min_args and f.arg_types.len > f.variadic_fixed;
    return f.arg_types.len == actual;
}

pub fn scalarDeclaredTypeAt(f: ScalarFn, i: usize) Type {
    if (f.variadic_min_args == null or i < f.variadic_fixed) return f.arg_types[i];
    const repeated = f.arg_types[f.variadic_fixed..];
    return repeated[(i - f.variadic_fixed) % repeated.len];
}

/// Whether an argument of type `given` takes a parameter declared `declared`
/// with no conversion. The text types share one representation (see
/// `stringViewOf`), so a `VARCHAR(n)` column takes a string parameter as it
/// is. A JSON parameter takes text too, as a document written out, but a
/// JSON value takes a text parameter only as its text (`argConversion`):
/// its stored bytes are JSONB, not the text MySQL's string functions read.
fn bindsAsIs(given: Type, declared: Type) bool {
    if (declared == .json) return given.isString();
    if (declared.isString()) return given.isString() and given != .json;
    return @as(TypeTag, given) == @as(TypeTag, declared);
}

/// The first argument of a function whose result is one of its arguments
/// (GREATEST, LEAST, COALESCE, IFNULL, NULLIF; IF after its condition):
/// those arguments take one type by `cast.commonType` when no overload
/// matches them as given. Null for any other function.
pub fn resultValueArgsStart(name: []const u8) ?usize {
    inline for (.{ "greatest", "least", "coalesce", "ifnull", "nullif" }) |n| {
        if (std.ascii.eqlIgnoreCase(name, n)) return 0;
    }
    if (std.ascii.eqlIgnoreCase(name, "if")) return 1;
    return null;
}

/// The type each argument converts to so `name` resolves, where the
/// implicit casts can't take it: a string parameter takes a number, a
/// decimal or a date as its text, as in StarRocks (`CONCAT('Q', quarter)`),
/// and a JSON value as its text, as in MySQL (`LOWER(doc)`); a float
/// parameter takes a decimal's value, as in MySQL and StarRocks
/// (`POWER(1.09, n)`, `SQRT(price)`); and a numeric parameter takes text or
/// JSON as the number it starts with, as in MySQL (`REPEAT('a', '3')`,
/// `'3abc' + 0`). Builtins and registered UDFs alike; the cheapest overload
/// wins. A function no overload of which takes these arguments that way
/// (`'7' DIV '2'`, whose integer and decimal forms resolve by type) reads
/// its text arguments as doubles when that resolves it, and then its date or
/// datetime arguments as numbers (`temporalNumberArgs`). Null when nothing
/// resolves or no argument needs converting.
pub fn convertedArgs(aa: Allocator, registry: ?*const udf_mod.UdfRegistry, name: []const u8, arg_types: []const Type) !?[]const ?Type {
    const f = try cheapestConverted(aa, registry, name, arg_types) orelse
        return try textAsDoubleArgs(aa, registry, name, arg_types) orelse try temporalNumberArgs(aa, registry, name, arg_types);
    const targets = try aa.alloc(?Type, arg_types.len);
    var any = false;
    for (arg_types, targets, 0..) |given, *t, i| {
        t.* = (try argConversion(aa, given, scalarDeclaredTypeAt(f, i))).?.target;
        any = any or t.* != null;
    }
    return if (any) targets else null;
}

fn cheapestConverted(aa: Allocator, registry: ?*const udf_mod.UdfRegistry, name: []const u8, arg_types: []const Type) !?ScalarFn {
    var best: ?ScalarFn = null;
    var best_cost: u64 = std.math.maxInt(u64);
    for (builtins) |f| try considerConverted(aa, f, name, arg_types, &best, &best_cost);
    if (registry) |reg| {
        for (reg.scalarEntries()) |entry| try considerConverted(aa, scalarFromUdf(entry), name, arg_types, &best, &best_cost);
    }
    return best;
}

/// A date or datetime argument read as its YYYYMMDD[HHMMSS] number
/// (`to_bigint`), as MySQL reads one in a numeric context (`CURDATE() + 0`,
/// `ABS(d)`, `d DIV 100`), when the call then resolves. Tried only after
/// every other conversion, so a call that takes a date's text keeps it.
fn temporalNumberArgs(aa: Allocator, registry: ?*const udf_mod.UdfRegistry, name: []const u8, arg_types: []const Type) !?[]const ?Type {
    const numbers = try aa.alloc(Type, arg_types.len);
    const targets = try aa.alloc(?Type, arg_types.len);
    var any = false;
    for (arg_types, numbers, targets) |given, *number, *target| {
        target.* = if (given.isTemporal()) .bigint else null;
        number.* = target.* orelse given;
        any = any or target.* != null;
    }
    if (!any) return null;
    if (try resolveWithRegistry(aa, registry, name, numbers) != null) return targets;
    return if (try cheapestConverted(aa, registry, name, numbers) != null) targets else null;
}

fn considerConverted(aa: Allocator, f: ScalarFn, name: []const u8, arg_types: []const Type, best: *?ScalarFn, best_cost: *u64) !void {
    if (!std.ascii.eqlIgnoreCase(f.name, name)) return;
    if (!scalarArityMatches(f, arg_types.len)) return;
    var total: u64 = 0;
    for (arg_types, 0..) |given, i| {
        total += (try argConversion(aa, given, scalarDeclaredTypeAt(f, i)) orelse return).cost;
    }
    if (total < best_cost.*) {
        best_cost.* = total;
        best.* = f;
    }
}

fn textAsDoubleArgs(aa: Allocator, registry: ?*const udf_mod.UdfRegistry, name: []const u8, arg_types: []const Type) !?[]const ?Type {
    const retyped = try aa.dupe(Type, arg_types);
    const targets = try aa.alloc(?Type, arg_types.len);
    var any = false;
    for (retyped, targets) |*t, *target| {
        target.* = if (t.isString()) .double else null;
        if (target.*) |d| t.* = d;
        any = any or target.* != null;
    }
    if (!any) return null;
    return if (try resolveWithRegistry(aa, registry, name, retyped) != null) targets else null;
}

const CONVERT_COST: u64 = 1000;

/// More than all the other conversions of a call's arguments add up to.
const FRACTION_DROP_COST: u64 = 1_000_000;

const ArgConversion = struct { cost: u64, target: ?Type = null };

/// Text converts to a number only where no overload takes it as text, and
/// to a double before an integer: MySQL types a text operand as DOUBLE, so
/// `ABS('-2.5')` is 2.5. A double or decimal meets an integer parameter as
/// the integer MySQL reads it as (`INTEGER_ARG_FN`), but only where no
/// overload takes every argument without dropping a fraction: that costs
/// more than any other conversion of the whole call, so `INTERVAL(2.5, 1,
/// 2.5, 3)` still compares doubles.
fn argConversion(aa: Allocator, given: Type, declared: Type) !?ArgConversion {
    if (declared.isString()) {
        if (bindsAsIs(given, declared)) return .{ .cost = 0 };
        return if (try resolve(aa, "to_string", &.{given}) != null) .{ .cost = CONVERT_COST, .target = .string } else null;
    }
    if (given.isString()) {
        if (declared.isFloat()) return .{ .cost = CONVERT_COST + 1, .target = .double };
        if (declared.isInteger()) return .{ .cost = CONVERT_COST + 2 + (argCastCost(.bigint, declared, true) orelse return null), .target = .bigint };
        return null;
    }
    if (declared.isFloat() and given.isDecimal()) return .{ .cost = CONVERT_COST, .target = .double };
    if (declared.isInteger() and (given.isFloat() or given.isDecimal()))
        return .{ .cost = FRACTION_DROP_COST + (argCastCost(.bigint, declared, true) orelse return null), .target = .bigint };
    return .{ .cost = argCastCost(given, declared, true) orelse return null };
}

/// The bit operators `& | ^ ~ << >>`, by the StarRocks function each
/// lowers to outside MySQL.
pub const BitOperator = enum { bitand, bitor, bitxor, bitnot, bit_shift_left, bit_shift_right };

/// The function bit operator `op` lowers to in `dialect`. MySQL reads the
/// operands as BIGINT UNSIGNED and returns one, held in a LARGEINT (`~1` is
/// 18446744073709551614, `-1 >> 1` is 2^63 - 1), so it takes internal
/// twins of StarRocks' functions, which keep BIGINT's two's complement as
/// DuckDB and PG do.
pub fn bitOperatorFn(op: BitOperator, dialect: types.Dialect) []const u8 {
    return switch (op) {
        inline else => |o| switch (dialect) {
            .mysql => "__mysql_" ++ @tagName(o),
            .neutral, .postgres => @tagName(o),
        },
    };
}

/// Internal: a double or decimal read as an integer argument, as MySQL reads
/// one where a function takes an integer (`REPEAT('a', 2.5)`, `ELT(1.5e0,
/// ...)`): a double rounds half to even (`common.doubleAsBigint`), a decimal
/// half away from zero (`dec.integerArgAt`), clamped to the BIGINT range.
pub const INTEGER_ARG_FN = "__integer_arg";

/// The function that reads a `given` number as the integer `target`
/// `convertedArgs` converts it to; null unless a double or decimal meets an
/// integer.
pub fn integerArgFn(given: Type, target: Type) ?[]const u8 {
    return if (target.isInteger() and (given.isFloat() or given.isDecimal())) INTEGER_ARG_FN else null;
}

/// Whether `name` reads an argument `convertedArgs` converts as CAST reads
/// it, rather than as MySQL reads a function argument. StarRocks reads
/// FROM_UNIXTIME's count so: `1.5` is 1 and `-0.5` is 0, truncated toward
/// zero, where MySQL's reading rounds; and `'1.5'` or `1e20` is NULL.
pub fn readsArgsAsCast(name: []const u8) bool {
    return std.ascii.eqlIgnoreCase(name, "from_unixtime");
}

/// Internal: text or JSON read as the number it starts with, where a
/// numeric parameter meets it and no CAST was written
/// (`common.leadingDouble`, `common.leadingInteger`).
pub const TEXT_AS_DOUBLE_FN = "__text_as_double";
pub const TEXT_AS_BIGINT_FN = "__text_as_bigint";

/// The function that reads text as a number of type `target`, the type
/// `convertedArgs` converts text to; null for any other type.
pub fn textAsNumberFn(target: Type) ?[]const u8 {
    return switch (target) {
        .double => TEXT_AS_DOUBLE_FN,
        .bigint => TEXT_AS_BIGINT_FN,
        else => null,
    };
}

fn scalarCastCost(f: ScalarFn, arg_types: []const Type, allow_narrowing: bool) ?u64 {
    if (!scalarArityMatches(f, arg_types.len)) return null;
    var total_cost: u64 = 0;
    for (arg_types, 0..) |given, i| {
        total_cost += argCastCost(given, scalarDeclaredTypeAt(f, i), allow_narrowing) orelse return null;
    }
    return total_cost;
}

fn argCastCost(given: Type, declared: Type, allow_narrowing: bool) ?u32 {
    if (bindsAsIs(given, declared)) return 0;
    const from: TypeTag = given;
    const to: TypeTag = declared;
    if (cast.castCost(from, to)) |c| return c;
    return if (allow_narrowing) cast.argNarrowingCost(from, to) else null;
}

fn expandScalarFn(aa: Allocator, f: ScalarFn, actual: usize) !ScalarFn {
    if (f.variadic_min_args == null) return f;
    var out = f;
    const arg_types = try aa.alloc(Type, actual);
    for (arg_types, 0..) |*slot, i| slot.* = scalarDeclaredTypeAt(f, i);
    out.arg_types = arg_types;
    out.variadic_min_args = null;
    out.variadic_fixed = 0;
    return out;
}

fn scalarFromUdf(entry: udf_mod.ScalarEntry) ScalarFn {
    return .{
        .name = entry.name,
        .arg_types = entry.arg_types,
        .return_type = entry.return_type,
        .null_strategy = entry.null_strategy,
        .volatility = entry.volatility,
        .udf_kernel = entry.kernel,
        .user_data = entry.user_data,
    };
}

/// Inverse lookup: return ALL registered overloads matching `name`.
/// Useful for error messages ("function 'foo' exists but no overload
/// matches your arg types").
pub fn overloadsOf(name: []const u8) []const ScalarFn {
    // Tiny scan — we don't have enough functions yet for an index to
    // matter. The whole registry fits in cache.
    var start: ?usize = null;
    var end: usize = 0;
    for (builtins, 0..) |f, i| {
        if (std.ascii.eqlIgnoreCase(f.name, name)) {
            if (start == null) start = i;
            end = i + 1;
        }
    }
    if (start) |s| return builtins[s..end];
    return &.{};
}

// ---------------------------------------------------------------------------
// Builtins — one row per registered overload. Kernels are defined in the
// per-category sibling files (scalar_fn_string / _math / _date / _cond).
// Order matters for ties in cost-based resolution (first-registered wins).
// ---------------------------------------------------------------------------

pub const builtins = [_]ScalarFn{
    // --- string → string ---
    .{ .name = "upper", .arg_types = &.{.string}, .return_type = .string, .kernel = string.upperKernel },
    .{ .name = "lower", .arg_types = &.{.string}, .return_type = .string, .kernel = string.lowerKernel },
    .{ .name = "ltrim", .arg_types = &.{.string}, .return_type = .string, .kernel = string.ltrimKernel },
    .{ .name = "rtrim", .arg_types = &.{.string}, .return_type = .string, .kernel = string.rtrimKernel },
    .{ .name = "trim", .arg_types = &.{.string}, .return_type = .string, .kernel = string.trimKernel },
    .{ .name = "ltrim", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = string.ltrimKernel },
    .{ .name = "rtrim", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = string.rtrimKernel },
    .{ .name = "trim", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = string.trimKernel },
    .{ .name = "ltrim_substring", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = string.ltrimSubstringKernel },
    .{ .name = "rtrim_substring", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = string.rtrimSubstringKernel },
    .{ .name = "trim_substring", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = string.trimSubstringKernel },
    .{ .name = "reverse", .arg_types = &.{.string}, .return_type = .string, .kernel = string.reverseKernel },
    // --- string → int ---
    // length / char_length count UTF-8 characters (DuckDB/standard semantics);
    // octet_length counts raw bytes.
    .{ .name = "length", .arg_types = &.{.string}, .return_type = .int, .kernel = string.charLengthKernel },
    .{ .name = "octet_length", .arg_types = &.{.string}, .return_type = .int, .kernel = string.lengthKernel },
    .{ .name = "char_length", .arg_types = &.{.string}, .return_type = .int, .kernel = string.charLengthKernel },
    // --- multi-arg string ---
    .{ .name = "concat", .arg_types = &.{.string}, .return_type = .string, .kernel = string.concatNKernel },
    .{ .name = "concat", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = string.concat2Kernel },
    .{ .name = "concat", .arg_types = &.{ .string, .string, .string }, .return_type = .string, .kernel = string.concat3Kernel },
    .{ .name = "concat", .arg_types = &.{.string}, .return_type = .string, .variadic_min_args = 4, .kernel = string.concatNKernel },
    .{ .name = "concat_ws", .arg_types = &.{.string}, .return_type = .string, .variadic_min_args = 2, .null_strategy = .kernel_managed, .kernel = string.concatWsKernel },
    .{ .name = "substring", .arg_types = &.{ .string, .int, .int }, .return_type = .string, .kernel = string.substringKernel },
    .{ .name = "substring", .arg_types = &.{ .string, .int }, .return_type = .string, .kernel = string.substringKernel },
    .{ .name = "left", .arg_types = &.{ .string, .int }, .return_type = .string, .kernel = string.leftKernel },
    .{ .name = "right", .arg_types = &.{ .string, .int }, .return_type = .string, .kernel = string.rightKernel },
    .{ .name = "replace", .arg_types = &.{ .string, .string, .string }, .return_type = .string, .kernel = string.replaceKernel },
    .{ .name = "regexp_replace", .arg_types = &.{ .string, .string, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = string.regexpReplaceKernel },
    .{ .name = "regexp_replace", .arg_types = &.{ .string, .string, .string, .bigint }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = string.regexpReplaceKernel },
    .{ .name = "regexp_replace", .arg_types = &.{ .string, .string, .string, .bigint, .bigint }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = string.regexpReplaceKernel },
    .{ .name = "regexp_replace", .arg_types = &.{ .string, .string, .string, .bigint, .bigint, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = string.regexpReplaceKernel },
    .{ .name = "regexp_like", .arg_types = &.{ .string, .string }, .return_type = .boolean, .null_strategy = .kernel_managed, .kernel = string.regexpLikeKernel },
    .{ .name = "regexp_like", .arg_types = &.{ .string, .string, .string }, .return_type = .boolean, .null_strategy = .kernel_managed, .kernel = string.regexpLikeKernel },
    .{ .name = "regexp_substr", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = string.regexpSubstrKernel },
    .{ .name = "regexp_substr", .arg_types = &.{ .string, .string, .bigint }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = string.regexpSubstrKernel },
    .{ .name = "regexp_substr", .arg_types = &.{ .string, .string, .bigint, .bigint }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = string.regexpSubstrKernel },
    .{ .name = "regexp_substr", .arg_types = &.{ .string, .string, .bigint, .bigint, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = string.regexpSubstrKernel },
    .{ .name = "regexp_instr", .arg_types = &.{ .string, .string }, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = string.regexpInstrKernel },
    .{ .name = "regexp_instr", .arg_types = &.{ .string, .string, .bigint }, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = string.regexpInstrKernel },
    .{ .name = "regexp_instr", .arg_types = &.{ .string, .string, .bigint, .bigint }, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = string.regexpInstrKernel },
    .{ .name = "regexp_instr", .arg_types = &.{ .string, .string, .bigint, .bigint, .bigint }, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = string.regexpInstrKernel },
    .{ .name = "regexp_instr", .arg_types = &.{ .string, .string, .bigint, .bigint, .bigint, .string }, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = string.regexpInstrKernel },
    // --- json ---
    .{ .name = "json_extract", .arg_types = &.{ .json, .string }, .return_type = .json, .null_strategy = .kernel_managed, .kernel = json.jsonExtractKernel },
    .{ .name = "json_value", .arg_types = &.{ .json, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = json.jsonValueKernel },
    .{ .name = "json_unquote", .arg_types = &.{.json}, .return_type = .string, .kernel = json.jsonUnquoteKernel },
    .{ .name = "json_quote", .arg_types = &.{.string}, .return_type = .string, .kernel = json.jsonQuoteKernel },
    .{ .name = "json_valid", .arg_types = &.{.json}, .return_type = .boolean, .null_strategy = .kernel_managed, .kernel = json.jsonValidKernel },
    .{ .name = "json_type", .arg_types = &.{.json}, .return_type = .string, .null_strategy = .kernel_managed, .kernel = json.jsonTypeKernel },
    .{ .name = "json_length", .arg_types = &.{.json}, .return_type = .int, .null_strategy = .kernel_managed, .kernel = json.jsonLengthKernel },
    .{ .name = "json_contains", .arg_types = &.{ .json, .json }, .return_type = .boolean, .null_strategy = .kernel_managed, .kernel = json.jsonContainsKernel },
    .{ .name = "json_keys", .arg_types = &.{.json}, .return_type = .json, .null_strategy = .kernel_managed, .kernel = json.jsonKeysKernel },
    .{ .name = "to_json", .arg_types = &.{.json}, .return_type = .json, .null_strategy = .kernel_managed, .kernel = json.toJsonKernel },
    .{ .name = JSON_AGG_ARRAY_FN, .arg_types = &.{.string}, .return_type = .json, .null_strategy = .kernel_managed, .kernel = json.jsonAggArrayKernel },
    .{ .name = JSON_AGG_OBJECT_FN, .arg_types = &.{.string}, .return_type = .json, .null_strategy = .kernel_managed, .kernel = json.jsonAggObjectKernel },
    // --- coalesce overloads ---
    .{ .name = "coalesce", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .absorbs, .kernel = cond.coalesceStringKernel },
    .{ .name = "coalesce", .arg_types = &.{.string}, .return_type = .string, .variadic_min_args = 2, .null_strategy = .absorbs, .kernel = cond.coalesceStringKernel },
    .{ .name = "coalesce", .arg_types = &.{ .int, .int }, .return_type = .int, .null_strategy = .absorbs, .kernel = cond.coalesceIntKernel },
    .{ .name = "coalesce", .arg_types = &.{.int}, .return_type = .int, .variadic_min_args = 2, .null_strategy = .absorbs, .kernel = cond.coalesceIntKernel },
    .{ .name = "coalesce", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .null_strategy = .absorbs, .kernel = cond.coalesceBigintKernel },
    .{ .name = "coalesce", .arg_types = &.{.bigint}, .return_type = .bigint, .variadic_min_args = 2, .null_strategy = .absorbs, .kernel = cond.coalesceBigintKernel },
    .{ .name = "coalesce", .arg_types = &.{ .largeint, .largeint }, .return_type = .largeint, .null_strategy = .absorbs, .kernel = cond.coalesceLargeintKernel },
    .{ .name = "coalesce", .arg_types = &.{.largeint}, .return_type = .largeint, .variadic_min_args = 2, .null_strategy = .absorbs, .kernel = cond.coalesceLargeintKernel },
    .{ .name = "coalesce", .arg_types = &.{ .double, .double }, .return_type = .double, .null_strategy = .absorbs, .kernel = cond.coalesceDoubleKernel },
    .{ .name = "coalesce", .arg_types = &.{.double}, .return_type = .double, .variadic_min_args = 2, .null_strategy = .absorbs, .kernel = cond.coalesceDoubleKernel },
    .{ .name = "coalesce", .arg_types = &.{ .boolean, .boolean }, .return_type = .boolean, .null_strategy = .absorbs, .kernel = cond.coalesceBooleanKernel },
    .{ .name = "coalesce", .arg_types = &.{.boolean}, .return_type = .boolean, .variadic_min_args = 2, .null_strategy = .absorbs, .kernel = cond.coalesceBooleanKernel },
    .{ .name = "coalesce", .arg_types = &.{ .date, .date }, .return_type = .date, .null_strategy = .absorbs, .kernel = cond.coalesceDateKernel },
    .{ .name = "coalesce", .arg_types = &.{.date}, .return_type = .date, .variadic_min_args = 2, .null_strategy = .absorbs, .kernel = cond.coalesceDateKernel },
    .{ .name = "coalesce", .arg_types = &.{ .datetime, .datetime }, .return_type = .datetime, .null_strategy = .absorbs, .kernel = cond.coalesceDatetimeKernel },
    .{ .name = "coalesce", .arg_types = &.{.datetime}, .return_type = .datetime, .variadic_min_args = 2, .null_strategy = .absorbs, .kernel = cond.coalesceDatetimeKernel },
    // --- math ---
    .{ .name = "abs", .arg_types = &.{.tinyint}, .return_type = .smallint, .kernel = math.absIntegerKernel(i8, i16) },
    .{ .name = "abs", .arg_types = &.{.smallint}, .return_type = .int, .kernel = math.absIntegerKernel(i16, i32) },
    .{ .name = "abs", .arg_types = &.{.int}, .return_type = .bigint, .kernel = math.absIntegerKernel(i32, i64) },
    .{ .name = "abs", .arg_types = &.{.bigint}, .return_type = .bigint, .kernel = math.absIntegerKernel(i64, i64) },
    .{ .name = "abs", .arg_types = &.{.largeint}, .return_type = .largeint, .kernel = math.absIntegerKernel(i128, i128) },
    .{ .name = "abs", .arg_types = &.{.double}, .return_type = .double, .kernel = math.absDoubleKernel },
    .{ .name = "ceil", .arg_types = &.{.double}, .return_type = .double, .kernel = math.ceilKernel },
    .{ .name = "floor", .arg_types = &.{.double}, .return_type = .double, .kernel = math.floorKernel },
    .{ .name = "round", .arg_types = &.{.double}, .return_type = .double, .kernel = math.roundKernel },
    .{ .name = "round", .arg_types = &.{ .double, .int }, .return_type = .double, .kernel = math.roundScaleKernel },
    .{ .name = "sign", .arg_types = &.{.double}, .return_type = .int, .kernel = math.signKernel },
    .{ .name = "pi", .arg_types = &.{}, .return_type = .double, .kernel = math.piKernel },
    .{ .name = "rand", .arg_types = &.{}, .return_type = .double, .volatility = .@"volatile", .kernel = math.randomKernel },
    .{ .name = "random", .arg_types = &.{}, .return_type = .double, .volatility = .@"volatile", .kernel = math.randomKernel },
    .{ .name = "uuid", .arg_types = &.{.bigint}, .return_type = .string, .volatility = .@"volatile", .kernel = string.uuidKernel },
    .{ .name = UUID_SHORT_FN, .arg_types = &.{.bigint}, .return_type = .largeint, .volatility = .@"volatile", .kernel = string.uuidShortKernel },
    // Integer MOD resolves in `resolveIntArith`. A floating operand on either
    // side: MySQL MOD keeps the dividend's sign (fmod), same as `%`.
    .{ .name = "mod", .arg_types = &.{ .double, .double }, .return_type = .double, .null_strategy = .kernel_managed, .kernel = math.modDoubleKernel },
    .{ .name = "pmod", .arg_types = &.{ .int, .int }, .return_type = .int, .null_strategy = .zero_divisor, .kernel = math.pmodIntKernel },
    .{ .name = "pmod", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .null_strategy = .zero_divisor, .kernel = math.pmodBigintKernel },
    .{ .name = "fmod", .arg_types = &.{ .double, .double }, .return_type = .double, .null_strategy = .kernel_managed, .kernel = math.fmodKernel },
    // Binary arithmetic — backs the SQL infix operators (+ - * /) in the
    // parser. Integer operands resolve in `resolveIntArith`; these overloads
    // take any floating operand, with integers promoted to double.
    .{ .name = "add", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.addDoubleKernel },
    .{ .name = "sub", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.subDoubleKernel },
    .{ .name = "mul", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.mulDoubleKernel },
    // `/` is true division per MySQL/StarRocks: integer operands widen to
    // double via the implicit-cast lattice (7 / 2 = 3.5). Explicit integer
    // division is the `DIV` operator, which lowers to `intdiv` and resolves
    // in `resolveIntArith`. A zero divisor gives NULL, as for DIV and %.
    .{ .name = "div", .arg_types = &.{ .double, .double }, .return_type = .double, .null_strategy = .zero_divisor, .kernel = math.divDoubleKernel },
    .{ .name = "pow", .arg_types = &.{ .double, .double }, .return_type = .double, .null_strategy = .kernel_managed, .kernel = math.powKernel },
    .{ .name = "sqrt", .arg_types = &.{.double}, .return_type = .double, .null_strategy = .kernel_managed, .kernel = math.sqrtKernel },
    .{ .name = "exp", .arg_types = &.{.double}, .return_type = .double, .null_strategy = .kernel_managed, .kernel = math.expKernel },
    .{ .name = "ln", .arg_types = &.{.double}, .return_type = .double, .null_strategy = .kernel_managed, .kernel = math.lnKernel },
    .{ .name = "log", .arg_types = &.{ .double, .double }, .return_type = .double, .null_strategy = .kernel_managed, .kernel = math.logBaseKernel },
    .{ .name = "log10", .arg_types = &.{.double}, .return_type = .double, .null_strategy = .kernel_managed, .kernel = math.log10Kernel },
    .{ .name = "log2", .arg_types = &.{.double}, .return_type = .double, .null_strategy = .kernel_managed, .kernel = math.log2Kernel },
    .{ .name = "greatest", .arg_types = &.{ .int, .int }, .return_type = .int, .kernel = math.greatestIntKernel },
    .{ .name = "greatest", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = math.greatestBigintKernel },
    .{ .name = "greatest", .arg_types = &.{ .largeint, .largeint }, .return_type = .largeint, .kernel = math.greatestLargeintKernel },
    .{ .name = "greatest", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.greatestDoubleKernel },
    .{ .name = "greatest", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = string.greatestStringKernel },
    .{ .name = "greatest", .arg_types = &.{ .date, .date }, .return_type = .date, .kernel = math.greatestDateKernel },
    .{ .name = "greatest", .arg_types = &.{ .datetime, .datetime }, .return_type = .datetime, .kernel = math.greatestDatetimeKernel },
    .{ .name = "least", .arg_types = &.{ .int, .int }, .return_type = .int, .kernel = math.leastIntKernel },
    .{ .name = "least", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = math.leastBigintKernel },
    .{ .name = "least", .arg_types = &.{ .largeint, .largeint }, .return_type = .largeint, .kernel = math.leastLargeintKernel },
    .{ .name = "least", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.leastDoubleKernel },
    .{ .name = "least", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = string.leastStringKernel },
    .{ .name = "least", .arg_types = &.{ .date, .date }, .return_type = .date, .kernel = math.leastDateKernel },
    .{ .name = "least", .arg_types = &.{ .datetime, .datetime }, .return_type = .datetime, .kernel = math.leastDatetimeKernel },
    .{ .name = "greatest", .arg_types = &.{.int}, .return_type = .int, .variadic_min_args = 3, .kernel = math.greatestIntKernel },
    .{ .name = "greatest", .arg_types = &.{.bigint}, .return_type = .bigint, .variadic_min_args = 3, .kernel = math.greatestBigintKernel },
    .{ .name = "greatest", .arg_types = &.{.largeint}, .return_type = .largeint, .variadic_min_args = 3, .kernel = math.greatestLargeintKernel },
    .{ .name = "greatest", .arg_types = &.{.double}, .return_type = .double, .variadic_min_args = 3, .kernel = math.greatestDoubleKernel },
    .{ .name = "greatest", .arg_types = &.{.string}, .return_type = .string, .variadic_min_args = 3, .kernel = string.greatestStringKernel },
    .{ .name = "greatest", .arg_types = &.{.date}, .return_type = .date, .variadic_min_args = 3, .kernel = math.greatestDateKernel },
    .{ .name = "greatest", .arg_types = &.{.datetime}, .return_type = .datetime, .variadic_min_args = 3, .kernel = math.greatestDatetimeKernel },
    .{ .name = "least", .arg_types = &.{.int}, .return_type = .int, .variadic_min_args = 3, .kernel = math.leastIntKernel },
    .{ .name = "least", .arg_types = &.{.bigint}, .return_type = .bigint, .variadic_min_args = 3, .kernel = math.leastBigintKernel },
    .{ .name = "least", .arg_types = &.{.largeint}, .return_type = .largeint, .variadic_min_args = 3, .kernel = math.leastLargeintKernel },
    .{ .name = "least", .arg_types = &.{.double}, .return_type = .double, .variadic_min_args = 3, .kernel = math.leastDoubleKernel },
    .{ .name = "least", .arg_types = &.{.string}, .return_type = .string, .variadic_min_args = 3, .kernel = string.leastStringKernel },
    .{ .name = "least", .arg_types = &.{.date}, .return_type = .date, .variadic_min_args = 3, .kernel = math.leastDateKernel },
    .{ .name = "least", .arg_types = &.{.datetime}, .return_type = .datetime, .variadic_min_args = 3, .kernel = math.leastDatetimeKernel },
    // --- math (expanded) ---
    .{ .name = "sin", .arg_types = &.{.double}, .return_type = .double, .kernel = math.sinKernel },
    .{ .name = "cos", .arg_types = &.{.double}, .return_type = .double, .kernel = math.cosKernel },
    .{ .name = "tan", .arg_types = &.{.double}, .return_type = .double, .kernel = math.tanKernel },
    .{ .name = "asin", .arg_types = &.{.double}, .return_type = .double, .null_strategy = .kernel_managed, .kernel = math.asinKernel },
    .{ .name = "acos", .arg_types = &.{.double}, .return_type = .double, .null_strategy = .kernel_managed, .kernel = math.acosKernel },
    .{ .name = "atan", .arg_types = &.{.double}, .return_type = .double, .kernel = math.atanKernel },
    .{ .name = "cot", .arg_types = &.{.double}, .return_type = .double, .null_strategy = .kernel_managed, .kernel = math.cotKernel },
    .{ .name = "cbrt", .arg_types = &.{.double}, .return_type = .double, .kernel = math.cbrtKernel },
    .{ .name = "square", .arg_types = &.{.double}, .return_type = .double, .kernel = math.squareKernel },
    // A LARGEINT overload comes before its BIGINT twin: a call mixing the
    // two ties on cost, and the tie goes to the first, which must not
    // narrow the LARGEINT.
    .{ .name = "bit_count", .arg_types = &.{.largeint}, .return_type = .bigint, .kernel = math.bitCountKernel(i128) },
    .{ .name = "bit_count", .arg_types = &.{.bigint}, .return_type = .bigint, .kernel = math.bitCountKernel(i64) },
    .{ .name = bitOperatorFn(.bitand, .mysql), .arg_types = &.{ .largeint, .largeint }, .return_type = .largeint, .kernel = math.unsignedBitwiseKernel(.@"and", i128) },
    .{ .name = bitOperatorFn(.bitand, .mysql), .arg_types = &.{ .bigint, .bigint }, .return_type = .largeint, .kernel = math.unsignedBitwiseKernel(.@"and", i64) },
    .{ .name = bitOperatorFn(.bitor, .mysql), .arg_types = &.{ .largeint, .largeint }, .return_type = .largeint, .kernel = math.unsignedBitwiseKernel(.@"or", i128) },
    .{ .name = bitOperatorFn(.bitor, .mysql), .arg_types = &.{ .bigint, .bigint }, .return_type = .largeint, .kernel = math.unsignedBitwiseKernel(.@"or", i64) },
    .{ .name = bitOperatorFn(.bitxor, .mysql), .arg_types = &.{ .largeint, .largeint }, .return_type = .largeint, .kernel = math.unsignedBitwiseKernel(.xor, i128) },
    .{ .name = bitOperatorFn(.bitxor, .mysql), .arg_types = &.{ .bigint, .bigint }, .return_type = .largeint, .kernel = math.unsignedBitwiseKernel(.xor, i64) },
    .{ .name = bitOperatorFn(.bit_shift_left, .mysql), .arg_types = &.{ .largeint, .largeint }, .return_type = .largeint, .kernel = math.unsignedBitwiseKernel(.shift_left, i128) },
    .{ .name = bitOperatorFn(.bit_shift_left, .mysql), .arg_types = &.{ .bigint, .bigint }, .return_type = .largeint, .kernel = math.unsignedBitwiseKernel(.shift_left, i64) },
    .{ .name = bitOperatorFn(.bit_shift_right, .mysql), .arg_types = &.{ .largeint, .largeint }, .return_type = .largeint, .kernel = math.unsignedBitwiseKernel(.shift_right, i128) },
    .{ .name = bitOperatorFn(.bit_shift_right, .mysql), .arg_types = &.{ .bigint, .bigint }, .return_type = .largeint, .kernel = math.unsignedBitwiseKernel(.shift_right, i64) },
    .{ .name = bitOperatorFn(.bitnot, .mysql), .arg_types = &.{.largeint}, .return_type = .largeint, .kernel = math.unsignedBitNotKernel(i128) },
    .{ .name = bitOperatorFn(.bitnot, .mysql), .arg_types = &.{.bigint}, .return_type = .largeint, .kernel = math.unsignedBitNotKernel(i64) },
    // StarRocks' bit functions, which the operators lower to outside MySQL.
    .{ .name = "bitand", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = math.bitwiseKernel(.@"and") },
    .{ .name = "bitor", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = math.bitwiseKernel(.@"or") },
    .{ .name = "bitxor", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = math.bitwiseKernel(.xor) },
    .{ .name = "bit_shift_left", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = math.bitwiseKernel(.shift_left) },
    .{ .name = "bit_shift_right", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = math.bitwiseKernel(.shift_right) },
    .{ .name = "bitnot", .arg_types = &.{.bigint}, .return_type = .bigint, .kernel = math.bitNotKernel },
    // A number reaches BIN and CONV as its text, as MySQL reads it there
    // (`readsNumberAsText`).
    .{ .name = "bin", .arg_types = &.{.string}, .return_type = .string, .null_strategy = .kernel_managed, .kernel = math.binKernel },
    .{ .name = "conv", .arg_types = &.{ .string, .int, .int }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = math.convKernel },
    .{ .name = "bin", .arg_types = &.{.boolean}, .return_type = .string, .null_strategy = .kernel_managed, .kernel = math.binBooleanKernel },
    .{ .name = "conv", .arg_types = &.{ .boolean, .int, .int }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = math.convBooleanKernel },
    .{ .name = CONV_BITS_FN, .arg_types = &.{ .largeint, .int, .int }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = math.convBitsKernel },
    .{ .name = "truncate", .arg_types = &.{ .double, .int }, .return_type = .double, .kernel = math.truncateKernel },
    .{ .name = "degrees", .arg_types = &.{.double}, .return_type = .double, .kernel = math.degreesKernel },
    .{ .name = "radians", .arg_types = &.{.double}, .return_type = .double, .kernel = math.radiansKernel },
    .{ .name = "atan2", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.atan2Kernel },
    // --- conditional ---
    .{ .name = "if", .arg_types = &.{ .boolean, .string, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = cond.ifStringKernel },
    .{ .name = "if", .arg_types = &.{ .boolean, .int, .int }, .return_type = .int, .null_strategy = .kernel_managed, .kernel = cond.ifIntKernel },
    .{ .name = "if", .arg_types = &.{ .boolean, .bigint, .bigint }, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = cond.ifBigintKernel },
    .{ .name = "if", .arg_types = &.{ .boolean, .double, .double }, .return_type = .double, .null_strategy = .kernel_managed, .kernel = cond.ifDoubleKernel },
    .{ .name = "if", .arg_types = &.{ .boolean, .boolean, .boolean }, .return_type = .boolean, .null_strategy = .kernel_managed, .kernel = cond.ifBooleanKernel },
    .{ .name = "if", .arg_types = &.{ .boolean, .date, .date }, .return_type = .date, .null_strategy = .kernel_managed, .kernel = cond.ifDateKernel },
    .{ .name = "if", .arg_types = &.{ .boolean, .datetime, .datetime }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = cond.ifDatetimeKernel },
    .{ .name = "ifnull", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .absorbs, .kernel = cond.ifnullStringKernel },
    .{ .name = "ifnull", .arg_types = &.{ .int, .int }, .return_type = .int, .null_strategy = .absorbs, .kernel = cond.ifnullIntKernel },
    .{ .name = "ifnull", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .null_strategy = .absorbs, .kernel = cond.ifnullBigintKernel },
    .{ .name = "ifnull", .arg_types = &.{ .largeint, .largeint }, .return_type = .largeint, .null_strategy = .absorbs, .kernel = cond.coalesceLargeintKernel },
    .{ .name = "ifnull", .arg_types = &.{ .double, .double }, .return_type = .double, .null_strategy = .absorbs, .kernel = cond.ifnullDoubleKernel },
    .{ .name = "ifnull", .arg_types = &.{ .boolean, .boolean }, .return_type = .boolean, .null_strategy = .absorbs, .kernel = cond.ifnullBooleanKernel },
    .{ .name = "ifnull", .arg_types = &.{ .date, .date }, .return_type = .date, .null_strategy = .absorbs, .kernel = cond.ifnullDateKernel },
    .{ .name = "ifnull", .arg_types = &.{ .datetime, .datetime }, .return_type = .datetime, .null_strategy = .absorbs, .kernel = cond.ifnullDatetimeKernel },
    .{ .name = "nullif", .arg_types = &.{ .int, .int }, .return_type = .int, .null_strategy = .kernel_managed, .kernel = cond.nullifIntKernel },
    .{ .name = "nullif", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = cond.nullifBigintKernel },
    .{ .name = "nullif", .arg_types = &.{ .largeint, .largeint }, .return_type = .largeint, .null_strategy = .kernel_managed, .kernel = cond.nullifLargeintKernel },
    .{ .name = "nullif", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = cond.nullifStringKernel },
    .{ .name = "nullif", .arg_types = &.{ .double, .double }, .return_type = .double, .null_strategy = .kernel_managed, .kernel = cond.nullifDoubleKernel },
    .{ .name = "nullif", .arg_types = &.{ .boolean, .boolean }, .return_type = .boolean, .null_strategy = .kernel_managed, .kernel = cond.nullifBooleanKernel },
    .{ .name = "nullif", .arg_types = &.{ .date, .date }, .return_type = .date, .null_strategy = .kernel_managed, .kernel = cond.nullifDateKernel },
    .{ .name = "nullif", .arg_types = &.{ .datetime, .datetime }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = cond.nullifDatetimeKernel },
    // --- date/time component extractors ---
    // NB: now() / current_date() are deferred — Zig 0.16's std.Io.Clock
    // needs an Io instance; kernel signature doesn't carry one yet.
    .{ .name = "year", .arg_types = &.{.date}, .return_type = .int, .kernel = date.yearFromDateKernel },
    .{ .name = "year", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.yearFromDatetimeKernel },
    .{ .name = "month", .arg_types = &.{.date}, .return_type = .int, .kernel = date.monthFromDateKernel },
    .{ .name = "month", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.monthFromDatetimeKernel },
    .{ .name = "day", .arg_types = &.{.date}, .return_type = .int, .kernel = date.dayFromDateKernel },
    .{ .name = "day", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.dayFromDatetimeKernel },
    .{ .name = "dayofmonth", .arg_types = &.{.date}, .return_type = .int, .kernel = date.dayFromDateKernel },
    .{ .name = "dayofmonth", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.dayFromDatetimeKernel },
    .{ .name = "makedate", .arg_types = &.{ .int, .int }, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.makedateKernel },
    // Text reads as MySQL reads a TIME argument: a TIME, or a DATETIME
    // when it spells one (scalar_fn_time.zig).
    .{ .name = "hour", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.hourKernel },
    .{ .name = "hour", .arg_types = &.{.string}, .return_type = .int, .null_strategy = .kernel_managed, .kernel = time.clockPartKernel(.hour) },
    .{ .name = "minute", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.minuteKernel },
    .{ .name = "minute", .arg_types = &.{.string}, .return_type = .int, .null_strategy = .kernel_managed, .kernel = time.clockPartKernel(.minute) },
    .{ .name = "second", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.secondKernel },
    .{ .name = "second", .arg_types = &.{.string}, .return_type = .int, .null_strategy = .kernel_managed, .kernel = time.clockPartKernel(.second) },
    .{ .name = "time", .arg_types = &.{.string}, .return_type = .string, .null_strategy = .kernel_managed, .kernel = time.timeKernel },
    .{ .name = "time", .arg_types = &.{.datetime}, .return_type = .string, .null_strategy = .kernel_managed, .kernel = time.timeKernel },
    .{ .name = "time", .arg_types = &.{.date}, .return_type = .string, .null_strategy = .kernel_managed, .kernel = time.timeKernel },
    .{ .name = "time_to_sec", .arg_types = &.{.string}, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = time.timeToSecKernel },
    .{ .name = "time_to_sec", .arg_types = &.{.datetime}, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = time.timeToSecKernel },
    .{ .name = "time_to_sec", .arg_types = &.{.date}, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = time.timeToSecKernel },
    .{ .name = "timediff", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = time.timediffKernel },
    .{ .name = "timediff", .arg_types = &.{ .datetime, .datetime }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = time.timediffKernel },
    .{ .name = "timediff", .arg_types = &.{ .date, .date }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = time.timediffKernel },
    .{ .name = "timediff", .arg_types = &.{ .datetime, .date }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = time.timediffKernel },
    .{ .name = "timediff", .arg_types = &.{ .date, .datetime }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = time.timediffKernel },
    .{ .name = "addtime", .arg_types = &.{ .datetime, .string }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = time.addTimeKernel(1) },
    .{ .name = "addtime", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = time.addTimeKernel(1) },
    .{ .name = "subtime", .arg_types = &.{ .datetime, .string }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = time.addTimeKernel(-1) },
    .{ .name = "subtime", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = time.addTimeKernel(-1) },
    .{ .name = "to_days", .arg_types = &.{.date}, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = date.toDaysKernel(.date) },
    .{ .name = "to_days", .arg_types = &.{.datetime}, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = date.toDaysKernel(.datetime) },
    .{ .name = "to_seconds", .arg_types = &.{.datetime}, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = date.toSecondsKernel },
    .{ .name = "from_days", .arg_types = &.{.bigint}, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.fromDaysKernel },
    .{ .name = "period_add", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = date.periodAddKernel },
    .{ .name = "period_diff", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = date.periodDiffKernel },
    .{ .name = "convert_tz", .arg_types = &.{ .datetime, .string, .string }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.convertTzKernel },
    // --- date arithmetic + epoch conversion ---
    .{ .name = "datediff", .arg_types = &.{ .date, .date }, .return_type = .int, .kernel = date.datediffKernel },
    .{ .name = "datediff", .arg_types = &.{ .datetime, .datetime }, .return_type = .int, .kernel = date.datediffDatetimeKernel },
    // Each interval unit has its own kernel, which the parser calls when it
    // lowers `x + INTERVAL n unit`, so the unit's size applies inside the
    // range check. A month step clamps the day on a short destination month
    // (`2024-01-31 + 1 month → 2024-02-29`), and a result outside years
    // 0-9999 is NULL, as in StarRocks.
    .{ .name = "date_add", .arg_types = &.{ .date, .int }, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.dateAddDaysKernel },
    .{ .name = "date_sub", .arg_types = &.{ .date, .int }, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.dateSubDaysKernel },
    .{ .name = "date_add_weeks", .arg_types = &.{ .date, .int }, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.dateAddWeeksKernel },
    .{ .name = "date_add_months", .arg_types = &.{ .date, .int }, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.dateAddMonthsKernel },
    .{ .name = "date_add_quarters", .arg_types = &.{ .date, .int }, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.dateAddQuartersKernel },
    .{ .name = "date_add_years", .arg_types = &.{ .date, .int }, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.dateAddYearsKernel },
    .{ .name = "date_add", .arg_types = &.{ .datetime, .int }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.datetimeAddDaysKernel },
    .{ .name = "date_sub", .arg_types = &.{ .datetime, .int }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.datetimeSubDaysKernel },
    .{ .name = "date_add_weeks", .arg_types = &.{ .datetime, .int }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.datetimeAddWeeksKernel },
    .{ .name = "date_add_months", .arg_types = &.{ .datetime, .int }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.datetimeAddMonthsKernel },
    .{ .name = "date_add_quarters", .arg_types = &.{ .datetime, .int }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.datetimeAddQuartersKernel },
    .{ .name = "date_add_years", .arg_types = &.{ .datetime, .int }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.datetimeAddYearsKernel },
    // A sub-day count is a BIGINT so that one past INT reaches the kernel,
    // which reads it as NULL, where a narrowing cast would saturate it.
    .{ .name = "date_add_hours", .arg_types = &.{ .datetime, .bigint }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.datetimeAddHoursKernel },
    .{ .name = "date_add_minutes", .arg_types = &.{ .datetime, .bigint }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.datetimeAddMinutesKernel },
    .{ .name = "date_add_seconds", .arg_types = &.{ .datetime, .bigint }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.datetimeAddSecondsKernel },
    .{ .name = "date_add_micros", .arg_types = &.{ .datetime, .bigint }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.datetimeAddMicrosKernel },
    .{ .name = "unix_timestamp", .arg_types = &.{.datetime}, .return_type = .bigint, .kernel = date.unixTimestampKernel },
    .{ .name = "from_unixtime", .arg_types = &.{.bigint}, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.fromUnixtimeKernel },
    .{ .name = "from_unixtime", .arg_types = &.{ .bigint, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = date.fromUnixtimeFormatKernel },
    .{ .name = "date_trunc", .arg_types = &.{ .string, .datetime }, .return_type = .datetime, .kernel = date.dateTruncKernel },
    .{ .name = "date_diff", .arg_types = &.{ .string, .date, .date }, .return_type = .bigint, .kernel = date.dateDiffDateKernel },
    .{ .name = "date_diff", .arg_types = &.{ .string, .datetime, .datetime }, .return_type = .bigint, .kernel = date.dateDiffDatetimeKernel },
    .{ .name = "years_diff", .arg_types = &.{ .datetime, .datetime }, .return_type = .bigint, .kernel = date.yearsDiffKernel },
    .{ .name = "months_diff", .arg_types = &.{ .datetime, .datetime }, .return_type = .bigint, .kernel = date.monthsDiffKernel },
    .{ .name = "weeks_diff", .arg_types = &.{ .datetime, .datetime }, .return_type = .bigint, .kernel = date.weeksDiffKernel },
    .{ .name = "days_diff", .arg_types = &.{ .datetime, .datetime }, .return_type = .bigint, .kernel = date.daysDiffKernel },
    .{ .name = "hours_diff", .arg_types = &.{ .datetime, .datetime }, .return_type = .bigint, .kernel = date.hoursDiffKernel },
    .{ .name = "minutes_diff", .arg_types = &.{ .datetime, .datetime }, .return_type = .bigint, .kernel = date.minutesDiffKernel },
    .{ .name = "seconds_diff", .arg_types = &.{ .datetime, .datetime }, .return_type = .bigint, .kernel = date.secondsDiffKernel },
    .{ .name = "milliseconds_diff", .arg_types = &.{ .datetime, .datetime }, .return_type = .bigint, .kernel = date.millisecondsDiffKernel },
    // DATEs widen to DATETIMEs at midnight, which leaves every unit's
    // count unchanged.
    .{ .name = "timestampdiff", .arg_types = &.{ .string, .datetime, .datetime }, .return_type = .bigint, .kernel = date.timestampDiffKernel },
    .{ .name = "timestampadd", .arg_types = &.{ .string, .int, .date }, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.timestampAddDateKernel },
    .{ .name = "timestampadd", .arg_types = &.{ .string, .int, .datetime }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.timestampAddDatetimeKernel },
    // --- date (expanded MySQL-style helpers) ---
    .{ .name = "dayname", .arg_types = &.{.date}, .return_type = .string, .kernel = date.daynameFromDateKernel },
    .{ .name = "dayname", .arg_types = &.{.datetime}, .return_type = .string, .kernel = date.daynameFromDatetimeKernel },
    .{ .name = "monthname", .arg_types = &.{.date}, .return_type = .string, .kernel = date.monthnameFromDateKernel },
    .{ .name = "monthname", .arg_types = &.{.datetime}, .return_type = .string, .kernel = date.monthnameFromDatetimeKernel },
    .{ .name = "dayofweek", .arg_types = &.{.date}, .return_type = .int, .kernel = date.dayofweekFromDateKernel },
    .{ .name = "dayofweek", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.dayofweekFromDatetimeKernel },
    .{ .name = "dayofyear", .arg_types = &.{.date}, .return_type = .int, .kernel = date.dayofyearFromDateKernel },
    .{ .name = "dayofyear", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.dayofyearFromDatetimeKernel },
    .{ .name = "quarter", .arg_types = &.{.date}, .return_type = .int, .kernel = date.quarterFromDateKernel },
    .{ .name = "quarter", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.quarterFromDatetimeKernel },
    .{ .name = "last_day", .arg_types = &.{.date}, .return_type = .date, .kernel = date.lastDayFromDateKernel },
    .{ .name = "last_day", .arg_types = &.{.datetime}, .return_type = .date, .kernel = date.lastDayFromDatetimeKernel },
    .{ .name = "date_format", .arg_types = &.{ .datetime, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = date.dateFormatDatetimeKernel },
    .{ .name = "date_format", .arg_types = &.{ .date, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = date.dateFormatDateKernel },
    .{ .name = "str_to_date", .arg_types = &.{ .string, .string }, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = datefmt.strToDateKernel },
    .{ .name = "str_to_time", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = datefmt.strToTimeKernel },
    .{ .name = "get_format", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = datefmt.getFormatKernel },
    .{ .name = "week", .arg_types = &.{.date}, .return_type = .int, .kernel = datefmt.weekKernel(.date) },
    .{ .name = "week", .arg_types = &.{.datetime}, .return_type = .int, .kernel = datefmt.weekKernel(.datetime) },
    .{ .name = "week", .arg_types = &.{ .date, .int }, .return_type = .int, .kernel = datefmt.weekKernel(.date) },
    .{ .name = "week", .arg_types = &.{ .datetime, .int }, .return_type = .int, .kernel = datefmt.weekKernel(.datetime) },
    .{ .name = "yearweek", .arg_types = &.{.date}, .return_type = .int, .kernel = datefmt.yearWeekKernel(.date) },
    .{ .name = "yearweek", .arg_types = &.{.datetime}, .return_type = .int, .kernel = datefmt.yearWeekKernel(.datetime) },
    .{ .name = "yearweek", .arg_types = &.{ .date, .int }, .return_type = .int, .kernel = datefmt.yearWeekKernel(.date) },
    .{ .name = "yearweek", .arg_types = &.{ .datetime, .int }, .return_type = .int, .kernel = datefmt.yearWeekKernel(.datetime) },
    .{ .name = "weekofyear", .arg_types = &.{.date}, .return_type = .int, .kernel = datefmt.weekOfYearKernel(.date) },
    .{ .name = "weekofyear", .arg_types = &.{.datetime}, .return_type = .int, .kernel = datefmt.weekOfYearKernel(.datetime) },
    .{ .name = "weekday", .arg_types = &.{.date}, .return_type = .int, .kernel = datefmt.weekdayKernel(.date) },
    .{ .name = "weekday", .arg_types = &.{.datetime}, .return_type = .int, .kernel = datefmt.weekdayKernel(.datetime) },
    .{ .name = "microsecond", .arg_types = &.{.date}, .return_type = .int, .kernel = datefmt.microsecondKernel(.date) },
    .{ .name = "microsecond", .arg_types = &.{.datetime}, .return_type = .int, .kernel = datefmt.microsecondKernel(.datetime) },
    .{ .name = "microsecond", .arg_types = &.{.string}, .return_type = .int, .null_strategy = .kernel_managed, .kernel = time.clockPartKernel(.microsecond) },
    // --- conversion ---
    // Numeric widening (int → bigint → double): always succeeds.
    .{ .name = "to_int", .arg_types = &.{.int}, .return_type = .int, .kernel = math.intIdentityKernel },
    .{ .name = "to_bigint", .arg_types = &.{.bigint}, .return_type = .bigint, .kernel = math.bigintIdentityKernel },
    .{ .name = "to_smallint", .arg_types = &.{.smallint}, .return_type = .smallint, .kernel = math.smallintIdentityKernel },
    .{ .name = "to_tinyint", .arg_types = &.{.tinyint}, .return_type = .tinyint, .kernel = math.tinyintIdentityKernel },
    .{ .name = "to_largeint", .arg_types = &.{.largeint}, .return_type = .largeint, .kernel = math.largeintIdentityKernel },
    .{ .name = "to_double", .arg_types = &.{.double}, .return_type = .double, .kernel = math.doubleIdentityKernel },
    .{ .name = "to_boolean", .arg_types = &.{.boolean}, .return_type = .boolean, .kernel = math.booleanIdentityKernel },
    .{ .name = "to_date", .arg_types = &.{.date}, .return_type = .date, .kernel = date.dateIdentityKernel },
    .{ .name = "to_datetime", .arg_types = &.{.datetime}, .return_type = .datetime, .kernel = date.datetimeIdentityKernel },
    .{ .name = "to_string", .arg_types = &.{.string}, .return_type = .string, .kernel = string.stringIdentityKernel },
    .{ .name = "to_string", .arg_types = &.{.{ .varchar = 0 }}, .return_type = .string, .kernel = string.stringIdentityKernel },
    .{ .name = "to_string", .arg_types = &.{.{ .char = 0 }}, .return_type = .string, .kernel = string.stringIdentityKernel },
    .{ .name = expr_mod.HEX_LITERAL_FN, .arg_types = &.{.string}, .return_type = .string, .kernel = string.stringIdentityKernel },
    .{ .name = "to_bigint", .arg_types = &.{.int}, .return_type = .bigint, .kernel = math.intToBigintKernel },
    .{ .name = "to_double", .arg_types = &.{.int}, .return_type = .double, .kernel = math.intToDoubleKernel },
    .{ .name = "to_double", .arg_types = &.{.bigint}, .return_type = .double, .kernel = math.bigintToDoubleKernel },
    .{ .name = "to_largeint", .arg_types = &.{.bigint}, .return_type = .largeint, .kernel = math.bigintToLargeintKernel },
    .{ .name = "to_boolean", .arg_types = &.{.bigint}, .return_type = .boolean, .kernel = math.bigintToBoolKernel },
    .{ .name = "to_boolean", .arg_types = &.{.double}, .return_type = .boolean, .kernel = math.doubleToBoolKernel },
    // Conversions that can fail, StarRocks semantics: numbers truncate
    // toward zero, and a value outside the target's range or text that
    // isn't a number of the target's kind is NULL. Narrower integer
    // sources widen to bigint first through the implicit-cast ranking, so a
    // bigint overload covers them.
    .{ .name = "to_tinyint", .arg_types = &.{.bigint}, .return_type = .tinyint, .null_strategy = .kernel_managed, .kernel = math.bigintToTinyintKernel },
    .{ .name = "to_tinyint", .arg_types = &.{.double}, .return_type = .tinyint, .null_strategy = .kernel_managed, .kernel = math.doubleToTinyintKernel },
    .{ .name = "to_tinyint", .arg_types = &.{.string}, .return_type = .tinyint, .null_strategy = .kernel_managed, .kernel = math.stringToTinyintKernel },
    .{ .name = "to_smallint", .arg_types = &.{.bigint}, .return_type = .smallint, .null_strategy = .kernel_managed, .kernel = math.bigintToSmallintKernel },
    .{ .name = "to_smallint", .arg_types = &.{.double}, .return_type = .smallint, .null_strategy = .kernel_managed, .kernel = math.doubleToSmallintKernel },
    .{ .name = "to_smallint", .arg_types = &.{.string}, .return_type = .smallint, .null_strategy = .kernel_managed, .kernel = math.stringToSmallintKernel },
    .{ .name = "to_int", .arg_types = &.{.bigint}, .return_type = .int, .null_strategy = .kernel_managed, .kernel = math.bigintToIntKernel },
    .{ .name = "to_int", .arg_types = &.{.double}, .return_type = .int, .null_strategy = .kernel_managed, .kernel = math.doubleToIntKernel },
    .{ .name = "to_int", .arg_types = &.{.string}, .return_type = .int, .null_strategy = .kernel_managed, .kernel = math.stringToIntKernel },
    .{ .name = "to_bigint", .arg_types = &.{.double}, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = math.doubleToBigintKernel },
    .{ .name = "to_bigint", .arg_types = &.{.string}, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = math.stringToBigintKernel },
    // A date or datetime as a number is its YYYYMMDD[HHMMSS] digits, as in MySQL.
    .{ .name = "to_bigint", .arg_types = &.{.date}, .return_type = .bigint, .kernel = date.dateToBigintKernel },
    .{ .name = "to_bigint", .arg_types = &.{.datetime}, .return_type = .bigint, .kernel = date.datetimeToBigintKernel },
    .{ .name = "to_bigint", .arg_types = &.{.largeint}, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = math.largeintToBigintKernel },
    .{ .name = "to_largeint", .arg_types = &.{.double}, .return_type = .largeint, .null_strategy = .kernel_managed, .kernel = math.doubleToLargeintKernel },
    .{ .name = "to_largeint", .arg_types = &.{.string}, .return_type = .largeint, .null_strategy = .kernel_managed, .kernel = math.stringToLargeintKernel },
    .{ .name = "to_largeint", .arg_types = &.{.date}, .return_type = .largeint, .kernel = date.dateToLargeintKernel },
    .{ .name = "to_largeint", .arg_types = &.{.datetime}, .return_type = .largeint, .kernel = date.datetimeToLargeintKernel },
    .{ .name = "to_double", .arg_types = &.{.string}, .return_type = .double, .null_strategy = .kernel_managed, .kernel = math.stringToDoubleKernel },
    .{ .name = "to_double", .arg_types = &.{.date}, .return_type = .double, .kernel = date.dateToDoubleKernel },
    .{ .name = "to_double", .arg_types = &.{.datetime}, .return_type = .double, .kernel = date.datetimeToDoubleKernel },
    .{ .name = "to_boolean", .arg_types = &.{.string}, .return_type = .boolean, .null_strategy = .kernel_managed, .kernel = math.stringToBoolKernel },
    // Text or JSON read as a number where no CAST was written (`argConversion`).
    .{ .name = TEXT_AS_DOUBLE_FN, .arg_types = &.{.json}, .return_type = .double, .kernel = math.textAsDoubleKernel },
    .{ .name = TEXT_AS_BIGINT_FN, .arg_types = &.{.json}, .return_type = .bigint, .kernel = math.textAsBigintKernel },
    // A double read where an integer parameter meets it (`argConversion`);
    // `resolveDecimal` takes a DECIMAL.
    .{ .name = INTEGER_ARG_FN, .arg_types = &.{.double}, .return_type = .bigint, .kernel = math.doubleIntegerArgKernel },
    // date <-> datetime
    .{ .name = "to_date", .arg_types = &.{.datetime}, .return_type = .date, .kernel = date.datetimeToDateKernel },
    .{ .name = "to_datetime", .arg_types = &.{.date}, .return_type = .datetime, .kernel = date.dateToDatetimeKernel },
    // Numbers and text read as dates, as StarRocks casts them: a value that
    // isn't a date is NULL. Compute reads a text literal once at plan time
    // instead, with the same kernel (`foldTextRead`). Narrower integers widen
    // to bigint. A decimal converts to double and is truncated: converting
    // it to text costs the same, so the number overloads come first to win
    // that tie.
    .{ .name = "to_date", .arg_types = &.{.bigint}, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.bigintToDateKernel },
    .{ .name = "to_date", .arg_types = &.{.double}, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.doubleToDateKernel },
    .{ .name = "to_datetime", .arg_types = &.{.bigint}, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.bigintToDatetimeKernel },
    .{ .name = "to_datetime", .arg_types = &.{.double}, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.doubleToDatetimeKernel },
    .{ .name = "to_date", .arg_types = &.{.string}, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.stringToDateKernel },
    .{ .name = "to_datetime", .arg_types = &.{.string}, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.stringToDatetimeKernel },
    // DATE(x) is CAST(x AS DATE) except for text, which it reads as a
    // DATETIME, so an invalid time of day makes it NULL.
    .{ .name = "date", .arg_types = &.{.datetime}, .return_type = .date, .kernel = date.datetimeToDateKernel },
    .{ .name = "date", .arg_types = &.{.date}, .return_type = .date, .kernel = date.dateIdentityKernel },
    .{ .name = "date", .arg_types = &.{.bigint}, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.bigintToDateKernel },
    .{ .name = "date", .arg_types = &.{.double}, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.doubleToDateKernel },
    .{ .name = "date", .arg_types = &.{.string}, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.stringDatetimeDayKernel },
    // Stringify numerics.
    .{ .name = "to_string", .arg_types = &.{.int}, .return_type = .string, .kernel = math.integerToStringKernel(i32) },
    .{ .name = "to_string", .arg_types = &.{.bigint}, .return_type = .string, .kernel = math.integerToStringKernel(i64) },
    .{ .name = "to_string", .arg_types = &.{.largeint}, .return_type = .string, .kernel = math.integerToStringKernel(i128) },
    .{ .name = "to_string", .arg_types = &.{.double}, .return_type = .string, .kernel = math.doubleToStringKernel },
    .{ .name = "to_string", .arg_types = &.{.float}, .return_type = .string, .kernel = math.floatToStringKernel },
    .{ .name = "to_string", .arg_types = &.{.boolean}, .return_type = .string, .kernel = math.boolToStringKernel },
    .{ .name = "to_string", .arg_types = &.{.date}, .return_type = .string, .kernel = date.dateToStringKernel },
    .{ .name = "to_string", .arg_types = &.{.datetime}, .return_type = .string, .kernel = date.datetimeToStringKernel },
    // --- hash ---
    .{ .name = "md5", .arg_types = &.{.string}, .return_type = .string, .kernel = string.md5Kernel },
    .{ .name = "md5sum", .arg_types = &.{.string}, .return_type = .string, .variadic_min_args = 1, .kernel = string.md5sumKernel },
    .{ .name = "sha1", .arg_types = &.{.string}, .return_type = .string, .kernel = string.sha1Kernel },
    .{ .name = "sha2", .arg_types = &.{ .string, .int }, .return_type = .string, .kernel = string.sha2Kernel },
    .{ .name = "sha256", .arg_types = &.{.string}, .return_type = .string, .kernel = string.sha256Kernel },
    .{ .name = "crc32", .arg_types = &.{.string}, .return_type = .bigint, .kernel = string.crc32Kernel },
    .{ .name = "murmur_hash3_32", .arg_types = &.{.string}, .return_type = .bigint, .kernel = string.murmurHash3_32Kernel },
    .{ .name = "xx_hash3_64", .arg_types = &.{.string}, .return_type = .bigint, .kernel = string.xxHash3_64Kernel },
    .{ .name = "xx_hash3_128", .arg_types = &.{.string}, .return_type = .string, .kernel = string.xxHash3_128Kernel },
    // --- encoding ---
    .{ .name = "hex", .arg_types = &.{.string}, .return_type = .string, .kernel = string.hexEncodeKernel },
    .{ .name = "hex", .arg_types = &.{.bigint}, .return_type = .string, .kernel = string.hexBigintKernel },
    .{ .name = "hex", .arg_types = &.{.largeint}, .return_type = .string, .kernel = string.hexLargeintKernel },
    .{ .name = "hex", .arg_types = &.{.double}, .return_type = .string, .kernel = string.hexDoubleKernel },
    .{ .name = "unhex", .arg_types = &.{.string}, .return_type = .string, .kernel = string.hexDecodeKernel },
    .{ .name = "to_base64", .arg_types = &.{.string}, .return_type = .string, .kernel = string.base64EncodeKernel },
    .{ .name = "from_base64", .arg_types = &.{.string}, .return_type = .string, .kernel = string.base64DecodeKernel },
    // --- string (expanded set; matches DuckDB / MySQL / StarRocks parity) ---
    .{ .name = "lpad", .arg_types = &.{ .string, .int, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = string.lpadKernel },
    .{ .name = "rpad", .arg_types = &.{ .string, .int, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = string.rpadKernel },
    .{ .name = "repeat", .arg_types = &.{ .string, .int }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = string.repeatKernel },
    .{ .name = "space", .arg_types = &.{.int}, .return_type = .string, .null_strategy = .kernel_managed, .kernel = string.spaceKernel },
    .{ .name = "ascii", .arg_types = &.{.string}, .return_type = .int, .kernel = string.asciiKernel },
    .{ .name = "ord", .arg_types = &.{.string}, .return_type = .int, .kernel = string.ordKernel },
    .{ .name = "bit_length", .arg_types = &.{.string}, .return_type = .int, .kernel = string.bitLengthKernel },
    .{ .name = "position", .arg_types = &.{ .string, .string }, .return_type = .int, .kernel = string.positionKernel },
    .{ .name = "locate", .arg_types = &.{ .string, .string }, .return_type = .int, .kernel = string.positionKernel },
    .{ .name = "locate", .arg_types = &.{ .string, .string, .int }, .return_type = .int, .kernel = string.locateFromKernel },
    .{ .name = "strpos", .arg_types = &.{ .string, .string }, .return_type = .int, .kernel = string.instrKernel },
    .{ .name = "instr", .arg_types = &.{ .string, .string }, .return_type = .int, .kernel = string.instrKernel },
    .{ .name = "starts_with", .arg_types = &.{ .string, .string }, .return_type = .boolean, .kernel = string.startsWithKernel },
    .{ .name = "ends_with", .arg_types = &.{ .string, .string }, .return_type = .boolean, .kernel = string.endsWithKernel },
    .{ .name = "split_part", .arg_types = &.{ .string, .string, .int }, .return_type = .string, .kernel = string.splitPartKernel },
    .{ .name = "substring_index", .arg_types = &.{ .string, .string, .int }, .return_type = .string, .kernel = string.substringIndexKernel },
    .{ .name = "strcmp", .arg_types = &.{ .string, .string }, .return_type = .int, .kernel = string.strcmpKernel },
    .{ .name = "field", .arg_types = &.{.string}, .return_type = .int, .variadic_min_args = 2, .kernel = string.fieldKernel },
    .{ .name = "find_in_set", .arg_types = &.{ .string, .string }, .return_type = .int, .kernel = string.findInSetKernel },
    .{ .name = "initcap", .arg_types = &.{.string}, .return_type = .string, .kernel = string.initcapKernel },
    .{ .name = "translate", .arg_types = &.{ .string, .string, .string }, .return_type = .string, .kernel = string.translateKernel },
    .{ .name = "chr", .arg_types = &.{.int}, .return_type = .string, .kernel = string.chrKernel },
    .{ .name = "elt", .arg_types = &.{ .bigint, .string }, .return_type = .string, .variadic_min_args = 2, .variadic_fixed = 1, .null_strategy = .kernel_managed, .kernel = string.eltKernel },
    .{ .name = "make_set", .arg_types = &.{ .bigint, .string }, .return_type = .string, .variadic_min_args = 2, .variadic_fixed = 1, .null_strategy = .kernel_managed, .kernel = string.makeSetKernel },
    .{ .name = "insert", .arg_types = &.{ .string, .int, .int, .string }, .return_type = .string, .kernel = string.insertKernel },
    .{ .name = "quote", .arg_types = &.{.string}, .return_type = .string, .null_strategy = .kernel_managed, .kernel = string.quoteKernel },
    .{ .name = "soundex", .arg_types = &.{.string}, .return_type = .string, .kernel = string.soundexKernel },
    // --- network addresses ---
    .{ .name = "inet_aton", .arg_types = &.{.string}, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = inet.inetAtonKernel },
    .{ .name = "inet_ntoa", .arg_types = &.{.bigint}, .return_type = .string, .null_strategy = .kernel_managed, .kernel = inet.inetNtoaKernel },
    .{ .name = "inet_ntoa", .arg_types = &.{.double}, .return_type = .string, .null_strategy = .kernel_managed, .kernel = inet.inetNtoaKernel },
    .{ .name = "inet_ntoa", .arg_types = &.{.string}, .return_type = .string, .null_strategy = .kernel_managed, .kernel = inet.inetNtoaKernel },
    .{ .name = "inet6_aton", .arg_types = &.{.string}, .return_type = .string, .null_strategy = .kernel_managed, .kernel = inet.inet6AtonKernel },
    .{ .name = "inet6_ntoa", .arg_types = &.{.string}, .return_type = .string, .null_strategy = .kernel_managed, .kernel = inet.inet6NtoaKernel },
    .{ .name = "is_ipv4", .arg_types = &.{.string}, .return_type = .bigint, .kernel = inet.isIpv4Kernel },
    .{ .name = "is_ipv6", .arg_types = &.{.string}, .return_type = .bigint, .kernel = inet.isIpv6Kernel },
    .{ .name = "is_ipv4_compat", .arg_types = &.{.string}, .return_type = .bigint, .kernel = inet.isIpv4CompatKernel },
    .{ .name = "is_ipv4_mapped", .arg_types = &.{.string}, .return_type = .bigint, .kernel = inet.isIpv4MappedKernel },
    // --- MySQL misc ---
    .{ .name = "interval", .arg_types = &.{.bigint}, .return_type = .bigint, .variadic_min_args = 2, .null_strategy = .kernel_managed, .kernel = math.intervalKernel("bigint") },
    .{ .name = "interval", .arg_types = &.{.double}, .return_type = .bigint, .variadic_min_args = 2, .null_strategy = .kernel_managed, .kernel = math.intervalKernel("double") },
    .{ .name = "sleep", .arg_types = &.{.double}, .return_type = .bigint, .null_strategy = .kernel_managed, .volatility = .@"volatile", .kernel = math.sleepKernel },
} ++ extractBuiltins();

/// `extract_<unit>` over text, a DATETIME or a DATE for each of MySQL's
/// compound EXTRACT units, which the parser lowers EXTRACT(unit FROM x) to.
fn extractBuiltins() [3 * (std.meta.fields(time.ClockUnit).len + 1)]ScalarFn {
    const operand_types = [_]Type{ .string, .datetime, .date };
    var fns: [3 * (std.meta.fields(time.ClockUnit).len + 1)]ScalarFn = undefined;
    for (operand_types, 0..) |t, i| fns[i] = .{
        .name = "extract_year_month",
        .arg_types = &[_]Type{t},
        .return_type = .bigint,
        .null_strategy = .kernel_managed,
        .kernel = time.extractYearMonthKernel,
    };
    for (std.enums.values(time.ClockUnit), 1..) |unit, u| {
        for (operand_types, 0..) |t, i| fns[u * 3 + i] = .{
            .name = "extract_" ++ @tagName(unit),
            .arg_types = &[_]Type{t},
            .return_type = .bigint,
            .null_strategy = .kernel_managed,
            .kernel = time.extractClockKernel(unit),
        };
    }
    return fns;
}

/// Other dialects' spellings of builtins. The parser rewrites a call to
/// its canonical name, so an alias carries every overload of its target
/// and every later name check sees one name.
const FUNCTION_ALIASES = [_]struct { alias: []const u8, name: []const u8 }{
    .{ .alias = "substr", .name = "substring" },
    .{ .alias = "mid", .name = "substring" },
    .{ .alias = "lcase", .name = "lower" },
    .{ .alias = "ucase", .name = "upper" },
    .{ .alias = "power", .name = "pow" },
    .{ .alias = "ceiling", .name = "ceil" },
    .{ .alias = "char", .name = "chr" },
    .{ .alias = "months_add", .name = "date_add_months" },
};

pub fn canonicalName(name: []const u8) []const u8 {
    for (FUNCTION_ALIASES) |a| {
        if (std.ascii.eqlIgnoreCase(name, a.alias)) return a.name;
    }
    return name;
}

// ---------------------------------------------------------------------------
// User-facing builder helpers
//
// These wrap `expr.call(arena, ...)` with a tighter signature per
// function so call sites read naturally:
//
//   try thindb.expr.upper(arena, thindb.expr.col("name"))
//
// One helper per registered function. Overloaded functions get a
// single helper that takes any matching arg type.
// ---------------------------------------------------------------------------

const expr_mod = @import("expr.zig");

// --- string ---
pub fn upper(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "upper", &.{arg});
}
pub fn lower(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "lower", &.{arg});
}
pub fn length(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "length", &.{arg});
}
pub fn coalesce(arena: Allocator, a: Expr, b: Expr) !Expr {
    return expr_mod.call(arena, "coalesce", &.{ a, b });
}
pub fn coalesceArgs(arena: Allocator, args: []const Expr) !Expr {
    return expr_mod.call(arena, "coalesce", args);
}
pub fn ltrim(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "ltrim", &.{arg});
}
pub fn rtrim(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "rtrim", &.{arg});
}
pub fn trim(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "trim", &.{arg});
}
pub fn reverse(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "reverse", &.{arg});
}
pub fn octetLength(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "octet_length", &.{arg});
}
pub fn charLength(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "char_length", &.{arg});
}
pub fn concat(arena: Allocator, args: []const Expr) !Expr {
    return expr_mod.call(arena, "concat", args);
}
pub fn substring(arena: Allocator, s: Expr, start: Expr, length_arg: Expr) !Expr {
    return expr_mod.call(arena, "substring", &.{ s, start, length_arg });
}
pub fn replace(arena: Allocator, haystack: Expr, needle: Expr, repl: Expr) !Expr {
    return expr_mod.call(arena, "replace", &.{ haystack, needle, repl });
}
pub fn concatWs(arena: Allocator, args: []const Expr) !Expr {
    return expr_mod.call(arena, "concat_ws", args);
}
pub fn left(arena: Allocator, s: Expr, n: Expr) !Expr {
    return expr_mod.call(arena, "left", &.{ s, n });
}
pub fn right(arena: Allocator, s: Expr, n: Expr) !Expr {
    return expr_mod.call(arena, "right", &.{ s, n });
}
pub fn locate(arena: Allocator, needle: Expr, hay: Expr) !Expr {
    return expr_mod.call(arena, "locate", &.{ needle, hay });
}
pub fn strpos(arena: Allocator, needle: Expr, hay: Expr) !Expr {
    return expr_mod.call(arena, "strpos", &.{ needle, hay });
}
pub fn startsWith(arena: Allocator, s: Expr, prefix: Expr) !Expr {
    return expr_mod.call(arena, "starts_with", &.{ s, prefix });
}
pub fn endsWith(arena: Allocator, s: Expr, suffix: Expr) !Expr {
    return expr_mod.call(arena, "ends_with", &.{ s, suffix });
}
pub fn splitPart(arena: Allocator, s: Expr, delim: Expr, part: Expr) !Expr {
    return expr_mod.call(arena, "split_part", &.{ s, delim, part });
}
pub fn regexpLike(arena: Allocator, s: Expr, pattern: Expr) !Expr {
    return expr_mod.call(arena, "regexp_like", &.{ s, pattern });
}
pub fn regexpSubstr(arena: Allocator, s: Expr, pattern: Expr) !Expr {
    return expr_mod.call(arena, "regexp_substr", &.{ s, pattern });
}
pub fn bitLength(arena: Allocator, s: Expr) !Expr {
    return expr_mod.call(arena, "bit_length", &.{s});
}
pub fn ord(arena: Allocator, s: Expr) !Expr {
    return expr_mod.call(arena, "ord", &.{s});
}
pub fn field(arena: Allocator, args: []const Expr) !Expr {
    return expr_mod.call(arena, "field", args);
}
pub fn findInSet(arena: Allocator, needle: Expr, set: Expr) !Expr {
    return expr_mod.call(arena, "find_in_set", &.{ needle, set });
}
pub fn initcap(arena: Allocator, s: Expr) !Expr {
    return expr_mod.call(arena, "initcap", &.{s});
}
pub fn translate(arena: Allocator, s: Expr, from: Expr, to: Expr) !Expr {
    return expr_mod.call(arena, "translate", &.{ s, from, to });
}

// --- math ---
pub fn abs(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "abs", &.{arg});
}
pub fn ceil(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "ceil", &.{arg});
}
pub fn floor(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "floor", &.{arg});
}
pub fn round(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "round", &.{arg});
}
pub fn roundScale(arena: Allocator, x: Expr, scale: Expr) !Expr {
    return expr_mod.call(arena, "round", &.{ x, scale });
}
pub fn sign(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "sign", &.{arg});
}
pub fn mod(arena: Allocator, a: Expr, b: Expr) !Expr {
    return expr_mod.call(arena, "mod", &.{ a, b });
}
pub fn pmod(arena: Allocator, a: Expr, b: Expr) !Expr {
    return expr_mod.call(arena, "pmod", &.{ a, b });
}
pub fn fmod(arena: Allocator, a: Expr, b: Expr) !Expr {
    return expr_mod.call(arena, "fmod", &.{ a, b });
}
pub fn pow(arena: Allocator, a: Expr, b: Expr) !Expr {
    return expr_mod.call(arena, "pow", &.{ a, b });
}
pub fn sqrt(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "sqrt", &.{arg});
}
pub fn exp(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "exp", &.{arg});
}
pub fn ln(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "ln", &.{arg});
}
pub fn log10(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "log10", &.{arg});
}
pub fn log2(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "log2", &.{arg});
}
pub fn greatest(arena: Allocator, a: Expr, b: Expr) !Expr {
    return expr_mod.call(arena, "greatest", &.{ a, b });
}
pub fn least(arena: Allocator, a: Expr, b: Expr) !Expr {
    return expr_mod.call(arena, "least", &.{ a, b });
}
pub fn pi(arena: Allocator) !Expr {
    return expr_mod.call(arena, "pi", &.{});
}
pub fn rand(arena: Allocator) !Expr {
    return expr_mod.call(arena, "rand", &.{});
}
pub fn random(arena: Allocator) !Expr {
    return expr_mod.call(arena, "random", &.{});
}
pub fn log(arena: Allocator, base: Expr, x: Expr) !Expr {
    return expr_mod.call(arena, "log", &.{ base, x });
}
pub fn sin(arena: Allocator, x: Expr) !Expr {
    return expr_mod.call(arena, "sin", &.{x});
}
pub fn cos(arena: Allocator, x: Expr) !Expr {
    return expr_mod.call(arena, "cos", &.{x});
}
pub fn tan(arena: Allocator, x: Expr) !Expr {
    return expr_mod.call(arena, "tan", &.{x});
}
pub fn asin(arena: Allocator, x: Expr) !Expr {
    return expr_mod.call(arena, "asin", &.{x});
}
pub fn acos(arena: Allocator, x: Expr) !Expr {
    return expr_mod.call(arena, "acos", &.{x});
}
pub fn atan(arena: Allocator, x: Expr) !Expr {
    return expr_mod.call(arena, "atan", &.{x});
}
pub fn cot(arena: Allocator, x: Expr) !Expr {
    return expr_mod.call(arena, "cot", &.{x});
}
pub fn cbrt(arena: Allocator, x: Expr) !Expr {
    return expr_mod.call(arena, "cbrt", &.{x});
}
pub fn square(arena: Allocator, x: Expr) !Expr {
    return expr_mod.call(arena, "square", &.{x});
}
pub fn bitCount(arena: Allocator, x: Expr) !Expr {
    return expr_mod.call(arena, "bit_count", &.{x});
}
pub fn bin(arena: Allocator, x: Expr) !Expr {
    return expr_mod.call(arena, "bin", &.{x});
}
pub fn conv(arena: Allocator, x: Expr, from_base: Expr, to_base: Expr) !Expr {
    return expr_mod.call(arena, "conv", &.{ x, from_base, to_base });
}

// --- conditional ---
pub fn ifnull(arena: Allocator, a: Expr, b: Expr) !Expr {
    return expr_mod.call(arena, "ifnull", &.{ a, b });
}
pub fn ifThenElse(arena: Allocator, condition: Expr, then_expr: Expr, else_expr: Expr) !Expr {
    return expr_mod.call(arena, "if", &.{ condition, then_expr, else_expr });
}
pub fn nullif(arena: Allocator, a: Expr, b: Expr) !Expr {
    return expr_mod.call(arena, "nullif", &.{ a, b });
}

// --- date/time ---
pub fn year(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "year", &.{arg});
}
pub fn month(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "month", &.{arg});
}
pub fn day(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "day", &.{arg});
}
pub fn hour(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "hour", &.{arg});
}
pub fn minute(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "minute", &.{arg});
}
pub fn second(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "second", &.{arg});
}
pub fn datediff(arena: Allocator, a: Expr, b: Expr) !Expr {
    return expr_mod.call(arena, "datediff", &.{ a, b });
}
pub fn dateAdd(arena: Allocator, d: Expr, n: Expr) !Expr {
    return expr_mod.call(arena, "date_add", &.{ d, n });
}
pub fn dateSub(arena: Allocator, d: Expr, n: Expr) !Expr {
    return expr_mod.call(arena, "date_sub", &.{ d, n });
}
pub fn makedate(arena: Allocator, year_expr: Expr, day_of_year: Expr) !Expr {
    return expr_mod.call(arena, "makedate", &.{ year_expr, day_of_year });
}
pub fn unixTimestamp(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "unix_timestamp", &.{arg});
}
pub fn fromUnixtime(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "from_unixtime", &.{arg});
}

// --- conversion ---
pub fn toInt(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "to_int", &.{arg});
}
pub fn toBigint(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "to_bigint", &.{arg});
}
pub fn toDouble(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "to_double", &.{arg});
}
pub fn toString(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "to_string", &.{arg});
}

// --- hash ---
pub fn md5(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "md5", &.{arg});
}
pub fn sha1(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "sha1", &.{arg});
}
pub fn sha256(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "sha256", &.{arg});
}
pub fn crc32(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "crc32", &.{arg});
}
pub fn sha2(arena: Allocator, arg: Expr, bits: Expr) !Expr {
    return expr_mod.call(arena, "sha2", &.{ arg, bits });
}
pub fn md5sum(arena: Allocator, args: []const Expr) !Expr {
    return expr_mod.call(arena, "md5sum", args);
}
pub fn murmurHash3_32(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "murmur_hash3_32", &.{arg});
}
pub fn xxHash3_64(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "xx_hash3_64", &.{arg});
}
pub fn xxHash3_128(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "xx_hash3_128", &.{arg});
}

// --- encoding ---
pub fn hex(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "hex", &.{arg});
}
pub fn unhex(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "unhex", &.{arg});
}
pub fn toBase64(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "to_base64", &.{arg});
}
pub fn fromBase64(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "from_base64", &.{arg});
}

// --- expanded string ---
pub fn lpad(arena: Allocator, s: Expr, n: Expr, pad: Expr) !Expr {
    return expr_mod.call(arena, "lpad", &.{ s, n, pad });
}
pub fn rpad(arena: Allocator, s: Expr, n: Expr, pad: Expr) !Expr {
    return expr_mod.call(arena, "rpad", &.{ s, n, pad });
}
pub fn repeat(arena: Allocator, s: Expr, n: Expr) !Expr {
    return expr_mod.call(arena, "repeat", &.{ s, n });
}
pub fn space(arena: Allocator, n: Expr) !Expr {
    return expr_mod.call(arena, "space", &.{n});
}
pub fn ascii(arena: Allocator, s: Expr) !Expr {
    return expr_mod.call(arena, "ascii", &.{s});
}
pub fn position(arena: Allocator, needle: Expr, hay: Expr) !Expr {
    return expr_mod.call(arena, "position", &.{ needle, hay });
}
pub fn instr(arena: Allocator, hay: Expr, needle: Expr) !Expr {
    return expr_mod.call(arena, "instr", &.{ hay, needle });
}
pub fn substringIndex(arena: Allocator, s: Expr, delim: Expr, count: Expr) !Expr {
    return expr_mod.call(arena, "substring_index", &.{ s, delim, count });
}
pub fn strcmp(arena: Allocator, a: Expr, b: Expr) !Expr {
    return expr_mod.call(arena, "strcmp", &.{ a, b });
}

// --- expanded math ---
pub fn truncate(arena: Allocator, x: Expr, d: Expr) !Expr {
    return expr_mod.call(arena, "truncate", &.{ x, d });
}
pub fn degrees(arena: Allocator, x: Expr) !Expr {
    return expr_mod.call(arena, "degrees", &.{x});
}
pub fn radians(arena: Allocator, x: Expr) !Expr {
    return expr_mod.call(arena, "radians", &.{x});
}
pub fn atan2(arena: Allocator, y: Expr, x: Expr) !Expr {
    return expr_mod.call(arena, "atan2", &.{ y, x });
}

// --- expanded date ---
pub fn dayofweek(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "dayofweek", &.{arg});
}
pub fn dayofyear(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "dayofyear", &.{arg});
}
pub fn quarter(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "quarter", &.{arg});
}
pub fn lastDay(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "last_day", &.{arg});
}
pub fn dateFormat(arena: Allocator, dt: Expr, fmt: Expr) !Expr {
    return expr_mod.call(arena, "date_format", &.{ dt, fmt });
}
pub fn dayofmonth(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "dayofmonth", &.{arg});
}
pub fn dayname(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "dayname", &.{arg});
}
pub fn monthname(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "monthname", &.{arg});
}
/// `later - earlier` in whole units, as SQL's DATE_DIFF(unit, later, earlier).
pub fn dateDiffUnit(arena: Allocator, unit: Expr, later: Expr, earlier: Expr) !Expr {
    return expr_mod.call(arena, "date_diff", &.{ unit, later, earlier });
}
pub fn timestampDiff(arena: Allocator, unit: Expr, start: Expr, end: Expr) !Expr {
    return expr_mod.call(arena, "timestampdiff", &.{ unit, start, end });
}
pub fn timestampAdd(arena: Allocator, unit: Expr, n: Expr, value: Expr) !Expr {
    return expr_mod.call(arena, "timestampadd", &.{ unit, n, value });
}

// --- MySQL aliases / one-off additions ---
pub fn lcase(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "lower", &.{arg});
}
pub fn ucase(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "upper", &.{arg});
}
pub fn power(arena: Allocator, a: Expr, b: Expr) !Expr {
    return expr_mod.call(arena, "pow", &.{ a, b });
}
pub fn ceiling(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "ceil", &.{arg});
}
pub fn chr(arena: Allocator, arg: Expr) !Expr {
    return expr_mod.call(arena, "chr", &.{arg});
}

// Tests live in scalar_fn_test.zig (companion).
