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
const cond = @import("scalar_fn_cond.zig");
const dec = @import("scalar_fn_decimal.zig");
const json = @import("scalar_fn_json.zig");
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
    name: []const u8,
    arg_types: []const Type,
) !?ResolvedOverload {
    // Decimal-involving calls resolve to scale-aware typed kernels (the static
    // builtins table can't express a dynamic output scale). Checked first so a
    // decimal operand never falls into an int/double overload that ignores scale.
    if (try resolveDecimal(aa, name, arg_types)) |ov| return ov;
    if (try resolveIntArith(aa, name, arg_types)) |ov| return ov;
    if (try resolveSingleRow(aa, name, arg_types)) |ov| return ov;
    if (try resolveTextKey(aa, name, arg_types)) |ov| return ov;

    // Fast path: exact TypeTag match. No allocation, no cost calc.
    for (builtins) |f| {
        if (!std.ascii.eqlIgnoreCase(f.name, name)) continue;
        if (!scalarArityMatches(f, arg_types.len)) continue;
        var all_match = true;
        for (arg_types, 0..) |given, i| {
            if (canonScalarTag(@as(TypeTag, scalarDeclaredTypeAt(f, i))) != canonScalarTag(@as(TypeTag, given))) {
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
        return null;
    }

    // ROUND/TRUNCATE(decimal, n) — keeps the source scale, rounds the value.
    if (arg_types.len == 2 and arg_types[0].isDecimal() and arg_types[1].isInteger()) {
        if (std.ascii.eqlIgnoreCase(name, "round"))
            return try buildDecFn(aa, name, arg_types, arg_types[0], dec.roundNKernel, .propagates);
        if (std.ascii.eqlIgnoreCase(name, "truncate"))
            return try buildDecFn(aa, name, arg_types, arg_types[0], dec.truncateNKernel, .propagates);
    }

    // COALESCE / IFNULL — first non-null, all operands decimal/int.
    if ((std.ascii.eqlIgnoreCase(name, "coalesce") or std.ascii.eqlIgnoreCase(name, "ifnull")) and allDecimalOrInt(arg_types)) {
        const spec = dec.commonSpec(arg_types) orelse return null;
        return try buildDecFn(aa, name, arg_types, dec.decTypeFor(spec.p, spec.s), dec.coalesceKernel, .absorbs);
    }

    if (std.ascii.eqlIgnoreCase(name, "nullif") and arg_types.len == 2 and allDecimalOrInt(arg_types)) {
        const spec = dec.commonSpec(arg_types) orelse return null;
        return try buildDecFn(aa, name, arg_types, dec.decTypeFor(spec.p, spec.s), dec.nullifKernel, .kernel_managed);
    }

    if (std.ascii.eqlIgnoreCase(name, "if") and arg_types.len == 3 and arg_types[0] == .boolean and allDecimalOrInt(arg_types[1..])) {
        const spec = dec.commonSpec(arg_types[1..]) orelse return null;
        return try buildDecFn(aa, name, arg_types, dec.decTypeFor(spec.p, spec.s), dec.ifKernel, .kernel_managed);
    }

    if ((std.ascii.eqlIgnoreCase(name, "greatest") or std.ascii.eqlIgnoreCase(name, "least")) and allDecimalOrInt(arg_types)) {
        const spec = dec.commonSpec(arg_types) orelse return null;
        const k = if (std.ascii.eqlIgnoreCase(name, "greatest")) dec.greatestKernel else dec.leastKernel;
        return try buildDecFn(aa, name, arg_types, dec.decTypeFor(spec.p, spec.s), k, .propagates);
    }

    return null;
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
/// INT + INT → BIGINT. Every other call keeps the parser's literal.
pub fn arithOperandLiteral(name: []const u8, arg_types: []const Type, lit: types.Value) types.Value {
    const op = intArithOp(name) orelse return lit;
    if (arg_types.len != 2 or intArithResultType(op, arg_types[0], arg_types[1]) == null) return lit;
    const x: i128 = switch (lit) {
        .tinyint => |x| x,
        .smallint => |x| x,
        .int => |x| x,
        .bigint => |x| x,
        .largeint => |x| x,
        else => return lit,
    };
    if (std.math.cast(i8, x)) |n| return .{ .tinyint = n };
    if (std.math.cast(i16, x)) |n| return .{ .smallint = n };
    if (std.math.cast(i32, x)) |n| return .{ .int = n };
    if (std.math.cast(i64, x)) |n| return .{ .bigint = n };
    return .{ .largeint = x };
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

/// Whether ANY overload named `name` exists — builtin, decimal-only, or a
/// registered UDF. Name-only, so it holds before argument types are known.
pub fn nameResolvable(registry: ?*const udf_mod.UdfRegistry, name: []const u8) bool {
    if (std.mem.startsWith(u8, name, "to_decimal")) return true;
    if (std.mem.eql(u8, name, SINGLE_ROW_FN)) return true;
    if (std.mem.startsWith(u8, name, TEXT_KEY_PREFIX)) return true;
    if (std.ascii.eqlIgnoreCase(name, "to_float")) return true;
    if (intArithOp(name) != null) return true;
    for (builtins) |f| if (std.ascii.eqlIgnoreCase(f.name, name)) return true;
    if (registry) |reg| {
        for (reg.scalarEntries()) |entry| if (std.ascii.eqlIgnoreCase(entry.name, name)) return true;
    }
    return false;
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
    if (!(numericLike(src) or src.isString())) return null;
    return try buildDecFn(aa, name, arg_types, dec.decTypeFor(p, s), dec.toDecimalKernel, .kernel_managed);
}

fn scalarArityMatches(f: ScalarFn, actual: usize) bool {
    if (f.variadic_min_args) |min_args| return actual >= min_args and f.arg_types.len > 0;
    return f.arg_types.len == actual;
}

fn scalarDeclaredTypeAt(f: ScalarFn, i: usize) Type {
    return if (f.variadic_min_args != null) f.arg_types[i % f.arg_types.len] else f.arg_types[i];
}

/// `.varchar`/`.char` share the physical StringView representation of `.string`
/// (see `stringViewOf`), so for scalar-overload matching they are the same type.
/// Folding them lets a `VARCHAR(n)` column resolve string builtins with no cast.
fn canonScalarTag(t: TypeTag) TypeTag {
    return switch (t) {
        .varchar, .char, .json => .string,
        else => t,
    };
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

/// Which arguments to pass through `to_string` so `name` resolves: a
/// string parameter takes a number, a decimal or a date as its text, as in
/// StarRocks (`CONCAT('Q', quarter)`). The cheapest overload wins; null when
/// no overload resolves that way or no argument needs it.
pub fn stringifiedArgs(aa: Allocator, name: []const u8, arg_types: []const Type) !?[]const bool {
    var best: ?ScalarFn = null;
    var best_cost: u64 = std.math.maxInt(u64);
    for (builtins) |f| {
        if (!std.ascii.eqlIgnoreCase(f.name, name)) continue;
        if (!scalarArityMatches(f, arg_types.len)) continue;
        var total: u64 = 0;
        for (arg_types, 0..) |given, i| {
            total += try stringifyArgCost(aa, given, scalarDeclaredTypeAt(f, i)) orelse break;
        } else if (total < best_cost) {
            best_cost = total;
            best = f;
        }
    }
    const f = best orelse return null;
    const wrap = try aa.alloc(bool, arg_types.len);
    var any = false;
    for (arg_types, wrap, 0..) |given, *w, i| {
        w.* = scalarDeclaredTypeAt(f, i).isString() and !given.isString();
        any = any or w.*;
    }
    return if (any) wrap else null;
}

const STRINGIFY_COST: u64 = 1000;

fn stringifyArgCost(aa: Allocator, given: Type, declared: Type) !?u64 {
    if (declared.isString()) {
        if (given.isString()) return 0;
        return if (try resolve(aa, "to_string", &.{given}) != null) STRINGIFY_COST else null;
    }
    return argCastCost(given, declared, true) orelse return null;
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
    .{ .name = "reverse", .arg_types = &.{.string}, .return_type = .string, .kernel = string.reverseKernel },
    // --- string → int ---
    // length / char_length count UTF-8 characters (DuckDB/standard semantics);
    // octet_length counts raw bytes.
    .{ .name = "length", .arg_types = &.{.string}, .return_type = .int, .kernel = string.charLengthKernel },
    .{ .name = "octet_length", .arg_types = &.{.string}, .return_type = .int, .kernel = string.lengthKernel },
    .{ .name = "char_length", .arg_types = &.{.string}, .return_type = .int, .kernel = string.charLengthKernel },
    // --- multi-arg string ---
    .{ .name = "concat", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = string.concat2Kernel },
    .{ .name = "concat", .arg_types = &.{ .string, .string, .string }, .return_type = .string, .kernel = string.concat3Kernel },
    .{ .name = "concat", .arg_types = &.{.string}, .return_type = .string, .variadic_min_args = 4, .kernel = string.concatNKernel },
    .{ .name = "concat_ws", .arg_types = &.{.string}, .return_type = .string, .variadic_min_args = 2, .null_strategy = .kernel_managed, .kernel = string.concatWsKernel },
    .{ .name = "substring", .arg_types = &.{ .string, .int, .int }, .return_type = .string, .kernel = string.substringKernel },
    .{ .name = "substring", .arg_types = &.{ .string, .int }, .return_type = .string, .kernel = string.substringKernel },
    .{ .name = "left", .arg_types = &.{ .string, .int }, .return_type = .string, .kernel = string.leftKernel },
    .{ .name = "right", .arg_types = &.{ .string, .int }, .return_type = .string, .kernel = string.rightKernel },
    .{ .name = "replace", .arg_types = &.{ .string, .string, .string }, .return_type = .string, .kernel = string.replaceKernel },
    .{ .name = "regexp_replace", .arg_types = &.{ .string, .string, .string }, .return_type = .string, .kernel = string.regexpReplaceKernel },
    .{ .name = "regexp_like", .arg_types = &.{ .string, .string }, .return_type = .boolean, .kernel = string.regexpLikeKernel },
    .{ .name = "regexp_substr", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = string.regexpSubstrKernel },
    // --- json ---
    .{ .name = "json_extract", .arg_types = &.{ .string, .string }, .return_type = .json, .null_strategy = .kernel_managed, .kernel = json.jsonExtractKernel },
    .{ .name = "json_value", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = json.jsonValueKernel },
    .{ .name = "json_unquote", .arg_types = &.{.string}, .return_type = .string, .kernel = json.jsonUnquoteKernel },
    .{ .name = "json_valid", .arg_types = &.{.string}, .return_type = .boolean, .null_strategy = .kernel_managed, .kernel = json.jsonValidKernel },
    .{ .name = "json_type", .arg_types = &.{.string}, .return_type = .string, .null_strategy = .kernel_managed, .kernel = json.jsonTypeKernel },
    .{ .name = "json_length", .arg_types = &.{.string}, .return_type = .int, .null_strategy = .kernel_managed, .kernel = json.jsonLengthKernel },
    .{ .name = "json_contains", .arg_types = &.{ .string, .string }, .return_type = .boolean, .null_strategy = .kernel_managed, .kernel = json.jsonContainsKernel },
    .{ .name = "json_keys", .arg_types = &.{.string}, .return_type = .json, .null_strategy = .kernel_managed, .kernel = json.jsonKeysKernel },
    .{ .name = "to_json", .arg_types = &.{.string}, .return_type = .json, .null_strategy = .kernel_managed, .kernel = json.toJsonKernel },
    // --- coalesce overloads ---
    .{ .name = "coalesce", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .absorbs, .kernel = cond.coalesceStringKernel },
    .{ .name = "coalesce", .arg_types = &.{.string}, .return_type = .string, .variadic_min_args = 2, .null_strategy = .absorbs, .kernel = cond.coalesceStringKernel },
    .{ .name = "coalesce", .arg_types = &.{ .int, .int }, .return_type = .int, .null_strategy = .absorbs, .kernel = cond.coalesceIntKernel },
    .{ .name = "coalesce", .arg_types = &.{.int}, .return_type = .int, .variadic_min_args = 2, .null_strategy = .absorbs, .kernel = cond.coalesceIntKernel },
    .{ .name = "coalesce", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .null_strategy = .absorbs, .kernel = cond.coalesceBigintKernel },
    .{ .name = "coalesce", .arg_types = &.{.bigint}, .return_type = .bigint, .variadic_min_args = 2, .null_strategy = .absorbs, .kernel = cond.coalesceBigintKernel },
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
    .{ .name = "abs", .arg_types = &.{.double}, .return_type = .double, .kernel = math.absDoubleKernel },
    .{ .name = "ceil", .arg_types = &.{.double}, .return_type = .double, .kernel = math.ceilKernel },
    .{ .name = "floor", .arg_types = &.{.double}, .return_type = .double, .kernel = math.floorKernel },
    .{ .name = "round", .arg_types = &.{.double}, .return_type = .double, .kernel = math.roundKernel },
    .{ .name = "round", .arg_types = &.{ .double, .int }, .return_type = .double, .kernel = math.roundScaleKernel },
    .{ .name = "sign", .arg_types = &.{.double}, .return_type = .int, .kernel = math.signKernel },
    .{ .name = "pi", .arg_types = &.{}, .return_type = .double, .kernel = math.piKernel },
    .{ .name = "rand", .arg_types = &.{}, .return_type = .double, .volatility = .@"volatile", .kernel = math.randomKernel },
    .{ .name = "random", .arg_types = &.{}, .return_type = .double, .volatility = .@"volatile", .kernel = math.randomKernel },
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
    .{ .name = "greatest", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.greatestDoubleKernel },
    .{ .name = "greatest", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = string.greatestStringKernel },
    .{ .name = "greatest", .arg_types = &.{ .date, .date }, .return_type = .date, .kernel = math.greatestDateKernel },
    .{ .name = "greatest", .arg_types = &.{ .datetime, .datetime }, .return_type = .datetime, .kernel = math.greatestDatetimeKernel },
    .{ .name = "least", .arg_types = &.{ .int, .int }, .return_type = .int, .kernel = math.leastIntKernel },
    .{ .name = "least", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = math.leastBigintKernel },
    .{ .name = "least", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.leastDoubleKernel },
    .{ .name = "least", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = string.leastStringKernel },
    .{ .name = "least", .arg_types = &.{ .date, .date }, .return_type = .date, .kernel = math.leastDateKernel },
    .{ .name = "least", .arg_types = &.{ .datetime, .datetime }, .return_type = .datetime, .kernel = math.leastDatetimeKernel },
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
    .{ .name = "bit_count", .arg_types = &.{.int}, .return_type = .int, .kernel = math.bitCountIntKernel },
    .{ .name = "bit_count", .arg_types = &.{.bigint}, .return_type = .int, .kernel = math.bitCountBigintKernel },
    .{ .name = "bin", .arg_types = &.{.int}, .return_type = .string, .kernel = math.binIntKernel },
    .{ .name = "bin", .arg_types = &.{.bigint}, .return_type = .string, .kernel = math.binBigintKernel },
    .{ .name = "conv", .arg_types = &.{ .string, .int, .int }, .return_type = .string, .kernel = math.convStringKernel },
    .{ .name = "conv", .arg_types = &.{ .bigint, .int, .int }, .return_type = .string, .kernel = math.convBigintKernel },
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
    .{ .name = "ifnull", .arg_types = &.{ .double, .double }, .return_type = .double, .null_strategy = .absorbs, .kernel = cond.ifnullDoubleKernel },
    .{ .name = "ifnull", .arg_types = &.{ .boolean, .boolean }, .return_type = .boolean, .null_strategy = .absorbs, .kernel = cond.ifnullBooleanKernel },
    .{ .name = "ifnull", .arg_types = &.{ .date, .date }, .return_type = .date, .null_strategy = .absorbs, .kernel = cond.ifnullDateKernel },
    .{ .name = "ifnull", .arg_types = &.{ .datetime, .datetime }, .return_type = .datetime, .null_strategy = .absorbs, .kernel = cond.ifnullDatetimeKernel },
    .{ .name = "nullif", .arg_types = &.{ .int, .int }, .return_type = .int, .null_strategy = .kernel_managed, .kernel = cond.nullifIntKernel },
    .{ .name = "nullif", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = cond.nullifBigintKernel },
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
    .{ .name = "makedate", .arg_types = &.{ .int, .int }, .return_type = .date, .kernel = date.makedateKernel },
    .{ .name = "hour", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.hourKernel },
    .{ .name = "minute", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.minuteKernel },
    .{ .name = "second", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.secondKernel },
    // --- date arithmetic + epoch conversion ---
    .{ .name = "datediff", .arg_types = &.{ .date, .date }, .return_type = .int, .kernel = date.datediffKernel },
    .{ .name = "date_add", .arg_types = &.{ .date, .int }, .return_type = .date, .kernel = date.dateAddKernel },
    .{ .name = "date_sub", .arg_types = &.{ .date, .int }, .return_type = .date, .kernel = date.dateSubKernel },
    // Calendar-aware month/year addition; clamps day on short destination
    // months (`2024-01-31 + 1 month → 2024-02-29`). Used by the parser
    // when it lowers `date + INTERVAL '<N>' MONTH|YEAR`.
    .{ .name = "date_add_months", .arg_types = &.{ .date, .int }, .return_type = .date, .kernel = date.dateAddMonthsKernel },
    .{ .name = "date_add_years", .arg_types = &.{ .date, .int }, .return_type = .date, .kernel = date.dateAddYearsKernel },
    .{ .name = "unix_timestamp", .arg_types = &.{.datetime}, .return_type = .bigint, .kernel = date.unixTimestampKernel },
    .{ .name = "from_unixtime", .arg_types = &.{.bigint}, .return_type = .datetime, .kernel = date.fromUnixtimeKernel },
    .{ .name = "date_trunc", .arg_types = &.{ .string, .datetime }, .return_type = .datetime, .kernel = date.dateTruncKernel },
    .{ .name = "date_diff", .arg_types = &.{ .string, .date, .date }, .return_type = .int, .kernel = date.dateDiffDateKernel },
    .{ .name = "date_diff", .arg_types = &.{ .string, .datetime, .datetime }, .return_type = .int, .kernel = date.dateDiffDatetimeKernel },
    .{ .name = "timestampdiff", .arg_types = &.{ .string, .date, .date }, .return_type = .int, .kernel = date.dateDiffDateKernel },
    .{ .name = "timestampdiff", .arg_types = &.{ .string, .datetime, .datetime }, .return_type = .int, .kernel = date.dateDiffDatetimeKernel },
    .{ .name = "timestampadd", .arg_types = &.{ .string, .int, .date }, .return_type = .date, .kernel = date.timestampAddDateKernel },
    .{ .name = "timestampadd", .arg_types = &.{ .string, .int, .datetime }, .return_type = .datetime, .kernel = date.timestampAddDatetimeKernel },
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
    .{ .name = "date_format", .arg_types = &.{ .datetime, .string }, .return_type = .string, .kernel = date.dateFormatDatetimeKernel },
    .{ .name = "date_format", .arg_types = &.{ .date, .string }, .return_type = .string, .kernel = date.dateFormatDateKernel },
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
    .{ .name = "to_largeint", .arg_types = &.{.double}, .return_type = .largeint, .null_strategy = .kernel_managed, .kernel = math.doubleToLargeintKernel },
    .{ .name = "to_largeint", .arg_types = &.{.string}, .return_type = .largeint, .null_strategy = .kernel_managed, .kernel = math.stringToLargeintKernel },
    .{ .name = "to_double", .arg_types = &.{.string}, .return_type = .double, .null_strategy = .kernel_managed, .kernel = math.stringToDoubleKernel },
    .{ .name = "to_boolean", .arg_types = &.{.string}, .return_type = .boolean, .null_strategy = .kernel_managed, .kernel = math.stringToBoolKernel },
    // date <-> datetime
    .{ .name = "to_date", .arg_types = &.{.datetime}, .return_type = .date, .kernel = date.datetimeToDateKernel },
    .{ .name = "to_datetime", .arg_types = &.{.date}, .return_type = .datetime, .kernel = date.dateToDatetimeKernel },
    // Text parsing: text that isn't a date is NULL. Compute parses a literal
    // argument once at plan time instead (coerceTemporalStringLiterals).
    .{ .name = "to_date", .arg_types = &.{.string}, .return_type = .date, .null_strategy = .kernel_managed, .kernel = date.stringToDateKernel },
    .{ .name = "to_datetime", .arg_types = &.{.string}, .return_type = .datetime, .null_strategy = .kernel_managed, .kernel = date.stringToDatetimeKernel },
    // Stringify numerics.
    .{ .name = "to_string", .arg_types = &.{.int}, .return_type = .string, .kernel = math.intToStringKernel },
    .{ .name = "to_string", .arg_types = &.{.bigint}, .return_type = .string, .kernel = math.bigintToStringKernel },
    .{ .name = "to_string", .arg_types = &.{.double}, .return_type = .string, .kernel = math.doubleToStringKernel },
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
    .{ .name = "unhex", .arg_types = &.{.string}, .return_type = .string, .kernel = string.hexDecodeKernel },
    .{ .name = "to_base64", .arg_types = &.{.string}, .return_type = .string, .kernel = string.base64EncodeKernel },
    .{ .name = "from_base64", .arg_types = &.{.string}, .return_type = .string, .kernel = string.base64DecodeKernel },
    // --- string (expanded set; matches DuckDB / MySQL / StarRocks parity) ---
    .{ .name = "lpad", .arg_types = &.{ .string, .int, .string }, .return_type = .string, .kernel = string.lpadKernel },
    .{ .name = "rpad", .arg_types = &.{ .string, .int, .string }, .return_type = .string, .kernel = string.rpadKernel },
    .{ .name = "repeat", .arg_types = &.{ .string, .int }, .return_type = .string, .kernel = string.repeatKernel },
    .{ .name = "space", .arg_types = &.{.int}, .return_type = .string, .kernel = string.spaceKernel },
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
};

/// Other dialects' spellings of builtins. The parser rewrites a call to
/// its canonical name, so an alias carries every overload of its target
/// and every later name check sees one name.
const FUNCTION_ALIASES = [_]struct { alias: []const u8, name: []const u8 }{
    .{ .alias = "substr", .name = "substring" },
    .{ .alias = "mid", .name = "substring" },
    .{ .alias = "date", .name = "to_date" },
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
pub fn dateDiffUnit(arena: Allocator, unit: Expr, start: Expr, end: Expr) !Expr {
    return expr_mod.call(arena, "date_diff", &.{ unit, start, end });
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
