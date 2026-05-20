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

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const TypeTag = types.TypeTag;
const Value = types.Value;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const store = @import("../engine/store.zig");
const ColumnStore = store.ColumnStore;

const Expr = @import("expr.zig").Expr;
const exec = @import("exec.zig");
const Error = exec.Error;

const cast = @import("cast.zig");
const CastKernel = cast.CastKernel;

// Kernel implementations are split into category files. Imported here for
// the builtins[] registry; the rest of the codebase only sees ScalarFn /
// resolve.
const string = @import("scalar_fn_string.zig");
const math = @import("scalar_fn_math.zig");
const date = @import("scalar_fn_date.zig");
const cond = @import("scalar_fn_cond.zig");

pub const NullStrategy = enum {
    /// Default: if any input row is null, output is null. The
    /// Compute operator pre-builds a combined null mask before
    /// invoking the kernel — kernels can assume non-null inputs.
    /// Compute writes the bitmap as AND of input validities.
    propagates,
    /// Kernel handles nulls itself. Compute writes the bitmap as OR
    /// of input validities (default: output non-null iff ANY input
    /// non-null — matches coalesce / ifnull semantics).
    absorbs,
    /// Kernel writes both data AND validity bits. Compute does no
    /// post-processing of the bitmap. Used by functions that can
    /// produce null from non-null inputs (e.g. nullif).
    kernel_managed,
};

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
    null_strategy: NullStrategy = .propagates,
    kernel: Kernel,
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
    // Fast path: exact TypeTag match. No allocation, no cost calc.
    for (builtins) |f| {
        if (!std.mem.eql(u8, f.name, name)) continue;
        if (f.arg_types.len != arg_types.len) continue;
        var all_match = true;
        for (f.arg_types, arg_types) |declared, given| {
            if (@as(TypeTag, declared) != @as(TypeTag, given)) {
                all_match = false;
                break;
            }
        }
        if (all_match) return ResolvedOverload{ .func = f, .arg_casts = null };
    }

    // Slow path: rank by cumulative cast cost. Iterate name-matching
    // overloads, compute cost, keep the cheapest.
    var best: ?ScalarFn = null;
    var best_cost: u64 = std.math.maxInt(u64);
    for (builtins) |f| {
        if (!std.mem.eql(u8, f.name, name)) continue;
        if (f.arg_types.len != arg_types.len) continue;
        var total_cost: u64 = 0;
        var castable = true;
        for (f.arg_types, arg_types) |declared, given| {
            const c = cast.castCost(@as(TypeTag, given), @as(TypeTag, declared)) orelse {
                castable = false;
                break;
            };
            total_cost += c;
        }
        if (!castable) continue;
        if (total_cost < best_cost) {
            best_cost = total_cost;
            best = f;
        }
    }

    const chosen = best orelse return null;
    // Build per-arg cast plan.
    const arg_casts = try aa.alloc(?CastKernel, chosen.arg_types.len);
    for (chosen.arg_types, arg_types, arg_casts) |declared, given, *slot| {
        const ft: TypeTag = @as(TypeTag, given);
        const tt: TypeTag = @as(TypeTag, declared);
        slot.* = if (ft == tt) null else cast.kernelFor(ft, tt);
    }
    return ResolvedOverload{ .func = chosen, .arg_casts = arg_casts };
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
        if (std.mem.eql(u8, f.name, name)) {
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
    .{ .name = "length", .arg_types = &.{.string}, .return_type = .int, .kernel = string.lengthKernel },
    .{ .name = "octet_length", .arg_types = &.{.string}, .return_type = .int, .kernel = string.lengthKernel },
    // char_length is byte-length for ASCII; UTF-8-aware counterpart
    // is a v2 follow-up (need codepoint iteration).
    .{ .name = "char_length", .arg_types = &.{.string}, .return_type = .int, .kernel = string.lengthKernel },
    // --- multi-arg string ---
    .{ .name = "concat", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = string.concat2Kernel },
    .{ .name = "concat", .arg_types = &.{ .string, .string, .string }, .return_type = .string, .kernel = string.concat3Kernel },
    .{ .name = "substring", .arg_types = &.{ .string, .int, .int }, .return_type = .string, .kernel = string.substringKernel },
    .{ .name = "replace", .arg_types = &.{ .string, .string, .string }, .return_type = .string, .kernel = string.replaceKernel },
    // --- coalesce overloads ---
    .{ .name = "coalesce", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .absorbs, .kernel = cond.coalesceStringKernel },
    .{ .name = "coalesce", .arg_types = &.{ .int, .int }, .return_type = .int, .null_strategy = .absorbs, .kernel = cond.coalesceIntKernel },
    .{ .name = "coalesce", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .null_strategy = .absorbs, .kernel = cond.coalesceBigintKernel },
    .{ .name = "coalesce", .arg_types = &.{ .double, .double }, .return_type = .double, .null_strategy = .absorbs, .kernel = cond.coalesceDoubleKernel },
    // --- math ---
    .{ .name = "abs", .arg_types = &.{.int}, .return_type = .int, .kernel = math.absIntKernel },
    .{ .name = "abs", .arg_types = &.{.bigint}, .return_type = .bigint, .kernel = math.absBigintKernel },
    .{ .name = "abs", .arg_types = &.{.double}, .return_type = .double, .kernel = math.absDoubleKernel },
    .{ .name = "ceil", .arg_types = &.{.double}, .return_type = .double, .kernel = math.ceilKernel },
    .{ .name = "floor", .arg_types = &.{.double}, .return_type = .double, .kernel = math.floorKernel },
    .{ .name = "round", .arg_types = &.{.double}, .return_type = .double, .kernel = math.roundKernel },
    .{ .name = "sign", .arg_types = &.{.double}, .return_type = .int, .kernel = math.signKernel },
    .{ .name = "mod", .arg_types = &.{ .int, .int }, .return_type = .int, .kernel = math.modIntKernel },
    .{ .name = "mod", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = math.modBigintKernel },
    // Binary arithmetic — backs the SQL infix operators (+ - * /) in the
    // parser. Overloaded on (int,int), (bigint,bigint), (double,double);
    // mixed-type combinations route through the existing scalar-fn
    // coercion machinery (int→bigint→double promotion).
    .{ .name = "add", .arg_types = &.{ .int, .int }, .return_type = .int, .kernel = math.addIntKernel },
    .{ .name = "add", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = math.addBigintKernel },
    .{ .name = "add", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.addDoubleKernel },
    .{ .name = "sub", .arg_types = &.{ .int, .int }, .return_type = .int, .kernel = math.subIntKernel },
    .{ .name = "sub", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = math.subBigintKernel },
    .{ .name = "sub", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.subDoubleKernel },
    .{ .name = "mul", .arg_types = &.{ .int, .int }, .return_type = .int, .kernel = math.mulIntKernel },
    .{ .name = "mul", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = math.mulBigintKernel },
    .{ .name = "mul", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.mulDoubleKernel },
    .{ .name = "div", .arg_types = &.{ .int, .int }, .return_type = .int, .kernel = math.divIntKernel },
    .{ .name = "div", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = math.divBigintKernel },
    .{ .name = "div", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.divDoubleKernel },
    .{ .name = "pow", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.powKernel },
    .{ .name = "sqrt", .arg_types = &.{.double}, .return_type = .double, .kernel = math.sqrtKernel },
    .{ .name = "exp", .arg_types = &.{.double}, .return_type = .double, .kernel = math.expKernel },
    .{ .name = "ln", .arg_types = &.{.double}, .return_type = .double, .kernel = math.lnKernel },
    .{ .name = "log10", .arg_types = &.{.double}, .return_type = .double, .kernel = math.log10Kernel },
    .{ .name = "log2", .arg_types = &.{.double}, .return_type = .double, .kernel = math.log2Kernel },
    .{ .name = "greatest", .arg_types = &.{ .int, .int }, .return_type = .int, .kernel = math.greatestIntKernel },
    .{ .name = "greatest", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = math.greatestBigintKernel },
    .{ .name = "greatest", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.greatestDoubleKernel },
    .{ .name = "greatest", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = string.greatestStringKernel },
    .{ .name = "least", .arg_types = &.{ .int, .int }, .return_type = .int, .kernel = math.leastIntKernel },
    .{ .name = "least", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = math.leastBigintKernel },
    .{ .name = "least", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.leastDoubleKernel },
    .{ .name = "least", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = string.leastStringKernel },
    // --- math (expanded) ---
    .{ .name = "truncate", .arg_types = &.{ .double, .int }, .return_type = .double, .kernel = math.truncateKernel },
    .{ .name = "degrees", .arg_types = &.{.double}, .return_type = .double, .kernel = math.degreesKernel },
    .{ .name = "radians", .arg_types = &.{.double}, .return_type = .double, .kernel = math.radiansKernel },
    .{ .name = "atan2", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.atan2Kernel },
    // --- conditional ---
    .{ .name = "ifnull", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .absorbs, .kernel = cond.ifnullStringKernel },
    .{ .name = "ifnull", .arg_types = &.{ .int, .int }, .return_type = .int, .null_strategy = .absorbs, .kernel = cond.ifnullIntKernel },
    .{ .name = "ifnull", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .null_strategy = .absorbs, .kernel = cond.ifnullBigintKernel },
    .{ .name = "ifnull", .arg_types = &.{ .double, .double }, .return_type = .double, .null_strategy = .absorbs, .kernel = cond.ifnullDoubleKernel },
    .{ .name = "nullif", .arg_types = &.{ .int, .int }, .return_type = .int, .null_strategy = .kernel_managed, .kernel = cond.nullifIntKernel },
    .{ .name = "nullif", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = cond.nullifBigintKernel },
    .{ .name = "nullif", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = cond.nullifStringKernel },
    // --- date/time component extractors ---
    // NB: now() / current_date() are deferred — Zig 0.16's std.Io.Clock
    // needs an Io instance; kernel signature doesn't carry one yet.
    .{ .name = "year", .arg_types = &.{.date}, .return_type = .int, .kernel = date.yearFromDateKernel },
    .{ .name = "year", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.yearFromDatetimeKernel },
    .{ .name = "month", .arg_types = &.{.date}, .return_type = .int, .kernel = date.monthFromDateKernel },
    .{ .name = "month", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.monthFromDatetimeKernel },
    .{ .name = "day", .arg_types = &.{.date}, .return_type = .int, .kernel = date.dayFromDateKernel },
    .{ .name = "day", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.dayFromDatetimeKernel },
    .{ .name = "hour", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.hourKernel },
    .{ .name = "minute", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.minuteKernel },
    .{ .name = "second", .arg_types = &.{.datetime}, .return_type = .int, .kernel = date.secondKernel },
    // --- date arithmetic + epoch conversion ---
    .{ .name = "datediff", .arg_types = &.{ .date, .date }, .return_type = .int, .kernel = date.datediffKernel },
    .{ .name = "date_add", .arg_types = &.{ .date, .int }, .return_type = .date, .kernel = date.dateAddKernel },
    .{ .name = "date_sub", .arg_types = &.{ .date, .int }, .return_type = .date, .kernel = date.dateSubKernel },
    .{ .name = "unix_timestamp", .arg_types = &.{.datetime}, .return_type = .bigint, .kernel = date.unixTimestampKernel },
    .{ .name = "from_unixtime", .arg_types = &.{.bigint}, .return_type = .datetime, .kernel = date.fromUnixtimeKernel },
    // --- date (expanded MySQL-style helpers) ---
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
    .{ .name = "to_bigint", .arg_types = &.{.int}, .return_type = .bigint, .kernel = math.intToBigintKernel },
    .{ .name = "to_double", .arg_types = &.{.int}, .return_type = .double, .kernel = math.intToDoubleKernel },
    .{ .name = "to_double", .arg_types = &.{.bigint}, .return_type = .double, .kernel = math.bigintToDoubleKernel },
    // Numeric narrowing (truncates, may lose precision; overflow saturates).
    .{ .name = "to_int", .arg_types = &.{.bigint}, .return_type = .int, .kernel = math.bigintToIntKernel },
    .{ .name = "to_int", .arg_types = &.{.double}, .return_type = .int, .kernel = math.doubleToIntKernel },
    .{ .name = "to_bigint", .arg_types = &.{.double}, .return_type = .bigint, .kernel = math.doubleToBigintKernel },
    // String parsing — on parse failure returns 0 (NULL-on-failure
    // would need .kernel_managed; deferred).
    .{ .name = "to_int", .arg_types = &.{.string}, .return_type = .int, .kernel = math.stringToIntKernel },
    .{ .name = "to_bigint", .arg_types = &.{.string}, .return_type = .bigint, .kernel = math.stringToBigintKernel },
    .{ .name = "to_double", .arg_types = &.{.string}, .return_type = .double, .kernel = math.stringToDoubleKernel },
    // Stringify numerics.
    .{ .name = "to_string", .arg_types = &.{.int}, .return_type = .string, .kernel = math.intToStringKernel },
    .{ .name = "to_string", .arg_types = &.{.bigint}, .return_type = .string, .kernel = math.bigintToStringKernel },
    .{ .name = "to_string", .arg_types = &.{.double}, .return_type = .string, .kernel = math.doubleToStringKernel },
    .{ .name = "to_string", .arg_types = &.{.boolean}, .return_type = .string, .kernel = math.boolToStringKernel },
    // --- hash ---
    .{ .name = "md5", .arg_types = &.{.string}, .return_type = .string, .kernel = string.md5Kernel },
    .{ .name = "sha1", .arg_types = &.{.string}, .return_type = .string, .kernel = string.sha1Kernel },
    .{ .name = "sha256", .arg_types = &.{.string}, .return_type = .string, .kernel = string.sha256Kernel },
    .{ .name = "crc32", .arg_types = &.{.string}, .return_type = .bigint, .kernel = string.crc32Kernel },
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
    .{ .name = "position", .arg_types = &.{ .string, .string }, .return_type = .int, .kernel = string.positionKernel },
    .{ .name = "instr", .arg_types = &.{ .string, .string }, .return_type = .int, .kernel = string.instrKernel },
    .{ .name = "substring_index", .arg_types = &.{ .string, .string, .int }, .return_type = .string, .kernel = string.substringIndexKernel },
    .{ .name = "strcmp", .arg_types = &.{ .string, .string }, .return_type = .int, .kernel = string.strcmpKernel },
    // --- MySQL aliases over existing kernels (zero new code) ---
    .{ .name = "lcase", .arg_types = &.{.string}, .return_type = .string, .kernel = string.lowerKernel },
    .{ .name = "ucase", .arg_types = &.{.string}, .return_type = .string, .kernel = string.upperKernel },
    .{ .name = "power", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = math.powKernel },
    .{ .name = "ceiling", .arg_types = &.{.double}, .return_type = .double, .kernel = math.ceilKernel },
    .{ .name = "chr", .arg_types = &.{.int}, .return_type = .string, .kernel = string.chrKernel },
};

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
pub fn upper(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "upper", &.{arg}); }
pub fn lower(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "lower", &.{arg}); }
pub fn length(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "length", &.{arg}); }
pub fn coalesce(arena: Allocator, a: Expr, b: Expr) !Expr { return expr_mod.call(arena, "coalesce", &.{ a, b }); }
pub fn ltrim(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "ltrim", &.{arg}); }
pub fn rtrim(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "rtrim", &.{arg}); }
pub fn trim(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "trim", &.{arg}); }
pub fn reverse(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "reverse", &.{arg}); }
pub fn octetLength(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "octet_length", &.{arg}); }
pub fn charLength(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "char_length", &.{arg}); }
pub fn concat(arena: Allocator, args: []const Expr) !Expr { return expr_mod.call(arena, "concat", args); }
pub fn substring(arena: Allocator, s: Expr, start: Expr, length_arg: Expr) !Expr { return expr_mod.call(arena, "substring", &.{ s, start, length_arg }); }
pub fn replace(arena: Allocator, haystack: Expr, needle: Expr, repl: Expr) !Expr { return expr_mod.call(arena, "replace", &.{ haystack, needle, repl }); }

// --- math ---
pub fn abs(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "abs", &.{arg}); }
pub fn ceil(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "ceil", &.{arg}); }
pub fn floor(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "floor", &.{arg}); }
pub fn round(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "round", &.{arg}); }
pub fn sign(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "sign", &.{arg}); }
pub fn mod(arena: Allocator, a: Expr, b: Expr) !Expr { return expr_mod.call(arena, "mod", &.{ a, b }); }
pub fn pow(arena: Allocator, a: Expr, b: Expr) !Expr { return expr_mod.call(arena, "pow", &.{ a, b }); }
pub fn sqrt(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "sqrt", &.{arg}); }
pub fn exp(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "exp", &.{arg}); }
pub fn ln(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "ln", &.{arg}); }
pub fn log10(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "log10", &.{arg}); }
pub fn log2(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "log2", &.{arg}); }
pub fn greatest(arena: Allocator, a: Expr, b: Expr) !Expr { return expr_mod.call(arena, "greatest", &.{ a, b }); }
pub fn least(arena: Allocator, a: Expr, b: Expr) !Expr { return expr_mod.call(arena, "least", &.{ a, b }); }

// --- conditional ---
pub fn ifnull(arena: Allocator, a: Expr, b: Expr) !Expr { return expr_mod.call(arena, "ifnull", &.{ a, b }); }
pub fn nullif(arena: Allocator, a: Expr, b: Expr) !Expr { return expr_mod.call(arena, "nullif", &.{ a, b }); }

// --- date/time ---
pub fn year(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "year", &.{arg}); }
pub fn month(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "month", &.{arg}); }
pub fn day(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "day", &.{arg}); }
pub fn hour(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "hour", &.{arg}); }
pub fn minute(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "minute", &.{arg}); }
pub fn second(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "second", &.{arg}); }
pub fn datediff(arena: Allocator, a: Expr, b: Expr) !Expr { return expr_mod.call(arena, "datediff", &.{ a, b }); }
pub fn dateAdd(arena: Allocator, d: Expr, n: Expr) !Expr { return expr_mod.call(arena, "date_add", &.{ d, n }); }
pub fn dateSub(arena: Allocator, d: Expr, n: Expr) !Expr { return expr_mod.call(arena, "date_sub", &.{ d, n }); }
pub fn unixTimestamp(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "unix_timestamp", &.{arg}); }
pub fn fromUnixtime(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "from_unixtime", &.{arg}); }

// --- conversion ---
pub fn toInt(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "to_int", &.{arg}); }
pub fn toBigint(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "to_bigint", &.{arg}); }
pub fn toDouble(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "to_double", &.{arg}); }
pub fn toString(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "to_string", &.{arg}); }

// --- hash ---
pub fn md5(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "md5", &.{arg}); }
pub fn sha1(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "sha1", &.{arg}); }
pub fn sha256(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "sha256", &.{arg}); }
pub fn crc32(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "crc32", &.{arg}); }

// --- encoding ---
pub fn hex(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "hex", &.{arg}); }
pub fn unhex(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "unhex", &.{arg}); }
pub fn toBase64(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "to_base64", &.{arg}); }
pub fn fromBase64(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "from_base64", &.{arg}); }

// --- expanded string ---
pub fn lpad(arena: Allocator, s: Expr, n: Expr, pad: Expr) !Expr { return expr_mod.call(arena, "lpad", &.{ s, n, pad }); }
pub fn rpad(arena: Allocator, s: Expr, n: Expr, pad: Expr) !Expr { return expr_mod.call(arena, "rpad", &.{ s, n, pad }); }
pub fn repeat(arena: Allocator, s: Expr, n: Expr) !Expr { return expr_mod.call(arena, "repeat", &.{ s, n }); }
pub fn space(arena: Allocator, n: Expr) !Expr { return expr_mod.call(arena, "space", &.{n}); }
pub fn ascii(arena: Allocator, s: Expr) !Expr { return expr_mod.call(arena, "ascii", &.{s}); }
pub fn position(arena: Allocator, needle: Expr, hay: Expr) !Expr { return expr_mod.call(arena, "position", &.{ needle, hay }); }
pub fn instr(arena: Allocator, hay: Expr, needle: Expr) !Expr { return expr_mod.call(arena, "instr", &.{ hay, needle }); }
pub fn substringIndex(arena: Allocator, s: Expr, delim: Expr, count: Expr) !Expr { return expr_mod.call(arena, "substring_index", &.{ s, delim, count }); }
pub fn strcmp(arena: Allocator, a: Expr, b: Expr) !Expr { return expr_mod.call(arena, "strcmp", &.{ a, b }); }

// --- expanded math ---
pub fn truncate(arena: Allocator, x: Expr, d: Expr) !Expr { return expr_mod.call(arena, "truncate", &.{ x, d }); }
pub fn degrees(arena: Allocator, x: Expr) !Expr { return expr_mod.call(arena, "degrees", &.{x}); }
pub fn radians(arena: Allocator, x: Expr) !Expr { return expr_mod.call(arena, "radians", &.{x}); }
pub fn atan2(arena: Allocator, y: Expr, x: Expr) !Expr { return expr_mod.call(arena, "atan2", &.{ y, x }); }

// --- expanded date ---
pub fn dayofweek(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "dayofweek", &.{arg}); }
pub fn dayofyear(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "dayofyear", &.{arg}); }
pub fn quarter(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "quarter", &.{arg}); }
pub fn lastDay(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "last_day", &.{arg}); }
pub fn dateFormat(arena: Allocator, dt: Expr, fmt: Expr) !Expr { return expr_mod.call(arena, "date_format", &.{ dt, fmt }); }

// --- MySQL aliases / one-off additions ---
pub fn lcase(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "lcase", &.{arg}); }
pub fn ucase(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "ucase", &.{arg}); }
pub fn power(arena: Allocator, a: Expr, b: Expr) !Expr { return expr_mod.call(arena, "power", &.{ a, b }); }
pub fn ceiling(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "ceiling", &.{arg}); }
pub fn chr(arena: Allocator, arg: Expr) !Expr { return expr_mod.call(arena, "chr", &.{arg}); }

// Tests live in scalar_fn_test.zig (companion).
