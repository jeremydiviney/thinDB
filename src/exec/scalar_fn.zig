//! Scalar function registry + first batch of builtin kernels.
//!
//! A `ScalarFn` is the runtime descriptor for one overload: name +
//! expected arg types + return type + kernel. The kernel is a generic
//! per-row evaluator. Multiple `ScalarFn` entries can share a name —
//! resolution picks the overload whose `arg_types` match the runtime
//! argument types (no implicit promotion yet; exact match required).
//!
//! Kernel contract:
//!   - Inputs are `[]const ColumnView` aligned to the function's args.
//!     Each view has `row_count` rows.
//!   - Output is a `*ColumnStore` of the function's declared
//!     `return_type`. The kernel appends exactly `row_count` rows.
//!   - Null propagation: by default, if ANY input row is null, the
//!     output row is null. The Compute operator handles the bookkeeping
//!     — kernels can assume all inputs are non-null (the Compute op
//!     pre-masks). `coalesce` is the exception; flagged with
//!     `null_strategy = .absorbs`.

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
// Builtins
// ---------------------------------------------------------------------------
//
// First batch — proves the infrastructure across the main type families
// (string-in/string-out, string-in/int-out, multi-arg, null-absorbing).
// More functions land in follow-up commits.

pub const builtins = [_]ScalarFn{
    // --- string → string ---
    .{ .name = "upper", .arg_types = &.{.string}, .return_type = .string, .kernel = upperKernel },
    .{ .name = "lower", .arg_types = &.{.string}, .return_type = .string, .kernel = lowerKernel },
    .{ .name = "ltrim", .arg_types = &.{.string}, .return_type = .string, .kernel = ltrimKernel },
    .{ .name = "rtrim", .arg_types = &.{.string}, .return_type = .string, .kernel = rtrimKernel },
    .{ .name = "trim", .arg_types = &.{.string}, .return_type = .string, .kernel = trimKernel },
    .{ .name = "reverse", .arg_types = &.{.string}, .return_type = .string, .kernel = reverseKernel },
    // --- string → int ---
    .{ .name = "length", .arg_types = &.{.string}, .return_type = .int, .kernel = lengthKernel },
    // octet_length is the SQL-standard byte-count alias for length.
    .{ .name = "octet_length", .arg_types = &.{.string}, .return_type = .int, .kernel = lengthKernel },
    // char_length is byte-length for ASCII; UTF-8-aware counterpart
    // is a v2 follow-up (need codepoint iteration).
    .{ .name = "char_length", .arg_types = &.{.string}, .return_type = .int, .kernel = lengthKernel },
    // --- multi-arg string ---
    .{ .name = "concat", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = concat2Kernel },
    .{ .name = "concat", .arg_types = &.{ .string, .string, .string }, .return_type = .string, .kernel = concat3Kernel },
    .{ .name = "substring", .arg_types = &.{ .string, .int, .int }, .return_type = .string, .kernel = substringKernel },
    .{ .name = "replace", .arg_types = &.{ .string, .string, .string }, .return_type = .string, .kernel = replaceKernel },
    // --- coalesce overloads ---
    .{ .name = "coalesce", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .absorbs, .kernel = coalesceStringKernel },
    .{ .name = "coalesce", .arg_types = &.{ .int, .int }, .return_type = .int, .null_strategy = .absorbs, .kernel = coalesceIntKernel },
    .{ .name = "coalesce", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .null_strategy = .absorbs, .kernel = coalesceBigintKernel },
    // --- math ---
    .{ .name = "abs", .arg_types = &.{.int}, .return_type = .int, .kernel = absIntKernel },
    .{ .name = "abs", .arg_types = &.{.bigint}, .return_type = .bigint, .kernel = absBigintKernel },
    .{ .name = "abs", .arg_types = &.{.double}, .return_type = .double, .kernel = absDoubleKernel },
    .{ .name = "ceil", .arg_types = &.{.double}, .return_type = .double, .kernel = ceilKernel },
    .{ .name = "floor", .arg_types = &.{.double}, .return_type = .double, .kernel = floorKernel },
    .{ .name = "round", .arg_types = &.{.double}, .return_type = .double, .kernel = roundKernel },
    .{ .name = "sign", .arg_types = &.{.double}, .return_type = .int, .kernel = signKernel },
    .{ .name = "mod", .arg_types = &.{ .int, .int }, .return_type = .int, .kernel = modIntKernel },
    .{ .name = "mod", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = modBigintKernel },
    .{ .name = "pow", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = powKernel },
    .{ .name = "sqrt", .arg_types = &.{.double}, .return_type = .double, .kernel = sqrtKernel },
    .{ .name = "exp", .arg_types = &.{.double}, .return_type = .double, .kernel = expKernel },
    .{ .name = "ln", .arg_types = &.{.double}, .return_type = .double, .kernel = lnKernel },
    .{ .name = "log10", .arg_types = &.{.double}, .return_type = .double, .kernel = log10Kernel },
    .{ .name = "log2", .arg_types = &.{.double}, .return_type = .double, .kernel = log2Kernel },
    .{ .name = "greatest", .arg_types = &.{ .int, .int }, .return_type = .int, .kernel = greatestIntKernel },
    .{ .name = "greatest", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = greatestBigintKernel },
    .{ .name = "greatest", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = greatestDoubleKernel },
    .{ .name = "least", .arg_types = &.{ .int, .int }, .return_type = .int, .kernel = leastIntKernel },
    .{ .name = "least", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .kernel = leastBigintKernel },
    .{ .name = "least", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = leastDoubleKernel },
    // --- conditional ---
    .{ .name = "ifnull", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .absorbs, .kernel = ifnullStringKernel },
    .{ .name = "ifnull", .arg_types = &.{ .int, .int }, .return_type = .int, .null_strategy = .absorbs, .kernel = ifnullIntKernel },
    .{ .name = "ifnull", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .null_strategy = .absorbs, .kernel = ifnullBigintKernel },
    .{ .name = "nullif", .arg_types = &.{ .int, .int }, .return_type = .int, .null_strategy = .kernel_managed, .kernel = nullifIntKernel },
    .{ .name = "nullif", .arg_types = &.{ .bigint, .bigint }, .return_type = .bigint, .null_strategy = .kernel_managed, .kernel = nullifBigintKernel },
    .{ .name = "nullif", .arg_types = &.{ .string, .string }, .return_type = .string, .null_strategy = .kernel_managed, .kernel = nullifStringKernel },
    // --- date/time ---
    // NB: now() / current_date() are deferred to a follow-up commit.
    // Wall-clock in Zig 0.16 routes through std.Io.Clock which needs
    // an Io instance — kernels don't carry one yet. Plumbing Io
    // through the kernel signature is a separate piece of work.
    //
    // Calendar extractors. v1: pre-1970 dates return 0 (Zig stdlib's
    // epoch helpers are unsigned). Sentinel-on-underflow is awkward
    // but covers the dominant use case (post-epoch timestamps).
    .{ .name = "year", .arg_types = &.{.date}, .return_type = .int, .kernel = yearFromDateKernel },
    .{ .name = "year", .arg_types = &.{.datetime}, .return_type = .int, .kernel = yearFromDatetimeKernel },
    .{ .name = "month", .arg_types = &.{.date}, .return_type = .int, .kernel = monthFromDateKernel },
    .{ .name = "month", .arg_types = &.{.datetime}, .return_type = .int, .kernel = monthFromDatetimeKernel },
    .{ .name = "day", .arg_types = &.{.date}, .return_type = .int, .kernel = dayFromDateKernel },
    .{ .name = "day", .arg_types = &.{.datetime}, .return_type = .int, .kernel = dayFromDatetimeKernel },
    .{ .name = "hour", .arg_types = &.{.datetime}, .return_type = .int, .kernel = hourKernel },
    .{ .name = "minute", .arg_types = &.{.datetime}, .return_type = .int, .kernel = minuteKernel },
    .{ .name = "second", .arg_types = &.{.datetime}, .return_type = .int, .kernel = secondKernel },
    // Arithmetic
    .{ .name = "datediff", .arg_types = &.{ .date, .date }, .return_type = .int, .kernel = datediffKernel },
    .{ .name = "date_add", .arg_types = &.{ .date, .int }, .return_type = .date, .kernel = dateAddKernel },
    .{ .name = "date_sub", .arg_types = &.{ .date, .int }, .return_type = .date, .kernel = dateSubKernel },
    // Epoch conversion
    .{ .name = "unix_timestamp", .arg_types = &.{.datetime}, .return_type = .bigint, .kernel = unixTimestampKernel },
    .{ .name = "from_unixtime", .arg_types = &.{.bigint}, .return_type = .datetime, .kernel = fromUnixtimeKernel },
    // --- conversion ---
    // Numeric widening (int → bigint → double): always succeeds.
    .{ .name = "to_bigint", .arg_types = &.{.int}, .return_type = .bigint, .kernel = intToBigintKernel },
    .{ .name = "to_double", .arg_types = &.{.int}, .return_type = .double, .kernel = intToDoubleKernel },
    .{ .name = "to_double", .arg_types = &.{.bigint}, .return_type = .double, .kernel = bigintToDoubleKernel },
    // Numeric narrowing (truncates, may lose precision; overflow saturates).
    .{ .name = "to_int", .arg_types = &.{.bigint}, .return_type = .int, .kernel = bigintToIntKernel },
    .{ .name = "to_int", .arg_types = &.{.double}, .return_type = .int, .kernel = doubleToIntKernel },
    .{ .name = "to_bigint", .arg_types = &.{.double}, .return_type = .bigint, .kernel = doubleToBigintKernel },
    // String parsing — on parse failure returns 0 (NULL-on-failure
    // would need .kernel_managed; deferred).
    .{ .name = "to_int", .arg_types = &.{.string}, .return_type = .int, .kernel = stringToIntKernel },
    .{ .name = "to_bigint", .arg_types = &.{.string}, .return_type = .bigint, .kernel = stringToBigintKernel },
    .{ .name = "to_double", .arg_types = &.{.string}, .return_type = .double, .kernel = stringToDoubleKernel },
    // Stringify numerics.
    .{ .name = "to_string", .arg_types = &.{.int}, .return_type = .string, .kernel = intToStringKernel },
    .{ .name = "to_string", .arg_types = &.{.bigint}, .return_type = .string, .kernel = bigintToStringKernel },
    .{ .name = "to_string", .arg_types = &.{.double}, .return_type = .string, .kernel = doubleToStringKernel },
    .{ .name = "to_string", .arg_types = &.{.boolean}, .return_type = .string, .kernel = boolToStringKernel },
    // --- hash ---
    .{ .name = "md5", .arg_types = &.{.string}, .return_type = .string, .kernel = md5Kernel },
    .{ .name = "sha1", .arg_types = &.{.string}, .return_type = .string, .kernel = sha1Kernel },
    .{ .name = "sha256", .arg_types = &.{.string}, .return_type = .string, .kernel = sha256Kernel },
    .{ .name = "crc32", .arg_types = &.{.string}, .return_type = .bigint, .kernel = crc32Kernel },
    // --- encoding ---
    .{ .name = "hex", .arg_types = &.{.string}, .return_type = .string, .kernel = hexEncodeKernel },
    .{ .name = "unhex", .arg_types = &.{.string}, .return_type = .string, .kernel = hexDecodeKernel },
    .{ .name = "to_base64", .arg_types = &.{.string}, .return_type = .string, .kernel = base64EncodeKernel },
    .{ .name = "from_base64", .arg_types = &.{.string}, .return_type = .string, .kernel = base64DecodeKernel },

    // --- string (expanded set; matches DuckDB / MySQL / StarRocks parity) ---
    .{ .name = "lpad", .arg_types = &.{ .string, .int, .string }, .return_type = .string, .kernel = lpadKernel },
    .{ .name = "rpad", .arg_types = &.{ .string, .int, .string }, .return_type = .string, .kernel = rpadKernel },
    .{ .name = "repeat", .arg_types = &.{ .string, .int }, .return_type = .string, .kernel = repeatKernel },
    .{ .name = "space", .arg_types = &.{.int}, .return_type = .string, .kernel = spaceKernel },
    .{ .name = "ascii", .arg_types = &.{.string}, .return_type = .int, .kernel = asciiKernel },
    .{ .name = "position", .arg_types = &.{ .string, .string }, .return_type = .int, .kernel = positionKernel },
    .{ .name = "instr", .arg_types = &.{ .string, .string }, .return_type = .int, .kernel = instrKernel },
    .{ .name = "substring_index", .arg_types = &.{ .string, .string, .int }, .return_type = .string, .kernel = substringIndexKernel },
    .{ .name = "strcmp", .arg_types = &.{ .string, .string }, .return_type = .int, .kernel = strcmpKernel },
    .{ .name = "greatest", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = greatestStringKernel },
    .{ .name = "least", .arg_types = &.{ .string, .string }, .return_type = .string, .kernel = leastStringKernel },

    // --- math (expanded) ---
    .{ .name = "truncate", .arg_types = &.{ .double, .int }, .return_type = .double, .kernel = truncateKernel },
    .{ .name = "degrees", .arg_types = &.{.double}, .return_type = .double, .kernel = degreesKernel },
    .{ .name = "radians", .arg_types = &.{.double}, .return_type = .double, .kernel = radiansKernel },
    .{ .name = "atan2", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = atan2Kernel },

    // --- date (expanded) ---
    .{ .name = "dayofweek", .arg_types = &.{.date}, .return_type = .int, .kernel = dayofweekFromDateKernel },
    .{ .name = "dayofweek", .arg_types = &.{.datetime}, .return_type = .int, .kernel = dayofweekFromDatetimeKernel },
    .{ .name = "dayofyear", .arg_types = &.{.date}, .return_type = .int, .kernel = dayofyearFromDateKernel },
    .{ .name = "dayofyear", .arg_types = &.{.datetime}, .return_type = .int, .kernel = dayofyearFromDatetimeKernel },
    .{ .name = "quarter", .arg_types = &.{.date}, .return_type = .int, .kernel = quarterFromDateKernel },
    .{ .name = "quarter", .arg_types = &.{.datetime}, .return_type = .int, .kernel = quarterFromDatetimeKernel },
    .{ .name = "last_day", .arg_types = &.{.date}, .return_type = .date, .kernel = lastDayFromDateKernel },
    .{ .name = "last_day", .arg_types = &.{.datetime}, .return_type = .date, .kernel = lastDayFromDatetimeKernel },

    // --- coalesce / ifnull (double overload — int families coerce here via cast.zig) ---
    .{ .name = "coalesce", .arg_types = &.{ .double, .double }, .return_type = .double, .null_strategy = .absorbs, .kernel = coalesceDoubleKernel },
    .{ .name = "ifnull", .arg_types = &.{ .double, .double }, .return_type = .double, .null_strategy = .absorbs, .kernel = ifnullDoubleKernel },

    // --- date_format: MySQL-style strftime subset ---
    .{ .name = "date_format", .arg_types = &.{ .datetime, .string }, .return_type = .string, .kernel = dateFormatDatetimeKernel },
    .{ .name = "date_format", .arg_types = &.{ .date, .string }, .return_type = .string, .kernel = dateFormatDateKernel },

    // --- MySQL aliases over existing kernels (zero new code) ---
    .{ .name = "lcase", .arg_types = &.{.string}, .return_type = .string, .kernel = lowerKernel },
    .{ .name = "ucase", .arg_types = &.{.string}, .return_type = .string, .kernel = upperKernel },
    .{ .name = "power", .arg_types = &.{ .double, .double }, .return_type = .double, .kernel = powKernel },
    .{ .name = "ceiling", .arg_types = &.{.double}, .return_type = .double, .kernel = ceilKernel },
    .{ .name = "chr", .arg_types = &.{.int}, .return_type = .string, .kernel = chrKernel },
};

// ---------------------------------------------------------------------------
// String kernels
// ---------------------------------------------------------------------------

fn upperKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const dst = try allocator.alloc(u8, src.len);
        defer allocator.free(dst);
        for (src, dst) |b, *d| d.* = std.ascii.toUpper(b);
        try store.StringStore.appendValue(stringStoreOf(out), allocator, dst);
    }
}

fn lowerKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const dst = try allocator.alloc(u8, src.len);
        defer allocator.free(dst);
        for (src, dst) |b, *d| d.* = std.ascii.toLower(b);
        try store.StringStore.appendValue(stringStoreOf(out), allocator, dst);
    }
}

fn lengthKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.int.append(allocator, @intCast(sv.rowBytes(i).len));
    }
}

fn ltrimKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        var start: usize = 0;
        while (start < src.len and std.ascii.isWhitespace(src[start])) : (start += 1) {}
        try ss.appendValue(allocator, src[start..]);
    }
}

fn rtrimKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        var end: usize = src.len;
        while (end > 0 and std.ascii.isWhitespace(src[end - 1])) : (end -= 1) {}
        try ss.appendValue(allocator, src[0..end]);
    }
}

fn trimKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        var start: usize = 0;
        while (start < src.len and std.ascii.isWhitespace(src[start])) : (start += 1) {}
        var end: usize = src.len;
        while (end > start and std.ascii.isWhitespace(src[end - 1])) : (end -= 1) {}
        try ss.appendValue(allocator, src[start..end]);
    }
}

fn reverseKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const dst = try allocator.alloc(u8, src.len);
        defer allocator.free(dst);
        for (src, 0..) |b, j| dst[src.len - 1 - j] = b;
        try ss.appendValue(allocator, dst);
    }
}

fn concat2Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = stringViewOf(args[0]);
    const b = stringViewOf(args[1]);
    const ss = stringStoreOf(out);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        scratch.clearRetainingCapacity();
        try scratch.appendSlice(allocator, a.rowBytes(i));
        try scratch.appendSlice(allocator, b.rowBytes(i));
        try ss.appendValue(allocator, scratch.items);
    }
}

fn concat3Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = stringViewOf(args[0]);
    const b = stringViewOf(args[1]);
    const c = stringViewOf(args[2]);
    const ss = stringStoreOf(out);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        scratch.clearRetainingCapacity();
        try scratch.appendSlice(allocator, a.rowBytes(i));
        try scratch.appendSlice(allocator, b.rowBytes(i));
        try scratch.appendSlice(allocator, c.rowBytes(i));
        try ss.appendValue(allocator, scratch.items);
    }
}

/// MySQL-style substring: 1-indexed start; negative start counts from
/// end; length < 0 → empty string. Out-of-range returns empty string
/// rather than erroring (matches MySQL).
fn substringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const starts = args[1].data.int;
    const lens = args[2].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const start_raw = starts[i];
        const len_raw = lens[i];
        if (len_raw <= 0 or src.len == 0) {
            try ss.appendValue(allocator, "");
            continue;
        }
        // 1-indexed; negative counts from end (-1 = last char).
        const src_len_i: i64 = @intCast(src.len);
        var start_0: i64 = if (start_raw > 0)
            @as(i64, start_raw) - 1
        else if (start_raw < 0)
            src_len_i + @as(i64, start_raw)
        else
            0;
        if (start_0 < 0) start_0 = 0;
        if (start_0 >= src_len_i) {
            try ss.appendValue(allocator, "");
            continue;
        }
        const end_0 = @min(start_0 + @as(i64, len_raw), src_len_i);
        const start_u: usize = @intCast(start_0);
        const end_u: usize = @intCast(end_0);
        try ss.appendValue(allocator, src[start_u..end_u]);
    }
}

/// MySQL REPLACE(haystack, needle, replacement). Empty needle leaves
/// the haystack unchanged (matches MySQL — avoids an infinite loop).
fn replaceKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const hay_view = stringViewOf(args[0]);
    const needle_view = stringViewOf(args[1]);
    const repl_view = stringViewOf(args[2]);
    const ss = stringStoreOf(out);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const hay = hay_view.rowBytes(i);
        const needle = needle_view.rowBytes(i);
        const repl = repl_view.rowBytes(i);
        if (needle.len == 0) {
            try ss.appendValue(allocator, hay);
            continue;
        }
        scratch.clearRetainingCapacity();
        var pos: usize = 0;
        while (std.mem.indexOfPos(u8, hay, pos, needle)) |found| {
            try scratch.appendSlice(allocator, hay[pos..found]);
            try scratch.appendSlice(allocator, repl);
            pos = found + needle.len;
        }
        try scratch.appendSlice(allocator, hay[pos..]);
        try ss.appendValue(allocator, scratch.items);
    }
}

// ---------------------------------------------------------------------------
// Null-absorbing kernels (coalesce)
// ---------------------------------------------------------------------------

fn coalesceStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0];
    const b = args[1];
    const a_sv = stringViewOf(a);
    const b_sv = stringViewOf(b);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        if (a.isValid(i)) {
            try ss.appendValue(allocator, a_sv.rowBytes(i));
        } else if (b.isValid(i)) {
            try ss.appendValue(allocator, b_sv.rowBytes(i));
        } else {
            // Both null → output null. Compute pre-allocates the bitmap
            // if the column is nullable; we write a placeholder + leave
            // the validity bit cleared (Compute writes the bitmap).
            try ss.appendValue(allocator, "");
        }
    }
}

fn coalesceIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0];
    const b = args[1];
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        if (a.isValid(i)) {
            try out.data.int.append(allocator, a.data.int[i]);
        } else if (b.isValid(i)) {
            try out.data.int.append(allocator, b.data.int[i]);
        } else {
            try out.data.int.append(allocator, 0);
        }
    }
}

fn coalesceBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0];
    const b = args[1];
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        if (a.isValid(i)) {
            try out.data.bigint.append(allocator, a.data.bigint[i]);
        } else if (b.isValid(i)) {
            try out.data.bigint.append(allocator, b.data.bigint[i]);
        } else {
            try out.data.bigint.append(allocator, 0);
        }
    }
}

// ---------------------------------------------------------------------------
// Math kernels
// ---------------------------------------------------------------------------

fn absIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        // INT_MIN's abs overflows; saturate to INT_MAX to avoid trap.
        const v = s[i];
        const r: i32 = if (v == std.math.minInt(i32)) std.math.maxInt(i32) else if (v < 0) -v else v;
        try out.data.int.append(allocator, r);
    }
}

fn absBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v = s[i];
        const r: i64 = if (v == std.math.minInt(i64)) std.math.maxInt(i64) else if (v < 0) -v else v;
        try out.data.bigint.append(allocator, r);
    }
}

fn absDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @abs(s[i]));
}

fn ceilKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @ceil(s[i]));
}

fn floorKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @floor(s[i]));
}

fn roundKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @round(s[i]));
}

fn signKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v = s[i];
        const r: i32 = if (v > 0) 1 else if (v < 0) -1 else 0;
        try out.data.int.append(allocator, r);
    }
}

fn modIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.int;
    const b = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        // MySQL convention: MOD by 0 returns 0 (we don't have nullable
        // output by default; surfacing NULL would require kernel_managed).
        const r: i32 = if (b[i] == 0) 0 else @rem(a[i], b[i]);
        try out.data.int.append(allocator, r);
    }
}

fn modBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.bigint;
    const b = args[1].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const r: i64 = if (b[i] == 0) 0 else @rem(a[i], b[i]);
        try out.data.bigint.append(allocator, r);
    }
}

fn powKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.double;
    const b = args[1].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, std.math.pow(f64, a[i], b[i]));
}

fn sqrtKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @sqrt(s[i]));
}

fn expKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @exp(s[i]));
}

fn lnKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @log(s[i]));
}

fn log10Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @log10(s[i]));
}

fn log2Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @log2(s[i]));
}

fn greatestIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.int;
    const b = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, @max(a[i], b[i]));
}

fn greatestBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.bigint;
    const b = args[1].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.bigint.append(allocator, @max(a[i], b[i]));
}

fn greatestDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.double;
    const b = args[1].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @max(a[i], b[i]));
}

fn leastIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.int;
    const b = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, @min(a[i], b[i]));
}

fn leastBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.bigint;
    const b = args[1].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.bigint.append(allocator, @min(a[i], b[i]));
}

fn leastDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.double;
    const b = args[1].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @min(a[i], b[i]));
}

// ---------------------------------------------------------------------------
// Conditional kernels
// ---------------------------------------------------------------------------
//
// IFNULL is structurally identical to a 2-arg coalesce — same null
// semantics, same dispatch. We register dedicated overloads so the
// query-builder helper reads naturally at call sites; the kernels are
// thin aliases.

fn ifnullStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    return coalesceStringKernel(allocator, args, out, row_count);
}
fn ifnullIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    return coalesceIntKernel(allocator, args, out, row_count);
}
fn ifnullBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    return coalesceBigintKernel(allocator, args, out, row_count);
}

// NULLIF(a, b) returns NULL when a == b, otherwise a. Can produce
// nulls from non-null inputs, so we manage the bitmap directly.

fn nullifIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0];
    const b = args[1];
    const base = out.data.rowCount();
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        // Equality compares only when both sides are non-null. If a is
        // null, output stays null (mirrors a). If b is null, can't
        // match a, so output is a.
        const a_valid = a.isValid(i);
        const av = a.data.int[i];
        const bv = b.data.int[i];
        const should_null = a_valid and b.isValid(i) and av == bv;
        try out.data.int.append(allocator, if (should_null) 0 else av);
        try out.appendValidBit(allocator, base + i, a_valid and !should_null);
    }
}

fn nullifBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0];
    const b = args[1];
    const base = out.data.rowCount();
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const a_valid = a.isValid(i);
        const av = a.data.bigint[i];
        const bv = b.data.bigint[i];
        const should_null = a_valid and b.isValid(i) and av == bv;
        try out.data.bigint.append(allocator, if (should_null) 0 else av);
        try out.appendValidBit(allocator, base + i, a_valid and !should_null);
    }
}

fn nullifStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0];
    const b = args[1];
    const a_sv = stringViewOf(a);
    const b_sv = stringViewOf(b);
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const a_valid = a.isValid(i);
        const a_bytes = a_sv.rowBytes(i);
        const matches = a_valid and b.isValid(i) and std.mem.eql(u8, a_bytes, b_sv.rowBytes(i));
        if (matches or !a_valid) {
            try ss.appendValue(allocator, "");
        } else {
            try ss.appendValue(allocator, a_bytes);
        }
        try out.appendValidBit(allocator, base + i, a_valid and !matches);
    }
}

// ---------------------------------------------------------------------------
// Date/time kernels
// ---------------------------------------------------------------------------
//
// Storage convention (see types.zig):
//   DATE     i32, days since 1970-01-01 UTC
//   DATETIME i64, microseconds since 1970-01-01T00:00:00 UTC
//
// v1 calendar extractors are non-negative-only — pre-1970 inputs
// return 0 rather than wrapping (Zig stdlib's epoch helpers are
// unsigned). Acceptable for the typical analytics workload.

/// Extract (year, month1to12, day1to31) from a day-since-epoch i32.
/// Returns null for pre-1970 dates.
fn daysToYmd(days: i32) ?struct { year: u16, month: u4, day: u5 } {
    if (days < 0) return null;
    const u_days: u47 = @intCast(days);
    const epoch_day = std.time.epoch.EpochDay{ .day = u_days };
    const year_day = epoch_day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    return .{
        .year = year_day.year,
        .month = month_day.month.numeric(),
        .day = month_day.day_index + 1,
    };
}

/// Extract (hour, minute, second) from a datetime i64 (micros).
/// Returns null for pre-1970 datetimes.
fn microsToHms(micros: i64) ?struct { hour: u5, minute: u6, second: u6 } {
    if (micros < 0) return null;
    const secs: u64 = @intCast(@divTrunc(micros, 1_000_000));
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = secs };
    const day_seconds = epoch_seconds.getDaySeconds();
    return .{
        .hour = day_seconds.getHoursIntoDay(),
        .minute = day_seconds.getMinutesIntoHour(),
        .second = day_seconds.getSecondsIntoMinute(),
    };
}

fn daysFromDatetime(micros: i64) i32 {
    // Floor-division so pre-epoch micros round towards -infinity.
    const secs = @divFloor(micros, 1_000_000);
    return @intCast(@divFloor(secs, 86_400));
}

fn yearFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const y: i32 = if (daysToYmd(s[i])) |ymd| @intCast(ymd.year) else 0;
        try out.data.int.append(allocator, y);
    }
}

fn yearFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const y: i32 = if (daysToYmd(daysFromDatetime(s[i]))) |ymd| @intCast(ymd.year) else 0;
        try out.data.int.append(allocator, y);
    }
}

fn monthFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const m: i32 = if (daysToYmd(s[i])) |ymd| @intCast(ymd.month) else 0;
        try out.data.int.append(allocator, m);
    }
}

fn monthFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const m: i32 = if (daysToYmd(daysFromDatetime(s[i]))) |ymd| @intCast(ymd.month) else 0;
        try out.data.int.append(allocator, m);
    }
}

fn dayFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const d: i32 = if (daysToYmd(s[i])) |ymd| @intCast(ymd.day) else 0;
        try out.data.int.append(allocator, d);
    }
}

fn dayFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const d: i32 = if (daysToYmd(daysFromDatetime(s[i]))) |ymd| @intCast(ymd.day) else 0;
        try out.data.int.append(allocator, d);
    }
}

fn hourKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const h: i32 = if (microsToHms(s[i])) |hms| @intCast(hms.hour) else 0;
        try out.data.int.append(allocator, h);
    }
}

fn minuteKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const m: i32 = if (microsToHms(s[i])) |hms| @intCast(hms.minute) else 0;
        try out.data.int.append(allocator, m);
    }
}

fn secondKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const sec: i32 = if (microsToHms(s[i])) |hms| @intCast(hms.second) else 0;
        try out.data.int.append(allocator, sec);
    }
}

fn datediffKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.date;
    const b = args[1].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, a[i] - b[i]);
}

fn dateAddKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const d = args[0].data.date;
    const n = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.date.append(allocator, d[i] + n[i]);
}

fn dateSubKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const d = args[0].data.date;
    const n = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.date.append(allocator, d[i] - n[i]);
}

fn unixTimestampKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.bigint.append(allocator, @divFloor(s[i], 1_000_000));
}

fn fromUnixtimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.datetime.append(allocator, s[i] * 1_000_000);
}

// ---------------------------------------------------------------------------
// Conversion kernels
// ---------------------------------------------------------------------------

fn intToBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.bigint.append(allocator, s[i]);
}

fn intToDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @floatFromInt(s[i]));
}

fn bigintToDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, @floatFromInt(s[i]));
}

fn bigintToIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
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

fn doubleToIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
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

fn doubleToBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
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

fn stringToIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v = std.fmt.parseInt(i32, sv.rowBytes(i), 10) catch 0;
        try out.data.int.append(allocator, v);
    }
}

fn stringToBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v = std.fmt.parseInt(i64, sv.rowBytes(i), 10) catch 0;
        try out.data.bigint.append(allocator, v);
    }
}

fn stringToDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v = std.fmt.parseFloat(f64, sv.rowBytes(i)) catch 0.0;
        try out.data.double.append(allocator, v);
    }
}

fn intToStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.int;
    const ss = stringStoreOf(out);
    var buf: [16]u8 = undefined;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const text = try std.fmt.bufPrint(&buf, "{d}", .{s[i]});
        try ss.appendValue(allocator, text);
    }
}

fn bigintToStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    const ss = stringStoreOf(out);
    var buf: [24]u8 = undefined;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const text = try std.fmt.bufPrint(&buf, "{d}", .{s[i]});
        try ss.appendValue(allocator, text);
    }
}

fn doubleToStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    const ss = stringStoreOf(out);
    var buf: [64]u8 = undefined;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const text = try std.fmt.bufPrint(&buf, "{d}", .{s[i]});
        try ss.appendValue(allocator, text);
    }
}

fn boolToStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.boolean;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try ss.appendValue(allocator, if (s[i] != 0) "true" else "false");
    }
}

// ---------------------------------------------------------------------------
// Hash kernels
// ---------------------------------------------------------------------------

fn md5Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var digest: [16]u8 = undefined;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        std.crypto.hash.Md5.hash(sv.rowBytes(i), &digest, .{});
        const hex_str = std.fmt.bytesToHex(digest, .lower);
        try ss.appendValue(allocator, &hex_str);
    }
}

fn sha1Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var digest: [20]u8 = undefined;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        std.crypto.hash.Sha1.hash(sv.rowBytes(i), &digest, .{});
        const hex_str = std.fmt.bytesToHex(digest, .lower);
        try ss.appendValue(allocator, &hex_str);
    }
}

fn sha256Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var digest: [32]u8 = undefined;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        std.crypto.hash.sha2.Sha256.hash(sv.rowBytes(i), &digest, .{});
        const hex_str = std.fmt.bytesToHex(digest, .lower);
        try ss.appendValue(allocator, &hex_str);
    }
}

fn crc32Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const c = std.hash.Crc32.hash(sv.rowBytes(i));
        try out.data.bigint.append(allocator, @intCast(c));
    }
}

// ---------------------------------------------------------------------------
// Encoding kernels
// ---------------------------------------------------------------------------

fn hexEncodeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const dst = try allocator.alloc(u8, src.len * 2);
        defer allocator.free(dst);
        const charset = "0123456789abcdef";
        for (src, 0..) |b, j| {
            dst[j * 2] = charset[b >> 4];
            dst[j * 2 + 1] = charset[b & 0x0F];
        }
        try ss.appendValue(allocator, dst);
    }
}

fn hexDecodeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        // Odd-length input + invalid chars → produce empty string
        // (MySQL convention is NULL but we'd need .kernel_managed).
        if (src.len % 2 != 0) {
            try ss.appendValue(allocator, "");
            continue;
        }
        const dst = try allocator.alloc(u8, src.len / 2);
        defer allocator.free(dst);
        var ok = true;
        for (dst, 0..) |*b, j| {
            const hi = decodeHexNibble(src[j * 2]) orelse {
                ok = false;
                break;
            };
            const lo = decodeHexNibble(src[j * 2 + 1]) orelse {
                ok = false;
                break;
            };
            b.* = (hi << 4) | lo;
        }
        try ss.appendValue(allocator, if (ok) dst else "");
    }
}

fn decodeHexNibble(c: u8) ?u8 {
    return switch (c) {
        '0'...'9' => c - '0',
        'a'...'f' => 10 + (c - 'a'),
        'A'...'F' => 10 + (c - 'A'),
        else => null,
    };
}

fn base64EncodeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    const enc = std.base64.standard.Encoder;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const dst = try allocator.alloc(u8, enc.calcSize(src.len));
        defer allocator.free(dst);
        const written = enc.encode(dst, src);
        try ss.appendValue(allocator, written);
    }
}

fn base64DecodeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    const dec = std.base64.standard.Decoder;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const out_len = dec.calcSizeForSlice(src) catch {
            try ss.appendValue(allocator, "");
            continue;
        };
        const dst = try allocator.alloc(u8, out_len);
        defer allocator.free(dst);
        dec.decode(dst, src) catch {
            try ss.appendValue(allocator, "");
            continue;
        };
        try ss.appendValue(allocator, dst);
    }
}

// ---------------------------------------------------------------------------
// Small helpers shared across kernels
// ---------------------------------------------------------------------------

inline fn stringViewOf(v: ColumnView) storage.StringView {
    return switch (v.data) {
        .varchar => |sv| sv,
        .string => |sv| sv,
        .char => |sv| sv,
        else => unreachable, // resolve() already gated on type
    };
}

inline fn stringStoreOf(out: *ColumnStore) *store.StringStore {
    return switch (out.data) {
        .varchar => |*ss| ss,
        .string => |*ss| ss,
        .char => |*ss| ss,
        else => unreachable,
    };
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

// ---------------------------------------------------------------------------
// Expanded string kernels
// ---------------------------------------------------------------------------

fn lpadKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const lens = args[1].data.int;
    const pad_sv = stringViewOf(args[2]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const target_len_i32 = lens[i];
        const pad = pad_sv.rowBytes(i);
        if (target_len_i32 <= 0 or pad.len == 0) {
            try ss.appendValue(allocator, src[0..@min(src.len, @as(usize, @intCast(@max(target_len_i32, 0))))]);
            continue;
        }
        const target_len: usize = @intCast(target_len_i32);
        if (src.len >= target_len) {
            try ss.appendValue(allocator, src[0..target_len]);
            continue;
        }
        var buf = try allocator.alloc(u8, target_len);
        defer allocator.free(buf);
        const pad_needed = target_len - src.len;
        var written: usize = 0;
        while (written < pad_needed) : (written += 1) buf[written] = pad[written % pad.len];
        @memcpy(buf[pad_needed..], src);
        try ss.appendValue(allocator, buf);
    }
}

fn rpadKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const lens = args[1].data.int;
    const pad_sv = stringViewOf(args[2]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const target_len_i32 = lens[i];
        const pad = pad_sv.rowBytes(i);
        if (target_len_i32 <= 0 or pad.len == 0) {
            try ss.appendValue(allocator, src[0..@min(src.len, @as(usize, @intCast(@max(target_len_i32, 0))))]);
            continue;
        }
        const target_len: usize = @intCast(target_len_i32);
        if (src.len >= target_len) {
            try ss.appendValue(allocator, src[0..target_len]);
            continue;
        }
        var buf = try allocator.alloc(u8, target_len);
        defer allocator.free(buf);
        @memcpy(buf[0..src.len], src);
        var j: usize = src.len;
        while (j < target_len) : (j += 1) buf[j] = pad[(j - src.len) % pad.len];
        try ss.appendValue(allocator, buf);
    }
}

fn repeatKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ns = args[1].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const n = ns[i];
        if (n <= 0 or src.len == 0) {
            try ss.appendValue(allocator, "");
            continue;
        }
        const total: usize = src.len * @as(usize, @intCast(n));
        var buf = try allocator.alloc(u8, total);
        defer allocator.free(buf);
        var k: usize = 0;
        while (k < @as(usize, @intCast(n))) : (k += 1) {
            @memcpy(buf[k * src.len ..][0..src.len], src);
        }
        try ss.appendValue(allocator, buf);
    }
}

fn spaceKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ns = args[0].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const n = ns[i];
        if (n <= 0) {
            try ss.appendValue(allocator, "");
            continue;
        }
        const len: usize = @intCast(n);
        const buf = try allocator.alloc(u8, len);
        defer allocator.free(buf);
        @memset(buf, ' ');
        try ss.appendValue(allocator, buf);
    }
}

fn asciiKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const v: i32 = if (src.len == 0) 0 else @intCast(src[0]);
        try out.data.int.append(allocator, v);
    }
}

/// 1-based offset of `needle` in `haystack`, or 0 if absent. Matches MySQL /
/// StarRocks / DuckDB. An empty needle returns 1 (consistent with most engines).
fn positionKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const needle_sv = stringViewOf(args[0]);
    const hay_sv = stringViewOf(args[1]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const needle = needle_sv.rowBytes(i);
        const hay = hay_sv.rowBytes(i);
        const v: i32 = if (needle.len == 0) 1 else if (std.mem.indexOf(u8, hay, needle)) |idx| @intCast(idx + 1) else 0;
        try out.data.int.append(allocator, v);
    }
}

/// MySQL's INSTR(haystack, needle). Same semantics as position; args swapped.
fn instrKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const hay_sv = stringViewOf(args[0]);
    const needle_sv = stringViewOf(args[1]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const hay = hay_sv.rowBytes(i);
        const needle = needle_sv.rowBytes(i);
        const v: i32 = if (needle.len == 0) 1 else if (std.mem.indexOf(u8, hay, needle)) |idx| @intCast(idx + 1) else 0;
        try out.data.int.append(allocator, v);
    }
}

/// SUBSTRING_INDEX(s, delim, count). Positive count: keep first N parts;
/// negative count: keep last |N| parts. count=0 → empty string. Matches MySQL.
fn substringIndexKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const delim_sv = stringViewOf(args[1]);
    const counts = args[2].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const src = sv.rowBytes(i);
        const delim = delim_sv.rowBytes(i);
        const n = counts[i];
        if (n == 0 or delim.len == 0) {
            try ss.appendValue(allocator, if (delim.len == 0) src else "");
            continue;
        }
        if (n > 0) {
            var remaining: i32 = n;
            var cursor: usize = 0;
            while (remaining > 0 and cursor < src.len) {
                if (std.mem.indexOfPos(u8, src, cursor, delim)) |idx| {
                    remaining -= 1;
                    if (remaining == 0) {
                        try ss.appendValue(allocator, src[0..idx]);
                        break;
                    }
                    cursor = idx + delim.len;
                } else break;
            } else {
                try ss.appendValue(allocator, src);
                continue;
            }
            if (remaining > 0) try ss.appendValue(allocator, src);
        } else {
            // Find the |n|-th delimiter from the right.
            var want: i32 = -n;
            var idx_opt: ?usize = src.len;
            while (want > 0) : (want -= 1) {
                const upper_bound = idx_opt orelse 0;
                if (upper_bound == 0) {
                    idx_opt = null;
                    break;
                }
                idx_opt = std.mem.lastIndexOf(u8, src[0..upper_bound], delim);
                if (idx_opt == null) break;
            }
            if (idx_opt) |idx| {
                try ss.appendValue(allocator, src[idx + delim.len ..]);
            } else {
                try ss.appendValue(allocator, src);
            }
        }
    }
}

fn strcmpKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a_sv = stringViewOf(args[0]);
    const b_sv = stringViewOf(args[1]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const a = a_sv.rowBytes(i);
        const b = b_sv.rowBytes(i);
        const v: i32 = switch (std.mem.order(u8, a, b)) {
            .lt => -1,
            .eq => 0,
            .gt => 1,
        };
        try out.data.int.append(allocator, v);
    }
}

fn greatestStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a_sv = stringViewOf(args[0]);
    const b_sv = stringViewOf(args[1]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const a = a_sv.rowBytes(i);
        const b = b_sv.rowBytes(i);
        try ss.appendValue(allocator, if (std.mem.order(u8, a, b) == .lt) b else a);
    }
}

fn leastStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a_sv = stringViewOf(args[0]);
    const b_sv = stringViewOf(args[1]);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const a = a_sv.rowBytes(i);
        const b = b_sv.rowBytes(i);
        try ss.appendValue(allocator, if (std.mem.order(u8, a, b) == .gt) b else a);
    }
}

// ---------------------------------------------------------------------------
// Expanded math kernels
// ---------------------------------------------------------------------------

fn truncateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const x = args[0].data.double;
    const d = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const scale: f64 = std.math.pow(f64, 10.0, @floatFromInt(d[i]));
        const v = @trunc(x[i] * scale) / scale;
        try out.data.double.append(allocator, v);
    }
}

fn degreesKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, s[i] * (180.0 / std.math.pi));
}

fn radiansKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, s[i] * (std.math.pi / 180.0));
}

fn atan2Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const y = args[0].data.double;
    const x = args[1].data.double;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.double.append(allocator, std.math.atan2(y[i], x[i]));
}

// ---------------------------------------------------------------------------
// Expanded date kernels
// ---------------------------------------------------------------------------

/// Days-since-epoch → ISO weekday index, then mapped to MySQL's
/// 1=Sunday … 7=Saturday convention (matches StarRocks).
fn dayofweekFromDays(days: i32) i32 {
    // 1970-01-01 was a Thursday. Thursday = 5 in MySQL convention
    // (Sun=1, Mon=2, …, Sat=7). Days arithmetic in mod 7.
    const d = @mod(days, 7);
    const offset_from_thu: i32 = @mod(d + 4, 7); // 4 = (Thu in MySQL=5) - 1
    return offset_from_thu + 1;
}

fn dayofweekFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, dayofweekFromDays(s[i]));
}

fn dayofweekFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, dayofweekFromDays(daysFromDatetime(s[i])));
}

fn dayofyearFromDays(days: i32) i32 {
    if (days < 0) return 0;
    const u_days: u47 = @intCast(days);
    const epoch_day = std.time.epoch.EpochDay{ .day = u_days };
    const year_day = epoch_day.calculateYearDay();
    return @as(i32, year_day.day) + 1;
}

fn dayofyearFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, dayofyearFromDays(s[i]));
}

fn dayofyearFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, dayofyearFromDays(daysFromDatetime(s[i])));
}

fn quarterFromDays(days: i32) i32 {
    const ymd = daysToYmd(days) orelse return 0;
    return @divTrunc(@as(i32, ymd.month) - 1, 3) + 1;
}

fn quarterFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, quarterFromDays(s[i]));
}

fn quarterFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, quarterFromDays(daysFromDatetime(s[i])));
}

/// LAST_DAY: return the date corresponding to the last day of the given
/// month. Days-since-epoch backed; pre-epoch returns 0.
fn lastDayFromDays(days: i32) i32 {
    const ymd = daysToYmd(days) orelse return 0;
    const last = daysInMonth(ymd.year, ymd.month);
    // Re-encode (year, month, last) as days-since-epoch. Built by stepping
    // from the start of the month: `days - (ymd.day - 1) + (last - 1)`.
    return days - @as(i32, ymd.day - 1) + @as(i32, last - 1);
}

fn daysInMonth(yr: u16, mo: u4) u5 {
    return switch (mo) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        2 => if (isLeapYear(yr)) 29 else 28,
        else => 0,
    };
}

fn isLeapYear(yr: u16) bool {
    if (yr % 400 == 0) return true;
    if (yr % 100 == 0) return false;
    return yr % 4 == 0;
}

fn lastDayFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.date.append(allocator, lastDayFromDays(s[i]));
}

fn lastDayFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.date.append(allocator, lastDayFromDays(daysFromDatetime(s[i])));
}

// ---------------------------------------------------------------------------
// Coalesce / ifnull — double overload
// ---------------------------------------------------------------------------

fn coalesceDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v: f64 = if (args[0].isValid(i)) args[0].data.double[i] else if (args[1].isValid(i)) args[1].data.double[i] else 0.0;
        try out.data.double.append(allocator, v);
    }
}

fn ifnullDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v: f64 = if (args[0].isValid(i)) args[0].data.double[i] else args[1].data.double[i];
        try out.data.double.append(allocator, v);
    }
}

// ---------------------------------------------------------------------------
// chr(int) — inverse of ascii. Out-of-range / negative input → empty string.
// ---------------------------------------------------------------------------
fn chrKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const codes = args[0].data.int;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const c = codes[i];
        if (c < 0 or c > 255) {
            try ss.appendValue(allocator, "");
        } else {
            const b: [1]u8 = .{@intCast(c)};
            try ss.appendValue(allocator, &b);
        }
    }
}

// ---------------------------------------------------------------------------
// date_format — MySQL-style strftime. Recognised specifiers:
//   %Y  4-digit year   %y  2-digit year (last two digits)
//   %m  month 01-12    %d  day 01-31
//   %H  hour 00-23     %i  minute 00-59  %s  second 00-59
//   %%  literal '%'
// Other %X sequences pass through with the '%' stripped (matches MySQL's
// "unknown specifier" behavior); bare text is copied verbatim.
//
// Per-row format strings are allowed but rare — most callers pass a literal
// format. We don't precompile (would require constant folding); each row
// re-parses, which is fine at ~few hundred ns per row.
// ---------------------------------------------------------------------------

fn appendDigits2(buf: *std.ArrayList(u8), aa: Allocator, v: u64) !void {
    try buf.append(aa, @intCast('0' + (v / 10) % 10));
    try buf.append(aa, @intCast('0' + (v % 10)));
}

fn appendDigits4(buf: *std.ArrayList(u8), aa: Allocator, v: u64) !void {
    try buf.append(aa, @intCast('0' + (v / 1000) % 10));
    try buf.append(aa, @intCast('0' + (v / 100) % 10));
    try buf.append(aa, @intCast('0' + (v / 10) % 10));
    try buf.append(aa, @intCast('0' + (v % 10)));
}

fn dateFormatRow(
    allocator: Allocator,
    out: *ColumnStore,
    fmt: []const u8,
    days: i32,
    micros_into_day: i64,
) !void {
    const ymd_opt = daysToYmd(days);
    var hh: u32 = 0;
    var mm: u32 = 0;
    var ss_v: u32 = 0;
    if (micros_into_day >= 0) {
        const total_secs = @divTrunc(micros_into_day, 1_000_000);
        hh = @intCast(@divTrunc(total_secs, 3600));
        mm = @intCast(@mod(@divTrunc(total_secs, 60), 60));
        ss_v = @intCast(@mod(total_secs, 60));
    }

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    var i: usize = 0;
    while (i < fmt.len) {
        if (fmt[i] != '%') {
            try buf.append(allocator, fmt[i]);
            i += 1;
            continue;
        }
        if (i + 1 >= fmt.len) {
            // Trailing '%' with no specifier — emit as literal.
            try buf.append(allocator, '%');
            i += 1;
            continue;
        }
        const spec = fmt[i + 1];
        i += 2;
        switch (spec) {
            'Y' => if (ymd_opt) |ymd| try appendDigits4(&buf, allocator, ymd.year) else try buf.appendSlice(allocator, "0000"),
            'y' => if (ymd_opt) |ymd| try appendDigits2(&buf, allocator, @as(u64, ymd.year) % 100) else try buf.appendSlice(allocator, "00"),
            'm' => if (ymd_opt) |ymd| try appendDigits2(&buf, allocator, ymd.month) else try buf.appendSlice(allocator, "00"),
            'd' => if (ymd_opt) |ymd| try appendDigits2(&buf, allocator, ymd.day) else try buf.appendSlice(allocator, "00"),
            'H' => try appendDigits2(&buf, allocator, hh),
            'i' => try appendDigits2(&buf, allocator, mm),
            's' => try appendDigits2(&buf, allocator, ss_v),
            '%' => try buf.append(allocator, '%'),
            else => try buf.append(allocator, spec), // unknown: pass through stripped
        }
    }

    try stringStoreOf(out).appendValue(allocator, buf.items);
}

fn dateFormatDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const dts = args[0].data.datetime;
    const fmt_sv = stringViewOf(args[1]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const micros = dts[i];
        const days = daysFromDatetime(micros);
        const micros_into_day = @mod(micros, std.time.us_per_day);
        try dateFormatRow(allocator, out, fmt_sv.rowBytes(i), days, micros_into_day);
    }
}

fn dateFormatDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ds = args[0].data.date;
    const fmt_sv = stringViewOf(args[1]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try dateFormatRow(allocator, out, fmt_sv.rowBytes(i), ds[i], 0);
    }
}

const expr_mod = @import("expr.zig");

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
// now() and current_date() helpers will land alongside their kernels
// when wall-clock plumbing through Io is wired up.
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
