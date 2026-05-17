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

/// Look up the first registered overload whose name + arg types match.
/// Type comparison is by `TypeTag` only — width metadata (varchar
/// length, decimal precision) doesn't affect overload selection.
/// Returns null when no overload matches.
pub fn resolve(name: []const u8, arg_types: []const Type) ?ScalarFn {
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
        if (all_match) return f;
    }
    return null;
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "scalar_fn: resolve picks the matching overload" {
    const f = resolve("upper", &.{.string}) orelse return error.NotFound;
    try std.testing.expectEqualStrings("upper", f.name);
    try std.testing.expectEqual(@as(TypeTag, .string), @as(TypeTag, f.return_type));

    // No overload for int → null
    try std.testing.expect(resolve("upper", &.{.int}) == null);

    // Unknown name → null
    try std.testing.expect(resolve("definitely_not_a_function", &.{.string}) == null);
}

test "scalar_fn: coalesce has multiple overloads, picks by arg type" {
    const f_str = resolve("coalesce", &.{ .string, .string }) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .string), @as(TypeTag, f_str.return_type));

    const f_int = resolve("coalesce", &.{ .int, .int }) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .int), @as(TypeTag, f_int.return_type));

    // No mixed-type overload → null
    try std.testing.expect(resolve("coalesce", &.{ .int, .string }) == null);
}
