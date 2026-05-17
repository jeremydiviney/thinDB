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
    propagates,
    /// Kernel handles nulls itself. Compute does NOT pre-mask; the
    /// kernel sees the raw inputs and must inspect `view.nulls` per
    /// arg. Used by `coalesce`, `ifnull`, etc.
    absorbs,
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
