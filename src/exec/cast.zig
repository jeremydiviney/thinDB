//! Implicit type-coercion scaffolding for scalar functions.
//!
//! Behavior matches DuckDB / StarRocks conventions:
//!   - Signed integer widening: tinyint → smallint → int → bigint → largeint
//!   - Int families → float / double (cross-family)
//!   - float → double
//!   - boolean → tinyint → … (boolean treated as 0/1 numeric)
//!   - date → datetime
//!
//! Things explicitly NOT implicitly cast (require `to_*` helpers):
//!   - string ↔ numeric (footgun; format-dependent)
//!   - numeric → date / datetime
//!   - any cast involving uuid
//!   - decimal precision/scale shifts (its own non-trivial problem)
//!   - int → decimal / double → int (lossy; require explicit caller intent)
//!
//! Each allowed cast has a cost (DuckDB-style). The resolver picks the
//! lowest-cost overload by summing per-arg costs; exact matches bypass
//! the lookup entirely (zero overhead on the hot path).

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const TypeTag = types.TypeTag;

const decimal = @import("scalar_fn_decimal.zig");

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const store = @import("../engine/store.zig");
const ColumnStore = store.ColumnStore;

/// Cast kernels share the scalar-function kernel signature so the
/// Compute operator can call them via the same dispatch path.
pub const CastKernel = *const fn (
    allocator: Allocator,
    args: []const ColumnView,
    out: *ColumnStore,
    row_count: usize,
) anyerror!void;

/// The cost of an implicit cast that can lose information.
pub const LOSSY_CAST_COST: u32 = 100;

/// Returns the implicit-cast cost from `from` to `to`, or null if no
/// implicit cast exists. Cost 0 = same type (callers usually short-
/// circuit before calling). Lower cost wins in overload ranking.
///
/// Cost ladder (DuckDB-inspired):
///   1–4: in-family integer widening (one promotion per step)
///   1:   float → double, decimal64 → decimal128, bool → tinyint,
///        date → datetime
///   10:  cross-family widen (int → float/double, bool further widen)
///   100: lossy (largeint → float/double)
pub fn castCost(from: TypeTag, to: TypeTag) ?u32 {
    if (from == to) return 0;
    return switch (from) {
        .tinyint => switch (to) {
            .smallint => 1,
            .int => 2,
            .bigint => 3,
            .largeint => 4,
            .float => 10,
            .double => 11,
            else => null,
        },
        .smallint => switch (to) {
            .int => 1,
            .bigint => 2,
            .largeint => 3,
            .float => 10,
            .double => 11,
            else => null,
        },
        .int => switch (to) {
            .bigint => 1,
            .largeint => 2,
            .float => 10,
            .double => 11,
            else => null,
        },
        .bigint => switch (to) {
            .largeint => 1,
            .float => LOSSY_CAST_COST,
            .double => 10,
            else => null,
        },
        .largeint => switch (to) {
            .float => LOSSY_CAST_COST,
            .double => LOSSY_CAST_COST,
            else => null,
        },
        .float => switch (to) {
            .double => 1,
            else => null,
        },
        .boolean => switch (to) {
            .tinyint => 1,
            .smallint => 2,
            .int => 3,
            .bigint => 4,
            .largeint => 5,
            else => null,
        },
        .date => switch (to) {
            .datetime => 1,
            else => null,
        },
        else => null,
    };
}

/// Returns the cast kernel for the (from, to) pair, or null if no
/// implicit cast exists. The kernel writes both data AND validity bits
/// when `out.nulls != null`; widening never introduces new nulls, so
/// the destination's validity tracks the source's.
pub fn kernelFor(from: TypeTag, to: TypeTag) ?CastKernel {
    if (from == to) return null;
    if (castCost(from, to) == null) return null;
    // Static dispatch: each allowed (from, to) pair gets its own
    // comptime-instantiated kernel. The outer switch is a jump table.
    return switch (from) {
        .tinyint => switch (to) {
            .smallint => makeIntWiden(i8, i16, .smallint),
            .int => makeIntWiden(i8, i32, .int),
            .bigint => makeIntWiden(i8, i64, .bigint),
            .largeint => makeIntWiden(i8, i128, .largeint),
            .float => makeIntToFloat(i8, f32, .float),
            .double => makeIntToFloat(i8, f64, .double),
            else => null,
        },
        .smallint => switch (to) {
            .int => makeIntWiden(i16, i32, .int),
            .bigint => makeIntWiden(i16, i64, .bigint),
            .largeint => makeIntWiden(i16, i128, .largeint),
            .float => makeIntToFloat(i16, f32, .float),
            .double => makeIntToFloat(i16, f64, .double),
            else => null,
        },
        .int => switch (to) {
            .bigint => makeIntWiden(i32, i64, .bigint),
            .largeint => makeIntWiden(i32, i128, .largeint),
            .float => makeIntToFloat(i32, f32, .float),
            .double => makeIntToFloat(i32, f64, .double),
            else => null,
        },
        .bigint => switch (to) {
            .largeint => makeIntWiden(i64, i128, .largeint),
            .float => makeIntToFloat(i64, f32, .float),
            .double => makeIntToFloat(i64, f64, .double),
            else => null,
        },
        .largeint => switch (to) {
            .float => makeIntToFloat(i128, f32, .float),
            .double => makeIntToFloat(i128, f64, .double),
            else => null,
        },
        .float => switch (to) {
            .double => makeFloatWiden(),
            else => null,
        },
        .boolean => switch (to) {
            .tinyint => makeBoolToInt(i8, .tinyint),
            .smallint => makeBoolToInt(i16, .smallint),
            .int => makeBoolToInt(i32, .int),
            .bigint => makeBoolToInt(i64, .bigint),
            .largeint => makeBoolToInt(i128, .largeint),
            else => null,
        },
        .date => switch (to) {
            .datetime => makeDateToDatetime(),
            else => null,
        },
        else => null,
    };
}

/// The cost of passing an integer argument to a narrower integer parameter,
/// or null when `from → to` is not an integer narrowing. Only the scalar
/// function resolver uses it, and only after no overload matched through
/// `castCost`, so it never changes a call that resolved by widening. It exists
/// because integer `+ - *` widen their result (DESIGN.md §3.4): StarRocks casts
/// such a BIGINT argument down to an INT parameter, e.g. `date_add(d, n + 1)`.
pub fn argNarrowingCost(from: TypeTag, to: TypeTag) ?u32 {
    const from_rank = intRank(from) orelse return null;
    const to_rank = intRank(to) orelse return null;
    return if (to_rank < from_rank) from_rank - to_rank else null;
}

fn intRank(t: TypeTag) ?u32 {
    return switch (t) {
        .tinyint => 0,
        .smallint => 1,
        .int => 2,
        .bigint => 3,
        .largeint => 4,
        else => null,
    };
}

/// The kernel for an `argNarrowingCost` cast. Out-of-range values saturate,
/// as an explicit `CAST(bigint AS INT)` does.
pub fn argNarrowingKernelFor(from: TypeTag, to: TypeTag) ?CastKernel {
    return switch (from) {
        .smallint => switch (to) {
            .tinyint => makeIntNarrow(i16, i8, .tinyint),
            else => null,
        },
        .int => switch (to) {
            .tinyint => makeIntNarrow(i32, i8, .tinyint),
            .smallint => makeIntNarrow(i32, i16, .smallint),
            else => null,
        },
        .bigint => switch (to) {
            .tinyint => makeIntNarrow(i64, i8, .tinyint),
            .smallint => makeIntNarrow(i64, i16, .smallint),
            .int => makeIntNarrow(i64, i32, .int),
            else => null,
        },
        .largeint => switch (to) {
            .tinyint => makeIntNarrow(i128, i8, .tinyint),
            .smallint => makeIntNarrow(i128, i16, .smallint),
            .int => makeIntNarrow(i128, i32, .int),
            .bigint => makeIntNarrow(i128, i64, .bigint),
            else => null,
        },
        else => null,
    };
}

/// THE result-type rule: the type one result takes when it may hold a value
/// of either type — CASE/IF branches, COALESCE/GREATEST/LEAST arguments,
/// UNION arms. StarRocks semantics: decimals meet at the precision and scale
/// covering both, a decimal and a float meet as DOUBLE, integers widen, DATE
/// meets DATETIME as DATETIME, and anything meets text as text. Null when
/// the two never share a result (a number and a date).
pub fn commonType(a: Type, b: Type) ?Type {
    if (sameRepresentation(a, b)) return if (a.isString()) commonText(a, b) else a;
    if (a.isDecimal() or b.isDecimal()) {
        if (a.isFloat() or b.isFloat()) return .double;
        if (decimal.commonSpec(&.{ a, b })) |spec| return decimal.decTypeFor(spec.p, spec.s);
    }
    const at: TypeTag = a;
    const bt: TypeTag = b;
    if (!a.isDecimal() and !b.isDecimal()) {
        if (castCost(at, bt) != null) return b;
        if (castCost(bt, at) != null) return a;
    }
    if (a.isString() or b.isString()) return .string;
    return null;
}

/// Two declared-length text types meet at the longer VARCHAR, as in MySQL;
/// unbounded text or JSON on either side meets as plain text.
fn commonText(a: Type, b: Type) Type {
    if (std.meta.eql(a, b)) return a;
    const a_len = declaredTextLength(a) orelse return .string;
    const b_len = declaredTextLength(b) orelse return .string;
    return .{ .varchar = @max(a_len, b_len) };
}

fn declaredTextLength(t: Type) ?u32 {
    return switch (t) {
        .varchar, .char => |n| n,
        else => null,
    };
}

/// `commonType` folded over every type; null when any pair never meets.
pub fn commonTypeOf(ts: []const Type) ?Type {
    if (ts.len == 0) return null;
    var acc = ts[0];
    for (ts[1..]) |t| acc = commonType(acc, t) orelse return null;
    return acc;
}

/// Whether a value of type `a` is stored exactly as the same value of type
/// `b`: the text family shares one representation, and a decimal's
/// mantissa means a different number at another scale.
pub fn sameRepresentation(a: Type, b: Type) bool {
    if (a.isString() and b.isString()) return true;
    if (a.isDecimal() or b.isDecimal()) return std.meta.eql(a, b);
    return @as(TypeTag, a) == @as(TypeTag, b);
}

// ---------------------------------------------------------------------------
// Comptime-generated kernel factories. Each returns a function pointer with
// the standard kernel signature so the Compute operator can call uniformly.
// ---------------------------------------------------------------------------

fn copyValidityIfNullable(
    allocator: Allocator,
    src: ColumnView,
    out: *ColumnStore,
    row_count: usize,
) !void {
    if (out.nulls == null) return;
    const base: u32 = @intCast(out.data.rowCount() - row_count);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.appendValidBit(allocator, base + @as(u32, @intCast(i)), src.isValid(@intCast(i)));
    }
}

fn makeIntWiden(comptime FromT: type, comptime ToT: type, comptime to_tag: TypeTag) CastKernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const src = @field(args[0].data, @tagName(srcTag(FromT)));
            const dst = &@field(out.data, @tagName(to_tag));
            var i: usize = 0;
            while (i < row_count) : (i += 1) try dst.append(allocator, @as(ToT, src[i]));
            try copyValidityIfNullable(allocator, args[0], out, row_count);
        }
    }.kernel;
}

fn makeIntNarrow(comptime FromT: type, comptime ToT: type, comptime to_tag: TypeTag) CastKernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const src = @field(args[0].data, @tagName(srcTag(FromT)));
            const dst = &@field(out.data, @tagName(to_tag));
            const lo: FromT = std.math.minInt(ToT);
            const hi: FromT = std.math.maxInt(ToT);
            var i: usize = 0;
            while (i < row_count) : (i += 1) try dst.append(allocator, @as(ToT, @intCast(std.math.clamp(src[i], lo, hi))));
            try copyValidityIfNullable(allocator, args[0], out, row_count);
        }
    }.kernel;
}

fn makeIntToFloat(comptime FromT: type, comptime ToT: type, comptime to_tag: TypeTag) CastKernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const src = @field(args[0].data, @tagName(srcTag(FromT)));
            const dst = &@field(out.data, @tagName(to_tag));
            var i: usize = 0;
            while (i < row_count) : (i += 1) try dst.append(allocator, @as(ToT, @floatFromInt(src[i])));
            try copyValidityIfNullable(allocator, args[0], out, row_count);
        }
    }.kernel;
}

fn makeFloatWiden() CastKernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const src = args[0].data.float;
            var i: usize = 0;
            while (i < row_count) : (i += 1) try out.data.double.append(allocator, @as(f64, src[i]));
            try copyValidityIfNullable(allocator, args[0], out, row_count);
        }
    }.kernel;
}

fn makeBoolToInt(comptime ToT: type, comptime to_tag: TypeTag) CastKernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const src = args[0].data.boolean;
            const dst = &@field(out.data, @tagName(to_tag));
            var i: usize = 0;
            while (i < row_count) : (i += 1) try dst.append(allocator, @as(ToT, @intCast(src[i])));
            try copyValidityIfNullable(allocator, args[0], out, row_count);
        }
    }.kernel;
}

fn makeDateToDatetime() CastKernel {
    return struct {
        // Date is days-since-epoch (i32); datetime is microseconds-since-
        // epoch (i64). Promote by multiplying days × 86_400_000_000.
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const src = args[0].data.date;
            var i: usize = 0;
            while (i < row_count) : (i += 1) {
                const micros: i64 = @as(i64, src[i]) * std.time.us_per_day;
                try out.data.datetime.append(allocator, micros);
            }
            try copyValidityIfNullable(allocator, args[0], out, row_count);
        }
    }.kernel;
}

/// Map a Zig scalar type back to its TypeTag for @field lookups. Only
/// needs to cover the source-side integer widths we cast FROM.
fn srcTag(comptime T: type) TypeTag {
    return switch (T) {
        i8 => .tinyint,
        i16 => .smallint,
        i32 => .int,
        i64 => .bigint,
        i128 => .largeint,
        else => @compileError("srcTag: unsupported source type " ++ @typeName(T)),
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "castCost: exact match is zero" {
    try std.testing.expectEqual(@as(?u32, 0), castCost(.int, .int));
    try std.testing.expectEqual(@as(?u32, 0), castCost(.bigint, .bigint));
}

test "castCost: integer widening is monotone" {
    try std.testing.expect(castCost(.tinyint, .smallint).? < castCost(.tinyint, .int).?);
    try std.testing.expect(castCost(.smallint, .int).? < castCost(.smallint, .bigint).?);
    try std.testing.expect(castCost(.int, .bigint).? < castCost(.int, .largeint).?);
}

test "castCost: int → float costs more than int widening" {
    try std.testing.expect(castCost(.int, .bigint).? < castCost(.int, .double).?);
}

test "castCost: largeint → float is high-cost lossy" {
    const c = castCost(.largeint, .double).?;
    try std.testing.expect(c >= LOSSY_CAST_COST);
}

test "castCost: no implicit string ↔ number" {
    try std.testing.expect(castCost(.string, .int) == null);
    try std.testing.expect(castCost(.int, .string) == null);
    try std.testing.expect(castCost(.varchar, .bigint) == null);
}

test "castCost: no implicit uuid casts" {
    try std.testing.expect(castCost(.uuid, .string) == null);
    try std.testing.expect(castCost(.string, .uuid) == null);
    try std.testing.expect(castCost(.uuid, .largeint) == null);
}

test "castCost: no float → int (lossy needs explicit cast)" {
    try std.testing.expect(castCost(.float, .int) == null);
    try std.testing.expect(castCost(.double, .bigint) == null);
}

test "castCost: bool widens through integer family" {
    try std.testing.expect(castCost(.boolean, .tinyint).? > 0);
    try std.testing.expect(castCost(.boolean, .bigint).? > castCost(.boolean, .tinyint).?);
}

test "castCost: date → datetime is cheap" {
    try std.testing.expectEqual(@as(?u32, 1), castCost(.date, .datetime));
    try std.testing.expect(castCost(.datetime, .date) == null);
}

test "commonType: one result type for values of either type" {
    const dec_10_2: Type = .{ .decimal64 = .{ .p = 10, .s = 2 } };
    const dec_12_4: Type = .{ .decimal64 = .{ .p = 12, .s = 4 } };
    const cases = .{
        .{ dec_10_2, dec_12_4, @as(?Type, dec_12_4) },
        .{ dec_10_2, @as(Type, .int), @as(?Type, .{ .decimal64 = .{ .p = 12, .s = 2 } }) },
        .{ dec_10_2, @as(Type, .double), @as(?Type, .double) },
        .{ @as(Type, .int), @as(Type, .bigint), @as(?Type, .bigint) },
        .{ @as(Type, .smallint), @as(Type, .double), @as(?Type, .double) },
        .{ @as(Type, .date), @as(Type, .datetime), @as(?Type, .datetime) },
        .{ @as(Type, .int), @as(Type, .{ .varchar = 5 }), @as(?Type, .string) },
        .{ @as(Type, .{ .varchar = 10 }), @as(Type, .{ .varchar = 20 }), @as(?Type, .{ .varchar = 20 }) },
        .{ @as(Type, .{ .char = 3 }), @as(Type, .{ .varchar = 2 }), @as(?Type, .{ .varchar = 3 }) },
        .{ @as(Type, .{ .varchar = 10 }), @as(Type, .string), @as(?Type, .string) },
        .{ @as(Type, .int), @as(Type, .date), @as(?Type, null) },
        .{ dec_10_2, @as(Type, .datetime), @as(?Type, null) },
    };
    inline for (cases) |c| {
        try std.testing.expectEqual(c[2], commonType(c[0], c[1]));
        try std.testing.expectEqual(c[2], commonType(c[1], c[0]));
    }
}

test "kernelFor: same-type returns null" {
    try std.testing.expect(kernelFor(.int, .int) == null);
}

test "kernelFor: every allowed cast has a kernel" {
    const tags = [_]TypeTag{
        .tinyint, .smallint, .int,  .bigint,   .largeint, .boolean,
        .float,   .double,   .date, .datetime, .string,   .varchar,
        .char,    .uuid,
    };
    for (tags) |f| for (tags) |t| {
        const has_cost = castCost(f, t) != null;
        const has_kernel = kernelFor(f, t) != null;
        if (f == t) {
            try std.testing.expect(!has_kernel);
        } else {
            try std.testing.expectEqual(has_cost, has_kernel);
        }
    };
}
