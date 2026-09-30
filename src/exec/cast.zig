//! Implicit type-coercion scaffolding for scalar functions.
//!
//! Behavior matches DuckDB / StarRocks conventions:
//!   - Signed integer widening: tinyint → smallint → int → bigint → largeint
//!   - Int families → float / double (cross-family)
//!   - float → double
//!   - boolean → tinyint → … → double (boolean treated as 0/1 numeric)
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
//!
//! The assignment rule, further down, is how INSERT and UPDATE convert a
//! value to its column's type.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const TypeTag = types.TypeTag;

const decimal = @import("scalar_fn_decimal.zig");
const common = @import("scalar_fn_common.zig");
const scalar_fn = @import("scalar_fn.zig");
const Expr = @import("expr.zig").Expr;

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
            .float => 12,
            .double => 13,
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
            .float => makeBoolToFloat(f32, .float),
            .double => makeBoolToFloat(f64, .double),
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

/// The kernel for an `argNarrowingCost` cast: `intNarrowKernel`, so a value
/// the parameter can't hold is NULL, as it is in StarRocks.
pub fn argNarrowingKernelFor(from: TypeTag, to: TypeTag) ?CastKernel {
    return switch (from) {
        .smallint => switch (to) {
            .tinyint => intNarrowKernel(i16, i8),
            else => null,
        },
        .int => switch (to) {
            .tinyint => intNarrowKernel(i32, i8),
            .smallint => intNarrowKernel(i32, i16),
            else => null,
        },
        .bigint => switch (to) {
            .tinyint => intNarrowKernel(i64, i8),
            .smallint => intNarrowKernel(i64, i16),
            .int => intNarrowKernel(i64, i32),
            else => null,
        },
        .largeint => switch (to) {
            .tinyint => intNarrowKernel(i128, i8),
            .smallint => intNarrowKernel(i128, i16),
            .int => intNarrowKernel(i128, i32),
            .bigint => intNarrowKernel(i128, i64),
            else => null,
        },
        else => null,
    };
}

/// Whether the argument cast `from → to` can make a value NULL, so its
/// buffer and the call's result must be nullable: an integer narrowing
/// (`argNarrowingKernelFor`).
pub fn argCastCanNull(from: TypeTag, to: TypeTag) bool {
    return argNarrowingCost(from, to) != null;
}

/// THE integer narrowing rule, StarRocks semantics in every dialect: an
/// integer becomes a narrower integer type's value when it fits and NULL
/// when it doesn't (`CAST(2147483648 AS INT)`, `LEFT(s, 4294967298)`, and in
/// the MySQL dialect `CAST(~5 AS SIGNED)`, whose operand is 2^64 - 6).
/// Explicit CASTs, arguments narrowed to their parameter, a double or
/// decimal read as an integer argument (`scalar_fn.INTEGER_ARG_FN`) and a
/// table function's scalar arguments all narrow by it. A write into a column
/// raises instead (`assignNumber`), as StarRocks' strict INSERT fails.
pub fn narrowInt(comptime T: type, x: anytype) ?T {
    return std.math.cast(T, x);
}

/// The kernel narrowing a `FromT` integer column to `ToT` by `narrowInt`.
/// It writes the NULLs, so `out` must be nullable. The explicit CAST
/// overloads (`to_int(bigint)`, `to_bigint(largeint)`, ...) and
/// `argNarrowingKernelFor` share it.
pub fn intNarrowKernel(comptime FromT: type, comptime ToT: type) CastKernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const src = @field(args[0].data, @tagName(intTag(FromT)));
            const dst = &@field(out.data, @tagName(intTag(ToT)));
            const base = out.data.rowCount();
            try dst.ensureUnusedCapacity(allocator, row_count);
            for (0..row_count) |row| {
                const v = if (args[0].isValid(row)) narrowInt(ToT, src[row]) else null;
                dst.appendAssumeCapacity(v orelse 0);
                try out.appendValidBit(allocator, base + row, v != null);
            }
        }
    }.kernel;
}

/// A table function's integer literal argument as the integer type its
/// parameter declares, by `narrowInt`: null when it doesn't fit. Any other
/// value, or a parameter of another type, is returned as it is.
pub fn narrowIntegerArg(v: types.Value, to: Type) ?types.Value {
    const x: i128 = switch (v) {
        .tinyint => |x| x,
        .smallint => |x| x,
        .int => |x| x,
        .bigint => |x| x,
        .largeint => |x| x,
        else => return v,
    };
    return switch (to) {
        .tinyint => .{ .tinyint = narrowInt(i8, x) orelse return null },
        .smallint => .{ .smallint = narrowInt(i16, x) orelse return null },
        .int => .{ .int = narrowInt(i32, x) orelse return null },
        .bigint => .{ .bigint = narrowInt(i64, x) orelse return null },
        .largeint => .{ .largeint = x },
        else => v,
    };
}

/// THE result-type rule: the type one result takes when it may hold a value
/// of either type — CASE/IF branches, COALESCE/GREATEST/LEAST arguments,
/// UNION arms. StarRocks semantics: decimals meet at the precision and scale
/// covering both, a decimal and a float meet as DOUBLE, a LARGEINT and a
/// decimal as `largeintMeetsDecimal` says, integers widen, DATE meets
/// DATETIME as DATETIME, a number meets a date as `numberMeetsTemporal`
/// says, and anything meets text as text. Null when the two never share a
/// result (a UUID and a number).
pub fn commonType(a: Type, b: Type) ?Type {
    if (sameRepresentation(a, b)) return if (a.isString()) commonText(a, b) else a;
    if (numberMeetsTemporal(a, b) orelse numberMeetsTemporal(b, a)) |t| return t;
    if (a.isDecimal() or b.isDecimal()) {
        if (a.isFloat() or b.isFloat()) return .double;
        if (a == .largeint) return largeintMeetsDecimal(b);
        if (b == .largeint) return largeintMeetsDecimal(a);
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

/// A number and a DATE or DATETIME meet as in StarRocks, which reads the
/// date as its YYYYMMDD number, an INT, and the datetime as its
/// YYYYMMDDhhmmss number, a BIGINT (`CAST(d AS BIGINT)`). An integer or
/// BOOLEAN widens with that number; a decimal meets either as DOUBLE, and so
/// does a float a DATETIME. A float and a DATE meet as text, as StarRocks
/// meets them. Null when `n` is no number or `t` no date.
fn numberMeetsTemporal(n: Type, t: Type) ?Type {
    if (t != .date and t != .datetime) return null;
    if (n.isDecimal()) return .double;
    if (n.isFloat()) return if (t == .date) .string else .double;
    if (!n.isInteger() and n != .boolean) return null;
    return commonType(n, if (t == .date) .int else .bigint);
}

/// No decimal covers a LARGEINT, whose values run to 39 digits. Beside a
/// decimal with a fraction it meets as DOUBLE, as in StarRocks. Beside a
/// DECIMAL(p,0), StarRocks meets it at DECIMAL(38,0) and lets that type's
/// values run past its 38 digits; thinDB's decimals keep their precision, so
/// the type covering both is LARGEINT, which holds every DECIMAL(p,0) value
/// and prints the same digits.
fn largeintMeetsDecimal(d: Type) Type {
    return if (d.decimalSpec().?.s > 0) .double else .largeint;
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
// Assignment: a value converted to the type of the column it is written into,
// by INSERT ... VALUES, INSERT ... SELECT and UPDATE ... SET alike. MySQL's
// strict mode sets the rule: a number or numeric text lands in any integer,
// float or boolean column, and a fraction rounds half away from zero into an
// integer column, as MySQL and DuckDB do (StarRocks truncates). A number, a
// DATE or a DATETIME lands in a text column as the text `CAST(x AS CHAR)`
// gives it. Text that isn't a number is a TypeMismatch, and a value the
// column can't hold is ValueOutOfRange where a CAST would clamp it.
// ---------------------------------------------------------------------------

pub const AssignError = error{ TypeMismatch, ValueOutOfRange };

/// Whether a column of type `from` written into a column of type `to`
/// converts by the assignment rule.
pub fn assignsByRule(from: Type, to: Type) bool {
    if (@as(TypeTag, from) == @as(TypeTag, to)) return false;
    const from_number = from.isInteger() or from.isFloat() or from.isDecimal() or from == .boolean;
    if (to.isString()) return to != .json and (from_number or from == .date or from == .datetime);
    const into_number = to.isInteger() or to.isFloat() or to == .boolean;
    return into_number and (from_number or (from.isString() and from != .json));
}

/// A literal written into a column whose values are `T`: an integer type, a
/// float type or `bool`.
pub fn assignValue(comptime T: type, v: types.Value) AssignError!T {
    return switch (v) {
        .text => |s| assignText(T, s),
        .date, .datetime, .decimal64, .decimal128, .uuid => error.TypeMismatch,
        inline else => |x| assignNumber(T, x),
    };
}

/// An integer, a float or a bool written into a `T` column.
pub fn assignNumber(comptime T: type, v: anytype) AssignError!T {
    if (@TypeOf(v) == bool) return assignNumber(T, @as(u1, @intFromBool(v)));
    if (T == bool) return v != 0;
    switch (@typeInfo(T)) {
        .int => switch (@typeInfo(@TypeOf(v))) {
            .int => return std.math.cast(T, v) orelse error.ValueOutOfRange,
            .float => {
                // The bounds are powers of two, which the float holds exactly.
                const F = @TypeOf(v);
                const limit: F = -@as(F, @floatFromInt(std.math.minInt(T)));
                const rounded = @round(v);
                if (!(rounded >= -limit and rounded < limit)) return error.ValueOutOfRange;
                return @intFromFloat(rounded);
            },
            else => @compileError("assignNumber takes an integer, a float or a bool"),
        },
        .float => switch (@typeInfo(@TypeOf(v))) {
            .int => return @floatFromInt(v),
            .float => {
                const out: T = @floatCast(v);
                if (std.math.isInf(out) and !std.math.isInf(v)) return error.ValueOutOfRange;
                return out;
            },
            else => @compileError("assignNumber takes an integer, a float or a bool"),
        },
        else => @compileError("assignNumber writes an integer, a float or a bool"),
    }
}

/// A decimal, mantissa `m` at scale `scale`, written into a `T` column.
pub fn assignScaled(comptime T: type, m: i128, scale: u8) AssignError!T {
    if (T == bool) return m != 0;
    if (@typeInfo(T) == .float) return assignNumber(T, @as(f64, @floatFromInt(m)) / std.math.pow(f64, 10.0, @floatFromInt(scale)));
    const unit = decimal.pow10(scale);
    const truncated = @divTrunc(m, unit);
    const rounds_away = @abs(@rem(m, unit)) * 2 >= @abs(unit);
    return assignNumber(T, if (!rounds_away) truncated else if (m < 0) truncated - 1 else truncated + 1);
}

/// Text written into a `T` column: a number read from the text, which an
/// integer column takes exactly, digit for digit. A BOOLEAN column also
/// takes the words CAST reads (`textBoolean`), and any number, as the
/// assignment rule lands one in any number column.
pub fn assignText(comptime T: type, text: []const u8) AssignError!T {
    if (T == bool) {
        if (common.textBoolean(text)) |b| return b;
        return switch (common.textNumber(text) orelse return error.TypeMismatch) {
            .exact => |d| d.m != 0,
            .float => |f| f != 0,
        };
    }
    if (@typeInfo(T) == .float) return assignNumber(T, common.textDouble(text) orelse return error.TypeMismatch);
    return switch (common.textNumber(text) orelse return error.TypeMismatch) {
        .exact => |d| assignScaled(T, d.m, d.s),
        .float => |f| assignNumber(T, f),
    };
}

/// A literal written into a text column, appended to `text`: text as is,
/// anything else as `appendText` spells it. A decimal literal carries no
/// scale to spell it by.
pub fn appendAssignedText(allocator: Allocator, text: *std.ArrayList(u8), v: types.Value) (AssignError || Allocator.Error)!void {
    switch (v) {
        .decimal64, .decimal128 => return error.TypeMismatch,
        else => try appendText(allocator, text, v, 0),
    }
}

/// `v` appended to `text` as `CAST(v AS CHAR)` spells it, a decimal at
/// scale `scale`, but a boolean as 1 or 0: MySQL has no boolean, and stores
/// TRUE in a text column as 1.
fn appendText(allocator: Allocator, text: *std.ArrayList(u8), v: types.Value, scale: u8) (AssignError || Allocator.Error)!void {
    var buf: [64]u8 = undefined;
    switch (v) {
        .text => |s| try text.appendSlice(allocator, s),
        .boolean => |b| try text.append(allocator, if (b) '1' else '0'),
        inline .tinyint, .smallint, .int, .bigint, .largeint => |x| try text.print(allocator, "{d}", .{x}),
        inline .float, .double => |x| {
            var float_buf: [common.FLOAT_TEXT_MAX]u8 = undefined;
            try text.appendSlice(allocator, common.floatText(&float_buf, x, .plain));
        },
        inline .decimal64, .decimal128 => |m| try text.appendSlice(allocator, decimal.formatDecimal(&buf, m, scale)),
        .date => |d| try text.appendSlice(allocator, common.formatDate(&buf, d) catch return error.ValueOutOfRange),
        .datetime => |d| try text.appendSlice(allocator, common.formatDateTime(&buf, d) catch return error.ValueOutOfRange),
        .uuid => return error.TypeMismatch,
    }
}

/// `src`, a column of type `from`, written into a column of type `to`, a
/// pair `assignsByRule` accepts. The rows land in a new column in
/// `allocator`, which `freeAssignedColumn` frees; a NULL row lands as 0
/// (as '' in a text column), and `src.nulls` carries over.
pub fn assignColumn(allocator: Allocator, src: ColumnView, from: Type, to: Type, rows: usize) (AssignError || Allocator.Error)!ColumnView {
    switch (to) {
        inline .tinyint, .smallint, .int, .bigint, .largeint, .float, .double, .boolean => |_, tag| {
            const Slot = std.meta.Child(@FieldType(storage.column.ValueView, @tagName(tag)));
            const dst = try allocator.alloc(Slot, rows);
            errdefer allocator.free(dst);
            try assignRows(if (tag == .boolean) bool else Slot, src, from, dst);
            return .{ .data = @unionInit(storage.column.ValueView, @tagName(tag), dst), .nulls = src.nulls };
        },
        inline .varchar, .string, .char => |_, tag| {
            const text = try assignTextRows(allocator, src, from, rows);
            return .{ .data = @unionInit(storage.column.ValueView, @tagName(tag), text), .nulls = src.nulls };
        },
        else => return error.TypeMismatch,
    }
}

fn assignTextRows(allocator: Allocator, src: ColumnView, from: Type, rows: usize) (AssignError || Allocator.Error)!storage.column.StringView {
    const scale = if (from.decimalSpec()) |spec| spec.s else 0;
    const offsets = try allocator.alloc(u32, rows + 1);
    errdefer allocator.free(offsets);
    var bytes: std.ArrayList(u8) = .empty;
    errdefer bytes.deinit(allocator);
    offsets[0] = 0;
    for (0..rows) |i| {
        if (src.isValid(i)) try appendText(allocator, &bytes, switch (src.data) {
            inline .tinyint, .smallint, .int, .bigint, .largeint, .float, .double, .decimal64, .decimal128, .date, .datetime => |values, tag| @unionInit(types.Value, @tagName(tag), values[i]),
            .boolean => |values| .{ .boolean = values[i] != 0 },
            else => return error.TypeMismatch,
        }, scale);
        offsets[i + 1] = std.math.cast(u32, bytes.items.len) orelse return error.ValueOutOfRange;
    }
    return .{ .offsets = offsets, .bytes = try bytes.toOwnedSlice(allocator) };
}

fn assignRows(comptime T: type, src: ColumnView, from: Type, dst: anytype) AssignError!void {
    switch (src.data) {
        .varchar, .string, .char, .json => |s| for (dst, 0..) |*d, i| {
            d.* = if (src.isValid(i)) slotOf(try assignText(T, s.rowBytes(i))) else 0;
        },
        .boolean => |values| for (dst, values[0..dst.len], 0..) |*d, v, i| {
            d.* = if (src.isValid(i)) slotOf(try assignNumber(T, v != 0)) else 0;
        },
        inline .decimal64, .decimal128 => |values| {
            const scale = switch (from) {
                .decimal64, .decimal128 => |spec| spec.s,
                else => return error.TypeMismatch,
            };
            for (dst, values[0..dst.len], 0..) |*d, v, i| {
                d.* = if (src.isValid(i)) slotOf(try assignScaled(T, v, scale)) else 0;
            }
        },
        inline .tinyint, .smallint, .int, .bigint, .largeint, .float, .double => |values| for (dst, values[0..dst.len], 0..) |*d, v, i| {
            d.* = if (src.isValid(i)) slotOf(try assignNumber(T, v)) else 0;
        },
        else => return error.TypeMismatch,
    }
}

/// A converted value as its column stores it: a BOOLEAN as a byte.
fn slotOf(v: anytype) if (@TypeOf(v) == bool) u8 else @TypeOf(v) {
    return if (@TypeOf(v) == bool) @intFromBool(v) else v;
}

pub fn freeAssignedColumn(allocator: Allocator, view: ColumnView) void {
    switch (view.data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint, .float, .double, .boolean => |values| allocator.free(values),
        .varchar, .string, .char => |text| {
            allocator.free(text.offsets);
            allocator.free(text.bytes);
        },
        else => unreachable,
    }
}

/// The cast a column `name` of type `from` takes before it is written into a
/// column of type `to`, or null when it lands as is or converts by the
/// assignment rule as it lands (`assignColumn`). A decimal target always
/// takes one when the types differ: the memtable matches decimal columns on
/// tag alone, so a payload at another scale would be stored misread. Text
/// parses into a DATE or DATETIME target, and a DATETIME lands in a DATE
/// column as its day, as MySQL and StarRocks store it: a DATE stepped by
/// days is a DATETIME at midnight. Other targets widen along the
/// implicit-cast ladder short of its lossy steps; the memtable admits or
/// rejects the rest. A row the cast turns NULL is a failed write
/// (`assignmentDroppedValue`). Allocates in `arena`.
pub fn assignmentCastExpr(arena: Allocator, name: []const u8, from: Type, to: Type) !?Expr {
    if (std.meta.eql(from, to) or assignsByRule(from, to)) return null;
    const widens = if (to.isDecimal())
        from.isInteger() or from.isFloat() or from.isDecimal() or from == .boolean or (from.isString() and from != .json)
    else if (to == .date or to == .datetime)
        from.isString() or from == .date or from == .datetime
    else if (castCost(@as(TypeTag, from), @as(TypeTag, to))) |cost|
        cost > 0 and cost < LOSSY_CAST_COST
    else
        false;
    if (!widens) return null;
    const fn_name = try scalar_fn.castFnName(arena, to) orelse return null;
    const args = try arena.alloc(Expr, 1);
    args[0] = .{ .col_ref = name };
    return .{ .call = .{ .fn_name = fn_name, .args = args } };
}

/// Whether a column cast by `assignmentCastExpr` is NULL where its source
/// had a value, as for text that isn't a number or a date. INSERT ... VALUES
/// rejects such a value, so every other write does too rather than storing
/// NULL.
pub fn assignmentDroppedValue(src: ColumnView, converted: ColumnView, rows: usize) bool {
    if (!converted.anyNull(rows)) return false;
    for (0..rows) |i| {
        if (src.isValid(i) and !converted.isValid(i)) return true;
    }
    return false;
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
            const src = @field(args[0].data, @tagName(intTag(FromT)));
            const dst = &@field(out.data, @tagName(to_tag));
            var i: usize = 0;
            while (i < row_count) : (i += 1) try dst.append(allocator, @as(ToT, src[i]));
            try copyValidityIfNullable(allocator, args[0], out, row_count);
        }
    }.kernel;
}

fn makeIntToFloat(comptime FromT: type, comptime ToT: type, comptime to_tag: TypeTag) CastKernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const src = @field(args[0].data, @tagName(intTag(FromT)));
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

fn makeBoolToFloat(comptime ToT: type, comptime to_tag: TypeTag) CastKernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const src = args[0].data.boolean;
            const dst = &@field(out.data, @tagName(to_tag));
            var i: usize = 0;
            while (i < row_count) : (i += 1) try dst.append(allocator, @as(ToT, @floatFromInt(src[i])));
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

/// An integer width's TypeTag, for @field lookups.
fn intTag(comptime T: type) TypeTag {
    return switch (T) {
        i8 => .tinyint,
        i16 => .smallint,
        i32 => .int,
        i64 => .bigint,
        i128 => .largeint,
        else => @compileError("intTag: not an integer type " ++ @typeName(T)),
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

test "castCost: bool widens through integer family, then to floats" {
    try std.testing.expect(castCost(.boolean, .tinyint).? > 0);
    try std.testing.expect(castCost(.boolean, .bigint).? > castCost(.boolean, .tinyint).?);
    try std.testing.expect(castCost(.boolean, .double).? > castCost(.boolean, .largeint).?);
    try std.testing.expectEqual(@as(?Type, .double), commonType(.boolean, .double));
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
        // A number beside a date, as StarRocks' IF, COALESCE and GREATEST type it.
        .{ @as(Type, .tinyint), @as(Type, .date), @as(?Type, .int) },
        .{ @as(Type, .boolean), @as(Type, .date), @as(?Type, .int) },
        .{ @as(Type, .int), @as(Type, .date), @as(?Type, .int) },
        .{ @as(Type, .smallint), @as(Type, .datetime), @as(?Type, .bigint) },
        .{ @as(Type, .bigint), @as(Type, .date), @as(?Type, .bigint) },
        .{ @as(Type, .largeint), @as(Type, .datetime), @as(?Type, .largeint) },
        .{ dec_10_2, @as(Type, .date), @as(?Type, .double) },
        .{ dec_10_2, @as(Type, .datetime), @as(?Type, .double) },
        .{ @as(Type, .float), @as(Type, .datetime), @as(?Type, .double) },
        .{ @as(Type, .double), @as(Type, .date), @as(?Type, .string) },
        .{ @as(Type, .int), @as(Type, .uuid), @as(?Type, null) },
        .{ @as(Type, .uuid), @as(Type, .date), @as(?Type, null) },
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

test "assignment: numbers and numeric text land in integer, float and boolean columns" {
    const t = std.testing;
    const ints = .{
        .{ assignNumber(i32, @as(f64, 1.6)), 2 },
        .{ assignNumber(i32, @as(f64, 1.5)), 2 },
        .{ assignNumber(i32, @as(f64, -1.5)), -2 },
        .{ assignNumber(i32, @as(f64, 2147483647.4)), 2147483647 },
        .{ assignNumber(i8, @as(f32, 127.4)), 127 },
        .{ assignNumber(i8, @as(i64, -128)), -128 },
        .{ assignNumber(i32, true), 1 },
        .{ assignText(i32, " 12 "), 12 },
        .{ assignText(i32, "+7"), 7 },
        .{ assignText(i32, "0012"), 12 },
        .{ assignText(i32, "1.6"), 2 },
        .{ assignText(i32, ".5"), 1 },
        .{ assignText(i32, "-.5"), -1 },
        .{ assignText(i32, "-2.49"), -2 },
        .{ assignText(i32, "1e2"), 100 },
        .{ assignScaled(i32, 250, 2), 3 },
        .{ assignScaled(i32, -250, 2), -3 },
        .{ assignScaled(i32, 249, 2), 2 },
    };
    inline for (ints) |c| try t.expectEqual(@as(i32, c[1]), @as(i32, try c[0]));
    try t.expectEqual(@as(i64, 9007199254740993), try assignText(i64, "9007199254740993"));
    try t.expectEqual(@as(i128, std.math.minInt(i128)), try assignText(i128, "-170141183460469231731687303715884105728"));

    try t.expectEqual(@as(f64, 1.5), try assignText(f64, "1.5"));
    try t.expectEqual(@as(f64, 25), try assignText(f64, " 2.5e1 "));
    try t.expectEqual(@as(f64, 1), try assignNumber(f64, true));
    try t.expectEqual(@as(f32, 2.5), try assignScaled(f32, 25, 1));
    try t.expectEqual(true, try assignText(bool, "true"));
    try t.expectEqual(true, try assignText(bool, "0.5"));
    try t.expectEqual(false, try assignText(bool, " 0.0 "));
    try t.expectError(error.TypeMismatch, assignText(bool, "abc"));
    try t.expectEqual(true, try assignNumber(bool, @as(i32, 5)));
    try t.expectEqual(false, try assignNumber(bool, @as(f64, 0)));

    inline for (.{ "12abc", "", "-0x10", "abc", "1 2" }) |bad| {
        try t.expectError(error.TypeMismatch, assignText(i32, bad));
        try t.expectError(error.TypeMismatch, assignText(f64, bad));
    }
    try t.expectError(error.ValueOutOfRange, assignNumber(i8, @as(i32, 300)));
    try t.expectError(error.ValueOutOfRange, assignText(i8, "300"));
    try t.expectError(error.ValueOutOfRange, assignNumber(i8, @as(f64, 127.5)));
    try t.expectError(error.ValueOutOfRange, assignNumber(i32, @as(f64, 2147483647.5)));
    try t.expectError(error.ValueOutOfRange, assignNumber(i64, @as(f64, 9223372036854775808.0)));
    try t.expectError(error.ValueOutOfRange, assignNumber(i64, std.math.nan(f64)));
    try t.expectError(error.ValueOutOfRange, assignNumber(f32, @as(f64, 1e39)));
    try t.expectError(error.ValueOutOfRange, assignScaled(i8, 12850, 2));
    try t.expectError(error.TypeMismatch, assignValue(i32, .{ .date = 1 }));
}

test "assignment: a column converts row by row and keeps its NULLs" {
    const t = std.testing;
    const text: storage.column.StringView = .{ .offsets = &.{ 0, 2, 2, 5 }, .bytes = "122.5" };
    const nulls = [_]u8{0b101};
    const col = try assignColumn(t.allocator, .{ .data = .{ .varchar = text }, .nulls = &nulls }, .{ .varchar = 8 }, .smallint, 3);
    defer freeAssignedColumn(t.allocator, col);
    try t.expectEqualSlices(i16, &.{ 12, 0, 3 }, col.data.smallint);

    const doubles = [_]f64{ 0.5, -0.5, 3 };
    const bools = try assignColumn(t.allocator, .{ .data = .{ .double = &doubles } }, .double, .boolean, 3);
    defer freeAssignedColumn(t.allocator, bools);
    try t.expectEqualSlices(u8, &.{ 1, 1, 1 }, bools.data.boolean);

    const decimals = [_]i64{ 150, -150, 149 };
    const ints = try assignColumn(t.allocator, .{ .data = .{ .decimal64 = &decimals } }, .{ .decimal64 = .{ .p = 6, .s = 2 } }, .bigint, 3);
    defer freeAssignedColumn(t.allocator, ints);
    try t.expectEqualSlices(i64, &.{ 2, -2, 1 }, ints.data.bigint);

    const wide = [_]i64{ 1, 40000 };
    try t.expectError(error.ValueOutOfRange, assignColumn(t.allocator, .{ .data = .{ .bigint = &wide } }, .bigint, .smallint, 2));
    try t.expect(assignsByRule(.{ .varchar = 8 }, .int));
    try t.expect(!assignsByRule(.int, .int));
    try t.expect(!assignsByRule(.json, .int));
    try t.expect(!assignsByRule(.int, .{ .decimal64 = .{ .p = 6, .s = 2 } }));
}
