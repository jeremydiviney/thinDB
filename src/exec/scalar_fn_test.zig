//! Tests for scalar_fn registry + resolver. Kept separate from
//! scalar_fn.zig itself per CLAUDE.md (companion test file when the
//! source grows beyond ~300 lines).

const std = @import("std");

const types = @import("../types.zig");
const TypeTag = types.TypeTag;

const scalar_fn = @import("scalar_fn.zig");
const resolve = scalar_fn.resolve;

test "scalar_fn: resolve picks the matching overload" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    const r = (try resolve(aa, "upper", &.{.string})) orelse return error.NotFound;
    try std.testing.expectEqualStrings("upper", r.func.name);
    try std.testing.expectEqual(@as(TypeTag, .string), @as(TypeTag, r.func.return_type));
    try std.testing.expect(r.arg_casts == null);

    // No overload for int → null (upper has no implicit cast from int)
    try std.testing.expect((try resolve(aa, "upper", &.{.int})) == null);

    // Unknown name → null
    try std.testing.expect((try resolve(aa, "definitely_not_a_function", &.{.string})) == null);
}

test "scalar_fn: coalesce has multiple overloads, picks by arg type" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    const r_str = (try resolve(aa, "coalesce", &.{ .string, .string })) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .string), @as(TypeTag, r_str.func.return_type));

    const r_int = (try resolve(aa, "coalesce", &.{ .int, .int })) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .int), @as(TypeTag, r_int.func.return_type));

    // No string ↔ int implicit cast → no overload picked.
    try std.testing.expect((try resolve(aa, "coalesce", &.{ .int, .string })) == null);
}

test "scalar_fn: resolve coerces mixed-int args to widest overload" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    // mod has (int,int) and (bigint,bigint) overloads. Calling with
    // (int, bigint) should pick bigint via implicit cast of arg 0.
    const r = (try resolve(aa, "mod", &.{ .int, .bigint })) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .bigint), @as(TypeTag, r.func.return_type));
    const casts = r.arg_casts orelse return error.ExpectedCastPlan;
    try std.testing.expect(casts[0] != null); // int → bigint
    try std.testing.expect(casts[1] == null); // bigint → bigint (exact)
}

test "scalar_fn: resolve picks cheapest overload on ambiguity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    // greatest has (int,int) (bigint,bigint) (double,double).
    // (smallint, smallint) → cheapest path is to (int,int), not (double,double).
    const r = (try resolve(aa, "greatest", &.{ .smallint, .smallint })) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .int), @as(TypeTag, r.func.return_type));
}

test "scalar_fn: float kernel is reached via int → double coercion" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    // sqrt has only (double) overload. Passing int should pick it via
    // int → double cast.
    const r = (try resolve(aa, "sqrt", &.{.int})) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .double), @as(TypeTag, r.func.return_type));
    const casts = r.arg_casts orelse return error.ExpectedCastPlan;
    try std.testing.expect(casts[0] != null);
}

test "scalar_fn: exact match short-circuits before cost calc" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    // abs has (int), (bigint), (double). Exact match on bigint must not
    // need a cast plan even though cheaper-by-cost would also be bigint.
    const r = (try resolve(aa, "abs", &.{.bigint})) orelse return error.NotFound;
    try std.testing.expect(r.arg_casts == null);
    try std.testing.expectEqual(@as(TypeTag, .bigint), @as(TypeTag, r.func.return_type));
}

test "scalar_fn: a wider integer argument narrows only when nothing widens" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const ColumnView = @import("../storage/storage.zig").ColumnView;
    const ColumnStore = @import("../engine/store.zig").ColumnStore;

    // `date_add(d, n + 1)` passes a BIGINT to an INT parameter, as StarRocks allows.
    const r = (try resolve(aa, "date_add", &.{ .date, .bigint })) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .int), @as(TypeTag, r.func.arg_types[1]));
    const narrow = (r.arg_casts orelse return error.ExpectedCastPlan)[1] orelse return error.ExpectedCastPlan;

    const src = [_]i64{ 7, std.math.maxInt(i64), std.math.minInt(i64) };
    const args = [_]ColumnView{.{ .data = .{ .bigint = &src } }};
    var out = try ColumnStore.init(allocator, .int, false);
    defer out.deinit(allocator);
    try narrow(allocator, &args, &out, src.len);
    try std.testing.expectEqualSlices(i32, &.{ 7, std.math.maxInt(i32), std.math.minInt(i32) }, out.view().data.int);

    // A widening overload still wins: bigint → double, not bigint → int.
    const s = (try resolve(aa, "sqrt", &.{.bigint})) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .double), @as(TypeTag, s.func.arg_types[0]));
    // Narrowing is integer-only.
    try std.testing.expect((try resolve(aa, "date_add", &.{ .date, .double })) == null);
}

// ---------------------------------------------------------------------------
// Expanded scalar function registry (lpad/rpad/repeat/space/ascii/position/
// instr/substring_index/strcmp + truncate/degrees/radians/atan2 + date funcs
// + double-overload coalesce/ifnull). Coverage focused on overload selection;
// per-row correctness is exercised via integration tests in
// tests/integration/compute_test.zig.
// ---------------------------------------------------------------------------

test "scalar_fn: lpad/rpad/repeat resolve with (string, int, string) and (string, int)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    try std.testing.expect((try resolve(aa, "lpad", &.{ .string, .int, .string })) != null);
    try std.testing.expect((try resolve(aa, "rpad", &.{ .string, .int, .string })) != null);
    try std.testing.expect((try resolve(aa, "repeat", &.{ .string, .int })) != null);
    try std.testing.expect((try resolve(aa, "space", &.{.int})) != null);
}

test "scalar_fn: position/instr return int; greatest/least for strings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const pos = (try resolve(aa, "position", &.{ .string, .string })) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .int), @as(TypeTag, pos.func.return_type));
    const ins = (try resolve(aa, "instr", &.{ .string, .string })) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .int), @as(TypeTag, ins.func.return_type));
    const g = (try resolve(aa, "greatest", &.{ .string, .string })) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .string), @as(TypeTag, g.func.return_type));
}

test "scalar_fn: date helpers — dayofweek/dayofyear/quarter/last_day overloads" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    try std.testing.expect((try resolve(aa, "dayofweek", &.{.date})) != null);
    try std.testing.expect((try resolve(aa, "dayofweek", &.{.datetime})) != null);
    try std.testing.expect((try resolve(aa, "dayofyear", &.{.date})) != null);
    try std.testing.expect((try resolve(aa, "quarter", &.{.datetime})) != null);
    const ld = (try resolve(aa, "last_day", &.{.date})) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .date), @as(TypeTag, ld.func.return_type));
}

test "scalar_fn: coalesce(double, double) now resolves directly + via float coercion" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    // Exact (double, double) → no cast.
    const r1 = (try resolve(aa, "coalesce", &.{ .double, .double })) orelse return error.NotFound;
    try std.testing.expect(r1.arg_casts == null);

    // (float, float) → coerces to (double, double) via cast.zig.
    const r2 = (try resolve(aa, "coalesce", &.{ .float, .float })) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .double), @as(TypeTag, r2.func.return_type));
    const casts = r2.arg_casts orelse return error.ExpectedCastPlan;
    try std.testing.expect(casts[0] != null);
    try std.testing.expect(casts[1] != null);
}

test "scalar_fn: truncate routes via int → double coercion for bigint arg" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    // truncate is (double, int). bigint arg position 0 must coerce.
    const r = (try resolve(aa, "truncate", &.{ .bigint, .int })) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .double), @as(TypeTag, r.func.return_type));
    const casts = r.arg_casts orelse return error.ExpectedCastPlan;
    try std.testing.expect(casts[0] != null);
    try std.testing.expect(casts[1] == null);
}

test "scalar_fn: variadic conditional and string additions resolve" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    const co = (try resolve(aa, "coalesce", &.{ .int, .int, .int })) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .int), @as(TypeTag, co.func.return_type));
    try std.testing.expectEqual(@as(usize, 3), co.func.arg_types.len);
    try std.testing.expect(co.arg_casts == null);

    const iff = (try resolve(aa, "if", &.{ .boolean, .string, .string })) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .string), @as(TypeTag, iff.func.return_type));

    const cws = (try resolve(aa, "concat_ws", &.{ .string, .string, .string, .string })) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .string), @as(TypeTag, cws.func.return_type));
    try std.testing.expectEqual(@as(usize, 4), cws.func.arg_types.len);

    const fld = (try resolve(aa, "field", &.{ .string, .string, .string })) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .int), @as(TypeTag, fld.func.return_type));
}

test "scalar_fn: expanded math date and hash additions resolve" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    inline for (&.{ "sin", "cos", "tan", "asin", "acos", "atan", "cot", "cbrt", "square" }) |name| {
        const r = (try resolve(aa, name, &.{.double})) orelse return error.NotFound;
        try std.testing.expectEqual(@as(TypeTag, .double), @as(TypeTag, r.func.return_type));
    }
    try std.testing.expect((try resolve(aa, "pi", &.{})) != null);
    try std.testing.expect((try resolve(aa, "rand", &.{})) != null);
    try std.testing.expect((try resolve(aa, "log", &.{ .double, .double })) != null);
    try std.testing.expect((try resolve(aa, "round", &.{ .double, .int })) != null);
    try std.testing.expect((try resolve(aa, "conv", &.{ .string, .int, .int })) != null);

    try std.testing.expect((try resolve(aa, "dayname", &.{.date})) != null);
    try std.testing.expect((try resolve(aa, "monthname", &.{.datetime})) != null);
    try std.testing.expect((try resolve(aa, "timestampadd", &.{ .string, .int, .date })) != null);

    const sha2 = (try resolve(aa, "sha2", &.{ .string, .int })) orelse return error.NotFound;
    try std.testing.expectEqual(@as(TypeTag, .string), @as(TypeTag, sha2.func.return_type));
    try std.testing.expect((try resolve(aa, "md5sum", &.{ .string, .string })) != null);
    try std.testing.expect((try resolve(aa, "xx_hash3_128", &.{.string})) != null);
}

test "scalar_fn: nameResolvable covers builtins and decimal-only names" {
    try std.testing.expect(scalar_fn.nameResolvable(null, "upper"));
    try std.testing.expect(scalar_fn.nameResolvable(null, "COALESCE"));
    try std.testing.expect(scalar_fn.nameResolvable(null, "to_float"));
    try std.testing.expect(scalar_fn.nameResolvable(null, "to_decimal:10:2"));
    try std.testing.expect(!scalar_fn.nameResolvable(null, "definitely_not_a_function"));
}

test "scalar_fn: integer arithmetic result types match StarRocks" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    // Unary minus parses as sub(0, x) and the 0 literal types as TINYINT, so
    // the ("sub", .tinyint, x) rows are -x.
    const cases = .{
        .{ .name = "add", .a = .tinyint, .b = .tinyint, .expected = TypeTag.smallint },
        .{ .name = "add", .a = .smallint, .b = .smallint, .expected = TypeTag.int },
        .{ .name = "mul", .a = .int, .b = .int, .expected = TypeTag.bigint },
        .{ .name = "add", .a = .int, .b = .bigint, .expected = TypeTag.bigint },
        .{ .name = "sub", .a = .bigint, .b = .bigint, .expected = TypeTag.bigint },
        .{ .name = "mul", .a = .boolean, .b = .smallint, .expected = TypeTag.int },
        .{ .name = "sub", .a = .tinyint, .b = .int, .expected = TypeTag.bigint },
        .{ .name = "sub", .a = .tinyint, .b = .smallint, .expected = TypeTag.int },
        .{ .name = "sub", .a = .tinyint, .b = .bigint, .expected = TypeTag.bigint },
        .{ .name = "intdiv", .a = .int, .b = .int, .expected = TypeTag.int },
        .{ .name = "intdiv", .a = .smallint, .b = .bigint, .expected = TypeTag.bigint },
        .{ .name = "mod", .a = .smallint, .b = .int, .expected = TypeTag.int },
        .{ .name = "mod", .a = .tinyint, .b = .tinyint, .expected = TypeTag.tinyint },
    };
    inline for (cases) |c| {
        const r = (try resolve(aa, c.name, &.{ c.a, c.b })) orelse return error.NotFound;
        try std.testing.expectEqual(c.expected, @as(TypeTag, r.func.return_type));
    }

    const abs_cases = .{
        .{ .arg = .tinyint, .expected = TypeTag.smallint },
        .{ .arg = .smallint, .expected = TypeTag.int },
        .{ .arg = .int, .expected = TypeTag.bigint },
        .{ .arg = .bigint, .expected = TypeTag.bigint },
    };
    inline for (abs_cases) |c| {
        const r = (try resolve(aa, "abs", &.{c.arg})) orelse return error.NotFound;
        try std.testing.expectEqual(c.expected, @as(TypeTag, r.func.return_type));
    }
}

test "scalar_fn: integer kernels wrap, and DIV/MOD by zero or -1 never trap" {
    const allocator = std.testing.allocator;
    const math = @import("scalar_fn_math.zig");
    const ColumnView = @import("../storage/storage.zig").ColumnView;
    const ColumnStore = @import("../engine/store.zig").ColumnStore;
    const min = std.math.minInt(i64);
    const max = std.math.maxInt(i64);

    const lhs = [_]i64{ max, min, max, min, 7, -7, 7 };
    const rhs = [_]i64{ 1, 1, 2, -1, -1, 2, 0 };
    const args = [_]ColumnView{ .{ .data = .{ .bigint = &lhs } }, .{ .data = .{ .bigint = &rhs } } };

    const cases = .{
        // Compute writes every operator's validity; a zero divisor's slot
        // holds 0 and `.zero_divisor` marks it NULL.
        .{ .kernel = math.wrappingArithKernel(i64, .add), .expected = [_]i64{ min, min + 1, min + 1, max, 6, -5, 7 } },
        .{ .kernel = math.wrappingArithKernel(i64, .sub), .expected = [_]i64{ max - 1, max, max - 2, min + 1, 8, -9, 7 } },
        .{ .kernel = math.wrappingArithKernel(i64, .mul), .expected = [_]i64{ max, min, -2, min, -7, -14, 0 } },
        .{ .kernel = math.intDivModKernel(i64, .div), .expected = [_]i64{ max, min, @divTrunc(max, 2), min, -7, -3, 0 } },
        .{ .kernel = math.intDivModKernel(i64, .mod), .expected = [_]i64{ 0, 0, 1, 0, 0, -1, 0 } },
    };
    inline for (cases) |c| {
        var out = try ColumnStore.init(allocator, .bigint, false);
        defer out.deinit(allocator);
        try c.kernel(allocator, &args, &out, lhs.len);
        const want = c.expected;
        try std.testing.expectEqualSlices(i64, &want, out.view().data.bigint);
    }

    const int_lhs = [_]i32{ std.math.minInt(i32), 5 };
    const int_rhs = [_]i32{ -1, 0 };
    const int_args = [_]ColumnView{ .{ .data = .{ .int = &int_lhs } }, .{ .data = .{ .int = &int_rhs } } };
    var int_out = try ColumnStore.init(allocator, .int, false);
    defer int_out.deinit(allocator);
    try math.intDivModKernel(i32, .div)(allocator, &int_args, &int_out, int_lhs.len);
    try std.testing.expectEqualSlices(i32, &.{ std.math.minInt(i32), 0 }, int_out.view().data.int);
}

test "scalar_fn: a double truncates into an integer type, NULL past its range" {
    const truncatedInt = @import("scalar_fn_math.zig").truncatedInt;
    try std.testing.expectEqual(@as(?i32, 2147483647), truncatedInt(i32, 2147483647.9));
    try std.testing.expectEqual(@as(?i32, -2147483648), truncatedInt(i32, -2147483648.5));
    try std.testing.expectEqual(@as(?i32, -2), truncatedInt(i32, -2.5));
    try std.testing.expectEqual(@as(?i32, null), truncatedInt(i32, 2147483648.0));
    try std.testing.expectEqual(@as(?i32, null), truncatedInt(i32, -2147483649.0));
    try std.testing.expectEqual(@as(?i64, null), truncatedInt(i64, 9223372036854775808.0));
    try std.testing.expectEqual(@as(?i64, std.math.minInt(i64)), truncatedInt(i64, -9223372036854775808.0));
    try std.testing.expectEqual(@as(?i8, null), truncatedInt(i8, std.math.nan(f64)));
    try std.testing.expectEqual(@as(?i128, null), truncatedInt(i128, std.math.inf(f64)));
}
