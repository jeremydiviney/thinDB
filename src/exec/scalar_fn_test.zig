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
