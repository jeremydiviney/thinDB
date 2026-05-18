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
