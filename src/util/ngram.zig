//! Trigram queries for the segment n-gram index (DESIGN.md §4.7): the
//! three-byte substrings, ASCII letters folded to lower case, that a value
//! must hold for a LIKE pattern or a regex to match it. A value holding them
//! may still not match; one lacking them never does.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Three folded bytes, the first in the high byte.
pub const Gram = u32;

pub const gram_space: usize = 1 << 24;

pub fn fold(b: u8) u8 {
    return std.ascii.toLower(b);
}

/// The gram of `bytes[i..][0..3]`.
pub fn gramAt(bytes: []const u8, i: usize) Gram {
    return @as(Gram, fold(bytes[i])) << 16 | @as(Gram, fold(bytes[i + 1])) << 8 | fold(bytes[i + 2]);
}

/// What a value must hold: every gram of an `and`, or the grams of one arm
/// of an `or`. `.all` asks nothing.
pub const Query = union(enum) {
    all,
    gram: Gram,
    @"and": []const Query,
    @"or": []const Query,
};

/// `arms` conjoined: the arms asking something, a lone one as itself.
pub fn conjoin(allocator: Allocator, arms: []const Query) !Query {
    var kept: std.ArrayList(Query) = .empty;
    defer kept.deinit(allocator);
    for (arms) |arm| switch (arm) {
        .all => {},
        .@"and" => |inner| try kept.appendSlice(allocator, inner),
        else => try kept.append(allocator, arm),
    };
    return switch (kept.items.len) {
        0 => .all,
        1 => kept.items[0],
        else => .{ .@"and" = try kept.toOwnedSlice(allocator) },
    };
}

/// `arms` disjoined: `.all` when any arm asks nothing.
pub fn disjoin(allocator: Allocator, arms: []const Query) !Query {
    var kept: std.ArrayList(Query) = .empty;
    defer kept.deinit(allocator);
    for (arms) |arm| switch (arm) {
        .all => return .all,
        .@"or" => |inner| try kept.appendSlice(allocator, inner),
        else => try kept.append(allocator, arm),
    };
    return switch (kept.items.len) {
        0 => .all,
        1 => kept.items[0],
        else => .{ .@"or" = try kept.toOwnedSlice(allocator) },
    };
}

/// Every gram of `text`, conjoined.
pub fn ofText(allocator: Allocator, text: []const u8) !Query {
    if (text.len < 3) return .all;
    var grams: std.ArrayList(Query) = .empty;
    defer grams.deinit(allocator);
    for (0..text.len - 2) |i| {
        const g = gramAt(text, i);
        const seen = for (grams.items) |q| {
            if (q.gram == g) break true;
        } else false;
        if (!seen) try grams.append(allocator, .{ .gram = g });
    }
    return conjoin(allocator, grams.items);
}

/// The grams of a LIKE pattern's literal runs (`util/like.zig` syntax):
/// the bytes between wildcards, with escapes resolved.
pub fn ofLike(allocator: Allocator, pattern: []const u8) !Query {
    var arms: std.ArrayList(Query) = .empty;
    defer arms.deinit(allocator);
    var run: std.ArrayList(u8) = .empty;
    defer run.deinit(allocator);
    var i: usize = 0;
    while (i <= pattern.len) : (i += 1) {
        const c = if (i < pattern.len) pattern[i] else '%';
        if (c == '%' or c == '_') {
            const q = try ofText(allocator, run.items);
            if (q != .all) try arms.append(allocator, q);
            run.clearRetainingCapacity();
            continue;
        }
        if (c == '\\' and i + 1 < pattern.len) i += 1;
        try run.append(allocator, pattern[i]);
    }
    return conjoin(allocator, arms.items);
}

test "ngram: LIKE runs give their grams, wildcards and escapes split them" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    try std.testing.expectEqual(Query.all, try ofLike(a, "%go%"));
    try std.testing.expectEqual(Query.all, try ofLike(a, "a_b_c"));
    try std.testing.expectEqual(Query{ .gram = gramAt("abc", 0) }, try ofLike(a, "%ABC%"));

    const google = try ofLike(a, "%google%");
    try std.testing.expectEqual(@as(usize, 4), google.@"and".len);
    try std.testing.expectEqual(gramAt("goo", 0), google.@"and"[0].gram);
    try std.testing.expectEqual(gramAt("gle", 0), google.@"and"[3].gram);

    const split = try ofLike(a, "abc_def%gh");
    try std.testing.expectEqual(@as(usize, 2), split.@"and".len);

    const escaped = try ofLike(a, "a\\%bc");
    try std.testing.expectEqual(@as(usize, 2), escaped.@"and".len);
    try std.testing.expectEqual(gramAt("a%b", 0), escaped.@"and"[0].gram);

    const repeated = try ofLike(a, "aaaa");
    try std.testing.expectEqual(Query{ .gram = gramAt("aaa", 0) }, repeated);
}

test "ngram: an arm asking nothing makes a disjunction ask nothing" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const g: Query = .{ .gram = 7 };
    try std.testing.expectEqual(Query.all, try disjoin(a, &.{ g, .all }));
    try std.testing.expectEqual(g, try conjoin(a, &.{ g, .all }));
    try std.testing.expectEqual(@as(usize, 2), (try disjoin(a, &.{ g, .{ .gram = 8 } })).@"or".len);
}
