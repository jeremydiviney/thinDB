//! Statistical / set-oriented aggregates added in the post-coercion
//! aggregate expansion: stddev_*, var_*, count_distinct, percentile,
//! group_concat. Smoke tests covering the global + grouped paths plus
//! the edge cases (empty input, single value, all-null).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

const schema_nums = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "x", .type = .double },
        .{ .name = "g", .type = .string },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const ok_nums = [_][]const u8{"id"};
const opts_nums = thindb.TableOptions{
    .order_key = &ok_nums,
    .unique = true,
    .row_group_size = 8,
};

fn openWithRows(allocator: std.mem.Allocator, io: anytype, tmp: anytype) !*thindb.Database {
    _ = tmp;
    _ = io;
    _ = allocator;
    unreachable;
}

test "aggregate: var_pop / var_samp / stddev_pop / stddev_samp on known sample" {
    // Numbers 1..5 — population variance = 2.0, sample variance = 2.5.
    // population stddev = sqrt(2.0), sample stddev = sqrt(2.5).
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema_nums, opts_nums);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .x = @as(f64, 1.0), .g = "a" },
        .{ .id = @as(i64, 2), .x = @as(f64, 2.0), .g = "a" },
        .{ .id = @as(i64, 3), .x = @as(f64, 3.0), .g = "a" },
        .{ .id = @as(i64, 4), .x = @as(f64, 4.0), .g = "a" },
        .{ .id = @as(i64, 5), .x = @as(f64, 5.0), .g = "a" },
    });
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var q = try base.aggregate(&.{
        .{ .func = .var_pop, .col = "x", .as = "vp" },
        .{ .func = .var_samp, .col = "x", .as = "vs" },
        .{ .func = .stddev_pop, .col = "x", .as = "sp" },
        .{ .func = .stddev_samp, .col = "x", .as = "ss" },
    });
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), b.values[0].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 2.5), b.values[1].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@sqrt(@as(f64, 2.0)), b.values[2].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@sqrt(@as(f64, 2.5)), b.values[3].data.double[0], 1e-9);
}

test "aggregate: stddev/variance over 1-row group emits 0 (no NULL surface in v1)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema_nums, opts_nums);
    try t.insert(&.{.{ .id = @as(i64, 1), .x = @as(f64, 42.0), .g = "a" }});
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var q = try base.aggregate(&.{
        .{ .func = .var_samp, .col = "x", .as = "vs" },
        .{ .func = .stddev_samp, .col = "x", .as = "ss" },
        .{ .func = .var_pop, .col = "x", .as = "vp" },
    });
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(f64, 0.0), b.values[0].data.double[0]); // n<2 → 0
    try std.testing.expectEqual(@as(f64, 0.0), b.values[1].data.double[0]);
    try std.testing.expectEqual(@as(f64, 0.0), b.values[2].data.double[0]); // var_pop of one value = 0
}

test "aggregate: count_distinct excludes NULLs and dedupes" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "tag", .type = .string, .nullable = true },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 8 };

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, opts);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .tag = @as(?[]const u8, "x") },
        .{ .id = @as(i64, 2), .tag = @as(?[]const u8, "y") },
        .{ .id = @as(i64, 3), .tag = @as(?[]const u8, "x") }, // dup of row 1
        .{ .id = @as(i64, 4), .tag = @as(?[]const u8, null) }, // excluded
        .{ .id = @as(i64, 5), .tag = @as(?[]const u8, "z") },
        .{ .id = @as(i64, 6), .tag = @as(?[]const u8, "y") }, // dup of row 2
    });
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var q = try base.aggregate(&.{
        .{ .func = .count_distinct, .col = "tag", .as = "nd" },
    });
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 3), b.values[0].data.bigint[0]); // x, y, z
}

test "aggregate: percentile_cont — median + p25 + p75 of 1..10" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema_nums, opts_nums);
    var i: i64 = 1;
    while (i <= 10) : (i += 1) {
        try t.insert(&.{.{ .id = i, .x = @as(f64, @floatFromInt(i)), .g = "a" }});
    }
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var q = try base.aggregate(&.{
        .{ .func = .percentile, .col = "x", .as = "p25", .params = .{ .percentile = 0.25 } },
        .{ .func = .percentile, .col = "x", .as = "p50", .params = .{ .percentile = 0.5 } },
        .{ .func = .percentile, .col = "x", .as = "p75", .params = .{ .percentile = 0.75 } },
    });
    defer q.deinit();
    const b = (try q.next()).?;
    // PostgreSQL percentile_cont rule on 1..10:
    //   p25 = 1 + 0.25*9 = 3.25
    //   p50 = 1 + 0.5 *9 = 5.5
    //   p75 = 1 + 0.75*9 = 7.75
    try std.testing.expectApproxEqAbs(@as(f64, 3.25), b.values[0].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 5.5), b.values[1].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 7.75), b.values[2].data.double[0], 1e-9);
}

test "aggregate: group_concat with separator preserves insertion order" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema_nums, opts_nums);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .x = @as(f64, 0.0), .g = "alpha" },
        .{ .id = @as(i64, 2), .x = @as(f64, 0.0), .g = "beta" },
        .{ .id = @as(i64, 3), .x = @as(f64, 0.0), .g = "gamma" },
    });
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var q = try base.aggregate(&.{
        .{ .func = .group_concat, .col = "g", .as = "joined", .params = .{ .separator = ", " } },
    });
    defer q.deinit();
    const b = (try q.next()).?;
    const sv = b.values[0].data.string;
    try std.testing.expectEqualStrings("alpha, beta, gamma", sv.rowBytes(0));
}

test "aggregate: grouped stddev_pop + count_distinct by tag" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema_nums, opts_nums);
    try t.insert(&.{
        // group "a": x = 1, 2, 3 → mean 2, var_pop = (1+0+1)/3 = 2/3
        .{ .id = @as(i64, 1), .x = @as(f64, 1.0), .g = "a" },
        .{ .id = @as(i64, 2), .x = @as(f64, 2.0), .g = "a" },
        .{ .id = @as(i64, 3), .x = @as(f64, 3.0), .g = "a" },
        // group "b": x = 4, 4, 4 → var_pop = 0
        .{ .id = @as(i64, 4), .x = @as(f64, 4.0), .g = "b" },
        .{ .id = @as(i64, 5), .x = @as(f64, 4.0), .g = "b" },
        .{ .id = @as(i64, 6), .x = @as(f64, 4.0), .g = "b" },
    });
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var q = try base.groupBy(&.{"g"}, &.{
        .{ .func = .stddev_pop, .col = "x", .as = "sp" },
        .{ .func = .count_distinct, .col = "x", .as = "nd" },
    });
    defer q.deinit();

    var rows_seen: usize = 0;
    while (try q.next()) |b| {
        for (0..b.row_count) |i| {
            rows_seen += 1;
            const g = b.values[0].data.string.rowBytes(i);
            const sp = b.values[1].data.double[i];
            const nd = b.values[2].data.bigint[i];
            if (std.mem.eql(u8, g, "a")) {
                try std.testing.expectApproxEqAbs(@sqrt(@as(f64, 2.0 / 3.0)), sp, 1e-9);
                try std.testing.expectEqual(@as(i64, 3), nd);
            } else if (std.mem.eql(u8, g, "b")) {
                try std.testing.expectApproxEqAbs(@as(f64, 0.0), sp, 1e-9);
                try std.testing.expectEqual(@as(i64, 1), nd);
            } else return error.UnexpectedGroup;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), rows_seen);
}

test "aggregate: invalid percentile param rejected" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema_nums, opts_nums);
    try t.insert(&.{.{ .id = @as(i64, 1), .x = @as(f64, 0.0), .g = "a" }});
    try t.flush();

    var base = try thindb.scan(allocator, t);
    const result = base.aggregate(&.{
        .{ .func = .percentile, .col = "x", .as = "bad", .params = .{ .percentile = 1.5 } },
    });
    try std.testing.expectError(thindb.exec.Error.AggregateInvalidParam, result);
    base.deinit();
}

/// Look up the bigint value the top-k result emitted for group `g`, or null
/// if `g` wasn't among the returned rows. Validates the heap selected the
/// right *set* of groups regardless of (non-deterministic) emit order.
fn topkBigint(b: thindb.exec.Batch, g: []const u8) ?i64 {
    for (0..b.row_count) |i| {
        if (std.mem.eql(u8, b.values[0].data.string.rowBytes(i), g)) return b.values[1].data.bigint[i];
    }
    return null;
}

fn topkDouble(b: thindb.exec.Batch, g: []const u8) ?f64 {
    for (0..b.row_count) |i| {
        if (std.mem.eql(u8, b.values[0].data.string.rowBytes(i), g)) return b.values[1].data.double[i];
    }
    return null;
}

fn topkHas(b: thindb.exec.Batch, g: []const u8) bool {
    for (0..b.row_count) |i| {
        if (std.mem.eql(u8, b.values[0].data.string.rowBytes(i), g)) return true;
    }
    return false;
}

test "aggregate: top-k fusion selects the correct k groups" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema_nums, opts_nums);
    // Five groups with strictly distinct counts AND distinct sums, but the
    // two orderings disagree — so a correct heap must use the named column,
    // not just row count. count: a5 b4 c3 d2 e1; sum: e100 d40 c9 b8 a5.
    var id: i64 = 0;
    inline for (.{
        .{ .g = "a", .n = 5, .x = 1.0 },
        .{ .g = "b", .n = 4, .x = 2.0 },
        .{ .g = "c", .n = 3, .x = 3.0 },
        .{ .g = "d", .n = 2, .x = 20.0 },
        .{ .g = "e", .n = 1, .x = 100.0 },
    }) |grp| {
        for (0..grp.n) |_| {
            id += 1;
            try t.insert(&.{.{ .id = id, .x = @as(f64, grp.x), .g = grp.g }});
        }
    }
    try t.flush();

    // COUNT(*) DESC LIMIT 3 → {a:5, b:4, c:3}; d, e excluded.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.groupByTopK(&.{"g"}, &.{
            .{ .func = .count, .as = "c" },
        }, .{ .k = 3, .keys = &.{.{ .col = "c", .desc = true }} }, null);
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 3), b.row_count);
        try std.testing.expectEqual(@as(?i64, 5), topkBigint(b, "a"));
        try std.testing.expectEqual(@as(?i64, 4), topkBigint(b, "b"));
        try std.testing.expectEqual(@as(?i64, 3), topkBigint(b, "c"));
        try std.testing.expectEqual(@as(?i64, null), topkBigint(b, "d"));
        try std.testing.expectEqual(@as(?i64, null), topkBigint(b, "e"));
    }

    // SUM(x) DESC LIMIT 3 → {e:100, d:40, c:9}; disjoint from the count top-3
    // except for c, proving the order column drives selection.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.groupByTopK(&.{"g"}, &.{
            .{ .func = .sum, .col = "x", .as = "s" },
        }, .{ .k = 3, .keys = &.{.{ .col = "s", .desc = true }} }, null);
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 3), b.row_count);
        try std.testing.expectApproxEqAbs(@as(f64, 100.0), topkDouble(b, "e").?, 1e-9);
        try std.testing.expectApproxEqAbs(@as(f64, 40.0), topkDouble(b, "d").?, 1e-9);
        try std.testing.expectApproxEqAbs(@as(f64, 9.0), topkDouble(b, "c").?, 1e-9);
        try std.testing.expectEqual(@as(?f64, null), topkDouble(b, "a"));
        try std.testing.expectEqual(@as(?f64, null), topkDouble(b, "b"));
    }

    // COUNT(*) ASC LIMIT 2 → the two smallest groups {e:1, d:2}.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.groupByTopK(&.{"g"}, &.{
            .{ .func = .count, .as = "c" },
        }, .{ .k = 2, .keys = &.{.{ .col = "c", .desc = false }} }, null);
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 2), b.row_count);
        try std.testing.expectEqual(@as(?i64, 1), topkBigint(b, "e"));
        try std.testing.expectEqual(@as(?i64, 2), topkBigint(b, "d"));
    }

    // Unresolvable order column (string MIN) → fall back to emitting every
    // group; the downstream Limit would still trim. All five groups present.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.groupByTopK(&.{"g"}, &.{
            .{ .func = .min, .col = "g", .as = "mg" },
        }, .{ .k = 2, .keys = &.{.{ .col = "mg", .desc = true }} }, null);
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 5), b.row_count);
    }
}

test "aggregate: top-k fusion honors multiple order keys (lexicographic + per-key direction)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema_nums, opts_nums);
    // Groups b and c tie on SUM(x)=8 but differ on COUNT (b=9, c=3). With
    // LIMIT 2 the cut falls *inside* that tie, so the secondary key decides
    // which of b/c survives — and flipping its direction flips the winner.
    // sums:  a=10, b=8, c=8, d=5;  counts: a=5, b=9, c=3, d=1.
    var id: i64 = 0;
    inline for (.{
        .{ .g = "a", .xs = [_]f64{ 2, 2, 2, 2, 2 } }, // sum 10, count 5
        .{ .g = "b", .xs = [_]f64{ 1, 1, 1, 1, 1, 1, 1, 1, 0 } }, // sum 8, count 9
        .{ .g = "c", .xs = [_]f64{ 4, 2, 2 } }, // sum 8, count 3
        .{ .g = "d", .xs = [_]f64{5} }, // sum 5, count 1
    }) |grp| {
        inline for (grp.xs) |xv| {
            id += 1;
            try t.insert(&.{.{ .id = id, .x = @as(f64, xv), .g = grp.g }});
        }
    }
    try t.flush();

    // SUM(x) DESC, COUNT DESC LIMIT 2 → a, then the SUM=8 tie breaks to b (9 > 3).
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.groupByTopK(&.{"g"}, &.{
            .{ .func = .sum, .col = "x", .as = "s" },
            .{ .func = .count, .as = "c" },
        }, .{ .k = 2, .keys = &.{ .{ .col = "s", .desc = true }, .{ .col = "c", .desc = true } } }, null);
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 2), b.row_count);
        try std.testing.expect(topkHas(b, "a"));
        try std.testing.expect(topkHas(b, "b"));
        try std.testing.expect(!topkHas(b, "c"));
        try std.testing.expect(!topkHas(b, "d"));
    }

    // SUM(x) DESC, COUNT ASC LIMIT 2 → a, then the tie flips to c (3 < 9).
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.groupByTopK(&.{"g"}, &.{
            .{ .func = .sum, .col = "x", .as = "s" },
            .{ .func = .count, .as = "c" },
        }, .{ .k = 2, .keys = &.{ .{ .col = "s", .desc = true }, .{ .col = "c", .desc = false } } }, null);
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 2), b.row_count);
        try std.testing.expect(topkHas(b, "a"));
        try std.testing.expect(topkHas(b, "c"));
        try std.testing.expect(!topkHas(b, "b"));
        try std.testing.expect(!topkHas(b, "d"));
    }
}

test "agg_stats: metadata-only MIN/MAX skips NULLs on a nullable column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "v", .type = .int, .nullable = true },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 4 };
    const t = try db.table("t", schema, opts);
    // Non-null values {10, 3, 7} interleaved with NULLs (and split across two
    // row groups by the size-4 setting) → MIN 3, MAX 10; NULLs ignored.
    try t.insert(&.{
        .{ .id = @as(i64, 1), .v = @as(i32, 10) },
        .{ .id = @as(i64, 2), .v = @as(?i32, null) },
        .{ .id = @as(i64, 3), .v = @as(i32, 3) },
        .{ .id = @as(i64, 4), .v = @as(?i32, null) },
        .{ .id = @as(i64, 5), .v = @as(i32, 7) },
        .{ .id = @as(i64, 6), .v = @as(?i32, null) },
    });
    try t.flush();

    const specs = [_]thindb.exec.MinMaxStatsSpec{
        .{ .col_idx = 1, .is_min = true, .out_name = "mn" },
        .{ .col_idx = 1, .is_min = false, .out_name = "mx" },
    };
    const maybe_q = try thindb.exec.minMaxStats(allocator, t, &specs);
    try std.testing.expect(maybe_q != null); // shortcut must fire for the nullable column
    var q = maybe_q.?;
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i32, 3), b.values[0].data.int[0]);
    try std.testing.expectEqual(@as(i32, 10), b.values[1].data.int[0]);

    // Ground truth: the null-aware scan path must agree.
    var base = try thindb.scan(allocator, t);
    var gq = try base.aggregate(&.{
        .{ .func = .min, .col = "v", .as = "mn" },
        .{ .func = .max, .col = "v", .as = "mx" },
    });
    defer gq.deinit();
    const gb = (try gq.next()).?;
    try std.testing.expectEqual(gb.values[0].data.int[0], b.values[0].data.int[0]);
    try std.testing.expectEqual(gb.values[1].data.int[0], b.values[1].data.int[0]);
}

/// `int RegionID-like group` + `nullable bigint UserID-like` distinct value:
/// the exact shape that routes through the combined COUNT(DISTINCT int) kernel
/// (int_layout group path + ≤64-bit int distinct value). Covers the cross-group
/// collision (value 100 in two regions must count once *per region*), NULL
/// exclusion, an all-NULL group (count 0), and multi-batch input (row_group=2).
const cd_schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "r", .type = .int },
        .{ .name = "u", .type = .bigint, .nullable = true },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const cd_ok = [_][]const u8{"id"};
const cd_opts = thindb.TableOptions{ .order_key = &cd_ok, .unique = true, .row_group_size = 2 };

fn cdBigint(b: thindb.exec.Batch, r: i32) ?i64 {
    for (0..b.row_count) |i| {
        if (b.values[0].data.int[i] == r) return b.values[1].data.bigint[i];
    }
    return null;
}

test "aggregate: combined COUNT(DISTINCT int) by int group — exact per-group counts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", cd_schema, cd_opts);
    // r=1: u={100,100,200,NULL} → distinct 2. r=2: u={100,300,300} → distinct 2
    //   (the shared value 100 must NOT collapse across regions). r=3: u={NULL,
    //   NULL} → distinct 0. r=4: u={500} → distinct 1.
    try t.insert(&.{
        .{ .id = @as(i64, 1), .r = @as(i32, 1), .u = @as(i64, 100) },
        .{ .id = @as(i64, 2), .r = @as(i32, 1), .u = @as(i64, 100) },
        .{ .id = @as(i64, 3), .r = @as(i32, 1), .u = @as(i64, 200) },
        .{ .id = @as(i64, 4), .r = @as(i32, 1), .u = @as(?i64, null) },
        .{ .id = @as(i64, 5), .r = @as(i32, 2), .u = @as(i64, 100) },
        .{ .id = @as(i64, 6), .r = @as(i32, 2), .u = @as(i64, 300) },
        .{ .id = @as(i64, 7), .r = @as(i32, 2), .u = @as(i64, 300) },
        .{ .id = @as(i64, 8), .r = @as(i32, 3), .u = @as(?i64, null) },
        .{ .id = @as(i64, 9), .r = @as(i32, 3), .u = @as(?i64, null) },
        .{ .id = @as(i64, 10), .r = @as(i32, 4), .u = @as(i64, 500) },
    });
    try t.flush();

    // Full grouped emit: every per-group distinct count is exact.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.groupBy(&.{"r"}, &.{
            .{ .func = .count_distinct, .col = "u", .as = "u" },
        });
        defer q.deinit();
        var rows_seen: usize = 0;
        while (try q.next()) |b| {
            for (0..b.row_count) |_| rows_seen += 1;
            try std.testing.expectEqual(@as(?i64, 2), cdBigint(b, 1));
            try std.testing.expectEqual(@as(?i64, 2), cdBigint(b, 2));
            try std.testing.expectEqual(@as(?i64, 0), cdBigint(b, 3));
            try std.testing.expectEqual(@as(?i64, 1), cdBigint(b, 4));
        }
        try std.testing.expectEqual(@as(usize, 4), rows_seen);
    }

    // Q08 shape: ORDER BY u DESC LIMIT 2 → the two count-2 regions {1, 2}; the
    // top-k heap must read the combined counter, not the (empty) AccState set.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.groupByTopK(&.{"r"}, &.{
            .{ .func = .count_distinct, .col = "u", .as = "u" },
        }, .{ .k = 2, .keys = &.{.{ .col = "u", .desc = true }} }, null);
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 2), b.row_count);
        try std.testing.expectEqual(@as(?i64, 2), cdBigint(b, 1));
        try std.testing.expectEqual(@as(?i64, 2), cdBigint(b, 2));
        try std.testing.expectEqual(@as(?i64, null), cdBigint(b, 3));
        try std.testing.expectEqual(@as(?i64, null), cdBigint(b, 4));
    }
}

test "aggregate: combined distinct alongside other aggregates (Q09 shape)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", cd_schema, cd_opts);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .r = @as(i32, 1), .u = @as(i64, 7) },
        .{ .id = @as(i64, 2), .r = @as(i32, 1), .u = @as(i64, 7) },
        .{ .id = @as(i64, 3), .r = @as(i32, 1), .u = @as(i64, 9) },
        .{ .id = @as(i64, 4), .r = @as(i32, 2), .u = @as(i64, 5) },
    });
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var q = try base.groupBy(&.{"r"}, &.{
        .{ .func = .count, .as = "c" },
        .{ .func = .sum, .col = "u", .as = "s" },
        .{ .func = .count_distinct, .col = "u", .as = "nd" },
    });
    defer q.deinit();
    var rows_seen: usize = 0;
    while (try q.next()) |b| {
        for (0..b.row_count) |i| {
            rows_seen += 1;
            const r = b.values[0].data.int[i];
            const c = b.values[1].data.bigint[i];
            const s = b.values[2].data.bigint[i];
            const nd = b.values[3].data.bigint[i];
            if (r == 1) {
                try std.testing.expectEqual(@as(i64, 3), c);
                try std.testing.expectEqual(@as(i64, 23), s); // 7+7+9
                try std.testing.expectEqual(@as(i64, 2), nd); // {7, 9}
            } else if (r == 2) {
                try std.testing.expectEqual(@as(i64, 1), c);
                try std.testing.expectEqual(@as(i64, 5), s);
                try std.testing.expectEqual(@as(i64, 1), nd);
            } else return error.UnexpectedGroup;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), rows_seen);
}

const cd_str_schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "s", .type = .string },
        .{ .name = "u", .type = .bigint, .nullable = true },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const cd_str_ok = [_][]const u8{"id"};
const cd_str_opts = thindb.TableOptions{ .order_key = &cd_str_ok, .unique = true, .row_group_size = 2 };

test "aggregate: combined distinct under a string group (Q13 byte-group + int-distinct combo)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", cd_str_schema, cd_str_opts);
    // String group keys (byte-table path) with the combined int-distinct kernel
    // running on top — the same value reused across distinct string groups must
    // stay separated by group gid.
    try t.insert(&.{
        .{ .id = @as(i64, 1), .s = "alpha", .u = @as(i64, 1) },
        .{ .id = @as(i64, 2), .s = "alpha", .u = @as(i64, 1) },
        .{ .id = @as(i64, 3), .s = "alpha", .u = @as(i64, 2) },
        .{ .id = @as(i64, 4), .s = "beta", .u = @as(i64, 1) },
        .{ .id = @as(i64, 5), .s = "beta", .u = @as(?i64, null) },
        .{ .id = @as(i64, 6), .s = "gamma", .u = @as(?i64, null) },
    });
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var q = try base.groupBy(&.{"s"}, &.{
        .{ .func = .count_distinct, .col = "u", .as = "u" },
    });
    defer q.deinit();
    var rows_seen: usize = 0;
    while (try q.next()) |b| {
        for (0..b.row_count) |i| {
            rows_seen += 1;
            const s = b.values[0].data.string.rowBytes(i);
            const nd = b.values[1].data.bigint[i];
            if (std.mem.eql(u8, s, "alpha")) {
                try std.testing.expectEqual(@as(i64, 2), nd); // {1, 2}
            } else if (std.mem.eql(u8, s, "beta")) {
                try std.testing.expectEqual(@as(i64, 1), nd); // {1}, NULL excluded
            } else if (std.mem.eql(u8, s, "gamma")) {
                try std.testing.expectEqual(@as(i64, 0), nd); // all NULL
            } else return error.UnexpectedGroup;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), rows_seen);
}

// Adaptive group-table sizing: the presize is a provable *ceiling*, and the
// table starts modest, jumping to that ceiling only on an actual overflow.
const adaptive_schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        // High-NDV group key (distinct per row) — yields a large ceiling estimate.
        .{ .name = "k", .type = .int },
        // Filter selector with few distinct values — a selective predicate on it
        // leaves only a tiny subset of `k` groups behind (the Q40 shape).
        .{ .name = "sel", .type = .int },
        .{ .name = "v", .type = .bigint },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const adaptive_ok = [_][]const u8{"id"};
const adaptive_opts = thindb.TableOptions{ .order_key = &adaptive_ok, .unique = true, .row_group_size = 1024 };

const AdaptiveRow = struct { id: i64, k: i32, sel: i32, v: i64 };

test "aggregate: high-NDV key behind a selective filter stays under the adaptive initial size" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", adaptive_schema, adaptive_opts);

    // 20_000 rows with a per-row-distinct `k` (NDV ≈ 20_000 → a multi-thousand
    // ceiling), but `sel` cycles 0..9. Filtering to a single `sel` bucket leaves
    // only ~2_000 actual groups — far under ADAPTIVE_INITIAL (65_536), so the
    // group table never overflows its modest initial size.
    const n_rows = 20_000;
    const rows = try allocator.alloc(AdaptiveRow, n_rows);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| {
        const ii: i64 = @intCast(i);
        r.* = .{ .id = ii, .k = @intCast(i), .sel = @intCast(@mod(i, 10)), .v = ii };
    }
    try t.insert(rows);
    try t.flush();

    // Reference: groups in `sel == 3` are k = 3, 13, 23, … ; each appears once,
    // so its COUNT(*) is 1 and SUM(v) is the row's own id (== k here).
    var expected_groups: usize = 0;
    var i: usize = 3;
    while (i < n_rows) : (i += 10) expected_groups += 1;

    var base = try thindb.scan(allocator, t);
    var filtered = try base.filter(thindb.leafExpr("sel", .eq, .{ .int = 3 }));
    var q = try filtered.groupBy(&.{"k"}, &.{
        .{ .func = .count, .as = "c" },
        .{ .func = .sum, .col = "v", .as = "s" },
    });
    defer q.deinit();

    var groups_seen: usize = 0;
    while (try q.next()) |b| {
        for (0..b.row_count) |row| {
            groups_seen += 1;
            const k = b.values[0].data.int[row];
            const c = b.values[1].data.bigint[row];
            const s = b.values[2].data.bigint[row];
            try std.testing.expectEqual(@as(i32, 3), @mod(k, 10)); // only sel==3 keys
            try std.testing.expectEqual(@as(i64, 1), c);
            try std.testing.expectEqual(@as(i64, k), s); // v == id == k
        }
    }
    try std.testing.expectEqual(expected_groups, groups_seen);
}

test "aggregate: genuinely high-card group-by overflows the initial size and jumps to the ceiling" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", adaptive_schema, adaptive_opts);

    // More distinct group keys than ADAPTIVE_INITIAL (65_536): the int group
    // table fills its modest initial size and must grow once, straight to the
    // ceiling. Each key appears exactly twice so COUNT(*) per group is 2 and the
    // emitted group count equals the distinct-key count — a full-coverage check
    // that the grow re-inserted every entry correctly.
    const n_keys: usize = 66_000;
    const rows = try allocator.alloc(AdaptiveRow, n_keys * 2);
    defer allocator.free(rows);
    for (0..n_keys) |key| {
        const a = key * 2;
        const b = a + 1;
        rows[a] = .{ .id = @intCast(a), .k = @intCast(key), .sel = 0, .v = @intCast(key) };
        rows[b] = .{ .id = @intCast(b), .k = @intCast(key), .sel = 0, .v = @intCast(key) };
    }
    try t.insert(rows);
    try t.flush();

    var base = try thindb.scan(allocator, t);
    // SUM(v) (not a bare COUNT) keeps this on the generic int-table path rather
    // than the count-in-slot fast path, so the adaptive grow is exercised.
    var q = try base.groupBy(&.{"k"}, &.{
        .{ .func = .count, .as = "c" },
        .{ .func = .sum, .col = "v", .as = "s" },
    });
    defer q.deinit();

    var groups_seen: usize = 0;
    var count_sum: u64 = 0;
    while (try q.next()) |b| {
        for (0..b.row_count) |row| {
            groups_seen += 1;
            const k = b.values[0].data.int[row];
            const c = b.values[1].data.bigint[row];
            const s = b.values[2].data.bigint[row];
            count_sum += @intCast(c);
            try std.testing.expectEqual(@as(i64, 2), c); // each key inserted twice
            try std.testing.expectEqual(@as(i64, k) * 2, s); // v == k, summed twice
        }
    }
    try std.testing.expectEqual(n_keys, groups_seen);
    try std.testing.expectEqual(@as(u64, n_keys * 2), count_sum);
}

test "aggregate: count_if bool bit value and distinct numeric additions" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "x", .type = .int, .nullable = true },
            .{ .name = "flag", .type = .boolean, .nullable = true },
            .{ .name = "bits", .type = .int, .nullable = true },
            .{ .name = "tag", .type = .string, .nullable = true },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &ok, .unique = true, .row_group_size = 8 });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .x = @as(?i32, 1), .flag = @as(?bool, true), .bits = @as(?i32, 7), .tag = @as(?[]const u8, "a") },
        .{ .id = @as(i64, 2), .x = @as(?i32, 2), .flag = @as(?bool, false), .bits = @as(?i32, 3), .tag = @as(?[]const u8, null) },
        .{ .id = @as(i64, 3), .x = @as(?i32, 2), .flag = @as(?bool, null), .bits = @as(?i32, null), .tag = @as(?[]const u8, "b") },
        .{ .id = @as(i64, 4), .x = @as(?i32, null), .flag = @as(?bool, true), .bits = @as(?i32, 1), .tag = @as(?[]const u8, "c") },
    });
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var q = try base.aggregate(&.{
        .{ .func = .count_if, .col = "flag", .as = "ct" },
        .{ .func = .bool_and, .col = "flag", .as = "ba" },
        .{ .func = .bool_or, .col = "flag", .as = "bo" },
        .{ .func = .bit_and, .col = "bits", .as = "band" },
        .{ .func = .bit_or, .col = "bits", .as = "bor" },
        .{ .func = .bit_xor, .col = "bits", .as = "bxor" },
        .{ .func = .sum_distinct, .col = "x", .as = "sd" },
        .{ .func = .avg_distinct, .col = "x", .as = "ad" },
        .{ .func = .any_value, .col = "tag", .as = "any" },
        .{ .func = .first, .col = "tag", .as = "first" },
        .{ .func = .last, .col = "tag", .as = "last" },
    });
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 2), b.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(u8, 0), b.values[1].data.boolean[0]);
    try std.testing.expectEqual(@as(u8, 1), b.values[2].data.boolean[0]);
    try std.testing.expectEqual(@as(i64, 1), b.values[3].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 7), b.values[4].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 5), b.values[5].data.bigint[0]);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), b.values[6].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 1.5), b.values[7].data.double[0], 1e-9);
    try std.testing.expectEqualStrings("a", b.values[8].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("a", b.values[9].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("c", b.values[10].data.string.rowBytes(0));
}

test "aggregate: new aggregates honor all-NULL inputs" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "x", .type = .int, .nullable = true },
            .{ .name = "flag", .type = .boolean, .nullable = true },
            .{ .name = "bits", .type = .int, .nullable = true },
            .{ .name = "tag", .type = .string, .nullable = true },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &ok, .unique = true, .row_group_size = 8 });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .x = @as(?i32, null), .flag = @as(?bool, null), .bits = @as(?i32, null), .tag = @as(?[]const u8, null) },
        .{ .id = @as(i64, 2), .x = @as(?i32, null), .flag = @as(?bool, null), .bits = @as(?i32, null), .tag = @as(?[]const u8, null) },
    });
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var q = try base.aggregate(&.{
        .{ .func = .count_if, .col = "flag", .as = "ct" },
        .{ .func = .bool_and, .col = "flag", .as = "ba" },
        .{ .func = .bit_or, .col = "bits", .as = "bor" },
        .{ .func = .sum_distinct, .col = "x", .as = "sd" },
        .{ .func = .avg_distinct, .col = "x", .as = "ad" },
        .{ .func = .first, .col = "tag", .as = "first" },
    });
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 0), b.values[0].data.bigint[0]);
    try std.testing.expect(!b.values[1].isValid(0));
    try std.testing.expect(!b.values[2].isValid(0));
    try std.testing.expect(!b.values[3].isValid(0));
    try std.testing.expect(!b.values[4].isValid(0));
    try std.testing.expect(!b.values[5].isValid(0));
}

// SUM over integers is BIGINT and wraps like StarRocks (DESIGN.md §3.4). Group
// g=1 sums 3·9e18 and g=2 sums 2·5e18, both past BIGINT; g=4 / hk ≥ 100 are
// zero-valued filler rows that make `hk` high-cardinality.
const wrap_schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "g", .type = .int },
        .{ .name = "hk", .type = .int },
        .{ .name = "v", .type = .bigint, .nullable = true },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const wrap_ok = [_][]const u8{"id"};
const WrapRow = struct { id: i64, g: i32, hk: i32, v: ?i64 };
const WRAP_FILLER_ROWS = 70_000;
const WRAP_NON_NULL_ROWS = 7 + WRAP_FILLER_ROWS;
const WRAP_G1: i64 = 8553255926290448384;
const WRAP_G2: i64 = -8446744073709551616;
const WRAP_G3: i64 = 3;
const WRAP_TOTAL: i64 = 106511852580896771;

const KeySum = struct { k: i64, s: ?i64 };

fn collectKeySums(allocator: std.mem.Allocator, q: anytype) ![]KeySum {
    var out: std.ArrayList(KeySum) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        for (0..b.row_count) |r| {
            const k: i64 = switch (b.values[0].data) {
                .int => |d| d[r],
                .bigint => |d| d[r],
                else => return error.UnexpectedType,
            };
            try out.append(allocator, .{ .k = k, .s = if (b.values[1].isValid(r)) b.values[1].data.bigint[r] else null });
        }
    }
    return out.toOwnedSlice(allocator);
}

fn expectKeySums(expected: []const KeySum, actual: []const KeySum) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |e, a| try std.testing.expectEqual(e, a);
}

fn expectKeySet(expected: []const i64, actual: []const KeySum) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected) |k| {
        var found = false;
        for (actual) |a| found = found or a.k == k;
        try std.testing.expect(found);
    }
}

fn expectAllHkGroups(pairs: []const KeySum) !void {
    try std.testing.expectEqual(@as(usize, 3 + WRAP_FILLER_ROWS), pairs.len);
    for (pairs) |p| {
        const want: ?i64 = switch (p.k) {
            1 => WRAP_G1,
            2 => WRAP_G2,
            3 => WRAP_G3,
            else => 0,
        };
        try std.testing.expectEqual(want, p.s);
    }
}

fn expectSqlKeySums(allocator: std.mem.Allocator, db: anytype, sql: []const u8, expected: []const KeySum) !void {
    var q = try helpers.runSql(allocator, db, sql);
    defer q.deinit();
    const got = try collectKeySums(allocator, &q);
    defer allocator.free(got);
    errdefer std.debug.print("query: {s}\n", .{sql});
    try expectKeySums(expected, got);
}

test "aggregate: SUM(BIGINT) wraps identically on every aggregate path" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const sum_v = [_]thindb.exec.AggSpec{.{ .func = .sum, .col = "v", .as = "s" }};

    for ([_]bool{ false, true }) |flushed| {
        errdefer std.debug.print("flushed={}\n", .{flushed});
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{
            .auto_flush_rows = std.math.maxInt(u64),
            .auto_flush_bytes = std.math.maxInt(usize),
            .auto_flush_secs = 0,
        });
        defer db.close();
        const t = try db.table("w", wrap_schema, .{ .order_key = &wrap_ok, .unique = true, .row_group_size = 4096 });

        const head = [_]WrapRow{
            .{ .id = 1, .g = 1, .hk = 1, .v = 9_000_000_000_000_000_000 },
            .{ .id = 2, .g = 1, .hk = 1, .v = 9_000_000_000_000_000_000 },
            .{ .id = 3, .g = 1, .hk = 1, .v = 9_000_000_000_000_000_000 },
            .{ .id = 4, .g = 2, .hk = 2, .v = 5_000_000_000_000_000_000 },
            .{ .id = 5, .g = 2, .hk = 2, .v = 5_000_000_000_000_000_000 },
            .{ .id = 6, .g = 3, .hk = 3, .v = 1 },
            .{ .id = 7, .g = 3, .hk = 3, .v = 2 },
            .{ .id = 8, .g = 3, .hk = 3, .v = null },
        };
        const rows = try allocator.alloc(WrapRow, head.len + WRAP_FILLER_ROWS);
        defer allocator.free(rows);
        @memcpy(rows[0..head.len], &head);
        for (rows[head.len..], 0..) |*r, i| r.* = .{ .id = @intCast(100 + i), .g = 4, .hk = @intCast(100 + i), .v = 0 };
        try t.insert(rows);
        if (flushed) try t.flush();

        // Global, plain and affine (SUM(v±k) reduces to SUM(v) ± k·COUNT(v)).
        {
            var q = try helpers.runSql(allocator, db, "SELECT SUM(v), SUM(v + 1), SUM(v - 1) FROM w");
            defer q.deinit();
            const b = (try q.next()).?;
            try std.testing.expectEqual(WRAP_TOTAL, b.values[0].data.bigint[0]);
            try std.testing.expectEqual(WRAP_TOTAL + WRAP_NON_NULL_ROWS, b.values[1].data.bigint[0]);
            try std.testing.expectEqual(WRAP_TOTAL - WRAP_NON_NULL_ROWS, b.values[2].data.bigint[0]);
        }

        // Grouped, small cardinality: plain, ranked by the wrapped value, and
        // filtered on it.
        try expectSqlKeySums(allocator, db, "SELECT g, SUM(v) AS s FROM w GROUP BY g ORDER BY g", &.{
            .{ .k = 1, .s = WRAP_G1 }, .{ .k = 2, .s = WRAP_G2 }, .{ .k = 3, .s = WRAP_G3 }, .{ .k = 4, .s = 0 },
        });
        try expectSqlKeySums(allocator, db, "SELECT g, SUM(v) AS s FROM w GROUP BY g ORDER BY s DESC LIMIT 2", &.{
            .{ .k = 1, .s = WRAP_G1 }, .{ .k = 3, .s = WRAP_G3 },
        });
        try expectSqlKeySums(allocator, db, "SELECT g, SUM(v) AS s FROM w GROUP BY g HAVING SUM(v) < 0", &.{
            .{ .k = 2, .s = WRAP_G2 },
        });

        // Grouped affine: three SUMs collapse to one {SUM(v), COUNT(v)} base set.
        {
            var q = try helpers.runSql(allocator, db, "SELECT g, SUM(v), SUM(v + 1), SUM(v + 2) FROM w GROUP BY g ORDER BY g");
            defer q.deinit();
            const want = [_][3]i64{
                .{ WRAP_G1, WRAP_G1 + 3, WRAP_G1 + 6 },
                .{ WRAP_G2, WRAP_G2 + 2, WRAP_G2 + 4 },
                .{ WRAP_G3, WRAP_G3 + 2, WRAP_G3 + 4 },
                .{ 0, WRAP_FILLER_ROWS, 2 * WRAP_FILLER_ROWS },
            };
            var row: usize = 0;
            while (try q.next()) |b| {
                for (0..b.row_count) |r| {
                    for (want[row], 1..) |w, c| try std.testing.expectEqual(w, b.values[c].data.bigint[r]);
                    row += 1;
                }
            }
            try std.testing.expectEqual(want.len, row);
        }

        // Grouped, high cardinality.
        try expectSqlKeySums(allocator, db, "SELECT hk, SUM(v) AS s FROM w GROUP BY hk ORDER BY s DESC LIMIT 2", &.{
            .{ .k = 1, .s = WRAP_G1 }, .{ .k = 3, .s = WRAP_G3 },
        });
        try expectSqlKeySums(allocator, db, "SELECT hk, SUM(v) AS s FROM w GROUP BY hk HAVING SUM(v) < 0", &.{
            .{ .k = 2, .s = WRAP_G2 },
        });
        try expectSqlKeySums(allocator, db, "SELECT hk, SUM(v) AS s FROM w GROUP BY hk ORDER BY hk LIMIT 3", &.{
            .{ .k = 1, .s = WRAP_G1 }, .{ .k = 2, .s = WRAP_G2 }, .{ .k = 3, .s = WRAP_G3 },
        });

        // Each operator directly, so no router choice hides a path.
        {
            var base = try thindb.scan(allocator, t);
            var q = try base.aggregate(&sum_v);
            defer q.deinit();
            try std.testing.expectEqual(WRAP_TOTAL, (try q.next()).?.values[0].data.bigint[0]);
        }
        {
            var base = try thindb.scan(allocator, t);
            var q = try base.groupBy(&.{"hk"}, &sum_v);
            defer q.deinit();
            const got = try collectKeySums(allocator, &q);
            defer allocator.free(got);
            try expectAllHkGroups(got);
        }
        {
            var base = try thindb.scan(allocator, t);
            var q = try base.groupByTopK(&.{"hk"}, &sum_v, .{ .k = 2, .keys = &.{.{ .col = "s", .desc = true }} }, null);
            defer q.deinit();
            const got = try collectKeySums(allocator, &q);
            defer allocator.free(got);
            try expectKeySet(&.{ 1, 3 }, got);
        }
        {
            var base = try thindb.scan(allocator, t);
            var q = try base.radixGroupBy(&.{"hk"}, &sum_v, null);
            defer q.deinit();
            const got = try collectKeySums(allocator, &q);
            defer allocator.free(got);
            try expectAllHkGroups(got);
        }
        {
            var base = try thindb.scan(allocator, t);
            var q = try base.radixGroupBy(&.{"hk"}, &sum_v, .{ .k = 2, .col = "s", .desc = true });
            defer q.deinit();
            const got = try collectKeySums(allocator, &q);
            defer allocator.free(got);
            try expectKeySet(&.{ 1, 3 }, got);
        }
    }
}
