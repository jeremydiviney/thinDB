//! Statistical / set-oriented aggregates added in the post-coercion
//! aggregate expansion: stddev_*, var_*, count_distinct, percentile,
//! group_concat. Smoke tests covering the global + grouped paths plus
//! the edge cases (empty input, single value, all-null).

const std = @import("std");
const thindb = @import("thindb");

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
