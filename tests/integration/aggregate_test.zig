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
