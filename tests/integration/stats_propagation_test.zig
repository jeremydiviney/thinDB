//! Per-column cardinality + min/max PROPAGATION through the operator
//! pipeline. Builds small pipelines on a known in-memory-then-flushed table
//! and asserts the PROPAGATED stats (`Query.stats().column_stats`) at each
//! stage. Every assertion checks the provable-upper-bound property: the
//! computed ndv is >= the true distinct count in the test data (never an
//! under-count), and min/max never exclude a value that actually survives.

const std = @import("std");
const thindb = @import("thindb");

const exec = thindb.exec;
const ColCard = exec.ColCard;

const schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "a", .type = .int },
        .{ .name = "b", .type = .int },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const ok = [_][]const u8{"id"};
const opts = thindb.TableOptions{ .order_key = &ok, .unique = true };

/// Insert six rows with known per-column distinct counts and ranges:
///   id: 1..6      (6 distinct, min 1, max 6)
///   a:  10,20,30  (3 distinct, min 10, max 30)
///   b:  100..600  (6 distinct, min 100, max 600)
fn seed(db: *thindb.Database) !*thindb.Table {
    const t = try db.table("sp", schema, opts);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .a = @as(i32, 10), .b = @as(i32, 100) },
        .{ .id = @as(i64, 2), .a = @as(i32, 20), .b = @as(i32, 200) },
        .{ .id = @as(i64, 3), .a = @as(i32, 30), .b = @as(i32, 300) },
        .{ .id = @as(i64, 4), .a = @as(i32, 10), .b = @as(i32, 400) },
        .{ .id = @as(i64, 5), .a = @as(i32, 20), .b = @as(i32, 500) },
        .{ .id = @as(i64, 6), .a = @as(i32, 30), .b = @as(i32, 600) },
    });
    try t.flush();
    return t;
}

fn ndvAtLeast(c: ColCard, lower: u32) !void {
    switch (c) {
        .exact => |n| try std.testing.expect(n >= lower),
        .unknown => {}, // unknown is a valid (loosest) upper bound
    }
}

test "stats: scan exposes ndv >= true distinct and exact min/max" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    const s = q.stats();

    try std.testing.expectEqual(@as(u64, 6), s.upper_rows);
    try std.testing.expectEqual(@as(usize, 3), s.column_stats.len);

    // ndv is a provable upper bound: >= the true distinct count per column.
    try ndvAtLeast(s.column_stats[0].ndv, 6); // id
    try ndvAtLeast(s.column_stats[1].ndv, 3); // a
    try ndvAtLeast(s.column_stats[2].ndv, 6); // b

    // min/max surface the manifest's per-column i128 bounds for int columns.
    try std.testing.expectEqual(@as(?i128, 1), s.column_stats[0].min);
    try std.testing.expectEqual(@as(?i128, 6), s.column_stats[0].max);
    try std.testing.expectEqual(@as(?i128, 10), s.column_stats[1].min);
    try std.testing.expectEqual(@as(?i128, 30), s.column_stats[1].max);
    try std.testing.expectEqual(@as(?i128, 100), s.column_stats[2].min);
    try std.testing.expectEqual(@as(?i128, 600), s.column_stats[2].max);
}

test "stats: WHERE a = v pins ndv to 1 and min == max == v" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    var base = try thindb.scan(allocator, t);
    var q = try base.filter(thindb.leafExpr("a", .eq, .{ .int = 20 }));
    defer q.deinit();
    const s = q.stats();

    // The equality column is pinned to one distinct value at v.
    try std.testing.expectEqual(ColCard{ .exact = 1 }, s.column_stats[1].ndv);
    try std.testing.expectEqual(@as(?i128, 20), s.column_stats[1].min);
    try std.testing.expectEqual(@as(?i128, 20), s.column_stats[1].max);

    // upper_rows is unchanged (filter is only provably <= input); other
    // columns' ndv stays capped at upper_rows and a valid upper bound.
    try std.testing.expectEqual(@as(u64, 6), s.upper_rows);
    try ndvAtLeast(s.column_stats[0].ndv, 1);
    try ndvAtLeast(s.column_stats[2].ndv, 1);
    switch (s.column_stats[0].ndv) {
        .exact => |n| try std.testing.expect(n <= 6),
        .unknown => {},
    }
}

test "stats: WHERE a IN (10, 20, 30) bounds ndv at <= 3" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    const set = [_]thindb.Value{ .{ .int = 10 }, .{ .int = 20 }, .{ .int = 30 } };
    const in_expr: thindb.exec.PredicateExpr = .{ .in_set = .{
        .col = "a",
        .values = &set,
        .negate = false,
    } };

    var base = try thindb.scan(allocator, t);
    var q = try base.filter(in_expr);
    defer q.deinit();
    const s = q.stats();

    switch (s.column_stats[1].ndv) {
        .exact => |n| {
            try std.testing.expect(n <= 3); // <= the set size
            try std.testing.expect(n >= 3); // still >= the 3 true distinct values
        },
        .unknown => return error.TestUnexpectedResult,
    }
    // The set's span clamps the range.
    try std.testing.expectEqual(@as(?i128, 10), s.column_stats[1].min);
    try std.testing.expectEqual(@as(?i128, 30), s.column_stats[1].max);
}

test "stats: WHERE b < v clamps max below v" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    var base = try thindb.scan(allocator, t);
    var q = try base.filter(thindb.leafExpr("b", .lt, .{ .int = 350 }));
    defer q.deinit();
    const s = q.stats();

    // max clamped to min(prior max=600, 350) = 350; min untouched.
    try std.testing.expectEqual(@as(?i128, 350), s.column_stats[2].max);
    try std.testing.expectEqual(@as(?i128, 100), s.column_stats[2].min);
    // No surviving b value (100,200,300) exceeds the clamped max.
    try std.testing.expect(s.column_stats[2].max.? >= 300);
}

test "stats: GROUP BY a, b sets upper_rows = min(ndv(a)*ndv(b), rows)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    // Capture the per-key ndvs the scan reports.
    var na: u64 = 0;
    var nb: u64 = 0;
    {
        var q = try thindb.scan(allocator, t);
        defer q.deinit();
        const s = q.stats();
        na = switch (s.column_stats[1].ndv) {
            .exact => |n| n,
            .unknown => 0,
        };
        nb = switch (s.column_stats[2].ndv) {
            .exact => |n| n,
            .unknown => 0,
        };
    }
    try std.testing.expect(na > 0 and nb > 0);

    var base = try thindb.scan(allocator, t);
    var q = try base.groupBy(&.{ "a", "b" }, &.{.{ .func = .count, .as = "n" }});
    defer q.deinit();
    const s = q.stats();

    const expected = @min(na *| nb, @as(u64, 6));
    try std.testing.expectEqual(expected, s.upper_rows);
    // The true distinct (a, b) pairs in the data is 6; the bound must not
    // under-count it.
    try std.testing.expect(s.upper_rows >= 6);

    // Output columns: group keys carry their input stats; the COUNT output
    // is bounded by the group count.
    try std.testing.expectEqual(@as(usize, 3), s.column_stats.len);
    try ndvAtLeast(s.column_stats[0].ndv, 3); // a
    try ndvAtLeast(s.column_stats[1].ndv, 6); // b
    try ndvAtLeast(s.column_stats[2].ndv, 1); // n (COUNT) <= group count
}

test "stats: global aggregate emits exactly one row" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    var base = try thindb.scan(allocator, t);
    var q = try base.aggregate(&.{.{ .func = .count, .as = "n" }});
    defer q.deinit();
    const s = q.stats();
    try std.testing.expectEqual(@as(u64, 1), s.upper_rows);
}

test "stats: LIMIT n sets upper_rows = min(n, input) and caps ndv" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    {
        var base = try thindb.scan(allocator, t);
        var q = try base.limit(2);
        defer q.deinit();
        const s = q.stats();
        try std.testing.expectEqual(@as(u64, 2), s.upper_rows);
        // Every column's ndv capped at the post-limit bound of 2.
        for (s.column_stats) |cs| switch (cs.ndv) {
            .exact => |n| try std.testing.expect(n <= 2),
            .unknown => {},
        };
    }
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.limit(100);
        defer q.deinit();
        const s = q.stats();
        try std.testing.expectEqual(@as(u64, 6), s.upper_rows);
    }
}

test "stats: end-to-end Scan -> Filter -> GroupBy chain" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    // WHERE a = 10 pins a to 1 distinct; GROUP BY a then yields <= 1 group.
    var base = try thindb.scan(allocator, t);
    var filtered = try base.filter(thindb.leafExpr("a", .eq, .{ .int = 10 }));
    var q = try filtered.groupBy(&.{"a"}, &.{.{ .func = .count, .as = "n" }});
    defer q.deinit();
    const s = q.stats();

    // ndv(a) collapsed to 1 by the filter ⇒ at most one group.
    try std.testing.expectEqual(@as(u64, 1), s.upper_rows);
    try std.testing.expectEqual(@as(usize, 2), s.column_stats.len);
    try std.testing.expectEqual(ColCard{ .exact = 1 }, s.column_stats[0].ndv); // a
    try std.testing.expectEqual(@as(?i128, 10), s.column_stats[0].min);
    try std.testing.expectEqual(@as(?i128, 10), s.column_stats[0].max);

    // Drain so the aggregate executes its full path (no leaks on teardown).
    var rows: u64 = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(u64, 1), rows);
}
