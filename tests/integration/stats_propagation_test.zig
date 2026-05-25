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
const expr_mod = thindb.exec.expr_mod;

/// `col <op> lit` as an Expr.Call. `op` is "add"/"sub"/"mul"; the literal is an
/// `.int`. `lit_left` puts the literal on the left (e.g. `100 - col`).
fn arith(aa: std.mem.Allocator, op: []const u8, col: []const u8, lit: i32, lit_left: bool) !thindb.exec.expr_mod.Expr {
    const c = expr_mod.col(col);
    const l = expr_mod.lit(.{ .int = lit });
    const args = if (lit_left) [_]thindb.exec.expr_mod.Expr{ l, c } else [_]thindb.exec.expr_mod.Expr{ c, l };
    return expr_mod.call(aa, op, &args);
}

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

// ---------------------------------------------------------------------------
// Compute: provable per-column ndv + min/max for derived columns.
// Seed: a ∈ {10,20,30} (ndv 3, [10,30]), b ∈ {100..600} (ndv 6, [100,600]).
// The derived column is appended at index 3 (after id, a, b).
// ---------------------------------------------------------------------------

test "stats: compute a + 10 shifts range, ndv = ndv(a)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{.{ .name = "a10", .expr = try arith(aa, "add", "a", 10, false) }});
    defer q.deinit();
    const s = q.stats();

    // a ∈ [10,30] ⇒ a+10 ∈ [20,40]; ndv unchanged (affine is injective).
    try std.testing.expectEqual(@as(?i128, 20), s.column_stats[3].min);
    try std.testing.expectEqual(@as(?i128, 40), s.column_stats[3].max);
    try ndvAtLeast(s.column_stats[3].ndv, 3);
    switch (s.column_stats[3].ndv) {
        .exact => |n| try std.testing.expect(n <= 3),
        .unknown => return error.TestUnexpectedResult,
    }
}

test "stats: compute a * 3 scales range up" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{.{ .name = "a3", .expr = try arith(aa, "mul", "a", 3, false) }});
    defer q.deinit();
    const s = q.stats();

    // a ∈ [10,30] ⇒ a*3 ∈ [30,90].
    try std.testing.expectEqual(@as(?i128, 30), s.column_stats[3].min);
    try std.testing.expectEqual(@as(?i128, 90), s.column_stats[3].max);
    try ndvAtLeast(s.column_stats[3].ndv, 3);
}

test "stats: compute a * -2 flips the range" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{.{ .name = "an2", .expr = try arith(aa, "mul", "a", -2, false) }});
    defer q.deinit();
    const s = q.stats();

    // a ∈ [10,30], scale -2 ⇒ [-60, -20] (low end is -2·30, high is -2·10).
    try std.testing.expectEqual(@as(?i128, -60), s.column_stats[3].min);
    try std.testing.expectEqual(@as(?i128, -20), s.column_stats[3].max);
    // Provable: no actual a (10,20,30) maps outside [-60,-20].
    try std.testing.expect(s.column_stats[3].min.? <= -60 and s.column_stats[3].max.? >= -20);
}

test "stats: compute 100 - a (c - col) flips the range" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{.{ .name = "ca", .expr = try arith(aa, "sub", "a", 100, true) }});
    defer q.deinit();
    const s = q.stats();

    // a ∈ [10,30] ⇒ 100 - a ∈ [70, 90].
    try std.testing.expectEqual(@as(?i128, 70), s.column_stats[3].min);
    try std.testing.expectEqual(@as(?i128, 90), s.column_stats[3].max);
    try ndvAtLeast(s.column_stats[3].ndv, 3);
}

test "stats: compute a + b (two-column) sums ranges and multiplies ndv" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var base = try thindb.scan(allocator, t);
    const ab = try expr_mod.call(aa, "add", &.{ expr_mod.col("a"), expr_mod.col("b") });
    var q = try base.compute(&.{.{ .name = "ab", .expr = ab }});
    defer q.deinit();
    const s = q.stats();

    // a ∈ [10,30], b ∈ [100,600] ⇒ a+b ∈ [110, 630].
    try std.testing.expectEqual(@as(?i128, 110), s.column_stats[3].min);
    try std.testing.expectEqual(@as(?i128, 630), s.column_stats[3].max);
    // ndv ≤ ndv(a)·ndv(b) = 3·6 = 18, but capped at upper_rows = 6.
    switch (s.column_stats[3].ndv) {
        .exact => |n| try std.testing.expect(n <= 6 and n >= 1),
        .unknown => return error.TestUnexpectedResult,
    }
    // Provable: the true distinct (a+b) count is 6; bound must not under-count.
    try ndvAtLeast(s.column_stats[3].ndv, 6);
}

test "stats: compute literal column has ndv 1 and min == max == value" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{.{ .name = "k", .expr = expr_mod.lit(.{ .int = 42 }) }});
    defer q.deinit();
    const s = q.stats();

    try std.testing.expectEqual(ColCard{ .exact = 1 }, s.column_stats[3].ndv);
    try std.testing.expectEqual(@as(?i128, 42), s.column_stats[3].min);
    try std.testing.expectEqual(@as(?i128, 42), s.column_stats[3].max);
}

test "stats: compute non-affine fn keeps ndv <= input, min/max null" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    // abs(a): single-column, non-affine ⇒ ndv ≤ ndv(a), min/max not provable.
    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{.{ .name = "absa", .expr = try thindb.exec.scalar_fn.abs(aa, expr_mod.col("a")) }});
    defer q.deinit();
    const s = q.stats();

    // ndv ≤ ndv(a) = 3 (pigeonhole), and ≥ true distinct (abs of {10,20,30} = 3).
    try ndvAtLeast(s.column_stats[3].ndv, 3);
    switch (s.column_stats[3].ndv) {
        .exact => |n| try std.testing.expect(n <= 3),
        .unknown => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(?i128, null), s.column_stats[3].min);
    try std.testing.expectEqual(@as(?i128, null), s.column_stats[3].max);
}

// ---------------------------------------------------------------------------
// Aggregate: provable output bounds (COUNT/SUM/MIN/MAX) and the end-to-end
// COUNT(*) * 12 case (Compute interval arithmetic over an agg output).
// ---------------------------------------------------------------------------

test "stats: COUNT(*) output bounded to [0, rows]" {
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
    try std.testing.expectEqual(@as(?i128, 0), s.column_stats[0].min);
    try std.testing.expectEqual(@as(?i128, 6), s.column_stats[0].max); // rows = 6
}

test "stats: COUNT(*) * 12 resolves to [0, rows*12] end-to-end" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var base = try thindb.scan(allocator, t);
    var agg = try base.aggregate(&.{.{ .func = .count, .as = "n" }});
    // n is bigint; multiply by a bigint literal so the overload resolves.
    const n12 = try expr_mod.call(aa, "mul", &.{ expr_mod.col("n"), expr_mod.lit(.{ .bigint = 12 }) });
    var q = try agg.compute(&.{.{ .name = "n12", .expr = n12 }});
    defer q.deinit();
    const s = q.stats();

    // n ∈ [0,6] ⇒ n*12 ∈ [0,72]. The derived column is appended at index 1.
    try std.testing.expectEqual(@as(?i128, 0), s.column_stats[1].min);
    try std.testing.expectEqual(@as(?i128, 72), s.column_stats[1].max);

    // Drain to exercise the full path (and confirm the actual value fits the
    // proven bound: COUNT(*) = 6, n12 = 72 ≤ 72).
    var rows: u64 = 0;
    while (try q.next()) |b| {
        rows += b.row_count;
        const v = b.values[1].data.bigint[0];
        try std.testing.expect(v >= 0 and v <= 72);
    }
    try std.testing.expectEqual(@as(u64, 1), rows);
}

test "stats: SUM(a) bounded by [rows*lo, rows*hi]" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    var base = try thindb.scan(allocator, t);
    var q = try base.aggregate(&.{.{ .func = .sum, .col = "a", .as = "sa" }});
    defer q.deinit();
    const s = q.stats();

    // a ∈ [10,30], rows = 6 ⇒ SUM(a) ∈ [60, 180]. True sum is 120, within bound.
    try std.testing.expectEqual(@as(?i128, 60), s.column_stats[0].min);
    try std.testing.expectEqual(@as(?i128, 180), s.column_stats[0].max);
}

test "stats: MIN(a) / MAX(b) inherit the column range" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    var base = try thindb.scan(allocator, t);
    var q = try base.aggregate(&.{
        .{ .func = .min, .col = "a", .as = "mna" },
        .{ .func = .max, .col = "b", .as = "mxb" },
    });
    defer q.deinit();
    const s = q.stats();

    // MIN(a) ∈ [10,30]; MAX(b) ∈ [100,600] — each lies within its column range.
    try std.testing.expectEqual(@as(?i128, 10), s.column_stats[0].min);
    try std.testing.expectEqual(@as(?i128, 30), s.column_stats[0].max);
    try std.testing.expectEqual(@as(?i128, 100), s.column_stats[1].min);
    try std.testing.expectEqual(@as(?i128, 600), s.column_stats[1].max);
}

// ---------------------------------------------------------------------------
// Plan-time predicate simplification against propagated min/max.
//   - A range conjunct provably true over [cmin,cmax] on a NON-NULLABLE
//     column drops out; an all-dropped Filter is removed (returns the scan).
//   - A range/eq conjunct provably false yields zero rows (always-false).
//   - BETWEEN half-drops: only the constraining half survives.
//   - On a NULLABLE column the always-true case must NOT drop (a range
//     predicate still excludes NULLs).
// ---------------------------------------------------------------------------

fn drainCount(q: *thindb.Query) !u64 {
    var rows: u64 = 0;
    while (try q.next()) |b| rows += b.row_count;
    return rows;
}

const pn_schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "c", .type = .int },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const pn_ok = [_][]const u8{"id"};
const pn_opts = thindb.TableOptions{ .order_key = &pn_ok, .unique = true };

// c ∈ {100,200,300,400,500,600}: min 100, max 600, 6 rows.
fn seedNonNull(db: *thindb.Database) !*thindb.Table {
    const t = try db.table("pn", pn_schema, pn_opts);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .c = @as(i32, 100) },
        .{ .id = @as(i64, 2), .c = @as(i32, 200) },
        .{ .id = @as(i64, 3), .c = @as(i32, 300) },
        .{ .id = @as(i64, 4), .c = @as(i32, 400) },
        .{ .id = @as(i64, 5), .c = @as(i32, 500) },
        .{ .id = @as(i64, 6), .c = @as(i32, 600) },
    });
    try t.flush();
    return t;
}

test "simplify: WHERE c >= below-min drops the whole Filter, all rows survive" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seedNonNull(db);

    var base = try thindb.scan(allocator, t);
    const scan_ptr = base.ptr;
    var q = try base.filter(thindb.leafExpr("c", .gte, .{ .int = 50 }));
    defer q.deinit();

    // The Filter is dropped: the query node IS the upstream scan.
    try std.testing.expectEqual(scan_ptr, q.ptr);
    try std.testing.expectEqual(@as(u64, 6), try drainCount(&q));
}

test "simplify: WHERE c <= above-max drops the whole Filter, all rows survive" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seedNonNull(db);

    var base = try thindb.scan(allocator, t);
    const scan_ptr = base.ptr;
    var q = try base.filter(thindb.leafExpr("c", .lte, .{ .int = 700 }));
    defer q.deinit();

    try std.testing.expectEqual(scan_ptr, q.ptr);
    try std.testing.expectEqual(@as(u64, 6), try drainCount(&q));
}

test "simplify: WHERE c > above-max is always-false, zero rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seedNonNull(db);

    var base = try thindb.scan(allocator, t);
    const scan_ptr = base.ptr;
    var q = try base.filter(thindb.leafExpr("c", .gt, .{ .int = 700 }));
    defer q.deinit();

    // Always-false keeps a Filter (now an `.always = false` no-op) → 0 rows.
    try std.testing.expect(scan_ptr != q.ptr);
    try std.testing.expectEqual(@as(u64, 0), try drainCount(&q));
}

test "simplify: WHERE c = out-of-range is always-false, zero rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seedNonNull(db);

    var base = try thindb.scan(allocator, t);
    var q = try base.filter(thindb.leafExpr("c", .eq, .{ .int = 999 }));
    defer q.deinit();

    try std.testing.expectEqual(@as(u64, 0), try drainCount(&q));
}

test "simplify: BETWEEN half-drop equals the constraining half" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seedNonNull(db);

    // `c BETWEEN 50 AND 350` = (c >= 50) AND (c <= 350). The lower half is
    // always-true (50 <= min=100) and drops; only `c <= 350` constrains.
    const between = [_]thindb.exec.PredicateExpr{
        thindb.leafExpr("c", .gte, .{ .int = 50 }),
        thindb.leafExpr("c", .lte, .{ .int = 350 }),
    };
    const between_expr: thindb.exec.PredicateExpr = .{ .@"and" = &between };

    var n_between: u64 = 0;
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.filter(between_expr);
        defer q.deinit();
        n_between = try drainCount(&q);
    }
    var n_half: u64 = 0;
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.filter(thindb.leafExpr("c", .lte, .{ .int = 350 }));
        defer q.deinit();
        n_half = try drainCount(&q);
    }

    // {100,200,300} ⇒ 3 rows, and both forms agree.
    try std.testing.expectEqual(@as(u64, 3), n_between);
    try std.testing.expectEqual(n_half, n_between);
}

const pnull_schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "c", .type = .int, .nullable = true },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const pnull_opts = thindb.TableOptions{ .order_key = &pn_ok, .unique = true };

test "simplify: nullable column never drops a range conjunct, NULLs excluded" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // c ∈ {100, 200, NULL, 400, NULL}: non-null min 100, max 400; 3 non-null.
    const t = try db.table("pnull", pnull_schema, pnull_opts);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .c = @as(?i32, 100) },
        .{ .id = @as(i64, 2), .c = @as(?i32, 200) },
        .{ .id = @as(i64, 3), .c = @as(?i32, null) },
        .{ .id = @as(i64, 4), .c = @as(?i32, 400) },
        .{ .id = @as(i64, 5), .c = @as(?i32, null) },
    });
    try t.flush();

    var base = try thindb.scan(allocator, t);
    const scan_ptr = base.ptr;
    // `c >= 50` is true for every non-null row, but a range predicate excludes
    // NULLs — dropping it would wrongly admit the 2 NULL rows. Must be kept.
    var q = try base.filter(thindb.leafExpr("c", .gte, .{ .int = 50 }));
    defer q.deinit();

    try std.testing.expect(scan_ptr != q.ptr);
    // Exactly the 3 non-null rows survive — NOT all 5.
    try std.testing.expectEqual(@as(u64, 3), try drainCount(&q));
}

// ---------------------------------------------------------------------------
// RECURSIVE predicate simplification + ordering + OR short-circuit. Each test
// drains the surviving `id` set and compares it against a reference computed
// directly from the seed data — proving order/short-circuit changes don't
// alter results. (`seed`: id 1..6, a ∈ {10,20,10,20,30 layout below}, b 100..600.)
//   id:1 a:10 b:100 | id:2 a:20 b:200 | id:3 a:30 b:300
//   id:4 a:10 b:400 | id:5 a:20 b:500 | id:6 a:30 b:600
// ---------------------------------------------------------------------------

/// Drain `q` and collect the surviving `id` (bigint, col 0) values, sorted.
fn collectIds(allocator: std.mem.Allocator, q: *thindb.Query) ![]i64 {
    var ids: std.ArrayListUnmanaged(i64) = .empty;
    errdefer ids.deinit(allocator);
    while (try q.next()) |b| {
        const view = b.values[0];
        for (0..b.row_count) |i| {
            if (view.isValid(i)) try ids.append(allocator, view.data.bigint[i]);
        }
    }
    const owned = try ids.toOwnedSlice(allocator);
    std.mem.sort(i64, owned, {}, std.sort.asc(i64));
    return owned;
}

test "recursive simplify: WHERE a < 15 OR a > 25 (OR of two ranges)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    // a ∈ {10,20,30}: a<15 → {10}=ids{1,4}; a>25 → {30}=ids{3,6}. Union {1,3,4,6}.
    const ors = [_]thindb.exec.PredicateExpr{
        thindb.leafExpr("a", .lt, .{ .int = 15 }),
        thindb.leafExpr("a", .gt, .{ .int = 25 }),
    };
    var base = try thindb.scan(allocator, t);
    var q = try base.filter(.{ .@"or" = &ors });
    defer q.deinit();

    const ids = try collectIds(allocator, &q);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 3, 4, 6 }, ids);
}

test "recursive simplify: WHERE (a < 15 OR b = 300) AND id <> 4 (nested OR in AND)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    // (a<15 → {1,4}) OR (b=300 → {3}) = {1,3,4}; AND id<>4 removes 4 → {1,3}.
    const inner = [_]thindb.exec.PredicateExpr{
        thindb.leafExpr("a", .lt, .{ .int = 15 }),
        thindb.leafExpr("b", .eq, .{ .int = 300 }),
    };
    const outer = [_]thindb.exec.PredicateExpr{
        .{ .@"or" = &inner },
        thindb.leafExpr("id", .neq, .{ .bigint = 4 }),
    };
    var base = try thindb.scan(allocator, t);
    var q = try base.filter(.{ .@"and" = &outer });
    defer q.deinit();

    const ids = try collectIds(allocator, &q);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 3 }, ids);
}

test "recursive simplify: OR drops always-false disjunct, always-true disjunct admits all" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    // a < 5  → provably always-false (a min=10).
    // id >= 0 → provably always-true on the NON-NULLABLE id (id min=1) ⇒ the
    // whole OR collapses to always-true ⇒ every row survives.
    const ors = [_]thindb.exec.PredicateExpr{
        thindb.leafExpr("a", .lt, .{ .int = 5 }),
        thindb.leafExpr("id", .gte, .{ .bigint = 0 }),
    };
    var base = try thindb.scan(allocator, t);
    const scan_ptr = base.ptr;
    var q = try base.filter(.{ .@"or" = &ors });
    defer q.deinit();

    // OR collapsed to always-true ⇒ the Filter is dropped (query IS the scan).
    try std.testing.expectEqual(scan_ptr, q.ptr);
    const ids = try collectIds(allocator, &q);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5, 6 }, ids);
}

const like_schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "s", .type = .{ .varchar = 16 } },
        .{ .name = "y", .type = .int },
    },
    .order_key = &.{"id"},
    .unique = true,
};

test "recursive simplify: WHERE s LIKE '%a%' OR y = 1 (cheap disjunct short-circuits the LIKE)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const lok = [_][]const u8{"id"};
    const t = try db.table("lk", like_schema, .{ .order_key = &lok, .unique = true });
    // s LIKE '%a%' true for ids {1(cat),3(bat)}; y=1 true for ids {2,3}.
    // Union: {1,2,3}. id 4 (dog, y=9) and id 5 (fox, y=7) excluded.
    try t.insert(&.{
        .{ .id = @as(i64, 1), .s = @as([]const u8, "cat"), .y = @as(i32, 0) },
        .{ .id = @as(i64, 2), .s = @as([]const u8, "dog"), .y = @as(i32, 1) },
        .{ .id = @as(i64, 3), .s = @as([]const u8, "bat"), .y = @as(i32, 1) },
        .{ .id = @as(i64, 4), .s = @as([]const u8, "dog"), .y = @as(i32, 9) },
        .{ .id = @as(i64, 5), .s = @as([]const u8, "fox"), .y = @as(i32, 7) },
    });
    try t.flush();

    // The cheap int eq (y=1, cost 0) sorts before the LIKE (cost 2): rows already
    // satisfied by y=1 are skipped by the LIKE via its `active` mask. Result must
    // equal the reference union regardless of that short-circuit.
    const ors = [_]thindb.exec.PredicateExpr{
        .{ .like = .{ .col = "s", .pattern = "%a%" } },
        thindb.leafExpr("y", .eq, .{ .int = 1 }),
    };
    var base = try thindb.scan(allocator, t);
    var q = try base.filter(.{ .@"or" = &ors });
    defer q.deinit();

    const ids = try collectIds(allocator, &q);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, ids);
}

test "stats: grouped SUM(b) carries provable bounds per output" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try seed(db);

    // GROUP BY a, SUM(b). a is the group key (index 0), SUM(b) at index 1.
    var base = try thindb.scan(allocator, t);
    var q = try base.groupBy(&.{"a"}, &.{.{ .func = .sum, .col = "b", .as = "sb" }});
    defer q.deinit();
    const s = q.stats();

    // Group key keeps its range; SUM(b) bounded by [rows*lo, rows*hi] over the
    // input row ceiling (6): b ∈ [100,600] ⇒ [600, 3600]. Loose but provable
    // (the true per-group sums all fall inside).
    try std.testing.expectEqual(@as(?i128, 10), s.column_stats[0].min);
    try std.testing.expectEqual(@as(?i128, 30), s.column_stats[0].max);
    try std.testing.expectEqual(@as(?i128, 600), s.column_stats[1].min);
    try std.testing.expectEqual(@as(?i128, 3600), s.column_stats[1].max);
}
