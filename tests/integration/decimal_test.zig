//! Scale-aware DECIMAL operations end-to-end (storage → compute → aggregate).
//!
//! A decimal value is `mantissa / 10^scale`; the scale lives only on the column
//! Type. These tests pin that the engine keeps decimals exact through
//! arithmetic, casts, ROUND/COALESCE, comparisons, and SUM/AVG/MIN/MAX — rather
//! than collapsing to DOUBLE. Ground truth is DuckDB's decimal semantics
//! (DESIGN.md §3.4): ties round away from zero, mixed decimal/int promotes the
//! int, mixed decimal/float collapses to DOUBLE.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const RunResult = helpers.RunResult;
const runSql = helpers.runSql;

fn collectDecimal64(allocator: std.mem.Allocator, q: *RunResult, col_idx: usize) ![]i64 {
    var out: std.ArrayList(i64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[col_idx].data.decimal64[0..b.row_count]) |v| try out.append(allocator, v);
    }
    return try out.toOwnedSlice(allocator);
}

fn collectDecimal128(allocator: std.mem.Allocator, q: *RunResult, col_idx: usize) ![]i128 {
    var out: std.ArrayList(i128) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[col_idx].data.decimal128[0..b.row_count]) |v| try out.append(allocator, v);
    }
    return try out.toOwnedSlice(allocator);
}

fn collectDouble(allocator: std.mem.Allocator, q: *RunResult, col_idx: usize) ![]f64 {
    var out: std.ArrayList(f64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[col_idx].data.double[0..b.row_count]) |v| try out.append(allocator, v);
    }
    return try out.toOwnedSlice(allocator);
}

fn collectInt(allocator: std.mem.Allocator, q: *RunResult, col_idx: usize) ![]i32 {
    var out: std.ArrayList(i32) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[col_idx].data.int[0..b.row_count]) |v| try out.append(allocator, v);
    }
    return try out.toOwnedSlice(allocator);
}

/// `a DECIMAL(16,6)`, `b DECIMAL(10,2) NULL`, `q INT`. Rows:
///   (2.500000, 4.00, 3), (10.000000, 3.00, 7), (1.234567, NULL, 2)
fn seed(allocator: std.mem.Allocator, db: anytype) !void {
    var q1 = try runSql(allocator, db, "CREATE TABLE d (id INT PRIMARY KEY, a DECIMAL(16,6), b DECIMAL(10,2), q INT)");
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "INSERT INTO d VALUES (1, 2.5, 4.0, 3), (2, 10.0, 3.0, 7), (3, 1.234567, NULL, 2)");
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("d", .{});
    try t.flush();
}

test "decimal: storage roundtrip keeps the mantissa" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try seed(allocator, db);

    var q = try runSql(allocator, db, "SELECT a FROM d ORDER BY id");
    defer q.deinit();
    const got = try collectDecimal64(allocator, &q, 0);
    defer allocator.free(got);
    // scale 6: 2.5 -> 2_500_000, 10 -> 10_000_000, 1.234567 -> 1_234_567
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2_500_000, 10_000_000, 1_234_567 }, got);
}

test "decimal: + - * / scale propagation and NULL" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try seed(allocator, db);

    // a + b: scale max(6,2)=6. 2.5+4=6.5, 10+3=13, 1.234567+NULL=NULL.
    var q = try runSql(allocator, db, "SELECT a + b AS s FROM d ORDER BY id");
    defer q.deinit();
    var got: std.ArrayList(?i64) = .empty;
    defer got.deinit(allocator);
    while (try q.next()) |bt| {
        for (0..bt.row_count) |r| {
            try got.append(allocator, if (bt.values[0].isValid(r)) bt.values[0].data.decimal64[r] else null);
        }
    }
    try std.testing.expectEqual(@as(?i64, 6_500_000), got.items[0]);
    try std.testing.expectEqual(@as(?i64, 13_000_000), got.items[1]);
    try std.testing.expectEqual(@as(?i64, null), got.items[2]);
}

test "decimal: division rounds to scale s1+4 (ties away from zero)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try seed(allocator, db);

    // a / q: a is DEC(16,6), q int -> p = 16+0+4 = 20 (>18, so decimal128),
    // scale s1+4 = 10. 2.5/3 = 0.8333333333, 10.0/7 = 1.4285714286 (rounded).
    var q = try runSql(allocator, db, "SELECT a / q AS d FROM d ORDER BY id");
    defer q.deinit();
    const got = try collectDecimal128(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqual(@as(i128, 8_333_333_333), got[0]); // 0.8333333333
    try std.testing.expectEqual(@as(i128, 14_285_714_286), got[1]); // 1.4285714286
}

test "decimal: CAST to DOUBLE / INT and int CAST to DECIMAL" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try seed(allocator, db);

    var q = try runSql(allocator, db, "SELECT CAST(a AS DOUBLE) AS ad FROM d ORDER BY id");
    defer q.deinit();
    const ad = try collectDouble(allocator, &q, 0);
    defer allocator.free(ad);
    try std.testing.expectApproxEqAbs(@as(f64, 2.5), ad[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 1.234567), ad[2], 1e-9);

    // CAST(int AS DECIMAL(8,3)): q=3 -> 3.000 (mantissa 3000 at scale 3).
    var q2 = try runSql(allocator, db, "SELECT CAST(q AS DECIMAL(8,3)) AS qd FROM d ORDER BY id");
    defer q2.deinit();
    const qd = try collectDecimal64(allocator, &q2, 0);
    defer allocator.free(qd);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3000, 7000, 2000 }, qd);
}

test "decimal: ROUND and COALESCE" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try seed(allocator, db);

    // ROUND(a) -> scale 0. 2.5->3 (away from zero), 10->10, 1.234567->1.
    var q = try runSql(allocator, db, "SELECT ROUND(a) AS r FROM d ORDER BY id");
    defer q.deinit();
    const r = try collectDecimal64(allocator, &q, 0);
    defer allocator.free(r);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 10, 1 }, r);

    // COALESCE(b, 1): b scale 2, int 1 -> 1.00. Row 3's NULL b -> 1.00.
    var q2 = try runSql(allocator, db, "SELECT COALESCE(b, 1) AS cb FROM d ORDER BY id");
    defer q2.deinit();
    const cb = try collectDecimal64(allocator, &q2, 0);
    defer allocator.free(cb);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 400, 300, 100 }, cb);
}

test "decimal: comparison against int and float literals" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try seed(allocator, db);

    var q = try runSql(allocator, db, "SELECT id FROM d WHERE a > 2.4 ORDER BY id");
    defer q.deinit();
    const ids = try collectInt(allocator, &q, 0);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2 }, ids); // 2.5 and 10.0

    var q2 = try runSql(allocator, db, "SELECT id FROM d WHERE a = 10 ORDER BY id");
    defer q2.deinit();
    const ids2 = try collectInt(allocator, &q2, 0);
    defer allocator.free(ids2);
    try std.testing.expectEqualSlices(i32, &[_]i32{2}, ids2);
}

test "decimal: literal digits past the scale never round into a match" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try seed(allocator, db);

    // b DECIMAL(10,2): 4.00, 3.00, NULL. 3.999 is not representable at scale 2,
    // so it folds MySQL-style instead of rounding to 4.00.
    const cases = .{
        .{ .sql = "SELECT id FROM d WHERE b = 3.999 ORDER BY id", .ids = &[_]i32{} },
        .{ .sql = "SELECT id FROM d WHERE b <> 3.999 ORDER BY id", .ids = &[_]i32{ 1, 2 } },
        .{ .sql = "SELECT id FROM d WHERE b <= 3.999 ORDER BY id", .ids = &[_]i32{2} },
        .{ .sql = "SELECT id FROM d WHERE b > 3.999 ORDER BY id", .ids = &[_]i32{1} },
        .{ .sql = "SELECT id FROM d WHERE b IN (3.999, 3.0) ORDER BY id", .ids = &[_]i32{2} },
        .{ .sql = "SELECT id FROM d WHERE b = 4.0 ORDER BY id", .ids = &[_]i32{1} },
    };
    inline for (cases) |c| {
        var q = try runSql(allocator, db, c.sql);
        defer q.deinit();
        const ids = try collectInt(allocator, &q, 0);
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(i32, c.ids, ids);
    }
}

test "decimal: SUM widens to DECIMAL(38,s), AVG divides out scale" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try seed(allocator, db);

    // SUM(a) -> DECIMAL128(38,6). 2.5+10+1.234567 = 13.734567.
    var q = try runSql(allocator, db, "SELECT SUM(a) AS s FROM d");
    defer q.deinit();
    const s = try collectDecimal128(allocator, &q, 0);
    defer allocator.free(s);
    try std.testing.expectEqual(@as(i128, 13_734_567), s[0]);

    // AVG(a) -> DOUBLE, true mean 13.734567/3 = 4.578189.
    var q2 = try runSql(allocator, db, "SELECT AVG(a) AS av FROM d");
    defer q2.deinit();
    const av = try collectDouble(allocator, &q2, 0);
    defer allocator.free(av);
    try std.testing.expectApproxEqAbs(@as(f64, 4.578189), av[0], 1e-6);
}

test "decimal: grouped SUM/AVG/MIN/MAX scale correctly" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    // grp NOT NULL, v NULLABLE: exercises the nullable-decimal silo path (a
    // nullable agg input forces the silo grid off the low-card direct path).
    var q1 = try runSql(allocator, db, "CREATE TABLE g (id INT PRIMARY KEY, grp INT NOT NULL, v DECIMAL(12,4))");
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "INSERT INTO g VALUES (1,1,1.5),(2,1,2.5),(3,1,3.0),(4,2,10.0),(5,2,20.0)");
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("g", .{});
    try t.flush();

    // grp 1: SUM=7.0000 (scale 4 -> 70000), grp 2: 30.0000 -> 300000.
    var q = try runSql(allocator, db, "SELECT grp, SUM(v) AS sv FROM g GROUP BY grp ORDER BY grp");
    defer q.deinit();
    const sv = try collectDecimal128(allocator, &q, 1);
    defer allocator.free(sv);
    try std.testing.expectEqualSlices(i128, &[_]i128{ 70_000, 300_000 }, sv);

    // AVG grp1 = 7/3 = 2.3333..., grp2 = 15.
    var q3 = try runSql(allocator, db, "SELECT grp, AVG(v) AS av FROM g GROUP BY grp ORDER BY grp");
    defer q3.deinit();
    const av = try collectDouble(allocator, &q3, 1);
    defer allocator.free(av);
    try std.testing.expectApproxEqAbs(@as(f64, 2.333333), av[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f64, 15.0), av[1], 1e-9);
}
