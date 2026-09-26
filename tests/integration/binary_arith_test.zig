//! Binary arithmetic operators (+ - * / %) in SELECT.
//!
//! The parser lowers these to scalar function calls (add/sub/mul/div/mod)
//! that the existing Compute operator already evaluates. Tests cover:
//!   - basic shapes per operator over BIGINT and DOUBLE columns
//!   - precedence (`*` higher than `+`)
//!   - parenthesized sub-expressions
//!   - composition with the existing scalar-function call syntax
//!   - the natural "delta" pattern from the LAG bench
//!   - `/`, DIV and MOD by zero return NULL
//!   - integer result types and wrapping match StarRocks (DESIGN.md §3.4)

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const RunResult = helpers.RunResult;
const runSql = helpers.runSql;

fn collectBigint(allocator: std.mem.Allocator, q: *RunResult, col_idx: usize) ![]i64 {
    var out: std.ArrayList(i64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[col_idx].data.bigint[0..b.row_count]) |v| try out.append(allocator, v);
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

fn seedSimple(allocator: std.mem.Allocator, db: anytype) !void {
    var q1 = try runSql(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty BIGINT, price DOUBLE)");
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "INSERT INTO t VALUES (1, 10, 1.5), (2, 20, 2.5), (3, 30, 3.5)");
    defer q2.deinit();
    _ = try q2.next();
    const t = try db.openTable("t", .{});
    try t.flush();
}

test "binary arith: column + literal" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(allocator, db, "SELECT qty + 100 AS adj FROM t ORDER BY id ASC");
    defer q.deinit();
    const got = try collectBigint(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 110, 120, 130 }, got);
}

test "binary arith: column - column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(allocator, db, "SELECT qty - id AS delta FROM t ORDER BY id ASC");
    defer q.deinit();
    const got = try collectBigint(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 9, 18, 27 }, got);
}

test "binary arith: column * literal (DOUBLE)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(allocator, db, "SELECT price * 2.0 AS doubled FROM t ORDER BY id ASC");
    defer q.deinit();
    const got = try collectDouble(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqual(@as(usize, 3), got.len);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), got[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 5.0), got[1], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 7.0), got[2], 1e-9);
}

test "binary arith: slash true-divides, DIV truncates" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(allocator, db, "SELECT qty / 7 AS d, qty DIV 7 AS i FROM t ORDER BY id ASC");
    defer q.deinit();
    var n: usize = 0;
    while (try q.next()) |b| {
        for (0..b.row_count) |r| {
            const expected_d = [_]f64{ 10.0 / 7.0, 20.0 / 7.0, 30.0 / 7.0 };
            const expected_i = [_]i64{ 1, 2, 4 };
            try std.testing.expectApproxEqAbs(expected_d[n], b.values[0].data.double[r], 1e-12);
            try std.testing.expectEqual(expected_i[n], b.values[1].data.bigint[r]);
            n += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), n);
}

test "binary arith: modulo" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(allocator, db, "SELECT qty % 7 AS r FROM t ORDER BY id ASC");
    defer q.deinit();
    const got = try collectBigint(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 6, 2 }, got);
}

test "binary arith: precedence — * binds tighter than +" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    // qty + 2 * 3 → qty + 6, NOT (qty + 2) * 3
    var q = try runSql(allocator, db, "SELECT qty + 2 * 3 AS v FROM t ORDER BY id ASC");
    defer q.deinit();
    const got = try collectBigint(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 16, 26, 36 }, got);
}

test "binary arith: parens override precedence" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    // (qty + 2) * 3 → multiplied AFTER addition
    var q = try runSql(allocator, db, "SELECT (qty + 2) * 3 AS v FROM t ORDER BY id ASC");
    defer q.deinit();
    const got = try collectBigint(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 36, 66, 96 }, got);
}

test "binary arith: left-associative chain" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    // qty - 1 - 2 → (qty - 1) - 2 = qty - 3
    var q = try runSql(allocator, db, "SELECT qty - 1 - 2 AS v FROM t ORDER BY id ASC");
    defer q.deinit();
    const got = try collectBigint(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 7, 17, 27 }, got);
}

test "binary arith: nested scalar function call combined with binary op" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    // abs(qty - 25) → row 1: |10-25|=15, row 2: |20-25|=5, row 3: |30-25|=5
    var q = try runSql(allocator, db, "SELECT abs(qty - 25) AS d FROM t ORDER BY id ASC");
    defer q.deinit();
    const got = try collectBigint(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 15, 5, 5 }, got);
}

test "binary arith: scalar call as binary operand" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    // abs(qty - 100) + id → row1: 90+1=91, row2: 80+2=82, row3: 70+3=73
    var q = try runSql(allocator, db, "SELECT abs(qty - 100) + id AS v FROM t ORDER BY id ASC");
    defer q.deinit();
    const got = try collectBigint(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 91, 82, 73 }, got);
}

test "binary arith: division by zero — slash, DIV and MOD return NULL" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(allocator, db, "SELECT qty / 0 AS d, qty DIV 0 AS i, qty % 0 AS r FROM t ORDER BY id ASC");
    defer q.deinit();
    var n: usize = 0;
    while (try q.next()) |b| {
        for (0..b.row_count) |r| {
            try std.testing.expect(!b.values[0].isValid(r));
            try std.testing.expect(!b.values[1].isValid(r));
            try std.testing.expect(!b.values[2].isValid(r));
            n += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), n);
}

// Known limitation: combining binary arithmetic with a window function
// in the same projection item (e.g. `qty - lag(qty) OVER (...)`) is not
// supported. The window operator generates new output columns; a
// Compute step on top of those would be required for the binary
// expression. Today the parser exclusively routes a projection item
// to either `.window` or `.expr`, never both. Workaround: project the
// window result under an alias in an inner subquery (also unsupported
// in the projection-list grammar today) — so for now, real "delta"
// queries either skip the LAG or skip the arithmetic. Tracked as a
// follow-up to either of those grammar extensions.

fn firstIntValue(q: *RunResult) !?i128 {
    const b = (try q.next()) orelse return error.NoRows;
    if (!b.values[0].isValid(0)) return null;
    return switch (b.values[0].data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint => |vals| vals[0],
        else => error.UnexpectedType,
    };
}

// Expected values are what StarRocks 4.0.10 returns for the same query.
test "binary arith: integer result types and wrapping match StarRocks" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const TypeTag = thindb.types.TypeTag;
    const bigint_min: i128 = std.math.minInt(i64);
    const bigint_max: i128 = std.math.maxInt(i64);

    const cases = .{
        .{ "SELECT 2147483647 + 1", TypeTag.bigint, @as(?i128, 2147483648) },
        .{ "SELECT x * 1000000 FROM o", TypeTag.bigint, @as(?i128, 5000000000) },
        .{ "SELECT i + i FROM o", TypeTag.bigint, @as(?i128, 4000000000) },
        .{ "SELECT n + n FROM o", TypeTag.bigint, @as(?i128, 4000000000) },
        .{ "SELECT i * 2 FROM o", TypeTag.bigint, @as(?i128, 4000000000) },
        .{ "SELECT i - 1 FROM o", TypeTag.bigint, @as(?i128, 1999999999) },
        .{ "SELECT s + s FROM o", TypeTag.int, @as(?i128, 65534) },
        .{ "SELECT s + 1 FROM o", TypeTag.int, @as(?i128, 32768) },
        .{ "SELECT -s FROM o", TypeTag.int, @as(?i128, -32767) },
        .{ "SELECT -m FROM o", TypeTag.bigint, @as(?i128, 2147483648) },
        .{ "SELECT abs(m) FROM o", TypeTag.bigint, @as(?i128, 2147483648) },
        .{ "SELECT abs(s) FROM o", TypeTag.int, @as(?i128, 32767) },
        .{ "SELECT 9223372036854775807 + 1", TypeTag.bigint, @as(?i128, bigint_min) },
        .{ "SELECT b + b FROM o", TypeTag.bigint, @as(?i128, -446744073709551616) },
        .{ "SELECT b * 2 FROM o", TypeTag.bigint, @as(?i128, -446744073709551616) },
        .{ "SELECT bm - 2 FROM o", TypeTag.bigint, @as(?i128, bigint_max) },
        .{ "SELECT -(bm - 1) FROM o", TypeTag.bigint, @as(?i128, bigint_min) },
        // StarRocks returns LARGEINT 9223372036854775808 here (DESIGN.md §3.4).
        .{ "SELECT abs(bm - 1) FROM o", TypeTag.bigint, @as(?i128, bigint_min) },
        .{ "SELECT m DIV -1 FROM o", TypeTag.int, @as(?i128, -2147483648) },
        .{ "SELECT (bm - 1) DIV -1 FROM o", TypeTag.bigint, @as(?i128, bigint_min) },
        .{ "SELECT i DIV 7 FROM o", TypeTag.int, @as(?i128, 285714285) },
        .{ "SELECT m % -1 FROM o", TypeTag.int, @as(?i128, 0) },
        .{ "SELECT b % -1 FROM o", TypeTag.bigint, @as(?i128, 0) },
        .{ "SELECT i DIV 0 FROM o", TypeTag.int, @as(?i128, null) },
        .{ "SELECT i % 0 FROM o", TypeTag.int, @as(?i128, null) },
        .{ "SELECT i DIV z FROM o", TypeTag.int, @as(?i128, null) },
        .{ "SELECT b % z FROM o", TypeTag.bigint, @as(?i128, null) },
    };

    for ([_]bool{ false, true }) |flushed| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        try helpers.exec(allocator, db, "CREATE TABLE o (id BIGINT PRIMARY KEY, i INT NOT NULL, n INT, b BIGINT NOT NULL, m INT NOT NULL, bm BIGINT NOT NULL, s SMALLINT NOT NULL, x INT NOT NULL, z INT NOT NULL)");
        try helpers.exec(allocator, db, "INSERT INTO o VALUES (1, 2000000000, 2000000000, 9000000000000000000, -2147483648, -9223372036854775807, 32767, 5000, 0)");
        if (flushed) try (try db.openTable("o", .{})).flush();

        inline for (cases) |c| {
            var q = try runSql(allocator, db, c[0]);
            defer q.deinit();
            errdefer std.debug.print("case: {s} (flushed={})\n", .{ c[0], flushed });
            try std.testing.expectEqual(c[1], std.meta.activeTag(q.outputSchema()[0].type));
            try std.testing.expectEqual(c[2], try firstIntValue(&q));
        }
    }
}
