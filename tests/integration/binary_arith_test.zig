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
//!   - scientific-notation and leading-dot literals are DOUBLE
//!   - bit operators and BIT_AND/OR/XOR: signed as in StarRocks, BIGINT
//!     UNSIGNED in MySQL
//!   - BIT_COUNT, BIN and CONV, and integers of any width written as text

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

test "binary arith: DIV over DOUBLE and DECIMAL truncates the exact quotient into BIGINT" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const TypeTag = thindb.types.TypeTag;

    // MySQL 8.4 answers every case the same.
    const cases = .{
        .{ "SELECT 5.5 DIV 2", @as(?i128, 2) },
        .{ "SELECT -5.5 DIV 2", @as(?i128, -2) },
        .{ "SELECT 5.5 DIV 0.5", @as(?i128, 11) },
        .{ "SELECT 7.5 DIV 2.5", @as(?i128, 3) },
        .{ "SELECT 7 DIV 2.0", @as(?i128, 3) },
        .{ "SELECT 5.5e0 DIV 2", @as(?i128, 2) },
        .{ "SELECT -7.9e0 DIV 1", @as(?i128, -7) },
        .{ "SELECT 0.3e0 DIV 0.1e0", @as(?i128, 3) },
        .{ "SELECT 9223372036854775807.9 DIV 1", @as(?i128, std.math.maxInt(i64)) },
        .{ "SELECT 1 DIV 0.0", @as(?i128, null) },
        .{ "SELECT d DIV 0.1 FROM f", @as(?i128, 3) },
        .{ "SELECT a DIV d FROM f", @as(?i128, 25) },
        .{ "SELECT a DIV z FROM f", @as(?i128, null) },
        .{ "SELECT d DIV z FROM f", @as(?i128, null) },
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE f (id BIGINT PRIMARY KEY, d DOUBLE NOT NULL, a DECIMAL(10,2) NOT NULL, z DOUBLE NOT NULL)");
    try helpers.exec(allocator, db, "INSERT INTO f VALUES (1, 0.3, 7.50, 0)");
    try (try db.openTable("f", .{})).flush();

    inline for (cases) |c| {
        var q = try runSql(allocator, db, c[0]);
        defer q.deinit();
        errdefer std.debug.print("case: {s}\n", .{c[0]});
        try std.testing.expectEqual(TypeTag.bigint, std.meta.activeTag(q.outputSchema()[0].type));
        try std.testing.expectEqual(c[1], try firstIntValue(&q));
    }

    inline for (.{ "SELECT 99999999999999999999.0 DIV 1", "SELECT 1e30 DIV 1" }) |sql| {
        var q = try runSql(allocator, db, sql);
        defer q.deinit();
        try std.testing.expectError(error.ArithmeticOverflow, q.next());
    }
}

test "binary arith: scientific-notation and leading-dot literals are DOUBLE" {
    // MySQL, StarRocks and DuckDB read `1e3` and `.5` as DOUBLE literals;
    // thinDB read `1e3` as `1 AS e3`.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);
    try helpers.exec(allocator, db, "INSERT INTO t VALUES (4, 10, 2.5E-3), (5, 50, .5e1)");

    const cases = .{
        .{ "SELECT 1e3 FROM t WHERE id = 1", 1000.0 },
        .{ "SELECT -2.5E-3 FROM t WHERE id = 1", -0.0025 },
        .{ "SELECT 1e0 - 1e0 FROM t WHERE id = 1", 0.0 },
        .{ "SELECT 6.02e+23 FROM t WHERE id = 1", 6.02e23 },
        .{ "SELECT .5 + price FROM t WHERE id = 1", 2.0 },
        .{ "SELECT price * 1e2 FROM t WHERE id = 1", 150.0 },
        .{ "SELECT price FROM t WHERE price < 1e-2", 0.0025 },
        .{ "SELECT price FROM t WHERE qty = 2e1", 2.5 },
        .{ "SELECT price FROM t WHERE id = 5", 5.0 },
    };
    inline for (cases) |c| {
        var q = try runSql(allocator, db, c[0]);
        defer q.deinit();
        errdefer std.debug.print("case: {s}\n", .{c[0]});
        try std.testing.expectEqual(thindb.types.TypeTag.double, std.meta.activeTag(q.outputSchema()[0].type));
        const got = try collectDouble(allocator, &q, 0);
        defer allocator.free(got);
        try std.testing.expectEqual(@as(usize, 1), got.len);
        try std.testing.expectApproxEqRel(@as(f64, c[1]), got[0], 1e-12);
    }

    try helpers.expectRunError(allocator, db, "SELECT 1e400 FROM t", error.LexInvalidNumber);
}

test "binary arith: bitwise operators, shifts and MOD bind as in MySQL" {
    // Values probed against MySQL 8.4, except that outside the MySQL
    // dialect the operators are StarRocks' bit functions, whose bits are
    // signed two's complement as in DuckDB and PG: ~10 is -11 and >> keeps
    // the sign. MySQL's unsigned reading is tested below.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    const cases = .{
        .{ "SELECT qty & 12 FROM t ORDER BY id", &[_]i64{ 8, 4, 12 } },
        .{ "SELECT qty | 1 FROM t ORDER BY id", &[_]i64{ 11, 21, 31 } },
        .{ "SELECT ~qty FROM t ORDER BY id", &[_]i64{ -11, -21, -31 } },
        .{ "SELECT qty << 2 FROM t ORDER BY id", &[_]i64{ 40, 80, 120 } },
        .{ "SELECT qty >> 1 FROM t ORDER BY id", &[_]i64{ 5, 10, 15 } },
        .{ "SELECT -qty >> 1 FROM t ORDER BY id", &[_]i64{ -5, -10, -15 } },
        .{ "SELECT qty MOD 7 FROM t ORDER BY id", &[_]i64{ 3, 6, 2 } },
        .{ "SELECT 1 | 2 & 3 FROM t WHERE id = 1", &[_]i64{3} },
        .{ "SELECT 2 + 3 << 1 FROM t WHERE id = 1", &[_]i64{10} },
        .{ "SELECT 1 + 2 | 4 FROM t WHERE id = 1", &[_]i64{7} },
        .{ "SELECT CAST(7 mod 3 + 1 AS BIGINT) FROM t WHERE id = 1", &[_]i64{2} },
        .{ "SELECT 1 << 64 FROM t WHERE id = 1", &[_]i64{0} },
        .{ "SELECT -1 >> 70 FROM t WHERE id = 1", &[_]i64{-1} },
        .{ "SELECT id FROM t WHERE qty & 4 = 4 ORDER BY id", &[_]i64{ 2, 3 } },
        .{ "SELECT id FROM t WHERE qty MOD 20 = 10 ORDER BY id", &[_]i64{ 1, 3 } },
        .{ "SELECT id FROM t WHERE ~qty < -15 ORDER BY id", &[_]i64{ 2, 3 } },
    };
    inline for (cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        var q = try runSql(allocator, db, c[0]);
        defer q.deinit();
        const got = try collectBigint(allocator, &q, 0);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, c[1], got);
    }

    const pg_cases = .{
        .{ "SELECT ~qty FROM t ORDER BY id", &[_]i64{ -11, -21, -31 } },
        .{ "SELECT -qty >> 1 FROM t ORDER BY id", &[_]i64{ -5, -10, -15 } },
        .{ "SELECT -qty | 0 FROM t ORDER BY id", &[_]i64{ -10, -20, -30 } },
    };
    inline for (pg_cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        var q = try helpers.runSqlDialect(allocator, db, c[0], .postgres);
        defer q.deinit();
        const got = try collectBigint(allocator, &q, 0);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, c[1], got);
    }

    // `^` is XOR on MySQL (tested below) and exponentiation on PG and DuckDB.
    var pow = try runSql(allocator, db, "SELECT 2 ^ 3 FROM t WHERE id = 1");
    defer pow.deinit();
    const powered = try collectDouble(allocator, &pow, 0);
    defer allocator.free(powered);
    try std.testing.expectEqualSlices(f64, &.{8.0}, powered);
}

/// `sql`'s first column, run as the MySQL wire runs it, is `want` as text.
fn expectMysqlText(allocator: std.mem.Allocator, db: anytype, sql: []const u8, want: []const ?[]const u8) !void {
    errdefer std.debug.print("case failed: {s}\n", .{sql});
    var q = try helpers.runSqlMysqlSession(allocator, db, sql);
    defer q.deinit();
    try expectColumnText(allocator, &q, want);
}

fn expectColumnText(allocator: std.mem.Allocator, q: *RunResult, want: []const ?[]const u8) !void {
    const got = try helpers.columnText(allocator, q);
    defer helpers.freeStrings(allocator, got);
    try std.testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| {
        if (w) |text| {
            try std.testing.expect(g != null);
            try std.testing.expectEqualStrings(text, g.?);
        } else try std.testing.expect(g == null);
    }
}

test "binary arith: MySQL's bit operators read and return BIGINT UNSIGNED" {
    // Values probed against MySQL 8.4 (#323).
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    const cases = .{
        .{ "SELECT ~qty FROM t ORDER BY id", &[_]?[]const u8{ "18446744073709551605", "18446744073709551595", "18446744073709551585" } },
        .{ "SELECT -qty >> 1 FROM t ORDER BY id", &[_]?[]const u8{ "9223372036854775803", "9223372036854775798", "9223372036854775793" } },
        .{ "SELECT -qty | 0 FROM t ORDER BY id", &[_]?[]const u8{ "18446744073709551606", "18446744073709551596", "18446744073709551586" } },
        .{ "SELECT qty & 12 FROM t ORDER BY id", &[_]?[]const u8{ "8", "4", "12" } },
        .{ "SELECT qty ^ 5 FROM t ORDER BY id", &[_]?[]const u8{ "15", "17", "27" } },
        .{ "SELECT qty << 60 FROM t ORDER BY id", &[_]?[]const u8{ "11529215046068469760", "4611686018427387904", "16140901064495857664" } },
        .{ "SELECT ~qty & 255 FROM t ORDER BY id", &[_]?[]const u8{ "245", "235", "225" } },
        // A double rounds half to even, a decimal half away from zero, and
        // text reads as the integer it starts with.
        .{ "SELECT price | 0 FROM t ORDER BY id", &[_]?[]const u8{ "2", "2", "4" } },
        .{ "SELECT 2.5 | 0 FROM t WHERE id = 1", &[_]?[]const u8{"3"} },
        .{ "SELECT '12abc' & 7 FROM t WHERE id = 1", &[_]?[]const u8{"4"} },
        .{ "SELECT 0xFFFFFFFFFFFFFFFF & -2 FROM t WHERE id = 1", &[_]?[]const u8{"18446744073709551614"} },
        .{ "SELECT 1 << 63 FROM t WHERE id = 1", &[_]?[]const u8{"9223372036854775808"} },
        .{ "SELECT 1 << 64 FROM t WHERE id = 1", &[_]?[]const u8{"0"} },
        .{ "SELECT 1 << -1 FROM t WHERE id = 1", &[_]?[]const u8{"0"} },
        .{ "SELECT -1 >> 70 FROM t WHERE id = 1", &[_]?[]const u8{"0"} },
        .{ "SELECT 5 ^ 3 FROM t WHERE id = 1", &[_]?[]const u8{"6"} },
        .{ "SELECT 2 * 3 ^ 1 FROM t WHERE id = 1", &[_]?[]const u8{"4"} },
        .{ "SELECT id FROM t WHERE ~qty > 18446744073709551590 ORDER BY id", &[_]?[]const u8{ "1", "2" } },
        // The result keeps its value where it is compared or chosen. Past
        // BIGINT it doesn't fit a SIGNED cast, which makes it NULL, as
        // StarRocks does (#450), where MySQL keeps its 64 bits.
        .{ "SELECT CAST(~qty AS SIGNED) FROM t ORDER BY id", &[_]?[]const u8{ null, null, null } },
        .{ "SELECT CAST(1 << 63 AS SIGNED) FROM t WHERE id = 1", &[_]?[]const u8{null} },
        .{ "SELECT CAST(~qty & 255 AS SIGNED) FROM t ORDER BY id", &[_]?[]const u8{ "245", "235", "225" } },
        .{ "SELECT IFNULL(~qty, 0) FROM t ORDER BY id", &[_]?[]const u8{ "18446744073709551605", "18446744073709551595", "18446744073709551585" } },
        .{ "SELECT COALESCE(NULL, ~qty) FROM t ORDER BY id", &[_]?[]const u8{ "18446744073709551605", "18446744073709551595", "18446744073709551585" } },
        .{ "SELECT NULLIF(~qty, ~10) FROM t ORDER BY id", &[_]?[]const u8{ null, "18446744073709551595", "18446744073709551585" } },
        .{ "SELECT GREATEST(~qty, 1) FROM t ORDER BY id", &[_]?[]const u8{ "18446744073709551605", "18446744073709551595", "18446744073709551585" } },
        .{ "SELECT LEAST(~qty, ~15) FROM t ORDER BY id", &[_]?[]const u8{ "18446744073709551600", "18446744073709551595", "18446744073709551585" } },
        .{ "SELECT BIT_OR(qty & 12) FROM t", &[_]?[]const u8{"12"} },
    };
    inline for (cases) |c| try expectMysqlText(allocator, db, c[0], c[1]);
}

test "binary arith: BIT_COUNT, BIN and CONV read a value as MySQL does" {
    // Values probed against MySQL 8.4 (#323).
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    const cases = .{
        .{ "SELECT BIT_COUNT(-1) FROM t WHERE id = 1", &[_]?[]const u8{"64"} },
        .{ "SELECT BIT_COUNT(~qty) FROM t ORDER BY id", &[_]?[]const u8{ "62", "62", "60" } },
        .{ "SELECT BIN(-1) FROM t WHERE id = 1", &[_]?[]const u8{"1" ** 64} },
        .{ "SELECT BIN(qty) FROM t ORDER BY id", &[_]?[]const u8{ "1010", "10100", "11110" } },
        // A number reaches BIN as its text, so a fraction is cut, not rounded.
        .{ "SELECT BIN(price) FROM t ORDER BY id", &[_]?[]const u8{ "1", "10", "11" } },
        .{ "SELECT BIN(TRUE) FROM t WHERE id = 1", &[_]?[]const u8{"1"} },
        .{ "SELECT BIN(0x41) FROM t WHERE id = 1", &[_]?[]const u8{"1000001"} },
        .{ "SELECT BIN('') FROM t WHERE id = 1", &[_]?[]const u8{null} },
        .{ "SELECT CONV(qty * 10 + 5, 10, 16) FROM t ORDER BY id", &[_]?[]const u8{ "69", "CD", "131" } },
        .{ "SELECT CONV(-1, 10, 16) FROM t WHERE id = 1", &[_]?[]const u8{"FFFFFFFFFFFFFFFF"} },
        .{ "SELECT CONV('ff', 16, -10) FROM t WHERE id = 1", &[_]?[]const u8{"255"} },
        .{ "SELECT CONV('8000000000000000', 16, -10) FROM t WHERE id = 1", &[_]?[]const u8{"-9223372036854775808"} },
        .{ "SELECT CONV('-8000000000000001', -16, 10) FROM t WHERE id = 1", &[_]?[]const u8{"9223372036854775808"} },
        .{ "SELECT CONV('zz', 36, 2) FROM t WHERE id = 1", &[_]?[]const u8{"10100001111"} },
        .{ "SELECT CONV('12x', 10, 36) FROM t WHERE id = 1", &[_]?[]const u8{"C"} },
        .{ "SELECT CONV(5, 1, 10) FROM t WHERE id = 1", &[_]?[]const u8{null} },
        .{ "SELECT CONV(5, 10, 37) FROM t WHERE id = 1", &[_]?[]const u8{null} },
    };
    inline for (cases) |c| try expectMysqlText(allocator, db, c[0], c[1]);
}

test "binary arith: MySQL's BIT_AND, BIT_OR and BIT_XOR return BIGINT UNSIGNED" {
    // Values probed against MySQL 8.4 (#349).
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE bz (id INT, v INT, d DOUBLE, m DECIMAL(10,2), c VARCHAR(20))");
    try helpers.exec(allocator, db, "INSERT INTO bz VALUES (1, -3, 2.7, 2.5, '12abc'), (2, 5, -1.5, -2.5, '-3'), (3, NULL, NULL, NULL, NULL)");

    const all_ones = "18446744073709551615";
    const cases = .{
        .{ "SELECT BIT_AND(v) FROM bz", &[_]?[]const u8{"5"} },
        .{ "SELECT BIT_OR(v) FROM bz", &[_]?[]const u8{"18446744073709551613"} },
        .{ "SELECT BIT_XOR(v) FROM bz", &[_]?[]const u8{"18446744073709551608"} },
        // No rows, or only NULLs, give the operation's identity.
        .{ "SELECT BIT_AND(v) FROM bz WHERE id > 10", &[_]?[]const u8{all_ones} },
        .{ "SELECT BIT_OR(v) FROM bz WHERE id > 10", &[_]?[]const u8{"0"} },
        .{ "SELECT BIT_XOR(v) FROM bz WHERE id = 3", &[_]?[]const u8{"0"} },
        .{ "SELECT BIT_AND(v) FROM bz GROUP BY id ORDER BY id", &[_]?[]const u8{ "18446744073709551613", "5", all_ones } },
        .{ "SELECT BIT_AND(v) FROM bz WHERE id > 10 GROUP BY id", &[_]?[]const u8{} },
        .{ "SELECT BIT_OR(v) + 1 FROM bz", &[_]?[]const u8{"18446744073709551614"} },
        .{ "SELECT BIT_OR(~v) FROM bz", &[_]?[]const u8{"18446744073709551610"} },
        .{ "SELECT BIT_OR(v) FROM bz HAVING BIT_OR(v) > 5", &[_]?[]const u8{"18446744073709551613"} },
        .{ "SELECT CAST(BIT_OR(v) AS SIGNED) FROM bz", &[_]?[]const u8{null} },
        .{ "SELECT CAST(BIT_AND(v) AS SIGNED) FROM bz", &[_]?[]const u8{"5"} },
        .{ "SELECT CONCAT(BIT_XOR(v), '') FROM bz", &[_]?[]const u8{"18446744073709551608"} },
        // A double rounds half to even, a decimal half away from zero, and
        // text reads as the integer it starts with, as for `|`.
        .{ "SELECT BIT_AND(d) FROM bz", &[_]?[]const u8{"2"} },
        .{ "SELECT BIT_XOR(d) FROM bz", &[_]?[]const u8{"18446744073709551613"} },
        .{ "SELECT BIT_AND(m) FROM bz", &[_]?[]const u8{"1"} },
        .{ "SELECT BIT_XOR(m) FROM bz", &[_]?[]const u8{"18446744073709551614"} },
        .{ "SELECT BIT_AND(c) FROM bz", &[_]?[]const u8{"12"} },
        .{ "SELECT BIT_XOR(c) FROM bz", &[_]?[]const u8{"18446744073709551601"} },
        .{ "SELECT (SELECT BIT_AND(v) FROM bz WHERE id > 10) FROM bz WHERE id = 1", &[_]?[]const u8{all_ones} },
        .{ "SELECT (SELECT BIT_AND(b2.v) FROM bz b2 WHERE b2.id = bz.id + 100) FROM bz ORDER BY id", &[_]?[]const u8{ all_ones, all_ones, all_ones } },
    };
    inline for (cases) |c| try expectMysqlText(allocator, db, c[0], c[1]);

    // The other dialects keep StarRocks' signed BIGINT, NULL over no rows.
    const neutral_cases = .{
        .{ "SELECT BIT_OR(v) FROM bz", "-3" },
        .{ "SELECT BIT_XOR(v) FROM bz", "-8" },
        .{ "SELECT BIT_AND(v) FROM bz WHERE id > 10", null },
    };
    inline for (neutral_cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        var q = try runSql(allocator, db, c[0]);
        defer q.deinit();
        try expectColumnText(allocator, &q, &.{c[1]});
    }
    try helpers.expectRunError(allocator, db, "SELECT BIT_AND(d) FROM bz", error.AggregateUnsupportedType);
}

test "binary arith: a LARGEINT keeps every digit, as text and cast to BIGINT" {
    // #309: a LARGEINT reached text through DOUBLE, so
    // 18446744073709551615 became 18446744073709552000.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    const mysql_cases = .{
        .{ "SELECT CAST(0xFFFFFFFFFFFFFFFF + 0 AS CHAR) FROM t WHERE id = 1", &[_]?[]const u8{"18446744073709551615"} },
        .{ "SELECT CONCAT(~qty, '') FROM t ORDER BY id", &[_]?[]const u8{ "18446744073709551605", "18446744073709551595", "18446744073709551585" } },
        .{ "SELECT CONCAT_WS(',', ~0, 0xFFFFFFFFFFFFFFFF + 0) FROM t WHERE id = 1", &[_]?[]const u8{"18446744073709551615,18446744073709551615"} },
        .{ "SELECT LENGTH(~qty) FROM t ORDER BY id", &[_]?[]const u8{ "20", "20", "20" } },
    };
    inline for (mysql_cases) |c| try expectMysqlText(allocator, db, c[0], c[1]);

    // A hex literal past BIGINT is a LARGEINT in arithmetic.
    const neutral_cases = .{
        .{ "SELECT CAST(0xFFFFFFFFFFFFFFFF + 0 AS VARCHAR(40)) FROM t WHERE id = 1", "18446744073709551615" },
        .{ "SELECT CONCAT((0xFFFFFFFFFFFFFFFF + 0) * 1000, '') FROM t WHERE id = 1", "18446744073709551615000" },
        .{ "SELECT CONCAT(0 - (0xFFFFFFFFFFFFFFFF + 0) * 1000, '') FROM t WHERE id = 1", "-18446744073709551615000" },
        // A LARGEINT reaches BIGINT exactly, not through DOUBLE, and one
        // past BIGINT is NULL, as in StarRocks (#450).
        .{ "SELECT CAST((0xFFFFFFFFFFFFFFFF + 0) DIV 2 AS BIGINT) FROM t WHERE id = 1", "9223372036854775807" },
        .{ "SELECT CAST(0xFFFFFFFFFFFFFFFF + 0 AS BIGINT) FROM t WHERE id = 1", null },
        .{ "SELECT CAST(0xFFFFFFFFFFFFFFFF + 1 AS BIGINT) FROM t WHERE id = 1", null },
        .{ "SELECT CAST(0 - (0xFFFFFFFFFFFFFFFF + 0) AS BIGINT) FROM t WHERE id = 1", null },
    };
    inline for (neutral_cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        var q = try runSql(allocator, db, c[0]);
        defer q.deinit();
        try expectColumnText(allocator, &q, &.{c[1]});
    }
}
