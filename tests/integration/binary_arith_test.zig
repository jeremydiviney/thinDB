//! Binary arithmetic operators (+ - * / %) in SELECT.
//!
//! The parser lowers these to scalar function calls (add/sub/mul/div/mod)
//! that the existing Compute operator already evaluates. Tests cover:
//!   - basic shapes per operator over BIGINT and DOUBLE columns
//!   - precedence (`*` higher than `+`)
//!   - parenthesized sub-expressions
//!   - composition with the existing scalar-function call syntax
//!   - the natural "delta" pattern from the LAG bench
//!   - integer division-by-zero returns 0 (MOD/DIV convention)

const std = @import("std");
const thindb = @import("thindb");

const RunResult = struct {
    arena: std.heap.ArenaAllocator,
    cq: thindb.net.CompiledQuery,

    pub fn deinit(self: *RunResult) void {
        self.cq.deinit();
        self.arena.deinit();
    }

    pub fn next(self: *RunResult) !?thindb.Batch {
        return self.cq.next();
    }
};

fn runSql(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !RunResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), sql);
    const cq = try thindb.net.compile(allocator, db, root);
    return .{ .arena = arena, .cq = cq };
}

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

test "binary arith: division produces integer truncation" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(allocator, db, "SELECT qty / 7 AS d FROM t ORDER BY id ASC");
    defer q.deinit();
    const got = try collectBigint(allocator, &q, 0);
    defer allocator.free(got);
    // 10/7=1, 20/7=2, 30/7=4 (integer truncation)
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 4 }, got);
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

test "binary arith: division by zero returns 0 (integer)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedSimple(allocator, db);

    var q = try runSql(allocator, db, "SELECT qty / 0 AS d FROM t ORDER BY id ASC");
    defer q.deinit();
    const got = try collectBigint(allocator, &q, 0);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 0, 0, 0 }, got);
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
