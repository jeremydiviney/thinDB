//! Algebraic aggregate reduction — a compile-stage plan rewrite (sibling of
//! the FD-collapse rewrite). Many SUM/MIN/MAX over affine transforms of one
//! base column (`a·col + b`) collapse to a small base set computed once plus
//! per-output-group arithmetic. ClickBench Q29 (`SUM(rw), SUM(rw+1), ...`) is
//! the target.
//!
//! Unlike the FD-collapse rewrite, this one runs in `net.compileOp` (it needs
//! the base column's declared type to prove overflow-identity), so the plan
//! text from `ir.explain` does NOT reflect it. These tests assert RESULT
//! correctness: the reduced path must produce byte-identical values to the
//! direct path — including the same ArithmeticOverflow error.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, w SMALLINT NOT NULL, g BIGINT NOT NULL, f DOUBLE NOT NULL)",
    );
    // w in [10, 60]; g in {1, 2} for grouped tests; f mirrors w as a double.
    try exec(allocator, db,
        "INSERT INTO t (id, w, g, f) VALUES " ++
            "(1, 10, 1, 10.0), (2, 20, 1, 20.0), (3, 30, 2, 30.0), " ++
            "(4, 40, 2, 40.0), (5, 50, 1, 50.0), (6, 60, 2, 60.0)",
    );
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

test "affine-agg: Q29-shape SUM(w+k) identical to SUM(w)+k*COUNT(w)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // SUM(w) = 210, COUNT(w) = 6. SUM(w+k) = 210 + 6k.
    var q = try runSql(allocator, db,
        "SELECT SUM(w), SUM(w + 1), SUM(w + 5), SUM(w + 89) FROM t",
    );
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expectEqual(@as(i64, 210), b.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 210 + 6 * 1), b.values[1].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 210 + 6 * 5), b.values[2].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 210 + 6 * 89), b.values[3].data.bigint[0]);
    try std.testing.expectEqual(@as(?thindb.Batch, null), try q.next());
}

test "affine-agg: SUM with sub and mul affine args" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // SUM(w) = 210, COUNT = 6.
    // SUM(w - 3)   = 210 - 18 = 192
    // SUM(3 - w)   = 18 - 210 = -192
    // SUM(2 * w)   = 420
    var q = try runSql(allocator, db,
        "SELECT SUM(w - 3), SUM(3 - w), SUM(2 * w), SUM(w) FROM t",
    );
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 192), b.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, -192), b.values[1].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 420), b.values[2].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 210), b.values[3].data.bigint[0]);
}

test "affine-agg: MIN/MAX over affine args (value selection, bit-identical)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // w in [10, 60]. MIN(w)=10, MAX(w)=60.
    // MIN(w-2) = 8, MAX(w+5) = 65, MIN(3-w) = 3-60 = -57 (a<0 flips to MAX),
    // MAX(2*w) = 120.
    var q = try runSql(allocator, db,
        "SELECT MIN(w), MIN(w - 2), MAX(w + 5), MIN(3 - w), MAX(2 * w) FROM t",
    );
    defer q.deinit();

    const b = (try q.next()).?;
    // MIN(w) keeps the input type (smallint); the affine args promote to int.
    try std.testing.expectEqual(@as(i16, 10), b.values[0].data.smallint[0]);
    try std.testing.expectEqual(@as(i32, 8), b.values[1].data.int[0]);
    try std.testing.expectEqual(@as(i32, 65), b.values[2].data.int[0]);
    try std.testing.expectEqual(@as(i32, -57), b.values[3].data.int[0]);
    try std.testing.expectEqual(@as(i32, 120), b.values[4].data.int[0]);
}

test "affine-agg: grouped SUM(w+k) per group identical" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // g=1: w in {10,20,50} → SUM=80, COUNT=3. g=2: w in {30,40,60} → SUM=130, COUNT=3.
    var q = try runSql(allocator, db,
        "SELECT g, SUM(w), SUM(w + 10) FROM t GROUP BY g ORDER BY g ASC",
    );
    defer q.deinit();

    const expect_g = [_]i64{ 1, 2 };
    const expect_sum = [_]i64{ 80, 130 };
    const expect_sum10 = [_]i64{ 80 + 30, 130 + 30 };
    var row: usize = 0;
    while (try q.next()) |b| {
        var i: usize = 0;
        while (i < b.row_count) : (i += 1) {
            try std.testing.expectEqual(expect_g[row], b.values[0].data.bigint[i]);
            try std.testing.expectEqual(expect_sum[row], b.values[1].data.bigint[i]);
            try std.testing.expectEqual(expect_sum10[row], b.values[2].data.bigint[i]);
            row += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), row);
}

test "affine-agg: grouped reduction fires for 3+ aggs (V2 group-topn path)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Three affine SUMs collapse to one base set {SUM(w), COUNT(w)}; the core
    // computes those and the three outputs derive per group. g=1: SUM=80,N=3.
    // g=2: SUM=130,N=3. SUM(w+k) = SUM + k*N.
    var q = try runSql(allocator, db,
        "SELECT g, SUM(w), SUM(w + 1), SUM(w + 2) FROM t GROUP BY g ORDER BY g ASC",
    );
    defer q.deinit();
    const exp = [2][4]i64{
        .{ 1, 80, 83, 86 },
        .{ 2, 130, 133, 136 },
    };
    var row: usize = 0;
    while (try q.next()) |b| {
        var i: usize = 0;
        while (i < b.row_count) : (i += 1) {
            try std.testing.expectEqual(exp[row][0], b.values[0].data.bigint[i]);
            try std.testing.expectEqual(exp[row][1], b.values[1].data.bigint[i]);
            try std.testing.expectEqual(exp[row][2], b.values[2].data.bigint[i]);
            try std.testing.expectEqual(exp[row][3], b.values[3].data.bigint[i]);
            row += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), row);
}

test "affine-agg: ORDER BY a reduced agg keeps it direct for ranking" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // s1 is in ORDER BY, so it must stay a direct core aggregate (the core ranks
    // on it); s0 and s2 still reduce. Top group by s1 DESC is g=2 (133 > 83).
    var q = try runSql(allocator, db,
        "SELECT g, SUM(w) s0, SUM(w + 1) s1, SUM(w + 2) s2 FROM t GROUP BY g ORDER BY s1 DESC",
    );
    defer q.deinit();
    const exp = [2][4]i64{
        .{ 2, 130, 133, 136 },
        .{ 1, 80, 83, 86 },
    };
    var row: usize = 0;
    while (try q.next()) |b| {
        var i: usize = 0;
        while (i < b.row_count) : (i += 1) {
            try std.testing.expectEqual(exp[row][0], b.values[0].data.bigint[i]);
            try std.testing.expectEqual(exp[row][1], b.values[1].data.bigint[i]);
            try std.testing.expectEqual(exp[row][2], b.values[2].data.bigint[i]);
            try std.testing.expectEqual(exp[row][3], b.values[3].data.bigint[i]);
            row += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), row);
}

test "affine-agg: float SUM is NOT reduced (stays direct, correct)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // f mirrors w: SUM(f)=210.0. SUM(f+1.0)=216.0. Float SUM is non-associative
    // so the rewrite must skip it — but the direct result is still correct here.
    var q = try runSql(allocator, db,
        "SELECT SUM(f), SUM(f + 1.0) FROM t",
    );
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectApproxEqAbs(@as(f64, 210.0), b.values[0].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 216.0), b.values[1].data.double[0], 1e-9);
}

test "affine-agg: non-affine arg (w*w) is NOT reduced (stays direct, correct)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // w*w: 100+400+900+1600+2500+3600 = 9100. Non-affine → left direct.
    var q = try runSql(allocator, db,
        "SELECT SUM(w * w), SUM(w) FROM t",
    );
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 9100), b.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 210), b.values[1].data.bigint[0]);
}

test "affine-agg: SUM overflow raises ArithmeticOverflow identically" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE big (id BIGINT PRIMARY KEY, c BIGINT NOT NULL)");
    // Three rows near i64 max so Σc overflows i64 — the reduced SUM(c) must
    // narrow with the same i64 range check the direct SUM finalize uses.
    try exec(allocator, db,
        "INSERT INTO big (id, c) VALUES (1, 9000000000000000000), (2, 9000000000000000000), (3, 9000000000000000000)",
    );
    const t = try db.openTable("big", .{});
    try t.flush();

    // Three plain SUM(c): base {SUM,COUNT}=2 < 3 ⇒ reduction fires. Σc overflows.
    var q = try runSql(allocator, db, "SELECT SUM(c), SUM(c), SUM(c) FROM big");
    defer q.deinit();
    try std.testing.expectError(error.ArithmeticOverflow, q.next());
}

test "affine-agg: single affine agg does not over-trigger (correct value)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Only one aggregate: the reduction would not shrink the agg set
    // (SUM+COUNT base = 2 > 1), so it bails — but the answer must be right.
    var q = try runSql(allocator, db, "SELECT SUM(w + 7) FROM t");
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 210 + 6 * 7), b.values[0].data.bigint[0]);
}
