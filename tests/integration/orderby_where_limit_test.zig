//! `SELECT … FROM t WHERE <pred> ORDER BY <keys> LIMIT n [OFFSET m]` over a
//! single base table. The shape has three engine paths — the zonemap
//! block-skipping top-N, the late-materialization scan, and the plain
//! scan → sort → limit — and every one must return exactly what the
//! reference `WHERE … ORDER BY` (no LIMIT) pipeline returns, sliced.
//!
//! Issue #45: on production `WHERE updatedAt >= '<text>' ORDER BY id LIMIT 3`
//! came back with the WHERE ignored — the zonemap top-N evaluated the raw
//! predicate without coercing the text literal to the DATETIME column's type
//! (in a release build the wrong `Value` field was read as the bound). The
//! matrix below crosses literal kinds × ORDER BY keys × LIMIT/OFFSET × where
//! the rows live (segments, segments + memtable + tombstones, memtable only).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

const ROWS: i64 = 512;
const TAIL_ROWS: i64 = 64;

/// Mirrors the production table that surfaced #45 (INT primary key, nullable
/// DATETIME(6) filter column) plus one column per literal kind the WHERE
/// clause can carry. Row i:
///   g         = i % 3 (NOT NULL, low cardinality: a non-null lead with ties)
///   v         = i % 7, NULL when i % 11 == 0
///   updatedAt = 2026-01-01 00:00 + i minutes, NULL when i % 13 == 0
///   d         = 2026-<1 + (i/28) % 12>-<1 + i % 28>, NULL when i % 17 == 0
///   s         = 'k' ++ (i % 5), NULL when i % 19 == 0
///   amt       = i * 1.25
///   big       = i * 1000
fn createTable(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    try exec(allocator, db, "CREATE TABLE t (id INT NOT NULL, g INT NOT NULL, v INT, updatedAt DATETIME(6), d DATE, s VARCHAR(8), amt DECIMAL(10,2), big BIGINT NOT NULL, PRIMARY KEY (id))");
}

fn twoDigits(n: i64) [2]u8 {
    return .{ '0' + @as(u8, @intCast(@divTrunc(n, 10))), '0' + @as(u8, @intCast(@mod(n, 10))) };
}

fn insertRows(allocator: std.mem.Allocator, db: *thindb.Database, first: i64, count: i64) !void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO t (id, g, v, updatedAt, d, s, amt, big) VALUES ");
    var line: [160]u8 = undefined;
    var i: i64 = first;
    while (i < first + count) : (i += 1) {
        if (i > first) try buf.appendSlice(allocator, ", ");
        const v = if (@mod(i, 11) == 0) "NULL" else try std.fmt.bufPrint(line[0..8], "{d}", .{@mod(i, 7)});
        var ts_buf: [24]u8 = undefined;
        const ts = if (@mod(i, 13) == 0) "NULL" else try std.fmt.bufPrint(&ts_buf, "'2026-01-01 {s}:{s}:00'", .{ twoDigits(@divTrunc(i, 60)), twoDigits(@mod(i, 60)) });
        var d_buf: [16]u8 = undefined;
        const d = if (@mod(i, 17) == 0) "NULL" else try std.fmt.bufPrint(&d_buf, "'2026-{s}-{s}'", .{ twoDigits(1 + @mod(@divTrunc(i, 28), 12)), twoDigits(1 + @mod(i, 28)) });
        var s_buf: [8]u8 = undefined;
        const s = if (@mod(i, 19) == 0) "NULL" else try std.fmt.bufPrint(&s_buf, "'k{d}'", .{@mod(i, 5)});
        const cents = i * 125;
        const row = try std.fmt.bufPrint(line[8..], "({d}, {d}, {s}, {s}, {s}, {s}, {d}.{s}, {d})", .{ i, @mod(i, 3), v, ts, d, s, @divTrunc(cents, 100), twoDigits(@mod(cents, 100)), i * 1000 });
        try buf.appendSlice(allocator, row);
    }
    try exec(allocator, db, buf.items);
}

const Placement = enum {
    /// Everything flushed: 8 row groups of 64 rows in one segment.
    segments,
    /// Flushed rows + a memtable tail of 64 rows + tombstones in both.
    mixed,
    /// Nothing flushed.
    memtable,
};

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype, placement: Placement, max_dop: usize) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{ .row_group_size = 64, .max_dop = max_dop });
    errdefer db.close();
    try createTable(allocator, db);
    try insertRows(allocator, db, 0, ROWS);
    switch (placement) {
        .memtable => {},
        .segments => {
            const t = try db.openTable("t", .{});
            try t.flush();
        },
        .mixed => {
            const t = try db.openTable("t", .{});
            try t.flush();
            try insertRows(allocator, db, ROWS, TAIL_ROWS);
            // Tombstones on the flushed segment (5, 300, 301, 450) and on the
            // memtable tail (515, 560); 300/301 sit at the front of the #45 answer.
            try exec(allocator, db, "DELETE FROM t WHERE id IN (5, 300, 301, 450, 515, 560)");
        },
    }
    return db;
}

/// Collect the single INT/BIGINT column of `sql` into an owned slice.
fn collectIds(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) ![]i64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(i64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |batch| {
        // Every batch carries one view per output column, rows or not (#46).
        try std.testing.expectEqual(batch.schema.len, batch.values.len);
        switch (batch.values[0].data) {
            .int => |vals| for (vals[0..batch.row_count]) |x| try out.append(allocator, x),
            .bigint => |vals| for (vals[0..batch.row_count]) |x| try out.append(allocator, x),
            else => return error.UnexpectedIdType,
        }
    }
    return out.toOwnedSlice(allocator);
}

const wheres = [_][]const u8{
    "",
    // text literal against DATETIME(6) — the #45 shape, every comparison op
    "WHERE updatedAt >= '2026-01-01 05:00:00'",
    "WHERE updatedAt > '2026-01-01 05:00:00'",
    "WHERE updatedAt < '2026-01-01 01:00:00'",
    "WHERE updatedAt <= '2026-01-01 01:00:00'",
    "WHERE updatedAt = '2026-01-01 05:05:00'",
    "WHERE updatedAt <> '2026-01-01 05:05:00'",
    "WHERE updatedAt BETWEEN '2026-01-01 02:00:00' AND '2026-01-01 03:00:00'",
    "WHERE updatedAt >= TIMESTAMP '2026-01-01 05:00:00'",
    "WHERE updatedAt IS NULL",
    "WHERE updatedAt IS NOT NULL",
    // text literal against DATE
    "WHERE d >= '2026-03-01'",
    "WHERE d = '2026-01-15'",
    "WHERE d < DATE '2026-02-01'",
    // integer literal widened to BIGINT, and predicates on the key itself
    "WHERE big > 5000",
    "WHERE big BETWEEN 100000 AND 200000",
    "WHERE id > 100",
    "WHERE id <= 10",
    // decimal literal kinds
    "WHERE amt > 100.5",
    "WHERE amt >= 10",
    "WHERE amt = 6.25",
    // strings
    "WHERE s = 'k2'",
    "WHERE s > 'k2'",
    "WHERE s <> 'k0'",
    "WHERE s IN ('k1', 'k3')",
    "WHERE s LIKE 'k%'",
    "WHERE s IS NULL",
    // nullable int
    "WHERE v = 3",
    "WHERE v IN (1, 2)",
    "WHERE v > 2 AND s <> 'k0'",
    // boolean combinations mixing literal kinds
    "WHERE big >= 5000 AND updatedAt >= '2026-01-01 00:10:00'",
    "WHERE v = 3 OR updatedAt >= '2026-01-01 08:00:00'",
    "WHERE NOT (v = 3)",
    "WHERE (updatedAt >= '2026-01-01 05:00:00' AND s = 'k1') OR d < '2026-01-05'",
    // nothing qualifies
    "WHERE updatedAt >= '2027-01-01 00:00:00'",
};

/// Every ORDER BY ends in `id` so the reference order is total and the slice
/// comparison is exact. Leads cover: the non-null key (zonemap top-N, both
/// directions), a non-null non-key column, a tied non-null lead followed by
/// nullable secondary keys (zonemap heap ordering NULLs past the prunable
/// prefix), and nullable leads of every kind (late-materialization path).
const orders = [_][]const u8{
    "ORDER BY id",
    "ORDER BY id DESC",
    "ORDER BY big, id",
    "ORDER BY big DESC, id DESC",
    "ORDER BY g, updatedAt, id",
    "ORDER BY g DESC, v DESC, id",
    "ORDER BY g, s, id DESC",
    "ORDER BY g DESC, d, id",
    "ORDER BY updatedAt, id",
    "ORDER BY updatedAt DESC, id",
    "ORDER BY v, id",
    "ORDER BY s DESC, id",
    "ORDER BY amt DESC, id",
    "ORDER BY d, id DESC",
};

const Limit = struct { n: usize, offset: usize };
const limits = [_]Limit{
    .{ .n = 3, .offset = 0 },
    .{ .n = 1, .offset = 0 },
    .{ .n = 5, .offset = 2 },
    .{ .n = 100, .offset = 0 },
    .{ .n = 4, .offset = 600 },
};

fn checkMatrix(allocator: std.mem.Allocator, db: *thindb.Database, placement: Placement) !void {
    var failures: usize = 0;
    for (wheres) |w| {
        for (orders) |o| {
            const ref_sql = try std.fmt.allocPrint(allocator, "SELECT id FROM t {s} {s}", .{ w, o });
            defer allocator.free(ref_sql);
            const ref = try collectIds(allocator, db, ref_sql);
            defer allocator.free(ref);

            for (limits) |l| {
                const sql = try std.fmt.allocPrint(allocator, "SELECT id FROM t {s} {s} LIMIT {d} OFFSET {d}", .{ w, o, l.n, l.offset });
                defer allocator.free(sql);
                const got = try collectIds(allocator, db, sql);
                defer allocator.free(got);

                const start = @min(l.offset, ref.len);
                const end = @min(start + l.n, ref.len);
                const want = ref[start..end];
                if (!std.mem.eql(i64, want, got)) {
                    failures += 1;
                    std.debug.print("[{s}] {s}\n  want {any}\n  got  {any}\n", .{ @tagName(placement), sql, want, got });
                }
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 0), failures);
}

test "#45: WHERE on nullable datetime + ORDER BY key ASC + LIMIT keeps the WHERE" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir, .segments, 1);
    defer db.close();

    // Rows 300.. carry updatedAt >= 05:00:00; 312 is NULL (312 % 13 == 0).
    const ids = try collectIds(allocator, db, "SELECT id FROM t WHERE updatedAt >= '2026-01-01 05:00:00' ORDER BY id LIMIT 3");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 300, 301, 302 }, ids);

    const later = try collectIds(allocator, db, "SELECT id FROM t WHERE updatedAt >= '2026-01-01 05:00:00' ORDER BY id LIMIT 2 OFFSET 11");
    defer allocator.free(later);
    try std.testing.expectEqualSlices(i64, &.{ 311, 313 }, later);
}

test "the reference pipeline itself is right on the #45 shape" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir, .segments, 1);
    defer db.close();

    // 212 rows have id >= 300; 16 of those (312, 325, …, 507) are NULL.
    const ids = try collectIds(allocator, db, "SELECT id FROM t WHERE updatedAt >= '2026-01-01 05:00:00' ORDER BY id");
    defer allocator.free(ids);
    try std.testing.expectEqual(@as(usize, 196), ids.len);
    try std.testing.expectEqual(@as(i64, 300), ids[0]);
    try std.testing.expectEqual(@as(i64, 511), ids[ids.len - 1]);
}

test "WHERE × ORDER BY × LIMIT matrix over flushed segments" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir, .segments, 1);
    defer db.close();
    try checkMatrix(allocator, db, .segments);
}

test "WHERE × ORDER BY × LIMIT matrix over segments + memtable tail + tombstones" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir, .mixed, 1);
    defer db.close();
    try checkMatrix(allocator, db, .mixed);
}

// --- #46: zero survivors. The zonemap top-N used to return a zero-row batch
// with NO column views; anything that indexed a view (the IN-subquery drain,
// any downstream operator) read out of bounds and took the server down. ---

test "#46: ORDER BY key LIMIT over an empty table yields no rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 64 });
    defer db.close();
    try createTable(allocator, db);

    const plain = try collectIds(allocator, db, "SELECT id FROM t ORDER BY id LIMIT 3");
    defer allocator.free(plain);
    try std.testing.expectEqual(@as(usize, 0), plain.len);

    const filtered = try collectIds(allocator, db, "SELECT id FROM t WHERE updatedAt >= '2026-01-01 05:00:00' ORDER BY id DESC LIMIT 3 OFFSET 1");
    defer allocator.free(filtered);
    try std.testing.expectEqual(@as(usize, 0), filtered.len);
}

test "#46: IN / NOT IN / EXISTS / scalar subqueries whose ORDER BY LIMIT inner is empty" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir, .segments, 1);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE e (id INT NOT NULL, note VARCHAR(8), PRIMARY KEY (id))");

    const cases = .{
        .{ .sql = "SELECT id FROM t WHERE id IN (SELECT id FROM e ORDER BY id LIMIT 3) ORDER BY id", .want = &[_]i64{} },
        .{ .sql = "SELECT id FROM t WHERE id NOT IN (SELECT id FROM e ORDER BY id LIMIT 3) ORDER BY id LIMIT 2", .want = &[_]i64{ 0, 1 } },
        .{ .sql = "SELECT id FROM t WHERE EXISTS (SELECT id FROM e ORDER BY id LIMIT 1) ORDER BY id LIMIT 2", .want = &[_]i64{} },
        .{ .sql = "SELECT id FROM t WHERE NOT EXISTS (SELECT id FROM e ORDER BY id LIMIT 1) ORDER BY id LIMIT 2", .want = &[_]i64{ 0, 1 } },
        // Non-empty inner through the same fast path, with the #45 literal.
        .{ .sql = "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE updatedAt >= '2026-01-01 05:00:00' ORDER BY id LIMIT 3) ORDER BY id", .want = &[_]i64{ 300, 301, 302 } },
        // A filtered-to-nothing inner over a populated table.
        .{ .sql = "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE updatedAt >= '2027-01-01 00:00:00' ORDER BY id LIMIT 3) ORDER BY id", .want = &[_]i64{} },
    };
    inline for (cases) |c| {
        const got = try collectIds(allocator, db, c.sql);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, c.want, got);
    }

    // A scalar subquery with zero rows is an error, not a crash.
    try helpers.expectRunError(allocator, db, "SELECT id FROM t WHERE id = (SELECT id FROM e ORDER BY id LIMIT 1)", error.BadRequest);
}

test "#46: db__schema-qualified inner table, and CREATE/DROP TABLE accept the qualifier" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir, .segments, 1);
    defer db.close();

    try exec(allocator, db, "CREATE DATABASE other");
    try exec(allocator, db, "CREATE TABLE other__public.e (id INT NOT NULL, PRIMARY KEY (id))");

    const empty = try collectIds(allocator, db, "SELECT id FROM t WHERE id IN (SELECT id FROM other__public.e ORDER BY id LIMIT 3) ORDER BY id");
    defer allocator.free(empty);
    try std.testing.expectEqual(@as(usize, 0), empty.len);

    try exec(allocator, db, "INSERT INTO other__public.e (id) VALUES (301), (7), (9999)");
    const hits = try collectIds(allocator, db, "SELECT id FROM t WHERE id IN (SELECT id FROM other__public.e ORDER BY id LIMIT 3) ORDER BY id");
    defer allocator.free(hits);
    try std.testing.expectEqualSlices(i64, &.{ 7, 301 }, hits);

    try exec(allocator, db, "DROP TABLE other__public.e");
    try helpers.expectRunError(allocator, db, "SELECT id FROM other__public.e", error.TableNotFound);
}

test "WHERE × ORDER BY × LIMIT matrix over the memtable only" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir, .memtable, 1);
    defer db.close();
    try checkMatrix(allocator, db, .memtable);
}

test "WHERE × ORDER BY × LIMIT matrix with parallel workers (max_dop 4)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir, .mixed, 4);
    defer db.close();
    try checkMatrix(allocator, db, .mixed);
}

test "#123: ORDER BY a SELECT alias that renames a plain column sorts on that column" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE s (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)");
    try exec(allocator, db, "INSERT INTO s (id, k, v) VALUES (1, 30, 5), (2, 10, 7), (3, 20, 5)");

    const cases = .{
        .{ "SELECT k AS kv, id FROM s ORDER BY kv", &[_]i64{ 10, 20, 30 } },
        .{ "SELECT s.k AS kv FROM s ORDER BY kv DESC", &[_]i64{ 30, 20, 10 } },
        .{ "SELECT k AS kv FROM s ORDER BY kv LIMIT 2", &[_]i64{ 10, 20 } },
        .{ "SELECT k AS kv FROM s WHERE v = 5 ORDER BY kv LIMIT 1", &[_]i64{20} },
        // The alias wins over a same-named input column; a qualified name is the input column.
        .{ "SELECT k AS id FROM s ORDER BY id", &[_]i64{ 10, 20, 30 } },
        .{ "SELECT k AS id FROM s ORDER BY s.id", &[_]i64{ 30, 10, 20 } },
        .{ "SELECT v AS g, COUNT(*) AS n FROM s GROUP BY v ORDER BY g DESC", &[_]i64{ 7, 5 } },
        .{ "SELECT v AS g, SUM(k) AS total FROM s GROUP BY v ORDER BY g", &[_]i64{ 5, 7 } },
        .{ "SELECT DISTINCT v AS g FROM s ORDER BY g", &[_]i64{ 5, 7 } },
        .{ "WITH c AS (SELECT k AS kv FROM s ORDER BY kv LIMIT 2) SELECT kv FROM c ORDER BY kv", &[_]i64{ 10, 20 } },
    };
    inline for (cases) |case| {
        const got = try helpers.collectBigints(allocator, db, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, case[1], got);
    }
}
