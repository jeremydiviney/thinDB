//! SQL `DELETE FROM t [WHERE ...]` — streaming bulk delete against
//! an existing table. Tombstones segment rows, clones the memtable.
//! Predicate uses the full PredicateExpr grammar (AND/OR/IN/etc).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSql = helpers.runSql;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, region VARCHAR(8) NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO t (id, qty, region) VALUES " ++
            "(1, 10, 'east'), (2, 20, 'east'), (3, 30, 'west'), (4, 40, 'west'), (5, 100, 'east')",
    );
    const tt = try db.openTable("t", .{});
    try tt.flush();
    return db;
}

test "DELETE FROM t WHERE qty > 20 — survivors only" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "DELETE FROM t WHERE qty > 20");
    const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, ids);
}

test "DELETE FROM t (no WHERE) — empties the table" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "DELETE FROM t");
    const ids = try collectBigints(allocator, db, "SELECT id FROM t");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{}, ids);
}

test "DELETE FROM t WHERE qty BETWEEN 15 AND 35 — multi-conjunct" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "DELETE FROM t WHERE qty BETWEEN 15 AND 35");
    // qty in [15, 35] → ids 2 (20) and 3 (30) gone.
    const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 4, 5 }, ids);
}

test "DELETE FROM t WHERE region = 'east'" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "DELETE FROM t WHERE region = 'east'");
    const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    // east: 1, 2, 5. Survivors: 3, 4.
    try std.testing.expectEqualSlices(i64, &.{ 3, 4 }, ids);
}

test "DELETE FROM t WHERE id IN (literal list)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "DELETE FROM t WHERE id IN (2, 4)");
    const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 3, 5 }, ids);
}

test "DELETE FROM t affects unflushed memtable rows too" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Add memtable-only rows (without flushing) — DELETE should hit them too.
    try exec(allocator, db, "INSERT INTO t (id, qty, region) VALUES (6, 5, 'south'), (7, 95, 'south')");
    try exec(allocator, db, "DELETE FROM t WHERE region = 'south'");
    const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    // south rows gone; original 5 east/west rows remain.
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5 }, ids);
}

test "DELETE FROM t WHERE qty > @cutoff — predicate uses session var" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "SET @cutoff = 25; DELETE FROM t WHERE qty > @cutoff");
    const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    // qty > 25 → ids 3 (30), 4 (40), 5 (100) gone.
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, ids);
}

test "DELETE FROM t — affected_rows reported" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "DELETE FROM t WHERE qty > 20");
    defer q.deinit();
    while (try q.next()) |_| {}
    // ids 3, 4, 5 deleted = 3 rows.
    try std.testing.expectEqual(@as(u64, 3), q.affectedRows());
}
