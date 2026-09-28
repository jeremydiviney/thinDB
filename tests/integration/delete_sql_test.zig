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
    try exec(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, region VARCHAR(8) NOT NULL)",
    );
    try exec(
        allocator,
        db,
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

test "DELETE FROM t WHERE computed operand — segment and memtable rows (#94)" {
    // Rows 6 and 7 stay in the memtable; 1-5 are in a flushed segment.
    const cases = .{
        .{ .sql = "DELETE FROM t WHERE id % 2 = 0", .survivors = &[_]i64{ 1, 3, 5, 7 }, .affected = 3 },
        .{ .sql = "DELETE FROM t WHERE MOD(id, 3) = 0", .survivors = &[_]i64{ 1, 2, 4, 5, 7 }, .affected = 2 },
        .{ .sql = "DELETE FROM t WHERE qty * 2 = 40", .survivors = &[_]i64{ 1, 3, 4, 5, 6, 7 }, .affected = 1 },
        .{ .sql = "DELETE FROM t WHERE UPPER(region) = 'SOUTH'", .survivors = &[_]i64{ 1, 2, 3, 4, 5 }, .affected = 2 },
        .{ .sql = "DELETE FROM t WHERE LENGTH(region) = 5", .survivors = &[_]i64{ 1, 2, 3, 4, 5 }, .affected = 2 },
        // A plain conjunct keeps its zonemap hint beside the computed one.
        .{ .sql = "DELETE FROM t WHERE region = 'east' AND qty % 20 = 0", .survivors = &[_]i64{ 1, 3, 4, 6, 7 }, .affected = 2 },
        // Full-key equality keeps the Bloom gate; the computed conjunct still filters.
        .{ .sql = "DELETE FROM t WHERE id = 3 AND qty % 2 = 1", .survivors = &[_]i64{ 1, 2, 3, 4, 5, 6, 7 }, .affected = 0 },
        .{ .sql = "DELETE FROM t WHERE id = 3 AND qty % 2 = 0", .survivors = &[_]i64{ 1, 2, 4, 5, 6, 7 }, .affected = 1 },
        .{ .sql = "DELETE FROM t WHERE id % 5 = 0 OR qty + 0 = 10", .survivors = &[_]i64{ 2, 3, 4, 6, 7 }, .affected = 2 },
        .{ .sql = "DELETE FROM t WHERE id IN (2, 4, 6) AND qty % 40 = 0", .survivors = &[_]i64{ 1, 2, 3, 5, 6, 7 }, .affected = 1 },
        .{ .sql = "DELETE FROM t WHERE qty - (SELECT MIN(qty) FROM t) = 0", .survivors = &[_]i64{ 1, 2, 3, 4, 5, 7 }, .affected = 1 },
    };
    inline for (cases) |c| {
        const allocator = std.testing.allocator;
        const io = std.testing.io;
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try setup(allocator, io, tmp.dir);
        defer db.close();
        try exec(allocator, db, "INSERT INTO t (id, qty, region) VALUES (6, 5, 'south'), (7, 95, 'south')");

        var q = try runSql(allocator, db, c.sql);
        defer q.deinit();
        while (try q.next()) |_| {}
        try std.testing.expectEqual(@as(u64, c.affected), q.affectedRows());

        const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(i64, c.survivors, ids);
    }
}

test "DELETE FROM t WHERE computed operand reads a session var (#94)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try exec(allocator, db, "SET @m = 2; DELETE FROM t WHERE id % @m = 1");
    const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 4 }, ids);
}

test "DELETE FROM t WHERE computed operand on an unknown column fails and deletes nothing (#94)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try helpers.expectRunError(allocator, db, "DELETE FROM t WHERE nosuch % 2 = 0", error.ColumnNotFound);
    const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5 }, ids);
}

fn affectedRows(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !u64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    while (try q.next()) |_| {}
    return q.affectedRows();
}

fn countRows(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !usize {
    const got = try collectBigints(allocator, db, sql);
    defer allocator.free(got);
    return @intCast(got[0]);
}

test "DELETE and UPDATE with a long IN list: segment row groups and memtable rows (#340)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // Small row groups give the list's zonemap hint groups to skip.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 64 });
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, region VARCHAR(8) NOT NULL)");

    const max_id = 1200;
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);
    // Ids 1..1000 flushed, 1001..1200 left in the memtable.
    for ([_][2]usize{ .{ 1, 1000 }, .{ 1001, max_id } }, 0..) |range, part| {
        sql.clearRetainingCapacity();
        try sql.appendSlice(allocator, "INSERT INTO t (id, qty, region) VALUES ");
        for (range[0]..range[1] + 1) |id| {
            if (id > range[0]) try sql.appendSlice(allocator, ", ");
            try sql.print(allocator, "({d}, {d}, 'r{d}')", .{ id, id % 7, id % 50 });
        }
        try exec(allocator, db, sql.items);
        if (part == 0) try (try db.openTable("t", .{})).flush();
    }

    // Keys: a stride over two id ranges (none in 501..1000, so whole row
    // groups hold no key), row-group and segment/memtable boundaries, a
    // duplicate, and keys the table doesn't hold.
    var listed = [_]bool{false} ** (max_id + 1);
    var list: std.ArrayList(u8) = .empty;
    defer list.deinit(allocator);
    try list.appendSlice(allocator, "(0, -5, 5000, 99999, 10");
    for (1..max_id + 1) |id| {
        const boundary = id == 64 or id == 65 or id == 128 or id == 129 or id == 1000 or id == 1001;
        if ((id % 7 == 3 and (id <= 500 or id > 1000)) or boundary) {
            listed[id] = true;
            try list.print(allocator, ", {d}", .{id});
        }
    }
    try list.appendSlice(allocator, ")");
    var in_list: usize = 0;
    for (listed) |l| in_list += @intFromBool(l);
    try std.testing.expect(in_list > 100);

    const Run = struct {
        fn sqlWith(a: std.mem.Allocator, buf: *std.ArrayList(u8), head: []const u8, tail: []const u8) ![]const u8 {
            buf.clearRetainingCapacity();
            try buf.appendSlice(a, head);
            try buf.appendSlice(a, tail);
            return buf.items;
        }
    };

    try std.testing.expectEqual(in_list, try countRows(allocator, db, try Run.sqlWith(allocator, &sql, "SELECT COUNT(*) FROM t WHERE id IN ", list.items)));

    // A listed row already deleted is not updated back to life.
    try std.testing.expectEqual(@as(u64, 1), try affectedRows(allocator, db, "DELETE FROM t WHERE id = 17"));
    listed[17] = false;
    const live_listed = in_list - 1;

    try std.testing.expectEqual(@as(u64, live_listed), try affectedRows(allocator, db, try Run.sqlWith(allocator, &sql, "UPDATE t SET qty = -1 WHERE id IN ", list.items)));
    try std.testing.expectEqual(live_listed, try countRows(allocator, db, "SELECT COUNT(*) FROM t WHERE qty = -1"));
    try std.testing.expectEqual(@as(usize, max_id - 1), try countRows(allocator, db, "SELECT COUNT(*) FROM t"));

    const unlisted = max_id - 1 - live_listed;
    try std.testing.expectEqual(@as(u64, unlisted), try affectedRows(allocator, db, try Run.sqlWith(allocator, &sql, "UPDATE t SET qty = -2 WHERE id NOT IN ", list.items)));
    try std.testing.expectEqual(unlisted, try countRows(allocator, db, "SELECT COUNT(*) FROM t WHERE qty = -2"));

    // An OR over two columns gives no row-group hint but must still read both.
    var either_hits: usize = 0;
    for (1..max_id + 1) |id| {
        if (id != 17 and !listed[id] and (id < 40 or id % 50 == 49)) either_hits += 1;
    }
    try std.testing.expectEqual(@as(u64, either_hits), try affectedRows(allocator, db, "UPDATE t SET qty = -3 WHERE qty = -2 AND (id < 40 OR region = 'r49')"));
    try std.testing.expectEqual(either_hits, try countRows(allocator, db, "SELECT COUNT(*) FROM t WHERE qty = -3"));

    try std.testing.expectEqual(@as(u64, live_listed), try affectedRows(allocator, db, try Run.sqlWith(allocator, &sql, "DELETE FROM t WHERE id IN ", list.items)));
    try std.testing.expectEqual(@as(usize, 0), try countRows(allocator, db, try Run.sqlWith(allocator, &sql, "SELECT COUNT(*) FROM t WHERE id IN ", list.items)));
    try std.testing.expectEqual(@as(usize, 0), try countRows(allocator, db, "SELECT COUNT(*) FROM t WHERE qty = -1"));
    try std.testing.expectEqual(unlisted, try countRows(allocator, db, "SELECT COUNT(*) FROM t"));

    var region_hits: usize = 0;
    for (1..max_id + 1) |id| {
        if (id != 17 and !listed[id] and id % 50 < 10) region_hits += 1;
    }
    try std.testing.expectEqual(@as(u64, region_hits), try affectedRows(allocator, db, "DELETE FROM t WHERE region IN ('r0', 'r1', 'r2', 'r3', 'r4', 'r5', 'r6', 'r7', 'r8', 'r9', 'none')"));
    try std.testing.expectEqual(unlisted - region_hits, try countRows(allocator, db, "SELECT COUNT(*) FROM t"));
    try std.testing.expectEqual(@as(usize, 0), try countRows(allocator, db, "SELECT COUNT(*) FROM t WHERE region IN ('r0', 'r5', 'r9')"));
}
