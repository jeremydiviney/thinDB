//! MySQL-style user-defined session variables: `SET @name = expr`,
//! reference with `@name` in any expression position. Resolution
//! happens at the pre-compile pass — by the time operators see the
//! IR, vars are baked in as literals. So queries with vars get the
//! same predicate-pushdown / stats-pruning treatment as queries with
//! hard-coded literals.

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
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, name VARCHAR(16) NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO t (id, qty, name) VALUES " ++
            "(1, 10, 'alpha'), (2, 20, 'beta'), (3, 30, 'gamma'), (4, 40, 'delta')",
    );
    const tt = try db.openTable("t", .{});
    try tt.flush();
    return db;
}

test "session var: SET @x = 5; SELECT WHERE qty > @x" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SET @cutoff = 15; SELECT id FROM t WHERE qty > @cutoff ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3, 4 }, ids);
}

test "session var: re-SET between statements changes the predicate" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SET @cutoff = 15; SET @cutoff = 30; SELECT id FROM t WHERE qty > @cutoff ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{4}, ids);
}

test "session var: text type" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SET @target = 'beta'; SELECT id FROM t WHERE name = @target",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{2}, ids);
}

test "session var: type widening — INT var into BIGINT column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // @id is INT (5 fits in i32); id col is BIGINT.
    const ids = try collectBigints(allocator, db,
        "SET @id = 3; SELECT id FROM t WHERE id = @id",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{3}, ids);
}

test "session var: var in SELECT expression position" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db,
        "SET @bonus = 100; SELECT id, qty + @bonus AS adj FROM t WHERE id = 1",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectEqual(@as(i32, 110), batch.values[1].data.int[0]);
}

test "session var: undefined var resolves to NULL" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // @undefined was never SET. MySQL treats a reference to an unset user
    // variable as SQL NULL (not an error), so `qty > @undefined` is UNKNOWN
    // under 3VL and excludes every row.
    const ids = try collectBigints(allocator, db,
        "SELECT id FROM t WHERE qty > @undefined",
    );
    defer allocator.free(ids);
    try std.testing.expectEqual(@as(usize, 0), ids.len);
}

test "session var: scalar subquery as RHS of SET" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // SET @x = (SELECT MAX(qty) FROM t) → @x = 40.
    // Then WHERE qty < @x → ids 1, 2, 3.
    const ids = try collectBigints(allocator, db,
        "SET @max_qty = (SELECT MAX(qty) FROM t); " ++
            "SELECT id FROM t WHERE qty < @max_qty ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, ids);
}
