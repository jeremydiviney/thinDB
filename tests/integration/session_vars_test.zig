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
    try exec(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, name VARCHAR(16) NOT NULL)",
    );
    try exec(
        allocator,
        db,
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

    const ids = try collectBigints(
        allocator,
        db,
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

    const ids = try collectBigints(
        allocator,
        db,
        "SET @cutoff = 15; SET @cutoff = 30; SELECT id FROM t WHERE qty > @cutoff ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{4}, ids);
}

test "session var: fractional constant" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
        "SET @cutoff = 25.5; SET @floor = -.5; SELECT id FROM t WHERE qty > @cutoff AND qty > @floor ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 3, 4 }, ids);
}

test "session var: text type" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
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
    const ids = try collectBigints(
        allocator,
        db,
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

    var q = try runSql(
        allocator,
        db,
        "SET @bonus = 100; SELECT id, qty + @bonus AS adj FROM t WHERE id = 1",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectEqual(@as(i64, 110), batch.values[1].data.bigint[0]);
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
    const ids = try collectBigints(
        allocator,
        db,
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
    const ids = try collectBigints(
        allocator,
        db,
        "SET @max_qty = (SELECT MAX(qty) FROM t); " ++
            "SELECT id FROM t WHERE qty < @max_qty ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, ids);
}

fn expectVarItems(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, names: []const []const u8, values: []const ?i64) !void {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqual(names.len, schema.len);
    for (names, schema) |name, col| try std.testing.expectEqualStrings(name, col.name);
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    for (values, batch.values) |expected, view| {
        if (expected == null) {
            try std.testing.expect(!view.isValid(0));
            continue;
        }
        const actual: i64 = switch (view.data) {
            .bigint => |v| v[0],
            .int => |v| v[0],
            else => return error.TestUnexpectedResult,
        };
        try std.testing.expectEqual(expected.?, actual);
    }
}

test "session var: a bare var is a projection item" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expectVarItems(allocator, db, "SET @w = 5; SELECT @w", &.{"@w"}, &.{5});
    try expectVarItems(allocator, db, "SET @w = 5; SELECT 1, @w", &.{ "1", "@w" }, &.{ 1, 5 });
    try expectVarItems(allocator, db, "SET @w = 5; SELECT @w AS v, id FROM t WHERE id = 2", &.{ "v", "id" }, &.{ 5, 2 });
    try expectVarItems(allocator, db, "SET @w = 5; SELECT @w + qty AS s FROM t WHERE id = 1", &.{"s"}, &.{15});
    try expectVarItems(allocator, db, "SELECT @never_set", &.{"@never_set"}, &.{null});
}

/// `par`: enough row groups for a filtered scan to run at the server's
/// max_dop of 4.
fn openParallel(allocator: std.mem.Allocator, dir: std.Io.Dir) !*thindb.Database {
    const db = try thindb.Database.open(allocator, std.testing.io, dir, .{
        .auto_flush_secs = 0,
        .max_dop = 4,
        .row_group_size = 1024,
    });
    errdefer db.close();
    const t = try db.table("par", .{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "g", .type = .bigint },
        },
        .order_key = &.{"id"},
        .unique = false,
    }, .{ .order_key = &.{"id"}, .unique = false });
    const Row = struct { id: i64, g: i64 };
    const rows = try allocator.alloc(Row, 20_000);
    defer allocator.free(rows);
    for (rows, 0..) |*row, i| row.* = .{ .id = @intCast(i), .g = @intCast(i % 100) };
    try t.insert(rows);
    try t.flush();
    return db;
}

const PARALLEL_SELECT = "SELECT id, g FROM par WHERE g < 3 ORDER BY id";

/// Whether `sql`'s EXPLAIN (the last statement of a batch) scans `par` at
/// `dop`.
fn scansAtDop(allocator: std.mem.Allocator, db: anytype, sql: []const u8, dop: usize) !bool {
    var q = try helpers.runSqlMysql(allocator, db, sql);
    defer q.deinit();
    var plan: std.ArrayList(u8) = .empty;
    defer plan.deinit(allocator);
    while (try q.next()) |b| {
        for (0..b.row_count) |i| {
            try plan.appendSlice(allocator, b.values[0].data.string.rowBytes(i));
            try plan.append(allocator, '\n');
        }
    }
    var want_buf: [64]u8 = undefined;
    const want = try std.fmt.bufPrint(&want_buf, "ParallelScan par (DOP={d},", .{dop});
    const found = std.mem.indexOf(u8, plan.items, want) != null;
    if (!found) std.debug.print("{s}\n{s}", .{ sql, plan.items });
    return found;
}

test "session option: SET thindb_max_dop caps the parallelism of later statements" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openParallel(allocator, tmp.dir);
    defer db.close();

    try std.testing.expect(try scansAtDop(allocator, db, "EXPLAIN " ++ PARALLEL_SELECT, 4));
    inline for (.{ "SET thindb_max_dop = 2", "SET SESSION thindb_max_dop = 2", "SET @@session.thindb_max_dop = 2" }) |set| {
        try std.testing.expect(try scansAtDop(allocator, db, set ++ "; SELECT 1; EXPLAIN " ++ PARALLEL_SELECT, 2));
    }
    try std.testing.expect(try scansAtDop(allocator, db, "SET thindb_max_dop = 2; SET thindb_max_dop = DEFAULT; EXPLAIN " ++ PARALLEL_SELECT, 4));
    try std.testing.expect(try scansAtDop(allocator, db, "SET thindb_max_dop = 64; EXPLAIN " ++ PARALLEL_SELECT, 4));
    // A user variable of the same name is not the option.
    try std.testing.expect(try scansAtDop(allocator, db, "SET @thindb_max_dop = 2; EXPLAIN " ++ PARALLEL_SELECT, 4));

    var reduced = try helpers.runSqlMysql(allocator, db, "SET thindb_max_dop = 1; SELECT COUNT(*) FROM par WHERE g < 3");
    defer reduced.deinit();
    const b = (try reduced.next()).?;
    try std.testing.expectEqual(@as(i64, 600), b.values[0].data.bigint[0]);
}

test "session option: a SET_VAR hint sets thindb_max_dop for its statement alone" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openParallel(allocator, tmp.dir);
    defer db.close();

    try std.testing.expect(try scansAtDop(allocator, db, "EXPLAIN SELECT /*+ SET_VAR(thindb_max_dop = 2) */ id, g FROM par WHERE g < 3 ORDER BY id", 2));
    try std.testing.expect(try scansAtDop(allocator, db, "SELECT /*+ SET_VAR(thindb_max_dop = 2) */ 1; EXPLAIN " ++ PARALLEL_SELECT, 4));
    try std.testing.expect(try scansAtDop(allocator, db, "SET thindb_max_dop = 2; EXPLAIN SELECT /*+ SET_VAR(thindb_max_dop = 3) */ id, g FROM par WHERE g < 3 ORDER BY id", 3));
    try std.testing.expect(try scansAtDop(allocator, db, "SET thindb_max_dop = 2; EXPLAIN SELECT /*+ SET_VAR(thindb_max_dop = 0) */ id, g FROM par WHERE g < 3 ORDER BY id", 4));
}

test "session option: SET thindb_max_dop rejects a value that is not a count" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    try std.testing.expectError(error.TypeMismatch, exec(allocator, db, "SET thindb_max_dop = -1"));
    try std.testing.expectError(error.TypeMismatch, exec(allocator, db, "SET thindb_max_dop = 'four'"));
}
