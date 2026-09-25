//! A reference that can never bind fails the statement even when the item
//! holding it is unused — the dead-column and dead-branch rewrites must not
//! delete it before any operator sees it. Unused VALID items still prune.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE ext (id BIGINT PRIMARY KEY, name VARCHAR(50) NOT NULL, amount DOUBLE NOT NULL)");
    try exec(allocator, db, "INSERT INTO ext (id, name, amount) VALUES (1, 'one', 1.5), (2, 'two', 2.5)");
    const t = try db.openTable("ext", .{});
    try t.flush();
    return db;
}

fn countRows(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) !i64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    const batch = (try q.next()).?;
    return batch.values[0].data.bigint[0];
}

test "unused CTE item with an unknown column fails the statement" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    try helpers.expectRunError(allocator, db,
        \\WITH b AS (SELECT id, nonexistent_col AS x FROM ext) SELECT count(*) FROM b
    , error.ColumnNotFound);
    try helpers.expectRunError(allocator, db,
        \\WITH b AS (SELECT id, upper(nonexistent_col) AS x FROM ext) SELECT count(*) FROM b
    , error.ColumnNotFound);
}

test "unused CTE item calling an unknown function fails the statement" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    try helpers.expectRunError(allocator, db,
        \\WITH b AS (SELECT id, invalid_function(name) AS x FROM ext) SELECT count(*) FROM b
    , error.ComputeNoSuchOverload);
}

test "dead UNION arm with an unknown column fails the statement" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    try helpers.expectRunError(allocator, db,
        \\SELECT id FROM ext UNION ALL SELECT nonexistent_col FROM ext WHERE 1 = 0
    , error.ColumnNotFound);
}

test "unused valid CTE items still run" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    try std.testing.expectEqual(@as(i64, 2), try countRows(allocator, db,
        \\WITH b AS (SELECT e.id, upper(e.name) AS x, amount * 2 AS y FROM ext e)
        \\SELECT count(*) FROM b
    ));
    try std.testing.expectEqual(@as(i64, 2), try countRows(allocator, db,
        \\WITH a AS (SELECT id, amount * 2 AS doubled FROM ext),
        \\     b AS (SELECT id, doubled + 1 AS bumped FROM a)
        \\SELECT count(*) FROM b
    ));
    try std.testing.expectEqual(@as(i64, 2), try countRows(allocator, db,
        \\SELECT count(*) FROM (SELECT id FROM ext UNION ALL SELECT id FROM ext WHERE 1 = 0) u
    ));
}

// The MySQL wire names the culprit from this report. It can't walk the tree
// instead: a failed compile has freed the nodes its rewrites spliced in.
test "a failed compile hands its caller the unbound reference" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ .sql = "WITH b AS (SELECT id, nonexistent_col AS x FROM ext) SELECT count(*) FROM b", .err = error.ColumnNotFound, .column = true, .name = "nonexistent_col" },
        .{ .sql = "WITH b AS (SELECT id, invalid_function(name) AS x FROM ext) SELECT count(*) FROM b", .err = error.ComputeNoSuchOverload, .column = false, .name = "invalid_function" },
    };
    inline for (cases) |c| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const root = try thindb.sql.parse(arena.allocator(), c.sql);
        var unbound: ?thindb.net.Unbound = null;
        defer if (unbound) |u| allocator.free(u.name());
        try std.testing.expectError(c.err, thindb.net.compileWithOptions(allocator, db, .{}, root, .{ .unbound = &unbound }));
        const u = unbound orelse return error.TestExpectedUnbound;
        try std.testing.expectEqual(c.column, u == .column);
        try std.testing.expectEqualStrings(c.name, u.name());
    }
}

test "a compile with nothing unbound reports none" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), "SELECT id FROM ext WHERE amount > 2");
    var unbound: ?thindb.net.Unbound = null;
    var cq = try thindb.net.compileWithOptions(allocator, db, .{}, root, .{ .unbound = &unbound });
    defer cq.deinit();
    try std.testing.expectEqual(@as(?thindb.net.Unbound, null), unbound);
}
