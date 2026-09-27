//! `SELECT *` over a join: every input column is present and named as an
//! explicit column list would name it — bare where unique, qualified where
//! both sides share the name. Regression for the derived-table join whose
//! plain side was pruned down to the join keys (issue #51). USING and
//! NATURAL joins merge their shared columns into one, listed first (#125).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE ext (id BIGINT PRIMARY KEY, projectId INT NOT NULL, externalPlanId VARCHAR(50) NOT NULL, name VARCHAR(50) NOT NULL)");
    try exec(allocator, db, "INSERT INTO ext (id, projectId, externalPlanId, name) VALUES (200, 1, 'p1', 'Plan One'), (201, 1, 'p2', 'Plan Two')");
    try exec(allocator, db, "CREATE TABLE amort (id BIGINT PRIMARY KEY, projectId INT NOT NULL, planId VARCHAR(50) NOT NULL)");
    try exec(allocator, db, "INSERT INTO amort (id, projectId, planId) VALUES (1, 1, 'p1'), (2, 1, 'p1'), (3, 1, 'p2')");
    inline for (.{ "ext", "amort" }) |name| {
        const t = try db.openTable(name, .{});
        try t.flush();
    }
    return db;
}

fn expectNames(schema: []const thindb.Column, expected: []const []const u8) !void {
    try std.testing.expectEqual(expected.len, schema.len);
    for (schema, expected) |col, name| try std.testing.expectEqualStrings(name, col.name);
}

test "SELECT * over a derived-table join keeps every column of the plain side" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db,
        \\SELECT * FROM ext e
        \\INNER JOIN (SELECT planId, projectId FROM amort GROUP BY planId, projectId) tmp
        \\ON tmp.planId = e.externalPlanId
        \\ORDER BY e.id
    );
    defer q.deinit();
    try expectNames(q.outputSchema(), &.{ "id", "e.projectId", "externalPlanId", "name", "planId", "tmp.projectId" });
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| {
        for (0..batch.row_count) |i| try ids.append(allocator, batch.values[0].data.bigint[i]);
    }
    try std.testing.expectEqualSlices(i64, &.{ 200, 201 }, ids.items);
}

test "SELECT * over a plain join names columns like an explicit list" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT * FROM ext e INNER JOIN amort a ON a.planId = e.externalPlanId WHERE a.id = 1");
    defer q.deinit();
    try expectNames(q.outputSchema(), &.{ "e.id", "e.projectId", "externalPlanId", "name", "a.id", "a.projectId", "planId" });
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectEqual(@as(i64, 200), batch.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 1), batch.values[4].data.bigint[0]);
}

fn setupUsing(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE l (id BIGINT PRIMARY KEY, x BIGINT, k BIGINT)");
    try exec(allocator, db, "INSERT INTO l (id, x, k) VALUES (1, 10, 100), (2, 20, 200), (3, 30, NULL)");
    try exec(allocator, db, "CREATE TABLE r (id BIGINT PRIMARY KEY, v BIGINT, k BIGINT)");
    try exec(allocator, db, "INSERT INTO r (id, v, k) VALUES (2, 7, 200), (3, 8, 300), (4, 9, 400)");
    try exec(allocator, db, "CREATE TABLE m (id INT PRIMARY KEY, w BIGINT)");
    try exec(allocator, db, "INSERT INTO m (id, w) VALUES (2, 5), (4, 6)");
    inline for (.{ "l", "r", "m" }) |name| {
        const t = try db.openTable(name, .{});
        try t.flush();
    }
    return db;
}

/// Every cell of an all-integer result, row by row, NULL as null.
fn collectCells(allocator: std.mem.Allocator, q: *helpers.RunResult) ![]?i64 {
    var out: std.ArrayList(?i64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |batch| {
        for (0..batch.row_count) |row| {
            for (batch.values) |column| {
                if (!column.isValid(row)) {
                    try out.append(allocator, null);
                    continue;
                }
                try out.append(allocator, switch (column.data) {
                    .int => |values| values[row],
                    .bigint => |values| values[row],
                    else => return error.TestUnexpectedResult,
                });
            }
        }
    }
    return out.toOwnedSlice(allocator);
}

test "JOIN USING merges each named column and keeps both sides addressable" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupUsing(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT id, x, v FROM l JOIN r USING (id) ORDER BY id", &[_]?i64{ 2, 20, 7, 3, 30, 8 } },
        .{ "SELECT id, r.id FROM l LEFT JOIN r USING (id) ORDER BY id", &[_]?i64{ 1, null, 2, 2, 3, 3 } },
        .{ "SELECT id, l.id FROM l RIGHT OUTER JOIN r USING (id) ORDER BY id", &[_]?i64{ 2, 2, 3, 3, 4, null } },
        .{ "SELECT id, l.id, r.id FROM l FULL JOIN r USING (id) ORDER BY id", &[_]?i64{ 1, 1, null, 2, 2, 2, 3, 3, 3, 4, null, 4 } },
        .{ "SELECT l.id FROM l LEFT JOIN r USING (id) WHERE r.id IS NULL", &[_]?i64{1} },
        .{ "SELECT id, k FROM l JOIN r USING (id, k)", &[_]?i64{ 2, 200 } },
        .{ "SELECT id, a.x, b.v FROM l a JOIN r b USING (id) WHERE id > 2", &[_]?i64{ 3, 30, 8 } },
        .{ "SELECT id, w FROM l JOIN r USING (id) JOIN m USING (id)", &[_]?i64{ 2, 5 } },
        .{ "SELECT id, w FROM l FULL JOIN r USING (id) FULL JOIN m USING (id) ORDER BY id", &[_]?i64{ 1, null, 2, 5, 3, null, 4, 6 } },
        .{ "SELECT id, w FROM l JOIN r USING (id) JOIN m ON m.id = l.id", &[_]?i64{ 2, 5 } },
        .{ "SELECT COUNT(*), SUM(id) FROM l JOIN r USING (id), m", &[_]?i64{ 4, 10 } },
    };
    inline for (cases) |case| {
        inline for (.{ helpers.runSql, helpers.runSqlCtx }) |run| {
            var q = try run(allocator, db, case[0]);
            defer q.deinit();
            const cells = try collectCells(allocator, &q);
            defer allocator.free(cells);
            try std.testing.expectEqualSlices(?i64, case[1], cells);
        }
    }
    try helpers.expectRunError(allocator, db, "SELECT id FROM l JOIN r USING (id, ID)", error.SqlUsingColumnRepeated);
}

test "SELECT * over USING and NATURAL joins lists each merged column once, first" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupUsing(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT * FROM l JOIN r USING (id) ORDER BY id", &[_][]const u8{ "id", "x", "l.k", "v", "r.k" }, &[_]?i64{ 2, 20, 200, 7, 200, 3, 30, null, 8, 300 } },
        .{ "SELECT * FROM l RIGHT JOIN r USING (id) WHERE id = 4", &[_][]const u8{ "id", "v", "r.k", "x", "l.k" }, &[_]?i64{ 4, 9, 400, null, null } },
        .{ "SELECT * FROM l NATURAL JOIN r", &[_][]const u8{ "id", "k", "x", "v" }, &[_]?i64{ 2, 200, 20, 7 } },
        .{ "SELECT * FROM l NATURAL LEFT JOIN m WHERE id < 3 ORDER BY 1", &[_][]const u8{ "id", "x", "k", "w" }, &[_]?i64{ 1, 10, 100, null, 2, 20, 200, 5 } },
        .{ "SELECT *, 1 AS one FROM r JOIN m USING (id) ORDER BY id", &[_][]const u8{ "id", "v", "k", "w", "one" }, &[_]?i64{ 2, 7, 200, 5, 1, 4, 9, 400, 6, 1 } },
        .{ "SELECT l.* FROM l JOIN m USING (id)", &[_][]const u8{ "id", "x", "k" }, &[_]?i64{ 2, 20, 200 } },
        // The hidden key an ON expression stages stays out of `*` and `l.*`.
        .{ "SELECT * FROM l JOIN r ON l.id + 0 = r.id ORDER BY l.id", &[_][]const u8{ "l.id", "x", "l.k", "r.id", "v", "r.k" }, &[_]?i64{ 2, 20, 200, 2, 7, 200, 3, 30, null, 3, 8, 300 } },
        .{ "SELECT l.* FROM l JOIN r ON l.id + 0 = r.id ORDER BY l.id", &[_][]const u8{ "id", "x", "k" }, &[_]?i64{ 2, 20, 200, 3, 30, null } },
    };
    inline for (cases) |case| {
        var q = try helpers.runSqlCtx(allocator, db, case[0]);
        defer q.deinit();
        try expectNames(q.outputSchema(), case[1]);
        const cells = try collectCells(allocator, &q);
        defer allocator.free(cells);
        try std.testing.expectEqualSlices(?i64, case[2], cells);
    }
}

test "a star repeating another item's column names the repeat name_N" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE o (id BIGINT NOT NULL, big BIGINT, PRIMARY KEY (id))");
    try exec(allocator, db, "INSERT INTO o (id, big) VALUES (1, 10), (2, 20)");

    const cases = .{
        .{ "SELECT *, big FROM o ORDER BY id", &[_][]const u8{ "id", "big", "big_1" }, &[_]?i64{ 1, 10, 10, 2, 20, 20 } },
        .{ "SELECT id, o.* FROM o ORDER BY id", &[_][]const u8{ "id", "id_1", "big" }, &[_]?i64{ 1, 1, 10, 2, 2, 20 } },
        .{ "SELECT *, * FROM o ORDER BY id", &[_][]const u8{ "id", "big", "id_1", "big_1" }, &[_]?i64{ 1, 10, 1, 10, 2, 20, 2, 20 } },
        .{ "SELECT big, * FROM o WHERE id = 2", &[_][]const u8{ "big", "id", "big_1" }, &[_]?i64{ 20, 2, 20 } },
        .{ "SELECT d.big_1 FROM (SELECT *, big FROM o) d ORDER BY d.id", &[_][]const u8{"big_1"}, &[_]?i64{ 10, 20 } },
        .{ "WITH c AS (SELECT * FROM o) SELECT *, big FROM c ORDER BY id", &[_][]const u8{ "id", "big", "big_1" }, &[_]?i64{ 1, 10, 10, 2, 20, 20 } },
        .{ "SELECT a.*, b.big FROM o a JOIN o b ON a.id = b.id ORDER BY a.id", &[_][]const u8{ "id", "a.big", "b.big" }, &[_]?i64{ 1, 10, 10, 2, 20, 20 } },
    };
    inline for (cases) |case| {
        var q = try helpers.runSqlCtx(allocator, db, case[0]);
        defer q.deinit();
        expectNames(q.outputSchema(), case[1]) catch |err| {
            std.debug.print("query: {s}\n", .{case[0]});
            return err;
        };
        const cells = try collectCells(allocator, &q);
        defer allocator.free(cells);
        try std.testing.expectEqualSlices(?i64, case[2], cells);
    }
}
