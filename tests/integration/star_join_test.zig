//! `SELECT *` over a join: every input column is present and named as an
//! explicit column list would name it — bare where unique, qualified where
//! both sides share the name. Regression for the derived-table join whose
//! plain side was pruned down to the join keys (issue #51).

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
