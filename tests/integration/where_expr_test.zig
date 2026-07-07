//! WHERE over a scalar function of a column (`WHERE UPPER(s)='X'`,
//! `WHERE LENGTH(s)=3`). The parser lowers the function LHS to a derived
//! `__pred_expr` in a Compute below the Filter; the scan-select builder must
//! run that Filter AFTER the Compute (not push it into the raw scan). A
//! predicate over only base columns still pushes into the scan.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSql = helpers.runSql;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE ft (id BIGINT PRIMARY KEY, s STRING NOT NULL, n INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO ft (id, s, n) VALUES (1,'abc',10),(2,'XYZ',20),(3,'abc',30),(4,'de',40)");
    const t = try db.openTable("ft", .{});
    try t.flush();
    return db;
}

fn ids(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]i64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(i64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| for (b.values[0].data.bigint[0..b.row_count]) |v| try out.append(allocator, v);
    return out.toOwnedSlice(allocator);
}

test "WHERE scalar-function-of-column compiles and filters correctly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Function LHS — the case that used to fail with "Unknown column".
    {
        const r = try ids(allocator, db, "SELECT id FROM ft WHERE UPPER(s) = 'ABC' ORDER BY id");
        defer allocator.free(r);
        try std.testing.expectEqualSlices(i64, &.{ 1, 3 }, r);
    }
    {
        const r = try ids(allocator, db, "SELECT id FROM ft WHERE LENGTH(s) = 3 ORDER BY id");
        defer allocator.free(r);
        try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, r);
    }
    // Mixed: a computed conjunct AND a base-column conjunct.
    {
        const r = try ids(allocator, db, "SELECT id FROM ft WHERE UPPER(s) = 'ABC' AND id > 1 ORDER BY id");
        defer allocator.free(r);
        try std.testing.expectEqualSlices(i64, &.{3}, r);
    }
    // A base-column-only predicate still works (scan pushdown path unchanged).
    {
        const r = try ids(allocator, db, "SELECT id FROM ft WHERE s = 'abc' ORDER BY id");
        defer allocator.free(r);
        try std.testing.expectEqualSlices(i64, &.{ 1, 3 }, r);
    }
    // Function LHS combined with ORDER BY + the projection dropping the derived col.
    {
        const r = try ids(allocator, db, "SELECT id FROM ft WHERE LENGTH(s) = 2 ORDER BY id DESC");
        defer allocator.free(r);
        try std.testing.expectEqualSlices(i64, &.{4}, r);
    }
}
