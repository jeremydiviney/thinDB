//! GROUP BY ... WITH ROLLUP, ROLLUP(), CUBE() and GROUPING SETS, with
//! GROUPING(): each grouping set's rows, their rolled-up keys NULL.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;

fn openSales(allocator: std.mem.Allocator, dir: std.Io.Dir, flush: bool) !*thindb.Database {
    const db = try thindb.Database.open(allocator, std.testing.io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE sales (id BIGINT NOT NULL, g INT NOT NULL, h INT, region VARCHAR(8), v INT NOT NULL, PRIMARY KEY (id))");
    try exec(allocator, db, "INSERT INTO sales VALUES (1, 1, 1, 'east', 10), (2, 1, 2, 'west', 20), (3, 2, 1, 'east', 30), (4, 2, 2, 'west', 40), (5, 2, 2, 'west', 50)");
    if (flush) try (try db.openTable("sales", .{})).flush();
    return db;
}

fn expectInts(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, want: []const i64) !void {
    const got = helpers.collectBigints(allocator, db, sql) catch |err| {
        std.debug.print("query: {s}\n", .{sql});
        return err;
    };
    defer allocator.free(got);
    std.testing.expectEqualSlices(i64, want, got) catch |err| {
        std.debug.print("query: {s}\n", .{sql});
        return err;
    };
}

test "WITH ROLLUP adds a subtotal per prefix of the keys and a grand total" {
    const allocator = std.testing.allocator;
    inline for (.{ false, true }) |flush| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try openSales(allocator, tmp.dir, flush);
        defer db.close();

        // Each row encoded as g * 10000 + h * 1000 + SUM(v), a NULL key as 0.
        const cases = .{
            .{ "SELECT COALESCE(g, 0) * 1000 + s FROM (SELECT g, SUM(v) AS s FROM sales GROUP BY g WITH ROLLUP) x ORDER BY 1", &[_]i64{ 150, 1030, 2120 } },
            .{ "SELECT COALESCE(g, 0) * 10000 + COALESCE(h, 0) * 1000 + s FROM (SELECT g, h, SUM(v) AS s FROM sales GROUP BY g, h WITH ROLLUP) x ORDER BY 1", &[_]i64{ 150, 10030, 11010, 12020, 20120, 21030, 22090 } },
            .{ "SELECT COALESCE(g, 0) * 10000 + COALESCE(h, 0) * 1000 + s FROM (SELECT g, h, SUM(v) AS s FROM sales GROUP BY ROLLUP(g, h)) x ORDER BY 1", &[_]i64{ 150, 10030, 11010, 12020, 20120, 21030, 22090 } },
            .{ "SELECT COALESCE(g, 0) * 10000 + COALESCE(h, 0) * 1000 + s FROM (SELECT g, h, SUM(v) AS s FROM sales GROUP BY CUBE(g, h)) x ORDER BY 1", &[_]i64{ 150, 1040, 2110, 10030, 11010, 12020, 20120, 21030, 22090 } },
            .{ "SELECT COALESCE(g, 0) * 10000 + COALESCE(h, 0) * 1000 + s FROM (SELECT g, h, SUM(v) AS s FROM sales GROUP BY GROUPING SETS ((g), (h), ())) x ORDER BY 1", &[_]i64{ 150, 1040, 2110, 10030, 20120 } },
            .{ "SELECT COALESCE(g, 0) * 10000 + COALESCE(h, 0) * 1000 + s FROM (SELECT g, h, SUM(v) AS s FROM sales GROUP BY g, ROLLUP(h)) x ORDER BY 1", &[_]i64{ 10030, 11010, 12020, 20120, 21030, 22090 } },
            // COUNT(*) and a key the SELECT list leaves out.
            .{ "SELECT n FROM (SELECT COUNT(*) AS n FROM sales GROUP BY g WITH ROLLUP) x ORDER BY 1", &[_]i64{ 2, 3, 5 } },
            // HAVING, ORDER BY and LIMIT see the rolled-up rows.
            .{ "SELECT SUM(v) FROM sales GROUP BY g WITH ROLLUP HAVING SUM(v) > 100 ORDER BY 1", &[_]i64{ 120, 150 } },
            .{ "SELECT SUM(v) AS s FROM sales GROUP BY g WITH ROLLUP ORDER BY s DESC LIMIT 2", &[_]i64{ 150, 120 } },
            // No rows, no groups: MySQL returns no grand total either.
            .{ "SELECT COUNT(*) FROM (SELECT g, SUM(v) FROM sales WHERE v > 1000 GROUP BY g WITH ROLLUP) x", &[_]i64{0} },
        };
        inline for (cases) |c| try expectInts(allocator, db, c[0], c[1]);
    }
}

test "GROUPING() tells a rolled-up key's NULL from a NULL key" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openSales(allocator, tmp.dir, false);
    defer db.close();
    try exec(allocator, db, "INSERT INTO sales VALUES (6, 3, NULL, NULL, 5)");

    const cases = .{
        .{ "SELECT GROUPING(g) FROM sales GROUP BY g WITH ROLLUP ORDER BY 1", &[_]i64{ 0, 0, 0, 1 } },
        .{ "SELECT GROUPING(g, h) * 1000 + SUM(v) FROM sales GROUP BY g, h WITH ROLLUP ORDER BY 1", &[_]i64{ 5, 10, 20, 30, 90, 1005, 1030, 1120, 3155 } },
        .{ "SELECT SUM(v) FROM sales GROUP BY h WITH ROLLUP HAVING GROUPING(h) = 0 ORDER BY 1", &[_]i64{ 5, 40, 110 } },
        .{ "SELECT SUM(v) FROM sales GROUP BY h WITH ROLLUP ORDER BY GROUPING(h), SUM(v)", &[_]i64{ 5, 40, 110, 155 } },
        .{ "SELECT CAST(CASE WHEN GROUPING(g) = 1 THEN -1 ELSE g END AS BIGINT) FROM sales GROUP BY g WITH ROLLUP ORDER BY 1", &[_]i64{ -1, 1, 2, 3 } },
        .{ "SELECT CAST(CASE WHEN g + 1 = 3 THEN 0 ELSE g END AS BIGINT) FROM sales GROUP BY g ORDER BY 1", &[_]i64{ 0, 1, 3 } },
        .{ "SELECT CAST(CASE WHEN SUM(v) + g > 100 THEN 1 ELSE 0 END AS BIGINT) FROM sales GROUP BY g ORDER BY 1", &[_]i64{ 0, 0, 1 } },
        .{ "SELECT SUM(CASE WHEN h + 1 = 2 THEN v ELSE 0 END) FROM sales GROUP BY g ORDER BY 1", &[_]i64{ 0, 10, 30 } },
        // Without grouping sets every key is grouped.
        .{ "SELECT GROUPING(g) + g FROM sales GROUP BY g ORDER BY 1", &[_]i64{ 1, 2, 3 } },
    };
    inline for (cases) |c| try expectInts(allocator, db, c[0], c[1]);

    const regions = try helpers.collectStrings(allocator, db, "SELECT COALESCE(region, '(all)') FROM sales WHERE region IS NOT NULL GROUP BY region WITH ROLLUP ORDER BY 1");
    defer helpers.freeStrings(allocator, regions);
    try std.testing.expectEqual(@as(usize, 3), regions.len);
    for ([_][]const u8{ "(all)", "east", "west" }, regions) |want, got| try std.testing.expectEqualStrings(want, got.?);

    try helpers.expectRunError(allocator, db, "SELECT GROUPING(v) FROM sales GROUP BY g WITH ROLLUP", error.SqlInvalidProjection);
    try helpers.expectRunError(allocator, db, "SELECT g FROM sales GROUP BY ROLLUP(g) WITH ROLLUP", error.SqlInvalidProjection);
}
