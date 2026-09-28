//! `WITH RECURSIVE`: anchor + recursive arms iterated to a fixpoint.
//! Expected values are MySQL 8.4's for the same statements.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSql = helpers.runSql;

/// Every row as `a,b,...` (NULL spelled out), rows joined by `;`, in the
/// order the query returns them.
fn rowsText(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]u8 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |batch| {
        for (0..batch.row_count) |row| {
            if (out.items.len > 0) try out.append(allocator, ';');
            for (batch.values, 0..) |col, c| {
                if (c > 0) try out.append(allocator, ',');
                if (!col.isValid(row)) {
                    try out.appendSlice(allocator, "NULL");
                    continue;
                }
                switch (col.data) {
                    .string, .varchar, .char => |sv| try out.appendSlice(allocator, sv.rowBytes(row)),
                    inline .tinyint, .smallint, .int, .bigint => |s| try out.print(allocator, "{d}", .{s[row]}),
                    else => return error.TestUnexpectedType,
                }
            }
        }
    }
    return out.toOwnedSlice(allocator);
}

fn expectRows(allocator: std.mem.Allocator, db: anytype, sql: []const u8, expected: []const u8) !void {
    const got = try rowsText(allocator, db, sql);
    defer allocator.free(got);
    try std.testing.expectEqualStrings(expected, got);
}

/// `sql` fails with `expected` at parse, compile or run time.
fn expectFailure(allocator: std.mem.Allocator, db: anytype, sql: []const u8, expected: anyerror) !void {
    var q = runSql(allocator, db, sql) catch |err| return std.testing.expectEqual(expected, err);
    defer q.deinit();
    while (q.next() catch |err| return std.testing.expectEqual(expected, err)) |_| {}
    return error.TestUnexpectedSuccess;
}

/// `edges`: a tree 1 -> {2, 3}, 2 -> 4, 3 -> 5, 5 -> 6; `cyc` adds 6 -> 1.
/// `bits`: 0 and 1.
fn openGraph(allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE edges (id INT NOT NULL, parent INT NOT NULL, child INT NOT NULL, PRIMARY KEY (id))");
    try exec(allocator, db, "INSERT INTO edges (id, parent, child) VALUES (1, 1, 2), (2, 1, 3), (3, 2, 4), (4, 3, 5), (5, 5, 6)");
    try exec(allocator, db, "CREATE TABLE cyc (id INT NOT NULL, parent INT NOT NULL, child INT NOT NULL, PRIMARY KEY (id))");
    try exec(allocator, db, "INSERT INTO cyc (id, parent, child) VALUES (1, 1, 2), (2, 1, 3), (3, 2, 4), (4, 3, 5), (5, 5, 6), (6, 6, 1)");
    try exec(allocator, db, "CREATE TABLE bits (b INT NOT NULL, PRIMARY KEY (b))");
    try exec(allocator, db, "INSERT INTO bits (b) VALUES (0), (1)");
    return db;
}

test "recursive cte: counts 1..N" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const cases = .{
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r", .rows = "1;2;3" },
        .{ .sql = "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT r.n + 1 FROM r WHERE r.n < 5) SELECT n FROM r ORDER BY n", .rows = "1;2;3;4;5" },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 1000) SELECT COUNT(*), SUM(n), MAX(n) FROM r", .rows = "1000,500500,1000" },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT n + 10 FROM r WHERE n < 20) SELECT n FROM r ORDER BY n", .rows = "1;2;11;12;21;22" },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 UNION ALL SELECT n + 10 FROM r WHERE n < 3) SELECT n FROM r ORDER BY n", .rows = "1;2;3;11;12" },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 WHERE 1 = 0 UNION ALL SELECT n + 1 FROM r) SELECT COUNT(*) FROM r", .rows = "0" },
    };
    inline for (cases) |c| try expectRows(allocator, db, c.sql, c.rows);
}

test "recursive cte: LIMIT over the body stops the iteration" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const cases = .{
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r LIMIT 5) SELECT n FROM r ORDER BY n", .rows = "1;2;3;4;5" },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r LIMIT 3 OFFSET 2) SELECT n FROM r ORDER BY n", .rows = "3;4;5" },
    };
    inline for (cases) |c| try expectRows(allocator, db, c.sql, c.rows);
    try expectFailure(allocator, db, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r LIMIT 2000) SELECT n FROM r", error.RecursiveCteDepthExceeded);
}

test "recursive cte: walks a tree and a cyclic graph" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openGraph(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{
            .sql = "WITH RECURSIVE r(node, depth) AS (SELECT 1, 0 UNION ALL SELECT e.child, r.depth + 1 FROM edges e JOIN r ON e.parent = r.node) SELECT node, depth FROM r ORDER BY node",
            .rows = "1,0;2,1;3,1;4,2;5,2;6,3",
        },
        .{
            .sql = "WITH RECURSIVE r(node) AS (SELECT 1 UNION SELECT e.child FROM edges e JOIN r ON e.parent = r.node) SELECT node FROM r ORDER BY node",
            .rows = "1;2;3;4;5;6",
        },
        // The cycle 6 -> 1 closes: UNION DISTINCT finds nothing new and stops.
        .{
            .sql = "WITH RECURSIVE r(node) AS (SELECT 1 UNION SELECT c.child FROM cyc c JOIN r ON c.parent = r.node) SELECT node FROM r ORDER BY node",
            .rows = "1;2;3;4;5;6",
        },
        .{
            .sql = "WITH RECURSIVE r(node) AS (SELECT 1 UNION DISTINCT SELECT c.child FROM r JOIN cyc c ON c.parent = r.node) SELECT COUNT(*) FROM r",
            .rows = "6",
        },
        // UNION ALL over the cycle needs its own bound.
        .{
            .sql = "WITH RECURSIVE r(node, depth) AS (SELECT 1, 0 UNION ALL SELECT c.child, r.depth + 1 FROM cyc c JOIN r ON c.parent = r.node WHERE r.depth < 8) SELECT COUNT(*), MAX(depth) FROM r",
            .rows = "13,8",
        },
        .{
            .sql = "WITH RECURSIVE r(node, path) AS (SELECT 1, CAST('1' AS CHAR(40)) UNION ALL SELECT c.child, CONCAT(r.path, '>', c.child) FROM cyc c JOIN r ON c.parent = r.node WHERE LENGTH(r.path) < 9) SELECT path FROM r WHERE node = 1 ORDER BY path",
            .rows = "1;1>3>5>6>1",
        },
    };
    inline for (cases) |c| try expectRows(allocator, db, c.sql, c.rows);
    try expectFailure(allocator, db, "WITH RECURSIVE r(node) AS (SELECT 1 UNION ALL SELECT c.child FROM cyc c JOIN r ON c.parent = r.node) SELECT node FROM r", error.RecursiveCteDepthExceeded);
}

test "recursive cte: columns take the anchor's types" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const cases = .{
        .{
            .sql = "WITH RECURSIVE r(n, s) AS (SELECT 1, CAST('a' AS CHAR(20)) UNION ALL SELECT n + 1, CONCAT(s, 'b') FROM r WHERE n < 4) SELECT n, s FROM r ORDER BY n",
            .rows = "1,a;2,ab;3,abb;4,abbb",
        },
        // thinDB keeps VARCHAR(n) unbounded, so the grown string survives
        // where MySQL's strict mode refuses it (1406).
        .{
            .sql = "WITH RECURSIVE r(n, s) AS (SELECT 1, 'a' UNION ALL SELECT n + 1, CONCAT(s, 'b') FROM r WHERE n < 3) SELECT n, s FROM r ORDER BY n",
            .rows = "1,a;2,ab;3,abb",
        },
        // A fraction rounds half away from zero into an integer column.
        .{
            .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 0.5 FROM r WHERE n < 3) SELECT n FROM r ORDER BY n",
            .rows = "1;2;3",
        },
        .{
            .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1.5 FROM r WHERE n < 6) SELECT n FROM r ORDER BY n",
            .rows = "1;3;5;7",
        },
        // An integer literal seeds a BIGINT column, as in MySQL.
        .{
            .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n * 1000 FROM r WHERE n < 1000000000000) SELECT n FROM r ORDER BY n",
            .rows = "1;1000;1000000;1000000000;1000000000000",
        },
        .{
            .sql = "WITH RECURSIVE r(n, s) AS (SELECT 1, CAST('0' AS CHAR(10)) UNION ALL SELECT n + 1, n * 7 FROM r WHERE n < 3) SELECT n, s FROM r ORDER BY n",
            .rows = "1,0;2,7;3,14",
        },
        .{
            .sql = "WITH RECURSIVE r(n, x) AS (SELECT 1, 2.50 UNION ALL SELECT n + 1, x * 1.5 FROM r WHERE n < 3) SELECT n, CAST(x AS CHAR) FROM r ORDER BY n",
            .rows = "1,2.50;2,3.75;3,5.63",
        },
        .{
            .sql = "WITH RECURSIVE r(n, s) AS (SELECT 1, CAST(NULL AS CHAR(10)) UNION ALL SELECT n + 1, CONCAT('x', n) FROM r WHERE n < 3) SELECT n, s FROM r ORDER BY n",
            .rows = "1,NULL;2,x1;3,x2",
        },
    };
    inline for (cases) |c| try expectRows(allocator, db, c.sql, c.rows);
}

test "recursive cte: joined, aggregated and read twice" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openGraph(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{
            .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT COUNT(*), SUM(a.n * b.n) FROM r a JOIN r b ON a.n = b.n",
            .rows = "5,55",
        },
        .{
            .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 6) SELECT e.parent, COUNT(*) FROM edges e JOIN r ON e.child = r.n GROUP BY e.parent ORDER BY e.parent",
            .rows = "1,2;2,1;3,1;5,1",
        },
        .{
            .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 4) SELECT n FROM r WHERE n > (SELECT AVG(n) FROM r) ORDER BY n",
            .rows = "3;4",
        },
        .{
            .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r UNION ALL SELECT n * 10 FROM r ORDER BY 1",
            .rows = "1;2;3;10;20;30",
        },
    };
    inline for (cases) |c| try expectRows(allocator, db, c.sql, c.rows);
}

test "recursive cte: iterations wider than one batch" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openGraph(allocator, std.testing.io, tmp.dir);
    defer db.close();

    // Level d holds 2^d rows: the last iteration adds 131072.
    const cases = .{
        .{
            .sql = "WITH RECURSIVE r(n, d) AS (SELECT 1, 0 UNION ALL SELECT r.n * 2 + bits.b, r.d + 1 FROM r, bits WHERE r.d < 17) SELECT COUNT(*), MAX(d), COUNT(DISTINCT n), MAX(n) FROM r",
            .rows = "262143,17,262143,262143",
        },
        .{
            .sql = "WITH RECURSIVE r(n, d) AS (SELECT 1, 0 UNION SELECT r.n * 2 + bits.b - r.n, r.d + 1 FROM r, bits WHERE r.d < 17) SELECT COUNT(*), MAX(d) FROM r",
            .rows = "171,17",
        },
    };
    inline for (cases) |c| try expectRows(allocator, db, c.sql, c.rows);
}

test "recursive cte: ordinary CTEs in the same list" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openGraph(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{
            .sql = "WITH RECURSIVE lim AS (SELECT 4 AS m), r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r, lim WHERE n < lim.m) SELECT n FROM r ORDER BY n",
            .rows = "1;2;3;4",
        },
        .{
            .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3), s AS (SELECT n * 10 AS m FROM r) SELECT m FROM s ORDER BY m",
            .rows = "10;20;30",
        },
        .{
            .sql = "WITH RECURSIVE roots AS (SELECT child FROM edges WHERE parent = 1), r(node, depth) AS (SELECT child, 1 FROM roots UNION ALL SELECT e.child, r.depth + 1 FROM r JOIN edges e ON e.parent = r.node) SELECT node, depth FROM r ORDER BY node",
            .rows = "2,1;3,1;4,2;5,2;6,3",
        },
        .{
            .sql = "WITH RECURSIVE a(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM a WHERE n < 3), b(n) AS (SELECT n FROM a UNION ALL SELECT n + 100 FROM b WHERE n < 100) SELECT n FROM b ORDER BY n",
            .rows = "1;2;3;101;102;103",
        },
    };
    inline for (cases) |c| try expectRows(allocator, db, c.sql, c.rows);
}

test "recursive cte: runaway recursion aborts after 1001 iterations" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    try expectFailure(allocator, db, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT n FROM r", error.RecursiveCteDepthExceeded);
    try expectFailure(allocator, db, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 1001) SELECT COUNT(*) FROM r", error.RecursiveCteDepthExceeded);
    try expectRows(allocator, db, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 1000) SELECT COUNT(*) FROM r", "1000");
}

test "recursive cte: rejected forms" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openGraph(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r", .err = error.SqlRecursiveCteNoUnion },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT n + 1 FROM r WHERE n < 3 UNION ALL SELECT 1) SELECT n FROM r", .err = error.SqlRecursiveCteAnchorFirst },
        .{ .sql = "WITH RECURSIVE r AS (SELECT n + 1 FROM r WHERE n < 3 UNION ALL SELECT 1 AS n) SELECT n FROM r", .err = error.SqlRecursiveCteAnchorFirst },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 UNION ALL SELECT 5) SELECT n FROM r", .err = error.SqlRecursiveCteAnchorFirst },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT COUNT(*) FROM r) SELECT n FROM r", .err = error.SqlRecursiveCteAggregate },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 GROUP BY n) SELECT n FROM r", .err = error.SqlRecursiveCteAggregate },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT ROW_NUMBER() OVER (ORDER BY n) + 1 FROM r WHERE n < 3) SELECT n FROM r", .err = error.SqlRecursiveCteAggregate },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT DISTINCT n + 1 FROM r WHERE n < 3) SELECT n FROM r", .err = error.SqlRecursiveCteDistinct },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 ORDER BY n) SELECT n FROM r", .err = error.SqlRecursiveCteOrderBy },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL (SELECT n + 1 FROM r WHERE n < 3 ORDER BY n)) SELECT n FROM r", .err = error.SqlRecursiveCteOrderBy },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL (SELECT n + 1 FROM r WHERE n < 3 LIMIT 1)) SELECT n FROM r", .err = error.SqlRecursiveCteLimit },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT a.n + 1 FROM r a JOIN r b ON a.n = b.n WHERE a.n < 3) SELECT n FROM r", .err = error.SqlRecursiveCteReference },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT d.n + 1 FROM (SELECT n FROM r) d WHERE d.n < 3) SELECT n FROM r", .err = error.SqlRecursiveCteReference },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 2 FROM edges WHERE EXISTS (SELECT 1 FROM r)) SELECT n FROM r", .err = error.SqlRecursiveCteReference },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT e.child FROM edges e LEFT JOIN r ON r.n = e.parent) SELECT n FROM r", .err = error.SqlRecursiveCteOuterJoin },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 INTERSECT SELECT 2) SELECT n FROM r", .err = error.SqlRecursiveCteSetOp },
    };
    inline for (cases) |c| try expectFailure(allocator, db, c.sql, c.err);
}
