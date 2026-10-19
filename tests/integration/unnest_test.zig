//! `unnest(array, ...)` as a FROM source: on its own inside `TABLE(...)`, and
//! lateral over the FROM items before it, the way StarRocks reads
//! `FROM t, unnest(t.arr)`. Lateral `generate_series` follows the same rules.
//! Expected rows are StarRocks' for the same statements.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSqlMysqlCtx = helpers.runSqlMysqlCtx;

/// Every row as `a,b,...` (NULL spelled out, arrays and maps as StarRocks
/// prints them), rows joined by `;`, in the order the query returns them.
fn rowsText(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]u8 {
    var q = try runSqlMysqlCtx(allocator, db, sql);
    defer q.deinit();
    const schema = q.outputSchema();
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var first = true;
    while (try q.next()) |batch| {
        for (0..batch.row_count) |row| {
            if (!first) try out.append(allocator, ';');
            first = false;
            for (batch.values, 0..) |col, c| {
                if (c > 0) try out.append(allocator, ',');
                if (!col.isValid(row)) {
                    try out.appendSlice(allocator, "NULL");
                    continue;
                }
                switch (col.data) {
                    inline .boolean, .tinyint, .smallint, .int, .bigint, .largeint, .double => |s| try out.print(allocator, "{d}", .{s[row]}),
                    .string, .varchar, .char => |sv| try out.appendSlice(allocator, sv.rowBytes(row)),
                    .nested => |sv| try thindb.exec.nested.appendText(allocator, &out, schema[c].type, sv.rowBytes(row), .starrocks),
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
    errdefer std.debug.print("query: {s}\n", .{sql});
    try std.testing.expectEqualStrings(expected, got);
}

/// `sql` fails with `expected` at parse, compile or run time.
fn expectFailure(allocator: std.mem.Allocator, db: anytype, sql: []const u8, expected: anyerror) !void {
    errdefer std.debug.print("query: {s}\n", .{sql});
    var q = runSqlMysqlCtx(allocator, db, sql) catch |err| return std.testing.expectEqual(expected, err);
    defer q.deinit();
    while (q.next() catch |err| return std.testing.expectEqual(expected, err)) |_| {}
    return error.TestUnexpectedSuccess;
}

/// `student_score` with its first three rows flushed to a segment and the
/// rest in the memtable, so lateral calls read both.
fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE student_score (id BIGINT, scores ARRAY<INT>)");
    try exec(allocator, db, "INSERT INTO student_score VALUES (1, [80, 85, 87]), (2, [77, NULL, 89]), (3, NULL)");
    const t = try db.openTable("student_score", .{});
    try t.flush();
    try exec(allocator, db, "INSERT INTO student_score VALUES (4, []), (5, [90, 92])");
    return db;
}

test "unnest: TABLE(unnest(...)) is a relation of the arrays' elements, zipped" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT * FROM TABLE(unnest(ARRAY<INT>[1, NULL, 10]))", "1;NULL;10" },
        .{ "SELECT x FROM TABLE(unnest([1, NULL, 10])) t(x)", "1;NULL;10" },
        .{ "SELECT unnest FROM unnest([5, 6])", "5;6" },
        .{ "SELECT unnest_1, unnest FROM TABLE(unnest([1, 2], [3]))", "3,1;NULL,2" },
        .{ "SELECT x, y FROM TABLE(unnest([1, NULL], [2, 3])) t(x, y)", "1,2;NULL,3" },
        .{ "SELECT x + 1 FROM TABLE(unnest([1, NULL], [2, 3])) t(x, y)", "2;NULL" },
        .{ "SELECT count(*) FROM TABLE(unnest(ARRAY<INT>[]))", "0" },
        .{ "SELECT t1.x, t2.x FROM TABLE(unnest([1, 2])) t1(x), TABLE(unnest([1, 3])) t2(x) WHERE t1.x = t2.x", "1,1" },
        .{ "SELECT t1.x, t2.x FROM TABLE(unnest([1, 2])) t1(x) LEFT JOIN TABLE(unnest([1, 3])) t2(x) ON t1.x = t2.x ORDER BY t1.x", "1,1;2,NULL" },
        .{ "SELECT t1.x, t2.x FROM TABLE(unnest([1, 2])) t1(x) RIGHT JOIN TABLE(unnest([1, 10, NULL])) t2(x) ON t1.x = t2.x ORDER BY t2.x", "NULL,NULL;1,1;NULL,10" },
        .{ "SELECT s.a FROM TABLE(unnest([named_struct('a', 10, 'b', 'x'), named_struct('a', 20, 'b', 'y')])) u(s)", "10;20" },
        .{ "SELECT u.s.b FROM TABLE(unnest([named_struct('a', 10, 'b', 'x'), named_struct('a', 20, 'b', 'y')])) u(s)", "x;y" },
        .{ "SELECT id, x.a FROM student_score, unnest([named_struct('a', id)]) u(x) WHERE id < 3 ORDER BY id", "1,1;2,2" },
        .{ "SELECT m['a'], m['b'] FROM TABLE(unnest([map{'a': 1, 'b': 2}, map{'a': 2, 'b': 3}])) u(m)", "1,2;2,3" },
        .{ "SELECT x FROM TABLE(unnest([[1, 2], NULL, []])) u(x)", "[1,2];NULL;[]" },
        .{ "SELECT j->>'$.name' FROM TABLE(unnest(array<json>['{\"name\": \"a\"}', '{\"name\": \"b\"}'])) u(j)", "a;b" },
    };
    inline for (cases) |case| try expectRows(allocator, db, case[0], case[1]);

    try exec(allocator, db, "CREATE VIEW uv AS SELECT x FROM TABLE(unnest([3, 4])) t(x)");
    try expectRows(allocator, db, "SELECT x FROM uv ORDER BY x", "3;4");
}

test "unnest: a call over earlier FROM items runs once per row, lateral" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT id, x FROM student_score, unnest(scores) AS u(x) ORDER BY id, x", "1,80;1,85;1,87;2,NULL;2,77;2,89;5,90;5,92" },
        .{ "SELECT id, unnest FROM student_score, unnest(student_score.scores) ORDER BY id, unnest", "1,80;1,85;1,87;2,NULL;2,77;2,89;5,90;5,92" },
        .{ "SELECT id, x FROM student_score LEFT JOIN unnest(scores) AS u(x) ON true ORDER BY id, x", "1,80;1,85;1,87;2,NULL;2,77;2,89;3,NULL;4,NULL;5,90;5,92" },
        .{ "SELECT id, x FROM student_score JOIN LATERAL unnest(scores) u(x) ON true WHERE x > 85 ORDER BY id, x", "1,87;2,89;5,90;5,92" },
        .{ "SELECT id, x FROM student_score JOIN unnest(scores) u(x) ON x > id * 40 ORDER BY id, x", "1,80;1,85;1,87;2,89" },
        .{ "SELECT count(*) FROM student_score CROSS JOIN unnest(scores)", "8" },
        .{ "SELECT id, e1, e2 FROM student_score, unnest(scores, [id]) tmp(e1, e2) ORDER BY id, e1", "1,80,1;1,85,NULL;1,87,NULL;2,NULL,NULL;2,77,2;2,89,NULL;3,NULL,3;4,NULL,4;5,90,5;5,92,NULL" },
        .{ "SELECT id, unnest, unnest_1 FROM student_score LEFT JOIN unnest(scores, []) ON true ORDER BY id, unnest", "1,80,NULL;1,85,NULL;1,87,NULL;2,NULL,NULL;2,77,NULL;2,89,NULL;3,NULL,NULL;4,NULL,NULL;5,90,NULL;5,92,NULL" },
        .{ "SELECT * FROM student_score, unnest(scores) u(x) WHERE id = 1 ORDER BY x", "1,[80,85,87],80;1,[80,85,87],85;1,[80,85,87],87" },
        .{ "SELECT u.* FROM student_score, unnest(scores) u(x) WHERE id = 5 ORDER BY x", "90;92" },
        .{ "SELECT id, x, n FROM student_score, unnest(scores) u(x), generate_series(1, x DIV 45) g(n) WHERE id IN (1, 5) ORDER BY id, x, n", "1,80,1;1,85,1;1,87,1;5,90,1;5,90,2;5,92,1;5,92,2" },
        .{ "SELECT id, sum(x) FROM student_score, unnest(scores) u(x) GROUP BY id ORDER BY id", "1,252;2,166;5,182" },
        .{ "WITH s AS (SELECT id, x FROM student_score, unnest(scores) u(x)) SELECT count(*) FROM s", "8" },
    };
    inline for (cases) |case| try expectRows(allocator, db, case[0], case[1]);

    try exec(allocator, db, "CREATE VIEW sv AS SELECT id, x FROM student_score, unnest(scores) u(x)");
    try expectRows(allocator, db, "SELECT count(*), sum(x) FROM sv", "8,600");
}

test "unnest: lateral generate_series" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT x, y FROM TABLE(generate_series(1, 3)) t(x), LATERAL generate_series(x, x + 1) g(y) ORDER BY x, y", "1,1;1,2;2,2;2,3;3,3;3,4" },
        .{ "SELECT id, n FROM student_score LEFT JOIN generate_series(1, id - 3) g(n) ON true ORDER BY id, n", "1,NULL;2,NULL;3,NULL;4,1;5,1;5,2" },
        .{ "SELECT count(*), sum(n) FROM student_score, generate_series(1, id * 2000) g(n)", "30000,110015000" },
        .{ "SELECT id, count(*) FROM student_score, generate_series(1, id * 2000) g(n) GROUP BY id ORDER BY id", "1,2000;2,4000;3,6000;4,8000;5,10000" },
        .{ "SELECT count(*), sum(n) FROM student_score, generate_series(1, (SELECT max(id) FROM student_score)) g(n)", "25,75" },
        .{ "SELECT count(*) FROM student_score LEFT JOIN generate_series(1, 2) g(n) ON n = id", "5" },
    };
    inline for (cases) |case| try expectRows(allocator, db, case[0], case[1]);
}

test "unnest: joins a lateral call can't take raise" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const unsupported = .{
        "SELECT id FROM student_score LEFT JOIN unnest(scores) u(x) ON false",
        "SELECT id FROM student_score LEFT JOIN unnest(scores) u(x) ON id = x",
        "SELECT id FROM student_score RIGHT JOIN unnest(scores) u(x) ON true",
        "SELECT id FROM student_score FULL JOIN unnest(scores) u(x) ON true",
        "SELECT id FROM student_score JOIN unnest(scores) u(x) USING (x)",
        "SELECT id FROM student_score NATURAL JOIN unnest(scores) u(x)",
        "SELECT id FROM student_score, unnest(scores) u(x) RIGHT JOIN student_score s2 ON true",
    };
    inline for (unsupported) |sql| try expectFailure(allocator, db, sql, error.SqlLateralJoinUnsupported);
    try expectFailure(allocator, db, "SELECT * FROM student_score, unnest(id)", error.UnnestNonArrayArgument);
}
