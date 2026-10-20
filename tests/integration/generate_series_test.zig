//! `generate_series(start, stop[, step])` as a FROM source, bare or inside
//! `TABLE(...)`. Expected values are PostgreSQL's, DuckDB's and StarRocks'
//! for the same statements; the three agree on every case here.

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
                    inline .boolean, .tinyint, .smallint, .int, .bigint, .largeint => |s| try out.print(allocator, "{d}", .{s[row]}),
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

test "generate_series counts from start to stop inclusive, by its step in either direction" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const cases = .{
        .{ "SELECT * FROM TABLE(generate_series(65530, 65536))", "65530;65531;65532;65533;65534;65535;65536" },
        .{ "SELECT * FROM generate_series(-5, 0)", "-5;-4;-3;-2;-1;0" },
        .{ "SELECT * FROM TABLE(generate_series(0, 0))", "0" },
        .{ "SELECT * FROM TABLE(generate_series(0, -2))", "" },
        .{ "SELECT * FROM TABLE(generate_series(1, 4, 2))", "1;3" },
        .{ "SELECT * FROM TABLE(generate_series(1, 1, 2))", "1" },
        .{ "SELECT * FROM generate_series(5, 2, -1)", "5;4;3;2" },
        .{ "SELECT * FROM TABLE(generate_series(-3, -1, -65535))", "" },
        .{ "SELECT * FROM TABLE(generate_series(1, 3, -1))", "" },
        .{ "SELECT * FROM TABLE(generate_series(9223372036854775806, 9223372036854775807, 2))", "9223372036854775806" },
        .{ "SELECT * FROM TABLE(generate_series(9223372036854775807, 9223372036854775807, 9223372036854775807))", "9223372036854775807" },
        .{ "SELECT * FROM TABLE(generate_series(-9223372036854775807, -9223372036854775807 - 1, -1))", "-9223372036854775807;-9223372036854775808" },
    };
    inline for (cases) |c| try expectRows(allocator, db, c[0], c[1]);
}

test "generate_series arguments are expressions; a NULL argument gives no rows" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const cases = .{
        .{ "SELECT sum(x) FROM TABLE(generate_series(1, 4096 * 2 + 1)) t(x)", "33566721" },
        .{ "SELECT count(*) FROM TABLE(generate_series(10000 - 1, 20000))", "10002" },
        .{ "SELECT * FROM TABLE(generate_series(CAST(10 AS TINYINT), CAST(20 AS TINYINT), CAST(127 AS TINYINT)))", "10" },
        .{ "SELECT * FROM TABLE(generate_series(CAST(32760 AS SMALLINT), CAST(32767 AS SMALLINT), CAST(3 AS SMALLINT)))", "32760;32763;32766" },
        .{ "SELECT * FROM generate_series(1, (SELECT 3))", "1;2;3" },
        .{ "SELECT * FROM TABLE(generate_series(1, NULL))", "" },
        .{ "SELECT * FROM TABLE(generate_series(NULL, 3))", "" },
        .{ "SELECT * FROM TABLE(generate_series(1, 3, NULL))", "" },
    };
    inline for (cases) |c| try expectRows(allocator, db, c[0], c[1]);
}

test "generate_series is named after itself unless an alias and column list rename it" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const cases = .{
        .{ "SELECT generate_series FROM generate_series(1, 2)", "1;2" },
        .{ "SELECT g.generate_series FROM TABLE(generate_series(1, 2)) g", "1;2" },
        .{ "SELECT x FROM TABLE(generate_series(1, 3)) t(x) WHERE x > 1", "2;3" },
        .{ "SELECT t.x * 10 FROM generate_series(1, 2) AS t(x)", "10;20" },
        .{ "SELECT x % 3 AS k, count(*) FROM TABLE(generate_series(1, 10)) t(x) GROUP BY k ORDER BY k", "0,3;1,4;2,3" },
        .{ "SELECT a.x, b.y FROM TABLE(generate_series(1, 2)) a(x), TABLE(generate_series(5, 6)) b(y) ORDER BY a.x, b.y", "1,5;1,6;2,5;2,6" },
        .{ "SELECT x FROM (SELECT x FROM generate_series(3, 1, -1) t(x)) d ORDER BY x", "1;2;3" },
    };
    inline for (cases) |c| try expectRows(allocator, db, c[0], c[1]);

    var q = try runSql(allocator, db, "SELECT * FROM TABLE(generate_series(1, 2)) t(x)");
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqual(@as(usize, 1), schema.len);
    try std.testing.expectEqualStrings("x", thindb.types.unqualifiedName(schema[0].name));
    try std.testing.expect(schema[0].type == .bigint);
}

test "generate_series feeds INSERT ... SELECT and joins a table" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE t2 (c0 INT NOT NULL, PRIMARY KEY (c0))");
    try exec(allocator, db, "INSERT INTO t2 SELECT * FROM TABLE(generate_series(100, 120, 3))");
    try expectRows(allocator, db, "SELECT c0 FROM t2 ORDER BY c0", "100;103;106;109;112;115;118");
    try expectRows(allocator, db, "SELECT c0, x FROM t2 JOIN TABLE(generate_series(100, 104)) s(x) ON c0 = x ORDER BY c0", "100,100;103,103");
    try expectRows(allocator, db, "SELECT count(*) FROM t2, generate_series(1, 3)", "21");
}

test "generate_series rejects a zero step, non-integer arguments and a wrong argument count" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const cases = .{
        .{ "SELECT * FROM TABLE(generate_series(1, 2, 0))", error.SeriesStepZero },
        .{ "SELECT * FROM TABLE(generate_series(1.5, 3))", error.SeriesNonIntegerArgument },
        .{ "SELECT * FROM generate_series('a', 3)", error.SeriesNonIntegerArgument },
        .{ "SELECT * FROM TABLE(generate_series(1))", error.SqlFunctionArgMismatch },
        .{ "SELECT * FROM generate_series(1, 2, 3, 4)", error.SqlFunctionArgMismatch },
        .{ "SELECT * FROM TABLE(generate_series(1, 2)) t(x, y)", error.SqlColumnListCountMismatch },
    };
    inline for (cases) |c| try expectFailure(allocator, db, c[0], c[1]);
}

test "generate_series takes its arguments by name, in any order" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const cases = .{
        .{ "SELECT * FROM TABLE(generate_series(start=>2, end=>5))", "2;3;4;5" },
        .{ "SELECT * FROM TABLE(generate_series(end=>5, start=>2))", "2;3;4;5" },
        .{ "SELECT * FROM TABLE(generate_series(start=>2, end=>5, step=>2))", "2;4" },
        .{ "SELECT * FROM TABLE(generate_series(step => 2, START => 2, End => 5))", "2;4" },
        .{ "SELECT * FROM generate_series(start => 1 + 1, end => (SELECT 3))", "2;3" },
        .{ "SELECT * FROM TABLE(generate_series(start=>2, end=>NULL))", "" },
    };
    inline for (cases) |c| try expectRows(allocator, db, c[0], c[1]);

    const failures = .{
        .{ "SELECT * FROM TABLE(generate_series(start=>2, stop=>5))", error.SqlFunctionArgMismatch },
        .{ "SELECT * FROM TABLE(generate_series(start=>2))", error.SqlFunctionArgMismatch },
        .{ "SELECT * FROM TABLE(generate_series(step=>2))", error.SqlFunctionArgMismatch },
        .{ "SELECT * FROM TABLE(generate_series(start=>2, step=>2, end=>4, start=>3))", error.SqlFunctionArgMismatch },
        .{ "SELECT * FROM TABLE(generate_series(start=>2, 5))", error.SqlExpectedIdent },
        .{ "SELECT * FROM TABLE(generate_series(start=>2, =>5))", error.SqlExpectedIdent },
        .{ "SELECT * FROM TABLE(generate_series(end=>5, start=>2, step = 2))", error.SqlExpectedToken },
        .{ "SELECT * FROM TABLE(generate_series(start=>2, end=>4, step->3))", error.SqlExpectedToken },
    };
    inline for (failures) |c| try expectFailure(allocator, db, c[0], c[1]);
}
