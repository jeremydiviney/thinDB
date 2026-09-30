//! HAVING — post-aggregate filter. References either grouped columns
//! or aggregate aliases. Validated against the GroupBy output schema.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, region VARCHAR(8) NOT NULL, qty INT NOT NULL)");
    try exec(
        allocator,
        db,
        "INSERT INTO t (id, region, qty) VALUES (1, 'east', 10), (2, 'east', 20), (3, 'west', 5), (4, 'north', 100), (5, 'east', 30)",
    );
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

test "HAVING: filters by aggregate alias" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(
        allocator,
        db,
        "SELECT region, COUNT(id) AS cnt FROM t GROUP BY region HAVING cnt > 1 ORDER BY region ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectEqualStrings("east", batch.values[0].data.varchar.rowBytes(0));
    try std.testing.expectEqual(@as(i64, 3), batch.values[1].data.bigint[0]);
}

test "HAVING: filters by grouped column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(
        allocator,
        db,
        "SELECT region, SUM(qty) AS total FROM t GROUP BY region HAVING region = 'east'",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectEqualStrings("east", batch.values[0].data.varchar.rowBytes(0));
    try std.testing.expectEqual(@as(i64, 60), batch.values[1].data.bigint[0]);
}

fn setupTags(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE tags (id BIGINT PRIMARY KEY, region VARCHAR(16) NOT NULL, def INT NOT NULL)");
    // east spans defs {1,2}; west only {1} (twice, so COUNT(*) can't stand in
    // for the distinct count); north only {3}.
    try exec(
        allocator,
        db,
        "INSERT INTO tags (id, region, def) VALUES (1, 'east', 1), (2, 'east', 2), (3, 'west', 1), (4, 'west', 1), (5, 'north', 3)",
    );
    const t = try db.openTable("tags", .{});
    try t.flush();
    return db;
}

// Regression: the shape lane's plan gate accepted a COUNT(DISTINCT) alias in
// HAVING by name, but the emit-filter evaluator had no arm for the func and
// treated every leaf as false — silently returning zero rows while the
// projected count itself was correct.
test "HAVING: filters by COUNT(DISTINCT) alias" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupTags(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(
        allocator,
        db,
        "SELECT region, COUNT(DISTINCT def) AS dc FROM tags GROUP BY region HAVING dc = 2",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectEqualStrings("east", batch.values[0].data.varchar.rowBytes(0));
    try std.testing.expectEqual(@as(i64, 2), batch.values[1].data.bigint[0]);
}

test "HAVING: COUNT(DISTINCT) alias with >= over an int group key" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupTags(allocator, io, tmp.dir);
    defer db.close();

    // The distinct column must be an integer (this lane declines string
    // distinct columns loudly) — id is the distinct payload here.
    var q = try runSql(
        allocator,
        db,
        "SELECT def, COUNT(DISTINCT id) AS rc FROM tags GROUP BY def HAVING rc >= 2 ORDER BY def ASC",
    );
    defer q.deinit();
    // def 1 covers ids {1,3,4} (rc=3); defs 2 and 3 cover one id each.
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectEqual(@as(i64, 1), batch.values[0].data.int[0]);
    try std.testing.expectEqual(@as(i64, 3), batch.values[1].data.bigint[0]);
}

test "HAVING: rejected without GROUP BY / aggregates" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const err = thindb.sql.parse(arena.allocator(), "SELECT id FROM t HAVING id > 1");
    try std.testing.expectError(thindb.sql.ParseError.SqlInvalidProjection, err);
}

/// Every row as its cells joined by `|` (NULL spelled out, a double to one
/// decimal), rows joined by newlines, in the order the query returns them.
fn rowsText(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]u8 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |batch| {
        for (0..batch.row_count) |row| {
            if (out.items.len > 0) try out.append(allocator, '\n');
            for (batch.values, 0..) |col, c| {
                if (c > 0) try out.append(allocator, '|');
                if (!col.isValid(row)) {
                    try out.appendSlice(allocator, "NULL");
                    continue;
                }
                switch (col.data) {
                    .string, .varchar, .char => |sv| try out.appendSlice(allocator, sv.rowBytes(row)),
                    inline .tinyint, .smallint, .int, .bigint, .largeint => |s| try out.print(allocator, "{d}", .{s[row]}),
                    .double => |s| try out.print(allocator, "{d:.1}", .{s[row]}),
                    else => return error.TestUnexpectedType,
                }
            }
        }
    }
    return out.toOwnedSlice(allocator);
}

// Expected rows are DuckDB's for the same data and statements (GROUP_CONCAT
// as string_agg, PERCENTILE_CONT as quantile_cont).
const repeated_aggregate_cases = .{
    .{ .sql = "SELECT k, MIN(s), COUNT(*) FROM r GROUP BY k HAVING MIN(s) <> '' AND COUNT(*) > 1 ORDER BY k", .want = "east|a|4\nnorth|e|3" },
    .{ .sql = "SELECT k, COUNT(*) FROM r GROUP BY k HAVING min(S) <> 'b' AND count(*) > 1 ORDER BY k", .want = "east|4\nnorth|3\nwest|2" },
    .{ .sql = "SELECT k, SUM(v) - COUNT(*) AS m, COUNT(*) AS c FROM r GROUP BY k ORDER BY k", .want = "east|56|4\nnorth|111|3\nsouth|0|1\nwest|8|2" },
    .{ .sql = "SELECT k, SUM(v + 1) AS s1 FROM r GROUP BY k ORDER BY SUM(V + 1) DESC, k LIMIT 2", .want = "north|117\neast|63" },
    .{ .sql = "SELECT k, COUNT(v) AS a, COUNT(DISTINCT v) AS b FROM r GROUP BY k HAVING COUNT(DISTINCT v) > 1 ORDER BY k", .want = "east|3|3\nnorth|3|2" },
    .{ .sql = "SELECT k, GROUP_CONCAT(s ORDER BY id) AS a, GROUP_CONCAT(s ORDER BY id SEPARATOR ';') AS b FROM r GROUP BY k HAVING GROUP_CONCAT(s ORDER BY id) <> '' ORDER BY k", .want = "east|b,a,c,b|b;a;c;b\nnorth|e,e|e;e\nsouth|f|f\nwest|,d|;d" },
    .{ .sql = "SELECT k, GROUP_CONCAT(s ORDER BY id) AS a, GROUP_CONCAT(s ORDER BY id DESC) AS b FROM r GROUP BY k HAVING GROUP_CONCAT(s ORDER BY id DESC) <> 'e,e' ORDER BY k", .want = "east|b,a,c,b|b,c,a,b\nsouth|f|f\nwest|,d|d," },
    .{ .sql = "SELECT k, PERCENTILE_CONT(v, 0.5) AS p50, PERCENTILE_CONT(v, 0.9) AS p90 FROM r GROUP BY k HAVING PERCENTILE_CONT(v, 0.5) > 6 ORDER BY k", .want = "east|20.0|28.0\nnorth|7.0|81.4" },
    .{ .sql = "SELECT SUM(v) - COUNT(*), COUNT(*), MAX(v) FROM r HAVING MAX(v) > 0", .want = "175|10|100" },
    .{ .sql = "SELECT k, COUNT(*) AS a, COUNT(*) AS b FROM r GROUP BY k ORDER BY b DESC, k", .want = "east|4|4\nnorth|3|3\nwest|2|2\nsouth|1|1" },
    .{ .sql = "SELECT g, SUM(v) AS sv FROM r GROUP BY g HAVING SUM(v) > 5 ORDER BY SUM(v) DESC LIMIT 3", .want = "1|110\n2|32\n4|31" },
    .{ .sql = "SELECT g, MAX(v) AS mx, MAX(v) + MIN(v) AS spread FROM r GROUP BY g HAVING MAX(v) - MIN(v) >= 0 ORDER BY MAX(v), g", .want = "3|7|12\n2|20|25\n4|30|31\n1|100|110" },
    .{ .sql = "SELECT k, COUNT(DISTINCT s) AS ds FROM r GROUP BY k HAVING COUNT(DISTINCT S) >= 2 AND COUNT(s) > 2 ORDER BY k", .want = "east|3" },
    .{ .sql = "SELECT k, COUNT(*) FROM r GROUP BY ROLLUP(k) HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC", .want = "NULL|10\neast|4\nnorth|3\nwest|2" },
    .{ .sql = "SELECT k, COUNT(*) AS c, SUM(COUNT(*)) OVER () AS total, RANK() OVER (ORDER BY COUNT(*) DESC) AS rk FROM r GROUP BY k ORDER BY k", .want = "east|4|10|1\nnorth|3|10|2\nsouth|1|10|4\nwest|2|10|3" },
    .{ .sql = "SELECT DISTINCT MAX(v) - MIN(v) AS d FROM r GROUP BY g HAVING MAX(v) > 0 ORDER BY MAX(v) - MIN(v) DESC", .want = "90\n29\n15\n2" },
};

test "HAVING: a repeated aggregate reads the one the statement computes, on every route" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();
    try exec(allocator, db, "CREATE TABLE r (id BIGINT PRIMARY KEY, k VARCHAR(8) NOT NULL, g BIGINT NOT NULL, v BIGINT, s VARCHAR(8))");
    try exec(
        allocator,
        db,
        "INSERT INTO r (id, k, g, v, s) VALUES (1, 'east', 1, 10, 'b'), (2, 'east', 2, 20, 'a'), (3, 'east', 1, NULL, 'c'), " ++
            "(4, 'west', 2, 5, ''), (5, 'west', 3, 5, 'd'), (6, 'north', 1, 100, NULL), (7, 'north', 3, 7, 'e'), " ++
            "(8, 'north', 2, 7, 'e'), (9, 'south', 4, 1, 'f'), (10, 'east', 4, 30, 'b')",
    );
    const t = try db.openTable("r", .{});
    try t.flush();

    const saved = thindb.exec.force_group_by;
    defer thindb.exec.force_group_by = saved;
    const Route = @TypeOf(saved);
    // The derived table runs the statement over a buffered stage.
    const routes = [_]struct { force: Route, source: []const u8 }{
        .{ .force = .auto, .source = "FROM r " },
        .{ .force = .hash, .source = "FROM r " },
        .{ .force = .sort, .source = "FROM r " },
        .{ .force = .radix, .source = "FROM r " },
        .{ .force = .auto, .source = "FROM (SELECT * FROM r) AS r " },
    };
    for (routes) |route| {
        thindb.exec.force_group_by = route.force;
        inline for (repeated_aggregate_cases) |c| {
            const sql = try std.mem.replaceOwned(u8, allocator, c.sql, "FROM r ", route.source);
            defer allocator.free(sql);
            const got = try rowsText(allocator, db, sql);
            defer allocator.free(got);
            std.testing.expectEqualStrings(c.want, got) catch |err| {
                std.debug.print("route {s}: {s}\n", .{ @tagName(route.force), sql });
                return err;
            };
        }
    }
}

/// Run `sql` to completion and return its row count and charged peak.
fn countAndPeak(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !struct { rows: usize, peak: usize } {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var rows: usize = 0;
    while (try q.next()) |batch| rows += batch.row_count;
    return .{ .rows = rows, .peak = q.cq.ctx.accountant.?.peak_bytes };
}

test "HAVING: repeating a SELECT aggregate keeps no second state per group" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{
        .query_memory_budget = 1 << 30,
        .memory_budget = 1 << 30,
        .auto_flush_secs = 0,
        .max_dop = 1,
    });
    defer db.close();
    const t = try db.table("p", .{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "g", .type = .bigint },
            .{ .name = "s", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = false,
    }, .{ .order_key = &.{"id"}, .unique = false });
    const Row = struct { id: i64, g: i64, s: []const u8 };
    const rows = 120_000;
    const texts = try allocator.alloc([24]u8, rows);
    defer allocator.free(texts);
    const batch = try allocator.alloc(Row, rows);
    defer allocator.free(batch);
    for (batch, texts, 0..) |*row, *text, i| {
        _ = try std.fmt.bufPrint(text, "{d:0>24}", .{(i * 7919) % rows});
        row.* = .{ .id = @intCast(i), .g = @intCast(i % 60_000), .s = text };
    }
    try t.insert(batch);
    try t.flush();

    const named = try countAndPeak(allocator, db, "SELECT g, MIN(s) AS m, COUNT(*) AS c FROM p GROUP BY g HAVING m <> '' AND c > 1");
    const repeated = try countAndPeak(allocator, db, "SELECT g, MIN(s), COUNT(*) FROM p GROUP BY g HAVING MIN(s) <> '' AND COUNT(*) > 1");
    try std.testing.expectEqual(@as(usize, 60_000), named.rows);
    try std.testing.expectEqual(named.rows, repeated.rows);
    try std.testing.expect(repeated.peak <= named.peak + named.peak / 20);
}
