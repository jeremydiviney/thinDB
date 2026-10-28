//! Unordered `GROUP BY … LIMIT n` emit-cap fusion. With no ORDER BY the
//! result rows are unspecified, so the hash aggregate emits only the first
//! `n + offset` groups (group-insertion order) instead of materializing every
//! group for the downstream Limit to discard. The build still consumes all
//! input, so the emitted groups' counts are exact. A HAVING between the Limit
//! and the GroupBy must NOT be capped — it changes which groups qualify.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

/// 50 groups keyed by `g`, group i has exactly (i % 5) + 1 rows — so the
/// per-group counts are known and verifiable regardless of which subset the
/// cap emits.
fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, g INT NOT NULL)");

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO t (id, g) VALUES ");
    var id: i64 = 1;
    var first = true;
    var grp: i64 = 0;
    var line: [64]u8 = undefined;
    while (grp < 50) : (grp += 1) {
        const reps: i64 = @mod(grp, 5) + 1;
        var r: i64 = 0;
        while (r < reps) : (r += 1) {
            if (!first) try buf.appendSlice(allocator, ", ");
            first = false;
            const s = try std.fmt.bufPrint(&line, "({d}, {d})", .{ id, grp });
            try buf.appendSlice(allocator, s);
            id += 1;
        }
    }
    try exec(allocator, db, buf.items);
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

fn expectedCount(g: i64) i64 {
    return @mod(g, 5) + 1;
}

test "GROUP BY ... LIMIT n (no ORDER BY): exactly n groups, correct counts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT g, COUNT(*) AS c FROM t GROUP BY g LIMIT 10");
    defer q.deinit();

    var rows: usize = 0;
    var seen = std.AutoHashMap(i64, void).init(allocator);
    defer seen.deinit();
    while (try q.next()) |batch| {
        var i: usize = 0;
        while (i < batch.row_count) : (i += 1) {
            const g = batch.values[0].data.int[i];
            const c = batch.values[1].data.bigint[i];
            try std.testing.expectEqual(expectedCount(g), c);
            try std.testing.expect(g >= 0 and g < 50);
            try std.testing.expect(!seen.contains(g)); // no duplicate groups
            try seen.put(g, {});
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 10), rows);
}

test "GROUP BY ... LIMIT n OFFSET m (no ORDER BY): n groups after skipping m" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT g, COUNT(*) AS c FROM t GROUP BY g LIMIT 5 OFFSET 7");
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |batch| {
        var i: usize = 0;
        while (i < batch.row_count) : (i += 1) {
            const g = batch.values[0].data.int[i];
            const c = batch.values[1].data.bigint[i];
            try std.testing.expectEqual(expectedCount(g), c);
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 5), rows);
}

test "GROUP BY ... LIMIT n: limit above group count emits all groups" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT g, COUNT(*) AS c FROM t GROUP BY g LIMIT 1000");
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |batch| rows += batch.row_count;
    try std.testing.expectEqual(@as(usize, 50), rows);
}

test "GROUP BY ... HAVING ... LIMIT: HAVING still filters (no wrong cap)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Groups with count = 5 are exactly those where g % 5 == 4: there are 10
    // of them (g = 4, 9, ..., 49). HAVING c = 5 filters to those. The emit
    // cap must NOT clip the GroupBy before the HAVING runs — capping at 3
    // groups in insertion order would (with the first groups being low-count)
    // wrongly yield zero qualifying rows. We expect exactly 3 rows, each a
    // genuine count-5 group.
    var q = try runSql(allocator, db, "SELECT g, COUNT(*) AS c FROM t GROUP BY g HAVING c = 5 LIMIT 3");
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |batch| {
        var i: usize = 0;
        while (i < batch.row_count) : (i += 1) {
            const g = batch.values[0].data.int[i];
            const c = batch.values[1].data.bigint[i];
            try std.testing.expectEqual(@as(i64, 5), c);
            try std.testing.expectEqual(@as(i64, 4), @mod(g, 5));
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), rows);
}

/// One row: string key `s`, small int key `g`, value `v`.
const KeyRow = struct { s: []const u8, g: i64, v: i64 };

/// Rows of `sql`'s result as `col|col|...` lines, in result order.
fn resultLines(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) !std.ArrayList(u8) {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |batch| {
        for (0..batch.row_count) |r| {
            for (batch.values, 0..) |col, c| {
                if (c != 0) try out.append(allocator, '|');
                switch (col.data) {
                    .string, .varchar, .char => |sv| try out.appendSlice(allocator, sv.rowBytes(r)),
                    .bigint => |vals| try out.print(allocator, "{d}", .{vals[r]}),
                    .int => |vals| try out.print(allocator, "{d}", .{vals[r]}),
                    else => return error.TestUnexpectedType,
                }
            }
            try out.append(allocator, '\n');
        }
    }
    return out;
}

/// The expected `resultLines` of grouping `rows` by `s` (and `g` when
/// `by_g`), ordered by `s` then `g` in the given directions, sliced to
/// LIMIT `n` OFFSET `m`.
fn expectedLines(allocator: std.mem.Allocator, rows: []KeyRow, by_g: bool, s_desc: bool, g_desc: bool, n: usize, m: usize) !std.ArrayList(u8) {
    const Ctx = struct {
        by_g: bool,
        s_desc: bool,
        g_desc: bool,
        fn lt(ctx: @This(), a: KeyRow, b: KeyRow) bool {
            const so = std.mem.order(u8, a.s, b.s);
            if (so != .eq) return if (ctx.s_desc) so == .gt else so == .lt;
            if (!ctx.by_g or a.g == b.g) return false;
            return if (ctx.g_desc) a.g > b.g else a.g < b.g;
        }
    };
    const ctx = Ctx{ .by_g = by_g, .s_desc = s_desc, .g_desc = g_desc };
    std.mem.sort(KeyRow, rows, ctx, Ctx.lt);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var group: usize = 0;
    var i: usize = 0;
    while (i < rows.len) {
        var j = i;
        var sum: i64 = 0;
        while (j < rows.len and !ctx.lt(rows[i], rows[j])) : (j += 1) sum += rows[j].v;
        if (group >= m and group < m + n) {
            try out.appendSlice(allocator, rows[i].s);
            if (by_g) try out.print(allocator, "|{d}", .{rows[i].g});
            try out.print(allocator, "|{d}|{d}\n", .{ j - i, sum });
        }
        group += 1;
        i = j;
    }
    return out;
}

// `ORDER BY <string key> LIMIT n` over a grouped result keeps only the groups
// led by the key's first n values, so each scan drops rows of other values as
// it reads them (a key bound). The kept groups' aggregates must still see
// every one of their rows: across row groups, segments, deleted rows, the
// memtable, a WHERE, and parallel workers.
test "GROUP BY string key ORDER BY key LIMIT: groups exact under the key bound" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .max_dop = 4, .row_group_size = 128 });
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, s VARCHAR(16) NOT NULL, g INT NOT NULL, v BIGINT NOT NULL)");

    const n_rows = 3000;
    const dead_below = 100;
    var keys: [n_rows][16]u8 = undefined;
    var rows: [n_rows]KeyRow = undefined;
    for (&rows, 0..) |*r, i| {
        const s = if (i % 97 == 0) "" else try std.fmt.bufPrint(&keys[i], "k{d}", .{(i * 389) % 900});
        r.* = .{ .s = s, .g = @intCast(i % 3), .v = @intCast(i) };
    }
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);
    const t = try db.openTable("t", .{});
    for ([_][2]usize{ .{ 0, 1000 }, .{ 1000, 2000 }, .{ 2000, n_rows } }) |span| {
        sql.clearRetainingCapacity();
        try sql.appendSlice(allocator, "INSERT INTO t VALUES ");
        for (span[0]..span[1]) |i| {
            if (i != span[0]) try sql.append(allocator, ',');
            try sql.print(allocator, "({d},'{s}',{d},{d})", .{ i, rows[i].s, rows[i].g, rows[i].v });
        }
        try exec(allocator, db, sql.items);
        if (span[1] == 2000) try exec(allocator, db, "DELETE FROM t WHERE id < 100");
        if (span[1] != n_rows) try t.flush();
    }

    const Case = struct { sql: []const u8, by_g: bool = false, s_desc: bool = false, g_desc: bool = false, min_v: i64 = 0, n: usize, m: usize = 0 };
    const cases = [_]Case{
        .{ .sql = "SELECT s, COUNT(*) AS c, SUM(v) AS sv FROM t GROUP BY s ORDER BY s LIMIT 5", .n = 5 },
        .{ .sql = "SELECT s, COUNT(*) AS c, SUM(v) AS sv FROM t GROUP BY s ORDER BY s DESC LIMIT 4 OFFSET 3", .s_desc = true, .n = 4, .m = 3 },
        .{ .sql = "SELECT s, COUNT(*) AS c, SUM(v) AS sv FROM t WHERE v >= 700 GROUP BY s ORDER BY s LIMIT 6", .min_v = 700, .n = 6 },
        .{ .sql = "SELECT s, g, COUNT(*) AS c, SUM(v) AS sv FROM t GROUP BY s, g ORDER BY s, g DESC LIMIT 7", .by_g = true, .g_desc = true, .n = 7 },
    };
    for (cases) |c| {
        errdefer std.debug.print("case: {s}\n", .{c.sql});
        const explain_sql = try std.fmt.allocPrint(allocator, "EXPLAIN {s}", .{c.sql});
        defer allocator.free(explain_sql);
        var plan = try resultLines(allocator, db, explain_sql);
        defer plan.deinit(allocator);
        try std.testing.expect(std.mem.indexOf(u8, plan.items, "key bound s") != null);

        var live: std.ArrayList(KeyRow) = .empty;
        defer live.deinit(allocator);
        for (rows[dead_below..]) |r| if (r.v >= c.min_v) try live.append(allocator, r);
        var want = try expectedLines(allocator, live.items, c.by_g, c.s_desc, c.g_desc, c.n, c.m);
        defer want.deinit(allocator);
        var got = try resultLines(allocator, db, c.sql);
        defer got.deinit(allocator);
        try std.testing.expectEqualStrings(want.items, got.items);
    }
}
