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

/// One row: string key `s`, string `u` for MIN/MAX, integer key `g`, value `v`.
const RankRow = struct { s: []const u8, u: []const u8, g: i64, v: i64 };
const RankGroup = struct { s: []const u8, g: i64, c: i64, sv: i64, mu: []const u8, xu: []const u8 };

/// `rows` grouped by `s` (or by `g`), with each group's COUNT(*), SUM(v),
/// MIN(u) and MAX(u).
fn rankGroups(allocator: std.mem.Allocator, rows: []RankRow, by_g: bool) ![]RankGroup {
    const Ctx = struct {
        by_g: bool,
        fn lt(ctx: @This(), a: RankRow, b: RankRow) bool {
            return if (ctx.by_g) a.g < b.g else std.mem.order(u8, a.s, b.s) == .lt;
        }
        fn same(ctx: @This(), a: RankRow, b: RankRow) bool {
            return if (ctx.by_g) a.g == b.g else std.mem.eql(u8, a.s, b.s);
        }
    };
    const ctx = Ctx{ .by_g = by_g };
    std.mem.sort(RankRow, rows, ctx, Ctx.lt);
    var groups: std.ArrayList(RankGroup) = .empty;
    errdefer groups.deinit(allocator);
    var i: usize = 0;
    while (i < rows.len) {
        var grp = RankGroup{ .s = rows[i].s, .g = rows[i].g, .c = 0, .sv = 0, .mu = rows[i].u, .xu = rows[i].u };
        var j = i;
        while (j < rows.len and ctx.same(rows[i], rows[j])) : (j += 1) {
            grp.c += 1;
            grp.sv += rows[j].v;
            if (std.mem.order(u8, rows[j].u, grp.mu) == .lt) grp.mu = rows[j].u;
            if (std.mem.order(u8, rows[j].u, grp.xu) == .gt) grp.xu = rows[j].u;
        }
        try groups.append(allocator, grp);
        i = j;
    }
    return groups.toOwnedSlice(allocator);
}

const RankCase = struct {
    sql: []const u8,
    by_g: bool = false,
    // Whether the query is post-sorted on a string key with an aggregate
    // prefix ranked in the core (EXPLAIN shows it).
    prefix: bool = true,
    min_c: i64 = 0,
    less: *const fn (RankGroup, RankGroup) bool,
    line: *const fn (*std.ArrayList(u8), std.mem.Allocator, RankGroup) anyerror!void,
    n: usize,
    m: usize = 0,
};

fn checkRankCase(allocator: std.mem.Allocator, db: *thindb.Database, rows: []RankRow, c: RankCase) !void {
    errdefer std.debug.print("case: {s}\n", .{c.sql});
    const explain_sql = try std.fmt.allocPrint(allocator, "EXPLAIN {s}", .{c.sql});
    defer allocator.free(explain_sql);
    var plan = try resultLines(allocator, db, explain_sql);
    defer plan.deinit(allocator);
    try std.testing.expectEqual(c.prefix, std.mem.indexOf(u8, plan.items, "rank prefix") != null);

    const groups = try rankGroups(allocator, rows, c.by_g);
    defer allocator.free(groups);
    var kept: std.ArrayList(RankGroup) = .empty;
    defer kept.deinit(allocator);
    for (groups) |g| if (g.c > c.min_c) try kept.append(allocator, g);
    std.mem.sort(RankGroup, kept.items, c.less, struct {
        fn lt(less: *const fn (RankGroup, RankGroup) bool, a: RankGroup, b: RankGroup) bool {
            return less(a, b);
        }
    }.lt);
    var want: std.ArrayList(u8) = .empty;
    defer want.deinit(allocator);
    const start = @min(c.m, kept.items.len);
    for (kept.items[start..@min(start + c.n, kept.items.len)]) |g| try c.line(&want, allocator, g);

    var got = try resultLines(allocator, db, c.sql);
    defer got.deinit(allocator);
    try std.testing.expectEqualStrings(want.items, got.items);
}

const RankOrder = struct {
    fn sc(out: *std.ArrayList(u8), allocator: std.mem.Allocator, g: RankGroup) anyerror!void {
        try out.print(allocator, "{s}|{d}\n", .{ g.s, g.c });
    }
    fn ssvc(out: *std.ArrayList(u8), allocator: std.mem.Allocator, g: RankGroup) anyerror!void {
        try out.print(allocator, "{s}|{d}|{d}\n", .{ g.s, g.sv, g.c });
    }
    fn smuc(out: *std.ArrayList(u8), allocator: std.mem.Allocator, g: RankGroup) anyerror!void {
        try out.print(allocator, "{s}|{s}|{d}\n", .{ g.s, g.mu, g.c });
    }
    fn ssv(out: *std.ArrayList(u8), allocator: std.mem.Allocator, g: RankGroup) anyerror!void {
        try out.print(allocator, "{s}|{d}\n", .{ g.s, g.sv });
    }
    fn sxu(out: *std.ArrayList(u8), allocator: std.mem.Allocator, g: RankGroup) anyerror!void {
        try out.print(allocator, "{s}|{s}\n", .{ g.s, g.xu });
    }
    fn gmu(out: *std.ArrayList(u8), allocator: std.mem.Allocator, g: RankGroup) anyerror!void {
        try out.print(allocator, "{d}|{s}\n", .{ g.g, g.mu });
    }
    fn gxuc(out: *std.ArrayList(u8), allocator: std.mem.Allocator, g: RankGroup) anyerror!void {
        try out.print(allocator, "{d}|{s}|{d}\n", .{ g.g, g.xu, g.c });
    }

    fn sOrder(a: RankGroup, b: RankGroup) std.math.Order {
        return std.mem.order(u8, a.s, b.s);
    }
    fn cDescS(a: RankGroup, b: RankGroup) bool {
        if (a.c != b.c) return a.c > b.c;
        return sOrder(a, b) == .lt;
    }
    fn cSDesc(a: RankGroup, b: RankGroup) bool {
        if (a.c != b.c) return a.c < b.c;
        return sOrder(a, b) == .gt;
    }
    fn cS(a: RankGroup, b: RankGroup) bool {
        if (a.c != b.c) return a.c < b.c;
        return sOrder(a, b) == .lt;
    }
    fn svDescCS(a: RankGroup, b: RankGroup) bool {
        if (a.sv != b.sv) return a.sv > b.sv;
        if (a.c != b.c) return a.c < b.c;
        return sOrder(a, b) == .lt;
    }
    fn svDescS(a: RankGroup, b: RankGroup) bool {
        if (a.sv != b.sv) return a.sv > b.sv;
        return sOrder(a, b) == .lt;
    }
    fn muS(a: RankGroup, b: RankGroup) bool {
        const o = std.mem.order(u8, a.mu, b.mu);
        if (o != .eq) return o == .lt;
        return sOrder(a, b) == .lt;
    }
    fn xuDescS(a: RankGroup, b: RankGroup) bool {
        const o = std.mem.order(u8, a.xu, b.xu);
        if (o != .eq) return o == .gt;
        return sOrder(a, b) == .lt;
    }
    fn muG(a: RankGroup, b: RankGroup) bool {
        const o = std.mem.order(u8, a.mu, b.mu);
        if (o != .eq) return o == .lt;
        return a.g < b.g;
    }
    fn xuDescCDescG(a: RankGroup, b: RankGroup) bool {
        const o = std.mem.order(u8, a.xu, b.xu);
        if (o != .eq) return o == .gt;
        if (a.c != b.c) return a.c > b.c;
        return a.g < b.g;
    }
};

fn insertRankRows(allocator: std.mem.Allocator, db: *thindb.Database, rows: []const RankRow, first_id: usize, chunk: usize) !void {
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);
    var at: usize = 0;
    while (at < rows.len) {
        const end = @min(at + chunk, rows.len);
        sql.clearRetainingCapacity();
        try sql.appendSlice(allocator, "INSERT INTO t VALUES ");
        for (rows[at..end], at..) |r, i| {
            if (i != at) try sql.append(allocator, ',');
            try sql.print(allocator, "({d},'{s}','{s}',{d},{d})", .{ first_id + i, r.s, r.u, r.g, r.v });
        }
        try exec(allocator, db, sql.items);
        at = end;
    }
}

// `ORDER BY <aggregates>, <string key> LIMIT n`: the core ranks the groups on
// the aggregate prefix, keeps every group tied at the cut, and the sort above
// finishes on the key. Ties at the cut are the hard part: a count ascending
// ties hundreds of groups. Numeric keys ordered by a string MIN/MAX rank in
// the core on the string bytes.
test "GROUP BY ORDER BY aggregate, string key LIMIT: core ranks the aggregate prefix with ties" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .max_dop = 4, .row_group_size = 128 });
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, s VARCHAR(16) NOT NULL, u VARCHAR(16) NOT NULL, g BIGINT NOT NULL, v BIGINT NOT NULL)");

    // 1000 keys with 3 rows each, then 37 of them gain about 27 more, so
    // counts tie heavily both at the top and at the bottom.
    const n_rows = 4000;
    const dead_below = 40;
    var s_buf: [n_rows][16]u8 = undefined;
    var u_buf: [n_rows][16]u8 = undefined;
    var rows: [n_rows]RankRow = undefined;
    for (&rows, 0..) |*r, i| {
        const x = if (i < 3000) (i * 7) % 1000 else i % 37;
        r.* = .{
            .s = try std.fmt.bufPrint(&s_buf[i], "k{d}", .{x}),
            .u = try std.fmt.bufPrint(&u_buf[i], "u{d}", .{(i * 13) % 211}),
            .g = @intCast(x * 100003),
            .v = @intCast(i % 17),
        };
    }
    const t = try db.openTable("t", .{});
    try insertRankRows(allocator, db, rows[0..2000], 0, 500);
    try t.flush();
    try exec(allocator, db, "DELETE FROM t WHERE id < 40");
    try insertRankRows(allocator, db, rows[2000..], 2000, 500);

    const O = RankOrder;
    const cases = [_]RankCase{
        .{ .sql = "SELECT s, COUNT(*) AS c FROM t GROUP BY s ORDER BY c DESC, s LIMIT 10", .less = O.cDescS, .line = O.sc, .n = 10 },
        .{ .sql = "SELECT s, COUNT(*) AS c FROM t GROUP BY s ORDER BY c, s DESC LIMIT 5 OFFSET 2", .less = O.cSDesc, .line = O.sc, .n = 5, .m = 2 },
        .{ .sql = "SELECT s, SUM(v) AS sv, COUNT(*) AS c FROM t GROUP BY s ORDER BY sv DESC, c, s LIMIT 8", .less = O.svDescCS, .line = O.ssvc, .n = 8 },
        .{ .sql = "SELECT s, MIN(u) AS mu, COUNT(*) AS c FROM t GROUP BY s ORDER BY mu, s LIMIT 6", .less = O.muS, .line = O.smuc, .n = 6 },
        .{ .sql = "SELECT s, COUNT(*) AS c FROM t GROUP BY s HAVING COUNT(*) > 3 ORDER BY c, s LIMIT 4", .min_c = 3, .less = O.cS, .line = O.sc, .n = 4 },
        .{ .sql = "SELECT g, MIN(u) AS mu FROM t GROUP BY g ORDER BY mu, g LIMIT 5", .by_g = true, .prefix = false, .less = O.muG, .line = O.gmu, .n = 5 },
        .{ .sql = "SELECT g, MAX(u) AS xu, COUNT(*) AS c FROM t GROUP BY g ORDER BY xu DESC, c DESC, g LIMIT 5 OFFSET 1", .by_g = true, .prefix = false, .less = O.xuDescCDescG, .line = O.gxuc, .n = 5, .m = 1 },
    };
    for (cases) |c| try checkRankCase(allocator, db, rows[dead_below..], c);
}

// Past 131072 groups the core's ranked emit splits the groups across fill
// workers, so the ties at the cut fall to different workers' sets.
test "GROUP BY ORDER BY aggregate, string key LIMIT: ties at the cut across fill workers" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, s VARCHAR(16) NOT NULL, u VARCHAR(16) NOT NULL, g BIGINT NOT NULL, v BIGINT NOT NULL)");

    const n_rows = 140_000;
    const s_buf = try allocator.alloc([16]u8, n_rows);
    defer allocator.free(s_buf);
    const u_buf = try allocator.alloc([16]u8, n_rows);
    defer allocator.free(u_buf);
    const rows = try allocator.alloc(RankRow, n_rows);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| {
        r.* = .{
            .s = try std.fmt.bufPrint(&s_buf[i], "k{d}", .{i}),
            .u = try std.fmt.bufPrint(&u_buf[i], "u{d}", .{i % 503}),
            .g = @intCast(i),
            .v = @intCast(i % 1000),
        };
    }
    try insertRankRows(allocator, db, rows, 0, 10_000);
    const t = try db.openTable("t", .{});
    try t.flush();

    const O = RankOrder;
    const cases = [_]RankCase{
        .{ .sql = "SELECT s, SUM(v) AS sv FROM t GROUP BY s ORDER BY sv DESC, s LIMIT 10", .less = O.svDescS, .line = O.ssv, .n = 10 },
        .{ .sql = "SELECT s, MAX(u) AS xu FROM t GROUP BY s ORDER BY xu DESC, s LIMIT 7 OFFSET 3", .less = O.xuDescS, .line = O.sxu, .n = 7, .m = 3 },
        .{ .sql = "SELECT s, COUNT(*) AS c FROM t GROUP BY s ORDER BY c, s LIMIT 3", .less = O.cS, .line = O.sc, .n = 3 },
    };
    for (cases) |c| try checkRankCase(allocator, db, rows, c);
}
