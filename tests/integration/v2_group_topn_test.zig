//! Focused battery for the V2 generic group-topN engine (Q15 shape).

const std = @import("std");
const thindb = @import("thindb");

test "memory: SQL budgets cover wide sort and V2 group output allocations" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(a, io, tmp.dir, .{
        .memory_budget = 16 * 1024,
        .auto_flush_secs = 0,
        .row_group_size = 64,
        .max_dop = 2,
    });
    defer db.close();
    const schema: thindb.TableSchema = .{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "payload", .type = .string } },
        .order_key = &.{"id"},
        .unique = false,
    };
    const table = try db.table("wide", schema, .{ .order_key = &.{"id"} });
    const payloads = try a.alloc(u8, 256 * 4096);
    defer a.free(payloads);
    @memset(payloads, 'x');
    const Row = struct { id: i64, payload: []const u8 };
    const rows = try a.alloc(Row, 256);
    defer a.free(rows);
    for (rows, 0..) |*row, i| {
        const bytes = payloads[i * 4096 ..][0..4096];
        _ = try std.fmt.bufPrint(bytes[0..8], "{d:0>8}", .{256 - i});
        row.* = .{ .id = @intCast(i), .payload = bytes };
    }
    try table.insert(rows);
    try table.flush();
    for ([_][]const u8{
        "SELECT id, payload FROM wide ORDER BY payload",
        "SELECT payload, COUNT(*) AS n FROM wide GROUP BY payload ORDER BY n DESC LIMIT 10",
    }) |sql_text| {
        var rejected = false;
        if (@import("sql_helpers.zig").runSql(a, db, sql_text)) |value| {
            var query = value;
            defer query.deinit();
            while (true) {
                const batch = query.next() catch |err| {
                    try std.testing.expectEqual(error.MemoryBudgetExceeded, err);
                    rejected = true;
                    break;
                };
                if (batch == null) break;
            }
        } else |err| {
            try std.testing.expectEqual(error.MemoryBudgetExceeded, err);
            rejected = true;
        }
        try std.testing.expect(rejected);
        try std.testing.expectEqual(@as(usize, 0), db.config.memory_pool.?.inUse());
    }
}
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

test "group completion: filtered compound groups retain large OFFSET pages across parallel executions" {
    const allocator = std.testing.allocator;
    const group_count = 10_037;
    const Row = struct { id: i64, g0: i32, g1: i32, kept: i32 };
    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop });
        defer db.close();
        const t = try db.table("completion_rows", .{
            .columns = &.{
                .{ .name = "id", .type = .bigint },
                .{ .name = "g0", .type = .int },
                .{ .name = "g1", .type = .int },
                .{ .name = "kept", .type = .int },
            },
            .order_key = &.{"id"},
            .unique = true,
        }, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 1024 });
        const input = try allocator.alloc(Row, group_count * 3);
        defer allocator.free(input);
        for (input, 0..) |*row, i| {
            const key = i / 3;
            row.* = .{ .id = @intCast(i), .g0 = @intCast(key / 101), .g1 = @intCast(key % 101), .kept = @intFromBool(i % 3 != 2) };
        }
        try t.insert(input);
        try t.flush();

        for (0..16) |_| {
            var q = try runSql(allocator, db, "SELECT g0, g1, COUNT(*) AS c FROM completion_rows WHERE kept = 1 GROUP BY g0, g1 ORDER BY c DESC LIMIT 10 OFFSET 10000");
            defer q.deinit();
            var seen = [_]bool{false} ** group_count;
            var count: usize = 0;
            while (try q.next()) |batch| {
                for (0..batch.row_count) |i| {
                    const key: usize = @intCast(batch.values[0].data.int[i] * 101 + batch.values[1].data.int[i]);
                    try std.testing.expect(key < group_count);
                    try std.testing.expect(!seen[key]);
                    seen[key] = true;
                    try std.testing.expectEqual(@as(i64, 2), batch.values[2].data.bigint[i]);
                    count += 1;
                }
            }
            try std.testing.expectEqual(@as(usize, 10), count);
        }
        inline for (.{
            "SELECT g0, g1, COUNT(*) AS c FROM completion_rows WHERE kept = 7 GROUP BY g0, g1 ORDER BY c DESC LIMIT 10 OFFSET 10000",
            "SELECT g0, g1, COUNT(*) AS c FROM completion_rows WHERE kept = 1 GROUP BY g0, g1 ORDER BY c DESC LIMIT 10 OFFSET 20000",
        }) |sql| {
            var empty = try runSql(allocator, db, sql);
            defer empty.deinit();
            var count: usize = 0;
            while (try empty.next()) |batch| count += batch.row_count;
            try std.testing.expectEqual(@as(usize, 0), count);
        }
    }
}

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE hits (id BIGINT PRIMARY KEY, UserID BIGINT NOT NULL, SearchEngineID SMALLINT NOT NULL)");

    // UserID u appears (u) times for u in 1..12, so top-10 by count desc is
    // UserIDs 12,11,...,3 with counts 12,11,...,3.
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO hits (id, UserID, SearchEngineID) VALUES ");
    var first = true;
    var id: i64 = 0;
    var u: i64 = 1;
    while (u <= 12) : (u += 1) {
        var n: i64 = 0;
        while (n < u) : (n += 1) {
            if (!first) try buf.appendSlice(allocator, ",");
            first = false;
            id += 1;
            var tmpbuf: [64]u8 = undefined;
            const s = try std.fmt.bufPrint(&tmpbuf, "({d},{d},{d})", .{ id, u, @as(i64, 7) });
            try buf.appendSlice(allocator, s);
        }
    }
    try exec(allocator, db, buf.items);
    const t = try db.openTable("hits", .{});
    try t.flush();
    return db;
}

test "V2 Q15: SearchEngineID/UserID count desc top-N" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10");
    defer q.deinit();

    const schema = q.outputSchema();
    std.debug.print("V2Q15 output schema cols={d}\n", .{schema.len});
    for (schema) |c| std.debug.print("  col name={s} type={any}\n", .{ c.name, c.type });

    var rows: usize = 0;
    while (try q.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            const uid = batch.values[0].data.bigint[r];
            const cnt = batch.values[1].data.bigint[r];
            std.debug.print("  row UserID={d} count={d}\n", .{ uid, cnt });
            rows += 1;
        }
    }
    std.debug.print("V2Q15 total rows={d}\n", .{rows});
    try std.testing.expectEqual(@as(usize, 10), rows);
}

fn setupFloat(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE hits (id BIGINT PRIMARY KEY, k SMALLINT NOT NULL, sd DOUBLE NOT NULL, sf FLOAT NOT NULL)");
    // Group k=1: sd/sf in {1.5, 2.5, 3.0}  -> SUM=7.0  MIN=1.5  MAX=3.0  COUNT=3
    // Group k=2: sd/sf in {10.25, -4.25}   -> SUM=6.0  MIN=-4.25 MAX=10.25 COUNT=2
    // All values are exactly representable in f32, so f32 widened to f64 sums exactly.
    try exec(allocator, db,
        \\INSERT INTO hits (id, k, sd, sf) VALUES
        \\ (1, 1, 1.5, 1.5), (2, 1, 2.5, 2.5), (3, 1, 3.0, 3.0),
        \\ (4, 2, 10.25, 10.25), (5, 2, -4.25, -4.25)
    );
    const t = try db.openTable("hits", .{});
    try t.flush();
    return db;
}

test "V2 float aggregates: SUM/AVG over double (CountSumAvg float gate)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupFloat(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT k, COUNT(*), SUM(sd), AVG(sd) FROM hits GROUP BY k ORDER BY k");
    defer q.deinit();

    var seen: usize = 0;
    while (try q.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            const k = batch.values[0].data.smallint[r];
            const c = batch.values[1].data.bigint[r];
            const sum = batch.values[2].data.double[r];
            const avg = batch.values[3].data.double[r];
            if (k == 1) {
                try std.testing.expectEqual(@as(i64, 3), c);
                try std.testing.expectEqual(@as(f64, 7.0), sum);
                try std.testing.expectApproxEqAbs(@as(f64, 7.0 / 3.0), avg, 1e-12);
            } else {
                try std.testing.expectEqual(@as(i64, 2), c);
                try std.testing.expectEqual(@as(f64, 6.0), sum);
                try std.testing.expectApproxEqAbs(@as(f64, 3.0), avg, 1e-12);
            }
            seen += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), seen);
}

test "V2 float aggregates: SUM/MIN/MAX over double (generic program path)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupFloat(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT k, SUM(sd), MIN(sd), MAX(sd) FROM hits GROUP BY k ORDER BY k");
    defer q.deinit();

    var seen: usize = 0;
    while (try q.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            const k = batch.values[0].data.smallint[r];
            const sum = batch.values[1].data.double[r];
            const mn = batch.values[2].data.double[r];
            const mx = batch.values[3].data.double[r];
            if (k == 1) {
                try std.testing.expectEqual(@as(f64, 7.0), sum);
                try std.testing.expectEqual(@as(f64, 1.5), mn);
                try std.testing.expectEqual(@as(f64, 3.0), mx);
            } else {
                try std.testing.expectEqual(@as(f64, 6.0), sum);
                try std.testing.expectEqual(@as(f64, -4.25), mn);
                try std.testing.expectEqual(@as(f64, 10.25), mx);
            }
            seen += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), seen);
}

test "V2 float aggregates: SUM/MIN/MAX over f32 (float output type)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupFloat(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT k, SUM(sf), MIN(sf), MAX(sf) FROM hits GROUP BY k ORDER BY k");
    defer q.deinit();

    var seen: usize = 0;
    while (try q.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            const k = batch.values[0].data.smallint[r];
            const sum = batch.values[1].data.double[r]; // SUM(float) -> double
            const mn = batch.values[2].data.float[r]; // MIN(float) -> float
            const mx = batch.values[3].data.float[r]; // MAX(float) -> float
            if (k == 1) {
                try std.testing.expectEqual(@as(f64, 7.0), sum);
                try std.testing.expectEqual(@as(f32, 1.5), mn);
                try std.testing.expectEqual(@as(f32, 3.0), mx);
            } else {
                try std.testing.expectEqual(@as(f64, 6.0), sum);
                try std.testing.expectEqual(@as(f32, -4.25), mn);
                try std.testing.expectEqual(@as(f32, 10.25), mx);
            }
            seen += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), seen);
}

test "V2 string-key GROUP BY over memtable-only rows (zero segments)" {
    // Regression: with NO flushed segments, the silo grid's tile claim space
    // was empty (lo=0 >= total=0), so no worker ever opened the tile that
    // carries the memtable — a memtable-only string-key GROUP BY silently
    // returned zero groups while plain scans and int-key groups (which route
    // through other handlers) worked.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE ev (id BIGINT PRIMARY KEY, qty INT NOT NULL, tag TEXT NOT NULL)");
    try exec(allocator, db, "INSERT INTO ev VALUES (1,10,'a'),(2,20,'a'),(3,30,'b'),(4,40,'b'),(5,50,'c'),(6,60,'c')");
    // Deliberately NO flush — every row lives in the memtable.

    var q = try runSql(allocator, db, "SELECT tag, COUNT(*) AS n, SUM(qty) AS total FROM ev GROUP BY tag ORDER BY total DESC");
    defer q.deinit();

    var tags: std.ArrayList(u8) = .empty;
    defer tags.deinit(allocator);
    var totals: std.ArrayList(i64) = .empty;
    defer totals.deinit(allocator);
    while (try q.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            const sv = batch.values[0].data.string;
            try tags.appendSlice(allocator, sv.rowBytes(r));
            try std.testing.expectEqual(@as(i64, 2), batch.values[1].data.bigint[r]);
            try totals.append(allocator, batch.values[2].data.bigint[r]);
        }
    }
    try std.testing.expectEqualStrings("cba", tags.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 110, 70, 30 }, totals.items);
}

test "V2 staged CTEs: chained boundaries materialize, group, filter, sort" {
    // Three-stage chain over the staged compiler: table-sourced stage (V2
    // scan handler) → grouped stage over the materialized result → filtered
    // stage → root ORDER BY. Exercises stage scheduling, the MatScan leaf,
    // and the drain-triggered background free (leak-checked allocator).
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE ev2 (id BIGINT PRIMARY KEY, qty INT NOT NULL, tag TEXT NOT NULL)");
    try exec(allocator, db, "INSERT INTO ev2 VALUES (1,10,'a'),(2,20,'a'),(3,30,'b'),(4,40,'b'),(5,50,'c'),(6,60,'c')");

    var q = try runSql(allocator, db, "WITH filtered AS (SELECT id, qty, tag FROM ev2 WHERE qty >= 20), " ++
        "by_tag AS (SELECT tag, SUM(qty) AS total FROM filtered GROUP BY tag), " ++
        "big AS (SELECT tag, total FROM by_tag WHERE tag <> 'a') " ++
        "SELECT tag, total FROM big ORDER BY total DESC");
    defer q.deinit();

    var tags: std.ArrayList(u8) = .empty;
    defer tags.deinit(allocator);
    var totals: std.ArrayList(i64) = .empty;
    defer totals.deinit(allocator);
    while (try q.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            try tags.appendSlice(allocator, batch.values[0].data.string.rowBytes(r));
            try totals.append(allocator, batch.values[1].data.bigint[r]);
        }
    }
    try std.testing.expectEqualStrings("cb", tags.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 110, 70 }, totals.items);
}

test "V2 staged CTEs: stats and sort order cross the materialize boundary" {
    // A single-reference CTE compiles INLINE (no stage/MatScan — the body
    // streams straight into the outer block), so the outer GROUP BY routes
    // on the body's native stats: a body sorted on the group key streams
    // (StreamAggregate) instead of hashing, and the root's row bound
    // reflects the body's, not maxInt.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE ev4 (id BIGINT PRIMARY KEY, qty INT NOT NULL, tag TEXT NOT NULL)");
    try exec(allocator, db, "INSERT INTO ev4 VALUES (1,10,'a'),(2,20,'a'),(3,30,'b'),(4,40,'b'),(5,50,'c'),(6,60,'c')");

    var q = try runSql(allocator, db, "WITH ordered AS (SELECT tag, qty FROM ev4 ORDER BY tag) " ++
        "SELECT tag, SUM(qty) AS total FROM ordered GROUP BY tag");
    defer q.deinit();

    const st = q.cq.query.stats();
    try std.testing.expect(st.upper_rows <= 6);

    var plan: std.ArrayList(u8) = .empty;
    defer plan.deinit(allocator);
    try q.cq.query.explain(&plan, allocator, 0);
    try std.testing.expect(std.mem.indexOf(u8, plan.items, "StreamAggregate") != null);
    try std.testing.expect(std.mem.indexOf(u8, plan.items, "MatScan") == null);

    var tags: std.ArrayList(u8) = .empty;
    defer tags.deinit(allocator);
    var totals: std.ArrayList(i64) = .empty;
    defer totals.deinit(allocator);
    while (try q.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            try tags.appendSlice(allocator, batch.values[0].data.string.rowBytes(r));
            try totals.append(allocator, batch.values[1].data.bigint[r]);
        }
    }
    try std.testing.expectEqualStrings("abc", tags.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 30, 70, 110 }, totals.items);
}

test "V2 staged subquery: FROM-clause subquery with alias qualification" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE ev3 (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO ev3 VALUES (1,10),(2,20),(3,30),(4,40)");

    var q = try runSql(allocator, db, "SELECT sub.id, qty FROM (SELECT id, qty FROM ev3 WHERE qty >= 20) AS sub ORDER BY sub.id DESC LIMIT 2");
    defer q.deinit();

    // Output names are UNQUALIFIED regardless of the alias (standard SQL).
    try std.testing.expectEqualStrings("id", q.outputSchema()[0].name);
    try std.testing.expectEqualStrings("qty", q.outputSchema()[1].name);

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) try ids.append(allocator, batch.values[0].data.bigint[r]);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 4, 3 }, ids.items);
}

fn openJoinDb(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE parts (id BIGINT PRIMARY KEY, name TEXT NOT NULL, cat_id BIGINT NOT NULL)");
    try exec(allocator, db, "INSERT INTO parts VALUES (1,'bolt',1),(2,'nut',1),(3,'washer',2)");
    try exec(allocator, db, "CREATE TABLE orders (id BIGINT PRIMARY KEY, part_id BIGINT NOT NULL, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO orders VALUES (1,1,10),(2,1,20),(3,2,5),(4,9,7)");
    return db;
}

const JoinRow = struct {
    qty: ?i32,
    name: ?[]const u8,
};

fn collectQtyName(allocator: std.mem.Allocator, name_arena: std.mem.Allocator, db: *thindb.Database, sql: []const u8) !std.ArrayList(JoinRow) {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var rows: std.ArrayList(JoinRow) = .empty;
    errdefer rows.deinit(allocator);
    while (try q.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            try rows.append(allocator, .{
                .qty = if (batch.values[0].isValid(r)) batch.values[0].data.int[r] else null,
                .name = if (batch.values[1].isValid(r))
                    try name_arena.dupe(u8, batch.values[1].data.string.rowBytes(r))
                else
                    null,
            });
        }
    }
    return rows;
}

test "V2 staged joins: INNER preserves matches only" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openJoinDb(allocator, io, tmp.dir);
    defer db.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var rows = try collectQtyName(allocator, arena.allocator(), db, "SELECT o.qty, p.name FROM orders o JOIN parts p ON o.part_id = p.id ORDER BY o.qty");
    defer rows.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), rows.items.len);
    try std.testing.expectEqual(@as(?i32, 5), rows.items[0].qty);
    try std.testing.expectEqualStrings("nut", rows.items[0].name.?);
    try std.testing.expectEqual(@as(?i32, 10), rows.items[1].qty);
    try std.testing.expectEqualStrings("bolt", rows.items[1].name.?);
    try std.testing.expectEqual(@as(?i32, 20), rows.items[2].qty);
    try std.testing.expectEqualStrings("bolt", rows.items[2].name.?);
}

test "V2 staged joins: LEFT null-extends unmatched probe rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openJoinDb(allocator, io, tmp.dir);
    defer db.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var rows = try collectQtyName(allocator, arena.allocator(), db, "SELECT o.qty, p.name FROM orders o LEFT JOIN parts p ON o.part_id = p.id ORDER BY o.qty");
    defer rows.deinit(allocator);

    // Order 4 (part_id 9) survives with a NULL part name.
    try std.testing.expectEqual(@as(usize, 4), rows.items.len);
    try std.testing.expectEqual(@as(?i32, 5), rows.items[0].qty);
    try std.testing.expectEqualStrings("nut", rows.items[0].name.?);
    try std.testing.expectEqual(@as(?i32, 7), rows.items[1].qty);
    try std.testing.expect(rows.items[1].name == null);
    try std.testing.expectEqualStrings("bolt", rows.items[2].name.?);
    try std.testing.expectEqualStrings("bolt", rows.items[3].name.?);
}

test "V2 staged joins: RIGHT null-extends unmatched build rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openJoinDb(allocator, io, tmp.dir);
    defer db.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var rows = try collectQtyName(allocator, arena.allocator(), db, "SELECT o.qty, p.name FROM orders o RIGHT JOIN parts p ON o.part_id = p.id ORDER BY p.name, o.qty");
    defer rows.deinit(allocator);

    // Part 3 (washer) has no orders: NULL qty.
    try std.testing.expectEqual(@as(usize, 4), rows.items.len);
    try std.testing.expectEqual(@as(?i32, 10), rows.items[0].qty);
    try std.testing.expectEqualStrings("bolt", rows.items[0].name.?);
    try std.testing.expectEqual(@as(?i32, 20), rows.items[1].qty);
    try std.testing.expectEqualStrings("bolt", rows.items[1].name.?);
    try std.testing.expectEqual(@as(?i32, 5), rows.items[2].qty);
    try std.testing.expectEqualStrings("nut", rows.items[2].name.?);
    try std.testing.expectEqual(@as(?i32, null), rows.items[3].qty);
    try std.testing.expectEqualStrings("washer", rows.items[3].name.?);
}

test "V2 staged joins: FULL preserves both sides" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openJoinDb(allocator, io, tmp.dir);
    defer db.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var rows = try collectQtyName(allocator, arena.allocator(), db, "SELECT o.qty, p.name FROM orders o FULL JOIN parts p ON o.part_id = p.id");
    defer rows.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 5), rows.items.len);
    var null_qty: usize = 0;
    var null_name: usize = 0;
    var qty_sum: i64 = 0;
    var saw_washer = false;
    for (rows.items) |row| {
        if (row.qty) |v| qty_sum += v else null_qty += 1;
        if (row.name) |n| {
            if (std.mem.eql(u8, n, "washer")) saw_washer = true;
        } else null_name += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), null_qty);
    try std.testing.expectEqual(@as(usize, 1), null_name);
    try std.testing.expectEqual(@as(i64, 42), qty_sum);
    try std.testing.expect(saw_washer);
}

test "V2 staged joins: three-table chain + WHERE + GROUP BY + ORDER BY" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openJoinDb(allocator, io, tmp.dir);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE cats (id BIGINT PRIMARY KEY, label TEXT NOT NULL)");
    try exec(allocator, db, "INSERT INTO cats VALUES (1,'metal'),(2,'rubber')");

    var q = try runSql(allocator, db, "SELECT c.label, SUM(o.qty) AS total " ++
        "FROM orders o JOIN parts p ON o.part_id = p.id JOIN cats c ON p.cat_id = c.id " ++
        "WHERE o.qty >= 5 GROUP BY c.label ORDER BY total DESC");
    defer q.deinit();

    var labels: std.ArrayList(u8) = .empty;
    defer labels.deinit(allocator);
    var totals: std.ArrayList(i64) = .empty;
    defer totals.deinit(allocator);
    while (try q.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            try labels.appendSlice(allocator, batch.values[0].data.string.rowBytes(r));
            try totals.append(allocator, batch.values[1].data.bigint[r]);
        }
    }
    // Orders 1/2/3 match parts 1/1/2, all cat 'metal' (35); order 4 matches nothing.
    try std.testing.expectEqualStrings("metal", labels.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{35}, totals.items);
}

test "V2 staged joins: self-join via aliases" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE emp (id BIGINT PRIMARY KEY, mgr_id BIGINT NOT NULL, name TEXT NOT NULL)");
    try exec(allocator, db, "INSERT INTO emp VALUES (1,1,'ada'),(2,1,'bob'),(3,2,'cyd')");

    var q = try runSql(allocator, db, "SELECT e.name, m.name FROM emp e JOIN emp m ON e.mgr_id = m.id ORDER BY e.id");
    defer q.deinit();

    var pairs: std.ArrayList(u8) = .empty;
    defer pairs.deinit(allocator);
    while (try q.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            try pairs.appendSlice(allocator, batch.values[0].data.string.rowBytes(r));
            try pairs.appendSlice(allocator, "->");
            try pairs.appendSlice(allocator, batch.values[1].data.string.rowBytes(r));
            try pairs.appendSlice(allocator, " ");
        }
    }
    try std.testing.expectEqualStrings("ada->ada bob->ada cyd->bob ", pairs.items);
}

test "V2 staged joins: CTE joined to itself shares one stage" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try openJoinDb(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "WITH totals AS (SELECT part_id, SUM(qty) AS total FROM orders GROUP BY part_id) " ++
        "SELECT a.part_id, b.total FROM totals a JOIN totals b ON a.part_id = b.part_id ORDER BY a.part_id");
    defer q.deinit();

    var part_ids: std.ArrayList(i64) = .empty;
    defer part_ids.deinit(allocator);
    var totals: std.ArrayList(i64) = .empty;
    defer totals.deinit(allocator);
    while (try q.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            try part_ids.append(allocator, batch.values[0].data.bigint[r]);
            try totals.append(allocator, batch.values[1].data.bigint[r]);
        }
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 9 }, part_ids.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 30, 5, 7 }, totals.items);
}

test "guided (NOT) LIKE over lz4_fsst strings with NULLs and a leading conjunct" {
    // Exercises the block-sourced LIKE path: a cheap comparison conjunct
    // masks first, then the (NOT) LIKE evaluates only surviving rows —
    // per-survivor decode when the block lands FSST-encoded, the raw guided
    // arm otherwise. NULL strings must fail both LIKE and NOT LIKE.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE pages (id BIGINT PRIMARY KEY, k INT NOT NULL, s TEXT) PROPERTIES (\"compression\" = \"lz4_fsst\")");

    const n: usize = 3000;
    var like_expected: i64 = 0;
    var notlike_expected: i64 = 0;
    var single_like_expected: i64 = 0;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO pages VALUES ");
    for (0..n) |i| {
        if (i != 0) try buf.appendSlice(allocator, ",");
        const k = i % 10;
        var row: [192]u8 = undefined;
        if (i % 7 == 0) {
            const r = try std.fmt.bufPrint(&row, "({d},{d},NULL)", .{ i, k });
            try buf.appendSlice(allocator, r);
        } else {
            const tag: []const u8 = if (i % 3 == 0) "needle" else "plain";
            const r = try std.fmt.bufPrint(&row, "({d},{d},'http://example.com/site/{s}/page-{x}?session=abcdef{d}')", .{ i, k, tag, i *% 2654435761, i });
            try buf.appendSlice(allocator, r);
            if (i % 3 == 0) single_like_expected += 1;
            if (k < 5) {
                if (i % 3 == 0) like_expected += 1 else notlike_expected += 1;
            }
        }
    }
    try exec(allocator, db, buf.items);
    const t = try db.openTable("pages", .{});
    try t.flush();

    const got_like = try helpers.collectBigints(allocator, db, "SELECT COUNT(*) FROM pages WHERE k < 5 AND s LIKE '%needle%'");
    defer allocator.free(got_like);
    try std.testing.expectEqualSlices(i64, &[_]i64{like_expected}, got_like);

    const got_notlike = try helpers.collectBigints(allocator, db, "SELECT COUNT(*) FROM pages WHERE k < 5 AND s NOT LIKE '%needle%'");
    defer allocator.free(got_notlike);
    try std.testing.expectEqualSlices(i64, &[_]i64{notlike_expected}, got_notlike);

    const got_single = try helpers.collectBigints(allocator, db, "SELECT COUNT(*) FROM pages WHERE s LIKE '%needle%'");
    defer allocator.free(got_single);
    try std.testing.expectEqualSlices(i64, &[_]i64{single_like_expected}, got_single);
}

test "string GROUP BY and COUNT(DISTINCT) over lz4_fsst blocks" {
    // Exercises the FSST key memo: group digests / dict codes are computed
    // once per distinct compressed value per block and translated per row.
    // Group counts are heavy with repeats (key j appears j+1 times) plus
    // blank-string and NULL groups; unfiltered and filtered lanes both run.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE visits (id BIGINT PRIMARY KEY, k INT NOT NULL, s TEXT) PROPERTIES (\"compression\" = \"lz4_fsst\")");

    const n_keys: usize = 100;
    var cnt_all = [_]i64{0} ** n_keys;
    var cnt_filt = [_]i64{0} ** n_keys;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO visits VALUES ");
    var id: usize = 0;
    for (0..n_keys) |j| {
        for (0..j + 1) |r| {
            if (id != 0) try buf.appendSlice(allocator, ",");
            const k = r % 10;
            var row: [224]u8 = undefined;
            const line = try std.fmt.bufPrint(&row, "({d},{d},'http://example.com/category/{x}/article-{d:0>3}-with-a-long-shared-suffix-for-fsst')", .{ id, k, j *% 2654435761, j });
            try buf.appendSlice(allocator, line);
            cnt_all[j] += 1;
            if (k < 5) cnt_filt[j] += 1;
            id += 1;
        }
    }
    // Blank and NULL groups ride along: blank is a real DISTINCT value and a
    // real group; NULL groups but is excluded from COUNT(DISTINCT s).
    for (0..13) |b| {
        var row: [64]u8 = undefined;
        try buf.appendSlice(allocator, try std.fmt.bufPrint(&row, ",({d},{d},'')", .{ id, b % 10 }));
        id += 1;
    }
    for (0..17) |b| {
        var row: [64]u8 = undefined;
        try buf.appendSlice(allocator, try std.fmt.bufPrint(&row, ",({d},{d},NULL)", .{ id, b % 10 }));
        id += 1;
    }
    try exec(allocator, db, buf.items);
    const t = try db.openTable("visits", .{});
    try t.flush();

    const got_ndv = try helpers.collectBigints(allocator, db, "SELECT COUNT(DISTINCT s) FROM visits");
    defer allocator.free(got_ndv);
    try std.testing.expectEqualSlices(i64, &[_]i64{@intCast(n_keys + 1)}, got_ndv);

    // Top-3 groups by count: keys 99, 98, 97 (counts 100, 99, 98 — unique,
    // no tie ambiguity; blank=13 and NULL=17 are far below).
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        var q = try runSql(allocator, db, "SELECT s, COUNT(*) AS c FROM visits GROUP BY s ORDER BY c DESC LIMIT 3");
        defer q.deinit();
        var keys: std.ArrayList([]const u8) = .empty;
        defer keys.deinit(allocator);
        var counts: std.ArrayList(i64) = .empty;
        defer counts.deinit(allocator);
        while (try q.next()) |batch| {
            var r: usize = 0;
            while (r < batch.row_count) : (r += 1) {
                try keys.append(allocator, try arena.allocator().dupe(u8, batch.values[0].data.string.rowBytes(r)));
                try counts.append(allocator, batch.values[1].data.bigint[r]);
            }
        }
        try std.testing.expectEqualSlices(i64, &[_]i64{ 100, 99, 98 }, counts.items);
        for (keys.items, [_]usize{ 99, 98, 97 }) |got, j| {
            var want: [224]u8 = undefined;
            const w = try std.fmt.bufPrint(&want, "http://example.com/category/{x}/article-{d:0>3}-with-a-long-shared-suffix-for-fsst", .{ j *% 2654435761, j });
            try std.testing.expectEqualStrings(w, got);
        }
    }

    // Filtered lane (hashSurvivorsFromBlock): every key has at least one
    // k<5 row, so the filtered GROUP BY must surface all 100 groups, and the
    // detail count pins the per-group memberships' total.
    {
        const groups = try helpers.collectBigints(allocator, db, "SELECT COUNT(*) FROM (SELECT s, COUNT(*) AS c FROM visits WHERE k < 5 AND s <> '' GROUP BY s) sub");
        defer allocator.free(groups);
        try std.testing.expectEqualSlices(i64, &[_]i64{@intCast(n_keys)}, groups);

        var expected_total: i64 = 0;
        for (cnt_filt) |c| expected_total += c;
        const total = try helpers.collectBigints(allocator, db, "SELECT COUNT(*) FROM visits WHERE k < 5 AND s <> ''");
        defer allocator.free(total);
        try std.testing.expectEqualSlices(i64, &[_]i64{expected_total}, total);
    }
}

test "V2 staged window: LAG/ROW_NUMBER with partition, multi-spec, tie determinism" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE events (id BIGINT PRIMARY KEY, grp INT NOT NULL, ord INT NOT NULL, val BIGINT NOT NULL)");
    // grp 1 carries an ORDER BY tie (ord=2 twice); the window tiebreak is
    // arrival order, so id=2 sorts before id=3 deterministically.
    try exec(allocator, db,
        \\INSERT INTO events VALUES
        \\(1,1,1,10),(2,1,2,20),(3,1,2,21),(4,1,3,30),
        \\(5,2,1,100),(6,2,2,110)
    );
    const t = try db.openTable("events", .{});
    try t.flush();

    var q = try runSql(allocator, db, "SELECT id, LAG(val, 1) OVER (PARTITION BY grp ORDER BY ord) AS prev, " ++
        "ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ord) AS rn, " ++
        "SUM(val) OVER (PARTITION BY grp) AS tot " ++
        "FROM events ORDER BY id");
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    var prevs: std.ArrayList(?i64) = .empty;
    defer prevs.deinit(allocator);
    var rns: std.ArrayList(i64) = .empty;
    defer rns.deinit(allocator);
    var tots: std.ArrayList(i64) = .empty;
    defer tots.deinit(allocator);
    while (try q.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            try ids.append(allocator, batch.values[0].data.bigint[r]);
            try prevs.append(allocator, if (batch.values[1].isValid(r)) batch.values[1].data.bigint[r] else null);
            try rns.append(allocator, batch.values[2].data.bigint[r]);
            try tots.append(allocator, batch.values[3].data.bigint[r]);
        }
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4, 5, 6 }, ids.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4, 1, 2 }, rns.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 81, 81, 81, 81, 210, 210 }, tots.items);
    const expected_prev = [_]?i64{ null, 10, 20, 21, null, 100 };
    for (prevs.items, expected_prev) |got, want| try std.testing.expectEqual(want, got);
}

test "V2 staged window: parallel partition buckets match analytic expectations" {
    // 100K rows / 1000 partitions with max_dop=4 crosses the operator's
    // parallel_min_rows gate, exercising the hash-scatter bucket path:
    // parallel key fill, bucket sort, per-bucket partition walks, and the
    // atomic validity writes (every partition's first LAG row is NULL,
    // scattered across shared bitmap bytes).
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();

    try exec(allocator, db, "CREATE TABLE big (id BIGINT PRIMARY KEY, p BIGINT NOT NULL, o BIGINT NOT NULL)");
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    var first = true;
    for (0..1000) |p| {
        buf.clearRetainingCapacity();
        try buf.appendSlice(allocator, "INSERT INTO big VALUES ");
        for (0..100) |o| {
            if (!first) try buf.appendSlice(allocator, ",");
            first = false;
            var row: [64]u8 = undefined;
            try buf.appendSlice(allocator, try std.fmt.bufPrint(&row, "({d},{d},{d})", .{ p * 1000 + o, p, o }));
        }
        first = true;
        try exec(allocator, db, buf.items);
    }
    const t = try db.openTable("big", .{});
    try t.flush();

    // ROW_NUMBER within each partition must equal o+1 for every row.
    const rn_bad = try helpers.collectBigints(allocator, db, "SELECT COUNT(*) FROM (SELECT o + 1 AS want, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS rn FROM big) t WHERE rn <> want");
    defer allocator.free(rn_bad);
    try std.testing.expectEqualSlices(i64, &[_]i64{0}, rn_bad);

    // LAG(id) must be id-1 within a partition and NULL at each partition's
    // first row — exactly 1000 NULLs.
    const lag_bad = try helpers.collectBigints(allocator, db, "SELECT COUNT(*) FROM (SELECT o, id - 1 AS idm, LAG(id, 1) OVER (PARTITION BY p ORDER BY o) AS prev FROM big) t " ++
        "WHERE (o = 0 AND prev IS NOT NULL) OR (o > 0 AND prev <> idm)");
    defer allocator.free(lag_bad);
    try std.testing.expectEqualSlices(i64, &[_]i64{0}, lag_bad);

    const lag_nulls = try helpers.collectBigints(allocator, db, "SELECT COUNT(*) FROM (SELECT o, LAG(id, 1) OVER (PARTITION BY p ORDER BY o) AS prev FROM big) t WHERE prev IS NULL");
    defer allocator.free(lag_nulls);
    try std.testing.expectEqualSlices(i64, &[_]i64{1000}, lag_nulls);

    // Global (no PARTITION BY) spec: the samplesort path. ROW_NUMBER over
    // id order equals each row's dense position; RANK over the heavily
    // tied p column equals p*100+1 for every row (tied keys split across
    // range buckets, serial eval walks the concatenated perm).
    const grn_bad = try helpers.collectBigints(allocator, db, "SELECT COUNT(*) FROM (SELECT p * 100 + o + 1 AS want, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM big) t WHERE rn <> want");
    defer allocator.free(grn_bad);
    try std.testing.expectEqualSlices(i64, &[_]i64{0}, grn_bad);

    const grk_bad = try helpers.collectBigints(allocator, db, "SELECT COUNT(*) FROM (SELECT p * 100 + 1 AS want, RANK() OVER (ORDER BY p) AS rk FROM big) t WHERE rk <> want");
    defer allocator.free(grk_bad);
    try std.testing.expectEqualSlices(i64, &[_]i64{0}, grk_bad);
}

test "V2 staged window: RANK + QUALIFY above a grouped block" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Below-window block = the V2 group handler in all-groups mode; the
    // window ranks the 12 group rows; QUALIFY filters on the window output.
    var q = try runSql(allocator, db, "SELECT UserID, c, RANK() OVER (ORDER BY c DESC) AS rnk " ++
        "FROM (SELECT UserID, COUNT(*) AS c FROM hits GROUP BY UserID) t " ++
        "QUALIFY rnk <= 3 ORDER BY rnk");
    defer q.deinit();

    var users: std.ArrayList(i64) = .empty;
    defer users.deinit(allocator);
    var counts: std.ArrayList(i64) = .empty;
    defer counts.deinit(allocator);
    var ranks: std.ArrayList(i64) = .empty;
    defer ranks.deinit(allocator);
    while (try q.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            users.append(allocator, batch.values[0].data.bigint[r]) catch unreachable;
            counts.append(allocator, batch.values[1].data.bigint[r]) catch unreachable;
            ranks.append(allocator, batch.values[2].data.bigint[r]) catch unreachable;
        }
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 12, 11, 10 }, users.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 12, 11, 10 }, counts.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, ranks.items);
}

// 200K single-row groups at max_dop=4 put the result emit on up to three
// range workers (65536+ groups each, fewer on a machine with fewer physical
// cores), and the pipeline hands each range out as its own batch. The BIGINT key takes the packed emit; the VARCHAR key the
// hashed emit, whose key values come back through the location order and
// its inverse permutation (both strided over the workers).
test "V2 group emit: parallel ranges emit every group once, as batches" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();

    const n_groups: usize = 200_000;
    try exec(allocator, db, "CREATE TABLE big (id BIGINT PRIMARY KEY, s VARCHAR(12) NOT NULL, v BIGINT NOT NULL)");
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    var next_id: usize = 0;
    while (next_id < n_groups) {
        buf.clearRetainingCapacity();
        try buf.appendSlice(allocator, "INSERT INTO big VALUES ");
        for (0..200) |i| {
            if (i != 0) try buf.appendSlice(allocator, ",");
            var row: [64]u8 = undefined;
            try buf.appendSlice(allocator, try std.fmt.bufPrint(&row, "({d},'k{d}',{d})", .{ next_id, next_id, next_id * 2 }));
            next_id += 1;
        }
        try exec(allocator, db, buf.items);
    }
    const t = try db.openTable("big", .{});
    try t.flush();

    const seen = try allocator.alloc(bool, n_groups);
    defer allocator.free(seen);
    inline for (.{
        .{ .sql = "SELECT id, MAX(v) AS mv FROM big GROUP BY id", .string_key = false },
        .{ .sql = "SELECT s, MAX(v) AS mv FROM big GROUP BY s", .string_key = true },
    }) |c| {
        @memset(seen, false);
        var q = try runSql(allocator, db, c.sql);
        defer q.deinit();
        var batches: usize = 0;
        var rows: usize = 0;
        while (try q.next()) |b| {
            batches += 1;
            for (0..b.row_count) |r| {
                const key: usize = if (c.string_key) blk: {
                    const bytes = switch (b.values[0].data) {
                        .varchar, .string => |sv| sv.rowBytes(r),
                        else => return error.TestUnexpectedResult,
                    };
                    try std.testing.expectEqual(@as(u8, 'k'), bytes[0]);
                    break :blk try std.fmt.parseInt(usize, bytes[1..], 10);
                } else @intCast(b.values[0].data.bigint[r]);
                try std.testing.expect(key < n_groups);
                try std.testing.expect(!seen[key]);
                seen[key] = true;
                try std.testing.expectEqual(@as(i64, @intCast(key * 2)), b.values[1].data.bigint[r]);
                rows += 1;
            }
        }
        try std.testing.expectEqual(n_groups, rows);
        try std.testing.expect(batches >= 2);
    }
}

test "string pipeline memory: parallel extrema preserve nulls, bytes, ties and partial batches" {
    const a = std.testing.allocator;
    const group_count = 16_391;
    const Row = struct { id: i64, g: i32, payload: ?[]const u8 };
    const Expected = struct { count: i64 = 0, lo: ?[]const u8 = null, hi: ?[]const u8 = null };
    const rows = try a.alloc(Row, group_count * 4);
    defer a.free(rows);
    const bytes = try a.alloc(u8, rows.len * 256);
    defer a.free(bytes);
    const expected = try a.alloc(Expected, group_count);
    defer a.free(expected);
    @memset(expected, .{});
    for (rows, 0..) |*row, i| {
        const g = i % group_count;
        const b = bytes[i * 256 ..][0 .. 64 + (i * 37) % 193];
        @memset(b, 'x');
        _ = try std.fmt.bufPrint(b[0..8], "{d:0>8}", .{(i * 31) % 997});
        const value: ?[]const u8 = if (g == 0 or (g > 2 and i % 11 == 0)) null else if (g == 1) "" else if (g == 2) "same\x00\xff" else b;
        row.* = .{ .id = @intCast(i), .g = @intCast(g), .payload = value };
        const e = &expected[g];
        e.count += 1;
        if (value) |v| {
            if (e.lo == null or std.mem.order(u8, v, e.lo.?) == .lt) e.lo = v;
            if (e.hi == null or std.mem.order(u8, v, e.hi.?) == .gt) e.hi = v;
        }
    }
    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(a, std.testing.io, tmp.dir, .{ .max_dop = dop, .auto_flush_secs = 0 });
        defer db.close();
        const table = try db.table("string_groups", .{
            .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "g", .type = .int }, .{ .name = "payload", .type = .string, .nullable = true } },
            .order_key = &.{"id"},
            .unique = false,
        }, .{ .order_key = &.{"id"}, .row_group_size = 256 });
        try table.insert(rows);
        try table.flush();
        for (0..3) |_| {
            var q = try runSql(a, db, "SELECT g, MIN(payload), MAX(payload), COUNT(*) FROM string_groups GROUP BY g HAVING COUNT(*) > 2 ORDER BY g LIMIT 20000");
            defer q.deinit();
            var seen: usize = 0;
            while (try q.next()) |batch| {
                for (0..batch.row_count) |r| {
                    const g: usize = @intCast(batch.values[0].data.int[r]);
                    try std.testing.expectEqual(seen, g);
                    const e = expected[g];
                    try std.testing.expectEqual(e.count, batch.values[3].data.bigint[r]);
                    for ([_]?[]const u8{ e.lo, e.hi }, 1..) |want, ci| {
                        try std.testing.expectEqual(want != null, batch.values[ci].isValid(r));
                        if (want) |v| try std.testing.expectEqualStrings(v, batch.values[ci].data.string.rowBytes(r));
                    }
                    seen += 1;
                }
            }
            try std.testing.expectEqual(@as(usize, group_count), seen);
        }
        var empty = try runSql(a, db, "SELECT g, MIN(payload), MAX(payload), COUNT(*) FROM string_groups WHERE g < 0 GROUP BY g ORDER BY g");
        defer empty.deinit();
        var empty_rows: usize = 0;
        while (try empty.next()) |batch| empty_rows += batch.row_count;
        try std.testing.expectEqual(@as(usize, 0), empty_rows);
    }
}

test "V2 weighted staging folds non-adjacent repeats with string extremes at DOP 1 and DOP 4" {
    // Four rows in five go to three hot keys, so a chunk's repeats are rarely
    // adjacent. `few` has too few keys to ever fill a chunk (its MIN inputs
    // strictly decrease, so every row appends bytes until the string-byte cap
    // publishes it), `many` fills chunks that fold, `cyclic` fills one with no
    // folds at all (switching the probe off), and `sk` is `many` as a string.
    // Aggregates sharing an input each stage their own copy of it.
    const a = std.testing.allocator;
    const n = 49_152;
    const lo_len = 512;
    const Row = struct { id: i64, few: i32, many: i32, cyclic: i32, sk: []const u8, v: i32, w: i32, lo_s: ?[]const u8, hi_s: ?[]const u8 };
    const Expected = struct { count: i64 = 0, sum_v: i64 = 0, min_v: i32 = std.math.maxInt(i32), max_v: i32 = std.math.minInt(i32), max_w: i32 = std.math.minInt(i32), lo: ?[]const u8 = null, hi: ?[]const u8 = null, hi_min: ?[]const u8 = null };
    const rows = try a.alloc(Row, n);
    defer a.free(rows);
    const lo_bytes = try a.alloc(u8, n * lo_len);
    defer a.free(lo_bytes);
    const text_bytes = try a.alloc(u8, n * 16);
    defer a.free(text_bytes);
    for (rows, 0..) |*row, i| {
        const hot = i % 5 != 0;
        const lo = lo_bytes[i * lo_len ..][0..lo_len];
        @memset(lo, 'p');
        _ = try std.fmt.bufPrint(lo[0..10], "{d:0>10}", .{n - i});
        const many: i32 = if (hot) @intCast(i % 3) else @intCast(100 + (i / 5) % 12_000);
        const few: i32 = if (hot) @intCast(i % 3) else @intCast(100 + (i / 5) % 2_000);
        const all_null = few == 100;
        row.* = .{
            .id = @intCast(i),
            .few = few,
            .many = many,
            .cyclic = @intCast(i % 9_001),
            .sk = try std.fmt.bufPrint(text_bytes[i * 16 ..][0..8], "k{d}", .{many}),
            .v = @intCast(i % 1_000),
            .w = @intCast((i * 31) % 9_973),
            .lo_s = if (all_null or i % 13 == 0) null else lo,
            .hi_s = if (all_null or i % 17 == 0) null else if (i % 23 == 0) "" else try std.fmt.bufPrint(text_bytes[i * 16 + 8 ..][0..8], "{d}", .{(i * 7_919) % 10_007}),
        };
    }
    const key_columns = .{ "few", "many", "cyclic", "sk" };
    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(a, std.testing.io, tmp.dir, .{ .max_dop = dop, .auto_flush_secs = 0 });
        defer db.close();
        const table = try db.table("folds", .{
            .columns = &.{
                .{ .name = "id", .type = .bigint },                     .{ .name = "few", .type = .int },
                .{ .name = "many", .type = .int },                      .{ .name = "cyclic", .type = .int },
                .{ .name = "sk", .type = .string },                     .{ .name = "v", .type = .int },
                .{ .name = "w", .type = .int },                         .{ .name = "lo_s", .type = .string, .nullable = true },
                .{ .name = "hi_s", .type = .string, .nullable = true },
            },
            .order_key = &.{"id"},
            .unique = false,
        }, .{ .order_key = &.{"id"}, .row_group_size = 4096 });
        try table.insert(rows);
        try table.flush();
        inline for (key_columns) |key| {
            const is_text = comptime std.mem.eql(u8, key, "sk");
            var expected: std.AutoHashMapUnmanaged(i32, Expected) = .empty;
            defer expected.deinit(a);
            for (rows) |row| {
                const e = try expected.getOrPut(a, if (is_text) row.many else @field(row, key));
                if (!e.found_existing) e.value_ptr.* = .{};
                e.value_ptr.count += 1;
                e.value_ptr.sum_v += row.v;
                e.value_ptr.min_v = @min(e.value_ptr.min_v, row.v);
                e.value_ptr.max_v = @max(e.value_ptr.max_v, row.v);
                e.value_ptr.max_w = @max(e.value_ptr.max_w, row.w);
                if (row.lo_s) |s| {
                    if (e.value_ptr.lo == null or std.mem.order(u8, s, e.value_ptr.lo.?) == .lt) e.value_ptr.lo = s;
                }
                if (row.hi_s) |s| {
                    if (e.value_ptr.hi == null or std.mem.order(u8, s, e.value_ptr.hi.?) == .gt) e.value_ptr.hi = s;
                    if (e.value_ptr.hi_min == null or std.mem.order(u8, s, e.value_ptr.hi_min.?) == .lt) e.value_ptr.hi_min = s;
                }
            }
            inline for (.{ false, true }) |shared| {
                const tail = if (shared) "MIN(hi_s), MAX(hi_s), MIN(v), MAX(v)" else "MIN(lo_s), MAX(hi_s)";
                const sql = "SELECT " ++ key ++ ", COUNT(*), SUM(v), MAX(w), " ++ tail ++ " FROM folds GROUP BY " ++ key;
                errdefer std.debug.print("dop {d}: {s}\n", .{ dop, sql });
                try expectPlanRunsGroupTopN(a, db, sql);
                var q = try runSql(a, db, sql);
                defer q.deinit();
                var seen: usize = 0;
                while (try q.next()) |batch| {
                    for (0..batch.row_count) |r| {
                        const k: i32 = if (is_text)
                            try std.fmt.parseInt(i32, batch.values[0].data.string.rowBytes(r)[1..], 10)
                        else
                            batch.values[0].data.int[r];
                        const e = expected.get(k) orelse return error.TestUnexpectedResult;
                        try std.testing.expectEqual(e.count, batch.values[1].data.bigint[r]);
                        try std.testing.expectEqual(e.sum_v, batch.values[2].data.bigint[r]);
                        try std.testing.expectEqual(e.max_w, batch.values[3].data.int[r]);
                        for ([_]?[]const u8{ if (shared) e.hi_min else e.lo, e.hi }, 4..) |want, ci| {
                            try std.testing.expectEqual(want != null, batch.values[ci].isValid(r));
                            if (want) |s| try std.testing.expectEqualStrings(s, batch.values[ci].data.string.rowBytes(r));
                        }
                        if (shared) {
                            try std.testing.expectEqual(e.min_v, batch.values[6].data.int[r]);
                            try std.testing.expectEqual(e.max_v, batch.values[7].data.int[r]);
                        }
                        seen += 1;
                    }
                }
                try std.testing.expectEqual(@as(usize, expected.count()), seen);
            }
        }
    }
}

/// The first column of `sql`'s rows as text.
fn firstColumnText(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) ![]?[]u8 {
    var q = try helpers.runSqlCtx(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(?[]u8) = .empty;
    errdefer {
        for (out.items) |v| if (v) |x| allocator.free(x);
        out.deinit(allocator);
    }
    while (try q.next()) |batch| {
        const col = batch.values[0];
        for (0..batch.row_count) |row| {
            const text: ?[]u8 = if (!col.isValid(row)) null else switch (col.data) {
                .string, .varchar, .char => |sv| try allocator.dupe(u8, sv.rowBytes(row)),
                inline .int, .bigint => |s| try std.fmt.allocPrint(allocator, "{d}", .{s[row]}),
                else => return error.TestUnexpectedType,
            };
            errdefer if (text) |x| allocator.free(x);
            try out.append(allocator, text);
        }
    }
    return out.toOwnedSlice(allocator);
}

test "V2 group-topN: ORDER BY and HAVING read a hashed group key from the finished groups (issue #329)" {
    // A string key, or integer keys wider than 128 bits together, group by a
    // digest whose key values only exist once emit reads them back.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE ds (id INT, s VARCHAR(10), a BIGINT, b BIGINT, c BIGINT)");
    try exec(allocator, db, "INSERT INTO ds VALUES (1, 'b', 3, 1, 1), (2, 'c', 1, 2, 2), (3, 'a', 2, 3, 3), (4, 'd', 5, 4, 4), (5, 'e', 4, 5, 5)");
    const t = try db.openTable("ds", .{});
    try t.flush();

    const cases = .{
        .{ "SELECT CONCAT('v', s) AS z, COUNT(*) AS n FROM ds GROUP BY z ORDER BY z", &[_][]const u8{ "va", "vb", "vc", "vd", "ve" } },
        .{ "SELECT CONCAT('v', s) AS z, COUNT(*) AS n FROM ds GROUP BY z ORDER BY z DESC LIMIT 2", &[_][]const u8{ "ve", "vd" } },
        .{ "SELECT CONCAT('v', s) AS z, COUNT(*) AS n FROM ds GROUP BY z ORDER BY z LIMIT 2 OFFSET 1", &[_][]const u8{ "vb", "vc" } },
        .{ "SELECT CONCAT('v', s) AS z FROM ds GROUP BY z ORDER BY z", &[_][]const u8{ "va", "vb", "vc", "vd", "ve" } },
        .{ "SELECT CAST(a AS CHAR) AS z, COUNT(*) AS n FROM ds GROUP BY z ORDER BY z", &[_][]const u8{ "1", "2", "3", "4", "5" } },
        .{ "SELECT a, b, c, COUNT(*) AS n FROM ds GROUP BY a, b, c ORDER BY a", &[_][]const u8{ "1", "2", "3", "4", "5" } },
        .{ "SELECT a, b, c, COUNT(*) AS n FROM ds GROUP BY a, b, c ORDER BY b DESC LIMIT 2 OFFSET 1", &[_][]const u8{ "5", "2" } },
        .{ "SELECT a * 1 AS x, b, c, COUNT(*) AS n FROM ds GROUP BY x, b, c HAVING x > 2 ORDER BY x", &[_][]const u8{ "3", "4", "5" } },
        .{ "SELECT CONCAT('v', s) AS z, COUNT(*) AS n FROM ds GROUP BY z HAVING z > 'vb' ORDER BY z", &[_][]const u8{ "vc", "vd", "ve" } },
        .{ "SELECT CONCAT(s, '') AS z, SUM(a) AS t FROM ds GROUP BY z ORDER BY t DESC LIMIT 3", &[_][]const u8{ "d", "e", "b" } },
    };
    inline for (cases) |case| {
        errdefer std.debug.print("query: {s}\n", .{case[0]});
        const got = try firstColumnText(allocator, db, case[0]);
        defer helpers.freeStrings(allocator, got);
        try std.testing.expectEqual(case[1].len, got.len);
        for (case[1], got) |want, cell| try std.testing.expectEqualStrings(want, cell.?);
    }
}

fn cellNumber(col: anytype, row: usize) !?f64 {
    if (!col.isValid(row)) return null;
    return switch (col.data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint => |s| @floatFromInt(s[row]),
        inline .float, .double => |s| @floatCast(s[row]),
        else => error.TestUnexpectedType,
    };
}

fn expectPlanRunsGroupTopN(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) !void {
    const explain_sql = try std.fmt.allocPrint(allocator, "EXPLAIN {s}", .{sql});
    defer allocator.free(explain_sql);
    var q = try runSql(allocator, db, explain_sql);
    defer q.deinit();
    var found = false;
    while (try q.next()) |batch| {
        for (0..batch.row_count) |i| {
            if (std.mem.indexOf(u8, batch.values[0].data.string.rowBytes(i), "V2 group-topN") != null) found = true;
        }
    }
    try std.testing.expect(found);
}

test "V2 group-topN: nullable and BIGINT aggregates agree at DOP 1 and DOP 4" {
    // DOP 1 folds each staged chunk straight into one group table; DOP 4
    // partitions chunks into buckets folded by several workers. Nullable
    // inputs take the validity-aware fold kernels, BIGINT SUM/AVG the
    // two-slot wide state, and group 0 sees only NULL inputs.
    const allocator = std.testing.allocator;
    const group_count = 20_011;
    const Row = struct { id: i64, g: ?i32, v: ?i32, b: ?i64, d: ?f64 };
    const Expected = struct {
        id_sum: i64 = 0,
        count: i64 = 0,
        v_sum: i64 = 0,
        v_n: i64 = 0,
        v_min: ?i64 = null,
        v_max: ?i64 = null,
        b_sum: i64 = 0,
        b_n: i64 = 0,
        d_sum: f64 = 0,
        d_min: ?f64 = null,
        d_max: ?f64 = null,
    };
    const rows = try allocator.alloc(Row, group_count * 6);
    defer allocator.free(rows);
    const expected = try allocator.alloc(Expected, group_count);
    defer allocator.free(expected);
    @memset(expected, .{});
    for (rows, 0..) |*row, i| {
        // Row pairs share a group, so the fold sees adjacent runs as well as
        // scattered rows. SUM(id) = 12g + const, a tie-free ranking.
        const g = (i / 2) % group_count;
        const all_null = g == 0;
        const v: ?i32 = if (all_null or i % 7 == 0) null else @as(i32, @intCast((i * 37) % 1000)) - 500;
        const b: ?i64 = if (all_null or i % 5 == 0) null else @as(i64, @intCast(i)) * 3_000_000_019;
        const d: ?f64 = if (all_null or i % 3 == 0) null else (@as(f64, @floatFromInt((i * 13) % 512)) - 256) * 0.25;
        row.* = .{ .id = @intCast(i), .g = @intCast(g), .v = v, .b = b, .d = d };
        const e = &expected[g];
        e.id_sum += @intCast(i);
        e.count += 1;
        if (v) |x| {
            const wide_x: i64 = x;
            e.v_sum += wide_x;
            e.v_n += 1;
            e.v_min = if (e.v_min) |m| @min(m, wide_x) else wide_x;
            e.v_max = if (e.v_max) |m| @max(m, wide_x) else wide_x;
        }
        if (b) |x| {
            e.b_sum += x;
            e.b_n += 1;
        }
        if (d) |x| {
            e.d_sum += x;
            e.d_min = if (e.d_min) |m| @min(m, x) else x;
            e.d_max = if (e.d_max) |m| @max(m, x) else x;
        }
    }
    const int_sql = "SELECT g, SUM(id) AS si, COUNT(*) AS c, SUM(v) AS sv, AVG(v) AS av, MIN(v) AS mnv, MAX(v) AS mxv, SUM(b) AS sb FROM agg_rows GROUP BY g ORDER BY si DESC";
    const float_sql = "SELECT g, SUM(id) AS si, AVG(b) AS ab, SUM(d) AS sd, MIN(d) AS mnd, MAX(d) AS mxd FROM agg_rows GROUP BY g ORDER BY si DESC";
    const pages = .{ .{ group_count, 0 }, .{ 25, 7 } };
    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .auto_flush_secs = 0 });
        defer db.close();
        const t = try db.table("agg_rows", .{
            .columns = &.{
                .{ .name = "id", .type = .bigint },
                .{ .name = "g", .type = .int, .nullable = true },
                .{ .name = "v", .type = .int, .nullable = true },
                .{ .name = "b", .type = .bigint, .nullable = true },
                .{ .name = "d", .type = .double, .nullable = true },
            },
            .order_key = &.{"id"},
            .unique = false,
        }, .{ .order_key = &.{"id"} });
        try t.insert(rows);
        try t.flush();
        inline for (.{ int_sql, float_sql }) |base_sql| {
            try expectPlanRunsGroupTopN(allocator, db, base_sql ++ " LIMIT 10");
            inline for (pages) |page| {
                const sql = std.fmt.comptimePrint("{s} LIMIT {d} OFFSET {d}", .{ base_sql, page[0], page[1] });
                errdefer std.debug.print("dop={d} query: {s}\n", .{ dop, sql });
                var q = try runSql(allocator, db, sql);
                defer q.deinit();
                var rank: usize = page[1];
                while (try q.next()) |batch| {
                    for (0..batch.row_count) |r| {
                        const g: usize = @intCast(batch.values[0].data.int[r]);
                        try std.testing.expectEqual(group_count - 1 - rank, g);
                        const e = expected[g];
                        try std.testing.expectEqual(@as(?f64, @floatFromInt(e.id_sum)), try cellNumber(batch.values[1], r));
                        if (comptime std.mem.eql(u8, base_sql, int_sql)) {
                            const v_avg: ?f64 = if (e.v_n == 0) null else @as(f64, @floatFromInt(e.v_sum)) / @as(f64, @floatFromInt(e.v_n));
                            try std.testing.expectEqual(@as(?f64, @floatFromInt(e.count)), try cellNumber(batch.values[2], r));
                            try std.testing.expectEqual(if (e.v_n == 0) null else @as(?f64, @floatFromInt(e.v_sum)), try cellNumber(batch.values[3], r));
                            try std.testing.expectEqual(v_avg, try cellNumber(batch.values[4], r));
                            try std.testing.expectEqual(if (e.v_min) |x| @as(?f64, @floatFromInt(x)) else null, try cellNumber(batch.values[5], r));
                            try std.testing.expectEqual(if (e.v_max) |x| @as(?f64, @floatFromInt(x)) else null, try cellNumber(batch.values[6], r));
                            try std.testing.expectEqual(if (e.b_n == 0) null else @as(?f64, @floatFromInt(e.b_sum)), try cellNumber(batch.values[7], r));
                        } else {
                            const b_avg: ?f64 = if (e.b_n == 0) null else @as(f64, @floatFromInt(e.b_sum)) / @as(f64, @floatFromInt(e.b_n));
                            try std.testing.expectEqual(b_avg, try cellNumber(batch.values[2], r));
                            try std.testing.expectEqual(if (e.d_min == null) null else @as(?f64, e.d_sum), try cellNumber(batch.values[3], r));
                            try std.testing.expectEqual(e.d_min, try cellNumber(batch.values[4], r));
                            try std.testing.expectEqual(e.d_max, try cellNumber(batch.values[5], r));
                        }
                        rank += 1;
                    }
                }
                try std.testing.expectEqual(@as(usize, page[1] + @min(page[0], group_count - page[1])), rank);
            }
        }
    }
}

fn collectNumberRows(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) ![][2]?f64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList([2]?f64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |batch| {
        for (0..batch.row_count) |r| try out.append(allocator, .{ try cellNumber(batch.values[0], r), try cellNumber(batch.values[1], r) });
    }
    return out.toOwnedSlice(allocator);
}

test "V2 group-topN: ranked emit returns the full sort's page across fill workers, with ties and HAVING" {
    // Past 2 × 65536 groups the core's emit ranks groups on several fill
    // workers, while each keeps under half of its range's groups; past that it
    // emits every group for the final heap selection, and from LIMIT+OFFSET at
    // half the groups for a full sort. All must yield the same page, ties in
    // the ORDER BY key included (they break on the group key).
    const allocator = std.testing.allocator;
    const group_count = 140_009;
    const Row = struct { id: i64, g: i64, v: i32 };
    var rows: std.ArrayList(Row) = .empty;
    defer rows.deinit(allocator);
    for (0..group_count) |k| {
        for (0..1 + k % 3) |j| {
            try rows.append(allocator, .{
                .id = @intCast(rows.items.len),
                .g = @as(i64, @intCast(k)) * 1_000_003,
                .v = @intCast((k * 31 + j * 7) % 17),
            });
        }
    }
    const cases = .{
        .{ "SELECT g, MIN(v) AS m FROM rank_rows GROUP BY g ORDER BY m DESC", 10, 3 },
        .{ "SELECT g, SUM(v) AS s FROM rank_rows GROUP BY g HAVING COUNT(*) > 1 ORDER BY s", 20, 0 },
        .{ "SELECT g, COUNT(*) AS c FROM rank_rows GROUP BY g ORDER BY c, g DESC", 7, 11 },
        .{ "SELECT g, AVG(v) AS a FROM rank_rows GROUP BY g ORDER BY a DESC, g", 5, 0 },
        .{ "SELECT g, SUM(v) AS s FROM rank_rows GROUP BY g ORDER BY s DESC, g", 40, 39_990 },
    };
    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .auto_flush_secs = 0 });
        defer db.close();
        const t = try db.table("rank_rows", .{
            .columns = &.{
                .{ .name = "id", .type = .bigint },
                .{ .name = "g", .type = .bigint },
                .{ .name = "v", .type = .int },
            },
            .order_key = &.{"id"},
            .unique = false,
        }, .{ .order_key = &.{"id"} });
        try t.insert(rows.items);
        try t.flush();
        inline for (cases) |case| {
            const page_sql = std.fmt.comptimePrint("{s} LIMIT {d} OFFSET {d}", .{ case[0], case[1], case[2] });
            const full_sql = std.fmt.comptimePrint("{s} LIMIT {d}", .{ case[0], group_count });
            errdefer std.debug.print("dop={d} query: {s}\n", .{ dop, page_sql });
            try expectPlanRunsGroupTopN(allocator, db, page_sql);
            const page = try collectNumberRows(allocator, db, page_sql);
            defer allocator.free(page);
            const full = try collectNumberRows(allocator, db, full_sql);
            defer allocator.free(full);
            try std.testing.expectEqual(@as(usize, case[1]), page.len);
            try std.testing.expectEqualSlices([2]?f64, full[case[2] .. case[2] + case[1]], page);
        }
    }
}

// One (group, value) membership for the grouped COUNT(DISTINCT) reference: a
// string value in `str`, a numeric one's canonical bits in `num`.
const RefPair = struct {
    g: usize,
    num: u64 = 0,
    str: []const u8 = "",

    fn lessThan(_: void, a: RefPair, b: RefPair) bool {
        if (a.g != b.g) return a.g < b.g;
        switch (std.mem.order(u8, a.str, b.str)) {
            .lt => return true,
            .gt => return false,
            .eq => return a.num < b.num,
        }
    }

    fn eql(a: RefPair, b: RefPair) bool {
        return a.g == b.g and a.num == b.num and std.mem.eql(u8, a.str, b.str);
    }
};

fn refDistinctCounts(pairs: []RefPair, counts: []i64) void {
    @memset(counts, 0);
    std.mem.sort(RefPair, pairs, {}, RefPair.lessThan);
    for (pairs, 0..) |p, i| {
        if (i > 0 and RefPair.eql(pairs[i - 1], p)) continue;
        counts[p.g] += 1;
    }
}

// -0.0 and 0.0 are one value, and so is every NaN (#82).
fn canonicalDoubleBits(x: f64) u64 {
    if (std.math.isNan(x)) return 0x7ff8_0000_0000_0000;
    if (x == 0) return 0;
    return @bitCast(x);
}

fn planContains(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, needle: []const u8) !bool {
    const explain_sql = try std.fmt.allocPrint(allocator, "EXPLAIN {s}", .{sql});
    defer allocator.free(explain_sql);
    var q = try runSql(allocator, db, explain_sql);
    defer q.deinit();
    var found = false;
    while (try q.next()) |batch| {
        for (0..batch.row_count) |i| {
            if (std.mem.indexOf(u8, batch.values[0].data.string.rowBytes(i), needle) != null) found = true;
        }
    }
    return found;
}

const GD_GROUPS = 1009;
const GdRow = struct { id: i64, g: i32, gn: ?i32, gb: ?i64, s: ?[]const u8, c: []const u8, d: ?f64, v: ?i32 };

const GdExpected = struct {
    n: []i64,
    ds: []i64,
    dc: []i64,
    dd: []i64,
    case_s: []i64,
    if_id: []i64,
    min_s: []?[]const u8,

    fn init(allocator: std.mem.Allocator) !GdExpected {
        var e: GdExpected = undefined;
        inline for (.{ "n", "ds", "dc", "dd", "case_s", "if_id" }) |f| {
            @field(e, f) = try allocator.alloc(i64, GD_GROUPS);
        }
        e.min_s = try allocator.alloc(?[]const u8, GD_GROUPS);
        return e;
    }

    fn deinit(self: GdExpected, allocator: std.mem.Allocator) void {
        inline for (.{ "n", "ds", "dc", "dd", "case_s", "if_id" }) |f| allocator.free(@field(self, f));
        allocator.free(self.min_s);
    }

    fn compute(self: GdExpected, allocator: std.mem.Allocator, rows: []const GdRow, live: []const bool) !void {
        @memset(self.n, 0);
        @memset(self.min_s, null);
        var pairs: std.ArrayList(RefPair) = .empty;
        defer pairs.deinit(allocator);
        inline for (.{ "ds", "dc", "dd", "case_s", "if_id" }) |f| {
            pairs.clearRetainingCapacity();
            for (rows, live) |r, keep| {
                if (!keep) continue;
                const g: usize = @intCast(r.g);
                const pair: ?RefPair = if (comptime std.mem.eql(u8, f, "ds"))
                    (if (r.s) |s| RefPair{ .g = g, .str = s } else null)
                else if (comptime std.mem.eql(u8, f, "dc"))
                    RefPair{ .g = g, .str = r.c }
                else if (comptime std.mem.eql(u8, f, "dd"))
                    (if (r.d) |d| RefPair{ .g = g, .num = canonicalDoubleBits(d) } else null)
                else if (comptime std.mem.eql(u8, f, "case_s"))
                    (if (r.v != null and r.v.? > 0 and r.s != null) RefPair{ .g = g, .str = r.s.? } else null)
                else
                    (if (r.v != null and r.v.? > 0) RefPair{ .g = g, .num = @intCast(@mod(r.id, 97)) } else null);
                if (pair) |p| try pairs.append(allocator, p);
            }
            refDistinctCounts(pairs.items, @field(self, f));
        }
        for (rows, live) |r, keep| {
            if (!keep) continue;
            const g: usize = @intCast(r.g);
            self.n[g] += 1;
            if (r.s) |s| {
                if (self.min_s[g] == null or std.mem.order(u8, s, self.min_s[g].?) == .lt) self.min_s[g] = s;
            }
        }
    }
};

// Keys `g` and `gn` (the same values in a nullable INT column) take the
// lowcard route; `gb`, the same values as a nullable BIGINT, packs past 64
// bits, where COUNT(DISTINCT) leaves lowcard for the silo's group-topN route.
fn checkGroupedDistinct(allocator: std.mem.Allocator, db: *thindb.Database, e: GdExpected, comptime key: []const u8, comptime having: []const u8, route: []const u8) !void {
    const multi_sql = "SELECT " ++ key ++ ", COUNT(DISTINCT s) AS ds, COUNT(DISTINCT c) AS dc, COUNT(DISTINCT d) AS dd, COUNT(*) AS n FROM gd GROUP BY " ++ key ++ having;
    const case_sql = "SELECT " ++ key ++ ", COUNT(DISTINCT CASE WHEN v > 0 THEN s END) AS cs, COUNT(DISTINCT IF(v > 0, id % 97, NULL)) AS ci FROM gd GROUP BY " ++ key ++ having;
    inline for (.{ multi_sql, case_sql }) |sql| {
        errdefer std.debug.print("route={s} query: {s}\n", .{ route, sql });
        try std.testing.expect(try planContains(allocator, db, sql, route));
        var q = try runSql(allocator, db, sql);
        defer q.deinit();
        var seen = [_]bool{false} ** GD_GROUPS;
        var groups: usize = 0;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |r| {
                const g: usize = @intFromFloat((try cellNumber(batch.values[0], r)).?);
                try std.testing.expect(!seen[g]);
                seen[g] = true;
                groups += 1;
                const want: []const i64 = if (comptime std.mem.eql(u8, sql, multi_sql))
                    &.{ e.ds[g], e.dc[g], e.dd[g], e.n[g] }
                else
                    &.{ e.case_s[g], e.if_id[g] };
                for (want, 1..) |w, ci| {
                    errdefer std.debug.print("group {d} column {d}\n", .{ g, ci });
                    try std.testing.expectEqual(@as(?f64, @floatFromInt(w)), try cellNumber(batch.values[ci], r));
                }
            }
        }
        try std.testing.expectEqual(@as(usize, GD_GROUPS), groups);
    }
}

// A byte-string distinct grouped by a dict-coded string key, optionally next
// to a COUNT(DISTINCT) over that key itself, which is 1 in every group (the
// key's coded batch value is a placeholder, so that shape stays on the silo).
fn checkKeyDistinct(allocator: std.mem.Allocator, db: *thindb.Database, rows: []const GdRow, live: []const bool, c_pool: []const []const u8, comptime with_key: bool, comptime having: []const u8, route: []const u8) !void {
    const sql = "SELECT c, COUNT(DISTINCT s) AS ds" ++ (if (with_key) ", COUNT(DISTINCT c) AS dk" else "") ++ " FROM gd GROUP BY c" ++ having;
    errdefer std.debug.print("route={s} query: {s}\n", .{ route, sql });
    try std.testing.expect(try planContains(allocator, db, sql, route));
    var pairs: std.ArrayList(RefPair) = .empty;
    defer pairs.deinit(allocator);
    for (rows, live) |r, keep| {
        if (!keep) continue;
        const ci = for (c_pool, 0..) |c, k| {
            if (std.mem.eql(u8, c, r.c)) break k;
        } else return error.TestUnexpectedResult;
        if (r.s) |s| try pairs.append(allocator, .{ .g = ci, .str = s });
    }
    const want = try allocator.alloc(i64, c_pool.len);
    defer allocator.free(want);
    refDistinctCounts(pairs.items, want);

    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var groups: usize = 0;
    while (try q.next()) |batch| {
        for (0..batch.row_count) |r| {
            const key = batch.values[0].data.string.rowBytes(r);
            const ci = for (c_pool, 0..) |c, k| {
                if (std.mem.eql(u8, c, key)) break k;
            } else return error.TestUnexpectedResult;
            groups += 1;
            try std.testing.expectEqual(@as(?f64, @floatFromInt(want[ci])), try cellNumber(batch.values[1], r));
            if (with_key) try std.testing.expectEqual(@as(?f64, 1), try cellNumber(batch.values[2], r));
        }
    }
    try std.testing.expectEqual(c_pool.len, groups);
}

test "V2 grouped COUNT(DISTINCT) over strings, doubles and CASE/IF inputs on both group routes at DOP 1 and DOP 4" {
    // Group 0 sees only NULL s/d/v, so its string, double and conditional
    // distinct counts are 0. The test build narrows the string digest to 8
    // bits, so equal-digest chains are the norm. Phase 1 reads coded and
    // byte-string inputs from flushed segments; phase 2 adds tombstones and
    // memtable rows, whose batches carry no dict-code sidecar.
    const allocator = std.testing.allocator;
    const batches = 4;
    const batch_rows = 3000;
    const rows = try allocator.alloc(GdRow, batches * batch_rows);
    defer allocator.free(rows);
    const live = try allocator.alloc(bool, rows.len);
    defer allocator.free(live);

    var s_pool: [600][]const u8 = undefined;
    var s_bytes: [600][48]u8 = undefined;
    for (&s_pool, &s_bytes, 0..) |*s, *buf, p| {
        s.* = switch (p % 50) {
            1 => "",
            2 => "a",
            3 => "a\x00",
            4 => "\x00",
            5 => "12345678",
            6 => "123456789",
            else => blk: {
                const len = 3 + (p * 7) % 40;
                @memset(buf[0..len], 'x');
                _ = try std.fmt.bufPrint(buf[0..3], "{d:0>3}", .{p});
                break :blk buf[0..len];
            },
        };
    }
    const c_pool = [_][]const u8{ "", "\x00\x01", "alpha", "beta", "a-value-longer-than-eight-bytes", "a-value-longer-than-eight-bytez", "Z", "z" };
    const nan_a: f64 = @bitCast(@as(u64, 0x7ff8_0000_0000_0001));
    const nan_b: f64 = @bitCast(@as(u64, 0xfff8_0000_0000_0000));
    const d_pool = [_]f64{ -0.0, 0.0, nan_a, nan_b, 1.5, -2.25, 1e300, -1e-300 };

    for (rows, 0..) |*row, i| {
        const g: usize = if (i % 2 == 0) (i / 4) % GD_GROUPS else (i * 7) % GD_GROUPS;
        const p = (i * 2_654_435_761) % 600;
        const all_null = g == 0;
        row.* = .{
            .id = @intCast(i),
            .g = @intCast(g),
            .gn = @intCast(g),
            .gb = @intCast(g),
            .s = if (all_null or p % 50 == 0) null else s_pool[p],
            .c = c_pool[(i * 5) % c_pool.len],
            .d = if (all_null or i % 13 == 0) null else if (i % 3 == 0) d_pool[i % d_pool.len] else @as(f64, @floatFromInt(i % 300)) * 0.5,
            .v = if (all_null or i % 11 == 0) null else @as(i32, @intCast((i * 37) % 200)) - 100,
        };
    }

    var expected = try GdExpected.init(allocator);
    defer expected.deinit(allocator);

    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        errdefer std.debug.print("dop={d}\n", .{dop});
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .auto_flush_secs = 0 });
        defer db.close();
        const t = try db.table("gd", .{
            .columns = &.{
                .{ .name = "id", .type = .bigint },
                .{ .name = "g", .type = .int },
                .{ .name = "gn", .type = .int, .nullable = true },
                .{ .name = "gb", .type = .bigint, .nullable = true },
                .{ .name = "s", .type = .string, .nullable = true },
                .{ .name = "c", .type = .string },
                .{ .name = "d", .type = .double, .nullable = true },
                .{ .name = "v", .type = .int, .nullable = true },
            },
            .order_key = &.{"id"},
            .unique = true,
        }, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 256 });

        for (0..batches - 1) |b| {
            try t.insert(rows[b * batch_rows ..][0..batch_rows]);
            try t.flush();
        }
        @memset(live, false);
        @memset(live[0 .. (batches - 1) * batch_rows], true);
        try expected.compute(allocator, rows, live);
        try checkGroupedDistinct(allocator, db, expected, "g", "", "lowcard");
        try checkGroupedDistinct(allocator, db, expected, "g", " HAVING COUNT(*) > 0", "lowcard");
        try checkGroupedDistinct(allocator, db, expected, "gn", " HAVING COUNT(*) > 0", "lowcard");
        try checkGroupedDistinct(allocator, db, expected, "gb", " HAVING COUNT(*) > 0", "V2 group-topN");
        // The low-NDV non-nullable string folds as dict codes, the nullable
        // one by bytes; the CASE/IF inputs are derived string and int values.
        try std.testing.expect(try planContains(allocator, db, "SELECT g, COUNT(DISTINCT s), COUNT(DISTINCT c), COUNT(DISTINCT d) FROM gd GROUP BY g", "distinct string,coded,float"));
        try std.testing.expect(try planContains(allocator, db, "SELECT g, COUNT(DISTINCT CASE WHEN v > 0 THEN s END), COUNT(DISTINCT IF(v > 0, id % 97, NULL)) FROM gd GROUP BY g", "distinct string,int"));
        try checkKeyDistinct(allocator, db, rows, live, &c_pool, false, "", "lowcard");
        try checkKeyDistinct(allocator, db, rows, live, &c_pool, true, "", "V2 group-topN");
        try checkKeyDistinct(allocator, db, rows, live, &c_pool, true, " HAVING COUNT(*) > 0", "V2 group-topN");

        // Tombstones in the first row groups only: their batches carry no
        // sidecar, so the coded key and coded distinct input intern their
        // bytes per row while the clean row groups arrive coded — both must
        // land in one code space on every worker.
        try exec(allocator, db, "DELETE FROM gd WHERE id % 17 = 0 AND id < 2000");
        for (live, 0..) |*keep, i| keep.* = i < (batches - 1) * batch_rows and (i % 17 != 0 or i >= 2000);
        try expected.compute(allocator, rows, live);
        try checkGroupedDistinct(allocator, db, expected, "g", "", "lowcard");
        try checkKeyDistinct(allocator, db, rows, live, &c_pool, false, "", "lowcard");

        try t.insert(rows[(batches - 1) * batch_rows ..]);
        // The DELETE ran before the memtable batch arrived.
        for (live, 0..) |*keep, i| keep.* = i >= (batches - 1) * batch_rows or i % 17 != 0 or i >= 2000;
        try expected.compute(allocator, rows, live);
        try checkGroupedDistinct(allocator, db, expected, "g", "", "lowcard");
        try checkGroupedDistinct(allocator, db, expected, "g", " HAVING COUNT(*) > 0", "lowcard");
        try checkGroupedDistinct(allocator, db, expected, "gn", " HAVING COUNT(*) > 0", "lowcard");
        try checkGroupedDistinct(allocator, db, expected, "gb", " HAVING COUNT(*) > 0", "V2 group-topN");

        // A string result and a string distinct share the staged string lane.
        const mixed_sql = "SELECT g, MIN(s) AS lo, COUNT(DISTINCT s) AS ds FROM gd GROUP BY g HAVING COUNT(*) > 0";
        var q = try runSql(allocator, db, mixed_sql);
        defer q.deinit();
        var groups: usize = 0;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |r| {
                const g: usize = @intCast(batch.values[0].data.int[r]);
                groups += 1;
                try std.testing.expectEqual(expected.min_s[g] != null, batch.values[1].isValid(r));
                if (expected.min_s[g]) |lo| try std.testing.expectEqualStrings(lo, batch.values[1].data.string.rowBytes(r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(expected.ds[g])), try cellNumber(batch.values[2], r));
            }
        }
        try std.testing.expectEqual(@as(usize, GD_GROUPS), groups);
    }
}

test "V2 grouped COUNT(DISTINCT json) counts byte-distinct documents like GROUP BY" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();
    try exec(allocator, db, "CREATE TABLE gj (id INT, g INT NOT NULL, j JSON)");
    try exec(allocator, db,
        \\INSERT INTO gj VALUES (1, 1, '{"a":1}'), (2, 1, '{"a": 1}'), (3, 1, '{"b":[1,2]}'), (4, 1, NULL),
        \\(5, 2, '[1,2]'), (6, 2, '[1,2]'), (7, 2, '[1, 2]'), (8, 2, '"x"'), (9, 3, NULL), (10, 3, NULL)
    );
    const t = try db.openTable("gj", .{});
    try t.flush();
    const reference = try helpers.collectBigints(allocator, db, "SELECT COUNT(*) FROM (SELECT g, j FROM gj WHERE j IS NOT NULL GROUP BY g, j) t GROUP BY g ORDER BY g");
    defer allocator.free(reference);
    try std.testing.expectEqual(@as(usize, 2), reference.len);
    inline for (.{ "", " HAVING COUNT(*) > 0" }) |having| {
        const got = try helpers.collectBigints(allocator, db, "SELECT COUNT(DISTINCT j) FROM gj GROUP BY g" ++ having ++ " ORDER BY g");
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, &.{ reference[0], reference[1], 0 }, got);
    }
}

test "V2 grouped string COUNT(DISTINCT) stays inside the query budget and leaks nothing when it fails" {
    const allocator = std.testing.allocator;
    const group_count = 61;
    const Row = struct { id: i64, g: i32, payload: []const u8 };
    const rows = try allocator.alloc(Row, 4096);
    defer allocator.free(rows);
    const bytes = try allocator.alloc(u8, rows.len * 200);
    defer allocator.free(bytes);
    for (rows, 0..) |*row, i| {
        const b = bytes[i * 200 ..][0..200];
        @memset(b, 'p');
        _ = try std.fmt.bufPrint(b[0..8], "{d:0>8}", .{i});
        row.* = .{ .id = @intCast(i), .g = @intCast(i % group_count), .payload = b };
    }
    const budgets = [_]usize{ 64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024, 64 * 1024 * 1024 };
    for (budgets, 0..) |budget, bi| {
        errdefer std.debug.print("budget={d}\n", .{budget});
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{
            .memory_budget = budget,
            .auto_flush_secs = 0,
            .max_dop = 4,
        });
        defer db.close();
        const t = try db.table("wide_distinct", .{
            .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "g", .type = .int }, .{ .name = "payload", .type = .string } },
            .order_key = &.{"id"},
            .unique = false,
        }, .{ .order_key = &.{"id"}, .row_group_size = 256 });
        try t.insert(rows);
        try t.flush();
        inline for (.{ "", " HAVING COUNT(*) > 0" }) |having| {
            const sql = "SELECT g, COUNT(DISTINCT payload) FROM wide_distinct GROUP BY g" ++ having;
            // Every payload is distinct, so the sets alone need more than the
            // smallest budget and far less than the largest.
            var rejected = false;
            var groups: usize = 0;
            if (runSql(allocator, db, sql)) |value| {
                var q = value;
                defer q.deinit();
                while (true) {
                    const batch = (q.next() catch |err| {
                        try std.testing.expectEqual(error.MemoryBudgetExceeded, err);
                        rejected = true;
                        break;
                    }) orelse break;
                    for (0..batch.row_count) |r| {
                        const g: usize = @intCast(batch.values[0].data.int[r]);
                        const want: i64 = @intCast((rows.len - g + group_count - 1) / group_count);
                        try std.testing.expectEqual(@as(?f64, @floatFromInt(want)), try cellNumber(batch.values[1], r));
                        groups += 1;
                    }
                }
            } else |err| {
                try std.testing.expectEqual(error.MemoryBudgetExceeded, err);
                rejected = true;
            }
            if (!rejected) try std.testing.expectEqual(@as(usize, group_count), groups);
            if (bi == 0) try std.testing.expect(rejected);
            if (bi == budgets.len - 1) try std.testing.expect(!rejected);
            // A finished silo query tears its workspace down asynchronously,
            // so only a rejected query's reservations must be gone already.
            if (rejected) try std.testing.expectEqual(@as(usize, 0), db.config.memory_pool.?.inUse());
        }
    }
}

// Issue #375: a HAVING Filter and a post-aggregate Compute, in whichever order
// the parser and the predicate pushdown leave them, keep the V2 group routes.
const HvRow = struct { id: i64, g: i32, gn: ?i32, u: i64, w: i32, x: ?i32 };
const HvStats = struct { key: ?i64, n: i64, du: i64, sw: i64, sx: ?i64 };
const HvOut = [3]?i64;
const HvCase = struct {
    sql: []const u8,
    // Whether the key takes the lowcard route.
    lowcard: bool = true,
    want: *const fn (HvStats) ?HvOut,
    order_col: ?usize = null,
    desc: bool = false,
    limit: ?usize = null,
    offset: usize = 0,
};

fn hvStats(allocator: std.mem.Allocator, rows: []const HvRow, comptime key: []const u8) ![]HvStats {
    var out: std.ArrayList(HvStats) = .empty;
    errdefer out.deinit(allocator);
    var masks: std.ArrayList(u32) = .empty;
    defer masks.deinit(allocator);
    for (rows) |r| {
        const k: ?i64 = if (comptime std.mem.eql(u8, key, "g")) r.g else if (r.gn) |v| v else null;
        const i = for (out.items, 0..) |s, i| {
            if (std.meta.eql(s.key, k)) break i;
        } else blk: {
            try out.append(allocator, .{ .key = k, .n = 0, .du = 0, .sw = 0, .sx = null });
            try masks.append(allocator, 0);
            break :blk out.items.len - 1;
        };
        const s = &out.items[i];
        s.n += 1;
        s.sw += r.w;
        if (r.x) |x| s.sx = (s.sx orelse 0) + x;
        masks.items[i] |= @as(u32, 1) << @intCast(r.u);
    }
    for (out.items, masks.items) |*s, m| s.du = @popCount(m);
    return out.toOwnedSlice(allocator);
}

fn hvLess(order_col: ?usize, desc: bool, a: HvOut, b: HvOut) bool {
    const cols: []const usize = if (order_col) |c| &.{c} else &.{ 0, 1, 2 };
    for (cols) |c| {
        const x = a[c] orelse std.math.minInt(i64);
        const y = b[c] orelse std.math.minInt(i64);
        if (x != y) return if (desc) x > y else x < y;
    }
    return false;
}

fn sortHv(rows: []HvOut, order_col: ?usize, desc: bool) void {
    const Ctx = struct { col: ?usize, desc: bool };
    std.mem.sort(HvOut, rows, Ctx{ .col = order_col, .desc = desc }, struct {
        fn lessThan(ctx: Ctx, a: HvOut, b: HvOut) bool {
            return hvLess(ctx.col, ctx.desc, a, b);
        }
    }.lessThan);
}

fn checkHvCase(allocator: std.mem.Allocator, db: *thindb.Database, stats: []const HvStats, c: HvCase, route: []const u8) !void {
    errdefer std.debug.print("route={s} query: {s}\n", .{ route, c.sql });
    try std.testing.expect(try planContains(allocator, db, c.sql, route));

    var want: std.ArrayList(HvOut) = .empty;
    defer want.deinit(allocator);
    for (stats) |s| if (c.want(s)) |row| try want.append(allocator, row);
    sortHv(want.items, c.order_col, c.desc);
    const start = @min(c.offset, want.items.len);
    const end = if (c.limit) |l| @min(start + l, want.items.len) else want.items.len;

    var got: std.ArrayList(HvOut) = .empty;
    defer got.deinit(allocator);
    var q = try runSql(allocator, db, c.sql);
    defer q.deinit();
    while (try q.next()) |batch| {
        for (0..batch.row_count) |r| {
            var row: HvOut = .{ null, null, null };
            for (batch.values, 0..) |col, ci| {
                if (try cellNumber(col, r)) |v| row[ci] = @intFromFloat(v);
            }
            try got.append(allocator, row);
        }
    }
    const order_col = c.order_col orelse {
        sortHv(got.items, null, false);
        return std.testing.expectEqualSlices(HvOut, want.items[start..end], got.items);
    };
    // Groups tied on the ORDER BY value may come back in any order, so a
    // page cut through a tie can hold any of the tied groups.
    try std.testing.expectEqual(end - start, got.items.len);
    for (got.items, want.items[start..end], 0..) |g, w, i| {
        try std.testing.expectEqual(w[order_col], g[order_col]);
        for (want.items) |cand| {
            if (std.meta.eql(cand, g)) break;
        } else return error.TestUnexpectedResult;
        for (got.items[0..i]) |prev| try std.testing.expect(!std.meta.eql(prev, g));
    }
}

test "V2 group routes keep HAVING with post-aggregate expressions in any order (issue #375)" {
    const allocator = std.testing.allocator;
    // Group g has 3g rows and g distinct u values; gn nulls every fourth row
    // of each group into one NULL group; x is NULL throughout group 2.
    var rows: std.ArrayList(HvRow) = .empty;
    defer rows.deinit(allocator);
    for (1..6) |g| {
        for (0..3 * g) |j| {
            try rows.append(allocator, .{
                .id = @intCast(rows.items.len),
                .g = @intCast(g),
                .gn = if (j % 4 == 3) null else @intCast(g),
                .u = @intCast(j % g),
                .w = @intCast(10 * g + j),
                .x = if (g == 2) null else @intCast(j),
            });
        }
    }

    const Fns = struct {
        fn distinctPlusCount(s: HvStats) ?HvOut {
            return if (s.n > 4) .{ s.key, s.du * 10 + s.n, null } else null;
        }
        fn sumPlusCount(s: HvStats) ?HvOut {
            return if (s.n > 4) .{ s.key, s.sw * 10 + s.n, null } else null;
        }
        fn aliasFilter(s: HvStats) ?HvOut {
            const e = s.sw * 10 + s.n;
            return if (e > 3000) .{ s.key, e, null } else null;
        }
        fn exprFilter(s: HvStats) ?HvOut {
            return if (s.sw * 2 > 600) .{ s.key, s.n, null } else null;
        }
        fn splitFilter(s: HvStats) ?HvOut {
            const e = s.sw * 10 + s.n;
            return if (s.n > 2 and e > 1000) .{ s.key, e, s.n } else null;
        }
        fn orFilter(s: HvStats) ?HvOut {
            const e = s.sw * 10 + s.n;
            return if (e > 3000 or s.n < 3) .{ s.key, e, null } else null;
        }
        fn derivedKey(s: HvStats) ?HvOut {
            return if (s.n > 4) .{ if (s.key) |k| k + 1 else null, s.du + s.sw, null } else null;
        }
        fn nullSum(s: HvStats) ?HvOut {
            return .{ s.key, if (s.sx) |x| x + 1 else null, null };
        }
        fn nullSumFilter(s: HvStats) ?HvOut {
            const x = s.sx orelse return null;
            return if (x + 1 > 0) .{ s.key, x + 1, null } else null;
        }
        fn none(_: HvStats) ?HvOut {
            return null;
        }
    };

    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        errdefer std.debug.print("dop={d}\n", .{dop});
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .auto_flush_secs = 0 });
        defer db.close();
        const t = try db.table("hv", .{
            .columns = &.{
                .{ .name = "id", .type = .bigint },
                .{ .name = "g", .type = .int },
                .{ .name = "gn", .type = .int, .nullable = true },
                .{ .name = "u", .type = .bigint },
                .{ .name = "w", .type = .int },
                .{ .name = "x", .type = .int, .nullable = true },
            },
            .order_key = &.{"id"},
            .unique = true,
        }, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 16 });
        try t.insert(rows.items);
        try t.flush();

        inline for (.{ "g", "gn" }) |k| {
            const stats = try hvStats(allocator, rows.items, k);
            defer allocator.free(stats);
            const cases = [_]HvCase{
                .{ .sql = "SELECT " ++ k ++ ", COUNT(DISTINCT u)*10 + COUNT(*) AS e FROM hv GROUP BY " ++ k ++ " HAVING COUNT(*) > 4", .want = Fns.distinctPlusCount },
                .{ .sql = "SELECT " ++ k ++ ", SUM(w)*10 + COUNT(*) AS e FROM hv GROUP BY " ++ k ++ " HAVING COUNT(*) > 4", .want = Fns.sumPlusCount },
                .{ .sql = "SELECT " ++ k ++ ", SUM(w)*10 + COUNT(*) AS e FROM hv GROUP BY " ++ k ++ " HAVING e > 3000", .want = Fns.aliasFilter },
                .{ .sql = "SELECT " ++ k ++ ", COUNT(*) AS c FROM hv GROUP BY " ++ k ++ " HAVING SUM(w)*2 > 600", .want = Fns.exprFilter },
                .{ .sql = "SELECT " ++ k ++ ", SUM(w)*10 + COUNT(*) AS e, COUNT(*) AS c FROM hv GROUP BY " ++ k ++ " HAVING c > 2 AND e > 1000 ORDER BY e DESC LIMIT 2", .want = Fns.splitFilter, .order_col = 1, .desc = true, .limit = 2 },
                .{ .sql = "SELECT " ++ k ++ ", SUM(w)*10 + COUNT(*) AS e FROM hv GROUP BY " ++ k ++ " HAVING e > 3000 OR COUNT(*) < 3 ORDER BY e LIMIT 2 OFFSET 1", .want = Fns.orFilter, .order_col = 1, .limit = 2, .offset = 1 },
                .{ .sql = "SELECT " ++ k ++ " + 1 AS k1, COUNT(DISTINCT u) + SUM(w) AS e FROM hv GROUP BY k1 HAVING COUNT(*) > 4", .want = Fns.derivedKey, .lowcard = false },
                .{ .sql = "SELECT " ++ k ++ ", SUM(x) + 1 AS s FROM hv GROUP BY " ++ k ++ " HAVING COUNT(*) > 0", .want = Fns.nullSum, .lowcard = false },
                .{ .sql = "SELECT " ++ k ++ ", SUM(x) + 1 AS s FROM hv GROUP BY " ++ k ++ " HAVING s > 0", .want = Fns.nullSumFilter, .lowcard = false },
                .{ .sql = "SELECT " ++ k ++ ", SUM(w)*10 + COUNT(*) AS e FROM hv GROUP BY " ++ k ++ " HAVING COUNT(*) > 1000", .want = Fns.none },
                .{ .sql = "SELECT " ++ k ++ ", SUM(w)*10 + COUNT(*) AS e FROM hv WHERE w < 0 GROUP BY " ++ k ++ " HAVING COUNT(*) > 0", .want = Fns.none },
            };
            for (cases) |c| {
                const route = if (c.lowcard) "lowcard" else "V2 group-topN";
                try checkHvCase(allocator, db, stats, c, route);
            }
        }

        // The global aggregate reads its post-aggregate chain the same way.
        var sum_w: i64 = 0;
        for (rows.items) |r| sum_w += r.w;
        const global_cases = .{
            .{ "SELECT SUM(w)*2 AS s FROM hv HAVING s > 0", @as(?i64, sum_w * 2) },
            .{ "SELECT SUM(w)*2 AS s FROM hv HAVING s < 0", @as(?i64, null) },
        };
        inline for (global_cases) |gc| {
            errdefer std.debug.print("query: {s}\n", .{gc[0]});
            try std.testing.expect(try planContains(allocator, db, gc[0], "V2 global"));
            var q = try runSql(allocator, db, gc[0]);
            defer q.deinit();
            var got: ?i64 = null;
            var n: usize = 0;
            while (try q.next()) |batch| {
                for (0..batch.row_count) |r| {
                    got = if (try cellNumber(batch.values[0], r)) |v| @intFromFloat(v) else null;
                    n += 1;
                }
            }
            try std.testing.expectEqual(@as(usize, if (gc[1] == null) 0 else 1), n);
            try std.testing.expectEqual(gc[1], got);
        }
    }
}

const LC_GROUPS = 37;

/// Peak charged bytes of the lowcard aggregate over scan batches of
/// `batch_rows`, after checking its per-group COUNT, SUM, MIN and MAX.
fn lowcardPeak(allocator: std.mem.Allocator, batch_rows: usize) !usize {
    const rows = 100_000;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{
        .memory_budget = 1 << 30,
        .auto_flush_secs = 0,
        .max_dop = 1,
    });
    defer db.close();
    const t = try db.table("lc", .{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "g", .type = .int },
            .{ .name = "v", .type = .bigint },
        },
        .order_key = &.{"id"},
        .unique = false,
    }, .{ .order_key = &.{"id"} });
    const Row = struct { id: i64, g: i32, v: i64 };
    const batch = try allocator.alloc(Row, rows);
    defer allocator.free(batch);
    for (batch, 0..) |*row, i| row.* = .{ .id = @intCast(i), .g = @intCast((i * 7) % LC_GROUPS), .v = @intCast(i) };
    try t.insert(batch);
    try t.flush();

    const saved = thindb.exec.scan_sub_batch;
    defer thindb.exec.scan_sub_batch = saved;
    thindb.exec.scan_sub_batch = batch_rows;
    const sql = "SELECT g, COUNT(*) AS n, SUM(v) AS sv, MIN(v) AS mn, MAX(v) AS mx FROM lc GROUP BY g";
    try std.testing.expect(try planContains(allocator, db, sql, "lowcard"));
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var seen = [_]bool{false} ** LC_GROUPS;
    while (try q.next()) |out| {
        for (0..out.row_count) |r| {
            const g: usize = @intCast(out.values[0].data.int[r]);
            try std.testing.expect(!seen[g]);
            seen[g] = true;
            var n: i64 = 0;
            var sum: i64 = 0;
            var min: ?i64 = null;
            var max: i64 = 0;
            for (batch) |row| {
                if (row.g != g) continue;
                n += 1;
                sum += row.v;
                if (min == null) min = row.v;
                max = row.v;
            }
            for ([_]i64{ n, sum, min.?, max }, 1..) |want, ci| {
                try std.testing.expectEqual(@as(?f64, @floatFromInt(want)), try cellNumber(out.values[ci], r));
            }
        }
    }
    for (seen) |s| try std.testing.expect(s);
    return q.cq.ctx.accountant.?.peak_bytes;
}

test "V2 lowcard aggregate sizes its state by its groups, not its batch rows" {
    const allocator = std.testing.allocator;
    const small = try lowcardPeak(allocator, 4096);
    const large = try lowcardPeak(allocator, 16384);
    // The batch's own key and gid scratch costs about 18 bytes a row; a group
    // table and accumulator arrays reserved for a whole batch of new groups
    // added about 190 more.
    try std.testing.expect(large -| small < 64 * (16384 - 4096));
}

// Nullable integer group keys on the lowcard route. `b` holds 0 next to its
// NULLs, and each 64-row row group draws `b` from its own range, so the
// encoding artifact in a NULL slot differs between row groups; `a` beside `b`
// packs 65 bits and so does the nullable BIGINT `c` alone.
const NkRow = struct { id: i64, a: i32, b: ?i32, c: ?i64, w: i32 };
const NkOut = [4]?i64;

fn nkRow(i: usize) NkRow {
    return .{
        .id = @intCast(i),
        .a = @intCast(i % 5),
        .b = if (i % 7 == 0) null else if (i % 5 == 0) 0 else @intCast(1000 * (i / 64 % 4) + i % 3),
        .c = if (i % 11 == 0) null else @as(i64, @intCast(i % 4)) - 1,
        .w = @intCast(i % 13),
    };
}

fn nkKey(r: NkRow, k: u8) ?i64 {
    return switch (k) {
        'a' => r.a,
        'b' => if (r.b) |v| v else null,
        else => r.c,
    };
}

fn nkLess(desc: bool, x: NkOut, y: NkOut) bool {
    for (x, y) |xv, yv| {
        if (std.meta.eql(xv, yv)) continue;
        const lt = if (xv) |xx| (if (yv) |yy| xx < yy else false) else true;
        return lt != desc;
    }
    return false;
}

// Per group of `keys`: COUNT(*) and SUM(w), or COUNT(DISTINCT w) and COUNT(*).
fn nkExpected(allocator: std.mem.Allocator, rows: []const NkRow, keys: []const u8, distinct: bool) ![]NkOut {
    var out: std.ArrayList(NkOut) = .empty;
    errdefer out.deinit(allocator);
    var masks: std.ArrayList(u64) = .empty;
    defer masks.deinit(allocator);
    for (rows) |r| {
        var key: NkOut = .{ null, null, null, null };
        for (keys, 0..) |k, ki| key[ki] = nkKey(r, k);
        const gi = for (out.items, 0..) |o, gi| {
            if (std.mem.eql(?i64, o[0..keys.len], key[0..keys.len])) break gi;
        } else blk: {
            key[keys.len] = 0;
            key[keys.len + 1] = 0;
            try out.append(allocator, key);
            try masks.append(allocator, 0);
            break :blk out.items.len - 1;
        };
        const o = &out.items[gi];
        if (distinct) {
            masks.items[gi] |= @as(u64, 1) << @intCast(r.w);
            o[keys.len] = @popCount(masks.items[gi]);
            o[keys.len + 1] = o[keys.len + 1].? + 1;
        } else {
            o[keys.len] = o[keys.len].? + 1;
            o[keys.len + 1] = o[keys.len + 1].? + r.w;
        }
    }
    return out.toOwnedSlice(allocator);
}

fn checkNk(allocator: std.mem.Allocator, db: *thindb.Database, rows: []const NkRow, sql: []const u8, keys: []const u8, distinct: bool, route: []const u8) !void {
    errdefer std.debug.print("route={s} query: {s}\n", .{ route, sql });
    try std.testing.expect(try planContains(allocator, db, sql, route));
    const want = try nkExpected(allocator, rows, keys, distinct);
    defer allocator.free(want);
    var got: std.ArrayList(NkOut) = .empty;
    defer got.deinit(allocator);
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    while (try q.next()) |batch| {
        for (0..batch.row_count) |r| {
            var row: NkOut = .{ null, null, null, null };
            for (batch.values, 0..) |col, ci| {
                if (try cellNumber(col, r)) |v| row[ci] = @intFromFloat(v);
            }
            try got.append(allocator, row);
        }
    }
    std.mem.sort(NkOut, want, false, nkLess);
    std.mem.sort(NkOut, got.items, false, nkLess);
    try std.testing.expectEqualSlices(NkOut, want, got.items);
}

// `b`'s groups by count, ranked by `b` itself: NULL first ascending, last
// descending.
fn checkNkOrder(allocator: std.mem.Allocator, db: *thindb.Database, rows: []const NkRow, comptime desc: bool) !void {
    const sql = "SELECT b, COUNT(*) AS n FROM nk GROUP BY b ORDER BY b" ++ (if (desc) " DESC" else "") ++ " LIMIT 5";
    errdefer std.debug.print("query: {s}\n", .{sql});
    try std.testing.expect(try planContains(allocator, db, sql, "lowcard"));
    const all = try nkExpected(allocator, rows, "b", false);
    defer allocator.free(all);
    for (all) |*o| o[2] = null;
    std.mem.sort(NkOut, all, desc, nkLess);
    var got: std.ArrayList(NkOut) = .empty;
    defer got.deinit(allocator);
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    while (try q.next()) |batch| {
        for (0..batch.row_count) |r| {
            var row: NkOut = .{ null, null, null, null };
            for (batch.values, 0..) |col, ci| {
                if (try cellNumber(col, r)) |v| row[ci] = @intFromFloat(v);
            }
            try got.append(allocator, row);
        }
    }
    try std.testing.expectEqualSlices(NkOut, all[0..@min(5, all.len)], got.items);
}

test "V2 lowcard groups nullable integer keys: NULL apart from 0, one NULL group across row groups, keys past 64 bits" {
    const allocator = std.testing.allocator;
    var rows: [1636]NkRow = undefined;
    for (&rows, 0..) |*r, i| r.* = nkRow(i);
    const flushed = 1536;

    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        errdefer std.debug.print("dop={d}\n", .{dop});
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .auto_flush_secs = 0 });
        defer db.close();
        const t = try db.table("nk", .{
            .columns = &.{
                .{ .name = "id", .type = .bigint },
                .{ .name = "a", .type = .int },
                .{ .name = "b", .type = .int, .nullable = true },
                .{ .name = "c", .type = .bigint, .nullable = true },
                .{ .name = "w", .type = .int },
            },
            .order_key = &.{"id"},
            .unique = true,
        }, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 64 });
        try t.insert(rows[0..flushed]);
        try t.flush();

        // The second pass adds memtable rows, whose batches arrive unencoded.
        for ([_]usize{ flushed, rows.len }) |live| {
            errdefer std.debug.print("rows={d}\n", .{live});
            if (live == rows.len) try t.insert(rows[flushed..]);
            const r = rows[0..live];
            var kept: std.ArrayList(NkRow) = .empty;
            defer kept.deinit(allocator);
            for (r) |row| if (row.w > 2) try kept.append(allocator, row);
            try checkNk(allocator, db, r, "SELECT a, b, COUNT(*) AS n, SUM(w) AS s FROM nk GROUP BY a, b", "ab", false, "lowcard");
            try checkNk(allocator, db, r, "SELECT b, COUNT(*) AS n, SUM(w) AS s FROM nk GROUP BY b", "b", false, "lowcard");
            try checkNk(allocator, db, r, "SELECT c, COUNT(*) AS n, SUM(w) AS s FROM nk GROUP BY c", "c", false, "lowcard");
            try checkNk(allocator, db, kept.items, "SELECT a, b, COUNT(*) AS n, SUM(w) AS s FROM nk WHERE w > 2 GROUP BY a, b", "ab", false, "lowcard");
            try checkNk(allocator, db, r, "SELECT b, COUNT(DISTINCT w) AS d, COUNT(*) AS n FROM nk GROUP BY b", "b", true, "lowcard");
            try checkNk(allocator, db, r, "SELECT a, b, COUNT(DISTINCT w) AS d, COUNT(*) AS n FROM nk GROUP BY a, b", "ab", true, "V2 group-topN");
            try checkNkOrder(allocator, db, r, false);
            try checkNkOrder(allocator, db, r, true);
        }
    }
}

// Single-key lowcard groups through the direct gid map. INT and BIGINT keys
// run negative, so they sit below the masked base; a DELETE empties the
// range's lowest key while the segment stats still cover it; memtable rows,
// with keys past the flushed range, null the int range (no map) and take the
// string key, with values the flushed dictionary never saw, off lowcard.
const DkRow = struct { id: i64, k: i32, kb: i64, s: []const u8, w: i32 };
const DK_STRINGS = [_][]const u8{ "", "east", "north", "south", "west", "a-key-longer-than-sixteen-bytes", "late-1", "late-2", "late-3" };
const DK_FLUSHED_STRINGS = 6;

fn dkRow(i: usize, flushed: usize) DkRow {
    const k: i32 = @as(i32, @intCast(i % 9)) - 4;
    return .{
        .id = @intCast(i),
        .k = if (i < flushed) k else k * 3,
        .kb = @as(i64, k) - 3_000_000_000,
        .s = DK_STRINGS[if (i < flushed) i % DK_FLUSHED_STRINGS else i % DK_STRINGS.len],
        .w = @intCast(i % 13),
    };
}

fn dkStringIndex(s: []const u8) ?usize {
    return for (DK_STRINGS, 0..) |d, i| {
        if (std.mem.eql(u8, d, s)) break i;
    } else null;
}

fn dkKey(r: DkRow, comptime key: []const u8) i64 {
    if (comptime std.mem.eql(u8, key, "k")) return r.k;
    if (comptime std.mem.eql(u8, key, "kb")) return r.kb;
    return @intCast(dkStringIndex(r.s).?);
}

fn checkDk(allocator: std.mem.Allocator, db: *thindb.Database, rows: []const DkRow, live: []const bool, comptime key: []const u8, route: []const u8, map: bool) !void {
    const sql = "SELECT " ++ key ++ ", COUNT(*) AS n, SUM(w) AS sw, MAX(w) AS mw FROM dk GROUP BY " ++ key;
    errdefer std.debug.print("route={s} query: {s}\n", .{ route, sql });
    try std.testing.expect(try planContains(allocator, db, sql, route));
    try std.testing.expectEqual(map, try planContains(allocator, db, sql, "gid map"));
    var want: std.AutoHashMapUnmanaged(i64, [3]i64) = .empty;
    defer want.deinit(allocator);
    for (rows, live) |r, keep| {
        if (!keep) continue;
        const e = try want.getOrPut(allocator, dkKey(r, key));
        if (!e.found_existing) e.value_ptr.* = .{ 0, 0, 0 };
        e.value_ptr[0] += 1;
        e.value_ptr[1] += r.w;
        e.value_ptr[2] = @max(e.value_ptr[2], r.w);
    }
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    while (try q.next()) |batch| {
        for (0..batch.row_count) |r| {
            const k: i64 = if (comptime std.mem.eql(u8, key, "s"))
                @intCast(dkStringIndex(batch.values[0].data.string.rowBytes(r)) orelse return error.TestUnexpectedResult)
            else
                @intFromFloat((try cellNumber(batch.values[0], r)) orelse return error.TestUnexpectedResult);
            errdefer std.debug.print("group {d}\n", .{k});
            const e = want.fetchRemove(k) orelse return error.TestUnexpectedResult;
            for (e.value, 1..) |v, ci| try std.testing.expectEqual(@as(?f64, @floatFromInt(v)), try cellNumber(batch.values[ci], r));
        }
    }
    try std.testing.expectEqual(@as(u32, 0), want.count());
}

test "V2 lowcard direct gid map: negative keys, emptied keys and memtable keys past the flushed range" {
    const allocator = std.testing.allocator;
    const flushed = 3000;
    var rows: [3400]DkRow = undefined;
    for (&rows, 0..) |*r, i| r.* = dkRow(i, flushed);
    var live: [rows.len]bool = undefined;

    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        errdefer std.debug.print("dop={d}\n", .{dop});
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .auto_flush_secs = 0 });
        defer db.close();
        const t = try db.table("dk", .{
            .columns = &.{
                .{ .name = "id", .type = .bigint },
                .{ .name = "k", .type = .int },
                .{ .name = "kb", .type = .bigint },
                .{ .name = "s", .type = .string },
                .{ .name = "w", .type = .int },
            },
            .order_key = &.{"id"},
            .unique = true,
        }, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 256 });
        try t.insert(rows[0..flushed]);
        try t.flush();
        for (&live, 0..) |*keep, i| keep.* = i < flushed;
        try checkDk(allocator, db, &rows, &live, "k", "lowcard", true);
        try checkDk(allocator, db, &rows, &live, "kb", "lowcard", true);
        try checkDk(allocator, db, &rows, &live, "s", "lowcard", true);

        try exec(allocator, db, "DELETE FROM dk WHERE id % 9 = 0 OR id % 7 = 3");
        for (&live, 0..) |*keep, i| keep.* = i < flushed and i % 9 != 0 and i % 7 != 3;
        try checkDk(allocator, db, &rows, &live, "k", "lowcard", true);
        try checkDk(allocator, db, &rows, &live, "kb", "lowcard", true);
        try checkDk(allocator, db, &rows, &live, "s", "lowcard", true);

        try t.insert(rows[flushed..]);
        for (&live, 0..) |*keep, i| keep.* = i >= flushed or (i % 9 != 0 and i % 7 != 3);
        try checkDk(allocator, db, &rows, &live, "k", "lowcard", false);
        try checkDk(allocator, db, &rows, &live, "kb", "lowcard", false);
        try checkDk(allocator, db, &rows, &live, "s", "V2 group-topN", false);
    }
}

const OL_GROUPS = 23;
const OlRow = struct { id: i64, g: i32, u: []const u8, v: []const u8, w: ?[]const u8 };
// One query's per-row contribution `[a, b, 1]` to its group, null when the
// WHERE drops the row; `route` is the plan's group operator, if pinned.
const OlCase = struct { sql: []const u8, route: ?[]const u8, row: *const fn (OlRow) ?[3]i64 };

fn olLen(s: []const u8) i64 {
    return @intCast(s.len);
}

// `octet_length` over a column read only through it takes the scan's
// byte-length sidecar on the lowcard, group-topN and global aggregate routes;
// every other read of the column (a conditional argument, a CASE, another
// function, a nullable column) keeps the bytes. All of them must agree with
// the stored strings across flushed row groups, tombstones and memtable rows.
test "V2 octet_length reads string lengths without the bytes" {
    const allocator = std.testing.allocator;
    const n_rows = 6000;
    const text = "https://www.example.org/some/longer/path/with-a-tail/" ** 4;
    const rows = try allocator.alloc(OlRow, n_rows);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| r.* = .{
        .id = @intCast(i),
        .g = @intCast((i * 7) % OL_GROUPS),
        .u = text[0..if (i % 9 == 0) 0 else (i * 31) % 200],
        .v = text[0 .. (i * 13) % 50],
        .w = if (i % 7 == 0) null else text[0 .. (i * 3) % 120],
    };
    const Fns = struct {
        fn filtered(r: OlRow) ?[3]i64 {
            if (r.u.len == 0) return null;
            return .{ olLen(r.u), olLen(r.v), 1 };
        }
        fn nested(r: OlRow) ?[3]i64 {
            return .{ olLen(r.u) * 2 + 1, olLen(r.v), 1 };
        }
        fn conditional(r: OlRow) ?[3]i64 {
            if (@mod(r.id, 5) == 1) return null;
            return .{ if (@mod(r.id, 2) == 0) olLen(r.u) else 0, if (@mod(r.id, 3) == 0) olLen(r.v) else 0, 1 };
        }
        fn otherRead(r: OlRow) ?[3]i64 {
            if (r.id <= 7) return null;
            return .{ olLen(r.u), olLen(r.u), 1 };
        }
        fn nullable(r: OlRow) ?[3]i64 {
            if (r.id <= 3) return null;
            return .{ if (r.w) |w| olLen(w) else 0, 0, 1 };
        }
    };
    const cases = [_]OlCase{
        .{ .sql = "SELECT g, SUM(octet_length(u)) AS a, SUM(octet_length(v)) AS b, COUNT(*) AS n FROM ol WHERE u <> '' GROUP BY g", .route = "lowcard", .row = Fns.filtered },
        .{ .sql = "SELECT g, SUM(octet_length(u) * 2 + 1) AS a, SUM(octet_length(v)) AS b, COUNT(*) AS n FROM ol GROUP BY g", .route = "lowcard", .row = Fns.nested },
        .{ .sql = "SELECT g, SUM(IF(id % 2 = 0, octet_length(u), 0)) AS a, SUM(CASE WHEN id % 3 = 0 THEN octet_length(v) ELSE 0 END) AS b, COUNT(*) AS n FROM ol WHERE id % 5 <> 1 GROUP BY g", .route = null, .row = Fns.conditional },
        .{ .sql = "SELECT g, SUM(octet_length(u)) AS a, SUM(octet_length(upper(u))) AS b, COUNT(*) AS n FROM ol WHERE id > 7 GROUP BY g", .route = "lowcard", .row = Fns.otherRead },
        .{ .sql = "SELECT g, SUM(octet_length(w)) AS a, SUM(0) AS b, COUNT(*) AS n FROM ol WHERE id > 3 GROUP BY g", .route = null, .row = Fns.nullable },
        // A derived key leaves lowcard for the group-topN grid.
        .{ .sql = "SELECT g + 0 AS k, SUM(octet_length(u)) AS a, SUM(octet_length(v)) AS b, COUNT(*) AS n FROM ol WHERE u <> '' GROUP BY k", .route = "V2 group-topN", .row = Fns.filtered },
        .{ .sql = "SELECT g + 0 AS k, SUM(octet_length(u) * 2 + 1) AS a, SUM(octet_length(v)) AS b, COUNT(*) AS n FROM ol GROUP BY k HAVING COUNT(*) > 0", .route = "V2 group-topN", .row = Fns.nested },
        .{ .sql = "SELECT g + 0 AS k, SUM(IF(id % 2 = 0, octet_length(u), 0)) AS a, SUM(CASE WHEN id % 3 = 0 THEN octet_length(v) ELSE 0 END) AS b, COUNT(*) AS n FROM ol WHERE id % 5 <> 1 GROUP BY k", .route = null, .row = Fns.conditional },
    };
    const totals = [_]OlCase{
        .{ .sql = "SELECT SUM(octet_length(u)) AS a, SUM(octet_length(v)) AS b, COUNT(*) AS n FROM ol WHERE u <> ''", .route = null, .row = Fns.filtered },
        .{ .sql = "SELECT SUM(octet_length(u) * 2 + 1) AS a, SUM(octet_length(v)) AS b, COUNT(*) AS n FROM ol", .route = null, .row = Fns.nested },
        .{ .sql = "SELECT SUM(octet_length(u)) AS a, SUM(octet_length(upper(u))) AS b, COUNT(*) AS n FROM ol WHERE id > 7", .route = null, .row = Fns.otherRead },
    };

    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        errdefer std.debug.print("dop={d}\n", .{dop});
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .auto_flush_secs = 0 });
        defer db.close();
        const t = try db.table("ol", .{
            .columns = &.{
                .{ .name = "id", .type = .bigint },
                .{ .name = "g", .type = .int },
                .{ .name = "u", .type = .string },
                .{ .name = "v", .type = .{ .varchar = 64 } },
                .{ .name = "w", .type = .string, .nullable = true },
            },
            .order_key = &.{"id"},
            .unique = true,
        }, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 256 });

        const live = try allocator.alloc(bool, n_rows);
        defer allocator.free(live);
        try t.insert(rows[0..2500]);
        try t.flush();
        try t.insert(rows[2500..5000]);
        try t.flush();
        try exec(allocator, db, "DELETE FROM ol WHERE id % 11 = 0 AND id < 1200");
        try t.insert(rows[5000..]);
        for (live, 0..) |*keep, i| keep.* = i % 11 != 0 or i >= 1200;

        for (cases) |c| {
            errdefer std.debug.print("query: {s}\n", .{c.sql});
            if (c.route) |route| try std.testing.expect(try planContains(allocator, db, c.sql, route));
            var want = [_][3]i64{.{ 0, 0, 0 }} ** OL_GROUPS;
            for (rows, live) |r, keep| {
                if (!keep) continue;
                const add = c.row(r) orelse continue;
                for (&want[@intCast(r.g)], add) |*w, a| w.* += a;
            }
            var groups: usize = 0;
            var q = try runSql(allocator, db, c.sql);
            defer q.deinit();
            while (try q.next()) |batch| {
                for (0..batch.row_count) |r| {
                    const g: usize = @intFromFloat((try cellNumber(batch.values[0], r)).?);
                    groups += 1;
                    for (want[g], 1..) |w, ci| {
                        errdefer std.debug.print("group {d} column {d}\n", .{ g, ci });
                        try std.testing.expectEqual(@as(?f64, @floatFromInt(w)), try cellNumber(batch.values[ci], r));
                    }
                }
            }
            try std.testing.expectEqual(@as(usize, OL_GROUPS), groups);
        }
        for (totals) |c| {
            errdefer std.debug.print("query: {s}\n", .{c.sql});
            var want = [3]i64{ 0, 0, 0 };
            for (rows, live) |r, keep| {
                if (!keep) continue;
                const add = c.row(r) orelse continue;
                for (&want, add) |*w, a| w.* += a;
            }
            var q = try runSql(allocator, db, c.sql);
            defer q.deinit();
            const batch = (try q.next()).?;
            try std.testing.expectEqual(@as(usize, 1), batch.row_count);
            for (want, 0..) |w, ci| try std.testing.expectEqual(@as(?f64, @floatFromInt(w)), try cellNumber(batch.values[ci], 0));
        }
    }
}

test "V2 COUNT top-K stays exact when the count sketch drops the small groups" {
    const allocator = std.testing.allocator;
    const heavy_groups = 12;
    // Mostly one-row groups, short of the share at which the stats call a
    // key unique and skip the sketch.
    const unique_rows = 245_000;
    const heavy_rows = comptime blk: {
        var n: usize = 0;
        for (0..heavy_groups) |g| n += 6000 - 250 * g;
        break :blk n;
    };
    const total = heavy_rows + unique_rows;
    const Row = struct { id: i64, k: i64, s: []const u8, v: i32 };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();
    const t = try db.table("skewed", .{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "k", .type = .bigint },
            .{ .name = "s", .type = .string },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    }, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 1024 });

    // Heavy group g holds 6000 - 250g rows; every other row is its own group.
    // Ids scatter the heavy rows over the row groups.
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const rows = try allocator.alloc(Row, total);
    defer allocator.free(rows);
    var counts = [_]i64{0} ** heavy_groups;
    var kept_counts = [_]i64{0} ** heavy_groups;
    var sums = [_]i64{0} ** heavy_groups;
    var i: usize = 0;
    for (0..heavy_groups) |g| {
        for (0..6000 - 250 * g) |_| {
            const v: i32 = @intCast(i % 7);
            rows[i] = .{ .id = 0, .k = @intCast(g), .s = try std.fmt.allocPrint(arena.allocator(), "key-{d}", .{g}), .v = v };
            counts[g] += 1;
            sums[g] += v;
            if (v != 3) kept_counts[g] += 1;
            i += 1;
        }
    }
    for (0..unique_rows) |j| {
        rows[i] = .{ .id = 0, .k = @intCast(1000 + j), .s = try std.fmt.allocPrint(arena.allocator(), "key-{d}", .{1000 + j}), .v = @intCast(i % 7) };
        i += 1;
    }
    for (rows, 0..) |*r, n| r.id = @intCast((n * 7919) % total);
    try t.insert(rows);
    try t.flush();

    {
        var q = try runSql(allocator, db, "SELECT k, COUNT(*) AS c, SUM(v) FROM skewed GROUP BY k ORDER BY c DESC LIMIT 10");
        defer q.deinit();
        var rank: usize = 0;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |r| {
                try std.testing.expectEqual(@as(?f64, @floatFromInt(rank)), try cellNumber(batch.values[0], r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(counts[rank])), try cellNumber(batch.values[1], r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(sums[rank])), try cellNumber(batch.values[2], r));
                rank += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 10), rank);
    }
    {
        var q = try runSql(allocator, db, "SELECT s, COUNT(*) AS c FROM skewed GROUP BY s ORDER BY c DESC LIMIT 10");
        defer q.deinit();
        var rank: usize = 0;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |r| {
                var buf: [16]u8 = undefined;
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "key-{d}", .{rank}), batch.values[0].data.string.rowBytes(r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(counts[rank])), try cellNumber(batch.values[1], r));
                rank += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 10), rank);
    }
    {
        var q = try runSql(allocator, db, "SELECT k, s, COUNT(*) AS c, SUM(v) FROM skewed GROUP BY k, s ORDER BY c DESC LIMIT 10");
        defer q.deinit();
        var rank: usize = 0;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |r| {
                var buf: [16]u8 = undefined;
                try std.testing.expectEqual(@as(?f64, @floatFromInt(rank)), try cellNumber(batch.values[0], r));
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "key-{d}", .{rank}), batch.values[1].data.string.rowBytes(r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(counts[rank])), try cellNumber(batch.values[2], r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(sums[rank])), try cellNumber(batch.values[3], r));
                rank += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 10), rank);
    }
    {
        // A derived key beside a hashed string key: the string's digests pass
        // through the derived columns' Compute.
        var q = try runSql(allocator, db, "SELECT s, k % 1000 AS m, COUNT(*) AS c FROM skewed GROUP BY s, m ORDER BY c DESC LIMIT 10");
        defer q.deinit();
        var rank: usize = 0;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |r| {
                var buf: [16]u8 = undefined;
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "key-{d}", .{rank}), batch.values[0].data.string.rowBytes(r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(rank)), try cellNumber(batch.values[1], r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(counts[rank])), try cellNumber(batch.values[2], r));
                rank += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 10), rank);
    }
    {
        var order: [heavy_groups]usize = undefined;
        for (&order, 0..) |*o, g| o.* = g;
        std.sort.pdq(usize, &order, &kept_counts, struct {
            fn gt(c: *const [heavy_groups]i64, a: usize, b: usize) bool {
                return c[a] > c[b];
            }
        }.gt);
        var q = try runSql(allocator, db, "SELECT k, COUNT(*) AS c FROM skewed WHERE v <> 3 GROUP BY k ORDER BY c DESC LIMIT 3 OFFSET 4");
        defer q.deinit();
        var rank: usize = 4;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |r| {
                try std.testing.expectEqual(@as(?f64, @floatFromInt(order[rank])), try cellNumber(batch.values[0], r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(kept_counts[order[rank]])), try cellNumber(batch.values[1], r));
                rank += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 7), rank);
    }
    {
        // Every group holds one row: the sketch declines and every row is grouped.
        var q = try runSql(allocator, db, "SELECT id, COUNT(*) AS c FROM skewed GROUP BY id ORDER BY c DESC LIMIT 5");
        defer q.deinit();
        var n: usize = 0;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |r| try std.testing.expectEqual(@as(?f64, 1), try cellNumber(batch.values[1], r));
            n += batch.row_count;
        }
        try std.testing.expectEqual(@as(usize, 5), n);
    }

    // The same rows in insertion order: each heavy group is one long run, so
    // its row groups reach the sketch run-encoded.
    const clustered = try db.table("clustered", .{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "k", .type = .bigint },
            .{ .name = "s", .type = .string },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    }, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 1024 });
    for (rows, 0..) |*r, n| r.id = @intCast(n);
    try clustered.insert(rows);
    try clustered.flush();
    {
        var q = try runSql(allocator, db, "SELECT k, COUNT(*) AS c FROM clustered GROUP BY k ORDER BY c DESC LIMIT 10");
        defer q.deinit();
        var rank: usize = 0;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |r| {
                try std.testing.expectEqual(@as(?f64, @floatFromInt(rank)), try cellNumber(batch.values[0], r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(counts[rank])), try cellNumber(batch.values[1], r));
                rank += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 10), rank);
    }

    // The filter pass sieves the row groups its tally saw whole by the
    // counters it recorded; deleted rows and memtable rows sit beside them.
    try exec(allocator, db, "DELETE FROM skewed WHERE id % 13 = 0");
    const extra_rows = 4000;
    const extra = try allocator.alloc(Row, extra_rows);
    defer allocator.free(extra);
    for (extra, 0..) |*r, j| r.* = .{ .id = @intCast(total + j), .k = 11, .s = "key-11", .v = 0 };
    try t.insert(extra);
    var live_counts = [_]i64{0} ** heavy_groups;
    for (rows, 0..) |r, n| {
        if (r.k >= heavy_groups or (n * 7919) % total % 13 == 0) continue;
        live_counts[@intCast(r.k)] += 1;
    }
    live_counts[11] += extra_rows;
    var best = live_counts;
    std.sort.pdq(i64, &best, {}, std.sort.desc(i64));
    inline for (.{
        "SELECT k, COUNT(*) AS c FROM skewed GROUP BY k ORDER BY c DESC LIMIT 10",
        "SELECT k % 1000 AS m, s, COUNT(*) AS c FROM skewed GROUP BY m, s ORDER BY c DESC LIMIT 10",
    }) |sql| {
        errdefer std.debug.print("query: {s}\n", .{sql});
        var q = try runSql(allocator, db, sql);
        defer q.deinit();
        const count_col = if (comptime std.mem.indexOf(u8, sql, " s,") != null) 2 else 1;
        var rank: usize = 0;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |r| {
                const k = (try cellNumber(batch.values[0], r)).?;
                try std.testing.expect(k >= 0 and k < heavy_groups);
                try std.testing.expectEqual(@as(?f64, @floatFromInt(live_counts[@intFromFloat(k)])), try cellNumber(batch.values[count_col], r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(best[rank])), try cellNumber(batch.values[count_col], r));
                rank += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 10), rank);
    }
}

const NuRow = struct { id: i64, w: i64, ip: i32, s: []const u8, u: []const u8, r: i32, x: i32 };
const NuGroup = struct { ip: i32, c: i64 = 0, sr: i64 = 0, sx: i64 = 0 };
const NuForm = enum { wide, string, narrow };
const NuCase = struct { sql: []const u8, form: NuForm, limit: usize, offset: usize, keep: *const fn (NuRow) bool };

fn nuKeepAll(_: NuRow) bool {
    return true;
}

fn nuKeepTagged(r: NuRow) bool {
    return r.s.len != 0;
}

fn nuKeepSmallX(r: NuRow) bool {
    return r.x < 990;
}

/// The result's rows are distinct groups of the rows `c.keep` passes, with
/// their exact aggregates, and their counts are the best counts there are.
fn checkNearUnique(allocator: std.mem.Allocator, db: *thindb.Database, rows: []const NuRow, live: []const bool, c: NuCase) !void {
    errdefer std.debug.print("query: {s}\n", .{c.sql});
    var want: std.AutoHashMapUnmanaged(i64, NuGroup) = .empty;
    defer want.deinit(allocator);
    for (rows, live) |r, keep| {
        if (!keep or !c.keep(r)) continue;
        const g = try want.getOrPut(allocator, r.w);
        if (!g.found_existing) g.value_ptr.* = .{ .ip = r.ip };
        g.value_ptr.c += 1;
        g.value_ptr.sr += r.r;
        g.value_ptr.sx += r.x;
    }
    const best = try allocator.alloc(i64, want.count());
    defer allocator.free(best);
    var it = want.valueIterator();
    var bi: usize = 0;
    while (it.next()) |g| : (bi += 1) best[bi] = g.c;
    std.sort.pdq(i64, best, {}, std.sort.desc(i64));

    var seen: std.AutoHashMapUnmanaged(i64, void) = .empty;
    defer seen.deinit(allocator);
    var got: std.ArrayList(i64) = .empty;
    defer got.deinit(allocator);
    const count_col: usize = if (c.form == .wide) 2 else 1;
    var q = try runSql(allocator, db, c.sql);
    defer q.deinit();
    while (try q.next()) |batch| {
        for (0..batch.row_count) |r| {
            const w: i64 = switch (c.form) {
                .string => try std.fmt.parseInt(i64, batch.values[0].data.string.rowBytes(r)[1..], 10),
                .wide, .narrow => @intFromFloat((try cellNumber(batch.values[0], r)).?),
            };
            const g = want.get(w) orelse return error.TestUnexpectedResult;
            try std.testing.expect(!(try seen.getOrPut(allocator, w)).found_existing);
            try std.testing.expectEqual(@as(?f64, @floatFromInt(g.c)), try cellNumber(batch.values[count_col], r));
            if (c.form == .wide) {
                try std.testing.expectEqual(@as(?f64, @floatFromInt(g.ip)), try cellNumber(batch.values[1], r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(g.sr)), try cellNumber(batch.values[3], r));
                const avg: f64 = @as(f64, @floatFromInt(g.sx)) / @as(f64, @floatFromInt(g.c));
                try std.testing.expectApproxEqAbs(avg, (try cellNumber(batch.values[4], r)).?, 1e-9);
            }
            try got.append(allocator, g.c);
        }
    }
    try std.testing.expectEqual(c.limit, got.items.len);
    std.sort.pdq(i64, got.items, {}, std.sort.desc(i64));
    try std.testing.expectEqualSlices(i64, best[c.offset..][0..c.limit], got.items);
}

test "V2 COUNT top-K over nearly unique keys groups only the keys that repeat" {
    const allocator = std.testing.allocator;
    const base_rows = 300_000;
    // A base row's index and how many more rows repeat its key; every other
    // key is a group of one.
    const repeats = [_][2]usize{ .{ 7, 1 }, .{ 1000, 2 }, .{ 5000, 40 }, .{ 50_001, 2 }, .{ 66_666, 1 }, .{ 120_002, 2 }, .{ 133_333, 1 }, .{ 180_000, 1 }, .{ 199_003, 2 }, .{ 250_000, 1 } };
    const total = comptime blk: {
        var n: usize = base_rows;
        for (repeats) |r| n += r[1];
        break :blk n;
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();
    const t = try db.table("hits", .{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "w", .type = .bigint },
            .{ .name = "ip", .type = .int },
            .{ .name = "s", .type = .string },
            .{ .name = "u", .type = .string },
            .{ .name = "r", .type = .int },
            .{ .name = "x", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    }, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 1024 });

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const rows = try allocator.alloc(NuRow, total + 2);
    defer allocator.free(rows);
    const live = try allocator.alloc(bool, total + 2);
    defer allocator.free(live);
    @memset(live, true);
    var n: usize = 0;
    var next_repeat: usize = 0;
    for (0..base_rows) |i| {
        const w: i64 = @intCast((i * 2654435761) % (1 << 40));
        const u = try std.fmt.allocPrint(arena.allocator(), "u{d}", .{w});
        const ip: i32 = @intCast(i % 1000);
        rows[n] = .{ .id = 0, .w = w, .ip = ip, .s = if (i % 5 == 0) "" else "p", .u = u, .r = @intCast(i % 3), .x = @intCast(i % 1000) };
        n += 1;
        if (next_repeat < repeats.len and repeats[next_repeat][0] == i) {
            for (0..repeats[next_repeat][1]) |j| {
                rows[n] = .{ .id = 0, .w = w, .ip = ip, .s = if (j % 3 == 2) "" else "p", .u = u, .r = @intCast(1 + j % 2), .x = @intCast(7 + j) };
                n += 1;
            }
            next_repeat += 1;
        }
    }
    // Ids scatter each repeated key's rows over the row groups.
    for (rows[0..total], 0..) |*r, pos| r.id = @intCast((pos * 7919) % total);
    try t.insert(rows[0..total]);
    try t.flush();

    const cases = [_]NuCase{
        .{ .sql = "SELECT w, ip, COUNT(*) AS c, SUM(r) AS sr, AVG(x) AS ax FROM hits GROUP BY w, ip ORDER BY c DESC LIMIT 15", .form = .wide, .limit = 15, .offset = 0, .keep = nuKeepAll },
        .{ .sql = "SELECT w, ip, COUNT(*) AS c, SUM(r) AS sr, AVG(x) AS ax FROM hits WHERE s <> '' GROUP BY w, ip ORDER BY c DESC LIMIT 4", .form = .wide, .limit = 4, .offset = 0, .keep = nuKeepTagged },
        .{ .sql = "SELECT u, COUNT(*) AS c FROM hits GROUP BY u ORDER BY c DESC LIMIT 6 OFFSET 7", .form = .string, .limit = 6, .offset = 7, .keep = nuKeepAll },
        .{ .sql = "SELECT w, COUNT(*) AS c FROM hits WHERE x < 990 GROUP BY w ORDER BY c DESC LIMIT 25", .form = .narrow, .limit = 25, .offset = 0, .keep = nuKeepSmallX },
    };
    for (cases) |c| try checkNearUnique(allocator, db, rows[0..total], live[0..total], c);

    // Deleted rows and memtable rows: a key whose second row is only in the
    // memtable, and one more row of a repeated key.
    try exec(allocator, db, "DELETE FROM hits WHERE id % 97 = 0");
    for (rows[0..total], live[0..total]) |r, *keep| keep.* = @mod(r.id, 97) != 0;
    const repeated_w: i64 = @intCast((1000 * 2654435761) % (1 << 40));
    rows[total] = rows[77_777];
    rows[total + 1] = for (rows[0..total]) |r| {
        if (r.w == repeated_w) break r;
    } else unreachable;
    for (rows[total..], 0..) |*r, j| r.id = @intCast(total + j);
    try t.insert(rows[total..]);
    for (cases) |c| try checkNearUnique(allocator, db, rows, live, c);
}

test "V2 COUNT(DISTINCT) top-K stays exact when the count sketch drops the small groups" {
    const allocator = std.testing.allocator;
    // Twelve groups of 1000+ rows by one user, ten of 600 rows by 300 users,
    // twelve of 400 down to 389 rows each by its own user, and one-row
    // groups. The sketch's first threshold keeps the first two kinds, whose
    // best ten hold 300 users, too few to prove the cut: the retry keeps the
    // third kind too, and they hold the answer.
    const unique_rows = 90_000;
    const Row = struct { id: i64, k: i64, s: []const u8, u: i64 };
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var list: std.ArrayListUnmanaged(Row) = .empty;
    defer list.deinit(allocator);
    for (0..12) |j| {
        for (0..1000 + 10 * j) |_| try list.append(allocator, .{ .id = 0, .k = @intCast(j), .s = "", .u = 7 });
    }
    for (0..10) |j| {
        for (0..600) |r| try list.append(allocator, .{ .id = 0, .k = @intCast(100 + j), .s = "", .u = @intCast(j * 1000 + r % 300) });
    }
    for (0..12) |j| {
        for (0..400 - j) |r| try list.append(allocator, .{ .id = 0, .k = @intCast(200 + j), .s = "", .u = @intCast(1_000_000 + j * 1000 + r) });
    }
    for (0..unique_rows) |j| try list.append(allocator, .{ .id = 0, .k = @intCast(10_000 + j), .s = "", .u = @intCast(5_000_000 + j) });
    const rows = list.items;
    for (rows, 0..) |*r, n| {
        r.id = @intCast((n * 7919) % rows.len);
        r.s = try std.fmt.allocPrint(arena.allocator(), "key-{d}", .{r.k});
    }

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();
    const t = try db.table("visits", .{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "k", .type = .bigint },
            .{ .name = "s", .type = .string },
            .{ .name = "u", .type = .bigint },
        },
        .order_key = &.{"id"},
        .unique = true,
    }, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 1024 });
    try t.insert(rows);
    try t.flush();

    inline for (.{
        "SELECT k, COUNT(DISTINCT u) AS d FROM visits GROUP BY k ORDER BY d DESC LIMIT 10",
        "SELECT s, COUNT(DISTINCT u) AS d FROM visits GROUP BY s ORDER BY d DESC LIMIT 10",
    }) |sql| {
        errdefer std.debug.print("query: {s}\n", .{sql});
        var q = try runSql(allocator, db, sql);
        defer q.deinit();
        var rank: usize = 0;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |r| {
                if (comptime std.mem.startsWith(u8, sql, "SELECT s")) {
                    var buf: [16]u8 = undefined;
                    try std.testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "key-{d}", .{200 + rank}), batch.values[0].data.string.rowBytes(r));
                } else {
                    try std.testing.expectEqual(@as(?f64, @floatFromInt(200 + rank)), try cellNumber(batch.values[0], r));
                }
                try std.testing.expectEqual(@as(?f64, @floatFromInt(400 - rank)), try cellNumber(batch.values[1], r));
                rank += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 10), rank);
    }
    {
        // Ranked by COUNT(*), beside a COUNT(DISTINCT).
        var q = try runSql(allocator, db, "SELECT k, COUNT(*) AS c, COUNT(DISTINCT u) AS d FROM visits GROUP BY k ORDER BY c DESC LIMIT 10");
        defer q.deinit();
        var rank: usize = 0;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |r| {
                const j = 11 - rank;
                try std.testing.expectEqual(@as(?f64, @floatFromInt(j)), try cellNumber(batch.values[0], r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(1000 + 10 * j)), try cellNumber(batch.values[1], r));
                try std.testing.expectEqual(@as(?f64, 1), try cellNumber(batch.values[2], r));
                rank += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 10), rank);
    }
}

test "V2 group-topN: a string-derived key evaluates once per dictionary value and folds repeats by count" {
    // 259 pages of 37 domains repeat throughout each row group, so the url
    // blocks are dictionaries holding each page once; one row in eleven is a
    // one-off domain the HAVING drops, and one in 97 is blank. `uniq` never
    // repeats, so its blocks stay raw. A quarter of the rows stay in the
    // memtable, with only each domain's middle pages: its first and last
    // pages, its MIN and MAX, come from dictionaries alone. The filters keep
    // a contiguous run of each dictionary (every value but the blank first
    // one), a gapped one (minus the d1* pages), and all of it. `w` varies
    // across a page's rows, so a collapsed row must carry its rows' fold.
    // The unrolled filter and variant loops build their SQL at comptime.
    @setEvalBranchQuota(4000);
    const a = std.testing.allocator;
    const n = 40_000;
    const Row = struct { id: i64, url: []const u8, uniq: []const u8, w: i32 };
    const rows = try a.alloc(Row, n);
    defer a.free(rows);
    const hosts = try a.alloc([]const u8, n);
    defer a.free(hosts);
    const text = try a.alloc(u8, n * 64);
    defer a.free(text);
    var uniq_len_sum: i64 = 0;
    for (rows, hosts, 0..) |*row, *host, i| {
        const buf = text[i * 64 ..][0..64];
        var url: []const u8 = "";
        host.* = "";
        if (i % 11 == 0) {
            url = try std.fmt.bufPrint(buf[0..32], "https://x{d}.org/q", .{i});
            host.* = url[8 .. url.len - 2];
        } else if (i % 97 != 0) {
            const page = if (i >= n / 4 * 3) 2 + (i / 37) % 3 else (i / 37) % 7;
            url = try std.fmt.bufPrint(buf[0..32], "http://www.d{d}.com/p{d}", .{ i % 37, page });
            host.* = url[11..std.mem.indexOfScalarPos(u8, url, 11, '/').?];
        }
        const uniq = try std.fmt.bufPrint(buf[32..], "https://z.net/{d}", .{i});
        row.* = .{ .id = @intCast(i), .url = url, .uniq = uniq, .w = @intCast((i * 7_919) % 1_009) };
        uniq_len_sum += @intCast(uniq.len);
    }
    const Filter = struct {
        where: []const u8,
        fn keeps(comptime where: []const u8, url: []const u8) bool {
            if (comptime std.mem.eql(u8, where, "WHERE url <> ''")) return url.len > 0;
            if (comptime std.mem.eql(u8, where, "WHERE url NOT LIKE 'http://www.d1%'")) return !std.mem.startsWith(u8, url, "http://www.d1");
            return true;
        }
    };
    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(a, std.testing.io, tmp.dir, .{ .max_dop = dop, .auto_flush_secs = 0 });
        defer db.close();
        const table = try db.table("hits", .{
            .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "url", .type = .string }, .{ .name = "uniq", .type = .string }, .{ .name = "w", .type = .int } },
            .order_key = &.{"id"},
            .unique = false,
        }, .{ .order_key = &.{"id"}, .row_group_size = 4096 });
        try table.insert(rows[0 .. n / 4 * 3]);
        try table.flush();
        try table.insert(rows[n / 4 * 3 ..]);

        inline for (.{ Filter{ .where = "WHERE url <> ''" }, Filter{ .where = "WHERE url NOT LIKE 'http://www.d1%'" }, Filter{ .where = "" } }) |filter| {
            const Expected = struct { count: i64 = 0, len_sum: i64 = 0, min_len: i64 = std.math.maxInt(i64), max_len: i64 = 0, w_sum: i64 = 0, w_min: i32 = std.math.maxInt(i32), w_max: i32 = std.math.minInt(i32), min_url: []const u8 = "", max_url: []const u8 = "" };
            var expected: std.StringHashMapUnmanaged(Expected) = .empty;
            defer expected.deinit(a);
            for (rows, hosts) |row, host| {
                if (!Filter.keeps(filter.where, row.url)) continue;
                const e = try expected.getOrPut(a, host);
                if (!e.found_existing) e.value_ptr.* = .{ .min_url = row.url, .max_url = row.url };
                e.value_ptr.count += 1;
                e.value_ptr.len_sum += @intCast(row.url.len);
                e.value_ptr.min_len = @min(e.value_ptr.min_len, @as(i64, @intCast(row.url.len)));
                e.value_ptr.max_len = @max(e.value_ptr.max_len, @as(i64, @intCast(row.url.len)));
                e.value_ptr.w_sum += row.w;
                e.value_ptr.w_min = @min(e.value_ptr.w_min, row.w);
                e.value_ptr.w_max = @max(e.value_ptr.w_max, row.w);
                if (std.mem.order(u8, row.url, e.value_ptr.min_url) == .lt) e.value_ptr.min_url = row.url;
                if (std.mem.order(u8, row.url, e.value_ptr.max_url) == .gt) e.value_ptr.max_url = row.url;
            }
            var groups: usize = 0;
            var it = expected.valueIterator();
            while (it.next()) |e| groups += @intFromBool(e.count > 1);

            // A dictionary's values reach the staging folds in ascending
            // order, which settles a MIN early and a MAX late. Two
            // aggregates over one input each stage their own copy of it, and
            // a program without string extremes collapses per value too, as
            // does one with numeric inputs of its own: each aggregate over
            // `w` carries its own fold of the value's rows.
            const Variant = struct { aggs: []const u8, min_url: ?usize = null, max_url: ?usize = null, len_range: ?usize = null, w_sum: ?usize = null, w_avg: ?usize = null, w_min: ?usize = null, w_max: ?usize = null };
            inline for (.{
                Variant{ .aggs = ", MIN(url)", .min_url = 3 },
                Variant{ .aggs = ", MAX(url)", .max_url = 3 },
                Variant{ .aggs = ", MIN(url), MAX(url)", .min_url = 3, .max_url = 4 },
                Variant{ .aggs = "" },
                Variant{ .aggs = ", MIN(length(url)), MAX(length(url))", .len_range = 3 },
                Variant{ .aggs = ", MIN(url), AVG(w)", .min_url = 3, .w_avg = 4 },
                Variant{ .aggs = ", MAX(w)", .w_max = 3 },
                Variant{ .aggs = ", SUM(w), MIN(url)", .w_sum = 3, .min_url = 4 },
                Variant{ .aggs = ", MIN(w), MAX(url)", .w_min = 3, .max_url = 4 },
                Variant{ .aggs = ", MIN(w), MAX(w)", .w_min = 3, .w_max = 4 },
                Variant{ .aggs = ", MAX(w), SUM(w), MIN(url), MIN(w), AVG(w)", .w_max = 3, .w_sum = 4, .min_url = 5, .w_min = 6, .w_avg = 7 },
            }) |v| {
                const sql = "SELECT REGEXP_REPLACE(url, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, AVG(length(url)) AS l, COUNT(*) AS c" ++
                    v.aggs ++ " FROM hits " ++ filter.where ++ " GROUP BY k HAVING COUNT(*) > 1 ORDER BY l DESC LIMIT 100";
                errdefer std.debug.print("dop {d}: {s}\n", .{ dop, sql });
                try expectPlanRunsGroupTopN(a, db, sql);
                var q = try runSql(a, db, sql);
                defer q.deinit();
                var seen: usize = 0;
                var prev_l: f64 = std.math.inf(f64);
                while (try q.next()) |batch| {
                    for (0..batch.row_count) |r| {
                        const e = expected.get(batch.values[0].data.string.rowBytes(r)) orelse return error.TestUnexpectedResult;
                        const l = (try cellNumber(batch.values[1], r)).?;
                        try std.testing.expect(l <= prev_l);
                        prev_l = l;
                        try std.testing.expectApproxEqRel(@as(f64, @floatFromInt(e.len_sum)) / @as(f64, @floatFromInt(e.count)), l, 1e-12);
                        try std.testing.expectEqual(e.count, batch.values[2].data.bigint[r]);
                        if (v.min_url) |i| try std.testing.expectEqualStrings(e.min_url, batch.values[i].data.string.rowBytes(r));
                        if (v.max_url) |i| try std.testing.expectEqualStrings(e.max_url, batch.values[i].data.string.rowBytes(r));
                        if (v.len_range) |i| {
                            try std.testing.expectEqual(@as(f64, @floatFromInt(e.min_len)), (try cellNumber(batch.values[i], r)).?);
                            try std.testing.expectEqual(@as(f64, @floatFromInt(e.max_len)), (try cellNumber(batch.values[i + 1], r)).?);
                        }
                        if (v.w_sum) |i| try std.testing.expectEqual(@as(f64, @floatFromInt(e.w_sum)), (try cellNumber(batch.values[i], r)).?);
                        if (v.w_avg) |i| try std.testing.expectApproxEqRel(@as(f64, @floatFromInt(e.w_sum)) / @as(f64, @floatFromInt(e.count)), (try cellNumber(batch.values[i], r)).?, 1e-12);
                        if (v.w_min) |i| try std.testing.expectEqual(@as(f64, @floatFromInt(e.w_min)), (try cellNumber(batch.values[i], r)).?);
                        if (v.w_max) |i| try std.testing.expectEqual(@as(f64, @floatFromInt(e.w_max)), (try cellNumber(batch.values[i], r)).?);
                        seen += 1;
                    }
                }
                try std.testing.expectEqual(groups, seen);
            }

            // The url itself as the key: each dictionary value is one row.
            var url_counts: std.StringHashMapUnmanaged(i64) = .empty;
            defer url_counts.deinit(a);
            for (rows) |row| {
                if (!Filter.keeps(filter.where, row.url)) continue;
                const e = try url_counts.getOrPut(a, row.url);
                if (!e.found_existing) e.value_ptr.* = 0;
                e.value_ptr.* += 1;
            }
            var url_groups: usize = 0;
            var uit = url_counts.valueIterator();
            while (uit.next()) |c| url_groups += @intFromBool(c.* > 1);

            const url_sql = "SELECT url, COUNT(*) AS c FROM hits " ++ filter.where ++ " GROUP BY url HAVING COUNT(*) > 1 ORDER BY c DESC LIMIT 1000";
            errdefer std.debug.print("dop {d}: {s}\n", .{ dop, url_sql });
            try expectPlanRunsGroupTopN(a, db, url_sql);
            var uq = try runSql(a, db, url_sql);
            defer uq.deinit();
            var url_seen: usize = 0;
            var prev_c: i64 = std.math.maxInt(i64);
            while (try uq.next()) |batch| {
                for (0..batch.row_count) |r| {
                    const want = url_counts.get(batch.values[0].data.string.rowBytes(r)) orelse return error.TestUnexpectedResult;
                    const c = batch.values[1].data.bigint[r];
                    try std.testing.expectEqual(want, c);
                    try std.testing.expect(c <= prev_c);
                    prev_c = c;
                    url_seen += 1;
                }
            }
            try std.testing.expectEqual(url_groups, url_seen);
        }

        const uniq_sql = "SELECT REGEXP_REPLACE(uniq, '^https?://([^/]+)/.*$', '\\1') AS k, COUNT(*), SUM(length(uniq)), MIN(uniq) FROM hits GROUP BY k ORDER BY 2 DESC LIMIT 5";
        var uq = try runSql(a, db, uniq_sql);
        defer uq.deinit();
        var uniq_rows: usize = 0;
        while (try uq.next()) |batch| {
            for (0..batch.row_count) |r| {
                try std.testing.expectEqualStrings("z.net", batch.values[0].data.string.rowBytes(r));
                try std.testing.expectEqual(@as(?f64, n), try cellNumber(batch.values[1], r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(uniq_len_sum)), try cellNumber(batch.values[2], r));
                try std.testing.expectEqualStrings("https://z.net/0", batch.values[3].data.string.rowBytes(r));
                uniq_rows += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 1), uniq_rows);
    }
}

test "V2 COUNT top-K: the sketch counts dictionary-collapsed rows by their repeat counts" {
    // Each heavy host is one url repeated in every row group, so each row
    // group's dictionary collapses it to one row standing for many. Each
    // medium host spreads over 600 one-off urls, so it collapses to many rows
    // of one. A sketch counting collapsed rows rather than their weights would
    // rank the medium hosts above the heavy ones and drop the heavy rows.
    // Light urls come in adjacent pairs: every row group stays under half
    // distinct (so its url block is a dictionary), and the groups are many
    // enough for the sketch to run, filtered too (a filter's group estimate
    // is a quarter of the rows). A collapsed row the sketch keeps keeps
    // its folds of `w` too, one per aggregate; light urls sort first in each
    // dictionary, so the kept rows move when the dropped ones compact away.
    const a = std.testing.allocator;
    const heavy_hosts = 12;
    const medium_hosts = 20;
    const medium_rows = 600;
    const light_pairs = 120_000;
    const heavy_rows = comptime blk: {
        var n: usize = 0;
        for (0..heavy_hosts) |g| n += 3000 - 100 * g;
        break :blk n;
    };
    const units = heavy_rows + medium_hosts * medium_rows + light_pairs;
    const total = heavy_rows + medium_hosts * medium_rows + 2 * light_pairs;

    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const unit_urls = try a.alloc([]const u8, units);
    defer a.free(unit_urls);
    var u: usize = 0;
    for (0..heavy_hosts) |g| {
        const url = try std.fmt.allocPrint(arena.allocator(), "http://heavy{d}.com/", .{g});
        for (0..3000 - 100 * g) |_| {
            unit_urls[u] = url;
            u += 1;
        }
    }
    for (0..medium_hosts) |m| {
        for (0..medium_rows) |r| {
            unit_urls[u] = try std.fmt.allocPrint(arena.allocator(), "http://medium{d}.net/{d}", .{ m, r });
            u += 1;
        }
    }
    const light_start = u;
    for (0..light_pairs) |l| {
        unit_urls[u] = try std.fmt.allocPrint(arena.allocator(), "http://alight{d}.org/x", .{l});
        u += 1;
    }

    const Row = struct { id: i64, url: []const u8, w: i32 };
    const rows = try a.alloc(Row, total);
    defer a.free(rows);
    const Folds = struct { min: i32 = std.math.maxInt(i32), max: i32 = std.math.minInt(i32), sum: i64 = 0 };
    var heavy_w: [heavy_hosts]Folds = @splat(.{});
    // Heavy rows with `w + 1 > 507`, as (host, count).
    var heavy_passing: [heavy_hosts][2]i64 = undefined;
    for (&heavy_passing, 0..) |*hp, g| hp.* = .{ @intCast(g), 0 };
    // 7919 is prime and doesn't divide `units`, so this visits every unit once.
    var i: usize = 0;
    for (0..units) |p| {
        const unit = (p * 7919) % units;
        const copies: usize = if (unit >= light_start) 2 else 1;
        for (0..copies) |_| {
            const w: i32 = @intCast((i * 104_729) % 1_013);
            rows[i] = .{ .id = @intCast(i), .url = unit_urls[unit], .w = w };
            if (std.mem.startsWith(u8, unit_urls[unit], "http://heavy")) {
                const host = try std.fmt.parseInt(usize, unit_urls[unit][12 .. unit_urls[unit].len - 5], 10);
                const f = &heavy_w[host];
                f.min = @min(f.min, w);
                f.max = @max(f.max, w);
                f.sum += w;
                if (w + 1 > 507) heavy_passing[host][1] += 1;
            }
            i += 1;
        }
    }

    std.mem.sort([2]i64, &heavy_passing, {}, struct {
        fn more(_: void, x: [2]i64, y: [2]i64) bool {
            return x[1] > y[1];
        }
    }.more);
    // The top 10 hosts by passing rows rank without ties.
    for (heavy_passing[0..10], heavy_passing[1..11]) |x, y| try std.testing.expect(x[1] > y[1]);

    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(a, std.testing.io, tmp.dir, .{ .max_dop = dop, .auto_flush_secs = 0 });
        defer db.close();
        const table = try db.table("hits", .{
            .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "url", .type = .string }, .{ .name = "w", .type = .int } },
            .order_key = &.{"id"},
            .unique = false,
        }, .{ .order_key = &.{"id"}, .row_group_size = 2048 });
        try table.insert(rows);
        try table.flush();

        inline for (.{ "", ", MIN(w), MAX(w), SUM(w)" }) |aggs| {
            const sql = "SELECT REGEXP_REPLACE(url, '^https?://([^/]+)/.*$', '\\1') AS k, COUNT(*) AS c" ++ aggs ++ " FROM hits GROUP BY k ORDER BY c DESC LIMIT 10";
            errdefer std.debug.print("dop {d}: {s}\n", .{ dop, sql });
            try expectPlanRunsGroupTopN(a, db, sql);
            var q = try runSql(a, db, sql);
            defer q.deinit();
            var rank: usize = 0;
            while (try q.next()) |batch| {
                for (0..batch.row_count) |r| {
                    var buf: [16]u8 = undefined;
                    try std.testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "heavy{d}.com", .{rank}), batch.values[0].data.string.rowBytes(r));
                    try std.testing.expectEqual(@as(?f64, @floatFromInt(3000 - 100 * rank)), try cellNumber(batch.values[1], r));
                    if (aggs.len != 0) {
                        try std.testing.expectEqual(@as(?f64, @floatFromInt(heavy_w[rank].min)), try cellNumber(batch.values[2], r));
                        try std.testing.expectEqual(@as(?f64, @floatFromInt(heavy_w[rank].max)), try cellNumber(batch.values[3], r));
                        try std.testing.expectEqual(@as(?f64, @floatFromInt(heavy_w[rank].sum)), try cellNumber(batch.values[4], r));
                    }
                    rank += 1;
                }
            }
            try std.testing.expectEqual(@as(usize, 10), rank);
        }

        // A key over `w` alone evaluates per row ahead of the collapse, which
        // leaves `w` spent; the filter pass compacts it with the kept rows.
        const pre_sql = "SELECT REGEXP_REPLACE(url, '^https?://([^/]+)/.*$', '\\1') AS k, w DIV 2000 AS h, COUNT(*) AS c FROM hits GROUP BY k, h ORDER BY c DESC LIMIT 10";
        errdefer std.debug.print("dop {d}: {s}\n", .{ dop, pre_sql });
        try expectPlanRunsGroupTopN(a, db, pre_sql);
        var pq = try runSql(a, db, pre_sql);
        defer pq.deinit();
        var pre_rank: usize = 0;
        while (try pq.next()) |batch| {
            for (0..batch.row_count) |r| {
                var buf: [16]u8 = undefined;
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "heavy{d}.com", .{pre_rank}), batch.values[0].data.string.rowBytes(r));
                try std.testing.expectEqual(@as(?f64, 0), try cellNumber(batch.values[1], r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(3000 - 100 * pre_rank)), try cellNumber(batch.values[2], r));
                pre_rank += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 10), pre_rank);

        // A WHERE over a computed operand drops collapsed rows ahead of the
        // sketch, which counts and keeps only the rows that pass it.
        const where_sql = "SELECT REGEXP_REPLACE(url, '^https?://([^/]+)/.*$', '\\1') AS k, COUNT(*) AS c FROM hits WHERE w + 1 > 507 GROUP BY k ORDER BY c DESC LIMIT 10";
        errdefer std.debug.print("dop {d}: {s}\n", .{ dop, where_sql });
        try std.testing.expect(try planContains(a, db, where_sql, "residual filter"));
        var wq = try runSql(a, db, where_sql);
        defer wq.deinit();
        var where_rank: usize = 0;
        while (try wq.next()) |batch| {
            for (0..batch.row_count) |r| {
                var buf: [16]u8 = undefined;
                const want = heavy_passing[where_rank];
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "heavy{d}.com", .{want[0]}), batch.values[0].data.string.rowBytes(r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(want[1])), try cellNumber(batch.values[1], r));
                where_rank += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 10), where_rank);

        // The url itself as the key, collapsed with no expression to evaluate.
        const url_sql = "SELECT url, COUNT(*) AS c FROM hits GROUP BY url ORDER BY c DESC LIMIT 10";
        errdefer std.debug.print("dop {d}: {s}\n", .{ dop, url_sql });
        try expectPlanRunsGroupTopN(a, db, url_sql);
        var uq = try runSql(a, db, url_sql);
        defer uq.deinit();
        var url_rank: usize = 0;
        while (try uq.next()) |batch| {
            for (0..batch.row_count) |r| {
                var buf: [24]u8 = undefined;
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "http://heavy{d}.com/", .{url_rank}), batch.values[0].data.string.rowBytes(r));
                try std.testing.expectEqual(@as(?f64, @floatFromInt(3000 - 100 * url_rank)), try cellNumber(batch.values[1], r));
                url_rank += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 10), url_rank);
    }
}

const HkRow = struct { id: i64, s: []const u8, v: i32 };
const HkGroup = struct { s: []const u8, count: i64 = 0, sum: i64 = 0 };

fn hkMoreRows(_: void, x: HkGroup, y: HkGroup) bool {
    if (x.count != y.count) return x.count > y.count;
    return std.mem.order(u8, x.s, y.s) == .lt;
}

/// The groups of `rows` by s, most rows first, without those of `skip`.
fn hkGroups(allocator: std.mem.Allocator, rows: []const HkRow, skip: []const u8) ![]HkGroup {
    var index: std.StringHashMapUnmanaged(usize) = .empty;
    defer index.deinit(allocator);
    var groups: std.ArrayList(HkGroup) = .empty;
    errdefer groups.deinit(allocator);
    for (rows) |row| {
        if (std.mem.eql(u8, row.s, skip)) continue;
        const gop = try index.getOrPut(allocator, row.s);
        if (!gop.found_existing) {
            gop.value_ptr.* = groups.items.len;
            try groups.append(allocator, .{ .s = row.s });
        }
        const g = &groups.items[gop.value_ptr.*];
        g.count += 1;
        g.sum += row.v;
    }
    const out = try groups.toOwnedSlice(allocator);
    std.sort.pdq(HkGroup, out, {}, hkMoreRows);
    return out;
}

test "V2 COUNT top-K: heavy keys rank exactly over dictionary, raw, tombstoned and memtable rows" {
    // Segment A's row groups are dictionaries: hot urls spread over them
    // among light urls in adjacent pairs, so each row group stays under half
    // distinct. Segment B's row groups are near-unique (raw blocks), with a
    // hot url every 16th row; some of segment C's hot rows are deleted; the
    // memtable adds more. Hot counts differ, so every ranked page is
    // tie-free. The light urls alone hold 2 rows each: too many candidates,
    // so that query falls back to grouping them all.
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const hot = 12;
    const light_pairs = 110_000;

    var hot_urls: [hot][]const u8 = undefined;
    for (&hot_urls, 0..) |*u, g| u.* = try std.fmt.allocPrint(arena, "http://hot{d}.example/", .{g});
    var units: std.ArrayList([]const u8) = .empty;
    for (0..hot) |g| {
        for (0..2400 - 90 * g) |_| try units.append(arena, hot_urls[g]);
    }
    const light_start = units.items.len;
    for (0..light_pairs) |l| try units.append(arena, try std.fmt.allocPrint(arena, "http://light{d}.example/x", .{l}));

    var seg_a: std.ArrayList(HkRow) = .empty;
    var id: i64 = 0;
    // 7919 is prime and doesn't divide the unit count: every unit once.
    for (0..units.items.len) |p| {
        const unit = (p * 7919) % units.items.len;
        const copies: usize = if (unit >= light_start) 2 else 1;
        for (0..copies) |_| {
            try seg_a.append(arena, .{ .id = id, .s = units.items[unit], .v = @intCast(@mod(id * 104_729, 1_013)) });
            id += 1;
        }
    }
    var seg_b: std.ArrayList(HkRow) = .empty;
    for (0..40_000) |i| {
        const s = if (i % 16 == 0) hot_urls[(i / 16) % hot] else try std.fmt.allocPrint(arena, "http://uniq{d}.example/", .{i});
        try seg_b.append(arena, .{ .id = id, .s = s, .v = @intCast(@mod(id * 104_729, 1_013)) });
        id += 1;
    }
    const c_first = id;
    var seg_c: std.ArrayList(HkRow) = .empty;
    for (0..3000) |i| {
        try seg_c.append(arena, .{ .id = id, .s = hot_urls[i % hot], .v = @intCast(@mod(id * 104_729, 1_013)) });
        id += 1;
    }
    const deleted_below = c_first + 1000;
    var mem: std.ArrayList(HkRow) = .empty;
    for (0..600) |i| {
        const s = if (i % 2 == 0) hot_urls[i % hot] else try std.fmt.allocPrint(arena, "http://mem{d}.example/", .{i % 7});
        try mem.append(arena, .{ .id = id, .s = s, .v = @intCast(@mod(id * 104_729, 1_013)) });
        id += 1;
    }

    var live: std.ArrayList(HkRow) = .empty;
    for ([_][]const HkRow{ seg_a.items, seg_b.items, seg_c.items, mem.items }) |part| {
        for (part) |row| {
            if (row.id >= c_first and row.id < deleted_below) continue;
            try live.append(arena, row);
        }
    }
    const all_groups = try hkGroups(allocator, live.items, "");
    defer allocator.free(all_groups);
    const no_hot0 = try hkGroups(allocator, live.items, hot_urls[0]);
    defer allocator.free(no_hot0);
    for (0..hot - 1) |i| try std.testing.expect(all_groups[i].count > all_groups[i + 1].count);

    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .auto_flush_secs = 0 });
        defer db.close();
        const t = try db.table("hk", .{
            .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "s", .type = .string }, .{ .name = "v", .type = .int } },
            .order_key = &.{"id"},
            .unique = false,
        }, .{ .order_key = &.{"id"}, .row_group_size = 1024 });
        for ([_][]const HkRow{ seg_a.items, seg_b.items, seg_c.items }) |part| {
            try t.insert(part);
            try t.flush();
        }
        const delete_sql = try std.fmt.allocPrint(allocator, "DELETE FROM hk WHERE id >= {d} AND id < {d}", .{ c_first, deleted_below });
        defer allocator.free(delete_sql);
        try exec(allocator, db, delete_sql);
        try t.insert(mem.items);

        const Case = struct { sql: []const u8, want: []const HkGroup, sum: bool };
        const cases = [_]Case{
            .{ .sql = "SELECT s, COUNT(*) AS c FROM hk GROUP BY s ORDER BY c DESC LIMIT 5", .want = all_groups[0..5], .sum = false },
            .{ .sql = "SELECT s, COUNT(*) AS c, SUM(v) AS t FROM hk WHERE s <> 'http://hot0.example/' GROUP BY s ORDER BY c DESC LIMIT 3 OFFSET 2", .want = no_hot0[2..5], .sum = true },
        };
        for (cases) |case| {
            errdefer std.debug.print("dop {d}: {s}\n", .{ dop, case.sql });
            try expectPlanRunsGroupTopN(allocator, db, case.sql);
            var q = try runSql(allocator, db, case.sql);
            defer q.deinit();
            var rank: usize = 0;
            while (try q.next()) |batch| {
                for (0..batch.row_count) |r| {
                    try std.testing.expectEqualStrings(case.want[rank].s, batch.values[0].data.string.rowBytes(r));
                    try std.testing.expectEqual(@as(?f64, @floatFromInt(case.want[rank].count)), try cellNumber(batch.values[1], r));
                    if (case.sum) try std.testing.expectEqual(@as(?f64, @floatFromInt(case.want[rank].sum)), try cellNumber(batch.values[2], r));
                    rank += 1;
                }
            }
            try std.testing.expectEqual(case.want.len, rank);
        }

        const flat_sql = "SELECT s, COUNT(*) AS c FROM hk WHERE s LIKE 'http://light%' GROUP BY s ORDER BY c DESC LIMIT 5";
        errdefer std.debug.print("dop {d}: {s}\n", .{ dop, flat_sql });
        var q = try runSql(allocator, db, flat_sql);
        defer q.deinit();
        var n: usize = 0;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |r| {
                try std.testing.expect(std.mem.startsWith(u8, batch.values[0].data.string.rowBytes(r), "http://light"));
                try std.testing.expectEqual(@as(?f64, 2), try cellNumber(batch.values[1], r));
            }
            n += batch.row_count;
        }
        try std.testing.expectEqual(@as(usize, 5), n);
    }
}

test "group keys packed past 96 bits keep groups that differ only in their high bits" {
    const allocator = std.testing.allocator;
    const Row = struct { id: i64, a: i64, b: i64, c: i32, d: i32 };
    // Each pair of `b` values, and every `d` value, agrees in the low 96
    // bits of the packed (a, b) and (a, c, d) keys.
    const a_vals = [_]i64{ std.math.minInt(i64), -1, 0, std.math.maxInt(i64) };
    const b_vals = [_]i64{ std.math.minInt(i64), std.math.minInt(i64) + (1 << 32), std.math.maxInt(i64), std.math.maxInt(i64) - (1 << 32) };
    const d_vals = [_]i32{ std.math.minInt(i32), -1, 1, std.math.maxInt(i32) };
    const n = 100_000;
    const groups = 16;
    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop });
        defer db.close();
        const t = try db.table("wide_keys", .{
            .columns = &.{
                .{ .name = "id", .type = .bigint },
                .{ .name = "a", .type = .bigint },
                .{ .name = "b", .type = .bigint },
                .{ .name = "c", .type = .int },
                .{ .name = "d", .type = .int },
            },
            .order_key = &.{"id"},
            .unique = false,
        }, .{ .order_key = &.{"id"} });
        const rows = try allocator.alloc(Row, n);
        defer allocator.free(rows);
        for (rows, 0..) |*row, i| row.* = .{ .id = @intCast(i), .a = a_vals[i % 4], .b = b_vals[(i / 4) % 4], .c = 7, .d = d_vals[(i / 4) % 4] };
        try t.insert(rows);
        try t.flush();
        inline for (.{
            .{ "SELECT a, b, COUNT(*) AS n FROM wide_keys GROUP BY a, b", 2 },
            .{ "SELECT a, b, COUNT(*) AS n FROM wide_keys GROUP BY a, b ORDER BY n DESC LIMIT 100", 2 },
            .{ "SELECT a, c, d, COUNT(*) AS n FROM wide_keys GROUP BY a, c, d", 3 },
            .{ "SELECT a, c, d, COUNT(*) AS n, SUM(id) AS s FROM wide_keys GROUP BY a, c, d", 3 },
        }) |case| {
            var q = try runSql(allocator, db, case[0]);
            defer q.deinit();
            var seen: usize = 0;
            while (try q.next()) |batch| {
                for (batch.values[case[1]].data.bigint[0..batch.row_count]) |count| {
                    try std.testing.expectEqual(@as(i64, n / groups), count);
                }
                seen += batch.row_count;
            }
            try std.testing.expectEqual(@as(usize, groups), seen);
        }
    }
}

test "grouped engines take computed comparison operands as CASE operands (keys, conditional sums, global)" {
    // `octet_length(s) > 3` compares a computed operand, which the parser
    // materializes in a Compute below the one reading it.
    const allocator = std.testing.allocator;
    const words = [_][]const u8{ "a", "bb", "cccc", "ddddd", "", "eeeeeeee" };
    const n = 240;
    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .row_group_size = 64, .auto_flush_secs = 0 });
        defer db.close();
        try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, g INT NOT NULL, s STRING NOT NULL, ns STRING)");
        var long_by_g = [_]i64{ 0, 0, 0 };
        var rows_by_g = [_]i64{ 0, 0, 0 };
        var long_ids: i64 = 0;
        var ns_long: i64 = 0;
        var ns_null: i64 = 0;
        for (0..2) |half| {
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(allocator);
            try buf.appendSlice(allocator, "INSERT INTO t (id, g, s, ns) VALUES ");
            for (half * n / 2..(half + 1) * n / 2) |i| {
                const s = words[i % words.len];
                const g = i % 3;
                if (i != half * n / 2) try buf.appendSlice(allocator, ",");
                if (i % 5 == 0) {
                    try buf.print(allocator, "({d},{d},'{s}',NULL)", .{ i, g, s });
                    ns_null += 1;
                } else {
                    try buf.print(allocator, "({d},{d},'{s}','{s}')", .{ i, g, s, s });
                    if (s.len > 3) ns_long += 1;
                }
                rows_by_g[g] += 1;
                if (s.len > 3) {
                    long_by_g[g] += 1;
                    long_ids += @intCast(i);
                }
            }
            try exec(allocator, db, buf.items);
            // The first half flushes; the second stays in the memtable.
            if (half == 0) try (try db.openTable("t", .{})).flush();
        }
        const long_total = long_by_g[0] + long_by_g[1] + long_by_g[2];

        const by_key = "SELECT octet_length(s) > 3 AS lng, COUNT(*) AS c FROM t GROUP BY lng ORDER BY c DESC LIMIT 10";
        try std.testing.expect(try planContains(allocator, db, by_key, "(V2 "));
        var q = try runSql(allocator, db, by_key);
        defer q.deinit();
        var counts: std.ArrayList(i64) = .empty;
        defer counts.deinit(allocator);
        while (try q.next()) |b| try counts.appendSlice(allocator, b.values[1].data.bigint[0..b.row_count]);
        std.mem.sort(i64, counts.items, {}, std.sort.asc(i64));
        try std.testing.expectEqualSlices(i64, &.{ long_total, n - long_total }, counts.items);

        const nullable_key = "SELECT octet_length(ns) > 3 AS lng, COUNT(*) AS c FROM t GROUP BY lng ORDER BY c DESC LIMIT 10";
        try std.testing.expect(try planContains(allocator, db, nullable_key, "(V2 "));
        var nq = try runSql(allocator, db, nullable_key);
        defer nq.deinit();
        counts.clearRetainingCapacity();
        while (try nq.next()) |b| try counts.appendSlice(allocator, b.values[1].data.bigint[0..b.row_count]);
        std.mem.sort(i64, counts.items, {}, std.sort.asc(i64));
        var want = [_]i64{ ns_long, ns_null, n - ns_long - ns_null };
        std.mem.sort(i64, &want, {}, std.sort.asc(i64));
        try std.testing.expectEqualSlices(i64, &want, counts.items);

        const conditional = "SELECT g, SUM(CASE WHEN octet_length(s) > 3 THEN 1 ELSE 0 END) AS longs, COUNT(*) AS c FROM t WHERE g <> 1 GROUP BY g ORDER BY g LIMIT 10";
        try std.testing.expect(try planContains(allocator, db, conditional, "(V2 "));
        var cq = try runSql(allocator, db, conditional);
        defer cq.deinit();
        var seen: usize = 0;
        while (try cq.next()) |b| {
            for (0..b.row_count) |r| {
                const g: usize = @intFromFloat((try cellNumber(b.values[0], r)).?);
                try std.testing.expect(g != 1);
                try std.testing.expectEqual(long_by_g[g], b.values[1].data.bigint[r]);
                try std.testing.expectEqual(rows_by_g[g], b.values[2].data.bigint[r]);
                seen += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 2), seen);

        const global = "SELECT SUM(CASE WHEN octet_length(s) > 3 THEN id ELSE 0 END) AS x FROM t";
        try std.testing.expect(try planContains(allocator, db, global, "(V2 "));
        var gq = try runSql(allocator, db, global);
        defer gq.deinit();
        const gb = (try gq.next()).?;
        try std.testing.expectEqual(long_ids, gb.values[0].data.bigint[0]);
    }
}

test "grouped engines collapse rows by string codes combined with other source columns" {
    // Derived keys over a string column next to an integer key (narrow or
    // wide), a second string column, or an integer the derived columns read:
    // each segment batch evaluates once per value combination its rows use.
    // A derived column over integers alone evaluates per row ahead of that,
    // as a key in the combination or as an aggregate input; one that can be
    // NULL keeps its NULL group.
    const allocator = std.testing.allocator;
    const words = [_][]const u8{ "a", "bb", "cccc", "ddddd", "", "eeeeeeee" };
    const uppers = [_][]const u8{ "A", "BB", "CCCC", "DDDDD", "", "EEEEEEEE" };
    const tags = [_][]const u8{ "p", "qq", "r" };
    const n = 384;
    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .row_group_size = 64, .auto_flush_secs = 0 });
        defer db.close();
        try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, g INT NOT NULL, s STRING NOT NULL, u STRING NOT NULL, x INT NOT NULL, w BIGINT NOT NULL)");
        var count_sg = [_][3]i64{.{ 0, 0, 0 }} ** words.len;
        var sum_sg = [_][3]i64{.{ 0, 0, 0 }} ** words.len;
        var count_su = [_][tags.len]i64{.{ 0, 0, 0 }} ** words.len;
        var sum_su = [_][tags.len]i64{.{ 0, 0, 0 }} ** words.len;
        var count_sw = [_][4]i64{.{ 0, 0, 0, 0 }} ** words.len;
        var max_w_sg = [_][3]i64{.{ std.math.minInt(i64), std.math.minInt(i64), std.math.minInt(i64) }} ** words.len;
        var sum_h_sg = [_][3]i64{.{ 0, 0, 0 }} ** words.len;
        var max_sq_s = [_]i64{std.math.minInt(i64)} ** words.len;
        var min_sq_s = [_]i64{std.math.maxInt(i64)} ** words.len;
        var quotient_counts: std.StringArrayHashMapUnmanaged(i64) = .empty;
        defer {
            for (quotient_counts.keys()) |k| allocator.free(k);
            quotient_counts.deinit(allocator);
        }
        for (0..2) |half| {
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(allocator);
            try buf.appendSlice(allocator, "INSERT INTO t (id, g, s, u, x, w) VALUES ");
            for (half * n / 2..(half + 1) * n / 2) |i| {
                const si = i % words.len;
                const ui = (i / 7) % tags.len;
                const g = i % 3;
                const x: i64 = @intCast(i % 11);
                if (i != half * n / 2) try buf.appendSlice(allocator, ",");
                const wi = i % 4;
                try buf.print(allocator, "({d},{d},'{s}','{s}',{d},{d})", .{ i, g, words[si], tags[ui], x, wideValue(wi) });
                count_sw[si][wi] += 1;
                max_w_sg[si][g] = @max(max_w_sg[si][g], wideValue(wi));
                sum_h_sg[si][g] += @as(i64, @intCast(wi)) - 2;
                max_sq_s[si] = @max(max_sq_s[si], x * x - @as(i64, @intCast(g)));
                min_sq_s[si] = @min(min_sq_s[si], x * x - @as(i64, @intCast(g)));
                const quotient = if (g == 0)
                    try std.fmt.allocPrint(allocator, "{s}|NULL", .{uppers[si]})
                else
                    try std.fmt.allocPrint(allocator, "{s}|{d}", .{ uppers[si], @divFloor(x, @as(i64, @intCast(g))) });
                const entry = try quotient_counts.getOrPut(allocator, quotient);
                if (entry.found_existing) allocator.free(quotient) else entry.value_ptr.* = 0;
                entry.value_ptr.* += 1;
                count_sg[si][g] += 1;
                sum_sg[si][g] += x;
                count_su[si][ui] += 1;
                sum_su[si][ui] += x;
            }
            try exec(allocator, db, buf.items);
            // The first half flushes; the second stays in the memtable.
            if (half == 0) try (try db.openTable("t", .{})).flush();
        }

        var arena_state = std.heap.ArenaAllocator.init(allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        var want_key_int: std.ArrayList([]const u8) = .empty;
        var want_read_int: std.ArrayList([]const u8) = .empty;
        var want_two_strings: std.ArrayList([]const u8) = .empty;
        var want_concat: std.ArrayList([]const u8) = .empty;
        var want_wide: std.ArrayList([]const u8) = .empty;
        var want_pre_key: std.ArrayList([]const u8) = .empty;
        var want_pre_sum: std.ArrayList([]const u8) = .empty;
        var want_pre_folds: std.ArrayList([]const u8) = .empty;
        var want_nullable_pre: std.ArrayList([]const u8) = .empty;
        for (quotient_counts.keys(), quotient_counts.values()) |k, c| {
            try want_nullable_pre.append(arena, try std.fmt.allocPrint(arena, "{s}|{d}", .{ k, c }));
        }
        for (words, uppers, 0..) |w, up, si| {
            for (0..4) |wi| {
                if (count_sw[si][wi] == 0) continue;
                try want_wide.append(arena, try std.fmt.allocPrint(arena, "{s}|{d}|{d}", .{ up, wideValue(wi), count_sw[si][wi] }));
                try want_pre_key.append(arena, try std.fmt.allocPrint(arena, "{s}|{d}|{d}", .{ up, @as(i64, @intCast(wi)) - 2, count_sw[si][wi] }));
            }
            var rows: i64 = 0;
            for (0..3) |g| {
                rows += count_sg[si][g];
                if (count_sg[si][g] == 0) continue;
                try want_pre_folds.append(arena, try std.fmt.allocPrint(arena, "{s}|{d}|{d}|{d}", .{ up, g, max_w_sg[si][g], sum_h_sg[si][g] }));
            }
            try want_pre_sum.append(arena, try std.fmt.allocPrint(arena, "{s}|{d}|{d}|{d}", .{ up, rows, max_sq_s[si], min_sq_s[si] }));
            for (0..3) |g| {
                if (count_sg[si][g] == 0) continue;
                try want_key_int.append(arena, try std.fmt.allocPrint(arena, "{s}|{d}|{d}|{d}|{s}", .{ up, g, count_sg[si][g], sum_sg[si][g], w }));
                try want_read_int.append(arena, try std.fmt.allocPrint(arena, "{s}|{d}|{d}", .{ up, g + 1, count_sg[si][g] }));
            }
            for (tags, 0..) |tag, ui| {
                if (count_su[si][ui] == 0) continue;
                try want_two_strings.append(arena, try std.fmt.allocPrint(arena, "{s}|{s}|{d}", .{ w, tag, count_su[si][ui] }));
                try want_concat.append(arena, try std.fmt.allocPrint(arena, "{s}{s}|{d}|{d}", .{ w, tag, count_su[si][ui], sum_su[si][ui] }));
            }
        }
        const cases = .{
            .{ "SELECT upper(s) AS k, g, COUNT(*) AS c, SUM(x) AS sx, MIN(s) AS ms FROM t GROUP BY k, g ORDER BY c DESC LIMIT 100", &want_key_int },
            .{ "SELECT upper(s) AS k, g + 1 AS g1, COUNT(*) AS c FROM t GROUP BY k, g1 ORDER BY c DESC LIMIT 100", &want_read_int },
            .{ "SELECT lower(s) AS a, u, COUNT(*) AS c FROM t GROUP BY a, u ORDER BY c DESC LIMIT 100", &want_two_strings },
            .{ "SELECT concat(s, u) AS k, COUNT(*) AS c, SUM(x) AS sx FROM t GROUP BY k ORDER BY c DESC LIMIT 100", &want_concat },
            .{ "SELECT upper(s) AS k, w, COUNT(*) AS c FROM t GROUP BY k, w ORDER BY c DESC LIMIT 100", &want_wide },
            .{ "SELECT upper(s) AS k, w DIV 1000003 AS h, COUNT(*) AS c FROM t GROUP BY k, h ORDER BY c DESC LIMIT 100", &want_pre_key },
            .{ "SELECT upper(s) AS k, COUNT(*) AS c, MAX(x * x - g) AS mq, MIN(x * x - g) AS nq FROM t GROUP BY k ORDER BY c DESC LIMIT 100", &want_pre_sum },
            .{ "SELECT upper(s) AS k, g, MAX(w) AS mw, SUM(w DIV 1000003) AS sh FROM t GROUP BY k, g ORDER BY mw DESC LIMIT 100", &want_pre_folds },
            .{ "SELECT upper(s) AS k, x DIV g AS q, COUNT(*) AS c FROM t GROUP BY k, q ORDER BY c DESC LIMIT 100", &want_nullable_pre },
        };
        inline for (cases) |c| {
            try std.testing.expect(try planContains(allocator, db, c[0], "(V2 "));
            const got = try sortedRowTexts(allocator, arena, db, c[0]);
            std.mem.sort([]const u8, c[1].items, {}, textLess);
            try std.testing.expectEqual(c[1].items.len, got.len);
            for (c[1].items, got) |w, g| try std.testing.expectEqualStrings(w, g);
        }
    }
}

test "grouped engines filter on computed WHERE operands after the Compute" {
    // A WHERE conjunct over a computed operand (`x + 1 > 5`,
    // `octet_length(u) > 1`) filters the Compute's output inside the grouped
    // engine, while the other conjuncts still fuse into the scan. The rows
    // it reads stay whole through a collapse, a hashed key it reads keeps
    // its bytes, and an unordered LIMIT doesn't pick its groups from rows
    // the filter would drop. A key over a column that arrives as byte
    // lengths evaluates after the filter, on the lengths of the rows kept.
    const allocator = std.testing.allocator;
    const words = [_][]const u8{ "a", "bb", "cccc", "ddddd", "", "eeeeeeee" };
    const uppers = [_][]const u8{ "A", "BB", "CCCC", "DDDDD", "", "EEEEEEEE" };
    const tags = [_][]const u8{ "p", "qq", "r" };
    const n = 384;
    const block_rows = 8;
    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .row_group_size = 64, .auto_flush_secs = 0 });
        defer db.close();
        try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, g INT NOT NULL, s STRING NOT NULL, u STRING NOT NULL, x INT NOT NULL, w BIGINT NOT NULL, b INT NOT NULL)");
        var count_x = [_]i64{0} ** words.len;
        var sum_x = [_]i64{0} ** words.len;
        var count_qq_not1 = [_]i64{0} ** words.len;
        var count_qq_or_a = [_]i64{0} ** words.len;
        var count_not_qq = [_][tags.len]i64{.{ 0, 0, 0 }} ** words.len;
        var count_w = [_][3]i64{.{ 0, 0, 0 }} ** words.len;
        var sum_w = [_][3]i64{.{ 0, 0, 0 }} ** words.len;
        for (0..2) |half| {
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(allocator);
            try buf.appendSlice(allocator, "INSERT INTO t (id, g, s, u, x, w, b) VALUES ");
            for (half * n / 2..(half + 1) * n / 2) |i| {
                const si = i % words.len;
                const ui = (i / 7) % tags.len;
                const g = i % 3;
                const x: i64 = @intCast(i % 11);
                const wi = i % 4;
                if (i != half * n / 2) try buf.appendSlice(allocator, ",");
                try buf.print(allocator, "({d},{d},'{s}','{s}',{d},{d},{d})", .{ i, g, words[si], tags[ui], x, wideValue(wi), i / block_rows });
                if (x >= 5) {
                    count_x[si] += 1;
                    sum_x[si] += x;
                }
                if (ui == 1 and g != 1) count_qq_not1[si] += 1;
                if (ui == 1 or si == 0) count_qq_or_a[si] += 1;
                if (ui != 1) count_not_qq[si][ui] += 1;
                if (wi >= 2) {
                    count_w[si][g] += 1;
                    sum_w[si][g] += wideValue(wi);
                }
            }
            try exec(allocator, db, buf.items);
            // The first half flushes; the second stays in the memtable.
            if (half == 0) try (try db.openTable("t", .{})).flush();
        }

        var arena_state = std.heap.ArenaAllocator.init(allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        var want_sum: std.ArrayList([]const u8) = .empty;
        var want_mixed: std.ArrayList([]const u8) = .empty;
        var want_or: std.ArrayList([]const u8) = .empty;
        var want_strings: std.ArrayList([]const u8) = .empty;
        var want_wide: std.ArrayList([]const u8) = .empty;
        var want_lengths: std.ArrayList([]const u8) = .empty;
        for (words, uppers, 0..) |w, up, si| {
            if (count_x[si] != 0) try want_sum.append(arena, try std.fmt.allocPrint(arena, "{s}|{d}|{d}", .{ w, count_x[si], sum_x[si] }));
            if (count_x[si] != 0) try want_lengths.append(arena, try std.fmt.allocPrint(arena, "{d}|{d}", .{ w.len, count_x[si] }));
            if (count_qq_not1[si] != 0) try want_mixed.append(arena, try std.fmt.allocPrint(arena, "{s}|{d}", .{ up, count_qq_not1[si] }));
            if (count_qq_or_a[si] != 0) try want_or.append(arena, try std.fmt.allocPrint(arena, "{s}|{d}", .{ w, count_qq_or_a[si] }));
            for (tags, 0..) |tag, ui| {
                if (count_not_qq[si][ui] == 0) continue;
                try want_strings.append(arena, try std.fmt.allocPrint(arena, "{s}|{s}|{d}", .{ w, tag, count_not_qq[si][ui] }));
            }
            for (0..3) |g| {
                if (count_w[si][g] == 0) continue;
                try want_wide.append(arena, try std.fmt.allocPrint(arena, "{s}|{d}|{d}", .{ up, g, sum_w[si][g] }));
            }
        }
        const cases = .{
            .{ "SELECT s, COUNT(*) AS c, SUM(x) AS sx FROM t WHERE x + 1 > 5 GROUP BY s ORDER BY c DESC LIMIT 100", &want_sum },
            .{ "SELECT upper(s) AS k, COUNT(*) AS c FROM t WHERE octet_length(u) > 1 AND g <> 1 GROUP BY k ORDER BY c DESC LIMIT 100", &want_mixed },
            .{ "SELECT s, COUNT(*) AS c FROM t WHERE octet_length(u) > 1 OR s = 'a' GROUP BY s ORDER BY c DESC LIMIT 100", &want_or },
            .{ "SELECT lower(s) AS a, u, COUNT(*) AS c FROM t WHERE upper(u) <> 'QQ' GROUP BY a, u ORDER BY c DESC LIMIT 100", &want_strings },
            .{ "SELECT upper(s) AS k, g, SUM(w) AS sw FROM t WHERE w DIV 1000003 >= 0 GROUP BY k, g ORDER BY sw DESC LIMIT 100", &want_wide },
            .{ "SELECT octet_length(s) AS l, COUNT(*) AS c FROM t WHERE x + 1 > 5 GROUP BY l ORDER BY c DESC LIMIT 100", &want_lengths },
        };
        inline for (cases) |c| {
            errdefer std.debug.print("dop {d}: {s}\n", .{ dop, c[0] });
            try std.testing.expect(try planContains(allocator, db, c[0], "residual filter"));
            const got = try sortedRowTexts(allocator, arena, db, c[0]);
            std.mem.sort([]const u8, c[1].items, {}, textLess);
            try std.testing.expectEqual(c[1].items.len, got.len);
            for (c[1].items, got) |w, g| try std.testing.expectEqualStrings(w, g);
        }

        // The first blocks in scan order all fail the filter.
        const unordered = "SELECT b, COUNT(*) AS c FROM t WHERE b + 1 > 40 GROUP BY b LIMIT 1";
        errdefer std.debug.print("dop {d}: {s}\n", .{ dop, unordered });
        try std.testing.expect(try planContains(allocator, db, unordered, "residual filter"));
        const got = try sortedRowTexts(allocator, arena, db, unordered);
        try std.testing.expectEqual(@as(usize, 1), got.len);
        var cells = std.mem.splitScalar(u8, got[0], '|');
        try std.testing.expect(try std.fmt.parseInt(i64, cells.next().?, 10) >= 40);
        try std.testing.expectEqualStrings(std.fmt.comptimePrint("{d}", .{block_rows}), cells.next().?);
    }
}

/// Spread past what a direct map over a row group's combinations covers.
fn wideValue(i: usize) i64 {
    return (@as(i64, @intCast(i)) - 2) * 1_000_003;
}

fn textLess(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

/// Each result row as its cells joined by `|`, sorted.
fn sortedRowTexts(allocator: std.mem.Allocator, arena: std.mem.Allocator, db: *thindb.Database, sql: []const u8) ![]const []const u8 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var rows: std.ArrayList([]const u8) = .empty;
    while (try q.next()) |b| {
        for (0..b.row_count) |r| {
            var line: std.ArrayList(u8) = .empty;
            for (b.values, 0..) |col, ci| {
                if (ci != 0) try line.append(arena, '|');
                if (!col.isValid(r)) {
                    try line.appendSlice(arena, "NULL");
                    continue;
                }
                switch (col.data) {
                    .varchar, .string => |sv| try line.appendSlice(arena, sv.rowBytes(r)),
                    else => try line.print(arena, "{d}", .{@as(i64, @intFromFloat((try cellNumber(col, r)).?))}),
                }
            }
            try rows.append(arena, line.items);
        }
    }
    std.mem.sort([]const u8, rows.items, {}, textLess);
    return rows.items;
}
