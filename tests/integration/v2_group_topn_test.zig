//! Focused battery for the V2 generic group-topN engine (Q15 shape).

const std = @import("std");
const thindb = @import("thindb");

test "memory: SQL budgets cover wide sort and V2 group output allocations" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(a, io, tmp.dir, .{
        .query_memory_budget = 16 * 1024,
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

// 200K single-row groups at max_dop=4 put the result emit on three range
// workers (65536+ groups each), and the pipeline hands each range out as
// its own batch. The BIGINT key takes the packed emit; the VARCHAR key the
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
        try std.testing.expect(batches >= 3);
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
const GdRow = struct { id: i64, g: i32, s: ?[]const u8, c: []const u8, d: ?f64, v: ?i32 };

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

fn checkGroupedDistinct(allocator: std.mem.Allocator, db: *thindb.Database, e: GdExpected, comptime having: []const u8, route: []const u8) !void {
    const multi_sql = "SELECT g, COUNT(DISTINCT s) AS ds, COUNT(DISTINCT c) AS dc, COUNT(DISTINCT d) AS dd, COUNT(*) AS n FROM gd GROUP BY g" ++ having;
    const case_sql = "SELECT g, COUNT(DISTINCT CASE WHEN v > 0 THEN s END) AS cs, COUNT(DISTINCT IF(v > 0, id % 97, NULL)) AS ci FROM gd GROUP BY g" ++ having;
    inline for (.{ multi_sql, case_sql }) |sql| {
        errdefer std.debug.print("route={s} query: {s}\n", .{ route, sql });
        try std.testing.expect(try planContains(allocator, db, sql, route));
        var q = try runSql(allocator, db, sql);
        defer q.deinit();
        var seen = [_]bool{false} ** GD_GROUPS;
        var groups: usize = 0;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |r| {
                const g: usize = @intCast(batch.values[0].data.int[r]);
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
        try checkGroupedDistinct(allocator, db, expected, "", "lowcard");
        try checkGroupedDistinct(allocator, db, expected, " HAVING COUNT(*) > 0", "V2 group-topN");
        // The low-NDV non-nullable string folds as dict codes, the nullable
        // one by bytes; the CASE/IF inputs are derived string and int values.
        try std.testing.expect(try planContains(allocator, db, "SELECT g, COUNT(DISTINCT s), COUNT(DISTINCT c), COUNT(DISTINCT d) FROM gd GROUP BY g", "distinct string,coded,float"));
        try std.testing.expect(try planContains(allocator, db, "SELECT g, COUNT(DISTINCT CASE WHEN v > 0 THEN s END), COUNT(DISTINCT IF(v > 0, id % 97, NULL)) FROM gd GROUP BY g", "distinct string,int"));
        try checkKeyDistinct(allocator, db, rows, live, &c_pool, false, "", "lowcard");
        try checkKeyDistinct(allocator, db, rows, live, &c_pool, true, "", "V2 group-topN");
        try checkKeyDistinct(allocator, db, rows, live, &c_pool, true, " HAVING COUNT(*) > 0", "V2 group-topN");

        try exec(allocator, db, "DELETE FROM gd WHERE id % 17 = 0");
        try t.insert(rows[(batches - 1) * batch_rows ..]);
        // The DELETE ran before the memtable batch arrived.
        for (live, 0..) |*keep, i| keep.* = i >= (batches - 1) * batch_rows or i % 17 != 0;
        try expected.compute(allocator, rows, live);
        try checkGroupedDistinct(allocator, db, expected, "", "lowcard");
        try checkGroupedDistinct(allocator, db, expected, " HAVING COUNT(*) > 0", "V2 group-topN");

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
            .query_memory_budget = budget,
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
