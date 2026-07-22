//! Focused battery for the V2 generic group-topN engine (Q15 shape).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

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
