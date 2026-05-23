//! Late-materialization correctness (ClickBench Q23 shape).
//!
//! `SELECT <wide> FROM t WHERE <like> [ORDER BY <intkey>] LIMIT k [OFFSET m]`
//! over a single base table compiles to a `LateScan` that decodes only the
//! filter + ORDER BY columns through the filter + bounded top-k, then fetches
//! the wide output columns for the survivors. These tests assert the rows +
//! values exactly equal the independently-computed expected result, and that
//! the planner actually picked the late-mat plan (EXPLAIN shows `LateScan`).
//!
//! Covered: (a) basic, (b) a deleted/tombstoned row among the matches,
//! (c) memtable-resident rows AND segment rows in one query, (d) LIMIT with
//! OFFSET, (e) a no-ORDER-BY limit.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSql = helpers.runSql;

const Row = struct { id: i64, sortk: i32, name: []const u8, payload: []const u8 };

fn createTable(allocator: std.mem.Allocator, db: anytype) !void {
    // Order key is `id` (segments sort by it). `sortk` is the ORDER BY key,
    // deliberately different from the order key so the sort is real. `name`
    // feeds the LIKE; `payload` is an extra wide column only the output needs.
    try exec(allocator, db,
        \\CREATE TABLE t (
        \\  id BIGINT PRIMARY KEY,
        \\  sortk INT NOT NULL,
        \\  name VARCHAR(64) NOT NULL,
        \\  payload VARCHAR(64) NOT NULL
        \\)
    );
}

fn insertRows(allocator: std.mem.Allocator, db: anytype, rows: []const Row) !void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO t (id, sortk, name, payload) VALUES ");
    for (rows, 0..) |r, i| {
        if (i > 0) try buf.appendSlice(allocator, ", ");
        var line: [256]u8 = undefined;
        const s = try std.fmt.bufPrint(&line, "({d}, {d}, '{s}', '{s}')", .{ r.id, r.sortk, r.name, r.payload });
        try buf.appendSlice(allocator, s);
    }
    try exec(allocator, db, buf.items);
}

/// One materialized result row (payload + sortk are enough to verify both the
/// fetched wide column and the ordering).
const Out = struct { id: i64, sortk: i32, payload: []const u8 };

/// Run a SELECT and collect (id, sortk, payload) in emit order. The SELECT
/// must project exactly those three columns in that order.
fn collect(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !std.ArrayList(Out) {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(Out) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        const ids = b.values[0].data.bigint;
        const sks = b.values[1].data.int;
        const pl = b.values[2].data.varchar;
        for (0..b.row_count) |i| {
            const owned = try allocator.dupe(u8, pl.rowBytes(i));
            try out.append(allocator, .{ .id = ids[i], .sortk = sks[i], .payload = owned });
        }
    }
    return out;
}

fn freeOut(allocator: std.mem.Allocator, out: *std.ArrayList(Out)) void {
    for (out.items) |o| allocator.free(o.payload);
    out.deinit(allocator);
}

fn expectPlanHasLateScan(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !void {
    var buf: [512]u8 = undefined;
    const explain_sql = try std.fmt.bufPrint(&buf, "EXPLAIN {s}", .{sql});
    var q = try runSql(allocator, db, explain_sql);
    defer q.deinit();
    var found = false;
    while (try q.next()) |b| {
        const sv = b.values[0].data.string;
        for (0..b.row_count) |i| {
            if (std.mem.indexOf(u8, sv.rowBytes(i), "LateScan") != null) found = true;
        }
    }
    try std.testing.expect(found);
}

test "late-mat (a): basic SELECT * shape over a single segment" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try createTable(allocator, db);
    try insertRows(allocator, db, &.{
        .{ .id = 1, .sortk = 50, .name = "google-search", .payload = "P1" },
        .{ .id = 2, .sortk = 10, .name = "bing", .payload = "P2" },
        .{ .id = 3, .sortk = 30, .name = "the-google-thing", .payload = "P3" },
        .{ .id = 4, .sortk = 20, .name = "duckduckgo", .payload = "P4" },
        .{ .id = 5, .sortk = 40, .name = "googleplex", .payload = "P5" },
    });
    const t = try db.openTable("t", .{});
    try t.flush();

    const sql = "SELECT id, sortk, payload FROM t WHERE name LIKE '%google%' ORDER BY sortk ASC LIMIT 10";
    try expectPlanHasLateScan(allocator, db, sql);

    var out = try collect(allocator, db, sql);
    defer freeOut(allocator, &out);

    // Matches: id 1 (50), 3 (30), 5 (40). Sorted by sortk ASC → 3, 5, 1.
    try std.testing.expectEqual(@as(usize, 3), out.items.len);
    try std.testing.expectEqual(@as(i64, 3), out.items[0].id);
    try std.testing.expectEqualStrings("P3", out.items[0].payload);
    try std.testing.expectEqual(@as(i64, 5), out.items[1].id);
    try std.testing.expectEqualStrings("P5", out.items[1].payload);
    try std.testing.expectEqual(@as(i64, 1), out.items[2].id);
    try std.testing.expectEqualStrings("P1", out.items[2].payload);
}

test "late-mat (b): a deleted/tombstoned row among the matches is skipped" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try createTable(allocator, db);
    try insertRows(allocator, db, &.{
        .{ .id = 1, .sortk = 50, .name = "google-a", .payload = "P1" },
        .{ .id = 2, .sortk = 10, .name = "google-b", .payload = "P2" },
        .{ .id = 3, .sortk = 30, .name = "nope", .payload = "P3" },
        .{ .id = 4, .sortk = 20, .name = "google-c", .payload = "P4" },
        .{ .id = 5, .sortk = 40, .name = "google-d", .payload = "P5" },
        .{ .id = 6, .sortk = 5, .name = "google-e", .payload = "P6" },
    });
    const t = try db.openTable("t", .{});
    try t.flush();
    // Tombstone two matching rows (id 2 and id 4).
    try exec(allocator, db, "DELETE FROM t WHERE id = 2");
    try exec(allocator, db, "DELETE FROM t WHERE id = 4");

    const sql = "SELECT id, sortk, payload FROM t WHERE name LIKE 'google%' ORDER BY sortk ASC LIMIT 10";
    try expectPlanHasLateScan(allocator, db, sql);

    var out = try collect(allocator, db, sql);
    defer freeOut(allocator, &out);

    // Live matches: 1 (50), 5 (40), 6 (5). Sorted ASC → 6, 5, 1.
    try std.testing.expectEqual(@as(usize, 3), out.items.len);
    try std.testing.expectEqual(@as(i64, 6), out.items[0].id);
    try std.testing.expectEqualStrings("P6", out.items[0].payload);
    try std.testing.expectEqual(@as(i64, 5), out.items[1].id);
    try std.testing.expectEqualStrings("P5", out.items[1].payload);
    try std.testing.expectEqual(@as(i64, 1), out.items[2].id);
    try std.testing.expectEqualStrings("P1", out.items[2].payload);
}

test "late-mat (c): mix of segment-resident and memtable-resident matches" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try createTable(allocator, db);
    // First batch → flush to a segment.
    try insertRows(allocator, db, &.{
        .{ .id = 1, .sortk = 50, .name = "google-seg", .payload = "S1" },
        .{ .id = 2, .sortk = 35, .name = "google-seg", .payload = "S2" },
        .{ .id = 3, .sortk = 15, .name = "other", .payload = "S3" },
    });
    const t = try db.openTable("t", .{});
    try t.flush();
    // Second batch stays in the memtable (no flush).
    try insertRows(allocator, db, &.{
        .{ .id = 4, .sortk = 25, .name = "google-mem", .payload = "M4" },
        .{ .id = 5, .sortk = 5, .name = "google-mem", .payload = "M5" },
        .{ .id = 6, .sortk = 45, .name = "other", .payload = "M6" },
    });

    const sql = "SELECT id, sortk, payload FROM t WHERE name LIKE 'google%' ORDER BY sortk ASC LIMIT 10";
    try expectPlanHasLateScan(allocator, db, sql);

    var out = try collect(allocator, db, sql);
    defer freeOut(allocator, &out);

    // Matches: seg 1(50),2(35); mem 4(25),5(5). Sorted ASC → 5, 4, 2, 1.
    try std.testing.expectEqual(@as(usize, 4), out.items.len);
    try std.testing.expectEqual(@as(i64, 5), out.items[0].id);
    try std.testing.expectEqualStrings("M5", out.items[0].payload);
    try std.testing.expectEqual(@as(i64, 4), out.items[1].id);
    try std.testing.expectEqualStrings("M4", out.items[1].payload);
    try std.testing.expectEqual(@as(i64, 2), out.items[2].id);
    try std.testing.expectEqualStrings("S2", out.items[2].payload);
    try std.testing.expectEqual(@as(i64, 1), out.items[3].id);
    try std.testing.expectEqualStrings("S1", out.items[3].payload);
}

test "late-mat (d): LIMIT with OFFSET" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try createTable(allocator, db);
    try insertRows(allocator, db, &.{
        .{ .id = 1, .sortk = 50, .name = "g1", .payload = "P1" },
        .{ .id = 2, .sortk = 10, .name = "g2", .payload = "P2" },
        .{ .id = 3, .sortk = 30, .name = "g3", .payload = "P3" },
        .{ .id = 4, .sortk = 20, .name = "g4", .payload = "P4" },
        .{ .id = 5, .sortk = 40, .name = "g5", .payload = "P5" },
    });
    const t = try db.openTable("t", .{});
    try t.flush();

    // All five match `g%`. Sorted ASC by sortk → 2(10),4(20),3(30),5(40),1(50).
    // LIMIT 2 OFFSET 1 → 4(20), 3(30).
    const sql = "SELECT id, sortk, payload FROM t WHERE name LIKE 'g%' ORDER BY sortk ASC LIMIT 2 OFFSET 1";
    try expectPlanHasLateScan(allocator, db, sql);

    var out = try collect(allocator, db, sql);
    defer freeOut(allocator, &out);

    try std.testing.expectEqual(@as(usize, 2), out.items.len);
    try std.testing.expectEqual(@as(i64, 4), out.items[0].id);
    try std.testing.expectEqualStrings("P4", out.items[0].payload);
    try std.testing.expectEqual(@as(i64, 3), out.items[1].id);
    try std.testing.expectEqualStrings("P3", out.items[1].payload);
}

test "late-mat (e): no-ORDER-BY bounded limit" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try createTable(allocator, db);
    try insertRows(allocator, db, &.{
        .{ .id = 1, .sortk = 50, .name = "google-1", .payload = "P1" },
        .{ .id = 2, .sortk = 10, .name = "nope", .payload = "P2" },
        .{ .id = 3, .sortk = 30, .name = "google-2", .payload = "P3" },
        .{ .id = 4, .sortk = 20, .name = "google-3", .payload = "P4" },
    });
    const t = try db.openTable("t", .{});
    try t.flush();

    const sql = "SELECT id, sortk, payload FROM t WHERE name LIKE 'google%' LIMIT 2";
    try expectPlanHasLateScan(allocator, db, sql);

    var out = try collect(allocator, db, sql);
    defer freeOut(allocator, &out);

    // No ORDER BY: rows come in scan order (id ascending within the segment).
    // Matches in order: 1, 3, 4. LIMIT 2 → first two by scan order: 1, 3.
    try std.testing.expectEqual(@as(usize, 2), out.items.len);
    try std.testing.expectEqual(@as(i64, 1), out.items[0].id);
    try std.testing.expectEqualStrings("P1", out.items[0].payload);
    try std.testing.expectEqual(@as(i64, 3), out.items[1].id);
    try std.testing.expectEqualStrings("P3", out.items[1].payload);
}

test "late-mat: SELECT * (full projection) returns every column for survivors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try createTable(allocator, db);
    try insertRows(allocator, db, &.{
        .{ .id = 1, .sortk = 50, .name = "google", .payload = "P1" },
        .{ .id = 2, .sortk = 10, .name = "nope", .payload = "P2" },
        .{ .id = 3, .sortk = 30, .name = "google", .payload = "P3" },
    });
    const t = try db.openTable("t", .{});
    try t.flush();

    const sql = "SELECT * FROM t WHERE name LIKE '%google%' ORDER BY sortk ASC LIMIT 10";
    try expectPlanHasLateScan(allocator, db, sql);

    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqual(@as(usize, 4), schema.len);

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    var names: std.ArrayList([]u8) = .empty;
    defer {
        for (names.items) |n| allocator.free(n);
        names.deinit(allocator);
    }
    while (try q.next()) |b| {
        const id_col = b.columnView("id").?.data.bigint;
        const name_col = b.columnView("name").?.data.varchar;
        for (0..b.row_count) |i| {
            try ids.append(allocator, id_col[i]);
            try names.append(allocator, try allocator.dupe(u8, name_col.rowBytes(i)));
        }
    }
    // Matches sorted ASC by sortk → id 3 (30), id 1 (50).
    try std.testing.expectEqual(@as(usize, 2), ids.items.len);
    try std.testing.expectEqual(@as(i64, 3), ids.items[0]);
    try std.testing.expectEqualStrings("google", names.items[0]);
    try std.testing.expectEqual(@as(i64, 1), ids.items[1]);
    try std.testing.expectEqualStrings("google", names.items[1]);
}
