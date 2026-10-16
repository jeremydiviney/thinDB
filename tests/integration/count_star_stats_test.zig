//! Metadata-only COUNT(*) (`MetaAggStats`): the bare `SELECT COUNT(*) FROM t`
//! shape answers from the live manifest's row counts − tombstone counts +
//! unflushed memtable rows, without a scan. The count must track every table
//! state a scan would see — flushed segments, deletes, memtable-only rows —
//! so each step here cross-checks the metadata answer against a forced-scan
//! count (an always-true WHERE defeats the shape gate). Guards the historical
//! over-count bug where a metadata shortcut summed superseded segments.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

fn countVia(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !i64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    const batch = (try q.next()) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    const v = batch.values[0].data.bigint[0];
    while (try q.next()) |_| {}
    return v;
}

fn expectCount(allocator: std.mem.Allocator, db: anytype, expected: i64) !void {
    const bare = try countVia(allocator, db, "SELECT COUNT(*) FROM t");
    const scanned = try countVia(allocator, db, "SELECT COUNT(*) FROM t WHERE id >= 0");
    try std.testing.expectEqual(expected, bare);
    try std.testing.expectEqual(expected, scanned);
}

test "bare COUNT(*) tracks segments, tombstones, and memtable rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL)");

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO t (id, k) VALUES ");
    var line: [48]u8 = undefined;
    var id: i64 = 0;
    while (id < 1000) : (id += 1) {
        if (id != 0) try buf.appendSlice(allocator, ", ");
        try buf.appendSlice(allocator, try std.fmt.bufPrint(&line, "({d}, {d})", .{ id, @mod(id, 7) }));
    }
    try exec(allocator, db, buf.items);

    // Memtable only — nothing flushed yet.
    try expectCount(allocator, db, 1000);

    // Flushed segment.
    const t = try db.openTable("t", .{});
    try t.flush();
    try expectCount(allocator, db, 1000);

    // Deletes → tombstones against the flushed segment.
    try exec(allocator, db, "DELETE FROM t WHERE id < 150");
    try expectCount(allocator, db, 850);

    // Fresh unflushed rows on top of segment + tombstones.
    try exec(allocator, db, "INSERT INTO t (id, k) VALUES (2001, 1), (2002, 2), (2003, 3)");
    try expectCount(allocator, db, 853);

    // Flush again: two segments + tombstones.
    try t.flush();
    try expectCount(allocator, db, 853);
}

fn row3(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![3]i64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    const batch = (try q.next()) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    const out: [3]i64 = .{
        batch.values[0].data.bigint[0],
        batch.values[1].data.bigint[0],
        batch.values[2].data.bigint[0],
    };
    while (try q.next()) |_| {}
    return out;
}

test "mixed COUNT/MIN/MAX metadata lane matches the scan path with tombstones" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL)");

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO t (id, k) VALUES ");
    var line: [48]u8 = undefined;
    var id: i64 = 0;
    while (id < 500) : (id += 1) {
        if (id != 0) try buf.appendSlice(allocator, ", ");
        try buf.appendSlice(allocator, try std.fmt.bufPrint(&line, "({d}, {d})", .{ id, 1000 - id }));
    }
    try exec(allocator, db, buf.items);
    const t = try db.openTable("t", .{});
    try t.flush();

    const meta_sql = "SELECT COUNT(*), MIN(k), MAX(id) FROM t";
    const scan_sql = "SELECT COUNT(*), MIN(k), MAX(id) FROM t WHERE id >= 0";

    // Tombstone-free: metadata lane must equal the scan path.
    {
        const meta = try row3(allocator, db, meta_sql);
        const scanned = try row3(allocator, db, scan_sql);
        try std.testing.expectEqual(scanned, meta);
        try std.testing.expectEqual([3]i64{ 500, 501, 499 }, meta);
    }

    // COUNT(col) on a non-nullable column counts every live row.
    try std.testing.expectEqual(
        @as(i64, 500),
        try countVia(allocator, db, "SELECT COUNT(k) FROM t"),
    );

    // Delete the row holding MIN(k) (id=499 → k=501): the stale segment
    // stats still say 501, so the lane decodes the row group's live rows.
    try exec(allocator, db, "DELETE FROM t WHERE id >= 499");
    {
        const meta = try row3(allocator, db, meta_sql);
        const scanned = try row3(allocator, db, scan_sql);
        try std.testing.expectEqual(scanned, meta);
        try std.testing.expectEqual([3]i64{ 499, 502, 498 }, meta);
        try std.testing.expect(try metaBlocksDecoded(allocator, db, meta_sql) != null);
    }

    // Counts stay metadata-served under tombstones.
    try std.testing.expectEqual(
        @as(i64, 499),
        try countVia(allocator, db, "SELECT COUNT(k) FROM t"),
    );
}

const SumAvgRow = struct {
    sum_k: i64,
    avg_v: f64,
    count_v: i64,
    sum_f: f64,
};

fn sumAvgRow(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !SumAvgRow {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    const batch = (try q.next()) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    const out: SumAvgRow = .{
        .sum_k = batch.values[0].data.bigint[0],
        .avg_v = batch.values[1].data.double[0],
        .count_v = batch.values[2].data.bigint[0],
        .sum_f = batch.values[3].data.double[0],
    };
    while (try q.next()) |_| {}
    return out;
}

test "SUM/AVG/COUNT(nullable) metadata lane matches the scan path across segments, tombstones and memtable rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, v INT, f DOUBLE NOT NULL)");

    // Two batches of 300 with a flush between → two segments. v is NULL on
    // every id % 3 == 0 row.
    var id: i64 = 0;
    while (id < 600) : (id += 300) {
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(allocator);
        try buf.appendSlice(allocator, "INSERT INTO t (id, k, v, f) VALUES ");
        var line: [96]u8 = undefined;
        var i: i64 = id;
        while (i < id + 300) : (i += 1) {
            if (i != id) try buf.appendSlice(allocator, ", ");
            if (@mod(i, 3) == 0) {
                try buf.appendSlice(allocator, try std.fmt.bufPrint(&line, "({d}, {d}, NULL, {d}.5)", .{ i, i * 2, i }));
            } else {
                try buf.appendSlice(allocator, try std.fmt.bufPrint(&line, "({d}, {d}, {d}, {d}.5)", .{ i, i * 2, i, i }));
            }
        }
        try exec(allocator, db, buf.items);
        const t = try db.openTable("t", .{});
        try t.flush();
    }

    const meta_sql = "SELECT SUM(k), AVG(v), COUNT(v), SUM(f) FROM t";
    const scan_sql = "SELECT SUM(k), AVG(v), COUNT(v), SUM(f) FROM t WHERE id >= 0";

    {
        const meta = try sumAvgRow(allocator, db, meta_sql);
        const scanned = try sumAvgRow(allocator, db, scan_sql);
        try std.testing.expectEqual(scanned, meta);
        // Σ 2·id = 359400; v non-null on 400 rows summing 120000 → AVG 300;
        // Σ (id + 0.5) = 180000.
        try std.testing.expectEqual(@as(i64, 359400), meta.sum_k);
        try std.testing.expectEqual(@as(f64, 300.0), meta.avg_v);
        try std.testing.expectEqual(@as(i64, 400), meta.count_v);
        try std.testing.expectEqual(@as(f64, 180000.0), meta.sum_f);
    }

    // Tombstones make the first segment's stored sums/null_counts stale, so
    // the lane decodes its row group (one block per column: k, v, f).
    try exec(allocator, db, "DELETE FROM t WHERE id < 9");
    {
        const meta = try sumAvgRow(allocator, db, meta_sql);
        const scanned = try sumAvgRow(allocator, db, scan_sql);
        try std.testing.expectEqual(scanned, meta);
        // Removed ids 0..8: Σ2·id drops 72; v drops ids {1,2,4,5,7,8} = 27
        // over 6 rows; f drops 36 + 9·0.5.
        try std.testing.expectEqual(@as(i64, 359400 - 72), meta.sum_k);
        try std.testing.expectEqual(@as(i64, 394), meta.count_v);
        try std.testing.expectEqual(@as(f64, (120000.0 - 27.0) / 394.0), meta.avg_v);
        try std.testing.expectEqual(@as(f64, 180000.0 - 40.5), meta.sum_f);
        try std.testing.expectEqual(@as(?usize, 3), try metaBlocksDecoded(allocator, db, meta_sql));
    }

    // Unflushed memtable rows fold in directly.
    try exec(allocator, db, "INSERT INTO t (id, k, v, f) VALUES (1000, 10, 5, 0.5)");
    {
        const meta = try sumAvgRow(allocator, db, meta_sql);
        const scanned = try sumAvgRow(allocator, db, scan_sql);
        try std.testing.expectEqual(scanned, meta);
        try std.testing.expectEqual(@as(i64, 359400 - 72 + 10), meta.sum_k);
        try std.testing.expectEqual(@as(i64, 395), meta.count_v);
        try std.testing.expectEqual(@as(?usize, 3), try metaBlocksDecoded(allocator, db, meta_sql));
    }
}

/// The `MetaAggStats` lane's decoded-block count from `sql`'s EXPLAIN, or
/// null when another plan answers `sql`.
fn metaBlocksDecoded(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !?usize {
    const explain_sql = try std.fmt.allocPrint(allocator, "EXPLAIN {s}", .{sql});
    defer allocator.free(explain_sql);
    var plan = try runSql(allocator, db, explain_sql);
    defer plan.deinit();
    var found: ?usize = null;
    while (try plan.next()) |b| {
        for (0..b.row_count) |i| {
            const text = b.values[0].data.string.rowBytes(i);
            if (std.mem.indexOf(u8, text, "MetaAggStats") == null) continue;
            const end = std.mem.indexOf(u8, text, " row-group blocks decoded") orelse return error.TestUnexpectedResult;
            const start = (std.mem.lastIndexOfScalar(u8, text[0..end], ' ') orelse return error.TestUnexpectedResult) + 1;
            found = try std.fmt.parseInt(usize, text[start..end], 10);
        }
    }
    return found;
}

fn optionalInts(comptime n: usize, allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![n]?i64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    const batch = (try q.next()) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    var out: [n]?i64 = undefined;
    for (&out, batch.values[0..n]) |*o, v| {
        o.* = if (!v.isValid(0)) null else switch (v.data) {
            .bigint => |d| d[0],
            .int => |d| d[0],
            else => return error.TestUnexpectedResult,
        };
    }
    while (try q.next()) |_| {}
    return out;
}

test "MIN/MAX decode only the row groups with tombstones whose bounds can still win" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 100 });
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, v INT, w INT)");
    const t = try db.openTable("t", .{});

    // Ten row groups of 100 ids. k = id; v = id but NULL over row groups 3
    // and 9; w is NULL everywhere.
    const Row = struct { id: i64, k: i64, v: ?i32, w: ?i32 };
    var rows: [1000]Row = undefined;
    for (&rows, 0..) |*r, i| {
        const rg = i / 100;
        r.* = .{ .id = @intCast(i), .k = @intCast(i), .v = if (rg == 3 or rg == 9) null else @intCast(i), .w = null };
    }
    try t.insert(rows[0..]);
    try t.flush();

    const sql = "SELECT MIN(k), MAX(k), MIN(v), MAX(v), MIN(w), MAX(w) FROM t";
    const scan_sql = sql ++ " WHERE id >= 0";
    const Six = [6]?i64;

    try std.testing.expectEqual(Six{ 0, 999, 0, 899, null, null }, try optionalInts(6, allocator, db, sql));
    try std.testing.expectEqual(@as(?usize, 0), try metaBlocksDecoded(allocator, db, sql));

    // Tombstones in row groups 5, 8 and 9 (the MAX(k) row, the MAX(v) row,
    // and a row of the all-NULL-v row group 9). MAX(k) decodes row group 9
    // (bound 999 > the clean 799) and stops at row group 8 (899 < 998);
    // MAX(v) decodes row group 8 (899 > 799) and stops at row group 5. The
    // MINs keep their clean 0, and w has no non-NULL row to visit.
    try exec(allocator, db, "DELETE FROM t WHERE id IN (500, 899, 950, 999)");
    try std.testing.expectEqual(Six{ 0, 998, 0, 898, null, null }, try optionalInts(6, allocator, db, sql));
    try std.testing.expectEqual(try optionalInts(6, allocator, db, scan_sql), try optionalInts(6, allocator, db, sql));
    try std.testing.expectEqual(@as(?usize, 2), try metaBlocksDecoded(allocator, db, sql));

    // An unflushed extreme wins without a decode for MAX(k); MAX(v) still
    // decodes row group 8.
    try exec(allocator, db, "INSERT INTO t (id, k, v, w) VALUES (5000, 5000, -7, NULL)");
    try std.testing.expectEqual(Six{ 0, 5000, -7, 898, null, null }, try optionalInts(6, allocator, db, sql));
    try std.testing.expectEqual(try optionalInts(6, allocator, db, scan_sql), try optionalInts(6, allocator, db, sql));
    try std.testing.expectEqual(@as(?usize, 1), try metaBlocksDecoded(allocator, db, sql));

    try exec(allocator, db, "DELETE FROM t WHERE id = 5000");
    try std.testing.expectEqual(Six{ 0, 998, 0, 898, null, null }, try optionalInts(6, allocator, db, sql));

    // COUNT over nullable columns decodes every row group with tombstones,
    // once per column, and MAX(v) folds the same decoded rows.
    const count_sql = "SELECT COUNT(v), COUNT(w), MAX(v) FROM t";
    try std.testing.expectEqual([3]?i64{ 798, 0, 898 }, try optionalInts(3, allocator, db, count_sql));
    try std.testing.expectEqual(try optionalInts(3, allocator, db, count_sql ++ " WHERE id >= 0"), try optionalInts(3, allocator, db, count_sql));
    try std.testing.expectEqual(@as(?usize, 6), try metaBlocksDecoded(allocator, db, count_sql));
}

test "reads past the stats lane's row budget go to the scan path" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 1000 });
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL)");
    const t = try db.openTable("t", .{});

    const Row = struct { id: i64, k: i64 };
    const rows = try allocator.alloc(Row, 70_000);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| r.* = .{ .id = @intCast(i), .k = @intCast(i % 1000) };
    try t.insert(rows);
    try t.flush();

    // One tombstone in each of the 70 row groups: k = 0.
    try exec(allocator, db, "DELETE FROM t WHERE k = 0");

    // SUM must decode all 70,000 rows, past the 65,536-row budget.
    try std.testing.expectEqual([1]?i64{70 * 499_500}, try optionalInts(1, allocator, db, "SELECT SUM(k) FROM t"));
    try std.testing.expectEqual(@as(?usize, null), try metaBlocksDecoded(allocator, db, "SELECT SUM(k) FROM t"));

    // MAX: the first row group decoded keeps 999, which no other bound beats.
    try std.testing.expectEqual([1]?i64{999}, try optionalInts(1, allocator, db, "SELECT MAX(k) FROM t"));
    try std.testing.expectEqual(@as(?usize, 1), try metaBlocksDecoded(allocator, db, "SELECT MAX(k) FROM t"));

    // MIN: every row group's stale min 0 beats the live 1, so the visit
    // runs out of budget and the scan path answers.
    try std.testing.expectEqual([1]?i64{1}, try optionalInts(1, allocator, db, "SELECT MIN(k) FROM t"));
    try std.testing.expectEqual(@as(?usize, null), try metaBlocksDecoded(allocator, db, "SELECT MIN(k) FROM t"));
}
