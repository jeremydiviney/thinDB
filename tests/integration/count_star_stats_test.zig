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

test "mixed COUNT/MIN/MAX metadata lane matches the scan path, bails on tombstones" {
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
    // stats still say 501, so the MIN/MAX lane must bail to the scan path.
    try exec(allocator, db, "DELETE FROM t WHERE id >= 499");
    {
        const meta = try row3(allocator, db, meta_sql);
        const scanned = try row3(allocator, db, scan_sql);
        try std.testing.expectEqual(scanned, meta);
        try std.testing.expectEqual([3]i64{ 499, 502, 498 }, meta);
    }

    // Counts stay metadata-served under tombstones.
    try std.testing.expectEqual(
        @as(i64, 499),
        try countVia(allocator, db, "SELECT COUNT(k) FROM t"),
    );
}

const SumAvgRow = struct {
    sum_k: i128,
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
        .sum_k = batch.values[0].data.largeint[0],
        .avg_v = batch.values[1].data.double[0],
        .count_v = batch.values[2].data.bigint[0],
        .sum_f = batch.values[3].data.double[0],
    };
    while (try q.next()) |_| {}
    return out;
}

test "SUM/AVG/COUNT(nullable) metadata lane matches the scan path across segments and bails on tombstones" {
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
        try std.testing.expectEqual(@as(i128, 359400), meta.sum_k);
        try std.testing.expectEqual(@as(f64, 300.0), meta.avg_v);
        try std.testing.expectEqual(@as(i64, 400), meta.count_v);
        try std.testing.expectEqual(@as(f64, 180000.0), meta.sum_f);
    }

    // Tombstones poison stored sums/null_counts — the lane must bail to the
    // scan path and still be exact.
    try exec(allocator, db, "DELETE FROM t WHERE id < 9");
    {
        const meta = try sumAvgRow(allocator, db, meta_sql);
        const scanned = try sumAvgRow(allocator, db, scan_sql);
        try std.testing.expectEqual(scanned, meta);
        // Removed ids 0..8: Σ2·id drops 72; v drops ids {1,2,4,5,7,8} = 27
        // over 6 rows; f drops 36 + 9·0.5.
        try std.testing.expectEqual(@as(i128, 359400 - 72), meta.sum_k);
        try std.testing.expectEqual(@as(i64, 394), meta.count_v);
        try std.testing.expectEqual(@as(f64, (120000.0 - 27.0) / 394.0), meta.avg_v);
        try std.testing.expectEqual(@as(f64, 180000.0 - 40.5), meta.sum_f);
    }

    // Unflushed memtable rows also force the scan path.
    try exec(allocator, db, "INSERT INTO t (id, k, v, f) VALUES (1000, 10, 5, 0.5)");
    {
        const meta = try sumAvgRow(allocator, db, meta_sql);
        const scanned = try sumAvgRow(allocator, db, scan_sql);
        try std.testing.expectEqual(scanned, meta);
        try std.testing.expectEqual(@as(i128, 359400 - 72 + 10), meta.sum_k);
        try std.testing.expectEqual(@as(i64, 395), meta.count_v);
    }
}
