//! Fixed-width table ids (DESIGN.md §4.7): a BIGINT, DATETIME or DECIMAL
//! column whose segment takes ids answers every query as its values do,
//! beside segments that took none, memtable rows and tombstones, and
//! through reopen, compaction and ALTER. A memtable-only twin of the table
//! is the oracle.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;

const io = std.testing.io;

const config: thindb.Config = .{
    .compact_min_segments = 100,
    .auto_flush_secs = 0,
    .auto_flush_rows = 1_000_000,
    .auto_flush_bytes = 64 * 1024 * 1024,
    .row_group_size = 128,
};

const u_column = 1;
const ts_column = 2;
const amt_column = 3;

const u_base: i64 = 1_000_000_000_000_000;
const u_step: i64 = 7_919_000_000_013;

const queries = [_][]const u8{
    "SELECT COUNT(*), COUNT(u), COUNT(DISTINCT u), COUNT(DISTINCT ts), COUNT(DISTINCT amt) FROM {T}",
    "SELECT COUNT(DISTINCT u) FROM {T}",
    "SELECT u, COUNT(*) FROM {T} GROUP BY u ORDER BY u",
    "SELECT u, COUNT(*) AS c FROM {T} GROUP BY u ORDER BY c DESC, u LIMIT 5",
    "SELECT SUM(u), MIN(u), MAX(u), SUM(amt), MIN(ts), MAX(ts) FROM {T}",
    "SELECT id, u, ts, amt FROM {T} WHERE u = 1039595000000080 ORDER BY id",
    "SELECT COUNT(*) FROM {T} WHERE u > 1039595000000080 AND u < 1150000000000000",
    "SELECT ts, COUNT(DISTINCT u) FROM {T} GROUP BY ts ORDER BY ts",
    "SELECT id, u FROM {T} ORDER BY u DESC, id LIMIT 7",
    "SELECT id, u, ts, amt FROM {T} WHERE id IN (3, 77, 391, 650, 1000, 1150) ORDER BY id",
    "SELECT id, u + 1, amt * 2 FROM {T} ORDER BY id LIMIT 5",
    "SELECT COUNT(*) FROM {T} WHERE u IS NULL",
    "SELECT amt, ts, COUNT(*) FROM {T} GROUP BY amt, ts ORDER BY amt, ts LIMIT 20",
    "SELECT u, ts, MAX(amt) FROM {T} WHERE id % 3 = 0 GROUP BY u, ts ORDER BY u, ts LIMIT 15",
    "SELECT u FROM {T} WHERE ts = '2024-01-05 00:00:16' ORDER BY id LIMIT 10",
    "SELECT amt, COUNT(DISTINCT u), COUNT(DISTINCT ts) FROM {T} GROUP BY amt ORDER BY amt",
    // Unordered, so their rows compare sorted: the keys and COUNT(DISTINCT)
    // inputs nothing else reads, which the scans may read by id.
    "SELECT u, COUNT(*) FROM {T} GROUP BY u",
    "SELECT amt, COUNT(*) FROM {T} GROUP BY amt",
    "SELECT ts, amt, COUNT(*) FROM {T} GROUP BY ts, amt",
    "SELECT u, ts, COUNT(*), SUM(id) FROM {T} WHERE id > 100 GROUP BY u, ts",
    "SELECT u, k, COUNT(*) FROM {T} GROUP BY u, k",
    "SELECT k, u, MIN(id) FROM {T} WHERE id > 50 GROUP BY k, u",
    "SELECT k, COUNT(DISTINCT u) FROM {T} GROUP BY k",
    "SELECT u, COUNT(*) FROM {T} GROUP BY u HAVING COUNT(*) > 20",
    "SELECT u, MAX(amt) FROM {T} GROUP BY u",
    "SELECT amt, COUNT(DISTINCT u) FROM {T} GROUP BY amt",
    "SELECT id % 5, COUNT(DISTINCT u), COUNT(DISTINCT ts) FROM {T} GROUP BY id % 5",
    "SELECT COUNT(DISTINCT u), COUNT(DISTINCT ts), COUNT(DISTINCT amt) FROM {T}",
    "SELECT COUNT(DISTINCT u) FROM {T} WHERE id > 100",
};

/// Every row of `sql` over table `table`, its cells joined by `|` and its
/// rows by newlines, sorted when `sql` leaves their order open. Caller owns
/// the result.
fn rowsText(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, table: []const u8) ![]u8 {
    const text = try std.mem.replaceOwned(u8, allocator, sql, "{T}", table);
    defer allocator.free(text);
    var q = try helpers.runSql(allocator, db, text);
    defer q.deinit();
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |batch| {
        for (0..batch.row_count) |row| {
            for (batch.values, 0..) |col, c| {
                if (c != 0) try out.append(allocator, '|');
                if (!col.isValid(row)) {
                    try out.appendSlice(allocator, "NULL");
                    continue;
                }
                switch (col.data) {
                    .string, .varchar, .char => |sv| try out.appendSlice(allocator, sv.rowBytes(row)),
                    inline .boolean, .tinyint, .smallint, .int, .bigint, .largeint, .date, .datetime, .decimal64, .decimal128, .double, .float => |s| try out.print(allocator, "{d}", .{s[row]}),
                    else => return error.TestUnexpectedType,
                }
            }
            try out.append(allocator, '\n');
        }
    }
    if (std.mem.indexOf(u8, sql, "ORDER BY") == null) try sortLines(allocator, &out);
    return out.toOwnedSlice(allocator);
}

fn sortLines(allocator: std.mem.Allocator, text: *std.ArrayList(u8)) !void {
    var lines: std.ArrayList([]const u8) = .empty;
    defer lines.deinit(allocator);
    var it = std.mem.splitScalar(u8, text.items, '\n');
    while (it.next()) |line| if (line.len > 0) try lines.append(allocator, line);
    std.mem.sort([]const u8, lines.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);
    var sorted: std.ArrayList(u8) = .empty;
    errdefer sorted.deinit(allocator);
    for (lines.items) |line| {
        try sorted.appendSlice(allocator, line);
        try sorted.append(allocator, '\n');
    }
    text.deinit(allocator);
    text.* = sorted;
}

/// Every query over `t` matches it over the memtable-only `r`.
fn expectMatchesOracle(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    for (queries) |sql| {
        const got = try rowsText(allocator, db, sql, "t");
        defer allocator.free(got);
        const want = try rowsText(allocator, db, sql, "r");
        defer allocator.free(want);
        errdefer std.debug.print("query: {s}\n", .{sql});
        try std.testing.expectEqualStrings(want, got);
    }
}

/// Every query's rows over `t`, for a later `expectSameAs`.
fn capture(allocator: std.mem.Allocator, db: *thindb.Database) ![queries.len][]u8 {
    var out: [queries.len][]u8 = undefined;
    var n: usize = 0;
    errdefer for (out[0..n]) |o| allocator.free(o);
    for (queries, &out) |sql, *o| {
        o.* = try rowsText(allocator, db, sql, "t");
        n += 1;
    }
    return out;
}

fn expectSameAs(allocator: std.mem.Allocator, db: *thindb.Database, captured: [queries.len][]u8) !void {
    for (queries, captured) |sql, want| {
        const got = try rowsText(allocator, db, sql, "t");
        defer allocator.free(got);
        errdefer std.debug.print("query: {s}\n", .{sql});
        try std.testing.expectEqualStrings(want, got);
    }
}

fn freeCaptured(allocator: std.mem.Allocator, captured: [queries.len][]u8) void {
    for (captured) |c| allocator.free(c);
}

const Rows = struct {
    first_id: usize,
    count: usize,
    /// Each row's `u` new to the table, rather than one of `u_values`.
    unique_u: bool = false,
    u_values: usize = 37,
    u_offset: usize = 0,
};

/// Inserts `rows` into `table`: every 13th `u` NULL, the rest cycling over
/// `u_values` wide values (or new on every row), `ts` over 23 days,
/// `amt` over 11 amounts and `k` over 4. No column's values share a common step, which
/// would narrow their blocks past any ids.
fn insertRows(allocator: std.mem.Allocator, db: *thindb.Database, table: []const u8, rows: Rows) !void {
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);
    try sql.print(allocator, "INSERT INTO {s} (id, u, ts, amt, k) VALUES ", .{table});
    for (0..rows.count) |i| {
        const id = rows.first_id + i;
        if (i != 0) try sql.appendSlice(allocator, ", ");
        try sql.print(allocator, "({d}, ", .{id});
        if (id % 13 == 0) {
            try sql.appendSlice(allocator, "NULL");
        } else if (rows.unique_u) {
            try sql.print(allocator, "{d}", .{-u_base + @as(i64, @intCast(id)) * 3_000_000_017});
        } else {
            const k: i64 = @intCast(rows.u_offset + id % rows.u_values);
            try sql.print(allocator, "{d}", .{u_base + k * u_step + @divExact(k * (k + 1), 2)});
        }
        const day = id % 23;
        const amount = id % 11;
        try sql.print(allocator, ", '2024-01-{d:0>2} 00:00:{d:0>2}', {d}.{d:0>2}, {d})", .{ 1 + day, day * day % 60, amount * 1_234_567, amount * amount * 7 % 100, id % 4 });
    }
    try exec(allocator, db, sql.items);
}

fn insertBoth(allocator: std.mem.Allocator, db: *thindb.Database, rows: Rows) !void {
    try insertRows(allocator, db, "t", rows);
    try insertRows(allocator, db, "r", rows);
}

/// Whether each of `t`'s segments stores column `ci` in table ids, oldest
/// first, into `out`.
fn idSegments(allocator: std.mem.Allocator, t: *thindb.Table, ci: usize, out: []bool) !void {
    try std.testing.expectEqual(out.len, t.manifest.segments.items.len);
    for (t.manifest.segments.items, out) |entry, *o| {
        var name_buf: [32]u8 = undefined;
        const name = try thindb.Table.segmentFileName(&name_buf, entry.segment_id);
        var seg = try thindb.storage.readSegment(allocator, t.io, t.segments_dir, name, t.schema);
        defer seg.deinit();
        o.* = seg.info.intIdColumn(ci);
    }
}

fn expectIdSegments(allocator: std.mem.Allocator, t: *thindb.Table, ci: usize, expected: []const bool) !void {
    const got = try allocator.alloc(bool, expected.len);
    defer allocator.free(got);
    try idSegments(allocator, t, ci, got);
    try std.testing.expectEqualSlices(bool, expected, got);
}

const create_t = "CREATE TABLE t (id BIGINT NOT NULL, u BIGINT, ts DATETIME NOT NULL, amt DECIMAL(18,2) NOT NULL, k INT NOT NULL, PRIMARY KEY (id))";
const create_r = "CREATE TABLE r (id BIGINT NOT NULL, u BIGINT, ts DATETIME NOT NULL, amt DECIMAL(18,2) NOT NULL, k INT NOT NULL, PRIMARY KEY (id))";

test "table int ids: queries over id-coded segments match the values" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var captured: [queries.len][]u8 = undefined;
    {
        const db = try thindb.Database.open(allocator, io, tmp.dir, config);
        defer db.close();
        try exec(allocator, db, create_t);
        try exec(allocator, db, create_r);
        const t = try db.openTable("t", .{});

        try insertBoth(allocator, db, .{ .first_id = 0, .count = 600 });
        try t.flush();
        try expectIdSegments(allocator, t, u_column, &.{true});
        try expectIdSegments(allocator, t, ts_column, &.{true});
        try expectIdSegments(allocator, t, amt_column, &.{true});
        try expectIdSegments(allocator, t, 0, &.{false});
        try expectMatchesOracle(allocator, db);

        // Half the values new to the list, first in the memtable beside the
        // segment, then in a segment of their own.
        try insertBoth(allocator, db, .{ .first_id = 600, .count = 300, .u_offset = 20 });
        try expectMatchesOracle(allocator, db);
        try t.flush();
        try expectIdSegments(allocator, t, u_column, &.{ true, true });
        try expectMatchesOracle(allocator, db);

        // Values new on every row take no ids.
        try insertBoth(allocator, db, .{ .first_id = 900, .count = 300, .unique_u = true });
        try t.flush();
        try expectIdSegments(allocator, t, u_column, &.{ true, true, false });
        try expectIdSegments(allocator, t, ts_column, &.{ true, true, true });
        try expectMatchesOracle(allocator, db);

        try exec(allocator, db, "DELETE FROM t WHERE id % 10 = 3");
        try exec(allocator, db, "DELETE FROM r WHERE id % 10 = 3");
        try expectMatchesOracle(allocator, db);
        captured = try capture(allocator, db);
    }
    defer freeCaptured(allocator, captured);

    const db = try thindb.Database.open(allocator, io, tmp.dir, config);
    defer db.close();
    const t = try db.openTable("t", .{});
    try expectSameAs(allocator, db, captured);

    // The merge's inputs don't all store `u` in ids, so it stores values;
    // they all store `ts` in ids, so it keeps them.
    try t.compact();
    try expectIdSegments(allocator, t, u_column, &.{false});
    try expectIdSegments(allocator, t, ts_column, &.{true});
    try expectSameAs(allocator, db, captured);

    // The rewrite plans the whole table's values afresh.
    try exec(allocator, db, "ALTER TABLE t ADD COLUMN n INT");
    const altered = try db.openTable("t", .{});
    try expectIdSegments(allocator, altered, u_column, &.{true});
    try expectSameAs(allocator, db, captured);
}

test "table int ids: a merge of id-coded segments keeps the ids, and new values continue the list" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, config);
    defer db.close();
    try exec(allocator, db, create_t);
    try exec(allocator, db, create_r);
    const t = try db.openTable("t", .{});

    try insertBoth(allocator, db, .{ .first_id = 0, .count = 400 });
    try t.flush();
    try insertBoth(allocator, db, .{ .first_id = 400, .count = 400, .u_offset = 30 });
    try t.flush();
    const before = t.strings.state(t.io, u_column).?.id_count;
    try t.compact();
    try expectIdSegments(allocator, t, u_column, &.{true});
    try std.testing.expectEqual(before, t.strings.state(t.io, u_column).?.id_count);
    try expectMatchesOracle(allocator, db);

    try insertBoth(allocator, db, .{ .first_id = 800, .count = 400, .u_offset = 60 });
    try t.flush();
    try expectIdSegments(allocator, t, u_column, &.{ true, true });
    try std.testing.expect(t.strings.state(t.io, u_column).?.id_count > before);
    try expectMatchesOracle(allocator, db);
}
