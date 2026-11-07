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
    // A few rows' values, fetched by location after the rows are picked.
    "SELECT id, u, ts, amt, k FROM {T} WHERE k = 1 ORDER BY id DESC LIMIT 6",
    "SELECT u, k, COUNT(*) AS c FROM {T} WHERE u > 1000000000000001 GROUP BY u, k ORDER BY c DESC, u, k LIMIT 4",
    // Equality and IN over ids, by the literal's id: values the list holds
    // and values it lacks, NULLs left out either way.
    "SELECT id, u FROM {T} WHERE u = 1000000000000000 ORDER BY id",
    "SELECT id, u FROM {T} WHERE u = 5 ORDER BY id",
    "SELECT id, u, ts FROM {T} WHERE u <> 1039595000000080 ORDER BY id LIMIT 30",
    "SELECT id FROM {T} WHERE u <> 1039595000000081 AND id < 60 ORDER BY id",
    "SELECT id, u FROM {T} WHERE u IN (1039595000000080, 5, 1000000000000000) ORDER BY id",
    "SELECT id, u FROM {T} WHERE u NOT IN (1039595000000080, 1000000000000000) AND id > 1100 ORDER BY id",
    "SELECT id FROM {T} WHERE u = 1039595000000080 OR u = 1000000000000000 ORDER BY id",
    "SELECT id, ts FROM {T} WHERE u = 1000000000000000 AND k = 2 ORDER BY id",
    "SELECT COUNT(*), SUM(id) FROM {T} WHERE u = 1039595000000080",
    "SELECT id, ts FROM {T} WHERE ts <> '2024-01-05 00:00:16' AND id < 40 ORDER BY id",
    "SELECT id, amt FROM {T} WHERE amt = 1234567.07 ORDER BY id LIMIT 10",
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

/// Drops `t`'s cached blocks and list pages, so the next query reads its
/// row groups as a cold one does: ids where they store ids.
fn purgeT(db: *thindb.Database) !void {
    const t = try db.openTable("t", .{});
    const tc = t.cacheRef();
    tc.cache.purgeTable(tc.table_uid);
    if (t.strings.reader) |r| tc.cache.purgeTable(r.uid);
}

/// Every query over `t` matches it over the memtable-only `r`, cold and
/// then over what the cold run cached.
fn expectMatchesOracle(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    for (queries) |sql| for (0..2) |round| {
        if (round == 0) try purgeT(db);
        const got = try rowsText(allocator, db, sql, "t");
        defer allocator.free(got);
        const want = try rowsText(allocator, db, sql, "r");
        defer allocator.free(want);
        errdefer std.debug.print("query (round {d}): {s}\n", .{ round, sql });
        try std.testing.expectEqualStrings(want, got);
    };
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
    for (queries, captured) |sql, want| for (0..2) |round| {
        if (round == 0) try purgeT(db);
        const got = try rowsText(allocator, db, sql, "t");
        defer allocator.free(got);
        errdefer std.debug.print("query (round {d}): {s}\n", .{ round, sql });
        try std.testing.expectEqualStrings(want, got);
    };
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

/// Queries over `s` reading `u`'s values for whole blocks, whose ids lie on
/// every page of its list, and for a few rows fetched by location.
const scattered_queries = [_][]const u8{
    "SELECT SUM(u - 1000000000000000), MIN(u), MAX(u) FROM {T}",
    "SELECT id, u FROM {T} WHERE u = 1039595000000080 ORDER BY id",
    "SELECT * FROM {T} WHERE name = 'n3' ORDER BY u DESC LIMIT 4",
    "SELECT * FROM {T} WHERE name = 'n3' ORDER BY id DESC LIMIT 4",
    "SELECT u, name, COUNT(*) FROM {T} GROUP BY u, name",
    "SELECT u, name, COUNT(*) AS c FROM {T} WHERE id % 7 = 2 GROUP BY u, name HAVING COUNT(*) > 1",
    // One NOT NULL column filtered by and gathered: the literal per match.
    "SELECT u FROM {T} WHERE u = 1039595000000080",
    "SELECT u FROM {T} WHERE u = 1039595000000081",
    "SELECT u FROM {T} WHERE u IN (1039595000000080, 1000000000000000, 7)",
};

test "table int ids: values come off a list of many pages for whole blocks and a few rows alike" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, config);
    defer db.close();
    // A nullable string key keeps a group key with `u` hashed.
    try exec(allocator, db, "CREATE TABLE s (id BIGINT NOT NULL, u BIGINT NOT NULL, name VARCHAR(8), PRIMARY KEY (id))");
    try exec(allocator, db, "CREATE TABLE q (id BIGINT NOT NULL, u BIGINT NOT NULL, name VARCHAR(8), PRIMARY KEY (id))");
    const s = try db.openTable("s", .{});

    // 17,000 values fill three of the list's pages. The first 17,000 rows
    // take them in row order; the rest name them scattered, so each of
    // their blocks reads every page.
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);
    for (0..40) |chunk| for ([_][]const u8{ "s", "q" }) |table| {
        sql.clearRetainingCapacity();
        try sql.print(allocator, "INSERT INTO {s} (id, u, name) VALUES ", .{table});
        for (0..1000) |j| {
            const id = chunk * 1000 + j;
            const k: i64 = @intCast(if (id < 17_000) id else id * 7919 % 17_000);
            if (j != 0) try sql.appendSlice(allocator, ", ");
            try sql.print(allocator, "({d}, {d}, 'n{d}')", .{ id, u_base + k * u_step + @divExact(k * (k + 1), 2), id % 5 });
        }
        try exec(allocator, db, sql.items);
    };
    try s.flush();
    try expectIdSegments(allocator, s, 1, &.{true});
    try std.testing.expect(s.strings.state(s.io, 1).?.id_count > 16_384);

    for (scattered_queries) |text| {
        // Cold: neither the values' blocks nor the list's pages cached.
        const tc = s.cacheRef();
        tc.cache.purgeTable(tc.table_uid);
        tc.cache.purgeTable(s.strings.reader.?.uid);
        const got = try rowsText(allocator, db, text, "s");
        defer allocator.free(got);
        const want = try rowsText(allocator, db, text, "q");
        defer allocator.free(want);
        errdefer std.debug.print("query: {s}\n", .{text});
        try std.testing.expectEqualStrings(want, got);
    }

    // An unordered LIMIT picks any groups, by id here: each counts every row
    // of its key.
    const tc = s.cacheRef();
    tc.cache.purgeTable(tc.table_uid);
    tc.cache.purgeTable(s.strings.reader.?.uid);
    const picked = try rowsText(allocator, db, "SELECT u, name, COUNT(*) FROM {T} GROUP BY u, name LIMIT 3", "s");
    defer allocator.free(picked);
    var lines = std.mem.splitScalar(u8, picked, '\n');
    var groups: usize = 0;
    while (lines.next()) |line| if (line.len > 0) {
        var cells = std.mem.splitScalar(u8, line, '|');
        const u = cells.next().?;
        const name = cells.next().?;
        const count = cells.next().?;
        const check = try std.fmt.allocPrint(allocator, "SELECT COUNT(*) FROM {{T}} WHERE u = {s} AND name = '{s}'", .{ u, name });
        defer allocator.free(check);
        const want = try rowsText(allocator, db, check, "q");
        defer allocator.free(want);
        try std.testing.expectEqualStrings(std.mem.trimEnd(u8, want, "\n"), count);
        groups += 1;
    };
    try std.testing.expectEqual(@as(usize, 3), groups);
}

/// Filters on `u` over ids in long runs, which its blocks store as runs.
const run_queries = [_][]const u8{
    "SELECT id, u FROM {T} WHERE u = 1039595000000080 ORDER BY id",
    "SELECT COUNT(*) FROM {T} WHERE u <> 1039595000000080",
    "SELECT id FROM {T} WHERE u <> 1039595000000080 AND id < 300 ORDER BY id",
    "SELECT id, u FROM {T} WHERE u IN (1000000000000000, 1039595000000080, 3) ORDER BY id",
    "SELECT COUNT(*), SUM(id) FROM {T} WHERE u NOT IN (1000000000000000, 1039595000000080)",
    "SELECT id FROM {T} WHERE u = 1039595000000081 ORDER BY id",
};

test "table int ids: equality and IN read ids stored in runs by the literal's id" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, config);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE p (id BIGINT NOT NULL, u BIGINT, PRIMARY KEY (id))");
    try exec(allocator, db, "CREATE TABLE o (id BIGINT NOT NULL, u BIGINT, PRIMARY KEY (id))");
    const p = try db.openTable("p", .{});

    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);
    for ([_][]const u8{ "p", "o" }) |table| {
        sql.clearRetainingCapacity();
        try sql.print(allocator, "INSERT INTO {s} (id, u) VALUES ", .{table});
        for (0..4000) |id| {
            if (id != 0) try sql.appendSlice(allocator, ", ");
            const k: i64 = @intCast(id / 40);
            if (id % 97 == 5) {
                try sql.print(allocator, "({d}, NULL)", .{id});
            } else {
                try sql.print(allocator, "({d}, {d})", .{ id, u_base + k * u_step + @divExact(k * (k + 1), 2) });
            }
        }
        try exec(allocator, db, sql.items);
    }
    try p.flush();
    try expectIdSegments(allocator, p, 1, &.{true});

    for (run_queries) |text| for (0..2) |round| {
        if (round == 0) {
            const tc = p.cacheRef();
            tc.cache.purgeTable(tc.table_uid);
            tc.cache.purgeTable(p.strings.reader.?.uid);
        }
        const got = try rowsText(allocator, db, text, "p");
        defer allocator.free(got);
        const want = try rowsText(allocator, db, text, "o");
        defer allocator.free(want);
        errdefer std.debug.print("query: {s}\n", .{text});
        try std.testing.expectEqualStrings(want, got);
    };
}
