//! On-disk run-length (RLE) encoding round-trip. Inserts integer-family
//! columns with long runs (so the writer's size comparison provably picks RLE
//! over raw and FOR) alongside an alternating column with no run structure
//! (stays FOR/raw), flushes to disk, then reads back via SQL and asserts every
//! value matches exactly. Covers: long uniform runs, a mostly-constant flag
//! column, run boundaries landing on row-group boundaries, interleaved NULLs
//! (placeholder slots must round-trip under the bitmap), a no-runs column,
//! and a multi-segment layout. Aggregates cross-check the expanded values.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

const ROWS: i64 = 2500; // multi-row-group at rg_size = 256; runs straddle RG edges

// `runs`: value changes every 100 rows → 25 runs of 100. For a 256-row block:
// raw = 2048B, FOR(width 1) = 276B, RLE ≈ 8 + ~4×12 = ~56B → RLE wins.
fn runsFor(i: i64) i64 {
    return 7_000_000_000 + @divTrunc(i, 100);
}

// `flag`: mostly 0 with a 1 every 250th row — the IsRefresh shape. Runs are
// long, values are tinyint-width.
fn flagFor(i: i64) i64 {
    return if (@mod(i, 250) == 249) 1 else 0;
}

// `alt`: strictly alternating — n_runs == n, so RLE can never win and the
// column stays FOR/raw. Values must still round-trip exactly.
fn altFor(i: i64) i64 {
    return 100 + @mod(i, 2);
}

// `runs_null`: runs of 50 with every 4th row NULL. NULL placeholder slots sit
// inside the run stream; the bitmap masks them and values round-trip exactly.
fn runsNullValid(i: i64) bool {
    return @mod(i, 4) != 0;
}
fn runsNullVal(i: i64) i64 {
    return 40_000 + @divTrunc(i, 50);
}

fn setupTable(allocator: std.mem.Allocator, db: anytype, rows: i64, id_base: i64) !void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO t (id, runs, flag, alt, runs_null) VALUES ");
    var line: [120]u8 = undefined;
    var i: i64 = 0;
    while (i < rows) : (i += 1) {
        if (i != 0) try buf.appendSlice(allocator, ", ");
        const id = id_base + i;
        if (runsNullValid(i)) {
            const s = try std.fmt.bufPrint(&line, "({d}, {d}, {d}, {d}, {d})", .{
                id, runsFor(i), flagFor(i), altFor(i), runsNullVal(i),
            });
            try buf.appendSlice(allocator, s);
        } else {
            const s = try std.fmt.bufPrint(&line, "({d}, {d}, {d}, {d}, NULL)", .{
                id, runsFor(i), flagFor(i), altFor(i),
            });
            try buf.appendSlice(allocator, s);
        }
    }
    try exec(allocator, db, buf.items);
}

const Segment = struct { base: i64, rows: i64 };

fn verify(allocator: std.mem.Allocator, db: anytype, segments: []const Segment) !void {
    var q = try runSql(allocator, db, "SELECT id, runs, flag, alt, runs_null FROM t ORDER BY id");
    defer q.deinit();

    var expected_rows: usize = 0;
    for (segments) |seg| expected_rows += @intCast(seg.rows);

    var seen: usize = 0;
    var saw_null = false;
    var saw_flag_one = false;
    while (try q.next()) |batch| {
        var j: usize = 0;
        while (j < batch.row_count) : (j += 1) {
            const id = batch.values[0].data.bigint[j];
            var i: i64 = undefined;
            for (segments) |seg| {
                if (id >= seg.base and id < seg.base + seg.rows) i = id - seg.base;
            }
            try std.testing.expectEqual(runsFor(i), batch.values[1].data.bigint[j]);
            try std.testing.expectEqual(flagFor(i), @as(i64, batch.values[2].data.tinyint[j]));
            try std.testing.expectEqual(altFor(i), @as(i64, batch.values[3].data.int[j]));
            if (runsNullValid(i)) {
                try std.testing.expect(batch.values[4].isValid(j));
                try std.testing.expectEqual(runsNullVal(i), batch.values[4].data.bigint[j]);
            } else {
                try std.testing.expect(!batch.values[4].isValid(j));
                saw_null = true;
            }
            if (flagFor(i) == 1) saw_flag_one = true;
            seen += 1;
        }
    }
    try std.testing.expectEqual(expected_rows, seen);
    try std.testing.expect(saw_null);
    try std.testing.expect(saw_flag_one);
}

fn sumVia(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !i64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    const batch = (try q.next()) orelse return error.TestUnexpectedResult;
    const v: i64 = switch (batch.values[0].data) {
        .bigint => |d| d[0],
        .largeint => |d| @intCast(d[0]),
        else => return error.TestUnexpectedResult,
    };
    while (try q.next()) |_| {}
    return v;
}

test "RLE-encoded columns round-trip exactly through flush" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 256 });
    defer db.close();
    try exec(allocator, db,
        \\CREATE TABLE t (
        \\  id BIGINT PRIMARY KEY, runs BIGINT NOT NULL, flag TINYINT NOT NULL,
        \\  alt INT NOT NULL, runs_null BIGINT
        \\)
    );

    try setupTable(allocator, db, ROWS, 1);
    const t = try db.openTable("t", .{});
    try t.flush();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    try verify(allocator, db, &.{.{ .base = 1, .rows = ROWS }});

    // Aggregate cross-checks over the expanded values (filters + sums exercise
    // the scan path over RLE blocks end to end).
    var flag_sum: i64 = 0;
    var runs_sum: i64 = 0;
    var i: i64 = 0;
    while (i < ROWS) : (i += 1) {
        flag_sum += flagFor(i);
        runs_sum += runsFor(i);
    }
    try std.testing.expectEqual(flag_sum, try sumVia(allocator, db, "SELECT SUM(flag) FROM t WHERE id >= 0"));
    try std.testing.expectEqual(runs_sum, try sumVia(allocator, db, "SELECT SUM(runs) FROM t WHERE id >= 0"));
    try std.testing.expectEqual(
        @as(i64, 100),
        try sumVia(allocator, db, "SELECT COUNT(*) FROM t WHERE runs = 7000000003"),
    );

    // A filtered COUNT(*) over one RLE column counts its runs.
    try std.testing.expectEqual(@as(i64, 10), try sumVia(allocator, db, "SELECT COUNT(*) FROM t WHERE flag <> 0"));
    try std.testing.expectEqual(@as(i64, 2490), try sumVia(allocator, db, "SELECT COUNT(*) FROM t WHERE flag = 0"));
    try std.testing.expectEqual(@as(i64, 1400), try sumVia(allocator, db, "SELECT COUNT(*) FROM t WHERE runs > 7000000010"));
    try std.testing.expectEqual(@as(i64, 1100), try sumVia(allocator, db, "SELECT COUNT(*) FROM t WHERE runs <= 7000000010"));

    // Deleted rows still leave the runs: rows 0..299 hold the flag at row 249.
    try exec(allocator, db, "DELETE FROM t WHERE id <= 300");
    try std.testing.expectEqual(@as(i64, 9), try sumVia(allocator, db, "SELECT COUNT(*) FROM t WHERE flag <> 0"));
    try std.testing.expectEqual(@as(i64, 1100 - 300), try sumVia(allocator, db, "SELECT COUNT(*) FROM t WHERE runs <= 7000000010"));
}

test "RLE-encoded columns round-trip across multiple segments" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 256 });
    defer db.close();
    try exec(allocator, db,
        \\CREATE TABLE t (
        \\  id BIGINT PRIMARY KEY, runs BIGINT NOT NULL, flag TINYINT NOT NULL,
        \\  alt INT NOT NULL, runs_null BIGINT
        \\)
    );

    try setupTable(allocator, db, ROWS, 1);
    const t = try db.openTable("t", .{});
    try t.flush();
    try setupTable(allocator, db, ROWS, ROWS + 1);
    try t.flush();
    try std.testing.expectEqual(@as(usize, 2), t.segmentCount());

    try verify(allocator, db, &.{
        .{ .base = 1, .rows = ROWS },
        .{ .base = ROWS + 1, .rows = ROWS },
    });
}

// Short runs: run k is 1 + 7k mod 13 rows long, so runs of 1 to 13 rows
// straddle row-group edges and the 8-row vector width. `v` is a spread 64-bit
// value per run (FOR can't narrow it, so RLE wins), `g` one of 37 values that
// changes every run, `w` a small value for sums, and `x` and `f` small signed
// values for MIN/MAX.
const SHORT_ROWS: usize = 3000;
const SHORT_GROUPS: usize = 37;

const ShortRun = struct { k: u64, start: usize };

fn shortRunOf(row: usize) ShortRun {
    var k: u64 = 0;
    var start: usize = 0;
    while (true) : (k += 1) {
        const len: usize = @intCast(1 + (k * 7) % 13);
        if (row < start + len) return .{ .k = k, .start = start };
        start += len;
    }
}
fn shortV(k: u64) i64 {
    return @bitCast(k *% 0x9E3779B97F4A7C15);
}
fn shortG(k: u64) i64 {
    return @intCast((k % SHORT_GROUPS) * 1000003);
}
fn shortW(k: u64) i64 {
    return @intCast(k % 5);
}
fn shortX(k: u64) i64 {
    return @as(i64, @intCast(k % 11)) - 5;
}
fn shortF(k: u64) f64 {
    return @as(f64, @floatFromInt(k % 9)) * 0.25 - 1.0;
}
fn shortCell(col: anytype, row: usize) !?f64 {
    if (!col.isValid(row)) return null;
    return switch (col.data) {
        inline .smallint, .int, .bigint, .largeint => |v| @floatFromInt(v[row]),
        inline .float, .double => |v| @floatCast(v[row]),
        else => error.TestUnexpectedType,
    };
}

test "RLE columns of short runs round-trip and group exactly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 256 });
    defer db.close();
    try exec(allocator, db, "CREATE TABLE s (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, g INT NOT NULL, w SMALLINT NOT NULL, x INT NOT NULL, f DOUBLE NOT NULL)");
    {
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(allocator);
        try buf.appendSlice(allocator, "INSERT INTO s VALUES ");
        var line: [128]u8 = undefined;
        for (0..SHORT_ROWS) |i| {
            if (i != 0) try buf.appendSlice(allocator, ", ");
            const k = shortRunOf(i).k;
            try buf.appendSlice(allocator, try std.fmt.bufPrint(&line, "({d}, {d}, {d}, {d}, {d}, {d})", .{ i, shortV(k), shortG(k), shortW(k), shortX(k), shortF(k) }));
        }
        try exec(allocator, db, buf.items);
    }
    try (try db.openTable("s", .{})).flush();

    {
        var q = try runSql(allocator, db, "SELECT id, v, g, w FROM s ORDER BY id");
        defer q.deinit();
        var seen: usize = 0;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |j| {
                const id: usize = @intCast(batch.values[0].data.bigint[j]);
                const k = shortRunOf(id).k;
                try std.testing.expectEqual(shortV(k), batch.values[1].data.bigint[j]);
                try std.testing.expectEqual(shortG(k), @as(i64, batch.values[2].data.int[j]));
                try std.testing.expectEqual(shortW(k), @as(i64, batch.values[3].data.smallint[j]));
                seen += 1;
            }
        }
        try std.testing.expectEqual(SHORT_ROWS, seen);
    }

    // Per g: rows, distinct v (one per run of g) and SUM(w).
    var rows: [SHORT_GROUPS]i64 = @splat(0);
    var runs: [SHORT_GROUPS]i64 = @splat(0);
    var sums: [SHORT_GROUPS]i64 = @splat(0);
    for (0..SHORT_ROWS) |i| {
        const r = shortRunOf(i);
        const g: usize = @intCast(r.k % SHORT_GROUPS);
        rows[g] += 1;
        if (r.start == i) runs[g] += 1;
        sums[g] += shortW(r.k);
    }
    var q = try runSql(allocator, db, "SELECT g, COUNT(*), COUNT(DISTINCT v), SUM(w) FROM s GROUP BY g ORDER BY g");
    defer q.deinit();
    var at: usize = 0;
    while (try q.next()) |batch| {
        for (0..batch.row_count) |j| {
            try std.testing.expectEqual(shortG(at), @as(i64, batch.values[0].data.int[j]));
            try std.testing.expectEqual(rows[at], batch.values[1].data.bigint[j]);
            try std.testing.expectEqual(runs[at], batch.values[2].data.bigint[j]);
            try std.testing.expectEqual(sums[at], batch.values[3].data.bigint[j]);
            at += 1;
        }
    }
    try std.testing.expectEqual(SHORT_GROUPS, at);

    // Per g: SUM/MIN/MAX(x), MIN/MAX(w), SUM/MIN/MAX/COUNT(f).
    var want: [SHORT_GROUPS][9]?f64 = undefined;
    for (&want) |*wg| wg.* = .{ 0, null, null, null, null, 0, null, null, 0 };
    for (0..SHORT_ROWS) |i| {
        const k = shortRunOf(i).k;
        const wg = &want[@intCast(k % SHORT_GROUPS)];
        const x: f64 = @floatFromInt(shortX(k));
        wg[0].? += x;
        wg[1] = @min(wg[1] orelse x, x);
        wg[2] = @max(wg[2] orelse x, x);
        const w: f64 = @floatFromInt(shortW(k));
        wg[3] = @min(wg[3] orelse w, w);
        wg[4] = @max(wg[4] orelse w, w);
        const f = shortF(k);
        wg[5].? += f;
        wg[6] = @min(wg[6] orelse f, f);
        wg[7] = @max(wg[7] orelse f, f);
        wg[8].? += 1;
    }
    var aq = try runSql(allocator, db, "SELECT g, SUM(x), MIN(x), MAX(x), MIN(w), MAX(w), SUM(f), MIN(f), MAX(f), COUNT(f) FROM s GROUP BY g ORDER BY g");
    defer aq.deinit();
    at = 0;
    while (try aq.next()) |batch| {
        for (0..batch.row_count) |j| {
            try std.testing.expectEqual(shortG(at), @as(i64, batch.values[0].data.int[j]));
            for (want[at], 1..) |wv, c| {
                errdefer std.debug.print("group {d} column {d}\n", .{ at, c });
                try std.testing.expectEqual(wv, try shortCell(batch.values[c], j));
            }
            at += 1;
        }
    }
    try std.testing.expectEqual(SHORT_GROUPS, at);
}

// One column's encoding changes from row group to row group: `v` holds long
// runs (RLE) in row group 0, a narrow range (FOR) in row group 1 and
// full-width values (raw) in row group 2, and `n` is its nullable twin with
// NULL runs. The scan views each block in place, or expands it into one buffer
// per column it reuses, so every row group, sub-batch and tombstoned row group
// must still read back its own values.
const MIXED_RG_ROWS: usize = 4096;

fn mixedV(i: usize) i64 {
    return switch (i / MIXED_RG_ROWS) {
        0 => @intCast(9_000_000_000 + i / 512),
        1 => @intCast(5_000 + (i * 37) % 200),
        else => @bitCast(@as(u64, i) *% 0x9E3779B97F4A7C15),
    };
}
fn mixedN(i: usize) ?i64 {
    return if ((i / 64) % 5 == 0) null else mixedV(i);
}

const MIXED_DELETED = [2]usize{ 4100, 4200 };

fn verifyMixed(allocator: std.mem.Allocator, db: anytype, deleted: bool) !void {
    const gone = MIXED_DELETED[1] - MIXED_DELETED[0];
    {
        var q = try runSql(allocator, db, "SELECT id, v, n FROM m");
        defer q.deinit();
        var seen: usize = 0;
        while (try q.next()) |batch| {
            for (0..batch.row_count) |r| {
                const i: usize = @intCast(batch.values[0].data.bigint[r]);
                if (deleted) try std.testing.expect(i < MIXED_DELETED[0] or i >= MIXED_DELETED[1]);
                try std.testing.expectEqual(mixedV(i), batch.values[1].data.bigint[r]);
                const n: ?i64 = if (batch.values[2].isValid(r)) batch.values[2].data.bigint[r] else null;
                try std.testing.expectEqual(mixedN(i), n);
                seen += 1;
            }
        }
        try std.testing.expectEqual(3 * MIXED_RG_ROWS - if (deleted) gone else 0, seen);
    }

    var min: i64 = std.math.maxInt(i64);
    var max: i64 = std.math.minInt(i64);
    var non_null: i64 = 0;
    for (0..3 * MIXED_RG_ROWS) |i| {
        if (deleted and i >= MIXED_DELETED[0] and i < MIXED_DELETED[1]) continue;
        min = @min(min, mixedV(i));
        max = @max(max, mixedV(i));
        if (mixedN(i) != null) non_null += 1;
    }
    var q = try runSql(allocator, db, "SELECT MIN(v), MAX(v), COUNT(n) FROM m");
    defer q.deinit();
    const batch = (try q.next()) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(min, batch.values[0].data.bigint[0]);
    try std.testing.expectEqual(max, batch.values[1].data.bigint[0]);
    try std.testing.expectEqual(non_null, batch.values[2].data.bigint[0]);
    while (try q.next()) |_| {}
}

test "a column switching encodings across row groups reads back exactly" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .row_group_size = MIXED_RG_ROWS, .auto_flush_secs = 0 });
    defer db.close();
    try exec(allocator, db, "CREATE TABLE m (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, n BIGINT)");
    const t = try db.openTable("m", .{});
    const Row = struct { id: i64, v: i64, n: ?i64 };
    const rows = try allocator.alloc(Row, 3 * MIXED_RG_ROWS);
    defer allocator.free(rows);
    for (rows, 0..) |*row, i| row.* = .{ .id = @intCast(i), .v = mixedV(i), .n = mixedN(i) };
    try t.insert(rows);
    try t.flush();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    const saved = thindb.exec.scan_sub_batch;
    defer thindb.exec.scan_sub_batch = saved;
    for ([_]usize{ 0, 1024 }) |sub_batch| {
        thindb.exec.scan_sub_batch = sub_batch;
        try verifyMixed(allocator, db, false);
    }

    // Tombstones send row group 1 down the owned-decode path between two
    // row groups whose blocks are held.
    var buf: [96]u8 = undefined;
    try exec(allocator, db, try std.fmt.bufPrint(&buf, "DELETE FROM m WHERE id >= {d} AND id < {d}", .{ MIXED_DELETED[0], MIXED_DELETED[1] }));
    for ([_]usize{ 0, 1024 }) |sub_batch| {
        thindb.exec.scan_sub_batch = sub_batch;
        try verifyMixed(allocator, db, true);
    }
}
