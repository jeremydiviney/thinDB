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
            try std.testing.expectEqual(flagFor(i), @as(i64, batch.values[2].data.smallint[j]));
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
