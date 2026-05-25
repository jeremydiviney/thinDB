//! On-disk Frame-of-Reference (FOR) encoding round-trip (Phase 2A). Inserts
//! integer-family columns whose value ranges narrow (so the writer picks FOR)
//! alongside a column whose range is too wide to narrow (stays raw), flushes to
//! disk, then reads back via SQL and asserts every value matches exactly.
//! Covers: a negative FOR base, the exact min/max boundary values, a
//! single-distinct-value column, interleaved NULLs, a stays-raw wide column,
//! and a multi-row-group / multi-segment layout.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

const ROWS: i64 = 2500; // > 2000, comfortably multi-row-group at rg_size = 256

// `k`: bounded positive range [1_000_000, 1_000_000 + 199] (200 distinct) — a
// BIGINT whose deltas fit in a u8, so FOR narrows 8→1 byte.
fn kFor(i: i64) i64 {
    return 1_000_000 + @mod(i, 200);
}

// `neg`: negative-base bounded range. Row 0 hits the exact MIN, row 1 the exact
// MAX, so the boundary values are present and must round-trip.
const NEG_MIN: i64 = -30_000;
const NEG_MAX: i64 = -25_000;
fn negFor(i: i64) i64 {
    if (i == 0) return NEG_MIN;
    if (i == 1) return NEG_MAX;
    return NEG_MIN + @mod(i * 7, NEG_MAX - NEG_MIN + 1);
}

// `wide`: a full-spread BIGINT range that cannot narrow below 8 bytes, so the
// block stays raw. Values must still round-trip exactly.
fn wideFor(i: i64) i64 {
    return @mod(i * 2_400_000_000_007, std.math.maxInt(i64));
}

// `solo`: a single distinct value across every row (range 0 → narrowest u8 FOR
// tier). Must round-trip to the same constant.
const SOLO: i64 = 12_345;

// `maybe_null`: an INT with a bounded range and every 4th row NULL, exercising
// FOR + the validity bitmap together (a NULL row still gets a delta slot).
fn maybeNullValid(i: i64) bool {
    return @mod(i, 4) != 0;
}
fn maybeNullVal(i: i64) i64 {
    return 500 + @mod(i, 50);
}

fn setupTable(allocator: std.mem.Allocator, db: anytype, rows: i64, id_base: i64) !void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO t (id, k, neg, wide, solo, maybe_null) VALUES ");
    var line: [160]u8 = undefined;
    var i: i64 = 0;
    while (i < rows) : (i += 1) {
        if (i != 0) try buf.appendSlice(allocator, ", ");
        const id = id_base + i;
        if (maybeNullValid(i)) {
            const s = try std.fmt.bufPrint(&line, "({d}, {d}, {d}, {d}, {d}, {d})", .{
                id, kFor(i), negFor(i), wideFor(i), SOLO, maybeNullVal(i),
            });
            try buf.appendSlice(allocator, s);
        } else {
            const s = try std.fmt.bufPrint(&line, "({d}, {d}, {d}, {d}, {d}, NULL)", .{
                id, kFor(i), negFor(i), wideFor(i), SOLO,
            });
            try buf.appendSlice(allocator, s);
        }
    }
    try exec(allocator, db, buf.items);
}

const Row = struct { id: i64, k: i64, neg: i64, wide: i64, solo: i64, mn: ?i64 };

fn expectedRow(i: i64, id_base: i64) Row {
    return .{
        .id = id_base + i,
        .k = kFor(i),
        .neg = negFor(i),
        .wide = wideFor(i),
        .solo = SOLO,
        .mn = if (maybeNullValid(i)) maybeNullVal(i) else null,
    };
}

// One inserted batch: `rows` rows whose ids run `[base, base + rows)`, each
// generated from index `i = id - base`.
const Segment = struct { base: i64, rows: i64 };

fn reverseExpected(segments: []const Segment, id: i64) Row {
    for (segments) |seg| {
        if (id >= seg.base and id < seg.base + seg.rows) {
            return expectedRow(id - seg.base, seg.base);
        }
    }
    unreachable;
}

/// Reads every row back ordered by id and asserts exact equality against the
/// generator across all inserted `segments`.
fn verify(allocator: std.mem.Allocator, db: anytype, segments: []const Segment) !void {
    var q = try runSql(allocator, db, "SELECT id, k, neg, wide, solo, maybe_null FROM t ORDER BY id");
    defer q.deinit();

    var expected_rows: usize = 0;
    for (segments) |seg| expected_rows += @intCast(seg.rows);

    var seen: usize = 0;
    var saw_neg_min = false;
    var saw_neg_max = false;
    var saw_null = false;
    while (try q.next()) |batch| {
        var j: usize = 0;
        while (j < batch.row_count) : (j += 1) {
            const id = batch.values[0].data.bigint[j];
            const want = reverseExpected(segments, id);
            try std.testing.expectEqual(want.k, batch.values[1].data.bigint[j]);
            try std.testing.expectEqual(want.neg, batch.values[2].data.bigint[j]);
            try std.testing.expectEqual(want.wide, batch.values[3].data.bigint[j]);
            try std.testing.expectEqual(want.solo, batch.values[4].data.bigint[j]);
            if (want.mn) |mn| {
                try std.testing.expect(batch.values[5].isValid(j));
                try std.testing.expectEqual(mn, batch.values[5].data.int[j]);
            } else {
                try std.testing.expect(!batch.values[5].isValid(j));
                saw_null = true;
            }
            if (want.neg == NEG_MIN) saw_neg_min = true;
            if (want.neg == NEG_MAX) saw_neg_max = true;
            seen += 1;
        }
    }
    try std.testing.expectEqual(expected_rows, seen);
    try std.testing.expect(saw_neg_min);
    try std.testing.expect(saw_neg_max);
    try std.testing.expect(saw_null);
}

test "FOR-encoded columns round-trip exactly through flush (single segment, multi row group)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 256 });
    defer db.close();
    try exec(allocator, db,
        \\CREATE TABLE t (
        \\  id BIGINT PRIMARY KEY, k BIGINT NOT NULL, neg BIGINT NOT NULL,
        \\  wide BIGINT NOT NULL, solo BIGINT NOT NULL, maybe_null INT
        \\)
    );

    try setupTable(allocator, db, ROWS, 1);
    const t = try db.openTable("t", .{});
    try t.flush();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    try verify(allocator, db, &.{.{ .base = 1, .rows = ROWS }});
}

test "FOR-encoded columns round-trip across multiple segments" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 256 });
    defer db.close();
    try exec(allocator, db,
        \\CREATE TABLE t (
        \\  id BIGINT PRIMARY KEY, k BIGINT NOT NULL, neg BIGINT NOT NULL,
        \\  wide BIGINT NOT NULL, solo BIGINT NOT NULL, maybe_null INT
        \\)
    );

    // Two insert+flush cycles → two on-disk segments, each independently
    // FOR-encoded, exercising the mixed multi-segment read path.
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
