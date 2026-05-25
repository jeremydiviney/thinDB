//! On-disk segment-local string dictionary (Phase 3) round-trip. Inserts a
//! low-cardinality string column (the writer picks dict) alongside a
//! high-cardinality column (stays raw) and a nullable column mixing NULLs and
//! empty strings, flushes to disk, then reads back via SQL and asserts every
//! value matches exactly — including the NULL-vs-empty-string distinction and a
//! GROUP BY over the dict-encoded column. Covers a single multi-row-group
//! segment and a multi-segment layout (each segment encodes independently).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

const ROWS: i64 = 2400; // > 2000 (multi-row-group at rg_size 256), multiple of 4

// `color`: 4 distinct values cycling → low-card → dict per row group.
const palette = [_][]const u8{ "red", "green", "blue", "magenta" };
fn colorOf(i: i64) []const u8 {
    return palette[@intCast(@mod(i, 4))];
}

// `note`: nullable, cycling NULL / "" / "yes" / "no". The empty string is a real
// distinct dict value; the NULL is masked by the validity bitmap.
fn noteIsNull(i: i64) bool {
    return @mod(i, 4) == 0;
}
fn noteOf(i: i64) []const u8 {
    return switch (@mod(i, 4)) {
        1 => "",
        2 => "yes",
        else => "no",
    };
}

/// Tag-agnostic string accessor (a VARCHAR column may surface as varchar/string/char).
fn strAt(val: anytype, i: usize) []const u8 {
    return switch (val.data) {
        .varchar, .string, .char => |sv| sv.rowBytes(i),
        else => unreachable,
    };
}

fn setupTable(allocator: std.mem.Allocator, db: anytype, rows: i64, id_base: i64) !void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO t (id, color, uniq, note) VALUES ");
    var line: [160]u8 = undefined;
    var i: i64 = 0;
    while (i < rows) : (i += 1) {
        if (i != 0) try buf.appendSlice(allocator, ", ");
        const id = id_base + i;
        if (noteIsNull(i)) {
            const s = try std.fmt.bufPrint(&line, "({d}, '{s}', 'u{d}', NULL)", .{ id, colorOf(i), id });
            try buf.appendSlice(allocator, s);
        } else {
            const s = try std.fmt.bufPrint(&line, "({d}, '{s}', 'u{d}', '{s}')", .{ id, colorOf(i), id, noteOf(i) });
            try buf.appendSlice(allocator, s);
        }
    }
    try exec(allocator, db, buf.items);
}

const Segment = struct { base: i64, rows: i64 };

fn idToIndex(segments: []const Segment, id: i64) i64 {
    for (segments) |seg| {
        if (id >= seg.base and id < seg.base + seg.rows) return id - seg.base;
    }
    unreachable;
}

fn verify(allocator: std.mem.Allocator, db: anytype, segments: []const Segment) !void {
    var q = try runSql(allocator, db, "SELECT id, color, uniq, note FROM t ORDER BY id");
    defer q.deinit();

    var expected_rows: usize = 0;
    for (segments) |seg| expected_rows += @intCast(seg.rows);

    var seen: usize = 0;
    var saw_empty = false;
    var saw_null = false;
    var line: [32]u8 = undefined;
    while (try q.next()) |batch| {
        var j: usize = 0;
        while (j < batch.row_count) : (j += 1) {
            const id = batch.values[0].data.bigint[j];
            const i = idToIndex(segments, id);

            try std.testing.expectEqualStrings(colorOf(i), strAt(batch.values[1], j));

            const want_uniq = try std.fmt.bufPrint(&line, "u{d}", .{id});
            try std.testing.expectEqualStrings(want_uniq, strAt(batch.values[2], j));

            if (noteIsNull(i)) {
                try std.testing.expect(!batch.values[3].isValid(j));
                saw_null = true;
            } else {
                try std.testing.expect(batch.values[3].isValid(j));
                const want_note = noteOf(i);
                try std.testing.expectEqualStrings(want_note, strAt(batch.values[3], j));
                if (want_note.len == 0) saw_empty = true;
            }
            seen += 1;
        }
    }
    try std.testing.expectEqual(expected_rows, seen);
    try std.testing.expect(saw_empty); // empty string survived as a non-null value
    try std.testing.expect(saw_null);
}

/// GROUP BY over the dict-encoded column: each of the 4 colors appears in
/// exactly a quarter of the rows.
fn verifyGroupBy(allocator: std.mem.Allocator, db: anytype, total_rows: i64) !void {
    var q = try runSql(allocator, db, "SELECT color, COUNT(*) FROM t GROUP BY color ORDER BY color");
    defer q.deinit();

    const expected = [_]struct { color: []const u8, count: i64 }{
        .{ .color = "blue", .count = @divExact(total_rows, 4) },
        .{ .color = "green", .count = @divExact(total_rows, 4) },
        .{ .color = "magenta", .count = @divExact(total_rows, 4) },
        .{ .color = "red", .count = @divExact(total_rows, 4) },
    };

    var idx: usize = 0;
    while (try q.next()) |batch| {
        var j: usize = 0;
        while (j < batch.row_count) : (j += 1) {
            try std.testing.expectEqualStrings(expected[idx].color, strAt(batch.values[0], j));
            try std.testing.expectEqual(expected[idx].count, batch.values[1].data.bigint[j]);
            idx += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 4), idx);
}

test "dict-encoded string columns round-trip through flush (single segment, multi row group)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 256 });
    defer db.close();
    try exec(allocator, db,
        \\CREATE TABLE t (
        \\  id BIGINT PRIMARY KEY, color VARCHAR(16) NOT NULL,
        \\  uniq VARCHAR(32) NOT NULL, note VARCHAR(16)
        \\)
    );

    try setupTable(allocator, db, ROWS, 1);
    const t = try db.openTable("t", .{});
    try t.flush();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    try verify(allocator, db, &.{.{ .base = 1, .rows = ROWS }});
    try verifyGroupBy(allocator, db, ROWS);
}

fn expectCount(allocator: std.mem.Allocator, db: anytype, sql: []const u8, want: i64) !void {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var got: i64 = 0;
    while (try q.next()) |b| {
        if (b.row_count > 0) got = b.values[0].data.bigint[0];
    }
    try std.testing.expectEqual(want, got);
}

test "GROUP BY on dict codes (forced hash) returns correct groups" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 256 });
    defer db.close();
    try exec(allocator, db,
        \\CREATE TABLE t (
        \\  id BIGINT PRIMARY KEY, color VARCHAR(16) NOT NULL,
        \\  uniq VARCHAR(32) NOT NULL, note VARCHAR(16)
        \\)
    );
    try setupTable(allocator, db, ROWS, 1);
    const t = try db.openTable("t", .{});
    try t.flush(); // color (4 distinct, NOT NULL) dict-encoded on disk

    // Force the hash GROUP BY path so the Phase 4.2 gate fires (single
    // dict-string key, no WHERE, COUNT(*) doesn't read the key, flushed).
    const saved = thindb.exec.force_group_by;
    thindb.exec.force_group_by = .hash;
    defer thindb.exec.force_group_by = saved;

    try verifyGroupBy(allocator, db, ROWS);
}

test "dict predicate pushdown returns correct rows (=, <>, range, LIKE)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 256 });
    defer db.close();
    try exec(allocator, db,
        \\CREATE TABLE t (
        \\  id BIGINT PRIMARY KEY, color VARCHAR(16) NOT NULL,
        \\  uniq VARCHAR(32) NOT NULL, note VARCHAR(16)
        \\)
    );
    try setupTable(allocator, db, ROWS, 1);
    const t = try db.openTable("t", .{});
    try t.flush(); // low-card color/note now dict-encoded on disk

    const quarter = @divExact(ROWS, 4);
    // color cycles red(0)/green(1)/blue(2)/magenta(3). thinDB allows only =/<>
    // on string columns (range ops rejected at validation), plus LIKE.
    try expectCount(allocator, db, "SELECT COUNT(*) FROM t WHERE color = 'red'", quarter);
    try expectCount(allocator, db, "SELECT COUNT(*) FROM t WHERE color <> 'red'", ROWS - quarter);
    try expectCount(allocator, db, "SELECT COUNT(*) FROM t WHERE color LIKE 'g%'", quarter); // green
    try expectCount(allocator, db, "SELECT COUNT(*) FROM t WHERE color LIKE '%re%'", quarter * 2); // red, green

    // note: NULL (i%4==0), '' (1), 'yes' (2), 'no' (3). NULL/'' excluded by <>''.
    try expectCount(allocator, db, "SELECT COUNT(*) FROM t WHERE note = 'yes'", quarter);
    try expectCount(allocator, db, "SELECT COUNT(*) FROM t WHERE note <> ''", quarter * 2); // yes + no
    try expectCount(allocator, db, "SELECT COUNT(*) FROM t WHERE note LIKE 'y%'", quarter); // yes
}

fn checkStringRanges(allocator: std.mem.Allocator, db: anytype) !void {
    // 400 rows, s cycles apple/banana/cherry/date (lexicographic order as listed),
    // 100 of each.
    try expectCount(allocator, db, "SELECT COUNT(*) FROM r WHERE s = 'cherry'", 100);
    try expectCount(allocator, db, "SELECT COUNT(*) FROM r WHERE s <> 'apple'", 300);
    try expectCount(allocator, db, "SELECT COUNT(*) FROM r WHERE s < 'banana'", 100); // apple
    try expectCount(allocator, db, "SELECT COUNT(*) FROM r WHERE s <= 'banana'", 200); // apple, banana
    try expectCount(allocator, db, "SELECT COUNT(*) FROM r WHERE s > 'apple'", 300); // banana, cherry, date
    try expectCount(allocator, db, "SELECT COUNT(*) FROM r WHERE s >= 'cherry'", 200); // cherry, date
}

test "string range comparisons evaluate over raw (memtable) and dict (segment)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 256 });
    defer db.close();
    try exec(allocator, db, "CREATE TABLE r (id BIGINT PRIMARY KEY, s VARCHAR(16) NOT NULL)");

    const words = [_][]const u8{ "apple", "banana", "cherry", "date" };
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO r (id, s) VALUES ");
    var line: [64]u8 = undefined;
    var i: i64 = 0;
    while (i < 400) : (i += 1) {
        if (i != 0) try buf.appendSlice(allocator, ", ");
        const s = try std.fmt.bufPrint(&line, "({d}, '{s}')", .{ i, words[@intCast(@mod(i, 4))] });
        try buf.appendSlice(allocator, s);
    }
    try exec(allocator, db, buf.items);

    // Memtable path: rows not yet flushed → raw per-row lexicographic compare.
    try checkStringRanges(allocator, db);

    // Dict path: flush → low-card `s` is dict-encoded; ranges evaluate on codes.
    const t = try db.openTable("r", .{});
    try t.flush();
    try checkStringRanges(allocator, db);
}

test "dict-encoded string columns round-trip across multiple segments" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 256 });
    defer db.close();
    try exec(allocator, db,
        \\CREATE TABLE t (
        \\  id BIGINT PRIMARY KEY, color VARCHAR(16) NOT NULL,
        \\  uniq VARCHAR(32) NOT NULL, note VARCHAR(16)
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
    try verifyGroupBy(allocator, db, ROWS * 2);
}
