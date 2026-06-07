//! FOR-narrow inline-state fast path for `GROUP BY <single int col> …
//! {SUM|MIN|MAX}(<int col>)`. The optimization FOR-normalizes the key into a
//! narrow slot holding the running accumulator, then lowers each slot into the
//! standard `gstate`/`gkeys_int` at emit — so the result must match a
//! brute-force reference computed directly from the inserted multiset. Covers:
//! a negative FOR base, the min/max key boundaries, more groups than the
//! initial presize (forces grows), a single-group case, and the nullable-key
//! fallback (which must take the canonical path and still be correct).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

/// Bounded key range with a negative base. 600 distinct keys × ~6 rows each
/// ⇒ >2000 rows, far past the small initial presize so the inline table grows.
/// The key range is [-200, 399] (base = −200), the values span negatives and
/// positives including the i32 boundaries-of-interest for MIN/MAX.
const KEY_MIN: i64 = -200;
const KEY_MAX: i64 = 399;
const N_KEYS: i64 = KEY_MAX - KEY_MIN + 1; // 600 distinct keys

fn keyFor(i: i64) i64 {
    return KEY_MIN + @mod(i, N_KEYS);
}

/// Deterministic, signed values spanning negatives/positives. Row 0 of every
/// key sees a large-magnitude value so the MIN/MAX boundaries are exercised.
fn valFor(i: i64, r: i64) i64 {
    if (r == 0) return (@mod(i, 2) * 2 - 1) * 1_000_000; // ±1e6 anchors
    return @mod(i * 31 + r * 7, 4001) - 2000; // [-2000, 2000]
}

const ROWS_PER_KEY: i64 = 6; // 600 * 6 = 3600 rows

const Ref = struct { sum: i64, min: i64, max: i64, n: i64 };

fn buildReference(allocator: std.mem.Allocator) !std.AutoHashMap(i64, Ref) {
    var ref = std.AutoHashMap(i64, Ref).init(allocator);
    errdefer ref.deinit();
    var i: i64 = 0;
    while (i < N_KEYS) : (i += 1) {
        const k = keyFor(i);
        var r: i64 = 0;
        while (r < ROWS_PER_KEY) : (r += 1) {
            const v = valFor(i, r);
            const gop = try ref.getOrPut(k);
            if (!gop.found_existing) {
                gop.value_ptr.* = .{ .sum = v, .min = v, .max = v, .n = 1 };
            } else {
                gop.value_ptr.sum += v;
                gop.value_ptr.min = @min(gop.value_ptr.min, v);
                gop.value_ptr.max = @max(gop.value_ptr.max, v);
                gop.value_ptr.n += 1;
            }
        }
    }
    return ref;
}

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype, nullable_key: bool) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    const ddl = if (nullable_key)
        "CREATE TABLE t (id BIGINT PRIMARY KEY, k INT, v BIGINT)"
    else
        "CREATE TABLE t (id BIGINT PRIMARY KEY, k INT NOT NULL, v BIGINT)";
    try exec(allocator, db, ddl);

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO t (id, k, v) VALUES ");
    var id: i64 = 1;
    var first = true;
    var line: [96]u8 = undefined;
    var i: i64 = 0;
    while (i < N_KEYS) : (i += 1) {
        const k = keyFor(i);
        var r: i64 = 0;
        while (r < ROWS_PER_KEY) : (r += 1) {
            if (!first) try buf.appendSlice(allocator, ", ");
            first = false;
            const s = try std.fmt.bufPrint(&line, "({d}, {d}, {d})", .{ id, k, valFor(i, r) });
            try buf.appendSlice(allocator, s);
            id += 1;
        }
    }
    try exec(allocator, db, buf.items);
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

/// Runs `SELECT k, <agg>(v) FROM t GROUP BY k` and asserts every emitted group
/// matches the brute-force reference for the chosen aggregate.
fn assertAgg(allocator: std.mem.Allocator, db: anytype, comptime agg: []const u8, comptime field: []const u8) !void {
    var ref = try buildReference(allocator);
    defer ref.deinit();

    const sql = "SELECT k, " ++ agg ++ "(v) AS a FROM t GROUP BY k";
    var q = try runSql(allocator, db, sql);
    defer q.deinit();

    var seen = std.AutoHashMap(i64, void).init(allocator);
    defer seen.deinit();
    var rows: usize = 0;
    var saw_min_key = false;
    var saw_max_key = false;
    while (try q.next()) |batch| {
        var j: usize = 0;
        while (j < batch.row_count) : (j += 1) {
            const k: i64 = batch.values[0].data.int[j];
            try std.testing.expect(!seen.contains(k)); // no duplicate groups
            try seen.put(k, {});
            const want = ref.get(k) orelse return error.UnexpectedGroup;
            // SUM over a 64-bit int widens to LARGEINT (i128); MIN/MAX stay i64.
            if (comptime std.mem.eql(u8, agg, "SUM")) {
                try std.testing.expectEqual(@as(i128, @field(want, field)), batch.values[1].data.largeint[j]);
            } else {
                try std.testing.expectEqual(@field(want, field), batch.values[1].data.bigint[j]);
            }
            if (k == KEY_MIN) saw_min_key = true;
            if (k == KEY_MAX) saw_max_key = true;
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, @intCast(N_KEYS)), rows);
    try std.testing.expect(saw_min_key);
    try std.testing.expect(saw_max_key);
}

test "GROUP BY int col SUM(v): matches the brute-force reference (negative base, boundaries)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir, false);
    defer db.close();
    try assertAgg(allocator, db, "SUM", "sum");
}

test "GROUP BY int col MIN(v): matches the brute-force reference" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir, false);
    defer db.close();
    try assertAgg(allocator, db, "MIN", "min");
}

test "GROUP BY int col MAX(v): matches the brute-force reference" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir, false);
    defer db.close();
    try assertAgg(allocator, db, "MAX", "max");
}

// A single-group case (every row shares one key) — range = 0, the narrowest
// (u8) FOR tier. SUM/MIN/MAX must still equal the global reference.
test "GROUP BY int col with a single group: SUM/MIN/MAX over one group" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, k INT NOT NULL, v BIGINT)");

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO t (id, k, v) VALUES ");
    var expect_sum: i64 = 0;
    var expect_min: i64 = std.math.maxInt(i64);
    var expect_max: i64 = std.math.minInt(i64);
    var line: [96]u8 = undefined;
    var id: i64 = 1;
    const ROWS: i64 = 3000;
    while (id <= ROWS) : (id += 1) {
        const v = @mod(id * 17 - 9, 5001) - 2500; // [-2500, 2500]
        expect_sum += v;
        expect_min = @min(expect_min, v);
        expect_max = @max(expect_max, v);
        if (id != 1) try buf.appendSlice(allocator, ", ");
        const s = try std.fmt.bufPrint(&line, "({d}, 42, {d})", .{ id, v });
        try buf.appendSlice(allocator, s);
    }
    try exec(allocator, db, buf.items);
    const t = try db.openTable("t", .{});
    try t.flush();

    inline for (.{
        .{ .agg = "SUM", .want = &expect_sum },
        .{ .agg = "MIN", .want = &expect_min },
        .{ .agg = "MAX", .want = &expect_max },
    }) |c| {
        var q = try runSql(allocator, db, "SELECT k, " ++ c.agg ++ "(v) AS a FROM t GROUP BY k");
        defer q.deinit();
        var rows: usize = 0;
        while (try q.next()) |batch| {
            var j: usize = 0;
            while (j < batch.row_count) : (j += 1) {
                try std.testing.expectEqual(@as(i64, 42), batch.values[0].data.int[j]);
                if (comptime std.mem.eql(u8, c.agg, "SUM")) {
                    try std.testing.expectEqual(@as(i128, c.want.*), batch.values[1].data.largeint[j]);
                } else {
                    try std.testing.expectEqual(c.want.*, batch.values[1].data.bigint[j]);
                }
                rows += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 1), rows);
    }
}

// Two aggregates disqualify the inline-FOR path (gate requires exactly one),
// so this falls back to the canonical int path. Both aggregates must match the
// reference — a direct check that the multi-agg fallback gate is correct.
test "GROUP BY int col SUM(v), MIN(v): two aggs fall back, both correct" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir, false);
    defer db.close();

    var ref = try buildReference(allocator);
    defer ref.deinit();

    var q = try runSql(allocator, db, "SELECT k, SUM(v) AS s, MIN(v) AS m FROM t GROUP BY k");
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |batch| {
        var j: usize = 0;
        while (j < batch.row_count) : (j += 1) {
            const k: i64 = batch.values[0].data.int[j];
            const s: i128 = batch.values[1].data.largeint[j]; // SUM(bigint) → LARGEINT
            const m: i64 = batch.values[2].data.bigint[j];
            const want = ref.get(k) orelse return error.UnexpectedGroup;
            try std.testing.expectEqual(@as(i128, want.sum), s);
            try std.testing.expectEqual(want.min, m);
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, @intCast(N_KEYS)), rows);
}

// A nullable group column disqualifies the inline-FOR path (the gate requires
// non-nullable), so the query must fall back to the canonical int path and
// still compute every group's SUM correctly. The data here has no actual NULLs
// — the nullability of the *column* alone is what trips the gate — so the
// emitted groups equal the brute-force reference exactly.
test "GROUP BY nullable int col SUM(v): falls back to the canonical path, correct sums" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir, true);
    defer db.close();

    var ref = try buildReference(allocator);
    defer ref.deinit();

    var q = try runSql(allocator, db, "SELECT k, SUM(v) AS a FROM t GROUP BY k");
    defer q.deinit();

    var seen = std.AutoHashMap(i64, void).init(allocator);
    defer seen.deinit();
    var rows: usize = 0;
    while (try q.next()) |batch| {
        var j: usize = 0;
        while (j < batch.row_count) : (j += 1) {
            const k: i64 = batch.values[0].data.int[j];
            const a: i128 = batch.values[1].data.largeint[j]; // SUM(bigint) → LARGEINT
            try std.testing.expect(!seen.contains(k));
            try seen.put(k, {});
            const want = ref.get(k) orelse return error.UnexpectedGroup;
            try std.testing.expectEqual(@as(i128, want.sum), a);
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, @intCast(N_KEYS)), rows);
}
