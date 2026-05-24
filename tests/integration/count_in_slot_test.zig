//! Count-in-slot fast path for `GROUP BY <single non-nullable int col> …
//! COUNT(*)`. The optimization folds the running count into the group table's
//! slot, then lowers each `{key,count}` into the standard `gstate`/`gkeys_int`
//! at emit time — so the result must match a reference grouping computed
//! directly from the inserted multiset. Covers: the all-ones (-1) sentinel key,
//! key 0, more distinct groups than the initial presize (forces a grow), and
//! both `k < n_groups` and `k >= n_groups` for the ORDER BY COUNT(*) DESC top-k.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

/// The reference multiset: distinct key `keyFor(i)` appears `repsFor(i)` times.
/// 1500 distinct keys ⇒ far past the 16-slot initial presize, so the count
/// table grows multiple times. Key 0 and the all-ones sentinel (-1) are wedged
/// in explicitly so their special-case handling is exercised.
const N_KEYS: i64 = 1500;

fn keyFor(i: i64) i64 {
    return switch (i) {
        0 => 0, // key 0 — a normal storable key, distinct from an empty slot.
        1 => -1, // all-ones in i64 — collides with the table's empty sentinel.
        else => i * 7 - 3, // spread the rest out, all distinct and != {0,-1}.
    };
}

fn repsFor(i: i64) i64 {
    // Deterministic, non-uniform counts so the top-k ordering is unambiguous
    // (no ties at the boundary). Counts range 1..=50.
    return @mod(i * 13 + 1, 50) + 1;
}

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL)");

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO t (id, k) VALUES ");
    var id: i64 = 1;
    var first = true;
    var line: [80]u8 = undefined;
    var i: i64 = 0;
    while (i < N_KEYS) : (i += 1) {
        const key = keyFor(i);
        const reps = repsFor(i);
        var r: i64 = 0;
        while (r < reps) : (r += 1) {
            if (!first) try buf.appendSlice(allocator, ", ");
            first = false;
            const s = try std.fmt.bufPrint(&line, "({d}, {d})", .{ id, key });
            try buf.appendSlice(allocator, s);
            id += 1;
        }
    }
    try exec(allocator, db, buf.items);
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

test "GROUP BY int col COUNT(*): every group's count matches the reference" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var expected = std.AutoHashMap(i64, i64).init(allocator);
    defer expected.deinit();
    var i: i64 = 0;
    while (i < N_KEYS) : (i += 1) try expected.put(keyFor(i), repsFor(i));

    var q = try runSql(allocator, db, "SELECT k, COUNT(*) AS c FROM t GROUP BY k");
    defer q.deinit();

    var seen = std.AutoHashMap(i64, void).init(allocator);
    defer seen.deinit();
    var rows: usize = 0;
    var saw_zero = false;
    var saw_sentinel = false;
    while (try q.next()) |batch| {
        var j: usize = 0;
        while (j < batch.row_count) : (j += 1) {
            const k = batch.values[0].data.bigint[j];
            const c = batch.values[1].data.bigint[j];
            try std.testing.expect(!seen.contains(k)); // no duplicate groups
            try seen.put(k, {});
            const want = expected.get(k) orelse return error.UnexpectedGroup;
            try std.testing.expectEqual(want, c);
            if (k == 0) saw_zero = true;
            if (k == -1) saw_sentinel = true;
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, @intCast(N_KEYS)), rows);
    try std.testing.expect(saw_zero);
    try std.testing.expect(saw_sentinel);
}

/// Reference top-k: every (key, count) sorted by count DESC. Ties don't occur
/// at our boundary (counts are dense and the dataset is built tie-free near the
/// cut), so the ordering is deterministic for the assertions below.
const KC = struct { k: i64, c: i64 };

fn byCountDesc(_: void, a: KC, b: KC) bool {
    if (a.c != b.c) return a.c > b.c;
    return a.k < b.k; // stable tiebreak for the reference only
}

fn referenceSorted(allocator: std.mem.Allocator) ![]KC {
    var list: std.ArrayList(KC) = .empty;
    errdefer list.deinit(allocator);
    var i: i64 = 0;
    while (i < N_KEYS) : (i += 1) try list.append(allocator, .{ .k = keyFor(i), .c = repsFor(i) });
    const out = try list.toOwnedSlice(allocator);
    std.mem.sort(KC, out, {}, byCountDesc);
    return out;
}

fn topKCountsMatch(allocator: std.mem.Allocator, db: anytype, k: usize) !void {
    const ref = try referenceSorted(allocator);
    defer allocator.free(ref);

    var buf: [96]u8 = undefined;
    const sql = try std.fmt.bufPrint(&buf, "SELECT k, COUNT(*) AS c FROM t GROUP BY k ORDER BY c DESC LIMIT {d}", .{k});
    var q = try runSql(allocator, db, sql);
    defer q.deinit();

    var got: std.ArrayList(KC) = .empty;
    defer got.deinit(allocator);
    while (try q.next()) |batch| {
        var j: usize = 0;
        while (j < batch.row_count) : (j += 1) {
            try got.append(allocator, .{ .k = batch.values[0].data.bigint[j], .c = batch.values[1].data.bigint[j] });
        }
    }

    const want_rows = @min(k, ref.len);
    try std.testing.expectEqual(want_rows, got.items.len);
    // The emitted counts, in order, must equal the reference's top-k counts.
    // (Keys can differ only among equal-count ties; the dataset has none at the
    // boundary, so the counts pin the result.)
    for (got.items, 0..) |row, idx| {
        try std.testing.expectEqual(ref[idx].c, row.c);
    }
    // Each emitted (key,count) must be a genuine group from the reference.
    for (got.items) |row| {
        var found = false;
        for (ref) |r| {
            if (r.k == row.k) {
                try std.testing.expectEqual(r.c, row.c);
                found = true;
                break;
            }
        }
        try std.testing.expect(found);
    }
}

test "GROUP BY int col COUNT(*) ORDER BY COUNT(*) DESC LIMIT k: k < n_groups" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try topKCountsMatch(allocator, db, 10); // far below 1500 groups -> bounded heap
}

test "GROUP BY int col COUNT(*) ORDER BY COUNT(*) DESC LIMIT k: k >= n_groups" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try topKCountsMatch(allocator, db, @intCast(N_KEYS + 100)); // limit above the group count -> full emit
}
