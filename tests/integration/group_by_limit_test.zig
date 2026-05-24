//! Unordered `GROUP BY … LIMIT n` emit-cap fusion. With no ORDER BY the
//! result rows are unspecified, so the hash aggregate emits only the first
//! `n + offset` groups (group-insertion order) instead of materializing every
//! group for the downstream Limit to discard. The build still consumes all
//! input, so the emitted groups' counts are exact. A HAVING between the Limit
//! and the GroupBy must NOT be capped — it changes which groups qualify.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

/// 50 groups keyed by `g`, group i has exactly (i % 5) + 1 rows — so the
/// per-group counts are known and verifiable regardless of which subset the
/// cap emits.
fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, g INT NOT NULL)");

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO t (id, g) VALUES ");
    var id: i64 = 1;
    var first = true;
    var grp: i64 = 0;
    var line: [64]u8 = undefined;
    while (grp < 50) : (grp += 1) {
        const reps: i64 = @mod(grp, 5) + 1;
        var r: i64 = 0;
        while (r < reps) : (r += 1) {
            if (!first) try buf.appendSlice(allocator, ", ");
            first = false;
            const s = try std.fmt.bufPrint(&line, "({d}, {d})", .{ id, grp });
            try buf.appendSlice(allocator, s);
            id += 1;
        }
    }
    try exec(allocator, db, buf.items);
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

fn expectedCount(g: i64) i64 {
    return @mod(g, 5) + 1;
}

test "GROUP BY ... LIMIT n (no ORDER BY): exactly n groups, correct counts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT g, COUNT(*) AS c FROM t GROUP BY g LIMIT 10");
    defer q.deinit();

    var rows: usize = 0;
    var seen = std.AutoHashMap(i64, void).init(allocator);
    defer seen.deinit();
    while (try q.next()) |batch| {
        var i: usize = 0;
        while (i < batch.row_count) : (i += 1) {
            const g = batch.values[0].data.int[i];
            const c = batch.values[1].data.bigint[i];
            try std.testing.expectEqual(expectedCount(g), c);
            try std.testing.expect(g >= 0 and g < 50);
            try std.testing.expect(!seen.contains(g)); // no duplicate groups
            try seen.put(g, {});
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 10), rows);
}

test "GROUP BY ... LIMIT n OFFSET m (no ORDER BY): n groups after skipping m" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT g, COUNT(*) AS c FROM t GROUP BY g LIMIT 5 OFFSET 7");
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |batch| {
        var i: usize = 0;
        while (i < batch.row_count) : (i += 1) {
            const g = batch.values[0].data.int[i];
            const c = batch.values[1].data.bigint[i];
            try std.testing.expectEqual(expectedCount(g), c);
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 5), rows);
}

test "GROUP BY ... LIMIT n: limit above group count emits all groups" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(allocator, db, "SELECT g, COUNT(*) AS c FROM t GROUP BY g LIMIT 1000");
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |batch| rows += batch.row_count;
    try std.testing.expectEqual(@as(usize, 50), rows);
}

test "GROUP BY ... HAVING ... LIMIT: HAVING still filters (no wrong cap)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Groups with count = 5 are exactly those where g % 5 == 4: there are 10
    // of them (g = 4, 9, ..., 49). HAVING c = 5 filters to those. The emit
    // cap must NOT clip the GroupBy before the HAVING runs — capping at 3
    // groups in insertion order would (with the first groups being low-count)
    // wrongly yield zero qualifying rows. We expect exactly 3 rows, each a
    // genuine count-5 group.
    var q = try runSql(allocator, db, "SELECT g, COUNT(*) AS c FROM t GROUP BY g HAVING c = 5 LIMIT 3");
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |batch| {
        var i: usize = 0;
        while (i < batch.row_count) : (i += 1) {
            const g = batch.values[0].data.int[i];
            const c = batch.values[1].data.bigint[i];
            try std.testing.expectEqual(@as(i64, 5), c);
            try std.testing.expectEqual(@as(i64, 4), @mod(g, 5));
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), rows);
}
