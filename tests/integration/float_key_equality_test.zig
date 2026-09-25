//! -0.0 and 0.0 are one value, and so is every NaN, wherever float values
//! meet as keys: GROUP BY, DISTINCT, COUNT(DISTINCT), joins, window
//! partitions, zone-map pruning and unique keys (#82). The matrix runs each
//! query from the memtable and from flushed segments, at DOP 1 and 4, with
//! and without enough filler rows to move GROUP BY and COUNT(DISTINCT) off
//! their small-input strategies.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

const Row = struct { id: i64, g: i32, d: f64 };

const nan_neg: f64 = @bitCast(@as(u64, 0xFFF8_0000_0000_0000));
const nan_payload: f64 = @bitCast(@as(u64, 0x7FF8_0000_0000_0001));

// Two row groups of -0.0 alone and one of 0.0 alone at row_group_size 2.
const special = [_]Row{
    .{ .id = 1, .g = 1, .d = -0.0 },
    .{ .id = 2, .g = 2, .d = -0.0 },
    .{ .id = 3, .g = 1, .d = 0.0 },
    .{ .id = 4, .g = 2, .d = 0.0 },
    .{ .id = 5, .g = 1, .d = 1.5 },
    .{ .id = 6, .g = 1, .d = std.math.nan(f64) },
    .{ .id = 7, .g = 2, .d = nan_neg },
    .{ .id = 8, .g = 2, .d = nan_payload },
};

fn count(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) !i64 {
    const got = try helpers.collectBigints(allocator, db, sql);
    defer allocator.free(got);
    if (got.len != 1) return error.TestExpectedOneRow;
    return got[0];
}

fn expectCount(allocator: std.mem.Allocator, db: *thindb.Database, want: i64, sql: []const u8) !void {
    const got = try count(allocator, db, sql);
    if (got != want) {
        std.debug.print("{s}\n  want {d}, got {d}\n", .{ sql, want, got });
        return error.TestUnexpectedResult;
    }
}

fn checkKeys(allocator: std.mem.Allocator, db: *thindb.Database, filler: i64) !void {
    const distinct = 3 + filler;
    try expectCount(allocator, db, distinct, "SELECT COUNT(DISTINCT d) FROM f");
    try expectCount(allocator, db, distinct, "WITH x AS (SELECT d FROM f GROUP BY d) SELECT COUNT(*) FROM x");
    try expectCount(allocator, db, distinct, "WITH x AS (SELECT DISTINCT d FROM f) SELECT COUNT(*) FROM x");
    try expectCount(allocator, db, 2, "WITH x AS (SELECT g, d FROM f WHERE id <= 8 GROUP BY g, d) SELECT COUNT(*) FROM x WHERE g = 2");

    const per_group = try helpers.collectBigints(allocator, db, "SELECT COUNT(DISTINCT d) AS n FROM f WHERE id <= 8 GROUP BY g ORDER BY g");
    defer allocator.free(per_group);
    try std.testing.expectEqualSlices(i64, &.{ 3, 2 }, per_group);

    // Every zero lands in one group, and its key reads back as +0.0.
    var q = try helpers.runSql(allocator, db, "SELECT d, COUNT(*) AS c FROM f WHERE id <= 8 GROUP BY d ORDER BY d");
    defer q.deinit();
    var groups: std.ArrayList(struct { d: f64, c: i64 }) = .empty;
    defer groups.deinit(allocator);
    while (try q.next()) |b| {
        for (0..b.row_count) |r| try groups.append(allocator, .{ .d = b.values[0].data.double[r], .c = b.values[1].data.bigint[r] });
    }
    try std.testing.expectEqual(@as(usize, 3), groups.items.len);
    try std.testing.expectEqual(@as(u64, 0), @as(u64, @bitCast(groups.items[0].d)));
    try std.testing.expectEqual(@as(i64, 4), groups.items[0].c);
    try std.testing.expectEqual(@as(f64, 1.5), groups.items[1].d);
    try std.testing.expect(std.math.isNan(groups.items[2].d));
    try std.testing.expectEqual(@as(i64, 3), groups.items[2].c);

    // 4 zeros × 4, 1.5 × 1, 3 NaNs × 3, each filler with itself.
    try expectCount(allocator, db, 26 + filler, "SELECT COUNT(*) FROM f a JOIN f b ON a.d = b.d");
    try expectCount(allocator, db, 7, "SELECT COUNT(*) FROM f JOIN z ON f.d = z.d");
    try expectCount(allocator, db, 5, "WITH w AS (SELECT ROW_NUMBER() OVER (PARTITION BY d ORDER BY id) AS rn FROM f WHERE id <= 8) SELECT COUNT(*) FROM w WHERE rn > 1");

    // A NaN sorts last in ORDER BY over an aggregate; before, std.math.order
    // reached `unreachable` on it. MIN and MAX skip NaN.
    var o = try helpers.runSql(allocator, db, "SELECT g, SUM(d) AS s, MIN(d) AS lo, MAX(d) AS hi FROM f WHERE id <= 6 GROUP BY g ORDER BY s");
    defer o.deinit();
    var order: std.ArrayList(i32) = .empty;
    defer order.deinit(allocator);
    while (try o.next()) |b| {
        for (0..b.row_count) |r| {
            try order.append(allocator, b.values[0].data.int[r]);
            try std.testing.expectEqual(@as(f64, 0), b.values[2].data.double[r]);
            try std.testing.expectEqual(@as(f64, if (b.values[0].data.int[r] == 1) 1.5 else 0), b.values[3].data.double[r]);
        }
    }
    try std.testing.expectEqualSlices(i32, &.{ 2, 1 }, order.items);

    try expectCount(allocator, db, 4, "SELECT COUNT(*) FROM f WHERE d = 0");
    try expectCount(allocator, db, 4, "SELECT COUNT(*) FROM f WHERE d <= 0");
    try expectCount(allocator, db, 4, "SELECT COUNT(*) FROM f WHERE d >= 0 AND d < 1");
    try expectCount(allocator, db, 0, "SELECT COUNT(*) FROM f WHERE d < 0");
}

test "float keys treat -0.0 as 0.0 and every NaN as one value" {
    const allocator = std.testing.allocator;
    inline for (.{ 1, 4 }) |dop| {
        inline for (.{ false, true }) |flushed| {
            inline for (.{ 0, 20_000 }) |filler| {
                var tmp = std.testing.tmpDir(.{});
                defer tmp.cleanup();
                const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .row_group_size = 2 });
                defer db.close();
                try helpers.exec(allocator, db, "CREATE TABLE f (id BIGINT PRIMARY KEY, g INT NOT NULL, d DOUBLE NOT NULL)");
                try helpers.exec(allocator, db, "CREATE TABLE z (k BIGINT PRIMARY KEY, d DOUBLE NOT NULL)");

                const rows = try allocator.alloc(Row, special.len + filler);
                defer allocator.free(rows);
                @memcpy(rows[0..special.len], &special);
                for (rows[special.len..], 0..) |*r, i| r.* = .{ .id = @intCast(100 + i), .g = 3, .d = 1000 + @as(f64, @floatFromInt(i)) };
                const f = try db.openTable("f", .{});
                try f.insert(rows);
                const z = try db.openTable("z", .{});
                try z.insert(&[_]struct { k: i64, d: f64 }{ .{ .k = 1, .d = -0.0 }, .{ .k = 2, .d = nan_payload } });
                if (flushed) {
                    try f.flush();
                    try z.flush();
                }

                checkKeys(allocator, db, filler) catch |err| {
                    std.debug.print("dop={d} flushed={} filler={d}\n", .{ dop, flushed, filler });
                    return err;
                };
            }
        }
    }
}

test "a float unique key upserts and deletes -0.0 as 0.0" {
    const allocator = std.testing.allocator;
    inline for (.{ false, true }) |flushed| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
        defer db.close();
        try helpers.exec(allocator, db, "CREATE TABLE u (d DOUBLE PRIMARY KEY, v BIGINT NOT NULL)");
        const u = try db.openTable("u", .{});
        const U = struct { d: f64, v: i64 };

        try u.insert(&[_]U{ .{ .d = -0.0, .v = 1 }, .{ .d = std.math.nan(f64), .v = 1 } });
        if (flushed) try u.flush();
        try u.insert(&[_]U{ .{ .d = 0.0, .v = 2 }, .{ .d = nan_neg, .v = 2 } });
        if (flushed) try u.flush();

        try expectCount(allocator, db, 2, "SELECT COUNT(*) FROM u");
        try expectCount(allocator, db, 2, "SELECT v FROM u WHERE d = 0");
        try helpers.exec(allocator, db, "DELETE FROM u WHERE d = 0");
        try expectCount(allocator, db, 1, "SELECT COUNT(*) FROM u");
    }
}
