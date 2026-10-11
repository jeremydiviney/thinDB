//! A comparison of a monotone date function of a DATE column with a constant
//! gains the column range it implies (`predicate_pushdown.deriveMonotoneBounds`),
//! which scans prune and filter by. The original comparison still decides
//! each row, so every result here must match the plain column predicate.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;

/// Rows on every day from 2023-11-15 through 2024-03-10 plus NULL dates,
/// flushed in two segments that split at 2024-01-01.
fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, d DATE NULL, v INT NOT NULL)");
    const t = try db.openTable("t", .{});
    const segment_days = [_][2]usize{ .{ 0, 47 }, .{ 47, 117 } };
    for (segment_days) |span| {
        var sql: std.ArrayList(u8) = .empty;
        defer sql.deinit(allocator);
        try sql.appendSlice(allocator, "INSERT INTO t (id, d, v) VALUES ");
        for (span[0]..span[1]) |day| {
            if (day > span[0]) try sql.appendSlice(allocator, ", ");
            try sql.print(allocator, "({d}, DATE '2023-11-15' + INTERVAL {d} DAY, {d})", .{ day, day, day });
        }
        try sql.print(allocator, ", ({d}, NULL, 0)", .{1000 + span[0]});
        try exec(allocator, db, sql.items);
        try t.flush();
    }
    return db;
}

fn countWhere(allocator: std.mem.Allocator, db: anytype, where: []const u8) !i64 {
    const sql = try std.fmt.allocPrint(allocator, "SELECT COUNT(*) FROM t WHERE {s}", .{where});
    defer allocator.free(sql);
    const counts = try helpers.collectBigints(allocator, db, sql);
    defer allocator.free(counts);
    return counts[0];
}

test "date function ranges: computed comparisons count what the plain column range counts" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = [_][2][]const u8{
        .{
            "ADDDATE(LAST_DAY(SUBDATE(d, INTERVAL 1 MONTH)), 1) >= '2024-01-01T00:00:00.000Z' AND ADDDATE(LAST_DAY(SUBDATE(d, INTERVAL 1 MONTH)), 1) < '2024-03-01T00:00:00.000Z'",
            "d >= '2024-01-01' AND d < '2024-03-01'",
        },
        .{ "YEAR(d) = 2024", "d >= '2024-01-01' AND d <= '2024-12-31'" },
        .{ "YEAR(d) BETWEEN 2023 AND 2023", "d <= '2023-12-31'" },
        .{ "LAST_DAY(d) <= '2024-01-15'", "d <= '2023-12-31'" },
        .{ "d + INTERVAL 10 DAY > '2024-01-10'", "d >= '2024-01-01'" },
        .{ "MONTH(d) = 1", "d >= '2024-01-01' AND d <= '2024-01-31'" },
        .{ "YEAR(d) = 2025", "d >= '2025-01-01'" },
    };
    for (cases) |c| {
        const computed = try countWhere(allocator, db, c[0]);
        const plain = try countWhere(allocator, db, c[1]);
        std.testing.expectEqual(plain, computed) catch |err| {
            std.debug.print("computed: {s}\n", .{c[0]});
            return err;
        };
    }
    try std.testing.expectEqual(@as(i64, 60), try countWhere(allocator, db, cases[0][0]));
}
