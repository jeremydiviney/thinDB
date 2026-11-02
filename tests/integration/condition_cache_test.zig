//! The condition cache (DESIGN.md §6.4) changes no result. Two databases take
//! the same writes, one with the cache off, and answer every query alike:
//! queries run twice so the second reads the records the first kept, filters
//! share AND arms so one filter starts from another's records, deletes land
//! after the records were kept, rows stay unflushed, and compaction writes
//! new segments.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

const queries = [_][]const u8{
    "SELECT COUNT(*) FROM t WHERE s LIKE '%ab%'",
    "SELECT id FROM t WHERE s LIKE '%ab%' ORDER BY id",
    "SELECT id FROM t WHERE s LIKE '%ab%' AND k < 4 ORDER BY id",
    "SELECT id FROM t WHERE k < 4 AND s LIKE '%ab%' AND u NOT LIKE '%c%' ORDER BY id",
    "SELECT id FROM t WHERE u NOT LIKE '%c%' ORDER BY id",
    "SELECT COUNT(*) FROM t WHERE u NOT LIKE '%c%'",
    "SELECT COUNT(*) FROM t WHERE u NOT LIKE '%c%' AND s LIKE '%ab%'",
    "SELECT COUNT(*) FROM t WHERE s LIKE '%ab%' GROUP BY u ORDER BY 1",
    "SELECT id FROM t WHERE s = 'abc' ORDER BY id",
    "SELECT id FROM t WHERE s <> '' AND s LIKE 'a%' ORDER BY id",
    "SELECT COUNT(*) FROM t WHERE s IN ('abc', 'xab') AND u LIKE '%b%'",
    "SELECT id FROM t WHERE s LIKE '%ab%' OR u LIKE '%zz%' ORDER BY id",
    "SELECT MAX(id) FROM t WHERE u LIKE '%b%' AND s NOT LIKE '%x%'",
};

const Row = struct { id: i64, k: i64, s: []const u8, u: []const u8 };

const ALPHABET = "abcxz";

fn word(random: std.Random, buf: []u8) []const u8 {
    const len = random.uintLessThan(usize, buf.len + 1);
    for (buf[0..len]) |*c| c.* = ALPHABET[random.uintLessThan(usize, ALPHABET.len)];
    return buf[0..len];
}

fn insertRows(allocator: std.mem.Allocator, dbs: anytype, random: std.Random, first_id: i64, n: usize) !void {
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    const text = try allocator.alloc(u8, n * 8);
    defer allocator.free(text);
    for (rows, 0..) |*r, i| r.* = .{
        .id = first_id + @as(i64, @intCast(i)),
        .k = random.intRangeLessThan(i64, 0, 8),
        .s = word(random, text[i * 8 ..][0..4]),
        .u = word(random, text[i * 8 + 4 ..][0..4]),
    };
    for (dbs) |db| try (try db.openTable("t", .{})).insert(rows);
}

test "the condition cache answers as a scan with no cache does, through repeats, shared arms, deletes, unflushed rows and compaction" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0xc0dec0de);
    const random = prng.random();

    inline for (.{ 1, 4 }) |dop| {
        var tmp_on = std.testing.tmpDir(.{});
        defer tmp_on.cleanup();
        var tmp_off = std.testing.tmpDir(.{});
        defer tmp_off.cleanup();
        const on = try thindb.Database.open(allocator, std.testing.io, tmp_on.dir, .{ .row_group_size = 64, .max_dop = dop, .auto_flush_secs = 0 });
        defer on.close();
        const off = try thindb.Database.open(allocator, std.testing.io, tmp_off.dir, .{ .row_group_size = 64, .max_dop = dop, .auto_flush_secs = 0, .condition_cache = false });
        defer off.close();
        const dbs = [_]*thindb.Database{ on, off };
        for (dbs) |db| try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, s VARCHAR(8) NOT NULL, u VARCHAR(8) NOT NULL)");

        var next_id: i64 = 0;
        try insertRows(allocator, dbs, random, next_id, 3000);
        next_id += 3000;
        for (dbs) |db| try (try db.openTable("t", .{})).flush();

        var sql: std.ArrayList(u8) = .empty;
        defer sql.deinit(allocator);
        for (0..6) |round| {
            for (queries) |q| for (0..2) |_| {
                const want = try collectBigints(allocator, off, q);
                defer allocator.free(want);
                const got = try collectBigints(allocator, on, q);
                defer allocator.free(got);
                std.testing.expectEqualSlices(i64, want, got) catch |err| {
                    std.debug.print("round {d}, dop {d}: {s}\n", .{ round, dop, q });
                    return err;
                };
            };

            sql.clearRetainingCapacity();
            try sql.appendSlice(allocator, "DELETE FROM t WHERE id IN (");
            for (0..40) |i| {
                if (i > 0) try sql.appendSlice(allocator, ", ");
                try sql.print(allocator, "{d}", .{random.intRangeLessThan(i64, 0, next_id)});
            }
            try sql.appendSlice(allocator, ")");
            for (dbs) |db| try exec(allocator, db, sql.items);

            try insertRows(allocator, dbs, random, next_id, 500);
            next_id += 500;
            for (dbs) |db| try (try db.openTable("t", .{})).flush();
            try insertRows(allocator, dbs, random, next_id, 30);
            next_id += 30;
            if (round == 3) for (dbs) |db| try (try db.openTable("t", .{})).compact();
        }
    }
}
