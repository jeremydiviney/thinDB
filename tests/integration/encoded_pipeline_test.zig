const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

test "encoded machinery combines disk runs raw blocks and memtable with tombstone fallback" {
    const allocator = std.testing.allocator;
    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .row_group_size = 128 });
        defer db.close();
        try helpers.exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, x SMALLINT NOT NULL, y BIGINT NOT NULL)");
        const table = try db.openTable("t", .{});
        const Row = struct { id: i64, x: i16, y: i64 };
        const rows = try allocator.alloc(Row, 1024);
        defer allocator.free(rows);
        var sum: i64 = 0;
        for (rows, 0..) |*row, i| {
            const x: i16 = if (i < 512) @as(i16, @intCast(i / 64)) - 4 else @as(i16, @intCast(i % 2)) * 200 - 100;
            row.* = .{ .id = @intCast(i), .x = x, .y = 7_000_000_000 + @as(i64, @intCast(i / 50)) };
            sum += x;
        }
        try table.insert(rows);
        try table.flush();
        try table.insert(&[_]Row{.{ .id = 1024, .x = -7, .y = 7_000_000_000 }});
        sum -= 7;
        for (0..2) |deleted| {
            if (deleted != 0) {
                try helpers.exec(allocator, db, "DELETE FROM t WHERE id = 0");
                sum -= rows[0].x;
            }
            var q = try helpers.runSql(allocator, db, "SELECT SUM(x), SUM(x+1), SUM(x-2), COUNT(x) FROM t");
            defer q.deinit();
            const batch = (try q.next()).?;
            const count: i64 = @intCast(1025 - deleted);
            try std.testing.expectEqual(sum, batch.values[0].data.bigint[0]);
            try std.testing.expectEqual(sum + count, batch.values[1].data.bigint[0]);
            try std.testing.expectEqual(sum - 2 * count, batch.values[2].data.bigint[0]);
            try std.testing.expectEqual(count, batch.values[3].data.bigint[0]);
            try std.testing.expect((try q.next()) == null);
        }
        var empty = try helpers.runSql(allocator, db, "SELECT SUM(x+1), SUM(x-2), COUNT(x) FROM t WHERE id < 0");
        defer empty.deinit();
        const batch = (try empty.next()).?;
        try std.testing.expect(!batch.values[0].isValid(0));
        try std.testing.expect(!batch.values[1].isValid(0));
        try std.testing.expectEqual(@as(i64, 0), batch.values[2].data.bigint[0]);
        try table.flush();
    }
}

test "encoded machinery count groups merge different run boundaries across workers" {
    const allocator = std.testing.allocator;
    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .row_group_size = 1024 });
        defer db.close();
        try helpers.exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, h BIGINT NOT NULL)");
        const table = try db.openTable("t", .{});
        const Row = struct { id: i64, g: i64, h: i64 };
        const rows = try allocator.alloc(Row, 32768);
        defer allocator.free(rows);
        var counts = [_][17]i64{[_]i64{0} ** 17} ** 31;
        for (rows, 0..) |*row, i| {
            const g = (i / 97) % 31;
            const h = (i / 43) % 17;
            row.* = .{ .id = @intCast(i), .g = 7_000_000_000 + @as(i64, @intCast(g)), .h = -8_000_000_000 + @as(i64, @intCast(h)) };
            counts[g][h] += 1;
        }
        try table.insert(rows[0..16384]);
        try table.flush();
        try table.insert(rows[16384..]);
        try table.flush();
        for (0..4) |_| {
            var q = try helpers.runSql(allocator, db, "SELECT g,h,COUNT(*) AS c FROM t GROUP BY g,h ORDER BY c DESC,g,h LIMIT 600");
            defer q.deinit();
            var seen = [_][17]bool{[_]bool{false} ** 17} ** 31;
            var total: i64 = 0;
            while (try q.next()) |batch| {
                for (0..batch.row_count) |i| {
                    const g: usize = @intCast(batch.values[0].data.bigint[i] - 7_000_000_000);
                    const h: usize = @intCast(batch.values[1].data.bigint[i] + 8_000_000_000);
                    try std.testing.expect(!seen[g][h]);
                    seen[g][h] = true;
                    try std.testing.expectEqual(counts[g][h], batch.values[2].data.bigint[i]);
                    total += batch.values[2].data.bigint[i];
                }
            }
            try std.testing.expectEqual(@as(i64, @intCast(rows.len)), total);
        }
    }
}

test "encoded machinery topn preserves nullable strings ties offset tombstones and memtable rows" {
    const allocator = std.testing.allocator;
    inline for (.{ @as(usize, 1), @as(usize, 4) }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .row_group_size = 16 });
        defer db.close();
        try helpers.exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, x INT NOT NULL, phrase VARCHAR(30))");
        const table = try db.openTable("t", .{});
        var expected: std.ArrayList(usize) = .empty;
        defer expected.deinit(allocator);
        for (0..128) |i| {
            var buf: [160]u8 = undefined;
            var phrase_buf: [32]u8 = undefined;
            const phrase = if (i % 3 == 0) "NULL" else if (i % 5 == 0) "''" else try std.fmt.bufPrint(&phrase_buf, "'p{d}'", .{i});
            const sql = try std.fmt.bufPrint(&buf, "INSERT INTO t VALUES ({d},{d},{s})", .{ i, (i * 37) % 19, phrase });
            try helpers.exec(allocator, db, sql);
            if (i == 95) try table.flush();
            if (i % 3 != 0 and i % 5 != 0 and i != 19) try expected.append(allocator, i);
        }
        try helpers.exec(allocator, db, "DELETE FROM t WHERE id=19");
        const Order = struct {
            fn less(_: void, a: usize, b: usize) bool {
                const ax = (a * 37) % 19;
                const bx = (b * 37) % 19;
                return if (ax != bx) ax < bx else a > b;
            }
        };
        std.mem.sort(usize, expected.items, {}, Order.less);
        for (0..3) |_| {
            var q = try helpers.runSql(allocator, db, "SELECT id,phrase FROM t WHERE phrase<>'' ORDER BY x ASC,id DESC LIMIT 7 OFFSET 3");
            defer q.deinit();
            var pos: usize = 3;
            while (try q.next()) |batch| {
                for (0..batch.row_count) |i| {
                    try std.testing.expect(pos < 10);
                    const id = expected.items[pos];
                    try std.testing.expectEqual(@as(i64, @intCast(id)), batch.values[0].data.bigint[i]);
                    var buf: [30]u8 = undefined;
                    const phrase = try std.fmt.bufPrint(&buf, "p{d}", .{id});
                    try std.testing.expectEqualStrings(phrase, batch.values[1].data.varchar.rowBytes(i));
                    pos += 1;
                }
            }
            try std.testing.expectEqual(@as(usize, 10), pos);
        }
        try table.flush();
    }
}
