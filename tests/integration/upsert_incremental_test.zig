//! Incremental upsert key-index regression (src/api/upsert.zig). The index
//! persists across insert batches so resolution is O(new rows), not O(memtable).
//! These exercise the paths that optimization touches: cross-batch dedup,
//! within-batch dedup, dedup against flushed segments, reopen, and the swaps
//! that carry the index over.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSql = helpers.runSql;

fn firstInt(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !i64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    const b = (try q.next()) orelse return error.NoRows;
    return switch (b.values[0].data) {
        .int => |s| s[0],
        .bigint => |s| s[0],
        else => error.NotInt,
    };
}

test "upsert: cross-batch and within-batch last-writer-wins" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE u (id INT NOT NULL, v INT NOT NULL, PRIMARY KEY (id))");
    try exec(allocator, db, "INSERT INTO u (id, v) VALUES (1,10),(2,20),(3,30)");
    // Separate batches re-hitting a key must upsert, not duplicate (this is the
    // incremental path — each batch only sees its own new rows).
    try exec(allocator, db, "INSERT INTO u (id, v) VALUES (2,99)");
    try exec(allocator, db, "INSERT INTO u (id, v) VALUES (1,11)");
    try exec(allocator, db, "INSERT INTO u (id, v) VALUES (1,12)");
    // Within-batch dupe: last occurrence in the VALUES list wins.
    try exec(allocator, db, "INSERT INTO u (id, v) VALUES (5,50),(5,55),(6,60)");

    try std.testing.expectEqual(@as(i64, 5), try firstInt(allocator, db, "SELECT COUNT(*) FROM u"));
    try std.testing.expectEqual(@as(i64, 12), try firstInt(allocator, db, "SELECT v FROM u WHERE id = 1"));
    try std.testing.expectEqual(@as(i64, 99), try firstInt(allocator, db, "SELECT v FROM u WHERE id = 2"));
    try std.testing.expectEqual(@as(i64, 55), try firstInt(allocator, db, "SELECT v FROM u WHERE id = 5"));
}

test "upsert: dedup against flushed segments + reopen" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        try exec(allocator, db, "CREATE TABLE u (id INT NOT NULL, v INT NOT NULL, PRIMARY KEY (id))");
        try exec(allocator, db, "INSERT INTO u (id, v) VALUES (1,10),(2,20),(3,30),(4,40)");
        const t = try db.openTable("u", .{});
        try t.flush(); // rows now live in a segment

        // Re-inserting a flushed key must tombstone the segment row (upsert),
        // not duplicate. This is the new-keys-only segment probe.
        try exec(allocator, db, "INSERT INTO u (id, v) VALUES (2,222),(5,50)");
        try std.testing.expectEqual(@as(i64, 5), try firstInt(allocator, db, "SELECT COUNT(*) FROM u"));
        try std.testing.expectEqual(@as(i64, 222), try firstInt(allocator, db, "SELECT v FROM u WHERE id = 2"));
        try t.flush();
    }

    // Reopen: deduped state persists.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try std.testing.expectEqual(@as(i64, 5), try firstInt(allocator, db, "SELECT COUNT(*) FROM u"));
    try std.testing.expectEqual(@as(i64, 222), try firstInt(allocator, db, "SELECT v FROM u WHERE id = 2"));
    // A further upsert after reopen still dedups against the reloaded segment.
    try exec(allocator, db, "INSERT INTO u (id, v) VALUES (3,333)");
    try std.testing.expectEqual(@as(i64, 5), try firstInt(allocator, db, "SELECT COUNT(*) FROM u"));
    try std.testing.expectEqual(@as(i64, 333), try firstInt(allocator, db, "SELECT v FROM u WHERE id = 3"));
}

const Model = std.AutoArrayHashMapUnmanaged(i64, i64);

fn appendRow(allocator: std.mem.Allocator, sql: *std.ArrayList(u8), first: bool, id: i64, v: i64) !void {
    if (!first) try sql.append(allocator, ',');
    try sql.print(allocator, "({d},'k{d}',{d})", .{ id, @mod(id, 7), v });
}

fn appendIdList(allocator: std.mem.Allocator, sql: *std.ArrayList(u8), ids: []const i64) !void {
    try sql.append(allocator, '(');
    for (ids, 0..) |id, i| {
        if (i > 0) try sql.append(allocator, ',');
        try sql.print(allocator, "{d}", .{id});
    }
    try sql.append(allocator, ')');
}

/// Memtable ids of the model, picked at random without repeats.
fn pickIds(allocator: std.mem.Allocator, rand: std.Random, model: *const Model, first_memtable_id: i64, count: usize) ![]i64 {
    var ids: std.ArrayList(i64) = .empty;
    errdefer ids.deinit(allocator);
    const keys = model.keys();
    var tries: usize = 0;
    while (ids.items.len < count and tries < 10 * count) : (tries += 1) {
        const id = keys[rand.uintLessThan(usize, keys.len)];
        if (id < first_memtable_id or std.mem.indexOfScalar(i64, ids.items, id) != null) continue;
        try ids.append(allocator, id);
    }
    return ids.toOwnedSlice(allocator);
}

test "upsert: memtable DELETE, UPDATE, dedup and pinned-reader swaps carry the key index (#346)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .auto_flush_secs = 0 });
    defer db.close();

    try exec(allocator, db, "CREATE TABLE u (id BIGINT NOT NULL, tag VARCHAR(8) NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (id, tag))");
    const t = try db.openTable("u", .{});
    var model: Model = .empty;
    defer model.deinit(allocator);
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);

    // 200 flushed rows, then 800 memtable rows.
    const first_memtable_id: i64 = 200;
    inline for (.{ .{ 0, 200 }, .{ 200, 1000 } }) |range| {
        sql.clearRetainingCapacity();
        try sql.appendSlice(allocator, "INSERT INTO u (id, tag, v) VALUES ");
        var id: i64 = range[0];
        while (id < range[1]) : (id += 1) {
            try appendRow(allocator, &sql, id == range[0], id, id);
            try model.put(allocator, id, id);
        }
        try exec(allocator, db, sql.items);
        if (range[0] == 0) try t.flush();
    }

    var prng = std.Random.DefaultPrng.init(346);
    const rand = prng.random();
    var next_id: i64 = 1000;
    var round: i64 = 0;
    while (round < 36) : (round += 1) {
        sql.clearRetainingCapacity();
        const gen_before = t.memtable_gen;
        var pinned: ?*thindb.engine.Memtable = null;
        switch (@mod(round, 6)) {
            // Upsert: memtable keys (a dedup swap), flushed keys and new keys.
            0, 5 => {
                if (@mod(round, 6) == 5) {
                    pinned = t.memtable;
                    t.memtable.acquire();
                }
                const hits = try pickIds(allocator, rand, &model, first_memtable_id, 20);
                defer allocator.free(hits);
                try sql.appendSlice(allocator, "INSERT INTO u (id, tag, v) VALUES ");
                for (hits, 0..) |id, i| {
                    try appendRow(allocator, &sql, i == 0, id, round * 10_000 + id);
                    try model.put(allocator, id, round * 10_000 + id);
                }
                const flushed_id = rand.uintLessThan(u32, 200);
                try appendRow(allocator, &sql, false, flushed_id, -round);
                try model.put(allocator, flushed_id, -round);
                for (0..10) |_| {
                    try appendRow(allocator, &sql, false, next_id, next_id);
                    try model.put(allocator, next_id, next_id);
                    next_id += 1;
                }
            },
            // DELETE of one memtable row, then of many.
            1 => {
                const ids = try pickIds(allocator, rand, &model, first_memtable_id, 1);
                defer allocator.free(ids);
                try sql.print(allocator, "DELETE FROM u WHERE id = {d}", .{ids[0]});
                _ = model.orderedRemove(ids[0]);
            },
            2 => {
                const ids = try pickIds(allocator, rand, &model, first_memtable_id, 12);
                defer allocator.free(ids);
                try sql.appendSlice(allocator, "DELETE FROM u WHERE id IN ");
                try appendIdList(allocator, &sql, ids);
                for (ids) |id| _ = model.orderedRemove(id);
            },
            3 => {
                const ids = try pickIds(allocator, rand, &model, first_memtable_id, 9);
                defer allocator.free(ids);
                try sql.appendSlice(allocator, "UPDATE u SET v = v + 1 WHERE id IN ");
                try appendIdList(allocator, &sql, ids);
                for (ids) |id| model.getPtr(id).?.* += 1;
            },
            // ON DUPLICATE KEY UPDATE that merges: memtable hits and a new key.
            4 => {
                const hits = try pickIds(allocator, rand, &model, first_memtable_id, 5);
                defer allocator.free(hits);
                try sql.appendSlice(allocator, "INSERT INTO u (id, tag, v) VALUES ");
                for (hits, 0..) |id, i| {
                    try appendRow(allocator, &sql, i == 0, id, 3);
                    model.getPtr(id).?.* += 3;
                }
                try appendRow(allocator, &sql, false, next_id, 3);
                try model.put(allocator, next_id, 3);
                next_id += 1;
                try sql.appendSlice(allocator, " ON DUPLICATE KEY UPDATE v = v + VALUES(v)");
            },
            else => unreachable,
        }
        try exec(allocator, db, sql.items);
        if (pinned) |mt| mt.release();
        // The statement swapped the memtable, and the index went along
        // instead of being rebuilt. Test builds also compare every carried
        // index with a rebuild of it (`upsert.verifyIndex`).
        try std.testing.expect(t.memtable_gen > gen_before);
        try std.testing.expectEqual(@as(?u64, t.memtable_gen), t.upsert_idx_gen);
        try std.testing.expectEqual(t.memtable.row_count, t.upsert_idx_rows);
    }

    const ids = try helpers.collectBigints(allocator, db, "SELECT id FROM u ORDER BY id");
    defer allocator.free(ids);
    const values = try helpers.collectBigints(allocator, db, "SELECT v FROM u ORDER BY id");
    defer allocator.free(values);
    const Sort = struct {
        keys: []const i64,
        pub fn lessThan(ctx: @This(), a: usize, b: usize) bool {
            return ctx.keys[a] < ctx.keys[b];
        }
    };
    model.sort(Sort{ .keys = model.keys() });
    try std.testing.expectEqualSlices(i64, model.keys(), ids);
    try std.testing.expectEqualSlices(i64, model.values(), values);
}
