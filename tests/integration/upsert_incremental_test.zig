//! Incremental upsert key-index regression (src/api/upsert.zig). The index
//! persists across insert batches so resolution is O(new rows), not O(memtable).
//! These exercise the paths that optimization touches: cross-batch dedup,
//! within-batch dedup, dedup against flushed segments, and reopen.

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
