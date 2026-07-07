//! Non-unique (sort-key-only) tables: `CREATE TABLE t (...) ORDER BY (cols)`.
//! The order key clusters/sorts but is NOT a uniqueness constraint — inserts
//! append and duplicate key values are kept (vs `PRIMARY KEY`, which upserts
//! last-writer-wins). The `unique = false` flag must persist across a reopen.

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

test "ORDER BY table appends duplicates; PRIMARY KEY table upserts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // Non-unique: same (ts,host) inserted twice → both kept.
    try exec(allocator, db, "CREATE TABLE evt (ts INT NOT NULL, host STRING NOT NULL, val INT NOT NULL) ORDER BY (ts, host)");
    try exec(allocator, db, "INSERT INTO evt (ts, host, val) VALUES (1,'a',10),(1,'a',20),(2,'b',30)");
    try std.testing.expectEqual(@as(i64, 3), try firstInt(allocator, db, "SELECT COUNT(*) FROM evt"));

    // Unique: same id twice → upsert, last wins.
    try exec(allocator, db, "CREATE TABLE dim (id INT NOT NULL, val INT NOT NULL, PRIMARY KEY (id))");
    try exec(allocator, db, "INSERT INTO dim (id, val) VALUES (1,10),(1,20),(2,30)");
    try std.testing.expectEqual(@as(i64, 2), try firstInt(allocator, db, "SELECT COUNT(*) FROM dim"));
    try std.testing.expectEqual(@as(i64, 20), try firstInt(allocator, db, "SELECT val FROM dim WHERE id = 1"));
}

test "non-unique table stays non-unique across a reopen" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        try exec(allocator, db, "CREATE TABLE evt (ts INT NOT NULL, host STRING NOT NULL, val INT NOT NULL) ORDER BY (ts, host)");
        try exec(allocator, db, "INSERT INTO evt (ts, host, val) VALUES (1,'a',10),(1,'a',20)");
        const t = try db.openTable("evt", .{});
        try t.flush();
    }

    // Reopen: the persisted schema must reload with unique = false.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    // Both flushed duplicates survived.
    try std.testing.expectEqual(@as(i64, 2), try firstInt(allocator, db, "SELECT COUNT(*) FROM evt"));
    // A further duplicate still appends (proves the reloaded table is non-unique,
    // not silently upserting).
    try exec(allocator, db, "INSERT INTO evt (ts, host, val) VALUES (1,'a',30)");
    try std.testing.expectEqual(@as(i64, 3), try firstInt(allocator, db, "SELECT COUNT(*) FROM evt"));
}

test "CREATE TABLE requires a key clause; PRIMARY KEY and ORDER BY are exclusive" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try std.testing.expectError(error.SqlInvalidProjection, exec(allocator, db, "CREATE TABLE nokey (a INT, b INT)"));
    try std.testing.expectError(error.SqlInvalidProjection, exec(allocator, db, "CREATE TABLE both (a INT, b INT, PRIMARY KEY (a)) ORDER BY (b)"));
}
