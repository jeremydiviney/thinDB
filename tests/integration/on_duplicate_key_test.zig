//! `INSERT ... ON DUPLICATE KEY UPDATE col = VALUES(col), ...` — the MySQL
//! upsert clause Flink's JDBC sink emits. thinDB's INSERT already upserts on a
//! unique table (last-writer-wins) and appends on a non-unique one, which is
//! exactly the clause's full-row-replace semantics, so the parser validates
//! that shape and drops it. Non-`VALUES` / accumulation update lists are
//! rejected rather than silently mis-applied.

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

test "ON DUPLICATE KEY UPDATE upserts on a unique table" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE dim (id INT NOT NULL, val INT NOT NULL, PRIMARY KEY (id))");
    try exec(allocator, db, "INSERT INTO dim (id, val) VALUES (1,10),(2,20)");
    // Conflict on id=1 → replace; brand-new id=3 → insert.
    try exec(allocator, db, "INSERT INTO dim (id, val) VALUES (1,99) ON DUPLICATE KEY UPDATE val = VALUES(val)");
    try exec(allocator, db, "INSERT INTO dim (id, val) VALUES (3,30) ON DUPLICATE KEY UPDATE val = VALUES(val)");
    try std.testing.expectEqual(@as(i64, 3), try firstInt(allocator, db, "SELECT COUNT(*) FROM dim"));
    try std.testing.expectEqual(@as(i64, 99), try firstInt(allocator, db, "SELECT val FROM dim WHERE id = 1"));
    try std.testing.expectEqual(@as(i64, 20), try firstInt(allocator, db, "SELECT val FROM dim WHERE id = 2"));

    // Accumulation / non-VALUES update lists are unsupported (can't full-replace).
    try std.testing.expectError(error.SqlInvalidProjection, exec(allocator, db, "INSERT INTO dim (id, val) VALUES (1,1) ON DUPLICATE KEY UPDATE val = val + VALUES(val)"));
    try std.testing.expectError(error.SqlInvalidProjection, exec(allocator, db, "INSERT INTO dim (id, val) VALUES (1,1) ON DUPLICATE KEY UPDATE val = 5"));
}

test "ON DUPLICATE KEY UPDATE on a non-unique table appends" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE evt (ts INT NOT NULL, val INT NOT NULL) ORDER BY (ts)");
    try exec(allocator, db, "INSERT INTO evt (ts, val) VALUES (1,10)");
    // No unique key → the clause never fires; the row appends (dup ts kept).
    try exec(allocator, db, "INSERT INTO evt (ts, val) VALUES (1,20) ON DUPLICATE KEY UPDATE val = VALUES(val)");
    try std.testing.expectEqual(@as(i64, 2), try firstInt(allocator, db, "SELECT COUNT(*) FROM evt"));
}
