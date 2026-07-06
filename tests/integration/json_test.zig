//! JSON type end-to-end: a JSON column stores JSONB, survives a flush to a
//! segment, and the JSON_* functions / `->` `->>` operators navigate it after
//! the disk round-trip.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE j (id BIGINT PRIMARY KEY, doc JSON NOT NULL)");
    try exec(allocator, db,
        \\INSERT INTO j (id, doc) VALUES
        \\ (1, '{"name":"alice","age":30,"tags":["a","b"],"addr":{"zip":"10001"}}'),
        \\ (2, '{"name":"bob","age":25,"tags":[]}')
    );
    // Flush to a segment so the read path exercises JSONB-on-disk.
    const t = try db.openTable("j", .{});
    try t.flush();
    return db;
}

/// First row's first column as an owned string copy (works for TEXT/JSON
/// scalar results, which arrive as a string view).
fn firstString(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]u8 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    const batch = (try q.next()) orelse return error.NoRows;
    const sv = switch (batch.values[0].data) {
        .string, .varchar, .char, .json => |s| s,
        else => return error.NotString,
    };
    return allocator.dupe(u8, sv.rowBytes(0));
}

fn firstInt(db: anytype, sql: []const u8, allocator: std.mem.Allocator) !i64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    const batch = (try q.next()) orelse return error.NoRows;
    return switch (batch.values[0].data) {
        .int => |s| s[0],
        .bigint => |s| s[0],
        .boolean => |s| @intFromBool(s[0] != 0),
        else => error.NotInt,
    };
}

test "JSON: extraction survives flush to segment" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // ->> unquotes to text.
    {
        const s = try firstString(allocator, db, "SELECT doc->>'$.name' FROM j WHERE id = 1");
        defer allocator.free(s);
        try std.testing.expectEqualStrings("alice", s);
    }
    // nested path
    {
        const s = try firstString(allocator, db, "SELECT doc->>'$.addr.zip' FROM j WHERE id = 1");
        defer allocator.free(s);
        try std.testing.expectEqualStrings("10001", s);
    }
    // JSON_VALUE array element
    {
        const s = try firstString(allocator, db, "SELECT JSON_VALUE(doc, '$.tags[1]') FROM j WHERE id = 1");
        defer allocator.free(s);
        try std.testing.expectEqualStrings("b", s);
    }
    // JSON_TYPE reads the stored JSONB tag
    {
        const s = try firstString(allocator, db, "SELECT JSON_TYPE(doc) FROM j WHERE id = 1");
        defer allocator.free(s);
        try std.testing.expectEqualStrings("OBJECT", s);
    }
    // -> keeps JSON quoting; JSON_TYPE of the extracted value is STRING
    {
        const s = try firstString(allocator, db, "SELECT JSON_TYPE(doc->'$.name') FROM j WHERE id = 1");
        defer allocator.free(s);
        try std.testing.expectEqualStrings("STRING", s);
    }
}

test "JSON: length, contains, keys after flush" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try std.testing.expectEqual(@as(i64, 2), try firstInt(db, "SELECT JSON_LENGTH(JSON_EXTRACT(doc, '$.tags')) FROM j WHERE id = 1", allocator));
    try std.testing.expectEqual(@as(i64, 0), try firstInt(db, "SELECT JSON_LENGTH(JSON_EXTRACT(doc, '$.tags')) FROM j WHERE id = 2", allocator));
    try std.testing.expectEqual(@as(i64, 1), try firstInt(db, "SELECT JSON_CONTAINS(doc, '{\"name\":\"alice\"}') FROM j WHERE id = 1", allocator));
    try std.testing.expectEqual(@as(i64, 0), try firstInt(db, "SELECT JSON_CONTAINS(doc, '{\"name\":\"zzz\"}') FROM j WHERE id = 1", allocator));

    // JSON_KEYS returns a (JSONB) array; JSONB canonicalizes objects so keys
    // come back sorted. Verify via navigation over the produced array.
    try std.testing.expectEqual(@as(i64, 3), try firstInt(db, "SELECT JSON_LENGTH(JSON_KEYS(doc)) FROM j WHERE id = 2", allocator));
    {
        const k0 = try firstString(allocator, db, "SELECT JSON_VALUE(JSON_KEYS(doc), '$[0]') FROM j WHERE id = 2");
        defer allocator.free(k0);
        try std.testing.expectEqualStrings("age", k0);
    }
}
