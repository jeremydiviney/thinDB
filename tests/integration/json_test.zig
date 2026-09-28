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

test "JSON: MIN and MAX over a JSON column agree with ORDER BY, on every aggregate path" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT, j JSON)");
    try exec(allocator, db,
        \\INSERT INTO t VALUES (1, '{"a": 1}'), (2, '[1, 2]'), (1, '3'), (2, '"x"'), (3, 'true'), (3, NULL)
    );

    const t = try db.openTable("t", .{});
    inline for (.{ false, true }) |flushed| {
        if (flushed) try t.flush();
        // Every MIN/MAX path agrees with ORDER BY; the next test pins that
        // order to MySQL's JSON rules.
        const cases = .{
            .{ "SELECT CAST(MAX(j) AS CHAR) FROM t", "SELECT CAST(j AS CHAR) FROM t WHERE j IS NOT NULL ORDER BY j DESC LIMIT 1" },
            .{ "SELECT CAST(MIN(j) AS CHAR) FROM t", "SELECT CAST(j AS CHAR) FROM t WHERE j IS NOT NULL ORDER BY j LIMIT 1" },
            .{ "SELECT CAST(MAX(x) AS CHAR) FROM (SELECT j x FROM t) t2", "SELECT CAST(j AS CHAR) FROM t WHERE j IS NOT NULL ORDER BY j DESC LIMIT 1" },
            .{ "SELECT CAST(MAX(j) AS CHAR) FROM t GROUP BY id ORDER BY id", "SELECT CAST(MAX(j) AS CHAR) FROM (SELECT id, j FROM t ORDER BY id, j DESC) s GROUP BY id ORDER BY id" },
            .{ "SELECT CAST(m AS CHAR) FROM (SELECT id, MIN(j) OVER (PARTITION BY id) m FROM t) w ORDER BY id, m", "SELECT CAST(m AS CHAR) FROM (SELECT id, MIN(j) m FROM t GROUP BY id) g JOIN t USING (id) ORDER BY id, m" },
        };
        inline for (cases) |c| {
            const got = try helpers.collectStrings(allocator, db, c[0]);
            defer helpers.freeStrings(allocator, got);
            const want = try helpers.collectStrings(allocator, db, c[1]);
            defer helpers.freeStrings(allocator, want);
            try std.testing.expectEqual(want.len, got.len);
            for (want, got) |w, g| try std.testing.expectEqualDeep(w, g);
        }
    }
}

/// Rows of the first column joined by `|`, NULL as `NULL`.
fn joinedStrings(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]u8 {
    const values = try helpers.collectStrings(allocator, db, sql);
    defer helpers.freeStrings(allocator, values);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (values, 0..) |v, i| {
        if (i > 0) try out.append(allocator, '|');
        try out.appendSlice(allocator, v orelse "NULL");
    }
    return out.toOwnedSlice(allocator);
}

test "JSON: comparison, ORDER BY, MIN and MAX follow MySQL's JSON rules, over the memtable and flushed segments" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT, g BIGINT, j JSON)");
    // Numbers and nulls land in their own segment, so a segment whose byte
    // range misses a literal still holds rows the literal matches.
    try exec(allocator, db,
        \\INSERT INTO t VALUES (3, 0, '3'), (6, 0, '10'), (7, 1, '3.0'), (8, 2, 'null'), (9, 0, NULL),
        \\ (15, 0, '2.5'), (16, 1, '-1e3'), (20, 2, '12345678901234567890')
    );
    const t = try db.openTable("t", .{});
    try t.flush();
    try exec(allocator, db,
        \\INSERT INTO t VALUES (1, 1, '{"a": 1}'), (2, 2, '[1, 2]'), (4, 1, '"x"'), (5, 2, 'true'),
        \\ (10, 1, 'false'), (11, 2, '{"b": 0, "c": 1}'), (12, 0, '[1, 2, 3]'), (13, 1, '"5"'),
        \\ (14, 2, '"X"'), (17, 2, '[1, 2, [3]]'), (18, 0, '"é"'), (19, 1, '[]')
    );

    // Expected results are MySQL 8.4's. MySQL sorts arrays and objects by
    // their element count alone (a non-scalar sort key is unsupported there),
    // so the ORDER BY cases keep every same-count pair in comparator order.
    const id_cases = .{
        .{ "SELECT id FROM t ORDER BY j, id", &[_]i64{ 9, 8, 16, 15, 3, 7, 6, 20, 13, 14, 4, 18, 1, 11, 19, 2, 12, 17, 10, 5 } },
        .{ "SELECT id FROM t ORDER BY j DESC, id DESC", &[_]i64{ 5, 10, 17, 12, 2, 19, 11, 1, 18, 4, 14, 13, 20, 6, 7, 3, 15, 16, 8, 9 } },
        .{ "SELECT id FROM t WHERE id NOT IN (7, 17) ORDER BY j", &[_]i64{ 9, 8, 16, 15, 3, 6, 20, 13, 14, 4, 18, 1, 11, 19, 2, 12, 10, 5 } },
        .{ "SELECT id FROM t WHERE id NOT IN (7, 17) ORDER BY j DESC LIMIT 6", &[_]i64{ 5, 10, 12, 2, 19, 11 } },
        .{ "SELECT id FROM t WHERE id NOT IN (7, 17) ORDER BY j LIMIT 6", &[_]i64{ 9, 8, 16, 15, 3, 6 } },
        .{ "SELECT id FROM t WHERE j > '5' ORDER BY id", &[_]i64{ 1, 2, 4, 5, 10, 11, 12, 14, 17, 18, 19 } },
        .{ "SELECT id FROM t WHERE j = 'x' ORDER BY id", &[_]i64{4} },
        .{ "SELECT id FROM t WHERE j = '[1, 2]' ORDER BY id", &[_]i64{} },
        .{ "SELECT id FROM t WHERE j = CAST('[1, 2]' AS JSON) ORDER BY id", &[_]i64{2} },
        .{ "SELECT id FROM t WHERE j > CAST('[1, 2]' AS JSON) ORDER BY id", &[_]i64{ 5, 10, 12, 17 } },
        .{ "SELECT id FROM t WHERE j = 3 ORDER BY id", &[_]i64{ 3, 7 } },
        .{ "SELECT id FROM t WHERE j < 3 ORDER BY id", &[_]i64{ 8, 15, 16 } },
        .{ "SELECT id FROM t WHERE j <= -1000 ORDER BY id", &[_]i64{ 8, 16 } },
        .{ "SELECT id FROM t WHERE j >= 2.5 ORDER BY id", &[_]i64{ 1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20 } },
        .{ "SELECT id FROM t WHERE j <> 3 ORDER BY id", &[_]i64{ 1, 2, 4, 5, 6, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 } },
        .{ "SELECT id FROM t WHERE j IN (3, 'x', 10) ORDER BY id", &[_]i64{ 3, 4, 6, 7 } },
        .{ "SELECT id FROM t WHERE j NOT IN (3, 'x') ORDER BY id", &[_]i64{ 1, 2, 5, 6, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 } },
        .{ "SELECT id FROM t WHERE j >= 2 AND j <= 5 ORDER BY id", &[_]i64{ 3, 7, 15 } },
        .{ "SELECT id FROM t WHERE j = TRUE ORDER BY id", &[_]i64{5} },
        .{ "SELECT id FROM t WHERE j < '\"' ORDER BY id", &[_]i64{ 3, 6, 7, 8, 15, 16, 20 } },
        .{ "SELECT id FROM t WHERE j > 12345678901234567889 ORDER BY id", &[_]i64{ 1, 2, 4, 5, 10, 11, 12, 13, 14, 17, 18, 19, 20 } },
        .{ "SELECT id FROM t WHERE j = 3.0 ORDER BY id", &[_]i64{ 3, 7 } },
        .{ "SELECT id FROM t WHERE j <> 'x' AND j < '5' ORDER BY id", &[_]i64{ 3, 6, 7, 8, 15, 16, 20 } },
        .{ "SELECT id FROM t WHERE j IN (3, 'x', 10, 11, 12, 13, 14, 15, 16, 17) ORDER BY id", &[_]i64{ 3, 4, 6, 7 } },
        .{ "SELECT id FROM t WHERE j > g ORDER BY id", &[_]i64{ 1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20 } },
        .{ "SELECT id FROM t WHERE j <= g ORDER BY id", &[_]i64{ 8, 16 } },
        .{ "SELECT a.id FROM t a JOIN t b ON a.j < b.j WHERE b.id = 13 ORDER BY a.id", &[_]i64{ 3, 6, 7, 8, 15, 16, 20 } },
        .{ "SELECT id FROM t WHERE j IN (SELECT j FROM t WHERE id IN (7, 13)) ORDER BY id", &[_]i64{ 3, 7, 13 } },
        .{ "SELECT id FROM t WHERE j > (SELECT j FROM t WHERE id = 13) ORDER BY id", &[_]i64{ 1, 2, 4, 5, 10, 11, 12, 14, 17, 18, 19 } },
    };
    const text_cases = .{
        .{ "SELECT CAST(MIN(j) AS CHAR) FROM t", "null" },
        .{ "SELECT CAST(MAX(j) AS CHAR) FROM t", "true" },
        .{ "SELECT CAST(MIN(j) AS CHAR) FROM t GROUP BY g ORDER BY g", "2.5|-1000.0|null" },
        .{ "SELECT CAST(MAX(j) AS CHAR) FROM t GROUP BY g ORDER BY g", "[1, 2, 3]|false|true" },
        .{ "SELECT CAST(MIN(j) AS CHAR) FROM t WHERE JSON_TYPE(j) IN ('INTEGER', 'DOUBLE', 'UNSIGNED INTEGER')", "-1000.0" },
        .{ "SELECT CAST(MAX(j) AS CHAR) FROM t WHERE JSON_TYPE(j) IN ('INTEGER', 'DOUBLE', 'UNSIGNED INTEGER')", "12345678901234567890" },
        .{ "SELECT CAST(MAX(j) AS CHAR) FROM t WHERE JSON_TYPE(j) = 'STRING'", "\"é\"" },
        .{ "SELECT CAST(j AS CHAR) FROM t WHERE JSON_TYPE(j) = 'ARRAY' ORDER BY j", "[]|[1, 2]|[1, 2, 3]|[1, 2, [3]]" },
        .{ "SELECT CAST(MAX(j) OVER (PARTITION BY g) AS CHAR) FROM t WHERE id <= 4 ORDER BY id", "{\"a\": 1}|[1, 2]|3|{\"a\": 1}" },
        .{ "SELECT CAST(MIN(j) OVER (PARTITION BY g) AS CHAR) FROM t WHERE id >= 15 ORDER BY id", "2.5|-1000.0|12345678901234567890|2.5|-1000.0|12345678901234567890" },
    };

    var failures: usize = 0;
    inline for (.{ false, true }) |flushed| {
        if (flushed) try t.flush();
        inline for (id_cases) |c| {
            const got = try helpers.collectBigints(allocator, db, c[0]);
            defer allocator.free(got);
            if (!std.mem.eql(i64, c[1], got)) {
                failures += 1;
                std.debug.print("flushed={} {s}\n  want {any}\n  got  {any}\n", .{ flushed, c[0], c[1], got });
            }
        }
        inline for (text_cases) |c| {
            const got = try joinedStrings(allocator, db, c[0]);
            defer allocator.free(got);
            if (!std.mem.eql(u8, c[1], got)) {
                failures += 1;
                std.debug.print("flushed={} {s}\n  want {s}\n  got  {s}\n", .{ flushed, c[0], c[1], got });
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 0), failures);
}
