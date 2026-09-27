//! LIKE / NOT LIKE pattern matching. `%` matches zero-or-more, `_`
//! matches one, and a backslash (or the ESCAPE character) makes the next
//! character literal. NULL never matches (two-valued logic).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(32) NOT NULL)");
    try exec(
        allocator,
        db,
        "INSERT INTO t (id, name) VALUES (1, 'alpha'), (2, 'alphabet'), (3, 'beta'), (4, 'gamma'), (5, 'al')",
    );
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

test "LIKE: % suffix wildcard" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE name LIKE 'alpha%' ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, ids);
}

test "LIKE: % prefix and middle wildcards" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE name LIKE '%a%a%' ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 4 }, ids);
}

test "LIKE: _ single-char wildcard" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE name LIKE 'al___' ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{1}, ids);
}

test "LIKE: NOT LIKE inverts the match" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE name NOT LIKE 'al%' ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 3, 4 }, ids);
}

test "LIKE: literal pattern with no wildcards = exact match" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE name LIKE 'beta' ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{3}, ids);
}

test "LIKE: matcher unit tests" {
    try std.testing.expect(thindb.exec.predicate.likeMatch("foo", "f%"));
    try std.testing.expect(thindb.exec.predicate.likeMatch("foo", "%o"));
    try std.testing.expect(thindb.exec.predicate.likeMatch("foo", "%"));
    try std.testing.expect(thindb.exec.predicate.likeMatch("foo", "f_o"));
    try std.testing.expect(!thindb.exec.predicate.likeMatch("foo", "f_oo"));
    try std.testing.expect(!thindb.exec.predicate.likeMatch("", "_"));
    try std.testing.expect(thindb.exec.predicate.likeMatch("", "%"));
    try std.testing.expect(thindb.exec.predicate.likeMatch("ababc", "a%c"));
    try std.testing.expect(!thindb.exec.predicate.likeMatch("ababc", "a%d"));
}

test "REGEXP and RLIKE match anywhere in the string" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT id FROM t WHERE name REGEXP '^al' ORDER BY id", &[_]i64{ 1, 2, 5 } },
        .{ "SELECT id FROM t WHERE name RLIKE 'ta$' ORDER BY id", &[_]i64{3} },
        .{ "SELECT id FROM t WHERE name regexp 'mm' ORDER BY id", &[_]i64{4} },
        .{ "SELECT id FROM t WHERE name NOT REGEXP 'a$' ORDER BY id", &[_]i64{ 2, 5 } },
        .{ "SELECT id FROM t WHERE NOT name RLIKE '^al' ORDER BY id", &[_]i64{ 3, 4 } },
        .{ "SELECT id FROM t WHERE UPPER(name) REGEXP '^AL.+T$' ORDER BY id", &[_]i64{2} },
        .{ "SELECT id FROM t WHERE name REGEXP CONCAT('^', 'g') OR id = 5 ORDER BY id", &[_]i64{ 4, 5 } },
        .{ "SELECT id FROM t WHERE id > 1 AND name REGEXP 'b' ORDER BY id", &[_]i64{ 2, 3 } },
    };
    inline for (cases) |case| {
        const ids = try collectBigints(allocator, db, case[0]);
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(i64, case[1], ids);
    }
}

test "LIKE: backslash escapes by default, ESCAPE names another character" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    // Neutral-dialect strings keep a backslash as written: row 4 is a\b.
    try exec(allocator, db, "CREATE TABLE lk (id BIGINT PRIMARY KEY, s VARCHAR(16))");
    try exec(allocator, db, "INSERT INTO lk VALUES (1, 'a%b'), (2, 'axb'), (3, 'a_b'), (4, 'a\\b')");

    const cases = .{
        .{ "s LIKE 'a|%b' ESCAPE '|'", &[_]i64{1} },
        .{ "s LIKE 'a\\%b'", &[_]i64{1} },
        .{ "s LIKE 'a\\_b'", &[_]i64{3} },
        .{ "s LIKE 'a_b'", &[_]i64{ 1, 2, 3, 4 } },
        .{ "s LIKE 'a\\\\b'", &[_]i64{4} },
        .{ "s LIKE 'a\\b' ESCAPE ''", &[_]i64{4} },
        .{ "s LIKE 'a\\b' ESCAPE '|'", &[_]i64{4} },
        .{ "s NOT LIKE 'a!_b' ESCAPE '!'", &[_]i64{ 1, 2, 4 } },
        .{ "s LIKE '%\\%%'", &[_]i64{1} },
        .{ "'a%b' LIKE 'a|%b' ESCAPE '|' AND id = 2", &[_]i64{2} },
    };
    inline for (cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        const ids = try collectBigints(allocator, db, "SELECT id FROM lk WHERE " ++ c[0] ++ " ORDER BY id");
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(i64, c[1], ids);
    }

    // MySQL string literals keep \% and \_ for LIKE, and \\ is one
    // backslash, so 'a\\\\b' is the pattern a\\b.
    const mysql_cases = .{
        .{ "SELECT id FROM lk WHERE s LIKE 'a\\%b'", &[_]i64{1} },
        .{ "SELECT id FROM lk WHERE s LIKE 'a\\_b'", &[_]i64{3} },
        .{ "SELECT id FROM lk WHERE s LIKE 'a\\\\\\\\b'", &[_]i64{4} },
    };
    inline for (mysql_cases) |c| {
        errdefer std.debug.print("case failed: {s}\n", .{c[0]});
        var q = try helpers.runSqlMysql(allocator, db, c[0]);
        defer q.deinit();
        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint[0..batch.row_count]);
        try std.testing.expectEqualSlices(i64, c[1], ids.items);
    }
}
