//! LIKE / NOT LIKE pattern matching. `%` matches zero-or-more, `_`
//! matches one. NULL never matches (two-valued logic).

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(32) NOT NULL)");
    try exec(allocator, db,
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
