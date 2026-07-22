//! UNION ALL — concatenate two SELECT pipelines with matching schemas.
//! v1 only supports UNION ALL (no dedup). Plain UNION errors at parse
//! time until a hash-dedup pass lands.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE a (id BIGINT PRIMARY KEY)");
    try exec(allocator, db, "CREATE TABLE b (id BIGINT PRIMARY KEY)");
    try exec(allocator, db, "INSERT INTO a (id) VALUES (1), (2), (3)");
    try exec(allocator, db, "INSERT INTO b (id) VALUES (2), (4)");
    const ta = try db.openTable("a", .{});
    try ta.flush();
    const tb = try db.openTable("b", .{});
    try tb.flush();
    return db;
}

test "UNION ALL: concatenates rows (duplicates preserved)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM a UNION ALL SELECT id FROM b");
    defer allocator.free(ids);
    // a → [1, 2, 3], b → [2, 4]. Union all = both, in order.
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 2, 4 }, ids);
}

test "UNION ALL: WHERE filters applied per side" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM a WHERE id > 1 UNION ALL SELECT id FROM b WHERE id < 4",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3, 2 }, ids);
}

test "UNION ALL: chained 3-way" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM a WHERE id = 1 UNION ALL SELECT id FROM a WHERE id = 2 UNION ALL SELECT id FROM b WHERE id = 4",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 4 }, ids);
}

test "UNION ALL: schema width mismatch rejected" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE one (id BIGINT PRIMARY KEY)");
    try exec(allocator, db, "CREATE TABLE two (id BIGINT PRIMARY KEY, x INT NOT NULL)");

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(
        arena.allocator(),
        "SELECT * FROM one UNION ALL SELECT * FROM two",
    );
    const cq = thindb.net.compile(allocator, db, root);
    if (cq) |ok| {
        var c = ok;
        c.deinit();
        return error.TestUnexpectedSuccess;
    } else |err| {
        try std.testing.expectEqual(thindb.net.Error.TypeMismatch, err);
    }
}

test "UNION (distinct) rejected — only UNION ALL ships in v1" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const err = thindb.sql.parse(arena.allocator(), "SELECT id FROM a UNION SELECT id FROM b");
    try std.testing.expectError(thindb.sql.ParseError.SqlInvalidProjection, err);
}
