//! CREATE TABLE name AS SELECT ... (CTAS) and INSERT INTO target [(cols)]
//! SELECT ... — schema-inferred table creation and query-driven inserts.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

test "CTAS: basic — copies source rows into a new table" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO src (id, qty) VALUES (1, 10), (2, 20), (3, 30)");
    const src = try db.openTable("src", .{});
    try src.flush();

    try exec(allocator, db, "CREATE TABLE dst AS SELECT id, qty FROM src WHERE qty >= 20");
    const dst = try db.openTable("dst", .{});
    try dst.flush();

    const ids = try collectBigints(allocator, db, "SELECT id FROM dst ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, ids);
}

test "CTAS and INSERT SELECT preserve aliases and NULL projection schemas" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO src (id, qty) VALUES (1, 10), (2, 20)");
    const src = try db.openTable("src", .{});
    try src.flush();

    try exec(allocator, db, "CREATE TABLE dst AS SELECT id, qty AS amount, NULL AS note FROM src");
    const dst = try db.openTable("dst", .{});
    try std.testing.expectEqualStrings("amount", dst.schema.columns[1].name);
    try std.testing.expectEqual(thindb.Type{ .int = {} }, dst.schema.columns[1].type);
    try std.testing.expectEqualStrings("note", dst.schema.columns[2].name);
    try std.testing.expectEqual(thindb.Type{ .string = {} }, dst.schema.columns[2].type);
    try std.testing.expect(dst.schema.columns[2].nullable);

    try exec(allocator, db, "CREATE TABLE sink (id BIGINT PRIMARY KEY, amount INT NOT NULL, note STRING NULL)");
    try exec(allocator, db, "INSERT INTO sink SELECT id, amount, note FROM dst");
    const sink = try db.openTable("sink", .{});
    try sink.flush();

    const ids = try collectBigints(allocator, db, "SELECT id FROM sink ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, ids);
}

test "CTAS: computed projection can replace a star-expanded source column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY, qty INT NOT NULL, tag STRING NOT NULL)");
    try exec(allocator, db, "INSERT INTO src (id, qty, tag) VALUES (1, 10, 'a'), (2, 20, 'b')");
    const src = try db.openTable("src", .{});
    try src.flush();

    try exec(allocator, db, "CREATE TABLE dst AS SELECT *, qty + 1 AS qty FROM src");
    const dst = try db.openTable("dst", .{});
    try std.testing.expectEqual(@as(usize, 3), dst.schema.columns.len);
    try std.testing.expectEqualStrings("id", dst.schema.columns[0].name);
    try std.testing.expectEqualStrings("qty", dst.schema.columns[1].name);
    try std.testing.expectEqualStrings("tag", dst.schema.columns[2].name);
    try dst.flush();

    var q = try helpers.runSql(allocator, db, "SELECT id, qty, tag FROM dst ORDER BY id ASC");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 1), b.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i32, 11), b.values[1].data.int[0]);
    try std.testing.expectEqualStrings("a", b.values[2].data.string.rowBytes(0));
    try std.testing.expectEqual(@as(i64, 2), b.values[0].data.bigint[1]);
    try std.testing.expectEqual(@as(i32, 21), b.values[1].data.int[1]);
    try std.testing.expectEqualStrings("b", b.values[2].data.string.rowBytes(1));
}

test "CTAS: rejects when target exists (unless IF NOT EXISTS)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY)");
    try exec(allocator, db, "INSERT INTO src (id) VALUES (1)");
    const src = try db.openTable("src", .{});
    try src.flush();
    try exec(allocator, db, "CREATE TABLE dst (id BIGINT PRIMARY KEY)");

    // Existing dst — CTAS should error.
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), "CREATE TABLE dst AS SELECT id FROM src");
    const cq = thindb.net.compile(allocator, db, root);
    if (cq) |ok| {
        var c = ok;
        c.deinit();
        return error.TestUnexpectedSuccess;
    } else |err| {
        try std.testing.expectEqual(thindb.net.Error.TableAlreadyExists, err);
    }

    // IF NOT EXISTS — silent no-op.
    try exec(allocator, db, "CREATE TABLE IF NOT EXISTS dst AS SELECT id FROM src");
}

test "INSERT SELECT: full positional copy" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "CREATE TABLE dst (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO src (id, qty) VALUES (1, 10), (2, 20)");
    const src = try db.openTable("src", .{});
    try src.flush();

    try exec(allocator, db, "INSERT INTO dst SELECT id, qty FROM src");
    const dst = try db.openTable("dst", .{});
    try dst.flush();

    const ids = try collectBigints(allocator, db, "SELECT id FROM dst ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, ids);
}

test "INSERT SELECT: named column list reorders source columns" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (a BIGINT PRIMARY KEY, b INT NOT NULL)");
    try exec(allocator, db, "CREATE TABLE dst (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO src (a, b) VALUES (5, 50)");
    const src = try db.openTable("src", .{});
    try src.flush();

    // dst.(id, qty) ← src.(a, b)
    try exec(allocator, db, "INSERT INTO dst (id, qty) SELECT a, b FROM src");
    const dst = try db.openTable("dst", .{});
    try dst.flush();

    const ids = try collectBigints(allocator, db, "SELECT id FROM dst");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{5}, ids);
}

test "INSERT SELECT: width mismatch rejected" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY)");
    try exec(allocator, db, "CREATE TABLE dst (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), "INSERT INTO dst SELECT id FROM src");
    const cq = thindb.net.compile(allocator, db, root);
    if (cq) |ok| {
        var c = ok;
        c.deinit();
        return error.TestUnexpectedSuccess;
    } else |err| {
        try std.testing.expectEqual(thindb.net.Error.BadRequest, err);
    }
}
