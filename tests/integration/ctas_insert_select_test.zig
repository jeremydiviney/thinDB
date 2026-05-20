//! CREATE TABLE name AS SELECT ... (CTAS) and INSERT INTO target [(cols)]
//! SELECT ... — schema-inferred table creation and query-driven inserts.

const std = @import("std");
const thindb = @import("thindb");

const RunResult = struct {
    arena: std.heap.ArenaAllocator,
    cq: thindb.net.CompiledQuery,

    pub fn deinit(self: *RunResult) void {
        self.cq.deinit();
        self.arena.deinit();
    }
    pub fn next(self: *RunResult) !?thindb.Batch {
        return self.cq.next();
    }
};

fn runSql(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !RunResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), sql);
    const cq = try thindb.net.compile(allocator, db, root);
    return .{ .arena = arena, .cq = cq };
}

fn exec(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !void {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    while (try q.next()) |_| {}
}

fn collectBigints(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]i64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(i64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |batch| {
        for (batch.values[0].data.bigint[0..batch.row_count]) |v| try out.append(allocator, v);
    }
    return out.toOwnedSlice(allocator);
}

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
