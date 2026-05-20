//! DEFAULT clause on CREATE TABLE columns.
//!
//! Tier 1 of column defaults — literal values only (no expressions /
//! function calls / AUTO_INCREMENT yet). The INSERT path fills the
//! default when the user omits the column from the column list.

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

fn expectRunError(
    allocator: std.mem.Allocator,
    db: anytype,
    sql: []const u8,
    expected: anyerror,
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const parsed = thindb.sql.parse(arena.allocator(), sql);
    if (parsed) |root| {
        const cq_result = thindb.net.compile(allocator, db, root);
        if (cq_result) |cq_ok| {
            var cq = cq_ok;
            cq.deinit();
            return error.TestUnexpectedSuccess;
        } else |err| {
            try std.testing.expectEqual(expected, err);
        }
    } else |err| {
        try std.testing.expectEqual(expected, err);
    }
}

test "DEFAULT: integer literal fills omitted column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL DEFAULT 100)",
    );
    try exec(allocator, db, "INSERT INTO t (id) VALUES (1), (2)");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db, "SELECT qty FROM t ORDER BY id ASC");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), batch.row_count);
    try std.testing.expectEqual(@as(i32, 100), batch.values[0].data.int[0]);
    try std.testing.expectEqual(@as(i32, 100), batch.values[0].data.int[1]);
}

test "DEFAULT: text literal fills omitted column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, role VARCHAR(16) NOT NULL DEFAULT 'member')",
    );
    try exec(allocator, db, "INSERT INTO t (id) VALUES (1)");
    try exec(allocator, db, "INSERT INTO t (id, role) VALUES (2, 'admin')");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db, "SELECT role FROM t ORDER BY id ASC");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), batch.row_count);
    try std.testing.expectEqualStrings("member", batch.values[0].data.varchar.rowBytes(0));
    try std.testing.expectEqualStrings("admin", batch.values[0].data.varchar.rowBytes(1));
}

test "DEFAULT: boolean literal" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, active BOOLEAN NOT NULL DEFAULT TRUE)",
    );
    try exec(allocator, db, "INSERT INTO t (id) VALUES (1)");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db, "SELECT active FROM t");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(u8, 1), batch.values[0].data.boolean[0]);
}

test "DEFAULT: NOT NULL without DEFAULT still rejects omitted column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)",
    );
    // qty is NOT NULL with no default → omitting it must error.
    try expectRunError(allocator, db,
        "INSERT INTO t (id) VALUES (1)",
        thindb.net.Error.ColumnNotFound,
    );
}

test "DEFAULT: provided value overrides default" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL DEFAULT 100)",
    );
    try exec(allocator, db, "INSERT INTO t (id, qty) VALUES (1, 5), (2, 200)");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db, "SELECT qty FROM t ORDER BY id ASC");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(i32, 5), batch.values[0].data.int[0]);
    try std.testing.expectEqual(@as(i32, 200), batch.values[0].data.int[1]);
}

test "DEFAULT: type mismatch rejected at CREATE TABLE" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // BIGINT column with a text DEFAULT — the literal's value tag
    // doesn't match the declared column type.
    try expectRunError(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty BIGINT DEFAULT 'oops')",
        thindb.net.Error.TypeMismatch,
    );
}

test "DEFAULT: survives reopen via schema.bin (v3 round-trip)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Phase 1: create + populate with default.
    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        try exec(allocator, db,
            "CREATE TABLE t (id BIGINT PRIMARY KEY, role VARCHAR(8) NOT NULL DEFAULT 'guest')",
        );
        try exec(allocator, db, "INSERT INTO t (id) VALUES (1)");
        const t = try db.openTable("t", .{});
        try t.flush();
    }

    // Phase 2: reopen the database; the persisted schema must round-trip
    // the DEFAULT so subsequent INSERTs still use it.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "INSERT INTO t (id) VALUES (2)");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db, "SELECT role FROM t ORDER BY id ASC");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), batch.row_count);
    try std.testing.expectEqualStrings("guest", batch.values[0].data.varchar.rowBytes(0));
    try std.testing.expectEqualStrings("guest", batch.values[0].data.varchar.rowBytes(1));
}
