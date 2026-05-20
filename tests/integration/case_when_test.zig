//! Searched CASE WHEN expressions in SELECT projections.
//!
//!   CASE WHEN cond THEN expr [WHEN cond THEN expr]* [ELSE expr] END
//!
//! v1 scope:
//!   - Searched form only (no `CASE col WHEN val THEN ...`).
//!   - All branches' THEN (+ optional ELSE) must unify to one type.
//!   - THEN/ELSE: column ref, literal, or scalar function call.
//!     Nested CASE in a branch is rejected at compile time.
//!   - Conditions can reference upstream columns (the predicates run
//!     against the input batch's schema, same as WHERE).

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

test "CASE WHEN: literal branches, ELSE present" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)",
    );
    try exec(allocator, db, "INSERT INTO t (id, qty) VALUES (1, 5), (2, 50), (3, 500)");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db,
        "SELECT CASE WHEN qty < 10 THEN 'small' WHEN qty < 100 THEN 'medium' ELSE 'large' END AS bucket FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), batch.row_count);
    try std.testing.expectEqualStrings("small", batch.values[0].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("medium", batch.values[0].data.string.rowBytes(1));
    try std.testing.expectEqualStrings("large", batch.values[0].data.string.rowBytes(2));
}

test "CASE WHEN: ELSE-less form emits NULL for unmatched rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)",
    );
    try exec(allocator, db, "INSERT INTO t (id, qty) VALUES (1, 5), (2, 50)");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db,
        "SELECT CASE WHEN qty < 10 THEN 'small' END AS bucket FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), batch.row_count);
    try std.testing.expect(batch.values[0].isValid(0));
    try std.testing.expectEqualStrings("small", batch.values[0].data.string.rowBytes(0));
    try std.testing.expect(!batch.values[0].isValid(1));
}

test "CASE WHEN: integer branches via column ref" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL)",
    );
    try exec(allocator, db, "INSERT INTO t (id, a, b) VALUES (1, 10, 20), (2, 30, 5)");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db,
        "SELECT CASE WHEN a < 25 THEN a ELSE b END AS picked FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), batch.row_count);
    try std.testing.expectEqual(@as(i32, 10), batch.values[0].data.int[0]);
    try std.testing.expectEqual(@as(i32, 5), batch.values[0].data.int[1]);
}

test "CASE WHEN: first-true wins across overlapping conditions" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)",
    );
    try exec(allocator, db, "INSERT INTO t (id, qty) VALUES (1, 5)");
    const t = try db.openTable("t", .{});
    try t.flush();

    // qty=5 matches BOTH conditions; first one wins.
    var q = try runSql(allocator, db,
        "SELECT CASE WHEN qty < 100 THEN 'small' WHEN qty < 1000 THEN 'medium' ELSE 'large' END AS bucket FROM t",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqualStrings("small", batch.values[0].data.string.rowBytes(0));
}

test "CASE WHEN: AND/OR conditions in WHEN" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, region VARCHAR(8) NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO t (id, qty, region) VALUES (1, 5, 'east'), (2, 50, 'west'), (3, 5, 'west')",
    );
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db,
        "SELECT CASE WHEN qty < 10 AND region = 'west' THEN 'small-west' ELSE 'other' END AS label FROM t ORDER BY id ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), batch.row_count);
    try std.testing.expectEqualStrings("other", batch.values[0].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("other", batch.values[0].data.string.rowBytes(1));
    try std.testing.expectEqualStrings("small-west", batch.values[0].data.string.rowBytes(2));
}

test "CASE WHEN: rejects branches with mismatched types" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(),
        "SELECT CASE WHEN qty < 10 THEN 1 ELSE 'big' END AS mix FROM t",
    );
    const cq = thindb.net.compile(allocator, db, root);
    if (cq) |ok| {
        var c = ok;
        c.deinit();
        return error.TestUnexpectedSuccess;
    } else |err| {
        try std.testing.expectEqual(thindb.exec.Error.ComputeUnsupportedExpr, err);
    }
}
