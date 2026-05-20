//! AUTO_INCREMENT column attribute — MySQL-style semantics.
//!
//! Counter advances past any explicit value the caller supplies, omitted
//! and explicit-NULL inserts pull the next id, the counter survives
//! reopen via the v5 manifest header, and on a crash-between-insert-
//! and-flush we defensively reconcile from segment column stats.

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

fn collectBigints(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]i64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(i64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |batch| {
        for (batch.values[0].data.bigint[0..batch.row_count]) |v| {
            try out.append(allocator, v);
        }
    }
    return out.toOwnedSlice(allocator);
}

test "AUTO_INCREMENT: fills omitted column with monotonic counter" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY AUTO_INCREMENT, label VARCHAR(8) NOT NULL)",
    );
    try exec(allocator, db, "INSERT INTO t (label) VALUES ('a'), ('b'), ('c')");
    const t = try db.openTable("t", .{});
    try t.flush();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, ids);
}

test "AUTO_INCREMENT: explicit value advances counter past it" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY AUTO_INCREMENT, label VARCHAR(8) NOT NULL)",
    );
    // Caller supplies 50 explicitly — counter jumps to 51.
    try exec(allocator, db, "INSERT INTO t (id, label) VALUES (50, 'big')");
    // Subsequent omitted inserts should pick up at 51, 52.
    try exec(allocator, db, "INSERT INTO t (label) VALUES ('a'), ('b')");
    const t = try db.openTable("t", .{});
    try t.flush();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 50, 51, 52 }, ids);
}

test "AUTO_INCREMENT: explicit value below counter leaves counter unchanged" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY AUTO_INCREMENT, label VARCHAR(8) NOT NULL)",
    );
    // Burn ids 1..3 via omitted inserts.
    try exec(allocator, db, "INSERT INTO t (label) VALUES ('a'), ('b'), ('c')");
    // Counter is now 4. An explicit 2 is below the counter so it must
    // NOT pull the counter backward.
    try exec(allocator, db, "INSERT INTO t (id, label) VALUES (100, 'big')");
    try exec(allocator, db, "INSERT INTO t (label) VALUES ('d')");
    const t = try db.openTable("t", .{});
    try t.flush();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    // Expected: 1, 2, 3 (omitted) → 100 (explicit) → 101 (counter now 101)
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 100, 101 }, ids);
}

test "AUTO_INCREMENT: explicit NULL on AI column behaves like omitted" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY AUTO_INCREMENT, label VARCHAR(8) NOT NULL)",
    );
    try exec(allocator, db, "INSERT INTO t (id, label) VALUES (NULL, 'a'), (NULL, 'b')");
    const t = try db.openTable("t", .{});
    try t.flush();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, ids);
}

test "AUTO_INCREMENT: counter survives reopen (manifest v5 round-trip)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        try exec(allocator, db,
            "CREATE TABLE t (id BIGINT PRIMARY KEY AUTO_INCREMENT, label VARCHAR(8) NOT NULL)",
        );
        try exec(allocator, db, "INSERT INTO t (label) VALUES ('a'), ('b'), ('c')");
        const t = try db.openTable("t", .{});
        try t.flush();
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "INSERT INTO t (label) VALUES ('d')");
    const t = try db.openTable("t", .{});
    try t.flush();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4 }, ids);
}

test "AUTO_INCREMENT: rejected on non-integer types" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try expectRunError(allocator, db,
        "CREATE TABLE t (id VARCHAR(8) PRIMARY KEY AUTO_INCREMENT)",
        thindb.net.Error.TypeMismatch,
    );
}

test "AUTO_INCREMENT: rejected when two columns declare it" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try expectRunError(allocator, db,
        "CREATE TABLE t (a BIGINT PRIMARY KEY AUTO_INCREMENT, b BIGINT NOT NULL AUTO_INCREMENT)",
        thindb.net.Error.UnsupportedOp,
    );
}

test "AUTO_INCREMENT: rejected with DEFAULT alongside" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try expectRunError(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY AUTO_INCREMENT DEFAULT 42)",
        thindb.net.Error.UnsupportedOp,
    );
}

test "AUTO_INCREMENT: INT column width is respected" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, label VARCHAR(8) NOT NULL)",
    );
    try exec(allocator, db, "INSERT INTO t (label) VALUES ('a'), ('b')");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), batch.row_count);
    try std.testing.expectEqual(@as(i32, 1), batch.values[0].data.int[0]);
    try std.testing.expectEqual(@as(i32, 2), batch.values[0].data.int[1]);
}
