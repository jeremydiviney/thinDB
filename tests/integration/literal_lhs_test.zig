//! Literal-on-LHS predicate comparisons. v1 handles:
//!   - lit op lit  → evaluated at parse time, predicate becomes
//!                   constant TRUE/FALSE (`.always`).
//!   - lit op col  → flipped to `col reverse_op lit` (normal leaf).
//! Subquery on either side of a literal-LHS is rejected; users
//! rewrite as `col op (SELECT ...)`.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db,
        "INSERT INTO t (id, qty) VALUES (1, 10), (2, 20), (3, 30)",
    );
    const tt = try db.openTable("t", .{});
    try tt.flush();
    return db;
}

test "literal-on-LHS: 1 = 1 → all rows pass" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE 1 = 1 ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, ids);
}

test "literal-on-LHS: 1 = 2 → no rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE 1 = 2");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{}, ids);
}

test "literal-on-LHS: 20 < qty → flipped to qty > 20" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE 20 < qty");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{3}, ids);
}

test "literal-on-LHS: 30 >= qty → flipped to qty <= 30" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE 30 >= qty ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, ids);
}

test "literal-on-LHS: composes with AND" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SELECT id FROM t WHERE 1 = 1 AND 15 < qty ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, ids);
}
