//! Qualified column references — `t.col` syntax and `FROM t AS alias`.
//!
//! With an alias, the scan's output schema is rewritten so columns
//! are exposed as `alias.colname`. Bare `col` still resolves via a
//! suffix-match fallback (any column ending in `.col`). Without an
//! alias, `t.col` resolves via a prefix-strip fallback. Together
//! that means everyday queries keep working while self-joins can
//! disambiguate same-named columns from two scans of the same table.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSql = helpers.runSql;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO t (id, qty) VALUES (1, 10), (2, 20), (3, 30)",
    );
    const tt = try db.openTable("t", .{});
    try tt.flush();
    return db;
}

test "qualified col: t.col against unaliased scan resolves via prefix strip" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SELECT t.id FROM t WHERE t.qty > 15 ORDER BY t.id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, ids);
}

test "qualified col: bare col against aliased scan resolves via suffix match" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SELECT id FROM t AS a WHERE qty > 15 ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, ids);
}

test "qualified col: aliased a.col resolves directly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SELECT a.id FROM t AS a WHERE a.qty >= 20 ORDER BY a.id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, ids);
}

test "qualified col: aliased col vs literal predicate" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SELECT a.id FROM t AS a WHERE a.qty = 20",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{2}, ids);
}

test "qualified col: self-join on aliased copies of same table" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db,
        "CREATE TABLE emp (id BIGINT PRIMARY KEY, manager_id BIGINT NOT NULL, name VARCHAR(16) NOT NULL)",
    );
    try exec(allocator, db,
        "INSERT INTO emp (id, manager_id, name) VALUES (1, 0, 'alice'), (2, 1, 'bob'), (3, 1, 'carol'), (4, 2, 'dave')",
    );
    const tt = try db.openTable("emp", .{});
    try tt.flush();

    const ids = try collectBigints(allocator, db,
        "SELECT e.id FROM emp AS e JOIN emp AS m ON e.manager_id = m.id ORDER BY e.id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3, 4 }, ids);
}

test "qualified col: aliased col in ORDER BY" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(allocator, db,
        "SELECT a.id FROM t AS a ORDER BY a.qty DESC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 3, 2, 1 }, ids);
}
