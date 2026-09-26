//! Literal-on-LHS predicates. Handled:
//!   - lit op lit  → evaluated at parse time when both share a type,
//!                   predicate becomes constant TRUE/FALSE (`.always`).
//!   - lit IS [NOT] NULL, NULL IS [NOT] NULL → `.always`; NULL op X and
//!                   lit op NULL → UNKNOWN.
//!   - lit [NOT] BETWEEN / IN / LIKE → the literal as a computed column.
//!   - lit op col  → flipped to `col reverse_op lit` (normal leaf).
//!   - an expression a literal leads, on either side (`1 + qty > 25`,
//!     `25 > qty + 1`) → a computed comparison operand.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(
        allocator,
        db,
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

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM t WHERE 1 = 1 AND 15 < qty ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, ids);
}

test "literal-on-LHS: IS [NOT] NULL, NULL comparisons, BETWEEN / IN / LIKE" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ .where = "NULL IS NULL", .ids = &[_]i64{ 1, 2, 3 } },
        .{ .where = "NULL IS NOT NULL", .ids = &[_]i64{} },
        .{ .where = "5 IS NULL", .ids = &[_]i64{} },
        .{ .where = "'x' IS NOT NULL", .ids = &[_]i64{ 1, 2, 3 } },
        .{ .where = "NULL = 1", .ids = &[_]i64{} },
        .{ .where = "1 = NULL", .ids = &[_]i64{} },
        .{ .where = "NOT ('a' <> NULL)", .ids = &[_]i64{} },
        .{ .where = "(NULL IS NULL OR 1 = NULL)", .ids = &[_]i64{ 1, 2, 3 } },
        .{ .where = "NOT (NULL = qty)", .ids = &[_]i64{} },
        .{ .where = "NOT (NULL IS NULL)", .ids = &[_]i64{} },
        // Optional-parameter guard as generated SQL binds it when the parameter is absent.
        .{ .where = "(NULL IS NULL OR ABS(qty) = NULL)", .ids = &[_]i64{ 1, 2, 3 } },
        .{ .where = "(NULL IS NOT NULL OR qty = 20)", .ids = &[_]i64{2} },
        .{ .where = "20 BETWEEN qty AND 30", .ids = &[_]i64{ 1, 2 } },
        .{ .where = "20 NOT BETWEEN qty AND 30", .ids = &[_]i64{3} },
        .{ .where = "20 IN (10, 20)", .ids = &[_]i64{ 1, 2, 3 } },
        .{ .where = "5 NOT IN (10, 20)", .ids = &[_]i64{ 1, 2, 3 } },
        .{ .where = "'abc' LIKE 'a%'", .ids = &[_]i64{ 1, 2, 3 } },
    };
    inline for (cases) |c| {
        const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE " ++ c.where ++ " ORDER BY id ASC");
        defer allocator.free(ids);
        std.testing.expectEqualSlices(i64, c.ids, ids) catch |err| {
            std.debug.print("WHERE {s}\n", .{c.where});
            return err;
        };
    }
}

test "literal-led expressions on either side of a comparison (issue #112)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ .where = "1 + qty > 25", .ids = &[_]i64{3} },
        .{ .where = "100 - qty < 85", .ids = &[_]i64{ 2, 3 } },
        .{ .where = "2 * qty = 40", .ids = &[_]i64{2} },
        .{ .where = "60 / qty > 2.5", .ids = &[_]i64{ 1, 2 } },
        .{ .where = "1 + qty * 2 > 50", .ids = &[_]i64{3} },
        .{ .where = "-qty < -15", .ids = &[_]i64{ 2, 3 } },
        .{ .where = "-1 + qty > 25", .ids = &[_]i64{3} },
        .{ .where = "25 > 1 + qty", .ids = &[_]i64{ 1, 2 } },
        .{ .where = "25 > qty + 1", .ids = &[_]i64{ 1, 2 } },
        .{ .where = "15 < ABS(qty)", .ids = &[_]i64{ 2, 3 } },
        .{ .where = "qty > 1 + 14", .ids = &[_]i64{ 2, 3 } },
        .{ .where = "qty > 5 + id * 5", .ids = &[_]i64{ 2, 3 } },
        .{ .where = "qty > (1 + 1) * 10", .ids = &[_]i64{3} },
        .{ .where = "qty + 1 > 20 + 1", .ids = &[_]i64{3} },
        .{ .where = "1 + 1 = 2", .ids = &[_]i64{ 1, 2, 3 } },
        .{ .where = "1 + 1 = 3", .ids = &[_]i64{} },
        .{ .where = "1 + 1 < qty", .ids = &[_]i64{ 1, 2, 3 } },
        .{ .where = "1 = 1.0", .ids = &[_]i64{ 1, 2, 3 } },
        .{ .where = "2.5 > 1", .ids = &[_]i64{ 1, 2, 3 } },
        .{ .where = "1 + qty BETWEEN 15 AND 25", .ids = &[_]i64{2} },
        .{ .where = "1 + qty IN (11, 31)", .ids = &[_]i64{ 1, 3 } },
        .{ .where = "1 + qty IS NOT NULL", .ids = &[_]i64{ 1, 2, 3 } },
        .{ .where = "NOT 1 + qty > 25", .ids = &[_]i64{ 1, 2 } },
        .{ .where = "qty > 5 AND 1 + qty > 25", .ids = &[_]i64{3} },
        .{ .where = "(qty + 1) * 2 > 50", .ids = &[_]i64{3} },
        .{ .where = "(qty) * 2 = 40", .ids = &[_]i64{2} },
        .{ .where = "5 > (SELECT MIN(qty) FROM t)", .ids = &[_]i64{} },
        .{ .where = "15 > (SELECT MIN(qty) FROM t)", .ids = &[_]i64{ 1, 2, 3 } },
    };
    inline for (cases) |c| {
        const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE " ++ c.where ++ " ORDER BY id ASC");
        defer allocator.free(ids);
        std.testing.expectEqualSlices(i64, c.ids, ids) catch |err| {
            std.debug.print("WHERE {s}\n", .{c.where});
            return err;
        };
    }

    try exec(allocator, db, "DELETE FROM t WHERE 1 + qty > 25");
    const left = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(left);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, left);
}

test "a bare literal or expression is truthiness: non-zero and non-NULL" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ .where = "1", .ids = &[_]i64{ 1, 2, 3 } },
        .{ .where = "0", .ids = &[_]i64{} },
        .{ .where = "0.0", .ids = &[_]i64{} },
        .{ .where = "TRUE", .ids = &[_]i64{ 1, 2, 3 } },
        .{ .where = "NOT 1", .ids = &[_]i64{} },
        .{ .where = "1 AND qty > 15", .ids = &[_]i64{ 2, 3 } },
        .{ .where = "qty - 20", .ids = &[_]i64{ 1, 3 } },
        .{ .where = "20 - qty", .ids = &[_]i64{ 1, 3 } },
        .{ .where = "ABS(qty)", .ids = &[_]i64{ 1, 2, 3 } },
        .{ .where = "ABS(qty - 20)", .ids = &[_]i64{ 1, 3 } },
        .{ .where = "NOT ABS(qty - 20)", .ids = &[_]i64{2} },
    };
    inline for (cases) |c| {
        const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE " ++ c.where ++ " ORDER BY id ASC");
        defer allocator.free(ids);
        std.testing.expectEqualSlices(i64, c.ids, ids) catch |err| {
            std.debug.print("WHERE {s}\n", .{c.where});
            return err;
        };
    }
}

test "HAVING compares computed operands, literal-led or not (issue #112)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ .having = "SUM(qty) + 1 > 25", .ids = &[_]i64{3} },
        .{ .having = "1 + SUM(qty) > 25", .ids = &[_]i64{3} },
        .{ .having = "45 < SUM(qty) * 2 + 1", .ids = &[_]i64{3} },
        .{ .having = "(1 + SUM(qty)) > 25", .ids = &[_]i64{3} },
        .{ .having = "ABS(SUM(qty)) > 15", .ids = &[_]i64{ 2, 3 } },
        .{ .having = "SUM(qty) / COUNT(*) >= 20", .ids = &[_]i64{ 2, 3 } },
        .{ .having = "SUM(qty) > id * 10 - 1", .ids = &[_]i64{ 1, 2, 3 } },
        .{ .having = "SUM(qty) * 2 BETWEEN 30 AND 50", .ids = &[_]i64{2} },
        .{ .having = "SUM(qty) > 1 + 14", .ids = &[_]i64{ 2, 3 } },
        .{ .having = "SUM(qty) + 1 > 25 OR id = 1", .ids = &[_]i64{ 1, 3 } },
        .{ .having = "total + 1 > 25", .ids = &[_]i64{3} },
    };
    inline for (cases) |c| {
        const ids = try collectBigints(allocator, db, "SELECT id, SUM(qty) AS total FROM t GROUP BY id HAVING " ++ c.having ++ " ORDER BY id ASC");
        defer allocator.free(ids);
        std.testing.expectEqualSlices(i64, c.ids, ids) catch |err| {
            std.debug.print("HAVING {s}\n", .{c.having});
            return err;
        };
    }

    const counts = try collectBigints(allocator, db, "SELECT COUNT(*) FROM t HAVING COUNT(*) * 2 = 6");
    defer allocator.free(counts);
    try std.testing.expectEqualSlices(i64, &.{3}, counts);
    const none = try collectBigints(allocator, db, "SELECT COUNT(*) FROM t HAVING 1 + COUNT(*) > 10");
    defer allocator.free(none);
    try std.testing.expectEqualSlices(i64, &.{}, none);
}

test "a computed operand may read another computed operand" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ .where = "(CASE WHEN 1 + qty > 25 THEN 1 ELSE 0 END) = 1", .ids = &[_]i64{3} },
        .{ .where = "(CASE WHEN qty + 1 > 25 THEN 1 ELSE 0 END) = 1", .ids = &[_]i64{3} },
        .{ .where = "(CASE WHEN (CASE WHEN qty > 15 THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END) = 1", .ids = &[_]i64{ 2, 3 } },
        .{ .where = "(CASE WHEN ABS(qty - 20) > 5 THEN qty ELSE 0 END) * 2 > 50", .ids = &[_]i64{3} },
    };
    inline for (cases) |c| {
        const ids = try collectBigints(allocator, db, "SELECT id FROM t WHERE " ++ c.where ++ " ORDER BY id ASC");
        defer allocator.free(ids);
        std.testing.expectEqualSlices(i64, c.ids, ids) catch |err| {
            std.debug.print("WHERE {s}\n", .{c.where});
            return err;
        };
    }

    const grouped = try collectBigints(allocator, db, "SELECT id FROM t GROUP BY id HAVING (CASE WHEN SUM(qty) + 1 > 25 THEN 1 ELSE 0 END) = 1");
    defer allocator.free(grouped);
    try std.testing.expectEqualSlices(i64, &.{3}, grouped);

    try exec(allocator, db, "DELETE FROM t WHERE (CASE WHEN (CASE WHEN qty > 15 THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END) = 1");
    const left = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(left);
    try std.testing.expectEqualSlices(i64, &.{1}, left);
}
