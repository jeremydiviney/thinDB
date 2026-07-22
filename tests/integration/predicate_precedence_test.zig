//! WHERE-clause operator precedence with no explicit parens: SQL binds
//! NOT tighter than AND tighter than OR. The plan-time predicate
//! optimizer only permutes siblings within a single AND/OR node, so the
//! precedence grouping the parser built is preserved through reordering.
//! These run the full SQL path and would fail if either the parser
//! mis-grouped or the optimizer restructured across an AND/OR boundary.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, c INT NOT NULL)",
    );
    // Chosen so a<5 / b=20 / c=30 each fall inside the observed column
    // range (no provable always-true/false collapse), and so AND-tighter
    // vs OR-tighter give different row sets — row 1 is the discriminator.
    try exec(
        allocator,
        db,
        "INSERT INTO t (id, a, b, c) VALUES (1, 1, 99, 99), (2, 99, 20, 30), (3, 99, 20, 99), (4, 99, 99, 99)",
    );
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

test "precedence: AND binds tighter than OR (no parens)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // a<5 OR b=20 AND c=30  ==  a<5 OR (b=20 AND c=30)
    //   row1: a<5 → T.                              ✓
    //   row2: b=20 AND c=30 → T.                    ✓
    //   row3: a<5 F, b=20 AND c=30 F (c=99).        ✗
    //   row4: all F.                                ✗
    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM t WHERE a < 5 OR b = 20 AND c = 30 ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, ids);
}

test "precedence: explicit parens override the default grouping" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // (a<5 OR b=20) AND c=30 — forcing the other grouping drops row 1
    // (c=99), leaving only row 2. Proves parens reshape the tree.
    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM t WHERE (a < 5 OR b = 20) AND c = 30 ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{2}, ids);
}

test "precedence: NOT binds tighter than AND (no parens)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // NOT a<5 AND b=20  ==  (NOT (a<5)) AND b=20 → a>=5 and b=20 → {2,3}.
    // If NOT bound looser (NOT (a<5 AND b=20)) every row would match.
    const ids = try collectBigints(
        allocator,
        db,
        "SELECT id FROM t WHERE NOT a < 5 AND b = 20 ORDER BY id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, ids);
}
