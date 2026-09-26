//! UNION ALL concatenates two SELECT pipelines with matching schemas;
//! UNION [DISTINCT] keeps one copy of each distinct row; INTERSECT and
//! EXCEPT keep the distinct rows both arms hold, or only the left one does.

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

test "UNION: keeps one copy of each distinct row" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE c (id BIGINT PRIMARY KEY, v BIGINT)");
    try exec(allocator, db, "INSERT INTO c (id, v) VALUES (1, NULL), (2, NULL), (3, 5)");

    const cases = .{
        .{ "SELECT id FROM a UNION SELECT id FROM b ORDER BY id", &[_]i64{ 1, 2, 3, 4 } },
        .{ "SELECT id FROM a UNION DISTINCT SELECT id FROM b ORDER BY 1", &[_]i64{ 1, 2, 3, 4 } },
        .{ "SELECT id % 2 FROM a UNION SELECT id % 2 FROM b ORDER BY 1", &[_]i64{ 0, 1 } },
        .{ "SELECT id, id * 0 AS z FROM a UNION SELECT id, 0 FROM b ORDER BY id", &[_]i64{ 1, 2, 3, 4 } },
        // Left-associative: (a ∪all a) ∪ b, then (a ∪ b) ∪all b.
        .{ "SELECT id FROM a UNION ALL SELECT id FROM a UNION SELECT id FROM b ORDER BY id", &[_]i64{ 1, 2, 3, 4 } },
        .{ "SELECT id FROM a UNION SELECT id FROM b UNION ALL SELECT id FROM b ORDER BY id", &[_]i64{ 1, 2, 2, 3, 4, 4 } },
        // A column no consumer reads still decides which rows are duplicates.
        .{ "SELECT k FROM (SELECT id % 2 AS k, id FROM a UNION SELECT id % 2, id FROM b) x ORDER BY k", &[_]i64{ 0, 0, 1, 1 } },
        .{ "SELECT id FROM a WHERE id IN (SELECT id FROM b UNION SELECT 3) ORDER BY id", &[_]i64{ 2, 3 } },
        // NULLs compare equal, as in SELECT DISTINCT.
        .{ "SELECT COUNT(*) FROM (SELECT v FROM c UNION SELECT v FROM c) x", &[_]i64{2} },
    };
    inline for (cases) |c| {
        const got = try collectBigints(allocator, db, c[0]);
        defer allocator.free(got);
        std.testing.expectEqualSlices(i64, c[1], got) catch |err| {
            std.debug.print("query: {s}\n", .{c[0]});
            return err;
        };
    }
}

test "UNION: a trailing ORDER BY / LIMIT applies to the whole union" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id DESC", &[_]i64{ 4, 3, 2, 2, 1 } },
        .{ "SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id LIMIT 3", &[_]i64{ 1, 2, 2 } },
        .{ "SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY 1 DESC LIMIT 2 OFFSET 1", &[_]i64{ 3, 2 } },
        .{ "SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id % 3, id", &[_]i64{ 3, 1, 4, 2, 2 } },
        .{ "SELECT id AS k FROM a UNION SELECT id FROM b ORDER BY k DESC", &[_]i64{ 4, 3, 2, 1 } },
        .{ "(SELECT id FROM a ORDER BY id DESC LIMIT 1) UNION ALL (SELECT id FROM b ORDER BY id LIMIT 1)", &[_]i64{ 3, 2 } },
        .{ "(SELECT id FROM a) UNION (SELECT id FROM b) ORDER BY id LIMIT 3", &[_]i64{ 1, 2, 3 } },
        .{ "SELECT id FROM a UNION ALL (SELECT id FROM b ORDER BY id DESC LIMIT 1) ORDER BY id", &[_]i64{ 1, 2, 3, 4 } },
    };
    inline for (cases) |c| {
        const got = try collectBigints(allocator, db, c[0]);
        defer allocator.free(got);
        std.testing.expectEqualSlices(i64, c[1], got) catch |err| {
            std.debug.print("query: {s}\n", .{c[0]});
            return err;
        };
    }

    const limited = try collectBigints(allocator, db, "SELECT id FROM a UNION ALL SELECT id FROM b LIMIT 2");
    defer allocator.free(limited);
    try std.testing.expectEqual(@as(usize, 2), limited.len);

    // The expression key's hidden column never reaches the output.
    var q = try helpers.runSql(allocator, db, "SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY -id");
    defer q.deinit();
    try std.testing.expectEqual(@as(usize, 1), q.outputSchema().len);
}

test "UNION: a bare NULL arm column takes the other arm's type" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE f (id BIGINT PRIMARY KEY, amt DOUBLE, d DATE)");
    try exec(allocator, db, "INSERT INTO f (id, amt, d) VALUES (7, 1.5, '2024-01-01')");

    const cases = .{
        .{ "SELECT COUNT(x) FROM (SELECT id, NULL AS x FROM a UNION ALL SELECT id, amt FROM f) t", &[_]i64{1} },
        .{ "SELECT COUNT(x) FROM (SELECT id, amt AS x FROM f UNION ALL SELECT id, NULL FROM a) t", &[_]i64{1} },
        .{ "SELECT COUNT(*) FROM (SELECT id, NULL AS x FROM a UNION SELECT id, d FROM f) t", &[_]i64{4} },
        .{ "SELECT k FROM (SELECT NULL AS k UNION ALL SELECT id FROM b) t WHERE k IS NOT NULL ORDER BY k", &[_]i64{ 2, 4 } },
        .{ "SELECT k FROM (SELECT id AS k FROM b UNION SELECT NULL FROM a) t WHERE k > 2", &[_]i64{4} },
        .{ "SELECT k FROM (SELECT v.k FROM (SELECT NULL AS k) v UNION ALL SELECT id FROM b) t WHERE k > 2", &[_]i64{4} },
    };
    inline for (cases) |c| {
        const got = try collectBigints(allocator, db, c[0]);
        defer allocator.free(got);
        std.testing.expectEqualSlices(i64, c[1], got) catch |err| {
            std.debug.print("query: {s}\n", .{c[0]});
            return err;
        };
    }

    var q = try helpers.runSql(allocator, db, "SELECT NULL AS x FROM a UNION ALL SELECT amt FROM f");
    defer q.deinit();
    try std.testing.expectEqual(thindb.types.TypeTag.double, std.meta.activeTag(q.outputSchema()[0].type));
}

test "UNION ALL: a computed column over a union CTE" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // The Compute splits into the arms above each one's SELECT list.
    const got = try collectBigints(allocator, db, "WITH u AS (SELECT id FROM a UNION ALL SELECT id FROM b) SELECT id % 3 AS m FROM u ORDER BY id");
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 2, 0, 1 }, got);
}

test "INTERSECT / EXCEPT: distinct rows both arms hold, or only the left" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE c (id BIGINT PRIMARY KEY, v BIGINT)");
    try exec(allocator, db, "INSERT INTO c (id, v) VALUES (1, NULL), (2, NULL), (3, 5)");
    try exec(allocator, db, "CREATE TABLE d (id BIGINT PRIMARY KEY, k BIGINT)");
    try exec(allocator, db, "INSERT INTO d (id, k) VALUES (1, 1), (2, 1), (3, 2), (4, NULL), (5, NULL), (6, 5)");

    const cases = .{
        .{ "SELECT id FROM a INTERSECT SELECT id FROM b ORDER BY id", &[_]i64{2} },
        .{ "SELECT id FROM a INTERSECT DISTINCT SELECT id FROM b ORDER BY id", &[_]i64{2} },
        .{ "SELECT id FROM a EXCEPT SELECT id FROM b ORDER BY id", &[_]i64{ 1, 3 } },
        .{ "SELECT id FROM a EXCEPT DISTINCT SELECT id FROM b ORDER BY id", &[_]i64{ 1, 3 } },
        .{ "SELECT id FROM a MINUS SELECT id FROM b ORDER BY id", &[_]i64{ 1, 3 } },
        .{ "SELECT id FROM b EXCEPT SELECT id FROM a", &[_]i64{4} },
        // INTERSECT binds tighter: a − (a ∩ b), not (a − a) ∩ b.
        .{ "SELECT id FROM a EXCEPT SELECT id FROM a INTERSECT SELECT id FROM b ORDER BY id", &[_]i64{ 1, 3 } },
        .{ "SELECT id FROM a INTERSECT SELECT id FROM b UNION SELECT 9 ORDER BY 1", &[_]i64{ 2, 9 } },
        // UNION and EXCEPT associate left: (a − b) ∪ b, (a ∪all b) − a.
        .{ "SELECT id FROM a EXCEPT SELECT id FROM b UNION SELECT id FROM b ORDER BY id", &[_]i64{ 1, 2, 3, 4 } },
        .{ "SELECT id FROM a UNION ALL SELECT id FROM b EXCEPT SELECT id FROM a", &[_]i64{4} },
        // One copy of each distinct row; NULLs compare equal, as in SELECT DISTINCT.
        .{ "SELECT COUNT(*) FROM (SELECT k FROM d INTERSECT SELECT k FROM d) x", &[_]i64{4} },
        .{ "SELECT COUNT(*) FROM (SELECT v FROM c INTERSECT SELECT v FROM c WHERE v IS NULL) x", &[_]i64{1} },
        .{ "SELECT v FROM c EXCEPT SELECT v FROM c WHERE v IS NULL", &[_]i64{5} },
        .{ "SELECT COUNT(*) FROM (SELECT k FROM d EXCEPT SELECT id FROM a) x", &[_]i64{2} },
        // Every column decides which rows match.
        .{ "SELECT id FROM (SELECT id % 2 AS m, id FROM a INTERSECT SELECT 0, id FROM b) x ORDER BY id", &[_]i64{2} },
        .{ "SELECT id FROM a INTERSECT SELECT CAST(id AS INT) FROM b", &[_]i64{2} },
        .{ "SELECT id FROM a EXCEPT SELECT id FROM b ORDER BY id DESC LIMIT 1", &[_]i64{3} },
        .{ "(SELECT id FROM a) INTERSECT (SELECT id FROM b)", &[_]i64{2} },
        .{ "SELECT id FROM a EXCEPT (SELECT id FROM b ORDER BY id LIMIT 1) ORDER BY id", &[_]i64{ 1, 3 } },
        .{ "SELECT id FROM a WHERE id IN (SELECT id FROM a EXCEPT SELECT id FROM b) ORDER BY id", &[_]i64{ 1, 3 } },
        .{ "WITH x AS (SELECT id FROM a INTERSECT SELECT id FROM b) SELECT id * 10 FROM x", &[_]i64{20} },
        .{ "SELECT id FROM a WHERE id > 10 INTERSECT SELECT id FROM b", &[_]i64{} },
        .{ "SELECT id FROM a EXCEPT SELECT id FROM b WHERE id > 10 ORDER BY id", &[_]i64{ 1, 2, 3 } },
        // Same arms, different operators: shared-subplan detection keeps them apart.
        .{ "WITH u AS (SELECT id FROM a UNION SELECT id FROM b), i AS (SELECT id FROM a INTERSECT SELECT id FROM b) SELECT COUNT(*) FROM u UNION ALL SELECT COUNT(*) FROM i", &[_]i64{ 4, 1 } },
    };
    inline for (cases) |c| {
        const got = try collectBigints(allocator, db, c[0]);
        defer allocator.free(got);
        std.testing.expectEqualSlices(i64, c[1], got) catch |err| {
            std.debug.print("query: {s}\n", .{c[0]});
            return err;
        };
    }

    var q = try helpers.runSql(allocator, db, "SELECT id, id * 2 AS twice FROM a EXCEPT SELECT id, id * 2 FROM b");
    defer q.deinit();
    try std.testing.expectEqual(@as(usize, 2), q.outputSchema().len);
    try std.testing.expectEqualStrings("twice", q.outputSchema()[1].name);
}

test "INTERSECT / EXCEPT: the ALL forms are rejected" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    inline for (.{ "SELECT id FROM a INTERSECT ALL SELECT id FROM b", "SELECT id FROM a EXCEPT ALL SELECT id FROM b" }) |sql| {
        try std.testing.expectError(error.SqlSetOpAllUnsupported, thindb.sql.parse(arena.allocator(), sql));
    }
}
