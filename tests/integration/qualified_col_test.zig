//! Qualified column references — `t.col` syntax and `FROM t AS alias`.
//!
//! With an alias, the scan's output schema is rewritten so columns
//! are exposed as `alias.colname`. Bare `col` still resolves via a
//! suffix-match fallback (any column ending in `.col`). Without an
//! alias, `t.col` resolves via a prefix-strip fallback. Together
//! that means everyday queries keep working while self-joins can
//! disambiguate same-named columns from two scans of the same table.
//! A bare name beside a select alias that shares it reads the FROM
//! column, bare or qualified, as MySQL binds it.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSql = helpers.runSql;
const collectBigints = helpers.collectBigints;

test "qualified pruning uses the same column resolution as row evaluation" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    const table = try db.openTable("t", .{});
    try exec(allocator, db, "INSERT INTO t (id, qty) VALUES (101, 110), (102, 120), (103, 130)");
    try table.flush();

    inline for (.{ "id", "t.id", "main.t.ID" }) |column| {
        const source = try thindb.exec.scan(allocator, table);
        const scan = thindb.exec.queryAs(thindb.exec.Scan, source).?;
        var query = try source.filter(.{ .leaf = .{ .col = column, .op = .eq, .val = .{ .bigint = 2 } } });
        defer query.deinit();
        var rows: usize = 0;
        while (try query.next()) |batch| {
            for (0..batch.row_count) |i| {
                try std.testing.expectEqual(@as(i64, 2), batch.values[0].data.bigint[i]);
                rows += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 1), rows);
        try std.testing.expect(scan.seg_skip != null);
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(bool, scan.seg_skip.?, &.{true}));
    }
}

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO t (id, qty) VALUES (1, 10), (2, 20), (3, 30)",
    );
    const tt = try db.openTable("t", .{});
    try tt.flush();
    return db;
}

test "qualified pruning follows renamed columns and stops at replaced values" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    const table = try db.openTable("t", .{});

    inline for (.{ false, true }) |project| {
        const source = try thindb.exec.scan(allocator, table);
        const renamed = if (project)
            try source.projectNamed(&.{"qty"}, &.{"id"})
        else
            try source.compute(&.{.{ .name = "id", .expr = .{ .col_ref = "qty" } }});
        var query = try renamed.filter(.{ .leaf = .{ .col = "t.id", .op = .eq, .val = .{ .int = 20 } } });
        defer query.deinit();
        var rows: usize = 0;
        while (try query.next()) |batch| {
            for (0..batch.row_count) |i| {
                try std.testing.expectEqual(@as(i32, 20), batch.values[0].data.int[i]);
                rows += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 1), rows);
    }

    const source = try thindb.exec.scan(allocator, table);
    const replaced = try source.compute(&.{.{ .name = "id", .expr = .{ .lit = .{ .bigint = 999 } } }});
    var query = try replaced.filter(.{ .leaf = .{ .col = "t.id", .op = .eq, .val = .{ .bigint = 999 } } });
    defer query.deinit();
    var rows: usize = 0;
    while (try query.next()) |batch| {
        for (0..batch.row_count) |i| {
            try std.testing.expectEqual(@as(i64, 999), batch.values[0].data.bigint[i]);
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), rows);
}

test "qualified pruning rejects predicates for a sibling alias" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    const table = try db.openTable("t", .{});
    const source = try thindb.exec.scan(allocator, table);
    var query = try thindb.exec.AliasRename.create(allocator, source, "a");
    defer query.deinit();
    try std.testing.expectError(error.ColumnNotFound, query.addPrune(.{ .col = "b.id", .op = .eq, .val = .{ .bigint = 999 } }));
    var rows: usize = 0;
    while (try query.next()) |batch| rows += batch.row_count;
    try std.testing.expectEqual(@as(usize, 3), rows);
}

test "qualified pruning respects limits windows and aggregate outputs" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    const table = try db.openTable("t", .{});
    try exec(allocator, db, "INSERT INTO t (id, qty) VALUES (101, 110), (102, 120), (103, 130)");
    try table.flush();
    inline for (.{ false, true }) |top| {
        const source = try thindb.exec.scan(allocator, table);
        const limited = if (top) try source.topN(&.{.{ .col = "id" }}, 3, 0) else try source.limit(3);
        var query = try limited.filter(.{ .leaf = .{ .col = "t.id", .op = .eq, .val = .{ .bigint = 101 } } });
        defer query.deinit();
        try std.testing.expect((try query.next()) == null);
    }
    {
        const source = try thindb.exec.scan(allocator, table);
        const window = try source.window(
            &.{.{ .partition_by = &.{}, .order_by = &.{.{ .col = "id" }}, .frame = thindb.ir.Frame.default_with_order }},
            &.{.{ .spec_idx = 0, .func = .row_number, .args = &.{}, .ignore_nulls = false, .output_name = "rn" }},
            1,
        );
        var query = try window.filter(.{ .leaf = .{ .col = "t.id", .op = .eq, .val = .{ .bigint = 101 } } });
        defer query.deinit();
        const batch = (try query.next()).?;
        try std.testing.expectEqual(@as(usize, 1), batch.row_count);
        try std.testing.expectEqual(@as(i64, 4), batch.values[2].data.bigint[0]);
        try std.testing.expect((try query.next()) == null);
    }
    {
        const source = try thindb.exec.scan(allocator, table);
        const aggregate = try source.groupBy(&.{}, &.{.{ .func = .sum, .col = "id", .as = "id" }});
        var query = try aggregate.filter(.{ .leaf = .{ .col = "t.id", .op = .eq, .val = .{ .bigint = 312 } } });
        defer query.deinit();
        const batch = (try query.next()).?;
        try std.testing.expectEqual(@as(usize, 1), batch.row_count);
        try std.testing.expectEqual(@as(i64, 312), batch.values[0].data.bigint[0]);
        try std.testing.expect((try query.next()) == null);
    }
}

test "qualified col: t.col against unaliased scan resolves via prefix strip" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
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

    const ids = try collectBigints(
        allocator,
        db,
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

    const ids = try collectBigints(
        allocator,
        db,
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

    const ids = try collectBigints(
        allocator,
        db,
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

    try exec(
        allocator,
        db,
        "CREATE TABLE emp (id BIGINT PRIMARY KEY, manager_id BIGINT NOT NULL, name VARCHAR(16) NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO emp (id, manager_id, name) VALUES (1, 0, 'alice'), (2, 1, 'bob'), (3, 1, 'carol'), (4, 2, 'dave')",
    );
    const tt = try db.openTable("emp", .{});
    try tt.flush();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT e.id FROM emp AS e JOIN emp AS m ON e.manager_id = m.id ORDER BY e.id ASC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3, 4 }, ids);
}

test "qualified join projection ignores sibling CTE wildcards and keeps nested join keys" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE lookup (id BIGINT PRIMARY KEY, qty INT NOT NULL, payload STRING)");
    try exec(allocator, db, "INSERT INTO lookup VALUES (1, 100, 'unused wide payload'), (3, 300, 'also unused')");
    const lookup = try db.openTable("lookup", .{});
    try lookup.flush();

    inline for (.{ 1, 4 }) |dop| {
        db.config.max_dop = dop;
        var result = try runSql(allocator, db,
            \\WITH source AS (SELECT * FROM t)
            \\SELECT s.id, a.qty AS qa, b.qty AS qb FROM source s
            \\LEFT JOIN lookup a ON a.id = s.id
            \\LEFT JOIN lookup b ON b.id = a.id
        );
        defer result.deinit();
        const root = thindb.exec.queryAs(thindb.exec.mat_stage.StagedRoot, result.cq.query).?;
        const projection = thindb.exec.queryAs(thindb.exec.Project, root.inner).?;
        const outer = thindb.exec.queryAs(thindb.exec.Join, projection.upstream).?;
        const inner = thindb.exec.queryAs(thindb.exec.Join, outer.left).?;
        for ([_]thindb.exec.Query{ inner.right, outer.right }) |build| {
            for (build.outputSchema()) |column| {
                try std.testing.expect(!std.mem.endsWith(u8, column.name, ".payload"));
            }
        }
        var seen = [_]bool{false} ** 3;
        while (try result.next()) |batch| {
            for (0..batch.row_count) |row| {
                const id = batch.values[0].data.bigint[row];
                try std.testing.expect(id >= 1 and id <= 3);
                const index: usize = @intCast(id - 1);
                try std.testing.expect(!seen[index]);
                seen[index] = true;
                for (batch.values[1..3]) |value| {
                    try std.testing.expectEqual(id != 2, thindb.storage.column.isValidBit(value.nulls, row));
                    if (id != 2) try std.testing.expectEqual(@as(i32, @intCast(id * 100)), value.data.int[row]);
                }
            }
        }
        try std.testing.expectEqualSlices(bool, &.{ true, true, true }, &seen);
    }
}

test "qualified join projection preserves wildcards predicates aggregates and alias scopes" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    const cases = .{
        .{ .sql = "WITH s AS (SELECT * FROM t) SELECT b.* FROM s a LEFT JOIN t b ON a.id = b.id", .columns = 2, .rows = 3 },
        .{ .sql = "WITH s AS (SELECT * FROM t) SELECT a.id FROM s a LEFT JOIN t b ON a.id = b.id WHERE b.qty >= 20", .columns = 1, .rows = 2 },
        .{ .sql = "WITH s AS (SELECT * FROM t) SELECT a.id FROM s a LEFT JOIN t b ON a.id = b.id AND b.qty >= 20", .columns = 1, .rows = 3 },
        .{ .sql = "WITH s AS (SELECT * FROM t) SELECT b.qty, COUNT(*) FROM s a LEFT JOIN t b ON a.id = b.id GROUP BY b.qty", .columns = 2, .rows = 3 },
        .{ .sql = "WITH s AS (SELECT * FROM t) SELECT x.renamed FROM (SELECT b.qty AS renamed FROM s a LEFT JOIN t b ON a.id = b.id) x", .columns = 1, .rows = 3 },
        .{ .sql = "WITH s AS (SELECT * FROM t) SELECT a.id FROM s a FULL JOIN t b ON a.id = b.id WHERE b.qty >= 20", .columns = 1, .rows = 2 },
    };
    inline for (cases) |case| {
        var result = try runSql(allocator, db, case.sql);
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, case.columns), result.outputSchema().len);
        var rows: usize = 0;
        while (try result.next()) |batch| rows += batch.row_count;
        try std.testing.expectEqual(@as(usize, case.rows), rows);
    }
}

fn setupOrders(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try setup(allocator, io, dir);
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE o (oid BIGINT PRIMARY KEY, tid BIGINT NOT NULL, amount INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO o (oid, tid, amount) VALUES (10, 1, 5), (11, 1, 7), (12, 3, 9), (13, 4, 1)");
    const o = try db.openTable("o", .{});
    try o.flush();
    return db;
}

test "unqualified ON columns resolve to the join input that exposes them" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupOrders(allocator, std.testing.io, tmp.dir);
    defer db.close();

    // Each unqualified ON must return what its qualified spelling returns.
    const cases = .{
        .{ "SELECT oid FROM t JOIN o ON id = tid ORDER BY oid", "SELECT oid FROM t JOIN o ON t.id = o.tid ORDER BY oid", &[_]i64{ 10, 11, 12 } },
        .{ "SELECT oid FROM t a JOIN o b ON id = b.tid ORDER BY oid", "SELECT oid FROM t a JOIN o b ON a.id = b.tid ORDER BY oid", &[_]i64{ 10, 11, 12 } },
        .{ "SELECT oid FROM o JOIN t ON id = tid ORDER BY oid", "SELECT oid FROM o JOIN t ON t.id = o.tid ORDER BY oid", &[_]i64{ 10, 11, 12 } },
        .{ "SELECT s.oid FROM (SELECT id AS k FROM t) d JOIN o s ON k = s.tid ORDER BY s.oid", "SELECT s.oid FROM (SELECT id AS k FROM t) d JOIN o s ON d.k = s.tid ORDER BY s.oid", &[_]i64{ 10, 11, 12 } },
        .{ "SELECT oid FROM o JOIN (SELECT id AS k FROM t) s ON tid = k ORDER BY oid", "SELECT oid FROM o JOIN (SELECT id AS k FROM t) s ON o.tid = s.k ORDER BY oid", &[_]i64{ 10, 11, 12 } },
        .{ "SELECT oid FROM t JOIN o ON id = tid AND amount > 5 ORDER BY oid", "SELECT oid FROM t JOIN o ON t.id = o.tid AND o.amount > 5 ORDER BY oid", &[_]i64{ 11, 12 } },
        .{ "SELECT oid FROM t JOIN o ON tid = id + 0 ORDER BY oid", "SELECT oid FROM t JOIN o ON o.tid = t.id + 0 ORDER BY oid", &[_]i64{ 10, 11, 12 } },
        .{ "WITH c AS (SELECT id, qty FROM t WHERE qty >= 20) SELECT oid FROM c JOIN o ON id = tid ORDER BY oid", "WITH c AS (SELECT id, qty FROM t WHERE qty >= 20) SELECT oid FROM c JOIN o ON c.id = o.tid ORDER BY oid", &[_]i64{12} },
        .{ "SELECT oid FROM t JOIN o ON id = tid JOIN (SELECT id AS k2 FROM t) x ON k2 = tid ORDER BY oid", "SELECT oid FROM t JOIN o ON t.id = o.tid JOIN (SELECT id AS k2 FROM t) x ON x.k2 = o.tid ORDER BY oid", &[_]i64{ 10, 11, 12 } },
        .{ "SELECT id FROM t LEFT JOIN o ON id = tid AND amount > 6 ORDER BY id", "SELECT id FROM t LEFT JOIN o ON t.id = o.tid AND o.amount > 6 ORDER BY id", &[_]i64{ 1, 2, 3 } },
    };
    inline for (cases) |case| {
        const unqualified = try helpers.collectBigintsCtx(allocator, db, case[0]);
        defer allocator.free(unqualified);
        const qualified = try helpers.collectBigintsCtx(allocator, db, case[1]);
        defer allocator.free(qualified);
        try std.testing.expectEqualSlices(i64, case[2], qualified);
        try std.testing.expectEqualSlices(i64, qualified, unqualified);
    }
}

test "unqualified ON column exposed by both inputs is ambiguous" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupOrders(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT a.id FROM t a JOIN t b ON id = b.qty", error.SqlOnColumnAmbiguous },
        .{ "SELECT oid FROM o JOIN (SELECT oid AS id, tid FROM o) d ON tid = d.id", error.SqlOnColumnAmbiguous },
        .{ "SELECT oid FROM t JOIN o ON nosuch = tid", error.SqlOnRefsUnknownTable },
    };
    inline for (cases) |case| {
        try std.testing.expectError(case[1], helpers.runSqlCtx(allocator, db, case[0]));
    }
}

test "qualified col: aliased col in ORDER BY" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const ids = try collectBigints(
        allocator,
        db,
        "SELECT a.id FROM t AS a ORDER BY a.qty DESC",
    );
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 3, 2, 1 }, ids);
}

test "an unaliased join input is qualified by its own name" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupOrders(allocator, std.testing.io, tmp.dir);
    defer db.close();
    // `u` shares both of `t`'s column names.
    try exec(allocator, db, "CREATE TABLE u (id BIGINT PRIMARY KEY, qty BIGINT NOT NULL)");
    try exec(allocator, db, "INSERT INTO u (id, qty) VALUES (1, 100), (2, 200), (3, 300)");

    const cases = .{
        .{ "SELECT u.qty FROM t JOIN u ON t.id = u.id ORDER BY u.id", &[_]i64{ 100, 200, 300 } },
        .{ "SELECT U.qty FROM t JOIN U ON t.id = U.id ORDER BY U.id", &[_]i64{ 100, 200, 300 } },
        .{ "SELECT SUM(u.qty) FROM t JOIN u ON t.id = u.id", &[_]i64{600} },
        .{ "SELECT u.qty FROM t JOIN u ON t.id = u.id WHERE u.qty > 150 ORDER BY u.qty", &[_]i64{ 200, 300 } },
        .{ "SELECT u.qty FROM t LEFT JOIN u ON t.id = u.id ORDER BY t.id", &[_]i64{ 100, 200, 300 } },
        .{ "SELECT u.qty FROM t CROSS JOIN u WHERE t.id = 1 ORDER BY u.qty", &[_]i64{ 100, 200, 300 } },
        .{ "SELECT u.id + u.qty FROM t JOIN u ON t.qty * 10 = u.qty ORDER BY u.id", &[_]i64{ 101, 202, 303 } },
        .{ "WITH c1 AS (SELECT id, qty FROM t), c2 AS (SELECT id, qty FROM u) SELECT c2.qty FROM c1 JOIN c2 ON c1.id = c2.id ORDER BY c2.qty", &[_]i64{ 100, 200, 300 } },
        .{ "WITH c2 AS (SELECT id, qty FROM u) SELECT c2.qty FROM t JOIN c2 ON t.id = c2.id ORDER BY c2.qty", &[_]i64{ 100, 200, 300 } },
        .{ "SELECT u.qty, o.oid FROM t JOIN o ON t.id = o.tid JOIN u ON u.id = t.id ORDER BY o.oid", &[_]i64{ 100, 100, 300 } },
        // `t.*` names `t`'s columns only.
        .{ "SELECT t.* FROM t JOIN o ON t.id = o.tid ORDER BY o.oid", &[_]i64{ 1, 1, 3 } },
        .{ "SELECT u.*, t.qty FROM t JOIN u ON t.id = u.id ORDER BY u.id", &[_]i64{ 1, 2, 3 } },
        .{ "WITH c AS (SELECT id FROM t) SELECT c.*, o.oid FROM c JOIN o ON c.id = o.tid ORDER BY o.oid", &[_]i64{ 1, 1, 3 } },
        // A lone unaliased source's `name.*` is `*`.
        .{ "WITH c AS (SELECT id, qty FROM t) SELECT c.* FROM c ORDER BY c.id", &[_]i64{ 1, 2, 3 } },
        .{ "WITH c AS (SELECT id, qty FROM t) SELECT c.*, c.id AS k FROM c WHERE c.qty > 15 ORDER BY c.id", &[_]i64{ 2, 3 } },
        .{ "WITH c AS (SELECT id, SUM(qty) AS s FROM t GROUP BY id) SELECT c.* FROM c ORDER BY c.id", &[_]i64{ 1, 2, 3 } },
        .{ "WITH c AS (SELECT id FROM t), d AS (SELECT * FROM c) SELECT d.* FROM d ORDER BY d.id", &[_]i64{ 1, 2, 3 } },
    };
    inline for (cases) |case| {
        const got = try collectBigints(allocator, db, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, case[1], got);
    }

    // A bare column both inputs expose names neither.
    try std.testing.expectError(error.ColumnNotFound, helpers.runSql(allocator, db, "SELECT qty FROM t JOIN u ON t.id = u.id"));
}

test "qualified refs in a single-table block name its one table" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    // `m` stays in the memtable: its aggregates run the non-segment lanes.
    try exec(allocator, db, "CREATE TABLE m (id BIGINT PRIMARY KEY, x DOUBLE, s VARCHAR(20))");
    try exec(allocator, db, "INSERT INTO m (id, x, s) VALUES (1, 1.5, 'a'), (2, 2.5, 'b'), (3, 3.5, 'a')");

    // Each qualified spelling must return what its bare spelling returns.
    const cases = .{
        .{ "SELECT a.id, SUM(a.qty) FROM t a GROUP BY a.id ORDER BY a.id", "SELECT id, SUM(qty) FROM t GROUP BY id ORDER BY id", &[_]i64{ 1, 2, 3 } },
        .{ "SELECT t.id, COUNT(*) FROM t GROUP BY t.id HAVING SUM(t.qty) > 15 ORDER BY t.id", "SELECT id, COUNT(*) FROM t GROUP BY id HAVING SUM(qty) > 15 ORDER BY id", &[_]i64{ 2, 3 } },
        .{ "SELECT SUM(a.id) FROM t a WHERE a.qty > 15", "SELECT SUM(id) FROM t WHERE qty > 15", &[_]i64{5} },
        .{ "SELECT SUM(m.id) FROM m", "SELECT SUM(id) FROM m", &[_]i64{6} },
        .{ "SELECT COUNT(*) FROM m a GROUP BY a.s ORDER BY COUNT(*)", "SELECT COUNT(*) FROM m GROUP BY s ORDER BY COUNT(*)", &[_]i64{ 1, 2 } },
        .{ "SELECT COUNT(DISTINCT a.s) FROM m a WHERE a.x > 1", "SELECT COUNT(DISTINCT s) FROM m WHERE x > 1", &[_]i64{2} },
        .{ "SELECT DISTINCT a.id FROM m a WHERE a.s = 'a' ORDER BY a.id", "SELECT DISTINCT id FROM m WHERE s = 'a' ORDER BY id", &[_]i64{ 1, 3 } },
        .{ "SELECT COUNT(*) FROM (SELECT DISTINCT a.s FROM m a) d", "SELECT COUNT(*) FROM (SELECT DISTINCT s FROM m) d", &[_]i64{2} },
        .{ "SELECT m.id FROM m ORDER BY m.x DESC LIMIT 1", "SELECT id FROM m ORDER BY x DESC LIMIT 1", &[_]i64{3} },
    };
    inline for (cases) |case| {
        const qualified = try collectBigints(allocator, db, case[0]);
        defer allocator.free(qualified);
        const bare = try collectBigints(allocator, db, case[1]);
        defer allocator.free(bare);
        try std.testing.expectEqualSlices(i64, case[2], bare);
        try std.testing.expectEqualSlices(i64, bare, qualified);
    }
}

test "a schema- or database-qualified column resolves by its table name" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupOrders(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT main.t.id FROM t ORDER BY main.t.id", &[_]i64{ 1, 2, 3 } },
        .{ "SELECT public.t.id FROM public.t WHERE public.t.qty > 15 ORDER BY 1", &[_]i64{ 2, 3 } },
        .{ "SELECT SUM(main.t.qty) FROM t", &[_]i64{60} },
        .{ "SELECT main.t.id FROM t GROUP BY main.t.id HAVING SUM(main.t.qty) > 15 ORDER BY main.t.id", &[_]i64{ 2, 3 } },
        .{ "SELECT main.o.oid FROM t JOIN o ON main.t.id = main.o.tid ORDER BY main.o.oid", &[_]i64{ 10, 11, 12 } },
        .{ "SELECT main.t.id + main.t.qty FROM t ORDER BY 1", &[_]i64{ 11, 22, 33 } },
        .{ "SELECT main.t.* FROM t ORDER BY id", &[_]i64{ 1, 2, 3 } },
        .{ "SELECT main.t.* FROM t JOIN o ON t.id = o.tid ORDER BY o.oid", &[_]i64{ 1, 1, 3 } },
    };
    inline for (cases) |case| {
        const got = try collectBigints(allocator, db, case[0]);
        defer allocator.free(got);
        std.testing.expectEqualSlices(i64, case[1], got) catch |err| {
            std.debug.print("query: {s}\n", .{case[0]});
            return err;
        };
    }
    try helpers.expectRunError(allocator, db, "SELECT main.t.nope FROM t", error.ColumnNotFound);
    try helpers.expectRunError(allocator, db, "SELECT a.main.t.id FROM t", error.SqlExpectedIdent);
}

fn setupShadowed(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE dn (n INT, m INT)");
    try exec(allocator, db, "INSERT INTO dn (n, m) VALUES (5, 7), (6, 8), (7, 9)");
    try exec(allocator, db, "CREATE TABLE dk (k INT, n INT)");
    try exec(allocator, db, "INSERT INTO dk (k, n) VALUES (5, 50), (6, 60), (7, 70)");
    const t = try db.openTable("dn", .{});
    try t.flush();
    return db;
}

fn expectNamedCells(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, names: []const []const u8, cells: []const ?i64) !void {
    errdefer std.debug.print("query: {s}\n", .{sql});
    var q = try helpers.runSqlCtx(allocator, db, sql);
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqual(names.len, schema.len);
    for (names, schema) |name, col| try std.testing.expectEqualStrings(name, col.name);
    const got = try helpers.collectIntCells(allocator, &q);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(?i64, cells, got);
}

test "a select alias leaves the FROM column it names to the rest of the query (issue #321)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setupShadowed(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT 0 AS n, n FROM dn ORDER BY 2", &[_][]const u8{ "n", "n_1" }, &[_]?i64{ 0, 5, 0, 6, 0, 7 } },
        .{ "SELECT n + 1 AS n, n FROM dn ORDER BY 2", &[_][]const u8{ "n", "n_1" }, &[_]?i64{ 6, 5, 7, 6, 8, 7 } },
        .{ "SELECT n, n + 1 AS n FROM dn ORDER BY 1", &[_][]const u8{ "n", "n_1" }, &[_]?i64{ 5, 6, 6, 7, 7, 8 } },
        .{ "SELECT CASE WHEN n > 5 THEN 1 ELSE 0 END AS n, n FROM dn WHERE n > 5 ORDER BY 2", &[_][]const u8{ "n", "n_1" }, &[_]?i64{ 1, 6, 1, 7 } },
        // A bare ORDER BY name is the alias; an expression reads the column.
        .{ "SELECT -n AS n, n FROM dn ORDER BY n", &[_][]const u8{ "n", "n_1" }, &[_]?i64{ -7, 7, -6, 6, -5, 5 } },
        .{ "SELECT -n AS n FROM dn ORDER BY n + 0", &[_][]const u8{"n"}, &[_]?i64{ -5, -6, -7 } },
        // An alias no FROM column has still names the item for later items.
        .{ "SELECT m + 1 AS b, b * 2 FROM dn ORDER BY 1", &[_][]const u8{ "b", "b * 2" }, &[_]?i64{ 8, 16, 9, 18, 10, 20 } },
        // Beside `*` the item still takes the slot of the column it names.
        .{ "SELECT *, n * 2 AS n FROM dn ORDER BY m", &[_][]const u8{ "n", "m" }, &[_]?i64{ 10, 7, 12, 8, 14, 9 } },
        .{ "SELECT *, SUM(n) OVER () AS n FROM dn ORDER BY m", &[_][]const u8{ "n", "m" }, &[_]?i64{ 18, 7, 18, 8, 18, 9 } },
        .{ "SELECT *, n * 2 AS n FROM (SELECT * FROM dn) d ORDER BY m", &[_][]const u8{ "n", "m" }, &[_]?i64{ 10, 7, 12, 8, 14, 9 } },
        .{ "SELECT n * 2 AS n, dn.* FROM dn ORDER BY m", &[_][]const u8{ "n", "n_1", "m" }, &[_]?i64{ 10, 5, 7, 12, 6, 8, 14, 7, 9 } },
        // A qualified reference to the same column names its repeat too.
        .{ "SELECT m * 2 AS m, dn.m FROM dn ORDER BY 2", &[_][]const u8{ "m", "m_1" }, &[_]?i64{ 14, 7, 16, 8, 18, 9 } },
        .{ "SELECT n, a.n FROM dn a ORDER BY 1", &[_][]const u8{ "n", "n_1" }, &[_]?i64{ 5, 5, 6, 6, 7, 7 } },
        .{ "SELECT * FROM (SELECT 0 AS n, n FROM dn) d ORDER BY 2", &[_][]const u8{ "n", "n_1" }, &[_]?i64{ 0, 5, 0, 6, 0, 7 } },
        .{ "WITH c AS (SELECT n * 10 AS n, n AS orig FROM dn) SELECT n + 1 AS n, n, orig FROM c ORDER BY 3", &[_][]const u8{ "n", "n_1", "orig" }, &[_]?i64{ 51, 50, 5, 61, 60, 6, 71, 70, 7 } },
        .{ "SELECT a.m + 100 AS m, m FROM dn a JOIN dk b ON a.n = b.k ORDER BY 2", &[_][]const u8{ "m", "m_1" }, &[_]?i64{ 107, 7, 108, 8, 109, 9 } },
        .{ "SELECT a.m + 100 AS k, k FROM dn a JOIN dk b ON a.n = b.k ORDER BY 2", &[_][]const u8{ "k", "k_1" }, &[_]?i64{ 107, 5, 108, 6, 109, 7 } },
    };
    inline for (cases) |case| try expectNamedCells(allocator, db, case[0], case[1], case[2]);
}

test "an aggregate or window alias leaves the FROM column it names to the rest of the query (issue #321)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setupShadowed(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT SUM(n) AS n, n FROM dn GROUP BY n ORDER BY 2", &[_][]const u8{ "n", "n_1" }, &[_]?i64{ 5, 5, 6, 6, 7, 7 } },
        .{ "SELECT SUM(m) AS n, n FROM dn GROUP BY n ORDER BY 2", &[_][]const u8{ "n", "n_1" }, &[_]?i64{ 7, 5, 8, 6, 9, 7 } },
        .{ "SELECT n + 1 AS n, COUNT(*) FROM dn GROUP BY n ORDER BY 1", &[_][]const u8{ "n", "COUNT(*)" }, &[_]?i64{ 6, 1, 7, 1, 8, 1 } },
        .{ "SELECT m, SUM(n) AS n FROM dn GROUP BY m HAVING n > 5 ORDER BY 1", &[_][]const u8{ "m", "n" }, &[_]?i64{ 8, 6, 9, 7 } },
        .{ "SELECT m, SUM(n) AS n FROM dn GROUP BY m ORDER BY n DESC", &[_][]const u8{ "m", "n" }, &[_]?i64{ 9, 7, 8, 6, 7, 5 } },
        .{ "SELECT DISTINCT n % 2 AS n, n FROM dn ORDER BY 2", &[_][]const u8{ "n", "n_1" }, &[_]?i64{ 1, 5, 0, 6, 1, 7 } },
        .{ "SELECT ROW_NUMBER() OVER (ORDER BY n DESC) AS n, n FROM dn ORDER BY 2", &[_][]const u8{ "n", "n_1" }, &[_]?i64{ 3, 5, 2, 6, 1, 7 } },
        .{ "SELECT n + 1 AS n, SUM(n) OVER (ORDER BY n) AS s FROM dn ORDER BY 1", &[_][]const u8{ "n", "s" }, &[_]?i64{ 6, 5, 7, 11, 8, 18 } },
        .{ "SELECT n * 2 AS n, SUM(n) OVER (PARTITION BY n) AS s FROM dn ORDER BY 1", &[_][]const u8{ "n", "s" }, &[_]?i64{ 10, 5, 12, 6, 14, 7 } },
        // QUALIFY filters the finished row, where a bare alias is the item.
        .{ "SELECT ROW_NUMBER() OVER (ORDER BY m DESC) AS n, m FROM dn QUALIFY n = 1", &[_][]const u8{ "n", "m" }, &[_]?i64{ 1, 9 } },
        .{ "SELECT n * 2 AS n, ROW_NUMBER() OVER (ORDER BY m) AS r FROM dn QUALIFY dn.n > 6", &[_][]const u8{ "n", "r" }, &[_]?i64{ 14, 3 } },
    };
    inline for (cases) |case| try expectNamedCells(allocator, db, case[0], case[1], case[2]);
}

test "a GROUP BY name reads the FROM column before a select alias that shares it (issue #328)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setupShadowed(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT n % 2 AS n, COUNT(*) AS c FROM dn GROUP BY n ORDER BY 1, 2", &[_][]const u8{ "n", "c" }, &[_]?i64{ 0, 1, 1, 1, 1, 1 } },
        // HAVING reads the grouped column; a bare ORDER BY name reads the alias.
        .{ "SELECT n % 2 AS n, SUM(m) AS s FROM dn GROUP BY n HAVING n > 5 ORDER BY 1", &[_][]const u8{ "n", "s" }, &[_]?i64{ 0, 8, 1, 9 } },
        .{ "SELECT n - 5 AS n, COUNT(*) AS c FROM dn GROUP BY n HAVING n < 7 ORDER BY 1", &[_][]const u8{ "n", "c" }, &[_]?i64{ 0, 1, 1, 1 } },
        .{ "SELECT n % 2 AS n, m FROM dn GROUP BY n, m ORDER BY n, m", &[_][]const u8{ "n", "m" }, &[_]?i64{ 0, 8, 1, 7, 1, 9 } },
        .{ "SELECT SUM(m) AS n, COUNT(*) AS c FROM dn GROUP BY n ORDER BY 1", &[_][]const u8{ "n", "c" }, &[_]?i64{ 7, 1, 8, 1, 9, 1 } },
        // A name no FROM column has, an ordinal and an expression reach the item.
        .{ "SELECT n % 2 AS k, COUNT(*) AS c FROM dn GROUP BY k ORDER BY 1", &[_][]const u8{ "k", "c" }, &[_]?i64{ 0, 1, 1, 2 } },
        .{ "SELECT n % 2 AS n, COUNT(*) AS c FROM dn GROUP BY 1 ORDER BY 1", &[_][]const u8{ "n", "c" }, &[_]?i64{ 0, 1, 1, 2 } },
        .{ "SELECT n % 2 AS n, COUNT(*) AS c FROM dn GROUP BY n % 2 ORDER BY 1", &[_][]const u8{ "n", "c" }, &[_]?i64{ 0, 1, 1, 2 } },
        .{ "SELECT DISTINCT n % 2 AS n FROM dn ORDER BY 1", &[_][]const u8{"n"}, &[_]?i64{ 0, 1 } },
        .{ "SELECT n * 10 AS n, SUM(m) AS s FROM dn GROUP BY n WITH ROLLUP ORDER BY 2", &[_][]const u8{ "n", "s" }, &[_]?i64{ 50, 7, 60, 8, 70, 9, null, 24 } },
        .{ "SELECT n % 2 AS n, GROUPING(n) AS g, COUNT(*) AS c FROM dn GROUP BY n WITH ROLLUP ORDER BY 2, 1", &[_][]const u8{ "n", "g", "c" }, &[_]?i64{ 0, 0, 1, 1, 0, 1, 1, 0, 1, null, 1, 3 } },
        .{ "SELECT n % 2 AS n, COUNT(*) AS c FROM (SELECT n FROM dn) t GROUP BY n ORDER BY 1, 2", &[_][]const u8{ "n", "c" }, &[_]?i64{ 0, 1, 1, 1, 1, 1 } },
        .{ "SELECT a.m % 2 AS m, COUNT(*) AS c FROM dn a JOIN dk b ON a.n = b.k GROUP BY m ORDER BY 1, 2", &[_][]const u8{ "m", "c" }, &[_]?i64{ 0, 1, 1, 1, 1, 1 } },
    };
    inline for (cases) |case| try expectNamedCells(allocator, db, case[0], case[1], case[2]);

    // The column the GROUP BY names leaves another column's alias ungrouped,
    // as MySQL's ONLY_FULL_GROUP_BY rejects it.
    try std.testing.expectError(error.SqlMixedAggAndPlainProjection, helpers.runSqlCtx(allocator, db, "SELECT n AS m, COUNT(*) FROM dn GROUP BY m"));
}

test "SELECT * skips the hidden ORDER BY keys and WHERE operands beside a computed item (issue #330)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setupShadowed(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT *, n * 2 AS z FROM dn ORDER BY m + 0", &[_][]const u8{ "n", "m", "z" }, &[_]?i64{ 5, 7, 10, 6, 8, 12, 7, 9, 14 } },
        .{ "SELECT *, SUM(n) OVER () AS z FROM dn ORDER BY m + 0", &[_][]const u8{ "n", "m", "z" }, &[_]?i64{ 5, 7, 18, 6, 8, 18, 7, 9, 18 } },
        .{ "SELECT *, n * 2 AS z FROM (SELECT * FROM dn) d ORDER BY m + 0", &[_][]const u8{ "n", "m", "z" }, &[_]?i64{ 5, 7, 10, 6, 8, 12, 7, 9, 14 } },
        .{ "SELECT *, n * 2 AS z FROM (SELECT * FROM dn) d WHERE m + 1 > 3 ORDER BY m", &[_][]const u8{ "n", "m", "z" }, &[_]?i64{ 5, 7, 10, 6, 8, 12, 7, 9, 14 } },
        .{ "SELECT *, ROW_NUMBER() OVER (ORDER BY m) AS rn FROM dn WHERE m + 1 > 3 ORDER BY m * 2 DESC", &[_][]const u8{ "n", "m", "rn" }, &[_]?i64{ 7, 9, 3, 6, 8, 2, 5, 7, 1 } },
        .{ "SELECT d.*, n * 2 AS z FROM dn d ORDER BY m + 0", &[_][]const u8{ "n", "m", "z" }, &[_]?i64{ 5, 7, 10, 6, 8, 12, 7, 9, 14 } },
        .{ "WITH c AS (SELECT *, n + 1 AS k FROM dn) SELECT *, k * 2 AS z FROM c WHERE m + 1 > 3 ORDER BY m + 0", &[_][]const u8{ "n", "m", "k", "z" }, &[_]?i64{ 5, 7, 6, 12, 6, 8, 7, 14, 7, 9, 8, 16 } },
    };
    inline for (cases) |case| try expectNamedCells(allocator, db, case[0], case[1], case[2]);
}
