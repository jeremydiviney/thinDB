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
        try std.testing.expectEqual(@as(i128, 312), batch.values[0].data.largeint[0]);
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
