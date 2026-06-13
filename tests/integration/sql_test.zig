//! SQL end-to-end integration tests: parse → compile → execute.
//!
//! Each test seeds a small table, parses a SQL string into an IR Op
//! tree, compiles that tree into an executable Query (via the existing
//! local.compileWithSession path), drains the result, and asserts the
//! output. Same architecture future tooling (EXPLAIN, network server)
//! will use.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;

const schema_t = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "k", .type = .int },
        .{ .name = "qty", .type = .int },
        .{ .name = "tag", .type = .string },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const ok_t = [_][]const u8{"id"};
const opts_t = thindb.TableOptions{ .order_key = &ok_t, .unique = true, .row_group_size = 8 };

fn seedT(db: anytype) !*thindb.Table {
    const t = try db.table("t", schema_t, opts_t);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .k = @as(i32, 100), .qty = @as(i32, 10), .tag = "a" },
        .{ .id = @as(i64, 2), .k = @as(i32, 100), .qty = @as(i32, 20), .tag = "b" },
        .{ .id = @as(i64, 3), .k = @as(i32, 200), .qty = @as(i32, 30), .tag = "a" },
        .{ .id = @as(i64, 4), .k = @as(i32, 200), .qty = @as(i32, 40), .tag = "b" },
        .{ .id = @as(i64, 5), .k = @as(i32, 300), .qty = @as(i32, 50), .tag = "c" },
    });
    try t.flush();
    return t;
}

const schema_big = thindb.TableSchema{
    .columns = &.{.{ .name = "id", .type = .bigint }},
    .order_key = &.{"id"},
    .unique = false,
};
const ok_big = [_][]const u8{"id"};
const opts_big = thindb.TableOptions{ .order_key = &ok_big, .unique = false, .row_group_size = 8 };

/// Seed a single-column `big` table with `rows` distinct bigint ids,
/// flushing every 8 rows so the scan yields many small segment batches.
fn seedBig(db: anytype, rows: i64) !void {
    const t = try db.table("big", schema_big, opts_big);
    var i: i64 = 0;
    while (i < rows) : (i += 1) {
        try t.insert(&.{.{ .id = i }});
        if (@mod(i + 1, 8) == 0) try t.flush();
    }
    try t.flush();
}

test "sql: SELECT DISTINCT dedups single and multi-column projections" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT DISTINCT k FROM t ORDER BY k");
    defer q.deinit();
    var ks: std.ArrayList(i32) = .empty;
    defer ks.deinit(allocator);
    while (try q.next()) |b| {
        for (0..b.row_count) |i| try ks.append(allocator, b.values[0].data.int[i]);
    }
    try std.testing.expectEqualSlices(i32, &[_]i32{ 100, 200, 300 }, ks.items);

    // (k, tag) pairs: (100,a)(100,b)(200,a)(200,b)(300,c) — all distinct,
    // 5 rows survive; single-column dedup above proved collapsing works.
    var q2 = try runSql(allocator, db, "SELECT DISTINCT k, tag FROM t ORDER BY k, tag");
    defer q2.deinit();
    var n: usize = 0;
    while (try q2.next()) |b| n += b.row_count;
    try std.testing.expectEqual(@as(usize, 5), n);
}

test "sql: SELECT DISTINCT with WHERE / LIMIT / expression projections" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT DISTINCT tag FROM t WHERE k < 300 ORDER BY tag");
    defer q.deinit();
    var tags: std.ArrayList([]const u8) = .empty;
    defer {
        for (tags.items) |s| allocator.free(s);
        tags.deinit(allocator);
    }
    while (try q.next()) |b| {
        for (0..b.row_count) |i| {
            try tags.append(allocator, try allocator.dupe(u8, b.values[0].data.string.rowBytes(@intCast(i))));
        }
    }
    try std.testing.expectEqual(@as(usize, 2), tags.items.len);
    try std.testing.expectEqualStrings("a", tags.items[0]);
    try std.testing.expectEqualStrings("b", tags.items[1]);

    var q2 = try runSql(allocator, db, "SELECT DISTINCT k FROM t ORDER BY k LIMIT 2");
    defer q2.deinit();
    var ks: std.ArrayList(i32) = .empty;
    defer ks.deinit(allocator);
    while (try q2.next()) |b| {
        for (0..b.row_count) |i| try ks.append(allocator, b.values[0].data.int[i]);
    }
    try std.testing.expectEqualSlices(i32, &[_]i32{ 100, 200 }, ks.items);

    // Expression projection: k / 100 collapses 5 rows to 3 distinct values.
    var q3 = try runSql(allocator, db, "SELECT DISTINCT k / 100 AS h FROM t ORDER BY h");
    defer q3.deinit();
    var n: usize = 0;
    while (try q3.next()) |b| n += b.row_count;
    try std.testing.expectEqual(@as(usize, 3), n);
}

test "sql: SELECT DISTINCT rejects star, aggregates, GROUP BY, HAVING" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    const cases = [_][]const u8{
        "SELECT DISTINCT * FROM t",
        "SELECT DISTINCT COUNT(*) FROM t",
        "SELECT DISTINCT k FROM t GROUP BY k",
        "SELECT DISTINCT k FROM t HAVING k > 1",
    };
    for (cases) |sql| {
        const res = runSql(allocator, db, sql);
        try std.testing.expectError(thindb.sql.ParseError.SqlInvalidProjection, res);
    }
}

test "sql: GROUP BY on the sorted order key streams and aggregates correctly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // order_key = grp (non-unique) with duplicates; a single flush yields
    // one segment globally sorted on grp, so GROUP BY grp routes to the
    // streaming sort-based aggregate (which emits in grp order).
    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "grp", .type = .int },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{"grp"},
        .unique = false,
    };
    const ok = [_][]const u8{"grp"};
    const t = try db.table("gk", schema, .{ .order_key = &ok, .unique = false });
    try t.insert(&.{
        .{ .grp = @as(i32, 2), .v = @as(i32, 10) },
        .{ .grp = @as(i32, 1), .v = @as(i32, 20) },
        .{ .grp = @as(i32, 2), .v = @as(i32, 30) },
        .{ .grp = @as(i32, 3), .v = @as(i32, 40) },
        .{ .grp = @as(i32, 1), .v = @as(i32, 50) },
        .{ .grp = @as(i32, 2), .v = @as(i32, 60) },
    });
    try t.flush();

    var q = try runSql(allocator, db, "SELECT grp, count(*) AS n, sum(v) AS total FROM gk GROUP BY grp");
    defer q.deinit();
    var grps: std.ArrayList(i32) = .empty;
    defer grps.deinit(allocator);
    var counts: std.ArrayList(i64) = .empty;
    defer counts.deinit(allocator);
    var totals: std.ArrayList(i64) = .empty;
    defer totals.deinit(allocator);
    while (try q.next()) |b| {
        for (0..b.row_count) |i| {
            try grps.append(allocator, b.values[0].data.int[i]);
            try counts.append(allocator, b.values[1].data.bigint[i]);
            try totals.append(allocator, b.values[2].data.bigint[i]);
        }
    }
    // Streaming emits in ascending group order.
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2, 3 }, grps.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3, 1 }, counts.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 70, 100, 40 }, totals.items);
}

test "sql: EXPLAIN returns the physical plan as a QUERY PLAN result set" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "EXPLAIN SELECT k, count(*) AS n FROM t GROUP BY k");
    defer q.deinit();
    try std.testing.expectEqualStrings("QUERY PLAN", q.outputSchema()[0].name);

    var plan: std.ArrayList(u8) = .empty;
    defer plan.deinit(allocator);
    while (try q.next()) |b| {
        const sv = b.values[0].data.string;
        for (0..b.row_count) |i| {
            try plan.appendSlice(allocator, sv.rowBytes(i));
            try plan.append(allocator, '\n');
        }
    }
    // The physical plan names the chosen operators.
    try std.testing.expect(std.mem.indexOf(u8, plan.items, "Aggregate") != null);
    try std.testing.expect(std.mem.indexOf(u8, plan.items, "Scan t") != null);
}

test "sql: GROUP BY with a constant key routes to hash, not sort fallback" {
    // Regression guard for the constant-key routing gap (ClickBench Q34):
    // `GROUP BY 1, tag` materializes the literal `1` via Compute. That derived
    // column must report NDV 1 so the router products it as 1 x NDV(tag) and
    // picks HashAggregate; before the fix it read as unknown cardinality and
    // forced the sort fallback (StreamAggregate over a full Sort) even though
    // tag alone trivially fits the budget. Independent of data size.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    {
        var q = try runSql(allocator, db, "EXPLAIN SELECT 1, tag, count(*) AS c FROM t GROUP BY 1, tag ORDER BY c DESC LIMIT 2");
        defer q.deinit();
        var plan: std.ArrayList(u8) = .empty;
        defer plan.deinit(allocator);
        while (try q.next()) |b| {
            const sv = b.values[0].data.string;
            for (0..b.row_count) |i| try plan.appendSlice(allocator, sv.rowBytes(i));
        }
        try std.testing.expect(std.mem.indexOf(u8, plan.items, "HashAggregate") != null);
        try std.testing.expect(std.mem.indexOf(u8, plan.items, "StreamAggregate") == null);
    }

    // And the results are correct: tags a (2) and b (2) outrank c (1).
    {
        var q = try runSql(allocator, db, "SELECT 1, tag, count(*) AS c FROM t GROUP BY 1, tag ORDER BY c DESC LIMIT 2");
        defer q.deinit();
        var rows: usize = 0;
        var saw_a = false;
        var saw_b = false;
        while (try q.next()) |b| {
            const tags = b.values[1].data.string;
            for (0..b.row_count) |i| {
                rows += 1;
                const tag = tags.rowBytes(i);
                const c = b.values[2].data.bigint[i];
                try std.testing.expectEqual(@as(i64, 2), c);
                if (std.mem.eql(u8, tag, "a")) saw_a = true;
                if (std.mem.eql(u8, tag, "b")) saw_b = true;
            }
        }
        try std.testing.expectEqual(@as(usize, 2), rows);
        try std.testing.expect(saw_a and saw_b);
    }
}

test "sql: EXPLAIN ANALYZE aliases EXPLAIN (same static plan)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "EXPLAIN ANALYZE SELECT k, count(*) AS n FROM t GROUP BY k");
    defer q.deinit();
    try std.testing.expectEqualStrings("QUERY PLAN", q.outputSchema()[0].name);
    var plan: std.ArrayList(u8) = .empty;
    defer plan.deinit(allocator);
    while (try q.next()) |b| {
        const sv = b.values[0].data.string;
        for (0..b.row_count) |i| try plan.appendSlice(allocator, sv.rowBytes(i));
    }
    try std.testing.expect(std.mem.indexOf(u8, plan.items, "Aggregate") != null);
    try std.testing.expect(std.mem.indexOf(u8, plan.items, "Scan t") != null);
}

test "sql: EXPLAIN FORMAT=JSON renders the plan as one JSON row" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // MySQL spelling and PG parenthesized spelling must both select JSON.
    for ([_][]const u8{
        "EXPLAIN FORMAT=JSON SELECT k, count(*) AS n FROM t GROUP BY k",
        "EXPLAIN (FORMAT JSON) SELECT k, count(*) AS n FROM t GROUP BY k",
    }) |sql| {
        var q = try runSql(allocator, db, sql);
        defer q.deinit();
        var json: std.ArrayList(u8) = .empty;
        defer json.deinit(allocator);
        var rows: usize = 0;
        while (try q.next()) |b| {
            const sv = b.values[0].data.string;
            for (0..b.row_count) |i| {
                try json.appendSlice(allocator, sv.rowBytes(i));
                rows += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 1), rows);
        try std.testing.expect(json.items[0] == '{');
        try std.testing.expect(std.mem.indexOf(u8, json.items, "\"node\":") != null);
        try std.testing.expect(std.mem.indexOf(u8, json.items, "\"children\":") != null);
        try std.testing.expect(std.mem.indexOf(u8, json.items, "Scan t") != null);
    }
}

test "sql: EXPLAIN result column name follows the wire dialect" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    const cases = .{
        .{ .dialect = thindb.Dialect.mysql, .name = "EXPLAIN" },
        .{ .dialect = thindb.Dialect.postgres, .name = "QUERY PLAN" },
        .{ .dialect = thindb.Dialect.neutral, .name = "QUERY PLAN" },
    };
    inline for (cases) |c| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const root = try thindb.sql.parse(arena.allocator(), "EXPLAIN SELECT k, count(*) FROM t GROUP BY k");
        var cq = try thindb.net.compileWithSession(allocator, db, .{ .dialect = c.dialect }, root);
        defer cq.deinit();
        try std.testing.expectEqualStrings(c.name, cq.outputSchema()[0].name);
    }
}

test "sql: double-quoted identifiers resolve columns (neutral/PG)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT \"id\", \"qty\" FROM t");
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqual(@as(usize, 2), schema.len);
    try std.testing.expectEqualStrings("id", schema[0].name);
    try std.testing.expectEqualStrings("qty", schema[1].name);
    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 5), rows);
}

test "sql: pg_catalog.pg_class lists user tables" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT relname FROM pg_class");
    defer q.deinit();
    var saw_t = false;
    while (try q.next()) |b| {
        const rn = b.values[0].data.string;
        for (0..b.row_count) |i| {
            if (std.mem.eql(u8, rn.rowBytes(i), "t")) saw_t = true;
        }
    }
    try std.testing.expect(saw_t);
}

test "sql: pg_class JOIN pg_namespace resolves a table to its schema" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT c.relname, n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace");
    defer q.deinit();
    var found = false;
    while (try q.next()) |b| {
        const rn = b.values[0].data.string;
        const ns = b.values[1].data.string;
        for (0..b.row_count) |i| {
            if (std.mem.eql(u8, rn.rowBytes(i), "t")) {
                try std.testing.expectEqualStrings("public", ns.rowBytes(i));
                found = true;
            }
        }
    }
    try std.testing.expect(found);
}

test "sql: pg_attribute JOIN pg_class lists a table's columns" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT a.attname FROM pg_attribute a JOIN pg_class c ON a.attrelid = c.oid WHERE c.relname = 't'");
    defer q.deinit();
    var n: usize = 0;
    var saw_id = false;
    var saw_tag = false;
    while (try q.next()) |b| {
        const an = b.values[0].data.string;
        for (0..b.row_count) |i| {
            const name = an.rowBytes(i);
            if (std.mem.eql(u8, name, "id")) saw_id = true;
            if (std.mem.eql(u8, name, "tag")) saw_tag = true;
            n += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 4), n);
    try std.testing.expect(saw_id and saw_tag);
}

test "sql: a bare non-grouped column under aggregation is rejected (strict GROUP BY)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // qty is neither grouped nor aggregated.
    try helpers.expectRunError(allocator, db, "SELECT k, qty FROM t GROUP BY k", thindb.sql.ParseError.SqlMixedAggAndPlainProjection);
    // Implicit aggregation (an aggregate present, no GROUP BY) is just as strict.
    try helpers.expectRunError(allocator, db, "SELECT tag, count(*) FROM t", thindb.sql.ParseError.SqlMixedAggAndPlainProjection);
}

test "sql: FETCH FIRST / OFFSET ROWS (ANSI/PG row limiting)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db); // ids 1..5

    const Case = struct { sql: []const u8, rows: usize, first: i64 };
    const cases = [_]Case{
        .{ .sql = "SELECT id FROM t ORDER BY id FETCH FIRST 2 ROWS ONLY", .rows = 2, .first = 1 },
        .{ .sql = "SELECT id FROM t ORDER BY id OFFSET 3 ROWS FETCH NEXT 2 ROWS ONLY", .rows = 2, .first = 4 },
        .{ .sql = "SELECT id FROM t ORDER BY id OFFSET 4 ROWS", .rows = 1, .first = 5 },
        .{ .sql = "SELECT id FROM t ORDER BY id FETCH FIRST ROW ONLY", .rows = 1, .first = 1 },
    };
    for (cases) |c| {
        var q = try runSql(allocator, db, c.sql);
        defer q.deinit();
        var rows: usize = 0;
        var first: ?i64 = null;
        while (try q.next()) |b| {
            const idv = b.values[0].data.bigint;
            for (0..b.row_count) |i| {
                if (first == null) first = idv[i];
                rows += 1;
            }
        }
        try std.testing.expectEqual(c.rows, rows);
        try std.testing.expectEqual(c.first, first.?);
    }
}

test "sql: FROM-less SELECT evaluates expressions over one row" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // SELECT 1 + 1 — one row, no FROM.
    {
        var q = try runSql(allocator, db, "SELECT 1 + 1 AS s");
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 1), b.row_count);
        try std.testing.expectEqual(@as(i32, 2), b.values[0].data.int[0]);
        try std.testing.expect((try q.next()) == null);
    }
    // SELECT now() — real wall-clock, no FROM.
    {
        var q = try runSql(allocator, db, "SELECT now() AS n");
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expect(b.values[0].data.datetime[0] > 1_500_000_000_000_000);
    }
    // Bare CURRENT_TIMESTAMP / CURRENT_DATE (no parens) resolve too.
    {
        var q = try runSql(allocator, db, "SELECT current_timestamp AS t, current_date AS d");
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expect(b.values[0].data.datetime[0] > 1_500_000_000_000_000);
        try std.testing.expect(b.values[1].data.date[0] > 18262);
    }
    // SELECT with string concat.
    {
        var q = try runSql(allocator, db, "SELECT 'a' || 'b' AS c");
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqualStrings("ab", b.values[0].data.string.rowBytes(0));
    }
}

test "sql: CAST(expr AS type) and PG expr::type" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db); // id=1 has qty=10

    // CAST(int AS bigint)
    {
        var q = try runSql(allocator, db, "SELECT CAST(qty AS bigint) AS b FROM t ORDER BY id LIMIT 1");
        defer q.deinit();
        const r = (try q.next()).?;
        try std.testing.expect(std.meta.activeTag(q.outputSchema()[0].type) == .bigint);
        try std.testing.expectEqual(@as(i64, 10), r.values[0].data.bigint[0]);
    }
    // PG postfix ::double
    {
        var q = try runSql(allocator, db, "SELECT qty::double AS d FROM t ORDER BY id LIMIT 1");
        defer q.deinit();
        const r = (try q.next()).?;
        try std.testing.expectEqual(@as(f64, 10), r.values[0].data.double[0]);
    }
    // CAST(bigint AS text) and CAST('42' AS int)
    {
        var q = try runSql(allocator, db, "SELECT CAST(id AS text) AS s, CAST('42' AS int) AS n FROM t ORDER BY id LIMIT 1");
        defer q.deinit();
        const r = (try q.next()).?;
        try std.testing.expectEqualStrings("1", r.values[0].data.string.rowBytes(0));
        try std.testing.expectEqual(@as(i32, 42), r.values[1].data.int[0]);
    }
    // `::` is not a cast operator on MySQL — it must fail to parse there.
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        if (thindb.sql.parseDialect(arena.allocator(), "SELECT qty::int FROM t", .mysql)) |_| {
            return error.TestUnexpectedSuccess;
        } else |_| {}
    }
    // Extended targets: smallint, boolean, datetime->date.
    {
        var q = try runSql(allocator, db, "SELECT CAST(qty AS smallint) AS s, CAST(1 AS boolean) AS b FROM t ORDER BY id LIMIT 1");
        defer q.deinit();
        const r = (try q.next()).?;
        try std.testing.expect(std.meta.activeTag(q.outputSchema()[0].type) == .smallint);
        try std.testing.expectEqual(@as(i16, 10), r.values[0].data.smallint[0]);
        try std.testing.expectEqual(@as(u8, 1), r.values[1].data.boolean[0]);
    }
    {
        var q = try runSql(allocator, db, "SELECT CAST(now() AS date) AS d");
        defer q.deinit();
        const r = (try q.next()).?;
        try std.testing.expect(r.values[0].data.date[0] > 18262); // after 2020-01-01
    }
}

test "sql: string escapes — PG E'...' and MySQL backslash" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // PG/neutral E'...' processes the \t escape into a tab.
    {
        var q = try runSql(allocator, db, "SELECT E'a\\tb' AS s FROM t LIMIT 1");
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqualStrings("a\tb", b.values[0].data.string.rowBytes(0));
    }
    // MySQL: backslash escapes in ordinary '...'.
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const root = try thindb.sql.parseDialect(arena.allocator(), "SELECT 'a\\tb' AS s FROM t LIMIT 1", .mysql);
        var cq = try thindb.net.compileWithSession(allocator, db, .{ .dialect = .mysql }, root);
        defer cq.deinit();
        const b = (try cq.next()).?;
        try std.testing.expectEqualStrings("a\tb", b.values[0].data.string.rowBytes(0));
    }
    // PG dollar-quoted string literal (raw contents).
    {
        var q = try runSql(allocator, db, "SELECT $$he'llo$$ AS s FROM t LIMIT 1");
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqualStrings("he'llo", b.values[0].data.string.rowBytes(0));
    }
}

test "sql: || is string concat on neutral/PG, logical OR on MySQL" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // neutral: `tag || tag` concatenates (tag 'c' for the k=300 row -> "cc").
    {
        var q = try runSql(allocator, db, "SELECT tag || tag AS s FROM t");
        defer q.deinit();
        var saw_cc = false;
        while (try q.next()) |b| {
            const sv = b.values[0].data.string;
            for (0..b.row_count) |i| {
                if (std.mem.eql(u8, sv.rowBytes(i), "cc")) saw_cc = true;
            }
        }
        try std.testing.expect(saw_cc);
    }

    // MySQL: `||` means OR — k=100 OR k=300 selects ids 1,2,5.
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const root = try thindb.sql.parseDialect(arena.allocator(), "SELECT id FROM t WHERE k = 100 || k = 300", .mysql);
        var cq = try thindb.net.compileWithSession(allocator, db, .{ .dialect = .mysql }, root);
        defer cq.deinit();
        var rows: usize = 0;
        while (try cq.next()) |b| rows += b.row_count;
        try std.testing.expectEqual(@as(usize, 3), rows);
    }
}

test "sql: GENERATED ALWAYS AS IDENTITY is an auto-increment column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try helpers.exec(allocator, db, "CREATE TABLE gi (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v int)");
    try helpers.exec(allocator, db, "INSERT INTO gi (v) VALUES (10)");
    try helpers.exec(allocator, db, "INSERT INTO gi (v) VALUES (20)");

    var q = try runSql(allocator, db, "SELECT id, v FROM gi ORDER BY id");
    defer q.deinit();
    var ids: [2]i64 = undefined;
    var n: usize = 0;
    while (try q.next()) |b| {
        for (0..b.row_count) |i| {
            ids[n] = b.values[0].data.bigint[i];
            n += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), n);
    try std.testing.expectEqual(@as(i64, 1), ids[0]);
    try std.testing.expectEqual(@as(i64, 2), ids[1]);
}

test "sql: PG type aliases + bigserial in CREATE TABLE" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try helpers.exec(allocator, db, "CREATE TABLE pgt (id bigserial PRIMARY KEY, n int4, big int8, small int2, label text)");
    try helpers.exec(allocator, db, "INSERT INTO pgt (n, big, small, label) VALUES (7, 99, 3, 'x')");

    var q = try runSql(allocator, db, "SELECT id, n, big, small FROM pgt");
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expect(std.meta.activeTag(schema[0].type) == .bigint); // bigserial
    try std.testing.expect(std.meta.activeTag(schema[1].type) == .int); // int4
    try std.testing.expect(std.meta.activeTag(schema[2].type) == .bigint); // int8
    try std.testing.expect(std.meta.activeTag(schema[3].type) == .smallint); // int2
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 1), b.values[0].data.bigint[0]); // auto-increment
    try std.testing.expectEqual(@as(i32, 7), b.values[1].data.int[0]);
}

test "sql: string_agg and group_concat concatenate grouped strings" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // string_agg(value, delimiter) — PG. tags per k: 100->a,b 200->a,b 300->c.
    {
        var q = try runSql(allocator, db, "SELECT k, string_agg(tag, '-') AS s FROM t GROUP BY k ORDER BY 1");
        defer q.deinit();
        var rows: usize = 0;
        var last: []const u8 = "";
        while (try q.next()) |b| {
            const sv = b.values[1].data.string;
            for (0..b.row_count) |i| {
                last = sv.rowBytes(i);
                rows += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 3), rows);
        // Last group (k=300) has a single tag, so no delimiter.
        try std.testing.expectEqualStrings("c", last);
    }

    // group_concat without a delimiter defaults to "," (MySQL).
    {
        var q = try runSql(allocator, db, "SELECT k, group_concat(tag) AS s FROM t GROUP BY k ORDER BY 1");
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqualStrings("a,b", b.values[1].data.string.rowBytes(0));
    }
}

test "sql: now()/current_timestamp()/current_date() resolve to real wall-clock" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT now() AS n, current_timestamp() AS t, current_date() AS d FROM t LIMIT 1");
    defer q.deinit();
    const b = (try q.next()).?;
    const now_us = b.values[0].data.datetime[0];
    const ts_us = b.values[1].data.datetime[0];
    const day = b.values[2].data.date[0];
    // Real wall-clock, not the old hardcoded 1970 epoch.
    try std.testing.expect(now_us > 1_500_000_000_000_000);
    // Statement-stable: all three share the one captured timestamp.
    try std.testing.expectEqual(now_us, ts_us);
    try std.testing.expectEqual(@as(i32, @intCast(@divFloor(now_us, std.time.us_per_day))), day);
}

test "sql: ORDER BY ordinal sorts by the Nth SELECT item" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // ORDER BY 1 DESC == ORDER BY qty DESC here; check descending order.
    var q = try runSql(allocator, db, "SELECT qty, id FROM t ORDER BY 1 DESC");
    defer q.deinit();
    var prev: ?i32 = null;
    var rows: usize = 0;
    while (try q.next()) |b| {
        const qv = b.values[0].data.int;
        for (0..b.row_count) |i| {
            if (prev) |p| try std.testing.expect(qv[i] <= p);
            prev = qv[i];
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 5), rows);

    // Same syntax against a pg_catalog vtable (the query psql/libpq sends).
    var q2 = try runSql(allocator, db, "SELECT datname FROM pg_catalog.pg_database ORDER BY 1");
    defer q2.deinit();
    var dbs: usize = 0;
    while (try q2.next()) |b| dbs += b.row_count;
    try std.testing.expect(dbs >= 1);
}

test "sql: pg_type maps a well-known OID to its type name" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT typname FROM pg_type WHERE oid = 23");
    defer q.deinit();
    var rows: usize = 0;
    while (try q.next()) |b| {
        const tn = b.values[0].data.string;
        for (0..b.row_count) |i| {
            try std.testing.expectEqualStrings("int4", tn.rowBytes(i));
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 1), rows);
}

test "sql: on MySQL a double-quoted token is a string literal, not a column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // "tag" under MySQL is the constant string 'tag', so every row's value
    // is the 3-byte literal — not the contents of the tag column.
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parseDialect(arena.allocator(), "SELECT \"tag\" FROM t", .mysql);
    var cq = try thindb.net.compileWithSession(allocator, db, .{ .dialect = .mysql }, root);
    defer cq.deinit();
    var rows: usize = 0;
    while (try cq.next()) |b| {
        const sv = b.values[0].data.string;
        for (0..b.row_count) |i| {
            try std.testing.expectEqualStrings("tag", sv.rowBytes(i));
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 5), rows);
}

test "sql: SELECT * FROM t returns all rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT * FROM t");
    defer q.deinit();
    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 5), rows);
}

test "sql: mixed star and expression projection preserves source columns first" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT *, qty + 1 AS next_qty FROM t ORDER BY id ASC");
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqual(@as(usize, 5), schema.len);
    try std.testing.expectEqualStrings("id", schema[0].name);
    try std.testing.expectEqualStrings("k", schema[1].name);
    try std.testing.expectEqualStrings("qty", schema[2].name);
    try std.testing.expectEqualStrings("tag", schema[3].name);
    try std.testing.expectEqualStrings("next_qty", schema[4].name);

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i32, 11), b.values[4].data.int[0]);
}

test "sql: alias star strips the alias prefix and qualifies colliding names" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT tt.* FROM t AS tt");
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqual(@as(usize, 4), schema.len);
    try std.testing.expectEqualStrings("id", schema[0].name);
    try std.testing.expectEqualStrings("k", schema[1].name);
    try std.testing.expectEqualStrings("qty", schema[2].name);
    try std.testing.expectEqualStrings("tag", schema[3].name);

    // Stripping both stars bare would collide; colliding names keep their
    // qualified form instead. The right join key (b.id) is dropped from the
    // join output (USING semantics), so a.id's bare `id` is unique.
    var star_q = try runSql(allocator, db, "SELECT a.*, b.* FROM t AS a JOIN t AS b ON a.id = b.id");
    defer star_q.deinit();
    const star_schema = star_q.outputSchema();
    try std.testing.expectEqual(@as(usize, 7), star_schema.len);
    try std.testing.expectEqualStrings("id", star_schema[0].name);
    try std.testing.expectEqualStrings("a.k", star_schema[1].name);
    try std.testing.expectEqualStrings("a.qty", star_schema[2].name);
    try std.testing.expectEqualStrings("a.tag", star_schema[3].name);
    try std.testing.expectEqualStrings("b.k", star_schema[4].name);
    try std.testing.expectEqualStrings("b.qty", star_schema[5].name);
    try std.testing.expectEqualStrings("b.tag", star_schema[6].name);

    var cte_q = try runSql(allocator, db,
        \\WITH left_rows AS (SELECT id, qty FROM t),
        \\     right_rows AS (SELECT id AS rid, tag FROM t)
        \\SELECT l.*, r.tag AS right_tag
        \\FROM left_rows AS l
        \\JOIN right_rows AS r ON l.id = r.rid
        \\ORDER BY id ASC
        \\LIMIT 1
    );
    defer cte_q.deinit();
    const cte_schema = cte_q.outputSchema();
    try std.testing.expectEqual(@as(usize, 3), cte_schema.len);
    try std.testing.expectEqualStrings("id", cte_schema[0].name);
    try std.testing.expectEqualStrings("qty", cte_schema[1].name);
    try std.testing.expectEqualStrings("right_tag", cte_schema[2].name);
}

test "sql: plain column aliases materialize through CTE output" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db,
        \\WITH renamed AS (SELECT qty AS amount FROM t)
        \\SELECT amount FROM renamed ORDER BY amount ASC
    );
    defer q.deinit();
    try std.testing.expectEqualStrings("amount", q.outputSchema()[0].name);
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i32, 10), b.values[0].data.int[0]);

    try helpers.expectRunError(
        allocator,
        db,
        "WITH renamed AS (SELECT qty AS amount FROM t) SELECT qty FROM renamed",
        thindb.exec.Error.ColumnNotFound,
    );
}

test "sql: NULL scalar projections produce nullable columns" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT NULL AS note, CAST(NULL AS INT) AS score FROM t");
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqual(@as(usize, 2), schema.len);
    try std.testing.expectEqualStrings("note", schema[0].name);
    try std.testing.expectEqual(thindb.Type{ .string = {} }, schema[0].type);
    try std.testing.expect(schema[0].nullable);
    try std.testing.expectEqualStrings("score", schema[1].name);
    try std.testing.expectEqual(thindb.Type{ .int = {} }, schema[1].type);
    try std.testing.expect(schema[1].nullable);

    const b = (try q.next()).?;
    try std.testing.expect(!b.values[0].isValid(0));
    try std.testing.expect(!b.values[1].isValid(0));
}

test "sql: SELECT id, qty FROM t (column projection)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT id, qty FROM t");
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqual(@as(usize, 2), schema.len);
    try std.testing.expectEqualStrings("id", schema[0].name);
    try std.testing.expectEqualStrings("qty", schema[1].name);
}

test "sql: WHERE with comparison + AND" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT id FROM t WHERE qty > 20 AND k <= 200");
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 4 }, ids.items);
}

test "sql: WHERE with OR + parens + string literal" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT id FROM t WHERE (tag = 'a' OR tag = 'c')");
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 3, 5 }, ids.items);
}

test "sql: ORDER BY DESC + LIMIT" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT id FROM t ORDER BY qty DESC LIMIT 2");
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    // qty descending: 50 (id 5), 40 (id 4). LIMIT 2.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 5, 4 }, ids.items);
}

test "sql: MySQL LIMIT offset,count with zero offset" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT id FROM t ORDER BY id ASC LIMIT 0, 2");
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2 }, ids.items);
}

test "sql: LIMIT count OFFSET off skips leading rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT id FROM t ORDER BY id ASC LIMIT 2 OFFSET 2");
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    // ids 1..5; skip 2 → start at id 3; take 2 → 3,4.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 4 }, ids.items);
}

test "sql: MySQL LIMIT offset,count with non-zero offset" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT id FROM t ORDER BY id ASC LIMIT 1, 3");
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    // offset 1, count 3 → ids 2,3,4.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3, 4 }, ids.items);
}

test "sql: OFFSET past the end yields no rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT id FROM t ORDER BY id ASC LIMIT 10 OFFSET 99");
    defer q.deinit();
    var n: usize = 0;
    while (try q.next()) |b| n += b.row_count;
    try std.testing.expectEqual(@as(usize, 0), n);
}

test "sql: GROUP BY with count(*) and sum(qty)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db,
        \\SELECT tag, count(*) AS n, sum(qty) AS total FROM t GROUP BY tag
    );
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqualStrings("tag", schema[0].name);
    try std.testing.expectEqualStrings("n", schema[1].name);
    try std.testing.expectEqualStrings("total", schema[2].name);

    var seen_a = false;
    var seen_b = false;
    var seen_c = false;
    while (try q.next()) |b| {
        for (0..b.row_count) |i| {
            const tag = b.values[0].data.string.rowBytes(i);
            const n = b.values[1].data.bigint[i];
            const total = b.values[2].data.bigint[i];
            if (std.mem.eql(u8, tag, "a")) {
                seen_a = true;
                try std.testing.expectEqual(@as(i64, 2), n);
                try std.testing.expectEqual(@as(i64, 40), total); // 10 + 30
            } else if (std.mem.eql(u8, tag, "b")) {
                seen_b = true;
                try std.testing.expectEqual(@as(i64, 2), n);
                try std.testing.expectEqual(@as(i64, 60), total); // 20 + 40
            } else if (std.mem.eql(u8, tag, "c")) {
                seen_c = true;
                try std.testing.expectEqual(@as(i64, 1), n);
                try std.testing.expectEqual(@as(i64, 50), total);
            } else return error.UnexpectedTag;
        }
    }
    try std.testing.expect(seen_a and seen_b and seen_c);
}

test "sql: grouped SUM/AVG over BIGINT accumulate in i128 (wide two-slot state)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const schema_w = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "tag", .type = .string },
            .{ .name = "v", .type = .bigint, .nullable = true },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok_w = [_][]const u8{"id"};
    const t = try db.table("w", schema_w, .{ .order_key = &ok_w, .unique = true, .row_group_size = 4 });
    const big: i64 = 4_000_000_000_000_000_000; // three of these overflow i64
    try t.insert(&.{
        .{ .id = @as(i64, 1), .tag = "a", .v = @as(?i64, big) },
        .{ .id = @as(i64, 2), .tag = "a", .v = @as(?i64, big) },
        .{ .id = @as(i64, 3), .tag = "a", .v = @as(?i64, big) },
        .{ .id = @as(i64, 4), .tag = "a", .v = @as(?i64, null) },
        .{ .id = @as(i64, 5), .tag = "b", .v = @as(?i64, -big) },
        .{ .id = @as(i64, 6), .tag = "b", .v = @as(?i64, -big) },
        .{ .id = @as(i64, 7), .tag = "b", .v = @as(?i64, -big) },
    });
    try t.flush();

    var q = try runSql(allocator, db,
        \\SELECT tag, sum(v) AS s, avg(v) AS a, count(v) AS c FROM w GROUP BY tag
    );
    defer q.deinit();
    var seen_a = false;
    var seen_b = false;
    while (try q.next()) |b| {
        for (0..b.row_count) |i| {
            const tag = b.values[0].data.string.rowBytes(i);
            const s = b.values[1].data.largeint[i];
            const a = b.values[2].data.double[i];
            const c = b.values[3].data.bigint[i];
            if (std.mem.eql(u8, tag, "a")) {
                seen_a = true;
                try std.testing.expectEqual(@as(i128, 3) * big, s);
                try std.testing.expectApproxEqRel(@as(f64, @floatFromInt(big)), a, 1e-12);
                try std.testing.expectEqual(@as(i64, 3), c);
            } else if (std.mem.eql(u8, tag, "b")) {
                seen_b = true;
                try std.testing.expectEqual(@as(i128, -3) * big, s);
                try std.testing.expectApproxEqRel(@as(f64, @floatFromInt(-big)), a, 1e-12);
                try std.testing.expectEqual(@as(i64, 3), c);
            } else return error.UnexpectedTag;
        }
    }
    try std.testing.expect(seen_a and seen_b);
}

test "sql: global aggregate (no GROUP BY) — count(*), avg, min, max" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db,
        \\SELECT count(*) AS n, avg(qty) AS a, min(qty) AS lo, max(qty) AS hi FROM t
    );
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expectEqual(@as(i64, 5), b.values[0].data.bigint[0]);
    try std.testing.expectApproxEqAbs(@as(f64, 30.0), b.values[1].data.double[0], 1e-9);
    try std.testing.expectEqual(@as(i32, 10), b.values[2].data.int[0]);
    try std.testing.expectEqual(@as(i32, 50), b.values[3].data.int[0]);
}

test "sql: ORDER BY ... LIMIT stays within a tight memory budget (Top-N)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // Tiny budget + small row groups: a full sort of 200 rows can't fit,
    // but a bounded Top-N keeping ~5 rows can.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .query_memory_budget = 1024,
        .row_group_size = 8,
    });
    defer db.close();

    const schema = thindb.TableSchema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("big", schema, .{ .order_key = &ok, .unique = false, .row_group_size = 8 });
    // Flush every 8 rows so the data lands in many small segments — the
    // scan then yields small per-row-group batches, which is what lets
    // Top-N prune between batches (a single batch bigger than the budget
    // can't be helped). 200 rows → 25 segments of 8.
    var i: i64 = 0;
    while (i < 200) : (i += 1) {
        try t.insert(&.{.{ .id = i }});
        if (@mod(i + 1, 8) == 0) try t.flush();
    }
    try t.flush();

    // Top-N: bounded → succeeds under the tight budget, correct top 5.
    {
        var q = try runSql(allocator, db, "SELECT id FROM big ORDER BY id DESC LIMIT 5");
        defer q.deinit();
        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        while (try q.next()) |b| {
            for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
        }
        try std.testing.expectEqualSlices(i64, &[_]i64{ 199, 198, 197, 196, 195 }, ids.items);
    }

    // With OFFSET: skip the top 2, take next 3.
    {
        var q = try runSql(allocator, db, "SELECT id FROM big ORDER BY id DESC LIMIT 3 OFFSET 2");
        defer q.deinit();
        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        while (try q.next()) |b| {
            for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
        }
        try std.testing.expectEqualSlices(i64, &[_]i64{ 197, 196, 195 }, ids.items);
    }

    // A full sort (no LIMIT) is unbounded → exceeds the same budget.
    {
        var q = try runSql(allocator, db, "SELECT id FROM big ORDER BY id DESC");
        defer q.deinit();
        try std.testing.expectError(error.MemoryBudgetExceeded, q.next());
    }
}

test "sql: HAVING with raw aggregate (aliased)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // HAVING count(*) > 1 (raw aggregate) binds to the aliased c.
    // Groups: 100(2),200(2),300(1) → keep 100,200.
    var q = try runSql(allocator, db,
        \\SELECT k, count(*) AS c FROM t GROUP BY k HAVING count(*) > 1 ORDER BY k ASC
    );
    defer q.deinit();
    var ks: std.ArrayList(i64) = .empty;
    defer ks.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.int[0..b.row_count]) |v| try ks.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 100, 200 }, ks.items);
}

test "sql: HAVING with raw aggregate (unaliased)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db,
        \\SELECT k, count(*) FROM t GROUP BY k HAVING count(*) > 1 ORDER BY k ASC
    );
    defer q.deinit();
    var ks: std.ArrayList(i64) = .empty;
    defer ks.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.int[0..b.row_count]) |v| try ks.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 100, 200 }, ks.items);
}

test "sql: DATE_TRUNC + GROUP BY/ORDER BY on the expression (ClickBench Q42 shape)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "ts", .type = .datetime },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("ev", schema, .{ .order_key = &ok, .unique = false });
    // ts in micros: 90s, 110s (both minute 60s), 130s (minute 120s).
    try t.insert(&.{
        .{ .id = @as(i64, 1), .ts = @as(i64, 90_000_000) },
        .{ .id = @as(i64, 2), .ts = @as(i64, 110_000_000) },
        .{ .id = @as(i64, 3), .ts = @as(i64, 130_000_000) },
    });
    try t.flush();

    var q = try runSql(allocator, db,
        \\SELECT date_trunc('minute', ts) AS m, count(*) AS c FROM ev
        \\GROUP BY date_trunc('minute', ts) ORDER BY date_trunc('minute', ts) ASC
    );
    defer q.deinit();
    var ms: std.ArrayList(i64) = .empty;
    var cs: std.ArrayList(i64) = .empty;
    defer ms.deinit(allocator);
    defer cs.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.datetime[0..b.row_count]) |v| try ms.append(allocator, v);
        for (b.values[1].data.bigint[0..b.row_count]) |v| try cs.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 60_000_000, 120_000_000 }, ms.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 1 }, cs.items);
}

test "sql: REGEXP_REPLACE extracts hostname (ClickBench Q28 function)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "url", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("u", schema, .{ .order_key = &ok, .unique = false });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .url = "https://www.example.com/a/b" },
        .{ .id = @as(i64, 2), .url = "http://sub.host.org/x" },
        .{ .id = @as(i64, 3), .url = "https://plain.net/" },
    });
    try t.flush();

    // Raw multiline string: backslashes are literal, so the SQL sees
    // `\.` and `\1` exactly (the lexer doesn't process backslash escapes).
    var q = try runSql(allocator, db,
        \\SELECT regexp_replace(url, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS host FROM u ORDER BY id ASC
    );
    defer q.deinit();
    var hosts: std.ArrayList([]const u8) = .empty;
    defer {
        for (hosts.items) |s| allocator.free(s);
        hosts.deinit(allocator);
    }
    while (try q.next()) |b| {
        const sv = b.values[0].data.string;
        var r: usize = 0;
        while (r < b.row_count) : (r += 1) {
            try hosts.append(allocator, try allocator.dupe(u8, sv.rowBytes(@intCast(r))));
        }
    }
    try std.testing.expectEqual(@as(usize, 3), hosts.items.len);
    try std.testing.expectEqualStrings("example.com", hosts.items[0]);
    try std.testing.expectEqualStrings("sub.host.org", hosts.items[1]);
    try std.testing.expectEqualStrings("plain.net", hosts.items[2]);
}

test "sql: literal in SELECT projection" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // Bare literal projected for every row.
    var q = try runSql(allocator, db, "SELECT 1 AS one, id FROM t ORDER BY id ASC");
    defer q.deinit();
    var ones: usize = 0;
    var rows: usize = 0;
    while (try q.next()) |b| {
        for (b.values[0].data.int[0..b.row_count]) |v| {
            if (v == 1) ones += 1;
        }
        rows += b.row_count;
    }
    try std.testing.expectEqual(@as(usize, 5), rows);
    try std.testing.expectEqual(@as(usize, 5), ones);
}

test "sql: SELECT literal with GROUP BY ordinal (ClickBench Q34 shape)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // SELECT 1, k, count(*) ... GROUP BY 1, k — the literal is ordinal 1.
    // Grouping by a constant + k ≡ grouping by k: 100(2),200(2),300(1).
    var q = try runSql(allocator, db,
        \\SELECT 1, k, count(*) AS c FROM t GROUP BY 1, k ORDER BY k ASC
    );
    defer q.deinit();
    var counts: std.ArrayList(i64) = .empty;
    defer counts.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[2].data.bigint[0..b.row_count]) |v| try counts.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 2, 1 }, counts.items);
}

test "sql: GROUP BY ordinal" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // GROUP BY 1 → the first SELECT item (k). k = 100,100,200,200,300.
    var q = try runSql(allocator, db, "SELECT k, count(*) AS n FROM t GROUP BY 1 ORDER BY k ASC");
    defer q.deinit();
    var counts: std.ArrayList(i64) = .empty;
    defer counts.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[1].data.bigint[0..b.row_count]) |v| try counts.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 2, 1 }, counts.items);
}

test "sql: GROUP BY alias of a computed expression" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // k + 1 = 101,101,201,201,301 → groups 101(2),201(2),301(1).
    var q = try runSql(allocator, db,
        \\SELECT k + 1 AS kp, count(*) AS n FROM t GROUP BY kp ORDER BY kp ASC
    );
    defer q.deinit();
    var kps: std.ArrayList(i64) = .empty;
    var counts: std.ArrayList(i64) = .empty;
    defer kps.deinit(allocator);
    defer counts.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.int[0..b.row_count]) |v| try kps.append(allocator, v);
        for (b.values[1].data.bigint[0..b.row_count]) |v| try counts.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 101, 201, 301 }, kps.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 2, 1 }, counts.items);
}

test "sql: GROUP BY raw expression matched to aliased SELECT expr" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // GROUP BY k - 1 (raw) binds structurally to SELECT k - 1 AS km.
    var q = try runSql(allocator, db,
        \\SELECT k - 1 AS km, count(*) AS n FROM t GROUP BY k - 1 ORDER BY km ASC
    );
    defer q.deinit();
    var counts: std.ArrayList(i64) = .empty;
    defer counts.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[1].data.bigint[0..b.row_count]) |v| try counts.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 2, 1 }, counts.items);
}

test "sql: GROUP BY multiple distinct arithmetic expressions" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // k - 1 and k - 2 get distinct default names (no collision) and both
    // resolve as grouping keys.
    var q = try runSql(allocator, db,
        \\SELECT k - 1, k - 2, count(*) AS n FROM t GROUP BY k - 1, k - 2
    );
    defer q.deinit();
    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    // Three distinct k values → three groups.
    try std.testing.expectEqual(@as(usize, 3), rows);
}

test "sql: GROUP BY an aliased plain column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // GROUP BY kk (alias of k) groups on the underlying column k.
    var q = try runSql(allocator, db,
        \\SELECT k AS kk, count(*) AS n FROM t GROUP BY kk ORDER BY k ASC
    );
    defer q.deinit();
    var counts: std.ArrayList(i64) = .empty;
    defer counts.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[1].data.bigint[0..b.row_count]) |v| try counts.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 2, 1 }, counts.items);
}

test "sql: non-grouped scalar expr alongside aggregate is still rejected" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // k + 1 is neither aggregated nor grouped → ambiguous.
    try std.testing.expectError(
        error.SqlMixedAggAndPlainProjection,
        runSql(allocator, db, "SELECT k + 1, count(*) FROM t GROUP BY k"),
    );
}

test "sql: predicate coercion — int literal narrows to smallint column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "engine", .type = .smallint },
            .{ .name = "d", .type = .date },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("ev", schema, .{ .order_key = &ok, .unique = false });
    // d: 2013-07-01 = 15887 days, 2013-08-01 = 15918 days since epoch.
    try t.insert(&.{
        .{ .id = @as(i64, 1), .engine = @as(i16, 0), .d = @as(i32, 15887) },
        .{ .id = @as(i64, 2), .engine = @as(i16, 3), .d = @as(i32, 15918) },
        .{ .id = @as(i64, 3), .engine = @as(i16, 0), .d = @as(i32, 15887) },
    });
    try t.flush();

    // engine <> 0 : int literal 0 must narrow to smallint.
    var q = try runSql(allocator, db, "SELECT count(*) AS n FROM ev WHERE engine <> 0");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 1), b.values[0].data.bigint[0]);
}

test "sql: predicate coercion — int literal narrows to smallint inside CASE WHEN" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "engine", .type = .smallint },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("ev", schema, .{ .order_key = &ok, .unique = false });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .engine = @as(i16, 0) },
        .{ .id = @as(i64, 2), .engine = @as(i16, 3) },
        .{ .id = @as(i64, 3), .engine = @as(i16, 0) },
    });
    try t.flush();

    // The CASE condition `engine = 0` compares a SMALLINT column to an int
    // literal. Without coercing the branch condition (as the Filter does
    // for WHERE), evaluateMaskWithPred reads the wrong Value union field
    // and panics. Expect 2 rows where engine = 0.
    var q = try runSql(allocator, db, "SELECT SUM(CASE WHEN engine = 0 THEN 1 ELSE 0 END) AS n FROM ev");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 2), b.values[0].data.bigint[0]);
}

test "sql: CASE result branches coerce numeric widths" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    {
        var q = try runSql(allocator, db,
            \\SELECT CASE WHEN id = 1 THEN id ELSE 0 END AS picked FROM t ORDER BY id ASC
        );
        defer q.deinit();
        const schema = q.outputSchema();
        try std.testing.expectEqual(thindb.Type.bigint, schema[0].type);
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 5), b.row_count);
        try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 0, 0, 0, 0 }, b.values[0].data.bigint[0..b.row_count]);
        try std.testing.expect((try q.next()) == null);
    }

    {
        // SUM over a bigint CASE widens to LARGEINT (i128) on this engine
        // (overflow-safe SUM), unlike the bare bigint the branch type implies.
        var q = try runSql(allocator, db,
            \\SELECT SUM(CASE WHEN tag = 'a' THEN id ELSE 0 END) AS total FROM t
        );
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(i128, 4), b.values[0].data.largeint[0]);
    }
}

test "sql: predicate coercion — string literal parses to DATE column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "d", .type = .date },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("ev2", schema, .{ .order_key = &ok, .unique = false });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .d = @as(i32, 15887) }, // 2013-07-01
        .{ .id = @as(i64, 2), .d = @as(i32, 15918) }, // 2013-08-01
    });
    try t.flush();

    // '2013-07-15' = 15901 days. Only row 1 (15887) is < that.
    var q = try runSql(allocator, db, "SELECT count(*) AS n FROM ev2 WHERE d < '2013-07-15'");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 1), b.values[0].data.bigint[0]);
}

test "sql: ORDER BY aggregate (COUNT(*))" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // tags a(2), b(2), c(1). ORDER BY COUNT(*) DESC → a/b before c.
    // tie between a,b broken by group order; assert the last row is c with 1.
    var q = try runSql(allocator, db,
        \\SELECT tag, count(*) FROM t GROUP BY tag ORDER BY count(*) DESC
    );
    defer q.deinit();
    var counts: std.ArrayList(i64) = .empty;
    defer counts.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[1].data.bigint[0..b.row_count]) |v| try counts.append(allocator, v);
    }
    try std.testing.expectEqual(@as(usize, 3), counts.items.len);
    try std.testing.expectEqual(@as(i64, 2), counts.items[0]);
    try std.testing.expectEqual(@as(i64, 2), counts.items[1]);
    try std.testing.expectEqual(@as(i64, 1), counts.items[2]);
}

test "sql: ORDER BY aggregate ASC on COUNT(col)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // ORDER BY count(qty) ASC: c has 1, a/b have 2 → c first. The
    // aggregate is unaliased so its output column keeps the canonical
    // name "count(qty)" that ORDER BY binds against.
    var q = try runSql(allocator, db,
        \\SELECT tag, count(qty) FROM t GROUP BY tag ORDER BY count(qty) ASC
    );
    defer q.deinit();
    var counts: std.ArrayList(i64) = .empty;
    defer counts.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[1].data.bigint[0..b.row_count]) |v| try counts.append(allocator, v);
    }
    try std.testing.expectEqual(@as(usize, 3), counts.items.len);
    try std.testing.expectEqual(@as(i64, 1), counts.items[0]);
}

test "sql: COUNT(DISTINCT col) global" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // tags a,b,a,b,c → 3 distinct. k values 100,100,200,200,300 → 3 distinct.
    var q = try runSql(allocator, db,
        \\SELECT count(DISTINCT tag) AS dt, count(DISTINCT k) AS dk FROM t
    );
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expectEqual(@as(i64, 3), b.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 3), b.values[1].data.bigint[0]);
}

test "sql: COUNT(DISTINCT col) grouped" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // GROUP BY tag: a→{100,200} (2), b→{100,200} (2), c→{300} (1).
    var q = try runSql(allocator, db,
        \\SELECT tag, count(DISTINCT k) AS dk FROM t GROUP BY tag ORDER BY tag ASC
    );
    defer q.deinit();
    var dks: std.ArrayList(i64) = .empty;
    defer dks.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[1].data.bigint[0..b.row_count]) |v| try dks.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 2, 1 }, dks.items);
}

test "sql: DISTINCT in a scalar call is rejected" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    try std.testing.expectError(
        error.SqlInvalidProjection,
        runSql(allocator, db, "SELECT upper(DISTINCT tag) FROM t"),
    );
}

test "sql: MIN/MAX on string column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // tags are a,b,a,b,c → lexicographic min "a", max "c".
    var q = try runSql(allocator, db, "SELECT min(tag) AS lo, max(tag) AS hi FROM t");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expectEqualStrings("a", b.values[0].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("c", b.values[1].data.string.rowBytes(0));
}

test "sql: MIN/MAX on string column grouped" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // GROUP BY k: k=100 → tags {a,b} → min a/max b; k=200 → {a,b}; k=300 → {c}.
    var q = try runSql(allocator, db,
        \\SELECT k, min(tag) AS lo, max(tag) AS hi FROM t GROUP BY k ORDER BY k ASC
    );
    defer q.deinit();
    var los: std.ArrayList([]const u8) = .empty;
    var his: std.ArrayList([]const u8) = .empty;
    defer {
        for (los.items) |s| allocator.free(s);
        for (his.items) |s| allocator.free(s);
        los.deinit(allocator);
        his.deinit(allocator);
    }
    while (try q.next()) |bb| {
        var r: usize = 0;
        while (r < bb.row_count) : (r += 1) {
            try los.append(allocator, try allocator.dupe(u8, bb.values[1].data.string.rowBytes(@intCast(r))));
            try his.append(allocator, try allocator.dupe(u8, bb.values[2].data.string.rowBytes(@intCast(r))));
        }
    }
    try std.testing.expectEqual(@as(usize, 3), los.items.len);
    try std.testing.expectEqualStrings("a", los.items[0]);
    try std.testing.expectEqualStrings("b", his.items[0]);
    try std.testing.expectEqualStrings("a", los.items[1]);
    try std.testing.expectEqualStrings("b", his.items[1]);
    try std.testing.expectEqualStrings("c", los.items[2]);
    try std.testing.expectEqualStrings("c", his.items[2]);
}

test "sql: IS NULL / IS NOT NULL on nullable column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "note", .type = .string, .nullable = true },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("nt", schema, .{ .order_key = &ok, .unique = true, .row_group_size = 8 });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .note = @as(?[]const u8, "hi") },
        .{ .id = @as(i64, 2), .note = @as(?[]const u8, null) },
        .{ .id = @as(i64, 3), .note = @as(?[]const u8, "yo") },
    });
    try t.flush();

    var q1 = try runSql(allocator, db, "SELECT id FROM nt WHERE note IS NULL");
    defer q1.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q1.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{2}, ids.items);

    var q2 = try runSql(allocator, db, "SELECT id FROM nt WHERE note IS NOT NULL");
    defer q2.deinit();
    ids.clearRetainingCapacity();
    while (try q2.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 3 }, ids.items);

    // OFFSET over a nullable string column exercises the null-bitmap
    // bit-shift in the boundary batch (skip 1 → drop the first row, whose
    // validity bit must not bleed into the kept rows' validity).
    var q3 = try runSql(allocator, db, "SELECT note FROM nt ORDER BY id ASC LIMIT 5 OFFSET 1");
    defer q3.deinit();
    var notes: std.ArrayList([]const u8) = .empty;
    defer {
        for (notes.items) |s| allocator.free(s);
        notes.deinit(allocator);
    }
    var saw_null = false;
    while (try q3.next()) |b| {
        const v = b.values[0];
        var r: usize = 0;
        while (r < b.row_count) : (r += 1) {
            if (!thindb.storage.column.isValidBit(v.nulls, r)) {
                saw_null = true;
            } else {
                const s = v.data.string;
                const bytes = s.bytes[s.offsets[r]..s.offsets[r + 1]];
                try notes.append(allocator, try allocator.dupe(u8, bytes));
            }
        }
    }
    // Rows after skipping id=1: id=2 (null), id=3 ("yo").
    try std.testing.expect(saw_null);
    try std.testing.expectEqual(@as(usize, 1), notes.items.len);
    try std.testing.expectEqualStrings("yo", notes.items[0]);
}

// ---------------------------------------------------------------------------
// Scalar function calls in SELECT (Compute step)
// ---------------------------------------------------------------------------

test "sql: scalar function in SELECT — upper(tag)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT id, upper(tag) AS u FROM t");
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqualStrings("id", schema[0].name);
    try std.testing.expectEqualStrings("u", schema[1].name);

    var seen: std.ArrayList(u8) = .empty;
    defer seen.deinit(allocator);
    while (try q.next()) |b| {
        const sv = b.values[1].data.string;
        for (0..b.row_count) |i| {
            try seen.appendSlice(allocator, sv.rowBytes(i));
            try seen.append(allocator, '|');
        }
    }
    try std.testing.expectEqualStrings("A|B|A|B|C|", seen.items);
}

test "sql: scalar function default alias derived from call form" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT length(tag) FROM t");
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqualStrings("length(tag)", schema[0].name);
}

test "sql: scalar function with single col-ref arg + WHERE + ORDER BY" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db,
        \\SELECT id, length(tag) AS taglen
        \\FROM t
        \\WHERE k >= 200
        \\ORDER BY id ASC
    );
    defer q.deinit();
    var got_ids: std.ArrayList(i64) = .empty;
    defer got_ids.deinit(allocator);
    var got_lens: std.ArrayList(i32) = .empty;
    defer got_lens.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try got_ids.append(allocator, v);
        for (b.values[1].data.int[0..b.row_count]) |v| try got_lens.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 4, 5 }, got_ids.items);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 1, 1 }, got_lens.items);
}

test "sql: scalar function with literal args (Compute literal-buffer path)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // lpad(tag, 3, '_') — column + int literal + string literal.
    // Each literal arg materializes as a per-batch constant column
    // inside Compute, then the kernel sees uniform-width arg slices.
    var q = try runSql(allocator, db,
        \\SELECT id, lpad(tag, 3, '_') AS padded
        \\FROM t
        \\WHERE k >= 200
        \\ORDER BY id ASC
    );
    defer q.deinit();
    var got: std.ArrayList(u8) = .empty;
    defer got.deinit(allocator);
    while (try q.next()) |b| {
        const sv = b.values[1].data.string;
        for (0..b.row_count) |i| {
            try got.appendSlice(allocator, sv.rowBytes(i));
            try got.append(allocator, '|');
        }
    }
    try std.testing.expectEqualStrings("__a|__b|__c|", got.items);
}

test "sql: nested scalar calls — length(upper(tag))" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT length(upper(tag)) AS n FROM t");
    defer q.deinit();
    var lens: std.ArrayList(i32) = .empty;
    defer lens.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.int[0..b.row_count]) |v| try lens.append(allocator, v);
    }
    // All five tags are single-char → length 1 each.
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 1, 1, 1, 1 }, lens.items);
}

test "sql: nested call with literal arg — lpad(upper(tag), 3, '_')" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db, "SELECT lpad(upper(tag), 3, '_') AS p FROM t ORDER BY id ASC");
    defer q.deinit();
    var got: std.ArrayList(u8) = .empty;
    defer got.deinit(allocator);
    while (try q.next()) |b| {
        const sv = b.values[0].data.string;
        for (0..b.row_count) |i| {
            try got.appendSlice(allocator, sv.rowBytes(i));
            try got.append(allocator, '|');
        }
    }
    try std.testing.expectEqualStrings("__A|__B|__A|__B|__C|", got.items);
}

test "sql: nested aggregate-inside-scalar rejected" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // Aggregates inside scalar calls aren't allowed.
    const res = runSql(allocator, db, "SELECT upper(count(*)) FROM t");
    try std.testing.expectError(thindb.sql.ParseError.SqlInvalidProjection, res);
}

test "sql: scalar functions disallowed alongside aggregates (v1 limit)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // The parser currently errors when a scalar expression sits in
    // the same SELECT list as an aggregate or a GROUP BY query.
    const res = runSql(allocator, db,
        \\SELECT upper(tag), count(*) FROM t GROUP BY tag
    );
    try std.testing.expectError(thindb.sql.ParseError.SqlMixedAggAndPlainProjection, res);
}

// ---------------------------------------------------------------------------
// JOIN support
// ---------------------------------------------------------------------------

const schema_orders = thindb.TableSchema{
    .columns = &.{
        .{ .name = "oid", .type = .bigint },
        .{ .name = "item_id", .type = .int },
        .{ .name = "qty", .type = .int },
    },
    .order_key = &.{"oid"},
    .unique = true,
};
const schema_items = thindb.TableSchema{
    .columns = &.{
        .{ .name = "iid", .type = .int },
        .{ .name = "name", .type = .string },
    },
    .order_key = &.{"iid"},
    .unique = true,
};
const ok_orders = [_][]const u8{"oid"};
const ok_items = [_][]const u8{"iid"};

fn seedOrdersItems(db: anytype) !void {
    const o = try db.table("orders", schema_orders, .{ .order_key = &ok_orders, .unique = true, .row_group_size = 8 });
    const i = try db.table("items", schema_items, .{ .order_key = &ok_items, .unique = true, .row_group_size = 8 });
    try o.insert(&.{
        .{ .oid = @as(i64, 1), .item_id = @as(i32, 100), .qty = @as(i32, 2) },
        .{ .oid = @as(i64, 2), .item_id = @as(i32, 200), .qty = @as(i32, 5) },
        .{ .oid = @as(i64, 3), .item_id = @as(i32, 999), .qty = @as(i32, 1) }, // no item
    });
    try i.insert(&.{
        .{ .iid = @as(i32, 100), .name = "alpha" },
        .{ .iid = @as(i32, 200), .name = "beta" },
        .{ .iid = @as(i32, 300), .name = "gamma" }, // no order
    });
    try o.flush();
    try i.flush();
}

test "sql: simple INNER JOIN with qualified ON" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedOrdersItems(db);

    var q = try runSql(allocator, db,
        \\SELECT oid, qty, name
        \\FROM orders INNER JOIN items ON orders.item_id = items.iid
    );
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    // Inner: 2 matches (orders 1 and 2 join to items 100 and 200; order 3 has no match).
    try std.testing.expectEqual(@as(usize, 2), rows);
}

test "sql: JOIN keyword alone defaults to INNER" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedOrdersItems(db);

    var q = try runSql(allocator, db,
        \\SELECT name FROM orders JOIN items ON orders.item_id = items.iid
    );
    defer q.deinit();
    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 2), rows);
}

test "sql: LEFT OUTER JOIN preserves unmatched left rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedOrdersItems(db);

    var q = try runSql(allocator, db,
        \\SELECT oid FROM orders LEFT OUTER JOIN items ON orders.item_id = items.iid
    );
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    // All three orders survive (order 3 has no items match but LEFT JOIN preserves it).
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, ids.items);
}

test "sql: JOIN feeds into WHERE + ORDER BY + LIMIT downstream" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedOrdersItems(db);

    var q = try runSql(allocator, db,
        \\SELECT name, qty
        \\FROM orders JOIN items ON orders.item_id = items.iid
        \\WHERE qty >= 2
        \\ORDER BY qty DESC
        \\LIMIT 1
    );
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expectEqualStrings("beta", b.values[0].data.string.rowBytes(0));
    try std.testing.expectEqual(@as(i32, 5), b.values[1].data.int[0]);
}

test "sql: three-table JOIN (A JOIN B ON ... JOIN C ON ...)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedOrdersItems(db);

    // Add a third table that joins to items.name.
    const schema_cats = thindb.TableSchema{
        .columns = &.{
            .{ .name = "cid", .type = .int },
            .{ .name = "name", .type = .string },
            .{ .name = "category", .type = .string },
        },
        .order_key = &.{"cid"},
        .unique = true,
    };
    const ok_cats = [_][]const u8{"cid"};
    const cats = try db.table("cats", schema_cats, .{ .order_key = &ok_cats, .unique = true, .row_group_size = 8 });
    try cats.insert(&.{
        .{ .cid = @as(i32, 1), .name = "alpha", .category = "fruit" },
        .{ .cid = @as(i32, 2), .name = "beta", .category = "vege" },
    });
    try cats.flush();

    // Three-table join: orders ⋈ items on item_id=iid, then ⋈ cats on items.name=cats.name.
    // No name collision: the second join uses `name` as a join key, so USING-style
    // elision drops the duplicate.
    var q = try runSql(allocator, db,
        \\SELECT oid FROM orders
        \\  JOIN items ON orders.item_id = items.iid
        \\  JOIN cats ON items.name = cats.name
    );
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    // orders {1,2,3} ⋈ items {100,200,300} ⋈ cats {alpha,beta}:
    //   order 1 (item_id=100, name=alpha, cat=fruit)  → kept
    //   order 2 (item_id=200, name=beta,  cat=vege)   → kept
    //   order 3 (item_id=999) — no items match — dropped before cat join
    try std.testing.expectEqual(@as(usize, 2), ids.items.len);
}

// ---------------------------------------------------------------------------
// CTE (WITH) + FROM-clause subqueries — multi-source via SQL
// ---------------------------------------------------------------------------

test "sql: single-reference CTE inlined as the FROM target" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db,
        \\WITH big AS (SELECT * FROM t WHERE k >= 200)
        \\SELECT id FROM big ORDER BY id ASC
    );
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 4, 5 }, ids.items);
}

test "sql: CTE referenced twice in a self-join (inline DAG)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // The CTE expression compiles into a single *ir.Op; both join
    // sides reference it. Compile produces two independent runtime
    // operators (inline semantics) — proven correct here by row count.
    // CTE exposes ONLY the join-key column; v1 SQL can't yet
    // disambiguate same-named non-key columns across join sides,
    // so the test uses the simplest shape that exercises the
    // DAG-shared-subtree path without hitting that limit.
    var q = try runSql(allocator, db,
        \\WITH big AS (SELECT k FROM t WHERE k >= 200)
        \\SELECT k FROM big JOIN big AS other ON big.k = other.k
    );
    defer q.deinit();
    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    // big = {200,200,300}. Self-join on k:
    //   k=200 pairs: 2×2 = 4 matches
    //   k=300 pairs: 1×1 = 1 match
    //   total = 5
    try std.testing.expectEqual(@as(usize, 5), rows);
}

test "sql: chained CTEs — later CTE references an earlier one" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db,
        \\WITH
        \\  big AS (SELECT * FROM t WHERE k >= 200),
        \\  ordered AS (SELECT id FROM big ORDER BY id DESC)
        \\SELECT id FROM ordered LIMIT 2
    );
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 5, 4 }, ids.items);
}

test "sql: redefining a CTE name errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    const res = runSql(allocator, db,
        \\WITH x AS (SELECT * FROM t), x AS (SELECT * FROM t) SELECT * FROM x
    );
    try std.testing.expectError(thindb.sql.ParseError.SqlCteRedefined, res);
}

test "sql: FROM-clause subquery (anonymous CTE)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db,
        \\SELECT id FROM (SELECT * FROM t WHERE k >= 200) AS sub ORDER BY id ASC
    );
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 4, 5 }, ids.items);
}

test "sql: aggregate-then-join via FROM-subquery (the canonical pattern)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedOrdersItems(db);

    // Aggregate orders by item_id, then join the result against items.
    // The aggregation runs on one branch of the join — previously only
    // expressible via PlanBuilder; now ergonomic in SQL.
    var q = try runSql(allocator, db,
        \\SELECT items.name, t.total_qty
        \\FROM items
        \\JOIN (SELECT item_id, sum(qty) AS total_qty FROM orders GROUP BY item_id) AS t
        \\  ON items.iid = t.item_id
    );
    defer q.deinit();

    var got: std.ArrayList(u8) = .empty;
    defer got.deinit(allocator);
    while (try q.next()) |b| {
        const sv = b.values[0].data.string;
        for (0..b.row_count) |i| {
            try got.appendSlice(allocator, sv.rowBytes(i));
            try got.append(allocator, ':');
            const tot = b.values[1].data.bigint[i];
            var nbuf: [16]u8 = undefined;
            const n = try std.fmt.bufPrint(&nbuf, "{d}", .{tot});
            try got.appendSlice(allocator, n);
            try got.append(allocator, '|');
        }
    }
    // orders k=100: qty=2, k=200: qty=5, k=999: qty=1.
    // After GROUP BY k → (100,2), (200,5), (999,1).
    // Join with items (iid in {100,200,300}) → (alpha, 2), (beta, 5).
    // Order is hash-routed → unstable; compare as a multiset by sorting.
    var lines: std.ArrayList([]const u8) = .empty;
    defer lines.deinit(allocator);
    var it = std.mem.tokenizeScalar(u8, got.items, '|');
    while (it.next()) |line| try lines.append(allocator, line);
    std.mem.sort([]const u8, lines.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lt);
    try std.testing.expectEqual(@as(usize, 2), lines.items.len);
    try std.testing.expectEqualStrings("alpha:2", lines.items[0]);
    try std.testing.expectEqualStrings("beta:5", lines.items[1]);
}

test "sql: subquery without alias errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    const res = runSql(allocator, db, "SELECT id FROM (SELECT * FROM t)");
    try std.testing.expectError(thindb.sql.ParseError.SqlSubqueryNeedsAlias, res);
}

test "sql: MATERIALIZED hint forces buffer even on single use" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db,
        \\WITH big AS MATERIALIZED (SELECT id FROM t WHERE k >= 200)
        \\SELECT id FROM big ORDER BY id ASC
    );
    defer q.deinit();

    // MATERIALIZED = a shared buffered boundary — which is also the default
    // under boundary semantics, so the hint is observable only through
    // results (the V2 staged path doesn't use the legacy ctx cache).
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 4, 5 }, ids.items);
}

test "sql: a materialized CTE is charged against the memory budget" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // Tiny budget: buffering 200 bigints (1600 B) must exceed 1024 B.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .query_memory_budget = 1024,
        .row_group_size = 8,
    });
    defer db.close();

    const schema = thindb.TableSchema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("big", schema, .{ .order_key = &ok, .unique = false, .row_group_size = 8 });
    var i: i64 = 0;
    while (i < 200) : (i += 1) {
        try t.insert(&.{.{ .id = i }});
        if (@mod(i + 1, 8) == 0) try t.flush();
    }
    try t.flush();

    // The MATERIALIZED CTE buffers the whole 200-row result; that
    // accumulation now reserves against the budget and must trip.
    var q = try runSql(allocator, db,
        \\WITH m AS MATERIALIZED (SELECT id FROM big)
        \\SELECT id FROM m
    );
    defer q.deinit();
    try std.testing.expectError(error.MemoryBudgetExceeded, q.next());
}

test "sql: ORDER BY releases its sort buffer budget once the result is drained" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .query_memory_budget = 1 << 20,
        .row_group_size = 8,
    });
    defer db.close();
    try seedBig(db, 200);

    var q = try runSql(allocator, db, "SELECT id FROM big ORDER BY id");
    defer q.deinit();
    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 200), rows);
    // DAG-aware eviction: the sort buffer is freed + its budget released
    // on the final (null) batch, so the query-scoped accountant is back
    // to zero once the result has been drained.
    try std.testing.expectEqual(@as(usize, 0), q.cq.ctx.accountant.?.current_bytes);
}

test "sql: GROUP BY releases its hash table budget after emitting" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .query_memory_budget = 1 << 20,
        .row_group_size = 8,
    });
    defer db.close();
    try seedBig(db, 200);

    var q = try runSql(allocator, db, "SELECT id, count(*) AS c FROM big GROUP BY id");
    defer q.deinit();
    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 200), rows);
    // The group accumulator arena is dropped + its budget released as
    // soon as the single result batch is built.
    try std.testing.expectEqual(@as(usize, 0), q.cq.ctx.accountant.?.current_bytes);
}

test "sql: a materialized CTE releases its budget after the last reader drains" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .query_memory_budget = 1 << 20,
        .row_group_size = 8,
    });
    defer db.close();
    try seedBig(db, 200);

    var q = try runSql(allocator, db,
        \\WITH m AS MATERIALIZED (SELECT id FROM big)
        \\SELECT id FROM m
    );
    defer q.deinit();
    // Legacy buffers count in ctx.materialized, V2 shared stages in
    // ctx.stage_count; the explicit MATERIALIZED yields ONE buffer either way.
    try std.testing.expectEqual(@as(u32, 1), q.cq.ctx.materialized.count() + q.cq.ctx.stage_count);
    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 200), rows);
    // Refcount eviction: with the single reader drained, the buffered
    // columns are freed and the budget handed back.
    try std.testing.expectEqual(@as(usize, 0), q.cq.ctx.accountant.?.current_bytes);
}

test "sql: NOT MATERIALIZED regenerates the CTE per reference" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db,
        \\WITH big AS NOT MATERIALIZED (SELECT k FROM t WHERE k >= 200)
        \\SELECT k FROM big JOIN big AS other ON big.k = other.k
    );
    defer q.deinit();

    // NOT MATERIALIZED waives the materialization fence (PG semantics: the
    // planner may fold/optimize freely). The structural CSE spots the
    // byte-identical bodies and shares ONE stage between the join branches —
    // same results, half the buffering of a per-reference copy.
    try std.testing.expectEqual(@as(u32, 1), q.cq.ctx.materialized.count() + q.cq.ctx.stage_count);

    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 5), rows);
}

test "sql: auto-materialize wraps a CTE referenced twice (single shared buffer)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // Same self-join shape as the inline DAG test, but without
    // NOT MATERIALIZED — the auto-detect refcount pass should wrap it
    // in a Materialize node, and compile should produce exactly ONE
    // buffer with two Readers sharing it.
    var q = try runSql(allocator, db,
        \\WITH big AS (SELECT k FROM t WHERE k >= 200)
        \\SELECT k FROM big JOIN big AS other ON big.k = other.k
    );
    defer q.deinit();

    try std.testing.expectEqual(@as(u32, 1), q.cq.ctx.materialized.count() + q.cq.ctx.stage_count);

    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 5), rows);
}

test "sql: single-use CTE materializes (boundary semantics)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db,
        \\WITH big AS (SELECT id FROM t WHERE k >= 200)
        \\SELECT id FROM big ORDER BY id ASC
    );
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 4, 5 }, ids.items);
}

test "sql: case-insensitive keywords + line comments + trailing semicolon" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    var q = try runSql(allocator, db,
        \\-- pick high-qty rows
        \\select id From t Where qty >= 30 Order By id Asc;
    );
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 4, 5 }, ids.items);
}
