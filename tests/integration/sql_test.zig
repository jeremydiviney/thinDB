//! SQL end-to-end integration tests: parse → compile → execute.
//!
//! Each test seeds a small table, parses a SQL string into an IR Op
//! tree, compiles that tree into an executable Query (via the existing
//! local.buildServerQuery path), drains the result, and asserts the
//! output. Same architecture future tooling (EXPLAIN, network server)
//! will use.

const std = @import("std");
const thindb = @import("thindb");

const schema_t = thindb.Schema{
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

/// Parse + compile in one go. The IR arena's lifetime is tied to the
/// returned Query — operators borrow slices (column names, predicates,
/// agg specs) directly from the IR tree, so the arena must outlive the
/// Query. Callers `pair.deinit()` once they're done with results.
const RunResult = struct {
    arena: std.heap.ArenaAllocator,
    q: thindb.Query,

    pub fn deinit(self: *RunResult) void {
        self.q.deinit();
        self.arena.deinit();
    }

    pub fn next(self: *RunResult) !?thindb.Batch {
        return self.q.next();
    }

    pub fn outputSchema(self: *RunResult) []const thindb.Column {
        return self.q.outputSchema();
    }
};

fn runSql(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !RunResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), sql);
    const q = try thindb.net.buildServerQuery(allocator, db, root.*);
    return .{ .arena = arena, .q = q };
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

test "sql: IS NULL / IS NOT NULL on nullable column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const schema = thindb.Schema{
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

test "sql: literal-arg scalar function rejected — col_ref-only restriction" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    // v1 of the SQL parser only allows col_ref args in scalar calls
    // (the Compute operator's restriction — task #154 lifts it).
    const res = runSql(allocator, db, "SELECT lpad(tag, 3, '_') FROM t");
    try std.testing.expectError(thindb.sql.ParseError.SqlInvalidProjection, res);
}

test "sql: nested call rejected — col_ref-only restriction" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedT(db);

    const res = runSql(allocator, db, "SELECT length(upper(tag)) FROM t");
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

const schema_orders = thindb.Schema{
    .columns = &.{
        .{ .name = "oid", .type = .bigint },
        .{ .name = "item_id", .type = .int },
        .{ .name = "qty", .type = .int },
    },
    .order_key = &.{"oid"},
    .unique = true,
};
const schema_items = thindb.Schema{
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
    const schema_cats = thindb.Schema{
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
