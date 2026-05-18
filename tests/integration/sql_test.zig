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
