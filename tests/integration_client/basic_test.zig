//! Walking-skeleton tests for the new client/server surface.
//!
//! Verifies that `thindb.local() → conn.scan(name).limit(n) → q.next()`
//! round-trips through the operator IR (encoded + decoded) and produces
//! the right batches. Setup of table data uses the existing library API
//! via `thindb.net.underlyingDb(conn)` since we don't yet have a client-
//! facing write path.

const std = @import("std");
const thindb = @import("thindb");

const schema_v1 = thindb.Schema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "qty", .type = .int },
        .{ .name = "active", .type = .boolean },
        .{ .name = "tag", .type = .string },
    },
    .order_key = &.{"id"},
    .unique = true,
};

const order_key = [_][]const u8{"id"};
const opts_v1 = thindb.TableOptions{
    .order_key = &order_key,
    .unique = true,
    .row_group_size = 4,
};

test "ir roundtrip: conn.scan returns all rows when no limit" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    // Seed via existing library API (no client-facing write path yet).
    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "c" },
    });
    try orders.flush();

    // Client-side: build, dispatch, drain.
    var q = try conn.scan("orders");
    defer q.deinit();

    var total: usize = 0;
    while (try q.next()) |batch| total += batch.row_count;
    try std.testing.expectEqual(@as(usize, 3), total);
}

test "ir roundtrip: limit truncates the stream at N rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "c" },
        .{ .id = @as(i64, 4), .qty = @as(i32, 40), .active = true, .tag = "d" },
        .{ .id = @as(i64, 5), .qty = @as(i32, 50), .active = false, .tag = "e" },
    });
    try orders.flush();

    var base = try conn.scan("orders");
    var q = try base.limit(2);
    defer q.deinit();

    var total: usize = 0;
    while (try q.next()) |batch| total += batch.row_count;
    try std.testing.expectEqual(@as(usize, 2), total);
}

test "ir error: scanning a table that doesn't exist returns TableNotFound" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    var q = try conn.scan("does_not_exist");
    defer q.deinit();

    try std.testing.expectError(thindb.net.Error.TableNotFound, q.next());
}

test "select: keeps only the named columns, in the listed order" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
    });
    try orders.flush();

    var base = try conn.scan("orders");
    var q = try base.select(&.{ "tag", "id" });
    defer q.deinit();

    var saw_batches: usize = 0;
    var total_rows: usize = 0;
    while (try q.next()) |batch| {
        saw_batches += 1;
        total_rows += batch.row_count;
        // Schema must be exactly the selected columns, in order.
        try std.testing.expectEqual(@as(usize, 2), batch.schema.len);
        try std.testing.expectEqualStrings("tag", batch.schema[0].name);
        try std.testing.expectEqualStrings("id", batch.schema[1].name);
    }
    try std.testing.expect(saw_batches > 0);
    try std.testing.expectEqual(@as(usize, 2), total_rows);
}

test "exclude: drops named columns and downstream cannot see them" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
    });
    try orders.flush();

    // schema_v1 columns: id, qty, active, tag.  Exclude active + tag.
    var base = try conn.scan("orders");
    var q = try base.exclude(&.{ "active", "tag" });
    defer q.deinit();

    while (try q.next()) |batch| {
        try std.testing.expectEqual(@as(usize, 2), batch.schema.len);
        try std.testing.expectEqualStrings("id", batch.schema[0].name);
        try std.testing.expectEqualStrings("qty", batch.schema[1].name);
    }
}

test "strict semantic: selecting an excluded column fails" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
    });
    try orders.flush();

    // Exclude "tag", then try to select it back. Server-side project
    // can't find the column → builds error from the existing exec layer.
    var base = try conn.scan("orders");
    var excluded = try base.exclude(&.{"tag"});
    var q = try excluded.select(&.{ "id", "tag" });
    defer q.deinit();

    // The build of the downstream project should fail because "tag" is
    // no longer in upstream's schema. exec.Project returns a ColumnNotFound
    // error from the exec error set.
    try std.testing.expectError(error.ColumnNotFound, q.next());
}

test "where: leaf predicate filters rows server-side" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 100), .active = true,  .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 50),  .active = false, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 200), .active = true,  .tag = "c" },
    });
    try orders.flush();

    var base = try conn.scan("orders");
    var q = try base.where(thindb.leafExpr("qty", .gt, .{ .int = 75 }));
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);

    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 3 }, ids.items);
}

test "where alias filter: same behavior under both names" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true,  .tag = "x" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "y" },
    });
    try orders.flush();

    var base = try conn.scan("orders");
    var q = try base.filter(thindb.leafExpr("active", .eq, .{ .boolean = true }));
    defer q.deinit();

    var total: usize = 0;
    while (try q.next()) |batch| total += batch.row_count;
    try std.testing.expectEqual(@as(usize, 1), total);
}

test "where + select + limit chain" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 100), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 200), .active = true, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 300), .active = true, .tag = "c" },
        .{ .id = @as(i64, 4), .qty = @as(i32, 400), .active = true, .tag = "d" },
        .{ .id = @as(i64, 5), .qty = @as(i32, 500), .active = true, .tag = "e" },
    });
    try orders.flush();

    var base = try conn.scan("orders");
    var filtered = try base.where(thindb.leafExpr("qty", .gte, .{ .int = 200 }));
    var projected = try filtered.select(&.{ "id", "tag" });
    var q = try projected.limit(2);
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |batch| {
        rows += batch.row_count;
        try std.testing.expectEqual(@as(usize, 2), batch.schema.len);
        try std.testing.expectEqualStrings("id", batch.schema[0].name);
        try std.testing.expectEqualStrings("tag", batch.schema[1].name);
    }
    try std.testing.expectEqual(@as(usize, 2), rows);
}

test "where on a string column with .eq" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "alpha" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = true, .tag = "beta" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "alpha" },
    });
    try orders.flush();

    var base = try conn.scan("orders");
    var q = try base.where(thindb.leafExpr("tag", .eq, .{ .text = "alpha" }));
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);

    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 3 }, ids.items);
}

test "orderBy: sort by single key descending" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 30), .active = true, .tag = "c" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 50), .active = true, .tag = "e" },
        .{ .id = @as(i64, 4), .qty = @as(i32, 20), .active = true, .tag = "b" },
    });
    try orders.flush();

    var base = try conn.scan("orders");
    var q = try base.orderBy(&.{.{ .col = "qty", .desc = true }});
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);

    // qty=50,30,20,10 → id=3,1,4,2
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 1, 4, 2 }, ids.items);
}

test "where + orderBy + limit chain (top-N)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 30), .active = true,  .tag = "c" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 10), .active = true,  .tag = "a" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 50), .active = false, .tag = "e" },
        .{ .id = @as(i64, 4), .qty = @as(i32, 20), .active = true,  .tag = "b" },
        .{ .id = @as(i64, 5), .qty = @as(i32, 40), .active = true,  .tag = "d" },
    });
    try orders.flush();

    var base = try conn.scan("orders");
    var filtered = try base.where(thindb.leafExpr("active", .eq, .{ .boolean = true }));
    var sorted = try filtered.orderBy(&.{.{ .col = "qty", .desc = true }});
    var q = try sorted.limit(2);
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);

    // active=true: qty 30,10,20,40 — sorted desc: 40,30,20,10 → first 2: id=5, id=1
    try std.testing.expectEqualSlices(i64, &[_]i64{ 5, 1 }, ids.items);
}

test "groupBy: sum with group, ordered desc — the canonical analytics query" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 100), .active = true,  .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 200), .active = true,  .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 50),  .active = false, .tag = "a" },
        .{ .id = @as(i64, 4), .qty = @as(i32, 75),  .active = true,  .tag = "a" },
        .{ .id = @as(i64, 5), .qty = @as(i32, 300), .active = true,  .tag = "b" },
    });
    try orders.flush();

    // Same query we walked through in the earlier server-side example:
    //   WHERE active = true
    //   GROUP BY tag
    //   SUM(qty) AS total
    //   ORDER BY total DESC
    var base = try conn.scan("orders");
    var filtered = try base.where(thindb.leafExpr("active", .eq, .{ .boolean = true }));
    var grouped = try filtered.groupBy(&.{"tag"}, &.{
        .{ .func = .sum, .col = "qty", .as = "total" },
    });
    var q = try grouped.orderBy(&.{.{ .col = "total", .desc = true }});
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), b.row_count);
    try std.testing.expectEqual(@as(usize, 2), b.schema.len);
    try std.testing.expectEqualStrings("tag", b.schema[0].name);
    try std.testing.expectEqualStrings("total", b.schema[1].name);
    // tag=b total 500, tag=a (active only) total 175
    try std.testing.expectEqualStrings("b", b.values[0].data.string.rowBytes(0));
    try std.testing.expectEqual(@as(i64, 500), b.values[1].data.bigint[0]);
    try std.testing.expectEqualStrings("a", b.values[0].data.string.rowBytes(1));
    try std.testing.expectEqual(@as(i64, 175), b.values[1].data.bigint[1]);
}

test "aggregate: global (no group keys) emits one row" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "x" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = true, .tag = "y" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "z" },
    });
    try orders.flush();

    var base = try conn.scan("orders");
    var q = try base.aggregate(&.{
        .{ .func = .count, .col = null,  .as = "n" },
        .{ .func = .sum,   .col = "qty", .as = "total" },
        .{ .func = .avg,   .col = "qty", .as = "avg_q" },
    });
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expectEqual(@as(usize, 3), b.schema.len);
    try std.testing.expectEqualStrings("n", b.schema[0].name);
    try std.testing.expectEqualStrings("total", b.schema[1].name);
    try std.testing.expectEqualStrings("avg_q", b.schema[2].name);
    try std.testing.expectEqual(@as(i64, 3), b.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 60), b.values[1].data.bigint[0]);
    try std.testing.expectEqual(@as(f64, 20.0), b.values[2].data.double[0]);
}

test "from: alias for scan; same chain works" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
    });
    try orders.flush();

    var base = try conn.from("orders");
    var q = try base.limit(1);
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
}

test "select *: empty list keeps upstream schema unchanged" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{.{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" }});
    try orders.flush();

    var base = try conn.from("orders");
    // select(&.{}) is SELECT *
    var q = try base.select(&.{});
    defer q.deinit();

    const b = (try q.next()).?;
    // Full schema still present (id, qty, active, tag).
    try std.testing.expectEqual(@as(usize, 4), b.schema.len);
    try std.testing.expectEqualStrings("id", b.schema[0].name);
    try std.testing.expectEqualStrings("tag", b.schema[3].name);
}

test "pipe: compose a sub-pipeline as a Query→Query function" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 5),   .active = true,  .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 50),  .active = false, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 500), .active = true,  .tag = "c" },
    });
    try orders.flush();

    // Reusable transform: filter active=true and pick (id, qty). Same
    // shape as the existing exec.Query.pipe — `data` is the upstream;
    // the body extends it.
    const activeIdAndQty = struct {
        fn apply(data: thindb.ClientQuery) anyerror!thindb.ClientQuery {
            var step1 = try data.where(thindb.leafExpr("active", .eq, .{ .boolean = true }));
            return step1.select(&.{ "id", "qty" });
        }
    }.apply;

    var base = try conn.from("orders");
    var piped = try base.pipe(activeIdAndQty);
    var q = try piped.limit(10);
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| {
        try ids.appendSlice(allocator, batch.values[0].data.bigint);
        try std.testing.expectEqual(@as(usize, 2), batch.schema.len);
        try std.testing.expectEqualStrings("id", batch.schema[0].name);
        try std.testing.expectEqualStrings("qty", batch.schema[1].name);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 3 }, ids.items);
}

test "select then limit chain works" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, io, tmp.dir, .{});
    defer conn.close();

    const db = thindb.net.underlyingDb(conn);
    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "c" },
    });
    try orders.flush();

    var base = try conn.scan("orders");
    var projected = try base.select(&.{"id"});
    var q = try projected.limit(2);
    defer q.deinit();

    var total: usize = 0;
    while (try q.next()) |batch| {
        total += batch.row_count;
        try std.testing.expectEqual(@as(usize, 1), batch.schema.len);
        try std.testing.expectEqualStrings("id", batch.schema[0].name);
    }
    try std.testing.expectEqual(@as(usize, 2), total);
}
