//! Tests for `src/exec/exec.zig`. Brought in via the parent file's `test`
//! block, so `zig build test` discovers them.

const std = @import("std");
const exec = @import("exec.zig");
const Query = exec.Query;
const PredicateExpr = exec.PredicateExpr;
const scan = exec.scan;
const leafExpr = exec.leafExpr;

const types = @import("../types.zig");
const api = @import("../api/api.zig");

test "pipeline stats propagate through scan, filter, limit, project, sort" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };

    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .unique = true });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20) },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30) },
        .{ .id = @as(i64, 4), .qty = @as(i32, 40) },
        .{ .id = @as(i64, 5), .qty = @as(i32, 50) },
    });
    // Flush so rows live in segments and survive across the multiple
    // scans this test opens (the first scan retires the memtable, so
    // without a flush subsequent scans see an empty active memtable).
    try t.flush();

    // Scan: 5 rows total, sorted on order key
    {
        var q = try scan(allocator, t);
        defer q.deinit();
        const s = q.stats();
        try std.testing.expectEqual(@as(u64, 5), s.upper_rows);
        try std.testing.expectEqual(@as(usize, 1), s.sort_state.keys.len);
        try std.testing.expectEqualStrings("id", s.sort_state.keys[0]);
    }

    // Filter: upper bound preserved, sort state preserved
    {
        var base = try scan(allocator, t);
        var q = try base.filter(leafExpr("qty", .gt, .{ .int = 20 }));
        defer q.deinit();
        const s = q.stats();
        try std.testing.expectEqual(@as(u64, 5), s.upper_rows); // unchanged — selectivity unknown
        try std.testing.expectEqual(@as(usize, 1), s.sort_state.keys.len);
    }

    // Limit: upper bound clamped to n
    {
        var base = try scan(allocator, t);
        var q = try base.limit(2);
        defer q.deinit();
        const s = q.stats();
        try std.testing.expectEqual(@as(u64, 2), s.upper_rows);
    }

    // Project dropping the order-key column: sort state should empty out
    {
        var base = try scan(allocator, t);
        var q = try base.project(&.{"qty"});
        defer q.deinit();
        const s = q.stats();
        try std.testing.expectEqual(@as(u64, 5), s.upper_rows);
        try std.testing.expectEqual(@as(usize, 0), s.sort_state.keys.len);
    }

    // Sort by a new key: claims global sort on the new key
    {
        var base = try scan(allocator, t);
        var q = try base.orderBy(&.{.{ .col = "qty", .desc = false }});
        defer q.deinit();
        const s = q.stats();
        try std.testing.expectEqual(@as(u64, 5), s.upper_rows);
        try std.testing.expectEqual(@as(usize, 1), s.sort_state.keys.len);
        try std.testing.expectEqualStrings("qty", s.sort_state.keys[0]);
        try std.testing.expect(s.sort_state.global);
    }

    // Sort descending: claims a global sort on the key, with direction
    // recorded in descs (grouping is direction-agnostic; an ascending
    // SMJ merge guards on direction separately).
    {
        var base = try scan(allocator, t);
        var q = try base.orderBy(&.{.{ .col = "qty", .desc = true }});
        defer q.deinit();
        const s = q.stats();
        try std.testing.expectEqual(@as(usize, 1), s.sort_state.keys.len);
        try std.testing.expectEqualStrings("qty", s.sort_state.keys[0]);
        try std.testing.expectEqual(@as(usize, 1), s.sort_state.descs.len);
        try std.testing.expect(s.sort_state.descs[0]);
        try std.testing.expect(s.sort_state.global);
    }

    // Global aggregate: 1 row out
    {
        var base = try scan(allocator, t);
        var q = try base.aggregate(&.{.{ .func = .count, .as = "n" }});
        defer q.deinit();
        const s = q.stats();
        try std.testing.expectEqual(@as(u64, 1), s.upper_rows);
    }
}

test "scan reads inserted rows from memtable" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };

    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20) },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30) },
    });

    var q = try scan(allocator, t);
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), b.row_count);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, b.values[0].data.bigint);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 10, 20, 30 }, b.values[1].data.int);

    try std.testing.expect((try q.next()) == null);
}

test "scan reads across flushed segments then memtable" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };

    var db = try api.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 2 });
    defer db.close();

    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .row_group_size = 2 });
    try t.insert(&.{
        .{ .id = @as(i64, 1) },
        .{ .id = @as(i64, 2) },
        .{ .id = @as(i64, 3) },
    });
    try t.flush();

    try t.insert(&.{
        .{ .id = @as(i64, 4) },
        .{ .id = @as(i64, 5) },
    });
    try t.flush();

    try t.insert(&.{.{ .id = @as(i64, 6) }});
    // Don't flush — these stay in memtable

    var q = try scan(allocator, t);
    defer q.deinit();

    var collected: std.ArrayList(i64) = .empty;
    defer collected.deinit(allocator);
    while (try q.next()) |b| {
        try collected.appendSlice(allocator, b.values[0].data.bigint);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4, 5, 6 }, collected.items);
}

test "filter on bigint column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "qty", .type = .int } },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20) },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30) },
        .{ .id = @as(i64, 4), .qty = @as(i32, 40) },
    });

    var base = try scan(allocator, t);
    var q = try base.filter(leafExpr("id", .gt, .{ .bigint = 2 }));
    defer q.deinit();

    var collected: std.ArrayList(i64) = .empty;
    defer collected.deinit(allocator);
    while (try q.next()) |b| {
        try collected.appendSlice(allocator, b.values[0].data.bigint);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 4 }, collected.items);
}

test "project narrows column set" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .tag = "b" },
    });

    var base = try scan(allocator, t);
    var q = try base.project(&.{ "id", "tag" });
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), b.schema.len);
    try std.testing.expectEqualStrings("id", b.schema[0].name);
    try std.testing.expectEqualStrings("tag", b.schema[1].name);
    try std.testing.expectEqualStrings("a", b.values[1].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("b", b.values[1].data.string.rowBytes(1));
}

test "limit cuts off after N rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 3 });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .row_group_size = 3 });
    try t.insert(&.{
        .{ .id = @as(i64, 1) }, .{ .id = @as(i64, 2) }, .{ .id = @as(i64, 3) },
        .{ .id = @as(i64, 4) }, .{ .id = @as(i64, 5) }, .{ .id = @as(i64, 6) },
        .{ .id = @as(i64, 7) }, .{ .id = @as(i64, 8) },
    });
    try t.flush();

    var base = try scan(allocator, t);
    var q = try base.limit(5);
    defer q.deinit();

    var collected: std.ArrayList(i64) = .empty;
    defer collected.deinit(allocator);
    while (try q.next()) |b| {
        try collected.appendSlice(allocator, b.values[0].data.bigint);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4, 5 }, collected.items);
}

test "aggregate: ungrouped COUNT + SUM + MIN + MAX" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20) },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30) },
        .{ .id = @as(i64, 4), .qty = @as(i32, 40) },
    });

    var base = try scan(allocator, t);
    var q = try base.aggregate(&.{
        .{ .func = .count, .as = "n" },
        .{ .func = .sum, .col = "qty", .as = "total_qty" },
        .{ .func = .min, .col = "qty", .as = "min_qty" },
        .{ .func = .max, .col = "qty", .as = "max_qty" },
    });
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expectEqual(@as(usize, 4), b.schema.len);
    try std.testing.expectEqualStrings("n", b.schema[0].name);
    try std.testing.expectEqual(@as(i64, 4), b.values[0].data.bigint[0]); // count
    try std.testing.expectEqual(@as(i64, 100), b.values[1].data.bigint[0]); // sum
    try std.testing.expectEqual(@as(i32, 10), b.values[2].data.int[0]); // min
    try std.testing.expectEqual(@as(i32, 40), b.values[3].data.int[0]); // max
    try std.testing.expect((try q.next()) == null);
}

test "aggregate: groupBy with COUNT and SUM" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "user_id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .user_id = @as(i64, 10), .qty = @as(i32, 5) },
        .{ .id = @as(i64, 2), .user_id = @as(i64, 11), .qty = @as(i32, 7) },
        .{ .id = @as(i64, 3), .user_id = @as(i64, 10), .qty = @as(i32, 13) },
        .{ .id = @as(i64, 4), .user_id = @as(i64, 11), .qty = @as(i32, 9) },
        .{ .id = @as(i64, 5), .user_id = @as(i64, 10), .qty = @as(i32, 2) },
    });

    var base = try scan(allocator, t);
    var q = try base.groupBy(&.{"user_id"}, &.{
        .{ .func = .count, .as = "n" },
        .{ .func = .sum, .col = "qty", .as = "total_qty" },
    });
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), b.row_count);
    try std.testing.expectEqualStrings("user_id", b.schema[0].name);
    try std.testing.expectEqualStrings("n", b.schema[1].name);
    try std.testing.expectEqualStrings("total_qty", b.schema[2].name);

    var got_n_for: std.AutoHashMap(i64, i64) = .init(allocator);
    defer got_n_for.deinit();
    var got_sum_for: std.AutoHashMap(i64, i64) = .init(allocator);
    defer got_sum_for.deinit();
    for (0..b.row_count) |i| {
        const u = b.values[0].data.bigint[i];
        const n = b.values[1].data.bigint[i];
        const s = b.values[2].data.bigint[i];
        try got_n_for.put(u, n);
        try got_sum_for.put(u, s);
    }
    try std.testing.expectEqual(@as(i64, 3), got_n_for.get(10).?);
    try std.testing.expectEqual(@as(i64, 2), got_n_for.get(11).?);
    try std.testing.expectEqual(@as(i64, 20), got_sum_for.get(10).?);
    try std.testing.expectEqual(@as(i64, 16), got_sum_for.get(11).?);
}

test "aggregate: groupBy with string column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "status", .type = .string },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .status = "paid", .qty = @as(i32, 10) },
        .{ .id = @as(i64, 2), .status = "pending", .qty = @as(i32, 5) },
        .{ .id = @as(i64, 3), .status = "paid", .qty = @as(i32, 7) },
        .{ .id = @as(i64, 4), .status = "paid", .qty = @as(i32, 3) },
        .{ .id = @as(i64, 5), .status = "pending", .qty = @as(i32, 4) },
    });

    var base = try scan(allocator, t);
    var q = try base.groupBy(&.{"status"}, &.{
        .{ .func = .count, .as = "n" },
        .{ .func = .sum, .col = "qty", .as = "total" },
    });
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), b.row_count);

    var seen_paid_n: i64 = -1;
    var seen_paid_s: i64 = -1;
    var seen_pending_n: i64 = -1;
    var seen_pending_s: i64 = -1;
    for (0..b.row_count) |i| {
        const s = b.values[0].data.string.rowBytes(i);
        const n = b.values[1].data.bigint[i];
        const sum = b.values[2].data.bigint[i];
        if (std.mem.eql(u8, s, "paid")) {
            seen_paid_n = n;
            seen_paid_s = sum;
        } else if (std.mem.eql(u8, s, "pending")) {
            seen_pending_n = n;
            seen_pending_s = sum;
        }
    }
    try std.testing.expectEqual(@as(i64, 3), seen_paid_n);
    try std.testing.expectEqual(@as(i64, 20), seen_paid_s);
    try std.testing.expectEqual(@as(i64, 2), seen_pending_n);
    try std.testing.expectEqual(@as(i64, 9), seen_pending_s);
}

test "aggregate: empty input emits zeroed counters" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    // No inserts.

    var base = try scan(allocator, t);
    var q = try base.aggregate(&.{
        .{ .func = .count, .as = "n" },
        .{ .func = .sum, .col = "qty", .as = "s" },
    });
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expectEqual(@as(i64, 0), b.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 0), b.values[1].data.bigint[0]);
}

test "filter with AND" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
            .{ .name = "active", .type = .boolean },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true },
        .{ .id = @as(i64, 4), .qty = @as(i32, 40), .active = true },
    });

    var base = try scan(allocator, t);
    var q = try base.filter(.{ .@"and" = &.{
        leafExpr("active", .eq, .{ .boolean = true }),
        leafExpr("qty", .gt, .{ .int = 15 }),
    } });
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| try ids.appendSlice(allocator, b.values[0].data.bigint);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 4 }, ids.items);
}

test "filter with OR" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .tag = "a" },
        .{ .id = @as(i64, 2), .tag = "b" },
        .{ .id = @as(i64, 3), .tag = "c" },
        .{ .id = @as(i64, 4), .tag = "d" },
    });

    var base = try scan(allocator, t);
    var q = try base.filter(.{ .@"or" = &.{
        leafExpr("tag", .eq, .{ .text = "a" }),
        leafExpr("tag", .eq, .{ .text = "c" }),
    } });
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| try ids.appendSlice(allocator, b.values[0].data.bigint);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 3 }, ids.items);
}

test "filter with NOT" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20) },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30) },
    });

    const inner = leafExpr("qty", .lt, .{ .int = 25 });
    var base = try scan(allocator, t);
    var q = try base.filter(.{ .not = &inner });
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| try ids.appendSlice(allocator, b.values[0].data.bigint);
    try std.testing.expectEqualSlices(i64, &[_]i64{3}, ids.items);
}

test "filter with nested AND inside OR" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
            .{ .name = "active", .type = .boolean },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 5), .active = true },
        .{ .id = @as(i64, 2), .qty = @as(i32, 50), .active = false },
        .{ .id = @as(i64, 3), .qty = @as(i32, 50), .active = true },
        .{ .id = @as(i64, 4), .qty = @as(i32, 100), .active = false },
    });

    // (qty > 40 AND active) OR (qty = 100)
    const branch_a: [2]PredicateExpr = .{
        leafExpr("qty", .gt, .{ .int = 40 }),
        leafExpr("active", .eq, .{ .boolean = true }),
    };
    var base = try scan(allocator, t);
    var q = try base.filter(.{ .@"or" = &.{
        .{ .@"and" = &branch_a },
        leafExpr("qty", .eq, .{ .int = 100 }),
    } });
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| try ids.appendSlice(allocator, b.values[0].data.bigint);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 4 }, ids.items);
}

test "sort: orderBy single bigint column ASC" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 3), .qty = @as(i32, 30) },
        .{ .id = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .id = @as(i64, 4), .qty = @as(i32, 40) },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20) },
    });

    var base = try scan(allocator, t);
    var q = try base.orderBy(&.{.{ .col = "qty", .desc = false }});
    defer q.deinit();

    var qtys: std.ArrayList(i32) = .empty;
    defer qtys.deinit(allocator);
    while (try q.next()) |b| try qtys.appendSlice(allocator, b.values[1].data.int);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 10, 20, 30, 40 }, qtys.items);
}

test "sort: orderBy DESC" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 30) },
        .{ .id = @as(i64, 2), .qty = @as(i32, 10) },
        .{ .id = @as(i64, 3), .qty = @as(i32, 40) },
        .{ .id = @as(i64, 4), .qty = @as(i32, 20) },
    });

    var base = try scan(allocator, t);
    var q = try base.orderBy(&.{.{ .col = "qty", .desc = true }});
    defer q.deinit();

    var qtys: std.ArrayList(i32) = .empty;
    defer qtys.deinit(allocator);
    while (try q.next()) |b| try qtys.appendSlice(allocator, b.values[1].data.int);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 40, 30, 20, 10 }, qtys.items);
}

test "sort: multi-column with mixed direction" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "user", .type = .bigint },
            .{ .name = "ts", .type = .bigint },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"user"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"user"} });
    try t.insert(&.{
        .{ .user = @as(i64, 1), .ts = @as(i64, 100), .tag = "a" },
        .{ .user = @as(i64, 2), .ts = @as(i64, 50), .tag = "b" },
        .{ .user = @as(i64, 1), .ts = @as(i64, 200), .tag = "c" },
        .{ .user = @as(i64, 2), .ts = @as(i64, 75), .tag = "d" },
    });

    var base = try scan(allocator, t);
    // ORDER BY user ASC, ts DESC
    var q = try base.orderBy(&.{
        .{ .col = "user", .desc = false },
        .{ .col = "ts", .desc = true },
    });
    defer q.deinit();

    var users: std.ArrayList(i64) = .empty;
    defer users.deinit(allocator);
    var tss: std.ArrayList(i64) = .empty;
    defer tss.deinit(allocator);
    while (try q.next()) |b| {
        try users.appendSlice(allocator, b.values[0].data.bigint);
        try tss.appendSlice(allocator, b.values[1].data.bigint);
    }
    // Expected: (1,200), (1,100), (2,75), (2,50)
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 2, 2 }, users.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 200, 100, 75, 50 }, tss.items);
}

test "sort: empty input emits nothing" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });

    var base = try scan(allocator, t);
    var q = try base.orderBy(&.{.{ .col = "id" }});
    defer q.deinit();
    try std.testing.expect((try q.next()) == null);
}

test "sort: groupBy then orderBy (composed)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "user", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .user = @as(i64, 10), .qty = @as(i32, 5) },
        .{ .id = @as(i64, 2), .user = @as(i64, 20), .qty = @as(i32, 3) },
        .{ .id = @as(i64, 3), .user = @as(i64, 10), .qty = @as(i32, 7) },
        .{ .id = @as(i64, 4), .user = @as(i64, 30), .qty = @as(i32, 1) },
        .{ .id = @as(i64, 5), .user = @as(i64, 20), .qty = @as(i32, 6) },
    });

    var base = try scan(allocator, t);
    var grouped = try base.groupBy(&.{"user"}, &.{
        .{ .func = .sum, .col = "qty", .as = "total" },
    });
    var q = try grouped.orderBy(&.{.{ .col = "total", .desc = true }});
    defer q.deinit();

    var totals: std.ArrayList(i64) = .empty;
    defer totals.deinit(allocator);
    var users: std.ArrayList(i64) = .empty;
    defer users.deinit(allocator);
    while (try q.next()) |b| {
        try users.appendSlice(allocator, b.values[0].data.bigint);
        try totals.appendSlice(allocator, b.values[1].data.bigint);
    }
    // Expected (sorted by total DESC):
    //   user=10 → 5+7=12
    //   user=20 → 3+6=9
    //   user=30 → 1
    try std.testing.expectEqualSlices(i64, &[_]i64{ 12, 9, 1 }, totals.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20, 30 }, users.items);
}

test "pipe composes a chain" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .tag = "c" },
        .{ .id = @as(i64, 4), .qty = @as(i32, 40), .tag = "d" },
    });

    const filterBig = struct {
        fn apply(q: Query) anyerror!Query {
            return q.filter(leafExpr("qty", .gt, .{ .int = 15 }));
        }
    }.apply;

    var base = try scan(allocator, t);
    var piped = try base.pipe(filterBig);
    var q = try piped.project(&.{ "id", "tag" });
    defer q.deinit();

    var collected_ids: std.ArrayList(i64) = .empty;
    defer collected_ids.deinit(allocator);
    var collected_tags: std.ArrayList(u8) = .empty;
    defer collected_tags.deinit(allocator);
    while (try q.next()) |b| {
        try collected_ids.appendSlice(allocator, b.values[0].data.bigint);
        for (0..b.row_count) |i| {
            try collected_tags.appendSlice(allocator, b.values[1].data.string.rowBytes(i));
        }
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3, 4 }, collected_ids.items);
    try std.testing.expectEqualStrings("bcd", collected_tags.items);
}

test "scan: segment-level pruning skips segments excluded by leading-key predicate" {
    // Three flushes, each producing one segment with disjoint id ranges:
    // seg0=[1..5], seg1=[11..15], seg2=[21..25]. A predicate
    // `id = 22` should skip seg0 and seg1 entirely (no segment file
    // opened), and open only seg2.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .unique = true });

    try t.insert(&.{
        .{ .id = @as(i64, 1) }, .{ .id = @as(i64, 2) }, .{ .id = @as(i64, 3) },
        .{ .id = @as(i64, 4) }, .{ .id = @as(i64, 5) },
    });
    try t.flush();
    try t.insert(&.{
        .{ .id = @as(i64, 11) }, .{ .id = @as(i64, 12) }, .{ .id = @as(i64, 13) },
        .{ .id = @as(i64, 14) }, .{ .id = @as(i64, 15) },
    });
    try t.flush();
    try t.insert(&.{
        .{ .id = @as(i64, 21) }, .{ .id = @as(i64, 22) }, .{ .id = @as(i64, 23) },
        .{ .id = @as(i64, 24) }, .{ .id = @as(i64, 25) },
    });
    try t.flush();

    var base = try scan(allocator, t);
    var q = try base.filter(leafExpr("id", .eq, .{ .bigint = 22 }));
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        try ids.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{22}, ids.items);

    // Reach through Filter → Scan to verify only one segment was opened.
    // Filter wraps Scan; Filter's `upstream` points at the Scan operator.
    const filter_op: *exec.Filter = @ptrCast(@alignCast(q.ptr));
    const scan_op: *exec.Scan = @ptrCast(@alignCast(filter_op.upstream.ptr));
    try std.testing.expectEqual(@as(u32, 1), scan_op.segments_opened);
}

test "scan: segment-level pruning works for non-leading-column predicates" {
    // The order key is `id` (bigint), but the predicate filters on
    // `qty` (a separate int column). Manifest v4 stores per-column
    // stats so segments where `qty` ranges don't overlap the predicate
    // value get skipped — without opening their .dat files.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .unique = true });

    // seg0 covers qty 1..2, seg1 qty 100..200, seg2 qty 1000..2000.
    // Disjoint qty ranges → predicate `qty = 150` matches seg1 only.
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 1) },
        .{ .id = @as(i64, 2), .qty = @as(i32, 2) },
    });
    try t.flush();
    try t.insert(&.{
        .{ .id = @as(i64, 3), .qty = @as(i32, 100) },
        .{ .id = @as(i64, 4), .qty = @as(i32, 150) },
        .{ .id = @as(i64, 5), .qty = @as(i32, 200) },
    });
    try t.flush();
    try t.insert(&.{
        .{ .id = @as(i64, 6), .qty = @as(i32, 1000) },
        .{ .id = @as(i64, 7), .qty = @as(i32, 2000) },
    });
    try t.flush();

    var base = try scan(allocator, t);
    var q = try base.filter(leafExpr("qty", .eq, .{ .int = 150 }));
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        try ids.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{4}, ids.items);

    // Only seg1 should have been opened.
    const filter_op: *exec.Filter = @ptrCast(@alignCast(q.ptr));
    const scan_op: *exec.Scan = @ptrCast(@alignCast(filter_op.upstream.ptr));
    try std.testing.expectEqual(@as(u32, 1), scan_op.segments_opened);
}

test "scan: segment-level pruning works for string leading-key predicates" {
    // Same shape as the bigint test but with a string order key,
    // exercising the prefix-encoded stats path.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{.{ .name = "slug", .type = .string }},
        .order_key = &.{"slug"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"slug"}, .unique = true });

    try t.insert(&.{
        .{ .slug = @as([]const u8, "alpha") },
        .{ .slug = @as([]const u8, "bravo") },
    });
    try t.flush();
    try t.insert(&.{
        .{ .slug = @as([]const u8, "delta") },
        .{ .slug = @as([]const u8, "echo") },
    });
    try t.flush();
    try t.insert(&.{
        .{ .slug = @as([]const u8, "tango") },
        .{ .slug = @as([]const u8, "victor") },
    });
    try t.flush();

    var base = try scan(allocator, t);
    var q = try base.filter(leafExpr("slug", .eq, .{ .text = "echo" }));
    defer q.deinit();

    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(allocator);
    while (try q.next()) |b| {
        for (0..b.row_count) |i| {
            try bytes.appendSlice(allocator, b.values[0].data.string.rowBytes(i));
            try bytes.append(allocator, '|');
        }
    }
    try std.testing.expectEqualStrings("echo|", bytes.items);

    const filter_op: *exec.Filter = @ptrCast(@alignCast(q.ptr));
    const scan_op: *exec.Scan = @ptrCast(@alignCast(filter_op.upstream.ptr));
    try std.testing.expectEqual(@as(u32, 1), scan_op.segments_opened);
}

test "scan: string eq predicate prunes row groups via prefix stats" {
    // Builds a segment with multiple row groups whose name-column
    // ranges don't overlap, then runs `name = 'mike'` (which falls
    // outside the first row group's stats). The filter must still
    // return the matching row — proving prune+decode stays correct
    // for prefix-encoded string stats.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "name", .type = .string },
        },
        .order_key = &.{"name"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 2 });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"name"}, .row_group_size = 2 });

    // Three row groups, name ranges: ["alice","bob"], ["carol","dave"],
    // ["eve","frank"]. Predicate "carol" matches row group 2 only;
    // row groups 1 and 3 should be skipped by the prefix-stat prune.
    try t.insert(&.{
        .{ .id = @as(i64, 1), .name = @as([]const u8, "alice") },
        .{ .id = @as(i64, 2), .name = @as([]const u8, "bob") },
        .{ .id = @as(i64, 3), .name = @as([]const u8, "carol") },
        .{ .id = @as(i64, 4), .name = @as([]const u8, "dave") },
        .{ .id = @as(i64, 5), .name = @as([]const u8, "eve") },
        .{ .id = @as(i64, 6), .name = @as([]const u8, "frank") },
    });
    try t.flush();

    var base = try scan(allocator, t);
    var q = try base.filter(leafExpr("name", .eq, .{ .text = "carol" }));
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        try ids.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{3}, ids.items);
}

test "scan: row-group range restriction tiles to the full serial scan" {
    // Parallel-scan foundation (Step 1a): a Scan can be confined to a half-open
    // (seg,rg) range, and a set of ranges tiling [0,total) — including a mid-
    // segment split and the memtable on the last range — reproduces exactly the
    // full serial scan's rows, in order. No threads: proves the range mechanism.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 4,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(u64),
    });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .row_group_size = 4 });

    // 3 segments × 10 rows (row groups [4,4,2] each ⇒ 3 RGs/segment, 9 total),
    // then a 5-row memtable tail. Flushed in id order so stored order == id.
    var next_id: i64 = 0;
    for (0..3) |_| {
        var rows: [10]struct { id: i64 } = undefined;
        for (&rows) |*r| {
            r.id = next_id;
            next_id += 1;
        }
        try t.insert(&rows);
        try t.flush();
    }
    var tail: [5]struct { id: i64 } = undefined;
    for (&tail) |*r| {
        r.id = next_id;
        next_id += 1;
    }
    try t.insert(&tail); // stays in the memtable

    const drain = struct {
        fn run(a: std.mem.Allocator, s: *exec.Scan, out: *std.ArrayList(i64)) !void {
            var q = exec.makeQuery(a, s);
            defer q.deinit();
            while (try q.next()) |b| {
                try out.appendSlice(a, b.values[0].data.bigint[0..b.row_count]);
            }
        }
    }.run;

    // Reference: full serial scan.
    var full: std.ArrayList(i64) = .empty;
    defer full.deinit(allocator);
    try drain(allocator, try exec.Scan.allocWithProjectionLoc(allocator, t, null, null, false, null), &full);
    try std.testing.expectEqual(@as(usize, 35), full.items.len);

    // Tile: seg 0 | seg 1 + seg 2's RG0 (mid-segment end) | seg 2's RG1,2 + memtable.
    const Range = struct { ss: usize, sr: usize, es: usize, er: usize, mt: bool };
    const tiles = [_]Range{
        .{ .ss = 0, .sr = 0, .es = 1, .er = 0, .mt = false },
        .{ .ss = 1, .sr = 0, .es = 2, .er = 1, .mt = false },
        .{ .ss = 2, .sr = 1, .es = std.math.maxInt(usize), .er = 0, .mt = true },
    };
    var tiled: std.ArrayList(i64) = .empty;
    defer tiled.deinit(allocator);
    for (tiles) |r| {
        const s = try exec.Scan.allocWithProjectionLoc(allocator, t, null, null, false, null);
        s.setRange(r.ss, r.sr, r.es, r.er, r.mt);
        try drain(allocator, s, &tiled);
    }

    try std.testing.expectEqualSlices(i64, full.items, tiled.items);
}

test "scan: string range predicate prunes row groups via prefix stats" {
    // Same disjoint name ranges as the eq test; `name > 'dave'` must skip the
    // groups whose prefix max is below 'dave' and return only the matches —
    // proving range pruning (not just eq) is sound on the prefix class.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "name", .type = .string },
        },
        .order_key = &.{"name"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 2 });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"name"}, .row_group_size = 2 });

    // RGs: ["alice","bob"], ["carol","dave"], ["eve","frank"].
    try t.insert(&.{
        .{ .id = @as(i64, 1), .name = @as([]const u8, "alice") },
        .{ .id = @as(i64, 2), .name = @as([]const u8, "bob") },
        .{ .id = @as(i64, 3), .name = @as([]const u8, "carol") },
        .{ .id = @as(i64, 4), .name = @as([]const u8, "dave") },
        .{ .id = @as(i64, 5), .name = @as([]const u8, "eve") },
        .{ .id = @as(i64, 6), .name = @as([]const u8, "frank") },
    });
    try t.flush();

    var base = try scan(allocator, t);
    var q = try base.filter(leafExpr("name", .gt, .{ .text = "dave" }));
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        try ids.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
    }
    // Only "eve" and "frank" exceed "dave".
    try std.testing.expectEqualSlices(i64, &[_]i64{ 5, 6 }, ids.items);
}

test "topn: heavily-tied ORDER BY LIMIT keeps correct values (no tie churn)" {
    // 500 rows tied at k=7, then 5 at k=2. DESC LIMIT 3 must return three 7s
    // (the buffer fills with 7s, then further 7s tie the worst and are dropped
    // instead of churning the buffer); ASC LIMIT 3 must return three 2s (the
    // strictly-smaller rows still displace the tied incumbents).
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "k", .type = .int },
            .{ .name = "id", .type = .bigint },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });

    var rows: [505]struct { k: i32, id: i64 } = undefined;
    for (0..500) |i| rows[i] = .{ .k = 7, .id = @intCast(i) };
    for (500..505) |i| rows[i] = .{ .k = 2, .id = @intCast(i) };
    try t.insert(&rows);
    try t.flush();

    inline for (.{ .{ true, @as(i32, 7) }, .{ false, @as(i32, 2) } }) |c| {
        var base = try scan(allocator, t);
        var q = try base.topN(&[_]SortSpec{.{ .col = "k", .desc = c[0] }}, 3, 0);
        defer q.deinit();
        var n: usize = 0;
        while (try q.next()) |b| {
            for (0..b.row_count) |r| {
                try std.testing.expectEqual(c[1], b.values[0].data.int[r]);
                n += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 3), n);
    }
}

test "scan: float range predicate prunes row groups via order-preserving stats" {
    // Row groups with disjoint double ranges; `f > 4.5` must skip the groups
    // whose max is below it and still return the matching rows — proving the
    // float min/max (encodeFloatOrder) prune+decode is correct.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "f", .type = .double },
        },
        .order_key = &.{"f"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 2 });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"f"}, .row_group_size = 2 });

    // RGs by f: [-2.5,-1.5], [0.5,3.25], [4.0,6.5]. `f > 4.5` matches only the
    // last group's 6.5 (id 6); the first two groups are pruned.
    try t.insert(&.{
        .{ .id = @as(i64, 1), .f = @as(f64, -2.5) },
        .{ .id = @as(i64, 2), .f = @as(f64, -1.5) },
        .{ .id = @as(i64, 3), .f = @as(f64, 0.5) },
        .{ .id = @as(i64, 4), .f = @as(f64, 3.25) },
        .{ .id = @as(i64, 5), .f = @as(f64, 4.0) },
        .{ .id = @as(i64, 6), .f = @as(f64, 6.5) },
    });
    try t.flush();

    var base = try scan(allocator, t);
    var q = try base.filter(leafExpr("f", .gt, .{ .double = 4.5 }));
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| {
        try ids.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{6}, ids.items);
}

test "streaming aggregate: sorted GROUP BY produces correct per-group results" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "grp", .type = .int },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("g", schema, .{ .order_key = &.{"id"}, .unique = true });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .grp = @as(i32, 2), .v = @as(i32, 10) },
        .{ .id = @as(i64, 2), .grp = @as(i32, 1), .v = @as(i32, 20) },
        .{ .id = @as(i64, 3), .grp = @as(i32, 2), .v = @as(i32, 30) },
        .{ .id = @as(i64, 4), .grp = @as(i32, 3), .v = @as(i32, 40) },
        .{ .id = @as(i64, 5), .grp = @as(i32, 1), .v = @as(i32, 50) },
        .{ .id = @as(i64, 6), .grp = @as(i32, 2), .v = @as(i32, 60) },
    });
    try t.flush();

    // Sort by grp so equal keys are adjacent, then stream-aggregate.
    var base = try scan(allocator, t);
    var sorted = try base.orderBy(&.{.{ .col = "grp", .desc = false }});
    var q = try sorted.streamGroupBy(&.{"grp"}, &.{
        .{ .func = .count, .as = "n" },
        .{ .func = .sum, .col = "v", .as = "total" },
    });
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
    // Ascending grp order: 1, 2, 3.
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2, 3 }, grps.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3, 1 }, counts.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 70, 100, 40 }, totals.items);
}

test "cardinality: bounds propagate through filter, sort, and project" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "a", .type = .int },
            .{ .name = "b", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("cp", schema, .{ .order_key = &.{"id"}, .unique = true });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .a = @as(i32, 10), .b = @as(i32, 100) },
        .{ .id = @as(i64, 2), .a = @as(i32, 20), .b = @as(i32, 200) },
        .{ .id = @as(i64, 3), .a = @as(i32, 30), .b = @as(i32, 300) },
        .{ .id = @as(i64, 4), .a = @as(i32, 10), .b = @as(i32, 400) },
        .{ .id = @as(i64, 5), .a = @as(i32, 20), .b = @as(i32, 100) },
        .{ .id = @as(i64, 6), .a = @as(i32, 30), .b = @as(i32, 200) },
    });
    try t.flush();

    // Baseline: scan exposes a stat per column; capture them (a=3 distinct,
    // b=4 distinct → both small → exact).
    var a_c: exec.ColCard = undefined;
    var b_c: exec.ColCard = undefined;
    {
        var q = try scan(allocator, t);
        defer q.deinit();
        const s = q.stats();
        try std.testing.expectEqual(@as(usize, 3), s.column_stats.len);
        try std.testing.expect(std.meta.activeTag(s.column_stats[1].ndv) == .exact);
        try std.testing.expect(std.meta.activeTag(s.column_stats[2].ndv) == .exact);
        a_c = s.column_stats[1].ndv;
        b_c = s.column_stats[2].ndv;
    }

    // Filter preserves the bounds (filtering only shrinks distinct counts).
    {
        var base = try scan(allocator, t);
        var q = try base.filter(leafExpr("a", .gt, .{ .int = 5 }));
        defer q.deinit();
        const s = q.stats();
        try std.testing.expectEqual(a_c, s.column_stats[1].ndv);
        try std.testing.expectEqual(b_c, s.column_stats[2].ndv);
    }

    // Sort preserves the bounds (reorder only).
    {
        var base = try scan(allocator, t);
        var q = try base.orderBy(&.{.{ .col = "a", .desc = false }});
        defer q.deinit();
        const s = q.stats();
        try std.testing.expectEqual(a_c, s.column_stats[1].ndv);
        try std.testing.expectEqual(b_c, s.column_stats[2].ndv);
    }

    // Project remaps the bounds to the projected column order: [b, a].
    {
        var base = try scan(allocator, t);
        var q = try base.project(&.{ "b", "a" });
        defer q.deinit();
        const s = q.stats();
        try std.testing.expectEqual(@as(usize, 2), s.column_stats.len);
        try std.testing.expectEqual(b_c, s.column_stats[0].ndv);
        try std.testing.expectEqual(a_c, s.column_stats[1].ndv);
    }
}

test "explain: physical plan shows hash vs stream group-by and sort elision" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "grp", .type = .int },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("ep", schema, .{ .order_key = &.{"id"}, .unique = true });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .grp = @as(i32, 1), .v = @as(i32, 10) },
        .{ .id = @as(i64, 2), .grp = @as(i32, 2), .v = @as(i32, 20) },
    });
    try t.flush();

    // Hash path: scan → filter → hash group-by. `v > 15` is selective over
    // v ∈ [10,20] (not provably constant), so the Filter node survives plan-
    // time simplification and shows up in the plan.
    {
        var base = try scan(allocator, t);
        var filtered = try base.filter(leafExpr("v", .gt, .{ .int = 15 }));
        var q = try filtered.groupBy(&.{"grp"}, &.{.{ .func = .count, .as = "n" }});
        defer q.deinit();
        const plan = try q.explainPlan(allocator);
        defer allocator.free(plan);
        try std.testing.expect(std.mem.indexOf(u8, plan, "HashAggregate") != null);
        try std.testing.expect(std.mem.indexOf(u8, plan, "Filter") != null);
        try std.testing.expect(std.mem.indexOf(u8, plan, "Scan ep") != null);
        // No Sort node in the hash path.
        try std.testing.expect(std.mem.indexOf(u8, plan, "Sort") == null);
    }

    // Streaming path: sort then stream-aggregate — the Sort node is visible.
    {
        var base = try scan(allocator, t);
        var sorted = try base.orderBy(&.{.{ .col = "grp", .desc = false }});
        var q = try sorted.streamGroupBy(&.{"grp"}, &.{.{ .func = .count, .as = "n" }});
        defer q.deinit();
        const plan = try q.explainPlan(allocator);
        defer allocator.free(plan);
        try std.testing.expect(std.mem.indexOf(u8, plan, "StreamAggregate") != null);
        try std.testing.expect(std.mem.indexOf(u8, plan, "Sort") != null);
    }
}

test "cardinality: join concatenates left and right bounds" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // Distinct non-key column names so the join output has no collision.
    const lschema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "lv", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const rschema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "rv", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const l = try db.table("jl", lschema, .{ .order_key = &.{"id"}, .unique = true });
    try l.insert(&.{
        .{ .id = @as(i64, 1), .lv = @as(i32, 7) },
        .{ .id = @as(i64, 2), .lv = @as(i32, 8) },
        .{ .id = @as(i64, 3), .lv = @as(i32, 9) },
    });
    try l.flush();
    const r = try db.table("jr", rschema, .{ .order_key = &.{"id"}, .unique = true });
    try r.insert(&.{
        .{ .id = @as(i64, 1), .rv = @as(i32, 70) },
        .{ .id = @as(i64, 2), .rv = @as(i32, 80) },
    });
    try r.flush();

    // Capture each side's per-column stats independently.
    var lcards: [2]exec.ColStat = undefined;
    var rcards: [2]exec.ColStat = undefined;
    {
        var q = try scan(allocator, l);
        defer q.deinit();
        const s = q.stats();
        lcards = .{ s.column_stats[0], s.column_stats[1] };
    }
    {
        var q = try scan(allocator, r);
        defer q.deinit();
        const s = q.stats();
        rcards = .{ s.column_stats[0], s.column_stats[1] };
    }

    // Join on id → output schema is (l.id, l.v, r.v); the right join key is
    // dropped. Stats should be [l.id, l.v, r.v].
    var left = try scan(allocator, l);
    const right = try scan(allocator, r);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "id", .right = "id" }},
        .algorithm = .auto,
    });
    defer q.deinit();
    const s = q.stats();
    try std.testing.expectEqual(@as(usize, 3), s.column_stats.len);
    try std.testing.expectEqual(lcards[0], s.column_stats[0]); // l.id
    try std.testing.expectEqual(lcards[1], s.column_stats[1]); // l.v
    try std.testing.expectEqual(rcards[1], s.column_stats[2]); // r.v (right key dropped)
    // Drain so the join tears down via its executed path.
    while (try q.next()) |_| {}
}

test "aggregate: integer fast path — compound int key with count/sum/avg/min/max + nulls" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            // Two-column compound key: smallint (16b) + int (32b) packs into u128.
            .{ .name = "region", .type = .smallint },
            .{ .name = "year", .type = .int },
            // Nullable aggregated column to exercise null handling on the
            // integer-key path (the agg update is shared with the byte path).
            .{ .name = "qty", .type = .int, .nullable = true },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .region = @as(i16, -3), .year = @as(i32, 2020), .qty = @as(?i32, 10) },
        .{ .id = @as(i64, 2), .region = @as(i16, -3), .year = @as(i32, 2020), .qty = @as(?i32, null) },
        .{ .id = @as(i64, 3), .region = @as(i16, -3), .year = @as(i32, 2020), .qty = @as(?i32, 30) },
        .{ .id = @as(i64, 4), .region = @as(i16, 7), .year = @as(i32, 2021), .qty = @as(?i32, 5) },
        .{ .id = @as(i64, 5), .region = @as(i16, 7), .year = @as(i32, 2021), .qty = @as(?i32, 9) },
        // A third group sharing region=-3 but a different year — verifies the
        // compound key distinguishes on the high field, not just the low one.
        .{ .id = @as(i64, 6), .region = @as(i16, -3), .year = @as(i32, 2021), .qty = @as(?i32, 100) },
    });

    var base = try scan(allocator, t);
    var q = try base.groupBy(&.{ "region", "year" }, &.{
        .{ .func = .count, .as = "n" },
        .{ .func = .sum, .col = "qty", .as = "s" },
        .{ .func = .avg, .col = "qty", .as = "a" },
        .{ .func = .min, .col = "qty", .as = "mn" },
        .{ .func = .max, .col = "qty", .as = "mx" },
    });
    defer q.deinit();

    const Row = struct { n: i64, s: i64, a: f64, mn: i32, mx: i32 };
    var seen: std.AutoHashMap([2]i64, Row) = .init(allocator);
    defer seen.deinit();

    // Output schema: region(0), year(1), n(2), s(3), a(4), mn(5), mx(6).
    var total_rows: usize = 0;
    while (try q.next()) |b| {
        for (0..b.row_count) |i| {
            const region: i64 = b.values[0].data.smallint[i];
            const year: i64 = b.values[1].data.int[i];
            try seen.put(.{ region, year }, .{
                .n = b.values[2].data.bigint[i],
                .s = b.values[3].data.bigint[i],
                .a = b.values[4].data.double[i],
                .mn = b.values[5].data.int[i],
                .mx = b.values[6].data.int[i],
            });
            total_rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), total_rows);

    // region=-3, year=2020: qty {10, null, 30} → count(*)=3, sum=40, avg=20,
    // min=10, max=30. COUNT(*) counts the null row; sum/avg/min/max skip it.
    const g1 = seen.get(.{ -3, 2020 }).?;
    try std.testing.expectEqual(@as(i64, 3), g1.n);
    try std.testing.expectEqual(@as(i64, 40), g1.s);
    try std.testing.expectEqual(@as(f64, 20.0), g1.a);
    try std.testing.expectEqual(@as(i32, 10), g1.mn);
    try std.testing.expectEqual(@as(i32, 30), g1.mx);

    const g2 = seen.get(.{ 7, 2021 }).?;
    try std.testing.expectEqual(@as(i64, 2), g2.n);
    try std.testing.expectEqual(@as(i64, 14), g2.s);
    try std.testing.expectEqual(@as(f64, 7.0), g2.a);
    try std.testing.expectEqual(@as(i32, 5), g2.mn);
    try std.testing.expectEqual(@as(i32, 9), g2.mx);

    const g3 = seen.get(.{ -3, 2021 }).?;
    try std.testing.expectEqual(@as(i64, 1), g3.n);
    try std.testing.expectEqual(@as(i64, 100), g3.s);
    try std.testing.expectEqual(@as(i32, 100), g3.mn);
}

test "aggregate: mixed int+string GROUP BY uses the byte path correctly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "region", .type = .int },
            .{ .name = "status", .type = .string },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .region = @as(i32, 1), .status = "paid", .qty = @as(i32, 10) },
        .{ .id = @as(i64, 2), .region = @as(i32, 1), .status = "paid", .qty = @as(i32, 5) },
        .{ .id = @as(i64, 3), .region = @as(i32, 1), .status = "pending", .qty = @as(i32, 7) },
        .{ .id = @as(i64, 4), .region = @as(i32, 2), .status = "paid", .qty = @as(i32, 3) },
    });

    var base = try scan(allocator, t);
    var q = try base.groupBy(&.{ "region", "status" }, &.{
        .{ .func = .count, .as = "n" },
        .{ .func = .sum, .col = "qty", .as = "s" },
    });
    defer q.deinit();

    var rows: usize = 0;
    var matched: usize = 0;
    while (try q.next()) |b| {
        for (0..b.row_count) |i| {
            const region = b.values[0].data.int[i];
            const status = b.values[1].data.string.rowBytes(i);
            const n = b.values[2].data.bigint[i];
            const s = b.values[3].data.bigint[i];
            rows += 1;
            if (region == 1 and std.mem.eql(u8, status, "paid")) {
                try std.testing.expectEqual(@as(i64, 2), n);
                try std.testing.expectEqual(@as(i64, 15), s);
                matched += 1;
            } else if (region == 1 and std.mem.eql(u8, status, "pending")) {
                try std.testing.expectEqual(@as(i64, 1), n);
                try std.testing.expectEqual(@as(i64, 7), s);
                matched += 1;
            } else if (region == 2 and std.mem.eql(u8, status, "paid")) {
                try std.testing.expectEqual(@as(i64, 1), n);
                try std.testing.expectEqual(@as(i64, 3), s);
                matched += 1;
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 3), rows);
    try std.testing.expectEqual(@as(usize, 3), matched);
}

test "aggregate: integer fast path — ORDER BY agg LIMIT k top-k emit" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "user", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .user = @as(i64, 10), .qty = @as(i32, 5) },
        .{ .id = @as(i64, 2), .user = @as(i64, 20), .qty = @as(i32, 3) },
        .{ .id = @as(i64, 3), .user = @as(i64, 10), .qty = @as(i32, 7) },
        .{ .id = @as(i64, 4), .user = @as(i64, 30), .qty = @as(i32, 1) },
        .{ .id = @as(i64, 5), .user = @as(i64, 20), .qty = @as(i32, 6) },
        .{ .id = @as(i64, 6), .user = @as(i64, 40), .qty = @as(i32, 100) },
    });

    // Top-2 by total DESC. The fused hash aggregate keeps only k groups
    // through the int-path `appendGroupRow`; the downstream OrderBy+Limit
    // produce the exact final order.
    const ir = @import("../ir/ir.zig");
    var base = try scan(allocator, t);
    var grouped = try base.groupByTopK(
        &.{"user"},
        &.{.{ .func = .sum, .col = "qty", .as = "total" }},
        ir.Op.TopK{ .k = 2, .keys = &.{.{ .col = "total", .desc = true }} },
        null,
    );
    var q = try grouped.orderBy(&.{.{ .col = "total", .desc = true }});
    q = try q.limit(2);
    defer q.deinit();

    var totals: std.ArrayList(i64) = .empty;
    defer totals.deinit(allocator);
    var users: std.ArrayList(i64) = .empty;
    defer users.deinit(allocator);
    while (try q.next()) |b| {
        try users.appendSlice(allocator, b.values[0].data.bigint);
        try totals.appendSlice(allocator, b.values[1].data.bigint);
    }
    // user=40 → 100, user=10 → 12 are the top two.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 100, 12 }, totals.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 40, 10 }, users.items);
}

test "aggregate: integer fast path — single bigint key, no ORDER BY, plain LIMIT" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "user", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    // Enough rows + groups to span multiple batches (>1024) and exercise the
    // prefetch look-ahead across batch boundaries.
    var rows: [4000]struct { id: i64, user: i64, qty: i32 } = undefined;
    var i: usize = 0;
    while (i < rows.len) : (i += 1) {
        rows[i] = .{ .id = @intCast(i + 1), .user = @intCast(i % 700), .qty = @intCast((i % 13) + 1) };
    }
    try t.insert(&rows);

    var base = try scan(allocator, t);
    var grouped = try base.groupBy(&.{"user"}, &.{
        .{ .func = .count, .as = "n" },
        .{ .func = .sum, .col = "qty", .as = "s" },
    });
    var q = try grouped.limit(50);
    defer q.deinit();

    // Recompute the expected per-group aggregates independently.
    var exp_n: [700]i64 = [_]i64{0} ** 700;
    var exp_s: [700]i64 = [_]i64{0} ** 700;
    i = 0;
    while (i < rows.len) : (i += 1) {
        const u: usize = @intCast(rows[i].user);
        exp_n[u] += 1;
        exp_s[u] += rows[i].qty;
    }

    var got: usize = 0;
    while (try q.next()) |b| {
        for (0..b.row_count) |r| {
            const u: usize = @intCast(b.values[0].data.bigint[r]);
            try std.testing.expectEqual(exp_n[u], b.values[1].data.bigint[r]);
            try std.testing.expectEqual(exp_s[u], b.values[2].data.bigint[r]);
            got += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 50), got);
}

test "aggregate: emit_limit caps the grouped emit at the planner hint" {
    // The exec-level mechanism behind the unordered `GROUP BY … LIMIT n`
    // fusion: with emit_limit set, the hash aggregate's *own* output batch is
    // capped at the hint (group-insertion order), not just clipped downstream.
    // We assert the aggregate emits exactly `emit_limit` rows even though far
    // more groups exist, and that those rows carry exact counts (build is
    // unchanged).
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "user", .type = .bigint },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    var rows: [600]struct { id: i64, user: i64 } = undefined;
    for (&rows, 0..) |*r, i| r.* = .{ .id = @intCast(i + 1), .user = @intCast(i % 200) };
    try t.insert(&rows);

    var base = try scan(allocator, t);
    // No top_k, emit_limit = 7. The aggregate (200 groups) must emit only 7
    // rows in one batch — no downstream Limit involved.
    var q = try base.groupByTopK(&.{"user"}, &.{.{ .func = .count, .as = "n" }}, null, 7);
    defer q.deinit();

    var got: usize = 0;
    while (try q.next()) |b| {
        for (0..b.row_count) |r| {
            // 600 rows over 200 groups (user = i % 200) → each group has 3.
            try std.testing.expectEqual(@as(i64, 3), b.values[1].data.bigint[r]);
            got += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 7), got);
}

const SortSpec = @import("sort.zig").SortSpec;

/// Drain a query, collecting the (bigint) key column at `key_col_idx` and the
/// (bigint) payload column at `pay_col_idx` into the supplied lists.
fn collectBigintPair(
    allocator: std.mem.Allocator,
    q: *Query,
    key_col_idx: usize,
    pay_col_idx: usize,
    keys: *std.ArrayList(i64),
    pays: *std.ArrayList(i64),
) !void {
    while (try q.next()) |b| {
        try keys.appendSlice(allocator, b.values[key_col_idx].data.bigint);
        try pays.appendSlice(allocator, b.values[pay_col_idx].data.bigint);
    }
}

/// The bounded Top-N must return exactly what a full ORDER BY then
/// `[offset, offset+limit)` slice returns. We verify that equivalence
/// directly: run the reference (`orderBy` over the whole input, then slice the
/// emit window) and the bounded `topN`, and assert the sort-key column matches
/// element-for-element. Both paths share the identical comparator, so the key
/// values — including ties at the cut line — line up exactly. We assert on the
/// sort key (the load-bearing equivalence); the payload only carries through to
/// confirm rows aren't scrambled relative to their key.
fn assertTopNMatchesFullSort(
    allocator: std.mem.Allocator,
    t: *api.Table,
    specs: []const SortSpec,
    key_idx: usize,
    pay_idx: usize,
    limit: usize,
    offset: usize,
) !void {
    // Reference: full sort, then take the emit window by hand.
    var ref_keys: std.ArrayList(i64) = .empty;
    defer ref_keys.deinit(allocator);
    var ref_pays: std.ArrayList(i64) = .empty;
    defer ref_pays.deinit(allocator);
    {
        var base = try scan(allocator, t);
        var q = try base.orderBy(specs);
        defer q.deinit();
        try collectBigintPair(allocator, &q, key_idx, pay_idx, &ref_keys, &ref_pays);
    }
    const win_start = @min(offset, ref_keys.items.len);
    const win_end = @min(offset + limit, ref_keys.items.len);
    const exp_keys = ref_keys.items[win_start..win_end];

    // Bounded Top-N.
    var got_keys: std.ArrayList(i64) = .empty;
    defer got_keys.deinit(allocator);
    var got_pays: std.ArrayList(i64) = .empty;
    defer got_pays.deinit(allocator);
    {
        var base = try scan(allocator, t);
        var q = try base.topN(specs, limit, offset);
        defer q.deinit();
        try collectBigintPair(allocator, &q, key_idx, pay_idx, &got_keys, &got_pays);
    }

    try std.testing.expectEqualSlices(i64, exp_keys, got_keys.items);
}

test "topn: bounded path matches full sort across keys, directions, and offsets" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint }, // unique tie-breaker payload
            .{ .name = "k1", .type = .bigint }, // primary key, moderate cardinality
            .{ .name = "k2", .type = .int }, // secondary key, low cardinality
            .{ .name = "name", .type = .string }, // string key
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });

    // ~3000 rows spanning several scan batches (>1024) so the bounded path
    // prunes repeatedly and the threshold pre-filter actually engages.
    const n = 3000;
    const Row = struct { id: i64, k1: i64, k2: i32, name: []const u8 };
    var rows: [n]Row = undefined;
    var prng = std.Random.DefaultPrng.init(0x70F4C0FFEE);
    const rnd = prng.random();
    // Small string alphabet → frequent duplicate names (ties on the string key).
    const names = [_][]const u8{ "", "alpha", "beta", "beta", "gamma", "delta", "delta", "delta" };
    var i: usize = 0;
    while (i < n) : (i += 1) {
        rows[i] = .{
            .id = @intCast(i), // unique
            .k1 = @intCast(rnd.intRangeAtMost(i64, 0, 40)), // many ties
            .k2 = @intCast(rnd.intRangeAtMost(i32, 0, 5)), // heavy ties
            .name = names[rnd.intRangeLessThan(usize, 0, names.len)],
        };
    }
    try t.insert(&rows);
    // Flush so each scenario re-scans from segments (the first scan retires
    // the memtable, so without a flush later scans would see no rows).
    try t.flush();

    // Column indices: id=0, k1=1, k2=2, name=3.
    // (payload asserted-through is id where the key is bigint.)
    const Case = struct {
        specs: []const SortSpec,
        key_idx: usize,
        limit: usize,
        offset: usize,
    };
    const cases = [_]Case{
        // single int key (k1), small + large limit, with/without offset
        .{ .specs = &.{.{ .col = "k1", .desc = false }}, .key_idx = 1, .limit = 10, .offset = 0 },
        .{ .specs = &.{.{ .col = "k1", .desc = true }}, .key_idx = 1, .limit = 10, .offset = 0 },
        .{ .specs = &.{.{ .col = "k1", .desc = false }}, .key_idx = 1, .limit = 10, .offset = 25 },
        // large-keep (offset far into the stream) — the non-regression shape.
        .{ .specs = &.{.{ .col = "k1", .desc = false }}, .key_idx = 1, .limit = 10, .offset = 1000 },
        // multi-key mixed ASC/DESC: k1 ASC, k2 DESC. Assert on k1.
        .{ .specs = &.{ .{ .col = "k1", .desc = false }, .{ .col = "k2", .desc = true } }, .key_idx = 1, .limit = 20, .offset = 0 },
        .{ .specs = &.{ .{ .col = "k1", .desc = false }, .{ .col = "k2", .desc = true } }, .key_idx = 1, .limit = 20, .offset = 15 },
        // duplicate / tie keys at the boundary — k2 alone is heavily tied, so
        // the cut line at almost any limit lands inside a run of equal keys.
        .{ .specs = &.{.{ .col = "k2", .desc = false }}, .key_idx = 2, .limit = 7, .offset = 0 },
        .{ .specs = &.{.{ .col = "k2", .desc = true }}, .key_idx = 2, .limit = 50, .offset = 30 },
    };
    inline for (cases) |c| {
        // key_idx==2 is the int column — handle bigint vs int payload below.
        if (c.key_idx == 2) {
            try assertTopNMatchesFullSortInt(allocator, t, c.specs, c.limit, c.offset);
        } else {
            try assertTopNMatchesFullSort(allocator, t, c.specs, c.key_idx, 0, c.limit, c.offset);
        }
    }
}

/// Same equivalence check as `assertTopNMatchesFullSort` but for an `int`
/// (i32) sort key at column index 2 (`k2`).
fn assertTopNMatchesFullSortInt(
    allocator: std.mem.Allocator,
    t: *api.Table,
    specs: []const SortSpec,
    limit: usize,
    offset: usize,
) !void {
    var ref: std.ArrayList(i32) = .empty;
    defer ref.deinit(allocator);
    {
        var base = try scan(allocator, t);
        var q = try base.orderBy(specs);
        defer q.deinit();
        while (try q.next()) |b| try ref.appendSlice(allocator, b.values[2].data.int);
    }
    const win_start = @min(offset, ref.items.len);
    const win_end = @min(offset + limit, ref.items.len);
    const exp = ref.items[win_start..win_end];

    var got: std.ArrayList(i32) = .empty;
    defer got.deinit(allocator);
    {
        var base = try scan(allocator, t);
        var q = try base.topN(specs, limit, offset);
        defer q.deinit();
        while (try q.next()) |b| try got.appendSlice(allocator, b.values[2].data.int);
    }
    try std.testing.expectEqualSlices(i32, exp, got.items);
}

test "topn: single string key matches full sort" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "phrase", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });

    const n = 2500;
    const Row = struct { id: i64, phrase: []const u8 };
    var rows: [n]Row = undefined;
    const phrases = [_][]const u8{ "", "apple", "banana", "banana", "cherry", "date", "fig", "grape", "grape", "kiwi" };
    var prng = std.Random.DefaultPrng.init(0x5EED);
    const rnd = prng.random();
    var i: usize = 0;
    while (i < n) : (i += 1) {
        rows[i] = .{ .id = @intCast(i), .phrase = phrases[rnd.intRangeLessThan(usize, 0, phrases.len)] };
    }
    try t.insert(&rows);
    try t.flush();

    const specs = [_]SortSpec{.{ .col = "phrase", .desc = false }};
    inline for (.{
        .{ .limit = @as(usize, 10), .offset = @as(usize, 0) },
        .{ .limit = @as(usize, 10), .offset = @as(usize, 50) },
        .{ .limit = @as(usize, 100), .offset = @as(usize, 0) },
    }) |c| {
        var ref: std.ArrayList(u8) = .empty;
        defer ref.deinit(allocator);
        var ref_off: std.ArrayList(usize) = .empty;
        defer ref_off.deinit(allocator);
        {
            var base = try scan(allocator, t);
            var q = try base.orderBy(&specs);
            defer q.deinit();
            while (try q.next()) |b| {
                const sv = b.values[1].data.string;
                for (0..b.row_count) |r| {
                    try ref.appendSlice(allocator, sv.rowBytes(r));
                    try ref_off.append(allocator, ref.items.len);
                }
            }
        }
        // Emit window of sorted phrases as a list of byte slices.
        const win_start = @min(c.offset, ref_off.items.len);
        const win_end = @min(c.offset + c.limit, ref_off.items.len);

        var got: std.ArrayList(u8) = .empty;
        defer got.deinit(allocator);
        var got_off: std.ArrayList(usize) = .empty;
        defer got_off.deinit(allocator);
        {
            var base = try scan(allocator, t);
            var q = try base.topN(&specs, c.limit, c.offset);
            defer q.deinit();
            while (try q.next()) |b| {
                const sv = b.values[1].data.string;
                for (0..b.row_count) |r| {
                    try got.appendSlice(allocator, sv.rowBytes(r));
                    try got_off.append(allocator, got.items.len);
                }
            }
        }
        // Compare phrase-by-phrase across the window.
        try std.testing.expectEqual(win_end - win_start, got_off.items.len);
        var prev_ref: usize = if (win_start == 0) 0 else ref_off.items[win_start - 1];
        var prev_got: usize = 0;
        for (win_start..win_end, 0..) |ri, gi| {
            const r_slice = ref.items[prev_ref..ref_off.items[ri]];
            const g_slice = got.items[prev_got..got_off.items[gi]];
            try std.testing.expectEqualSlices(u8, r_slice, g_slice);
            prev_ref = ref_off.items[ri];
            prev_got = got_off.items[gi];
        }
    }
}

test "topn: input smaller than limit+offset emits the whole sorted input" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "k", .type = .bigint },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .k = @as(i64, 30) },
        .{ .id = @as(i64, 2), .k = @as(i64, 10) },
        .{ .id = @as(i64, 3), .k = @as(i64, 20) },
    });
    try t.flush();

    const specs = [_]SortSpec{.{ .col = "k", .desc = false }};

    // limit 10 > 3 rows: emit all three, sorted.
    {
        var base = try scan(allocator, t);
        var q = try base.topN(&specs, 10, 0);
        defer q.deinit();
        var ks: std.ArrayList(i64) = .empty;
        defer ks.deinit(allocator);
        while (try q.next()) |b| try ks.appendSlice(allocator, b.values[1].data.bigint);
        try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20, 30 }, ks.items);
    }
    // offset 5 past the end with 3 rows: emit nothing.
    {
        var base = try scan(allocator, t);
        var q = try base.topN(&specs, 10, 5);
        defer q.deinit();
        try std.testing.expect((try q.next()) == null);
    }
}

// --------------------------------------------------------------------------
// Scan-side in-place (fused) filter — eliminates the decode-copy. These prove
// the fused path emits byte-identical survivors to a known-good expected set,
// across fixed-width + string columns, for selective / none / all selectivity,
// and via a string LIKE filter. `base.filter(...)` fuses the predicate into the
// Scan; we assert `Filter.fused` so a regression that silently drops fusion
// fails loudly.
// --------------------------------------------------------------------------

const FuseRow = struct { id: i64, qty: i32, ratio: f64, tag: []const u8 };

fn collectFused(
    allocator: std.mem.Allocator,
    q: *Query,
    out_ids: *std.ArrayList(i64),
    out_qty: *std.ArrayList(i32),
    out_ratio: *std.ArrayList(f64),
    out_tags: *std.ArrayList(u8),
    out_tag_off: *std.ArrayList(usize),
) !void {
    while (try q.next()) |b| {
        const ids = b.values[0].data.bigint;
        const qtys = b.values[1].data.int;
        const ratios = b.values[2].data.double;
        const tags = b.values[3].data.string;
        for (0..b.row_count) |i| {
            try out_ids.append(allocator, ids[i]);
            try out_qty.append(allocator, qtys[i]);
            try out_ratio.append(allocator, ratios[i]);
            try out_tags.appendSlice(allocator, tags.rowBytes(i));
            try out_tag_off.append(allocator, out_tags.items.len);
        }
    }
}

test "fused filter: byte-identical survivors across selectivity (selective/none/all) + string LIKE" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
            .{ .name = "ratio", .type = .double },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .unique = true });

    const rows = [_]FuseRow{
        .{ .id = 1, .qty = 10, .ratio = 1.5, .tag = "apple" },
        .{ .id = 2, .qty = 20, .ratio = 2.5, .tag = "apricot" },
        .{ .id = 3, .qty = 30, .ratio = 3.5, .tag = "banana" },
        .{ .id = 4, .qty = 40, .ratio = 4.5, .tag = "blueberry" },
        .{ .id = 5, .qty = 50, .ratio = 5.5, .tag = "cherry" },
        .{ .id = 6, .qty = 60, .ratio = 6.5, .tag = "apex" },
    };
    inline for (rows) |r| {
        try t.insert(&.{.{ .id = r.id, .qty = r.qty, .ratio = r.ratio, .tag = r.tag }});
    }
    try t.flush();

    // Build the expected survivor set in Zig for a given predicate-on-row test,
    // then run it through the fused scan and compare column-by-column.
    const Case = struct {
        name: []const u8,
        expr: PredicateExpr,
        keep: *const fn (FuseRow) bool,
    };
    const cases = [_]Case{
        // Selective: few survivors.
        .{ .name = "selective", .expr = leafExpr("qty", .gte, .{ .int = 40 }), .keep = struct {
            fn f(r: FuseRow) bool {
                return r.qty >= 40;
            }
        }.f },
        // None: matches nothing. In-range value (qty ∈ [10,60]) absent from the
        // data so plan-time simplification can't prove it false — still fuses.
        .{ .name = "none", .expr = leafExpr("qty", .eq, .{ .int = 25 }), .keep = struct {
            fn f(r: FuseRow) bool {
                return r.qty == 25;
            }
        }.f },
        // All: matches everything. `<>` is never simplified from range alone,
        // so the fused path is exercised rather than an always-true drop.
        .{ .name = "all", .expr = leafExpr("qty", .neq, .{ .int = 999 }), .keep = struct {
            fn f(r: FuseRow) bool {
                return r.qty != 999;
            }
        }.f },
        // String LIKE on the fast-path string column.
        .{ .name = "like", .expr = .{ .like = .{ .col = "tag", .pattern = "ap%" } }, .keep = struct {
            fn f(r: FuseRow) bool {
                return std.mem.startsWith(u8, r.tag, "ap");
            }
        }.f },
    };

    inline for (cases) |c| {
        var base = try scan(allocator, t);
        var q = try base.filter(c.expr);
        defer q.deinit();

        // Confirm the predicate was actually fused into the Scan.
        const filter_op: *exec.Filter = @ptrCast(@alignCast(q.ptr));
        try std.testing.expect(filter_op.fused);

        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        var qty: std.ArrayList(i32) = .empty;
        defer qty.deinit(allocator);
        var ratio: std.ArrayList(f64) = .empty;
        defer ratio.deinit(allocator);
        var tags: std.ArrayList(u8) = .empty;
        defer tags.deinit(allocator);
        var tag_off: std.ArrayList(usize) = .empty;
        defer tag_off.deinit(allocator);
        try collectFused(allocator, &q, &ids, &qty, &ratio, &tags, &tag_off);

        // Expected.
        var e_ids: std.ArrayList(i64) = .empty;
        defer e_ids.deinit(allocator);
        var e_qty: std.ArrayList(i32) = .empty;
        defer e_qty.deinit(allocator);
        var e_ratio: std.ArrayList(f64) = .empty;
        defer e_ratio.deinit(allocator);
        var e_tags: std.ArrayList(u8) = .empty;
        defer e_tags.deinit(allocator);
        inline for (rows) |r| {
            if (c.keep(r)) {
                try e_ids.append(allocator, r.id);
                try e_qty.append(allocator, r.qty);
                try e_ratio.append(allocator, r.ratio);
                try e_tags.appendSlice(allocator, r.tag);
            }
        }

        try std.testing.expectEqualSlices(i64, e_ids.items, ids.items);
        try std.testing.expectEqualSlices(i32, e_qty.items, qty.items);
        try std.testing.expectEqualSlices(f64, e_ratio.items, ratio.items);
        try std.testing.expectEqualSlices(u8, e_tags.items, tags.items);
    }
}

test "fused filter: applies to unflushed memtable rows too" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .unique = true });

    // No flush: rows stay in the memtable. The fused Scan must still filter
    // them (Filter is a pass-through once fused).
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .id = @as(i64, 2), .qty = @as(i32, 25) },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30) },
        .{ .id = @as(i64, 4), .qty = @as(i32, 5) },
    });

    var base = try scan(allocator, t);
    var q = try base.filter(leafExpr("qty", .gte, .{ .int = 25 }));
    defer q.deinit();

    const filter_op: *exec.Filter = @ptrCast(@alignCast(q.ptr));
    try std.testing.expect(filter_op.fused);

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| try ids.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3 }, ids.items);

    try t.flush();
}

test "fused filter: tombstoned rows removed before the predicate applies" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .unique = true });

    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20) },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30) },
        .{ .id = @as(i64, 4), .qty = @as(i32, 40) },
        .{ .id = @as(i64, 5), .qty = @as(i32, 50) },
    });
    try t.flush();
    // Delete id=3 → tombstone in the segment. The fused path ANDs the
    // tombstone keep-mask into the predicate mask, so id=3 must not survive.
    _ = try t.delete(.{ .col = "id", .op = .eq, .val = .{ .bigint = 3 } });

    var base = try scan(allocator, t);
    var q = try base.filter(leafExpr("qty", .gte, .{ .int = 20 }));
    defer q.deinit();

    const filter_op: *exec.Filter = @ptrCast(@alignCast(q.ptr));
    try std.testing.expect(filter_op.fused);

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |b| try ids.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
    // qty>=20 → {2,3,4,5}; tombstone removes 3 → {2,4,5}.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 4, 5 }, ids.items);
}
