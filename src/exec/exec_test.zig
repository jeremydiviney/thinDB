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
const core_scheduler = @import("../util/core_scheduler.zig");

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

test "parallel scan matches serial across DOP levels (with fused filter)" {
    // ParallelScan over disjoint row-group ranges + a fused WHERE must return
    // the exact same row SET as the serial scan at every DOP (order differs at
    // DOP>1 — reproducible-per-DOP — so compare as a sorted multiset). DOP=1
    // must additionally match serial order exactly. Also stresses the shared
    // block cache under concurrent worker decodes.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "v", .type = .int } },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 16,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(u64),
    });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .row_group_size = 16 });

    // 4 segments × 100 rows (~7 RGs each), then a 30-row memtable tail.
    var next_id: i64 = 0;
    for (0..4) |_| {
        var rows: [100]struct { id: i64, v: i32 } = undefined;
        for (&rows) |*r| {
            r.id = next_id;
            r.v = @intCast(@mod(next_id, 7));
            next_id += 1;
        }
        try t.insert(&rows);
        try t.flush();
    }
    var tail: [30]struct { id: i64, v: i32 } = undefined;
    for (&tail) |*r| {
        r.id = next_id;
        r.v = @intCast(@mod(next_id, 7));
        next_id += 1;
    }
    try t.insert(&tail);

    const collect = struct {
        fn run(a: std.mem.Allocator, q: *Query, out: *std.ArrayList(i64)) !void {
            defer q.deinit();
            while (try q.next()) |b| try out.appendSlice(a, b.values[0].data.bigint[0..b.row_count]);
        }
    }.run;

    // Serial reference: SELECT id FROM t WHERE id >= 150.
    var serial: std.ArrayList(i64) = .empty;
    defer serial.deinit(allocator);
    {
        var base = try scan(allocator, t);
        var q = try base.filter(leafExpr("id", .gte, .{ .bigint = 150 }));
        try collect(allocator, &q, &serial);
    }
    try std.testing.expectEqual(@as(usize, 280), serial.items.len); // 430 rows, ids 150..429

    inline for (.{ 1, 2, 4, 8 }) |dop| {
        var got: std.ArrayList(i64) = .empty;
        defer got.deinit(allocator);
        var base = try exec.ParallelScan.create(allocator, t, null, null, dop);
        var q = try base.filter(leafExpr("id", .gte, .{ .bigint = 150 }));
        try collect(allocator, &q, &got);

        try std.testing.expectEqual(serial.items.len, got.items.len);
        if (dop == 1) {
            // Single worker, whole table → identical order to serial.
            try std.testing.expectEqualSlices(i64, serial.items, got.items);
        } else {
            // Reproducible-per-DOP but interleaved: compare as a sorted multiset.
            const a = try allocator.dupe(i64, serial.items);
            defer allocator.free(a);
            const b = try allocator.dupe(i64, got.items);
            defer allocator.free(b);
            std.sort.pdq(i64, a, {}, std.sort.asc(i64));
            std.sort.pdq(i64, b, {}, std.sort.asc(i64));
            try std.testing.expectEqualSlices(i64, a, b);
        }
    }
}

test "parallel scan: the next pull frees the materialized buffer the consumer was given" {
    // Batch data lives only until the next pull, so a consumer that copies its
    // input (sort, partitioned aggregate) must not also keep the scan's copy
    // charged: each emitted survivor buffer goes back as soon as the consumer
    // asks for the next batch.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "v", .type = .int } },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 16,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(u64),
    });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .row_group_size = 16 });

    var next_id: i64 = 0;
    for (0..4) |_| {
        var rows: [100]struct { id: i64, v: i32 } = undefined;
        for (&rows) |*r| {
            r.id = next_id;
            r.v = @intCast(@mod(next_id, 7));
            next_id += 1;
        }
        try t.insert(&rows);
        try t.flush();
    }

    const survivors = 250;
    const expected = blk: {
        var ids: [survivors]i64 = undefined;
        for (&ids, 0..) |*id, i| id.* = @intCast(150 + i);
        break :blk ids;
    };
    const materialize = @intFromEnum(exec.memory.Source.materialize);

    inline for (.{ 1, 4 }) |dop| {
        // Physical tracking (the scan mints its own accountant from the table
        // budget, as on the server): freeing is what lowers the charge.
        {
            var base = try exec.ParallelScan.create(allocator, t, null, null, dop);
            const ps = exec.queryAs(exec.ParallelScan, base).?;
            var q = try base.filter(leafExpr("id", .gte, .{ .bigint = 150 }));
            defer q.deinit();
            var got: std.ArrayList(i64) = .empty;
            defer got.deinit(allocator);
            var first: usize = 0;
            var prev: usize = std.math.maxInt(usize);
            while (try q.next()) |b| {
                try std.testing.expect(ps.mode == .materialize);
                const charged = ps.acct.?.current_bytes;
                if (got.items.len == 0) first = charged;
                try std.testing.expect(charged < prev);
                prev = charged;
                try got.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
            }
            try std.testing.expect(first - ps.acct.?.current_bytes >= survivors * (@sizeOf(i64) + @sizeOf(i32)));
            std.sort.pdq(i64, got.items, {}, std.sort.asc(i64));
            try std.testing.expectEqualSlices(i64, &expected, got.items);
        }
        // Reserved accounting: the `.materialize` reservation steps down with
        // each pull and reaches zero at the end of the stream.
        {
            var acct = exec.memory.MemoryAccountant.init(64 << 20);
            var base = try exec.ParallelScan.create(allocator, t, &acct, null, dop);
            var q = try base.filter(leafExpr("id", .gte, .{ .bigint = 150 }));
            defer q.deinit();
            var rows: usize = 0;
            var batches: usize = 0;
            var prev: usize = std.math.maxInt(usize);
            while (try q.next()) |b| {
                const charged = acct.by_source[materialize];
                try std.testing.expect(charged > 0 and charged < prev);
                prev = charged;
                rows += b.row_count;
                batches += 1;
            }
            try std.testing.expectEqual(@as(usize, survivors), rows);
            if (dop > 1) try std.testing.expect(batches > 1);
            try std.testing.expectEqual(@as(usize, 0), acct.by_source[materialize]);
        }
        // A consumer that stops early (LIMIT) leaves the rest to deinit.
        {
            var acct = exec.memory.MemoryAccountant.init(64 << 20);
            {
                var base = try exec.ParallelScan.create(allocator, t, &acct, null, dop);
                var q = try base.filter(leafExpr("id", .gte, .{ .bigint = 150 }));
                defer q.deinit();
                _ = try q.next();
                _ = try q.next();
            }
            try std.testing.expectEqual(@as(usize, 0), acct.current_bytes);
        }
    }
}

test "parallel scan: a filtered drain past its wave bound hands over what it has and resumes" {
    // A filter that keeps most of a table must not park every survivor in
    // the scan's buffers before the consumer reads a row: once the buffered
    // chunks pass the bound the workers stop claiming, the consumer takes
    // those chunks, and the drain picks up at the next one. Same rows, same
    // order as the one-wave drain.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "v", .type = .int } },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 16,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(u64),
    });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .row_group_size = 16 });

    var next_id: i64 = 0;
    for (0..4) |_| {
        var rows: [100]struct { id: i64, v: i32 } = undefined;
        for (&rows) |*r| {
            r.id = next_id;
            r.v = @intCast(@mod(next_id, 7));
            next_id += 1;
        }
        try t.insert(&rows);
        try t.flush();
    }
    const survivors = 350;
    const materialize = @intFromEnum(exec.memory.Source.materialize);

    inline for (.{ 1, 4 }) |dop| {
        // A scan with no filter streams, so it buffers nothing ahead of its
        // consumer; a filtered one reports a wave: a chunk per thread on top
        // of its bound.
        {
            var plain = try exec.ParallelScan.create(allocator, t, null, null, dop);
            defer plain.deinit();
            try std.testing.expectEqual(exec.Buffered{}, plain.stats().buffered);
        }
        // A filter the column's range proves empty never pulls the scan, so
        // no wave is ever held behind it.
        {
            var base = try exec.ParallelScan.create(allocator, t, null, null, dop);
            var q = try base.filter(leafExpr("id", .lt, .{ .bigint = 0 }));
            defer q.deinit();
            const st = q.stats();
            try std.testing.expectEqual(@as(u64, 0), st.upper_rows);
            try std.testing.expectEqual(exec.Buffered{}, st.buffered);
            try std.testing.expect((try q.next()) == null);
        }
        var one_wave: std.ArrayList(i64) = .empty;
        defer one_wave.deinit(allocator);
        var one_wave_charge: usize = 0;
        {
            var acct = exec.memory.MemoryAccountant.init(64 << 20);
            var base = try exec.ParallelScan.create(allocator, t, &acct, null, dop);
            const ps = exec.queryAs(exec.ParallelScan, base).?;
            var q = try base.filter(leafExpr("id", .gte, .{ .bigint = 50 }));
            defer q.deinit();
            const st = q.stats();
            try std.testing.expectEqual(@min(st.upper_rows, (st.upper_rows / ps.workers.len + 1) * ps.n_threads), st.buffered.rows);
            try std.testing.expectEqual(@as(u64, ps.drain_wave_bytes), st.buffered.bytes);
            try std.testing.expect(st.buffered.rows > 0);
            while (try q.next()) |b| {
                try std.testing.expectEqual(ps.wbufs.len, ps.drained_chunks);
                one_wave_charge = @max(one_wave_charge, acct.by_source[materialize]);
                try one_wave.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
            }
        }
        try std.testing.expectEqual(@as(usize, survivors), one_wave.items.len);

        var waves: std.ArrayList(i64) = .empty;
        defer waves.deinit(allocator);
        var acct = exec.memory.MemoryAccountant.init(64 << 20);
        var base = try exec.ParallelScan.create(allocator, t, &acct, null, dop);
        const ps = exec.queryAs(exec.ParallelScan, base).?;
        // Any chunk with a survivor ends its worker's wave.
        ps.drain_wave_bytes = 1;
        var q = try base.filter(leafExpr("id", .gte, .{ .bigint = 50 }));
        defer q.deinit();
        var wave_charge: usize = 0;
        var stops: usize = 0;
        var drained: usize = 0;
        while (try q.next()) |b| {
            if (ps.drained_chunks != drained) {
                drained = ps.drained_chunks;
                stops += 1;
            }
            wave_charge = @max(wave_charge, acct.by_source[materialize]);
            try waves.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
        }
        try std.testing.expectEqual(ps.wbufs.len, ps.drained_chunks);
        try std.testing.expect(stops > 1);
        try std.testing.expect(wave_charge < one_wave_charge);
        try std.testing.expectEqual(@as(usize, 0), acct.by_source[materialize]);
        try std.testing.expectEqualSlices(i64, one_wave.items, waves.items);
    }
}

// Issue #502. A column only the fused filter reads is viewed for the mask and
// never gathered: once the projection above declares its reads, the scan
// stops emitting it. `tag <> ''` takes the block-sourced filter, `tag IS NOT
// NULL` the borrowed-view one; the memtable tail goes through neither.
test "scan: a column only its fused filter reads is not gathered" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "tag", .type = .string, .nullable = true },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 16,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(u64),
    });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .row_group_size = 16 });

    const Row = struct { id: i64, tag: ?[]const u8, v: i32 };
    const tags = [_]?[]const u8{ null, "", "red", "green", "blue" };
    const total = 430;
    var next_id: i64 = 0;
    for (0..5) |part| {
        var rows: [100]Row = undefined;
        const n: usize = if (part < 4) 100 else 30;
        for (rows[0..n]) |*r| {
            r.* = .{ .id = next_id, .tag = tags[@intCast(@mod(next_id, 5))], .v = @intCast(@mod(next_id, 7)) };
            next_id += 1;
        }
        try t.insert(rows[0..n]);
        if (part < 4) try t.flush();
    }

    const cases = .{
        .{ .expr = leafExpr("tag", .neq, .{ .text = "" }), .keeps_from = 2, .guided = true },
        .{ .expr = exec.isNotNullExpr("tag"), .keeps_from = 1, .guided = false },
    };
    const projections = .{ &[_][]const u8{"id"}, &[_][]const u8{ "v", "id" } };

    inline for (cases) |case| {
        var want: std.ArrayList(i64) = .empty;
        defer want.deinit(allocator);
        for (0..total) |id| {
            if (id % 5 >= case.keeps_from) try want.append(allocator, @intCast(id));
        }
        inline for (projections) |keep| {
            inline for (.{ 0, 1, 4 }) |dop| {
                var base = if (dop == 0) try scan(allocator, t) else try exec.ParallelScan.create(allocator, t, null, null, dop);
                const leaf: *exec.Scan = if (dop == 0) exec.queryAs(exec.Scan, base).? else exec.queryAs(exec.ParallelScan, base).?.workers[0].segment;
                var filtered = try base.filter(case.expr);
                var q = try filtered.project(keep);
                defer q.deinit();

                var got: std.ArrayList(i64) = .empty;
                defer got.deinit(allocator);
                while (try q.next()) |b| {
                    try std.testing.expectEqual(keep.len, b.values.len);
                    const id_col = types.findColumn(b.schema, "id").?;
                    const ids = b.values[id_col].data.bigint[0..b.row_count];
                    if (keep.len == 2) {
                        for (ids, b.values[0].data.int[0..b.row_count]) |id, v| try std.testing.expectEqual(@as(i32, @intCast(@mod(id, 7))), v);
                    }
                    try got.appendSlice(allocator, ids);
                }
                std.sort.pdq(i64, got.items, {}, std.sort.asc(i64));
                try std.testing.expectEqualSlices(i64, want.items, got.items);

                try std.testing.expectEqual(keep.len, leaf.out_phys.len);
                try std.testing.expectEqualSlices(usize, &.{1}, leaf.filter_phys);
                // A parallel worker has released its survivor buffers by now.
                if (leaf.filtered) |gathered| try std.testing.expectEqual(keep.len, gathered.len);
                try std.testing.expectEqual(case.guided, leaf.rgs_guided > 0);
            }
        }
    }
}

// A window keeps its input in one block arena per column, so a column holds
// the buffers it is using and nothing else. A bump arena also kept every
// buffer the column had outgrown, in nodes larger than the column asked for.
test "window: an accumulated column holds only its live buffers" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const ir = @import("../ir/ir.zig");
    const Window = @import("window.zig").Window;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "tag", .type = .string, .nullable = true },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 16,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(u64),
    });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .row_group_size = 16 });

    const Row = struct { id: i64, tag: ?[]const u8, v: i32 };
    const tags = [_]?[]const u8{ null, "", "red", "green", "blue" };
    const total = 2000;
    var next_id: i64 = 0;
    for (0..20) |_| {
        var rows: [100]Row = undefined;
        for (&rows) |*r| {
            r.* = .{ .id = next_id, .tag = tags[@intCast(@mod(next_id, 5))], .v = @intCast(@mod(next_id, 7)) };
            next_id += 1;
        }
        try t.insert(&rows);
        try t.flush();
    }

    const base = try scan(allocator, t);
    var q = try base.window(
        &.{.{ .partition_by = &.{}, .order_by = &.{.{ .col = "id" }}, .frame = ir.Frame.default_with_order }},
        &.{.{ .spec_idx = 0, .func = .row_number, .args = &.{}, .ignore_nulls = false, .output_name = "rn" }},
        1,
    );
    defer q.deinit();
    const win = exec.queryAs(Window, q).?;
    try win.ensureDrained();
    try std.testing.expectEqual(@as(u64, total), win.accumulated_rows);

    for (win.accumulated, win.acc_arenas) |store, arena| {
        try std.testing.expectEqual(@as(usize, total), store.rowCount());
        try std.testing.expectEqual(store.heldBytes(), arena.queryCapacity());
    }

    var rows: usize = 0;
    while (try q.next()) |batch| {
        for (0..batch.row_count) |i| {
            try std.testing.expectEqual(@as(i64, @intCast(rows + i)), batch.values[0].data.bigint[i]);
            try std.testing.expectEqual(@as(i64, @intCast(rows + i + 1)), batch.values[3].data.bigint[i]);
        }
        rows += batch.row_count;
    }
    try std.testing.expectEqual(@as(usize, total), rows);
}

const SlotHolder = struct {
    sched: *core_scheduler.CoreScheduler,
    release: *std.atomic.Value(bool),
    reported: *std.atomic.Value(usize),
    io: std.Io,

    fn run(self: SlotHolder) void {
        var lease = self.sched.tryAcquire();
        defer lease.release();
        _ = self.reported.fetchAdd(1, .release);
        if (!lease.owns) return;
        while (!self.release.load(.acquire)) std.Io.sleep(self.io, .fromMilliseconds(1), .awake) catch {};
    }
};

const Watchdog = struct {
    release: *std.atomic.Value(bool),
    io: std.Io,
    done: std.atomic.Value(bool) = .init(false),
    fired: std.atomic.Value(bool) = .init(false),

    fn run(self: *Watchdog) void {
        var waited_ms: u32 = 0;
        while (!self.done.load(.acquire)) : (waited_ms += 1) {
            if (waited_ms == 5000) {
                self.fired.store(true, .release);
                self.release.store(true, .release);
                return;
            }
            std.Io.sleep(self.io, .fromMilliseconds(1), .awake) catch {};
        }
    }
};

test "a parallel scan finishes while every other core slot is leased" {
    // The other slots belong to threads waiting on something else, as other
    // statements' connection threads do while they join their own workers. A
    // worker that blocks for a slot while its statement's calling thread holds
    // one never finishes, and neither does this scan's join.
    const sched = core_scheduler.global();
    if (sched.disabled or sched.capacity() == 0) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "v", .type = .int } },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 1024,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(u64),
    });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .row_group_size = 1024 });

    // Chunks heavy enough that the spawned workers claim some before the
    // calling thread has drained them all.
    var next_id: i64 = 0;
    for (0..16) |_| {
        for (0..8) |_| {
            var rows: [1024]struct { id: i64, v: i32 } = undefined;
            for (&rows) |*r| {
                r.id = next_id;
                r.v = @intCast(@mod(next_id, 7));
                next_id += 1;
            }
            try t.insert(&rows);
        }
        try t.flush();
    }

    var own = sched.acquire();
    defer own.release();
    var release = std.atomic.Value(bool).init(false);
    var reported = std.atomic.Value(usize).init(0);
    const holders = try allocator.alloc(std.Thread, sched.capacity());
    defer allocator.free(holders);
    var spawned: usize = 0;
    defer {
        release.store(true, .release);
        for (holders[0..spawned]) |h| h.join();
    }
    for (holders) |*h| {
        h.* = try std.Thread.spawn(.{}, SlotHolder.run, .{SlotHolder{ .sched = sched, .release = &release, .reported = &reported, .io = io }});
        spawned += 1;
    }
    while (reported.load(.acquire) < spawned) std.Thread.yield() catch {};

    var watchdog: Watchdog = .{ .release = &release, .io = io };
    const watch = try std.Thread.spawn(.{}, Watchdog.run, .{&watchdog});
    var rows: usize = 0;
    {
        // A fused filter routes the scan through its work-stealing drain.
        var base = try exec.ParallelScan.create(allocator, t, null, null, 4);
        var q = try base.filter(leafExpr("v", .gte, .{ .int = 1 }));
        defer q.deinit();
        while (try q.next()) |b| rows += b.row_count;
    }
    watchdog.done.store(true, .release);
    watch.join();

    try std.testing.expect(!watchdog.fired.load(.acquire));
    const total_rows = 16 * 8 * 1024;
    try std.testing.expectEqual(@as(usize, total_rows - (total_rows + 6) / 7), rows);
}

test "parallel scan matches serial — byte-skewed string row groups" {
    // Exercises the byte-aware partition: a string column whose row groups vary
    // ~250× in byte size. Equal-row-group spans would carry wildly unequal work;
    // byte-aware spans cut on bytes. Either way the materialized SET must equal
    // the serial scan at every DOP. Also stresses deep-copy of long survivors.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "s", .type = .string } },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 16,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(u64),
    });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .row_group_size = 16 });

    const short: []const u8 = "z";
    const long: []const u8 = "z" ** 250;
    // 128 rows → 8 row groups of 16. First 4 short, last 4 long → ~250× byte
    // skew across the segment's row groups. Flush, then a 16-row memtable tail.
    var rows: [128]struct { id: i64, s: []const u8 } = undefined;
    for (&rows, 0..) |*r, i| {
        r.id = @intCast(i);
        r.s = if (i < 64) short else long;
    }
    try t.insert(&rows);
    try t.flush();
    var tail: [16]struct { id: i64, s: []const u8 } = undefined;
    for (&tail, 0..) |*r, i| {
        r.id = @intCast(128 + i);
        r.s = short;
    }
    try t.insert(&tail);

    const collect = struct {
        fn run(a: std.mem.Allocator, q: *Query, out: *std.ArrayList(i64)) !void {
            defer q.deinit();
            while (try q.next()) |b| try out.appendSlice(a, b.values[0].data.bigint[0..b.row_count]);
        }
    }.run;

    // Serial reference: SELECT id FROM t WHERE s <> '' (all 144 rows match).
    var serial: std.ArrayList(i64) = .empty;
    defer serial.deinit(allocator);
    {
        var base = try scan(allocator, t);
        var q = try base.filter(leafExpr("s", .neq, .{ .text = "" }));
        try collect(allocator, &q, &serial);
    }
    try std.testing.expectEqual(@as(usize, 144), serial.items.len);

    inline for (.{ 1, 2, 4, 8 }) |dop| {
        var got: std.ArrayList(i64) = .empty;
        defer got.deinit(allocator);
        var base = try exec.ParallelScan.create(allocator, t, null, null, dop);
        var q = try base.filter(leafExpr("s", .neq, .{ .text = "" }));
        try collect(allocator, &q, &got);

        try std.testing.expectEqual(serial.items.len, got.items.len);
        const a = try allocator.dupe(i64, serial.items);
        defer allocator.free(a);
        const b = try allocator.dupe(i64, got.items);
        defer allocator.free(b);
        std.sort.pdq(i64, a, {}, std.sort.asc(i64));
        std.sort.pdq(i64, b, {}, std.sort.asc(i64));
        try std.testing.expectEqualSlices(i64, a, b);
    }
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

test "topn: one oversized batch holds only the rows it can emit (issue #395)" {
    const allocator = std.testing.allocator;
    const SingleBatchSource = @import("single_batch.zig").SingleBatchSource;
    const storage = @import("../storage/storage.zig");
    const n: usize = 50_000;
    const limit: usize = 10;
    const offset: usize = 5;
    const keys = try allocator.alloc(i64, n);
    defer allocator.free(keys);
    const ids = try allocator.alloc(i64, n);
    defer allocator.free(ids);
    const valid = try allocator.alloc(u8, (n + 7) / 8);
    defer allocator.free(valid);
    @memset(valid, 0);
    var prng = std.Random.DefaultPrng.init(0x395);
    const rnd = prng.random();
    for (keys, ids, 0..) |*k, *id, i| {
        k.* = rnd.intRangeAtMost(i64, 0, 999);
        id.* = @intCast(i);
        if (i % 97 != 0) storage.column.setValidBit(valid, i, true);
    }
    const schema = [_]types.Column{
        .{ .name = "k", .type = .bigint, .nullable = true },
        .{ .name = "id", .type = .bigint },
    };
    const views = [_]storage.ColumnView{
        .{ .data = .{ .bigint = keys }, .nulls = valid },
        .{ .data = .{ .bigint = ids }, .nulls = null },
    };
    const row_bytes = exec.memory.estimateRowBytes(&schema);

    const Reference = struct {
        keys: []const i64,
        valid: []const u8,
        desc: bool,

        fn lessThan(ctx: @This(), a: usize, b: usize) bool {
            const av = storage.column.isValidBit(ctx.valid, a);
            const bv = storage.column.isValidBit(ctx.valid, b);
            if (av != bv) return av == ctx.desc;
            if (av and ctx.keys[a] != ctx.keys[b]) return (ctx.keys[a] < ctx.keys[b]) != ctx.desc;
            return a < b;
        }
    };
    const order = try allocator.alloc(usize, n);
    defer allocator.free(order);

    // DESC puts NULL keys last; ASC puts them first, and more of them than
    // are kept.
    inline for (.{ true, false }) |desc| {
        for (order, 0..) |*o, i| o.* = i;
        std.sort.pdq(usize, order, Reference{ .keys = keys, .valid = valid, .desc = desc }, Reference.lessThan);

        // Room for the kept rows only, far short of the batch.
        var acct = exec.memory.MemoryAccountant.init(2 * (limit + offset) * row_bytes);
        var source = try SingleBatchSource.create(allocator, .{ .schema = &schema, .values = &views, .row_count = n });
        source.resources = &acct;
        var q = try source.topN(&.{ .{ .col = "k", .desc = desc }, .{ .col = "id", .desc = false } }, limit, offset);
        var got: std.ArrayList(i64) = .empty;
        defer got.deinit(allocator);
        {
            defer q.deinit();
            while (try q.next()) |b| try got.appendSlice(allocator, b.values[1].data.bigint[0..b.row_count]);
        }
        try std.testing.expectEqual(limit, got.items.len);
        for (order[offset .. offset + limit], got.items) |want, id| try std.testing.expectEqual(@as(i64, @intCast(want)), id);
        try std.testing.expect(acct.peak_bytes <= (limit + offset) * row_bytes);
        try std.testing.expectEqual(@as(usize, 0), acct.current_bytes);
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

test "fused filter: guided IN-list, OR and NOT IN shapes stay block-sourced and byte-identical" {
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

    // `col IN (...)` parses to an OR of equality leaves; NOT IN over a
    // subquery materializes to `.in_set` with negate.
    const tag_in = [_]PredicateExpr{ leafExpr("tag", .eq, .{ .text = "apple" }), leafExpr("tag", .eq, .{ .text = "cherry" }), leafExpr("tag", .eq, .{ .text = "zzz" }) };
    const qty_in = [_]PredicateExpr{ leafExpr("qty", .eq, .{ .int = 20 }), leafExpr("qty", .eq, .{ .int = 60 }), leafExpr("qty", .eq, .{ .int = 999 }) };
    const tag_in3 = [_]PredicateExpr{ leafExpr("tag", .eq, .{ .text = "apple" }), leafExpr("tag", .eq, .{ .text = "banana" }), leafExpr("tag", .eq, .{ .text = "apex" }) };
    const and_with_or = [_]PredicateExpr{ leafExpr("qty", .gte, .{ .int = 20 }), .{ .@"or" = &tag_in3 } };
    const mixed_or = [_]PredicateExpr{ leafExpr("qty", .eq, .{ .int = 10 }), .{ .like = .{ .col = "tag", .pattern = "ch%" } } };
    const bare_or = [_]PredicateExpr{ leafExpr("qty", .lt, .{ .int = 20 }), leafExpr("qty", .gt, .{ .int = 50 }) };
    const not_in_tags = [_]types.Value{ .{ .text = "apple" }, .{ .text = "banana" } };
    const not_in_ids = [_]types.Value{ .{ .bigint = 1 }, .{ .bigint = 2 } };
    const in_ids = [_]types.Value{ .{ .bigint = 4 }, .{ .bigint = 6 }, .{ .bigint = 40 } };

    const Case = struct {
        name: []const u8,
        expr: PredicateExpr,
        keep: *const fn (FuseRow) bool,
    };
    const cases = [_]Case{
        .{ .name = "string IN", .expr = .{ .@"or" = &tag_in }, .keep = struct {
            fn f(r: FuseRow) bool {
                return std.mem.eql(u8, r.tag, "apple") or std.mem.eql(u8, r.tag, "cherry");
            }
        }.f },
        .{ .name = "int IN", .expr = .{ .@"or" = &qty_in }, .keep = struct {
            fn f(r: FuseRow) bool {
                return r.qty == 20 or r.qty == 60;
            }
        }.f },
        .{ .name = "AND with IN child", .expr = .{ .@"and" = &and_with_or }, .keep = struct {
            fn f(r: FuseRow) bool {
                return r.qty >= 20 and (std.mem.eql(u8, r.tag, "apple") or std.mem.eql(u8, r.tag, "banana") or std.mem.eql(u8, r.tag, "apex"));
            }
        }.f },
        .{ .name = "OR of leaf and LIKE", .expr = .{ .@"or" = &mixed_or }, .keep = struct {
            fn f(r: FuseRow) bool {
                return r.qty == 10 or std.mem.startsWith(u8, r.tag, "ch");
            }
        }.f },
        .{ .name = "bare OR of ranges", .expr = .{ .@"or" = &bare_or }, .keep = struct {
            fn f(r: FuseRow) bool {
                return r.qty < 20 or r.qty > 50;
            }
        }.f },
        .{ .name = "string NOT IN", .expr = .{ .in_set = .{ .col = "tag", .values = &not_in_tags, .negate = true } }, .keep = struct {
            fn f(r: FuseRow) bool {
                return !(std.mem.eql(u8, r.tag, "apple") or std.mem.eql(u8, r.tag, "banana"));
            }
        }.f },
        .{ .name = "bigint NOT IN", .expr = .{ .in_set = .{ .col = "id", .values = &not_in_ids, .negate = true } }, .keep = struct {
            fn f(r: FuseRow) bool {
                return r.id != 1 and r.id != 2;
            }
        }.f },
        .{ .name = "bigint IN set", .expr = .{ .in_set = .{ .col = "id", .values = &in_ids, .negate = false } }, .keep = struct {
            fn f(r: FuseRow) bool {
                return r.id == 4 or r.id == 6;
            }
        }.f },
    };

    inline for (cases) |c| {
        var base = try scan(allocator, t);
        var q = try base.filter(c.expr);
        defer q.deinit();

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

        // Every shape above must take the block-sourced path, not the
        // decode-then-filter fallback.
        const s: *exec.Scan = @ptrCast(@alignCast(filter_op.upstream.ptr));
        try std.testing.expect(s.rgs_guided > 0);

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

test "parallel scan + fused projection compute matches serial" {
    // A row-local scalar projection (length(s)) fused into the parallel workers
    // must produce the same (filtered, computed) rows as the serial
    // scan→filter→compute path at every DOP. Exercises the per-worker Compute
    // isolation (own program + scratch) and the materialize/concat path over a
    // widened output schema.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "s", .type = .string } },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 16,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(u64),
    });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .row_group_size = 16 });

    // s length cycles 1..5 (so length() is verifiable), every 7th row empty so
    // the `s <> ''` filter has work. 128 rows flushed + a 16-row memtable tail.
    const pool = [_][]const u8{ "x", "xx", "xxx", "xxxx", "xxxxx" };
    var rows: [128]struct { id: i64, s: []const u8 } = undefined;
    for (&rows, 0..) |*r, i| {
        r.id = @intCast(i);
        r.s = if (i % 7 == 0) "" else pool[i % 5];
    }
    try t.insert(&rows);
    try t.flush();
    var tail: [16]struct { id: i64, s: []const u8 } = undefined;
    for (&tail, 0..) |*r, i| {
        r.id = @intCast(128 + i);
        r.s = if (i % 3 == 0) "" else pool[i % 5];
    }
    try t.insert(&tail);

    const derived = [_]@import("compute.zig").Derived{
        .{ .name = "l", .expr = .{ .call = .{ .fn_name = "length", .args = &.{.{ .col_ref = "s" }} } } },
    };

    const collectL = struct {
        fn run(a: std.mem.Allocator, q: *Query, out: *std.ArrayList(i32)) !void {
            defer q.deinit();
            while (try q.next()) |b| try out.appendSlice(a, b.values[2].data.int[0..b.row_count]);
        }
    }.run;

    // Serial reference: scan → filter(s<>'') → compute(l = length(s)).
    var serial: std.ArrayList(i32) = .empty;
    defer serial.deinit(allocator);
    {
        var base = try scan(allocator, t);
        var filtered = try base.filter(leafExpr("s", .neq, .{ .text = "" }));
        var q = try filtered.compute(&derived);
        try collectL(allocator, &q, &serial);
    }
    try std.testing.expect(serial.items.len > 0);

    inline for (.{ 1, 2, 4, 8 }) |dop| {
        var got: std.ArrayList(i32) = .empty;
        defer got.deinit(allocator);
        var base = try exec.ParallelScan.create(allocator, t, null, null, dop);
        var q = try base.filter(leafExpr("s", .neq, .{ .text = "" }));
        const fused = try q.tryFuseCompute(&derived);
        try std.testing.expect(fused); // length(s) is row-local → must fuse
        try collectL(allocator, &q, &got);

        try std.testing.expectEqual(serial.items.len, got.items.len);
        const a = try allocator.dupe(i32, serial.items);
        defer allocator.free(a);
        const b = try allocator.dupe(i32, got.items);
        defer allocator.free(b);
        std.sort.pdq(i32, a, {}, std.sort.asc(i32));
        std.sort.pdq(i32, b, {}, std.sort.asc(i32));
        try std.testing.expectEqualSlices(i32, a, b);
    }
}

test "parallel scan compute split: safe derived fused, unsafe (CASE) stays serial" {
    // A mixed projection — `l = length(s)` (row-local → fusable) and `c = CASE`
    // (rejected → must stay serial above the merge) — must split and still match
    // the all-serial scan→filter→compute. Verifies derivedFusable classification
    // and that fusing only the safe subset + layering the unsafe subset serially
    // produces identical (l, c) rows.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "s", .type = .string } },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 16,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(u64),
    });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .row_group_size = 16 });

    const pool = [_][]const u8{ "x", "xx", "xxx", "xxxx", "xxxxx" };
    var rows: [96]struct { id: i64, s: []const u8 } = undefined;
    for (&rows, 0..) |*r, i| {
        r.id = @intCast(i);
        r.s = if (i % 7 == 0) "" else pool[i % 5];
    }
    try t.insert(&rows);
    try t.flush();
    var tail: [16]struct { id: i64, s: []const u8 } = undefined;
    for (&tail, 0..) |*r, i| {
        r.id = @intCast(96 + i);
        r.s = pool[i % 5];
    }
    try t.insert(&tail);

    const Derived = @import("compute.zig").Derived;
    const safe = Derived{ .name = "l", .expr = .{ .call = .{ .fn_name = "length", .args = &.{.{ .col_ref = "s" }} } } };
    const else_zero = @import("expr.zig").Expr{ .lit = .{ .int = 0 } };
    const unsafe = Derived{ .name = "c", .expr = .{ .case = .{
        .branches = &.{.{ .cond = leafExpr("id", .gte, .{ .bigint = 50 }), .then = .{ .lit = .{ .int = 1 } } }},
        .else_branch = &else_zero,
    } } };

    // Classification: length(s) is fusable; a row-local CASE now classifies
    // fusable too (the measured table-source exclusion lives in
    // ParallelScan.tryFuseCompute, not here). A CASE whose branch condition
    // references a column outside the scan schema stays non-fusable.
    try std.testing.expect(exec.derivedFusable(safe, schema.columns));
    try std.testing.expect(exec.derivedFusable(unsafe, schema.columns));
    const sibling_case = Derived{ .name = "c2", .expr = .{ .case = .{
        .branches = &.{.{ .cond = leafExpr("not_a_column", .gte, .{ .bigint = 50 }), .then = .{ .lit = .{ .int = 1 } } }},
        .else_branch = &else_zero,
    } } };
    try std.testing.expect(!exec.derivedFusable(sibling_case, schema.columns));

    // Encode each surviving row's (l, c) as one i64 so we can compare multisets.
    const collectLC = struct {
        fn run(a: std.mem.Allocator, q: *Query, out: *std.ArrayList(i64)) !void {
            defer q.deinit();
            while (try q.next()) |b| {
                const ls = b.values[2].data.int;
                const cs = b.values[3].data.int;
                for (0..b.row_count) |i| try out.append(a, @as(i64, ls[i]) * 100 + cs[i]);
            }
        }
    }.run;

    // Serial reference: scan → filter → compute([l, c]) (both serial).
    var serial: std.ArrayList(i64) = .empty;
    defer serial.deinit(allocator);
    {
        var base = try scan(allocator, t);
        var filtered = try base.filter(leafExpr("s", .neq, .{ .text = "" }));
        var q = try filtered.compute(&.{ safe, unsafe });
        try collectLC(allocator, &q, &serial);
    }
    try std.testing.expect(serial.items.len > 0);

    inline for (.{ 1, 2, 4, 8 }) |dop| {
        var got: std.ArrayList(i64) = .empty;
        defer got.deinit(allocator);
        var base = try exec.ParallelScan.create(allocator, t, null, null, dop);
        var filtered = try base.filter(leafExpr("s", .neq, .{ .text = "" }));
        // Split: fuse only the safe derived into the workers, then layer the
        // unsafe CASE serially above — what fuseSplitCompute does at compile.
        const fused = try filtered.tryFuseCompute(&.{safe});
        try std.testing.expect(fused);
        // Table-backed scan declines the CASE offer (measured net loss);
        // it stays a serial layer above.
        try std.testing.expect(!(try filtered.tryFuseCompute(&.{unsafe})));
        var q = try filtered.compute(&.{unsafe});
        try collectLC(allocator, &q, &got);

        try std.testing.expectEqual(serial.items.len, got.items.len);
        const a = try allocator.dupe(i64, serial.items);
        defer allocator.free(a);
        const b = try allocator.dupe(i64, got.items);
        defer allocator.free(b);
        std.sort.pdq(i64, a, {}, std.sort.asc(i64));
        std.sort.pdq(i64, b, {}, std.sort.asc(i64));
        try std.testing.expectEqualSlices(i64, a, b);
    }
}

test "parallel scan: a filter on a fused derived column runs in the workers; a compute alone streams" {
    // `WHERE a % 32 = 0` lowers to a Filter on a hidden computed column above
    // a fused Compute. No Scan can take that predicate, so the workers must
    // apply it inside their compute pipelines, bounding what they materialize.
    // With no filter, the fused compute keeps every row, so the scan must
    // stream instead of copying the whole table before its first emit.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "s", .type = .string } },
        .order_key = &.{"id"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 16,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(u64),
    });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .row_group_size = 16 });

    const pool = [_][]const u8{ "x", "xx", "xxx", "xxxx", "xxxxx" };
    var rows: [128]struct { id: i64, s: []const u8 } = undefined;
    for (&rows, 0..) |*r, i| {
        r.id = @intCast(i);
        r.s = if (i % 7 == 0) "" else pool[i % 5];
    }
    try t.insert(&rows);
    try t.flush();
    var tail: [16]struct { id: i64, s: []const u8 } = undefined;
    for (&tail, 0..) |*r, i| {
        r.id = @intCast(128 + i);
        r.s = pool[i % 5];
    }
    try t.insert(&tail);

    const derived = [_]@import("compute.zig").Derived{
        .{ .name = "l", .expr = .{ .call = .{ .fn_name = "length", .args = &.{.{ .col_ref = "s" }} } } },
    };
    const long_only = leafExpr("l", .gte, .{ .int = 3 });

    const sortedLengths = struct {
        fn run(a: std.mem.Allocator, q: *Query) ![]i32 {
            var out: std.ArrayList(i32) = .empty;
            errdefer out.deinit(a);
            while (try q.next()) |b| try out.appendSlice(a, b.values[2].data.int[0..b.row_count]);
            const s = try out.toOwnedSlice(a);
            std.sort.pdq(i32, s, {}, std.sort.asc(i32));
            return s;
        }
    }.run;

    const expect_all = blk: {
        var base = try scan(allocator, t);
        var q = try base.compute(&derived);
        defer q.deinit();
        break :blk try sortedLengths(allocator, &q);
    };
    defer allocator.free(expect_all);
    const expect_long = blk: {
        var base = try scan(allocator, t);
        var computed = try base.compute(&derived);
        var q = try computed.filter(long_only);
        defer q.deinit();
        break :blk try sortedLengths(allocator, &q);
    };
    defer allocator.free(expect_long);
    try std.testing.expect(expect_long.len > 0 and expect_long.len < expect_all.len);

    inline for (.{ 1, 2, 4, 8 }) |dop| {
        {
            var base = try exec.ParallelScan.create(allocator, t, null, null, dop);
            const ps = exec.queryAs(exec.ParallelScan, base).?;
            try std.testing.expect(try base.tryFuseCompute(&derived));
            var q = try base.filter(long_only);
            defer q.deinit();
            try std.testing.expect(exec.queryAs(@import("filter.zig").Filter, q).?.fused);
            const got = try sortedLengths(allocator, &q);
            defer allocator.free(got);
            try std.testing.expect(ps.mode == .materialize);
            try std.testing.expectEqualSlices(i32, expect_long, got);
        }
        {
            var q = try exec.ParallelScan.create(allocator, t, null, null, dop);
            defer q.deinit();
            const ps = exec.queryAs(exec.ParallelScan, q).?;
            try std.testing.expect(try q.tryFuseCompute(&derived));
            const got = try sortedLengths(allocator, &q);
            defer allocator.free(got);
            try std.testing.expect(ps.mode == .round);
            try std.testing.expectEqualSlices(i32, expect_all, got);
        }
    }
}

test "parallel buffer scan: a filter on a fused derived column runs in the stripe workers and still streams" {
    const allocator = std.testing.allocator;
    const SingleBatchSource = @import("single_batch.zig").SingleBatchSource;
    const storage = @import("../storage/storage.zig");
    const n: usize = @import("mat_stage.zig").chunk_rows * 2 + 500;
    const data = try allocator.alloc(i64, n);
    defer allocator.free(data);
    for (data, 0..) |*d, i| d.* = @intCast(i);
    const source_schema = [_]types.Column{.{ .name = "v", .type = .bigint }};
    const source_views = [_]storage.ColumnView{.{ .data = .{ .bigint = data }, .nulls = null }};

    const set = try exec.StageSet.create(allocator);
    defer set.deinit();
    const source = try SingleBatchSource.create(allocator, .{ .schema = &source_schema, .values = &source_views, .row_count = n });
    const stage = try set.addStage(source, null);

    var base = try exec.ParallelScan.createOverStage(allocator, allocator, stage, null, 3);
    const ps = exec.queryAs(exec.ParallelScan, base).?;
    const derived = [_]@import("compute.zig").Derived{
        .{ .name = "m", .expr = .{ .call = .{ .fn_name = "mod", .args = &.{ .{ .col_ref = "v" }, .{ .lit = .{ .bigint = 7 } } } } } },
    };
    try std.testing.expect(try base.tryFuseCompute(&derived));
    var q = try base.filter(leafExpr("m", .eq, .{ .bigint = 0 }));
    defer q.deinit();
    try std.testing.expect(exec.queryAs(@import("filter.zig").Filter, q).?.fused);

    var count: usize = 0;
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try std.testing.expectEqual(@as(i64, 0), @mod(v, 7));
        count += b.row_count;
    }
    try std.testing.expect(ps.mode == .round);
    try std.testing.expectEqual((n + 6) / 7, count);
}

test "SetUnion probe forwarding: join probes union arms inside scan workers" {
    // A LEFT join whose probe side is a UNION ALL of two arms must forward
    // its probe sink through the union: a ParallelScan arm probes in its
    // stripe workers (fused), a plain-Scan arm declines and the union probes
    // its raw batches serially through the same sink. All three shapes —
    // both arms fused, one fused, none (reference) — must emit the same
    // (id, lv, rv) multiset, including NULL rv for unmatched probe rows.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const pschema = types.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "lv", .type = .int } },
        .order_key = &.{"id"},
        .unique = true,
    };
    const bschema = types.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "rv", .type = .int } },
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 64,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(u64),
    });
    defer db.close();

    const a1 = try db.table("a1", pschema, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 64 });
    const a2 = try db.table("a2", pschema, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 64 });
    const b = try db.table("b", bschema, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 64 });

    var rows1: [600]struct { id: i64, lv: i32 } = undefined;
    for (&rows1, 0..) |*r, i| {
        r.id = @intCast(i);
        r.lv = @intCast(i * 2);
    }
    try a1.insert(&rows1);
    try a1.flush();
    var rows2: [400]struct { id: i64, lv: i32 } = undefined;
    for (&rows2, 0..) |*r, i| {
        r.id = @intCast(1000 + i);
        r.lv = @intCast(i * 3);
    }
    try a2.insert(&rows2);
    try a2.flush();
    // Every 3rd id across both arm ranges matches; the rest miss (NULL rv).
    var brows: [467]struct { id: i64, rv: i32 } = undefined;
    for (&brows, 0..) |*r, i| {
        r.id = @intCast(i * 3);
        r.rv = @intCast(i * 3 * 10);
    }
    try b.insert(&brows);
    try b.flush();

    const collect = struct {
        fn run(a: std.mem.Allocator, q: *Query, out: *std.ArrayList(i64)) !void {
            defer q.deinit();
            while (try q.next()) |batch| {
                const ids = batch.values[0].data.bigint;
                const lvs = batch.values[1].data.int;
                const rvs = batch.values[2].data.int;
                for (0..batch.row_count) |i| {
                    const rv: i64 = if (batch.values[2].isValid(@intCast(i))) rvs[i] else -1;
                    try out.append(a, ids[i] * 1_000_000 + @as(i64, lvs[i]) * 100_000 + rv);
                }
            }
        }
    }.run;

    const spec = @import("join.zig").Spec{
        .on = &.{.{ .left = "id", .right = "id" }},
        .join_type = .left,
        .algorithm = .auto,
    };

    // Serial reference: plain scans decline the probe offer end-to-end.
    var ref: std.ArrayList(i64) = .empty;
    defer ref.deinit(allocator);
    {
        const l1 = try scan(allocator, a1);
        const l2 = try scan(allocator, a2);
        var u = try exec.SetUnion.create(allocator, l1, l2, true);
        errdefer u.deinit();
        var q = try u.join(try scan(allocator, b), spec);
        const j = exec.queryAs(@import("join.zig").Join, q).?;
        try std.testing.expect(!j.probe_fused);
        try collect(allocator, &q, &ref);
    }
    try std.testing.expectEqual(@as(usize, 1000), ref.items.len);
    std.sort.pdq(i64, ref.items, {}, std.sort.asc(i64));

    // Both arms parallel: probe fuses through the union into both scans.
    {
        const l1 = try exec.ParallelScan.create(allocator, a1, null, null, 4);
        const l2 = try exec.ParallelScan.create(allocator, a2, null, null, 4);
        var u = try exec.SetUnion.create(allocator, l1, l2, true);
        errdefer u.deinit();
        var q = try u.join(try scan(allocator, b), spec);
        const j = exec.queryAs(@import("join.zig").Join, q).?;
        try std.testing.expect(j.probe_fused);
        const su = exec.queryAs(exec.SetUnion, j.left).?;
        try std.testing.expect(su.left_fused and su.right_fused);
        var got: std.ArrayList(i64) = .empty;
        defer got.deinit(allocator);
        try collect(allocator, &q, &got);
        std.sort.pdq(i64, got.items, {}, std.sort.asc(i64));
        try std.testing.expectEqualSlices(i64, ref.items, got.items);
    }

    // Mixed arms: the plain-Scan arm declines — the union probes it
    // serially through the sink's chunk-0 scratch after the fused arm
    // drains. Same multiset.
    {
        const l1 = try exec.ParallelScan.create(allocator, a1, null, null, 4);
        const l2 = try scan(allocator, a2);
        var u = try exec.SetUnion.create(allocator, l1, l2, true);
        errdefer u.deinit();
        var q = try u.join(try scan(allocator, b), spec);
        const j = exec.queryAs(@import("join.zig").Join, q).?;
        try std.testing.expect(j.probe_fused);
        const su = exec.queryAs(exec.SetUnion, j.left).?;
        try std.testing.expect(su.left_fused and !su.right_fused);
        var got: std.ArrayList(i64) = .empty;
        defer got.deinit(allocator);
        try collect(allocator, &q, &got);
        std.sort.pdq(i64, got.items, {}, std.sort.asc(i64));
        try std.testing.expectEqualSlices(i64, ref.items, got.items);
    }

    // Cast-needing arm: a3.lv is SMALLINT, so the union widens it to INT.
    // That arm can't host worker probes (the sink's indices address the
    // POST-cast schema) — it must take the serial lane, cast first, while
    // the cast-free ParallelScan arm still fuses.
    const cschema = types.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "lv", .type = .smallint } },
        .order_key = &.{"id"},
        .unique = true,
    };
    const a3 = try db.table("a3", cschema, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 64 });
    var rows3: [300]struct { id: i64, lv: i16 } = undefined;
    for (&rows3, 0..) |*r, i| {
        r.id = @intCast(2000 + i);
        r.lv = @intCast(i);
    }
    try a3.insert(&rows3);
    try a3.flush();

    var ref3: std.ArrayList(i64) = .empty;
    defer ref3.deinit(allocator);
    {
        const l1 = try scan(allocator, a1);
        const l3 = try scan(allocator, a3);
        var u = try exec.SetUnion.create(allocator, l1, l3, true);
        errdefer u.deinit();
        var q = try u.join(try scan(allocator, b), spec);
        try collect(allocator, &q, &ref3);
    }
    try std.testing.expectEqual(@as(usize, 900), ref3.items.len);
    std.sort.pdq(i64, ref3.items, {}, std.sort.asc(i64));
    {
        const l1 = try exec.ParallelScan.create(allocator, a1, null, null, 4);
        const l3 = try exec.ParallelScan.create(allocator, a3, null, null, 4);
        var u = try exec.SetUnion.create(allocator, l1, l3, true);
        errdefer u.deinit();
        var q = try u.join(try scan(allocator, b), spec);
        const j = exec.queryAs(@import("join.zig").Join, q).?;
        try std.testing.expect(j.probe_fused);
        const su = exec.queryAs(exec.SetUnion, j.left).?;
        try std.testing.expect(su.left_fused and !su.right_fused);
        var got: std.ArrayList(i64) = .empty;
        defer got.deinit(allocator);
        try collect(allocator, &q, &got);
        std.sort.pdq(i64, got.items, {}, std.sort.asc(i64));
        try std.testing.expectEqualSlices(i64, ref3.items, got.items);
    }
}

test "SetUnion rechain: a second join above the union extends the fused pipeline" {
    // Join stack over a union: j2(j1(union)). j1 fuses its probe through the
    // union into the arm scans; j2 then RECHAINS through j1 and the union so
    // the whole two-join tail runs inside the workers. Must match the
    // all-serial reference multiset.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const pschema = types.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "lv", .type = .int } },
        .order_key = &.{"id"},
        .unique = true,
    };
    const b1schema = types.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "rv", .type = .int } },
        .order_key = &.{"id"},
        .unique = true,
    };
    const b2schema = types.TableSchema{
        .columns = &.{ .{ .name = "rv", .type = .int }, .{ .name = "sv", .type = .int } },
        .order_key = &.{"rv"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 64,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(u64),
    });
    defer db.close();

    const a1 = try db.table("ra1", pschema, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 64 });
    const a2 = try db.table("ra2", pschema, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 64 });
    const b1 = try db.table("rb1", b1schema, .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 64 });
    const b2 = try db.table("rb2", b2schema, .{ .order_key = &.{"rv"}, .unique = true, .row_group_size = 64 });

    var rows1: [400]struct { id: i64, lv: i32 } = undefined;
    for (&rows1, 0..) |*r, i| {
        r.id = @intCast(i);
        r.lv = @intCast(i);
    }
    try a1.insert(&rows1);
    try a1.flush();
    var rows2: [300]struct { id: i64, lv: i32 } = undefined;
    for (&rows2, 0..) |*r, i| {
        r.id = @intCast(1000 + i);
        r.lv = @intCast(i);
    }
    try a2.insert(&rows2);
    try a2.flush();
    // b1 covers every 2nd id; rv = id * 7 mod 97 gives spread for b2.
    var brows1: [650]struct { id: i64, rv: i32 } = undefined;
    for (&brows1, 0..) |*r, i| {
        r.id = @intCast(i * 2);
        r.rv = @intCast((i * 7) % 97);
    }
    try b1.insert(&brows1);
    try b1.flush();
    // b2 covers rv 0..96 step 3.
    var brows2: [33]struct { rv: i32, sv: i32 } = undefined;
    for (&brows2, 0..) |*r, i| {
        r.rv = @intCast(i * 3);
        r.sv = @intCast(i * 100);
    }
    try b2.insert(&brows2);
    try b2.flush();

    const spec1 = @import("join.zig").Spec{
        .on = &.{.{ .left = "id", .right = "id" }},
        .join_type = .left,
        .algorithm = .auto,
    };
    const spec2 = @import("join.zig").Spec{
        .on = &.{.{ .left = "rv", .right = "rv" }},
        .join_type = .left,
        .algorithm = .auto,
    };

    const collect2 = struct {
        fn run(a: std.mem.Allocator, q: *Query, out: *std.ArrayList(i64)) !void {
            defer q.deinit();
            while (try q.next()) |batch| {
                const n = batch.values.len;
                for (0..batch.row_count) |i| {
                    var acc: i64 = 0;
                    for (0..n) |c| {
                        const v: i64 = if (!batch.values[c].isValid(@intCast(i))) -1 else switch (batch.values[c].data) {
                            .bigint => |s| s[i],
                            .int => |s| s[i],
                            else => 0,
                        };
                        acc = acc *% 1099511628211 +% v;
                    }
                    try out.append(a, acc);
                }
            }
        }
    }.run;

    var ref: std.ArrayList(i64) = .empty;
    defer ref.deinit(allocator);
    {
        const l1 = try scan(allocator, a1);
        const l2 = try scan(allocator, a2);
        var u = try exec.SetUnion.create(allocator, l1, l2, true);
        errdefer u.deinit();
        var j1 = try u.join(try scan(allocator, b1), spec1);
        var q = try j1.join(try scan(allocator, b2), spec2);
        try collect2(allocator, &q, &ref);
    }
    try std.testing.expectEqual(@as(usize, 700), ref.items.len);
    std.sort.pdq(i64, ref.items, {}, std.sort.asc(i64));

    {
        const l1 = try exec.ParallelScan.create(allocator, a1, null, null, 4);
        const l2 = try exec.ParallelScan.create(allocator, a2, null, null, 4);
        var u = try exec.SetUnion.create(allocator, l1, l2, true);
        errdefer u.deinit();
        var j1 = try u.join(try scan(allocator, b1), spec1);
        var q = try j1.join(try scan(allocator, b2), spec2);
        const j2op = exec.queryAs(@import("join.zig").Join, q).?;
        // Both joins must be probe-fused: j1 through the union into the arm
        // scans, j2 by RECHAINING through j1 and the union (the new path).
        try std.testing.expect(j2op.probe_fused);
        const j1op = exec.queryAs(@import("join.zig").Join, j2op.left).?;
        try std.testing.expect(j1op.probe_fused);
        var got: std.ArrayList(i64) = .empty;
        defer got.deinit(allocator);
        try collect2(allocator, &q, &got);
        std.sort.pdq(i64, got.items, {}, std.sort.asc(i64));
        try std.testing.expectEqualSlices(i64, ref.items, got.items);
    }
}

test "join: empty fused stage filter skips every lookup in a join chain" {
    const allocator = std.testing.allocator;
    const ex = exec;
    const SingleBatchSource = @import("single_batch.zig").SingleBatchSource;
    const storage = @import("../storage/storage.zig");
    const source_schema = [_]types.Column{
        .{ .name = "id", .type = .bigint, .nullable = true },
        .{ .name = "other", .type = .bigint, .nullable = true },
    };
    const lookup_schema = [_]types.Column{
        .{ .name = "key", .type = .bigint },
        .{ .name = "value", .type = .bigint },
    };
    const ids = [_]i64{ 1, 2, 3 };
    const cases = .{
        .{ .other = &[_]i64{ 1, 2, 3 }, .nulls = @as(?[]const u8, null), .rows = 3, .expected = 0 },
        .{ .other = &[_]i64{ 1, 2, 4 }, .nulls = @as(?[]const u8, null), .rows = 3, .expected = 1 },
        .{ .other = &[_]i64{ 1, 2, 4 }, .nulls = @as(?[]const u8, &.{0b00000011}), .rows = 3, .expected = 0 },
        .{ .other = &[_]i64{ 1, 2, 3 }, .nulls = @as(?[]const u8, null), .rows = 0, .expected = 0 },
    };
    inline for (cases) |case| {
        const set = try ex.StageSet.create(allocator);
        defer set.deinit();
        const source_views = [_]storage.ColumnView{
            .{ .data = .{ .bigint = &ids }, .nulls = null },
            .{ .data = .{ .bigint = case.other }, .nulls = case.nulls },
        };
        const source = try SingleBatchSource.create(allocator, .{ .schema = &source_schema, .values = &source_views, .row_count = case.rows });
        const stage = try set.addStage(source, null);
        const lookup_views = [_]storage.ColumnView{
            .{ .data = .{ .bigint = &ids }, .nulls = null },
            .{ .data = .{ .bigint = &ids }, .nulls = null },
        };
        const lookup_batch = ex.Batch{ .schema = &lookup_schema, .values = &lookup_views, .row_count = 3 };
        const lookup1 = try set.addStage(try SingleBatchSource.create(allocator, lookup_batch), null);
        const lookup2 = try set.addStage(try SingleBatchSource.create(allocator, lookup_batch), null);
        const parallel = try ex.ParallelScan.createOverStageDeferred(allocator, allocator, stage, null, 2);
        const alias = try ex.AliasRename.create(allocator, parallel, "p");
        const projected = try alias.projectNamed(&.{ "p.other", "p.id" }, &.{ "comparison", "probe_key" });
        const filtered = try projected.filter(.{ .leaf_col_col = .{ .left = "probe_key", .op = .neq, .right = "comparison" } });
        const rhs1 = try ex.AliasRename.create(allocator, try ex.MatScan.create(allocator, lookup1), "a");
        const first = try filtered.join(rhs1, .{ .algorithm = .hash, .join_type = .left, .on = &.{.{ .left = "probe_key", .right = "a.key" }} });
        const rhs2 = try ex.AliasRename.create(allocator, try ex.MatScan.create(allocator, lookup2), "b");
        var query = try first.join(rhs2, .{ .algorithm = .hash, .join_type = .left, .on = &.{.{ .left = "probe_key", .right = "b.key" }} });
        defer query.deinit();
        try std.testing.expect(ex.queryAs(ex.Join, first).?.probe_fused);
        try std.testing.expect(ex.queryAs(ex.Join, query).?.probe_fused);
        try std.testing.expect(stage.result == null);
        set.releaseCompilePins();
        var rows: usize = 0;
        while (try query.next()) |batch| {
            for (0..batch.row_count) |i| {
                try std.testing.expectEqual(@as(i64, 3), batch.values[1].data.bigint[i]);
                try std.testing.expectEqual(@as(i64, 3), batch.values[2].data.bigint[i]);
                try std.testing.expectEqual(@as(i64, 3), batch.values[3].data.bigint[i]);
                rows += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, case.expected), rows);
        if (case.expected == 0) {
            try std.testing.expect(lookup1.query_alive);
            try std.testing.expect(lookup2.query_alive);
            try std.testing.expect(lookup1.result == null);
            try std.testing.expect(lookup2.result == null);
        }
    }
}

const ColStat = exec.ColStat;
const ColCard = exec.ColCard;
const ColumnOrigin = exec.ColumnOrigin;

test "union NDV: arms over one table column hold at most its NDV, other arms sum" {
    const s1a: ColumnOrigin = .{ .snapshot = 1, .column = 0, .ndv = .{ .exact = 100 } };
    const s1b: ColumnOrigin = .{ .snapshot = 1, .column = 1, .ndv = .{ .exact = 100 } };
    const s2a: ColumnOrigin = .{ .snapshot = 2, .column = 0, .ndv = .{ .exact = 100 } };
    const cases = .{
        // Same column of one snapshot: the sum, capped at the column's NDV.
        .{ .l = ColStat{ .ndv = .{ .exact = 80 }, .origin = s1a }, .l_rows = 1000, .r = ColStat{ .ndv = .{ .exact = 60 }, .origin = s1a }, .r_rows = 1000, .ndv = ColCard{ .exact = 100 }, .origin = @as(?ColumnOrigin, s1a) },
        .{ .l = ColStat{ .ndv = .{ .exact = 30 }, .origin = s1a }, .l_rows = 1000, .r = ColStat{ .ndv = .{ .exact = 20 }, .origin = s1a }, .r_rows = 1000, .ndv = ColCard{ .exact = 50 }, .origin = @as(?ColumnOrigin, s1a) },
        .{ .l = ColStat{ .ndv = .unknown, .origin = s1a }, .l_rows = 1000, .r = ColStat{ .ndv = .{ .exact = 60 }, .origin = s1a }, .r_rows = 1000, .ndv = ColCard{ .exact = 100 }, .origin = @as(?ColumnOrigin, s1a) },
        // Another column, another snapshot, or no origin: the plain sum.
        .{ .l = ColStat{ .ndv = .{ .exact = 80 }, .origin = s1a }, .l_rows = 1000, .r = ColStat{ .ndv = .{ .exact = 60 }, .origin = s1b }, .r_rows = 1000, .ndv = ColCard{ .exact = 140 }, .origin = @as(?ColumnOrigin, null) },
        .{ .l = ColStat{ .ndv = .{ .exact = 80 }, .origin = s1a }, .l_rows = 1000, .r = ColStat{ .ndv = .{ .exact = 60 }, .origin = s2a }, .r_rows = 1000, .ndv = ColCard{ .exact = 140 }, .origin = @as(?ColumnOrigin, null) },
        .{ .l = ColStat{ .ndv = .{ .exact = 80 } }, .l_rows = 1000, .r = ColStat{ .ndv = .{ .exact = 60 } }, .r_rows = 1000, .ndv = ColCard{ .exact = 140 }, .origin = @as(?ColumnOrigin, null) },
        .{ .l = ColStat{ .ndv = .unknown, .origin = s1a }, .l_rows = 1000, .r = ColStat{ .ndv = .{ .exact = 60 }, .origin = s2a }, .r_rows = 1000, .ndv = ColCard.unknown, .origin = @as(?ColumnOrigin, null) },
        // An empty arm adds nothing: the other arm's stat stands.
        .{ .l = ColStat{ .ndv = .{ .exact = 80 }, .origin = s1a }, .l_rows = 1000, .r = ColStat{ .ndv = .{ .exact = 60 }, .origin = s2a }, .r_rows = 0, .ndv = ColCard{ .exact = 80 }, .origin = @as(?ColumnOrigin, s1a) },
        .{ .l = ColStat{ .ndv = .{ .exact = 80 }, .origin = s1a }, .l_rows = 0, .r = ColStat{ .ndv = .{ .exact = 60 }, .origin = s2a }, .r_rows = 1000, .ndv = ColCard{ .exact = 60 }, .origin = @as(?ColumnOrigin, s2a) },
    };
    inline for (cases) |c| {
        const merged = exec.mergeUnionColStat(c.l, c.l_rows, c.r, c.r_rows);
        try std.testing.expectEqual(c.ndv, merged.ndv);
        try std.testing.expectEqual(c.origin, merged.origin);
    }
}

test "union row origin: shared by arms over one snapshot, or the non-empty arm's" {
    const s1: exec.RowOrigin = .{ .snapshot = 1, .rows = 100 };
    const s2: exec.RowOrigin = .{ .snapshot = 2, .rows = 100 };
    const cases = .{
        .{ .l = exec.PipelineStats{ .upper_rows = 100, .row_origin = s1 }, .r = exec.PipelineStats{ .upper_rows = 40, .row_origin = s1 }, .want = @as(?exec.RowOrigin, s1) },
        .{ .l = exec.PipelineStats{ .upper_rows = 100, .row_origin = s1 }, .r = exec.PipelineStats{ .upper_rows = 40, .row_origin = s2 }, .want = @as(?exec.RowOrigin, null) },
        .{ .l = exec.PipelineStats{ .upper_rows = 100, .row_origin = s1 }, .r = exec.PipelineStats{ .upper_rows = 40 }, .want = @as(?exec.RowOrigin, null) },
        .{ .l = exec.PipelineStats{ .upper_rows = 100, .row_origin = s1 }, .r = exec.PipelineStats{ .upper_rows = 0 }, .want = @as(?exec.RowOrigin, s1) },
        .{ .l = exec.PipelineStats{ .upper_rows = 0, .row_origin = s1 }, .r = exec.PipelineStats{ .upper_rows = 40, .row_origin = s2 }, .want = @as(?exec.RowOrigin, s2) },
    };
    inline for (cases) |c| try std.testing.expectEqual(c.want, exec.unionRowOrigin(c.l, c.r));
}

test "distinct row bound: key tuples from one snapshot's rows cap at its rows" {
    const o0: ColumnOrigin = .{ .snapshot = 1, .column = 0, .ndv = .{ .exact = 50 } };
    const o1: ColumnOrigin = .{ .snapshot = 1, .column = 1, .ndv = .{ .exact = 50 } };
    const other: ColumnOrigin = .{ .snapshot = 9, .column = 0, .ndv = .{ .exact = 50 } };
    const cols = [_]ColStat{
        .{ .ndv = .{ .exact = 50 }, .origin = o0 },
        .{ .ndv = .{ .exact = 50 }, .origin = o1 },
        .{ .ndv = .{ .exact = 3 } },
        .{ .ndv = .{ .exact = 50 }, .origin = other },
        .{ .ndv = .unknown, .origin = o0 },
    };
    const union_of_two_arms: exec.PipelineStats = .{ .upper_rows = 200, .column_stats = &cols, .row_origin = .{ .snapshot = 1, .rows = 100 } };
    const no_origin: exec.PipelineStats = .{ .upper_rows = 200, .column_stats = &cols };
    const cases = .{
        .{ .st = union_of_two_arms, .width = 1, .want = 50 },
        .{ .st = union_of_two_arms, .width = 2, .want = 100 },
        .{ .st = union_of_two_arms, .width = 3, .want = 200 },
        .{ .st = union_of_two_arms, .width = 4, .want = 200 },
        .{ .st = union_of_two_arms, .width = 5, .want = 200 },
        .{ .st = no_origin, .width = 2, .want = 200 },
    };
    inline for (cases) |c| try std.testing.expectEqual(@as(u64, c.want), exec.distinctRowBound(c.st, c.width));

    var only_origin_keys = union_of_two_arms;
    only_origin_keys.column_stats = &.{ cols[0], cols[4] };
    try std.testing.expectEqual(@as(u64, 100), exec.distinctRowBound(only_origin_keys, 2));
}

test "INTERSECT / EXCEPT bound: at most the left arm's distinct rows, and the right's for INTERSECT" {
    const left_cols = [_]ColStat{
        .{ .ndv = .{ .exact = 10 }, .min = 0, .max = 9 },
        .{ .ndv = .{ .exact = 20 } },
    };
    const right_cols = [_]ColStat{
        .{ .ndv = .{ .exact = 5 }, .min = 5, .max = 30 },
        .{ .ndv = .{ .exact = 50 } },
    };
    const left: exec.PipelineStats = .{ .upper_rows = 1000, .column_stats = &left_cols };
    const right: exec.PipelineStats = .{ .upper_rows = 50, .column_stats = &right_cols };
    const cases = .{
        .{ .kind = exec.SubsetSetOp.except, .rows = 200, .ndv0 = 10, .min0 = 0, .max0 = 9, .ndv1 = 20 },
        .{ .kind = exec.SubsetSetOp.intersect, .rows = 50, .ndv0 = 5, .min0 = 5, .max0 = 9, .ndv1 = 20 },
    };
    inline for (cases) |c| {
        var out: [2]ColStat = undefined;
        try std.testing.expectEqual(@as(u64, c.rows), exec.subsetSetOpBound(c.kind, left, right, &out));
        try std.testing.expectEqual(ColCard{ .exact = c.ndv0 }, out[0].ndv);
        try std.testing.expectEqual(@as(?i128, c.min0), out[0].min);
        try std.testing.expectEqual(@as(?i128, c.max0), out[0].max);
        try std.testing.expectEqual(ColCard{ .exact = c.ndv1 }, out[1].ndv);
    }

    var tiny_left = left;
    tiny_left.upper_rows = 3;
    var out: [2]ColStat = undefined;
    try std.testing.expectEqual(@as(u64, 3), exec.subsetSetOpBound(.except, tiny_left, right, &out));
    try std.testing.expectEqual(ColCard{ .exact = 3 }, out[0].ndv);
}

/// One BIGINT key held in chunks of `sizes` rows: consecutive runs of
/// `values`, or, with `restart`, each chunk from its start.
const TestKeyBuffer = struct {
    values: []const i64,
    sizes: []const usize,
    restart: bool,

    const View = @import("../storage/storage.zig").ColumnView;

    pub fn len(self: TestKeyBuffer) usize {
        return self.sizes.len;
    }
    pub fn rows(self: TestKeyBuffer, i: usize) usize {
        return self.sizes[i];
    }
    pub fn views(self: TestKeyBuffer, i: usize, out: []View) void {
        var start: usize = 0;
        if (!self.restart) {
            for (self.sizes[0..i]) |s| start += s;
        }
        out[0] = .{ .data = .{ .bigint = self.values[start..][0..self.sizes[i]] } };
    }
};

test "a key sample reads a small buffer whole, else a sixteenth of it in windows across its chunks (issue #478)" {
    const alloc = std.testing.allocator;
    const values = try alloc.alloc(i64, 2_000_000);
    defer alloc.free(values);
    for (values, 0..) |*v, i| v.* = @intCast(i);
    const cases = .{
        // Small chunks within a row group's worth: read whole.
        .{ .sizes = &([_]usize{5000} ** 12), .restart = false, .rows = 60_000, .complete = true },
        // Many small chunks past it: windows across them, some chunks empty.
        .{ .sizes = &([_]usize{ 5000, 0 } ** 100), .restart = false, .rows = 16 * (exec.KEY_SAMPLE_MIN_ROWS / 16), .complete = false },
        // One chunk of a mid-sized input: a sixteenth of it.
        .{ .sizes = &[_]usize{2_000_000}, .restart = false, .rows = 16 * (2_000_000 / 16 / 16), .complete = false },
        // A large input: no more than the cap. Its chunks repeat their keys.
        .{ .sizes = &([_]usize{2_000_000} ** 10), .restart = true, .rows = exec.KEY_SAMPLE_MAX_ROWS, .complete = false },
    };
    inline for (cases) |c| {
        var sample = try exec.KeySample.init(alloc, 1);
        defer sample.deinit(alloc);
        var views: [1]TestKeyBuffer.View = undefined;
        exec.sampleBuffer(&sample, TestKeyBuffer{ .values = values, .sizes = c.sizes, .restart = c.restart }, &views);
        try std.testing.expectEqual(@as(u64, c.rows), sample.rows);
        try std.testing.expectEqual(c.complete, sample.complete);
        // Every key is distinct, so a row read twice would show as fewer.
        if (!c.restart) {
            const seen = sample.keys[0].estimate();
            try std.testing.expect(seen * 100 >= c.rows * 96 and seen * 100 <= c.rows * 104);
        }
    }
}

/// A source of BIGINT columns that honors `setEmitProjection` by keeping the
/// named columns in the REVERSE of their order, so a consumer that reads by
/// position after narrowing reads another column's values.
const NarrowingSource = struct {
    allocator: std.mem.Allocator,
    schema: []types.Column,
    views: []@import("../storage/storage.zig").ColumnView,
    rows: usize,
    emitted: bool = false,
    declarations: usize = 0,
    declared: [8][]const u8 = undefined,
    declared_len: usize = 0,

    fn create(allocator: std.mem.Allocator, names: []const []const u8, columns: []const []const i64) !Query {
        const self = try allocator.create(NarrowingSource);
        errdefer allocator.destroy(self);
        const schema = try allocator.alloc(types.Column, names.len);
        errdefer allocator.free(schema);
        const views = try allocator.alloc(@import("../storage/storage.zig").ColumnView, names.len);
        for (names, columns, schema, views) |name, values, *column, *view| {
            column.* = .{ .name = name, .type = .bigint };
            view.* = .{ .data = .{ .bigint = values }, .nulls = null };
        }
        self.* = .{ .allocator = allocator, .schema = schema, .views = views, .rows = columns[0].len };
        return exec.makeQuery(allocator, self);
    }

    pub fn deinit(self: *NarrowingSource) void {
        const allocator = self.allocator;
        allocator.free(self.schema);
        allocator.free(self.views);
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *NarrowingSource) []const types.Column {
        return self.schema;
    }

    pub fn addPrune(_: *NarrowingSource, _: exec.Predicate) !void {}

    pub fn stats(self: *NarrowingSource) exec.PipelineStats {
        return .{ .upper_rows = self.rows };
    }

    pub fn accountant(_: *NarrowingSource) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(_: *NarrowingSource, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "NarrowingSource");
    }

    pub fn setEmitProjection(self: *NarrowingSource, keep: []const []const u8) !void {
        self.declarations += 1;
        self.declared_len = keep.len;
        for (keep, self.declared[0..keep.len]) |name, *slot| slot.* = name;
        const schema = try self.allocator.alloc(types.Column, keep.len);
        errdefer self.allocator.free(schema);
        const views = try self.allocator.alloc(@import("../storage/storage.zig").ColumnView, keep.len);
        for (keep, 0..) |name, i| {
            const src = types.findColumn(self.schema, name).?;
            schema[keep.len - 1 - i] = self.schema[src];
            views[keep.len - 1 - i] = self.views[src];
        }
        self.allocator.free(self.schema);
        self.allocator.free(self.views);
        self.schema = schema;
        self.views = views;
    }

    pub fn next(self: *NarrowingSource) !?exec.Batch {
        if (self.emitted) return null;
        self.emitted = true;
        return .{ .schema = self.schema, .values = self.views, .row_count = self.rows };
    }
};

// Issue #492. A projection is what knows which of its upstream's columns are
// read above it. It says so once, when it is first pulled: by then the plan
// is built, and the fusion offers that cross a projection (a compute, a
// filter, a join probe) have been made against the upstream's full output.
test "Project declares the columns it reads to its upstream on the first pull" {
    const allocator = std.testing.allocator;
    const names = [_][]const u8{ "a", "b", "c", "d" };
    const columns = [_][]const i64{ &.{ 1, 2 }, &.{ 10, 20 }, &.{ 100, 200 }, &.{ 1000, 2000 } };
    const cases = .{
        // A subset, reordered and repeated: declared once, in upstream order.
        .{ .select = &[_][]const u8{ "d", "a", "d" }, .labels = &[_][]const u8{ "x", "y", "z" }, .declared = &[_][]const u8{ "a", "d" }, .want = &[_]i64{ 1000, 1, 1000 } },
        // Every upstream column, reordered: nothing to drop, nothing declared.
        .{ .select = &[_][]const u8{ "d", "c", "b", "a" }, .labels = &[_][]const u8{ "d", "c", "b", "a" }, .declared = &[_][]const u8{}, .want = &[_]i64{ 1000, 100, 10, 1 } },
    };
    inline for (cases) |case| {
        var source = try NarrowingSource.create(allocator, &names, &columns);
        const narrowing = exec.queryAs(NarrowingSource, source).?;
        var q = source.projectNamed(case.select, case.labels) catch |err| {
            source.deinit();
            return err;
        };
        defer q.deinit();
        try std.testing.expectEqual(@as(usize, 0), narrowing.declarations);

        const batch = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, @intFromBool(case.declared.len > 0)), narrowing.declarations);
        try std.testing.expectEqual(case.declared.len, narrowing.declared_len);
        for (case.declared, narrowing.declared[0..narrowing.declared_len]) |want, got| try std.testing.expectEqualStrings(want, got);
        try std.testing.expectEqual(case.want.len, batch.values.len);
        for (case.want, case.labels, batch.values, batch.schema) |want, label, view, column| {
            try std.testing.expectEqualStrings(label, column.name);
            try std.testing.expectEqual(want, view.data.bigint[0]);
            try std.testing.expectEqual(want * 2, view.data.bigint[1]);
        }

        try std.testing.expectEqual(@as(?exec.Batch, null), try q.next());
        try std.testing.expectEqual(@as(usize, @intFromBool(case.declared.len > 0)), narrowing.declarations);
    }
}
