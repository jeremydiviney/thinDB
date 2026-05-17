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

    const schema = types.Schema{
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

    // Sort descending: no usable sort claim (we only claim ascending)
    {
        var base = try scan(allocator, t);
        var q = try base.orderBy(&.{.{ .col = "qty", .desc = true }});
        defer q.deinit();
        const s = q.stats();
        try std.testing.expectEqual(@as(usize, 0), s.sort_state.keys.len);
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

    const schema = types.Schema{
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

test "scan: segment-level pruning works for string leading-key predicates" {
    // Same shape as the bigint test but with a string order key,
    // exercising the prefix-encoded stats path.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.Schema{
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

    const schema = types.Schema{
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
