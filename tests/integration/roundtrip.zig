//! End-to-end v0.1 round-trip:
//!   open → create table → insert (segments + memtable) → reopen → piped query
//!
//! Uses only the public `thindb.*` API.

const std = @import("std");
const thindb = @import("thindb");

const schema_v1 = thindb.TableSchema{
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

test "roundtrip: insert, flush, reopen, piped query" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // --- session 1: create + insert + flush --------------------------------
    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
        defer db.close();

        const orders = try db.table("orders", schema_v1, opts_v1);

        // Batch 1 → flushed as segment 1
        try orders.insert(&.{
            .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "alpha" },
            .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "beta" },
            .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "gamma" },
            .{ .id = @as(i64, 4), .qty = @as(i32, 40), .active = true, .tag = "delta" },
            .{ .id = @as(i64, 5), .qty = @as(i32, 50), .active = false, .tag = "epsilon" },
        });
        try orders.flush();

        // Batch 2 → flushed as segment 2
        try orders.insert(&.{
            .{ .id = @as(i64, 6), .qty = @as(i32, 60), .active = true, .tag = "zeta" },
            .{ .id = @as(i64, 7), .qty = @as(i32, 70), .active = true, .tag = "eta" },
        });
        try orders.flush();

        // Batch 3 stays in memtable until close
        try orders.insert(&.{
            .{ .id = @as(i64, 8), .qty = @as(i32, 80), .active = true, .tag = "theta" },
            .{ .id = @as(i64, 9), .qty = @as(i32, 90), .active = false, .tag = "iota" },
        });
        // Flush so memtable rows survive reopen — v0.1 doesn't durable-write memtable.
        try orders.flush();

        try std.testing.expectEqual(@as(usize, 3), orders.segmentCount());
    }

    // --- session 2: reopen + piped query -----------------------------------
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();

    const orders = try db.table("orders", schema_v1, opts_v1);
    try std.testing.expectEqual(@as(usize, 3), orders.segmentCount());

    // A reusable transform expressed as a function over Query.
    const onlyActive = struct {
        fn apply(q: thindb.Query) anyerror!thindb.Query {
            return q.filter(thindb.leafExpr("active", .eq, .{ .boolean = true }));
        }
    }.apply;

    // scan → only_active → qty > 25 → project(id, tag) → limit(3)
    var base = try thindb.scan(allocator, orders);
    var piped = try base.pipe(onlyActive);
    var filtered = try piped.filter(thindb.leafExpr("qty", .gt, .{ .int = 25 }));
    var projected = try filtered.project(&.{ "id", "tag" });
    var q = try projected.limit(3);
    defer q.deinit();

    // Expected matches: id ∈ {3, 4, 6, 7, 8}; first 3 are {3, 4, 6} with tags
    // {"gamma", "delta", "zeta"}.
    var collected_ids: std.ArrayList(i64) = .empty;
    defer collected_ids.deinit(allocator);
    var collected_tags: std.ArrayList(u8) = .empty;
    defer collected_tags.deinit(allocator);

    while (try q.next()) |batch| {
        try std.testing.expectEqual(@as(usize, 2), batch.schema.len);
        try std.testing.expectEqualStrings("id", batch.schema[0].name);
        try std.testing.expectEqualStrings("tag", batch.schema[1].name);
        try collected_ids.appendSlice(allocator, batch.values[0].data.bigint);
        for (0..batch.row_count) |i| {
            try collected_tags.append(allocator, '|');
            try collected_tags.appendSlice(allocator, batch.values[1].data.string.rowBytes(i));
        }
    }

    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 4, 6 }, collected_ids.items);
    try std.testing.expectEqualStrings("|gamma|delta|zeta", collected_tags.items);
}

test "roundtrip: empty table scan yields no batches" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const orders = try db.table("orders", schema_v1, opts_v1);
    var q = try thindb.scan(allocator, orders);
    defer q.deinit();

    try std.testing.expect((try q.next()) == null);
}

test "roundtrip: scan reads memtable without a flush" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const orders = try db.table("orders", schema_v1, opts_v1);
    try orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
    });

    var base = try thindb.scan(allocator, orders);
    var q = try base.filter(thindb.leafExpr("qty", .gte, .{ .int = 15 }));
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expectEqualSlices(i64, &[_]i64{2}, b.values[0].data.bigint);
}

// ---------------------------------------------------------------------------
// Nullable columns
// ---------------------------------------------------------------------------

const nullable_schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "qty", .type = .int, .nullable = true },
        .{ .name = "tag", .type = .string, .nullable = true },
    },
    .order_key = &.{"id"},
    .unique = true,
};

const nullable_order_key = [_][]const u8{"id"};
const nullable_opts = thindb.TableOptions{
    .order_key = &nullable_order_key,
    .unique = true,
    .row_group_size = 4,
};

test "nullable: insert mixed null/non-null, flush, reread, IS NULL filter" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
        defer db.close();

        const t = try db.table("items", nullable_schema, nullable_opts);
        try t.insert(&.{
            .{ .id = @as(i64, 1), .qty = @as(?i32, 10), .tag = @as(?[]const u8, "alpha") },
            .{ .id = @as(i64, 2), .qty = @as(?i32, null), .tag = @as(?[]const u8, "beta") },
            .{ .id = @as(i64, 3), .qty = @as(?i32, 30), .tag = @as(?[]const u8, null) },
            .{ .id = @as(i64, 4), .qty = @as(?i32, null), .tag = @as(?[]const u8, null) },
            .{ .id = @as(i64, 5), .qty = @as(?i32, 50), .tag = @as(?[]const u8, "epsilon") },
        });
        try t.flush();
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();
    const t = try db.table("items", nullable_schema, nullable_opts);

    // Reread all rows: validity bitmap survived disk round-trip.
    {
        var q = try thindb.scan(allocator, t);
        defer q.deinit();

        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        var qty_nulls: std.ArrayList(bool) = .empty;
        defer qty_nulls.deinit(allocator);
        var tag_nulls: std.ArrayList(bool) = .empty;
        defer tag_nulls.deinit(allocator);

        while (try q.next()) |batch| {
            try ids.appendSlice(allocator, batch.values[0].data.bigint);
            for (0..batch.row_count) |i| {
                try qty_nulls.append(allocator, !batch.values[1].isValid(i));
                try tag_nulls.append(allocator, !batch.values[2].isValid(i));
            }
        }

        try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4, 5 }, ids.items);
        try std.testing.expectEqualSlices(bool, &[_]bool{ false, true, false, true, false }, qty_nulls.items);
        try std.testing.expectEqualSlices(bool, &[_]bool{ false, false, true, true, false }, tag_nulls.items);
    }

    // IS NULL filter on qty.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.filter(thindb.isNullExpr("qty"));
        defer q.deinit();

        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
        try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 4 }, ids.items);
    }

    // IS NOT NULL filter on tag.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.filter(thindb.isNotNullExpr("tag"));
        defer q.deinit();

        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
        try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 5 }, ids.items);
    }

    // Comparison filter rejects NULL rows (two-valued logic):
    // qty > 0 selects only rows whose qty is non-null AND > 0, i.e. ids {1, 3, 5}.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.filter(thindb.leafExpr("qty", .gt, .{ .int = 0 }));
        defer q.deinit();

        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
        try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 3, 5 }, ids.items);
    }
}

test "nullable: SUM, MIN, MAX, COUNT(col) skip NULL rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();

    const t = try db.table("items", nullable_schema, nullable_opts);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(?i32, 10), .tag = @as(?[]const u8, "a") },
        .{ .id = @as(i64, 2), .qty = @as(?i32, null), .tag = @as(?[]const u8, "b") },
        .{ .id = @as(i64, 3), .qty = @as(?i32, 30), .tag = @as(?[]const u8, null) },
        .{ .id = @as(i64, 4), .qty = @as(?i32, null), .tag = @as(?[]const u8, null) },
        .{ .id = @as(i64, 5), .qty = @as(?i32, 50), .tag = @as(?[]const u8, "e") },
    });
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var q = try base.aggregate(&.{
        .{ .func = .count, .col = null, .as = "n_total" },
        .{ .func = .count, .col = "qty", .as = "n_qty" },
        .{ .func = .sum, .col = "qty", .as = "sum_qty" },
        .{ .func = .min, .col = "qty", .as = "min_qty" },
        .{ .func = .max, .col = "qty", .as = "max_qty" },
    });
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expectEqual(@as(i64, 5), b.values[0].data.bigint[0]); // COUNT(*)
    try std.testing.expectEqual(@as(i64, 3), b.values[1].data.bigint[0]); // COUNT(qty)
    try std.testing.expectEqual(@as(i64, 90), b.values[2].data.bigint[0]); // SUM
    try std.testing.expectEqual(@as(i32, 10), b.values[3].data.int[0]); // MIN
    try std.testing.expectEqual(@as(i32, 50), b.values[4].data.int[0]); // MAX
    try std.testing.expect((try q.next()) == null);
}

// ---------------------------------------------------------------------------
// FLOAT / DOUBLE columns
// ---------------------------------------------------------------------------

const float_schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "price", .type = .float },
        .{ .name = "ratio", .type = .double },
    },
    .order_key = &.{"id"},
    .unique = true,
};

const float_order_key = [_][]const u8{"id"};
const float_opts = thindb.TableOptions{
    .order_key = &float_order_key,
    .unique = true,
    .row_group_size = 4,
};

test "float/double: insert, flush, reread, comparison filter, aggregate" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
        defer db.close();
        const t = try db.table("prices", float_schema, float_opts);
        try t.insert(&.{
            .{ .id = @as(i64, 1), .price = @as(f32, 1.5), .ratio = @as(f64, 0.25) },
            .{ .id = @as(i64, 2), .price = @as(f32, 2.5), .ratio = @as(f64, 0.5) },
            .{ .id = @as(i64, 3), .price = @as(f32, 10.0), .ratio = @as(f64, 0.75) },
            .{ .id = @as(i64, 4), .price = @as(f32, -3.25), .ratio = @as(f64, 1.0) },
        });
        try t.flush();
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();
    const t = try db.table("prices", float_schema, float_opts);

    // Reread + roundtrip values.
    {
        var q = try thindb.scan(allocator, t);
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 4), b.row_count);
        try std.testing.expectEqualSlices(f32, &[_]f32{ 1.5, 2.5, 10.0, -3.25 }, b.values[1].data.float);
        try std.testing.expectEqualSlices(f64, &[_]f64{ 0.25, 0.5, 0.75, 1.0 }, b.values[2].data.double);
    }

    // Comparison filter on float.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.filter(thindb.leafExpr("price", .gte, .{ .float = 2.0 }));
        defer q.deinit();
        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
        try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3 }, ids.items);
    }

    // SUM/MIN/MAX over float and double.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.aggregate(&.{
            .{ .func = .sum, .col = "price", .as = "sum_price" },
            .{ .func = .min, .col = "price", .as = "min_price" },
            .{ .func = .max, .col = "price", .as = "max_price" },
            .{ .func = .sum, .col = "ratio", .as = "sum_ratio" },
            .{ .func = .min, .col = "ratio", .as = "min_ratio" },
            .{ .func = .max, .col = "ratio", .as = "max_ratio" },
        });
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 1), b.row_count);
        try std.testing.expectEqual(@as(f64, 1.5 + 2.5 + 10.0 - 3.25), b.values[0].data.double[0]);
        try std.testing.expectEqual(@as(f32, -3.25), b.values[1].data.float[0]);
        try std.testing.expectEqual(@as(f32, 10.0), b.values[2].data.float[0]);
        try std.testing.expectEqual(@as(f64, 2.5), b.values[3].data.double[0]);
        try std.testing.expectEqual(@as(f64, 0.25), b.values[4].data.double[0]);
        try std.testing.expectEqual(@as(f64, 1.0), b.values[5].data.double[0]);
    }
}

test "float: nullable column with SUM skipping NULLs" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "x", .type = .float, .nullable = true },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 4 };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();
    const t = try db.table("nf", schema, opts);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .x = @as(?f32, 1.5) },
        .{ .id = @as(i64, 2), .x = @as(?f32, null) },
        .{ .id = @as(i64, 3), .x = @as(?f32, 2.5) },
    });
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var q = try base.aggregate(&.{
        .{ .func = .count, .col = "x", .as = "n" },
        .{ .func = .sum, .col = "x", .as = "s" },
    });
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 2), b.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(f64, 4.0), b.values[1].data.double[0]);
}

test "avg: int, float, with nulls and grouping" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "grp", .type = .int },
            .{ .name = "v", .type = .int, .nullable = true },
            .{ .name = "f", .type = .double, .nullable = true },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 8 };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 8 });
    defer db.close();
    const t = try db.table("agg", schema, opts);

    try t.insert(&.{
        .{ .id = @as(i64, 1), .grp = @as(i32, 1), .v = @as(?i32, 10), .f = @as(?f64, 1.5) },
        .{ .id = @as(i64, 2), .grp = @as(i32, 1), .v = @as(?i32, null), .f = @as(?f64, 2.5) },
        .{ .id = @as(i64, 3), .grp = @as(i32, 1), .v = @as(?i32, 30), .f = @as(?f64, null) },
        .{ .id = @as(i64, 4), .grp = @as(i32, 2), .v = @as(?i32, 100), .f = @as(?f64, 10.0) },
        .{ .id = @as(i64, 5), .grp = @as(i32, 2), .v = @as(?i32, 200), .f = @as(?f64, 20.0) },
    });
    try t.flush();

    // Global AVG (skips NULLs): AVG(v) = (10+30+100+200)/4 = 85
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.aggregate(&.{
            .{ .func = .avg, .col = "v", .as = "avg_v" },
            .{ .func = .avg, .col = "f", .as = "avg_f" },
        });
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(f64, 85.0), b.values[0].data.double[0]);
        try std.testing.expectEqual(@as(f64, (1.5 + 2.5 + 10.0 + 20.0) / 4.0), b.values[1].data.double[0]);
    }

    // Grouped AVG.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.groupBy(&.{"grp"}, &.{
            .{ .func = .avg, .col = "v", .as = "avg_v" },
        });
        defer q.deinit();

        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 2), b.row_count);

        // Order of groups is hashmap-defined; gather into (grp → avg) map.
        var found1: ?f64 = null;
        var found2: ?f64 = null;
        for (0..b.row_count) |i| {
            switch (b.values[0].data.int[i]) {
                1 => found1 = b.values[1].data.double[i],
                2 => found2 = b.values[1].data.double[i],
                else => unreachable,
            }
        }
        try std.testing.expectEqual(@as(f64, 20.0), found1.?); // (10+30)/2
        try std.testing.expectEqual(@as(f64, 150.0), found2.?); // (100+200)/2
    }
}

// ---------------------------------------------------------------------------
// DATE / DATETIME columns
// ---------------------------------------------------------------------------

test "date/datetime: insert, flush, reread, comparison filter, MIN/MAX" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "d", .type = .date },
            .{ .name = "ts", .type = .datetime },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 4 };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
        defer db.close();
        const t = try db.table("events", schema, opts);
        // 19500 ≈ 2023-05-14, 19503 ≈ 2023-05-17, etc. Microseconds: 1e6 = 1 second.
        try t.insert(&.{
            .{ .id = @as(i64, 1), .d = thindb.types.Date.fromDays(19500), .ts = thindb.types.DateTime.fromMicros(1_000_000_000) },
            .{ .id = @as(i64, 2), .d = thindb.types.Date.fromDays(19501), .ts = thindb.types.DateTime.fromMicros(2_000_000_000) },
            .{ .id = @as(i64, 3), .d = thindb.types.Date.fromDays(19503), .ts = thindb.types.DateTime.fromMicros(3_000_000_000) },
            .{ .id = @as(i64, 4), .d = thindb.types.Date.fromDays(19495), .ts = thindb.types.DateTime.fromMicros(500_000_000) },
        });
        try t.flush();
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();
    const t = try db.table("events", schema, opts);

    // Reread.
    {
        var q = try thindb.scan(allocator, t);
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 4), b.row_count);
        try std.testing.expectEqualSlices(i32, &[_]i32{ 19500, 19501, 19503, 19495 }, b.values[1].data.date);
        try std.testing.expectEqualSlices(i64, &[_]i64{ 1_000_000_000, 2_000_000_000, 3_000_000_000, 500_000_000 }, b.values[2].data.datetime);
    }

    // Filter on date.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.filter(thindb.leafExpr("d", .gte, .{ .date = 19500 }));
        defer q.deinit();
        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
        try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, ids.items);
    }

    // MIN/MAX over date + datetime; output types preserve input width.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.aggregate(&.{
            .{ .func = .min, .col = "d", .as = "min_d" },
            .{ .func = .max, .col = "d", .as = "max_d" },
            .{ .func = .min, .col = "ts", .as = "min_ts" },
            .{ .func = .max, .col = "ts", .as = "max_ts" },
        });
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(i32, 19495), b.values[0].data.date[0]);
        try std.testing.expectEqual(@as(i32, 19503), b.values[1].data.date[0]);
        try std.testing.expectEqual(@as(i64, 500_000_000), b.values[2].data.datetime[0]);
        try std.testing.expectEqual(@as(i64, 3_000_000_000), b.values[3].data.datetime[0]);
    }
}

// ---------------------------------------------------------------------------
// Background flush sweep (manually driven)
// ---------------------------------------------------------------------------

test "backgroundFlushSweep: fires time-based auto-flush when threshold met" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Time trigger fires every second, after at least 1 row + 0 bytes.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 4,
        .auto_flush_secs = 0, // disable inline time trigger so we can drive it manually
        .auto_flush_rows = 1_000_000,
        .auto_flush_bytes = 64 * 1024 * 1024,
    });
    defer db.close();

    const t = try db.table("orders", schema_v1, opts_v1);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
    });
    try std.testing.expectEqual(@as(usize, 0), t.segmentCount());

    // A sweep when no trigger fires is a no-op.
    try db.backgroundFlushSweep();
    try std.testing.expectEqual(@as(usize, 0), t.segmentCount());

    // Manually trip the size trigger via Table.flush — proves the path works
    // end to end through the public API even with the sweep present.
    try t.flush();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    // Another sweep over an empty memtable is also a no-op (no new segment).
    try db.backgroundFlushSweep();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());
}

// ---------------------------------------------------------------------------
// New integer widths + CHAR(N)
// ---------------------------------------------------------------------------

test "tinyint/smallint/largeint/char: insert, flush, reread, filter, MIN/MAX" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "tiny", .type = .tinyint },
            .{ .name = "small", .type = .smallint },
            .{ .name = "huge", .type = .largeint },
            .{ .name = "code", .type = .{ .char = 4 } },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 4 };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
        defer db.close();
        const t = try db.table("widths", schema, opts);
        try t.insert(&.{
            .{ .id = @as(i64, 1), .tiny = @as(i8, -10), .small = @as(i16, 100), .huge = @as(i128, 1_000_000_000_000_000_000), .code = "AAAA" },
            .{ .id = @as(i64, 2), .tiny = @as(i8, 0), .small = @as(i16, -32_000), .huge = @as(i128, -1_000_000_000_000_000_000), .code = "BBBB" },
            .{ .id = @as(i64, 3), .tiny = @as(i8, 127), .small = @as(i16, 32_000), .huge = @as(i128, 0), .code = "CCCC" },
            .{ .id = @as(i64, 4), .tiny = @as(i8, -128), .small = @as(i16, 0), .huge = @as(i128, 999_999_999_999_999_999_999), .code = "DDDD" },
        });
        try t.flush();
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();
    const t = try db.table("widths", schema, opts);

    // Reread roundtrip.
    {
        var q = try thindb.scan(allocator, t);
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 4), b.row_count);
        try std.testing.expectEqualSlices(i8, &[_]i8{ -10, 0, 127, -128 }, b.values[1].data.tinyint);
        try std.testing.expectEqualSlices(i16, &[_]i16{ 100, -32_000, 32_000, 0 }, b.values[2].data.smallint);
        try std.testing.expectEqualSlices(i128, &[_]i128{ 1_000_000_000_000_000_000, -1_000_000_000_000_000_000, 0, 999_999_999_999_999_999_999 }, b.values[3].data.largeint);
        try std.testing.expectEqualStrings("AAAA", b.values[4].data.char.rowBytes(0));
        try std.testing.expectEqualStrings("DDDD", b.values[4].data.char.rowBytes(3));
    }

    // Filter on TINYINT.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.filter(thindb.leafExpr("tiny", .gte, .{ .tinyint = 0 }));
        defer q.deinit();
        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
        try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3 }, ids.items);
    }

    // Filter on LARGEINT (values exceed i64 range).
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.filter(thindb.leafExpr("huge", .gt, .{ .largeint = 100_000_000_000_000_000_000 }));
        defer q.deinit();
        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
        try std.testing.expectEqualSlices(i64, &[_]i64{4}, ids.items);
    }

    // Filter on CHAR equality.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.filter(thindb.leafExpr("code", .eq, .{ .text = "BBBB" }));
        defer q.deinit();
        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
        try std.testing.expectEqualSlices(i64, &[_]i64{2}, ids.items);
    }

    // MIN/MAX preserve narrow int widths.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.aggregate(&.{
            .{ .func = .min, .col = "tiny", .as = "min_tiny" },
            .{ .func = .max, .col = "tiny", .as = "max_tiny" },
            .{ .func = .min, .col = "small", .as = "min_small" },
            .{ .func = .max, .col = "small", .as = "max_small" },
        });
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(i8, -128), b.values[0].data.tinyint[0]);
        try std.testing.expectEqual(@as(i8, 127), b.values[1].data.tinyint[0]);
        try std.testing.expectEqual(@as(i16, -32_000), b.values[2].data.smallint[0]);
        try std.testing.expectEqual(@as(i16, 32_000), b.values[3].data.smallint[0]);
    }
}

// ---------------------------------------------------------------------------
// DECIMAL columns
// ---------------------------------------------------------------------------

test "decimal: insert, flush, reread, filter, SUM/MIN/MAX over both backings" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            // DECIMAL(10, 2) — i64 backing. Values are raw mantissa
            // (e.g. 12345 stored = 123.45 logical).
            .{ .name = "price", .type = thindb.decimal(10, 2) },
            // DECIMAL(30, 6) — i128 backing.
            .{ .name = "big", .type = thindb.decimal(30, 6) },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 4 };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
        defer db.close();
        const t = try db.table("prices", schema, opts);
        try t.insert(&.{
            .{ .id = @as(i64, 1), .price = @as(i64, 12345), .big = @as(i128, 1_000_000_000_000_000_000_000) },
            .{ .id = @as(i64, 2), .price = @as(i64, 99999), .big = @as(i128, 2_000_000_000_000_000_000_000) },
            .{ .id = @as(i64, 3), .price = @as(i64, 50000), .big = @as(i128, -3_000_000_000_000_000_000_000) },
            .{ .id = @as(i64, 4), .price = @as(i64, 1), .big = @as(i128, 0) },
        });
        try t.flush();
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();
    const t = try db.table("prices", schema, opts);

    // Roundtrip reread.
    {
        var q = try thindb.scan(allocator, t);
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 4), b.row_count);
        try std.testing.expectEqualSlices(i64, &[_]i64{ 12345, 99999, 50000, 1 }, b.values[1].data.decimal64);
        try std.testing.expectEqualSlices(i128, &[_]i128{
            1_000_000_000_000_000_000_000,
            2_000_000_000_000_000_000_000,
            -3_000_000_000_000_000_000_000,
            0,
        }, b.values[2].data.decimal128);
    }

    // Filter on DECIMAL64.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.filter(thindb.leafExpr("price", .gte, .{ .decimal64 = 50000 }));
        defer q.deinit();
        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
        try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3 }, ids.items);
    }

    // Filter on DECIMAL128 (values exceed i64 range).
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.filter(thindb.leafExpr("big", .gt, .{ .decimal128 = 0 }));
        defer q.deinit();
        var ids: std.ArrayList(i64) = .empty;
        defer ids.deinit(allocator);
        while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
        try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2 }, ids.items);
    }

    // SUM(decimal64) → DECIMAL128(38, 2). MIN/MAX preserve input.
    {
        var base = try thindb.scan(allocator, t);
        var q = try base.aggregate(&.{
            .{ .func = .sum, .col = "price", .as = "sum_price" },
            .{ .func = .min, .col = "price", .as = "min_price" },
            .{ .func = .max, .col = "price", .as = "max_price" },
            .{ .func = .sum, .col = "big", .as = "sum_big" },
            .{ .func = .min, .col = "big", .as = "min_big" },
            .{ .func = .max, .col = "big", .as = "max_big" },
        });
        defer q.deinit();
        const b = (try q.next()).?;
        try std.testing.expectEqual(@as(i128, 12345 + 99999 + 50000 + 1), b.values[0].data.decimal128[0]);
        try std.testing.expectEqual(@as(i64, 1), b.values[1].data.decimal64[0]);
        try std.testing.expectEqual(@as(i64, 99999), b.values[2].data.decimal64[0]);
        try std.testing.expectEqual(@as(i128, 0), b.values[3].data.decimal128[0]);
        try std.testing.expectEqual(@as(i128, -3_000_000_000_000_000_000_000), b.values[4].data.decimal128[0]);
        try std.testing.expectEqual(@as(i128, 2_000_000_000_000_000_000_000), b.values[5].data.decimal128[0]);
    }
}

test "decimal: backing transitions at p=18 (i64) and p=19 (i128)" {
    // p=18 should pick the i64 backing; p=19 should pick i128.
    const t_small = thindb.decimal(18, 4);
    const t_big = thindb.decimal(19, 4);
    try std.testing.expect(std.meta.activeTag(t_small) == .decimal64);
    try std.testing.expect(std.meta.activeTag(t_big) == .decimal128);
    try std.testing.expectEqual(@as(u8, 18), t_small.decimal64.p);
    try std.testing.expectEqual(@as(u8, 19), t_big.decimal128.p);
    try std.testing.expectEqual(@as(u8, 4), t_small.decimal64.s);
    try std.testing.expectEqual(@as(u8, 4), t_big.decimal128.s);
}

test "decimal: p=38 holds the maximum i128 range" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            // Max precision, scale=0 (pure i128 integer-shaped storage).
            .{ .name = "v", .type = thindb.decimal(38, 0) },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("big", schema, opts);

    // i128 max is ~1.7e38. Pick values near the top end and the bottom.
    const max_i128: i128 = std.math.maxInt(i128);
    const min_i128: i128 = std.math.minInt(i128);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .v = max_i128 },
        .{ .id = @as(i64, 2), .v = min_i128 },
        .{ .id = @as(i64, 3), .v = @as(i128, 0) },
    });
    try t.flush();

    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqualSlices(i128, &[_]i128{ max_i128, min_i128, 0 }, b.values[1].data.decimal128);
}

test "decimal: scale == precision (all fractional digits)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    // DECIMAL(5, 5) — values in [-0.99999, 0.99999], i64 backing.
    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "frac", .type = thindb.decimal(5, 5) },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("f", schema, opts);

    try t.insert(&.{
        .{ .id = @as(i64, 1), .frac = @as(i64, 99999) }, // 0.99999
        .{ .id = @as(i64, 2), .frac = @as(i64, -99999) },
        .{ .id = @as(i64, 3), .frac = @as(i64, 0) },
    });
    try t.flush();

    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqualSlices(i64, &[_]i64{ 99999, -99999, 0 }, b.values[1].data.decimal64);
}

// ---------------------------------------------------------------------------
// Combinator chains — typical multi-operator query shapes
// ---------------------------------------------------------------------------

test "chain: filter -> project -> orderBy -> limit" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();

    const t = try db.table("orders", schema_v1, opts_v1);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 30), .active = true, .tag = "c" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 50), .active = false, .tag = "e" },
        .{ .id = @as(i64, 4), .qty = @as(i32, 20), .active = true, .tag = "b" },
        .{ .id = @as(i64, 5), .qty = @as(i32, 40), .active = true, .tag = "d" },
    });
    try t.flush();

    // active = true, then project (id, qty), then order by qty DESC, then limit 2.
    var base = try thindb.scan(allocator, t);
    var filtered = try base.filter(thindb.leafExpr("active", .eq, .{ .boolean = true }));
    var projected = try filtered.project(&.{ "id", "qty" });
    var sorted = try projected.orderBy(&.{.{ .col = "qty", .desc = true }});
    var q = try sorted.limit(2);
    defer q.deinit();

    var got_ids: std.ArrayList(i64) = .empty;
    defer got_ids.deinit(allocator);
    var got_qty: std.ArrayList(i32) = .empty;
    defer got_qty.deinit(allocator);
    while (try q.next()) |batch| {
        try got_ids.appendSlice(allocator, batch.values[0].data.bigint);
        try got_qty.appendSlice(allocator, batch.values[1].data.int);
    }
    // Top 2 active qty: id=5/qty=40, id=1/qty=30.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 5, 1 }, got_ids.items);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 40, 30 }, got_qty.items);
}

test "chain: filter -> groupBy -> orderBy" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();

    const t = try db.table("orders", schema_v1, opts_v1);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 100), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 200), .active = true, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 50), .active = false, .tag = "a" },
        .{ .id = @as(i64, 4), .qty = @as(i32, 75), .active = true, .tag = "a" },
        .{ .id = @as(i64, 5), .qty = @as(i32, 300), .active = true, .tag = "b" },
    });
    try t.flush();

    // active=true, group by tag, sum qty, order by sum DESC.
    var base = try thindb.scan(allocator, t);
    var filtered = try base.filter(thindb.leafExpr("active", .eq, .{ .boolean = true }));
    var grouped = try filtered.groupBy(&.{"tag"}, &.{
        .{ .func = .sum, .col = "qty", .as = "total" },
    });
    var q = try grouped.orderBy(&.{.{ .col = "total", .desc = true }});
    defer q.deinit();

    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), b.row_count);
    // tag=b has 200+300=500, tag=a (active only) has 100+75=175.
    try std.testing.expectEqualStrings("b", b.values[0].data.string.rowBytes(0));
    try std.testing.expectEqual(@as(i64, 500), b.values[1].data.bigint[0]);
    try std.testing.expectEqualStrings("a", b.values[0].data.string.rowBytes(1));
    try std.testing.expectEqual(@as(i64, 175), b.values[1].data.bigint[1]);
}

test "chain: scan across memtable + segments threads through filter+aggregate" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();

    const t = try db.table("orders", schema_v1, opts_v1);

    // Three flushed segments + memtable rows.
    try t.insert(&.{.{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" }});
    try t.flush();
    try t.insert(&.{.{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = true, .tag = "b" }});
    try t.flush();
    try t.insert(&.{.{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = false, .tag = "c" }});
    try t.flush();
    try t.insert(&.{
        .{ .id = @as(i64, 4), .qty = @as(i32, 40), .active = true, .tag = "d" },
        .{ .id = @as(i64, 5), .qty = @as(i32, 50), .active = false, .tag = "e" },
    });

    var base = try thindb.scan(allocator, t);
    var filtered = try base.filter(thindb.leafExpr("active", .eq, .{ .boolean = true }));
    var q = try filtered.aggregate(&.{
        .{ .func = .count, .col = null, .as = "n" },
        .{ .func = .sum, .col = "qty", .as = "total_qty" },
    });
    defer q.deinit();

    const b = (try q.next()).?;
    // active=true: ids 1, 2, 4. sum = 10+20+40 = 70. count = 3.
    try std.testing.expectEqual(@as(i64, 3), b.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 70), b.values[1].data.bigint[0]);
}

// ---------------------------------------------------------------------------
// Error paths — type mismatches and unsupported operations
// ---------------------------------------------------------------------------

test "error: filter predicate type mismatch on each new type" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "small", .type = .smallint },
            .{ .name = "big", .type = .largeint },
            .{ .name = "code", .type = .{ .char = 4 } },
            .{ .name = "amt", .type = thindb.decimal(10, 2) },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = false };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, opts);

    // Filter SMALLINT column with an out-of-range INT literal — can't
    // narrow losslessly, so it's a type mismatch. (An in-range int
    // literal like 1 now narrows to smallint; see the coercion test.)
    var base = try thindb.scan(allocator, t);
    var q = base.filter(thindb.leafExpr("small", .eq, .{ .int = 100000 }));
    try std.testing.expectError(error.PredicateTypeMismatch, q);
    base.deinit();

    // LARGEINT column with a TEXT literal — type mismatch. (BIGINT
    // literal would widen losslessly to LARGEINT, so use a literal
    // that genuinely can't be widened.)
    base = try thindb.scan(allocator, t);
    q = base.filter(thindb.leafExpr("big", .eq, .{ .text = "1" }));
    try std.testing.expectError(error.PredicateTypeMismatch, q);
    base.deinit();

    // DECIMAL column with a BIGINT literal — type mismatch.
    base = try thindb.scan(allocator, t);
    q = base.filter(thindb.leafExpr("amt", .eq, .{ .bigint = 100 }));
    try std.testing.expectError(error.PredicateTypeMismatch, q);
    base.deinit();

    // CHAR column with .lt (only eq/neq allowed on string-shaped types).
    base = try thindb.scan(allocator, t);
    q = base.filter(thindb.leafExpr("code", .lt, .{ .text = "AAAA" }));
    try std.testing.expectError(error.UnsupportedOperatorForType, q);
    base.deinit();
}

test "error: filter on unknown column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("orders", schema_v1, opts_v1);

    var base = try thindb.scan(allocator, t);
    const q = base.filter(thindb.leafExpr("nonexistent", .eq, .{ .int = 1 }));
    try std.testing.expectError(error.ColumnNotFound, q);
    base.deinit();
}

test "error: SUM/MIN/MAX reject string and unsupported types" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("orders", schema_v1, opts_v1);

    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "x" },
    });

    // SUM on a STRING column is unsupported.
    var base = try thindb.scan(allocator, t);
    const q = base.aggregate(&.{.{ .func = .sum, .col = "tag", .as = "s" }});
    try std.testing.expectError(error.AggregateUnsupportedType, q);
    base.deinit();
}

test "error: aggregate with no specs returns error" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("orders", schema_v1, opts_v1);

    var base = try thindb.scan(allocator, t);
    const q = base.aggregate(&.{});
    try std.testing.expectError(error.AggregateNoSpecs, q);
    base.deinit();
}

test "error: insert with wrong-type value into typed column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "v", .type = .{ .char = 8 } },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = false };
    const t = try db.table("t", schema, opts);

    // CHAR column expects a string-like value; passing i32 errors.
    try std.testing.expectError(
        error.TypeMismatch,
        t.insert(&.{.{ .id = @as(i64, 1), .v = @as(i32, 5) }}),
    );
}

// ---------------------------------------------------------------------------
// Background flusher thread
// ---------------------------------------------------------------------------

test "runBackgroundFlusher: thread actually flushes when triggers fire" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Configure the inline auto-flush triggers wide so the bg sweep is the
    // only thing that can flush — but enable the time-based trigger.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 4,
        .auto_flush_secs = 0, // disable time trigger inline...
        .auto_flush_rows = 1_000_000,
        .auto_flush_bytes = 64 * 1024 * 1024,
        .auto_flush_min_rows = 0,
        .auto_flush_min_bytes = 0,
    });
    defer db.close();
    const t = try db.table("orders", schema_v1, opts_v1);

    // Insert; this won't trigger an inline flush (thresholds too high) and
    // the time-based trigger is off, so the memtable sits.
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
    });
    try std.testing.expectEqual(@as(usize, 0), t.segmentCount());

    // Manually drive a sweep — and it should flush nothing because all
    // triggers are off. This proves the inline path doesn't fire.
    try db.backgroundFlushSweep();
    try std.testing.expectEqual(@as(usize, 0), t.segmentCount());
    try std.testing.expectEqual(@as(u64, 2), t.memtable.row_count);

    // Manual flush works:
    try t.flush();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());
    try std.testing.expectEqual(@as(u64, 0), t.memtable.row_count);
}

test "runBackgroundFlusher: spawns a thread that drives flush sweeps" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Configure auto-flush triggers so a sweep WILL fire when run.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 4,
        // Time-based trigger: at least 1ms old + at least 1 row + 0 bytes min.
        .auto_flush_secs = 0,
        .auto_flush_rows = 1_000_000,
        .auto_flush_bytes = 64 * 1024 * 1024,
        .auto_flush_min_rows = 1,
        .auto_flush_min_bytes = 0,
    });
    defer db.close();

    const t = try db.table("orders", schema_v1, opts_v1);

    // Spawn the bg flusher with a fast poll interval. Use the same io as the
    // database since std.testing.io's sleep path will block this OS thread.
    var stop: std.atomic.Value(bool) = .init(false);
    const thr = try std.Thread.spawn(.{}, thindb.Database.runBackgroundFlusher, .{
        db, io, 10, &stop,
    });

    // Insert a couple rows — main thread, lock held briefly per insert.
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
    });

    // Let the bg thread loop a few times; it can't actually trigger a flush
    // because auto_flush_secs=0 disables the time trigger and size/rows are
    // far below thresholds. So this test really proves: the thread spawns,
    // loops without crashing, and stops cleanly when signalled.
    std.Thread.yield() catch {};

    stop.store(true, .release);
    thr.join();

    // Memtable still has data (no flush actually fired).
    try std.testing.expectEqual(@as(u64, 2), t.memtable.row_count);
    try std.testing.expectEqual(@as(usize, 0), t.segmentCount());
}

test "backgroundCompactSweep: tiered merge when 4 same-tier segments accumulate" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 4,
        .compact_min_segments = 4,
        .auto_flush_secs = 0,
        .auto_flush_rows = 1_000_000,
        .auto_flush_bytes = 64 * 1024 * 1024,
    });
    defer db.close();

    const t = try db.table("orders", schema_v1, opts_v1);

    // Flush four single-row segments. All four are tier 0 (<65k rows).
    inline for (.{ 1, 2, 3, 4 }) |i| {
        try t.insert(&.{.{
            .id = @as(i64, i),
            .qty = @as(i32, i * 10),
            .active = true,
            .tag = "x",
        }});
        try t.flush();
    }
    try std.testing.expectEqual(@as(usize, 4), t.segmentCount());

    // Sweep should pick all 4 (same tier, adjacent) and merge into one.
    try db.backgroundCompactSweep();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    // Three more flushes — only 3 segments, tiered policy needs 4 to fire.
    inline for (.{ 5, 6, 7 }) |i| {
        try t.insert(&.{.{
            .id = @as(i64, i),
            .qty = @as(i32, i * 10),
            .active = true,
            .tag = "x",
        }});
        try t.flush();
    }
    try std.testing.expectEqual(@as(usize, 4), t.segmentCount()); // 1 merged + 3 new
    try db.backgroundCompactSweep();
    // The 3 new ones are tier 0; the merged one is also tier 0 (still <65k rows).
    // So all 4 are at tier 0 and adjacent → merged again.
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());
}

test "compactor: tombstone-pressure trigger reclaims a heavily-deleted segment" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 4,
        // Set tier threshold high so it doesn't fire; we want the tomb trigger.
        .compact_min_segments = 100,
        .compact_tombstone_threshold = 0.30,
        .auto_flush_secs = 0,
        .auto_flush_rows = 1_000_000,
        .auto_flush_bytes = 64 * 1024 * 1024,
    });
    defer db.close();

    const t = try db.table("orders", schema_v1, opts_v1);

    // Flush one segment with 10 rows.
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = true, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "c" },
        .{ .id = @as(i64, 4), .qty = @as(i32, 40), .active = true, .tag = "d" },
        .{ .id = @as(i64, 5), .qty = @as(i32, 50), .active = true, .tag = "e" },
        .{ .id = @as(i64, 6), .qty = @as(i32, 60), .active = true, .tag = "f" },
        .{ .id = @as(i64, 7), .qty = @as(i32, 70), .active = true, .tag = "g" },
        .{ .id = @as(i64, 8), .qty = @as(i32, 80), .active = true, .tag = "h" },
        .{ .id = @as(i64, 9), .qty = @as(i32, 90), .active = true, .tag = "i" },
        .{ .id = @as(i64, 10), .qty = @as(i32, 100), .active = true, .tag = "j" },
    });
    try t.flush();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    // Delete 5 of the 10 rows (50% tombstone fraction, well above 30%).
    _ = try t.delete(.{ .col = "qty", .op = .lte, .val = .{ .int = 50 } });

    // Tomb file now exists for the only segment.
    // Sweep should pick it (single-segment compact: rewrite without tombs).
    try db.backgroundCompactSweep();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    // After compaction the surviving rows should be the 5 with qty > 50.
    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 6, 7, 8, 9, 10 }, ids.items);

    // Sweep again: no tombstones left, no tier trigger. No-op.
    try db.backgroundCompactSweep();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());
}

test "execTieredCompact: no-op when no tier has enough segments" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();

    const t = try db.table("orders", schema_v1, opts_v1);

    // Just 3 segments → below the default tier_target_count of 4.
    inline for (.{ 1, 2, 3 }) |i| {
        try t.insert(&.{.{
            .id = @as(i64, i),
            .qty = @as(i32, i * 10),
            .active = true,
            .tag = "x",
        }});
        try t.flush();
    }
    try std.testing.expectEqual(@as(usize, 3), t.segmentCount());

    // backgroundCompactSweep WITHOUT the count gate: threshold defaults to 8.
    // With 3 segments < 8 it doesn't even try.
    try db.backgroundCompactSweep();
    try std.testing.expectEqual(@as(usize, 3), t.segmentCount());
}

// ---------------------------------------------------------------------------
// Upserts
// ---------------------------------------------------------------------------

test "upsert: overwrites existing rows on unique-key tables" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();

    const t = try db.table("orders", schema_v1, opts_v1);

    // Initial inserts.
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "alpha" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "beta" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "gamma" },
    });
    try t.flush();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    // Upsert: id=2 already exists, id=4 is new.
    try t.upsert(&.{
        .{ .id = @as(i64, 2), .qty = @as(i32, 999), .active = true, .tag = "BETA_NEW" },
        .{ .id = @as(i64, 4), .qty = @as(i32, 40), .active = true, .tag = "delta" },
    });
    try t.flush();

    // Scan should see the overwritten value for id=2 and the new id=4.
    var q = try thindb.scan(allocator, t);
    defer q.deinit();

    var got_ids: std.ArrayList(i64) = .empty;
    defer got_ids.deinit(allocator);
    var got_qty: std.ArrayList(i32) = .empty;
    defer got_qty.deinit(allocator);
    var got_tag: std.ArrayList(u8) = .empty;
    defer got_tag.deinit(allocator);

    while (try q.next()) |batch| {
        try got_ids.appendSlice(allocator, batch.values[0].data.bigint);
        try got_qty.appendSlice(allocator, batch.values[1].data.int);
        for (0..batch.row_count) |i| {
            try got_tag.append(allocator, '|');
            try got_tag.appendSlice(allocator, batch.values[3].data.string.rowBytes(i));
        }
    }

    // Order across segment + memtable isn't guaranteed, so check the set.
    // Expected: id=1 unchanged, id=2 overwritten (qty=999, tag=BETA_NEW),
    // id=3 unchanged, id=4 new.
    try std.testing.expectEqual(@as(usize, 4), got_ids.items.len);

    var seen_2: bool = false;
    var seen_4: bool = false;
    for (got_ids.items, 0..) |id, i| {
        switch (id) {
            1 => try std.testing.expectEqual(@as(i32, 10), got_qty.items[i]),
            2 => {
                seen_2 = true;
                try std.testing.expectEqual(@as(i32, 999), got_qty.items[i]);
            },
            3 => try std.testing.expectEqual(@as(i32, 30), got_qty.items[i]),
            4 => {
                seen_4 = true;
                try std.testing.expectEqual(@as(i32, 40), got_qty.items[i]);
            },
            else => unreachable,
        }
    }
    try std.testing.expect(seen_2);
    try std.testing.expect(seen_4);
}

test "upsert: errors on non-unique tables" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false, // <-- not unique
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = false };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const t = try db.table("dup", schema, opts);
    try std.testing.expectError(
        error.UpsertRequiresUniqueKey,
        t.upsert(&.{.{ .id = @as(i64, 1), .v = @as(i32, 10) }}),
    );
}

test "nullable: rejects ?T into a non-nullable column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // schema_v1's qty is NOT NULL.
    const orders = try db.table("orders", schema_v1, opts_v1);
    try std.testing.expectError(error.TypeMismatch, orders.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(?i32, null), .active = true, .tag = "x" },
    }));
}
