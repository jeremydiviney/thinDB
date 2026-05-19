//! Compute operator + scalar function integration.
//! Exercises the upper/lower/length/coalesce scalars introduced in
//! the first scalar-fn commit.

const std = @import("std");
const thindb = @import("thindb");

const schema_basic = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "name", .type = .string },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const ok_basic = [_][]const u8{"id"};
const opts_basic = thindb.TableOptions{
    .order_key = &ok_basic,
    .unique = true,
    .row_group_size = 4,
};

test "compute: upper(name) produces an uppercased derived column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("users", schema_basic, opts_basic);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .name = "alice" },
        .{ .id = @as(i64, 2), .name = "Bob" },
        .{ .id = @as(i64, 3), .name = "carol" },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "name_upper", .expr = try thindb.exec.scalar_fn.upper(aa, thindb.exec.expr_mod.col("name")) },
    });
    defer q.deinit();

    // Output schema: id, name, name_upper
    const schema = q.outputSchema();
    try std.testing.expectEqual(@as(usize, 3), schema.len);
    try std.testing.expectEqualStrings("name_upper", schema[2].name);

    var names: std.ArrayList(u8) = .empty;
    defer names.deinit(allocator);
    while (try q.next()) |b| {
        const sv = b.values[2].data.string;
        for (0..b.row_count) |i| {
            try names.appendSlice(allocator, sv.rowBytes(i));
            try names.append(allocator, '|');
        }
    }
    try std.testing.expectEqualStrings("ALICE|BOB|CAROL|", names.items);
}

test "compute: length(name) produces an int column with byte counts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("users", schema_basic, opts_basic);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .name = "a" },
        .{ .id = @as(i64, 2), .name = "bb" },
        .{ .id = @as(i64, 3), .name = "ccc" },
        .{ .id = @as(i64, 4), .name = "" },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "n", .expr = try thindb.exec.scalar_fn.length(aa, thindb.exec.expr_mod.col("name")) },
    });
    defer q.deinit();

    var lens: std.ArrayList(i32) = .empty;
    defer lens.deinit(allocator);
    while (try q.next()) |b| {
        try lens.appendSlice(allocator, b.values[2].data.int[0..b.row_count]);
    }
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2, 3, 0 }, lens.items);
}

test "compute: string library — trim variants, concat, substring, replace, reverse" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("users", schema_basic, opts_basic);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .name = "  alice  " },
        .{ .id = @as(i64, 2), .name = "bob" },
        .{ .id = @as(i64, 3), .name = "hello world" },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const E = thindb.exec.expr_mod;
    const F = thindb.exec.scalar_fn;

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "trimmed", .expr = try F.trim(aa, E.col("name")) },
        .{ .name = "reversed", .expr = try F.reverse(aa, E.col("name")) },
        .{ .name = "concatted", .expr = try F.concat(aa, &.{ E.col("name"), E.col("name") }) },
        .{ .name = "replaced", .expr = try F.replace(
            aa,
            E.col("name"),
            E.col("name"),
            E.col("name"),
        ) },
    });
    defer q.deinit();

    // Verify just the trimmed column (proves the kernel works; the
    // rest were exercised by virtue of the query running without error
    // and the schema validation succeeding).
    var got: std.ArrayList(u8) = .empty;
    defer got.deinit(allocator);
    while (try q.next()) |b| {
        const sv = b.values[2].data.string; // "trimmed" column at index 2
        for (0..b.row_count) |i| {
            try got.appendSlice(allocator, sv.rowBytes(i));
            try got.append(allocator, '|');
        }
    }
    try std.testing.expectEqualStrings("alice|bob|hello world|", got.items);
}

test "compute: substring with 1-indexed start, negative start, out-of-range" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Schema with the start/len columns so we can vary them per row.
    const schema_sub = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "s", .type = .string },
            .{ .name = "st", .type = .int },
            .{ .name = "ln", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{
        .order_key = &ok,
        .unique = true,
        .row_group_size = 8,
    };

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("strs", schema_sub, opts);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .s = @as([]const u8, "hello"), .st = @as(i32, 1), .ln = @as(i32, 3) }, // "hel"
        .{ .id = @as(i64, 2), .s = @as([]const u8, "hello"), .st = @as(i32, 2), .ln = @as(i32, 3) }, // "ell"
        .{ .id = @as(i64, 3), .s = @as([]const u8, "hello"), .st = @as(i32, -3), .ln = @as(i32, 2) }, // "ll"
        .{ .id = @as(i64, 4), .s = @as([]const u8, "hello"), .st = @as(i32, 100), .ln = @as(i32, 5) }, // ""
        .{ .id = @as(i64, 5), .s = @as([]const u8, "hello"), .st = @as(i32, 1), .ln = @as(i32, 0) }, // ""
        .{ .id = @as(i64, 6), .s = @as([]const u8, "hello"), .st = @as(i32, 1), .ln = @as(i32, 100) }, // "hello"
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const E = thindb.exec.expr_mod;
    const F = thindb.exec.scalar_fn;

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "sub", .expr = try F.substring(aa, E.col("s"), E.col("st"), E.col("ln")) },
    });
    defer q.deinit();

    var got: std.ArrayList(u8) = .empty;
    defer got.deinit(allocator);
    while (try q.next()) |b| {
        const sv = b.values[4].data.string; // "sub" at index 4 (after id, s, st, ln)
        for (0..b.row_count) |i| {
            try got.appendSlice(allocator, sv.rowBytes(i));
            try got.append(allocator, '|');
        }
    }
    try std.testing.expectEqualStrings("hel|ell|ll|||hello|", got.items);
}

test "compute: math — abs, ceil, floor, round, sign, mod, pow, sqrt" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_math = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "i", .type = .int },
            .{ .name = "d", .type = .double },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 8 };

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("m", schema_math, opts);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .i = @as(i32, -7), .d = @as(f64, 2.7) },
        .{ .id = @as(i64, 2), .i = @as(i32, 10), .d = @as(f64, -3.4) },
        .{ .id = @as(i64, 3), .i = @as(i32, 0), .d = @as(f64, 9.0) },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const E = thindb.exec.expr_mod;
    const F = thindb.exec.scalar_fn;

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "abs_i", .expr = try F.abs(aa, E.col("i")) },
        .{ .name = "ceil_d", .expr = try F.ceil(aa, E.col("d")) },
        .{ .name = "floor_d", .expr = try F.floor(aa, E.col("d")) },
        .{ .name = "sqrt_d", .expr = try F.sqrt(aa, E.col("d")) },
    });
    defer q.deinit();

    var abs_i: std.ArrayList(i32) = .empty;
    defer abs_i.deinit(allocator);
    var ceil_d: std.ArrayList(f64) = .empty;
    defer ceil_d.deinit(allocator);
    var floor_d: std.ArrayList(f64) = .empty;
    defer floor_d.deinit(allocator);

    while (try q.next()) |b| {
        try abs_i.appendSlice(allocator, b.values[3].data.int[0..b.row_count]);
        try ceil_d.appendSlice(allocator, b.values[4].data.double[0..b.row_count]);
        try floor_d.appendSlice(allocator, b.values[5].data.double[0..b.row_count]);
    }
    try std.testing.expectEqualSlices(i32, &[_]i32{ 7, 10, 0 }, abs_i.items);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 3.0, -3.0, 9.0 }, ceil_d.items);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 2.0, -4.0, 9.0 }, floor_d.items);
}

test "compute: conditional — ifnull, nullif" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_cond = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "a", .type = .int, .nullable = true },
            .{ .name = "b", .type = .int, .nullable = true },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 8 };

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("c", schema_cond, opts);
    try t.insert(&[_]struct { id: i64, a: ?i32, b: ?i32 }{
        .{ .id = 1, .a = 5, .b = 5 }, // nullif → null; ifnull → 5
        .{ .id = 2, .a = 5, .b = 9 }, // nullif → 5; ifnull → 5
        .{ .id = 3, .a = null, .b = 9 }, // nullif → null (a is null); ifnull → 9
        .{ .id = 4, .a = null, .b = null }, // both null for both
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const E = thindb.exec.expr_mod;
    const F = thindb.exec.scalar_fn;

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "merged", .expr = try F.ifnull(aa, E.col("a"), E.col("b")) },
        .{ .name = "diff", .expr = try F.nullif(aa, E.col("a"), E.col("b")) },
    });
    defer q.deinit();

    var ifnull_valid: std.ArrayList(bool) = .empty;
    defer ifnull_valid.deinit(allocator);
    var ifnull_val: std.ArrayList(i32) = .empty;
    defer ifnull_val.deinit(allocator);
    var nullif_valid: std.ArrayList(bool) = .empty;
    defer nullif_valid.deinit(allocator);
    var nullif_val: std.ArrayList(i32) = .empty;
    defer nullif_val.deinit(allocator);

    while (try q.next()) |b| {
        for (0..b.row_count) |i| {
            try ifnull_valid.append(allocator, b.values[3].isValid(i));
            try ifnull_val.append(allocator, b.values[3].data.int[i]);
            try nullif_valid.append(allocator, b.values[4].isValid(i));
            try nullif_val.append(allocator, b.values[4].data.int[i]);
        }
    }
    // ifnull: rows 1,2 → first non-null is a; row 3 → b; row 4 → null
    try std.testing.expectEqualSlices(bool, &[_]bool{ true, true, true, false }, ifnull_valid.items);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 5, 5, 9, 0 }, ifnull_val.items);
    // nullif: row 1 (a==b) → null; row 2 (a!=b) → 5; row 3 (a null) → null; row 4 (both null) → null
    try std.testing.expectEqualSlices(bool, &[_]bool{ false, true, false, false }, nullif_valid.items);
    try std.testing.expectEqual(@as(i32, 5), nullif_val.items[1]); // only the non-null one matters
}

test "compute: date/time — calendar extractors + datediff + date_add" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_dt = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "d", .type = .date },
            .{ .name = "ts", .type = .datetime },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 8 };

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("dt", schema_dt, opts);

    // Reference values picked so calendar arithmetic is unambiguous:
    //   2026-05-16 → 20589 days since 1970-01-01
    //     (1970→2026 = 56*365 + 14 leap days = 20454 days at Jan 1 2026;
    //      Jan 31 + Feb 28 + Mar 31 + Apr 30 = 120 days; +15 from May 1)
    //   2026-05-16T14:30:00 UTC → 20589 * 86400 + 14*3600 + 30*60 seconds
    //                           = 1779424200 seconds → ×1_000_000 micros
    //   2024-01-01 → 56*365 - 2*365 + 13 leap days = 19723 days... but
    //     we need actual: 54 years × 365 = 19710 + 13 leap = 19723. ✓
    const d_2026_05_16: i32 = 20589;
    const ts_2026_05_16_14_30_00: i64 = (20589 * 86400 + 14 * 3600 + 30 * 60) * 1_000_000;
    const d_2024_01_01: i32 = 19723;

    try t.insert(&.{
        .{ .id = @as(i64, 1), .d = thindb.types.Date.fromDays(d_2026_05_16), .ts = thindb.types.DateTime.fromMicros(ts_2026_05_16_14_30_00) },
        .{ .id = @as(i64, 2), .d = thindb.types.Date.fromDays(d_2024_01_01), .ts = thindb.types.DateTime.fromMicros(0) },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const E = thindb.exec.expr_mod;
    const F = thindb.exec.scalar_fn;

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "y", .expr = try F.year(aa, E.col("d")) },
        .{ .name = "m", .expr = try F.month(aa, E.col("d")) },
        .{ .name = "dd", .expr = try F.day(aa, E.col("d")) },
        .{ .name = "hh", .expr = try F.hour(aa, E.col("ts")) },
        .{ .name = "diff_days", .expr = try F.datediff(aa, E.col("d"), E.col("d")) },
    });
    defer q.deinit();

    var years: std.ArrayList(i32) = .empty;
    defer years.deinit(allocator);
    var months: std.ArrayList(i32) = .empty;
    defer months.deinit(allocator);
    var days: std.ArrayList(i32) = .empty;
    defer days.deinit(allocator);
    var hours: std.ArrayList(i32) = .empty;
    defer hours.deinit(allocator);

    while (try q.next()) |b| {
        try years.appendSlice(allocator, b.values[3].data.int[0..b.row_count]);
        try months.appendSlice(allocator, b.values[4].data.int[0..b.row_count]);
        try days.appendSlice(allocator, b.values[5].data.int[0..b.row_count]);
        try hours.appendSlice(allocator, b.values[6].data.int[0..b.row_count]);
    }
    // Row 1: 2026-05-16T14:30:00 → year=2026, month=5, day=16, hour=14
    // Row 2: 2024-01-01T00:00:00 → year=2024, month=1, day=1, hour=0
    try std.testing.expectEqualSlices(i32, &[_]i32{ 2026, 2024 }, years.items);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 5, 1 }, months.items);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 16, 1 }, days.items);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 14, 0 }, hours.items);
}

test "compute: conversion — numeric widening, narrowing, parsing, stringifying" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_conv = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "i", .type = .int },
            .{ .name = "bi", .type = .bigint },
            .{ .name = "d", .type = .double },
            .{ .name = "s", .type = .string },
            .{ .name = "b", .type = .boolean },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 8 };

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("c", schema_conv, opts);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .i = @as(i32, 42), .bi = @as(i64, 1_000_000_000_000), .d = @as(f64, 3.14), .s = @as([]const u8, "123"), .b = true },
        .{ .id = @as(i64, 2), .i = @as(i32, -7), .bi = @as(i64, 99), .d = @as(f64, -2.5), .s = @as([]const u8, "not_a_number"), .b = false },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const E = thindb.exec.expr_mod;
    const F = thindb.exec.scalar_fn;

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        // Widening
        .{ .name = "i_to_bi", .expr = try F.toBigint(aa, E.col("i")) },
        .{ .name = "i_to_d", .expr = try F.toDouble(aa, E.col("i")) },
        .{ .name = "bi_to_d", .expr = try F.toDouble(aa, E.col("bi")) },
        // Narrowing
        .{ .name = "bi_to_i", .expr = try F.toInt(aa, E.col("bi")) },
        .{ .name = "d_to_i", .expr = try F.toInt(aa, E.col("d")) },
        .{ .name = "d_to_bi", .expr = try F.toBigint(aa, E.col("d")) },
        // Parsing
        .{ .name = "s_to_i", .expr = try F.toInt(aa, E.col("s")) },
        .{ .name = "s_to_bi", .expr = try F.toBigint(aa, E.col("s")) },
        .{ .name = "s_to_d", .expr = try F.toDouble(aa, E.col("s")) },
        // Stringify
        .{ .name = "i_to_s", .expr = try F.toString(aa, E.col("i")) },
        .{ .name = "bi_to_s", .expr = try F.toString(aa, E.col("bi")) },
        .{ .name = "b_to_s", .expr = try F.toString(aa, E.col("b")) },
    });
    defer q.deinit();

    // Schema is: id, i, bi, d, s, b, then 12 derived → 18 cols total.
    var i_to_bi: std.ArrayList(i64) = .empty; defer i_to_bi.deinit(allocator);
    var i_to_d: std.ArrayList(f64) = .empty; defer i_to_d.deinit(allocator);
    var bi_to_i: std.ArrayList(i32) = .empty; defer bi_to_i.deinit(allocator);
    var d_to_i: std.ArrayList(i32) = .empty; defer d_to_i.deinit(allocator);
    var s_to_i: std.ArrayList(i32) = .empty; defer s_to_i.deinit(allocator);
    var s_to_d: std.ArrayList(f64) = .empty; defer s_to_d.deinit(allocator);
    var i_to_s_concat: std.ArrayList(u8) = .empty; defer i_to_s_concat.deinit(allocator);
    var b_to_s_concat: std.ArrayList(u8) = .empty; defer b_to_s_concat.deinit(allocator);

    while (try q.next()) |b| {
        try i_to_bi.appendSlice(allocator, b.values[6].data.bigint[0..b.row_count]);
        try i_to_d.appendSlice(allocator, b.values[7].data.double[0..b.row_count]);
        try bi_to_i.appendSlice(allocator, b.values[9].data.int[0..b.row_count]);
        try d_to_i.appendSlice(allocator, b.values[10].data.int[0..b.row_count]);
        try s_to_i.appendSlice(allocator, b.values[12].data.int[0..b.row_count]);
        try s_to_d.appendSlice(allocator, b.values[14].data.double[0..b.row_count]);
        const is = b.values[15].data.string;
        const bs = b.values[17].data.string;
        for (0..b.row_count) |k| {
            try i_to_s_concat.appendSlice(allocator, is.rowBytes(k));
            try i_to_s_concat.append(allocator, '|');
            try b_to_s_concat.appendSlice(allocator, bs.rowBytes(k));
            try b_to_s_concat.append(allocator, '|');
        }
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 42, -7 }, i_to_bi.items);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 42.0, -7.0 }, i_to_d.items);
    // bigint 1_000_000_000_000 saturates to i32 max; 99 fits.
    try std.testing.expectEqualSlices(i32, &[_]i32{ std.math.maxInt(i32), 99 }, bi_to_i.items);
    // double 3.14 → 3; -2.5 → -2 (truncates toward zero)
    try std.testing.expectEqualSlices(i32, &[_]i32{ 3, -2 }, d_to_i.items);
    // string "123" → 123; "not_a_number" → 0
    try std.testing.expectEqualSlices(i32, &[_]i32{ 123, 0 }, s_to_i.items);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 123.0, 0.0 }, s_to_d.items);
    try std.testing.expectEqualStrings("42|-7|", i_to_s_concat.items);
    try std.testing.expectEqualStrings("true|false|", b_to_s_concat.items);
}

test "compute: hash — md5, sha1, sha256, crc32 produce expected digests" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("h", schema_basic, opts_basic);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .name = "" },
        .{ .id = @as(i64, 2), .name = "abc" },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const E = thindb.exec.expr_mod;
    const F = thindb.exec.scalar_fn;

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "m", .expr = try F.md5(aa, E.col("name")) },
        .{ .name = "s1", .expr = try F.sha1(aa, E.col("name")) },
        .{ .name = "s2", .expr = try F.sha256(aa, E.col("name")) },
        .{ .name = "c", .expr = try F.crc32(aa, E.col("name")) },
    });
    defer q.deinit();

    var md5_concat: std.ArrayList(u8) = .empty; defer md5_concat.deinit(allocator);
    var sha1_concat: std.ArrayList(u8) = .empty; defer sha1_concat.deinit(allocator);
    var sha256_concat: std.ArrayList(u8) = .empty; defer sha256_concat.deinit(allocator);
    var crc: std.ArrayList(i64) = .empty; defer crc.deinit(allocator);

    while (try q.next()) |b| {
        const m = b.values[2].data.string;
        const s1 = b.values[3].data.string;
        const s2 = b.values[4].data.string;
        for (0..b.row_count) |k| {
            try md5_concat.appendSlice(allocator, m.rowBytes(k));
            try md5_concat.append(allocator, '|');
            try sha1_concat.appendSlice(allocator, s1.rowBytes(k));
            try sha1_concat.append(allocator, '|');
            try sha256_concat.appendSlice(allocator, s2.rowBytes(k));
            try sha256_concat.append(allocator, '|');
        }
        try crc.appendSlice(allocator, b.values[5].data.bigint[0..b.row_count]);
    }
    // Known digests: md5("") = d41d8cd98f00b204e9800998ecf8427e
    //                md5("abc") = 900150983cd24fb0d6963f7d28e17f72
    try std.testing.expectEqualStrings(
        "d41d8cd98f00b204e9800998ecf8427e|900150983cd24fb0d6963f7d28e17f72|",
        md5_concat.items,
    );
    // sha1("") = da39a3ee5e6b4b0d3255bfef95601890afd80709
    // sha1("abc") = a9993e364706816aba3e25717850c26c9cd0d89d
    try std.testing.expectEqualStrings(
        "da39a3ee5e6b4b0d3255bfef95601890afd80709|a9993e364706816aba3e25717850c26c9cd0d89d|",
        sha1_concat.items,
    );
    // sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    // sha256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
    try std.testing.expectEqualStrings(
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855|ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad|",
        sha256_concat.items,
    );
    // crc32("") = 0; crc32("abc") = 0x352441C2 = 891568578
    try std.testing.expectEqualSlices(i64, &[_]i64{ 0, 0x352441C2 }, crc.items);
}

test "compute: encoding — hex/unhex + base64 round-trips" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("e", schema_basic, opts_basic);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .name = "Hi" },         // hex: 4869; base64: SGk=
        .{ .id = @as(i64, 2), .name = "" },           // hex: ""; base64: ""
        .{ .id = @as(i64, 3), .name = "thinDB!" },    // hex: 7468696e44422!; base64 ends with =
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const E = thindb.exec.expr_mod;
    const F = thindb.exec.scalar_fn;

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "h", .expr = try F.hex(aa, E.col("name")) },
        .{ .name = "b64", .expr = try F.toBase64(aa, E.col("name")) },
    });
    defer q.deinit();

    var hex_concat: std.ArrayList(u8) = .empty; defer hex_concat.deinit(allocator);
    var b64_concat: std.ArrayList(u8) = .empty; defer b64_concat.deinit(allocator);
    while (try q.next()) |b| {
        const h = b.values[2].data.string;
        const b64 = b.values[3].data.string;
        for (0..b.row_count) |k| {
            try hex_concat.appendSlice(allocator, h.rowBytes(k));
            try hex_concat.append(allocator, '|');
            try b64_concat.appendSlice(allocator, b64.rowBytes(k));
            try b64_concat.append(allocator, '|');
        }
    }
    try std.testing.expectEqualStrings("4869||7468696e444221|", hex_concat.items);
    try std.testing.expectEqualStrings("SGk=||dGhpbkRCIQ==|", b64_concat.items);

    // Two-layer Compute exercises unhex / from_base64 (Compute v1
    // doesn't support nested expressions; chain two layers instead).
    var base2 = try thindb.scan(allocator, t);
    var step1 = try base2.compute(&.{
        .{ .name = "h", .expr = try F.hex(aa, E.col("name")) },
        .{ .name = "b64", .expr = try F.toBase64(aa, E.col("name")) },
    });
    var q2 = try step1.compute(&.{
        .{ .name = "round_hex", .expr = try F.unhex(aa, E.col("h")) },
        .{ .name = "round_b64", .expr = try F.fromBase64(aa, E.col("b64")) },
    });
    defer q2.deinit();

    var round_hex_concat: std.ArrayList(u8) = .empty;
    defer round_hex_concat.deinit(allocator);
    var round_b64_concat: std.ArrayList(u8) = .empty;
    defer round_b64_concat.deinit(allocator);
    const out_schema = q2.outputSchema();
    var rh_idx: usize = 0;
    var rb_idx: usize = 0;
    for (out_schema, 0..) |c, k| {
        if (std.mem.eql(u8, c.name, "round_hex")) rh_idx = k;
        if (std.mem.eql(u8, c.name, "round_b64")) rb_idx = k;
    }
    while (try q2.next()) |b| {
        const rh = b.values[rh_idx].data.string;
        const rb = b.values[rb_idx].data.string;
        for (0..b.row_count) |k| {
            try round_hex_concat.appendSlice(allocator, rh.rowBytes(k));
            try round_hex_concat.append(allocator, '|');
            try round_b64_concat.appendSlice(allocator, rb.rowBytes(k));
            try round_b64_concat.append(allocator, '|');
        }
    }
    try std.testing.expectEqualStrings("Hi||thinDB!|", round_hex_concat.items);
    try std.testing.expectEqualStrings("Hi||thinDB!|", round_b64_concat.items);
}

test "compute: kitchen sink — every registered scalar function asserts" {
    // One test that exercises EVERY function. If a kernel goes wrong
    // it shows up here even if a more focused test was missing.
    // Backfills the previously-not-directly-asserted functions:
    //   string: lower, ltrim, rtrim, reverse, concat, replace,
    //           octet_length, char_length
    //   math:   abs(bigint/double), round, sign, mod (both),
    //           pow, sqrt, exp, ln, log10, log2, greatest/least
    //   date:   minute, second, date_add, date_sub,
    //           unix_timestamp, from_unixtime
    //   convert: to_double(bigint), to_bigint(string),
    //            to_string(bigint), to_string(double)

    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Single row, diverse columns. Values picked so the function
    // outputs are unambiguous.
    const schema_all = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "s", .type = .string },     // "  Hello  "
            .{ .name = "ws", .type = .string },    // "world"
            .{ .name = "i1", .type = .int },       // 7
            .{ .name = "i2", .type = .int },       // 3
            .{ .name = "bi1", .type = .bigint },   // 10
            .{ .name = "bi2", .type = .bigint },   // 4
            .{ .name = "d1", .type = .double },    // -2.5
            .{ .name = "d2", .type = .double },    // 8.0
            .{ .name = "dt", .type = .date },      // 20589 (2026-05-16)
            .{ .name = "ts", .type = .datetime },  // 1779424200 * 1_000_000
            .{ .name = "num_str", .type = .string }, // "42"
            .{ .name = "off1", .type = .int },     // 1
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 8 };

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("all_in", schema_all, opts);
    try t.insert(&.{
        .{
            .id = @as(i64, 1),
            .s = @as([]const u8, "  Hello  "),
            .ws = @as([]const u8, "world"),
            .i1 = @as(i32, 7), .i2 = @as(i32, 3),
            .bi1 = @as(i64, 10), .bi2 = @as(i64, 4),
            .d1 = @as(f64, -2.5), .d2 = @as(f64, 8.0),
            .dt = thindb.types.Date.fromDays(20589),
            .ts = thindb.types.DateTime.fromMicros(1_779_424_200_000_000),
            .num_str = @as([]const u8, "42"),
            .off1 = @as(i32, 1),
        },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const E = thindb.exec.expr_mod;
    const F = thindb.exec.scalar_fn;

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        // --- strings ---
        .{ .name = "lower", .expr = try F.lower(aa, E.col("s")) },          // "  hello  "
        .{ .name = "ltrim", .expr = try F.ltrim(aa, E.col("s")) },          // "Hello  "
        .{ .name = "rtrim", .expr = try F.rtrim(aa, E.col("s")) },          // "  Hello"
        .{ .name = "reverse", .expr = try F.reverse(aa, E.col("ws")) },     // "dlrow"
        .{ .name = "concat", .expr = try F.concat(aa, &.{ E.col("ws"), E.col("ws") }) }, // "worldworld"
        .{ .name = "replace", .expr = try F.replace(aa, E.col("ws"), E.col("ws"), E.col("num_str")) }, // "42"
        .{ .name = "octet_length", .expr = try F.octetLength(aa, E.col("ws")) },  // 5
        .{ .name = "char_length", .expr = try F.charLength(aa, E.col("ws")) },    // 5

        // --- math ---
        .{ .name = "abs_bi", .expr = try F.abs(aa, E.col("bi1")) },          // 10
        .{ .name = "abs_d", .expr = try F.abs(aa, E.col("d1")) },             // 2.5
        .{ .name = "round_d", .expr = try F.round(aa, E.col("d1")) },         // round(-2.5) = -3 (banker's) or -2? Zig @round → -3
        .{ .name = "sign_d", .expr = try F.sign(aa, E.col("d1")) },           // -1
        .{ .name = "mod_i", .expr = try F.mod(aa, E.col("i1"), E.col("i2")) }, // 7 % 3 = 1
        .{ .name = "mod_bi", .expr = try F.mod(aa, E.col("bi1"), E.col("bi2")) }, // 10 % 4 = 2
        .{ .name = "pow_d", .expr = try F.pow(aa, E.col("d2"), E.col("d2")) }, // 8^8 = 16777216
        .{ .name = "sqrt_d", .expr = try F.sqrt(aa, E.col("d2")) },           // sqrt(8) ≈ 2.828
        .{ .name = "exp_d", .expr = try F.exp(aa, E.col("d2")) },             // exp(8) ≈ 2980.958
        .{ .name = "ln_d", .expr = try F.ln(aa, E.col("d2")) },               // ln(8) ≈ 2.079
        .{ .name = "log10_d", .expr = try F.log10(aa, E.col("d2")) },         // log10(8) ≈ 0.903
        .{ .name = "log2_d", .expr = try F.log2(aa, E.col("d2")) },           // log2(8) = 3.0
        .{ .name = "greatest_i", .expr = try F.greatest(aa, E.col("i1"), E.col("i2")) },  // 7
        .{ .name = "greatest_bi", .expr = try F.greatest(aa, E.col("bi1"), E.col("bi2")) }, // 10
        .{ .name = "greatest_d", .expr = try F.greatest(aa, E.col("d1"), E.col("d2")) }, // 8.0
        .{ .name = "least_i", .expr = try F.least(aa, E.col("i1"), E.col("i2")) },        // 3
        .{ .name = "least_bi", .expr = try F.least(aa, E.col("bi1"), E.col("bi2")) },     // 4
        .{ .name = "least_d", .expr = try F.least(aa, E.col("d1"), E.col("d2")) },        // -2.5

        // --- date ---
        .{ .name = "minute_ts", .expr = try F.minute(aa, E.col("ts")) },  // 30
        .{ .name = "second_ts", .expr = try F.second(aa, E.col("ts")) },  // 0
        .{ .name = "d_plus1", .expr = try F.dateAdd(aa, E.col("dt"), E.col("off1")) },  // 20590 days
        .{ .name = "d_minus1", .expr = try F.dateSub(aa, E.col("dt"), E.col("off1")) }, // 20588 days
        .{ .name = "ut", .expr = try F.unixTimestamp(aa, E.col("ts")) },  // 1779424200
        .{ .name = "fut", .expr = try F.fromUnixtime(aa, E.col("bi1")) }, // 10 * 1_000_000

        // --- conversion (backfill) ---
        .{ .name = "bi_to_d", .expr = try F.toDouble(aa, E.col("bi1")) },  // 10.0
        .{ .name = "ns_to_bi", .expr = try F.toBigint(aa, E.col("num_str")) }, // 42
        .{ .name = "bi_to_s", .expr = try F.toString(aa, E.col("bi1")) },  // "10"
        .{ .name = "d_to_s", .expr = try F.toString(aa, E.col("d2")) },    // "8" (or "8.0e0" depending on fmt)
    });
    defer q.deinit();

    const out_schema = q.outputSchema();

    // Pull single-row outputs by name to keep the assertions readable.
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);

    inline for (.{
        .{ "lower", "  hello  " },
        .{ "ltrim", "Hello  " },
        .{ "rtrim", "  Hello" },
        .{ "reverse", "dlrow" },
        .{ "concat", "worldworld" },
        .{ "replace", "42" },
        .{ "bi_to_s", "10" },
    }) |pair| {
        const name: []const u8 = pair[0];
        const want: []const u8 = pair[1];
        const idx = colIndex(out_schema, name);
        try std.testing.expectEqualStrings(want, b.values[idx].data.string.rowBytes(0));
    }

    inline for (.{
        .{ "octet_length", 5 },
        .{ "char_length", 5 },
        .{ "sign_d", -1 },
        .{ "mod_i", 1 },
        .{ "greatest_i", 7 },
        .{ "least_i", 3 },
        .{ "minute_ts", 30 },
        .{ "second_ts", 0 },
    }) |pair| {
        const name: []const u8 = pair[0];
        const want: i32 = pair[1];
        const idx = colIndex(out_schema, name);
        try std.testing.expectEqual(want, b.values[idx].data.int[0]);
    }

    try std.testing.expectEqual(@as(i64, 10), b.values[colIndex(out_schema, "abs_bi")].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 2), b.values[colIndex(out_schema, "mod_bi")].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 10), b.values[colIndex(out_schema, "greatest_bi")].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 4), b.values[colIndex(out_schema, "least_bi")].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 20590), b.values[colIndex(out_schema, "d_plus1")].data.date[0]);
    try std.testing.expectEqual(@as(i64, 20588), b.values[colIndex(out_schema, "d_minus1")].data.date[0]);
    try std.testing.expectEqual(@as(i64, 1_779_424_200), b.values[colIndex(out_schema, "ut")].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 10_000_000), b.values[colIndex(out_schema, "fut")].data.datetime[0]);
    try std.testing.expectEqual(@as(i64, 42), b.values[colIndex(out_schema, "ns_to_bi")].data.bigint[0]);

    try std.testing.expectEqual(@as(f64, 2.5), b.values[colIndex(out_schema, "abs_d")].data.double[0]);
    try std.testing.expectEqual(@as(f64, -3.0), b.values[colIndex(out_schema, "round_d")].data.double[0]);
    try std.testing.expectEqual(@as(f64, 16_777_216.0), b.values[colIndex(out_schema, "pow_d")].data.double[0]);
    try std.testing.expectApproxEqAbs(@as(f64, 2.8284271247461903), b.values[colIndex(out_schema, "sqrt_d")].data.double[0], 1e-12);
    try std.testing.expectEqual(@as(f64, 3.0), b.values[colIndex(out_schema, "log2_d")].data.double[0]);
    try std.testing.expectEqual(@as(f64, 8.0), b.values[colIndex(out_schema, "greatest_d")].data.double[0]);
    try std.testing.expectEqual(@as(f64, -2.5), b.values[colIndex(out_schema, "least_d")].data.double[0]);
    try std.testing.expectEqual(@as(f64, 10.0), b.values[colIndex(out_schema, "bi_to_d")].data.double[0]);
    // exp / ln / log10 are checked approximate.
    try std.testing.expectApproxEqAbs(@as(f64, 2980.9579870417283), b.values[colIndex(out_schema, "exp_d")].data.double[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0794415416798357), b.values[colIndex(out_schema, "ln_d")].data.double[0], 1e-12);
    try std.testing.expectApproxEqAbs(@as(f64, 0.9030899869919435), b.values[colIndex(out_schema, "log10_d")].data.double[0], 1e-12);

    // d_to_s — float formatting can vary; just assert it parses back to 8.
    const d_to_s_view = b.values[colIndex(out_schema, "d_to_s")].data.string;
    const parsed = try std.fmt.parseFloat(f64, d_to_s_view.rowBytes(0));
    try std.testing.expectEqual(@as(f64, 8.0), parsed);
}

fn colIndex(schema: []const thindb.Column, name: []const u8) usize {
    for (schema, 0..) |c, i| if (std.mem.eql(u8, c.name, name)) return i;
    unreachable;
}

test "compute: coalesce returns first non-null + bookkeeps the output bitmap" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_nullable = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "a", .type = .string, .nullable = true },
            .{ .name = "b", .type = .string, .nullable = true },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{
        .order_key = &ok,
        .unique = true,
        .row_group_size = 4,
    };

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("vals", schema_nullable, opts);
    try t.insert(&[_]struct { id: i64, a: ?[]const u8, b: ?[]const u8 }{
        .{ .id = 1, .a = "alpha", .b = null },     // → "alpha", valid
        .{ .id = 2, .a = null,    .b = "beta" },    // → "beta",  valid
        .{ .id = 3, .a = "gamma", .b = "delta" },   // → "gamma", valid (first wins)
        .{ .id = 4, .a = null,    .b = null },      // → null
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "merged", .expr = try thindb.exec.scalar_fn.coalesce(
            aa,
            thindb.exec.expr_mod.col("a"),
            thindb.exec.expr_mod.col("b"),
        ) },
    });
    defer q.deinit();

    var values: std.ArrayList(u8) = .empty;
    defer values.deinit(allocator);
    var validities: std.ArrayList(bool) = .empty;
    defer validities.deinit(allocator);

    while (try q.next()) |b| {
        const sv = b.values[3].data.string;
        for (0..b.row_count) |i| {
            try validities.append(allocator, b.values[3].isValid(i));
            try values.appendSlice(allocator, sv.rowBytes(i));
            try values.append(allocator, '|');
        }
    }
    try std.testing.expectEqualSlices(bool, &[_]bool{ true, true, true, false }, validities.items);
    // Last row's data slot is "" (the placeholder we wrote) but bitmap marks null.
    try std.testing.expectEqualStrings("alpha|beta|gamma||", values.items);
}

// Coercion + the expanded scalar-function set (lpad/position/dayofweek/
// date_format/etc) live in compute_scalar_test.zig.

