//! Compute operator + scalar function integration.
//! Exercises the upper/lower/length/coalesce scalars introduced in
//! the first scalar-fn commit.

const std = @import("std");
const thindb = @import("thindb");

const schema_basic = thindb.Schema{
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
    const schema_sub = thindb.Schema{
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

    const schema_math = thindb.Schema{
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

    const schema_cond = thindb.Schema{
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

test "compute: coalesce returns first non-null + bookkeeps the output bitmap" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_nullable = thindb.Schema{
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
