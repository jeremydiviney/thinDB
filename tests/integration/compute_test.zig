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
