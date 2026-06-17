//! Compute-operator coverage for the implicit-coercion path and the
//! DuckDB/StarRocks parity scalar-function expansion (lpad, position,
//! substring_index, dayofweek/quarter/last_day, date_format, ascii).
//!
//! Split out of compute_test.zig once that file passed 1.3 kLOC. The
//! core operator tests + the original scalar set stay there; this file
//! covers everything added post-coercion-commit.

const std = @import("std");
const thindb = @import("thindb");

// ---------------------------------------------------------------------------
// Implicit type coercion (DuckDB-style promotion graph in src/exec/cast.zig)
// ---------------------------------------------------------------------------

const schema_mixed = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "small", .type = .int },
        .{ .name = "big", .type = .bigint },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const ok_mixed = [_][]const u8{"id"};
const opts_mixed = thindb.TableOptions{
    .order_key = &ok_mixed,
    .unique = true,
    .row_group_size = 4,
};

test "coercion: sqrt(bigint_col) routes via implicit bigint→double" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema_mixed, opts_mixed);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .small = @as(i32, 4), .big = @as(i64, 9) },
        .{ .id = @as(i64, 2), .small = @as(i32, 16), .big = @as(i64, 25) },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        // sqrt is registered only as (double). big is bigint → must
        // coerce. Output column type = double.
        .{ .name = "root", .expr = try thindb.exec.scalar_fn.sqrt(aa, thindb.exec.expr_mod.col("big")) },
    });
    defer q.deinit();

    const schema = q.outputSchema();
    try std.testing.expectEqual(@as(thindb.types.TypeTag, .double), @as(thindb.types.TypeTag, schema[3].type));

    var roots: std.ArrayList(f64) = .empty;
    defer roots.deinit(allocator);
    while (try q.next()) |b| {
        for (0..b.row_count) |i| try roots.append(allocator, b.values[3].data.double[i]);
    }
    try std.testing.expectEqual(@as(usize, 2), roots.items.len);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), roots.items[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 5.0), roots.items[1], 0.0001);
}

test "coercion: mod(int_col, bigint_col) picks bigint overload + casts int" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema_mixed, opts_mixed);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .small = @as(i32, 17), .big = @as(i64, 5) },
        .{ .id = @as(i64, 2), .small = @as(i32, 23), .big = @as(i64, 7) },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "rem", .expr = try thindb.exec.scalar_fn.mod(
            aa,
            thindb.exec.expr_mod.col("small"),
            thindb.exec.expr_mod.col("big"),
        ) },
    });
    defer q.deinit();

    // (int, bigint) → resolved to (bigint, bigint); return type bigint.
    const schema = q.outputSchema();
    try std.testing.expectEqual(@as(thindb.types.TypeTag, .bigint), @as(thindb.types.TypeTag, schema[3].type));

    var rems: std.ArrayList(i64) = .empty;
    defer rems.deinit(allocator);
    while (try q.next()) |b| {
        for (0..b.row_count) |i| try rems.append(allocator, b.values[3].data.bigint[i]);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 2 }, rems.items);
}

test "coercion: no implicit string ↔ number — concat(string, int) still errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema_mixed, opts_mixed);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .small = @as(i32, 4), .big = @as(i64, 9) },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var base = try thindb.scan(allocator, t);
    // Two-arg concat exists only as (string, string). small is int.
    // Per DuckDB/StarRocks convention, string ↔ number requires an
    // explicit cast — verify we don't silently auto-stringify.
    const result = base.compute(&.{
        .{ .name = "joined", .expr = try thindb.exec.scalar_fn.concat(aa, &.{
            thindb.exec.expr_mod.col("small"),
            thindb.exec.expr_mod.col("small"),
        }) },
    });
    try std.testing.expectError(thindb.exec.Error.ComputeNoSuchOverload, result);
    base.deinit();
}

// ---------------------------------------------------------------------------
// Expanded scalar functions: per-row correctness through Compute.
// ---------------------------------------------------------------------------

const schema_str = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "s", .type = .string },
        .{ .name = "n", .type = .int },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const ok_str = [_][]const u8{"id"};
const opts_str = thindb.TableOptions{ .order_key = &ok_str, .unique = true, .row_group_size = 8 };

test "scalar: lpad pads + truncates" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema_str, opts_str);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .s = "abc", .n = @as(i32, 1) }, // truncate → "a"
        .{ .id = @as(i64, 2), .s = "hi", .n = @as(i32, 5) }, // pad → "hihhi" (pad = s repeated)
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "lp", .expr = try thindb.exec.scalar_fn.lpad(
            aa,
            thindb.exec.expr_mod.col("s"),
            thindb.exec.expr_mod.col("n"),
            thindb.exec.expr_mod.col("s"),
        ) },
    });
    defer q.deinit();
    const b = (try q.next()).?;
    // (id, s, n) + derived lp → index 3
    const sv = b.values[3].data.string;
    try std.testing.expectEqualStrings("a", sv.rowBytes(0));
    // pad "hi" with "hi" repeating → first 3 pad chars + "hi" → "hihhi"
    try std.testing.expectEqualStrings("hihhi", sv.rowBytes(1));
}

test "scalar: position / instr with present + absent needles" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "needle", .type = .string },
            .{ .name = "hay", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("t", schema, .{ .order_key = &ok, .unique = true, .row_group_size = 8 });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .needle = "ll", .hay = "hello" }, // → 3
        .{ .id = @as(i64, 2), .needle = "xyz", .hay = "hello" }, // → 0
        .{ .id = @as(i64, 3), .needle = "", .hay = "anything" }, // → 1 (empty needle convention)
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "p", .expr = try thindb.exec.scalar_fn.position(
            aa,
            thindb.exec.expr_mod.col("needle"),
            thindb.exec.expr_mod.col("hay"),
        ) },
    });
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqualSlices(i32, &[_]i32{ 3, 0, 1 }, b.values[3].data.int[0..3]);
}

test "scalar: substring_index smoke through Compute (delim = column)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "s", .type = .string },
            .{ .name = "n", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("t", schema, .{ .order_key = &ok, .unique = true, .row_group_size = 8 });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .s = "a.b.c.d", .n = @as(i32, 2) },
        .{ .id = @as(i64, 2), .s = "a.b.c.d", .n = @as(i32, -2) },
        .{ .id = @as(i64, 3), .s = "a.b.c.d", .n = @as(i32, 0) },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    // Compute v1 has no string literal exprs, so delim defaults to the
    // s column itself — degenerate but the kernel still executes; we
    // verify rows materialize without crash.
    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "r", .expr = try thindb.exec.scalar_fn.substringIndex(
            aa,
            thindb.exec.expr_mod.col("s"),
            thindb.exec.expr_mod.col("s"),
            thindb.exec.expr_mod.col("n"),
        ) },
    });
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), b.row_count);
}

test "scalar: dayofweek / quarter / last_day on known dates" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "d", .type = .date },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("t", schema, .{ .order_key = &ok, .unique = true, .row_group_size = 8 });
    // 1970-01-01 was Thursday → MySQL dayofweek = 5; quarter = 1; last_day = Jan 31 = day 30
    // 2024-02-15 was Thursday → dow = 5; quarter = 1; last_day = 2024-02-29 (leap year)
    const days_1970_01_01: i32 = 0;
    const days_2024_02_15: i32 = 19_768;
    try t.insert(&.{
        .{ .id = @as(i64, 1), .d = days_1970_01_01 },
        .{ .id = @as(i64, 2), .d = days_2024_02_15 },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "dow", .expr = try thindb.exec.scalar_fn.dayofweek(aa, thindb.exec.expr_mod.col("d")) },
        .{ .name = "qtr", .expr = try thindb.exec.scalar_fn.quarter(aa, thindb.exec.expr_mod.col("d")) },
        .{ .name = "ld", .expr = try thindb.exec.scalar_fn.lastDay(aa, thindb.exec.expr_mod.col("d")) },
    });
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i32, 5), b.values[2].data.int[0]); // Thu
    try std.testing.expectEqual(@as(i32, 5), b.values[2].data.int[1]);
    try std.testing.expectEqual(@as(i32, 1), b.values[3].data.int[0]); // Q1
    try std.testing.expectEqual(@as(i32, 1), b.values[3].data.int[1]);
    try std.testing.expectEqual(@as(i32, 30), b.values[4].data.date[0]); // 1970-01-31
    try std.testing.expectEqual(@as(i32, 19_782), b.values[4].data.date[1]); // 2024-02-29
}

test "scalar: date_format with %Y-%m-%d %H:%i:%s on datetime + date" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "ts", .type = .datetime },
            .{ .name = "fmt", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("t", schema, .{ .order_key = &ok, .unique = true, .row_group_size = 8 });
    // 2024-02-15 13:45:30 UTC → micros since epoch.
    const day_micros: i64 = 19_768 * std.time.us_per_day;
    const tod_secs: i64 = 13 * 3600 + 45 * 60 + 30;
    const ts1 = day_micros + tod_secs * 1_000_000;
    try t.insert(&.{
        .{ .id = @as(i64, 1), .ts = ts1, .fmt = "%Y-%m-%d %H:%i:%s" },
        .{ .id = @as(i64, 2), .ts = ts1, .fmt = "%y/%m/%d" },
        .{ .id = @as(i64, 3), .ts = ts1, .fmt = "literal %% percent" },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "out", .expr = try thindb.exec.scalar_fn.dateFormat(
            aa,
            thindb.exec.expr_mod.col("ts"),
            thindb.exec.expr_mod.col("fmt"),
        ) },
    });
    defer q.deinit();
    const b = (try q.next()).?;
    // (id, ts, fmt) + derived out → index 3
    const sv = b.values[3].data.string;
    try std.testing.expectEqualStrings("2024-02-15 13:45:30", sv.rowBytes(0));
    try std.testing.expectEqualStrings("24/02/15", sv.rowBytes(1));
    try std.testing.expectEqualStrings("literal % percent", sv.rowBytes(2));
}

test "scalar: ascii on first byte" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema_str, opts_str);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .s = "A", .n = @as(i32, 0) },
        .{ .id = @as(i64, 2), .s = "Z", .n = @as(i32, 0) },
        .{ .id = @as(i64, 3), .s = "", .n = @as(i32, 0) },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "a", .expr = try thindb.exec.scalar_fn.ascii(aa, thindb.exec.expr_mod.col("s")) },
    });
    defer q.deinit();
    const b = (try q.next()).?;
    // (id, s, n) + derived a → index 3
    try std.testing.expectEqualSlices(i32, &[_]i32{ 65, 90, 0 }, b.values[3].data.int[0..3]);
}

test "scalar: expanded missing-function kernels through Compute" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "s", .type = .string },
            .{ .name = "needle", .type = .string },
            .{ .name = "setv", .type = .string },
            .{ .name = "n", .type = .int },
            .{ .name = "x", .type = .double },
            .{ .name = "flag", .type = .boolean },
            .{ .name = "d", .type = .date },
            .{ .name = "ts", .type = .datetime },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("t", schema, .{ .order_key = &ok, .unique = true, .row_group_size = 8 });
    try t.insert(&.{
        .{ .id = @as(i64, 1), .s = "hello", .needle = "he", .setv = "aa,hello,zz", .n = @as(i32, 2), .x = @as(f64, 0.0), .flag = true, .d = @as(i32, 0), .ts = @as(i64, 0) },
        .{ .id = @as(i64, 2), .s = "world", .needle = "or", .setv = "world,aa", .n = @as(i32, 3), .x = @as(f64, 8.0), .flag = false, .d = @as(i32, 31), .ts = @as(i64, 31 * std.time.us_per_day) },
    });
    try t.flush();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const F = thindb.exec.scalar_fn;
    const E = thindb.exec.expr_mod;
    const lit_dash = E.lit(.{ .text = "-" });
    const lit_lo = E.lit(.{ .text = "lo" });
    const lit_vowels = E.lit(.{ .text = "[aeiou]+" });
    const lit_x = E.lit(.{ .text = "x" });
    const lit_day = E.lit(.{ .text = "day" });
    const lit_256 = E.lit(.{ .int = 256 });

    var base = try thindb.scan(allocator, t);
    var q = try base.compute(&.{
        .{ .name = "joined", .expr = try F.concatWs(aa, &.{ lit_dash, E.col("s"), E.col("needle") }) },
        .{ .name = "l", .expr = try F.left(aa, E.col("s"), E.col("n")) },
        .{ .name = "r", .expr = try F.right(aa, E.col("s"), E.col("n")) },
        .{ .name = "sw", .expr = try F.startsWith(aa, E.col("s"), E.col("needle")) },
        .{ .name = "ew", .expr = try F.endsWith(aa, E.col("s"), lit_lo) },
        .{ .name = "rx", .expr = try F.regexpLike(aa, E.col("s"), lit_vowels) },
        .{ .name = "rs", .expr = try F.regexpSubstr(aa, E.col("s"), lit_vowels) },
        .{ .name = "bits", .expr = try F.bitLength(aa, E.col("s")) },
        .{ .name = "ordv", .expr = try F.ord(aa, E.col("s")) },
        .{ .name = "fld", .expr = try F.field(aa, &.{ E.col("s"), lit_x, E.col("s") }) },
        .{ .name = "fis", .expr = try F.findInSet(aa, E.col("s"), E.col("setv")) },
        .{ .name = "cap", .expr = try F.initcap(aa, E.col("s")) },
        .{ .name = "tr", .expr = try F.translate(aa, E.col("s"), lit_lo, E.lit(.{ .text = "12" })) },
        .{ .name = "chosen", .expr = try F.ifThenElse(aa, E.col("flag"), E.col("s"), E.col("needle")) },
        .{ .name = "cbr", .expr = try F.cbrt(aa, E.col("x")) },
        .{ .name = "sq", .expr = try F.square(aa, E.col("x")) },
        .{ .name = "bc", .expr = try F.bitCount(aa, E.col("n")) },
        .{ .name = "dn", .expr = try F.dayname(aa, E.col("d")) },
        .{ .name = "mn", .expr = try F.monthname(aa, E.col("d")) },
        .{ .name = "added", .expr = try F.timestampAdd(aa, lit_day, E.col("n"), E.col("d")) },
        .{ .name = "dd", .expr = try F.dateDiffUnit(aa, lit_day, E.col("d"), try F.timestampAdd(aa, lit_day, E.col("n"), E.col("d"))) },
        .{ .name = "sha", .expr = try F.sha2(aa, E.col("s"), lit_256) },
        .{ .name = "md", .expr = try F.md5sum(aa, &.{ E.col("s"), E.col("needle") }) },
        .{ .name = "xx", .expr = try F.xxHash3_128(aa, E.col("s")) },
        .{ .name = "binv", .expr = try F.bin(aa, E.col("n")) },
        .{ .name = "convv", .expr = try F.conv(aa, E.lit(.{ .text = "ff" }), E.lit(.{ .int = 16 }), E.lit(.{ .int = 10 })) },
    });
    defer q.deinit();

    const b = (try q.next()).?;
    const base_cols = 9;
    try std.testing.expectEqualStrings("hello-he", b.values[base_cols + 0].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("he", b.values[base_cols + 1].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("rld", b.values[base_cols + 2].data.string.rowBytes(1));
    try std.testing.expectEqual(@as(u8, 1), b.values[base_cols + 3].data.boolean[0]);
    try std.testing.expectEqual(@as(u8, 0), b.values[base_cols + 3].data.boolean[1]);
    try std.testing.expectEqual(@as(u8, 1), b.values[base_cols + 4].data.boolean[0]);
    try std.testing.expectEqual(@as(u8, 1), b.values[base_cols + 5].data.boolean[0]);
    try std.testing.expectEqualStrings("e", b.values[base_cols + 6].data.string.rowBytes(0));
    try std.testing.expectEqualSlices(i32, &[_]i32{ 40, 40 }, b.values[base_cols + 7].data.int[0..2]);
    try std.testing.expectEqual(@as(i32, 'h'), b.values[base_cols + 8].data.int[0]);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 2, 2 }, b.values[base_cols + 9].data.int[0..2]);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 2, 1 }, b.values[base_cols + 10].data.int[0..2]);
    try std.testing.expectEqualStrings("Hello", b.values[base_cols + 11].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("he112", b.values[base_cols + 12].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("hello", b.values[base_cols + 13].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("or", b.values[base_cols + 13].data.string.rowBytes(1));
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), b.values[base_cols + 14].data.double[1], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 64.0), b.values[base_cols + 15].data.double[1], 1e-9);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2 }, b.values[base_cols + 16].data.int[0..2]);
    try std.testing.expectEqualStrings("Thursday", b.values[base_cols + 17].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("January", b.values[base_cols + 18].data.string.rowBytes(0));
    try std.testing.expectEqualSlices(i32, &[_]i32{ 2, 34 }, b.values[base_cols + 19].data.date[0..2]);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 2, 3 }, b.values[base_cols + 20].data.int[0..2]);
    try std.testing.expectEqual(@as(usize, 64), b.values[base_cols + 21].data.string.rowBytes(0).len);
    try std.testing.expectEqual(@as(usize, 32), b.values[base_cols + 22].data.string.rowBytes(0).len);
    try std.testing.expectEqual(@as(usize, 32), b.values[base_cols + 23].data.string.rowBytes(0).len);
    try std.testing.expectEqualStrings("10", b.values[base_cols + 24].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("255", b.values[base_cols + 25].data.string.rowBytes(0));
}
