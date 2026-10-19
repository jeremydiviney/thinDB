//! Compute-operator coverage for the implicit-coercion path and the
//! DuckDB/StarRocks parity scalar-function expansion (lpad, position,
//! substring_index, dayofweek/quarter/last_day, date_format, ascii).
//!
//! Split out of compute_test.zig once that file passed 1.3 kLOC. The
//! core operator tests + the original scalar set stay there; this file
//! covers everything added post-coercion-commit.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

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

test "coercion: a string parameter takes a number as its text — concat(int, int)" {
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
    // Two-arg concat exists only as (string, string); as in StarRocks and
    // MySQL, an int argument passes as its text.
    var q = try base.compute(&.{
        .{ .name = "joined", .expr = try thindb.exec.scalar_fn.concat(aa, &.{
            thindb.exec.expr_mod.col("small"),
            thindb.exec.expr_mod.col("small"),
        }) },
    });
    defer q.deinit();
    const b = (try q.next()) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("44", b.values[3].data.string.rowBytes(0));
}

test "a statement whose Compute fails to build lets the database close" {
    // The literal buffers Compute built before its overload failed were
    // charged to the statement's tracked allocator and leaked. The statement's
    // accountant, and the gate lease Database.close waits on, outlived the
    // statement, and close hung (#63).
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    try helpers.exec(allocator, db, "CREATE TABLE raw (id BIGINT PRIMARY KEY, s VARCHAR(32))");
    try helpers.exec(allocator, db, "INSERT INTO raw (id, s) VALUES (1, 'a')");
    inline for (.{
        "SELECT sqrt('x', 'y') AS v FROM raw",
        "SELECT upper(s) AS u, concat('a', sqrt('x', 'y')) AS v FROM raw",
        "SELECT CASE WHEN id > 0 THEN 'y' ELSE sqrt('x', 'y') END AS v FROM raw",
    }) |sql| {
        try helpers.expectRunError(allocator, db, sql, thindb.exec.Error.ComputeNoSuchOverload);
    }
    // Checked before close, which would wait on a leaked lease forever.
    try std.testing.expectEqual(@as(usize, 0), db.config.statement_gate.?.allocator_owners);
    db.close();
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
        .{ .name = "dd", .expr = try F.dateDiffUnit(aa, lit_day, try F.timestampAdd(aa, lit_day, E.col("n"), E.col("d")), E.col("d")) },
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
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2 }, b.values[base_cols + 16].data.bigint[0..2]);
    try std.testing.expectEqualStrings("Thursday", b.values[base_cols + 17].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("January", b.values[base_cols + 18].data.string.rowBytes(0));
    const day = std.time.us_per_day;
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2 * day, 34 * day }, b.values[base_cols + 19].data.datetime[0..2]);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3 }, b.values[base_cols + 20].data.bigint[0..2]);
    try std.testing.expectEqual(@as(usize, 64), b.values[base_cols + 21].data.string.rowBytes(0).len);
    try std.testing.expectEqual(@as(usize, 32), b.values[base_cols + 22].data.string.rowBytes(0).len);
    try std.testing.expectEqual(@as(i128, -98478366302105124680504504609445627880), b.values[base_cols + 23].data.largeint[0]);
    try std.testing.expectEqualStrings("10", b.values[base_cols + 24].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("255", b.values[base_cols + 25].data.string.rowBytes(0));
}

// ---------------------------------------------------------------------------
// Math domain errors and overflow
// ---------------------------------------------------------------------------

test "a math function answers a domain error or overflow with NULL" {
    // MySQL and StarRocks return NULL for SQRT(-1), LN(0), ASIN(2), EXP(1000),
    // MOD(x, 0); thinDB returned NaN and ±inf, which then misbehaved as keys.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE m (id BIGINT PRIMARY KEY, x DOUBLE)");
    try helpers.exec(allocator, db, "INSERT INTO m (id, x) VALUES (1, -1.0), (2, 0.0), (3, 4.0), (4, 1000.0), (5, NULL)");

    const pi = std.math.pi;
    const cases = .{
        .{ "SQRT(x)", [_]?f64{ null, 0, 2, @sqrt(1000.0), null } },
        .{ "SQRT(id - 3)", [_]?f64{ null, null, 0, 1, @sqrt(2.0) } },
        .{ "LN(x)", [_]?f64{ null, null, @log(4.0), @log(1000.0), null } },
        .{ "LOG10(x)", [_]?f64{ null, null, @log10(4.0), 3, null } },
        .{ "LOG2(x)", [_]?f64{ null, null, 2, @log2(1000.0), null } },
        .{ "LOG(2, x)", [_]?f64{ null, null, 2, @log(1000.0) / @log(2.0), null } },
        .{ "LOG(1, x)", [_]?f64{ null, null, null, null, null } },
        .{ "EXP(x)", [_]?f64{ @exp(-1.0), 1, @exp(4.0), null, null } },
        .{ "POW(x, 0.5)", [_]?f64{ null, 0, 2, std.math.pow(f64, 1000, 0.5), null } },
        .{ "POWER(x, -1)", [_]?f64{ -1, null, 0.25, 0.001, null } },
        .{ "ASIN(x)", [_]?f64{ -pi / 2.0, 0, null, null, null } },
        .{ "ACOS(x)", [_]?f64{ pi, pi / 2.0, null, null, null } },
        .{ "COT(x)", [_]?f64{ 1.0 / @tan(-1.0), null, 1.0 / @tan(4.0), 1.0 / @tan(1000.0), null } },
        .{ "MOD(x, 3)", [_]?f64{ -1, 0, 1, 1, null } },
        .{ "x % 0", [_]?f64{ null, null, null, null, null } },
        .{ "FMOD(x, 0)", [_]?f64{ null, null, null, null, null } },
    };
    inline for (cases) |c| {
        var q = try helpers.runSql(allocator, db, "SELECT " ++ c[0] ++ " FROM m ORDER BY id");
        defer q.deinit();
        var row: usize = 0;
        while (try q.next()) |b| {
            for (0..b.row_count) |r| {
                const got: ?f64 = if (b.values[0].isValid(r)) b.values[0].data.double[r] else null;
                // Comptime folds the expected transcendentals, which can differ
                // from the runtime result in the last bit.
                const same = if (c[1][row]) |want| got != null and std.math.approxEqRel(f64, want, got.?, 1e-12) else got == null;
                if (!same) {
                    std.debug.print("{s} row {d}: want {?d} got {?d}\n", .{ c[0], row, c[1][row], got });
                    return error.TestUnexpectedResult;
                }
                row += 1;
            }
        }
        try std.testing.expectEqual(c[1].len, row);
    }
}

// ---------------------------------------------------------------------------
// Division by zero
// ---------------------------------------------------------------------------

fn numericCell(t: thindb.types.Type, v: thindb.storage.ColumnView, row: usize) !?f64 {
    if (!v.isValid(row)) return null;
    return switch (v.data) {
        .double => |s| s[row],
        .bigint => |s| @floatFromInt(s[row]),
        .int => |s| @floatFromInt(s[row]),
        .decimal64 => |s| @as(f64, @floatFromInt(s[row])) / std.math.pow(f64, 10, @floatFromInt(t.decimalSpec().?.s)),
        .decimal128 => |s| @as(f64, @floatFromInt(s[row])) / std.math.pow(f64, 10, @floatFromInt(t.decimalSpec().?.s)),
        else => error.TestUnexpectedResult,
    };
}

fn expectNumericColumn(allocator: std.mem.Allocator, db: anytype, sql: []const u8, want: []const ?f64) !void {
    var q = try helpers.runSql(allocator, db, sql);
    defer q.deinit();
    const t = q.outputSchema()[0].type;
    var row: usize = 0;
    while (try q.next()) |b| {
        for (0..b.row_count) |r| {
            const got = try numericCell(t, b.values[0], r);
            const same = if (want[row]) |w| got != null and std.math.approxEqRel(f64, w, got.?, 1e-12) else got == null;
            if (!same) {
                std.debug.print("{s} row {d}: want {?d} got {?d}\n", .{ sql, row, want[row], got });
                return error.TestUnexpectedResult;
            }
            row += 1;
        }
    }
    try std.testing.expectEqual(want.len, row);
}

test "division by zero is NULL for every numeric type and operator" {
    // MySQL and StarRocks answer `/`, DIV, %, MOD and PMOD by zero with NULL.
    // thinDB returned ±inf or NaN for `/`, and 0 for a decimal divisor, which
    // then leaked into SUM/AVG and passed range filters.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE dz (id BIGINT PRIMARY KEY, x DOUBLE, y DOUBLE, n BIGINT, k INT, d DECIMAL(10,2), e DECIMAL(10,2))");
    try helpers.exec(allocator, db,
        \\INSERT INTO dz (id, x, y, n, k, d, e) VALUES
        \\(1, 6.0, 2.0, 6, 2, 6.00, 2.00),
        \\(2, 6.0, 0.0, 6, 0, 6.00, 0.00),
        \\(3, 0.0, 0.0, 0, 0, 0.00, 0.00),
        \\(4, -6.0, -0.0, -6, 0, -6.00, 0.00),
        \\(5, NULL, 2.0, NULL, 2, NULL, 2.00),
        \\(6, 6.0, NULL, 6, NULL, 6.00, NULL)
    );

    const all_null = [_]?f64{ null, null, null, null, null, null };
    const cases = .{
        .{ "x / y", [_]?f64{ 3, null, null, null, null, null } },
        .{ "n / k", [_]?f64{ 3, null, null, null, null, null } },
        .{ "d / e", [_]?f64{ 3, null, null, null, null, null } },
        .{ "d / y", [_]?f64{ 3, null, null, null, null, null } },
        .{ "x / e", [_]?f64{ 3, null, null, null, null, null } },
        .{ "n DIV k", [_]?f64{ 3, null, null, null, null, null } },
        .{ "n % k", [_]?f64{ 0, null, null, null, null, null } },
        .{ "MOD(n, k)", [_]?f64{ 0, null, null, null, null, null } },
        .{ "PMOD(n, k)", [_]?f64{ 0, null, null, null, null, null } },
        .{ "d % e", [_]?f64{ 0, null, null, null, null, null } },
        .{ "x % y", [_]?f64{ 0, null, null, null, null, null } },
        .{ "(x + 1) / (y - 2)", [_]?f64{ null, -3.5, -0.5, 2.5, null, null } },
        .{ "x / 0", all_null },
        .{ "x / 0.0", all_null },
        .{ "n / 0", all_null },
        .{ "d / 0.00", all_null },
        .{ "0.0 / 0", all_null },
        .{ "id / (id - id)", all_null },
        .{ "id / 2", [_]?f64{ 0.5, 1, 1.5, 2, 2.5, 3 } },
    };
    inline for (cases) |c| {
        const want = c[1];
        try expectNumericColumn(allocator, db, "SELECT " ++ c[0] ++ " FROM dz ORDER BY id", &want);
    }

    // Aggregates skip the NULLs, and no inf or NaN reaches a filter.
    try expectNumericColumn(allocator, db, "SELECT SUM(x / y) FROM dz", &.{3});
    try expectNumericColumn(allocator, db, "SELECT COUNT(n / k) FROM dz", &.{1});
    try expectNumericColumn(allocator, db, "SELECT AVG(d / e) FROM dz", &.{3});
    try expectNumericColumn(allocator, db, "SELECT id FROM dz WHERE x / y > 0 OR x / y < 0 ORDER BY id", &.{1});
    try expectNumericColumn(allocator, db, "SELECT id FROM dz WHERE n / k IS NULL ORDER BY id", &.{ 2, 3, 4, 5, 6 });

    // Only a divisor that can be zero makes the result nullable.
    const nullability = .{
        .{ "id / 2", false },
        .{ "id % 10", false },
        .{ "id DIV 3", false },
        .{ "id / 2.5", false },
        .{ "id / 0", true },
        .{ "id / k", true },
        .{ "id / id", true },
    };
    inline for (nullability) |c| {
        var q = try helpers.runSql(allocator, db, "SELECT " ++ c[0] ++ " FROM dz");
        defer q.deinit();
        try std.testing.expectEqual(c[1], q.outputSchema()[0].nullable);
    }
}

// ---------------------------------------------------------------------------
// Conditionals raise only for the rows that reach a failing branch
// ---------------------------------------------------------------------------

fn expectQueryError(allocator: std.mem.Allocator, db: anytype, sql: []const u8, expected: anyerror) !void {
    var q = helpers.runSql(allocator, db, sql) catch |err| return std.testing.expectEqual(expected, err);
    defer q.deinit();
    while (q.next()) |batch| {
        if (batch == null) return error.TestUnexpectedSuccess;
    } else |err| return std.testing.expectEqual(expected, err);
}

fn expectTextColumn(allocator: std.mem.Allocator, db: anytype, sql: []const u8, want: []const ?[]const u8) !void {
    var q = try helpers.runSql(allocator, db, sql);
    defer q.deinit();
    var row: usize = 0;
    while (try q.next()) |b| {
        for (0..b.row_count) |r| {
            const got: ?[]const u8 = if (b.values[0].isValid(r)) b.values[0].data.string.rowBytes(r) else null;
            if (want[row]) |w| try std.testing.expectEqualStrings(w, got orelse "NULL") else try std.testing.expectEqual(@as(?[]const u8, null), got);
            row += 1;
        }
    }
    try std.testing.expectEqual(want.len, row);
}

test "CASE, IF and COALESCE raise only where a row takes the failing branch" {
    // MySQL, StarRocks and DuckDB evaluate a branch only for the rows that
    // take it; thinDB evaluated every branch over the whole batch, so a
    // decimal overflow on a row the CASE sent elsewhere failed the query.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    // Odd ids hold a d whose cube overflows DECIMAL(38,0); even ids a small d.
    // Row 9 is the one big-d row without an n for COALESCE to take.
    try helpers.exec(allocator, db, "CREATE TABLE g (id BIGINT PRIMARY KEY, d DECIMAL(18,0) NOT NULL, n DECIMAL(10,2), small BOOLEAN NOT NULL)");
    try helpers.exec(allocator, db,
        \\INSERT INTO g (id, d, n, small) VALUES
        \\(1, 999999999999999999, 2.00, FALSE), (2, 2, NULL, TRUE),
        \\(3, 999999999999999999, 2.00, FALSE), (4, 4, NULL, TRUE),
        \\(5, 999999999999999999, 2.00, FALSE), (6, 6, NULL, TRUE),
        \\(7, 999999999999999999, 2.00, FALSE), (8, 8, NULL, TRUE),
        \\(9, 999999999999999999, NULL, FALSE)
    );

    const cubes = [_]?f64{ 0, 8, 0, 64, 0, 216, 0, 512 };
    const cases = .{
        .{ "CASE WHEN d < 100 THEN d * d * d ELSE 0 END", cubes },
        .{ "IF(d < 100, d * d * d, 0)", cubes },
        .{ "COALESCE(n, d * d * d)", [_]?f64{ 2, 8, 2, 64, 2, 216, 2, 512 } },
        .{ "IFNULL(n, d * d * d)", [_]?f64{ 2, 8, 2, 64, 2, 216, 2, 512 } },
        .{ "CASE WHEN d < 100 THEN CAST(d AS DECIMAL(5,2)) END", [_]?f64{ null, 2, null, 4, null, 6, null, 8 } },
        // A branch no row takes.
        .{ "CASE WHEN d < 0 THEN d * d * d ELSE 1 END", [_]?f64{ 1, 1, 1, 1, 1, 1, 1, 1 } },
    };
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("g", .{})).flush();
        inline for (cases) |c| {
            const want = c[1];
            try expectNumericColumn(allocator, db, "SELECT " ++ c[0] ++ " FROM g WHERE id <= 8 ORDER BY id", &want);
        }
        // The ELSE's COALESCE fails on row 9, which the WHEN takes.
        try expectNumericColumn(
            allocator,
            db,
            "SELECT CASE WHEN d > 100 THEN 0 ELSE COALESCE(n, d * d * d) END FROM g ORDER BY id",
            &.{ 0, 8, 0, 64, 0, 216, 0, 512, 0 },
        );
        try expectTextColumn(
            allocator,
            db,
            "SELECT CASE WHEN d < 100 THEN CAST(d * d * d AS CHAR) ELSE 'big' END FROM g WHERE id <= 4 ORDER BY id",
            &.{ "big", "8", "big", "64" },
        );

        // A row that takes the failing branch still fails.
        try expectQueryError(allocator, db, "SELECT CASE WHEN d > 0 THEN d * d * d ELSE 0 END FROM g", error.ArithmeticOverflow);
        try expectQueryError(allocator, db, "SELECT COALESCE(n, d * d * d) FROM g", error.ArithmeticOverflow);
    }

    // The `if` function, which SQL's IF lowers past, reads its branches the
    // same way.
    var base = try thindb.scan(allocator, try db.openTable("g", .{}));
    var q = try base.compute(&.{.{ .name = "v", .expr = .{ .call = .{ .fn_name = "if", .args = &.{
        .{ .col_ref = "small" },
        .{ .call = .{ .fn_name = "mul", .args = &.{
            .{ .call = .{ .fn_name = "mul", .args = &.{ .{ .col_ref = "d" }, .{ .col_ref = "d" } } } },
            .{ .col_ref = "d" },
        } } },
        .{ .lit = .{ .int = 0 } },
    } } } }});
    defer q.deinit();
    const v_type = q.outputSchema()[4].type;
    var got: std.ArrayList(?f64) = .empty;
    defer got.deinit(allocator);
    while (try q.next()) |b| {
        for (0..b.row_count) |r| try got.append(allocator, try numericCell(v_type, b.values[4], r));
    }
    try std.testing.expectEqualSlices(?f64, &.{ 0, 8, 0, 64, 0, 216, 0, 512, 0 }, got.items);
}

test "a WHEN's computed operand is evaluated only on the rows still open at that WHEN" {
    // A condition compares columns, so a computed operand such as d * d * d
    // was anchored below the CASE and computed for every row, including rows
    // an earlier WHEN had taken. MySQL, StarRocks and DuckDB compute it only
    // for the rows that reach its WHEN (#193).
    const allocator = std.testing.allocator;
    inline for (.{ 1, 4 }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .max_dop = dop, .row_group_size = 2 });
        defer db.close();
        // Odd ids hold a d whose cube overflows DECIMAL(38,0); an earlier
        // WHEN takes them wherever the cube is read.
        try helpers.exec(allocator, db, "CREATE TABLE g (id BIGINT PRIMARY KEY, d DECIMAL(18,0) NOT NULL, n DECIMAL(10,2), k INT NOT NULL)");
        try helpers.exec(allocator, db,
            \\INSERT INTO g (id, d, n, k) VALUES
            \\(1, 999999999999999999, 2.00, 1), (2, 2, NULL, 2),
            \\(3, 999999999999999999, 3.00, 1), (4, 4, NULL, 2)
        );

        const per_row = .{
            .{ "CASE WHEN d > 100 THEN 1 WHEN d * d * d > 0 THEN 2 ELSE 3 END", .{ 1, 2, 1, 2 } },
            // Simple CASE: a WHEN value is computed only for the rows it
            // is compared on.
            .{ "CASE d WHEN 999999999999999999 THEN 1 WHEN d * d * d - 6 THEN 2 ELSE 3 END", .{ 1, 2, 1, 3 } },
            .{ "CASE k WHEN 1 THEN 1 WHEN (d * d * d > 10) + 1 THEN 2 ELSE 3 END", .{ 1, 3, 1, 2 } },
            // Nested: a CASE inside a later WHEN, and in a THEN or ELSE.
            .{ "CASE WHEN d > 100 THEN 1 WHEN (CASE WHEN d * d * d > 10 THEN 1 ELSE 0 END) = 1 THEN 2 ELSE 3 END", .{ 1, 3, 1, 2 } },
            .{ "CASE WHEN d > 100 THEN 1 ELSE CASE WHEN d * d * d > 10 THEN 2 ELSE 3 END END", .{ 1, 3, 1, 2 } },
            .{ "CASE WHEN d < 100 THEN CASE WHEN k = 2 THEN CASE WHEN d * d * d > 10 THEN 2 ELSE 3 END END ELSE 1 END", .{ 1, 3, 1, 2 } },
            .{ "IF(d > 100, 1, IF(d * d * d > 10, 2, 3))", .{ 1, 3, 1, 2 } },
            .{ "COALESCE(n, CASE WHEN d * d * d > 10 THEN 20 ELSE 30 END)", .{ 2, 30, 3, 20 } },
            .{ "IFNULL(n, IF(d * d * d > 10, 20, 30))", .{ 2, 30, 3, 20 } },
            .{ "CASE WHEN d > 100 THEN 1 ELSE (d * d * d > 10) END", .{ 1, 0, 1, 1 } },
            .{ "CASE WHEN d > 100 THEN 1 WHEN ISNULL(d * d * d) THEN 2 ELSE 3 END", .{ 1, 3, 1, 3 } },
            // One operand read by two later WHENs, and inside NOT / OR.
            .{ "CASE WHEN d > 100 THEN 1 WHEN d * d * d IN (4, 8) THEN 2 WHEN d * d * d BETWEEN 10 AND 100 THEN 3 END", .{ 1, 2, 1, 3 } },
            .{ "CASE WHEN d > 100 THEN 1 WHEN NOT (d * d * d > 10) OR d * d * d = 64 THEN 2 ELSE 3 END", .{ 1, 2, 1, 2 } },
            .{ "CASE WHEN d > 100 THEN 1 WHEN CAST(d AS DECIMAL(5,2)) > 3 THEN 2 ELSE 3 END", .{ 1, 3, 1, 2 } },
            // Two copies of one CASE agree.
            .{ "CASE WHEN d > 100 THEN 1 WHEN d * d * d > 10 THEN 2 ELSE 3 END * 10 + CASE WHEN d > 100 THEN 1 WHEN d * d * d > 10 THEN 2 ELSE 3 END", .{ 11, 33, 11, 22 } },
        };
        const aggregated = .{
            .{ "SELECT SUM(CASE WHEN d > 100 THEN 1 WHEN d * d * d > 10 THEN 10 ELSE 100 END) FROM g", &[_]?f64{112} },
            .{ "SELECT SUM(CASE WHEN d > 100 THEN 1 WHEN d * d * d > 10 THEN 10 ELSE 100 END) FROM g GROUP BY k ORDER BY k", &[_]?f64{ 2, 110 } },
            .{ "SELECT COUNT(CASE WHEN d > 100 THEN NULL WHEN d * d * d > 10 THEN 1 END) FROM g GROUP BY k ORDER BY k", &[_]?f64{ 0, 1 } },
            // Over aggregates: the cube of k = 1's sum overflows.
            .{ "SELECT CASE WHEN MAX(d) > 100 THEN 1 WHEN SUM(d) * SUM(d) * SUM(d) > 5 THEN 2 ELSE 3 END FROM g GROUP BY k ORDER BY k", &[_]?f64{ 1, 2 } },
        };
        const ids = .{
            .{ "SELECT id FROM g WHERE CASE WHEN d > 100 THEN 0 WHEN d * d * d > 10 THEN 1 ELSE 0 END = 1 ORDER BY id", &[_]i64{4} },
            .{ "SELECT id FROM g ORDER BY CASE WHEN d > 100 THEN 2 WHEN d * d * d > 10 THEN 0 ELSE 1 END, id", &[_]i64{ 4, 2, 1, 3 } },
            .{ "SELECT k FROM g GROUP BY k HAVING CASE WHEN MAX(d) > 100 THEN 0 WHEN SUM(d) * SUM(d) * SUM(d) > 5 THEN 1 ELSE 0 END = 1", &[_]i64{2} },
        };
        for (0..2) |pass| {
            if (pass == 1) try (try db.openTable("g", .{})).flush();
            inline for (per_row) |c| {
                const want: [4]?f64 = c[1];
                try expectNumericColumn(allocator, db, "SELECT " ++ c[0] ++ " FROM g ORDER BY id", &want);
            }
            inline for (aggregated) |c| try expectNumericColumn(allocator, db, c[0], c[1]);
            inline for (ids) |c| {
                const got = try helpers.collectBigints(allocator, db, c[0]);
                defer allocator.free(got);
                try std.testing.expectEqualSlices(i64, c[1], got);
            }

            // A row still open at the failing WHEN still fails, and a first
            // WHEN is read by every row.
            try expectQueryError(allocator, db, "SELECT CASE WHEN d < 0 THEN 1 WHEN d * d * d > 0 THEN 2 END FROM g", error.ArithmeticOverflow);
            try expectQueryError(allocator, db, "SELECT CASE WHEN d < 0 THEN 1 WHEN CAST(d AS DECIMAL(5,2)) > 3 THEN 2 END FROM g", error.ArithmeticOverflow);
            try expectQueryError(allocator, db, "SELECT CASE WHEN d * d * d > 0 THEN 1 ELSE 2 END FROM g", error.ArithmeticOverflow);
            try expectQueryError(allocator, db, "SELECT SUM(CASE WHEN d < 0 THEN 1 WHEN d * d * d > 0 THEN 2 END) FROM g GROUP BY k", error.ArithmeticOverflow);
        }
    }
}

test "TRIM removes spaces, a character set, or whole copies of a string" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, s VARCHAR(20) NOT NULL, both VARCHAR(20) NOT NULL)");
    try helpers.exec(allocator, db, "INSERT INTO t (id, s, both) VALUES (1, 'xxhixx', ' a'), (2, '  hi  ', 'b '), (3, 'hiabab', 'c'), (4, 'abhiba', 'd'), (5, 'éhié', 'e'), (6, '\t hi', 'f')");

    const cases = .{
        // MySQL's keyword form removes whole copies of remstr, spaces by default.
        .{ "TRIM(LEADING 'x' FROM s)", .{ "hixx", "  hi  ", "hiabab", "abhiba", "éhié", "\t hi" } },
        .{ "TRIM(TRAILING 'ab' FROM s)", .{ "xxhixx", "  hi  ", "hi", "abhiba", "éhié", "\t hi" } },
        .{ "TRIM(BOTH 'ab' FROM s)", .{ "xxhixx", "  hi  ", "hi", "hiba", "éhié", "\t hi" } },
        .{ "TRIM('x' FROM s)", .{ "hi", "  hi  ", "hiabab", "abhiba", "éhié", "\t hi" } },
        .{ "TRIM('é' FROM s)", .{ "xxhixx", "  hi  ", "hiabab", "abhiba", "hi", "\t hi" } },
        .{ "TRIM(BOTH FROM s)", .{ "xxhixx", "hi", "hiabab", "abhiba", "éhié", "\t hi" } },
        .{ "TRIM(LEADING FROM s)", .{ "xxhixx", "hi  ", "hiabab", "abhiba", "éhié", "\t hi" } },
        .{ "TRIM(TRAILING FROM s)", .{ "xxhixx", "  hi", "hiabab", "abhiba", "éhié", "\t hi" } },
        // The call forms remove spaces only, or any character of a set, as
        // StarRocks and DuckDB do.
        .{ "TRIM(s)", .{ "xxhixx", "hi", "hiabab", "abhiba", "éhié", "\t hi" } },
        .{ "TRIM(s, 'ab')", .{ "xxhixx", "  hi  ", "hi", "hi", "éhié", "\t hi" } },
        .{ "LTRIM(s, 'x')", .{ "hixx", "  hi  ", "hiabab", "abhiba", "éhié", "\t hi" } },
        .{ "RTRIM(s, 'ba')", .{ "xxhixx", "  hi  ", "hi", "abhi", "éhié", "\t hi" } },
        .{ "TRIM(s, 'é')", .{ "xxhixx", "  hi  ", "hiabab", "abhiba", "hi", "\t hi" } },
        // A lone side word is a column of that name.
        .{ "TRIM(both)", .{ "a", "b", "c", "d", "e", "f" } },
    };
    inline for (cases) |c| {
        const want: [6]?[]const u8 = c[1];
        expectTextColumn(allocator, db, "SELECT " ++ c[0] ++ " FROM t ORDER BY id", &want) catch |err| {
            std.debug.print("expr: {s}\n", .{c[0]});
            return err;
        };
    }
    const matched = try helpers.collectBigints(allocator, db, "SELECT id FROM t WHERE TRIM(LEADING 'x' FROM s) = 'hixx' OR TRIM(TRAILING 'ab' FROM s) = 'hi' ORDER BY id");
    defer allocator.free(matched);
    try std.testing.expectEqualSlices(i64, &.{ 1, 3 }, matched);
}

test "ASSERT_TRUE is true where its condition holds and fails the query on a false or NULL one" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE at (id BIGINT PRIMARY KEY, v BIGINT)");
    try helpers.exec(allocator, db, "INSERT INTO at VALUES (1, 10), (2, 20), (3, NULL)");

    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("at", .{})).flush();
        try helpers.expectTexts(allocator, db, "SELECT CAST(ASSERT_TRUE(v > 0) AS CHAR) FROM at WHERE id <= 2 ORDER BY id", &.{ "1", "1" });
        try helpers.expectText(allocator, db, "SELECT CAST(ASSERT_TRUE(COUNT(v) = 2 AND COUNT(DISTINCT v) = 2) AS CHAR) FROM at", "1");
        try std.testing.expectError(error.AssertTrueFailed, helpers.exec(allocator, db, "SELECT ASSERT_TRUE(v > 10) FROM at"));
        try std.testing.expectError(error.AssertTrueFailed, helpers.exec(allocator, db, "SELECT ASSERT_TRUE(v > 0) FROM at"));
        try std.testing.expectError(error.AssertTrueFailed, helpers.exec(allocator, db, "SELECT ASSERT_TRUE(COUNT(*) = 2) FROM at"));
    }
}

test "compute: TO_BINARY, FROM_BINARY, FORMAT_BYTES, BAR and POSITIVE follow StarRocks" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE b (id BIGINT PRIMARY KEY, s VARCHAR(16), f VARCHAR(10), n BIGINT)");
    try helpers.exec(allocator, db,
        \\INSERT INTO b VALUES (1, 'ab01', 'hex', 0), (2, 'qwE=', 'ENCODE64', 1023), (3, 'ab01', 'utf8', 1536), (4, '1', 'hex', 123456789),
        \\  (5, '', 'encode64', -1), (6, NULL, 'hex', NULL), (7, 'abab', NULL, 1152921504606846976)
    );
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("b", .{})).flush();
        try helpers.expectTexts(allocator, db, "SELECT HEX(TO_BINARY(s, f)) FROM b ORDER BY id", &.{ "AB01", "AB01", "61623031", "", null, null, null });
        try helpers.expectTexts(allocator, db, "SELECT FROM_BINARY(TO_BINARY(s), 'encode64') FROM b ORDER BY id", &.{ "qwE=", "", "qwE=", "", "", null, "q6s=" });
        try helpers.expectTexts(allocator, db, "SELECT FROM_BINARY(s, f) FROM b ORDER BY id", &.{ "61623031", "cXdFPQ==", "ab01", "31", "", null, null });
        try helpers.expectTexts(allocator, db, "SELECT FORMAT_BYTES(n) FROM b ORDER BY id", &.{ "0 B", "1023 B", "1.50 KB", "117.74 MB", null, null, "1.00 EB" });
    }
    try helpers.expectText(allocator, db, "SELECT FORMAT_BYTES(1048576 + 52429)", "1.05 MB");
    try std.testing.expectError(error.InvalidBinaryFormat, helpers.exec(allocator, db, "SELECT TO_BINARY('ab', 'base32')"));
    try helpers.expectTexts(allocator, db, "SELECT BAR(x, 0, 10, 20) FROM generate_series(0, 10, 5) AS t(x)", &.{ "", "▓▓▓▓▓▓▓▓▓▓", "▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓" });
    try helpers.expectText(allocator, db, "SELECT BAR(3, 0, 7, 4)", "▓");
    try std.testing.expectError(error.InvalidBarArguments, helpers.exec(allocator, db, "SELECT BAR(11, 0, 10, 20)"));
    try helpers.expectText(allocator, db, "SELECT CAST(POSITIVE(-2.50) AS CHAR)", "-2.50");
    try helpers.expectTexts(allocator, db, "SELECT id FROM b WHERE POSITIVE(n) > 1000 ORDER BY id", &.{ "2", "3", "4", "7" });
}

test "EQUIWIDTH_BUCKET counts whole-width buckets from min, as StarRocks does" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    const cases = .{
        .{ "5, 0, 10, 3", "1" },
        .{ "9, 0, 10, 3", "3" },
        .{ "10, 0, 10, 3", "3" },
        .{ "7, 0, 10, 4", "3" },
        .{ "0, 0, 10, 20", "0" },
        .{ "10, 0, 10, 20", "10" },
        .{ "10, 0, 10, 1", "1" },
        .{ "3, 1, 10, 2", "0" },
        .{ "99, 0, 100, 7", "7" },
        .{ "10, 2, 10, 3", "4" },
        .{ "-5, -10, -1, 4", "2" },
        .{ "5, -10, 10, 4", "3" },
        .{ "100, 50, 9223372036854775807, 3", "0" },
        .{ "9223372036854775807, 0, 9223372036854775807, 2", "2" },
        .{ "NULL, 0, 10, 3", null },
        .{ "5, 0, NULL, 3", null },
    };
    inline for (cases) |c| try helpers.expectText(allocator, db, "SELECT EQUIWIDTH_BUCKET(" ++ c[0] ++ ")", c[1]);
    try helpers.expectTexts(allocator, db, "SELECT EQUIWIDTH_BUCKET(r, 0, 10, 3) FROM TABLE(generate_series(0, 10)) AS s(r)", &.{ "0", "0", "0", "1", "1", "1", "2", "2", "2", "3", "3" });
    inline for (.{ "11, 0, 10, 3", "1, 2, 10, 3", "5, 10, 0, 3", "5, 5, 5, 3", "5, 0, 10, 0", "5, 0, 10, -2" }) |args| {
        try std.testing.expectError(error.InvalidEquiwidthBucketArguments, helpers.exec(allocator, db, "SELECT EQUIWIDTH_BUCKET(" ++ args ++ ")"));
    }

    try helpers.exec(allocator, db, "CREATE TABLE h (id BIGINT PRIMARY KEY, v INT)");
    try helpers.exec(allocator, db, "INSERT INTO h VALUES (1, 2), (2, NULL), (3, 50), (4, 100)");
    try helpers.expectTexts(allocator, db, "SELECT EQUIWIDTH_BUCKET(v, 0, 100, 7) FROM h ORDER BY id", &.{ "0", null, "3", "7" });
}

test "AES_ENCRYPT and AES_DECRYPT in every StarRocks mode, with MySQL's key folding" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    const key = "'F3229A0B371ED2D9441B830D21A390C3'";
    const cases = .{
        .{ "TO_BASE64(AES_ENCRYPT('starrocks', " ++ key ++ "))", "uv/Lhzm74syo8JlfWarwKA==" },
        .{ "TO_BASE64(AES_ENCRYPT('starrocks', " ++ key ++ ", NULL, 'aes_128_ecb'))", "uv/Lhzm74syo8JlfWarwKA==" },
        .{ "TO_BASE64(AES_ENCRYPT('starrocks', " ++ key ++ ", '', 'AES_192_ECB'))", "xaf+kW/d5YPRSYFGnFkOfQ==" },
        .{ "TO_BASE64(AES_ENCRYPT('starrocks', " ++ key ++ ", '', 'AES_128_CBC'))", "ne2YHEKECQSni3/kRI/nBg==" },
        .{ "TO_BASE64(AES_ENCRYPT('starrocks', " ++ key ++ ", 'abcdefg', 'AES_256_CBC'))", "4w0ceQx2FqG8GGTivC0QPg==" },
        .{ "TO_BASE64(AES_ENCRYPT('starrocks', " ++ key ++ ", 'abcdefg', 'AES_192_CFB1'))", "MltCAryodTud" },
        .{ "TO_BASE64(AES_ENCRYPT('starrocks', " ++ key ++ ", 'abcdefg', 'AES_256_CFB8'))", "p8NH5PX2mufr" },
        .{ "TO_BASE64(AES_ENCRYPT('starrocks', " ++ key ++ ", '', 'AES_128_CFB'))", "3wGmUeFO4/Ex" },
        .{ "TO_BASE64(AES_ENCRYPT('starrocks', " ++ key ++ ", 'abcdefg', 'AES_192_OFB'))", "Ma7ozVBB4lGP" },
        .{ "TO_BASE64(AES_ENCRYPT('starrocks', " ++ key ++ ", 'abcdefg', 'AES_256_CTR'))", "p6+axkiBSNf2" },
        .{ "TO_BASE64(AES_ENCRYPT('starrocks', " ++ key ++ ", 'abcdefghijklmnop', 'AES_192_GCM', 'abcdefg'))", "YWJjZGVmZ2hpamtsIpxL5blh0/zHNXw5nG/tL68lc1BJZ/fCPA==" },
        .{ "AES_DECRYPT(FROM_BASE64('uv/Lhzm74syo8JlfWarwKA=='), " ++ key ++ ")", "starrocks" },
        .{ "AES_DECRYPT(FROM_BASE64('h5rRJ9HmkeqM'), " ++ key ++ ", 'abcdefg', 'AES_256_CFB1')", "starrocks" },
        .{ "AES_DECRYPT(FROM_BASE64('YWJjZGVmZ2hpamts2bRovmWbg/Y5qmz8Lt1L2oebY6lOV92Ozw=='), " ++ key ++ ", 'abcdefghijklmnop', 'AES_256_GCM', 'abcdefg')", "starrocks" },
        .{ "AES_ENCRYPT('starrocks', " ++ key ++ ", NULL, 'AES_128_CBC')", null },
        .{ "AES_ENCRYPT(NULL, " ++ key ++ ")", null },
        .{ "AES_DECRYPT(FROM_BASE64('YWJjZGVmZ2hpamts2bRovmWbg/Y5qmz8Lt1L2oebY6lOV92Ozw=='), " ++ key ++ ", '', 'AES_256_GCM', 'other')", null },
        .{ "AES_DECRYPT('not a ciphertext', " ++ key ++ ")", null },
        .{ "AES_DECRYPT(AES_ENCRYPT('x', 'k1'), 'k2')", null },
        .{ "AES_DECRYPT(AES_ENCRYPT('folded key', 'a key longer than sixteen bytes'), 'a key longer than sixteen bytes')", "folded key" },
    };
    inline for (cases) |c| try helpers.expectText(allocator, db, "SELECT " ++ c[0], c[1]);
    try std.testing.expectError(error.InvalidEncryptionMode, helpers.exec(allocator, db, "SELECT AES_ENCRYPT('a', 'k', '', 'AES_512_ECB')"));
    try std.testing.expectError(error.InvalidEncryptionMode, helpers.exec(allocator, db, "SELECT AES_DECRYPT('a', 'k', '', 'AES_128_CBC', 'aad')"));

    try helpers.exec(allocator, db, "CREATE TABLE secret (id BIGINT PRIMARY KEY, msg VARCHAR(40), k VARCHAR(40), mode VARCHAR(16))");
    try helpers.exec(allocator, db,
        \\INSERT INTO secret VALUES (1, 'one', 'key one', 'AES_128_CBC'), (2, '', 'key two', 'AES_256_GCM'),
        \\  (3, 'a message of more than two blocks', 'k', 'AES_192_CTR'), (4, NULL, 'k', 'AES_128_OFB'), (5, 'five', NULL, 'AES_128_CFB8')
    );
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("secret", .{})).flush();
        try helpers.expectTexts(allocator, db, "SELECT AES_DECRYPT(AES_ENCRYPT(msg, k, 'iv', mode), k, 'iv', mode) FROM secret ORDER BY id", &.{ "one", "", "a message of more than two blocks", null, null });
        try helpers.expectTexts(allocator, db, "SELECT OCTET_LENGTH(AES_ENCRYPT(msg, k, 'iv', mode)) FROM secret ORDER BY id", &.{ "16", "28", "33", null, null });
    }
}

test "URL functions read a URL's host and query parameters, and encode text as a form does" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE u (id BIGINT PRIMARY KEY, url VARCHAR(80), k VARCHAR(8))");
    try helpers.exec(allocator, db,
        \\INSERT INTO u VALUES (1, 'https://a.com/doc?k1=10&k2=3&k1=100', 'k1'), (2, 'https://u:p@b.org:8030/?x&k2=%41+b#k2=no', 'k2'),
        \\  (3, 'https://c.net/doc?k1=10', 'k3'), (4, 'https://d.io/doc?k 1=2', 'k 1'), (5, 'mailto:me?x=1', 'x'), (6, '', 'k1'),
        \\  (7, NULL, 'k1'), (8, 'https://e.com/?k1', 'k1'), (9, 'http://[::1]:80/', NULL), (10, '/123?k=1&v=2', 'v'),
        \\  (11, '//f.com/a?v=3', 'v'), (12, 'a/b:c?v=4', 'v'), (13, '1a:b?v=5', 'v')
    );
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("u", .{})).flush();
        try helpers.expectTexts(allocator, db, "SELECT URL_EXTRACT_HOST(url) FROM u ORDER BY id", &.{ "a.com", "b.org", "c.net", null, null, null, null, "e.com", "[::1]", null, "f.com", null, null });
        try helpers.expectTexts(allocator, db, "SELECT URL_EXTRACT_PARAMETER(url, k) FROM u ORDER BY id", &.{ "10", "A b", null, null, "1", null, null, "", null, "2", "3", "4", null });
        try helpers.expectTexts(allocator, db, "SELECT URL_DECODE(URL_ENCODE(url)) = url FROM u WHERE id <= 6 ORDER BY id", &.{ "1", "1", "1", "1", "1", "1" });
    }
    try helpers.expectText(allocator, db, "SELECT URL_ENCODE('a b/c~é*-._')", "a+b%2Fc%7E%C3%A9*-._");
    try helpers.expectText(allocator, db, "SELECT URL_DECODE('a+b%2Fc%7e%C3%A9%2x%')", "a b/c~é%2x%");
    try helpers.expectText(allocator, db, "SELECT URL_ENCODE(NULL)", null);
}

test "NGRAM_SEARCH is the share of the needle's grams the haystack holds, in single precision as StarRocks computes it" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE ng (id BIGINT PRIMARY KEY, s VARCHAR(40), needle VARCHAR(40), n INT)");
    try helpers.exec(allocator, db,
        \\INSERT INTO ng VALUES (1, 'chinese', 'china', 4), (2, 'CHINESE', 'chinese', 4), (3, 'aaaa', 'aaaaaa', 2), (4, 'chi', 'chi', 4),
        \\  (5, NULL, 'x', 1), (6, 'abc', 'abc', NULL), (7, '000073a7-274f-46bf-bfaf-678868cc26cd', '1bd32347-274f-4a30-93f3-9087594de9cd', 4)
    );
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("ng", .{})).flush();
        try helpers.expectTexts(allocator, db, "SELECT CAST(NGRAM_SEARCH(s, needle, n) AS CHAR) FROM ng ORDER BY id", &.{ "0.5", "0", "0.6000000238418579", "0", null, null, "0.15151512622833252" });
        try helpers.expectTexts(allocator, db, "SELECT CAST(NGRAM_SEARCH_CASE_INSENSITIVE(s, needle, n) AS CHAR) FROM ng WHERE id <= 2 ORDER BY id", &.{ "0.5", "1" });
    }
    try std.testing.expectError(error.InvalidNgramLength, helpers.exec(allocator, db, "SELECT NGRAM_SEARCH('chi', 'chi', 0)"));
    try std.testing.expectError(error.InvalidNgramLength, helpers.exec(allocator, db, "SELECT NGRAM_SEARCH(s, needle, -1) FROM ng"));
}

test "XX_HASH3_128 is XXH3-128 of its arguments run together and XX_HASH3_64 chains each hash into the next seed, as StarRocks computes them" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    const cases = .{
        .{ "XX_HASH3_128('hello')", "-98478366302105124680504504609445627880" },
        .{ "XX_HASH3_128('hello', 'world')", "-45235302294609689438180196264627189905" },
        .{ "XX_HASH3_128('hello', 'world') = XX_HASH3_128('helloworld')", "1" },
        .{ "XX_HASH3_128('hello', 'world', 'abc')", "45899992149942203672094639952011705760" },
        .{ "XX_HASH3_128('hello', REPEAT('abcdefgh', 40))", "-69142760343838966408980957017265335729" },
        .{ "XX_HASH3_128(REPEAT('abcdefgh', 40), 'hello')", "-129982448230658873377600368474277818453" },
        .{ "BIT_SHIFT_RIGHT(XX_HASH3_128('starrocks'), 64)", "3846997910503780466" },
        .{ "BITAND(XX_HASH3_128('starrocks'), 18446744073709551615)", "1697546255957561686" },
        .{ "XX_HASH3_128('hello', NULL)", null },
        .{ "TYPEOF(XX_HASH3_128('hello'))", "largeint" },
        .{ "XX_HASH3_64('hello')", "-7685981735718036227" },
        .{ "XX_HASH3_64('hello', 'world')", "7001965798170371843" },
        .{ "XX_HASH3_64(NULL, 'world')", null },
        .{ "TYPEOF(XX_HASH3_64('hello'))", "bigint" },
    };
    inline for (cases) |c| try helpers.expectText(allocator, db, "SELECT " ++ c[0], c[1]);

    try helpers.exec(allocator, db, "CREATE TABLE h (id BIGINT PRIMARY KEY, a VARCHAR(16), b VARCHAR(16))");
    try helpers.exec(allocator, db, "INSERT INTO h VALUES (1, 'hello', 'world'), (2, 'hello', 'starrocks'), (3, 'hello', NULL)");
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("h", .{})).flush();
        try helpers.expectTexts(allocator, db, "SELECT BIT_SHIFT_RIGHT(XX_HASH3_128(a, b), 64) FROM h ORDER BY id", &.{ "-2452210651042717451", "1559307639436096304", null });
        try helpers.expectTexts(allocator, db, "SELECT BITAND(XX_HASH3_128(a, b), 18446744073709551615) FROM h ORDER BY id", &.{ "1087493910761260911", "8859976453967563600", null });
    }
}

test "StarRocks' bit functions keep all 128 bits of a LARGEINT" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    const cases = .{
        .{ "BITAND(CAST(-1 AS LARGEINT), 18446744073709551615)", "18446744073709551615" },
        .{ "BITAND(170141183460469231731687303715884105727, CAST(-18446744073709551616 AS LARGEINT))", "170141183460469231713240559642174554112" },
        .{ "BITOR(CAST(18446744073709551616 AS LARGEINT), 1)", "18446744073709551617" },
        .{ "BITXOR(CAST(-1 AS LARGEINT), 18446744073709551615)", "-18446744073709551616" },
        .{ "BITNOT(CAST(18446744073709551615 AS LARGEINT))", "-18446744073709551616" },
        .{ "BIT_SHIFT_LEFT(CAST(1 AS LARGEINT), 100)", "1267650600228229401496703205376" },
        .{ "BIT_SHIFT_RIGHT(CAST(-18446744073709551616 AS LARGEINT), 64)", "-1" },
        .{ "BIT_SHIFT_RIGHT(1267650600228229401496703205376, 99)", "2" },
        .{ "TYPEOF(BITAND(CAST(1 AS LARGEINT), 1))", "largeint" },
        .{ "TYPEOF(BITAND(6, 3))", "bigint" },
        .{ "BITAND(6, 3)", "2" },
        .{ "BIT_SHIFT_LEFT(1, 62)", "4611686018427387904" },
    };
    inline for (cases) |c| try helpers.expectText(allocator, db, "SELECT " ++ c[0], c[1]);
}

test "StarRocks' extra overloads: LAST_DAY by unit, STRPOS by instance, two-argument pads, ARRAY_GENERATE(n), one-argument GREATEST and LEAST, UNIX_TIMESTAMP()" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();
    const cases = .{
        .{ "CAST(LAST_DAY('2023-02-12', 'month') AS CHAR)", "2023-02-28" },
        .{ "CAST(LAST_DAY('2023-05-10', 'Quarter') AS CHAR)", "2023-06-30" },
        .{ "CAST(LAST_DAY('2023-11-10', 'QUARTER') AS CHAR)", "2023-12-31" },
        .{ "CAST(LAST_DAY('2024-02-10 10:00:00', 'year') AS CHAR)", "2024-12-31" },
        .{ "CAST(LAST_DAY(DATE '2024-02-10', 'month') AS CHAR)", "2024-02-29" },
        .{ "LAST_DAY('2023-05-10', NULL)", null },
        .{ "LAST_DAY(NULL, 'month')", null },
        .{ "TYPEOF(LAST_DAY('2023-05-10', 'year'))", "date" },
        .{ "STRPOS('abc', 'b', 1)", "2" },
        .{ "STRPOS('abcabc', 'abc', 2)", "4" },
        .{ "STRPOS('abcabc', 'abc', 3)", "0" },
        .{ "STRPOS('hello world hello', 'hello', 2)", "13" },
        .{ "STRPOS('abcabc', 'abc', -1)", "4" },
        .{ "STRPOS('abcabc', 'abc', -2)", "1" },
        .{ "STRPOS('abcabc', 'abc', -3)", "0" },
        .{ "STRPOS('aaa', 'aa', 2)", "2" },
        .{ "STRPOS('aaa', 'aa', -1)", "2" },
        .{ "STRPOS('aaa', 'aa', -2)", "1" },
        .{ "STRPOS('abc', '', 5)", "1" },
        .{ "STRPOS('abc', '', -2)", "1" },
        .{ "STRPOS('abc', '', 0)", "0" },
        .{ "STRPOS('', 'abc', 1)", "0" },
        .{ "STRPOS('abc', 'abc', 0)", "0" },
        .{ "STRPOS('中文中文', '文', 2)", "4" },
        .{ "STRPOS('中文中文', '文', -2)", "2" },
        .{ "STRPOS('abc', 'b', NULL)", null },
        .{ "LPAD('test', 8)", "    test" },
        .{ "LPAD('test', 2)", "te" },
        .{ "LPAD('test', 0)", "" },
        .{ "RPAD('test', 8)", "test    " },
        .{ "LPAD('中文', 3)", " 中文" },
        .{ "RPAD('中文', 5)", "中文   " },
        .{ "LPAD('test', -1)", null },
        .{ "RPAD('test', -1, 'x')", null },
        .{ "LPAD('test', -1, '')", null },
        .{ "LPAD(NULL, 3)", null },
        .{ "LPAD('test', NULL)", null },
        .{ "ARRAY_GENERATE(9)", "[1,2,3,4,5,6,7,8,9]" },
        .{ "ARRAY_GENERATE(1)", "[1]" },
        .{ "ARRAY_GENERATE(0)", "[1,0]" },
        .{ "ARRAY_GENERATE(-3)", "[1,0,-1,-2,-3]" },
        .{ "ARRAY_GENERATE(NULL)", null },
        .{ "LEAST(5)", "5" },
        .{ "GREATEST(5)", "5" },
        .{ "LEAST('x')", "x" },
        .{ "CAST(GREATEST(5.5) AS CHAR)", "5.5" },
        .{ "CAST(GREATEST(DATE '2024-01-01') AS CHAR)", "2024-01-01" },
        .{ "LEAST(NULL)", null },
        .{ "UNIX_TIMESTAMP() > 1700000000", "1" },
        .{ "UNIX_TIMESTAMP() = UNIX_TIMESTAMP(NOW())", "1" },
    };
    inline for (cases) |c| try helpers.expectText(allocator, db, "SELECT " ++ c[0], c[1]);
    inline for (.{ "'week'", "'day'", "''", "'months'" }) |unit| {
        try std.testing.expectError(error.InvalidLastDayUnit, helpers.exec(allocator, db, "SELECT LAST_DAY('2023-05-10', " ++ unit ++ ")"));
    }

    try helpers.exec(allocator, db, "CREATE TABLE ov (id BIGINT PRIMARY KEY, d DATE, dt DATETIME, unit VARCHAR(8), s VARCHAR(20), sub VARCHAR(20), n INT, ts BIGINT)");
    try helpers.exec(allocator, db,
        \\INSERT INTO ov VALUES (1, '2020-02-12', '2020-02-12 08:08:14', 'month', 'hello world', 'world', 1, UNIX_TIMESTAMP()),
        \\  (2, '2021-03-28', '2021-03-28 08:08:14', 'quarter', 'hello world', 'world', -1, UNIX_TIMESTAMP()),
        \\  (3, '2022-04-28', '2022-04-28 08:08:14', 'year', 'abcabc', 'abc', 2, UNIX_TIMESTAMP()),
        \\  (4, NULL, NULL, NULL, 'test', NULL, -1, NULL)
    );
    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("ov", .{})).flush();
        try helpers.expectTexts(allocator, db, "SELECT CAST(LAST_DAY(d, unit) AS CHAR) FROM ov ORDER BY id", &.{ "2020-02-29", "2021-03-31", "2022-12-31", null });
        try helpers.expectTexts(allocator, db, "SELECT CAST(LAST_DAY(dt, unit) AS CHAR) FROM ov ORDER BY id", &.{ "2020-02-29", "2021-03-31", "2022-12-31", null });
        try helpers.expectTexts(allocator, db, "SELECT CAST(LAST_DAY(d, 'quarter') AS CHAR) FROM ov ORDER BY id", &.{ "2020-03-31", "2021-03-31", "2022-06-30", null });
        try helpers.expectTexts(allocator, db, "SELECT STRPOS(s, sub, n) FROM ov ORDER BY id", &.{ "7", "7", "4", null });
        try helpers.expectTexts(allocator, db, "SELECT LPAD(s, n + 9) FROM ov ORDER BY id", &.{ "hello worl", "hello wo", "     abcabc", "    test" });
        try helpers.expectTexts(allocator, db, "SELECT RPAD(s, n) FROM ov ORDER BY id", &.{ "h", null, "ab", null });
        try helpers.expectTexts(allocator, db, "SELECT ARRAY_GENERATE(n) FROM ov ORDER BY id", &.{ "[1]", "[1,0,-1]", "[1,2]", "[1,0,-1]" });
        try helpers.expectTexts(allocator, db, "SELECT GREATEST(n) FROM ov ORDER BY id", &.{ "1", "-1", "2", "-1" });
        try helpers.expectTexts(allocator, db, "SELECT COUNT(*) FROM ov WHERE ts > 1700000000", &.{"3"});
    }
}
