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
        "SELECT sqrt('x') AS v FROM raw",
        "SELECT upper(s) AS u, concat('a', sqrt('x')) AS v FROM raw",
        "SELECT CASE WHEN id > 0 THEN 'y' ELSE sqrt('x') END AS v FROM raw",
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
