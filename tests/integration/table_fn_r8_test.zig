//! Round-8 TVF features: multi-input pass-through (row-aligned to input 0)
//! and TVF output-order advertisement (TVF→TVF and window→TVF rides).

const std = @import("std");
const thindb = @import("thindb");
const tdb = thindb.tdb;
const helpers = @import("sql_helpers.zig");

const schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "g", .type = .int },
        .{ .name = "amt", .type = .bigint },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const ok = [_][]const u8{"id"};
const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 8 };

fn seed(db: *thindb.Database) !void {
    const t = try db.table("t", schema, opts);
    try t.insert(&.{
        .{ .id = @as(i64, 4), .g = @as(i32, 2), .amt = @as(i64, 40) },
        .{ .id = @as(i64, 1), .g = @as(i32, 1), .amt = @as(i64, 10) },
        .{ .id = @as(i64, 3), .g = @as(i32, 1), .amt = @as(i64, 30) },
        .{ .id = @as(i64, 2), .g = @as(i32, 1), .amt = @as(i64, 20) },
        .{ .id = @as(i64, 5), .g = @as(i32, 2), .amt = @as(i64, 50) },
    });
    try t.flush();
}

fn registryFor(db: *thindb.Database) *const thindb.UdfRegistry {
    if (db.catalog) |catalog| return &catalog.udfs;
    return &db.owned_catalog.?.udfs;
}

fn run(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) !helpers.RunResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const root = try thindb.sql.parseDialectWithUdfs(arena.allocator(), sql, .neutral, registryFor(db));
    const cq = try thindb.net.compile(allocator, db, root);
    return .{
        .arena = arena,
        .cq = cq,
        .owned_vars = cq.sessionValue().vars,
        .backing_allocator = allocator,
    };
}

// ---------------------------------------------------------------------------
// Multi-input pass-through: row-aligned 1:1 with input 0, small lookup input.
// ---------------------------------------------------------------------------

/// val = amt * rate; rate comes from the co-partitioned lookup input (one
/// row per key; an EMPTY lookup partition yields NULL vals). The kernel
/// sees only input 0's kernel prefix (amt) — id is pass-through, g is a
/// carry-style key column past the prefix.
fn fxConvert(
    ctx: *const thindb.udf.TvfContext,
    parts: []const thindb.udf.TvfPartition,
    out: *thindb.udf.TvfOutput,
) !void {
    _ = ctx;
    const main = &parts[0];
    const rates = &parts[1];
    const amts = main.columns[0].data.bigint;
    const rate: ?i64 = if (rates.row_count > 0) rates.columns[1].data.bigint[0] else null;
    const val = out.columns[0];
    for (0..main.row_count) |i| {
        try val.data.bigint.append(out.allocator, if (rate) |r| amts[i] * r else 0);
        try val.appendValidBit(out.allocator, val.rowCount() - 1, rate != null);
    }
}

const fx_in0 = [_]thindb.Column{
    .{ .name = "amt", .type = .bigint },
    .{ .name = "id", .type = .bigint },
    .{ .name = "g", .type = .int },
};
const fx_in1 = [_]thindb.Column{
    .{ .name = "g", .type = .int },
    .{ .name = "rate", .type = .bigint },
};
const fx_out = [_]thindb.Column{
    .{ .name = "id", .type = .bigint },
    .{ .name = "val", .type = .bigint, .nullable = true },
};

fn seedRates(db: *thindb.Database) !void {
    const rates_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "g", .type = .int },
            .{ .name = "rate", .type = .bigint },
        },
        .order_key = &.{"g"},
        .unique = true,
    };
    const rk = [_][]const u8{"g"};
    const t = try db.table("rates", rates_schema, .{ .order_key = &rk, .unique = true, .row_group_size = 8 });
    // Only g=1 has a rate — g=2's lookup partition arrives EMPTY.
    try t.insert(&.{.{ .g = @as(i32, 1), .rate = @as(i64, 2) }});
    try t.flush();
}

fn registerFx(db: *thindb.Database) !void {
    try db.registerTableUdf(.{
        .name = "fx_convert",
        .input_schemas = &.{ &fx_in0, &fx_in1 },
        .output_schema = &fx_out,
        .execution = .partitioned,
        .row_aligned = true,
        .passthrough = &.{.{ .out_idx = 0, .in_idx = 1 }},
        .kernel_input_cols = 1,
        .process = fxConvert,
    });
}

fn expectFxValues(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    // No ORDER BY: keys must resolve in EVERY co-partitioned input's
    // schema and the lookup input has no `id`; row alignment to input 0
    // is order-independent anyway.
    var res = try run(allocator, db,
        \\SELECT id, val FROM TABLE(fx_convert(
        \\  (SELECT amt, id, g FROM t),
        \\  (SELECT g, rate FROM rates)
        \\) PARTITION BY g)
    );
    defer res.deinit();
    var vals: std.AutoHashMapUnmanaged(i64, ?i64) = .empty;
    defer vals.deinit(allocator);
    while (try res.next()) |batch| {
        for (0..batch.row_count) |i| {
            try vals.put(
                allocator,
                batch.values[0].data.bigint[i],
                if (batch.values[1].isValid(i)) batch.values[1].data.bigint[i] else null,
            );
        }
    }
    try std.testing.expectEqual(@as(usize, 5), vals.count());
    try std.testing.expectEqual(@as(??i64, @as(?i64, 20)), vals.get(1));
    try std.testing.expectEqual(@as(??i64, @as(?i64, 40)), vals.get(2));
    try std.testing.expectEqual(@as(??i64, @as(?i64, 60)), vals.get(3));
    try std.testing.expectEqual(@as(??i64, @as(?i64, null)), vals.get(4));
    try std.testing.expectEqual(@as(??i64, @as(?i64, null)), vals.get(5));
}

test "table UDF multi-input passthrough: row-aligned to input 0 (serial)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(db);
    try seedRates(db);
    try registerFx(db);
    try expectFxValues(allocator, db);
}

test "table UDF multi-input passthrough: parallel matches serial" {
    var gpa = std.heap.DebugAllocator(.{ .thread_safe = true }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();
    try seed(db);
    try seedRates(db);
    try registerFx(db);

    thindb.exec.table_fn.force_parallel_in_tests = true;
    defer thindb.exec.table_fn.force_parallel_in_tests = false;
    try expectFxValues(allocator, db);
}

test "table UDF multi-input passthrough: validation" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // Multi-input passthrough without row_aligned: rejected.
    try std.testing.expectError(thindb.udf.Error.FunctionInvalidDefinition, db.registerTableUdf(.{
        .name = "bad_multi",
        .input_schemas = &.{ &fx_in0, &fx_in1 },
        .output_schema = &fx_out,
        .row_aligned = false,
        .passthrough = &.{.{ .out_idx = 0, .in_idx = 1 }},
        .process = fxConvert,
    }));
    // Pair in_idx must index INPUT 0's columns — an index valid only for
    // input 1 is rejected.
    try std.testing.expectError(thindb.udf.Error.FunctionInvalidDefinition, db.registerTableUdf(.{
        .name = "bad_multi2",
        .input_schemas = &.{ &fx_in1, &fx_in0 },
        .output_schema = &fx_out,
        .row_aligned = true,
        .passthrough = &.{.{ .out_idx = 0, .in_idx = 2 }},
        .process = fxConvert,
    }));
}

// ---------------------------------------------------------------------------
// TVF output-order advertisement: TVF→TVF and window→TVF rides.
// ---------------------------------------------------------------------------

/// Row-GENERATING kernel with `ordered_output`: re-emits the partition's
/// rows in order, then one synthetic trailer row (id = last + 100, amt 0)
/// — the gap-fill shape. Keys (g, id) are computed outputs, so only the
/// flag can advertise the order.
fn gapPad(
    ctx: *const thindb.udf.TvfContext,
    parts: []const thindb.udf.TvfPartition,
    out: *thindb.udf.TvfOutput,
) !void {
    _ = ctx;
    const p = &parts[0];
    const ids = p.columns[0].data.bigint;
    const gs = p.columns[1].data.int;
    const amts = p.columns[2].data.bigint;
    for (0..p.row_count) |i| {
        try out.columns[0].data.bigint.append(out.allocator, ids[i]);
        try out.columns[1].data.int.append(out.allocator, gs[i]);
        try out.columns[2].data.bigint.append(out.allocator, amts[i]);
    }
    if (p.row_count > 0) {
        try out.columns[0].data.bigint.append(out.allocator, ids[p.row_count - 1] + 100);
        try out.columns[1].data.int.append(out.allocator, gs[p.row_count - 1]);
        try out.columns[2].data.bigint.append(out.allocator, 0);
    }
}

fn runningTotal(
    ctx: *const thindb.udf.TvfContext,
    parts: []const thindb.udf.TvfPartition,
    out: *thindb.udf.TvfOutput,
) !void {
    _ = ctx;
    const part = &parts[0];
    const ids = part.columns[0].data.bigint;
    const amts = part.columns[2].data.bigint;
    var running: i64 = 0;
    for (0..part.row_count) |i| {
        running += amts[i];
        try out.columns[0].data.bigint.append(out.allocator, ids[i]);
        try out.columns[1].data.bigint.append(out.allocator, running);
    }
}

const iga_cols = [_]thindb.Column{
    .{ .name = "id", .type = .bigint },
    .{ .name = "g", .type = .int },
    .{ .name = "amt", .type = .bigint },
};
const running_out = [_]thindb.Column{
    .{ .name = "id", .type = .bigint },
    .{ .name = "running", .type = .bigint },
};

fn registerChain(db: *thindb.Database) !void {
    try db.registerTableUdf(.{
        .name = "gap_pad",
        .input_schemas = &.{&iga_cols},
        .output_schema = &iga_cols,
        .execution = .partitioned,
        .ordered_output = true,
        .process = gapPad,
    });
    try db.registerTableUdf(.{
        .name = "running2",
        .input_schemas = &.{&iga_cols},
        .output_schema = &running_out,
        .execution = .partitioned,
        .process = runningTotal,
    });
}

fn expectChainValues(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    var res = try run(allocator, db,
        \\WITH a AS (SELECT id, g, amt FROM TABLE(gap_pad((SELECT id, g, amt FROM t)) PARTITION BY g ORDER BY id))
        \\SELECT id, running FROM TABLE(running2((SELECT id, g, amt FROM a)) PARTITION BY g ORDER BY id)
    );
    defer res.deinit();
    var got: std.AutoHashMapUnmanaged(i64, i64) = .empty;
    defer got.deinit(allocator);
    while (try res.next()) |batch| {
        for (0..batch.row_count) |i| {
            try got.put(allocator, batch.values[0].data.bigint[i], batch.values[1].data.bigint[i]);
        }
    }
    // g=1: (1,10)(2,20)(3,30)(103,0) -> 10,30,60,60; g=2: (4,40)(5,50)(105,0)
    // -> 40,90,90.
    try std.testing.expectEqual(@as(usize, 7), got.count());
    try std.testing.expectEqual(@as(?i64, 10), got.get(1));
    try std.testing.expectEqual(@as(?i64, 30), got.get(2));
    try std.testing.expectEqual(@as(?i64, 60), got.get(3));
    try std.testing.expectEqual(@as(?i64, 60), got.get(103));
    try std.testing.expectEqual(@as(?i64, 40), got.get(4));
    try std.testing.expectEqual(@as(?i64, 90), got.get(5));
    try std.testing.expectEqual(@as(?i64, 90), got.get(105));
}

test "table UDF ordered_output: TVF-to-TVF chain rides the advertised order (serial)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(db);
    try registerChain(db);
    try expectChainValues(allocator, db);
}

test "table UDF ordered_output: TVF-to-TVF chain rides — parallel matches" {
    var gpa = std.heap.DebugAllocator(.{ .thread_safe = true }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();
    try seed(db);
    try registerChain(db);

    thindb.exec.table_fn.force_parallel_in_tests = true;
    defer thindb.exec.table_fn.force_parallel_in_tests = false;
    try expectChainValues(allocator, db);
}

test "table UDF ordered_output: a same-key window rides the TVF stage" {
    var gpa = std.heap.DebugAllocator(.{ .thread_safe = true }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // max_dop > 1 so the window rider machinery engages.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();
    try seed(db);
    try registerChain(db);

    var res = try run(allocator, db,
        \\WITH a AS (SELECT id, g, amt FROM TABLE(gap_pad((SELECT id, g, amt FROM t)) PARTITION BY g ORDER BY id))
        \\SELECT id, amt, LAG(amt) OVER (PARTITION BY g ORDER BY id) AS prev FROM a
    );
    defer res.deinit();
    var got: std.AutoHashMapUnmanaged(i64, [2]?i64) = .empty;
    defer got.deinit(allocator);
    while (try res.next()) |batch| {
        for (0..batch.row_count) |i| {
            try got.put(allocator, batch.values[0].data.bigint[i], .{
                if (batch.values[1].isValid(i)) batch.values[1].data.bigint[i] else null,
                if (batch.values[2].isValid(i)) batch.values[2].data.bigint[i] else null,
            });
        }
    }
    try std.testing.expectEqual(@as(usize, 7), got.count());
    const expected = [_]struct { id: i64, amt: ?i64, prev: ?i64 }{
        .{ .id = 1, .amt = 10, .prev = null },
        .{ .id = 2, .amt = 20, .prev = 10 },
        .{ .id = 3, .amt = 30, .prev = 20 },
        .{ .id = 103, .amt = 0, .prev = 30 },
        .{ .id = 4, .amt = 40, .prev = null },
        .{ .id = 5, .amt = 50, .prev = 40 },
        .{ .id = 105, .amt = 0, .prev = 50 },
    };
    for (expected) |e| {
        const row = got.get(e.id) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(e.amt, row[0]);
        try std.testing.expectEqual(e.prev, row[1]);
    }
}

test "table UDF ordered_output: a same-key window rides THROUGH a left join" {
    var gpa = std.heap.DebugAllocator(.{ .thread_safe = true }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();
    try seed(db);
    try seedRates(db);
    try registerChain(db);

    // Order rides the ordered TVF stage through the LEFT equi-join: the
    // join preserves left-side adjacency/order (probe = left, hash pinned,
    // serial probe under force_ordered), so the LAG keeps its values with
    // or without the ride. rates matches g=1 only — g=2 rows null-extend.
    var res = try run(allocator, db,
        \\WITH a AS (SELECT id, g, amt FROM TABLE(gap_pad((SELECT id, g, amt FROM t)) PARTITION BY g ORDER BY id)),
        \\b AS (SELECT a.id, a.g, a.amt, r.rate FROM a LEFT JOIN (SELECT g AS rg, rate FROM rates) r ON r.rg = a.g)
        \\SELECT id, rate, LAG(amt) OVER (PARTITION BY g ORDER BY id) AS prev FROM b
    );
    defer res.deinit();
    var got: std.AutoHashMapUnmanaged(i64, [2]?i64) = .empty;
    defer got.deinit(allocator);
    while (try res.next()) |batch| {
        for (0..batch.row_count) |i| {
            try got.put(allocator, batch.values[0].data.bigint[i], .{
                if (batch.values[1].isValid(i)) batch.values[1].data.bigint[i] else null,
                if (batch.values[2].isValid(i)) batch.values[2].data.bigint[i] else null,
            });
        }
    }
    try std.testing.expectEqual(@as(usize, 7), got.count());
    const expected = [_]struct { id: i64, rate: ?i64, prev: ?i64 }{
        .{ .id = 1, .rate = 2, .prev = null },
        .{ .id = 2, .rate = 2, .prev = 10 },
        .{ .id = 3, .rate = 2, .prev = 20 },
        .{ .id = 103, .rate = 2, .prev = 30 },
        .{ .id = 4, .rate = null, .prev = null },
        .{ .id = 5, .rate = null, .prev = 40 },
        .{ .id = 105, .rate = null, .prev = 50 },
    };
    for (expected) |e| {
        const row = got.get(e.id) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(e.rate, row[0]);
        try std.testing.expectEqual(e.prev, row[1]);
    }
}

test "table UDF ordered_output: ride through a DUPLICATING left join stays value-correct" {
    var gpa = std.heap.DebugAllocator(.{ .thread_safe = true }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();
    try seed(db);
    try registerChain(db);

    // Two matches for g=1: every g=1 row duplicates, and the duplicated
    // pair shares (id, amt) — LAG(amt) values stay deterministic under any
    // tie order, so ridden and sorted paths must agree exactly.
    const dup_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "dg", .type = .int },
            .{ .name = "tag", .type = .bigint },
        },
        .order_key = &.{"tag"},
        .unique = true,
    };
    const dk = [_][]const u8{"tag"};
    const dup = try db.table("dup", dup_schema, .{ .order_key = &dk, .unique = true, .row_group_size = 8 });
    try dup.insert(&.{
        .{ .dg = @as(i32, 1), .tag = @as(i64, 1) },
        .{ .dg = @as(i32, 1), .tag = @as(i64, 2) },
    });
    try dup.flush();

    var res = try run(allocator, db,
        \\WITH a AS (SELECT id, g, amt FROM TABLE(gap_pad((SELECT id, g, amt FROM t)) PARTITION BY g ORDER BY id)),
        \\b AS (SELECT a.id, a.g, a.amt FROM a LEFT JOIN dup d ON d.dg = a.g)
        \\SELECT id, LAG(amt) OVER (PARTITION BY g ORDER BY id) AS prev FROM b
    );
    defer res.deinit();
    var prevs: std.AutoHashMapUnmanaged(i64, std.ArrayListUnmanaged(i64)) = .empty;
    defer {
        var it = prevs.valueIterator();
        while (it.next()) |l| l.deinit(allocator);
        prevs.deinit(allocator);
    }
    var total: usize = 0;
    while (try res.next()) |batch| {
        for (0..batch.row_count) |i| {
            const id = batch.values[0].data.bigint[i];
            const prev: i64 = if (batch.values[1].isValid(i)) batch.values[1].data.bigint[i] else -1;
            const entry = try prevs.getOrPut(allocator, id);
            if (!entry.found_existing) entry.value_ptr.* = .empty;
            try entry.value_ptr.append(allocator, prev);
            total += 1;
        }
    }
    // g=1 rows duplicate (8 rows), g=2 unmatched null-extends (3 rows).
    try std.testing.expectEqual(@as(usize, 11), total);
    const cases = .{
        .{ .id = @as(i64, 1), .want = [_]i64{ -1, 10 } },
        .{ .id = @as(i64, 2), .want = [_]i64{ 10, 20 } },
        .{ .id = @as(i64, 3), .want = [_]i64{ 20, 30 } },
        .{ .id = @as(i64, 103), .want = [_]i64{ 0, 30 } },
        .{ .id = @as(i64, 4), .want = [_]i64{-1} },
        .{ .id = @as(i64, 5), .want = [_]i64{40} },
        .{ .id = @as(i64, 105), .want = [_]i64{50} },
    };
    inline for (cases) |c| {
        const list = prevs.get(c.id) orelse return error.TestExpectedEqual;
        const items = try allocator.dupe(i64, list.items);
        defer allocator.free(items);
        std.mem.sort(i64, items, {}, std.sort.asc(i64));
        const want = c.want;
        try std.testing.expectEqualSlices(i64, &want, items);
    }
}

// ---------------------------------------------------------------------------
// Multi-input generic-sort fallback: ORDER BY shapes packedSortFor declines
// (multi-column, string, string+date) must run — round 8.5.
// ---------------------------------------------------------------------------

/// Emits, per group, one row PER INPUT: (g, src, sig) where sig is the
/// comma-joined `v` values in the exact order the kernel received the
/// partition — a direct probe of within-partition ORDER BY handling.
fn orderProbe(
    ctx: *const thindb.udf.TvfContext,
    parts: []const thindb.udf.TvfPartition,
    out: *thindb.udf.TvfOutput,
) !void {
    const g: i32 = if (parts[0].keys[0]) |kv| kv.int else -1;
    for (parts, 0..) |p, src| {
        var sig: std.ArrayList(u8) = .empty;
        const vs = p.columns[3].data.bigint;
        for (0..p.row_count) |r| {
            if (r > 0) try sig.append(ctx.arena, ',');
            try sig.print(ctx.arena, "{d}", .{vs[r]});
        }
        try out.columns[0].data.int.append(out.allocator, g);
        try out.columns[1].data.int.append(out.allocator, @intCast(src));
        switch (out.columns[2].data) {
            .varchar, .string, .char, .json => |*s| try s.appendValue(out.allocator, sig.items),
            else => unreachable,
        }
    }
}

const probe_in = [_]thindb.Column{
    .{ .name = "g", .type = .int },
    .{ .name = "cur", .type = .string },
    .{ .name = "d", .type = .date },
    .{ .name = "v", .type = .bigint },
};
const probe_out = [_]thindb.Column{
    .{ .name = "g", .type = .int },
    .{ .name = "src", .type = .int },
    .{ .name = "sig", .type = .string },
};

fn seedOrderProbe(db: *thindb.Database) !void {
    const shape = thindb.TableSchema{
        .columns = &.{
            .{ .name = "g", .type = .int },
            .{ .name = "cur", .type = .{ .varchar = 8 } },
            .{ .name = "d", .type = .date },
            .{ .name = "v", .type = .bigint },
        },
        .order_key = &.{"v"},
        .unique = true,
    };
    const vk = [_][]const u8{"v"};
    const t_opts = thindb.TableOptions{ .order_key = &vk, .unique = true, .row_group_size = 8 };
    // Scan emits by order_key v — deliberately NOT any tested ORDER BY.
    const ta = try db.table("ta", shape, t_opts);
    try ta.insert(&.{
        .{ .g = @as(i32, 1), .cur = "a", .d = @as(i32, 10), .v = @as(i64, 13) },
        .{ .g = @as(i32, 1), .cur = "b", .d = @as(i32, 20), .v = @as(i64, 11) },
        .{ .g = @as(i32, 2), .cur = "c", .d = @as(i32, 10), .v = @as(i64, 14) },
        .{ .g = @as(i32, 1), .cur = "a", .d = @as(i32, 30), .v = @as(i64, 12) },
    });
    try ta.flush();
    // tb has NO g=2 rows — that group's input-1 partition arrives empty.
    const tb = try db.table("tb", shape, t_opts);
    try tb.insert(&.{
        .{ .g = @as(i32, 1), .cur = "c", .d = @as(i32, 10), .v = @as(i64, 22) },
        .{ .g = @as(i32, 1), .cur = "a", .d = @as(i32, 20), .v = @as(i64, 21) },
    });
    try tb.flush();
    try db.registerTableUdf(.{
        .name = "order_probe",
        .input_schemas = &.{ &probe_in, &probe_in },
        .output_schema = &probe_out,
        .execution = .partitioned,
        .process = orderProbe,
    });
}

const ProbeRow = struct { g: i32, src: i32, sig: []const u8 };

fn expectOrderProbe(allocator: std.mem.Allocator, db: *thindb.Database, order_by: []const u8, expected: []const ProbeRow) !void {
    const sql = try std.fmt.allocPrint(allocator,
        \\SELECT g, src, sig FROM TABLE(order_probe(
        \\  (SELECT g, cur, d, v FROM ta),
        \\  (SELECT g, cur, d, v FROM tb)
        \\) PARTITION BY g ORDER BY {s})
    , .{order_by});
    defer allocator.free(sql);
    var res = try run(allocator, db, sql);
    defer res.deinit();
    var rows: usize = 0;
    while (try res.next()) |batch| {
        for (0..batch.row_count) |i| {
            rows += 1;
            const g = batch.values[0].data.int[i];
            const src = batch.values[1].data.int[i];
            const sig = switch (batch.values[2].data) {
                .varchar, .string, .char, .json => |sv| sv.rowBytes(i),
                else => unreachable,
            };
            for (expected) |e| {
                if (e.g == g and e.src == src) {
                    try std.testing.expectEqualStrings(e.sig, sig);
                    break;
                }
            } else return error.TestExpectedEqual;
        }
    }
    try std.testing.expectEqual(expected.len, rows);
}

fn expectAllOrderShapes(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    // (a) Two-column int-family ORDER BY (packed sort declines on arity).
    try expectOrderProbe(allocator, db, "d, v", &.{
        .{ .g = 1, .src = 0, .sig = "13,11,12" },
        .{ .g = 1, .src = 1, .sig = "22,21" },
        .{ .g = 2, .src = 0, .sig = "14" },
        .{ .g = 2, .src = 1, .sig = "" },
    });
    // (b) String ORDER BY (declines on type); the cur="a" tie resolves by
    // arrival (scan emits by order_key v: 12 before 13).
    try expectOrderProbe(allocator, db, "cur", &.{
        .{ .g = 1, .src = 0, .sig = "12,13,11" },
        .{ .g = 1, .src = 1, .sig = "21,22" },
        .{ .g = 2, .src = 0, .sig = "14" },
        .{ .g = 2, .src = 1, .sig = "" },
    });
    // (c) String + date ORDER BY: the cur="a" tie now resolves by d.
    try expectOrderProbe(allocator, db, "cur, d", &.{
        .{ .g = 1, .src = 0, .sig = "13,12,11" },
        .{ .g = 1, .src = 1, .sig = "21,22" },
        .{ .g = 2, .src = 0, .sig = "14" },
        .{ .g = 2, .src = 1, .sig = "" },
    });
}

test "table UDF multi-input: generic-sort ORDER BY shapes (serial)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seedOrderProbe(db);
    try expectAllOrderShapes(allocator, db);
}

test "table UDF multi-input: generic-sort ORDER BY shapes (parallel)" {
    var gpa = std.heap.DebugAllocator(.{ .thread_safe = true }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();
    try seedOrderProbe(db);

    thindb.exec.table_fn.force_parallel_in_tests = true;
    defer thindb.exec.table_fn.force_parallel_in_tests = false;
    try expectAllOrderShapes(allocator, db);
}

// ---------------------------------------------------------------------------
// Broadcast inputs: the lookup input arrives WHOLE in every partition, its
// schema carries no call keys, and the kernel builds its probe map once per
// worker (worker_state / worker_arena).
// ---------------------------------------------------------------------------

const BcastState = struct { map: std.AutoHashMapUnmanaged(i32, i64) };

/// fxConvert semantics via a broadcast lookup: build g→rate once per
/// worker from the full rates input, probe with this partition's key.
fn fxConvertBcast(
    ctx: *const thindb.udf.TvfContext,
    parts: []const thindb.udf.TvfPartition,
    out: *thindb.udf.TvfOutput,
) !void {
    const main = &parts[0];
    const rates = &parts[1];
    const st: *BcastState = blk: {
        if (ctx.worker_state) |slot| {
            if (slot.*) |p| break :blk @ptrCast(@alignCast(p));
        }
        const wa = ctx.worker_arena orelse ctx.arena;
        const s = try wa.create(BcastState);
        s.* = .{ .map = .empty };
        for (0..rates.row_count) |i| {
            try s.map.put(wa, rates.columns[0].data.int[i], rates.columns[1].data.bigint[i]);
        }
        if (ctx.worker_state) |slot| slot.* = s;
        break :blk s;
    };
    const rate: ?i64 = if (main.keys[0]) |kv| st.map.get(kv.int) else null;
    const amts = main.columns[0].data.bigint;
    const val = out.columns[0];
    for (0..main.row_count) |i| {
        try val.data.bigint.append(out.allocator, if (rate) |r| amts[i] * r else 0);
        try val.appendValidBit(out.allocator, val.rowCount() - 1, rate != null);
    }
}

// No `g` column: a broadcast input needs no call keys in its schema.
const bcast_in1 = [_]thindb.Column{
    .{ .name = "gg", .type = .int },
    .{ .name = "rate", .type = .bigint },
};

fn registerFxBcast(db: *thindb.Database) !void {
    try db.registerTableUdf(.{
        .name = "fx_bcast",
        .input_schemas = &.{ &fx_in0, &bcast_in1 },
        .output_schema = &fx_out,
        .execution = .partitioned,
        .row_aligned = true,
        .broadcast_inputs = &.{1},
        .passthrough = &.{.{ .out_idx = 0, .in_idx = 1 }},
        .kernel_input_cols = 1,
        .process = fxConvertBcast,
    });
}

fn expectBcastValues(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    var res = try run(allocator, db,
        \\SELECT id, val FROM TABLE(fx_bcast(
        \\  (SELECT amt, id, g FROM t),
        \\  (SELECT g AS gg, rate FROM rates)
        \\) PARTITION BY g)
    );
    defer res.deinit();
    var vals: std.AutoHashMapUnmanaged(i64, ?i64) = .empty;
    defer vals.deinit(allocator);
    while (try res.next()) |batch| {
        for (0..batch.row_count) |i| {
            try vals.put(
                allocator,
                batch.values[0].data.bigint[i],
                if (batch.values[1].isValid(i)) batch.values[1].data.bigint[i] else null,
            );
        }
    }
    try std.testing.expectEqual(@as(usize, 5), vals.count());
    try std.testing.expectEqual(@as(??i64, @as(?i64, 20)), vals.get(1));
    try std.testing.expectEqual(@as(??i64, @as(?i64, 40)), vals.get(2));
    try std.testing.expectEqual(@as(??i64, @as(?i64, 60)), vals.get(3));
    try std.testing.expectEqual(@as(??i64, @as(?i64, null)), vals.get(4));
    try std.testing.expectEqual(@as(??i64, @as(?i64, null)), vals.get(5));
}

test "table UDF broadcast input: whole lookup per partition, no call keys in its schema (serial)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(db);
    try seedRates(db);
    try registerFxBcast(db);
    try expectBcastValues(allocator, db);
}

test "table UDF broadcast input: parallel matches serial (worker_state per worker)" {
    var gpa = std.heap.DebugAllocator(.{ .thread_safe = true }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();
    try seed(db);
    try seedRates(db);
    try registerFxBcast(db);

    thindb.exec.table_fn.force_parallel_in_tests = true;
    defer thindb.exec.table_fn.force_parallel_in_tests = false;
    try expectBcastValues(allocator, db);
}

test "table UDF broadcast input: validation rejects input 0 and out-of-range indices" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try std.testing.expectError(thindb.udf.Error.FunctionInvalidDefinition, db.registerTableUdf(.{
        .name = "bad_bcast0",
        .input_schemas = &.{ &fx_in0, &bcast_in1 },
        .output_schema = &fx_out,
        .row_aligned = true,
        .broadcast_inputs = &.{0},
        .kernel_input_cols = 1,
        .process = fxConvertBcast,
    }));
    try std.testing.expectError(thindb.udf.Error.FunctionInvalidDefinition, db.registerTableUdf(.{
        .name = "bad_bcast2",
        .input_schemas = &.{ &fx_in0, &bcast_in1 },
        .output_schema = &fx_out,
        .row_aligned = true,
        .broadcast_inputs = &.{2},
        .kernel_input_cols = 1,
        .process = fxConvertBcast,
    }));
}
