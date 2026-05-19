//! Materialized-CTE benchmark.
//!
//! Goal: quantify the cost of redraining a shared CTE subtree vs
//! materializing it once into a shared buffer (Option A — per-compile
//! `CompileCtx` cache keyed by `*ir.Op`).
//!
//! Each scenario runs the SAME query under three configurations:
//!   1. `NOT MATERIALIZED` hint → upstream subtree drains twice (one
//!      drain per JOIN side).
//!   2. `MATERIALIZED`     hint → upstream drains once; both JOIN sides
//!      read from a shared `MaterializedBuffer`.
//!   3. (no hint)          → auto-detect refcount ≥ 2 wraps it just
//!      like (2). Run for parity confirmation.
//!
//! Setup (insert + flush) is excluded from timing. The measurement is:
//! parse → compile → drain every batch → report.

const std = @import("std");
const thindb = @import("thindb");
const common = @import("common.zig");

const Allocator = common.Allocator;
const Io = common.Io;
const elapsedNs = common.elapsedNs;
const freshDir = common.freshDir;

// ----------------------------------------------------------------------------
// Schema
// ----------------------------------------------------------------------------

const mat_schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "qty", .type = .int },
        .{ .name = "tag", .type = .{ .varchar = 16 } },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const mat_ok = [_][]const u8{"id"};
const mat_opts = thindb.TableOptions{
    .order_key = &mat_ok,
    .unique = true,
    .row_group_size = 65_536,
};

const tag_pool = [_][]const u8{ "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta" };

const Row = struct {
    id: i64,
    qty: i32,
    tag: []const u8,
};

fn buildRows(allocator: Allocator, n: usize) ![]Row {
    const rows = try allocator.alloc(Row, n);
    var rng = std.Random.DefaultPrng.init(0xC0FFEE);
    const r = rng.random();
    for (rows, 0..) |*row, i| {
        row.* = .{
            .id = @intCast(i),
            // Wide-cardinality qty (random 0..9999) so ORDER BY qty
            // actually has work to do — not a near-sorted fast path.
            .qty = @intCast(r.uintLessThan(u32, 10_000)),
            .tag = tag_pool[i % tag_pool.len],
        };
    }
    return rows;
}

// ----------------------------------------------------------------------------
// Run helper — parse → compile → drain → time.
// ----------------------------------------------------------------------------

const RunStats = struct {
    elapsed_ns: u64,
    out_rows: u64,
    /// Number of `.materialize` IR nodes the compile path turned into
    /// a shared buffer. 0 means upstream was redrained per reference.
    materialized_buffers: u32,
};

fn runQuery(
    allocator: Allocator,
    io: Io,
    db: *thindb.Database,
    sql: []const u8,
) !RunStats {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), sql);

    const t0 = Io.Clock.awake.now(io);
    var cq = try thindb.net.compile(allocator, db, root);
    defer cq.deinit();

    var out_rows: u64 = 0;
    while (try cq.next()) |b| out_rows += b.row_count;
    const elapsed = elapsedNs(io, t0);

    return .{
        .elapsed_ns = elapsed,
        .out_rows = out_rows,
        .materialized_buffers = cq.ctx.materialized.count(),
    };
}

fn reportRow(label: []const u8, s: RunStats) void {
    const ms = @as(f64, @floatFromInt(s.elapsed_ns)) / 1e6;
    std.debug.print(
        "  {s:<44}  {d:>9.2} ms   out={d:>10}   buffers={d}\n",
        .{ label, ms, s.out_rows, s.materialized_buffers },
    );
}

fn reportSpeedup(baseline_ns: u64, optimized_ns: u64) void {
    const a = @as(f64, @floatFromInt(baseline_ns));
    const b = @as(f64, @floatFromInt(optimized_ns));
    const speedup = a / b;
    const saved_ms = (a - b) / 1e6;
    std.debug.print(
        "  -> speedup {d:>5.2}x   saved {d:>7.2} ms\n",
        .{ speedup, saved_ms },
    );
}

// ----------------------------------------------------------------------------
// Scenarios
// ----------------------------------------------------------------------------

/// Heavy upstream: filter + sort over 1M rows by a non-order-key column.
/// Used as both sides of a self-join on `id`. Without materialization
/// the entire filter+sort runs twice (once per join side); with it,
/// once.
fn benchSortedCteSelfJoin(
    allocator: Allocator,
    io: Io,
    n_rows: usize,
) !void {
    var dir = try freshDir(io, ".bench-data/materialize_sort_join");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    const t = try db.table("t", mat_schema, mat_opts);
    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);
    try t.insert(rows);
    try t.flush();

    std.debug.print(
        "\nMaterialized CTE — filter + sort + self-join  ({d}-row table)\n",
        .{n_rows},
    );
    std.debug.print(
        "  CTE: SELECT id FROM t WHERE qty >= 5000 ORDER BY qty DESC\n",
        .{},
    );
    std.debug.print(
        "  Outer: SELECT count(*) FROM cte JOIN cte AS o ON cte.id = o.id\n",
        .{},
    );
    std.debug.print(
        "--------------------------------------------------------------------------------\n",
        .{},
    );

    const not_mat =
        \\WITH cte AS NOT MATERIALIZED (SELECT id FROM t WHERE qty >= 5000 ORDER BY qty DESC)
        \\SELECT count(*) FROM cte JOIN cte AS o ON cte.id = o.id
    ;
    const force_mat =
        \\WITH cte AS MATERIALIZED (SELECT id FROM t WHERE qty >= 5000 ORDER BY qty DESC)
        \\SELECT count(*) FROM cte JOIN cte AS o ON cte.id = o.id
    ;
    const auto_mat =
        \\WITH cte AS (SELECT id FROM t WHERE qty >= 5000 ORDER BY qty DESC)
        \\SELECT count(*) FROM cte JOIN cte AS o ON cte.id = o.id
    ;

    // Warm-up: first run pays page-fault + cache-miss costs that swamp
    // the differences we're trying to measure. One throwaway run lets
    // the row-group cache settle.
    _ = try runQuery(allocator, io, db, not_mat);

    const r_no = try runQuery(allocator, io, db, not_mat);
    const r_force = try runQuery(allocator, io, db, force_mat);
    const r_auto = try runQuery(allocator, io, db, auto_mat);

    reportRow("NOT MATERIALIZED (upstream drained 2x)", r_no);
    reportRow("MATERIALIZED     (upstream drained 1x)", r_force);
    reportRow("(auto-detect, refcount=2)", r_auto);
    reportSpeedup(r_no.elapsed_ns, r_force.elapsed_ns);
}

/// Lighter upstream — just a scan + filter, no sort. Self-join on id.
/// Establishes the floor: when upstream is cheap, the materialize buffer
/// itself adds copy overhead; gain may be small or negative.
fn benchFilterCteSelfJoin(
    allocator: Allocator,
    io: Io,
    n_rows: usize,
) !void {
    var dir = try freshDir(io, ".bench-data/materialize_filter_join");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    const t = try db.table("t", mat_schema, mat_opts);
    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);
    try t.insert(rows);
    try t.flush();

    std.debug.print(
        "\nMaterialized CTE — filter-only self-join  ({d}-row table)\n",
        .{n_rows},
    );
    std.debug.print(
        "  CTE: SELECT id FROM t WHERE qty >= 5000\n",
        .{},
    );
    std.debug.print(
        "  Outer: SELECT count(*) FROM cte JOIN cte AS o ON cte.id = o.id\n",
        .{},
    );
    std.debug.print(
        "--------------------------------------------------------------------------------\n",
        .{},
    );

    const not_mat =
        \\WITH cte AS NOT MATERIALIZED (SELECT id FROM t WHERE qty >= 5000)
        \\SELECT count(*) FROM cte JOIN cte AS o ON cte.id = o.id
    ;
    const force_mat =
        \\WITH cte AS MATERIALIZED (SELECT id FROM t WHERE qty >= 5000)
        \\SELECT count(*) FROM cte JOIN cte AS o ON cte.id = o.id
    ;

    _ = try runQuery(allocator, io, db, not_mat);

    const r_no = try runQuery(allocator, io, db, not_mat);
    const r_force = try runQuery(allocator, io, db, force_mat);

    reportRow("NOT MATERIALIZED (upstream drained 2x)", r_no);
    reportRow("MATERIALIZED     (upstream drained 1x)", r_force);
    reportSpeedup(r_no.elapsed_ns, r_force.elapsed_ns);
}

// ----------------------------------------------------------------------------
// 3-source star pipeline: events ⊕ dim_a, dim_b
//
// Real-world shape: one big "fact" table gets aggregated by group key,
// and two dimension tables each look up that aggregate before being
// joined together on a foreign key. The aggregate is referenced twice
// (once per dim join), so auto-detect refcount ≥ 2 wraps it in a
// Materialize node — both lookups read from the same drained buffer
// instead of redoing the full scan + GROUP BY.
// ----------------------------------------------------------------------------

const events_schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "eid", .type = .bigint },
        .{ .name = "group_k", .type = .int },
        .{ .name = "val", .type = .int },
    },
    .order_key = &.{"eid"},
    .unique = true,
};
const events_ok = [_][]const u8{"eid"};
const events_opts = thindb.TableOptions{
    .order_key = &events_ok,
    .unique = true,
    .row_group_size = 65_536,
};

const dim_a_schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "a_id", .type = .bigint },
        .{ .name = "a_group", .type = .int },
        .{ .name = "a_name", .type = .{ .varchar = 16 } },
    },
    .order_key = &.{"a_id"},
    .unique = true,
};
const dim_a_ok = [_][]const u8{"a_id"};
const dim_a_opts = thindb.TableOptions{
    .order_key = &dim_a_ok,
    .unique = true,
    .row_group_size = 65_536,
};

const dim_b_schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "b_id", .type = .bigint },
        .{ .name = "b_group", .type = .int },
        .{ .name = "b_score", .type = .int },
    },
    .order_key = &.{"b_id"},
    .unique = true,
};
const dim_b_ok = [_][]const u8{"b_id"};
const dim_b_opts = thindb.TableOptions{
    .order_key = &dim_b_ok,
    .unique = true,
    .row_group_size = 65_536,
};

const EventRow = struct { eid: i64, group_k: i32, val: i32 };
const DimARow = struct { a_id: i64, a_group: i32, a_name: []const u8 };
const DimBRow = struct { b_id: i64, b_group: i32, b_score: i32 };

const num_groups: usize = 200;

fn buildEvents(allocator: Allocator, n: usize) ![]EventRow {
    const rows = try allocator.alloc(EventRow, n);
    for (rows, 0..) |*r, i| {
        r.* = .{
            .eid = @intCast(i),
            .group_k = @intCast(i % num_groups),
            .val = @intCast(i % 1000),
        };
    }
    return rows;
}

fn buildDimA(allocator: Allocator, n: usize) ![]DimARow {
    const rows = try allocator.alloc(DimARow, n);
    for (rows, 0..) |*r, i| {
        r.* = .{
            .a_id = @intCast(i),
            .a_group = @intCast(i % num_groups),
            .a_name = tag_pool[i % tag_pool.len],
        };
    }
    return rows;
}

fn buildDimB(allocator: Allocator, n: usize) ![]DimBRow {
    const rows = try allocator.alloc(DimBRow, n);
    for (rows, 0..) |*r, i| {
        r.* = .{
            .b_id = @intCast(i),
            .b_group = @intCast(i % num_groups),
            .b_score = @intCast(i % 10_000),
        };
    }
    return rows;
}

fn benchStarPipeline(
    allocator: Allocator,
    io: Io,
    n_events: usize,
    n_dim: usize,
) !void {
    var dir = try freshDir(io, ".bench-data/materialize_star");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();

    const events = try db.table("events", events_schema, events_opts);
    const dim_a = try db.table("dim_a", dim_a_schema, dim_a_opts);
    const dim_b = try db.table("dim_b", dim_b_schema, dim_b_opts);

    const erows = try buildEvents(allocator, n_events);
    defer allocator.free(erows);
    const arows = try buildDimA(allocator, n_dim);
    defer allocator.free(arows);
    const brows = try buildDimB(allocator, n_dim);
    defer allocator.free(brows);

    try events.insert(erows);
    try events.flush();
    try dim_a.insert(arows);
    try dim_a.flush();
    try dim_b.insert(brows);
    try dim_b.flush();

    std.debug.print(
        "\nMaterialized CTE — 3-source star pipeline  (events={d}, dim_a=dim_b={d})\n",
        .{ n_events, n_dim },
    );
    std.debug.print(
        "  agg     = GROUP BY group_k from events  (≈{d} groups)\n",
        .{num_groups},
    );
    std.debug.print(
        "  with_a  = dim_a JOIN agg  ON a_group = group_k\n",
        .{},
    );
    std.debug.print(
        "  with_b  = dim_b JOIN agg  ON b_group = group_k\n",
        .{},
    );
    std.debug.print(
        "  final   = with_a JOIN with_b ON a_id = b_id\n",
        .{},
    );
    std.debug.print(
        "  agg is referenced twice → auto-materialize should kick in.\n",
        .{},
    );
    std.debug.print(
        "--------------------------------------------------------------------------------\n",
        .{},
    );

    const not_mat =
        \\WITH
        \\  agg AS NOT MATERIALIZED (SELECT group_k, sum(val) AS total FROM events GROUP BY group_k),
        \\  with_a AS (SELECT a_id, a_name FROM dim_a JOIN agg ON dim_a.a_group = agg.group_k),
        \\  with_b AS (SELECT b_id, b_score FROM dim_b JOIN agg ON dim_b.b_group = agg.group_k)
        \\SELECT count(*) FROM with_a JOIN with_b ON with_a.a_id = with_b.b_id
    ;
    const force_mat =
        \\WITH
        \\  agg AS MATERIALIZED (SELECT group_k, sum(val) AS total FROM events GROUP BY group_k),
        \\  with_a AS (SELECT a_id, a_name FROM dim_a JOIN agg ON dim_a.a_group = agg.group_k),
        \\  with_b AS (SELECT b_id, b_score FROM dim_b JOIN agg ON dim_b.b_group = agg.group_k)
        \\SELECT count(*) FROM with_a JOIN with_b ON with_a.a_id = with_b.b_id
    ;
    const auto_mat =
        \\WITH
        \\  agg AS (SELECT group_k, sum(val) AS total FROM events GROUP BY group_k),
        \\  with_a AS (SELECT a_id, a_name FROM dim_a JOIN agg ON dim_a.a_group = agg.group_k),
        \\  with_b AS (SELECT b_id, b_score FROM dim_b JOIN agg ON dim_b.b_group = agg.group_k)
        \\SELECT count(*) FROM with_a JOIN with_b ON with_a.a_id = with_b.b_id
    ;

    _ = try runQuery(allocator, io, db, not_mat);

    const r_no = try runQuery(allocator, io, db, not_mat);
    const r_force = try runQuery(allocator, io, db, force_mat);
    const r_auto = try runQuery(allocator, io, db, auto_mat);

    reportRow("NOT MATERIALIZED (agg drained 2x)", r_no);
    reportRow("MATERIALIZED     (agg drained 1x)", r_force);
    reportRow("(auto-detect, refcount=2)", r_auto);
    reportSpeedup(r_no.elapsed_ns, r_force.elapsed_ns);
}

// ----------------------------------------------------------------------------
// Entry point
// ----------------------------------------------------------------------------

pub fn runAll(allocator: Allocator, io: Io) !void {
    try benchSortedCteSelfJoin(allocator, io, 1_000_000);
    try benchFilterCteSelfJoin(allocator, io, 1_000_000);
    try benchStarPipeline(allocator, io, 1_000_000, 50_000);
}
