//! Window function benchmarks. Run via
//! `zig build bench -Doptimize=ReleaseFast`.
//!
//! Coverage: every window function in Tier 1 + Tier 2 at 1M rows,
//! plus shape variations:
//!   - Single function alone (baseline per-function cost)
//!   - Two functions sharing the same spec (exercises the
//!     parser's spec-dedup + the operator's single-sort path)
//!   - Two functions with different specs (worst case — two sorts)
//!   - Partition cardinality sweep (1, 100, 10K, 1M partitions)
//!   - Frame-shape sweep (whole-partition, running, sliding-N)
//!
//! Each bench reuses the same `bench_table` data so the sort + scan
//! costs are comparable across rows.

const std = @import("std");
const thindb = @import("thindb");
const ir = thindb.ir;
const SortSpec = thindb.SortSpec;

const common = @import("common.zig");
const Allocator = common.Allocator;
const Io = common.Io;
const elapsedNs = common.elapsedNs;
const freshDir = common.freshDir;
const report = common.report;

const bench_rows: usize = 1_000_000;

/// Bench-specific schema with two useful partition keys (`grp_lo` =
/// few partitions; `grp_hi` = many partitions) and an INT value
/// column. Keeps rows narrow so per-row overhead is dominated by
/// window logic, not I/O.
pub const Row = struct {
    id: i64,
    grp_lo: i32, // 100 distinct values
    grp_hi: i32, // 10_000 distinct values
    qty: i64,
};

const bench_schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "grp_lo", .type = .int },
        .{ .name = "grp_hi", .type = .int },
        .{ .name = "qty", .type = .bigint },
    },
    .order_key = &.{"id"},
    .unique = false,
};

const order_key = [_][]const u8{"id"};
const bench_options = thindb.TableOptions{
    .order_key = &order_key,
    .unique = false,
    .row_group_size = 65_536,
};

fn buildRows(allocator: Allocator, n: usize) ![]Row {
    const rows = try allocator.alloc(Row, n);
    for (rows, 0..) |*r, i| {
        r.* = .{
            .id = @intCast(i),
            .grp_lo = @intCast(i % 100),
            .grp_hi = @intCast(i % 10_000),
            .qty = @intCast(i % 1000),
        };
    }
    return rows;
}

const Setup = struct {
    dir: Io.Dir,
    db: *thindb.Database,
    table: *thindb.Table,
};

fn setup(allocator: Allocator, io: Io, label: []const u8, n_rows: usize) !Setup {
    var path_buf: [128]u8 = undefined;
    const dir_path = try std.fmt.bufPrint(&path_buf, ".bench-data/window_{s}", .{label});
    var dir = try freshDir(io, dir_path);
    errdefer dir.close(io);

    var db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    const t = try db.table("t", bench_schema, bench_options);
    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);
    try t.insert(rows);
    try t.flush();
    return .{ .dir = dir, .db = db, .table = t };
}

fn teardown(s: *Setup, io: Io) void {
    s.db.close();
    s.dir.close(io);
}

/// Run a window query end-to-end (scan → window → drain) and return
/// the elapsed time. Drains every batch so the entire row range is
/// materialized.
fn runWindow(
    allocator: Allocator,
    io: Io,
    t: *thindb.Table,
    specs: []const ir.WindowSpec,
    calls: []const ir.WindowCall,
) !u64 {
    const t0 = Io.Clock.awake.now(io);
    var base = try thindb.scan(allocator, t);
    var q = try base.window(specs, calls);
    defer q.deinit();
    var checksum: i64 = 0;
    while (try q.next()) |b| {
        // Touch the output to prevent dead-code elimination of the
        // materialization step.
        if (b.row_count > 0) {
            switch (b.values[b.values.len - 1].data) {
                .bigint => |arr| checksum +%= arr[0],
                .int => |arr| checksum +%= arr[0],
                .double => {},
                else => {},
            }
        }
    }
    std.mem.doNotOptimizeAway(&checksum);
    return elapsedNs(io, t0);
}

// ---------------------------------------------------------------------------
// Spec / call builders — small helpers that keep the bench bodies readable.
// ---------------------------------------------------------------------------

fn specSpec(allocator: Allocator, partition_cols: []const []const u8, order_cols: []const []const u8, frame: ir.Frame) !ir.WindowSpec {
    const part = try allocator.alloc([]const u8, partition_cols.len);
    for (partition_cols, part) |c, *d| d.* = c;
    const ord = try allocator.alloc(SortSpec, order_cols.len);
    for (order_cols, ord) |c, *d| d.* = .{ .col = c, .desc = false };
    return .{ .partition_by = part, .order_by = ord, .frame = frame };
}

fn callNullary(spec_idx: u32, func: ir.WindowFunc, name: []const u8) ir.WindowCall {
    return .{
        .spec_idx = spec_idx,
        .func = func,
        .args = &.{},
        .ignore_nulls = false,
        .output_name = name,
    };
}

/// Builds a `WindowCall` whose single arg is a column reference.
/// Args slice is arena-allocated so it lives past the call boundary
/// (a stack-allocated `&[_]` would dangle as soon as we return).
fn callOneArg(
    arena: Allocator,
    spec_idx: u32,
    func: ir.WindowFunc,
    col: []const u8,
    name: []const u8,
) !ir.WindowCall {
    const args = try arena.alloc(ir.Expr, 1);
    args[0] = .{ .col_ref = col };
    return .{
        .spec_idx = spec_idx,
        .func = func,
        .args = args,
        .ignore_nulls = false,
        .output_name = name,
    };
}

const default_frame_with_order = ir.Frame.default_with_order;
const default_frame_no_order = ir.Frame.default_no_order;
const rows_unbounded_to_current = ir.Frame{
    .kind = .rows,
    .start = .unbounded_preceding,
    .end = .current_row,
};

// ---------------------------------------------------------------------------
// Bench bodies — one per shape we want to compare.
// ---------------------------------------------------------------------------

pub fn benchRowNumber(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "row_number", bench_rows);
    defer teardown(&s, io);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{"id"}, default_frame_with_order),
    };
    const calls = &[_]ir.WindowCall{callNullary(0, .row_number, "rn")};

    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("row_number / 100 partitions", bench_rows, ns, null);
}

pub fn benchRank(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "rank", bench_rows);
    defer teardown(&s, io);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{"qty"}, default_frame_with_order),
    };
    const calls = &[_]ir.WindowCall{callNullary(0, .rank, "rk")};
    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("rank / 100 partitions, ties", bench_rows, ns, null);
}

pub fn benchDenseRank(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "dense_rank", bench_rows);
    defer teardown(&s, io);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{"qty"}, default_frame_with_order),
    };
    const calls = &[_]ir.WindowCall{callNullary(0, .dense_rank, "drk")};
    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("dense_rank / 100 partitions, ties", bench_rows, ns, null);
}

pub fn benchLag(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "lag", bench_rows);
    defer teardown(&s, io);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{"id"}, default_frame_with_order),
    };
    const calls = &[_]ir.WindowCall{try callOneArg(a, 0, .lag, "qty", "prev")};
    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("lag(qty) / 100 partitions", bench_rows, ns, null);
}

pub fn benchFirstValue(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "first_value", bench_rows);
    defer teardown(&s, io);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{"id"}, default_frame_with_order),
    };
    const calls = &[_]ir.WindowCall{try callOneArg(a, 0, .first_value, "qty", "first_q")};
    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("first_value(qty) / 100 partitions", bench_rows, ns, null);
}

pub fn benchSumRunning(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "sum_running", bench_rows);
    defer teardown(&s, io);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // Default frame with ORDER BY = RANGE UNBOUNDED PRECEDING TO CURRENT
    // → hits the prefix fast path.
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{"id"}, default_frame_with_order),
    };
    const calls = &[_]ir.WindowCall{try callOneArg(a, 0, .sum, "qty", "rs")};
    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("sum(qty) running / 100 partitions", bench_rows, ns, null);
}

pub fn benchSumWholePartition(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "sum_whole", bench_rows);
    defer teardown(&s, io);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // No ORDER BY → default frame is UNBOUNDED both sides → whole-partition fast path.
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{}, default_frame_no_order),
    };
    const calls = &[_]ir.WindowCall{try callOneArg(a, 0, .sum, "qty", "total")};
    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("sum(qty) whole partition / 100", bench_rows, ns, null);
}

pub fn benchSumSliding10(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "sum_sliding", bench_rows);
    defer teardown(&s, io);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // ROWS BETWEEN 10 PRECEDING AND CURRENT ROW → falls back to per-row
    // O(N×W) path. Comparison against the running fast path measures
    // the cost of the naive frame walk.
    const sliding_frame = ir.Frame{
        .kind = .rows,
        .start = .{ .preceding = 10 },
        .end = .current_row,
    };
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{"id"}, sliding_frame),
    };
    const calls = &[_]ir.WindowCall{try callOneArg(a, 0, .sum, "qty", "trailing10")};
    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("sum(qty) ROWS 10 PRECEDING / 100", bench_rows, ns, null);
}

pub fn benchAvgRunning(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "avg_running", bench_rows);
    defer teardown(&s, io);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{"id"}, default_frame_with_order),
    };
    const calls = &[_]ir.WindowCall{try callOneArg(a, 0, .avg, "qty", "ra")};
    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("avg(qty) running / 100 partitions", bench_rows, ns, null);
}

pub fn benchMinMax(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "min_max", bench_rows);
    defer teardown(&s, io);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // Two functions sharing the same spec — verifies shared-sort path.
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{"id"}, default_frame_with_order),
    };
    const calls = &[_]ir.WindowCall{
        try callOneArg(a, 0, .min, "qty", "rmin"),
        try callOneArg(a, 0, .max, "qty", "rmax"),
    };
    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("min+max shared spec / 100", bench_rows, ns, null);
}

pub fn benchNtile(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "ntile", bench_rows);
    defer teardown(&s, io);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{"id"}, default_frame_with_order),
    };
    const ntile_args = &[_]ir.Expr{.{ .lit = .{ .int = 10 } }};
    const calls = &[_]ir.WindowCall{.{
        .spec_idx = 0,
        .func = .ntile,
        .args = ntile_args,
        .ignore_nulls = false,
        .output_name = "bucket",
    }};
    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("ntile(10) / 100 partitions", bench_rows, ns, null);
}

pub fn benchPercentRank(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "percent_rank", bench_rows);
    defer teardown(&s, io);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{"qty"}, default_frame_with_order),
    };
    const calls = &[_]ir.WindowCall{callNullary(0, .percent_rank, "pr")};
    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("percent_rank / 100 partitions", bench_rows, ns, null);
}

pub fn benchCumeDist(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "cume_dist", bench_rows);
    defer teardown(&s, io);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{"qty"}, default_frame_with_order),
    };
    const calls = &[_]ir.WindowCall{callNullary(0, .cume_dist, "cd")};
    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("cume_dist / 100 partitions", bench_rows, ns, null);
}

// ---------- Spec sharing comparisons ----------

pub fn benchTwoSameSpec(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "two_same_spec", bench_rows);
    defer teardown(&s, io);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // ONE sort serves BOTH calls.
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{"id"}, default_frame_with_order),
    };
    const calls = &[_]ir.WindowCall{
        callNullary(0, .row_number, "rn"),
        try callOneArg(a, 0, .sum, "qty", "rs"),
    };
    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("2 calls / same spec (1 sort)", bench_rows, ns, null);
}

pub fn benchTwoDifferentSpecs(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "two_diff_specs", bench_rows);
    defer teardown(&s, io);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // TWO sorts — different partition keys.
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{"id"}, default_frame_with_order),
        try specSpec(a, &.{"grp_hi"}, &.{"id"}, default_frame_with_order),
    };
    const calls = &[_]ir.WindowCall{
        callNullary(0, .row_number, "rn_lo"),
        callNullary(1, .row_number, "rn_hi"),
    };
    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("2 calls / different specs (2 sorts)", bench_rows, ns, null);
}

pub fn benchFourSameSpec(allocator: Allocator, io: Io) !void {
    var s = try setup(allocator, io, "four_same_spec", bench_rows);
    defer teardown(&s, io);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const specs = &[_]ir.WindowSpec{
        try specSpec(a, &.{"grp_lo"}, &.{"id"}, default_frame_with_order),
    };
    const calls = &[_]ir.WindowCall{
        callNullary(0, .row_number, "rn"),
        callNullary(0, .rank, "rk"),
        try callOneArg(a, 0, .sum, "qty", "rs"),
        try callOneArg(a, 0, .avg, "qty", "ra"),
    };
    const ns = try runWindow(allocator, io, s.table, specs, calls);
    try report("4 calls / same spec (1 sort)", bench_rows, ns, null);
}

// ---------- Partition cardinality sweep ----------

pub fn benchPartitionSweep(allocator: Allocator, io: Io) !void {
    // One sort per setup, function is ROW_NUMBER (cheapest), partition
    // cardinality varies. Highlights the partition-boundary scan cost
    // independent of function workload.
    const Variant = struct { label: []const u8, col: []const u8 };
    const variants = [_]Variant{
        .{ .label = "partition=1 (no PARTITION)", .col = "" },
        .{ .label = "partition=100", .col = "grp_lo" },
        .{ .label = "partition=10000", .col = "grp_hi" },
    };

    for (variants) |v| {
        var s = try setup(allocator, io, v.label, bench_rows);
        defer teardown(&s, io);

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const part_cols: []const []const u8 = if (v.col.len == 0) &.{} else &.{v.col};
        const specs = &[_]ir.WindowSpec{
            try specSpec(a, part_cols, &.{"id"}, default_frame_with_order),
        };
        const calls = &[_]ir.WindowCall{callNullary(0, .row_number, "rn")};
        const ns = try runWindow(allocator, io, s.table, specs, calls);
        try report(v.label, bench_rows, ns, null);
    }
}

// ---------- Aggregator: called from bench/main.zig ----------

pub fn runAll(allocator: Allocator, io: Io) !void {
    std.debug.print("\nWindow functions ({d} rows)\n", .{bench_rows});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});

    try benchRowNumber(allocator, io);
    try benchRank(allocator, io);
    try benchDenseRank(allocator, io);
    try benchLag(allocator, io);
    try benchFirstValue(allocator, io);
    try benchSumRunning(allocator, io);
    try benchSumWholePartition(allocator, io);
    try benchSumSliding10(allocator, io);
    try benchAvgRunning(allocator, io);
    try benchMinMax(allocator, io);
    try benchNtile(allocator, io);
    try benchPercentRank(allocator, io);
    try benchCumeDist(allocator, io);

    std.debug.print("\nWindow spec sharing\n", .{});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});
    try benchTwoSameSpec(allocator, io);
    try benchTwoDifferentSpecs(allocator, io);
    try benchFourSameSpec(allocator, io);

    std.debug.print("\nWindow partition cardinality\n", .{});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});
    try benchPartitionSweep(allocator, io);
}
