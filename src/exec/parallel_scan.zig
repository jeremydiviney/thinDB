//! Parallel scan orchestrator — intra-query parallelism for the scan/filter/
//! projection leaf (parallel-scan Phase 1).
//!
//! Reuses the serial `Scan` wholesale: it partitions the flat row-group list
//! into `dop` contiguous spans (see `Scan.setRange`) and runs one range-Scan
//! per worker. All workers share ONE captured snapshot (`Scan.captureSnapshot`)
//! so a concurrent flush can't make them disagree on segments-vs-memtable, and
//! the orchestrator holds a single shared `ddl_lock` covering them all. The
//! per-table block cache is already thread-safe (pinning LRU); `Scan.next()`
//! never touches the (non-thread-safe) accountant; each worker has isolated
//! scratch and allocates from the table's thread-safe allocator — so concurrent
//! `worker.next()` calls are safe with no further locking.
//!
//! Two execution strategies, chosen lazily on the first `next()` once filter
//! fusion (which happens after `create`) has settled:
//!   * `materialize` (a WHERE predicate was fused in): spawn workers once —
//!     worker 0 inline, the rest on their own threads — each draining its WHOLE
//!     slice to completion and deep-copying its survivors into owned
//!     ColumnStores. We then emit the per-worker buffers in slice order (concat).
//!     One spawn+join for the whole scan; bounded by the filter's survivor count
//!     (charged to the query budget after the join).
//!   * `round` (no fused filter ⇒ survivors would be the whole table): the
//!     streaming fork-join path — each `next()` runs one `worker.next()` per
//!     live worker concurrently and emits in worker order, in-flight memory
//!     bounded to `dop` batches.
//! Both emit in worker/slice order, so results are reproducible for a given
//! `dop` (DOP=1 is the canonical serial result). Per-query parallelism is capped
//! by `max_dop`; the process-global CoreScheduler (util/core_scheduler.zig) then
//! throttles globally — each worker leases (and pins to) a physical core for its
//! run and blocks when the machine is saturated, so concurrent queries share
//! cores instead of oversubscribing, and the lease bucket doubles as admission
//! control. Pinning recovers the ~40% scaling the OS scheduler loses by parking
//! two of our threads on one core's SMT siblings while another core idles.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const buffer_pool = @import("../util/buffer_pool.zig");
const types = @import("../types.zig");
const Column = types.Column;

const api = @import("../api/api.zig");
const Table = api.Table;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Scan = @import("scan.zig").Scan;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;
const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;
const transform = @import("../engine/transform.zig");
const Derived = @import("compute.zig").Derived;
const core_scheduler = @import("../util/core_scheduler.zig");
const Expr = @import("expr.zig").Expr;
const ChunkRangeScan = @import("mat_stage.zig").ChunkRangeScan;
const Stage = @import("mat_stage.zig").Stage;

/// A (segment, row_group) coordinate over the flat row-group list.
const Coord = struct { seg: usize, rg: usize };

/// Materialize-mode chunking: cut the row groups into `dop * CHUNK_FACTOR`
/// byte-balanced chunks (not one per thread) and let `dop` threads steal them
/// from a shared cursor. Finer chunks + dynamic claiming smooth out work skew —
/// a thread that draws light chunks grabs more, instead of one fat slice gating
/// the join. Survivor count (the real cost for filter+regex) isn't predictable
/// from bytes, so static per-thread slices leave imbalance that stealing fixes.
const CHUNK_FACTOR: usize = 4;

/// Selective-query right-sizing: one worker thread per this many surviving
/// (post-zone-map) row groups, so a point lookup that touches 13 of 958 row
/// groups doesn't pay 12 thread spawns. 2 keeps per-thread wall at roughly two
/// block decodes — small enough that the clamp never gates a real workload.
const RGS_PER_THREAD: usize = 2;

/// Execution strategy, chosen lazily on the first `next()` (fusion happens
/// after `create`). `materialize` is the fused-filter path: each worker drains
/// its whole slice and deep-copies survivors, then we concat. `round` is the
/// streaming fork-join path used when no filter was fused (so survivors would
/// be the whole table — can't materialize).
const Mode = enum { unset, round, materialize };

/// One worker's fully-drained, deep-copied survivor set (materialize mode).
/// `columns` are owned and allocated from the table's thread-safe allocator
/// (workers run concurrently); freed in `deinit`.
const WorkerBuf = struct {
    columns: []ColumnStore = &.{},
    row_count: u32 = 0,
    bytes: usize = 0,
    // Perf-counter ticks spent in scan.next() (decode + filter) vs.
    // appendAllColumn (deep-copying survivors). Populated only when
    // `exec.prof.enabled`; printed by runMaterialize.
    scan_ticks: u64 = 0,
    copy_ticks: u64 = 0,
};

/// The thread-safe allocator the per-worker scans/aggregates/survivor buffers
/// draw from. In production this is the process-global retaining buffer pool
/// (recycles the large decode buffers instead of `munmap`-ing them every scan);
/// `THINDB_NO_BUFPOOL=1` reverts to the raw backing allocator for an A/B without
/// a rebuild. Tests pass their own (`testing.allocator`/`c_allocator`) so leak
/// detection observes every free — the pool retains blocks, which a leak
/// detector would flag.
fn workerAlloc(base: Allocator) Allocator {
    if (builtin.is_test) return base;
    if (getenv("THINDB_NO_BUFPOOL")) |v| {
        if (v[0] == '1') return base;
    }
    return buffer_pool.allocator();
}

test "createOverStage: parallel buffer scan preserves the row multiset" {
    const allocator = std.testing.allocator;
    const mat_stage = @import("mat_stage.zig");

    const EmitStub = struct {
        allocator: Allocator,
        schema: [1]Column = .{.{ .name = "v", .type = .{ .bigint = {} }, .nullable = false }},
        data: []i64,
        views: [1]ColumnView = undefined,
        emitted: bool = false,

        pub fn next(self: *@This()) !?Batch {
            if (self.emitted) return null;
            self.emitted = true;
            self.views[0] = .{ .data = .{ .bigint = self.data }, .nulls = null };
            return Batch{ .schema = &self.schema, .values = &self.views, .row_count = self.data.len };
        }
        pub fn deinit(self: *@This()) void {
            self.allocator.destroy(self);
        }
        pub fn outputSchema(self: *@This()) []const Column {
            return &self.schema;
        }
        pub fn addPrune(_: *@This(), _: predicate.Predicate) !void {}
        pub fn stats(_: *@This()) exec.PipelineStats {
            return .{ .upper_rows = 0, .sort_state = .{}, .column_stats = &.{} };
        }
        pub fn accountant(_: *@This()) ?*exec.memory.MemoryAccountant {
            return null;
        }
        pub fn explain(_: *@This(), out: *std.ArrayList(u8), a: Allocator, depth: usize) !void {
            try exec.explainLine(out, a, depth, "EmitStub");
        }
    };

    // > 2 chunks so some stripes can span multiple chunks; DOP > 1 spawns the
    // round pool. The scan preserves the multiset (not necessarily order across
    // multi-chunk stripes), so check the sorted values.
    const n: usize = mat_stage.chunk_rows * 2 + 500;
    const data = try allocator.alloc(i64, n);
    defer allocator.free(data);
    for (data, 0..) |*d, i| d.* = @intCast(i);

    const stub = try allocator.create(EmitStub);
    stub.* = .{ .allocator = allocator, .data = data };
    const sq = exec.makeQuery(allocator, stub);

    const set = try mat_stage.StageSet.create(allocator);
    defer set.deinit();
    const stage = try set.addStage(sq, null);

    var q = try ParallelScan.createOverStage(allocator, allocator, stage, null, 3);
    defer q.deinit();

    const seen = try allocator.alloc(bool, n);
    defer allocator.free(seen);
    @memset(seen, false);
    var count: usize = 0;
    while (try q.next()) |batch| {
        for (batch.values[0].data.bigint) |v| {
            const idx: usize = @intCast(v);
            try std.testing.expect(idx < n);
            try std.testing.expect(!seen[idx]); // no duplicates
            seen[idx] = true;
            count += 1;
        }
    }
    try std.testing.expectEqual(n, count);
    for (seen) |s| try std.testing.expect(s); // every row exactly once
}

test "createOverStageOrdered: emission is the stage's exact row order" {
    const allocator = std.testing.allocator;
    const mat_stage = @import("mat_stage.zig");

    const EmitStub = struct {
        allocator: Allocator,
        schema: [1]Column = .{.{ .name = "v", .type = .{ .bigint = {} }, .nullable = false }},
        data: []i64,
        views: [1]ColumnView = undefined,
        emitted: bool = false,

        pub fn next(self: *@This()) !?Batch {
            if (self.emitted) return null;
            self.emitted = true;
            self.views[0] = .{ .data = .{ .bigint = self.data }, .nulls = null };
            return Batch{ .schema = &self.schema, .values = &self.views, .row_count = self.data.len };
        }
        pub fn deinit(self: *@This()) void {
            self.allocator.destroy(self);
        }
        pub fn outputSchema(self: *@This()) []const Column {
            return &self.schema;
        }
        pub fn addPrune(_: *@This(), _: predicate.Predicate) !void {}
        pub fn stats(_: *@This()) exec.PipelineStats {
            return .{ .upper_rows = 0, .sort_state = .{}, .column_stats = &.{} };
        }
        pub fn accountant(_: *@This()) ?*exec.memory.MemoryAccountant {
            return null;
        }
        pub fn explain(_: *@This(), out: *std.ArrayList(u8), a: Allocator, depth: usize) !void {
            try exec.explainLine(out, a, depth, "EmitStub");
        }
    };

    // Several chunks + DOP > 1 so parallel compute happens, then assert the
    // stream is EXACTLY 0..n-1 in order — the ordered contract ride/borrow
    // chains rely on (round mode only guarantees the multiset).
    const n: usize = mat_stage.chunk_rows * 3 + 777;
    const data = try allocator.alloc(i64, n);
    defer allocator.free(data);
    for (data, 0..) |*d, i| d.* = @intCast(i);

    const stub = try allocator.create(EmitStub);
    stub.* = .{ .allocator = allocator, .data = data };
    const sq = exec.makeQuery(allocator, stub);

    const set = try mat_stage.StageSet.create(allocator);
    defer set.deinit();
    const stage = try set.addStage(sq, null);

    var q = try ParallelScan.createOverStageOrdered(allocator, allocator, stage, null, 3);
    defer q.deinit();

    var expect_next: i64 = 0;
    while (try q.next()) |batch| {
        for (batch.values[0].data.bigint) |v| {
            try std.testing.expectEqual(expect_next, v);
            expect_next += 1;
        }
    }
    try std.testing.expectEqual(@as(i64, @intCast(n)), expect_next);
}

test "createOverStage + fused partial aggregate drains without corruption" {
    const allocator = std.testing.allocator;
    const mat_stage = @import("mat_stage.zig");
    // Workers deep-copy partials concurrently → needs a thread-safe allocator
    // (the real server path uses the db allocator; testing.allocator is not
    // thread-safe). c_allocator mirrors the production thread-safety.
    const worker_alloc = std.heap.c_allocator;

    const EmitStub2 = struct {
        allocator: Allocator,
        schema: [2]Column = .{
            .{ .name = "k", .type = .{ .bigint = {} }, .nullable = false },
            .{ .name = "v", .type = .{ .bigint = {} }, .nullable = false },
        },
        kdata: []i64,
        vdata: []i64,
        views: [2]ColumnView = undefined,
        emitted: bool = false,

        pub fn next(self: *@This()) !?Batch {
            if (self.emitted) return null;
            self.emitted = true;
            self.views[0] = .{ .data = .{ .bigint = self.kdata }, .nulls = null };
            self.views[1] = .{ .data = .{ .bigint = self.vdata }, .nulls = null };
            return Batch{ .schema = &self.schema, .values = &self.views, .row_count = self.kdata.len };
        }
        pub fn deinit(self: *@This()) void {
            self.allocator.destroy(self);
        }
        pub fn outputSchema(self: *@This()) []const Column {
            return &self.schema;
        }
        pub fn addPrune(_: *@This(), _: predicate.Predicate) !void {}
        pub fn stats(_: *@This()) exec.PipelineStats {
            return .{ .upper_rows = 0, .sort_state = .{}, .column_stats = &.{} };
        }
        pub fn accountant(_: *@This()) ?*exec.memory.MemoryAccountant {
            return null;
        }
        pub fn explain(_: *@This(), out: *std.ArrayList(u8), a: Allocator, depth: usize) !void {
            try exec.explainLine(out, a, depth, "EmitStub2");
        }
    };

    // Span several 65536-row chunks so DOP>1 actually stripes them across
    // workers — each builds a partial aggregate over its own stripe.
    const n: usize = 200_000;
    const kdata = try allocator.alloc(i64, n);
    defer allocator.free(kdata);
    const vdata = try allocator.alloc(i64, n);
    defer allocator.free(vdata);
    for (kdata, vdata, 0..) |*k, *v, i| {
        k.* = @intCast(i % 4); // 4 groups (low cardinality)
        v.* = 1;
    }

    const stub = try allocator.create(EmitStub2);
    stub.* = .{ .allocator = allocator, .kdata = kdata, .vdata = vdata };
    const sq = exec.makeQuery(allocator, stub);

    const set = try mat_stage.StageSet.create(allocator);
    defer set.deinit();
    const stage = try set.addStage(sq, null);

    var up = try ParallelScan.createOverStage(allocator, worker_alloc, stage, null, 4);
    defer up.deinit();

    const group_cols = [_][]const u8{"k"};
    const aggs = [_]exec.AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "v", .as = "sv" },
    };
    try std.testing.expect(try up.tryFuseAggregate(&group_cols, &aggs));

    // Each stripe worker emits partial groups; summed across every partial row
    // the count column (and the SUM(v=1) column) must each equal n — every row
    // counted exactly once across the parallel stripes, none dropped/doubled.
    // COUNT(*) stays bigint; SUM(bigint) widens to a largeint (i128) accumulator.
    var total_count: i64 = 0;
    var total_sum: i128 = 0;
    while (try up.next()) |b| {
        for (b.values[1].data.bigint) |c| total_count += c;
        for (b.values[2].data.largeint) |s| total_sum += s;
    }
    try std.testing.expectEqual(@as(i64, @intCast(n)), total_count);
    try std.testing.expectEqual(@as(i128, @intCast(n)), total_sum);
}

/// A parallel-scan worker leaf — either a range-restricted segment `Scan`
/// (table source) or a `ChunkRangeScan` over a materialized-stage stripe
/// (buffer source). The orchestrator above this — steal loop, fusion, merge,
/// DOP clamp — is source-agnostic; only worker creation, the snapshot/lock,
/// the row-group profiling counters, and zone-map pruning are segment-specific.
/// The `.chunk` arm reports "no fused filter / no surviving-unit prune", so a
/// buffer scan runs the full-DOP streaming/materialize path unchanged.
const Leaf = union(enum) {
    segment: *Scan,
    chunk: *ChunkRangeScan,

    pub fn next(self: Leaf) !?Batch {
        return switch (self) {
            inline else => |s| s.next(),
        };
    }
    fn deinit(self: Leaf) void {
        switch (self) {
            inline else => |s| s.deinit(),
        }
    }
    fn outputSchema(self: Leaf) []const Column {
        return switch (self) {
            inline else => |s| s.outputSchema(),
        };
    }
    fn stats(self: Leaf) exec.PipelineStats {
        return switch (self) {
            inline else => |s| s.stats(),
        };
    }
    fn addPrune(self: Leaf, pred: predicate.Predicate) !void {
        return switch (self) {
            inline else => |s| s.addPrune(pred),
        };
    }
    fn explain(self: Leaf, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        return switch (self) {
            inline else => |s| s.explain(out, allocator, depth),
        };
    }
    /// Segment-only: whether a WHERE was fused into the leaf (decides
    /// materialize vs round mode). A buffer leaf never fuses a filter.
    fn fusedActive(self: Leaf) bool {
        return switch (self) {
            .segment => |s| s.fusedActive(),
            .chunk => false,
        };
    }
    /// Segment-only: zone-map-surviving row groups for the DOP clamp. A buffer
    /// has no zone maps → null → the clamp keeps the full configured DOP.
    fn survivingWorkUnits(self: Leaf) ?usize {
        return switch (self) {
            .segment => |s| s.survivingWorkUnits(),
            .chunk => null,
        };
    }
    /// Segment-only: a buffer leaf declines filter fusion (any WHERE in a CTE
    /// block was already applied by the producer stage).
    fn tryFuseFilter(self: Leaf, expr: predicate.PredicateExpr) !bool {
        return switch (self) {
            .segment => |s| s.tryFuseFilter(expr),
            .chunk => false,
        };
    }
};

pub const ParallelScan = struct {
    allocator: Allocator,
    /// Table source (null for a materialized-buffer source). Used for the
    /// ddl-lock release and the explain name; the thread-safe allocator that
    /// workers allocate from concurrently is `worker_alloc` (set to
    /// `table.allocator` for a table, or the db allocator for a buffer).
    table: ?*Table,
    /// Thread-safe allocator for concurrent per-worker allocations
    /// (materialize deep-copy, fusion wrappers). For a table this is
    /// `table.allocator`; for a buffer the caller passes a thread-safe one.
    worker_alloc: Allocator,
    /// Buffer source (null for a table). The scan registers one use on it at
    /// create and releases that use at deinit, so the result stays alive for
    /// the whole consumption and frees afterward.
    stage: ?*Stage = null,

    /// One range-restricted Scan per CHUNK (materialize mode cuts ~`dop *
    /// CHUNK_FACTOR` byte-balanced chunks; round mode uses one per thread). In
    /// materialize mode `n_threads` worker threads steal these via `next_chunk`;
    /// in round mode it's one batch per chunk per fork-join round.
    workers: []Leaf,
    /// Worker thread count (the effective DOP). ≤ `workers.len`; in materialize
    /// mode `n_threads` threads steal `workers.len` chunks.
    n_threads: usize,
    /// Threads actually spawned by the last workSteal/startRoundPool — the
    /// prune-clamped effective DOP (see `effectiveThreads`). For reporting.
    eff_threads: usize = 0,
    /// Shared steal cursor: each worker claims chunk `next_chunk.fetchAdd(1)`
    /// until it reaches `workers.len`. Materialize mode only.
    next_chunk: std.atomic.Value(usize),

    acct: ?*exec.memory.MemoryAccountant,
    owns_acct: bool,
    out_schema: []const Column, // borrowed from workers[0], or owned when projected

    // Emit projection (materialize path): when a downstream consumer (a GROUP BY)
    // reports it reads only a subset of columns, drop the rest from the deep-copy
    // — they were decoded only to feed a fused filter/compute and are dead above.
    // `emit_keep[j]` is the worker-output index of projected output column j.
    emit_keep: ?[]const usize = null,
    owns_out_schema: bool = false,
    out_col_stats: []exec.ColStat = &.{},

    // Round state (touched only on the calling thread, between fork-joins).
    round: []?Batch, // one slot per worker; the batch it produced this round
    werr: []?anyerror, // one slot per worker; error from its next(), if any
    threads: []std.Thread,
    thread_active: []bool,
    exhausted: []bool, // worker has returned null (done)
    round_cursor: usize, // next round slot to emit; == workers.len ⇒ run a round
    all_done: bool,
    /// Ordered stage scan: one chunk-scan PER stage chunk, so each worker
    /// yields exactly one batch and round emission (chunk-index order) IS the
    /// stage's original row order. `stats` then keeps the source's sort
    /// property instead of clearing it — the seam a window's ride/borrow
    /// chain can cross without giving up parallel compute.
    ordered: bool = false,

    // Round-mode persistent worker pool. The fork-join in `runRound` reuses
    // `n_threads-1` long-lived threads (the calling thread is the nth) that
    // work-steal the `n_chunks` chunk-scans from `next_chunk` each round — so a
    // DOP-N scan runs N threads, NOT N*CHUNK_FACTOR, and never oversubscribes the
    // cores (one thread per chunk thrashed 48 threads onto 12 cores and the idle
    // ones stole cycles from the serial consumer). Persisting the threads avoids
    // re-spawning per round (hundreds–thousands of rounds for a big unfiltered
    // scan cost more in spawns than the decode). Workers spin on a sense-reversing
    // generation barrier (no std.Thread.Condition in this Zig fork): main bumps
    // `round_gen` to release them and waits on `round_done` to reach `n_chunks`.
    pool_started: bool = false,
    round_gen: std.atomic.Value(u32) = .{ .raw = 0 },
    round_done: std.atomic.Value(u32) = .{ .raw = 0 },
    pool_shutdown: std.atomic.Value(bool) = .{ .raw = false },
    prof_round_ticks: i64 = 0,
    prof_rounds: usize = 0,

    // Materialize-mode state (fused-filter path). Chosen lazily on the first
    // next() once we know whether a filter was fused in. `wbufs`/`emit_views`
    // are allocated then, not in create(); both stay empty in round mode.
    mode: Mode = .unset,
    wbufs: []WorkerBuf = &.{},
    emit_views: []ColumnView = &.{},
    emit_cursor: usize = 0,
    reserved_bytes: usize = 0,

    // Fused projection Compute (set via tryFuseCompute): when present, each
    // worker drains `compute_q[i]` (a Compute over its ranged Scan) instead of
    // the bare scan, so row-local derived columns (regex / scalar fns) compute
    // in parallel. `compute_q[i]` OWNS `workers[i]` (built from the thread-safe
    // table allocator); `compute_built` is how many were constructed (drives
    // deinit ownership, incl. the partial-build error path).
    compute_q: []Query = &.{},
    compute_built: usize = 0,
    compute_fused: bool = false,

    // Fused PARTIAL aggregate (set via tryFuseAggregate, two-phase GROUP BY):
    // each worker drains `agg_q[i]` (a partial Aggregate over its ranged Scan)
    // and emits its partial groups, which the materialize path concats for a
    // downstream serial combine aggregate. `agg_q[i]` OWNS `workers[i]`;
    // `agg_built` drives deinit ownership. Mutually exclusive with compute fusion
    // in P1 (the gate declines when a compute already fused).
    agg_q: []Query = &.{},
    agg_built: usize = 0,
    agg_fused: bool = false,

    // Fused join probe (set via tryFuseProbe; round mode only): each round
    // worker hands its scanned batch to the sink, which probes the offering
    // Join's hash table and returns the joined batch to stage instead. The
    // sink's per-chunk buffers make staged batches round-stable, same as the
    // bare scans' scratch. Mutually exclusive with every other fusion.
    probe_sink: ?exec.ProbeSink = null,
    /// Lazy stage barrier (createOverStageDeferred): the stage does NOT run
    /// at compile time — an eager barrier would materialize it (and its
    /// whole upstream stage DAG) during compile, pinning buffers that the
    /// normal execution order would have already freed. Workers build on
    /// the FIRST pull instead, when execution has reached this point in the
    /// DAG. Until then `workers` is empty and sinks are bound against the
    /// planned upper-bound chunk count.
    stage_deferred: bool = false,
    deferred_dop: usize = 0,
    /// Stage use released early (round drain exhausted) — the buffer frees
    /// as soon as the last consumer finishes, like a MatScan drain, instead
    /// of pinning until query teardown. deinit skips the second release.
    stage_released: bool = false,
    /// Per-chunk view scratch for sink.probe_map (a narrowing Project on
    /// the probe side): batch values are remapped into the chunk's slice
    /// before the sink sees them. Empty when the map is identity.
    probe_map_views: [][]ColumnView = &.{},

    /// Build a parallel scan over `table` projecting `needed` (null = all
    /// columns), with up to `max_dop` workers. Always returns a valid operator:
    /// when only one slot is available it runs serially through the same
    /// machinery (one whole-table worker, no threads spawned).
    pub fn create(
        allocator: Allocator,
        table: *Table,
        injected_acct: ?*exec.memory.MemoryAccountant,
        needed: ?[]const []const u8,
        max_dop: usize,
    ) !Query {
        // Spawn up to `max_dop` workers; the CoreScheduler throttles at run time
        // (each worker leases+pins a core, blocking when the machine is full), so
        // there's no up-front global slot budget to negotiate here.
        const dop = @max(@as(usize, 1), max_dop);

        const t_lock = exec.prof.nowTicks();
        table.ddl_lock.lockSharedUncancelable(table.io);
        errdefer table.ddl_lock.unlockShared(table.io);
        exec.prof.addPhase("pscan.create.ddl_lock", @intCast(exec.prof.nowTicks() - t_lock));

        const t_snap = exec.prof.nowTicks();
        // Dupe the entry array too (allocator-backed): every worker reads
        // this stable copy, never the live manifest ArrayList a concurrent
        // flush can realloc out from under them (#139 UAF).
        const snap = try Scan.captureSnapshotAlloc(table, allocator);
        exec.prof.addPhase("pscan.create.snapshot", @intCast(exec.prof.nowTicks() - t_snap));
        var pin_held = true;
        errdefer if (pin_held) snap.memtable_snap.release();
        defer allocator.free(snap.segments);

        // Flat row-group total + per-segment prefix sums, from the STABLE
        // snapshot (each entry carries its row_group_count) — no footer reads.
        const seg_start = try allocator.alloc(usize, snap.segment_count + 1);
        defer allocator.free(seg_start);
        var total_rgs: usize = 0;
        for (snap.segments, 0..) |e, i| {
            seg_start[i] = total_rgs;
            total_rgs += e.row_group_count;
        }
        seg_start[snap.segment_count] = total_rgs;

        // `n_threads` worker threads (the effective DOP) bounded by row-group
        // count. `n_chunks` is the finer work unit they steal — `dop *
        // CHUNK_FACTOR`, capped to the row-group total and never below n_threads.
        const n_threads = @max(@as(usize, 1), @min(dop, @max(total_rgs, 1)));
        const n_chunks = @max(n_threads, @min(dop * CHUNK_FACTOR, @max(total_rgs, 1)));

        // Mint or adopt the single accountant exposed to the downstream serial
        // tail. Workers get it as `injected` (they never touch it, so sharing is
        // race-free) so they don't each mint their own.
        var acct: ?*exec.memory.MemoryAccountant = injected_acct;
        var owns_acct = false;
        if (injected_acct == null and (table.query_memory_budget > 0 or table.memory_pool != null)) {
            const a = try allocator.create(exec.memory.MemoryAccountant);
            a.* = exec.memory.MemoryAccountant.initWithPool(table.query_memory_budget, table.memory_pool);
            acct = a;
            owns_acct = true;
        }
        errdefer if (owns_acct) {
            if (acct) |a| allocator.destroy(a);
        };

        // Byte-aware partition: weight each row group by the on-disk bytes of the
        // PROJECTED columns and cut contiguous spans of ~equal bytes, instead of
        // equal row-group COUNT. Variable-length columns make equal-count spans
        // carry very unequal work (one worker's URLs may be 2× another's), so the
        // join waits on a straggler. Reads each segment footer once (footer-only;
        // ~1 MB/segment, warm in page cache). Returns null — falling back to the
        // equal-count split below — on any footer-read failure, so a sizing
        // hiccup can never break the query.
        const t_part = exec.prof.nowTicks();
        const bounds: ?[]const usize = if (n_chunks > 1 and total_rgs > n_chunks)
            byteAwareBounds(allocator, table, snap.segments, total_rgs, n_chunks, needed)
        else
            null;
        defer if (bounds) |b| allocator.free(b);
        exec.prof.addPhase("pscan.create.byte_partition(footers)", @intCast(exec.prof.nowTicks() - t_part));

        const wa = workerAlloc(table.allocator);
        const t_workers = exec.prof.nowTicks();
        const workers = try allocator.alloc(Leaf, n_chunks);
        var built: usize = 0;
        errdefer {
            for (workers[0..built]) |w| w.deinit();
            allocator.free(workers);
        }
        for (0..n_chunks) |i| {
            const lo = if (bounds) |b| b[i] else i * total_rgs / n_chunks;
            const hi = if (bounds) |b| b[i + 1] else (if (i == n_chunks - 1) total_rgs else (i + 1) * total_rgs / n_chunks);
            const start = flatToCoord(lo, seg_start, snap.segment_count, total_rgs);
            const end = flatToCoord(hi, seg_start, snap.segment_count, total_rgs);
            const w = try Scan.allocWithProjectionLoc(wa, table, acct, needed, false, snap);
            workers[i] = .{ .segment = w };
            built += 1;
            // The last chunk also drains the memtable (ordered last).
            w.setRange(start.seg, start.rg, end.seg, end.rg, i == n_chunks - 1);
        }
        exec.prof.addPhase("pscan.create.worker_scans", @intCast(exec.prof.nowTicks() - t_workers));

        // Each chunk pinned the memtable; drop the orchestrator's capture pin.
        snap.memtable_snap.release();
        pin_held = false;

        const round = try allocator.alloc(?Batch, n_chunks);
        errdefer allocator.free(round);
        const werr = try allocator.alloc(?anyerror, n_chunks);
        errdefer allocator.free(werr);
        const threads = try allocator.alloc(std.Thread, n_chunks);
        errdefer allocator.free(threads);
        const thread_active = try allocator.alloc(bool, n_chunks);
        errdefer allocator.free(thread_active);
        const exhausted = try allocator.alloc(bool, n_chunks);
        errdefer allocator.free(exhausted);
        @memset(exhausted, false);

        const self = try allocator.create(ParallelScan);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .table = table,
            .worker_alloc = wa,
            .stage = null,
            .workers = workers,
            .n_threads = n_threads,
            .next_chunk = std.atomic.Value(usize).init(0),
            .acct = acct,
            .owns_acct = owns_acct,
            .out_schema = workers[0].outputSchema(),
            .round = round,
            .werr = werr,
            .threads = threads,
            .thread_active = thread_active,
            .exhausted = exhausted,
            .round_cursor = n_chunks, // force a round on the first next()
            .all_done = false,
        };
        return makeQuery(allocator, self);
    }

    /// Build a parallel scan over a materialized stage's result: shard its
    /// chunk list (uniform 65536-row tiles) into equal-count stripes that
    /// `max_dop` workers steal. Runs the stage ONCE here — the barrier — so the
    /// result is frozen before any worker reads it, and registers one use on
    /// the stage (released at deinit). `worker_alloc` MUST be thread-safe: the
    /// workers allocate concurrently (materialize deep-copy, fusion scratch).
    pub fn createOverStage(
        allocator: Allocator,
        worker_alloc_in: Allocator,
        stage: *Stage,
        injected_acct: ?*exec.memory.MemoryAccountant,
        max_dop: usize,
    ) !Query {
        return createOverStageMode(allocator, worker_alloc_in, stage, injected_acct, max_dop, false);
    }

    /// Order-preserving variant: emission is the stage's original row order
    /// (see `ordered` field). One round computes every chunk in parallel and
    /// holds all its batches until emitted, so the cap below bounds the
    /// transient footprint; a bigger stage declines (caller compiles serial).
    pub fn createOverStageOrdered(
        allocator: Allocator,
        worker_alloc_in: Allocator,
        stage: *Stage,
        injected_acct: ?*exec.memory.MemoryAccountant,
        max_dop: usize,
    ) !Query {
        return createOverStageMode(allocator, worker_alloc_in, stage, injected_acct, max_dop, true);
    }

    const ORDERED_MAX_CHUNKS: usize = 1024;

    fn createOverStageMode(
        allocator: Allocator,
        worker_alloc_in: Allocator,
        stage: *Stage,
        injected_acct: ?*exec.memory.MemoryAccountant,
        max_dop: usize,
        ordered: bool,
    ) !Query {
        const worker_alloc = workerAlloc(worker_alloc_in);
        const dop = @max(@as(usize, 1), max_dop);

        // The barrier: drain the producer once. After this the result is frozen
        // and N workers read disjoint chunk stripes lock-free.
        try stage.ensureRun();
        const result = stage.result orelse return error.UnsupportedQueryShape;
        const total_chunks = result.chunks.items.len;
        if (ordered and total_chunks > ORDERED_MAX_CHUNKS) return error.UnsupportedQueryShape;

        // Each 65536-row chunk is a uniform work unit, so equal-count stripes are
        // already byte-balanced (no footer-weighted partition needed).
        const n_threads = @max(@as(usize, 1), @min(dop, @max(total_chunks, 1)));
        const n_chunks = if (ordered)
            @max(total_chunks, 1)
        else
            @max(n_threads, @min(dop * CHUNK_FACTOR, @max(total_chunks, 1)));

        const workers = try allocator.alloc(Leaf, n_chunks);
        var built: usize = 0;
        errdefer {
            for (workers[0..built]) |w| w.deinit();
            allocator.free(workers);
        }
        for (0..n_chunks) |i| {
            const lo = i * total_chunks / n_chunks;
            const hi = if (i == n_chunks - 1) total_chunks else (i + 1) * total_chunks / n_chunks;
            const w = try ChunkRangeScan.alloc(worker_alloc, stage, result, lo, hi);
            workers[i] = .{ .chunk = w };
            built += 1;
        }

        const round = try allocator.alloc(?Batch, n_chunks);
        errdefer allocator.free(round);
        const werr = try allocator.alloc(?anyerror, n_chunks);
        errdefer allocator.free(werr);
        const threads = try allocator.alloc(std.Thread, n_chunks);
        errdefer allocator.free(threads);
        const thread_active = try allocator.alloc(bool, n_chunks);
        errdefer allocator.free(thread_active);
        const exhausted = try allocator.alloc(bool, n_chunks);
        errdefer allocator.free(exhausted);
        @memset(exhausted, false);

        const self = try allocator.create(ParallelScan);
        errdefer allocator.destroy(self);

        // One consumer; released at deinit. Nothing fails after this point.
        stage.registerUse();

        self.* = .{
            .allocator = allocator,
            .table = null,
            .worker_alloc = worker_alloc,
            .stage = stage,
            .workers = workers,
            .n_threads = n_threads,
            .next_chunk = std.atomic.Value(usize).init(0),
            .acct = injected_acct,
            .owns_acct = false,
            .out_schema = workers[0].outputSchema(),
            .round = round,
            .werr = werr,
            .threads = threads,
            .thread_active = thread_active,
            .exhausted = exhausted,
            .round_cursor = n_chunks,
            .all_done = false,
            .ordered = ordered,
        };
        return makeQuery(allocator, self);
    }

    /// Like createOverStage but with a LAZY barrier: nothing runs here; the
    /// stage materializes on the first pull. Fusion offers made in between
    /// (a join's probe sink, chained sinks above it) bind against
    /// `plannedChunks` — an upper bound the real chunk count never exceeds.
    /// Compute/aggregate fusion is declined instead (a serial Compute above
    /// still parallelizes by forwarding the probe offer with per-chunk
    /// clones), so the only worker pipeline is the bare chunk scan.
    pub fn createOverStageDeferred(
        allocator: Allocator,
        worker_alloc_in: Allocator,
        stage: *Stage,
        injected_acct: ?*exec.memory.MemoryAccountant,
        max_dop: usize,
    ) !Query {
        const worker_alloc = workerAlloc(worker_alloc_in);
        const self = try allocator.create(ParallelScan);
        errdefer allocator.destroy(self);

        // One consumer; released at deinit. Nothing fails after this point.
        stage.registerUse();

        self.* = .{
            .allocator = allocator,
            .table = null,
            .worker_alloc = worker_alloc,
            .stage = stage,
            .stage_deferred = true,
            .deferred_dop = @max(@as(usize, 1), max_dop),
            .workers = &.{},
            .n_threads = 1,
            .next_chunk = std.atomic.Value(usize).init(0),
            .acct = injected_acct,
            .owns_acct = false,
            .out_schema = stage.schema,
            .round = &.{},
            .werr = &.{},
            .threads = &.{},
            .thread_active = &.{},
            .exhausted = &.{},
            .round_cursor = 0,
            .all_done = false,
        };
        return makeQuery(allocator, self);
    }

    /// Sink-bind sizing before the deferred barrier runs: the real chunk
    /// count is `max(n_threads, min(dop*CHUNK_FACTOR, total))` — always
    /// ≤ dop*CHUNK_FACTOR, so per-chunk sink state bound at this size
    /// covers every chunk index the workers will use.
    fn plannedChunks(self: *const ParallelScan) usize {
        if (self.stage_deferred and self.workers.len == 0) return self.deferred_dop * CHUNK_FACTOR;
        return self.workers.len;
    }

    /// The lazy barrier: run the stage, shard its chunks, build the ranged
    /// workers + round state. First pull only.
    fn buildDeferredWorkers(self: *ParallelScan) !void {
        const stage = self.stage.?;
        try stage.ensureRun();
        const result = stage.result orelse return error.UnsupportedQueryShape;
        const total_chunks = result.chunks.items.len;
        const dop = self.deferred_dop;
        const n_threads = @max(@as(usize, 1), @min(dop, @max(total_chunks, 1)));
        const n_chunks = @max(n_threads, @min(dop * CHUNK_FACTOR, @max(total_chunks, 1)));

        const workers = try self.allocator.alloc(Leaf, n_chunks);
        var built: usize = 0;
        errdefer {
            for (workers[0..built]) |w| w.deinit();
            self.allocator.free(workers);
        }
        for (0..n_chunks) |i| {
            const lo = i * total_chunks / n_chunks;
            const hi = if (i == n_chunks - 1) total_chunks else (i + 1) * total_chunks / n_chunks;
            const w = try ChunkRangeScan.alloc(self.worker_alloc, stage, result, lo, hi);
            workers[i] = .{ .chunk = w };
            built += 1;
        }
        const round = try self.allocator.alloc(?Batch, n_chunks);
        errdefer self.allocator.free(round);
        const werr = try self.allocator.alloc(?anyerror, n_chunks);
        errdefer self.allocator.free(werr);
        const threads = try self.allocator.alloc(std.Thread, n_chunks);
        errdefer self.allocator.free(threads);
        const thread_active = try self.allocator.alloc(bool, n_chunks);
        errdefer self.allocator.free(thread_active);
        const exhausted = try self.allocator.alloc(bool, n_chunks);
        errdefer self.allocator.free(exhausted);
        @memset(exhausted, false);

        self.workers = workers;
        self.n_threads = n_threads;
        self.round = round;
        self.werr = werr;
        self.threads = threads;
        self.thread_active = thread_active;
        self.exhausted = exhausted;
        self.round_cursor = n_chunks;
    }

    pub fn deinit(self: *ParallelScan) void {
        // Round-mode pool may still be parked on the barrier (query abandoned
        // before the stream drained, e.g. an error or early LIMIT). Join first.
        const t_pool = exec.prof.nowTicks();
        self.shutdownRoundPool();
        exec.prof.addPhase("pscan.deinit.thread_join", @intCast(exec.prof.nowTicks() - t_pool));
        const t_free = exec.prof.nowTicks();
        defer exec.prof.addPhase("pscan.deinit.free_buffers", @intCast(exec.prof.nowTicks() - t_free));
        // Materialize-mode buffers: column data is owned by the table's
        // allocator (workers allocated it); the wbufs/views slices are the
        // operator's own. Release the charged bytes before the accountant is
        // torn down below.
        if (self.reserved_bytes > 0) {
            if (self.acct) |a| a.release(.materialize, self.reserved_bytes);
        }
        const ta = self.worker_alloc;
        for (self.wbufs) |*wb| {
            for (wb.columns) |*c| c.deinit(ta);
            if (wb.columns.len > 0) ta.free(wb.columns);
        }
        if (self.wbufs.len > 0) self.allocator.free(self.wbufs);
        if (self.emit_views.len > 0) self.allocator.free(self.emit_views);
        if (self.probe_map_views.len > 0) {
            for (self.probe_map_views) |s| self.allocator.free(s);
            self.allocator.free(self.probe_map_views);
        }

        // Ownership split (compute-fused path): scans [0..compute_built) are
        // owned by their per-worker Compute (compute_q) and freed by its deinit;
        // scans [compute_built..) are still bare and freed directly. With no
        // fused compute, compute_built == 0 → all scans freed here as usual.
        for (self.agg_q[0..self.agg_built]) |q| {
            var qq = q;
            qq.deinit();
        }
        if (self.agg_q.len > 0) self.allocator.free(self.agg_q);
        for (self.compute_q[0..self.compute_built]) |q| {
            var qq = q;
            qq.deinit();
        }
        if (self.compute_q.len > 0) self.allocator.free(self.compute_q);
        // agg-fusion and compute-fusion are mutually exclusive, so exactly one
        // of these counts is non-zero; their wrappers own workers[0..owned],
        // the rest are freed directly. Clamped: the deferred-leaf agg path
        // sizes agg_q by plannedChunks() — an upper bound that can exceed the
        // realized worker count (its wrappers wrap ProbeChunkScan, and like
        // the probe-fused normal path, no worker is directly deinited here).
        const owned = @min(self.agg_built + self.compute_built, self.workers.len);
        for (self.workers[owned..]) |w| w.deinit();
        self.allocator.free(self.workers);
        self.allocator.free(self.round);
        self.allocator.free(self.werr);
        self.allocator.free(self.threads);
        self.allocator.free(self.thread_active);
        self.allocator.free(self.exhausted);
        if (self.emit_keep) |k| self.allocator.free(k);
        if (self.owns_out_schema) self.allocator.free(@constCast(self.out_schema));
        if (self.out_col_stats.len > 0) self.allocator.free(self.out_col_stats);
        if (self.table) |t| t.ddl_lock.unlockShared(t.io);
        if (self.stage) |s| {
            if (!self.stage_released) s.releaseUse();
        }
        if (self.owns_acct) {
            if (self.acct) |a| self.allocator.destroy(a);
        }
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *ParallelScan) []const Column {
        return self.out_schema;
    }

    pub fn accountant(self: *ParallelScan) ?*exec.memory.MemoryAccountant {
        return self.acct;
    }

    pub fn addPrune(self: *ParallelScan, pred: predicate.Predicate) !void {
        for (self.workers) |w| try w.addPrune(pred);
    }

    pub fn tryFuseFilter(self: *ParallelScan, expr: predicate.PredicateExpr) !bool {
        // Every worker is an identical Scan over the same table/projection, so
        // fusion succeeds (or not) uniformly. Apply to all to stay consistent.
        var fused = false;
        for (self.workers, 0..) |w, i| {
            const r = try w.tryFuseFilter(expr);
            if (i == 0) fused = r;
        }
        return fused;
    }

    /// Absorb a row-local projection Compute: build one Compute per worker over
    /// its ranged scan (each owns the scan, allocated from the thread-safe table
    /// allocator with its own derived scratch), so the derived columns — incl.
    /// the per-row scalar work like REGEXP_REPLACE — evaluate inside the workers.
    /// Declines (stays serial) for non-row-local derived (CASE / subquery / var).
    /// `compute_built` tracks construction so deinit's ownership split is correct
    /// even if a mid-build allocation fails.
    pub fn tryFuseCompute(self: *ParallelScan, derived: []const Derived) !bool {
        if (self.stage_deferred) return false;
        if (self.mode != .unset or self.compute_fused) return false;
        const scan_cols = self.workers[0].outputSchema();
        for (derived) |d| {
            if (!derivedFusable(d, scan_cols)) return false;
        }
        const q = try self.allocator.alloc(Query, self.workers.len);
        self.compute_q = q;
        for (self.workers, 0..) |w, i| {
            const sq = switch (w) {
                inline else => |p| makeQuery(self.worker_alloc, p),
            };
            q[i] = try sq.compute(derived);
            self.compute_built = i + 1;
        }
        self.out_schema = q[0].outputSchema();
        self.compute_fused = true;
        return true;
    }

    /// A probe-fused join ABOVE the current sink's join chains its own sink
    /// onto this scan's emission: bind it over the same chunk count and
    /// re-type the emitted schema to its output. The original probe_sink
    /// stays — the joins feed each other inside sinkProcess per chunk; this
    /// scan only needs to know the FINAL schema its rounds now carry.
    /// Compile time only (before the first pull settles a mode).
    pub fn rechainProbeSink(self: *ParallelScan, sink: exec.ProbeSink) !bool {
        if (self.mode != .unset) return false;
        if (self.probe_sink == null or self.agg_fused or self.owns_out_schema) return false;
        try sink.bind(sink.ctx, self.plannedChunks(), self.worker_alloc);
        self.out_schema = sink.out_schema;
        return true;
    }

    /// Two-phase GROUP BY (P1): wrap each worker in a PARTIAL aggregate so it
    /// aggregates its own slice on its own core — no cross-core feed — and emits
    /// partial groups; a downstream serial combine aggregate re-aggregates the
    /// concatenated partials. Declines once a mode/compute/aggregate is settled.
    /// `agg_q[i]` OWNS `workers[i]`; `agg_built` drives the deinit ownership split.
    /// (The partial aggregates touch the shared, non-thread-safe accountant only
    /// to bump a fixed per-source usize counter — benign undercounting under
    /// concurrency; the materialize step does the real byte reservation. TODO:
    /// null the partial accountant before enabling this path by default.)
    /// Accept a join-probe sink when nothing else has claimed the workers'
    /// output. Works in both modes: round workers stage the sink's joined
    /// batches directly; materialize-mode (fused-filter) workers drain their
    /// chunks through the sink and deep-copy the joined output — so the
    /// buffers, emission views, and budget charge re-type to the JOIN's
    /// schema (`out_schema` swap below).
    pub fn tryFuseProbe(self: *ParallelScan, sink: exec.ProbeSink) !bool {
        const trace_jf = getenv("THINDB_TRACE_JOINFUSE") != null;
        if (self.mode != .unset) {
            if (trace_jf) std.debug.print("[jf]   ps decline: mode={s} table={}\n", .{ @tagName(self.mode), self.table != null });
            return false;
        }
        if (self.agg_fused) {
            if (trace_jf) std.debug.print("[jf]   ps decline: agg_fused\n", .{});
            return false;
        }
        // A stage-backed scan with fused per-stripe computes still accepts:
        // round mode probes each stripe's compute chain, so the sink sees
        // derived batches. Table-backed compute fusion keeps declining — its
        // materialize-mode drain composes compute and sink differently.
        if (self.compute_fused and self.table != null) {
            if (trace_jf) std.debug.print("[jf]   ps decline: table compute_fused\n", .{});
            return false;
        }
        if (self.emit_keep != null or self.owns_out_schema) {
            if (trace_jf) std.debug.print("[jf]   ps decline: emit_keep={} owns_schema={}\n", .{ self.emit_keep != null, self.owns_out_schema });
            return false;
        }
        if (sink.probe_map) |m| {
            const slices = try self.allocator.alloc([]ColumnView, self.plannedChunks());
            var built: usize = 0;
            errdefer {
                for (slices[0..built]) |s| self.allocator.free(s);
                self.allocator.free(slices);
            }
            for (slices) |*s| {
                s.* = try self.allocator.alloc(ColumnView, m.len);
                built += 1;
            }
            self.probe_map_views = slices;
        }
        try sink.bind(sink.ctx, self.plannedChunks(), self.worker_alloc);
        self.probe_sink = sink;
        self.out_schema = sink.out_schema;
        return true;
    }

    pub fn probeFusionReachable(_: *const ParallelScan) bool {
        return true;
    }

    /// Apply the sink's probe-side column remap (if any) so the sink's
    /// compiled indices address the batch the way the declining Project
    /// would have presented it.
    fn remapProbeBatch(self: *ParallelScan, sink: exec.ProbeSink, chunk: usize, batch: Batch) Batch {
        const m = sink.probe_map orelse return batch;
        const vs = self.probe_map_views[chunk];
        for (m, vs) |src, *v| v.* = batch.values[src];
        return .{ .schema = batch.schema, .values = vs, .row_count = batch.row_count };
    }

    pub fn tryFuseAggregate(self: *ParallelScan, group_cols: []const []const u8, aggs: []const exec.AggSpec) !bool {
        if (self.stage_deferred and self.workers.len == 0) {
            // Deferred leaf: the chunk scans don't exist yet, but with a
            // fused probe sink the per-chunk aggregate pipelines wrap
            // ProbeChunkScan, which needs only the chunk INDEX — buildable
            // now against the planned upper bound (chunks past the realized
            // worker count yield empty partials). Without a probe sink
            // there is nothing chunk-shaped to wrap; decline.
            if (self.probe_sink == null) return false;
            if (self.mode != .unset or self.compute_fused or self.agg_fused) return false;
            const n = self.plannedChunks();
            const q = try self.allocator.alloc(Query, n);
            self.agg_q = q;
            for (q, 0..) |*slot, i| {
                const pcs = try self.worker_alloc.create(ProbeChunkScan);
                pcs.* = .{ .ps = self, .chunk = i, .schema = self.out_schema };
                slot.* = try makeQuery(self.worker_alloc, pcs).groupBy(group_cols, aggs);
                self.agg_built = i + 1;
            }
            self.out_schema = q[0].outputSchema();
            self.agg_fused = true;
            return true;
        }
        if (self.mode != .unset or self.compute_fused or self.agg_fused) return false;
        const q = try self.allocator.alloc(Query, self.workers.len);
        self.agg_q = q;
        for (self.workers, 0..) |w, i| {
            // With a fused join probe, the per-chunk pipeline aggregates the
            // JOINED batches: scan → probe (ProbeChunkScan) → partial agg.
            const sq = if (self.probe_sink != null) blk: {
                const pcs = try self.worker_alloc.create(ProbeChunkScan);
                pcs.* = .{ .ps = self, .chunk = i, .schema = self.out_schema };
                break :blk makeQuery(self.worker_alloc, pcs);
            } else switch (w) {
                inline else => |p| makeQuery(self.worker_alloc, p),
            };
            q[i] = try sq.groupBy(group_cols, aggs);
            self.agg_built = i + 1;
        }
        self.out_schema = q[0].outputSchema();
        self.agg_fused = true;
        return true;
    }

    /// Full high-cardinality lease GROUP BY replacement. The aggregate owns this
    /// ParallelScan on success and drives the chunk scans directly into radix
    /// partitions, bypassing the normal `Query.next()` batch cursor.
    pub fn tryLeaseGroupBy(self: *ParallelScan, group_cols: []const []const u8, aggs: []const exec.AggSpec, top_k: ?@import("../ir/ir.zig").Op.TopK, emit_limit: ?u32, dop: usize) !?Query {
        if (getenv("THINDB_LEASE_SCAN_FUSE") == null and !self.shouldDirectLeaseGroupBy(group_cols)) return null;
        if (self.mode != .unset or self.agg_fused) return null;
        if (emit_limit != null) return null;
        const ra = @import("radix_aggregate.zig");
        const rtk: ?ra.TopK = if (top_k) |tk| blk: {
            if (tk.keys.len != 1) return null;
            break :blk .{ .k = tk.k, .col = tk.keys[0].col, .desc = tk.keys[0].desc };
        } else null;
        return ra.RadixLeaseAggregate.createFromParallelScan(self.allocator, self, group_cols, aggs, rtk, dop) catch |e| switch (e) {
            error.UnsupportedOperatorForType, error.AggregateUnsupportedType => null,
            else => e,
        };
    }

    fn shouldDirectLeaseGroupBy(self: *ParallelScan, group_cols: []const []const u8) bool {
        const st = self.baseStats();
        if (st.upper_rows == 0 or st.column_stats.len == 0) return false;
        var est: u64 = 1;
        for (group_cols) |gc| {
            const idx = types.findColumn(self.out_schema, gc) orelse return false;
            if (idx >= st.column_stats.len) return false;
            switch (st.column_stats[idx].ndv) {
                .exact => |nd| est *|= nd,
                .unknown => return false,
            }
        }
        est = @min(est, st.upper_rows);
        // The scan-fused path wins when many rows collapse into materially fewer
        // groups because it avoids the serialized ParallelScan.next() boundary.
        // Near-unique groupings pay the same partition cost but get little reuse,
        // so leave those on the default lease path unless explicitly forced.
        return est *| 4 <= st.upper_rows;
    }

    fn applyEmitProjection(self: *ParallelScan, keep: []const []const u8) !void {
        if (self.emit_keep != null) return;
        const full = self.out_schema;
        var idxs = try self.allocator.alloc(usize, full.len);
        errdefer self.allocator.free(idxs);
        var n: usize = 0;
        for (full, 0..) |c, i| {
            for (keep) |k| {
                if (types.columnNameEql(c.name, k)) {
                    idxs[n] = i;
                    n += 1;
                    break;
                }
            }
        }
        if (n == full.len) { // consumer reads everything — nothing to drop
            self.allocator.free(idxs);
            return;
        }
        const reduced = try self.allocator.alloc(Column, n);
        errdefer self.allocator.free(reduced);
        for (0..n) |j| reduced[j] = full[idxs[j]];
        const full_stats = self.baseStats();
        if (full_stats.column_stats.len >= full.len) {
            const reduced_stats = try self.allocator.alloc(exec.ColStat, n);
            errdefer self.allocator.free(reduced_stats);
            for (0..n) |j| reduced_stats[j] = full_stats.column_stats[idxs[j]];
            self.out_col_stats = reduced_stats;
        }
        self.emit_keep = try self.allocator.realloc(idxs, n);
        self.out_schema = reduced;
        self.owns_out_schema = true;
    }

    /// A downstream consumer reports it reads only `keep`. On the materialize
    /// path, drop every other output column from the survivor deep-copy — they
    /// were decoded purely to feed a fused filter/compute (e.g. `URL` behind
    /// `length(URL)` + `URL<>''`) and nothing above reads them. Safe because the
    /// forwarding chain only reaches here past a fused (pass-through) Filter; an
    /// unfused Filter swallows the projection before it reaches us.
    /// No-op for the round (stream) path — that emits the scan's already-pruned
    /// columns, so there is nothing dead to drop.
    pub fn setEmitProjection(self: *ParallelScan, keep: []const []const u8) !void {
        if (self.stage_deferred and self.workers.len == 0) return; // round path: nothing dead to drop
        if (!(self.workers[0].fusedActive() or self.compute_fused)) return;
        try self.applyEmitProjection(keep);
    }

    fn baseStats(self: *ParallelScan) exec.PipelineStats {
        if (self.agg_fused) return self.agg_q[0].stats();
        if (self.compute_fused) return self.compute_q[0].stats();
        if (self.stage_deferred and self.workers.len == 0) {
            // Report the stage's column stats too — downstream GROUP BY
            // routing (NDV products) runs at compile time, before the
            // deferred workers exist; dropping them here demotes fused
            // aggregates to the generic path.
            return .{
                .upper_rows = self.stage.?.stats_upper_rows,
                .sort_state = self.stage.?.sort_state,
                .column_stats = self.stage.?.col_stats,
            };
        }
        return self.workers[0].stats();
    }

    pub fn stats(self: *ParallelScan) exec.PipelineStats {
        var st = self.baseStats();
        // Workers report the SOURCE's sort property, but multi-stripe
        // emission (round steal / drain merge) interleaves stripes — the
        // emitted stream is only the same multiset. Claiming the source's
        // order here would let a downstream sorted-stream GROUP BY consume
        // scrambled rows as if grouped-adjacent. The ORDERED stage scan is
        // the exception: one chunk-scan per stage chunk, emitted in chunk
        // index order = the source's row order, so its claim stands.
        if (self.n_threads > 1 and !self.ordered) st.sort_state = .{};
        if (self.out_col_stats.len > 0) {
            return .{ .upper_rows = st.upper_rows, .sort_state = st.sort_state, .column_stats = self.out_col_stats };
        }
        return st;
    }

    pub fn explain(self: *ParallelScan, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        var buf: [192]u8 = undefined;
        const tag = if (self.ordered) "ordered" else if (self.agg_fused) "materialize+partial-agg" else if (self.compute_fused) "materialize+compute" else if (self.workers.len == 0) "deferred" else if (self.workers[0].fusedActive()) "materialize" else "stream";
        const src_name = if (self.table) |t| t.name else "<buffer>";
        const line = std.fmt.bufPrint(&buf, "ParallelScan {s} (DOP={d}, {s})", .{ src_name, self.n_threads, tag }) catch "ParallelScan";
        try exec.explainLine(out, allocator, depth, line);
        if (self.agg_fused) {
            try self.agg_q[0].explain(out, allocator, depth + 1);
        } else if (self.compute_fused) {
            try self.compute_q[0].explain(out, allocator, depth + 1);
        } else if (self.workers.len > 0) {
            try self.workers[0].explain(out, allocator, depth + 1);
        }
    }

    /// Prune-aware chunk re-cut, run once at the first pull (prune hints are
    /// installed by then; chunk bounds were cut byte-balanced over ALL row
    /// groups at create, before any hint existed). On a clustered table a
    /// selective filter empties most chunks — the survivors bunch into a few,
    /// and the scan degrades to their worker count (a 12-thread scan observed
    /// running effectively 4-wide, straggler chunk 12× the mean). Re-cut the
    /// same flat index space so each chunk holds ~equal SURVIVING row groups;
    /// pruned groups in between are skipped at near-zero cost. Only when at
    /// most half the groups survive — above that, the create-time byte
    /// balance is the better work model. Bounds stay ascending, so chunk
    /// emission order (and everything riding it) is unchanged.
    fn rebalanceChunksForPruning(self: *ParallelScan) void {
        if (self.table == null or self.workers.len <= 1) return;
        const first = switch (self.workers[0]) {
            .segment => |s| s,
            .chunk => return,
        };
        const mask = first.survivingMask(self.allocator) orelse return;
        defer self.allocator.free(mask);
        var surviving: usize = 0;
        for (mask) |m| {
            if (m) surviving += 1;
        }
        if (surviving == 0 or surviving > mask.len / 2) return;

        const n = self.workers.len;
        const seg_start = self.allocator.alloc(usize, first.segs.len + 1) catch return;
        defer self.allocator.free(seg_start);
        var total: usize = 0;
        for (first.segs, 0..) |entry, i| {
            seg_start[i] = total;
            total += entry.row_group_count;
        }
        seg_start[first.segs.len] = total;
        if (total != mask.len) return;

        // Quantile cut: chunk c ends once ceil((c+1)·surviving/n) survivors
        // are behind it. Trailing chunks may end empty when surviving < n.
        var lo: usize = 0;
        var seen: usize = 0;
        var cut: usize = 0;
        for (self.workers, 0..) |w, c| {
            const target = (c + 1) * surviving / n + @intFromBool((c + 1) * surviving % n != 0);
            while (cut < total and seen < target) : (cut += 1) {
                if (mask[cut]) seen += 1;
            }
            const hi = if (c == n - 1) total else cut;
            const start = flatToCoord(lo, seg_start, first.segs.len, total);
            const end = flatToCoord(hi, seg_start, first.segs.len, total);
            switch (w) {
                .segment => |s| s.resetRange(start.seg, start.rg, end.seg, end.rg, c == n - 1),
                .chunk => {},
            }
            lo = hi;
        }
        if (exec.prof.enabled) std.debug.print(
            "[pscan] prune-rebalance: surviving={d}/{d} rgs recut across {d} chunks\n",
            .{ surviving, total, n },
        );
    }

    pub fn next(self: *ParallelScan) !?Batch {
        if (self.stage_deferred and self.workers.len == 0) try self.buildDeferredWorkers();
        if (self.mode == .unset) {
            self.rebalanceChunksForPruning();
            // Fusion happens after create() (the wrapping Filter/Compute fuse on
            // their way up), so the strategy is settled by the first pull. A
            // fused filter (bounded survivors) ⇒ materialize + concat. A fused
            // compute over a BUFFER source streams (round mode): the workers run
            // the derived columns per stripe and emit batch-at-a-time, so a
            // streaming CTE chain crosses the stage boundary parallel with NO
            // full-result copy. (Table-source compute still materializes — the
            // table hot path is unchanged.)
            const compute_streams = self.compute_fused and self.table == null;
            const force_mat = self.agg_fused or self.workers[0].fusedActive() or
                (self.compute_fused and !compute_streams);
            self.mode = if (force_mat) .materialize else .round;
            if (self.mode == .materialize) try self.runMaterialize();
        }
        return switch (self.mode) {
            .materialize => self.nextMaterialize(),
            .round => self.nextRound(),
            .unset => unreachable,
        };
    }

    fn nextRound(self: *ParallelScan) !?Batch {
        while (true) {
            // Emit this round's batches in worker order.
            while (self.round_cursor < self.workers.len) {
                const idx = self.round_cursor;
                self.round_cursor += 1;
                if (self.round[idx]) |b| return b;
            }
            if (self.all_done) {
                self.releaseStageUse();
                return null;
            }
            try self.runRound();
        }
    }

    /// Drop this scan's consumer count on its source stage once the drain
    /// completes — batches already emitted were consumed before the caller
    /// pulled the null (standard batch-validity contract), so the buffer
    /// can free as soon as the remaining consumers finish. Idempotent;
    /// deinit covers abandoned (never-exhausted) scans.
    fn releaseStageUse(self: *ParallelScan) void {
        if (self.stage_released) return;
        if (self.stage) |st| {
            self.stage_released = true;
            st.releaseUse();
        }
    }

    /// Fused-filter path: spawn workers 1..n-1 once (worker 0 inline), each
    /// draining its entire slice to completion and deep-copying its survivors
    /// into its own ColumnStores. One spawn+join for the whole scan instead of
    /// one per sub-batch. Charges the merged survivor bytes against the query
    /// budget after the join (single-threaded — the accountant isn't
    /// thread-safe and workers never touch it).
    fn runMaterialize(self: *ParallelScan) !void {
        // Per-drainable buffers: a deferred leaf's agg pipelines were sized
        // to the PLANNED chunk bound, which can exceed the realized workers.
        const n = if (self.agg_fused) self.agg_q.len else if (self.compute_fused) self.compute_q.len else self.workers.len;
        const ta = self.worker_alloc;

        const wbufs = try self.allocator.alloc(WorkerBuf, n);
        for (wbufs) |*wb| wb.* = .{};
        self.wbufs = wbufs;
        self.emit_views = try self.allocator.alloc(ColumnView, self.out_schema.len);

        if (self.werr.len < n) {
            self.allocator.free(self.werr);
            self.werr = try self.allocator.alloc(?anyerror, n);
        }
        for (self.werr) |*e| e.* = null;
        @memset(self.thread_active, false);
        self.next_chunk.store(0, .monotonic);

        const wall0 = if (exec.prof.enabled) exec.prof.nowTicks() else 0;

        // Work-steal the chunks: `n_threads` workers (the calling thread + the
        // rest spawned) each claim chunks from `next_chunk` until exhausted. The
        // drainables are the Compute(scan) chains when a projection was fused in,
        // else the bare chunk scans. Output is emitted later in chunk order.
        if (self.agg_fused) {
            self.workSteal(self.agg_q, ta);
        } else if (self.compute_fused) {
            self.workSteal(self.compute_q, ta);
        } else {
            self.workSteal(self.workers, ta);
        }

        if (exec.prof.enabled) self.reportDrain(exec.prof.ticksToMs(exec.prof.nowTicks() - wall0));

        for (self.werr) |e| if (e) |err| return err;

        var total: usize = 0;
        for (self.wbufs) |wb| total += wb.bytes;
        if (self.acct) |a| try a.reserve(.materialize, total);
        self.reserved_bytes = total;
    }

    /// Threads actually worth spawning: the configured DOP clamped by the work
    /// that survives zone-map pruning (`Scan.survivingWorkUnits`). Runs at
    /// spawn time — the first `next()` — which is after the Query is composed,
    /// so the workers' prune hints are installed. No hints → full DOP.
    fn effectiveThreads(self: *ParallelScan) usize {
        if (self.n_threads <= 1) return self.n_threads;
        const surviving = self.workers[0].survivingWorkUnits() orelse return self.n_threads;
        const want = (surviving + RGS_PER_THREAD - 1) / RGS_PER_THREAD;
        return @max(@as(usize, 1), @min(self.n_threads, want));
    }

    /// Spawn `effectiveThreads()-1` workers (the calling thread is the last);
    /// each runs `stealLoop`, claiming chunks from the shared cursor until none
    /// remain, then join. Finer chunks + dynamic claiming balance work that
    /// static per-thread slices can't (survivor count ≠ bytes for filter/regex).
    fn workSteal(self: *ParallelScan, drainables: anytype, ta: Allocator) void {
        const eff = self.effectiveThreads();
        self.eff_threads = eff;
        var t: usize = 1;
        while (t < eff) : (t += 1) {
            if (std.Thread.spawn(.{}, stealLoop, .{ self, drainables, ta })) |th| {
                self.threads[t] = th;
                self.thread_active[t] = true;
            } else |_| {
                stealLoop(self, drainables, ta);
            }
        }
        stealLoop(self, drainables, ta); // calling thread is a worker too
        t = 1;
        while (t < eff) : (t += 1) {
            if (self.thread_active[t]) self.threads[t].join();
        }
    }

    /// Drain breakdown summary (only when --profile-ops): the spawn→join wall and
    /// the per-CHUNK time spread (min/max/mean) — a tighter spread than the old
    /// per-thread slices means finer chunks + stealing balanced the work.
    fn reportDrain(self: *ParallelScan, drain_wall_ms: f64) void {
        var rows: u64 = 0;
        var min_t: u64 = std.math.maxInt(u64);
        var max_t: u64 = 0;
        var sum_t: u64 = 0;
        for (self.wbufs) |wb| {
            rows += wb.row_count;
            const t = wb.scan_ticks + wb.copy_ticks;
            min_t = @min(min_t, t);
            max_t = @max(max_t, t);
            sum_t += t;
        }
        // Pruning effectiveness: how many row groups the workers actually decoded
        // vs considered within their assigned ranges. `considered - scanned` is
        // the zone-map prune count; `max_rgs` shows per-worker skew.
        var considered: u64 = 0;
        var scanned: u64 = 0;
        var rows_in: u64 = 0;
        var max_rgs: u64 = 0;
        for (self.workers) |w| switch (w) {
            .segment => |s| {
                considered += s.rgs_considered;
                scanned += s.rgs_scanned;
                rows_in += s.rows_scanned;
                max_rgs = @max(max_rgs, s.rgs_scanned);
            },
            .chunk => {},
        };
        std.debug.print("[pscan] threads={d}(eff={d}) chunks={d} drain_wall={d:.1}ms survivors={d} chunk_ms[min={d:.1} max={d:.1} mean={d:.1}]\n", .{
            self.n_threads,                       self.eff_threads,                     self.workers.len,                                        drain_wall_ms,                        rows,
            exec.prof.ticksToMs(@intCast(min_t)), exec.prof.ticksToMs(@intCast(max_t)), exec.prof.ticksToMs(@intCast(sum_t / self.workers.len)),
        });
        std.debug.print("[pscan] rowgroups: considered={d} scanned={d} pruned={d} ({d:.1}%)  rows_decoded={d}  busiest_worker_rgs={d}\n", .{
            considered,                  scanned, considered - scanned,
            if (considered > 0) @as(f64, @floatFromInt(considered - scanned)) * 100.0 / @as(f64, @floatFromInt(considered)) else 0.0,
            rows_in,                     max_rgs,
        });
        switch (self.workers[0]) {
            .segment => |w0| std.debug.print("[pscan] prune hints on worker0: leaf={d} in_set={d} seg_skip={}\n", .{
                w0.prunes.items.len, w0.in_prunes.items.len, w0.seg_skip != null,
            }),
            .chunk => {},
        }

        // Tight scan-kernel time: summed ACROSS workers (so it exceeds drain_wall
        // when DOP>1 — it's total core-time in the inner loop, not wall). Borrow =
        // block decompress/cache-pin; kernel = the actual compare/gather over the
        // bytes. Everything else in drain_wall is loop/decision/farm-out overhead.
        var borrow_ticks: u64 = 0;
        var kernel_ticks: u64 = 0;
        for (self.workers) |w| switch (w) {
            .segment => |s| {
                borrow_ticks +%= s.scan_borrow_ticks;
                kernel_ticks +%= s.scan_kernel_ticks;
            },
            .chunk => {},
        };
        std.debug.print("[pscan] scan-kernel core-time (summed over workers): borrow={d:.2}ms kernel(compare/gather)={d:.2}ms  rows_decoded={d} → {d:.1} M rows/core-sec in kernel\n", .{
            exec.prof.ticksToMs(@intCast(borrow_ticks)),
            exec.prof.ticksToMs(@intCast(kernel_ticks)),
            rows_in,
            if (kernel_ticks > 0) @as(f64, @floatFromInt(rows_in)) / (exec.prof.ticksToMs(@intCast(kernel_ticks)) * 1000.0) else 0.0,
        });

        // Per-chunk scan balance: how many row groups each chunk actually
        // decoded vs. pruned. With work-stealing the n_threads workers steal
        // these n_chunks chunks, so a chunk whose whole range zone-map-prunes
        // costs ~nothing while a chunk full of survivors gates a thread.
        var min_s: u64 = std.math.maxInt(u64);
        var sum_s: u64 = 0;
        var n_empty: usize = 0;
        for (self.workers) |w| {
            const rgs = switch (w) {
                .segment => |s| s.rgs_scanned,
                .chunk => 0,
            };
            min_s = @min(min_s, rgs);
            sum_s += rgs;
            if (rgs == 0) n_empty += 1;
        }
        const mean_s = if (self.workers.len > 0) sum_s / self.workers.len else 0;
        const skew = if (mean_s > 0) @as(f64, @floatFromInt(max_rgs)) / @as(f64, @floatFromInt(mean_s)) else 0.0;
        std.debug.print("[pscan] chunk scan balance: scanned/considered min={d} max={d} mean={d}  empty_chunks={d}/{d}  max/mean_skew={d:.2}x\n", .{
            min_s, max_rgs, mean_s, n_empty, self.workers.len, skew,
        });
        std.debug.print("[pscan] per-chunk scanned: ", .{});
        for (self.workers, 0..) |w, i| switch (w) {
            .segment => |s| std.debug.print("{d}:{d}/{d} ", .{ i, s.rgs_scanned, s.rgs_considered }),
            .chunk => std.debug.print("{d}:-/- ", .{i}),
        };
        std.debug.print("\n", .{});
    }

    /// Emit each worker's materialized survivor buffer as one batch, in slice
    /// order (worker 0 first) — the canonical serial row order.
    fn nextMaterialize(self: *ParallelScan) !?Batch {
        while (self.emit_cursor < self.wbufs.len) {
            const wb = &self.wbufs[self.emit_cursor];
            self.emit_cursor += 1;
            if (wb.row_count == 0) continue;
            for (wb.columns, self.emit_views) |*c, *v| v.* = c.view();
            return Batch{ .schema = self.out_schema, .values = self.emit_views, .row_count = wb.row_count };
        }
        return null;
    }

    /// Run one `next()` on every live worker concurrently, then stage their
    /// batches for in-order emission. The workers are a persistent pool spawned
    /// on the first round; each round is a generation-barrier fork-join, so a
    /// long unfiltered scan no longer pays N thread spawns per round.
    fn runRound(self: *ParallelScan) !void {
        const n = self.workers.len;
        const _rt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        defer if (exec.prof.enabled) {
            self.prof_round_ticks += @max(0, exec.prof.nowTicks() - _rt);
            self.prof_rounds += 1;
        };
        if (!self.pool_started) self.startRoundPool();

        for (self.round) |*r| r.* = null;
        for (self.werr) |*e| e.* = null;

        // Open this round: reset the steal cursor + done counter, then bump the
        // generation the pooled workers are parked on. They — and this thread —
        // claim chunks from `next_chunk` until it passes `n`.
        self.next_chunk.store(0, .release);
        self.round_done.store(0, .release);
        _ = self.round_gen.fetchAdd(1, .release);

        // The calling thread is a worker too; it would otherwise just block here.
        self.roundSteal();

        // Wait until every chunk has been run this round (one increment each).
        while (self.round_done.load(.acquire) < n) std.Thread.yield() catch {};

        // Propagate the first worker error (others' batches are discarded).
        for (self.werr) |e| if (e) |err| return err;

        var all = true;
        for (0..n) |w| {
            if (!self.exhausted[w] and self.round[w] == null) self.exhausted[w] = true;
            if (!self.exhausted[w]) all = false;
        }
        self.all_done = all;
        self.round_cursor = 0;
        // Stream exhausted: tear the pool down so its threads stop spinning. The
        // already-staged batches in `round[]` stay valid (they alias the worker
        // Scans, freed only in deinit) and are emitted by `nextRound`.
        if (all) {
            self.shutdownRoundPool();
            if (exec.prof.enabled) {
                std.debug.print("[hprof] pscan.rounds ordered={} chunks={d} threads={d} rounds={d} round_wall={d:.2} ms compute_fused={}\n", .{
                    self.ordered,
                    n,
                    self.eff_threads,
                    self.prof_rounds,
                    exec.prof.ticksToMs(self.prof_round_ticks),
                    self.compute_fused,
                });
            }
        }
    }

    /// Claim chunk-scans from `next_chunk` and run one `next()` on each into its
    /// own `round`/`werr` slot, until the cursor passes the chunk count. Shared by
    /// the calling thread and every pooled worker — `n_threads` runners draining
    /// `workers.len` chunks. Every claimed chunk bumps `round_done` (even an
    /// already-exhausted one, which is skipped) so the barrier counts exactly `n`.
    fn roundSteal(self: *ParallelScan) void {
        const n = self.workers.len;
        while (true) {
            const i = self.next_chunk.fetchAdd(1, .acq_rel);
            if (i >= n) break;
            if (!self.exhausted[i]) {
                if (self.probe_sink) |sink|
                    self.runOneProbe(sink, i)
                else if (self.compute_fused)
                    // Streaming compute: each stripe runs its fused Compute chain
                    // and stages the derived batch (round mode = no full copy).
                    runOne(&self.compute_q[i], &self.round[i], &self.werr[i])
                else
                    runOne(self.workers[i], &self.round[i], &self.werr[i]);
            }
            _ = self.round_done.fetchAdd(1, .release);
        }
    }

    /// Probe-fused chunk step: keep pulling scan batches until the sink turns
    /// one into a non-empty joined batch (staged for emission) or the chunk
    /// exhausts. Looping on empty join output keeps a round from staging
    /// zero-row batches while still bounding in-flight memory to one staged
    /// batch per chunk.
    fn runOneProbe(self: *ParallelScan, sink: exec.ProbeSink, i: usize) void {
        while (true) {
            const mb = (if (self.compute_fused) self.compute_q[i].next() else self.workers[i].next()) catch |e| {
                self.werr[i] = e;
                return;
            };
            const batch = mb orelse {
                self.round[i] = null;
                return;
            };
            const joined = sink.process(sink.ctx, i, self.remapProbeBatch(sink, i, batch)) catch |e| {
                self.werr[i] = e;
                return;
            };
            if (joined) |jb| {
                if (jb.row_count > 0) {
                    self.round[i] = jb;
                    return;
                }
            }
        }
    }

    /// Spawn `n_threads-1` persistent work-stealing threads (the calling thread is
    /// the nth runner). A thread that fails to spawn is simply absent — the steal
    /// cursor guarantees the remaining runners still cover every chunk, so the
    /// scan stays correct at any spawn count.
    fn startRoundPool(self: *ParallelScan) void {
        self.round_gen.store(0, .monotonic);
        self.round_done.store(0, .monotonic);
        self.pool_shutdown.store(false, .monotonic);
        @memset(self.thread_active, false);
        const eff = self.effectiveThreads();
        self.eff_threads = eff;
        var i: usize = 1;
        while (i < eff) : (i += 1) {
            if (std.Thread.spawn(.{}, roundWorker, .{self})) |th| {
                self.threads[i] = th;
                self.thread_active[i] = true;
            } else |_| {}
        }
        self.pool_started = true;
    }

    /// Signal the pool to exit and join every spawned worker. Idempotent.
    fn shutdownRoundPool(self: *ParallelScan) void {
        if (!self.pool_started) return;
        self.pool_shutdown.store(true, .release);
        _ = self.round_gen.fetchAdd(1, .release); // wake workers parked on the barrier
        var i: usize = 1;
        while (i < self.n_threads) : (i += 1) {
            if (self.thread_active[i]) {
                self.threads[i].join();
                self.thread_active[i] = false;
            }
        }
        self.pool_started = false;
    }
};

/// Per-chunk leaf for composed probe+aggregate fusion: pulls the chunk's
/// scan, runs each batch through the join's ProbeSink, and emits the joined
/// batches — the per-chunk partial Aggregate built on top drains this inside
/// drainWorker, on the worker thread that owns the chunk. Owns the chunk's
/// Scan, mirroring the bare agg_q ownership convention (pscan.deinit skips
/// workers[0..agg_built]).
const ProbeChunkScan = struct {
    ps: *ParallelScan,
    chunk: usize,
    /// Joined-batch schema, captured at creation. `ps.out_schema` is NOT
    /// stable — tryFuseAggregate replaces it with the partial-agg OUTPUT
    /// schema right after building the per-chunk pipelines, and the
    /// per-chunk Aggregate lazily re-reads upstream.outputSchema() at
    /// first accumulate (agg-input type resolution). Reading the live
    /// pointer there mis-typed — or indexed out of bounds of — the
    /// narrower agg schema (#118).
    schema: []const Column,

    pub fn next(self: *ProbeChunkScan) !?Batch {
        const ps = self.ps;
        const sink = ps.probe_sink.?;
        // A deferred leaf plans an upper-bound chunk count; realized workers
        // can be fewer — the excess chunks are empty.
        if (self.chunk >= ps.workers.len) return null;
        while (true) {
            const batch = (try ps.workers[self.chunk].next()) orelse return null;
            const joined = try sink.process(sink.ctx, self.chunk, ps.remapProbeBatch(sink, self.chunk, batch));
            if (joined) |jb| {
                if (jb.row_count > 0) return jb;
            }
        }
    }

    pub fn deinit(self: *ProbeChunkScan) void {
        if (self.chunk < self.ps.workers.len) self.ps.workers[self.chunk].deinit();
        self.ps.worker_alloc.destroy(self);
    }

    pub fn outputSchema(self: *ProbeChunkScan) []const Column {
        return self.schema;
    }

    pub fn addPrune(self: *ProbeChunkScan, pred: predicate.Predicate) !void {
        return self.ps.workers[self.chunk].addPrune(pred);
    }

    /// Join output cardinality is unknowable here; the partial Aggregate
    /// sizes its table from the data.
    pub fn stats(_: *ProbeChunkScan) exec.PipelineStats {
        return .{ .upper_rows = std.math.maxInt(u64) };
    }

    /// Workers never touch the (non-thread-safe) query accountant.
    pub fn accountant(_: *ProbeChunkScan) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(_: *ProbeChunkScan, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "ProbeChunkScan");
    }
};

fn runOne(scan: anytype, out_batch: *?Batch, out_err: *?anyerror) void {
    const r = scan.next() catch |e| {
        out_err.* = e;
        return;
    };
    out_batch.* = r;
}

/// One persistent round-mode pool thread. Parks on the generation barrier, then
/// work-steals chunk-scans for the round via `roundSteal`. Exits when
/// `pool_shutdown` is set. Touches only the chunk slots it claims plus the shared
/// barrier/cursor atomics, so — like the materialize-mode steal workers — it needs
/// no further locking (the cache + table allocator are thread-safe; the accountant
/// is untouched here).
fn roundWorker(self: *ParallelScan) void {
    var seen: u32 = 0;
    while (true) {
        var spins: u32 = 0;
        while (true) {
            const g = self.round_gen.load(.acquire);
            if (g != seen) {
                seen = g;
                break;
            }
            if (self.pool_shutdown.load(.acquire)) return;
            // Park on the barrier with a PAUSE-heavy spin: between rounds the
            // serial consumer (often a GROUP BY hash aggregate) is the bottleneck,
            // and a worker busy-spinning on its own logical core would steal the
            // sibling hyperthread's execution units from it. `spinLoopHint` (PAUSE)
            // cedes those units without giving up the core, so the worker still
            // wakes instantly on the next gen bump — no sleep latency. An occasional
            // OS yield covers genuine oversubscription (DOP > physical cores).
            spins +%= 1;
            if (spins & 63 == 0) std.Thread.yield() catch {} else std.atomic.spinLoopHint();
        }
        if (self.pool_shutdown.load(.acquire)) return;
        self.roundSteal();
    }
}

/// One worker thread's steal loop: repeatedly claim the next chunk from the
/// shared `next_chunk` cursor and drain it into its WorkerBuf, until the cursor
/// passes the chunk count. `drainables` is `[]Query` (compute-fused) or `[]*Scan`
/// (bare); monomorphized per call site.
fn stealLoop(self: *ParallelScan, drainables: anytype, ta: Allocator) void {
    // Lease a core only AFTER claiming the first chunk: a worker that loses the
    // race for every chunk (all drained by faster peers) never blocks on the
    // scheduler and never stalls the join. A worker already holding a core (the
    // inline calling thread carrying a serial-stage lease) reuses it — the
    // scheduler's no-hold-and-wait guard returns a non-owning lease.
    const sched = core_scheduler.global();
    var lease: core_scheduler.Lease = undefined;
    var leased = false;
    defer if (leased) lease.release();
    while (true) {
        const i = self.next_chunk.fetchAdd(1, .monotonic);
        if (i >= drainables.len) break;
        if (!leased) {
            lease = sched.acquire();
            leased = true;
        }
        // With a composed probe+aggregate fusion, the probing happens inside
        // each agg_q chunk pipeline (ProbeChunkScan) — drain plain.
        const sink = if (self.agg_fused) null else self.probe_sink;
        const remap: ?[]ColumnView = if (sink != null and self.probe_map_views.len > 0) self.probe_map_views[i] else null;
        drainWorker(drainables[i], ta, self.out_schema, self.emit_keep, sink, i, remap, &self.wbufs[i], &self.werr[i]);
    }
}

/// Drain `drainable` (a range-restricted, filter-fused Scan, or a Compute over
/// one when a projection was fused) to completion on its own thread,
/// deep-copying every output batch into freshly-allocated owned ColumnStores
/// (batches alias reused scratch, so we can't just retain them). `alloc` must
/// be thread-safe (the table allocator) — workers run concurrently and never
/// touch the query accountant. `drainable` is `*Scan` or `Query`; both expose
/// `.next()`, so this is monomorphized per call site.
fn drainWorker(drainable: anytype, alloc: Allocator, schema: []const Column, keep: ?[]const usize, sink: ?exec.ProbeSink, chunk: usize, probe_remap: ?[]ColumnView, wb: *WorkerBuf, out_err: *?anyerror) void {
    var d = drainable;
    const cols = alloc.alloc(ColumnStore, schema.len) catch |e| {
        out_err.* = e;
        return;
    };
    var inited: usize = 0;
    for (schema, cols) |col, *store| {
        store.* = ColumnStore.init(alloc, col.type, col.nullable) catch |e| {
            for (cols[0..inited]) |*c| c.deinit(alloc);
            alloc.free(cols);
            out_err.* = e;
            return;
        };
        inited += 1;
    }
    wb.columns = cols; // hand to deinit for cleanup, even if a later step errors

    const timing = exec.prof.enabled;
    var scan_ticks: u64 = 0;
    var copy_ticks: u64 = 0;
    var rc: u32 = 0;
    outer: while (true) {
        const s0 = if (timing) exec.prof.nowTicks() else 0;
        const mb = d.next() catch |e| {
            out_err.* = e;
            break;
        };
        if (timing) scan_ticks += @intCast(exec.prof.nowTicks() - s0);
        const batch = mb orelse break;
        const c0 = if (timing) exec.prof.nowTicks() else 0;
        if (sink) |s| {
            // Probe-fused drain: the sink turns the scanned batch into a
            // joined batch (aliasing its per-chunk staging, schema =
            // `out_schema` = the join's) which we deep-copy like any other.
            var pbatch = batch;
            if (s.probe_map) |m| {
                const vs = probe_remap.?;
                for (m, vs) |src, *v| v.* = batch.values[src];
                pbatch = .{ .schema = batch.schema, .values = vs, .row_count = batch.row_count };
            }
            const joined = s.process(s.ctx, chunk, pbatch) catch |e| {
                out_err.* = e;
                break :outer;
            };
            if (joined) |jb| {
                for (cols, jb.values) |*store, v| {
                    transform.appendAllColumn(alloc, v, store) catch |e| {
                        out_err.* = e;
                        break :outer;
                    };
                }
                rc += @intCast(jb.row_count);
            }
            if (timing) copy_ticks += @intCast(exec.prof.nowTicks() - c0);
            continue;
        }
        // `keep[j]` maps projected output column j to its batch index; without a
        // projection the mapping is the identity (copy every column).
        for (cols, 0..) |*store, j| {
            const src = if (keep) |k| k[j] else j;
            transform.appendAllColumn(alloc, batch.values[src], store) catch |e| {
                out_err.* = e;
                break :outer;
            };
        }
        if (timing) copy_ticks += @intCast(exec.prof.nowTicks() - c0);
        rc += @intCast(batch.row_count);
    }
    wb.row_count = rc;
    wb.bytes = rc * exec.memory.estimateRowBytes(schema);
    wb.scan_ticks = scan_ticks;
    wb.copy_ticks = copy_ticks;
}

/// A single derived column is fusable into the parallel workers iff its
/// expression is row-local: literals, scalar function calls (recursively), and
/// column refs that resolve to an UPSTREAM (scan) column. CASE and the
/// subquery/variable Expr variants are rejected — a CASE branch condition can
/// carry a correlated predicate, and subquery/var nodes touch query-wide state.
/// A col_ref to a SIBLING derived (a name not in `scan_cols`) is also rejected,
/// so the compile split leaves it for the serial post-parallel Compute, where
/// that column exists. (Subquery/var nodes are normally pre-resolved to literals
/// before operators are built; rejecting them defensively guards future changes.)
pub fn derivedFusable(d: Derived, scan_cols: []const Column) bool {
    return exprFusable(d.expr, scan_cols);
}

fn exprFusable(e: Expr, scan_cols: []const Column) bool {
    return switch (e) {
        .col_ref => |name| types.findColumn(scan_cols, name) != null,
        .lit => true,
        .null_lit => true,
        .call => |c| {
            for (c.args) |a| {
                if (!exprFusable(a, scan_cols)) return false;
            }
            return true;
        },
        // CASE stays serial by MEASUREMENT, not capability: the per-stripe
        // Compute instances evaluate it correctly (own CasePlan per op), but
        // the wayroll A/B (2026-07-11) showed fusing CASE-heavy chains is a
        // net loss — the work is memory-bandwidth-bound, so N workers buy
        // nothing while the per-chunk operator instances add allocation cost.
        .case, .scalar_subquery, .exists_subquery, .var_ref => false,
    };
}

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

/// Map a flat row-group index to its `(segment, row_group)` coordinate. `f ==
/// total` (a worker's exclusive end) maps to the past-the-end sentinel.
fn flatToCoord(f: usize, seg_start: []const usize, segment_count: usize, total: usize) Coord {
    if (f >= total) return .{ .seg = segment_count, .rg = 0 };
    var seg: usize = 0;
    while (seg + 1 < segment_count and seg_start[seg + 1] <= f) seg += 1;
    return .{ .seg = seg, .rg = f - seg_start[seg] };
}

/// On-disk byte weight of one row group for partitioning: the summed block
/// length of the PROJECTED columns (`proj` = their schema indices), or the whole
/// row group's `length` when projecting all columns (`proj == null`). A column's
/// block runs from its `col_offsets[ci]` to the next column's offset (the last
/// column ends at `rg.offset + rg.length`). Falls back to whole-RG `length` if
/// the footer didn't carry per-column offsets.
fn rgWeight(rg: storage.RowGroupMeta, proj: ?[]const usize, ncols: usize) u64 {
    const cols = proj orelse return rg.length;
    if (cols.len == 0 or rg.col_offsets.len < ncols) return rg.length;
    var sum: u64 = 0;
    for (cols) |ci| {
        const start = rg.col_offsets[ci];
        const end = if (ci + 1 < ncols) rg.col_offsets[ci + 1] else rg.offset + rg.length;
        sum += end - start;
    }
    return sum;
}

/// Compute `eff + 1` contiguous row-group boundaries that split the flat
/// row-group list into spans of ~equal projected bytes (see `rgWeight`). Reads
/// each segment's footer once. Returns null on any failure (caller falls back to
/// the equal-count split) — never partially applied. Caller owns the returned
/// slice.
fn byteAwareBounds(
    allocator: Allocator,
    table: *Table,
    segs: []const storage.ManifestEntry,
    total_rgs: usize,
    eff: usize,
    needed: ?[]const []const u8,
) ?[]const usize {
    const ncols = table.schema.columns.len;

    // Projected physical column indices (null ⇒ all columns ⇒ whole-RG length).
    var proj: ?[]usize = null;
    defer if (proj) |p| allocator.free(p);
    if (needed) |names| {
        const p = allocator.alloc(usize, names.len) catch return null;
        var n: usize = 0;
        for (names) |nm| {
            if (types.findColumn(table.schema.columns, nm)) |idx| {
                p[n] = idx;
                n += 1;
            }
        }
        proj = p[0..n];
    }

    const weights = allocator.alloc(u64, total_rgs) catch return null;
    defer allocator.free(weights);

    var total_bytes: u64 = 0;
    var flat: usize = 0;
    for (segs) |entry| {
        const handle = table.acquireSegment(entry.segment_id) catch return null;
        defer table.releaseSegment(handle);
        for (handle.seg.info.row_groups) |rg| {
            if (flat >= total_rgs) return null; // manifest/footer row-group disagreement
            weights[flat] = rgWeight(rg, proj, ncols);
            total_bytes += weights[flat];
            flat += 1;
        }
    }
    if (flat != total_rgs or total_bytes == 0) return null;

    const bounds = allocator.alloc(usize, eff + 1) catch return null;
    bounds[0] = 0;
    bounds[eff] = total_rgs;
    var cum: u64 = 0;
    var bi: usize = 1;
    for (weights, 0..) |w, idx| {
        cum += w;
        // First RG whose cumulative weight reaches bi/eff of the total ends
        // span bi-1. `cum * eff` vs `total_bytes * bi` avoids division rounding;
        // both stay well within u64 (bytes × small dop).
        while (bi < eff and cum *% eff >= total_bytes *% @as(u64, bi)) {
            bounds[bi] = idx + 1;
            bi += 1;
        }
    }
    while (bi < eff) : (bi += 1) bounds[bi] = total_rgs;

    if (exec.prof.enabled) {
        std.debug.print("[pscan-part] byte-aware DOP={d} total_bytes={d}\n", .{ eff, total_bytes });
        for (0..eff) |i| {
            var span: u64 = 0;
            for (bounds[i]..bounds[i + 1]) |k| span += weights[k];
            std.debug.print("[pscan-part]   w{d}: rgs[{d}..{d}) bytes={d}\n", .{ i, bounds[i], bounds[i + 1], span });
        }
    }

    return bounds;
}
