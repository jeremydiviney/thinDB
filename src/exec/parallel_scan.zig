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
//! Execution is fork-join by round: each `next()` round runs one
//! `worker.next()` per live worker concurrently (worker 0 inline, the rest
//! spawned), then emits the produced batches in worker order. In-flight memory
//! is bounded to `dop` batches; the emission order is reproducible for a given
//! `dop` (DOP=1 is the canonical serial result). Per-query parallelism is capped
//! by `max_dop` and globally by a shared worker-slot budget (~cores−1) so
//! concurrent queries share cores instead of oversubscribing.

const std = @import("std");
const Allocator = std.mem.Allocator;

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

/// Process-global worker-slot budget shared across all queries. The inline
/// (calling) thread is always free; these slots gate the SPAWNED workers, so
/// total spawned threads across concurrent queries stay near `cores − 1`.
const GlobalSlots = struct {
    var used = std.atomic.Value(usize).init(0);

    fn cap() usize {
        const total = std.Thread.getCpuCount() catch 2;
        return if (total > 1) total - 1 else 1;
    }

    /// Grant up to `want` slots (0 when none free). CAS so concurrent queries
    /// don't over-grant.
    fn acquire(want: usize) usize {
        if (want == 0) return 0;
        const c = cap();
        while (true) {
            const cur = used.load(.monotonic);
            if (cur >= c) return 0;
            const grant = @min(want, c - cur);
            if (grant == 0) return 0;
            if (used.cmpxchgWeak(cur, cur + grant, .acquire, .monotonic) == null) return grant;
        }
    }

    fn release(n: usize) void {
        if (n > 0) _ = used.fetchSub(n, .release);
    }
};

/// A (segment, row_group) coordinate over the flat row-group list.
const Coord = struct { seg: usize, rg: usize };

pub const ParallelScan = struct {
    allocator: Allocator,
    table: *Table,

    /// One full range-restricted Scan per worker. `workers[0]` runs on the
    /// calling thread; `workers[1..]` are spawned each round.
    workers: []*Scan,
    /// Extra worker slots granted by `GlobalSlots` (== workers.len − 1),
    /// returned at deinit.
    granted: usize,

    acct: ?*exec.memory.MemoryAccountant,
    owns_acct: bool,
    out_schema: []const Column, // borrowed from workers[0]

    // Round state (touched only on the calling thread, between fork-joins).
    round: []?Batch, // one slot per worker; the batch it produced this round
    werr: []?anyerror, // one slot per worker; error from its next(), if any
    threads: []std.Thread,
    thread_active: []bool,
    exhausted: []bool, // worker has returned null (done)
    round_cursor: usize, // next round slot to emit; == workers.len ⇒ run a round
    all_done: bool,

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
        const want_extra = if (max_dop > 1) max_dop - 1 else 0;
        const granted = GlobalSlots.acquire(want_extra);
        errdefer GlobalSlots.release(granted);
        const dop = 1 + granted;

        table.ddl_lock.lockSharedUncancelable(table.io);
        errdefer table.ddl_lock.unlockShared(table.io);

        const snap = Scan.captureSnapshot(table);
        var pin_held = true;
        errdefer if (pin_held) snap.memtable_snap.release();

        // Flat row-group total + per-segment prefix sums, straight from the
        // manifest (each entry carries its row_group_count) — no footer reads.
        const seg_start = try allocator.alloc(usize, snap.segment_count + 1);
        defer allocator.free(seg_start);
        var total_rgs: usize = 0;
        for (table.manifest.segments.items[0..snap.segment_count], 0..) |e, i| {
            seg_start[i] = total_rgs;
            total_rgs += e.row_group_count;
        }
        seg_start[snap.segment_count] = total_rgs;

        // No point in more workers than blocks (+1 lets a tiny/empty table still
        // have one worker for the memtable). Return the now-excess slots.
        const eff = @max(@as(usize, 1), @min(dop, @max(total_rgs, 1)));
        if (dop > eff) {
            GlobalSlots.release(dop - eff);
        }
        const eff_granted = eff - 1;

        // Mint or adopt the single accountant exposed to the downstream serial
        // tail. Workers get it as `injected` (they never touch it, so sharing is
        // race-free) so they don't each mint their own.
        var acct: ?*exec.memory.MemoryAccountant = injected_acct;
        var owns_acct = false;
        if (injected_acct == null and table.query_memory_budget > 0) {
            const a = try allocator.create(exec.memory.MemoryAccountant);
            a.* = exec.memory.MemoryAccountant.init(table.query_memory_budget);
            acct = a;
            owns_acct = true;
        }
        errdefer if (owns_acct) {
            if (acct) |a| allocator.destroy(a);
        };

        const workers = try allocator.alloc(*Scan, eff);
        var built: usize = 0;
        errdefer {
            for (workers[0..built]) |w| w.deinit();
            allocator.free(workers);
        }
        for (0..eff) |i| {
            const lo = i * total_rgs / eff;
            const hi = if (i == eff - 1) total_rgs else (i + 1) * total_rgs / eff;
            const start = flatToCoord(lo, seg_start, snap.segment_count, total_rgs);
            const end = flatToCoord(hi, seg_start, snap.segment_count, total_rgs);
            const w = try Scan.allocWithProjectionLoc(table.allocator, table, acct, needed, false, snap);
            workers[i] = w;
            built += 1;
            // The last worker also drains the memtable (ordered last).
            w.setRange(start.seg, start.rg, end.seg, end.rg, i == eff - 1);
        }

        // Workers each pinned the memtable; drop the orchestrator's capture pin.
        snap.memtable_snap.release();
        pin_held = false;

        const round = try allocator.alloc(?Batch, eff);
        errdefer allocator.free(round);
        const werr = try allocator.alloc(?anyerror, eff);
        errdefer allocator.free(werr);
        const threads = try allocator.alloc(std.Thread, eff);
        errdefer allocator.free(threads);
        const thread_active = try allocator.alloc(bool, eff);
        errdefer allocator.free(thread_active);
        const exhausted = try allocator.alloc(bool, eff);
        errdefer allocator.free(exhausted);
        @memset(exhausted, false);

        const self = try allocator.create(ParallelScan);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .table = table,
            .workers = workers,
            .granted = eff_granted,
            .acct = acct,
            .owns_acct = owns_acct,
            .out_schema = workers[0].outputSchema(),
            .round = round,
            .werr = werr,
            .threads = threads,
            .thread_active = thread_active,
            .exhausted = exhausted,
            .round_cursor = eff, // force a round on the first next()
            .all_done = false,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *ParallelScan) void {
        for (self.workers) |w| w.deinit();
        self.allocator.free(self.workers);
        self.allocator.free(self.round);
        self.allocator.free(self.werr);
        self.allocator.free(self.threads);
        self.allocator.free(self.thread_active);
        self.allocator.free(self.exhausted);
        GlobalSlots.release(self.granted);
        self.table.ddl_lock.unlockShared(self.table.io);
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

    pub fn stats(self: *ParallelScan) exec.PipelineStats {
        return self.workers[0].stats();
    }

    pub fn explain(self: *ParallelScan, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        var buf: [48]u8 = undefined;
        const line = std.fmt.bufPrint(&buf, "ParallelScan (DOP={d})", .{self.workers.len}) catch "ParallelScan";
        try exec.explainLine(out, allocator, depth, line);
        try self.workers[0].explain(out, allocator, depth + 1);
    }

    pub fn next(self: *ParallelScan) !?Batch {
        while (true) {
            // Emit this round's batches in worker order.
            while (self.round_cursor < self.workers.len) {
                const idx = self.round_cursor;
                self.round_cursor += 1;
                if (self.round[idx]) |b| return b;
            }
            if (self.all_done) return null;
            try self.runRound();
        }
    }

    /// Run one `next()` on every live worker concurrently, then stage their
    /// batches for in-order emission.
    fn runRound(self: *ParallelScan) !void {
        const n = self.workers.len;
        for (self.round) |*r| r.* = null;
        for (self.werr) |*e| e.* = null;
        @memset(self.thread_active, false);

        // Spawn the non-exhausted workers 1..n-1; run worker 0 inline.
        var i: usize = 1;
        while (i < n) : (i += 1) {
            if (self.exhausted[i]) continue;
            if (std.Thread.spawn(.{}, runOne, .{ self.workers[i], &self.round[i], &self.werr[i] })) |th| {
                self.threads[i] = th;
                self.thread_active[i] = true;
            } else |_| {
                // Spawn failed (slot/OOM): run it inline rather than drop it.
                runOne(self.workers[i], &self.round[i], &self.werr[i]);
            }
        }
        if (!self.exhausted[0]) runOne(self.workers[0], &self.round[0], &self.werr[0]);

        i = 1;
        while (i < n) : (i += 1) {
            if (self.thread_active[i]) self.threads[i].join();
        }

        // Propagate the first worker error (others' batches are discarded).
        for (self.werr) |e| if (e) |err| return err;

        var all = true;
        for (0..n) |w| {
            if (!self.exhausted[w] and self.round[w] == null) self.exhausted[w] = true;
            if (!self.exhausted[w]) all = false;
        }
        self.all_done = all;
        self.round_cursor = 0;
    }
};

fn runOne(scan: *Scan, out_batch: *?Batch, out_err: *?anyerror) void {
    const r = scan.next() catch |e| {
        out_err.* = e;
        return;
    };
    out_batch.* = r;
}

/// Map a flat row-group index to its `(segment, row_group)` coordinate. `f ==
/// total` (a worker's exclusive end) maps to the past-the-end sentinel.
fn flatToCoord(f: usize, seg_start: []const usize, segment_count: usize, total: usize) Coord {
    if (f >= total) return .{ .seg = segment_count, .rg = 0 };
    var seg: usize = 0;
    while (seg + 1 < segment_count and seg_start[seg + 1] <= f) seg += 1;
    return .{ .seg = seg, .rg = f - seg_start[seg] };
}
