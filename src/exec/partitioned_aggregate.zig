//! Partition-parallel grouped aggregate over a buffered (non-table) input.
//!
//! The radix aggregate parallelises high-card GROUP BY only for integer keys
//! that bit-pack into a u128 and only for fixed-state aggregates; the silo
//! (the string/wide-key parallel path) sources exclusively from a table scan.
//! A grouped aggregate whose input is a materialized CTE buffer with a string
//! key and/or MAX_BY/ANY_VALUE therefore falls to the serial hash aggregate.
//!
//! This operator restores parallelism for that case by partitioning, not
//! combining: it hashes each input row's group key into one of N partitions,
//! then runs an independent serial `Aggregate` over each partition on its own
//! thread. Partitioning by the group key guarantees every row of a group lands
//! in exactly one partition, so the per-partition outputs concatenate with no
//! cross-partition merge — which is what lets it carry ANY aggregate the serial
//! core supports (MAX_BY, ANY_VALUE, COUNT(DISTINCT), …) and any key type.
//!
//! Each partition owns an arena backed by a thread-safe allocator (the workers
//! allocate concurrently); the per-query arena is never touched off-thread.

const std = @import("std");
const Allocator = std.mem.Allocator;

const getenv_pa = @extern(*const fn (name: [*:0]const u8) callconv(.c) ?[*:0]const u8, .{ .name = "getenv", .library_name = "c" });

const exec = @import("exec.zig");
const types = @import("../types.zig");
const engine = @import("../engine/engine.zig");
const storage = @import("../storage/storage.zig");
const aggregate = @import("aggregate.zig");
const ir = @import("../ir/ir.zig");

const Column = types.Column;
const ColumnView = storage.ColumnView;
const Batch = exec.Batch;
const Query = exec.Query;
const AggSpec = ir.AggSpec;

/// Below this realized input row count the threading overhead outweighs the
/// serial hash aggregate, so the router keeps the serial path.
pub const MIN_ROWS_FOR_PARALLEL: u64 = 96 * 1024;

const MAX_PARTS: usize = 32;

/// A partition's gathered input columns (one store per upstream column) plus
/// the serial aggregate's retained output, both living in `arena`.
const Partition = struct {
    arena: std.heap.ArenaAllocator,
    in_cols: []engine.ColumnStore,
    in_rows: usize = 0,
    out_cols: []engine.ColumnStore = &.{},
    out_rows: usize = 0,
    err: ?anyerror = null,
    /// Set once aggregated — partition 0 runs early as the core-selection
    /// probe, and the pooled `.aggregate` phase must not run it twice.
    agg_done: bool = false,
};

/// Single-batch source over a partition's gathered input columns — the serial
/// Aggregate drains it exactly like a scan leaf.
const InputScan = struct {
    schema: []const Column,
    views: []ColumnView,
    rows: usize,
    yielded: bool = false,

    pub fn next(self: *InputScan) !?Batch {
        if (self.yielded) return null;
        self.yielded = true;
        if (self.rows == 0) return null;
        return Batch{ .schema = self.schema, .values = self.views, .row_count = self.rows };
    }
    pub fn deinit(_: *InputScan) void {}
    pub fn outputSchema(self: *InputScan) []const Column {
        return self.schema;
    }
    pub fn addPrune(_: *InputScan, _: exec.Predicate) !void {}
    pub fn stats(self: *InputScan) exec.PipelineStats {
        return .{ .upper_rows = self.rows };
    }
    pub fn accountant(_: *InputScan) ?*exec.memory.MemoryAccountant {
        return null;
    }
    pub fn explain(self: *InputScan, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) !void {
        _ = self;
        try exec.explainLine(out, alloc, depth, "PartitionInput");
    }
};

/// Sorted view over an already-buffered partition. The generic Sort operator
/// would first copy every input column into another full-size accumulation
/// buffer; here the partition stores are stable, so only the permutation and
/// one bounded output batch are needed.
const PermutedInputScan = struct {
    allocator: Allocator,
    schema: []const Column,
    source: []const ColumnView,
    perm: []const u32,
    output: []engine.ColumnStore,
    views: []ColumnView,
    offset: usize = 0,

    const batch_size: usize = 8192;

    fn init(
        allocator: Allocator,
        schema: []const Column,
        source: []const ColumnView,
        perm: []const u32,
    ) !PermutedInputScan {
        const output = try allocator.alloc(engine.ColumnStore, schema.len);
        errdefer allocator.free(output);
        var inited: usize = 0;
        errdefer for (output[0..inited]) |*store| store.deinit(allocator);
        for (schema, output) |sc, *store| {
            store.* = try engine.ColumnStore.init(allocator, sc.type, sc.nullable);
            inited += 1;
        }
        const views = try allocator.alloc(ColumnView, schema.len);
        errdefer allocator.free(views);
        return .{
            .allocator = allocator,
            .schema = schema,
            .source = source,
            .perm = perm,
            .output = output,
            .views = views,
        };
    }

    pub fn next(self: *PermutedInputScan) !?Batch {
        if (self.offset >= self.perm.len) return null;
        const end = @min(self.offset + batch_size, self.perm.len);
        const indices = self.perm[self.offset..end];
        for (self.output, self.source, self.views) |*store, source, *view| {
            store.clear();
            try engine.transform.appendByIndices(self.allocator, source, indices, store);
            view.* = store.view();
        }
        self.offset = end;
        return Batch{ .schema = self.schema, .values = self.views, .row_count = indices.len };
    }

    pub fn deinit(_: *PermutedInputScan) void {}
    pub fn outputSchema(self: *PermutedInputScan) []const Column {
        return self.schema;
    }
    pub fn addPrune(_: *PermutedInputScan, _: exec.Predicate) !void {}
    pub fn stats(self: *PermutedInputScan) exec.PipelineStats {
        return .{ .upper_rows = self.perm.len };
    }
    pub fn accountant(_: *PermutedInputScan) ?*exec.memory.MemoryAccountant {
        return null;
    }
    pub fn explain(_: *PermutedInputScan, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) !void {
        try exec.explainLine(out, alloc, depth, "PermutedPartitionInput");
    }
};

pub const PartitionedAggregate = struct {
    allocator: Allocator,
    up: Query,
    group_cols: []const []const u8,
    group_indices: []const usize,
    aggs: []const AggSpec,
    output_schema: []Column,
    n_parts: usize,
    parts: []Partition,
    ran: bool = false,
    emit_part: usize = 0,
    emit_chunk: usize = 0,
    views: []ColumnView,
    /// Chosen by the partition-0 probe: near-unique groups with heavy
    /// per-group states (MAX_BY/ANY_VALUE/...) make the hash core pay a
    /// heap state + string dupes for EVERY group; sort+stream holds one
    /// live group. Low-NDV shapes keep the hash core — the sort would be
    /// pure loss there.
    sorted_stream_rest: bool = false,

    pub fn create(
        allocator: Allocator,
        up: Query,
        group_cols: []const []const u8,
        aggs: []const AggSpec,
        n_parts_hint: usize,
    ) !Query {
        const up_schema = up.outputSchema();
        const out_schema = try aggregate.outputSchemaFor(allocator, up_schema, group_cols, aggs);
        errdefer allocator.free(out_schema);

        const gci = try allocator.alloc(usize, group_cols.len);
        errdefer allocator.free(gci);
        for (group_cols, gci) |name, *slot| {
            slot.* = types.findColumn(up_schema, name) orelse return error.ColumnNotFound;
        }

        const n_parts = @max(@as(usize, 2), @min(n_parts_hint, MAX_PARTS));
        const parts = try allocator.alloc(Partition, n_parts);
        errdefer allocator.free(parts);
        var inited: usize = 0;
        errdefer for (parts[0..inited]) |*p| p.arena.deinit();
        for (parts) |*p| {
            p.* = .{ .arena = std.heap.ArenaAllocator.init(std.heap.c_allocator), .in_cols = &.{} };
            const aa = p.arena.allocator();
            p.in_cols = try aa.alloc(engine.ColumnStore, up_schema.len);
            for (up_schema, p.in_cols) |sc, *store| {
                store.* = try engine.ColumnStore.init(aa, sc.type, sc.nullable);
            }
            inited += 1;
        }

        const views = try allocator.alloc(ColumnView, out_schema.len);
        errdefer allocator.free(views);

        const self = try allocator.create(PartitionedAggregate);
        self.* = .{
            .allocator = allocator,
            .up = up,
            .group_cols = group_cols,
            .group_indices = gci,
            .aggs = aggs,
            .output_schema = out_schema,
            .n_parts = n_parts,
            .parts = parts,
            .views = views,
        };
        return exec.makeQuery(allocator, self);
    }

    /// murmur3 finalizer — mixes a combined cell hash into the running row hash.
    fn mix64(x0: u64) u64 {
        var x = x0;
        x ^= x >> 33;
        x *%= 0xff51afd7ed558ccd;
        x ^= x >> 33;
        x *%= 0xc4ceb9fe1a85ec53;
        x ^= x >> 33;
        return x;
    }

    /// Fold one key column into every row's partition hash — column-outer so
    /// the type dispatch runs once per column, not once per row. The hash only
    /// routes rows to partitions (equal keys → equal hash is the sole
    /// requirement), so a NULL folds as a fixed sentinel and collisions are
    /// harmless.
    fn hashColumnInto(view: ColumnView, hashes: []u64) void {
        const NULL_SENTINEL: u64 = 0xFFFF_FFFF_FFFF_FFFF;
        switch (view.data) {
            .int, .date => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) @as(u64, @bitCast(@as(i64, v))) else NULL_SENTINEL));
            },
            .bigint, .datetime, .decimal64 => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) @as(u64, @bitCast(v)) else NULL_SENTINEL));
            },
            .boolean => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) @as(u64, v) else NULL_SENTINEL));
            },
            .tinyint => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) @as(u64, @bitCast(@as(i64, v))) else NULL_SENTINEL));
            },
            .smallint => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) @as(u64, @bitCast(@as(i64, v))) else NULL_SENTINEL));
            },
            .float => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) @as(u64, @as(u32, @bitCast(v))) else NULL_SENTINEL));
            },
            .double => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) @as(u64, @bitCast(v)) else NULL_SENTINEL));
            },
            .largeint, .decimal128 => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                const u: u128 = @bitCast(v);
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) (@as(u64, @truncate(u)) ^ @as(u64, @truncate(u >> 64))) else NULL_SENTINEL));
            },
            .uuid => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) (@as(u64, @truncate(v)) ^ @as(u64, @truncate(v >> 64))) else NULL_SENTINEL));
            },
            .varchar, .string, .char, .json => |sv| for (hashes, 0..) |*h, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) std.hash.Wyhash.hash(0, sv.rowBytes(row)) else NULL_SENTINEL));
            },
        }
    }

    /// Two-phase worker pool: one worker per partition (its arena stays
    /// thread-exclusive across both phases).
    ///
    ///   .copy      — the conn thread pulled a batch and bucketed its row
    ///                indices per partition; every worker bulk-copies ITS
    ///                partition's rows out of the (batch-lifetime) views.
    ///                Barrier per batch — the views die at the next pull.
    ///   .aggregate — after the last batch, each worker runs the serial
    ///                Aggregate over its gathered partition.
    ///
    /// The conn thread participates as partition 0's worker in both phases.
    const Pool = struct {
        owner: *PartitionedAggregate,
        batch: Batch = undefined,
        idx_lists: []std.ArrayListUnmanaged(u32) = &.{},
        mode: Mode = .copy,
        gen: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        done: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        parked: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

        const Mode = enum { copy, aggregate };

        fn workerMain(pool: *Pool, part_idx: usize) void {
            var seen: usize = 0;
            while (true) {
                var spins: usize = 0;
                while (pool.gen.load(.acquire) == seen) {
                    if (pool.stop.load(.acquire)) return;
                    spins += 1;
                    if (spins < 4096) std.atomic.spinLoopHint() else std.Thread.yield() catch std.atomic.spinLoopHint();
                }
                seen = pool.gen.load(.acquire);
                _ = pool.parked.fetchSub(1, .acq_rel);
                pool.runPhase(part_idx);
                _ = pool.done.fetchAdd(1, .acq_rel);
                _ = pool.parked.fetchAdd(1, .acq_rel);
            }
        }

        fn runPhase(pool: *Pool, part_idx: usize) void {
            const self = pool.owner;
            const part = &self.parts[part_idx];
            switch (pool.mode) {
                .copy => copyPartition(self, part, pool.batch, pool.idx_lists[part_idx].items) catch |e| {
                    part.err = e;
                },
                .aggregate => self.aggregateOne(part),
            }
        }

        /// Publish a phase, work partition 0 on this thread, wait for every
        /// unit done AND every worker re-parked (cycles never overlap).
        fn runBarrier(pool: *Pool, mode: Mode) void {
            const self = pool.owner;
            pool.mode = mode;
            pool.done.store(0, .release);
            _ = pool.gen.fetchAdd(1, .release);
            pool.runPhase(0);
            const want = self.n_parts - 1;
            var spins: usize = 0;
            while (pool.done.load(.acquire) < want or pool.parked.load(.acquire) < want) {
                spins += 1;
                if (spins < 4096) std.atomic.spinLoopHint() else std.Thread.yield() catch {};
            }
        }
    };

    /// Each partition's arena holds its gathered input plus retained output —
    /// hundreds of MB total on multi-million-row groups, and OS page release
    /// dominates teardown. The arenas are independent and exclusively owned
    /// here, so free them concurrently.
    fn freeArenasParallel(parts: []Partition) void {
        if (parts.len < 2) {
            for (parts) |*p| p.arena.deinit();
            return;
        }
        var threads: [MAX_PARTS]?std.Thread = .{null} ** MAX_PARTS;
        for (parts[1..], 1..) |*p, i| {
            threads[i] = std.Thread.spawn(.{}, freeOneArena, .{p}) catch null;
            if (threads[i] == null) p.arena.deinit();
        }
        parts[0].arena.deinit();
        for (threads[1..parts.len]) |maybe| if (maybe) |th| th.join();
    }

    fn freeOneArena(part: *Partition) void {
        part.arena.deinit();
    }

    fn copyPartition(_: *PartitionedAggregate, part: *Partition, batch: Batch, indices: []const u32) !void {
        if (indices.len == 0) return;
        const aa = part.arena.allocator();
        for (part.in_cols, 0..) |*store, ci| {
            try engine.transform.appendByIndices(aa, batch.values[ci], indices, store);
        }
        part.in_rows += indices.len;
    }

    /// Aggregates whose per-group state lives on the heap (a `Value` copy or
    /// worse). Near-unique group counts multiply that cost by the row count —
    /// the trigger for the sort+stream core.
    fn heavyStateAggCount(aggs: []const AggSpec) usize {
        var n: usize = 0;
        for (aggs) |a| switch (a.func) {
            .max_by, .max_by_key, .any_value, .first, .last, .group_concat, .count_distinct, .sum_distinct, .avg_distinct => n += 1,
            else => {},
        };
        return n;
    }

    /// Phase B (one worker per partition): run the serial Aggregate over the
    /// partition's gathered input and retain its output rows in the same arena.
    fn aggregateOne(self: *PartitionedAggregate, part: *Partition) void {
        if (part.agg_done) return;
        part.agg_done = true;
        runPartition(self, part) catch |e| {
            part.err = e;
        };
    }

    fn runPartition(self: *PartitionedAggregate, part: *Partition) !void {
        const aa = part.arena.allocator();
        const up_schema = self.up.outputSchema();

        const in_views = try aa.alloc(ColumnView, up_schema.len);
        for (part.in_cols, in_views) |*store, *v| v.* = store.view();
        var scan = InputScan{ .schema = up_schema, .views = in_views, .rows = part.in_rows };
        var permuted_scan: PermutedInputScan = undefined;

        // These columns are already stable in the partition arena, so sort a
        // row permutation directly. The environment switch retains the prior
        // generic Sort path as an operational fallback.
        var agg = if (self.sorted_stream_rest and getenv_pa("THINDB_PAGG_BUFFERED_SORT") == null) blk: {
            const perm = try aa.alloc(u32, part.in_rows);
            for (perm, 0..) |*row, i| row.* = @intCast(i);
            const SortCtx = struct {
                columns: []const engine.ColumnStore,
                indices: []const usize,

                fn lessThan(ctx: @This(), a: u32, b: u32) bool {
                    for (ctx.indices) |ci| {
                        const order = engine.transform.compareInColumnNullsFirst(ctx.columns[ci], a, b);
                        if (order == .lt) return true;
                        if (order == .gt) return false;
                    }
                    return false;
                }
            };
            std.sort.pdq(u32, perm, SortCtx{
                .columns = part.in_cols,
                .indices = self.group_indices,
            }, SortCtx.lessThan);
            permuted_scan = try PermutedInputScan.init(aa, up_schema, in_views, perm);
            const src = exec.makeQuery(aa, &permuted_scan);
            break :blk try src.streamGroupBy(self.group_cols, self.aggs);
        } else if (self.sorted_stream_rest) blk: {
            // Near-unique groups (probe verdict): sort this partition by the
            // group keys and stream — one live group's state instead of a
            // hash table holding a heap state per group.
            const specs = try aa.alloc(exec.SortSpec, self.group_cols.len);
            for (self.group_cols, specs) |gc, *s| s.* = .{ .col = gc, .desc = false };
            const src = exec.makeQuery(aa, &scan);
            const sorted = try src.orderBy(specs);
            break :blk try sorted.streamGroupBy(self.group_cols, self.aggs);
        } else blk: {
            const src = exec.makeQuery(aa, &scan);
            break :blk try aggregate.Aggregate.create(aa, src, self.group_cols, self.aggs, null, null);
        };
        defer agg.deinit();

        const out_cols = try aa.alloc(engine.ColumnStore, self.output_schema.len);
        for (self.output_schema, out_cols) |sc, *store| {
            store.* = try engine.ColumnStore.init(aa, sc.type, sc.nullable);
        }
        var out_rows: usize = 0;
        while (try agg.next()) |b| {
            for (out_cols, 0..) |*store, ci| {
                try engine.transform.appendColumnRange(aa, b.values[ci], 0, b.row_count, store);
            }
            out_rows += b.row_count;
        }
        part.out_cols = out_cols;
        part.out_rows = out_rows;
    }

    fn run(self: *PartitionedAggregate) !void {
        const prof_on = exec.prof.enabled;
        const t0 = if (prof_on) exec.prof.nowTicks() else 0;

        var pool = Pool{ .owner = self };
        const idx_lists = try self.allocator.alloc(std.ArrayListUnmanaged(u32), self.n_parts);
        defer {
            for (idx_lists) |*l| l.deinit(self.allocator);
            self.allocator.free(idx_lists);
        }
        for (idx_lists) |*l| l.* = .empty;
        pool.idx_lists = idx_lists;
        var hashes: std.ArrayListUnmanaged(u64) = .empty;
        defer hashes.deinit(self.allocator);

        var threads: [MAX_PARTS]?std.Thread = .{null} ** MAX_PARTS;
        var spawned: usize = 0;
        pool.parked.store(self.n_parts - 1, .release);
        {
            var t: usize = 1;
            while (t < self.n_parts) : (t += 1) {
                threads[t] = std.Thread.spawn(.{}, Pool.workerMain, .{ &pool, t }) catch null;
                if (threads[t] != null) spawned += 1;
            }
        }
        defer {
            pool.stop.store(true, .release);
            for (threads[1..self.n_parts]) |maybe| if (maybe) |th| th.join();
        }
        // Spawn failures degrade by folding those partitions into the conn
        // thread's phase work.
        const spawn_ok = spawned == self.n_parts - 1;

        var pull_ticks: i64 = 0;
        var hash_ticks: i64 = 0;
        var copy_ticks: i64 = 0;
        while (true) {
            const p0 = if (prof_on) exec.prof.nowTicks() else 0;
            const maybe = try self.up.next();
            if (prof_on) pull_ticks += exec.prof.nowTicks() - p0;
            const batch = maybe orelse break;
            const h0 = if (prof_on) exec.prof.nowTicks() else 0;
            try hashes.resize(self.allocator, batch.row_count);
            @memset(hashes.items, 0x9e3779b97f4a7c15);
            for (self.group_indices) |ci| hashColumnInto(batch.values[ci], hashes.items);
            for (idx_lists) |*l| l.clearRetainingCapacity();
            for (hashes.items, 0..) |h, row| {
                try idx_lists[h % self.n_parts].append(self.allocator, @intCast(row));
            }
            const c0 = if (prof_on) exec.prof.nowTicks() else 0;
            if (prof_on) hash_ticks += c0 - h0;
            pool.batch = batch;
            if (spawn_ok) {
                pool.runBarrier(.copy);
            } else {
                for (self.parts, 0..) |*part, pi| try copyPartition(self, part, batch, idx_lists[pi].items);
            }
            if (prof_on) copy_ticks += exec.prof.nowTicks() - c0;
        }
        const t1 = if (prof_on) exec.prof.nowTicks() else 0;

        // Core-selection probe: aggregate partition 0 on this thread with the
        // hash core, then read its groups/rows ratio. Near-unique groups with
        // heavy per-group states send the REMAINING partitions down the
        // sort+stream core (partition 0 keeps its hash result — same output
        // either way, only the core differs).
        self.aggregateOne(&self.parts[0]);
        // ≥90% unique: at moderate ratios (measured: 73% unique, 3.6M rows)
        // the hash core still wins — the sort's row-bound cost only pays off
        // when nearly every row opens a fresh group's heap states.
        if (self.parts[0].in_rows >= 4096 and
            self.parts[0].out_rows * 10 >= self.parts[0].in_rows * 9 and
            heavyStateAggCount(self.aggs) >= 2)
        {
            self.sorted_stream_rest = true;
            if (prof_on) std.debug.print(
                "[hprof] pagg.probe: groups/rows={d}/{d} heavy_aggs={d} -> sort+stream rest\n",
                .{ self.parts[0].out_rows, self.parts[0].in_rows, heavyStateAggCount(self.aggs) },
            );
        }

        if (spawn_ok) {
            pool.runBarrier(.aggregate);
        } else {
            for (self.parts) |*part| self.aggregateOne(part);
        }
        if (prof_on) {
            const t2 = exec.prof.nowTicks();
            var rows: u64 = 0;
            for (self.parts) |*p| rows += p.in_rows;
            std.debug.print("[hprof] pagg.scatter {d:.2} ms (pull={d:.2} hash+idx={d:.2} copy={d:.2})  pagg.partitions {d:.2} ms  (parts={d} rows={d})\n", .{
                exec.prof.ticksToMs(t1 - t0),
                exec.prof.ticksToMs(pull_ticks),
                exec.prof.ticksToMs(hash_ticks),
                exec.prof.ticksToMs(copy_ticks),
                exec.prof.ticksToMs(t2 - t1),
                self.n_parts,
                rows,
            });
        }

        for (self.parts) |*p| if (p.err) |e| return e;
        self.ran = true;
    }

    pub fn next(self: *PartitionedAggregate) !?Batch {
        if (!self.ran) try self.run();
        while (self.emit_part < self.n_parts) {
            const part = &self.parts[self.emit_part];
            if (self.emit_chunk == 0 and part.out_rows > 0) {
                for (part.out_cols, self.views) |*store, *v| v.* = store.view();
                self.emit_chunk = 1;
                return Batch{ .schema = self.output_schema, .values = self.views, .row_count = part.out_rows };
            }
            self.emit_part += 1;
            self.emit_chunk = 0;
        }
        return null;
    }

    pub fn outputSchema(self: *PartitionedAggregate) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(_: *PartitionedAggregate, _: exec.Predicate) !void {}

    pub fn stats(self: *PartitionedAggregate) exec.PipelineStats {
        return .{ .upper_rows = self.up.stats().upper_rows };
    }

    pub fn accountant(self: *PartitionedAggregate) ?*exec.memory.MemoryAccountant {
        return self.up.accountant();
    }

    pub fn explain(self: *PartitionedAggregate, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) !void {
        var buf: [80]u8 = undefined;
        const line = std.fmt.bufPrint(&buf, "PartitionedAggregate parts={d}", .{self.n_parts}) catch "PartitionedAggregate";
        try exec.explainLine(out, alloc, depth, line);
        try self.up.explain(out, alloc, depth + 1);
    }

    pub fn deinit(self: *PartitionedAggregate) void {
        self.up.deinit();
        freeArenasParallel(self.parts);
        self.allocator.free(self.parts);
        self.allocator.free(self.group_indices);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.views);
        self.allocator.destroy(self);
    }
};

const testing = std.testing;
const engine_store = engine.ColumnStore;

fn testReadI128(v: ColumnView, row: usize) i128 {
    return switch (v.data) {
        .int => |s| s[row],
        .bigint => |s| s[row],
        .largeint => |s| s[row],
        .smallint => |s| s[row],
        .tinyint => |s| s[row],
        else => unreachable,
    };
}

/// Drain a grouped query into `key|count|sum|maxby` lines (one per group),
/// sorted — the partition-parallel and serial aggregates emit the same groups
/// in different orders, so compare the canonicalized set.
fn testCollectSorted(allocator: Allocator, q: *Query) ![][]u8 {
    var lines: std.ArrayListUnmanaged([]u8) = .empty;
    errdefer {
        for (lines.items) |l| allocator.free(l);
        lines.deinit(allocator);
    }
    while (try q.next()) |b| {
        var row: usize = 0;
        while (row < b.row_count) : (row += 1) {
            const key = b.values[0].data.string.rowBytes(@intCast(row));
            const cnt = testReadI128(b.values[1], row);
            const sm = testReadI128(b.values[2], row);
            const mb = b.values[3].data.string.rowBytes(@intCast(row));
            const line = try std.fmt.allocPrint(allocator, "{s}|{d}|{d}|{s}", .{ key, cnt, sm, mb });
            try lines.append(allocator, line);
        }
    }
    std.mem.sort([]u8, lines.items, {}, struct {
        fn lt(_: void, a: []u8, b: []u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
    return lines.toOwnedSlice(allocator);
}

test "PartitionedAggregate matches serial aggregate on string key + MAX_BY" {
    const a = testing.allocator;
    const N = 4000;

    const schema = [_]Column{
        .{ .name = "key", .type = .string, .nullable = false },
        .{ .name = "val", .type = .bigint, .nullable = false },
        .{ .name = "ord", .type = .bigint, .nullable = false },
        .{ .name = "label", .type = .string, .nullable = false },
    };
    var stores: [4]engine_store = undefined;
    for (&stores, schema) |*s, col| s.* = try engine_store.init(a, col.type, col.nullable);
    defer for (&stores) |*s| s.deinit(a);

    var i: usize = 0;
    while (i < N) : (i += 1) {
        var kb: [8]u8 = undefined;
        const k = try std.fmt.bufPrint(&kb, "g{d}", .{i % 7});
        try stores[0].data.string.appendValue(a, k);
        try stores[1].data.bigint.append(a, @intCast(i));
        try stores[2].data.bigint.append(a, @intCast((i * 31) % N));
        var lb: [8]u8 = undefined;
        const lbl = try std.fmt.bufPrint(&lb, "L{d}", .{i % 13});
        try stores[3].data.string.appendValue(a, lbl);
    }

    var views: [4]ColumnView = undefined;
    for (&views, &stores) |*v, *s| v.* = s.view();
    const group_cols = [_][]const u8{"key"};
    const aggs = [_]AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "val", .as = "s" },
        .{ .func = .max_by, .col = "label", .arg2_col = "ord", .as = "mb" },
    };

    var scan_p = InputScan{ .schema = &schema, .views = &views, .rows = N };
    var pa = try PartitionedAggregate.create(a, exec.makeQuery(a, &scan_p), &group_cols, &aggs, 4);
    const par_lines = try testCollectSorted(a, &pa);
    defer {
        for (par_lines) |l| a.free(l);
        a.free(par_lines);
    }
    pa.deinit();

    var scan_s = InputScan{ .schema = &schema, .views = &views, .rows = N };
    var ser = try @import("aggregate.zig").Aggregate.create(a, exec.makeQuery(a, &scan_s), &group_cols, &aggs, null, null);
    const ser_lines = try testCollectSorted(a, &ser);
    defer {
        for (ser_lines) |l| a.free(l);
        a.free(ser_lines);
    }
    ser.deinit();

    try testing.expectEqual(@as(usize, 7), ser_lines.len);
    try testing.expectEqual(ser_lines.len, par_lines.len);
    for (par_lines, ser_lines) |p, s| try testing.expectEqualStrings(s, p);
}

test "PartitionedAggregate near-unique direct sort matches serial aggregate" {
    const a = testing.allocator;
    const row_count = 20_000;
    const group_count = 19_000;

    const schema = [_]Column{
        .{ .name = "key", .type = .string, .nullable = false },
        .{ .name = "val", .type = .bigint, .nullable = false },
        .{ .name = "ord", .type = .bigint, .nullable = false },
        .{ .name = "label", .type = .string, .nullable = false },
    };
    var stores: [4]engine_store = undefined;
    for (&stores, schema) |*s, col| s.* = try engine_store.init(a, col.type, col.nullable);
    defer for (&stores) |*s| s.deinit(a);

    for (0..row_count) |i| {
        const group_id = i % group_count;
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "g{d}", .{group_id});
        try stores[0].data.string.appendValue(a, key);
        try stores[1].data.bigint.append(a, @intCast(group_id));
        try stores[2].data.bigint.append(a, @intCast(i));
        var label_buf: [16]u8 = undefined;
        const label = try std.fmt.bufPrint(&label_buf, "L{d}", .{i});
        try stores[3].data.string.appendValue(a, label);
    }

    var views: [4]ColumnView = undefined;
    for (&views, &stores) |*v, *s| v.* = s.view();
    const group_cols = [_][]const u8{"key"};
    const aggs = [_]AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .any_value, .col = "val", .as = "v" },
        .{ .func = .max_by, .col = "label", .arg2_col = "ord", .as = "mb" },
    };

    var scan_p = InputScan{ .schema = &schema, .views = &views, .rows = row_count };
    var pa = try PartitionedAggregate.create(a, exec.makeQuery(a, &scan_p), &group_cols, &aggs, 4);
    const par_lines = try testCollectSorted(a, &pa);
    defer {
        for (par_lines) |line| a.free(line);
        a.free(par_lines);
    }
    pa.deinit();

    var scan_s = InputScan{ .schema = &schema, .views = &views, .rows = row_count };
    var ser = try aggregate.Aggregate.create(a, exec.makeQuery(a, &scan_s), &group_cols, &aggs, null, null);
    const ser_lines = try testCollectSorted(a, &ser);
    defer {
        for (ser_lines) |line| a.free(line);
        a.free(ser_lines);
    }
    ser.deinit();

    try testing.expectEqual(@as(usize, group_count), par_lines.len);
    try testing.expectEqual(ser_lines.len, par_lines.len);
    for (par_lines, ser_lines) |p, s| try testing.expectEqualStrings(s, p);
}

test "two-phase max_by via max_by_key partials matches single-phase (NULL-value trap)" {
    const a = testing.allocator;
    const Aggregate = @import("aggregate.zig").Aggregate;

    const schema = [_]Column{
        .{ .name = "key", .type = .string, .nullable = false },
        .{ .name = "val", .type = .string, .nullable = true },
        .{ .name = "ord", .type = .bigint, .nullable = true },
    };

    // Chunk A carries g0's HIGHEST ord on a NULL-value row — a naive hidden
    // MAX(ord) would carry ord 99 alongside chunk A's value "early" and beat
    // chunk B's honest ("winner", 50) in the combine. max_by_key shares
    // max_by's skip-if-either-NULL pair semantics, so A's pair is (early, 10).
    const Row = struct { k: []const u8, v: ?[]const u8, o: ?i64 };
    const chunk_a = [_]Row{
        .{ .k = "g0", .v = "early", .o = 10 },
        .{ .k = "g0", .v = null, .o = 99 },
        .{ .k = "g1", .v = "a", .o = 5 },
        .{ .k = "g1", .v = "z", .o = null },
    };
    const chunk_b = [_]Row{
        .{ .k = "g0", .v = "winner", .o = 50 },
        .{ .k = "g0", .v = "low", .o = 1 },
        .{ .k = "g1", .v = "b", .o = 7 },
        .{ .k = "g2", .v = "solo", .o = 3 },
    };

    const fillStores = struct {
        fn fill(alloc: Allocator, stores: []engine_store, rows: []const Row) !void {
            for (rows) |r| {
                try stores[0].data.string.appendValue(alloc, r.k);
                if (r.v) |v| {
                    const row = stores[1].rowCount();
                    try stores[1].data.string.appendValue(alloc, v);
                    try stores[1].appendValidBit(alloc, row, true);
                } else try stores[1].appendNulls(alloc, 1);
                if (r.o) |o| {
                    const row = stores[2].rowCount();
                    try stores[2].data.bigint.append(alloc, o);
                    try stores[2].appendValidBit(alloc, row, true);
                } else try stores[2].appendNulls(alloc, 1);
            }
        }
    }.fill;

    const part_aggs = [_]AggSpec{
        .{ .func = .max_by, .col = "val", .arg2_col = "ord", .as = "v" },
        .{ .func = .max_by_key, .col = "val", .arg2_col = "ord", .as = "__o", .out_type_override = .bigint },
    };
    const group_cols = [_][]const u8{"key"};

    // Phase 1: partial per chunk, appended into the combine input stores.
    const part_schema = [_]Column{
        .{ .name = "key", .type = .string, .nullable = false },
        .{ .name = "v", .type = .string, .nullable = true },
        .{ .name = "__o", .type = .bigint, .nullable = true },
    };
    var part_stores: [3]engine_store = undefined;
    for (&part_stores, part_schema) |*s, col| s.* = try engine_store.init(a, col.type, col.nullable);
    defer for (&part_stores) |*s| s.deinit(a);

    inline for (.{ chunk_a[0..], chunk_b[0..] }) |rows| {
        var stores: [3]engine_store = undefined;
        for (&stores, schema) |*s, col| s.* = try engine_store.init(a, col.type, col.nullable);
        defer for (&stores) |*s| s.deinit(a);
        try fillStores(a, &stores, rows);
        var views: [3]ColumnView = undefined;
        for (&views, &stores) |*v, *s| v.* = s.view();
        var scan = InputScan{ .schema = &schema, .views = &views, .rows = rows.len };
        var agg = try Aggregate.create(a, exec.makeQuery(a, &scan), &group_cols, &part_aggs, null, null);
        defer agg.deinit();
        while (try agg.next()) |b| {
            var row: usize = 0;
            while (row < b.row_count) : (row += 1) {
                try part_stores[0].data.string.appendValue(a, b.values[0].data.string.rowBytes(@intCast(row)));
                if (b.values[1].isValid(@intCast(row))) {
                    const pr = part_stores[1].rowCount();
                    try part_stores[1].data.string.appendValue(a, b.values[1].data.string.rowBytes(@intCast(row)));
                    try part_stores[1].appendValidBit(a, pr, true);
                } else try part_stores[1].appendNulls(a, 1);
                if (b.values[2].isValid(@intCast(row))) {
                    const pr = part_stores[2].rowCount();
                    try part_stores[2].data.bigint.append(a, b.values[2].data.bigint[row]);
                    try part_stores[2].appendValidBit(a, pr, true);
                } else try part_stores[2].appendNulls(a, 1);
            }
        }
    }

    const collect = struct {
        fn run(alloc: Allocator, q: *Query) ![][]u8 {
            var lines: std.ArrayList([]u8) = .empty;
            defer lines.deinit(alloc);
            while (try q.next()) |b| {
                var row: usize = 0;
                while (row < b.row_count) : (row += 1) {
                    const key = b.values[0].data.string.rowBytes(@intCast(row));
                    const v = if (b.values[1].isValid(@intCast(row))) b.values[1].data.string.rowBytes(@intCast(row)) else "<NULL>";
                    try lines.append(alloc, try std.fmt.allocPrint(alloc, "{s}|{s}", .{ key, v }));
                }
            }
            std.mem.sort([]u8, lines.items, {}, struct {
                fn lt(_: void, x: []u8, y: []u8) bool {
                    return std.mem.lessThan(u8, x, y);
                }
            }.lt);
            return lines.toOwnedSlice(alloc);
        }
    }.run;

    // Phase 2: combine over the concatenated partials.
    const combine_aggs = [_]AggSpec{
        .{ .func = .max_by, .col = "v", .arg2_col = "__o", .as = "v" },
    };
    var part_views: [3]ColumnView = undefined;
    for (&part_views, &part_stores) |*v, *s| v.* = s.view();
    var part_scan = InputScan{ .schema = &part_schema, .views = &part_views, .rows = part_stores[0].rowCount() };
    var comb = try Aggregate.create(a, exec.makeQuery(a, &part_scan), &group_cols, &combine_aggs, null, null);
    const two_phase = try collect(a, &comb);
    defer {
        for (two_phase) |l| a.free(l);
        a.free(two_phase);
    }
    comb.deinit();

    // Single-phase truth over all rows.
    var all_stores: [3]engine_store = undefined;
    for (&all_stores, schema) |*s, col| s.* = try engine_store.init(a, col.type, col.nullable);
    defer for (&all_stores) |*s| s.deinit(a);
    try fillStores(a, &all_stores, chunk_a[0..]);
    try fillStores(a, &all_stores, chunk_b[0..]);
    var all_views: [3]ColumnView = undefined;
    for (&all_views, &all_stores) |*v, *s| v.* = s.view();
    var all_scan = InputScan{ .schema = &schema, .views = &all_views, .rows = chunk_a.len + chunk_b.len };
    var single = try Aggregate.create(a, exec.makeQuery(a, &all_scan), &group_cols, &[_]AggSpec{
        .{ .func = .max_by, .col = "val", .arg2_col = "ord", .as = "v" },
    }, null, null);
    const one_phase = try collect(a, &single);
    defer {
        for (one_phase) |l| a.free(l);
        a.free(one_phase);
    }
    single.deinit();

    try testing.expectEqual(@as(usize, 3), one_phase.len);
    try testing.expectEqual(one_phase.len, two_phase.len);
    for (two_phase, one_phase) |t, s| try testing.expectEqualStrings(s, t);
    try testing.expectEqualStrings("g0|winner", two_phase[0]);
}
