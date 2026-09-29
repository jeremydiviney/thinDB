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
//! The input is buffered once, at its exact size: every upstream batch is
//! split into contiguous slices, and each scatter worker copies its slice into
//! a chunk and buckets the chunk's rows by partition. A partition reads its
//! rows from every chunk in input order, and the last partition to read a
//! chunk frees it, so the buffer shrinks while the partitions aggregate. The
//! charge is the raw input bytes plus four bytes a row. Growing one store per
//! partition and column instead strands every outgrown buffer in an arena,
//! which charged 7-8x the raw bytes (issue #380).
//!
//! Chunks, outputs and each partition's operators come from the thread-safe
//! worker allocator (the workers allocate concurrently); the per-query
//! allocator is never touched off-thread.

const std = @import("std");
const Allocator = std.mem.Allocator;

const getenv_pa = @extern(*const fn (name: [*:0]const u8) callconv(.c) ?[*:0]const u8, .{ .name = "getenv", .library_name = "c" });

const exec = @import("exec.zig");
const types = @import("../types.zig");
const engine = @import("../engine/engine.zig");
const storage = @import("../storage/storage.zig");
const aggregate = @import("aggregate.zig");
const ir = @import("../ir/ir.zig");
const hll = @import("../util/hll.zig");

const Column = types.Column;
const ColumnView = storage.ColumnView;
const Batch = exec.Batch;
const Query = exec.Query;
const AggSpec = ir.AggSpec;

/// Below this realized input row count the threading overhead outweighs the
/// serial hash aggregate, so the router keeps the serial path.
pub const MIN_ROWS_FOR_PARALLEL: u64 = 96 * 1024;

const MAX_PARTS: usize = 32;

/// A batch is split into at most one slice per this many rows: each slice
/// becomes a chunk with a few allocations of its own.
const MIN_SLICE_ROWS: usize = 8192;

/// One scatter worker's contiguous slice of an upstream batch, copied at its
/// exact size. `index` holds the slice's row numbers grouped by partition,
/// followed by the `n_parts + 1` group bounds; a counting sort fills it, so
/// each partition's rows keep their input order.
const Chunk = struct {
    cols: []engine.ColumnStore,
    index: []u32,
    rows: usize,
    /// Partitions that have not read their rows yet; the last one frees the
    /// chunk.
    readers: std.atomic.Value(usize),

    fn rowsOf(chunk: *const Chunk, part_idx: usize) []const u32 {
        const bounds = chunk.index[chunk.rows..];
        return chunk.index[bounds[part_idx]..bounds[part_idx + 1]];
    }

    fn destroy(chunk: *Chunk, allocator: Allocator) void {
        freeStores(allocator, chunk.cols);
        allocator.free(chunk.index);
        allocator.destroy(chunk);
    }
};

fn freeStores(allocator: Allocator, stores: []engine.ColumnStore) void {
    for (stores) |*store| store.deinit(allocator);
    allocator.free(stores);
}

fn initStores(allocator: Allocator, schema: []const Column) ![]engine.ColumnStore {
    const stores = try allocator.alloc(engine.ColumnStore, schema.len);
    errdefer allocator.free(stores);
    var inited: usize = 0;
    errdefer for (stores[0..inited]) |*store| store.deinit(allocator);
    for (schema, stores) |sc, *store| {
        store.* = try engine.ColumnStore.init(allocator, sc.type, sc.nullable);
        inited += 1;
    }
    return stores;
}

fn stringBytesInRange(view: ColumnView, lo: usize, hi: usize) usize {
    return switch (view.data) {
        .varchar, .string, .char, .json => |sv| sv.offsets[hi] - sv.offsets[lo],
        else => 0,
    };
}

/// The bytes a view's first `rows` rows occupy, validity aside.
fn viewBytes(view: ColumnView, rows: usize) usize {
    return switch (view.data) {
        .varchar, .string, .char, .json => |sv| sv.offsets[rows] - sv.offsets[0] + rows * @sizeOf(u32),
        inline else => |values| rows * @sizeOf(std.meta.Elem(@TypeOf(values))),
    };
}

fn stringBytesAt(view: ColumnView, rows: []const u32) usize {
    return switch (view.data) {
        .varchar, .string, .char, .json => |sv| blk: {
            var total: usize = 0;
            for (rows) |row| total += sv.offsets[row + 1] - sv.offsets[row];
            break :blk total;
        },
        else => 0,
    };
}

/// A scatter worker's scratch, reused across batches.
const Worker = struct {
    hashes: std.ArrayListUnmanaged(u64) = .empty,
    ndv: hll.Hll = .{},
    chunk: ?*Chunk = null,
    err: ?anyerror = null,
};

/// A partition's read position over the chunks and the serial aggregate's
/// retained output.
const Partition = struct {
    /// Next chunk to read; every chunk before it has been released.
    cursor: usize = 0,
    out_cols: []engine.ColumnStore = &.{},
    out_rows: usize = 0,
    err: ?anyerror = null,
};

/// Source over one partition's rows across every chunk, in input order. The
/// Aggregate sizes each batch for its worst case (every row a new group, every
/// value a new distinct pair), so one partition-sized batch made a 93M-row
/// partition with five groups reserve state for 134M groups (issue #375); the
/// rows therefore arrive in scan-sized windows. Each chunk is released once
/// its rows are copied out.
const ChunkScan = struct {
    owner: *PartitionedAggregate,
    part_idx: usize,
    allocator: Allocator,
    window: []engine.ColumnStore,
    views: []ColumnView,
    /// Rows of the cursor chunk already copied out.
    taken: usize = 0,

    const batch_rows: usize = 64 * 1024;

    fn init(allocator: Allocator, owner: *PartitionedAggregate, part_idx: usize) !ChunkScan {
        const schema = owner.up.outputSchema();
        const window = try initStores(allocator, schema);
        errdefer freeStores(allocator, window);
        for (window) |*store| try store.reserveTotal(allocator, batch_rows, 0);
        const views = try allocator.alloc(ColumnView, schema.len);
        return .{
            .owner = owner,
            .part_idx = part_idx,
            .allocator = allocator,
            .window = window,
            .views = views,
        };
    }

    pub fn next(self: *ChunkScan) !?Batch {
        const owner = self.owner;
        const part = &owner.parts[self.part_idx];
        for (self.window) |*store| store.clear();
        var filled: usize = 0;
        while (filled < batch_rows and part.cursor < owner.chunks.items.len) {
            const chunk = owner.chunks.items[part.cursor].?;
            const rows = chunk.rowsOf(self.part_idx);
            const take = @min(rows.len - self.taken, batch_rows - filled);
            if (take > 0) {
                const indices = rows[self.taken..][0..take];
                for (self.window, chunk.cols) |*store, *col| {
                    try engine.transform.appendByIndices(self.allocator, col.view(), indices, store);
                }
                filled += take;
                self.taken += take;
            }
            if (self.taken == rows.len) {
                owner.releaseChunk(part.cursor);
                part.cursor += 1;
                self.taken = 0;
            }
        }
        if (filled == 0) return null;
        for (self.window, self.views) |*store, *view| view.* = store.view();
        return Batch{ .schema = owner.up.outputSchema(), .values = self.views, .row_count = filled };
    }
    /// Idempotent: the operator above deinits its upstream, and the partition
    /// run deinits the scan again in case that operator's create failed.
    pub fn deinit(self: *ChunkScan) void {
        freeStores(self.allocator, self.window);
        self.allocator.free(self.views);
        self.window = &.{};
        self.views = &.{};
    }
    pub fn outputSchema(self: *ChunkScan) []const Column {
        return self.owner.up.outputSchema();
    }
    pub fn addPrune(_: *ChunkScan, _: exec.Predicate) !void {}
    pub fn stats(self: *ChunkScan) exec.PipelineStats {
        return .{ .upper_rows = self.owner.partitionRows(self.part_idx) };
    }
    pub fn accountant(_: *ChunkScan) ?*exec.memory.MemoryAccountant {
        return null;
    }
    pub fn explain(_: *ChunkScan, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) !void {
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
        const output = try initStores(allocator, schema);
        errdefer freeStores(allocator, output);
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

    /// Output stores are recycled every call — the previous batch's views die
    /// on the next `next()` (consume-before-next contract), unlike the old
    /// Sort path whose accumulated batches stayed live for the whole drain.
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

    /// Idempotent, as `ChunkScan.deinit`.
    pub fn deinit(self: *PermutedInputScan) void {
        freeStores(self.allocator, self.output);
        self.allocator.free(self.views);
        self.output = &.{};
        self.views = &.{};
    }
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
    /// Thread-safe and charged to the query: chunks, outputs, and each
    /// partition's operators.
    worker_alloc: Allocator,
    up: Query,
    group_cols: []const []const u8,
    group_indices: []const usize,
    aggs: []const AggSpec,
    output_schema: []Column,
    n_parts: usize,
    parts: []Partition,
    workers: []Worker,
    /// Every chunk in input order; a slot goes null once its last reader
    /// frees the chunk.
    chunks: std.ArrayListUnmanaged(?*Chunk) = .empty,
    ran: bool = false,
    emit_part: usize = 0,
    emit_chunk: usize = 0,
    views: []ColumnView,
    /// The scatter feeds each row's key hash to a distinct-key estimate that
    /// picks the core; only heavy per-group states make the choice matter.
    track_ndv: bool = false,
    /// Chosen from a distinct-key estimate over the scatter hashes:
    /// near-unique groups with heavy per-group states (MAX_BY/ANY_VALUE/...)
    /// make the hash core pay a heap state + string dupes for EVERY group;
    /// sort+stream holds one live group. Low-NDV shapes keep the hash core —
    /// the sort would be pure loss there.
    sorted_stream: bool = false,

    pub fn create(
        allocator: Allocator,
        worker_alloc: Allocator,
        up: Query,
        group_cols: []const []const u8,
        aggs: []const AggSpec,
        n_parts_hint: usize,
    ) !Query {
        // Not the retaining pool: its power-of-two classes would charge up to
        // twice each chunk's exact size, and a chunk lives until its
        // partitions consume it, so there is no churn for the pool to absorb.
        const tracked_worker_alloc = try exec.memory.trackedBackend(worker_alloc, exec.memory.accountantOf(allocator));

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
        for (parts) |*p| p.* = .{};
        const workers = try allocator.alloc(Worker, n_parts);
        errdefer allocator.free(workers);
        for (workers) |*w| w.* = .{};

        const views = try allocator.alloc(ColumnView, out_schema.len);
        errdefer allocator.free(views);

        const self = try allocator.create(PartitionedAggregate);
        self.* = .{
            .allocator = allocator,
            .worker_alloc = tracked_worker_alloc,
            .up = up,
            .group_cols = group_cols,
            .group_indices = gci,
            .aggs = aggs,
            .output_schema = out_schema,
            .n_parts = n_parts,
            .parts = parts,
            .workers = workers,
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
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) @as(u64, types.canonicalFloatBits(v)) else NULL_SENTINEL));
            },
            .double => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) types.canonicalFloatBits(v) else NULL_SENTINEL));
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

    /// Two-phase worker pool: thread `t` scatters slice `t` of each batch,
    /// then aggregates partition `t`.
    ///
    ///   .scatter   — the conn thread pulled a batch; every worker copies its
    ///                slice into a chunk and buckets the slice's rows by
    ///                partition. Barrier per batch — the views die at the
    ///                next pull.
    ///   .aggregate — after the last batch, each worker runs the serial
    ///                Aggregate over its partition's rows.
    ///
    /// The conn thread participates as worker 0 in both phases.
    const Pool = struct {
        owner: *PartitionedAggregate,
        batch: Batch = undefined,
        slices: usize = 0,
        mode: Mode = .scatter,
        gen: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        done: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        parked: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

        const Mode = enum { scatter, aggregate };

        fn workerMain(pool: *Pool, worker_idx: usize) void {
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
                pool.runPhase(worker_idx);
                _ = pool.done.fetchAdd(1, .acq_rel);
                _ = pool.parked.fetchAdd(1, .acq_rel);
            }
        }

        fn runPhase(pool: *Pool, worker_idx: usize) void {
            const self = pool.owner;
            switch (pool.mode) {
                .scatter => self.scatterOne(worker_idx, pool.batch, pool.slices),
                .aggregate => self.aggregateOne(worker_idx),
            }
        }

        /// Publish a phase, work unit 0 on this thread, wait for every
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

    /// Slice boundaries fall on multiples of 8 so each slice's validity
    /// bitmap starts on a byte.
    fn sliceStart(rows: usize, slices: usize, slice_idx: usize) usize {
        if (slice_idx == 0) return 0;
        if (slice_idx >= slices) return rows;
        return std.mem.alignBackward(usize, rows * slice_idx / slices, 8);
    }

    fn scatterOne(self: *PartitionedAggregate, worker_idx: usize, batch: Batch, slices: usize) void {
        const worker = &self.workers[worker_idx];
        worker.chunk = null;
        if (worker_idx >= slices) return;
        worker.chunk = self.scatterSlice(worker, batch, sliceStart(batch.row_count, slices, worker_idx), sliceStart(batch.row_count, slices, worker_idx + 1)) catch |e| {
            worker.err = e;
            return;
        };
    }

    fn scatterSlice(self: *PartitionedAggregate, worker: *Worker, batch: Batch, lo: usize, hi: usize) !?*Chunk {
        if (hi == lo) return null;
        const rows = hi - lo;
        const alloc = self.worker_alloc;

        try worker.hashes.resize(alloc, rows);
        const hashes = worker.hashes.items;
        @memset(hashes, 0x9e3779b97f4a7c15);
        for (self.group_indices) |ci| hashColumnInto(engine.transform.subViewAligned(batch.values[ci], lo, rows), hashes);
        if (self.track_ndv) for (hashes) |h| worker.ndv.add(h);

        var counts = [_]u32{0} ** MAX_PARTS;
        for (hashes) |*h| {
            h.* %= self.n_parts;
            counts[@intCast(h.*)] += 1;
        }

        const chunk = try alloc.create(Chunk);
        errdefer alloc.destroy(chunk);
        const index = try alloc.alloc(u32, rows + self.n_parts + 1);
        errdefer alloc.free(index);
        const bounds = index[rows..];
        var slot: [MAX_PARTS]u32 = undefined;
        bounds[0] = 0;
        for (0..self.n_parts) |p| {
            slot[p] = bounds[p];
            bounds[p + 1] = bounds[p] + counts[p];
        }
        for (hashes, 0..) |part, row| {
            index[slot[@intCast(part)]] = @intCast(row);
            slot[@intCast(part)] += 1;
        }

        const schema = self.up.outputSchema();
        const cols = try initStores(alloc, schema);
        errdefer freeStores(alloc, cols);
        for (cols, batch.values) |*store, view| {
            try store.reserveTotal(alloc, rows, stringBytesInRange(view, lo, hi));
            try engine.transform.appendColumnRange(alloc, view, lo, hi, store);
        }

        chunk.* = .{ .cols = cols, .index = index, .rows = rows, .readers = .init(self.n_parts) };
        return chunk;
    }

    fn releaseChunk(self: *PartitionedAggregate, chunk_idx: usize) void {
        const chunk = self.chunks.items[chunk_idx].?;
        if (chunk.readers.fetchSub(1, .acq_rel) != 1) return;
        self.chunks.items[chunk_idx] = null;
        chunk.destroy(self.worker_alloc);
    }

    /// Rows of partition `part_idx` not yet read.
    fn partitionRows(self: *PartitionedAggregate, part_idx: usize) usize {
        var rows: usize = 0;
        for (self.chunks.items[self.parts[part_idx].cursor..]) |chunk| rows += chunk.?.rowsOf(part_idx).len;
        return rows;
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
    /// partition's rows and retain its output rows. Whatever chunks the run
    /// left unread (an error stops it early) are released here.
    fn aggregateOne(self: *PartitionedAggregate, part_idx: usize) void {
        const part = &self.parts[part_idx];
        self.runPartition(part_idx) catch |e| {
            part.err = e;
        };
        while (part.cursor < self.chunks.items.len) : (part.cursor += 1) self.releaseChunk(part.cursor);
    }

    /// The partition's rows copied into one set of stores at their exact size,
    /// for the permutation sort; null when a string column would pass the 4 GiB
    /// a column view can address.
    fn gatherPartition(self: *PartitionedAggregate, part_idx: usize) !?[]engine.ColumnStore {
        const alloc = self.worker_alloc;
        const part = &self.parts[part_idx];
        const rows = self.partitionRows(part_idx);
        const schema = self.up.outputSchema();
        const cols = try initStores(alloc, schema);
        errdefer freeStores(alloc, cols);
        for (cols, 0..) |*store, ci| {
            var bytes: usize = 0;
            for (self.chunks.items[part.cursor..]) |chunk| bytes += stringBytesAt(chunk.?.cols[ci].view(), chunk.?.rowsOf(part_idx));
            if (bytes > std.math.maxInt(u32)) {
                freeStores(alloc, cols);
                return null;
            }
            try store.reserveTotal(alloc, rows, bytes);
        }
        while (part.cursor < self.chunks.items.len) : (part.cursor += 1) {
            const chunk = self.chunks.items[part.cursor].?;
            const indices = chunk.rowsOf(part_idx);
            if (indices.len > 0) for (cols, chunk.cols) |*store, *col| {
                try engine.transform.appendByIndices(alloc, col.view(), indices, store);
            };
            self.releaseChunk(part.cursor);
        }
        return cols;
    }

    /// The partition's operators allocate from `worker_alloc` itself. The
    /// Aggregate keeps its own arena; nesting that inside another arena
    /// charged every node twice over, both arenas sizing each new node at
    /// 1.5x everything before it.
    fn runPartition(self: *PartitionedAggregate, part_idx: usize) !void {
        const part = &self.parts[part_idx];
        const alloc = self.worker_alloc;
        const up_schema = self.up.outputSchema();

        var sorted_input: ?[]engine.ColumnStore = null;
        defer if (sorted_input) |cols| freeStores(alloc, cols);
        if (self.sorted_stream and getenv_pa("THINDB_PAGG_BUFFERED_SORT") == null) {
            sorted_input = try self.gatherPartition(part_idx);
        }

        var in_views: []ColumnView = &.{};
        defer alloc.free(in_views);
        var perm: []u32 = &.{};
        defer alloc.free(perm);
        var specs: []exec.SortSpec = &.{};
        defer alloc.free(specs);
        var chunk_scan: ?ChunkScan = null;
        defer if (chunk_scan) |*scan| scan.deinit();
        var permuted_scan: ?PermutedInputScan = null;
        defer if (permuted_scan) |*scan| scan.deinit();

        // A partition gathered into stable stores sorts a row permutation
        // directly. The environment switch, and a partition too large for
        // column views, take the generic Sort path.
        var agg = if (sorted_input) |in_cols| blk: {
            in_views = try alloc.alloc(ColumnView, up_schema.len);
            for (in_cols, in_views) |*store, *v| v.* = store.view();
            const in_rows = if (in_cols.len > 0) in_cols[0].rowCount() else 0;
            perm = try alloc.alloc(u32, in_rows);
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
                    // pdq is unstable; the row index keeps a group's rows in
                    // input order, as the hash core sees them (ANY_VALUE,
                    // FIRST/LAST, MAX_BY ties).
                    return a < b;
                }
            };
            std.sort.pdq(u32, perm, SortCtx{
                .columns = in_cols,
                .indices = self.group_indices,
            }, SortCtx.lessThan);
            permuted_scan = try PermutedInputScan.init(alloc, up_schema, in_views, perm);
            break :blk try exec.makeQuery(alloc, &permuted_scan.?).streamGroupBy(self.group_cols, self.aggs);
        } else blk: {
            chunk_scan = try ChunkScan.init(alloc, self, part_idx);
            const src = exec.makeQuery(alloc, &chunk_scan.?);
            if (!self.sorted_stream) break :blk try aggregate.Aggregate.create(alloc, src, self.group_cols, self.aggs, null, null);
            // Near-unique groups: sort this partition by the group keys and
            // stream — one live group's state instead of a hash table
            // holding a heap state per group.
            specs = try alloc.alloc(exec.SortSpec, self.group_cols.len);
            for (self.group_cols, specs) |gc, *spec| spec.* = .{ .col = gc, .desc = false };
            var sorted = try src.orderBy(specs);
            errdefer sorted.deinit();
            break :blk try sorted.streamGroupBy(self.group_cols, self.aggs);
        };
        defer agg.deinit();

        part.out_cols = try initStores(alloc, self.output_schema);
        var out_rows: usize = 0;
        while (try agg.next()) |b| {
            for (part.out_cols, 0..) |*store, ci| {
                try engine.transform.appendColumnRange(alloc, b.values[ci], 0, b.row_count, store);
            }
            out_rows += b.row_count;
        }
        part.out_rows = out_rows;
    }

    fn run(self: *PartitionedAggregate) !void {
        const prof_on = exec.prof.enabled;
        const t0 = if (prof_on) exec.prof.nowTicks() else 0;

        var pool = Pool{ .owner = self };
        defer for (self.workers) |*w| w.hashes.deinit(self.worker_alloc);

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
        // Spawn failures degrade by folding every unit into the conn
        // thread's phase work.
        const spawn_ok = spawned == self.n_parts - 1;

        self.track_ndv = heavyStateAggCount(self.aggs) >= 2;
        var rows_in: u64 = 0;
        var bytes_in: usize = 0;
        var pull_ticks: i64 = 0;
        var scatter_ticks: i64 = 0;
        while (true) {
            const p0 = if (prof_on) exec.prof.nowTicks() else 0;
            const maybe = try self.up.next();
            if (prof_on) pull_ticks += exec.prof.nowTicks() - p0;
            const batch = maybe orelse break;
            if (batch.row_count == 0) continue;
            const s0 = if (prof_on) exec.prof.nowTicks() else 0;
            try self.chunks.ensureUnusedCapacity(self.allocator, self.n_parts);
            const slices = @min(self.n_parts, (batch.row_count + MIN_SLICE_ROWS - 1) / MIN_SLICE_ROWS);
            if (spawn_ok and slices > 1) {
                pool.batch = batch;
                pool.slices = slices;
                pool.runBarrier(.scatter);
            } else {
                for (0..slices) |w| self.scatterOne(w, batch, slices);
            }
            for (self.workers[0..slices]) |*w| if (w.chunk) |chunk| {
                self.chunks.appendAssumeCapacity(chunk);
                w.chunk = null;
            };
            // A refused slice ends the statement here: pulling on would only
            // retry the refused growth batch after batch.
            for (self.workers[0..slices]) |*w| if (w.err) |e| return e;
            rows_in += batch.row_count;
            if (prof_on) {
                for (batch.values) |view| bytes_in += viewBytes(view, batch.row_count);
                scatter_ticks += exec.prof.nowTicks() - s0;
            }
        }
        const t1 = if (prof_on) exec.prof.nowTicks() else 0;

        // Core selection: the scatter hashes identify each row's group, so a
        // HyperLogLog over them (~3% error) gives the group count before any
        // partition runs — every partition then aggregates in the one
        // parallel phase (a serial hash-core probe of partition 0 used to
        // precede it). ≥90% unique: at moderate ratios (measured: 73%
        // unique, 3.6M rows) the hash core still wins — the sort's row-bound
        // cost only pays off when nearly every row opens a fresh group's
        // heap states.
        if (self.track_ndv and rows_in >= 4096 * self.n_parts) {
            var ndv: hll.Hll = .{};
            for (self.workers) |*w| ndv.merge(&w.ndv);
            const est = ndv.estimate();
            self.sorted_stream = est * 10 >= rows_in * 9;
            if (prof_on) std.debug.print(
                "[hprof] pagg.core: est_groups/rows={d}/{d} heavy_aggs={d} -> {s}\n",
                .{ est, rows_in, heavyStateAggCount(self.aggs), if (self.sorted_stream) "sort+stream" else "hash" },
            );
        }

        const chunk_count = self.chunks.items.len;
        if (spawn_ok) {
            pool.runBarrier(.aggregate);
        } else {
            for (0..self.n_parts) |p| self.aggregateOne(p);
        }
        if (prof_on) {
            const t2 = exec.prof.nowTicks();
            std.debug.print("[hprof] pagg.scatter {d:.2} ms (pull={d:.2} split+copy={d:.2})  pagg.partitions {d:.2} ms  (parts={d} rows={d} in={d} MiB chunks={d})\n", .{
                exec.prof.ticksToMs(t1 - t0),
                exec.prof.ticksToMs(pull_ticks),
                exec.prof.ticksToMs(scatter_ticks),
                exec.prof.ticksToMs(t2 - t1),
                self.n_parts,
                rows_in,
                bytes_in >> 20,
                chunk_count,
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
        const t_free = exec.prof.nowTicks();
        for (self.chunks.items) |maybe| if (maybe) |chunk| chunk.destroy(self.worker_alloc);
        self.chunks.deinit(self.allocator);
        for (self.parts) |*p| freeStores(self.worker_alloc, p.out_cols);
        exec.prof.addPhase("pagg.deinit.buffers", @intCast(exec.prof.nowTicks() - t_free));
        self.allocator.free(self.parts);
        self.allocator.free(self.workers);
        self.allocator.free(self.group_indices);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.views);
        self.allocator.destroy(self);
    }
};

const testing = std.testing;
const engine_store = engine.ColumnStore;

/// Test source: `rows` rows of borrowed column views, served in windows of
/// `batch_rows` rows (a multiple of 8, so each window's validity bitmap starts
/// on a byte).
const InputScan = struct {
    schema: []const Column,
    source: []const ColumnView,
    views: []ColumnView,
    rows: usize,
    offset: usize = 0,
    batch_rows: usize = 64 * 1024,

    pub fn next(self: *InputScan) !?Batch {
        if (self.offset >= self.rows) return null;
        const take = @min(self.batch_rows, self.rows - self.offset);
        for (self.source, self.views) |src, *view| view.* = engine.transform.subViewAligned(src, self.offset, take);
        self.offset += take;
        return Batch{ .schema = self.schema, .values = self.views, .row_count = take };
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
    pub fn explain(_: *InputScan, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) !void {
        try exec.explainLine(out, alloc, depth, "TestInput");
    }
};

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
    var window: [4]ColumnView = undefined;
    const group_cols = [_][]const u8{"key"};
    const aggs = [_]AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "val", .as = "s" },
        .{ .func = .max_by, .col = "label", .arg2_col = "ord", .as = "mb" },
    };

    var scan_p = InputScan{ .schema = &schema, .source = &views, .views = &window, .rows = N };
    var pa = try PartitionedAggregate.create(a, a, exec.makeQuery(a, &scan_p), &group_cols, &aggs, 4);
    const par_lines = try testCollectSorted(a, &pa);
    defer {
        for (par_lines) |l| a.free(l);
        a.free(par_lines);
    }
    pa.deinit();

    var scan_s = InputScan{ .schema = &schema, .source = &views, .views = &window, .rows = N };
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

test "PartitionedAggregate sort+stream core keeps a group's rows in input order" {
    const a = testing.allocator;
    const row_count = 24_000;
    const group_count = 23_500;

    const schema = [_]Column{
        .{ .name = "key", .type = .string, .nullable = false },
        .{ .name = "ord", .type = .bigint, .nullable = false },
        .{ .name = "tie", .type = .bigint, .nullable = false },
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
        try stores[1].data.bigint.append(a, @intCast(i));
        try stores[2].data.bigint.append(a, @intCast(group_id));
        var label_buf: [16]u8 = undefined;
        const label = try std.fmt.bufPrint(&label_buf, "L{d}", .{i});
        try stores[3].data.string.appendValue(a, label);
    }

    var views: [4]ColumnView = undefined;
    for (&views, &stores) |*v, *s| v.* = s.view();
    var window: [4]ColumnView = undefined;
    const group_cols = [_][]const u8{"key"};
    // The repeated groups carry a different ord/label per row and an equal
    // MAX_BY argument, so the order the core sees the rows in decides the
    // ANY_VALUE and MAX_BY results.
    const aggs = [_]AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .any_value, .col = "ord", .as = "v" },
        .{ .func = .max_by, .col = "label", .arg2_col = "tie", .as = "mb" },
    };

    var scan_p = InputScan{ .schema = &schema, .source = &views, .views = &window, .rows = row_count };
    var pa = try PartitionedAggregate.create(a, a, exec.makeQuery(a, &scan_p), &group_cols, &aggs, 4);
    const par_lines = try testCollectSorted(a, &pa);
    defer {
        for (par_lines) |line| a.free(line);
        a.free(par_lines);
    }
    pa.deinit();

    var scan_s = InputScan{ .schema = &schema, .source = &views, .views = &window, .rows = row_count };
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
    var window: [4]ColumnView = undefined;
    const group_cols = [_][]const u8{"key"};
    const aggs = [_]AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .any_value, .col = "val", .as = "v" },
        .{ .func = .max_by, .col = "label", .arg2_col = "ord", .as = "mb" },
    };

    var scan_p = InputScan{ .schema = &schema, .source = &views, .views = &window, .rows = row_count };
    var pa = try PartitionedAggregate.create(a, a, exec.makeQuery(a, &scan_p), &group_cols, &aggs, 4);
    const par_lines = try testCollectSorted(a, &pa);
    defer {
        for (par_lines) |line| a.free(line);
        a.free(par_lines);
    }
    pa.deinit();

    var scan_s = InputScan{ .schema = &schema, .source = &views, .views = &window, .rows = row_count };
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

test "PartitionedAggregate drains a partition in windows with exact NULL and distinct counts" {
    const a = testing.allocator;
    // Five groups over four partitions: some partition holds two groups, so
    // its rows span several windows, and the last window is not a multiple of 8.
    const row_count = 3 * ChunkScan.batch_rows + 1003;
    const group_count = 5;

    const schema = [_]Column{
        .{ .name = "g", .type = .bigint, .nullable = false },
        .{ .name = "v", .type = .bigint, .nullable = true },
    };
    var stores: [2]engine_store = undefined;
    for (&stores, schema) |*s, col| s.* = try engine_store.init(a, col.type, col.nullable);
    defer for (&stores) |*s| s.deinit(a);

    const Pair = struct { g: i64, v: i64 };
    var pairs: std.AutoHashMapUnmanaged(Pair, void) = .empty;
    defer pairs.deinit(a);
    var want_rows = [_]i64{0} ** group_count;
    var want_valid = [_]i64{0} ** group_count;
    var want_distinct = [_]i64{0} ** group_count;
    for (0..row_count) |i| {
        const g: i64 = @intCast(i % group_count);
        // Each group sees each value twice; every seventh row is NULL.
        const v: i64 = @intCast(i / 10);
        const valid = i % 7 != 0;
        try stores[0].data.bigint.append(a, g);
        try stores[1].appendValidBit(a, i, valid);
        try stores[1].data.bigint.append(a, if (valid) v else 0);
        want_rows[@intCast(g)] += 1;
        if (valid) {
            want_valid[@intCast(g)] += 1;
            const gop = try pairs.getOrPut(a, .{ .g = g, .v = v });
            if (!gop.found_existing) want_distinct[@intCast(g)] += 1;
        }
    }

    var views: [2]ColumnView = undefined;
    for (&views, &stores) |*v, *s| v.* = s.view();
    var window: [2]ColumnView = undefined;
    const group_cols = [_][]const u8{"g"};
    const aggs = [_]AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .count, .col = "v", .as = "cv" },
        .{ .func = .count_distinct, .col = "v", .as = "dv" },
    };

    var scan = InputScan{ .schema = &schema, .source = &views, .views = &window, .rows = row_count };
    var pa = try PartitionedAggregate.create(a, a, exec.makeQuery(a, &scan), &group_cols, &aggs, 4);
    defer pa.deinit();
    var seen: usize = 0;
    while (try pa.next()) |b| {
        for (0..b.row_count) |row| {
            const g: usize = @intCast(testReadI128(b.values[0], row));
            try testing.expectEqual(@as(i128, want_rows[g]), testReadI128(b.values[1], row));
            try testing.expectEqual(@as(i128, want_valid[g]), testReadI128(b.values[2], row));
            try testing.expectEqual(@as(i128, want_distinct[g]), testReadI128(b.values[3], row));
            seen += 1;
        }
    }
    try testing.expectEqual(@as(usize, group_count), seen);
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
        var window: [3]ColumnView = undefined;
        var scan = InputScan{ .schema = &schema, .source = &views, .views = &window, .rows = rows.len };
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
    var part_window: [3]ColumnView = undefined;
    var part_scan = InputScan{ .schema = &part_schema, .source = &part_views, .views = &part_window, .rows = part_stores[0].rowCount() };
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
    var all_window: [3]ColumnView = undefined;
    var all_scan = InputScan{ .schema = &schema, .source = &all_views, .views = &all_window, .rows = chunk_a.len + chunk_b.len };
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

const mixed_schema = [_]Column{
    .{ .name = "k", .type = .string, .nullable = true },
    .{ .name = "n", .type = .int, .nullable = false },
    .{ .name = "v", .type = .bigint, .nullable = true },
    .{ .name = "d", .type = .double, .nullable = false },
    .{ .name = "j", .type = .json, .nullable = true },
    .{ .name = "s", .type = .string, .nullable = false },
    .{ .name = "o", .type = .bigint, .nullable = false },
};

const mixed_aggs = [_]AggSpec{
    .{ .func = .count, .col = null, .as = "c" },
    .{ .func = .sum, .col = "n", .as = "sn" },
    .{ .func = .count, .col = "v", .as = "cv" },
    .{ .func = .count_distinct, .col = "v", .as = "dv" },
    .{ .func = .min, .col = "d", .as = "md" },
    .{ .func = .max_by, .col = "j", .arg2_col = "o", .as = "mj" },
    .{ .func = .min, .col = "s", .as = "ms" },
    .{ .func = .any_value, .col = "s", .as = "av" },
};

const MixedStores = [mixed_schema.len]engine_store;

/// Row `i` of the mixed input: a nullable string key over 37 values plus
/// NULL, fixed and variable widths, NULLs in three columns, JSON, and a
/// unique ordering column for MAX_BY.
fn testAppendMixedRow(a: Allocator, stores: *MixedStores, i: usize) !void {
    const row = stores[0].rowCount();
    var buf: [48]u8 = undefined;
    const key_valid = i % 11 != 0;
    try stores[0].data.string.appendValue(a, if (key_valid) try std.fmt.bufPrint(&buf, "k{d}", .{i % 37}) else "");
    try stores[0].appendValidBit(a, row, key_valid);
    try stores[1].data.int.append(a, @as(i32, @intCast(i % 1000)) - 500);
    try stores[2].data.bigint.append(a, @intCast(i % 997));
    try stores[2].appendValidBit(a, row, i % 7 != 0);
    try stores[3].data.double.append(a, @as(f64, @floatFromInt((i * 7919) % 10007)) / 8.0);
    const json_valid = i % 5 != 0;
    try stores[4].data.json.appendValue(a, if (json_valid) try std.fmt.bufPrint(&buf, "{{\"i\":{d}}}", .{i}) else "");
    try stores[4].appendValidBit(a, row, json_valid);
    try stores[5].data.string.appendValue(a, try std.fmt.bufPrint(&buf, "s-{d}-payload", .{(i * 31) % 5003}));
    try stores[6].data.bigint.append(a, @intCast(i));
}

/// The same rows twice: as one set of stores, and split into batches of the
/// given sizes, each with its own stores.
const MixedInput = struct {
    all: MixedStores,
    batches: []MixedStores,
    rows: usize,

    fn init(a: Allocator, sizes: []const usize) !MixedInput {
        const batches = try a.alloc(MixedStores, sizes.len);
        var self = MixedInput{ .all = undefined, .batches = batches, .rows = 0 };
        for (&self.all, mixed_schema) |*s, col| s.* = try engine_store.init(a, col.type, col.nullable);
        for (self.batches) |*b| for (b, mixed_schema) |*s, col| {
            s.* = try engine_store.init(a, col.type, col.nullable);
        };
        errdefer self.deinit(a);
        for (sizes, self.batches) |size, *b| {
            for (0..size) |_| {
                try testAppendMixedRow(a, b, self.rows);
                try testAppendMixedRow(a, &self.all, self.rows);
                self.rows += 1;
            }
        }
        return self;
    }

    fn deinit(self: *MixedInput, a: Allocator) void {
        for (&self.all) |*s| s.deinit(a);
        for (self.batches) |*b| for (b) |*s| s.deinit(a);
        a.free(self.batches);
    }

    fn rawBatchBytes(self: *const MixedInput) usize {
        var total: usize = 0;
        for (self.batches) |*b| for (b) |*s| {
            if (s.nulls) |n| total += n.items.len;
            total += switch (s.data) {
                .varchar, .string, .char, .json => |ss| ss.bytes.items.len + ss.offsets.items.len * @sizeOf(u32),
                inline else => |list| list.items.len * @sizeOf(std.meta.Elem(@TypeOf(list.items))),
            };
        };
        return total;
    }
};

/// Test source: one batch per element of `batches`, whatever its size.
const BatchListScan = struct {
    batches: []const MixedStores,
    views: *[mixed_schema.len]ColumnView,
    next_batch: usize = 0,

    pub fn next(self: *BatchListScan) !?Batch {
        if (self.next_batch >= self.batches.len) return null;
        const stores = &self.batches[self.next_batch];
        self.next_batch += 1;
        for (stores, self.views) |*s, *v| v.* = s.view();
        return Batch{ .schema = &mixed_schema, .values = self.views, .row_count = stores[0].rowCount() };
    }
    pub fn deinit(_: *BatchListScan) void {}
    pub fn outputSchema(_: *BatchListScan) []const Column {
        return &mixed_schema;
    }
    pub fn addPrune(_: *BatchListScan, _: exec.Predicate) !void {}
    pub fn stats(self: *BatchListScan) exec.PipelineStats {
        var rows: usize = 0;
        for (self.batches) |*b| rows += b[0].rowCount();
        return .{ .upper_rows = rows };
    }
    pub fn accountant(_: *BatchListScan) ?*exec.memory.MemoryAccountant {
        return null;
    }
    pub fn explain(_: *BatchListScan, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) !void {
        try exec.explainLine(out, alloc, depth, "TestBatchList");
    }
};

fn testFormatRow(allocator: Allocator, b: Batch, row: usize) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (b.values, 0..) |v, ci| {
        if (ci > 0) try out.append(allocator, '|');
        if (!v.isValid(row)) {
            try out.appendSlice(allocator, "NULL");
            continue;
        }
        switch (v.data) {
            .varchar, .string, .char, .json => |sv| try out.appendSlice(allocator, sv.rowBytes(row)),
            .float => |s| try out.print(allocator, "{d}", .{s[row]}),
            .double => |s| try out.print(allocator, "{d}", .{s[row]}),
            .decimal64 => |s| try out.print(allocator, "{d}", .{s[row]}),
            .decimal128 => |s| try out.print(allocator, "{d}", .{s[row]}),
            else => try out.print(allocator, "{d}", .{testReadI128(v, row)}),
        }
    }
    return out.toOwnedSlice(allocator);
}

fn testFreeLines(allocator: Allocator, lines: [][]u8) void {
    for (lines) |line| allocator.free(line);
    allocator.free(lines);
}

/// Every output row formatted and sorted — the partition-parallel and serial
/// aggregates emit the same groups in different orders.
fn testCollectLines(allocator: Allocator, q: *Query) ![][]u8 {
    var lines: std.ArrayList([]u8) = .empty;
    errdefer {
        for (lines.items) |line| allocator.free(line);
        lines.deinit(allocator);
    }
    while (try q.next()) |b| {
        for (0..b.row_count) |row| {
            const line = try testFormatRow(allocator, b, row);
            errdefer allocator.free(line);
            try lines.append(allocator, line);
        }
    }
    std.mem.sort([]u8, lines.items, {}, struct {
        fn lt(_: void, x: []u8, y: []u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.lt);
    return lines.toOwnedSlice(allocator);
}

test "PartitionedAggregate over many odd-sized batches of strings, JSON and NULLs matches the serial aggregate" {
    const a = testing.allocator;
    // Sizes below, at and far above a scatter slice, not multiples of 8, so
    // the slices, the chunks and the partition windows all cut unevenly.
    const sizes = [_]usize{ 1, 8191, 8193, 40000, 7, 65536, 100003, 3, 16384, 24 };
    var input = try MixedInput.init(a, &sizes);
    defer input.deinit(a);
    const group_cols = [_][]const u8{"k"};

    var batch_views: [mixed_schema.len]ColumnView = undefined;
    var list_scan = BatchListScan{ .batches = input.batches, .views = &batch_views };
    var pa = try PartitionedAggregate.create(a, a, exec.makeQuery(a, &list_scan), &group_cols, &mixed_aggs, 4);
    const par_lines = try testCollectLines(a, &pa);
    defer testFreeLines(a, par_lines);
    pa.deinit();

    var all_views: [mixed_schema.len]ColumnView = undefined;
    for (&all_views, &input.all) |*v, *s| v.* = s.view();
    var window: [mixed_schema.len]ColumnView = undefined;
    var scan_s = InputScan{ .schema = &mixed_schema, .source = &all_views, .views = &window, .rows = input.rows };
    var ser = try aggregate.Aggregate.create(a, exec.makeQuery(a, &scan_s), &group_cols, &mixed_aggs, null, null);
    const ser_lines = try testCollectLines(a, &ser);
    defer testFreeLines(a, ser_lines);
    ser.deinit();

    try testing.expectEqual(@as(usize, 38), ser_lines.len);
    try testing.expectEqual(ser_lines.len, par_lines.len);
    for (par_lines, ser_lines) |p, s| try testing.expectEqualStrings(s, p);

    var total: usize = 0;
    for (par_lines) |line| {
        var fields = std.mem.splitScalar(u8, line, '|');
        _ = fields.next();
        total += try std.fmt.parseInt(usize, fields.next().?, 10);
    }
    try testing.expectEqual(input.rows, total);
}

fn testTrackedAccountant(a: Allocator) !*exec.memory.MemoryAccountant {
    const account = try a.create(exec.memory.MemoryAccountant);
    account.* = exec.memory.MemoryAccountant.initWithPool(1 << 40, null);
    account.trackAllocations(a);
    return account;
}

test "PartitionedAggregate charges about the raw input bytes and frees the input before it emits" {
    const a = testing.allocator;
    const sizes = [_]usize{65536} ** 16 ++ [_]usize{4099};
    var input = try MixedInput.init(a, &sizes);
    defer input.deinit(a);
    const raw = input.rawBatchBytes();
    const group_cols = [_][]const u8{"k"};
    const aggs = mixed_aggs[0..2];
    const n_parts = 2;

    // Each partition runs one serial aggregate, whose state is sized for a
    // window of new groups however few groups there are.
    const serial_peak = blk: {
        const account = try testTrackedAccountant(a);
        defer account.releaseOwner(a);
        const tracked = try account.executionAllocator();
        var all_views: [mixed_schema.len]ColumnView = undefined;
        for (&all_views, &input.all) |*v, *s| v.* = s.view();
        var window: [mixed_schema.len]ColumnView = undefined;
        var scan = InputScan{ .schema = &mixed_schema, .source = &all_views, .views = &window, .rows = input.rows };
        var ser = try aggregate.Aggregate.create(tracked, exec.makeQuery(tracked, &scan), &group_cols, aggs, null, null);
        while (try ser.next()) |_| {}
        ser.deinit();
        break :blk account.peak_bytes;
    };

    const account = try testTrackedAccountant(a);
    defer account.releaseOwner(a);
    const tracked = try account.executionAllocator();
    var batch_views: [mixed_schema.len]ColumnView = undefined;
    var list_scan = BatchListScan{ .batches = input.batches, .views = &batch_views };
    var pa = try PartitionedAggregate.create(tracked, a, exec.makeQuery(tracked, &list_scan), &group_cols, aggs, n_parts);
    var groups: usize = 0;
    groups += (try pa.next()).?.row_count;
    // Every partition has aggregated: only the outputs are still charged.
    const after_run = account.current_bytes;
    while (try pa.next()) |b| groups += b.row_count;
    pa.deinit();

    try testing.expectEqual(@as(usize, 38), groups);
    // The buffered input stays within 1.5x its raw bytes, beside one
    // aggregate's state per partition; the per-partition arenas it replaced
    // charged 7-8x the input.
    try testing.expect(account.peak_bytes > raw);
    try testing.expect(account.peak_bytes < raw + raw / 2 + n_parts * serial_peak);
    try testing.expect(after_run < raw / 64);
    try testing.expectEqual(@as(usize, 0), account.current_bytes);
}
