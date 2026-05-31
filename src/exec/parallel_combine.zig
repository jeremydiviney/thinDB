//! Parallel combine for the two-phase parallel GROUP BY.
//!
//! The per-worker partial aggregate (fused into the ParallelScan workers, see
//! `parallel_scan.zig` `tryFuseAggregate`) emits partial group rows; this
//! operator combines them into the final groups. The *serial* combine (one
//! `Aggregate` over the concatenated partials) makes the merge the bottleneck on
//! high cardinality. This operator instead **hash-partitions** the partial rows
//! by group key into `n_parts` disjoint partitions — every final key lands in
//! exactly one partition, so each partition is an independent `Aggregate` with
//! no cross-partition merge — and (when parallelized) runs them across threads.
//!
//! Phase A (this commit): build + partition + per-partition combine run
//! serially, gated behind the same env flag as the partial fusion. It is a
//! correctness scaffold — identical results to the serial combine — and the base
//! the threaded phase B parallelizes. The per-partition `Aggregate` is the
//! existing operator, so the merge's hash/sort/radix routing is reused for free.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const makeQuery = exec.makeQuery;
const memory = exec.memory;
const PipelineStats = exec.PipelineStats;

const store = @import("../engine/store.zig");
const ColumnStore = store.ColumnStore;
const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const aggregate = @import("aggregate.zig");
const AggSpec = aggregate.AggSpec;
const single_batch = @import("single_batch.zig");
const predicate = @import("predicate.zig");

/// Append one cell of `src[src_idx]` onto `dst`, preserving NULLs. Mirrors the
/// per-type copy in `udf_aggregate.zig`; kept local so this operator has no
/// dependency on that file.
fn appendCellFromView(allocator: Allocator, dst: *ColumnStore, src: ColumnView, src_idx: usize) !void {
    if (!src.isValid(src_idx)) {
        try dst.data.appendNullPlaceholder(allocator);
        try dst.appendValidBit(allocator, dst.rowCount() - 1, false);
        return;
    }
    switch (dst.data) {
        .int => |*l| try l.append(allocator, src.data.int[src_idx]),
        .bigint => |*l| try l.append(allocator, src.data.bigint[src_idx]),
        .smallint => |*l| try l.append(allocator, src.data.smallint[src_idx]),
        .tinyint => |*l| try l.append(allocator, src.data.tinyint[src_idx]),
        .largeint => |*l| try l.append(allocator, src.data.largeint[src_idx]),
        .float => |*l| try l.append(allocator, src.data.float[src_idx]),
        .double => |*l| try l.append(allocator, src.data.double[src_idx]),
        .boolean => |*l| try l.append(allocator, src.data.boolean[src_idx]),
        .date => |*l| try l.append(allocator, src.data.date[src_idx]),
        .datetime => |*l| try l.append(allocator, src.data.datetime[src_idx]),
        .decimal64 => |*l| try l.append(allocator, src.data.decimal64[src_idx]),
        .decimal128 => |*l| try l.append(allocator, src.data.decimal128[src_idx]),
        .uuid => |*l| try l.append(allocator, src.data.uuid[src_idx]),
        .varchar => |*s| try s.appendValue(allocator, strBytes(src, src_idx)),
        .string => |*s| try s.appendValue(allocator, strBytes(src, src_idx)),
        .char => |*s| try s.appendValue(allocator, strBytes(src, src_idx)),
    }
    try dst.appendValidBit(allocator, dst.rowCount() - 1, true);
}

fn strBytes(src: ColumnView, row: usize) []const u8 {
    return switch (src.data) {
        .varchar => |sv| sv.rowBytes(row),
        .string => |sv| sv.rowBytes(row),
        .char => |sv| sv.rowBytes(row),
        else => unreachable,
    };
}

/// Hash of row `r`'s group-key columns. Only used to assign a partition, so any
/// deterministic value-dependent hash works — exact grouping is the per-
/// partition `Aggregate`'s job. NULLs hash to a fixed sentinel.
fn hashRowKey(views: []const ColumnView, key_idx: []const usize, r: usize) u64 {
    var h = std.hash.Wyhash.init(0x9e3779b97f4a7c15);
    for (key_idx) |ci| {
        const v = views[ci];
        if (!v.isValid(r)) {
            h.update(&[_]u8{0xff});
            continue;
        }
        switch (v.data) {
            .int => |a| h.update(std.mem.asBytes(&a[r])),
            .bigint => |a| h.update(std.mem.asBytes(&a[r])),
            .smallint => |a| h.update(std.mem.asBytes(&a[r])),
            .tinyint => |a| h.update(std.mem.asBytes(&a[r])),
            .largeint => |a| h.update(std.mem.asBytes(&a[r])),
            .float => |a| h.update(std.mem.asBytes(&a[r])),
            .double => |a| h.update(std.mem.asBytes(&a[r])),
            .boolean => |a| h.update(std.mem.asBytes(&a[r])),
            .date => |a| h.update(std.mem.asBytes(&a[r])),
            .datetime => |a| h.update(std.mem.asBytes(&a[r])),
            .decimal64 => |a| h.update(std.mem.asBytes(&a[r])),
            .decimal128 => |a| h.update(std.mem.asBytes(&a[r])),
            .uuid => |a| h.update(std.mem.asBytes(&a[r])),
            .varchar, .string, .char => h.update(strBytes(v, r)),
        }
    }
    return h.final();
}

/// Number of hash partitions. Enough to spread work across the thread pool with
/// headroom for skew; the per-partition `Aggregate` sizes its own table.
const N_PARTS: usize = 32;

/// Below this many partial rows the merge runs single-threaded — the thread-
/// spawn cost would dwarf the work (low/med-card GROUP BYs whose per-worker
/// partials already collapsed to a handful of groups).
const PARALLEL_MIN_ROWS: usize = 16384;

pub const ParallelCombine = struct {
    allocator: Allocator,
    upstream: Query,
    group_cols: []const []const u8,
    combine: []const AggSpec,
    key_idx: []usize,
    out_schema: []const Column,
    emit_limit: ?u32,
    acct: ?*memory.MemoryAccountant,

    // Buffered final result (built on first next()).
    out_cols: []ColumnStore,
    out_views: []ColumnView,
    built: bool = false,
    emitted: bool = false,
    n_out: usize = 0,

    // Phase-A partition buffers, read by the phase-B worker threads (each
    // partition is claimed by exactly one thread via the work-steal cursor, so
    // there is no shared mutable access). `ncol` = upstream column count.
    part: [][]ColumnStore = &.{},
    ncol: usize = 0,

    pub fn create(
        allocator: Allocator,
        upstream: Query,
        group_cols: []const []const u8,
        combine: []const AggSpec,
        emit_limit: ?u32,
    ) !Query {
        const up_schema = upstream.outputSchema();
        const key_idx = try allocator.alloc(usize, group_cols.len);
        errdefer allocator.free(key_idx);
        for (group_cols, key_idx) |name, *dst| dst.* = types.findColumn(up_schema, name) orelse return exec.Error.ColumnNotFound;

        // The combine produces the same schema as the partial input (group cols +
        // partial agg cols, same types — the combine is sum/min/max of each
        // partial column with its type pinned via out_type_override).
        const out_schema = try allocator.dupe(Column, up_schema);
        errdefer allocator.free(out_schema);

        const out_cols = try allocator.alloc(ColumnStore, up_schema.len);
        errdefer allocator.free(out_cols);
        const out_views = try allocator.alloc(ColumnView, up_schema.len);
        errdefer allocator.free(out_views);

        const self = try allocator.create(ParallelCombine);
        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .group_cols = group_cols,
            .combine = combine,
            .key_idx = key_idx,
            .out_schema = out_schema,
            .emit_limit = emit_limit,
            .acct = upstream.accountant(),
            .out_cols = out_cols,
            .out_views = out_views,
        };
        return makeQuery(allocator, self);
    }

    fn build(self: *ParallelCombine) !void {
        const a = self.allocator;
        const up_schema = self.out_schema; // dup of the partial schema (same cols)
        const ncol = up_schema.len;
        self.ncol = ncol;

        // Per-(partition, column) accumulators for the hash exchange.
        const part: [][]ColumnStore = try a.alloc([]ColumnStore, N_PARTS);
        for (part) |*p| {
            p.* = try a.alloc(ColumnStore, ncol);
            for (p.*, up_schema) |*c, col| c.* = try ColumnStore.init(a, col.type, col.nullable);
        }
        self.part = part;

        // Phase A: drain partials, route each row to its key-hash partition.
        while (try self.upstream.next()) |batch| {
            const n = batch.row_count;
            var r: usize = 0;
            while (r < n) : (r += 1) {
                const p = hashRowKey(batch.values, self.key_idx, r) % N_PARTS;
                for (0..ncol) |c| try appendCellFromView(a, &part[p][c], batch.values[c], r);
            }
        }
        for (self.out_cols, up_schema) |*c, col| c.* = try ColumnStore.init(a, col.type, col.nullable);

        // Phase B: combine the disjoint partitions across threads. Each thread
        // owns a page-backed arena for its outputs (survives join) and a fresh
        // per-partition scratch arena (freed per partition). The work-steal
        // cursor hands each partition to exactly one thread.
        //
        // Adaptive degree: a small partial set (low/med card, where the per-worker
        // partial already collapsed the rows) isn't worth the thread-spawn cost —
        // run it on the calling thread. Only the high-card merges fan out.
        var total_partials: usize = 0;
        for (part) |pcols| total_partials += pcols[0].rowCount();
        const T = if (total_partials < PARALLEL_MIN_ROWS) 1 else @min(N_PARTS, @max(@as(usize, 1), std.Thread.getCpuCount() catch 1));
        const arenas = try a.alloc(std.heap.ArenaAllocator, T);
        const touts = try a.alloc([]ColumnStore, T);
        const terr = try a.alloc(?anyerror, T);
        for (0..T) |t| {
            arenas[t] = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            terr[t] = null;
            const ta = arenas[t].allocator();
            touts[t] = try ta.alloc(ColumnStore, ncol);
            for (touts[t], up_schema) |*c, col| c.* = try ColumnStore.init(ta, col.type, col.nullable);
        }
        defer for (arenas) |*ar| ar.deinit();

        var cursor = std.atomic.Value(usize).init(0);
        const threads = try a.alloc(std.Thread, T);
        var spawned: usize = 0;
        var ti: usize = 1;
        while (ti < T) : (ti += 1) {
            threads[ti] = std.Thread.spawn(.{}, worker, .{ self, &cursor, arenas, touts, terr, ti }) catch break;
            spawned = ti;
        }
        worker(self, &cursor, arenas, touts, terr, 0);
        var j: usize = 1;
        while (j <= spawned) : (j += 1) threads[j].join();
        for (terr) |e| if (e) |err| return err;

        // Concat per-thread outputs into the query-arena result + apply LIMIT.
        var produced: usize = 0;
        outer: for (touts) |to| {
            const n = to[0].rowCount();
            var r: usize = 0;
            while (r < n) : (r += 1) {
                if (self.emit_limit) |lim| if (produced >= lim) break :outer;
                for (0..ncol) |c| try appendCellFromView(a, &self.out_cols[c], to[c].view(), r);
                produced += 1;
            }
        }
        self.n_out = produced;
        self.built = true;
    }

    /// Phase-B thread body: claim partitions off the shared cursor and combine
    /// each into this thread's output `touts[t]`. Errors land in `terr[t]`.
    fn worker(self: *ParallelCombine, cursor: *std.atomic.Value(usize), arenas: []std.heap.ArenaAllocator, touts: [][]ColumnStore, terr: []?anyerror, t: usize) void {
        const out_alloc = arenas[t].allocator();
        while (true) {
            const p = cursor.fetchAdd(1, .monotonic);
            if (p >= N_PARTS) break;
            const pcols = self.part[p];
            if (pcols[0].rowCount() == 0) continue;
            self.combinePartition(pcols, out_alloc, touts[t]) catch |e| {
                terr[t] = e;
                return;
            };
        }
    }

    /// Aggregate one partition's rows with the combine specs, appending the final
    /// groups onto `out` (owned by the caller's surviving arena). Uses a fresh
    /// page-backed scratch arena for the hash table, freed on return.
    fn combinePartition(self: *ParallelCombine, pcols: []ColumnStore, out_alloc: Allocator, out: []ColumnStore) !void {
        var scr = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer scr.deinit();
        const sa = scr.allocator();
        const ncol = self.ncol;
        const views = try sa.alloc(ColumnView, ncol);
        for (0..ncol) |c| views[c] = pcols[c].view();
        const pbatch = Batch{ .schema = self.out_schema, .values = views, .row_count = @intCast(pcols[0].rowCount()) };
        const src = try single_batch.SingleBatchSource.create(sa, pbatch);
        var agg = try aggregate.Aggregate.create(sa, src, self.group_cols, self.combine, null, null);
        defer agg.deinit();
        while (try agg.next()) |ob| {
            const on = ob.row_count;
            var r: usize = 0;
            while (r < on) : (r += 1) {
                for (0..ncol) |c| try appendCellFromView(out_alloc, &out[c], ob.values[c], r);
            }
        }
    }

    pub fn next(self: *ParallelCombine) !?Batch {
        if (!self.built) try self.build();
        if (self.emitted) return null;
        self.emitted = true;
        for (self.out_cols, 0..) |c, i| self.out_views[i] = c.view();
        return Batch{ .schema = self.out_schema, .values = self.out_views, .row_count = @intCast(self.n_out) };
    }

    pub fn deinit(self: *ParallelCombine) void {
        const a = self.allocator;
        self.upstream.deinit();
        // out_cols are only initialized in build(); on an EXPLAIN-only path (no
        // next()) they're allocated-but-uninitialized — don't deinit those.
        if (self.built) for (self.out_cols) |*c| c.deinit(a);
        a.free(self.out_cols);
        a.free(self.out_views);
        a.free(self.key_idx);
        a.free(@constCast(self.out_schema));
        a.destroy(self);
    }

    pub fn outputSchema(self: *ParallelCombine) []const Column {
        return self.out_schema;
    }

    pub fn addPrune(self: *ParallelCombine, pred: predicate.Predicate) !void {
        _ = self;
        _ = pred;
    }

    pub fn stats(self: *ParallelCombine) PipelineStats {
        return .{ .upper_rows = @intCast(self.n_out), .sort_state = .{ .keys = &.{}, .global = false } };
    }

    pub fn accountant(self: *ParallelCombine) ?*memory.MemoryAccountant {
        return self.acct;
    }

    pub fn explain(self: *ParallelCombine, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "ParallelCombine (hash-partition merge)");
        try self.upstream.explain(out, allocator, depth + 1);
    }
};
