//! CTE / FROM-subquery stage materialization (engine V2).
//!
//! Every CTE and FROM-subquery boundary materializes: the stage's pipeline
//! drains ONCE into a chunked columnar `MaterializedResult`, and each
//! downstream reference scans it through a `MatScan` leaf — a fresh query
//! pipeline per stage, chained stage-to-stage. Semantics:
//!
//!   - shared stage (MATERIALIZED / default): all references read one result
//!   - regenerated stage (NOT MATERIALIZED): each reference owns its own
//!     Stage over the same IR subtree, recomputed per use
//!
//! Execution is lazy and demand-driven: a stage runs the first time one of
//! its consumers calls `next()`, which recursively runs its own upstream
//! stages the same way. All `next()`/teardown calls happen on the single
//! connection thread driving the root query, so stage state needs no locks.
//!
//! The result frees the moment its LAST consumer finishes draining it — on a
//! background thread so the teardown overlaps the next stage's work — and
//! the thread is joined at StageSet teardown so memory accounting stays
//! deterministic (leak-checked tests included).
//!
//! Chunks are sized for striping (`chunk_rows` ≈ one row group): a future
//! parallel consumer claims chunk ranges exactly like segment row-group
//! tiles. Today each stage has one serial MatScan reader; the parallelism
//! lives inside table-sourced stages (which run the regular V2 handlers).

const std = @import("std");
const Allocator = std.mem.Allocator;

const exec = @import("exec.zig");
const window_mod = @import("window.zig");
const types = @import("../types.zig");
const engine = @import("../engine/engine.zig");
const storage = @import("../storage/storage.zig");

const Column = types.Column;
const ColumnView = storage.ColumnView;

/// A/B escape hatch: `THINDB_MAT_PERROW` forces the legacy per-cell
/// `appendOneRow` materialization loop so the bulk column-major path can be
/// benchmarked against it in a single running server. Default (unset) = bulk.
extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

/// Rows per chunk — mirrors the default segment row-group size so chunk
/// striping behaves like row-group tiling for future parallel readers.
pub const chunk_rows: usize = 65_536;

pub const MaterializedResult = struct {
    allocator: Allocator,
    /// Borrowed from the owning Stage's schema copy.
    schema: []const Column,
    chunks: std.ArrayListUnmanaged(Chunk) = .empty,
    total_rows: u64 = 0,
    /// Provable upper bound on the final row count (the stage's compile-time
    /// `stats_upper_rows`), used to size a fresh chunk's columns exactly: a
    /// small stage allocates a small chunk instead of a full `chunk_rows`.
    expected_rows: u64 = 0,
    /// Set when the result was adopted zero-copy from a producer's own
    /// contiguous buffers (window-output-as-stage) instead of pull-copied.
    /// The chunks are then borrowed view windows over these stores.
    adopted: ?Adopted = null,
    /// The slice-key column name when this result was slice-adopted (chunks
    /// carry key ranges); readers validate their skip column against it.
    sliced_key: ?[]const u8 = null,

    /// Ownership record for adopted contiguous columns: entries flagged in
    /// `arena_backed` are reclaimed wholesale by sweeping `arenas`; the
    /// rest deinit with the result's allocator.
    pub const Adopted = struct {
        stores: []engine.ColumnStore,
        arenas: []std.heap.ArenaAllocator,
        arena_backed: []bool,
    };

    pub const Chunk = struct {
        /// Owned column storage (the pull-copy path). Empty for adopted
        /// results — readers consume `views`.
        cols: []engine.ColumnStore = &.{},
        /// Borrowed view windows over adopted contiguous stores. Chunk
        /// bounds land on 64K-row (byte-aligned) offsets, so the validity
        /// bitmaps slice cleanly.
        views: []ColumnView = &.{},
        rows: usize = 0,
        /// SEPARABLE sliced results: this chunk's slice-key range —
        /// values are in (key_lo, key_hi], null bound = unbounded. A
        /// slice-range reader skips chunks with disjoint ranges (see
        /// MatScan.setSliceSkip). Defaults are the "no information"
        /// state: never skipped.
        key_lo: ?types.Value = null,
        key_hi: ?types.Value = null,
        key_nulls: bool = true,
    };

    /// Take ownership of a producer's contiguous result columns and expose
    /// them as `chunk_rows`-sized view chunks — same shape the parallel
    /// buffer scan partitions, no copy.
    fn adoptContiguous(self: *MaterializedResult, adopted: Adopted, rows: u64) !void {
        self.adopted = adopted;
        const n: usize = @intCast(rows);
        var lo: usize = 0;
        while (lo < n) {
            const take = @min(chunk_rows, n - lo);
            const views = try self.allocator.alloc(ColumnView, adopted.stores.len);
            errdefer self.allocator.free(views);
            for (adopted.stores, self.schema, views) |*st, sc, *v| {
                v.* = presentAsSchemaType(engine.transform.subViewAligned(st.view(), lo, take), sc.type);
            }
            try self.chunks.append(self.allocator, .{ .views = views, .rows = take });
            lo += take;
        }
        self.total_rows = rows;
    }

    /// SEPARABLE sliced fill: adopt N slice sinks' contiguous stores as this
    /// result's chunks, in slice order — a deterministic concat (disjoint key
    /// ranges in ascending slice order also preserve a leading-slice-key
    /// sort). Takes full ownership: the sinks' store/arena arrays are copied
    /// into one flat Adopted record and their top-level arrays freed; the
    /// arena-backed column data frees with the result's normal adopted sweep.
    pub const SliceKey = struct {
        col: []const u8,
        /// Ascending slice boundaries: slice i holds (bounds[i-1], bounds[i]],
        /// open at the ends; NULL keys live in slice 0. Borrowed — must
        /// outlive the result (the sliced-fill ctx owns them).
        bounds: []const types.Value,
    };

    pub fn adoptSlices(self: *MaterializedResult, sinks: []ContigSink, key: ?SliceKey) !void {
        const ncols = self.schema.len;
        const stores = try self.allocator.alloc(engine.ColumnStore, sinks.len * ncols);
        errdefer self.allocator.free(stores);
        const arenas = try self.allocator.alloc(std.heap.ArenaAllocator, sinks.len * ncols);
        errdefer self.allocator.free(arenas);
        const backed = try self.allocator.alloc(bool, sinks.len * ncols);
        errdefer self.allocator.free(backed);
        for (sinks, 0..) |*sk, i| {
            const ad = sk.take();
            @memcpy(stores[i * ncols ..][0..ncols], ad.stores);
            @memcpy(arenas[i * ncols ..][0..ncols], ad.arenas);
            @memcpy(backed[i * ncols ..][0..ncols], ad.arena_backed);
            sk.allocator.free(ad.stores);
            sk.allocator.free(ad.arenas);
            sk.allocator.free(ad.arena_backed);
        }
        self.adopted = .{ .stores = stores, .arenas = arenas, .arena_backed = backed };
        if (key) |k| self.sliced_key = k.col;
        for (sinks, 0..) |*sk, i| {
            const pstores = stores[i * ncols ..][0..ncols];
            const n: usize = @intCast(sk.rows);
            var lo: usize = 0;
            while (lo < n) {
                const take_n = @min(chunk_rows, n - lo);
                const views = try self.allocator.alloc(ColumnView, ncols);
                errdefer self.allocator.free(views);
                for (pstores, self.schema, views) |*st, sc, *v| {
                    v.* = presentAsSchemaType(engine.transform.subViewAligned(st.view(), lo, take_n), sc.type);
                }
                var chunk: Chunk = .{ .views = views, .rows = take_n };
                if (key) |k| {
                    chunk.key_lo = if (i > 0) k.bounds[i - 1] else null;
                    chunk.key_hi = if (i < k.bounds.len) k.bounds[i] else null;
                    chunk.key_nulls = i == 0;
                }
                try self.chunks.append(self.allocator, chunk);
                lo += take_n;
            }
            self.total_rows += sk.rows;
        }
    }

    /// The pull-copy path launders physical string-family tags into the
    /// stage schema's tag (chunk stores are schema-typed); zero-copy
    /// adoption must present the same tags or consumers compiled against
    /// the schema hit the wrong union arm. varchar/string/char share one
    /// physical layout (offsets + bytes), so the retag is free.
    fn presentAsSchemaType(v: ColumnView, t: types.Type) ColumnView {
        const sv = switch (v.data) {
            .varchar, .string, .char => |x| x,
            else => return v,
        };
        return switch (t) {
            .varchar => .{ .data = .{ .varchar = sv }, .nulls = v.nulls },
            .string => .{ .data = .{ .string = sv }, .nulls = v.nulls },
            .char => .{ .data = .{ .char = sv }, .nulls = v.nulls },
            else => v,
        };
    }

    fn appendBatch(self: *MaterializedResult, batch: exec.Batch) !void {
        const per_row = getenv("THINDB_MAT_PERROW") != null;
        var row: usize = 0;
        while (row < batch.row_count) {
            const chunk = try self.openChunk();
            const take = @min(chunk_rows - chunk.rows, batch.row_count - row);
            if (per_row) {
                var r = row;
                while (r < row + take) : (r += 1) {
                    for (chunk.cols, 0..) |*dst, ci| {
                        try engine.transform.appendOneRow(self.allocator, batch.values[ci], r, dst);
                    }
                }
            } else {
                for (chunk.cols, 0..) |*dst, ci| {
                    try engine.transform.appendColumnRange(self.allocator, batch.values[ci], row, row + take, dst);
                }
            }
            chunk.rows += take;
            self.total_rows += take;
            row += take;
        }
    }

    /// The last chunk if it has room, else a fresh one — with every row-counted
    /// buffer (fixed-width data, string offsets, validity bitmap) pre-sized to
    /// `cap_rows` so the fill loop never reallocates them. `cap_rows` is exact:
    /// the provable row-count bound caps it below `chunk_rows` for small stages.
    /// A string column's variable-width byte buffer is reserved from the
    /// PREVIOUS full chunk's exact bytes/row (+10% slack) — a strong, locally
    /// adaptive predictor that tracks clustered data without the global-average
    /// variance that would under-reserve a dense chunk into a geometric realloc
    /// that then stays bloated. The first chunk has no predecessor, so its byte
    /// buffer grows naturally (a single chunk's worth of reallocs).
    fn openChunk(self: *MaterializedResult) !*Chunk {
        const prev: ?*Chunk = if (self.chunks.items.len > 0) &self.chunks.items[self.chunks.items.len - 1] else null;
        if (prev) |last| {
            if (last.rows < chunk_rows) return last;
        }
        const remaining: u64 = if (self.expected_rows > self.total_rows) self.expected_rows - self.total_rows else 0;
        const cap_rows: usize = if (remaining == 0) chunk_rows else @intCast(@min(@as(u64, chunk_rows), remaining));
        const cols = try self.allocator.alloc(engine.ColumnStore, self.schema.len);
        var inited: usize = 0;
        errdefer {
            for (cols[0..inited]) |*c| c.deinit(self.allocator);
            self.allocator.free(cols);
        }
        for (self.schema, 0..) |sc, i| {
            const bytes_cap: usize = switch (sc.type) {
                .varchar, .string, .char => blk: {
                    const p = prev orelse break :blk 0;
                    if (p.rows == 0) break :blk 0;
                    const bpr = (colStrBytes(&p.cols[i]) + p.rows - 1) / p.rows;
                    break :blk ((bpr * cap_rows) * 11) / 10;
                },
                else => 0,
            };
            cols[i] = try engine.ColumnStore.initCapacity(self.allocator, sc.type, sc.nullable, cap_rows, bytes_cap);
            inited += 1;
        }
        try self.chunks.append(self.allocator, .{ .cols = cols });
        return &self.chunks.items[self.chunks.items.len - 1];
    }

    /// Bytes held by a string-family column store; 0 for fixed-width.
    fn colStrBytes(col: *const engine.ColumnStore) usize {
        return switch (col.data) {
            .varchar, .string, .char => |ss| ss.bytes.items.len,
            else => 0,
        };
    }

    fn deinitChunks(self: *MaterializedResult) void {
        for (self.chunks.items) |*c| {
            for (c.cols) |*col| col.deinit(self.allocator);
            if (c.cols.len > 0) self.allocator.free(c.cols);
            if (c.views.len > 0) self.allocator.free(c.views);
        }
        self.chunks.deinit(self.allocator);
        if (self.adopted) |*ad| {
            for (ad.stores, ad.arena_backed) |*st, ab| {
                if (!ab) st.deinit(self.allocator);
            }
            for (ad.arenas) |*a| a.deinit();
            self.allocator.free(ad.stores);
            self.allocator.free(ad.arenas);
            self.allocator.free(ad.arena_backed);
            self.adopted = null;
        }
    }
};

/// Accumulates a stage's pull-copied result as ONE contiguous store per
/// column (adopted-style; per-column arenas so the sweep-free ownership
/// story matches window adoption), for stages a downstream window wants to
/// borrow from. Fixed-width buffers pre-reserve the stage's row bound.
pub const ContigSink = struct {
    allocator: Allocator,
    stores: []engine.ColumnStore,
    arenas: []std.heap.ArenaAllocator,
    arena_backed: []bool,
    rows: u64 = 0,
    taken: bool = false,

    pub fn init(allocator: Allocator, schema: []const Column, expect_rows: usize) !ContigSink {
        const stores = try allocator.alloc(engine.ColumnStore, schema.len);
        errdefer allocator.free(stores);
        const arenas = try allocator.alloc(std.heap.ArenaAllocator, schema.len);
        errdefer allocator.free(arenas);
        const arena_backed = try allocator.alloc(bool, schema.len);
        errdefer allocator.free(arena_backed);
        @memset(arena_backed, true);
        for (arenas) |*a| a.* = std.heap.ArenaAllocator.init(std.heap.c_allocator);
        errdefer for (arenas) |*a| a.deinit();
        for (schema, stores, arenas) |sc, *st, *ar| {
            st.* = try engine.ColumnStore.initCapacity(ar.allocator(), sc.type, sc.nullable, expect_rows, 0);
        }
        return .{ .allocator = allocator, .stores = stores, .arenas = arenas, .arena_backed = arena_backed };
    }

    pub fn append(self: *ContigSink, batch: exec.Batch) !void {
        for (self.stores, self.arenas, 0..) |*st, *ar, ci| {
            try engine.transform.appendAllColumn(ar.allocator(), batch.values[ci], st);
        }
        self.rows += batch.row_count;
    }

    pub fn take(self: *ContigSink) MaterializedResult.Adopted {
        self.taken = true;
        return .{ .stores = self.stores, .arenas = self.arenas, .arena_backed = self.arena_backed };
    }

    pub fn deinit(self: *ContigSink) void {
        if (self.taken) return;
        for (self.arenas) |*a| a.deinit();
        self.allocator.free(self.stores);
        self.allocator.free(self.arenas);
        self.allocator.free(self.arena_backed);
    }
};

/// One materialization boundary: a compiled pipeline, run at most once, and
/// its result's consumer accounting.
pub const Stage = struct {
    allocator: Allocator,
    /// The stage's compiled pipeline. Drained and torn down by `ensureRun`;
    /// torn down by StageSet if the stage never ran (error paths, LIMIT
    /// upstream cutting the plan short).
    query: exec.Query,
    query_alive: bool = true,
    /// Deep copy of the pipeline's output schema (StageSet arena) — the
    /// pipeline itself is gone after `ensureRun`, but MatScan batches and
    /// the result keep referring to this.
    schema: []const Column,
    /// The pipeline's compile-time output stats, captured at `addStage`
    /// before the pipeline can be torn down (column stats and sort keys are
    /// arena copies). These are provable bounds on the materialized result —
    /// the result preserves the pipeline's row order, so the sort state
    /// carries over verbatim. `ensureRun` tightens `upper_rows` to the exact
    /// materialized count and caps the NDV bounds, so consumers compiled (or
    /// re-querying stats) after the stage ran see exact figures.
    stats_upper_rows: u64,
    sort_state: exec.SortState,
    col_stats: []exec.ColStat,
    result: ?*MaterializedResult = null,
    /// Consumers bound at compile time / consumers finished. When the last
    /// finishes, the result frees in the background. Atomic: SEPARABLE
    /// slice pipelines register (compile) and release (deinit) from their
    /// own threads; the FINAL release is still single-threaded — the fill
    /// holds a guard use per input stage until every slice has joined.
    uses_total: std.atomic.Value(usize) = .init(0),
    uses_done: std.atomic.Value(usize) = .init(0),
    free_thread: ?std.Thread = null,
    /// Query-scoped accountant the buffered result is charged against
    /// (null = no tracking). All reserve/release calls happen on the
    /// driving connection thread — the background free thread only frees
    /// memory, never touches accounting.
    accountant: ?*exec.memory.MemoryAccountant = null,
    reserved_bytes: usize = 0,
    /// Compile-order index in the StageSet — a stable label for `--profile-ops`
    /// per-CTE timing (`[cte]` lines), nothing more.
    id: usize = 0,
    /// `--profile-ops` only: ticks spent compiling this block's body (setup),
    /// recorded by the staged compiler before the drain runs.
    setup_ticks: i64 = 0,
    /// Window-output-as-stage: when the compiled pipeline IS a Window
    /// operator (set via exec.queryAs by the staged compiler), ensureRun
    /// adopts its materialized buffers zero-copy instead of pull-copying
    /// the emit stream.
    adopt_window: ?*window_mod.Window = null,
    /// Column borrowing: this stage's adopted result contains shallow
    /// references into an upstream stage's contiguous buffers (the window
    /// borrowed its pass-through input columns), so that stage must stay
    /// alive as long as this result. One use registered at compile,
    /// released when this stage's result frees. Pins chain transitively.
    pinned_upstream: ?*Stage = null,
    /// A compile-time borrower exists downstream (a window on a row-aligned
    /// chain): materialize this stage's result as contiguous columns
    /// (adopted-style, view chunks) instead of per-chunk stores, so the
    /// borrower can take shallow references. Set by the staged compiler
    /// BEFORE any run; a stage that already ran eagerly just misses the
    /// optimization (the borrower's bind degrades to normal accumulation).
    want_contiguous: bool = false,
    /// SEPARABLE BY: the stage fills by running N per-key-range slice
    /// pipelines concurrently and adopting their outputs in slice order,
    /// instead of draining `query`. Installed by the staged compiler; the
    /// hook lives in net/cte_stages (it re-enters the block compiler), so
    /// the coupling stays behind opaque fn pointers — same philosophy as
    /// `adopt_window`. `query` still compiles normally (it provides the
    /// schema/stats and is torn down undrained).
    sliced_fill: ?SlicedFill = null,

    pub const SlicedFill = struct {
        ctx: *anyopaque,
        /// Returns false to DECLINE (no sliceable input, degenerate bounds):
        /// ensureRun then falls through to the normal drain of `query`.
        run: *const fn (ctx: *anyopaque, stage: *Stage, res: *MaterializedResult) anyerror!bool,
        drop: *const fn (ctx: *anyopaque) void,
    };

    pub fn ensureRun(self: *Stage) anyerror!void {
        if (self.result != null) return;
        if (!self.query_alive) return error.UnsupportedQueryShape; // re-run after teardown: can't happen via MatScan
        const prof_on = exec.prof.enabled;
        const w0 = if (prof_on) exec.prof.nowTicks() else 0;
        const child0 = if (prof_on) exec.prof.cteChildTicks() else 0;
        const snap = if (prof_on) exec.prof.snapSlots() else undefined;
        var append_ticks: i64 = 0;
        const res = try self.allocator.create(MaterializedResult);
        errdefer self.allocator.destroy(res);
        res.* = .{ .allocator = self.allocator, .schema = self.schema, .expected_rows = self.stats_upper_rows };
        errdefer res.deinitChunks();
        errdefer self.releaseReserved();
        const row_bytes = exec.memory.estimateRowBytes(self.schema);
        var sliced = false;
        if (self.sliced_fill) |sf| {
            sliced = try sf.run(sf.ctx, self, res);
            if (sliced) {
                if (self.accountant) |acct| {
                    const bytes = row_bytes * @as(usize, @intCast(res.total_rows));
                    try acct.reserve(.materialize, bytes);
                    self.reserved_bytes += bytes;
                }
            }
        }
        if (sliced) {} else if (self.adopt_window) |win| {
            try win.ensureDrained();
            if (self.accountant) |acct| {
                const bytes = row_bytes * @as(usize, @intCast(win.accumulated_rows));
                try acct.reserve(.materialize, bytes);
                self.reserved_bytes += bytes;
            }
            const ad = try win.adoptBuffers();
            try res.adoptContiguous(
                .{ .stores = ad.stores, .arenas = ad.arenas, .arena_backed = ad.arena_backed },
                ad.rows,
            );
        } else if (self.want_contiguous) {
            var contig = try ContigSink.init(self.allocator, self.schema, self.expectedRowsHint());
            errdefer contig.deinit();
            while (try self.query.next()) |batch| {
                if (self.accountant) |acct| {
                    const bytes = row_bytes * batch.row_count;
                    try acct.reserve(.materialize, bytes);
                    self.reserved_bytes += bytes;
                }
                const a0 = if (prof_on) exec.prof.nowTicks() else 0;
                try contig.append(batch);
                if (prof_on) append_ticks += exec.prof.nowTicks() - a0;
            }
            try res.adoptContiguous(contig.take(), contig.rows);
        } else while (try self.query.next()) |batch| {
            if (self.accountant) |acct| {
                const bytes = row_bytes * batch.row_count;
                try acct.reserve(.materialize, bytes);
                self.reserved_bytes += bytes;
            }
            const a0 = if (prof_on) exec.prof.nowTicks() else 0;
            try res.appendBatch(batch);
            if (prof_on) append_ticks += exec.prof.nowTicks() - a0;
        }
        // Release the pipeline's operator buffers right away — the stage's
        // working memory drops to just the chunked result.
        const td0 = if (prof_on) exec.prof.nowTicks() else 0;
        self.query.deinit();
        const teardown = if (prof_on) exec.prof.nowTicks() - td0 else 0;
        self.query_alive = false;
        self.result = res;
        self.stats_upper_rows = res.total_rows;
        exec.capColStats(self.col_stats, res.total_rows);
        if (prof_on) {
            const wall = exec.prof.nowTicks() - w0;
            // Nested upstream stages triggered lazily during this drain charge
            // their own wall to cte_child_ticks; subtract so this line is SELF.
            const children: i64 = @intCast(exec.prof.cteChildTicks() - child0);
            exec.prof.dumpStageDelta(self.id, res.total_rows, wall - children, wall, self.setup_ticks, teardown, snap);
            exec.prof.addCteChildTicks(@intCast(@max(wall, 0)));
            if (append_ticks > 0) std.debug.print("[stage-append] stage#{d} copy={d:.1}ms rows={d} contig={}\n", .{
                self.id, exec.prof.ticksToMs(append_ticks), res.total_rows, self.want_contiguous,
            });
        }
    }

    fn expectedRowsHint(self: *const Stage) usize {
        const cap: u64 = 1 << 22; // don't pre-reserve absurd compile-time bounds
        return @intCast(@min(self.stats_upper_rows, cap));
    }

    fn releaseReserved(self: *Stage) void {
        if (self.reserved_bytes == 0) return;
        if (self.accountant) |acct| acct.release(.materialize, self.reserved_bytes);
        self.reserved_bytes = 0;
    }

    /// Register a consumer at compile time — parity with `MatScan.create`'s
    /// `uses_total` bump. A parallel buffer scan (which fans out to N workers
    /// over the one immutable result) counts as ONE use; it registers here and
    /// releases once at its own deinit.
    pub fn registerUse(self: *Stage) void {
        _ = self.uses_total.fetchAdd(1, .monotonic);
    }

    /// A consumer finished (fully drained or torn down). On the last one,
    /// hand the chunks to a background free so the teardown overlaps the
    /// next stage; the thread is joined in `deinit`. The budget is handed
    /// back HERE (driving thread, before the async free) so accounting
    /// stays deterministic for the caller.
    pub fn releaseUse(self: *Stage) void {
        const done = self.uses_done.fetchAdd(1, .acq_rel) + 1;
        if (done < self.uses_total.load(.acquire)) return;
        const res = self.result orelse return;
        self.result = null;
        self.releaseReserved();
        if (self.pinned_upstream) |src| {
            self.pinned_upstream = null;
            src.releaseUse();
        }
        if (std.Thread.spawn(.{}, freeResultThread, .{res})) |th| {
            self.free_thread = th;
        } else |_| {
            freeResultThread(res);
        }
    }

    fn deinit(self: *Stage) void {
        if (self.sliced_fill) |sf| sf.drop(sf.ctx);
        if (self.free_thread) |th| th.join();
        if (self.result) |res| freeResultThread(res);
        self.releaseReserved();
        if (self.query_alive) self.query.deinit();
        self.allocator.destroy(self);
    }
};

fn freeResultThread(res: *MaterializedResult) void {
    const allocator = res.allocator;
    res.deinitChunks();
    allocator.destroy(res);
}

/// Owns every Stage of one staged query plus the arena backing their schema
/// copies. Torn down by the StagedRoot wrapper after the root pipeline.
pub const StageSet = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    stages: std.ArrayListUnmanaged(*Stage) = .empty,

    pub fn create(allocator: Allocator) !*StageSet {
        const self = try allocator.create(StageSet);
        self.* = .{ .allocator = allocator, .arena = std.heap.ArenaAllocator.init(allocator) };
        return self;
    }

    /// Wrap a compiled stage pipeline. Takes ownership of `query` (even on
    /// error). The schema is deep-copied into the set's arena. `accountant`
    /// (null = no tracking) is charged for the buffered result while it
    /// lives — reserved as the stage drains, released when the last reader
    /// finishes.
    pub fn addStage(self: *StageSet, query: exec.Query, accountant: ?*exec.memory.MemoryAccountant) !*Stage {
        var q = query;
        errdefer q.deinit();
        const aa = self.arena.allocator();
        const src = q.outputSchema();
        const schema = try aa.alloc(Column, src.len);
        for (src, 0..) |c, i| {
            schema[i] = .{
                .name = try aa.dupe(u8, c.name),
                .type = c.type,
                .nullable = c.nullable,
            };
        }
        const src_stats = q.stats();
        // A per-column array of the wrong length carries no usable mapping —
        // treat it like "no information" rather than misattribute bounds.
        const col_stats = try aa.dupe(
            exec.ColStat,
            if (src_stats.column_stats.len == src.len) src_stats.column_stats else &.{},
        );
        const sort_keys = try aa.alloc([]const u8, src_stats.sort_state.keys.len);
        for (src_stats.sort_state.keys, sort_keys) |k, *d| d.* = try aa.dupe(u8, k);
        const sort_state: exec.SortState = .{
            .keys = sort_keys,
            .descs = try aa.dupe(bool, src_stats.sort_state.descs),
            .global = src_stats.sort_state.global,
        };
        const stage = try self.allocator.create(Stage);
        errdefer self.allocator.destroy(stage);
        stage.* = .{
            .allocator = self.allocator,
            .query = q,
            .schema = schema,
            .stats_upper_rows = src_stats.upper_rows,
            .sort_state = sort_state,
            .col_stats = col_stats,
            .accountant = accountant,
            .id = self.stages.items.len,
            // Compile pin: chains over stages run EAGERLY during compilation
            // (createOverStage's barrier), and such a run can fully drain an
            // upstream stage whose later consumers haven't compiled (and so
            // haven't registered) yet — without the pin, the result would
            // free out from under them. Released by releaseCompilePins once
            // the whole plan is built.
            .uses_total = .init(1),
        };
        try self.stages.append(self.allocator, stage);
        return stage;
    }

    /// Release every stage's compile pin (see addStage) — call exactly once,
    /// after the whole plan (all stage bodies + the root block) has compiled
    /// and every real consumer is registered. A stage fully drained during an
    /// eager compile-time run frees here; the rest free when their last
    /// runtime reader finishes.
    pub fn releaseCompilePins(self: *StageSet) void {
        for (self.stages.items) |stage| stage.releaseUse();
    }

    pub fn deinit(self: *StageSet) void {
        var i = self.stages.items.len;
        while (i > 0) {
            i -= 1;
            self.stages.items[i].deinit();
        }
        self.stages.deinit(self.allocator);
        self.arena.deinit();
        self.allocator.destroy(self);
    }
};

/// Scan leaf over a stage's materialized result: one chunk per `next()`,
/// views borrowed straight from the chunk's column stores (zero copy).
/// Triggers the stage's (and transitively its upstreams') execution on
/// first use; releases its use on exhaustion so the result can free while
/// downstream operators keep working.
pub const MatScan = struct {
    allocator: Allocator,
    stage: *Stage,
    views: []ColumnView,
    cursor: usize = 0,
    released: bool = false,
    /// Slice-range chunk skip hint (see setSliceSkip).
    skip_col: ?[]const u8 = null,
    skip_lo: ?types.Value = null,
    skip_hi: ?types.Value = null,
    skip_want_null: bool = false,

    pub fn create(allocator: Allocator, stage: *Stage) !exec.Query {
        const views = try allocator.alloc(ColumnView, stage.schema.len);
        errdefer allocator.free(views);
        const self = try allocator.create(MatScan);
        self.* = .{ .allocator = allocator, .stage = stage, .views = views };
        stage.registerUse();
        return exec.makeQuery(allocator, self);
    }

    /// SEPARABLE slice reader hint: this scan feeds a range filter
    /// `col > lo AND col <= hi` (null bound = open; `want_null` when the
    /// filter also passes NULL keys). Chunks of a slice-adopted result carry
    /// key ranges — disjoint chunks are skipped wholesale. Pure optimization:
    /// the filter above still evaluates, so a wrong hint can only cost rows
    /// it would have discarded anyway... never invent rows.
    pub fn setSliceSkip(self: *MatScan, col: []const u8, lo: ?types.Value, hi: ?types.Value, want_null: bool) void {
        self.skip_col = col;
        self.skip_lo = lo;
        self.skip_hi = hi;
        self.skip_want_null = want_null;
    }

    fn chunkDisjoint(self: *const MatScan, chunk: *const MaterializedResult.Chunk) bool {
        if (self.skip_want_null and chunk.key_nulls) return false;
        if (self.skip_lo) |lo| {
            // Chunk holds values <= key_hi; filter needs values > lo.
            if (chunk.key_hi) |chi| {
                if (chi.compare(lo) != .gt) return true;
            }
        }
        if (self.skip_hi) |hi| {
            // Chunk holds values > key_lo; filter needs values <= hi.
            if (chunk.key_lo) |clo| {
                if (clo.compare(hi) != .lt) return true;
            }
        }
        return false;
    }

    pub fn next(self: *MatScan) !?exec.Batch {
        try self.stage.ensureRun();
        const res = self.stage.result orelse {
            self.releaseOnce();
            return null;
        };
        const skip_active = self.skip_col != null and res.sliced_key != null and
            types.columnNameEql(self.skip_col.?, res.sliced_key.?);
        while (self.cursor < res.chunks.items.len) {
            const chunk = res.chunks.items[self.cursor];
            self.cursor += 1;
            if (chunk.rows == 0) continue;
            if (skip_active and self.chunkDisjoint(&chunk)) continue;
            if (chunk.views.len > 0) {
                return exec.Batch{
                    .schema = self.stage.schema,
                    .values = chunk.views,
                    .row_count = chunk.rows,
                };
            }
            for (chunk.cols, 0..) |*col, i| self.views[i] = col.view();
            return exec.Batch{
                .schema = self.stage.schema,
                .values = self.views,
                .row_count = chunk.rows,
            };
        }
        self.releaseOnce();
        return null;
    }

    fn releaseOnce(self: *MatScan) void {
        if (self.released) return;
        self.released = true;
        self.stage.releaseUse();
    }

    pub fn deinit(self: *MatScan) void {
        self.releaseOnce();
        self.allocator.free(self.views);
        self.allocator.destroy(self);
    }

    pub fn outputSchema(self: *MatScan) []const Column {
        return self.stage.schema;
    }

    pub fn addPrune(_: *MatScan, _: exec.Predicate) !void {}

    pub fn stats(self: *MatScan) exec.PipelineStats {
        return .{
            .upper_rows = self.stage.stats_upper_rows,
            .sort_state = self.stage.sort_state,
            .column_stats = self.stage.col_stats,
        };
    }

    pub fn accountant(_: *MatScan) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(self: *MatScan, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        var buf: [64]u8 = undefined;
        const line = std.fmt.bufPrint(&buf, "MatScan cols={d}", .{self.stage.schema.len}) catch "MatScan";
        try exec.explainLine(out, allocator, depth, line);
    }
};

/// A ranged, lock-free reader over a `[chunk_lo, chunk_hi)` stripe of a stage's
/// already-materialized result — the buffer-side analogue of a range-restricted
/// `Scan` worker. The parallel buffer scan hands each worker a disjoint stripe;
/// they read concurrently with no locks because the `MaterializedResult` is
/// immutable once `ensureRun` completes and every emitted `ColumnView` borrows
/// `[]const` chunk storage.
///
/// Unlike `MatScan`, `next` does NOT call `ensureRun` — the orchestrator runs
/// the stage ONCE (its barrier) before spawning workers, so a per-worker
/// `ensureRun` would race. `stats` reports the whole stage's stats (not just the
/// stripe), mirroring how a ranged table `Scan` reports whole-table stats — so
/// `workers[0].stats()` yields the full-result figures the routing layer needs.
///
/// Ownership: a ChunkRangeScan borrows the result; it never registers a stage
/// `use` (the single parent parallel-scan counts as one use and releases it).
pub const ChunkRangeScan = struct {
    allocator: Allocator,
    stage: *Stage,
    result: *const MaterializedResult,
    views: []ColumnView,
    chunk_lo: usize,
    chunk_hi: usize,
    cursor: usize,

    /// Raw constructor returning the concrete pointer — the parallel buffer
    /// scan stores these in its worker array (parity with `Scan`'s raw alloc).
    pub fn alloc(allocator: Allocator, stage: *Stage, result: *const MaterializedResult, chunk_lo: usize, chunk_hi: usize) !*ChunkRangeScan {
        const views = try allocator.alloc(ColumnView, stage.schema.len);
        errdefer allocator.free(views);
        const self = try allocator.create(ChunkRangeScan);
        self.* = .{
            .allocator = allocator,
            .stage = stage,
            .result = result,
            .views = views,
            .chunk_lo = chunk_lo,
            .chunk_hi = chunk_hi,
            .cursor = chunk_lo,
        };
        return self;
    }

    pub fn create(allocator: Allocator, stage: *Stage, result: *const MaterializedResult, chunk_lo: usize, chunk_hi: usize) !exec.Query {
        return exec.makeQuery(allocator, try alloc(allocator, stage, result, chunk_lo, chunk_hi));
    }

    pub fn next(self: *ChunkRangeScan) !?exec.Batch {
        while (self.cursor < self.chunk_hi) {
            const chunk = self.result.chunks.items[self.cursor];
            self.cursor += 1;
            if (chunk.rows == 0) continue;
            if (chunk.views.len > 0) {
                return exec.Batch{
                    .schema = self.stage.schema,
                    .values = chunk.views,
                    .row_count = chunk.rows,
                };
            }
            for (chunk.cols, 0..) |*col, i| self.views[i] = col.view();
            return exec.Batch{
                .schema = self.stage.schema,
                .values = self.views,
                .row_count = chunk.rows,
            };
        }
        return null;
    }

    pub fn outputSchema(self: *ChunkRangeScan) []const Column {
        return self.stage.schema;
    }

    pub fn stats(self: *ChunkRangeScan) exec.PipelineStats {
        return .{
            .upper_rows = self.stage.stats_upper_rows,
            .sort_state = self.stage.sort_state,
            .column_stats = self.stage.col_stats,
        };
    }

    pub fn addPrune(_: *ChunkRangeScan, _: exec.Predicate) !void {}

    pub fn accountant(_: *ChunkRangeScan) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn deinit(self: *ChunkRangeScan) void {
        self.allocator.free(self.views);
        self.allocator.destroy(self);
    }

    pub fn explain(self: *ChunkRangeScan, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        var buf: [80]u8 = undefined;
        const line = std.fmt.bufPrint(&buf, "ChunkRangeScan cols={d} chunks=[{d},{d})", .{ self.stage.schema.len, self.chunk_lo, self.chunk_hi }) catch "ChunkRangeScan";
        try exec.explainLine(out, allocator, depth, line);
    }
};

test "stage captures pipeline stats and tightens them after the run" {
    const allocator = std.testing.allocator;

    const Stub = struct {
        allocator: Allocator,
        schema: [1]Column = .{.{ .name = "k", .type = .{ .bigint = {} }, .nullable = false }},
        col_stats: [1]exec.ColStat = .{.{ .ndv = .{ .exact = 7 }, .min = 1, .max = 9 }},

        pub fn next(_: *@This()) !?exec.Batch {
            return null;
        }
        pub fn deinit(self: *@This()) void {
            self.allocator.destroy(self);
        }
        pub fn outputSchema(self: *@This()) []const Column {
            return &self.schema;
        }
        pub fn addPrune(_: *@This(), _: exec.Predicate) !void {}
        pub fn stats(self: *@This()) exec.PipelineStats {
            return .{
                .upper_rows = 42,
                .sort_state = .{ .keys = &.{"k"}, .global = true },
                .column_stats = &self.col_stats,
            };
        }
        pub fn accountant(_: *@This()) ?*exec.memory.MemoryAccountant {
            return null;
        }
        pub fn explain(_: *@This(), out: *std.ArrayList(u8), a: Allocator, depth: usize) !void {
            try exec.explainLine(out, a, depth, "Stub");
        }
    };

    const stub = try allocator.create(Stub);
    stub.* = .{ .allocator = allocator };
    const sq = exec.makeQuery(allocator, stub);

    const set = try StageSet.create(allocator);
    defer set.deinit();
    const stage = try set.addStage(sq, null);

    var scan = try MatScan.create(allocator, stage);
    defer scan.deinit();

    // Pre-run: the boundary serves the source pipeline's provable bounds.
    const pre = scan.stats();
    try std.testing.expectEqual(@as(u64, 42), pre.upper_rows);
    try std.testing.expectEqual(@as(u32, 7), pre.column_stats[0].ndv.exact);
    try std.testing.expectEqual(@as(?i128, 1), pre.column_stats[0].min);
    try std.testing.expectEqualStrings("k", pre.sort_state.keys[0]);
    try std.testing.expect(pre.sort_state.global);

    // Drain (the stub emits nothing): exact figures replace the bounds.
    try std.testing.expect((try scan.next()) == null);
    const post = scan.stats();
    try std.testing.expectEqual(@as(u64, 0), post.upper_rows);
    try std.testing.expectEqual(@as(u32, 0), post.column_stats[0].ndv.exact);
    try std.testing.expectEqualStrings("k", post.sort_state.keys[0]);
}

test "ChunkRangeScan: disjoint stripes cover every row exactly once" {
    const allocator = std.testing.allocator;

    // A stub that emits all `n` rows of a single bigint column in one batch,
    // then null — the stage chunks them at `chunk_rows`.
    const EmitStub = struct {
        allocator: Allocator,
        schema: [1]Column = .{.{ .name = "v", .type = .{ .bigint = {} }, .nullable = false }},
        data: []i64,
        views: [1]ColumnView = undefined,
        emitted: bool = false,

        pub fn next(self: *@This()) !?exec.Batch {
            if (self.emitted) return null;
            self.emitted = true;
            self.views[0] = .{ .data = .{ .bigint = self.data }, .nulls = null };
            return exec.Batch{ .schema = &self.schema, .values = &self.views, .row_count = self.data.len };
        }
        pub fn deinit(self: *@This()) void {
            self.allocator.destroy(self);
        }
        pub fn outputSchema(self: *@This()) []const Column {
            return &self.schema;
        }
        pub fn addPrune(_: *@This(), _: exec.Predicate) !void {}
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

    // One chunk (3 rows) and a multi-chunk result (> chunk_rows) exercise both
    // the in-range and the stripe-boundary paths.
    const row_counts = [_]usize{ 3, chunk_rows + 100 };
    for (row_counts) |n| {
        const data = try allocator.alloc(i64, n);
        defer allocator.free(data);
        for (data, 0..) |*d, i| d.* = @intCast(i);

        const stub = try allocator.create(EmitStub);
        stub.* = .{ .allocator = allocator, .data = data };
        const sq = exec.makeQuery(allocator, stub);

        const set = try StageSet.create(allocator);
        defer set.deinit();
        const stage = try set.addStage(sq, null);
        try stage.ensureRun();
        const res = stage.result.?;
        try std.testing.expectEqual(@as(u64, n), res.total_rows);

        // Split the chunk list into two disjoint stripes and confirm together
        // they read every row exactly once, in order.
        const n_chunks = res.chunks.items.len;
        const mid = n_chunks / 2;
        var seen: u64 = 0;
        var expect_val: i64 = 0;
        const ranges = [_][2]usize{ .{ 0, mid }, .{ mid, n_chunks } };
        for (ranges) |r| {
            var leaf = try ChunkRangeScan.create(allocator, stage, res, r[0], r[1]);
            defer leaf.deinit();
            while (try leaf.next()) |batch| {
                const vals = batch.values[0].data.bigint;
                for (vals) |v| {
                    try std.testing.expectEqual(expect_val, v);
                    expect_val += 1;
                    seen += 1;
                }
            }
        }
        try std.testing.expectEqual(@as(u64, n), seen);
    }
}

/// Root wrapper: forwards everything to the outermost pipeline and tears the
/// StageSet down after it.
pub const StagedRoot = struct {
    allocator: Allocator,
    inner: exec.Query,
    set: *StageSet,

    pub fn create(allocator: Allocator, inner: exec.Query, set: *StageSet) !exec.Query {
        const self = try allocator.create(StagedRoot);
        self.* = .{ .allocator = allocator, .inner = inner, .set = set };
        return exec.makeQuery(allocator, self);
    }

    pub fn next(self: *StagedRoot) !?exec.Batch {
        return self.inner.next();
    }

    pub fn deinit(self: *StagedRoot) void {
        self.inner.deinit();
        self.set.deinit();
        self.allocator.destroy(self);
    }

    pub fn outputSchema(self: *StagedRoot) []const Column {
        return self.inner.outputSchema();
    }

    pub fn addPrune(self: *StagedRoot, pred: exec.Predicate) !void {
        return self.inner.addPrune(pred);
    }

    pub fn stats(self: *StagedRoot) exec.PipelineStats {
        return self.inner.stats();
    }

    pub fn accountant(self: *StagedRoot) ?*exec.memory.MemoryAccountant {
        return self.inner.accountant();
    }

    pub fn explain(self: *StagedRoot, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "StagedRoot");
        try self.inner.explain(out, allocator, depth + 1);
    }
};
