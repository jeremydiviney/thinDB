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
const types = @import("../types.zig");
const engine = @import("../engine/engine.zig");
const storage = @import("../storage/storage.zig");

const Column = types.Column;
const ColumnView = storage.ColumnView;

/// Rows per chunk — mirrors the default segment row-group size so chunk
/// striping behaves like row-group tiling for future parallel readers.
pub const chunk_rows: usize = 65_536;

pub const MaterializedResult = struct {
    allocator: Allocator,
    /// Borrowed from the owning Stage's schema copy.
    schema: []const Column,
    chunks: std.ArrayListUnmanaged(Chunk) = .empty,
    total_rows: u64 = 0,

    pub const Chunk = struct {
        cols: []engine.ColumnStore,
        rows: usize = 0,
    };

    fn appendBatch(self: *MaterializedResult, batch: exec.Batch) !void {
        var row: usize = 0;
        while (row < batch.row_count) {
            const chunk = try self.openChunk();
            const take = @min(chunk_rows - chunk.rows, batch.row_count - row);
            var r = row;
            while (r < row + take) : (r += 1) {
                for (chunk.cols, 0..) |*dst, ci| {
                    try engine.transform.appendOneRow(self.allocator, batch.values[ci], r, dst);
                }
            }
            chunk.rows += take;
            self.total_rows += take;
            row += take;
        }
    }

    /// The last chunk if it has room, else a fresh one.
    fn openChunk(self: *MaterializedResult) !*Chunk {
        if (self.chunks.items.len > 0) {
            const last = &self.chunks.items[self.chunks.items.len - 1];
            if (last.rows < chunk_rows) return last;
        }
        const cols = try self.allocator.alloc(engine.ColumnStore, self.schema.len);
        var inited: usize = 0;
        errdefer {
            for (cols[0..inited]) |*c| c.deinit(self.allocator);
            self.allocator.free(cols);
        }
        for (self.schema, 0..) |sc, i| {
            cols[i] = try engine.ColumnStore.init(self.allocator, sc.type, sc.nullable);
            inited += 1;
        }
        try self.chunks.append(self.allocator, .{ .cols = cols });
        return &self.chunks.items[self.chunks.items.len - 1];
    }

    fn deinitChunks(self: *MaterializedResult) void {
        for (self.chunks.items) |*c| {
            for (c.cols) |*col| col.deinit(self.allocator);
            self.allocator.free(c.cols);
        }
        self.chunks.deinit(self.allocator);
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
    /// finishes, the result frees in the background.
    uses_total: usize = 0,
    uses_done: usize = 0,
    free_thread: ?std.Thread = null,
    /// Query-scoped accountant the buffered result is charged against
    /// (null = no tracking). All reserve/release calls happen on the
    /// driving connection thread — the background free thread only frees
    /// memory, never touches accounting.
    accountant: ?*exec.memory.MemoryAccountant = null,
    reserved_bytes: usize = 0,

    pub fn ensureRun(self: *Stage) anyerror!void {
        if (self.result != null) return;
        if (!self.query_alive) return error.UnsupportedQueryShape; // re-run after teardown: can't happen via MatScan
        const res = try self.allocator.create(MaterializedResult);
        errdefer self.allocator.destroy(res);
        res.* = .{ .allocator = self.allocator, .schema = self.schema };
        errdefer res.deinitChunks();
        errdefer self.releaseReserved();
        const row_bytes = exec.memory.estimateRowBytes(self.schema);
        while (try self.query.next()) |batch| {
            if (self.accountant) |acct| {
                const bytes = row_bytes * batch.row_count;
                try acct.reserve(.materialize, bytes);
                self.reserved_bytes += bytes;
            }
            try res.appendBatch(batch);
        }
        // Release the pipeline's operator buffers right away — the stage's
        // working memory drops to just the chunked result.
        self.query.deinit();
        self.query_alive = false;
        self.result = res;
        self.stats_upper_rows = res.total_rows;
        exec.capColStats(self.col_stats, res.total_rows);
    }

    fn releaseReserved(self: *Stage) void {
        if (self.reserved_bytes == 0) return;
        if (self.accountant) |acct| acct.release(.materialize, self.reserved_bytes);
        self.reserved_bytes = 0;
    }

    /// A consumer finished (fully drained or torn down). On the last one,
    /// hand the chunks to a background free so the teardown overlaps the
    /// next stage; the thread is joined in `deinit`. The budget is handed
    /// back HERE (driving thread, before the async free) so accounting
    /// stays deterministic for the caller.
    fn releaseUse(self: *Stage) void {
        self.uses_done += 1;
        if (self.uses_done < self.uses_total) return;
        const res = self.result orelse return;
        self.result = null;
        self.releaseReserved();
        if (std.Thread.spawn(.{}, freeResultThread, .{res})) |th| {
            self.free_thread = th;
        } else |_| {
            freeResultThread(res);
        }
    }

    fn deinit(self: *Stage) void {
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
        };
        try self.stages.append(self.allocator, stage);
        return stage;
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

    pub fn create(allocator: Allocator, stage: *Stage) !exec.Query {
        const views = try allocator.alloc(ColumnView, stage.schema.len);
        errdefer allocator.free(views);
        const self = try allocator.create(MatScan);
        self.* = .{ .allocator = allocator, .stage = stage, .views = views };
        stage.uses_total += 1;
        return exec.makeQuery(allocator, self);
    }

    pub fn next(self: *MatScan) !?exec.Batch {
        try self.stage.ensureRun();
        const res = self.stage.result orelse {
            self.releaseOnce();
            return null;
        };
        while (self.cursor < res.chunks.items.len) {
            const chunk = res.chunks.items[self.cursor];
            self.cursor += 1;
            if (chunk.rows == 0) continue;
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
