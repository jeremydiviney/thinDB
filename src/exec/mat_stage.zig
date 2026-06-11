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
    result: ?*MaterializedResult = null,
    /// Consumers bound at compile time / consumers finished. When the last
    /// finishes, the result frees in the background.
    uses_total: usize = 0,
    uses_done: usize = 0,
    free_thread: ?std.Thread = null,

    pub fn ensureRun(self: *Stage) anyerror!void {
        if (self.result != null) return;
        if (!self.query_alive) return error.UnsupportedQueryShape; // re-run after teardown: can't happen via MatScan
        const res = try self.allocator.create(MaterializedResult);
        errdefer self.allocator.destroy(res);
        res.* = .{ .allocator = self.allocator, .schema = self.schema };
        errdefer res.deinitChunks();
        while (try self.query.next()) |batch| {
            try res.appendBatch(batch);
        }
        // Release the pipeline's operator buffers right away — the stage's
        // working memory drops to just the chunked result.
        self.query.deinit();
        self.query_alive = false;
        self.result = res;
    }

    /// A consumer finished (fully drained or torn down). On the last one,
    /// hand the chunks to a background free so the teardown overlaps the
    /// next stage; the thread is joined in `deinit`.
    fn releaseUse(self: *Stage) void {
        self.uses_done += 1;
        if (self.uses_done < self.uses_total) return;
        const res = self.result orelse return;
        self.result = null;
        if (std.Thread.spawn(.{}, freeResultThread, .{res})) |th| {
            self.free_thread = th;
        } else |_| {
            freeResultThread(res);
        }
    }

    fn deinit(self: *Stage) void {
        if (self.free_thread) |th| th.join();
        if (self.result) |res| freeResultThread(res);
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
    /// error). The schema is deep-copied into the set's arena.
    pub fn addStage(self: *StageSet, query: exec.Query) !*Stage {
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
        const stage = try self.allocator.create(Stage);
        errdefer self.allocator.destroy(stage);
        stage.* = .{ .allocator = self.allocator, .query = q, .schema = schema };
        try self.stages.append(self.allocator, stage);
        return stage;
    }

    pub fn deinit(self: *StageSet) void {
        for (self.stages.items) |stage| stage.deinit();
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
        const rows: u64 = if (self.stage.result) |res| res.total_rows else std.math.maxInt(u64);
        return .{ .upper_rows = rows };
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
