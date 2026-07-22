//! Materialized buffer + Reader operator.
//!
//! Used to back the SQL `WITH name AS MATERIALIZED (...)` hint and the
//! auto-detected case (refcount ≥ 2). The buffer holds the upstream's
//! drained result as `[]engine.ColumnStore` — same shape as a memtable,
//! transient to the query's lifetime. One drain, many readers, each
//! emitting independent batches from the buffer with its own cursor.
//!
//! Wired in by the compile context (`CompileCtx` in `net/local.zig`):
//! the first time a given `*ir.Op.materialize` is compiled, a
//! MaterializedBuffer is allocated, the upstream Query is built into
//! it, and a Reader is wrapped around it. Subsequent compile calls
//! on the same `*ir.Op` skip the build and just hand back another
//! Reader pointing at the cached buffer.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const makeQuery = exec.makeQuery;

const transform = @import("../engine/transform.zig");

/// Drained upstream rows held in column-store form for the query's
/// lifetime. Multiple Reader instances borrow the same buffer.
/// Owned + freed by the CompileCtx that allocated it.
pub const MaterializedBuffer = struct {
    allocator: Allocator,
    schema: []Column,
    columns: []ColumnStore,
    row_count: u32 = 0,
    /// Drained on first read via `fill()`. Idempotent.
    filled: bool = false,
    /// Source query — non-null until drained, then released early to
    /// free its operator tree's memory.
    upstream: ?Query,
    /// Query-scoped accountant (owned by the CompileCtx). The buffered
    /// rows are charged against it in `fill()` and released when the last
    /// Reader is exhausted (DAG-aware eviction). Null = no tracking.
    acct: ?*exec.memory.MemoryAccountant,
    /// Bytes charged for the buffered rows; released exactly once on
    /// eviction.
    reserved_bytes: usize = 0,
    /// Number of Readers not yet exhausted. Incremented in `Reader.create`,
    /// decremented when a Reader finishes draining. At zero the column data
    /// is freed and the budget released — the buffer is no longer a
    /// downstream dependency of anything in the DAG.
    live_readers: u32 = 0,
    /// True once the column data has been freed + budget released.
    evicted: bool = false,
    /// Per-column NDV/min-max snapshot taken from the upstream at build time
    /// (while it's still alive) so cardinality stats survive the materialize
    /// boundary instead of resetting to "unknown". Owned copy — the upstream's
    /// slice dies with it after `fill()`. Capped at the realized row count once
    /// drained. We do NOT rescan the buffer to recompute these.
    column_stats: []exec.ColStat = &.{},
    /// Upstream's row upper-bound, captured at build time. Reported until the
    /// buffer is filled, after which the exact `row_count` is known.
    upper_rows_hint: u64 = 0,

    pub fn init(
        allocator: Allocator,
        upstream: Query,
        acct: ?*exec.memory.MemoryAccountant,
    ) !*MaterializedBuffer {
        const up_schema = upstream.outputSchema();
        const buf = try allocator.create(MaterializedBuffer);
        errdefer allocator.destroy(buf);

        // Own a copy of the schema — upstream's lifetime ends after
        // fill(), so we can't borrow.
        const schema_dup = try allocator.alloc(Column, up_schema.len);
        errdefer allocator.free(schema_dup);
        for (up_schema, schema_dup) |c, *out| out.* = c;

        const columns = try allocator.alloc(ColumnStore, up_schema.len);
        errdefer allocator.free(columns);
        var inited: usize = 0;
        errdefer for (columns[0..inited]) |*c| c.deinit(allocator);
        for (up_schema, columns) |col, *store| {
            store.* = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }

        // Snapshot the upstream's propagated stats now — it's released after
        // fill(), so this is the only point its column_stats are reachable.
        const up_stats = upstream.stats();
        const cs = try allocator.alloc(exec.ColStat, up_stats.column_stats.len);
        errdefer allocator.free(cs);
        @memcpy(cs, up_stats.column_stats);

        buf.* = .{
            .allocator = allocator,
            .schema = schema_dup,
            .columns = columns,
            .upstream = upstream,
            .acct = acct,
            .column_stats = cs,
            .upper_rows_hint = up_stats.upper_rows,
        };
        return buf;
    }

    pub fn deinit(self: *MaterializedBuffer) void {
        if (self.upstream) |*u| u.deinit();
        // If the buffer was never fully drained by its Readers (e.g. a
        // LIMIT above stopped pulling), the column data is still resident
        // here. Free it. We do NOT release the budget on this teardown
        // path — the query is ending and the accountant is about to be
        // destroyed by the CompileCtx.
        if (!self.evicted) {
            for (self.columns) |*c| c.deinit(self.allocator);
            self.allocator.free(self.columns);
        }
        self.allocator.free(self.schema);
        self.allocator.free(self.column_stats);
        self.allocator.destroy(self);
    }

    /// Free the buffered column data and hand its bytes back to the query
    /// budget. Called when the last Reader is exhausted; idempotent.
    fn evict(self: *MaterializedBuffer) void {
        if (self.evicted) return;
        for (self.columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.columns);
        self.columns = &.{};
        if (self.acct) |a| a.release(.materialize, self.reserved_bytes);
        self.reserved_bytes = 0;
        self.evicted = true;
    }

    /// A Reader has finished draining. When the last one finishes the
    /// buffer is no longer a downstream dependency, so evict it.
    fn readerFinished(self: *MaterializedBuffer) void {
        std.debug.assert(self.live_readers > 0);
        self.live_readers -= 1;
        if (self.live_readers == 0) self.evict();
    }

    /// Drain `upstream` once, appending each batch into the per-column
    /// stores. Idempotent — subsequent calls are no-ops. The upstream
    /// Query is released eagerly after a successful drain to reclaim
    /// its operator-tree memory.
    pub fn fill(self: *MaterializedBuffer) !void {
        if (self.filled) return;
        const upstream = &self.upstream.?;
        // Charge the buffered rows against the query budget as we drain.
        // This is a blocking, fully-materializing operator just like Sort
        // / hash GroupBy, so an oversized CTE fails fast with
        // MemoryBudgetExceeded instead of silently accumulating GBs. The
        // accountant is the query-scoped one (owned by the CompileCtx, not
        // the upstream Scan), so it stays valid after we release the
        // upstream below and lets us release these bytes again when the
        // last Reader is done.
        const row_bytes = exec.memory.estimateRowBytes(self.schema);
        errdefer if (self.acct) |a| {
            a.release(.materialize, self.reserved_bytes);
            self.reserved_bytes = 0;
        };
        while (try upstream.next()) |batch| {
            const b = batch.row_count * row_bytes;
            if (self.acct) |a| try a.reserve(.materialize, b);
            self.reserved_bytes += b;
            for (batch.values, 0..) |view, i| {
                try transform.appendAllColumn(self.allocator, view, &self.columns[i]);
            }
            self.row_count += @intCast(batch.row_count);
        }
        // Release the upstream — drain is done, we hold the data now. The
        // upstream Scan does not own the query accountant, so this leaves
        // our reservation intact.
        var u = self.upstream.?;
        u.deinit();
        self.upstream = null;
        self.filled = true;
        // Tighten the snapshot NDV bounds to the realized row count (a column
        // can't hold more distinct values than there are rows). min/max are
        // untouched — no rescan.
        exec.capColStats(self.column_stats, self.row_count);
    }
};

/// Reader cursor over a shared `MaterializedBuffer`. Doesn't own the
/// buffer — multiple Readers can coexist, each consuming the buffer
/// independently. v1: emits the full buffer in a single Batch then
/// returns null. Future work could slice into ~1024-row batches if
/// downstream operators benefit from smaller chunks.
pub const Reader = struct {
    allocator: Allocator,
    buffer: *MaterializedBuffer,
    views: []ColumnView,
    emitted: bool = false,
    /// True once this Reader has reported itself finished to the buffer
    /// (drained past its single data batch). Guards a double decrement.
    done: bool = false,

    pub fn create(allocator: Allocator, buffer: *MaterializedBuffer) !Query {
        const views = try allocator.alloc(ColumnView, buffer.schema.len);
        errdefer allocator.free(views);
        const self = try allocator.create(Reader);
        self.* = .{ .allocator = allocator, .buffer = buffer, .views = views };
        buffer.live_readers += 1;
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Reader) void {
        // Don't deinit the buffer — it's owned by the CompileCtx.
        self.allocator.free(self.views);
        const a = self.allocator;
        a.destroy(self);
    }

    pub fn outputSchema(self: *Reader) []const Column {
        return self.buffer.schema;
    }

    pub fn addPrune(self: *Reader, pred: exec.Predicate) !void {
        // The buffer is already materialized; pruning would need to
        // happen against the in-memory store. Not done in v1 — drop
        // the prune silently (correctness preserved, perf left on the
        // table).
        _ = self;
        _ = pred;
    }

    pub fn next(self: *Reader) !?Batch {
        try self.buffer.fill();
        if (self.done) return null;
        if (self.emitted) {
            // Our single data batch has been consumed; this Reader is
            // finished. Report it so the buffer can evict once the last
            // Reader is done.
            self.done = true;
            self.buffer.readerFinished();
            return null;
        }
        self.emitted = true;
        if (self.buffer.row_count == 0) {
            self.done = true;
            self.buffer.readerFinished();
            return null;
        }
        for (self.buffer.columns, self.views) |*c, *v| v.* = c.view();
        return Batch{
            .schema = self.buffer.schema,
            .values = self.views,
            .row_count = self.buffer.row_count,
        };
    }

    pub fn stats(self: *Reader) exec.PipelineStats {
        // Before fill() the realized row_count is 0 (meaningless as a bound),
        // so report the upstream's snapshot until the buffer is drained.
        const ur = if (self.buffer.filled) self.buffer.row_count else self.buffer.upper_rows_hint;
        return .{ .upper_rows = ur, .column_stats = self.buffer.column_stats };
    }

    pub fn accountant(self: *Reader) ?*exec.memory.MemoryAccountant {
        return self.buffer.acct;
    }

    pub fn explain(self: *Reader, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "MaterializedScan");
        // Before the buffer is drained the source pipeline is still
        // attached — show it as the materialized subtree.
        if (self.buffer.upstream) |*u| try u.explain(out, allocator, depth + 1);
    }
};

test "materialized reader forwards column stats across the boundary" {
    const a = std.testing.allocator;
    const schema = try a.alloc(Column, 1);
    schema[0] = .{ .name = "x", .type = .int, .nullable = false };
    const columns = try a.alloc(ColumnStore, 1);
    columns[0] = try ColumnStore.init(a, .int, false);
    const cs = try a.alloc(exec.ColStat, 1);
    cs[0] = .{ .ndv = .{ .exact = 3 }, .min = 1, .max = 9 };

    const buf = try a.create(MaterializedBuffer);
    buf.* = .{
        .allocator = a,
        .schema = schema,
        .columns = columns,
        .upstream = null,
        .acct = null,
        .column_stats = cs,
        .upper_rows_hint = 10,
    };
    defer buf.deinit();

    var q = try Reader.create(a, buf);
    defer q.deinit();

    // Pre-fill: report the upstream's row hint, but the column stats already flow.
    {
        const st = q.stats();
        try std.testing.expectEqual(@as(u64, 10), st.upper_rows);
        try std.testing.expectEqual(@as(usize, 1), st.column_stats.len);
        try std.testing.expectEqual(@as(u32, 3), st.column_stats[0].ndv.exact);
        try std.testing.expectEqual(@as(?i128, 1), st.column_stats[0].min);
        try std.testing.expectEqual(@as(?i128, 9), st.column_stats[0].max);
    }
    // Post-fill: exact realized row count, stats preserved.
    buf.filled = true;
    buf.row_count = 5;
    {
        const st = q.stats();
        try std.testing.expectEqual(@as(u64, 5), st.upper_rows);
        try std.testing.expectEqual(@as(u32, 3), st.column_stats[0].ndv.exact);
    }
}
