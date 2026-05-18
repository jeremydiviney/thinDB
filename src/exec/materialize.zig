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
const Error = exec.Error;
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

    pub fn init(
        allocator: Allocator,
        upstream: Query,
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

        buf.* = .{
            .allocator = allocator,
            .schema = schema_dup,
            .columns = columns,
            .upstream = upstream,
        };
        return buf;
    }

    pub fn deinit(self: *MaterializedBuffer) void {
        if (self.upstream) |*u| u.deinit();
        for (self.columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.columns);
        self.allocator.free(self.schema);
        self.allocator.destroy(self);
    }

    /// Drain `upstream` once, appending each batch into the per-column
    /// stores. Idempotent — subsequent calls are no-ops. The upstream
    /// Query is released eagerly after a successful drain to reclaim
    /// its operator-tree memory.
    pub fn fill(self: *MaterializedBuffer) !void {
        if (self.filled) return;
        const upstream = &self.upstream.?;
        while (try upstream.next()) |batch| {
            for (batch.values, 0..) |view, i| {
                try transform.appendAllColumn(self.allocator, view, &self.columns[i]);
            }
            self.row_count += @intCast(batch.row_count);
        }
        // Release the upstream — drain is done, we hold the data now.
        var u = self.upstream.?;
        u.deinit();
        self.upstream = null;
        self.filled = true;
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

    pub fn create(allocator: Allocator, buffer: *MaterializedBuffer) !Query {
        const views = try allocator.alloc(ColumnView, buffer.schema.len);
        errdefer allocator.free(views);
        const self = try allocator.create(Reader);
        self.* = .{ .allocator = allocator, .buffer = buffer, .views = views };
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
        if (self.emitted) return null;
        self.emitted = true;
        if (self.buffer.row_count == 0) return null;
        for (self.buffer.columns, self.views) |*c, *v| v.* = c.view();
        return Batch{
            .schema = self.buffer.schema,
            .values = self.views,
            .row_count = self.buffer.row_count,
        };
    }

    pub fn stats(self: *Reader) exec.PipelineStats {
        return .{ .upper_rows = self.buffer.row_count };
    }

    pub fn accountant(self: *Reader) ?*exec.memory.MemoryAccountant {
        _ = self;
        return null;
    }
};
