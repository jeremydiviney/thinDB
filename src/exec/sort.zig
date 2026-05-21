//! Sort (ORDER BY) operator. Blocking — drains all upstream batches into
//! per-column buffers, builds a permutation via pdqsort, then emits sorted
//! batches in chunks of `batch_size`.

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

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

pub const SortSpec = struct {
    col: []const u8,
    desc: bool = false,
};

pub const Sort = struct {
    allocator: Allocator,
    upstream: Query,
    schema: []const Column,

    sort_col_indices: []usize,
    sort_desc: []bool,
    /// Sort-key column names, allocated once at create-time so `stats()`
    /// can publish the output sort claim without re-allocating. Always
    /// populated (ascending or descending) — the per-key direction is
    /// `sort_desc`, which doubles as `SortState.descs`.
    sort_state_keys: []const []const u8,

    /// All upstream rows, accumulated by column. Built lazily on first
    /// `next()` call (Sort is a blocking operator).
    accumulated: []ColumnStore,
    accumulated_rows: u64 = 0,
    /// Bytes charged against the query budget for `accumulated` + `perm`.
    /// Released (and the buffers freed) once the last row is emitted —
    /// the sorted input is no longer a downstream dependency.
    reserved_bytes: usize = 0,
    evicted: bool = false,

    drained: bool = false,
    perm: []u32 = &.{},
    emit_offset: usize = 0,

    output_columns: []ColumnStore,
    views: []ColumnView,

    const batch_size: usize = 1024;

    pub fn create(allocator: Allocator, upstream: Query, sort_specs: []const SortSpec) !Query {
        if (sort_specs.len == 0) return Error.SortNoKeys;
        const schema = upstream.outputSchema();

        const sort_col_indices = try allocator.alloc(usize, sort_specs.len);
        errdefer allocator.free(sort_col_indices);
        const sort_desc = try allocator.alloc(bool, sort_specs.len);
        errdefer allocator.free(sort_desc);

        for (sort_specs, 0..) |spec, i| {
            sort_col_indices[i] = types.findColumn(schema, spec.col) orelse return Error.ColumnNotFound;
            sort_desc[i] = spec.desc;
        }
        // Publish the sort claim for every key, ascending or descending —
        // direction lives in `sort_desc` (= SortState.descs). Grouping is
        // direction-agnostic; an SMJ ascending-merge guards on direction
        // in joinKeysCovered.
        const sort_state_keys = try allocator.alloc([]const u8, sort_specs.len);
        for (sort_col_indices, 0..) |idx, i| sort_state_keys[i] = schema[idx].name;
        errdefer allocator.free(sort_state_keys);

        const accumulated = try allocator.alloc(ColumnStore, schema.len);
        errdefer allocator.free(accumulated);
        var inited: usize = 0;
        errdefer for (accumulated[0..inited]) |*c| c.deinit(allocator);
        for (schema, 0..) |col, i| {
            accumulated[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }

        const output_columns = try allocator.alloc(ColumnStore, schema.len);
        errdefer allocator.free(output_columns);
        var oinited: usize = 0;
        errdefer for (output_columns[0..oinited]) |*c| c.deinit(allocator);
        for (schema, 0..) |col, i| {
            output_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            oinited += 1;
        }

        const views = try allocator.alloc(ColumnView, schema.len);
        errdefer allocator.free(views);

        const self = try allocator.create(Sort);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .schema = schema,
            .sort_col_indices = sort_col_indices,
            .sort_desc = sort_desc,
            .sort_state_keys = sort_state_keys,
            .accumulated = accumulated,
            .output_columns = output_columns,
            .views = views,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Sort) void {
        var up = self.upstream;
        up.deinit();
        if (!self.evicted) {
            for (self.accumulated) |*c| c.deinit(self.allocator);
            self.allocator.free(self.accumulated);
            if (self.perm.len > 0) self.allocator.free(self.perm);
        }
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.sort_col_indices);
        self.allocator.free(self.sort_desc);
        self.allocator.free(self.sort_state_keys);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Sort) []const Column {
        return self.schema;
    }

    pub fn addPrune(self: *Sort, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    /// Sort makes the output globally sorted on its sort keys, in the
    /// requested per-key direction (`sort_desc`). Downstream grouping uses
    /// this regardless of direction; an ascending-merge SMJ guards on
    /// direction itself.
    pub fn stats(self: *Sort) exec.PipelineStats {
        const up = self.upstream.stats();
        return .{
            .upper_rows = up.upper_rows,
            .sort_state = .{
                .keys = self.sort_state_keys,
                .descs = self.sort_desc,
                .global = true,
            },
            // Sorting reorders rows but doesn't change any column's values,
            // so per-column distinct bounds carry through unchanged.
            .column_cards = up.column_cards,
        };
    }

    pub fn accountant(self: *Sort) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *Sort, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "Sort");
        try self.upstream.explain(out, allocator, depth + 1);
    }

    /// Free the accumulated buffer + permutation and hand their bytes back
    /// to the query budget. Called once all sorted rows have been emitted
    /// (or on an empty input). Idempotent.
    fn evict(self: *Sort) void {
        if (self.evicted) return;
        for (self.accumulated) |*c| c.deinit(self.allocator);
        self.allocator.free(self.accumulated);
        self.accumulated = &.{};
        if (self.perm.len > 0) self.allocator.free(self.perm);
        self.perm = &.{};
        if (self.upstream.accountant()) |a| a.release(self.reserved_bytes);
        self.reserved_bytes = 0;
        self.evicted = true;
    }

    pub fn next(self: *Sort) !?Batch {
        if (!self.drained) try self.drainAndSort();

        const remaining = self.accumulated_rows - self.emit_offset;
        if (remaining == 0) {
            self.evict();
            return null;
        }
        const n: usize = @intCast(@min(@as(u64, batch_size), remaining));

        for (self.output_columns) |*c| c.clear();
        for (self.output_columns, 0..) |*out, ci| {
            try engine.memtable.appendByIndices(
                self.allocator,
                self.accumulated[ci].view(),
                self.perm[self.emit_offset .. self.emit_offset + n],
                out,
            );
        }

        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        self.emit_offset += n;
        return Batch{ .schema = self.schema, .values = self.views, .row_count = n };
    }

    fn drainAndSort(self: *Sort) !void {
        const acc = self.upstream.accountant();
        const row_bytes = exec.memory.estimateRowBytes(self.upstream.outputSchema());

        while (try self.upstream.next()) |batch| {
            const b = batch.row_count * row_bytes;
            if (acc) |a| try a.reserve(b);
            self.reserved_bytes += b;
            for (batch.values, 0..) |view, ci| {
                try engine.memtable.appendAllColumn(self.allocator, view, &self.accumulated[ci]);
            }
            self.accumulated_rows += batch.row_count;
        }

        const n: usize = @intCast(self.accumulated_rows);
        if (n == 0) {
            self.drained = true;
            return;
        }
        // Account for the perm array (u32 per row).
        if (acc) |a| try a.reserve(n * @sizeOf(u32));
        self.reserved_bytes += n * @sizeOf(u32);
        self.perm = try self.allocator.alloc(u32, n);
        for (self.perm, 0..) |*p, i| p.* = @intCast(i);

        const Ctx = struct {
            accumulated: []const ColumnStore,
            indices: []const usize,
            desc: []const bool,

            pub fn lessThan(ctx: @This(), a: u32, b: u32) bool {
                for (ctx.indices, 0..) |ci, i| {
                    const ord = engine.memtable.compareInColumn(ctx.accumulated[ci], a, b);
                    if (ord == .lt) return !ctx.desc[i];
                    if (ord == .gt) return ctx.desc[i];
                }
                return false;
            }
        };

        std.sort.pdq(u32, self.perm, Ctx{
            .accumulated = self.accumulated,
            .indices = self.sort_col_indices,
            .desc = self.sort_desc,
        }, Ctx.lessThan);

        self.drained = true;
    }
};
