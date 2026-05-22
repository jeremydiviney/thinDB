//! Aggregate / GROUP BY operator. Drains the upstream into a per-aggregate
//! accumulator (single global slot, or hash-keyed per group), then emits
//! one batch with the final results.
//!
//! Supported aggregates: COUNT, SUM, MIN, MAX, AVG. SUM/MIN/MAX/AVG dispatch
//! over input column type to choose the right accumulator state.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;

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

pub const AggFunc = enum {
    count,
    sum,
    min,
    max,
    avg,
    /// Population stddev. sqrt(sum((x-mean)^2) / n). 0 when n=0.
    stddev_pop,
    /// Sample stddev. sqrt(sum((x-mean)^2) / (n-1)). 0 when n<2.
    stddev_samp,
    /// Population variance. sum((x-mean)^2) / n.
    var_pop,
    /// Sample variance. sum((x-mean)^2) / (n-1).
    var_samp,
    /// Exact distinct count. Hash set of dup'd value bytes; output bigint.
    count_distinct,
    /// Exact continuous percentile. params.percentile in [0, 1].
    /// O(N) memory; sorts at finalize. Output double.
    percentile,
    /// Concatenate string values with a separator. params.separator
    /// is prepended before every value after the first. Output string.
    group_concat,
};

/// Per-aggregate parameters not expressible via `col` / `as`. `.none`
/// covers all existing aggregates; percentile and group_concat carry
/// their own payload.
pub const AggParams = union(enum) {
    none,
    percentile: f64,
    separator: []const u8,
};

pub const AggSpec = struct {
    func: AggFunc,
    /// Column to aggregate. `null` is only valid for `COUNT(*)`.
    col: ?[]const u8 = null,
    /// Output column name.
    as: []const u8,
    /// Per-function payload. Defaults to `.none` so existing call
    /// sites compile unchanged.
    params: AggParams = .none,
};

/// Per-aggregate accumulator state. Integer types accumulate into i64
/// (MIN/MAX) or i128 (SUM); float/double types accumulate into f64; LARGEINT
/// gets dedicated i128 min/max variants. The final value is cast back to the
/// declared output column type.
const AccState = union(enum) {
    count: u64,
    sum_int: i128,
    sum_float: f64,
    min_int: ?i64,
    max_int: ?i64,
    min_float: ?f64,
    max_float: ?f64,
    /// Separate i128 min/max variants for LARGEINT inputs (don't fit in i64).
    min_large: ?i128,
    max_large: ?i128,
    /// MIN/MAX over string-family columns. Holds the running extreme as
    /// arena-dup'd bytes (the view's bytes are transient per batch).
    min_str: ?[]const u8,
    max_str: ?[]const u8,
    avg: AvgAcc,
    /// Welford's online algorithm: numerically stable variance/stddev.
    /// Covers stddev_pop, stddev_samp, var_pop, var_samp.
    welford: WelfordAcc,
    /// Exact distinct count: set of arena-dup'd value bytes.
    distinct: std.StringHashMapUnmanaged(void),
    /// Exact percentile: keep every observed value (as f64), sort + interpolate at finalize.
    percentile_values: std.ArrayListUnmanaged(f64),
    /// group_concat buffer + a flag so an empty first value is still
    /// distinguishable from "nothing appended yet" (the latter must
    /// NOT prepend a separator on the next append).
    concat: ConcatAcc,
};

const AvgAcc = struct {
    sum: f64,
    count: u64,
};

const WelfordAcc = struct {
    mean: f64 = 0.0,
    m2: f64 = 0.0,
    count: u64 = 0,
};

const ConcatAcc = struct {
    buf: std.ArrayListUnmanaged(u8) = .empty,
    nonempty: bool = false,
};

pub const Aggregate = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    upstream: Query,

    /// Index in the *upstream* schema for each group-by column.
    group_col_indices: []usize,
    /// For each agg, index in upstream schema (or null for COUNT(*)).
    agg_col_indices: []?usize,
    /// Each agg's spec (borrowed from caller).
    aggs: []const AggSpec,

    output_schema: []Column,
    output_columns: []ColumnStore,
    views: []ColumnView,

    /// Used only when there are no group-by columns (single global group).
    single_state: []AccState,
    /// Used only when grouping. Maps compound-key bytes → owned state array.
    groups: std.StringHashMapUnmanaged([]AccState),
    /// Reusable buffer for building per-row group keys during accumulate.
    /// Allocated once, grown to max-key-size, cleared+reused per row.
    /// Saves ~1 arena alloc per row in the inner loop.
    key_scratch: std.ArrayList(u8),

    emitted: bool = false,
    /// Bytes charged against the query budget for the group hash table /
    /// accumulator state (held in `arena`). Released when the single
    /// result batch has been built — the input is no longer needed.
    reserved_bytes: usize = 0,
    evicted: bool = false,

    pub fn create(
        allocator: Allocator,
        upstream: Query,
        group_cols: []const []const u8,
        aggs: []const AggSpec,
    ) !Query {
        if (aggs.len == 0) return Error.AggregateNoSpecs;
        const up_schema = upstream.outputSchema();

        // Resolve group-by column indices.
        const group_col_indices = try allocator.alloc(usize, group_cols.len);
        errdefer allocator.free(group_col_indices);
        for (group_cols, 0..) |name, i| {
            group_col_indices[i] = types.findColumn(up_schema, name) orelse return Error.ColumnNotFound;
        }

        // Resolve agg column indices and build output schema.
        const agg_col_indices = try allocator.alloc(?usize, aggs.len);
        errdefer allocator.free(agg_col_indices);

        const output_schema = try allocator.alloc(Column, group_cols.len + aggs.len);
        errdefer allocator.free(output_schema);

        for (group_col_indices, 0..) |src_idx, i| {
            output_schema[i] = up_schema[src_idx];
        }

        for (aggs, 0..) |a, i| {
            agg_col_indices[i] = if (a.col) |name|
                (types.findColumn(up_schema, name) orelse return Error.ColumnNotFound)
            else
                null;

            output_schema[group_cols.len + i] = .{
                .name = a.as,
                .type = try aggOutputType(a.func, if (agg_col_indices[i]) |idx| up_schema[idx].type else null),
            };
        }

        for (aggs, agg_col_indices) |a, maybe_idx| {
            const t = if (maybe_idx) |idx| up_schema[idx].type else null;
            try validateAggFn(a.func, t, a.params);
        }

        const output_columns = try allocator.alloc(ColumnStore, output_schema.len);
        errdefer allocator.free(output_columns);
        var inited: usize = 0;
        errdefer for (output_columns[0..inited]) |*c| c.deinit(allocator);
        for (output_schema, 0..) |col, i| {
            output_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }

        const views = try allocator.alloc(ColumnView, output_schema.len);
        errdefer allocator.free(views);

        const single_state = try allocator.alloc(AccState, aggs.len);
        errdefer allocator.free(single_state);
        for (aggs, agg_col_indices, single_state) |a, idx, *s| {
            const in_t: ?Type = if (idx) |i| up_schema[i].type else null;
            s.* = initialState(a.func, in_t);
        }

        const self = try allocator.create(Aggregate);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .upstream = upstream,
            .group_col_indices = group_col_indices,
            .agg_col_indices = agg_col_indices,
            .aggs = aggs,
            .output_schema = output_schema,
            .output_columns = output_columns,
            .views = views,
            .single_state = single_state,
            .groups = .empty,
            .key_scratch = .empty,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Aggregate) void {
        var up = self.upstream;
        up.deinit();
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.group_col_indices);
        self.allocator.free(self.agg_col_indices);
        self.allocator.free(self.single_state);
        self.key_scratch.deinit(self.allocator);
        self.arena.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Aggregate) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(self: *Aggregate, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    /// Global aggregate (no group_cols): always emits exactly 1 row.
    /// Grouped aggregate: emits at most NDV(group_cols) rows, which is
    /// bounded above by the upstream row count. We don't have NDV
    /// without HLL, so for grouped agg the upper bound = upstream's.
    /// Sort state: hash-based aggregate destroys any prior sort.
    pub fn stats(self: *Aggregate) exec.PipelineStats {
        if (self.group_col_indices.len == 0) {
            return .{ .upper_rows = 1 };
        }
        const up = self.upstream.stats();
        return .{ .upper_rows = up.upper_rows };
    }

    pub fn accountant(self: *Aggregate) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *Aggregate, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, if (self.group_col_indices.len == 0) "Aggregate (global)" else "HashAggregate");
        try self.upstream.explain(out, allocator, depth + 1);
    }

    pub fn next(self: *Aggregate) !?Batch {
        if (self.emitted) return null;
        self.emitted = true;

        while (try self.upstream.next()) |batch| {
            try self.accumulateBatch(batch);
        }

        if (self.group_col_indices.len == 0) {
            try self.appendSingleResult();
        } else {
            try self.appendGroupedResults();
        }

        // Results are now materialized into `output_columns` (allocator-
        // owned, independent of the arena), so the group hash table /
        // accumulator state is no longer a downstream dependency. Free it
        // and hand its budget back before emitting.
        self.evict();

        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        return Batch{
            .schema = self.output_schema,
            .values = self.views,
            .row_count = self.output_columns[0].rowCount(),
        };
    }

    /// Drop the group accumulator arena and release its reserved budget.
    /// Idempotent. The arena is left in a valid (empty) state so the
    /// later `deinit` call remains safe.
    fn evict(self: *Aggregate) void {
        if (self.evicted) return;
        _ = self.arena.reset(.free_all);
        if (self.upstream.accountant()) |a| a.release(.hash_aggregate, self.reserved_bytes);
        self.reserved_bytes = 0;
        self.evicted = true;
    }

    fn accumulateBatch(self: *Aggregate, batch: Batch) !void {
        const n = batch.row_count;
        const aa_state = self.arena.allocator();
        if (self.group_col_indices.len == 0) {
            for (self.aggs, 0..) |a, ai| {
                try updateState(aa_state, &self.single_state[ai], a, batch, self.agg_col_indices[ai], 0, @intCast(n));
            }
            return;
        }

        const aa = self.arena.allocator();
        var row: u32 = 0;
        while (row < n) : (row += 1) {
            // Build the key into a reusable scratch buffer instead of
            // allocating fresh storage every row. Most rows hit existing
            // groups — the scratch bytes only need to outlive the lookup
            // itself, so we can wipe and reuse them next iteration. New
            // groups get an arena-owned copy.
            self.key_scratch.clearRetainingCapacity();
            try buildCompoundGroupKey(self.allocator, &self.key_scratch, batch, self.group_col_indices, row);

            const gop = try self.groups.getOrPut(aa, self.key_scratch.items);
            if (!gop.found_existing) {
                // New group — reserve its memory against the query
                // budget. Approximate: key bytes + per-agg state +
                // ~32 bytes hashmap overhead.
                const approx = self.key_scratch.items.len + self.aggs.len * @sizeOf(AccState) + 32;
                if (self.upstream.accountant()) |acct| {
                    try acct.reserve(.hash_aggregate, approx);
                }
                self.reserved_bytes += approx;
                // The hashmap kept a reference to our scratch slice — but
                // we're about to reuse that buffer. Replace key_ptr with
                // an arena-owned dup so it survives.
                gop.key_ptr.* = try aa.dupe(u8, self.key_scratch.items);

                const state = try aa.alloc(AccState, self.aggs.len);
                const up_schema = self.upstream.outputSchema();
                for (self.aggs, self.agg_col_indices, state) |a, maybe_idx, *s| {
                    const in_t: ?Type = if (maybe_idx) |i| up_schema[i].type else null;
                    s.* = initialState(a.func, in_t);
                }
                gop.value_ptr.* = state;
            }
            const state = gop.value_ptr.*;
            for (self.aggs, 0..) |a, ai| {
                try updateState(aa_state, &state[ai], a, batch, self.agg_col_indices[ai], row, row + 1);
            }
        }
    }

    fn appendSingleResult(self: *Aggregate) !void {
        for (self.aggs, 0..) |a, ai| {
            try appendAccToColumn(self.allocator, a, self.single_state[ai], &self.output_columns[ai], self.output_schema[ai].type);
        }
    }

    fn appendGroupedResults(self: *Aggregate) !void {
        var it = self.groups.iterator();
        while (it.next()) |entry| {
            const key_bytes = entry.key_ptr.*;
            const state = entry.value_ptr.*;

            try appendGroupKey(self.allocator, key_bytes, self.group_col_indices, self.upstream.outputSchema(), self.output_columns[0..self.group_col_indices.len]);

            for (self.aggs, 0..) |a, ai| {
                const out_idx = self.group_col_indices.len + ai;
                try appendAccToColumn(self.allocator, a, state[ai], &self.output_columns[out_idx], self.output_schema[out_idx].type);
            }
        }
    }
};

/// Streaming GROUP BY for input already sorted such that equal group keys
/// are adjacent (direction-agnostic). Holds only the *current* group's
/// accumulator state — O(1) in cardinality, unlike the hash Aggregate
/// which holds every group at once. When the key changes, the open group's
/// result row is appended to the output batch and the per-group transient
/// arena is reset. Emits in `batch_size` chunks so operator memory stays
/// bounded regardless of how many groups there are.
///
/// Requires `group_cols.len > 0`. The router in net/local.zig selects this
/// only when `sort_state` proves the group keys are a sorted prefix.
pub const SortedAggregate = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    upstream: Query,

    group_col_indices: []usize,
    agg_col_indices: []?usize,
    aggs: []const AggSpec,

    output_schema: []Column,
    output_columns: []ColumnStore,
    views: []ColumnView,

    /// The open group's key bytes + accumulators. `cur_state` is allocated
    /// once and re-initialized per group; its transient sub-allocations
    /// (string min/max, distinct sets, ...) live in `arena`, reset between
    /// groups.
    cur_key: std.ArrayList(u8),
    cur_state: []AccState,
    open: bool = false,
    /// Scratch for building a candidate row's key to compare against
    /// `cur_key`.
    key_scratch: std.ArrayList(u8),

    /// Resumable input cursor: we may stop mid-batch when the output batch
    /// fills, and continue from here on the next `next()` call.
    cur_batch: ?Batch = null,
    cur_row: u32 = 0,
    upstream_done: bool = false,

    const batch_size: usize = 1024;

    pub fn create(
        allocator: Allocator,
        upstream: Query,
        group_cols: []const []const u8,
        aggs: []const AggSpec,
    ) !Query {
        if (aggs.len == 0) return Error.AggregateNoSpecs;
        // Streaming only makes sense with grouping keys; the no-group
        // (global) case has no sortedness to exploit and stays on Aggregate.
        if (group_cols.len == 0) return Error.AggregateNoSpecs;
        const up_schema = upstream.outputSchema();

        const group_col_indices = try allocator.alloc(usize, group_cols.len);
        errdefer allocator.free(group_col_indices);
        for (group_cols, 0..) |name, i| {
            group_col_indices[i] = types.findColumn(up_schema, name) orelse return Error.ColumnNotFound;
        }

        const agg_col_indices = try allocator.alloc(?usize, aggs.len);
        errdefer allocator.free(agg_col_indices);

        const output_schema = try allocator.alloc(Column, group_cols.len + aggs.len);
        errdefer allocator.free(output_schema);
        for (group_col_indices, 0..) |src_idx, i| output_schema[i] = up_schema[src_idx];
        for (aggs, 0..) |a, i| {
            agg_col_indices[i] = if (a.col) |name|
                (types.findColumn(up_schema, name) orelse return Error.ColumnNotFound)
            else
                null;
            output_schema[group_cols.len + i] = .{
                .name = a.as,
                .type = try aggOutputType(a.func, if (agg_col_indices[i]) |idx| up_schema[idx].type else null),
            };
        }
        for (aggs, agg_col_indices) |a, maybe_idx| {
            const t = if (maybe_idx) |idx| up_schema[idx].type else null;
            try validateAggFn(a.func, t, a.params);
        }

        const output_columns = try allocator.alloc(ColumnStore, output_schema.len);
        errdefer allocator.free(output_columns);
        var inited: usize = 0;
        errdefer for (output_columns[0..inited]) |*c| c.deinit(allocator);
        for (output_schema, 0..) |col, i| {
            output_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }

        const views = try allocator.alloc(ColumnView, output_schema.len);
        errdefer allocator.free(views);

        const cur_state = try allocator.alloc(AccState, aggs.len);
        errdefer allocator.free(cur_state);

        const self = try allocator.create(SortedAggregate);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .upstream = upstream,
            .group_col_indices = group_col_indices,
            .agg_col_indices = agg_col_indices,
            .aggs = aggs,
            .output_schema = output_schema,
            .output_columns = output_columns,
            .views = views,
            .cur_state = cur_state,
            .cur_key = .empty,
            .key_scratch = .empty,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *SortedAggregate) void {
        var up = self.upstream;
        up.deinit();
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.group_col_indices);
        self.allocator.free(self.agg_col_indices);
        self.allocator.free(self.cur_state);
        self.cur_key.deinit(self.allocator);
        self.key_scratch.deinit(self.allocator);
        self.arena.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *SortedAggregate) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(self: *SortedAggregate, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    pub fn stats(self: *SortedAggregate) exec.PipelineStats {
        const up = self.upstream.stats();
        return .{ .upper_rows = up.upper_rows };
    }

    pub fn accountant(self: *SortedAggregate) ?*exec.memory.MemoryAccountant {
        // Bounded memory by construction — no budget reservation needed.
        return self.upstream.accountant();
    }

    pub fn explain(self: *SortedAggregate, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        // A Sort child below means "sorted then streamed"; its absence means
        // the input was already sorted on the group key (no sort needed).
        try exec.explainLine(out, allocator, depth, "StreamAggregate (sorted input)");
        try self.upstream.explain(out, allocator, depth + 1);
    }

    fn beginGroup(self: *SortedAggregate) !void {
        // key_scratch already holds the new group's key bytes.
        self.cur_key.clearRetainingCapacity();
        try self.cur_key.appendSlice(self.allocator, self.key_scratch.items);
        const up_schema = self.upstream.outputSchema();
        for (self.aggs, self.agg_col_indices, self.cur_state) |a, maybe_idx, *s| {
            const in_t: ?Type = if (maybe_idx) |i| up_schema[i].type else null;
            s.* = initialState(a.func, in_t);
        }
        self.open = true;
    }

    fn finalizeGroup(self: *SortedAggregate) !void {
        try appendGroupKey(
            self.allocator,
            self.cur_key.items,
            self.group_col_indices,
            self.upstream.outputSchema(),
            self.output_columns[0..self.group_col_indices.len],
        );
        for (self.aggs, 0..) |a, ai| {
            const out_idx = self.group_col_indices.len + ai;
            try appendAccToColumn(self.allocator, a, self.cur_state[ai], &self.output_columns[out_idx], self.output_schema[out_idx].type);
        }
        // Group done — drop its transient state, keep the buffer for reuse.
        _ = self.arena.reset(.retain_capacity);
        self.open = false;
    }

    pub fn next(self: *SortedAggregate) !?Batch {
        for (self.output_columns) |*c| c.clear();
        var out_rows: usize = 0;

        outer: while (out_rows < batch_size) {
            if (self.cur_batch == null) {
                if (self.upstream_done) {
                    if (self.open) {
                        try self.finalizeGroup();
                        out_rows += 1;
                    }
                    break;
                }
                self.cur_batch = try self.upstream.next();
                if (self.cur_batch == null) {
                    self.upstream_done = true;
                    continue;
                }
                self.cur_row = 0;
            }
            const batch = self.cur_batch.?;
            while (self.cur_row < batch.row_count) {
                self.key_scratch.clearRetainingCapacity();
                try buildCompoundGroupKey(self.allocator, &self.key_scratch, batch, self.group_col_indices, self.cur_row);

                if (self.open and !std.mem.eql(u8, self.key_scratch.items, self.cur_key.items)) {
                    try self.finalizeGroup();
                    out_rows += 1;
                    if (out_rows == batch_size) {
                        // Output batch is full and the current row hasn't
                        // started its group yet. Emit now; the next call
                        // resumes at this same row (open == false).
                        break :outer;
                    }
                }
                if (!self.open) try self.beginGroup();
                for (self.aggs, 0..) |a, ai| {
                    try updateState(self.arena.allocator(), &self.cur_state[ai], a, batch, self.agg_col_indices[ai], self.cur_row, self.cur_row + 1);
                }
                self.cur_row += 1;
            }
            if (self.cur_row >= batch.row_count) self.cur_batch = null;
        }

        if (out_rows == 0) return null;
        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        return Batch{
            .schema = self.output_schema,
            .values = self.views,
            .row_count = @intCast(out_rows),
        };
    }
};

fn initialState(func: AggFunc, in: ?Type) AccState {
    return switch (func) {
        .count => .{ .count = 0 },
        .sum => if (in != null and in.?.isFloat())
            .{ .sum_float = 0.0 }
        else
            // i128 accumulator covers BIGINT, LARGEINT, DECIMAL64, DECIMAL128.
            .{ .sum_int = 0 },
        .min => if (in != null and in.?.isFloat())
            .{ .min_float = null }
        else if (in != null and in.?.isString())
            .{ .min_str = null }
        else if (in != null and (in.? == .largeint or in.? == .decimal128))
            .{ .min_large = null }
        else
            .{ .min_int = null },
        .max => if (in != null and in.?.isFloat())
            .{ .max_float = null }
        else if (in != null and in.?.isString())
            .{ .max_str = null }
        else if (in != null and (in.? == .largeint or in.? == .decimal128))
            .{ .max_large = null }
        else
            .{ .max_int = null },
        .avg => .{ .avg = .{ .sum = 0.0, .count = 0 } },
        .stddev_pop, .stddev_samp, .var_pop, .var_samp => .{ .welford = .{} },
        .count_distinct => .{ .distinct = .empty },
        .percentile => .{ .percentile_values = .empty },
        .group_concat => .{ .concat = .{} },
    };
}

fn aggOutputType(func: AggFunc, in: ?Type) !Type {
    return switch (func) {
        .count, .count_distinct => .bigint,
        .sum => blk: {
            const t = in orelse return Error.AggregateColumnRequired;
            // DESIGN.md §3.4: SUM(DECIMAL(p, s)) -> DECIMAL(38, s).
            if (t.decimalSpec()) |spec| break :blk .{ .decimal128 = .{ .p = 38, .s = spec.s } };
            if (t.isFloat()) break :blk .double;
            if (t == .largeint) break :blk .largeint;
            break :blk .bigint;
        },
        .min, .max => in orelse return Error.AggregateNoSpecs,
        .avg, .stddev_pop, .stddev_samp, .var_pop, .var_samp, .percentile => .double,
        .group_concat => .string,
    };
}

fn validateAggFn(func: AggFunc, in: ?Type, params: AggParams) !void {
    switch (func) {
        .count => return,
        .sum, .avg, .stddev_pop, .stddev_samp, .var_pop, .var_samp => {
            const t = in orelse return Error.AggregateColumnRequired;
            if (!(t.isInteger() or t.isDecimal() or t == .boolean or t == .float or t == .double)) {
                return Error.AggregateUnsupportedType;
            }
        },
        .min, .max => {
            const t = in orelse return Error.AggregateColumnRequired;
            if (!(t.isInteger() or t.isDecimal() or t == .boolean or t == .float or t == .double or t == .date or t == .datetime or t.isString())) {
                return Error.AggregateUnsupportedType;
            }
        },
        .count_distinct => {
            // Any column type works (we hash the encoded bytes).
            _ = in orelse return Error.AggregateColumnRequired;
        },
        .percentile => {
            const t = in orelse return Error.AggregateColumnRequired;
            if (!(t.isInteger() or t.isDecimal() or t == .boolean or t == .float or t == .double or t == .date or t == .datetime)) {
                return Error.AggregateUnsupportedType;
            }
            switch (params) {
                .percentile => |p| if (p < 0.0 or p > 1.0) return Error.AggregateInvalidParam,
                else => return Error.AggregateInvalidParam,
            }
        },
        .group_concat => {
            const t = in orelse return Error.AggregateColumnRequired;
            if (!t.isString()) return Error.AggregateUnsupportedType;
            switch (params) {
                .separator => {},
                else => return Error.AggregateInvalidParam,
            }
        },
    }
}

fn updateState(
    aa: Allocator,
    s: *AccState,
    spec: AggSpec,
    batch: Batch,
    col_idx: ?usize,
    row_start: u32,
    row_end: u32,
) !void {
    const func = spec.func;
    switch (func) {
        .count => {
            // COUNT(*) counts every row. COUNT(col) skips NULLs.
            if (col_idx) |idx| {
                const view = batch.values[idx];
                if (view.nulls == null) {
                    s.count += @as(u64, row_end - row_start);
                } else {
                    var r: u32 = row_start;
                    while (r < row_end) : (r += 1) {
                        if (view.isValid(r)) s.count += 1;
                    }
                }
            } else {
                s.count += @as(u64, row_end - row_start);
            }
        },
        .sum => {
            const idx = col_idx.?;
            const view = batch.values[idx];
            switch (view.data) {
                .int => |s_int| for (s_int[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .bigint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .boolean => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .tinyint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .smallint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .largeint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .decimal64 => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .decimal128 => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .float => |s_f| for (s_f[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_float += v;
                },
                .double => |s_d| for (s_d[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_float += v;
                },
                else => unreachable,
            }
        },
        .min => {
            const idx = col_idx.?;
            const view = batch.values[idx];
            switch (view.data) {
                .int, .date => |s_int| for (s_int[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.min_int == null or iv < s.min_int.?) s.min_int = iv;
                },
                .bigint, .datetime => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.min_int == null or v < s.min_int.?) s.min_int = v;
                },
                .boolean => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.min_int == null or iv < s.min_int.?) s.min_int = iv;
                },
                .tinyint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.min_int == null or iv < s.min_int.?) s.min_int = iv;
                },
                .smallint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.min_int == null or iv < s.min_int.?) s.min_int = iv;
                },
                .largeint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.min_large == null or v < s.min_large.?) s.min_large = v;
                },
                .decimal64 => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.min_int == null or v < s.min_int.?) s.min_int = v;
                },
                .decimal128 => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.min_large == null or v < s.min_large.?) s.min_large = v;
                },
                .float => |s_f| for (s_f[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const fv: f64 = v;
                    if (s.min_float == null or fv < s.min_float.?) s.min_float = fv;
                },
                .double => |s_d| for (s_d[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.min_float == null or v < s.min_float.?) s.min_float = v;
                },
                .varchar, .string, .char => {
                    var r: u32 = row_start;
                    while (r < row_end) : (r += 1) {
                        if (!view.isValid(r)) continue;
                        const bytes = stringRowBytes(view, r);
                        if (s.min_str == null or std.mem.order(u8, bytes, s.min_str.?) == .lt) {
                            s.min_str = try aa.dupe(u8, bytes);
                        }
                    }
                },
                else => unreachable,
            }
        },
        .max => {
            const idx = col_idx.?;
            const view = batch.values[idx];
            switch (view.data) {
                .int, .date => |s_int| for (s_int[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.max_int == null or iv > s.max_int.?) s.max_int = iv;
                },
                .bigint, .datetime => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.max_int == null or v > s.max_int.?) s.max_int = v;
                },
                .boolean => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.max_int == null or iv > s.max_int.?) s.max_int = iv;
                },
                .tinyint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.max_int == null or iv > s.max_int.?) s.max_int = iv;
                },
                .smallint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.max_int == null or iv > s.max_int.?) s.max_int = iv;
                },
                .largeint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.max_large == null or v > s.max_large.?) s.max_large = v;
                },
                .decimal64 => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.max_int == null or v > s.max_int.?) s.max_int = v;
                },
                .decimal128 => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.max_large == null or v > s.max_large.?) s.max_large = v;
                },
                .float => |s_f| for (s_f[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const fv: f64 = v;
                    if (s.max_float == null or fv > s.max_float.?) s.max_float = fv;
                },
                .double => |s_d| for (s_d[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.max_float == null or v > s.max_float.?) s.max_float = v;
                },
                .varchar, .string, .char => {
                    var r: u32 = row_start;
                    while (r < row_end) : (r += 1) {
                        if (!view.isValid(r)) continue;
                        const bytes = stringRowBytes(view, r);
                        if (s.max_str == null or std.mem.order(u8, bytes, s.max_str.?) == .gt) {
                            s.max_str = try aa.dupe(u8, bytes);
                        }
                    }
                },
                else => unreachable,
            }
        },
        .avg => {
            const idx = col_idx.?;
            const view = batch.values[idx];
            switch (view.data) {
                .int, .bigint, .boolean, .tinyint, .smallint, .decimal64 => {
                    avgUpdateInt(s, view, row_start, row_end);
                },
                .largeint => |slice| for (slice[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.avg.sum += @as(f64, @floatFromInt(v));
                    s.avg.count += 1;
                },
                .decimal128 => |slice| for (slice[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.avg.sum += @as(f64, @floatFromInt(v));
                    s.avg.count += 1;
                },
                .float => |slice| for (slice[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.avg.sum += v;
                    s.avg.count += 1;
                },
                .double => |slice| for (slice[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.avg.sum += v;
                    s.avg.count += 1;
                },
                else => unreachable,
            }
        },
        .stddev_pop, .stddev_samp, .var_pop, .var_samp => {
            try welfordUpdate(s, batch.values[col_idx.?], row_start, row_end);
        },
        .count_distinct => {
            try distinctUpdate(aa, s, batch.values[col_idx.?], row_start, row_end);
        },
        .percentile => {
            try percentileUpdate(aa, s, batch.values[col_idx.?], row_start, row_end);
        },
        .group_concat => {
            const sep = switch (spec.params) {
                .separator => |sv| sv,
                else => return Error.AggregateInvalidParam,
            };
            try groupConcatUpdate(aa, s, batch.values[col_idx.?], row_start, row_end, sep);
        },
    }
}

/// Welford's online algorithm — numerically stable mean + M2 (sum of
/// squared deviations from the running mean). Variance = M2 / n or
/// M2 / (n-1) depending on population vs sample. Updates one row at a
/// time so the result is invariant to batch boundaries.
fn welfordUpdate(s: *AccState, view: ColumnView, row_start: u32, row_end: u32) !void {
    switch (view.data) {
        inline .int, .bigint, .boolean, .tinyint, .smallint, .largeint, .decimal64, .decimal128 => |slice| {
            for (slice[row_start..row_end], row_start..) |v, r| {
                if (!view.isValid(r)) continue;
                welfordStep(&s.welford, @as(f64, @floatFromInt(v)));
            }
        },
        inline .float, .double => |slice| {
            for (slice[row_start..row_end], row_start..) |v, r| {
                if (!view.isValid(r)) continue;
                welfordStep(&s.welford, @as(f64, v));
            }
        },
        else => unreachable,
    }
}

fn welfordStep(w: *WelfordAcc, x: f64) void {
    w.count += 1;
    const n: f64 = @floatFromInt(w.count);
    const delta = x - w.mean;
    w.mean += delta / n;
    const delta2 = x - w.mean;
    w.m2 += delta * delta2;
}

/// COUNT(DISTINCT col): hash the encoded value bytes; first sighting
/// arena-dups the key for storage. Validation rejects NULL — SQL
/// semantics say NULL is excluded from DISTINCT counts.
fn distinctUpdate(aa: Allocator, s: *AccState, view: ColumnView, row_start: u32, row_end: u32) !void {
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(aa);
    var r: u32 = row_start;
    while (r < row_end) : (r += 1) {
        if (!view.isValid(r)) continue;
        scratch.clearRetainingCapacity();
        try encodeOneValue(aa, &scratch, view, r);
        const gop = try s.distinct.getOrPut(aa, scratch.items);
        if (!gop.found_existing) {
            gop.key_ptr.* = try aa.dupe(u8, scratch.items);
        }
    }
}

/// PERCENTILE_CONT(p): collect every valid value as f64, sort at
/// finalize, linear-interpolate at p×(n-1). Memory O(N).
fn percentileUpdate(aa: Allocator, s: *AccState, view: ColumnView, row_start: u32, row_end: u32) !void {
    switch (view.data) {
        inline .int, .bigint, .boolean, .tinyint, .smallint, .largeint, .date, .datetime, .decimal64, .decimal128 => |slice| {
            for (slice[row_start..row_end], row_start..) |v, r| {
                if (!view.isValid(r)) continue;
                try s.percentile_values.append(aa, @as(f64, @floatFromInt(v)));
            }
        },
        inline .float, .double => |slice| {
            for (slice[row_start..row_end], row_start..) |v, r| {
                if (!view.isValid(r)) continue;
                try s.percentile_values.append(aa, @as(f64, v));
            }
        },
        else => unreachable,
    }
}

/// GROUP_CONCAT: append separator + value bytes for each non-null row.
/// `nonempty` distinguishes "no values yet" from "first value was empty".
fn groupConcatUpdate(aa: Allocator, s: *AccState, view: ColumnView, row_start: u32, row_end: u32, sep: []const u8) !void {
    var r: u32 = row_start;
    while (r < row_end) : (r += 1) {
        if (!view.isValid(r)) continue;
        const bytes = switch (view.data) {
            .string => |sv| sv.rowBytes(r),
            .varchar => |sv| sv.rowBytes(r),
            .char => |sv| sv.rowBytes(r),
            else => unreachable,
        };
        if (s.concat.nonempty) try s.concat.buf.appendSlice(aa, sep);
        try s.concat.buf.appendSlice(aa, bytes);
        s.concat.nonempty = true;
    }
}

/// Bytes of a string-family value at `row`. Caller must ensure the view
/// is varchar/string/char.
fn stringRowBytes(view: ColumnView, row: u32) []const u8 {
    return switch (view.data) {
        .varchar => |sv| sv.rowBytes(row),
        .string => |sv| sv.rowBytes(row),
        .char => |sv| sv.rowBytes(row),
        else => unreachable,
    };
}

/// Encode a single value as bytes for hashing (count_distinct). Mirrors
/// the layout in buildCompoundGroupKey but for one row, one column.
fn encodeOneValue(aa: Allocator, out: *std.ArrayList(u8), view: ColumnView, row: u32) !void {
    switch (view.data) {
        .int, .date => |s| try storage.format.appendI32(aa, out, s[row]),
        .bigint, .datetime => |s| try storage.format.appendI64(aa, out, s[row]),
        .boolean => |s| try out.append(aa, s[row]),
        .tinyint => |s| try out.append(aa, @bitCast(s[row])),
        .smallint => |s| {
            var b: [2]u8 = undefined;
            std.mem.writeInt(i16, &b, s[row], .little);
            try out.appendSlice(aa, &b);
        },
        .largeint => |s| {
            var b: [16]u8 = undefined;
            std.mem.writeInt(i128, &b, s[row], .little);
            try out.appendSlice(aa, &b);
        },
        .decimal64 => |s| try storage.format.appendI64(aa, out, s[row]),
        .decimal128 => |s| {
            var b: [16]u8 = undefined;
            std.mem.writeInt(i128, &b, s[row], .little);
            try out.appendSlice(aa, &b);
        },
        .uuid => |s| {
            var b: [16]u8 = undefined;
            std.mem.writeInt(u128, &b, s[row], .little);
            try out.appendSlice(aa, &b);
        },
        .float => |s| {
            var b: [4]u8 = undefined;
            storage.format.writeF32(&b, s[row]);
            try out.appendSlice(aa, &b);
        },
        .double => |s| {
            var b: [8]u8 = undefined;
            storage.format.writeF64(&b, s[row]);
            try out.appendSlice(aa, &b);
        },
        .string, .varchar, .char => |sv| {
            const bytes = sv.rowBytes(row);
            try storage.format.appendU32(aa, out, @intCast(bytes.len));
            try out.appendSlice(aa, bytes);
        },
    }
}

fn avgUpdateInt(s: *AccState, view: ColumnView, row_start: u32, row_end: u32) void {
    switch (view.data) {
        inline .int, .bigint, .boolean, .tinyint, .smallint, .decimal64 => |slice| {
            for (slice[row_start..row_end], row_start..) |v, r| {
                if (!view.isValid(r)) continue;
                s.avg.sum += @as(f64, @floatFromInt(v));
                s.avg.count += 1;
            }
        },
        else => unreachable,
    }
}

fn appendAccToColumn(
    allocator: Allocator,
    spec: AggSpec,
    state: AccState,
    col: *ColumnStore,
    out_type: Type,
) !void {
    const func = spec.func;
    switch (func) {
        .count => {
            try col.data.bigint.append(allocator, @intCast(state.count));
        },
        .sum => switch (state) {
            .sum_int => |total| switch (out_type) {
                .largeint => try col.data.largeint.append(allocator, total),
                // DESIGN.md §3.4: SUM(DECIMAL) -> DECIMAL128(38, s). i128
                // overflow is impossible here because total is already i128;
                // any further widening would only occur in row-level arithmetic.
                .decimal128 => try col.data.decimal128.append(allocator, total),
                else => {
                    if (total > std.math.maxInt(i64) or total < std.math.minInt(i64)) {
                        return Error.ArithmeticOverflow;
                    }
                    try col.data.bigint.append(allocator, @intCast(total));
                },
            },
            .sum_float => |total| try col.data.double.append(allocator, total),
            else => unreachable,
        },
        .min, .max => switch (state) {
            .min_int, .max_int => {
                const v: i64 = if (func == .min) (state.min_int orelse 0) else (state.max_int orelse 0);
                switch (out_type) {
                    .int => try col.data.int.append(allocator, @intCast(v)),
                    .bigint => try col.data.bigint.append(allocator, v),
                    .boolean => try col.data.boolean.append(allocator, @intCast(v)),
                    .date => try col.data.date.append(allocator, @intCast(v)),
                    .datetime => try col.data.datetime.append(allocator, v),
                    .tinyint => try col.data.tinyint.append(allocator, @intCast(v)),
                    .smallint => try col.data.smallint.append(allocator, @intCast(v)),
                    .decimal64 => try col.data.decimal64.append(allocator, v),
                    else => unreachable,
                }
            },
            .min_large, .max_large => {
                const v: i128 = if (func == .min) (state.min_large orelse 0) else (state.max_large orelse 0);
                switch (out_type) {
                    .largeint => try col.data.largeint.append(allocator, v),
                    .decimal128 => try col.data.decimal128.append(allocator, v),
                    else => unreachable,
                }
            },
            .min_float, .max_float => {
                const v: f64 = if (func == .min) (state.min_float orelse 0.0) else (state.max_float orelse 0.0);
                switch (out_type) {
                    .float => try col.data.float.append(allocator, @floatCast(v)),
                    .double => try col.data.double.append(allocator, v),
                    else => unreachable,
                }
            },
            .min_str, .max_str => {
                // Empty-set MIN/MAX over strings yields "" (we don't surface
                // aggregate-result NULLs yet), matching the numeric path's
                // 0-default.
                const v: []const u8 = if (func == .min) (state.min_str orelse "") else (state.max_str orelse "");
                switch (out_type) {
                    .varchar => try col.data.varchar.appendValue(allocator, v),
                    .string => try col.data.string.appendValue(allocator, v),
                    .char => try col.data.char.appendValue(allocator, v),
                    else => unreachable,
                }
            },
            else => unreachable,
        },
        .avg => {
            const a = state.avg;
            // AVG over an empty set → 0.0 (we don't surface aggregate-result
            // NULLs yet). Guard against div-by-zero.
            const v: f64 = if (a.count == 0) 0.0 else a.sum / @as(f64, @floatFromInt(a.count));
            try col.data.double.append(allocator, v);
        },
        .var_pop, .var_samp, .stddev_pop, .stddev_samp => {
            const w = state.welford;
            const variance: f64 = blk: {
                if (w.count == 0) break :blk 0.0;
                switch (func) {
                    .var_pop, .stddev_pop => break :blk w.m2 / @as(f64, @floatFromInt(w.count)),
                    .var_samp, .stddev_samp => {
                        if (w.count < 2) break :blk 0.0;
                        break :blk w.m2 / @as(f64, @floatFromInt(w.count - 1));
                    },
                    else => unreachable,
                }
            };
            const out: f64 = if (func == .stddev_pop or func == .stddev_samp) @sqrt(variance) else variance;
            try col.data.double.append(allocator, out);
        },
        .count_distinct => {
            try col.data.bigint.append(allocator, @intCast(state.distinct.count()));
        },
        .percentile => {
            const p: f64 = switch (spec.params) {
                .percentile => |pv| pv,
                else => 0.5,
            };
            const vals = state.percentile_values.items;
            if (vals.len == 0) {
                try col.data.double.append(allocator, 0.0);
            } else {
                // Sort in place — arena owns the backing slice; nothing
                // outside this aggregate observes the buffer.
                std.mem.sortUnstable(f64, @constCast(vals), {}, std.sort.asc(f64));
                const n: f64 = @floatFromInt(vals.len);
                // Linear interpolation (PostgreSQL percentile_cont rule):
                //   idx = p * (n - 1); blend floor and ceil.
                const idx = p * (n - 1);
                const lo: usize = @intFromFloat(@floor(idx));
                const hi: usize = @intFromFloat(@ceil(idx));
                const frac = idx - @floor(idx);
                const v = if (lo == hi) vals[lo] else vals[lo] + (vals[hi] - vals[lo]) * frac;
                try col.data.double.append(allocator, v);
            }
        },
        .group_concat => {
            try col.data.string.appendValue(allocator, state.concat.buf.items);
        },
    }
}

/// Pack the group-by columns of the current batch row into `out`. Layout
/// per type matches `comparison.appendColumnValueBytes`. `out` is owned
/// by the caller and is cleared+reused across rows in the accumulate
/// loop — only new groups get arena-owned copies.
fn buildCompoundGroupKey(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    batch: Batch,
    group_col_indices: []const usize,
    row: u32,
) !void {
    for (group_col_indices) |ci| {
        const view = batch.values[ci];
        switch (view.data) {
            .int => |s| try storage.format.appendI32(allocator, out, s[row]),
            .bigint => |s| try storage.format.appendI64(allocator, out, s[row]),
            .boolean => |s| try out.append(allocator, s[row]),
            .varchar => |sv| {
                const bytes = sv.rowBytes(row);
                try storage.format.appendU32(allocator, out, @intCast(bytes.len));
                try out.appendSlice(allocator, bytes);
            },
            .string => |sv| {
                const bytes = sv.rowBytes(row);
                try storage.format.appendU32(allocator, out, @intCast(bytes.len));
                try out.appendSlice(allocator, bytes);
            },
            .float => |s| {
                var b: [4]u8 = undefined;
                storage.format.writeF32(&b, s[row]);
                try out.appendSlice(allocator, &b);
            },
            .double => |s| {
                var b: [8]u8 = undefined;
                storage.format.writeF64(&b, s[row]);
                try out.appendSlice(allocator, &b);
            },
            .date => |s| try storage.format.appendI32(allocator, out, s[row]),
            .datetime => |s| try storage.format.appendI64(allocator, out, s[row]),
            .tinyint => |s| try out.append(allocator, @bitCast(s[row])),
            .smallint => |s| {
                var b: [2]u8 = undefined;
                std.mem.writeInt(i16, &b, s[row], .little);
                try out.appendSlice(allocator, &b);
            },
            .largeint => |s| {
                var b: [16]u8 = undefined;
                std.mem.writeInt(i128, &b, s[row], .little);
                try out.appendSlice(allocator, &b);
            },
            .char => |sv| {
                const bytes = sv.rowBytes(row);
                try storage.format.appendU32(allocator, out, @intCast(bytes.len));
                try out.appendSlice(allocator, bytes);
            },
            .decimal64 => |s| try storage.format.appendI64(allocator, out, s[row]),
            .decimal128 => |s| {
                var b: [16]u8 = undefined;
                std.mem.writeInt(i128, &b, s[row], .little);
                try out.appendSlice(allocator, &b);
            },
            .uuid => |s| {
                var b: [16]u8 = undefined;
                std.mem.writeInt(u128, &b, s[row], .little);
                try out.appendSlice(allocator, &b);
            },
        }
    }
}

/// Decode a packed group key back into the output columns (one value per
/// group column). Mirrors the encoding in `compoundGroupKey`.
fn appendGroupKey(
    allocator: Allocator,
    key_bytes: []const u8,
    group_col_indices: []const usize,
    up_schema: []const Column,
    out_cols: []ColumnStore,
) !void {
    var cursor: usize = 0;
    for (group_col_indices, 0..) |src_idx, i| {
        const t = up_schema[src_idx].type;
        switch (t) {
            .int => {
                const v = storage.format.readI32(key_bytes[cursor .. cursor + 4]);
                cursor += 4;
                try out_cols[i].data.int.append(allocator, v);
            },
            .bigint => {
                const v = storage.format.readI64(key_bytes[cursor .. cursor + 8]);
                cursor += 8;
                try out_cols[i].data.bigint.append(allocator, v);
            },
            .boolean => {
                try out_cols[i].data.boolean.append(allocator, key_bytes[cursor]);
                cursor += 1;
            },
            .varchar, .string, .char => {
                const len = storage.format.readU32(key_bytes[cursor .. cursor + 4]);
                cursor += 4;
                const bytes = key_bytes[cursor .. cursor + len];
                cursor += len;
                const ss: *engine.StringStore = switch (out_cols[i].data) {
                    .varchar => |*x| x,
                    .string => |*x| x,
                    .char => |*x| x,
                    else => unreachable,
                };
                try ss.appendValue(allocator, bytes);
            },
            .float => {
                const v = storage.format.readF32(key_bytes[cursor .. cursor + 4]);
                cursor += 4;
                try out_cols[i].data.float.append(allocator, v);
            },
            .double => {
                const v = storage.format.readF64(key_bytes[cursor .. cursor + 8]);
                cursor += 8;
                try out_cols[i].data.double.append(allocator, v);
            },
            .date => {
                const v = storage.format.readI32(key_bytes[cursor .. cursor + 4]);
                cursor += 4;
                try out_cols[i].data.date.append(allocator, v);
            },
            .datetime => {
                const v = storage.format.readI64(key_bytes[cursor .. cursor + 8]);
                cursor += 8;
                try out_cols[i].data.datetime.append(allocator, v);
            },
            .tinyint => {
                const v: i8 = @bitCast(key_bytes[cursor]);
                cursor += 1;
                try out_cols[i].data.tinyint.append(allocator, v);
            },
            .smallint => {
                const v = std.mem.readInt(i16, key_bytes[cursor..][0..2], .little);
                cursor += 2;
                try out_cols[i].data.smallint.append(allocator, v);
            },
            .largeint => {
                const v = std.mem.readInt(i128, key_bytes[cursor..][0..16], .little);
                cursor += 16;
                try out_cols[i].data.largeint.append(allocator, v);
            },
            .decimal64 => {
                const v = storage.format.readI64(key_bytes[cursor .. cursor + 8]);
                cursor += 8;
                try out_cols[i].data.decimal64.append(allocator, v);
            },
            .decimal128 => {
                const v = std.mem.readInt(i128, key_bytes[cursor..][0..16], .little);
                cursor += 16;
                try out_cols[i].data.decimal128.append(allocator, v);
            },
            .uuid => {
                const v = std.mem.readInt(u128, key_bytes[cursor..][0..16], .little);
                cursor += 16;
                try out_cols[i].data.uuid.append(allocator, v);
            },
        }
    }
}
