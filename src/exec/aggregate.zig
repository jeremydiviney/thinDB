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

const simd = @import("../util/simd.zig");

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

/// One ORDER BY key in a top-k hint: an aggregate output column name + its
/// sort direction. Mirrors `ir.SortSpec`; kept dependency-free so `aggregate`
/// need not import `ir` (which already depends on this file's `AggSpec`).
pub const TopKKey = struct {
    col: []const u8,
    desc: bool,
};

/// Planner hint: this hash aggregate sits directly under `ORDER BY <keys>
/// LIMIT k`. When every key resolves to a numeric aggregate output, the
/// operator emits only the top-k groups instead of every group (the downstream
/// OrderBy+Limit then re-sort the small set).
pub const TopKHint = struct {
    k: u32,
    keys: []const TopKKey,
};

/// A comparable order value pulled from a finalized accumulator. The active
/// variant follows the accumulator (integer-family vs float), and every group
/// shares the same variant for a given key — so comparisons only ever match
/// `int`↔`int` or `float`↔`float`.
const OrderVal = union(enum) {
    int: i128,
    float: f64,
};

/// One ORDER BY key after binding its column name to a concrete aggregate
/// index. Direction is per-key (mixed ASC/DESC supported).
const ResolvedKey = struct {
    agg_idx: usize,
    desc: bool,
};

/// `TopKHint` after binding every key. Owns `keys` (allocator-backed; freed in
/// `deinit`). `null` (unresolved) means fall back to emitting all groups.
const ResolvedTopK = struct {
    k: usize,
    keys: []ResolvedKey,
};

/// Maximum ORDER BY keys the fusion handles. Beyond this the hint is left
/// unresolved (full emit) — analytic top-N almost never sorts on more keys, so
/// a small inline value cache beats a per-entry allocation.
const MAX_TOPK_KEYS: usize = 4;

/// One group competing for a top-k slot. `key`/`state` borrow into the group
/// hash table (valid only until the result batch is materialized); `vals`
/// caches each ORDER BY key's order value so the heap comparator does no
/// accumulator decoding in its inner loop. Only `vals[0..keys.len]` is live.
const TopKEntry = struct {
    key: []const u8,
    state: []AccState,
    vals: [MAX_TOPK_KEYS]OrderVal,
};

/// Heap path is used only when k is modest; beyond this we emit all groups
/// and let the downstream Limit trim, avoiding a large heap allocation for a
/// degenerate `LIMIT <huge>`.
const TOPK_HEAP_CAP: usize = 1 << 16;

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
    /// MIN/MAX for LARGEINT/DECIMAL128 inputs (don't fit in i64). Held inline
    /// as a presence-flagged struct rather than `?i128` so the i128 can sit at
    /// 8-byte alignment — that keeps the whole `AccState` union at 32 bytes
    /// instead of 48 (the `?i128`'s 16-byte alignment would dominate).
    min_large: LargeAcc,
    max_large: LargeAcc,
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
    /// group_concat buffer, lazily boxed (the `ConcatAcc` is 32 bytes — too
    /// wide to sit inline without inflating every other group's state — so it
    /// lives behind a pointer, null until the first value is appended).
    concat: ?*ConcatAcc,
};

/// Inline MIN/MAX accumulator for 128-bit inputs. `align(8)` on the i128 keeps
/// the enclosing `AccState` union 8-aligned (32 B) rather than 16-aligned.
const LargeAcc = struct { v: i128 align(8) = 0, present: bool = false };

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
    /// True when grouping by exactly one string-typed column. The group key
    /// is then the row's raw string bytes (already decoded in the batch), so
    /// the per-row key build skips the scratch copy + length prefix and the
    /// stored key needs no decoding on output.
    single_str_key: bool = false,

    /// Resolved top-k hint, or null to emit every group (the default).
    top_k: ?ResolvedTopK = null,

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
        top_k: ?TopKHint,
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

        // Resolve the top-k hint against this aggregate's output. Fuses only
        // when grouping and *every* order key binds to a numeric aggregate
        // output (string MIN/MAX, stddev/variance, percentile, group_concat,
        // and group-key columns have no `OrderVal` — any such key leaves the
        // whole hint unresolved so the operator falls back to a full emit).
        const resolved_top_k = try resolveTopK(allocator, top_k, aggs, group_cols.len, output_schema);
        errdefer if (resolved_top_k) |r| allocator.free(r.keys);

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
            .single_str_key = group_col_indices.len == 1 and switch (up_schema[group_col_indices[0]].type) {
                .string, .varchar, .char => true,
                else => false,
            },
            .top_k = resolved_top_k,
        };

        // Pre-size the group hash table from the upstream cardinality
        // estimate so it doesn't rehash repeatedly as it fills toward its
        // final size (a high-card GROUP BY otherwise rehashes ~log2(N) times,
        // re-moving every live entry each time). The router only sends us
        // here when this count fits the budget, so the up-front allocation is
        // safe. Skipped when the estimate is unknown or trivially small.
        if (group_col_indices.len > 0) {
            const st = self.upstream.stats();
            var est: u64 = 1;
            var known = true;
            for (group_col_indices) |ci| {
                if (ci >= st.column_cards.len) {
                    known = false;
                    break;
                }
                switch (st.column_cards[ci]) {
                    .exact => |nd| est *|= nd,
                    .unknown => {
                        known = false;
                        break;
                    },
                }
            }
            if (known and est > 1024) {
                const cap: u32 = @intCast(@min(est, @max(st.upper_rows, 1)));
                self.groups.ensureTotalCapacity(self.arena.allocator(), cap) catch {};
            }
        }
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
        if (self.top_k) |r| self.allocator.free(r.keys);
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
        } else if (self.top_k) |r| {
            // Use the bounded-heap top-k path only when k is modest and
            // actually smaller than the group count; otherwise emitting every
            // group and letting the downstream Limit trim is cheaper than a
            // large heap that would keep nearly all groups anyway.
            if (r.k <= TOPK_HEAP_CAP and r.k < self.groups.count()) {
                try self.appendTopKResults(r);
            } else {
                try self.appendGroupedResults();
            }
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
        // Single string key: the key is the row's raw string bytes, already
        // sitting decoded in the batch — no scratch copy / length prefix.
        const str_view: ?storage.StringView = if (self.single_str_key)
            switch (batch.values[self.group_col_indices[0]].data) {
                .string, .varchar, .char => |sv| sv,
                else => unreachable,
            }
        else
            null;
        var row: u32 = 0;
        while (row < n) : (row += 1) {
            // Build the key into a reusable scratch buffer instead of
            // allocating fresh storage every row. Most rows hit existing
            // groups — the scratch bytes only need to outlive the lookup
            // itself, so we can wipe and reuse them next iteration. New
            // groups get an arena-owned copy.
            const key: []const u8 = if (str_view) |sv| sv.rowBytes(row) else blk: {
                self.key_scratch.clearRetainingCapacity();
                try buildCompoundGroupKey(self.allocator, &self.key_scratch, batch, self.group_col_indices, row);
                break :blk self.key_scratch.items;
            };

            const gop = try self.groups.getOrPut(aa, key);
            if (!gop.found_existing) {
                // New group — reserve its memory against the query
                // budget. Approximate: key bytes + per-agg state +
                // ~32 bytes hashmap overhead.
                const approx = key.len + self.aggs.len * @sizeOf(AccState) + 32;
                if (self.upstream.accountant()) |acct| {
                    try acct.reserve(.hash_aggregate, approx);
                }
                self.reserved_bytes += approx;
                // The hashmap borrowed `key`, which points into either the
                // reused scratch buffer or the batch's column bytes — both
                // outlive only this iteration. Replace key_ptr with an
                // arena-owned dup so it survives.
                gop.key_ptr.* = try aa.dupe(u8, key);

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
            try self.appendGroupRow(entry.key_ptr.*, entry.value_ptr.*);
        }
    }

    /// Materialize one group's key + aggregate values into `output_columns`.
    /// Copies into allocator-owned storage, so the borrowed `key_bytes` /
    /// `state` (which live in the group arena) need not outlive this call.
    fn appendGroupRow(self: *Aggregate, key_bytes: []const u8, state: []AccState) !void {
        if (self.single_str_key) {
            // Raw string bytes — no compound framing to decode.
            switch (self.output_columns[0].data) {
                .string, .varchar, .char => |*ss| try ss.appendValue(self.allocator, key_bytes),
                else => unreachable,
            }
        } else {
            try appendGroupKey(self.allocator, key_bytes, self.group_col_indices, self.upstream.outputSchema(), self.output_columns[0..self.group_col_indices.len]);
        }

        for (self.aggs, 0..) |a, ai| {
            const out_idx = self.group_col_indices.len + ai;
            try appendAccToColumn(self.allocator, a, state[ai], &self.output_columns[out_idx], self.output_schema[out_idx].type);
        }
    }

    /// Top-k emit path: a single pass over the group hash table keeps only the
    /// `k` most-preferred groups (by the resolved ORDER BY keys, lexicographic
    /// with per-key direction) in a bounded heap, then materializes just those.
    /// The downstream OrderBy+Limit re-sort the small surviving set, so the heap
    /// need not produce sorted output — it only has to pick the correct k
    /// groups. This avoids building (and string-copying) every group's row only
    /// for TopN to discard them.
    fn appendTopKResults(self: *Aggregate, r: ResolvedTopK) !void {
        const k = r.k;
        const heap = try self.arena.allocator().alloc(TopKEntry, k);
        var len: usize = 0;

        var it = self.groups.iterator();
        while (it.next()) |entry| {
            const cand = topkEntry(entry.key_ptr.*, entry.value_ptr.*, r.keys);
            if (len < k) {
                heap[len] = cand;
                len += 1;
                if (len == k) topkBuildHeap(heap, r.keys);
            } else if (topkLessPreferred(heap[0], cand, r.keys)) {
                // The current worst kept group (root) is less preferred than
                // the candidate — evict it.
                heap[0] = cand;
                topkSiftDown(heap, 0, k, r.keys);
            }
        }

        for (heap[0..len]) |w| {
            try self.appendGroupRow(w.key, w.state);
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

/// Bind a top-k hint to concrete aggregate indices. Returns `null` (fall back
/// to a full emit) unless grouping and *every* order key names an aggregate
/// whose output is orderable. On success the returned `keys` slice is
/// allocator-owned (freed in `Aggregate.deinit`).
fn resolveTopK(
    allocator: Allocator,
    hint: ?TopKHint,
    aggs: []const AggSpec,
    group_cols_len: usize,
    output_schema: []const Column,
) !?ResolvedTopK {
    const h = hint orelse return null;
    if (group_cols_len == 0 or h.keys.len == 0 or h.keys.len > MAX_TOPK_KEYS) return null;
    const rkeys = try allocator.alloc(ResolvedKey, h.keys.len);
    for (h.keys, rkeys) |hk, *rk| {
        const idx = findAggByName(aggs, hk.col) orelse {
            allocator.free(rkeys);
            return null;
        };
        if (!topkOrderable(aggs[idx].func, output_schema[group_cols_len + idx].type)) {
            allocator.free(rkeys);
            return null;
        }
        rk.* = .{ .agg_idx = idx, .desc = hk.desc };
    }
    return ResolvedTopK{ .k = h.k, .keys = rkeys };
}

fn findAggByName(aggs: []const AggSpec, name: []const u8) ?usize {
    for (aggs, 0..) |a, i| {
        if (std.mem.eql(u8, a.as, name)) return i;
    }
    return null;
}

/// Whether `func` (producing output type `out_t`) yields a value the top-k heap
/// can order. String MIN/MAX, stddev/variance, percentile and group_concat have
/// no numeric `OrderVal`; everything else (count, sum, avg, numeric MIN/MAX)
/// does. Aggregates never surface NULL — empty/all-null groups emit 0/0.0 — so
/// `aggOrderValue` is always defined for an orderable key.
fn topkOrderable(func: AggFunc, out_t: Type) bool {
    return switch (func) {
        .count, .count_distinct, .sum, .avg => true,
        .min, .max => !out_t.isString(),
        else => false,
    };
}

/// Extract a comparable order value from a finalized accumulator, mirroring the
/// value `appendAccToColumn` would emit — including the 0/0.0 defaults for
/// empty MIN/MAX/AVG — so the heap orders groups identically to the downstream
/// OrderBy. Only reached for variants `topkOrderable` accepts.
fn aggOrderValue(s: AccState) OrderVal {
    return switch (s) {
        .count => |c| .{ .int = @intCast(c) },
        .sum_int => |v| .{ .int = v },
        .sum_float => |v| .{ .float = v },
        .min_int, .max_int => |m| .{ .int = m orelse 0 },
        .min_large, .max_large => |m| .{ .int = if (m.present) m.v else 0 },
        .min_float, .max_float => |m| .{ .float = m orelse 0.0 },
        .avg => |a| .{ .float = if (a.count == 0) 0.0 else a.sum / @as(f64, @floatFromInt(a.count)) },
        .distinct => |set| .{ .int = @intCast(set.count()) },
        else => unreachable,
    };
}

fn ovOrder(a: OrderVal, b: OrderVal) std.math.Order {
    return switch (a) {
        .int => |x| std.math.order(x, b.int),
        .float => |x| std.math.order(x, b.float),
    };
}

/// Build a heap entry, caching each ORDER BY key's order value up front so the
/// comparator never touches the (large, scattered) accumulator union.
fn topkEntry(key: []const u8, state: []AccState, keys: []const ResolvedKey) TopKEntry {
    var e = TopKEntry{ .key = key, .state = state, .vals = undefined };
    for (keys, 0..) |kk, i| e.vals[i] = aggOrderValue(state[kk.agg_idx]);
    return e;
}

/// Lexicographic comparison of two groups under the resolved ORDER BY keys.
/// `.lt` ⟺ `a` ranks before `b` in the final ordering (i.e. `a` is more
/// preferred / closer to the kept set), honoring each key's direction.
fn topkOrder(a: TopKEntry, b: TopKEntry, keys: []const ResolvedKey) std.math.Order {
    for (keys, 0..) |key, i| {
        const ord = ovOrder(a.vals[i], b.vals[i]);
        if (ord != .eq) return if (key.desc) ord.invert() else ord;
    }
    return .eq;
}

/// True when `a` ranks after `b` (so `a` is the better eviction candidate).
fn topkLessPreferred(a: TopKEntry, b: TopKEntry, keys: []const ResolvedKey) bool {
    return topkOrder(a, b, keys) == .gt;
}

/// Min-heap on preference: the root is the least-preferred kept entry, so a new
/// candidate need only beat the root to earn a slot.
fn topkSiftDown(heap: []TopKEntry, start: usize, n: usize, keys: []const ResolvedKey) void {
    var i = start;
    while (true) {
        const l = 2 * i + 1;
        const r = 2 * i + 2;
        var least = i;
        if (l < n and topkLessPreferred(heap[l], heap[least], keys)) least = l;
        if (r < n and topkLessPreferred(heap[r], heap[least], keys)) least = r;
        if (least == i) break;
        std.mem.swap(TopKEntry, &heap[i], &heap[least]);
        i = least;
    }
}

fn topkBuildHeap(heap: []TopKEntry, keys: []const ResolvedKey) void {
    const n = heap.len;
    if (n < 2) return;
    var i = n / 2;
    while (i > 0) {
        i -= 1;
        topkSiftDown(heap, i, n, keys);
    }
}

// Fold a SIMD-reduced extreme into the running MIN/MAX accumulator. Used by
// the no-null fast paths; `m` comes from `simd.minOf`/`maxOf` over the batch.
fn foldMinInt(s: *AccState, m: i64) void {
    if (s.min_int == null or m < s.min_int.?) s.min_int = m;
}
fn foldMaxInt(s: *AccState, m: i64) void {
    if (s.max_int == null or m > s.max_int.?) s.max_int = m;
}
fn foldMinLarge(s: *AccState, m: i128) void {
    if (!s.min_large.present or m < s.min_large.v) s.min_large = .{ .v = m, .present = true };
}
fn foldMaxLarge(s: *AccState, m: i128) void {
    if (!s.max_large.present or m > s.max_large.v) s.max_large = .{ .v = m, .present = true };
}
fn foldMinFloat(s: *AccState, m: f64) void {
    if (s.min_float == null or m < s.min_float.?) s.min_float = m;
}
fn foldMaxFloat(s: *AccState, m: f64) void {
    if (s.max_float == null or m > s.max_float.?) s.max_float = m;
}

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
            .{ .min_large = .{} }
        else
            .{ .min_int = null },
        .max => if (in != null and in.?.isFloat())
            .{ .max_float = null }
        else if (in != null and in.?.isString())
            .{ .max_str = null }
        else if (in != null and (in.? == .largeint or in.? == .decimal128))
            .{ .max_large = .{} }
        else
            .{ .max_int = null },
        .avg => .{ .avg = .{ .sum = 0.0, .count = 0 } },
        .stddev_pop, .stddev_samp, .var_pop, .var_samp => .{ .welford = .{} },
        .count_distinct => .{ .distinct = .empty },
        .percentile => .{ .percentile_values = .empty },
        .group_concat => .{ .concat = null },
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
            const lo = row_start;
            const hi = row_end;
            if (view.nulls == null) {
                // No nulls in this column: reduce the contiguous range with a
                // tight SIMD kernel (no per-row validity branch). Small ints
                // widen through i64 lanes; 64-bit-plus inputs stay scalar
                // (i64-lane overflow / i128 isn't a SIMD win) but lose the
                // per-row branch.
                switch (view.data) {
                    .int => |sl| s.sum_int += simd.sumWiden(i32, sl[lo..hi]),
                    .smallint => |sl| s.sum_int += simd.sumWiden(i16, sl[lo..hi]),
                    .tinyint => |sl| s.sum_int += simd.sumWiden(i8, sl[lo..hi]),
                    .boolean => |sl| s.sum_int += simd.sumWiden(u8, sl[lo..hi]),
                    .float => |sl| s.sum_float += simd.sumFloat(f32, sl[lo..hi]),
                    .double => |sl| s.sum_float += simd.sumFloat(f64, sl[lo..hi]),
                    .bigint, .decimal64 => |sl| for (sl[lo..hi]) |v| {
                        s.sum_int += v;
                    },
                    .largeint, .decimal128 => |sl| for (sl[lo..hi]) |v| {
                        s.sum_int += v;
                    },
                    else => unreachable,
                }
                return;
            }
            switch (view.data) {
                .int => |s_int| for (s_int[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .bigint => |s_b| for (s_b[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .boolean => |s_b| for (s_b[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .tinyint => |s_b| for (s_b[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .smallint => |s_b| for (s_b[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .largeint => |s_b| for (s_b[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .decimal64 => |s_b| for (s_b[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .decimal128 => |s_b| for (s_b[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .float => |s_f| for (s_f[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_float += v;
                },
                .double => |s_d| for (s_d[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_float += v;
                },
                else => unreachable,
            }
        },
        .min => {
            const idx = col_idx.?;
            const view = batch.values[idx];
            const lo = row_start;
            const hi = row_end;
            // No-null numeric fast path: SIMD-reduce the range, fold the one
            // extreme into the accumulator. Strings can't reduce numerically —
            // they fall through to the scalar loop below.
            if (view.nulls == null and hi > lo) switch (view.data) {
                .int, .date => |sl| return foldMinInt(s, simd.minOf(i32, sl[lo..hi])),
                .bigint, .datetime => |sl| return foldMinInt(s, simd.minOf(i64, sl[lo..hi])),
                .boolean => |sl| return foldMinInt(s, simd.minOf(u8, sl[lo..hi])),
                .tinyint => |sl| return foldMinInt(s, simd.minOf(i8, sl[lo..hi])),
                .smallint => |sl| return foldMinInt(s, simd.minOf(i16, sl[lo..hi])),
                .decimal64 => |sl| return foldMinInt(s, simd.minOf(i64, sl[lo..hi])),
                .largeint, .decimal128 => |sl| return foldMinLarge(s, simd.minOf(i128, sl[lo..hi])),
                .float => |sl| return foldMinFloat(s, simd.minOf(f32, sl[lo..hi])),
                .double => |sl| return foldMinFloat(s, simd.minOf(f64, sl[lo..hi])),
                .varchar, .string, .char => {},
                else => unreachable,
            };
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
                    if (!s.min_large.present or v < s.min_large.v) s.min_large = .{ .v = v, .present = true };
                },
                .decimal64 => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.min_int == null or v < s.min_int.?) s.min_int = v;
                },
                .decimal128 => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (!s.min_large.present or v < s.min_large.v) s.min_large = .{ .v = v, .present = true };
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
            const lo = row_start;
            const hi = row_end;
            if (view.nulls == null and hi > lo) switch (view.data) {
                .int, .date => |sl| return foldMaxInt(s, simd.maxOf(i32, sl[lo..hi])),
                .bigint, .datetime => |sl| return foldMaxInt(s, simd.maxOf(i64, sl[lo..hi])),
                .boolean => |sl| return foldMaxInt(s, simd.maxOf(u8, sl[lo..hi])),
                .tinyint => |sl| return foldMaxInt(s, simd.maxOf(i8, sl[lo..hi])),
                .smallint => |sl| return foldMaxInt(s, simd.maxOf(i16, sl[lo..hi])),
                .decimal64 => |sl| return foldMaxInt(s, simd.maxOf(i64, sl[lo..hi])),
                .largeint, .decimal128 => |sl| return foldMaxLarge(s, simd.maxOf(i128, sl[lo..hi])),
                .float => |sl| return foldMaxFloat(s, simd.maxOf(f32, sl[lo..hi])),
                .double => |sl| return foldMaxFloat(s, simd.maxOf(f64, sl[lo..hi])),
                .varchar, .string, .char => {},
                else => unreachable,
            };
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
                    if (!s.max_large.present or v > s.max_large.v) s.max_large = .{ .v = v, .present = true };
                },
                .decimal64 => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.max_int == null or v > s.max_int.?) s.max_int = v;
                },
                .decimal128 => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (!s.max_large.present or v > s.max_large.v) s.max_large = .{ .v = v, .present = true };
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
            const lo = row_start;
            const hi = row_end;
            // No-null fast path: SIMD-sum the range, count is the row count.
            // Integers sum exactly in i128 then convert (more accurate than the
            // per-row f64 accumulation it replaces); floats use f64 lanes.
            if (view.nulls == null and hi > lo) {
                switch (view.data) {
                    .int => |sl| s.avg.sum += @floatFromInt(simd.sumWiden(i32, sl[lo..hi])),
                    .smallint => |sl| s.avg.sum += @floatFromInt(simd.sumWiden(i16, sl[lo..hi])),
                    .tinyint => |sl| s.avg.sum += @floatFromInt(simd.sumWiden(i8, sl[lo..hi])),
                    .boolean => |sl| s.avg.sum += @floatFromInt(simd.sumWiden(u8, sl[lo..hi])),
                    .bigint, .decimal64 => |sl| for (sl[lo..hi]) |v| {
                        s.avg.sum += @floatFromInt(v);
                    },
                    .largeint, .decimal128 => |sl| for (sl[lo..hi]) |v| {
                        s.avg.sum += @floatFromInt(v);
                    },
                    .float => |sl| s.avg.sum += simd.sumFloat(f32, sl[lo..hi]),
                    .double => |sl| s.avg.sum += simd.sumFloat(f64, sl[lo..hi]),
                    else => unreachable,
                }
                s.avg.count += hi - lo;
                return;
            }
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
        const c = s.concat orelse blk: {
            const box = try aa.create(ConcatAcc);
            box.* = .{};
            s.concat = box;
            break :blk box;
        };
        if (c.nonempty) try c.buf.appendSlice(aa, sep);
        try c.buf.appendSlice(aa, bytes);
        c.nonempty = true;
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
                const v: i128 = if (func == .min)
                    (if (state.min_large.present) state.min_large.v else 0)
                else
                    (if (state.max_large.present) state.max_large.v else 0);
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
            const items: []const u8 = if (state.concat) |c| c.buf.items else "";
            try col.data.string.appendValue(allocator, items);
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
