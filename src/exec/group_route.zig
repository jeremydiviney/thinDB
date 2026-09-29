//! GROUP BY strategy routing over an arbitrary upstream Query — the
//! sorted-stream / radix / partitioned / hash / sort plans plus the two-phase
//! join-partial combine. Engine-neutral: depends only on exec operators and
//! pipeline stats, and is shared by the staged CTE/join/window block compiler
//! (net/cte_stages.zig). Strategy:
//!   - input already sorted on the group keys → streaming aggregate
//!   - under a tracked budget, the fastest plan whose estimated peak fits
//!     what the statement has left (`planNeeds`): radix, partitioned, hash,
//!     then sort + stream
//!   - no accountant, a forced mode, or no plan fits → the budget-blind
//!     route: sort when the key space can't fit half the budget, then radix,
//!     partitioned, hash

const std = @import("std");

const getenv_gr = @extern(*const fn (name: [*:0]const u8) callconv(.c) ?[*:0]const u8, .{ .name = "getenv", .library_name = "c" });
const Allocator = std.mem.Allocator;
const exec = @import("exec.zig");
const Query = exec.Query;
const ir = @import("../ir/ir.zig");
const types = @import("../types.zig");
const storage = @import("../storage/storage.zig");
const partitioned_aggregate = @import("partitioned_aggregate.zig");
const parallel_reduce = @import("parallel_reduce.zig");

/// GROUP BY routing shared by the compile paths. An input sorted on the keys
/// streams. Otherwise, when the statement has an accountant and no mode is
/// forced, the first plan in `PLAN_ORDER` whose `planNeeds` estimate fits
/// the accountant's headroom wins: a bigger budget must never pick a plan
/// that needs more than the budget while a slower one fits. When none fits,
/// the budget-blind route stands (spilling is not built yet).
/// `partition_dop` > 1 admits the partitioned plan. Follows
/// `routeStreamGroupBy`'s ownership contract: `upstream` is reassigned in
/// place when the route orders the keys, and the caller's errdefer owns
/// whatever it points at on error; ownership moves into the returned Query
/// on success.
pub fn routeGroupBy(
    allocator: Allocator,
    worker_alloc: Allocator,
    upstream: *Query,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    top_k: ?ir.Op.TopK,
    emit_limit: ?u32,
    budget: usize,
    partition_dop: usize,
) !Query {
    const trace = getenv_gr("THINDB_TRACE_GBROUTE") != null;
    const st = upstream.stats();
    const partition_ok = partitionCandidate(st, group_cols, top_k, emit_limit, partition_dop) and
        exec.force_group_by == .auto;
    if (group_cols.len > 0 and exec.force_group_by == .auto and !groupKeysSortedPrefix(st.sort_state, group_cols)) budgeted: {
        const account = upstream.accountant() orelse break :budgeted;
        const needs = planNeeds(st, upstream.outputSchema(), group_cols, aggs, emit_limit) orelse break :budgeted;
        const headroom = account.headroom();
        if (trace) traceNeeds(st.upper_rows, needs, headroom, partition_ok);
        for (PLAN_ORDER) |plan| {
            if (needs.of(plan) > headroom) continue;
            switch (plan) {
                .radix => if (try routeRadixGroupBy(upstream.*, group_cols, aggs, top_k, emit_limit)) |q| {
                    if (trace) std.debug.print("[gbroute]   -> radix\n", .{});
                    return q;
                },
                .partitioned => if (partition_ok) {
                    if (trace) std.debug.print("[gbroute]   -> partitioned (dop={d})\n", .{partition_dop});
                    return partitioned_aggregate.PartitionedAggregate.create(allocator, worker_alloc, upstream.*, group_cols, aggs, partition_dop);
                },
                .hash => {
                    if (trace) std.debug.print("[gbroute]   -> hash\n", .{});
                    return upstream.groupByTopK(group_cols, aggs, top_k, emit_limit);
                },
                .sort => {
                    if (trace) std.debug.print("[gbroute]   -> sort + stream\n", .{});
                    return sortThenStream(allocator, upstream, group_cols, aggs);
                },
            }
        }
        if (trace) std.debug.print("[gbroute]   no plan fits; budget-blind route\n", .{});
    }
    if (try routeStreamGroupBy(allocator, upstream, group_cols, aggs, budget)) |q| return q;
    if (try routeRadixGroupBy(upstream.*, group_cols, aggs, top_k, emit_limit)) |q| return q;
    if (partition_ok) {
        if (trace) std.debug.print("[gbroute]   -> partitioned (budget-blind, dop={d})\n", .{partition_dop});
        return partitioned_aggregate.PartitionedAggregate.create(allocator, worker_alloc, upstream.*, group_cols, aggs, partition_dop);
    }
    return upstream.groupByTopK(group_cols, aggs, top_k, emit_limit);
}

/// `routeGroupBy` for compile paths with NO materialized stages beneath (a
/// table-backed GROUP BY inside a CTE block never reaches the V2 table
/// handlers or the stage-priming AdaptiveGroupBy): a global aggregate folds
/// on `dop` threads, and a keyed one may partition its hash aggregate across
/// them. upper_rows is a PRE-filter bound here, so the partitioned gate
/// over-admits selective filters — its fixed cost there is one scatter pass
/// plus arena setup.
///
/// A/B notes (plan-selection campaign, customer_monthly_totals: keys=4/SUM
/// over 3.4M filtered rows): pagg = agg 1.31s→0.69s, query −0.6s across 4/4
/// samples. A parallel sort+streamGroupBy arm was tried and is NEGATIVE
/// (the 3.4M-row sort costs more than the serial agg saves; the downstream
/// window does not ride the group-key order). A single-sample pair earlier
/// suggested pagg's shuffled emission was refunded downstream — noise;
/// trust multi-run same-server medians only. THINDB_NO_PAGG_FALLBACK=1
/// restores the serial hash fallback.
pub fn routeGroupByDop(
    allocator: Allocator,
    worker_alloc: Allocator,
    upstream: *Query,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    top_k: ?ir.Op.TopK,
    emit_limit: ?u32,
    budget: usize,
    dop: usize,
) !Query {
    if (group_cols.len == 0 and top_k == null and emit_limit == null and exec.force_group_by == .auto) {
        if (try routeGlobalReduce(allocator, worker_alloc, upstream, aggs, dop)) |q| return q;
    }
    const partition_dop: usize = if (getenv_gr("THINDB_NO_PAGG_FALLBACK") != null) 1 else dop;
    return routeGroupBy(allocator, worker_alloc, upstream, group_cols, aggs, top_k, emit_limit, budget, partition_dop);
}

/// True when the partitioned aggregate can carry this GROUP BY on
/// `partition_dop` threads: keyed, no top-k or LIMIT emit (the hash path's
/// early-outs serve those), over an input big enough to repay the threads.
fn partitionCandidate(
    st: exec.PipelineStats,
    group_cols: []const []const u8,
    top_k: ?ir.Op.TopK,
    emit_limit: ?u32,
    partition_dop: usize,
) bool {
    return partition_dop > 1 and group_cols.len > 0 and top_k == null and emit_limit == null and
        st.upper_rows >= partitioned_aggregate.MIN_ROWS_FOR_PARALLEL;
}

/// True when the budget router would weigh the partitioned plan, whose fit
/// turns on the input's size: a caller able to realize the input first
/// (`RealizedInput`) routes on exact bytes instead of the pre-filter bound.
pub fn routesOnInputSize(
    st: exec.PipelineStats,
    group_cols: []const []const u8,
    top_k: ?ir.Op.TopK,
    emit_limit: ?u32,
    partition_dop: usize,
) bool {
    return exec.force_group_by == .auto and
        partitionCandidate(st, group_cols, top_k, emit_limit, partition_dop) and
        !groupKeysSortedPrefix(st.sort_state, group_cols);
}

fn sortThenStream(
    allocator: Allocator,
    upstream: *Query,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
) !Query {
    const specs = try allocator.alloc(exec.SortSpec, group_cols.len);
    defer allocator.free(specs);
    for (group_cols, specs) |gc, *s| s.* = .{ .col = gc, .desc = false };
    upstream.* = try upstream.orderBy(specs);
    return upstream.streamGroupBy(group_cols, aggs);
}

/// Keyed GROUP BY plans, fastest first: the order the budget router tries.
pub const Plan = enum { radix, partitioned, hash, sort };
pub const PLAN_ORDER = [_]Plan{ .radix, .partitioned, .hash, .sort };

/// Estimated peak bytes each keyed plan allocates beyond what the statement
/// already holds when it is routed. A streaming aggregate over sorted input
/// holds one group, so it has no entry.
pub const PlanNeeds = struct {
    radix: u64,
    partitioned: u64,
    hash: u64,
    sort: u64,

    pub fn of(self: PlanNeeds, plan: Plan) u64 {
        return switch (plan) {
            inline else => |p| @field(self, @tagName(p)),
        };
    }
};

/// Growth of a group table past its live bytes: its arrays double as groups
/// arrive and copied string payloads land in growing buffers, so up to half
/// of what is held is slack.
const GROUP_STATE_GROWTH: u64 = 2;

/// Width assumed for a string value that no stage or realized buffer has
/// measured — the same guess `memory.estimateColumnBytes` makes.
const GUESSED_STRING_WIDTH: u64 = 32;

/// Each keyed plan's estimated peak over an input described by `st` (row
/// bound, key NDV bounds, measured string widths) and `schema`:
///   - input buffer B = rows × Σ column bytes (a string's width plus its
///     4-byte offset, a validity byte when nullable)
///   - group state S = groups × per-group bytes + distinct/collect sets,
///     where groups is the product of the keys' NDV bounds capped at rows,
///     or rows when any key's NDV is unknown; under a bare LIMIT whose
///     aggregates keep bounded state the hash table stops at `emit_limit`
///     groups plus an overflow group (the hash plan is the only one that
///     takes a LIMIT)
///   - partitioned = B + 4 B/row index + S (it buffers its input at its
///     exact size, issue #380); radix = B + S; hash = S (it streams its
///     input); sort = 1.5 B + 4 B/row permutation.
/// Null when a named column is missing from `schema`.
pub fn planNeeds(
    st: exec.PipelineStats,
    schema: []const types.Column,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    emit_limit: ?u32,
) ?PlanNeeds {
    const rows = st.upper_rows;
    var row_bytes: u64 = 0;
    for (0..schema.len) |i| row_bytes += columnRowBytes(st, schema, i);
    const input = rows *| row_bytes;
    const state = groupStateBytes(st, schema, group_cols, aggs, emit_limit) orelse return null;
    const index = rows *| @sizeOf(u32);
    return .{
        .radix = input +| state,
        .partitioned = input +| index +| state,
        .hash = state,
        .sort = (input +| input / 2) +| index,
    };
}

fn valueWidth(st: exec.PipelineStats, schema: []const types.Column, idx: usize) u64 {
    const t = schema[idx].type;
    if (!t.isString()) return exec.memory.estimateColumnBytes(t);
    if (idx < st.column_stats.len) if (st.column_stats[idx].avg_width) |w| return w;
    return GUESSED_STRING_WIDTH;
}

fn columnRowBytes(st: exec.PipelineStats, schema: []const types.Column, idx: usize) u64 {
    const offset: u64 = if (schema[idx].type.isString()) @sizeOf(u32) else 0;
    const validity: u64 = @intFromBool(schema[idx].nullable);
    return valueWidth(st, schema, idx) + offset + validity;
}

fn estimateGroups(st: exec.PipelineStats, schema: []const types.Column, group_cols: []const []const u8) u64 {
    const rows = @max(st.upper_rows, 1);
    var product: u64 = 1;
    for (group_cols) |gc| {
        const idx = types.findColumn(schema, gc) orelse return rows;
        if (idx >= st.column_stats.len) return rows;
        switch (st.column_stats[idx].ndv) {
            .exact => |n| product *|= n,
            .unknown => return rows,
        }
    }
    return @min(product, rows);
}

/// Group table bytes: per group, the slot and key copy with every
/// aggregate's state at the 0.75 load factor, the string payloads the state
/// copies out of the batches, and the emitted row; plus the per-group value
/// sets of DISTINCT aggregates and the values GROUP_CONCAT / PERCENTILE keep.
fn groupStateBytes(
    st: exec.PipelineStats,
    schema: []const types.Column,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    emit_limit: ?u32,
) ?u64 {
    const rows = st.upper_rows;
    var table: u64 = 16 + 16;
    var payload: u64 = 0;
    var out: u64 = 0;
    for (group_cols) |gc| {
        const idx = types.findColumn(schema, gc) orelse return null;
        table += valueWidth(st, schema, idx);
        out += columnRowBytes(st, schema, idx);
    }
    const all_groups = estimateGroups(st, schema, group_cols);
    const capped = emit_limit != null and exec.aggregate_op.aggsAllowGroupCap(aggs);
    const groups = if (capped) @min(all_groups, @as(u64, emit_limit.?) + 1) else all_groups;
    var sets: u64 = 0;
    for (aggs) |a| {
        const in_idx: ?usize = if (a.col) |name| (types.findColumn(schema, name) orelse return null) else null;
        const key_idx: ?usize = if (a.arg2_col) |name| (types.findColumn(schema, name) orelse return null) else null;
        const in_t: ?types.Type = if (in_idx) |i| schema[i].type else null;
        const key_t: ?types.Type = if (key_idx) |i| schema[i].type else null;
        table += exec.aggregate_op.aggStateWidth(a.func, in_t, key_t);
        switch (a.func) {
            .min, .max, .any_value, .first, .last, .max_by => {
                const string_value = if (in_t) |t| t.isString() else false;
                if (string_value) {
                    payload += valueWidth(st, schema, in_idx.?);
                    out += columnRowBytes(st, schema, in_idx.?);
                } else {
                    out += 16;
                }
                const string_key = if (key_t) |t| t.isString() else false;
                if (string_key) payload += valueWidth(st, schema, key_idx.?);
            },
            .count_distinct, .sum_distinct, .avg_distinct => {
                out += 16;
                const i = in_idx orelse continue;
                const pairs = if (i < st.column_stats.len) switch (st.column_stats[i].ndv) {
                    .exact => |n| @min(rows, groups *| n),
                    .unknown => rows,
                } else rows;
                sets +|= pairs *| ((valueWidth(st, schema, i) + 16) * 4 / 3 * GROUP_STATE_GROWTH);
            },
            .group_concat, .percentile => {
                out += 16;
                var width: u64 = if (in_idx) |i| valueWidth(st, schema, i) else 0;
                if (key_idx) |i| width += valueWidth(st, schema, i);
                sets +|= rows *| width;
            },
            else => out += 16,
        }
    }
    const per_group = GROUP_STATE_GROWTH * (table * 4 / 3 + payload + out);
    return (groups *| per_group) +| sets;
}

fn traceNeeds(rows: u64, needs: PlanNeeds, headroom: usize, partition_ok: bool) void {
    const mib = 1024 * 1024;
    std.debug.print(
        "[gbroute] rows={d} headroom={d} MiB needs: radix={d} partitioned={d}{s} hash={d} sort={d} MiB\n",
        .{ rows, headroom / mib, needs.radix / mib, needs.partitioned / mib, if (partition_ok) "" else "(n/a)", needs.hash / mib, needs.sort / mib },
    );
}

/// Parallel reduce for a GLOBAL (no-key) aggregate over a stable-data
/// upstream — the missing parallel lane between the V2 table handlers (which
/// have their own reduce) and the grouped pagg/radix/stream trio (all gated
/// on `group_cols.len > 0`). Without it a `SELECT SUM(..), MAX(..) FROM cte`
/// folds millions of rows serially on the connection thread. Engages only
/// when the upstream's batch data is stable (stage-backed reads and pure view
/// remaps above them — see `VTable.stableData`) and every aggregate is
/// two-phase combinable. THINDB_NO_GLOBAL_REDUCE=1 restores the serial path.
/// Consumes `upstream` only on success.
pub fn routeGlobalReduce(
    allocator: Allocator,
    worker_alloc: Allocator,
    upstream: *Query,
    aggs: []const ir.AggSpec,
    dop: usize,
) !?Query {
    if (dop <= 1) return null;
    if (getenv_gr("THINDB_NO_GLOBAL_REDUCE") != null) return null;
    if (upstream.stats().upper_rows < partitioned_aggregate.MIN_ROWS_FOR_PARALLEL) return null;
    if (!upstream.stableData()) return null;
    return parallel_reduce.ParallelReduceAggregate.create(allocator, worker_alloc, upstream.*, aggs, dop);
}

pub fn routeStreamGroupBy(
    allocator: Allocator,
    upstream: *Query,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    budget: usize,
) !?Query {
    if (group_cols.len == 0) return null;
    const st = upstream.stats();
    switch (exec.force_group_by) {
        .hash, .radix => return null,
        .sort => return try sortThenStream(allocator, upstream, group_cols, aggs),
        .auto => {
            if (groupKeysSortedPrefix(st.sort_state, group_cols)) {
                return try upstream.streamGroupBy(group_cols, aggs);
            }
            if (!groupKeysCardUnderLimit(st, upstream.outputSchema(), group_cols, aggs, budget)) {
                return try sortThenStream(allocator, upstream, group_cols, aggs);
            }
            return null;
        },
    }
}

/// A drained pipeline's owned chunks (`Query.takeOwnedChunks`) replayed as
/// batches, so a GROUP BY can route on its input's realized size. A chunk is
/// freed when the consumer pulls the next one: the chosen plan's own copy
/// replaces the buffer instead of doubling it. Stats are exact: the realized
/// row count, the source's column bounds and each string column's measured
/// width.
pub const RealizedInput = struct {
    allocator: Allocator,
    source: Query,
    owned: exec.OwnedChunks,
    /// Chunks before `freed` are released; the one before `cursor` is the
    /// batch last returned.
    freed: usize = 0,
    cursor: usize = 0,
    views: []storage.ColumnView,
    col_stats: []exec.ColStat,
    rows: u64,

    /// Takes `owned` whatever happens, and `source` on success (a drained
    /// pipeline, kept for its schema and sort order until deinit).
    pub fn create(allocator: Allocator, source: Query, owned: exec.OwnedChunks) !Query {
        errdefer exec.deinitOwnedChunks(owned);
        const schema = source.outputSchema();
        const views = try allocator.alloc(storage.ColumnView, schema.len);
        errdefer allocator.free(views);
        const col_stats = try allocator.alloc(exec.ColStat, schema.len);
        errdefer allocator.free(col_stats);
        var rows: u64 = 0;
        for (owned.chunks) |c| rows += c.rows;
        const bounds = source.stats().column_stats;
        for (col_stats, schema, 0..) |*stat, col, i| {
            stat.* = if (i < bounds.len) bounds[i] else .{};
            if (!col.type.isString()) continue;
            var payload: u64 = 0;
            for (owned.chunks) |c| payload += exec.stringPayloadBytes(c.stores[i].view());
            stat.avg_width = exec.avgWidth(payload, rows);
        }
        exec.capColStats(col_stats, rows);
        const self = try allocator.create(RealizedInput);
        self.* = .{
            .allocator = allocator,
            .source = source,
            .owned = owned,
            .views = views,
            .col_stats = col_stats,
            .rows = rows,
        };
        return exec.makeQuery(allocator, self);
    }

    fn releaseChunks(self: *RealizedInput, end: usize) void {
        while (self.freed < end) : (self.freed += 1) {
            const chunk = &self.owned.chunks[self.freed];
            for (chunk.stores) |*store| store.deinit(self.owned.alloc);
            self.owned.alloc.free(chunk.stores);
        }
    }

    pub fn next(self: *RealizedInput) !?exec.Batch {
        self.releaseChunks(self.cursor);
        if (self.cursor == self.owned.chunks.len) return null;
        const chunk = self.owned.chunks[self.cursor];
        self.cursor += 1;
        for (chunk.stores, self.views) |store, *view| view.* = store.view();
        return .{ .schema = self.source.outputSchema(), .values = self.views, .row_count = chunk.rows };
    }

    pub fn outputSchema(self: *RealizedInput) []const types.Column {
        return self.source.outputSchema();
    }

    pub fn addPrune(_: *RealizedInput, _: exec.Predicate) !void {}

    pub fn stats(self: *RealizedInput) exec.PipelineStats {
        return .{
            .upper_rows = self.rows,
            .sort_state = self.source.stats().sort_state,
            .column_stats = self.col_stats,
        };
    }

    pub fn accountant(self: *RealizedInput) ?*exec.memory.MemoryAccountant {
        return self.source.accountant();
    }

    pub fn explain(self: *RealizedInput, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) !void {
        var buf: [64]u8 = undefined;
        const line = std.fmt.bufPrint(&buf, "RealizedInput rows={d}", .{self.rows}) catch "RealizedInput";
        try exec.explainLine(out, alloc, depth, line);
        try self.source.explain(out, alloc, depth + 1);
    }

    pub fn deinit(self: *RealizedInput) void {
        self.releaseChunks(self.owned.chunks.len);
        self.owned.alloc.free(self.owned.chunks);
        self.allocator.free(self.views);
        self.allocator.free(self.col_stats);
        self.source.deinit();
        self.allocator.destroy(self);
    }
};

/// Est. group-table size (groups × per-group state+key) above which `.auto`
/// routes to the radix-partitioned aggregate. Below it, the table is cache-
/// resident and the hash path's inline-state / count-slot fast paths win.
pub const RADIX_CACHE_BYTES: u64 = 16 * 1024 * 1024;

/// Radix-partitioned aggregate routing — the standard high-cardinality path.
/// Returns a RadixAggregate Query when the GROUP BY qualifies: a native integer
/// key (string/dict-coded keys still take the coded paths the radix operator
/// doesn't carry yet), fixed-state aggregates, and either no LIMIT or a single-
/// key `ORDER BY <agg> LIMIT k` (the downstream OrderBy+Limit re-finalizes
/// order). Under `.auto` a cache-model gate restricts it to high-cardinality
/// cases; `--force-group-by radix` skips that gate (still requires the query to
/// qualify structurally). Returns null to fall through to the hash
/// `groupByTopK`; consumes `upstream` into the returned Query only on success.
pub fn routeRadixGroupBy(
    upstream: Query,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    top_k: ?ir.Op.TopK,
    emit_limit: ?u32,
) !?Query {
    switch (exec.force_group_by) {
        .hash, .sort => return null,
        .auto, .radix => {},
    }
    if (group_cols.len == 0) return null;
    // Top-k emit covers single-key `ORDER BY <agg> LIMIT k` only; a bare LIMIT
    // (emit_limit) is better served by the hash path's insertion-order early-out.
    if (emit_limit != null) return null;
    if (top_k) |tk| {
        if (tk.keys.len != 1) return null;
    }

    const schema = upstream.outputSchema();
    for (group_cols) |gc| {
        const idx = types.findColumn(schema, gc) orelse return null;
        switch (schema[idx].type) {
            .varchar, .string, .char, .json => return null,
            else => {},
        }
    }

    if (exec.force_group_by == .auto) {
        // The single-int-key COUNT(*) shape has a specialized count-in-slot
        // group table on the hash path that beats the generic compact core.
        if (group_cols.len == 1 and aggs.len == 1 and aggs[0].func == .count and aggs[0].col == null) return null;

        const st = upstream.stats();
        var est: u64 = 1;
        var known = true;
        for (group_cols) |gc| {
            const idx = types.findColumn(schema, gc).?;
            if (idx >= st.column_stats.len) {
                known = false;
                break;
            }
            switch (st.column_stats[idx].ndv) {
                .exact => |nd| est *|= nd,
                .unknown => {
                    known = false;
                    break;
                },
            }
        }
        // Known low-cardinality → the hash path's inline-state / count-slot fast
        // paths win, so decline. UNKNOWN cardinality → take radix: its adaptive
        // sizing bounds the worst case (an unexpectedly-huge group count would
        // otherwise hit the generic 96B-state path), trading a few ms on
        // unknown-but-low for bounded behaviour on unknown-but-high.
        if (known) {
            est = @min(est, @max(st.upper_rows, 1));
            const per_group_bytes = perGroupTableBytes(schema, group_cols, aggs);
            if (per_group_bytes != 0 and est *| per_group_bytes <= RADIX_CACHE_BYTES) return null;
        }
    }

    const rtk: ?exec.radix_aggregate.TopK = if (top_k) |tk|
        .{ .k = tk.k, .col = tk.keys[0].col, .desc = tk.keys[0].desc }
    else
        null;

    // create declines (cleanly, without consuming upstream) when the key won't
    // pack into ≤128 bits or an aggregate isn't fixed-state — fall through.
    return upstream.radixGroupBy(group_cols, aggs, rtk) catch |e| switch (e) {
        error.UnsupportedOperatorForType, error.AggregateUnsupportedType => null,
        else => e,
    };
}

/// Two-phase aggregate over a probe-fused join (no env gate — the offer only
/// lands when the upstream chain bottoms out at a ParallelScan already
/// running the join probe in its workers; Join forwards iff fused, an
/// UNFUSED residual Filter swallows it, and table-backed blocks never call
/// this). Each scan chunk runs scan → probe → partial aggregate on its own
/// core; the serial combine above re-aggregates the per-chunk partials.
///
/// Gated to combinable aggregates (COUNT/SUM/MIN/MAX/ANY_VALUE, plus
/// MAX_BY via a hidden max_by_key partial) — global and grouped alike:
/// post-#79 an empty chunk's GLOBAL partial emits COUNT=0 and NULL for
/// everything else, and every combine function skips NULLs (max_by via its
/// pair semantics), so empty partials can't poison the result. Cardinality
/// gate mirrors routeParallelGroupBy: only proven-small key spaces — above
/// the radix cache line the serial combine over ~unreduced partials loses.
///
/// `combine_arena` must outlive the query (the combine Aggregate borrows
/// its specs); callers pass the statement's node arena. Consumes `upstream`
/// only on success.
pub fn routeJoinPartialGroupBy(
    combine_arena: Allocator,
    upstream: *Query,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    top_k: ?ir.Op.TopK,
    emit_limit: ?u32,
) !?Query {
    const trace_pa = getenv_gr("THINDB_TRACE_PARTIALAGG") != null;
    if (trace_pa) std.debug.print("[pagg-route] enter keys={d} aggs={d}\n", .{ group_cols.len, aggs.len });
    if (!parallel_reduce.combinable(aggs)) {
        if (trace_pa) std.debug.print("[pagg-route]   decline: non-combinable agg\n", .{});
        return null;
    }

    const part_aggs = (try parallel_reduce.partialSpecs(combine_arena, upstream.outputSchema(), aggs)) orelse return null;

    if (group_cols.len > 0) {
        // Decline only a PROVEN-large key space. Unknown cardinality takes
        // the partial route anyway: partials are combinable rows, and the
        // combine below (`groupByTopK`) self-routes adaptively — worst case
        // (unreduced partials) costs about one extra hash pass, while the
        // common deep-CTE case (derived keys with unknowable NDV but few
        // realized groups, e.g. a monthly roll-up) collapses in the workers.
        const schema = upstream.outputSchema();
        const st = upstream.stats();
        var est: u64 = 1;
        var known = true;
        for (group_cols) |gc| {
            const idx = types.findColumn(schema, gc) orelse {
                if (trace_pa) std.debug.print("[pagg-route]   decline key not found: {s}\n", .{gc});
                return null;
            };
            if (idx >= st.column_stats.len) {
                known = false;
                break;
            }
            switch (st.column_stats[idx].ndv) {
                .exact => |nd| est *|= nd,
                .unknown => {
                    known = false;
                    break;
                },
            }
        }
        if (known) {
            est = @min(est, @max(st.upper_rows, 1));
            const per_group = perGroupTableBytes(schema, group_cols, part_aggs);
            if (per_group != 0 and est *| per_group > RADIX_CACHE_BYTES) return null;
        }
    }

    const fused = try upstream.tryFuseAggregate(group_cols, part_aggs);
    if (trace_pa) std.debug.print("[pagg-route]   tryFuseAggregate -> {}\n", .{fused});
    if (!fused) return null;

    // Combine specs over the partials (see `parallel_reduce.combineSpecs`);
    // the partial schema is the fused upstream's output.
    const combine = try parallel_reduce.combineSpecs(combine_arena, aggs, part_aggs, upstream.outputSchema(), group_cols.len);
    return try upstream.groupByTopK(group_cols, combine, top_k, emit_limit);
}

fn groupKeysSortedPrefix(state: exec.SortState, group_cols: []const []const u8) bool {
    if (!state.global) return false;
    if (group_cols.len == 0 or state.keys.len < group_cols.len) return false;
    const prefix = state.keys[0..group_cols.len];
    for (group_cols) |gc| {
        var found = false;
        for (prefix) |pk| {
            if (types.columnNameEql(pk, gc)) {
                found = true;
                break;
            }
        }
        if (!found) return false;
    }
    return true;
}

/// each aggregate's real SoA state-column width (`aggStateWidth`: count 8,
/// sum 16, avg 16, numeric min/max 16, 48 for the wide `.other` column). Scaled
/// by the 0.75 load-factor headroom, since the slot table, key array, and state
/// columns are all sized to capacity ≈ groups / 0.75. Single source of truth for
/// both the hash-vs-sort budget gate and the radix-vs-hash cache-residency gate
/// so they size the table identically. Returns 0 iff a group column is missing.
pub fn perGroupTableBytes(
    schema: []const types.Column,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
) u64 {
    var raw: u64 = 16 + 16; // slot {key/hash, gid} + per-group key copy
    for (group_cols) |gc| {
        const idx = types.findColumn(schema, gc) orelse return 0;
        raw += exec.memory.estimateColumnBytes(schema[idx].type);
    }
    for (aggs) |a| {
        const in_t: ?types.Type = if (a.col) |name| blk: {
            const idx = types.findColumn(schema, name) orelse break :blk null;
            break :blk schema[idx].type;
        } else null;
        const key_t: ?types.Type = if (a.arg2_col) |name| blk: {
            const idx = types.findColumn(schema, name) orelse break :blk null;
            break :blk schema[idx].type;
        } else null;
        raw += exec.aggregate_op.aggStateWidth(a.func, in_t, key_t);
    }
    return raw * 4 / 3; // 0.75 load-factor headroom
}

pub fn groupKeysCardUnderLimit(
    st: exec.PipelineStats,
    schema: []const types.Column,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    budget: usize,
) bool {
    const per_group = perGroupTableBytes(schema, group_cols, aggs);
    if (per_group == 0) return false; // missing group column → conservative: sort

    // Use up to half the budget for the group table; the rest covers input
    // batches, the output, and any sibling operators. budget==0 means
    // tracking is disabled — fall back to a fixed 1 GiB allowance.
    const allowed: u64 = if (budget == 0) (1 << 30) else @as(u64, budget) / 2;
    const max_groups = allowed / per_group;

    // Estimate the combined group count, clamped to the row-count ceiling.
    const ceiling: u64 = st.upper_rows;
    var product: u64 = 1;
    var any_unknown = false;
    for (group_cols) |gc| {
        const idx = types.findColumn(schema, gc).?;
        if (idx >= st.column_stats.len) {
            any_unknown = true;
            continue;
        }
        switch (st.column_stats[idx].ndv) {
            .unknown => any_unknown = true,
            .exact => |nd| product *|= nd,
        }
    }
    const est: u64 = if (any_unknown) ceiling else @min(product, ceiling);
    if (exec.trace_group_by) traceGroupByDecision(st, schema, group_cols, per_group, est, max_groups);
    return est < max_groups;
}

/// `--trace-group-by` diagnostic: dump the hash-vs-sort decision inputs. An
/// `OOB` key (schema index ≥ stats length) flags a schema/stats desync — the
/// derived-key NDV-dropping bug class (#337). Off by default; no hot-path cost.
fn traceGroupByDecision(
    st: exec.PipelineStats,
    schema: []const types.Column,
    group_cols: []const []const u8,
    per_group: u64,
    est: u64,
    max_groups: u64,
) void {
    std.debug.print("[gbroute] schema_len={d} stats_len={d} per_group={d} ", .{ schema.len, st.column_stats.len, per_group });
    for (group_cols) |gc| {
        const idx = types.findColumn(schema, gc) orelse {
            std.debug.print("{s}=NOTFOUND ", .{gc});
            continue;
        };
        if (idx >= st.column_stats.len) {
            std.debug.print("{s}@{d}=OOB ", .{ gc, idx });
        } else switch (st.column_stats[idx].ndv) {
            .unknown => std.debug.print("{s}@{d}=unknown ", .{ gc, idx }),
            .exact => |n| std.debug.print("{s}@{d}=ndv({d}) ", .{ gc, idx, n }),
        }
    }
    std.debug.print("| est_groups={d} cutoff_groups={d} -> {s}\n", .{ est, max_groups, if (est < max_groups) "HASH" else "SORT" });
}

test "GROUP BY size estimate + NDV-driven hash/sort gate" {
    const Column = types.Column;
    // Mirrors Q28's shape: a string group key `k` plus an int column feeding
    // AVG, with a COUNT(*). The router sizes the hash table as
    // groups × perGroupTableBytes and routes to sort only when that exceeds
    // budget/2 — so a KNOWN-low key NDV must pick hash, an UNKNOWN one (the
    // derived-key fallback to the row ceiling) must pick sort.
    const schema = [_]Column{
        .{ .name = "k", .type = .{ .varchar = 255 } },
        .{ .name = "len_src", .type = .int },
    };
    const group_cols = [_][]const u8{"k"};
    const aggs = [_]ir.AggSpec{
        .{ .func = .avg, .col = "len_src", .as = "l" },
        .{ .func = .count, .col = null, .as = "c" },
    };

    // The per-group footprint is a real, bounded number (slot + key + state +
    // load factor), dominated by fixed overhead — NOT the string bytes.
    const pg = perGroupTableBytes(&schema, &group_cols, &aggs);
    try std.testing.expect(pg >= 64 and pg <= 256);

    // Budget chosen so the cutoff (budget/2 = 2 GiB) sits between the real
    // 3M-group table (~0.4 GiB) and the 81M row-ceiling fallback (~10 GiB).
    const budget: usize = 4 * 1024 * 1024 * 1024;
    const upper_rows: u64 = 81_000_000;

    // KNOWN low NDV (the real ~3M distinct hosts) → table fits → HASH.
    const known = exec.PipelineStats{
        .upper_rows = upper_rows,
        .column_stats = &.{
            .{ .ndv = .{ .exact = 3_000_000 } },
            .{ .ndv = .unknown },
        },
    };
    try std.testing.expect(groupKeysCardUnderLimit(known, &schema, &group_cols, &aggs, budget));

    // UNKNOWN key NDV → fall back to the row ceiling (81M) → over budget/2 → SORT.
    const unknown = exec.PipelineStats{
        .upper_rows = upper_rows,
        .column_stats = &.{
            .{ .ndv = .unknown },
            .{ .ndv = .unknown },
        },
    };
    try std.testing.expect(!groupKeysCardUnderLimit(unknown, &schema, &group_cols, &aggs, budget));

    // And the propagated bound is what flips it: the SAME query with the key's
    // NDV bounded (e.g. NDV(REGEXP_REPLACE(Referer)) ≤ NDV(Referer)=19.7M) at a
    // budget whose cutoff clears 19.7M×pg but not 81M×pg → HASH not SORT.
    const big_budget: usize = 12 * 1024 * 1024 * 1024; // cutoff 6 GiB
    const bounded = exec.PipelineStats{
        .upper_rows = upper_rows,
        .column_stats = &.{
            .{ .ndv = .{ .exact = 19_700_000 } }, // bound from the source column
            .{ .ndv = .unknown },
        },
    };
    try std.testing.expect(groupKeysCardUnderLimit(bounded, &schema, &group_cols, &aggs, big_budget));
    // Without the bound, the same budget would sort (81M × pg ≈ 10 GiB > 6 GiB).
    try std.testing.expect(!groupKeysCardUnderLimit(unknown, &schema, &group_cols, &aggs, big_budget));
}

test "plan needs price the input buffer, the group state and measured widths" {
    const schema = [_]types.Column{
        .{ .name = "k", .type = .{ .varchar = 255 } },
        .{ .name = "v", .type = .{ .varchar = 255 }, .nullable = true },
        .{ .name = "t", .type = .bigint },
    };
    const group_cols = [_][]const u8{"k"};
    const aggs = [_]ir.AggSpec{
        .{ .func = .max_by, .col = "v", .arg2_col = "t", .as = "u" },
        .{ .func = .count, .col = null, .as = "c" },
    };
    const rows: u64 = 1_000_000;

    // A row of k (20 B + offset), v (100 B + offset + validity) and t.
    const measured = exec.PipelineStats{ .upper_rows = rows, .column_stats = &.{
        .{ .avg_width = 20 },
        .{ .avg_width = 100 },
        .{},
    } };
    const needs = planNeeds(measured, &schema, &group_cols, &aggs, null).?;
    const input = rows * (24 + 105 + 8);
    try std.testing.expectEqual(input + needs.hash, needs.radix);
    try std.testing.expectEqual(input + 4 * rows + needs.hash, needs.partitioned);
    try std.testing.expectEqual(input + input / 2 + 4 * rows, needs.sort);
    // An unknown key NDV prices a group per row, each copying out its MAX_BY
    // value and emitting a row: the hash state outweighs a sort of the input.
    try std.testing.expect(needs.sort < needs.hash);
    try std.testing.expect(needs.hash > rows * 2 * (100 + 24 + 105));

    // A proven key space of 1000 groups prices 1000 groups' state.
    const few = exec.PipelineStats{ .upper_rows = rows, .column_stats = &.{
        .{ .ndv = .{ .exact = 1000 }, .avg_width = 20 },
        .{ .avg_width = 100 },
        .{},
    } };
    const few_needs = planNeeds(few, &schema, &group_cols, &aggs, null).?;
    try std.testing.expectEqual(needs.hash, few_needs.hash * 1000);
    try std.testing.expectEqual(needs.sort, few_needs.sort);

    // A bare LIMIT over bounded aggregate state stops the hash table at the
    // limit plus an overflow group; MAX_BY's value is not bounded state.
    const count_only = [_]ir.AggSpec{.{ .func = .count, .col = null, .as = "c" }};
    const unlimited = planNeeds(measured, &schema, &group_cols, &count_only, null).?;
    const limited = planNeeds(measured, &schema, &group_cols, &count_only, 10).?;
    try std.testing.expectEqual(unlimited.hash / rows * 11, limited.hash);
    try std.testing.expectEqual(needs.hash, planNeeds(measured, &schema, &group_cols, &aggs, 10).?.hash);

    // Unmeasured strings take the 32-byte guess.
    const guessed = planNeeds(.{ .upper_rows = rows }, &schema, &group_cols, &aggs, null).?;
    try std.testing.expectEqual(rows * (36 + 37 + 8), guessed.radix - guessed.hash);

    // A DISTINCT set holds a (group, value) pair per distinct pair, bounded
    // by the rows and by groups × the value's NDV.
    const distinct_aggs = [_]ir.AggSpec{.{ .func = .count_distinct, .col = "t", .as = "d" }};
    const small_t = exec.PipelineStats{ .upper_rows = rows, .column_stats = &.{
        .{ .ndv = .{ .exact = 1000 }, .avg_width = 20 },
        .{},
        .{ .ndv = .{ .exact = 10 } },
    } };
    const all_t = exec.PipelineStats{ .upper_rows = rows, .column_stats = &.{
        .{ .ndv = .{ .exact = 1000 }, .avg_width = 20 },
        .{},
        .{},
    } };
    const small_sets = planNeeds(small_t, &schema, &group_cols, &distinct_aggs, null).?.hash;
    const all_sets = planNeeds(all_t, &schema, &group_cols, &distinct_aggs, null).?.hash;
    try std.testing.expectEqual((rows - 10_000) * ((8 + 16) * 4 / 3 * GROUP_STATE_GROWTH), all_sets - small_sets);

    const missing = [_]ir.AggSpec{.{ .func = .max, .col = "nope", .as = "m" }};
    try std.testing.expectEqual(@as(?PlanNeeds, null), planNeeds(measured, &schema, &group_cols, &missing, null));
}

const test_schema = [_]types.Column{
    .{ .name = "k", .type = .string },
    .{ .name = "v", .type = .string },
    .{ .name = "t", .type = .bigint },
};

/// Owned chunks of `chunk_rows` rows each, as a drained scan hands them over:
/// 5003 string keys, a distinct string value per row, and a unique ordering
/// column for MAX_BY.
fn testOwnedChunks(alloc: Allocator, chunk_rows: []const usize) !exec.OwnedChunks {
    const engine = @import("../engine/engine.zig");
    const chunks = try alloc.alloc(exec.OwnedChunk, chunk_rows.len);
    var built: usize = 0;
    errdefer {
        for (chunks[0..built]) |c| {
            for (c.stores) |*store| store.deinit(alloc);
            alloc.free(c.stores);
        }
        alloc.free(chunks);
    }
    var row: usize = 0;
    for (chunk_rows, chunks) |n, *chunk| {
        const stores = try alloc.alloc(engine.ColumnStore, test_schema.len);
        var inited: usize = 0;
        errdefer {
            for (stores[0..inited]) |*store| store.deinit(alloc);
            alloc.free(stores);
        }
        for (stores, test_schema) |*store, col| {
            store.* = try engine.ColumnStore.init(alloc, col.type, col.nullable);
            inited += 1;
        }
        var buf: [64]u8 = undefined;
        for (0..n) |_| {
            try stores[0].data.string.appendValue(alloc, try std.fmt.bufPrint(&buf, "key-{d}", .{row % 5003}));
            try stores[1].data.string.appendValue(alloc, try std.fmt.bufPrint(&buf, "value-{d}-{d}", .{ row, row * row }));
            try stores[2].data.bigint.append(alloc, @intCast((row * 7919) % 1_000_003));
            row += 1;
        }
        chunk.* = .{ .stores = stores, .rows = n };
        built += 1;
    }
    return .{ .chunks = chunks, .alloc = alloc };
}

/// The drained pipeline a `RealizedInput` keeps for its schema and sort order.
const TestDrained = struct {
    account: *exec.memory.MemoryAccountant,

    pub fn next(_: *TestDrained) !?exec.Batch {
        return null;
    }
    pub fn deinit(_: *TestDrained) void {}
    pub fn outputSchema(_: *TestDrained) []const types.Column {
        return &test_schema;
    }
    pub fn addPrune(_: *TestDrained, _: exec.Predicate) !void {}
    pub fn stats(_: *TestDrained) exec.PipelineStats {
        return .{ .upper_rows = 0 };
    }
    pub fn accountant(self: *TestDrained) ?*exec.memory.MemoryAccountant {
        return self.account;
    }
    pub fn explain(_: *TestDrained, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) !void {
        try exec.explainLine(out, alloc, depth, "TestDrained");
    }
};

fn testAccountant(a: Allocator, budget: usize) !*exec.memory.MemoryAccountant {
    const account = try a.create(exec.memory.MemoryAccountant);
    account.* = exec.memory.MemoryAccountant.initWithPool(budget, null);
    account.trackAllocations(a);
    return account;
}

/// Every output row as `col|col|…|`, sorted: the plans emit groups in
/// different orders.
fn testLines(a: Allocator, q: *Query) ![][]u8 {
    var lines: std.ArrayList([]u8) = .empty;
    errdefer {
        for (lines.items) |line| a.free(line);
        lines.deinit(a);
    }
    while (try q.next()) |b| {
        for (0..b.row_count) |row| {
            var line: std.ArrayList(u8) = .empty;
            defer line.deinit(a);
            for (b.values) |v| {
                if (!v.isValid(row)) {
                    try line.appendSlice(a, "NULL|");
                    continue;
                }
                switch (v.data) {
                    .varchar, .string, .char, .json => |sv| try line.print(a, "{s}|", .{sv.rowBytes(row)}),
                    .bigint => |values| try line.print(a, "{d}|", .{values[row]}),
                    else => return error.UnexpectedColumnType,
                }
            }
            const owned = try line.toOwnedSlice(a);
            errdefer a.free(owned);
            try lines.append(a, owned);
        }
    }
    std.mem.sort([]u8, lines.items, {}, struct {
        fn lt(_: void, x: []u8, y: []u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.lt);
    return lines.toOwnedSlice(a);
}

fn testFreeLines(a: Allocator, lines: [][]u8) void {
    for (lines) |line| a.free(line);
    a.free(lines);
}

test "RealizedInput replays owned chunks with exact stats and frees each once passed" {
    const a = std.testing.allocator;
    const account = try testAccountant(a, 1 << 40);
    defer account.releaseOwner(a);
    const owned = try testOwnedChunks(try account.wrapAllocator(a), &.{ 1000, 2000, 3000 });
    const held = account.current_bytes;
    var drained = TestDrained{ .account = account };
    var q = try RealizedInput.create(a, exec.makeQuery(a, &drained), owned);
    defer q.deinit();

    var key_bytes: u64 = 0;
    var buf: [32]u8 = undefined;
    for (0..6000) |row| key_bytes += (try std.fmt.bufPrint(&buf, "key-{d}", .{row % 5003})).len;
    const st = q.stats();
    try std.testing.expectEqual(@as(u64, 6000), st.upper_rows);
    try std.testing.expectEqual(exec.avgWidth(key_bytes, 6000), st.column_stats[0].avg_width);
    try std.testing.expect(st.column_stats[1].avg_width.? > 10);
    try std.testing.expectEqual(@as(?u32, null), st.column_stats[2].avg_width);

    try std.testing.expectEqual(@as(usize, 1000), (try q.next()).?.row_count);
    try std.testing.expectEqual(held, account.current_bytes);
    try std.testing.expectEqual(@as(usize, 2000), (try q.next()).?.row_count);
    try std.testing.expect(account.current_bytes < held);
    try std.testing.expectEqual(@as(usize, 3000), (try q.next()).?.row_count);
    try std.testing.expect((try q.next()) == null);
}

const RoutedPlan = enum { partitioned, hash, sort_stream, other };

const RoutedRun = struct {
    plan: RoutedPlan,
    lines: [][]u8,
    needs: PlanNeeds,
    held: usize,
};

/// Route MAX_BY + COUNT over a realized input under `budget` and run the
/// chosen plan to completion.
fn testRouteRealized(a: Allocator, budget: usize) !RoutedRun {
    const account = try testAccountant(a, budget);
    defer account.releaseOwner(a);
    const tracked = try account.executionAllocator();
    const worker = try account.wrapAllocator(a);
    const owned = try testOwnedChunks(worker, &.{ 30_000, 30_000, 30_000, 30_000 });
    var drained = TestDrained{ .account = account };
    const group_cols = [_][]const u8{"k"};
    const aggs = [_]ir.AggSpec{
        .{ .func = .max_by, .col = "v", .arg2_col = "t", .as = "u" },
        .{ .func = .count, .col = null, .as = "c" },
    };
    var needs: PlanNeeds = undefined;
    var held: usize = undefined;
    var q = routed: {
        var up = try RealizedInput.create(tracked, exec.makeQuery(tracked, &drained), owned);
        errdefer up.deinit();
        needs = planNeeds(up.stats(), up.outputSchema(), &group_cols, &aggs, null).?;
        held = account.current_bytes;
        break :routed try routeGroupBy(tracked, worker, &up, &group_cols, &aggs, null, null, budget, 4);
    };
    defer q.deinit();
    const plan: RoutedPlan = if (exec.queryAs(partitioned_aggregate.PartitionedAggregate, q) != null)
        .partitioned
    else if (exec.queryAs(exec.Aggregate, q) != null)
        .hash
    else if (exec.queryAs(exec.aggregate_op.SortedAggregate, q) != null)
        .sort_stream
    else
        .other;
    return .{ .plan = plan, .lines = try testLines(a, &q), .needs = needs, .held = held };
}

test "a budget between the plans' needs routes to the plan that fits, and it matches the partitioned result" {
    const a = std.testing.allocator;
    const roomy = try testRouteRealized(a, 1 << 40);
    defer testFreeLines(a, roomy.lines);
    try std.testing.expectEqual(RoutedPlan.partitioned, roomy.plan);
    try std.testing.expectEqual(@as(usize, 5003), roomy.lines.len);
    const needs = roomy.needs;
    try std.testing.expect(needs.sort < needs.hash and needs.hash < needs.partitioned);

    // Between the hash and partitioned needs the budget-blind route took the
    // partitioned plan; the hash aggregate fits.
    const mid = try testRouteRealized(a, roomy.held + (needs.hash + needs.partitioned) / 2);
    defer testFreeLines(a, mid.lines);
    try std.testing.expectEqual(RoutedPlan.hash, mid.plan);
    try std.testing.expectEqual(roomy.lines.len, mid.lines.len);
    for (roomy.lines, mid.lines) |r, m| try std.testing.expectEqualStrings(r, m);

    // Below the hash need, sorting the input and streaming the groups fits.
    const tight = try testRouteRealized(a, roomy.held + (needs.sort + needs.hash) / 2);
    defer testFreeLines(a, tight.lines);
    try std.testing.expectEqual(RoutedPlan.sort_stream, tight.plan);
    try std.testing.expectEqual(roomy.lines.len, tight.lines.len);
    for (roomy.lines, tight.lines) |r, t| try std.testing.expectEqualStrings(r, t);
}
