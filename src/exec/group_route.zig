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
const engine = @import("../engine/engine.zig");
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
    const partition_ok = partitionCandidate(st, upstream.outputSchema(), group_cols, aggs, top_k, emit_limit, partition_dop) and
        exec.force_group_by == .auto;
    if (group_cols.len > 0 and exec.force_group_by == .auto and !groupKeysSortedPrefix(st.sort_state, group_cols)) budgeted: {
        const account = upstream.accountant() orelse break :budgeted;
        const needs = inputNeeds(upstream, group_cols, aggs, emit_limit, partitioned_aggregate.partitionCount(partition_dop)) orelse break :budgeted;
        const headroom = account.headroom();
        if (trace) {
            traceNeeds(st.upper_rows, needs, headroom, partition_ok);
            if (exec.queryAs(RealizedInput, upstream.*)) |r| std.debug.print(
                "[gbroute]   realized input: {d} chunks, held={d} MiB, largest={d} MiB\n",
                .{ r.owned.chunks.len, r.held_bytes >> 20, r.largest_chunk_bytes >> 20 },
            );
            if (groupState(st, upstream.outputSchema(), group_cols, aggs, emit_limit)) |gs| std.debug.print(
                "[gbroute]   state: groups={d} slot={d} B group={d} B sets={d} MiB\n",
                .{ gs.groups, gs.slot, gs.group, gs.sets >> 20 },
            );
        }
        for (PLAN_ORDER) |plan| {
            if (!needs.admits(plan, partition_ok) or needs.of(plan) > headroom) continue;
            switch (plan) {
                .radix => if (try routeRadixGroupBy(upstream.*, group_cols, aggs, top_k, emit_limit)) |q| {
                    if (trace) std.debug.print("[gbroute]   -> radix\n", .{});
                    return q;
                },
                .partitioned => {
                    if (trace) std.debug.print("[gbroute]   -> partitioned (dop={d})\n", .{partition_dop});
                    return partitioned_aggregate.PartitionedAggregate.create(allocator, worker_alloc, upstream.*, group_cols, aggs, partition_dop, .auto);
                },
                .partitioned_sort => {
                    if (trace) std.debug.print("[gbroute]   -> partitioned, sort cores (dop={d})\n", .{partition_dop});
                    return partitioned_aggregate.PartitionedAggregate.create(allocator, worker_alloc, upstream.*, group_cols, aggs, partition_dop, .sort);
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
        return partitioned_aggregate.PartitionedAggregate.create(allocator, worker_alloc, upstream.*, group_cols, aggs, partition_dop, .auto);
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

/// Narrow a keyed GROUP BY's input to the columns it reads: its keys and
/// every aggregate's inputs. Nothing above the aggregate can read any other
/// input column, but a plan that buffers its input (the partitioned
/// aggregate's chunks, the sort before a streamed group, a realized input)
/// would copy each of them for the whole input: a Compute below passes
/// through the wide column its expression read (issue #390). A parallel scan
/// is told first, so its fused compute and its survivor copy drop those
/// columns too. The input stays as it is when the GROUP BY reads every
/// column, has no keys (nothing is buffered), names a column the input lacks
/// (the aggregate reports it), or a kept column's label resolves to another.
/// Ownership follows `routeGroupBy`: `upstream` is reassigned only on
/// success.
pub fn narrowToAggregateInputs(
    allocator: Allocator,
    upstream: *Query,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
) !void {
    if (group_cols.len == 0) return;
    const schema = upstream.outputSchema();
    const read = try allocator.alloc(bool, schema.len);
    defer allocator.free(read);
    @memset(read, false);
    for (group_cols) |name| read[types.findColumn(schema, name) orelse return] = true;
    for (aggs) |a| {
        if (a.col) |name| read[types.findColumn(schema, name) orelse return] = true;
        if (a.arg2_col) |name| read[types.findColumn(schema, name) orelse return] = true;
        for (a.udf_arg_cols) |name| read[types.findColumn(schema, name) orelse return] = true;
    }
    const kept = std.mem.count(bool, read, &.{true});
    if (kept == schema.len) return;
    const names = try allocator.alloc([]const u8, kept);
    defer allocator.free(names);
    var n: usize = 0;
    for (schema, read, 0..) |col, keep, i| {
        if (!keep) continue;
        if (types.findColumn(schema, col.name) != i) return;
        names[n] = col.name;
        n += 1;
    }
    try upstream.setEmitProjection(names);
    upstream.* = try upstream.project(names);
}

/// True when the partitioned aggregate can carry this GROUP BY on
/// `partition_dop` threads and may beat the hash plan: keyed, no top-k or
/// LIMIT emit (the hash path's early-outs serve those), over an input big
/// enough to repay the threads, with a key space not proven cache-resident.
/// A few groups leave the partitions little to split, and a skewed few put
/// nearly every row in one partition, while the plan still copies its whole
/// input (issue #396: 44 keys, 4x slower than the hash plan).
fn partitionCandidate(
    st: exec.PipelineStats,
    schema: []const types.Column,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    top_k: ?ir.Op.TopK,
    emit_limit: ?u32,
    partition_dop: usize,
) bool {
    return partition_dop > 1 and group_cols.len > 0 and top_k == null and emit_limit == null and
        st.upper_rows >= partitioned_aggregate.MIN_ROWS_FOR_PARALLEL and
        !keySpaceCacheResident(st, schema, group_cols, aggs);
}

/// True when the budget router would weigh the partitioned plan, whose fit
/// turns on the input's size: a caller able to realize the input first
/// (`RealizedInput`) routes on exact bytes instead of the pre-filter bound.
pub fn routesOnInputSize(
    st: exec.PipelineStats,
    schema: []const types.Column,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    top_k: ?ir.Op.TopK,
    emit_limit: ?u32,
    partition_dop: usize,
) bool {
    return exec.force_group_by == .auto and
        partitionCandidate(st, schema, group_cols, aggs, top_k, emit_limit, partition_dop) and
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
/// `partitioned` leaves each partition's core to the operator;
/// `partitioned_sort` makes every partition sort and stream its groups.
pub const Plan = enum { radix, partitioned, partitioned_sort, hash, sort };
pub const PLAN_ORDER = [_]Plan{ .radix, .partitioned, .partitioned_sort, .hash, .sort };

/// Estimated peak bytes each keyed plan allocates beyond what the statement
/// already holds when it is routed. A streaming aggregate over sorted input
/// holds one group, so it has no entry.
pub const PlanNeeds = struct {
    radix: u64,
    partitioned: u64,
    partitioned_sort: u64,
    hash: u64,
    sort: u64,
    /// The partitioned plan's need with hash-table cores, and the input
    /// bytes those cores buffer at once (a round).
    hash_cores: u64,
    round: u64,
    /// Whether the partitioned plan's operator may pick sort cores, which
    /// prices it at the larger of the two cores' needs.
    may_sort: bool,
    /// Whether the estimated groups are near-unique, the only case where
    /// the partitions' sort core pays off.
    near_unique: bool,

    pub fn of(self: PlanNeeds, plan: Plan) u64 {
        return switch (plan) {
            inline else => |p| @field(self, @tagName(p)),
        };
    }

    /// Whether `plan` can carry this GROUP BY at all, before its need is
    /// weighed.
    fn admits(self: PlanNeeds, plan: Plan, partition_ok: bool) bool {
        return switch (plan) {
            .partitioned => partition_ok,
            .partitioned_sort => partition_ok and self.near_unique,
            .radix, .hash, .sort => true,
        };
    }

    /// The needs over an input that already holds `held` charged bytes and
    /// frees each of its chunks, none over `largest_chunk` bytes, once the
    /// plan pulls the next (`RealizedInput`). The partitioned plans and the
    /// sort copy the input as they pull it, so the copy replaces the input's
    /// buffers: they need what they add beyond them, plus the chunk being
    /// copied. Hash cores hold only a round of the copy, so only a round of
    /// the held buffers offsets it. Radix and hash stream the input into
    /// their tables while its buffers are still held.
    pub fn consuming(self: PlanNeeds, held: u64, largest_chunk: u64) PlanNeeds {
        const hash_cores = (self.hash_cores -| @min(held, self.round)) +| largest_chunk;
        const partitioned_sort = (self.partitioned_sort -| held) +| largest_chunk;
        return .{
            .radix = self.radix,
            .partitioned = if (self.may_sort) @max(hash_cores, partitioned_sort) else hash_cores,
            .partitioned_sort = partitioned_sort,
            .hash = self.hash,
            .sort = (self.sort -| held) +| largest_chunk,
            .hash_cores = hash_cores,
            .round = self.round,
            .may_sort = self.may_sort,
            .near_unique = self.near_unique,
        };
    }
};

/// `planNeeds` for `upstream` as the router prices it, with the partitioned
/// plan split `partitions` ways; over a `RealizedInput`, the batches are its
/// chunks and the copying plans are credited with the buffers their copy
/// frees.
pub fn inputNeeds(
    upstream: *Query,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    emit_limit: ?u32,
    partitions: u64,
) ?PlanNeeds {
    const st = upstream.stats();
    const schema = upstream.outputSchema();
    const round = partitioned_aggregate.roundBytes(if (upstream.accountant()) |a| a.budget else null);
    const realized = exec.queryAs(RealizedInput, upstream.*) orelse
        return planNeeds(st, schema, group_cols, aggs, emit_limit, SCAN_BATCH_ROWS, partitions, round);
    const needs = planNeeds(st, schema, group_cols, aggs, emit_limit, realized.largest_chunk_rows, partitions, round) orelse return null;
    return needs.consuming(realized.held_bytes, realized.largest_chunk_bytes);
}

/// Rows a table scan's batch carries: one row group at the default size.
const SCAN_BATCH_ROWS: u64 = 64 * 1024;

/// A hash aggregate's slack over its live bytes: its arena sizes each new
/// node at 1.5x the last plus the request, and its output columns grow by
/// half.
const STATE_SLACK_NUM: u64 = 3;
const STATE_SLACK_DEN: u64 = 2;

/// A group table's slots per entry it holds: the 0.75 load factor, rounded
/// up to a power of two.
const SLOTS_NUM: u64 = 8;
const SLOTS_DEN: u64 = 3;

/// Per-row scratch an Aggregate keeps for its largest batch: each row's key
/// slice, hash and group id.
const BATCH_SCRATCH_BYTES: u64 = 16 + 8 + 4;

/// Growth of a DISTINCT value set past its live bytes: it doubles as values
/// arrive.
const SET_GROWTH: u64 = 2;

/// Width assumed for a string value that no stage or realized buffer has
/// measured — the same guess `memory.estimateColumnBytes` makes.
const GUESSED_STRING_WIDTH: u64 = 32;

/// Each keyed plan's estimated peak over an input described by `st` (row
/// bound, key NDV bounds, measured string widths) and `schema`, read in
/// batches of at most `batch_rows`:
///   - input buffer B = rows × Σ column bytes (a string's width plus its
///     4-byte offset, a validity byte when nullable)
///   - group state S(w), for tables that also take w rows of the batches
///     being inserted (`GroupState`)
///   - radix = hash = S(batch_rows): they stream their input into the table
///   - partitioned with hash-table cores = R + 4 B/row index + 2 W row
///     bytes + S(W): it buffers its input at its exact size (issue #380),
///     but only a round of it: R is the smaller of B and `round_bytes` plus
///     the batch that crosses it. Each of its `partitions` absorbs its rows
///     of a round in windows of `PARTITION_BATCH_ROWS` (W rows in all, their
///     strings in doubling buffers)
///   - partitioned with sort cores (`partitioned_sort`) = 2 B + 8 B/row +
///     1.5 × groups × a group's bytes: each partition gathers its rows out of
///     the buffered input, sorts a row permutation, and streams its groups
///     into its output
///   - `partitioned`, which lets the operator pick the cores, is the larger
///     of the two when the operator may sort (`estimatesCore`)
///   - sort = 1.5 B + 4 B/row permutation.
/// Null when a named column is missing from `schema`.
pub fn planNeeds(
    st: exec.PipelineStats,
    schema: []const types.Column,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    emit_limit: ?u32,
    batch_rows: u64,
    partitions: u64,
    round_bytes: u64,
) ?PlanNeeds {
    const rows = st.upper_rows;
    var row_bytes: u64 = 0;
    for (0..schema.len) |i| row_bytes += columnRowBytes(st, schema, i);
    const input = rows *| row_bytes;
    const state = groupState(st, schema, group_cols, aggs, emit_limit) orelse return null;
    const index = rows *| @sizeOf(u32);
    const windows = @min(rows, partitions *| partitioned_aggregate.PARTITION_BATCH_ROWS);
    const streamed = state.bytes(@min(rows, batch_rows));
    const round_rows = @min(rows, round_bytes / @max(row_bytes, 1) +| batch_rows);
    const round = @min(input, round_rows *| row_bytes);
    const hash_cores = round +| round_rows *| @sizeOf(u32) +| 2 *| windows *| row_bytes +| state.bytes(windows);
    const sort_cores = 2 *| (input +| index) +| state.groups *| state.group *| STATE_SLACK_NUM / STATE_SLACK_DEN;
    const may_sort = partitioned_aggregate.estimatesCore(aggs, rows, partitions);
    return .{
        .radix = streamed,
        .partitioned = if (may_sort) @max(hash_cores, sort_cores) else hash_cores,
        .partitioned_sort = sort_cores,
        .hash = streamed,
        .sort = (input +| input / 2) +| index,
        .hash_cores = hash_cores,
        .round = round,
        .may_sort = may_sort,
        .near_unique = partitioned_aggregate.nearUnique(state.groups, rows),
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

/// A hash aggregate's state. `groups` is the product of the keys' NDV bounds
/// capped at rows, or rows when any key's NDV is unknown; under a bare LIMIT
/// whose aggregates keep bounded state the table stops at `emit_limit`
/// groups plus an overflow group (the hash plan is the only one that takes
/// a LIMIT).
const GroupState = struct {
    groups: u64,
    /// A table slot: its hash and group id, the key slice, and every
    /// aggregate's cell.
    slot: u64,
    /// The key and string payloads a group's state copies out of the
    /// batches, and its emitted row.
    group: u64,
    /// The per-group value sets of DISTINCT aggregates and the values
    /// GROUP_CONCAT / PERCENTILE keep.
    sets: u64,

    /// The state when the tables also take `batch_rows`: an Aggregate grows
    /// its table and cells to hold a whole batch before inserting it, and
    /// keeps per-row scratch for it.
    fn bytes(self: GroupState, batch_rows: u64) u64 {
        const slots = (self.groups +| batch_rows) *| SLOTS_NUM / SLOTS_DEN;
        const live = slots *| self.slot +| self.groups *| self.group +| batch_rows *| BATCH_SCRATCH_BYTES;
        return live *| STATE_SLACK_NUM / STATE_SLACK_DEN +| self.sets;
    }
};

fn groupState(
    st: exec.PipelineStats,
    schema: []const types.Column,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    emit_limit: ?u32,
) ?GroupState {
    const rows = st.upper_rows;
    var slot: u64 = 16 + 16;
    var group: u64 = 0;
    for (group_cols) |gc| {
        const idx = types.findColumn(schema, gc) orelse return null;
        group += valueWidth(st, schema, idx) + columnRowBytes(st, schema, idx);
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
        slot += exec.aggregate_op.aggStateWidth(a.func, in_t, key_t);
        switch (a.func) {
            .min, .max, .any_value, .first, .last, .max_by => {
                const string_value = if (in_t) |t| t.isString() else false;
                if (string_value) {
                    group += valueWidth(st, schema, in_idx.?) + columnRowBytes(st, schema, in_idx.?);
                } else {
                    group += 16;
                }
                const string_key = if (key_t) |t| t.isString() else false;
                if (string_key) group += valueWidth(st, schema, key_idx.?);
            },
            .count_distinct, .sum_distinct, .avg_distinct => {
                group += 16;
                const i = in_idx orelse continue;
                const pairs = if (i < st.column_stats.len) switch (st.column_stats[i].ndv) {
                    .exact => |n| @min(rows, groups *| n),
                    .unknown => rows,
                } else rows;
                sets +|= pairs *| ((valueWidth(st, schema, i) + 16) * 4 / 3 * SET_GROWTH);
            },
            .group_concat, .percentile => {
                group += 16;
                var width: u64 = if (in_idx) |i| valueWidth(st, schema, i) else 0;
                if (key_idx) |i| width += valueWidth(st, schema, i);
                sets +|= rows *| width;
            },
            else => group += 16,
        }
    }
    return .{ .groups = groups, .slot = slot, .group = group, .sets = sets };
}

fn traceNeeds(rows: u64, needs: PlanNeeds, headroom: usize, partition_ok: bool) void {
    const mib = 1024 * 1024;
    const partitioned_na = if (needs.admits(.partitioned, partition_ok)) "" else "(n/a)";
    const sort_cores_na = if (needs.admits(.partitioned_sort, partition_ok)) "" else "(n/a)";
    std.debug.print(
        "[gbroute] rows={d} headroom={d} MiB needs: radix={d} partitioned={d}{s} partitioned_sort={d}{s} hash={d} sort={d} MiB\n",
        .{ rows, headroom / mib, needs.radix / mib, needs.partitioned / mib, partitioned_na, needs.partitioned_sort / mib, sort_cores_na, needs.hash / mib, needs.sort / mib },
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
/// replaces the buffer instead of doubling it (`PlanNeeds.consuming`).
/// Stats are exact: the realized row count, the source's column bounds and
/// each string column's measured width.
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
    /// Bytes the chunks hold, each chunk's column stores and store array.
    held_bytes: u64,
    /// The most any one chunk holds.
    largest_chunk_bytes: u64,
    /// The most rows any one chunk carries.
    largest_chunk_rows: u64,

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
        var held_bytes: u64 = 0;
        var largest_chunk_bytes: u64 = 0;
        var largest_chunk_rows: u64 = 0;
        for (owned.chunks) |c| {
            rows += c.rows;
            var chunk_bytes: u64 = c.stores.len * @sizeOf(engine.ColumnStore);
            for (c.stores) |store| chunk_bytes += store.heldBytes();
            held_bytes += chunk_bytes;
            largest_chunk_bytes = @max(largest_chunk_bytes, chunk_bytes);
            largest_chunk_rows = @max(largest_chunk_rows, c.rows);
        }
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
            .held_bytes = held_bytes,
            .largest_chunk_bytes = largest_chunk_bytes,
            .largest_chunk_rows = largest_chunk_rows,
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

/// True when the key space is proven to fit a cache-resident group table:
/// every key's NDV is exact, and their product (at most the rows) times a
/// group's table bytes stays within `RADIX_CACHE_BYTES`.
pub fn keySpaceCacheResident(
    st: exec.PipelineStats,
    schema: []const types.Column,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
) bool {
    var groups: u64 = 1;
    for (group_cols) |gc| {
        const idx = types.findColumn(schema, gc) orelse return false;
        if (idx >= st.column_stats.len) return false;
        switch (st.column_stats[idx].ndv) {
            .exact => |nd| groups *|= nd,
            .unknown => return false,
        }
    }
    groups = @min(groups, @max(st.upper_rows, 1));
    const per_group = perGroupTableBytes(schema, group_cols, aggs);
    return per_group != 0 and groups *| per_group <= RADIX_CACHE_BYTES;
}

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

        // Known low-cardinality → the hash path's inline-state / count-slot fast
        // paths win, so decline. UNKNOWN cardinality → take radix: its adaptive
        // sizing bounds the worst case (an unexpectedly-huge group count would
        // otherwise hit the generic 96B-state path), trading a few ms on
        // unknown-but-low for bounded behaviour on unknown-but-high.
        if (keySpaceCacheResident(upstream.stats(), schema, group_cols, aggs)) return null;
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
    // A round past the input's size buffers all of it.
    const whole = std.math.maxInt(u64);

    // A row of k (20 B + offset), v (100 B + offset + validity) and t.
    const measured = exec.PipelineStats{ .upper_rows = rows, .column_stats = &.{
        .{ .avg_width = 20 },
        .{ .avg_width = 100 },
        .{},
    } };
    const needs = planNeeds(measured, &schema, &group_cols, &aggs, null, 0, 4, whole).?;
    const row_bytes = 24 + 105 + 8;
    const input = rows * row_bytes;
    // A slot holds the hash, group id, key slice and both aggregates' cells;
    // a group copies its key and MAX_BY value and emits them with its count.
    const state = groupState(measured, &schema, &group_cols, &aggs, null).?;
    const cells = exec.aggregate_op.aggStateWidth(.max_by, schema[1].type, .bigint) + exec.aggregate_op.aggStateWidth(.count, null, null);
    try std.testing.expectEqual(16 + 16 + cells, state.slot);
    try std.testing.expectEqual((20 + 24) + (100 + 105) + 16, state.group);
    try std.testing.expectEqual(rows, state.groups);
    try std.testing.expectEqual(state.bytes(0), needs.hash);
    try std.testing.expectEqual(needs.hash, needs.radix);
    // A streamed batch widens the table and adds its scratch.
    const batched = planNeeds(measured, &schema, &group_cols, &aggs, null, 1000, 4, whole).?;
    try std.testing.expectEqual(state.bytes(1000), batched.hash);
    try std.testing.expect(batched.hash > needs.hash);
    // Four partitions read windows of 64Ki rows, and each one's table takes
    // its window on top of its groups.
    const windows = 4 * partitioned_aggregate.PARTITION_BATCH_ROWS;
    try std.testing.expectEqual(input + 4 * rows + 2 * windows * row_bytes + state.bytes(windows), needs.partitioned);
    try std.testing.expectEqual(input + input / 2 + 4 * rows, needs.sort);
    // Hash cores buffer a round of the input at a time: its bytes, plus the
    // batch that crosses it.
    const round_rows = 10_000_000 / row_bytes + 1000;
    const rounded = planNeeds(measured, &schema, &group_cols, &aggs, null, 1000, 4, 10_000_000).?;
    try std.testing.expectEqual(round_rows * row_bytes + 4 * round_rows + 2 * windows * row_bytes + state.bytes(windows), rounded.partitioned);
    try std.testing.expectEqual(needs.sort, rounded.sort);
    try std.testing.expectEqual(needs.partitioned_sort, rounded.partitioned_sort);
    // Windows past the input's rows hold only its rows.
    const wide = planNeeds(measured, &schema, &group_cols, &aggs, null, 0, 16, whole).?;
    try std.testing.expectEqual(input + 4 * rows + 2 * input + state.bytes(rows), wide.partitioned);
    // An unknown key NDV prices a group per row, each copying out its MAX_BY
    // value and emitting a row: the hash state outweighs a sort of the input.
    try std.testing.expect(needs.sort < needs.hash);
    try std.testing.expect(needs.hash > rows * 2 * (100 + 24 + 105));
    // Sort cores gather each partition's rows out of the buffered input and
    // sort a permutation of them, and the outputs hold every group. They are
    // open to these near-unique groups; with one heavy aggregate the operator
    // keeps its hash cores.
    const sort_cores = 2 * (input + 4 * rows) + state.groups * state.group * 3 / 2;
    try std.testing.expectEqual(sort_cores, needs.partitioned_sort);
    try std.testing.expect(needs.near_unique);
    try std.testing.expect(needs.admits(.partitioned_sort, true));
    try std.testing.expect(!needs.admits(.partitioned_sort, false));
    try std.testing.expect(!needs.admits(.partitioned, false));
    // Two heavy aggregates let the operator pick its cores, so the plan that
    // leaves the choice to it is priced at the larger of the two.
    const heavy_aggs = [_]ir.AggSpec{
        .{ .func = .max_by, .col = "v", .arg2_col = "t", .as = "u" },
        .{ .func = .any_value, .col = "v", .as = "w" },
    };
    const heavy = planNeeds(measured, &schema, &group_cols, &heavy_aggs, null, 0, 4, whole).?;
    const heavy_state = groupState(measured, &schema, &group_cols, &heavy_aggs, null).?;
    const heavy_hash_cores = input + 4 * rows + 2 * windows * row_bytes + heavy_state.bytes(windows);
    try std.testing.expectEqual(2 * (input + 4 * rows) + heavy_state.groups * heavy_state.group * 3 / 2, heavy.partitioned_sort);
    try std.testing.expectEqual(@max(heavy_hash_cores, heavy.partitioned_sort), heavy.partitioned);

    // Over an input whose chunks are freed as the plan copies them, the
    // copying plans need what their copy and state add beyond the held
    // buffers, plus the chunk being copied; the streaming plans keep their
    // whole state on top of them.
    const consumed = needs.consuming(input, 1000);
    try std.testing.expectEqual(needs.radix, consumed.radix);
    try std.testing.expectEqual(needs.partitioned - input + 1000, consumed.partitioned);
    try std.testing.expectEqual(needs.partitioned_sort - input + 1000, consumed.partitioned_sort);
    try std.testing.expectEqual(needs.hash, consumed.hash);
    try std.testing.expectEqual(input / 2 + 4 * rows + 1000, consumed.sort);
    try std.testing.expect(consumed.near_unique);
    const overheld = needs.consuming(needs.partitioned + needs.partitioned_sort + needs.sort, 1000);
    try std.testing.expectEqual(needs.partitioned - input + 1000, overheld.partitioned);
    try std.testing.expectEqual(@as(u64, 1000), overheld.partitioned_sort);
    try std.testing.expectEqual(@as(u64, 1000), overheld.sort);
    // Hash cores copy a round at a time, so the held input past a round
    // still sits beside their tables.
    const rounded_consumed = rounded.consuming(input, 1000);
    try std.testing.expectEqual(rounded.partitioned - round_rows * row_bytes + 1000, rounded_consumed.partitioned);
    try std.testing.expectEqual(consumed.partitioned_sort, rounded_consumed.partitioned_sort);
    // When the operator may sort its partitions, the plan is priced at the
    // larger of the two cores' needs after the credit too.
    const heavy_consumed = heavy.consuming(input, 1000);
    try std.testing.expectEqual(@max(heavy_hash_cores - input + 1000, heavy.partitioned_sort - input + 1000), heavy_consumed.partitioned);

    // A proven key space of 1000 groups prices 1000 groups' state.
    const few = exec.PipelineStats{ .upper_rows = rows, .column_stats = &.{
        .{ .ndv = .{ .exact = 1000 }, .avg_width = 20 },
        .{ .avg_width = 100 },
        .{},
    } };
    const few_state = groupState(few, &schema, &group_cols, &aggs, null).?;
    try std.testing.expectEqual(@as(u64, 1000), few_state.groups);
    const few_needs = planNeeds(few, &schema, &group_cols, &aggs, null, 0, 4, whole).?;
    try std.testing.expectEqual(few_state.bytes(0), few_needs.hash);
    try std.testing.expectEqual(needs.sort, few_needs.sort);
    try std.testing.expect(!few_needs.near_unique);
    try std.testing.expect(!few_needs.admits(.partitioned_sort, true));

    // A bare LIMIT over bounded aggregate state stops the hash table at the
    // limit plus an overflow group; MAX_BY's value is not bounded state.
    const count_only = [_]ir.AggSpec{.{ .func = .count, .col = null, .as = "c" }};
    try std.testing.expectEqual(rows, groupState(measured, &schema, &group_cols, &count_only, null).?.groups);
    try std.testing.expectEqual(@as(u64, 11), groupState(measured, &schema, &group_cols, &count_only, 10).?.groups);
    try std.testing.expectEqual(needs.hash, planNeeds(measured, &schema, &group_cols, &aggs, 10, 0, 4, whole).?.hash);

    // Unmeasured strings take the 32-byte guess.
    const guessed = planNeeds(.{ .upper_rows = rows }, &schema, &group_cols, &aggs, null, 0, 4, whole).?;
    const guessed_input = rows * (36 + 37 + 8);
    try std.testing.expectEqual(guessed_input + guessed_input / 2 + 4 * rows, guessed.sort);

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
    const small_sets = planNeeds(small_t, &schema, &group_cols, &distinct_aggs, null, 0, 4, whole).?.hash;
    const all_sets = planNeeds(all_t, &schema, &group_cols, &distinct_aggs, null, 0, 4, whole).?.hash;
    try std.testing.expectEqual((rows - 10_000) * ((8 + 16) * 4 / 3 * SET_GROWTH), all_sets - small_sets);

    const missing = [_]ir.AggSpec{.{ .func = .max, .col = "nope", .as = "m" }};
    try std.testing.expectEqual(@as(?PlanNeeds, null), planNeeds(measured, &schema, &group_cols, &missing, null, 0, 4, whole));
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
    // Every tracked byte but the chunk array is a chunk's.
    const chunk_array = 3 * @sizeOf(exec.OwnedChunk);
    const realized = exec.queryAs(RealizedInput, q).?;
    try std.testing.expectEqual(held - chunk_array, realized.held_bytes);
    try std.testing.expectEqual(@as(u64, 3000), realized.largest_chunk_rows);

    try std.testing.expectEqual(@as(usize, 1000), (try q.next()).?.row_count);
    try std.testing.expectEqual(held, account.current_bytes);
    try std.testing.expectEqual(@as(usize, 2000), (try q.next()).?.row_count);
    try std.testing.expect(account.current_bytes < held);
    try std.testing.expectEqual(@as(usize, 3000), (try q.next()).?.row_count);
    try std.testing.expectEqual(chunk_array + realized.largest_chunk_bytes, account.current_bytes);
    try std.testing.expect((try q.next()) == null);
}

const RoutedPlan = enum { partitioned, partitioned_sort, hash, sort_stream, other };

const RoutedRun = struct {
    plan: RoutedPlan,
    lines: [][]u8,
    needs: PlanNeeds,
    /// Bytes charged when the plan was chosen.
    held: usize,
    peak: usize,
    /// Whether the budget-blind route admitted the hash-table plans (and so
    /// the partitioned one) at this budget.
    blind_hash_ok: bool,
};

/// Route MAX_BY + COUNT over a realized input under `budget` and run the
/// chosen plan to completion; `blind` runs the partitioned plan the
/// budget-blind route picks instead.
fn testRouteRealized(a: Allocator, budget: usize, partition_dop: usize, blind: bool) !RoutedRun {
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
    var blind_hash_ok: bool = undefined;
    var q = routed: {
        var up = try RealizedInput.create(tracked, exec.makeQuery(tracked, &drained), owned);
        errdefer up.deinit();
        needs = inputNeeds(&up, &group_cols, &aggs, null, partitioned_aggregate.partitionCount(partition_dop)).?;
        held = account.current_bytes;
        blind_hash_ok = groupKeysCardUnderLimit(up.stats(), up.outputSchema(), &group_cols, &aggs, budget);
        if (blind) break :routed try partitioned_aggregate.PartitionedAggregate.create(tracked, worker, up, &group_cols, &aggs, partition_dop, .auto);
        break :routed try routeGroupBy(tracked, worker, &up, &group_cols, &aggs, null, null, budget, partition_dop);
    };
    defer q.deinit();
    const plan: RoutedPlan = if (exec.queryAs(partitioned_aggregate.PartitionedAggregate, q)) |pa| switch (pa.core) {
        .auto => .partitioned,
        .sort => .partitioned_sort,
    } else if (exec.queryAs(exec.Aggregate, q) != null)
        .hash
    else if (exec.queryAs(exec.aggregate_op.SortedAggregate, q) != null)
        .sort_stream
    else
        .other;
    const lines = try testLines(a, &q);
    return .{
        .plan = plan,
        .lines = lines,
        .needs = needs,
        .held = held,
        .peak = account.peak_bytes,
        .blind_hash_ok = blind_hash_ok,
    };
}

test "a budget between the plans' needs routes to the plan that fits, and it matches the other plans' result" {
    const a = std.testing.allocator;
    const roomy = try testRouteRealized(a, 1 << 40, 4, false);
    defer testFreeLines(a, roomy.lines);
    try std.testing.expectEqual(RoutedPlan.partitioned, roomy.plan);
    try std.testing.expectEqual(@as(usize, 5003), roomy.lines.len);
    // The key's NDV is unknown, so the hash-table plans price a group per
    // row. The partitioned plan stays within its price.
    const needs = roomy.needs;
    try std.testing.expect(needs.sort < needs.hash and needs.sort < needs.partitioned);
    try std.testing.expect(roomy.peak - roomy.held <= needs.partitioned);

    // An eighth below what the partitioned plan took, the budget-blind route
    // still partitions, and the router takes the next plan whose need fits.
    // Whether the partitioned plan overruns there moves with how its
    // partitions interleave and how the platform allocator grows buffers, so
    // it is held only to failing cleanly or matching.
    const gap_budget = roomy.held + (roomy.peak - roomy.held) * 7 / 8;
    if (testRouteRealized(a, gap_budget, 4, true)) |forced| {
        defer testFreeLines(a, forced.lines);
        try std.testing.expectEqual(roomy.lines.len, forced.lines.len);
        for (roomy.lines, forced.lines) |r, f| try std.testing.expectEqualStrings(r, f);
    } else |err| try std.testing.expectEqual(error.MemoryBudgetExceeded, err);
    const gap = try testRouteRealized(a, gap_budget, 4, false);
    defer testFreeLines(a, gap.lines);
    try std.testing.expect(gap.blind_hash_ok);
    const gap_headroom = gap_budget - gap.held;
    const fitting: RoutedPlan = if (needs.partitioned_sort <= gap_headroom)
        .partitioned_sort
    else if (needs.hash <= gap_headroom)
        .hash
    else
        .sort_stream;
    try std.testing.expectEqual(fitting, gap.plan);
    try std.testing.expect(gap.peak <= gap_budget);
    try std.testing.expectEqual(roomy.lines.len, gap.lines.len);
    for (roomy.lines, gap.lines) |r, g| try std.testing.expectEqualStrings(r, g);

    // Below the partitioned plans' and the hash need too, sorting the input
    // and streaming the groups fits.
    try std.testing.expect(needs.sort < needs.partitioned_sort);
    const sort_budget = roomy.held + (needs.sort + @min(needs.hash, needs.partitioned, needs.partitioned_sort)) / 2;
    const sorted = try testRouteRealized(a, sort_budget, 4, false);
    defer testFreeLines(a, sorted.lines);
    try std.testing.expectEqual(RoutedPlan.sort_stream, sorted.plan);
    try std.testing.expect(sorted.peak <= sort_budget);
    try std.testing.expectEqual(roomy.lines.len, sorted.lines.len);
    for (roomy.lines, sorted.lines) |r, s| try std.testing.expectEqualStrings(r, s);

    // With no threads to partition across, the hash aggregate carries it.
    const serial = try testRouteRealized(a, 1 << 40, 1, false);
    defer testFreeLines(a, serial.lines);
    try std.testing.expectEqual(RoutedPlan.hash, serial.plan);
    try std.testing.expectEqual(roomy.lines.len, serial.lines.len);
    for (roomy.lines, serial.lines) |r, h| try std.testing.expectEqualStrings(r, h);
}

test "near-unique groups whose hash-table partitions do not fit partition with sort cores" {
    const a = std.testing.allocator;
    const roomy = try testRouteRealized(a, 1 << 40, 4, false);
    defer testFreeLines(a, roomy.lines);
    // The key's NDV is unknown, so the groups count as near-unique and each
    // partition may sort instead of holding a hash table.
    const needs = roomy.needs;
    try std.testing.expect(needs.near_unique);
    try std.testing.expect(needs.partitioned_sort < needs.partitioned);
    const budget = roomy.held + (needs.partitioned_sort + needs.partitioned) / 2;
    const sorted = try testRouteRealized(a, budget, 4, false);
    defer testFreeLines(a, sorted.lines);
    try std.testing.expectEqual(RoutedPlan.partitioned_sort, sorted.plan);
    try std.testing.expect(sorted.peak - sorted.held <= needs.partitioned_sort);
    try std.testing.expectEqual(roomy.lines.len, sorted.lines.len);
    for (roomy.lines, sorted.lines) |r, s| try std.testing.expectEqualStrings(r, s);
}

test "a proven cache-resident key space takes the hash plan, not the partitioned one (issue #396)" {
    const a = std.testing.allocator;
    const aggs = [_]ir.AggSpec{
        .{ .func = .max_by, .col = "v", .arg2_col = "t", .as = "u" },
        .{ .func = .count, .col = null, .as = "c" },
    };
    const group_cols = [_][]const u8{"k"};
    // The same input and budget partition while the key's NDV is unknown.
    const unknown = try testRouteRealized(a, 1 << 40, 4, false);
    defer testFreeLines(a, unknown.lines);
    try std.testing.expectEqual(RoutedPlan.partitioned, unknown.plan);

    const account = try testAccountant(a, 1 << 40);
    defer account.releaseOwner(a);
    const tracked = try account.executionAllocator();
    const worker = try account.wrapAllocator(a);
    const owned = try testOwnedChunks(worker, &.{ 30_000, 30_000, 30_000, 30_000 });
    var drained = TestDrained{ .account = account };
    var q = routed: {
        var up = try RealizedInput.create(tracked, exec.makeQuery(tracked, &drained), owned);
        errdefer up.deinit();
        // Its 5003 keys, proven: their group table stays cache-resident.
        exec.queryAs(RealizedInput, up).?.col_stats[0].ndv = .{ .exact = 5003 };
        try std.testing.expect(keySpaceCacheResident(up.stats(), up.outputSchema(), &group_cols, &aggs));
        try std.testing.expect(!routesOnInputSize(up.stats(), up.outputSchema(), &group_cols, &aggs, null, null, 4));
        break :routed try routeGroupBy(tracked, worker, &up, &group_cols, &aggs, null, null, 1 << 40, 4);
    };
    defer q.deinit();
    try std.testing.expect(exec.queryAs(exec.Aggregate, q) != null);
    const lines = try testLines(a, &q);
    defer testFreeLines(a, lines);
    try std.testing.expectEqual(unknown.lines.len, lines.len);
    for (unknown.lines, lines) |p, h| try std.testing.expectEqualStrings(p, h);
}
