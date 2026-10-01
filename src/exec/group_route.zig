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
const group_table = @import("group_table.zig");
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
    var sampled: ?u64 = null;
    if (group_cols.len > 0 and exec.force_group_by == .auto and !groupKeysSortedPrefix(st.sort_state, group_cols)) budgeted: {
        const account = upstream.accountant() orelse break :budgeted;
        const priced_cols = try allocator.alloc(exec.ColStat, upstream.outputSchema().len);
        defer allocator.free(priced_cols);
        const priced = try withSampledWidths(allocator, upstream, st, group_cols, aggs, priced_cols);
        sampled = try sampledGroups(allocator, upstream, st, group_cols);
        const needs = inputNeeds(upstream, priced, group_cols, aggs, emit_limit, partitioned_aggregate.partitionCount(partition_dop), sampled) orelse break :budgeted;
        const headroom = account.headroom();
        if (trace) {
            traceNeeds(st.upper_rows, needs, headroom, partition_ok);
            traceInput(st, priced, upstream.outputSchema());
            if (exec.queryAs(RealizedInput, upstream.*)) |r| std.debug.print(
                "[gbroute]   realized input: {d} chunks, held={d} MiB, largest={d} MiB\n",
                .{ r.owned.chunks.len, r.held_bytes >> 20, r.largest_chunk_bytes >> 20 },
            );
            if (groupState(priced, upstream.outputSchema(), group_cols, aggs, emit_limit, sampled)) |gs| {
                std.debug.print(
                    "[gbroute]   state: groups={d} slot={d} B group={d} B payload={d} B out={d}+{d} B sets={d} MiB; hash cores absorb={d} emit={d} MiB\n",
                    .{ gs.groups, gs.slot, gs.group, gs.payload, gs.out_fixed, gs.out_strings, gs.sets >> 20, needs.cores_absorb >> 20, needs.cores_emit >> 20 },
                );
                if (gs.radix) |fp| std.debug.print(
                    "[gbroute]   radix: slot={d} B cell={d} B out={d} B + {d} validity bits\n",
                    .{ fp.slot, fp.cell, fp.out, fp.nullable_outs },
                );
            }
        }
        for (PLAN_ORDER) |plan| {
            if (!needs.admits(plan, partition_ok) or needs.of(plan) > headroom) continue;
            switch (plan) {
                .radix => if (try routeRadixGroupBy(upstream.*, group_cols, aggs, top_k, emit_limit, sampled)) |q| {
                    if (trace) std.debug.print("[gbroute]   -> radix\n", .{});
                    return q;
                },
                .partitioned => {
                    const presize = presizeGroups(priced, upstream.outputSchema(), group_cols, aggs, headroom, sampled);
                    if (trace) std.debug.print("[gbroute]   -> partitioned (dop={d}, presize={d} groups)\n", .{ partition_dop, presize });
                    return partitioned_aggregate.PartitionedAggregate.create(allocator, worker_alloc, upstream.*, group_cols, aggs, partition_dop, .auto, presize);
                },
                .partitioned_sort => {
                    if (trace) std.debug.print("[gbroute]   -> partitioned, sort cores (dop={d})\n", .{partition_dop});
                    return partitioned_aggregate.PartitionedAggregate.create(allocator, worker_alloc, upstream.*, group_cols, aggs, partition_dop, .sort, 0);
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
    if (try routeRadixGroupBy(upstream.*, group_cols, aggs, top_k, emit_limit, sampled)) |q| return q;
    if (partition_ok) {
        if (trace) std.debug.print("[gbroute]   -> partitioned (budget-blind, dop={d})\n", .{partition_dop});
        return partitioned_aggregate.PartitionedAggregate.create(allocator, worker_alloc, upstream.*, group_cols, aggs, partition_dop, .auto, 0);
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
    /// The partitioned plan's need with hash-table cores: the larger of
    /// its absorb phase (`cores_absorb`: a round of buffered input beside
    /// the tables) and its emit phase (`cores_emit`: the tables beside every
    /// partition's output). `round` is the input bytes a round buffers.
    hash_cores: u64,
    cores_absorb: u64,
    cores_emit: u64,
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
            .radix => self.radix != std.math.maxInt(u64),
            .hash, .sort => true,
        };
    }

    /// The needs over an input that already holds `held` charged bytes and
    /// frees each of its chunks, none over `largest_chunk` bytes, once the
    /// plan pulls the next (`RealizedInput`). The partitioned plans and the
    /// sort copy the input as they pull it, so the copy replaces the input's
    /// buffers: they need what they add beyond them, plus the chunk being
    /// copied. Hash cores hold only a round of the copy, so only a round of
    /// the held buffers offsets their absorb phase; by their emit every
    /// chunk is freed. Radix and hash stream the input into their tables
    /// while its buffers are still held.
    pub fn consuming(self: PlanNeeds, held: u64, largest_chunk: u64) PlanNeeds {
        const cores_absorb = (self.cores_absorb -| @min(held, self.round)) +| largest_chunk;
        const cores_emit = (self.cores_emit -| held) +| largest_chunk;
        const hash_cores = @max(cores_absorb, cores_emit);
        const partitioned_sort = (self.partitioned_sort -| held) +| largest_chunk;
        return .{
            .radix = self.radix,
            .partitioned = if (self.may_sort) @max(hash_cores, partitioned_sort) else hash_cores,
            .partitioned_sort = partitioned_sort,
            .hash = self.hash,
            .sort = (self.sort -| held) +| largest_chunk,
            .hash_cores = hash_cores,
            .cores_absorb = cores_absorb,
            .cores_emit = cores_emit,
            .round = self.round,
            .may_sort = self.may_sort,
            .near_unique = self.near_unique,
        };
    }
};

/// `planNeeds` for `upstream` described by `st` (its stats as the router
/// prices them, `withSampledWidths`), with the partitioned plan split
/// `partitions` ways; over a `RealizedInput`, the batches are its chunks and
/// the copying plans are credited with the buffers their copy frees.
pub fn inputNeeds(
    upstream: *Query,
    st: exec.PipelineStats,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    emit_limit: ?u32,
    partitions: u64,
    sampled: ?u64,
) ?PlanNeeds {
    const schema = upstream.outputSchema();
    const round = partitioned_aggregate.roundBytes(if (upstream.accountant()) |a| a.budget else null);
    const realized = exec.queryAs(RealizedInput, upstream.*) orelse
        return planNeeds(st, schema, group_cols, aggs, emit_limit, SCAN_BATCH_ROWS, partitions, round, sampled);
    const needs = planNeeds(st, schema, group_cols, aggs, emit_limit, realized.largest_chunk_rows, partitions, round, sampled) orelse return null;
    return needs.consuming(realized.held_bytes, realized.largest_chunk_bytes);
}

/// Rows a table scan's batch carries: one row group at the default size.
const SCAN_BATCH_ROWS: u64 = 64 * 1024;

/// The sort cores' slack over their live group bytes.
const STATE_SLACK_NUM: u64 = 3;
const STATE_SLACK_DEN: u64 = 2;

/// A hash aggregate's arena over the payload it copies: each new node is
/// 1.5x the last plus the request, so it holds 1.0x to 1.5x of what it hands
/// out; 1.23x and 1.31x were measured on ClickBench string GROUP BYs (issue
/// #464).
const ARENA_SLACK_NUM: u64 = 4;
const ARENA_SLACK_DEN: u64 = 3;

/// An emitted string column the emit can't reserve exactly grows by half
/// at a time, so it holds 1.25x its bytes on average.
const OUTPUT_GROWTH_NUM: u64 = 5;
const OUTPUT_GROWTH_DEN: u64 = 4;

/// A group table's slots per entry it holds: the 0.75 load factor, rounded
/// up to a power of two (the presize caps).
const SLOTS_NUM: u64 = 8;
const SLOTS_DEN: u64 = 3;

/// Per-row scratch an Aggregate keeps for its largest batch: each row's key
/// slice, hash and group id.
const BATCH_SCRATCH_BYTES: u64 = 16 + 8 + 4;

/// Growth of a DISTINCT value set past its live bytes: it doubles as values
/// arrive.
const SET_GROWTH: u64 = 2;

/// Width assumed for a string value that no stage, realized buffer or table
/// sample has measured — the same guess `memory.estimateColumnBytes` makes.
const GUESSED_STRING_WIDTH: u64 = 32;

/// Each keyed plan's estimated peak over an input described by `st` (row
/// bound, key NDV bounds, measured string widths), `schema` and a key
/// sample's group estimate when there is one (`sampled`, see
/// `GroupState`), read in batches of at most `batch_rows`:
///   - input buffer B = rows × Σ column bytes (a string's width plus its
///     4-byte offset, a validity byte when nullable)
///   - radix's state S(w), for a table that also takes w rows of the
///     batches being inserted (`GroupState.bytes`, issue #476): the larger
///     of its drain, which holds its table, cells and batch scratch, and its
///     emit, which holds its cells and output; a hash aggregate's tables H(t, w),
///     split over t tables that each take w rows at a time, beside its
///     arena, and its output O (`GroupState.hashTables`, issue #464)
///   - radix = S(batch_rows): it streams its input into its table
///   - hash = H(1, batch_rows) + O: it streams its input into its table and
///     emits every group before freeing it
///   - partitioned with hash-table cores = the larger of its absorb phase,
///     R + 4 B/row index + 2 W row bytes + H(partitions, window), and its
///     emit phase, H(partitions, window) + O: it buffers its input at its
///     exact size (issue #380), but only a round of it: R is the smaller of
///     B and `round_bytes` plus the batch that crosses it. Each of its
///     `partitions` absorbs its rows of a round in windows of
///     `PARTITION_BATCH_ROWS` (W rows in all, their strings in doubling
///     buffers); the partitions then emit together, each core freeing its
///     table once its output is built
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
    sampled: ?u64,
) ?PlanNeeds {
    const rows = st.upper_rows;
    const row_bytes = rowBytes(st, schema);
    const input = rows *| row_bytes;
    const state = groupState(st, schema, group_cols, aggs, emit_limit, sampled) orelse return null;
    const index = rows *| @sizeOf(u32);
    const windows = @min(rows, partitions *| partitioned_aggregate.PARTITION_BATCH_ROWS);
    const streamed = state.bytes(@min(rows, batch_rows));
    const serial = state.hashTables(1, @min(rows, batch_rows));
    const cores = state.hashTables(partitions, @min(rows, partitioned_aggregate.PARTITION_BATCH_ROWS));
    const round_rows = @min(rows, round_bytes / @max(row_bytes, 1) +| batch_rows);
    const round = @min(input, round_rows *| row_bytes);
    const cores_absorb = round +| round_rows *| @sizeOf(u32) +| 2 *| windows *| row_bytes +| cores.held;
    const cores_emit = cores.held +| cores.output;
    const hash_cores = @max(cores_absorb, cores_emit);
    const sort_cores = 2 *| (input +| index) +| state.groups *| state.group *| STATE_SLACK_NUM / STATE_SLACK_DEN;
    const may_sort = partitioned_aggregate.estimatesCore(aggs, rows, partitions);
    return .{
        .radix = streamed,
        .partitioned = if (may_sort) @max(hash_cores, sort_cores) else hash_cores,
        .partitioned_sort = sort_cores,
        .hash = serial.held +| serial.output,
        .sort = (input +| input / 2) +| index,
        .hash_cores = hash_cores,
        .cores_absorb = cores_absorb,
        .cores_emit = cores_emit,
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

/// A string key's bytes in one group: its distinct width, since a group
/// holds one copy of its key however many rows carry it, else its per-row
/// width.
fn keyWidth(st: exec.PipelineStats, schema: []const types.Column, idx: usize) u64 {
    if (idx < st.column_stats.len) if (st.column_stats[idx].distinct_width) |w| return w;
    return valueWidth(st, schema, idx);
}

/// A string aggregate's kept value in one group: one of its rows' values.
/// A skewed key leaves most groups with few rows, whose values the per-row
/// width weighs by the big groups' rows instead, so the wider of the two
/// widths.
fn keptValueWidth(st: exec.PipelineStats, schema: []const types.Column, idx: usize) u64 {
    return @max(keyWidth(st, schema, idx), valueWidth(st, schema, idx));
}

/// Whether `func`'s state keeps one of its input values (and MAX_BY's key)
/// per group.
fn keepsValue(func: ir.AggFunc) bool {
    return switch (func) {
        .min, .max, .any_value, .first, .last, .max_by => true,
        else => false,
    };
}

fn columnRowBytes(st: exec.PipelineStats, schema: []const types.Column, idx: usize) u64 {
    const offset: u64 = if (schema[idx].type.isString()) @sizeOf(u32) else 0;
    const validity: u64 = @intFromBool(schema[idx].nullable);
    return valueWidth(st, schema, idx) + offset + validity;
}

fn rowBytes(st: exec.PipelineStats, schema: []const types.Column) u64 {
    var bytes: u64 = 0;
    for (0..schema.len) |i| bytes += columnRowBytes(st, schema, i);
    return bytes;
}

/// `st` with its column stats copied into `out` (one per output column of
/// `upstream`), giving each string column no stage or realized buffer
/// measured the width `Query.sampleWidths` samples from its table, and
/// each string a group keeps (a key, an aggregate's value) its distinct
/// width. An unfiltered table input is then priced from its data rather
/// than the 32-byte guess, which can be off by 3x on a URL column (issue
/// #397), and its keys by the bytes a group holds rather than the row mean,
/// which an empty string on most rows pulls 7x below it (issue #464).
fn withSampledWidths(
    allocator: Allocator,
    upstream: *Query,
    st: exec.PipelineStats,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    out: []exec.ColStat,
) !exec.PipelineStats {
    const schema = upstream.outputSchema();
    const widths = try allocator.alloc(exec.SampledWidth, schema.len);
    defer allocator.free(widths);
    for (out, widths, 0..) |*o, *w, i| {
        o.* = if (i < st.column_stats.len) st.column_stats[i] else .{};
        w.* = .{ .row = o.avg_width, .distinct = o.distinct_width };
    }
    for (group_cols) |gc| {
        if (types.findColumn(schema, gc)) |i| widths[i].distinct_wanted = true;
    }
    for (aggs) |a| {
        if (!keepsValue(a.func)) continue;
        for ([_]?[]const u8{ a.col, a.arg2_col }) |name| {
            const i = types.findColumn(schema, name orelse continue) orelse continue;
            widths[i].distinct_wanted = true;
        }
    }
    var unmeasured = false;
    for (schema, widths) |col, w| {
        if (col.type.isString() and !w.complete()) unmeasured = true;
    }
    if (unmeasured) try upstream.sampleWidths(widths);
    for (schema, out, widths) |col, *o, w| {
        if (!col.type.isString()) continue;
        o.avg_width = o.avg_width orelse w.row;
        o.distinct_width = o.distinct_width orelse w.distinct;
    }
    var priced = st;
    priced.column_stats = out;
    return priced;
}

/// The product of the keys' NDV bounds capped at the rows, or at the row
/// origin's rows when every key comes from it (`exec.keyTupleBound`), and
/// at `sampled` when given; null when a key's NDV is unknown.
fn estimateGroups(st: exec.PipelineStats, schema: []const types.Column, group_cols: []const []const u8, sampled: ?u64) ?u64 {
    var product: u64 = 1;
    var from_row = true;
    for (group_cols) |gc| {
        const idx = types.findColumn(schema, gc) orelse return null;
        if (idx >= st.column_stats.len) return null;
        from_row = from_row and exec.fromRowOrigin(st, st.column_stats[idx]);
        switch (st.column_stats[idx].ndv) {
            .exact => |n| product *|= n,
            .unknown => return null,
        }
    }
    const bound = @min(product, @max(exec.keyTupleBound(st, from_row), 1));
    return @min(bound, sampled orelse bound);
}

/// Rows per value of the widest key a key sample must hold before the
/// router trusts its tuples per value (issue #478). A sample that sees a
/// key value once or twice can't tell a key with one partner from one
/// with many it hasn't met yet.
const KEY_SAMPLE_MIN_REPEATS: u64 = 4;

/// The fewest groups a key sample may shrink: below that, radix's first
/// table already holds them, and the sample would cost more than its
/// estimate saves.
const KEY_SAMPLE_MIN_GROUPS: u64 = 64 * 1024;

/// What the router prices over a key sample's estimate (issue #478): a
/// plan is priced for this many times the groups the sample estimates,
/// capped at the keys' NDV product, so a sample that undercounts the
/// groups by up to half still prices the groups the plan will hold.
const SAMPLED_GROUPS_MARGIN: u64 = 2;

/// The groups a key sample estimates for a multi-key GROUP BY whose NDV
/// product overshoots (issue #478), or null to keep the product. The
/// product treats the keys as independent; when one key nearly fixes the
/// others (a user's search engine, an IP's region) it can overshoot the
/// groups many times over. The sample counts key tuples per value of the
/// widest key, and the estimate scales that by the widest key's NDV.
///
/// Sampled only when there is something to win: at least two keys, every
/// key's NDV known, and a product past both twice the widest key's NDV and
/// `KEY_SAMPLE_MIN_GROUPS`. A sample of the whole input counts its tuples
/// outright. A partial one is trusted only when it saw each value of the
/// widest key `KEY_SAMPLE_MIN_REPEATS` times on average, when its rows
/// repeat their tuples at least twice on average (a sample whose tuples are
/// mostly new has not seen enough of each key's partners to count them),
/// and when it saw at least 1/`SAMPLED_GROUPS_MARGIN` of each widest value's
/// rows on average (`coversWidestRows`).
fn sampledGroups(
    allocator: Allocator,
    upstream: *Query,
    st: exec.PipelineStats,
    group_cols: []const []const u8,
) !?u64 {
    if (group_cols.len < 2) return null;
    const schema = upstream.outputSchema();
    const bound = estimateGroups(st, schema, group_cols, null) orelse return null;
    const all_cols = try allocator.alloc(usize, group_cols.len);
    defer allocator.free(all_cols);
    var n_cols: usize = 0;
    var widest: usize = 0;
    var widest_ndv: u64 = 0;
    var widest_name: []const u8 = "";
    for (group_cols) |gc| {
        const idx = types.findColumn(schema, gc) orelse return null;
        const ndv = st.column_stats[idx].ndv.exact;
        // A key of one value and no NULLs (a constant, often computed)
        // adds no tuples.
        if (ndv <= 1 and !schema[idx].nullable) continue;
        if (ndv > widest_ndv) {
            widest = n_cols;
            widest_ndv = ndv;
            widest_name = gc;
        }
        all_cols[n_cols] = idx;
        n_cols += 1;
    }
    if (n_cols < 2 or bound <= KEY_SAMPLE_MIN_GROUPS or bound / 2 <= widest_ndv) return null;
    const cols = all_cols[0..n_cols];
    const trace = getenv_gr("THINDB_TRACE_KEYSAMPLE") != null;
    const started = exec.prof.nowTicks();
    var sample = try exec.KeySample.init(allocator, cols.len);
    defer sample.deinit(allocator);
    const answered = try upstream.sampleKeys(cols, &sample);
    const elapsed: u64 = @intCast(@max(exec.prof.nowTicks() - started, 0));
    exec.prof.addPhase("plan.key_sample", elapsed);
    const tuples = sample.tuple.estimate();
    const widest_seen = sample.keys[widest].estimate();
    const verdict: KeySampleVerdict = if (!answered)
        .declined
    else if (sample.complete)
        .{ .estimate = @min(bound, @max(tuples, 1)) }
    else if (widest_seen == 0 or sample.rows / widest_seen < KEY_SAMPLE_MIN_REPEATS)
        .few_repeats
    else if (!coversWidestRows(sample.rows, widest_seen, st.upper_rows, widest_ndv))
        .shallow
    else if (tuples > sample.rows / 2)
        .saturated
    else
        .{ .estimate = @min(bound, @max(widest_ndv *| tuples / widest_seen, tuples, widest_ndv)) };
    if (trace) traceKeySample(sample, widest_name, widest_ndv, widest_seen, st.upper_rows, bound, verdict, elapsed);
    return switch (verdict) {
        .estimate => |e| e,
        else => null,
    };
}

/// Whether a partial key sample saw, on average, at least
/// 1/`SAMPLED_GROUPS_MARGIN` of the rows of each widest-key value it met:
/// its rows per widest value seen against the input's rows per widest
/// value, its coverage. Coverage is what keeps the estimate from
/// undercounting. A sample of whole row groups or chunks holding a fraction
/// f of the input's rows meets each of the input's tuples with a chance of
/// at least about f, so its tuples divided by f overcount the input's. The
/// estimate divides them by the share of widest values the sample saw
/// instead, which is f over the coverage, so it counts at least the
/// coverage times the input's tuples; at a coverage of 1/2, the margin
/// still prices them all, however each value's partners are spread (one
/// dominant partner and a long tail of rare ones included). A key whose
/// values scatter over the input has a low coverage: the sample sees a few
/// rows of each value and misses its rare partners.
fn coversWidestRows(sample_rows: u64, widest_seen: u64, rows: u64, widest_ndv: u64) bool {
    return @as(u128, sample_rows) * widest_ndv * SAMPLED_GROUPS_MARGIN >= @as(u128, rows) * widest_seen;
}

const KeySampleVerdict = union(enum) {
    estimate: u64,
    declined,
    few_repeats,
    shallow,
    saturated,
};

fn traceKeySample(sample: exec.KeySample, widest: []const u8, widest_ndv: u64, widest_seen: u64, rows: u64, bound: u64, verdict: KeySampleVerdict, ticks: u64) void {
    const per_value_sample = @as(f64, @floatFromInt(sample.rows)) / @as(f64, @floatFromInt(@max(widest_seen, 1)));
    const per_value_input = @as(f64, @floatFromInt(rows)) / @as(f64, @floatFromInt(@max(widest_ndv, 1)));
    std.debug.print(
        "[keysample] source={s} keys={d} rows={d} complete={} tuples={d} widest={s} ndv={d} seen={d} coverage={d:.2} bound={d} took={d:.2} ms -> ",
        .{ sample.source, sample.keys.len, sample.rows, sample.complete, sample.tuple.estimate(), widest, widest_ndv, widest_seen, per_value_sample / per_value_input, bound, exec.prof.ticksToMs(@intCast(ticks)) },
    );
    switch (verdict) {
        .estimate => |e| std.debug.print("estimate={d}\n", .{e}),
        else => std.debug.print("product ({s})\n", .{@tagName(verdict)}),
    }
}

/// The groups the partitioned plan's hash cores size their tables for (issue
/// #464): the keys' NDV estimate, or a key sample's when there is one
/// (`sampledGroups`), or 0 without either, when the cores grow from empty.
/// Capped at the groups whose slots `headroom` holds, so an estimate the
/// budget can't hold never reserves past it.
fn presizeGroups(
    st: exec.PipelineStats,
    schema: []const types.Column,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    headroom: u64,
    sampled: ?u64,
) u64 {
    const groups = estimateGroups(st, schema, group_cols, sampled) orelse return 0;
    const state = groupState(st, schema, group_cols, aggs, null, sampled) orelse return 0;
    return @min(groups, headroom / (state.slot *| SLOTS_NUM / SLOTS_DEN));
}

/// The groups radix's table jumps to once it outgrows its first one (issue
/// #476): the keys' NDV estimate, or a key sample's when there is one
/// (`sampledGroups`), or 0 without either, when it doubles from a small
/// table. Capped at the groups whose table and cells `headroom` holds: g
/// groups take up to 8/3 g slots, and cells for 3/4 of those.
fn radixPresize(
    st: exec.PipelineStats,
    schema: []const types.Column,
    group_cols: []const []const u8,
    fp: exec.radix_aggregate.Footprint,
    headroom: u64,
    sampled: ?u64,
) u64 {
    const groups = estimateGroups(st, schema, group_cols, sampled) orelse return 0;
    return @min(groups, headroom / (fp.slot *| SLOTS_NUM / SLOTS_DEN +| fp.cell *| 2));
}

/// A hash aggregate's state. `groups` is the product of the keys' NDV bounds
/// capped at rows, or rows when any key's NDV is unknown, or
/// `SAMPLED_GROUPS_MARGIN` times a key sample's estimate when there is one
/// and it is smaller; under a bare LIMIT whose aggregates keep bounded state
/// the table stops at `emit_limit` groups plus an overflow group (the hash
/// plan is the only one that takes a LIMIT).
const GroupState = struct {
    groups: u64,
    /// The groups radix's table jumps to from its first one
    /// (`radixPresize`): a key sample's estimate, or `groups` without one.
    jump: u64,
    /// A table slot: its hash and group id, the key slice, and every
    /// aggregate's cell.
    slot: u64,
    /// The sort cores' bytes per group: the key and string payloads at
    /// their per-row widths, and the emitted row.
    group: u64,
    /// What a hash aggregate's arena copies per group: each key (a string
    /// key at its distinct width, `keyWidth`) and each string aggregate
    /// value and MAX_BY key (`keptValueWidth`).
    payload: u64,
    /// A group's emitted bytes the emit reserves exactly: fixed-width
    /// values, string offsets and validity, a lone string key's bytes.
    out_fixed: u64,
    /// A group's emitted string bytes appended into growing buffers: string
    /// aggregate values, and string keys beside other keys.
    out_strings: u64,
    /// The per-group value sets of DISTINCT aggregates and the values
    /// GROUP_CONCAT / PERCENTILE keep.
    sets: u64,
    /// What radix holds per slot, group and emitted row; null when it
    /// can't carry the GROUP BY, or under a bare LIMIT, which it leaves to
    /// the hash plan.
    radix: ?exec.radix_aggregate.Footprint,
    /// Whether `groups` comes from the keys' NDV, which radix sizes its
    /// table to, rather than the rows.
    estimated: bool,

    /// Radix's peak when its table also takes `batch_rows` (issue #476).
    /// It grows its table to hold a whole batch before inserting it, keeps
    /// a cell for every group the table holds
    /// (`radix_aggregate.cellCapacity`) and scratch for the batch. While a
    /// grow moves the groups, it also holds the table and cells it
    /// outgrew: its first table when it jumps from there to `jump`, else
    /// the half it doubled from. It frees the table and scratch, then
    /// emits every group into columns reserved to fit. Unbounded when radix
    /// can't carry the GROUP BY.
    fn bytes(self: GroupState, batch_rows: u64) u64 {
        const fp = self.radix orelse return std.math.maxInt(u64);
        const ra = exec.radix_aggregate;
        const slots = tableSlots(self.groups +| batch_rows);
        const first_groups: u64 = if (self.estimated) @min(self.jump, ra.INITIAL_GROUPS) else ra.UNESTIMATED_GROUPS;
        const first = tableSlots(first_groups);
        const target = if (self.estimated) tableSlots(self.jump) else first;
        const outgrown: u64 = if (slots <= first) 0 else if (slots <= target) first else slots / 2;
        const cells: u64 = ra.cellCapacity(slots);
        const drain = (slots +| outgrown) *| fp.slot +| (cells +| ra.cellCapacity(outgrown)) *| fp.cell +| batch_rows *| BATCH_SCRATCH_BYTES;
        const emit = cells *| fp.cell +| fp.output(self.groups);
        return @max(drain, emit);
    }

    /// A hash aggregate's state split over `tables` tables (the
    /// partitions' cores, or one), each inserting up to `batch_rows` rows
    /// at a time. Each table is the power of two its share of the groups
    /// plus a batch needs (an Aggregate grows to hold a whole batch before
    /// inserting it, and sizes its cells and key list to the table), with
    /// per-row scratch for the batch. `held` adds the arena's payload
    /// copies and the value sets; `output` is every group's emitted row.
    fn hashTables(self: GroupState, tables: u64, batch_rows: u64) HashState {
        const t = @max(tables, 1);
        const per_table = self.groups / t + @intFromBool(self.groups % t != 0);
        const slots = t *| tableSlots(per_table +| batch_rows) *| self.slot;
        const scratch = t *| batch_rows *| BATCH_SCRATCH_BYTES;
        const arena = self.groups *| self.payload *| ARENA_SLACK_NUM / ARENA_SLACK_DEN;
        const output = self.groups *| (self.out_fixed +| self.out_strings *| OUTPUT_GROWTH_NUM / OUTPUT_GROWTH_DEN);
        return .{ .held = slots +| scratch +| arena +| self.sets, .output = output };
    }
};

const HashState = struct {
    held: u64,
    output: u64,
};

/// `group_table.capacityFor`, saturating where the power of two would not
/// fit.
fn tableSlots(entries: u64) u64 {
    if (entries > std.math.maxInt(u64) / 4) return std.math.maxInt(u64);
    return group_table.capacityFor(entries);
}

fn groupState(
    st: exec.PipelineStats,
    schema: []const types.Column,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    emit_limit: ?u32,
    sampled: ?u64,
) ?GroupState {
    const rows = st.upper_rows;
    var slot: u64 = 16 + 16;
    var group: u64 = 0;
    var payload: u64 = 0;
    var out_fixed: u64 = 0;
    var out_strings: u64 = 0;
    for (group_cols) |gc| {
        const idx = types.findColumn(schema, gc) orelse return null;
        group += valueWidth(st, schema, idx) + columnRowBytes(st, schema, idx);
        const validity: u64 = @intFromBool(schema[idx].nullable);
        if (!schema[idx].type.isString()) {
            const width = exec.memory.estimateColumnBytes(schema[idx].type);
            payload += width;
            out_fixed += width + validity;
            continue;
        }
        const width = keyWidth(st, schema, idx);
        out_fixed += @sizeOf(u32) + validity;
        if (group_cols.len == 1) {
            payload += width;
            out_fixed += width;
        } else {
            payload += @sizeOf(u32) + width;
            out_strings += width;
        }
    }
    const estimate = estimateGroups(st, schema, group_cols, if (sampled) |s| s *| SAMPLED_GROUPS_MARGIN else null);
    const all_groups = estimate orelse @max(rows, 1);
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
                    const kept = keptValueWidth(st, schema, in_idx.?);
                    payload += kept;
                    out_fixed += @sizeOf(u32) + 1;
                    out_strings += kept;
                } else {
                    group += 16;
                    out_fixed += 16;
                }
                const string_key = if (key_t) |t| t.isString() else false;
                if (string_key) {
                    group += valueWidth(st, schema, key_idx.?);
                    payload += keptValueWidth(st, schema, key_idx.?);
                }
            },
            .count_distinct, .sum_distinct, .avg_distinct => {
                group += 16;
                out_fixed += 16;
                const i = in_idx orelse continue;
                const pairs = if (i < st.column_stats.len) switch (st.column_stats[i].ndv) {
                    .exact => |n| @min(rows, groups *| n),
                    .unknown => rows,
                } else rows;
                sets +|= pairs *| ((valueWidth(st, schema, i) + 16) * 4 / 3 * SET_GROWTH);
            },
            .group_concat, .percentile => {
                group += 16;
                out_fixed += 16;
                var width: u64 = if (in_idx) |i| valueWidth(st, schema, i) else 0;
                if (key_idx) |i| width += valueWidth(st, schema, i);
                sets +|= rows *| width;
            },
            else => {
                group += 16;
                out_fixed += 16;
            },
        }
    }
    return .{
        .groups = groups,
        .jump = @min(groups, sampled orelse groups),
        .slot = slot,
        .group = group,
        .payload = payload,
        .out_fixed = out_fixed,
        .out_strings = out_strings,
        .sets = sets,
        .radix = if (emit_limit == null) exec.radix_aggregate.footprint(schema, group_cols, aggs) else null,
        .estimated = estimate != null,
    };
}

fn traceInput(st: exec.PipelineStats, priced: exec.PipelineStats, schema: []const types.Column) void {
    std.debug.print("[gbroute]   input={d} MiB ({d} B/row):", .{ (st.upper_rows *| rowBytes(priced, schema)) >> 20, rowBytes(priced, schema) });
    for (schema, 0..) |col, i| {
        if (!col.type.isString()) continue;
        const measured = i < st.column_stats.len and st.column_stats[i].avg_width != null;
        const how = if (measured) "measured" else if (priced.column_stats[i].avg_width != null) "sampled" else "guessed";
        std.debug.print(" {s}={d} B {s}", .{ col.name, valueWidth(priced, schema, i), how });
        if (priced.column_stats[i].distinct_width) |w| std.debug.print(" (distinct {d} B)", .{w});
    }
    std.debug.print("\n", .{});
}

fn traceNeeds(rows: u64, needs: PlanNeeds, headroom: usize, partition_ok: bool) void {
    const mib = 1024 * 1024;
    const radix_ok = needs.admits(.radix, partition_ok);
    const radix_na = if (radix_ok) "" else "(n/a)";
    const partitioned_na = if (needs.admits(.partitioned, partition_ok)) "" else "(n/a)";
    const sort_cores_na = if (needs.admits(.partitioned_sort, partition_ok)) "" else "(n/a)";
    std.debug.print(
        "[gbroute] rows={d} headroom={d} MiB needs: radix={d}{s} partitioned={d}{s} partitioned_sort={d}{s} hash={d} sort={d} MiB\n",
        .{ rows, headroom / mib, if (radix_ok) needs.radix / mib else 0, radix_na, needs.partitioned / mib, partitioned_na, needs.partitioned_sort / mib, sort_cores_na, needs.hash / mib, needs.sort / mib },
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

    /// `VTable.sampleWidths`: the distinct widths the router wants for its
    /// keys, sampled at one stride across the chunks still held.
    pub fn sampleWidths(self: *RealizedInput, widths: []exec.SampledWidth) !void {
        const schema = self.source.outputSchema();
        if (widths.len != schema.len) return;
        const Sampler = storage.column.DistinctWidthSampler;
        const step = Sampler.stride(self.rows, Sampler.ROWS_PER_BUFFER);
        for (schema, widths, 0..) |col, *w, i| {
            if (!col.type.isString() or !w.wantsDistinct()) continue;
            var sampler: Sampler = .{};
            defer sampler.deinit(self.allocator);
            var next_row: usize = 0;
            for (self.owned.chunks[self.freed..]) |c| next_row = try sampler.addStrided(self.allocator, c.stores[i].view(), next_row, step);
            w.distinct = sampler.width();
        }
    }

    /// `VTable.sampleKeys` over the rows still held (`exec.sampleBuffer`).
    pub fn sampleKeys(self: *RealizedInput, cols: []const usize, sample: *exec.KeySample) !bool {
        const schema = self.source.outputSchema();
        for (cols) |c| if (c >= schema.len) return false;
        const views = try self.allocator.alloc(storage.ColumnView, cols.len);
        defer self.allocator.free(views);
        exec.sampleBuffer(sample, HeldChunks{ .chunks = self.owned.chunks[self.freed..], .cols = cols }, views);
        sample.complete = sample.complete and self.freed == 0;
        sample.source = "realized input";
        return true;
    }

    /// The chunks a realized input still holds, as `exec.sampleBuffer`
    /// reads them.
    const HeldChunks = struct {
        chunks: []const exec.OwnedChunk,
        cols: []const usize,

        pub fn len(self: HeldChunks) usize {
            return self.chunks.len;
        }
        pub fn rows(self: HeldChunks, i: usize) usize {
            return self.chunks[i].rows;
        }
        pub fn views(self: HeldChunks, i: usize, out: []storage.ColumnView) void {
            for (self.cols, out) |col, *v| v.* = self.chunks[i].stores[col].view();
        }
    };

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
/// `sampled` is a key sample's group estimate (`sampledGroups`), which the
/// table presizes to in place of the NDV product.
pub fn routeRadixGroupBy(
    upstream: Query,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    top_k: ?ir.Op.TopK,
    emit_limit: ?u32,
    sampled: ?u64,
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
    return radixPresized(upstream, group_cols, aggs, rtk, sampled);
}

/// A RadixAggregate over `upstream` sized by `radixPresize` against the
/// statement's headroom, or null when radix can't carry the GROUP BY;
/// consumes `upstream` only on success.
fn radixPresized(
    upstream: Query,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    top_k: ?exec.radix_aggregate.TopK,
    sampled: ?u64,
) !?Query {
    const schema = upstream.outputSchema();
    const headroom: u64 = if (upstream.accountant()) |a| a.headroom() else std.math.maxInt(u64);
    const presize = if (exec.radix_aggregate.footprint(schema, group_cols, aggs)) |fp|
        radixPresize(upstream.stats(), schema, group_cols, fp, headroom, sampled)
    else
        0;
    if (getenv_gr("THINDB_TRACE_GBROUTE") != null) std.debug.print("[gbroute]   radix presize={d} groups\n", .{presize});
    // create declines (cleanly, without consuming upstream) when the key won't
    // pack into ≤128 bits or an aggregate isn't fixed-state — fall through.
    return upstream.radixGroupBy(group_cols, aggs, top_k, presize) catch |e| switch (e) {
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
    var product: u64 = 1;
    var any_unknown = false;
    var from_row = true;
    for (group_cols) |gc| {
        const idx = types.findColumn(schema, gc).?;
        if (idx >= st.column_stats.len) {
            any_unknown = true;
            from_row = false;
            continue;
        }
        from_row = from_row and exec.fromRowOrigin(st, st.column_stats[idx]);
        switch (st.column_stats[idx].ndv) {
            .unknown => any_unknown = true,
            .exact => |nd| product *|= nd,
        }
    }
    const ceiling = exec.keyTupleBound(st, from_row);
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
    const needs = planNeeds(measured, &schema, &group_cols, &aggs, null, 0, 4, whole, null).?;
    const row_bytes = 24 + 105 + 8;
    const input = rows * row_bytes;
    // A slot holds the hash, group id, key slice and both aggregates' cells.
    // A hash aggregate's arena copies a group's key and MAX_BY value; its
    // row emits the key (reserved exactly, as the lone string key), the
    // value's offset and validity, and the count, and the value's bytes
    // into a growing buffer. The sort cores still price both at the
    // per-row widths, and the string key leaves radix out.
    const state = groupState(measured, &schema, &group_cols, &aggs, null, null).?;
    const cells = exec.aggregate_op.aggStateWidth(.max_by, schema[1].type, .bigint) + exec.aggregate_op.aggStateWidth(.count, null, null);
    try std.testing.expectEqual(16 + 16 + cells, state.slot);
    try std.testing.expectEqual((20 + 24) + (100 + 105) + 16, state.group);
    try std.testing.expectEqual(20 + 100, state.payload);
    try std.testing.expectEqual((4 + 20) + (4 + 1) + 16, state.out_fixed);
    try std.testing.expectEqual(100, state.out_strings);
    try std.testing.expectEqual(rows, state.groups);
    try std.testing.expectEqual(@as(?exec.radix_aggregate.Footprint, null), state.radix);
    try std.testing.expect(!needs.admits(.radix, true));
    // The hash plan's one table is the power of two its groups need under
    // the 0.75 load; it holds the arena's copies and then the emitted rows.
    const serial = state.hashTables(1, 0);
    const serial_slots = group_table.capacityFor(rows) * state.slot;
    try std.testing.expectEqual(serial_slots + rows * 120 * 4 / 3, serial.held);
    try std.testing.expectEqual(rows * (45 + 100 * 5 / 4), serial.output);
    try std.testing.expectEqual(serial.held + serial.output, needs.hash);
    // A streamed batch widens the tables and adds its scratch.
    const batched = planNeeds(measured, &schema, &group_cols, &aggs, null, 1000, 4, whole, null).?;
    const batched_serial = state.hashTables(1, 1000);
    try std.testing.expectEqual(batched_serial.held + batched_serial.output, batched.hash);
    try std.testing.expectEqual(serial.held + 1000 * BATCH_SCRATCH_BYTES, batched_serial.held);
    // Four partitions read windows of 64Ki rows, and each one's table takes
    // its window on top of its share of the groups. They absorb a round
    // beside their tables, then emit together.
    const windows = 4 * partitioned_aggregate.PARTITION_BATCH_ROWS;
    const cores = state.hashTables(4, partitioned_aggregate.PARTITION_BATCH_ROWS);
    try std.testing.expectEqual(4 * group_table.capacityFor(rows / 4 + partitioned_aggregate.PARTITION_BATCH_ROWS) * state.slot, cores.held - rows * 120 * 4 / 3 - windows * BATCH_SCRATCH_BYTES);
    const absorb = input + 4 * rows + 2 * windows * row_bytes + cores.held;
    const emit = cores.held + cores.output;
    try std.testing.expectEqual(absorb, needs.cores_absorb);
    try std.testing.expectEqual(emit, needs.cores_emit);
    try std.testing.expectEqual(@max(absorb, emit), needs.partitioned);
    try std.testing.expectEqual(input + input / 2 + 4 * rows, needs.sort);
    // Hash cores buffer a round of the input at a time: its bytes, plus the
    // batch that crosses it.
    const round_rows = 10_000_000 / row_bytes + 1000;
    const rounded = planNeeds(measured, &schema, &group_cols, &aggs, null, 1000, 4, 10_000_000, null).?;
    const rounded_absorb = round_rows * row_bytes + 4 * round_rows + 2 * windows * row_bytes + cores.held;
    try std.testing.expectEqual(@max(rounded_absorb, emit), rounded.partitioned);
    try std.testing.expectEqual(needs.sort, rounded.sort);
    try std.testing.expectEqual(needs.partitioned_sort, rounded.partitioned_sort);
    // Windows past the input's rows hold only its rows.
    const wide = planNeeds(measured, &schema, &group_cols, &aggs, null, 0, 16, whole, null).?;
    const wide_cores = state.hashTables(16, partitioned_aggregate.PARTITION_BATCH_ROWS);
    try std.testing.expectEqual(@max(input + 4 * rows + 2 * input + wide_cores.held, wide_cores.held + wide_cores.output), wide.partitioned);
    // An unknown key NDV prices a group per row, each copying out its key
    // and MAX_BY value and emitting a row: the hash state outweighs a sort
    // of the input.
    try std.testing.expect(needs.sort < needs.hash);
    try std.testing.expect(needs.hash > rows * (120 + 45 + 100));
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
    const heavy = planNeeds(measured, &schema, &group_cols, &heavy_aggs, null, 0, 4, whole, null).?;
    const heavy_state = groupState(measured, &schema, &group_cols, &heavy_aggs, null, null).?;
    const heavy_cores = heavy_state.hashTables(4, partitioned_aggregate.PARTITION_BATCH_ROWS);
    const heavy_hash_cores = @max(input + 4 * rows + 2 * windows * row_bytes + heavy_cores.held, heavy_cores.held + heavy_cores.output);
    try std.testing.expectEqual(2 * (input + 4 * rows) + heavy_state.groups * heavy_state.group * 3 / 2, heavy.partitioned_sort);
    try std.testing.expectEqual(@max(heavy_hash_cores, heavy.partitioned_sort), heavy.partitioned);

    // Over an input whose chunks are freed as the plan copies them, the
    // copying plans need what their copy and state add beyond the held
    // buffers, plus the chunk being copied; the streaming plans keep their
    // whole state on top of them. Hash cores emit once every chunk is
    // freed.
    const consumed = needs.consuming(input, 1000);
    try std.testing.expectEqual(needs.radix, consumed.radix);
    try std.testing.expectEqual(needs.partitioned - input + 1000, consumed.partitioned);
    try std.testing.expectEqual(needs.partitioned_sort - input + 1000, consumed.partitioned_sort);
    try std.testing.expectEqual(needs.hash, consumed.hash);
    try std.testing.expectEqual(input / 2 + 4 * rows + 1000, consumed.sort);
    try std.testing.expect(consumed.near_unique);
    const overheld = needs.consuming(needs.partitioned + needs.partitioned_sort + needs.sort, 1000);
    try std.testing.expectEqual(@as(u64, 1000), overheld.cores_emit);
    try std.testing.expectEqual(absorb - input + 1000, overheld.partitioned);
    try std.testing.expectEqual(@as(u64, 1000), overheld.partitioned_sort);
    try std.testing.expectEqual(@as(u64, 1000), overheld.sort);
    // Hash cores copy a round at a time, so the held input past a round
    // still sits beside their tables.
    const rounded_consumed = rounded.consuming(input, 1000);
    try std.testing.expectEqual(@max(rounded_absorb - round_rows * row_bytes + 1000, emit - input + 1000), rounded_consumed.partitioned);
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
    const few_state = groupState(few, &schema, &group_cols, &aggs, null, null).?;
    try std.testing.expectEqual(@as(u64, 1000), few_state.groups);
    const few_needs = planNeeds(few, &schema, &group_cols, &aggs, null, 0, 4, whole, null).?;
    const few_serial = few_state.hashTables(1, 0);
    try std.testing.expectEqual(few_serial.held + few_serial.output, few_needs.hash);
    try std.testing.expectEqual(group_table.capacityFor(1000) * few_state.slot + 1000 * 120 * 4 / 3, few_serial.held);
    try std.testing.expectEqual(needs.sort, few_needs.sort);
    try std.testing.expect(!few_needs.near_unique);
    try std.testing.expect(!few_needs.admits(.partitioned_sort, true));

    // A bare LIMIT over bounded aggregate state stops the hash table at the
    // limit plus an overflow group; MAX_BY's value is not bounded state.
    const count_only = [_]ir.AggSpec{.{ .func = .count, .col = null, .as = "c" }};
    try std.testing.expectEqual(rows, groupState(measured, &schema, &group_cols, &count_only, null, null).?.groups);
    try std.testing.expectEqual(@as(u64, 11), groupState(measured, &schema, &group_cols, &count_only, 10, null).?.groups);
    try std.testing.expectEqual(needs.hash, planNeeds(measured, &schema, &group_cols, &aggs, 10, 0, 4, whole, null).?.hash);

    // Unmeasured strings take the 32-byte guess.
    const guessed = planNeeds(.{ .upper_rows = rows }, &schema, &group_cols, &aggs, null, 0, 4, whole, null).?;
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
    const small_sets = planNeeds(small_t, &schema, &group_cols, &distinct_aggs, null, 0, 4, whole, null).?.hash;
    const all_sets = planNeeds(all_t, &schema, &group_cols, &distinct_aggs, null, 0, 4, whole, null).?.hash;
    try std.testing.expectEqual((rows - 10_000) * ((8 + 16) * 4 / 3 * SET_GROWTH), all_sets - small_sets);

    const missing = [_]ir.AggSpec{.{ .func = .max, .col = "nope", .as = "m" }};
    try std.testing.expectEqual(@as(?PlanNeeds, null), planNeeds(measured, &schema, &group_cols, &missing, null, 0, 4, whole, null));
}

test "a hash aggregate prices the strings a group keeps at their distinct width" {
    const schema = [_]types.Column{
        .{ .name = "k", .type = .string },
        .{ .name = "j", .type = .string, .nullable = true },
        .{ .name = "n", .type = .bigint },
    };
    const aggs = [_]ir.AggSpec{.{ .func = .count, .col = null, .as = "c" }};
    const rows: u64 = 1_000_000;
    const whole = std.math.maxInt(u64);
    // Most rows hold a short key, so the row mean (9 B) sits far below the
    // bytes a group holds (65 B).
    const row_mean = exec.PipelineStats{ .upper_rows = rows, .column_stats = &.{
        .{ .ndv = .{ .exact = 50_000 }, .avg_width = 9 },
        .{ .ndv = .{ .exact = 4 }, .avg_width = 30 },
        .{ .ndv = .{ .exact = 2 } },
    } };
    const sampled = exec.PipelineStats{ .upper_rows = rows, .column_stats = &.{
        .{ .ndv = .{ .exact = 50_000 }, .avg_width = 9, .distinct_width = 65 },
        .{ .ndv = .{ .exact = 4 }, .avg_width = 30, .distinct_width = 12 },
        .{ .ndv = .{ .exact = 2 } },
    } };
    // A lone string key: the arena copies it once per group, and the emit
    // reserves its bytes exactly.
    const one = [_][]const u8{"k"};
    const plain = groupState(row_mean, &schema, &one, &aggs, null, null).?;
    const keyed = groupState(sampled, &schema, &one, &aggs, null, null).?;
    try std.testing.expectEqual(9, plain.payload);
    try std.testing.expectEqual(65, keyed.payload);
    try std.testing.expectEqual(4 + 65 + 16, keyed.out_fixed);
    try std.testing.expectEqual(0, keyed.out_strings);
    try std.testing.expectEqual(plain.group, keyed.group);
    const plain_needs = planNeeds(row_mean, &schema, &one, &aggs, null, 0, 4, whole, null).?;
    const keyed_needs = planNeeds(sampled, &schema, &one, &aggs, null, 0, 4, whole, null).?;
    try std.testing.expectEqual(plain_needs.partitioned_sort, keyed_needs.partitioned_sort);
    try std.testing.expectEqual(50_000 * (65 - 9) * 4 / 3 + 50_000 * (65 - 9), keyed_needs.hash - plain_needs.hash);
    // Beside another key, each string key also frames its length, and the
    // emit appends its bytes into a growing buffer.
    const three = [_][]const u8{ "k", "j", "n" };
    const multi = groupState(sampled, &schema, &three, &aggs, null, null).?;
    try std.testing.expectEqual((4 + 65) + (4 + 12) + 8, multi.payload);
    try std.testing.expectEqual(4 + (4 + 1) + (8 + 0) + 16, multi.out_fixed);
    try std.testing.expectEqual(65 + 12, multi.out_strings);
    // A MIN or MAX keeps one of its group's values, priced at the wider of
    // its column's row and distinct widths: k's distinct width, j's row
    // width. The sort cores still price them per row.
    const by_n = [_][]const u8{"n"};
    const kept_aggs = [_]ir.AggSpec{
        .{ .func = .min, .col = "k", .as = "lo" },
        .{ .func = .max, .col = "j", .as = "hi" },
    };
    const kept = groupState(sampled, &schema, &by_n, &kept_aggs, null, null).?;
    try std.testing.expectEqual(8 + 65 + 30, kept.payload);
    try std.testing.expectEqual(65 + 30, kept.out_strings);
    try std.testing.expectEqual(groupState(row_mean, &schema, &by_n, &kept_aggs, null, null).?.group, kept.group);
}

test "radix prices the table, cells and output it allocates (issue #476)" {
    const schema = [_]types.Column{
        .{ .name = "k", .type = .bigint },
        .{ .name = "e", .type = .smallint },
        .{ .name = "v", .type = .int, .nullable = true },
        .{ .name = "n", .type = .bigint, .nullable = true },
        .{ .name = "t", .type = .string },
    };
    const keys = [_][]const u8{ "k", "e" };
    const aggs = [_]ir.AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "v", .as = "s" },
        .{ .func = .max, .col = "v", .as = "m" },
    };
    const ra = exec.radix_aggregate;
    // Its 80 key bits take the 16-byte slot. A group's cells hold its packed
    // key, the count, the nullable SUM's i128 and seen flag, and MAX's value
    // and present flag. It emits both keys, the count and SUM as BIGINT and
    // MAX as INT, with validity for SUM and MAX.
    const fp = ra.footprint(&schema, &keys, &aggs).?;
    try std.testing.expectEqual(16, fp.slot);
    try std.testing.expectEqual(16 + 8 * (1 + 3 + 2), fp.cell);
    try std.testing.expectEqual(8 + 2 + 8 + 8 + 4, fp.out);
    try std.testing.expectEqual(2, fp.nullable_outs);
    // A nullable key, keys past 128 bits, a string key or a string MAX
    // leave radix out.
    try std.testing.expectEqual(@as(?ra.Footprint, null), ra.footprint(&schema, &.{"n"}, &aggs));
    try std.testing.expectEqual(@as(?ra.Footprint, null), ra.footprint(&schema, &.{ "k", "k", "e" }, &aggs));
    try std.testing.expectEqual(@as(?ra.Footprint, null), ra.footprint(&schema, &.{"t"}, &aggs));
    try std.testing.expectEqual(@as(?ra.Footprint, null), ra.footprint(&schema, &keys, &.{.{ .func = .max, .col = "t", .as = "m" }}));

    const rows: u64 = 10_000_000;
    const batch: u64 = 64 * 1024;
    const whole = std.math.maxInt(u64);
    // An estimate of 1M groups: the first table takes 64Ki groups, then
    // jumps to the 2Mi slots the estimate and a batch need, holding the
    // first table and its cells while their groups move. The emit holds the
    // cells beside the output.
    const est = exec.PipelineStats{ .upper_rows = rows, .column_stats = &.{
        .{ .ndv = .{ .exact = 1000 } },
        .{ .ndv = .{ .exact = 1000 } },
        .{},
        .{},
        .{},
    } };
    const state = groupState(est, &schema, &keys, &aggs, null, null).?;
    try std.testing.expect(state.estimated);
    const slots: u64 = 2 * 1024 * 1024;
    try std.testing.expectEqual(slots, group_table.capacityFor(1_000_000 + batch));
    const first: u64 = group_table.capacityFor(ra.INITIAL_GROUPS);
    const drain = (slots + first) * 16 + (slots / 4 * 3 + first / 4 * 3) * fp.cell + batch * BATCH_SCRATCH_BYTES;
    const emit = slots / 4 * 3 * fp.cell + 1_000_000 * fp.out + 2 * ((1_000_000 + 7) / 8);
    try std.testing.expectEqual(@max(drain, emit), state.bytes(batch));
    try std.testing.expectEqual(state.bytes(batch), planNeeds(est, &schema, &keys, &aggs, null, batch, 4, whole, null).?.radix);
    // A few groups stay in the first table.
    const few = groupState(.{ .upper_rows = 1000, .column_stats = est.column_stats }, &schema, &keys, &aggs, null, null).?;
    try std.testing.expectEqual(2048 * 16 + 1536 * fp.cell, few.bytes(0));
    // Without an estimate it prices a group per row, and doubles from a
    // small table, holding the half it doubled from beside the last one.
    const blind = groupState(.{ .upper_rows = rows }, &schema, &keys, &aggs, null, null).?;
    try std.testing.expect(!blind.estimated);
    const blind_slots = group_table.capacityFor(rows + batch);
    try std.testing.expectEqual(
        (blind_slots + blind_slots / 2) * 16 + (blind_slots / 4 * 3 + blind_slots / 8 * 3) * fp.cell + batch * BATCH_SCRATCH_BYTES,
        blind.bytes(batch),
    );
    // Under a bare LIMIT it leaves the GROUP BY to the hash plan.
    try std.testing.expectEqual(@as(?ra.Footprint, null), groupState(est, &schema, &keys, &aggs, 10, null).?.radix);
    try std.testing.expect(!planNeeds(est, &schema, &keys, &aggs, 10, batch, 4, whole, null).?.admits(.radix, true));
}

/// Batches of `batch_rows` rows of a BIGINT key and value, whose stats
/// bound the rows at `upper_rows` and give the key's NDV.
const TestIntSource = struct {
    keys: []const i64,
    values: []const i64,
    batch_rows: usize,
    col_stats: [2]exec.ColStat,
    upper_rows: u64,
    account: *exec.memory.MemoryAccountant,
    views: [2]storage.ColumnView = undefined,
    pos: usize = 0,

    const schema = [_]types.Column{ .{ .name = "k", .type = .bigint }, .{ .name = "v", .type = .bigint } };

    pub fn next(self: *TestIntSource) !?exec.Batch {
        if (self.pos == self.keys.len) return null;
        const start = self.pos;
        self.pos = @min(start + self.batch_rows, self.keys.len);
        self.views[0] = .{ .data = .{ .bigint = self.keys[start..self.pos] } };
        self.views[1] = .{ .data = .{ .bigint = self.values[start..self.pos] } };
        return .{ .schema = &schema, .values = &self.views, .row_count = self.pos - start };
    }
    pub fn deinit(_: *TestIntSource) void {}
    pub fn outputSchema(_: *TestIntSource) []const types.Column {
        return &schema;
    }
    pub fn addPrune(_: *TestIntSource, _: exec.Predicate) !void {}
    pub fn stats(self: *TestIntSource) exec.PipelineStats {
        return .{ .upper_rows = self.upper_rows, .column_stats = &self.col_stats };
    }
    pub fn accountant(self: *TestIntSource) ?*exec.memory.MemoryAccountant {
        return self.account;
    }
    pub fn explain(_: *TestIntSource, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) !void {
        try exec.explainLine(out, alloc, depth, "TestIntSource");
    }
};

const RadixRow = struct { k: i64, c: i64, s: i64, m: i64 };

fn radixRowLess(_: void, x: RadixRow, y: RadixRow) bool {
    return x.k < y.k;
}

const RadixRun = struct {
    /// Its rows, by key.
    rows: []RadixRow,
    /// What the router prices radix at.
    price: u64,
    /// The most radix held beyond what its creation left charged.
    peak: usize,
};

/// COUNT, SUM and MAX by key over `keys`/`values` with radix sized as the
/// router sizes it.
fn testRadixRun(a: Allocator, keys: []const i64, values: []const i64, batch_rows: usize, ndv: ?u32, upper_rows: u64) !RadixRun {
    const account = try testAccountant(a, 1 << 40);
    defer account.releaseOwner(a);
    var src = TestIntSource{
        .keys = keys,
        .values = values,
        .batch_rows = batch_rows,
        .col_stats = .{ .{ .ndv = if (ndv) |n| .{ .exact = n } else .unknown }, .{} },
        .upper_rows = upper_rows,
        .account = account,
    };
    const group_cols = [_][]const u8{"k"};
    const aggs = [_]ir.AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "v", .as = "s" },
        .{ .func = .max, .col = "v", .as = "m" },
    };
    const up = exec.makeQuery(a, &src);
    const price = planNeeds(up.stats(), up.outputSchema(), &group_cols, &aggs, null, batch_rows, 1, std.math.maxInt(u64), null).?.radix;
    var q = (try radixPresized(up, &group_cols, &aggs, null, null)).?;
    defer q.deinit();
    const held = account.current_bytes;
    var rows: std.ArrayList(RadixRow) = .empty;
    errdefer rows.deinit(a);
    while (try q.next()) |b| {
        const v = b.values;
        for (0..b.row_count) |r| try rows.append(a, .{ .k = v[0].data.bigint[r], .c = v[1].data.bigint[r], .s = v[2].data.bigint[r], .m = v[3].data.bigint[r] });
    }
    std.mem.sort(RadixRow, rows.items, {}, radixRowLess);
    return .{ .rows = try rows.toOwnedSlice(a), .price = price, .peak = account.peak_bytes - held };
}

test "radix holds what the router prices it at, sized to the estimate (issue #476)" {
    const a = std.testing.allocator;
    const n = 200_000;
    const keys = try a.alloc(i64, n);
    defer a.free(keys);
    const values = try a.alloc(i64, n);
    defer a.free(values);
    for (keys, values, 0..) |*k, *v, i| {
        k.* = @intCast(i * 7919 % 1_000_003);
        v.* = @intCast(i % 1000);
    }
    // An exact estimate: the first batch outgrows the first table, which
    // jumps straight to the estimate.
    const exact = try testRadixRun(a, keys, values, n / 2, n, n);
    defer a.free(exact.rows);
    try std.testing.expectEqual(@as(usize, n), exact.rows.len);
    for (exact.rows) |row| {
        try std.testing.expectEqual(@as(i64, 1), row.c);
        try std.testing.expectEqual(row.s, row.m);
    }
    try std.testing.expect(exact.peak <= exact.price);
    try std.testing.expect(exact.price - exact.peak <= exact.peak / 4);
    // Without one it doubles from a small table.
    const blind = try testRadixRun(a, keys, values, n / 2, null, n);
    defer a.free(blind.rows);
    try std.testing.expect(blind.peak <= blind.price);
    try std.testing.expectEqualSlices(RadixRow, exact.rows, blind.rows);
    // A filter's bound ten times the groups its rows hold sizes the first
    // table and its cells for that bound, as priced.
    const over = try testRadixRun(a, keys[0..5000], values[0..5000], 5000, 50_000, 50_000);
    defer a.free(over.rows);
    try std.testing.expectEqual(@as(usize, 5000), over.rows.len);
    try std.testing.expectEqual(over.price, over.peak);
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
        needs = inputNeeds(&up, up.stats(), &group_cols, &aggs, null, partitioned_aggregate.partitionCount(partition_dop), null).?;
        held = account.current_bytes;
        blind_hash_ok = groupKeysCardUnderLimit(up.stats(), up.outputSchema(), &group_cols, &aggs, budget);
        if (blind) break :routed try partitioned_aggregate.PartitionedAggregate.create(tracked, worker, up, &group_cols, &aggs, partition_dop, .auto, 0);
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

    // An eighth below what the partitioned plan took, the router takes the
    // next plan whose need fits. The budget-blind route partitions there
    // while its hash-table admission holds, which since the partitions'
    // tables left their arenas (issue #464) this input's peak can sit below.
    // Whether that plan overruns moves with how its partitions interleave
    // and how the platform allocator grows buffers, so it is held only to
    // failing cleanly or matching.
    const gap_budget = roomy.held + (roomy.peak - roomy.held) * 7 / 8;
    const gap = try testRouteRealized(a, gap_budget, 4, false);
    defer testFreeLines(a, gap.lines);
    if (gap.blind_hash_ok) {
        if (testRouteRealized(a, gap_budget, 4, true)) |forced| {
            defer testFreeLines(a, forced.lines);
            try std.testing.expectEqual(roomy.lines.len, forced.lines.len);
            for (roomy.lines, forced.lines) |r, f| try std.testing.expectEqualStrings(r, f);
        } else |err| try std.testing.expectEqual(error.MemoryBudgetExceeded, err);
    }
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

/// Two BIGINT keys whose stats bound `upper_rows` rows and give each key's
/// NDV, and whose key sample is the rows it holds: all of its input when
/// `complete`, else a part of it.
const TestKeySource = struct {
    a: []const i64,
    b: []const i64,
    complete: bool,
    col_stats: [2]exec.ColStat,
    upper_rows: u64,
    samples: usize = 0,

    const schema = [_]types.Column{ .{ .name = "a", .type = .bigint }, .{ .name = "b", .type = .bigint } };

    pub fn next(_: *TestKeySource) !?exec.Batch {
        return null;
    }
    pub fn deinit(_: *TestKeySource) void {}
    pub fn outputSchema(_: *TestKeySource) []const types.Column {
        return &schema;
    }
    pub fn addPrune(_: *TestKeySource, _: exec.Predicate) !void {}
    pub fn stats(self: *TestKeySource) exec.PipelineStats {
        return .{ .upper_rows = self.upper_rows, .column_stats = &self.col_stats };
    }
    pub fn accountant(_: *TestKeySource) ?*exec.memory.MemoryAccountant {
        return null;
    }
    pub fn explain(_: *TestKeySource, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) !void {
        try exec.explainLine(out, alloc, depth, "TestKeySource");
    }
    pub fn sampleKeys(self: *TestKeySource, cols: []const usize, sample: *exec.KeySample) !bool {
        self.samples += 1;
        var views: [2]storage.ColumnView = undefined;
        for (cols, views[0..cols.len]) |c, *v| v.* = .{ .data = .{ .bigint = if (c == 0) self.a else self.b } };
        sample.addRows(views[0..cols.len], 0, self.a.len);
        sample.complete = sample.complete and self.complete;
        return true;
    }
};

/// What `sampledGroups` estimates over `a`/`b`, with `upper_rows` rows and
/// the keys' NDVs as their stats, and how many times it sampled.
fn testSampledGroups(alloc: Allocator, a: []const i64, b: []const i64, complete: bool, ndv_a: u32, ndv_b: u32, upper_rows: u64, keys: []const []const u8) !struct { groups: ?u64, samples: usize } {
    var src = TestKeySource{
        .a = a,
        .b = b,
        .complete = complete,
        .col_stats = .{ .{ .ndv = .{ .exact = ndv_a } }, .{ .ndv = .{ .exact = ndv_b } } },
        .upper_rows = upper_rows,
    };
    var q = exec.makeQuery(alloc, &src);
    const groups = try sampledGroups(alloc, &q, q.stats(), keys);
    return .{ .groups = groups, .samples = src.samples };
}

test "a key sample estimates the groups of correlated keys, and declines when it can't tell (issue #478)" {
    const alloc = std.testing.allocator;
    const sample_rows = 160_000;
    const a = try alloc.alloc(i64, sample_rows);
    defer alloc.free(a);
    const b = try alloc.alloc(i64, sample_rows);
    defer alloc.free(b);
    const keys = [_][]const u8{ "a", "b" };
    // 100K values of `a` with 10 rows each, clustered, each with one `b` of
    // 1000; the sample holds the first 16K of them. The NDV product caps
    // at the million rows.
    for (a, b, 0..) |*x, *y, i| {
        x.* = @intCast(i / 10);
        y.* = @mod(x.* * 7919, 1000);
    }
    const one_partner = try testSampledGroups(alloc, a, b, false, 100_000, 1000, 1_000_000, &keys);
    try std.testing.expect(one_partner.groups.? >= 100_000 and one_partner.groups.? <= 115_000);

    // Three partners each: three times the groups.
    for (a, b, 0..) |x, *y, i| y.* = @mod(x * 7919 + @as(i64, @intCast(i % 3)), 1000);
    const three = try testSampledGroups(alloc, a, b, false, 100_000, 1000, 1_000_000, &keys);
    try std.testing.expect(three.groups.? >= 255_000 and three.groups.? <= 345_000);

    // Each row its own partner: the sample is saturated with new tuples, so
    // it can't tell how many partners it hasn't met.
    for (b, 0..) |*y, i| y.* = @intCast(i % 10);
    const saturated = try testSampledGroups(alloc, a, b, false, 100_000, 1000, 1_000_000, &keys);
    try std.testing.expectEqual(@as(?u64, null), saturated.groups);
    try std.testing.expectEqual(@as(usize, 1), saturated.samples);

    // The same rows as the whole input: their tuples, counted outright.
    const whole = try testSampledGroups(alloc, a, b, true, 16_000, 10, sample_rows, &keys);
    try std.testing.expect(whole.groups.? >= 150_000 and whole.groups.? <= sample_rows);

    // A key whose rows are scattered over the input: the sample sees each
    // value about once, too few times to count its partners.
    for (a, b, 0..) |*x, *y, i| {
        x.* = @intCast(i);
        y.* = @mod(x.* * 7919, 1000);
    }
    const scattered = try testSampledGroups(alloc, a, b, false, 1_000_000, 1000, 10_000_000, &keys);
    try std.testing.expectEqual(@as(?u64, null), scattered.groups);
    try std.testing.expectEqual(@as(usize, 1), scattered.samples);

    // 10K values of `a` with 100 rows each, nine in ten of them on one
    // dominant `b` and the rest on rare ones of 5K more: about 11 partners
    // each, 110K groups in the million rows.
    const skewed_b = struct {
        fn of(x: i64, i: usize) i64 {
            const rare = 1000 + @mod(@as(i64, @intCast(i)) * 7919, 4999);
            return if (i / 7 % 10 == 0) rare else @mod(x * 7919, 1000);
        }
    };
    // Scattered, the sample sees 16 of each value's 100 rows: enough repeats
    // and few enough tuples to pass for under three partners each, but it
    // saw too little of each value to have met its rare partners.
    for (a, b, 0..) |*x, *y, i| {
        x.* = @intCast(i % 10_000);
        y.* = skewed_b.of(x.*, i);
    }
    const skewed_scattered = try testSampledGroups(alloc, a, b, false, 10_000, 6000, 1_000_000, &keys);
    try std.testing.expectEqual(@as(?u64, null), skewed_scattered.groups);
    try std.testing.expectEqual(@as(usize, 1), skewed_scattered.samples);
    // Clustered, the sample sees all of the rows of the values it meets, and
    // their rare partners with them.
    for (a, b, 0..) |*x, *y, i| {
        x.* = @intCast(i / 100);
        y.* = skewed_b.of(x.*, i);
    }
    const skewed_clustered = try testSampledGroups(alloc, a, b, false, 10_000, 6000, 1_000_000, &keys);
    try std.testing.expect(skewed_clustered.groups.? >= 100_000 and skewed_clustered.groups.? <= 120_000);

    // Nothing to win, so no sample: one key, a product within twice the
    // widest key's NDV, or one under radix's first table.
    const one_key = try testSampledGroups(alloc, a, b, false, 100_000, 1000, 1_000_000, keys[0..1]);
    try std.testing.expectEqual(@as(?u64, null), one_key.groups);
    try std.testing.expectEqual(@as(usize, 0), one_key.samples);
    const narrow = try testSampledGroups(alloc, a, b, false, 100_000, 2, 1_000_000, &keys);
    try std.testing.expectEqual(@as(usize, 0), narrow.samples);
    const small = try testSampledGroups(alloc, a, b, false, 1000, 60, 1_000_000, &keys);
    try std.testing.expectEqual(@as(usize, 0), small.samples);
}

test "the router prices twice a key sample's groups and presizes radix to them (issue #478)" {
    const schema = [_]types.Column{ .{ .name = "a", .type = .bigint }, .{ .name = "b", .type = .bigint } };
    const keys = [_][]const u8{ "a", "b" };
    const aggs = [_]ir.AggSpec{.{ .func = .count, .col = null, .as = "c" }};
    const col_stats = [_]exec.ColStat{ .{ .ndv = .{ .exact = 100_000 } }, .{ .ndv = .{ .exact = 1000 } } };
    const st = exec.PipelineStats{ .upper_rows = 1_000_000, .column_stats = &col_stats };
    const product = groupState(st, &schema, &keys, &aggs, null, null).?;
    try std.testing.expectEqual(@as(u64, 1_000_000), product.groups);
    const sampled = groupState(st, &schema, &keys, &aggs, null, 100_000).?;
    try std.testing.expectEqual(@as(u64, 200_000), sampled.groups);
    try std.testing.expectEqual(@as(u64, 100_000), sampled.jump);
    // Never past the product.
    try std.testing.expectEqual(@as(u64, 1_000_000), groupState(st, &schema, &keys, &aggs, null, 900_000).?.groups);
    const fp = sampled.radix.?;
    try std.testing.expectEqual(@as(u64, 100_000), radixPresize(st, &schema, &keys, fp, std.math.maxInt(u64), 100_000));
    try std.testing.expectEqual(@as(u64, 1_000_000), radixPresize(st, &schema, &keys, fp, std.math.maxInt(u64), null));
    try std.testing.expect(sampled.bytes(SCAN_BATCH_ROWS) < product.bytes(SCAN_BATCH_ROWS));
}
