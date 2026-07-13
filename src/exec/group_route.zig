//! GROUP BY strategy routing over an arbitrary upstream Query — the
//! sorted-stream / radix / hash trio plus the two-phase join-partial combine.
//! Engine-neutral: depends only on exec operators and pipeline stats, and is
//! shared by the staged CTE/join/window block compiler (net/cte_stages.zig)
//! and the legacy dispatchers until those are deleted. Strategy:
//!   - input already sorted on the group keys → streaming aggregate
//!   - proven-over-budget or unknown cardinality → sort, then stream
//!   - high-cardinality native-int keys → radix-partitioned aggregate
//!   - proven-small key space → hash aggregate (groupByTopK)

const std = @import("std");

const getenv_gr = @extern(*const fn (name: [*:0]const u8) callconv(.c) ?[*:0]const u8, .{ .name = "getenv", .library_name = "c" });
const Allocator = std.mem.Allocator;
const exec = @import("exec.zig");
const Query = exec.Query;
const ir = @import("../ir/ir.zig");
const types = @import("../types.zig");

/// The standard GROUP BY routing trio — sorted-stream / radix / hash — shared
/// by the embedded dispatcher and the staged CTE mat-block compiler. Follows
/// `routeStreamGroupBy`'s ownership contract: `upstream` is reassigned in
/// place when the route orders the keys, and the caller's errdefer owns
/// whatever it points at on error; ownership moves into the returned Query on
/// success.
pub fn routeGroupBy(
    allocator: Allocator,
    upstream: *Query,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    top_k: ?ir.Op.TopK,
    emit_limit: ?u32,
    budget: usize,
) !Query {
    if (try routeStreamGroupBy(allocator, upstream, group_cols, aggs, budget)) |q| return q;
    if (try routeRadixGroupBy(upstream.*, group_cols, aggs, top_k, emit_limit)) |q| return q;
    return upstream.groupByTopK(group_cols, aggs, top_k, emit_limit);
}

// NEGATIVE RESULT (2026-07-13, plan-selection campaign): a dop-aware variant
// of routeGroupBy for the no-stages compile fallback was built and reverted
// twice. The target was customer_monthly_totals (keys=4/SUM over 3.4M
// filtered rows, serial hash Aggregate 1.2s inside a 14s query).
// (a) PartitionedAggregate arm: agg dropped 1.31s -> 0.70s but the consumer
//     stage refunded it exactly (+0.8s exec, +0.45s teardown) — pagg's
//     partition-concat emission shuffles the input-clustered order the
//     downstream window implicitly exploited. Wall-neutral.
// (b) Parallel sort + streamGroupBy arm: key-sorted output, but the 3.4M-row
//     sort cost MORE than the serial agg saved (wall +1s) — the window keys
//     don't ride the group-key order here.
// The serial hash aggregate's insertion-order emission is load-bearing for
// this shape. Any parallel replacement must preserve (or make the consumer
// exploit) that order — see task #173.

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
        .sort => {
            const specs = try allocator.alloc(exec.SortSpec, group_cols.len);
            defer allocator.free(specs);
            for (group_cols, specs) |gc, *s| s.* = .{ .col = gc, .desc = false };
            upstream.* = try upstream.orderBy(specs);
            return try upstream.streamGroupBy(group_cols, aggs);
        },
        .auto => {
            if (groupKeysSortedPrefix(st.sort_state, group_cols)) {
                return try upstream.streamGroupBy(group_cols, aggs);
            }
            if (!groupKeysCardUnderLimit(st, upstream.outputSchema(), group_cols, aggs, budget)) {
                const specs = try allocator.alloc(exec.SortSpec, group_cols.len);
                defer allocator.free(specs);
                for (group_cols, specs) |gc, *s| s.* = .{ .col = gc, .desc = false };
                upstream.* = try upstream.orderBy(specs);
                return try upstream.streamGroupBy(group_cols, aggs);
            }
            return null;
        },
    }
}

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
/// MAX_BY via a hidden max_by_key partial). A GLOBAL
/// (no-key) aggregate additionally excludes MIN/MAX: an empty chunk's
/// global partial emits a zero row (the SUM-over-empty dialect), which
/// combines safely for COUNT/SUM but would poison MIN/MAX. Grouped partials
/// emit nothing for empty chunks, so all four are safe there. Cardinality
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
    for (aggs) |a| switch (a.func) {
        .count, .sum => {},
        // any_value combines like min/max: a representative of per-chunk
        // representatives is still a representative. Grouped only — like
        // MIN/MAX, a global partial over an empty chunk would emit a NULL
        // row that could win the combine over real values.
        //
        // max_by combines via a hidden max_by_key twin appended to the
        // partial list: each partial row carries (value-at-chunk-max-ord,
        // that pair's ord), and the combine is max_by(value, hidden ord).
        // The twin shares max_by's skip-if-EITHER-NULL pair semantics — a
        // plain MAX(ord) would count NULL-value rows and could carry an ord
        // higher than its partial's value, poisoning the combine.
        .min, .max, .any_value, .max_by => if (group_cols.len == 0) return null,
        else => {
            if (trace_pa) std.debug.print("[pagg-route]   decline agg func={s}\n", .{@tagName(a.func)});
            return null;
        },
    };

    // Partial spec list: the originals, plus one hidden max_by_key twin per
    // max_by so its winning ord rides along for the combine.
    var n_hidden: usize = 0;
    for (aggs) |a| {
        if (a.func == .max_by) n_hidden += 1;
    }
    const part_aggs: []const ir.AggSpec = if (n_hidden == 0) aggs else blk: {
        const up_schema = upstream.outputSchema();
        const buf = try combine_arena.alloc(ir.AggSpec, aggs.len + n_hidden);
        @memcpy(buf[0..aggs.len], aggs);
        var j: usize = aggs.len;
        for (aggs) |a| {
            if (a.func != .max_by) continue;
            const ord_name = a.arg2_col orelse return null;
            const ord_idx = types.findColumn(up_schema, ord_name) orelse return null;
            buf[j] = .{
                .func = .max_by_key,
                .col = a.col,
                .arg2_col = ord_name,
                .as = try std.fmt.allocPrint(combine_arena, "__mb_ord__{s}", .{a.as}),
                .out_type_override = up_schema[ord_idx].type,
            };
            j += 1;
        }
        break :blk buf;
    };

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

    // Combine specs over the partials, read by `.as`, type-forced to the
    // partial output type (COUNT's bigint must not be widened by SUM).
    // Hidden max_by_key columns sit past aggs.len in the partial schema —
    // combine inputs only, never re-emitted.
    const part_schema = upstream.outputSchema();
    const combine = try combine_arena.alloc(ir.AggSpec, aggs.len);
    var hj: usize = aggs.len;
    for (aggs, 0..) |a, i| {
        combine[i] = .{
            .func = switch (a.func) {
                .count, .sum => .sum,
                .min => .min,
                .max => .max,
                .any_value => .any_value,
                .max_by => .max_by,
                else => unreachable,
            },
            .col = a.as,
            .as = a.as,
            .out_type_override = part_schema[group_cols.len + i].type,
        };
        if (a.func == .max_by) {
            combine[i].arg2_col = part_aggs[hj].as;
            hj += 1;
        }
    }
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
        raw += exec.aggregate_op.aggStateWidth(a.func, in_t);
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
