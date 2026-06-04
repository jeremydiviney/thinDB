//! Radix-partitioned aggregate — the high-cardinality GROUP BY path.
//!
//! See `thindb-radix-aggregate-plan` (memory) for the full design. The single
//! giant hash table the generic `Aggregate` builds for a high-card GROUP BY
//! dwarfs the L3, so every probe/scatter is a DRAM miss. This operator instead
//! materializes the rows, **2-pass radix-partitions** them by key hash so each
//! partition's table + state fits cache, then aggregates each partition with a
//! **compact, fused** per-group state.
//!
//! Phase 1 (this file, so far): the compact fixed-state core — a per-aggregate
//! `{kind, offset}` layout over a flat `[]u64` per-group record, with per-agg
//! scatter (dispatch hoisted out of the row loop) and a canonical finalize.
//! Gated to fixed-width-state aggregates: COUNT / SUM / AVG / MIN / MAX
//! (numeric, ≤64-bit) / STDDEV / VAR (Welford). The radix partitioning, the
//! operator vtable, and routing land in later phases.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const agg = @import("aggregate.zig");
const AggSpec = agg.AggSpec;
const AggFunc = agg.AggFunc;

const exec = @import("exec.zig");
const Batch = exec.Batch;
const Query = exec.Query;
const makeQuery = exec.makeQuery;
const Error = exec.Error;
const predicate = exec.predicate;
const PipelineStats = exec.PipelineStats;
const memory = exec.memory;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;
const gt = @import("group_table.zig");
const prof = @import("../util/prof.zig");
const transform = @import("../engine/transform.zig");
const ParallelScan = @import("parallel_scan.zig").ParallelScan;

const PREFETCH_DIST: usize = 12;

/// How one aggregate's accumulator lives inside the compact per-group record.
/// Each occupies a whole number of u64 words at `off` (word index), so the
/// record is naturally 8-byte aligned and i128/f64 are read/written via
/// `@bitCast` — no unaligned access. Zero-initialized state is the correct
/// initial accumulator for every kind (count 0, sums 0, avg {0,0}, MIN/MAX
/// present-flag 0 = "no value seen", Welford {0,0,0}).
pub const CompactKind = enum {
    count, // 1 word: u64 running count
    sum_int, // 2 words: i128 running sum (integer-family inputs)
    sum_float, // 1 word: f64 running sum (float/double inputs)
    avg, // 2 words: f64 sum, u64 count
    min_int, // 2 words: i64 value, u64 present-flag
    max_int, // 2 words: i64 value, u64 present-flag
    min_float, // 2 words: f64 value, u64 present-flag
    max_float, // 2 words: f64 value, u64 present-flag
    welford, // 3 words: f64 mean, f64 m2, u64 count

    fn words(self: CompactKind) usize {
        return switch (self) {
            .count, .sum_float => 1,
            .sum_int, .avg, .min_int, .max_int, .min_float, .max_float => 2,
            .welford => 3,
        };
    }
};

pub const CompactAgg = struct {
    kind: CompactKind,
    /// Word offset of this aggregate's slot within the per-group record.
    off: usize,
    /// Input column index in the upstream schema; null only for COUNT(*).
    col_idx: ?usize,
    /// Original aggregate function — disambiguates the four Welford finalizes
    /// (var/stddev × pop/samp). Unused for the other kinds.
    func: AggFunc,
};

/// The compact per-group state layout for a set of aggregates. `words` is the
/// per-group stride (group `g` occupies `state[g*words .. g*words+words]`).
pub const CompactLayout = struct {
    aggs: []CompactAgg,
    words: usize,

    pub fn deinit(self: CompactLayout, allocator: Allocator) void {
        allocator.free(self.aggs);
    }
};

fn minMaxIntEligible(t: Type) bool {
    return switch (t) {
        .int, .date, .bigint, .datetime, .boolean, .tinyint, .smallint, .decimal64 => true,
        else => false,
    };
}

fn welfordEligible(t: Type) bool {
    return t.isInteger() or t.isDecimal() or t == .boolean or t == .float or t == .double;
}

/// Plan the compact layout for `aggs`, or return null if any aggregate is not
/// compact-eligible (variable-state, or a fixed-state shape this core doesn't
/// cover — string/large MIN/MAX), in which case the caller falls back to the
/// generic `Aggregate`. `agg_col_idx` is the upstream column index per aggregate
/// (null for COUNT(*)); `up_schema` supplies input types.
pub fn planCompact(
    allocator: Allocator,
    aggs: []const AggSpec,
    agg_col_idx: []const ?usize,
    up_schema: []const types.Column,
) !?CompactLayout {
    const out = try allocator.alloc(CompactAgg, aggs.len);
    errdefer allocator.free(out);
    var w: usize = 0;
    for (aggs, agg_col_idx, out) |a, idx, *dst| {
        // Null is not an error, so `errdefer` won't fire — free explicitly when
        // declining so the partial allocation doesn't leak.
        const decline = struct {
            fn f(al: Allocator, buf: []CompactAgg) ?CompactLayout {
                al.free(buf);
                return null;
            }
        }.f;
        const in_t: ?Type = if (idx) |i| up_schema[i].type else null;
        const kind: CompactKind = switch (a.func) {
            .count => .count,
            .sum => if (in_t.?.isFloat()) .sum_float else .sum_int,
            .avg => .avg,
            .min, .max => blk: {
                const t = in_t.?;
                if (minMaxIntEligible(t)) break :blk if (a.func == .min) .min_int else .max_int;
                if (t == .float or t == .double) break :blk if (a.func == .min) .min_float else .max_float;
                return decline(allocator, out); // string / largeint / decimal128 MIN/MAX
            },
            .stddev_pop, .stddev_samp, .var_pop, .var_samp => if (welfordEligible(in_t.?)) .welford else return decline(allocator, out),
            // count_distinct / percentile / group_concat → variable state.
            else => return decline(allocator, out),
        };
        dst.* = .{ .kind = kind, .off = w, .col_idx = idx, .func = a.func };
        w += kind.words();
    }
    return .{ .aggs = out, .words = w };
}

/// Accumulate one batch into the compact `state`, given each row's resolved
/// group id in `gids`. One pass per aggregate so the per-column ValueView type
/// switch is hoisted out of the row loop. `state[g*layout.words ..]` must
/// already be zeroed for every gid present (the caller zeroes a group's slot
/// when it is first created).
pub fn scatter(layout: CompactLayout, state: []u64, gids: []const u32, batch: Batch) void {
    const stride = layout.words;
    for (layout.aggs) |ca| {
        const base = ca.off;
        switch (ca.kind) {
            .count => {
                if (ca.col_idx) |idx| {
                    const view = batch.values[idx];
                    if (view.nulls == null) {
                        for (gids) |g| state[@as(usize, g) * stride + base] += 1;
                    } else {
                        for (gids, 0..) |g, r| {
                            if (view.isValid(r)) state[@as(usize, g) * stride + base] += 1;
                        }
                    }
                } else {
                    for (gids) |g| state[@as(usize, g) * stride + base] += 1;
                }
            },
            .sum_int => scatterSumInt(state, stride, base, gids, batch.values[ca.col_idx.?]),
            .sum_float => scatterSumFloat(state, stride, base, gids, batch.values[ca.col_idx.?]),
            .avg => scatterAvg(state, stride, base, gids, batch.values[ca.col_idx.?]),
            .min_int => scatterMinMaxInt(state, stride, base, gids, batch.values[ca.col_idx.?], true),
            .max_int => scatterMinMaxInt(state, stride, base, gids, batch.values[ca.col_idx.?], false),
            .min_float => scatterMinMaxFloat(state, stride, base, gids, batch.values[ca.col_idx.?], true),
            .max_float => scatterMinMaxFloat(state, stride, base, gids, batch.values[ca.col_idx.?], false),
            .welford => scatterWelford(state, stride, base, gids, batch.values[ca.col_idx.?]),
        }
    }
}

inline fn addI128(state: []u64, slot: usize, v: i128) void {
    const cur: i128 = @bitCast([2]u64{ state[slot], state[slot + 1] });
    const nv: [2]u64 = @bitCast(cur + v);
    state[slot] = nv[0];
    state[slot + 1] = nv[1];
}

fn scatterSumInt(state: []u64, stride: usize, base: usize, gids: []const u32, view: ColumnView) void {
    const has_nulls = view.nulls != null;
    switch (view.data) {
        inline .int, .smallint, .tinyint, .boolean, .bigint, .decimal64, .largeint, .decimal128 => |sl| {
            if (has_nulls) {
                for (gids, 0..) |g, r| {
                    if (!view.isValid(r)) continue;
                    addI128(state, @as(usize, g) * stride + base, sl[r]);
                }
            } else {
                for (gids, 0..) |g, r| addI128(state, @as(usize, g) * stride + base, sl[r]);
            }
        },
        else => unreachable,
    }
}

fn scatterSumFloat(state: []u64, stride: usize, base: usize, gids: []const u32, view: ColumnView) void {
    const has_nulls = view.nulls != null;
    switch (view.data) {
        inline .float, .double => |sl| {
            for (gids, 0..) |g, r| {
                if (has_nulls and !view.isValid(r)) continue;
                const slot = @as(usize, g) * stride + base;
                var cur: f64 = @bitCast(state[slot]);
                cur += sl[r];
                state[slot] = @bitCast(cur);
            }
        },
        else => unreachable,
    }
}

fn scatterAvg(state: []u64, stride: usize, base: usize, gids: []const u32, view: ColumnView) void {
    const has_nulls = view.nulls != null;
    switch (view.data) {
        inline .int, .smallint, .tinyint, .boolean, .bigint, .decimal64 => |sl| {
            for (gids, 0..) |g, r| {
                if (has_nulls and !view.isValid(r)) continue;
                const slot = @as(usize, g) * stride + base;
                var sum: f64 = @bitCast(state[slot]);
                sum += @floatFromInt(sl[r]);
                state[slot] = @bitCast(sum);
                state[slot + 1] += 1;
            }
        },
        inline .float, .double => |sl| {
            for (gids, 0..) |g, r| {
                if (has_nulls and !view.isValid(r)) continue;
                const slot = @as(usize, g) * stride + base;
                var sum: f64 = @bitCast(state[slot]);
                sum += sl[r];
                state[slot] = @bitCast(sum);
                state[slot + 1] += 1;
            }
        },
        else => unreachable,
    }
}

/// MIN/MAX over i64-accumulator int families. Slot: [value:i64, present:u64].
/// Mirrors the generic operator's `scatterMinMax` `?i64` semantics.
fn scatterMinMaxInt(state: []u64, stride: usize, base: usize, gids: []const u32, view: ColumnView, comptime is_min: bool) void {
    const has_nulls = view.nulls != null;
    switch (view.data) {
        inline .int, .date, .bigint, .datetime, .boolean, .tinyint, .smallint, .decimal64 => |sl| {
            for (gids, 0..) |g, r| {
                if (has_nulls and !view.isValid(r)) continue;
                const iv: i64 = sl[r];
                const slot = @as(usize, g) * stride + base;
                if (state[slot + 1] == 0) {
                    state[slot] = @bitCast(iv);
                    state[slot + 1] = 1;
                } else {
                    const cur: i64 = @bitCast(state[slot]);
                    if (if (is_min) iv < cur else iv > cur) state[slot] = @bitCast(iv);
                }
            }
        },
        else => unreachable,
    }
}

/// MIN/MAX over float/double. Slot: [value:f64, present:u64].
fn scatterMinMaxFloat(state: []u64, stride: usize, base: usize, gids: []const u32, view: ColumnView, comptime is_min: bool) void {
    const has_nulls = view.nulls != null;
    switch (view.data) {
        inline .float, .double => |sl| {
            for (gids, 0..) |g, r| {
                if (has_nulls and !view.isValid(r)) continue;
                const fv: f64 = sl[r];
                const slot = @as(usize, g) * stride + base;
                if (state[slot + 1] == 0) {
                    state[slot] = @bitCast(fv);
                    state[slot + 1] = 1;
                } else {
                    const cur: f64 = @bitCast(state[slot]);
                    if (if (is_min) fv < cur else fv > cur) state[slot] = @bitCast(fv);
                }
            }
        },
        else => unreachable,
    }
}

/// One Welford online step on the group's slot [mean:f64, m2:f64, count:u64].
/// Bit-identical to the generic operator's `welfordStep`.
inline fn welfordSlot(state: []u64, slot: usize, x: f64) void {
    var mean: f64 = @bitCast(state[slot]);
    var m2: f64 = @bitCast(state[slot + 1]);
    const count = state[slot + 2] + 1;
    const n: f64 = @floatFromInt(count);
    const delta = x - mean;
    mean += delta / n;
    const delta2 = x - mean;
    m2 += delta * delta2;
    state[slot] = @bitCast(mean);
    state[slot + 1] = @bitCast(m2);
    state[slot + 2] = count;
}

fn scatterWelford(state: []u64, stride: usize, base: usize, gids: []const u32, view: ColumnView) void {
    const has_nulls = view.nulls != null;
    switch (view.data) {
        inline .int, .smallint, .tinyint, .boolean, .bigint, .decimal64, .largeint, .decimal128 => |sl| {
            for (gids, 0..) |g, r| {
                if (has_nulls and !view.isValid(r)) continue;
                welfordSlot(state, @as(usize, g) * stride + base, @floatFromInt(sl[r]));
            }
        },
        inline .float, .double => |sl| {
            for (gids, 0..) |g, r| {
                if (has_nulls and !view.isValid(r)) continue;
                welfordSlot(state, @as(usize, g) * stride + base, sl[r]);
            }
        },
        else => unreachable,
    }
}

/// Canonical finalized value of one aggregate for group `g`. The output-type
/// coercion (i128 SUM → bigint with overflow check, MIN narrowing, etc.) lives
/// in the emit phase, which mirrors `aggregate.appendAccToColumn`; this returns
/// the raw accumulator result for correctness testing and as that phase's input.
pub const Finalized = union(enum) {
    int: u64, // COUNT
    sum_int: i128, // SUM over integers
    signed: i64, // MIN/MAX over integers (empty → 0)
    float: f64, // SUM over floats, AVG, MIN/MAX over floats, STDDEV/VAR
};

pub fn finalize(layout: CompactLayout, state: []const u64, g: usize, ai: usize) Finalized {
    const ca = layout.aggs[ai];
    const slot = g * layout.words + ca.off;
    return switch (ca.kind) {
        .count => .{ .int = state[slot] },
        .sum_int => .{ .sum_int = @bitCast([2]u64{ state[slot], state[slot + 1] }) },
        .sum_float => .{ .float = @bitCast(state[slot]) },
        .avg => blk: {
            const sum: f64 = @bitCast(state[slot]);
            const cnt = state[slot + 1];
            break :blk .{ .float = if (cnt == 0) 0.0 else sum / @as(f64, @floatFromInt(cnt)) };
        },
        // Empty MIN/MAX (no non-null value seen) → 0 / 0.0, matching the generic
        // operator's default (it doesn't surface aggregate-result NULLs yet).
        .min_int, .max_int => .{ .signed = if (state[slot + 1] == 0) 0 else @bitCast(state[slot]) },
        .min_float, .max_float => .{ .float = if (state[slot + 1] == 0) 0.0 else @bitCast(state[slot]) },
        .welford => blk: {
            const m2: f64 = @bitCast(state[slot + 1]);
            const cnt = state[slot + 2];
            var variance: f64 = 0.0;
            if (cnt != 0) variance = switch (ca.func) {
                .var_pop, .stddev_pop => m2 / @as(f64, @floatFromInt(cnt)),
                .var_samp, .stddev_samp => if (cnt < 2) 0.0 else m2 / @as(f64, @floatFromInt(cnt - 1)),
                else => unreachable,
            };
            const out: f64 = if (ca.func == .stddev_pop or ca.func == .stddev_samp) @sqrt(variance) else variance;
            break :blk .{ .float = out };
        },
    };
}

/// Map a compact `Finalized` value to one output column, applying the output-
/// type coercion — mirrors `aggregate.appendAccToColumn` for the fixed-state
/// aggregates.
fn appendFinalized(allocator: Allocator, f: Finalized, col: *ColumnStore, out_type: Type) !void {
    switch (f) {
        .int => |c| try col.data.bigint.append(allocator, @intCast(c)), // COUNT
        .sum_int => |total| switch (out_type) {
            .largeint => try col.data.largeint.append(allocator, total),
            .decimal128 => try col.data.decimal128.append(allocator, total),
            else => {
                if (total > std.math.maxInt(i64) or total < std.math.minInt(i64)) return Error.ArithmeticOverflow;
                try col.data.bigint.append(allocator, @intCast(total));
            },
        },
        .signed => |v| switch (out_type) { // MIN/MAX over integers → input type
            .int => try col.data.int.append(allocator, @intCast(v)),
            .bigint => try col.data.bigint.append(allocator, v),
            .boolean => try col.data.boolean.append(allocator, @intCast(v)),
            .date => try col.data.date.append(allocator, @intCast(v)),
            .datetime => try col.data.datetime.append(allocator, v),
            .tinyint => try col.data.tinyint.append(allocator, @intCast(v)),
            .smallint => try col.data.smallint.append(allocator, @intCast(v)),
            .decimal64 => try col.data.decimal64.append(allocator, v),
            else => unreachable,
        },
        .float => |v| switch (out_type) { // SUM(float), AVG, MIN/MAX(float), STDDEV/VAR
            .double => try col.data.double.append(allocator, v),
            .float => try col.data.float.append(allocator, @floatCast(v)),
            else => unreachable,
        },
    }
}

/// A top-k hint for the operator: emit only the `k` groups most-preferred by a
/// single ORDER BY key that resolves to an aggregate output (e.g. ORDER BY
/// COUNT(*) DESC LIMIT k). The router passes this when the GROUP BY flows
/// directly into `ORDER BY <agg> [DESC] LIMIT k`; otherwise the operator emits
/// every group. Multi-key or group-column order keys fall back to full emit.
pub const TopK = struct { k: u32, col: []const u8, desc: bool };

const ResolvedTopK = struct { k: u32, agg_idx: usize, desc: bool };

/// A comparable order value pulled from one group's finalized order aggregate.
/// All groups share the active variant (set by the order aggregate's kind).
const OrderVal = union(enum) {
    i: i128,
    f: f64,
    /// True if `self` is preferred over `other` under direction `desc`.
    fn preferred(self: OrderVal, other: OrderVal, desc: bool) bool {
        return switch (self) {
            .i => |v| if (desc) v > other.i else v < other.i,
            .f => |v| if (desc) v > other.f else v < other.f,
        };
    }
};

fn orderValOf(layout: CompactLayout, state: []const u64, g: usize, ai: usize) OrderVal {
    return switch (finalize(layout, state, g, ai)) {
        .int => |c| .{ .i = @intCast(c) },
        .sum_int => |v| .{ .i = v },
        .signed => |v| .{ .i = v },
        .float => |v| .{ .f = v },
    };
}

/// Min-heap on *preference*: the root is the least-preferred kept group, so a
/// new candidate need only beat the root to earn a slot. `sel`/`ov` are kept in
/// lockstep (gid + its order value). Sifts the node at `start` down over `[0,n)`.
fn topkSiftDown(sel: []usize, ov: []OrderVal, start: usize, n: usize, desc: bool) void {
    var i = start;
    while (true) {
        const l = 2 * i + 1;
        const r = 2 * i + 2;
        var least = i;
        // ov[least].preferred(ov[c]) ⟺ child c is *less* preferred ⟹ bubble it up.
        if (l < n and ov[least].preferred(ov[l], desc)) least = l;
        if (r < n and ov[least].preferred(ov[r], desc)) least = r;
        if (least == i) break;
        std.mem.swap(usize, &sel[i], &sel[least]);
        std.mem.swap(OrderVal, &ov[i], &ov[least]);
        i = least;
    }
}

fn topkBuildHeap(sel: []usize, ov: []OrderVal, desc: bool) void {
    const n = sel.len;
    if (n < 2) return;
    var i = n / 2;
    while (i > 0) {
        i -= 1;
        topkSiftDown(sel, ov, i, n, desc);
    }
}

/// Radix-partitioned aggregate operator (Phase 2b). Step (i): a single pre-sized
/// compact-state table — no partitioning yet (added next). Drains the upstream,
/// packs each row's int key, probes one group table, scatters via the compact
/// core, and emits group columns (decoded from the packed key) + the finalized
/// aggregates. With a `TopK` hint it emits only the k most-preferred groups
/// (ORDER BY <agg> LIMIT k). Eligibility (int key ≤128 bits, fixed-state aggs)
/// is the router's responsibility; `create` errors if the layouts don't apply.
pub const RadixAggregate = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    upstream: Query,
    group_col_indices: []usize,
    agg_col_indices: []?usize,
    aggs: []const AggSpec,
    int_layout: agg.IntKeyLayout,
    compact: CompactLayout,
    output_schema: []types.Column,
    output_columns: []ColumnStore,
    views: []ColumnView,
    cap_groups: usize,
    top_k: ?ResolvedTopK,
    done: bool = false,

    pub fn create(allocator: Allocator, upstream: Query, group_cols: []const []const u8, aggs: []const AggSpec, top_k: ?TopK) !Query {
        if (aggs.len == 0) return Error.AggregateNoSpecs;
        const up_schema = upstream.outputSchema();

        const gci = try allocator.alloc(usize, group_cols.len);
        errdefer allocator.free(gci);
        for (group_cols, gci) |name, *dst| dst.* = types.findColumn(up_schema, name) orelse return Error.ColumnNotFound;

        const aci = try allocator.alloc(?usize, aggs.len);
        errdefer allocator.free(aci);
        for (aggs, aci) |a, *dst| dst.* = if (a.col) |name| (types.findColumn(up_schema, name) orelse return Error.ColumnNotFound) else null;

        const layout = (try agg.planIntKey(allocator, gci, up_schema, null, &.{})) orelse return Error.UnsupportedOperatorForType;
        errdefer layout.deinit(allocator);

        const compact = (try planCompact(allocator, aggs, aci, up_schema)) orelse return Error.AggregateUnsupportedType;
        errdefer compact.deinit(allocator);

        const out_schema = try allocator.alloc(types.Column, group_cols.len + aggs.len);
        errdefer allocator.free(out_schema);
        for (gci, 0..) |ci, i| out_schema[i] = up_schema[ci];
        for (aggs, aci, 0..) |a, idx, i| {
            const in_t: ?Type = if (idx) |x| up_schema[x].type else null;
            out_schema[group_cols.len + i] = .{ .name = a.as, .type = try agg.aggOutputTypeFor(a, in_t) };
        }

        const out_cols = try allocator.alloc(ColumnStore, out_schema.len);
        errdefer allocator.free(out_cols);
        var inited: usize = 0;
        errdefer for (out_cols[0..inited]) |*c| c.deinit(allocator);
        for (out_schema, 0..) |col, i| {
            out_cols[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }

        const views = try allocator.alloc(ColumnView, out_schema.len);
        errdefer allocator.free(views);

        // Group-count estimate from upstream stats → presize (grow covers a miss).
        const st = upstream.stats();
        var est: u64 = 1;
        var known = true;
        for (gci) |ci| {
            if (ci >= st.column_stats.len) {
                known = false;
                break;
            }
            switch (st.column_stats[ci].ndv) {
                .exact => |nd| est *|= nd,
                .unknown => {
                    known = false;
                    break;
                },
            }
        }
        const cap_groups: usize = if (known and est > 0) @intCast(@min(est, @max(st.upper_rows, 1))) else 4096;

        // Resolve the top-k hint: its order column must be one of the aggregate
        // outputs (a fixed-state numeric value). A group-column or unknown order
        // key leaves it unresolved → full emit (the router shouldn't route those).
        var resolved_tk: ?ResolvedTopK = null;
        if (top_k) |tk| {
            for (aggs, 0..) |a, ai| {
                if (types.columnNameEql(a.as, tk.col)) {
                    resolved_tk = .{ .k = tk.k, .agg_idx = ai, .desc = tk.desc };
                    break;
                }
            }
        }

        const self = try allocator.create(RadixAggregate);
        self.* = .{
            .allocator = allocator,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .upstream = upstream,
            .group_col_indices = gci,
            .agg_col_indices = aci,
            .aggs = aggs,
            .int_layout = layout,
            .compact = compact,
            .output_schema = out_schema,
            .output_columns = out_cols,
            .views = views,
            .cap_groups = @max(cap_groups, 256),
            .top_k = resolved_tk,
        };
        return makeQuery(allocator, self);
    }

    fn drainTier(self: *RadixAggregate, comptime Table: type, prof_on: bool) !void {
        const aa = self.arena.allocator();
        const words = self.compact.words;
        const layout = self.int_layout;
        // Adaptive sizing (mirrors aggregate.zig #295): start modest and grow
        // straight to the provable ceiling on the first overflow. `cap_groups`
        // is a min(∏NDV, upper_rows) UPPER bound — a selective filter over a
        // high-table-wide-NDV key (Q40/Q41) over-estimates it wildly, so presizing
        // to it would allocate+fault a multi-million-slot table for a few-K-group
        // result. Starting small keeps those cache-resident; true high-card (Q32)
        // overflows once and jumps to the ceiling.
        const ADAPTIVE_INITIAL: usize = 1 << 16;
        const init_cap = @min(self.cap_groups, ADAPTIVE_INITIAL);
        var table = try Table.init(aa, init_cap);
        if (self.cap_groups > init_cap) table.grow_target = gt.capacityFor(self.cap_groups);
        var gstate: std.ArrayListUnmanaged(u64) = .empty;
        var gkeys: std.ArrayListUnmanaged(u128) = .empty;
        try gstate.ensureTotalCapacity(aa, init_cap * words);
        try gkeys.ensureTotalCapacity(aa, init_cap);
        var kb: std.ArrayListUnmanaged(u128) = .empty;
        var hb: std.ArrayListUnmanaged(u64) = .empty;
        var gidbuf: std.ArrayListUnmanaged(u32) = .empty;
        var n_groups: u32 = 0;
        var rows: usize = 0;
        const t0 = if (prof_on) prof.nowTicks() else 0;
        var scan_ticks: i64 = 0;
        var group_ticks: i64 = 0;

        while (true) {
            const scan_t0 = if (prof_on) prof.nowTicks() else 0;
            const maybe = try self.upstream.next();
            if (prof_on) scan_ticks += prof.nowTicks() - scan_t0;
            const batch = maybe orelse break;
            const n = batch.row_count;
            if (n == 0) continue;

            const group_t0 = if (prof_on) prof.nowTicks() else 0;
            rows += n;
            if (table.needsGrow(n)) {
                try table.grow(aa, n);
                try gstate.ensureTotalCapacity(aa, table.slots.len * words);
                try gkeys.ensureTotalCapacity(aa, table.slots.len);
            }
            try gstate.ensureUnusedCapacity(aa, n * words);
            try gkeys.ensureUnusedCapacity(aa, n);
            try kb.ensureTotalCapacity(aa, n);
            try hb.ensureTotalCapacity(aa, n);
            try gidbuf.ensureTotalCapacity(aa, n);

            kb.clearRetainingCapacity();
            kb.appendNTimesAssumeCapacity(0, n);
            for (self.group_col_indices, layout.fields) |ci, f| agg.orKeyColumn(kb.items[0..n], batch, ci, f);

            hb.clearRetainingCapacity();
            for (0..n) |j| hb.appendAssumeCapacity(Table.hashKey(kb.items[j]));

            gidbuf.clearRetainingCapacity();
            for (0..n) |j| {
                if (j + PREFETCH_DIST < n) @prefetch(table.slotAddr(table.bucketOf(hb.items[j + PREFETCH_DIST])), .{ .rw = .write, .locality = 1 });
                const p = table.getOrPut(hb.items[j], kb.items[j]);
                const g = if (p.found) p.gid else blk: {
                    table.commit(p.slot, kb.items[j], n_groups);
                    gkeys.appendAssumeCapacity(kb.items[j]);
                    const base = @as(usize, n_groups) * words;
                    gstate.items.len = base + words;
                    @memset(gstate.items[base .. base + words], 0);
                    const ng = n_groups;
                    n_groups += 1;
                    break :blk ng;
                };
                gidbuf.appendAssumeCapacity(g);
            }
            scatter(self.compact, gstate.items, gidbuf.items[0..n], batch);
            if (prof_on) group_ticks += prof.nowTicks() - group_t0;
        }

        const t1 = if (prof_on) prof.nowTicks() else 0;
        try self.emitGroups(gstate.items, gkeys.items[0..n_groups]);
        if (prof_on) {
            const t2 = prof.nowTicks();
            std.debug.print("[radix-prof] drain={d:.1}ms emit={d:.1}ms rows={d} groups={d}\n", .{
                prof.ticksToMs(t1 - t0),
                prof.ticksToMs(t2 - t1),
                rows,
                n_groups,
            });
            std.debug.print("[group-pipeline] scan_filter={d:.1}ms group_by={d:.1}ms order_topn_emit={d:.1}ms rows={d} groups={d} dop=1\n", .{
                prof.ticksToMs(scan_ticks),
                prof.ticksToMs(group_ticks),
                prof.ticksToMs(t2 - t1),
                rows,
                n_groups,
            });
        }
    }

    /// Emit group `g`'s row into the allocator-owned output columns: decode its
    /// key into the group columns + finalize each aggregate.
    fn emitOne(self: *RadixAggregate, gstate: []const u64, g: usize, key: u128) !void {
        try agg.appendIntGroupKey(self.allocator, key, self.int_layout, self.output_columns[0..self.group_col_indices.len]);
        for (self.compact.aggs, 0..) |_, ai| {
            const oi = self.group_col_indices.len + ai;
            try appendFinalized(self.allocator, finalize(self.compact, gstate, g, ai), &self.output_columns[oi], self.output_schema[oi].type);
        }
    }

    /// Emit every group, or — with a resolved top-k hint — only the k most-
    /// preferred by the order aggregate, selected with a bounded min-heap
    /// (O(n_groups·log k), vs the naive O(n_groups·k) scan-for-worst). The
    /// downstream ORDER BY re-sorts the small kept set, so emit order doesn't
    /// matter. `gkeys` is indexed by gid 0..n_groups.
    fn emitGroups(self: *RadixAggregate, gstate: []const u64, gkeys: []const u128) !void {
        const tk = self.top_k orelse {
            for (0..gkeys.len) |g| try self.emitOne(gstate, g, gkeys[g]);
            return;
        };
        const k = @min(@as(usize, tk.k), gkeys.len);
        if (k == 0) return;
        const aa = self.arena.allocator();
        const sel = try aa.alloc(usize, k);
        const ov = try aa.alloc(OrderVal, k);
        var len: usize = 0;
        for (0..gkeys.len) |g| {
            const v = orderValOf(self.compact, gstate, g, tk.agg_idx);
            if (len < k) {
                sel[len] = g;
                ov[len] = v;
                len += 1;
                if (len == k) topkBuildHeap(sel, ov, tk.desc);
            } else if (v.preferred(ov[0], tk.desc)) {
                // More preferred than the worst kept (the heap root) — evict it.
                sel[0] = g;
                ov[0] = v;
                topkSiftDown(sel, ov, 0, k, tk.desc);
            }
        }
        for (0..len) |i| try self.emitOne(gstate, sel[i], gkeys[sel[i]]);
    }

    pub fn next(self: *RadixAggregate) !?Batch {
        if (self.done) return null;
        self.done = true;
        const prof_on = getenv("THINDB_LEASE_PROF") != null;
        switch (self.int_layout.tier) {
            .bits32 => try self.drainTier(gt.IntKeyTable(32), prof_on),
            .bits96 => try self.drainTier(gt.IntKeyTable(96), prof_on),
            .bits128 => try self.drainTier(gt.IntKeyTable(128), prof_on),
        }
        _ = self.arena.reset(.free_all); // group table/state no longer needed
        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        return Batch{ .schema = self.output_schema, .values = self.views, .row_count = self.output_columns[0].rowCount() };
    }

    pub fn deinit(self: *RadixAggregate) void {
        self.upstream.deinit();
        self.arena.deinit();
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        self.compact.deinit(self.allocator);
        self.int_layout.deinit(self.allocator);
        self.allocator.free(self.agg_col_indices);
        self.allocator.free(self.group_col_indices);
        self.allocator.destroy(self);
    }

    pub fn outputSchema(self: *RadixAggregate) []const types.Column {
        return self.output_schema;
    }
    pub fn addPrune(self: *RadixAggregate, pred: predicate.Predicate) !void {
        return self.upstream.addPrune(pred);
    }
    pub fn stats(self: *RadixAggregate) PipelineStats {
        return .{ .upper_rows = self.upstream.stats().upper_rows };
    }
    pub fn accountant(self: *RadixAggregate) ?*memory.MemoryAccountant {
        return self.upstream.accountant();
    }
    pub fn explain(self: *RadixAggregate, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "RadixAggregate (compact, single-table)");
        try self.upstream.explain(out, allocator, depth + 1);
    }
};

// ===========================================================================
// RadixLeaseAggregate — the PARALLEL high-card GROUP BY path.
//
// The single shared table the generic Aggregate builds is contention-bound under
// DOP>1 (12 cores ping-pong the hot lines). Microbench `cht_experiments.zig`
// proved the fix: partition rows into chunky buckets by key-hash, then let worker
// threads LEASE whole buckets and aggregate each one EXCLUSIVELY. No write
// contention (one owner per bucket), no merge (a key lives in exactly one
// bucket → concatenate), work-stealing balances skew. Each bucket's table is
// small enough to stay cache-resident, so it beats even a private one-big-table.
//
// Phase 1 (partition): drain the upstream, pack each row's int key, radix-order
// each batch inside a worker, and append bucket-contiguous runs into per-shard
// buffers.
// Phase 2 (lease, parallel): threads pull buckets off one atomic counter and
// aggregate each into a fresh compact-state table, publishing finalized groups
// into central per-bucket result slots. Phase 3 (emit): concatenate (or global
// top-k).
//
// Reuses the compact core (planCompact/scatter/finalize/appendFinalized), the
// int-key packing (agg.orKeyColumn/appendIntGroupKey), and the top-k heap — so it
// is byte-identical to RadixAggregate / the generic Aggregate, just parallel.
// ===========================================================================

inline fn bucketHashU128(key: u128) u64 {
    const lo: u64 = @truncate(key);
    const hi: u64 = @truncate(key >> 64);
    var z = lo ^ (hi *% 0x9e3779b97f4a7c15);
    z = (z ^ (z >> 30)) *% 0xbf58476d1ce4e5b9;
    z = (z ^ (z >> 27)) *% 0x94d049bb133111eb;
    return z ^ (z >> 31);
}

const MAX_LEASE_THREADS: usize = 64;

// Tiny test-and-set spinlock. This Zig routes mutexes through the async Io
// abstraction (no plain blocking mutex), and the raw-thread paths here already
// coordinate with atomics. The partition's critical section (one upstream
// next() + a per-batch column copy) is short, so a spinlock is the right tool.
const SpinLock = struct {
    flag: std.atomic.Value(u32) = .init(0),
    fn lock(self: *SpinLock) void {
        while (self.flag.cmpxchgWeak(0, 1, .acquire, .monotonic) != null) std.atomic.spinLoopHint();
    }
    fn unlock(self: *SpinLock) void {
        self.flag.store(0, .release);
    }
};

/// Test-only: force the bucket count (the synthetic source reports no NDV, so the
/// real heuristic would pick 1). Set/cleared by the differential tests.
var force_buckets: ?usize = null;

pub const RadixLeaseAggregate = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator, // partition buffers + output (single-threaded phases)
    upstream: Query,
    group_col_indices: []usize,
    agg_col_indices: []?usize,
    aggs: []const AggSpec,
    int_layout: agg.IntKeyLayout,
    compact: CompactLayout,
    output_schema: []types.Column,
    output_columns: []ColumnStore,
    views: []ColumnView,
    cap_groups: usize,
    top_k: ?ResolvedTopK,
    dop: usize,
    n_shards: usize,
    n_buckets: usize,
    bshift: u6,
    used_cols: []usize, // distinct upstream column indices referenced by the aggregates
    done: bool = false,
    prof_scan_ticks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    prof_radix_ticks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    prof_group_ticks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    prof_bucket_topk_ticks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    // Per-shard bucket buffers: shard s owns buckets[s*n_buckets .. +n_buckets].
    // The partition scatter is parallelized across shards (one thread each); the
    // lease reads bucket b across all shards.
    buckets: []Bucket = &.{},
    shard_arenas: []std.heap.ArenaAllocator = &.{},
    // The distinct group-key ∪ agg-input columns each partition worker deep-copies
    // per batch (mini-batch order), plus the slot maps from a group-key / agg-input
    // column to its index in that order.
    need_cols: []usize,
    key_flat_slot: []usize, // mini-col slot per group-key column
    val_flat_slot: []usize, // mini-col slot per used (agg-input) column
    fused_pscan: ?*ParallelScan = null,

    // One bucket's partitioned rows: packed keys + a value buffer per used column.
    const Bucket = struct {
        keys: std.ArrayListUnmanaged(u128) = .empty,
        keys32: std.ArrayListUnmanaged(u32) = .empty,
        cols: []ColumnStore = &.{}, // aligned to `used_cols`
    };

    // One bucket's finalized groups in the central lease result set. A bucket is
    // claimed by exactly one worker through the lease counter, so that worker is
    // the only writer for `results[b]`; emit can later walk the central bucket
    // array without a merge because keys are disjoint across buckets.
    const BucketResult = struct {
        keys: std.ArrayListUnmanaged(u128) = .empty,
        keys32: std.ArrayListUnmanaged(u32) = .empty,
        state: std.ArrayListUnmanaged(u64) = .empty, // words-strided, aligned to keys
        sel: []usize = &.{},
        ov: []OrderVal = &.{},
        sel_len: usize = 0,
    };

    // One lease worker's private scratch. Completed groups are published into
    // the central `BucketResult` slot for the bucket the worker leased, not into
    // a per-worker result silo.
    const LeaseWorker = struct {
        arena: std.heap.ArenaAllocator,
        err: ?anyerror = null,
    };

    const CountSumAvgPlan = struct {
        count_off: usize,
        sum_off: usize,
        avg_off: usize,
        sum_ui: usize,
        avg_ui: usize,
    };

    fn countSumAvgPlan(self: *const RadixLeaseAggregate) ?CountSumAvgPlan {
        if (self.compact.aggs.len != 3) return null;
        var out: CountSumAvgPlan = undefined;
        var have_count = false;
        var have_sum = false;
        var have_avg = false;
        for (self.compact.aggs) |ca| {
            switch (ca.kind) {
                .count => {
                    if (ca.col_idx != null) return null;
                    out.count_off = ca.off;
                    have_count = true;
                },
                .sum_int => {
                    out.sum_ui = self.usedSlot(ca.col_idx orelse return null) orelse return null;
                    out.sum_off = ca.off;
                    have_sum = true;
                },
                .avg => {
                    out.avg_ui = self.usedSlot(ca.col_idx orelse return null) orelse return null;
                    out.avg_off = ca.off;
                    have_avg = true;
                },
                else => return null,
            }
        }
        return if (have_count and have_sum and have_avg) out else null;
    }

    fn usedSlot(self: *const RadixLeaseAggregate, ci: usize) ?usize {
        for (self.used_cols, 0..) |u, i| if (u == ci) return i;
        return null;
    }

    pub fn create(allocator: Allocator, upstream: Query, group_cols: []const []const u8, aggs: []const AggSpec, top_k: ?TopK, dop: usize) !Query {
        if (aggs.len == 0) return Error.AggregateNoSpecs;
        const up_schema = upstream.outputSchema();

        const gci = try allocator.alloc(usize, group_cols.len);
        errdefer allocator.free(gci);
        for (group_cols, gci) |name, *dst| dst.* = types.findColumn(up_schema, name) orelse return Error.ColumnNotFound;

        const aci = try allocator.alloc(?usize, aggs.len);
        errdefer allocator.free(aci);
        for (aggs, aci) |a, *dst| dst.* = if (a.col) |name| (types.findColumn(up_schema, name) orelse return Error.ColumnNotFound) else null;

        const layout = (try agg.planIntKey(allocator, gci, up_schema, null, &.{})) orelse return Error.UnsupportedOperatorForType;
        errdefer layout.deinit(allocator);

        const compact = (try planCompact(allocator, aggs, aci, up_schema)) orelse return Error.AggregateUnsupportedType;
        errdefer compact.deinit(allocator);

        // Distinct upstream columns referenced by the aggregates (SUM(v)/AVG(v)
        // share one buffer). COUNT(*) contributes none.
        var used = std.ArrayListUnmanaged(usize).empty;
        errdefer used.deinit(allocator);
        for (aci) |idx| {
            if (idx) |ci| {
                var seen = false;
                for (used.items) |u| {
                    if (u == ci) {
                        seen = true;
                        break;
                    }
                }
                if (!seen) try used.append(allocator, ci);
            }
        }
        const used_cols = try used.toOwnedSlice(allocator);
        errdefer allocator.free(used_cols);

        // Union of group-key and agg-input columns — materialize copies these raw;
        // the parallel scatter packs keys from them per row-slice (slot maps below).
        var need = std.ArrayListUnmanaged(usize).empty;
        errdefer need.deinit(allocator);
        const addUniq = struct {
            fn f(list: *std.ArrayListUnmanaged(usize), al: Allocator, ci: usize) !void {
                for (list.items) |x| if (x == ci) return;
                try list.append(al, ci);
            }
        }.f;
        for (gci) |ci| try addUniq(&need, allocator, ci);
        for (used_cols) |ci| try addUniq(&need, allocator, ci);
        const need_cols = try need.toOwnedSlice(allocator);
        errdefer allocator.free(need_cols);
        const slotOf = struct {
            fn f(nc: []const usize, ci: usize) usize {
                for (nc, 0..) |x, i| if (x == ci) return i;
                unreachable;
            }
        }.f;
        const key_flat_slot = try allocator.alloc(usize, gci.len);
        errdefer allocator.free(key_flat_slot);
        for (gci, key_flat_slot) |ci, *d| d.* = slotOf(need_cols, ci);
        const val_flat_slot = try allocator.alloc(usize, used_cols.len);
        errdefer allocator.free(val_flat_slot);
        for (used_cols, val_flat_slot) |ci, *d| d.* = slotOf(need_cols, ci);

        const out_schema = try allocator.alloc(types.Column, group_cols.len + aggs.len);
        errdefer allocator.free(out_schema);
        for (gci, 0..) |ci, i| out_schema[i] = up_schema[ci];
        for (aggs, aci, 0..) |a, idx, i| {
            const in_t: ?Type = if (idx) |x| up_schema[x].type else null;
            out_schema[group_cols.len + i] = .{ .name = a.as, .type = try agg.aggOutputTypeFor(a, in_t) };
        }

        const out_cols = try allocator.alloc(ColumnStore, out_schema.len);
        errdefer allocator.free(out_cols);
        var inited: usize = 0;
        errdefer for (out_cols[0..inited]) |*c| c.deinit(allocator);
        for (out_schema, 0..) |col, i| {
            out_cols[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }

        const views = try allocator.alloc(ColumnView, out_schema.len);
        errdefer allocator.free(views);

        const st = upstream.stats();
        var est: u64 = 1;
        var known = true;
        for (gci) |ci| {
            if (ci >= st.column_stats.len) {
                known = false;
                break;
            }
            switch (st.column_stats[ci].ndv) {
                .exact => |nd| est *|= nd,
                .unknown => {
                    known = false;
                    break;
                },
            }
        }
        const cap_groups: usize = if (known and est > 0) @intCast(@min(est, @max(st.upper_rows, 1))) else 4096;

        var resolved_tk: ?ResolvedTopK = null;
        if (top_k) |tk| {
            for (aggs, 0..) |a, ai| {
                if (types.columnNameEql(a.as, tk.col)) {
                    resolved_tk = .{ .k = tk.k, .agg_idx = ai, .desc = tk.desc };
                    break;
                }
            }
        }

        const eff_dop = @max(@as(usize, 1), @min(dop, MAX_LEASE_THREADS));

        // Bucket count is a global logical hash partition count. Each shard owns
        // a local slice for every bucket, and the lease phase assigns those
        // global bucket IDs to workers. Too many buckets make partition/emit
        // metadata and locality dominate; current ClickBench high-cardinality
        // sweeps on the focused ClientIP top-count workload now favor 128-256:
        // fewer buckets substantially reduce partition metadata and allocator
        // churn, while still leaving enough bucket work for 12-way leasing.
        // Scale from propagated row/cardinality
        // estimates, but keep a cap until scan-fused partitioning changes this
        // tradeoff.
        const stat_rows: usize = @intCast(@min(st.upper_rows, @as(u64, std.math.maxInt(usize))));
        const sizing_rows = @max(@max(cap_groups, stat_rows), eff_dop * @as(usize, 32 * 1024));
        const target_rows_per_bucket: usize = 32 * 1024;
        const max_auto_buckets: usize = 256;
        const want_buckets = std.math.clamp(sizing_rows / target_rows_per_bucket, 1, max_auto_buckets);
        var nb: usize = 1;
        var nbits: u6 = 0;
        while (nb < want_buckets) {
            nb <<= 1;
            nbits += 1;
        }
        const ov_buckets = force_buckets orelse (getenvUsize("THINDB_LEASE_BUCKETS") orelse 0);
        if (ov_buckets != 0) {
            nb = 1;
            nbits = 0;
            while (nb < ov_buckets and nb < (1 << 16)) {
                nb <<= 1;
                nbits += 1;
            }
        }

        const self = try allocator.create(RadixLeaseAggregate);
        self.* = .{
            .allocator = allocator,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .upstream = upstream,
            .group_col_indices = gci,
            .agg_col_indices = aci,
            .aggs = aggs,
            .int_layout = layout,
            .compact = compact,
            .output_schema = out_schema,
            .output_columns = out_cols,
            .views = views,
            .cap_groups = @max(cap_groups, 256),
            .top_k = resolved_tk,
            .dop = eff_dop,
            .n_shards = eff_dop,
            .n_buckets = nb,
            .bshift = if (nbits == 0) 0 else @intCast(64 - @as(usize, nbits)), // unused when n_buckets==1 (bucketOf short-circuits)
            .used_cols = used_cols,
            .need_cols = need_cols,
            .key_flat_slot = key_flat_slot,
            .val_flat_slot = val_flat_slot,
        };
        return makeQuery(allocator, self);
    }

    pub fn createFromParallelScan(allocator: Allocator, pscan: *ParallelScan, group_cols: []const []const u8, aggs: []const AggSpec, top_k: ?TopK, dop: usize) !Query {
        const q = try create(allocator, makeQuery(allocator, pscan), group_cols, aggs, top_k, dop);
        const self: *RadixLeaseAggregate = @ptrCast(@alignCast(q.ptr));
        self.fused_pscan = pscan;
        return q;
    }

    inline fn bucketOf(self: *const RadixLeaseAggregate, key: u128) usize {
        return self.bucketOfHash(bucketHashU128(key));
    }

    inline fn bucketOfHash(self: *const RadixLeaseAggregate, h: u64) usize {
        // Top `nbits` of the hash → independent of the low bits the per-bucket
        // table uses for its slot. `n_buckets == 1` ⇒ all → bucket 0.
        if (self.n_buckets == 1) return 0;
        return @intCast(h >> self.bshift);
    }

    inline fn addProfileTicks(counter: *std.atomic.Value(u64), start: i64, enabled: bool) void {
        if (!enabled) return;
        const elapsed = prof.nowTicks() - start;
        if (elapsed > 0) _ = counter.fetchAdd(@intCast(elapsed), .monotonic);
    }

    fn useKey32Partitions(self: *const RadixLeaseAggregate) bool {
        // The focused ClientIP query is a single native 32-bit integer key. Keep
        // the narrower partition/result buffers to that shape so compound and
        // coded keys continue through the canonical u128 path.
        return self.fused_pscan != null and self.int_layout.tier == .bits32 and self.int_layout.fields.len == 1 and !self.int_layout.fields[0].coded and self.int_layout.fields[0].offset == 0;
    }

    fn fillKey32(out: []u32, batch: Batch, ci: usize, f: agg.IntKeyField) void {
        std.debug.assert(f.offset == 0);
        switch (batch.values[ci].data) {
            .boolean => |s| {
                for (out, s) |*k, v| k.* = @as(u32, v);
            },
            .tinyint => |s| {
                for (out, s) |*k, v| k.* = @as(u32, @as(u8, @bitCast(v)));
            },
            .smallint => |s| {
                for (out, s) |*k, v| k.* = @as(u32, @as(u16, @bitCast(v)));
            },
            inline .int, .date => |s| {
                for (out, s) |*k, v| k.* = @as(u32, @bitCast(v));
            },
            else => unreachable,
        }
    }

    // --- Phase 1: each thread drains its own batches off a shared cursor and
    // radix-sorts them into its private per-bucket blocks — no serial materialize.
    // A mutex guards only the upstream cursor + the per-batch copy; the key-pack
    // and bucket scatter (the real cost) run lock-free. The lease (phase 2) then
    // reads bucket b across every shard.
    fn initPartitionBuffers(self: *RadixLeaseAggregate, shard_count: usize) !void {
        const up_schema = self.upstream.outputSchema();
        self.n_shards = shard_count;
        const S = self.n_shards;
        self.buckets = try self.arena.allocator().alloc(Bucket, S * self.n_buckets);
        self.shard_arenas = try self.allocator.alloc(std.heap.ArenaAllocator, S);
        for (self.shard_arenas) |*ar| ar.* = std.heap.ArenaAllocator.init(self.allocator);
        const per_shard = @as(usize, @intCast(@max(self.upstream.stats().upper_rows, 1))) / S;
        const reserve = @min((per_shard / self.n_buckets) * 3 / 2 + 16, 1 << 20);
        for (0..S) |s| {
            const sa = self.shard_arenas[s].allocator();
            for (0..self.n_buckets) |b| {
                const bk = &self.buckets[s * self.n_buckets + b];
                bk.* = .{ .cols = try sa.alloc(ColumnStore, self.used_cols.len) };
                if (self.useKey32Partitions()) {
                    try bk.keys32.ensureTotalCapacity(sa, reserve);
                } else {
                    try bk.keys.ensureTotalCapacity(sa, reserve);
                }
                for (self.used_cols, bk.cols) |ci, *cs| cs.* = try ColumnStore.initCapacity(sa, up_schema[ci].type, up_schema[ci].nullable, reserve, 0);
            }
        }
    }

    fn partition(self: *RadixLeaseAggregate) !void {
        if (self.fused_pscan) |ps| return self.partitionParallelScan(ps);

        const S = self.dop;
        try self.initPartitionBuffers(S);

        var mutex: SpinLock = .{};
        const Entry = struct {
            fn go(op: *RadixLeaseAggregate, mx: *SpinLock, s: usize, errp: *?anyerror) void {
                op.partitionShard(mx, s) catch |e| {
                    errp.* = e;
                };
            }
        };
        const errs = try self.allocator.alloc(?anyerror, S);
        defer self.allocator.free(errs);
        @memset(errs, null);
        var threads: [MAX_LEASE_THREADS]std.Thread = undefined;
        var spawned: usize = 0;
        var t: usize = 1;
        while (t < S) : (t += 1) {
            threads[spawned] = std.Thread.spawn(.{}, Entry.go, .{ self, &mutex, t, &errs[t] }) catch {
                Entry.go(self, &mutex, t, &errs[t]);
                continue;
            };
            spawned += 1;
        }
        Entry.go(self, &mutex, 0, &errs[0]);
        for (threads[0..spawned]) |th| th.join();
        for (errs) |e| if (e) |err| return err;
    }

    // One partition worker: pull a batch off the shared upstream cursor, deep-copy
    // its group-key + agg-input columns into thread-local buffers under the mutex
    // (so they stay valid for ANY upstream lifetime after release), then pack the
    // int key and radix-scatter each row into this shard's bucket blocks lock-free.
    fn partitionShard(self: *RadixLeaseAggregate, mutex: *SpinLock, s: usize) !void {
        const sa = self.shard_arenas[s].allocator();
        const up_schema = self.upstream.outputSchema();
        const shard_buckets = self.buckets[s * self.n_buckets .. (s + 1) * self.n_buckets];
        const layout = self.int_layout;

        const mini_cols = try sa.alloc(ColumnStore, self.need_cols.len);
        for (self.need_cols, mini_cols) |ci, *cs| cs.* = try ColumnStore.init(sa, up_schema[ci].type, up_schema[ci].nullable);
        const mini_vals = try sa.alloc(ColumnView, up_schema.len);
        var kb: std.ArrayListUnmanaged(u128) = .empty;
        var bof: std.ArrayListUnmanaged(u16) = .empty;
        var nonempty: std.ArrayListUnmanaged(usize) = .empty;
        var row_order: std.ArrayListUnmanaged(usize) = .empty;
        var sorted_keys: std.ArrayListUnmanaged(u128) = .empty;
        const counts = try sa.alloc(usize, self.n_buckets);
        const offsets = try sa.alloc(usize, self.n_buckets + 1);
        const cursors = try sa.alloc(usize, self.n_buckets);

        while (true) {
            const prof_on = getenv("THINDB_LEASE_PROF") != null;
            const scan_t0 = if (prof_on) prof.nowTicks() else 0;
            mutex.lock();
            const maybe = self.upstream.next() catch |e| {
                mutex.unlock();
                return e;
            };
            const batch = maybe orelse {
                mutex.unlock();
                break;
            };
            const n = batch.row_count;
            if (n != 0) {
                for (self.need_cols, mini_cols) |ci, *cs| {
                    cs.clear();
                    try transform.appendAllColumn(sa, batch.values[ci], cs);
                }
            }
            mutex.unlock();
            addProfileTicks(&self.prof_scan_ticks, scan_t0, prof_on);
            if (n == 0) continue;

            const radix_t0 = if (prof_on) prof.nowTicks() else 0;
            try kb.ensureTotalCapacity(sa, n);
            kb.clearRetainingCapacity();
            kb.appendNTimesAssumeCapacity(0, n);
            for (self.group_col_indices, self.key_flat_slot) |ci, ks| mini_vals[ci] = mini_cols[ks].view();
            const mini = Batch{ .schema = up_schema, .values = mini_vals, .row_count = n };
            for (self.group_col_indices, layout.fields) |ci, f| agg.orKeyColumn(kb.items[0..n], mini, ci, f);

            try bof.ensureTotalCapacity(sa, n);
            bof.clearRetainingCapacity();
            try nonempty.ensureTotalCapacity(sa, @min(n, self.n_buckets));
            nonempty.clearRetainingCapacity();
            @memset(counts, 0);
            for (0..n) |i| {
                const b: u16 = @intCast(self.bucketOf(kb.items[i]));
                bof.appendAssumeCapacity(b);
                if (counts[b] == 0) nonempty.appendAssumeCapacity(b);
                counts[b] += 1;
            }
            offsets[0] = 0;
            for (0..self.n_buckets) |b| offsets[b + 1] = offsets[b] + counts[b];
            @memcpy(cursors, offsets[0..self.n_buckets]);

            try row_order.ensureTotalCapacity(sa, n);
            row_order.clearRetainingCapacity();
            row_order.appendNTimesAssumeCapacity(0, n);
            try sorted_keys.ensureTotalCapacity(sa, n);
            sorted_keys.clearRetainingCapacity();
            sorted_keys.appendNTimesAssumeCapacity(0, n);
            for (0..n) |i| {
                const b = bof.items[i];
                const dst = cursors[b];
                cursors[b] = dst + 1;
                row_order.items[dst] = i;
                sorted_keys.items[dst] = kb.items[i];
            }

            for (nonempty.items) |b| {
                const start = offsets[b];
                const end = offsets[b + 1];
                try shard_buckets[b].keys.appendSlice(sa, sorted_keys.items[start..end]);
            }
            for (self.val_flat_slot, 0..) |vs, ui| {
                const view = mini_cols[vs].view();
                for (nonempty.items) |b| {
                    const start = offsets[b];
                    const end = offsets[b + 1];
                    try appendColumnRows(sa, view, row_order.items[start..end], &shard_buckets[b].cols[ui]);
                }
            }
            addProfileTicks(&self.prof_radix_ticks, radix_t0, prof_on);
        }
    }

    fn partitionParallelScan(self: *RadixLeaseAggregate, pscan: *ParallelScan) !void {
        if (pscan.mode != .unset or pscan.agg_fused) return Error.UnsupportedOperatorForType;
        const shard_count = pscan.n_threads;
        try self.initPartitionBuffers(shard_count);

        const Entry = struct {
            fn scanGo(op: *RadixLeaseAggregate, ps: *ParallelScan, cursor: *std.atomic.Value(usize), shard: usize, errp: *?anyerror) void {
                while (true) {
                    const i = cursor.fetchAdd(1, .monotonic);
                    if (i >= ps.workers.len) break;
                    op.partitionDrain(shard, ps.workers[i]) catch |e| {
                        errp.* = e;
                        break;
                    };
                }
            }
            fn queryGo(op: *RadixLeaseAggregate, ps: *ParallelScan, cursor: *std.atomic.Value(usize), shard: usize, errp: *?anyerror) void {
                while (true) {
                    const i = cursor.fetchAdd(1, .monotonic);
                    if (i >= ps.compute_q.len) break;
                    op.partitionDrain(shard, ps.compute_q[i]) catch |e| {
                        errp.* = e;
                        break;
                    };
                }
            }
        };

        const errs = try self.allocator.alloc(?anyerror, pscan.n_threads);
        defer self.allocator.free(errs);
        @memset(errs, null);
        var cursor = std.atomic.Value(usize).init(0);
        var threads: [MAX_LEASE_THREADS]std.Thread = undefined;
        var spawned: usize = 0;
        var t: usize = 1;
        if (pscan.compute_fused) {
            while (t < pscan.n_threads and t < MAX_LEASE_THREADS) : (t += 1) {
                threads[spawned] = std.Thread.spawn(.{}, Entry.queryGo, .{ self, pscan, &cursor, t, &errs[t] }) catch {
                    Entry.queryGo(self, pscan, &cursor, t, &errs[t]);
                    continue;
                };
                spawned += 1;
            }
            Entry.queryGo(self, pscan, &cursor, 0, &errs[0]);
        } else {
            while (t < pscan.n_threads and t < MAX_LEASE_THREADS) : (t += 1) {
                threads[spawned] = std.Thread.spawn(.{}, Entry.scanGo, .{ self, pscan, &cursor, t, &errs[t] }) catch {
                    Entry.scanGo(self, pscan, &cursor, t, &errs[t]);
                    continue;
                };
                spawned += 1;
            }
            Entry.scanGo(self, pscan, &cursor, 0, &errs[0]);
        }
        for (threads[0..spawned]) |th| th.join();
        for (errs) |e| if (e) |err| return err;
    }

    fn partitionDrain(self: *RadixLeaseAggregate, s: usize, drainable: anytype) !void {
        if (self.useKey32Partitions()) return self.partitionDrain32(s, drainable);

        const sa = self.shard_arenas[s].allocator();
        const shard_buckets = self.buckets[s * self.n_buckets .. (s + 1) * self.n_buckets];
        const layout = self.int_layout;

        var d = drainable;
        var kb: std.ArrayListUnmanaged(u128) = .empty;
        var bof: std.ArrayListUnmanaged(u16) = .empty;
        var nonempty: std.ArrayListUnmanaged(usize) = .empty;
        var row_order: std.ArrayListUnmanaged(usize) = .empty;
        var sorted_keys: std.ArrayListUnmanaged(u128) = .empty;
        const counts = try sa.alloc(usize, self.n_buckets);
        const offsets = try sa.alloc(usize, self.n_buckets + 1);
        const cursors = try sa.alloc(usize, self.n_buckets);

        const prof_on = getenv("THINDB_LEASE_PROF") != null;
        while (true) {
            const scan_t0 = if (prof_on) prof.nowTicks() else 0;
            const maybe = try d.next();
            addProfileTicks(&self.prof_scan_ticks, scan_t0, prof_on);
            const batch = maybe orelse break;
            const n = batch.row_count;
            if (n == 0) continue;

            const radix_t0 = if (prof_on) prof.nowTicks() else 0;
            try kb.ensureTotalCapacity(sa, n);
            kb.clearRetainingCapacity();
            kb.appendNTimesAssumeCapacity(0, n);
            for (self.group_col_indices, layout.fields) |ci, f| agg.orKeyColumn(kb.items[0..n], batch, ci, f);

            try bof.ensureTotalCapacity(sa, n);
            bof.clearRetainingCapacity();
            try nonempty.ensureTotalCapacity(sa, @min(n, self.n_buckets));
            nonempty.clearRetainingCapacity();
            @memset(counts, 0);
            for (0..n) |i| {
                const b: u16 = @intCast(self.bucketOf(kb.items[i]));
                bof.appendAssumeCapacity(b);
                if (counts[b] == 0) nonempty.appendAssumeCapacity(b);
                counts[b] += 1;
            }
            offsets[0] = 0;
            for (0..self.n_buckets) |b| offsets[b + 1] = offsets[b] + counts[b];
            @memcpy(cursors, offsets[0..self.n_buckets]);

            try row_order.ensureTotalCapacity(sa, n);
            row_order.clearRetainingCapacity();
            row_order.appendNTimesAssumeCapacity(0, n);
            try sorted_keys.ensureTotalCapacity(sa, n);
            sorted_keys.clearRetainingCapacity();
            sorted_keys.appendNTimesAssumeCapacity(0, n);
            for (0..n) |i| {
                const b = bof.items[i];
                const dst = cursors[b];
                cursors[b] = dst + 1;
                row_order.items[dst] = i;
                sorted_keys.items[dst] = kb.items[i];
            }

            for (nonempty.items) |b| {
                const start = offsets[b];
                const end = offsets[b + 1];
                try shard_buckets[b].keys.appendSlice(sa, sorted_keys.items[start..end]);
            }
            for (self.used_cols, 0..) |ci, ui| {
                const view = batch.values[ci];
                for (nonempty.items) |b| {
                    const start = offsets[b];
                    const end = offsets[b + 1];
                    try appendColumnRows(sa, view, row_order.items[start..end], &shard_buckets[b].cols[ui]);
                }
            }
            addProfileTicks(&self.prof_radix_ticks, radix_t0, prof_on);
        }
    }

    fn partitionDrain32(self: *RadixLeaseAggregate, s: usize, drainable: anytype) !void {
        const sa = self.shard_arenas[s].allocator();
        const shard_buckets = self.buckets[s * self.n_buckets .. (s + 1) * self.n_buckets];
        const field = self.int_layout.fields[0];
        const ci = self.group_col_indices[0];

        var d = drainable;
        var kb: std.ArrayListUnmanaged(u32) = .empty;
        var bof: std.ArrayListUnmanaged(u16) = .empty;
        var nonempty: std.ArrayListUnmanaged(usize) = .empty;
        var row_order: std.ArrayListUnmanaged(usize) = .empty;
        var sorted_keys: std.ArrayListUnmanaged(u32) = .empty;
        const counts = try sa.alloc(usize, self.n_buckets);
        const offsets = try sa.alloc(usize, self.n_buckets + 1);
        const cursors = try sa.alloc(usize, self.n_buckets);

        const Table32 = gt.IntKeyTable(32);
        const prof_on = getenv("THINDB_LEASE_PROF") != null;
        while (true) {
            const scan_t0 = if (prof_on) prof.nowTicks() else 0;
            const maybe = try d.next();
            addProfileTicks(&self.prof_scan_ticks, scan_t0, prof_on);
            const batch = maybe orelse break;
            const n = batch.row_count;
            if (n == 0) continue;

            const radix_t0 = if (prof_on) prof.nowTicks() else 0;
            try kb.ensureTotalCapacity(sa, n);
            kb.clearRetainingCapacity();
            kb.appendNTimesAssumeCapacity(0, n);
            fillKey32(kb.items[0..n], batch, ci, field);

            try bof.ensureTotalCapacity(sa, n);
            bof.clearRetainingCapacity();
            try nonempty.ensureTotalCapacity(sa, @min(n, self.n_buckets));
            nonempty.clearRetainingCapacity();
            @memset(counts, 0);
            for (0..n) |i| {
                const b: u16 = @intCast(self.bucketOfHash(Table32.hashKey(@as(u128, kb.items[i]))));
                bof.appendAssumeCapacity(b);
                if (counts[b] == 0) nonempty.appendAssumeCapacity(b);
                counts[b] += 1;
            }
            offsets[0] = 0;
            for (0..self.n_buckets) |b| offsets[b + 1] = offsets[b] + counts[b];
            @memcpy(cursors, offsets[0..self.n_buckets]);

            try row_order.ensureTotalCapacity(sa, n);
            row_order.clearRetainingCapacity();
            row_order.appendNTimesAssumeCapacity(0, n);
            try sorted_keys.ensureTotalCapacity(sa, n);
            sorted_keys.clearRetainingCapacity();
            sorted_keys.appendNTimesAssumeCapacity(0, n);
            for (0..n) |i| {
                const b = bof.items[i];
                const dst = cursors[b];
                cursors[b] = dst + 1;
                row_order.items[dst] = i;
                sorted_keys.items[dst] = kb.items[i];
            }

            for (nonempty.items) |b| {
                const start = offsets[b];
                const end = offsets[b + 1];
                try shard_buckets[b].keys32.appendSlice(sa, sorted_keys.items[start..end]);
            }
            for (self.used_cols, 0..) |ci_val, ui| {
                const view = batch.values[ci_val];
                for (nonempty.items) |b| {
                    const start = offsets[b];
                    const end = offsets[b + 1];
                    try appendColumnRows(sa, view, row_order.items[start..end], &shard_buckets[b].cols[ui]);
                }
            }
            addProfileTicks(&self.prof_radix_ticks, radix_t0, prof_on);
        }
    }

    // Append an already-radix-ordered row run from `src` into one bucket column.
    // The compact aggregate path excludes string/uuid aggregate inputs.
    fn appendColumnRows(aa: Allocator, src: ColumnView, rows: []const usize, out: *ColumnStore) !void {
        const dst_start = out.rowCount();
        switch (src.data) {
            inline .int, .bigint, .smallint, .tinyint, .boolean, .date, .datetime, .largeint, .decimal64, .decimal128, .float, .double => |sl, tag| {
                const list = &@field(out.data, @tagName(tag));
                try list.ensureUnusedCapacity(aa, rows.len);
                for (rows) |r| list.appendAssumeCapacity(sl[r]);
            },
            else => unreachable,
        }
        if (out.nulls) |*nulls| {
            try nulls.ensureTotalCapacity(aa, (dst_start + rows.len + 7) >> 3);
            for (rows, 0..) |r, i| try out.appendValidBit(aa, dst_start + i, src.isValid(r));
        }
    }

    fn readIntAsI128(view: ColumnView, row: usize) i128 {
        return switch (view.data) {
            .int => |sl| sl[row],
            .smallint => |sl| sl[row],
            .tinyint => |sl| sl[row],
            .boolean => |sl| sl[row],
            .bigint => |sl| sl[row],
            .decimal64 => |sl| sl[row],
            .largeint => |sl| sl[row],
            .decimal128 => |sl| sl[row],
            else => unreachable,
        };
    }

    fn readNumericAsF64(view: ColumnView, row: usize) f64 {
        return switch (view.data) {
            .int => |sl| @floatFromInt(sl[row]),
            .smallint => |sl| @floatFromInt(sl[row]),
            .tinyint => |sl| @floatFromInt(sl[row]),
            .boolean => |sl| @floatFromInt(sl[row]),
            .bigint => |sl| @floatFromInt(sl[row]),
            .decimal64 => |sl| @floatFromInt(sl[row]),
            .float => |sl| sl[row],
            .double => |sl| sl[row],
            else => unreachable,
        };
    }

    inline fn accumulateCountSumAvg(plan: CountSumAvgPlan, state: []u64, words: usize, gid: u32, sum_view: ColumnView, avg_view: ColumnView, row: usize) void {
        const base = @as(usize, gid) * words;
        state[base + plan.count_off] += 1;
        if (sum_view.isValid(row)) addI128(state, base + plan.sum_off, readIntAsI128(sum_view, row));
        if (avg_view.isValid(row)) {
            const slot = base + plan.avg_off;
            var sum: f64 = @bitCast(state[slot]);
            sum += readNumericAsF64(avg_view, row);
            state[slot] = @bitCast(sum);
            state[slot + 1] += 1;
        }
    }

    // --- Phase 2: lease + aggregate (parallel) -----------------------------
    fn workerRun(
        self: *RadixLeaseAggregate,
        comptime Table: type,
        worker: *LeaseWorker,
        ctr: *std.atomic.Value(usize),
        results: []BucketResult,
    ) !void {
        const aa = worker.arena.allocator();
        const words = self.compact.words;
        const up_schema = self.upstream.outputSchema();
        var gidbuf: std.ArrayListUnmanaged(u32) = .empty;
        const vbuf = try aa.alloc(ColumnView, up_schema.len);

        while (true) {
            const b = ctr.fetchAdd(1, .monotonic);
            if (b >= self.n_buckets) break;
            // Bucket b's rows are spread across all shards (the partition wrote one
            // shard per scatter thread). One table per bucket combines them.
            var total: usize = 0;
            for (0..self.n_shards) |s| total += self.buckets[s * self.n_buckets + b].keys.items.len;
            if (total == 0) continue;

            const prof_on = getenv("THINDB_LEASE_PROF") != null;
            const group_t0 = if (prof_on) prof.nowTicks() else 0;
            var table = try Table.init(aa, total);
            const result = &results[b];
            result.keys.clearRetainingCapacity();
            result.state.clearRetainingCapacity();
            var local_groups: u32 = 0;

            for (0..self.n_shards) |s| {
                const bucket = &self.buckets[s * self.n_buckets + b];
                const n = bucket.keys.items.len;
                if (n == 0) continue;
                gidbuf.clearRetainingCapacity();
                try gidbuf.ensureTotalCapacity(aa, n);
                for (0..n) |j| {
                    const key = bucket.keys.items[j];
                    const h = Table.hashKey(key);
                    if (j + PREFETCH_DIST < n) {
                        const pk = bucket.keys.items[j + PREFETCH_DIST];
                        @prefetch(table.slotAddr(table.bucketOf(Table.hashKey(pk))), .{ .rw = .write, .locality = 1 });
                    }
                    const p = table.getOrPut(h, key);
                    const g = if (p.found) p.gid else blk: {
                        table.commit(p.slot, key, local_groups);
                        try result.keys.append(aa, key);
                        const base = @as(usize, local_groups) * words;
                        try result.state.ensureTotalCapacity(aa, base + words);
                        result.state.items.len = base + words;
                        @memset(result.state.items[base .. base + words], 0);
                        const ng = local_groups;
                        local_groups += 1;
                        break :blk ng;
                    };
                    gidbuf.appendAssumeCapacity(g);
                }
                // Synthetic batch over this shard's bucket buffers; scatter its
                // rows into the shared per-bucket state at their resolved gids.
                for (self.used_cols, 0..) |ci, ui| vbuf[ci] = bucket.cols[ui].view();
                const synth = Batch{ .schema = up_schema, .values = vbuf, .row_count = n };
                scatter(self.compact, result.state.items, gidbuf.items[0..n], synth);
            }
            addProfileTicks(&self.prof_group_ticks, group_t0, prof_on);

            // The lease counter guarantees that no other worker wrote this
            // central result slot while we filled it.
            std.debug.assert(result.keys.items.len == local_groups);
            if (self.top_k != null) {
                const topk_t0 = if (prof_on) prof.nowTicks() else 0;
                try self.bucketTopK(result, aa);
                addProfileTicks(&self.prof_bucket_topk_ticks, topk_t0, prof_on);
            }
        }
    }

    /// Reduce one leased bucket's groups to a bucket-local top-k while this
    /// worker still owns the bucket. Emit merges bucket-local candidates from the
    /// central result array instead of rescanning every group serially.
    fn bucketTopK(self: *RadixLeaseAggregate, result: *BucketResult, aa: Allocator) !void {
        const tk = self.top_k.?;
        const ng = if (self.useKey32Partitions()) result.keys32.items.len else result.keys.items.len;
        const k = @min(@as(usize, tk.k), ng);
        result.sel = try aa.alloc(usize, k);
        result.ov = try aa.alloc(OrderVal, k);
        var len: usize = 0;
        for (0..ng) |g| {
            const val = orderValOf(self.compact, result.state.items, g, tk.agg_idx);
            if (len < k) {
                result.sel[len] = g;
                result.ov[len] = val;
                len += 1;
                if (len == k) topkBuildHeap(result.sel, result.ov, tk.desc);
            } else if (val.preferred(result.ov[0], tk.desc)) {
                result.sel[0] = g;
                result.ov[0] = val;
                topkSiftDown(result.sel, result.ov, 0, k, tk.desc);
            }
        }
        result.sel_len = len;
    }

    fn runLease(self: *RadixLeaseAggregate, comptime Table: type, workers: []LeaseWorker, results: []BucketResult) !void {
        // Entry captures `Table` at comptime (it can't ride through Thread.spawn's
        // runtime arg tuple); only the runtime self/worker/ctr are passed.
        const Entry = struct {
            fn go(s: *RadixLeaseAggregate, w: *LeaseWorker, c: *std.atomic.Value(usize), r: []BucketResult) void {
                s.workerRun(Table, w, c, r) catch |e| {
                    w.err = e;
                };
            }
        };
        var ctr = std.atomic.Value(usize).init(0);
        var threads: [MAX_LEASE_THREADS]std.Thread = undefined;
        var spawned: usize = 0;
        // Spawn dop-1 helpers; the calling thread runs the last share.
        var t: usize = 1;
        while (t < self.dop) : (t += 1) {
            threads[spawned] = std.Thread.spawn(.{}, Entry.go, .{ self, &workers[t], &ctr, results }) catch {
                Entry.go(self, &workers[t], &ctr, results); // spawn failed: run inline
                continue;
            };
            spawned += 1;
        }
        Entry.go(self, &workers[0], &ctr, results);
        for (threads[0..spawned]) |th| th.join();
    }

    fn workerRun32(
        self: *RadixLeaseAggregate,
        worker: *LeaseWorker,
        ctr: *std.atomic.Value(usize),
        results: []BucketResult,
    ) !void {
        const Table = gt.IntKeyTable(32);
        const aa = worker.arena.allocator();
        const words = self.compact.words;
        const up_schema = self.upstream.outputSchema();
        var gidbuf: std.ArrayListUnmanaged(u32) = .empty;
        const vbuf = try aa.alloc(ColumnView, up_schema.len);

        while (true) {
            const b = ctr.fetchAdd(1, .monotonic);
            if (b >= self.n_buckets) break;
            var total: usize = 0;
            for (0..self.n_shards) |s| total += self.buckets[s * self.n_buckets + b].keys32.items.len;
            if (total == 0) continue;

            const prof_on = getenv("THINDB_LEASE_PROF") != null;
            const group_t0 = if (prof_on) prof.nowTicks() else 0;
            var table = try Table.init(aa, total);
            const result = &results[b];
            result.keys32.clearRetainingCapacity();
            result.state.clearRetainingCapacity();
            var local_groups: u32 = 0;

            for (0..self.n_shards) |s| {
                const bucket = &self.buckets[s * self.n_buckets + b];
                const n = bucket.keys32.items.len;
                if (n == 0) continue;
                gidbuf.clearRetainingCapacity();
                try gidbuf.ensureTotalCapacity(aa, n);
                for (0..n) |j| {
                    const key32 = bucket.keys32.items[j];
                    const key = @as(u128, key32);
                    const h = Table.hashKey(key);
                    if (j + PREFETCH_DIST < n) {
                        const pk = @as(u128, bucket.keys32.items[j + PREFETCH_DIST]);
                        @prefetch(table.slotAddr(table.bucketOf(Table.hashKey(pk))), .{ .rw = .write, .locality = 1 });
                    }
                    const p = table.getOrPut(h, key);
                    const g = if (p.found) p.gid else blk: {
                        table.commit(p.slot, key, local_groups);
                        try result.keys32.append(aa, key32);
                        const base = @as(usize, local_groups) * words;
                        try result.state.ensureTotalCapacity(aa, base + words);
                        result.state.items.len = base + words;
                        @memset(result.state.items[base .. base + words], 0);
                        const ng = local_groups;
                        local_groups += 1;
                        break :blk ng;
                    };
                    gidbuf.appendAssumeCapacity(g);
                }
                for (self.used_cols, 0..) |ci, ui| vbuf[ci] = bucket.cols[ui].view();
                const synth = Batch{ .schema = up_schema, .values = vbuf, .row_count = n };
                scatter(self.compact, result.state.items, gidbuf.items[0..n], synth);
            }
            addProfileTicks(&self.prof_group_ticks, group_t0, prof_on);

            std.debug.assert(result.keys32.items.len == local_groups);
            if (self.top_k != null) {
                const topk_t0 = if (prof_on) prof.nowTicks() else 0;
                try self.bucketTopK(result, aa);
                addProfileTicks(&self.prof_bucket_topk_ticks, topk_t0, prof_on);
            }
        }
    }

    fn runLease32(self: *RadixLeaseAggregate, workers: []LeaseWorker, results: []BucketResult) !void {
        const Entry = struct {
            fn go(s: *RadixLeaseAggregate, w: *LeaseWorker, c: *std.atomic.Value(usize), r: []BucketResult) void {
                s.workerRun32(w, c, r) catch |e| {
                    w.err = e;
                };
            }
        };
        var ctr = std.atomic.Value(usize).init(0);
        var threads: [MAX_LEASE_THREADS]std.Thread = undefined;
        var spawned: usize = 0;
        var t: usize = 1;
        while (t < self.dop) : (t += 1) {
            threads[spawned] = std.Thread.spawn(.{}, Entry.go, .{ self, &workers[t], &ctr, results }) catch {
                Entry.go(self, &workers[t], &ctr, results);
                continue;
            };
            spawned += 1;
        }
        Entry.go(self, &workers[0], &ctr, results);
        for (threads[0..spawned]) |th| th.join();
    }

    fn workerRunCountSumAvg(
        self: *RadixLeaseAggregate,
        comptime Table: type,
        plan: CountSumAvgPlan,
        worker: *LeaseWorker,
        ctr: *std.atomic.Value(usize),
        results: []BucketResult,
    ) !void {
        const aa = worker.arena.allocator();
        const words = self.compact.words;
        while (true) {
            const b = ctr.fetchAdd(1, .monotonic);
            if (b >= self.n_buckets) break;
            var total: usize = 0;
            for (0..self.n_shards) |s| total += self.buckets[s * self.n_buckets + b].keys.items.len;
            if (total == 0) continue;

            const prof_on = getenv("THINDB_LEASE_PROF") != null;
            const group_t0 = if (prof_on) prof.nowTicks() else 0;
            var table = try Table.init(aa, total);
            const result = &results[b];
            result.keys.clearRetainingCapacity();
            result.state.clearRetainingCapacity();
            var local_groups: u32 = 0;

            for (0..self.n_shards) |s| {
                const bucket = &self.buckets[s * self.n_buckets + b];
                const n = bucket.keys.items.len;
                if (n == 0) continue;
                const sum_view = bucket.cols[plan.sum_ui].view();
                const avg_view = bucket.cols[plan.avg_ui].view();
                for (0..n) |j| {
                    const key = bucket.keys.items[j];
                    const h = Table.hashKey(key);
                    if (j + PREFETCH_DIST < n) {
                        const pk = bucket.keys.items[j + PREFETCH_DIST];
                        @prefetch(table.slotAddr(table.bucketOf(Table.hashKey(pk))), .{ .rw = .write, .locality = 1 });
                    }
                    const p = table.getOrPut(h, key);
                    const g = if (p.found) p.gid else blk: {
                        table.commit(p.slot, key, local_groups);
                        try result.keys.append(aa, key);
                        const base = @as(usize, local_groups) * words;
                        try result.state.ensureTotalCapacity(aa, base + words);
                        result.state.items.len = base + words;
                        @memset(result.state.items[base .. base + words], 0);
                        const ng = local_groups;
                        local_groups += 1;
                        break :blk ng;
                    };
                    accumulateCountSumAvg(plan, result.state.items, words, g, sum_view, avg_view, j);
                }
            }
            addProfileTicks(&self.prof_group_ticks, group_t0, prof_on);
            std.debug.assert(result.keys.items.len == local_groups);
            if (self.top_k != null) {
                const topk_t0 = if (prof_on) prof.nowTicks() else 0;
                try self.bucketTopK(result, aa);
                addProfileTicks(&self.prof_bucket_topk_ticks, topk_t0, prof_on);
            }
        }
    }

    fn runLeaseCountSumAvg(self: *RadixLeaseAggregate, comptime Table: type, plan: CountSumAvgPlan, workers: []LeaseWorker, results: []BucketResult) !void {
        const Entry = struct {
            fn go(s: *RadixLeaseAggregate, p: CountSumAvgPlan, w: *LeaseWorker, c: *std.atomic.Value(usize), r: []BucketResult) void {
                s.workerRunCountSumAvg(Table, p, w, c, r) catch |e| {
                    w.err = e;
                };
            }
        };
        var ctr = std.atomic.Value(usize).init(0);
        var threads: [MAX_LEASE_THREADS]std.Thread = undefined;
        var spawned: usize = 0;
        var t: usize = 1;
        while (t < self.dop) : (t += 1) {
            threads[spawned] = std.Thread.spawn(.{}, Entry.go, .{ self, plan, &workers[t], &ctr, results }) catch {
                Entry.go(self, plan, &workers[t], &ctr, results);
                continue;
            };
            spawned += 1;
        }
        Entry.go(self, plan, &workers[0], &ctr, results);
        for (threads[0..spawned]) |th| th.join();
    }

    // --- Phase 3: emit (serial concat / global top-k) ----------------------
    fn emitBucketResults(self: *RadixLeaseAggregate, results: []const BucketResult) !void {
        const key32 = self.useKey32Partitions();
        const tk = self.top_k orelse {
            for (results) |*r| {
                if (key32) {
                    for (r.keys32.items, 0..) |key, g| try self.emitOne(r.state.items, g, @as(u128, key));
                } else {
                    for (r.keys.items, 0..) |key, g| try self.emitOne(r.state.items, g, key);
                }
            }
            return;
        };
        // Merge bucket-local top-k candidates. Encode (bucket<<40 | gid).
        var total_cand: usize = 0;
        for (results) |*r| total_cand += r.sel_len;
        const k = @min(@as(usize, tk.k), total_cand);
        if (k == 0) return;
        const aa = self.arena.allocator();
        const sel = try aa.alloc(usize, k);
        const ov = try aa.alloc(OrderVal, k);
        var len: usize = 0;
        for (results, 0..) |*r, bi| {
            for (0..r.sel_len) |i| {
                const v = r.ov[i];
                const tag = (bi << 40) | r.sel[i];
                if (len < k) {
                    sel[len] = tag;
                    ov[len] = v;
                    len += 1;
                    if (len == k) topkBuildHeap(sel, ov, tk.desc);
                } else if (v.preferred(ov[0], tk.desc)) {
                    sel[0] = tag;
                    ov[0] = v;
                    topkSiftDown(sel, ov, 0, k, tk.desc);
                }
            }
        }
        for (0..len) |i| {
            const bi = sel[i] >> 40;
            const g = sel[i] & ((1 << 40) - 1);
            const key: u128 = if (key32) @as(u128, results[bi].keys32.items[g]) else results[bi].keys.items[g];
            try self.emitOne(results[bi].state.items, g, key);
        }
    }

    fn emitOne(self: *RadixLeaseAggregate, gstate: []const u64, g: usize, key: u128) !void {
        try agg.appendIntGroupKey(self.allocator, key, self.int_layout, self.output_columns[0..self.group_col_indices.len]);
        for (self.compact.aggs, 0..) |_, ai| {
            const oi = self.group_col_indices.len + ai;
            try appendFinalized(self.allocator, finalize(self.compact, gstate, g, ai), &self.output_columns[oi], self.output_schema[oi].type);
        }
    }

    fn freeShards(self: *RadixLeaseAggregate) void {
        for (self.shard_arenas) |*ar| ar.deinit();
        if (self.shard_arenas.len > 0) self.allocator.free(self.shard_arenas);
        self.shard_arenas = &.{};
    }

    fn partitionRowCount(self: *const RadixLeaseAggregate) usize {
        var rows: usize = 0;
        if (self.useKey32Partitions()) {
            for (self.buckets) |*bucket| rows += bucket.keys32.items.len;
        } else {
            for (self.buckets) |*bucket| rows += bucket.keys.items.len;
        }
        return rows;
    }

    fn resultGroupCount(self: *const RadixLeaseAggregate, results: []const BucketResult) usize {
        var groups: usize = 0;
        if (self.useKey32Partitions()) {
            for (results) |*result| groups += result.keys32.items.len;
        } else {
            for (results) |*result| groups += result.keys.items.len;
        }
        return groups;
    }

    pub fn next(self: *RadixLeaseAggregate) !?Batch {
        if (self.done) return null;
        self.done = true;

        const prof_on = getenv("THINDB_LEASE_PROF") != null;
        if (prof_on) {
            self.prof_scan_ticks.store(0, .monotonic);
            self.prof_radix_ticks.store(0, .monotonic);
            self.prof_group_ticks.store(0, .monotonic);
            self.prof_bucket_topk_ticks.store(0, .monotonic);
        }

        const t0 = prof.nowTicks();
        try self.partition();
        const t1 = prof.nowTicks();
        const rows = if (prof_on) self.partitionRowCount() else 0;

        const results = try self.arena.allocator().alloc(BucketResult, self.n_buckets);
        for (results) |*r| r.* = .{};
        const workers = try self.allocator.alloc(LeaseWorker, self.dop);
        defer self.allocator.free(workers);
        for (workers) |*w| w.* = .{ .arena = std.heap.ArenaAllocator.init(self.allocator) };
        defer for (workers) |*w| w.arena.deinit();

        if (self.useKey32Partitions()) {
            try self.runLease32(workers, results);
        } else {
            const special_csa = if (getenv("THINDB_LEASE_CSA") != null) self.countSumAvgPlan() else null;
            switch (self.int_layout.tier) {
                .bits32 => if (special_csa) |plan| try self.runLeaseCountSumAvg(gt.IntKeyTable(32), plan, workers, results) else try self.runLease(gt.IntKeyTable(32), workers, results),
                .bits96 => if (special_csa) |plan| try self.runLeaseCountSumAvg(gt.IntKeyTable(96), plan, workers, results) else try self.runLease(gt.IntKeyTable(96), workers, results),
                .bits128 => if (special_csa) |plan| try self.runLeaseCountSumAvg(gt.IntKeyTable(128), plan, workers, results) else try self.runLease(gt.IntKeyTable(128), workers, results),
            }
        }
        for (workers) |*w| if (w.err) |e| return e;
        const t2 = prof.nowTicks();
        const groups = if (prof_on) self.resultGroupCount(results) else 0;

        // Do not tear down the per-shard partition arenas before returning the
        // result batch. Arena deinit is pure cleanup and was showing up as
        // ~100ms of query latency on large GROUP BY + LIMIT queries. The query
        // owner calls deinit immediately after draining us, where freeShards()
        // releases the same memory off the result-producing critical path.
        const tf0 = prof.nowTicks();
        const tf1 = tf0;
        try self.emitBucketResults(results);
        const t3 = prof.nowTicks();
        if (prof_on) {
            const scan_cpu = prof.ticksToMs(@intCast(self.prof_scan_ticks.load(.monotonic)));
            const radix_cpu = prof.ticksToMs(@intCast(self.prof_radix_ticks.load(.monotonic)));
            const group_cpu = prof.ticksToMs(@intCast(self.prof_group_ticks.load(.monotonic)));
            const topk_cpu = prof.ticksToMs(@intCast(self.prof_bucket_topk_ticks.load(.monotonic)));
            std.debug.print("[lease-prof] partition={d:.1}ms lease={d:.1}ms emit={d:.1}ms rows={d} groups={d} buckets={d} dop={d}\n", .{ prof.ticksToMs(t1 - t0), prof.ticksToMs(t2 - t1), prof.ticksToMs(t3 - t2), rows, groups, self.n_buckets, self.dop });
            std.debug.print("[lease-prof-detail] scan_next_cpu={d:.1}ms radix_partition_cpu={d:.1}ms group_by_cpu={d:.1}ms bucket_topk_cpu={d:.1}ms free_partitions_wall={d:.1}ms final_topk_emit_wall={d:.1}ms\n", .{ scan_cpu, radix_cpu, group_cpu, topk_cpu, prof.ticksToMs(tf1 - tf0), prof.ticksToMs(t3 - tf1) });
            std.debug.print("[group-pipeline] scan_filter_cpu={d:.1}ms radix_partition_cpu={d:.1}ms group_by_cpu={d:.1}ms order_topn_cpu={d:.1}ms wall_scan_partition={d:.1}ms wall_group_stage={d:.1}ms wall_emit_final_topn={d:.1}ms rows={d} groups={d} buckets={d} dop={d}\n", .{ scan_cpu, radix_cpu, group_cpu, topk_cpu, prof.ticksToMs(t1 - t0), prof.ticksToMs(t2 - t1), prof.ticksToMs(t3 - t2), rows, groups, self.n_buckets, self.dop });
        }

        _ = self.arena.reset(.free_all);
        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        return Batch{ .schema = self.output_schema, .values = self.views, .row_count = self.output_columns[0].rowCount() };
    }

    pub fn deinit(self: *RadixLeaseAggregate) void {
        self.upstream.deinit();
        self.freeShards(); // free if next() errored before its own freeShards
        self.arena.deinit();
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.used_cols);
        self.allocator.free(self.need_cols);
        self.allocator.free(self.key_flat_slot);
        self.allocator.free(self.val_flat_slot);
        self.compact.deinit(self.allocator);
        self.int_layout.deinit(self.allocator);
        self.allocator.free(self.agg_col_indices);
        self.allocator.free(self.group_col_indices);
        self.allocator.destroy(self);
    }

    pub fn outputSchema(self: *RadixLeaseAggregate) []const types.Column {
        return self.output_schema;
    }
    pub fn addPrune(self: *RadixLeaseAggregate, pred: predicate.Predicate) !void {
        return self.upstream.addPrune(pred);
    }
    pub fn stats(self: *RadixLeaseAggregate) PipelineStats {
        return .{ .upper_rows = self.upstream.stats().upper_rows };
    }
    pub fn accountant(self: *RadixLeaseAggregate) ?*memory.MemoryAccountant {
        return self.upstream.accountant();
    }
    pub fn explain(self: *RadixLeaseAggregate, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "RadixLeaseAggregate (partition + lease, parallel)");
        try self.upstream.explain(out, allocator, depth + 1);
    }
};

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

fn getenvUsize(name: [*:0]const u8) ?usize {
    const v = getenv(name) orelse return null;
    return std.fmt.parseInt(usize, std.mem.span(v), 10) catch null;
}

// --- differential-test support: a tiny synthetic source over (k:int, v:smallint),
// emitting in 4-row batches so the operator's multi-batch drain is exercised.
const TestSource = struct {
    schema: [2]types.Column,
    k: []const i32,
    v: []const i16,
    views: [2]ColumnView = undefined,
    pos: usize = 0,
    allocator: Allocator,

    fn create(a: Allocator, k: []const i32, v: []const i16) !*TestSource {
        const s = try a.create(TestSource);
        s.* = .{
            .schema = .{ .{ .name = "k", .type = .int }, .{ .name = "v", .type = .smallint } },
            .k = k,
            .v = v,
            .allocator = a,
        };
        return s;
    }
    pub fn next(self: *TestSource) !?Batch {
        if (self.pos >= self.k.len) return null;
        const lo = self.pos;
        const hi = @min(lo + 4, self.k.len);
        self.pos = hi;
        self.views[0] = .{ .data = .{ .int = self.k[lo..hi] } };
        self.views[1] = .{ .data = .{ .smallint = self.v[lo..hi] } };
        return Batch{ .schema = self.schema[0..], .values = self.views[0..], .row_count = hi - lo };
    }
    pub fn deinit(self: *TestSource) void {
        self.allocator.destroy(self);
    }
    pub fn outputSchema(self: *TestSource) []const types.Column {
        return self.schema[0..];
    }
    pub fn addPrune(_: *TestSource, _: predicate.Predicate) !void {}
    pub fn stats(self: *TestSource) PipelineStats {
        return .{ .upper_rows = self.k.len };
    }
    pub fn accountant(_: *TestSource) ?*memory.MemoryAccountant {
        return null;
    }
    pub fn explain(_: *TestSource, _: *std.ArrayList(u8), _: Allocator, _: usize) !void {}
};

const DiffRow = struct { k: i32, c: i64, s: i64, a: f64, mn: i16, mx: i16 };
fn diffLessK(_: void, x: DiffRow, y: DiffRow) bool {
    return x.k < y.k;
}
fn collectDiffRows(allocator: Allocator, q: *Query) ![]DiffRow {
    var list: std.ArrayListUnmanaged(DiffRow) = .empty;
    errdefer list.deinit(allocator);
    while (try q.next()) |b| {
        for (0..b.row_count) |r| try list.append(allocator, .{
            .k = b.values[0].data.int[r],
            .c = b.values[1].data.bigint[r],
            .s = b.values[2].data.bigint[r],
            .a = b.values[3].data.double[r],
            .mn = b.values[4].data.smallint[r],
            .mx = b.values[5].data.smallint[r],
        });
    }
    const rows = try list.toOwnedSlice(allocator);
    std.mem.sort(DiffRow, rows, {}, diffLessK);
    return rows;
}

test "RadixAggregate matches the generic Aggregate (count/sum/avg/min/max)" {
    const ta = std.testing.allocator;
    const k = [_]i32{ 1, 2, 1, 3, 2, 1, 3, 3 };
    const v = [_]i16{ 10, 20, 30, 40, 5, 15, 25, 35 };
    const group_cols = [_][]const u8{"k"};
    const aggs = [_]AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "v", .as = "s" },
        .{ .func = .avg, .col = "v", .as = "a" },
        .{ .func = .min, .col = "v", .as = "mn" },
        .{ .func = .max, .col = "v", .as = "mx" },
    };

    var qr = try RadixAggregate.create(ta, makeQuery(ta, try TestSource.create(ta, &k, &v)), group_cols[0..], aggs[0..], null);
    defer qr.deinit();
    const rr = try collectDiffRows(ta, &qr);
    defer ta.free(rr);

    var qa = try makeQuery(ta, try TestSource.create(ta, &k, &v)).groupBy(group_cols[0..], aggs[0..]);
    defer qa.deinit();
    const expected = try collectDiffRows(ta, &qa);
    defer ta.free(expected);

    try std.testing.expectEqual(expected.len, rr.len);
    for (expected, rr) |e, g| {
        try std.testing.expectEqual(e.k, g.k);
        try std.testing.expectEqual(e.c, g.c);
        try std.testing.expectEqual(e.s, g.s);
        try std.testing.expectApproxEqAbs(e.a, g.a, 1e-9);
        try std.testing.expectEqual(e.mn, g.mn);
        try std.testing.expectEqual(e.mx, g.mx);
    }
}

test "RadixAggregate top-k emits only the k most-preferred groups" {
    const ta = std.testing.allocator;
    // Distinct SUM(v) per group so the top-k set is unambiguous (no ties).
    // g1:sum100 g2:sum30 g3:sum5 g4:sum80 g5:sum1 → top-2 desc = {g1,g4}.
    const k = [_]i32{ 1, 2, 2, 3, 4, 4, 5 };
    const v = [_]i16{ 100, 10, 20, 5, 40, 40, 1 };
    const group_cols = [_][]const u8{"k"};
    const aggs = [_]AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "v", .as = "s" },
        .{ .func = .avg, .col = "v", .as = "a" },
        .{ .func = .min, .col = "v", .as = "mn" },
        .{ .func = .max, .col = "v", .as = "mx" },
    };

    var qr = try RadixAggregate.create(ta, makeQuery(ta, try TestSource.create(ta, &k, &v)), group_cols[0..], aggs[0..], .{ .k = 2, .col = "s", .desc = true });
    defer qr.deinit();
    const rr = try collectDiffRows(ta, &qr);
    defer ta.free(rr);

    try std.testing.expectEqual(@as(usize, 2), rr.len);
    try std.testing.expectEqual(@as(i32, 1), rr[0].k); // g1, sum 100
    try std.testing.expectEqual(@as(i64, 100), rr[0].s);
    try std.testing.expectEqual(@as(i32, 4), rr[1].k); // g4, sum 80
    try std.testing.expectEqual(@as(i64, 80), rr[1].s);
}

test "RadixLeaseAggregate 32-bit partition key encoding round-trips signed and narrow keys" {
    const ta = std.testing.allocator;

    {
        const expected = [_]i32{ -39921974, -1, 0, 17 };
        const col = types.Column{ .name = "i", .type = .int };
        const field = agg.IntKeyField{ .offset = 0, .bits = 32, .type_tag = .int };
        const batch = Batch{ .schema = &.{col}, .values = &.{.{ .data = .{ .int = &expected } }}, .row_count = expected.len };
        var keys: [expected.len]u32 = undefined;
        RadixLeaseAggregate.fillKey32(keys[0..], batch, 0, field);
        var out_cols = [_]ColumnStore{try ColumnStore.init(ta, col.type, false)};
        defer out_cols[0].deinit(ta);
        for (keys) |key| try agg.appendIntGroupKey(ta, @as(u128, key), .{ .fields = @constCast(&[_]agg.IntKeyField{field}), .tier = .bits32 }, out_cols[0..]);
        try std.testing.expectEqualSlices(i32, &expected, out_cols[0].view().data.int);
    }
    {
        const expected = [_]i16{ -32768, -1, 0, 1234 };
        const col = types.Column{ .name = "s", .type = .smallint };
        const field = agg.IntKeyField{ .offset = 0, .bits = 16, .type_tag = .smallint };
        const batch = Batch{ .schema = &.{col}, .values = &.{.{ .data = .{ .smallint = &expected } }}, .row_count = expected.len };
        var keys: [expected.len]u32 = undefined;
        RadixLeaseAggregate.fillKey32(keys[0..], batch, 0, field);
        var out_cols = [_]ColumnStore{try ColumnStore.init(ta, col.type, false)};
        defer out_cols[0].deinit(ta);
        for (keys) |key| try agg.appendIntGroupKey(ta, @as(u128, key), .{ .fields = @constCast(&[_]agg.IntKeyField{field}), .tier = .bits32 }, out_cols[0..]);
        try std.testing.expectEqualSlices(i16, &expected, out_cols[0].view().data.smallint);
    }
    {
        const expected = [_]i8{ -128, -1, 0, 12 };
        const col = types.Column{ .name = "t", .type = .tinyint };
        const field = agg.IntKeyField{ .offset = 0, .bits = 8, .type_tag = .tinyint };
        const batch = Batch{ .schema = &.{col}, .values = &.{.{ .data = .{ .tinyint = &expected } }}, .row_count = expected.len };
        var keys: [expected.len]u32 = undefined;
        RadixLeaseAggregate.fillKey32(keys[0..], batch, 0, field);
        var out_cols = [_]ColumnStore{try ColumnStore.init(ta, col.type, false)};
        defer out_cols[0].deinit(ta);
        for (keys) |key| try agg.appendIntGroupKey(ta, @as(u128, key), .{ .fields = @constCast(&[_]agg.IntKeyField{field}), .tier = .bits32 }, out_cols[0..]);
        try std.testing.expectEqualSlices(i8, &expected, out_cols[0].view().data.tinyint);
    }
    {
        const expected = [_]u8{ 0, 1, 1, 0 };
        const col = types.Column{ .name = "b", .type = .boolean };
        const field = agg.IntKeyField{ .offset = 0, .bits = 8, .type_tag = .boolean };
        const batch = Batch{ .schema = &.{col}, .values = &.{.{ .data = .{ .boolean = &expected } }}, .row_count = expected.len };
        var keys: [expected.len]u32 = undefined;
        RadixLeaseAggregate.fillKey32(keys[0..], batch, 0, field);
        var out_cols = [_]ColumnStore{try ColumnStore.init(ta, col.type, false)};
        defer out_cols[0].deinit(ta);
        for (keys) |key| try agg.appendIntGroupKey(ta, @as(u128, key), .{ .fields = @constCast(&[_]agg.IntKeyField{field}), .tier = .bits32 }, out_cols[0..]);
        try std.testing.expectEqualSlices(u8, &expected, out_cols[0].view().data.boolean);
    }
    {
        const expected = [_]i32{ -365, 0, 19000, 25000 };
        const col = types.Column{ .name = "d", .type = .date };
        const field = agg.IntKeyField{ .offset = 0, .bits = 32, .type_tag = .date };
        const batch = Batch{ .schema = &.{col}, .values = &.{.{ .data = .{ .date = &expected } }}, .row_count = expected.len };
        var keys: [expected.len]u32 = undefined;
        RadixLeaseAggregate.fillKey32(keys[0..], batch, 0, field);
        var out_cols = [_]ColumnStore{try ColumnStore.init(ta, col.type, false)};
        defer out_cols[0].deinit(ta);
        for (keys) |key| try agg.appendIntGroupKey(ta, @as(u128, key), .{ .fields = @constCast(&[_]agg.IntKeyField{field}), .tier = .bits32 }, out_cols[0..]);
        try std.testing.expectEqualSlices(i32, &expected, out_cols[0].view().data.date);
    }
}

// ---------------------------------------------------------------------------
test "compact core: COUNT(*) / SUM(int) / AVG(int) over grouped batches" {
    const ta = std.testing.allocator;
    const aggs = [_]AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "v", .as = "s" },
        .{ .func = .avg, .col = "v", .as = "a" },
    };
    const schema = [_]types.Column{
        .{ .name = "k", .type = .int },
        .{ .name = "v", .type = .smallint },
    };
    const agg_col_idx = [_]?usize{ null, 1, 1 };

    const layout = (try planCompact(ta, &aggs, &agg_col_idx, &schema)).?;
    defer layout.deinit(ta);
    try std.testing.expectEqual(@as(usize, 5), layout.words); // 1 + 2 + 2

    const state = try ta.alloc(u64, 2 * layout.words);
    defer ta.free(state);
    @memset(state, 0);

    const vvals = [_]i16{ 10, 20, 30, 40, 5 };
    const gids = [_]u32{ 0, 1, 0, 1, 0 }; // g0: 10,30,5  g1: 20,40
    const batch = Batch{
        .schema = &schema,
        .values = &.{
            .{ .data = .{ .int = &[_]i32{ 0, 0, 0, 0, 0 } } },
            .{ .data = .{ .smallint = &vvals } },
        },
        .row_count = 5,
    };
    scatter(layout, state, &gids, batch);

    try std.testing.expectEqual(@as(u64, 3), finalize(layout, state, 0, 0).int);
    try std.testing.expectEqual(@as(i128, 45), finalize(layout, state, 0, 1).sum_int);
    try std.testing.expectEqual(@as(f64, 15.0), finalize(layout, state, 0, 2).float);
    try std.testing.expectEqual(@as(u64, 2), finalize(layout, state, 1, 0).int);
    try std.testing.expectEqual(@as(i128, 60), finalize(layout, state, 1, 1).sum_int);
    try std.testing.expectEqual(@as(f64, 30.0), finalize(layout, state, 1, 2).float);
}

test "compact core: MIN / MAX (int) + STDDEV_POP over grouped batches" {
    const ta = std.testing.allocator;
    const aggs = [_]AggSpec{
        .{ .func = .min, .col = "v", .as = "mn" },
        .{ .func = .max, .col = "v", .as = "mx" },
        .{ .func = .stddev_pop, .col = "v", .as = "sd" },
    };
    const schema = [_]types.Column{.{ .name = "v", .type = .int }};
    const agg_col_idx = [_]?usize{ 0, 0, 0 };

    const layout = (try planCompact(ta, &aggs, &agg_col_idx, &schema)).?;
    defer layout.deinit(ta);
    try std.testing.expectEqual(@as(usize, 7), layout.words); // 2 + 2 + 3

    const state = try ta.alloc(u64, layout.words); // one group
    defer ta.free(state);
    @memset(state, 0);

    const vvals = [_]i32{ 10, 30, 5 }; // mean 15, var_pop 350/3, stddev sqrt
    const gids = [_]u32{ 0, 0, 0 };
    const batch = Batch{
        .schema = &schema,
        .values = &.{.{ .data = .{ .int = &vvals } }},
        .row_count = 3,
    };
    scatter(layout, state, &gids, batch);

    try std.testing.expectEqual(@as(i64, 5), finalize(layout, state, 0, 0).signed);
    try std.testing.expectEqual(@as(i64, 30), finalize(layout, state, 0, 1).signed);
    const expected_sd = @sqrt((25.0 + 225.0 + 100.0) / 3.0);
    try std.testing.expectApproxEqAbs(expected_sd, finalize(layout, state, 0, 2).float, 1e-9);
}

test "planCompact declines variable-state and string/large MIN/MAX" {
    const ta = std.testing.allocator;
    {
        const aggs = [_]AggSpec{.{ .func = .count_distinct, .col = "v", .as = "d" }};
        const schema = [_]types.Column{.{ .name = "v", .type = .int }};
        try std.testing.expect((try planCompact(ta, &aggs, &.{0}, &schema)) == null);
    }
    {
        const aggs = [_]AggSpec{.{ .func = .min, .col = "s", .as = "m" }};
        const schema = [_]types.Column{.{ .name = "s", .type = .string }};
        try std.testing.expect((try planCompact(ta, &aggs, &.{0}, &schema)) == null);
    }
}

fn genKV(ta: Allocator, n: usize, n_keys: u64) !struct { k: []i32, v: []i16 } {
    const k = try ta.alloc(i32, n);
    errdefer ta.free(k);
    const v = try ta.alloc(i16, n);
    var x: u64 = 0x1234_5678_9abc_def0;
    for (0..n) |i| {
        x = x *% 6364136223846793005 +% 1442695040888963407;
        k[i] = @intCast((x >> 20) % n_keys);
        v[i] = @intCast((x >> 33) % 1000);
    }
    return .{ .k = k, .v = v };
}

test "RadixLeaseAggregate matches the generic Aggregate (parallel, multi-bucket)" {
    const ta = std.testing.allocator;
    const data = try genKV(ta, 4000, 257); // ~257 distinct keys spread over 16 buckets
    defer ta.free(data.k);
    defer ta.free(data.v);
    const group_cols = [_][]const u8{"k"};
    const aggs = [_]AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "v", .as = "s" },
        .{ .func = .avg, .col = "v", .as = "a" },
        .{ .func = .min, .col = "v", .as = "mn" },
        .{ .func = .max, .col = "v", .as = "mx" },
    };

    force_buckets = 16;
    defer force_buckets = null;
    var qr = try RadixLeaseAggregate.create(ta, makeQuery(ta, try TestSource.create(ta, data.k, data.v)), group_cols[0..], aggs[0..], null, 4);
    defer qr.deinit();
    const rr = try collectDiffRows(ta, &qr);
    defer ta.free(rr);

    var qa = try makeQuery(ta, try TestSource.create(ta, data.k, data.v)).groupBy(group_cols[0..], aggs[0..]);
    defer qa.deinit();
    const expected = try collectDiffRows(ta, &qa);
    defer ta.free(expected);

    try std.testing.expectEqual(expected.len, rr.len);
    for (expected, rr) |e, g| {
        try std.testing.expectEqual(e.k, g.k);
        try std.testing.expectEqual(e.c, g.c);
        try std.testing.expectEqual(e.s, g.s);
        try std.testing.expectApproxEqAbs(e.a, g.a, 1e-9);
        try std.testing.expectEqual(e.mn, g.mn);
        try std.testing.expectEqual(e.mx, g.mx);
    }
}

test "RadixLeaseAggregate single-bucket + dop=1 still matches (degenerate path)" {
    const ta = std.testing.allocator;
    const data = try genKV(ta, 1500, 64);
    defer ta.free(data.k);
    defer ta.free(data.v);
    const group_cols = [_][]const u8{"k"};
    const aggs = [_]AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "v", .as = "s" },
        .{ .func = .avg, .col = "v", .as = "a" },
        .{ .func = .min, .col = "v", .as = "mn" },
        .{ .func = .max, .col = "v", .as = "mx" },
    };
    force_buckets = 1;
    defer force_buckets = null;
    var qr = try RadixLeaseAggregate.create(ta, makeQuery(ta, try TestSource.create(ta, data.k, data.v)), group_cols[0..], aggs[0..], null, 1);
    defer qr.deinit();
    const rr = try collectDiffRows(ta, &qr);
    defer ta.free(rr);

    var qa = try makeQuery(ta, try TestSource.create(ta, data.k, data.v)).groupBy(group_cols[0..], aggs[0..]);
    defer qa.deinit();
    const expected = try collectDiffRows(ta, &qa);
    defer ta.free(expected);

    try std.testing.expectEqual(expected.len, rr.len);
    for (expected, rr) |e, g| {
        try std.testing.expectEqual(e.k, g.k);
        try std.testing.expectEqual(e.c, g.c);
        try std.testing.expectEqual(e.s, g.s);
    }
}

test "RadixLeaseAggregate top-k selects the k largest across buckets" {
    const ta = std.testing.allocator;
    const data = try genKV(ta, 4000, 257);
    defer ta.free(data.k);
    defer ta.free(data.v);
    const group_cols = [_][]const u8{"k"};
    const aggs = [_]AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "v", .as = "s" },
        .{ .func = .avg, .col = "v", .as = "a" },
        .{ .func = .min, .col = "v", .as = "mn" },
        .{ .func = .max, .col = "v", .as = "mx" },
    };

    force_buckets = 16;
    defer force_buckets = null;
    var qr = try RadixLeaseAggregate.create(ta, makeQuery(ta, try TestSource.create(ta, data.k, data.v)), group_cols[0..], aggs[0..], .{ .k = 5, .col = "s", .desc = true }, 4);
    defer qr.deinit();
    const rr = try collectDiffRows(ta, &qr); // sorted by k; we re-sort by s below
    defer ta.free(rr);

    var qa = try makeQuery(ta, try TestSource.create(ta, data.k, data.v)).groupBy(group_cols[0..], aggs[0..]);
    defer qa.deinit();
    const expected = try collectDiffRows(ta, &qa);
    defer ta.free(expected);

    // The k largest SUM(v) values must match (tie-robust: compares the values,
    // not which key won a boundary tie).
    try std.testing.expectEqual(@as(usize, 5), rr.len);
    const SortS = struct {
        fn desc(_: void, a: DiffRow, b: DiffRow) bool {
            return a.s > b.s;
        }
    };
    std.mem.sort(DiffRow, expected, {}, SortS.desc);
    std.mem.sort(DiffRow, rr, {}, SortS.desc);
    for (0..5) |i| try std.testing.expectEqual(expected[i].s, rr[i].s);
}

// Source with a NULLABLE value column (every 5th row null), to exercise the
// validity-carrying scatter path that real (ClickBench) columns hit.
const TestSourceNull = struct {
    schema: [2]types.Column,
    k: []const i32,
    v: []const i16,
    views: [2]ColumnView = undefined,
    nbuf: u8 = 0,
    pos: usize = 0,
    allocator: Allocator,

    fn create(a: Allocator, k: []const i32, v: []const i16) !*TestSourceNull {
        const s = try a.create(TestSourceNull);
        s.* = .{
            .schema = .{ .{ .name = "k", .type = .int }, .{ .name = "v", .type = .smallint, .nullable = true } },
            .k = k,
            .v = v,
            .allocator = a,
        };
        return s;
    }
    pub fn next(self: *TestSourceNull) !?Batch {
        if (self.pos >= self.k.len) return null;
        const lo = self.pos;
        const hi = @min(lo + 4, self.k.len);
        self.pos = hi;
        self.nbuf = 0;
        for (lo..hi) |r| {
            if ((r % 5) != 0) self.nbuf |= (@as(u8, 1) << @intCast(r - lo)); // valid bit
        }
        self.views[0] = .{ .data = .{ .int = self.k[lo..hi] } };
        self.views[1] = .{ .data = .{ .smallint = self.v[lo..hi] }, .nulls = (&self.nbuf)[0..1] };
        return Batch{ .schema = self.schema[0..], .values = self.views[0..], .row_count = hi - lo };
    }
    pub fn deinit(self: *TestSourceNull) void {
        self.allocator.destroy(self);
    }
    pub fn outputSchema(self: *TestSourceNull) []const types.Column {
        return self.schema[0..];
    }
    pub fn addPrune(_: *TestSourceNull, _: predicate.Predicate) !void {}
    pub fn stats(self: *TestSourceNull) PipelineStats {
        return .{ .upper_rows = self.k.len };
    }
    pub fn accountant(_: *TestSourceNull) ?*memory.MemoryAccountant {
        return null;
    }
    pub fn explain(_: *TestSourceNull, _: *std.ArrayList(u8), _: Allocator, _: usize) !void {}
};

test "RadixLeaseAggregate matches generic Aggregate with NULLable agg input" {
    const ta = std.testing.allocator;
    const data = try genKV(ta, 3000, 200);
    defer ta.free(data.k);
    defer ta.free(data.v);
    const group_cols = [_][]const u8{"k"};
    const aggs = [_]AggSpec{
        .{ .func = .count, .col = null, .as = "c" }, // COUNT(*) = all rows
        .{ .func = .sum, .col = "v", .as = "s" }, // skips nulls
        .{ .func = .avg, .col = "v", .as = "a" },
        .{ .func = .min, .col = "v", .as = "mn" },
        .{ .func = .max, .col = "v", .as = "mx" },
    };
    force_buckets = 16;
    defer force_buckets = null;
    var qr = try RadixLeaseAggregate.create(ta, makeQuery(ta, try TestSourceNull.create(ta, data.k, data.v)), group_cols[0..], aggs[0..], null, 4);
    defer qr.deinit();
    const rr = try collectDiffRows(ta, &qr);
    defer ta.free(rr);

    var qa = try makeQuery(ta, try TestSourceNull.create(ta, data.k, data.v)).groupBy(group_cols[0..], aggs[0..]);
    defer qa.deinit();
    const expected = try collectDiffRows(ta, &qa);
    defer ta.free(expected);

    try std.testing.expectEqual(expected.len, rr.len);
    for (expected, rr) |e, g| {
        try std.testing.expectEqual(e.k, g.k);
        try std.testing.expectEqual(e.c, g.c);
        try std.testing.expectEqual(e.s, g.s);
        try std.testing.expectApproxEqAbs(e.a, g.a, 1e-9);
        try std.testing.expectEqual(e.mn, g.mn);
        try std.testing.expectEqual(e.mx, g.mx);
    }
}

// Larger synthetic source (2048-row batches) for the operator-level benchmark.
const BenchSource = struct {
    schema: [2]types.Column,
    k: []const i32,
    v: []const i32,
    bsz: usize,
    views: [2]ColumnView = undefined,
    pos: usize = 0,
    allocator: Allocator,
    fn create(a: Allocator, k: []const i32, v: []const i32, bsz: usize) !*BenchSource {
        const s = try a.create(BenchSource);
        s.* = .{ .schema = .{ .{ .name = "k", .type = .int }, .{ .name = "v", .type = .int } }, .k = k, .v = v, .bsz = bsz, .allocator = a };
        return s;
    }
    pub fn next(self: *BenchSource) !?Batch {
        if (self.pos >= self.k.len) return null;
        const lo = self.pos;
        const hi = @min(lo + self.bsz, self.k.len);
        self.pos = hi;
        self.views[0] = .{ .data = .{ .int = self.k[lo..hi] } };
        self.views[1] = .{ .data = .{ .int = self.v[lo..hi] } };
        return Batch{ .schema = self.schema[0..], .values = self.views[0..], .row_count = hi - lo };
    }
    pub fn deinit(self: *BenchSource) void {
        self.allocator.destroy(self);
    }
    pub fn outputSchema(self: *BenchSource) []const types.Column {
        return self.schema[0..];
    }
    pub fn addPrune(_: *BenchSource, _: predicate.Predicate) !void {}
    pub fn stats(self: *BenchSource) PipelineStats {
        return .{ .upper_rows = self.k.len };
    }
    pub fn accountant(_: *BenchSource) ?*memory.MemoryAccountant {
        return null;
    }
    pub fn explain(_: *BenchSource, _: *std.ArrayList(u8), _: Allocator, _: usize) !void {}
};

fn benchDrain(q: *Query) !struct { ms: f64, groups: usize } {
    var groups: usize = 0;
    const t0 = prof.nowTicks();
    while (try q.next()) |b| groups += b.row_count;
    return .{ .ms = prof.ticksToMs(prof.nowTicks() - t0), .groups = groups };
}

test "bench: RadixLeaseAggregate vs serial radix/generic (high-card)" {
    if (getenv("THINDB_BENCH") == null) return error.SkipZigTest;
    const ta = std.heap.page_allocator;
    const N = getenvUsize("THINDB_BENCH_N") orelse 50_000_000;
    const D = getenvUsize("THINDB_BENCH_D") orelse 10_000_000;
    const dop = getenvUsize("THINDB_BENCH_T") orelse 12;
    const nb = getenvUsize("THINDB_LEASE_BUCKETS") orelse 1024;

    const k = try ta.alloc(i32, N);
    defer ta.free(k);
    const v = try ta.alloc(i32, N);
    defer ta.free(v);
    var x: u64 = 0x9e3779b97f4a7c15;
    for (0..N) |i| {
        x = x *% 6364136223846793005 +% 1442695040888963407;
        k[i] = @intCast(@as(u64, (x >> 20) % D));
        v[i] = @intCast(@as(u64, (x >> 34) % 1000));
    }

    const gc = [_][]const u8{"k"};
    const aggs = [_]AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "v", .as = "s" },
        .{ .func = .avg, .col = "v", .as = "a" },
        .{ .func = .min, .col = "v", .as = "mn" },
        .{ .func = .max, .col = "v", .as = "mx" },
    };

    std.debug.print("\n[bench] N={d} distinct={d} dop={d} buckets={d}\n", .{ N, D, dop, nb });

    var qg = try makeQuery(ta, try BenchSource.create(ta, k, v, 2048)).groupBy(gc[0..], aggs[0..]);
    const rg = try benchDrain(&qg);
    qg.deinit();
    std.debug.print("[bench] generic Aggregate : {d:8.1} ms  groups={d}\n", .{ rg.ms, rg.groups });

    var qr = try RadixAggregate.create(ta, makeQuery(ta, try BenchSource.create(ta, k, v, 2048)), gc[0..], aggs[0..], null);
    const rr = try benchDrain(&qr);
    qr.deinit();
    std.debug.print("[bench] serial  RadixAgg   : {d:8.1} ms  groups={d}\n", .{ rr.ms, rr.groups });

    force_buckets = nb;
    defer force_buckets = null;
    var ql = try RadixLeaseAggregate.create(ta, makeQuery(ta, try BenchSource.create(ta, k, v, 2048)), gc[0..], aggs[0..], null, dop);
    const rl = try benchDrain(&ql);
    ql.deinit();
    std.debug.print("[bench] lease   (dop={d})   : {d:8.1} ms  groups={d}\n", .{ dop, rl.ms, rl.groups });

    std.debug.print("[bench] lease vs serial-radix: {d:.2}x   lease vs generic: {d:.2}x\n", .{ rr.ms / rl.ms, rg.ms / rl.ms });
    try std.testing.expectEqual(rr.groups, rl.groups);
    try std.testing.expectEqual(rg.groups, rl.groups);

    // Realistic ORDER BY <agg> DESC LIMIT 10 shape (Q32/33/34): emit is ~free
    // (10 rows), isolating partition + aggregate. force_buckets is still `nb`.
    const tk = TopK{ .k = 10, .col = "s", .desc = true };
    var qrt = try RadixAggregate.create(ta, makeQuery(ta, try BenchSource.create(ta, k, v, 2048)), gc[0..], aggs[0..], tk);
    const rrt = try benchDrain(&qrt);
    qrt.deinit();
    var qlt = try RadixLeaseAggregate.create(ta, makeQuery(ta, try BenchSource.create(ta, k, v, 2048)), gc[0..], aggs[0..], tk, dop);
    const rlt = try benchDrain(&qlt);
    qlt.deinit();
    std.debug.print("[bench] +LIMIT10: radix {d:.1}ms  lease {d:.1}ms  ({d:.2}x)\n", .{ rrt.ms, rlt.ms, rrt.ms / rlt.ms });
}
