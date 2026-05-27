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
//! Gated to fixed-width-state aggregates; COUNT / SUM / AVG implemented first
//! (the Q32/Q30/Q31/Q09 set). MIN/MAX/STDDEV/VAR, the radix partitioning, the
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
const Batch = @import("exec.zig").Batch;

/// How one aggregate's accumulator lives inside the compact per-group record.
/// Each occupies a whole number of u64 words at `off` (word index), so the
/// record is naturally 8-byte aligned and i128 spans two words read/written via
/// `@bitCast([2]u64)` — no unaligned access.
pub const CompactKind = enum {
    count, // 1 word: u64 running count
    sum_int, // 2 words: i128 running sum (integer-family inputs)
    sum_float, // 1 word: f64 running sum (float/double inputs)
    avg, // 2 words: f64 sum, u64 count

    fn words(self: CompactKind) usize {
        return switch (self) {
            .count, .sum_float => 1,
            .sum_int, .avg => 2,
        };
    }
};

pub const CompactAgg = struct {
    kind: CompactKind,
    /// Word offset of this aggregate's slot within the per-group record.
    off: usize,
    /// Input column index in the upstream schema; null only for COUNT(*).
    col_idx: ?usize,
};

/// The compact per-group state layout for a set of aggregates. `words` is the
/// per-group stride (group `g` occupies `state[g*words .. g*words+words]`).
/// Zero-initialized state is the correct initial accumulator for every kind
/// here (count 0, sum 0, avg {0,0}).
pub const CompactLayout = struct {
    aggs: []CompactAgg,
    words: usize,

    pub fn deinit(self: CompactLayout, allocator: Allocator) void {
        allocator.free(self.aggs);
    }
};

/// Plan the compact layout for `aggs`, or return null if any aggregate is not
/// compact-eligible (variable-state, or a not-yet-implemented fixed-state kind),
/// in which case the caller falls back to the generic `Aggregate`. `agg_col_idx`
/// is the upstream column index per aggregate (null for COUNT(*)); `up_schema`
/// supplies input types for the SUM int-vs-float decision.
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
        const kind: CompactKind = switch (a.func) {
            .count => .count,
            .sum => blk: {
                const in_t = up_schema[idx.?].type;
                break :blk if (in_t.isFloat()) .sum_float else .sum_int;
            },
            .avg => .avg,
            // MIN/MAX/STDDEV/VAR (fixed-state, later increments) and the
            // variable-state aggregates all fall back to the generic operator.
            // Null is not an error, so `errdefer` won't fire — free explicitly.
            else => {
                allocator.free(out);
                return null;
            },
        };
        dst.* = .{ .kind = kind, .off = w, .col_idx = idx };
        w += kind.words();
    }
    return .{ .aggs = out, .words = w };
}

/// Accumulate one batch into the compact `state`, given each row's resolved
/// group id in `gids`. One pass per aggregate so the per-column ValueView type
/// switch is hoisted out of the row loop (matching the generic operator's
/// scatter). `state[g*layout.words ..]` must already be zeroed for every gid
/// present (the caller zeroes a group's slot when it is first created).
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

/// Canonical finalized value of one aggregate for group `g`. The output-type
/// coercion (i128 SUM → bigint with overflow check, etc.) lives in the emit
/// phase, which mirrors `aggregate.appendAccToColumn`; this returns the raw
/// accumulator result for correctness testing and as that phase's input.
pub const Finalized = union(enum) {
    int: u64, // COUNT
    sum_int: i128, // SUM over integers
    float: f64, // SUM over floats, AVG
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
    };
}

// ---------------------------------------------------------------------------
test "compact core: COUNT(*) / SUM(int) / AVG(int) over grouped batches" {
    const ta = std.testing.allocator;
    // Three aggregates: COUNT(*), SUM(v), AVG(v) where v is a smallint column.
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
    // count(1) + sum_int(2) + avg(2) = 5 words/group.
    try std.testing.expectEqual(@as(usize, 5), layout.words);

    // Two groups, gids 0 and 1. Values for v per row.
    const n_groups = 2;
    const state = try ta.alloc(u64, n_groups * layout.words);
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

    // g0: count 3, sum 45, avg 15;  g1: count 2, sum 60, avg 30.
    try std.testing.expectEqual(@as(u64, 3), finalize(layout, state, 0, 0).int);
    try std.testing.expectEqual(@as(i128, 45), finalize(layout, state, 0, 1).sum_int);
    try std.testing.expectEqual(@as(f64, 15.0), finalize(layout, state, 0, 2).float);
    try std.testing.expectEqual(@as(u64, 2), finalize(layout, state, 1, 0).int);
    try std.testing.expectEqual(@as(i128, 60), finalize(layout, state, 1, 1).sum_int);
    try std.testing.expectEqual(@as(f64, 30.0), finalize(layout, state, 1, 2).float);
}

test "planCompact declines variable-state aggregates" {
    const ta = std.testing.allocator;
    const aggs = [_]AggSpec{.{ .func = .count_distinct, .col = "v", .as = "d" }};
    const schema = [_]types.Column{.{ .name = "v", .type = .int }};
    const agg_col_idx = [_]?usize{0};
    try std.testing.expect((try planCompact(ta, &aggs, &agg_col_idx, &schema)) == null);
}
