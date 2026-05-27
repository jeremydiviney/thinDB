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
const Batch = @import("exec.zig").Batch;

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
