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

/// Radix-partitioned aggregate operator (Phase 2b). Step (i): a single pre-sized
/// compact-state table — no partitioning yet (added next). Drains the upstream,
/// packs each row's int key, probes one group table, scatters via the compact
/// core, and emits group columns (decoded from the packed key) + the finalized
/// aggregates. Eligibility (int key ≤128 bits, fixed-state aggs) is the router's
/// responsibility; `create` errors if the layouts don't apply.
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
    done: bool = false,

    pub fn create(allocator: Allocator, upstream: Query, group_cols: []const []const u8, aggs: []const AggSpec) !Query {
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
        };
        return makeQuery(allocator, self);
    }

    fn drainTier(self: *RadixAggregate, comptime Table: type) !void {
        const aa = self.arena.allocator();
        const words = self.compact.words;
        const layout = self.int_layout;
        var table = try Table.init(aa, self.cap_groups);
        var gstate: std.ArrayListUnmanaged(u64) = .empty;
        var gkeys: std.ArrayListUnmanaged(u128) = .empty;
        try gstate.ensureTotalCapacity(aa, self.cap_groups * words);
        try gkeys.ensureTotalCapacity(aa, self.cap_groups);
        var kb: std.ArrayListUnmanaged(u128) = .empty;
        var hb: std.ArrayListUnmanaged(u64) = .empty;
        var gidbuf: std.ArrayListUnmanaged(u32) = .empty;
        var n_groups: u32 = 0;

        while (try self.upstream.next()) |batch| {
            const n = batch.row_count;
            if (n == 0) continue;
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
        }

        // Emit: decode each group's key + finalize its aggregates into the
        // allocator-owned output columns (independent of the arena).
        for (0..n_groups) |g| {
            try agg.appendIntGroupKey(self.allocator, gkeys.items[g], layout, self.output_columns[0..self.group_col_indices.len]);
            for (self.compact.aggs, 0..) |_, ai| {
                const oi = self.group_col_indices.len + ai;
                try appendFinalized(self.allocator, finalize(self.compact, gstate.items, g, ai), &self.output_columns[oi], self.output_schema[oi].type);
            }
        }
    }

    pub fn next(self: *RadixAggregate) !?Batch {
        if (self.done) return null;
        self.done = true;
        switch (self.int_layout.tier) {
            .bits32 => try self.drainTier(gt.IntKeyTable(32)),
            .bits96 => try self.drainTier(gt.IntKeyTable(96)),
            .bits128 => try self.drainTier(gt.IntKeyTable(128)),
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

    var qr = try RadixAggregate.create(ta, makeQuery(ta, try TestSource.create(ta, &k, &v)), group_cols[0..], aggs[0..]);
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
