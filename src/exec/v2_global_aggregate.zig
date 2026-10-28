//! V2 global-aggregate shape: table scan -> optional fused filter -> a single
//! whole-table aggregate row (no GROUP BY). One group, so there is no keying,
//! no scatter, and no staging — every surviving row folds into one accumulator.
//!
//! This is a deliberately separate handler from the group-topN core: that core's
//! machinery exists to route rows to different groups, which is pure overhead
//! when there is exactly one group. The parallel strategy here is a reduction —
//! each lane folds its share into a local accumulator, then a single thread
//! merges the lanes — built on its own scheduler (see runParallel). The scan
//! layer is reused verbatim.
//!
//! Scope: COUNT(*), COUNT(col), SUM, AVG, MIN, MAX over any int or float column,
//! COUNT(DISTINCT col) over ANY column type (int of any width, 128-bit numeric,
//! float, or string), and aggregate-over-expression, all with an optional WHERE.
//! There is NO serial fallback: a shape this handler can't run is an error, so
//! every global aggregate goes through the parallel reduce. HAVING (a 0/1-row
//! post-filter) is the one remaining decline.

const std = @import("std");
const Allocator = std.mem.Allocator;

const api = @import("../api/api.zig");
const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;
const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;
const ValueView = storage.column.ValueView;
const store = @import("../engine/store.zig");
const ColumnStore = store.ColumnStore;

const exec = @import("exec.zig");
const aggregate = @import("aggregate.zig");
const json_binary = @import("json_binary.zig");
const compute = @import("compute.zig");
const udf_mod = @import("../udf.zig");
const expr = @import("expr.zig");
const Scan = @import("scan.zig").Scan;
const RowGroupTiles = @import("rg_tiles.zig").RowGroupTiles;
const partitioned_distinct = @import("partitioned_distinct.zig");
const SiloCore = exec.silo_group_core;
const platform = @import("../util/platform.zig");
const core_scheduler = @import("../util/core_scheduler.zig");

const Batch = exec.Batch;
const Query = exec.Query;
const PredicateExpr = exec.PredicateExpr;
const AggSpec = exec.AggSpec;

pub const Request = struct {
    aggs: []const AggSpec,
    where_filter: ?PredicateExpr,
    having_filter: ?PredicateExpr,
    // Row-local derived columns (e.g. SUM(ResolutionWidth + 1)) computed in
    // the scan layer by a Compute operator before the fold sees them. Empty
    // for plain column aggregates.
    derived: []const compute.Derived,
    dop: usize,
    // Resolves UDAF aggregate names → callback entries. Null when the query has
    // no UDAFs; a UDAF without a `combine` callback declines (it can't merge
    // per-lane partials) and routes to the serial UdfAggregate fallback.
    udf_registry: ?*const udf_mod.UdfRegistry = null,
};

// A scan wrapped in an optional Compute layer (for aggregate-over-expression
// inputs). `scan` is borrowed for range resets; `drive` owns the operator
// chain (Compute → Scan, or just Scan) for iteration and teardown.
const ScanSource = struct {
    scan: *Scan,
    drive: Query,
    encoded: bool = false,

    fn next(self: *ScanSource) !?Batch {
        return self.drive.next();
    }

    fn resetRange(self: *ScanSource, start_seg: usize, start_rg: usize, end_seg: usize, end_rg: usize, scan_memtable: bool) void {
        self.scan.resetRange(start_seg, start_rg, end_seg, end_rg, scan_memtable);
    }

    fn schema(self: *ScanSource) []const Column {
        return self.drive.outputSchema();
    }

    fn deinit(self: *ScanSource) void {
        self.drive.deinit();
    }
};

fn openScanSource(
    allocator: Allocator,
    table: *api.Table,
    needed: []const []const u8,
    where_filter: ?PredicateExpr,
    derived: []const compute.Derived,
    hash_cols: []const []const u8,
    snap: ?Scan.Snapshot,
    registry: ?*const udf_mod.UdfRegistry,
) !ScanSource {
    const scan = try Scan.allocWithProjectionLoc(allocator, table, null, needed, false, snap);
    errdefer scan.deinit();
    if (where_filter) |w| {
        // Never produce a result from an unapplied filter.
        if (!try scan.tryFuseFilter(w)) return error.UnsupportedQueryShape;
    }
    // A decline (nullable column) or a sidecar-less batch falls back to
    // digesting the materialized bytes in the fold — same digest either way.
    for (hash_cols) |hc| _ = scan.setHashKeyColumn(hc);
    if (derived.len == 0) {
        return .{ .scan = scan, .drive = exec.makeQuery(allocator, scan), .encoded = where_filter == null and hash_cols.len == 0 };
    }
    const drive = try compute.Compute.createWithRegistry(allocator, exec.makeQuery(allocator, scan), derived, registry);
    return .{ .scan = scan, .drive = drive };
}

const AggOp = enum { count_star, count_col, sum, avg, min, max, count_distinct, udf };

// Max numeric/typed arg columns a global-reducer UDAF accepts.
const MAX_GLOBAL_UDF_ARGS: usize = 16;

// COUNT(DISTINCT col): the parallel reduce routes every lane's keys into one
// partition-first set per distinct field (partitioned_distinct.zig), so each key
// is inserted once and no cross-lane merge is needed; the serial path folds into
// the lane's own silo-core membership set. The full 128-bit key space is free
// (no gid to pack), so every column type maps to a lossless or
// collision-negligible u128 key — see foldDistinctGlobal.
const DistinctSet = SiloCore.DistinctSet;
const NarrowDistinct = partitioned_distinct.PartitionedDistinct(u64);
const WideDistinct = partitioned_distinct.PartitionedDistinct(u128);

// A parallel count_distinct field's shared set and one lane's producer for it,
// keyed at the field's tier width (`distinctTier`).
const SharedDistinct = union(enum) {
    narrow: *NarrowDistinct,
    wide: *WideDistinct,
};
const DistinctProducer = union(enum) {
    narrow: *NarrowDistinct.Producer,
    wide: *WideDistinct.Producer,
};

fn createProducer(comptime Shared: type, allocator: Allocator, shared: *Shared) !*Shared.Producer {
    const producer = try allocator.create(Shared.Producer);
    producer.* = .{ .shared = shared };
    return producer;
}

// How a COUNT(DISTINCT) column's value becomes its u128 set key:
//   .int    — ≤64-bit integer family, zero-extended bit pattern (lossless)
//   .wide   — 128-bit numeric (largeint / decimal128 / uuid), value as-is (lossless)
//   .float  — f32/f64 widened to f64 then bit-cast (distinct-preserving)
//   .string — 128-bit hash of the bytes (collision-negligible at ClickBench scale)
const DistinctKind = enum { int, wide, float, string };

// Map a distinct-key kind to the membership-set tier (slot width). int/float
// keys are ≤64-bit (int family zero-extended, float f64-bitcast); wide is a
// 128-bit numeric; string is a 128-bit content hash. A narrow int still uses
// the 64-bit tier because the fold zero-extends a possibly-negative i64,
// whose bit pattern needs the full 64 bits.
fn distinctTier(dkind: DistinctKind) DistinctSet.Tier {
    return switch (dkind) {
        .int, .float => .u64,
        .wide, .string => .u128,
    };
}

const AggPlan = struct {
    op: AggOp,
    // Folded input column name; null for COUNT(*). The batch column index is
    // resolved by NAME at run time — the scan projects in table-schema order,
    // not in the order columns were requested.
    input_name: ?[]const u8,
    is_float: bool,
    // MIN/MAX over a string column: the accumulator is the running extreme
    // bytes (`Lane.sstr`), not a numeric slot. Only ever set for .min/.max.
    is_string: bool = false,
    // Decimal input scale (digits after the point). SUM accumulates raw
    // mantissas — already correct at this scale — but AVG must divide the
    // mantissa-sum by 10^input_scale to recover the true mean. 0 for non-decimal.
    input_scale: u8 = 0,
    // Distinct-key derivation; meaningful only for .count_distinct.
    dkind: DistinctKind = .int,
    output_type: Type,
    name: []const u8,
    // User-defined aggregate. Each lane folds its row share into a private
    // opaque state blob (`Lane.udf_states[i]`); the single-threaded merge
    // calls `combine` to reduce the lanes (so the entry MUST provide combine —
    // resolution declines otherwise). `finalize` writes straight into the
    // output column. Args resolve by name from each batch's real ColumnViews.
    is_udf: bool = false,
    udf_entry: ?udf_mod.AggregateEntry = null,
    udf_arg_names: []const []const u8 = &.{},
};

// A per-lane partial accumulator. Aggregate i uses `isum[i]` (integer
// sum/min/max — i128, so a SUM over bigint is exact until the wrap to BIGINT
// at emit) OR
// `fsum[i]` (float sum/min/max), chosen by the aggregate's input type. `ns[i]`
// is the non-null input count (AVG denominator, COUNT(col)); `count` is the
// group's row count (COUNT(*)). Nothing here is column-specific. Per-group
// memory is irrelevant with one group, so the wide i128 accumulator is free.
const Lane = struct {
    count: u64 = 0,
    isum: []i128,
    fsum: []f64,
    ns: []u64,
    // Per-aggregate running MIN/MAX bytes for string aggregates (owned by
    // `allocator`; empty slice when the agg isn't a string min/max or no row
    // has been seen). Only slot `i` of a `.is_string` plan is ever populated.
    sstr: [][]const u8,
    // Serial path only: per-aggregate distinct membership set. Only
    // count_distinct aggs touch their slot; the others stay empty (allocate
    // nothing).
    dsets: []DistinctSet,
    // Parallel path only: per-aggregate producer into the field's shared
    // partition-first set, used instead of `dsets`. Owned by reduceParallel.
    producers: []const ?DistinctProducer = &.{},
    // Final distinct count per count_distinct agg (filled from the shared sets
    // on the parallel path, or `finalizeDistinct` on the serial path). Read by
    // emit.
    distinct_counts: []u64,
    // Per-aggregate "saw the empty string" flag for string COUNT(DISTINCT).
    // ClickBench-style data stores no-value rows as `''` (87% of SearchPhrase),
    // so the fold short-circuits them on a `len == 0` check — no hash, no set
    // probe — and the one `''` value re-enters the count as +1 at finalize.
    has_blank: []bool,
    // Per-aggregate opaque UDAF state blob (16-aligned, sized to the entry's
    // state_size). Empty slice for non-UDAF aggregates. Each lane folds into its
    // own; the merge reduces them via the entry's `combine`.
    udf_states: [][]align(16) u8,
    // Borrowed plan slice — `deinit` reads it to `destroy` the UDAF states.
    plans: []const AggPlan = &.{},
    allocator: Allocator,

    fn init(allocator: Allocator, plans: []const AggPlan) !Lane {
        const n = plans.len;
        const isum = try allocator.alloc(i128, n);
        errdefer allocator.free(isum);
        const fsum = try allocator.alloc(f64, n);
        errdefer allocator.free(fsum);
        const ns = try allocator.alloc(u64, n);
        errdefer allocator.free(ns);
        const sstr = try allocator.alloc([]const u8, n);
        errdefer allocator.free(sstr);
        const distinct_counts = try allocator.alloc(u64, n);
        errdefer allocator.free(distinct_counts);
        const has_blank = try allocator.alloc(bool, n);
        errdefer allocator.free(has_blank);
        const udf_states = try allocator.alloc([]align(16) u8, n);
        errdefer allocator.free(udf_states);
        @memset(udf_states, &.{});
        // Free any UDAF blobs already allocated if a later init fails. Frees
        // only (a failed-init state has no `destroy` contract); the success path
        // destroys them in `deinit`.
        errdefer for (udf_states) |st| if (st.len > 0) allocator.free(st);
        const dsets = try allocator.alloc(DistinctSet, n);
        errdefer allocator.free(dsets);
        @memset(isum, 0);
        @memset(fsum, 0);
        @memset(ns, 0);
        @memset(sstr, &.{});
        @memset(distinct_counts, 0);
        @memset(has_blank, false);
        @memset(dsets, .{});
        // Size each distinct field's set to its key width: a 64-bit value uses
        // 8-byte slots, a 128-bit value / string hash 16-byte ones.
        for (plans, dsets) |p, *d| {
            if (p.op == .count_distinct) d.configure(distinctTier(p.dkind));
        }
        // Allocate + init each UDAF's per-lane state.
        for (plans, 0..) |p, i| {
            if (!p.is_udf) continue;
            const entry = p.udf_entry.?;
            const blob = try allocator.alignedAlloc(u8, .@"16", entry.state_size);
            udf_states[i] = blob;
            const ctx = udf_mod.AggregateContext{ .allocator = allocator, .user_data = entry.user_data };
            try entry.init(&ctx, @ptrCast(blob.ptr));
        }
        return .{ .isum = isum, .fsum = fsum, .ns = ns, .sstr = sstr, .dsets = dsets, .distinct_counts = distinct_counts, .has_blank = has_blank, .udf_states = udf_states, .plans = plans, .allocator = allocator };
    }

    fn deinit(self: *Lane, allocator: Allocator) void {
        for (self.plans, self.udf_states) |p, st| {
            if (!p.is_udf or st.len == 0) continue;
            if (p.udf_entry) |entry| if (entry.destroy) |destroy| {
                const ctx = udf_mod.AggregateContext{ .allocator = allocator, .user_data = entry.user_data };
                destroy(&ctx, @ptrCast(st.ptr));
            };
            allocator.free(st);
        }
        allocator.free(self.udf_states);
        for (self.sstr) |s| if (s.len > 0) allocator.free(s);
        for (self.dsets) |*d| d.deinit(allocator);
        allocator.free(self.dsets);
        allocator.free(self.has_blank);
        allocator.free(self.distinct_counts);
        allocator.free(self.sstr);
        allocator.free(self.isum);
        allocator.free(self.fsum);
        allocator.free(self.ns);
    }

    // Each count_distinct agg's set size into `distinct_counts`, plus 1 for the
    // out-of-band `''` if any row carried it. Serial path only; the parallel
    // path counts the shared sets instead.
    fn finalizeDistinct(self: *Lane, plans: []const AggPlan) void {
        for (plans, 0..) |p, i| {
            if (p.op != .count_distinct) continue;
            self.distinct_counts[i] = self.dsets[i].count() + @intFromBool(self.has_blank[i]);
        }
    }

    // Replace this lane's slot-`i` min/max bytes with an owned copy of `b`.
    fn setStr(self: *Lane, i: usize, b: []const u8) !void {
        if (self.sstr[i].len > 0) self.allocator.free(self.sstr[i]);
        self.sstr[i] = try self.allocator.dupe(u8, b);
    }

    fn mergeFrom(self: *Lane, other: Lane, plans: []const AggPlan) !void {
        self.count += other.count;
        for (plans, 0..) |p, i| {
            if (p.is_udf) {
                // UDAFs don't track `ns`; their per-lane state always merges
                // (an empty lane's state is the init'd identity). combine is
                // guaranteed present — resolution declines a UDAF without it.
                const entry = p.udf_entry.?;
                const ctx = udf_mod.AggregateContext{ .allocator = self.allocator, .user_data = entry.user_data };
                try entry.combine.?(&ctx, @ptrCast(self.udf_states[i].ptr), @ptrCast(other.udf_states[i].ptr));
                continue;
            }
            if (other.ns[i] == 0) continue;
            if (p.is_string) {
                if (self.ns[i] == 0) {
                    try self.setStr(i, other.sstr[i]);
                } else {
                    const cmp = json_binary.columnOrder(p.output_type == .json, other.sstr[i], self.sstr[i]);
                    const better = if (p.op == .min) cmp == .lt else cmp == .gt;
                    if (better) try self.setStr(i, other.sstr[i]);
                }
                self.ns[i] += other.ns[i];
                continue;
            }
            if (p.op == .count_distinct) {
                // Every lane fed the same shared set, so there is nothing to
                // union; only the out-of-band blank flag folds here.
                if (other.has_blank[i]) self.has_blank[i] = true;
                continue;
            }
            const had = self.ns[i];
            self.ns[i] += other.ns[i];
            switch (p.op) {
                // Distinct unioned above; UDAF combined above — both continue'd.
                .count_distinct, .udf => unreachable,
                .count_star, .count_col => {},
                .sum, .avg => {
                    if (p.is_float) self.fsum[i] += other.fsum[i] else self.isum[i] += other.isum[i];
                },
                .min => {
                    if (p.is_float) {
                        if (had == 0 or other.fsum[i] < self.fsum[i]) self.fsum[i] = other.fsum[i];
                    } else {
                        if (had == 0 or other.isum[i] < self.isum[i]) self.isum[i] = other.isum[i];
                    }
                },
                .max => {
                    if (p.is_float) {
                        if (had == 0 or other.fsum[i] > self.fsum[i]) self.fsum[i] = other.fsum[i];
                    } else {
                        if (had == 0 or other.isum[i] > self.isum[i]) self.isum[i] = other.isum[i];
                    }
                },
            }
        }
    }
};

inline fn read128(v: ValueView, row: usize) u128 {
    return switch (v) {
        .largeint, .decimal128 => |s| @bitCast(s[row]),
        .uuid => |s| s[row],
        else => 0,
    };
}

// String distinct keys are `exec.stringKeyDigest` values (128-bit two-seed
// Wyhash — the same digest the scan's `Batch.hashed` sidecar carries, so the
// two sources interchange freely). At ~6M distinct values the birthday
// collision probability against 2^128 is negligible, so the distinct count is
// exact in practice without storing the string bytes in the set.

// Each plan dispatches ONCE to a specialized kernel — the op switch and the
// ValueView tag switch are hoisted out of the row loops, which accumulate into
// locals and fold back at the end.
fn foldUdfBatch(lane: *Lane, p: AggPlan, i: usize, batch: Batch) !void {
    const entry = p.udf_entry.?;
    var arg_buf: [MAX_GLOBAL_UDF_ARGS]ColumnView = undefined;
    if (p.udf_arg_names.len > arg_buf.len) return error.UnsupportedQueryShape;
    for (p.udf_arg_names, 0..) |nm, k| {
        const ai = batch.columnIndex(nm) orelse return error.UnsupportedQueryShape;
        arg_buf[k] = batch.values[ai];
    }
    const args = arg_buf[0..p.udf_arg_names.len];
    const ctx = udf_mod.AggregateContext{ .allocator = lane.allocator, .user_data = entry.user_data };
    const state: *anyopaque = @ptrCast(lane.udf_states[i].ptr);
    if (entry.update_batch) |update_batch| {
        try update_batch(&ctx, state, args, batch.row_count);
    } else {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) try entry.update_one(&ctx, state, args, r);
    }
}

fn foldBatch(lane: *Lane, plans: []const AggPlan, resolved: []const ?usize, batch: Batch) !void {
    lane.count += batch.row_count;
    const n = batch.row_count;
    for (plans, 0..) |p, i| {
        if (p.is_udf) {
            try foldUdfBatch(lane, p, i, batch);
            continue;
        }
        const idx = resolved[i] orelse continue;
        const view = batch.values[idx];
        if (p.op == .count_distinct) {
            const digests: ?[]const u128 = if (batch.hashed) |hs| hs[idx] else null;
            try foldDistinctGlobal(lane, p, i, view, digests, n);
            continue;
        }
        if (p.is_string) {
            // MIN/MAX over a string column: keep an owned copy of the running
            // extreme bytes. The batch's StringView is transient (recycled per
            // batch), so the kept value must be dup'd, not borrowed.
            const sv = switch (view.data) {
                .varchar, .string, .char, .json, .nested, .bitmap => |s| s,
                else => continue,
            };
            switch (p.output_type == .json) {
                inline else => |json| {
                    var r: usize = 0;
                    while (r < n) : (r += 1) {
                        if (!view.isValid(r)) continue;
                        lane.ns[i] += 1;
                        const b = sv.rowBytes(r);
                        if (lane.ns[i] == 1) {
                            try lane.setStr(i, b);
                        } else {
                            const cmp = json_binary.columnOrder(json, b, lane.sstr[i]);
                            const better = if (p.op == .min) cmp == .lt else cmp == .gt;
                            if (better) try lane.setStr(i, b);
                        }
                    }
                },
            }
            continue;
        }
        switch (p.op) {
            // count_star never resolves an input column; count_distinct, string
            // MIN/MAX, and UDAF took their own paths above.
            .count_star, .count_distinct, .udf => unreachable,
            .count_col => foldCountColGlobal(view, n, &lane.ns[i]),
            .sum, .avg => if (p.is_float)
                foldSumFloatGlobal(view, n, &lane.ns[i], &lane.fsum[i])
            else
                foldSumIntGlobal(view, n, &lane.ns[i], &lane.isum[i]),
            .min => if (p.is_float)
                foldExtremeFloatGlobal(true, view, n, &lane.ns[i], &lane.fsum[i])
            else
                foldExtremeIntGlobal(true, view, n, &lane.ns[i], &lane.isum[i]),
            .max => if (p.is_float)
                foldExtremeFloatGlobal(false, view, n, &lane.ns[i], &lane.fsum[i])
            else
                foldExtremeIntGlobal(false, view, n, &lane.ns[i], &lane.isum[i]),
        }
    }
}

fn foldCountColGlobal(view: ColumnView, n: usize, ns: *u64) void {
    if (view.nulls == null) {
        ns.* += n;
        return;
    }
    var c: u64 = 0;
    for (0..n) |r| {
        if (view.isValid(r)) c += 1;
    }
    ns.* += c;
}

fn foldSumIntGlobal(view: ColumnView, n: usize, ns: *u64, acc: *i128) void {
    switch (view.data) {
        inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |s| {
            if (view.nulls == null) {
                const T = @typeInfo(@TypeOf(s)).pointer.child;
                if (@sizeOf(T) <= 4 and n <= std.math.maxInt(u32)) {
                    var sum: i64 = 0;
                    for (s[0..n]) |v| sum += v;
                    acc.* += sum;
                } else {
                    var sum: i128 = 0;
                    for (s[0..n]) |v| sum += v;
                    acc.* += sum;
                }
                ns.* += n;
                return;
            }
            var c: u64 = 0;
            var sum: i128 = 0;
            for (0..n) |r| {
                if (!view.isValid(r)) continue;
                c += 1;
                sum += s[r];
            }
            ns.* += c;
            acc.* += sum;
        },
        else => {},
    }
}

fn foldSumFloatGlobal(view: ColumnView, n: usize, ns: *u64, acc: *f64) void {
    switch (view.data) {
        inline .float, .double => |s| {
            var c: u64 = 0;
            var sum: f64 = 0;
            for (0..n) |r| {
                if (!view.isValid(r)) continue;
                c += 1;
                sum += @floatCast(s[r]);
            }
            ns.* += c;
            acc.* += sum;
        },
        else => {},
    }
}

fn foldExtremeIntGlobal(comptime is_min: bool, view: ColumnView, n: usize, ns: *u64, acc: *i128) void {
    switch (view.data) {
        inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |s| {
            var c: u64 = 0;
            var ext: i128 = 0;
            for (0..n) |r| {
                if (!view.isValid(r)) continue;
                const v: i128 = s[r];
                const better = if (is_min) v < ext else v > ext;
                if (c == 0 or better) ext = v;
                c += 1;
            }
            if (c > 0) {
                const better = if (is_min) ext < acc.* else ext > acc.*;
                if (ns.* == 0 or better) acc.* = ext;
                ns.* += c;
            }
        },
        else => {},
    }
}

fn foldExtremeFloatGlobal(comptime is_min: bool, view: ColumnView, n: usize, ns: *u64, acc: *f64) void {
    switch (view.data) {
        inline .float, .double => |s| {
            var c: u64 = 0;
            var ext: f64 = 0;
            for (0..n) |r| {
                if (!view.isValid(r)) continue;
                const v: f64 = @floatCast(s[r]);
                const better = if (is_min) v < ext else v > ext;
                if (c == 0 or better) ext = v;
                c += 1;
            }
            if (c > 0) {
                const better = if (is_min) ext < acc.* else ext > acc.*;
                if (ns.* == 0 or better) acc.* = ext;
                ns.* += c;
            }
        },
        else => {},
    }
}

// COUNT(DISTINCT) fold: on the parallel path into the lane's producer for the
// field's shared set, on the serial path into the lane's own set through a
// `DistinctFeed`, which overlaps the set probes' cache misses.
fn foldDistinctGlobal(lane: *Lane, p: AggPlan, i: usize, view: ColumnView, digests: ?[]const u128, n: usize) !void {
    if (i < lane.producers.len) if (lane.producers[i]) |producer| switch (producer) {
        inline else => |prod| return foldDistinctInto(prod, lane, p, i, view, digests, n),
    };
    var feed: DistinctFeed = .{ .set = &lane.dsets[i], .allocator = lane.allocator };
    try foldDistinctInto(&feed, lane, p, i, view, digests, n);
    try feed.drain();
}

// The dkind dispatch happens once and each kernel runs over a typed slice.
// Every arm skips an adjacent-equal key: physical order clusters repeated
// values (UserID, phrases within a session) into runs, and an identical key
// can't be new — the count bump still happens, only the set insert is elided.
// `sink.push` takes the key at the sink's width; the field's tier makes the
// truncation lossless.
fn foldDistinctInto(sink: anytype, lane: *Lane, p: AggPlan, i: usize, view: ColumnView, digests: ?[]const u128, n: usize) !void {
    switch (p.dkind) {
        .string => {
            // Scan-emitted digests: the column is non-nullable (the scan's
            // hash gate), so every row counts; `''` keeps its fast path by
            // digest equality.
            if (digests) |ds| {
                const blank = exec.stringKeyDigest("");
                lane.ns[i] += n;
                var prev_key: u128 = 0;
                var have_prev = false;
                for (ds[0..n]) |key| {
                    if (key == blank) {
                        lane.has_blank[i] = true;
                        continue;
                    }
                    if (have_prev and key == prev_key) continue;
                    prev_key = key;
                    have_prev = true;
                    try sink.push(@truncate(key));
                }
                return;
            }
            const sv = switch (view.data) {
                .varchar, .string, .char, .json, .nested, .bitmap => |s| s,
                else => return,
            };
            var prev_key: u128 = 0;
            var have_prev = false;
            var r: usize = 0;
            while (r < n) : (r += 1) {
                if (!view.isValid(r)) continue;
                lane.ns[i] += 1;
                const b = sv.rowBytes(r);
                if (b.len == 0) {
                    lane.has_blank[i] = true;
                    continue;
                }
                const key = exec.stringKeyDigest(b);
                if (have_prev and key == prev_key) continue;
                prev_key = key;
                have_prev = true;
                try sink.push(@truncate(key));
            }
        },
        .int => switch (view.data) {
            inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |s| {
                if (view.nulls == null) {
                    lane.ns[i] += n;
                    return pushRunStarts(sink, s[0..n]);
                }
                var prev_key: u128 = 0;
                var have_prev = false;
                var r: usize = 0;
                while (r < n) : (r += 1) {
                    if (!view.isValid(r)) continue;
                    lane.ns[i] += 1;
                    const key = distinctIntKey(s[r]);
                    if (have_prev and key == prev_key) continue;
                    prev_key = key;
                    have_prev = true;
                    try sink.push(@truncate(key));
                }
            },
            else => {},
        },
        .float => switch (view.data) {
            inline .float, .double => |s| {
                var prev_key: u128 = 0;
                var have_prev = false;
                var r: usize = 0;
                while (r < n) : (r += 1) {
                    if (!view.isValid(r)) continue;
                    lane.ns[i] += 1;
                    const key = @as(u128, types.canonicalFloatBits(@as(f64, @floatCast(s[r]))));
                    if (have_prev and key == prev_key) continue;
                    prev_key = key;
                    have_prev = true;
                    try sink.push(@truncate(key));
                }
            },
            else => {},
        },
        .wide => {
            var prev_key: u128 = 0;
            var have_prev = false;
            var r: usize = 0;
            while (r < n) : (r += 1) {
                if (!view.isValid(r)) continue;
                lane.ns[i] += 1;
                const key = read128(view.data, r);
                if (have_prev and key == prev_key) continue;
                prev_key = key;
                have_prev = true;
                try sink.push(@truncate(key));
            }
        },
    }
}

inline fn distinctIntKey(value: anytype) u128 {
    return @as(u64, @bitCast(@as(i64, value)));
}

// Push the first value of each run of equal adjacent values: compare a vector
// of values with the same vector one row back and visit only the lanes that
// differ, so a repeat costs no branch of its own.
fn pushRunStarts(sink: anytype, values: anytype) !void {
    const T = @typeInfo(@TypeOf(values)).pointer.child;
    const width = std.simd.suggestVectorLength(T) orelse 8;
    const Lanes = std.meta.Int(.unsigned, width);
    if (values.len == 0) return;
    try sink.push(@truncate(distinctIntKey(values[0])));
    var r: usize = 1;
    while (r + width <= values.len) : (r += width) {
        const current: @Vector(width, T) = values[r..][0..width].*;
        const previous: @Vector(width, T) = values[r - 1 ..][0..width].*;
        var starts: Lanes = @bitCast(current != previous);
        while (starts != 0) : (starts &= starts - 1) {
            try sink.push(@truncate(distinctIntKey(values[r + @ctz(starts)])));
        }
    }
    while (r < values.len) : (r += 1) {
        if (values[r] != values[r - 1]) try sink.push(@truncate(distinctIntKey(values[r])));
    }
}

// Keeps `PREFETCH_DIST_DISTINCT` keys in flight between the serial fold and its
// set: a key's slot is prefetched when it enters and probed when it leaves. The
// look-ahead therefore counts keys that reach the set, not rows, most of which
// an adjacent repeat skips before they get here. A grow between a key's
// prefetch and its probe only wastes that one hint.
const DistinctFeed = struct {
    set: *DistinctSet,
    allocator: Allocator,
    keys: [PREFETCH_DIST_DISTINCT]u128 = undefined,
    pushed: usize = 0,

    inline fn push(self: *DistinctFeed, key: u128) !void {
        self.set.prefetchKey(key);
        const at = self.pushed % PREFETCH_DIST_DISTINCT;
        if (self.pushed >= PREFETCH_DIST_DISTINCT) _ = try self.set.insertIsNew(self.allocator, self.keys[at]);
        self.keys[at] = key;
        self.pushed += 1;
    }

    fn drain(self: *DistinctFeed) !void {
        var k = self.pushed - @min(self.pushed, PREFETCH_DIST_DISTINCT);
        while (k < self.pushed) : (k += 1) {
            _ = try self.set.insertIsNew(self.allocator, self.keys[k % PREFETCH_DIST_DISTINCT]);
        }
        self.pushed = 0;
    }
};

const TILE_RGS: usize = 16;
// Keys a `DistinctFeed` keeps in flight; a power of two keeps its ring index a
// mask.
const PREFETCH_DIST_DISTINCT: usize = 16;

const Coord = struct { seg: usize, rg: usize };

fn flatToCoord(f: usize, seg_start: []const usize, segment_count: usize, total: usize) Coord {
    if (f >= total) return .{ .seg = segment_count, .rg = 0 };
    var seg: usize = 0;
    while (seg + 1 < segment_count and seg_start[seg + 1] <= f) seg += 1;
    return .{ .seg = seg, .rg = f - seg_start[seg] };
}

const Worker = struct {
    index: usize,
    cpu: ?usize,
    source: ScanSource,
    lane: Lane,
    plans: []const AggPlan,
    resolved: []?usize,
    allocator: Allocator,
    seg_start: []const usize,
    segment_count: usize,
    total_rgs: usize,
    tiles: *RowGroupTiles,
    err: ?anyerror = null,
};

fn workerMain(w: *Worker) void {
    // Lease a core from the process-global scheduler (round-robins distinct
    // cores across ALL in-flight queries, global per-core cap) rather than
    // pinning to a per-query `cpus[worker_index]`, which would serialize N
    // concurrent single-worker queries on one core. tryAcquire (non-blocking):
    // workers coordinate, so none may block on a full bucket.
    var lease = core_scheduler.global().tryAcquire();
    defer lease.release();
    workerRun(w) catch |e| {
        w.err = e;
    };
}

// One thread of the final COUNT(DISTINCT) drain: once every lane's fold is
// done, claim partitions of each shared set until none are left.
const DistinctDrainJob = struct {
    shared: []const ?SharedDistinct,
    err: ?anyerror = null,
};

fn distinctDrainMain(job: *DistinctDrainJob) void {
    // Global core lease (non-blocking) instead of a per-query pin — see workerMain.
    var lease = core_scheduler.global().tryAcquire();
    defer lease.release();
    distinctDrainRun(job) catch |e| {
        job.err = e;
    };
}

fn distinctDrainRun(job: *DistinctDrainJob) !void {
    var narrow_spare: NarrowDistinct.Spare = .{};
    var wide_spare: WideDistinct.Spare = .{};
    for (job.shared) |slot| if (slot) |shared| switch (shared) {
        .narrow => |set| while (try set.drainNext(&narrow_spare)) |_| {},
        .wide => |set| while (try set.drainNext(&wide_spare)) |_| {},
    };
}

fn workerRun(w: *Worker) !void {
    var have_resolved = false;
    if (w.total_rgs == 0) {
        // Memtable-only (no segments / no row groups): one lane scans it all.
        if (w.index != 0) return;
        w.source.resetRange(0, 0, w.segment_count, 0, true);
        try driveTile(w, &have_resolved);
        return;
    }
    while (true) {
        try exec.memory.checkCancelled(w.allocator);
        const tile = w.tiles.claim() orelse break;
        const start = flatToCoord(tile.lo, w.seg_start, w.segment_count, w.total_rgs);
        const end = flatToCoord(tile.hi, w.seg_start, w.segment_count, w.total_rgs);
        // The final tile also drains the memtable (scan_memtable = true).
        w.source.resetRange(start.seg, start.rg, end.seg, end.rg, tile.hi == w.total_rgs);
        try driveTile(w, &have_resolved);
    }
}

const EncodedFold = struct {
    lane: *Lane,
    plans: []const AggPlan,

    pub fn consume(self: *EncodedFold, batch: @import("scan.zig").EncodedBatch) !bool {
        for (self.plans) |p| {
            if (p.is_udf or p.is_float or p.is_string or p.op == .count_distinct) return false;
            const idx = batch.column_index(p.input_name orelse continue) orelse return error.ColumnNotFound;
            const col = batch.schema[idx];
            const block = batch.blocks[idx];
            switch (block.encoding) {
                .raw => if (storage.segment_reader.viewRawColumn(col.type, block.bytes, batch.row_count, .{ .has_nulls = col.nullable }, .raw) == null) return false,
                .rle => if (col.nullable or !aggInputSupported(col.type)) return false,
                else => return false,
            }
        }
        self.lane.count += batch.row_count;
        for (self.plans, 0..) |p, i| {
            const idx = batch.column_index(p.input_name orelse continue).?;
            const col = batch.schema[idx];
            const block = batch.blocks[idx];
            if (block.encoding == .rle) {
                try fold_run_aggregate(self.lane, p, i, col.type, storage.segment_reader.rleViewOf(block.bytes, batch.row_count, .{}).block, batch.row_count);
                continue;
            }
            const view = storage.segment_reader.viewRawColumn(col.type, block.bytes, batch.row_count, .{ .has_nulls = col.nullable }, .raw).?;
            switch (p.op) {
                .count_col => foldCountColGlobal(view, batch.row_count, &self.lane.ns[i]),
                .sum, .avg => foldSumIntGlobal(view, batch.row_count, &self.lane.ns[i], &self.lane.isum[i]),
                .min => foldExtremeIntGlobal(true, view, batch.row_count, &self.lane.ns[i], &self.lane.isum[i]),
                .max => foldExtremeIntGlobal(false, view, batch.row_count, &self.lane.ns[i], &self.lane.isum[i]),
                else => unreachable,
            }
        }
        return true;
    }
};

fn fold_run_aggregate(lane: *Lane, p: AggPlan, i: usize, typ: Type, runs: storage.segment_reader.RleBlock, row_count: u32) !void {
    switch (runs.value_width) {
        inline 1, 2, 4, 8 => |width| {
            const T = std.meta.Int(.signed, width * 8);
            const Acc = if (width <= 4) i64 else i128;
            var count: u64 = 0;
            var sum: Acc = 0;
            var extreme: i64 = 0;
            for (0..runs.n_runs) |r| {
                const value: i64 = if (typ == .boolean) runs.values[r] else std.mem.readInt(T, runs.values[r * width ..][0..width], .little);
                const length = runs.runLength(r);
                if (length == 0 or length > row_count -| count) return error.CorruptColumnBlockHeader;
                switch (p.op) {
                    .sum, .avg => sum += @as(Acc, @intCast(value)) * @as(Acc, length),
                    .min => if (count == 0 or value < extreme) {
                        extreme = value;
                    },
                    .max => if (count == 0 or value > extreme) {
                        extreme = value;
                    },
                    .count_col => {},
                    else => unreachable,
                }
                count += length;
            }
            if (count != row_count) return error.CorruptColumnBlockHeader;
            if (count == 0) return;
            switch (p.op) {
                .sum, .avg => lane.isum[i] += sum,
                .min => if (lane.ns[i] == 0 or extreme < lane.isum[i]) {
                    lane.isum[i] = extreme;
                },
                .max => if (lane.ns[i] == 0 or extreme > lane.isum[i]) {
                    lane.isum[i] = extreme;
                },
                .count_col => {},
                else => unreachable,
            }
            lane.ns[i] += count;
        },
        else => return error.CorruptColumnBlockHeader,
    }
}

fn can_fold_encoded(plans: []const AggPlan) bool {
    for (plans) |p| if (p.is_udf or p.is_float or p.is_string or p.op == .count_distinct) return false;
    return true;
}

fn driveTile(w: *Worker, have_resolved: *bool) !void {
    var consumer = EncodedFold{ .lane = &w.lane, .plans = w.plans };
    const encoded = w.source.encoded and can_fold_encoded(w.plans);
    while (true) {
        try exec.memory.checkCancelled(w.allocator);
        if (encoded and try w.source.scan.consume_encoded(&consumer)) continue;
        const batch = (try w.source.next()) orelse break;
        if (!have_resolved.*) {
            try resolveBatchIndices(w.plans, w.resolved, batch);
            have_resolved.* = true;
        }
        try foldBatch(&w.lane, w.plans, w.resolved, batch);
    }
}

fn driveScan(lane: *Lane, plans: []const AggPlan, allocator: Allocator, source: *ScanSource) !void {
    const resolved = try allocator.alloc(?usize, plans.len);
    defer allocator.free(resolved);
    var have_resolved = false;
    var consumer = EncodedFold{ .lane = lane, .plans = plans };
    const encoded = source.encoded and can_fold_encoded(plans);
    while (true) {
        try exec.memory.checkCancelled(allocator);
        if (encoded and try source.scan.consume_encoded(&consumer)) continue;
        const batch = (try source.next()) orelse break;
        if (!have_resolved) {
            try resolveBatchIndices(plans, resolved, batch);
            have_resolved = true;
        }
        try foldBatch(lane, plans, resolved, batch);
    }
}

// The scan projects in table-schema order, so each aggregate's folded column
// is resolved by NAME against the batch schema (not by request order).
fn resolveBatchIndices(plans: []const AggPlan, resolved: []?usize, batch: Batch) !void {
    for (plans, 0..) |p, i| {
        resolved[i] = if (p.input_name) |nm| (batch.columnIndex(nm) orelse return error.UnsupportedQueryShape) else null;
    }
}

fn aggInputSupported(typ: Type) bool {
    return switch (typ) {
        .boolean, .tinyint, .smallint, .int, .bigint, .date, .datetime, .decimal64, .float, .double => true,
        else => false,
    };
}

fn isFloatType(typ: Type) bool {
    return typ == .float or typ == .double;
}

// Map a COUNT(DISTINCT) column type to its key-derivation kind, or null for a
// DECIMAL256, whose 256 bits no key kind holds (the generic aggregate counts
// it). Exhaustive: a new Type variant breaks this switch — a deliberate
// compile-time prompt to pick its distinct keying.
fn distinctKindFor(typ: Type) ?DistinctKind {
    return switch (typ) {
        .boolean, .tinyint, .smallint, .int, .bigint, .date, .datetime, .decimal64 => .int,
        .largeint, .decimal128, .uuid => .wide,
        .float, .double => .float,
        .varchar, .string, .char, .json, .nested, .bitmap => .string,
        .decimal256 => null,
    };
}

fn isStringType(typ: Type) bool {
    return switch (typ) {
        .varchar, .string, .char, .json, .nested, .bitmap => true,
        else => false,
    };
}

fn isDerivedName(derived: []const compute.Derived, name: []const u8) bool {
    for (derived) |d| if (types.columnNameEql(d.name, name)) return true;
    return false;
}

// Base-table type for `name` if it is a real column, else the derived
// column's computed type from the (probe-resolved) Compute output schema.
fn resolveAggInputType(table: *api.Table, schema: ?[]const Column, name: []const u8) ?Type {
    if (columnType(table, name)) |t| return t;
    if (schema) |s| {
        for (s) |c| if (types.columnNameEql(c.name, name)) return c.type;
    }
    return null;
}

// Walk a derived-column expression and project every base column it reads, so
// the scan supplies them to the Compute layer.
fn collectExprCols(allocator: Allocator, needed: *std.ArrayListUnmanaged([]const u8), table: *api.Table, e: expr.Expr) !void {
    switch (e) {
        .col_ref => |nm| _ = try addNeeded(allocator, needed, table, nm),
        .call => |c| for (c.args) |arg| try collectExprCols(allocator, needed, table, arg),
        .case => |cs| {
            for (cs.operands) |o| try collectExprCols(allocator, needed, table, o.expr);
            for (cs.branches) |b| {
                try collectPredCols(allocator, needed, table, b.cond);
                try collectExprCols(allocator, needed, table, b.then);
            }
            if (cs.else_branch) |eb| try collectExprCols(allocator, needed, table, eb.*);
        },
        .lambda => {
            var refs: std.ArrayListUnmanaged([]const u8) = .empty;
            defer refs.deinit(allocator);
            try compute.collectColumnRefs(allocator, &refs, e);
            for (refs.items) |nm| _ = try addNeeded(allocator, needed, table, nm);
        },
        else => {},
    }
}

fn collectPredCols(allocator: Allocator, needed: *std.ArrayListUnmanaged([]const u8), table: *api.Table, p: PredicateExpr) !void {
    switch (p) {
        .leaf, .day_leaf, .text_as_number => |l| _ = try addNeeded(allocator, needed, table, l.col),
        .leaf_col_col => |c| {
            _ = try addNeeded(allocator, needed, table, c.left);
            _ = try addNeeded(allocator, needed, table, c.right);
        },
        .is_null, .is_not_null => |nm| _ = try addNeeded(allocator, needed, table, nm),
        .like => |lk| _ = try addNeeded(allocator, needed, table, lk.col),
        .in_set, .text_as_number_set => |s| _ = try addNeeded(allocator, needed, table, s.col),
        .@"and", .@"or" => |kids| for (kids) |k| try collectPredCols(allocator, needed, table, k),
        .not => |k| try collectPredCols(allocator, needed, table, k.*),
        else => {},
    }
}

pub fn tryBuild(allocator: Allocator, table: *api.Table, request: Request) !?Query {
    if (request.aggs.len == 0) return null;
    // HAVING over a global aggregate is a 0/1-row post-filter; not in this cut.
    if (request.having_filter != null) return null;

    var plans = try allocator.alloc(AggPlan, request.aggs.len);
    errdefer allocator.free(plans);

    // Projected (and folded) input columns, de-duplicated. Filter-only columns
    // are sourced by the fused filter itself and never enter this projection.
    var needed: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer needed.deinit(allocator);

    // Pass 1: shape each aggregate and project its base inputs. A folded input
    // is either a real table column (projected here) or a derived column
    // produced by the Compute layer (whose base inputs are projected below).
    // sum/avg/min/max input/output types are resolved in pass 2, once a probe
    // ScanSource can report the derived columns' computed types.
    for (request.aggs, 0..) |agg, i| {
        switch (agg.func) {
            .count => {
                if (agg.col) |col_name| {
                    if (columnType(table, col_name) != null) {
                        _ = (try addNeeded(allocator, &needed, table, col_name)) orelse return declineFree(allocator, plans, &needed);
                    } else if (!isDerivedName(request.derived, col_name)) {
                        return declineFree(allocator, plans, &needed);
                    }
                    plans[i] = .{ .op = .count_col, .input_name = col_name, .is_float = false, .output_type = .bigint, .name = agg.as };
                } else {
                    plans[i] = .{ .op = .count_star, .input_name = null, .is_float = false, .output_type = .bigint, .name = agg.as };
                }
            },
            .sum, .avg, .min, .max => {
                const col_name = agg.col orelse return declineFree(allocator, plans, &needed);
                if (columnType(table, col_name) != null) {
                    _ = (try addNeeded(allocator, &needed, table, col_name)) orelse return declineFree(allocator, plans, &needed);
                } else if (!isDerivedName(request.derived, col_name)) {
                    return declineFree(allocator, plans, &needed);
                }
                plans[i] = .{
                    .op = switch (agg.func) {
                        .sum => .sum,
                        .avg => .avg,
                        .min => .min,
                        .max => .max,
                        else => unreachable,
                    },
                    .input_name = col_name,
                    .is_float = false,
                    .output_type = .bigint,
                    .name = agg.as,
                };
            },
            .count_distinct => {
                const col_name = agg.col orelse return declineFree(allocator, plans, &needed);
                if (columnType(table, col_name) != null) {
                    _ = (try addNeeded(allocator, &needed, table, col_name)) orelse return declineFree(allocator, plans, &needed);
                } else if (!isDerivedName(request.derived, col_name)) {
                    return declineFree(allocator, plans, &needed);
                }
                plans[i] = .{ .op = .count_distinct, .input_name = col_name, .is_float = false, .output_type = .bigint, .name = agg.as };
            },
            .udf => {
                // Parallel global UDAF: each lane folds a partial, merged via
                // `combine`. Decline (→ serial UdfAggregate) without a registry,
                // name, or args; the combine requirement is checked in pass 2.
                if (request.udf_registry == null) return declineFree(allocator, plans, &needed);
                if (agg.udf_name == null or agg.udf_arg_cols.len == 0 or agg.udf_arg_cols.len > MAX_GLOBAL_UDF_ARGS) {
                    return declineFree(allocator, plans, &needed);
                }
                for (agg.udf_arg_cols) |cn| {
                    if (columnType(table, cn) != null) {
                        _ = (try addNeeded(allocator, &needed, table, cn)) orelse return declineFree(allocator, plans, &needed);
                    } else if (!isDerivedName(request.derived, cn)) {
                        return declineFree(allocator, plans, &needed);
                    }
                }
                plans[i] = .{ .op = .udf, .is_udf = true, .input_name = null, .is_float = false, .output_type = .bigint, .name = agg.as, .udf_arg_names = agg.udf_arg_cols };
            },
            else => return declineFree(allocator, plans, &needed),
        }
    }

    // Project every base column read by a derived expression so the Compute
    // layer can evaluate it.
    for (request.derived) |d| {
        try collectExprCols(allocator, &needed, table, d.expr);
    }

    // A column-less COUNT(*) still needs ≥1 projected column for the scan to
    // iterate row-groups with correct visibility (an empty projection neither
    // counts correctly nor lets a WHERE fuse). Project the narrowest column to
    // minimize the decode it's forced to do.
    if (needed.items.len == 0) {
        _ = (try addNeeded(allocator, &needed, table, narrowestColumn(table))) orelse return declineFree(allocator, plans, &needed);
    }

    // Pass 2: resolve sum/avg/min/max input + output types. Derived inputs need
    // the Compute output schema, obtained from a probe ScanSource (no batch is
    // fetched — only the schema is read, then it is torn down).
    {
        var probe: ?ScanSource = null;
        defer if (probe) |*p| p.deinit();
        var probe_schema: ?[]const Column = null;
        if (request.derived.len > 0) {
            probe = try openScanSource(allocator, table, needed.items, null, request.derived, &.{}, null, request.udf_registry);
            probe_schema = probe.?.schema();
        }
        for (request.aggs, plans) |agg, *p| {
            switch (agg.func) {
                .sum, .avg, .min, .max => {
                    const ctyp = resolveAggInputType(table, probe_schema, agg.col.?) orelse return declineFree(allocator, plans, &needed);
                    if (isStringType(ctyp)) {
                        // Only MIN/MAX have string semantics; SUM/AVG over a
                        // string column is a type error → decline.
                        if (agg.func != .min and agg.func != .max) return declineFree(allocator, plans, &needed);
                        p.is_string = true;
                        p.is_float = false;
                        p.output_type = ctyp;
                    } else {
                        if (!aggInputSupported(ctyp) or aggregate.readsTemporalAsNumber(agg.func, ctyp)) return declineFree(allocator, plans, &needed);
                        const out_type = aggregate.aggOutputTypeFor(agg, ctyp) catch return declineFree(allocator, plans, &needed);
                        p.is_float = isFloatType(ctyp);
                        if (ctyp.decimalSpec()) |sp| p.input_scale = sp.s;
                        p.output_type = out_type;
                    }
                },
                .count_distinct => {
                    const ctyp = resolveAggInputType(table, probe_schema, agg.col.?) orelse return declineFree(allocator, plans, &needed);
                    p.dkind = distinctKindFor(ctyp) orelse return declineFree(allocator, plans, &needed);
                },
                .udf => {
                    const registry = request.udf_registry.?;
                    var arg_types: [MAX_GLOBAL_UDF_ARGS]Type = undefined;
                    for (p.udf_arg_names, 0..) |cn, k| {
                        arg_types[k] = resolveAggInputType(table, probe_schema, cn) orelse return declineFree(allocator, plans, &needed);
                    }
                    const entry = registry.resolveAggregateExact(agg.udf_name.?, arg_types[0..p.udf_arg_names.len]) orelse return declineFree(allocator, plans, &needed);
                    // No `combine` → can't merge per-lane partials → decline to
                    // the serial UdfAggregate fallback.
                    if (entry.combine == null) return declineFree(allocator, plans, &needed);
                    p.udf_entry = entry;
                    p.output_type = entry.return_type;
                },
                else => {},
            }
        }
    }

    // Scan-side digests for string COUNT(DISTINCT) inputs: the fold only ever
    // hashes the bytes (the digest IS the membership-set key), so the scan can
    // emit per-row digests off the cached block and the strings never
    // materialize. Excluded when a string MIN/MAX or a derived expression
    // reads the column's real bytes.
    var hash_cols: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer hash_cols.deinit(allocator);
    plan_loop: for (request.aggs, plans) |agg, p| {
        if (p.op != .count_distinct or p.dkind != .string) continue;
        const col_name = agg.col.?;
        if (columnType(table, col_name) == null) continue; // derived input — Compute reads bytes
        for (request.aggs, plans) |qa, q| {
            if (q.is_string and types.columnNameEql(qa.col.?, col_name)) continue :plan_loop;
        }
        for (request.derived) |d| {
            var refs: std.ArrayListUnmanaged([]const u8) = .empty;
            defer refs.deinit(allocator);
            collectExprCols(allocator, &refs, table, d.expr) catch continue :plan_loop;
            for (refs.items) |ref| {
                if (types.columnNameEql(ref, col_name)) continue :plan_loop;
            }
        }
        for (hash_cols.items) |existing| {
            if (types.columnNameEql(existing, col_name)) continue :plan_loop;
        }
        try hash_cols.append(allocator, col_name);
    }

    const op = try allocator.create(GlobalAggregate);
    errdefer allocator.destroy(op);
    const hash_cols_owned = try hash_cols.toOwnedSlice(allocator);
    errdefer allocator.free(hash_cols_owned);
    const needed_owned = try needed.toOwnedSlice(allocator);
    errdefer allocator.free(needed_owned);
    op.* = try GlobalAggregate.init(allocator, table, request, plans, needed_owned, hash_cols_owned);
    return exec.makeQuery(allocator, op);
}

fn declineFree(allocator: Allocator, plans: []AggPlan, needed: *std.ArrayListUnmanaged([]const u8)) ?Query {
    allocator.free(plans);
    needed.deinit(allocator);
    return null;
}

fn columnType(table: *api.Table, name: []const u8) ?Type {
    const idx = types.findColumn(table.schema.columns, name) orelse return null;
    return table.schema.columns[idx].type;
}

// Byte width for ranking the cheapest column to project; strings/variable
// types rank last so a fixed narrow column wins.
fn typeWidthRank(t: Type) usize {
    return switch (t) {
        .boolean, .tinyint => 1,
        .smallint => 2,
        .int, .date, .float => 4,
        .bigint, .datetime, .double, .decimal64 => 8,
        .largeint, .decimal128, .uuid => 16,
        else => 64,
    };
}

fn narrowestColumn(table: *api.Table) []const u8 {
    var best: usize = 0;
    var best_rank: usize = typeWidthRank(table.schema.columns[0].type);
    for (table.schema.columns, 0..) |c, i| {
        const rank = typeWidthRank(c.type);
        if (rank < best_rank) {
            best_rank = rank;
            best = i;
        }
    }
    return table.schema.columns[best].name;
}

fn addNeeded(allocator: Allocator, needed: *std.ArrayListUnmanaged([]const u8), table: *api.Table, name: []const u8) !?usize {
    if (types.findColumn(table.schema.columns, name) == null) return null;
    for (needed.items, 0..) |existing, i| {
        if (types.columnNameEql(existing, name)) return i;
    }
    const idx = needed.items.len;
    try needed.append(allocator, name);
    return idx;
}

const GlobalAggregate = struct {
    allocator: Allocator,
    table: *api.Table,
    where_filter: ?PredicateExpr,
    derived: []const compute.Derived,
    dop: usize,
    plans: []AggPlan,
    needed: []const []const u8,
    // String COUNT(DISTINCT) inputs the worker scans emit as key digests.
    hash_cols: []const []const u8,
    output_schema: []Column,
    output_cols: []ColumnStore,
    views: []ColumnView,
    emitted: bool = false,
    built: bool = false,
    row_count: usize = 0,

    udf_registry: ?*const udf_mod.UdfRegistry,
    fn init(allocator: Allocator, table: *api.Table, request: Request, plans: []AggPlan, needed: []const []const u8, hash_cols: []const []const u8) !GlobalAggregate {
        const output_schema = try allocator.alloc(Column, plans.len);
        errdefer allocator.free(output_schema);
        for (plans, 0..) |p, i| {
            // SUM/AVG/MIN/MAX over zero qualifying values is SQL NULL, so
            // those outputs are nullable; the COUNT family is always 0+.
            output_schema[i] = .{ .name = p.name, .type = p.output_type, .nullable = switch (p.op) {
                .sum, .avg, .min, .max, .udf => true,
                .count_star, .count_col, .count_distinct => false,
            } };
        }

        const output_cols = try allocator.alloc(ColumnStore, output_schema.len);
        errdefer allocator.free(output_cols);
        var built_cols: usize = 0;
        errdefer for (output_cols[0..built_cols]) |*c| c.deinit(allocator);
        for (output_schema, 0..) |col, i| {
            output_cols[i] = try ColumnStore.initCapacity(allocator, col.type, col.nullable, 1, 0);
            built_cols += 1;
        }

        const views = try allocator.alloc(ColumnView, output_schema.len);
        return .{
            .allocator = allocator,
            .udf_registry = request.udf_registry,
            .table = table,
            .where_filter = request.where_filter,
            .derived = request.derived,
            .dop = request.dop,
            .plans = plans,
            .needed = needed,
            .hash_cols = hash_cols,
            .output_schema = output_schema,
            .output_cols = output_cols,
            .views = views,
        };
    }

    pub fn deinit(self: *GlobalAggregate) void {
        for (self.output_cols) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_cols);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.plans);
        self.allocator.free(@constCast(self.needed));
        if (self.hash_cols.len > 0) self.allocator.free(@constCast(self.hash_cols));
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *GlobalAggregate) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(_: *GlobalAggregate, _: exec.Predicate) !void {}

    pub fn accountant(_: *GlobalAggregate) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn stats(_: *GlobalAggregate) exec.PipelineStats {
        return .{ .upper_rows = 1 };
    }

    pub fn explain(self: *GlobalAggregate, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "Aggregate (V2 global: parallel reduce)");
        try exec.explainIndent(out, allocator, depth + 1);
        try out.appendSlice(allocator, "Scan ");
        try out.appendSlice(allocator, self.table.name);
        try out.appendSlice(allocator, " (parallel)\n");
    }

    pub fn next(self: *GlobalAggregate) !?Batch {
        if (self.emitted) return null;
        if (!self.built) {
            try self.execute();
            self.built = true;
        }
        self.emitted = true;
        for (self.output_cols, 0..) |c, i| self.views[i] = c.view();
        return .{ .schema = self.output_schema, .values = self.views, .row_count = self.row_count };
    }

    fn execute(self: *GlobalAggregate) !void {
        var lane = if (self.dop > 1)
            try self.reduceParallel()
        else
            try self.reduceSerial();
        defer lane.deinit(self.allocator);
        try self.emitRow(lane);
        self.row_count = 1;
    }

    fn openScan(self: *GlobalAggregate, allocator: Allocator, snap: ?Scan.Snapshot) !ScanSource {
        return openScanSource(allocator, self.table, self.needed, self.where_filter, self.derived, self.hash_cols, snap, self.udf_registry);
    }

    fn reduceSerial(self: *GlobalAggregate) !Lane {
        var lane = try Lane.init(self.allocator, self.plans);
        errdefer lane.deinit(self.allocator);
        var source = try self.openScan(self.allocator, null);
        defer source.deinit();
        try driveScan(&lane, self.plans, self.allocator, &source);
        lane.finalizeDistinct(self.plans);
        return lane;
    }

    // Dedicated reduction scheduler: one scan per lane over a disjoint,
    // dynamically-claimed row-group range (balanced under zone-map skew), each
    // folding into its own accumulator, then a single-thread merge.
    fn reduceParallel(self: *GlobalAggregate) !Lane {
        const table = self.table;
        table.ddl_lock.lockSharedUncancelable(table.io);
        defer table.ddl_lock.unlockShared(table.io);

        const snap = try Scan.captureSnapshotAlloc(table, self.allocator, exec.memory.accountantOf(self.allocator));
        var pin_held = true;
        defer if (pin_held) snap.release(table);
        defer self.allocator.free(snap.segments);

        const seg_start = try self.allocator.alloc(usize, snap.segment_count + 1);
        defer self.allocator.free(seg_start);
        var total_rgs: usize = 0;
        for (snap.segments, 0..) |entry, i| {
            seg_start[i] = total_rgs;
            total_rgs += entry.row_group_count;
        }
        seg_start[snap.segment_count] = total_rgs;

        // Pin lanes round-robin across the CPU layout (physical cores first,
        // then hyperthread siblings). Capping at the logical count means
        // workers stack evenly — e.g. dop=24 on a 12-core/24-thread host pins
        // 2 lanes per physical core (one on each of its logical CPUs).
        var layout = platform.cpuLayout(self.allocator) catch platform.CpuLayout{ .order = &.{}, .physical_count = 0 };
        defer layout.deinit(self.allocator);
        const cpu_count = @max(@as(usize, 1), layout.order.len);
        var n_workers = @max(@as(usize, 1), @min(self.dop, cpu_count));
        if (total_rgs > 0) n_workers = @min(n_workers, total_rgs);

        // Right-size to the row groups that survive zone-map pruning, one
        // worker per two, as the lowcard handler does: every lane opens its own
        // scan.
        if (self.where_filter) |w| sized: {
            const probe = Scan.allocWithProjectionLoc(self.allocator, table, null, self.needed, false, snap) catch break :sized;
            defer probe.deinit();
            if (!(probe.tryFuseFilter(w) catch break :sized)) break :sized;
            const surviving = probe.survivingWorkUnits() orelse break :sized;
            n_workers = @max(@as(usize, 1), @min(n_workers, (surviving + 1) / 2));
        }
        var grant = core_scheduler.reserveWorkers(n_workers);
        defer grant.release();
        n_workers = grant.count;

        // COUNT(DISTINCT): one partition-first set per distinct field, which
        // every lane feeds through its own producer. Declared before the
        // producers so they are torn down first.
        const shared = try self.allocator.alloc(?SharedDistinct, self.plans.len);
        defer self.allocator.free(shared);
        @memset(shared, null);
        defer self.destroyShared(shared, n_workers);
        for (self.plans, shared) |p, *slot| {
            if (p.op != .count_distinct) continue;
            slot.* = if (distinctTier(p.dkind) == .u64)
                .{ .narrow = try NarrowDistinct.create(self.allocator) }
            else
                .{ .wide = try WideDistinct.create(self.allocator) };
        }
        const producers = try self.allocator.alloc(?DistinctProducer, n_workers * self.plans.len);
        defer self.allocator.free(producers);
        @memset(producers, null);
        defer for (producers) |slot| if (slot) |producer| switch (producer) {
            inline else => |prod| self.allocator.destroy(prod),
        };
        for (0..n_workers) |w| {
            for (shared, producers[w * self.plans.len ..][0..self.plans.len]) |slot, *out| {
                const s = slot orelse continue;
                out.* = switch (s) {
                    .narrow => |set| .{ .narrow = try createProducer(NarrowDistinct, self.allocator, set) },
                    .wide => |set| .{ .wide = try createProducer(WideDistinct, self.allocator, set) },
                };
            }
        }

        const workers = try self.allocator.alloc(Worker, n_workers);
        defer self.allocator.free(workers);
        var built: usize = 0;
        defer for (workers[0..built]) |*w| {
            w.source.deinit();
            w.lane.deinit(self.allocator);
            self.allocator.free(w.resolved);
        };

        var tiles: RowGroupTiles = .{};
        defer tiles.deinit(self.allocator);
        // Worker decode buffers recycle through the process buffer pool rather
        // than paying page faults and an OS release every query.
        const scan_allocator = try exec.memory.workerAllocatorOf(self.allocator);
        for (workers, 0..) |*w, i| {
            var source = try self.openScan(scan_allocator, snap);
            errdefer source.deinit();
            var lane = try Lane.init(self.allocator, self.plans);
            errdefer lane.deinit(self.allocator);
            lane.producers = producers[i * self.plans.len ..][0..self.plans.len];
            const resolved = try self.allocator.alloc(?usize, self.plans.len);
            w.* = .{
                .index = i,
                .cpu = if (layout.order.len == 0) null else layout.order[i % layout.order.len],
                .source = source,
                .lane = lane,
                .plans = self.plans,
                .resolved = resolved,
                .allocator = self.allocator,
                .seg_start = seg_start,
                .segment_count = snap.segment_count,
                .total_rgs = total_rgs,
                .tiles = &tiles,
            };
            built += 1;
        }
        // Every worker scan carries the same prune hints; cut the tiles over
        // the row groups they leave.
        {
            const mask = workers[0].source.scan.survivingMask(self.allocator);
            defer if (mask) |m| self.allocator.free(m);
            tiles = try RowGroupTiles.init(self.allocator, total_rgs, mask, n_workers, TILE_RGS);
        }
        // All worker scans now hold their own memtable pins.
        snap.release(table);
        pin_held = false;

        // Spawn every lane as its own pinned thread; the calling thread only
        // orchestrates and merges (left unpinned). A lane whose thread fails to
        // spawn runs inline unpinned — the shared atomic tile claim keeps every
        // row-group counted exactly once regardless of where lanes run.
        const threads = try self.allocator.alloc(std.Thread, n_workers);
        defer self.allocator.free(threads);
        const spawned = try self.allocator.alloc(bool, n_workers);
        defer self.allocator.free(spawned);
        @memset(spawned, false);
        for (workers, 0..) |*w, i| {
            if (std.Thread.spawn(.{}, workerMain, .{w})) |th| {
                threads[i] = th;
                spawned[i] = true;
            } else |_| {}
        }
        for (workers, 0..) |*w, i| if (!spawned[i]) {
            workerRun(w) catch |e| {
                w.err = e;
            };
        };
        for (workers, 0..) |_, i| if (spawned[i]) threads[i].join();

        for (workers) |*w| if (w.err) |e| return e;

        // Scalar aggregates: single-threaded lane-to-lane fold (cheap — one row).
        var merged = try Lane.init(self.allocator, self.plans);
        errdefer merged.deinit(self.allocator);
        for (workers) |*w| try merged.mergeFrom(w.lane, self.plans);
        // COUNT(DISTINCT): publish each lane's partial chunks, drain what is
        // still queued across the workers' cores, then count the shared sets.
        // The out-of-band `''` (folded as a boolean, never inserted into a set)
        // re-enters as one distinct value if any lane saw it.
        for (producers) |slot| if (slot) |producer| switch (producer) {
            inline else => |prod| prod.finish(),
        };
        try self.drainDistinct(shared, n_workers);
        for (shared, 0..) |slot, i| if (slot) |s| {
            const count = switch (s) {
                inline else => |set| set.count(),
            };
            merged.distinct_counts[i] = count + @intFromBool(merged.has_blank[i]);
        };
        return merged;
    }

    // A high-cardinality field's sets run to hundreds of MiB, and freeing them
    // took 10-20 ms on the result path. With an accountant the frees are
    // collected, so the query stops being charged here, and detached threads
    // hand the memory back.
    fn destroyShared(self: *GlobalAggregate, shared: []const ?SharedDistinct, n_threads: usize) void {
        const deferred = exec.memory.accountantOf(self.allocator) != null;
        var frees = exec.memory.DeferredFrees.init(n_threads);
        if (deferred) frees.collect();
        for (shared) |slot| if (slot) |s| switch (s) {
            inline else => |set| set.destroy(),
        };
        if (!deferred) return;
        frees.stop();
        frees.releaseDetached();
    }

    fn drainDistinct(self: *GlobalAggregate, shared: []const ?SharedDistinct, n_threads: usize) !void {
        for (shared) |slot| {
            if (slot != null) break;
        } else return;
        const jobs = try self.allocator.alloc(DistinctDrainJob, n_threads);
        defer self.allocator.free(jobs);
        @memset(jobs, .{ .shared = shared });
        const threads = try self.allocator.alloc(std.Thread, n_threads);
        defer self.allocator.free(threads);
        const spawned = try self.allocator.alloc(bool, n_threads);
        defer self.allocator.free(spawned);
        @memset(spawned, false);
        for (jobs, 0..) |*j, t| {
            if (std.Thread.spawn(.{}, distinctDrainMain, .{j})) |th| {
                threads[t] = th;
                spawned[t] = true;
            } else |_| {}
        }
        // Partitions are claimed atomically, so a job whose thread failed to
        // spawn just leaves its share to the others; run one inline in case
        // none spawned.
        if (std.mem.indexOfScalar(bool, spawned, false)) |t| distinctDrainRun(&jobs[t]) catch |e| {
            jobs[t].err = e;
        };
        for (0..n_threads) |t| if (spawned[t]) threads[t].join();
        for (jobs) |*j| if (j.err) |e| return e;
    }

    fn emitRow(self: *GlobalAggregate, lane: Lane) !void {
        const a = self.allocator;
        for (self.plans, 0..) |p, i| {
            const col = &self.output_cols[i];
            // A UDAF's finalize appends its own value (and may append its own
            // validity bit) straight into the typed output column.
            if (p.is_udf) {
                const before = col.data.rowCount();
                const entry = p.udf_entry.?;
                const ctx = udf_mod.AggregateContext{ .allocator = a, .user_data = entry.user_data };
                try entry.finalize(&ctx, @ptrCast(lane.udf_states[i].ptr), col);
                try ensureFinalizeValidity(a, col, before);
                continue;
            }
            const row = col.data.rowCount();
            // SUM/AVG/MIN/MAX over zero qualifying (non-NULL) inputs is SQL
            // NULL — empty scan and all-NULL column alike. COUNTs stay 0.
            var is_null = false;
            switch (p.op) {
                .udf => unreachable,
                .count_star => try col.data.bigint.append(a, @intCast(lane.count)),
                .count_col => try col.data.bigint.append(a, @intCast(lane.ns[i])),
                .count_distinct => try col.data.bigint.append(a, @intCast(lane.distinct_counts[i])),
                .sum => {
                    if (lane.ns[i] == 0) {
                        try col.data.appendNullPlaceholder(a);
                        is_null = true;
                    } else if (p.is_float) {
                        try appendFloat(a, col, p.output_type, lane.fsum[i]);
                    } else {
                        try appendInt(a, col, p.output_type, lane.isum[i]);
                    }
                },
                .avg => {
                    const n = lane.ns[i];
                    if (n == 0) {
                        try col.data.appendNullPlaceholder(a);
                        is_null = true;
                    } else {
                        const avg: f64 = if (p.is_float)
                            lane.fsum[i] / @as(f64, @floatFromInt(n))
                        else
                            // Decimal: isum is the mantissa-sum, so divide by the
                            // scale factor too. input_scale 0 for plain integers.
                            @as(f64, @floatFromInt(lane.isum[i])) / (@as(f64, @floatFromInt(n)) * std.math.pow(f64, 10.0, @floatFromInt(p.input_scale)));
                        try col.data.double.append(a, avg);
                    }
                },
                .min, .max => {
                    if (lane.ns[i] == 0) {
                        try col.data.appendNullPlaceholder(a);
                        is_null = true;
                    } else if (p.is_string) {
                        switch (p.output_type) {
                            .varchar => try col.data.varchar.appendValue(a, lane.sstr[i]),
                            .string => try col.data.string.appendValue(a, lane.sstr[i]),
                            .char => try col.data.char.appendValue(a, lane.sstr[i]),
                            .json => try col.data.json.appendValue(a, lane.sstr[i]),
                            .nested => try col.data.nested.appendValue(a, lane.sstr[i]),
                            .bitmap => try col.data.bitmap.appendValue(a, lane.sstr[i]),
                            else => return error.TypeMismatch,
                        }
                    } else if (p.is_float) {
                        try appendFloat(a, col, p.output_type, lane.fsum[i]);
                    } else {
                        try appendInt(a, col, p.output_type, lane.isum[i]);
                    }
                },
            }
            try col.appendValidBit(a, row, !is_null);
        }
    }
};

// A UDAF's finalize may or may not append its own validity bit; if it didn't,
// backfill a `true` so the column's nulls bitmap stays rectangular.
fn ensureFinalizeValidity(allocator: Allocator, col: *ColumnStore, row: usize) !void {
    const nulls = col.nulls orelse return;
    if (nulls.items.len * 8 <= row) try col.appendValidBit(allocator, row, true);
}

fn appendInt(allocator: Allocator, col: *ColumnStore, out_type: Type, value: i128) !void {
    switch (out_type) {
        .boolean => try col.data.boolean.append(allocator, @intCast(value)),
        .tinyint => try col.data.tinyint.append(allocator, @intCast(value)),
        .smallint => try col.data.smallint.append(allocator, @intCast(value)),
        .int => try col.data.int.append(allocator, @intCast(value)),
        .date => try col.data.date.append(allocator, @intCast(value)),
        // DESIGN.md §3.4: an integer SUM wraps to BIGINT (MIN/MAX always fit).
        .bigint => try col.data.bigint.append(allocator, @truncate(value)),
        .datetime => try col.data.datetime.append(allocator, @intCast(value)),
        .decimal64 => try col.data.decimal64.append(allocator, @intCast(value)),
        .decimal128 => try col.data.decimal128.append(allocator, value),
        .largeint => try col.data.largeint.append(allocator, value),
        .double => try col.data.double.append(allocator, @floatFromInt(value)),
        else => return error.TypeMismatch,
    }
}

fn appendFloat(allocator: Allocator, col: *ColumnStore, out_type: Type, value: f64) !void {
    switch (out_type) {
        .float => try col.data.float.append(allocator, @floatCast(value)),
        .double => try col.data.double.append(allocator, value),
        else => return error.TypeMismatch,
    }
}

test "encoded machinery folds integer runs with wide totals and unsigned booleans" {
    const cases = .{
        .{ .typ = Type.tinyint, .T = i8, .a = -128, .b = 127 },
        .{ .typ = Type.smallint, .T = i16, .a = -32768, .b = 32767 },
        .{ .typ = Type.int, .T = i32, .a = std.math.minInt(i32), .b = std.math.maxInt(i32) },
        .{ .typ = Type.bigint, .T = i64, .a = std.math.minInt(i64), .b = std.math.maxInt(i64) },
        .{ .typ = Type.boolean, .T = u8, .a = 0, .b = 1 },
    };
    inline for (cases) |case| {
        const plans = [_]AggPlan{
            .{ .op = .sum, .input_name = "x", .is_float = false, .output_type = .bigint, .name = "s" },
            .{ .op = .count_col, .input_name = "x", .is_float = false, .output_type = .bigint, .name = "n" },
            .{ .op = .min, .input_name = "x", .is_float = false, .output_type = case.typ, .name = "lo" },
            .{ .op = .max, .input_name = "x", .is_float = false, .output_type = case.typ, .name = "hi" },
            .{ .op = .avg, .input_name = "x", .is_float = false, .output_type = .double, .name = "mean" },
        };
        var lane = try Lane.init(std.testing.allocator, &plans);
        defer lane.deinit(std.testing.allocator);
        const width = @sizeOf(case.T);
        var bytes: [8 + 2 * width + 8]u8 align(16) = @splat(0);
        std.mem.writeInt(u32, bytes[0..4], 2, .little);
        bytes[4] = width;
        std.mem.writeInt(case.T, bytes[8..][0..width], case.a, .little);
        std.mem.writeInt(case.T, bytes[8 + width ..][0..width], case.b, .little);
        std.mem.writeInt(u32, bytes[8 + 2 * width ..][0..4], 3, .little);
        std.mem.writeInt(u32, bytes[12 + 2 * width ..][0..4], 5, .little);
        const blocks = [_]storage.ReadSegment.BorrowedBlock{.{ .bytes = &bytes, .encoding = .rle }};
        const schema = [_]Column{.{ .name = "x", .type = case.typ }};
        var consumer = EncodedFold{ .lane = &lane, .plans = &plans };
        for (0..2) |_| try std.testing.expect(try consumer.consume(.{ .schema = &schema, .blocks = &blocks, .row_count = 8 }));
        const sum = @as(i128, case.a) * 3 + @as(i128, case.b) * 5;
        try std.testing.expectEqual(sum * 2, lane.isum[0]);
        try std.testing.expectEqual(@as(u64, 16), lane.ns[1]);
        try std.testing.expectEqual(@as(i128, case.a), lane.isum[2]);
        try std.testing.expectEqual(@as(i128, case.b), lane.isum[3]);
        try std.testing.expectEqual(sum * 2, lane.isum[4]);
        try std.testing.expectEqual(@as(u64, 16), lane.count);
    }
}

test "encoded machinery decline leaves aggregate state unchanged and nullable sums stay exact" {
    const plans = [_]AggPlan{
        .{ .op = .sum, .input_name = "x", .is_float = false, .output_type = .bigint, .name = "s" },
        .{ .op = .sum, .input_name = "y", .is_float = false, .output_type = .bigint, .name = "t" },
    };
    var lane = try Lane.init(std.testing.allocator, &plans);
    defer lane.deinit(std.testing.allocator);
    const raw = [_]i16{ -5, 0, 12 };
    const blocks = [_]storage.ReadSegment.BorrowedBlock{
        .{ .bytes = std.mem.sliceAsBytes(&raw), .encoding = .raw },
        .{ .bytes = &.{}, .encoding = .for_ },
    };
    const schema = [_]Column{ .{ .name = "x", .type = .smallint }, .{ .name = "y", .type = .smallint } };
    var consumer = EncodedFold{ .lane = &lane, .plans = &plans };
    try std.testing.expect(!try consumer.consume(.{ .schema = &schema, .blocks = &blocks, .row_count = 3 }));
    try std.testing.expectEqual(@as(u64, 0), lane.count);
    try std.testing.expectEqual(@as(i128, 0), lane.isum[0]);
    const valid = [_]u8{0b00000101};
    foldSumIntGlobal(.{ .data = .{ .smallint = &raw }, .nulls = &valid }, raw.len, &lane.ns[0], &lane.isum[0]);
    try std.testing.expectEqual(@as(i128, 7), lane.isum[0]);
    try std.testing.expectEqual(@as(u64, 2), lane.ns[0]);
}
