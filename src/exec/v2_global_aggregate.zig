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
const compute = @import("compute.zig");
const expr = @import("expr.zig");
const Scan = @import("scan.zig").Scan;
const HarnessCore = exec.group_topn_harness_core;
const group_table = exec.group_table;

// Partition a distinct key into one of `parts` buckets via Lemire's multiply-
// shift reduction — `(hash * parts) >> 64` maps a uniform 64-bit hash into
// `[0, parts)` for ANY `parts` (no power-of-two constraint, no division), with
// the same uniform spread as a modulo. A key lands in the same partition in
// every worker, so the per-partition merges are over disjoint key sets and
// their distinct counts simply sum.
//
// The hash is sized to the tier — a 64-bit `UserID` mixes 64 bits, not a full
// u128 — using the SAME hash each set buckets on internally. Multiply-shift
// reads the hash's HIGH bits while the set's `& mask` reads the LOW bits, so
// the partition split and the in-table probe stay independent.
inline fn distinctPartition(tier: DistinctSet.Tier, key: u128, parts: usize) usize {
    const h: u64 = switch (tier) {
        .u32 => group_table.mix64(@as(u64, @as(u32, @truncate(key)))),
        .u64 => group_table.mix64(@truncate(key)),
        .u96 => group_table.Key96.fromU128(key).hash(),
        .u128 => group_table.hashU128(key),
    };
    return @intCast((@as(u128, h) *% @as(u128, parts)) >> 64);
}

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
};

// A scan wrapped in an optional Compute layer (for aggregate-over-expression
// inputs). `scan` is borrowed for range resets; `drive` owns the operator
// chain (Compute → Scan, or just Scan) for iteration and teardown.
const ScanSource = struct {
    scan: *Scan,
    drive: Query,

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
        return .{ .scan = scan, .drive = exec.makeQuery(allocator, scan) };
    }
    const drive = try compute.Compute.create(allocator, exec.makeQuery(allocator, scan), derived);
    return .{ .scan = scan, .drive = drive };
}

const AggOp = enum { count_star, count_col, sum, avg, min, max, count_distinct };

// COUNT(DISTINCT col): reuse the silo core's membership set. With a single global
// group there's no key to partition on, so each lane builds a partial set over
// its row-group share (NULLs skipped) and the single-threaded merge layer unions
// the lanes per distinct field via DistinctSet.mergeInto. The full 128-bit key
// space is free (no gid to pack), so every column type maps to a lossless or
// collision-negligible u128 key — see foldDistinctGlobal.
const DistinctSet = HarnessCore.DistinctSet;

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
    // Distinct-key derivation; meaningful only for .count_distinct.
    dkind: DistinctKind = .int,
    output_type: Type,
    name: []const u8,
};

// A per-lane partial accumulator. Aggregate i uses `isum[i]` (integer
// sum/min/max — i128, so a 100M-row SUM over bigint can't overflow) OR
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
    // Per-aggregate distinct membership sets, `parts` of them per aggregate
    // (indexed `[i * parts + part]`), so a count_distinct fold scatters its
    // values across `parts` private sub-tables by key-hash. Only count_distinct
    // aggs touch their slots; other aggs' slots stay empty (allocate nothing).
    // The partitioning lets the cross-worker merge run one partition per thread
    // (disjoint key sets), replacing the single-threaded union.
    dsets: []DistinctSet,
    parts: usize,
    // Final distinct count per count_distinct agg (filled by the parallel
    // partition merge, or `finalizeDistinct` on the serial path). Read by emit.
    distinct_counts: []u64,
    // Per-aggregate "saw the empty string" flag for string COUNT(DISTINCT).
    // ClickBench-style data stores no-value rows as `''` (87% of SearchPhrase),
    // so the fold short-circuits them on a `len == 0` check — no hash, no set
    // probe — and the one `''` value re-enters the count as +1 at finalize.
    has_blank: []bool,
    allocator: Allocator,

    fn init(allocator: Allocator, plans: []const AggPlan, parts: usize) !Lane {
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
        const dsets = try allocator.alloc(DistinctSet, n * parts);
        @memset(isum, 0);
        @memset(fsum, 0);
        @memset(ns, 0);
        @memset(sstr, &.{});
        @memset(distinct_counts, 0);
        @memset(has_blank, false);
        @memset(dsets, .{});
        // Size each distinct field's sub-tables to its key width: a 64-bit value
        // uses 8-byte-slot sets, a 128-bit value / string hash 16-byte ones.
        for (plans, 0..) |p, i| {
            if (p.op != .count_distinct) continue;
            for (dsets[i * parts ..][0..parts]) |*d| d.configure(distinctTier(p.dkind));
        }
        return .{ .isum = isum, .fsum = fsum, .ns = ns, .sstr = sstr, .dsets = dsets, .parts = parts, .distinct_counts = distinct_counts, .has_blank = has_blank, .allocator = allocator };
    }

    fn deinit(self: *Lane, allocator: Allocator) void {
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

    // Sum each count_distinct agg's `parts` sub-tables into `distinct_counts`,
    // plus 1 for the out-of-band `''` if any row carried it. Used by the serial
    // path (one lane, `parts == 1`); the parallel path fills `distinct_counts`
    // from the cross-worker partition merge instead.
    fn finalizeDistinct(self: *Lane, plans: []const AggPlan) void {
        for (plans, 0..) |p, i| {
            if (p.op != .count_distinct) continue;
            var total: u64 = @intFromBool(self.has_blank[i]);
            for (self.dsets[i * self.parts ..][0..self.parts]) |d| total += d.count();
            self.distinct_counts[i] = total;
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
            if (other.ns[i] == 0) continue;
            if (p.is_string) {
                if (self.ns[i] == 0) {
                    try self.setStr(i, other.sstr[i]);
                } else {
                    const cmp = std.mem.order(u8, other.sstr[i], self.sstr[i]);
                    const better = if (p.op == .min) cmp == .lt else cmp == .gt;
                    if (better) try self.setStr(i, other.sstr[i]);
                }
                self.ns[i] += other.ns[i];
                continue;
            }
            if (p.op == .count_distinct) {
                // Distinct sets are unioned by the parallel partition merge, not
                // here; this lane-to-lane merge only folds the out-of-band blank
                // flag and leaves the sets to the partition pass.
                if (other.has_blank[i]) self.has_blank[i] = true;
                continue;
            }
            const had = self.ns[i];
            self.ns[i] += other.ns[i];
            switch (p.op) {
                // Unioned above via DistinctSet.mergeInto before this switch.
                .count_distinct => unreachable,
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
fn foldBatch(lane: *Lane, plans: []const AggPlan, resolved: []const ?usize, batch: Batch) !void {
    lane.count += batch.row_count;
    const n = batch.row_count;
    for (plans, 0..) |p, i| {
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
                .varchar, .string, .char => |s| s,
                else => continue,
            };
            var r: usize = 0;
            while (r < n) : (r += 1) {
                if (!view.isValid(r)) continue;
                lane.ns[i] += 1;
                const b = sv.rowBytes(r);
                if (lane.ns[i] == 1) {
                    try lane.setStr(i, b);
                } else {
                    const cmp = std.mem.order(u8, b, lane.sstr[i]);
                    const better = if (p.op == .min) cmp == .lt else cmp == .gt;
                    if (better) try lane.setStr(i, b);
                }
            }
            continue;
        }
        switch (p.op) {
            // count_star never resolves an input column; count_distinct and
            // string MIN/MAX took their own paths above.
            .count_star, .count_distinct => unreachable,
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
    var c: u64 = 0;
    for (0..n) |r| {
        if (view.isValid(r)) c += 1;
    }
    ns.* += c;
}

fn foldSumIntGlobal(view: ColumnView, n: usize, ns: *u64, acc: *i128) void {
    switch (view.data) {
        inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |s| {
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

// COUNT(DISTINCT) fold: scatter values into the lane's partition sub-tables.
// The dkind dispatch happens once, each kernel runs over a typed slice, and
// the ≤64-bit kernels prefetch the set slot a look-ahead window ahead (the
// probe is the cache-miss bottleneck; a grow between a prefetch and its
// insert only wastes that one hint). Strings keep the `''` fast path — their
// per-row cost is the Wyhash itself, which a prefetch can't hide without
// hashing twice.
fn foldDistinctGlobal(lane: *Lane, p: AggPlan, i: usize, view: ColumnView, digests: ?[]const u128, n: usize) !void {
    const tier = distinctTier(p.dkind);
    const parts = lane.parts;
    const dsets = lane.dsets[i * parts ..][0..parts];
    switch (p.dkind) {
        .string => {
            // Scan-emitted digests: the column is non-nullable (the scan's
            // hash gate), so every row counts; `''` keeps its fast path by
            // digest equality, and the precomputed key finally makes the
            // set-slot prefetch possible (hashing twice used to rule it out).
            // All four arms below skip an adjacent-equal key: physical order
            // clusters repeated values (UserID, phrases within a session) into
            // runs, and an identical key can't be new — the count bump still
            // happens, only the cache-missing set probe is elided.
            if (digests) |ds| {
                const blank = exec.stringKeyDigest("");
                lane.ns[i] += n;
                var prev_key: u128 = 0;
                var have_prev = false;
                var r: usize = 0;
                while (r < n) : (r += 1) {
                    const key = ds[r];
                    if (key == blank) {
                        lane.has_blank[i] = true;
                        continue;
                    }
                    if (have_prev and key == prev_key) continue;
                    prev_key = key;
                    have_prev = true;
                    const pf = r + PREFETCH_DIST_DISTINCT;
                    if (pf < n and ds[pf] != blank) {
                        dsets[distinctPartition(tier, ds[pf], parts)].prefetchKey(ds[pf]);
                    }
                    _ = try dsets[distinctPartition(tier, key, parts)].insertIsNew(lane.allocator, key);
                }
                return;
            }
            const sv = switch (view.data) {
                .varchar, .string, .char => |s| s,
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
                _ = try dsets[distinctPartition(tier, key, parts)].insertIsNew(lane.allocator, key);
            }
        },
        .int => switch (view.data) {
            inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |s| {
                var prev_key: u128 = 0;
                var have_prev = false;
                var r: usize = 0;
                while (r < n) : (r += 1) {
                    if (!view.isValid(r)) continue;
                    lane.ns[i] += 1;
                    const key = @as(u128, @as(u64, @bitCast(@as(i64, s[r]))));
                    if (have_prev and key == prev_key) continue;
                    prev_key = key;
                    have_prev = true;
                    const pf = r + PREFETCH_DIST_DISTINCT;
                    if (pf < n and view.isValid(pf)) {
                        const k_pf = @as(u128, @as(u64, @bitCast(@as(i64, s[pf]))));
                        dsets[distinctPartition(tier, k_pf, parts)].prefetchKey(k_pf);
                    }
                    _ = try dsets[distinctPartition(tier, key, parts)].insertIsNew(lane.allocator, key);
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
                    const key = @as(u128, @as(u64, @bitCast(@as(f64, @floatCast(s[r])))));
                    if (have_prev and key == prev_key) continue;
                    prev_key = key;
                    have_prev = true;
                    const pf = r + PREFETCH_DIST_DISTINCT;
                    if (pf < n and view.isValid(pf)) {
                        const k_pf = @as(u128, @as(u64, @bitCast(@as(f64, @floatCast(s[pf])))));
                        dsets[distinctPartition(tier, k_pf, parts)].prefetchKey(k_pf);
                    }
                    _ = try dsets[distinctPartition(tier, key, parts)].insertIsNew(lane.allocator, key);
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
                const pf = r + PREFETCH_DIST_DISTINCT;
                if (pf < n and view.isValid(pf)) {
                    const k_pf = read128(view.data, pf);
                    dsets[distinctPartition(tier, k_pf, parts)].prefetchKey(k_pf);
                }
                _ = try dsets[distinctPartition(tier, key, parts)].insertIsNew(lane.allocator, key);
            }
        },
    }
}

const TILE_RGS: usize = 16;
// Look-ahead distance for the COUNT(DISTINCT) fold's set-slot prefetch.
const PREFETCH_DIST_DISTINCT: usize = 24;

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
    next_rg: *std.atomic.Value(usize),
    err: ?anyerror = null,
};

fn workerMain(w: *Worker) void {
    if (w.cpu) |cpu| HarnessCore.pinToCpu(cpu);
    workerRun(w) catch |e| {
        w.err = e;
    };
}

// One partition of the cross-worker COUNT(DISTINCT) merge: union column `part`
// (`workers[*].lane.dsets[i*parts + part]`) of every count_distinct agg into a
// fresh set and record each agg's distinct count for this partition.
const PartMergeJob = struct {
    part: usize,
    parts: usize,
    workers: []Worker,
    plans: []const AggPlan,
    allocator: Allocator,
    cpu: ?usize,
    counts: []u64,
    err: ?anyerror = null,
};

fn partMergeMain(job: *PartMergeJob) void {
    if (job.cpu) |cpu| HarnessCore.pinToCpu(cpu);
    partMergeRun(job) catch |e| {
        job.err = e;
    };
}

fn partMergeRun(job: *PartMergeJob) !void {
    for (job.plans, 0..) |p, i| {
        if (p.op != .count_distinct) continue;
        var out: DistinctSet = .{};
        out.configure(distinctTier(p.dkind));
        defer out.deinit(job.allocator);
        // Presize once to the sum of the per-worker partition counts (the union's
        // upper bound, since a key can sit in several workers). `capacityFor`
        // folds in the load-factor headroom, so the per-worker unions below
        // never grow or rehash mid-merge — one allocation, zero rehashing.
        var total: usize = 0;
        for (job.workers) |*w| total += w.lane.dsets[i * job.parts + job.part].count();
        try out.ensureForBatch(job.allocator, total);
        for (job.workers) |*w| {
            try out.mergeInto(job.allocator, &w.lane.dsets[i * job.parts + job.part]);
        }
        job.counts[i] = out.count();
    }
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
        const lo = w.next_rg.fetchAdd(TILE_RGS, .monotonic);
        if (lo >= w.total_rgs) break;
        const hi = @min(lo + TILE_RGS, w.total_rgs);
        const start = flatToCoord(lo, w.seg_start, w.segment_count, w.total_rgs);
        const end = flatToCoord(hi, w.seg_start, w.segment_count, w.total_rgs);
        // The final tile also drains the memtable (scan_memtable = true).
        w.source.resetRange(start.seg, start.rg, end.seg, end.rg, hi == w.total_rgs);
        try driveTile(w, &have_resolved);
    }
}

fn driveTile(w: *Worker, have_resolved: *bool) !void {
    while (try w.source.next()) |batch| {
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
    while (try source.next()) |batch| {
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

// Map a COUNT(DISTINCT) column type to its key-derivation kind. Exhaustive: the
// handler supports distinct over every column type, so it never declines on type
// grounds. A new Type variant breaks this switch — a deliberate compile-time
// prompt to pick its distinct keying.
fn distinctKindFor(typ: Type) DistinctKind {
    return switch (typ) {
        .boolean, .tinyint, .smallint, .int, .bigint, .date, .datetime, .decimal64 => .int,
        .largeint, .decimal128, .uuid => .wide,
        .float, .double => .float,
        .varchar, .string, .char => .string,
    };
}

fn isStringType(typ: Type) bool {
    return switch (typ) {
        .varchar, .string, .char => true,
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
            for (cs.branches) |b| {
                try collectPredCols(allocator, needed, table, b.cond);
                try collectExprCols(allocator, needed, table, b.then);
            }
            if (cs.else_branch) |eb| try collectExprCols(allocator, needed, table, eb.*);
        },
        else => {},
    }
}

fn collectPredCols(allocator: Allocator, needed: *std.ArrayListUnmanaged([]const u8), table: *api.Table, p: PredicateExpr) !void {
    switch (p) {
        .leaf => |l| _ = try addNeeded(allocator, needed, table, l.col),
        .leaf_col_col => |c| {
            _ = try addNeeded(allocator, needed, table, c.left);
            _ = try addNeeded(allocator, needed, table, c.right);
        },
        .is_null, .is_not_null => |nm| _ = try addNeeded(allocator, needed, table, nm),
        .like => |lk| _ = try addNeeded(allocator, needed, table, lk.col),
        .in_set => |s| _ = try addNeeded(allocator, needed, table, s.col),
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
            probe = try openScanSource(allocator, table, needed.items, null, request.derived, &.{}, null);
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
                        if (!aggInputSupported(ctyp)) return declineFree(allocator, plans, &needed);
                        const out_type = aggregate.aggOutputTypeFor(agg, ctyp) catch return declineFree(allocator, plans, &needed);
                        p.is_float = isFloatType(ctyp);
                        // The affine-aggregate reduction pins a base SUM to
                        // largeint so the post-agg `a·SUM + b·COUNT` derivation
                        // runs in i128 without an intermediate narrow.
                        p.output_type = agg.out_type_override orelse out_type;
                    }
                },
                .count_distinct => {
                    const ctyp = resolveAggInputType(table, probe_schema, agg.col.?) orelse return declineFree(allocator, plans, &needed);
                    p.dkind = distinctKindFor(ctyp);
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
    op.* = try GlobalAggregate.init(allocator, table, request, plans, try needed.toOwnedSlice(allocator), hash_cols_owned);
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

    fn init(allocator: Allocator, table: *api.Table, request: Request, plans: []AggPlan, needed: []const []const u8, hash_cols: []const []const u8) !GlobalAggregate {
        const output_schema = try allocator.alloc(Column, plans.len);
        errdefer allocator.free(output_schema);
        for (plans, 0..) |p, i| {
            // SUM/AVG/MIN/MAX over zero qualifying values is SQL NULL, so
            // those outputs are nullable; the COUNT family is always 0+.
            output_schema[i] = .{ .name = p.name, .type = p.output_type, .nullable = switch (p.op) {
                .sum, .avg, .min, .max => true,
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

    fn openScan(self: *GlobalAggregate, snap: ?Scan.Snapshot) !ScanSource {
        return openScanSource(self.allocator, self.table, self.needed, self.where_filter, self.derived, self.hash_cols, snap);
    }

    fn reduceSerial(self: *GlobalAggregate) !Lane {
        // One lane → one partition; the fold scatters into `parts == 1` and
        // `finalizeDistinct` just reads back that single sub-table's count.
        var lane = try Lane.init(self.allocator, self.plans, 1);
        errdefer lane.deinit(self.allocator);
        var source = try self.openScan(null);
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

        const snap = Scan.captureSnapshot(table);
        var pin_held = true;
        defer if (pin_held) snap.memtable_snap.release();

        const seg_start = try self.allocator.alloc(usize, snap.segment_count + 1);
        defer self.allocator.free(seg_start);
        var total_rgs: usize = 0;
        for (table.manifest.segments.items[0..snap.segment_count], 0..) |entry, i| {
            seg_start[i] = total_rgs;
            total_rgs += entry.row_group_count;
        }
        seg_start[snap.segment_count] = total_rgs;

        // Pin lanes round-robin across the CPU layout (physical cores first,
        // then hyperthread siblings). Capping at the logical count means
        // workers stack evenly — e.g. dop=24 on a 12-core/24-thread host pins
        // 2 lanes per physical core (one on each of its logical CPUs).
        var layout = HarnessCore.cpuLayout(self.allocator) catch HarnessCore.CpuLayout{ .order = &.{}, .physical_count = 0 };
        defer layout.deinit(self.allocator);
        const cpu_count = @max(@as(usize, 1), layout.order.len);
        var n_workers = @max(@as(usize, 1), @min(self.dop, cpu_count));
        if (total_rgs > 0) n_workers = @min(n_workers, total_rgs);

        const workers = try self.allocator.alloc(Worker, n_workers);
        defer self.allocator.free(workers);
        var built: usize = 0;
        defer for (workers[0..built]) |*w| {
            w.source.deinit();
            w.lane.deinit(self.allocator);
            self.allocator.free(w.resolved);
        };

        var next_rg = std.atomic.Value(usize).init(0);
        for (workers, 0..) |*w, i| {
            const source = try self.openScan(snap);
            w.* = .{
                .index = i,
                .cpu = if (layout.order.len == 0) null else layout.order[i % layout.order.len],
                .source = source,
                .lane = try Lane.init(self.allocator, self.plans, n_workers),
                .plans = self.plans,
                .resolved = try self.allocator.alloc(?usize, self.plans.len),
                .allocator = self.allocator,
                .seg_start = seg_start,
                .segment_count = snap.segment_count,
                .total_rgs = total_rgs,
                .next_rg = &next_rg,
            };
            built += 1;
        }
        // All worker scans now hold their own memtable pins.
        snap.memtable_snap.release();
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
        var merged = try Lane.init(self.allocator, self.plans, n_workers);
        errdefer merged.deinit(self.allocator);
        for (workers) |*w| try merged.mergeFrom(w.lane, self.plans);
        // COUNT(DISTINCT): union each partition column across all workers in
        // parallel (disjoint key sets), then sum the partition counts.
        try self.mergeDistinctPartitioned(workers, n_workers, layout.order, merged.distinct_counts);
        // The out-of-band `''` (folded as a boolean, never inserted into any
        // partition set) re-enters as one distinct value if any lane saw it.
        for (self.plans, 0..) |p, i| {
            if (p.op == .count_distinct and merged.has_blank[i]) merged.distinct_counts[i] += 1;
        }
        return merged;
    }

    // Cross-worker COUNT(DISTINCT) merge. Partition `p` of every worker holds a
    // disjoint slice of the key space, so one thread per partition unions that
    // column (`worker[*].dsets[i*parts + p]`) into a fresh set and records its
    // count; the per-partition counts then sum to the exact distinct total.
    // No locks: each merge thread writes only its own output set and reads the
    // (now-immutable) worker sets. Fills `out_counts[i]` for count_distinct aggs.
    fn mergeDistinctPartitioned(self: *GlobalAggregate, workers: []Worker, parts: usize, cpus: []const usize, out_counts: []u64) !void {
        var any = false;
        for (self.plans) |p| {
            if (p.op == .count_distinct) any = true;
        }
        if (!any) return;

        const jobs = try self.allocator.alloc(PartMergeJob, parts);
        defer self.allocator.free(jobs);
        // Per-(partition, agg) count scratch; job p writes its row, main sums.
        const counts = try self.allocator.alloc(u64, parts * self.plans.len);
        defer self.allocator.free(counts);
        @memset(counts, 0);
        for (jobs, 0..) |*j, p| {
            j.* = .{
                .part = p,
                .parts = parts,
                .workers = workers,
                .plans = self.plans,
                .allocator = self.allocator,
                .cpu = if (cpus.len == 0) null else cpus[p % cpus.len],
                .counts = counts[p * self.plans.len ..][0..self.plans.len],
            };
        }

        const threads = try self.allocator.alloc(std.Thread, parts);
        defer self.allocator.free(threads);
        const spawned = try self.allocator.alloc(bool, parts);
        defer self.allocator.free(spawned);
        @memset(spawned, false);
        for (jobs, 0..) |*j, p| {
            if (std.Thread.spawn(.{}, partMergeMain, .{j})) |th| {
                threads[p] = th;
                spawned[p] = true;
            } else |_| {}
        }
        for (jobs, 0..) |*j, p| if (!spawned[p]) {
            partMergeRun(j) catch |e| {
                j.err = e;
            };
        };
        for (0..parts) |p| if (spawned[p]) threads[p].join();
        for (jobs) |*j| if (j.err) |e| return e;

        for (jobs) |*j| {
            for (self.plans, 0..) |p, i| {
                if (p.op == .count_distinct) out_counts[i] += j.counts[i];
            }
        }
    }

    fn emitRow(self: *GlobalAggregate, lane: Lane) !void {
        const a = self.allocator;
        for (self.plans, 0..) |p, i| {
            const col = &self.output_cols[i];
            const row = col.data.rowCount();
            // SUM/AVG/MIN/MAX over zero qualifying (non-NULL) inputs is SQL
            // NULL — empty scan and all-NULL column alike. COUNTs stay 0.
            var is_null = false;
            switch (p.op) {
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
                            @as(f64, @floatFromInt(lane.isum[i])) / @as(f64, @floatFromInt(n));
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

fn appendInt(allocator: Allocator, col: *ColumnStore, out_type: Type, value: i128) !void {
    switch (out_type) {
        .boolean => try col.data.boolean.append(allocator, @intCast(value)),
        .tinyint => try col.data.tinyint.append(allocator, @intCast(value)),
        .smallint => try col.data.smallint.append(allocator, @intCast(value)),
        .int => try col.data.int.append(allocator, @intCast(value)),
        .date => try col.data.date.append(allocator, @intCast(value)),
        .bigint => try col.data.bigint.append(allocator, @intCast(value)),
        .datetime => try col.data.datetime.append(allocator, @intCast(value)),
        .decimal64 => try col.data.decimal64.append(allocator, @intCast(value)),
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
