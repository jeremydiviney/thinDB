//! Join operator. v1 scope:
//!   - INNER equi-join only.
//!   - Hash algorithm only (SMJ + INLJ + NLJ come in follow-ups).
//!   - Single- or multi-column join key (all keys equality).
//!   - Build side chosen automatically by `upper_rows` comparison;
//!     the smaller-by-upper-bound side is materialized fully and
//!     becomes the hash table; the other side streams as probe.
//!   - NULL join-key values never match anything (standard SQL).
//!
//! Future (other commits add):
//!   - SMJ + decision tree (hash vs SMJ via observed skew stats)
//!   - INLJ for small × large-sorted
//!   - NLJ for tiny × tiny + non-equi predicates
//!   - LEFT / RIGHT / FULL OUTER, SEMI, ANTI join types
//!   - Memory accountant integration (refuse pre-flight when build
//!     too big; abort mid-build if it grows past budget)
//!   - HLL / peek-scan for refined cardinality before materialization

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const TypeTag = types.TypeTag;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;
const StringView = storage.StringView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;
const StringStore = engine.StringStore;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const Error = exec.Error;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

const transform = @import("../engine/transform.zig");

const cell_io = @import("cell_io.zig");
const appendNullTo = cell_io.appendNullTo;
const appendOneFromBuild = cell_io.appendOneFromBuild;
const appendOneFromView = cell_io.appendOneFromView;

/// One column-pair equality in the ON clause.
pub const KeyPair = struct {
    left: []const u8,
    right: []const u8,
};

pub const JoinType = enum {
    inner,
    /// Preserve every left row. Right-side columns are NULL when no
    /// match exists. Right-side join-key columns are dropped from the
    /// output (USING semantic) — the kept left key is always present.
    left,
    /// Preserve every right row. Left-side columns are NULL when no
    /// match exists. For unmatched right rows, the left's copy of the
    /// join key in the output is also NULL (USING-semantic deviation
    /// from strict SQL, which would COALESCE; documented limitation).
    right,
    /// Preserve every row from both sides. Same USING caveat as RIGHT
    /// applies to unmatched right rows.
    full,
    // Future: semi, anti, cross.
};

pub const Algorithm = enum {
    /// Planner picks per-query using cheap stats (sort_state per
    /// side, eventually Misra-Gries-observed skew). Default. Always
    /// safe — the planner only ever picks an algorithm it can prove
    /// won't catastrophically fail given the available signal.
    auto,
    /// Build a hash table on the smaller side; probe with the other.
    /// Best for equi-joins with at least one side fitting comfortably
    /// in memory, no heavy skew. Pick explicitly when you know the
    /// shape and want to skip the planner.
    hash,
    /// Sort both sides on the join key, walk in lockstep. Predictable
    /// memory + degrades smoothly under skew. Best when both sides
    /// are large, skew is heavy, or output needs to be sorted.
    sort_merge,
    /// Materialize both sides, double-loop, evaluate ranges +
    /// extra_predicate per pair. O(N*M) — use only when there's no
    /// equi join key to exploit (pure range, opaque predicates) or
    /// when at least one side is tiny. Auto picks this for pure
    /// joins that don't fit the more specialized range_sweep
    /// shape (multi-range, non-range ops, etc.).
    nested_loop,
    /// Materialize both sides, sort each on the range column,
    /// two-pointer walk to emit matching pairs. O((N+M) log + matches)
    /// — much faster than nested_loop for selective range joins.
    /// Restricted to: empty `on`, exactly one range predicate with
    /// op in {lt, lte, gt, gte}. .auto picks this when those
    /// conditions hold.
    range_sweep,
    // Future: inlj
};

/// A column-pair range predicate: `left.<col> OP right.<col>` where
/// OP is one of `<`, `<=`, `>`, `>=`. Combined with the equi `on`
/// pairs via AND. The pair must reference numeric / comparable types
/// of matching shape (per JoinKeyTypeMismatch rules).
pub const RangePredicate = struct {
    left: []const u8,
    op: predicate.PredicateOp,
    right: []const u8,
};

pub const Spec = struct {
    join_type: JoinType = .inner,
    on: []const KeyPair,
    /// Algorithm choice. Default `.auto` lets the planner decide
    /// from cheap stats. Override with `.hash` or `.sort_merge`
    /// when you want to lock the choice (benchmarking, known shape).
    algorithm: Algorithm = .auto,
    /// Optional non-equi predicate evaluated AFTER the equi-join.
    /// Column references resolve against the output schema (left
    /// columns + right_kept columns). Semantically equivalent to
    /// chaining a `.filter()` after the join: WHERE-clause behavior,
    /// not ON-clause. For outer joins, null-extended rows where the
    /// predicate references the null side will fail (NULL never
    /// compares true) and get dropped.
    extra_predicate: ?predicate.PredicateExpr = null,
    /// Inequality predicates between left columns and right columns,
    /// AND'd together AND'd onto the equi join. Pairs that satisfy
    /// the equi keys but fail any range get dropped. Empty slice =
    /// no range constraints. v1 supports ranges on INNER joins only —
    /// outer + range would need per-row match tracking.
    ///
    /// When `on` is empty AND `ranges` is non-empty, the planner
    /// picks the nested-loop algorithm (no equi prefix to exploit).
    ranges: []const RangePredicate = &.{},
    /// Skew detection for hash joins. When BOTH conditions hold at end
    /// of build phase, the join transparently re-routes to sort-merge:
    ///   ratio:    top_freq / observed_total >= skew_ratio_threshold
    ///   absolute: estimated_bucket_size       >= skew_absolute_threshold
    /// where estimated_bucket_size = top_freq * skew_sample_interval.
    ///
    /// Both gates are needed. The ratio catches "is the heavy bucket
    /// actually hit by a meaningful fraction of probes?"; the absolute
    /// catches "is the bucket actually slow to walk?". Either alone
    /// produces false positives:
    ///   - high ratio, low absolute → small build, small bucket. Hash
    ///     bucket walk is L1/L2-resident and faster than SMJ's setup.
    ///   - low ratio, high absolute → huge build with a tail-heavy bucket.
    ///     Only a small fraction of probes pay the slow walk; resorting
    ///     the entire build+probe up front is far more expensive.
    ///
    /// Defaults — `skew_ratio_threshold = 0.3` and
    /// `skew_absolute_threshold = 20_000`. The absolute floor lines up
    /// with the L2 cache boundary (20k × 8 bytes per row index ≈ 160KB)
    /// where bucket walks begin to actually feel slow. Set
    /// `skew_ratio_threshold` to 0.0 to disable detection entirely
    /// (skip the per-build-row sampling overhead). Only applies to
    /// `.hash` / `.auto`-routed hash joins.
    ///
    /// Detection samples 1 in `skew_sample_interval` build rows
    /// (default 10). Misra-Gries with sampling preserves the
    /// fractional detection threshold in expectation. Lower
    /// intervals (1 = no sampling) cost more per build row;
    /// higher intervals lose accuracy on borderline thresholds.
    skew_ratio_threshold: f32 = 0.3,
    skew_absolute_threshold: u32 = 20_000,
    skew_sample_interval: u32 = 10,
    /// Opaque per-pair predicate evaluated during NLJ. Returns true
    /// to keep the (left_row, right_row) pair, false to drop it.
    /// Enables arbitrary cross-side predicates (fuzzy matching,
    /// geo prefilter, computed comparisons) that don't fit equi /
    /// range. Setting this forces the join to NLJ regardless of
    /// other settings — equi/range can still be specified and run
    /// alongside as cheaper first-cut filters.
    ///
    /// INNER joins only in v1.
    opaque_predicate: ?OpaquePredicate = null,
};

/// User-supplied cross-side predicate callback. Receives the
/// materialized columns + row indices for left and right.
/// Return true to emit the pair, false to drop. The callback must
/// NOT retain references to the views past its own return.
pub const OpaquePredicate = struct {
    eval: *const fn (
        ctx: ?*anyopaque,
        left: []const ColumnView,
        left_row: u32,
        right: []const ColumnView,
        right_row: u32,
    ) bool,
    ctx: ?*anyopaque = null,
};

/// Number of rows emitted per output batch. Bounded so emission stays
/// streaming even when one probe row matches many build rows.
const output_batch_rows: usize = 1024;

/// Sentinel in FastTable.heads / .next chains and in build_rows_scratch
/// (where it marks a preserved-side probe miss row). Never a valid build
/// row index — build sides are capped well below u32 max.
const FAST_EMPTY = std.math.maxInt(u32);

const FastKeyKind = enum { int, string };

/// Compile-time mirror of tryBuildFastTable's key-type gate.
fn fastKindOfType(t: TypeTag) ?FastKeyKind {
    return switch (t) {
        .int, .bigint, .date, .datetime, .tinyint, .smallint, .boolean => .int,
        .varchar, .string, .char => .string,
        else => null,
    };
}

/// Open-addressing hash table over a single build-side join key,
/// replacing the byte-compound StringHashMap for the probe hot loop.
/// Int-family keys are widened to 64 bits (exact, no verification);
/// string keys store the wyhash digest in the slot and verify bytes
/// per chain entry (digest-colliding strings share a chain).
///
/// Chains are built inserting build rows in REVERSE order, so walking
/// `heads[slot]` → `next[...]` visits ascending build rows — the same
/// per-key match order as the general bucket path, keeping output
/// row order identical between the two probe implementations.
const FastTable = struct {
    kind: FastKeyKind,
    /// `heads[slot] == FAST_EMPTY` marks an empty slot; otherwise
    /// `slot_keys[slot]` identifies the key resident there.
    slot_keys: []u64,
    heads: []u32,
    mask: u64,
    next: []u32,
    /// View over the build key column, captured after buildPhase
    /// (build_columns are immutable from then on).
    build_key_view: ColumnView,
};

/// murmur3 finalizer — raw int keys need mixing before masking.
fn fastMix(x0: u64) u64 {
    var x = x0;
    x ^= x >> 33;
    x *%= 0xff51afd7ed558ccd;
    x ^= x >> 33;
    x *%= 0xc4ceb9fe1a85ec53;
    x ^= x >> 33;
    return x;
}

/// Widen an int-family key cell to 64 bits. Sign-extension is exact:
/// two i64-equal values are equal in the original type and vice versa.
fn fastIntKey(view: ColumnView, row: u32) u64 {
    return switch (view.data) {
        .int => |s| @bitCast(@as(i64, s[row])),
        .bigint => |s| @bitCast(s[row]),
        .date => |s| @bitCast(@as(i64, s[row])),
        .datetime => |s| @bitCast(s[row]),
        .tinyint => |s| @bitCast(@as(i64, s[row])),
        .smallint => |s| @bitCast(@as(i64, s[row])),
        .boolean => |s| s[row],
        else => unreachable,
    };
}

fn stringRowBytes(view: ColumnView, row: u32) []const u8 {
    return switch (view.data) {
        .varchar, .string, .char => |sv| sv.rowBytes(row),
        else => unreachable,
    };
}

inline fn pushPair(
    alloc: Allocator,
    probe_rows: *std.ArrayListUnmanaged(u32),
    build_rows: *std.ArrayListUnmanaged(u32),
    probe_row: u32,
    build_row: u32,
) !void {
    try probe_rows.append(alloc, probe_row);
    try build_rows.append(alloc, build_row);
}

pub const Join = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,

    left: Query,
    right: Query,
    join_type: JoinType,

    /// Per-side join key column indices, in the order they appear in
    /// the `on` spec. Used by the compound-key builder.
    left_key_indices: []usize,
    right_key_indices: []usize,

    /// Optional skew detector. Set when Spec.skew_ratio_threshold > 0;
    /// observed during buildPhase, checked at end. Allocated in
    /// the join's arena.
    skew_detector: ?*@import("skew.zig").MisraGries,
    skew_ratio_threshold: f32,
    skew_absolute_threshold: u32,
    /// Sampling interval for the detector. Observe 1 in N rows.
    skew_sample_interval: u32,
    /// When skew is detected at end of buildPhase, Join transfers
    /// build_columns + both Queries to a SortMergeJoin and delegates
    /// next() to it. Join.deinit must then NOT also deinit those.
    skew_smj: ?Query = null,
    /// True once the skew route has taken ownership of left/right/
    /// build_columns. Gates Join.deinit so we don't double-free.
    transferred_to_skew: bool = false,

    /// Range predicates resolved to column indices. Each candidate
    /// (probe_row, build_row) pair must satisfy ALL of them. Empty
    /// slice = no range constraints. Owned in the arena.
    ranges: []const ResolvedRange,

    /// True iff left is the smaller side and is therefore the build
    /// side. False iff right is build. Build is materialized fully;
    /// the other side streams as probe.
    build_is_left: bool,

    /// Output schema = left.schema ++ (right.schema − right join keys).
    /// Right-side join keys are dropped from output (USING semantic):
    /// they'd be equal to the left's copies by construction.
    output_schema: []Column,
    /// Number of columns from the left side; right columns start at
    /// this index in `output_schema` and `output_columns`.
    left_col_count: usize,
    /// Per right-side column: true if it should be emitted (i.e., it
    /// isn't a join key). Sized to right_schema.len.
    right_kept_mask: []const bool,
    /// Per-output-column stat (left columns, then kept right columns). A
    /// join can't grow a column's distinct-value count, and an unchanged
    /// column keeps its min/max, so each side's upstream stat stays valid.
    /// Cached at create. Empty when neither side carries stats info.
    cached_stats: []const exec.ColStat = &.{},

    /// Materialized build side. One ColumnStore per build-side column.
    build_columns: []ColumnStore,
    build_rows: u32 = 0,

    /// Hash table: compound key bytes → list of row indices into
    /// `build_columns`. Multiple matches per key carried as a list.
    /// All allocations land in `arena`; map and lists too.
    hash_table: std.StringHashMapUnmanaged(std.ArrayListUnmanaged(u32)),

    /// Reusable scratch buffer for building per-row compound keys.
    /// Cleared+reused per row to avoid one alloc per probe.
    key_scratch: std.ArrayList(u8),

    /// Vectorized single-key probe fast path, built at the end of
    /// buildPhase when the join shape qualifies (see tryBuildFastTable).
    /// Null = the general row-at-a-time probe runs instead.
    fast_table: ?FastTable = null,
    /// Whole-batch pair-collection scratch for the fast path: parallel
    /// arrays of (probe row, build row) in probe-row order, FAST_EMPTY
    /// build entries marking preserved-side miss rows.
    probe_rows_scratch: std.ArrayListUnmanaged(u32) = .empty,
    build_rows_scratch: std.ArrayListUnmanaged(u32) = .empty,

    /// Probe fused into the probe side's parallel-scan workers (the
    /// tryFuseProbe offer was accepted): upstream batches arrive already
    /// joined and next() passes them through. `probe_chunks` holds the
    /// per-chunk pair scratches + output staging the workers write —
    /// allocated by sinkBind from the scan's thread-safe allocator.
    probe_fused: bool = false,
    probe_chunks: []ProbeChunk = &.{},
    probe_chunk_alloc: Allocator = undefined,

    /// Output staging: ColumnStores we append matched rows into,
    /// emitted as a single Batch when full or when probe is exhausted.
    output_columns: []ColumnStore,
    views: []ColumnView,

    /// State machine.
    phase: Phase = .building,
    /// While probing: current probe batch + position within it. We
    /// can't fully consume a probe row in one .next() call if it
    /// matches many build rows; we resume from where we left off.
    cur_probe_batch: ?Batch = null,
    cur_probe_row: u32 = 0,
    /// While processing one probe row's matches: the bucket list +
    /// position within it.
    cur_match_list: []const u32 = &.{},
    cur_match_pos: usize = 0,
    /// For outer joins with range: tracks whether the current probe
    /// row has had any actual emit (passed range checks). Reset on
    /// new probe row, set true on each successful emit. When the
    /// match list exhausts with this still false AND probe-side is
    /// preserved, we emit one null-extended row.
    cur_probe_any_match: bool = false,
    /// True if we just flushed a batch and the next emit should
    /// clear output_columns before appending. We can't clear at the
    /// end of flushOutput because the returned Batch's views still
    /// borrow into output_columns' buffers.
    pending_clear: bool = false,

    /// FULL OUTER: bitmap of matched build rows, allocated after build
    /// phase. The `draining_unmatched` phase walks it to emit unmatched
    /// build rows with NULLs on the probe side. Null for other join types.
    matched_build: ?std.DynamicBitSetUnmanaged = null,
    /// FULL OUTER drain cursor — index into build_columns.
    drain_cursor: u32 = 0,

    pub const ResolvedRange = struct {
        left_col: usize,
        right_col: usize,
        op: predicate.PredicateOp,
    };

    const Phase = enum {
        building,
        probing,
        /// FULL OUTER only — after probe drains, walk matched_build
        /// and emit unmatched build rows with NULLs on the probe side.
        draining_unmatched,
        done,
    };

    pub fn create(
        allocator: Allocator,
        left: Query,
        right: Query,
        spec: Spec,
    ) !Query {
        // Resolve algorithm. Opaque predicate forces NLJ (the only
        // algorithm that evaluates per-pair callbacks). Otherwise
        // .auto picks range_sweep for the specialized pure-single-
        // range shape; nested_loop for empty `on`; the equi-driven
        // algorithms via chooseAlgorithm.
        const chosen = if (spec.opaque_predicate != null)
            .nested_loop
        else if (spec.algorithm == .auto)
            (if (canUseRangeSweep(spec)) .range_sweep
                else if (spec.on.len == 0) .nested_loop
                else chooseAlgorithm(left, right, spec.on))
        else
            spec.algorithm;

        // Nested-loop / range_sweep are the only algorithms that
        // handle an empty `on` clause. Reject empty-on with any other.
        if (spec.on.len == 0 and chosen != .nested_loop and chosen != .range_sweep) {
            return Error.JoinEmptyOnClause;
        }

        if (chosen == .range_sweep) {
            const rs_spec: Spec = .{
                .join_type = spec.join_type,
                .on = spec.on,
                .algorithm = .range_sweep,
                .extra_predicate = spec.extra_predicate,
                .ranges = spec.ranges,
            };
            return @import("range_sweep.zig").RangeSweepJoin.create(allocator, left, right, rs_spec);
        }

        if (chosen == .nested_loop) {
            const nl_spec: Spec = .{
                .join_type = spec.join_type,
                .on = spec.on,
                .algorithm = .nested_loop,
                .extra_predicate = spec.extra_predicate,
                .ranges = spec.ranges,
                .opaque_predicate = spec.opaque_predicate,
            };
            return @import("nlj.zig").NestedLoopJoin.create(allocator, left, right, nl_spec);
        }

        if (chosen == .sort_merge) {
            // Re-spec with explicit algorithm so SMJ doesn't recurse
            // back through .auto if it ever calls back.
            const sm_spec: Spec = .{
                .join_type = spec.join_type,
                .on = spec.on,
                .algorithm = .sort_merge,
                .extra_predicate = spec.extra_predicate,
                .ranges = spec.ranges,
            };
            return @import("smj.zig").SortMergeJoin.create(allocator, left, right, sm_spec);
        }

        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const aa = arena.allocator();

        const left_schema = left.outputSchema();
        const right_schema = right.outputSchema();

        // Resolve key column indices on each side.
        const left_keys = try aa.alloc(usize, spec.on.len);
        const right_keys = try aa.alloc(usize, spec.on.len);
        for (spec.on, 0..) |pair, i| {
            left_keys[i] = columnIndex(left_schema, pair.left) orelse return Error.ColumnNotFound;
            right_keys[i] = columnIndex(right_schema, pair.right) orelse return Error.ColumnNotFound;
            // Types must match by tag (varchar/string/char are
            // compatible string-family; otherwise must match exactly).
            const lt: TypeTag = left_schema[left_keys[i]].type;
            const rt: TypeTag = right_schema[right_keys[i]].type;
            if (lt != rt and !(isStringTag(lt) and isStringTag(rt))) {
                return Error.JoinKeyTypeMismatch;
            }
        }

        // Resolve range predicates into column indices.
        const resolved_ranges = try aa.alloc(Join.ResolvedRange, spec.ranges.len);
        for (spec.ranges, 0..) |rp, i| {
            const lidx = columnIndex(left_schema, rp.left) orelse return Error.ColumnNotFound;
            const ridx = columnIndex(right_schema, rp.right) orelse return Error.ColumnNotFound;
            const lt: TypeTag = left_schema[lidx].type;
            const rt: TypeTag = right_schema[ridx].type;
            if (lt != rt and !(isStringTag(lt) and isStringTag(rt))) {
                return Error.JoinKeyTypeMismatch;
            }
            switch (rp.op) {
                .lt, .lte, .gt, .gte => {},
                else => return Error.UnsupportedOperatorForType,
            }
            resolved_ranges[i] = .{ .left_col = lidx, .right_col = ridx, .op = rp.op };
        }

        // Build right-side column index → keep? map. The right side's
        // join-key columns are DROPPED from the output (USING-clause
        // semantic: emit just the left's copy since they're equal by
        // construction). This lets users join two tables that share a
        // join column name (the common case) without manual aliasing.
        const right_kept_mask = try aa.alloc(bool, right_schema.len);
        for (right_kept_mask) |*m| m.* = true;
        for (right_keys) |idx| right_kept_mask[idx] = false;

        var right_kept_count: usize = 0;
        for (right_kept_mask) |m| {
            if (m) right_kept_count += 1;
        }

        // Outer joins: the "other" side's columns become nullable in
        // the output (unmatched preserved rows have NULL on the other
        // side). Inner keeps original nullability.
        const left_nullable_in_output = switch (spec.join_type) {
            .inner, .left => false,
            .right, .full => true,
        };
        const right_nullable_in_output = switch (spec.join_type) {
            .inner, .right => false,
            .left, .full => true,
        };

        // Compose output schema: left columns + right columns minus
        // join keys. Refuse if any NON-KEY column name collides — the
        // user must explicitly rename via .compute() / .exclude() in
        // that case.
        const output_schema = try allocator.alloc(Column, left_schema.len + right_kept_count);
        errdefer allocator.free(output_schema);
        for (left_schema, 0..) |c, i| {
            output_schema[i] = c;
            if (left_nullable_in_output) output_schema[i].nullable = true;
        }
        var out_idx: usize = left_schema.len;
        for (right_schema, 0..) |c, i| {
            if (!right_kept_mask[i]) continue;
            // Check against left columns + already-placed right columns.
            for (output_schema[0..out_idx]) |prior| {
                if (types.columnNameEql(prior.name, c.name)) return Error.JoinColumnNameCollision;
            }
            output_schema[out_idx] = c;
            if (right_nullable_in_output) output_schema[out_idx].nullable = true;
            out_idx += 1;
        }

        // Persist the right-keep mask for emission.
        const right_kept_mask_owned = try allocator.alloc(bool, right_schema.len);
        @memcpy(right_kept_mask_owned, right_kept_mask);
        errdefer allocator.free(right_kept_mask_owned);

        // Build-side selection. INNER + FULL: pick smaller side (less
        // memory, less hashing). LEFT/RIGHT: build is the NON-preserved
        // side (the preserved side must be the probe so we walk every
        // row and know per-row whether a match occurred).
        const left_stats = left.stats();
        const right_stats = right.stats();
        const build_is_left = switch (spec.join_type) {
            .inner, .full => left_stats.upper_rows <= right_stats.upper_rows,
            .left => false, // preserve left → probe = left → build = right
            .right => true, // preserve right → probe = right → build = left
        };

        const build_schema = if (build_is_left) left_schema else right_schema;

        // Allocate build-side column stores.
        const build_columns = try allocator.alloc(ColumnStore, build_schema.len);
        errdefer allocator.free(build_columns);
        var binited: usize = 0;
        errdefer for (build_columns[0..binited]) |*c| c.deinit(allocator);
        for (build_schema, 0..) |col, i| {
            build_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            binited += 1;
        }

        // Output staging.
        const output_columns = try allocator.alloc(ColumnStore, output_schema.len);
        errdefer allocator.free(output_columns);
        var oinited: usize = 0;
        errdefer for (output_columns[0..oinited]) |*c| c.deinit(allocator);
        for (output_schema, 0..) |col, i| {
            output_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            oinited += 1;
        }

        const views = try allocator.alloc(ColumnView, output_schema.len);
        errdefer allocator.free(views);

        const cached_stats = try exec.concatJoinStats(allocator, left, right, left_schema.len, right_kept_mask, output_schema.len);
        errdefer if (cached_stats.len > 0) allocator.free(cached_stats);

        const self = try allocator.create(Join);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .arena = arena,
            .left = left,
            .right = right,
            .join_type = spec.join_type,
            .left_key_indices = left_keys,
            .right_key_indices = right_keys,
            .ranges = resolved_ranges,
            .skew_detector = null,
            .skew_ratio_threshold = spec.skew_ratio_threshold,
            .skew_absolute_threshold = spec.skew_absolute_threshold,
            .skew_sample_interval = if (spec.skew_sample_interval == 0) 1 else spec.skew_sample_interval,
            .build_is_left = build_is_left,
            .output_schema = output_schema,
            .left_col_count = left_schema.len,
            .right_kept_mask = right_kept_mask_owned,
            .cached_stats = cached_stats,
            .build_columns = build_columns,
            .hash_table = .empty,
            .key_scratch = .empty,
            .output_columns = output_columns,
            .views = views,
        };
        // Commit the parallel probe at COMPILE time when the shape is
        // FastTable-eligible — fusion must be settled before any operator
        // above (a two-phase GROUP BY combine) composes against it. The
        // FastTable itself is still built after the build phase; by the
        // time the probe side first pulls (and the sink runs), it exists.
        if (spec.join_type != .full and resolved_ranges.len == 0 and
            spec.on.len == 1 and fastKindOfType(build_schema[(if (build_is_left) left_keys else right_keys)[0]].type) != null)
        {
            const probe = if (build_is_left) self.right else self.left;
            self.probe_fused = probe.tryFuseProbe(.{
                .ctx = self,
                .out_schema = self.output_schema,
                .bind = sinkBind,
                .process = sinkProcess,
            }) catch false;
        }

        // Skew detector must be constructed AFTER `self.* = ...` so its
        // captured Allocator points to the arena INSIDE `self`, not the
        // local stack value that just got moved. The struct AND its key
        // dupes live in the arena — uniform data otherwise churns
        // malloc/free per sampled observation in the build hot loop.
        //
        // A probe-fused join skips skew detection entirely: rerouting to
        // SMJ would hand raw probe batches to a scan that's committed to
        // emitting joined batches, and the FastTable's array-linear chain
        // walk + parallel probe absorb heavy buckets far better than the
        // bucket-list walk the reroute was built to escape.
        if (spec.skew_ratio_threshold > 0.0 and !self.probe_fused) {
            const arena_alloc = self.arena.allocator();
            const det = try arena_alloc.create(@import("skew.zig").MisraGries);
            det.* = @import("skew.zig").MisraGries.init(arena_alloc);
            self.skew_detector = det;
        }
        const q = makeQuery(allocator, self);
        if (spec.extra_predicate) |pred| {
            return @import("filter.zig").Filter.create(allocator, q, pred);
        }
        return q;
    }

    pub fn deinit(self: *Join) void {
        if (self.transferred_to_skew) {
            // skew_smj owns left, right, and build_columns. Calling
            // its deinit cascades to those resources.
            if (self.skew_smj) |sm| {
                var s = sm;
                s.deinit();
            }
        } else {
            var l = self.left;
            l.deinit();
            var r = self.right;
            r.deinit();
            for (self.build_columns) |*c| c.deinit(self.allocator);
            self.allocator.free(self.build_columns);
        }
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.right_kept_mask);
        if (self.cached_stats.len > 0) self.allocator.free(@constCast(self.cached_stats));
        self.key_scratch.deinit(self.allocator);
        self.probe_rows_scratch.deinit(self.allocator);
        self.build_rows_scratch.deinit(self.allocator);
        if (self.probe_chunks.len > 0) {
            for (self.probe_chunks) |*ch| freeProbeChunk(self.probe_chunk_alloc, ch);
            self.probe_chunk_alloc.free(self.probe_chunks);
        }
        if (self.matched_build) |*mb| mb.deinit(self.allocator);
        if (self.skew_detector) |det| det.deinit();
        self.arena.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Join) []const Column {
        // Probe-fused: this operator passes the probe side's batches
        // through verbatim, so report THAT schema live. It equals
        // `output_schema` (the sink's out_schema) until a two-phase
        // GROUP BY fuses below, after which it's the partial-aggregate
        // schema the serial combine above composes against.
        if (self.probe_fused) {
            var probe = if (self.build_is_left) self.right else self.left;
            return probe.outputSchema();
        }
        return self.output_schema;
    }

    /// Forward a partial-aggregate offer to the probe side when the probe
    /// is fused there: the scan workers then run scan → probe → partial
    /// aggregate per chunk and this Join passes the partial groups through
    /// to the serial combine above. Without probe fusion the offer dies
    /// here — joined batches only exist above this operator.
    pub fn tryFuseAggregate(self: *Join, group_cols: []const []const u8, aggs: []const exec.AggSpec) !bool {
        if (!self.probe_fused) return false;
        var probe = if (self.build_is_left) self.right else self.left;
        return probe.tryFuseAggregate(group_cols, aggs);
    }

    pub fn addPrune(self: *Join, pred: Predicate) !void {
        // Push pruning to both sides; each will only accept predicates
        // referencing its own columns (via the column-not-found check
        // in its addPrune). The other side silently ignores via the
        // existing error path.
        self.left.addPrune(pred) catch |e| switch (e) {
            error.ColumnNotFound => {},
            else => return e,
        };
        self.right.addPrune(pred) catch |e| switch (e) {
            error.ColumnNotFound => {},
            else => return e,
        };
    }

    /// Pre-execution stats on join output.
    /// Upper bound: left × right (cross-product worst case for inner).
    /// Sort state: empty (hash join output is unordered).
    pub fn accountant(self: *Join) ?*exec.memory.MemoryAccountant {
        // Both inputs share the same accountant (set by the bottom-
        // most Scan); return either.
        return self.left.accountant();
    }

    pub fn explain(self: *Join, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "HashJoin");
        try self.left.explain(out, allocator, depth + 1);
        try self.right.explain(out, allocator, depth + 1);
    }

    pub fn stats(self: *Join) exec.PipelineStats {
        const l = self.left.stats();
        const r = self.right.stats();
        const product = std.math.mul(u64, l.upper_rows, r.upper_rows) catch std.math.maxInt(u64);
        return .{ .upper_rows = product, .column_stats = self.cached_stats };
    }

    pub fn next(self: *Join) !?Batch {
        // Skew auto-route: if buildPhase detected heavy skew and
        // handed off to SMJ, all subsequent next() calls delegate.
        if (self.skew_smj) |*sm| return sm.next();

        while (true) {
            switch (self.phase) {
                .building => {
                    try self.buildPhase();
                    if (self.skew_smj) |*sm| return sm.next();
                    // FULL OUTER needs a matched-row bitmap so we can
                    // emit the unmatched build rows at the end.
                    if (self.join_type == .full) {
                        self.matched_build = try std.DynamicBitSetUnmanaged.initEmpty(
                            self.allocator,
                            self.build_rows,
                        );
                    }
                    self.phase = .probing;
                },
                .probing => {
                    if (self.probe_fused) {
                        var probe = if (self.build_is_left) &self.right else &self.left;
                        if (try probe.next()) |batch| return batch;
                        self.phase = .done;
                        return null;
                    }
                    const step = if (self.fast_table) |*ft|
                        try self.probeStepFast(ft)
                    else
                        try self.probeStep();
                    if (step) |batch| return batch;
                    // probeStep returned null → exhausted
                    if (self.join_type == .full) {
                        self.phase = .draining_unmatched;
                        // Don't flush here — the drain phase will keep
                        // appending into the same output buffer.
                        continue;
                    }
                    self.phase = .done;
                    if (try self.flushOutput()) |batch| return batch;
                    return null;
                },
                .draining_unmatched => {
                    if (try self.drainStep()) |batch| return batch;
                    self.phase = .done;
                    if (try self.flushOutput()) |batch| return batch;
                    return null;
                },
                .done => return null,
            }
        }
    }

    // -----------------------------------------------------------------
    // Build phase: drain the build side, materialize into
    // build_columns, populate the hash table.
    // -----------------------------------------------------------------

    fn buildPhase(self: *Join) !void {
        var up = if (self.build_is_left) &self.left else &self.right;
        const key_indices = if (self.build_is_left) self.left_key_indices else self.right_key_indices;
        const acc = up.accountant();
        // Build-side row width + hash-table overhead estimate.
        // Bucket overhead ~32 bytes (entry + small array list).
        const row_bytes = exec.memory.estimateRowBytes(up.outputSchema());
        const per_row_overhead = row_bytes + 32;

        while (try up.next()) |batch| {
            const n = batch.row_count;
            if (acc) |a| try a.reserve(.join_build, n * per_row_overhead);
            // Append batch into build_columns.
            for (batch.values, 0..) |v, i| {
                try transform.appendAllColumn(self.allocator, v, &self.build_columns[i]);
            }
            // Insert into hash table per row.
            var i: u32 = 0;
            while (i < n) : (i += 1) {
                // Skip rows where any join key is null — SQL semantic:
                // NULL never matches anything.
                if (anyKeyNull(batch, key_indices, i)) {
                    self.build_rows += 1;
                    continue;
                }
                self.key_scratch.clearRetainingCapacity();
                try buildCompoundKey(self.allocator, &self.key_scratch, batch, key_indices, i);

                const aa = self.arena.allocator();
                const gop = try self.hash_table.getOrPut(aa, self.key_scratch.items);
                if (!gop.found_existing) {
                    // Dup the key into the arena — scratch is reused.
                    gop.key_ptr.* = try aa.dupe(u8, self.key_scratch.items);
                    gop.value_ptr.* = .empty;
                }
                try gop.value_ptr.append(aa, self.build_rows + i);
                // Sampling: observe one in every `skew_sample_interval`
                // rows. Misra-Gries preserves the fractional detection
                // threshold in expectation under uniform sampling.
                if (self.skew_detector) |det| {
                    if ((self.build_rows + i) % self.skew_sample_interval == 0) {
                        try det.observe(self.key_scratch.items);
                    }
                }
            }
            self.build_rows += @intCast(n);
        }

        // After build, check skew via two-gate AND:
        //   ratio:    top_freq / observed_total >= skew_ratio_threshold
        //   absolute: top_freq * sample_interval >= skew_absolute_threshold
        //
        // Misra-Gries reports an UNDER-estimate of the heavy hitter's
        // frequency. Compare ratio against det.observed_total (the SAMPLED
        // total, not build_rows) — sampling preserves the fractional
        // ratio in expectation. For absolute, scale the sampled top_freq
        // back up by skew_sample_interval to estimate true bucket size.
        //
        // On detection: skip probe entirely. Transfer the already-
        // materialized build_columns to a SortMergeJoin (it owns
        // them now; we null them out so deinit doesn't double-free).
        // SMJ drains the unconsumed probe side itself.
        if (self.skew_detector) |det| {
            if (det.observed_total > 0) {
                const top_sampled = det.topFrequency();
                const top: f32 = @floatFromInt(top_sampled);
                const total: f32 = @floatFromInt(det.observed_total);
                const ratio_hit = top / total >= self.skew_ratio_threshold;
                const est_bucket: u64 = @as(u64, top_sampled) * @as(u64, self.skew_sample_interval);
                const absolute_hit = est_bucket >= self.skew_absolute_threshold;
                if (ratio_hit and absolute_hit) {
                    try self.routeToSmjOnSkew();
                }
            }
        }

        try self.tryBuildFastTable();
    }

    /// Build the vectorized-probe FastTable when the join shape
    /// qualifies: exactly one join key, int-family or string type,
    /// no range predicates, not skew-routed. Float/decimal/uuid keys
    /// stay on the general path (rare in practice; float bit-equality
    /// vs compound-key bit-equality is the same, but not worth a lane).
    fn tryBuildFastTable(self: *Join) !void {
        if (self.skew_smj != null) return;
        if (self.ranges.len > 0) return;
        if (self.left_key_indices.len != 1) return;
        const build_key_idx = if (self.build_is_left) self.left_key_indices[0] else self.right_key_indices[0];
        const key_view = self.build_columns[build_key_idx].view();
        const kind: FastKeyKind = switch (key_view.data) {
            .int, .bigint, .date, .datetime, .tinyint, .smallint, .boolean => .int,
            .varchar, .string, .char => .string,
            else => return,
        };

        const aa = self.arena.allocator();
        const n: usize = self.build_rows;
        const cap = std.math.ceilPowerOfTwoAssert(usize, @max(16, n * 2));
        const slot_keys = try aa.alloc(u64, cap);
        const heads = try aa.alloc(u32, cap);
        @memset(heads, FAST_EMPTY);
        const chain_next = try aa.alloc(u32, n);
        const mask: u64 = cap - 1;

        var row: u32 = @intCast(n);
        while (row > 0) {
            row -= 1;
            if (!key_view.isValid(row)) continue;
            const key = switch (kind) {
                .int => fastIntKey(key_view, row),
                .string => std.hash.Wyhash.hash(0, stringRowBytes(key_view, row)),
            };
            var slot = (if (kind == .int) fastMix(key) else key) & mask;
            while (true) {
                if (heads[slot] == FAST_EMPTY) {
                    slot_keys[slot] = key;
                    chain_next[row] = FAST_EMPTY;
                    heads[slot] = row;
                    break;
                }
                if (slot_keys[slot] == key) {
                    chain_next[row] = heads[slot];
                    heads[slot] = row;
                    break;
                }
                slot = (slot + 1) & mask;
            }
        }

        self.fast_table = .{
            .kind = kind,
            .slot_keys = slot_keys,
            .heads = heads,
            .mask = mask,
            .next = chain_next,
            .build_key_view = key_view,
        };
    }

    fn routeToSmjOnSkew(self: *Join) !void {
        // INNER joins only for the auto-route path in v1. Outer +
        // skew would need to thread the preserve semantics through
        // and is more invasive.
        if (self.join_type != .inner) return;
        if (self.ranges.len > 0) return;

        const smj_spec: Spec = .{
            .join_type = .inner,
            .on = blk: {
                // Reconstruct an `on` slice from the resolved key
                // indices + the operator's output schema. We have
                // left/right key indices; we need column names.
                const pairs = try self.allocator.alloc(KeyPair, self.left_key_indices.len);
                const left_schema = self.left.outputSchema();
                const right_schema = self.right.outputSchema();
                for (self.left_key_indices, self.right_key_indices, 0..) |li, ri, i| {
                    pairs[i] = .{ .left = left_schema[li].name, .right = right_schema[ri].name };
                }
                break :blk pairs;
            },
            .algorithm = .sort_merge,
        };
        defer self.allocator.free(@constCast(smj_spec.on));

        const smj_q = try @import("smj.zig").SortMergeJoin.createForSkewRoute(
            self.allocator,
            self.left,
            self.right,
            smj_spec,
            self.build_columns,
            self.build_rows,
            self.build_is_left,
        );
        // SMJ now owns the Queries + the transferred build_columns.
        self.skew_smj = smj_q;
        self.transferred_to_skew = true;
        // Empty the slice so the deinit loop is a no-op for these.
        self.build_columns = &.{};
    }

    // -----------------------------------------------------------------
    // Probe phase: stream the probe side, look each row up in the
    // hash table, emit matched output rows.
    // -----------------------------------------------------------------

    /// Vectorized probe: consume whole probe batches, collect all
    /// (probe row, build row) match pairs into the scratch arrays,
    /// then bulk-gather every output column and emit one batch per
    /// probe batch. Replaces the per-row compound-key build +
    /// StringHashMap lookup + per-cell appends of the general path.
    fn probeStepFast(self: *Join, ft: *const FastTable) !?Batch {
        var probe = if (self.build_is_left) &self.right else &self.left;
        while (true) {
            const batch = (try probe.next()) orelse return null;
            try self.collectPairs(ft, batch, self.allocator, &self.probe_rows_scratch, &self.build_rows_scratch);
            if (self.probe_rows_scratch.items.len == 0) continue;
            return try self.emitPairs(batch);
        }
    }

    /// Collect every (probe row, build row) match pair for one probe batch
    /// into the given scratch arrays — FAST_EMPTY build entries marking
    /// preserved-side miss rows. Reads only build-immutable state plus the
    /// passed scratches, so concurrent calls with distinct scratches are
    /// safe (the fused-probe workers rely on this; matched_build is null
    /// there — FULL never fuses).
    fn collectPairs(
        self: *Join,
        ft: *const FastTable,
        batch: Batch,
        alloc: Allocator,
        probe_rows: *std.ArrayListUnmanaged(u32),
        build_rows: *std.ArrayListUnmanaged(u32),
    ) !void {
        const probe_key_idx = if (self.build_is_left) self.right_key_indices[0] else self.left_key_indices[0];
        const preserved = self.probeSidePreserved();
        const n = batch.row_count;
        const key_view = batch.values[probe_key_idx];

        probe_rows.clearRetainingCapacity();
        build_rows.clearRetainingCapacity();
        try probe_rows.ensureUnusedCapacity(alloc, n);
        try build_rows.ensureUnusedCapacity(alloc, n);

        var i: u32 = 0;
        while (i < n) : (i += 1) {
            if (!key_view.isValid(i)) {
                if (preserved) try pushPair(alloc, probe_rows, build_rows, i, FAST_EMPTY);
                continue;
            }
            var found = false;
            switch (ft.kind) {
                .int => {
                    const key = fastIntKey(key_view, i);
                    var slot = fastMix(key) & ft.mask;
                    while (true) {
                        const head = ft.heads[slot];
                        if (head == FAST_EMPTY) break;
                        if (ft.slot_keys[slot] == key) {
                            var r = head;
                            while (r != FAST_EMPTY) : (r = ft.next[r]) {
                                found = true;
                                try pushPair(alloc, probe_rows, build_rows, i, r);
                                if (self.matched_build) |*mb| mb.set(r);
                            }
                            break;
                        }
                        slot = (slot + 1) & ft.mask;
                    }
                },
                .string => {
                    const bytes = stringRowBytes(key_view, i);
                    const key = std.hash.Wyhash.hash(0, bytes);
                    var slot = key & ft.mask;
                    while (true) {
                        const head = ft.heads[slot];
                        if (head == FAST_EMPTY) break;
                        if (ft.slot_keys[slot] == key) {
                            var r = head;
                            while (r != FAST_EMPTY) : (r = ft.next[r]) {
                                if (!std.mem.eql(u8, stringRowBytes(ft.build_key_view, r), bytes)) continue;
                                found = true;
                                try pushPair(alloc, probe_rows, build_rows, i, r);
                                if (self.matched_build) |*mb| mb.set(r);
                            }
                            break;
                        }
                        slot = (slot + 1) & ft.mask;
                    }
                },
            }
            if (!found and preserved) try pushPair(alloc, probe_rows, build_rows, i, FAST_EMPTY);
        }
    }

    /// Bulk-gather the collected pairs into output_columns and flush
    /// as one batch. Probe-side columns gather by probe row (the probe
    /// value is emitted even on preserved-side miss rows); build-side
    /// columns gather by build row, with FAST_EMPTY runs emitting NULLs.
    fn emitPairs(self: *Join, batch: Batch) !?Batch {
        if (self.pending_clear) {
            for (self.output_columns) |*c| c.clear();
            self.pending_clear = false;
        }
        try self.gatherPairs(batch, self.probe_rows_scratch.items, self.build_rows_scratch.items, self.output_columns, self.allocator);
        return try self.flushOutput();
    }

    /// Bulk-gather collected pairs into `out_cols` (join output column
    /// order). Probe-side columns gather by probe row; build-side columns
    /// by build row with FAST_EMPTY miss runs emitting NULLs. Reads only
    /// build-immutable state — concurrent calls with distinct out_cols are
    /// safe given a thread-safe `alloc`.
    fn gatherPairs(
        self: *Join,
        batch: Batch,
        probe_rows: []const u32,
        build_rows: []const u32,
        out_cols: []ColumnStore,
        alloc: Allocator,
    ) !void {
        const left_count = self.left_col_count;
        var out_idx: usize = 0;

        if (self.build_is_left) {
            var i: usize = 0;
            while (i < left_count) : (i += 1) {
                try gatherBuildColumn(alloc, self.build_columns[i].view(), build_rows, &out_cols[out_idx]);
                out_idx += 1;
            }
            for (batch.values, 0..) |v, idx2| {
                if (!self.right_kept_mask[idx2]) continue;
                try transform.appendByIndices(alloc, v, probe_rows, &out_cols[out_idx]);
                out_idx += 1;
            }
        } else {
            var i: usize = 0;
            while (i < left_count) : (i += 1) {
                try transform.appendByIndices(alloc, batch.values[i], probe_rows, &out_cols[out_idx]);
                out_idx += 1;
            }
            for (self.build_columns, 0..) |*bc, idx2| {
                if (!self.right_kept_mask[idx2]) continue;
                try gatherBuildColumn(alloc, bc.view(), build_rows, &out_cols[out_idx]);
                out_idx += 1;
            }
        }
    }

    // -----------------------------------------------------------------
    // Fused parallel probe (ProbeSink implementation): the probe side's
    // ParallelScan workers call sinkProcess concurrently, one in-flight
    // call per chunk. Everything read here is immutable after the build
    // phase; all mutation lands in the chunk's own scratches/stores.
    // -----------------------------------------------------------------

    const ProbeChunk = struct {
        probe_rows: std.ArrayListUnmanaged(u32) = .empty,
        build_rows: std.ArrayListUnmanaged(u32) = .empty,
        out_cols: []ColumnStore = &.{},
        views: []ColumnView = &.{},
    };

    fn sinkBind(ctx: *anyopaque, n_chunks: usize, alloc: Allocator) anyerror!void {
        const self: *Join = @ptrCast(@alignCast(ctx));
        self.probe_chunk_alloc = alloc;
        const chunks = try alloc.alloc(ProbeChunk, n_chunks);
        var done: usize = 0;
        errdefer {
            for (chunks[0..done]) |*ch| freeProbeChunk(alloc, ch);
            alloc.free(chunks);
        }
        for (chunks) |*ch| {
            ch.* = .{};
            const cols = try alloc.alloc(ColumnStore, self.output_schema.len);
            var inited: usize = 0;
            errdefer {
                for (cols[0..inited]) |*c| c.deinit(alloc);
                alloc.free(cols);
            }
            for (self.output_schema, cols) |col, *store| {
                store.* = try ColumnStore.init(alloc, col.type, col.nullable);
                inited += 1;
            }
            ch.out_cols = cols;
            done += 1;
            ch.views = try alloc.alloc(ColumnView, self.output_schema.len);
        }
        self.probe_chunks = chunks;
    }

    fn sinkProcess(ctx: *anyopaque, chunk: usize, batch: Batch) anyerror!?Batch {
        const self: *Join = @ptrCast(@alignCast(ctx));
        const ch = &self.probe_chunks[chunk];
        const alloc = self.probe_chunk_alloc;
        const ft = &self.fast_table.?;
        try self.collectPairs(ft, batch, alloc, &ch.probe_rows, &ch.build_rows);
        if (ch.probe_rows.items.len == 0) return null;
        for (ch.out_cols) |*c| c.clear();
        try self.gatherPairs(batch, ch.probe_rows.items, ch.build_rows.items, ch.out_cols, alloc);
        for (ch.out_cols, ch.views) |*c, *v| v.* = c.view();
        return Batch{
            .schema = self.output_schema,
            .values = ch.views,
            .row_count = ch.out_cols[0].data.rowCount(),
        };
    }

    fn freeProbeChunk(alloc: Allocator, ch: *ProbeChunk) void {
        ch.probe_rows.deinit(alloc);
        ch.build_rows.deinit(alloc);
        for (ch.out_cols) |*c| c.deinit(alloc);
        if (ch.out_cols.len > 0) alloc.free(ch.out_cols);
        if (ch.views.len > 0) alloc.free(ch.views);
    }

    /// Gather build-side rows into `out`, splitting `rows` into runs of
    /// real indices (bulk appendByIndices) and FAST_EMPTY miss runs
    /// (per-row NULL). Inner joins have no miss runs — one bulk call.
    fn gatherBuildColumn(alloc: Allocator, view: ColumnView, rows: []const u32, out: *ColumnStore) !void {
        var start: usize = 0;
        while (start < rows.len) {
            var end = start;
            if (rows[start] == FAST_EMPTY) {
                while (end < rows.len and rows[end] == FAST_EMPTY) : (end += 1) {}
                var j = start;
                while (j < end) : (j += 1) try appendNullTo(alloc, out);
            } else {
                while (end < rows.len and rows[end] != FAST_EMPTY) : (end += 1) {}
                try transform.appendByIndices(alloc, view, rows[start..end], out);
            }
            start = end;
        }
    }

    fn probeStep(self: *Join) !?Batch {
        var probe = if (self.build_is_left) &self.right else &self.left;
        const probe_key_indices = if (self.build_is_left) self.right_key_indices else self.left_key_indices;

        while (true) {
            // If we still have an in-progress match list, continue emitting from it.
            if (self.cur_match_pos < self.cur_match_list.len) {
                const consumed_full = try self.emitMatchesUntilFull(probe_key_indices);
                if (consumed_full) return try self.flushOutput();
                // List exhausted. If outer + range filtered out all
                // candidates (any_match still false), emit one null-
                // extended row for the probe side now.
                if (!self.cur_probe_any_match and self.probeSidePreserved()) {
                    const b = self.cur_probe_batch.?;
                    const full_after = try self.emitProbeOnlyRow(b, self.cur_probe_row);
                    self.cur_probe_row += 1;
                    self.cur_probe_any_match = false;
                    if (full_after) return try self.flushOutput();
                    continue;
                }
                self.cur_probe_row += 1;
                self.cur_probe_any_match = false;
                continue;
            }

            // Advance within the current probe batch, or fetch next.
            if (self.cur_probe_batch) |batch| {
                if (self.cur_probe_row >= batch.row_count) {
                    self.cur_probe_batch = null;
                    self.cur_probe_row = 0;
                    continue;
                }
                // Look up this probe row.
                if (!anyKeyNull(batch, probe_key_indices, self.cur_probe_row)) {
                    self.key_scratch.clearRetainingCapacity();
                    try buildCompoundKey(self.allocator, &self.key_scratch, batch, probe_key_indices, self.cur_probe_row);
                    if (self.hash_table.get(self.key_scratch.items)) |bucket| {
                        self.cur_match_list = bucket.items;
                        self.cur_match_pos = 0;
                        self.cur_probe_any_match = false;
                        continue; // emit matches in next loop iteration
                    }
                }
                // No bucket → no candidates at all. If outer, emit
                // one null-extended row before advancing.
                if (self.probeSidePreserved()) {
                    const full_after = try self.emitProbeOnlyRow(batch, self.cur_probe_row);
                    self.cur_probe_row += 1;
                    if (full_after) return try self.flushOutput();
                    continue;
                }
                self.cur_probe_row += 1;
                continue;
            }

            // Need a new probe batch.
            const next_batch = (try probe.next()) orelse {
                return null; // probe exhausted
            };
            self.cur_probe_batch = next_batch;
            self.cur_probe_row = 0;
        }
    }

    /// Emit (build_row, probe_row) output rows for the current
    /// match list until the output buffer fills. Returns `true` if
    /// the buffer is full (caller should flush+return); `false` if
    /// the current match list was exhausted (caller should advance
    /// to the next probe row).
    ///
    /// Output ordering: left columns (all of them), then right
    /// columns minus right-side join keys (USING semantic). Which
    /// physical side (build/probe) provides which depends on
    /// `build_is_left`.
    fn emitMatchesUntilFull(self: *Join, probe_key_indices: []const usize) !bool {
        _ = probe_key_indices;
        const batch = self.cur_probe_batch.?;
        const probe_row = self.cur_probe_row;
        const left_count = self.left_col_count;

        // First emit after a flush: clear leftover-but-already-yielded
        // rows from output_columns. We deferred the clear from flush
        // because the returned Batch's views borrow into these buffers.
        if (self.pending_clear) {
            for (self.output_columns) |*c| c.clear();
            self.pending_clear = false;
        }

        while (self.cur_match_pos < self.cur_match_list.len) {
            const build_row = self.cur_match_list[self.cur_match_pos];
            self.cur_match_pos += 1;

            // Range filter — every range must hold (AND-combined).
            // NULL on either side fails (two-valued logic).
            if (!self.passesAllRanges(batch, probe_row, build_row)) continue;

            // Actual match — record so probeStep doesn't null-extend
            // this probe row, and (FULL) so drainStep skips this
            // build row.
            self.cur_probe_any_match = true;
            if (self.matched_build) |*mb| mb.set(build_row);

            // Append left-side columns first.
            var out_idx: usize = 0;
            if (self.build_is_left) {
                // Left side = build. All build columns are left.
                var i: usize = 0;
                while (i < left_count) : (i += 1) {
                    try appendOneFromBuild(self.allocator, &self.output_columns[out_idx], &self.build_columns[i], build_row);
                    out_idx += 1;
                }
            } else {
                // Left side = probe. All probe columns are left.
                var i: usize = 0;
                while (i < left_count) : (i += 1) {
                    try appendOneFromView(self.allocator, &self.output_columns[out_idx], batch.values[i], probe_row);
                    out_idx += 1;
                }
            }

            // Append right-side columns, skipping the ones in the join key mask.
            if (self.build_is_left) {
                // Right side = probe. Skip right-key columns.
                for (batch.values, 0..) |v, i| {
                    if (!self.right_kept_mask[i]) continue;
                    try appendOneFromView(self.allocator, &self.output_columns[out_idx], v, probe_row);
                    out_idx += 1;
                }
            } else {
                // Right side = build. Skip right-key columns.
                for (self.build_columns, 0..) |*bc, i| {
                    if (!self.right_kept_mask[i]) continue;
                    try appendOneFromBuild(self.allocator, &self.output_columns[out_idx], bc, build_row);
                    out_idx += 1;
                }
            }

            if (self.output_columns[0].data.rowCount() >= output_batch_rows) {
                // If this was the last match in the list, retire the
                // probe row too — otherwise the next call would re-run
                // hash lookup on the same probe row and re-emit its
                // matches. Without this, every probe row that lands on
                // an exact batch boundary gets double-counted.
                if (self.cur_match_pos >= self.cur_match_list.len) {
                    self.cur_probe_row += 1;
                    self.cur_match_list = &.{};
                    self.cur_match_pos = 0;
                    self.cur_probe_any_match = false;
                }
                return true;
            }
        }

        // Match list exhausted. Reset list state but DON'T advance
        // probe_row — probeStep does that after checking
        // cur_probe_any_match for the outer-with-range case.
        self.cur_match_list = &.{};
        self.cur_match_pos = 0;
        return false;
    }

    /// Evaluate every range predicate against one (probe_row,
    /// build_row) candidate. AND semantics: any failing range
    /// rejects the pair. NULL on either side fails (two-valued logic).
    fn passesAllRanges(self: Join, batch: Batch, probe_row: u32, build_row: u32) bool {
        for (self.ranges) |rg| {
            const left_view: ColumnView = if (self.build_is_left)
                self.build_columns[rg.left_col].view()
            else
                batch.values[rg.left_col];
            const right_view: ColumnView = if (self.build_is_left)
                batch.values[rg.right_col]
            else
                self.build_columns[rg.right_col].view();
            const lrow: u32 = if (self.build_is_left) build_row else probe_row;
            const rrow: u32 = if (self.build_is_left) probe_row else build_row;
            if (!compareCellsOp(left_view, lrow, right_view, rrow, rg.op)) return false;
        }
        return true;
    }

    fn flushOutput(self: *Join) !?Batch {
        // pending_clear means the staged rows were already yielded by a
        // previous flush and just haven't been cleared yet (the clear is
        // deferred to the next emit). Without this guard, a probe that
        // exhausts exactly on a flush boundary re-emits its last batch.
        if (self.pending_clear) return null;
        const rows = self.output_columns[0].data.rowCount();
        if (rows == 0) return null;
        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        const batch = Batch{
            .schema = self.output_schema,
            .values = self.views,
            .row_count = rows,
        };
        // Defer clearing output_columns until the START of the next
        // emit. Clearing here would set items.len = 0 on the buffers
        // the returned Batch's views point into. Caller contract
        // (matches Aggregate / Sort): consume the returned Batch
        // synchronously before calling next() again.
        self.pending_clear = true;
        return batch;
    }

    /// True iff the side currently being probed (= the not-build side
    /// per `build_is_left`) is preserved by the join semantics.
    /// - LEFT  preserves left → probe is left when build_is_left == false.
    /// - RIGHT preserves right → probe is right when build_is_left == true.
    /// - FULL  preserves both.
    fn probeSidePreserved(self: Join) bool {
        return switch (self.join_type) {
            .inner => false,
            .left => !self.build_is_left, // probe == left
            .right => self.build_is_left, // probe == right
            .full => true,
        };
    }

    /// Emit one output row for an unmatched probe row: probe-side
    /// columns from `batch[probe_row]`, NULL for build-side columns.
    /// Special-cases the USING-dropped right-side join key columns:
    /// when build == right (i.e., probe == left, LEFT/FULL OUTER), the
    /// left-side join-key columns in output already hold the probe's
    /// value naturally. When build == left (i.e., probe == right,
    /// RIGHT OUTER), the left side's copy of the join key is NULL —
    /// see the JoinType.right comment for the SQL deviation.
    /// Returns `true` if the output buffer hit its threshold.
    fn emitProbeOnlyRow(self: *Join, batch: Batch, probe_row: u32) !bool {
        if (self.pending_clear) {
            for (self.output_columns) |*c| c.clear();
            self.pending_clear = false;
        }
        const left_count = self.left_col_count;
        var out_idx: usize = 0;

        if (self.build_is_left) {
            // Build = left → probe = right. Left columns get NULL.
            var i: usize = 0;
            while (i < left_count) : (i += 1) {
                try appendNullTo(self.allocator, &self.output_columns[out_idx]);
                out_idx += 1;
            }
            // Right (probe) columns, skipping right-key columns.
            for (batch.values, 0..) |v, idx2| {
                if (!self.right_kept_mask[idx2]) continue;
                try appendOneFromView(self.allocator, &self.output_columns[out_idx], v, probe_row);
                out_idx += 1;
            }
        } else {
            // Build = right → probe = left. Left (probe) cols normal.
            var i: usize = 0;
            while (i < left_count) : (i += 1) {
                try appendOneFromView(self.allocator, &self.output_columns[out_idx], batch.values[i], probe_row);
                out_idx += 1;
            }
            // Right (build) columns all NULL, skipping right-key cols.
            for (self.right_kept_mask) |kept| {
                if (!kept) continue;
                try appendNullTo(self.allocator, &self.output_columns[out_idx]);
                out_idx += 1;
            }
        }
        return self.output_columns[0].data.rowCount() >= output_batch_rows;
    }

    /// FULL OUTER drain phase: walk matched_build, emit one
    /// null-extended row per unmatched build row.
    fn drainStep(self: *Join) !?Batch {
        const mb = if (self.matched_build) |*m| m else return null;
        if (self.pending_clear) {
            for (self.output_columns) |*c| c.clear();
            self.pending_clear = false;
        }
        const left_count = self.left_col_count;
        while (self.drain_cursor < self.build_rows) : (self.drain_cursor += 1) {
            if (mb.isSet(self.drain_cursor)) continue;
            const build_row = self.drain_cursor;
            var out_idx: usize = 0;

            if (self.build_is_left) {
                // Build = left. Emit left (build) cols normally,
                // right cols NULL (skipping dropped right-key cols).
                var i: usize = 0;
                while (i < left_count) : (i += 1) {
                    try appendOneFromBuild(self.allocator, &self.output_columns[out_idx], &self.build_columns[i], build_row);
                    out_idx += 1;
                }
                for (self.right_kept_mask) |kept| {
                    if (!kept) continue;
                    try appendNullTo(self.allocator, &self.output_columns[out_idx]);
                    out_idx += 1;
                }
            } else {
                // Build = right. Emit NULL for left cols, right (build)
                // cols normally (skipping dropped right-key cols).
                var i: usize = 0;
                while (i < left_count) : (i += 1) {
                    try appendNullTo(self.allocator, &self.output_columns[out_idx]);
                    out_idx += 1;
                }
                for (self.build_columns, 0..) |*bc, idx2| {
                    if (!self.right_kept_mask[idx2]) continue;
                    try appendOneFromBuild(self.allocator, &self.output_columns[out_idx], bc, build_row);
                    out_idx += 1;
                }
            }

            if (self.output_columns[0].data.rowCount() >= output_batch_rows) {
                self.drain_cursor += 1; // retire this build row
                return try self.flushOutput();
            }
        }
        return null;
    }
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Decision tree — picks the best algorithm from cheap stats only.
// ---------------------------------------------------------------------------
//
// v1 rules (extensible):
//
//   1. Both sides globally sorted on a prefix that covers the join
//      keys → sort_merge (the merge-only fast path is essentially
//      free; full SMJ here still beats hash by skipping the build).
//   2. Otherwise → hash (the OLAP default — best for the common
//      dim × fact shape).
//
// Future v2 refinements:
//   - Detect "small × large pre-sorted" → INLJ
//   - Materialize the smaller side first, observe max-frequency via
//     Misra-Gries, route to SMJ if skew threshold exceeded
//   - Use HLL-derived NDV estimates for output cardinality prediction
//
// The decision is intentionally conservative — we only route to SMJ
// when we can prove the structural advantage (both sides sorted).
// Hash remains the default for the broad middle of analytics shapes
// where it wins.

/// True iff the spec fits range_sweep's narrow contract: pure range
/// (empty `on`), exactly one range predicate, op in {lt, lte, gt, gte},
/// INNER join. Multi-range / outer / opaque shapes fall back to NLJ.
fn canUseRangeSweep(spec: Spec) bool {
    if (spec.join_type != .inner) return false;
    if (spec.on.len != 0) return false;
    if (spec.ranges.len != 1) return false;
    return switch (spec.ranges[0].op) {
        .lt, .lte, .gt, .gte => true,
        else => false,
    };
}

fn chooseAlgorithm(left: Query, right: Query, on: []const KeyPair) Algorithm {
    const ls = left.stats();
    const rs = right.stats();

    // Build the list of join-key column names per side.
    if (joinKeysCovered(ls.sort_state, on, .left) and
        joinKeysCovered(rs.sort_state, on, .right) and
        ls.sort_state.global and rs.sort_state.global)
    {
        return .sort_merge;
    }

    return .hash;
}

pub const KeySide = enum { left, right };

/// Returns true if `state.keys` is a leading prefix of (or equal to)
/// the join-key columns on the named side of the `on` pairs.
pub fn joinKeysCovered(state: exec.SortState, on: []const KeyPair, side: KeySide) bool {
    if (state.keys.len < on.len) return false;
    for (on, 0..) |pair, i| {
        const required = switch (side) {
            .left => pair.left,
            .right => pair.right,
        };
        if (!std.mem.eql(u8, state.keys[i], required)) return false;
        // The merge advances both cursors assuming ascending order; a
        // descending-sorted prefix can't drive it, so don't claim coverage.
        if (!state.ascendingAt(i)) return false;
    }
    return true;
}

fn columnIndex(schema: []const Column, name: []const u8) ?usize {
    return types.findColumn(schema, name);
}

fn isStringTag(t: TypeTag) bool {
    return switch (t) {
        .varchar, .string, .char => true,
        else => false,
    };
}

/// True if any key column has a NULL value at row `i`.
fn anyKeyNull(batch: Batch, key_indices: []const usize, i: u32) bool {
    for (key_indices) |idx| {
        if (!batch.values[idx].isValid(i)) return true;
    }
    return false;
}

/// Build a compound key for hashing/comparison. Mirrors the layout
/// used by Aggregate's groupBy key builder.
fn buildCompoundKey(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    batch: Batch,
    key_indices: []const usize,
    row: u32,
) !void {
    for (key_indices) |ci| {
        const view = batch.values[ci];
        switch (view.data) {
            .int => |s| try appendInt(allocator, out, i32, s[row]),
            .bigint => |s| try appendInt(allocator, out, i64, s[row]),
            .boolean => |s| try out.append(allocator, s[row]),
            .float => |s| try appendBits(allocator, out, u32, @bitCast(s[row])),
            .double => |s| try appendBits(allocator, out, u64, @bitCast(s[row])),
            .date => |s| try appendInt(allocator, out, i32, s[row]),
            .datetime => |s| try appendInt(allocator, out, i64, s[row]),
            .tinyint => |s| try out.append(allocator, @bitCast(s[row])),
            .smallint => |s| try appendInt(allocator, out, i16, s[row]),
            .largeint => |s| try appendInt(allocator, out, i128, s[row]),
            .decimal64 => |s| try appendInt(allocator, out, i64, s[row]),
            .decimal128 => |s| try appendInt(allocator, out, i128, s[row]),
            .uuid => |s| try appendInt(allocator, out, u128, s[row]),
            .varchar, .string, .char => |sv| {
                const bytes = sv.rowBytes(row);
                try appendBits(allocator, out, u32, @intCast(bytes.len));
                try out.appendSlice(allocator, bytes);
            },
        }
    }
}

fn appendInt(allocator: Allocator, out: *std.ArrayList(u8), comptime T: type, v: T) !void {
    var b: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &b, v, .little);
    try out.appendSlice(allocator, &b);
}

fn appendBits(allocator: Allocator, out: *std.ArrayList(u8), comptime T: type, v: T) !void {
    var b: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &b, v, .little);
    try out.appendSlice(allocator, &b);
}

/// Append one row's worth of data from `src` (a build-side
/// ColumnStore) into `dst` (an output ColumnStore).
/// Compare two cells from same-typed columns with the given op.
/// Returns false for NULL on either side (two-valued logic). Strings
/// use lex byte order. Floats use IEEE compare (NaN never compares
/// true to anything — handled implicitly).
pub fn compareCellsOp(left: ColumnView, lrow: u32, right: ColumnView, rrow: u32, op: predicate.PredicateOp) bool {
    if (!left.isValid(lrow) or !right.isValid(rrow)) return false;
    switch (left.data) {
        .int => |s| return cmpOp(i32, s[lrow], right.data.int[rrow], op),
        .bigint => |s| return cmpOp(i64, s[lrow], right.data.bigint[rrow], op),
        .boolean => |s| return cmpOp(u8, s[lrow], right.data.boolean[rrow], op),
        .float => |s| return cmpOp(f32, s[lrow], right.data.float[rrow], op),
        .double => |s| return cmpOp(f64, s[lrow], right.data.double[rrow], op),
        .date => |s| return cmpOp(i32, s[lrow], right.data.date[rrow], op),
        .datetime => |s| return cmpOp(i64, s[lrow], right.data.datetime[rrow], op),
        .tinyint => |s| return cmpOp(i8, s[lrow], right.data.tinyint[rrow], op),
        .smallint => |s| return cmpOp(i16, s[lrow], right.data.smallint[rrow], op),
        .largeint => |s| return cmpOp(i128, s[lrow], right.data.largeint[rrow], op),
        .decimal64 => |s| return cmpOp(i64, s[lrow], right.data.decimal64[rrow], op),
        .decimal128 => |s| return cmpOp(i128, s[lrow], right.data.decimal128[rrow], op),
        .uuid => |s| return cmpOp(u128, s[lrow], right.data.uuid[rrow], op),
        .varchar => |sv| return cmpBytesOp(sv.rowBytes(lrow), right.data.varchar.rowBytes(rrow), op),
        .string => |sv| return cmpBytesOp(sv.rowBytes(lrow), right.data.string.rowBytes(rrow), op),
        .char => |sv| return cmpBytesOp(sv.rowBytes(lrow), right.data.char.rowBytes(rrow), op),
    }
}

fn cmpOp(comptime T: type, a: T, b: T, op: predicate.PredicateOp) bool {
    return switch (op) {
        .eq => a == b,
        .neq => a != b,
        .lt => a < b,
        .lte => a <= b,
        .gt => a > b,
        .gte => a >= b,
    };
}

fn cmpBytesOp(a: []const u8, b: []const u8, op: predicate.PredicateOp) bool {
    const ord = std.mem.order(u8, a, b);
    return switch (op) {
        .eq => ord == .eq,
        .neq => ord != .eq,
        .lt => ord == .lt,
        .lte => ord != .gt,
        .gt => ord == .gt,
        .gte => ord != .lt,
    };
}

