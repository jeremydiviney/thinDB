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
const Type = types.Type;
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
const cast = @import("cast.zig");

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

const transform = @import("../engine/transform.zig");

const cell_io = @import("cell_io.zig");

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

fn keyValueAt(v: ColumnView, row: usize) ?types.Value {
    return switch (v.data) {
        .int => |s| .{ .int = s[row] },
        .bigint => |s| .{ .bigint = s[row] },
        .tinyint => |s| .{ .tinyint = s[row] },
        .smallint => |s| .{ .smallint = s[row] },
        .date => |s| .{ .date = s[row] },
        .datetime => |s| .{ .datetime = s[row] },
        .float => |s| .{ .float = s[row] },
        .double => |s| .{ .double = s[row] },
        .decimal64 => |s| .{ .decimal64 = s[row] },
        .largeint => |s| .{ .largeint = s[row] },
        .varchar, .string, .char, .json => |s| .{ .text = s.bytes[s.offsets[row]..s.offsets[row + 1]] },
        else => null,
    };
}
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

const NormalizedJoinInputs = struct {
    left: Query,
    right: Query,
};

fn normalizeJoinKeyTypes(
    aa: Allocator,
    left: Query,
    right: Query,
    spec: Spec,
) !NormalizedJoinInputs {
    const left_schema = left.outputSchema();
    const right_schema = right.outputSchema();
    var left_casts: std.ArrayList(exec.Derived) = .empty;
    var right_casts: std.ArrayList(exec.Derived) = .empty;

    for (spec.on) |pair| {
        const left_idx = columnIndex(left_schema, pair.left) orelse return Error.ColumnNotFound;
        const right_idx = columnIndex(right_schema, pair.right) orelse return Error.ColumnNotFound;
        try appendJoinKeyCasts(
            aa,
            &left_casts,
            &right_casts,
            pair.left,
            left_schema[left_idx].type,
            pair.right,
            right_schema[right_idx].type,
        );
    }

    for (spec.ranges) |rp| {
        const left_idx = columnIndex(left_schema, rp.left) orelse return Error.ColumnNotFound;
        const right_idx = columnIndex(right_schema, rp.right) orelse return Error.ColumnNotFound;
        try appendJoinKeyCasts(
            aa,
            &left_casts,
            &right_casts,
            rp.left,
            left_schema[left_idx].type,
            rp.right,
            right_schema[right_idx].type,
        );
    }

    var left_out = left;
    var right_out = right;
    if (left_casts.items.len > 0) {
        left_out = try left_out.compute(try left_casts.toOwnedSlice(aa));
    }
    if (right_casts.items.len > 0) {
        right_out = try right_out.compute(try right_casts.toOwnedSlice(aa));
    }
    return .{ .left = left_out, .right = right_out };
}

fn appendJoinKeyCasts(
    aa: Allocator,
    left_casts: *std.ArrayList(exec.Derived),
    right_casts: *std.ArrayList(exec.Derived),
    left_name: []const u8,
    left_type: Type,
    right_name: []const u8,
    right_type: Type,
) !void {
    if (!joinKeyCoercionEligible(left_name) and !joinKeyCoercionEligible(right_name)) return;
    const lt = typeTag(left_type);
    const rt = typeTag(right_type);
    if (lt == rt or (isStringTag(lt) and isStringTag(rt))) return;
    const target = commonJoinKeyTag(lt, rt) orelse return;
    if (lt != target) try appendCastDerived(aa, left_casts, left_name, target);
    if (rt != target) try appendCastDerived(aa, right_casts, right_name, target);
}

fn joinKeyCoercionEligible(name: []const u8) bool {
    return std.mem.indexOfScalar(u8, name, '.') != null or
        std.mem.startsWith(u8, name, "__join_on_");
}

fn appendCastDerived(
    aa: Allocator,
    casts: *std.ArrayList(exec.Derived),
    name: []const u8,
    target: TypeTag,
) !void {
    for (casts.items) |existing| {
        if (types.columnNameEql(existing.name, name)) return;
    }
    const fn_name = castFunctionName(target) orelse return;
    const args = try aa.alloc(exec.Expr, 1);
    args[0] = .{ .col_ref = try aa.dupe(u8, name) };
    try casts.append(aa, .{
        .name = try aa.dupe(u8, name),
        .expr = .{ .call = .{
            .fn_name = try aa.dupe(u8, fn_name),
            .args = args,
        } },
    });
}

fn commonJoinKeyTag(left: TypeTag, right: TypeTag) ?TypeTag {
    if (isStringTag(left) and canStringifyJoinKey(right)) return .string;
    if (isStringTag(right) and canStringifyJoinKey(left)) return .string;
    if (cast.castCost(left, right) != null and castFunctionName(right) != null) return right;
    if (cast.castCost(right, left) != null and castFunctionName(left) != null) return left;
    return null;
}

fn canStringifyJoinKey(tag: TypeTag) bool {
    return switch (tag) {
        .int, .bigint, .double, .boolean => true,
        .string, .varchar, .char, .json => true,
        else => false,
    };
}

fn castFunctionName(tag: TypeTag) ?[]const u8 {
    return switch (tag) {
        .tinyint => "to_tinyint",
        .smallint => "to_smallint",
        .int => "to_int",
        .bigint => "to_bigint",
        .largeint => "to_largeint",
        .double => "to_double",
        .date => "to_date",
        .datetime => "to_datetime",
        .string => "to_string",
        else => null,
    };
}

fn typeTag(t: Type) TypeTag {
    return std.meta.activeTag(t);
}

/// Number of rows emitted per output batch. Bounded so emission stays
/// streaming even when one probe row matches many build rows.
const output_batch_rows: usize = 1024;

/// Sentinel in FastTable.heads / .next chains and in build_rows_scratch
/// (where it marks a preserved-side probe miss row). Never a valid build
/// row index — build sides are capped well below u32 max.
const FAST_EMPTY = std.math.maxInt(u32);

const FastKeyKind = enum { int, string, compound };

/// Compile-time mirror of tryBuildFastTable's key-type gate.
fn fastKindOfType(t: TypeTag) ?FastKeyKind {
    return switch (t) {
        .int, .bigint, .date, .datetime, .tinyint, .smallint, .boolean => .int,
        .varchar, .string, .char, .json => .string,
        else => null,
    };
}

/// Most key columns a compound FastTable carries. More falls back to the
/// general compound-key hash path (never seen in practice).
const MAX_FAST_KEYS: usize = 8;

/// Digest one key cell for compound-key slotting: int-family values by
/// their widened 64-bit pattern, strings by wyhash of the bytes. Only used
/// for slot selection — chain entries verify real equality per cell.
fn fastCellDigest(view: ColumnView, row: u32) u64 {
    return switch (view.data) {
        .int, .bigint, .date, .datetime, .tinyint, .smallint, .boolean => fastIntKey(view, row),
        .varchar, .string, .char, .json => std.hash.Wyhash.hash(0, stringRowBytes(view, row)),
        else => unreachable,
    };
}

/// Order-sensitive combine of the per-cell digests of `views` at `row`.
/// Build and probe use the SAME pair order (each side's key views in
/// `on`-spec order), so equal key tuples digest identically.
fn fastCompoundDigest(views: []const ColumnView, row: u32) u64 {
    var h: u64 = 0x9e3779b97f4a7c15;
    for (views) |v| h = fastMix(h ^ fastCellDigest(v, row));
    return h;
}

fn anyViewNull(views: []const ColumnView, row: u32) bool {
    for (views) |v| if (!v.isValid(row)) return true;
    return false;
}

/// Real per-cell equality between a probe row and a build row across every
/// key pair — the compound chain's digest-collision guard.
fn compoundRowsEqual(probe_views: []const ColumnView, probe_row: u32, build_views: []const ColumnView, build_row: u32) bool {
    for (probe_views, build_views) |pv, bv| {
        if (!compareCellsOp(pv, probe_row, bv, build_row, .eq)) return false;
    }
    return true;
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
    /// (build_columns are immutable from then on). Single-key kinds only.
    build_key_view: ColumnView,
    /// `.compound` only: one view per key column, in `on`-pair order.
    /// Arena-owned; empty for single-key kinds.
    build_key_views: []const ColumnView = &.{},
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
        .varchar, .string, .char, .json => |sv| sv.rowBytes(row),
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
    /// Per-side join key column NAMES (arena copies). The index slices
    /// above are resolved against the create-time schemas and go stale
    /// when later projection pushdown narrows a side — anything running
    /// at execution time (the build-key prune offer) must re-resolve by
    /// name against the side's CURRENT outputSchema().
    left_key_names: []const []const u8,
    right_key_names: []const []const u8,

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

    /// Shape qualifies for the FastTable probe (single join key of an
    /// int/string-family type, no range predicates — decided at create).
    /// When set, buildPhase skips populating the legacy compound-key
    /// hash_table entirely; tryBuildFastTable supplies the probe table.
    fast_eligible: bool = false,
    /// Vectorized single-key probe fast path, built at the end of
    /// buildPhase when the join shape qualifies (see tryBuildFastTable).
    /// Null = the general row-at-a-time probe runs instead.
    fast_table: ?FastTable = null,
    /// True while every non-NULL build key is distinct — tracked for free
    /// during buildPhase (compound-key map) / tryBuildFastTable (chains).
    /// String-digest collisions conservatively clear it. With a preserved
    /// probe side, uniqueness guarantees exactly one output row per probe
    /// row, in probe order — the gate for the pass-through probe.
    build_keys_unique: bool = true,
    /// Pass-through probe mode, decided once after buildPhase (see
    /// passThroughEligible): every probe row emits exactly once in order,
    /// so probe-side output columns are borrowed views straight from the
    /// probe batch (zero copy) and only build-side columns are gathered
    /// (or bulk-NULLed when the build is empty). Replaces both the
    /// per-cell general emit and the pair-gather emit for this shape.
    passthrough: bool = false,
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
    /// A probe-fused join ABOVE this one in a left-deep chain: our per-chunk
    /// joined batches feed straight into its sink (same chunk index, same
    /// worker thread), so the whole join tail runs inside the scan's stripe
    /// workers. Set via tryFuseProbe forwarding; not owned.
    chained_sink: ?exec.ProbeSink = null,

    /// Output staging: ColumnStores we append matched rows into,
    /// emitted as a single Batch when full or when probe is exhausted.
    output_columns: []ColumnStore,
    views: []ColumnView,

    /// State machine.
    phase: Phase = .building,
    /// Empty-probe short-circuit: the first probe batch is peeked before
    /// the build phase. If the probe is empty, a non-FULL join produces
    /// nothing, so we skip building the (potentially huge) build side
    /// entirely. When the probe is non-empty the peeked batch is stashed
    /// here and replayed first by `nextProbe`, so the probe side is never
    /// double-pulled. Gated off when a skew hand-off could transfer the
    /// probe side to an SMJ that wouldn't see this stashed batch.
    peeked_probe: ?Batch = null,
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
        var coerce_arena = std.heap.ArenaAllocator.init(allocator);
        defer coerce_arena.deinit();
        const coerce_aa = coerce_arena.allocator();
        const normalized = try normalizeJoinKeyTypes(coerce_aa, left, right, spec);
        const left_in = normalized.left;
        const right_in = normalized.right;

        // Resolve algorithm. Opaque predicate forces NLJ (the only
        // algorithm that evaluates per-pair callbacks). Otherwise
        // .auto picks range_sweep for the specialized pure-single-
        // range shape; nested_loop for empty `on`; the equi-driven
        // algorithms via chooseAlgorithm.
        const chosen = if (spec.opaque_predicate != null)
            .nested_loop
        else if (spec.algorithm == .auto)
            (if (canUseRangeSweep(spec)) .range_sweep else if (spec.on.len == 0) .nested_loop else chooseAlgorithm(left_in, right_in, spec.on))
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
            return @import("range_sweep.zig").RangeSweepJoin.create(allocator, left_in, right_in, rs_spec);
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
            return @import("nlj.zig").NestedLoopJoin.create(allocator, left_in, right_in, nl_spec);
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
            return @import("smj.zig").SortMergeJoin.create(allocator, left_in, right_in, sm_spec);
        }

        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const aa = arena.allocator();

        const left_schema = left_in.outputSchema();
        const right_schema = right_in.outputSchema();

        // Resolve key column indices on each side.
        const left_keys = try aa.alloc(usize, spec.on.len);
        const right_keys = try aa.alloc(usize, spec.on.len);
        const left_key_names = try aa.alloc([]const u8, spec.on.len);
        const right_key_names = try aa.alloc([]const u8, spec.on.len);
        for (spec.on, 0..) |pair, i| {
            left_keys[i] = columnIndex(left_schema, pair.left) orelse return Error.ColumnNotFound;
            right_keys[i] = columnIndex(right_schema, pair.right) orelse return Error.ColumnNotFound;
            left_key_names[i] = try aa.dupe(u8, left_schema[left_keys[i]].name);
            right_key_names[i] = try aa.dupe(u8, right_schema[right_keys[i]].name);
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
        for (right_schema, 0..) |rc, ri| {
            if (!right_kept_mask[ri]) continue;
            for (left_schema) |lc| {
                if (types.columnNameEql(lc.name, rc.name)) {
                    right_kept_mask[ri] = false;
                    break;
                }
            }
        }

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
        // Duplicate right-side names were already removed from
        // right_kept_mask above; this final collision check protects
        // against duplicates within the kept right-side schema.
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
            .left = left_in,
            .right = right_in,
            .join_type = spec.join_type,
            .left_key_names = left_key_names,
            .right_key_names = right_key_names,
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
        self.fast_eligible = blk: {
            if (resolved_ranges.len != 0 or spec.on.len == 0 or spec.on.len > MAX_FAST_KEYS) break :blk false;
            const bkeys = if (build_is_left) left_keys else right_keys;
            for (bkeys) |ki| {
                if (fastKindOfType(build_schema[ki].type) == null) break :blk false;
            }
            break :blk true;
        };

        // Commit the parallel probe at COMPILE time when the shape is
        // FastTable-eligible — fusion must be settled before any operator
        // above (a two-phase GROUP BY combine) composes against it. The
        // FastTable itself is still built after the build phase; by the
        // time the probe side first pulls (and the sink runs), it exists.
        // FULL stays serial (matched_build writes would race) but is still
        // fast_eligible — its serial probe uses the FastTable too.
        if (self.fast_eligible and spec.join_type != .full) {
            const probe = if (build_is_left) self.right else self.left;
            self.probe_fused = probe.tryFuseProbe(.{
                .ctx = self,
                .out_schema = self.output_schema,
                .bind = sinkBind,
                .process = sinkProcess,
            }) catch false;
        }
        if (getenv("THINDB_TRACE_JOINFUSE") != null) {
            std.debug.print("[jf] create {s} fast={} on={d} ranges={d} extra={} build_left={} out_cols={d} -> probe_fused={}\n", .{
                @tagName(spec.join_type),
                self.fast_eligible,
                spec.on.len,
                resolved_ranges.len,
                spec.extra_predicate != null,
                build_is_left,
                output_schema.len,
                self.probe_fused,
            });
            if (!self.probe_fused) {
                var buf: std.ArrayList(u8) = .empty;
                defer buf.deinit(std.heap.page_allocator);
                const probe = if (build_is_left) self.right else self.left;
                probe.explain(&buf, std.heap.page_allocator, 0) catch {};
                var it = std.mem.splitScalar(u8, buf.items, '\n');
                var depth: usize = 0;
                while (it.next()) |line| : (depth += 1) {
                    if (depth >= 14 or line.len == 0) break;
                    std.debug.print("[jf]     probe: {s}\n", .{std.mem.trim(u8, line, " ")});
                }
            }
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

    /// A join whose probe side is THIS join (a left-deep join tail) offers
    /// its sink here. When this join is itself probe-fused, chain it: bind
    /// the upper sink at the bottom scan over the same chunk stripes, then
    /// have our sinkProcess feed each joined batch straight into it — the
    /// whole tail becomes one parallel pipeline. Each join still builds its
    /// own (small) build side serially before its probe first pulls, so the
    /// natural next() cascade completes every build before workers spawn.
    pub fn tryFuseProbe(self: *Join, sink: exec.ProbeSink) !bool {
        const trace = getenv("THINDB_TRACE_JOINFUSE") != null;
        if (!self.probe_fused or self.chained_sink != null) {
            if (trace) std.debug.print("[jf]   inner-join decline: probe_fused={} chained={}\n", .{ self.probe_fused, self.chained_sink != null });
            return false;
        }
        const probe = if (self.build_is_left) self.right else self.left;
        if (!(try probe.rechainProbeSink(sink))) {
            if (trace) std.debug.print("[jf]   inner-join decline: rechain refused below\n", .{});
            return false;
        }
        if (sink.probe_map) |m| {
            for (self.probe_chunks) |*ch| {
                ch.chain_views = try self.probe_chunk_alloc.alloc(ColumnView, m.len);
            }
        }
        self.chained_sink = sink;
        return true;
    }

    pub fn probeFusionReachable(self: *const Join) bool {
        return self.probe_fused and self.chained_sink == null;
    }

    /// A rechain passing through this join: intermediate joins already emit
    /// through their own chained sinks, so the new sink (kept by the CALLER,
    /// the pipeline's current tail) just continues down the probe side to
    /// the ParallelScan at the bottom.
    pub fn rechainProbeSink(self: *Join, sink: exec.ProbeSink) anyerror!bool {
        if (!self.probe_fused) return false;
        const probe = if (self.build_is_left) self.right else self.left;
        return probe.rechainProbeSink(sink);
    }

    /// Feed a joined batch into the chained (upper) join's sink, or pass it
    /// through when this join is the top of the fused pipeline. Null from
    /// the upper sink (zero output rows) propagates — the scan worker pulls
    /// the next batch.
    fn chainEmit(self: *Join, chunk: usize, b: Batch) anyerror!?Batch {
        const cs = self.chained_sink orelse return b;
        var out = b;
        if (cs.probe_map) |m| {
            const vs = self.probe_chunks[chunk].chain_views;
            for (m, vs) |src, *v| v.* = b.values[src];
            out = .{ .schema = b.schema, .values = vs, .row_count = b.row_count };
        }
        return try cs.process(cs.ctx, chunk, out);
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
        const ur = self.estimateUpperRows(l, r);
        // A column's distinct count can never exceed the row count — re-cap the
        // concatenated per-side NDVs at this join's output ceiling (idempotent).
        exec.capColStats(@constCast(self.cached_stats), ur);
        return .{ .upper_rows = ur, .column_stats = self.cached_stats };
    }

    /// Row-count ceiling for the join output. With no equi key the honest bound
    /// is the cartesian product. With equi keys, the System-R estimate
    /// |L⋈R| ≈ |L|·|R| / ∏ max(V(L,kᵢ),V(R,kᵢ)) — computed in u128 so the
    /// |L|·|R| product can't overflow and saturate to maxInt, which used to
    /// blind every GROUP BY router downstream of a join (the deep-CTE-chain
    /// `est_groups=maxInt → SORT` misroute).
    ///
    /// `reduced` tracks whether ANY key gave a real selectivity signal. An
    /// OUTER join with no signal is overwhelmingly an FK/dimension lookup
    /// (~1 match per preserved row), so its realistic estimate is the
    /// preserved side's row count, NOT the cartesian product — the product
    /// is the pathological worst case and inflating to it cascades into a
    /// runaway ceiling down a deep chain. (The shared MemoryPool admission
    /// is the true OOM backstop, so a realistic-not-worst-case estimate is
    /// the right call for routing.)
    fn estimateUpperRows(self: *Join, l: exec.PipelineStats, r: exec.PipelineStats) u64 {
        const lr = l.upper_rows;
        const rr = r.upper_rows;
        if (self.left_key_indices.len == 0) {
            return std.math.mul(u64, lr, rr) catch std.math.maxInt(u64);
        }
        var divisor: u128 = 1;
        var reduced = false;
        for (self.left_key_indices, self.right_key_indices) |li, ri| {
            const lv: ?u64 = if (li < l.column_stats.len) switch (l.column_stats[li].ndv) {
                .exact => |n| n,
                .unknown => null,
            } else null;
            const rv: ?u64 = if (ri < r.column_stats.len) switch (r.column_stats[ri].ndv) {
                .exact => |n| n,
                .unknown => null,
            } else null;
            if (lv == null and rv == null) continue;
            reduced = true;
            const d: u64 = if (lv) |a| (if (rv) |b| @max(a, b) else a) else rv.?;
            if (d > 1) divisor *|= d;
        }
        const prod: u128 = @as(u128, lr) * @as(u128, rr);
        const est128: u128 = prod / divisor;
        const est: u64 = if (est128 > std.math.maxInt(u64)) std.math.maxInt(u64) else @intCast(est128);
        // An OUTER join preserves every row of its preserved side and, being a
        // lookup, matches ~1 row on the other — so its row count is the
        // preserved side's, NOT the cartesian product (System-R over a deep
        // chain of correlated keys compounds into a runaway ceiling; the
        // preserved-side count is the bound the data actually guarantees as a
        // floor and the FK case hits as the exact value). INNER has no
        // preserved side, so it keeps the System-R estimate (product when no
        // key gives a selectivity signal).
        // An equi-join with a selectivity signal is FK-like: its output can't
        // exceed the larger input (the fact side; the smaller is the ~unique
        // dimension). Capping the System-R estimate at max(lr,rr) absorbs the
        // composite-key-uniqueness it can't see from per-column NDVs. With NO
        // signal we keep the honest cartesian product.
        return switch (self.join_type) {
            .inner => if (reduced) @min(est, @max(lr, rr)) else (std.math.mul(u64, lr, rr) catch std.math.maxInt(u64)),
            .left => lr,
            .right => rr,
            .full => lr +| rr,
        };
    }

    pub fn next(self: *Join) !?Batch {
        // Skew auto-route: if buildPhase detected heavy skew and
        // handed off to SMJ, all subsequent next() calls delegate.
        if (self.skew_smj) |*sm| return sm.next();

        while (true) {
            switch (self.phase) {
                .building => {
                    // Empty-probe short-circuit: a non-FULL join whose
                    // probe (preserved) side is empty produces nothing, so
                    // skip building the other side entirely. Gated off when
                    // a skew hand-off could fire (INNER only) and transfer
                    // the probe side to an SMJ that wouldn't see the peek.
                    if (self.peeked_probe == null and
                        self.join_type != .full and
                        !self.probe_fused and
                        !(self.join_type == .inner and self.skew_detector != null))
                    {
                        if (try self.nextProbe()) |first| {
                            self.peeked_probe = first;
                        } else {
                            self.phase = .done;
                            return null;
                        }
                    }
                    try self.buildPhase();
                    if (self.skew_smj) |*sm| return sm.next();
                    // Empty-build short-circuit: with nothing to match and
                    // no preserved probe side, the join produces nothing —
                    // skip streaming the probe entirely.
                    if (self.build_rows == 0 and !self.probeSidePreserved()) {
                        self.phase = .done;
                        return null;
                    }
                    // Sideways info passing: an INNER join only emits probe
                    // rows whose keys fall inside the build keys' [min, max],
                    // so offer that range to the probe subtree as prune hints
                    // (zonemap row-group/segment skips). Only when the probe
                    // hasn't started — Scan.addPrune isn't safe on a running
                    // scan, and a peeked batch means workers are live.
                    if (self.join_type == .inner and self.peeked_probe == null and self.build_rows > 0) {
                        self.offerBuildKeyPrunes();
                    }
                    // FULL OUTER needs a matched-row bitmap so we can
                    // emit the unmatched build rows at the end.
                    if (self.join_type == .full) {
                        self.matched_build = try std.DynamicBitSetUnmanaged.initEmpty(
                            self.allocator,
                            self.build_rows,
                        );
                    }
                    self.passthrough = self.passThroughEligible();
                    self.phase = .probing;
                },
                .probing => {
                    if (self.probe_fused) {
                        if (try self.nextProbe()) |batch| return batch;
                        self.phase = .done;
                        return null;
                    }
                    const step = if (self.passthrough)
                        try self.probeStepPass()
                    else if (self.fast_table) |*ft|
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

    /// Derive per-key-column [min, max] over the finished build side and
    /// offer them to the probe subtree as prune hints. Each column's range
    /// is independently superset-safe (a probe row outside ANY key column's
    /// build range cannot match), NULL build keys never match an INNER join
    /// so validity-skipping them tightens the range for free, and Value
    /// comparison is byte order for strings — the same order the zonemap
    /// stats use. Best-effort: any unsupported type or rejection just means
    /// no pruning for that column.
    fn offerBuildKeyPrunes(self: *Join) void {
        const build_names = if (self.build_is_left) self.left_key_names else self.right_key_names;
        const probe_names = if (self.build_is_left) self.right_key_names else self.left_key_names;
        // Resolve by NAME against the CURRENT schemas: the create-time index
        // slices go stale when projection pushdown narrows a side, and
        // build_columns follow the runtime batch layout.
        const build_schema = if (self.build_is_left) self.left.outputSchema() else self.right.outputSchema();
        const probe_schema = if (self.build_is_left) self.right.outputSchema() else self.left.outputSchema();
        var probe = if (self.build_is_left) &self.right else &self.left;
        const jtrace = getenv("THINDB_TRACE_JOINFUSE") != null;
        keys: for (build_names, probe_names) |bname, pname| {
            const bci = columnIndex(build_schema, bname) orelse continue;
            const pci = columnIndex(probe_schema, pname) orelse continue;
            if (bci >= self.build_columns.len) continue;
            // Mismatched physical types would make the Value comparison in
            // predicate eval subtly wrong — only prune same-typed keys.
            if (!std.meta.eql(build_schema[bci].type, probe_schema[pci].type)) continue;
            const view = self.build_columns[bci].view();
            var mn: ?types.Value = null;
            var mx: ?types.Value = null;
            var row: usize = 0;
            while (row < self.build_rows) : (row += 1) {
                if (!view.isValid(row)) continue;
                const v = keyValueAt(view, row) orelse continue :keys;
                if (mn == null or v.compare(mn.?) == .lt) mn = v;
                if (mx == null or v.compare(mx.?) == .gt) mx = v;
            }
            const lo = mn orelse continue;
            const hi = mx orelse continue;
            probe.addPrune(.{ .col = pname, .op = .gte, .val = lo }) catch continue;
            probe.addPrune(.{ .col = pname, .op = .lte, .val = hi }) catch {};
            if (jtrace) {
                std.debug.print("[jf] build-key prune offered on '{s}' ({d} build rows)\n", .{ pname, self.build_rows });
            }
        }
    }

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
            if (self.fast_eligible) {
                // The FastTable (built once after the drain, straight from
                // build_columns) replaces the compound-key map for this
                // shape — per-row getOrPut + arena key dupes here would be
                // pure waste. Only the skew detector still wants compound
                // keys, and only for its 1-in-N sample.
                if (self.skew_detector) |det| {
                    var i: u32 = 0;
                    while (i < n) : (i += 1) {
                        if ((self.build_rows + i) % self.skew_sample_interval != 0) continue;
                        if (anyKeyNull(batch, key_indices, i)) continue;
                        self.key_scratch.clearRetainingCapacity();
                        try buildCompoundKey(self.allocator, &self.key_scratch, batch, key_indices, i);
                        try det.observe(self.key_scratch.items);
                    }
                }
                self.build_rows += @intCast(n);
                continue;
            }

            // Insert into hash table per row.
            var i: u32 = 0;
            while (i < n) : (i += 1) {
                // Skip rows where any join key is null — SQL semantic:
                // NULL never matches anything. The row still occupies its
                // slot in build_columns (it was appended above), so it
                // must NOT bump build_rows here — `build_rows + i` is the
                // row's physical index and the post-loop advance already
                // counts every batch row. Bumping here shifted every
                // subsequent id and over-counted the total (OOB reads in
                // emit and the FULL drain).
                if (anyKeyNull(batch, key_indices, i)) continue;
                self.key_scratch.clearRetainingCapacity();
                try buildCompoundKey(self.allocator, &self.key_scratch, batch, key_indices, i);

                const aa = self.arena.allocator();
                const gop = try self.hash_table.getOrPut(aa, self.key_scratch.items);
                if (!gop.found_existing) {
                    // Dup the key into the arena — scratch is reused.
                    gop.key_ptr.* = try aa.dupe(u8, self.key_scratch.items);
                    gop.value_ptr.* = .empty;
                } else {
                    self.build_keys_unique = false;
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
        if (!self.fast_eligible) return;
        const aa = self.arena.allocator();
        const build_key_indices = if (self.build_is_left) self.left_key_indices else self.right_key_indices;
        const key_view = self.build_columns[build_key_indices[0]].view();
        const kind: FastKeyKind = if (build_key_indices.len > 1) .compound else switch (key_view.data) {
            .int, .bigint, .date, .datetime, .tinyint, .smallint, .boolean => .int,
            .varchar, .string, .char, .json => .string,
            else => return,
        };
        var key_views: []ColumnView = &.{};
        if (kind == .compound) {
            key_views = try aa.alloc(ColumnView, build_key_indices.len);
            for (build_key_indices, key_views) |ki, *v| v.* = self.build_columns[ki].view();
        }

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
            const key = switch (kind) {
                .int => blk: {
                    if (!key_view.isValid(row)) continue;
                    break :blk fastIntKey(key_view, row);
                },
                .string => blk: {
                    if (!key_view.isValid(row)) continue;
                    break :blk std.hash.Wyhash.hash(0, stringRowBytes(key_view, row));
                },
                .compound => blk: {
                    if (anyViewNull(key_views, row)) continue;
                    break :blk fastCompoundDigest(key_views, row);
                },
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
                    // Same key (or same digest, for string/compound kinds —
                    // treated as a duplicate conservatively) already
                    // resident: chains of length > 1 exist, so pass-through
                    // can't assume one match per probe row.
                    self.build_keys_unique = false;
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
            .build_key_views = key_views,
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

    /// Probe-side `.next()` with a one-batch replay buffer. The
    /// empty-probe short-circuit peeks the first probe batch before the
    /// build phase; `nextProbe` returns that stashed batch first so the
    /// build side is never drained when the probe is non-empty.
    fn nextProbe(self: *Join) !?Batch {
        if (self.peeked_probe) |b| {
            self.peeked_probe = null;
            return b;
        }
        var probe = if (self.build_is_left) &self.right else &self.left;
        return probe.next();
    }

    /// Vectorized probe: consume whole probe batches, collect all
    /// (probe row, build row) match pairs into the scratch arrays,
    /// then bulk-gather every output column and emit one batch per
    /// probe batch. Replaces the per-row compound-key build +
    /// StringHashMap lookup + per-cell appends of the general path.
    fn probeStepFast(self: *Join, ft: *const FastTable) !?Batch {
        while (true) {
            const batch = (try self.nextProbe()) orelse return null;
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
    /// The probe side's key views in `on`-pair order (compound kind), into
    /// caller-provided storage.
    fn probeKeyViews(self: *const Join, batch: Batch, storage_buf: *[MAX_FAST_KEYS]ColumnView) []const ColumnView {
        const indices = if (self.build_is_left) self.right_key_indices else self.left_key_indices;
        for (indices, 0..) |ki, i| storage_buf[i] = batch.values[ki];
        return storage_buf[0..indices.len];
    }

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
        var pkv_buf: [MAX_FAST_KEYS]ColumnView = undefined;
        const probe_views = if (ft.kind == .compound) self.probeKeyViews(batch, &pkv_buf) else &.{};

        probe_rows.clearRetainingCapacity();
        build_rows.clearRetainingCapacity();
        try probe_rows.ensureUnusedCapacity(alloc, n);
        try build_rows.ensureUnusedCapacity(alloc, n);

        var i: u32 = 0;
        while (i < n) : (i += 1) {
            const key_null = if (ft.kind == .compound) anyViewNull(probe_views, i) else !key_view.isValid(i);
            if (key_null) {
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
                .compound => {
                    const key = fastCompoundDigest(probe_views, i);
                    var slot = key & ft.mask;
                    while (true) {
                        const head = ft.heads[slot];
                        if (head == FAST_EMPTY) break;
                        if (ft.slot_keys[slot] == key) {
                            var r = head;
                            while (r != FAST_EMPTY) : (r = ft.next[r]) {
                                if (!compoundRowsEqual(probe_views, i, ft.build_key_views, r)) continue;
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
    // Pass-through probe: when the probe side is preserved and the build
    // guarantees at most one match per probe row (empty, or all keys
    // unique), every probe row emits exactly once in probe order. The
    // output batch is then row-aligned with the probe batch, so probe-
    // side columns are borrowed views (zero copy) and only build-side
    // columns get gathered — or bulk-NULLed when the build is empty.
    // Serial probe and fused workers share resolve + emit below.
    // -----------------------------------------------------------------

    /// Decided once, right after buildPhase.
    fn passThroughEligible(self: *const Join) bool {
        if (self.skew_smj != null) return false;
        if (self.probeSidePreserved()) {
            // Empty build: every probe row null-extends exactly once; FULL's
            // drain has nothing to add, so even FULL qualifies here.
            if (self.build_rows == 0) return true;
            // Non-empty FULL can't pass through — the drain phase appends
            // unmatched build rows after the probe.
            if (self.join_type == .full) return false;
            return self.build_keys_unique;
        }
        // INNER with a unique-key build emits each probe row 0 or 1 times.
        // The pass-through emit is row-aligned with the probe batch, so it
        // applies per BATCH: only when every row matched (the common
        // FK-lookup case) — a batch with misses falls back to the pair
        // gather (passBatchAllMatched / compactPassPairs).
        return self.join_type == .inner and self.build_rows > 0 and self.build_keys_unique;
    }

    /// Preserved joins pass through unconditionally (misses null-extend);
    /// INNER pass-through requires the whole batch to have matched.
    fn passBatchAllMatched(self: *const Join, rows: []const u32) bool {
        if (self.probeSidePreserved()) return true;
        for (rows) |r| {
            if (r == FAST_EMPTY) return false;
        }
        return true;
    }

    /// Fallback for an INNER pass-through batch with misses: compact the
    /// per-row match slots in place into an aligned (probe, build) pair
    /// list for the standard gather emit.
    fn compactPassPairs(
        alloc: Allocator,
        build_rows: *std.ArrayListUnmanaged(u32),
        probe_rows: *std.ArrayListUnmanaged(u32),
    ) !void {
        probe_rows.clearRetainingCapacity();
        try probe_rows.ensureUnusedCapacity(alloc, build_rows.items.len);
        var w: usize = 0;
        for (build_rows.items, 0..) |m, i| {
            if (m == FAST_EMPTY) continue;
            probe_rows.appendAssumeCapacity(@intCast(i));
            build_rows.items[w] = m;
            w += 1;
        }
        build_rows.items.len = w;
    }

    fn probeStepPass(self: *Join) !?Batch {
        while (true) {
            const batch = (try self.nextProbe()) orelse return null;
            if (batch.row_count == 0) continue;
            try self.resolvePassMatches(batch, self.allocator, &self.build_rows_scratch);
            if (self.passBatchAllMatched(self.build_rows_scratch.items)) {
                return try self.emitPassThrough(batch, self.build_rows_scratch.items, self.output_columns, self.views, self.allocator);
            }
            try compactPassPairs(self.allocator, &self.build_rows_scratch, &self.probe_rows_scratch);
            if (self.probe_rows_scratch.items.len == 0) continue;
            for (self.output_columns) |*c| c.clear();
            try self.gatherPairs(batch, self.probe_rows_scratch.items, self.build_rows_scratch.items, self.output_columns, self.allocator);
            for (self.output_columns, self.views) |*c, *v| v.* = c.view();
            return Batch{
                .schema = self.output_schema,
                .values = self.views,
                .row_count = self.output_columns[0].data.rowCount(),
            };
        }
    }

    /// First build row whose key equals probe row `i`'s key, or FAST_EMPTY.
    /// In pass-through mode chains are length 1 (uniqueness gate), so
    /// "first" is "only" — the byte/cell verification still guards digest
    /// equality against genuine mismatches. `probe_views` is read only by
    /// the compound kind.
    fn fastLookupFirst(ft: *const FastTable, key_view: ColumnView, probe_views: []const ColumnView, i: u32) u32 {
        switch (ft.kind) {
            .int => {
                const key = fastIntKey(key_view, i);
                var slot = fastMix(key) & ft.mask;
                while (true) {
                    if (ft.heads[slot] == FAST_EMPTY) return FAST_EMPTY;
                    if (ft.slot_keys[slot] == key) return ft.heads[slot];
                    slot = (slot + 1) & ft.mask;
                }
            },
            .string => {
                const bytes = stringRowBytes(key_view, i);
                const key = std.hash.Wyhash.hash(0, bytes);
                var slot = key & ft.mask;
                while (true) {
                    const head = ft.heads[slot];
                    if (head == FAST_EMPTY) return FAST_EMPTY;
                    if (ft.slot_keys[slot] == key) {
                        var r = head;
                        while (r != FAST_EMPTY) : (r = ft.next[r]) {
                            if (std.mem.eql(u8, stringRowBytes(ft.build_key_view, r), bytes)) return r;
                        }
                        return FAST_EMPTY;
                    }
                    slot = (slot + 1) & ft.mask;
                }
            },
            .compound => {
                const key = fastCompoundDigest(probe_views, i);
                var slot = key & ft.mask;
                while (true) {
                    const head = ft.heads[slot];
                    if (head == FAST_EMPTY) return FAST_EMPTY;
                    if (ft.slot_keys[slot] == key) {
                        var r = head;
                        while (r != FAST_EMPTY) : (r = ft.next[r]) {
                            if (compoundRowsEqual(probe_views, i, ft.build_key_views, r)) return r;
                        }
                        return FAST_EMPTY;
                    }
                    slot = (slot + 1) & ft.mask;
                }
            },
        }
    }

    /// Resolve the (at most one) build match per probe row into `out_rows`
    /// — exactly one entry per probe row, FAST_EMPTY marking a miss. Range
    /// predicates reject in place (the row null-extends, preserving the
    /// one-emit-per-probe-row alignment). Thread-safe for fused workers:
    /// the compound-key branch (which reuses self.key_scratch) is
    /// unreachable there because fusion is only offered to FastTable
    /// shapes.
    fn resolvePassMatches(
        self: *Join,
        batch: Batch,
        alloc: Allocator,
        out_rows: *std.ArrayListUnmanaged(u32),
    ) !void {
        const n = batch.row_count;
        out_rows.clearRetainingCapacity();
        if (self.build_rows == 0) {
            try out_rows.appendNTimes(alloc, FAST_EMPTY, n);
            return;
        }
        try out_rows.ensureUnusedCapacity(alloc, n);
        const probe_key_indices = if (self.build_is_left) self.right_key_indices else self.left_key_indices;
        if (self.fast_table) |*ft| {
            const key_view = batch.values[probe_key_indices[0]];
            var pkv_buf: [MAX_FAST_KEYS]ColumnView = undefined;
            const probe_views = if (ft.kind == .compound) self.probeKeyViews(batch, &pkv_buf) else &.{};
            var i: u32 = 0;
            while (i < n) : (i += 1) {
                const key_null = if (ft.kind == .compound) anyViewNull(probe_views, i) else !key_view.isValid(i);
                var m: u32 = if (!key_null) fastLookupFirst(ft, key_view, probe_views, i) else FAST_EMPTY;
                if (m != FAST_EMPTY and self.ranges.len > 0 and !self.passesAllRanges(batch, i, m)) m = FAST_EMPTY;
                out_rows.appendAssumeCapacity(m);
            }
        } else {
            var i: u32 = 0;
            while (i < n) : (i += 1) {
                var m: u32 = FAST_EMPTY;
                if (!anyKeyNull(batch, probe_key_indices, i)) {
                    self.key_scratch.clearRetainingCapacity();
                    try buildCompoundKey(self.allocator, &self.key_scratch, batch, probe_key_indices, i);
                    if (self.hash_table.get(self.key_scratch.items)) |bucket| m = bucket.items[0];
                }
                if (m != FAST_EMPTY and self.ranges.len > 0 and !self.passesAllRanges(batch, i, m)) m = FAST_EMPTY;
                out_rows.appendAssumeCapacity(m);
            }
        }
    }

    /// Emit one output batch row-aligned with `batch`: probe-side output
    /// columns are the probe batch's own views; build-side columns gather
    /// by `rows` into this path's staging stores (cleared per emit — the
    /// previous batch's views die with the consume-before-next() contract,
    /// same as flushOutput's deferred clear).
    fn emitPassThrough(
        self: *Join,
        batch: Batch,
        rows: []const u32,
        out_cols: []ColumnStore,
        out_views: []ColumnView,
        alloc: Allocator,
    ) !Batch {
        const left_count = self.left_col_count;
        var out_idx: usize = 0;
        if (self.build_is_left) {
            var i: usize = 0;
            while (i < left_count) : (i += 1) {
                const store = &out_cols[out_idx];
                store.clear();
                try gatherBuildColumn(alloc, self.build_columns[i].view(), rows, store);
                out_views[out_idx] = store.view();
                out_idx += 1;
            }
            for (batch.values, 0..) |v, idx2| {
                if (!self.right_kept_mask[idx2]) continue;
                out_views[out_idx] = v;
                out_idx += 1;
            }
        } else {
            var i: usize = 0;
            while (i < left_count) : (i += 1) {
                out_views[out_idx] = batch.values[i];
                out_idx += 1;
            }
            for (self.build_columns, 0..) |*bc, idx2| {
                if (!self.right_kept_mask[idx2]) continue;
                const store = &out_cols[out_idx];
                store.clear();
                try gatherBuildColumn(alloc, bc.view(), rows, store);
                out_views[out_idx] = store.view();
                out_idx += 1;
            }
        }
        return .{ .schema = self.output_schema, .values = out_views, .row_count = batch.row_count };
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
        /// Remap scratch when the chained (upper) sink carries a probe_map
        /// (a Project narrowed this join's output before the upper join
        /// compiled against it): our joined batch remaps into these views
        /// before the upper sink processes it.
        chain_views: []ColumnView = &.{},
    };

    fn sinkBind(ctx: *anyopaque, n_chunks: usize, alloc: Allocator) anyerror!void {
        const self: *Join = @ptrCast(@alignCast(ctx));
        // Grow-only: a SetUnion forwarding this sink binds once per accepting
        // arm scan (compile time, nothing in flight yet) — keep the larger
        // chunk space so either arm's chunk indices stay valid.
        if (self.probe_chunks.len >= n_chunks) return;
        if (self.probe_chunks.len > 0) {
            for (self.probe_chunks) |*ch| freeProbeChunk(self.probe_chunk_alloc, ch);
            self.probe_chunk_alloc.free(self.probe_chunks);
            self.probe_chunks = &.{};
        }
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
        if (self.passthrough) {
            if (batch.row_count == 0) return null;
            try self.resolvePassMatches(batch, alloc, &ch.build_rows);
            if (self.passBatchAllMatched(ch.build_rows.items)) {
                return try self.chainEmit(chunk, try self.emitPassThrough(batch, ch.build_rows.items, ch.out_cols, ch.views, alloc));
            }
            try compactPassPairs(alloc, &ch.build_rows, &ch.probe_rows);
            if (ch.probe_rows.items.len == 0) return null;
            for (ch.out_cols) |*c| c.clear();
            try self.gatherPairs(batch, ch.probe_rows.items, ch.build_rows.items, ch.out_cols, alloc);
            for (ch.out_cols, ch.views) |*c, *v| v.* = c.view();
            return try self.chainEmit(chunk, Batch{
                .schema = self.output_schema,
                .values = ch.views,
                .row_count = ch.out_cols[0].data.rowCount(),
            });
        }
        const ft = &self.fast_table.?;
        try self.collectPairs(ft, batch, alloc, &ch.probe_rows, &ch.build_rows);
        if (ch.probe_rows.items.len == 0) return null;
        for (ch.out_cols) |*c| c.clear();
        try self.gatherPairs(batch, ch.probe_rows.items, ch.build_rows.items, ch.out_cols, alloc);
        for (ch.out_cols, ch.views) |*c, *v| v.* = c.view();
        return try self.chainEmit(chunk, Batch{
            .schema = self.output_schema,
            .values = ch.views,
            .row_count = ch.out_cols[0].data.rowCount(),
        });
    }

    fn freeProbeChunk(alloc: Allocator, ch: *ProbeChunk) void {
        ch.probe_rows.deinit(alloc);
        ch.build_rows.deinit(alloc);
        for (ch.out_cols) |*c| c.deinit(alloc);
        if (ch.out_cols.len > 0) alloc.free(ch.out_cols);
        if (ch.views.len > 0) alloc.free(ch.views);
        if (ch.chain_views.len > 0) alloc.free(ch.chain_views);
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
                try out.appendNulls(alloc, end - start);
            } else {
                while (end < rows.len and rows[end] != FAST_EMPTY) : (end += 1) {}
                try transform.appendByIndices(alloc, view, rows[start..end], out);
            }
            start = end;
        }
    }

    fn probeStep(self: *Join) !?Batch {
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
            const next_batch = (try self.nextProbe()) orelse {
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
        // Pass-through emits whole batches directly and leaves its last
        // gathered build columns in output_columns (the returned views
        // borrow them). Flushing those here would re-emit stale rows.
        if (self.passthrough) return null;
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
        .varchar, .string, .char, .json => true,
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
            .varchar, .string, .char, .json => |sv| {
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
        .json => |sv| return cmpBytesOp(sv.rowBytes(lrow), right.data.json.rowBytes(rrow), op),
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
