//! Operator pipeline — front door.
//!
//! Type-erased `Query` value backed by a small vtable. Each operator
//! (Scan, Filter, Project, Limit, Sort, Aggregate) lives in its own file
//! and exposes `next()`, `deinit()`, `outputSchema()`, `addPrune()`. The
//! vtable wires them together.
//!
//! Public re-exports of types/functions defined in sibling files appear at
//! the bottom of this file so callers can keep importing `exec.*` without
//! caring how the operators are split internally.

const std = @import("std");

test {
    _ = @import("v2_global_aggregate.zig");
    _ = @import("v2_shape_group_topn.zig");
}
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;
const engine = @import("../engine/engine.zig");

const api = @import("../api/api.zig");
const Table = api.Table;

pub const memory = @import("../memory.zig");

pub const prof = @import("../util/prof.zig");
pub const silo_group_core = @import("silo_group_core.zig");
pub const table_fn = @import("table_fn.zig");
pub const region_exec = @import("region_exec.zig");
pub const v2_group_topn_engine = @import("v2_group_topn_engine.zig");

/// Diagnostic override for the GROUP BY path selection (see net/local.zig).
/// `.auto` is normal cardinality/budget-based routing; `.hash`, `.sort`, and
/// `.radix` force the hash table, the sort+stream path, or the radix-partitioned
/// aggregate respectively, bypassing the cardinality check (`.radix` still
/// requires the query to qualify structurally: int key ≤128 bits, fixed-state
/// aggregates). `.hash` also disables radix auto-routing. For benchmarking the
/// paths on the same query only.
pub var force_group_by: enum { auto, hash, sort, radix } = .auto;

/// Diagnostic (`--trace-group-by`): when set, the GROUP BY router prints the
/// inputs to every hash-vs-sort decision — the estimated group count, per-group
/// footprint, the budget-derived cutoff, each key's propagated NDV (or `OOB`
/// when a schema/stats desync drops it, the #337 class of bug), and the chosen
/// path. Turns a multi-hour stats-propagation trace into a one-flag read.
pub var trace_group_by: bool = false;

/// Benchmark override for the scan sub-batch size (rows). 0 = use the
/// cache-aware auto size (`autoScanBatch`); >0 forces this many rows.
pub var scan_sub_batch: usize = 0;

/// Per-core L2 working-set budget for the auto scan sub-batch. 1 MiB matches
/// modern x86 (Zen ~1 MiB/core; Intel ~1.25–2 MiB). The exact value is not
/// load-bearing: the sub-batch win is robust across the 512 KiB–4 MiB L2 range
/// (→ 4K–32K-row batches all beat the full 64K row group), so a default
/// suffices and runtime probing would add little. Overridable per-deployment if
/// it ever matters.
const PER_CORE_L2_BYTES: usize = 1 << 20;

/// Cache-aware scan sub-batch size (rows) for a projection whose summed per-row
/// width is `row_bytes`. The scan emits a decoded row group in chunks of this
/// many rows so a wide multi-column aggregate's column buffers stay resident in
/// L2 instead of spilling — measured ~7% on the widest ClickBench GROUP BY (Q32)
/// with no regression on narrow queries. Targets ≈⅛ of per-core L2 (headroom for
/// the group table / accumulators that share the cache), clamped to [2048,
/// 32768] and rounded to a multiple of 64 (a sub-batch's null-bitmap start must
/// be byte-aligned). `row_bytes == 0` (count-only scan) ⇒ 0 ⇒ no sub-batching.
/// `scan_sub_batch > 0` forces a fixed size (benchmark override).
pub fn autoScanBatch(row_bytes: usize) usize {
    const raw: usize = if (scan_sub_batch > 0)
        scan_sub_batch
    else if (row_bytes == 0)
        return 0
    else
        @max(@as(usize, 2048), @min((PER_CORE_L2_BYTES / 8) / row_bytes, 32768));
    return raw & ~@as(usize, 63);
}

pub const Error = error{
    ColumnNotFound,
    TypeMismatch,
    PredicateTypeMismatch,
    /// A DATE or DATETIME is compared with a string constant the statement
    /// spells that doesn't read as a date or datetime (`d = 'abc'`,
    /// `d < '2026-09-31'`). `predicate.takeInvalidTemporalMessage` names the
    /// constant.
    InvalidTemporalLiteral,
    UnsupportedOperatorForType,
    SortNoKeys,
    AggregateNoSpecs,
    AggregateColumnRequired,
    AggregateUnsupportedType,
    AggregateInvalidParam,
    ArithmeticOverflow,
    /// A value written into a column (INSERT, UPDATE) lies outside the
    /// column type's range, as 300 for a TINYINT.
    ValueOutOfRange,
    /// A scalar subquery returned more than one row for a row that reads it.
    SubqueryMultipleRows,
    /// A subquery reads an enclosing query's columns in a form that can't be
    /// decorrelated, such as a reference two levels out.
    UnsupportedCorrelatedSubquery,
    /// A recursive CTE still produced rows after 1000 iterations (MySQL's
    /// default cte_max_recursion_depth).
    RecursiveCteDepthExceeded,
    /// Compute operator: no derived columns provided.
    ComputeNoColumns,
    /// Table-valued UDF contract violations: call-site PARTITION BY vs
    /// declared execution mode; input schema vs declared shape; callback
    /// left output columns non-rectangular.
    TableFnExecutionMismatch,
    TableFnInputMismatch,
    TableFnOutputMismatch,
    /// Compute/projection operator: duplicate derived/final output name.
    ComputeNameCollision,
    /// Compute operator: an expression shape not yet supported in v1
    /// (nested calls, literal-only derived).
    ComputeUnsupportedExpr,
    /// Compute operator: no scalar function overload matches the call's
    /// `(name, arg_types)`.
    ComputeNoSuchOverload,
    /// Compute operator: kernel call arity exceeds the internal fixed
    /// buffer (currently 16). Wider arities need a heap-allocated arg
    /// view buffer.
    ComputeTooManyArgs,
    /// Join operator: the requested join_type isn't implemented yet.
    /// v1 supports inner only; outer/semi/anti land in follow-ups.
    JoinUnsupportedType,
    /// Join operator: the `on` clause has no key pairs.
    JoinEmptyOnClause,
    /// Join operator: a key-pair's column types don't match (e.g.
    /// joining bigint to string).
    JoinKeyTypeMismatch,
    /// Join operator: left and right outputs have a colliding column
    /// name. v1 doesn't auto-alias; user must rename via .compute()
    /// or .exclude() before the join.
    JoinColumnNameCollision,
    /// A blocking operator (Sort, Aggregate, Join build, SMJ, NLJ)
    /// would exceed `Config.query_memory_budget` if it kept
    /// materializing. Aborts mid-build with a clear error rather
    /// than letting the underlying allocator OOM the process.
    MemoryBudgetExceeded,
    /// Window operator: shape unsupported in the current implementation
    /// (string-typed window output, args other than column refs for
    /// aggregates, etc.). Tier 1 ships a deliberately narrow subset.
    WindowUnsupported,
};

// ---------------------------------------------------------------------------
// Batch — the unit of data flowing between operators
// ---------------------------------------------------------------------------

pub const Batch = struct {
    /// Schema metadata for each output column (name + type), in column order.
    schema: []const Column,
    /// Borrowed column views — pointing into operator-owned buffers. Valid
    /// only until the next `Query.next()` call.
    values: []const ColumnView,
    row_count: usize,
    /// Optional per-column dictionary-code sidecar (Phase 4.2 Option A). When
    /// `coded[j]` is set, column `j` is *also* available as global dict codes
    /// (the `values[j]` view is still valid — a materialized placeholder/real
    /// column — so non-code-aware consumers ignore the sidecar and work
    /// unchanged). A code-aware consumer (the aggregate group key) reads the
    /// narrow codes instead of hashing the strings. Same per-`next()` lifetime
    /// as `values`. Null (the default) means no column is coded.
    coded: ?[]const ?CodedColumn = null,
    /// Optional per-column key-digest sidecar. When `hashed[j]` is set, column
    /// `j` (a non-nullable string group key consumed only as hashed identity —
    /// real bytes recovered later via rowref late-materialization) is available
    /// as per-row `stringKeyDigest` values, computed by the scan directly off
    /// the cached decompressed block — the dict→string expansion never runs.
    /// `values[j]` holds a placeholder; a digest-aware consumer must use the
    /// sidecar, and must fall back to `stringKeyDigest(bytes)` on batches
    /// without one (memtable, tombstoned row group) so keys stay identical.
    hashed: ?[]const ?[]const u128 = null,
    /// Optional per-column RLE run sidecar. When `runs[j]` is set, column `j`'s
    /// rows arrive as adjacent-equal runs straight off the block's RLE header:
    /// `values_i64[k]` (sign-extended to i64) repeated `lengths[k]` times
    /// reproduces the column. `values[j]` stays a real materialized view, so
    /// run-unaware consumers work unchanged; a run-aware consumer (the
    /// weighted group emitter) iterates run spans instead of rows. Attached
    /// only to whole, tombstone-free segment row groups on the unfiltered
    /// path — never memtable or compacted batches. Same per-`next()` lifetime
    /// as `values`. Filled only when the scan's `emit_runs` is requested.
    runs: ?[]const ?RunsColumn = null,

    pub fn columnIndex(self: Batch, name: []const u8) ?usize {
        for (self.schema, 0..) |c, i| {
            if (@import("../types.zig").columnNameEql(c.name, name)) return i;
        }
        return null;
    }

    pub fn columnView(self: Batch, name: []const u8) ?ColumnView {
        const idx = self.columnIndex(name) orelse return null;
        return self.values[idx];
    }
};

// ---------------------------------------------------------------------------
// Query — type-erased operator handle
// ---------------------------------------------------------------------------

/// Join-probe fusion handle (offered by Join after its build phase, accepted
/// by an unfused round-mode ParallelScan): the scan workers call `process`
/// concurrently — at most one in-flight call per chunk index — turning each
/// scanned probe batch into a joined batch. The returned batch's views alias
/// the sink's per-chunk buffers and stay valid until the next `process` call
/// with the same chunk index (the scan's round barrier guarantees the staged
/// batch is consumed first). `bind` runs once, single-threaded, with the
/// chunk count and the thread-safe allocator `process` must allocate from.
pub const ProbeSink = struct {
    ctx: *anyopaque,
    /// The join's output schema — what `process`-returned batches carry.
    /// A materialize-mode acceptor re-types its drain buffers with this.
    out_schema: []const Column,
    /// Probe-side column remap (probe-schema idx → scan-batch idx), set by
    /// a narrowing Project forwarding the offer downward. The accepting
    /// scan applies it to each batch's values BEFORE calling `process`, so
    /// the sink's compiled indices stay valid. Null = identity.
    probe_map: ?[]const usize = null,
    bind: *const fn (ctx: *anyopaque, n_chunks: usize, alloc: Allocator) anyerror!void,
    /// Returns null when this probe batch produced no output rows (the
    /// worker pulls the next scan batch); never called on an exhausted chunk.
    process: *const fn (ctx: *anyopaque, chunk: usize, batch: Batch) anyerror!?Batch,
};

/// One fully-materialized chunk handed over by `takeOwnedChunks`: owned
/// column stores in the producer's output-schema order, plus its row count.
pub const OwnedChunk = struct {
    stores: []engine.ColumnStore,
    rows: usize,
};

/// A pipeline's whole materialized output as owned per-chunk stores. `alloc`
/// owns every store's buffers AND the `chunks` / per-chunk `stores` arrays —
/// the adopter frees through it (producers allocate these from a thread-safe
/// worker allocator, not the consumer's).
pub const OwnedChunks = struct {
    chunks: []OwnedChunk,
    alloc: Allocator,
};

/// Free an `OwnedChunks` handle in full — every store's buffers plus the
/// arrays — through its owning allocator. For consumers that fail before
/// adopting (ownership transferred at take; it must not leak on the error
/// path).
pub fn deinitOwnedChunks(oc: OwnedChunks) void {
    for (oc.chunks) |c| {
        for (c.stores) |*st| st.deinit(oc.alloc);
        oc.alloc.free(c.stores);
    }
    oc.alloc.free(oc.chunks);
}

pub const VTable = struct {
    next: *const fn (ptr: *anyopaque) anyerror!?Batch,
    deinit: *const fn (ptr: *anyopaque) void,
    outputSchema: *const fn (ptr: *anyopaque) []const Column,
    /// Operators that can act on hints (e.g. Scan) use them to skip row
    /// groups; others (Filter, Project, Limit) simply forward to upstream.
    addPrune: *const fn (ptr: *anyopaque, pred: predicate.Predicate) anyerror!void,
    /// Offer a full predicate to this operator for in-place evaluation. The
    /// Scan accepts (returns true) and applies the filter directly over its
    /// borrowed cache bytes, emitting compacted owned survivors. Every other
    /// operator declines (returns false), so the caller (Filter) keeps doing
    /// its own masking. The borrowed view the Scan builds lives entirely
    /// inside one `next()` call — no cross-operator lifetime contract.
    tryFuseFilter: *const fn (ptr: *anyopaque, expr: predicate.PredicateExpr) anyerror!bool,
    /// Offer a projection Compute (derived columns) to this operator to run
    /// internally. ParallelScan accepts row-local derived (so the per-row
    /// scalar work — e.g. REGEXP_REPLACE — runs in its parallel workers); every
    /// other operator declines. A fused Filter forwards it to its upstream.
    tryFuseCompute: *const fn (ptr: *anyopaque, derived: []const @import("compute.zig").Derived, registry: ?*const @import("../udf.zig").UdfRegistry) anyerror!bool,
    /// Offer a PARTIAL aggregate to run inside this operator's parallel workers
    /// (two-phase GROUP BY): each worker aggregates its own slice on its own core
    /// — no cross-core feed — and emits partial groups; a serial combine aggregate
    /// re-aggregates them. Only ParallelScan accepts (and only for combinable
    /// aggregates); every other operator declines via the `@hasDecl` guard.
    tryFuseAggregate: *const fn (ptr: *anyopaque, group_cols: []const []const u8, aggs: []const AggSpec) anyerror!bool,
    /// Offer a join-probe sink to run inside this operator's parallel workers
    /// (see `ProbeSink`). Only an unfused round-mode ParallelScan accepts —
    /// its workers then emit already-joined batches and the offering Join
    /// becomes a pass-through. AliasRename forwards (turning pass-through
    /// itself); every other operator declines via the `@hasDecl` guard.
    tryFuseProbe: *const fn (ptr: *anyopaque, sink: ProbeSink) anyerror!bool,
    /// Rebind a NEW sink at the bottom of an already probe-fused pipeline
    /// (a join tail extending upward): pass-through layers forward to their
    /// upstream, the bottom ParallelScan binds + re-types. The CALLER (the
    /// pipeline's current tail) keeps the sink and feeds it per chunk; the
    /// layers in between are untouched. Declines by default.
    rechainProbeSink: *const fn (ptr: *anyopaque, sink: ProbeSink) anyerror!bool,
    /// Offer a full parallel lease GROUP BY replacement to this operator. Only
    /// a directly-adjacent ParallelScan should accept: it can let its scan
    /// workers build radix partitions directly and return a specialized
    /// aggregate query. Blocking operators decline, preserving the normal path.
    tryLeaseGroupBy: *const fn (ptr: *anyopaque, group_cols: []const []const u8, aggs: []const AggSpec, top_k: ?@import("../ir/ir.zig").Op.TopK, emit_limit: ?u32, dop: usize) anyerror!?Query,
    /// Pre-execution statistics on this operator's OUTPUT: upper bound
    /// on rows, sort state. Cheap — computed from manifest + operator
    /// definitions, no data read required. Used by downstream planners
    /// (Join especially) to make algorithm decisions.
    stats: *const fn (ptr: *anyopaque) PipelineStats,
    /// Per-query memory accountant. Returns the same pointer
    /// throughout the query pipeline (operators inherit from their
    /// upstream). Null = no budget tracking (default; common in tests).
    accountant: *const fn (ptr: *anyopaque) ?*memory.MemoryAccountant,
    /// Render this operator's physical plan line(s) into `out` at the given
    /// indentation `depth`, then recurse into upstream(s) at `depth + 1`.
    /// Shows the chosen physical operator + decisions (hash vs sort group-by,
    /// join algorithm, pre-sorted/sort-elided), so the tree shape reveals
    /// what the compiler picked.
    explain: *const fn (ptr: *anyopaque, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) anyerror!void,
    /// Phase 4.2: ask a Scan to emit the named string column as global dict
    /// codes (the `Batch.coded` sidecar) instead of materialized strings.
    /// Returns true iff the operator is an eligible Scan that accepted. Every
    /// other operator (via `makeQuery`'s `@hasDecl` guard) returns false.
    setDictCodeColumn: *const fn (ptr: *anyopaque, name: []const u8, dict: *global_dict.GlobalDict) bool,
    /// Phase 4.2 multi-key: non-committing pre-check — can the underlying Scan
    /// emit `name` as dict codes? The gate validates all keys before committing.
    canCodeColumn: *const fn (ptr: *anyopaque, name: []const u8) bool,
    /// Phase 4.2 multi-key: roll back a Scan's coded-column setup (gate undo).
    clearDictCodeColumns: *const fn (ptr: *anyopaque) void,
    /// Restrict this operator's output to the named columns — the only ones a
    /// downstream consumer (e.g. a GROUP BY) actually reads. Lets a parallel
    /// scan skip carrying and deep-copying columns that were decoded only to
    /// feed a fused filter/compute (e.g. `URL` behind `length(URL)` +
    /// `URL<>''`) and are dead above. The consumer must read the result through
    /// the names, since the schema may shrink. A fused (pass-through) Filter
    /// forwards it; an UNFUSED Filter swallows it (its predicate still needs
    /// those columns at runtime). Every other operator no-ops via
    /// `makeQuery`'s `@hasDecl` guard.
    setEmitProjection: *const fn (ptr: *anyopaque, keep: []const []const u8) anyerror!void,
    /// True when the column DATA (and any sidecar payloads) behind every batch
    /// this operator emits stays valid until the operator's deinit — only the
    /// view-struct arrays may be per-`next()` scratch. Loosens the Batch
    /// lifetime contract for a parallel consumer that holds batches across
    /// further `next()` calls (it must copy the view structs, never the data).
    /// True for readers over immutable materialized stages (MatScan) and pure
    /// view remaps above them (Project/AliasRename/cast-free SetUnion); false
    /// by default — any operator that materializes into reused scratch must
    /// never claim it.
    stableData: *const fn (ptr: *anyopaque) bool,
    /// Stage-adoption handshake: hand the pipeline's FULLY-materialized output
    /// over as owned per-chunk column stores instead of streaming it batch by
    /// batch — the consumer (a materializing Stage) adopts the buffers and the
    /// pull-copy's second deep copy disappears. Only a materialize-mode
    /// ParallelScan produces one (running its drain if not yet started);
    /// pass-through layers forward — a fused Filter as-is, a Project with its
    /// column remap applied. Null (the default) = stream normally. Must be
    /// called before any `next()`; on success the producer keeps nothing to
    /// emit and its deinit skips the transferred buffers.
    takeOwnedChunks: *const fn (ptr: *anyopaque) anyerror!?OwnedChunks,
    /// Plan-time string widths for the GROUP BY router (issues #397, #464):
    /// fill each null field of `widths[i]` for a string output column `i`
    /// that traces to a table column, from that column's row groups
    /// (`Table.sampledStringWidths`). Only a table scan samples; pass-through
    /// layers map their columns onto their upstream's, and every other
    /// operator leaves `widths` as it is. Reads data, so it runs only when
    /// the router prices a plan by its input's bytes.
    sampleWidths: *const fn (ptr: *anyopaque, widths: []SampledWidth) anyerror!void,
    /// Plan-time key sample for the GROUP BY router (issue #478): add to
    /// `sample` the values of output columns `cols` (one per `sample.keys`,
    /// in order) over a sample of the rows this pipeline reads. True when
    /// it sampled. False (the default) when it can't: a computed key, or an
    /// operator that emits other rows than it reads (aggregate, join,
    /// window); the caller then drops the sample. A table scan samples
    /// runs of the row groups its hints keep, a materialized buffer runs of
    /// its rows, within `keySampleBudget`; pass-through layers map the columns onto their upstream's, and a
    /// filter's sample is of the rows it reads, whose tuples include its
    /// survivors'. A union folds its arms' samples (`KeySample.mergeArm`).
    sampleKeys: *const fn (ptr: *anyopaque, cols: []const usize, sample: *KeySample) anyerror!bool,
};

/// Write `depth` levels of indentation then a complete label line.
pub fn explainLine(out: *std.ArrayList(u8), allocator: Allocator, depth: usize, text: []const u8) !void {
    try explainIndent(out, allocator, depth);
    try out.appendSlice(allocator, text);
    try out.append(allocator, '\n');
}

/// Write `depth` levels of indentation (no newline). For operators that
/// build a dynamic label line (column lists, names) directly into `out`.
pub fn explainIndent(out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
    var i: usize = 0;
    while (i < depth) : (i += 1) try out.appendSlice(allocator, "  ");
}

/// Sort property of an operator's output stream.
pub const SortState = struct {
    /// Columns this stream is sorted by, in lexicographic order. Empty
    /// slice = not sorted on any known prefix. A join planner can
    /// check whether its join key matches a leading prefix of these.
    keys: []const []const u8 = &.{},
    /// Per-key sort direction. Either empty (⟺ every key ascending — the
    /// common case) or the same length as `keys`, where `descs[i] == true`
    /// means `keys[i]` is sorted descending. Grouping is direction-
    /// agnostic (equal values are adjacent in either order), but a
    /// sort-merge join's ascending merge must reject a descending prefix —
    /// see `joinKeysCovered`.
    descs: []const bool = &.{},
    /// `true` = sorted across the whole stream (globally). `false` =
    /// sorted only within each emitted batch (e.g., scan of an
    /// uncompacted table where each row group is sorted but segments
    /// can overlap). Joins exploit `global=true` for the SMJ-merge-only
    /// fast path.
    global: bool = false,

    /// Whether key `i` is ascending (the empty-`descs` convention means
    /// all ascending).
    pub fn ascendingAt(self: SortState, i: usize) bool {
        return self.descs.len == 0 or !self.descs[i];
    }
};

/// Per-column distinct-value cardinality as it flows through the pipeline.
/// `exact: n` means a proven upper bound of `n` distinct values (filters
/// only shrink distinct counts, so an upstream bound stays valid). `unknown`
/// means no proven bound (also used for the on-disk "big" marker). The
/// GROUP BY planner multiplies the group keys' bounds: all `exact` and the
/// product under the limit ⇒ hash fits; any `unknown` ⇒ sort.
pub const ColCard = union(enum) {
    exact: u32,
    unknown,
};

/// Per-column propagated statistic: a distinct-value bound plus an optional
/// proven min/max range. `min`/`max` are i128 in the value's own domain
/// (the same encoding `statsOverlapPredicate` compares against) and are only
/// populated for fixed-width int-family columns (integers, temporal,
/// boolean, decimal); they stay `null` for float, string, and uuid columns
/// whose manifest stats aren't a usable numeric range. `ndv`, `min` and `max`
/// are PROVABLE UPPER/inclusive bounds — operators only ever tighten them,
/// never estimate beyond what the data guarantees.
pub const ColStat = struct {
    ndv: ColCard = .unknown,
    min: ?i128 = null,
    max: ?i128 = null,
    /// Mean payload bytes per row of a string column, measured on a realized
    /// buffer (a stage's result, a drained GROUP BY input). A measurement,
    /// not a bound: operators carry it through unchanged, so a filter's
    /// survivors may average differently. Null = not measured.
    avg_width: ?u32 = null,
    /// Mean payload bytes per DISTINCT value of a string column, sampled
    /// like `avg_width` (`storage.column.DistinctWidthSampler`). A GROUP BY
    /// keeps one key copy per group, so it prices key bytes by this: a value
    /// on most rows (an empty string) pulls `avg_width` far below it. Null =
    /// not measured.
    distinct_width: ?u32 = null,
    /// The table column this column's values are drawn from. Null once an
    /// operator computes new values. See `ColumnOrigin`.
    origin: ?ColumnOrigin = null,
};

/// A column of one table snapshot. A column keeps it only while its values
/// stay a subset of that column's: filters, projections, renames, joins and
/// group keys copy their input's stat, and anything that computes new values
/// builds a fresh one. Two union arms whose same-position columns name the
/// same origin together hold at most `ndv` distinct values, where summing
/// the arms' NDVs would count a shared value once per arm (issue #478).
pub const ColumnOrigin = struct {
    /// `snapshotId` of the table snapshot the column was scanned from.
    snapshot: u64,
    /// The column's index in that table's schema.
    column: u32,
    /// The column's distinct values over the whole snapshot.
    ndv: ColCard,
};

/// Every output row is one row of a table snapshot, and every column whose
/// `ColStat.origin` names that snapshot holds the row's own value. A key
/// tuple of such columns is then a function of one row, so it takes at most
/// `rows` distinct values however many rows are emitted, as in a UNION ALL
/// of two arms over one table. Operators that build a row from several input
/// rows (joins, aggregates, windows) don't pass it on.
pub const RowOrigin = struct {
    snapshot: u64,
    /// The snapshot's rows.
    rows: u64,
};

/// Identity of one table's snapshot: the same table at the same segment set
/// and memtable size. Two scans that agree on it read the same rows.
pub fn snapshotId(table_uid: u64, segments: []const storage.ManifestEntry, memtable_rows: u64) u64 {
    var h = std.hash.Wyhash.init(table_uid);
    h.update(std.mem.asBytes(&memtable_rows));
    for (segments) |e| {
        h.update(std.mem.asBytes(&e.segment_id));
        h.update(std.mem.asBytes(&e.row_count));
    }
    return h.final();
}

/// Whether `stat` is the row's own value of a column of the snapshot
/// `st.row_origin` names.
pub fn fromRowOrigin(st: PipelineStats, stat: ColStat) bool {
    const ro = st.row_origin orelse return false;
    const o = stat.origin orelse return false;
    return o.snapshot == ro.snapshot;
}

/// The most distinct key tuples `st`'s rows hold: its row bound, or its row
/// origin's rows when every key column is `fromRowOrigin`.
pub fn keyTupleBound(st: PipelineStats, keys_from_row_origin: bool) u64 {
    if (keys_from_row_origin) if (st.row_origin) |ro| return @min(st.upper_rows, ro.rows);
    return st.upper_rows;
}

/// The widths `Query.sampleWidths` measures for one string column: its
/// `ColStat.avg_width` and `ColStat.distinct_width`.
pub const SampledWidth = struct {
    row: ?u32 = null,
    distinct: ?u32 = null,
    /// Whether the caller prices this column's distinct width (a GROUP BY
    /// key). A producer samples `distinct` only when it is wanted: it reads
    /// whole blocks, where `row` reads block headers.
    distinct_wanted: bool = false,

    pub fn complete(self: SampledWidth) bool {
        return self.row != null and (self.distinct != null or !self.distinct_wanted);
    }

    pub fn wantsDistinct(self: SampledWidth) bool {
        return self.distinct_wanted and self.distinct == null;
    }

    /// `self` with each null width taken from `other`, wanting what either
    /// wants.
    pub fn orElse(self: SampledWidth, other: SampledWidth) SampledWidth {
        return .{
            .row = self.row orelse other.row,
            .distinct = self.distinct orelse other.distinct,
            .distinct_wanted = self.distinct_wanted or other.distinct_wanted,
        };
    }
};

/// `ColStat.avg_width` for `payload` string bytes spread over `rows` rows,
/// rounded up. Null when there are no rows to measure.
pub fn avgWidth(payload: u64, rows: u64) ?u32 {
    if (rows == 0) return null;
    const mean = payload / rows + @intFromBool(payload % rows != 0);
    return @intCast(@min(mean, std.math.maxInt(u32)));
}

/// What `Query.sampleKeys` gathers for the GROUP BY router's group estimate
/// over correlated keys (issue #478): the rows it read, and sketches of
/// their key tuples and of each key.
pub const KeySample = struct {
    rows: u64 = 0,
    /// Whether every row the source reads was sampled, so `tuple` counts
    /// the input's own tuples.
    complete: bool = true,
    tuple: KeySketch = .{},
    /// One sketch per key, in the order of the caller's key columns.
    keys: []KeySketch,
    /// The sampled source, for the router's trace.
    source: []const u8 = "",

    pub fn init(allocator: Allocator, key_count: usize) !KeySample {
        const keys = try allocator.alloc(KeySketch, key_count);
        @memset(keys, .{});
        return .{ .keys = keys };
    }

    pub fn deinit(self: *KeySample, allocator: Allocator) void {
        allocator.free(self.keys);
        self.* = undefined;
    }

    /// Add rows `[lo, hi)` of `views`, one view per key in key order.
    pub fn addRows(self: *KeySample, views: []const ColumnView, lo: usize, hi: usize) void {
        std.debug.assert(views.len == self.keys.len);
        var tuples: [KEY_SAMPLE_BLOCK_ROWS]u64 = undefined;
        var start = lo;
        while (start < hi) {
            const n = @min(hi - start, KEY_SAMPLE_BLOCK_ROWS);
            @memset(tuples[0..n], KEY_TUPLE_SEED);
            for (views, self.keys) |view, *sketch| hashKeyBlock(view, start, tuples[0..n], sketch);
            for (tuples[0..n]) |t| self.tuple.add(t);
            start += n;
        }
        self.rows += hi - lo;
    }

    /// Fold in the sample of another union arm. The rows are the larger
    /// arm's, not the sum: arms over the same table sample the same rows,
    /// and counting them twice would pass the repeats off as the
    /// multiplicity the router's guard asks for.
    pub fn mergeArm(self: *KeySample, other: *const KeySample) void {
        std.debug.assert(other.keys.len == self.keys.len);
        self.rows = @max(self.rows, other.rows);
        self.complete = self.complete and other.complete;
        self.tuple.merge(&other.tuple);
        for (self.keys, other.keys) |*k, *o| k.merge(o);
    }
};

/// Row groups or buffer windows a key sample reads at most.
pub const KEY_SAMPLE_PARTS: usize = 16;

/// The part pick `pick` of `picks` reads out of `parts`: the middle of the
/// pick's equal share, so the picks spread evenly over the parts.
pub fn keySamplePart(parts: usize, pick: usize, picks: usize) usize {
    return (2 * pick + 1) * parts / (2 * picks);
}

/// An input a key sample reads whole: one default row group's worth.
pub const KEY_SAMPLE_MIN_ROWS: usize = 64 * 1024;

/// Rows a key sample reads at most: `KEY_SAMPLE_PARTS` default row groups.
pub const KEY_SAMPLE_MAX_ROWS: usize = KEY_SAMPLE_PARTS * KEY_SAMPLE_MIN_ROWS;

/// Rows a key sample reads of an input of `rows`: all of them up to
/// `KEY_SAMPLE_MIN_ROWS`, else a sixteenth of them, kept between that and
/// `KEY_SAMPLE_MAX_ROWS`. Hashing a key tuple costs a few nanoseconds, a
/// fair share of what aggregating its row costs, and the sample runs on one
/// thread before a GROUP BY that may run on many: a sample of the whole of
/// a mid-sized input would add much of the GROUP BY's own time, where a
/// sixteenth adds a sliver.
pub fn keySampleBudget(rows: usize) usize {
    if (rows <= KEY_SAMPLE_MIN_ROWS) return rows;
    return std.math.clamp(rows / 16, KEY_SAMPLE_MIN_ROWS, KEY_SAMPLE_MAX_ROWS);
}

pub const RowSpan = struct { lo: usize, hi: usize };

/// The `window` rows around the middle of `[lo, hi)`, or all of them when
/// they fit.
pub fn keySampleWindow(lo: usize, hi: usize, window: usize) RowSpan {
    if (hi - lo <= window) return .{ .lo = lo, .hi = hi };
    const start = lo + (hi - lo - window) / 2;
    return .{ .lo = start, .hi = start + window };
}

/// Feed `sample` from a buffer held in chunks: all of its rows when
/// `keySampleBudget` takes them all, else a window of a sixteenth of the
/// budget around the middle of each of `KEY_SAMPLE_PARTS` equal shares of
/// its rows, across whatever chunks that window spans. A buffer's chunks
/// are whatever its producer emitted, from a few rows to all of them, so
/// windows over its rows bound the sample's cost where whole chunks would
/// not. `buffer` gives `len()` chunks, chunk `i`'s `rows(i)`, and
/// `views(i, out)`, its key columns.
pub fn sampleBuffer(sample: *KeySample, buffer: anytype, views: []ColumnView) void {
    var total: usize = 0;
    for (0..buffer.len()) |i| total += buffer.rows(i);
    const budget = keySampleBudget(total);
    const parts: usize = if (budget == total) 1 else KEY_SAMPLE_PARTS;
    if (parts > 1) sample.complete = false;
    const share = struct {
        fn window(rows: usize, n: usize, part: usize, window_rows: usize) RowSpan {
            if (n == 1) return .{ .lo = 0, .hi = rows };
            return keySampleWindow(part * rows / n, (part + 1) * rows / n, window_rows);
        }
    };
    const window_rows = budget / parts;
    var pick: usize = 0;
    var win = share.window(total, parts, 0, window_rows);
    var end: usize = 0;
    for (0..buffer.len()) |i| {
        const rows = buffer.rows(i);
        if (rows == 0) continue;
        const start = end;
        end += rows;
        if (pick == parts or win.lo >= end) continue;
        buffer.views(i, views);
        while (true) {
            sample.addRows(views, @max(win.lo, start) - start, @min(win.hi, end) - start);
            if (win.hi > end) break;
            pick += 1;
            if (pick == parts) break;
            win = share.window(total, parts, pick, window_rows);
            if (win.lo >= end) break;
        }
    }
}

const hll = @import("../util/hll.zig");
/// A key sample's sketches. The router divides one count by another and
/// scales the quotient up, so it takes four times the registers of a
/// column's stored sketch for half the error (about 1.6%); a sample keeps
/// only a few of them, briefly.
const KeySketch = hll.Sketch(12);
const KEY_SAMPLE_BLOCK_ROWS = 1024;
const KEY_TUPLE_SEED: u64 = 0x2545F4914F6CDD1D;
const NULL_KEY_HASH: u64 = 0x9E3779B97F4A7C15;

/// The murmur3 finalizer: a bijection whose output bits each depend on
/// every input bit, which HyperLogLog needs from its hashes. The offset
/// keeps a zero key, common in data, off the zero hash HLL ranks highest.
fn mixKey(x: u64) u64 {
    var h = x +% NULL_KEY_HASH;
    h = (h ^ (h >> 33)) *% 0xFF51AFD7ED558CCD;
    h = (h ^ (h >> 33)) *% 0xC4CEB9FE1A85EC53;
    return h ^ (h >> 33);
}

/// Hash rows `[start, start + tuples.len)` of `view` into `sketch`, and
/// fold each into its row's tuple hash. Integers hash by value whatever
/// their width, floats by their canonical bits, so union arms of other
/// widths agree.
fn hashKeyBlock(view: ColumnView, start: usize, tuples: []u64, sketch: *KeySketch) void {
    switch (view.data) {
        .varchar, .string, .char, .json => |sv| for (tuples, start..) |*t, r| {
            const h = if (view.isValid(r)) std.hash.Wyhash.hash(0, sv.rowBytes(r)) else NULL_KEY_HASH;
            sketch.add(h);
            t.* = mixKey(t.* ^ h);
        },
        inline .float, .double => |s| for (tuples, start..) |*t, r| {
            const h = if (view.isValid(r)) mixKey(types.canonicalFloatBits(s[r])) else NULL_KEY_HASH;
            sketch.add(h);
            t.* = mixKey(t.* ^ h);
        },
        inline .largeint, .decimal128, .uuid => |s| for (tuples, start..) |*t, r| {
            const h = if (view.isValid(r)) std.hash.Wyhash.hash(0, std.mem.asBytes(&s[r])) else NULL_KEY_HASH;
            sketch.add(h);
            t.* = mixKey(t.* ^ h);
        },
        inline else => |s| for (tuples, start..) |*t, r| {
            const h = if (view.isValid(r)) mixKey(@bitCast(@as(i64, s[r]))) else NULL_KEY_HASH;
            sketch.add(h);
            t.* = mixKey(t.* ^ h);
        },
    }
}

/// String payload bytes a view holds (0 for fixed-width data).
pub fn stringPayloadBytes(view: storage.ColumnView) u64 {
    return switch (view.data) {
        .varchar, .string, .char, .json => |sv| sv.offsets[sv.offsets.len - 1] - sv.offsets[0],
        else => 0,
    };
}

/// Cap a column statistic's distinct-value bound at `upper_rows`: a column
/// can never hold more distinct values than there are rows. Leaves min/max
/// untouched. An `.unknown` ndv stays `.unknown` — it signals "no usable
/// finite bound" to the GROUP BY router, and turning it into a concrete
/// `upper_rows` figure would change routing. Only an existing `.exact`
/// bound is tightened. Applied at the end of every operator's transform.
pub fn capColStat(stat: ColStat, upper_rows: u64) ColStat {
    var out = stat;
    const cap: u32 = if (upper_rows > std.math.maxInt(u32)) std.math.maxInt(u32) else @intCast(upper_rows);
    switch (out.ndv) {
        .exact => |n| out.ndv = .{ .exact = @min(n, cap) },
        .unknown => {},
    }
    return out;
}

/// Cap every column statistic in `stats` at `upper_rows` in place.
pub fn capColStats(stats: []ColStat, upper_rows: u64) void {
    for (stats) |*s| s.* = capColStat(s.*, upper_rows);
}

/// Pre-execution statistics about an operator's output.
pub const PipelineStats = struct {
    /// Upper bound on the number of rows this operator will emit.
    /// Never null — for operators with selectivity (Filter), this is
    /// the conservative upper bound (input row count). Refined to
    /// `exact_rows` only after the operator's input has been drained.
    upper_rows: u64,
    /// Sort property of the output stream. See `SortState`.
    sort_state: SortState = .{},
    /// Per-output-column propagated statistic (distinct-value bound + min/max
    /// range), indexed by output schema column. Empty ⇒ no information (all
    /// columns unknown).
    column_stats: []const ColStat = &.{},
    /// See `RowOrigin`. Null unless every operator below passes it on.
    row_origin: ?RowOrigin = null,
};

pub const Query = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    allocator: Allocator,
    resources: ?*memory.MemoryAccountant = null,
    /// Cached at operator construction. False means no probe-fusion source is
    /// reachable through this pipeline, so speculative sink offers can skip a
    /// recursive walk that is otherwise quadratic in deep operator stacks.
    probe_fusion_reachable: bool,

    pub fn next(self: *Query) !?Batch {
        if (self.resources) |a| try a.checkCancelled();
        return self.vtable.next(self.ptr) catch |err| {
            if (err == error.OutOfMemory) if (self.resources) |a| {
                if (a.exceeded.load(.acquire)) return error.MemoryBudgetExceeded;
            };
            return err;
        };
    }

    pub fn deinit(self: *Query) void {
        self.vtable.deinit(self.ptr);
        self.* = undefined;
    }

    pub fn outputSchema(self: Query) []const Column {
        return self.vtable.outputSchema(self.ptr);
    }

    pub fn addPrune(self: *Query, pred: predicate.Predicate) !void {
        return self.vtable.addPrune(self.ptr, pred);
    }

    /// Offer a join-probe sink for in-worker probing (see `ProbeSink`).
    pub fn tryFuseProbe(self: Query, sink: ProbeSink) !bool {
        return self.vtable.tryFuseProbe(self.ptr, sink);
    }

    pub fn probeFusionReachable(self: Query) bool {
        return self.probe_fusion_reachable;
    }

    /// Rebind a new sink at the bottom of an already probe-fused pipeline.
    pub fn rechainProbeSink(self: Query, sink: ProbeSink) !bool {
        return self.vtable.rechainProbeSink(self.ptr, sink);
    }

    /// Offer `expr` to this operator for in-place filtering. Returns true if
    /// the operator took ownership of applying the predicate (the caller then
    /// becomes a pass-through). See `VTable.tryFuseFilter`.
    pub fn tryFuseFilter(self: *Query, expr: predicate.PredicateExpr) !bool {
        return self.vtable.tryFuseFilter(self.ptr, expr);
    }

    /// Offer a projection Compute to this operator to absorb. Returns true if it
    /// took ownership (caller becomes a pass-through). See `VTable.tryFuseCompute`.
    pub fn tryFuseCompute(self: *Query, derived: []const @import("compute.zig").Derived) !bool {
        return self.tryFuseComputeWithRegistry(derived, null);
    }

    pub fn tryFuseComputeWithRegistry(self: *Query, derived: []const @import("compute.zig").Derived, registry: ?*const @import("../udf.zig").UdfRegistry) !bool {
        return self.vtable.tryFuseCompute(self.ptr, derived, registry);
    }

    /// Offer a partial aggregate to run inside this operator's parallel workers
    /// (two-phase GROUP BY). Returns true iff accepted. See `VTable.tryFuseAggregate`.
    pub fn tryFuseAggregate(self: *Query, group_cols: []const []const u8, aggs: []const AggSpec) !bool {
        return self.vtable.tryFuseAggregate(self.ptr, group_cols, aggs);
    }

    /// Offer a full lease GROUP BY replacement to this operator. Returns a new
    /// query only when accepted; otherwise the caller should use its fallback.
    pub fn tryLeaseGroupBy(self: Query, group_cols: []const []const u8, aggs: []const AggSpec, top_k: ?@import("../ir/ir.zig").Op.TopK, emit_limit: ?u32, dop: usize) !?Query {
        return self.vtable.tryLeaseGroupBy(self.ptr, group_cols, aggs, top_k, emit_limit, dop);
    }

    /// Pre-execution stats on this operator's output. Cheap; no data
    /// scanned. See `PipelineStats`.
    pub fn stats(self: Query) PipelineStats {
        return self.vtable.stats(self.ptr);
    }

    /// Per-query memory accountant. Set up by the bottom-most Scan
    /// when Table.query_memory_budget > 0. Combinators upstream
    /// inherit by calling this method on their input.
    fn operatorAllocator(self: Query) !Allocator {
        return memory.executionAllocator(self.allocator, self.accountant());
    }

    pub fn accountant(self: Query) ?*memory.MemoryAccountant {
        return self.resources orelse self.vtable.accountant(self.ptr);
    }

    pub fn explain(self: Query, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        return self.vtable.explain(self.ptr, out, allocator, depth);
    }

    /// Render the whole compiled operator tree as an indented physical plan.
    /// Caller owns the returned bytes.
    pub fn explainPlan(self: Query, allocator: Allocator) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(allocator);
        try self.explain(&out, allocator, 0);
        return out.toOwnedSlice(allocator);
    }

    /// Phase 4.2: ask the underlying Scan to emit `name` as dict codes (sidecar).
    /// Returns true iff an eligible Scan accepted; false for any other operator.
    pub fn setDictCodeColumn(self: Query, name: []const u8, dict: *global_dict.GlobalDict) bool {
        return self.vtable.setDictCodeColumn(self.ptr, name, dict);
    }

    /// Tell the underlying parallel scan that the consumer above only reads
    /// `keep` — so it can drop dead columns (decoded for a fused
    /// filter/compute, never read above). See `VTable.setEmitProjection`.
    pub fn setEmitProjection(self: Query, keep: []const []const u8) !void {
        return self.vtable.setEmitProjection(self.ptr, keep);
    }

    /// Phase 4.2 multi-key: can the underlying Scan emit `name` as codes?
    pub fn canCodeColumn(self: Query, name: []const u8) bool {
        return self.vtable.canCodeColumn(self.ptr, name);
    }

    /// Phase 4.2 multi-key: roll back the underlying Scan's coded-column setup.
    pub fn clearDictCodeColumns(self: Query) void {
        return self.vtable.clearDictCodeColumns(self.ptr);
    }

    /// True when emitted batches' DATA stays valid until deinit (view structs
    /// may be per-`next()` scratch). See `VTable.stableData`.
    pub fn stableData(self: Query) bool {
        return self.vtable.stableData(self.ptr);
    }

    /// Take ownership of the pipeline's materialized per-chunk buffers (stage
    /// adoption). Null = not offerable; stream via `next()` instead. See
    /// `VTable.takeOwnedChunks`.
    pub fn takeOwnedChunks(self: Query) !?OwnedChunks {
        return self.vtable.takeOwnedChunks(self.ptr);
    }

    /// Sample the string output columns whose `widths` entry has a null field. See
    /// `VTable.sampleWidths`.
    pub fn sampleWidths(self: Query, widths: []SampledWidth) !void {
        return self.vtable.sampleWidths(self.ptr, widths);
    }

    /// Sample key columns `cols` into `sample`. See `VTable.sampleKeys`.
    pub fn sampleKeys(self: Query, cols: []const usize, sample: *KeySample) !bool {
        return self.vtable.sampleKeys(self.ptr, cols, sample);
    }

    // ----- Combinators -----

    pub fn filter(self: Query, expr: predicate.PredicateExpr) !Query {
        return @import("filter.zig").Filter.create(try self.operatorAllocator(), self, expr);
    }

    /// `filter` whose kept rows the caller bounds further. See `Filter.Bound`.
    pub fn filterBounded(self: Query, expr: predicate.PredicateExpr, bound: @import("filter.zig").Filter.Bound) !Query {
        return @import("filter.zig").Filter.createBounded(try self.operatorAllocator(), self, expr, bound);
    }

    pub fn project(self: Query, columns: []const []const u8) !Query {
        return @import("project_limit.zig").Project.create(try self.operatorAllocator(), self, columns);
    }

    pub fn projectNamed(self: Query, columns: []const []const u8, output_names: []const []const u8) !Query {
        return @import("project_limit.zig").Project.createNamed(try self.operatorAllocator(), self, columns, output_names);
    }

    pub fn limit(self: Query, n: usize) !Query {
        return @import("project_limit.zig").Limit.create(try self.operatorAllocator(), self, n);
    }

    pub fn limitOffset(self: Query, n: usize, offset: usize) !Query {
        return @import("project_limit.zig").Limit.createOffset(try self.operatorAllocator(), self, n, offset);
    }

    /// Aggregate over the entire upstream (no grouping).
    pub fn aggregate(self: Query, aggs: []const AggSpec) !Query {
        return @import("aggregate.zig").Aggregate.create(try self.operatorAllocator(), self, &.{}, aggs, null, null);
    }

    /// Hash-grouped aggregation. `group_cols` lists the upstream columns to
    /// group by; one output row is emitted per distinct group.
    pub fn groupBy(self: Query, group_cols: []const []const u8, aggs: []const AggSpec) !Query {
        return @import("aggregate.zig").Aggregate.create(try self.operatorAllocator(), self, group_cols, aggs, null, null);
    }

    /// Hash GROUP BY with an optional top-k hint (set when this aggregate is
    /// directly under `ORDER BY <keys> LIMIT k`). When every order key resolves
    /// to a numeric aggregate output, the aggregate emits only the top-k groups.
    /// `emit_limit` (set for an unordered `GROUP BY … LIMIT n`) caps the emit at
    /// the first n groups in group-insertion order; it is mutually exclusive
    /// with `top_k` (the ORDER BY path).
    pub fn groupByTopK(self: Query, group_cols: []const []const u8, aggs: []const AggSpec, top_k: ?@import("../ir/ir.zig").Op.TopK, emit_limit: ?u32) !Query {
        const agg = @import("aggregate.zig");
        const t = top_k orelse
            return agg.Aggregate.create(try self.operatorAllocator(), self, group_cols, aggs, null, emit_limit);
        // The hint's keys are resolved (to agg indices) synchronously inside
        // create, so this temporary translation array need only outlive the call.
        const keys = try self.allocator.alloc(agg.TopKKey, t.keys.len);
        defer self.allocator.free(keys);
        for (t.keys, keys) |src, *dst| dst.* = .{ .col = src.col, .desc = src.desc };
        return agg.Aggregate.create(try self.operatorAllocator(), self, group_cols, aggs, agg.TopKHint{ .k = t.k, .keys = keys }, emit_limit);
    }

    /// Radix-partitioned hash aggregation over a compact fixed-state core.
    /// Standard high-cardinality path: int key ≤128 bits (native or dict-coded),
    /// fixed-state aggregates only. A `top_k` hint (ORDER BY <agg> LIMIT k) emits
    /// only the k most-preferred groups; the downstream OrderBy+Limit still
    /// finalizes exact order. The router gates eligibility before calling.
    /// `expected_groups` is its estimate of the groups, 0 when it has none.
    pub fn radixGroupBy(self: Query, group_cols: []const []const u8, aggs: []const AggSpec, top_k: ?@import("radix_aggregate.zig").TopK, expected_groups: u64) !Query {
        return @import("radix_aggregate.zig").RadixAggregate.create(try self.operatorAllocator(), self, group_cols, aggs, top_k, expected_groups);
    }

    /// Parallel partition+lease grouped aggregation (high-card path). Same
    /// eligibility as radixGroupBy (int key ≤128 bits, fixed-state aggs) but
    /// partitions rows into buckets and aggregates them across `dop` threads.
    pub fn leaseGroupBy(self: Query, group_cols: []const []const u8, aggs: []const AggSpec, top_k: ?@import("radix_aggregate.zig").TopK, dop: usize) !Query {
        return @import("radix_aggregate.zig").RadixLeaseAggregate.create(try self.operatorAllocator(), self, group_cols, aggs, top_k, dop);
    }

    /// Streaming sort-based grouped aggregation. Requires the input to be
    /// sorted such that equal group keys are adjacent. Holds only one
    /// group's state at a time (O(1) in cardinality). Caller (planner)
    /// must verify the sortedness precondition via `stats().sort_state`.
    pub fn streamGroupBy(self: Query, group_cols: []const []const u8, aggs: []const AggSpec) !Query {
        return @import("aggregate.zig").SortedAggregate.create(try self.operatorAllocator(), self, group_cols, aggs);
    }

    pub fn udfGroupBy(
        self: Query,
        group_cols: []const []const u8,
        aggs: []const AggSpec,
        udf_registry: *const @import("../udf.zig").UdfRegistry,
    ) !Query {
        return @import("udf_aggregate.zig").UdfAggregate.create(try self.operatorAllocator(), self, group_cols, aggs, udf_registry);
    }

    /// Sort upstream rows by `sort_specs` (multi-column, ASC/DESC per key).
    /// Blocking — materializes all upstream rows before emitting any output.
    pub fn orderBy(self: Query, sort_specs: []const SortSpec) !Query {
        return @import("sort.zig").Sort.create(try self.operatorAllocator(), self, sort_specs) catch |err| return memory.allocationError(self.accountant(), err);
    }

    /// Bounded `ORDER BY ... LIMIT limit OFFSET offset` — keeps only the
    /// `limit + offset` rows it might emit instead of materializing the
    /// whole input. The planner fuses `Limit(OrderBy(X))` into this.
    pub fn topN(self: Query, sort_specs: []const SortSpec, n: usize, offset: usize) !Query {
        return @import("topn.zig").TopN.create(try self.operatorAllocator(), self, sort_specs, n, offset);
    }

    /// Add derived columns via scalar function calls. Each `Derived`
    /// names the new column and supplies an `Expr` that resolves to a
    /// function on upstream columns (v1: no nesting). Output schema
    /// extends the upstream schema with these new columns appended.
    pub fn compute(self: Query, derived: []const @import("compute.zig").Derived) !Query {
        return @import("compute.zig").Compute.create(try self.operatorAllocator(), self, derived);
    }

    pub fn computeWithRegistry(
        self: Query,
        derived: []const @import("compute.zig").Derived,
        udf_registry: ?*const @import("../udf.zig").UdfRegistry,
    ) !Query {
        return @import("compute.zig").Compute.createWithRegistry(try self.operatorAllocator(), self, derived, udf_registry);
    }

    /// Window function step. `specs` is the list of unique window
    /// specifications referenced by `calls`; `calls` carry a `spec_idx`
    /// into `specs`. Operator sorts the input once per spec and
    /// evaluates all calls sharing that spec in a single sweep.
    /// `dop` > 1 lets a partitioned spec over a large input sort and
    /// evaluate its partition buckets on that many worker threads.
    pub fn window(
        self: Query,
        specs: []const @import("../ir/ir.zig").WindowSpec,
        calls: []const @import("../ir/ir.zig").WindowCall,
        dop: usize,
    ) !Query {
        return @import("window.zig").Window.create(try self.operatorAllocator(), self, specs, calls, dop);
    }

    /// Inner equi-join with `other`. Output schema is this side's
    /// columns followed by `other`'s columns; column names must not
    /// collide (rename one side via `.compute()` if needed). Algorithm
    /// is hash join in v1 — build side is whichever has the smaller
    /// upper-bound row count.
    pub fn join(self: Query, other: Query, spec: @import("join.zig").Spec) !Query {
        return @import("join.zig").Join.create(try self.operatorAllocator(), self, other, spec);
    }

    /// `f` is either a function taking `Query` and returning `!Query`, or a
    /// function returning `Query` (we accept both by being generic).
    pub fn pipe(self: Query, f: anytype) !Query {
        return f(self);
    }
};

/// Lift an operator pointer into a Query. The operator type must define
/// `next()`, `deinit()`, `outputSchema()`, and `addPrune()` methods.
pub fn makeQuery(allocator: Allocator, op: anytype) Query {
    const OpPtr = @TypeOf(op);
    const Op = comptime blk: {
        const info = @typeInfo(OpPtr);
        if (info != .pointer) @compileError("makeQuery: expected pointer to operator");
        break :blk info.pointer.child;
    };
    const probe_fusion_reachable = if (@hasDecl(Op, "probeFusionReachable")) op.probeFusionReachable() else false;
    return .{
        .ptr = op,
        .vtable = &OpWrapper(Op).vt,
        .allocator = allocator,
        .resources = memory.accountantOf(allocator) orelse (if (@hasDecl(Op, "accountant")) op.accountant() else null),
        .probe_fusion_reachable = probe_fusion_reachable,
    };
}

/// Comptime-typed downcast of a type-erased `Query`. The vtable is a
/// per-operator-type static (`OpWrapper(Op).vt`), so pointer identity is a
/// proof of the concrete type. Null when `q` wraps a different operator.
pub fn queryAs(comptime Op: type, q: Query) ?*Op {
    if (q.vtable != &OpWrapper(Op).vt) return null;
    return @ptrCast(@alignCast(q.ptr));
}

fn OpWrapper(comptime Op: type) type {
    return struct {
        fn nextWrap(ptr: *anyopaque) anyerror!?Batch {
            const o: *Op = @ptrCast(@alignCast(ptr));
            if (!prof.enabled) return o.next();
            const saved = prof.selfEnter();
            const t0 = prof.nowTicks();
            const r = o.next();
            const d = prof.nowTicks() - t0;
            const incl: u64 = if (d > 0) @intCast(d) else 0;
            prof.add(@typeName(Op), incl);
            const self_t = prof.selfLeave(@typeName(Op), incl, saved);
            const rows: u64 = if (r) |maybe| (if (maybe) |b| @intCast(b.row_count) else 0) else |_| 0;
            prof.instAdd(ptr, self_t, rows);
            return r;
        }
        fn deinitWrap(ptr: *anyopaque) void {
            const o: *Op = @ptrCast(@alignCast(ptr));
            if (!prof.enabled) return o.deinit();
            if (prof.instTake(ptr)) |st| {
                if (prof.ticksToMs(@intCast(st.ticks)) >= prof.INST_PRINT_MS) printInstance(o, st);
            }
            const saved = prof.deinitEnter();
            const t0 = prof.nowTicks();
            o.deinit();
            const d = prof.nowTicks() - t0;
            prof.deinitLeave(@typeName(Op), if (d > 0) @intCast(d) else 0, saved);
        }
        fn printInstance(o: *Op, st: prof.InstStat) void {
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(std.heap.page_allocator);
            o.explain(&buf, std.heap.page_allocator, 0) catch {};
            var head: [200]u8 = undefined;
            var n: usize = 0;
            var lines = std.mem.splitScalar(u8, buf.items, '\n');
            var taken: usize = 0;
            while (lines.next()) |raw| {
                const line = std.mem.trim(u8, raw, " ");
                if (line.len == 0) continue;
                if (taken == 3 or n + line.len + 3 > head.len) break;
                if (taken > 0) {
                    @memcpy(head[n .. n + 3], " | ");
                    n += 3;
                }
                @memcpy(head[n .. n + line.len], line);
                n += line.len;
                taken += 1;
            }
            std.debug.print("[inst] {s: <22} {d: >8.2} ms  ({d} calls, {d} rows, ~{d} thr)  {s}\n", .{
                prof.shortOpName(@typeName(Op)), prof.ticksToMs(@intCast(st.ticks)), st.calls, st.rows, st.threads(), head[0..n],
            });
        }
        fn outputSchemaWrap(ptr: *anyopaque) []const Column {
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.outputSchema();
        }
        fn addPruneWrap(ptr: *anyopaque, pred: predicate.Predicate) anyerror!void {
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.addPrune(pred);
        }
        fn tryFuseFilterWrap(ptr: *anyopaque, expr: predicate.PredicateExpr) anyerror!bool {
            if (!@hasDecl(Op, "tryFuseFilter")) return false;
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.tryFuseFilter(expr);
        }
        fn tryFuseComputeWrap(ptr: *anyopaque, derived: []const @import("compute.zig").Derived, registry: ?*const @import("../udf.zig").UdfRegistry) anyerror!bool {
            if (!@hasDecl(Op, "tryFuseCompute")) return false;
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.tryFuseCompute(derived, registry);
        }
        fn tryFuseAggregateWrap(ptr: *anyopaque, group_cols: []const []const u8, aggs: []const AggSpec) anyerror!bool {
            if (!@hasDecl(Op, "tryFuseAggregate")) return false;
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.tryFuseAggregate(group_cols, aggs);
        }
        fn tryFuseProbeWrap(ptr: *anyopaque, sink: ProbeSink) anyerror!bool {
            if (!@hasDecl(Op, "tryFuseProbe")) return false;
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.tryFuseProbe(sink);
        }
        fn rechainProbeSinkWrap(ptr: *anyopaque, sink: ProbeSink) anyerror!bool {
            if (!@hasDecl(Op, "rechainProbeSink")) return false;
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.rechainProbeSink(sink);
        }
        fn tryLeaseGroupByWrap(ptr: *anyopaque, group_cols: []const []const u8, aggs: []const AggSpec, top_k: ?@import("../ir/ir.zig").Op.TopK, emit_limit: ?u32, dop: usize) anyerror!?Query {
            if (!@hasDecl(Op, "tryLeaseGroupBy")) return null;
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.tryLeaseGroupBy(group_cols, aggs, top_k, emit_limit, dop);
        }
        fn statsWrap(ptr: *anyopaque) PipelineStats {
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.stats();
        }
        fn accountantWrap(ptr: *anyopaque) ?*memory.MemoryAccountant {
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.accountant();
        }
        fn explainWrap(ptr: *anyopaque, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) anyerror!void {
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.explain(out, alloc, depth);
        }
        fn setDictCodeColumnWrap(ptr: *anyopaque, name: []const u8, dict: *global_dict.GlobalDict) bool {
            if (!@hasDecl(Op, "setDictCodeColumn")) return false;
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.setDictCodeColumn(name, dict);
        }
        fn canCodeColumnWrap(ptr: *anyopaque, name: []const u8) bool {
            if (!@hasDecl(Op, "canCodeColumn")) return false;
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.canCodeColumn(name);
        }
        fn clearDictCodeColumnsWrap(ptr: *anyopaque) void {
            if (!@hasDecl(Op, "clearDictCodeColumns")) return;
            const o: *Op = @ptrCast(@alignCast(ptr));
            o.clearDictCodeColumns();
        }
        fn setEmitProjectionWrap(ptr: *anyopaque, keep: []const []const u8) anyerror!void {
            if (!@hasDecl(Op, "setEmitProjection")) return;
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.setEmitProjection(keep);
        }
        fn stableDataWrap(ptr: *anyopaque) bool {
            if (!@hasDecl(Op, "stableData")) return false;
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.stableData();
        }
        fn takeOwnedChunksWrap(ptr: *anyopaque) anyerror!?OwnedChunks {
            if (!@hasDecl(Op, "takeOwnedChunks")) return null;
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.takeOwnedChunks();
        }
        fn sampleWidthsWrap(ptr: *anyopaque, widths: []SampledWidth) anyerror!void {
            if (!@hasDecl(Op, "sampleWidths")) return;
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.sampleWidths(widths);
        }
        fn sampleKeysWrap(ptr: *anyopaque, cols: []const usize, sample: *KeySample) anyerror!bool {
            if (!@hasDecl(Op, "sampleKeys")) return false;
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.sampleKeys(cols, sample);
        }

        const vt: VTable = .{
            .next = nextWrap,
            .deinit = deinitWrap,
            .outputSchema = outputSchemaWrap,
            .addPrune = addPruneWrap,
            .tryFuseFilter = tryFuseFilterWrap,
            .tryFuseCompute = tryFuseComputeWrap,
            .tryFuseAggregate = tryFuseAggregateWrap,
            .tryFuseProbe = tryFuseProbeWrap,
            .rechainProbeSink = rechainProbeSinkWrap,
            .tryLeaseGroupBy = tryLeaseGroupByWrap,
            .stats = statsWrap,
            .accountant = accountantWrap,
            .explain = explainWrap,
            .setDictCodeColumn = setDictCodeColumnWrap,
            .canCodeColumn = canCodeColumnWrap,
            .clearDictCodeColumns = clearDictCodeColumnsWrap,
            .setEmitProjection = setEmitProjectionWrap,
            .stableData = stableDataWrap,
            .takeOwnedChunks = takeOwnedChunksWrap,
            .sampleWidths = sampleWidthsWrap,
            .sampleKeys = sampleKeysWrap,
        };
    };
}

/// Top-level entry point: build a scan query against a Table.
pub fn scan(allocator: Allocator, table: *Table) !Query {
    return @import("scan.zig").Scan.create(allocator, table);
}

pub const MinMaxStatsSpec = @import("agg_stats.zig").Spec;

/// Metadata-only MIN/MAX over a bare table: folds the manifest's per-segment
/// column stats instead of scanning. Returns null when the shortcut can't
/// apply (caller compiles the normal scan+aggregate). See `agg_stats.zig`.
pub fn minMaxStats(allocator: Allocator, table: *Table, specs: []const MinMaxStatsSpec) !?Query {
    const lease = if (table.statement_gate) |gate| try gate.acquire(false) else null;
    defer if (lease) |l| l.release();
    return @import("agg_stats.zig").MinMaxStats.create(allocator, table, specs);
}

pub const MetaAggSpec = @import("agg_stats.zig").MetaSpec;

/// Metadata-only lane for a bare global aggregate: any mix of COUNT(*) /
/// COUNT(non-nullable col) / MIN/MAX(exact-stats col). Counts work for any
/// table state (tombstones subtract, memtable adds); MIN/MAX declines (null)
/// on tombstones, unflushed rows, or inexact stats — the SHAPE gate (no
/// WHERE/GROUP BY/HAVING/derived) is the caller's job. See `agg_stats.zig`.
pub fn metaAggStats(allocator: Allocator, table: *Table, specs: []const MetaAggSpec) !?Query {
    const lease = if (table.statement_gate) |gate| try gate.acquire(false) else null;
    defer if (lease) |l| l.release();
    return @import("agg_stats.zig").MetaAggStats.create(allocator, table, specs);
}

/// Scan that uses a query-scoped accountant owned by the caller (the
/// SQL compile path's `CompileCtx`). Pass `null` to fall back to the
/// self-minting behaviour of `scan`.
pub fn scanWithAccountant(
    allocator: Allocator,
    table: *Table,
    accountant_ptr: ?*memory.MemoryAccountant,
) !Query {
    return @import("scan.zig").Scan.createWithAccountant(allocator, table, accountant_ptr);
}

/// Like `scanWithAccountant`, but `needed` (when non-null) restricts the
/// scan to those columns by name — projection pushdown. `null` reads all.
pub fn scanWithProjection(
    allocator: Allocator,
    table: *Table,
    accountant_ptr: ?*memory.MemoryAccountant,
    needed: ?[]const []const u8,
) !Query {
    return @import("scan.zig").Scan.createWithProjection(allocator, table, accountant_ptr, needed);
}

pub fn fileScan(
    allocator: Allocator,
    io: std.Io,
    access: api.FileScanAccess,
    spec: @import("../ir/ir.zig").FileScan,
    needed: ?[]const []const u8,
) !Query {
    return @import("file_scan.zig").FileScan.create(allocator, io, access, spec, needed);
}

/// Build a late-materialization plan for `SELECT <output_names> FROM table
/// WHERE <pred> [ORDER BY <order_specs>] LIMIT n OFFSET offset`.
///
/// Inner pipeline: `Scan(probe_names + __rowloc) → Filter(pred) →
/// TopN(order_specs, n, offset) | Limit(n, offset)`. The inner decodes only
/// the probe columns (filter ∪ ORDER BY) plus the location; the wrapping
/// `LateScan` fetches the wide `output_names` columns for the ≤ n survivors.
/// `order_specs == null` means no ORDER BY (a plain bounded limit).
pub fn lateScan(
    allocator: Allocator,
    table: *Table,
    accountant_ptr: ?*memory.MemoryAccountant,
    probe_names: []const []const u8,
    pred: predicate.PredicateExpr,
    order_specs: ?[]const SortSpec,
    output_names: []const []const u8,
    n: usize,
    offset: usize,
) !Query {
    const scan_ptr = try @import("scan.zig").Scan.allocWithProjectionLoc(
        allocator,
        table,
        accountant_ptr,
        probe_names,
        true,
        null,
    );
    var inner = makeQuery(allocator, scan_ptr);
    errdefer inner.deinit();

    inner = try inner.filter(pred);
    inner = if (order_specs) |specs|
        try inner.topN(specs, n, offset)
    else
        try inner.limitOffset(n, offset);

    return @import("latescan.zig").LateScan.create(allocator, inner, scan_ptr, table, output_names);
}

/// Build a zonemap block-skipping top-N plan for the same shape `lateScan`
/// handles (`SELECT <cols> FROM table WHERE <pred> ORDER BY <keys> LIMIT n
/// OFFSET m`). Returns null when the shape's leading ORDER BY key isn't a
/// non-nullable numeric/temporal column with usable footer stats — the caller
/// then falls back to `lateScan` (correct, just unoptimized). `order_specs`
/// must be non-empty. See `zonemap_topn.zig`.
pub fn zonemapTopN(
    allocator: Allocator,
    table: *Table,
    accountant_ptr: ?*memory.MemoryAccountant,
    probe_names: []const []const u8,
    pred: predicate.PredicateExpr,
    order_specs: []const SortSpec,
    output_names: []const []const u8,
    n: usize,
    offset: usize,
    dop: usize,
) !?Query {
    return @import("zonemap_topn.zig").ZonemapTopN.create(
        allocator,
        table,
        accountant_ptr,
        probe_names,
        pred,
        order_specs,
        output_names,
        n,
        offset,
        dop,
    );
}

/// Build a join's output `column_stats` by concatenating the left columns'
/// stats with the kept right columns' stats (the join output schema is
/// `left ⧺ right-where-kept`). A join can't grow a column's distinct count,
/// and an unchanged column keeps its min/max, so each side's stat stays a
/// valid upper bound. Returns `&.{}` when neither side carries stats. Shared
/// by all join operators. `right_kept_mask` is null when every right column
/// is kept (range joins that drop no equi key).
pub fn concatJoinStats(
    allocator: Allocator,
    left: Query,
    right: Query,
    left_col_count: usize,
    right_kept_mask: ?[]const bool,
    output_len: usize,
) ![]const ColStat {
    const ls = left.stats().column_stats;
    const rs = right.stats().column_stats;
    if (ls.len == 0 and rs.len == 0) return &.{};
    const cc = try allocator.alloc(ColStat, output_len);
    for (cc[0..left_col_count], 0..) |*out, i| out.* = if (i < ls.len) ls[i] else .{};
    var oi: usize = left_col_count;
    if (right_kept_mask) |mask| {
        for (mask, 0..) |keep, ri| {
            if (!keep) continue;
            cc[oi] = if (ri < rs.len) rs[ri] else .{};
            oi += 1;
        }
    } else {
        var ri: usize = 0;
        while (oi < output_len) : (oi += 1) {
            cc[oi] = if (ri < rs.len) rs[ri] else .{};
            ri += 1;
        }
    }
    return cc;
}

/// Merge two same-position column stats across a UNION ALL (vertical row
/// concatenation) of arms bounded at `l_rows` and `r_rows` rows. An empty arm
/// adds nothing, so the other arm's stat stands. NDV: the union holds at most
/// `l + r` distinct values, and at most the origin column's when both arms
/// draw from the same column of the same snapshot, where the sum would count
/// a value both arms hold twice (issue #478). An unknown side stays unknown
/// unless that origin bounds it. Range: the union spans both, so min/max
/// widen to the outer bounds — but only when BOTH sides bound that end (a
/// null on either side means that end is unbounded). Width: the union's mean
/// lies between the sides', so the wider one covers it.
pub fn mergeUnionColStat(l: ColStat, l_rows: u64, r: ColStat, r_rows: u64) ColStat {
    if (r_rows == 0) return l;
    if (l_rows == 0) return r;
    const origin = sharedOrigin(l.origin, r.origin);
    const summed: ColCard = switch (l.ndv) {
        .unknown => .unknown,
        .exact => |ln| switch (r.ndv) {
            .unknown => .unknown,
            .exact => |rn| .{ .exact = ln +| rn },
        },
    };
    const ndv = if (origin) |o| minCard(summed, o.ndv) else summed;
    const min: ?i128 = if (l.min) |lm| (if (r.min) |rm| @min(lm, rm) else null) else null;
    const max: ?i128 = if (l.max) |lm| (if (r.max) |rm| @max(lm, rm) else null) else null;
    return .{
        .ndv = ndv,
        .min = min,
        .max = max,
        .avg_width = widerWidth(l.avg_width, r.avg_width),
        .distinct_width = widerWidth(l.distinct_width, r.distinct_width),
        .origin = origin,
    };
}

fn sharedOrigin(l: ?ColumnOrigin, r: ?ColumnOrigin) ?ColumnOrigin {
    const lo = l orelse return null;
    const ro = r orelse return null;
    return if (lo.snapshot == ro.snapshot and lo.column == ro.column) lo else null;
}

/// The tighter of two distinct-value bounds.
pub fn minCard(a: ColCard, b: ColCard) ColCard {
    return switch (a) {
        .unknown => b,
        .exact => |x| switch (b) {
            .unknown => a,
            .exact => |y| .{ .exact = @min(x, y) },
        },
    };
}

fn widerWidth(l: ?u32, r: ?u32) ?u32 {
    return if (l) |lw| (if (r) |rw| @max(lw, rw) else null) else null;
}

/// Build the per-column stats for a UNION ALL over two arms whose schemas align
/// position-for-position. NDV is re-capped at the summed row ceiling. Returns
/// empty when neither arm carries stats (no information to merge).
pub fn unionColStats(
    allocator: Allocator,
    left: Query,
    right: Query,
    output_len: usize,
) ![]const ColStat {
    const l = left.stats();
    const r = right.stats();
    if (l.column_stats.len == 0 and r.column_stats.len == 0) return &.{};
    const ceiling = l.upper_rows +| r.upper_rows;
    const cc = try allocator.alloc(ColStat, output_len);
    for (cc, 0..) |*out, i| {
        const lstat: ColStat = if (i < l.column_stats.len) l.column_stats[i] else .{};
        const rstat: ColStat = if (i < r.column_stats.len) r.column_stats[i] else .{};
        out.* = capColStat(mergeUnionColStat(lstat, l.upper_rows, rstat, r.upper_rows), ceiling);
    }
    return cc;
}

/// A UNION ALL's row origin: the one both arms share, or the other arm's
/// when one is empty.
pub fn unionRowOrigin(l: PipelineStats, r: PipelineStats) ?RowOrigin {
    if (r.upper_rows == 0) return l.row_origin;
    if (l.upper_rows == 0) return r.row_origin;
    const lo = l.row_origin orelse return null;
    const ro = r.row_origin orelse return null;
    return if (lo.snapshot == ro.snapshot) lo else null;
}

/// The most distinct rows the first `width` columns of `st` hold: the
/// product of their NDVs when every one is known, capped at the key tuple
/// bound.
pub fn distinctRowBound(st: PipelineStats, width: usize) u64 {
    var product: ?u64 = 1;
    var from_row = width > 0;
    for (0..width) |i| {
        const stat: ColStat = if (i < st.column_stats.len) st.column_stats[i] else .{};
        from_row = from_row and fromRowOrigin(st, stat);
        product = switch (stat.ndv) {
            .exact => |n| if (product) |p| p *| n else null,
            .unknown => null,
        };
    }
    const ceiling = keyTupleBound(st, from_row);
    return if (product) |p| @min(p, ceiling) else ceiling;
}

/// The set operations that keep a subset of their left arm's distinct rows.
pub const SubsetSetOp = enum { intersect, except };

/// What an INTERSECT or EXCEPT over arms with stats `l` and `r` can return,
/// for its first `out.len` columns: every row it keeps is a distinct row of
/// the left arm, and for INTERSECT of the right arm too, so each column's
/// values are a subset of the left arm's column (and the right's). Fills
/// `out` and returns the row bound.
pub fn subsetSetOpBound(kind: SubsetSetOp, l: PipelineStats, r: PipelineStats, out: []ColStat) u64 {
    const rows = switch (kind) {
        .except => distinctRowBound(l, out.len),
        .intersect => @min(distinctRowBound(l, out.len), distinctRowBound(r, out.len)),
    };
    for (out, 0..) |*o, i| {
        const ls: ColStat = if (i < l.column_stats.len) l.column_stats[i] else .{};
        const rs: ColStat = if (i < r.column_stats.len) r.column_stats[i] else .{};
        o.* = switch (kind) {
            .except => ls,
            .intersect => .{
                .ndv = minCard(ls.ndv, rs.ndv),
                .min = tighterEnd(ls.min, rs.min, .max),
                .max = tighterEnd(ls.max, rs.max, .min),
                .avg_width = ls.avg_width,
                .distinct_width = ls.distinct_width,
                .origin = ls.origin,
            },
        };
        o.* = capColStat(o.*, rows);
    }
    return rows;
}

fn tighterEnd(a: ?i128, b: ?i128, pick: enum { min, max }) ?i128 {
    const x = a orelse return b;
    const y = b orelse return a;
    return switch (pick) {
        .min => @min(x, y),
        .max => @max(x, y),
    };
}

// ---------------------------------------------------------------------------
// Re-exports — callers @import("exec.zig") for everything operator-related
// ---------------------------------------------------------------------------

pub const predicate = @import("predicate.zig");
pub const Predicate = predicate.Predicate;
pub const PredicateOp = predicate.PredicateOp;
pub const PredicateExpr = predicate.PredicateExpr;
pub const leafExpr = predicate.leafExpr;
pub const isNullExpr = predicate.isNullExpr;
pub const isNotNullExpr = predicate.isNotNullExpr;
pub const statsOverlapPredicate = predicate.statsOverlapPredicate;

pub const Scan = @import("scan.zig").Scan;
pub const ParallelScan = @import("parallel_scan.zig").ParallelScan;
/// True if a derived projection column is row-local (so a parallel scan can run
/// it in its workers). Used by the compile layer to split a Compute into a
/// fusable subset + a serial remainder. See `parallel_scan.derivedFusable`.
pub const derivedFusable = @import("parallel_scan.zig").derivedFusable;
pub const FileScan = @import("file_scan.zig").FileScan;
pub const Filter = @import("filter.zig").Filter;
pub const Project = @import("project_limit.zig").Project;
pub const Limit = @import("project_limit.zig").Limit;
pub const LateScan = @import("latescan.zig").LateScan;
pub const ZonemapTopN = @import("zonemap_topn.zig").ZonemapTopN;
pub const rowloc = @import("rowloc.zig");

pub const global_dict = @import("global_dict.zig");
pub const GlobalDict = global_dict.GlobalDict;
pub const CodedColumn = global_dict.CodedColumn;

/// One column's RLE run view for `Batch.runs`: run k's value (sign-extended
/// to i64) repeats `lengths[k]` times. Slices live in Scan-owned scratch with
/// the batch's per-`next()` lifetime.
pub const RunsColumn = struct {
    values_i64: []const i64,
    lengths: []const u32,
};

/// Canonical 128-bit digest of one string group-key column's bytes — the unit
/// every hashed-key path agrees on: the scan's `Batch.hashed` sidecar carries
/// these, and consumers compute the identical digest from raw bytes for
/// batches without a sidecar. The digest (not the raw bytes) is what feeds a
/// composite key hash, so per-column digests compose across mixed sources.
pub fn stringKeyDigest(bytes: []const u8) u128 {
    const lo = std.hash.Wyhash.hash(0x9E3779B97F4A7C15, bytes);
    const hi = std.hash.Wyhash.hash(0xD1B54A32D192ED03, bytes);
    return (@as(u128, hi) << 64) | @as(u128, lo);
}

pub const sort_op = @import("sort.zig");
pub const Sort = sort_op.Sort;
pub const SortSpec = sort_op.SortSpec;

pub const aggregate_op = @import("aggregate.zig");
pub const Aggregate = aggregate_op.Aggregate;
pub const UdfAggregate = @import("udf_aggregate.zig").UdfAggregate;
pub const AggFunc = aggregate_op.AggFunc;
pub const AggSpec = aggregate_op.AggSpec;

pub const group_table = @import("group_table.zig");
pub const radix_aggregate = @import("radix_aggregate.zig");

pub const expr_mod = @import("expr.zig");
pub const Expr = expr_mod.Expr;
pub const scalar_fn = @import("scalar_fn.zig");
pub const ScalarFn = scalar_fn.ScalarFn;

pub const compute_op = @import("compute.zig");
pub const Compute = compute_op.Compute;
pub const Derived = compute_op.Derived;

pub const SetUnion = @import("set_union.zig").SetUnion;

pub const mat_stage = @import("mat_stage.zig");
pub const StageSet = mat_stage.StageSet;
pub const MatScan = mat_stage.MatScan;

pub const AliasRename = @import("alias_rename.zig").AliasRename;

pub const join_op = @import("join.zig");
pub const Join = join_op.Join;
pub const JoinSpec = join_op.Spec;
pub const JoinType = join_op.JoinType;
pub const KeyPair = join_op.KeyPair;

// PipelineStats / SortState are defined above; re-exported for clarity.

test {
    _ = predicate;
    _ = Scan;
    _ = Filter;
    _ = Project;
    _ = Limit;
    _ = Sort;
    _ = Aggregate;
    _ = UdfAggregate;
    _ = @import("exec_test.zig");
    _ = @import("aggregate_test.zig");
    _ = @import("scalar_fn_test.zig");
    _ = @import("scalar_fn_json.zig");
    _ = @import("scalar_fn_date.zig");
    _ = @import("time_zone.zig");
    _ = @import("scalar_fn_time.zig");
    _ = @import("scalar_fn_inet.zig");
    _ = @import("json_binary.zig");
    _ = @import("cast.zig");
    _ = LateScan;
    _ = ZonemapTopN;
    _ = rowloc;
    _ = @import("zonemap_topn_test.zig");
    _ = @import("group_table.zig");
    _ = silo_group_core;
    _ = v2_group_topn_engine;
    _ = @import("global_dict.zig");
    _ = @import("radix_aggregate.zig");
    _ = @import("concurrent_int_table.zig");
}
