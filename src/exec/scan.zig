//! Scan operator — reads segments (in manifest order), then the memtable.
//! Emits one Batch per row group (with tombstones applied) plus one final
//! Batch for the memtable.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const types = @import("../types.zig");
const Column = types.Column;
const Value = types.Value;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;
const sformat = @import("../storage/format.zig");
const hll = @import("../util/hll.zig");

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

const api = @import("../api/api.zig");
const Table = api.Table;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const Error = exec.Error;
const makeQuery = exec.makeQuery;

/// Slice a full-row-group `ColumnView` down to rows `[off, off+n)` for the scan
/// sub-batch experiment. `off` is a multiple of 64 (the sub-batch stride), so a
/// nullable column's bitmap slices on a byte boundary. Fixed-width slices the
/// element slice; strings keep the shared bytes and slice the `n+1` offsets.
fn subView(v: ColumnView, off: usize, n: usize) ColumnView {
    const nulls: ?[]const u8 = if (v.nulls) |bm| bm[off / 8 ..] else null;
    return switch (v.data) {
        .int => |s| .{ .data = .{ .int = s[off..][0..n] }, .nulls = nulls },
        .bigint => |s| .{ .data = .{ .bigint = s[off..][0..n] }, .nulls = nulls },
        .boolean => |s| .{ .data = .{ .boolean = s[off..][0..n] }, .nulls = nulls },
        .tinyint => |s| .{ .data = .{ .tinyint = s[off..][0..n] }, .nulls = nulls },
        .smallint => |s| .{ .data = .{ .smallint = s[off..][0..n] }, .nulls = nulls },
        .float => |s| .{ .data = .{ .float = s[off..][0..n] }, .nulls = nulls },
        .double => |s| .{ .data = .{ .double = s[off..][0..n] }, .nulls = nulls },
        .date => |s| .{ .data = .{ .date = s[off..][0..n] }, .nulls = nulls },
        .datetime => |s| .{ .data = .{ .datetime = s[off..][0..n] }, .nulls = nulls },
        .largeint => |s| .{ .data = .{ .largeint = s[off..][0..n] }, .nulls = nulls },
        .decimal64 => |s| .{ .data = .{ .decimal64 = s[off..][0..n] }, .nulls = nulls },
        .decimal128 => |s| .{ .data = .{ .decimal128 = s[off..][0..n] }, .nulls = nulls },
        .uuid => |s| .{ .data = .{ .uuid = s[off..][0..n] }, .nulls = nulls },
        .varchar => |sv| .{ .data = .{ .varchar = .{ .offsets = sv.offsets[off..][0 .. n + 1], .bytes = sv.bytes } }, .nulls = nulls },
        .string => |sv| .{ .data = .{ .string = .{ .offsets = sv.offsets[off..][0 .. n + 1], .bytes = sv.bytes } }, .nulls = nulls },
        .char => |sv| .{ .data = .{ .char = .{ .offsets = sv.offsets[off..][0 .. n + 1], .bytes = sv.bytes } }, .nulls = nulls },
    };
}

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;
const PredicateExpr = predicate.PredicateExpr;
const PredicateOp = predicate.PredicateOp;
const statsOverlapPredicate = predicate.statsOverlapPredicate;
const filter_mod = @import("filter.zig");

const rowloc = @import("rowloc.zig");

/// Collect the schema indices of every column a predicate references (deduped),
/// for any predicate shape the evaluator supports. Unknown column names are
/// skipped — the evaluator surfaces them loudly if they are genuinely absent.
fn collectPredicateColumns(
    allocator: std.mem.Allocator,
    expr: PredicateExpr,
    schema: []const Column,
    out: *std.ArrayListUnmanaged(usize),
) !void {
    const t = @import("../types.zig");
    switch (expr) {
        .leaf => |p| try addPredicateColumn(allocator, out, t.findColumn(schema, p.col)),
        .leaf_col_col => |lc| {
            try addPredicateColumn(allocator, out, t.findColumn(schema, lc.left));
            try addPredicateColumn(allocator, out, t.findColumn(schema, lc.right));
        },
        .is_null, .is_not_null => |col_name| try addPredicateColumn(allocator, out, t.findColumn(schema, col_name)),
        .like => |lp| try addPredicateColumn(allocator, out, t.findColumn(schema, lp.col)),
        .in_set => |s| try addPredicateColumn(allocator, out, t.findColumn(schema, s.col)),
        .@"and", .@"or" => |children| for (children) |child| try collectPredicateColumns(allocator, child, schema, out),
        .not => |child| try collectPredicateColumns(allocator, child.*, schema, out),
        else => {},
    }
}

fn addPredicateColumn(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(usize), idx_opt: ?usize) !void {
    const idx = idx_opt orelse return;
    for (out.items) |e| if (e == idx) return;
    try out.append(allocator, idx);
}

/// Build the per-projected-column propagated statistic for the whole scan:
///   - `ndv`: merge each column's per-segment HyperLogLog sketches
///     (register-wise max) into a distinct-value estimate, plus a
///     conservative allowance for un-flushed memtable rows. Merging HLL
///     avoids the over-counting that summing per-segment counts suffers as
///     segment count grows. `unknown` only when the estimate can't fit the
///     u32 bound; otherwise `exact`.
///   - `min`/`max`: fold the manifest's per-segment per-column i128 min/max
///     for int-family columns (see `predicate.typeHasRange`). A non-empty
///     memtable holds un-summarized rows, so its presence forces min/max to
///     `null` (we can't prove a bound over rows with no stored stats).
///     Columns whose type carries no usable numeric range stay `null`.
fn computeColumnStats(
    allocator: Allocator,
    columns: []const Column,
    segs: []const storage.ManifestEntry,
    out_phys: []const usize,
    memtable_rows: u64,
) ![]exec.ColStat {
    const stats = try allocator.alloc(exec.ColStat, out_phys.len);
    errdefer allocator.free(stats);
    for (stats, 0..) |*stat, j| {
        const ci = out_phys[j];
        var merged: hll.Hll = .{};
        var have_sketch = false;
        for (segs) |e| {
            const off = ci * hll.m;
            if (e.column_sketches.len >= off + hll.m) {
                const seg_sketch = hll.Hll.fromBytes(e.column_sketches[off .. off + hll.m]);
                merged.merge(&seg_sketch);
                have_sketch = true;
            }
        }

        const ndv: exec.ColCard = if (!have_sketch and memtable_rows == 0)
            .{ .exact = 0 }
        else blk: {
            const est = merged.estimate() +| memtable_rows;
            break :blk if (est > std.math.maxInt(u32)) .unknown else .{ .exact = @intCast(est) };
        };

        // Min/max only when the column type carries a usable numeric range
        // AND every contributing row is summarized in the manifest (the
        // memtable holds un-summarized rows, so any memtable row breaks the
        // proof). Fold the per-segment column min/max.
        var min: ?i128 = null;
        var max: ?i128 = null;
        if (predicate.typeHasRange(columns[ci].type) and memtable_rows == 0) {
            for (segs) |e| {
                if (e.column_stats.len <= ci) continue;
                const cs = e.column_stats[ci];
                min = if (min) |m| @min(m, cs.min) else cs.min;
                max = if (max) |m| @max(m, cs.max) else cs.max;
            }
        }

        stat.* = .{ .ndv = ndv, .min = min, .max = max };
    }
    return stats;
}

/// Resolve the projection-pushdown column set to physical (table-order)
/// indices. `needed == null` keeps every column. Names may be qualified
/// (`alias.col`); resolution defers to `types.findColumn`, which strips a
/// matching table/alias prefix. Duplicates collapse and the result is in
/// table order. An empty `needed` (e.g. `COUNT(*)` referencing no column)
/// yields an empty set — the scan then emits row-count-only batches and
/// never decodes (or even opens) a segment.
fn resolveOutPhys(
    allocator: Allocator,
    columns: []const Column,
    needed: ?[]const []const u8,
) ![]usize {
    const names = needed orelse {
        const all = try allocator.alloc(usize, columns.len);
        for (all, 0..) |*p, i| p.* = i;
        return all;
    };

    const seen = try allocator.alloc(bool, columns.len);
    defer allocator.free(seen);
    @memset(seen, false);
    var count: usize = 0;
    for (names) |name| {
        const idx = @import("../types.zig").findColumn(columns, name) orelse return Error.ColumnNotFound;
        if (!seen[idx]) {
            seen[idx] = true;
            count += 1;
        }
    }
    const out = try allocator.alloc(usize, count);
    var j: usize = 0;
    for (seen, 0..) |s, i| if (s) {
        out[j] = i;
        j += 1;
    };
    return out;
}

/// Max provable distinct-value bound for a string key to be dict-CODED for
/// GROUP BY (Phase 4.2). Above this, interning the column into the global dict
/// costs more than coding saves (profiled on high-card URL), so the key stays
/// on the normal materialized path.
const dict_code_max_ndv: u32 = 65536;

/// Process-wide FSST digest-fill accounting (--profile-ops). The fill runs on
/// silo-grid worker threads whose thread-local oprof slots never reach the
/// connection thread's dump, so these aggregate atomically; the MySQL handler
/// prints + resets them per query alongside the rgcache line.
pub var g_fsst_digest_ticks = std.atomic.Value(u64).init(0);
pub var g_fsst_digest_rows = std.atomic.Value(u64).init(0);
pub var g_fsst_digest_bytes = std.atomic.Value(u64).init(0);

pub const Scan = struct {
    allocator: Allocator,
    io: Io,
    table: *Table,

    segment_count: usize,

    /// Pinned snapshot of the memtable at scan-create time. The scan holds
    /// an `acquire`d reference so flush/delete/upsert can swap the table's
    /// active memtable without invalidating this scan's view. Released in
    /// `deinit`. Iteration uses `memtable_row_count` to bound the scan to
    /// rows that existed at capture time — appends after capture are
    /// invisible to this scan.
    memtable_snap: *engine.Memtable,
    memtable_row_count: u64,

    phase: Phase = .segments,
    cur_seg_idx: usize = 0,
    cur_rg_idx: usize = 0,

    /// Half-open row-group range this scan covers, as `(segment, row_group)`
    /// coordinates over the manifest snapshot. Default = the whole table:
    /// start `(0,0)`, end `(maxInt,0)` (a sentinel above any real segment
    /// index, so `atRangeEnd` is a no-op and iteration stops at the natural
    /// `segment_count` boundary). A parallel scan hands each worker a
    /// contiguous slice of the flat row-group list via `setRange` — the slice
    /// may start and end mid-segment, crossing segment boundaries. The
    /// COUNT(*)-style 0-column fast path is never range-split (it's manifest-
    /// only) so these only constrain the column-decoding paths.
    range_start_seg: usize = 0,
    range_start_rg: usize = 0,
    range_end_seg: usize = std.math.maxInt(usize),
    range_end_rg: usize = 0,
    /// Whether this scan also drains the memtable after its segment range.
    /// In a parallel scan exactly one worker (the last range) sets this true;
    /// all others false, so the memtable is scanned exactly once and ordered
    /// last. Default true (serial scans always cover the memtable).
    scan_memtable: bool = true,
    /// Whether this scan owns the table's shared ddl_lock (released in deinit).
    /// False for parallel workers — the orchestrator holds one shared lock for
    /// all of them. Default true (serial scans take and release their own).
    holds_ddl: bool = true,

    /// Pinned handle from the table's segment cache; `cur_segment` aliases its
    /// parsed `ReadSegment`. Released (not closed) by `closeCurSegment`.
    cur_seg_entry: ?*storage.cache.SegmentHandles.Entry = null,
    cur_segment: ?*storage.ReadSegment = null,
    /// Sorted, deduped tombstone offsets for the current segment (or null).
    cur_segment_tomb: ?[]u32 = null,
    /// Prefix sum: `cur_rg_first_row[k]` is the first row offset of row group k
    /// within the current segment.
    cur_rg_first_row: []u32 = &.{},

    decoded: []storage.OwnedColumn,
    decoded_valid: bool = false,
    views: []ColumnView,

    /// Scan sub-batch cursor (active only when `exec.scan_sub_batch > 0`). The
    /// row group is decoded once; `sub_off`/`sub_count` walk it in sub-batches,
    /// re-deriving sliced views into `views` from the still-live `decoded`
    /// buffers each pull. `sub_off == sub_count` ⟺ row group exhausted.
    sub_off: usize = 0,
    sub_count: usize = 0,
    /// Cache-aware rows-per-emit for the plain projection path (0 = emit the
    /// full row group). Computed once at create from the projection width via
    /// `exec.autoScanBatch`.
    sub_batch_rows: usize = 0,

    /// Physical column index for each PROJECTED column. Projection pushdown:
    /// when the query references only a subset of the table's columns, the
    /// Scan decodes and emits just those, so blocking operators upstream
    /// buffer fewer columns. `out_phys[j]` is the table-schema index of
    /// projected column `j`. Defaults to all columns in table order. When
    /// `emit_loc` is set, `out_phys.len` is one less than the output
    /// width — the trailing output column is the synthesized `__rowloc`.
    out_phys: []usize,
    /// Output schema = the table columns at `out_phys`, in that order, plus
    /// a trailing `__rowloc` BIGINT column when `emit_loc` is set. Aliases
    /// `table.schema.columns` only when no projection AND not emitting loc.
    out_schema: []const Column,
    out_schema_owned: bool = false,

    /// Late-materialization mode: append a trailing hidden `__rowloc` BIGINT
    /// column carrying each row's physical location (see rowloc.zig). The
    /// location is synthesized into the `decoded` array as just another
    /// column so tombstone compaction carries it correctly. Off by default —
    /// every existing call site is byte-for-byte unchanged.
    emit_loc: bool = false,

    /// Lazily allocated when a row group has rows tombstoned and we need to
    /// materialize a filtered batch. Reused across batches.
    filtered: ?[]ColumnStore = null,

    /// Late-mat (`emit_loc`) scratch holding the packed `__rowloc` values for
    /// the memtable batch. Owned; freed in `deinit`.
    memtable_loc_buf: []i64 = &.{},

    /// Pushed-down predicates used to skip row groups via min/max stats.
    prunes: std.ArrayList(PruneHint),
    /// Pushed-down `IN (...)` set hints for the same min/max row-group skip.
    in_prunes: std.ArrayListUnmanaged(InPruneHint) = .empty,
    /// Owns the reordered AND/OR child arrays produced when cost-ordering the
    /// fused predicate (see `orderFusedConjuncts`); freed in `deinit`.
    filter_rewritten: std.ArrayListUnmanaged([]PredicateExpr) = .empty,

    /// Fused WHERE predicate (set via `tryFuseFilter`). When non-null the Scan
    /// evaluates it itself: for each row group it builds a BORROWED typed view
    /// over the pinned/decompressed cache bytes, evaluates the predicate, and
    /// compacts only the survivors into owned `filtered` buffers — eliminating
    /// the full-column owned-copy that the old decode→Filter path made. The
    /// borrowed view + cache pins live entirely within one `next()` call and
    /// are released before returning; downstream only ever sees the owned,
    /// compacted survivor batch. The expr's backing memory is owned by the
    /// fusing Filter (which outlives this Scan), so the Scan never frees it.
    fused_filter: ?PredicateExpr = null,
    /// Scratch row-mask, grown on demand for the largest row group seen.
    /// Reused across `next()` calls; freed in `deinit`.
    mask_buf: []bool = &.{},
    /// Second scratch row-mask, used by the FOR-aware AND filter to hold each
    /// additional leaf's per-row result before ANDing it into `mask_buf`.
    /// Grown on demand; reused across `next()` calls; freed in `deinit`.
    mask_buf2: []bool = &.{},
    /// Scratch array of borrowed cache blocks for the fused-filter fast path,
    /// one slot per projected column. Reused across `next()` calls; the pins
    /// it holds are released within each call. Freed in `deinit`.
    borrow_blocks: []storage.ReadSegment.BorrowedBlock = &.{},

    /// Fused-filter eval-only columns: table-schema indices referenced by
    /// `fused_filter` that are NOT in `out_phys`. The scan decodes these per
    /// row group purely to evaluate the predicate against blocks — they are
    /// never emitted (the output stays exactly `out_phys`). Empty unless a
    /// fused filter references unprojected columns (the V2 group-topN path);
    /// the legacy path projects its filter columns, so this stays empty and
    /// the whole mechanism is a no-op. Owned; freed in `deinit`.
    filter_phys: []usize = &.{},
    /// Eval schema = `out_schema` ++ the `filter_phys` columns, used to resolve
    /// predicate column names. Owned; freed in `deinit`.
    filter_eval_schema: []Column = &.{},
    /// Scratch view array sized `out_schema.len + filter_phys.len`, presenting
    /// the combined (output ++ filter-only) batch to the evaluator. Owned.
    filter_eval_views: []ColumnView = &.{},
    /// Owned decode of the `filter_phys` columns for the current row group
    /// (segment path). Reused; released after each row group. Owned.
    filter_decoded: []storage.OwnedColumn = &.{},
    filter_decoded_valid: bool = false,

    /// The query memory accountant shared by every operator above this
    /// Scan (reached via `Query.accountant()`). Null = no tracking.
    /// `owns_accountant` is true only when this Scan minted it (the
    /// standalone / raw-builder path); on the SQL compile path the
    /// accountant is injected and owned by the query root (`CompileCtx`).
    owned_accountant: ?*exec.memory.MemoryAccountant = null,
    owns_accountant: bool = false,

    /// Per-projected-column propagated statistic (distinct-value bound +
    /// min/max range), merged across this scan's segment snapshot (+
    /// memtable rows). Computed once at create; borrowed by `stats()`. One
    /// slot per projected column.
    cached_stats: []exec.ColStat = &.{},

    /// When non-null, `seg_skip[i] == true` means segment at manifest
    /// index `i` is excluded by a pushed-down predicate on the leading
    /// order-key column (whose stats live in the manifest entry).
    /// Built incrementally by `addPrune` — the segment file is never
    /// opened for skipped segments. Null when no leading-key predicates
    /// have been pushed.
    seg_skip: ?[]bool = null,

    /// Diagnostic counter: number of segments actually opened via
    /// `readSegment`. Skipped segments don't increment it. Tests use
    /// this to verify segment-level pruning fires.
    segments_opened: u32 = 0,

    /// Diagnostic counters (read by ParallelScan's `--profile-ops` report and
    /// pruning tests): row groups this scan considered within its assigned
    /// range, how many it actually decoded (passed `rowGroupCanMatch`), and the
    /// total rows in those decoded row groups. `rgs_considered - rgs_scanned`
    /// is the zone-map row-group prune count for this scan.
    rgs_considered: u64 = 0,
    rgs_scanned: u64 = 0,
    rows_scanned: u64 = 0,

    /// Tight scan-kernel accounting (--profile-ops): ticks spent in the per-row-
    /// group data work, split into `scan_borrow_ticks` (decompress / cache-pin a
    /// column block) and `scan_kernel_ticks` (the actual compare / gather over
    /// the bytes — the inner SIMD loop). Excludes loop control, rowGroupCanMatch
    /// decisions, segment opening, and the ParallelScan worker farm-out.
    scan_borrow_ticks: u64 = 0,
    scan_kernel_ticks: u64 = 0,

    /// Phase 4.2 (Option A): when set, the projected column at `out_phys[code_col]`
    /// is emitted as global dict CODES (via the `Batch.coded` sidecar) instead of
    /// materialized strings — the consuming aggregate groups on the narrow code,
    /// skipping the dict→string expansion (60–97% of scan time on string GROUP
    /// BYs). `gdict` is the query-scoped GlobalDict the scan interns into. Set by
    /// the compile gate after construction; null = normal materialized emit.
    /// Per-projected-column GlobalDict for coded key columns: non-null at each
    /// position whose strings are emitted as global dict CODES (via the
    /// `Batch.coded` sidecar) instead of materialized strings, so the aggregate
    /// packs the narrow code into its group key and skips dict→string expansion.
    /// Lazily allocated (sized `out_phys.len`) by `setDictCodeColumn`; an empty
    /// slice means no coded columns. Freed in deinit.
    coded_dicts_by_j: []?*exec.GlobalDict = &.{},
    n_coded: usize = 0,
    /// Per-projected-column global-code buffer (reused across batches); only the
    /// coded positions are used. Each emitted `CodedColumn` aliases its buffer
    /// (same per-`next()` lifetime as `views`). Freed in deinit.
    code_bufs: []std.ArrayListUnmanaged(u32) = &.{},
    /// Sidecar slots (one per projected column), null except coded positions in
    /// a coded batch. Lazily allocated; freed in deinit.
    coded_slots: []?exec.CodedColumn = &.{},
    /// Sidecar produced by the FILTERED path (`materializeSurvivors` /
    /// `evalAndCompact`) when coding; `nextFiltered` attaches it to the emitted
    /// batch. Aliases `coded_slots`; null on a non-coded row group.
    filtered_coded: ?[]const ?exec.CodedColumn = null,
    /// Key-digest emit (`Batch.hashed`): positions whose strings are consumed
    /// only as hashed group identity (recovered at emit via rowref late-mat).
    /// The digest is computed straight off the cached decompressed block — a
    /// dict block hashes each distinct value once, a raw block hashes rows
    /// through an in-place view — so the dict→string expansion (the dominant
    /// scan cost on wide string keys) never runs. Tombstoned row groups and
    /// the memtable's unfiltered path emit plain batches without the sidecar;
    /// consumers recompute the same digest from bytes there.
    hash_cols_by_j: []bool = &.{},
    n_hashed: usize = 0,
    hash_bufs: []std.ArrayListUnmanaged(u128) = &.{},
    hashed_slots: []?[]const u128 = &.{},
    filtered_hashed: ?[]const ?[]const u128 = null,
    /// RLE run emit (`Batch.runs`): when requested, the unfiltered segment path
    /// attaches each int-family column's run list (widened off the block's RLE
    /// header) alongside the materialized values — a run-aware consumer skips
    /// per-row key work entirely. Tombstoned row groups, the memtable, and the
    /// filtered path emit no sidecar (compaction breaks run alignment).
    emit_runs: bool = false,
    runs_v_bufs: []std.ArrayListUnmanaged(i64) = &.{},
    runs_l_bufs: []std.ArrayListUnmanaged(u32) = &.{},
    runs_slots: []?exec.RunsColumn = &.{},

    const Phase = enum { segments, memtable, done };

    pub const PruneHint = struct {
        col_idx: usize,
        op: PredicateOp,
        val: Value,
        /// String range hints (`lt`/`lte`) only: another hint on this column
        /// proves no `''` row can survive the full conjunct set, so the
        /// overlap test may use the blank-excluded min instead of the plain
        /// min (which is `''` for nearly every string column). Maintained by
        /// `addPrune` as hints arrive.
        blanks_excluded: bool = false,
    };

    /// Set-membership (`col IN (...)`) zone-map hint: a row group can be
    /// skipped when none of `values` falls in the column's [min,max]. `values`
    /// are the IN literals reduced to i128 (owned; freed in `deinit`).
    pub const InPruneHint = struct {
        col_idx: usize,
        values: []i128,
    };

    /// Standalone scan: mints + owns its own accountant from the table's
    /// configured budget. Used by the raw builder API and tests.
    pub fn create(allocator: Allocator, table: *Table) !Query {
        return createWithAccountant(allocator, table, null);
    }

    /// Compile-path scan: when `injected` is non-null it becomes the
    /// query-scoped accountant (owned by the query root, not by this
    /// Scan). When null, behaves like `create` (self-mint per budget).
    pub fn createWithAccountant(
        allocator: Allocator,
        table: *Table,
        injected: ?*exec.memory.MemoryAccountant,
    ) !Query {
        return createWithProjection(allocator, table, injected, null);
    }

    /// Like `createWithAccountant`, but `needed` (when non-null) restricts
    /// the scan's output to those columns by name — projection pushdown, so
    /// upstream blocking operators buffer fewer columns. Unknown names
    /// error; `null` reads every column.
    pub fn createWithProjection(
        allocator: Allocator,
        table: *Table,
        injected: ?*exec.memory.MemoryAccountant,
        needed: ?[]const []const u8,
    ) !Query {
        return createWithProjectionLoc(allocator, table, injected, needed, false);
    }

    /// Like `createWithProjection`, but `emit_loc` (when true) appends a
    /// trailing hidden `__rowloc` BIGINT column carrying each row's physical
    /// location. Used by `LateScan` for late materialization. Every other
    /// call site goes through `createWithProjection` with `emit_loc = false`,
    /// leaving its output schema unchanged.
    pub fn createWithProjectionLoc(
        allocator: Allocator,
        table: *Table,
        injected: ?*exec.memory.MemoryAccountant,
        needed: ?[]const []const u8,
        emit_loc: bool,
    ) !Query {
        const self = try allocWithProjectionLoc(allocator, table, injected, needed, emit_loc, null);
        return makeQuery(allocator, self);
    }

    /// Same as `createWithProjectionLoc` but returns the raw `*Scan` instead of
    /// the type-erased `Query`. `LateScan` builds its inner Scan through this
    /// so it can reach `memtableSnap()` directly, then wraps the pointer in a
    /// `Query` via `exec.makeQuery`.
    /// A consistent `(segments, memtable)` view captured once under the table
    /// mutex. The parallel-scan orchestrator captures this and hands the SAME
    /// snapshot to every worker (via `allocWithProjectionLoc`'s `injected_snap`)
    /// so a flush between worker constructions can't make them disagree on which
    /// rows live in segments vs. the memtable.
    pub const Snapshot = struct {
        segment_count: usize,
        memtable_snap: *engine.Memtable,
        memtable_row_count: u64,
    };

    /// Capture a consistent snapshot, pinning the memtable. The caller owns the
    /// returned pin and must `memtable_snap.release()` it once every worker Scan
    /// (each of which acquires its own pin) has been constructed.
    pub fn captureSnapshot(table: *Table) Snapshot {
        table.mutex.lockUncancelable(table.io);
        defer table.mutex.unlock(table.io);
        const ms = table.memtable;
        ms.acquire();
        return .{
            .segment_count = table.manifest.segments.items.len,
            .memtable_snap = ms,
            .memtable_row_count = ms.row_count,
        };
    }

    pub fn allocWithProjectionLoc(
        allocator: Allocator,
        table: *Table,
        injected: ?*exec.memory.MemoryAccountant,
        needed: ?[]const []const u8,
        emit_loc: bool,
        injected_snap: ?Snapshot,
    ) !*Scan {
        const out_phys = try resolveOutPhys(allocator, table.schema.columns, needed);
        errdefer allocator.free(out_phys);

        // The output schema owns its own buffer whenever it diverges from the
        // table's column slice — i.e. on any projection OR when appending the
        // synthesized `__rowloc` column.
        const out_schema_owned = needed != null or emit_loc;
        const proj_len = out_phys.len;
        const out_len = proj_len + @intFromBool(emit_loc);
        const out_schema: []const Column = if (out_schema_owned) blk: {
            const s = try allocator.alloc(Column, out_len);
            for (s[0..proj_len], out_phys) |*c, p| c.* = table.schema.columns[p];
            if (emit_loc) s[proj_len] = .{ .name = rowloc.col_name, .type = .bigint };
            break :blk s;
        } else table.schema.columns;
        errdefer if (out_schema_owned) allocator.free(@constCast(out_schema));

        const k = out_len;
        const decoded = try allocator.alloc(storage.OwnedColumn, k);
        errdefer allocator.free(decoded);
        const views = try allocator.alloc(ColumnView, k);
        errdefer allocator.free(views);

        const self = try allocator.create(Scan);
        errdefer allocator.destroy(self);

        // Take the table's ddl_lock SHARED for the scan's entire lifetime.
        // This blocks DDL (drop/alter/rename) from running while we have
        // segment files and a pinned memtable in-flight. DDL waits at its
        // exclusive lock acquisition until we deinit and release.
        //
        // A parallel-scan worker (injected_snap != null) does NOT take it: the
        // orchestrator holds a single shared lock covering all its workers.
        // Multiple shared acquisitions during construction could otherwise
        // deadlock against an arriving DDL writer (writer-preference would block
        // the next worker's acquire while the orchestrator can't finish).
        const holds_ddl = injected_snap == null;
        if (holds_ddl) table.ddl_lock.lockSharedUncancelable(table.io);
        errdefer if (holds_ddl) table.ddl_lock.unlockShared(table.io);

        // Capture (segment_count, memtable_snap, memtable_row_count) atomically
        // under the table mutex so we see a consistent (segments, memtable)
        // pair. Pin the memtable via `acquire` so a subsequent flush/delete
        // swap doesn't invalidate our pointer.
        //
        // Iteration is bounded by the row count we capture here, so any rows
        // appended after we release the mutex are invisible to this scan.
        // The remaining hazard — a concurrent writer reallocating an
        // ArrayList we're iterating — is handled in the writer paths:
        // `Table.insertLocked` / `insertBatch` detect a pinned memtable
        // (`Memtable.hasSnapshotReaders`) and clone-then-replace before
        // mutating, leaving our snapshot's buffers untouched.
        var segment_count: usize = undefined;
        var memtable_snap: *engine.Memtable = undefined;
        var memtable_row_count: u64 = undefined;
        if (injected_snap) |snap| {
            // Parallel worker: reuse the orchestrator's single captured view so
            // every worker agrees. Acquire our own pin (balanced by deinit).
            segment_count = snap.segment_count;
            memtable_snap = snap.memtable_snap;
            memtable_snap.acquire();
            memtable_row_count = snap.memtable_row_count;
        } else {
            table.mutex.lockUncancelable(table.io);
            segment_count = table.manifest.segments.items.len;
            memtable_snap = table.memtable;
            memtable_snap.acquire();
            memtable_row_count = memtable_snap.row_count;
            table.mutex.unlock(table.io);
        }

        // Use the injected query-scoped accountant when present. Otherwise
        // mint our own from the table's configured budget (heap-allocated
        // so all operators in the pipeline share it via a pointer) and own
        // its lifetime.
        var owned_accountant: ?*exec.memory.MemoryAccountant = injected;
        var owns_accountant = false;
        if (injected == null and table.query_memory_budget > 0) {
            const acc = try allocator.create(exec.memory.MemoryAccountant);
            acc.* = exec.memory.MemoryAccountant.init(table.query_memory_budget);
            owned_accountant = acc;
            owns_accountant = true;
        }
        errdefer if (owns_accountant) {
            if (owned_accountant) |a| allocator.destroy(a);
        };

        const cached_stats = try computeColumnStats(
            allocator,
            table.schema.columns,
            table.manifest.segments.items[0..segment_count],
            out_phys,
            memtable_row_count,
        );
        errdefer allocator.free(cached_stats);

        self.* = .{
            .allocator = allocator,
            .io = table.io,
            .table = table,
            .segment_count = segment_count,
            .memtable_snap = memtable_snap,
            .memtable_row_count = memtable_row_count,
            .decoded = decoded,
            .views = views,
            .out_phys = out_phys,
            .out_schema = out_schema,
            .out_schema_owned = out_schema_owned,
            .emit_loc = emit_loc,
            .holds_ddl = holds_ddl,
            .prunes = .empty,
            .owned_accountant = owned_accountant,
            .owns_accountant = owns_accountant,
            .cached_stats = cached_stats,
        };

        // Cache-aware scan sub-batch: size the per-emit row count from the
        // projection's per-row width so a wide multi-column batch stays
        // L2-resident. Disabled for late-mat (its own narrow-probe path).
        if (!emit_loc) {
            var row_bytes: usize = 0;
            for (out_schema) |col| row_bytes += exec.memory.estimateColumnBytes(col.type);
            self.sub_batch_rows = exec.autoScanBatch(row_bytes);
        }

        return self;
    }

    pub fn accountant(self: *Scan) ?*exec.memory.MemoryAccountant {
        return self.owned_accountant;
    }

    /// Restrict this scan to the half-open flat row-group range
    /// `[(start_seg,start_rg), (end_seg,end_rg))` and choose whether it drains
    /// the memtable. Must be called before the first `next()`. The parallel
    /// scan orchestrator uses this to hand each worker a contiguous block span.
    pub fn setRange(self: *Scan, start_seg: usize, start_rg: usize, end_seg: usize, end_rg: usize, scan_memtable: bool) void {
        self.range_start_seg = start_seg;
        self.range_start_rg = start_rg;
        self.range_end_seg = end_seg;
        self.range_end_rg = end_rg;
        self.scan_memtable = scan_memtable;
        self.cur_seg_idx = start_seg;
    }

    /// Reuse this scan object for another half-open row-group range. This keeps
    /// projection/filter setup cached while allowing a scheduler to lease scan
    /// tiles dynamically.
    pub fn resetRange(self: *Scan, start_seg: usize, start_rg: usize, end_seg: usize, end_rg: usize, scan_memtable: bool) void {
        self.releaseBatch();
        self.closeCurSegment();
        self.phase = .segments;
        self.cur_seg_idx = start_seg;
        self.cur_rg_idx = start_rg;
        self.range_start_seg = start_seg;
        self.range_start_rg = start_rg;
        self.range_end_seg = end_seg;
        self.range_end_rg = end_rg;
        self.scan_memtable = scan_memtable;
    }

    /// True once the cursor has reached this scan's assigned range end — the
    /// signal to stop the segments phase (mid-segment ends are honored). A
    /// no-op for the default full-table range (`range_end_seg == maxInt`).
    fn atRangeEnd(self: *const Scan) bool {
        return self.cur_seg_idx > self.range_end_seg or
            (self.cur_seg_idx == self.range_end_seg and self.cur_rg_idx >= self.range_end_rg);
    }

    /// Projected-column position of `name` if it can be emitted as global dict
    /// CODES, else null. Qualifies: a flushed table (empty memtable), not
    /// late-mat, a non-nullable string column, PROVABLY low-cardinality (exact
    /// NDV ≤ the cap). Coding a high-card column is a ~2× regression (Q33 `GROUP
    /// BY URL`): building the global dict interns millions of distinct strings,
    /// far more than the narrow grouping saves. (A fused WHERE is fine — the
    /// filtered path codes survivors row-granularly.)
    fn codeColIdx(self: *Scan, name: []const u8) ?usize {
        if (self.emit_loc or self.memtable_row_count != 0) return null;
        for (self.out_phys, 0..) |phys, j| {
            const col = self.table.schema.columns[phys];
            if (!@import("../types.zig").columnNameEql(col.name, name)) continue;
            if (col.nullable) return null;
            switch (col.type) {
                .varchar, .string, .char => {},
                else => return null,
            }
            const low_card = j < self.cached_stats.len and switch (self.cached_stats[j].ndv) {
                .exact => |ndv| ndv <= dict_code_max_ndv,
                .unknown => false,
            };
            return if (low_card) j else null;
        }
        return null;
    }

    /// Non-committing pre-check: can `name` be grouped as dict codes? The GROUP
    /// BY gate validates EVERY key column before committing any (a partial
    /// commit would desync the scan's coded emit from the aggregate's key).
    pub fn canCodeColumn(self: *Scan, name: []const u8) bool {
        return self.codeColIdx(name) != null;
    }

    /// Mark the projected column `name` for coded emit (see `codeColIdx`).
    pub fn setDictCodeColumn(self: *Scan, name: []const u8, dict: *exec.GlobalDict) bool {
        const j = self.codeColIdx(name) orelse return false;
        self.ensureCodedArrays() catch return false;
        self.coded_dicts_by_j[j] = dict;
        self.n_coded += 1;
        return true;
    }

    /// Undo all coded-column setup — used by the gate to roll back a partial
    /// commit when the packed group key turns out not to fit the int budget.
    pub fn clearDictCodeColumns(self: *Scan) void {
        for (self.coded_dicts_by_j) |*d| d.* = null;
        self.n_coded = 0;
    }

    /// Mark the projected column `name` for key-digest emit (`Batch.hashed`).
    /// Qualifies: a non-nullable string column. No NDV gate — the digest
    /// replaces materialization for keys consumed only as hashed identity, at
    /// any cardinality.
    pub fn setHashKeyColumn(self: *Scan, name: []const u8) bool {
        const j = self.hashColIdx(name) orelse return false;
        self.ensureHashArrays() catch return false;
        if (!self.hash_cols_by_j[j]) {
            self.hash_cols_by_j[j] = true;
            self.n_hashed += 1;
        }
        return true;
    }

    fn hashColIdx(self: *Scan, name: []const u8) ?usize {
        for (self.out_phys, 0..) |phys, j| {
            const col = self.table.schema.columns[phys];
            if (!@import("../types.zig").columnNameEql(col.name, name)) continue;
            if (col.nullable) return null;
            return switch (col.type) {
                .varchar, .string, .char => j,
                else => null,
            };
        }
        return null;
    }

    fn ensureHashArrays(self: *Scan) !void {
        if (self.hash_cols_by_j.len == self.out_phys.len) return;
        const flags = try self.allocator.alloc(bool, self.out_phys.len);
        @memset(flags, false);
        const bufs = self.allocator.alloc(std.ArrayListUnmanaged(u128), self.out_phys.len) catch |e| {
            self.allocator.free(flags);
            return e;
        };
        for (bufs) |*b| b.* = .empty;
        self.hash_cols_by_j = flags;
        self.hash_bufs = bufs;
    }

    fn ensureHashedSlots(self: *Scan) ![]?[]const u128 {
        if (self.hashed_slots.len != self.out_phys.len) {
            if (self.hashed_slots.len > 0) self.allocator.free(self.hashed_slots);
            self.hashed_slots = try self.allocator.alloc(?[]const u128, self.out_phys.len);
        }
        for (self.hashed_slots) |*s| s.* = null;
        return self.hashed_slots;
    }

    /// Fill `runs_slots[j]` for every projected non-nullable int-family column
    /// whose block in this row group is RLE: widen the run values to i64 and
    /// copy the run lengths into reused scratch. Returns the slots when at
    /// least one column produced runs, else null.
    fn fillRunsSidecar(self: *Scan, seg: *storage.ReadSegment, rg_idx: usize, rg_count: u32) !?[]const ?exec.RunsColumn {
        const slots = try self.ensureRunsArrays();
        var any = false;
        for (self.out_phys, 0..) |phys, j| {
            switch (self.table.schema.columns[phys].type) {
                .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => {},
                else => continue,
            }
            // A nullable column's run stream carries NULL placeholder values;
            // skip rather than hand consumers placeholders as identity.
            if (self.table.schema.columns[phys].nullable) continue;
            const flags = storage.format.ColumnBlockFlags{ .has_nulls = false };
            var block = try seg.borrowColumnBlock(self.allocator, rg_idx, phys, &self.table.cache);
            defer block.release(self.allocator, &self.table.cache);
            if (block.encoding != .rle) continue;
            const rb = storage.segment_reader.rleViewOf(block.bytes, rg_count, flags).block;
            switch (rb.value_width) {
                inline 1, 2, 4, 8 => |W| {
                    const T = std.meta.Int(.signed, W * 8);
                    try self.runs_v_bufs[j].resize(self.allocator, rb.n_runs);
                    try self.runs_l_bufs[j].resize(self.allocator, rb.n_runs);
                    const vs = self.runs_v_bufs[j].items;
                    const ls = self.runs_l_bufs[j].items;
                    for (0..rb.n_runs) |k| {
                        vs[k] = std.mem.readInt(T, rb.values[k * W ..][0..W], .little);
                        ls[k] = rb.runLength(k);
                    }
                    slots[j] = .{ .values_i64 = vs, .lengths = ls };
                    any = true;
                },
                else => {},
            }
        }
        return if (any) slots else null;
    }

    fn ensureRunsArrays(self: *Scan) ![]?exec.RunsColumn {
        if (self.runs_v_bufs.len != self.out_phys.len) {
            const vb = try self.allocator.alloc(std.ArrayListUnmanaged(i64), self.out_phys.len);
            for (vb) |*b| b.* = .empty;
            const lb = self.allocator.alloc(std.ArrayListUnmanaged(u32), self.out_phys.len) catch |e| {
                self.allocator.free(vb);
                return e;
            };
            for (lb) |*b| b.* = .empty;
            self.runs_v_bufs = vb;
            self.runs_l_bufs = lb;
        }
        if (self.runs_slots.len != self.out_phys.len) {
            if (self.runs_slots.len > 0) self.allocator.free(self.runs_slots);
            self.runs_slots = try self.allocator.alloc(?exec.RunsColumn, self.out_phys.len);
        }
        for (self.runs_slots) |*s| s.* = null;
        return self.runs_slots;
    }

    /// Lazily allocate the per-projected-column coded arrays (dict map + code
    /// buffers), sized to `out_phys.len`. Idempotent across repeated
    /// `setDictCodeColumn` calls (one per coded group column).
    fn ensureCodedArrays(self: *Scan) !void {
        if (self.coded_dicts_by_j.len == self.out_phys.len) return;
        const dicts = try self.allocator.alloc(?*exec.GlobalDict, self.out_phys.len);
        for (dicts) |*d| d.* = null;
        const bufs = try self.allocator.alloc(std.ArrayListUnmanaged(u32), self.out_phys.len);
        for (bufs) |*b| b.* = .empty;
        self.coded_dicts_by_j = dicts;
        self.code_bufs = bufs;
    }

    /// The pinned memtable snapshot this scan reads from. `LateScan` reaches
    /// through it to materialize memtable-resident survivors by row index.
    pub fn memtableSnap(self: *Scan) *engine.Memtable {
        return self.memtable_snap;
    }

    pub fn explain(self: *Scan, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainIndent(out, allocator, depth);
        try out.appendSlice(allocator, "Scan ");
        try out.appendSlice(allocator, self.table.name);
        try out.append(allocator, '\n');
    }

    pub fn deinit(self: *Scan) void {
        self.releaseBatch();
        self.closeCurSegment();
        if (self.mask_buf.len > 0) self.allocator.free(self.mask_buf);
        if (self.mask_buf2.len > 0) self.allocator.free(self.mask_buf2);
        if (self.borrow_blocks.len > 0) self.allocator.free(self.borrow_blocks);
        if (self.memtable_loc_buf.len > 0) self.allocator.free(self.memtable_loc_buf);
        if (self.cached_stats.len > 0) self.allocator.free(self.cached_stats);
        for (self.code_bufs) |*b| b.deinit(self.allocator);
        if (self.code_bufs.len > 0) self.allocator.free(self.code_bufs);
        if (self.coded_dicts_by_j.len > 0) self.allocator.free(self.coded_dicts_by_j);
        if (self.coded_slots.len > 0) self.allocator.free(self.coded_slots);
        for (self.hash_bufs) |*b| b.deinit(self.allocator);
        if (self.hash_bufs.len > 0) self.allocator.free(self.hash_bufs);
        if (self.hash_cols_by_j.len > 0) self.allocator.free(self.hash_cols_by_j);
        if (self.hashed_slots.len > 0) self.allocator.free(self.hashed_slots);
        for (self.runs_v_bufs) |*b| b.deinit(self.allocator);
        if (self.runs_v_bufs.len > 0) self.allocator.free(self.runs_v_bufs);
        for (self.runs_l_bufs) |*b| b.deinit(self.allocator);
        if (self.runs_l_bufs.len > 0) self.allocator.free(self.runs_l_bufs);
        if (self.runs_slots.len > 0) self.allocator.free(self.runs_slots);
        self.prunes.deinit(self.allocator);
        for (self.in_prunes.items) |hint| self.allocator.free(hint.values);
        self.in_prunes.deinit(self.allocator);
        for (self.filter_rewritten.items) |slice| self.allocator.free(slice);
        self.filter_rewritten.deinit(self.allocator);
        if (self.owns_accountant) {
            if (self.owned_accountant) |a| self.allocator.destroy(a);
        }
        if (self.seg_skip) |s| self.allocator.free(s);
        if (self.filtered) |arr| {
            for (arr) |*c| c.deinit(self.allocator);
            self.allocator.free(arr);
        }
        self.allocator.free(self.decoded);
        self.allocator.free(self.views);
        if (self.filter_phys.len > 0) self.allocator.free(self.filter_phys);
        if (self.filter_eval_schema.len > 0) self.allocator.free(self.filter_eval_schema);
        if (self.filter_eval_views.len > 0) self.allocator.free(self.filter_eval_views);
        if (self.filter_decoded.len > 0) self.allocator.free(self.filter_decoded);
        self.allocator.free(self.out_phys);
        if (self.out_schema_owned) self.allocator.free(@constCast(self.out_schema));
        // Drop our pinned memtable reference. If we held the last one and
        // the memtable was retired (a flush/delete swapped it out), it's
        // freed here.
        self.memtable_snap.release();
        // Release our shared ddl_lock — any DDL waiter on the exclusive
        // lock can now proceed once all shared holders have released.
        // Parallel workers don't hold it (the orchestrator does).
        if (self.holds_ddl) self.table.ddl_lock.unlockShared(self.table.io);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    fn closeCurSegment(self: *Scan) void {
        if (self.cur_seg_entry) |e| {
            self.table.releaseSegment(e);
            self.cur_seg_entry = null;
            self.cur_segment = null;
        }
        if (self.cur_segment_tomb) |t| {
            self.allocator.free(t);
            self.cur_segment_tomb = null;
        }
        if (self.cur_rg_first_row.len > 0) {
            self.allocator.free(self.cur_rg_first_row);
            self.cur_rg_first_row = &.{};
        }
    }

    fn ensureFilteredBuffers(self: *Scan) ![]ColumnStore {
        if (self.filtered) |arr| return arr;
        const arr = try self.allocator.alloc(ColumnStore, self.out_schema.len);
        errdefer self.allocator.free(arr);
        var inited: usize = 0;
        errdefer for (arr[0..inited]) |*c| c.deinit(self.allocator);
        for (self.out_schema, 0..) |col, i| {
            arr[i] = try ColumnStore.init(self.allocator, col.type, col.nullable);
            inited += 1;
        }
        self.filtered = arr;
        return arr;
    }

    /// Accept a full predicate for scan-side in-place filtering. Declines
    /// (returns false) only for the count-only scan (no columns to filter). The
    /// late-materialization scan (`emit_loc`) IS now fused: the survivor-
    /// compaction paths (`materializeSurvivors` / `evalAndCompact`) additionally
    /// pack each survivor's `__rowloc` from the same mask, so the filter runs in
    /// the scan and only survivors flow out — no separate Filter over all rows.
    /// On acceptance the predicate's leaves must reference only columns this scan
    /// projects — guaranteed when the fusing Filter sits directly above and
    /// shares this scan's output schema. The expr is stored by value; its
    /// backing memory is owned by the caller and must outlive this scan.
    /// True once a WHERE predicate has been fused into this scan (so `next()`
    /// emits compacted survivors, not raw row groups). ParallelScan reads this
    /// to choose its execution strategy.
    pub fn fusedActive(self: *const Scan) bool {
        return self.fused_filter != null;
    }

    pub fn tryFuseFilter(self: *Scan, expr: PredicateExpr) !bool {
        if (self.out_phys.len == 0) return false;
        if (self.fused_filter != null) return false;
        // Coerce/validate the predicate's literals against the table schema
        // (e.g. a text date literal `'2013-07-01'` → a date value). The legacy
        // path does this in `Filter.create` before fusing; a directly-fused
        // V2 filter would otherwise compare a date column to raw text and match
        // nothing. Idempotent — re-validating an already-coerced expr is a
        // no-op.
        var coerced = expr;
        try predicate.validateExpr(&coerced, self.table.schema.columns);
        self.fused_filter = coerced;
        try self.setupFilterEval(coerced);
        try self.extractPruneHints(coerced);
        try self.orderFusedConjuncts();
        return true;
    }

    /// Cost-order the fused predicate's conjuncts (cheapest/most-selective
    /// first) so the evaluator's short-circuit skips already-excluded rows in
    /// the costlier predicates — parity with the legacy Filter operator's
    /// `simplifyPredicate`. Reordering is commutative, so results are
    /// unchanged. Ordering uses SCHEMA-WIDE manifest stats (the filter may
    /// reference unprojected columns): real NDVs put a hyper-selective
    /// equality (URLHash = const, NDV ~20M) ahead of its cost-class peers,
    /// which the guided AND's empty-mask early-exit then turns into whole
    /// column blocks never borrowed.
    fn orderFusedConjuncts(self: *Scan) !void {
        const expr = self.fused_filter orelse return;
        const schema = self.table.schema.columns;
        const all_phys = try self.allocator.alloc(usize, schema.len);
        defer self.allocator.free(all_phys);
        for (all_phys, 0..) |*p, i| p.* = i;
        const col_stats = try computeColumnStats(
            self.allocator,
            schema,
            self.table.manifest.segments.items[0..self.segment_count],
            all_phys,
            self.memtable_row_count,
        );
        defer self.allocator.free(col_stats);
        self.fused_filter = try filter_mod.orderPredicate(self.allocator, &self.filter_rewritten, expr, schema, col_stats);
    }

    /// Walk the top-level AND conjuncts and register zone-map hints for each
    /// comparison leaf (eq/neq/range, via `addPrune`) and each `IN (...)` set
    /// (via `addInPrune`). Conjunction is sound for pruning: every conjunct
    /// must hold, so a row group excluded by any one can be skipped. OR/NOT
    /// subtrees are not descended — pruning a disjunct's column is unsound.
    fn extractPruneHints(self: *Scan, expr: PredicateExpr) !void {
        switch (expr) {
            .@"and" => |children| for (children) |child| try self.extractPruneHints(child),
            .leaf => |p| self.addPrune(p) catch {},
            .in_set => |s| if (!s.negate) try self.addInPrune(s),
            else => {},
        }
    }

    fn addInPrune(self: *Scan, s: predicate.InSet) !void {
        const col_idx = types.findColumn(self.table.schema.columns, s.col) orelse return;
        if (!storage.format.typeHasStats(self.table.schema.columns[col_idx].type)) return;
        var values = try self.allocator.alloc(i128, s.values.len);
        errdefer self.allocator.free(values);
        var n: usize = 0;
        for (s.values) |v| {
            if (predicate.valueToRangeI128(v)) |iv| {
                values[n] = iv;
                n += 1;
            }
        }
        if (n == 0) {
            self.allocator.free(values);
            return;
        }
        try self.in_prunes.append(self.allocator, .{ .col_idx = col_idx, .values = values[0..n] });
    }

    /// Set up block-sourced evaluation for any predicate column not already in
    /// the projection, so the fused filter evaluates without projecting those
    /// columns into the output. No-op when every filter column is already
    /// projected (nothing extra to source). Runs for late-materialization
    /// (`emit_loc`) scans too: the V2 group-topN string-key path projects only
    /// its group keys, so the WHERE columns are unprojected and must be
    /// block-sourced here — decoded on demand for the predicate, then discarded,
    /// never carried into the grouped rows (the same thin treatment the key
    /// values get via the rowref). The eval schema is sized to the projected
    /// physical columns (`out_phys`), excluding the synthesized trailing
    /// `__rowloc` that `out_schema` carries under `emit_loc`: the filtered path
    /// presents `out_phys` views + the block-sourced columns, and packs each
    /// survivor's `__rowloc` separately.
    fn setupFilterEval(self: *Scan, expr: PredicateExpr) !void {
        var refs: std.ArrayListUnmanaged(usize) = .empty;
        defer refs.deinit(self.allocator);
        try collectPredicateColumns(self.allocator, expr, self.table.schema.columns, &refs);

        var extra: std.ArrayListUnmanaged(usize) = .empty;
        errdefer extra.deinit(self.allocator);
        for (refs.items) |phys| {
            var projected = false;
            for (self.out_phys) |o| {
                if (o == phys) {
                    projected = true;
                    break;
                }
            }
            if (!projected) try extra.append(self.allocator, phys);
        }
        if (extra.items.len == 0) {
            extra.deinit(self.allocator);
            return;
        }
        self.filter_phys = try extra.toOwnedSlice(self.allocator);
        errdefer {
            self.allocator.free(self.filter_phys);
            self.filter_phys = &.{};
        }
        const out_w = self.out_phys.len;
        const eval_schema = try self.allocator.alloc(Column, out_w + self.filter_phys.len);
        errdefer self.allocator.free(eval_schema);
        @memcpy(eval_schema[0..out_w], self.out_schema[0..out_w]);
        for (self.filter_phys, 0..) |phys, i| eval_schema[out_w + i] = self.table.schema.columns[phys];
        self.filter_eval_schema = eval_schema;
        self.filter_eval_views = try self.allocator.alloc(ColumnView, out_w + self.filter_phys.len);
        errdefer self.allocator.free(self.filter_eval_views);
        self.filter_decoded = try self.allocator.alloc(storage.OwnedColumn, self.filter_phys.len);
    }

    fn releaseFilterDecoded(self: *Scan) void {
        if (self.filter_decoded_valid) {
            for (self.filter_decoded) |*c| c.deinit(self.allocator);
            self.filter_decoded_valid = false;
        }
    }

    pub fn addPrune(self: *Scan, pred: Predicate) !void {
        const col_idx = blk: {
            for (self.table.schema.columns, 0..) |c, i| {
                if (@import("../types.zig").columnNameEql(c.name, pred.col)) break :blk i;
            }
            return Error.ColumnNotFound;
        };
        // Drop hints for types whose `Stats` slot is `{0, 0}` — no usable
        // min/max. statsOverlapPredicate would conservatively return true
        // anyway, but skipping the append avoids the per-row-group work.
        if (!storage.format.typeHasStats(self.table.schema.columns[col_idx].type)) return;

        // Cross-leaf blank exclusion: prune hints are exactly the top-level
        // AND conjuncts, so a sibling hint on the same column that rules out
        // `''` lets a string range hint compare against the blank-excluded
        // min. Flags flow both ways since hints arrive one at a time; an
        // upgraded earlier hint gets a second segment-pruning pass (e.g.
        // `URL < 'x' AND URL <> ''` — the range hint arrives first).
        var blanks_excluded = false;
        const new_excludes = predicate.leafExcludesBlank(pred.op, pred.val);
        for (self.prunes.items) |*h| {
            if (h.col_idx != col_idx) continue;
            if (predicate.leafExcludesBlank(h.op, h.val)) blanks_excluded = true;
            if (new_excludes and !h.blanks_excluded) {
                h.blanks_excluded = true;
                try self.segmentPrunePass(h.col_idx, h.op, h.val, true);
            }
        }

        try self.prunes.append(self.allocator, .{
            .col_idx = col_idx,
            .op = pred.op,
            .val = pred.val,
            .blanks_excluded = blanks_excluded,
        });

        try self.segmentPrunePass(col_idx, pred.op, pred.val, blanks_excluded);
    }

    /// Segment-level pruning for one hint: mark segments whose manifest stats
    /// can't match. The caller already proved this column's type has stats,
    /// so any manifest entry carrying per-column stats (v4+) has a valid slot
    /// at `col_idx`. Older manifests fall back to `leading_key_stats` when
    /// the predicate is on the leading order-key column.
    fn segmentPrunePass(self: *Scan, col_idx: usize, op: PredicateOp, val: Value, blanks_excluded: bool) !void {
        const order_key_cols = self.table.order_key_indices;
        const is_leading = order_key_cols.len > 0 and order_key_cols[0] == col_idx;
        const segs = self.table.manifest.segments.items[0..self.segment_count];
        var any_skipped = false;
        var skipped_buf: ?[]bool = self.seg_skip;
        for (segs, 0..) |entry, i| {
            const lk_opt: ?storage.format.Stats = blk: {
                if (entry.column_stats.len > col_idx) break :blk entry.column_stats[col_idx];
                if (is_leading) break :blk entry.leading_key_stats;
                break :blk null;
            };
            const lk = lk_opt orelse continue;
            if (!predicate.statsOverlapPredicateBlankAware(lk, op, val, blanks_excluded)) {
                if (skipped_buf == null) {
                    const buf = try self.allocator.alloc(bool, self.segment_count);
                    @memset(buf, false);
                    skipped_buf = buf;
                }
                skipped_buf.?[i] = true;
                any_skipped = true;
            }
        }
        if (any_skipped) self.seg_skip = skipped_buf;
    }

    pub fn rowGroupCanMatch(self: *const Scan, rg: storage.RowGroupMeta) bool {
        for (self.prunes.items) |hint| {
            const col_stats = rg.stats[hint.col_idx];
            if (!predicate.statsOverlapPredicateBlankAware(col_stats, hint.op, hint.val, hint.blanks_excluded)) return false;
        }
        for (self.in_prunes.items) |hint| {
            const col_stats = rg.stats[hint.col_idx];
            var any = false;
            for (hint.values) |v| {
                if (v >= col_stats.min and v <= col_stats.max) {
                    any = true;
                    break;
                }
            }
            if (!any) return false;
        }
        return true;
    }

    /// Work units this scan would actually decode: row groups surviving the
    /// installed prune hints, plus the memtable as 64K-row equivalents. The
    /// shared basis for selective-query worker right-sizing (ParallelScan,
    /// silo grid, lowcard handler). Footers come from the per-table
    /// segment-handle cache, so the warm cost is hash lookups. Null when the
    /// scan has no prune hints (no basis to size below full DOP) or a footer
    /// can't be read — callers keep their full worker count.
    pub fn survivingWorkUnits(self: *const Scan) ?usize {
        if (self.prunes.items.len == 0 and self.in_prunes.items.len == 0) return null;
        var surviving: usize = (@as(usize, @intCast(self.memtable_row_count)) + 65535) / 65536;
        for (self.table.manifest.segments.items[0..self.segment_count]) |entry| {
            const handle = self.table.acquireSegment(entry.segment_id) catch return null;
            defer self.table.releaseSegment(handle);
            for (handle.seg.info.row_groups) |rg| {
                if (self.rowGroupCanMatch(rg)) surviving += 1;
            }
        }
        return surviving;
    }

    pub fn outputSchema(self: *Scan) []const Column {
        return self.out_schema;
    }

    /// Pre-execution stats: sum of segment row counts + the memtable
    /// snapshot row count gives the exact upper bound.
    ///
    /// Sort state is the table's order key. `global` is true when the
    /// whole scan's output is guaranteed sorted by that key:
    ///   - zero or one segment AND the memtable snapshot is empty
    ///     (segments are always written sorted; the memtable is an
    ///     unordered append buffer that would emit as a trailing batch); OR
    ///   - multiple segments whose leading-key min/max (from the
    ///     manifest) form a strictly increasing run AND the memtable is
    ///     empty. For single-column order keys the check is weak
    ///     (`max[i] <= min[i+1]`); for multi-column order keys it's
    ///     strict (`max[i] < min[i+1]`) since equal boundary values
    ///     break secondary-key ordering across segments.
    pub fn stats(self: *Scan) exec.PipelineStats {
        const segs = self.table.manifest.segments.items[0..self.segment_count];
        var seg_rows: u64 = 0;
        for (segs) |s| seg_rows += s.row_count;

        const memtable_empty = self.memtable_row_count == 0;
        const global = memtable_empty and self.scanIsGloballySorted(segs);

        return .{
            .upper_rows = seg_rows + self.memtable_row_count,
            .sort_state = .{
                .keys = self.table.schema.order_key,
                .global = global,
            },
            .column_stats = self.cached_stats,
        };
    }

    fn scanIsGloballySorted(self: *Scan, segs: []const storage.ManifestEntry) bool {
        if (segs.len <= 1) return true;
        if (self.table.schema.order_key.len == 0) return false;

        // Need leading-key stats on every segment to prove non-overlap.
        // v1 manifests (or string/float/largeint/decimal128/uuid leading
        // keys) report null stats — we can't conclude anything then.
        //
        // Iterates in MANIFEST order (= the order Scan emits segments).
        // For the scan's output to be globally sorted, each segment's
        // leading-key min must be >= the previous segment's max — i.e.
        // segments are non-overlapping AND manifest order matches
        // leading-key order. The strict variant for multi-column order
        // keys avoids equal boundary values breaking secondary-key
        // ordering across the seam.
        const multi_key = self.table.schema.order_key.len > 1;
        var prev_max: i128 = std.math.minInt(i128);
        for (segs, 0..) |entry, i| {
            const lk = entry.leading_key_stats orelse return false;
            if (i > 0) {
                const overlap = if (multi_key)
                    lk.min <= prev_max
                else
                    lk.min < prev_max;
                if (overlap) return false;
            }
            prev_max = lk.max;
        }
        return true;
    }

    /// Reset + return the per-projection sidecar slots (all null). Lazily sized.
    fn ensureCodedSlots(self: *Scan) ![]?exec.CodedColumn {
        if (self.coded_slots.len != self.out_phys.len) {
            if (self.coded_slots.len > 0) self.allocator.free(self.coded_slots);
            self.coded_slots = try self.allocator.alloc(?exec.CodedColumn, self.out_phys.len);
        }
        for (self.coded_slots) |*s| s.* = null;
        return self.coded_slots;
    }

    /// Cheap empty-string placeholder for `values[code_col]` (a valid view for
    /// any non-code-aware consumer; the real values flow as codes via the
    /// sidecar). Offsets all-zero, no bytes.
    fn emptyStringColumn(self: *Scan, col_type: types.Type, n: u32) !storage.OwnedColumn {
        const offsets = try self.allocator.alloc(u32, @as(usize, n) + 1);
        errdefer self.allocator.free(offsets);
        @memset(offsets, 0);
        const bytes = try self.allocator.alloc(u8, 0);
        const sc = storage.OwnedStringColumn{ .offsets = offsets, .bytes = bytes };
        return switch (col_type) {
            .varchar => .{ .data = .{ .varchar = sc } },
            .string => .{ .data = .{ .string = sc } },
            .char => .{ .data = .{ .char = sc } },
            else => unreachable,
        };
    }

    /// Fill `code_buf` with global dict codes for the key column's current row
    /// group, and return the placeholder `OwnedColumn` for `values[code_col]`.
    /// Dict blocks translate local→global via a LUT (NO string expansion — the
    /// Phase 4.2 win); raw blocks decode + intern per row (a high-card column
    /// pays roughly what the group-table hash would have anyway).
    fn fillKeyCodes(self: *Scan, seg: *storage.ReadSegment, rg_idx: usize, phys: usize, rg_count: u32, j: usize) !storage.OwnedColumn {
        const gdict = self.coded_dicts_by_j[j].?;
        const col_type = self.table.schema.columns[phys].type;
        try self.code_bufs[j].resize(self.allocator, rg_count);
        const codes = self.code_bufs[j].items[0..rg_count];

        const flags = storage.format.ColumnBlockFlags{ .has_nulls = self.table.schema.columns[phys].nullable };
        var block = try seg.borrowColumnBlock(self.allocator, rg_idx, phys, &self.table.cache);
        defer block.release(self.allocator, &self.table.cache);

        if (block.encoding == .dict) {
            const _pt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
            defer if (exec.prof.enabled) exec.prof.add("dict-code (LUT+translate)", @intCast(@max(0, exec.prof.nowTicks() - _pt)));
            var values = block.bytes;
            if (flags.has_nulls) values = block.bytes[storage.column.bitmapBytes(rg_count)..];
            const db = storage.segment_reader.dictBlockOf(values, rg_count);
            const lut = try gdict.buildLut(self.allocator, db);
            defer self.allocator.free(lut);
            for (0..rg_count) |i| codes[i] = lut[db.rowCode(i)];
        } else if (block.encoding == .fsst) {
            // Per-row decode into a one-value scratch with the adjacent-run
            // shortcut — no full-column expansion, and runs hit the
            // spinlocked global dict once instead of per row.
            const _pt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
            defer if (exec.prof.enabled) exec.prof.add("dict-code (fsst run intern)", @intCast(@max(0, exec.prof.nowTicks() - _pt)));
            const fv = try storage.segment_reader.fsstViewOf(block.bytes, rg_count, flags);
            var scratch: std.ArrayListUnmanaged(u8) = .empty;
            defer scratch.deinit(self.allocator);
            var prev_comp: []const u8 = &.{};
            var prev_code: u32 = 0;
            for (0..rg_count) |i| {
                if (i + 8 < rg_count) @prefetch(fv.block.rowComp(i + 8).ptr, .{ .rw = .read, .locality = 2 });
                const comp = fv.block.rowComp(i);
                if (i == 0 or !std.mem.eql(u8, prev_comp, comp)) {
                    try scratch.resize(self.allocator, storage.fsst.decodedSizeBound(comp.len));
                    const n = fv.block.table.decodeIntoUnchecked(comp, scratch.items);
                    prev_code = try gdict.intern(self.allocator, scratch.items[0..n]);
                    prev_comp = comp;
                }
                codes[i] = prev_code;
            }
        } else {
            const _pt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
            defer if (exec.prof.enabled) exec.prof.add("dict-code (raw intern per-row)", @intCast(@max(0, exec.prof.nowTicks() - _pt)));
            var owned = try seg.decodeColumnMaybeCached(self.allocator, self.table.schema, rg_idx, phys, &self.table.cache);
            defer owned.deinit(self.allocator);
            const sv = switch (owned.data) {
                .varchar, .string, .char => |s| s.view(),
                else => unreachable,
            };
            for (0..rg_count) |i| codes[i] = try gdict.intern(self.allocator, sv.rowBytes(i));
        }
        return self.emptyStringColumn(col_type, rg_count);
    }

    /// Fill `hash_bufs[j]` with per-row key digests for one row group's string
    /// column, straight off the cached decompressed block: a dict block hashes
    /// each distinct value once and translates rows through the LUT; a raw
    /// block hashes rows through an in-place view (no decode copy). Returns
    /// the placeholder value column.
    fn fillKeyHashes(self: *Scan, seg: *storage.ReadSegment, rg_idx: usize, phys: usize, rg_count: u32, j: usize) !storage.OwnedColumn {
        const col_type = self.table.schema.columns[phys].type;
        try self.hash_bufs[j].resize(self.allocator, rg_count);
        const digests = self.hash_bufs[j].items[0..rg_count];

        const flags = storage.format.ColumnBlockFlags{ .has_nulls = self.table.schema.columns[phys].nullable };
        var block = try seg.borrowColumnBlock(self.allocator, rg_idx, phys, &self.table.cache);
        defer block.release(self.allocator, &self.table.cache);

        if (block.encoding == .dict) {
            var values = block.bytes;
            if (flags.has_nulls) values = block.bytes[storage.column.bitmapBytes(rg_count)..];
            const db = storage.segment_reader.dictBlockOf(values, rg_count);
            const lut = try self.allocator.alloc(u128, db.ndv);
            defer self.allocator.free(lut);
            for (lut, 0..) |*d, c| d.* = exec.stringKeyDigest(db.dictValue(@intCast(c)));
            for (0..rg_count) |i| digests[i] = lut[db.rowCode(i)];
        } else if (block.encoding == .fsst) {
            // Plaintext digests off the compressed block via a one-value
            // scratch, with an adjacent-run shortcut: equal compressed bytes
            // within one block mean equal plaintext (fixed symbol table,
            // deterministic encoder), so a repeat of the previous row reuses
            // its digest without decoding. (See hashSurvivorsFromBlock for
            // why the digest itself can't be over compressed bytes.)
            const _pt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
            const fv = try storage.segment_reader.fsstViewOf(block.bytes, rg_count, flags);
            var scratch: std.ArrayListUnmanaged(u8) = .empty;
            defer scratch.deinit(self.allocator);
            var decoded_bytes: u64 = 0;
            var prev_comp: []const u8 = &.{};
            var prev_val: u128 = 0;
            for (0..rg_count) |i| {
                if (i + 8 < rg_count) @prefetch(fv.block.rowComp(i + 8).ptr, .{ .rw = .read, .locality = 2 });
                const comp = fv.block.rowComp(i);
                if (i == 0 or !std.mem.eql(u8, prev_comp, comp)) {
                    try scratch.resize(self.allocator, storage.fsst.decodedSizeBound(comp.len));
                    const n = fv.block.table.decodeIntoUnchecked(comp, scratch.items);
                    decoded_bytes += n;
                    prev_val = exec.stringKeyDigest(scratch.items[0..n]);
                    prev_comp = comp;
                }
                digests[i] = prev_val;
            }
            if (exec.prof.enabled) {
                _ = g_fsst_digest_ticks.fetchAdd(@intCast(@max(0, exec.prof.nowTicks() - _pt)), .monotonic);
                _ = g_fsst_digest_rows.fetchAdd(rg_count, .monotonic);
                _ = g_fsst_digest_bytes.fetchAdd(decoded_bytes, .monotonic);
            }
        } else if (storage.segment_reader.viewRawColumn(col_type, block.bytes, rg_count, flags, block.encoding)) |view| {
            const sv = switch (view.data) {
                .varchar, .string, .char => |s| s,
                else => return error.TypeMismatch,
            };
            for (0..rg_count) |i| digests[i] = exec.stringKeyDigest(sv.rowBytes(i));
        } else {
            var owned = try seg.decodeColumnMaybeCached(self.allocator, self.table.schema, rg_idx, phys, &self.table.cache);
            defer owned.deinit(self.allocator);
            const sv = switch (owned.data) {
                .varchar, .string, .char => |s| s.view(),
                else => return error.TypeMismatch,
            };
            for (0..rg_count) |i| digests[i] = exec.stringKeyDigest(sv.rowBytes(i));
        }
        return self.emptyStringColumn(col_type, rg_count);
    }

    /// Emit the next sub-batch of the current (already-decoded) row group:
    /// slice each live `decoded` column to `[sub_off, sub_off+n)` into `views`
    /// and advance the cursor. Only the plain projection path uses this.
    fn emitSub(self: *Scan) Batch {
        const n = @min(self.sub_batch_rows, self.sub_count - self.sub_off);
        for (self.decoded[0..self.views.len], 0..) |c, i| self.views[i] = subView(c.view(), self.sub_off, n);
        self.sub_off += n;
        return Batch{ .schema = self.out_schema, .values = self.views, .row_count = @intCast(n) };
    }

    pub fn next(self: *Scan) !?Batch {
        if (self.fused_filter) |ff| {
            // Proven-empty predicate (e.g. an out-of-range equality folded to
            // `.always = false` by the Filter's stats simplification): no row
            // group or memtable row can match, so emit nothing instead of
            // scanning the whole table to filter every row out.
            if (ff == .always and !ff.always) return null;
            return self.nextFiltered();
        }

        // Scan sub-batch experiment: serve the next slice of the already-decoded
        // row group without releasing or re-decoding it.
        if (self.sub_off < self.sub_count) return self.emitSub();

        self.releaseBatch();

        // Count-only fast path: nothing references a column (e.g. a
        // WHERE-less COUNT(*)), so we never open or decode a segment. Emit
        // one row-count-only batch per segment straight from the manifest
        // (total rows minus tombstoned), then the memtable snapshot count.
        // A 0-column predicate set means no segment can be pruned, so every
        // segment contributes its full surviving count. (Late-mat scans
        // always project >=1 probe column, so `emit_loc` never reaches here.)
        if (self.out_phys.len == 0 and !self.emit_loc) {
            while (self.phase == .segments) {
                if (self.cur_seg_idx >= self.segment_count) {
                    self.phase = .memtable;
                    break;
                }
                const entry = self.table.manifest.segments.items[self.cur_seg_idx];
                self.cur_seg_idx += 1;
                const tombs = try storage.tombstone.read(self.allocator, self.io, self.table.segments_dir, entry.segment_id);
                defer if (tombs) |t| self.allocator.free(t);
                const dead: u64 = if (tombs) |t| t.len else 0;
                const surviving: u64 = if (entry.row_count > dead) entry.row_count - dead else 0;
                if (surviving == 0) continue;
                return Batch{ .schema = self.out_schema, .values = self.views, .row_count = @intCast(surviving) };
            }
            if (self.phase == .memtable) {
                self.phase = .done;
                if (!self.scan_memtable or self.memtable_row_count == 0) return null;
                return Batch{ .schema = self.out_schema, .values = self.views, .row_count = @intCast(self.memtable_row_count) };
            }
            return null;
        }

        // Segments phase
        while (self.phase == .segments) {
            if (self.cur_segment == null and !try self.openCurSegment()) break;

            const seg = self.cur_segment.?;
            // Reached this worker's assigned range end (possibly mid-segment) —
            // stop the segments phase and fall through to the memtable.
            if (self.atRangeEnd()) {
                self.closeCurSegment();
                self.phase = .memtable;
                continue;
            }
            if (self.cur_rg_idx >= seg.info.row_groups.len) {
                self.closeCurSegment();
                self.cur_seg_idx += 1;
                continue;
            }

            const rg = seg.info.row_groups[self.cur_rg_idx];
            self.rgs_considered += 1;
            if (!self.rowGroupCanMatch(rg)) {
                self.cur_rg_idx += 1;
                continue;
            }
            self.rgs_scanned += 1;
            self.rows_scanned += rg.row_count;

            const rg_count = rg.row_count;

            var decoded_cols: usize = 0;
            errdefer {
                for (self.decoded[0..decoded_cols]) |*c| c.deinit(self.allocator);
                // A later step (e.g. tombstone application) can fail after
                // `decoded_valid` is set; clear it so the deinit-time
                // `releaseBatch` doesn't free these columns a second time.
                self.decoded_valid = false;
            }
            // Phase 4.2: emit the gated key column as codes (sidecar) instead of
            // materialized strings. Disabled when the segment has tombstones (the
            // survivor compaction would desync the codes) or in late-mat mode.
            const coding = self.n_coded > 0 and self.cur_segment_tomb == null and !self.emit_loc;
            // Key-digest emit follows the same tombstone rule (the survivor
            // compaction would desync the sidecar) but is late-mat compatible:
            // the synthesized __rowloc column is exactly how the real key
            // bytes come back at emit.
            const hashing = self.n_hashed > 0 and self.cur_segment_tomb == null;
            for (self.out_phys, 0..) |phys, j| {
                if (coding and self.coded_dicts_by_j[j] != null) {
                    self.decoded[j] = try self.fillKeyCodes(seg, self.cur_rg_idx, phys, rg_count, j);
                } else if (hashing and self.hash_cols_by_j[j]) {
                    self.decoded[j] = try self.fillKeyHashes(seg, self.cur_rg_idx, phys, rg_count, j);
                } else {
                    self.decoded[j] = try seg.decodeColumnMaybeCached(
                        self.allocator,
                        self.table.schema,
                        self.cur_rg_idx,
                        phys,
                        &self.table.cache,
                    );
                }
                decoded_cols += 1;
            }

            // Late-mat: synthesize the trailing `__rowloc` column as a normal
            // decoded column. The local offset is the row's index within this
            // row group, so a survivor can later be re-fetched via
            // `decodeColumnMaybeCached(rg_idx, phys)[offset]`. Carrying it
            // through `applyTombsIfAny` keeps each surviving row's physical
            // location correct after tombstone compaction.
            if (self.emit_loc) {
                const locs = try self.allocator.alloc(i64, rg_count);
                for (locs, 0..) |*v, i| v.* = rowloc.packSegment(self.cur_seg_idx, self.cur_rg_idx, i);
                self.decoded[self.out_phys.len] = .{ .data = .{ .bigint = locs } };
                decoded_cols += 1;
            }
            self.decoded_valid = true;

            const rg_first = self.cur_rg_first_row[self.cur_rg_idx];
            self.cur_rg_idx += 1;

            // Apply tombstones if any fall within this row group.
            const masked = try self.applyTombsIfAny(rg_first, rg_count);
            if (masked) |out| return out;

            for (self.decoded, 0..) |c, i| self.views[i] = c.view();
            var sidecar: ?[]const ?exec.CodedColumn = null;
            if (coding) {
                const slots = try self.ensureCodedSlots();
                for (0..self.out_phys.len) |j| {
                    if (self.coded_dicts_by_j[j]) |d| {
                        slots[j] = .{ .codes = self.code_bufs[j].items[0..rg_count], .dict = d };
                    }
                }
                sidecar = slots;
            }
            var hash_sidecar: ?[]const ?[]const u128 = null;
            if (hashing) {
                const slots = try self.ensureHashedSlots();
                for (0..self.out_phys.len) |j| {
                    if (self.hash_cols_by_j[j]) slots[j] = self.hash_bufs[j].items[0..rg_count];
                }
                hash_sidecar = slots;
            }
            // Sub-batch experiment: emit the row group in `scan_sub_batch`-row
            // slices (plain projection only — not coded/hashed/late-mat, which
            // keep the full batch). The decoded buffers stay live until the
            // cursor exhausts and the next `next()` calls `releaseBatch`.
            // `emit_runs` also keeps the full row group: the run sidecar is
            // per-RG (a run-aware consumer does near-zero per-row work, so the
            // L2-residency slicing buys nothing there).
            if (self.sub_batch_rows > 0 and !coding and !hashing and !self.emit_loc and !self.emit_runs and rg_count > self.sub_batch_rows) {
                self.sub_off = 0;
                self.sub_count = rg_count;
                return self.emitSub();
            }
            var runs_sidecar: ?[]const ?exec.RunsColumn = null;
            if (self.emit_runs and self.cur_segment_tomb == null) {
                runs_sidecar = try self.fillRunsSidecar(seg, self.cur_rg_idx - 1, rg_count);
            }
            return Batch{
                .schema = self.out_schema,
                .values = self.views,
                .row_count = rg_count,
                .coded = sidecar,
                .hashed = hash_sidecar,
                .runs = runs_sidecar,
            };
        }

        // Memtable phase — read from the pinned snapshot, not the table's
        // live memtable. Bounded by `memtable_row_count` captured at scan
        // create time; rows appended after that are invisible to this scan.
        if (self.phase == .memtable) {
            self.phase = .done;
            if (!self.scan_memtable or self.memtable_row_count == 0) return null;

            for (self.out_phys, 0..) |phys, j| {
                self.views[j] = self.memtable_snap.columns[phys].view();
            }
            if (self.emit_loc) {
                const n: usize = @intCast(self.memtable_row_count);
                self.memtable_loc_buf = try self.allocator.alloc(i64, n);
                for (self.memtable_loc_buf, 0..) |*v, i| v.* = rowloc.packMemtable(i);
                self.views[self.out_phys.len] = .{ .data = .{ .bigint = self.memtable_loc_buf } };
            }
            return Batch{
                .schema = self.out_schema,
                .values = self.views,
                .row_count = @intCast(self.memtable_row_count),
            };
        }

        return null;
    }

    /// Advance past leading-key-excluded segments and open the next eligible
    /// one into `cur_segment` (+ tombstones, row-group prefix sums, cursor).
    /// Returns false (and transitions to the memtable phase) when no more
    /// segments remain. Shared by `next()` and `nextFiltered()`.
    fn openCurSegment(self: *Scan) !bool {
        while (self.cur_seg_idx < self.segment_count) : (self.cur_seg_idx += 1) {
            const skip = if (self.seg_skip) |s| s[self.cur_seg_idx] else false;
            if (!skip) break;
        }
        if (self.cur_seg_idx >= self.segment_count) {
            self.phase = .memtable;
            return false;
        }
        const entry = self.table.manifest.segments.items[self.cur_seg_idx];
        const handle = try self.table.acquireSegment(entry.segment_id);
        self.cur_seg_entry = handle;
        self.cur_segment = &handle.seg;
        self.segments_opened += 1;
        self.cur_segment_tomb = try self.table.segmentTombstones(self.allocator, handle);
        const rgs = self.cur_segment.?.info.row_groups;
        self.cur_rg_first_row = try self.allocator.alloc(u32, rgs.len);
        var running: u32 = 0;
        for (rgs, 0..) |rg, i| {
            self.cur_rg_first_row[i] = running;
            running += rg.row_count;
        }
        // Honor the assigned range's start row group on the first segment of a
        // parallel worker's span; every later segment starts at row group 0.
        self.cur_rg_idx = if (self.cur_seg_idx == self.range_start_seg) self.range_start_rg else 0;
        return true;
    }

    /// Scan-side in-place filter path. Walks segment row groups then the
    /// memtable, applying `fused_filter` directly to borrowed views over the
    /// pinned/decompressed block bytes and compacting only survivors into the
    /// owned `filtered` buffers. Returns one batch per row group that has at
    /// least one survivor; null at end of stream.
    ///
    /// Lifetime: within ONE call, the borrow (cache pins + typed views) is
    /// created in `tryBorrowViews`, used for predicate eval + compaction, and
    /// released before `filterRowGroup` returns. The emitted batch aliases only
    /// `self.filtered` / `self.views` (fully owned), exactly like the copy path
    /// — so downstream sees no borrowed cache memory.
    fn nextFiltered(self: *Scan) !?Batch {
        const expr = self.fused_filter.?;

        while (self.phase == .segments) {
            if (self.cur_segment == null and !try self.openCurSegment()) break;

            const seg = self.cur_segment.?;
            // Reached this worker's assigned range end (possibly mid-segment) —
            // stop the segments phase and fall through to the memtable.
            if (self.atRangeEnd()) {
                self.closeCurSegment();
                self.phase = .memtable;
                continue;
            }
            if (self.cur_rg_idx >= seg.info.row_groups.len) {
                self.closeCurSegment();
                self.cur_seg_idx += 1;
                continue;
            }

            const rg = seg.info.row_groups[self.cur_rg_idx];
            self.rgs_considered += 1;
            if (!self.rowGroupCanMatch(rg)) {
                self.cur_rg_idx += 1;
                continue;
            }
            self.rgs_scanned += 1;
            self.rows_scanned += rg.row_count;

            const rg_first = self.cur_rg_first_row[self.cur_rg_idx];
            const rg_count = rg.row_count;
            const this_rg = self.cur_rg_idx;
            self.cur_rg_idx += 1;

            const matched = try self.filterRowGroup(seg, this_rg, rg_first, rg_count, expr);
            if (matched == 0) continue;
            for (self.filtered.?, 0..) |c, i| self.views[i] = c.view();
            return Batch{ .schema = self.out_schema, .values = self.views, .row_count = matched, .coded = self.filtered_coded, .hashed = self.filtered_hashed };
        }

        if (self.phase == .memtable) {
            self.phase = .done;
            if (!self.scan_memtable or self.memtable_row_count == 0) return null;

            const n: usize = @intCast(self.memtable_row_count);
            const mem_views = try self.allocator.alloc(ColumnView, self.out_phys.len);
            defer self.allocator.free(mem_views);
            for (self.out_phys, 0..) |phys, j| mem_views[j] = self.memtable_snap.columns[phys].view();

            const matched = if (self.filter_phys.len == 0)
                try self.evalAndCompact(mem_views, self.out_schema, mem_views.len, n, null, expr, .memtable)
            else blk: {
                const oc = mem_views.len;
                @memcpy(self.filter_eval_views[0..oc], mem_views);
                for (self.filter_phys, 0..) |phys, j| self.filter_eval_views[oc + j] = self.memtable_snap.columns[phys].view();
                break :blk try self.evalAndCompact(self.filter_eval_views[0 .. oc + self.filter_phys.len], self.filter_eval_schema, oc, n, null, expr, .memtable);
            };
            if (matched == 0) return null;
            for (self.filtered.?, 0..) |c, i| self.views[i] = c.view();
            return Batch{ .schema = self.out_schema, .values = self.views, .row_count = matched, .coded = self.filtered_coded, .hashed = self.filtered_hashed };
        }

        return null;
    }

    /// Filter one segment row group into `filtered`. Fast path: borrow typed
    /// views directly over the pinned/decompressed cache blocks (no owned
    /// decode) when every projected column's bytes are alignment-safe to view;
    /// otherwise fall back to the owned-decode path. Both paths AND in the
    /// optional tombstone keep-mask before applying the predicate, so deleted
    /// rows never survive. All cache pins are released before returning.
    fn filterRowGroup(
        self: *Scan,
        seg: *storage.ReadSegment,
        rg_idx: usize,
        rg_first: u32,
        rg_count: u32,
        expr: PredicateExpr,
    ) !usize {
        if (try self.tryFusedLeafGather(seg, rg_idx, rg_count, expr)) |m| return m;
        const tomb_mask = try self.tombstoneMask(rg_first, rg_count);
        defer if (tomb_mask) |m| self.allocator.free(m);

        // FOR-aware fast path: a single comparison leaf, or an AND of comparison
        // leaves, evaluated column-by-column in the narrow code domain (FOR
        // blocks) or over a borrowed native view (raw blocks) — no full-column
        // expand, only survivors are materialized. Returns null when the shape
        // doesn't apply (non-comparison op, string/LIKE/sub-query leaf, an
        // un-projectable column), so the borrow / owned-decode paths below run
        // unchanged (and the borrow path is itself per-column FOR-aware now).
        if (try self.tryFilterForGuided(seg, rg_idx, rg_count, tomb_mask, expr)) |matched| {
            return matched;
        }

        // Fast path: borrow views over cache bytes. Bail to copy if any column
        // can't be viewed (misalignment / big-endian).
        if (try self.tryBorrowViews(seg, rg_idx, rg_count)) |borrow| {
            defer for (borrow.blocks) |*b| b.release(self.allocator, &self.table.cache);
            return self.evalAndCompactSegment(seg, rg_idx, rg_count, borrow.views, tomb_mask, expr, .{ .segment = rg_idx });
        }

        // Fallback: owned decode (as the non-fused path), then evaluate +
        // compact through the same kernel.
        const owned_views = try self.decodeOwnedViews(seg, rg_idx, rg_count);
        defer self.releaseDecoded();
        return self.evalAndCompactSegment(seg, rg_idx, rg_count, owned_views, tomb_mask, expr, .{ .segment = rg_idx });
    }

    /// Evaluate the fused predicate for one segment row group. When the filter
    /// references unprojected columns (`filter_phys`), owned-decode those from
    /// the same row group, present them to the evaluator alongside `out_views`,
    /// and release them after — they never reach the output.
    fn evalAndCompactSegment(self: *Scan, seg: *storage.ReadSegment, rg_idx: usize, rg_count: u32, out_views: []const ColumnView, tomb_mask: ?[]const bool, expr: PredicateExpr, loc: SurvivorLoc) !usize {
        if (self.filter_phys.len == 0) {
            return self.evalAndCompact(out_views, self.out_schema, out_views.len, rg_count, tomb_mask, expr, loc);
        }
        for (self.filter_phys, 0..) |phys, j| {
            self.filter_decoded[j] = try seg.decodeColumnMaybeCached(self.allocator, self.table.schema, rg_idx, phys, &self.table.cache);
        }
        self.filter_decoded_valid = true;
        defer self.releaseFilterDecoded();
        const oc = out_views.len;
        @memcpy(self.filter_eval_views[0..oc], out_views);
        for (self.filter_decoded, 0..) |c, j| self.filter_eval_views[oc + j] = c.view();
        return self.evalAndCompact(self.filter_eval_views[0 .. oc + self.filter_phys.len], self.filter_eval_schema, oc, rg_count, tomb_mask, expr, loc);
    }

    /// FOR-aware fused filter (Phase 2B + per-leaf AND generalization). Handles a
    /// single comparison `.leaf`, or an `.@"and"` whose children are ALL
    /// comparison leaves on numeric/temporal columns: each leaf is evaluated in
    /// the narrow FOR code domain when its block is FOR-encoded (no native
    /// expand), or over a borrowed native view when raw, and the per-leaf masks
    /// are ANDed into the survivor mask. Only survivors are then expanded
    /// (`materializeSurvivors`). Returns null — taking and releasing no extra
    /// state — when any leaf's shape doesn't fit (non-comparison op, string /
    /// LIKE / sub-query leaf, un-projectable column), so the caller's per-column
    /// borrow path (also FOR-aware) handles the row group instead.
    fn tryFilterForGuided(
        self: *Scan,
        seg: *storage.ReadSegment,
        rg_idx: usize,
        rg_count: u32,
        tomb_mask: ?[]const bool,
        expr: PredicateExpr,
    ) !?usize {
        const mask = try self.ensureMask(rg_count);
        switch (expr) {
            .leaf, .like, .not => {
                if (!try self.buildGuidedChild(seg, rg_idx, rg_count, expr, null, mask[0..rg_count])) return null;
            },
            .@"and" => |children| {
                if (children.len == 0) return null;
                // Every child must be a comparison leaf or a (NOT) LIKE we can
                // evaluate block-sourced; otherwise decline the whole AND
                // (mixed shapes fall back).
                for (children) |c| if (!guidedChildShape(c)) return null;

                if (!try self.buildGuidedChild(seg, rg_idx, rg_count, children[0], null, mask[0..rg_count])) return null;
                if (children.len > 1) {
                    const scratch = try self.ensureMask2(rg_count);
                    // Empty-mask early exit: conjuncts arrive selectivity-
                    // ordered, so the leading leaf often kills the whole row
                    // group — stop borrowing/evaluating the remaining columns'
                    // blocks the moment no row survives. Sound for an AND: the
                    // skipped leaves could only remove more rows.
                    var live = blk: {
                        for (mask[0..rg_count]) |m| {
                            if (m) break :blk true;
                        }
                        break :blk false;
                    };
                    for (children[1..]) |c| {
                        if (!live) break;
                        // Later children receive the accumulated mask: an
                        // expensive leaf (LIKE over an FSST block) then
                        // decodes/tests ONLY the rows still alive.
                        if (!try self.buildGuidedChild(seg, rg_idx, rg_count, c, mask[0..rg_count], scratch[0..rg_count])) return null;
                        var any = false;
                        for (mask[0..rg_count], scratch[0..rg_count]) |*m, s| {
                            m.* = m.* and s;
                            any = any or m.*;
                        }
                        live = any;
                    }
                }
            },
            else => return null,
        }

        if (tomb_mask) |tm| {
            for (mask[0..rg_count], tm) |*m, keep| m.* = m.* and keep;
        }

        const _tcnt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        var matched: usize = 0;
        for (mask[0..rg_count]) |m| matched += @intFromBool(m);
        if (exec.prof.enabled) exec.prof.add("scan.mask_count", @intCast(@max(0, exec.prof.nowTicks() - _tcnt)));
        if (matched == 0) return @as(usize, 0);

        const _tm = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        try self.materializeSurvivors(seg, rg_idx, rg_count, mask[0..rg_count]);
        if (exec.prof.enabled) exec.prof.add("scan.materialize_survivors", @intCast(@max(0, exec.prof.nowTicks() - _tm)));
        return matched;
    }

    /// Evaluate one comparison `leaf` for the current row group into `out`
    /// (sized `rg_count`), with NULLs cleared — tombstones are ANDed once by the
    /// caller. A FOR-encoded block compares the narrow codes in place; a raw
    /// block compares over a borrowed native view. Returns false (handling no
    /// pins) when the leaf can't be evaluated this way (non-comparison op,
    /// non-numeric column, an un-projectable column, or a block whose bytes
    /// can't be viewed raw), so the caller declines the FOR-aware path entirely.
    fn buildLeafMask(
        self: *Scan,
        seg: *storage.ReadSegment,
        rg_idx: usize,
        rg_count: u32,
        leaf: Predicate,
        out: []bool,
    ) !bool {
        switch (leaf.op) {
            .eq, .neq, .lt, .lte, .gt, .gte => {},
        }
        const pred_phys = blk: {
            for (self.table.schema.columns, 0..) |col, phys| {
                if (@import("../types.zig").columnNameEql(col.name, leaf.col)) break :blk phys;
            }
            return false;
        };
        const col_type = self.table.schema.columns[pred_phys].type;

        const flags = storage.format.ColumnBlockFlags{ .has_nulls = self.table.schema.columns[pred_phys].nullable };
        const _tb = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        var block = try seg.borrowColumnBlock(self.allocator, rg_idx, pred_phys, &self.table.cache);
        defer block.release(self.allocator, &self.table.cache);
        if (exec.prof.enabled) exec.prof.add("scan.leaf.borrow_block", @intCast(@max(0, exec.prof.nowTicks() - _tb)));

        // Dict-encoded string column: test the comparison against each distinct
        // dict value once into a matched-codes bitset, then map per row via the
        // narrow code — no expansion to strings. Only `col OP <text literal>`;
        // anything else (e.g. a coerced non-text value) declines to the general
        // path. (Phase 4.4 predicate pushdown.)
        if (block.encoding == .dict) {
            const lit: []const u8 = switch (leaf.val) {
                .text => |t| t,
                else => return false,
            };
            evalDictLeaf(block, rg_count, flags, leaf.op, lit, out);
            return true;
        }

        switch (col_type) {
            .varchar, .string, .char => {
                const lit: []const u8 = switch (leaf.val) {
                    .text => |t| t,
                    else => return false,
                };
                if (block.encoding == .fsst) {
                    return try evalFsstStringLeaf(self.allocator, block.bytes, rg_count, flags, leaf.op, lit, out);
                }
                if (block.encoding != .raw) return false;
                evalRawStringLeafBlock(block.bytes, rg_count, flags, leaf.op, lit, out);
                return true;
            },
            else => {},
        }

        if (!predicate.typeHasRange(col_type)) return false;

        if (block.encoding == .for_) {
            const fv = storage.segment_reader.forViewOf(block.bytes, rg_count, flags);
            // span = max - base (base == stats.min for a FOR block); resolves the
            // boundary (.none/.all) cases precisely.
            const col_stats = seg.info.row_groups[rg_idx].stats[pred_phys];
            if (col_stats.max < fv.block.base) return false; // degenerate all-null sentinel — shouldn't reach FOR
            const span: u128 = @intCast(col_stats.max - fv.block.base);
            const plan = predicate.translateForLeaf(fv.block.base, span, leaf.op, leaf.val) orelse return false;
            switch (plan) {
                .none => @memset(out, false),
                .all => @memset(out, true),
                .compare => |cp| storage.segment_reader.forCompareInto(fv.block, cmpOpToSimd(cp.op), cp.code, rg_count, out),
            }
            if (plan != .none and fv.nulls != null) {
                for (0..rg_count) |i| {
                    if (!storage.column.isValidBit(fv.nulls, i)) out[i] = false;
                }
            }
            return true;
        }

        if (block.encoding == .rle) {
            const want = predicate.valueToRangeI128(leaf.val) orelse return false;
            const rv = storage.segment_reader.rleViewOf(block.bytes, rg_count, flags);
            if (!rleCompareInto(rv.block, rg_count, leaf.op, want, out)) return false;
            if (rv.nulls != null) {
                for (0..rg_count) |i| {
                    if (!storage.column.isValidBit(rv.nulls, i)) out[i] = false;
                }
            }
            return true;
        }

        // Raw block: compare over a borrowed native view (clears NULLs). Decline
        // if the bytes can't be viewed in place (misaligned / big-endian) — the
        // caller's owned-decode fallback handles it.
        const view = storage.segment_reader.viewRawColumn(col_type, block.bytes, rg_count, flags, block.encoding) orelse return false;
        const _tc = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        try predicate.evaluateMaskWithPred(view, leaf, rg_count, out);
        if (exec.prof.enabled) exec.prof.add("scan.leaf.compare_raw", @intCast(@max(0, exec.prof.nowTicks() - _tc)));
        return true;
    }

    /// Evaluate `col =|<> literal` against an FSST-encoded string block in the
    /// COMPRESSED domain: encode the literal once with the block's symbol
    /// table (deterministic greedy encode ⇒ equal plaintext ⇔ equal
    /// compressed bytes under one table), then memcmp each row's compressed
    /// slice — no decode at all. The common `<> ''` shape degenerates to a
    /// per-row length check off the offsets. Ordering ops return false
    /// (compressed bytes don't preserve lex order) and fall back to the
    /// owned-decode path. NULL rows clear to false (2-valued logic, same as
    /// the raw/dict leaf paths).
    fn evalFsstStringLeaf(
        allocator: Allocator,
        raw: []const u8,
        rg_count: u32,
        flags: storage.format.ColumnBlockFlags,
        op: predicate.PredicateOp,
        literal: []const u8,
        out: []bool,
    ) !bool {
        const want_eq = switch (op) {
            .eq => true,
            .neq => false,
            else => return false,
        };
        const fv = try storage.segment_reader.fsstViewOf(raw, rg_count, flags);

        var lit_comp: std.ArrayListUnmanaged(u8) = .empty;
        defer lit_comp.deinit(allocator);
        try lit_comp.ensureTotalCapacity(allocator, storage.fsst.encodedSizeBound(literal.len));
        fv.block.table.encodeAppend(literal, &lit_comp);
        const want = lit_comp.items;

        for (out[0..rg_count], 0..) |*m, i| {
            const eq = std.mem.eql(u8, fv.block.rowComp(i), want);
            m.* = (eq == want_eq);
        }
        if (fv.nulls != null) {
            for (0..rg_count) |i| {
                if (!storage.column.isValidBit(fv.nulls, i)) out[i] = false;
            }
        }
        return true;
    }

    /// Evaluate `col OP literal` against a DICT-encoded string block by testing
    /// each distinct dict value once into a matched-codes bitset, then mapping
    /// per row via the narrow code — no expansion to strings. NULL rows (and
    /// rows whose code maps to a non-matching value) clear to false, matching
    /// the 2-valued logic the FOR/raw leaf paths use. Bounded by ndv (≤ 65536),
    /// so the bitset is at most 8 KiB and L1-resident; the dict is sorted, so a
    /// later refinement could binary-search a code range instead of scanning.
    fn evalDictLeaf(
        block: storage.ReadSegment.BorrowedBlock,
        rg_count: u32,
        flags: storage.format.ColumnBlockFlags,
        op: predicate.PredicateOp,
        literal: []const u8,
        out: []bool,
    ) void {
        const _pt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        defer if (exec.prof.enabled) exec.prof.add("dict-filter (cmp per-distinct)", @intCast(@max(0, exec.prof.nowTicks() - _pt)));

        var nulls: ?[]const u8 = null;
        var values = block.bytes;
        if (flags.has_nulls) {
            const bm_len = storage.column.bitmapBytes(rg_count);
            nulls = block.bytes[0..bm_len];
            values = block.bytes[bm_len..];
        }
        const db = storage.segment_reader.dictBlockOf(values, rg_count);

        var matched: [8192]u8 = undefined; // 65536 codes / 8
        const nbytes = (db.ndv + 7) / 8;
        @memset(matched[0..nbytes], 0);
        var c: u32 = 0;
        while (c < db.ndv) : (c += 1) {
            const v = db.dictValue(c);
            const hit = switch (op) {
                .eq => std.mem.eql(u8, v, literal),
                .neq => !std.mem.eql(u8, v, literal),
                .lt => std.mem.order(u8, v, literal) == .lt,
                .lte => std.mem.order(u8, v, literal) != .gt,
                .gt => std.mem.order(u8, v, literal) == .gt,
                .gte => std.mem.order(u8, v, literal) != .lt,
            };
            if (hit) matched[c >> 3] |= (@as(u8, 1) << @intCast(c & 7));
        }

        for (0..rg_count) |i| {
            if (storage.column.isValidBit(nulls, i)) {
                const code = db.rowCode(i);
                out[i] = (matched[code >> 3] & (@as(u8, 1) << @intCast(code & 7))) != 0;
            } else {
                out[i] = false;
            }
        }
    }

    fn cmpStringBytes(v: []const u8, literal: []const u8, op: predicate.PredicateOp) bool {
        return switch (op) {
            .eq => std.mem.eql(u8, v, literal),
            .neq => !std.mem.eql(u8, v, literal),
            .lt => std.mem.order(u8, v, literal) == .lt,
            .lte => std.mem.order(u8, v, literal) != .gt,
            .gt => std.mem.order(u8, v, literal) == .gt,
            .gte => std.mem.order(u8, v, literal) != .lt,
        };
    }

    fn evalRawStringLeafBlock(
        raw: []const u8,
        rg_count: u32,
        flags: storage.format.ColumnBlockFlags,
        op: predicate.PredicateOp,
        literal: []const u8,
        out: []bool,
    ) void {
        var nulls: ?[]const u8 = null;
        var values = raw;
        if (flags.has_nulls) {
            const bm_len = storage.column.bitmapBytes(rg_count);
            nulls = raw[0..bm_len];
            values = raw[bm_len..];
        }

        const off_start: usize = 4;
        const off_count = @as(usize, rg_count) + 1;
        const data_start = off_start + off_count * 4;
        const bytes = values[data_start..];

        if (literal.len == 0 and (op == .eq or op == .neq)) {
            const want_non_empty = op == .neq;
            var i: usize = 0;
            while (i < rg_count) : (i += 1) {
                if (!storage.column.isValidBit(nulls, i)) {
                    out[i] = false;
                    continue;
                }
                const a: usize = storage.format.readU32(values[off_start + i * 4 ..][0..4]);
                const b: usize = storage.format.readU32(values[off_start + (i + 1) * 4 ..][0..4]);
                out[i] = (b != a) == want_non_empty;
            }
            return;
        }

        var i: usize = 0;
        while (i < rg_count) : (i += 1) {
            if (!storage.column.isValidBit(nulls, i)) {
                out[i] = false;
                continue;
            }
            const a: usize = storage.format.readU32(values[off_start + i * 4 ..][0..4]);
            const b: usize = storage.format.readU32(values[off_start + (i + 1) * 4 ..][0..4]);
            out[i] = cmpStringBytes(bytes[a..b], literal, op);
        }
    }

    /// Dispatch one guided-filter child (a comparison `.leaf` or a `.like`) into
    /// `out`. Returns false (declining the whole guided path) for any other shape
    /// or any block the narrow path can't handle.
    /// True when `c` is a shape `buildGuidedChild` can evaluate block-sourced:
    /// a comparison leaf, a LIKE, or a NOT directly over a LIKE.
    fn guidedChildShape(c: PredicateExpr) bool {
        return switch (c) {
            .leaf, .like => true,
            .not => |inner| inner.* == .like,
            else => false,
        };
    }

    fn buildGuidedChild(
        self: *Scan,
        seg: *storage.ReadSegment,
        rg_idx: usize,
        rg_count: u32,
        child: PredicateExpr,
        active: ?[]const bool,
        out: []bool,
    ) !bool {
        return switch (child) {
            .leaf => |leaf| self.buildLeafMask(seg, rg_idx, rg_count, leaf, out),
            .like => |lp| self.buildLikeMask(seg, rg_idx, rg_count, lp, false, active, out),
            .not => |inner| switch (inner.*) {
                .like => |lp| self.buildLikeMask(seg, rg_idx, rg_count, lp, true, active, out),
                else => false,
            },
            else => false,
        };
    }

    /// Evaluate `col [NOT] LIKE pattern` for the current row group into `out`,
    /// block-sourced. Dict blocks match each distinct value once and map per
    /// row via the code; FSST blocks decode ONLY the rows still alive in
    /// `active` (per-survivor decode — the whole point of keeping the block
    /// compressed); raw blocks match over the zero-copy view. NULL rows clear
    /// to false under both LIKE and NOT LIKE (SQL: NULL never matches).
    /// Returns false (no pins held) for shapes it can't evaluate, declining
    /// the guided path.
    fn buildLikeMask(
        self: *Scan,
        seg: *storage.ReadSegment,
        rg_idx: usize,
        rg_count: u32,
        lp: predicate.LikePred,
        negate: bool,
        active: ?[]const bool,
        out: []bool,
    ) !bool {
        const pred_phys = blk: {
            for (self.table.schema.columns, 0..) |col, phys| {
                if (@import("../types.zig").columnNameEql(col.name, lp.col)) break :blk phys;
            }
            return false;
        };
        const col_type = self.table.schema.columns[pred_phys].type;
        switch (col_type) {
            .varchar, .string, .char => {},
            else => return false,
        }

        const flags = storage.format.ColumnBlockFlags{ .has_nulls = self.table.schema.columns[pred_phys].nullable };
        var block = try seg.borrowColumnBlock(self.allocator, rg_idx, pred_phys, &self.table.cache);
        defer block.release(self.allocator, &self.table.cache);

        if (block.encoding == .dict) {
            evalDictLike(block, rg_count, flags, lp.pattern, negate, out);
            return true;
        }
        if (block.encoding == .fsst) {
            try evalFsstLike(self.allocator, block.bytes, rg_count, flags, lp.pattern, negate, active, out);
            return true;
        }
        if (block.encoding == .raw) {
            const view = storage.segment_reader.viewRawColumn(col_type, block.bytes, rg_count, flags, block.encoding) orelse return false;
            const sv = switch (view.data) {
                .varchar, .string, .char => |s| s,
                else => return false,
            };
            const plan = predicate.compileLike(lp.pattern);
            for (0..rg_count) |i| {
                if (active) |a| {
                    if (!a[i]) {
                        out[i] = false;
                        continue;
                    }
                }
                out[i] = view.isValid(i) and (plan.match(sv.rowBytes(i)) != negate);
            }
            return true;
        }
        return false;
    }

    /// `col [NOT] LIKE pattern` over an FSST block: decode each ACTIVE row
    /// into a reused scratch and match the compiled plan — rows an earlier
    /// conjunct already eliminated never decode at all. This is the per-
    /// survivor decode that whole-block compression (LZ4-at-rest) cannot do.
    fn evalFsstLike(
        allocator: Allocator,
        raw: []const u8,
        rg_count: u32,
        flags: storage.format.ColumnBlockFlags,
        pattern: []const u8,
        negate: bool,
        active: ?[]const bool,
        out: []bool,
    ) !void {
        const _pt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        defer if (exec.prof.enabled) exec.prof.add("fsst-filter (LIKE per-survivor)", @intCast(@max(0, exec.prof.nowTicks() - _pt)));

        const fv = try storage.segment_reader.fsstViewOf(raw, rg_count, flags);
        const plan = predicate.compileLike(pattern);
        var scratch: std.ArrayListUnmanaged(u8) = .empty;
        defer scratch.deinit(allocator);
        for (0..rg_count) |i| {
            if (active) |a| {
                if (!a[i]) {
                    out[i] = false;
                    continue;
                }
            }
            if (!storage.column.isValidBit(fv.nulls, i)) {
                out[i] = false;
                continue;
            }
            const comp = fv.block.rowComp(i);
            try scratch.resize(allocator, storage.fsst.decodedSizeBound(comp.len));
            const n = fv.block.table.decodeIntoUnchecked(comp, scratch.items);
            out[i] = plan.match(scratch.items[0..n]) != negate;
        }
    }

    /// `col LIKE pattern` over a DICT-encoded block: compile the pattern once,
    /// match each distinct dict value into a matched-codes bitset, then map per
    /// row via the narrow code. NULL rows clear to false. Bounded by ndv.
    fn evalDictLike(
        block: storage.ReadSegment.BorrowedBlock,
        rg_count: u32,
        flags: storage.format.ColumnBlockFlags,
        pattern: []const u8,
        negate: bool,
        out: []bool,
    ) void {
        const _pt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        defer if (exec.prof.enabled) exec.prof.add("dict-filter (LIKE per-distinct)", @intCast(@max(0, exec.prof.nowTicks() - _pt)));

        var nulls: ?[]const u8 = null;
        var values = block.bytes;
        if (flags.has_nulls) {
            const bm_len = storage.column.bitmapBytes(rg_count);
            nulls = block.bytes[0..bm_len];
            values = block.bytes[bm_len..];
        }
        const db = storage.segment_reader.dictBlockOf(values, rg_count);

        const plan = predicate.compileLike(pattern);
        var matched: [8192]u8 = undefined;
        const nbytes = (db.ndv + 7) / 8;
        @memset(matched[0..nbytes], 0);
        var c: u32 = 0;
        while (c < db.ndv) : (c += 1) {
            if (plan.match(db.dictValue(c))) matched[c >> 3] |= (@as(u8, 1) << @intCast(c & 7));
        }

        for (0..rg_count) |i| {
            if (storage.column.isValidBit(nulls, i)) {
                const code = db.rowCode(i);
                const hit = (matched[code >> 3] & (@as(u8, 1) << @intCast(code & 7))) != 0;
                out[i] = hit != negate;
            } else {
                out[i] = false;
            }
        }
    }

    fn fcmpFor(comptime o: PredicateOp, v: i128, w: i128) bool {
        return switch (o) { .eq => v == w, .neq => v != w, .lt => v < w, .lte => v <= w, .gt => v > w, .gte => v >= w };
    }

    /// Run-aware analogue of `forCompareInto`: evaluate `value <op> want` ONCE
    /// per run and memset the run's mask range. Run values read signed at the
    /// block's native width (boolean's 0/1 reads identically as i8). Returns
    /// false on a width the writer never emits (decline rather than mis-filter);
    /// a corrupt short fill clears the tail rather than leaving stale bits.
    fn rleCompareInto(rb: storage.segment_reader.RleBlock, rg_count: u32, op: PredicateOp, want: i128, out: []bool) bool {
        var pos: usize = 0;
        switch (op) {
            inline else => |o| switch (rb.value_width) {
                inline 1, 2, 4, 8 => |W| {
                    const T = std.meta.Int(.signed, W * 8);
                    var run: usize = 0;
                    while (run < rb.n_runs and pos < rg_count) : (run += 1) {
                        const v: i128 = std.mem.readInt(T, rb.values[run * W ..][0..W], .little);
                        const len = @min(@as(usize, rb.runLength(run)), rg_count - pos);
                        @memset(out[pos .. pos + len], fcmpFor(o, v, want));
                        pos += len;
                    }
                },
                else => return false,
            },
        }
        if (pos < rg_count) @memset(out[pos..rg_count], false);
        return true;
    }

    /// Fused single-pass compare+gather: when the filter is a single comparison
    /// leaf whose ONLY projected column is that same (non-nullable, FOR-encoded)
    /// int column, decode + compare + branchlessly gather survivors in ONE pass —
    /// instead of the general path's two O(rows) passes (SIMD compare → bool mask,
    /// then a second borrow + gather re-reading the codes) plus the survivor count.
    /// Returns the survivor count, or null when the shape doesn't fit (caller runs
    /// the normal path). The common `WHERE k <op> c GROUP BY k` / `COUNT(*) WHERE
    /// k <op> c` shape.
    fn tryFusedLeafGather(self: *Scan, seg: *storage.ReadSegment, rg_idx: usize, rg_count: u32, expr: PredicateExpr) !?usize {
        if (self.n_coded != 0) return null;
        // Late-mat needs each survivor's `__rowloc`; this single-column gather
        // path doesn't produce the mask the loc emission rides on, so let the
        // mask-based paths (FOR-guided / borrow / owned) handle the row group.
        if (self.emit_loc) return null;
        if (self.out_phys.len != 1) return null;
        if (self.cur_segment_tomb) |t| {
            if (t.len != 0) return null;
        }
        const leaf = switch (expr) {
            .leaf => |l| l,
            else => return null,
        };
        const phys = self.out_phys[0];
        if (!@import("../types.zig").columnNameEql(self.table.schema.columns[phys].name, leaf.col)) return null;
        const col = self.table.schema.columns[phys];
        if (col.nullable) return null;
        switch (col.type) {
            .int, .date, .smallint, .tinyint, .bigint, .datetime, .decimal64 => {},
            else => return null, // ≤64-bit int family only (largeint/decimal128/float/string decline)
        }
        const want = predicate.valueToRangeI128(leaf.val) orelse return null;

        const flags = storage.format.ColumnBlockFlags{ .has_nulls = false };
        const _tb = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        var block = try seg.borrowColumnBlock(self.allocator, rg_idx, phys, &self.table.cache);
        defer block.release(self.allocator, &self.table.cache);
        if (exec.prof.enabled) self.scan_borrow_ticks +%= @intCast(@max(0, exec.prof.nowTicks() - _tb));
        const filtered = try self.ensureFilteredBuffers();
        const _tk = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        const matched: usize = switch (block.encoding) {
            .for_ => fr: {
                const fv = storage.segment_reader.forViewOf(block.bytes, rg_count, flags);
                if (fv.block.width != 1 and fv.block.width != 2 and fv.block.width != 4) return null;
                filtered[0].clear();
                break :fr try fusedGatherFor(self.allocator, &filtered[0], fv, rg_count, leaf.op, want);
            },
            // Raw int32/int64 only (point/range lookups on a full-range key like
            // UserID). Other raw types decline to the general path.
            .raw => rw: {
                if (col.type != .int and col.type != .bigint) return null;
                const view = storage.segment_reader.viewRawColumn(col.type, block.bytes, rg_count, flags, block.encoding) orelse return null;
                filtered[0].clear();
                break :rw (try fusedGatherRaw(self.allocator, &filtered[0], view, rg_count, leaf.op, want)) orelse return null;
            },
            .rle => rl: {
                const rv = storage.segment_reader.rleViewOf(block.bytes, rg_count, flags);
                const w = rv.block.value_width;
                if (w != 1 and w != 2 and w != 4 and w != 8) return null;
                filtered[0].clear();
                break :rl try fusedGatherRle(self.allocator, &filtered[0], rv.block, rg_count, leaf.op, want);
            },
            else => return null,
        };
        if (exec.prof.enabled) self.scan_kernel_ticks +%= @intCast(@max(0, exec.prof.nowTicks() - _tk));
        self.filtered_coded = null;
        self.filtered_hashed = null;
        return matched;
    }

    /// Raw int32/int64 sibling of `fusedGatherFor`: one pass comparing the native
    /// value + branchless gather (no mask array, no count pass). Switches on the
    /// destination's active tag for safety; returns null for any other type.
    fn fusedGatherRaw(allocator: Allocator, dst: *ColumnStore, view: ColumnView, rg_count: u32, op: PredicateOp, want: i128) !?usize {
        switch (dst.data) {
            .int => |*list| {
                if (view.data != .int) return null;
                return try fusedGatherSlice(i32, allocator, list, view.data.int, rg_count, op, want);
            },
            .bigint => |*list| {
                if (view.data != .bigint) return null;
                return try fusedGatherSlice(i64, allocator, list, view.data.bigint, rg_count, op, want);
            },
            else => return null,
        }
    }

    fn fusedGatherSlice(comptime T: type, allocator: Allocator, list: *std.ArrayList(T), src: []const T, rg_count: u32, op: PredicateOp, want: i128) !usize {
        try list.ensureUnusedCapacity(allocator, rg_count);
        list.items.len = rg_count;
        const out = list.items;
        var j: usize = 0;
        var i: usize = 0;
        // SIMD-screen each chunk; only scalar-gather chunks that have a match.
        // For a 0-survivor point lookup (e.g. UserID = const) this stays pure
        // vector compare + reduce — no per-row gather work, no mask array.
        const N = comptime (std.simd.suggestVectorLength(T) orelse 1);
        if (N > 1 and want >= std.math.minInt(T) and want <= std.math.maxInt(T)) {
            const V = @Vector(N, T);
            const wv: V = @splat(@as(T, @intCast(want)));
            switch (op) {
                inline else => |o| {
                    while (i + N <= rg_count) : (i += N) {
                        const v: V = src[i..][0..N].*;
                        const m: @Vector(N, bool) = switch (o) {
                            .eq => v == wv,
                            .neq => v != wv,
                            .lt => v < wv,
                            .lte => v <= wv,
                            .gt => v > wv,
                            .gte => v >= wv,
                        };
                        if (@reduce(.Or, m)) {
                            inline for (0..N) |k| {
                                out[j] = src[i + k];
                                j += @intFromBool(m[k]);
                            }
                        }
                    }
                },
            }
        }
        switch (op) {
            inline else => |o| {
                while (i < rg_count) : (i += 1) {
                    const x = src[i];
                    out[j] = x;
                    j += @intFromBool(fcmpFor(o, @as(i128, x), want));
                }
            },
        }
        list.items.len = j;
        return j;
    }

    fn fusedGatherFor(allocator: Allocator, dst: *ColumnStore, fv: storage.segment_reader.ForView, rg_count: u32, op: PredicateOp, want: i128) !usize {
        return switch (dst.data) {
            inline .int, .date, .smallint, .tinyint, .bigint, .datetime, .decimal64 => |*list| {
                try list.ensureUnusedCapacity(allocator, rg_count);
                list.items.len = rg_count;
                const out = list.items;
                const base: i128 = fv.block.base;
                const codes = fv.block.codes;
                var j: usize = 0;
                switch (op) {
                    inline else => |o| switch (fv.block.width) {
                        inline 1, 2, 4 => |W| {
                            const CW = std.meta.Int(.unsigned, W * 8);
                            var i: usize = 0;
                            // Width-1 SIMD screen for low-cardinality FOR columns:
                            // `base+code <op> want` is order-preserving, so it equals
                            // the code-space compare `code <op> (want-base)`. Vector-
                            // screen the bytes and only scalar-gather chunks with a
                            // match, so a selective filter stays mostly pure vector
                            // compare instead of 5M scalar decode+compare iterations.
                            if (W == 1) {
                                const wc: i128 = want - base;
                                if (wc >= 0 and wc <= std.math.maxInt(u8)) {
                                    const wantcode: u8 = @intCast(wc);
                                    const N = comptime (std.simd.suggestVectorLength(u8) orelse 1);
                                    if (N > 1) {
                                        const V = @Vector(N, u8);
                                        const wv: V = @splat(wantcode);
                                        while (i + N <= rg_count) : (i += N) {
                                            const cv: V = codes[i..][0..N].*;
                                            const m: @Vector(N, bool) = switch (o) {
                                                .eq => cv == wv,
                                                .neq => cv != wv,
                                                .lt => cv < wv,
                                                .lte => cv <= wv,
                                                .gt => cv > wv,
                                                .gte => cv >= wv,
                                            };
                                            if (@reduce(.Or, m)) {
                                                const ma: [N]bool = m;
                                                var k: usize = 0;
                                                while (k < N) : (k += 1) {
                                                    out[j] = @intCast(base +% @as(i128, codes[i + k]));
                                                    j += @intFromBool(ma[k]);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            while (i < rg_count) : (i += 1) {
                                const code = std.mem.readInt(CW, codes[i * W ..][0..W], .little);
                                const v: i128 = base +% @as(i128, code);
                                out[j] = @intCast(v);
                                j += @intFromBool(fcmpFor(o, v, want));
                            }
                        },
                        else => unreachable,
                    },
                }
                list.items.len = j;
                return j;
            },
            else => unreachable,
        };
    }

    /// RLE sibling of `fusedGatherFor`: ONE compare per run, then a memset of
    /// the run's value for matching runs — a selective filter never touches row
    /// width at all, and a long matching run materializes as a single memset.
    fn fusedGatherRle(allocator: Allocator, dst: *ColumnStore, rb: storage.segment_reader.RleBlock, rg_count: u32, op: PredicateOp, want: i128) !usize {
        return switch (dst.data) {
            inline .int, .date, .smallint, .tinyint, .bigint, .datetime, .decimal64 => |*list| {
                try list.ensureUnusedCapacity(allocator, rg_count);
                list.items.len = rg_count;
                const out = list.items;
                var j: usize = 0;
                var pos: usize = 0;
                switch (op) {
                    inline else => |o| switch (rb.value_width) {
                        inline 1, 2, 4, 8 => |W| {
                            const RT = std.meta.Int(.signed, W * 8);
                            var run: usize = 0;
                            while (run < rb.n_runs and pos < rg_count) : (run += 1) {
                                const v: i128 = std.mem.readInt(RT, rb.values[run * W ..][0..W], .little);
                                const len = @min(@as(usize, rb.runLength(run)), rg_count - pos);
                                if (fcmpFor(o, v, want)) {
                                    const tv: std.meta.Child(@TypeOf(out)) = @intCast(v);
                                    @memset(out[j .. j + len], tv);
                                    j += len;
                                }
                                pos += len;
                            }
                        },
                        else => unreachable, // caller pre-checks the width
                    },
                }
                list.items.len = j;
                return j;
            },
            else => unreachable,
        };
    }

    /// Compact the masked survivors of every projected column into `filtered`.
    /// Each column is borrowed from the cache; a FOR-encoded block expands only
    /// its survivors to native (`forExpandSurvivors`), a raw block is viewed in
    /// place and run through the shared masked-compaction. The borrow pins are
    /// released before returning.
    fn materializeSurvivors(self: *Scan, seg: *storage.ReadSegment, rg_idx: usize, rg_count: u32, mask: []const bool) !void {
        const filtered_cols = try self.ensureFilteredBuffers();
        for (filtered_cols) |*c| c.clear();

        const coding = self.n_coded > 0;
        const slots: ?[]?exec.CodedColumn = if (coding) try self.ensureCodedSlots() else null;
        const hashing = self.n_hashed > 0;
        const hash_slots: ?[]?[]const u128 = if (hashing) try self.ensureHashedSlots() else null;
        var matched: usize = 0;
        if (coding or hashing) {
            for (mask) |m| matched += @intFromBool(m);
        }

        for (self.out_phys, 0..) |phys, j| {
            const col_type = self.table.schema.columns[phys].type;
            const flags = storage.format.ColumnBlockFlags{ .has_nulls = self.table.schema.columns[phys].nullable };

            // Coded group key: translate only the survivors' codes into the code
            // buffer (dict block → LUT, raw block → intern), never expanding the
            // full block to strings. `values[j]` gets an empty-string placeholder
            // (the aggregate reads the sidecar codes, not the value column).
            if (coding and self.coded_dicts_by_j[j] != null) {
                try self.codeSurvivorsFromBlock(seg, rg_idx, phys, j, rg_count, flags, mask, matched);
                try self.fillEmptyStrings(&filtered_cols[j], matched);
                slots.?[j] = .{ .codes = self.code_bufs[j].items[0..matched], .dict = self.coded_dicts_by_j[j].? };
                continue;
            }

            // Hashed group key: digest only the survivors, same no-expansion
            // rule as the coded path.
            if (hashing and self.hash_cols_by_j[j]) {
                try self.hashSurvivorsFromBlock(seg, rg_idx, phys, j, rg_count, flags, mask, matched);
                try self.fillEmptyStrings(&filtered_cols[j], matched);
                hash_slots.?[j] = self.hash_bufs[j].items[0..matched];
                continue;
            }

            var block = try seg.borrowColumnBlock(self.allocator, rg_idx, phys, &self.table.cache);
            defer block.release(self.allocator, &self.table.cache);

            if (block.encoding == .for_) {
                const fv = storage.segment_reader.forViewOf(block.bytes, rg_count, flags);
                try self.appendForSurvivors(fv, mask, &filtered_cols[j]);
                continue;
            }

            if (block.encoding == .rle) {
                const rv = storage.segment_reader.rleViewOf(block.bytes, rg_count, flags);
                try self.appendRleSurvivors(rv, mask, &filtered_cols[j]);
                continue;
            }

            if (block.encoding == .fsst) {
                const fv = try storage.segment_reader.fsstViewOf(block.bytes, rg_count, flags);
                try self.appendFsstSurvivors(fv, mask, &filtered_cols[j]);
                continue;
            }

            // Raw block: borrow a native view in place when possible, else fall
            // back to an owned decode just for this column. Both feed the shared
            // masked-compaction.
            if (storage.segment_reader.viewRawColumn(col_type, block.bytes, rg_count, flags, block.encoding)) |view| {
                try engine.memtable.appendMaskedColumn(self.allocator, view, mask, &filtered_cols[j]);
            } else {
                var owned = try seg.decodeColumnMaybeCached(self.allocator, self.table.schema, rg_idx, phys, &self.table.cache);
                defer owned.deinit(self.allocator);
                try engine.memtable.appendMaskedColumn(self.allocator, owned.view(), mask, &filtered_cols[j]);
            }
        }
        if (self.emit_loc) try self.appendSurvivorLocs(mask, rg_idx);
        self.filtered_coded = slots;
        self.filtered_hashed = hash_slots;
    }

    /// Late-mat: append each survivor's packed `__rowloc` into the trailing
    /// filtered buffer (a bigint `ColumnStore`), in the same mask order the
    /// projected columns were compacted — so loc[k] matches survivor row k.
    /// `rg_idx` non-null → segment row group (pack seg/rg/in-group-offset);
    /// null → memtable (pack the row index). The local offset `i` is exactly
    /// what `LateScan` unpacks to re-fetch the survivor's wide columns.
    fn appendSurvivorLocs(self: *Scan, mask: []const bool, rg_idx: ?usize) !void {
        const loc_col = &self.filtered.?[self.out_phys.len];
        for (mask, 0..) |m, i| {
            if (!m) continue;
            const loc: i64 = if (rg_idx) |rg|
                rowloc.packSegment(self.cur_seg_idx, rg, i)
            else
                rowloc.packMemtable(i);
            try loc_col.data.bigint.append(self.allocator, loc);
        }
    }

    /// Fill `code_bufs[j]` with the global dict codes of just the surviving rows
    /// (`mask`) of one row group's column — dict blocks LUT-translate the local
    /// codes, raw blocks decode + intern per survivor. No full-block string
    /// expansion. `matched` is the survivor count.
    fn codeSurvivorsFromBlock(
        self: *Scan,
        seg: *storage.ReadSegment,
        rg_idx: usize,
        phys: usize,
        j: usize,
        rg_count: u32,
        flags: storage.format.ColumnBlockFlags,
        mask: []const bool,
        matched: usize,
    ) !void {
        const gdict = self.coded_dicts_by_j[j].?;
        try self.code_bufs[j].resize(self.allocator, matched);
        const codes = self.code_bufs[j].items[0..matched];

        var block = try seg.borrowColumnBlock(self.allocator, rg_idx, phys, &self.table.cache);
        defer block.release(self.allocator, &self.table.cache);

        if (block.encoding == .dict) {
            var values = block.bytes;
            if (flags.has_nulls) values = block.bytes[storage.column.bitmapBytes(rg_count)..];
            const db = storage.segment_reader.dictBlockOf(values, rg_count);
            const lut = try gdict.buildLut(self.allocator, db);
            defer self.allocator.free(lut);
            var k: usize = 0;
            for (mask, 0..) |m, row| if (m) {
                codes[k] = lut[db.rowCode(row)];
                k += 1;
            };
        } else {
            var owned = try seg.decodeColumnMaybeCached(self.allocator, self.table.schema, rg_idx, phys, &self.table.cache);
            defer owned.deinit(self.allocator);
            const sv = switch (owned.data) {
                .varchar, .string, .char => |s| s.view(),
                else => unreachable,
            };
            var k: usize = 0;
            for (mask, 0..) |m, row| if (m) {
                codes[k] = try gdict.intern(self.allocator, sv.rowBytes(row));
                k += 1;
            };
        }
    }

    /// Survivor-granular sibling of `fillKeyHashes`: digest just the masked
    /// rows of one row group's string column — dict block via a per-distinct
    /// digest LUT, raw block via the in-place view, owned decode as fallback.
    fn hashSurvivorsFromBlock(
        self: *Scan,
        seg: *storage.ReadSegment,
        rg_idx: usize,
        phys: usize,
        j: usize,
        rg_count: u32,
        flags: storage.format.ColumnBlockFlags,
        mask: []const bool,
        matched: usize,
    ) !void {
        const col_type = self.table.schema.columns[phys].type;
        try self.hash_bufs[j].resize(self.allocator, matched);
        const digests = self.hash_bufs[j].items[0..matched];

        var block = try seg.borrowColumnBlock(self.allocator, rg_idx, phys, &self.table.cache);
        defer block.release(self.allocator, &self.table.cache);

        if (block.encoding == .dict) {
            var values = block.bytes;
            if (flags.has_nulls) values = block.bytes[storage.column.bitmapBytes(rg_count)..];
            const db = storage.segment_reader.dictBlockOf(values, rg_count);
            const lut = try self.allocator.alloc(u128, db.ndv);
            defer self.allocator.free(lut);
            for (lut, 0..) |*d, c| d.* = exec.stringKeyDigest(db.dictValue(@intCast(c)));
            var k: usize = 0;
            for (mask, 0..) |m, row| if (m) {
                digests[k] = lut[db.rowCode(row)];
                k += 1;
            };
        } else if (block.encoding == .fsst) {
            // Digest straight off the compressed rows: decode each survivor
            // into a one-value scratch — no full-column expansion, no offsets
            // rebuild — with the adjacent-run shortcut (equal compressed
            // bytes within one block ⇔ equal plaintext) reusing the previous
            // survivor's digest. The digest is over PLAINTEXT bytes (it must
            // be a cross-segment/memtable identity; compressed bytes are only
            // comparable within one block's table).
            const fv = try storage.segment_reader.fsstViewOf(block.bytes, rg_count, flags);
            var scratch: std.ArrayListUnmanaged(u8) = .empty;
            defer scratch.deinit(self.allocator);
            var prev_comp: []const u8 = &.{};
            var prev_val: u128 = 0;
            var k: usize = 0;
            for (mask, 0..) |m, row| if (m) {
                const comp = fv.block.rowComp(row);
                if (k == 0 or !std.mem.eql(u8, prev_comp, comp)) {
                    try scratch.resize(self.allocator, storage.fsst.decodedSizeBound(comp.len));
                    const n = fv.block.table.decodeIntoUnchecked(comp, scratch.items);
                    prev_val = exec.stringKeyDigest(scratch.items[0..n]);
                    prev_comp = comp;
                }
                digests[k] = prev_val;
                k += 1;
            };
        } else if (storage.segment_reader.viewRawColumn(col_type, block.bytes, rg_count, flags, block.encoding)) |view| {
            const sv = switch (view.data) {
                .varchar, .string, .char => |s| s,
                else => return error.TypeMismatch,
            };
            var k: usize = 0;
            for (mask, 0..) |m, row| if (m) {
                digests[k] = exec.stringKeyDigest(sv.rowBytes(row));
                k += 1;
            };
        } else {
            var owned = try seg.decodeColumnMaybeCached(self.allocator, self.table.schema, rg_idx, phys, &self.table.cache);
            defer owned.deinit(self.allocator);
            const sv = switch (owned.data) {
                .varchar, .string, .char => |s| s.view(),
                else => return error.TypeMismatch,
            };
            var k: usize = 0;
            for (mask, 0..) |m, row| if (m) {
                digests[k] = exec.stringKeyDigest(sv.rowBytes(row));
                k += 1;
            };
        }
    }

    /// Append `n` empty-string rows to `out` — the placeholder value column for a
    /// coded group key (downstream reads the sidecar codes, not this column).
    fn fillEmptyStrings(self: *Scan, out: *ColumnStore, n: usize) !void {
        switch (out.data) {
            .varchar, .string, .char => |*ss| {
                var i: usize = 0;
                while (i < n) : (i += 1) try ss.appendValue(self.allocator, "");
            },
            else => unreachable,
        }
    }

    /// Expand a FOR column's survivors (`base + code` for set mask bits) into a
    /// `ColumnStore`. Only survivors are reconstructed — `forExpandSurvivors`
    /// writes them compactly into a scratch native slice, which is appended to
    /// the destination list. The per-survivor validity bit is carried so a NULL
    /// row that happens to survive (not the current single-leaf shape, which
    /// already cleared NULLs, but kept correct for any future composed mask)
    /// materializes as NULL.
    fn appendForSurvivors(self: *Scan, fv: storage.segment_reader.ForView, mask: []const bool, out: *ColumnStore) !void {
        var matched: usize = 0;
        for (mask) |m| matched += @intFromBool(m);

        switch (out.data) {
            .int => |*list| try self.expandForInto(i32, fv.block, mask, matched, list),
            .date => |*list| try self.expandForInto(i32, fv.block, mask, matched, list),
            .bigint => |*list| try self.expandForInto(i64, fv.block, mask, matched, list),
            .datetime => |*list| try self.expandForInto(i64, fv.block, mask, matched, list),
            .decimal64 => |*list| try self.expandForInto(i64, fv.block, mask, matched, list),
            .smallint => |*list| try self.expandForInto(i16, fv.block, mask, matched, list),
            .tinyint => |*list| try self.expandForInto(i8, fv.block, mask, matched, list),
            .boolean => |*list| try self.expandForInto(u8, fv.block, mask, matched, list),
            else => unreachable,
        }

        if (out.nulls != null) {
            var j: usize = 0;
            for (mask, 0..) |m, src_row| {
                if (!m) continue;
                const valid = storage.column.isValidBit(fv.nulls, src_row);
                try out.appendValidBit(self.allocator, j, valid);
                j += 1;
            }
        }
    }

    fn expandForInto(
        self: *Scan,
        comptime T: type,
        fb: storage.segment_reader.ForBlock,
        mask: []const bool,
        matched: usize,
        list: *std.ArrayList(T),
    ) !void {
        if (matched == 0) return;
        try list.ensureUnusedCapacity(self.allocator, matched);
        const start = list.items.len;
        list.items.len = start + matched;
        storage.segment_reader.forExpandSurvivors(T, fb, mask, list.items[start..]);
    }

    /// RLE sibling of `appendForSurvivors`: read each run's value once and walk
    /// only its mask range — never the full row-width expansion
    /// `decodeRleColumn` pays.
    fn appendRleSurvivors(self: *Scan, rv: storage.segment_reader.RleView, mask: []const bool, out: *ColumnStore) !void {
        var matched: usize = 0;
        for (mask) |m| matched += @intFromBool(m);

        switch (out.data) {
            .int => |*list| try self.expandRleInto(i32, rv.block, mask, matched, list),
            .date => |*list| try self.expandRleInto(i32, rv.block, mask, matched, list),
            .bigint => |*list| try self.expandRleInto(i64, rv.block, mask, matched, list),
            .datetime => |*list| try self.expandRleInto(i64, rv.block, mask, matched, list),
            .decimal64 => |*list| try self.expandRleInto(i64, rv.block, mask, matched, list),
            .smallint => |*list| try self.expandRleInto(i16, rv.block, mask, matched, list),
            .tinyint => |*list| try self.expandRleInto(i8, rv.block, mask, matched, list),
            .boolean => |*list| try self.expandRleInto(u8, rv.block, mask, matched, list),
            else => unreachable,
        }

        if (out.nulls != null) {
            var j: usize = 0;
            for (mask, 0..) |m, src_row| {
                if (!m) continue;
                const valid = storage.column.isValidBit(rv.nulls, src_row);
                try out.appendValidBit(self.allocator, j, valid);
                j += 1;
            }
        }
    }

    /// Survivor-only FSST expansion: decode just the masked rows off the
    /// cached compressed block — the full row-group string column never
    /// materializes. The scratch holds one decoded value at a time.
    fn appendFsstSurvivors(self: *Scan, fv: storage.segment_reader.FsstView, mask: []const bool, out: *ColumnStore) !void {
        var scratch: std.ArrayListUnmanaged(u8) = .empty;
        defer scratch.deinit(self.allocator);
        switch (out.data) {
            .varchar, .string, .char => |*ss| {
                for (mask, 0..) |m, row| {
                    if (!m) continue;
                    const comp = fv.block.rowComp(row);
                    try scratch.resize(self.allocator, storage.fsst.decodedSizeBound(comp.len));
                    const n = fv.block.table.decodeIntoUnchecked(comp, scratch.items);
                    try ss.appendValue(self.allocator, scratch.items[0..n]);
                }
            },
            else => unreachable, // the writer only FSST-encodes string-family columns
        }

        if (out.nulls != null) {
            var j: usize = 0;
            for (mask, 0..) |m, src_row| {
                if (!m) continue;
                const valid = storage.column.isValidBit(fv.nulls, src_row);
                try out.appendValidBit(self.allocator, j, valid);
                j += 1;
            }
        }
    }

    fn expandRleInto(
        self: *Scan,
        comptime T: type,
        rb: storage.segment_reader.RleBlock,
        mask: []const bool,
        matched: usize,
        list: *std.ArrayList(T),
    ) !void {
        if (matched == 0) return;
        try list.ensureUnusedCapacity(self.allocator, matched);
        const start = list.items.len;
        list.items.len = start + matched;
        const out = list.items[start..];
        var j: usize = 0;
        var pos: usize = 0;
        var run: usize = 0;
        while (run < rb.n_runs and pos < mask.len) : (run += 1) {
            const v = std.mem.readInt(T, rb.values[run * @sizeOf(T) ..][0..@sizeOf(T)], .little);
            const len = @min(@as(usize, rb.runLength(run)), mask.len - pos);
            for (mask[pos .. pos + len]) |m| {
                if (m) {
                    out[j] = v;
                    j += 1;
                }
            }
            pos += len;
        }
    }

    const Borrow = struct {
        blocks: []storage.ReadSegment.BorrowedBlock,
        views: []ColumnView,
    };

    /// Try to build borrowed typed views over the cache blocks for every
    /// projected column. Per-column: a raw block views zero-copy in place; a
    /// FOR-encoded block is expanded ONCE into an owned native buffer carried by
    /// its `BorrowedBlock` (so one FOR column never forces the raw columns onto
    /// the owned-decode path). Returns null (after releasing any pins taken)
    /// only when a column's bytes are neither viewable raw nor FOR (misaligned /
    /// big-endian raw). The returned `blocks` / `views` alias `self`-owned
    /// scratch (`borrow_blocks` / `views`); the caller must release the blocks
    /// within the same `next()` call (which frees any FOR expansion buffers).
    fn tryBorrowViews(self: *Scan, seg: *storage.ReadSegment, rg_idx: usize, rg_count: u32) !?Borrow {
        const blocks = try self.ensureBorrowBlocks();

        var got: usize = 0;
        errdefer for (blocks[0..got]) |*b| b.release(self.allocator, &self.table.cache);

        for (self.out_phys, 0..) |phys, j| {
            const col_type = self.table.schema.columns[phys].type;
            const flags = storage.format.ColumnBlockFlags{ .has_nulls = self.table.schema.columns[phys].nullable };
            var block = try seg.borrowColumnBlock(self.allocator, rg_idx, phys, &self.table.cache);

            if (block.encoding == .fsst) {
                // FSST: expand once into the cache's recycled scratch pool
                // (returned on `block.release`) — a fresh allocation per
                // borrow re-faults zeroed pages every scan `next()`.
                const view = storage.segment_reader.expandFsstPooled(&block, &self.table.cache, col_type, rg_count, flags) catch |e| {
                    block.release(self.allocator, &self.table.cache);
                    for (blocks[0..got]) |*b| b.release(self.allocator, &self.table.cache);
                    return e;
                };
                blocks[j] = block;
                self.views[j] = view;
                got += 1;
                continue;
            }

            if (block.encoding != .raw) {
                // Narrow-encoded (FOR or dict): expand the codes once into a
                // native buffer owned by the block; the view aliases that buffer
                // and the expansion is freed on `block.release`. The raw columns
                // alongside stay zero-copy — one narrow column never forces the
                // whole row group onto the owned-decode path.
                block.expanded = storage.segment_reader.decodeColumnPayload(
                    self.allocator,
                    col_type,
                    block.bytes,
                    rg_count,
                    flags,
                    block.encoding,
                ) catch |e| {
                    block.release(self.allocator, &self.table.cache);
                    for (blocks[0..got]) |*b| b.release(self.allocator, &self.table.cache);
                    return e;
                };
                blocks[j] = block;
                self.views[j] = block.expanded.?.view();
                got += 1;
                continue;
            }

            const view = storage.segment_reader.viewRawColumn(col_type, block.bytes, rg_count, flags, block.encoding) orelse {
                // Misaligned / big-endian raw: release this block and abandon the
                // fast path for the whole row group (release the rest too).
                block.release(self.allocator, &self.table.cache);
                for (blocks[0..got]) |*b| b.release(self.allocator, &self.table.cache);
                return null;
            };
            blocks[j] = block;
            self.views[j] = view;
            got += 1;
        }
        return .{ .blocks = blocks[0..self.out_phys.len], .views = self.views[0..self.out_phys.len] };
    }

    /// Owned-decode the projected columns of one row group into `self.decoded`
    /// and return their views. Sets `decoded_valid`; release via
    /// `releaseDecoded`.
    fn decodeOwnedViews(self: *Scan, seg: *storage.ReadSegment, rg_idx: usize, _: u32) ![]ColumnView {
        var decoded_cols: usize = 0;
        errdefer {
            for (self.decoded[0..decoded_cols]) |*c| c.deinit(self.allocator);
            self.decoded_valid = false;
        }
        for (self.out_phys, 0..) |phys, j| {
            self.decoded[j] = try seg.decodeColumnMaybeCached(
                self.allocator,
                self.table.schema,
                rg_idx,
                phys,
                &self.table.cache,
            );
            decoded_cols += 1;
        }
        self.decoded_valid = true;
        for (self.decoded[0..self.out_phys.len], 0..) |c, i| self.views[i] = c.view();
        return self.views[0..self.out_phys.len];
    }

    fn releaseDecoded(self: *Scan) void {
        if (self.decoded_valid) {
            for (self.decoded[0..self.out_phys.len]) |*c| c.deinit(self.allocator);
            self.decoded_valid = false;
        }
    }

    /// Where a fused late-mat survivor's `__rowloc` comes from — the compaction
    /// packs it from the same mask. Segment row group (by index) vs memtable.
    const SurvivorLoc = union(enum) { segment: usize, memtable };

    /// Evaluate `expr` over `views` (a batch of `n` rows), AND in the optional
    /// `tomb_mask` (true = keep), and compact survivors into `filtered`. When
    /// late-mat (`emit_loc`), also packs each survivor's `__rowloc` per `loc`.
    /// Returns the survivor count.
    /// Evaluate `expr` over `eval_views`/`eval_schema` (which may carry trailing
    /// filter-only columns past `out_count`), AND in the optional `tomb_mask`,
    /// and compact only the first `out_count` (output) columns into `filtered`.
    /// `out_count == eval_views.len` whenever there are no filter-only columns.
    fn evalAndCompact(self: *Scan, eval_views: []const ColumnView, eval_schema: []const Column, out_count: usize, n: usize, tomb_mask: ?[]const bool, expr: PredicateExpr, loc: SurvivorLoc) !usize {
        const mask = try self.ensureMask(n);
        const batch = Batch{ .schema = eval_schema, .values = eval_views, .row_count = n };
        try predicate.evaluateExprGuided(self.allocator, expr, eval_schema, batch, mask, null);
        if (tomb_mask) |tm| {
            for (mask[0..n], tm) |*m, keep| m.* = m.* and keep;
        }
        var matched: usize = 0;
        for (mask[0..n]) |m| matched += @intFromBool(m);
        if (matched == 0) {
            self.filtered_coded = null;
            self.filtered_hashed = null;
            return 0;
        }

        const coding = self.n_coded > 0;
        const slots: ?[]?exec.CodedColumn = if (coding) try self.ensureCodedSlots() else null;
        const hashing = self.n_hashed > 0;
        const hash_slots: ?[]?[]const u128 = if (hashing) try self.ensureHashedSlots() else null;

        const filtered_cols = try self.ensureFilteredBuffers();
        for (filtered_cols) |*c| c.clear();
        // Only the first `out_count` columns are emitted; any trailing entries
        // in `eval_views` are filter-only and stay inside the scan. (Under
        // emit_loc `filtered_cols` also carries a trailing `__rowloc` buffer.)
        for (eval_views[0..out_count], filtered_cols[0..out_count], 0..) |src, *dst, j| {
            // Coded group key: intern just the survivors' strings into the code
            // buffer (the views are already decoded here); placeholder in the
            // value column. The aggregate reads the sidecar codes.
            if (coding and self.coded_dicts_by_j[j] != null) {
                const gdict = self.coded_dicts_by_j[j].?;
                try self.code_bufs[j].resize(self.allocator, matched);
                const codes = self.code_bufs[j].items[0..matched];
                const sv = switch (src.data) {
                    .varchar, .string, .char => |s| s,
                    else => unreachable,
                };
                var k: usize = 0;
                for (mask[0..n], 0..) |m, row| if (m) {
                    codes[k] = try gdict.intern(self.allocator, sv.rowBytes(row));
                    k += 1;
                };
                try self.fillEmptyStrings(dst, matched);
                slots.?[j] = .{ .codes = codes, .dict = gdict };
                continue;
            }
            // Hashed group key over already-decoded views (memtable / non-block
            // sources): digest the survivors so every filtered batch carries
            // the sidecar uniformly.
            if (hashing and self.hash_cols_by_j[j]) {
                try self.hash_bufs[j].resize(self.allocator, matched);
                const digests = self.hash_bufs[j].items[0..matched];
                const sv = switch (src.data) {
                    .varchar, .string, .char => |s| s,
                    else => return error.TypeMismatch,
                };
                var k: usize = 0;
                for (mask[0..n], 0..) |m, row| if (m) {
                    digests[k] = exec.stringKeyDigest(sv.rowBytes(row));
                    k += 1;
                };
                try self.fillEmptyStrings(dst, matched);
                hash_slots.?[j] = digests;
                continue;
            }
            try engine.memtable.appendMaskedColumn(self.allocator, src, mask[0..n], dst);
        }
        if (self.emit_loc) switch (loc) {
            .segment => |rg| try self.appendSurvivorLocs(mask[0..n], rg),
            .memtable => try self.appendSurvivorLocs(mask[0..n], null),
        };
        self.filtered_coded = slots;
        self.filtered_hashed = hash_slots;
        return matched;
    }

    /// Build a keep-mask for tombstoned rows in `[rg_first, rg_first+rg_count)`,
    /// or null when no tombstones fall in range. `true` = surviving row.
    fn tombstoneMask(self: *Scan, rg_first: u32, rg_count: u32) !?[]bool {
        const tombs = self.cur_segment_tomb orelse return null;
        if (tombs.len == 0) return null;
        const rg_end = rg_first + rg_count;
        const lo = std.sort.lowerBound(u32, tombs, rg_first, cmpU32);
        const hi = std.sort.lowerBound(u32, tombs, rg_end, cmpU32);
        if (lo == hi) return null;
        const mask = try self.allocator.alloc(bool, rg_count);
        @memset(mask, true);
        for (tombs[lo..hi]) |off| mask[off - rg_first] = false;
        return mask;
    }

    fn ensureMask(self: *Scan, n: usize) ![]bool {
        if (self.mask_buf.len < n) {
            if (self.mask_buf.len > 0) self.allocator.free(self.mask_buf);
            self.mask_buf = try self.allocator.alloc(bool, n);
        }
        return self.mask_buf;
    }

    fn ensureMask2(self: *Scan, n: usize) ![]bool {
        if (self.mask_buf2.len < n) {
            if (self.mask_buf2.len > 0) self.allocator.free(self.mask_buf2);
            self.mask_buf2 = try self.allocator.alloc(bool, n);
        }
        return self.mask_buf2;
    }

    fn ensureBorrowBlocks(self: *Scan) ![]storage.ReadSegment.BorrowedBlock {
        if (self.borrow_blocks.len >= self.out_phys.len) return self.borrow_blocks;
        if (self.borrow_blocks.len > 0) self.allocator.free(self.borrow_blocks);
        self.borrow_blocks = try self.allocator.alloc(storage.ReadSegment.BorrowedBlock, self.out_phys.len);
        return self.borrow_blocks;
    }

    fn releaseBatch(self: *Scan) void {
        if (self.decoded_valid) {
            for (self.decoded) |*c| c.deinit(self.allocator);
            self.decoded_valid = false;
        }
        self.sub_off = 0;
        self.sub_count = 0;
    }

    /// If any tombstone offsets fall within `[rg_first, rg_first + rg_count)`,
    /// materialize a filtered batch into `filtered` and return it. Otherwise
    /// returns null so the caller emits the unfiltered batch.
    fn applyTombsIfAny(self: *Scan, rg_first: u32, rg_count: u32) !?Batch {
        const tombs = self.cur_segment_tomb orelse return null;
        if (tombs.len == 0) return null;

        const rg_end = rg_first + rg_count;
        const lo = std.sort.lowerBound(u32, tombs, rg_first, cmpU32);
        const hi = std.sort.lowerBound(u32, tombs, rg_end, cmpU32);
        if (lo == hi) return null;

        const tomb_slice = tombs[lo..hi];
        const filtered_cols = try self.ensureFilteredBuffers();
        for (filtered_cols) |*c| c.clear();

        const mask = try self.allocator.alloc(bool, rg_count);
        defer self.allocator.free(mask);
        @memset(mask, true);
        for (tomb_slice) |off| {
            mask[off - rg_first] = false;
        }

        var kept: usize = 0;
        for (mask) |m| if (m) {
            kept += 1;
        };

        for (self.decoded, filtered_cols) |src, *dst| {
            try engine.memtable.appendMaskedColumn(self.allocator, src.view(), mask, dst);
        }
        for (filtered_cols, 0..) |c, i| self.views[i] = c.view();

        return Batch{
            .schema = self.out_schema,
            .values = self.views,
            .row_count = kept,
        };
    }
};

fn cmpU32(target: u32, item: u32) std.math.Order {
    return std.math.order(target, item);
}

/// Map a `PredicateOp` to the structurally-identical `simd.CmpOp` consumed by
/// the FOR-code comparison kernel.
fn cmpOpToSimd(op: PredicateOp) @import("../util/simd.zig").CmpOp {
    return switch (op) {
        .eq => .eq,
        .neq => .neq,
        .lt => .lt,
        .lte => .lte,
        .gt => .gt,
        .gte => .gte,
    };
}
