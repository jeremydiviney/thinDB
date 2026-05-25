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

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;
const PredicateExpr = predicate.PredicateExpr;
const PredicateOp = predicate.PredicateOp;
const statsOverlapPredicate = predicate.statsOverlapPredicate;

const rowloc = @import("rowloc.zig");

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
    cur_segment: ?storage.ReadSegment = null,
    /// Sorted, deduped tombstone offsets for the current segment (or null).
    cur_segment_tomb: ?[]u32 = null,
    /// Prefix sum: `cur_rg_first_row[k]` is the first row offset of row group k
    /// within the current segment.
    cur_rg_first_row: []u32 = &.{},

    decoded: []storage.OwnedColumn,
    decoded_valid: bool = false,
    views: []ColumnView,

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
    /// Scratch array of borrowed cache blocks for the fused-filter fast path,
    /// one slot per projected column. Reused across `next()` calls; the pins
    /// it holds are released within each call. Freed in `deinit`.
    borrow_blocks: []storage.ReadSegment.BorrowedBlock = &.{},

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

    const Phase = enum { segments, memtable, done };

    pub const PruneHint = struct {
        col_idx: usize,
        op: PredicateOp,
        val: Value,
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
        const self = try allocWithProjectionLoc(allocator, table, injected, needed, emit_loc);
        return makeQuery(allocator, self);
    }

    /// Same as `createWithProjectionLoc` but returns the raw `*Scan` instead of
    /// the type-erased `Query`. `LateScan` builds its inner Scan through this
    /// so it can reach `memtableSnap()` directly, then wraps the pointer in a
    /// `Query` via `exec.makeQuery`.
    pub fn allocWithProjectionLoc(
        allocator: Allocator,
        table: *Table,
        injected: ?*exec.memory.MemoryAccountant,
        needed: ?[]const []const u8,
        emit_loc: bool,
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
        table.ddl_lock.lockSharedUncancelable(table.io);
        errdefer table.ddl_lock.unlockShared(table.io);

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
        table.mutex.lockUncancelable(table.io);
        const segment_count = table.manifest.segments.items.len;
        const memtable_snap = table.memtable;
        memtable_snap.acquire();
        const memtable_row_count = memtable_snap.row_count;
        table.mutex.unlock(table.io);

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
            .prunes = .empty,
            .owned_accountant = owned_accountant,
            .owns_accountant = owns_accountant,
            .cached_stats = cached_stats,
        };

        return self;
    }

    pub fn accountant(self: *Scan) ?*exec.memory.MemoryAccountant {
        return self.owned_accountant;
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
        if (self.borrow_blocks.len > 0) self.allocator.free(self.borrow_blocks);
        if (self.memtable_loc_buf.len > 0) self.allocator.free(self.memtable_loc_buf);
        if (self.cached_stats.len > 0) self.allocator.free(self.cached_stats);
        self.prunes.deinit(self.allocator);
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
        self.allocator.free(self.out_phys);
        if (self.out_schema_owned) self.allocator.free(@constCast(self.out_schema));
        // Drop our pinned memtable reference. If we held the last one and
        // the memtable was retired (a flush/delete swapped it out), it's
        // freed here.
        self.memtable_snap.release();
        // Release our shared ddl_lock — any DDL waiter on the exclusive
        // lock can now proceed once all shared holders have released.
        self.table.ddl_lock.unlockShared(self.table.io);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    fn closeCurSegment(self: *Scan) void {
        if (self.cur_segment) |*seg| {
            seg.deinit();
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
    /// (returns false) for the count-only scan (no columns to filter) and the
    /// late-materialization scan (`emit_loc`, which has its own pipeline). On
    /// acceptance the predicate's leaves must reference only columns this scan
    /// projects — guaranteed when the fusing Filter sits directly above and
    /// shares this scan's output schema. The expr is stored by value; its
    /// backing memory is owned by the caller and must outlive this scan.
    pub fn tryFuseFilter(self: *Scan, expr: PredicateExpr) !bool {
        if (self.out_phys.len == 0 or self.emit_loc) return false;
        if (self.fused_filter != null) return false;
        self.fused_filter = expr;
        return true;
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

        try self.prunes.append(self.allocator, .{
            .col_idx = col_idx,
            .op = pred.op,
            .val = pred.val,
        });

        // Segment-level pruning. The early-return above already proved
        // this column's type has stats, so any manifest entry carrying
        // per-column stats (v4+) has a valid slot at `col_idx`. Older
        // manifests fall back to `leading_key_stats` when the predicate
        // is on the leading order-key column.
        const order_key = self.table.schema.order_key;
        const is_leading = order_key.len > 0 and std.mem.eql(u8, pred.col, order_key[0]);
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
            if (!statsOverlapPredicate(lk, pred.op, pred.val)) {
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

    fn rowGroupCanMatch(self: Scan, rg: storage.RowGroupMeta) bool {
        for (self.prunes.items) |hint| {
            const col_stats = rg.stats[hint.col_idx];
            if (!statsOverlapPredicate(col_stats, hint.op, hint.val)) return false;
        }
        return true;
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

    pub fn next(self: *Scan) !?Batch {
        if (self.fused_filter != null) return self.nextFiltered();

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
                if (self.memtable_row_count == 0) return null;
                return Batch{ .schema = self.out_schema, .values = self.views, .row_count = @intCast(self.memtable_row_count) };
            }
            return null;
        }

        // Segments phase
        while (self.phase == .segments) {
            if (self.cur_segment == null and !try self.openCurSegment()) break;

            const seg = &self.cur_segment.?;
            if (self.cur_rg_idx >= seg.info.row_groups.len) {
                self.closeCurSegment();
                self.cur_seg_idx += 1;
                continue;
            }

            const rg = seg.info.row_groups[self.cur_rg_idx];
            if (!self.rowGroupCanMatch(rg)) {
                self.cur_rg_idx += 1;
                continue;
            }

            const rg_count = rg.row_count;

            var decoded_cols: usize = 0;
            errdefer {
                for (self.decoded[0..decoded_cols]) |*c| c.deinit(self.allocator);
                // A later step (e.g. tombstone application) can fail after
                // `decoded_valid` is set; clear it so the deinit-time
                // `releaseBatch` doesn't free these columns a second time.
                self.decoded_valid = false;
            }
            for (self.out_phys, 0..) |phys, j| {
                self.decoded[j] = try seg.decodeColumnMaybeCached(
                    self.allocator,
                    self.table.schema,
                    self.cur_rg_idx,
                    phys,
                    &self.table.cache,
                );
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
            return Batch{
                .schema = self.out_schema,
                .values = self.views,
                .row_count = rg_count,
            };
        }

        // Memtable phase — read from the pinned snapshot, not the table's
        // live memtable. Bounded by `memtable_row_count` captured at scan
        // create time; rows appended after that are invisible to this scan.
        if (self.phase == .memtable) {
            self.phase = .done;
            if (self.memtable_row_count == 0) return null;

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
        var name_buf: [32]u8 = undefined;
        const file_name = try Table.segmentFileName(&name_buf, entry.segment_id);
        self.cur_segment = try storage.readSegment(
            self.allocator,
            self.io,
            self.table.segments_dir,
            file_name,
            self.table.schema,
        );
        self.segments_opened += 1;
        self.cur_segment_tomb = try storage.tombstone.read(
            self.allocator,
            self.io,
            self.table.segments_dir,
            entry.segment_id,
        );
        const rgs = self.cur_segment.?.info.row_groups;
        self.cur_rg_first_row = try self.allocator.alloc(u32, rgs.len);
        var running: u32 = 0;
        for (rgs, 0..) |rg, i| {
            self.cur_rg_first_row[i] = running;
            running += rg.row_count;
        }
        self.cur_rg_idx = 0;
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

            const seg = &self.cur_segment.?;
            if (self.cur_rg_idx >= seg.info.row_groups.len) {
                self.closeCurSegment();
                self.cur_seg_idx += 1;
                continue;
            }

            const rg = seg.info.row_groups[self.cur_rg_idx];
            if (!self.rowGroupCanMatch(rg)) {
                self.cur_rg_idx += 1;
                continue;
            }

            const rg_first = self.cur_rg_first_row[self.cur_rg_idx];
            const rg_count = rg.row_count;
            const this_rg = self.cur_rg_idx;
            self.cur_rg_idx += 1;

            const matched = try self.filterRowGroup(seg, this_rg, rg_first, rg_count, expr);
            if (matched == 0) continue;
            for (self.filtered.?, 0..) |c, i| self.views[i] = c.view();
            return Batch{ .schema = self.out_schema, .values = self.views, .row_count = matched };
        }

        if (self.phase == .memtable) {
            self.phase = .done;
            if (self.memtable_row_count == 0) return null;

            const n: usize = @intCast(self.memtable_row_count);
            const mem_views = try self.allocator.alloc(ColumnView, self.out_phys.len);
            defer self.allocator.free(mem_views);
            for (self.out_phys, 0..) |phys, j| mem_views[j] = self.memtable_snap.columns[phys].view();

            const matched = try self.evalAndCompact(mem_views, n, null, expr);
            if (matched == 0) return null;
            for (self.filtered.?, 0..) |c, i| self.views[i] = c.view();
            return Batch{ .schema = self.out_schema, .values = self.views, .row_count = matched };
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
        const tomb_mask = try self.tombstoneMask(rg_first, rg_count);
        defer if (tomb_mask) |m| self.allocator.free(m);

        // FOR-aware fast path: a single comparison leaf whose column is
        // FOR-encoded compares the narrow codes directly (no native expand).
        // Returns null when the shape doesn't apply (non-leaf, non-comparison
        // op, non-FOR block) — then the borrow / owned-decode paths run
        // unchanged (for a FOR block that means decode-expand then native
        // filter: correct, just without the bandwidth win).
        if (try self.tryFilterForLeaf(seg, rg_idx, rg_count, tomb_mask, expr)) |matched| {
            return matched;
        }

        // Fast path: borrow views over cache bytes. Bail to copy if any column
        // can't be viewed (misalignment / big-endian).
        if (try self.tryBorrowViews(seg, rg_idx, rg_count)) |borrow| {
            defer for (borrow.blocks) |*b| b.release(self.allocator, &self.table.cache);
            return self.evalAndCompact(borrow.views, rg_count, tomb_mask, expr);
        }

        // Fallback: owned decode (as the non-fused path), then evaluate +
        // compact through the same kernel.
        const owned_views = try self.decodeOwnedViews(seg, rg_idx, rg_count);
        defer self.releaseDecoded();
        return self.evalAndCompact(owned_views, rg_count, tomb_mask, expr);
    }

    /// FOR-aware leaf filter (Phase 2B). Applies when `expr` is a single
    /// comparison `.leaf` whose projected column's block is FOR-encoded: the
    /// constant is translated into the block's code domain once, the narrow
    /// codes are compared directly to build the row mask (validity + tombstones
    /// folded in exactly as the native path), and only survivors are expanded to
    /// native during compaction. Returns null (taking no pins) when the shape
    /// doesn't apply, so the caller's existing paths run unchanged.
    fn tryFilterForLeaf(
        self: *Scan,
        seg: *storage.ReadSegment,
        rg_idx: usize,
        rg_count: u32,
        tomb_mask: ?[]const bool,
        expr: PredicateExpr,
    ) !?usize {
        const leaf = switch (expr) {
            .leaf => |p| p,
            else => return null,
        };
        switch (leaf.op) {
            .eq, .neq, .lt, .lte, .gt, .gte => {},
        }
        // The leaf column must be one this scan projects (true for every fused
        // filter — the fusing Filter shares the scan's output schema).
        const proj = blk: {
            for (self.out_phys, 0..) |phys, j| {
                if (@import("../types.zig").columnNameEql(self.table.schema.columns[phys].name, leaf.col)) {
                    break :blk .{ .phys = phys, .j = j };
                }
            }
            return null;
        };
        const col_type = self.table.schema.columns[proj.phys].type;
        if (!predicate.typeHasRange(col_type)) return null;

        // Borrow the predicate column's block. Decline (no FOR win) if it's not
        // FOR-encoded; release the pin and let the caller's path handle it.
        const flags = storage.format.ColumnBlockFlags{ .has_nulls = self.table.schema.columns[proj.phys].nullable };
        var pred_block = try seg.borrowColumnBlock(self.allocator, rg_idx, proj.phys, &self.table.cache);
        if (pred_block.encoding != .for_) {
            pred_block.release(self.allocator, &self.table.cache);
            return null;
        }
        defer pred_block.release(self.allocator, &self.table.cache);

        const fv = storage.segment_reader.forViewOf(pred_block.bytes, rg_count, flags);

        // span = max - base, from the row-group stats (base == stats.min for a
        // FOR block). Used to resolve the boundary (.none/.all) cases precisely.
        const col_stats = seg.info.row_groups[rg_idx].stats[proj.phys];
        if (col_stats.max < fv.block.base) return null; // degenerate (all-null sentinel) — shouldn't reach FOR
        const span: u128 = @intCast(col_stats.max - fv.block.base);

        const plan = predicate.translateForLeaf(fv.block.base, span, leaf.op, leaf.val) orelse return null;

        const mask = try self.ensureMask(rg_count);
        switch (plan) {
            .none => @memset(mask[0..rg_count], false),
            .all => @memset(mask[0..rg_count], true),
            .compare => |cp| {
                storage.segment_reader.forCompareInto(fv.block, cmpOpToSimd(cp.op), cp.code, rg_count, mask[0..rg_count]);
            },
        }
        // NULLs never match a comparison — clear them (skipped for `.none`,
        // already all-false). The native leaf path does the same.
        if (plan != .none and fv.nulls != null) {
            for (0..rg_count) |i| {
                if (!storage.column.isValidBit(fv.nulls, i)) mask[i] = false;
            }
        }
        if (tomb_mask) |tm| {
            for (mask[0..rg_count], tm) |*m, keep| m.* = m.* and keep;
        }

        var matched: usize = 0;
        for (mask[0..rg_count]) |m| matched += @intFromBool(m);
        if (matched == 0) return @as(usize, 0);

        try self.materializeSurvivors(seg, rg_idx, rg_count, mask[0..rg_count]);
        return matched;
    }

    /// Compact the masked survivors of every projected column into `filtered`.
    /// Each column is borrowed from the cache; a FOR-encoded block expands only
    /// its survivors to native (`forExpandSurvivors`), a raw block is viewed in
    /// place and run through the shared masked-compaction. The borrow pins are
    /// released before returning.
    fn materializeSurvivors(self: *Scan, seg: *storage.ReadSegment, rg_idx: usize, rg_count: u32, mask: []const bool) !void {
        const filtered_cols = try self.ensureFilteredBuffers();
        for (filtered_cols) |*c| c.clear();

        for (self.out_phys, 0..) |phys, j| {
            const col_type = self.table.schema.columns[phys].type;
            const flags = storage.format.ColumnBlockFlags{ .has_nulls = self.table.schema.columns[phys].nullable };
            var block = try seg.borrowColumnBlock(self.allocator, rg_idx, phys, &self.table.cache);
            defer block.release(self.allocator, &self.table.cache);

            if (block.encoding == .for_) {
                const fv = storage.segment_reader.forViewOf(block.bytes, rg_count, flags);
                try self.appendForSurvivors(fv, mask, &filtered_cols[j]);
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

    const Borrow = struct {
        blocks: []storage.ReadSegment.BorrowedBlock,
        views: []ColumnView,
    };

    /// Try to build borrowed typed views over the cache blocks for every
    /// projected column. Returns null (after releasing any pins taken) if any
    /// column's bytes aren't safe to view in place. The returned `blocks` /
    /// `views` alias `self`-owned scratch (`borrow_blocks` / `views`); the
    /// caller must release the blocks within the same `next()` call.
    fn tryBorrowViews(self: *Scan, seg: *storage.ReadSegment, rg_idx: usize, rg_count: u32) !?Borrow {
        const blocks = try self.ensureBorrowBlocks();

        var got: usize = 0;
        errdefer for (blocks[0..got]) |*b| b.release(self.allocator, &self.table.cache);

        for (self.out_phys, 0..) |phys, j| {
            const col_type = self.table.schema.columns[phys].type;
            const flags = storage.format.ColumnBlockFlags{ .has_nulls = self.table.schema.columns[phys].nullable };
            var block = try seg.borrowColumnBlock(self.allocator, rg_idx, phys, &self.table.cache);
            const view = storage.segment_reader.viewRawColumn(col_type, block.bytes, rg_count, flags, block.encoding) orelse {
                // Misaligned / FOR-encoded / unsupported: release this block and
                // abandon the fast path for the whole row group (release the rest too).
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

    /// Evaluate `expr` over `views` (a batch of `n` rows), AND in the optional
    /// `tomb_mask` (true = keep), and compact survivors into `filtered`.
    /// Returns the survivor count.
    fn evalAndCompact(self: *Scan, views: []const ColumnView, n: usize, tomb_mask: ?[]const bool, expr: PredicateExpr) !usize {
        const mask = try self.ensureMask(n);
        const batch = Batch{ .schema = self.out_schema, .values = views, .row_count = n };
        try predicate.evaluateExprGuided(self.allocator, expr, self.out_schema, batch, mask, null);
        if (tomb_mask) |tm| {
            for (mask[0..n], tm) |*m, keep| m.* = m.* and keep;
        }
        var matched: usize = 0;
        for (mask[0..n]) |m| matched += @intFromBool(m);
        if (matched == 0) return 0;

        const filtered_cols = try self.ensureFilteredBuffers();
        for (filtered_cols) |*c| c.clear();
        for (views, filtered_cols) |src, *dst| {
            try engine.memtable.appendMaskedColumn(self.allocator, src, mask[0..n], dst);
        }
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
