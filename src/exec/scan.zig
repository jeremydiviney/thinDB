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
const PredicateOp = predicate.PredicateOp;
const statsOverlapPredicate = predicate.statsOverlapPredicate;

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

    /// Lazily allocated when a row group has rows tombstoned and we need to
    /// materialize a filtered batch. Reused across batches.
    filtered: ?[]ColumnStore = null,

    /// Pushed-down predicates used to skip row groups via min/max stats.
    prunes: std.ArrayList(PruneHint),

    /// Owned by this Scan when `Table.query_memory_budget > 0`. All
    /// operators in this query inherit it via `Query.accountant()`.
    /// Null = no tracking.
    owned_accountant: ?*exec.memory.MemoryAccountant = null,

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

    pub fn create(allocator: Allocator, table: *Table) !Query {
        const n = table.schema.columns.len;

        const decoded = try allocator.alloc(storage.OwnedColumn, n);
        errdefer allocator.free(decoded);
        const views = try allocator.alloc(ColumnView, n);
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

        // Allocate the per-query memory accountant if the Table's
        // config sets a non-zero budget. Heap-allocated so all
        // operators in the pipeline can share via a pointer.
        var owned_accountant: ?*exec.memory.MemoryAccountant = null;
        if (table.query_memory_budget > 0) {
            const acc = try allocator.create(exec.memory.MemoryAccountant);
            acc.* = exec.memory.MemoryAccountant.init(table.query_memory_budget);
            owned_accountant = acc;
        }
        errdefer if (owned_accountant) |a| allocator.destroy(a);

        self.* = .{
            .allocator = allocator,
            .io = table.io,
            .table = table,
            .segment_count = segment_count,
            .memtable_snap = memtable_snap,
            .memtable_row_count = memtable_row_count,
            .decoded = decoded,
            .views = views,
            .prunes = .empty,
            .owned_accountant = owned_accountant,
        };

        return makeQuery(allocator, self);
    }

    pub fn accountant(self: *Scan) ?*exec.memory.MemoryAccountant {
        return self.owned_accountant;
    }

    pub fn deinit(self: *Scan) void {
        self.releaseBatch();
        self.closeCurSegment();
        self.prunes.deinit(self.allocator);
        if (self.owned_accountant) |a| self.allocator.destroy(a);
        if (self.seg_skip) |s| self.allocator.free(s);
        if (self.filtered) |arr| {
            for (arr) |*c| c.deinit(self.allocator);
            self.allocator.free(arr);
        }
        self.allocator.free(self.decoded);
        self.allocator.free(self.views);
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
        const arr = try self.allocator.alloc(ColumnStore, self.table.schema.columns.len);
        errdefer self.allocator.free(arr);
        var inited: usize = 0;
        errdefer for (arr[0..inited]) |*c| c.deinit(self.allocator);
        for (self.table.schema.columns, 0..) |col, i| {
            arr[i] = try ColumnStore.init(self.allocator, col.type, col.nullable);
            inited += 1;
        }
        self.filtered = arr;
        return arr;
    }

    pub fn addPrune(self: *Scan, pred: Predicate) !void {
        const col_idx = blk: {
            for (self.table.schema.columns, 0..) |c, i| {
                if (std.mem.eql(u8, c.name, pred.col)) break :blk i;
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
        return self.table.schema.columns;
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
        self.releaseBatch();

        // Segments phase
        while (self.phase == .segments) {
            if (self.cur_segment == null) {
                // Advance past segments excluded by leading-key predicates.
                while (self.cur_seg_idx < self.segment_count) : (self.cur_seg_idx += 1) {
                    const skip = if (self.seg_skip) |s| s[self.cur_seg_idx] else false;
                    if (!skip) break;
                }
                if (self.cur_seg_idx >= self.segment_count) {
                    self.phase = .memtable;
                    break;
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
            }

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

            for (self.table.schema.columns, 0..) |_, i| {
                self.decoded[i] = try seg.decodeColumn(
                    self.allocator,
                    self.table.schema,
                    self.cur_rg_idx,
                    i,
                );
            }
            self.decoded_valid = true;

            const rg_first = self.cur_rg_first_row[self.cur_rg_idx];
            const rg_count = rg.row_count;
            self.cur_rg_idx += 1;

            // Apply tombstones if any fall within this row group.
            const masked = try self.applyTombsIfAny(rg_first, rg_count);
            if (masked) |out| return out;

            for (self.decoded, 0..) |c, i| self.views[i] = c.view();
            return Batch{
                .schema = self.table.schema.columns,
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

            for (self.memtable_snap.columns, 0..) |c, i| {
                self.views[i] = c.view();
            }
            return Batch{
                .schema = self.table.schema.columns,
                .values = self.views,
                .row_count = @intCast(self.memtable_row_count),
            };
        }

        return null;
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
            .schema = self.table.schema.columns,
            .values = self.views,
            .row_count = kept,
        };
    }
};

fn cmpU32(target: u32, item: u32) std.math.Order {
    return std.math.order(target, item);
}
