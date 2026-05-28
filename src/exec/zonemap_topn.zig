//! Zonemap top-N operator — block-skipping `ORDER BY <keys> LIMIT n [OFFSET m]`.
//!
//! The same query shape `LateScan` handles (single base table, WHERE, ORDER BY
//! + LIMIT, no GROUP BY / aggregates) — but instead of scanning + filtering
//! EVERY row and feeding a bounded TopN, this visits row groups in best-corner
//! order and stops once the heap's K-th best beats every remaining row group's
//! best-possible tuple. Per-row-group min/max from the segment footer give the
//! bound.
//!
//! Algorithm (need top `K = n + m`):
//!   1. Enumerate every (segment, row_group). For each, read the leading order
//!      key's min/max from the footer `Stats`.
//!   2. best-corner = the most-preferred value the RG could hold per key: ASC →
//!      min, DESC → max. Only the leading key is encoded; secondary keys (and
//!      any key without usable stats) contribute a non-tightening sentinel —
//!      only the leading key prunes in v1.
//!   3. Sort the RG list by best-corner (leading-key direction).
//!   4. Bounded max-heap of the K best rows (worst at root), keyed by the FULL
//!      ORDER BY comparator over actual decoded values (`compareInColumn` +
//!      per-key `desc`, identical to `TopN.Comparator`).
//!   5. Visit RGs in best-corner order. Once the heap holds K rows AND this
//!      RG's best-corner is worse than the heap's worst-kept leading key →
//!      STOP (every later RG is also worse).
//!   6. Else decode this RG's probe columns, apply WHERE + tombstones, push
//!      each survivor (sort keys + packed `__rowloc`) into the heap.
//!   7. The memtable (no zonemap) is always scanned — never skippable.
//!   8. Sort the heap's survivors, drop `m`, keep `n`.
//!   9. Materialize the WIDE output columns by `__rowloc` via `LateScan`.
//!
//! CORRECTNESS: best-corner is a provable lower (ASC) / upper (DESC) bound on
//! the leading key of every row in the RG. Once it's worse than the K-th best
//! already held, the RG can't improve the answer; visiting in best-corner order
//! makes "this RG fails ⇒ all later fail" hold. The heap comparator and final
//! sort match `TopN` exactly, so the output is byte-identical to the lateScan
//! reference path. When ANY precondition isn't met, the builder returns null
//! and the caller falls back to lateScan.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;
const Memtable = engine.Memtable;
const transform = engine.transform;

const api = @import("../api/api.zig");
const Table = api.Table;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const Error = exec.Error;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const PredicateExpr = predicate.PredicateExpr;
const SortSpec = @import("sort.zig").SortSpec;

const rowloc = @import("rowloc.zig");
const Scan = @import("scan.zig").Scan;
const LateScan = @import("latescan.zig").LateScan;

/// One row group's identity + its leading-key best-corner bound.
const RgRef = struct {
    seg_idx: usize,
    rg_idx: usize,
    row_count: u32,
    /// best-corner leading-key value: ASC → min, DESC → max.
    corner: i128,
};

pub const ZonemapTopN = struct {
    allocator: Allocator,
    table: *Table,
    /// Inner late-mat plan (Scan(probe,loc)→Filter→TopN→LateScan). We never
    /// drive it; it carries the pinned memtable snapshot + ddl_lock and the
    /// wide-column materializer we reuse.
    late: *LateScan,
    inner_scan: *Scan,

    segment_count: usize,

    pred: PredicateExpr,
    /// Probe columns = filter ∪ ORDER BY, physical (table) indices. Owned.
    probe_phys: []usize,
    /// Probe-only schema (table columns at `probe_phys`, in order) — for the
    /// predicate's name resolution. Owned.
    probe_schema: []Column,
    /// Physical (table) index + direction for each ORDER BY key. Owned.
    key_phys: []usize,
    key_desc: []bool,
    /// Probe-batch index of each ORDER BY key column. Owned.
    key_probe_idx: []usize,
    /// Leading order key (the column whose footer stats drive pruning).
    leading_phys: usize,
    leading_desc: bool,

    n: usize,
    offset: usize,
    /// K = n + m (saturating).
    keep: usize,

    /// Append-only candidate store: `key_cols[i]` holds ORDER BY key `i`'s
    /// decoded value for each appended candidate row; `loc_store` holds the
    /// packed `__rowloc`. The heap holds row indices into these. Rows that fall
    /// out of the heap become dead; `compact` periodically rebuilds the store
    /// down to just the live heap rows so memory stays O(keep).
    key_cols: []ColumnStore,
    loc_store: std.ArrayListUnmanaged(i64) = .empty,
    /// Binary max-heap (worst-kept at root) of row indices into `key_cols`.
    heap: std.ArrayListUnmanaged(u32) = .empty,

    decoded: []storage.OwnedColumn,
    decoded_valid: bool = false,

    done: bool = false,
    out_schema: []const Column,

    /// Build a zonemap top-N plan, or null when the shape/leading-key isn't
    /// supported (caller falls back to `exec.lateScan`). Same parameter shape
    /// as `exec.lateScan`. `order_specs` MUST be non-empty. On a null return
    /// NOTHING is allocated.
    pub fn create(
        allocator: Allocator,
        table: *Table,
        accountant_ptr: ?*exec.memory.MemoryAccountant,
        probe_names: []const []const u8,
        pred: PredicateExpr,
        order_specs: []const SortSpec,
        output_names: []const []const u8,
        n: usize,
        offset: usize,
    ) !?Query {
        if (order_specs.len == 0) return null;

        const lead = order_specs[0];
        const lead_idx = types.findColumn(table.schema.columns, lead.col) orelse return null;
        const lead_col = table.schema.columns[lead_idx];
        if (lead_col.nullable) return null;
        if (!leadingKeyTypeSupported(lead_col.type)) return null;

        // Resolve every ORDER BY key + probe column to a physical index up
        // front; any unresolved name ⇒ bail (never risk a wrong plan). The
        // probe set is guaranteed to contain every key column by the caller,
        // but we verify and map it explicitly.
        const key_phys = try allocator.alloc(usize, order_specs.len);
        errdefer allocator.free(key_phys);
        const key_desc = try allocator.alloc(bool, order_specs.len);
        errdefer allocator.free(key_desc);
        for (order_specs, 0..) |sp, i| {
            key_phys[i] = types.findColumn(table.schema.columns, sp.col) orelse return null;
            key_desc[i] = sp.desc;
        }

        const probe_phys = try allocator.alloc(usize, probe_names.len);
        errdefer allocator.free(probe_phys);
        for (probe_names, 0..) |nm, i| {
            probe_phys[i] = types.findColumn(table.schema.columns, nm) orelse return null;
        }

        // Every ORDER BY key must be present in the probe set (so we can read
        // its value during filtering). The late-mat shape builder guarantees
        // this, but verify rather than trust.
        const key_probe_idx = try allocator.alloc(usize, order_specs.len);
        errdefer allocator.free(key_probe_idx);
        for (key_phys, 0..) |phys, i| {
            key_probe_idx[i] = blk: {
                for (probe_phys, 0..) |p, j| if (p == phys) break :blk j;
                return null;
            };
        }

        const probe_schema = try allocator.alloc(Column, probe_phys.len);
        errdefer allocator.free(probe_schema);
        for (probe_phys, 0..) |phys, i| probe_schema[i] = table.schema.columns[phys];

        // Build the inner late-mat plan: pins the memtable + ddl_lock and gives
        // us the wide-column materializer. We never call `inner.next()`.
        const scan_ptr = try Scan.allocWithProjectionLoc(allocator, table, accountant_ptr, probe_names, true);
        var inner = makeQuery(allocator, scan_ptr);
        var late_built = false;
        errdefer if (!late_built) inner.deinit();

        inner = try inner.filter(pred);
        inner = try inner.topN(order_specs, n, offset);
        var late_q = try LateScan.create(allocator, inner, scan_ptr, table, output_names);
        late_built = true;
        errdefer late_q.deinit();
        const late: *LateScan = @ptrCast(@alignCast(late_q.ptr));

        const decoded = try allocator.alloc(storage.OwnedColumn, probe_phys.len);
        errdefer allocator.free(decoded);

        const key_cols = try allocator.alloc(ColumnStore, order_specs.len);
        errdefer allocator.free(key_cols);
        var kc_inited: usize = 0;
        errdefer for (key_cols[0..kc_inited]) |*c| c.deinit(allocator);
        for (key_phys, 0..) |phys, i| {
            const c = table.schema.columns[phys];
            key_cols[i] = try ColumnStore.init(allocator, c.type, c.nullable);
            kc_inited += 1;
        }

        const self = try allocator.create(ZonemapTopN);
        errdefer allocator.destroy(self);

        const keep = std.math.add(usize, n, offset) catch std.math.maxInt(usize);

        self.* = .{
            .allocator = allocator,
            .table = table,
            .late = late,
            .inner_scan = scan_ptr,
            .segment_count = scan_ptr.segment_count,
            .pred = pred,
            .probe_phys = probe_phys,
            .probe_schema = probe_schema,
            .key_phys = key_phys,
            .key_desc = key_desc,
            .key_probe_idx = key_probe_idx,
            .leading_phys = lead_idx,
            .leading_desc = lead.desc,
            .n = n,
            .offset = offset,
            .keep = keep,
            .key_cols = key_cols,
            .decoded = decoded,
            .out_schema = late.outputSchema(),
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *ZonemapTopN) void {
        self.releaseDecoded();
        self.late.deinit();
        for (self.key_cols) |*c| c.deinit(self.allocator);
        self.allocator.free(self.key_cols);
        self.loc_store.deinit(self.allocator);
        self.heap.deinit(self.allocator);
        self.allocator.free(self.decoded);
        self.allocator.free(self.probe_phys);
        self.allocator.free(self.probe_schema);
        self.allocator.free(self.key_phys);
        self.allocator.free(self.key_desc);
        self.allocator.free(self.key_probe_idx);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *ZonemapTopN) []const Column {
        return self.out_schema;
    }

    pub fn addPrune(self: *ZonemapTopN, pred: predicate.Predicate) !void {
        _ = self;
        _ = pred;
    }

    pub fn stats(self: *ZonemapTopN) exec.PipelineStats {
        return self.late.stats();
    }

    pub fn accountant(self: *ZonemapTopN) ?*exec.memory.MemoryAccountant {
        return self.late.accountant();
    }

    pub fn explain(self: *ZonemapTopN, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "ZonemapTopN (block-skipping top-N)");
        // Render the reused late-materialization sub-tree (its "LateScan" line
        // included) so the plan reflects that the wide columns are fetched by
        // location for the surviving top-N rows.
        try self.late.explain(out, allocator, depth + 1);
    }

    pub fn next(self: *ZonemapTopN) !?Batch {
        if (self.done) return null;
        self.done = true;

        const locs = try self.collectTopK();
        defer self.allocator.free(locs);
        if (locs.len == 0) {
            return Batch{ .schema = self.out_schema, .values = &.{}, .row_count = 0 };
        }

        try self.late.materializeInto(locs, self.inner_scan.memtableSnap());
        const out = self.late.outputColumns();
        for (out.columns, 0..) |c, i| self.late.views[i] = c.view();
        return Batch{
            .schema = out.schema,
            .values = self.late.views[0..out.columns.len],
            .row_count = out.columns[0].rowCount(),
        };
    }

    /// Run the zonemap heap and return the final sorted survivor `__rowloc`s
    /// (after dropping OFFSET, keeping n), caller-owned.
    fn collectTopK(self: *ZonemapTopN) ![]i64 {
        const refs = try self.enumerateRowGroups();
        defer self.allocator.free(refs);
        std.sort.pdq(RgRef, refs, self, cornerLess);

        for (refs) |ref| {
            if (self.keep > 0 and self.heap.items.len >= self.keep and self.cornerWorseThanWorst(ref.corner)) break;
            try self.processSegmentRowGroup(ref);
        }

        try self.processMemtable();
        return self.finalize();
    }

    /// Read the leading-key min/max footer stat for each (segment, row group),
    /// computing the best-corner. A segment that can't be opened propagates the
    /// error (correctness over silent skip).
    fn enumerateRowGroups(self: *ZonemapTopN) ![]RgRef {
        var list: std.ArrayListUnmanaged(RgRef) = .empty;
        errdefer list.deinit(self.allocator);

        var seg_idx: usize = 0;
        while (seg_idx < self.segment_count) : (seg_idx += 1) {
            const entry = self.table.manifest.segments.items[seg_idx];
            var name_buf: [32]u8 = undefined;
            const file_name = try Table.segmentFileName(&name_buf, entry.segment_id);
            var seg = try storage.readSegment(
                self.allocator,
                self.table.io,
                self.table.segments_dir,
                file_name,
                self.table.schema,
            );
            defer seg.deinit();

            for (seg.info.row_groups, 0..) |rg, rg_idx| {
                const s = rg.stats[self.leading_phys];
                const corner: i128 = if (self.leading_desc) s.max else s.min;
                try list.append(self.allocator, .{
                    .seg_idx = seg_idx,
                    .rg_idx = rg_idx,
                    .row_count = rg.row_count,
                    .corner = corner,
                });
            }
        }
        return list.toOwnedSlice(self.allocator);
    }

    /// best-corner visit order: most-preferred first (ASC → smaller corner
    /// first; DESC → larger corner first). Only the leading key is encoded.
    fn cornerLess(self: *ZonemapTopN, a: RgRef, b: RgRef) bool {
        if (a.corner == b.corner) return false;
        return if (self.leading_desc) a.corner > b.corner else a.corner < b.corner;
    }

    /// True when a row group whose best-corner leading value is `corner` cannot
    /// beat the current worst-kept row — its smallest (ASC) / largest (DESC)
    /// possible leading value already sorts strictly after the worst-kept's
    /// leading key. Conservative on equality (returns false) so a tie-on-leading
    /// RG is still processed (secondary keys could displace the incumbent).
    fn cornerWorseThanWorst(self: *ZonemapTopN, corner: i128) bool {
        const worst_lead = leadingValueI128(self.key_cols[0], self.heap.items[0]);
        return if (self.leading_desc) corner < worst_lead else corner > worst_lead;
    }

    fn processSegmentRowGroup(self: *ZonemapTopN, ref: RgRef) !void {
        const entry = self.table.manifest.segments.items[ref.seg_idx];
        var name_buf: [32]u8 = undefined;
        const file_name = try Table.segmentFileName(&name_buf, entry.segment_id);
        var seg = try storage.readSegment(
            self.allocator,
            self.table.io,
            self.table.segments_dir,
            file_name,
            self.table.schema,
        );
        defer seg.deinit();

        const rg_count = ref.row_count;
        for (self.probe_phys, 0..) |phys, j| {
            self.decoded[j] = try seg.decodeColumnMaybeCached(
                self.allocator,
                self.table.schema,
                ref.rg_idx,
                phys,
                &self.table.cache,
            );
        }
        self.decoded_valid = true;
        defer self.releaseDecoded();

        const vbuf = try self.allocator.alloc(ColumnView, self.probe_phys.len);
        defer self.allocator.free(vbuf);
        for (self.decoded[0..self.probe_phys.len], 0..) |c, i| vbuf[i] = c.view();

        const mask = try self.allocator.alloc(bool, rg_count);
        defer self.allocator.free(mask);
        try self.evalPredicate(vbuf, rg_count, mask);
        try self.applyTombstones(entry.segment_id, ref.rg_idx, seg, rg_count, mask);

        for (mask, 0..) |keep, row| {
            if (!keep) continue;
            try self.pushCandidate(vbuf, row, rowloc.packSegment(ref.seg_idx, ref.rg_idx, row));
        }
    }

    fn processMemtable(self: *ZonemapTopN) !void {
        const snap = self.inner_scan.memtableSnap();
        const n: usize = @intCast(self.inner_scan.memtable_row_count);
        if (n == 0) return;

        const vbuf = try self.allocator.alloc(ColumnView, self.probe_phys.len);
        defer self.allocator.free(vbuf);
        for (self.probe_phys, 0..) |phys, i| vbuf[i] = snap.columns[phys].view();

        const mask = try self.allocator.alloc(bool, n);
        defer self.allocator.free(mask);
        try self.evalPredicate(vbuf, n, mask);

        for (mask, 0..) |keep, row| {
            if (!keep) continue;
            try self.pushCandidate(vbuf, row, rowloc.packMemtable(row));
        }
    }

    /// Evaluate the WHERE predicate over a probe batch into `mask` (true =
    /// keep). NULL handling matches the reference Filter path: `evaluateExprGuided`
    /// clears a leaf's bit on a NULL row.
    fn evalPredicate(self: *ZonemapTopN, views: []const ColumnView, n: usize, mask: []bool) !void {
        const batch = Batch{ .schema = self.probe_schema, .values = views, .row_count = n };
        try predicate.evaluateExprGuided(self.allocator, self.pred, self.probe_schema, batch, mask, null);
    }

    fn applyTombstones(
        self: *ZonemapTopN,
        segment_id: u64,
        rg_idx: usize,
        seg: storage.ReadSegment,
        rg_count: u32,
        mask: []bool,
    ) !void {
        const tombs = try storage.tombstone.read(self.allocator, self.table.io, self.table.segments_dir, segment_id);
        defer if (tombs) |t| self.allocator.free(t);
        const t = tombs orelse return;
        if (t.len == 0) return;

        var rg_first: u32 = 0;
        for (seg.info.row_groups[0..rg_idx]) |rg| rg_first += rg.row_count;
        const rg_end = rg_first + rg_count;
        for (t) |off| {
            if (off >= rg_first and off < rg_end) mask[off - rg_first] = false;
        }
    }

    /// Push one survivor (its ORDER BY key values + packed loc) into the bounded
    /// max-heap. Below `keep`: append + sift up. At capacity: if the candidate
    /// is NOT worse than the worst-kept (root), append it, replace the root with
    /// it, sift down (the old root becomes a dead row); else drop.
    fn pushCandidate(self: *ZonemapTopN, views: []const ColumnView, row: usize, loc: i64) !void {
        if (self.keep == 0) return;

        if (self.heap.items.len < self.keep) {
            const slot = try self.appendCandidateRow(views, row, loc);
            try self.heap.append(self.allocator, slot);
            self.siftUp(self.heap.items.len - 1);
            return;
        }

        const worst_row = self.heap.items[0];
        if (self.candidateWorseThanRow(views, row, worst_row)) return;

        const slot = try self.appendCandidateRow(views, row, loc);
        self.heap.items[0] = slot;
        self.siftDown(0);
        self.compactIfNeeded();
    }

    /// Append the candidate's per-key values + loc as a fresh row; return its
    /// row index.
    fn appendCandidateRow(self: *ZonemapTopN, views: []const ColumnView, row: usize, loc: i64) !u32 {
        const slot: u32 = @intCast(self.loc_store.items.len);
        for (self.key_cols, 0..) |*kc, i| {
            try transform.appendOneRow(self.allocator, views[self.key_probe_idx[i]], row, kc);
        }
        try self.loc_store.append(self.allocator, loc);
        return slot;
    }

    /// True when candidate (`views`,`row`) sorts strictly AFTER stored row
    /// `other` — i.e. it is worse and would be rejected when the heap is full.
    /// Equal tuples return false (a tie can displace, matching `TopN.isCandidate`).
    fn candidateWorseThanRow(self: *ZonemapTopN, views: []const ColumnView, row: usize, other: u32) bool {
        for (self.key_cols, 0..) |kc, i| {
            const ord = transform.compareViewRows(views[self.key_probe_idx[i]], row, kc.view(), other);
            if (ord == .lt) return self.key_desc[i];
            if (ord == .gt) return !self.key_desc[i];
        }
        return false;
    }

    /// Heap order: is stored row `a` WORSE than stored row `b` (sorts after it
    /// under the ORDER BY direction)? The max-heap keeps the worst at the root.
    fn rowWorse(self: *ZonemapTopN, a: u32, b: u32) bool {
        for (self.key_cols, 0..) |kc, i| {
            const ord = transform.compareInColumn(kc, a, b);
            if (ord == .lt) return self.key_desc[i];
            if (ord == .gt) return !self.key_desc[i];
        }
        return false;
    }

    fn siftUp(self: *ZonemapTopN, start: usize) void {
        var idx = start;
        while (idx > 0) {
            const parent = (idx - 1) / 2;
            if (self.rowWorse(self.heap.items[idx], self.heap.items[parent])) {
                std.mem.swap(u32, &self.heap.items[idx], &self.heap.items[parent]);
                idx = parent;
            } else break;
        }
    }

    fn siftDown(self: *ZonemapTopN, start: usize) void {
        const len = self.heap.items.len;
        var idx = start;
        while (true) {
            const l = 2 * idx + 1;
            const r = 2 * idx + 2;
            var worst = idx;
            if (l < len and self.rowWorse(self.heap.items[l], self.heap.items[worst])) worst = l;
            if (r < len and self.rowWorse(self.heap.items[r], self.heap.items[worst])) worst = r;
            if (worst == idx) break;
            std.mem.swap(u32, &self.heap.items[idx], &self.heap.items[worst]);
            idx = worst;
        }
    }

    /// Rebuild the candidate store down to just the live heap rows once dead
    /// rows dominate (store grown past 2×keep), so memory stays O(keep). Heap
    /// indices are remapped to the compacted positions.
    fn compactIfNeeded(self: *ZonemapTopN) void {
        if (self.keep == std.math.maxInt(usize)) return;
        const cap = std.math.mul(usize, self.keep, 2) catch return;
        if (self.loc_store.items.len <= cap) return;
        self.compact() catch {}; // best-effort; correctness unaffected if it no-ops
    }

    fn compact(self: *ZonemapTopN) !void {
        const live = self.heap.items.len;
        var fresh = try self.allocator.alloc(ColumnStore, self.key_cols.len);
        var finited: usize = 0;
        errdefer {
            for (fresh[0..finited]) |*c| c.deinit(self.allocator);
            self.allocator.free(fresh);
        }
        for (self.key_cols, 0..) |src, i| {
            const c = self.table.schema.columns[self.key_phys[i]];
            fresh[i] = try ColumnStore.init(self.allocator, c.type, c.nullable);
            finited += 1;
            _ = src;
        }
        var fresh_locs: std.ArrayListUnmanaged(i64) = .empty;
        errdefer fresh_locs.deinit(self.allocator);
        try fresh_locs.ensureTotalCapacity(self.allocator, live);

        // Append each live heap row (in heap order) into the fresh store; the
        // heap's row index becomes its new position.
        for (self.heap.items, 0..) |row, new_idx| {
            for (self.key_cols, 0..) |src, i| {
                const one = [_]u32{row};
                try transform.appendByIndices(self.allocator, src.view(), &one, &fresh[i]);
            }
            fresh_locs.appendAssumeCapacity(self.loc_store.items[row]);
            self.heap.items[new_idx] = @intCast(new_idx);
        }

        for (self.key_cols) |*c| c.deinit(self.allocator);
        self.allocator.free(self.key_cols);
        self.key_cols = fresh;
        self.loc_store.deinit(self.allocator);
        self.loc_store = fresh_locs;
    }

    /// Sort the live heap rows by the comparator, drop the first `offset`, keep
    /// `n`, and return their locs in emit order (caller-owned).
    fn finalize(self: *ZonemapTopN) ![]i64 {
        const total = self.heap.items.len;
        if (total == 0) return self.allocator.alloc(i64, 0);

        // Sort the live heap row indices best-first.
        const perm = try self.allocator.alloc(u32, total);
        defer self.allocator.free(perm);
        @memcpy(perm, self.heap.items);
        std.sort.pdq(u32, perm, self, permLess);

        const start = @min(self.offset, total);
        const end = @min(std.math.add(usize, self.offset, self.n) catch total, total);
        const out_n = if (end > start) end - start else 0;
        const out = try self.allocator.alloc(i64, out_n);
        for (out, 0..) |*o, i| o.* = self.loc_store.items[perm[start + i]];
        return out;
    }

    /// Ascending sort comparator (best-first), identical to `TopN.Comparator`.
    fn permLess(self: *ZonemapTopN, a: u32, b: u32) bool {
        for (self.key_cols, 0..) |kc, i| {
            const ord = transform.compareInColumn(kc, a, b);
            if (ord == .lt) return !self.key_desc[i];
            if (ord == .gt) return self.key_desc[i];
        }
        return false;
    }

    fn releaseDecoded(self: *ZonemapTopN) void {
        if (self.decoded_valid) {
            for (self.decoded) |*c| c.deinit(self.allocator);
            self.decoded_valid = false;
        }
    }
};

/// Leading order-key types with a usable, order-preserving i128 footer min/max:
/// non-nullable numeric/temporal. Excludes float/double (no stats), strings
/// (`{0,0}`), uuid/largeint (the v1 gate keeps leading-key pruning to numeric/
/// temporal whose i128 corner is a faithful bound).
fn leadingKeyTypeSupported(t: types.Type) bool {
    return switch (t) {
        .int, .smallint, .tinyint, .bigint, .date, .datetime, .decimal64, .decimal128 => true,
        else => false,
    };
}

/// Read stored row `r`'s leading-key value as an i128 (same domain as the
/// footer `Stats`), for the corner comparison. Only called on supported types.
fn leadingValueI128(col: ColumnStore, r: u32) i128 {
    return switch (col.data) {
        .int => |l| l.items[r],
        .smallint => |l| l.items[r],
        .tinyint => |l| l.items[r],
        .bigint => |l| l.items[r],
        .date => |l| l.items[r],
        .datetime => |l| l.items[r],
        .decimal64 => |l| l.items[r],
        .decimal128 => |l| l.items[r],
        else => unreachable,
    };
}
