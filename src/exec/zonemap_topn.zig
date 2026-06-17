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
//!   1. Enumerate every (segment, row_group). For each, read the footer min/max
//!      of every key in the prunable PREFIX — the longest leading run of order
//!      keys that are non-nullable with usable footer stats (any stat-bearing
//!      type; strings prune at 16-byte-prefix granularity, floats use the
//!      NaN-last total order).
//!   2. best-corner = per prefix key, the most-preferred value the RG could
//!      hold: ASC → min, DESC → max. This per-key-independent tuple is still a
//!      valid lexicographic lower bound on every row's key tuple, because lex
//!      compares left-to-right and each component is independently bounded.
//!      Keys past the prefix (nullable) don't prune but still order rows in the
//!      heap comparator.
//!   3. Sort the RG list by best-corner, lexicographically over the prefix
//!      (per-key direction).
//!   4. Bounded max-heap of the K best rows (worst at root), keyed by the FULL
//!      ORDER BY comparator over actual decoded values (`compareInColumn` +
//!      per-key `desc`, identical to `TopN.Comparator`).
//!   5. Visit RGs in best-corner order. Once the heap holds K rows AND this
//!      RG's best-corner is lexicographically worse than the heap's worst-kept
//!      prefix tuple → STOP (every later RG is also worse).
//!   6. Else decode this RG's probe columns, apply WHERE + tombstones, push
//!      each survivor (sort keys + packed `__rowloc`) into the heap.
//!   7. The memtable (no zonemap) is always scanned — never skippable.
//!   8. Sort the heap's survivors, drop `m`, keep `n`.
//!   9. Materialize the WIDE output columns by `__rowloc` via `LateScan`.
//!
//! CORRECTNESS: best-corner is a provable most-preferred bound on the prefix
//! key tuple of every row in the RG. Once it's lexicographically worse than the
//! K-th best already held, the RG can't improve the answer; visiting in
//! best-corner order makes "this RG fails ⇒ all later fail" hold. The heap comparator and final
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
const HarnessCore = exec.group_topn_harness_core;
const core_scheduler = @import("../util/core_scheduler.zig");

/// One row group's identity + the offset of its best-corner prefix tuple in the
/// flat `corners` buffer (`prefix_len` consecutive i128s).
const RgRef = struct {
    seg_idx: usize,
    rg_idx: usize,
    row_count: u32,
    corner_off: usize,
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
    /// Prunable prefix length: the longest leading run of ORDER BY keys with
    /// non-nullable stat-bearing footer stats (≥1; see `leadingKeyTypeSupported`).
    /// Only these keys drive row-group pruning; the rest still order rows in the
    /// heap comparator.
    prefix_len: usize,
    /// Flat best-corner store, `prefix_len` i128s per row group. Scratch for one
    /// `collectTopK`; allocated in `enumerateRowGroups`, freed at its end.
    corners: []i128 = &.{},

    n: usize,
    offset: usize,
    /// K = n + m (saturating).
    keep: usize,

    /// Worker count for the row-group visit. Workers claim row groups off the
    /// globally-sorted corner list and keep PRIVATE candidate heaps — a stale
    /// (loose) local threshold only over-accepts, never over-rejects, so no
    /// locking is needed. The early stop survives parallelism: a worker's
    /// local K-th best is worse-or-equal to the global K-th best, so when ITS
    /// stop condition fires on the sorted claim order, the global condition
    /// holds for every later row group too — one shared flag stops everyone.
    dop: usize,

    /// Main candidate heap: receives the memtable rows and the merged
    /// per-worker survivors; `finalize` reads it.
    main: CandidateHeap,

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
        dop: usize,
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

        // Prunable prefix: the leading key is already validated as prunable;
        // extend while each subsequent key is also non-nullable with usable
        // footer stats. The first nullable key caps the prefix.
        var prefix_len: usize = 1;
        while (prefix_len < order_specs.len) : (prefix_len += 1) {
            const c = table.schema.columns[key_phys[prefix_len]];
            if (c.nullable or !leadingKeyTypeSupported(c.type)) break;
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
        const scan_ptr = try Scan.allocWithProjectionLoc(allocator, table, accountant_ptr, probe_names, true, null);
        var inner = makeQuery(allocator, scan_ptr);
        var late_built = false;
        errdefer if (!late_built) inner.deinit();

        inner = try inner.filter(pred);
        inner = try inner.topN(order_specs, n, offset);
        var late_q = try LateScan.create(allocator, inner, scan_ptr, table, output_names);
        late_built = true;
        errdefer late_q.deinit();
        const late: *LateScan = @ptrCast(@alignCast(late_q.ptr));

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
            .prefix_len = prefix_len,
            .n = n,
            .offset = offset,
            .keep = keep,
            .dop = @max(dop, 1),
            .main = undefined,
            .out_schema = late.outputSchema(),
        };
        self.main = try CandidateHeap.init(self, allocator);
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *ZonemapTopN) void {
        self.late.deinit();
        self.main.deinit();
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
        defer {
            self.allocator.free(self.corners);
            self.corners = &.{};
        }
        std.sort.pdq(RgRef, refs, self, cornerLess);

        if (refs.len > 0) try self.visitRowGroups(refs);
        try self.processMemtable();
        return self.finalize();
    }

    /// Visit the sorted row-group list: workers claim entries off a shared
    /// atomic cursor, fold survivors into PRIVATE heaps, and any worker whose
    /// local stop condition fires sets the shared stop flag (its threshold is
    /// conservative w.r.t. the global one, so the proof carries). Per-worker
    /// survivors then merge into the main heap. dop == 1 runs the same claim
    /// loop inline on the main heap — the serial reference behavior.
    fn visitRowGroups(self: *ZonemapTopN, refs: []const RgRef) !void {
        const allocator = self.allocator;
        var layout = HarnessCore.cpuLayout(allocator) catch HarnessCore.CpuLayout{ .order = &.{}, .physical_count = 0 };
        defer layout.deinit(allocator);
        const cpu_count = @max(@as(usize, 1), layout.order.len);
        const n_workers = @max(@as(usize, 1), @min(self.dop, @min(cpu_count, refs.len)));

        var next_ref = std.atomic.Value(usize).init(0);
        var stop = std.atomic.Value(bool).init(false);

        if (n_workers == 1) {
            var w: ZWorker = .{
                .z = self,
                .allocator = allocator,
                .cpu = null,
                .ch = &self.main,
                .refs = refs,
                .next_ref = &next_ref,
                .stop = &stop,
            };
            return zWorkerRun(&w);
        }

        const workers = try allocator.alloc(ZWorker, n_workers);
        defer allocator.free(workers);
        const heaps = try allocator.alloc(CandidateHeap, n_workers);
        defer allocator.free(heaps);
        var built: usize = 0;
        defer for (heaps[0..built]) |*h| h.deinit();
        for (heaps) |*h| {
            h.* = try CandidateHeap.init(self, allocator);
            built += 1;
        }
        for (workers, heaps, 0..) |*w, *h, i| {
            w.* = .{
                .z = self,
                .allocator = allocator,
                .cpu = if (layout.order.len == 0) null else layout.order[i % layout.order.len],
                .ch = h,
                .refs = refs,
                .next_ref = &next_ref,
                .stop = &stop,
            };
        }

        const threads = try allocator.alloc(std.Thread, n_workers);
        defer allocator.free(threads);
        const spawned = try allocator.alloc(bool, n_workers);
        defer allocator.free(spawned);
        @memset(spawned, false);
        for (workers, 0..) |*w, i| {
            if (std.Thread.spawn(.{}, zWorkerMain, .{w})) |th| {
                threads[i] = th;
                spawned[i] = true;
            } else |_| {}
        }
        for (workers, 0..) |*w, i| if (!spawned[i]) {
            zWorkerRun(w) catch |e| {
                w.err = e;
            };
        };
        for (0..n_workers) |i| if (spawned[i]) threads[i].join();
        for (workers) |*w| if (w.err) |e| return e;

        for (heaps) |*h| try self.mergeHeapInto(h);
    }

    /// Fold a worker's surviving candidates into the main heap. The push path
    /// reads views only at the key positions, so a probe-shaped view buffer
    /// with just those slots populated suffices.
    fn mergeHeapInto(self: *ZonemapTopN, src: *CandidateHeap) !void {
        if (src.heap.items.len == 0) return;
        const vbuf = try self.allocator.alloc(ColumnView, self.probe_phys.len);
        defer self.allocator.free(vbuf);
        for (self.key_probe_idx, 0..) |pi, i| vbuf[pi] = src.key_cols[i].view();
        for (src.heap.items) |row| {
            try self.main.pushCandidate(vbuf, row, src.loc_store.items[row]);
        }
    }

    /// Read the prefix keys' min/max footer stats for each (segment, row group),
    /// building the flat best-corner store (`self.corners`) and the RG list. A
    /// segment that can't be opened propagates the error (correctness over
    /// silent skip).
    fn enumerateRowGroups(self: *ZonemapTopN) ![]RgRef {
        var list: std.ArrayListUnmanaged(RgRef) = .empty;
        errdefer list.deinit(self.allocator);
        var corners: std.ArrayListUnmanaged(i128) = .empty;
        errdefer corners.deinit(self.allocator);

        // ASC string keys whose predicate excludes '' use the blank-excluded
        // min (the string `Stats.sum` slot): '' is the plain min of nearly
        // every string column, which makes all corners tie and kills both the
        // visit order and the early stop. Surviving rows are provably
        // non-blank, so the nonblank min still lower-bounds every survivor.
        const use_nonblank = try self.allocator.alloc(bool, self.prefix_len);
        defer self.allocator.free(use_nonblank);
        for (0..self.prefix_len) |k| {
            const col = self.table.schema.columns[self.key_phys[k]];
            use_nonblank[k] = !self.key_desc[k] and col.type.isString() and
                predicateExcludesBlank(self.pred, col.name);
        }

        var seg_idx: usize = 0;
        while (seg_idx < self.segment_count) : (seg_idx += 1) {
            const entry = self.table.manifest.segments.items[seg_idx];
            const handle = try self.table.acquireSegment(entry.segment_id);
            defer self.table.releaseSegment(handle);

            for (handle.seg.info.row_groups, 0..) |rg, rg_idx| {
                const off = corners.items.len;
                for (0..self.prefix_len) |k| {
                    const s = rg.stats[self.key_phys[k]];
                    const corner = if (self.key_desc[k])
                        s.max
                    else if (use_nonblank[k])
                        s.sum
                    else
                        s.min;
                    try corners.append(self.allocator, corner);
                }
                try list.append(self.allocator, .{
                    .seg_idx = seg_idx,
                    .rg_idx = rg_idx,
                    .row_count = rg.row_count,
                    .corner_off = off,
                });
            }
        }
        const refs = try list.toOwnedSlice(self.allocator);
        errdefer self.allocator.free(refs);
        self.corners = try corners.toOwnedSlice(self.allocator);
        return refs;
    }

    /// True when `expr` provably excludes the empty string for `col` — no
    /// surviving row can carry '' there. Conservative: only a top-level AND
    /// conjunct qualifying under `predicate.leafExcludesBlank` counts;
    /// anything else — OR branches, NOT, LIKE — returns false.
    fn predicateExcludesBlank(expr: PredicateExpr, col: []const u8) bool {
        return switch (expr) {
            .leaf => |p| types.columnNameEql(p.col, col) and
                predicate.leafExcludesBlank(p.op, p.val),
            .@"and" => |children| blk: {
                for (children) |c| {
                    if (predicateExcludesBlank(c, col)) break :blk true;
                }
                break :blk false;
            },
            else => false,
        };
    }

    /// The prefix best-corner tuple stored for a row group.
    fn cornerOf(self: *const ZonemapTopN, ref: RgRef) []const i128 {
        return self.corners[ref.corner_off..][0..self.prefix_len];
    }

    /// best-corner visit order: most-preferred first, lexicographically over the
    /// prefix (per-key ASC → smaller first; DESC → larger first).
    fn cornerLess(self: *const ZonemapTopN, a: RgRef, b: RgRef) bool {
        const ca = self.cornerOf(a);
        const cb = self.cornerOf(b);
        for (ca, cb, 0..) |av, bv, i| {
            if (av == bv) continue;
            return if (self.key_desc[i]) av > bv else av < bv;
        }
        return false;
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
        try evalPredicate(self, self.allocator, vbuf, n, mask);

        for (mask, 0..) |keep, row| {
            if (!keep) continue;
            try self.main.pushCandidate(vbuf, row, rowloc.packMemtable(row));
        }
    }

    /// Sort the live heap rows by the comparator, drop the first `offset`, keep
    /// `n`, and return their locs in emit order (caller-owned).
    fn finalize(self: *ZonemapTopN) ![]i64 {
        const total = self.main.heap.items.len;
        if (total == 0) return self.allocator.alloc(i64, 0);

        // Sort the live heap row indices best-first.
        const perm = try self.allocator.alloc(u32, total);
        defer self.allocator.free(perm);
        @memcpy(perm, self.main.heap.items);
        std.sort.pdq(u32, perm, &self.main, CandidateHeap.permLess);

        const start = @min(self.offset, total);
        const end = @min(std.math.add(usize, self.offset, self.n) catch total, total);
        const out_n = if (end > start) end - start else 0;
        const out = try self.allocator.alloc(i64, out_n);
        for (out, 0..) |*o, i| o.* = self.main.loc_store.items[perm[start + i]];
        return out;
    }
};

/// One worker of the parallel row-group visit: claims sorted refs off the
/// shared cursor, folds survivors into a private heap, and raises the shared
/// stop flag when its own (conservative) stop condition fires.
const ZWorker = struct {
    z: *const ZonemapTopN,
    allocator: Allocator,
    cpu: ?usize,
    ch: *CandidateHeap,
    refs: []const RgRef,
    next_ref: *std.atomic.Value(usize),
    stop: *std.atomic.Value(bool),
    err: ?anyerror = null,
};

fn zWorkerMain(w: *ZWorker) void {
    // Lease a core from the process-global scheduler (round-robins distinct
    // cores across ALL in-flight queries, global per-core cap) rather than
    // pinning to a per-query `cpus[worker_index]`, which would serialize N
    // concurrent single-worker queries on one core. tryAcquire (non-blocking):
    // workers coordinate, so none may block on a full bucket.
    var lease = core_scheduler.global().tryAcquire();
    defer lease.release();
    zWorkerRun(w) catch |e| {
        w.err = e;
    };
}

fn zWorkerRun(w: *ZWorker) !void {
    const z = w.z;
    const allocator = w.allocator;
    const decoded = try allocator.alloc(storage.OwnedColumn, z.probe_phys.len);
    defer allocator.free(decoded);

    // Per-worker segment + tombstone caches: the corner-sorted claim order
    // scatters across segments, and re-opening a segment (footer parse) plus a
    // tombstone file probe PER ROW GROUP dominated the degenerate full-visit
    // case. Each worker opens a segment and reads its tombstones at most once.
    var cache: SegCache = .{
        .entries = try allocator.alloc(?*storage.cache.SegmentHandles.Entry, z.segment_count),
        .tombs = try allocator.alloc(?[]u32, z.segment_count),
        .tombs_loaded = try allocator.alloc(bool, z.segment_count),
    };
    @memset(cache.entries, null);
    @memset(cache.tombs, null);
    @memset(cache.tombs_loaded, false);
    defer cache.deinit(z, allocator);

    while (true) {
        if (w.stop.load(.acquire)) break;
        const idx = w.next_ref.fetchAdd(1, .monotonic);
        if (idx >= w.refs.len) break;
        const ref = w.refs[idx];
        // Local stop: this worker's K-th best is worse-or-equal to the global
        // K-th best, so corner-worse-than-local implies corner-worse-than-
        // global — and the claim order is the sorted corner order, so every
        // later row group is worse still. Stop everyone.
        if (z.keep > 0 and w.ch.full() and w.ch.cornerWorseThanWorst(ref)) {
            w.stop.store(true, .release);
            break;
        }
        try processSegmentRowGroup(z, allocator, w.ch, decoded, &cache, ref);
    }
}

const SegCache = struct {
    entries: []?*storage.cache.SegmentHandles.Entry,
    tombs: []?[]u32,
    tombs_loaded: []bool,

    fn deinit(self: *SegCache, z: *const ZonemapTopN, allocator: Allocator) void {
        for (self.entries) |e| if (e) |entry| z.table.releaseSegment(entry);
        allocator.free(self.entries);
        for (self.tombs) |t| if (t) |tt| allocator.free(tt);
        allocator.free(self.tombs);
        allocator.free(self.tombs_loaded);
    }

    fn segment(self: *SegCache, z: *const ZonemapTopN, allocator: Allocator, seg_idx: usize) !*storage.ReadSegment {
        _ = allocator;
        if (self.entries[seg_idx] == null) {
            const entry = z.table.manifest.segments.items[seg_idx];
            self.entries[seg_idx] = try z.table.acquireSegment(entry.segment_id);
        }
        return &self.entries[seg_idx].?.seg;
    }

    fn tombstones(self: *SegCache, z: *const ZonemapTopN, allocator: Allocator, seg_idx: usize) !?[]const u32 {
        if (!self.tombs_loaded[seg_idx]) {
            _ = try self.segment(z, allocator, seg_idx);
            self.tombs[seg_idx] = try z.table.segmentTombstones(allocator, self.entries[seg_idx].?);
            self.tombs_loaded[seg_idx] = true;
        }
        return self.tombs[seg_idx];
    }
};

fn processSegmentRowGroup(z: *const ZonemapTopN, allocator: Allocator, ch: *CandidateHeap, decoded: []storage.OwnedColumn, cache: *SegCache, ref: RgRef) !void {
    const seg = try cache.segment(z, allocator, ref.seg_idx);

    const rg_count = ref.row_count;
    var decoded_n: usize = 0;
    defer for (decoded[0..decoded_n]) |*c| c.deinit(allocator);
    for (z.probe_phys, 0..) |phys, j| {
        decoded[j] = try seg.decodeColumnMaybeCached(
            allocator,
            z.table.schema,
            ref.rg_idx,
            phys,
            &z.table.cache,
        );
        decoded_n += 1;
    }

    const vbuf = try allocator.alloc(ColumnView, z.probe_phys.len);
    defer allocator.free(vbuf);
    for (decoded[0..z.probe_phys.len], 0..) |c, i| vbuf[i] = c.view();

    const mask = try allocator.alloc(bool, rg_count);
    defer allocator.free(mask);
    try evalPredicate(z, allocator, vbuf, rg_count, mask);
    try applyTombstones(z, allocator, cache, ref, seg.*, rg_count, mask);

    for (mask, 0..) |keep, row| {
        if (!keep) continue;
        try ch.pushCandidate(vbuf, row, rowloc.packSegment(ref.seg_idx, ref.rg_idx, row));
    }
}

/// Evaluate the WHERE predicate over a probe batch into `mask` (true =
/// keep). NULL handling matches the reference Filter path: `evaluateExprGuided`
/// clears a leaf's bit on a NULL row.
fn evalPredicate(z: *const ZonemapTopN, allocator: Allocator, views: []const ColumnView, n: usize, mask: []bool) !void {
    const batch = Batch{ .schema = z.probe_schema, .values = views, .row_count = n };
    try predicate.evaluateExprGuided(allocator, z.pred, z.probe_schema, batch, mask, null);
}

fn applyTombstones(
    z: *const ZonemapTopN,
    allocator: Allocator,
    cache: *SegCache,
    ref: RgRef,
    seg: storage.ReadSegment,
    rg_count: u32,
    mask: []bool,
) !void {
    const t = (try cache.tombstones(z, allocator, ref.seg_idx)) orelse return;
    if (t.len == 0) return;

    var rg_first: u32 = 0;
    for (seg.info.row_groups[0..ref.rg_idx]) |rg| rg_first += rg.row_count;
    const rg_end = rg_first + rg_count;
    for (t) |off| {
        if (off >= rg_first and off < rg_end) mask[off - rg_first] = false;
    }
}

/// Bounded candidate heap: an append-only store of each candidate's ORDER BY
/// key values (`key_cols`) + packed `__rowloc` (`loc_store`), and a binary
/// max-heap of row indices (worst-kept at root). Rows that fall out of the
/// heap become dead; `compact` periodically rebuilds the store down to the
/// live rows so memory stays O(keep). One instance per visit worker (private,
/// no locks) plus the main one that collects the memtable + the merge.
const CandidateHeap = struct {
    z: *const ZonemapTopN,
    allocator: Allocator,
    key_cols: []ColumnStore,
    loc_store: std.ArrayListUnmanaged(i64) = .empty,
    heap: std.ArrayListUnmanaged(u32) = .empty,

    fn init(z: *const ZonemapTopN, allocator: Allocator) !CandidateHeap {
        const key_cols = try allocator.alloc(ColumnStore, z.key_phys.len);
        errdefer allocator.free(key_cols);
        var inited: usize = 0;
        errdefer for (key_cols[0..inited]) |*c| c.deinit(allocator);
        for (z.key_phys, 0..) |phys, i| {
            const c = z.table.schema.columns[phys];
            key_cols[i] = try ColumnStore.init(allocator, c.type, c.nullable);
            inited += 1;
        }
        return .{ .z = z, .allocator = allocator, .key_cols = key_cols };
    }

    fn deinit(self: *CandidateHeap) void {
        for (self.key_cols) |*c| c.deinit(self.allocator);
        self.allocator.free(self.key_cols);
        self.loc_store.deinit(self.allocator);
        self.heap.deinit(self.allocator);
    }

    fn full(self: *const CandidateHeap) bool {
        return self.heap.items.len >= self.z.keep;
    }

    /// True when a row group cannot beat this heap's current worst-kept row —
    /// its best-corner prefix tuple already sorts strictly after the worst-
    /// kept's prefix-key tuple under the per-key directions. Conservative on a
    /// full tie (returns false) so a tie-on-prefix RG is still processed
    /// (later keys could displace the incumbent).
    fn cornerWorseThanWorst(self: *const CandidateHeap, ref: RgRef) bool {
        const z = self.z;
        const worst = self.heap.items[0];
        const corner = z.cornerOf(ref);
        for (corner, 0..) |c, i| {
            const w = keyValueI128(self.key_cols[i], worst);
            if (c == w) continue;
            return if (z.key_desc[i]) c < w else c > w;
        }
        return false;
    }

    /// Push one survivor (its ORDER BY key values + packed loc) into the bounded
    /// max-heap. Below `keep`: append + sift up. At capacity: if the candidate
    /// is NOT worse than the worst-kept (root), append it, replace the root with
    /// it, sift down (the old root becomes a dead row); else drop.
    fn pushCandidate(self: *CandidateHeap, views: []const ColumnView, row: usize, loc: i64) !void {
        if (self.z.keep == 0) return;

        if (self.heap.items.len < self.z.keep) {
            const slot = try self.appendCandidateRow(views, row, loc);
            try self.heap.append(self.allocator, slot);
            self.siftUp(self.heap.items.len - 1);
            return;
        }

        const worst_row = self.heap.items[0];
        if (self.candidateWorseThanRow(views, row, loc, worst_row)) return;

        const slot = try self.appendCandidateRow(views, row, loc);
        self.heap.items[0] = slot;
        self.siftDown(0);
        self.compactIfNeeded();
    }

    /// Append the candidate's per-key values + loc as a fresh row; return its
    /// row index.
    fn appendCandidateRow(self: *CandidateHeap, views: []const ColumnView, row: usize, loc: i64) !u32 {
        const slot: u32 = @intCast(self.loc_store.items.len);
        for (self.key_cols, 0..) |*kc, i| {
            try transform.appendOneRow(self.allocator, views[self.z.key_probe_idx[i]], row, kc);
        }
        try self.loc_store.append(self.allocator, loc);
        return slot;
    }

    /// True when candidate (`views`,`row`,`loc`) is NOT strictly better than
    /// stored row `other` — i.e. it would be rejected when the heap is full.
    /// Full key ties break on the storage location (smaller wins), making the
    /// kept set a total order — DETERMINISTIC and independent of visit order,
    /// so the parallel workers and the serial reference keep identical rows.
    /// (The reference `TopN` converges to the same set: its arrival order IS
    /// storage order and it keeps the earliest arrival among ties.)
    fn candidateWorseThanRow(self: *const CandidateHeap, views: []const ColumnView, row: usize, loc: i64, other: u32) bool {
        for (self.key_cols, 0..) |kc, i| {
            const ord = transform.compareViewRows(views[self.z.key_probe_idx[i]], row, kc.view(), other);
            if (ord == .lt) return self.z.key_desc[i];
            if (ord == .gt) return !self.z.key_desc[i];
        }
        return loc > self.loc_store.items[other];
    }

    /// Heap order: is stored row `a` WORSE than stored row `b` (sorts after it
    /// under the ORDER BY direction)? The max-heap keeps the worst at the root.
    /// Key ties break on storage location, matching `candidateWorseThanRow`.
    fn rowWorse(self: *const CandidateHeap, a: u32, b: u32) bool {
        for (self.key_cols, 0..) |kc, i| {
            const ord = transform.compareInColumn(kc, a, b);
            if (ord == .lt) return self.z.key_desc[i];
            if (ord == .gt) return !self.z.key_desc[i];
        }
        return self.loc_store.items[a] > self.loc_store.items[b];
    }

    /// Ascending sort comparator (best-first), `TopN.Comparator` plus the
    /// location tie-break for a deterministic emit order.
    fn permLess(self: *CandidateHeap, a: u32, b: u32) bool {
        for (self.key_cols, 0..) |kc, i| {
            const ord = transform.compareInColumn(kc, a, b);
            if (ord == .lt) return !self.z.key_desc[i];
            if (ord == .gt) return self.z.key_desc[i];
        }
        return self.loc_store.items[a] < self.loc_store.items[b];
    }

    fn siftUp(self: *CandidateHeap, start: usize) void {
        var idx = start;
        while (idx > 0) {
            const parent = (idx - 1) / 2;
            if (self.rowWorse(self.heap.items[idx], self.heap.items[parent])) {
                std.mem.swap(u32, &self.heap.items[idx], &self.heap.items[parent]);
                idx = parent;
            } else break;
        }
    }

    fn siftDown(self: *CandidateHeap, start: usize) void {
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
    fn compactIfNeeded(self: *CandidateHeap) void {
        if (self.z.keep == std.math.maxInt(usize)) return;
        const cap = std.math.mul(usize, self.z.keep, 2) catch return;
        if (self.loc_store.items.len <= cap) return;
        self.compact() catch {}; // best-effort; correctness unaffected if it no-ops
    }

    fn compact(self: *CandidateHeap) !void {
        const live = self.heap.items.len;
        var fresh = try self.allocator.alloc(ColumnStore, self.key_cols.len);
        var finited: usize = 0;
        errdefer {
            for (fresh[0..finited]) |*c| c.deinit(self.allocator);
            self.allocator.free(fresh);
        }
        for (0..self.key_cols.len) |i| {
            const c = self.z.table.schema.columns[self.z.key_phys[i]];
            fresh[i] = try ColumnStore.init(self.allocator, c.type, c.nullable);
            finited += 1;
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
};

/// Leading/prefix order-key types that prune via the footer i128 min/max. Every
/// stat-bearing type qualifies — `keyValueI128` mirrors the writer's per-type
/// encoding so corners and heap rows live in the same i128 space. Strings prune
/// at 16-byte-prefix granularity (sound: the writer's min/max are the true prefix
/// classes, and `cornerWorseThanWorst` is conservative on ties, so a prefix
/// collision just processes the row group). Floats use the NaN-last total order
/// (`types.floatOrder` / `encodeFloatOrder`), so the corner bound and the heap
/// comparator agree — a NaN row raises the row group's `max` to the NaN sentinel,
/// keeping it from being wrongly pruned.
fn leadingKeyTypeSupported(t: types.Type) bool {
    return storage.format.typeHasStats(t);
}

/// Read stored row `r`'s key value as an i128 in the same per-type encoding the
/// footer `Stats` use, for the corner comparison. Only called on prefix keys,
/// which `leadingKeyTypeSupported` guarantees are stat-bearing types.
fn keyValueI128(col: ColumnStore, r: u32) i128 {
    return switch (col.data) {
        .int => |l| l.items[r],
        .smallint => |l| l.items[r],
        .tinyint => |l| l.items[r],
        .bigint => |l| l.items[r],
        .boolean => |l| l.items[r],
        .date => |l| l.items[r],
        .datetime => |l| l.items[r],
        .decimal64 => |l| l.items[r],
        .decimal128 => |l| l.items[r],
        .largeint => |l| l.items[r],
        .uuid => |l| storage.format.encodeUnsignedU128(l.items[r]),
        .string, .varchar, .char => |s| storage.format.encodeStringPrefix(s.view().rowBytes(r)),
        .float => |l| storage.format.encodeFloatOrder(@as(f64, l.items[r])),
        .double => |l| storage.format.encodeFloatOrder(l.items[r]),
    };
}

test "predicateExcludesBlank accepts only provable non-blank conjuncts" {
    const t = std.testing;
    const blank = predicate.leafExpr("s", .neq, .{ .text = "" });
    try t.expect(ZonemapTopN.predicateExcludesBlank(blank, "s"));
    try t.expect(!ZonemapTopN.predicateExcludesBlank(blank, "other"));

    try t.expect(ZonemapTopN.predicateExcludesBlank(predicate.leafExpr("s", .gt, .{ .text = "" }), "s"));
    try t.expect(ZonemapTopN.predicateExcludesBlank(predicate.leafExpr("s", .gt, .{ .text = "m" }), "s"));
    try t.expect(ZonemapTopN.predicateExcludesBlank(predicate.leafExpr("s", .eq, .{ .text = "x" }), "s"));
    try t.expect(ZonemapTopN.predicateExcludesBlank(predicate.leafExpr("s", .gte, .{ .text = "a" }), "s"));

    // '' itself satisfies these — must NOT qualify.
    try t.expect(!ZonemapTopN.predicateExcludesBlank(predicate.leafExpr("s", .eq, .{ .text = "" }), "s"));
    try t.expect(!ZonemapTopN.predicateExcludesBlank(predicate.leafExpr("s", .gte, .{ .text = "" }), "s"));
    try t.expect(!ZonemapTopN.predicateExcludesBlank(predicate.leafExpr("s", .lt, .{ .text = "z" }), "s"));
    try t.expect(!ZonemapTopN.predicateExcludesBlank(predicate.leafExpr("s", .neq, .{ .text = "x" }), "s"));

    // AND: any qualifying conjunct suffices. OR: never.
    const conj = [_]PredicateExpr{ predicate.leafExpr("k", .gt, .{ .bigint = 5 }), blank };
    try t.expect(ZonemapTopN.predicateExcludesBlank(.{ .@"and" = &conj }, "s"));
    try t.expect(!ZonemapTopN.predicateExcludesBlank(.{ .@"or" = &conj }, "s"));
}
