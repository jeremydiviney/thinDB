//! Late-materialization operator.
//!
//! ClickBench Q23 — `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY
//! EventTime LIMIT 10` over a wide table — only keeps 10 of millions of rows,
//! yet the naive `TopN → Filter → Scan(all columns)` plan decodes every wide
//! column for every row before the filter even runs. Late materialization
//! splits that: the inner pipeline decodes only the probe columns (filter ∪
//! ORDER BY) plus a hidden `__rowloc` per row, runs the filter + bounded
//! top-k, and yields just the ≤ k survivors' locations. This operator then
//! fetches the WIDE output columns for only those survivors.
//!
//! The inner pipeline is `Scan(probe, emit_loc) → Filter(pred) →
//! TopN | Limit`, all built from the existing operators. TopN (or Limit when
//! there's no ORDER BY) already carries `__rowloc` through as a normal column,
//! so this operator just reads the location of each survivor and rebuilds the
//! output batch by re-decoding the few row groups they live in (cheap — the
//! row-group cache absorbs repeats) and reading the matching memtable rows.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;

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

const rowloc = @import("rowloc.zig");
const Scan = @import("scan.zig").Scan;
const parallel = @import("../util/parallel.zig");
const core_scheduler = @import("../util/core_scheduler.zig");

pub const LateScan = struct {
    allocator: Allocator,
    /// Inner pipeline: Scan(emit_loc) → Filter → TopN|Limit. Owns the ddl_lock
    /// + memtable pin (released when this is deinit'd). Kept alive until after
    /// the fetch so `inner_scan.memtableSnap()` stays valid.
    inner: Query,
    /// Bottom Scan of `inner`. Borrowed (owned by `inner`). A location is a
    /// segment index and a memtable row in the snapshot of the scan that
    /// produced it, so every location resolves against this scan's pinned
    /// segments and memtable, never the table's live ones: a flush, DELETE or
    /// compaction since then renumbers both.
    inner_scan: *Scan,
    table: *Table,

    /// Output schema (full projection, in output order) and the physical
    /// table-column index for each. Both owned.
    out_schema: []Column,
    out_phys: []usize,

    /// Output column buffers + their views, built once during the single
    /// emit. Owned.
    output_columns: []ColumnStore,
    views: []ColumnView,

    done: bool = false,

    /// Threads the fetch may read the survivors' blocks on (`prefill`).
    /// One for an operator that runs inside a worker already.
    dop: usize = 1,

    /// Scratch for one (segment, row-group) run of survivor offsets; reused
    /// across runs so a multi-million-row emit does not grow a list per run.
    offsets: std.ArrayListUnmanaged(u32) = .empty,

    pub fn create(
        allocator: Allocator,
        inner: Query,
        inner_scan: *Scan,
        table: *Table,
        output_names: []const []const u8,
    ) !Query {
        const out_schema = try allocator.alloc(Column, output_names.len);
        errdefer allocator.free(out_schema);
        const out_phys = try allocator.alloc(usize, output_names.len);
        errdefer allocator.free(out_phys);
        for (output_names, 0..) |name, i| {
            const idx = types.findColumn(table.schema.columns, name) orelse return Error.ColumnNotFound;
            out_phys[i] = idx;
            out_schema[i] = table.schema.columns[idx];
        }

        const output_columns = try allocator.alloc(ColumnStore, output_names.len);
        errdefer allocator.free(output_columns);
        var inited: usize = 0;
        errdefer for (output_columns[0..inited]) |*c| c.deinit(allocator);
        for (out_schema, 0..) |col, i| {
            output_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }

        const views = try allocator.alloc(ColumnView, output_names.len);
        errdefer allocator.free(views);

        const self = try allocator.create(LateScan);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .inner = inner,
            .inner_scan = inner_scan,
            .table = table,
            .out_schema = out_schema,
            .out_phys = out_phys,
            .output_columns = output_columns,
            .views = views,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *LateScan) void {
        var inner = self.inner;
        inner.deinit();
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.offsets.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.out_schema);
        self.allocator.free(self.out_phys);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *LateScan) []const Column {
        return self.out_schema;
    }

    pub fn addPrune(self: *LateScan, pred: Predicate) !void {
        return self.inner.addPrune(pred);
    }

    pub fn addPruneSet(self: *LateScan, set: predicate.InSet) !void {
        return self.inner.addPruneSet(set);
    }

    /// At most k rows survive; the inner TopN/Limit already bounds that, and
    /// its stats carry the sort claim. Borrow them, but cap `upper_rows` at
    /// the inner bound and drop the (now-stripped) `__rowloc` from the
    /// column-card view by reporting none — the output columns are the wide
    /// table columns, whose distinct counts we don't track here.
    pub fn stats(self: *LateScan) exec.PipelineStats {
        const inner = self.inner.stats();
        return .{
            .upper_rows = inner.upper_rows,
            .sort_state = inner.sort_state,
        };
    }

    pub fn accountant(self: *LateScan) ?*exec.memory.MemoryAccountant {
        return self.inner.accountant();
    }

    pub fn explain(self: *LateScan, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "LateScan (late materialization)");
        try self.inner.explain(out, allocator, depth + 1);
    }

    pub fn next(self: *LateScan) !?Batch {
        if (self.done) return null;
        self.done = true;
        try self.fetch();
        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        return Batch{
            .schema = self.out_schema,
            .values = self.views,
            .row_count = self.output_columns[0].rowCount(),
        };
    }

    /// Drain the inner pipeline, collecting each survivor's packed `__rowloc`
    /// in emit order, then materialize the wide output columns by location.
    fn fetch(self: *LateScan) !void {
        var locs: std.ArrayListUnmanaged(i64) = .empty;
        defer locs.deinit(self.allocator);

        while (try self.inner.next()) |batch| {
            const loc_idx = batch.columnIndex(rowloc.col_name) orelse return Error.ColumnNotFound;
            const packed_locs = batch.values[loc_idx].data.bigint;
            try locs.appendSlice(self.allocator, packed_locs[0..batch.row_count]);
        }
        if (locs.items.len == 0) return;

        try self.materialize(locs.items);
    }

    /// Public wide-column materializer for callers that collect survivor
    /// `__rowloc`s themselves (e.g. the zonemap top-N operator). `locs` is in
    /// the final emit order and must come from a scan over the same snapshot
    /// as `inner_scan`. The resolved output columns are read back via
    /// `outputColumns`. Kept here so the late-mat fetch logic lives in
    /// exactly one place.
    pub fn materializeInto(self: *LateScan, locs: []const i64) !void {
        try self.materialize(locs);
    }

    /// The materialized output columns + the resolved schema, for a caller
    /// that drove `materializeInto`. Valid until the next materialize call.
    pub fn outputColumns(self: *LateScan) struct { schema: []const Column, columns: []ColumnStore } {
        return .{ .schema = self.out_schema, .columns = self.output_columns };
    }

    /// Build the output columns for the survivors. Rows are processed in
    /// `locs` order (the inner already sorted them). Segment rows are batched
    /// per (segment, row group): we decode each needed column of that row
    /// group once and gather the relevant offsets, so a run of survivors in
    /// one row group costs one decode per column (and the row-group cache
    /// makes repeats across runs cheap). Memtable survivors read directly from
    /// the pinned snapshot's columns by row index.
    fn materialize(self: *LateScan, locs: []const i64) !void {
        if (!std.sort.isSorted(i64, locs, {}, std.sort.asc(i64))) return self.materializeUnordered(locs);
        if (self.dop > 1) try self.prefill(locs);
        var i: usize = 0;
        var cur_entry: ?*storage.cache.SegmentHandles.Entry = null;
        var cur_seg_idx: usize = std.math.maxInt(usize);
        defer if (cur_entry) |e| self.table.releaseSegment(e);

        while (i < locs.len) {
            switch (rowloc.unpack(locs[i])) {
                .memtable => |m| {
                    try self.appendMemtableRow(m.row);
                    i += 1;
                },
                .segment => |seg| {
                    if (cur_seg_idx != seg.seg_idx) {
                        if (cur_entry) |e| self.table.releaseSegment(e);
                        cur_entry = null;
                        cur_seg_idx = seg.seg_idx;
                        cur_entry = try self.table.acquireSegment(self.inner_scan.segs[seg.seg_idx].segment_id);
                    }
                    // Gather the maximal run of consecutive survivors sharing
                    // this (segment, row group) so each column decodes once.
                    var run_end = i;
                    self.offsets.clearRetainingCapacity();
                    while (run_end < locs.len) : (run_end += 1) {
                        const loc = rowloc.unpack(locs[run_end]);
                        if (loc != .segment) break;
                        if (loc.segment.seg_idx != seg.seg_idx or loc.segment.rg_idx != seg.rg_idx) break;
                        try self.offsets.append(self.allocator, @intCast(loc.segment.offset));
                    }
                    try self.appendSegmentRows(&cur_entry.?.seg, seg.rg_idx, self.offsets.items);
                    i = run_end;
                },
            }
        }
    }

    /// Reads the blocks `appendSegmentRows` borrows whole for the survivors
    /// in `locs` (sorted) into the cache, one job per row group and output
    /// column, on up to `dop` threads: a cold fetch of a wide row otherwise
    /// reads and decompresses every column's block one after another on
    /// one thread. The gather then finds them cached. A column a scan cached
    /// without its codes is left to the gather, which may take less of it;
    /// one read values apart has just its rows' values read
    /// (`loadValuesApart`).
    fn prefill(self: *LateScan, locs: []const i64) !void {
        // A row group's survivors are `locs[first..][0..rows]`.
        const Group = struct { seg_idx: usize, rg_idx: usize, entry: *storage.cache.SegmentHandles.Entry, first: usize, rows: usize };
        var groups: std.ArrayListUnmanaged(Group) = .empty;
        defer {
            for (groups.items, 0..) |g, i| {
                if (i == 0 or groups.items[i - 1].entry != g.entry) self.table.releaseSegment(g.entry);
            }
            groups.deinit(self.allocator);
        }
        for (locs, 0..) |packed_loc, i| switch (rowloc.unpack(packed_loc)) {
            .memtable => {},
            .segment => |s| {
                const last = if (groups.items.len > 0) groups.items[groups.items.len - 1] else null;
                if (last) |g| if (g.seg_idx == s.seg_idx and g.rg_idx == s.rg_idx) {
                    groups.items[groups.items.len - 1].rows += 1;
                    continue;
                };
                const entry = if (last != null and last.?.seg_idx == s.seg_idx)
                    last.?.entry
                else
                    try self.table.acquireSegment(self.inner_scan.segs[s.seg_idx].segment_id);
                groups.append(self.allocator, .{ .seg_idx = s.seg_idx, .rg_idx = s.rg_idx, .entry = entry, .first = i, .rows = 1 }) catch |err| {
                    if (last == null or last.?.entry != entry) self.table.releaseSegment(entry);
                    return err;
                };
            },
        };
        const jobs = groups.items.len * self.out_phys.len;
        if (jobs < 2) return;
        var grant = core_scheduler.reserveWorkers(@min(self.dop, jobs));
        defer grant.release();
        if (grant.count < 2) return;

        const Prefill = struct {
            late: *LateScan,
            groups: []const Group,
            locs: []const i64,
            allocator: Allocator,
            failed: std.atomic.Value(bool) = .init(false),
            err: ?anyerror = null,

            fn job(ctx: *@This(), _: usize, j: usize) void {
                if (ctx.failed.load(.monotonic)) return;
                const g = ctx.groups[j / ctx.late.out_phys.len];
                const phys = ctx.late.out_phys[j % ctx.late.out_phys.len];
                ctx.fill(&g.entry.seg, g.rg_idx, phys, ctx.locs[g.first..][0..g.rows]) catch |err| {
                    if (ctx.failed.swap(true, .acq_rel)) return;
                    ctx.err = err;
                };
            }

            fn fill(ctx: *@This(), seg: *storage.ReadSegment, rg_idx: usize, phys: usize, locs_of_group: []const i64) !void {
                const c = ctx.late.table.cacheRef();
                if (seg.cachedStringBlock(rg_idx, phys, c, .no_codes)) |cached| {
                    var values = cached;
                    values.release(ctx.allocator, c);
                    return;
                }
                if (ctx.late.readsValuesApart(seg, rg_idx, phys)) return ctx.late.loadValuesApart(ctx.allocator, seg, rg_idx, phys, locs_of_group);
                if (seg.readsIdRows(rg_idx, phys, c)) return ctx.late.loadIdRows(ctx.allocator, seg, rg_idx, phys, locs_of_group);
                var bb = try seg.borrowColumnBlock(ctx.allocator, rg_idx, phys, c);
                bb.release(ctx.allocator, c);
            }
        };
        var ctx: Prefill = .{
            .late = self,
            .groups = groups.items,
            .locs = locs,
            .allocator = try exec.memory.workerAllocator(exec.memory.accountantOf(self.allocator), std.heap.c_allocator),
        };
        parallel.forJobs(grant.count, jobs, &ctx, Prefill.job);
        if (ctx.err) |err| return err;
    }

    /// Out-of-order locations (TopN emit order, group-hash order) degenerate
    /// the per-(segment,row-group) run batching in `materialize` to per-row
    /// block borrows. Sort the locations, materialize location-ordered into
    /// scratch columns (maximal runs), then scatter rows back into `locs`
    /// order — a bounded extra copy, orders of magnitude cheaper than
    /// per-survivor block decodes.
    fn materializeUnordered(self: *LateScan, locs: []const i64) anyerror!void {
        const n = locs.len;
        const order = try self.allocator.alloc(u32, n);
        defer self.allocator.free(order);
        try rowloc.sortedOrder(self.allocator, locs, order);
        const sorted = try self.allocator.alloc(i64, n);
        defer self.allocator.free(sorted);
        for (order, 0..) |oi, j| sorted[j] = locs[oi];

        const scratch = try self.allocator.alloc(ColumnStore, self.output_columns.len);
        var inited: usize = 0;
        defer {
            for (scratch[0..inited]) |*c| c.deinit(self.allocator);
            self.allocator.free(scratch);
        }
        for (self.out_schema, 0..) |col, i| {
            scratch[i] = try ColumnStore.init(self.allocator, col.type, col.nullable);
            inited += 1;
        }
        const real = self.output_columns;
        self.output_columns = scratch;
        defer self.output_columns = real;
        try self.materialize(sorted);

        const idx = try self.allocator.alloc(u32, n);
        defer self.allocator.free(idx);
        for (order, 0..) |oi, j| idx[oi] = @intCast(j);
        for (real, scratch) |*out, *sc| {
            try engine.transform.appendByIndices(self.allocator, sc.view(), idx, out);
        }
    }

    /// Fetch only the survivor `offsets` (ascending within the row group) of
    /// each output column from one row group — never the full 65,536-row
    /// block. The block is decompressed once (cached pin), then gathered by
    /// the same per-encoding row gathers the filtered scan uses: FOR and RLE
    /// straight off the narrow codes, dict through the sorted dictionary, FSST
    /// decoding only the survivor rows, raw through an in-place borrowed view.
    fn appendSegmentRows(self: *LateScan, seg: *storage.ReadSegment, rg_idx: usize, offsets: []const u32) !void {
        const sr = storage.segment_reader;
        const rc = seg.info.row_groups[rg_idx].row_count;
        for (self.out_phys, 0..) |phys, out_idx| {
            const col_type = self.table.schema.columns[phys].type;
            const flags = storage.format.ColumnBlockFlags{ .has_nulls = self.table.schema.columns[phys].nullable };
            const out = &self.output_columns[out_idx];

            // A scan that counted the block's values cached it without its
            // codes and located each value by its first row, which the
            // block's stats resolve with no read; other rows' codes come from
            // the block without its values' bytes, a fraction of the whole
            // block's fill.
            if (seg.cachedStringBlock(rg_idx, phys, self.table.cacheRef(), .no_codes)) |cached| {
                var values = cached;
                defer values.release(self.allocator, self.table.cacheRef());
                if (try engine.transform.appendDictFirstRows(self.allocator, values.bytes, rc, flags, offsets, out)) continue;
                var codes = try seg.borrowStringBlock(self.allocator, rg_idx, phys, self.table.cacheRef(), .no_bytes);
                defer codes.release(self.allocator, self.table.cacheRef());
                try engine.transform.appendDictRowsOf(self.allocator, codes.bytes, values.bytes, rc, flags, offsets, out);
                continue;
            }
            if (try self.appendValuesApart(seg, rg_idx, phys, rc, offsets, out)) continue;
            if (seg.readsIdRows(rg_idx, phys, self.table.cacheRef())) {
                try self.appendIdRows(seg, rg_idx, phys, offsets, out);
                continue;
            }

            var bb = try seg.borrowColumnBlock(self.allocator, rg_idx, phys, self.table.cacheRef());
            defer bb.release(self.allocator, self.table.cacheRef());

            switch (bb.encoding) {
                .for_ => try engine.transform.appendForRows(self.allocator, sr.forViewOf(bb.bytes, rc, flags), offsets, out),
                .rle => try engine.transform.appendRleRows(self.allocator, sr.rleViewOf(bb.bytes, rc, flags), offsets, out),
                .dict => try engine.transform.appendDictRows(self.allocator, bb.bytes, rc, flags, offsets, out),
                .fsst => try engine.transform.appendFsstRows(self.allocator, try sr.fsstViewOf(bb.bytes, rc, flags), offsets, out),
                .raw => {
                    if (sr.viewRawColumn(col_type, bb.bytes, rc, flags, .raw)) |view| {
                        try engine.transform.appendByIndices(self.allocator, view, offsets, out);
                    } else {
                        // Alignment / big-endian fallback: decode the block, gather.
                        var col = try sr.decodeColumnPayload(self.allocator, col_type, bb.bytes, rc, flags, .raw);
                        defer col.deinit(self.allocator);
                        try engine.transform.appendByIndices(self.allocator, col.view(), offsets, out);
                    }
                },
            }
        }
    }

    /// Whether row group `rg_idx` of non-nullable string column `phys` reads
    /// its rows' values apart (`appendValuesApart`): a scan cached its block
    /// without its values' bytes, as one keying the column by table id does,
    /// and no reader holds the whole block. The whole block's shared values
    /// span pages a few rows don't need.
    fn readsValuesApart(self: *LateScan, seg: *storage.ReadSegment, rg_idx: usize, phys: usize) bool {
        if (self.table.schema.columns[phys].nullable) return false;
        const c = self.table.cacheRef();
        var codes = seg.cachedStringBlock(rg_idx, phys, c, .no_bytes) orelse return false;
        codes.release(self.allocator, c);
        return !self.holdsWholeBlock(seg, rg_idx, phys);
    }

    fn holdsWholeBlock(self: *LateScan, seg: *storage.ReadSegment, rg_idx: usize, phys: usize) bool {
        const c = self.table.cacheRef();
        return c.cache.holdsOrFills(.{
            .table_uid = c.table_uid,
            .segment_id = seg.info.segment_id,
            .row_group_idx = @intCast(rg_idx),
            .column_idx = @intCast(phys),
        });
    }

    /// When `readsValuesApart`, the values of `rows` (ascending) of row
    /// group `rg_idx` of column `phys`, each read apart
    /// (`ReadSegment.EntryValues`): its code from the cached block without
    /// bytes, then its page alone or the row group's own bytes alone. False,
    /// appending nothing, otherwise.
    fn appendValuesApart(self: *LateScan, seg: *storage.ReadSegment, rg_idx: usize, phys: usize, rc: u32, rows: []const u32, out: *ColumnStore) !bool {
        if (self.table.schema.columns[phys].nullable) return false;
        const c = self.table.cacheRef();
        var codes = seg.cachedStringBlock(rg_idx, phys, c, .no_bytes) orelse return false;
        defer codes.release(self.allocator, c);
        if (self.holdsWholeBlock(seg, rg_idx, phys)) return false;
        const db = storage.segment_reader.dictBlockOf(codes.bytes, rc);
        if (!db.hasSegmentIds()) return false;
        var values = seg.entryValues(self.allocator, rg_idx, phys, c) orelse return false;
        defer values.release();
        for (rows) |row| try values.want(db.segmentId(db.rowCode(row)));
        try values.loadWanted();
        var total_bytes: usize = 0;
        for (rows) |row| total_bytes += (try values.value(db.segmentId(db.rowCode(row)))).len;
        switch (out.data) {
            .varchar, .string, .char, .json, .nested, .bitmap => |*ss| {
                try ss.ensureUnusedValueCapacity(self.allocator, rows.len, total_bytes);
                for (rows) |row| ss.appendValueAssumeCapacity(try values.value(db.segmentId(db.rowCode(row))));
            },
            else => return false,
        }
        return true;
    }

    /// The values of `rows` (ascending) of row group `rg_idx` of column
    /// `phys`, which `ReadSegment.readsIdRows`: their ids, then only the
    /// list pages those lie on.
    fn appendIdRows(self: *LateScan, seg: *storage.ReadSegment, rg_idx: usize, phys: usize, rows: []const u32, out: *ColumnStore) !void {
        const col = self.table.schema.columns[phys];
        const flags = storage.format.ColumnBlockFlags{ .has_nulls = col.nullable };
        const payload = try seg.idRows(self.allocator, rg_idx, phys, self.table.cacheRef(), col.nullable, rows);
        defer self.allocator.free(payload);
        const n: u32 = @intCast(rows.len);
        if (storage.segment_reader.viewRawColumn(col.type, payload, n, flags, .raw)) |view| return engine.transform.appendAllColumn(self.allocator, view, out);
        // A bitmap ahead of the values leaves them unaligned to view.
        var decoded = try storage.segment_reader.decodeColumnPayload(self.allocator, col.type, payload, n, flags, .raw);
        defer decoded.deinit(self.allocator);
        try engine.transform.appendAllColumn(self.allocator, decoded.view(), out);
    }

    /// Reads into the cache what `appendIdRows` reads for the survivors
    /// `locs` of row group `rg_idx`.
    fn loadIdRows(self: *LateScan, allocator: Allocator, seg: *storage.ReadSegment, rg_idx: usize, phys: usize, locs: []const i64) !void {
        const rows = try allocator.alloc(u32, locs.len);
        defer allocator.free(rows);
        for (locs, rows) |loc, *row| row.* = @intCast(rowloc.unpack(loc).segment.offset);
        const payload = try seg.idRows(allocator, rg_idx, phys, self.table.cacheRef(), self.table.schema.columns[phys].nullable, rows);
        allocator.free(payload);
    }

    /// Reads into the cache what `appendValuesApart` reads for the
    /// survivors `locs` of row group `rg_idx`: their shared values' pages
    /// and the row group's own values.
    fn loadValuesApart(self: *LateScan, allocator: Allocator, seg: *storage.ReadSegment, rg_idx: usize, phys: usize, locs: []const i64) !void {
        const c = self.table.cacheRef();
        var codes = seg.cachedStringBlock(rg_idx, phys, c, .no_bytes) orelse return;
        defer codes.release(allocator, c);
        const db = storage.segment_reader.dictBlockOf(codes.bytes, seg.info.row_groups[rg_idx].row_count);
        if (!db.hasSegmentIds()) return;
        var values = seg.entryValues(allocator, rg_idx, phys, c) orelse return;
        defer values.release();
        for (locs) |loc| try values.want(db.segmentId(db.rowCode(@intCast(rowloc.unpack(loc).segment.offset))));
        try values.loadWanted();
        for (locs) |loc| _ = try values.value(db.segmentId(db.rowCode(@intCast(rowloc.unpack(loc).segment.offset))));
    }

    fn appendMemtableRow(self: *LateScan, row: usize) !void {
        const snap = self.inner_scan.memtableSnap();
        const one = [_]u32{@intCast(row)};
        for (self.out_phys, 0..) |phys, out_idx| {
            try engine.transform.appendByIndices(
                self.allocator,
                snap.columns[phys].view(),
                &one,
                &self.output_columns[out_idx],
            );
        }
    }
};
