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

pub const LateScan = struct {
    allocator: Allocator,
    /// Inner pipeline: Scan(emit_loc) → Filter → TopN|Limit. Owns the ddl_lock
    /// + memtable pin (released when this is deinit'd). Kept alive until after
    /// the fetch so `inner_scan.memtableSnap()` stays valid.
    inner: Query,
    /// Bottom Scan of `inner` — the source of `memtableSnap()` and the table
    /// the locations point into. Borrowed (owned by `inner`).
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

    /// Build the output columns for the survivors. Rows are processed in
    /// `locs` order (the inner already sorted them). Segment rows are batched
    /// per (segment, row group): we decode each needed column of that row
    /// group once and gather the relevant offsets, so a run of survivors in
    /// one row group costs one decode per column (and the row-group cache
    /// makes repeats across runs cheap). Memtable survivors read directly from
    /// the pinned snapshot's columns by row index.
    fn materialize(self: *LateScan, locs: []const i64) !void {
        var i: usize = 0;
        var cur_segment: ?storage.ReadSegment = null;
        var cur_seg_idx: usize = std.math.maxInt(usize);
        defer if (cur_segment) |*s| s.deinit();

        while (i < locs.len) {
            switch (rowloc.unpack(locs[i])) {
                .memtable => |m| {
                    try self.appendMemtableRow(m.row);
                    i += 1;
                },
                .segment => |seg| {
                    if (cur_seg_idx != seg.seg_idx) {
                        if (cur_segment) |*s| s.deinit();
                        cur_segment = null;
                        cur_seg_idx = seg.seg_idx;
                        const entry = self.table.manifest.segments.items[seg.seg_idx];
                        var name_buf: [32]u8 = undefined;
                        const file_name = try Table.segmentFileName(&name_buf, entry.segment_id);
                        cur_segment = try storage.readSegment(
                            self.allocator,
                            self.table.io,
                            self.table.segments_dir,
                            file_name,
                            self.table.schema,
                        );
                    }
                    // Gather the maximal run of consecutive survivors sharing
                    // this (segment, row group) so each column decodes once.
                    var run_end = i;
                    var offsets: std.ArrayListUnmanaged(u32) = .empty;
                    defer offsets.deinit(self.allocator);
                    while (run_end < locs.len) : (run_end += 1) {
                        const loc = rowloc.unpack(locs[run_end]);
                        if (loc != .segment) break;
                        if (loc.segment.seg_idx != seg.seg_idx or loc.segment.rg_idx != seg.rg_idx) break;
                        try offsets.append(self.allocator, @intCast(loc.segment.offset));
                    }
                    try self.appendSegmentRows(&cur_segment.?, seg.rg_idx, offsets.items);
                    i = run_end;
                },
            }
        }
    }

    fn appendSegmentRows(self: *LateScan, seg: *storage.ReadSegment, rg_idx: usize, offsets: []const u32) !void {
        for (self.out_phys, 0..) |phys, out_idx| {
            var col = try seg.decodeColumnMaybeCached(
                self.allocator,
                self.table.schema,
                rg_idx,
                phys,
                &self.table.cache,
            );
            defer col.deinit(self.allocator);
            try engine.transform.appendByIndices(self.allocator, col.view(), offsets, &self.output_columns[out_idx]);
        }
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
