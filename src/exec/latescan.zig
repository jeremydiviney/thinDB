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

    /// When non-null, `appendMemtableRow` resolves memtable survivors against
    /// this snapshot instead of `inner_scan.memtableSnap()`. Set only by the
    /// external `materializeInto` entry point (the zonemap top-N operator owns
    /// its own pinned snapshot); the normal `fetch` path leaves it null.
    materialize_snap_override: ?*engine.Memtable = null,

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

    /// Public wide-column materializer for callers that collect survivor
    /// `__rowloc`s themselves (e.g. the zonemap top-N operator). `locs` is in
    /// the final emit order; memtable survivors resolve against `snap`. The
    /// resolved output columns are read back via `outputColumns`. Kept here so
    /// the late-mat fetch logic lives in exactly one place.
    pub fn materializeInto(self: *LateScan, locs: []const i64, snap: *engine.Memtable) !void {
        self.materialize_snap_override = snap;
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
                        const entry = self.table.manifest.segments.items[seg.seg_idx];
                        cur_entry = try self.table.acquireSegment(entry.segment_id);
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
                    try self.appendSegmentRows(&cur_entry.?.seg, seg.rg_idx, offsets.items);
                    i = run_end;
                },
            }
        }
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

    /// Fetch only the survivor `offsets` of each output column from one row
    /// group — decode just those rows, never the full 65,536-row block. The
    /// block is decompressed once (cached pin), then:
    ///   - raw  : an in-place borrowed view; `appendByIndices` copies only the
    ///            survivor offsets (zero full-block materialization),
    ///   - dict : index each survivor's code into the dict directly (no expand),
    ///   - FOR  : expand the block (cheap, vectorized) then gather the offsets.
    fn appendSegmentRows(self: *LateScan, seg: *storage.ReadSegment, rg_idx: usize, offsets: []const u32) !void {
        const sr = storage.segment_reader;
        const rc = seg.info.row_groups[rg_idx].row_count;
        for (self.out_phys, 0..) |phys, out_idx| {
            const col_type = self.table.schema.columns[phys].type;
            const flags = storage.format.ColumnBlockFlags{ .has_nulls = self.table.schema.columns[phys].nullable };
            const out = &self.output_columns[out_idx];

            var bb = try seg.borrowColumnBlock(self.allocator, rg_idx, phys, self.table.cacheRef());
            defer bb.release(self.allocator, self.table.cacheRef());

            switch (bb.encoding) {
                .dict => try appendDictRows(self.allocator, bb.bytes, rc, flags, offsets, out),
                .fsst => try appendFsstRows(self.allocator, bb.bytes, rc, flags, offsets, out),
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
                .for_, .rle => |enc| {
                    var col = try sr.decodeColumnPayload(self.allocator, col_type, bb.bytes, rc, flags, enc);
                    defer col.deinit(self.allocator);
                    try engine.transform.appendByIndices(self.allocator, col.view(), offsets, out);
                },
            }
        }
    }

    fn appendMemtableRow(self: *LateScan, row: usize) !void {
        const snap = self.materialize_snap_override orelse self.inner_scan.memtableSnap();
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

/// Gather only `offsets`' rows of a dict-encoded string block into `out` by
/// indexing each row's code into the sorted dict — never expanding the full
/// block (the late-mat survivor fetch is ≤ k rows scattered across 65,536-row
/// blocks). `raw` is the decompressed payload; the validity handling mirrors
/// `transform.appendByIndices`.
fn appendDictRows(
    allocator: Allocator,
    raw: []const u8,
    row_count: u32,
    flags: storage.format.ColumnBlockFlags,
    offsets: []const u32,
    out: *ColumnStore,
) !void {
    var values = raw;
    var nulls: ?[]const u8 = null;
    if (flags.has_nulls) {
        const bm = storage.column.bitmapBytes(row_count);
        nulls = raw[0..bm];
        values = raw[bm..];
    }
    const db = storage.segment_reader.dictBlockOf(values, row_count);
    const dst_start = out.data.rowCount();
    switch (out.data) {
        .varchar, .string, .char, .json => |*ss| {
            for (offsets) |off| try ss.appendValue(allocator, db.dictValue(db.rowCode(off)));
        },
        else => unreachable, // the writer only dict-encodes string-family columns
    }
    if (out.nulls != null) {
        for (offsets, 0..) |off, j| {
            try out.appendValidBit(allocator, dst_start + j, storage.column.isValidBit(nulls, off));
        }
    }
}

/// Gather only `offsets`' rows of an FSST-encoded string block into `out`,
/// decoding just those rows — the whole point of pairing late
/// materialization with compressed strings: a LIMIT-k query decodes k
/// strings, not 65,536.
fn appendFsstRows(
    allocator: Allocator,
    raw: []const u8,
    row_count: u32,
    flags: storage.format.ColumnBlockFlags,
    offsets: []const u32,
    out: *ColumnStore,
) !void {
    var values = raw;
    var nulls: ?[]const u8 = null;
    if (flags.has_nulls) {
        const bm = storage.column.bitmapBytes(row_count);
        nulls = raw[0..bm];
        values = raw[bm..];
    }
    const fb = try storage.segment_reader.fsstBlockOf(values, row_count);
    var scratch: std.ArrayListUnmanaged(u8) = .empty;
    defer scratch.deinit(allocator);
    const dst_start = out.data.rowCount();
    switch (out.data) {
        .varchar, .string, .char, .json => |*ss| {
            for (offsets) |off| {
                const comp = fb.rowComp(off);
                try scratch.resize(allocator, storage.fsst.decodedSizeBound(comp.len));
                const n = fb.table.decodeIntoUnchecked(comp, scratch.items);
                try ss.appendValue(allocator, scratch.items[0..n]);
            }
        },
        else => unreachable, // the writer only FSST-encodes string-family columns
    }
    if (out.nulls != null) {
        for (offsets, 0..) |off, j| {
            try out.appendValidBit(allocator, dst_start + j, storage.column.isValidBit(nulls, off));
        }
    }
}
