//! Streaming UPDATE — per-segment "delete-old + insert-new" pairs
//! that keep memory bounded by row-group size + memtable budget,
//! regardless of how many rows the UPDATE touches.
//!
//! Architecture:
//!   1. Lock the table mutex (held for the whole UPDATE — concurrent
//!      readers stay snapshot-isolated via their own Scan captures).
//!   2. Snapshot bounds: (segs_at_start, mt_rows_at_start). New
//!      segments created by auto-flush during step 4 land beyond
//!      segs_at_start and are never touched by the tombstone step.
//!   3. Memtable phase — process rows [0..mt_rows_at_start]:
//!      decode → predicate mask → compute new values via assignments
//!      → clone memtable with non-matching rows + append the new
//!      replacements. Done BEFORE any segment work so the memtable
//!      can't get auto-flushed while still holding matching rows.
//!   4. Segment phase — for each segment[0..segs_at_start], iterate
//!      row groups: decode → predicate mask → compute new values →
//!      append new rows to the live memtable (may auto-flush) →
//!      record matched offsets. After all row groups, merge
//!      tombstones for that segment.
//!   5. Unlock.
//!
//! "Delete and insert happen together in batches": each segment's
//! tombstone-merge + its new-row inserts happen contiguously under
//! one mutex hold. Per-batch atomicity.
//!
//! Assignment evaluation reuses the standard Compute operator wired
//! via SingleBatchSource so we don't duplicate that machinery here.

const std = @import("std");
const types = @import("../types.zig");
const Column = types.Column;
const ValueTag = types.ValueTag;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const exec = @import("../exec/exec.zig");
const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;
const Memtable = engine.Memtable;

const api = @import("api.zig");
const Table = api.Table;

const ir = @import("../ir/ir.zig");

/// One assignment from the IR UpdateOp: column name + Expr to compute
/// the new value. Caller is responsible for the lifetime of the
/// strings/Expr-tree — typically the SQL compile arena.
pub const Assignment = struct {
    col: []const u8,
    value: ir.Expr,
};

/// Streaming UPDATE entry point. Caller pre-resolved the predicate +
/// assignments through the pre-compile pass (subqueries / @vars
/// already folded to literals). Returns the affected row count.
pub fn execUpdateStreaming(
    t: *Table,
    pred_in: ?exec.PredicateExpr,
    assignments: []const Assignment,
) !usize {
    // Validate + widen predicate literals up front so both the WAL-
    // logged form and the per-row-group eval see the same shape.
    var pred_local: ?exec.PredicateExpr = pred_in;
    if (pred_local) |*p| try exec.predicate.validateExpr(p, t.schema.columns);

    // Verify every assigned column exists.
    for (assignments) |asn| {
        _ = types.findColumn(t.schema.columns, asn.col) orelse return exec.Error.ColumnNotFound;
    }

    t.mutex.lockUncancelable(t.io);
    var wal_target: ?u64 = null;
    var affected: usize = 0;
    {
        defer t.mutex.unlock(t.io);

        // Snapshot bounds. These freeze for the duration of the UPDATE.
        const segs_at_start = t.manifest.segments.items.len;
        const mt_rows_at_start: usize = @intCast(t.memtable.row_count);

        // Log the predicate (and the new-row inserts, later) into WAL
        // so a crash mid-UPDATE leaves a recoverable state.
        wal_target = try t.logDeleteExprLocked(pred_local);

        // -- Phase 1: memtable rows [0..mt_rows_at_start] --------
        if (mt_rows_at_start > 0) {
            affected += try processMemtable(t, pred_local, assignments, mt_rows_at_start);
        }

        // -- Phase 2: segments[0..segs_at_start] -----------------
        if (segs_at_start > 0) {
            affected += try processSegments(t, pred_local, assignments, segs_at_start);
        }
    }
    try t.awaitWalDurable(wal_target);
    return affected;
}

// =============================================================================
// Phase 1 — memtable rows.
// =============================================================================

fn processMemtable(
    t: *Table,
    pred_opt: ?exec.PredicateExpr,
    assignments: []const Assignment,
    mt_rows_at_start: usize,
) !usize {
    const allocator = t.allocator;

    // Snapshot the memtable's first mt_rows_at_start rows as a Batch.
    // ColumnStore.view() returns the full column slice; we bound the
    // logical row count via `Batch.row_count` so eval / mask logic
    // only reads the snapshot prefix.
    const views = try allocator.alloc(ColumnView, t.schema.columns.len);
    defer allocator.free(views);
    for (t.memtable.columns, views) |*c, *v| v.* = c.view();
    const batch: exec.Batch = .{
        .schema = t.schema.columns,
        .values = views,
        .row_count = mt_rows_at_start,
    };

    // Evaluate predicate → mask. Allow null = match everything.
    const mask = try allocator.alloc(bool, mt_rows_at_start);
    defer allocator.free(mask);
    if (pred_opt) |p| {
        try exec.predicate.evaluatePredicate(allocator, p, t.schema.columns, batch, mask);
    } else {
        @memset(mask, true);
    }

    var matched_count: usize = 0;
    for (mask) |m| if (m) {
        matched_count += 1;
    };
    if (matched_count == 0) return 0;

    // Compute new values for matched rows via the standard pipeline:
    // SingleBatchSource → Compute(synthetic derived) → drain.
    var new_rows = try computeNewRows(t, batch, mask, matched_count, assignments);
    defer freeMaterializedRows(allocator, &new_rows);

    // Filter the memtable: keep rows where !mask. Append new rows
    // (post-assignment) to the now-filtered memtable.
    const keep = try allocator.alloc(bool, mt_rows_at_start);
    defer allocator.free(keep);
    for (mask, keep) |m, *k| k.* = !m;

    if (try t.memtable.cloneWithRetainedRows(allocator, keep)) |new_mt| {
        const old_mt = t.memtable;
        t.memtable = new_mt;
        old_mt.retire();
        old_mt.release();
    }

    try insertMaterializedRows(t, &new_rows);
    return matched_count;
}

// =============================================================================
// Phase 2 — segment iteration.
// =============================================================================

fn processSegments(
    t: *Table,
    pred_opt: ?exec.PredicateExpr,
    assignments: []const Assignment,
    segs_at_start: usize,
) !usize {
    var total: usize = 0;
    var i: usize = 0;
    while (i < segs_at_start) : (i += 1) {
        total += try processOneSegment(t, pred_opt, assignments, t.manifest.segments.items[i]);
    }
    return total;
}

fn processOneSegment(
    t: *Table,
    pred_opt: ?exec.PredicateExpr,
    assignments: []const Assignment,
    entry: storage.manifest.ManifestEntry,
) !usize {
    const allocator = t.allocator;
    var name_buf: [32]u8 = undefined;
    const file_name = try Table.segmentFileName(&name_buf, entry.segment_id);
    var seg = try storage.readSegment(allocator, t.io, t.segments_dir, file_name, t.schema);
    defer seg.deinit();

    var deleted: std.ArrayList(u32) = .empty;
    defer deleted.deinit(allocator);

    var row_offset: u32 = 0;
    for (seg.info.row_groups, 0..) |rg, rg_idx| {
        const n = rg.row_count;

        // Decode all schema columns for this row group. (Future: only
        // decode columns referenced by predicate + assignments.)
        const owned_cols = try allocator.alloc(storage.OwnedColumn, t.schema.columns.len);
        defer {
            for (owned_cols) |*oc| oc.deinit(allocator);
            allocator.free(owned_cols);
        }
        for (t.schema.columns, 0..) |_, ci| {
            owned_cols[ci] = try seg.decodeColumn(allocator, t.schema, rg_idx, ci);
        }

        const views = try allocator.alloc(ColumnView, t.schema.columns.len);
        defer allocator.free(views);
        for (owned_cols, views) |oc, *v| v.* = oc.view();

        const batch: exec.Batch = .{
            .schema = t.schema.columns,
            .values = views,
            .row_count = n,
        };

        const mask = try allocator.alloc(bool, n);
        defer allocator.free(mask);
        if (pred_opt) |p| {
            try exec.predicate.evaluatePredicate(allocator, p, t.schema.columns, batch, mask);
        } else {
            @memset(mask, true);
        }

        var matched_in_rg: usize = 0;
        for (mask) |m| if (m) {
            matched_in_rg += 1;
        };

        if (matched_in_rg > 0) {
            // Record original row offsets (within the segment) for tombstoning.
            for (mask, 0..) |m, k| if (m) {
                try deleted.append(allocator, row_offset + @as(u32, @intCast(k)));
            };

            // Compute new values and append to memtable.
            var new_rows = try computeNewRows(t, batch, mask, matched_in_rg, assignments);
            defer freeMaterializedRows(allocator, &new_rows);
            try insertMaterializedRows(t, &new_rows);
        }
        row_offset += n;
    }

    if (deleted.items.len > 0) {
        try storage.tombstone.merge(
            allocator,
            t.io,
            t.segments_dir,
            entry.segment_id,
            deleted.items,
            t.syncEnabled(),
        );
        t.seg_handles.invalidateTombstones(t.allocator, entry.segment_id);
    }
    return deleted.items.len;
}

// =============================================================================
// Helpers — compute new-row batch via Compute, materialize, insert.
// =============================================================================

/// Materialized new-row buffer with one ColumnStore per table schema
/// column. Lives for the duration of one row-group's processing.
const NewRows = struct {
    stores: []ColumnStore,
    row_count: usize,
};

fn freeMaterializedRows(allocator: std.mem.Allocator, rows: *NewRows) void {
    for (rows.stores) |*c| c.deinit(allocator);
    allocator.free(rows.stores);
}

/// Given a batch + match mask + assignments, produce a NewRows with
/// `matched_count` rows where each column carries either the
/// post-assignment value (for assigned cols) or the original value
/// from `batch` (for the others).
///
/// Strategy: build a filtered batch holding only matched rows
/// (via `appendMaskedColumn` into per-column ColumnStores), then
/// run a SingleBatchSource(filtered_batch) → Compute(synthetic
/// derived) pipeline to produce the post-assignment columns.
fn computeNewRows(
    t: *Table,
    batch: exec.Batch,
    mask: []const bool,
    matched_count: usize,
    assignments: []const Assignment,
) !NewRows {
    const allocator = t.allocator;
    const schema = t.schema;

    // Step 1: materialize filtered (matched-only) batch into ColumnStores.
    var filtered = try allocator.alloc(ColumnStore, schema.columns.len);
    var inited: usize = 0;
    errdefer {
        for (filtered[0..inited]) |*c| c.deinit(allocator);
        allocator.free(filtered);
    }
    for (schema.columns, 0..) |sc, ci| {
        filtered[ci] = try ColumnStore.init(allocator, sc.type, sc.nullable);
        inited += 1;
    }
    for (schema.columns, 0..) |sc, ci| {
        _ = sc;
        try engine.transform.appendMaskedColumn(
            allocator,
            batch.values[ci],
            mask,
            &filtered[ci],
        );
    }

    // Step 2: build a Batch view over the filtered columns.
    const filtered_views = try allocator.alloc(ColumnView, schema.columns.len);
    defer allocator.free(filtered_views);
    for (filtered, filtered_views) |*c, *v| v.* = c.view();
    const filtered_batch: exec.Batch = .{
        .schema = schema.columns,
        .values = filtered_views,
        .row_count = matched_count,
    };

    // Step 3: wrap in SingleBatchSource and chain through Compute.
    // Compute's `Derived` list uses synthetic names so its outputs
    // don't collide with the upstream cols of the same name.
    const synth_names = try allocator.alloc([]u8, assignments.len);
    defer {
        for (synth_names) |s| allocator.free(s);
        allocator.free(synth_names);
    }
    const derived = try allocator.alloc(exec.Derived, assignments.len);
    defer allocator.free(derived);
    for (assignments, 0..) |asn, i| {
        synth_names[i] = try std.fmt.allocPrint(allocator, "__upd_{d}__{s}", .{ i, asn.col });
        derived[i] = .{ .name = synth_names[i], .expr = asn.value };
    }

    var src_q = try @import("../exec/single_batch.zig").SingleBatchSource.create(allocator, filtered_batch);
    var compute_q = try src_q.compute(derived);
    defer compute_q.deinit();

    // Step 4: drain Compute (just one batch out, since input is one batch).
    var got: ?exec.Batch = null;
    while (try compute_q.next()) |b| {
        if (b.row_count == 0) continue;
        got = b;
        break;
    }
    const out = got orelse return error.UpdateNoRowsFromCompute;

    // Step 5: build the final NewRows — for each table schema column,
    // pick either the synthetic (assigned) or the filtered original.
    const out_stores = try allocator.alloc(ColumnStore, schema.columns.len);
    var out_inited: usize = 0;
    errdefer {
        for (out_stores[0..out_inited]) |*c| c.deinit(allocator);
        allocator.free(out_stores);
    }

    for (schema.columns, 0..) |sc, ci| {
        var src_view: ColumnView = filtered[ci].view();
        // Was this column assigned? If so, replace src_view with the
        // synthetic column from Compute's output.
        for (assignments, synth_names) |asn, syn| {
            if (@import("../types.zig").columnNameEql(asn.col, sc.name)) {
                // Find synthetic column in `out`'s schema. The synthetic
                // name is generated internally so it's an exact match;
                // no case-folding needed.
                for (out.schema, 0..) |out_col, oi| {
                    if (std.mem.eql(u8, out_col.name, syn)) {
                        src_view = out.values[oi];
                        break;
                    }
                }
                break;
            }
        }

        // Allocate the destination store and copy src_view's rows in.
        out_stores[ci] = try ColumnStore.init(allocator, sc.type, sc.nullable);
        out_inited += 1;
        const yes_all = try allocator.alloc(bool, matched_count);
        defer allocator.free(yes_all);
        @memset(yes_all, true);
        try engine.transform.appendMaskedColumn(
            allocator,
            src_view,
            yes_all,
            &out_stores[ci],
        );
    }

    // Filtered columns served their purpose; release.
    for (filtered) |*c| c.deinit(allocator);
    allocator.free(filtered);

    return NewRows{ .stores = out_stores, .row_count = matched_count };
}

/// Push the materialized new rows into the table's memtable via
/// `insertBatchInner` (which also WAL-logs and may trigger
/// auto-flush). The translation step converts the synthetic Compute
/// names back to the schema names (they're identical here because we
/// already moved synthetic outputs into the right slots).
fn insertMaterializedRows(t: *Table, rows: *NewRows) !void {
    if (rows.row_count == 0) return;
    const allocator = t.allocator;
    const schema = t.schema;
    const views = try allocator.alloc(ColumnView, schema.columns.len);
    defer allocator.free(views);
    for (rows.stores, views) |*c, *v| v.* = c.view();
    _ = try t.insertBatchInner(schema.columns, views, rows.row_count);
}
