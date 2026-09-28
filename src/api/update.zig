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
//! Per-batch atomicity: the memtable phase and each matching row group
//! is one batch, logged as one `replace` WAL record holding both its
//! deletes and its replacement rows (`Table.replaceRowsLocked`). A crash
//! keeps or drops whole batches; it never keeps a delete without its
//! rows or the rows without their delete (#48).
//!
//! Assignment evaluation reuses the standard Compute operator wired
//! via SingleBatchSource so we don't duplicate that machinery here.

const std = @import("std");
const types = @import("../types.zig");

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const exec = @import("../exec/exec.zig");
const cast = @import("../exec/cast.zig");
const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

const api = @import("api.zig");
const Table = api.Table;
const DmlFilter = @import("delete.zig").DmlFilter;
const LiveSegment = @import("delete.zig").LiveSegment;
const upsert = @import("upsert.zig");

const ir = @import("../ir/ir.zig");

/// One assignment from the IR UpdateOp: column name + Expr to compute
/// the new value. Caller is responsible for the lifetime of the
/// strings/Expr-tree — typically the SQL compile arena.
pub const Assignment = struct {
    col: []const u8,
    value: ir.Expr,
};

/// Streaming UPDATE entry point. Caller pre-resolved the predicate,
/// its computed operands (`derived`) and the assignments through the
/// pre-compile pass (subqueries / @vars already folded to literals).
/// Returns the affected row count.
pub fn execUpdateStreaming(
    t: *Table,
    pred_in: ?exec.PredicateExpr,
    derived: []const exec.Derived,
    assignments: []const Assignment,
) !usize {
    // Validate + widen predicate literals up front so every per-row-group
    // eval sees the same shape.
    var filter: ?DmlFilter = if (pred_in) |p| try DmlFilter.init(t.allocator, t.schema.columns, p, derived) else null;
    defer if (filter) |*f| f.deinit();
    const filter_ref: ?*const DmlFilter = if (filter) |*f| f else null;

    // Verify every assigned column exists.
    for (assignments) |asn| {
        _ = types.findColumn(t.schema.columns, asn.col) orelse return exec.Error.ColumnNotFound;
    }

    t.mutex.lockUncancelable(t.io);
    var wal_target: ?u64 = null;
    var affected: usize = 0;
    {
        defer t.mutex.unlock(t.io);
        try t.ensureUsable();

        // Snapshot bounds. These freeze for the duration of the UPDATE.
        const segs_at_start = t.manifest.segments.items.len;
        const mt_rows_at_start: usize = @intCast(t.memtable.row_count);

        // Batches applied before a failure keep their replacement rows in
        // the memtable, so their logged offsets must still reach the
        // segments. If even that fails, only a reopen (which replays the
        // log) restores a consistent table.
        errdefer t.mergeLoggedTombstonesLocked() catch t.requireRecovery();

        // -- Phase 1: memtable rows [0..mt_rows_at_start] --------
        if (mt_rows_at_start > 0) {
            affected += processMemtable(t, filter_ref, assignments, mt_rows_at_start, &wal_target) catch |err| switch (err) {
                error.ColumnTypeMismatch => return exec.Error.TypeMismatch,
                else => return err,
            };
        }

        // -- Phase 2: segments[0..segs_at_start] -----------------
        if (segs_at_start > 0) {
            affected += processSegments(t, filter_ref, assignments, segs_at_start, &wal_target) catch |err| switch (err) {
                error.ColumnTypeMismatch => return exec.Error.TypeMismatch,
                else => return err,
            };
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
    filter: ?*const DmlFilter,
    assignments: []const Assignment,
    mt_rows_at_start: usize,
    wal_target: *?u64,
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
    if (filter) |f| {
        try f.evaluate(allocator, batch, mask);
    } else {
        @memset(mask, true);
    }

    var matched_count: usize = 0;
    for (mask) |m| if (m) {
        matched_count += 1;
    };
    if (matched_count == 0) return 0;

    var matched = try materializeMatched(t, batch, mask, matched_count);
    defer freeMaterializedRows(allocator, &matched);
    var new_rows = try computeNewRows(t, matched, &.{}, assignments);
    defer freeMaterializedRows(allocator, &new_rows);

    const keep = try allocator.alloc(bool, mt_rows_at_start);
    defer allocator.free(keep);
    for (mask, keep) |m, *k| k.* = !m;

    const replaced: Table.Replaced = .{ .memtable = .{ .keep = keep, .rows = matched.stores, .row_count = matched.row_count } };
    if (try t.replaceRowsLocked(replaced, new_rows.stores, new_rows.row_count)) |target| wal_target.* = target;
    return matched_count;
}

// =============================================================================
// Phase 2 — segment iteration.
// =============================================================================

fn processSegments(
    t: *Table,
    filter: ?*const DmlFilter,
    assignments: []const Assignment,
    segs_at_start: usize,
    wal_target: *?u64,
) !usize {
    // Full-key Bloom gate (#143): a keyed UPDATE (every order-key column
    // pinned by AND-equality) skips segments whose Bloom rejects the key(s)
    // — this path otherwise decodes EVERY column of EVERY row group.
    var gate_arena = std.heap.ArenaAllocator.init(t.allocator);
    defer gate_arena.deinit();
    const upsert_mod = @import("upsert.zig");
    const key_hashes: ?[]u64 = if (filter) |f|
        upsert_mod.keyHashesFromPredicateExpr(t, gate_arena.allocator(), f.predicate) catch null
    else
        null;

    var total: usize = 0;
    var i: usize = 0;
    while (i < segs_at_start) : (i += 1) {
        const entry = t.manifest.segments.items[i];
        if (key_hashes) |hs| {
            if (!upsert_mod.bloomAdmitsAny(entry.key_bloom, hs)) continue;
        }
        total += try processOneSegment(t, filter, assignments, entry, wal_target);
    }
    return total;
}

fn processOneSegment(
    t: *Table,
    filter: ?*const DmlFilter,
    assignments: []const Assignment,
    entry: storage.manifest.ManifestEntry,
    wal_target: *?u64,
) !usize {
    const allocator = t.allocator;
    var live = try LiveSegment.open(t, entry.segment_id);
    defer live.close(t);
    const seg = live.segment();

    var offsets: std.ArrayList(u32) = .empty;
    defer offsets.deinit(allocator);
    var deleted: usize = 0;

    var row_offset: u32 = 0;
    for (seg.info.row_groups, 0..) |rg, rg_idx| {
        const n = rg.row_count;

        // Decode all schema columns for this row group. (Future: only
        // decode columns referenced by predicate + assignments.)
        const owned_cols = try allocator.alloc(storage.OwnedColumn, t.schema.columns.len);
        var decoded: usize = 0;
        defer {
            for (owned_cols[0..decoded]) |*oc| oc.deinit(allocator);
            allocator.free(owned_cols);
        }
        for (owned_cols, 0..) |*oc, ci| {
            oc.* = try seg.decodeColumn(allocator, t.schema, rg_idx, ci);
            decoded += 1;
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
        if (filter) |f| {
            try f.evaluate(allocator, batch, mask);
        } else {
            @memset(mask, true);
        }

        const matched_in_rg = live.keepLive(row_offset, mask);

        if (matched_in_rg > 0) {
            offsets.clearRetainingCapacity();
            for (mask, 0..) |m, k| if (m) {
                try offsets.append(allocator, row_offset + @as(u32, @intCast(k)));
            };

            var matched = try materializeMatched(t, batch, mask, matched_in_rg);
            defer freeMaterializedRows(allocator, &matched);
            var new_rows = try computeNewRows(t, matched, &.{}, assignments);
            defer freeMaterializedRows(allocator, &new_rows);

            const replaced: Table.Replaced = .{ .segment = .{ .id = entry.segment_id, .offsets = offsets.items } };
            if (try t.replaceRowsLocked(replaced, new_rows.stores, new_rows.row_count)) |target| wal_target.* = target;
            deleted += matched_in_rg;
        }
        row_offset += n;
    }

    // One tombstone-file rewrite per segment, not per row group.
    try t.mergeLoggedTombstonesLocked();
    return deleted;
}

// =============================================================================
// Helpers — compute new-row batch via Compute, materialize, insert.
// =============================================================================

/// Rows with one ColumnStore per table schema column. Lives for the
/// duration of one batch's processing.
pub const MaterializedRows = struct {
    stores: []ColumnStore,
    row_count: usize,
};

pub fn freeMaterializedRows(allocator: std.mem.Allocator, rows: *MaterializedRows) void {
    for (rows.stores) |*c| c.deinit(allocator);
    allocator.free(rows.stores);
}

/// Copy out the `matched_count` rows of `batch` that `mask` selects.
fn materializeMatched(t: *Table, batch: exec.Batch, mask: []const bool, matched_count: usize) !MaterializedRows {
    const allocator = t.allocator;
    const stores = try allocator.alloc(ColumnStore, t.schema.columns.len);
    var inited: usize = 0;
    errdefer {
        for (stores[0..inited]) |*c| c.deinit(allocator);
        allocator.free(stores);
    }
    for (t.schema.columns, stores) |sc, *store| {
        store.* = try ColumnStore.init(allocator, sc.type, sc.nullable);
        inited += 1;
    }
    for (batch.values, stores) |view, *store| {
        try engine.transform.appendMaskedColumn(allocator, view, mask, store);
    }
    return .{ .stores = stores, .row_count = matched_count };
}

/// Given the matched rows + assignments, produce rows where each column
/// carries either the post-assignment value (for assigned cols) or the
/// original value (for the others), via a SingleBatchSource(matched) →
/// Compute(synthetic derived) pipeline. `incoming` (empty for UPDATE) holds
/// one more row per matched row, in the table's column order, that the
/// assignments see under `upsert.incoming_prefix`: the row an INSERT ... ON
/// DUPLICATE KEY UPDATE would have inserted.
pub fn computeNewRows(
    t: *Table,
    matched: MaterializedRows,
    incoming: []const ColumnView,
    assignments: []const Assignment,
) !MaterializedRows {
    const allocator = t.allocator;
    const schema = t.schema;
    const matched_count = matched.row_count;

    const width = schema.columns.len + incoming.len;
    const filtered_views = try allocator.alloc(ColumnView, width);
    defer allocator.free(filtered_views);
    for (matched.stores, filtered_views[0..schema.columns.len]) |*c, *v| v.* = c.view();
    @memcpy(filtered_views[schema.columns.len..], incoming);
    const batch_schema = try allocator.alloc(types.Column, width);
    defer allocator.free(batch_schema);
    @memcpy(batch_schema[0..schema.columns.len], schema.columns);
    var names_made: usize = 0;
    defer for (batch_schema[schema.columns.len..][0..names_made]) |c| allocator.free(c.name);
    for (batch_schema[schema.columns.len..], schema.columns[0..incoming.len]) |*dst, c| {
        dst.* = c;
        dst.name = try std.mem.concat(allocator, u8, &.{ upsert.incoming_prefix, c.name });
        names_made += 1;
    }
    const filtered_batch: exec.Batch = .{
        .schema = batch_schema,
        .values = filtered_views,
        .row_count = matched_count,
    };

    // Wrap in SingleBatchSource and chain through Compute.
    // Compute's `Derived` list uses synthetic names so its outputs
    // don't collide with the upstream cols of the same name.
    const synth_names = try allocator.alloc([]u8, assignments.len);
    var synth_made: usize = 0;
    defer {
        for (synth_names[0..synth_made]) |s| allocator.free(s);
        allocator.free(synth_names);
    }
    const derived = try allocator.alloc(exec.Derived, assignments.len);
    defer allocator.free(derived);
    for (assignments, 0..) |asn, i| {
        synth_names[i] = try std.fmt.allocPrint(allocator, "__upd_{d}__{s}", .{ i, asn.col });
        synth_made += 1;
        derived[i] = .{ .name = synth_names[i], .expr = asn.value };
    }

    var src_q = try @import("../exec/single_batch.zig").SingleBatchSource.create(allocator, filtered_batch);
    var compute_q = src_q.compute(derived) catch |err| {
        src_q.deinit();
        return err;
    };
    defer compute_q.deinit();

    // Drain Compute (just one batch out, since input is one batch).
    var got: ?exec.Batch = null;
    while (try compute_q.next()) |b| {
        if (b.row_count == 0) continue;
        got = b;
        break;
    }
    const out = got orelse return error.UpdateNoRowsFromCompute;

    // For each table schema column, pick either the synthetic (assigned)
    // or the matched original.
    const out_stores = try allocator.alloc(ColumnStore, schema.columns.len);
    var out_inited: usize = 0;
    errdefer {
        for (out_stores[0..out_inited]) |*c| c.deinit(allocator);
        allocator.free(out_stores);
    }

    for (schema.columns, 0..) |sc, ci| {
        var src_view: ColumnView = matched.stores[ci].view();
        var src_type = sc.type;
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
                        src_type = out_col.type;
                        break;
                    }
                }
                break;
            }
        }
        const assigned: ?ColumnView = if (cast.assignsByRule(src_type, sc.type))
            try cast.assignColumn(allocator, src_view, src_type, sc.type, matched_count)
        else
            null;
        defer if (assigned) |view| cast.freeAssignedColumn(allocator, view);
        if (assigned) |view| src_view = view;

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

    return .{ .stores = out_stores, .row_count = matched_count };
}
