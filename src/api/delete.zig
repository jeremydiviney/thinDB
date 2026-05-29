//! Delete-by-predicate orchestration. Per segment: prune by stats where
//! possible, otherwise decode the predicate column and emit tombstones for
//! matching rows. Memtable rows are filtered in-place via retainRows.

const std = @import("std");
const types = @import("../types.zig");
const ValueTag = types.ValueTag;
const Column = types.Column;
const storage = @import("../storage/storage.zig");
const exec = @import("../exec/exec.zig");
const predicate = exec.predicate;

const api = @import("api.zig");
const Table = api.Table;
const comparison = @import("comparison.zig");

/// Implements `Table.delete`. Returns the number of rows deleted.
pub fn execDelete(t: *Table, pred: exec.Predicate) !usize {
    const col_idx = t.schema.columnIndex(pred.col) orelse return exec.Error.ColumnNotFound;
    const col_type = t.schema.columns[col_idx].type;
    if (ValueTag.fromType(col_type) != std.meta.activeTag(pred.val)) {
        return exec.Error.PredicateTypeMismatch;
    }
    if (col_type.isString() and pred.op != .eq and pred.op != .neq) {
        return exec.Error.UnsupportedOperatorForType;
    }

    var total: usize = 0;

    // ---- Segments ----
    for (t.manifest.segments.items) |entry| {
        var name_buf: [32]u8 = undefined;
        const file_name = try Table.segmentFileName(&name_buf, entry.segment_id);
        var seg = try storage.readSegment(t.allocator, t.io, t.segments_dir, file_name, t.schema);
        defer seg.deinit();

        var deleted: std.ArrayList(u32) = .empty;
        defer deleted.deinit(t.allocator);

        var row_offset: u32 = 0;
        for (seg.info.row_groups, 0..) |rg, rg_idx| {
            // Quick prune via stats. String columns prune too (eq + range via
            // the 16-byte prefix class — `statsOverlapPredicate` stays
            // conservative on prefix ties, so no matching row is skipped).
            if (!exec.statsOverlapPredicate(rg.stats[col_idx], pred.op, pred.val)) {
                row_offset += rg.row_count;
                continue;
            }

            var col = try seg.decodeColumn(t.allocator, t.schema, rg_idx, col_idx);
            defer col.deinit(t.allocator);

            const n = rg.row_count;
            var i: u32 = 0;
            while (i < n) : (i += 1) {
                if (comparison.evalRow(col.view(), i, pred)) {
                    try deleted.append(t.allocator, row_offset + i);
                }
            }
            row_offset += rg.row_count;
        }

        if (deleted.items.len > 0) {
            try storage.tombstone.merge(
                t.allocator,
                t.io,
                t.segments_dir,
                entry.segment_id,
                deleted.items,
                t.syncEnabled(),
            );
            total += deleted.items.len;
        }
    }

    // ---- Memtable ----
    // Snapshot-isolated path: build a NEW memtable with the surviving rows,
    // atomically swap the table's pointer, retire the old. Concurrent scans
    // that captured the old memtable continue to see the pre-delete state
    // until they finish; the old memtable's columns are never mutated again.
    if (t.memtable.row_count > 0) {
        const n: usize = @intCast(t.memtable.row_count);
        const keep = try t.allocator.alloc(bool, n);
        defer t.allocator.free(keep);
        const view = t.memtable.columns[col_idx].view();
        for (0..n) |i| keep[i] = !comparison.evalRow(view, @intCast(i), pred);

        if (try t.memtable.cloneWithRetainedRows(t.allocator, keep)) |new_mt| {
            const old_mt = t.memtable;
            t.memtable = new_mt;
            old_mt.retire();
            old_mt.release();
            total += n - new_mt.row_count;
        }
    }

    return total;
}

/// `DELETE FROM t [WHERE expr]` — generalized delete accepting the
/// rich `PredicateExpr` (AND/OR/IN/etc). Same per-segment streaming
/// shape as `execDelete` — tombstone offsets accumulate per segment
/// (bounded by segment size, never by total table size), get merged
/// into the segment's tombstone file, then the buffer is freed
/// before moving to the next segment. Memtable rows are filtered
/// via clone-and-swap, same as the simple `execDelete` path.
///
/// `pred_or_null == null` means delete every row.
pub fn execDeleteByExpr(t: *Table, pred_in: ?predicate.PredicateExpr) !usize {
    var total: usize = 0;

    // Make a local mutable copy of the predicate so validateExpr can
    // widen literals in place (Zig function parameters are immutable,
    // so we can't mutate `pred_in` directly even via a const-cast).
    var pred_local: ?predicate.PredicateExpr = pred_in;
    if (pred_local) |*p| {
        try predicate.validateExpr(p, t.schema.columns);
    }
    const pred_or_null = pred_local;

    // ---- Segments ----
    for (t.manifest.segments.items) |entry| {
        var name_buf: [32]u8 = undefined;
        const file_name = try Table.segmentFileName(&name_buf, entry.segment_id);
        var seg = try storage.readSegment(t.allocator, t.io, t.segments_dir, file_name, t.schema);
        defer seg.deinit();

        var deleted: std.ArrayList(u32) = .empty;
        defer deleted.deinit(t.allocator);

        var row_offset: u32 = 0;
        for (seg.info.row_groups, 0..) |rg, rg_idx| {
            const n = rg.row_count;

            if (pred_or_null == null) {
                // No predicate → every row tombstoned. Skip decoding.
                var i: u32 = 0;
                while (i < n) : (i += 1) try deleted.append(t.allocator, row_offset + i);
                row_offset += n;
                continue;
            }

            // Decode all columns the predicate might touch — for v1
            // simplicity, decode the whole row group. (A future
            // optimization could pre-scan the predicate to learn
            // which columns are referenced.)
            const owned_cols = try t.allocator.alloc(storage.OwnedColumn, t.schema.columns.len);
            defer {
                for (owned_cols) |*oc| oc.deinit(t.allocator);
                t.allocator.free(owned_cols);
            }
            for (t.schema.columns, 0..) |_, ci| {
                owned_cols[ci] = try seg.decodeColumn(t.allocator, t.schema, rg_idx, ci);
            }

            const views = try t.allocator.alloc(storage.ColumnView, t.schema.columns.len);
            defer t.allocator.free(views);
            for (owned_cols, views) |oc, *v| v.* = oc.view();

            const fake_batch: exec.Batch = .{
                .schema = t.schema.columns,
                .values = views,
                .row_count = n,
            };
            const mask = try t.allocator.alloc(bool, n);
            defer t.allocator.free(mask);
            try predicate.evaluatePredicate(t.allocator, pred_or_null.?, t.schema.columns, fake_batch, mask);

            var i: u32 = 0;
            while (i < n) : (i += 1) {
                if (mask[i]) try deleted.append(t.allocator, row_offset + i);
            }
            row_offset += n;
        }

        if (deleted.items.len > 0) {
            try storage.tombstone.merge(
                t.allocator,
                t.io,
                t.segments_dir,
                entry.segment_id,
                deleted.items,
                t.syncEnabled(),
            );
            total += deleted.items.len;
        }
    }

    // ---- Memtable ----
    // Same snapshot-isolated clone-and-swap shape as `execDelete`.
    if (t.memtable.row_count > 0) {
        const n: usize = @intCast(t.memtable.row_count);
        const keep = try t.allocator.alloc(bool, n);
        defer t.allocator.free(keep);

        if (pred_or_null == null) {
            @memset(keep, false);
        } else {
            const views = try t.allocator.alloc(storage.ColumnView, t.schema.columns.len);
            defer t.allocator.free(views);
            for (t.memtable.columns, views) |*c, *v| v.* = c.view();
            const fake_batch: exec.Batch = .{
                .schema = t.schema.columns,
                .values = views,
                .row_count = n,
            };
            const mask = try t.allocator.alloc(bool, n);
            defer t.allocator.free(mask);
            try predicate.evaluatePredicate(t.allocator, pred_or_null.?, t.schema.columns, fake_batch, mask);
            for (mask, keep) |m, *k| k.* = !m;
        }

        if (try t.memtable.cloneWithRetainedRows(t.allocator, keep)) |new_mt| {
            const old_mt = t.memtable;
            t.memtable = new_mt;
            old_mt.retire();
            old_mt.release();
            total += n - new_mt.row_count;
        }
    }

    return total;
}
