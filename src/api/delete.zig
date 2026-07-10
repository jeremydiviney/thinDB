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
            t.seg_handles.invalidateTombstones(t.allocator, entry.segment_id);
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

const RgHint = struct {
    col_idx: usize,
    op: exec.PredicateOp,
    val: types.Value,
};

/// Walk a predicate collecting (a) row-group zonemap prune hints from
/// AND-tree leaf comparisons and (b) the set of columns the predicate
/// references. Errors on any node shape it doesn't understand — the caller
/// then falls back to decode-everything / prune-nothing, which is always
/// correct.
fn collectDeletePruneInfo(
    columns: []const Column,
    expr: predicate.PredicateExpr,
    aa: std.mem.Allocator,
    hints: *std.ArrayList(RgHint),
    ref_cols: []bool,
) !void {
    switch (expr) {
        .@"and" => |kids| for (kids) |k| try collectDeletePruneInfo(columns, k, aa, hints, ref_cols),
        .leaf => |p| {
            const ci = types.findColumn(columns, p.col) orelse return error.UnknownShape;
            ref_cols[ci] = true;
            try hints.append(aa, .{ .col_idx = ci, .op = p.op, .val = p.val });
        },
        .in_set => |s| {
            const ci = types.findColumn(columns, s.col) orelse return error.UnknownShape;
            ref_cols[ci] = true;
            // IN sets don't produce a single-op hint; referenced-column
            // tracking alone is the win here.
        },
        else => return error.UnknownShape,
    }
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

    // Full-key Bloom gate (#143): a keyed DELETE — every order-key column
    // pinned by AND-equality, the exact shape a CDC binlog delete takes —
    // can skip every segment whose Bloom rejects the key(s): no open, no
    // whole-row-group decode. Arena-scoped; null = not a keyed delete.
    var gate_arena = std.heap.ArenaAllocator.init(t.allocator);
    defer gate_arena.deinit();
    const key_hashes: ?[]u64 = if (pred_or_null) |p|
        @import("upsert.zig").keyHashesFromPredicateExpr(t, gate_arena.allocator(), p) catch null
    else
        null;

    // Row-group prune hints + referenced-column set. Top-level AND-leaf
    // conjuncts prune row groups via the footer zonemaps (a keyed CDC
    // delete on an order-key-sorted segment prunes to ~one row group);
    // whatever the predicate's shape, only the columns it references get
    // decoded. Falls back to no-prune/all-columns on unhandled shapes.
    const ga = gate_arena.allocator();
    var rg_hints: std.ArrayList(RgHint) = .empty;
    const ref_cols: []bool = try ga.alloc(bool, t.schema.columns.len);
    @memset(ref_cols, false);
    if (pred_or_null) |p| {
        collectDeletePruneInfo(t.schema.columns, p, ga, &rg_hints, ref_cols) catch {
            @memset(ref_cols, true);
            rg_hints.clearRetainingCapacity();
        };
    }

    // ---- Segments ----
    for (t.manifest.segments.items) |entry| {
        if (key_hashes) |hs| {
            if (!@import("upsert.zig").bloomAdmitsAny(entry.key_bloom, hs)) continue;
        }
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

            var rg_can_match = true;
            for (rg_hints.items) |h| {
                if (!predicate.statsOverlapPredicate(rg.stats[h.col_idx], h.op, h.val)) {
                    rg_can_match = false;
                    break;
                }
            }
            if (!rg_can_match) {
                row_offset += n;
                continue;
            }

            // Decode only the predicate's referenced columns; the rest get
            // empty placeholder views that predicate evaluation never reads.
            const owned_cols = try t.allocator.alloc(?storage.OwnedColumn, t.schema.columns.len);
            defer {
                for (owned_cols) |*oc| if (oc.*) |*c| c.deinit(t.allocator);
                t.allocator.free(owned_cols);
            }
            @memset(owned_cols, null);
            for (t.schema.columns, 0..) |_, ci| {
                if (ref_cols[ci]) {
                    owned_cols[ci] = try seg.decodeColumn(t.allocator, t.schema, rg_idx, ci);
                }
            }

            const views = try t.allocator.alloc(storage.ColumnView, t.schema.columns.len);
            defer t.allocator.free(views);
            for (owned_cols, views) |oc, *v| {
                v.* = if (oc) |c| c.view() else .{ .data = .{ .int = &.{} } };
            }

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
            t.seg_handles.invalidateTombstones(t.allocator, entry.segment_id);
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
