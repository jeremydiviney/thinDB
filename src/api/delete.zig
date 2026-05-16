//! Delete-by-predicate orchestration. Per segment: prune by stats where
//! possible, otherwise decode the predicate column and emit tombstones for
//! matching rows. Memtable rows are filtered in-place via retainRows.

const std = @import("std");
const ValueTag = @import("../types.zig").ValueTag;
const storage = @import("../storage/storage.zig");
const exec = @import("../exec/exec.zig");

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
            // Quick prune via stats for fixed-width columns.
            if (!col_type.isString() and !exec.statsOverlapPredicate(rg.stats[col_idx], pred.op, pred.val)) {
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
            );
            total += deleted.items.len;
        }
    }

    // ---- Memtable ----
    if (t.memtable.row_count > 0) {
        const n: usize = @intCast(t.memtable.row_count);
        const keep = try t.allocator.alloc(bool, n);
        defer t.allocator.free(keep);
        const view = t.memtable.columns[col_idx].view();
        for (0..n) |i| keep[i] = !comparison.evalRow(view, @intCast(i), pred);

        const kept = try t.memtable.retainRows(keep);
        total += n - kept;
    }

    return total;
}
