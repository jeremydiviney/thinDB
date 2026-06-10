//! Unique-key upsert resolution. Implements StarRocks-style "last writer
//! wins" semantics on tables created with `unique = true`. Called from
//! `Table.insert` after `insertRows` lands the new rows.

const std = @import("std");
const Allocator = std.mem.Allocator;

const storage = @import("../storage/storage.zig");
const engine = @import("../engine/engine.zig");

const api = @import("api.zig");
const Table = api.Table;
const comparison = @import("comparison.zig");

/// After `insertRows`, every newly-inserted row whose order key already
/// exists somewhere in the table (older memtable row, or a flushed segment)
/// causes the older copy to be tombstoned. Always keeps the LAST occurrence
/// in the memtable.
pub fn applyUpsertResolution(t: *Table) !void {
    std.debug.assert(t.order_key_indices.len > 0);

    var arena = std.heap.ArenaAllocator.init(t.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    const n: usize = @intCast(t.memtable.row_count);
    if (n == 0) return;

    // ---- 1. Intra-memtable dedup: keep only the last occurrence per key. --
    const keep = try t.allocator.alloc(bool, n);
    defer t.allocator.free(keep);
    @memset(keep, true);

    var last_seen: std.StringHashMapUnmanaged(u32) = .empty;
    // Owned by arena; no explicit deinit needed.

    for (0..n) |i| {
        const key_bytes = try compoundKeyFromColumnStores(aa, t.memtable.columns, t.order_key_indices, @intCast(i));
        const gop = try last_seen.getOrPut(aa, key_bytes);
        if (gop.found_existing) keep[gop.value_ptr.*] = false;
        gop.value_ptr.* = @intCast(i);
    }

    // Snapshot-isolated retire-replace if any rows were dropped. Scans that
    // pinned the pre-resolution memtable keep seeing those rows; new scans
    // see the deduped state.
    if (try t.memtable.cloneWithRetainedRows(t.allocator, keep)) |new_mt| {
        const old_mt = t.memtable;
        t.memtable = new_mt;
        old_mt.retire();
        old_mt.release();
    }

    // ---- 2. Build a set of surviving keys to probe segments with. --------
    const surviving_n: usize = @intCast(t.memtable.row_count);
    if (surviving_n == 0 or t.manifest.segments.items.len == 0) return;

    var surviving_set: std.StringHashMapUnmanaged(void) = .empty;
    try surviving_set.ensureTotalCapacity(aa, @intCast(surviving_n));
    for (0..surviving_n) |i| {
        const key_bytes = try compoundKeyFromColumnStores(aa, t.memtable.columns, t.order_key_indices, @intCast(i));
        surviving_set.putAssumeCapacity(key_bytes, {});
    }

    // ---- 3. For each segment, scan row groups, find matching keys. --------
    for (t.manifest.segments.items) |entry| {
        var name_buf: [32]u8 = undefined;
        const file_name = try Table.segmentFileName(&name_buf, entry.segment_id);
        var seg = try storage.readSegment(t.allocator, t.io, t.segments_dir, file_name, t.schema);
        defer seg.deinit();

        var deleted: std.ArrayList(u32) = .empty;
        defer deleted.deinit(t.allocator);

        var row_offset: u32 = 0;
        for (seg.info.row_groups, 0..) |rg, rg_idx| {
            // Decode all order-key columns for this row group. Compound keys
            // with a string component would need string stats to prune at
            // the row-group level; for now, always scan.
            const decoded_keys = try aa.alloc(storage.OwnedColumn, t.order_key_indices.len);
            for (t.order_key_indices, 0..) |col_idx, i| {
                decoded_keys[i] = try seg.decodeColumn(t.allocator, t.schema, rg_idx, col_idx);
            }
            defer for (decoded_keys) |*c| {
                var d = c.*;
                d.deinit(t.allocator);
            };

            const rg_n = rg.row_count;
            var row: u32 = 0;
            while (row < rg_n) : (row += 1) {
                const key_bytes = try compoundKeyFromOwnedColumns(aa, decoded_keys, row);
                if (surviving_set.contains(key_bytes)) {
                    try deleted.append(t.allocator, row_offset + row);
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
        }
    }
}

/// Pack the order-key columns of `row` from a memtable's `ColumnStore` array
/// into a contiguous byte slice suitable for hashing/comparison. Allocated
/// in `aa`; lifetime = arena.
fn compoundKeyFromColumnStores(
    aa: Allocator,
    columns: []const engine.ColumnStore,
    key_indices: []const usize,
    row: u32,
) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    for (key_indices) |ci| {
        try comparison.appendColumnValueBytes(aa, &buf, columns[ci].view(), row);
    }
    return buf.toOwnedSlice(aa);
}

/// Same as above but for the per-row-group decoded `OwnedColumn` array used
/// during segment scans.
fn compoundKeyFromOwnedColumns(
    aa: Allocator,
    decoded: []storage.OwnedColumn,
    row: u32,
) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    for (decoded) |c| {
        try comparison.appendColumnValueBytes(aa, &buf, c.view(), row);
    }
    return buf.toOwnedSlice(aa);
}
