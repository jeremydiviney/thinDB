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
const bloom = @import("../util/bloom.zig");

/// Hash each row's compound primary key and append to `out`. Segment writers
/// (flush + compaction) call this to build the per-segment key Bloom; it uses
/// the SAME key-byte encoding as the probe below, so hashes line up. `columns`
/// is the full column set; `key_indices` selects the order-key columns.
pub fn appendKeyHashes(
    allocator: Allocator,
    out: *std.ArrayList(u64),
    columns: []const storage.ColumnView,
    key_indices: []const usize,
    row_count: usize,
) !void {
    var keybuf: std.ArrayList(u8) = .empty;
    defer keybuf.deinit(allocator);
    var r: u32 = 0;
    while (r < row_count) : (r += 1) {
        keybuf.clearRetainingCapacity();
        for (key_indices) |ci| try comparison.appendColumnValueBytes(allocator, &keybuf, columns[ci], r);
        try out.append(allocator, bloom.keyHash(keybuf.items));
    }
}

/// Build + serialize a key Bloom from accumulated hashes. Caller owns the slice.
pub fn serializeKeyBloom(allocator: Allocator, hashes: []const u64) ![]u8 {
    var bf = try bloom.Bloom.build(allocator, hashes, bloom.default_bits_per_key);
    defer bf.deinit(allocator);
    const out = try allocator.alloc(u8, bf.serializedLen());
    _ = bf.writeTo(out);
    return out;
}

/// After `insertRows`, every newly-inserted row whose order key already
/// exists somewhere in the table (older memtable row, or a flushed segment)
/// causes the older copy to be tombstoned. Always keeps the LAST occurrence
/// in the memtable.
/// Drop the persistent index (memtable swapped/emptied). Keeps map + arena
/// capacity for reuse; the next resolution rebuilds from row 0.
fn upsertIndexReset(t: *Table) void {
    t.upsert_idx.clearRetainingCapacity();
    if (t.upsert_idx_arena) |*a| _ = a.reset(.retain_capacity);
    t.upsert_idx_mt = null;
    t.upsert_idx_rows = 0;
}

pub fn applyUpsertResolution(t: *Table) !void {
    std.debug.assert(t.order_key_indices.len > 0);

    const n: usize = @intCast(t.memtable.row_count);
    if (n == 0) {
        upsertIndexReset(t);
        return;
    }

    // Bind the persistent key index to the current memtable generation. A
    // memtable swap (flush / dedup-clone) or shrink invalidates it → rebuild
    // from row 0; otherwise process only the rows added since last time.
    const same_gen = if (t.upsert_idx_mt) |m| m == t.memtable else false;
    if (!same_gen or t.upsert_idx_rows > n) {
        upsertIndexReset(t);
        t.upsert_idx_mt = t.memtable;
    }
    if (t.upsert_idx_arena == null) t.upsert_idx_arena = std.heap.ArenaAllocator.init(t.allocator);
    const idx_aa = t.upsert_idx_arena.?.allocator();
    const start: usize = t.upsert_idx_rows;

    // Scratch for this batch's temporaries (probe key set).
    var arena = std.heap.ArenaAllocator.init(t.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    // ---- 1. Incremental intra-memtable dedup: only the NEW rows. Look each
    // new row's key up in the persistent index; a hit tombstones the older
    // memtable row (last writer wins) and a miss is a key we must also probe
    // against segments (below).
    var dropped: std.ArrayList(u32) = .empty;
    defer dropped.deinit(t.allocator);
    var new_keys: std.ArrayList([]const u8) = .empty; // arena-owned; survive an index reset

    for (start..n) |i| {
        const key_bytes = try compoundKeyFromColumnStores(idx_aa, t.memtable.columns, t.order_key_indices, @intCast(i));
        const gop = try t.upsert_idx.getOrPut(t.allocator, key_bytes);
        if (gop.found_existing) {
            try dropped.append(t.allocator, gop.value_ptr.*);
        } else {
            try new_keys.append(aa, try aa.dupe(u8, key_bytes));
        }
        gop.value_ptr.* = @intCast(i);
    }
    t.upsert_idx_rows = @intCast(n);

    // Snapshot-isolated retire-replace for the tombstoned older rows. Scans
    // that pinned the pre-resolution memtable keep seeing them; new scans see
    // the deduped state. The swap changes row indices, so drop the index (the
    // next batch rebuilds against the cloned memtable).
    if (dropped.items.len > 0) {
        const keep = try t.allocator.alloc(bool, n);
        defer t.allocator.free(keep);
        @memset(keep, true);
        for (dropped.items) |d| keep[d] = false;
        if (try t.memtable.cloneWithRetainedRows(t.allocator, keep)) |new_mt| {
            const old_mt = t.memtable;
            t.memtable = new_mt;
            old_mt.retire();
            old_mt.release();
            upsertIndexReset(t);
        }
    }

    // ---- 2. Probe segments only for keys NEW to the memtable this batch. A
    // key needs a segment tombstone check exactly once — when it first enters
    // the memtable; a re-insert already tombstoned its segment match.
    if (new_keys.items.len == 0 or t.manifest.segments.items.len == 0) return;

    var surviving_set: std.StringHashMapUnmanaged(void) = .empty;
    try surviving_set.ensureTotalCapacity(aa, @intCast(new_keys.items.len));
    for (new_keys.items) |k| surviving_set.putAssumeCapacity(k, {});

    // Precompute this batch's Bloom hashes once; reused across every segment.
    const key_hashes = try aa.alloc(u64, new_keys.items.len);
    for (new_keys.items, 0..) |k, i| key_hashes[i] = bloom.keyHash(k);

    // ---- 3. For each segment, scan row groups, find matching keys. --------
    for (t.manifest.segments.items) |entry| {
        // Bloom prune: if the segment carries a key filter and none of this
        // batch's keys may be present, skip it entirely — no file open, no
        // decode. Turns the probe from O(all segment rows) into O(survivors),
        // which is the fix for the O(n²) bulk-upsert load (#138).
        if (entry.key_bloom.len > 0) {
            var maybe = false;
            for (key_hashes) |h| {
                if (bloom.Bloom.mayContainSerialized(entry.key_bloom, h)) {
                    maybe = true;
                    break;
                }
            }
            if (!maybe) continue;
        }
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
