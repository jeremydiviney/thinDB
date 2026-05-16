//! Compaction primitive: full merge of all live segments into a single new
//! segment, dropping tombstoned rows. Tiered/incremental compaction is
//! a future variant on top of this primitive.

const std = @import("std");
const storage = @import("../storage/storage.zig");
const engine = @import("../engine/engine.zig");

const api = @import("api.zig");
const Table = api.Table;
const comparison = @import("comparison.zig");

/// Implements `Table.compact` (the "merge all" variant).
pub fn execCompact(t: *Table) !void {
    var work = try engine.Memtable.init(t.allocator, t.schema);
    defer work.deinit();

    // Walk every segment, decode every row group, apply tombstones, append.
    for (t.manifest.segments.items) |entry| {
        var name_buf: [32]u8 = undefined;
        const file_name = try Table.segmentFileName(&name_buf, entry.segment_id);
        var seg = try storage.readSegment(t.allocator, t.io, t.segments_dir, file_name, t.schema);
        defer seg.deinit();

        const tombs = try storage.tombstone.read(t.allocator, t.io, t.segments_dir, entry.segment_id);
        defer if (tombs) |x| t.allocator.free(x);

        var row_offset: u32 = 0;
        for (seg.info.row_groups, 0..) |rg, rg_idx| {
            const decoded = try t.allocator.alloc(storage.OwnedColumn, t.schema.columns.len);
            defer t.allocator.free(decoded);

            var decoded_inited: usize = 0;
            defer for (decoded[0..decoded_inited]) |*c| c.deinit(t.allocator);

            for (t.schema.columns, 0..) |_, ci| {
                decoded[ci] = try seg.decodeColumn(t.allocator, t.schema, rg_idx, ci);
                decoded_inited += 1;
            }

            // Build keep mask using tombstones (if any).
            const mask = try t.allocator.alloc(bool, rg.row_count);
            defer t.allocator.free(mask);
            @memset(mask, true);
            if (tombs) |arr| {
                const rg_end = row_offset + rg.row_count;
                const lo = std.sort.lowerBound(u32, arr, row_offset, comparison.cmpU32);
                const hi = std.sort.lowerBound(u32, arr, rg_end, comparison.cmpU32);
                for (arr[lo..hi]) |off| {
                    mask[off - row_offset] = false;
                }
            }

            var kept: usize = 0;
            for (mask) |m| if (m) {
                kept += 1;
            };

            if (kept > 0) {
                for (work.columns, 0..) |*dst, ci| {
                    try engine.memtable.appendMaskedColumn(t.allocator, decoded[ci].view(), mask, dst);
                }
                work.row_count += @intCast(kept);
            }

            row_offset += rg.row_count;
        }
    }

    // Capture old IDs for cleanup.
    var old_ids: std.ArrayList(u64) = .empty;
    defer old_ids.deinit(t.allocator);
    for (t.manifest.segments.items) |e| try old_ids.append(t.allocator, e.segment_id);

    if (work.row_count == 0) {
        // Everything was tombstoned. Just empty the manifest + delete files.
        t.manifest.segments.clearRetainingCapacity();
        try storage.writeManifest(t.io, t.table_dir, t.manifest);
        for (old_ids.items) |old| try t.deleteSegmentFiles(old);
        return;
    }

    // Sort + write as a new segment.
    const new_seg_id = t.manifest.nextSegmentId();
    var name_buf: [32]u8 = undefined;
    const file_name = try Table.segmentFileName(&name_buf, new_seg_id);

    var snapshot = try work.buildSortedSnapshot(t.allocator, t.order_key_indices);
    defer snapshot.deinit();

    var info = try storage.writeSegment(
        t.allocator,
        t.io,
        t.segments_dir,
        file_name,
        t.schema,
        new_seg_id,
        t.schema_fingerprint,
        t.row_group_size,
        snapshot.views,
    );
    defer info.deinit(t.allocator);

    // Atomically swap the manifest contents.
    const new_row_count: u64 = work.row_count;
    t.manifest.segments.clearRetainingCapacity();
    try t.manifest.appendSegment(.{ .segment_id = new_seg_id, .row_count = new_row_count });
    try storage.writeManifest(t.io, t.table_dir, t.manifest);

    // Clean up old segment + tomb files.
    for (old_ids.items) |old| try t.deleteSegmentFiles(old);
}
