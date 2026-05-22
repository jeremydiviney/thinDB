//! Compaction: merge a set of segments into one, dropping tombstoned rows.
//!
//! Two entry points:
//!   - `execCompact(t)` merges every live segment (the "full" variant —
//!     what `Table.compact()` exposes to users).
//!   - `execTieredCompact(t)` picks a subset via the tiered LSM strategy
//!     and merges only those (drives the background compactor).
//!
//! Both call into `mergeSegments` which is the shared work.

const std = @import("std");
const storage = @import("../storage/storage.zig");
const engine = @import("../engine/engine.zig");

const api = @import("api.zig");
const Table = api.Table;
const comparison = @import("comparison.zig");

// Tiering constants. See DESIGN.md §7.2.
//
// Segments are bucketed by row count into tiers. Each tier is `tier_ratio`×
// larger than the previous. When `tier_target_count` adjacent (by segment_id)
// segments accumulate at the same tier, they get merged into one and the
// output joins the next tier up.

/// Row count cap for tier 0. Roughly one row group's worth.
pub const tier_base_rows: u64 = 65_536;
/// Each tier holds segments up to `tier_ratio`× the previous tier's cap.
pub const tier_ratio: u64 = 4;
/// Number of adjacent same-tier segments that triggers a merge.
pub const tier_target_count: usize = 4;

/// The tier index of a segment, derived from its row count. Tier 0 is the
/// smallest; the cap doubles by `tier_ratio` per level.
pub fn segmentTier(rows: u64) u8 {
    if (rows == 0) return 0;
    var t: u8 = 0;
    var bucket_cap: u64 = tier_base_rows;
    while (rows >= bucket_cap and t < 31) : (t += 1) {
        bucket_cap *|= tier_ratio;
    }
    return t;
}

/// Implements `Table.compact` (the "merge all" variant).
pub fn execCompact(t: *Table) !void {
    const all_ids = try t.allocator.alloc(u64, t.manifest.segments.items.len);
    defer t.allocator.free(all_ids);
    for (t.manifest.segments.items, 0..) |entry, i| all_ids[i] = entry.segment_id;
    try mergeSegments(t, all_ids);
}

/// Background compactor entry point. Picks one compaction group and
/// merges it. First check is tombstone pressure: any segment with
/// `tombs / row_count >= tomb_threshold` is compacted (with adjacent
/// same-tier neighbors if there are any). If no tombstone-pressured
/// segment, fall back to the count-based tier picker. No-op if no
/// group qualifies.
///
/// Pass `tomb_threshold > 1.0` to disable the tombstone trigger.
pub fn execTieredCompact(t: *Table, tomb_threshold: f32) !void {
    // 1. Tombstone pressure first.
    if (tomb_threshold <= 1.0) {
        const tomb_pick = try pickTombstonePressuredGroup(t, tomb_threshold);
        if (tomb_pick) |ids| {
            defer t.allocator.free(ids);
            try mergeSegments(t, ids);
            return;
        }
    }
    // 2. Tier-based count trigger.
    const seg_ids = try pickCompactionGroup(t.allocator, t.manifest.segments.items) orelse return;
    defer t.allocator.free(seg_ids);
    try mergeSegments(t, seg_ids);
}

/// Find the first segment whose tombstone fraction exceeds `threshold`
/// and return it together with adjacent same-tier neighbors (up to
/// `tier_target_count` total). If no neighbors share the tier, returns
/// just the single segment (a "rewrite to drop tombs" compaction).
/// Caller owns the returned slice.
fn pickTombstonePressuredGroup(t: *Table, threshold: f32) !?[]u64 {
    for (t.manifest.segments.items, 0..) |entry, i| {
        if (entry.row_count == 0) continue;
        const tombs = try storage.tombstone.read(t.allocator, t.io, t.segments_dir, entry.segment_id);
        defer if (tombs) |arr| t.allocator.free(arr);
        if (tombs == null) continue;
        const ratio = @as(f32, @floatFromInt(tombs.?.len)) / @as(f32, @floatFromInt(entry.row_count));
        if (ratio < threshold) continue;

        // Found a heavily-tombstoned segment. Grow to a run of adjacent
        // same-tier neighbors so the merged output stays on tier.
        const tier = segmentTier(entry.row_count);
        var lo: usize = i;
        var hi: usize = i + 1;
        while (lo > 0 and segmentTier(t.manifest.segments.items[lo - 1].row_count) == tier and (hi - lo) < tier_target_count) {
            lo -= 1;
        }
        while (hi < t.manifest.segments.items.len and segmentTier(t.manifest.segments.items[hi].row_count) == tier and (hi - lo) < tier_target_count) {
            hi += 1;
        }
        const out = try t.allocator.alloc(u64, hi - lo);
        for (out, 0..) |*id, j| id.* = t.manifest.segments.items[lo + j].segment_id;
        return out;
    }
    return null;
}

/// Walk the manifest looking for `tier_target_count` adjacent-by-position
/// segments at the same tier. Returns owned slice of segment IDs to merge,
/// or null if no run qualifies. Prefers the smallest tier (most segments
/// to absorb early).
pub fn pickCompactionGroup(
    allocator: std.mem.Allocator,
    segments: []const storage.ManifestEntry,
) !?[]u64 {
    if (segments.len < tier_target_count) return null;

    // Scan for the first run of tier_target_count adjacent segments at
    // the same tier. Prefer lower tiers by walking from smallest tier up.
    var best_tier: ?u8 = null;
    var best_start: usize = 0;
    var run_start: usize = 0;
    var run_tier: u8 = segmentTier(segments[0].row_count);
    for (segments[1..], 1..) |seg, i| {
        const t = segmentTier(seg.row_count);
        if (t != run_tier) {
            // Close out the previous run.
            const run_len = i - run_start;
            if (run_len >= tier_target_count) {
                if (best_tier == null or run_tier < best_tier.?) {
                    best_tier = run_tier;
                    best_start = run_start;
                }
            }
            run_start = i;
            run_tier = t;
        }
    }
    // Final run.
    const final_run_len = segments.len - run_start;
    if (final_run_len >= tier_target_count) {
        if (best_tier == null or run_tier < best_tier.?) {
            best_tier = run_tier;
            best_start = run_start;
        }
    }

    if (best_tier == null) return null;

    const out = try allocator.alloc(u64, tier_target_count);
    for (out, 0..) |*id, i| id.* = segments[best_start + i].segment_id;
    return out;
}

/// Merge the given segments into one. `seg_ids` must be a non-empty subset
/// of the table's current manifest segments. The new segment replaces them
/// in the manifest; old segment + tombstone files are deleted.
pub fn mergeSegments(t: *Table, seg_ids: []const u64) !void {
    if (seg_ids.len == 0) return;

    var work = try engine.Memtable.init(t.allocator, t.schema);
    defer work.deinit();

    for (seg_ids) |id| {
        var name_buf: [32]u8 = undefined;
        const file_name = try Table.segmentFileName(&name_buf, id);
        var seg = try storage.readSegment(t.allocator, t.io, t.segments_dir, file_name, t.schema);
        defer seg.deinit();

        const tombs = try storage.tombstone.read(t.allocator, t.io, t.segments_dir, id);
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

    // Build the new manifest: keep segments not in `seg_ids`, in original
    // order, and slot the new merged segment in where the first input was.
    var keep: std.ArrayList(storage.ManifestEntry) = .empty;
    defer keep.deinit(t.allocator);
    try keep.ensureTotalCapacity(t.allocator, t.manifest.segments.items.len);

    var first_dropped_idx: ?usize = null;
    for (t.manifest.segments.items, 0..) |entry, idx| {
        var is_input = false;
        for (seg_ids) |id| if (id == entry.segment_id) {
            is_input = true;
            if (first_dropped_idx == null) first_dropped_idx = idx;
            break;
        };
        if (!is_input) {
            keep.appendAssumeCapacity(entry);
        } else {
            // Dropped entry — Manifest no longer reaches its owned slices,
            // so free them now (the kept entries keep theirs via pointer
            // copy into `keep`).
            if (entry.column_stats.len > 0) t.allocator.free(entry.column_stats);
            if (entry.column_sketches.len > 0) t.allocator.free(entry.column_sketches);
        }
    }

    const sync = t.syncEnabled();

    if (work.row_count == 0) {
        // Everything was tombstoned. Just drop the inputs from the manifest.
        t.manifest.segments.clearRetainingCapacity();
        try t.manifest.segments.appendSlice(t.allocator, keep.items);
        try storage.writeManifest(t.io, t.table_dir, t.manifest, sync);
        for (seg_ids) |id| try t.deleteSegmentFiles(id);
        return;
    }

    // Write merged output.
    const new_seg_id = t.allocSegmentId();
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
        sync,
    );
    defer info.deinit(t.allocator);

    // Splice the new segment into the keep list at the position of the
    // first dropped input. Preserves order: older segments stay older.
    const insert_at = first_dropped_idx orelse keep.items.len;
    const new_entry = try t.entryFor(info);
    try keep.insert(t.allocator, @min(insert_at, keep.items.len), new_entry);

    t.manifest.segments.clearRetainingCapacity();
    try t.manifest.segments.appendSlice(t.allocator, keep.items);
    try storage.writeManifest(t.io, t.table_dir, t.manifest, sync);

    for (seg_ids) |id| try t.deleteSegmentFiles(id);
}

// ---------- tests --------------------------------------------------------

test "segmentTier buckets row counts logarithmically" {
    try std.testing.expectEqual(@as(u8, 0), segmentTier(0));
    try std.testing.expectEqual(@as(u8, 0), segmentTier(1));
    try std.testing.expectEqual(@as(u8, 0), segmentTier(65_535));
    try std.testing.expectEqual(@as(u8, 1), segmentTier(65_536));
    try std.testing.expectEqual(@as(u8, 1), segmentTier(65_536 * 3));
    try std.testing.expectEqual(@as(u8, 2), segmentTier(65_536 * 4));
    try std.testing.expectEqual(@as(u8, 3), segmentTier(65_536 * 16));
}
