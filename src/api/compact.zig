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

/// A cheap snapshot of one segment's identity + size, copied out from
/// under the table mutex. The pick logic (and its tombstone-file reads)
/// then runs without holding any lock.
const SegMeta = struct { id: u64, rows: u64 };

/// Copy (segment_id, row_count) for every live segment under the table
/// mutex. Caller owns the returned slice.
fn snapshotSegMeta(t: *Table) ![]SegMeta {
    t.mutex.lockUncancelable(t.io);
    defer t.mutex.unlock(t.io);
    const out = try t.allocator.alloc(SegMeta, t.manifest.segments.items.len);
    for (t.manifest.segments.items, 0..) |e, i| out[i] = .{ .id = e.segment_id, .rows = e.row_count };
    return out;
}

/// Implements `Table.compact` (the "merge all" variant). Caller holds
/// `compact_lock`.
pub fn execCompact(t: *Table) !void {
    const metas = try snapshotSegMeta(t);
    defer t.allocator.free(metas);
    if (metas.len <= 1) return;
    const all_ids = try t.allocator.alloc(u64, metas.len);
    defer t.allocator.free(all_ids);
    for (metas, 0..) |m, i| all_ids[i] = m.id;
    try mergeSegments(t, all_ids);
}

/// Background compactor entry point. Picks one compaction group and
/// merges it. First check is tombstone pressure: any segment with
/// `tombs / row_count >= tomb_threshold` is compacted (with adjacent
/// same-tier neighbors if there are any). If no tombstone-pressured
/// segment, fall back to the count-based tier picker. No-op if no
/// group qualifies. Caller holds `compact_lock`.
///
/// Pass `tomb_threshold > 1.0` to disable the tombstone trigger.
pub fn execTieredCompact(t: *Table, tomb_threshold: f32) !void {
    const metas = try snapshotSegMeta(t);
    defer t.allocator.free(metas);

    // 1. Tombstone pressure first.
    if (tomb_threshold <= 1.0) {
        const tomb_pick = try pickTombstonePressuredGroup(t, metas, tomb_threshold);
        if (tomb_pick) |ids| {
            defer t.allocator.free(ids);
            try mergeSegments(t, ids);
            return;
        }
    }
    // 2. Tier-based count trigger.
    const seg_ids = try pickCompactionGroup(t.allocator, metas) orelse return;
    defer t.allocator.free(seg_ids);
    try mergeSegments(t, seg_ids);
}

/// Find the first segment whose tombstone fraction exceeds `threshold`
/// and return it together with adjacent same-tier neighbors (up to
/// `tier_target_count` total). If no neighbors share the tier, returns
/// just the single segment (a "rewrite to drop tombs" compaction).
/// Caller owns the returned slice.
fn pickTombstonePressuredGroup(t: *Table, metas: []const SegMeta, threshold: f32) !?[]u64 {
    for (metas, 0..) |meta, i| {
        if (meta.rows == 0) continue;
        const tombs = try storage.tombstone.read(t.allocator, t.io, t.segments_dir, meta.id);
        defer if (tombs) |arr| t.allocator.free(arr);
        if (tombs == null) continue;
        const ratio = @as(f32, @floatFromInt(tombs.?.len)) / @as(f32, @floatFromInt(meta.rows));
        if (ratio < threshold) continue;

        // Found a heavily-tombstoned segment. Grow to a run of adjacent
        // same-tier neighbors so the merged output stays on tier.
        const tier = segmentTier(meta.rows);
        var lo: usize = i;
        var hi: usize = i + 1;
        while (lo > 0 and segmentTier(metas[lo - 1].rows) == tier and (hi - lo) < tier_target_count) {
            lo -= 1;
        }
        while (hi < metas.len and segmentTier(metas[hi].rows) == tier and (hi - lo) < tier_target_count) {
            hi += 1;
        }
        const out = try t.allocator.alloc(u64, hi - lo);
        for (out, 0..) |*id, j| id.* = metas[lo + j].id;
        return out;
    }
    return null;
}

/// Walk a segment snapshot looking for `tier_target_count` adjacent-by-
/// position segments at the same tier. Returns owned slice of segment IDs
/// to merge, or null if no run qualifies. Prefers the smallest tier (most
/// segments to absorb early).
pub fn pickCompactionGroup(
    allocator: std.mem.Allocator,
    segments: []const SegMeta,
) !?[]u64 {
    if (segments.len < tier_target_count) return null;

    // Scan for the first run of tier_target_count adjacent segments at
    // the same tier. Prefer lower tiers by walking from smallest tier up.
    var best_tier: ?u8 = null;
    var best_start: usize = 0;
    var run_start: usize = 0;
    var run_tier: u8 = segmentTier(segments[0].rows);
    for (segments[1..], 1..) |seg, i| {
        const t = segmentTier(seg.rows);
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
    for (out, 0..) |*id, i| id.* = segments[best_start + i].id;
    return out;
}

/// Merge the given segments into one. `seg_ids` must be a subset of the
/// table's current manifest segments. The caller holds `compact_lock`,
/// which excludes other compactions and DDL (drop/alter/rename) for the
/// whole call — so the inputs' schema, directory, and files are stable.
///
/// Two phases:
///   ASIDE  — read the inputs and write the merged output to a fresh
///            segment file. Holds no `ddl_lock`/`mutex` (only a brief
///            mutex to snapshot, done by the caller), so scans and
///            inserts run unimpeded.
///   COMMIT — under `ddl_lock` exclusive (waits out in-flight scans) +
///            `mutex`: rebuild the keep-list from the *current* manifest
///            (a flush may have appended during the merge), swap it in,
///            and delete the old files. Brief.
pub fn mergeSegments(t: *Table, seg_ids: []const u64) !void {
    if (seg_ids.len == 0) return;

    // --- ASIDE -----------------------------------------------------------
    var work = try engine.Memtable.init(t.allocator, t.schema);
    defer work.deinit();

    // Snapshot every current segment's per-column sketch for the global
    // dict-eligibility gate. Brief lock so a concurrent flush can't realloc the
    // manifest mid-read; the duped bytes outlive the lock. Passing the inputs'
    // own sketches too is harmless — the merged output's sketch already covers
    // their rows, so the HLL union is unchanged.
    var prior_sketches: std.ArrayList([]const u8) = .empty;
    defer {
        for (prior_sketches.items) |p| t.allocator.free(p);
        prior_sketches.deinit(t.allocator);
    }
    {
        t.mutex.lockUncancelable(t.io);
        defer t.mutex.unlock(t.io);
        for (t.manifest.segments.items) |e| {
            if (e.column_sketches.len > 0) {
                try prior_sketches.append(t.allocator, try t.allocator.dupe(u8, e.column_sketches));
            }
        }
    }

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

    const sync = t.syncEnabled();

    // Write the merged output to a brand-new segment file (unless every
    // input row was tombstoned). The entry owns its stats independent of
    // the manifest until spliced in at commit; errdefer frees it if commit
    // setup fails before then.
    var new_entry: ?storage.manifest.ManifestEntry = null;
    errdefer if (new_entry) |e| {
        if (e.column_stats.len > 0) t.allocator.free(e.column_stats);
        if (e.column_sketches.len > 0) t.allocator.free(e.column_sketches);
    };

    if (work.row_count > 0) {
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
            prior_sketches.items,
            sync,
        );
        defer info.deinit(t.allocator);

        new_entry = try t.entryFor(info);
    }

    // --- COMMIT ----------------------------------------------------------
    t.ddl_lock.lockUncancelable(t.io);
    defer t.ddl_lock.unlock(t.io);
    t.mutex.lockUncancelable(t.io);
    defer t.mutex.unlock(t.io);

    // Rebuild the keep-list from the CURRENT manifest. A flush may have
    // appended segments during the aside merge — those aren't in `seg_ids`,
    // so they're preserved. Free each dropped input's owned stats; the kept
    // entries carry theirs forward via the shallow struct copy.
    var keep: std.ArrayList(storage.ManifestEntry) = .empty;
    defer keep.deinit(t.allocator);
    try keep.ensureTotalCapacity(t.allocator, t.manifest.segments.items.len + 1);

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
            if (entry.column_stats.len > 0) t.allocator.free(entry.column_stats);
            if (entry.column_sketches.len > 0) t.allocator.free(entry.column_sketches);
        }
    }

    if (new_entry) |entry| {
        // Splice the new segment in where the first dropped input was, so
        // older segments stay older.
        const insert_at = first_dropped_idx orelse keep.items.len;
        try keep.insert(t.allocator, @min(insert_at, keep.items.len), entry);
        new_entry = null; // ownership now lives in the manifest
    }

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
