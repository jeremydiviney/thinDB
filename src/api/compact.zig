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
const types = @import("../types.zig");

const api = @import("api.zig");
const Table = api.Table;
const comparison = @import("comparison.zig");

// Tiering constants. See DESIGN.md §7.2.
//
// Segments are bucketed by row count into tiers. Each tier is `tier_ratio`×
// larger than the previous. When `tier_target_count` adjacent (by segment_id)
// segments accumulate at the same tier, they get merged into one and the
// output joins the next tier up.

/// Tier-1 floor: a segment with fewer rows than this is "tier 0" (staging).
/// 2^19 ≈ 0.5M, so a freshly-flushed load segment (~125K rows) starts in tier 0
/// and gets swept up into a tier-1 segment by `pickTier0Group`.
pub const tier_base_rows: u64 = 1 << 19;
/// Each tier holds segments up to `tier_ratio`× the previous tier's cap.
pub const tier_ratio: u64 = 4;
/// Number of adjacent same-tier segments that triggers a merge (tier 1+).
pub const tier_target_count: usize = 4;
/// Cap on rows accumulated by a single tier-0 sweep merge. Bounds the merge's
/// memory and lands the result in tier 1 (just under the 2^21 tier-2 floor),
/// so a pile of small segments collapses in one pass instead of climbing
/// 4-at-a-time.
pub const tier0_sweep_cap: u64 = 2_000_000;

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
/// Pass `tomb_threshold > 1.0` to disable the tombstone trigger. Returns true
/// if a merge was performed (so the background loop can keep draining without
/// sleeping), false if no group qualified.
pub fn execTieredCompact(t: *Table, tomb_threshold: f32) !bool {
    const metas = try snapshotSegMeta(t);
    defer t.allocator.free(metas);

    // 1. Tombstone pressure first.
    if (tomb_threshold <= 1.0) {
        const tomb_pick = try pickTombstonePressuredGroup(t, metas, tomb_threshold);
        if (tomb_pick) |ids| {
            defer t.allocator.free(ids);
            try mergeSegments(t, ids);
            return true;
        }
    }
    // 2. Tier-0 sweep: collapse a run of small staging segments into one
    // ~2M (tier-1) segment in a single merge, ahead of the 4-wide picker.
    if (try pickTier0Group(t.allocator, metas)) |ids| {
        defer t.allocator.free(ids);
        try mergeSegments(t, ids);
        return true;
    }
    // 3. Tier-based count trigger (tier 1+).
    const seg_ids = try pickCompactionGroup(t.allocator, metas) orelse return false;
    defer t.allocator.free(seg_ids);
    try mergeSegments(t, seg_ids);
    return true;
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

/// Tier-0 sweep picker. Finds the first run of adjacent tier-0 segments
/// (row_count < `tier_base_rows`) and returns a prefix of that run whose rows
/// sum to at most `tier0_sweep_cap` — so a wad of freshly-flushed small
/// segments collapses into one ~2M segment in a single merge (landing in
/// tier 1) instead of climbing four-at-a-time. Returns null unless ≥2 tier-0
/// segments can be swept together: a lone small segment is left to accumulate
/// rather than rewritten on every sweep. Caller owns the returned slice.
pub fn pickTier0Group(allocator: std.mem.Allocator, segments: []const SegMeta) !?[]u64 {
    var i: usize = 0;
    while (i < segments.len) {
        if (segmentTier(segments[i].rows) != 0) {
            i += 1;
            continue;
        }
        // Start of a tier-0 run; gather a ≤cap prefix (always take the first).
        var sum: u64 = 0;
        var j: usize = i;
        while (j < segments.len and segmentTier(segments[j].rows) == 0) : (j += 1) {
            if (j > i and sum + segments[j].rows > tier0_sweep_cap) break;
            sum += segments[j].rows;
        }
        if (j - i >= 2) {
            const out = try allocator.alloc(u64, j - i);
            for (out, 0..) |*id, k| id.* = segments[i + k].id;
            return out;
        }
        i = j; // too-short run (≤1 tier-0 seg here) — skip and keep scanning
    }
    return null;
}

/// One input segment's read cursor for the streaming k-way merge. Owns its
/// `ReadSegment` + tombstone array, decodes one row-group at a time, and walks
/// kept (non-tombstoned) rows in order. At most one decoded row-group is held
/// per cursor, so the merge's memory stays bounded regardless of input size.
const MergeCursor = struct {
    allocator: std.mem.Allocator,
    schema: types.TableSchema,
    seg: storage.ReadSegment,
    tombs: ?[]u32,

    rg_idx: usize,
    /// Decoded columns of the current row-group (one per schema column), or
    /// empty before the first `loadRowGroup` / after exhaustion.
    decoded: []storage.OwnedColumn,
    decoded_live: bool,
    pos: usize, // local index within the current row-group
    rg_rows: usize, // row count of the current row-group
    rg_base: u32, // segment-absolute offset of row 0 of the current row-group
    exhausted: bool,

    fn open(t: *Table, id: u64) !MergeCursor {
        var name_buf: [32]u8 = undefined;
        const file_name = try Table.segmentFileName(&name_buf, id);
        var seg = try storage.readSegment(t.allocator, t.io, t.segments_dir, file_name, t.schema);
        errdefer seg.deinit();

        const tombs = try storage.tombstone.read(t.allocator, t.io, t.segments_dir, id);
        errdefer if (tombs) |x| t.allocator.free(x);

        const decoded = try t.allocator.alloc(storage.OwnedColumn, t.schema.columns.len);
        errdefer t.allocator.free(decoded);

        var self: MergeCursor = .{
            .allocator = t.allocator,
            .schema = t.schema,
            .seg = seg,
            .tombs = tombs,
            .rg_idx = 0,
            .decoded = decoded,
            .decoded_live = false,
            .pos = 0,
            .rg_rows = 0,
            .rg_base = 0,
            .exhausted = false,
        };
        try self.advanceToValid();
        return self;
    }

    fn deinit(self: *MergeCursor) void {
        self.freeDecoded();
        self.allocator.free(self.decoded);
        if (self.tombs) |x| self.allocator.free(x);
        self.seg.deinit();
        self.* = undefined;
    }

    fn freeDecoded(self: *MergeCursor) void {
        if (self.decoded_live) {
            for (self.decoded) |*c| c.deinit(self.allocator);
            self.decoded_live = false;
        }
    }

    /// Decode row-group `self.rg_idx` into `self.decoded`. Caller has ensured
    /// the index is in range and previous decode (if any) is freed.
    fn loadRowGroup(self: *MergeCursor) !void {
        const rg = self.seg.info.row_groups[self.rg_idx];
        var inited: usize = 0;
        errdefer for (self.decoded[0..inited]) |*c| c.deinit(self.allocator);
        for (self.schema.columns, 0..) |_, ci| {
            self.decoded[ci] = try self.seg.decodeColumn(self.allocator, self.schema, self.rg_idx, ci);
            inited += 1;
        }
        self.decoded_live = true;
        self.rg_rows = rg.row_count;
        self.pos = 0;
    }

    /// True iff the segment-absolute row at `self.rg_base + local` is tombstoned.
    fn isTombstoned(self: *const MergeCursor, local: usize) bool {
        const arr = self.tombs orelse return false;
        const off: u32 = self.rg_base + @as(u32, @intCast(local));
        const i = std.sort.lowerBound(u32, arr, off, comparison.cmpU32);
        return i < arr.len and arr[i] == off;
    }

    /// Position the cursor on the next kept row, loading row-groups as needed.
    /// Sets `self.exhausted` when no further kept row exists.
    fn advanceToValid(self: *MergeCursor) !void {
        while (true) {
            if (self.decoded_live and self.pos < self.rg_rows) {
                if (!self.isTombstoned(self.pos)) return;
                self.pos += 1;
                continue;
            }
            // Current row-group consumed (or none loaded): move to the next.
            if (self.decoded_live) {
                self.rg_base += @intCast(self.rg_rows);
                self.freeDecoded();
                self.rg_idx += 1;
            }
            if (self.rg_idx >= self.seg.info.row_groups.len) {
                self.exhausted = true;
                return;
            }
            try self.loadRowGroup();
        }
    }

    /// Step past the current row, then land on the next kept one.
    fn next(self: *MergeCursor) !void {
        self.pos += 1;
        try self.advanceToValid();
    }
};

/// Streaming k-way merge of the (already order-key-sorted) input segments into
/// one new segment, dropping tombstoned rows. Returns the written segment's
/// `SegmentInfo` (caller owns), or null when every input row was tombstoned
/// (no segment is written). Holds at most one decoded row-group per input plus
/// one output row-group plus the in-flight segment buffer — never all rows.
fn streamMerge(
    t: *Table,
    seg_ids: []const u64,
    prior_sketches: []const []const u8,
    new_seg_id: u64,
    file_name: []const u8,
    sync: bool,
) !?storage.format.SegmentInfo {
    const ncols = t.schema.columns.len;

    // Output dict eligibility + sketch from the union of the input segments'
    // per-column HLL sketches. The union over-approximates the merged output's
    // NDV (it can only lose rows to tombstones), so it's a correct upper bound
    // for the dict gate and an acceptable stored sketch. `prior_sketches`
    // already includes the inputs' own sketches, so it is the global view.
    const dict_eligible = try storage.dictEligibleFromSketches(t.allocator, ncols, prior_sketches);
    defer t.allocator.free(dict_eligible);

    // Ownership of `out_sketches` transfers to the writer at `begin`; until then
    // it is freed on any early error via this flag-guarded errdefer.
    const out_sketches = try unionInputSketches(t, seg_ids, ncols);
    var sketches_owned_here = true;
    errdefer if (sketches_owned_here) t.allocator.free(out_sketches);

    // Open one cursor per input. Each positions itself on its first kept row.
    var cursors = try t.allocator.alloc(MergeCursor, seg_ids.len);
    defer t.allocator.free(cursors);
    var opened: usize = 0;
    errdefer for (cursors[0..opened]) |*c| c.deinit();
    for (seg_ids) |id| {
        cursors[opened] = try MergeCursor.open(t, id);
        opened += 1;
    }

    // Any kept rows at all? If every input is exhausted, write no segment.
    var any = false;
    for (cursors) |c| {
        if (!c.exhausted) {
            any = true;
            break;
        }
    }
    if (!any) {
        for (cursors[0..opened]) |*c| c.deinit();
        opened = 0;
        return null; // sketches_owned_here errdefer frees out_sketches
    }

    var writer = try storage.MergedSegmentWriter.begin(
        t.allocator,
        t.schema,
        new_seg_id,
        t.schema_fingerprint,
        t.row_group_size,
        dict_eligible,
        out_sketches, // ownership transfers to the writer here
    );
    sketches_owned_here = false; // writer.deinit / finish now owns out_sketches
    errdefer writer.deinit();

    // Output row-group accumulator: one ColumnStore per column, reused across
    // row-groups (cleared, capacity retained). Flushed to `writer` each time it
    // fills to `row_group_size`.
    var out_cols = try t.allocator.alloc(engine.ColumnStore, ncols);
    var oc_inited: usize = 0;
    defer {
        for (out_cols[0..oc_inited]) |*c| c.deinit(t.allocator);
        t.allocator.free(out_cols);
    }
    for (t.schema.columns, 0..) |sch, ci| {
        out_cols[ci] = try engine.ColumnStore.initCapacity(t.allocator, sch.type, sch.nullable, t.row_group_size, 0);
        oc_inited += 1;
    }

    const out_views = try t.allocator.alloc(storage.ColumnView, ncols);
    defer t.allocator.free(out_views);

    var acc_rows: usize = 0;
    while (true) {
        // Pick the smallest current row across live cursors by the order key,
        // tie-broken by input index for a deterministic total order. k is small,
        // so a linear scan beats a heap's bookkeeping.
        var best: ?usize = null;
        for (cursors, 0..) |*c, ci| {
            if (c.exhausted) continue;
            if (best == null) {
                best = ci;
                continue;
            }
            const b = &cursors[best.?];
            if (lessThanCursor(t.order_key_indices, c, b)) best = ci;
        }
        const pick = best orelse break;

        // Emit the picked row into the output accumulator.
        const src = &cursors[pick];
        for (out_cols, 0..) |*dst, ci| {
            try engine.transform.appendOneRow(t.allocator, src.decoded[ci].view(), src.pos, dst);
        }
        acc_rows += 1;
        try src.next();

        if (acc_rows == t.row_group_size) {
            for (out_cols, 0..) |c, ci| out_views[ci] = c.view();
            try writer.writeRowGroup(out_views);
            for (out_cols) |*c| c.clear();
            acc_rows = 0;
        }
    }

    if (acc_rows > 0) {
        for (out_cols, 0..) |c, ci| out_views[ci] = c.view();
        try writer.writeRowGroup(out_views);
    }

    for (cursors[0..opened]) |*c| c.deinit();
    opened = 0;

    return try writer.finish(t.io, t.segments_dir, file_name, sync);
}

/// `a`'s current row sorts strictly before `b`'s under the composite order key.
/// Mirrors `Memtable.buildSortedSnapshot`'s lexicographic comparator exactly.
fn lessThanCursor(order_key_indices: []const usize, a: *const MergeCursor, b: *const MergeCursor) bool {
    for (order_key_indices) |ci| {
        const ord = engine.transform.compareViewRows(a.decoded[ci].view(), a.pos, b.decoded[ci].view(), b.pos);
        if (ord == .lt) return true;
        if (ord == .gt) return false;
    }
    return false;
}

/// Per-column HLL union across the input segments' sketches, in the
/// `SegmentInfo.column_sketches` layout. Caller owns the returned slice.
fn unionInputSketches(t: *Table, seg_ids: []const u64, ncols: usize) ![]u8 {
    const hll = @import("../util/hll.zig");
    const out = try t.allocator.alloc(u8, ncols * hll.m);
    errdefer t.allocator.free(out);
    @memset(out, 0);

    t.mutex.lockUncancelable(t.io);
    defer t.mutex.unlock(t.io);
    for (t.manifest.segments.items) |e| {
        var is_input = false;
        for (seg_ids) |id| if (id == e.segment_id) {
            is_input = true;
            break;
        };
        if (!is_input or e.column_sketches.len == 0) continue;
        for (0..ncols) |ci| {
            if (e.column_sketches.len < (ci + 1) * hll.m) break;
            var acc = hll.Hll.fromBytes(out[ci * hll.m .. (ci + 1) * hll.m]);
            const ph = hll.Hll.fromBytes(e.column_sketches[ci * hll.m .. (ci + 1) * hll.m]);
            acc.merge(&ph);
            @memcpy(out[ci * hll.m .. (ci + 1) * hll.m], acc.bytes());
        }
    }
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

    {
        const new_seg_id = t.allocSegmentId();
        var name_buf: [32]u8 = undefined;
        const file_name = try Table.segmentFileName(&name_buf, new_seg_id);

        var maybe_info = try streamMerge(t, seg_ids, prior_sketches.items, new_seg_id, file_name, sync);
        if (maybe_info) |*info| {
            defer info.deinit(t.allocator);
            new_entry = try t.entryFor(info.*);
        }
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
    // tier-1 floor is 2^19 (524,288); each tier is 4x the previous.
    try std.testing.expectEqual(@as(u8, 0), segmentTier(0));
    try std.testing.expectEqual(@as(u8, 0), segmentTier(1));
    try std.testing.expectEqual(@as(u8, 0), segmentTier(524_287));
    try std.testing.expectEqual(@as(u8, 1), segmentTier(524_288));
    try std.testing.expectEqual(@as(u8, 1), segmentTier(524_288 * 3));
    try std.testing.expectEqual(@as(u8, 2), segmentTier(524_288 * 4));
    try std.testing.expectEqual(@as(u8, 3), segmentTier(524_288 * 16));
}

test "pickTier0Group sweeps a run of small segments up to the cap" {
    const a = std.testing.allocator;
    // Six ~125K tier-0 segments + one tier-1 segment. A sweep should grab the
    // first run of tier-0 segments (all six here, summing 750K < 2M cap).
    const metas = [_]SegMeta{
        .{ .id = 1, .rows = 125_000 },
        .{ .id = 2, .rows = 125_000 },
        .{ .id = 3, .rows = 125_000 },
        .{ .id = 4, .rows = 125_000 },
        .{ .id = 5, .rows = 125_000 },
        .{ .id = 6, .rows = 125_000 },
        .{ .id = 7, .rows = 600_000 }, // tier 1, not swept
    };
    const got = (try pickTier0Group(a, &metas)).?;
    defer a.free(got);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3, 4, 5, 6 }, got);

    // A lone tier-0 segment (next is tier-1) is left alone.
    const metas2 = [_]SegMeta{ .{ .id = 1, .rows = 100_000 }, .{ .id = 2, .rows = 600_000 } };
    try std.testing.expectEqual(@as(?[]u64, null), try pickTier0Group(a, &metas2));

    // A long run is capped at ~2M (16 x 125K = 2.0M; the 17th would overflow).
    var big: [20]SegMeta = undefined;
    for (&big, 0..) |*m, k| m.* = .{ .id = @intCast(k + 1), .rows = 125_000 };
    const got3 = (try pickTier0Group(a, &big)).?;
    defer a.free(got3);
    try std.testing.expectEqual(@as(usize, 16), got3.len);
}

test "pickCompactionGroup picks 4 adjacent same-tier segments (tier 1+)" {
    const a = std.testing.allocator;
    // Five tier-1 segments (~600K rows each, in [2^19, 2^21)): the picker takes
    // the first full run of `tier_target_count` (4).
    const metas = [_]SegMeta{
        .{ .id = 1, .rows = 600_000 },
        .{ .id = 2, .rows = 600_000 },
        .{ .id = 3, .rows = 600_000 },
        .{ .id = 4, .rows = 600_000 },
        .{ .id = 5, .rows = 600_000 },
    };
    const got = (try pickCompactionGroup(a, &metas)).?;
    defer a.free(got);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3, 4 }, got);

    // Only three same-tier segments → no group yet.
    const metas2 = [_]SegMeta{
        .{ .id = 1, .rows = 600_000 },
        .{ .id = 2, .rows = 600_000 },
        .{ .id = 3, .rows = 600_000 },
    };
    try std.testing.expectEqual(@as(?[]u64, null), try pickCompactionGroup(a, &metas2));
}
