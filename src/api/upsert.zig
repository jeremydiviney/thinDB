//! Unique-key upsert resolution. Implements StarRocks-style "last writer
//! wins" semantics on tables created with `unique = true`. Called from
//! `Table.insert` after `insertRows` lands the new rows. INSERT IGNORE and
//! INSERT ... ON DUPLICATE KEY UPDATE resolve a present key their own way
//! (`insertOnDuplicateLocked`) before the rows reach it.

const std = @import("std");
const Allocator = std.mem.Allocator;

const storage = @import("../storage/storage.zig");
const engine = @import("../engine/engine.zig");

const api = @import("api.zig");
const exec = @import("../exec/exec.zig");
const types = @import("../types.zig");
const Table = api.Table;
const comparison = @import("comparison.zig");
const bloom = @import("../util/bloom.zig");
const delete_mod = @import("delete.zig");
const update_mod = @import("update.zig");

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

/// Drop the persistent index (memtable swapped or emptied, or rows it
/// indexed taken back out). Keeps map + arena capacity for reuse; the next
/// resolution rebuilds from row 0.
pub fn resetIndex(t: *Table) void {
    t.upsert_idx.clearRetainingCapacity();
    if (t.upsert_idx_arena) |*a| _ = a.reset(.retain_capacity);
    t.upsert_idx_gen = null;
    t.upsert_idx_rows = 0;
}

/// What upsert resolution changes for the memtable rows it hasn't seen yet.
/// Working it out changes nothing but the key index, so a writer can still
/// take the rows back out (and `resetIndex`) until it logs them.
pub const Resolution = struct {
    /// The memtable without the older rows the new ones replace.
    deduped: ?*engine.Memtable = null,
    /// Tombstone files with the segment rows the new ones replace added.
    tombstone_files: std.ArrayList(TombstoneFile) = .empty,

    pub fn deinit(self: *Resolution, allocator: Allocator) void {
        if (self.deduped) |mt| mt.release();
        for (self.tombstone_files.items) |f| allocator.free(f.bytes);
        self.tombstone_files.deinit(allocator);
        self.* = undefined;
    }
};

const TombstoneFile = struct { segment_id: u64, bytes: []u8 };

/// Last writer wins: each of `mt`'s unseen rows replaces the older row with
/// its order key, in the memtable or in a flushed segment. `gen` is the
/// memtable generation `mt` has, or gets once installed.
pub fn prepareResolution(t: *Table, mt: *const engine.Memtable, gen: u64) !Resolution {
    std.debug.assert(t.order_key_indices.len > 0);
    var resolution: Resolution = .{};
    errdefer resolution.deinit(t.allocator);
    errdefer resetIndex(t);

    const n: usize = @intCast(mt.row_count);
    if (n == 0) {
        resetIndex(t);
        return resolution;
    }

    // Bind the persistent key index to the memtable generation. ANY swap
    // (flush / delete / update / dedup-clone) bumps the generation via
    // installMemtableLocked → rebuild from row 0; otherwise process only
    // the rows added since last time. NOT a pointer compare — a freed
    // memtable's address can be reused by a later clone (ABA), silently
    // revalidating a stale index whose row mappings then tombstone
    // unrelated rows.
    const same_gen = if (t.upsert_idx_gen) |g| g == gen else false;
    if (!same_gen or t.upsert_idx_rows > n) {
        resetIndex(t);
        t.upsert_idx_gen = gen;
    }
    if (t.upsert_idx_arena == null) t.upsert_idx_arena = std.heap.ArenaAllocator.init(t.allocator);
    const idx_aa = t.upsert_idx_arena.?.allocator();
    const start: usize = t.upsert_idx_rows;

    // Scratch for this batch's temporaries (probe key set).
    var arena = std.heap.ArenaAllocator.init(t.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    // ---- 1. Incremental intra-memtable dedup: only the NEW rows. Look each
    // new row's key up in the persistent index; a hit drops the older
    // memtable row (last writer wins) and a miss is a key we must also probe
    // against segments (below).
    var dropped: std.ArrayList(u32) = .empty;
    defer dropped.deinit(t.allocator);
    var new_keys: std.ArrayList([]const u8) = .empty; // arena-owned; survive an index reset
    // First order-key column value per NEW key, for row-group zonemap
    // pruning during the segment probe (#138).
    var new_first_vals: std.ArrayList(types.Value) = .empty;
    var prune_ok = true;

    const first_key_view = mt.columns[t.order_key_indices[0]].view();
    for (start..n) |i| {
        const key_bytes = try compoundKeyFromColumnStores(idx_aa, mt.columns, t.order_key_indices, @intCast(i));
        const gop = try t.upsert_idx.getOrPut(t.allocator, key_bytes);
        if (gop.found_existing) {
            try dropped.append(t.allocator, gop.value_ptr.*);
        } else {
            try new_keys.append(aa, try aa.dupe(u8, key_bytes));
            if (try viewValueAt(aa, first_key_view, @intCast(i))) |v| {
                try new_first_vals.append(aa, v);
            } else {
                prune_ok = false;
            }
        }
        gop.value_ptr.* = @intCast(i);
    }
    t.upsert_idx_rows = @intCast(n);

    // Snapshot-isolated retire-replace for the dropped older rows, on
    // commit: scans that pinned the pre-resolution memtable keep seeing
    // them; new scans see the deduped state.
    if (dropped.items.len > 0) {
        const keep = try t.allocator.alloc(bool, n);
        defer t.allocator.free(keep);
        @memset(keep, true);
        for (dropped.items) |d| keep[d] = false;
        resolution.deduped = try mt.cloneWithRetainedRows(t.allocator, keep);
    }

    // ---- 2. Probe segments only for keys NEW to the memtable this batch. A
    // key needs a segment tombstone check exactly once — when it first enters
    // the memtable; a re-insert already tombstoned its segment match.
    if (new_keys.items.len == 0 or t.manifest.segments.items.len == 0) return resolution;

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
        // Pinned cache handle, not a direct open: reuses the parsed footer
        // across batches and can't race a concurrent compaction's delete of
        // a just-retired segment file (#137) — a pinned entry keeps the
        // handle alive until release even if the segment is retired mid-probe.
        const handle = try t.acquireSegment(entry.segment_id);
        defer t.releaseSegment(handle);
        const seg = &handle.seg;

        var deleted: std.ArrayList(u32) = .empty;
        defer deleted.deinit(t.allocator);

        var row_offset: u32 = 0;
        for (seg.info.row_groups, 0..) |rg, rg_idx| {
            // Zonemap prune on the first key column (#138): segments are
            // sorted by the order key, so a batch's keys land in a handful
            // of row groups — skip decoding the rest entirely.
            if (prune_ok) {
                var admit = false;
                for (new_first_vals.items) |v| {
                    if (exec.predicate.statsOverlapPredicate(rg.stats[t.order_key_indices[0]], .eq, v)) {
                        admit = true;
                        break;
                    }
                }
                if (!admit) {
                    row_offset += rg.row_count;
                    continue;
                }
            }

            const decoded_keys = try aa.alloc(storage.OwnedColumn, t.order_key_indices.len);
            var decoded_count: usize = 0;
            defer for (decoded_keys[0..decoded_count]) |*c| c.deinit(t.allocator);
            for (t.order_key_indices, decoded_keys) |col_idx, *c| {
                c.* = try seg.decodeColumn(t.allocator, t.schema, rg_idx, col_idx);
                decoded_count += 1;
            }

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
            try resolution.tombstone_files.ensureUnusedCapacity(t.allocator, 1);
            const bytes = try storage.tombstone.encodeMerged(t.allocator, t.io, t.segments_dir, entry.segment_id, deleted.items);
            resolution.tombstone_files.appendAssumeCapacity(.{ .segment_id = entry.segment_id, .bytes = bytes });
        }
    }
    return resolution;
}

/// Apply `resolution` once the table holds the rows it resolved. Only the
/// tombstone file writes can fail.
pub fn commitResolution(t: *Table, resolution: *Resolution) !void {
    if (resolution.deduped) |mt| {
        resolution.deduped = null;
        t.installMemtableLocked(mt);
        // The swap moved the rows the index points at.
        resetIndex(t);
    }
    for (resolution.tombstone_files.items) |f| try t.writeTombstoneFile(f.segment_id, f.bytes);
}

/// Resolve the memtable rows no resolution has seen, in one step.
pub fn applyUpsertResolution(t: *Table) !void {
    var resolution = try prepareResolution(t, t.memtable, t.memtable_gen);
    defer resolution.deinit(t.allocator);
    try commitResolution(t, &resolution);
}

/// Full-key Bloom candidates for a keyed DELETE/UPDATE (#143). When the
/// predicate's top-level AND conjuncts (or a bare leaf) pin every order-key
/// column with equality — allowing at most one column an IN list of ≤256
/// values (an IN set, or the OR of equalities a literal list parses to) —
/// return the compound-key hashes, encoded exactly as the Bloom was built. Returns null when the key set can't be derived (caller scans
/// normally). Allocated in `aa`.
pub fn keyHashesFromPredicateExpr(
    t: *Table,
    aa: Allocator,
    expr: exec.predicate.PredicateExpr,
) !?[]u64 {
    const oki = t.order_key_indices;
    if (!t.schema.unique or oki.len == 0) return null;
    const max_keys = 256;

    // The parser builds left-deep binary AND trees (`a AND b AND c` =
    // and(and(a,b),c)), so conjuncts must be collected recursively — a
    // one-level view leaves all but the last key column "unbound" and
    // silently disables the gate for any 3+-conjunct keyed statement.
    var conj_list: std.ArrayList(exec.predicate.PredicateExpr) = .empty;
    if (!try appendConjuncts(aa, &conj_list, expr)) return null;
    const conjuncts: []const exec.predicate.PredicateExpr = conj_list.items;

    const eq_vals = try aa.alloc(?types.Value, oki.len);
    @memset(eq_vals, null);
    var in_vals: ?[]const types.Value = null;
    var in_pos: usize = 0;
    for (oki, 0..) |col_idx, k| {
        const col_name = t.schema.columns[col_idx].name;
        for (conjuncts) |c| switch (c) {
            .leaf => |p| {
                if (p.op == .eq and types.columnNameEql(p.col, col_name)) {
                    eq_vals[k] = p.val;
                }
            },
            .in_set, .@"or" => {
                const listed = (try inListValues(aa, c, col_name)) orelse continue;
                if (eq_vals[k] == null) {
                    if (in_vals != null and in_pos != k) return null; // two IN-bound key columns
                    if (listed.len == 0 or listed.len > max_keys) return null;
                    in_vals = listed;
                    in_pos = k;
                }
            },
            else => {},
        };
        if (eq_vals[k] == null and (in_vals == null or in_pos != k)) return null; // unbound
    }

    var hashes: std.ArrayList(u64) = .empty;
    var key_buf: std.ArrayList(u8) = .empty;
    const n_combos: usize = if (in_vals) |vs| vs.len else 1;
    var ci: usize = 0;
    while (ci < n_combos) : (ci += 1) {
        key_buf.clearRetainingCapacity();
        for (oki, 0..) |col_idx, k| {
            const v = eq_vals[k] orelse in_vals.?[ci];
            if (!try comparison.appendPredicateValueBytes(aa, &key_buf, t.schema.columns[col_idx].type, v)) return null;
        }
        try hashes.append(aa, bloom.keyHash(key_buf.items));
    }
    return try hashes.toOwnedSlice(aa);
}

/// The values of an IN list on `col_name`: a non-negated IN set, or an OR
/// of equalities on that one column. Null for any other conjunct.
fn inListValues(aa: Allocator, conjunct: exec.predicate.PredicateExpr, col_name: []const u8) !?[]const types.Value {
    switch (conjunct) {
        .in_set => |s| return if (!s.negate and types.columnNameEql(s.col, col_name)) s.values else null,
        .@"or" => |arms| {
            const col = exec.predicate.eqDisjunctionColumn(arms) orelse return null;
            if (!types.columnNameEql(col, col_name)) return null;
            const values = try aa.alloc(types.Value, arms.len);
            for (arms, values) |arm, *v| v.* = arm.leaf.val;
            return values;
        },
        else => return null,
    }
}

/// Flatten a (possibly nested) AND tree into its leaf and IN-list conjuncts
/// (an IN set, or an OR of equalities on one column). Returns false when the
/// expression contains any other OR, a NOT, ... — the caller must then skip
/// bloom gating entirely.
pub fn appendConjuncts(
    aa: Allocator,
    list: *std.ArrayList(exec.predicate.PredicateExpr),
    expr: exec.predicate.PredicateExpr,
) !bool {
    switch (expr) {
        .@"and" => |children| {
            for (children) |c| {
                if (!try appendConjuncts(aa, list, c)) return false;
            }
            return true;
        },
        .leaf, .in_set => {
            try list.append(aa, expr);
            return true;
        },
        .@"or" => |arms| {
            if (exec.predicate.eqDisjunctionColumn(arms) == null) return false;
            try list.append(aa, expr);
            return true;
        },
        else => return false,
    }
}

/// True when `key_bloom` (may be empty = no filter) admits at least one of
/// `hashes` — i.e. the segment cannot be skipped.
pub fn bloomAdmitsAny(key_bloom: []const u8, hashes: []const u64) bool {
    if (key_bloom.len == 0) return true;
    for (hashes) |h| {
        if (bloom.Bloom.mayContainSerialized(key_bloom, h)) return true;
    }
    return false;
}

/// Read row `row` of a column view as a `types.Value` for zonemap checks.
/// String bytes are duped into `aa` so the value outlives a memtable swap.
/// Returns null for a NULL cell — the caller must then skip pruning.
pub fn viewValueAt(aa: Allocator, view: storage.ColumnView, row: u32) !?types.Value {
    if (!view.isValid(row)) return null;
    return switch (view.data) {
        .int => |s| .{ .int = s[row] },
        .bigint => |s| .{ .bigint = s[row] },
        .boolean => |s| .{ .boolean = s[row] != 0 },
        .varchar, .string, .char, .json => |sv| .{ .text = try aa.dupe(u8, sv.rowBytes(row)) },
        .float => |s| .{ .float = s[row] },
        .double => |s| .{ .double = s[row] },
        .date => |s| .{ .date = s[row] },
        .datetime => |s| .{ .datetime = s[row] },
        .tinyint => |s| .{ .tinyint = s[row] },
        .smallint => |s| .{ .smallint = s[row] },
        .largeint => |s| .{ .largeint = s[row] },
        .decimal64 => |s| .{ .decimal64 = s[row] },
        .decimal128 => |s| .{ .decimal128 = s[row] },
        .uuid => |s| .{ .uuid = s[row] },
    };
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

/// What an INSERT does with a row whose key the table already holds, where
/// plain INSERT replaces it (last writer wins).
pub const OnDuplicate = union(enum) {
    /// INSERT IGNORE: keep the stored row and drop the new one. A key repeated
    /// within the statement keeps its first row.
    ignore,
    /// ON DUPLICATE KEY UPDATE: rewrite the stored row through the
    /// assignments, which name its columns as they are and the new row's under
    /// `incoming_prefix`. A key repeated within the statement applies them in
    /// turn, each row against the result of the one before.
    update: []const update_mod.Assignment,
};

/// Column-name prefix under which ON DUPLICATE KEY UPDATE assignments see the
/// row being inserted.
pub const incoming_prefix = "__incoming__.";

pub const DuplicateCounts = struct {
    inserted: usize = 0,
    updated: usize = 0,
};

/// INSERT under a duplicate-key rule on a unique table. Caller holds the
/// table mutex; `wal_target` receives the WAL offset to await.
pub fn insertOnDuplicateLocked(
    t: *Table,
    batch_schema: []const types.Column,
    views: []const storage.ColumnView,
    row_count: usize,
    action: OnDuplicate,
    wal_target: *?u64,
) !DuplicateCounts {
    std.debug.assert(t.schema.unique and t.order_key_indices.len > 0);
    try t.ensureUsable();
    var arena = std.heap.ArenaAllocator.init(t.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    // Staged in the table's own types, the rows' keys encode exactly as the
    // stored rows' keys do.
    var incoming = try engine.Memtable.init(t.allocator, t.schema);
    defer incoming.deinit();
    try incoming.insertColumnarBatch(batch_schema, views, row_count);

    var slot_of: std.StringHashMapUnmanaged(u32) = .empty;
    var slot_first_row: std.ArrayList(u32) = .empty;
    const row_slot = try aa.alloc(u32, row_count);
    for (row_slot, 0..) |*slot, i| {
        const key = try compoundKeyFromColumnStores(aa, incoming.columns, t.order_key_indices, @intCast(i));
        const gop = try slot_of.getOrPut(aa, key);
        if (!gop.found_existing) {
            gop.value_ptr.* = @intCast(slot_first_row.items.len);
            try slot_first_row.append(aa, @intCast(i));
        }
        slot.* = gop.value_ptr.*;
    }

    var current = try engine.Memtable.init(t.allocator, t.schema);
    defer current.deinit();
    const slot_current = try aa.alloc(?u32, slot_first_row.items.len);
    @memset(slot_current, null);
    try collectStoredRows(t, aa, &slot_of, &incoming, slot_first_row.items, &current, slot_current);

    return switch (action) {
        .ignore => try insertAbsentRows(t, aa, &incoming, row_slot, slot_current, wal_target),
        .update => |assignments| try mergeRows(t, aa, &incoming, row_slot, &current, slot_current, assignments, wal_target),
    };
}

/// Copy into `current` the stored row of every key in `slot_of` the table
/// holds, recording where it landed in `slot_current`. Upsert resolution
/// leaves one live row per key, in the memtable or in a segment.
fn collectStoredRows(
    t: *Table,
    aa: Allocator,
    slot_of: *const std.StringHashMapUnmanaged(u32),
    incoming: *const engine.Memtable,
    slot_first_row: []const u32,
    current: *engine.Memtable,
    slot_current: []?u32,
) !void {
    const oki = t.order_key_indices;
    var keybuf: std.ArrayList(u8) = .empty;
    var found: usize = 0;

    var hits: std.ArrayList(u32) = .empty;
    var hit_slots: std.ArrayList(u32) = .empty;
    const mt_rows: usize = @intCast(t.memtable.row_count);
    for (0..mt_rows) |i| {
        keybuf.clearRetainingCapacity();
        for (oki) |ci| try comparison.appendColumnValueBytes(aa, &keybuf, t.memtable.columns[ci].view(), @intCast(i));
        const slot = slot_of.get(keybuf.items) orelse continue;
        if (slot_current[slot] != null) continue;
        slot_current[slot] = 0;
        try hits.append(aa, @intCast(i));
        try hit_slots.append(aa, slot);
    }
    try appendHits(t.allocator, t.memtable.columns, hits.items, hit_slots.items, current, slot_current);
    found += hits.items.len;
    if (found == slot_current.len or t.manifest.segments.items.len == 0) return;

    var hashes: std.ArrayList(u64) = .empty;
    var first_vals: std.ArrayList(types.Value) = .empty;
    var prune_ok = true;
    var it = slot_of.iterator();
    while (it.next()) |e| {
        if (slot_current[e.value_ptr.*] != null) continue;
        try hashes.append(aa, bloom.keyHash(e.key_ptr.*));
        const first_row = slot_first_row[e.value_ptr.*];
        if (try viewValueAt(aa, incoming.columns[oki[0]].view(), first_row)) |v| {
            try first_vals.append(aa, v);
        } else {
            prune_ok = false;
        }
    }

    for (t.manifest.segments.items) |entry| {
        if (found == slot_current.len) break;
        if (!bloomAdmitsAny(entry.key_bloom, hashes.items)) continue;
        var live = try delete_mod.LiveSegment.open(t, entry.segment_id);
        defer live.close(t);
        const seg = live.segment();

        var row_offset: u32 = 0;
        for (seg.info.row_groups, 0..) |rg, rg_idx| {
            defer row_offset += rg.row_count;
            if (prune_ok) {
                const admit = for (first_vals.items) |v| {
                    if (exec.predicate.statsOverlapPredicate(rg.stats[oki[0]], .eq, v)) break true;
                } else false;
                if (!admit) continue;
            }

            const decoded_keys = try aa.alloc(storage.OwnedColumn, oki.len);
            var decoded_count: usize = 0;
            defer for (decoded_keys[0..decoded_count]) |*c| c.deinit(t.allocator);
            for (oki, decoded_keys) |col_idx, *c| {
                c.* = try seg.decodeColumn(t.allocator, t.schema, rg_idx, col_idx);
                decoded_count += 1;
            }

            hits.clearRetainingCapacity();
            hit_slots.clearRetainingCapacity();
            for (0..rg.row_count) |r| {
                const row: u32 = @intCast(r);
                if (!live.isLive(row_offset + row)) continue;
                const key = try compoundKeyFromOwnedColumns(aa, decoded_keys, row);
                const slot = slot_of.get(key) orelse continue;
                if (slot_current[slot] != null) continue;
                slot_current[slot] = 0;
                try hits.append(aa, row);
                try hit_slots.append(aa, slot);
            }
            if (hits.items.len == 0) continue;

            const decoded_all = try aa.alloc(storage.OwnedColumn, t.schema.columns.len);
            var all_count: usize = 0;
            defer for (decoded_all[0..all_count]) |*c| c.deinit(t.allocator);
            const stores = try aa.alloc(storage.ColumnView, t.schema.columns.len);
            for (decoded_all, stores, 0..) |*c, *view, ci| {
                c.* = try seg.decodeColumn(t.allocator, t.schema, rg_idx, ci);
                all_count += 1;
                view.* = c.view();
            }
            try appendHitViews(t.allocator, stores, hits.items, hit_slots.items, current, slot_current);
            found += hits.items.len;
        }
    }
}

fn appendHits(
    allocator: Allocator,
    columns: []const engine.ColumnStore,
    rows: []const u32,
    slots: []const u32,
    current: *engine.Memtable,
    slot_current: []?u32,
) !void {
    if (rows.len == 0) return;
    for (columns, current.columns) |src, *dst| try engine.transform.appendByIndices(allocator, src.view(), rows, dst);
    assignSlots(current, slots, slot_current);
}

fn appendHitViews(
    allocator: Allocator,
    views: []const storage.ColumnView,
    rows: []const u32,
    slots: []const u32,
    current: *engine.Memtable,
    slot_current: []?u32,
) !void {
    for (views, current.columns) |view, *dst| try engine.transform.appendByIndices(allocator, view, rows, dst);
    assignSlots(current, slots, slot_current);
}

/// Point `slots` at the rows just appended to `current`, in order.
fn assignSlots(current: *engine.Memtable, slots: []const u32, slot_current: []?u32) void {
    const base: u32 = @intCast(current.row_count);
    for (slots, 0..) |slot, k| slot_current[slot] = base + @as(u32, @intCast(k));
    current.row_count += slots.len;
}

/// INSERT IGNORE: insert each key's first row when the table doesn't hold
/// the key.
fn insertAbsentRows(
    t: *Table,
    aa: Allocator,
    incoming: *const engine.Memtable,
    row_slot: []const u32,
    slot_current: []const ?u32,
    wal_target: *?u64,
) !DuplicateCounts {
    const keep = try aa.alloc(bool, row_slot.len);
    const seen = try aa.alloc(bool, slot_current.len);
    @memset(seen, false);
    var kept: usize = 0;
    for (row_slot, keep) |slot, *k| {
        k.* = slot_current[slot] == null and !seen[slot];
        seen[slot] = true;
        if (k.*) kept += 1;
    }
    if (kept == 0) return .{};
    const retained = try incoming.cloneWithRetainedRows(t.allocator, keep);
    defer if (retained) |m| {
        m.retire();
        m.release();
    };
    const rows = retained orelse incoming;
    const views = try aa.alloc(storage.ColumnView, rows.columns.len);
    for (rows.columns, views) |*c, *v| v.* = c.view();
    wal_target.* = try t.insertBatchInner(t.schema.columns, views, kept);
    return .{ .inserted = kept };
}

/// ON DUPLICATE KEY UPDATE, in rounds: round r takes each key's r-th row of
/// the statement, so a repeated key meets the row its previous occurrence
/// left, as MySQL's row-at-a-time loop would have it. A row whose key has a
/// current row rewrites it through the assignments; any other row becomes
/// its key's current row as is. Every key the statement touched is then
/// written once, replacing its stored row.
fn mergeRows(
    t: *Table,
    aa: Allocator,
    incoming: *const engine.Memtable,
    row_slot: []const u32,
    current: *engine.Memtable,
    slot_current: []?u32,
    assignments: []const update_mod.Assignment,
    wal_target: *?u64,
) !DuplicateCounts {
    var counts: DuplicateCounts = .{};
    const occurrence = try aa.alloc(u32, row_slot.len);
    const seen_count = try aa.alloc(u32, slot_current.len);
    @memset(seen_count, 0);
    var rounds: u32 = 0;
    for (row_slot, occurrence) |slot, *occ| {
        occ.* = seen_count[slot];
        seen_count[slot] += 1;
        rounds = @max(rounds, seen_count[slot]);
    }
    const touched = try aa.alloc(bool, slot_current.len);
    @memset(touched, false);

    var fresh: std.ArrayList(u32) = .empty;
    var fresh_slots: std.ArrayList(u32) = .empty;
    var pair_incoming: std.ArrayList(u32) = .empty;
    var pair_current: std.ArrayList(u32) = .empty;
    var pair_slots: std.ArrayList(u32) = .empty;
    for (0..rounds) |round| {
        fresh.clearRetainingCapacity();
        fresh_slots.clearRetainingCapacity();
        pair_incoming.clearRetainingCapacity();
        pair_current.clearRetainingCapacity();
        pair_slots.clearRetainingCapacity();
        for (row_slot, occurrence, 0..) |slot, occ, i| {
            if (occ != round) continue;
            touched[slot] = true;
            if (slot_current[slot]) |cur| {
                try pair_incoming.append(aa, @intCast(i));
                try pair_current.append(aa, cur);
                try pair_slots.append(aa, slot);
            } else {
                try fresh.append(aa, @intCast(i));
                try fresh_slots.append(aa, slot);
            }
        }
        if (fresh.items.len > 0) {
            try appendHits(t.allocator, incoming.columns, fresh.items, fresh_slots.items, current, slot_current);
            counts.inserted += fresh.items.len;
        }
        if (pair_incoming.items.len > 0) {
            try appendMergedRows(t, aa, incoming, current, pair_incoming.items, pair_current.items, assignments);
            assignSlots(current, pair_slots.items, slot_current);
            counts.updated += pair_incoming.items.len;
        }
    }

    var final_rows: std.ArrayList(u32) = .empty;
    for (touched, slot_current) |was_touched, cur| {
        if (was_touched) try final_rows.append(aa, cur.?);
    }
    var out = try engine.Memtable.init(t.allocator, t.schema);
    defer out.deinit();
    for (current.columns, out.columns) |src, *dst| try engine.transform.appendByIndices(t.allocator, src.view(), final_rows.items, dst);
    const views = try aa.alloc(storage.ColumnView, out.columns.len);
    for (out.columns, views) |*c, *v| v.* = c.view();
    wal_target.* = try t.insertBatchInner(t.schema.columns, views, final_rows.items.len);
    return counts;
}

/// Append to `current` the assignments' result for each pair: row
/// `current_rows[k]` of `current` as the stored row, row
/// `incoming_rows[k]` of `incoming` as the new one.
fn appendMergedRows(
    t: *Table,
    aa: Allocator,
    incoming: *const engine.Memtable,
    current: *engine.Memtable,
    incoming_rows: []const u32,
    current_rows: []const u32,
    assignments: []const update_mod.Assignment,
) !void {
    var stored = try engine.Memtable.init(t.allocator, t.schema);
    defer stored.deinit();
    var new_rows = try engine.Memtable.init(t.allocator, t.schema);
    defer new_rows.deinit();
    for (current.columns, incoming.columns, stored.columns, new_rows.columns) |cur_src, inc_src, *cur_dst, *inc_dst| {
        try engine.transform.appendByIndices(t.allocator, cur_src.view(), current_rows, cur_dst);
        try engine.transform.appendByIndices(t.allocator, inc_src.view(), incoming_rows, inc_dst);
    }
    const incoming_views = try aa.alloc(storage.ColumnView, new_rows.columns.len);
    for (new_rows.columns, incoming_views) |*c, *v| v.* = c.view();

    var merged = try update_mod.computeNewRows(t, .{ .stores = stored.columns, .row_count = current_rows.len }, incoming_views, assignments);
    defer update_mod.freeMaterializedRows(t.allocator, &merged);
    for (merged.stores, current.columns) |*src, *dst| try engine.transform.appendAllColumn(t.allocator, src.view(), dst);
}

test "memtable swaps invalidate the incremental upsert index (gen counter, not pointer)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .unique = true });

    // Interleave upserts (which build/extend the index) with DELETEs (which
    // clone-and-swap the memtable) many times. With the old pointer-identity
    // binding, an allocator reusing a freed memtable's address revalidated a
    // stale index whose row mappings then tombstoned UNRELATED live rows.
    var round: i32 = 1;
    while (round <= 40) : (round += 1) {
        try t.insert(&.{
            .{ .id = @as(i64, 1), .v = round }, .{ .id = @as(i64, 2), .v = round },
            .{ .id = @as(i64, 3), .v = round }, .{ .id = @as(i64, 4), .v = round },
            .{ .id = @as(i64, 5), .v = round }, .{ .id = @as(i64, 6), .v = round },
        });
        const gen_before = t.memtable_gen;
        const pred: exec.PredicateExpr = .{ .leaf = .{ .col = "id", .op = .eq, .val = .{ .bigint = 3 } } };
        _ = try t.deleteByExpr(pred, &.{});
        try std.testing.expect(t.memtable_gen > gen_before); // swap bumped the generation
        // Re-add the deleted key; the rebuilt index must dedup it correctly.
        try t.insert(&.{.{ .id = @as(i64, 3), .v = round }});
    }
    // No flush happened: every live row is in the memtable, and dedup
    // physically removes older versions — exactly 6 keys must remain.
    try std.testing.expectEqual(@as(u32, 6), t.memtable.row_count);
}

test "keyed Bloom gate: a literal IN list, spelled as an OR of equalities, pins the key" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .unique = true });
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    const Pe = exec.PredicateExpr;
    const listed = [_]Pe{
        .{ .leaf = .{ .col = "ID", .op = .eq, .val = .{ .bigint = 1 } } },
        .{ .leaf = .{ .col = "id", .op = .eq, .val = .{ .bigint = 2 } } },
        .{ .leaf = .{ .col = "id", .op = .eq, .val = .{ .bigint = 3 } } },
    };
    const in_list: Pe = .{ .@"or" = &listed };
    const with_extra = [_]Pe{ in_list, .{ .leaf = .{ .col = "v", .op = .gt, .val = .{ .int = 0 } } } };

    var point_hashes: [3]u64 = undefined;
    for (&listed, &point_hashes) |arm, *h| h.* = (try keyHashesFromPredicateExpr(t, aa, arm)).?[0];
    try std.testing.expectEqualSlices(u64, &point_hashes, (try keyHashesFromPredicateExpr(t, aa, in_list)).?);
    try std.testing.expectEqualSlices(u64, &point_hashes, (try keyHashesFromPredicateExpr(t, aa, .{ .@"and" = &with_extra })).?);

    // An OR over two columns, or a NOT IN, pins nothing.
    const mixed = [_]Pe{ listed[0], with_extra[1] };
    try std.testing.expect((try keyHashesFromPredicateExpr(t, aa, .{ .@"or" = &mixed })) == null);
    try std.testing.expect((try keyHashesFromPredicateExpr(t, aa, .{ .not = &in_list })) == null);
}

test "upsert probe with zonemap pruning still tombstones the old segment copy" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .unique = true });

    // 12 rows -> one flushed segment with 3 row groups.
    try t.insert(&.{
        .{ .id = @as(i64, 1), .v = @as(i32, 1) },   .{ .id = @as(i64, 2), .v = @as(i32, 2) },
        .{ .id = @as(i64, 3), .v = @as(i32, 3) },   .{ .id = @as(i64, 4), .v = @as(i32, 4) },
        .{ .id = @as(i64, 5), .v = @as(i32, 5) },   .{ .id = @as(i64, 6), .v = @as(i32, 6) },
        .{ .id = @as(i64, 7), .v = @as(i32, 7) },   .{ .id = @as(i64, 8), .v = @as(i32, 8) },
        .{ .id = @as(i64, 9), .v = @as(i32, 9) },   .{ .id = @as(i64, 10), .v = @as(i32, 10) },
        .{ .id = @as(i64, 11), .v = @as(i32, 11) }, .{ .id = @as(i64, 12), .v = @as(i32, 12) },
    });
    try t.flush();
    try std.testing.expectEqual(@as(usize, 1), t.manifest.segments.items.len);
    const seg_id = t.manifest.segments.items[0].segment_id;

    // Re-insert key 6 (middle row group) — resolution must tombstone the
    // old copy even though the other row groups get zonemap-pruned.
    try t.insert(&.{.{ .id = @as(i64, 6), .v = @as(i32, 60) }});

    const tombs = (try storage.tombstone.read(allocator, io, t.segments_dir, seg_id)) orelse
        return error.TestUnexpectedResult;
    defer allocator.free(tombs);
    try std.testing.expectEqualSlices(u32, &.{5}, tombs); // id=6 sits at offset 5
}

test "upsert probe pruning: old copy lives in a COMPACTION-MERGED segment" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const compact_mod = @import("compact.zig");

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"id"}, .unique = true });

    try t.insert(&.{
        .{ .id = @as(i64, 1), .v = @as(i32, 1) },
        .{ .id = @as(i64, 2), .v = @as(i32, 2) },
        .{ .id = @as(i64, 3), .v = @as(i32, 3) },
        .{ .id = @as(i64, 4), .v = @as(i32, 4) },
    });
    try t.flush();
    try t.insert(&.{
        .{ .id = @as(i64, 5), .v = @as(i32, 5) },
        .{ .id = @as(i64, 6), .v = @as(i32, 6) },
    });
    try t.flush();

    // Fold both flush segments into ONE merged segment — the probe's target
    // is now a MergedSegmentWriter product, not a flush product.
    const input_ids = [_]u64{
        t.manifest.segments.items[0].segment_id,
        t.manifest.segments.items[1].segment_id,
    };
    try compact_mod.mergeSegments(t, &input_ids);
    try std.testing.expectEqual(@as(usize, 1), t.manifest.segments.items.len);
    const merged_id = t.manifest.segments.items[0].segment_id;

    // Re-upsert key 3: resolution must tombstone the old copy inside the
    // merged segment (zonemap pruning must admit its row group).
    try t.insert(&.{.{ .id = @as(i64, 3), .v = @as(i32, 30) }});

    const tombs = (try storage.tombstone.read(allocator, io, t.segments_dir, merged_id)) orelse
        return error.TestUnexpectedResult; // BUG: old copy in merged segment survived
    defer allocator.free(tombs);
    try std.testing.expectEqual(@as(usize, 1), tombs.len);
    try std.testing.expectEqualSlices(u32, &.{2}, tombs); // id=3 at merged offset 2
}

test "upsert probe pruning: string first key column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "k", .type = .string },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{"k"},
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 2 });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"k"}, .unique = true });

    try t.insert(&.{
        .{ .k = "alpha", .v = @as(i32, 1) },
        .{ .k = "bravo", .v = @as(i32, 2) },
        .{ .k = "charlie", .v = @as(i32, 3) },
        .{ .k = "delta", .v = @as(i32, 4) },
    });
    try t.flush();
    const seg_id = t.manifest.segments.items[0].segment_id;

    try t.insert(&.{.{ .k = "delta", .v = @as(i32, 40) }});

    const tombs = (try storage.tombstone.read(allocator, io, t.segments_dir, seg_id)) orelse
        return error.TestUnexpectedResult;
    defer allocator.free(tombs);
    try std.testing.expectEqualSlices(u32, &.{3}, tombs);
}
