//! Unique-key upsert resolution. Implements StarRocks-style "last writer
//! wins" semantics on tables created with `unique = true`. Called from
//! `Table.insert` after `insertRows` lands the new rows.

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
    t.upsert_idx_gen = null;
    t.upsert_idx_rows = 0;
}

pub fn applyUpsertResolution(t: *Table) !void {
    std.debug.assert(t.order_key_indices.len > 0);

    const n: usize = @intCast(t.memtable.row_count);
    if (n == 0) {
        upsertIndexReset(t);
        return;
    }

    // Bind the persistent key index to the current memtable generation
    // counter. ANY swap (flush / delete / update / dedup-clone) bumps the
    // counter via installMemtableLocked → rebuild from row 0; otherwise
    // process only the rows added since last time. NOT a pointer compare —
    // a freed memtable's address can be reused by a later clone (ABA),
    // silently revalidating a stale index whose row mappings then
    // tombstone unrelated rows.
    const same_gen = if (t.upsert_idx_gen) |g| g == t.memtable_gen else false;
    if (!same_gen or t.upsert_idx_rows > n) {
        upsertIndexReset(t);
        t.upsert_idx_gen = t.memtable_gen;
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
    // First order-key column value per NEW key, for row-group zonemap
    // pruning during the segment probe (#138). String bytes are duped into
    // the arena — the memtable may be swapped before the probe runs.
    var new_first_vals: std.ArrayList(types.Value) = .empty;
    var prune_ok = true;

    const first_key_view = t.memtable.columns[t.order_key_indices[0]].view();
    for (start..n) |i| {
        const key_bytes = try compoundKeyFromColumnStores(idx_aa, t.memtable.columns, t.order_key_indices, @intCast(i));
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
            t.installMemtableLocked(new_mt);
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

/// Full-key Bloom candidates for a keyed DELETE/UPDATE (#143). When the
/// predicate's top-level AND conjuncts (or a bare leaf) pin every order-key
/// column with equality — allowing at most one column an IN set of ≤256
/// values — return the compound-key hashes, encoded exactly as the Bloom was
/// built. Returns null when the key set can't be derived (caller scans
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
            .in_set => |s| {
                if (!s.negate and types.columnNameEql(s.col, col_name) and eq_vals[k] == null) {
                    if (in_vals != null and in_pos != k) return null; // two IN-bound key columns
                    if (s.values.len == 0 or s.values.len > max_keys) return null;
                    in_vals = s.values;
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

/// Flatten a (possibly nested) AND tree into its leaf/in_set conjuncts.
/// Returns false when the expression contains any non-conjunctive node
/// (OR, NOT, ...) — the caller must then skip bloom gating entirely.
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
fn viewValueAt(aa: Allocator, view: storage.ColumnView, row: u32) !?types.Value {
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
        _ = try t.deleteByExpr(pred);
        try std.testing.expect(t.memtable_gen > gen_before); // swap bumped the generation
        // Re-add the deleted key; the rebuilt index must dedup it correctly.
        try t.insert(&.{.{ .id = @as(i64, 3), .v = round }});
    }
    // No flush happened: every live row is in the memtable, and dedup
    // physically removes older versions — exactly 6 keys must remain.
    try std.testing.expectEqual(@as(u32, 6), t.memtable.row_count);
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
