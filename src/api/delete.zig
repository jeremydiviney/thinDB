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
const upsert = @import("upsert.zig");
const bloom = @import("../util/bloom.zig");

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

/// A statement's full order key, encoded with the same byte layout the
/// key Bloom uses, plus the first key column's value for zonemap checks.
const StrictKey = struct {
    bytes: []const u8,
    first_val: types.Value,
};

/// Extract the exact order key from a keyed DELETE's predicate. Strict:
/// every conjunct must be an `=` on a distinct order-key column and every
/// key column must be bound — anything else (extra conjuncts, ranges, IN
/// sets, OR trees, a column bound twice) returns null so the caller runs
/// the statement through the general path. Unlike the Bloom gate's
/// `keyHashesFromPredicateExpr` (which may ignore extra conjuncts because
/// it only ever *skips* segments), this result *is* the delete condition,
/// so it must capture the predicate exactly.
fn strictKeyFromExpr(t: *Table, aa: std.mem.Allocator, expr: predicate.PredicateExpr) !?StrictKey {
    const oki = t.order_key_indices;
    var conj_list: std.ArrayList(predicate.PredicateExpr) = .empty;
    if (!try upsert.appendConjuncts(aa, &conj_list, expr)) return null;

    const eq_vals = try aa.alloc(?types.Value, oki.len);
    @memset(eq_vals, null);
    for (conj_list.items) |c| {
        const p = switch (c) {
            .leaf => |p| p,
            else => return null,
        };
        if (p.op != .eq) return null;
        const slot: usize = blk: {
            for (oki, 0..) |col_idx, k| {
                if (types.columnNameEql(p.col, t.schema.columns[col_idx].name)) break :blk k;
            }
            return null;
        };
        if (eq_vals[slot] != null) return null;
        eq_vals[slot] = p.val;
    }

    var key_buf: std.ArrayList(u8) = .empty;
    for (oki, 0..) |col_idx, k| {
        const v = eq_vals[k] orelse return null;
        if (!try comparison.appendPredicateValueBytes(aa, &key_buf, t.schema.columns[col_idx].type, v)) return null;
    }
    return .{ .bytes = try key_buf.toOwnedSlice(aa), .first_val = eq_vals[0].? };
}

/// True when every statement in `preds` is a strict full-key delete on
/// this unique table — i.e. `execDeleteKeyedBatch` will take the batch.
/// Callers that must WAL-log before executing check this first.
pub fn keyedBatchEligible(t: *Table, preds: []const ?predicate.PredicateExpr) !bool {
    if (!t.schema.unique or t.order_key_indices.len == 0) return false;
    var arena = std.heap.ArenaAllocator.init(t.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    for (preds) |pred_opt| {
        const p = pred_opt orelse return false;
        if ((try strictKeyFromExpr(t, aa, p)) == null) return false;
    }
    return true;
}

/// Batched keyed DELETE (#146): execute N statements' worth of full-key
/// deletes as ONE table sweep. Builds a hash set of all encoded keys,
/// Bloom-gates each segment against the whole set, zonemap-prunes row
/// groups on the first key column, decodes only the key columns of
/// admitted row groups, probes each row against the set, and writes each
/// segment's tombstone file at most once. `counts[j]` receives statement
/// j's matched-row count (serial semantics: a key repeated across
/// statements credits the first). Returns null — before any mutation —
/// when the batch isn't eligible; total matched rows otherwise.
///
/// Caller holds the table mutex. Literals must already be widened
/// (`validateExpr`), same as `execDeleteByExpr`.
pub fn execDeleteKeyedBatch(
    t: *Table,
    preds: []const ?predicate.PredicateExpr,
    counts: []usize,
) !?usize {
    std.debug.assert(preds.len == counts.len);
    if (!t.schema.unique or t.order_key_indices.len == 0) return null;
    const oki = t.order_key_indices;

    var arena = std.heap.ArenaAllocator.init(t.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var key_map: std.StringHashMapUnmanaged(u32) = .empty;
    var hashes: std.ArrayList(u64) = .empty;
    var first_vals: std.ArrayList(types.Value) = .empty;
    for (preds, 0..) |pred_opt, j| {
        const p = pred_opt orelse return null;
        const key = (try strictKeyFromExpr(t, aa, p)) orelse return null;
        const gop = try key_map.getOrPut(aa, key.bytes);
        if (!gop.found_existing) {
            gop.value_ptr.* = @intCast(j);
            try hashes.append(aa, bloom.keyHash(key.bytes));
            try first_vals.append(aa, key.first_val);
        }
    }
    @memset(counts, 0);
    var total: usize = 0;

    var keybuf: std.ArrayList(u8) = .empty;

    // ---- Segments ----
    for (t.manifest.segments.items) |entry| {
        if (!upsert.bloomAdmitsAny(entry.key_bloom, hashes.items)) continue;

        const handle = try t.acquireSegment(entry.segment_id);
        defer t.releaseSegment(handle);
        const seg = &handle.seg;

        var deleted: std.ArrayList(u32) = .empty;
        defer deleted.deinit(t.allocator);

        var row_offset: u32 = 0;
        for (seg.info.row_groups, 0..) |rg, rg_idx| {
            const n = rg.row_count;

            var admit = false;
            for (first_vals.items) |v| {
                if (predicate.statsOverlapPredicate(rg.stats[oki[0]], .eq, v)) {
                    admit = true;
                    break;
                }
            }
            if (!admit) {
                row_offset += n;
                continue;
            }

            const decoded = try aa.alloc(storage.OwnedColumn, oki.len);
            for (oki, 0..) |col_idx, k| {
                decoded[k] = try seg.decodeColumn(t.allocator, t.schema, rg_idx, col_idx);
            }
            defer for (decoded) |*c| {
                var d = c.*;
                d.deinit(t.allocator);
            };

            var row: u32 = 0;
            while (row < n) : (row += 1) {
                keybuf.clearRetainingCapacity();
                for (decoded) |c| try comparison.appendColumnValueBytes(aa, &keybuf, c.view(), row);
                if (key_map.get(keybuf.items)) |stmt_idx| {
                    try deleted.append(t.allocator, row_offset + row);
                    counts[stmt_idx] += 1;
                }
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
    // Same snapshot-isolated clone-and-swap shape as `execDeleteByExpr`.
    if (t.memtable.row_count > 0) {
        const n: usize = @intCast(t.memtable.row_count);
        const keep = try t.allocator.alloc(bool, n);
        defer t.allocator.free(keep);

        var matched_any = false;
        for (0..n) |i| {
            keybuf.clearRetainingCapacity();
            for (oki) |ci| {
                try comparison.appendColumnValueBytes(aa, &keybuf, t.memtable.columns[ci].view(), @intCast(i));
            }
            if (key_map.get(keybuf.items)) |stmt_idx| {
                keep[i] = false;
                matched_any = true;
                counts[stmt_idx] += 1;
                total += 1;
            } else {
                keep[i] = true;
            }
        }

        if (matched_any) {
            if (try t.memtable.cloneWithRetainedRows(t.allocator, keep)) |new_mt| {
                const old_mt = t.memtable;
                t.memtable = new_mt;
                old_mt.retire();
                old_mt.release();
            }
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

test "keyed batch delete: one sweep, per-statement counts, literal widening" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "mt", .type = .string },
            .{ .name = "v", .type = .int },
        },
        .order_key = &.{ "id", "mt" },
        .unique = true,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{ "id", "mt" }, .unique = true });

    try t.insert(&.{
        .{ .id = @as(i64, 1), .mt = "a", .v = @as(i32, 10) },
        .{ .id = @as(i64, 2), .mt = "b", .v = @as(i32, 20) },
        .{ .id = @as(i64, 3), .mt = "c", .v = @as(i32, 30) },
        .{ .id = @as(i64, 4), .mt = "d", .v = @as(i32, 40) },
    });
    try t.flush();
    try t.insert(&.{
        .{ .id = @as(i64, 5), .mt = "e", .v = @as(i32, 50) },
        .{ .id = @as(i64, 6), .mt = "f", .v = @as(i32, 60) },
    });
    try t.flush();
    try t.insert(&.{
        .{ .id = @as(i64, 7), .mt = "g", .v = @as(i32, 70) },
    });
    try std.testing.expectEqual(@as(usize, 2), t.manifest.segments.items.len);

    const Pe = predicate.PredicateExpr;
    // Compound keys parse as nested ANDs in real SQL; two-conjunct trees
    // exercise the same appendConjuncts flatten path. Arrays are `var`
    // because validateExpr widens literals in place.
    var c0 = [_]Pe{
        .{ .leaf = .{ .col = "id", .op = .eq, .val = .{ .bigint = 1 } } },
        .{ .leaf = .{ .col = "mt", .op = .eq, .val = .{ .text = "a" } } },
    };
    var c1 = [_]Pe{
        .{ .leaf = .{ .col = "id", .op = .eq, .val = .{ .bigint = 42 } } },
        .{ .leaf = .{ .col = "mt", .op = .eq, .val = .{ .text = "zz" } } },
    };
    var c2 = [_]Pe{
        .{ .leaf = .{ .col = "id", .op = .eq, .val = .{ .bigint = 7 } } },
        .{ .leaf = .{ .col = "mt", .op = .eq, .val = .{ .text = "g" } } },
    };
    var c3 = [_]Pe{
        .{ .leaf = .{ .col = "id", .op = .eq, .val = .{ .bigint = 1 } } },
        .{ .leaf = .{ .col = "mt", .op = .eq, .val = .{ .text = "a" } } },
    };
    // .int literal on a bigint key column — the wrapper's validateExpr
    // widening must make this key encode identically to .bigint.
    var c4 = [_]Pe{
        .{ .leaf = .{ .col = "id", .op = .eq, .val = .{ .int = 5 } } },
        .{ .leaf = .{ .col = "mt", .op = .eq, .val = .{ .text = "e" } } },
    };
    const preds = [_]?Pe{
        .{ .@"and" = &c0 },
        .{ .@"and" = &c1 },
        .{ .@"and" = &c2 },
        .{ .@"and" = &c3 },
        .{ .@"and" = &c4 },
    };
    var counts: [5]usize = undefined;

    const total = (try t.deleteKeyedBatch(&preds, &counts)) orelse
        return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 3), total);
    try std.testing.expectEqualSlices(usize, &.{ 1, 0, 1, 0, 1 }, &counts);
    try std.testing.expectEqual(@as(u32, 0), t.memtable.row_count);

    const seg1 = t.manifest.segments.items[0].segment_id;
    const seg2 = t.manifest.segments.items[1].segment_id;
    const tombs1 = (try storage.tombstone.read(allocator, io, t.segments_dir, seg1)) orelse
        return error.TestUnexpectedResult;
    defer allocator.free(tombs1);
    try std.testing.expectEqualSlices(u32, &.{0}, tombs1);
    const tombs2 = (try storage.tombstone.read(allocator, io, t.segments_dir, seg2)) orelse
        return error.TestUnexpectedResult;
    defer allocator.free(tombs2);
    try std.testing.expectEqualSlices(u32, &.{0}, tombs2);
}

test "keyed batch delete: rejects shapes it can't take exactly" {
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

    try t.insert(&.{
        .{ .id = @as(i64, 1), .v = @as(i32, 10) },
        .{ .id = @as(i64, 2), .v = @as(i32, 20) },
    });
    try t.flush();

    const Pe = predicate.PredicateExpr;
    var counts: [1]usize = undefined;

    // Extra non-key conjunct: `id=1 AND v=999` matches nothing, so taking
    // it as "delete key 1" would over-delete. Must decline the batch.
    var extra = [_]Pe{
        .{ .leaf = .{ .col = "id", .op = .eq, .val = .{ .bigint = 1 } } },
        .{ .leaf = .{ .col = "v", .op = .eq, .val = .{ .int = 999 } } },
    };
    const with_extra = [_]?Pe{.{ .@"and" = &extra }};
    try std.testing.expectEqual(@as(?usize, null), try t.deleteKeyedBatch(&with_extra, &counts));

    // Range on the key column.
    const ranged = [_]?Pe{.{ .leaf = .{ .col = "id", .op = .gt, .val = .{ .bigint = 0 } } }};
    try std.testing.expectEqual(@as(?usize, null), try t.deleteKeyedBatch(&ranged, &counts));

    // Missing predicate (DELETE without WHERE).
    const bare = [_]?Pe{null};
    try std.testing.expectEqual(@as(?usize, null), try t.deleteKeyedBatch(&bare, &counts));

    // Key column bound twice (`id=1 AND id=2` is unsatisfiable serially).
    var twice = [_]Pe{
        .{ .leaf = .{ .col = "id", .op = .eq, .val = .{ .bigint = 1 } } },
        .{ .leaf = .{ .col = "id", .op = .eq, .val = .{ .bigint = 2 } } },
    };
    const bound_twice = [_]?Pe{.{ .@"and" = &twice }};
    try std.testing.expectEqual(@as(?usize, null), try t.deleteKeyedBatch(&bound_twice, &counts));

    // Nothing was deleted by any of the declined batches.
    try std.testing.expectEqual(
        @as(?[]u32, null),
        try storage.tombstone.read(allocator, io, t.segments_dir, t.manifest.segments.items[0].segment_id),
    );
}
