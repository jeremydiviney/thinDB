//! Per-column transform helpers — used both by the memtable (for sort
//! permutation, retain-row rebuilds) and by exec operators (for filter
//! masking, materializing batches in sort order).

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const store = @import("store.zig");
const ColumnStore = store.ColumnStore;
const DataStore = store.DataStore;
const StringStore = store.StringStore;

/// True when `row`'s validity bit is set (or the column is non-nullable /
/// the bitmap was never written that far — both mean "no NULL recorded").
pub fn rowIsValid(col: ColumnStore, row: u32) bool {
    const nb = col.nulls orelse return true;
    const byte_idx = row >> 3;
    if (byte_idx >= nb.items.len) return true;
    return (nb.items[byte_idx] & (@as(u8, 1) << @intCast(row & 7))) != 0;
}

/// Validity-aware variant of `compareInColumn` for QUERY-side consumers
/// (Sort, TopN, Window): NULL orders before every value (the dialect's
/// NULLs-first-ASC convention; DESC handling in the callers puts it last),
/// two NULLs compare equal. A NULL slot's payload bytes are encoding
/// artifacts (FOR base, dict entry 0), so the raw comparator must not see
/// them. The raw `compareInColumn` keeps the storage-side contract (the
/// segment k-way merge deliberately ignores validity).
pub fn compareInColumnNullsFirst(col: ColumnStore, a: u32, b: u32) std.math.Order {
    if (col.nulls != null) {
        const av = rowIsValid(col, a);
        const bv = rowIsValid(col, b);
        if (!av or !bv) {
            if (av == bv) return .eq;
            return if (av) .gt else .lt;
        }
    }
    return compareInColumn(col, a, b);
}

/// Compare row `a` vs row `b` within a single column. Lexicographic order
/// for strings, numeric order for everything else. Used by sort kernels.
/// Raw value order — validity is NOT consulted (storage merge contract);
/// query-side ordering goes through `compareInColumnNullsFirst`.
pub fn compareInColumn(col: ColumnStore, a: u32, b: u32) std.math.Order {
    return switch (col.data) {
        .int => |l| std.math.order(l.items[a], l.items[b]),
        .bigint => |l| std.math.order(l.items[a], l.items[b]),
        .boolean => |l| std.math.order(l.items[a], l.items[b]),
        // rowBytesWide transparently honors the u64 sidecar (Sort over >4 GiB).
        .varchar => |s| std.mem.order(u8, s.rowBytesWide(a), s.rowBytesWide(b)),
        .string => |s| std.mem.order(u8, s.rowBytesWide(a), s.rowBytesWide(b)),
        .char => |s| std.mem.order(u8, s.rowBytesWide(a), s.rowBytesWide(b)),
        .tinyint => |l| std.math.order(l.items[a], l.items[b]),
        .smallint => |l| std.math.order(l.items[a], l.items[b]),
        .largeint => |l| std.math.order(l.items[a], l.items[b]),
        .float => |l| types.floatOrder(l.items[a], l.items[b]),
        .double => |l| types.floatOrder(l.items[a], l.items[b]),
        .date => |l| std.math.order(l.items[a], l.items[b]),
        .datetime => |l| std.math.order(l.items[a], l.items[b]),
        .decimal64 => |l| std.math.order(l.items[a], l.items[b]),
        .decimal128 => |l| std.math.order(l.items[a], l.items[b]),
        .uuid => |l| std.math.order(l.items[a], l.items[b]),
    };
}

/// Compare row `a` of `va` against row `b` of `vb`, where both are views over
/// the SAME column type. Mirrors `compareInColumn` exactly (raw value order,
/// including NULL-slot placeholders — validity is not consulted) so a streaming
/// k-way merge across segments produces the same total order as the single-table
/// `buildSortedSnapshot` sort. The two views' tags must match.
pub fn compareViewRows(va: ColumnView, a: usize, vb: ColumnView, b: usize) std.math.Order {
    return switch (va.data) {
        .int => |l| std.math.order(l[a], vb.data.int[b]),
        .bigint => |l| std.math.order(l[a], vb.data.bigint[b]),
        .boolean => |l| std.math.order(l[a], vb.data.boolean[b]),
        .varchar => |s| std.mem.order(u8, s.rowBytes(a), vb.data.varchar.rowBytes(b)),
        .string => |s| std.mem.order(u8, s.rowBytes(a), vb.data.string.rowBytes(b)),
        .char => |s| std.mem.order(u8, s.rowBytes(a), vb.data.char.rowBytes(b)),
        .tinyint => |l| std.math.order(l[a], vb.data.tinyint[b]),
        .smallint => |l| std.math.order(l[a], vb.data.smallint[b]),
        .largeint => |l| std.math.order(l[a], vb.data.largeint[b]),
        .float => |l| types.floatOrder(l[a], vb.data.float[b]),
        .double => |l| types.floatOrder(l[a], vb.data.double[b]),
        .date => |l| std.math.order(l[a], vb.data.date[b]),
        .datetime => |l| std.math.order(l[a], vb.data.datetime[b]),
        .decimal64 => |l| std.math.order(l[a], vb.data.decimal64[b]),
        .decimal128 => |l| std.math.order(l[a], vb.data.decimal128[b]),
        .uuid => |l| std.math.order(l[a], vb.data.uuid[b]),
    };
}

/// Append every row of `view` (including its validity bits if `view.nulls`
/// is non-null) to `out`. Types must match.
pub fn appendAllColumn(
    allocator: Allocator,
    view: ColumnView,
    out: *ColumnStore,
) !void {
    const dst_start = out.data.rowCount();
    switch (view.data) {
        .int => |s| switch (out.data) {
            .int => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
        .bigint => |s| switch (out.data) {
            .bigint => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
        .boolean => |s| switch (out.data) {
            .boolean => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
        .varchar => |sv| switch (out.data) {
            .varchar => |*ss| {
                for (0..sv.rowCount()) |i| try ss.appendValue(allocator, sv.rowBytes(i));
            },
            else => unreachable,
        },
        .string => |sv| switch (out.data) {
            .string => |*ss| {
                for (0..sv.rowCount()) |i| try ss.appendValue(allocator, sv.rowBytes(i));
            },
            else => unreachable,
        },
        .float => |s| switch (out.data) {
            .float => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
        .double => |s| switch (out.data) {
            .double => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
        .date => |s| switch (out.data) {
            .date => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
        .datetime => |s| switch (out.data) {
            .datetime => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
        .tinyint => |s| switch (out.data) {
            .tinyint => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
        .smallint => |s| switch (out.data) {
            .smallint => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
        .largeint => |s| switch (out.data) {
            .largeint => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
        .char => |sv| switch (out.data) {
            .char => |*ss| {
                for (0..sv.rowCount()) |i| try ss.appendValue(allocator, sv.rowBytes(i));
            },
            else => unreachable,
        },
        .decimal64 => |s| switch (out.data) {
            .decimal64 => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
        .decimal128 => |s| switch (out.data) {
            .decimal128 => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
        .uuid => |s| switch (out.data) {
            .uuid => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
    }
    if (out.nulls != null) {
        try out.appendValidityRange(allocator, dst_start, view.nulls, view.data.rowCount());
    }
}

/// Append a single row (`row`) from `view` to `out`, carrying its validity
/// bit. Used by the bounded Top-N to copy in only the rows that survive its
/// threshold pre-filter, one candidate at a time, rather than the whole batch.
pub fn appendOneRow(
    allocator: Allocator,
    view: ColumnView,
    row: usize,
    out: *ColumnStore,
) !void {
    const dst_row = out.data.rowCount();
    switch (view.data) {
        .int => |s| switch (out.data) {
            .int => |*list| try list.append(allocator, s[row]),
            else => unreachable,
        },
        .bigint => |s| switch (out.data) {
            .bigint => |*list| try list.append(allocator, s[row]),
            else => unreachable,
        },
        .boolean => |s| switch (out.data) {
            .boolean => |*list| try list.append(allocator, s[row]),
            else => unreachable,
        },
        .varchar => |sv| switch (out.data) {
            .varchar => |*ss| try ss.appendValue(allocator, sv.rowBytes(row)),
            else => unreachable,
        },
        .string => |sv| switch (out.data) {
            .string => |*ss| try ss.appendValue(allocator, sv.rowBytes(row)),
            else => unreachable,
        },
        .float => |s| switch (out.data) {
            .float => |*list| try list.append(allocator, s[row]),
            else => unreachable,
        },
        .double => |s| switch (out.data) {
            .double => |*list| try list.append(allocator, s[row]),
            else => unreachable,
        },
        .date => |s| switch (out.data) {
            .date => |*list| try list.append(allocator, s[row]),
            else => unreachable,
        },
        .datetime => |s| switch (out.data) {
            .datetime => |*list| try list.append(allocator, s[row]),
            else => unreachable,
        },
        .tinyint => |s| switch (out.data) {
            .tinyint => |*list| try list.append(allocator, s[row]),
            else => unreachable,
        },
        .smallint => |s| switch (out.data) {
            .smallint => |*list| try list.append(allocator, s[row]),
            else => unreachable,
        },
        .largeint => |s| switch (out.data) {
            .largeint => |*list| try list.append(allocator, s[row]),
            else => unreachable,
        },
        .char => |sv| switch (out.data) {
            .char => |*ss| try ss.appendValue(allocator, sv.rowBytes(row)),
            else => unreachable,
        },
        .decimal64 => |s| switch (out.data) {
            .decimal64 => |*list| try list.append(allocator, s[row]),
            else => unreachable,
        },
        .decimal128 => |s| switch (out.data) {
            .decimal128 => |*list| try list.append(allocator, s[row]),
            else => unreachable,
        },
        .uuid => |s| switch (out.data) {
            .uuid => |*list| try list.append(allocator, s[row]),
            else => unreachable,
        },
    }
    if (out.nulls != null) {
        const valid = storage.column.isValidBit(view.nulls, row);
        try out.appendValidBit(allocator, dst_row, valid);
    }
}

/// Append rows from `view` to `out`, picking by the given indices into `view`.
/// Used by Sort to materialize batches in permutation order. Validity bits
/// are carried across via `view.nulls`.
pub fn appendByIndices(
    allocator: Allocator,
    view: ColumnView,
    indices: []const u32,
    out: *ColumnStore,
) !void {
    const dst_start = out.data.rowCount();
    switch (view.data) {
        .int => |s| switch (out.data) {
            .int => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
        .bigint => |s| switch (out.data) {
            .bigint => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
        .boolean => |s| switch (out.data) {
            .boolean => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
        .varchar => |sv| switch (out.data) {
            .varchar => |*ss| {
                for (indices) |idx| try ss.appendValue(allocator, sv.rowBytes(idx));
            },
            else => unreachable,
        },
        .string => |sv| switch (out.data) {
            .string => |*ss| {
                for (indices) |idx| try ss.appendValue(allocator, sv.rowBytes(idx));
            },
            else => unreachable,
        },
        .float => |s| switch (out.data) {
            .float => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
        .double => |s| switch (out.data) {
            .double => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
        .date => |s| switch (out.data) {
            .date => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
        .datetime => |s| switch (out.data) {
            .datetime => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
        .tinyint => |s| switch (out.data) {
            .tinyint => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
        .smallint => |s| switch (out.data) {
            .smallint => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
        .largeint => |s| switch (out.data) {
            .largeint => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
        .char => |sv| switch (out.data) {
            .char => |*ss| {
                for (indices) |idx| try ss.appendValue(allocator, sv.rowBytes(idx));
            },
            else => unreachable,
        },
        .decimal64 => |s| switch (out.data) {
            .decimal64 => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
        .decimal128 => |s| switch (out.data) {
            .decimal128 => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
        .uuid => |s| switch (out.data) {
            .uuid => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
    }
    if (out.nulls != null) {
        for (indices, 0..) |src_idx, j| {
            const valid = storage.column.isValidBit(view.nulls, src_idx);
            try out.appendValidBit(allocator, dst_start + j, valid);
        }
    }
}

/// Append the mask-selected elements of `src` onto `list` via branchless
/// stream compaction: reserve the exact survivor count once, then write every
/// element to the running output slot and advance the slot only on a set mask
/// bit. This keeps one predictable store per row (no per-element `append` call,
/// capacity check, or data-dependent branch) — an order of magnitude faster
/// than a per-row `if (m) try append` on a non-selective filter.
fn compactInto(
    comptime T: type,
    allocator: Allocator,
    src: []const T,
    mask: []const bool,
    list: *std.ArrayList(T),
) !void {
    var survivors: usize = 0;
    for (mask) |m| survivors += @intFromBool(m);
    if (survivors == 0) return;
    // The branchless body writes `base[pos] = v` for EVERY row and advances
    // `pos` only on survivors, so after the last survivor any trailing
    // non-survivor stores into `base[len + survivors]` — one slot past the
    // `survivors` live elements. Reserve that extra scratch slot so the throw-
    // away store stays in-bounds (its value is never committed: `len` below is
    // set to `pos == old_len + survivors`). Omitting the +1 is a heap overrun
    // whenever the last selected row is filtered out.
    try list.ensureUnusedCapacity(allocator, survivors + 1);
    const base = list.items.ptr;
    var pos = list.items.len;
    for (src, mask) |v, m| {
        base[pos] = v;
        pos += @intFromBool(m);
    }
    list.items.len = pos;
}

pub fn appendMaskedColumn(
    allocator: Allocator,
    view: ColumnView,
    mask: []const bool,
    out: *ColumnStore,
) !void {
    const dst_start = out.data.rowCount();
    switch (view.data) {
        .int => |s| switch (out.data) {
            .int => |*list| try compactInto(i32, allocator, s, mask, list),
            else => unreachable,
        },
        .bigint => |s| switch (out.data) {
            .bigint => |*list| try compactInto(i64, allocator, s, mask, list),
            else => unreachable,
        },
        .boolean => |s| switch (out.data) {
            .boolean => |*list| try compactInto(u8, allocator, s, mask, list),
            else => unreachable,
        },
        // String family: any source (varchar/string/char) can land
        // in any string-family destination — the wire / Compute paths
        // may produce a `.string`-tagged column even when the table's
        // schema column is declared as `varchar(N)` or `char(N)`.
        .varchar => |sv| try appendMaskedStringy(allocator, sv, mask, out),
        .string => |sv| try appendMaskedStringy(allocator, sv, mask, out),
        .char => |sv| try appendMaskedStringy(allocator, sv, mask, out),
        .float => |s| switch (out.data) {
            .float => |*list| try compactInto(f32, allocator, s, mask, list),
            else => unreachable,
        },
        .double => |s| switch (out.data) {
            .double => |*list| try compactInto(f64, allocator, s, mask, list),
            else => unreachable,
        },
        .date => |s| switch (out.data) {
            .date => |*list| try compactInto(i32, allocator, s, mask, list),
            else => unreachable,
        },
        .datetime => |s| switch (out.data) {
            .datetime => |*list| try compactInto(i64, allocator, s, mask, list),
            else => unreachable,
        },
        .tinyint => |s| switch (out.data) {
            .tinyint => |*list| try compactInto(i8, allocator, s, mask, list),
            else => unreachable,
        },
        .smallint => |s| switch (out.data) {
            .smallint => |*list| try compactInto(i16, allocator, s, mask, list),
            else => unreachable,
        },
        .largeint => |s| switch (out.data) {
            .largeint => |*list| try compactInto(i128, allocator, s, mask, list),
            else => unreachable,
        },
        .decimal64 => |s| switch (out.data) {
            .decimal64 => |*list| try compactInto(i64, allocator, s, mask, list),
            else => unreachable,
        },
        .decimal128 => |s| switch (out.data) {
            .decimal128 => |*list| try compactInto(i128, allocator, s, mask, list),
            else => unreachable,
        },
        .uuid => |s| switch (out.data) {
            .uuid => |*list| try compactInto(u128, allocator, s, mask, list),
            else => unreachable,
        },
    }
    if (out.nulls != null) {
        var j: usize = 0;
        for (mask, 0..) |m, src_row| {
            if (!m) continue;
            const valid = storage.column.isValidBit(view.nulls, src_row);
            try out.appendValidBit(allocator, dst_start + j, valid);
            j += 1;
        }
    }
}

/// Copy mask-selected rows from any string-family source view
/// (varchar/string/char) into any string-family destination
/// ColumnStore. Used by appendMaskedColumn to bridge the case where
/// the source's tag doesn't match the destination's declared type
/// (e.g. Compute emits a `.string` column but the table schema
/// declares it as `varchar(N)`).
fn appendMaskedStringy(allocator: Allocator, sv: anytype, mask: []const bool, out: *ColumnStore) !void {
    // Reserve once to upper bounds (every row a survivor; all source bytes) so
    // the per-survivor copy never branches on capacity or reallocs — this gather
    // is the filtered-scan materialize hot path. Bounds are one row group's
    // worth and the buffer is reused across row groups, so the over-reserve is
    // transient and cheap relative to eliminating the per-row growth checks.
    const n = mask.len;
    const src_span: usize = if (n == 0) 0 else sv.offsets[n] - sv.offsets[0];
    switch (out.data) {
        .varchar, .string, .char => |*ss| {
            try ss.ensureUnusedValueCapacity(allocator, n, src_span);
            for (mask, 0..) |m, row| {
                if (m) ss.appendValueAssumeCapacity(sv.rowBytes(row));
            }
        },
        else => unreachable,
    }
}

/// Reorder a `ColumnStore` by `perm` and produce a fresh owned store.
/// Used to materialize a sorted snapshot of the memtable for flush.
pub fn applyPermutation(
    allocator: Allocator,
    src: ColumnStore,
    perm: []const u32,
) !ColumnStore {
    const dst_data: DataStore = switch (src.data) {
        .int => |l| blk: {
            var dst: std.ArrayList(i32) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .int = dst };
        },
        .bigint => |l| blk: {
            var dst: std.ArrayList(i64) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .bigint = dst };
        },
        .boolean => |l| blk: {
            var dst: std.ArrayList(u8) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .boolean = dst };
        },
        .varchar => |s| blk: {
            var dst = try StringStore.init(allocator);
            errdefer dst.deinit(allocator);
            for (perm) |p| try dst.appendValue(allocator, s.view().rowBytes(p));
            break :blk DataStore{ .varchar = dst };
        },
        .string => |s| blk: {
            var dst = try StringStore.init(allocator);
            errdefer dst.deinit(allocator);
            for (perm) |p| try dst.appendValue(allocator, s.view().rowBytes(p));
            break :blk DataStore{ .string = dst };
        },
        .float => |l| blk: {
            var dst: std.ArrayList(f32) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .float = dst };
        },
        .double => |l| blk: {
            var dst: std.ArrayList(f64) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .double = dst };
        },
        .date => |l| blk: {
            var dst: std.ArrayList(i32) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .date = dst };
        },
        .datetime => |l| blk: {
            var dst: std.ArrayList(i64) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .datetime = dst };
        },
        .tinyint => |l| blk: {
            var dst: std.ArrayList(i8) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .tinyint = dst };
        },
        .smallint => |l| blk: {
            var dst: std.ArrayList(i16) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .smallint = dst };
        },
        .largeint => |l| blk: {
            var dst: std.ArrayList(i128) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .largeint = dst };
        },
        .char => |s| blk: {
            var dst = try StringStore.init(allocator);
            errdefer dst.deinit(allocator);
            for (perm) |p| try dst.appendValue(allocator, s.view().rowBytes(p));
            break :blk DataStore{ .char = dst };
        },
        .decimal64 => |l| blk: {
            var dst: std.ArrayList(i64) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .decimal64 = dst };
        },
        .decimal128 => |l| blk: {
            var dst: std.ArrayList(i128) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .decimal128 = dst };
        },
        .uuid => |l| blk: {
            var dst: std.ArrayList(u128) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .uuid = dst };
        },
    };

    var nulls: ?std.ArrayList(u8) = null;
    if (src.nulls) |src_bits| {
        var b: std.ArrayList(u8) = try .initCapacity(allocator, storage.column.bitmapBytes(perm.len));
        errdefer b.deinit(allocator);
        try b.appendNTimes(allocator, 0, storage.column.bitmapBytes(perm.len));
        for (perm, 0..) |src_row, dst_row| {
            if (storage.column.isValidBit(src_bits.items, src_row)) {
                const byte_idx = dst_row >> 3;
                const bit: u3 = @intCast(dst_row & 7);
                b.items[byte_idx] |= (@as(u8, 1) << bit);
            }
        }
        nulls = b;
    }

    return .{ .data = dst_data, .nulls = nulls };
}

test "appendMaskedColumn fixed-width: trailing filtered-out row never overruns" {
    const testing = std.testing;
    // The branchless compaction in `compactInto` stores `base[pos]` for EVERY
    // source row and only advances `pos` on survivors, so the final trailing
    // non-survivor writes one slot past the survivor count. Reservation must
    // include that scratch slot; otherwise this is a heap overrun (only the
    // *last* row being filtered out triggers it). Exercise that exact shape.
    const cases = .{
        .{ .src = [_]i64{ 10, 20, 30, 40, 50 }, .mask = [_]bool{ true, false, true, false, false }, .want = [_]i64{ 10, 30 } },
        .{ .src = [_]i64{ 1, 2, 3 }, .mask = [_]bool{ false, false, false }, .want = [_]i64{} }, // all out
        .{ .src = [_]i64{ 7, 8, 9 }, .mask = [_]bool{ true, true, true }, .want = [_]i64{ 7, 8, 9 } }, // none out
        .{ .src = [_]i64{ 5 }, .mask = [_]bool{false}, .want = [_]i64{} }, // single, filtered
    };
    inline for (cases) |c| {
        var src = c.src; // runtime copies — comptime-var pointers can't reach a runtime fn
        var mask = c.mask;
        const want = c.want;
        var out = try ColumnStore.init(testing.allocator, .bigint, false);
        defer out.deinit(testing.allocator);
        const view = ColumnView{ .data = .{ .bigint = &src } };
        try appendMaskedColumn(testing.allocator, view, &mask, &out);
        try testing.expectEqualSlices(i64, &want, out.data.bigint.items);
    }
}
