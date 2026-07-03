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

/// Borrowed row-range view over a column — the zero-copy slicing primitive
/// for splitting one materialized column into chunk/batch windows. `off`
/// must be a multiple of 8 so the validity bitmap slices on a byte
/// boundary. String offsets stay absolute into the full bytes buffer, so
/// the offsets window slices without rebasing.
pub fn subViewAligned(v: ColumnView, off: usize, n: usize) ColumnView {
    std.debug.assert(off % 8 == 0);
    const nulls: ?[]const u8 = if (v.nulls) |bm| bm[off / 8 ..] else null;
    return switch (v.data) {
        .int => |s| .{ .data = .{ .int = s[off..][0..n] }, .nulls = nulls },
        .bigint => |s| .{ .data = .{ .bigint = s[off..][0..n] }, .nulls = nulls },
        .boolean => |s| .{ .data = .{ .boolean = s[off..][0..n] }, .nulls = nulls },
        .tinyint => |s| .{ .data = .{ .tinyint = s[off..][0..n] }, .nulls = nulls },
        .smallint => |s| .{ .data = .{ .smallint = s[off..][0..n] }, .nulls = nulls },
        .float => |s| .{ .data = .{ .float = s[off..][0..n] }, .nulls = nulls },
        .double => |s| .{ .data = .{ .double = s[off..][0..n] }, .nulls = nulls },
        .date => |s| .{ .data = .{ .date = s[off..][0..n] }, .nulls = nulls },
        .datetime => |s| .{ .data = .{ .datetime = s[off..][0..n] }, .nulls = nulls },
        .largeint => |s| .{ .data = .{ .largeint = s[off..][0..n] }, .nulls = nulls },
        .decimal64 => |s| .{ .data = .{ .decimal64 = s[off..][0..n] }, .nulls = nulls },
        .decimal128 => |s| .{ .data = .{ .decimal128 = s[off..][0..n] }, .nulls = nulls },
        .uuid => |s| .{ .data = .{ .uuid = s[off..][0..n] }, .nulls = nulls },
        .varchar => |sv| .{ .data = .{ .varchar = .{ .offsets = sv.offsets[off..][0 .. n + 1], .bytes = sv.bytes } }, .nulls = nulls },
        .string => |sv| .{ .data = .{ .string = .{ .offsets = sv.offsets[off..][0 .. n + 1], .bytes = sv.bytes } }, .nulls = nulls },
        .char => |sv| .{ .data = .{ .char = .{ .offsets = sv.offsets[off..][0 .. n + 1], .bytes = sv.bytes } }, .nulls = nulls },
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
        // String family: dispatch on whichever string store is active — a
        // union / stage boundary may reconcile varchar/char data under a
        // `string` schema tag while streaming the arm's views uncast (see
        // set_union.zig sameRepr), so exact-tag matching would be wrong.
        .varchar, .string, .char => |sv| {
            for (0..sv.rowCount()) |i| try appendStrValue(allocator, out, sv.rowBytes(i));
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

/// Append rows `[start, end)` of `view` to `out` in bulk — one `appendSlice`
/// (memcpy) per fixed-width column and a single byte-range memcpy + offset
/// rebuild per string column, instead of the per-cell `appendOneRow` dispatch.
/// This is the chunked-materialization hot path; a batch that straddles a
/// chunk boundary calls it once per chunk-resident sub-range. Types must match.
pub fn appendColumnRange(
    allocator: Allocator,
    view: ColumnView,
    start: usize,
    end: usize,
    out: *ColumnStore,
) !void {
    if (end <= start) return;
    const n = end - start;
    const dst_start = out.data.rowCount();
    switch (view.data) {
        .int => |s| switch (out.data) {
            .int => |*list| try list.appendSlice(allocator, s[start..end]),
            else => unreachable,
        },
        .bigint => |s| switch (out.data) {
            .bigint => |*list| try list.appendSlice(allocator, s[start..end]),
            else => unreachable,
        },
        .boolean => |s| switch (out.data) {
            .boolean => |*list| try list.appendSlice(allocator, s[start..end]),
            else => unreachable,
        },
        .float => |s| switch (out.data) {
            .float => |*list| try list.appendSlice(allocator, s[start..end]),
            else => unreachable,
        },
        .double => |s| switch (out.data) {
            .double => |*list| try list.appendSlice(allocator, s[start..end]),
            else => unreachable,
        },
        .date => |s| switch (out.data) {
            .date => |*list| try list.appendSlice(allocator, s[start..end]),
            else => unreachable,
        },
        .datetime => |s| switch (out.data) {
            .datetime => |*list| try list.appendSlice(allocator, s[start..end]),
            else => unreachable,
        },
        .tinyint => |s| switch (out.data) {
            .tinyint => |*list| try list.appendSlice(allocator, s[start..end]),
            else => unreachable,
        },
        .smallint => |s| switch (out.data) {
            .smallint => |*list| try list.appendSlice(allocator, s[start..end]),
            else => unreachable,
        },
        .largeint => |s| switch (out.data) {
            .largeint => |*list| try list.appendSlice(allocator, s[start..end]),
            else => unreachable,
        },
        .decimal64 => |s| switch (out.data) {
            .decimal64 => |*list| try list.appendSlice(allocator, s[start..end]),
            else => unreachable,
        },
        .decimal128 => |s| switch (out.data) {
            .decimal128 => |*list| try list.appendSlice(allocator, s[start..end]),
            else => unreachable,
        },
        .uuid => |s| switch (out.data) {
            .uuid => |*list| try list.appendSlice(allocator, s[start..end]),
            else => unreachable,
        },
        .varchar => |sv| try appendStrRange(allocator, out, sv, start, end),
        .string => |sv| try appendStrRange(allocator, out, sv, start, end),
        .char => |sv| try appendStrRange(allocator, out, sv, start, end),
    }
    if (out.nulls != null) {
        try out.appendValidityRangeFrom(allocator, dst_start, view.nulls, start, n);
    }
}

/// Two-phase, thread-parallel column append.
///
/// `prepareAppend` (one caller per column) extends `out` to its final size
/// for `view`'s `n` rows — data slots undefined, validity bytes zeroed —
/// and returns the write bases. `writeAppendSlice` then fills DISJOINT row
/// ranges positionally with no allocation, so concurrent writers can split
/// one wide batch across threads. Writers must tile on absolute 8-row
/// validity-byte boundaries (relative to `base_row`) so no two of them
/// share a bitmap byte; the final tile owns the tail byte exclusively.
pub const PreparedAppend = struct {
    /// Destination row where this batch begins.
    base_row: usize,
    /// Destination byte offset for string columns; 0 otherwise.
    byte_base: usize = 0,
    /// False when the column can't take positional writes (a wide / about-
    /// to-go-wide string store) — caller falls back to appendColumnRange.
    positional: bool = true,
};

pub fn prepareAppend(allocator: Allocator, view: ColumnView, n: usize, out: *ColumnStore) !PreparedAppend {
    const base_row = out.data.rowCount();
    var prep = PreparedAppend{ .base_row = base_row };
    switch (view.data) {
        .varchar, .string, .char => |sv| {
            const ss = switch (out.data) {
                .varchar, .string, .char => |*s| s,
                else => unreachable,
            };
            const total: usize = sv.offsets[n] - sv.offsets[0];
            if (ss.isWide() or ss.bytes.items.len + total > std.math.maxInt(u32)) {
                prep.positional = false;
                return prep;
            }
            prep.byte_base = ss.bytes.items.len;
            try ss.bytes.resize(allocator, prep.byte_base + total);
            try ss.offsets.resize(allocator, base_row + 1 + n);
        },
        else => {
            switch (out.data) {
                .varchar, .string, .char => unreachable,
                inline else => |*list| try list.resize(allocator, base_row + n),
            }
        },
    }
    if (out.nulls) |*nb| {
        const need = (base_row + n + 7) / 8;
        if (nb.items.len < need) try nb.appendNTimes(allocator, 0, need - nb.items.len);
    }
    return prep;
}

/// Fill rows [lo, hi) of the prepared batch. Positional stores only —
/// no allocation, no length mutation, safe concurrently for disjoint,
/// byte-aligned tiles.
pub fn writeAppendSlice(view: ColumnView, lo: usize, hi: usize, out: *ColumnStore, prep: PreparedAppend) void {
    if (hi <= lo) return;
    switch (view.data) {
        .varchar, .string, .char => |sv| {
            const ss = switch (out.data) {
                .varchar, .string, .char => |*s| s,
                else => unreachable,
            };
            const src0 = sv.offsets[0];
            const blo = sv.offsets[lo] - src0;
            const bhi = sv.offsets[hi] - src0;
            @memcpy(ss.bytes.items[prep.byte_base + blo ..][0 .. bhi - blo], sv.bytes[sv.offsets[lo]..sv.offsets[hi]]);
            var r = lo;
            while (r < hi) : (r += 1) {
                ss.offsets.items[prep.base_row + 1 + r] = @intCast(prep.byte_base + (sv.offsets[r + 1] - src0));
            }
        },
        inline .int, .date => |s| writeFixedSlice(i32, s, lo, hi, out, prep),
        inline .bigint, .datetime, .decimal64 => |s| writeFixedSlice(i64, s, lo, hi, out, prep),
        .boolean => |s| writeFixedSlice(u8, s, lo, hi, out, prep),
        .float => |s| writeFixedSlice(f32, s, lo, hi, out, prep),
        .double => |s| writeFixedSlice(f64, s, lo, hi, out, prep),
        .tinyint => |s| writeFixedSlice(i8, s, lo, hi, out, prep),
        .smallint => |s| writeFixedSlice(i16, s, lo, hi, out, prep),
        inline .largeint, .decimal128 => |s| writeFixedSlice(i128, s, lo, hi, out, prep),
        .uuid => |s| writeFixedSlice(u128, s, lo, hi, out, prep),
    }
    if (out.nulls) |nb| {
        const dst = nb.items;
        if (view.nulls) |src_nulls| {
            var r = lo;
            while (r < hi) : (r += 1) {
                if ((src_nulls[r >> 3] >> @intCast(r & 7)) & 1 != 0) {
                    const bit = prep.base_row + r;
                    dst[bit >> 3] |= @as(u8, 1) << @intCast(bit & 7);
                }
            }
        } else {
            store.setBitRangeTrue(dst, prep.base_row + lo, hi - lo);
        }
    }
}

fn writeFixedSlice(comptime T: type, s: []const T, lo: usize, hi: usize, out: *ColumnStore, prep: PreparedAppend) void {
    switch (out.data) {
        .varchar, .string, .char => unreachable,
        inline else => |*list| {
            if (comptime std.meta.Child(@TypeOf(list.items)) == T) {
                @memcpy(list.items[prep.base_row + lo ..][0 .. hi - lo], s[lo..hi]);
            } else unreachable;
        },
    }
}

inline fn appendStrRange(allocator: Allocator, out: *ColumnStore, sv: storage.StringView, start: usize, end: usize) !void {
    switch (out.data) {
        .varchar => |*ss| try ss.appendRange(allocator, sv, start, end),
        .string => |*ss| try ss.appendRange(allocator, sv, start, end),
        .char => |*ss| try ss.appendRange(allocator, sv, start, end),
        else => unreachable,
    }
}

/// `varchar` / `string` / `char` share one physical `StringStore`; only their
/// SQL type tag differs. A staged column can arrive viewed as one tag while its
/// destination store was built from a schema that picked another (e.g. a window
/// stage whose output schema normalizes to `string` over `varchar` data), so a
/// string-bytes copy must dispatch on whichever string-family store is active
/// rather than demanding an exact tag match.
inline fn appendStrValue(allocator: Allocator, out: *ColumnStore, bytes: []const u8) !void {
    switch (out.data) {
        .varchar => |*ss| try ss.appendValue(allocator, bytes),
        .string => |*ss| try ss.appendValue(allocator, bytes),
        .char => |*ss| try ss.appendValue(allocator, bytes),
        else => unreachable,
    }
}

/// Bulk string gather: size the destination once (one pass to sum byte
/// lengths), then append every value capacity-assumed — no per-value
/// dispatch, capacity check, or realloc. Falls back to per-value appends
/// only when the column is (or would go) wide.
fn gatherStrByIndices(allocator: Allocator, sv: storage.StringView, indices: []const u32, out: *ColumnStore) !void {
    const ss = switch (out.data) {
        .varchar, .string, .char => |*s| s,
        else => unreachable,
    };
    var total: usize = 0;
    for (indices) |idx| total += sv.rowBytes(idx).len;
    if (ss.isWide() or ss.bytes.items.len + total > std.math.maxInt(u32)) {
        for (indices) |idx| try ss.appendValue(allocator, sv.rowBytes(idx));
        return;
    }
    try ss.ensureUnusedValueCapacity(allocator, indices.len, total);
    for (indices) |idx| ss.appendValueAssumeCapacity(sv.rowBytes(idx));
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
        .varchar => |sv| try appendStrValue(allocator, out, sv.rowBytes(row)),
        .string => |sv| try appendStrValue(allocator, out, sv.rowBytes(row)),
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
        .char => |sv| try appendStrValue(allocator, out, sv.rowBytes(row)),
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
        .varchar => |sv| try gatherStrByIndices(allocator, sv, indices, out),
        .string => |sv| try gatherStrByIndices(allocator, sv, indices, out),
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
        .char => |sv| try gatherStrByIndices(allocator, sv, indices, out),
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
    if (out.nulls) |*nb| {
        // Bulk validity gather: extend the bitmap once (new bytes arrive
        // zeroed and bits at/above the old row count are 0 by invariant),
        // then set only the valid bits — no per-row grow checks or clears.
        const need = (dst_start + indices.len + 7) / 8;
        if (nb.items.len < need) try nb.appendNTimes(allocator, 0, need - nb.items.len);
        const dst = nb.items;
        if (view.nulls) |src_nulls| {
            for (indices, 0..) |src_idx, j| {
                if ((src_nulls[src_idx >> 3] >> @intCast(src_idx & 7)) & 1 != 0) {
                    const bit = dst_start + j;
                    dst[bit >> 3] |= @as(u8, 1) << @intCast(bit & 7);
                }
            }
        } else {
            store.setBitRangeTrue(dst, dst_start, indices.len);
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

test "appendColumnRange matches per-row appendOneRow (fixed-width + strings + nulls + straddle)" {
    const testing = std.testing;
    // 6 source rows; nulls bitmap: rows 1 and 4 are NULL (bit 0 = valid).
    var ints = [_]i64{ 10, 20, 30, 40, 50, 60 };
    const offsets = [_]u32{ 0, 2, 2, 5, 8, 8, 11 }; // "aa","", "bbb","ccc","", "ddd"
    const bytes = "aabbbccc" ++ "ddd";
    var nulls = [_]u8{0b0010_1101}; // valid rows: 0,2,3,5 ; null rows: 1,4
    const int_view = ColumnView{ .data = .{ .bigint = &ints }, .nulls = &nulls };
    const str_view = ColumnView{ .data = .{ .varchar = .{ .offsets = &offsets, .bytes = bytes } }, .nulls = &nulls };

    // Reference: per-row appendOneRow over [start, end). Test ranges that start
    // at 0 (aligned fast path) and at 2 (straddle / src offset).
    const ranges = [_][2]usize{ .{ 0, 6 }, .{ 2, 5 }, .{ 1, 4 } };
    for (ranges) |rg| {
        const start = rg[0];
        const end = rg[1];
        inline for (.{ "bigint", "varchar" }) |kind| {
            const view = if (comptime std.mem.eql(u8, kind, "bigint")) int_view else str_view;
            const ty: types.Type = if (comptime std.mem.eql(u8, kind, "bigint")) .bigint else .{ .varchar = 64 };
            var bulk = try ColumnStore.init(testing.allocator, ty, true);
            defer bulk.deinit(testing.allocator);
            var perrow = try ColumnStore.init(testing.allocator, ty, true);
            defer perrow.deinit(testing.allocator);
            try appendColumnRange(testing.allocator, view, start, end, &bulk);
            var r = start;
            while (r < end) : (r += 1) try appendOneRow(testing.allocator, view, r, &perrow);
            try testing.expectEqual(perrow.rowCount(), bulk.rowCount());
            var i: usize = 0;
            while (i < perrow.rowCount()) : (i += 1) {
                try testing.expectEqual(rowIsValid(perrow, @intCast(i)), rowIsValid(bulk, @intCast(i)));
                if (comptime std.mem.eql(u8, kind, "bigint")) {
                    try testing.expectEqual(perrow.data.bigint.items[i], bulk.data.bigint.items[i]);
                } else {
                    try testing.expectEqualStrings(perrow.data.varchar.view().rowBytes(i), bulk.data.varchar.view().rowBytes(i));
                }
            }
        }
    }
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
