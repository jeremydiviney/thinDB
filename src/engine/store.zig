//! Memtable backing storage: per-column buffers (StringStore, DataStore,
//! ColumnStore) and the validity-bit machinery. Decoupled from Memtable
//! itself so transform helpers and the memtable can evolve independently.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const TypeTag = types.TypeTag;

// column.zig directly (not storage.zig): keeps this file std-only in its
// transitive imports so it can compile into function DLLs without the
// storage subsystem (and its C compression libs).
const storage_column = @import("../storage/column.zig");
const ColumnView = storage_column.ColumnView;
const ValueView = storage_column.ValueView;
const StringView = storage_column.StringView;

/// Borrowed view over a string column whose total bytes exceed 4 GiB, so its
/// offsets need u64. Only big in-memory accumulations (the Sort operator over a
/// huge result) ever produce one — see `StringStore.wide_offsets`. Mirrors
/// `storage.StringView`'s read interface (`rowBytes`/`rowCount`) so the radix
/// sort and comparators are generic over both.
pub const WideStringView = struct {
    offsets: []const u64,
    bytes: []const u8,

    pub fn rowCount(self: WideStringView) usize {
        return self.offsets.len - 1;
    }
    pub fn rowBytes(self: WideStringView, row: usize) []const u8 {
        return self.bytes[@intCast(self.offsets[row])..@intCast(self.offsets[row + 1])];
    }
};

/// Variable-width string column buffer. `offsets` is invariant length
/// `row_count + 1` with `offsets[0] == 0` and `offsets[row_count]` = total bytes.
pub const StringStore = struct {
    offsets: std.ArrayList(u32),
    bytes: std.ArrayList(u8),
    /// u64 offset sidecar, lazily allocated the moment `bytes` would cross the
    /// u32 (4 GiB) limit. While null the column is "narrow" (the common case,
    /// zero overhead). Once set, the u32 `offsets` is frozen/stale and all reads
    /// go through `wide_offsets` — see `rowBytesWide`/`wideView`/`isWide`. Only
    /// the Sort's full-result accumulation realistically gets here; a memtable
    /// flushes long before 4 GiB.
    wide_offsets: ?std.ArrayList(u64) = null,

    pub fn init(allocator: Allocator) Allocator.Error!StringStore {
        return initCapacity(allocator, 0, 0);
    }

    /// Initialize with pre-reserved capacity. Required for snapshot isolation:
    /// once a reader pins a slice into `offsets`/`bytes`, an `append` that
    /// triggers realloc would invalidate the reader's pointer. By reserving
    /// enough capacity up-front, appends never realloc.
    pub fn initCapacity(allocator: Allocator, rows_cap: usize, bytes_cap: usize) Allocator.Error!StringStore {
        var offsets: std.ArrayList(u32) = .empty;
        errdefer offsets.deinit(allocator);
        try offsets.ensureTotalCapacity(allocator, rows_cap + 1);
        try offsets.append(allocator, 0);
        var bytes: std.ArrayList(u8) = .empty;
        errdefer bytes.deinit(allocator);
        try bytes.ensureTotalCapacity(allocator, bytes_cap);
        return .{ .offsets = offsets, .bytes = bytes };
    }

    pub fn deinit(self: *StringStore, allocator: Allocator) void {
        self.offsets.deinit(allocator);
        self.bytes.deinit(allocator);
        if (self.wide_offsets) |*wo| wo.deinit(allocator);
        self.* = undefined;
    }

    pub fn appendValue(self: *StringStore, allocator: Allocator, slice: []const u8) !void {
        try self.bytes.appendSlice(allocator, slice);
        const total = self.bytes.items.len;
        if (self.wide_offsets) |*wo| {
            try wo.append(allocator, total);
            return;
        }
        // First value that pushes the column past 4 GiB: migrate the u32 offsets
        // into a u64 sidecar and append there from now on (instead of
        // @intCast-panicking). Rare; the narrow path above stays the norm.
        if (total > std.math.maxInt(u32)) {
            var wo: std.ArrayList(u64) = .empty;
            errdefer wo.deinit(allocator);
            try wo.ensureTotalCapacity(allocator, self.offsets.items.len + 1);
            for (self.offsets.items) |o| wo.appendAssumeCapacity(o);
            wo.appendAssumeCapacity(total);
            self.wide_offsets = wo;
            return;
        }
        try self.offsets.append(allocator, @intCast(total));
    }

    pub fn isWide(self: StringStore) bool {
        return self.wide_offsets != null;
    }

    /// Read row `row`'s bytes, transparently honoring the u64 sidecar. Use this
    /// (not `view().rowBytes`) anywhere a column might have crossed 4 GiB.
    pub fn rowBytesWide(self: StringStore, row: usize) []const u8 {
        if (self.wide_offsets) |wo| {
            return self.bytes.items[@intCast(wo.items[row])..@intCast(wo.items[row + 1])];
        }
        return self.bytes.items[self.offsets.items[row]..self.offsets.items[row + 1]];
    }

    /// Borrowed wide view; only valid when `isWide()`.
    pub fn wideView(self: StringStore) WideStringView {
        return .{ .offsets = self.wide_offsets.?.items, .bytes = self.bytes.items };
    }

    /// Reserve room for `rows` more values totaling `bytes_len` more bytes, so a
    /// run of `appendValueAssumeCapacity` never reallocs or branches on capacity
    /// — the bulk-materialize fast path (`appendMaskedStringy`).
    pub fn ensureUnusedValueCapacity(self: *StringStore, allocator: Allocator, rows: usize, bytes_len: usize) Allocator.Error!void {
        try self.offsets.ensureUnusedCapacity(allocator, rows);
        try self.bytes.ensureUnusedCapacity(allocator, bytes_len);
    }

    pub fn appendValueAssumeCapacity(self: *StringStore, slice: []const u8) void {
        self.bytes.appendSliceAssumeCapacity(slice);
        self.offsets.appendAssumeCapacity(@intCast(self.bytes.items.len));
    }

    /// Bulk-append rows `[start, end)` of `sv`: one `appendSlice` (memcpy) for
    /// the whole contiguous byte segment, then a rebuilt offset per row (cheap
    /// arithmetic, no per-row memcpy). The per-value `appendValue` path
    /// dominated chunked materialization. Falls back to per-row only when the
    /// append would cross the 4 GiB u32-offset ceiling (then `appendValue`
    /// handles the wide migration).
    pub fn appendRange(self: *StringStore, allocator: Allocator, sv: StringView, start: usize, end: usize) !void {
        const n = end - start;
        if (n == 0) return;
        const byte_start = sv.offsets[start];
        const byte_end = sv.offsets[end];
        const seg_len: usize = byte_end - byte_start;
        if (self.wide_offsets != null or self.bytes.items.len + seg_len > std.math.maxInt(u32)) {
            try self.ensureUnusedValueCapacity(allocator, n, seg_len);
            for (start..end) |i| try self.appendValue(allocator, sv.bytes[sv.offsets[i]..sv.offsets[i + 1]]);
            return;
        }
        const base: u32 = @intCast(self.bytes.items.len);
        try self.bytes.appendSlice(allocator, sv.bytes[byte_start..byte_end]);
        try self.offsets.ensureUnusedCapacity(allocator, n);
        var i = start + 1;
        while (i <= end) : (i += 1) {
            self.offsets.appendAssumeCapacity(base + (sv.offsets[i] - byte_start));
        }
    }

    /// Append `n` empty values: n more offsets at the current total byte
    /// length, no byte-buffer growth. Bulk equivalent of `appendValue("")`
    /// × n — the per-value path dominated wide all-NULL emits.
    pub fn appendEmptyValues(self: *StringStore, allocator: Allocator, n: usize) !void {
        if (self.wide_offsets) |*wo| {
            try wo.appendNTimes(allocator, self.bytes.items.len, n);
            return;
        }
        try self.offsets.appendNTimes(allocator, @intCast(self.bytes.items.len), n);
    }

    pub fn rowCount(self: StringStore) usize {
        if (self.wide_offsets) |wo| return wo.items.len - 1;
        return self.offsets.items.len - 1;
    }

    pub fn view(self: StringStore) StringView {
        // A wide column can't be represented by u32 offsets; readers that might
        // see >4 GiB columns (the Sort) must use `rowBytesWide`/`wideView`.
        std.debug.assert(self.wide_offsets == null);
        return .{ .offsets = self.offsets.items, .bytes = self.bytes.items };
    }

    pub fn clear(self: *StringStore) void {
        self.bytes.clearRetainingCapacity();
        if (self.wide_offsets) |*wo| {
            wo.clearRetainingCapacity();
            wo.appendAssumeCapacity(0);
        } else {
            self.offsets.clearRetainingCapacity();
            self.offsets.appendAssumeCapacity(0);
        }
    }
};

pub const ColumnStore = struct {
    data: DataStore,
    /// Validity bitmap (1 = valid, 0 = null). Present iff the column is
    /// nullable. Grown alongside the data so `data.rowCount()` rows always
    /// have a bit available.
    nulls: ?std.ArrayList(u8) = null,

    pub fn init(allocator: Allocator, t: Type, nullable: bool) Allocator.Error!ColumnStore {
        return initCapacity(allocator, t, nullable, 0, 0);
    }

    /// An empty store of `v`'s physical type, for rows copied out of a
    /// view whose declared type isn't at hand.
    pub fn initLike(allocator: Allocator, v: ColumnView, nullable: bool) Allocator.Error!ColumnStore {
        return .{
            .data = try DataStore.initTag(allocator, std.meta.activeTag(v.data)),
            .nulls = if (nullable) .empty else null,
        };
    }

    pub fn initCapacity(
        allocator: Allocator,
        t: Type,
        nullable: bool,
        rows_cap: usize,
        bytes_cap: usize,
    ) Allocator.Error!ColumnStore {
        var nulls_opt: ?std.ArrayList(u8) = if (nullable) blk: {
            var nb: std.ArrayList(u8) = .empty;
            errdefer nb.deinit(allocator);
            try nb.ensureTotalCapacity(allocator, (rows_cap + 7) >> 3);
            break :blk nb;
        } else null;
        errdefer if (nulls_opt) |*n| n.deinit(allocator);
        return .{
            .data = try DataStore.initCapacity(allocator, t, rows_cap, bytes_cap),
            .nulls = nulls_opt,
        };
    }

    /// Capacity for `rows_total` rows (and `bytes_total` string bytes) in
    /// one allocation — for a fill whose total is known before it starts.
    pub fn reserveTotal(self: *ColumnStore, allocator: Allocator, rows_total: usize, bytes_total: usize) Allocator.Error!void {
        if (self.nulls) |*nb| try nb.ensureTotalCapacityPrecise(allocator, (rows_total + 7) >> 3);
        switch (self.data) {
            .varchar, .string, .char, .json => |*ss| {
                try ss.offsets.ensureTotalCapacityPrecise(allocator, rows_total + 1);
                try ss.bytes.ensureTotalCapacityPrecise(allocator, bytes_total);
            },
            inline else => |*list| try list.ensureTotalCapacityPrecise(allocator, rows_total),
        }
    }

    pub fn deinit(self: *ColumnStore, allocator: Allocator) void {
        self.data.deinit(allocator);
        if (self.nulls) |*n| n.deinit(allocator);
        self.* = undefined;
    }

    pub fn rowCount(self: ColumnStore) usize {
        return self.data.rowCount();
    }

    pub fn view(self: ColumnStore) ColumnView {
        return .{
            .data = self.data.view(),
            .nulls = if (self.nulls) |n| n.items else null,
        };
    }

    pub fn clear(self: *ColumnStore) void {
        self.data.clear();
        if (self.nulls) |*n| n.clearRetainingCapacity();
    }

    /// Append a single validity bit for the row at index `row` (= current row
    /// count BEFORE this call's data append). Grows the bitmap byte by byte
    /// as needed. No-op on non-nullable columns.
    pub fn appendValidBit(self: *ColumnStore, allocator: Allocator, row: usize, valid: bool) !void {
        const nulls = self.nullsPtr() orelse return;
        const byte_idx = row >> 3;
        if (byte_idx >= nulls.items.len) {
            const need = byte_idx + 1 - nulls.items.len;
            try nulls.appendNTimes(allocator, 0, need);
        }
        const bit: u3 = @intCast(row & 7);
        if (valid) {
            nulls.items[byte_idx] |= (@as(u8, 1) << bit);
        } else {
            nulls.items[byte_idx] &= ~(@as(u8, 1) << bit);
        }
    }

    /// Bulk-append `n` validity bits starting at row `dst_start`, copied
    /// from a packed source bitmap (bit i = source row i) or all-valid when
    /// `src_nulls` is null. Equivalent to `n` `appendValidBit` calls — the
    /// per-row loop dominated large accumulations (a window drain appends
    /// tens of millions of bits). Relies on the bitmap invariant that bits
    /// at or above the current row count are 0 (append-only growth zeroes
    /// new bytes and nothing sets bits past the end), so OR-merging shifted
    /// source bytes is exact: source 0-bits (NULLs) stay 0.
    pub fn appendValidityRange(
        self: *ColumnStore,
        allocator: Allocator,
        dst_start: usize,
        src_nulls: ?[]const u8,
        n: usize,
    ) !void {
        const nb = self.nullsPtr() orelse return;
        if (n == 0) return;
        const need = (dst_start + n + 7) / 8;
        if (nb.items.len < need) try nb.appendNTimes(allocator, 0, need - nb.items.len);
        const dst = nb.items;
        const src = src_nulls orelse {
            setBitRangeTrue(dst, dst_start, n);
            return;
        };
        const shift: u3 = @intCast(dst_start & 7);
        const db = dst_start >> 3;
        const src_bytes = (n + 7) / 8;
        var k: usize = 0;
        while (k < src_bytes) : (k += 1) {
            var b = src[k];
            if (k == src_bytes - 1) {
                const keep: u3 = @intCast(n & 7);
                if (keep != 0) b &= (@as(u8, 1) << keep) - 1;
            }
            dst[db + k] |= b << shift;
            if (shift != 0 and db + k + 1 < dst.len) {
                dst[db + k + 1] |= b >> @intCast(8 - @as(u4, shift));
            }
        }
    }

    /// Like `appendValidityRange`, but reads the source bits starting at
    /// `src_start` (the source row offset) rather than bit 0 — needed when a
    /// batch straddles a chunk boundary and only its tail rows append here.
    /// Delegates to the aligned fast path when `src_start` is byte-aligned at
    /// 0; otherwise copies per-bit (validity is one bit/row — cheap next to the
    /// data memcpy this rides alongside).
    pub fn appendValidityRangeFrom(
        self: *ColumnStore,
        allocator: Allocator,
        dst_start: usize,
        src_nulls: ?[]const u8,
        src_start: usize,
        n: usize,
    ) !void {
        if (src_start == 0) return self.appendValidityRange(allocator, dst_start, src_nulls, n);
        const nb = self.nullsPtr() orelse return;
        if (n == 0) return;
        const need = (dst_start + n + 7) / 8;
        if (nb.items.len < need) try nb.appendNTimes(allocator, 0, need - nb.items.len);
        const dst = nb.items;
        const src = src_nulls orelse {
            setBitRangeTrue(dst, dst_start, n);
            return;
        };
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sbit = src_start + i;
            if ((src[sbit >> 3] >> @intCast(sbit & 7)) & 1 != 0) {
                const dbit = dst_start + i;
                dst[dbit >> 3] |= @as(u8, 1) << @intCast(dbit & 7);
            }
        }
    }

    /// Bulk-append the validity bits of the `n` rows `mask` selects from a
    /// packed source bitmap (all-valid when `src_nulls` is null), starting at
    /// row `dst_start`. Equivalent to one `appendValidBit` per selected row,
    /// which dominated compacting a nullable column through a filter. Same
    /// invariant as `appendValidityRange`: bits at or above the row count are
    /// 0, so only the valid bits need setting.
    pub fn appendMaskedValidity(
        self: *ColumnStore,
        allocator: Allocator,
        dst_start: usize,
        src_nulls: ?[]const u8,
        mask: []const bool,
        n: usize,
    ) !void {
        const nb = self.nullsPtr() orelse return;
        if (n == 0) return;
        const src = src_nulls orelse return self.appendValidityRange(allocator, dst_start, null, n);
        const need = (dst_start + n + 7) / 8;
        if (nb.items.len < need) try nb.appendNTimes(allocator, 0, need - nb.items.len);
        const dst = nb.items;
        var j = dst_start;
        var row: usize = 0;
        // The selected rows of an all-valid 64-row source word are one run of
        // valid bits, so only words that hold a NULL go row by row.
        while (row + 64 <= mask.len) : (row += 64) {
            const word = std.mem.readInt(u64, src[row / 8 ..][0..8], .little);
            if (word == std.math.maxInt(u64)) {
                const selected: @Vector(64, u8) = std.mem.sliceAsBytes(mask[row..][0..64])[0..64].*;
                const count: usize = @reduce(.Add, @as(@Vector(64, u16), selected));
                setBitRangeTrue(dst, j, count);
                j += count;
            } else {
                j = gatherMaskedBits(dst, j, src, mask[row..][0..64], row);
            }
        }
        _ = gatherMaskedBits(dst, j, src, mask[row..], row);
    }

    /// Append the validity bits of the rows `mask` selects (source rows
    /// `first_row..`) at bit `j`; returns the next bit. Branch-free: an
    /// unselected row ORs a 0 bit, and past the last survivor `j` stops
    /// advancing, so the clamp keeps its byte index in range.
    fn gatherMaskedBits(dst: []u8, j_start: usize, src: []const u8, mask: []const bool, first_row: usize) usize {
        const last = dst.len - 1;
        var j = j_start;
        for (mask, first_row..) |m, row| {
            const bit = @intFromBool(m) & ((src[row >> 3] >> @intCast(row & 7)) & 1);
            dst[@min(j >> 3, last)] |= bit << @intCast(j & 7);
            j += @intFromBool(m);
        }
        return j;
    }

    /// Bulk-append the validity bits of source rows `rows` (all-valid when
    /// `src_nulls` is null), starting at row `dst_start`: the index-gather
    /// sibling of `appendMaskedValidity`, under the same invariant.
    pub fn appendGatheredValidity(
        self: *ColumnStore,
        allocator: Allocator,
        dst_start: usize,
        src_nulls: ?[]const u8,
        rows: []const u32,
    ) !void {
        const nb = self.nullsPtr() orelse return;
        if (rows.len == 0) return;
        const src = src_nulls orelse return self.appendValidityRange(allocator, dst_start, null, rows.len);
        const need = (dst_start + rows.len + 7) / 8;
        if (nb.items.len < need) try nb.appendNTimes(allocator, 0, need - nb.items.len);
        const dst = nb.items;
        for (rows, dst_start..) |row, j| {
            const bit = (src[row >> 3] >> @intCast(row & 7)) & 1;
            dst[j >> 3] |= bit << @intCast(j & 7);
        }
    }

    /// Bulk-append `n` NULL rows: placeholder data slots + n invalid (0)
    /// validity bits. The bitmap only needs to grow to cover the new rows —
    /// fresh bytes arrive zeroed and the append-only invariant keeps bits
    /// at/above the previous row count 0, so the new rows read as NULL
    /// without touching any bit.
    pub fn appendNulls(self: *ColumnStore, allocator: Allocator, n: usize) !void {
        if (n == 0) return;
        try self.data.appendNullPlaceholders(allocator, n);
        if (self.nullsPtr()) |nb| {
            const need = (self.data.rowCount() + 7) / 8;
            if (nb.items.len < need) try nb.appendNTimes(allocator, 0, need - nb.items.len);
        }
    }

    fn nullsPtr(self: *ColumnStore) ?*std.ArrayList(u8) {
        if (self.nulls) |_| return &self.nulls.?;
        return null;
    }
};

/// Append rows [start,end) of `v` onto `dst`, validity included, as slice
/// copies rather than per-value appends.
pub fn appendViewRange(alloc: Allocator, dst: *ColumnStore, v: ColumnView, start: usize, end: usize) !void {
    switch (v.data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint, .boolean, .uuid, .float, .double, .date, .datetime, .decimal64, .decimal128 => |s, tag| {
            try @field(dst.data, @tagName(tag)).appendSlice(alloc, s[start..end]);
        },
        .varchar, .string, .char, .json => |sv| switch (dst.data) {
            .varchar, .string, .char, .json => |*d| try d.appendRange(alloc, sv, start, end),
            else => unreachable,
        },
    }
    if (dst.nulls != null) {
        const base = dst.rowCount() - (end - start);
        try dst.appendValidityRangeFrom(alloc, base, v.nulls, start, end - start);
    }
}

pub fn setBitRangeTrue(bytes: []u8, start: usize, n: usize) void {
    var i = start;
    const end = start + n;
    while (i < end and (i & 7) != 0) : (i += 1) bytes[i >> 3] |= @as(u8, 1) << @intCast(i & 7);
    while (i + 8 <= end) : (i += 8) bytes[i >> 3] = 0xFF;
    while (i < end) : (i += 1) bytes[i >> 3] |= @as(u8, 1) << @intCast(i & 7);
}

test "appendValidityRange matches per-bit appends across alignments" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0xfeedface);
    const rand = prng.random();
    inline for (.{ 0, 1, 3, 7, 8, 13 }) |dst_start| {
        inline for (.{ 1, 5, 8, 9, 64, 200 }) |n| {
            var src_bits: [32]u8 = undefined;
            rand.bytes(&src_bits);

            var bulk = try ColumnStore.init(allocator, .{ .bigint = {} }, true);
            defer bulk.deinit(allocator);
            var perbit = try ColumnStore.init(allocator, .{ .bigint = {} }, true);
            defer perbit.deinit(allocator);
            // Seed dst_start leading bits identically via the per-bit path.
            for (0..dst_start) |i| {
                const v = rand.boolean();
                try bulk.appendValidBit(allocator, i, v);
                try perbit.appendValidBit(allocator, i, v);
            }

            try bulk.appendValidityRange(allocator, dst_start, &src_bits, n);
            for (0..n) |i| {
                const v = src_bits[i >> 3] & (@as(u8, 1) << @intCast(i & 7)) != 0;
                try perbit.appendValidBit(allocator, dst_start + i, v);
            }
            try std.testing.expectEqualSlices(u8, perbit.nulls.?.items, bulk.nulls.?.items);

            // All-valid source (null bitmap).
            var bulk2 = try ColumnStore.init(allocator, .{ .bigint = {} }, true);
            defer bulk2.deinit(allocator);
            try bulk2.appendValidityRange(allocator, dst_start, null, n);
            for (0..dst_start + n) |i| {
                const expected = i >= dst_start;
                const got = bulk2.nulls.?.items[i >> 3] & (@as(u8, 1) << @intCast(i & 7)) != 0;
                try std.testing.expectEqual(expected, got);
            }
        }
    }
}

test "appendMaskedValidity and appendGatheredValidity match per-bit appends" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x5eed1e55);
    const rand = prng.random();
    inline for (.{ 0, 1, 3, 7, 8, 13 }) |dst_start| {
        inline for (.{ 1, 5, 8, 9, 64, 200 }) |len| {
            var src_bits: [32]u8 = undefined;
            rand.bytes(&src_bits);
            // All-valid 64-row words take the run path; the rest go row by row.
            @memset(src_bits[0..8], 0xFF);
            @memset(src_bits[16..24], 0xFF);
            var mask: [len]bool = undefined;
            for (&mask) |*m| m.* = rand.boolean();
            // End on a selected row half the time, so the last survivor lands
            // on the final bitmap byte.
            mask[len - 1] = rand.boolean();
            var rows_buf: [len]u32 = undefined;
            var n: usize = 0;
            for (mask, 0..) |m, row| if (m) {
                rows_buf[n] = @intCast(row);
                n += 1;
            };
            const rows = rows_buf[0..n];

            inline for (.{ true, false }) |has_src| {
                const src: ?[]const u8 = if (has_src) &src_bits else null;
                var perbit = try ColumnStore.init(allocator, .{ .bigint = {} }, true);
                defer perbit.deinit(allocator);
                var masked = try ColumnStore.init(allocator, .{ .bigint = {} }, true);
                defer masked.deinit(allocator);
                var gathered = try ColumnStore.init(allocator, .{ .bigint = {} }, true);
                defer gathered.deinit(allocator);
                for (0..dst_start) |i| {
                    const v = rand.boolean();
                    try perbit.appendValidBit(allocator, i, v);
                    try masked.appendValidBit(allocator, i, v);
                    try gathered.appendValidBit(allocator, i, v);
                }

                for (rows, 0..) |row, j| {
                    try perbit.appendValidBit(allocator, dst_start + j, storage_column.isValidBit(src, row));
                }
                try masked.appendMaskedValidity(allocator, dst_start, src, &mask, n);
                try gathered.appendGatheredValidity(allocator, dst_start, src, rows);
                try std.testing.expectEqualSlices(u8, perbit.nulls.?.items, masked.nulls.?.items);
                try std.testing.expectEqualSlices(u8, perbit.nulls.?.items, gathered.nulls.?.items);
            }
        }
    }
}

pub const DataStore = union(TypeTag) {
    int: std.ArrayList(i32),
    bigint: std.ArrayList(i64),
    boolean: std.ArrayList(u8),
    varchar: StringStore,
    string: StringStore,
    float: std.ArrayList(f32),
    double: std.ArrayList(f64),
    date: std.ArrayList(i32),
    datetime: std.ArrayList(i64),
    tinyint: std.ArrayList(i8),
    smallint: std.ArrayList(i16),
    largeint: std.ArrayList(i128),
    char: StringStore,
    decimal64: std.ArrayList(i64),
    decimal128: std.ArrayList(i128),
    uuid: std.ArrayList(u128),
    json: StringStore,

    pub fn init(allocator: Allocator, t: Type) Allocator.Error!DataStore {
        return initCapacity(allocator, t, 0, 0);
    }

    pub fn initTag(allocator: Allocator, tag: TypeTag) Allocator.Error!DataStore {
        return switch (tag) {
            inline else => |t| @unionInit(
                DataStore,
                @tagName(t),
                if (@FieldType(DataStore, @tagName(t)) == StringStore) try StringStore.init(allocator) else .empty,
            ),
        };
    }

    pub fn initCapacity(
        allocator: Allocator,
        t: Type,
        rows_cap: usize,
        bytes_cap: usize,
    ) Allocator.Error!DataStore {
        return switch (t) {
            .int => .{ .int = try ensuredCapList(i32, allocator, rows_cap) },
            .bigint => .{ .bigint = try ensuredCapList(i64, allocator, rows_cap) },
            .boolean => .{ .boolean = try ensuredCapList(u8, allocator, rows_cap) },
            .varchar => .{ .varchar = try StringStore.initCapacity(allocator, rows_cap, bytes_cap) },
            .string => .{ .string = try StringStore.initCapacity(allocator, rows_cap, bytes_cap) },
            .float => .{ .float = try ensuredCapList(f32, allocator, rows_cap) },
            .double => .{ .double = try ensuredCapList(f64, allocator, rows_cap) },
            .date => .{ .date = try ensuredCapList(i32, allocator, rows_cap) },
            .datetime => .{ .datetime = try ensuredCapList(i64, allocator, rows_cap) },
            .tinyint => .{ .tinyint = try ensuredCapList(i8, allocator, rows_cap) },
            .smallint => .{ .smallint = try ensuredCapList(i16, allocator, rows_cap) },
            .largeint => .{ .largeint = try ensuredCapList(i128, allocator, rows_cap) },
            .char => .{ .char = try StringStore.initCapacity(allocator, rows_cap, bytes_cap) },
            .decimal64 => .{ .decimal64 = try ensuredCapList(i64, allocator, rows_cap) },
            .decimal128 => .{ .decimal128 = try ensuredCapList(i128, allocator, rows_cap) },
            .uuid => .{ .uuid = try ensuredCapList(u128, allocator, rows_cap) },
            .json => .{ .json = try StringStore.initCapacity(allocator, rows_cap, bytes_cap) },
        };
    }

    fn ensuredCapList(comptime T: type, allocator: Allocator, cap: usize) !std.ArrayList(T) {
        var list: std.ArrayList(T) = .empty;
        if (cap > 0) try list.ensureTotalCapacity(allocator, cap);
        return list;
    }

    pub fn deinit(self: *DataStore, allocator: Allocator) void {
        switch (self.*) {
            .int => |*list| list.deinit(allocator),
            .bigint => |*list| list.deinit(allocator),
            .boolean => |*list| list.deinit(allocator),
            .varchar => |*ss| ss.deinit(allocator),
            .string => |*ss| ss.deinit(allocator),
            .float => |*list| list.deinit(allocator),
            .double => |*list| list.deinit(allocator),
            .date => |*list| list.deinit(allocator),
            .datetime => |*list| list.deinit(allocator),
            .tinyint => |*list| list.deinit(allocator),
            .smallint => |*list| list.deinit(allocator),
            .largeint => |*list| list.deinit(allocator),
            .char => |*ss| ss.deinit(allocator),
            .decimal64 => |*list| list.deinit(allocator),
            .decimal128 => |*list| list.deinit(allocator),
            .uuid => |*list| list.deinit(allocator),
            .json => |*ss| ss.deinit(allocator),
        }
        self.* = undefined;
    }

    pub fn rowCount(self: DataStore) usize {
        return switch (self) {
            .int => |l| l.items.len,
            .bigint => |l| l.items.len,
            .boolean => |l| l.items.len,
            .varchar => |s| s.rowCount(),
            .string => |s| s.rowCount(),
            .float => |l| l.items.len,
            .double => |l| l.items.len,
            .date => |l| l.items.len,
            .datetime => |l| l.items.len,
            .tinyint => |l| l.items.len,
            .smallint => |l| l.items.len,
            .largeint => |l| l.items.len,
            .char => |s| s.rowCount(),
            .decimal64 => |l| l.items.len,
            .decimal128 => |l| l.items.len,
            .uuid => |l| l.items.len,
            .json => |s| s.rowCount(),
        };
    }

    pub fn view(self: DataStore) ValueView {
        return switch (self) {
            .int => |l| .{ .int = l.items },
            .bigint => |l| .{ .bigint = l.items },
            .boolean => |l| .{ .boolean = l.items },
            .varchar => |s| .{ .varchar = s.view() },
            .string => |s| .{ .string = s.view() },
            .float => |l| .{ .float = l.items },
            .double => |l| .{ .double = l.items },
            .date => |l| .{ .date = l.items },
            .datetime => |l| .{ .datetime = l.items },
            .tinyint => |l| .{ .tinyint = l.items },
            .smallint => |l| .{ .smallint = l.items },
            .largeint => |l| .{ .largeint = l.items },
            .char => |s| .{ .char = s.view() },
            .decimal64 => |l| .{ .decimal64 = l.items },
            .decimal128 => |l| .{ .decimal128 = l.items },
            .uuid => |l| .{ .uuid = l.items },
            .json => |s| .{ .json = s.view() },
        };
    }

    pub fn clear(self: *DataStore) void {
        switch (self.*) {
            .int => |*l| l.clearRetainingCapacity(),
            .bigint => |*l| l.clearRetainingCapacity(),
            .boolean => |*l| l.clearRetainingCapacity(),
            .varchar => |*s| s.clear(),
            .string => |*s| s.clear(),
            .float => |*l| l.clearRetainingCapacity(),
            .double => |*l| l.clearRetainingCapacity(),
            .date => |*l| l.clearRetainingCapacity(),
            .datetime => |*l| l.clearRetainingCapacity(),
            .tinyint => |*l| l.clearRetainingCapacity(),
            .smallint => |*l| l.clearRetainingCapacity(),
            .largeint => |*l| l.clearRetainingCapacity(),
            .char => |*s| s.clear(),
            .decimal64 => |*l| l.clearRetainingCapacity(),
            .decimal128 => |*l| l.clearRetainingCapacity(),
            .uuid => |*l| l.clearRetainingCapacity(),
            .json => |*s| s.clear(),
        }
    }

    /// Append a placeholder/null value (zero for ints, false for bool,
    /// empty for strings). Used when the row's actual value is NULL — the
    /// data slot still has to be filled to keep row indices aligned.
    pub fn appendNullPlaceholder(self: *DataStore, allocator: Allocator) !void {
        switch (self.*) {
            .int => |*l| try l.append(allocator, 0),
            .bigint => |*l| try l.append(allocator, 0),
            .boolean => |*l| try l.append(allocator, 0),
            .varchar => |*s| try s.appendValue(allocator, ""),
            .string => |*s| try s.appendValue(allocator, ""),
            .float => |*l| try l.append(allocator, 0.0),
            .double => |*l| try l.append(allocator, 0.0),
            .date => |*l| try l.append(allocator, 0),
            .datetime => |*l| try l.append(allocator, 0),
            .tinyint => |*l| try l.append(allocator, 0),
            .smallint => |*l| try l.append(allocator, 0),
            .largeint => |*l| try l.append(allocator, 0),
            .char => |*s| try s.appendValue(allocator, ""),
            .decimal64 => |*l| try l.append(allocator, 0),
            .decimal128 => |*l| try l.append(allocator, 0),
            .uuid => |*l| try l.append(allocator, 0),
            .json => |*s| try s.appendValue(allocator, ""),
        }
    }

    /// Bulk `appendNullPlaceholder` × n — one appendNTimes per column
    /// instead of n per-value calls.
    pub fn appendNullPlaceholders(self: *DataStore, allocator: Allocator, n: usize) !void {
        switch (self.*) {
            .int => |*l| try l.appendNTimes(allocator, 0, n),
            .bigint => |*l| try l.appendNTimes(allocator, 0, n),
            .boolean => |*l| try l.appendNTimes(allocator, 0, n),
            .varchar => |*s| try s.appendEmptyValues(allocator, n),
            .string => |*s| try s.appendEmptyValues(allocator, n),
            .float => |*l| try l.appendNTimes(allocator, 0.0, n),
            .double => |*l| try l.appendNTimes(allocator, 0.0, n),
            .date => |*l| try l.appendNTimes(allocator, 0, n),
            .datetime => |*l| try l.appendNTimes(allocator, 0, n),
            .tinyint => |*l| try l.appendNTimes(allocator, 0, n),
            .smallint => |*l| try l.appendNTimes(allocator, 0, n),
            .largeint => |*l| try l.appendNTimes(allocator, 0, n),
            .char => |*s| try s.appendEmptyValues(allocator, n),
            .decimal64 => |*l| try l.appendNTimes(allocator, 0, n),
            .decimal128 => |*l| try l.appendNTimes(allocator, 0, n),
            .uuid => |*l| try l.appendNTimes(allocator, 0, n),
            .json => |*s| try s.appendEmptyValues(allocator, n),
        }
    }
};
