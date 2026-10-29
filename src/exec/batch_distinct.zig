//! Collapse a batch to its distinct rows with each row's repeat count, so an
//! expression over the batch runs once per distinct input instead of once per
//! row. A weighted consumer folds each distinct row `weight` times over.

const std = @import("std");
const Allocator = std.mem.Allocator;
const exec = @import("exec.zig");
const storage = @import("../storage/storage.zig");
const ColumnStore = @import("../engine/store.zig").ColumnStore;
const transform = @import("../engine/transform.zig");

const Batch = exec.Batch;
const ColumnView = storage.ColumnView;

const NULL_HASH: u64 = 0x9E37_79B9_7F4A_7C15;
const MIN_SLOTS: usize = 1024;

pub const BatchDistinct = struct {
    /// Open-addressed: a distinct row's index + 1, or 0 when empty.
    slots: []u32 = &.{},
    row_hashes: std.ArrayListUnmanaged(u64) = .empty,
    hashes: std.ArrayListUnmanaged(u64) = .empty,
    /// The first row of each distinct value, in first-seen order.
    rows: std.ArrayListUnmanaged(u32) = .empty,
    weights: std.ArrayListUnmanaged(u32) = .empty,
    stores: []ColumnStore = &.{},
    views: []ColumnView = &.{},

    pub fn deinit(self: *BatchDistinct, allocator: Allocator) void {
        if (self.slots.len > 0) allocator.free(self.slots);
        self.row_hashes.deinit(allocator);
        self.hashes.deinit(allocator);
        self.rows.deinit(allocator);
        self.weights.deinit(allocator);
        self.deinitColumns(allocator);
    }

    fn deinitColumns(self: *BatchDistinct, allocator: Allocator) void {
        for (self.stores) |*st| st.deinit(allocator);
        if (self.stores.len > 0) allocator.free(self.stores);
        if (self.views.len > 0) allocator.free(self.views);
        self.stores = &.{};
        self.views = &.{};
    }

    /// Find `batch`'s distinct rows, comparing every column but `skip`.
    /// Returns how many; `rows` and `weights` then describe them.
    pub fn collapse(self: *BatchDistinct, allocator: Allocator, batch: Batch, skip: ?usize) !usize {
        const n = batch.row_count;
        self.rows.clearRetainingCapacity();
        self.weights.clearRetainingCapacity();
        self.hashes.clearRetainingCapacity();
        if (n == 0) return 0;
        if (n > std.math.maxInt(u32)) return error.UnsupportedOperatorForType;

        try self.row_hashes.resize(allocator, n);
        const hs = self.row_hashes.items;
        @memset(hs, 0);
        for (batch.values, 0..) |view, ci| {
            if (skip != null and skip.? == ci) continue;
            hashColumn(view, hs);
        }

        const cap = @max(MIN_SLOTS, std.math.ceilPowerOfTwoAssert(usize, n * 2));
        if (self.slots.len < cap) {
            if (self.slots.len > 0) allocator.free(self.slots);
            self.slots = &.{};
            self.slots = try allocator.alloc(u32, cap);
        }
        const slots = self.slots[0..cap];
        @memset(slots, 0);
        const shift: u6 = @intCast(@as(u32, 64) - std.math.log2_int(usize, cap));
        const mask = cap - 1;

        try self.rows.ensureTotalCapacity(allocator, n);
        try self.weights.ensureTotalCapacity(allocator, n);
        try self.hashes.ensureTotalCapacity(allocator, n);
        for (hs, 0..) |h, r| {
            var s: usize = @intCast(h >> shift);
            while (true) : (s = (s + 1) & mask) {
                const held = slots[s];
                if (held == 0) {
                    slots[s] = @intCast(self.rows.items.len + 1);
                    self.rows.appendAssumeCapacity(@intCast(r));
                    self.weights.appendAssumeCapacity(1);
                    self.hashes.appendAssumeCapacity(h);
                    break;
                }
                const d = held - 1;
                if (self.hashes.items[d] == h and rowsEqual(batch, skip, self.rows.items[d], r)) {
                    self.weights.items[d] += 1;
                    break;
                }
            }
        }
        return self.rows.items.len;
    }

    /// The distinct rows `collapse` found, copied into a batch shaped like
    /// `batch`. Valid until the next call.
    pub fn gather(self: *BatchDistinct, allocator: Allocator, batch: Batch) !Batch {
        try self.ensureColumns(allocator, batch);
        for (self.stores, batch.values, self.views) |*st, view, *out| {
            st.clear();
            if (!try transform.appendGatheredColumn(allocator, view, self.rows.items, st)) return error.TypeMismatch;
            out.* = st.view();
        }
        return .{ .schema = batch.schema, .values = self.views, .row_count = self.rows.items.len };
    }

    /// One store per column, typed like the batch's views. A memtable batch
    /// and a segment batch may type or null a column differently.
    fn ensureColumns(self: *BatchDistinct, allocator: Allocator, batch: Batch) !void {
        if (self.holds(batch)) return;
        self.deinitColumns(allocator);
        const stores = try allocator.alloc(ColumnStore, batch.values.len);
        var built: usize = 0;
        errdefer {
            for (stores[0..built]) |*st| st.deinit(allocator);
            allocator.free(stores);
        }
        for (batch.values, batch.schema, 0..) |view, col, i| {
            stores[i] = try ColumnStore.initLike(allocator, view, col.nullable or view.nulls != null);
            built += 1;
        }
        const views = try allocator.alloc(ColumnView, batch.values.len);
        self.stores = stores;
        self.views = views;
    }

    fn holds(self: *const BatchDistinct, batch: Batch) bool {
        if (self.stores.len != batch.values.len) return false;
        for (self.stores, batch.values) |st, view| {
            if (std.meta.activeTag(st.data) != std.meta.activeTag(view.data)) return false;
            if (view.nulls != null and st.nulls == null) return false;
        }
        return true;
    }
};

inline fn mix(h: u64, v: u64) u64 {
    return (std.math.rotl(u64, h, 5) ^ v) *% 0x9E37_79B9_7F4A_7C15;
}

fn hashColumn(view: ColumnView, hs: []u64) void {
    switch (view.data) {
        .varchar, .string, .char, .json, .nested, .bitmap => |sv| {
            for (hs, 0..) |*h, r| {
                const v = if (view.isValid(r)) std.hash.XxHash3.hash(0, sv.rowBytes(r)) else NULL_HASH;
                h.* = mix(h.*, v);
            }
        },
        inline else => |vals| {
            const T = std.meta.Child(@TypeOf(vals));
            const U = std.meta.Int(.unsigned, @bitSizeOf(T));
            for (hs, vals, 0..) |*h, x, r| {
                const bits: U = @bitCast(x);
                const v: u64 = if (!view.isValid(r)) NULL_HASH else if (@bitSizeOf(U) <= 64) bits else @truncate(bits ^ (bits >> 64));
                h.* = mix(h.*, v);
            }
        },
    }
}

fn rowsEqual(batch: Batch, skip: ?usize, a: usize, b: usize) bool {
    for (batch.values, 0..) |view, ci| {
        if (skip != null and skip.? == ci) continue;
        const va = view.isValid(a);
        if (va != view.isValid(b)) return false;
        if (!va) continue;
        const same = switch (view.data) {
            .varchar, .string, .char, .json, .nested, .bitmap => |sv| std.mem.eql(u8, sv.rowBytes(a), sv.rowBytes(b)),
            inline else => |vals| blk: {
                const U = std.meta.Int(.unsigned, @bitSizeOf(std.meta.Child(@TypeOf(vals))));
                break :blk @as(U, @bitCast(vals[a])) == @as(U, @bitCast(vals[b]));
            },
        };
        if (!same) return false;
    }
    return true;
}

test "batch distinct collapses repeated rows and counts them" {
    const allocator = std.testing.allocator;
    const offsets = [_]u32{ 0, 1, 2, 3, 4, 5, 6, 7 };
    const ids = [_]i64{ 1, 1, 1, 0, 0, 2, 1 };
    // Rows 3 and 4 have a NULL id.
    const id_valid = [_]u8{0b0110_0111};
    const schema = [_]@import("../types.zig").Column{ .{ .name = "name", .type = .string }, .{ .name = "id", .type = .bigint, .nullable = true } };
    const views = [_]ColumnView{
        .{ .data = .{ .string = .{ .offsets = &offsets, .bytes = "abaaaab" } } },
        .{ .data = .{ .bigint = &ids }, .nulls = &id_valid },
    };
    const batch = Batch{ .schema = &schema, .values = &views, .row_count = ids.len };

    var dd: BatchDistinct = .{};
    defer dd.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 4), try dd.collapse(allocator, batch, null));
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 3, 5 }, dd.rows.items);
    try std.testing.expectEqualSlices(u32, &.{ 2, 2, 2, 1 }, dd.weights.items);

    const out = try dd.gather(allocator, batch);
    try std.testing.expectEqual(@as(usize, 4), out.row_count);
    try std.testing.expectEqualStrings("b", out.values[0].data.string.rowBytes(1));
    try std.testing.expect(!out.values[1].isValid(2));
    try std.testing.expectEqual(@as(i64, 2), out.values[1].data.bigint[3]);

    try std.testing.expectEqual(@as(usize, 2), try dd.collapse(allocator, batch, 1));
    try std.testing.expectEqualSlices(u32, &.{ 5, 2 }, dd.weights.items);
}
