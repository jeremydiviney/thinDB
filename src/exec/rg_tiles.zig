//! Dynamic row-group tile claims for the parallel aggregate cores (silo grid,
//! low-cardinality group, global aggregate). Workers pull tiles of consecutive
//! row groups off one shared atomic cursor until it runs dry.
//!
//! Tiles are cut over the row groups that survive zone-map pruning, not over
//! the table's flat row-group space. On a clustered table a selective filter's
//! survivors sit in one narrow run; fixed tiles over the flat space put that
//! whole run into one or two tiles and the rest of the fleet idles. The tile
//! size also shrinks with the work, so every worker the core sized gets claims.
const std = @import("std");

/// Flat row-group range [lo, hi). The tile whose `hi` equals the claim space's
/// `total_rgs` is the final one and also drains the memtable.
pub const Tile = struct { lo: usize, hi: usize };

/// Claims each worker should get on average: one slow tile then can't leave the
/// rest of the fleet waiting on a single straggler.
const TILES_PER_WORKER: usize = 2;

pub const RowGroupTiles = struct {
    /// Ascending flat indices of the surviving row groups; null claims every
    /// row group of the flat space.
    survivors: ?[]u32 = null,
    unit_count: usize = 0,
    total_rgs: usize = 0,
    tile_units: usize = 1,
    next: std.atomic.Value(usize) = .init(0),

    /// `surviving` is a per-row-group survival mask over the same flat space
    /// (`Scan.survivingMask`); it is copied, not retained.
    pub fn init(
        allocator: std.mem.Allocator,
        total_rgs: usize,
        surviving: ?[]const bool,
        workers: usize,
        max_tile_rgs: usize,
    ) !RowGroupTiles {
        var self: RowGroupTiles = .{ .unit_count = total_rgs, .total_rgs = total_rgs };
        if (surviving) |mask| if (mask.len == total_rgs) {
            var n: usize = 0;
            for (mask) |m| n += @intFromBool(m);
            // With no survivor the flat space is still claimed: its final tile
            // carries the memtable, and pruned groups skip at near-zero cost.
            if (n > 0 and n < total_rgs) {
                const list = try allocator.alloc(u32, n);
                var i: usize = 0;
                for (mask, 0..) |m, flat| if (m) {
                    list[i] = @intCast(flat);
                    i += 1;
                };
                self.survivors = list;
                self.unit_count = n;
            }
        };
        self.tile_units = std.math.clamp(self.unit_count / (@max(workers, 1) * TILES_PER_WORKER), 1, @max(max_tile_rgs, 1));
        return self;
    }

    pub fn deinit(self: *RowGroupTiles, allocator: std.mem.Allocator) void {
        if (self.survivors) |s| allocator.free(s);
        self.survivors = null;
    }

    pub fn claim(self: *RowGroupTiles) ?Tile {
        const lo = self.next.fetchAdd(self.tile_units, .monotonic);
        if (lo >= self.unit_count) return null;
        const hi = @min(lo + self.tile_units, self.unit_count);
        const survivors = self.survivors orelse return .{ .lo = lo, .hi = hi };
        // Pruned groups between two claimed survivors ride the earlier tile;
        // the last tile runs to the end of the flat space for the memtable.
        return .{ .lo = survivors[lo], .hi = if (hi == self.unit_count) self.total_rgs else survivors[hi] };
    }

    pub fn exhausted(self: *const RowGroupTiles) bool {
        return self.next.load(.acquire) >= self.unit_count;
    }
};

fn drainTiles(allocator: std.mem.Allocator, tiles: *RowGroupTiles) ![]Tile {
    var out: std.ArrayList(Tile) = .empty;
    errdefer out.deinit(allocator);
    while (tiles.claim()) |t| try out.append(allocator, t);
    return out.toOwnedSlice(allocator);
}

test "RowGroupTiles cuts a clustered survivor run across every worker" {
    const allocator = std.testing.allocator;
    var mask = [_]bool{false} ** 100;
    for (mask[40..52]) |*m| m.* = true;
    var tiles = try RowGroupTiles.init(allocator, mask.len, &mask, 6, 16);
    defer tiles.deinit(allocator);
    const got = try drainTiles(allocator, &tiles);
    defer allocator.free(got);
    try std.testing.expectEqual(@as(usize, 12), got.len);
    for (got[0 .. got.len - 1], 0..) |t, i| {
        try std.testing.expectEqual(40 + i, t.lo);
        try std.testing.expectEqual(41 + i, t.hi);
    }
    try std.testing.expectEqual(@as(usize, 51), got[got.len - 1].lo);
    try std.testing.expectEqual(@as(usize, 100), got[got.len - 1].hi);
    try std.testing.expect(tiles.exhausted());
}

test "RowGroupTiles covers every survivor exactly once and ends at the flat total" {
    const allocator = std.testing.allocator;
    const cases = .{
        .{ .total = 37, .stride = 3, .workers = 4, .max = 16 },
        .{ .total = 1000, .stride = 7, .workers = 12, .max = 16 },
        .{ .total = 64, .stride = 1, .workers = 12, .max = 16 },
        .{ .total = 5, .stride = 2, .workers = 1, .max = 16 },
    };
    inline for (cases) |c| {
        var mask: [c.total]bool = undefined;
        for (&mask, 0..) |*m, i| m.* = i % c.stride == 0;
        var tiles = try RowGroupTiles.init(allocator, c.total, &mask, c.workers, c.max);
        defer tiles.deinit(allocator);
        const got = try drainTiles(allocator, &tiles);
        defer allocator.free(got);
        var seen = [_]u8{0} ** c.total;
        for (got, 0..) |t, i| {
            try std.testing.expect(t.lo < t.hi);
            if (i > 0) try std.testing.expectEqual(got[i - 1].hi, t.lo);
            for (t.lo..t.hi) |rg| seen[rg] += 1;
        }
        try std.testing.expectEqual(@as(usize, c.total), got[got.len - 1].hi);
        for (mask, seen) |m, s| if (m) try std.testing.expectEqual(@as(u8, 1), s);
    }
}

test "RowGroupTiles keeps the flat space without pruning, without survivors, or on a torn mask" {
    const allocator = std.testing.allocator;
    const all = [_]bool{true} ** 40;
    const none = [_]bool{false} ** 40;
    const torn = [_]bool{true} ** 39;
    const masks = [_]?[]const bool{ null, &all, &none, &torn };
    for (masks) |mask| {
        var tiles = try RowGroupTiles.init(allocator, 40, mask, 4, 16);
        defer tiles.deinit(allocator);
        try std.testing.expect(tiles.survivors == null);
        try std.testing.expectEqual(@as(usize, 40), tiles.unit_count);
        try std.testing.expectEqual(@as(usize, 5), tiles.tile_units);
    }
}

test "RowGroupTiles sizes tiles to the work under the cap" {
    const allocator = std.testing.allocator;
    var big = try RowGroupTiles.init(allocator, 1500, null, 12, 16);
    defer big.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 16), big.tile_units);
    var small = try RowGroupTiles.init(allocator, 20, null, 10, 16);
    defer small.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), small.tile_units);
    var capped = try RowGroupTiles.init(allocator, 1500, null, 12, 4);
    defer capped.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 4), capped.tile_units);
    var empty = try RowGroupTiles.init(allocator, 0, null, 4, 16);
    defer empty.deinit(allocator);
    try std.testing.expect(empty.claim() == null);
    try std.testing.expect(empty.exhausted());
}
