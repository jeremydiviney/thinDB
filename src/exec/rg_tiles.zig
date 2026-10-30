//! Dynamic row-group claims: tiles for the parallel aggregate cores (silo grid,
//! low-cardinality group, global aggregate), and the row groups the chunks of a
//! parallel scan's fused partial aggregate drain. Workers pull consecutive row
//! groups off one shared atomic cursor until it runs dry.
//!
//! Tiles are cut over the row groups that survive zone-map pruning, not over
//! the table's flat row-group space. On a clustered table a selective filter's
//! survivors sit in one narrow run; fixed tiles over the flat space put that
//! whole run into one or two tiles and the rest of the fleet idles. The tile
//! size also shrinks with the work, so every worker the core sized gets claims.
//! Guided tiles shrink with the work left too, so a run of costly row groups
//! late in the order can't leave one worker draining a whole tile of them while
//! the rest idle.
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
    workers: usize = 1,
    guided: bool = false,
    next: std.atomic.Value(usize) = .init(0),

    /// `surviving` is a per-row-group survival mask over the same flat space
    /// (`Scan.survivingMask`); it is copied, not retained.
    pub fn init(
        allocator: std.mem.Allocator,
        total_rgs: usize,
        surviving: ?[]const bool,
        workers: usize,
        max_tile_rgs: usize,
        guided: bool,
    ) !RowGroupTiles {
        var self: RowGroupTiles = .{ .unit_count = total_rgs, .total_rgs = total_rgs, .workers = @max(workers, 1), .guided = guided };
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
        self.tile_units = std.math.clamp(self.unit_count / (self.workers * TILES_PER_WORKER), 1, @max(max_tile_rgs, 1));
        return self;
    }

    pub fn deinit(self: *RowGroupTiles, allocator: std.mem.Allocator) void {
        if (self.survivors) |s| allocator.free(s);
        self.survivors = null;
    }

    /// A tile of `tile_units`; a guided one of few enough units to leave
    /// every worker `TILES_PER_WORKER` claims of what remains, but at least one.
    pub fn claim(self: *RowGroupTiles) ?Tile {
        const want = if (self.guided) guided: {
            const left = self.unit_count -| self.next.load(.monotonic);
            break :guided std.math.clamp(left / (self.workers * TILES_PER_WORKER), 1, self.tile_units);
        } else self.tile_units;
        const lo = self.next.fetchAdd(want, .monotonic);
        if (lo >= self.unit_count) return null;
        const hi = @min(lo + want, self.unit_count);
        const survivors = self.survivors orelse return .{ .lo = lo, .hi = hi };
        // Pruned groups between two claimed survivors ride the earlier tile;
        // the last tile runs to the end of the flat space for the memtable.
        return .{ .lo = survivors[lo], .hi = if (hi == self.unit_count) self.total_rgs else survivors[hi] };
    }

    pub fn exhausted(self: *const RowGroupTiles) bool {
        return self.next.load(.acquire) >= self.unit_count;
    }
};

/// A half-open row-group range in `(segment, row_group)` coordinates, and
/// whether the memtable follows it.
pub const Range = struct {
    start_seg: usize,
    start_rg: usize,
    end_seg: usize,
    end_rg: usize,
    scan_memtable: bool,
};

/// A fine claim takes at most one part in `CLAIM_SPREAD × runners` of the
/// survivors left, leaving every runner that many claims ahead, so the claims
/// shrink toward single row groups as the survivors run out.
const CLAIM_SPREAD: usize = 2;

/// The surviving row groups a parallel scan's fused partial aggregate
/// chunks claim as they drain, instead of each draining a fixed range (#652).
/// A fixed cut has to guess what each group costs, and zone maps can't tell
/// a group passing nearly every row from one passing a handful, so dense
/// groups paired into a few chunks that ran long after the rest. Off one
/// cursor a group goes to whichever runner is free. A chunk takes at most
/// its share (`ChunkFeed.share`) in all, as many survivors as an
/// equal-count cut gives it, so the chunks keep that cut's sizes.
///
/// Every cut between two chunks costs the combine the groups that straddle
/// it, which for keys near the rows' cardinality is most of a row group.
/// So a claim takes the chunk's whole share, one contiguous run like an
/// equal-count cut, until `fine` says the finished chunks' partials
/// collapse. Cuts are cheap then, and claims shrink toward single row groups
/// as the survivors run out so the runners finish within a group of each
/// other.
pub const ChunkClaims = struct {
    /// Ascending flat indices of the surviving row groups.
    survivors: []const u32,
    /// Each segment's first flat row-group index, then the flat total.
    seg_start: []const usize,
    runners: usize,
    /// Whether the partials collapse enough for claims to split runs.
    fine: std.atomic.Value(bool) = .init(false),
    next: std.atomic.Value(usize) = .init(0),

    const Units = struct { lo: usize, hi: usize };

    /// Survivors `[lo, hi)` (positions in `survivors`): `room` of them (at
    /// least 1), or once `fine`, few enough to leave every runner
    /// `CLAIM_SPREAD` claims of what remains, but at least one. Null once
    /// all are claimed.
    fn claimUnits(self: *ChunkClaims, room: usize) ?Units {
        const units = self.survivors.len;
        const left = units -| self.next.load(.monotonic);
        const want = if (self.fine.load(.monotonic))
            std.math.clamp(left / (CLAIM_SPREAD * @max(self.runners, 1)), 1, room)
        else
            room;
        const lo = self.next.fetchAdd(want, .monotonic);
        if (lo >= units) return null;
        return .{ .lo = lo, .hi = @min(lo + want, units) };
    }

    /// Survivors `[lo, hi)` as a row-group range. Pruned groups between two
    /// claimed survivors ride the earlier claim; the claim of the last
    /// survivor runs to the end of the flat space and drains the memtable.
    fn range(self: *const ChunkClaims, units: Units) Range {
        const last = units.hi == self.survivors.len;
        const start = coord(self.seg_start, self.survivors[units.lo]);
        const end = coord(self.seg_start, if (last) self.seg_start[self.seg_start.len - 1] else self.survivors[units.hi]);
        return .{ .start_seg = start.seg, .start_rg = start.rg, .end_seg = end.seg, .end_rg = end.rg, .scan_memtable = last };
    }
};

/// One chunk's claims on a `ChunkClaims`, `share` survivors at most.
pub const ChunkFeed = struct {
    claims: *ChunkClaims,
    share: usize,
    taken: usize = 0,

    pub fn claim(self: *ChunkFeed) ?Range {
        if (self.taken >= self.share) return null;
        const units = self.claims.claimUnits(self.share - self.taken) orelse return null;
        self.taken += units.hi - units.lo;
        return self.claims.range(units);
    }
};

/// Flat row-group index `flat` as (segment, row group); the flat total maps
/// past the last segment.
fn coord(seg_start: []const usize, flat: usize) struct { seg: usize, rg: usize } {
    const segs = seg_start.len - 1;
    if (flat >= seg_start[segs]) return .{ .seg = segs, .rg = 0 };
    var seg: usize = 0;
    while (seg + 1 < segs and seg_start[seg + 1] <= flat) seg += 1;
    return .{ .seg = seg, .rg = flat - seg_start[seg] };
}

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
    var tiles = try RowGroupTiles.init(allocator, mask.len, &mask, 6, 16, false);
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
        var tiles = try RowGroupTiles.init(allocator, c.total, &mask, c.workers, c.max, false);
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
        var tiles = try RowGroupTiles.init(allocator, 40, mask, 4, 16, false);
        defer tiles.deinit(allocator);
        try std.testing.expect(tiles.survivors == null);
        try std.testing.expectEqual(@as(usize, 40), tiles.unit_count);
        try std.testing.expectEqual(@as(usize, 5), tiles.tile_units);
    }
}

test "RowGroupTiles sizes tiles to the work under the cap" {
    const allocator = std.testing.allocator;
    var big = try RowGroupTiles.init(allocator, 1500, null, 12, 16, false);
    defer big.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 16), big.tile_units);
    var small = try RowGroupTiles.init(allocator, 20, null, 10, 16, false);
    defer small.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), small.tile_units);
    var capped = try RowGroupTiles.init(allocator, 1500, null, 12, 4, false);
    defer capped.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 4), capped.tile_units);
    var empty = try RowGroupTiles.init(allocator, 0, null, 4, 16, false);
    defer empty.deinit(allocator);
    try std.testing.expect(empty.claim() == null);
    try std.testing.expect(empty.exhausted());
}

test "RowGroupTiles shrinks claims toward single row groups as the work runs out" {
    const allocator = std.testing.allocator;
    var tiles = try RowGroupTiles.init(allocator, 1500, null, 12, 16, true);
    defer tiles.deinit(allocator);
    const got = try drainTiles(allocator, &tiles);
    defer allocator.free(got);
    try std.testing.expectEqual(@as(usize, 16), got[0].hi - got[0].lo);
    for (got[1..], got[0 .. got.len - 1]) |t, prev| try std.testing.expect(t.hi - t.lo <= prev.hi - prev.lo);
    // Under 2 claims per worker of work left, every claim is one row group.
    for (got[got.len - 24 ..]) |t| try std.testing.expectEqual(@as(usize, 1), t.hi - t.lo);
    try std.testing.expectEqual(@as(usize, 1500), got[got.len - 1].hi);
}

test "ChunkClaims give each chunk one contiguous run until the partials collapse" {
    // Three segments of 10, 6 and 8 row groups; every third group survives.
    const seg_start = [_]usize{ 0, 10, 16, 24 };
    var survivors: [8]u32 = undefined;
    for (&survivors, 0..) |*sv, i| sv.* = @intCast(i * 3);
    var claims: ChunkClaims = .{ .survivors = &survivors, .seg_start = &seg_start, .runners = 1 };
    var feeds = [_]ChunkFeed{
        .{ .claims = &claims, .share = 3 },
        .{ .claims = &claims, .share = 3 },
        .{ .claims = &claims, .share = 2 },
    };

    // One runner draining chunk after chunk: each takes its share in one
    // claim, pruned groups riding along, and the claim of the survivor at
    // flat 21 (the last) runs to the end and alone drains the memtable.
    const want = [_]struct { chunk: usize, range: Range }{
        .{ .chunk = 0, .range = .{ .start_seg = 0, .start_rg = 0, .end_seg = 0, .end_rg = 9, .scan_memtable = false } },
        .{ .chunk = 1, .range = .{ .start_seg = 0, .start_rg = 9, .end_seg = 2, .end_rg = 2, .scan_memtable = false } },
        .{ .chunk = 2, .range = .{ .start_seg = 2, .start_rg = 2, .end_seg = 3, .end_rg = 0, .scan_memtable = true } },
    };
    var got: std.ArrayList(struct { chunk: usize, range: Range }) = .empty;
    defer got.deinit(std.testing.allocator);
    for (&feeds, 0..) |*feed, c| while (feed.claim()) |r| try got.append(std.testing.allocator, .{ .chunk = c, .range = r });
    try std.testing.expectEqual(want.len, got.items.len);
    for (want, got.items) |w, g| {
        try std.testing.expectEqual(w.chunk, g.chunk);
        try std.testing.expectEqual(w.range, g.range);
    }
    try std.testing.expectEqual(@as(usize, 3), feeds[0].taken);
    try std.testing.expectEqual(@as(usize, 3), feeds[1].taken);
    try std.testing.expectEqual(@as(usize, 2), feeds[2].taken);
}

test "ChunkClaims shrink toward single row groups once the partials collapse" {
    var survivors: [96]u32 = undefined;
    for (&survivors, 0..) |*sv, i| sv.* = @intCast(i);
    const seg_start = [_]usize{ 0, 96 };
    var claims: ChunkClaims = .{ .survivors = &survivors, .seg_start = &seg_start, .runners = 4 };
    claims.fine.store(true, .monotonic);
    var sizes: std.ArrayList(usize) = .empty;
    defer sizes.deinit(std.testing.allocator);
    while (claims.claimUnits(4)) |u| try sizes.append(std.testing.allocator, u.hi - u.lo);
    var sum: usize = 0;
    for (sizes.items, 0..) |n, i| {
        sum += n;
        try std.testing.expect(n >= 1 and n <= 4);
        if (i > 0) try std.testing.expect(n <= sizes.items[i - 1]);
    }
    try std.testing.expectEqual(@as(usize, 96), sum);
    try std.testing.expectEqual(@as(usize, 4), sizes.items[0]);
    // The last two claims per runner are single row groups.
    for (sizes.items[sizes.items.len - 2 * 4 ..]) |n| try std.testing.expectEqual(@as(usize, 1), n);
}
