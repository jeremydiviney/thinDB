//! Two-pass exact top-K for `GROUP BY ... ORDER BY COUNT(*) DESC LIMIT k`.
//!
//! The tally pass counts every input row into one of 2^bits counters by a
//! hash of its group identity, so a counter holds the summed row count of the
//! groups hashing to it and bounds each of them from above. The filter pass
//! groups only the rows whose counter reaches `threshold`: a group's rows
//! share one counter, so every kept group is complete, and every dropped
//! group has at most `threshold - 1` rows. Once the k-th best kept group has
//! `threshold - 1` rows or more, no dropped group can outrank it (one may tie
//! it, and SQL leaves ties at the cut unordered), so the kept groups' top k
//! is the answer.
//!
//! A count no greater than the row count, such as COUNT(DISTINCT), ranks the
//! same way: a dropped group's count is below the threshold too, so the same
//! proof holds once the k-th best kept group reaches `threshold - 1`.
//!
//! It pays when a few groups hold far more rows than the rest (URLs, users,
//! search phrases): the filter pass then groups a few percent of the rows.
//! Near-unique keys spread their rows evenly over the counters, keep nearly
//! every row, and `settle` declines.
//!
//! `HeavyKeys` serves keys that arrive counted per row group (dictionary
//! codes): its tally hashes only the values a row group holds many rows of.
//! `DupSketch` serves near-unique keys, finding the few groups of two rows
//! or more.

const std = @import("std");
const Allocator = std.mem.Allocator;
const parallel = @import("../util/parallel.zig");

/// 2^18 u32 counters are 1 MiB per worker, and 100M rows leave ~380 rows of
/// collision noise per counter: far below the counts a skewed key's top
/// groups reach (thousands to hundreds of thousands on ClickBench).
pub const DEFAULT_BITS: u6 = 18;

/// The filter pass must keep at most 1/this of the rows to beat grouping
/// them all.
const MAX_KEPT_FRACTION_DENOM: u64 = 4;

/// A first filter pass ranked by a count below the row count may keep 1/this
/// of the rows, the lower threshold making a retry less likely.
const BELOW_ROW_COUNT_KEPT_FRACTION_DENOM: u64 = 16;

/// Thresholds `settle` weighs below half the need-th largest counter for a
/// count below the row count.
const BELOW_ROW_COUNT_THRESHOLD_STEPS: u64 = 4096;

/// What the top-K ranks groups by.
pub const RankedBy = enum {
    /// COUNT(*): a group's counter bounds it from above.
    row_count,
    /// A count no greater than the row count, by an unknown margin
    /// (COUNT(DISTINCT)).
    at_most_row_count,
};

/// Longest LIMIT + OFFSET the sketch serves; past it the threshold search
/// over the counters stops being cheap.
pub const MAX_NEED: usize = 1024;

/// Identities whose counter slots `tally` computes before incrementing them.
const TALLY_LANE: usize = 1024;

/// How many increments ahead `tally` prefetches. A worker's counters (1 MiB
/// at DEFAULT_BITS) overflow L2, and each random increment that waits on its
/// line holds up the stores queued behind it.
const TALLY_PREFETCH_DISTANCE: usize = 16;

pub const CountSketch = struct {
    bits: u6,
    /// `workers` slices of 2^bits counters, one per grid worker. A worker
    /// zeroes its own slice on first use, so idle workers cost nothing.
    counts: []u32,
    touched: []bool,
    /// Bit c set: the filter pass keeps the rows of counter c.
    keep: []u64 = &.{},
    threshold: u64 = 0,
    rows: u64 = 0,
    kept_rows: u64 = 0,
    /// The fewest rows a group needs to place at all: the filter pass
    /// drops every counter below it, and once the threshold is down to it
    /// the pass holds every group that can place.
    floor: u64 = 0,
    /// Where the tally records its rows' counters for the filter pass.
    row_slots: ?RowSlots = null,
    /// Set when the tally declined because a key is near-unique, which
    /// `DupSketch` serves.
    near_unique: bool = false,

    pub fn init(allocator: Allocator, bits: u6, workers: usize) !CountSketch {
        std.debug.assert(bits >= 6);
        const counts = try allocator.alloc(u32, workers << bits);
        errdefer allocator.free(counts);
        const touched = try allocator.alloc(bool, workers);
        @memset(touched, false);
        return .{ .bits = bits, .counts = counts, .touched = touched };
    }

    pub fn deinit(self: *CountSketch, allocator: Allocator) void {
        allocator.free(self.counts);
        allocator.free(self.touched);
        if (self.keep.len > 0) allocator.free(self.keep);
        if (self.row_slots) |*rs| rs.deinit();
        self.* = undefined;
    }

    pub fn counter(self: *const CountSketch, identity: u128) usize {
        return @intCast(identityHash(identity) >> @intCast(64 - @as(u7, self.bits)));
    }

    /// Counts each identity's rows (`weights[i]` of them, else one) into
    /// `counts`, a worker's counters, and writes identity i's counter to
    /// `slots_out[i]` when given.
    pub fn tally(self: *const CountSketch, counts: []u32, ids: []const u128, weights: ?[]const u32, slots_out: ?[]u32) void {
        var slots: [TALLY_LANE]u32 = undefined;
        var lo: usize = 0;
        while (lo < ids.len) : (lo += TALLY_LANE) {
            const lane = ids[lo..@min(ids.len, lo + TALLY_LANE)];
            const lane_slots = if (slots_out) |out| out[lo..][0..lane.len] else slots[0..lane.len];
            for (lane, lane_slots) |id, *s| s.* = @intCast(self.counter(id));
            if (weights) |w| {
                for (lane_slots, w[lo..][0..lane.len], 0..) |s, n, i| {
                    if (i + TALLY_PREFETCH_DISTANCE < lane_slots.len) @prefetch(&counts[lane_slots[i + TALLY_PREFETCH_DISTANCE]], .{ .rw = .write });
                    counts[s] += n;
                }
            } else {
                for (lane_slots, 0..) |s, i| {
                    if (i + TALLY_PREFETCH_DISTANCE < lane_slots.len) @prefetch(&counts[lane_slots[i + TALLY_PREFETCH_DISTANCE]], .{ .rw = .write });
                    counts[s] += 1;
                }
            }
        }
    }

    /// Worker `w`'s counters. Only worker `w` may call this during a tally.
    pub fn workerCounts(self: *CountSketch, w: usize) []u32 {
        const n = @as(usize, 1) << self.bits;
        const slice = self.counts[w * n ..][0..n];
        if (!self.touched[w]) {
            @memset(slice, 0);
            self.touched[w] = true;
        }
        return slice;
    }

    /// Sum the workers' counters and pick the threshold for the `need` best
    /// groups: half the need-th largest counter, or for a count below the row
    /// count as far below that as a filter pass keeping 1/16 of the rows
    /// allows. False when the filter pass would keep too many rows to pay.
    pub fn settle(self: *CountSketch, allocator: Allocator, need: usize, ranked_by: RankedBy) !bool {
        std.debug.assert(need > 0 and need <= MAX_NEED);
        const n = @as(usize, 1) << self.bits;
        var merged: ?[]u32 = null;
        for (self.touched, 0..) |t, w| {
            if (!t) continue;
            const slice = self.counts[w * n ..][0..n];
            if (merged) |m| {
                for (m, slice) |*d, s| d.* += s;
            } else merged = slice;
        }
        const totals = merged orelse return false;
        self.rows = 0;
        for (totals) |c| self.rows += c;

        var best: [MAX_NEED]u32 = undefined;
        const kth = kthLargest(u32, totals, best[0..need]);
        if (@as(f64, @floatFromInt(@max(kth, self.floor))) <= collisionCeiling(self.rows, self.bits)) return false;
        self.threshold = @max(1, kth / 2);
        if (ranked_by == .at_most_row_count) self.threshold = lowestThreshold(totals, self.threshold, self.rows / BELOW_ROW_COUNT_KEPT_FRACTION_DENOM);
        self.threshold = @max(self.threshold, self.floor);

        self.keep = try allocator.alloc(u64, n / 64);
        return self.fillKeep(totals);
    }

    /// Lower the threshold to `kth_count + 1` after a filter pass whose k-th
    /// best group held `kth_count` rows: that pass's top groups all survive,
    /// so the next filter pass is exact. False when it would keep too many
    /// rows to pay.
    pub fn retarget(self: *CountSketch, kth_count: u64) bool {
        const n = @as(usize, 1) << self.bits;
        const w = std.mem.indexOfScalar(bool, self.touched, true) orelse return false;
        self.threshold = @max(kth_count + 1, self.floor);
        return self.fillKeep(self.counts[w * n ..][0..n]);
    }

    fn fillKeep(self: *CountSketch, totals: []const u32) bool {
        @memset(self.keep, 0);
        self.kept_rows = 0;
        for (totals, 0..) |c, i| {
            if (c < self.threshold) continue;
            self.keep[i >> 6] |= @as(u64, 1) << @intCast(i & 63);
            self.kept_rows += c;
        }
        return self.kept_rows * MAX_KEPT_FRACTION_DENOM <= self.rows;
    }

    pub fn keeps(self: *const CountSketch, identity: u128) bool {
        const c = self.counter(identity);
        return (self.keep[c >> 6] >> @intCast(c & 63)) & 1 != 0;
    }

    /// Whether a filter pass that found `found` groups, the need-th best
    /// holding `kth_count` rows, found the top `need` of every group.
    pub fn exact(self: *const CountSketch, need: usize, found: usize, kth_count: u64) bool {
        if (self.kept_rows == self.rows or self.threshold <= self.floor) return true;
        return found >= need and kth_count + 1 >= self.threshold;
    }
};

/// The rows a tally saw of each row group it saw in stored order, and their
/// counters, so the filter pass's scan can pick the rows of the counters it
/// keeps without running its filter or decoding the rest
/// (`Scan.RowSieve`). A tally that can't count by row still holds which
/// rows its filter passed, so the filter pass reads only those.
pub const RowSlots = struct {
    allocator: Allocator,
    /// Per segment of the snapshot, the flat index of its first row group.
    seg_first: []usize,
    /// Per flat row group, once a tally has recorded all of it: bit r set
    /// for each row r the tally saw, and those rows' counters in row order
    /// (null when it held the rows alone).
    held: []?[]const u64,
    rows: []?[]const u32,
    /// Per flat row group, what a tally is still recording.
    pending: []?Pending,

    const Pending = struct {
        held: []u64,
        slots: ?[]u32,
        /// The row the next recorded rows start at, and the counters so far.
        next: u32,
        len: u32,
    };

    /// Room for the row groups of `segments`, each entry's
    /// `row_group_count` of them.
    pub fn init(allocator: Allocator, segments: anytype) !RowSlots {
        const seg_first = try allocator.alloc(usize, segments.len);
        errdefer allocator.free(seg_first);
        var total: usize = 0;
        for (segments, seg_first) |entry, *first| {
            first.* = total;
            total += entry.row_group_count;
        }
        const held = try allocator.alloc(?[]const u64, total);
        errdefer allocator.free(held);
        const rows = try allocator.alloc(?[]const u32, total);
        errdefer allocator.free(rows);
        const pending = try allocator.alloc(?Pending, total);
        @memset(held, null);
        @memset(rows, null);
        @memset(pending, null);
        return .{ .allocator = allocator, .seg_first = seg_first, .held = held, .rows = rows, .pending = pending };
    }

    pub fn deinit(self: *RowSlots) void {
        for (self.held, self.rows) |h, r| {
            if (h) |bits| self.allocator.free(bits);
            if (r) |slots| self.allocator.free(slots);
        }
        for (self.pending) |p| if (p) |open| self.freePending(open);
        self.allocator.free(self.held);
        self.allocator.free(self.rows);
        self.allocator.free(self.pending);
        self.allocator.free(self.seg_first);
    }

    pub fn holdsAny(self: *const RowSlots) bool {
        for (self.held) |h| {
            if (h != null) return true;
        }
        return false;
    }

    fn freePending(self: *RowSlots, open: Pending) void {
        self.allocator.free(open.held);
        if (open.slots) |slots| self.allocator.free(slots);
    }

    /// Room for the counters of the rows a tally sees of segment `seg`'s
    /// row group `rg`, of `rows` rows, from row `offset` on: the next
    /// `n`, or of those only the ones at `survivors`, ascending row-group
    /// positions. Rows record in order: null when the rows before them
    /// weren't. The row group's record takes effect with its last rows.
    /// Only the worker that tallies a row group may record it.
    pub fn record(self: *RowSlots, seg: usize, rg: usize, rows: u32, offset: u32, n: u32, survivors: ?[]const u32) !?[]u32 {
        const i = self.seg_first[seg] + rg;
        const open = (try self.openRecord(i, rows, offset, true)) orelse return null;
        const start = open.len;
        holdRows(open, offset, n, survivors);
        const out = open.slots.?[start..open.len];
        self.closeIfWhole(i, rows);
        return out;
    }

    /// Note which rows of segment `seg`'s row group `rg` a tally saw, as
    /// `record` does, without counters: the filter pass keeps every one.
    pub fn hold(self: *RowSlots, seg: usize, rg: usize, rows: u32, offset: u32, n: u32, survivors: ?[]const u32) !void {
        const i = self.seg_first[seg] + rg;
        const open = (try self.openRecord(i, rows, offset, false)) orelse return;
        holdRows(open, offset, n, survivors);
        self.closeIfWhole(i, rows);
    }

    /// Flat row group `i`'s record in progress, begun afresh at row 0; null
    /// when the rows before `offset` weren't recorded, or were recorded
    /// with (or without) counters when these aren't.
    fn openRecord(self: *RowSlots, i: usize, rows: u32, offset: u32, counted: bool) !?*Pending {
        if (offset == 0) {
            if (self.pending[i]) |prior| self.freePending(prior);
            self.pending[i] = null;
            const held = try self.allocator.alloc(u64, (rows + 63) / 64);
            errdefer self.allocator.free(held);
            @memset(held, 0);
            const slots: ?[]u32 = if (counted) try self.allocator.alloc(u32, rows) else null;
            self.pending[i] = .{ .held = held, .slots = slots, .next = 0, .len = 0 };
        }
        const open = if (self.pending[i]) |*p| p else return null;
        if (open.next != offset or (open.slots != null) != counted) return null;
        return open;
    }

    fn holdRows(open: *Pending, offset: u32, n: u32, survivors: ?[]const u32) void {
        if (survivors) |at| {
            for (at) |r| open.held[r >> 6] |= @as(u64, 1) << @intCast(r & 63);
            open.len += @intCast(at.len);
        } else {
            holdRange(open.held, offset, offset + n);
            open.len += n;
        }
        open.next = offset + n;
    }

    /// Flat row group `i`'s record takes effect once it reaches the last row.
    fn closeIfWhole(self: *RowSlots, i: usize, rows: u32) void {
        const open = &self.pending[i].?;
        if (open.next != rows) return;
        if (open.slots) |slots| {
            if (open.len > 0 and open.len < rows and self.allocator.resize(slots, open.len)) open.slots = slots[0..open.len];
        }
        if (self.held[i]) |prior| self.allocator.free(prior);
        if (self.rows[i]) |prior| self.allocator.free(prior);
        self.held[i] = open.held;
        self.rows[i] = open.slots;
        self.pending[i] = null;
    }

    fn holdRange(held: []u64, lo: usize, hi: usize) void {
        var r = lo;
        while (r < hi and r & 63 != 0) : (r += 1) held[r >> 6] |= @as(u64, 1) << @intCast(r & 63);
        while (r + 64 <= hi) : (r += 64) held[r >> 6] = ~@as(u64, 0);
        while (r < hi) : (r += 1) held[r >> 6] |= @as(u64, 1) << @intCast(r & 63);
    }
};

fn identityHash(identity: u128) u64 {
    var x: u64 = @as(u64, @truncate(identity)) ^ (@as(u64, @truncate(identity >> 64)) *% 0x9e3779b97f4a7c15);
    x ^= x >> 33;
    x *%= 0xff51afd7ed558ccd;
    x ^= x >> 33;
    x *%= 0xc4ceb9fe1a85ec53;
    x ^= x >> 33;
    return x;
}

/// Fingerprints a cache line holds. A worker gathers each partition's next
/// line before copying it to the partition's pages, so the tally's scattered
/// writes stay in cache.
const DUP_LINE: usize = 16;

/// Fingerprints a page holds: 1 KiB.
const DUP_PAGE: usize = 256;

/// Pages a worker carves from one allocation.
const DUP_SLAB_PAGES: usize = 256;

const DUP_NO_PAGE: u32 = std.math.maxInt(u32);

/// Fingerprints a `DupSketch` partition aims to hold.
const DUP_PART_TARGET: u64 = 64 * 1024;

const DUP_MIN_PART_BITS: u6 = 4;
/// Bounds a worker's gathering lines at 64 KiB, within L2 beside the scan's
/// batches.
const DUP_MAX_PART_BITS: u6 = 10;

/// A settling set's mark on a fingerprint met twice; fingerprints are 31 bits.
const DUP_REPEATED: u32 = 1 << 31;

/// How many fingerprints ahead a settle prefetches the slot of: a large
/// partition's set outgrows L2.
const DUP_PREFETCH: usize = 8;

/// Bits per kept fingerprint in the bitmap most rows fail first, and the
/// bitmap's bounds (512 B to 16 MiB).
const DUP_BITS_PER_KEPT: usize = 32;
const DUP_MIN_BITS: usize = 1 << 12;
const DUP_MAX_BITS: usize = 1 << 27;

/// Exact top-K by COUNT(*) over near-unique keys, where nearly every group
/// holds one row (finding duplicate IDs). CountSketch's counters can't tell
/// a group of two from collision noise there.
///
/// The tally sends each row's identity hash to one of 2^part_bits
/// partitions as a 31-bit fingerprint, and `settle` finds the fingerprints
/// a partition holds more than once, and `need` it holds once. The filter
/// pass groups the rows of those: every row of a group of two or more among
/// them, and `need` groups of one. A dropped group then holds one row, so
/// none outranks the need-th found (it may tie it); and with fewer than
/// `need` groups of one, every row is kept.
pub const DupSketch = struct {
    need: usize,
    part_bits: u6,
    /// Grows the workers' pages, from their threads during a tally.
    page_allocator: Allocator,
    workers: []Worker,
    /// Each row's fingerprint, per row group a tally saw whole, so the
    /// filter pass's scan drops the rows of the others before decoding them.
    row_slots: ?RowSlots = null,
    /// After `settle`: the kept hashes as `partition << 32 | fingerprint`,
    /// sorted; a bitmap by fingerprint that most rows fail first; and the
    /// kept fingerprints alone, ascending, for the scan's sieve.
    kept: []u64 = &.{},
    kept_bits: []u64 = &.{},
    kept_fingerprints: []u32 = &.{},
    /// Whether the groups of one were fewer than `need`, so all are kept.
    kept_all: bool = false,
    rows: u64 = 0,
    kept_rows: u64 = 0,

    const Part = struct {
        head: u32 = DUP_NO_PAGE,
        tail: u32 = DUP_NO_PAGE,
        tail_len: u32 = 0,
    };

    pub const Worker = struct {
        /// Per partition, the line it gathers and how much of it is full.
        lines: []align(64) [DUP_LINE]u32,
        line_len: []u8,
        parts: []Part,
        /// Per page, the next of its partition's.
        next_page: std.ArrayListUnmanaged(u32) = .empty,
        slabs: std.ArrayListUnmanaged([]u32) = .empty,
        rows: u64 = 0,
        /// What the worker's share of `settle` found, and its set.
        repeated: std.ArrayListUnmanaged(u64) = .empty,
        singles: std.ArrayListUnmanaged(u64) = .empty,
        kept_rows: u64 = 0,
        set: []u32 = &.{},
        err: ?anyerror = null,

        fn init(allocator: Allocator, parts: usize) !Worker {
            const lines = try allocator.alignedAlloc([DUP_LINE]u32, .@"64", parts);
            errdefer allocator.free(lines);
            const line_len = try allocator.alloc(u8, parts);
            errdefer allocator.free(line_len);
            const part_pages = try allocator.alloc(Part, parts);
            @memset(line_len, 0);
            @memset(part_pages, .{});
            return .{ .lines = lines, .line_len = line_len, .parts = part_pages };
        }

        fn deinitParts(self: *Worker, allocator: Allocator) void {
            allocator.free(self.lines);
            allocator.free(self.line_len);
            allocator.free(self.parts);
        }

        fn page(self: *const Worker, id: u32) []u32 {
            return self.slabs.items[id / DUP_SLAB_PAGES][(id % DUP_SLAB_PAGES) * DUP_PAGE ..][0..DUP_PAGE];
        }

        fn partLen(self: *const Worker, p: usize) usize {
            const at = self.parts[p];
            var n: usize = self.line_len[p];
            var id = at.head;
            while (id != DUP_NO_PAGE) : (id = self.next_page.items[id]) n += if (id == at.tail) at.tail_len else DUP_PAGE;
            return n;
        }

        fn freePages(self: *Worker, allocator: Allocator) void {
            for (self.slabs.items) |slab| allocator.free(slab);
            self.slabs.deinit(allocator);
            self.slabs = .empty;
            self.next_page.deinit(allocator);
            self.next_page = .empty;
        }

        /// Counts `fps`, partition `part`'s, into `set`, noting each
        /// fingerprint the moment it repeats.
        fn settleFingerprints(self: *Worker, allocator: Allocator, set: []u32, part: usize, fps: []const u32) !void {
            const mask = set.len - 1;
            for (fps, 0..) |fp, i| {
                if (i + DUP_PREFETCH < fps.len) @prefetch(&set[fps[i + DUP_PREFETCH] & mask], .{ .rw = .write });
                var slot: usize = fp & mask;
                while (true) : (slot = (slot + 1) & mask) {
                    const held = set[slot];
                    if (held == 0) {
                        set[slot] = fp;
                        break;
                    }
                    if (held & ~DUP_REPEATED != fp) continue;
                    if (held == fp) {
                        set[slot] = fp | DUP_REPEATED;
                        try self.repeated.append(allocator, @as(u64, part) << 32 | fp);
                        self.kept_rows += 2;
                    } else self.kept_rows += 1;
                    break;
                }
            }
        }

        /// Notes fingerprints `set` holds once, partition `part`'s, until
        /// the worker has `need`.
        fn pickSingles(self: *Worker, allocator: Allocator, set: []const u32, part: usize, need: usize) !void {
            for (set) |held| {
                if (self.singles.items.len >= need) return;
                if (held == 0 or held & DUP_REPEATED != 0) continue;
                try self.singles.append(allocator, @as(u64, part) << 32 | held);
            }
        }
    };

    /// Partitions sized for `rows` input rows, for `workers` workers.
    pub fn init(allocator: Allocator, page_allocator: Allocator, need: usize, workers: usize, rows: u64) !DupSketch {
        std.debug.assert(need > 0 and need <= MAX_NEED);
        const want = std.math.log2_int_ceil(u64, @max(1, rows / DUP_PART_TARGET));
        const part_bits: u6 = @intCast(std.math.clamp(want, DUP_MIN_PART_BITS, DUP_MAX_PART_BITS));
        const ws = try allocator.alloc(Worker, workers);
        var built: usize = 0;
        errdefer {
            for (ws[0..built]) |*w| w.deinitParts(allocator);
            allocator.free(ws);
        }
        for (ws) |*w| {
            w.* = try Worker.init(allocator, @as(usize, 1) << part_bits);
            built += 1;
        }
        return .{ .need = need, .part_bits = part_bits, .page_allocator = page_allocator, .workers = ws };
    }

    pub fn deinit(self: *DupSketch, allocator: Allocator) void {
        for (self.workers) |*w| {
            w.freePages(self.page_allocator);
            w.repeated.deinit(self.page_allocator);
            w.singles.deinit(self.page_allocator);
            self.page_allocator.free(w.set);
            w.deinitParts(allocator);
        }
        allocator.free(self.workers);
        if (self.row_slots) |*rs| rs.deinit();
        if (self.kept.len > 0) allocator.free(self.kept);
        if (self.kept_bits.len > 0) allocator.free(self.kept_bits);
        if (self.kept_fingerprints.len > 0) allocator.free(self.kept_fingerprints);
        self.* = undefined;
    }

    fn partition(self: *const DupSketch, hash: u64) usize {
        return @intCast(hash >> @intCast(64 - @as(u7, self.part_bits)));
    }

    /// The hash's low 31 bits, which the partition's high bits don't
    /// overlap. Zero marks an empty slot, so it becomes 1.
    fn fingerprint(hash: u64) u32 {
        const fp = @as(u32, @truncate(hash)) & ~DUP_REPEATED;
        return fp | @intFromBool(fp == 0);
    }

    /// Sends each identity's fingerprint to its partition, twice for a row
    /// standing for several (`weights[i]` > 1), and into `slots[i]` when
    /// given. Only worker `w` may tally for `w`.
    pub fn tally(self: *DupSketch, w: usize, ids: []const u128, weights: ?[]const u32, slots: ?[]u32) !void {
        const worker = &self.workers[w];
        for (ids, 0..) |id, i| {
            const fp = try self.tallyRun(worker, id, if (weights) |ws| ws[i] else 1);
            if (slots) |s| s[i] = fp;
        }
    }

    /// Tallies `rows` rows of one identity.
    pub fn tallyRows(self: *DupSketch, w: usize, identity: u128, rows: u64) !void {
        _ = try self.tallyRun(&self.workers[w], identity, rows);
    }

    inline fn tallyRun(self: *DupSketch, worker: *Worker, identity: u128, rows: u64) !u32 {
        const hash = identityHash(identity);
        const part = self.partition(hash);
        const fp = fingerprint(hash);
        try self.append(worker, part, fp);
        if (rows > 1) try self.append(worker, part, fp);
        worker.rows += rows;
        return fp;
    }

    inline fn append(self: *DupSketch, worker: *Worker, part: usize, fp: u32) !void {
        const n = worker.line_len[part];
        worker.lines[part][n] = fp;
        if (n + 1 < DUP_LINE) {
            worker.line_len[part] = n + 1;
            return;
        }
        try self.flushLine(worker, part);
        worker.line_len[part] = 0;
    }

    noinline fn flushLine(self: *DupSketch, worker: *Worker, part: usize) !void {
        const at = &worker.parts[part];
        if (at.tail == DUP_NO_PAGE or at.tail_len == DUP_PAGE) {
            const id = try self.newPage(worker);
            if (at.tail == DUP_NO_PAGE) at.head = id else worker.next_page.items[at.tail] = id;
            at.tail = id;
            at.tail_len = 0;
        }
        @memcpy(worker.page(at.tail)[at.tail_len..][0..DUP_LINE], &worker.lines[part]);
        at.tail_len += DUP_LINE;
    }

    fn newPage(self: *DupSketch, worker: *Worker) !u32 {
        const id: u32 = @intCast(worker.next_page.items.len);
        try worker.next_page.ensureUnusedCapacity(self.page_allocator, 1);
        if (id % DUP_SLAB_PAGES == 0) {
            const slab = try self.page_allocator.alloc(u32, DUP_PAGE * DUP_SLAB_PAGES);
            errdefer self.page_allocator.free(slab);
            try worker.slabs.append(self.page_allocator, slab);
        }
        worker.next_page.appendAssumeCapacity(DUP_NO_PAGE);
        return id;
    }

    /// Finds the fingerprints the partitions hold more than once, and
    /// `need` they hold once, one thread per worker, then frees the pages.
    /// False when the filter pass would keep too many rows to pay.
    pub fn settle(self: *DupSketch, allocator: Allocator) !bool {
        self.rows = 0;
        for (self.workers) |w| self.rows += w.rows;
        if (self.rows == 0) return false;

        var next = std.atomic.Value(usize).init(0);
        const helpers = try allocator.alloc(parallel.Helper, self.workers.len - 1);
        defer allocator.free(helpers);
        // A helper that doesn't start leaves its partitions to the others:
        // this thread claims partitions until none are left.
        var spawned: usize = 0;
        for (helpers, 1..) |*h, t| {
            h.* = parallel.spawn(settleWorker, .{ self, t, &next }) catch break;
            spawned += 1;
        }
        settleWorker(self, 0, &next);
        for (helpers[0..spawned]) |h| h.join();
        for (self.workers) |*w| w.freePages(self.page_allocator);
        for (self.workers) |w| {
            if (w.err) |e| return e;
        }

        // A worker stops at `need` groups of one, so fewer in all means
        // each found all of its partitions'.
        var repeated: usize = 0;
        var singles: usize = 0;
        self.kept_rows = 0;
        for (self.workers) |w| {
            repeated += w.repeated.items.len;
            singles += w.singles.items.len;
            self.kept_rows += w.kept_rows;
        }
        if (self.kept_rows * MAX_KEPT_FRACTION_DENOM > self.rows) return false;
        self.kept_all = singles < self.need;
        const chosen = @min(singles, self.need);
        self.kept_rows += chosen;

        const kept = try allocator.alloc(u64, repeated + chosen);
        errdefer allocator.free(kept);
        var at: usize = 0;
        for (self.workers) |w| {
            @memcpy(kept[at..][0..w.repeated.items.len], w.repeated.items);
            at += w.repeated.items.len;
        }
        for (self.workers) |w| {
            const take = @min(w.singles.items.len, kept.len - at);
            @memcpy(kept[at..][0..take], w.singles.items[0..take]);
            at += take;
        }
        std.sort.pdq(u64, kept, {}, std.sort.asc(u64));

        const bits = std.math.ceilPowerOfTwoAssert(usize, std.math.clamp(kept.len * DUP_BITS_PER_KEPT, DUP_MIN_BITS, DUP_MAX_BITS));
        const kept_bits = try allocator.alloc(u64, bits / 64);
        errdefer allocator.free(kept_bits);
        @memset(kept_bits, 0);
        const fingerprints = try allocator.alloc(u32, kept.len);
        errdefer allocator.free(fingerprints);
        for (kept, fingerprints) |k, *f| {
            f.* = @truncate(k);
            const b = f.* & (bits - 1);
            kept_bits[b >> 6] |= @as(u64, 1) << @intCast(b & 63);
        }
        std.sort.pdq(u32, fingerprints, {}, std.sort.asc(u32));
        var distinct: usize = 0;
        for (fingerprints) |f| {
            if (distinct > 0 and fingerprints[distinct - 1] == f) continue;
            fingerprints[distinct] = f;
            distinct += 1;
        }
        self.kept = kept;
        self.kept_bits = kept_bits;
        self.kept_fingerprints = try allocator.realloc(fingerprints, distinct);
        return true;
    }

    fn settleWorker(self: *DupSketch, t: usize, next: *std.atomic.Value(usize)) void {
        self.settlePartitions(t, next) catch |e| {
            self.workers[t].err = e;
        };
    }

    fn settlePartitions(self: *DupSketch, t: usize, next: *std.atomic.Value(usize)) !void {
        const me = &self.workers[t];
        const parts = @as(usize, 1) << self.part_bits;
        while (true) {
            const p = next.fetchAdd(1, .monotonic);
            if (p >= parts) return;
            var total: usize = 0;
            for (self.workers) |*w| total += w.partLen(p);
            if (total == 0) continue;
            const cap = std.math.ceilPowerOfTwoAssert(usize, total * 2);
            if (me.set.len < cap) {
                self.page_allocator.free(me.set);
                me.set = &.{};
                me.set = try self.page_allocator.alloc(u32, cap);
            }
            const set = me.set[0..cap];
            @memset(set, 0);
            for (self.workers) |*w| {
                const at = w.parts[p];
                var id = at.head;
                while (id != DUP_NO_PAGE) : (id = w.next_page.items[id]) {
                    const n = if (id == at.tail) at.tail_len else DUP_PAGE;
                    try me.settleFingerprints(self.page_allocator, set, p, w.page(id)[0..n]);
                }
                try me.settleFingerprints(self.page_allocator, set, p, w.lines[p][0..w.line_len[p]]);
            }
            if (me.singles.items.len < self.need) try me.pickSingles(self.page_allocator, set, p, self.need);
        }
    }

    /// Whether `identity`'s fingerprint is a kept one of its partition:
    /// true for every group of two rows or more, for the groups of one
    /// `settle` chose, and for the few others sharing a fingerprint.
    pub fn keeps(self: *const DupSketch, identity: u128) bool {
        const hash = identityHash(identity);
        const fp = fingerprint(hash);
        const b = fp & (self.kept_bits.len * 64 - 1);
        if ((self.kept_bits[b >> 6] >> @intCast(b & 63)) & 1 == 0) return false;
        const key = @as(u64, self.partition(hash)) << 32 | fp;
        return std.sort.binarySearch(u64, self.kept, key, orderU64) != null;
    }

    fn orderU64(a: u64, b: u64) std.math.Order {
        return std.math.order(a, b);
    }

    /// Whether a filter pass that found `found` groups found the top `need`.
    pub fn exact(self: *const DupSketch, found: usize) bool {
        return found >= self.need or self.kept_all;
    }
};

/// The lowest threshold up to `ceiling`, in steps of a 1/4096 of it, whose
/// counters hold at most `budget` rows; `ceiling` when none does.
fn lowestThreshold(totals: []const u32, ceiling: u64, budget: u64) u64 {
    const step = ceiling / BELOW_ROW_COUNT_THRESHOLD_STEPS + 1;
    const top = ceiling / step;
    var rows_at: [BELOW_ROW_COUNT_THRESHOLD_STEPS]u64 = @splat(0);
    for (totals) |c| rows_at[@min(c / step, top)] += c;
    var best = ceiling;
    var kept: u64 = 0;
    var b = top;
    while (b >= 1) : (b -= 1) {
        kept += rows_at[b];
        if (kept > budget) break;
        best = b * step;
    }
    return best;
}

/// Candidates past this many keep too many rows for the filter pass to beat
/// the counter sketch.
const MAX_HEAVY_CANDIDATES: usize = 1 << 16;

/// A unit's t - 1 is 1/this of its estimated share of τ, so the units'
/// slack sums to about τ / this.
const SLACK_SHARE_DENOM: u64 = 4;

/// A binary-search probe costs about this many rows of the signature
/// filter: the filter streams every row's bytes, a probe touches one row's.
const SEARCH_ROWS_PER_PROBE: usize = 8;

/// A tally whose units report more than 1/this of their rows grows tables
/// that settle then merges serially, past the cost of the counter sketch's
/// pass over every row. A short key's tally reporting 1/20 of its rows still
/// beats the sketch, so the bar sits well above that.
const MAX_REPORTS_SHARE_DENOM: u64 = 8;

/// Units the workers close, all told, before their reports may decline the
/// tally: a worker's first units report most of their keys, before its
/// partials know which keys lead.
const DECLINE_MIN_UNITS: u64 = 64;

/// Exact top-K over units whose keys arrive counted (TPUT: Cao & Wang,
/// "Efficient top-K query calculation in distributed networks", PODC 2004).
/// A row group's dictionary codes count its distinct values exactly, so a
/// tally reports only the values a unit holds at least `t` rows of and
/// hashes nothing else. A key's reported counts sum to its partial, and
/// each unit that didn't report it held at most its t - 1 rows of it:
///
///     partial(key) <= count(key) <= partial(key) + slack
///
/// with `slack` the units' summed t - 1. The need-th best partial τ is then
/// at most the need-th best count, so a key whose partial + slack falls
/// short of τ ranks strictly below the top `need`, ties included, and the
/// filter pass counts only the candidates whose partial + slack reach τ.
/// Its top `need` is exact; no retry is needed.
pub const HeavyKeys = struct {
    need: usize,
    workers: []Worker,
    top_keys: []u128,
    top_counts: []u64,
    /// The fewest rows a group needs to place at all: `settle` passes over
    /// every key that can't reach it, as over those that can't reach τ.
    floor: u64 = 0,
    /// At least the rows the tally sees, over which `floor` prorates.
    input_rows: u64 = 0,
    /// Set when the tally declined before scanning for want of input: the
    /// counter sketch would decline too.
    declined_small: bool = false,
    /// Set when it declined because the key is near-unique, which
    /// `DupSketch` serves.
    near_unique: bool = false,
    /// Set mid-pass when the units report too many of their keys (see
    /// `MAX_REPORTS_SHARE_DENOM`): the rest of the pass is wasted.
    declined_flat: std.atomic.Value(bool) = .init(false),
    /// Which rows the tally's filter passed, for the filter pass's scan.
    row_slots: ?RowSlots = null,
    /// The workers' closed units, their rows, and the keys they reported.
    tallied_units: std.atomic.Value(u64) = .init(0),
    tallied_rows: std.atomic.Value(u64) = .init(0),
    tallied_reports: std.atomic.Value(u64) = .init(0),
    /// After `settle`: the candidate keys, their signatures by open
    /// addressing (0 marks an empty slot), and the candidates by bytes when
    /// every candidate's bytes were kept.
    candidates: std.AutoHashMapUnmanaged(u128, void) = .empty,
    signatures: []u64 = &.{},
    signature_bits: u6 = 0,
    sorted: ?[]Candidate = null,

    pub const Worker = struct {
        partials: std.AutoHashMapUnmanaged(u128, Partial) = .empty,
        /// The bytes of the worker's partials' keys that reached
        /// `KEPT_BYTES_MIN_COUNT` rows.
        key_bytes: std.heap.ArenaAllocator,
        /// The keys and counts of the worker's `need` best partials, and
        /// the slot of the least.
        top_keys: []u128,
        top_counts: []u64,
        top_len: usize = 0,
        least: usize = 0,
        slack: u64 = 0,
        rows: u64 = 0,
        /// Keys reported since the worker's unit opened.
        unit_reports: u64 = 0,

        fn findLeast(self: *Worker) void {
            var least: usize = 0;
            for (self.top_counts[0..self.top_len], 0..) |c, i| {
                if (c < self.top_counts[least]) least = i;
            }
            self.least = least;
        }

        fn kth(self: *const Worker) u64 {
            return if (self.top_len == self.top_counts.len) self.top_counts[self.least] else 0;
        }
    };

    const Partial = struct { count: u64, signature: u64, slot: u32, bytes: ?[]const u8 };

    /// A near-unique unit reports every key it holds, so a key's bytes are
    /// kept once its partial reaches this many rows; a candidate holding
    /// fewer leaves the filter pass to the signatures.
    const KEPT_BYTES_MIN_COUNT: u64 = 2;
    const NO_SLOT = std.math.maxInt(u32);

    pub const Candidate = struct { bytes: []const u8, key: u128 };

    fn candidateBefore(_: void, a: Candidate, b: Candidate) bool {
        return std.mem.lessThan(u8, a.bytes, b.bytes);
    }

    pub fn init(allocator: Allocator, need: usize, workers: usize) !HeavyKeys {
        std.debug.assert(need > 0 and need <= MAX_NEED);
        const ws = try allocator.alloc(Worker, workers);
        errdefer allocator.free(ws);
        const top_keys = try allocator.alloc(u128, workers * need);
        errdefer allocator.free(top_keys);
        const top_counts = try allocator.alloc(u64, workers * need);
        for (ws, 0..) |*w, i| w.* = .{
            .key_bytes = .init(allocator),
            .top_keys = top_keys[i * need ..][0..need],
            .top_counts = top_counts[i * need ..][0..need],
        };
        return .{ .need = need, .workers = ws, .top_keys = top_keys, .top_counts = top_counts };
    }

    pub fn deinit(self: *HeavyKeys, allocator: Allocator) void {
        for (self.workers) |*w| {
            w.partials.deinit(allocator);
            w.key_bytes.deinit();
        }
        allocator.free(self.workers);
        allocator.free(self.top_keys);
        allocator.free(self.top_counts);
        self.candidates.deinit(allocator);
        if (self.signatures.len > 0) allocator.free(self.signatures);
        if (self.sorted) |sorted| allocator.free(sorted);
        if (self.row_slots) |*rs| rs.deinit();
        self.* = undefined;
    }

    /// The threshold for a unit of `rows` rows whose need-th best key holds
    /// `unit_kth` of them: t - 1 is a quarter of the lesser of two estimates
    /// of the unit's share of τ. The unit's own need-th count overshoots it
    /// where hot keys come and go between units; worker `w`'s need-th best
    /// partial prorated by rows undershoots it early on, which costs only
    /// hashing. A floor above τ raises the share to the floor's, prorated
    /// over the input.
    pub fn unitThreshold(self: *const HeavyKeys, w: usize, rows: u64, unit_kth: u64) u64 {
        const worker = &self.workers[w];
        var share = unit_kth;
        if (worker.rows > 0) share = @min(share, @as(u64, @intCast(@as(u128, worker.kth()) * rows / worker.rows)));
        if (self.input_rows > 0) share = @max(share, @as(u64, @intCast(@as(u128, self.floor) * @min(rows, self.input_rows) / self.input_rows)));
        return 1 + share / SLACK_SHARE_DENOM;
    }

    /// Adds a unit's `count` rows of `key`, the digest of `bytes`, to
    /// worker `w`'s partials. Only worker `w` may call this.
    pub fn report(self: *HeavyKeys, allocator: Allocator, w: usize, key: u128, bytes: []const u8, count: u64) !void {
        const worker = &self.workers[w];
        worker.unit_reports += 1;
        const gop = try worker.partials.getOrPut(allocator, key);
        if (!gop.found_existing) gop.value_ptr.* = .{ .count = 0, .signature = keySignature(bytes), .slot = NO_SLOT, .bytes = null };
        const p = gop.value_ptr;
        p.count += count;
        if (p.bytes == null and p.count >= KEPT_BYTES_MIN_COUNT) p.bytes = try worker.key_bytes.allocator().dupe(u8, bytes);
        if (p.slot != NO_SLOT) {
            worker.top_counts[p.slot] = p.count;
            if (p.slot == worker.least) worker.findLeast();
            return;
        }
        if (worker.top_len < self.need) {
            p.slot = @intCast(worker.top_len);
            worker.top_keys[worker.top_len] = key;
            worker.top_counts[worker.top_len] = p.count;
            worker.top_len += 1;
            worker.findLeast();
            return;
        }
        if (p.count <= worker.top_counts[worker.least]) return;
        // A top key never leaves the partials.
        worker.partials.getPtr(worker.top_keys[worker.least]).?.slot = NO_SLOT;
        p.slot = @intCast(worker.least);
        worker.top_keys[worker.least] = key;
        worker.top_counts[worker.least] = p.count;
        worker.findLeast();
    }

    /// Closes worker `w`'s unit of `rows` rows, which reported every key it
    /// held `threshold` rows of or more.
    pub fn endUnit(self: *HeavyKeys, w: usize, rows: u64, threshold: u64) void {
        const worker = &self.workers[w];
        worker.slack += threshold - 1;
        worker.rows += rows;
        // Pooled over the workers: one worker's units can report many keys
        // early on, before its need-th best partial nears its share of τ.
        const reports = self.tallied_reports.fetchAdd(worker.unit_reports, .monotonic) + worker.unit_reports;
        const tallied_rows = self.tallied_rows.fetchAdd(rows, .monotonic) + rows;
        worker.unit_reports = 0;
        const units = self.tallied_units.fetchAdd(1, .monotonic) + 1;
        if (units >= DECLINE_MIN_UNITS and reports * MAX_REPORTS_SHARE_DENOM > tallied_rows) {
            self.declined_flat.store(true, .monotonic);
        }
    }

    pub fn declined(self: *const HeavyKeys) bool {
        return self.declined_flat.load(.monotonic);
    }

    /// Merges the workers' partials of the keys that may place and picks
    /// the candidates. False when the slack reaches τ (a key no unit
    /// reported might then rank) or the candidates are too many to pay.
    pub fn settle(self: *HeavyKeys, allocator: Allocator) !bool {
        var slack: u64 = 0;
        for (self.workers) |*w| slack += w.slack;
        var merged = try self.mergeReachable(allocator, slack);
        defer merged.deinit(allocator);
        const counts = try allocator.alloc(u64, merged.count());
        defer allocator.free(counts);
        var i: usize = 0;
        var values = merged.valueIterator();
        while (values.next()) |p| : (i += 1) counts[i] = p.count;
        var best: [MAX_NEED]u64 = undefined;
        const tau = @max(kthLargest(u64, counts, best[0..self.need]), self.floor);
        if (tau <= slack) return false;

        var n: usize = 0;
        for (counts) |c| n += @intFromBool(c + slack >= tau);
        if (n > MAX_HEAVY_CANDIDATES) return false;
        try self.candidates.ensureTotalCapacity(allocator, @intCast(n));
        self.signature_bits = @intCast(@max(6, std.math.log2_int_ceil(usize, @max(1, 2 * n))));
        self.signatures = try allocator.alloc(u64, @as(usize, 1) << self.signature_bits);
        @memset(self.signatures, 0);
        const sorted = try allocator.alloc(Candidate, n);
        var j: usize = 0;
        var it = merged.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.count + slack < tau) continue;
            self.candidates.putAssumeCapacity(e.key_ptr.*, {});
            self.addSignature(e.value_ptr.signature);
            if (e.value_ptr.bytes) |bytes| {
                sorted[j] = .{ .bytes = bytes, .key = e.key_ptr.* };
                j += 1;
            }
        }
        if (j < n) {
            allocator.free(sorted);
            return true;
        }
        std.sort.pdq(Candidate, sorted, {}, candidateBefore);
        self.sorted = sorted;
        return true;
    }

    /// The merged partials of the keys whose partials may reach τ - slack,
    /// with τ the need-th best partial or `floor`: every key `settle` can
    /// pick, and the need best when they reach it (τ is `floor` when they
    /// don't). The workers' own best keys bound τ from below by `lower`,
    /// and a key whose partial reaches lower - slack has a worker holding a
    /// workers-th of that, so only such keys' partials merge; all of them
    /// when that share is a row or less.
    fn mergeReachable(self: *const HeavyKeys, allocator: Allocator, slack: u64) !std.AutoHashMapUnmanaged(u128, Partial) {
        var merged: std.AutoHashMapUnmanaged(u128, Partial) = .empty;
        errdefer merged.deinit(allocator);
        const lower = @max(try self.bestKeysBound(allocator), self.floor);
        const span = lower -| slack;
        if (span <= self.workers.len) {
            var reported: usize = 0;
            for (self.workers) |*w| reported += w.partials.count();
            try merged.ensureTotalCapacity(allocator, @intCast(reported));
            for (self.workers) |*w| {
                var it = w.partials.iterator();
                while (it.next()) |e| {
                    const gop = merged.getOrPutAssumeCapacity(e.key_ptr.*);
                    if (gop.found_existing) {
                        gop.value_ptr.count += e.value_ptr.count;
                        if (gop.value_ptr.bytes == null) gop.value_ptr.bytes = e.value_ptr.bytes;
                    } else gop.value_ptr.* = e.value_ptr.*;
                }
            }
            return merged;
        }
        for (self.workers) |*w| {
            var it = w.partials.iterator();
            while (it.next()) |e| {
                if (e.value_ptr.count *| self.workers.len < span) continue;
                const gop = try merged.getOrPut(allocator, e.key_ptr.*);
                if (!gop.found_existing) gop.value_ptr.* = self.mergedPartial(e.key_ptr.*);
            }
        }
        return merged;
    }

    /// The need-th best merged partial of the keys some worker ranks among
    /// its own `need` best (0 when they are fewer): at most τ, being the
    /// need-th best of some of the keys.
    fn bestKeysBound(self: *const HeavyKeys, allocator: Allocator) !u64 {
        var seen: std.AutoHashMapUnmanaged(u128, void) = .empty;
        defer seen.deinit(allocator);
        try seen.ensureTotalCapacity(allocator, @intCast(self.top_keys.len));
        const partials = try allocator.alloc(u64, self.top_keys.len);
        defer allocator.free(partials);
        var n: usize = 0;
        for (self.workers) |*w| {
            for (w.top_keys[0..w.top_len]) |key| {
                if (seen.getOrPutAssumeCapacity(key).found_existing) continue;
                partials[n] = self.mergedPartial(key).count;
                n += 1;
            }
        }
        var best: [MAX_NEED]u64 = undefined;
        return kthLargest(u64, partials[0..n], best[0..self.need]);
    }

    /// `key`'s partials summed over the workers.
    fn mergedPartial(self: *const HeavyKeys, key: u128) Partial {
        var merged: Partial = .{ .count = 0, .signature = 0, .slot = NO_SLOT, .bytes = null };
        for (self.workers) |*w| {
            const p = w.partials.get(key) orelse continue;
            merged.count += p.count;
            merged.signature = p.signature;
            if (merged.bytes == null) merged.bytes = p.bytes;
        }
        return merged;
    }

    /// Whether binary searches for the candidates beat signing every row of
    /// a batch of `rows` ascending distinct keys.
    pub fn searchPays(self: *const HeavyKeys, rows: usize) bool {
        const sorted = self.sorted orelse return false;
        const probes = sorted.len * (std.math.log2_int_ceil(usize, rows + 1) + 1);
        return probes * SEARCH_ROWS_PER_PROBE <= rows;
    }

    /// Marks the rows of `keys` (a `rowBytes` source of `mask.len`
    /// ascending distinct keys) that are candidates, with their digests in
    /// `ids`, and returns how many: the sorted candidates' searches each
    /// start past the last one's row. Only once `searchPays`.
    pub fn findSorted(self: *const HeavyKeys, keys: anytype, mask: []bool, ids: []u128) usize {
        @memset(mask, false);
        var lo: usize = 0;
        var found: usize = 0;
        for (self.sorted.?) |c| {
            var hi = mask.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (std.mem.lessThan(u8, keys.rowBytes(mid), c.bytes)) lo = mid + 1 else hi = mid;
            }
            if (lo == mask.len) break;
            if (!std.mem.eql(u8, keys.rowBytes(lo), c.bytes)) continue;
            mask[lo] = true;
            ids[lo] = c.key;
            found += 1;
            lo += 1;
        }
        return found;
    }

    fn addSignature(self: *HeavyKeys, signature: u64) void {
        const mask = self.signatures.len - 1;
        var i: usize = @intCast(signature >> @intCast(64 - @as(u7, self.signature_bits)));
        while (self.signatures[i] != 0) : (i = (i + 1) & mask) {
            if (self.signatures[i] == signature) return;
        }
        self.signatures[i] = signature;
    }

    /// Whether some candidate signs as `signature`: a key that doesn't is
    /// no candidate, and needs no digest to tell.
    pub fn maySign(self: *const HeavyKeys, signature: u64) bool {
        const mask = self.signatures.len - 1;
        var i: usize = @intCast(signature >> @intCast(64 - @as(u7, self.signature_bits)));
        while (true) : (i = (i + 1) & mask) {
            const s = self.signatures[i];
            if (s == signature) return true;
            if (s == 0) return false;
        }
    }

    pub fn keeps(self: *const HeavyKeys, key: u128) bool {
        return self.candidates.contains(key);
    }
};

/// A filter on a key's bytes ahead of its digest: its length and its first
/// and last 8 bytes, mixed. Never 0, the empty signature slot.
pub fn keySignature(bytes: []const u8) u64 {
    var head: u64 = 0;
    var tail: u64 = 0;
    if (bytes.len >= 8) {
        head = std.mem.readInt(u64, bytes[0..8], .little);
        tail = std.mem.readInt(u64, bytes[bytes.len - 8 ..][0..8], .little);
    } else {
        var buf: [8]u8 = @splat(0);
        @memcpy(buf[0..bytes.len], bytes);
        head = std.mem.readInt(u64, &buf, .little);
    }
    var x = head ^ std.math.rotl(u64, tail, 29) ^ (@as(u64, bytes.len) *% 0x9e3779b97f4a7c15);
    x ^= x >> 32;
    x *%= 0xd6e8feb86659fd93;
    x ^= x >> 32;
    return x | 1;
}

/// A lower bound within 2x on the need-th largest of `counts` (0 when fewer
/// than `need` are nonzero): the power of two the need-th largest reaches.
pub fn approxKth(counts: []const u32, need: usize) u64 {
    var by_log: [32]u32 = @splat(0);
    for (counts) |c| {
        if (c != 0) by_log[std.math.log2_int(u32, c)] += 1;
    }
    var seen: usize = 0;
    var b: usize = by_log.len;
    while (b > 0) {
        b -= 1;
        seen += by_log[b];
        if (seen >= need) return @as(u64, 1) << @intCast(b);
    }
    return 0;
}

/// About the largest counter `rows` rows of distinct groups reach by
/// collisions alone: each of the 2^bits counters draws ~Poisson(rows/2^bits)
/// rows, and the most loaded lands near mean + sqrt(2·mean·ln n), with a
/// ln n tail when the mean is small. A need-th largest counter at or below
/// it may hold no group bigger than one row, and a filter pass under it
/// keeps a slice of noise that rarely proves exact.
fn collisionCeiling(rows: u64, bits: u6) f64 {
    const counters: f64 = @floatFromInt(@as(u64, 1) << bits);
    const mean = @as(f64, @floatFromInt(rows)) / counters;
    const log_n = @log(counters);
    return mean + @sqrt(2 * mean * log_n) + log_n;
}

/// The `best.len`-th largest of `values` (0 when there are fewer), using
/// `best` as scratch kept in descending order.
fn kthLargest(comptime T: type, values: []const T, best: []T) T {
    const k = best.len;
    var len: usize = 0;
    for (values) |v| {
        if (len == k and v <= best[k - 1]) continue;
        var i = if (len < k) len else k - 1;
        while (i > 0 and best[i - 1] < v) : (i -= 1) best[i] = best[i - 1];
        best[i] = v;
        if (len < k) len += 1;
    }
    return if (len == k) best[k - 1] else 0;
}

test "kthLargest picks the k-th largest value" {
    var best: [3]u32 = undefined;
    try std.testing.expectEqual(@as(u32, 7), kthLargest(u32, &.{ 5, 9, 1, 7, 8, 2 }, &best));
    try std.testing.expectEqual(@as(u32, 0), kthLargest(u32, &.{ 4, 6 }, &best));
    var one: [1]u32 = undefined;
    try std.testing.expectEqual(@as(u32, 9), kthLargest(u32, &.{ 5, 9, 1 }, &one));
}

test "count sketch keeps the heavy groups and proves the top-k exact" {
    const allocator = std.testing.allocator;
    var sketch = try CountSketch.init(allocator, 10, 2);
    defer sketch.deinit(allocator);

    // Groups 0..3 hold 1000, 900, 800, 700 rows; groups 4.. hold one row each.
    var expected = std.AutoHashMap(u128, u64).init(allocator);
    defer expected.deinit();
    const heavy = [_]u64{ 1000, 900, 800, 700 };
    for (heavy, 0..) |n, g| {
        const counts = sketch.workerCounts(g % 2);
        counts[sketch.counter(g)] += @intCast(n);
        try expected.put(g, n);
    }
    var g: u128 = 4;
    while (g < 20_004) : (g += 1) {
        const counts = sketch.workerCounts(@intCast(g % 2));
        counts[sketch.counter(g)] += 1;
        try expected.put(g, 1);
    }

    try std.testing.expect(try sketch.settle(allocator, 3, .row_count));
    try std.testing.expectEqual(@as(u64, 23_400), sketch.rows);
    for (0..4) |h| try std.testing.expect(sketch.keeps(h));
    try std.testing.expect(sketch.kept_rows < sketch.rows / 4);

    // The kept groups' 3rd best has 800 rows: well over the threshold.
    try std.testing.expect(sketch.exact(3, 4, 800));
    try std.testing.expect(!sketch.exact(3, 2, 900));
    try std.testing.expect(!sketch.exact(3, 4, sketch.threshold - 2));
}

test "tally counts like one increment per identity, by weight when given" {
    const allocator = std.testing.allocator;
    var sketch = try CountSketch.init(allocator, 9, 3);
    defer sketch.deinit(allocator);
    var prng = std.Random.DefaultPrng.init(0x7a11);
    const random = prng.random();

    const n = 2 * TALLY_LANE + 37;
    const ids = try allocator.alloc(u128, n);
    defer allocator.free(ids);
    const weights = try allocator.alloc(u32, n);
    defer allocator.free(weights);
    for (ids, weights) |*id, *w| {
        id.* = random.uintLessThan(u128, 3000);
        w.* = random.intRangeAtMost(u32, 1, 9);
    }

    const recorded = try allocator.alloc(u32, n);
    defer allocator.free(recorded);
    sketch.tally(sketch.workerCounts(0), ids, null, recorded);
    sketch.tally(sketch.workerCounts(1), ids, weights, null);
    const expected_rows = try allocator.alloc(u32, @as(usize, 1) << sketch.bits);
    defer allocator.free(expected_rows);
    const expected_weighted = try allocator.alloc(u32, expected_rows.len);
    defer allocator.free(expected_weighted);
    @memset(expected_rows, 0);
    @memset(expected_weighted, 0);
    for (ids, weights) |id, w| {
        expected_rows[sketch.counter(id)] += 1;
        expected_weighted[sketch.counter(id)] += w;
    }
    try std.testing.expectEqualSlices(u32, expected_rows, sketch.workerCounts(0));
    try std.testing.expectEqualSlices(u32, expected_weighted, sketch.workerCounts(1));
    for (ids, recorded) |id, slot| try std.testing.expectEqual(sketch.counter(id), slot);

    sketch.tally(sketch.workerCounts(2), ids[0..0], null, null);
    for (sketch.workerCounts(2)) |c| try std.testing.expectEqual(@as(u32, 0), c);
}

test "count sketch declines near-unique keys" {
    const allocator = std.testing.allocator;
    var sketch = try CountSketch.init(allocator, 8, 1);
    defer sketch.deinit(allocator);
    const counts = sketch.workerCounts(0);
    var g: u128 = 0;
    while (g < 100_000) : (g += 1) counts[sketch.counter(g)] += 1;
    try std.testing.expect(!try sketch.settle(allocator, 10, .row_count));
}

test "count sketch declines unique keys spread thinner than the counters" {
    // ~1.2 rows per counter: a filter pass at half the 5th-largest counter
    // would keep a small slice of rows, all of them one-row groups.
    const allocator = std.testing.allocator;
    var sketch = try CountSketch.init(allocator, 12, 1);
    defer sketch.deinit(allocator);
    const counts = sketch.workerCounts(0);
    var g: u128 = 0;
    while (g < 5_000) : (g += 1) counts[sketch.counter(g)] += 1;
    try std.testing.expect(!try sketch.settle(allocator, 5, .row_count));
}

test "retarget lowers the threshold to one past the found k-th count" {
    const allocator = std.testing.allocator;
    var sketch = try CountSketch.init(allocator, 8, 1);
    defer sketch.deinit(allocator);
    const counts = sketch.workerCounts(0);
    counts[sketch.counter(1)] += 500;
    counts[sketch.counter(2)] += 400;
    var g: u128 = 10;
    while (g < 5_000) : (g += 1) counts[sketch.counter(g)] += 1;
    try std.testing.expect(try sketch.settle(allocator, 2, .row_count));
    try std.testing.expect(sketch.retarget(399));
    try std.testing.expectEqual(@as(u64, 400), sketch.threshold);
    try std.testing.expect(sketch.keeps(1) and sketch.keeps(2));
    try std.testing.expect(sketch.exact(2, 2, 399));
}

test "a count below the row count settles as low as the kept-row budget allows" {
    const allocator = std.testing.allocator;
    var sketch = try CountSketch.init(allocator, 16, 1);
    defer sketch.deinit(allocator);
    const counts = sketch.workerCounts(0);
    // Two groups of 4000 rows, then groups of 800, 400 and 200, among
    // one-row groups: half the 2nd largest counter is about 2000, and 1/16
    // of the 148,000 rows is 9250.
    counts[sketch.counter(1)] += 4000;
    counts[sketch.counter(2)] += 4000;
    counts[sketch.counter(3)] += 800;
    counts[sketch.counter(4)] += 400;
    counts[sketch.counter(5)] += 200;
    var g: u128 = 10;
    while (g < 138_610) : (g += 1) counts[sketch.counter(g)] += 1;
    try std.testing.expect(try sketch.settle(allocator, 2, .at_most_row_count));
    try std.testing.expectEqual(@as(u64, 148_000), sketch.rows);
    // The 800- and 400-row groups fit the budget beside the two largest; the
    // 200-row one would pass it.
    try std.testing.expect(sketch.threshold > counts[sketch.counter(5)] and sketch.threshold <= counts[sketch.counter(4)]);
    for (1..5) |h| try std.testing.expect(sketch.keeps(h));
    try std.testing.expect(!sketch.keeps(5));
}

test "approxKth bounds the need-th largest count from below within 2x" {
    try std.testing.expectEqual(@as(u64, 8), approxKth(&.{ 3, 40, 9, 0, 12, 1 }, 3));
    try std.testing.expectEqual(@as(u64, 32), approxKth(&.{ 3, 40, 9, 0, 12, 1 }, 1));
    try std.testing.expectEqual(@as(u64, 1), approxKth(&.{ 3, 40, 9, 0, 12, 1 }, 5));
    try std.testing.expectEqual(@as(u64, 0), approxKth(&.{ 3, 40, 9, 0, 12, 1 }, 6));
    try std.testing.expectEqual(@as(u64, 0), approxKth(&.{}, 1));
}

test "keySignature reads short, long and empty keys" {
    try std.testing.expect(keySignature("") != keySignature("a"));
    try std.testing.expect(keySignature("abc") != keySignature("abd"));
    try std.testing.expect(keySignature("http://example.com/a") != keySignature("http://example.com/b"));
    try std.testing.expect(keySignature("abcdefgh") != keySignature("abcdefghabcdefgh"));
    try std.testing.expectEqual(keySignature("same bytes here"), keySignature("same bytes here"));
    for ([_][]const u8{ "", "x", "12345678", "a much longer key than eight bytes" }) |k| try std.testing.expect(keySignature(k) & 1 == 1);
}

/// Test key k's bytes: "k" and its decimal digits.
fn testKeyBytes(buf: []u8, key: u128) []const u8 {
    return std.fmt.bufPrint(buf, "k{d}", .{key}) catch unreachable;
}

/// Feeds `units` (per unit, each key's row count) through a heavy tally the
/// way grid workers do: unit u on worker u % workers.
fn heavyTallyUnits(allocator: Allocator, heavy: *HeavyKeys, units: []const []const [2]u64, workers: usize) !void {
    var buf: [48]u8 = undefined;
    const counts = try allocator.alloc(u32, 4096);
    defer allocator.free(counts);
    for (units, 0..) |unit, u| {
        const w = u % workers;
        var rows: u64 = 0;
        for (unit, 0..) |kc, i| {
            counts[i] = @intCast(kc[1]);
            rows += kc[1];
        }
        const t = heavy.unitThreshold(w, rows, approxKth(counts[0..unit.len], heavy.need));
        for (unit) |kc| {
            if (kc[1] >= t) try heavy.report(allocator, w, kc[0], testKeyBytes(&buf, kc[0]), kc[1]);
        }
        heavy.endUnit(w, rows, t);
    }
}

test "heavy keys keep every key that can reach the top and prove the rest can't" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x7e47);
    const random = prng.random();
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // 60 units of 1000 keys: keys 0..5 are hot everywhere, keys 6..25 warm
    // in a third of the units, and the rest one-offs.
    var totals = std.AutoHashMap(u128, u64).init(allocator);
    defer totals.deinit();
    const units = try arena.alloc([]const [2]u64, 60);
    var next_light: u64 = 1000;
    for (units, 0..) |*unit, u| {
        const keys = try arena.alloc([2]u64, 1000);
        for (keys, 0..) |*kc, i| {
            kc.* = if (i < 6)
                .{ i, 40 + 7 * (5 - i) + random.uintLessThan(u64, 5) }
            else if (i < 26 and u % 3 == i % 3)
                .{ i, 20 + random.uintLessThan(u64, 30) }
            else blk: {
                next_light += 1;
                break :blk .{ next_light, 1 + random.uintLessThan(u64, 3) };
            };
            const gop = try totals.getOrPut(kc[0]);
            if (!gop.found_existing) gop.value_ptr.* = 0;
            gop.value_ptr.* += kc[1];
        }
        unit.* = keys;
    }

    inline for (.{ @as(usize, 1), @as(usize, 3) }) |workers| {
        var heavy = try HeavyKeys.init(allocator, 4, workers);
        defer heavy.deinit(allocator);
        try heavyTallyUnits(allocator, &heavy, units, workers);
        try std.testing.expect(try heavy.settle(allocator));
        try std.testing.expect(heavy.candidates.count() < 100);

        var all: std.ArrayList(u64) = .empty;
        defer all.deinit(allocator);
        var it = totals.valueIterator();
        while (it.next()) |c| try all.append(allocator, c.*);
        std.sort.pdq(u64, all.items, {}, std.sort.desc(u64));
        const kth = all.items[3];
        var buf: [48]u8 = undefined;
        var kit = totals.iterator();
        while (kit.next()) |e| {
            const key = e.key_ptr.*;
            if (e.value_ptr.* >= kth) try std.testing.expect(heavy.keeps(key));
            if (heavy.keeps(key)) try std.testing.expect(heavy.maySign(keySignature(testKeyBytes(&buf, key))));
        }
        const sorted = heavy.sorted.?;
        try std.testing.expectEqual(heavy.candidates.count(), sorted.len);
        for (sorted, 0..) |c, i| {
            try std.testing.expect(heavy.keeps(c.key));
            try std.testing.expectEqualStrings(testKeyBytes(&buf, c.key), c.bytes);
            if (i > 0) try std.testing.expect(std.mem.lessThan(u8, sorted[i - 1].bytes, c.bytes));
        }
    }
}

test "heavy keys merge only the partials that may place, picking what a full merge picks" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x5e77);
    const random = prng.random();
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // 96 units: keys 0..7 hot everywhere, keys 8..39 spread over every unit
    // with totals straddling the cut, and one-offs. A spread key near the
    // cut holds about the per-worker share on some workers and less on the
    // others.
    const units = try arena.alloc([]const [2]u64, 96);
    var input_rows: u64 = 0;
    var next_light: u64 = 1000;
    for (units) |*unit| {
        const keys = try arena.alloc([2]u64, 340);
        for (keys, 0..) |*kc, i| {
            kc.* = if (i < 8)
                .{ i, 60 + random.uintLessThan(u64, 20) }
            else if (i < 40)
                .{ i, 40 + (i - 8) * 20 / 32 + random.uintLessThan(u64, 10) }
            else blk: {
                next_light += 1;
                break :blk .{ next_light, 1 + random.uintLessThan(u64, 2) };
            };
            input_rows += kc[1];
        }
        unit.* = keys;
    }

    var settled: usize = 0;
    inline for (.{ @as(usize, 1), @as(usize, 3), @as(usize, 8) }) |workers| {
        for ([_]u64{ 0, 7600, 9000 }) |floor| {
            var heavy = try HeavyKeys.init(allocator, 6, workers);
            defer heavy.deinit(allocator);
            heavy.floor = floor;
            heavy.input_rows = input_rows;
            try heavyTallyUnits(allocator, &heavy, units, workers);

            var merged = std.AutoHashMap(u128, u64).init(allocator);
            defer merged.deinit();
            var slack: u64 = 0;
            for (heavy.workers) |*w| {
                slack += w.slack;
                var it = w.partials.iterator();
                while (it.next()) |e| {
                    const gop = try merged.getOrPut(e.key_ptr.*);
                    if (!gop.found_existing) gop.value_ptr.* = 0;
                    gop.value_ptr.* += e.value_ptr.count;
                }
            }
            var partials: std.ArrayList(u64) = .empty;
            defer partials.deinit(allocator);
            var values = merged.valueIterator();
            while (values.next()) |p| try partials.append(allocator, p.*);
            std.sort.pdq(u64, partials.items, {}, std.sort.desc(u64));
            const tau = @max(partials.items[heavy.need - 1], floor);

            try std.testing.expectEqual(tau > slack, try heavy.settle(allocator));
            if (tau <= slack) continue;
            settled += 1;
            var expected: usize = 0;
            var it = merged.iterator();
            while (it.next()) |e| {
                const can_place = e.value_ptr.* + slack >= tau;
                expected += @intFromBool(can_place);
                try std.testing.expectEqual(can_place, heavy.keeps(e.key_ptr.*));
            }
            try std.testing.expectEqual(expected, heavy.candidates.count());
        }
    }
    try std.testing.expect(settled >= 6);
}

test "heavy keys decline when the slack reaches the need-th partial" {
    // Every unit holds 50 fresh keys of 8 rows, one unit per worker: each
    // worker's lone unit has t = 3, so the four units' slack (8) reaches the
    // best partial (8), and a key no unit reported could tie it.
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const units = try arena.alloc([]const [2]u64, 4);
    for (units, 0..) |*unit, u| {
        const keys = try arena.alloc([2]u64, 50);
        for (keys, 0..) |*kc, i| kc.* = .{ u * 50 + i, 8 };
        unit.* = keys;
    }
    var heavy = try HeavyKeys.init(allocator, 2, 4);
    defer heavy.deinit(allocator);
    try heavyTallyUnits(allocator, &heavy, units, 4);
    try std.testing.expect(!try heavy.settle(allocator));
}

test "heavy keys decline once the units report too many of their keys" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    // 300 one-row keys a unit: t = 1, so a unit reports every row's key.
    const even = try arena.alloc([]const [2]u64, DECLINE_MIN_UNITS);
    for (even, 0..) |*unit, u| {
        const keys = try arena.alloc([2]u64, 300);
        for (keys, 0..) |*kc, i| kc.* = .{ u * 300 + i, 1 };
        unit.* = keys;
    }
    var flat = try HeavyKeys.init(allocator, 10, 1);
    defer flat.deinit(allocator);
    try heavyTallyUnits(allocator, &flat, even[0 .. even.len - 1], 1);
    try std.testing.expect(!flat.declined());
    try heavyTallyUnits(allocator, &flat, even[even.len - 1 ..], 1);
    try std.testing.expect(flat.declined());

    // Two keys of 1000 rows beside the one-row keys: t passes over the light
    // keys, and the tally goes on.
    const skewed = try arena.alloc([]const [2]u64, 2 * DECLINE_MIN_UNITS);
    for (skewed, 0..) |*unit, u| {
        const keys = try arena.alloc([2]u64, 302);
        keys[0] = .{ 1 << 40, 1000 };
        keys[1] = .{ (1 << 40) + 1, 1000 };
        for (keys[2..], 0..) |*kc, i| kc.* = .{ u * 300 + i, 1 };
        unit.* = keys;
    }
    var heavy = try HeavyKeys.init(allocator, 2, 1);
    defer heavy.deinit(allocator);
    try heavyTallyUnits(allocator, &heavy, skewed, 1);
    try std.testing.expect(!heavy.declined());
}

test "heavy keys track each worker's need-th best partial as keys overtake it" {
    const allocator = std.testing.allocator;
    var heavy = try HeavyKeys.init(allocator, 2, 1);
    defer heavy.deinit(allocator);
    try heavy.report(allocator, 0, 1, "k1", 10);
    try std.testing.expectEqual(@as(u64, 0), heavy.workers[0].kth());
    try heavy.report(allocator, 0, 2, "k2", 5);
    try std.testing.expectEqual(@as(u64, 5), heavy.workers[0].kth());
    try heavy.report(allocator, 0, 3, "k3", 7);
    try std.testing.expectEqual(@as(u64, 7), heavy.workers[0].kth());
    try heavy.report(allocator, 0, 2, "k2", 6);
    try std.testing.expectEqual(@as(u64, 10), heavy.workers[0].kth());
    try heavy.report(allocator, 0, 1, "k1", 1);
    try std.testing.expectEqual(@as(u64, 11), heavy.workers[0].kth());
    heavy.endUnit(0, 100, 1);
    // A 50-row unit's share of the 11-row second best is 5; its own second
    // best (40) is larger, so t - 1 is a quarter of 5.
    try std.testing.expectEqual(@as(u64, 2), heavy.unitThreshold(0, 50, 40));
}

test "heavy keys find the candidates among ascending distinct keys by search" {
    const allocator = std.testing.allocator;
    var heavy = try HeavyKeys.init(allocator, 2, 2);
    defer heavy.deinit(allocator);
    // Keys "b", "d" and "f" are hot on both workers.
    const hot = [_][]const u8{ "b", "d", "f" };
    for (0..2) |w| {
        for (hot, 0..) |k, i| try heavy.report(allocator, w, 100 + i, k, 50);
        heavy.endUnit(w, 200, 1);
    }
    try std.testing.expect(try heavy.settle(allocator));
    try std.testing.expectEqual(@as(usize, 3), heavy.sorted.?.len);

    const Keys = struct {
        values: []const []const u8,
        fn rowBytes(self: @This(), r: usize) []const u8 {
            return self.values[r];
        }
    };
    const Case = struct { rows: []const []const u8, want: []const usize };
    const cases = [_]Case{
        .{ .rows = &.{ "a", "b", "c", "d", "e", "f", "g" }, .want = &.{ 1, 3, 5 } },
        .{ .rows = &.{ "c", "d", "e" }, .want = &.{1} },
        .{ .rows = &.{ "a", "c", "e", "g" }, .want = &.{} },
        .{ .rows = &.{ "b", "f" }, .want = &.{ 0, 1 } },
        .{ .rows = &.{}, .want = &.{} },
    };
    var mask: [8]bool = undefined;
    var ids: [8]u128 = undefined;
    for (cases) |c| {
        const n_rows = c.rows.len;
        const found = heavy.findSorted(Keys{ .values = c.rows }, mask[0..n_rows], ids[0..n_rows]);
        try std.testing.expectEqual(c.want.len, found);
        var n: usize = 0;
        for (mask[0..n_rows], 0..) |m, r| {
            if (!m) continue;
            try std.testing.expect(n < c.want.len);
            try std.testing.expectEqual(c.want[n], r);
            try std.testing.expect(heavy.keeps(ids[r]));
            n += 1;
        }
    }
    try std.testing.expect(heavy.searchPays(1000));
    try std.testing.expect(!heavy.searchPays(20));
}

test "heavy keys search only when every candidate's bytes were kept" {
    const allocator = std.testing.allocator;
    var heavy = try HeavyKeys.init(allocator, 1, 1);
    defer heavy.deinit(allocator);
    try heavy.report(allocator, 0, 1, "a", 1);
    try heavy.report(allocator, 0, 2, "b", 1);
    heavy.endUnit(0, 2, 1);
    try std.testing.expect(try heavy.settle(allocator));
    try std.testing.expect(heavy.keeps(1) and heavy.keeps(2));
    try std.testing.expect(heavy.sorted == null);
    try std.testing.expect(!heavy.searchPays(1 << 20));
}

test "dup sketch keeps every repeated identity and need groups of one" {
    const allocator = std.testing.allocator;
    var dups = try DupSketch.init(allocator, allocator, 3, 2, 50_000);
    defer dups.deinit(allocator);
    var ids: [20_000]u128 = undefined;
    for (&ids, 0..) |*id, i| id.* = i;
    var slots: [20_000]u32 = undefined;
    try dups.tally(0, &ids, null, &slots);
    for (&ids, 0..) |*id, i| id.* = 20_000 + i;
    try dups.tally(1, &ids, null, null);
    // Repeats within a worker, across workers, a collapsed row standing for
    // three, and a run of two.
    try dups.tally(1, &.{ 20_005, 7 }, null, null);
    try dups.tally(0, &.{30_000}, &.{3}, null);
    try dups.tallyRows(1, 40_000, 2);
    try std.testing.expect(try dups.settle(allocator));
    try std.testing.expectEqual(@as(u64, 40_007), dups.rows);
    try std.testing.expect(!dups.kept_all);

    var kept: usize = 0;
    for (0..40_001) |i| kept += @intFromBool(dups.keeps(i));
    for ([_]u128{ 7, 20_005, 30_000, 40_000 }) |id| try std.testing.expect(dups.keeps(id));
    try std.testing.expect(kept >= 4 + 3 and kept < 4 + 3 + 8);
    try std.testing.expect(dups.exact(3));
    try std.testing.expect(!dups.exact(2));

    // A kept row's recorded fingerprint passes the scan's sieve.
    const order = struct {
        fn f(a: u32, b: u32) std.math.Order {
            return std.math.order(a, b);
        }
    }.f;
    for (slots, 0..) |s, i| {
        const passes = std.sort.binarySearch(u32, dups.kept_fingerprints, s, order) != null;
        if (dups.keeps(i)) try std.testing.expect(passes);
    }
}

test "dup sketch keeps every group of one short of the need" {
    const allocator = std.testing.allocator;
    var dups = try DupSketch.init(allocator, allocator, 10, 2, 100);
    defer dups.deinit(allocator);
    try dups.tally(0, &.{ 1, 2, 3 }, null, null);
    try std.testing.expect(try dups.settle(allocator));
    for ([_]u128{ 1, 2, 3 }) |id| try std.testing.expect(dups.keeps(id));
    try std.testing.expect(dups.kept_all and dups.exact(3));
}

test "dup sketch declines keys that repeat too often to pay" {
    const allocator = std.testing.allocator;
    var dups = try DupSketch.init(allocator, allocator, 1, 1, 1000);
    defer dups.deinit(allocator);
    var ids: [1000]u128 = undefined;
    for (&ids, 0..) |*id, i| id.* = i % 300;
    try dups.tally(0, &ids, null, null);
    try std.testing.expect(!try dups.settle(allocator));
}
