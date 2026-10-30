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

const std = @import("std");
const Allocator = std.mem.Allocator;

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
    /// Where the tally records its rows' counters for the filter pass.
    row_slots: ?RowSlots = null,

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
        var x: u64 = @as(u64, @truncate(identity)) ^ (@as(u64, @truncate(identity >> 64)) *% 0x9e3779b97f4a7c15);
        x ^= x >> 33;
        x *%= 0xff51afd7ed558ccd;
        x ^= x >> 33;
        x *%= 0xc4ceb9fe1a85ec53;
        x ^= x >> 33;
        return @intCast(x >> @intCast(64 - @as(u7, self.bits)));
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
        if (@as(f64, @floatFromInt(kth)) <= collisionCeiling(self.rows, self.bits)) return false;
        self.threshold = @max(1, kth / 2);
        if (ranked_by == .at_most_row_count) self.threshold = lowestThreshold(totals, self.threshold, self.rows / BELOW_ROW_COUNT_KEPT_FRACTION_DENOM);

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
        self.threshold = kth_count + 1;
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
        if (self.kept_rows == self.rows) return true;
        return found >= need and kth_count + 1 >= self.threshold;
    }
};

/// Each row's counter, per row group a tally saw whole, so the filter pass's
/// scan can drop the rows of the counters it doesn't keep before decoding
/// them (`Scan.RowSieve`). The filter pass still tests every row it gets.
pub const RowSlots = struct {
    allocator: Allocator,
    /// Per segment of the snapshot, the flat index of its first row group.
    seg_first: []usize,
    /// Per flat row group, its rows' counters once a tally records them.
    rows: []?[]const u32,

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
        const rows = try allocator.alloc(?[]const u32, total);
        @memset(rows, null);
        return .{ .allocator = allocator, .seg_first = seg_first, .rows = rows };
    }

    pub fn deinit(self: *RowSlots) void {
        for (self.rows) |r| if (r) |slots| self.allocator.free(slots);
        self.allocator.free(self.rows);
        self.allocator.free(self.seg_first);
    }

    /// Room for the `n` counters of segment `seg`'s row group `rg`. Only
    /// the worker that tallies a row group may record it.
    pub fn record(self: *RowSlots, seg: usize, rg: usize, n: usize) ![]u32 {
        const slots = try self.allocator.alloc(u32, n);
        const at = &self.rows[self.seg_first[seg] + rg];
        if (at.*) |prior| self.allocator.free(prior);
        at.* = slots;
        return slots;
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
    /// Set when the tally declined before scanning for want of input: the
    /// counter sketch would decline too.
    declined_small: bool = false,
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
        self.* = undefined;
    }

    /// The threshold for a unit of `rows` rows whose need-th best key holds
    /// `unit_kth` of them: t - 1 is a quarter of the lesser of two estimates
    /// of the unit's share of τ. The unit's own need-th count overshoots it
    /// where hot keys come and go between units; worker `w`'s need-th best
    /// partial prorated by rows undershoots it early on, which costs only
    /// hashing.
    pub fn unitThreshold(self: *const HeavyKeys, w: usize, rows: u64, unit_kth: u64) u64 {
        const worker = &self.workers[w];
        var share = unit_kth;
        if (worker.rows > 0) share = @min(share, @as(u64, @intCast(@as(u128, worker.kth()) * rows / worker.rows)));
        return 1 + share / SLACK_SHARE_DENOM;
    }

    /// Adds a unit's `count` rows of `key`, the digest of `bytes`, to
    /// worker `w`'s partials. Only worker `w` may call this.
    pub fn report(self: *HeavyKeys, allocator: Allocator, w: usize, key: u128, bytes: []const u8, count: u64) !void {
        const worker = &self.workers[w];
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
        self.workers[w].slack += threshold - 1;
        self.workers[w].rows += rows;
    }

    /// Merges the workers' partials and picks the candidates. False when the
    /// slack reaches τ (a key no unit reported might then rank) or the
    /// candidates are too many to pay.
    pub fn settle(self: *HeavyKeys, allocator: Allocator) !bool {
        var slack: u64 = 0;
        var reported: usize = 0;
        for (self.workers) |*w| {
            slack += w.slack;
            reported += w.partials.count();
        }
        var merged: std.AutoHashMapUnmanaged(u128, Partial) = .empty;
        defer merged.deinit(allocator);
        try merged.ensureTotalCapacity(allocator, @intCast(reported));
        for (self.workers) |*w| {
            var it = w.partials.iterator();
            while (it.next()) |e| {
                const gop = merged.getOrPutAssumeCapacity(e.key_ptr.*);
                if (gop.found_existing) gop.value_ptr.count += e.value_ptr.count else gop.value_ptr.* = e.value_ptr.*;
            }
        }
        const counts = try allocator.alloc(u64, merged.count());
        defer allocator.free(counts);
        var i: usize = 0;
        var values = merged.valueIterator();
        while (values.next()) |p| : (i += 1) counts[i] = p.count;
        var best: [MAX_NEED]u64 = undefined;
        const tau = kthLargest(u64, counts, best[0..self.need]);
        if (tau <= slack) return false;

        var n: usize = 0;
        for (counts) |c| n += @intFromBool(c + slack >= tau);
        if (n > MAX_HEAVY_CANDIDATES) return false;
        try self.candidates.ensureTotalCapacity(allocator, @intCast(n));
        self.signature_bits = @intCast(@max(6, std.math.log2_int_ceil(usize, 2 * n)));
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
