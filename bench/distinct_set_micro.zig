//! Standalone micro-bench for the COUNT(DISTINCT) membership set (the Q09 / Q04
//! / Q08-11 bottleneck). Models the real workload: ~100M inserts of a 128-bit
//! (gid<<64 | value) composite where `value` is a full-width 64-bit UserID and
//! the distinct count is ~18M (the measured DISTINCT (RegionID,UserID) pairs).
//!
//! Compares the current set (std.AutoHashMap(u128,void)) against compact
//! open-addressing variants to find a faster exact distinct set.
//!
//!   zig run -OReleaseFast bench/distinct_set_micro.zig
//!
//! Single-threaded: it measures per-set throughput; the engine runs 12 of these
//! in parallel, so the relative speedup is what carries over.

const std = @import("std");
const win = std.os.windows;

fn nowTicks() i64 {
    var c: win.LARGE_INTEGER = 0;
    _ = win.ntdll.RtlQueryPerformanceCounter(&c);
    return c;
}
fn ticksToMs(ticks: i64) f64 {
    var f: win.LARGE_INTEGER = 0;
    _ = win.ntdll.RtlQueryPerformanceFrequency(&f);
    return @as(f64, @floatFromInt(ticks)) * 1000.0 / @as(f64, @floatFromInt(f));
}

const N: usize = 100_000_000; // inserts
const DISTINCT_USERS: u64 = 18_000_000; // ~ matches 18M distinct (region,user)
const REGIONS: u64 = 270;

inline fn splitmix64(x: u64) u64 {
    var z = x +% 0x9E3779B97F4A7C15;
    z = (z ^ (z >> 30)) *% 0xBF58476D1CE4E5B9;
    z = (z ^ (z >> 27)) *% 0x94D049BB133111EB;
    return z ^ (z >> 31);
}

// Reconstruct the i-th composite key without materializing 100M of them (that
// array alone would be 1.6 GB and dominate the cache). Draw the whole
// (region,user) PAIR from an 18M-distinct pool so each pair repeats ~5.5x —
// matching the real data (100M rows, 18M distinct (RegionID,UserID) pairs),
// not a near-all-distinct stream.
inline fn compositeFor(i: usize) u128 {
    const pair = splitmix64(@intCast(i)) % DISTINCT_USERS; // one of ~18M pairs
    const user = splitmix64(pair *% 0xD1B54A32D192ED03); // full 64-bit value
    const region = pair % REGIONS;
    return (@as(u128, region) << 64) | @as(u128, user);
}

// ---- compact open-addressing u128 set -------------------------------------

const EMPTY: u128 = std.math.maxInt(u128);

const OpenSet = struct {
    slots: []u128,
    mask: u64,
    len: u64 = 0,
    has_sentinel: bool = false, // the one key == EMPTY, tracked out-of-band

    fn init(allocator: std.mem.Allocator, expected: u64) !OpenSet {
        // Power-of-two capacity at ~0.7 load factor.
        var cap: u64 = 16;
        while (cap * 7 < expected * 10) cap <<= 1;
        const slots = try allocator.alloc(u128, cap);
        @memset(slots, EMPTY);
        return .{ .slots = slots, .mask = cap - 1 };
    }

    fn deinit(self: *OpenSet, allocator: std.mem.Allocator) void {
        allocator.free(self.slots);
    }

    inline fn hash(key: u128) u64 {
        const lo: u64 = @truncate(key);
        const hi: u64 = @truncate(key >> 64);
        var h = lo ^ (hi *% 0x9E3779B97F4A7C15);
        h *%= 0xD6E8FEB86659FD93;
        return h ^ (h >> 32);
    }

    inline fn insertIsNew(self: *OpenSet, allocator: std.mem.Allocator, key: u128) !bool {
        if (key == EMPTY) {
            const was = self.has_sentinel;
            self.has_sentinel = true;
            return !was;
        }
        if ((self.len + 1) * 10 >= (self.mask + 1) * 7) try self.grow(allocator);
        var i = hash(key) & self.mask;
        while (true) : (i = (i + 1) & self.mask) {
            const cur = self.slots[i];
            if (cur == key) return false;
            if (cur == EMPTY) {
                self.slots[i] = key;
                self.len += 1;
                return true;
            }
        }
    }

    fn grow(self: *OpenSet, allocator: std.mem.Allocator) !void {
        const new_cap = (self.mask + 1) << 1;
        const old = self.slots;
        const slots = try allocator.alloc(u128, new_cap);
        @memset(slots, EMPTY);
        const new_mask = new_cap - 1;
        for (old) |k| {
            if (k == EMPTY) continue;
            var i = hash(k) & new_mask;
            while (self.slots_at(slots, i) != EMPTY) : (i = (i + 1) & new_mask) {}
            slots[i] = k;
        }
        allocator.free(old);
        self.slots = slots;
        self.mask = new_mask;
    }

    inline fn slots_at(_: *OpenSet, slots: []u128, i: u64) u128 {
        return slots[i];
    }

    fn count(self: *const OpenSet) u64 {
        return self.len + @intFromBool(self.has_sentinel);
    }
};

// Mimics group_table.IntGroupTable: a {key:u128, gid:u32} slot (32B after
// alignment) with gid==EMPTY occupancy — measures the cost of the 2x slot
// footprint vs the keys-only 16B OpenSet.
const GidSlot = struct { key: u128, gid: u32 };
const GID_EMPTY: u32 = std.math.maxInt(u32);

const GidTable = struct {
    slots: []GidSlot,
    mask: u64,
    len: u64 = 0,

    fn init(allocator: std.mem.Allocator, expected: u64) !GidTable {
        var cap: u64 = 16;
        while (cap * 7 < expected * 10) cap <<= 1;
        const slots = try allocator.alloc(GidSlot, cap);
        for (slots) |*s| s.gid = GID_EMPTY;
        return .{ .slots = slots, .mask = cap - 1 };
    }
    fn deinit(self: *GidTable, allocator: std.mem.Allocator) void {
        allocator.free(self.slots);
    }
    inline fn insertIsNew(self: *GidTable, key: u128) bool {
        var i = OpenSet.hash(key) & self.mask;
        while (true) : (i = (i + 1) & self.mask) {
            const s = self.slots[i];
            if (s.gid == GID_EMPTY) {
                self.slots[i] = .{ .key = key, .gid = @truncate(self.len) };
                self.len += 1;
                return true;
            }
            if (s.key == key) return false;
        }
    }
    fn count(self: *const GidTable) u64 {
        return self.len;
    }
};

fn runGid(allocator: std.mem.Allocator) !struct { ms: f64, n: u64 } {
    var set = try GidTable.init(allocator, DISTINCT_USERS * 12 / 10);
    defer set.deinit(allocator);
    const start = nowTicks();
    var i: usize = 0;
    while (i < N) : (i += 1) _ = set.insertIsNew(compositeFor(i));
    return .{ .ms = ticksToMs(nowTicks() - start), .n = set.count() };
}

const PF = 16; // prefetch distance for the batched variant

fn runStd(allocator: std.mem.Allocator, presize: bool) !struct { ms: f64, n: u64 } {
    var set: std.AutoHashMapUnmanaged(u128, void) = .empty;
    defer set.deinit(allocator);
    if (presize) try set.ensureTotalCapacity(allocator, @intCast(DISTINCT_USERS * 12 / 10));
    const start = nowTicks();
    var i: usize = 0;
    while (i < N) : (i += 1) {
        const gop = try set.getOrPut(allocator, compositeFor(i));
        _ = gop;
    }
    return .{ .ms = ticksToMs(nowTicks() - start), .n = set.count() };
}

fn runOpen(allocator: std.mem.Allocator) !struct { ms: f64, n: u64 } {
    var set = try OpenSet.init(allocator, DISTINCT_USERS * 12 / 10);
    defer set.deinit(allocator);
    const start = nowTicks();
    var i: usize = 0;
    while (i < N) : (i += 1) {
        _ = try set.insertIsNew(allocator, compositeFor(i));
    }
    return .{ .ms = ticksToMs(nowTicks() - start), .n = set.count() };
}

// Batched: compute keys + hashes for a window, prefetch their slots, then probe.
// Overlaps the memory latency of independent probes (the cache-miss-bound win).
fn runOpenPrefetch(allocator: std.mem.Allocator) !struct { ms: f64, n: u64 } {
    var set = try OpenSet.init(allocator, DISTINCT_USERS * 12 / 10);
    defer set.deinit(allocator);
    var keys: [PF]u128 = undefined;
    var slot0: [PF]u64 = undefined;
    const start = nowTicks();
    var i: usize = 0;
    while (i < N) {
        const batch = @min(PF, N - i);
        for (0..batch) |j| {
            keys[j] = compositeFor(i + j);
            slot0[j] = OpenSet.hash(keys[j]) & set.mask;
            @prefetch(&set.slots[slot0[j]], .{ .rw = .write, .locality = 1 });
        }
        for (0..batch) |j| {
            // Grow check kept out of the hot batch; pre-sized so it never fires.
            var s = slot0[j];
            const key = keys[j];
            if (key == EMPTY) {
                set.has_sentinel = true;
                continue;
            }
            while (true) : (s = (s + 1) & set.mask) {
                const cur = set.slots[s];
                if (cur == key) break;
                if (cur == EMPTY) {
                    set.slots[s] = key;
                    set.len += 1;
                    break;
                }
            }
        }
        i += batch;
    }
    return .{ .ms = ticksToMs(nowTicks() - start), .n = set.count() };
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    std.debug.print("distinct-set micro: N={d} inserts, ~{d} distinct, {d} regions\n\n", .{ N, DISTINCT_USERS, REGIONS });

    {
        const r = try runStd(allocator, false);
        std.debug.print("std.AutoHashMap(u128) no presize : {d:>8.0} ms  ({d:.1} M/s)  distinct={d}\n", .{ r.ms, @as(f64, N) / r.ms / 1000.0, r.n });
    }
    {
        const r = try runStd(allocator, true);
        std.debug.print("std.AutoHashMap(u128) presized   : {d:>8.0} ms  ({d:.1} M/s)  distinct={d}\n", .{ r.ms, @as(f64, N) / r.ms / 1000.0, r.n });
    }
    {
        const r = try runOpen(allocator);
        std.debug.print("OpenSet(u128) 16B keys-only      : {d:>8.0} ms  ({d:.1} M/s)  distinct={d}\n", .{ r.ms, @as(f64, N) / r.ms / 1000.0, r.n });
    }
    {
        const r = try runGid(allocator);
        std.debug.print("GidTable(u128) 32B {{key,gid}}     : {d:>8.0} ms  ({d:.1} M/s)  distinct={d}\n", .{ r.ms, @as(f64, N) / r.ms / 1000.0, r.n });
    }
    {
        const r = try runOpenPrefetch(allocator);
        std.debug.print("OpenSet(u128) + prefetch batch   : {d:>8.0} ms  ({d:.1} M/s)  distinct={d}\n", .{ r.ms, @as(f64, N) / r.ms / 1000.0, r.n });
    }
}
