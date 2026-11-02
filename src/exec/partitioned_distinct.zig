//! Partition-first exact distinct set for a parallel fold.
//!
//! Private per-worker sets followed by a cross-worker merge insert every key
//! twice, and each worker's set tends toward the whole key space when the input
//! isn't clustered. Once the sets outgrow the cache, each insert also moves a
//! cache line to place one key. Here workers instead route each key by hash
//! into one of `PARTITIONS` partitions, appending it to a small chunk; full
//! chunks go onto that partition's lock-free stack. Each partition is drained
//! into its final set by one thread at a time, so a set is built while it is
//! cache-sized, and each key reaches exactly one set.
//!
//! Memory follows the distinct count, not the row count: once the queued keys
//! exceed the larger of a floor (see `QUEUE_FLOOR_KEYS`) and the keys already
//! in the final sets, the worker that notices drains partitions until the queue
//! halves. Whatever is still queued when the fold ends is drained through
//! `drainNext`.

const std = @import("std");
const Allocator = std.mem.Allocator;
const group_table = @import("group_table.zig");
const CountSlotTable = group_table.CountSlotTable;
const memory = @import("../memory.zig");

// Enough partitions that the sets the threads drain at once stay near L2 size
// for tens of millions of keys: at 256, an 18M-key grouped distinct drained
// into 2 MB sets from DRAM. At 2048, the producers' open chunks outgrew the
// cache instead.
pub const PARTITIONS: usize = 1024;
const PARTITION_SHIFT: u6 = 64 - @ctz(@as(u64, PARTITIONS));
const CHUNK_KEYS: usize = 256;
// Keys the queue may hold before workers drain, however small the final sets
// are. It slides from the floor toward the ceiling with the share of drained
// keys that were new: while most are, a longer queue means fewer passes over
// sets that are still growing (each pass reloads its set), and a stream of
// new keys up to the ceiling drains once, at the end; once they are mostly
// repeats, a short queue keeps its chunks in cache until they drain.
const QUEUE_FLOOR_KEYS: usize = 1 << 21;
const QUEUE_CEILING_BYTES: usize = 512 << 20;
const INSERT_PREFETCH: usize = 16;
const LINE_BYTES: usize = 64;
// How far ahead of its insert cursor a drain prefetches its chunks' keys.
// Chunks are written long before they drain, so their keys come from DRAM,
// and the list hops between chunks the hardware prefetcher can't predict.
const KEY_LOOKAHEAD_BYTES: usize = 1024;
// Publishes a producer lets pass without a relieve after one that found
// nothing worth draining: every partition was empty, small, or taken.
const RELIEVE_BACKOFF: u32 = 8;
// Partitions one relieve drains at most. The worker that notices the queue
// over budget keeps folding after a few drains instead of draining half the
// queue alone: a long relieve late in the scan left the other workers idle at
// the join.
const RELIEVE_MAX_DRAINS: usize = 4;
// Chunks come from slabs of this many, recycled through spares and freed only
// with the set: per-chunk frees at teardown, all threads at once, contend on
// the allocator's shared counters.
const SLAB_CHUNKS: usize = 32;

comptime {
    std.debug.assert(std.math.isPowerOfTwo(PARTITIONS));
}

/// One draining thread's tally of the keys it found new, by `key >> shift`:
/// a grouped COUNT(DISTINCT) whose composites carry the group key above
/// `shift` reads its per-group counts from these instead of rescanning every
/// set. Each key is new in exactly one drain, so the tallies of all drainers
/// sum to the exact counts.
pub const NewKeyCounts = struct {
    table: CountSlotTable = .empty,
    shift: u7,
    allocator: Allocator,

    pub fn deinit(self: *NewKeyCounts) void {
        if (self.table.slots.len > 0) self.table.deinit(self.allocator);
        self.table = .empty;
    }
};

pub fn PartitionedDistinct(comptime Key: type) type {
    return struct {
        const Self = @This();

        pub const Set = switch (Key) {
            u64 => group_table.DistinctU64Set,
            u128 => group_table.DistinctU128Set,
            else => @compileError("PartitionedDistinct keys are u64 or u128"),
        };

        const Chunk = struct {
            next: ?*Chunk,
            len: usize,
            keys: [CHUNK_KEYS]Key,
        };

        const Slab = struct {
            next: ?*Slab,
            chunks: [SLAB_CHUNKS]Chunk,
        };

        const Partition = struct {
            full: std.atomic.Value(?*Chunk) = .init(null),
            pending: std.atomic.Value(usize) = .init(0),
            draining: std.atomic.Value(bool) = .init(false),
            set: Set = Set.empty,
        };

        allocator: Allocator,
        // Sets and slabs recycle through the process buffer pool: from the
        // query's heap, every statement mapped them fresh and faulted in each
        // page its sets' growth touched (a fifth of a COUNT(DISTINCT) over a
        // string column), then unmapped them again.
        pooled: Allocator,
        partitions: [PARTITIONS]Partition = @splat(.{}),
        slabs: std.atomic.Value(?*Slab) = .init(null),
        queued: std.atomic.Value(usize) = .init(0),
        in_sets: std.atomic.Value(usize) = .init(0),
        drained: std.atomic.Value(usize) = .init(0),
        relieve_cursor: std.atomic.Value(usize) = .init(0),
        final_cursor: std.atomic.Value(usize) = .init(0),
        queue_floor: usize = QUEUE_FLOOR_KEYS,
        queue_ceiling: usize = QUEUE_CEILING_BYTES / @sizeOf(Key),

        pub fn create(allocator: Allocator) !*Self {
            const pooled = try memory.workerAllocatorOf(allocator);
            const self = try allocator.create(Self);
            self.* = .{ .allocator = allocator, .pooled = pooled };
            return self;
        }

        pub fn destroy(self: *Self) void {
            for (&self.partitions) |*part| part.set.deinit(self.pooled);
            var slabs = self.slabs.load(.monotonic);
            while (slabs) |slab| {
                slabs = slab.next;
                self.pooled.destroy(slab);
            }
            self.allocator.destroy(self);
        }

        fn newSlab(self: *Self) !*Slab {
            const slab = try self.pooled.create(Slab);
            var head = self.slabs.load(.monotonic);
            while (true) {
                slab.next = head;
                head = self.slabs.cmpxchgWeak(head, slab, .release, .monotonic) orelse break;
            }
            return slab;
        }

        // The top bits of the hash the set buckets on; the set's slot comes
        // from the low bits, so routing doesn't skew the probe.
        inline fn partitionOf(key: Key) usize {
            const h = if (Key == u64) group_table.mix64(key) else group_table.hashU128(key);
            return @intCast(h >> PARTITION_SHIFT);
        }

        // The counts go up before the chunk is visible, so a drain that takes
        // it can never subtract them first and wrap the counters.
        fn publish(self: *Self, p: usize, ch: *Chunk) void {
            const part = &self.partitions[p];
            _ = self.queued.fetchAdd(ch.len, .monotonic);
            _ = part.pending.fetchAdd(ch.len, .monotonic);
            var head = part.full.load(.monotonic);
            while (true) {
                ch.next = head;
                head = part.full.cmpxchgWeak(head, ch, .release, .monotonic) orelse break;
            }
        }

        // Insert partition `p`'s queued chunks into its set, unless another
        // thread is already draining it. The emptied chunks become `spare`.
        // Once a chunk comes out mostly new keys, the set is sized for the rest
        // of the batch at that rate instead of doubling its way through it; a
        // batch of mostly repeats grows only as keys arrive, so a small set
        // stays small. A final drain of a partition no relieve has drained
        // counts its keys in the drainer's scratch set instead: no later key
        // can join that set, and one set reused while it stays in cache saves
        // building (and first zeroing) a set per partition from DRAM.
        noinline fn drain(self: *Self, p: usize, spare: *Spare, counts: ?*NewKeyCounts, final: bool) !bool {
            const part = &self.partitions[p];
            // Plain loads first: a relieve walks partitions that others have
            // just emptied, and a write to their lines would bounce them.
            if (part.full.load(.monotonic) == null or part.draining.load(.monotonic)) return false;
            if (part.draining.swap(true, .acquire)) return false;
            defer part.draining.store(false, .release);
            var list: ?*Chunk = part.full.swap(null, .acquire) orelse return false;
            try memory.checkCancelled(self.allocator);
            // Each chunk is counted into `pending` before it goes on the list,
            // so `pending` bounds the list's keys without walking it: a walk
            // misses on every chunk's header in turn.
            var remaining = part.pending.load(.monotonic);
            var queued: usize = 0;
            const set = if (final and part.set.slots.len == 0) spare.clearedScratch() else &part.set;
            const before = set.count();
            var last_new: usize = 0;
            var last_len: usize = 0;
            while (list) |ch| {
                remaining -|= ch.len;
                queued += ch.len;
                const next: []const Key = if (ch.next) |n| next: {
                    @prefetch(&n.next, .{});
                    break :next &n.keys;
                } else &.{};
                const ahead = if (last_new * 2 > last_len) remaining * last_new / last_len else 0;
                try set.ensureFor(self.pooled, ch.len + ahead);
                const prior = set.count();
                if (counts) |c| {
                    try c.table.ensureFor(c.allocator, ch.len);
                    insertAll(true, set, ch.keys[0..ch.len], next, c);
                } else insertAll(false, set, ch.keys[0..ch.len], next, {});
                last_new = set.count() - prior;
                last_len = ch.len;
                list = ch.next;
                spare.put(ch);
            }
            _ = part.pending.fetchSub(queued, .monotonic);
            _ = self.queued.fetchSub(queued, .monotonic);
            _ = self.drained.fetchAdd(queued, .monotonic);
            _ = self.in_sets.fetchAdd(set.count() - before, .monotonic);
            return true;
        }

        inline fn hashOf(key: Key) u64 {
            return if (Key == u64) group_table.mix64(key) else group_table.hashU128(key);
        }

        // The probe walks local copies of the set's fields: through `set`
        // they reload after every slot store, which may alias them. Each key's
        // hash is computed once, when its slot is prefetched. The tally, too,
        // is a local copy, written back once. The key prefetch runs on into
        // `next`, the chunk drained after this one.
        fn insertAll(comptime counting: bool, set: *Set, keys: []const Key, next: []const Key, counts: if (counting) *NewKeyCounts else void) void {
            const slots = set.slots;
            var tally = if (counting) counts.table else {};
            const shift = if (counting) counts.shift else {};
            const mask = slots.len - 1;
            const line_keys = LINE_BYTES / @sizeOf(Key);
            const lookahead = KEY_LOOKAHEAD_BYTES / @sizeOf(Key);
            var hashes: [INSERT_PREFETCH]u64 = undefined;
            for (keys[0..@min(keys.len, INSERT_PREFETCH)], 0..) |key, j| {
                hashes[j] = hashOf(key);
                @prefetch(&slots[hashes[j] & mask], .{ .rw = .write, .locality = 3 });
            }
            var added: u32 = 0;
            var sentinel = false;
            for (keys, 0..) |key, j| {
                if (j % line_keys == 0) {
                    const at = j + lookahead;
                    if (at < keys.len) {
                        @prefetch(&keys[at], .{});
                    } else if (at - keys.len < next.len) @prefetch(&next[at - keys.len], .{});
                }
                const ring = j % INSERT_PREFETCH;
                const h = hashes[ring];
                if (j + INSERT_PREFETCH < keys.len) {
                    hashes[ring] = hashOf(keys[j + INSERT_PREFETCH]);
                    @prefetch(&slots[hashes[ring] & mask], .{ .rw = .write, .locality = 3 });
                }
                if (key == Set.SENTINEL) {
                    if (counting and !sentinel and !set.has_sentinel) tally.insert(@truncate(@as(u128, key) >> shift));
                    sentinel = true;
                    continue;
                }
                var b = h & mask;
                while (true) : (b = (b + 1) & mask) {
                    const slot = slots[b];
                    if (slot == Set.SENTINEL) {
                        slots[b] = key;
                        added += 1;
                        if (counting) tally.insert(@truncate(@as(u128, key) >> shift));
                        break;
                    }
                    if (slot == key) break;
                }
            }
            set.len += added;
            if (sentinel) set.has_sentinel = true;
            if (counting) counts.table = tally;
        }

        // While the queue is over budget, drain up to `RELIEVE_MAX_DRAINS`
        // partitions, stopping early once it is back under half the budget or
        // every partition has been tried. Each relieve
        // starts at its own point of the round so concurrent ones spread out,
        // and passes over partitions holding less than an even share of the
        // queue: one refilled since its last drain holds a chunk or two, not
        // worth a pass over its set. True when over budget with nothing worth
        // draining.
        fn relieve(self: *Self, spare: *Spare, counts: ?*NewKeyCounts) !bool {
            const in_sets = self.in_sets.load(.monotonic);
            const drained = self.drained.load(.monotonic);
            const floor = if (drained == 0)
                self.queue_floor
            else
                self.queue_floor + (self.queue_ceiling - self.queue_floor) * in_sets / drained;
            const budget = @max(floor, in_sets);
            const queued = self.queued.load(.monotonic);
            if (queued <= budget) return false;
            const share = queued / PARTITIONS;
            const start = self.relieve_cursor.fetchAdd(PARTITIONS / 16, .monotonic);
            var drains: usize = 0;
            var tried: usize = 0;
            while (tried < PARTITIONS and drains < RELIEVE_MAX_DRAINS and self.queued.load(.monotonic) > budget / 2) : (tried += 1) {
                const p = (start + tried) % PARTITIONS;
                if (self.partitions[p].pending.load(.monotonic) < share) continue;
                if (try self.drain(p, spare, counts, false)) drains += 1;
            }
            return drains == 0;
        }

        /// Drain the next partition once every producer has finished; false
        /// when none are left. Any number of threads may call it, each with
        /// its own `spare` and `counts`.
        pub fn drainNext(self: *Self, spare: *Spare, counts: ?*NewKeyCounts) !bool {
            const p = self.final_cursor.fetchAdd(1, .monotonic);
            if (p >= PARTITIONS) return false;
            _ = try self.drain(p, spare, counts, true);
            return true;
        }

        /// Exact once every partition has drained.
        pub fn count(self: *const Self) u64 {
            return self.in_sets.load(.monotonic);
        }

        /// One thread's emptied chunks, plus the set its final drains count
        /// partitions in; `deinit` frees that set.
        pub const Spare = struct {
            head: ?*Chunk = null,
            scratch: Set = Set.empty,

            pub fn deinit(self: *Spare, shared: *const Self) void {
                self.scratch.deinit(shared.pooled);
            }

            fn clearedScratch(self: *Spare) *Set {
                self.scratch.clearRetainingCapacity();
                return &self.scratch;
            }

            fn take(self: *Spare) ?*Chunk {
                const ch = self.head orelse return null;
                self.head = ch.next;
                return ch;
            }

            fn put(self: *Spare, ch: *Chunk) void {
                ch.next = self.head;
                self.head = ch;
            }
        };

        /// One worker's side: an open chunk per partition and its spare
        /// chunks. The fill counts live here, not in the chunks, so a push
        /// writes only the line its key lands in. The worker's drains tally
        /// new keys into `counts`, when set.
        pub const Producer = struct {
            shared: *Self,
            counts: ?*NewKeyCounts = null,
            open: [PARTITIONS]?*Chunk = @splat(null),
            fill: [PARTITIONS]u16 = @splat(0),
            spare: Spare = .{},
            relieve_skip: u32 = 0,
            slab: ?*Slab = null,
            slab_used: usize = SLAB_CHUNKS,

            // A key starting a line also prefetches the line after it: the
            // partition's next keys arrive around a thousand pushes later,
            // and an open chunk's lines are cold, so without the prefetch the
            // first store to each line waits on its fetch.
            pub inline fn push(self: *Producer, key: Key) !void {
                const p = partitionOf(key);
                const ch = self.open[p] orelse try self.openChunk(p);
                const at = self.fill[p];
                ch.keys[at] = key;
                const line_keys = LINE_BYTES / @sizeOf(Key);
                if (at % line_keys == 0 and at + line_keys < CHUNK_KEYS) @prefetch(&ch.keys[at + line_keys], .{ .rw = .write });
                if (at + 1 < CHUNK_KEYS) {
                    self.fill[p] = at + 1;
                    return;
                }
                try self.publishFull(p);
            }

            fn publishFull(self: *Producer, p: usize) !void {
                const ch = self.open[p].?;
                ch.len = CHUNK_KEYS;
                self.open[p] = null;
                self.fill[p] = 0;
                self.shared.publish(p, ch);
                if (self.relieve_skip > 0) {
                    self.relieve_skip -= 1;
                    return;
                }
                if (try self.shared.relieve(&self.spare, self.counts)) self.relieve_skip = RELIEVE_BACKOFF;
            }

            fn openChunk(self: *Producer, p: usize) !*Chunk {
                const ch = self.spare.take() orelse try self.carve();
                ch.next = null;
                self.open[p] = ch;
                return ch;
            }

            fn carve(self: *Producer) !*Chunk {
                if (self.slab_used == SLAB_CHUNKS) {
                    self.slab = try self.shared.newSlab();
                    self.slab_used = 0;
                }
                defer self.slab_used += 1;
                return &self.slab.?.chunks[self.slab_used];
            }

            /// Publish the partly filled chunks once the worker's fold is done.
            pub fn finish(self: *Producer) void {
                for (&self.open, &self.fill, 0..) |*slot, *fill, p| if (slot.*) |ch| {
                    ch.len = fill.*;
                    slot.* = null;
                    fill.* = 0;
                    self.shared.publish(p, ch);
                };
            }
        };
    };
}

fn pushAll(comptime Key: type, producer: *PartitionedDistinct(Key).Producer, keys: []const Key) !void {
    for (keys) |k| try producer.push(k);
}

test "partitioned distinct counts exactly through budget drains" {
    // A tiny floor makes producers drain mid-fold, so the final drains join
    // sets; at the default floor nothing drains until the end, and every
    // partition is counted in the drainer's scratch set.
    inline for (.{ u64, u128 }) |Key| for ([_]usize{ 1000, QUEUE_FLOOR_KEYS }) |floor| {
        const PD = PartitionedDistinct(Key);
        const pd = try PD.create(std.testing.allocator);
        defer pd.destroy();
        pd.queue_floor = floor;
        pd.queue_ceiling = 4 * floor;
        var producers: [3]PD.Producer = @splat(.{ .shared = pd });
        var reference = std.AutoHashMap(Key, void).init(std.testing.allocator);
        defer reference.deinit();
        for (0..60_000) |i| {
            const key: Key = @intCast((i * 7919) % 20_011);
            try producers[i % 3].push(key);
            try reference.put(key, {});
        }
        // The one value equal to the sets' empty-slot sentinel counts once.
        try producers[1].push(std.math.maxInt(Key));
        try producers[2].push(std.math.maxInt(Key));
        try reference.put(std.math.maxInt(Key), {});
        for (&producers) |*p| p.finish();
        var spare: PD.Spare = .{};
        defer spare.deinit(pd);
        while (try pd.drainNext(&spare, null)) {}
        try std.testing.expectEqual(@as(u64, reference.count()), pd.count());
    };
}

test "partitioned distinct is exact with concurrent producers and drains" {
    const PD = PartitionedDistinct(u64);
    const pd = try PD.create(std.testing.allocator);
    defer pd.destroy();
    pd.queue_floor = 4096;
    pd.queue_ceiling = 16384;
    const Worker = struct {
        fn run(producer: *PD.Producer, keys: []const u64, err: *?anyerror) void {
            pushAll(u64, producer, keys) catch |e| {
                err.* = e;
            };
        }
        fn drainAll(shared: *PD, err: *?anyerror) void {
            var spare: PD.Spare = .{};
            defer spare.deinit(shared);
            while (shared.drainNext(&spare, null) catch |e| {
                err.* = e;
                return;
            }) {}
        }
    };
    // Four overlapping ranges of 50_000 starting 20_000 apart: 110_000 values.
    var inputs: [4][]u64 = undefined;
    for (&inputs, 0..) |*keys, t| {
        keys.* = try std.testing.allocator.alloc(u64, 50_000);
        for (keys.*, 0..) |*k, i| k.* = @intCast(t * 20_000 + i);
    }
    defer for (inputs) |keys| std.testing.allocator.free(keys);
    var producers: [4]PD.Producer = @splat(.{ .shared = pd });
    var errs: [4]?anyerror = @splat(null);
    var threads: [4]std.Thread = undefined;
    for (&threads, 0..) |*th, t| th.* = try std.Thread.spawn(.{}, Worker.run, .{ &producers[t], inputs[t], &errs[t] });
    for (threads) |th| th.join();
    for (errs) |e| if (e) |err| return err;
    for (&producers) |*p| p.finish();
    for (&threads, 0..) |*th, t| th.* = try std.Thread.spawn(.{}, Worker.drainAll, .{ pd, &errs[t] });
    for (threads) |th| th.join();
    for (errs) |e| if (e) |err| return err;
    try std.testing.expectEqual(@as(u64, 110_000), pd.count());
}

test "partitioned distinct tallies each new key by prefix in exactly one drainer" {
    inline for (.{ u64, u128 }) |Key| {
        const PD = PartitionedDistinct(Key);
        const shift = @bitSizeOf(Key) / 2;
        const pd = try PD.create(std.testing.allocator);
        defer pd.destroy();
        // A tiny floor makes producers drain mid-fold, not only at the end.
        pd.queue_floor = 1000;
        pd.queue_ceiling = 4000;
        var counts: [4]NewKeyCounts = @splat(.{ .shift = shift, .allocator = std.testing.allocator });
        defer for (&counts) |*c| c.deinit();
        var producers: [3]PD.Producer = undefined;
        for (&producers, 0..) |*p, i| p.* = .{ .shared = pd, .counts = &counts[i] };
        var want = std.AutoHashMap(u64, u64).init(std.testing.allocator);
        defer want.deinit();
        var seen = std.AutoHashMap(Key, void).init(std.testing.allocator);
        defer seen.deinit();
        for (0..60_002) |i| {
            // The last two pushes are the sets' empty-slot sentinel.
            const key: Key = if (i >= 60_000) std.math.maxInt(Key) else (@as(Key, i % 7) << shift) | ((i * 7919) % 20_011);
            try producers[i % 3].push(key);
            if ((try seen.getOrPut(key)).found_existing) continue;
            const e = try want.getOrPut(@truncate(@as(u128, key) >> shift));
            e.value_ptr.* = if (e.found_existing) e.value_ptr.* + 1 else 1;
        }
        for (&producers) |*p| p.finish();
        var spare: PD.Spare = .{};
        defer spare.deinit(pd);
        while (try pd.drainNext(&spare, &counts[3])) {}
        var got = std.AutoHashMap(u64, u64).init(std.testing.allocator);
        defer got.deinit();
        for (counts) |c| {
            for (c.table.slots) |s| if (s.key != CountSlotTable.SENTINEL) {
                const e = try got.getOrPut(s.key);
                e.value_ptr.* = if (e.found_existing) e.value_ptr.* + s.count else s.count;
            };
            if (c.table.has_sentinel) {
                const e = try got.getOrPut(CountSlotTable.SENTINEL);
                e.value_ptr.* = if (e.found_existing) e.value_ptr.* + c.table.sentinel_count else c.table.sentinel_count;
            }
        }
        try std.testing.expectEqual(want.count(), got.count());
        var it = want.iterator();
        while (it.next()) |e| try std.testing.expectEqual(e.value_ptr.*, got.get(e.key_ptr.*).?);
    }
}
