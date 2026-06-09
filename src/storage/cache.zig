//! Pinning LRU buffer pool for decompressed column blocks, shared across all
//! queries against a `Table`.
//!
//! Keyed by `(segment_id, row_group_idx, column_idx)`. Value is the raw,
//! decompressed (but not decoded) bytes of one column block. A cache hit
//! still pays a cheap decode step but skips the expensive zstd decompress.
//!
//! Bounded by total cached bytes (`capacity_bytes`). The cache does NOT assume
//! the dataset fits in memory: when over budget it evicts unpinned LRU entries,
//! and if every entry down to budget is currently pinned it temporarily exceeds
//! capacity rather than free a block someone is reading.
//!
//! ## Concurrency
//! Thread-per-connection servers share one cache per table. All metadata
//! mutations (map, LRU links, byte counter, pin counts) are guarded by `mutex`,
//! held only for the O(1) bookkeeping — never across decompress or decode.
//!
//! ## Pinning (in-use accounting)
//! `acquire`/`insertPinned` return an entry with its pin count incremented;
//! eviction never frees a pinned entry. The caller decodes from `entry.bytes`
//! and then calls `release`. Pins are meant to be held only for the duration of
//! one block decode, so callers should `defer cache.release(entry)` immediately
//! — that guarantees the pin drops on every return path, including a `try`
//! error mid-decode, so a failed query can't leak a pin and wedge a block.
//!
//! ## Coherence
//! Segments are immutable once written and segment IDs come from a monotonic,
//! never-reused counter, so a cached block is valid for the entire life of its
//! segment and keys never collide with a future segment. Retired segments (post
//! compaction) are simply never looked up again and age out via LRU. There is
//! no in-place mutation to invalidate, and the memtable is never cached here.

const std = @import("std");
const Allocator = std.mem.Allocator;
const format = @import("format.zig");
const huge_page = @import("../util/huge_page.zig");

pub const Key = struct {
    segment_id: u64,
    row_group_idx: u32,
    column_idx: u32,
};

/// CAS-based spinlock. Zig 0.16's stdlib `std.Thread.Mutex` is gone and
/// `Io.Mutex` requires an `Io` the decode path doesn't carry. The cache's
/// critical sections are O(1) bookkeeping, so spinning is the right tool.
const SpinLock = struct {
    state: std.atomic.Value(bool) = .{ .raw = false },

    fn lock(self: *SpinLock) void {
        while (self.state.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
            std.atomic.spinLoopHint();
        }
    }

    fn unlock(self: *SpinLock) void {
        self.state.store(false, .release);
    }
};

// Process-wide cache counters, summed across all tables' caches. Diagnostic
// only (read by the MySQL handler under `--profile-ops` to print per-query
// deltas): a spike in `g_misses`/`g_miss_bytes` on a query that was fast in
// isolation pins the slowdown to cache thrashing (re-read + re-decompress from
// disk), not CPU. Incremented with the per-cache counters; atomic so the
// parallel scan workers don't race.
pub var g_hits = std.atomic.Value(u64).init(0);
pub var g_misses = std.atomic.Value(u64).init(0);
pub var g_evictions = std.atomic.Value(u64).init(0);
pub var g_miss_bytes = std.atomic.Value(u64).init(0);
// Live bytes held across all caches (decompressed block payloads only). Insert
// adds, eviction/deinit subtracts. Lets a leak check separate cache memory from
// the rest of the process's committed footprint.
pub var g_cache_bytes = std.atomic.Value(u64).init(0);

pub const GlobalStats = struct { hits: u64, misses: u64, evictions: u64, miss_bytes: u64, cache_bytes: u64 };

pub fn globalStats() GlobalStats {
    return .{
        .hits = g_hits.load(.monotonic),
        .misses = g_misses.load(.monotonic),
        .evictions = g_evictions.load(.monotonic),
        .miss_bytes = g_miss_bytes.load(.monotonic),
        .cache_bytes = g_cache_bytes.load(.monotonic),
    };
}

pub const Cache = struct {
    /// Metadata allocator (Entry structs + the key map). Block payloads do NOT
    /// come from here — they're sub-allocated from `pool` so the decompressed
    /// set lands on huge, locked pages (see huge_page.zig).
    allocator: Allocator,
    /// Huge-page-backed pool for the decompressed block bytes. Sub-MIN_BLOCK
    /// blocks pass straight through to `allocator`, so the cache's unit tests
    /// (tiny payloads) behave exactly as before.
    pool: huge_page.Pool,
    capacity_bytes: usize,
    current_bytes: usize = 0,

    map: std.AutoHashMapUnmanaged(Key, *Entry) = .empty,
    /// Doubly-linked LRU list. Head = most recently used; tail = LRU (next evict).
    head: ?*Entry = null,
    tail: ?*Entry = null,

    /// Guards every field below the allocator/capacity. Held only for O(1)
    /// bookkeeping; never across a decompress or decode.
    mutex: SpinLock = .{},

    /// Stats (read under `mutex`, for benches and debugging).
    hits: u64 = 0,
    misses: u64 = 0,
    evictions: u64 = 0,

    pub const Entry = struct {
        key: Key,
        bytes: []align(16) u8,
        prev: ?*Entry,
        next: ?*Entry,
        /// In-use refcount. Nonzero entries are never evicted.
        pins: u32 = 0,
        /// Value encoding of the cached (decompressed) block. The block header
        /// isn't cached, so the encoding rides alongside the payload bytes here;
        /// the decode path needs it to interpret `bytes` (raw vs FOR). Defaults
        /// to `.raw` so callers that don't set it (and tests) behave as before.
        encoding: format.Encoding = .raw,
    };

    pub fn init(allocator: Allocator, capacity_bytes: usize) Cache {
        return .{
            .allocator = allocator,
            .pool = huge_page.Pool.init(allocator),
            .capacity_bytes = capacity_bytes,
        };
    }

    /// Allocator for decompressed block payloads. The segment reader allocates
    /// the bytes it hands to `insertPinned` from here so the cache can free them
    /// through the same pool on eviction.
    pub fn blockAllocator(self: *Cache) Allocator {
        return self.pool.allocator();
    }

    pub fn deinit(self: *Cache) void {
        _ = g_cache_bytes.fetchSub(self.current_bytes, .monotonic);
        const block_alloc = self.pool.allocator();
        var cur = self.head;
        while (cur) |e| {
            const nxt = e.next;
            block_alloc.free(e.bytes);
            self.allocator.destroy(e);
            cur = nxt;
        }
        self.map.deinit(self.allocator);
        self.pool.deinit();
        self.* = undefined;
    }

    /// Acquire a pinned reference to the block for `key`, or null on miss. On
    /// hit the entry is pinned (eviction-proof until released) and moved to MRU.
    /// The caller MUST `release` it when done — prefer `defer cache.release(e)`
    /// so the pin drops on error paths too. Thread-safe.
    pub fn acquire(self: *Cache, key: Key) ?*Entry {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.map.get(key)) |entry| {
            entry.pins += 1;
            self.touch(entry);
            self.hits += 1;
            _ = g_hits.fetchAdd(1, .monotonic);
            return entry;
        }
        self.misses += 1;
        _ = g_misses.fetchAdd(1, .monotonic);
        return null;
    }

    /// Insert `bytes` for `key` (ownership transfers to the cache) and return a
    /// pinned entry to decode from. `encoding` records how to interpret `bytes`
    /// (raw vs FOR). If another thread inserted `key` between the caller's miss
    /// and this call, `bytes` is freed and the existing entry is returned pinned
    /// instead. On allocation failure the entry is not stored and `bytes` is left
    /// for the caller to free. Caller MUST `release`.
    pub fn insertPinned(self: *Cache, key: Key, bytes: []align(16) u8, encoding: format.Encoding) !*Entry {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.map.get(key)) |existing| {
            self.pool.allocator().free(bytes);
            existing.pins += 1;
            self.touch(existing);
            self.hits += 1;
            return existing;
        }

        const entry = try self.allocator.create(Entry);
        errdefer self.allocator.destroy(entry);
        entry.* = .{ .key = key, .bytes = bytes, .prev = null, .next = null, .pins = 1, .encoding = encoding };

        try self.map.put(self.allocator, key, entry);

        self.linkHead(entry);
        // Account the resident cell, not the logical payload, so `capacity_bytes`
        // bounds the pool's actual footprint (slab cells round up by class).
        // `miss_bytes` stays logical — it measures bytes decompressed per miss.
        const resident = huge_page.cellSize(bytes.len);
        self.current_bytes += resident;
        _ = g_miss_bytes.fetchAdd(bytes.len, .monotonic);
        _ = g_cache_bytes.fetchAdd(resident, .monotonic);
        self.evictUnpinnedToCapacity();
        return entry;
    }

    /// Drop one pin previously taken by `acquire`/`insertPinned`. Thread-safe.
    pub fn release(self: *Cache, entry: *Entry) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        std.debug.assert(entry.pins > 0);
        entry.pins -= 1;
    }

    /// Evict unpinned entries from the LRU end until within budget. Pinned
    /// entries are skipped, never freed — so a dataset larger than the cache
    /// (or a moment where the whole tail is in use) temporarily exceeds
    /// capacity rather than freeing a block a reader still holds. Caller holds
    /// `mutex`.
    fn evictUnpinnedToCapacity(self: *Cache) void {
        var victim = self.tail;
        while (self.current_bytes > self.capacity_bytes) {
            const v = victim orelse break;
            const prev = v.prev;
            if (v.pins == 0) {
                self.unlink(v);
                _ = self.map.remove(v.key);
                const resident = huge_page.cellSize(v.bytes.len);
                self.current_bytes -= resident;
                _ = g_cache_bytes.fetchSub(resident, .monotonic);
                self.pool.allocator().free(v.bytes);
                self.allocator.destroy(v);
                self.evictions += 1;
                _ = g_evictions.fetchAdd(1, .monotonic);
            }
            victim = prev;
        }
    }

    fn touch(self: *Cache, entry: *Entry) void {
        if (self.head == entry) return;
        self.unlink(entry);
        self.linkHead(entry);
    }

    fn linkHead(self: *Cache, entry: *Entry) void {
        entry.prev = null;
        entry.next = self.head;
        if (self.head) |h| h.prev = entry;
        self.head = entry;
        if (self.tail == null) self.tail = entry;
    }

    fn unlink(self: *Cache, entry: *Entry) void {
        if (entry.prev) |p| p.next = entry.next else self.head = entry.next;
        if (entry.next) |n| n.prev = entry.prev else self.tail = entry.prev;
        entry.prev = null;
        entry.next = null;
    }
};

// ---------- tests --------------------------------------------------------

fn put(c: *Cache, key: Key, bytes: []align(16) u8) !void {
    c.release(try c.insertPinned(key, bytes, .raw));
}

fn adup(allocator: Allocator, s: []const u8) ![]align(16) u8 {
    const b = try allocator.alignedAlloc(u8, .@"16", s.len);
    @memcpy(b, s);
    return b;
}

test "cache acquire/insert round-trip + hit/miss counters" {
    const allocator = std.testing.allocator;
    var c: Cache = .init(allocator, 1024);
    defer c.deinit();

    const k = Key{ .segment_id = 1, .row_group_idx = 0, .column_idx = 0 };
    try std.testing.expect(c.acquire(k) == null);
    try std.testing.expectEqual(@as(u64, 1), c.misses);

    const e1 = try c.insertPinned(k, try adup(allocator,"hello"), .raw);
    try std.testing.expectEqualStrings("hello", e1.bytes);
    c.release(e1);

    const e2 = c.acquire(k).?;
    try std.testing.expectEqualStrings("hello", e2.bytes);
    try std.testing.expectEqual(@as(u64, 1), c.hits);
    c.release(e2);
}

test "cache evicts unpinned LRU entries when over budget" {
    const allocator = std.testing.allocator;
    var c: Cache = .init(allocator, 30);
    defer c.deinit();

    // Four 10-byte entries, all released so they're evictable; the fourth
    // pushes us to 40 > 30, evicting the LRU tail (rg 0).
    inline for (0..4) |i| {
        try put(&c, .{ .segment_id = 1, .row_group_idx = @intCast(i), .column_idx = 0 }, try adup(allocator,"0123456789"));
    }

    try std.testing.expect(c.acquire(.{ .segment_id = 1, .row_group_idx = 0, .column_idx = 0 }) == null);
    const last = c.acquire(.{ .segment_id = 1, .row_group_idx = 3, .column_idx = 0 }).?;
    c.release(last);
    try std.testing.expectEqual(@as(u64, 1), c.evictions);
}

test "cache LRU order updated on acquire" {
    const allocator = std.testing.allocator;
    var c: Cache = .init(allocator, 20);
    defer c.deinit();

    const k1 = Key{ .segment_id = 1, .row_group_idx = 0, .column_idx = 0 };
    const k2 = Key{ .segment_id = 1, .row_group_idx = 1, .column_idx = 0 };
    try put(&c, k1, try adup(allocator,"0123456789"));
    try put(&c, k2, try adup(allocator,"abcdefghij"));

    // Touch k1 → MRU; k2 becomes the LRU tail.
    c.release(c.acquire(k1).?);

    // Inserting a third 10-byte entry over a cap of 20 evicts the tail (k2).
    const k3 = Key{ .segment_id = 1, .row_group_idx = 2, .column_idx = 0 };
    try put(&c, k3, try adup(allocator,"XXXXXXXXXX"));

    try std.testing.expect(c.acquire(k2) == null);
    c.release(c.acquire(k1).?);
    c.release(c.acquire(k3).?);
}

test "cache never evicts a pinned entry, even over budget" {
    const allocator = std.testing.allocator;
    var c: Cache = .init(allocator, 20);
    defer c.deinit();

    // Hold k1 pinned (in-use) the whole time.
    const k1 = Key{ .segment_id = 1, .row_group_idx = 0, .column_idx = 0 };
    const pinned = try c.insertPinned(k1, try adup(allocator,"0123456789"), .raw);

    // Flood the cache well past budget while k1 — the oldest entry — stays pinned.
    inline for (1..4) |i| {
        try put(&c, .{ .segment_id = 1, .row_group_idx = @intCast(i), .column_idx = 0 }, try adup(allocator,"XXXXXXXXXX"));
    }

    // k1 must still be resident despite being LRU and us being over budget.
    const again = c.acquire(k1).?;
    try std.testing.expectEqualStrings("0123456789", again.bytes);
    c.release(again);
    c.release(pinned);
}

test "cache insertPinned dedupes a racing duplicate key" {
    const allocator = std.testing.allocator;
    var c: Cache = .init(allocator, 1024);
    defer c.deinit();

    const k = Key{ .segment_id = 7, .row_group_idx = 2, .column_idx = 3 };
    const first = try c.insertPinned(k, try adup(allocator,"AAAA"), .raw);
    // Second insert of the same key (simulating a lost decompress race) must
    // free the redundant bytes and hand back the existing entry.
    const second = try c.insertPinned(k, try adup(allocator,"BBBB"), .raw);
    try std.testing.expectEqual(first, second);
    try std.testing.expectEqualStrings("AAAA", second.bytes);
    try std.testing.expectEqual(@as(usize, 4), c.current_bytes);
    c.release(first);
    c.release(second);
}
