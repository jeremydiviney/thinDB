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

    /// Reusable LZ4-decompress scratch buffers. LZ4-at-rest entries decompress
    /// on EVERY access into a transient multi-MB buffer; a fresh heap alloc per
    /// access hands back freshly-zeroed pages each time (the Windows soft-fault
    /// storm the huge-page pool exists to kill for resident entries). A small
    /// pool of recycled buffers pays the page-commit cost once and amortizes it
    /// across every later access.
    scratch: [scratch_slots]?[]align(16) u8 = @splat(null),
    scratch_lock: SpinLock = .{},

    /// Stats (read under `mutex`, for benches and debugging).
    hits: u64 = 0,
    misses: u64 = 0,
    evictions: u64 = 0,

    /// 32 ≈ max-dop workers × a few concurrently-borrowed string columns; the
    /// pool self-bounds at the 32 largest hot block sizes (~5 MB each on the
    /// 100M ClickBench table → ~160 MB worst case, outside the cache budget).
    const scratch_slots = 32;

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
        /// `.lz4` ⟺ `bytes` is the still-compressed on-disk payload (large raw
        /// string blocks stay compressed at rest in the cache — see
        /// `format.Compression.lz4`); accessors must decompress to
        /// `uncompressed_size` per use. `.none` ⟺ `bytes` is directly usable.
        /// `.zstd` never appears here (zstd blocks are decompressed at fill).
        compression: format.Compression = .none,
        uncompressed_size: u32 = 0,
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
        for (self.scratch) |s| {
            if (s) |buf| self.allocator.free(buf);
        }
        self.pool.deinit();
        self.* = undefined;
    }

    /// Check out a scratch buffer of at least `min_len` bytes for a transient
    /// LZ4 decompress. Best-fit from the pool; on a dry pool, allocates rounded
    /// up to 1 MiB granularity so blocks of similar size reuse it later. Return
    /// the FULL slice via `releaseScratch` (callers slice to their length).
    pub fn acquireScratch(self: *Cache, min_len: usize) ![]align(16) u8 {
        self.scratch_lock.lock();
        var best: ?usize = null;
        for (self.scratch, 0..) |s, i| {
            const buf = s orelse continue;
            if (buf.len < min_len) continue;
            if (best == null or buf.len < self.scratch[best.?].?.len) best = i;
        }
        if (best) |i| {
            const buf = self.scratch[i].?;
            self.scratch[i] = null;
            self.scratch_lock.unlock();
            return buf;
        }
        self.scratch_lock.unlock();
        const rounded = std.mem.alignForward(usize, @max(min_len, 1), 1 << 20);
        return self.allocator.alignedAlloc(u8, .@"16", rounded);
    }

    /// Return a buffer from `acquireScratch` to the pool. Fills an empty slot,
    /// else displaces the smallest pooled buffer if this one is larger, else
    /// frees — the pool converges on the `scratch_slots` largest hot sizes.
    pub fn releaseScratch(self: *Cache, buf: []align(16) u8) void {
        self.scratch_lock.lock();
        var smallest: ?usize = null;
        for (self.scratch, 0..) |s, i| {
            const existing = s orelse {
                self.scratch[i] = buf;
                self.scratch_lock.unlock();
                return;
            };
            if (smallest == null or existing.len < self.scratch[smallest.?].?.len) smallest = i;
        }
        if (smallest != null and self.scratch[smallest.?].?.len < buf.len) {
            const evicted = self.scratch[smallest.?].?;
            self.scratch[smallest.?] = buf;
            self.scratch_lock.unlock();
            self.allocator.free(evicted);
            return;
        }
        self.scratch_lock.unlock();
        self.allocator.free(buf);
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
        return self.insertPinnedCompressed(key, bytes, encoding, .none, 0);
    }

    /// `insertPinned` variant for blocks cached at rest in compressed form
    /// (`compression == .lz4`): `bytes` is the on-disk payload and
    /// `uncompressed_size` its decompressed length.
    pub fn insertPinnedCompressed(self: *Cache, key: Key, bytes: []align(16) u8, encoding: format.Encoding, compression: format.Compression, uncompressed_size: u32) !*Entry {
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
        entry.* = .{
            .key = key,
            .bytes = bytes,
            .prev = null,
            .next = null,
            .pins = 1,
            .encoding = encoding,
            .compression = compression,
            .uncompressed_size = uncompressed_size,
        };

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

    const e1 = try c.insertPinned(k, try adup(allocator, "hello"), .raw);
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
        try put(&c, .{ .segment_id = 1, .row_group_idx = @intCast(i), .column_idx = 0 }, try adup(allocator, "0123456789"));
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
    try put(&c, k1, try adup(allocator, "0123456789"));
    try put(&c, k2, try adup(allocator, "abcdefghij"));

    // Touch k1 → MRU; k2 becomes the LRU tail.
    c.release(c.acquire(k1).?);

    // Inserting a third 10-byte entry over a cap of 20 evicts the tail (k2).
    const k3 = Key{ .segment_id = 1, .row_group_idx = 2, .column_idx = 0 };
    try put(&c, k3, try adup(allocator, "XXXXXXXXXX"));

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
    const pinned = try c.insertPinned(k1, try adup(allocator, "0123456789"), .raw);

    // Flood the cache well past budget while k1 — the oldest entry — stays pinned.
    inline for (1..4) |i| {
        try put(&c, .{ .segment_id = 1, .row_group_idx = @intCast(i), .column_idx = 0 }, try adup(allocator, "XXXXXXXXXX"));
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
    const first = try c.insertPinned(k, try adup(allocator, "AAAA"), .raw);
    // Second insert of the same key (simulating a lost decompress race) must
    // free the redundant bytes and hand back the existing entry.
    const second = try c.insertPinned(k, try adup(allocator, "BBBB"), .raw);
    try std.testing.expectEqual(first, second);
    try std.testing.expectEqualStrings("AAAA", second.bytes);
    try std.testing.expectEqual(@as(usize, 4), c.current_bytes);
    c.release(first);
    c.release(second);
}

// ---------------------------------------------------------------------------
// SegmentHandles — per-table cache of opened/parsed segments
// ---------------------------------------------------------------------------

/// Per-table cache of opened segments: the parsed footer (`ReadSegment`) plus
/// the segment's lazily-loaded tombstone list, shared across queries and
/// across a query's scan workers. Parsing a footer costs ~4ms (hundreds of
/// row groups × every column's stats); without this cache each of N workers
/// paid it per segment on EVERY query (12 workers × 6 segments ≈ 61 parses ≈
/// 244ms CPU per query on the 100M ClickBench table).
///
/// Segments are immutable and ids never reused, so a cached entry can't go
/// stale in content — invalidation is pure lifecycle:
///   - `retire` when compaction deletes the files (closing our handle FIRST —
///     Windows refuses to delete an open file),
///   - `clear` on ALTER/TRUNCATE/close (the parse is schema-dependent),
///   - `invalidateTombstones` when DELETE/UPDATE/UPSERT merges new tombstones.
///
/// `tombstones` returns a caller-owned DUPE of the cached list, so readers
/// keep today's ownership semantics and a concurrent invalidation can never
/// free bytes a reader is still walking. Entries are pinned while a scan
/// holds them; `retire` defers destruction to the last `release`.
pub const SegmentHandles = struct {
    const segment_reader = @import("segment_reader.zig");
    const tombstone = @import("tombstone.zig");
    const types_mod = @import("../types.zig");

    pub const Entry = struct {
        segment_id: u64,
        seg: segment_reader.ReadSegment,
        tombs: ?[]u32 = null,
        tombs_loaded: bool = false,
        pins: u32 = 0,
        retired: bool = false,
    };

    map: std.AutoHashMapUnmanaged(u64, *Entry) = .empty,
    lock: std.atomic.Mutex = .unlocked,

    fn lockSpin(self: *SegmentHandles) void {
        while (!self.lock.tryLock()) std.atomic.spinLoopHint();
    }

    /// Get-or-open the segment, pinned. A first touch parses the footer while
    /// holding the lock — one-time per segment per table lifetime; peers spin
    /// only on that cold start.
    pub fn acquire(
        self: *SegmentHandles,
        allocator: Allocator,
        io: std.Io,
        dir: std.Io.Dir,
        file_name: []const u8,
        schema: types_mod.TableSchema,
        segment_id: u64,
    ) !*Entry {
        self.lockSpin();
        defer self.lock.unlock();
        const gop = try self.map.getOrPut(allocator, segment_id);
        if (gop.found_existing) {
            const e = gop.value_ptr.*;
            e.pins += 1;
            return e;
        }
        errdefer _ = self.map.remove(segment_id);
        const entry = try allocator.create(Entry);
        errdefer allocator.destroy(entry);
        entry.* = .{ .segment_id = segment_id, .seg = undefined, .pins = 1 };
        entry.seg = try segment_reader.readSegment(allocator, io, dir, file_name, schema);
        gop.value_ptr.* = entry;
        return entry;
    }

    pub fn release(self: *SegmentHandles, allocator: Allocator, entry: *Entry) void {
        self.lockSpin();
        defer self.lock.unlock();
        entry.pins -= 1;
        if (entry.retired and entry.pins == 0) destroyEntry(allocator, entry);
    }

    /// The segment's tombstone row list as a caller-owned dupe (null = none).
    /// The underlying file is read once and cached until `invalidateTombstones`.
    /// `gpa` must be the table-lifetime allocator: the cached `entry.tombs`
    /// outlives any query, and `invalidateTombstones`/`destroyEntry` free it
    /// with that allocator. A query-scoped allocator here leaves a dangling
    /// cache entry the compactor later frees (#136). `out_allocator` (typically
    /// per-query) owns only the returned dupe.
    pub fn tombstones(
        self: *SegmentHandles,
        gpa: Allocator,
        out_allocator: Allocator,
        io: std.Io,
        dir: std.Io.Dir,
        entry: *Entry,
    ) !?[]u32 {
        self.lockSpin();
        defer self.lock.unlock();
        if (!entry.tombs_loaded) {
            entry.tombs = try tombstone.read(gpa, io, dir, entry.segment_id);
            entry.tombs_loaded = true;
        }
        const t = entry.tombs orelse return null;
        return try out_allocator.dupe(u32, t);
    }

    /// A tombstone merge wrote new offsets for `segment_id` — drop the cached
    /// list so the next reader re-reads the file.
    pub fn invalidateTombstones(self: *SegmentHandles, allocator: Allocator, segment_id: u64) void {
        self.lockSpin();
        defer self.lock.unlock();
        const e = self.map.get(segment_id) orelse return;
        if (e.tombs) |t| allocator.free(t);
        e.tombs = null;
        e.tombs_loaded = false;
    }

    /// Compaction is about to delete this segment's files: close our handle
    /// now (Windows can't delete an open file). Runs under the ddl exclusive
    /// lock, so no reader holds a pin in practice; a pinned entry (defense)
    /// is destroyed by its last `release`.
    pub fn retire(self: *SegmentHandles, allocator: Allocator, segment_id: u64) void {
        self.lockSpin();
        defer self.lock.unlock();
        const kv = self.map.fetchRemove(segment_id) orelse return;
        const e = kv.value;
        if (e.pins == 0) destroyEntry(allocator, e) else e.retired = true;
    }

    /// Drop everything — ALTER/TRUNCATE (schema-dependent parse) and table
    /// close. Callers hold the table exclusively; no pins outstanding.
    pub fn clear(self: *SegmentHandles, allocator: Allocator) void {
        self.lockSpin();
        var it = self.map.valueIterator();
        while (it.next()) |e| destroyEntry(allocator, e.*);
        self.map.clearRetainingCapacity();
        self.lock.unlock();
    }

    pub fn deinit(self: *SegmentHandles, allocator: Allocator) void {
        self.clear(allocator);
        self.map.deinit(allocator);
    }

    fn destroyEntry(allocator: Allocator, e: *Entry) void {
        e.seg.deinit();
        if (e.tombs) |t| allocator.free(t);
        allocator.destroy(e);
    }
};
