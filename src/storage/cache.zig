//! Pinning LRU buffer pool for decompressed column blocks, shared across all
//! queries against ALL tables of a Catalog — one global budget, so the LRU
//! evicts the coldest block process-wide regardless of which table owns it.
//!
//! Keyed by `(table_uid, segment_id, row_group_idx, column_idx)`. Value is the
//! decompressed (but still encoded) bytes of one column block — nothing in
//! the cache is ever held compressed. A cache hit still pays a cheap decode
//! step but skips the block decompress.
//!
//! Lives under the memory controller's one budget (`memory.MemoryPool`):
//! the cache holds what that budget leaves after statements and idle memory,
//! never under the controller's floor (`MemoryPool.cacheTarget`), and gives
//! memory back when the controller asks (`reclaim`). The cache does NOT
//! assume the dataset fits in memory: when over budget it evicts unpinned LRU
//! entries, and if every entry down to budget is currently pinned it
//! temporarily exceeds its target rather than free a block someone is
//! reading.
//!
//! ## Concurrency
//! Thread-per-connection servers share the one cache across every connection
//! and table. All metadata mutations (map, LRU links, byte counter, pin counts)
//! are guarded by `mutex`, held only for the O(1) bookkeeping — never across
//! decompress or decode.
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
//! Segments are immutable once written and, within one table generation,
//! segment IDs come from a monotonic never-reused counter — so a cached block
//! is valid for the entire life of its segment. Retired segments (post
//! compaction) are simply never looked up again and age out via LRU. Segment
//! IDs are PER-TABLE counters though, and TRUNCATE/ALTER reset them — the
//! `table_uid` key component scopes every entry to one table GENERATION: each
//! `Table.open` (and each truncate/alter re-init) takes a fresh uid via
//! `newTableUid`, which invalidates that table's entries in O(1) — the old
//! uid's keys become unreachable and age out (or are eagerly freed via
//! `purgeTable`). The memtable is never cached here.

const std = @import("std");
const Allocator = std.mem.Allocator;
const format = @import("format.zig");
const huge_page = @import("../util/huge_page.zig");
const memory = @import("../memory.zig");

pub const Key = struct {
    /// Table-generation namespace (see the Coherence section above). 0 in
    /// unit tests that exercise a cache directly.
    table_uid: u64 = 0,
    segment_id: u64,
    row_group_idx: u32,
    column_idx: u32,
};

/// A table's handle into the shared block cache: the cache plus the table's
/// uid namespace within it. Passed by value down the segment-read path so key
/// construction at the storage boundary can scope entries to the table
/// generation without the reader knowing about `Table`.
pub const TableCache = struct {
    cache: *Cache,
    table_uid: u64,
};

var next_table_uid = std.atomic.Value(u64).init(1);

/// Mint a process-unique table-generation uid. Never reused, so bumping a
/// table's uid is an O(1) whole-table cache invalidation.
pub fn newTableUid() u64 {
    return next_table_uid.fetchAdd(1, .monotonic);
}

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

/// Lossy set of `Scan` predicate-miss keys ("this predicate matched no stored
/// row of this row group", keyed by table generation, segment, row group and
/// predicate digest). Segments are immutable and ALTER/TRUNCATE re-mint the
/// generation, so a key never goes stale; it is only dead weight once its
/// segment retires. The slots are 4-way set-associative and allocated on the
/// first insert: a full set overwrites one of its ways, so the memo never
/// grows past its size and dead keys age out. 0 marks a free slot, so keys
/// are never 0.
pub const PredicateMemo = struct {
    allocator: Allocator,
    slot_count: usize,
    slots: std.atomic.Value(?[*]u128) = .init(null),
    alloc_lock: SpinLock = .{},
    stripes: [STRIPES]SpinLock = @splat(.{}),

    const WAYS = 4;
    const STRIPES = 64;
    const MIN_SLOTS = 64 * WAYS;

    /// The most slots `max_bytes` holds, a power of two; none (the memo is
    /// off) when that is under `MIN_SLOTS`: too few to be worth a lookup
    /// per row group.
    fn init(allocator: Allocator, max_bytes: usize) PredicateMemo {
        const want = max_bytes / @sizeOf(u128);
        return .{ .allocator = allocator, .slot_count = if (want < MIN_SLOTS) 0 else std.math.floorPowerOfTwo(usize, want) };
    }

    fn deinit(self: *PredicateMemo) void {
        if (self.slots.load(.acquire)) |s| self.allocator.free(s[0..self.slot_count]);
    }

    pub fn slotBytes(self: *const PredicateMemo) usize {
        return self.slot_count * @sizeOf(u128);
    }

    pub fn contains(self: *PredicateMemo, key: u128) bool {
        const slots = self.slots.load(.acquire) orelse return false;
        const k = nonZero(key);
        const set = self.setOf(k);
        const stripe = &self.stripes[set % STRIPES];
        stripe.lock();
        defer stripe.unlock();
        return std.mem.indexOfScalar(u128, slots[set * WAYS ..][0..WAYS], k) != null;
    }

    /// Best effort: a memo that cannot allocate its slots stays empty.
    pub fn insert(self: *PredicateMemo, key: u128) void {
        if (self.slot_count == 0) return;
        const slots = self.slots.load(.acquire) orelse self.allocateSlots() orelse return;
        const k = nonZero(key);
        const set = self.setOf(k);
        const ways = slots[set * WAYS ..][0..WAYS];
        const stripe = &self.stripes[set % STRIPES];
        stripe.lock();
        defer stripe.unlock();
        if (std.mem.indexOfScalar(u128, ways, k) != null) return;
        const way = std.mem.indexOfScalar(u128, ways, 0) orelse @as(usize, @intCast((k >> 64) % WAYS));
        ways[way] = k;
    }

    fn allocateSlots(self: *PredicateMemo) ?[*]u128 {
        self.alloc_lock.lock();
        defer self.alloc_lock.unlock();
        if (self.slots.load(.acquire)) |s| return s;
        const s = self.allocator.alloc(u128, self.slot_count) catch return null;
        @memset(s, 0);
        self.slots.store(s.ptr, .release);
        return s.ptr;
    }

    fn setOf(self: *const PredicateMemo, k: u128) usize {
        return @as(usize, @truncate(k)) & (self.slot_count / WAYS - 1);
    }

    fn nonZero(key: u128) u128 {
        return key | 1;
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
    /// The memory controller whose budget the cache lives under.
    controller: *memory.MemoryPool,
    /// Bytes of the blocks held, each counted at its cell size. Written under
    /// `mutex`; `footprint` reads it without.
    current_bytes: usize = 0,
    /// The part of `current_bytes` the pool's arena holds; the rest came from
    /// its backing allocator. Written under `mutex`.
    arena_bytes: usize = 0,
    /// Bytes inserted since the cache last balanced the controller's budget.
    /// Guarded by `mutex`.
    unbalanced_bytes: usize = 0,
    /// Bytes of the scratch buffers, pooled or checked out.
    scratch_bytes: std.atomic.Value(usize) = .init(0),

    map: std.AutoHashMapUnmanaged(Key, *Entry) = .empty,
    /// Doubly-linked LRU list. Head = most recently used; tail = LRU (next evict).
    head: ?*Entry = null,
    tail: ?*Entry = null,

    /// Guards every field below the allocator/capacity. Held only for O(1)
    /// bookkeeping; never across a decompress or decode.
    mutex: SpinLock = .{},

    /// Reusable scratch buffers for transient per-borrow expansions (FSST
    /// blocks expand to `[offsets][bytes]` on every access). A fresh heap alloc
    /// per access hands back freshly-zeroed pages each time (the Windows
    /// soft-fault storm the huge-page pool exists to kill for resident
    /// entries); a small pool of recycled buffers pays the page-commit cost
    /// once and amortizes it across every later access.
    scratch: [scratch_slots]?[]align(16) u8 = @splat(null),
    scratch_lock: SpinLock = .{},

    /// Stats (read under `mutex`, for benches and debugging).
    hits: u64 = 0,
    misses: u64 = 0,
    evictions: u64 = 0,

    /// Row groups a scan predicate matched no stored row of. Its slots are
    /// charged to the controller's budget whole.
    predicate_memo: PredicateMemo,

    /// 32 ≈ max-dop workers × a few concurrently-expanded string columns; the
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
    };

    /// A cache under `controller`'s budget, with a predicate memo of at most
    /// `memo_bytes` (0: none). It grows within the budget once the
    /// controller shares it (`MemoryPool.shareCache`), which also keeps its
    /// floor and charges its memo.
    pub fn init(allocator: Allocator, controller: *memory.MemoryPool, memo_bytes: usize) Cache {
        const memo = PredicateMemo.init(allocator, memo_bytes);
        return .{
            .allocator = allocator,
            .pool = huge_page.Pool.init(allocator, controller.budget -| memo.slotBytes()),
            .controller = controller,
            .predicate_memo = memo,
        };
    }

    /// Allocator for decompressed block payloads. The segment reader allocates
    /// the bytes it hands to `insertPinned` from here so the cache can free them
    /// through the same pool on eviction.
    pub fn blockAllocator(self: *Cache) Allocator {
        return self.pool.allocator();
    }

    pub fn deinit(self: *Cache) void {
        self.controller.unshareCache(self);
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
        self.predicate_memo.deinit();
        for (self.scratch) |s| {
            if (s) |buf| self.allocator.free(buf);
        }
        self.pool.deinit();
        self.* = undefined;
    }

    /// Check out a scratch buffer of at least `min_len` bytes for a transient
    /// expansion. Best-fit from the pool; on a dry pool, allocates rounded
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
        const buf = try self.allocator.alignedAlloc(u8, .@"16", rounded);
        _ = self.scratch_bytes.fetchAdd(buf.len, .monotonic);
        return buf;
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
            self.freeScratch(evicted);
            return;
        }
        self.scratch_lock.unlock();
        self.freeScratch(buf);
    }

    fn freeScratch(self: *Cache, buf: []align(16) u8) void {
        _ = self.scratch_bytes.fetchSub(buf.len, .monotonic);
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
        const inserted = try self.insertPinnedLocked(key, bytes, encoding);
        if (inserted.balance) self.controller.balance();
        return inserted.entry;
    }

    const Inserted = struct {
        entry: *Entry,
        /// The cache grew a step since it last balanced the controller's
        /// budget; balanced once `mutex` is free, since a balance may
        /// reclaim from this cache.
        balance: bool = false,
    };

    fn insertPinnedLocked(self: *Cache, key: Key, bytes: []align(16) u8, encoding: format.Encoding) !Inserted {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.map.get(key)) |existing| {
            self.pool.allocator().free(bytes);
            existing.pins += 1;
            self.touch(existing);
            self.hits += 1;
            return .{ .entry = existing };
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
        };

        try self.map.put(self.allocator, key, entry);

        self.linkHead(entry);
        // Account the resident cell, not the logical payload, so the capacity
        // bounds the pool's actual footprint (slab cells round up by class).
        // `miss_bytes` stays logical — it measures bytes decompressed per miss.
        const resident = self.countBlock(bytes, true);
        _ = g_miss_bytes.fetchAdd(bytes.len, .monotonic);
        self.evictUnpinned(self.controller.cacheTarget(), 0);
        self.unbalanced_bytes += resident;
        if (self.unbalanced_bytes < self.controller.balanceStep()) return .{ .entry = entry };
        self.unbalanced_bytes = 0;
        return .{ .entry = entry, .balance = true };
    }

    /// Adds a block's cell to the byte counts, or takes it off them, and
    /// returns the cell's bytes. Caller holds `mutex`.
    fn countBlock(self: *Cache, bytes: []align(16) u8, added: bool) usize {
        const cell = huge_page.cellSize(bytes.len);
        const arena_cell = if (self.pool.holds(bytes.ptr)) cell else 0;
        if (added) {
            @atomicStore(usize, &self.current_bytes, self.current_bytes + cell, .monotonic);
            @atomicStore(usize, &self.arena_bytes, self.arena_bytes + arena_cell, .monotonic);
            _ = g_cache_bytes.fetchAdd(cell, .monotonic);
        } else {
            @atomicStore(usize, &self.current_bytes, self.current_bytes - cell, .monotonic);
            @atomicStore(usize, &self.arena_bytes, self.arena_bytes - arena_cell, .monotonic);
            _ = g_cache_bytes.fetchSub(cell, .monotonic);
        }
        return cell;
    }

    /// Bytes the cache holds against the controller's budget: the arena's
    /// committed pages, the blocks from outside the arena and the scratch
    /// buffers.
    pub fn footprint(self: *const Cache) usize {
        const outside = @atomicLoad(usize, &self.current_bytes, .monotonic) -| @atomicLoad(usize, &self.arena_bytes, .monotonic);
        return self.pool.committedBytes() +| outside +| self.scratch_bytes.load(.monotonic);
    }

    /// What the cache holds beyond its blocks' cells and its free pages: the
    /// unused cells of the arena's regions, and the scratch buffers. Evicting
    /// a block leaves its cell unused until its region empties, so the cache
    /// counts this apart from its blocks or it would evict to cover it.
    pub fn overhead(self: *const Cache) usize {
        return self.pool.carvedBytes() -| @atomicLoad(usize, &self.arena_bytes, .monotonic) +| self.scratch_bytes.load(.monotonic);
    }

    /// Gives back up to about `want` bytes of arena pages without losing a
    /// block: free pages are decommitted, and when they fall short, blocks
    /// move out of sparse slabs so those empty too. Returns the bytes given
    /// back.
    pub fn releaseFreePages(self: *Cache, want: usize) usize {
        const given = self.pool.trim(want);
        if (given >= want) return given;
        self.compact(want - given);
        return given + self.pool.trim(want - given);
    }

    /// Moves unpinned blocks out of the slabs a compaction pass drains, into
    /// free cells of fuller slabs, until about `want` bytes of slabs are
    /// empty. A block is copied with the mutex dropped and a pin held, so it
    /// stays put; a reader that pins it meanwhile may be reading the old
    /// bytes, so then the copy is dropped instead. A hit relinks the block
    /// being copied at the head and ends the walk, hence the second pass.
    fn compact(self: *Cache, want: usize) void {
        if (!self.pool.beginCompaction(want)) return;
        defer self.pool.endCompaction();
        const goal = self.pool.carvedBytes() -| want;
        const block_alloc = self.pool.allocator();
        self.mutex.lock();
        defer self.mutex.unlock();
        var passes: usize = 0;
        while (passes < 2 and self.pool.carvedBytes() > goal) : (passes += 1) {
            var cursor = self.tail;
            while (cursor) |entry| : (cursor = entry.prev) {
                if (self.pool.carvedBytes() <= goal) break;
                if (entry.pins != 0) continue;
                const into = self.pool.relocation(entry.bytes.ptr) orelse continue;
                const moved: []align(16) u8 = @alignCast(into[0..entry.bytes.len]);
                entry.pins += 1;
                self.mutex.unlock();
                @memcpy(moved, entry.bytes);
                self.mutex.lock();
                entry.pins -= 1;
                if (entry.pins == 0) {
                    block_alloc.free(entry.bytes);
                    entry.bytes = moved;
                } else {
                    block_alloc.free(moved);
                }
            }
        }
    }

    /// Gives the controller about `want` bytes: unpinned blocks go, least
    /// recently used first, and the arena pages they empty go back to the OS,
    /// until the footprint has shrunk by `want` or the next block would take
    /// the cache under `floor` bytes. Returns the bytes the footprint shrank
    /// by. An arena block's pages go back once its slab is empty, which
    /// `releaseFreePages` brings about by moving the blocks evictions leave
    /// in sparse slabs; past twice `want` evicted, the rest waits for the
    /// next balance.
    pub fn reclaim(self: *Cache, want: usize, floor: usize) usize {
        var given: usize = 0;
        var evicted: usize = 0;
        while (given < want and evicted < want *| 2) {
            self.mutex.lock();
            const before = self.current_bytes;
            const outside_before = self.current_bytes - self.arena_bytes;
            self.evictUnpinned(self.current_bytes -| (want - given), floor);
            const gone = before - self.current_bytes;
            const outside_freed = outside_before - (self.current_bytes - self.arena_bytes);
            self.mutex.unlock();
            if (gone == 0) break;
            evicted += gone;
            given += outside_freed;
            given += self.releaseFreePages(want -| given);
        }
        return given;
    }

    /// Drop one pin previously taken by `acquire`/`insertPinned`. Thread-safe.
    pub fn release(self: *Cache, entry: *Entry) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        std.debug.assert(entry.pins > 0);
        entry.pins -= 1;
    }

    /// Evict unpinned entries from the LRU end until `target` bytes are left,
    /// stopping where the next one would leave fewer than `floor`. Pinned
    /// entries are skipped, never freed — so a dataset larger than the
    /// cache (or a moment where the whole tail is in use) temporarily exceeds
    /// capacity rather than freeing a block a reader still holds. Caller holds
    /// `mutex`.
    fn evictUnpinned(self: *Cache, target: usize, floor: usize) void {
        var victim = self.tail;
        while (self.current_bytes > target) {
            const v = victim orelse break;
            const prev = v.prev;
            if (v.pins == 0) {
                if (self.current_bytes - huge_page.cellSize(v.bytes.len) < floor) break;
                self.unlink(v);
                _ = self.map.remove(v.key);
                _ = self.countBlock(v.bytes, false);
                self.pool.allocator().free(v.bytes);
                self.allocator.destroy(v);
                self.evictions += 1;
                _ = g_evictions.fetchAdd(1, .monotonic);
            }
            victim = prev;
        }
    }

    /// Eagerly free every unpinned entry belonging to `table_uid`. Correctness
    /// comes from the uid bump alone (the old uid's keys are never looked up
    /// again); this just returns the dead bytes to the budget immediately —
    /// table close, TRUNCATE, ALTER. Pinned entries are skipped (a concurrent
    /// reader may still be decoding from them) and age out via LRU.
    pub fn purgeTable(self: *Cache, table_uid: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        var cur = self.tail;
        while (cur) |e| {
            const prev = e.prev;
            if (e.key.table_uid == table_uid and e.pins == 0) {
                self.unlink(e);
                _ = self.map.remove(e.key);
                _ = self.countBlock(e.bytes, false);
                self.pool.allocator().free(e.bytes);
                self.allocator.destroy(e);
            }
            cur = prev;
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

/// A cache that keeps all of `controller`'s budget as its floor: the budget
/// also counts the process's idle scratch memory, which must not shrink the
/// cache under a test.
fn shareWhole(c: *Cache, controller: *memory.MemoryPool) void {
    c.* = .init(std.testing.allocator, controller, 0);
    controller.shareCache(c, controller.budget);
}

test "cache acquire/insert round-trip + hit/miss counters" {
    const allocator = std.testing.allocator;
    var controller = memory.MemoryPool.init(1024);
    var c: Cache = undefined;
    shareWhole(&c, &controller);
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

test "predicate memo: at most its size, lossy per set, off when tiny" {
    const allocator = std.testing.allocator;
    var controller = memory.MemoryPool.init(8 << 20);
    var c: Cache = .init(allocator, &controller, 10 << 10);
    defer c.deinit();
    const memo = &c.predicate_memo;
    try std.testing.expectEqual(@as(usize, 512), memo.slot_count);
    try std.testing.expectEqual(@as(usize, 8 << 10), memo.slotBytes());

    try std.testing.expect(!memo.contains(42));
    memo.insert(42);
    memo.insert(42);
    try std.testing.expect(memo.contains(42));
    try std.testing.expect(!memo.contains(43 << 64 | 42));

    // Five keys of one set: the set keeps four, so the fifth overwrites one.
    const sets = memo.slot_count / PredicateMemo.WAYS;
    var kept: usize = 0;
    for (1..6) |hi| memo.insert(@as(u128, hi) << 64 | (7 + sets));
    for (1..6) |hi| kept += @intFromBool(memo.contains(@as(u128, hi) << 64 | (7 + sets)));
    try std.testing.expectEqual(@as(usize, PredicateMemo.WAYS), kept);
    try std.testing.expect(memo.contains(@as(u128, 5) << 64 | (7 + sets)));
    try std.testing.expect(memo.contains(42));

    var tiny: Cache = .init(allocator, &controller, 2 << 10);
    defer tiny.deinit();
    try std.testing.expectEqual(@as(usize, 0), tiny.predicate_memo.slot_count);
    tiny.predicate_memo.insert(42);
    try std.testing.expect(!tiny.predicate_memo.contains(42));
}

test "cache evicts unpinned LRU entries when over budget" {
    const allocator = std.testing.allocator;
    var controller = memory.MemoryPool.init(30);
    var c: Cache = undefined;
    shareWhole(&c, &controller);
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
    var controller = memory.MemoryPool.init(20);
    var c: Cache = undefined;
    shareWhole(&c, &controller);
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
    var controller = memory.MemoryPool.init(20);
    var c: Cache = undefined;
    shareWhole(&c, &controller);
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
    var controller = memory.MemoryPool.init(1024);
    var c: Cache = undefined;
    shareWhole(&c, &controller);
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

test "shared cache evicts globally: oldest block goes regardless of table" {
    const allocator = std.testing.allocator;
    var controller = memory.MemoryPool.init(30);
    var c: Cache = undefined;
    shareWhole(&c, &controller);
    defer c.deinit();

    // Table A inserts two blocks, then table B inserts two. Same (segment, rg,
    // col) coordinates on both tables — the uid keeps them distinct. The fourth
    // insert pushes 40 > 30 and must evict the globally-oldest block (table
    // A's rg 0), not something scoped to the inserting table.
    try put(&c, .{ .table_uid = 1, .segment_id = 1, .row_group_idx = 0, .column_idx = 0 }, try adup(allocator, "AAAAAAAAAA"));
    try put(&c, .{ .table_uid = 1, .segment_id = 1, .row_group_idx = 1, .column_idx = 0 }, try adup(allocator, "aaaaaaaaaa"));
    try put(&c, .{ .table_uid = 2, .segment_id = 1, .row_group_idx = 0, .column_idx = 0 }, try adup(allocator, "BBBBBBBBBB"));
    try put(&c, .{ .table_uid = 2, .segment_id = 1, .row_group_idx = 1, .column_idx = 0 }, try adup(allocator, "bbbbbbbbbb"));

    try std.testing.expect(c.acquire(.{ .table_uid = 1, .segment_id = 1, .row_group_idx = 0, .column_idx = 0 }) == null);
    const b0 = c.acquire(.{ .table_uid = 2, .segment_id = 1, .row_group_idx = 0, .column_idx = 0 }).?;
    try std.testing.expectEqualStrings("BBBBBBBBBB", b0.bytes);
    c.release(b0);
    try std.testing.expectEqual(@as(u64, 1), c.evictions);
}

test "purgeTable frees one table's unpinned entries, leaves other tables and pins" {
    const allocator = std.testing.allocator;
    var controller = memory.MemoryPool.init(1024);
    var c: Cache = undefined;
    shareWhole(&c, &controller);
    defer c.deinit();

    const dead_free = Key{ .table_uid = 5, .segment_id = 1, .row_group_idx = 0, .column_idx = 0 };
    const dead_pinned = Key{ .table_uid = 5, .segment_id = 1, .row_group_idx = 1, .column_idx = 0 };
    const live = Key{ .table_uid = 6, .segment_id = 1, .row_group_idx = 0, .column_idx = 0 };
    try put(&c, dead_free, try adup(allocator, "XXXX"));
    const pinned = try c.insertPinned(dead_pinned, try adup(allocator, "YYYY"), .raw);
    try put(&c, live, try adup(allocator, "ZZZZ"));

    c.purgeTable(5);

    try std.testing.expect(c.acquire(dead_free) == null);
    c.release(c.acquire(live).?);
    // The pinned entry survives the purge (a reader may be mid-decode) and is
    // simply unreachable once the table's uid moves on; it drains via LRU.
    try std.testing.expectEqualStrings("YYYY", pinned.bytes);
    c.release(pinned);
    c.purgeTable(5);
    try std.testing.expect(c.acquire(dead_pinned) == null);
}

fn putBlock(c: *Cache, index: usize, len: usize) !void {
    const bytes = try c.blockAllocator().alignedAlloc(u8, .@"16", len);
    try put(c, .{ .segment_id = 1, .row_group_idx = @intCast(index), .column_idx = 0 }, bytes);
}

test "cache footprint counts arena pages, outside blocks and scratch; reclaim stops at twice its ask and at the floor" {
    const allocator = std.testing.allocator;
    const mib: usize = 1 << 20;
    var controller = memory.MemoryPool.init(64 * mib);
    var c: Cache = undefined;
    shareWhole(&c, &controller);
    defer c.deinit();
    if (c.pool.arena.len == 0) return error.SkipZigTest;
    // Two slabs of eight 1 MiB cells, then a block from outside the arena.
    for (0..16) |i| try putBlock(&c, i, mib);
    try put(&c, .{ .segment_id = 2, .row_group_idx = 0, .column_idx = 0 }, try adup(allocator, "small"));
    try std.testing.expectEqual(16 * mib + 5, c.current_bytes);
    try std.testing.expectEqual(16 * mib, c.arena_bytes);
    try std.testing.expectEqual(16 * mib + 5, c.footprint());
    try std.testing.expectEqual(@as(usize, 0), c.overhead());
    c.releaseScratch(try c.acquireScratch(mib + 1));
    try std.testing.expectEqual(18 * mib + 5, c.footprint());
    try std.testing.expectEqual(2 * mib, c.overhead());

    // Six of the first slab's blocks go and none of its pages: it still
    // holds two.
    try std.testing.expectEqual(@as(usize, 0), c.reclaim(3 * mib, 0));
    try std.testing.expectEqual(10 * mib + 5, c.current_bytes);
    try std.testing.expectEqual(8 * mib, c.overhead());
    // Two more empty it, and a huge page of it goes back.
    try std.testing.expectEqual(2 * mib, c.reclaim(2 * mib, 0));
    try std.testing.expectEqual(16 * mib + 5, c.footprint());
    try std.testing.expectEqual(6 * mib, c.releaseFreePages(std.math.maxInt(usize)));
    try std.testing.expectEqual(10 * mib + 5, c.footprint());

    // Asked for everything, the cache stops before the block that would take
    // it under the floor.
    try std.testing.expectEqual(@as(usize, 0), c.reclaim(64 * mib, 4 * mib));
    try std.testing.expectEqual(4 * mib + 5, c.current_bytes);
    try std.testing.expectEqual(@as(u64, 12), c.evictions);
}

test "freeing pages moves a sparse slab's blocks into a fuller one, losing none and leaving pinned ones be" {
    const mib: usize = 1 << 20;
    var controller = memory.MemoryPool.init(64 * mib);
    var c: Cache = undefined;
    shareWhole(&c, &controller);
    defer c.deinit();
    if (c.pool.arena.len == 0) return error.SkipZigTest;
    // Two slabs of eight 1 MiB cells, block i filled with byte i. Purging
    // table 2 leaves the first slab six blocks and the second two.
    for (0..16) |i| {
        const bytes = try c.blockAllocator().alignedAlloc(u8, .@"16", mib);
        @memset(bytes, @intCast(i));
        const purged = i < 2 or (i >= 8 and i < 14);
        try put(&c, .{ .table_uid = if (purged) 2 else 1, .segment_id = 1, .row_group_idx = @intCast(i), .column_idx = 0 }, bytes);
    }
    c.purgeTable(2);
    try std.testing.expectEqual(8 * mib, c.current_bytes);
    try std.testing.expectEqual(16 * mib, c.footprint());
    const evictions = c.evictions;

    const held = c.acquire(.{ .table_uid = 1, .segment_id = 1, .row_group_idx = 15, .column_idx = 0 }).?;
    try std.testing.expectEqual(@as(usize, 0), c.releaseFreePages(8 * mib));
    c.release(held);
    try std.testing.expectEqual(8 * mib, c.releaseFreePages(8 * mib));
    try std.testing.expectEqual(8 * mib, c.footprint());
    try std.testing.expectEqual(@as(usize, 0), c.overhead());
    try std.testing.expectEqual(evictions, c.evictions);
    for ([_]u32{ 2, 3, 4, 5, 6, 7, 14, 15 }) |i| {
        const entry = c.acquire(.{ .table_uid = 1, .segment_id = 1, .row_group_idx = i, .column_idx = 0 }).?;
        defer c.release(entry);
        for (entry.bytes) |byte| try std.testing.expectEqual(@as(u8, @intCast(i)), byte);
    }
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
///   - `publishTombstones` when DELETE/UPDATE/UPSERT adds tombstones.
///
/// A published list reaches readers before its tombstone file does: it
/// stays in `unwritten`, outranking the file, until the table writes the
/// file outside its mutex and `settleTombstones` drops it (#605).
///
/// Readers share the cached tombstone list through `acquireTombstones`: a
/// reference-counted, immutable list, so an invalidation can never free
/// bytes a reader is still walking. Scans reopen a segment for every tile
/// they claim; a per-open copy of a multi-MB list under `lock` serialized
/// the whole worker fleet (#564). Entries are pinned while a scan holds
/// them; `retire` defers destruction to the last `release`.
///
/// A snapshot reads tombstones as of its capture (DESIGN §6.1) through the
/// `TombstoneEpoch` it pins (`pinEpoch`, then the `...At` readers), so a
/// delete published mid-query never reaches a segment the query opens
/// later.
pub const SegmentHandles = struct {
    const segment_reader = @import("segment_reader.zig");
    const tombstone = @import("tombstone.zig");
    const types_mod = @import("../types.zig");
    const column = @import("column.zig");

    /// A loaded, non-empty tombstone list. `refs` counts the entry's own
    /// reference plus one per reader; the last `unref` frees it.
    pub const Tombstones = struct {
        rows: []const u32,
        refs: std.atomic.Value(u32),

        fn unref(self: *Tombstones, gpa: Allocator) void {
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            gpa.free(self.rows);
            gpa.destroy(self);
        }
    };

    /// One state of the table's tombstones. Each publish starts a new
    /// epoch carrying the rows it added to one segment, and the epoch it
    /// supersedes links to it while anything pins that one. A pinned epoch
    /// so keeps every later one alive, and a segment's list as of it is the
    /// newest list minus what the later epochs added. `refs` counts pins,
    /// the newest epoch's table reference and a predecessor's link.
    pub const TombstoneEpoch = struct {
        refs: std.atomic.Value(u32),
        segment_id: u64 = 0,
        /// Sorted, and none were tombstoned before. Dropped when nothing can
        /// read through it: no older epoch was pinned when it was published.
        added: []const u32 = &.{},
        next: ?*TombstoneEpoch = null,
        /// Each segment's list as of this epoch (null = none), kept on its
        /// first read for the rest of the snapshot's segment opens.
        views: std.AutoHashMapUnmanaged(u64, ?*Tombstones) = .empty,
    };

    /// A list ready to publish, with the epoch its publish starts.
    pub const Pending = struct {
        segment_id: u64,
        tombs: *Tombstones,
        epoch: *TombstoneEpoch,
    };

    pub const Entry = struct {
        segment_id: u64,
        seg: segment_reader.ReadSegment,
        tombs: ?*Tombstones = null,
        tombs_loaded: bool = false,
        pins: u32 = 0,
        retired: bool = false,
        /// Per column, what the GROUP BY router sampled from each row
        /// group's block, kept for every later plan. A column's list is
        /// allocated on its first sample.
        string_samples: [][]StringSample = &.{},
    };

    /// One row group's block of a string column as the GROUP BY router
    /// sampled it: the bytes it decodes to (`rowGroupStringBytes`) and its
    /// sampled distinct values (`rowGroupDistinctStrings`).
    pub const StringSample = struct {
        bytes: ?u64 = null,
        distinct: ?DistinctCount = null,
    };

    pub const DistinctCount = struct {
        bytes: u64,
        values: u64,
    };

    map: std.AutoHashMapUnmanaged(u64, *Entry) = .empty,
    /// Per segment, the newest published list its tombstone file doesn't
    /// hold yet. Not handle state: `clear` keeps it, since dropping it would
    /// bring deleted rows back.
    unwritten: std.AutoHashMapUnmanaged(u64, *Tombstones) = .empty,
    unwritten_count: std.atomic.Value(usize) = .init(0),
    /// Segment-only deletes need to invalidate cached query state even when
    /// the memtable and manifest do not change, or no handle was cached.
    tombstone_generation: std.atomic.Value(u64) = .init(0),
    /// The newest `TombstoneEpoch`, created by the first pin or publish.
    epoch: ?*TombstoneEpoch = null,
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

    /// The segment's tombstone list, shared (null = none, including an empty
    /// file); pair with `releaseTombstones`. The newest published list, else
    /// the file, read once and cached until the next publish. `gpa` must be
    /// the table-lifetime allocator: the cached list outlives any query and
    /// its last reference can drop on any thread. A query-scoped allocator
    /// here leaves a dangling cache entry the compactor later frees (#136).
    pub fn acquireTombstones(
        self: *SegmentHandles,
        gpa: Allocator,
        io: std.Io,
        dir: std.Io.Dir,
        entry: *Entry,
    ) !?*Tombstones {
        return self.acquireTombstonesAt(gpa, io, dir, entry, null);
    }

    /// `acquireTombstones` as of `epoch` (null = the newest list).
    pub fn acquireTombstonesAt(
        self: *SegmentHandles,
        gpa: Allocator,
        io: std.Io,
        dir: std.Io.Dir,
        entry: *Entry,
        epoch: ?*TombstoneEpoch,
    ) !?*Tombstones {
        const as_of = blk: {
            self.lockSpin();
            defer self.lock.unlock();
            try self.loadEntryTombstones(gpa, io, dir, entry);
            const current = refTombstones(entry.tombs);
            const pinned = epoch orelse return current;
            break :blk self.asOfLocked(gpa, pinned, entry.segment_id, current);
        };
        return self.finishAsOf(gpa, epoch.?, entry.segment_id, as_of);
    }

    /// `acquireTombstones` by segment id, for a reader that holds no handle.
    /// Reads the file uncached when no handle is cached either.
    pub fn currentTombstones(
        self: *SegmentHandles,
        gpa: Allocator,
        io: std.Io,
        dir: std.Io.Dir,
        segment_id: u64,
    ) !?*Tombstones {
        return self.currentTombstonesAt(gpa, io, dir, segment_id, null);
    }

    /// `currentTombstones` as of `epoch` (null = the newest list).
    pub fn currentTombstonesAt(
        self: *SegmentHandles,
        gpa: Allocator,
        io: std.Io,
        dir: std.Io.Dir,
        segment_id: u64,
        epoch: ?*TombstoneEpoch,
    ) !?*Tombstones {
        const as_of = blk: {
            self.lockSpin();
            defer self.lock.unlock();
            const current = if (self.map.get(segment_id)) |entry| current: {
                try self.loadEntryTombstones(gpa, io, dir, entry);
                break :current refTombstones(entry.tombs);
            } else if (self.unwritten.get(segment_id)) |t|
                refTombstones(t)
            else
                try loadTombstones(gpa, io, dir, segment_id);
            const pinned = epoch orelse return current;
            break :blk self.asOfLocked(gpa, pinned, segment_id, current);
        };
        return self.finishAsOf(gpa, epoch.?, segment_id, as_of);
    }

    fn refTombstones(t: ?*Tombstones) ?*Tombstones {
        if (t) |list| _ = list.refs.fetchAdd(1, .monotonic);
        return t;
    }

    const AsOf = union(enum) {
        done: ?*Tombstones,
        /// `current` minus what the epochs after the pinned one, through
        /// `newest`, added to the segment.
        build: struct { current: *Tombstones, newest: *TombstoneEpoch },
    };

    /// Segment `segment_id`'s list as of `pinned`, given its newest list
    /// `current`, whose reference this takes.
    fn asOfLocked(self: *SegmentHandles, gpa: Allocator, pinned: *TombstoneEpoch, segment_id: u64, current: ?*Tombstones) AsOf {
        if (pinned == self.epoch) return .{ .done = current };
        if (pinned.views.get(segment_id)) |view| {
            if (current) |c| c.unref(gpa);
            return .{ .done = refTombstones(view) };
        }
        if (current) |c| {
            var later = pinned.next;
            while (later) |e| : (later = e.next) {
                if (e.segment_id == segment_id and e.added.len > 0) return .{ .build = .{ .current = c, .newest = self.epoch.? } };
            }
        }
        rememberView(gpa, pinned, segment_id, current);
        return .{ .done = current };
    }

    fn finishAsOf(self: *SegmentHandles, gpa: Allocator, pinned: *TombstoneEpoch, segment_id: u64, as_of: AsOf) !?*Tombstones {
        const build = switch (as_of) {
            .done => |t| return t,
            .build => |b| b,
        };
        defer build.current.unref(gpa);
        const view = try listWithout(gpa, build.current.rows, pinned, build.newest, segment_id);
        self.lockSpin();
        defer self.lock.unlock();
        if (pinned.views.get(segment_id)) |raced| {
            if (view) |v| v.unref(gpa);
            return refTombstones(raced);
        }
        rememberView(gpa, pinned, segment_id, view);
        return view;
    }

    /// Keep `view` as `epoch`'s list of `segment_id`, which it has none of
    /// yet. A failed allocation only costs a later open the rebuild.
    fn rememberView(gpa: Allocator, epoch: *TombstoneEpoch, segment_id: u64, view: ?*Tombstones) void {
        epoch.views.put(gpa, segment_id, view) catch return;
        _ = refTombstones(view);
    }

    /// `rows` without what the epochs after `pinned`, through `newest`,
    /// added to `segment_id` (null when nothing is left).
    fn listWithout(gpa: Allocator, rows: []const u32, pinned: *TombstoneEpoch, newest: *TombstoneEpoch, segment_id: u64) !?*Tombstones {
        var count: usize = 0;
        var later = pinned;
        while (later != newest) {
            later = later.next.?;
            if (later.segment_id == segment_id) count += later.added.len;
        }
        const removed = try gpa.alloc(u32, count);
        defer gpa.free(removed);
        var filled: usize = 0;
        later = pinned;
        while (later != newest) {
            later = later.next.?;
            if (later.segment_id != segment_id) continue;
            @memcpy(removed[filled..][0..later.added.len], later.added);
            filled += later.added.len;
        }
        std.sort.pdq(u32, removed, {}, std.sort.asc(u32));

        const kept = try gpa.alloc(u32, rows.len);
        var len: usize = 0;
        var j: usize = 0;
        for (rows) |row| {
            while (j < removed.len and removed[j] < row) j += 1;
            if (j < removed.len and removed[j] == row) continue;
            kept[len] = row;
            len += 1;
        }
        if (len == 0) {
            gpa.free(kept);
            return null;
        }
        const owned = gpa.realloc(kept, len) catch |err| {
            gpa.free(kept);
            return err;
        };
        errdefer gpa.free(owned);
        return try ownTombstones(gpa, owned);
    }

    /// Pin the newest epoch for a snapshot; pair with `unpinEpoch`. Callers
    /// hold the table mutex, so the epoch matches the rest of the snapshot.
    pub fn pinEpoch(self: *SegmentHandles, gpa: Allocator) !*TombstoneEpoch {
        self.lockSpin();
        defer self.lock.unlock();
        const epoch = self.epoch orelse blk: {
            const first = try gpa.create(TombstoneEpoch);
            first.* = .{ .refs = .init(1) };
            self.epoch = first;
            break :blk first;
        };
        _ = epoch.refs.fetchAdd(1, .monotonic);
        return epoch;
    }

    /// Another pin on an already pinned epoch.
    pub fn repinEpoch(epoch: *TombstoneEpoch) void {
        _ = epoch.refs.fetchAdd(1, .monotonic);
    }

    pub fn unpinEpoch(gpa: Allocator, epoch: *TombstoneEpoch) void {
        var next: ?*TombstoneEpoch = epoch;
        while (next) |e| {
            if (e.refs.fetchSub(1, .acq_rel) != 1) return;
            next = e.next;
            gpa.free(e.added);
            var it = e.views.valueIterator();
            while (it.next()) |view| if (view.*) |t| t.unref(gpa);
            e.views.deinit(gpa);
            gpa.destroy(e);
        }
    }

    /// The epoch a publish of `added` to `segment_id` starts, owning
    /// `added` (sorted, newly tombstoned, allocated with `gpa`) once this
    /// returns.
    pub fn newEpoch(gpa: Allocator, segment_id: u64, added: []const u32) !*TombstoneEpoch {
        const epoch = try gpa.create(TombstoneEpoch);
        epoch.* = .{ .refs = .init(1), .segment_id = segment_id, .added = added };
        return epoch;
    }

    pub fn releasePending(gpa: Allocator, p: Pending) void {
        p.tombs.unref(gpa);
        unpinEpoch(gpa, p.epoch);
    }

    /// Make `epoch`, with its one table reference, the newest. Its
    /// additions only matter to a snapshot pinned on an older epoch.
    fn advanceEpochLocked(self: *SegmentHandles, gpa: Allocator, epoch: *TombstoneEpoch) void {
        const previous = self.epoch;
        self.epoch = epoch;
        const prev = previous orelse {
            dropAdded(gpa, epoch);
            return;
        };
        // Only the table's reference: pins take `lock`, so none can come.
        if (prev.refs.load(.acquire) == 1) {
            dropAdded(gpa, epoch);
        } else {
            _ = epoch.refs.fetchAdd(1, .monotonic);
            prev.next = epoch;
        }
        unpinEpoch(gpa, prev);
    }

    fn dropAdded(gpa: Allocator, epoch: *TombstoneEpoch) void {
        gpa.free(epoch.added);
        epoch.added = &.{};
    }

    fn loadEntryTombstones(self: *SegmentHandles, gpa: Allocator, io: std.Io, dir: std.Io.Dir, entry: *Entry) !void {
        if (entry.tombs_loaded) return;
        if (self.unwritten.get(entry.segment_id)) |t| {
            _ = t.refs.fetchAdd(1, .monotonic);
            entry.tombs = t;
        } else {
            entry.tombs = try loadTombstones(gpa, io, dir, entry.segment_id);
        }
        entry.tombs_loaded = true;
    }

    pub fn releaseTombstones(gpa: Allocator, t: *Tombstones) void {
        t.unref(gpa);
    }

    fn loadTombstones(gpa: Allocator, io: std.Io, dir: std.Io.Dir, segment_id: u64) !?*Tombstones {
        const rows = (try tombstone.read(gpa, io, dir, segment_id)) orelse return null;
        if (rows.len == 0) {
            gpa.free(rows);
            return null;
        }
        errdefer gpa.free(rows);
        return try ownTombstones(gpa, rows);
    }

    /// A list holding `rows` (sorted, deduped, non-empty, allocated with
    /// `gpa`), with the caller's one reference.
    pub fn ownTombstones(gpa: Allocator, rows: []const u32) !*Tombstones {
        const t = try gpa.create(Tombstones);
        t.* = .{ .rows = rows, .refs = .init(1) };
        return t;
    }

    /// Room for `count` more `publishTombstones`, so a commit can publish
    /// without failing.
    pub fn reserveTombstones(self: *SegmentHandles, gpa: Allocator, count: usize) !void {
        self.lockSpin();
        defer self.lock.unlock();
        try self.unwritten.ensureUnusedCapacity(gpa, @intCast(count));
    }

    /// Make `p.tombs` segment `p.segment_id`'s list for every later reader
    /// and start `p.epoch`, taking the caller's references. Readers holding
    /// the previous list keep it until they release. Needs a
    /// `reserveTombstones` slot. Callers hold the table mutex, so publishes
    /// don't race each other.
    pub fn publishTombstones(self: *SegmentHandles, gpa: Allocator, p: Pending) void {
        const segment_id = p.segment_id;
        const t = p.tombs;
        self.lockSpin();
        defer self.lock.unlock();
        _ = self.tombstone_generation.fetchAdd(1, .monotonic);
        self.advanceEpochLocked(gpa, p.epoch);
        const gop = self.unwritten.getOrPutAssumeCapacity(segment_id);
        if (gop.found_existing) gop.value_ptr.*.unref(gpa) else _ = self.unwritten_count.fetchAdd(1, .monotonic);
        gop.value_ptr.* = t;
        const e = self.map.get(segment_id) orelse return;
        if (e.tombs) |old| old.unref(gpa);
        _ = t.refs.fetchAdd(1, .monotonic);
        e.tombs = t;
        e.tombs_loaded = true;
    }

    pub const Unwritten = struct { segment_id: u64, tombs: *Tombstones };

    pub fn hasUnwritten(self: *const SegmentHandles) bool {
        return self.unwritten_count.load(.monotonic) > 0;
    }

    /// Append each unwritten list to `out`, with a reference the caller
    /// releases.
    pub fn collectUnwritten(self: *SegmentHandles, gpa: Allocator, out: *std.ArrayList(Unwritten)) !void {
        self.lockSpin();
        defer self.lock.unlock();
        try out.ensureUnusedCapacity(gpa, self.unwritten.count());
        var it = self.unwritten.iterator();
        while (it.next()) |kv| {
            _ = kv.value_ptr.*.refs.fetchAdd(1, .monotonic);
            out.appendAssumeCapacity(.{ .segment_id = kv.key_ptr.*, .tombs = kv.value_ptr.* });
        }
    }

    /// Segment `segment_id`'s tombstone file now holds `t`. Drops it from
    /// `unwritten` unless a newer list replaced it meanwhile.
    pub fn settleTombstones(self: *SegmentHandles, gpa: Allocator, segment_id: u64, t: *Tombstones) void {
        self.lockSpin();
        defer self.lock.unlock();
        const current = self.unwritten.get(segment_id) orelse return;
        if (current != t) return;
        _ = self.unwritten.remove(segment_id);
        _ = self.unwritten_count.fetchSub(1, .monotonic);
        t.unref(gpa);
    }

    /// Forget every unwritten list, for a caller whose files now hold an
    /// older state on purpose (an XA rollback restoring them).
    pub fn discardUnwritten(self: *SegmentHandles, gpa: Allocator) void {
        self.lockSpin();
        defer self.lock.unlock();
        self.discardUnwrittenLocked(gpa);
        var it = self.map.valueIterator();
        while (it.next()) |e| {
            if (e.*.tombs) |t| t.unref(gpa);
            e.*.tombs = null;
            e.*.tombs_loaded = false;
        }
    }

    fn discardUnwrittenLocked(self: *SegmentHandles, gpa: Allocator) void {
        var it = self.unwritten.valueIterator();
        while (it.next()) |t| t.*.unref(gpa);
        self.unwritten.clearRetainingCapacity();
        self.unwritten_count.store(0, .monotonic);
    }

    /// Compaction is about to delete this segment's files: close our handle
    /// now (Windows can't delete an open file), and forget its unwritten
    /// list. Runs under the ddl exclusive lock, so no reader holds a pin in
    /// practice; a pinned entry (defense) is destroyed by its last `release`.
    pub fn retire(self: *SegmentHandles, allocator: Allocator, segment_id: u64) void {
        self.lockSpin();
        defer self.lock.unlock();
        if (self.unwritten.fetchRemove(segment_id)) |kv| {
            _ = self.unwritten_count.fetchSub(1, .monotonic);
            kv.value.unref(allocator);
        }
        const kv = self.map.fetchRemove(segment_id) orelse return;
        const e = kv.value;
        if (e.pins == 0) destroyEntry(allocator, e) else e.retired = true;
    }

    /// Drop every handle — ALTER/TRUNCATE (schema-dependent parse) and table
    /// close. Unwritten lists stay. Callers hold the table exclusively; no
    /// pins outstanding.
    pub fn clear(self: *SegmentHandles, allocator: Allocator) void {
        self.lockSpin();
        var it = self.map.valueIterator();
        while (it.next()) |e| destroyEntry(allocator, e.*);
        self.map.clearRetainingCapacity();
        self.lock.unlock();
    }

    pub fn deinit(self: *SegmentHandles, allocator: Allocator) void {
        self.clear(allocator);
        if (self.epoch) |e| unpinEpoch(allocator, e);
        self.epoch = null;
        self.discardUnwrittenLocked(allocator);
        self.unwritten.deinit(allocator);
        self.map.deinit(allocator);
    }

    /// The string bytes row group `rg_idx`'s block of string column
    /// `column_idx` decodes to (`ReadSegment.stringBlockBytes`), read on the
    /// first ask and kept on the entry for every later one. Both indexes
    /// must be in the segment's footer. `gpa` must be the table-lifetime
    /// allocator that frees the entry; `scratch` holds only a block read on
    /// a cache miss.
    pub fn rowGroupStringBytes(
        self: *SegmentHandles,
        gpa: Allocator,
        scratch: Allocator,
        entry: *Entry,
        rg_idx: usize,
        column_idx: usize,
        nullable: bool,
        c: TableCache,
    ) !u64 {
        if (self.cachedSample(entry, rg_idx, column_idx).bytes) |bytes| return bytes;
        // Read outside the lock: planners racing on a cold row group both
        // read it and keep the same count.
        const bytes = try entry.seg.stringBlockBytes(scratch, rg_idx, column_idx, nullable, c);
        self.lockSpin();
        defer self.lock.unlock();
        (try sampleSlot(gpa, entry, rg_idx, column_idx)).bytes = bytes;
        return bytes;
    }

    /// The distinct values `ReadSegment.sampleDistinctStrings` samples from
    /// row group `rg_idx`'s block of string column `column_idx`, deduped
    /// within the block; sampled on the first ask and kept on the entry for
    /// every later one. Same index and allocator contract as
    /// `rowGroupStringBytes`; `scratch` also holds the dedupe set.
    pub fn rowGroupDistinctStrings(
        self: *SegmentHandles,
        gpa: Allocator,
        scratch: Allocator,
        entry: *Entry,
        rg_idx: usize,
        column_idx: usize,
        nullable: bool,
        c: TableCache,
    ) !DistinctCount {
        if (self.cachedSample(entry, rg_idx, column_idx).distinct) |count| return count;
        var sampler: column.DistinctWidthSampler = .{};
        defer sampler.deinit(scratch);
        try entry.seg.sampleDistinctStrings(scratch, rg_idx, column_idx, nullable, c, &sampler);
        const count: DistinctCount = .{ .bytes = sampler.bytes, .values = sampler.values };
        self.lockSpin();
        defer self.lock.unlock();
        (try sampleSlot(gpa, entry, rg_idx, column_idx)).distinct = count;
        return count;
    }

    fn cachedSample(self: *SegmentHandles, entry: *Entry, rg_idx: usize, column_idx: usize) StringSample {
        self.lockSpin();
        defer self.lock.unlock();
        if (column_idx >= entry.string_samples.len or entry.string_samples[column_idx].len == 0) return .{};
        return entry.string_samples[column_idx][rg_idx];
    }

    /// The entry's sample slot for one row group's block, allocating the
    /// per-column lists on first use. Caller holds `lock`.
    fn sampleSlot(gpa: Allocator, entry: *Entry, rg_idx: usize, column_idx: usize) !*StringSample {
        const row_groups = entry.seg.info.row_groups;
        if (entry.string_samples.len == 0) {
            const columns = try gpa.alloc([]StringSample, row_groups[rg_idx].col_offsets.len);
            @memset(columns, &.{});
            entry.string_samples = columns;
        }
        if (entry.string_samples[column_idx].len == 0) {
            const per_row_group = try gpa.alloc(StringSample, row_groups.len);
            @memset(per_row_group, .{});
            entry.string_samples[column_idx] = per_row_group;
        }
        return &entry.string_samples[column_idx][rg_idx];
    }

    fn destroyEntry(allocator: Allocator, e: *Entry) void {
        e.seg.deinit();
        if (e.tombs) |t| t.unref(allocator);
        for (e.string_samples) |per_row_group| {
            if (per_row_group.len > 0) allocator.free(per_row_group);
        }
        if (e.string_samples.len > 0) allocator.free(e.string_samples);
        allocator.destroy(e);
    }
};
