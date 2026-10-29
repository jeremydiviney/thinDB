//! Cross-platform huge-page-backed slab pool for the row-group cache's
//! decompressed column blocks. Two wins over allocating each block straight
//! from the general allocator, both measured on the 100M ClickBench scan:
//!
//!   1. Bigger pages. On Linux the pool's memory is advised for transparent
//!      2 MiB huge pages, so a 20 GiB resident cache needs ~512x fewer TLB
//!      entries to map. Once the cache grows to tens of GiB the scan layer was
//!      spending most of its time on address translation (soft page faults),
//!      not decode.
//!   2. No working-set trimming. Committed memory is VirtualLock/mlock'd, so
//!      the OS can't trim cache pages out of the resident set and force a
//!      re-fault on the next scan of that block.
//!
//! Best-effort by design: a refused lock leaves plain committed pages, and a
//! refused reservation or commit sends blocks to the backing allocator — so the
//! pool is always usable. Huge pages are a performance hint, never a
//! correctness gate (mirrors util/affinity.zig).
//!
//! ## Allocation model
//! The pool reserves one address range, its arena, sized a quarter over the
//! cache's capacity, and carves every region from it in huge-page multiples.
//! Blocks (>= MIN_BLOCK) are rounded to a quantized size class (<=25% internal
//! waste; fixed-width column blocks are exact powers of two and land on a class
//! boundary with zero waste). Classes up to MAX_SLAB_CLASS are cells of a slab
//! region that serves that one class: a power of two from 2 to 32 MiB, sized
//! to hold at least CELLS_PER_SLAB cells. A slab tracks its live cells and
//! keeps its freed cells on its own intrusive list; slabs with a free cell sit
//! on their class's partial list. A larger block gets a region of its own,
//! rounded up to a huge page.
//!
//! The footprint is bounded by the arena, not by the high-water mark of every
//! size class the workload touched:
//!   - A slab whose last cell is freed (or a big block's region) returns its
//!     range to the arena's free extents, merged with free neighbours, and any
//!     class's next region is carved best-fit from them. Freed ranges stay
//!     committed, so the evict/insert churn of a scan larger than the cache
//!     reuses resident pages without OS calls or faults.
//!   - Each huge page is committed when a region first covers it and stays
//!     committed until `trim` gives it back: `trim` decommits free extents, so
//!     a cache that shrinks for the shared memory budget returns real memory
//!     (DESIGN §8.3). A request no free extent fits (fragmentation, or pinned
//!     blocks holding the cache over capacity) is served by the backing
//!     allocator and freed back there.
//!   - Evicting in LRU order leaves free cells scattered across a class's
//!     slabs, few of which empty. A compaction pass drains the sparsest slabs
//!     whose blocks the fuller ones have free cells for: the cache moves those
//!     blocks (`relocation`), and the slabs they leave empty go to the free
//!     extents for `trim`.
//! Sub-MIN_BLOCK requests (entry-metadata-sized buffers never reach here, but
//! headers/odd sizes might) fall through to the backing allocator unchanged.
//!
//! ## Concurrency
//! Scan workers decompress and insert blocks concurrently (the cache holds its
//! own mutex only for O(1) bookkeeping, never across decompress), so the pool
//! guards its state with its own spinlock. Lock order is always cache.mutex
//! then pool.lock — the pool never calls back into the cache. Commits run with
//! the lock dropped: a locked commit faults in and zeroes every page up front,
//! and the cache frees evicted blocks under its own mutex, so a commit made
//! under the pool lock would stall every cache lookup behind it.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;

pub const HUGE_PAGE: usize = 2 * 1024 * 1024;
const SLAB_BYTES: usize = 32 * 1024 * 1024;
const MIN_BLOCK: usize = 64 * 1024;
const CELLS_PER_SLAB: usize = 8;
const MAX_SLAB_CLASS: usize = SLAB_BYTES / CELLS_PER_SLAB;
/// Every class is a multiple of 16 KiB and every region base is at least
/// MIN_BLOCK-aligned, so cells honor alignments up to this.
const MAX_CELL_ALIGN: usize = 16 * 1024;
/// Keeps the arena length from overflowing for an effectively unbounded
/// capacity; a reservation this large is refused and the pool falls back.
const MAX_ARENA: usize = 1 << 44;

/// Diagnostic counters for the process's cache footprint: `g_slab_bytes` is
/// what the pools have committed; `g_fallback_bytes` is what they hand out
/// from the backing allocator in blocks of MIN_BLOCK or more (no free extent
/// fits, or at an alignment a cell can't give).
pub var g_slab_bytes = std.atomic.Value(u64).init(0);
pub var g_fallback_bytes = std.atomic.Value(u64).init(0);

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

const Node = struct { next: ?*Node };

const Region = struct {
    base: [*]align(MIN_BLOCK) u8,
    len: usize,
    /// Size of the cells this region is carved into. A big block's region is
    /// one cell of `len`.
    cell: usize,
    cells: usize,
    /// Cells handed out by bumping; the rest of the region is untouched.
    carved: usize = 1,
    live: usize = 1,
    free: ?*Node = null,
    partial_prev: ?*Region = null,
    partial_next: ?*Region = null,
    in_partial: bool = false,
    /// A compaction pass is emptying this slab: it stays off its class's
    /// partial list, so no allocation refills the cells its blocks leave.
    draining: bool = false,

    fn isFull(self: *const Region) bool {
        return self.free == null and self.carved == self.cells;
    }
};

/// A free byte range of the arena.
const Extent = struct { off: usize, len: usize };

/// Round `len` up to a size class with at most 25% internal waste: four
/// sub-classes per power-of-two octave. A power-of-two `len` is returned
/// unchanged, so fixed-width column blocks (65536 rows * {1,2,4,8} bytes) get
/// an exact-fit cell.
fn classSize(len: usize) usize {
    if (len <= MIN_BLOCK) return MIN_BLOCK;
    const octave = 63 - @clz(len); // floor(log2(len))
    const quantum = @as(usize, 1) << @intCast(octave - 2);
    return std.mem.alignForward(usize, len, quantum);
}

/// Power-of-two slab lengths keep the arena's freed ranges few in kind, so a
/// range an emptied slab frees usually fits the next slab whole.
fn slabLen(class: usize) usize {
    return std.math.clamp(std.math.ceilPowerOfTwoAssert(usize, class * CELLS_PER_SLAB), HUGE_PAGE, SLAB_BYTES);
}

/// Resident bytes a block of `len` actually occupies in the pool: `len` for
/// sub-MIN_BLOCK blocks (exact backing allocation), the class cell for a slab
/// block, or the huge-page-rounded region for a big block. The cache uses this
/// so its capacity bounds real footprint.
pub fn cellSize(len: usize) usize {
    if (len < MIN_BLOCK) return len;
    const class = classSize(len);
    if (class <= MAX_SLAB_CLASS) return class;
    return std.mem.alignForward(usize, len, HUGE_PAGE);
}

/// Move `g_fallback_bytes` for a backing-allocator block going from `old_len`
/// to `new_len` bytes (0 = not allocated). Shorter blocks than MIN_BLOCK are
/// the backing allocator's by design, so only longer ones count.
fn trackFallback(old_len: usize, new_len: usize) void {
    if (old_len >= MIN_BLOCK) _ = g_fallback_bytes.fetchSub(old_len, .monotonic);
    if (new_len >= MIN_BLOCK) _ = g_fallback_bytes.fetchAdd(new_len, .monotonic);
}

/// What an allocation needs done after the pool lock is dropped.
const Plan = union(enum) {
    block: [*]u8,
    /// Commit arena offsets `from..to` inside `region`, `newly` bytes of
    /// which were uncommitted, then hand out its first cell.
    commit: struct { region: *Region, from: usize, to: usize, newly: usize },
    /// Serve the block from the backing allocator.
    fallback,
};

pub const Pool = struct {
    backing: Allocator,
    lock: SpinLock = .{},
    /// Empty when the OS refused the reservation; every block then comes from
    /// the backing allocator. Fixed after `init`, so reading it needs no lock.
    arena: []align(MIN_BLOCK) u8 = &.{},
    /// Bytes of the arena counted as committed (a commit may still be in
    /// flight on the thread that carved them). Written under `lock`; read
    /// without it through `committedBytes`.
    committed: usize = 0,
    /// Which huge pages of the arena are committed.
    page_committed: std.DynamicBitSetUnmanaged = .{},
    /// The region holding each huge page of the arena, null while free.
    owners: []?*Region = &.{},
    /// Free ranges of the arena, sorted by offset, no two adjacent. There is
    /// at most one more than the regions, retired ranges and ranges out for
    /// a `trim` between them, and its capacity is kept that high so freeing a
    /// region never allocates.
    free_extents: std.ArrayListUnmanaged(Extent) = .empty,
    /// Regions, plus the free ranges a `trim` has taken out to decommit.
    region_count: usize = 0,
    /// Ranges whose commit failed, out of circulation for good.
    retired_count: usize = 0,
    /// Head of each class's partial list: slabs with at least one free cell.
    /// Few distinct sizes (~36), so a hashmap of list heads is cheap and avoids
    /// size-class index arithmetic. Entries are never removed.
    partial: std.AutoHashMapUnmanaged(usize, ?*Region) = .empty,
    /// Bytes of the arena held by regions. Written under `lock`; read
    /// without it through `carvedBytes`.
    carved_bytes: usize = 0,

    /// `capacity` is the cache's live-byte budget. The arena is a quarter
    /// over it, for partially filled slabs and free ranges too short for the
    /// next region, and holds at least one full slab.
    pub fn init(backing: Allocator, capacity: usize) Pool {
        var self: Pool = .{ .backing = backing };
        const want = @min(@max(capacity +| capacity / 4, SLAB_BYTES), MAX_ARENA);
        const len = std.mem.alignForward(usize, want, HUGE_PAGE);
        const arena = osReserve(len) orelse return self;
        const owners = backing.alloc(?*Region, len / HUGE_PAGE) catch {
            osRelease(arena);
            return self;
        };
        @memset(owners, null);
        var page_committed = std.DynamicBitSetUnmanaged.initEmpty(backing, len / HUGE_PAGE) catch {
            backing.free(owners);
            osRelease(arena);
            return self;
        };
        self.free_extents.append(backing, .{ .off = 0, .len = len }) catch {
            page_committed.deinit(backing);
            backing.free(owners);
            osRelease(arena);
            return self;
        };
        self.arena = arena;
        self.owners = owners;
        self.page_committed = page_committed;
        return self;
    }

    pub fn deinit(self: *Pool) void {
        for (self.owners, 0..) |owner, i| {
            const r = owner orelse continue;
            if (r.base == self.arena.ptr + i * HUGE_PAGE) self.backing.destroy(r);
        }
        if (self.arena.len > 0) {
            _ = g_slab_bytes.fetchSub(self.committed, .monotonic);
            osRelease(self.arena);
        }
        self.backing.free(self.owners);
        self.page_committed.deinit(self.backing);
        self.free_extents.deinit(self.backing);
        self.partial.deinit(self.backing);
        self.* = undefined;
    }

    /// Bytes of the arena committed now; safe without the lock.
    pub fn committedBytes(self: *const Pool) usize {
        return @atomicLoad(usize, &self.committed, .monotonic);
    }

    /// Bytes of the arena held by regions now; safe without the lock. What
    /// they hold beyond their live cells is the pool's overhead.
    pub fn carvedBytes(self: *const Pool) usize {
        return @atomicLoad(usize, &self.carved_bytes, .monotonic);
    }

    /// True when `p` points into the arena (a block the pool's pages hold, not
    /// one the backing allocator served).
    pub fn holds(self: *const Pool, p: [*]const u8) bool {
        return self.inArena(p);
    }

    /// Caller holds `lock`.
    fn addCommitted(self: *Pool, delta: isize) void {
        const now: usize = @intCast(@as(isize, @intCast(self.committed)) + delta);
        @atomicStore(usize, &self.committed, now, .monotonic);
    }

    /// Caller holds `lock`.
    fn addCarved(self: *Pool, delta: isize) void {
        const now: usize = @intCast(@as(isize, @intCast(self.carved_bytes)) + delta);
        @atomicStore(usize, &self.carved_bytes, now, .monotonic);
    }

    pub fn allocator(self: *Pool) Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = Allocator.VTable{
        .alloc = vtAlloc,
        .resize = vtResize,
        .remap = vtRemap,
        .free = vtFree,
    };

    fn vtAlloc(ctx: *anyopaque, len: usize, alignment: Alignment, ra: usize) ?[*]u8 {
        const self: *Pool = @ptrCast(@alignCast(ctx));
        if (len >= MIN_BLOCK and alignment.toByteUnits() <= MAX_CELL_ALIGN) {
            self.lock.lock();
            const plan = self.allocBlock(len);
            self.lock.unlock();
            switch (plan) {
                .block => |p| return p,
                .commit => |c| if (self.finishCommit(c.region, c.from, c.to, c.newly)) |p| return p,
                .fallback => {},
            }
        }
        const p = self.backing.rawAlloc(len, alignment, ra) orelse return null;
        trackFallback(0, len);
        return p;
    }

    fn vtResize(ctx: *anyopaque, mem: []u8, alignment: Alignment, new_len: usize, ra: usize) bool {
        const self: *Pool = @ptrCast(@alignCast(ctx));
        if (!self.inArena(mem.ptr)) {
            if (!self.backing.rawResize(mem, alignment, new_len, ra)) return false;
            trackFallback(mem.len, new_len);
            return true;
        }
        // In place only when the new length still fits the same cell.
        return new_len >= MIN_BLOCK and cellSize(new_len) == cellSize(mem.len);
    }

    fn vtRemap(ctx: *anyopaque, mem: []u8, alignment: Alignment, new_len: usize, ra: usize) ?[*]u8 {
        if (vtResize(ctx, mem, alignment, new_len, ra)) return mem.ptr;
        return null;
    }

    fn vtFree(ctx: *anyopaque, mem: []u8, alignment: Alignment, ra: usize) void {
        const self: *Pool = @ptrCast(@alignCast(ctx));
        if (self.inArena(mem.ptr)) {
            self.lock.lock();
            defer self.lock.unlock();
            self.freeCell(self.regionAt(mem.ptr).?, mem.ptr);
            return;
        }
        self.backing.rawFree(mem, alignment, ra);
        trackFallback(mem.len, 0);
    }

    fn inArena(self: *const Pool, p: [*]const u8) bool {
        const addr = @intFromPtr(p);
        const start = @intFromPtr(self.arena.ptr);
        return addr >= start and addr - start < self.arena.len;
    }

    fn regionAt(self: *Pool, p: [*]u8) ?*Region {
        if (!self.inArena(p)) return null;
        return self.owners[(@intFromPtr(p) - @intFromPtr(self.arena.ptr)) / HUGE_PAGE];
    }

    /// Serves the block from a partial slab, or carves a region for it from
    /// the arena's free extents (leaving the caller to commit any untouched
    /// tail); falls back when no free extent fits.
    fn allocBlock(self: *Pool, len: usize) Plan {
        if (self.arena.len == 0) return .fallback;
        const class = classSize(len);
        var region_len: usize = undefined;
        if (class > MAX_SLAB_CLASS) {
            region_len = std.mem.alignForward(usize, len, HUGE_PAGE);
        } else {
            const head = self.partial.getOrPut(self.backing, class) catch return .fallback;
            if (!head.found_existing) head.value_ptr.* = null;
            if (head.value_ptr.*) |slab| {
                const p = takeCell(slab);
                if (slab.isFull()) self.unlinkPartial(slab);
                return .{ .block = p };
            }
            region_len = slabLen(class);
        }
        self.free_extents.ensureTotalCapacity(self.backing, self.region_count + self.retired_count + 2) catch return .fallback;
        const region = self.backing.create(Region) catch return .fallback;
        const off = self.takeExtent(region_len) orelse {
            self.backing.destroy(region);
            return .fallback;
        };
        const big = class > MAX_SLAB_CLASS;
        region.* = .{
            .base = @alignCast(self.arena.ptr + off),
            .len = region_len,
            .cell = if (big) region_len else class,
            .cells = if (big) 1 else region_len / class,
        };
        @memset(self.owners[off / HUGE_PAGE ..][0 .. region_len / HUGE_PAGE], region);
        self.region_count += 1;
        self.addCarved(@intCast(region_len));
        if (self.markCommitted(off / HUGE_PAGE, (off + region_len) / HUGE_PAGE)) |span| {
            return .{ .commit = .{ .region = region, .from = span.first * HUGE_PAGE, .to = span.end * HUGE_PAGE, .newly = span.newly } };
        }
        if (!region.isFull()) self.pushPartial(region);
        return .{ .block = region.base };
    }

    const PageSpan = struct { first: usize, end: usize, newly: usize };

    /// Marks the uncommitted pages of `first..end` committed and returns the
    /// span from the first to the last of them, which the caller commits
    /// (re-committing a committed page between them is harmless). Null when
    /// every page is already committed. Caller holds `lock`.
    fn markCommitted(self: *Pool, first: usize, end: usize) ?PageSpan {
        var span: ?PageSpan = null;
        var newly: usize = 0;
        for (first..end) |page| {
            if (self.page_committed.isSet(page)) continue;
            self.page_committed.set(page);
            newly += 1;
            if (span) |*sp| sp.end = page + 1 else span = .{ .first = page, .end = page + 1, .newly = 0 };
        }
        self.addCommitted(@intCast(newly * HUGE_PAGE));
        if (span) |*sp| sp.newly = newly * HUGE_PAGE;
        return span;
    }

    /// Unmarks every committed page of `first..end`; returns their bytes.
    /// Caller holds `lock`.
    fn unmarkCommitted(self: *Pool, first: usize, end: usize) usize {
        var cleared: usize = 0;
        for (first..end) |page| {
            if (!self.page_committed.isSet(page)) continue;
            self.page_committed.unset(page);
            cleared += HUGE_PAGE;
        }
        self.addCommitted(-@as(isize, @intCast(cleared)));
        return cleared;
    }

    /// The lock-free half of a `.commit` plan. Until the region is listed as
    /// partial, its first cell is the only way into it, so no other thread
    /// touches the range being committed. Null when the OS refuses the commit.
    fn finishCommit(self: *Pool, region: *Region, from: usize, to: usize, newly: usize) ?[*]u8 {
        const off = @intFromPtr(region.base) - @intFromPtr(self.arena.ptr);
        if (!osCommit(self.arena[from..to])) {
            // The OS is out of memory or commit; keep the range out of
            // circulation rather than retry it on every allocation.
            osDecommit(self.arena[off..][0..region.len]);
            self.lock.lock();
            const was_committed = self.unmarkCommitted(off / HUGE_PAGE, (off + region.len) / HUGE_PAGE);
            @memset(self.owners[off / HUGE_PAGE ..][0 .. region.len / HUGE_PAGE], null);
            self.region_count -= 1;
            self.retired_count += 1;
            self.addCarved(-@as(isize, @intCast(region.len)));
            self.lock.unlock();
            // The pages this plan marked never reached g_slab_bytes.
            _ = g_slab_bytes.fetchSub(was_committed - newly, .monotonic);
            self.backing.destroy(region);
            return null;
        }
        _ = g_slab_bytes.fetchAdd(newly, .monotonic);
        if (!region.isFull()) {
            self.lock.lock();
            self.pushPartial(region);
            self.lock.unlock();
        }
        return region.base;
    }

    fn takeCell(slab: *Region) [*]u8 {
        slab.live += 1;
        if (slab.free) |node| {
            slab.free = node.next;
            return @ptrCast(node);
        }
        const p = slab.base + slab.carved * slab.cell;
        slab.carved += 1;
        return p;
    }

    fn freeCell(self: *Pool, region: *Region, p: [*]u8) void {
        region.live -= 1;
        if (region.live == 0) {
            if (region.in_partial) self.unlinkPartial(region);
            self.releaseRegion(region);
            return;
        }
        const node: *Node = @ptrCast(@alignCast(p));
        node.next = region.free;
        region.free = node;
        if (!region.in_partial and !region.draining) self.pushPartial(region);
    }

    fn releaseRegion(self: *Pool, region: *Region) void {
        const off = @intFromPtr(region.base) - @intFromPtr(self.arena.ptr);
        @memset(self.owners[off / HUGE_PAGE ..][0 .. region.len / HUGE_PAGE], null);
        self.region_count -= 1;
        self.addCarved(-@as(isize, @intCast(region.len)));
        self.giveExtent(off, region.len);
        self.backing.destroy(region);
    }

    fn pushPartial(self: *Pool, slab: *Region) void {
        const head = self.partial.getPtr(slab.cell).?;
        slab.partial_prev = null;
        slab.partial_next = head.*;
        if (head.*) |h| h.partial_prev = slab;
        head.* = slab;
        slab.in_partial = true;
    }

    fn unlinkPartial(self: *Pool, slab: *Region) void {
        if (slab.partial_prev) |prev| {
            prev.partial_next = slab.partial_next;
        } else {
            self.partial.getPtr(slab.cell).?.* = slab.partial_next;
        }
        if (slab.partial_next) |next| next.partial_prev = slab.partial_prev;
        slab.partial_prev = null;
        slab.partial_next = null;
        slab.in_partial = false;
    }

    /// The start of the shortest free extent of at least `len` bytes, the
    /// lowest among equals. The untouched tail is the longest extent until the
    /// arena fills, so committed ranges are reused before it grows.
    fn takeExtent(self: *Pool, len: usize) ?usize {
        var pick: ?usize = null;
        for (self.free_extents.items, 0..) |e, i| {
            if (e.len < len) continue;
            if (pick == null or e.len < self.free_extents.items[pick.?].len) pick = i;
        }
        const i = pick orelse return null;
        const e = &self.free_extents.items[i];
        const off = e.off;
        if (e.len == len) {
            _ = self.free_extents.orderedRemove(i);
        } else {
            e.off += len;
            e.len -= len;
        }
        return off;
    }

    fn giveExtent(self: *Pool, off: usize, len: usize) void {
        const items = self.free_extents.items;
        var i: usize = 0;
        var hi = items.len;
        while (i < hi) {
            const mid = i + (hi - i) / 2;
            if (items[mid].off < off) i = mid + 1 else hi = mid;
        }
        const joins_prev = i > 0 and items[i - 1].off + items[i - 1].len == off;
        const joins_next = i < items.len and off + len == items[i].off;
        if (joins_prev and joins_next) {
            items[i - 1].len += len + items[i].len;
            _ = self.free_extents.orderedRemove(i);
        } else if (joins_prev) {
            items[i - 1].len += len;
        } else if (joins_next) {
            items[i].off = off;
            items[i].len += len;
        } else {
            self.free_extents.insertAssumeCapacity(i, .{ .off = off, .len = len });
        }
    }

    /// Decommit committed pages of the free extents until about `want` bytes
    /// have gone back to the OS (whole huge pages, so it can overshoot by
    /// one); returns the bytes decommitted. Live blocks are untouched. Each
    /// run of pages leaves the free extents while the OS call runs outside
    /// the lock, so allocations meanwhile carve elsewhere or fall back.
    pub fn trim(self: *Pool, want: usize) usize {
        if (self.arena.len == 0) return 0;
        var released: usize = 0;
        while (released < want) {
            self.lock.lock();
            const run = self.takeCommittedRun(want - released);
            self.lock.unlock();
            const r = run orelse break;
            osDecommit(self.arena[r.off..][0..r.len]);
            self.lock.lock();
            _ = self.unmarkCommitted(r.off / HUGE_PAGE, (r.off + r.len) / HUGE_PAGE);
            self.region_count -= 1;
            self.giveExtent(r.off, r.len);
            self.lock.unlock();
            _ = g_slab_bytes.fetchSub(r.len, .monotonic);
            released += r.len;
        }
        return released;
    }

    /// Starts a compaction pass toward freeing about `want` bytes of slabs.
    /// Evicting blocks in LRU order leaves free cells scattered across a
    /// class's slabs, and a slab's pages go back only once it is empty; so in
    /// each class the sparsest slabs whose blocks fit in the free cells of
    /// the fuller ones are drained, and `relocation` gives their blocks cells
    /// elsewhere. False when no slab qualifies; otherwise the caller ends the
    /// pass with `endCompaction`.
    pub fn beginCompaction(self: *Pool, want: usize) bool {
        if (self.arena.len == 0) return false;
        self.lock.lock();
        defer self.lock.unlock();
        var slabs: std.ArrayListUnmanaged(*Region) = .empty;
        defer slabs.deinit(self.backing);
        var drained: usize = 0;
        var heads = self.partial.valueIterator();
        while (heads.next()) |head| {
            if (drained >= want) break;
            slabs.clearRetainingCapacity();
            var cur = head.*;
            while (cur) |slab| : (cur = slab.partial_next) {
                slabs.append(self.backing, slab) catch return drained > 0;
            }
            std.mem.sort(*Region, slabs.items, {}, fullerFirst);
            var moving: usize = 0;
            for (slabs.items) |slab| moving += slab.live;
            var room: usize = 0;
            var kept: usize = 0;
            while (kept < slabs.items.len and moving > room) : (kept += 1) {
                moving -= slabs.items[kept].live;
                room += slabs.items[kept].cells - slabs.items[kept].live;
            }
            var sparsest = slabs.items.len;
            while (sparsest > kept and drained < want) {
                sparsest -= 1;
                const slab = slabs.items[sparsest];
                self.unlinkPartial(slab);
                slab.draining = true;
                drained += slab.len;
            }
        }
        return drained > 0;
    }

    fn fullerFirst(_: void, a: *const Region, b: *const Region) bool {
        if (a.live != b.live) return a.live > b.live;
        return @intFromPtr(a.base) < @intFromPtr(b.base);
    }

    /// During a compaction pass, a free cell in a fuller slab for the block
    /// at `p` when p's slab is draining; null otherwise. The caller copies the
    /// block into it and frees `p`, or frees the cell to leave the block be.
    pub fn relocation(self: *Pool, p: [*]u8) ?[*]u8 {
        if (!self.inArena(p)) return null;
        self.lock.lock();
        defer self.lock.unlock();
        const from = self.regionAt(p).?;
        if (!from.draining) return null;
        const into = self.partial.get(from.cell).? orelse return null;
        const cell = takeCell(into);
        if (into.isFull()) self.unlinkPartial(into);
        return cell;
    }

    /// Ends a compaction pass: a slab that kept blocks (pinned, or still
    /// being filled) goes back on its partial list.
    pub fn endCompaction(self: *Pool) void {
        self.lock.lock();
        defer self.lock.unlock();
        for (self.owners, 0..) |owner, page| {
            const region = owner orelse continue;
            if (region.base != self.arena.ptr + page * HUGE_PAGE or !region.draining) continue;
            region.draining = false;
            self.pushPartial(region);
        }
    }

    /// Takes the first run of committed pages, at most `want` rounded up to a
    /// huge page, out of the free extents, counted like a region until
    /// `trim` gives it back. Null when no free extent holds a committed page
    /// or the extent list can't grow for the split. Caller holds `lock`.
    fn takeCommittedRun(self: *Pool, want: usize) ?Extent {
        const max_pages = want / HUGE_PAGE + @intFromBool(want % HUGE_PAGE != 0);
        for (self.free_extents.items, 0..) |e, i| {
            const first_page = e.off / HUGE_PAGE;
            const end_page = (e.off + e.len) / HUGE_PAGE;
            var start = first_page;
            while (start < end_page and !self.page_committed.isSet(start)) start += 1;
            if (start == end_page) continue;
            var end = start;
            while (end < end_page and end - start < max_pages and self.page_committed.isSet(end)) end += 1;
            self.free_extents.ensureTotalCapacity(self.backing, self.region_count + self.retired_count + 3) catch return null;
            const run: Extent = .{ .off = start * HUGE_PAGE, .len = (end - start) * HUGE_PAGE };
            self.cutExtent(i, run);
            self.region_count += 1;
            return run;
        }
        return null;
    }

    /// Removes `run`, which lies inside free extent `i`, from the free list.
    /// Caller holds `lock` and has ensured room for one more extent.
    fn cutExtent(self: *Pool, i: usize, run: Extent) void {
        const e = &self.free_extents.items[i];
        const e_end = e.off + e.len;
        const run_end = run.off + run.len;
        if (run.off == e.off and run_end == e_end) {
            _ = self.free_extents.orderedRemove(i);
        } else if (run.off == e.off) {
            e.off = run_end;
            e.len = e_end - run_end;
        } else if (run_end == e_end) {
            e.len = run.off - e.off;
        } else {
            e.len = run.off - e.off;
            self.free_extents.insertAssumeCapacity(i + 1, .{ .off = run_end, .len = e_end - run_end });
        }
    }
};

// ---- OS arena ----------------------------------------------------------------

/// Reserve `len` (a multiple of HUGE_PAGE) bytes of address space without
/// committing memory to it. Null on any failure, so the pool falls back to
/// the backing allocator.
fn osReserve(len: usize) ?[]align(MIN_BLOCK) u8 {
    return switch (builtin.os.tag) {
        .windows => reserveWindows(len),
        .linux, .macos => reservePosix(len),
        else => null,
    };
}

/// Commit `range` of the arena and lock it into the resident set where the
/// OS allows it.
fn osCommit(range: []u8) bool {
    return switch (builtin.os.tag) {
        .windows => commitWindows(range),
        .linux => commitLinux(range),
        // The reservation is already readable and writable.
        else => true,
    };
}

/// Give `range` of the arena back to the OS, leaving it reserved as before
/// its first commit, so `osCommit` can bring it back. Best-effort.
fn osDecommit(range: []u8) void {
    switch (builtin.os.tag) {
        .windows => {
            _ = VirtualUnlock(range.ptr, range.len);
            _ = VirtualFree(range.ptr, range.len, MEM_DECOMMIT);
        },
        .linux => {
            const linux = std.os.linux;
            _ = linux.munlock(range.ptr, range.len);
            _ = linux.madvise(range.ptr, range.len, linux.MADV.DONTNEED);
            _ = linux.mprotect(range.ptr, range.len, .{});
        },
        .macos => std.posix.madvise(@alignCast(range.ptr), range.len, std.posix.MADV.DONTNEED) catch {},
        else => {},
    }
}

fn osRelease(arena: []align(MIN_BLOCK) u8) void {
    switch (builtin.os.tag) {
        .windows => {
            _ = VirtualFree(arena.ptr, 0, MEM_RELEASE);
        },
        .linux, .macos => {
            std.posix.munmap(@alignCast(arena));
        },
        else => {},
    }
}

// ---- Windows ---------------------------------------------------------------

const win = std.os.windows;

const MEM_COMMIT: u32 = 0x1000;
const MEM_RESERVE: u32 = 0x2000;
const MEM_DECOMMIT: u32 = 0x4000;
const MEM_RELEASE: u32 = 0x8000;
const PAGE_READWRITE: u32 = 0x04;

extern "kernel32" fn VirtualAlloc(addr: ?*anyopaque, size: usize, alloc_type: u32, protect: u32) callconv(.winapi) ?*anyopaque;
extern "kernel32" fn VirtualFree(addr: *anyopaque, size: usize, free_type: u32) callconv(.winapi) win.BOOL;
extern "kernel32" fn VirtualLock(addr: *anyopaque, size: usize) callconv(.winapi) win.BOOL;
extern "kernel32" fn VirtualUnlock(addr: *anyopaque, size: usize) callconv(.winapi) win.BOOL;
extern "kernel32" fn SetProcessWorkingSetSizeEx(process: win.HANDLE, min: usize, max: usize, flags: u32) callconv(.winapi) win.BOOL;
extern "kernel32" fn GetCurrentProcess() callconv(.winapi) win.HANDLE;

var win_ws_raised = std.atomic.Value(bool).init(false);

/// Windows reservations are 64 KiB-aligned (MIN_BLOCK). Large pages can only
/// be committed together with their reservation, so the arena uses normal
/// pages there.
fn reserveWindows(len: usize) ?[]align(MIN_BLOCK) u8 {
    const p = VirtualAlloc(null, len, MEM_RESERVE, PAGE_READWRITE) orelse return null;
    const base: [*]align(MIN_BLOCK) u8 = @ptrCast(@alignCast(p));
    return base[0..len];
}

fn commitWindows(range: []u8) bool {
    if (VirtualAlloc(range.ptr, range.len, MEM_COMMIT, PAGE_READWRITE) == null) return false;
    // VirtualLock is bounded by the process working-set maximum, so raise that
    // ceiling first or a multi-GiB cache can't be pinned.
    raiseWorkingSetQuota();
    _ = VirtualLock(range.ptr, range.len); // best-effort
    return true;
}

/// Raise this process's working-set maximum once so VirtualLock has quota to
/// pin the cache slabs. Soft limits (flags=0): we lift the ceiling for locking
/// without imposing a hard minimum that would starve the rest of the machine.
fn raiseWorkingSetQuota() void {
    if (win_ws_raised.swap(true, .monotonic)) return;
    const total = std.process.totalSystemMemory() catch return;
    const max_ws: usize = @intCast(total * 90 / 100);
    const min_ws: usize = @min(max_ws, @as(usize, 256) * 1024 * 1024);
    _ = SetProcessWorkingSetSizeEx(GetCurrentProcess(), min_ws, max_ws, 0);
}

// ---- Linux / macOS ---------------------------------------------------------

/// On Linux the reservation is PROT_NONE, so it counts against no commit
/// limit until `commitLinux` opens a range. macOS doesn't account commit, so
/// its reservation is writable from the start.
fn reservePosix(len: usize) ?[]align(MIN_BLOCK) u8 {
    // Over-map by one huge page so the arena can be aligned to a 2 MiB
    // boundary — transparent huge pages only collapse aligned, huge-sized spans.
    const over = len + HUGE_PAGE;
    const prot: std.posix.PROT = if (builtin.os.tag == .linux) .{} else .{ .READ = true, .WRITE = true };
    const raw = std.posix.mmap(null, over, prot, .{ .TYPE = .PRIVATE, .ANONYMOUS = true }, -1, 0) catch return null;
    const raw_addr = @intFromPtr(raw.ptr);
    const aligned = std.mem.alignForward(usize, raw_addr, HUGE_PAGE);
    const head = aligned - raw_addr;
    if (head != 0) std.posix.munmap(@alignCast(raw[0..head]));
    const tail_start = head + len;
    if (over > tail_start) std.posix.munmap(@alignCast(raw[tail_start..over]));
    const arena: [*]align(HUGE_PAGE) u8 = @ptrFromInt(aligned);
    if (builtin.os.tag == .linux) {
        std.posix.madvise(arena, len, std.os.linux.MADV.HUGEPAGE) catch {};
    }
    return arena[0..len];
}

/// mlock also faults the range in, so the scan never pays for it later.
fn commitLinux(range: []u8) bool {
    const linux = std.os.linux;
    if (linux.errno(linux.mprotect(range.ptr, range.len, .{ .READ = true, .WRITE = true })) != .SUCCESS) return false;
    _ = linux.mlock(range.ptr, range.len); // best-effort
    return true;
}

// ---- tests -----------------------------------------------------------------

test "classSize keeps fixed-width column blocks exact and bounds waste" {
    try std.testing.expectEqual(@as(usize, 64 * 1024), classSize(64 * 1024)); // u8 col
    try std.testing.expectEqual(@as(usize, 256 * 1024), classSize(256 * 1024)); // u32 col
    try std.testing.expectEqual(@as(usize, 512 * 1024), classSize(512 * 1024)); // u64 col
    // <=25% internal waste mid-octave.
    const s = classSize(300 * 1024);
    try std.testing.expect(s >= 300 * 1024);
    try std.testing.expect(s <= 300 * 1024 * 5 / 4);
}

test "pool round-trips blocks and recycles freed cells" {
    var pool = Pool.init(std.testing.allocator, 64 << 20);
    defer pool.deinit();
    const a = pool.allocator();

    const b1 = try a.alignedAlloc(u8, .@"16", 256 * 1024);
    @memset(b1, 0xAB);
    const p1 = b1.ptr;
    a.free(b1);
    // Same-class request should reuse the just-freed cell.
    const b2 = try a.alignedAlloc(u8, .@"16", 256 * 1024);
    try std.testing.expectEqual(p1, b2.ptr);
    a.free(b2);

    // A sub-MIN_BLOCK request bypasses the pool via the backing allocator.
    const small = try a.alloc(u8, 128);
    a.free(small);
}

test "a freed cell is reused while its slab still holds others" {
    var pool = Pool.init(std.testing.allocator, 64 << 20);
    defer pool.deinit();
    const a = pool.allocator();

    const b1 = try a.alignedAlloc(u8, .@"16", 64 * 1024);
    const b2 = try a.alignedAlloc(u8, .@"16", 64 * 1024);
    defer a.free(b2);
    const p1 = b1.ptr;
    a.free(b1);
    const b3 = try a.alignedAlloc(u8, .@"16", 64 * 1024);
    defer a.free(b3);
    try std.testing.expectEqual(p1, b3.ptr);
    try std.testing.expectEqual(HUGE_PAGE, pool.committed);
}

test "an emptied slab's range serves another class" {
    var pool = Pool.init(std.testing.allocator, 64 << 20);
    defer pool.deinit();
    const a = pool.allocator();

    const b1 = try a.alignedAlloc(u8, .@"16", 64 * 1024);
    const p1 = b1.ptr;
    a.free(b1);
    try std.testing.expectEqual(@as(usize, 0), pool.carved_bytes);
    // A 192 KiB class also lives in 2 MiB slabs, so it takes the same range.
    const b2 = try a.alignedAlloc(u8, .@"16", 192 * 1024);
    defer a.free(b2);
    try std.testing.expectEqual(p1, b2.ptr);
    try std.testing.expectEqual(HUGE_PAGE, pool.committed);
}

test "a freed big block's range splits for shorter regions without committing more" {
    var pool = Pool.init(std.testing.allocator, 64 << 20);
    defer pool.deinit();
    const a = pool.allocator();

    const b1 = try a.alignedAlloc(u8, .@"16", 7 << 20);
    const p1 = b1.ptr;
    a.free(b1);
    try std.testing.expectEqual(@as(usize, 8 << 20), pool.committed);
    // A 5 MiB block takes 6 MiB of the freed 8, a 64 KiB slab the other 2.
    const b2 = try a.alignedAlloc(u8, .@"16", 5 << 20);
    defer a.free(b2);
    const b3 = try a.alignedAlloc(u8, .@"16", 64 * 1024);
    defer a.free(b3);
    try std.testing.expectEqual(p1, b2.ptr);
    try std.testing.expectEqual(p1 + (6 << 20), b3.ptr);
    try std.testing.expectEqual(@as(usize, 8 << 20), pool.committed);
}

test "neighbouring freed regions merge to serve a longer one" {
    var pool = Pool.init(std.testing.allocator, 64 << 20);
    defer pool.deinit();
    const a = pool.allocator();

    const b1 = try a.alignedAlloc(u8, .@"16", 5 << 20);
    const b2 = try a.alignedAlloc(u8, .@"16", 5 << 20);
    const b3 = try a.alignedAlloc(u8, .@"16", 5 << 20);
    defer a.free(b3);
    const p1 = b1.ptr;
    a.free(b2);
    a.free(b1);
    // The two freed 6 MiB regions merge into one 12 MiB range.
    const wide = try a.alignedAlloc(u8, .@"16", 11 << 20);
    defer a.free(wide);
    try std.testing.expectEqual(p1, wide.ptr);
    try std.testing.expectEqual(@as(usize, 18 << 20), pool.committed);
    try std.testing.expectEqual(@as(usize, 1), pool.free_extents.items.len);
}

test "the arena bounds committed memory and the backing allocator serves the rest" {
    var pool = Pool.init(std.testing.allocator, 0);
    defer pool.deinit();
    const a = pool.allocator();
    try std.testing.expectEqual(@as(usize, 32 << 20), pool.arena.len);

    const fallback_before = g_fallback_bytes.load(.monotonic);
    var blocks: [6][]align(16) u8 = undefined;
    for (&blocks, 0..) |*b, i| {
        b.* = try a.alignedAlloc(u8, .@"16", 5 << 20);
        @memset(b.*, @intCast(i));
    }
    try std.testing.expectEqual(@as(usize, 30 << 20), pool.committed);
    try std.testing.expect(pool.regionAt(blocks[5].ptr) == null);
    try std.testing.expectEqual(fallback_before + (5 << 20), g_fallback_bytes.load(.monotonic));
    for (blocks, 0..) |b, i| try std.testing.expectEqual(@as(u8, @intCast(i)), b[b.len - 1]);
    for (blocks) |b| a.free(b);
    try std.testing.expectEqual(fallback_before, g_fallback_bytes.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), pool.free_extents.items.len);
}

test "threads churning blocks past the arena keep their bytes and stay inside it" {
    var pool = Pool.init(std.testing.allocator, 48 << 20);
    defer pool.deinit();

    const Churn = struct {
        fn run(p: *Pool, seed: u64, failed: *std.atomic.Value(bool)) void {
            const a = p.allocator();
            var prng = std.Random.DefaultPrng.init(seed);
            const rand = prng.random();
            var held: [6]?[]align(16) u8 = @splat(null);
            for (0..400) |step| {
                const slot = rand.uintLessThan(usize, held.len);
                if (held[slot]) |b| {
                    const stamp = std.mem.readInt(u64, b[0..8], .little);
                    if (std.mem.readInt(u64, b[b.len - 8 ..][0..8], .little) != stamp) failed.store(true, .monotonic);
                    a.free(b);
                    held[slot] = null;
                }
                // Mixed slab classes and big blocks, so emptied slabs free
                // ranges that other classes and big blocks reuse.
                const len = if (step % 3 == 0)
                    rand.intRangeAtMost(usize, 5 << 20, 9 << 20)
                else
                    rand.intRangeAtMost(usize, MIN_BLOCK, 1 << 20);
                const b = a.alignedAlloc(u8, .@"16", len) catch {
                    failed.store(true, .monotonic);
                    return;
                };
                // A block overlapping another would have one of its stamps
                // overwritten by a different (thread, step) pair.
                const stamp = seed << 32 | step;
                std.mem.writeInt(u64, b[0..8], stamp, .little);
                std.mem.writeInt(u64, b[b.len - 8 ..][0..8], stamp, .little);
                held[slot] = b;
            }
            for (held) |h| if (h) |b| a.free(b);
        }
    };
    var failed = std.atomic.Value(bool).init(false);
    var threads: [4]std.Thread = undefined;
    for (&threads, 0..) |*t, i| t.* = try std.Thread.spawn(.{}, Churn.run, .{ &pool, i + 1, &failed });
    for (threads) |t| t.join();
    try std.testing.expect(!failed.load(.monotonic));
    try std.testing.expect(pool.committed <= pool.arena.len);
    try std.testing.expectEqual(@as(usize, 0), pool.carved_bytes);
    try std.testing.expectEqual(@as(usize, 1), pool.free_extents.items.len);
}

test "trim decommits free pages only, and a later region commits them again" {
    var pool = Pool.init(std.testing.allocator, 64 << 20);
    defer pool.deinit();
    const a = pool.allocator();
    const slab_before = g_slab_bytes.load(.monotonic);

    const big = try a.alignedAlloc(u8, .@"16", 7 << 20);
    const keep = try a.alignedAlloc(u8, .@"16", 64 * 1024);
    defer a.free(keep);
    @memset(keep, 0xC3);
    const big_ptr = big.ptr;
    a.free(big);
    try std.testing.expectEqual(@as(usize, 10 << 20), pool.committed);

    // Whole huge pages: 3 MiB asked takes two pages.
    try std.testing.expectEqual(@as(usize, 4 << 20), pool.trim(3 << 20));
    try std.testing.expectEqual(@as(usize, 6 << 20), pool.committed);
    try std.testing.expectEqual(@as(usize, 4 << 20), pool.trim(64 << 20));
    try std.testing.expectEqual(@as(usize, 2 << 20), pool.committed);
    try std.testing.expectEqual(@as(usize, 0), pool.trim(1));
    try std.testing.expectEqual(slab_before + (2 << 20), g_slab_bytes.load(.monotonic));
    for (keep) |byte| try std.testing.expectEqual(@as(u8, 0xC3), byte);

    const again = try a.alignedAlloc(u8, .@"16", 7 << 20);
    defer a.free(again);
    @memset(again, 0x5A);
    try std.testing.expectEqual(big_ptr, again.ptr);
    try std.testing.expectEqual(@as(usize, 10 << 20), pool.committed);
    try std.testing.expectEqual(slab_before + (10 << 20), g_slab_bytes.load(.monotonic));
}

test "trim racing threads that churn blocks never touches a live block" {
    var pool = Pool.init(std.testing.allocator, 48 << 20);
    defer pool.deinit();

    const Churn = struct {
        fn run(p: *Pool, seed: u64, failed: *std.atomic.Value(bool)) void {
            const a = p.allocator();
            var prng = std.Random.DefaultPrng.init(seed);
            const rand = prng.random();
            var held: [6]?[]align(16) u8 = @splat(null);
            for (0..400) |step| {
                const slot = rand.uintLessThan(usize, held.len);
                if (held[slot]) |b| {
                    const stamp = std.mem.readInt(u64, b[0..8], .little);
                    if (std.mem.readInt(u64, b[b.len - 8 ..][0..8], .little) != stamp) failed.store(true, .monotonic);
                    a.free(b);
                    held[slot] = null;
                }
                const len = if (step % 3 == 0)
                    rand.intRangeAtMost(usize, 5 << 20, 9 << 20)
                else
                    rand.intRangeAtMost(usize, MIN_BLOCK, 1 << 20);
                const b = a.alignedAlloc(u8, .@"16", len) catch {
                    failed.store(true, .monotonic);
                    return;
                };
                const stamp = seed << 32 | step;
                std.mem.writeInt(u64, b[0..8], stamp, .little);
                std.mem.writeInt(u64, b[b.len - 8 ..][0..8], stamp, .little);
                held[slot] = b;
            }
            for (held) |h| if (h) |b| a.free(b);
        }
        fn trimmer(p: *Pool, stop: *std.atomic.Value(bool)) void {
            while (!stop.load(.monotonic)) _ = p.trim(8 << 20);
        }
    };
    var failed = std.atomic.Value(bool).init(false);
    var stop = std.atomic.Value(bool).init(false);
    const trimmer = try std.Thread.spawn(.{}, Churn.trimmer, .{ &pool, &stop });
    var threads: [3]std.Thread = undefined;
    for (&threads, 0..) |*t, i| t.* = try std.Thread.spawn(.{}, Churn.run, .{ &pool, i + 1, &failed });
    for (threads) |t| t.join();
    stop.store(true, .monotonic);
    trimmer.join();
    try std.testing.expect(!failed.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), pool.carved_bytes);
    try std.testing.expectEqual(@as(usize, 1), pool.free_extents.items.len);
    _ = pool.trim(std.math.maxInt(usize));
    try std.testing.expectEqual(@as(usize, 0), pool.committed);
}

test "compaction drains a sparse slab into a fuller one's free cells, only when they fit" {
    var pool = Pool.init(std.testing.allocator, 64 << 20);
    defer pool.deinit();
    if (pool.arena.len == 0) return error.SkipZigTest;
    const a = pool.allocator();
    const mib: usize = 1 << 20;

    // Two slabs of eight 1 MiB cells.
    var blocks: [16][]align(16) u8 = undefined;
    for (&blocks, 0..) |*b, i| {
        b.* = try a.alignedAlloc(u8, .@"16", mib);
        @memset(b.*, @intCast(i));
    }
    // Six and five blocks left: the first slab's two free cells can't take
    // the second's five.
    for ([_]usize{ 0, 1, 8, 9, 10 }) |i| a.free(blocks[i]);
    try std.testing.expect(!pool.beginCompaction(1));
    for ([_]usize{ 11, 12, 13 }) |i| a.free(blocks[i]);
    try std.testing.expectEqual(16 * mib, pool.carvedBytes());

    try std.testing.expect(pool.beginCompaction(1));
    try std.testing.expectEqual(@as(?[*]u8, null), pool.relocation(blocks[2].ptr));
    for ([_]usize{ 14, 15 }) |i| {
        const moved: []align(16) u8 = @alignCast(pool.relocation(blocks[i].ptr).?[0..mib]);
        @memcpy(moved, blocks[i]);
        a.free(blocks[i]);
        blocks[i] = moved;
    }
    pool.endCompaction();
    try std.testing.expectEqual(8 * mib, pool.carvedBytes());
    try std.testing.expectEqual(8 * mib, pool.trim(std.math.maxInt(usize)));
    for ([_]usize{ 2, 3, 4, 5, 6, 7, 14, 15 }) |i| {
        for (blocks[i]) |byte| try std.testing.expectEqual(@as(u8, @intCast(i)), byte);
        a.free(blocks[i]);
    }
    try std.testing.expectEqual(@as(usize, 0), pool.carvedBytes());
}

test "resize stays in place only within the block's cell" {
    var pool = Pool.init(std.testing.allocator, 64 << 20);
    defer pool.deinit();
    const a = pool.allocator();

    // A 100 KiB block sits in a 112 KiB cell.
    var b = try a.alignedAlloc(u8, .@"16", 100 * 1024);
    try std.testing.expect(a.resize(b, 110 * 1024));
    b.len = 110 * 1024;
    try std.testing.expect(!a.resize(b, 120 * 1024));
    try std.testing.expect(!a.resize(b, 1024));
    a.free(b);
}

test "pool serves many concurrent-sized blocks without overlap" {
    var pool = Pool.init(std.testing.allocator, 64 << 20);
    defer pool.deinit();
    const a = pool.allocator();

    var blocks: [32][]u8 = undefined;
    for (&blocks, 0..) |*b, i| {
        b.* = try a.alignedAlloc(u8, .@"16", 64 * 1024 + i * 4096);
        @memset(b.*, @intCast(i));
    }
    // Each block keeps its own bytes — no aliasing across cells.
    for (blocks, 0..) |b, i| {
        for (b) |byte| try std.testing.expectEqual(@as(u8, @intCast(i)), byte);
    }
    for (blocks) |b| a.free(b);
}
