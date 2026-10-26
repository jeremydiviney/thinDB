//! Process-global retaining buffer pool — a size-classed free-list `Allocator`
//! that recycles the large, short-lived scratch buffers a parallel scan churns
//! through (per-worker column decode buffers, aggregate arenas, survivor copies).
//!
//! Why: those buffers are multi-MB, so the backing general allocator services
//! them with `mmap`/`VirtualAlloc` on alloc and `munmap`/`VirtualFree` on free —
//! and the free is the expensive half (page-table teardown + TLB shootdowns),
//! ~90-100ms of a wide single-scan teardown. Backgrounding that free only
//! *shuffles* the cost onto a contending thread (measured: a wash on pipelined
//! and back-to-back workloads). Retaining the block instead *eliminates* it: a
//! free pushes the block onto a free-list (no `munmap`), and the next scan's
//! alloc pops it (no `mmap`) — a double win on the common back-to-back path.
//!
//! Scope: only allocations in `[min_class, max_large_class]` with alignment ≤
//! `max_align` are pooled. Smaller allocations pass straight through (the
//! backing allocator already pools those cheaply). Up to 64 MiB the classes
//! are powers of two; past it they step by a quarter of their power of two, so
//! a statement's one-off multi-hundred-MiB result array is recycled too (each
//! fresh mapping of one faults in every page it touches) without reserving
//! nearly twice its size. The free-list decision uses the same `(len, alignment)`
//! predicate on alloc and free, so a block is always classified identically both
//! ways. Retained bytes are capped. A free past the cap first evicts idle
//! blocks of classes allocated from less recently than its own, so the retained
//! set follows the current workload: a pool left full of one query's classes
//! would otherwise mint and evict every block of the next query that uses
//! different sizes. Only when no staler class can make room does the freed
//! block itself go back. Idle blocks also go back on request (`reclaim`): the
//! shared memory budget asks when live queries need the room.
//!
//! Thread-safety: workers allocate concurrently, so each size class carries its
//! own spinlock (distinct sizes rarely contend). The pool is a process
//! singleton, mirroring util/core_scheduler.zig and the huge-page slab pool — a
//! machine resource shared by every query. Bypassed requests go to
//! `c_allocator`; class blocks are minted from `mint_allocator`.

const std = @import("std");
const builtin = @import("builtin");
const affinity = @import("affinity.zig");
const Allocator = std.mem.Allocator;
const getenv = @extern(*const fn (name: [*:0]const u8) callconv(.c) ?[*:0]const u8, .{ .name = "getenv", .library_name = "c" });

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

/// Power-of-two classes span `[1<<min_log2, 1<<max_log2]` bytes. 64 KiB covers
/// the mmap-threshold boundary (glibc/CRT route allocations past ~128 KiB to
/// the kernel; 64 KiB is a safe floor). Quarter-step classes continue from
/// there to `1<<max_large_log2`.
const min_log2: u6 = 16; // 64 KiB
const max_log2: u6 = 26; // 64 MiB
const max_large_log2: u6 = 31; // 2 GiB
const pow2_class_count: usize = max_log2 - min_log2 + 1;
const class_count: usize = pow2_class_count + (max_large_log2 - max_log2) * 4;

/// Blocks are over-aligned to a cache line so a pooled block satisfies any
/// request with alignment ≤ this; requests above it bypass the pool.
const max_align_log2: u6 = 6; // 64 bytes

const FreeNode = struct {
    next: ?*FreeNode,
};

const Class = struct {
    lock: SpinLock = .{},
    head: ?*FreeNode = null,
    count: usize = 0,
    /// Pool clock reading at this class's latest allocation; orders the
    /// classes for eviction when a free finds the pool at its cap.
    last_use: std.atomic.Value(u64) = .{ .raw = 0 },
};

const IdleChain = struct {
    head: ?*FreeNode,
    taken: usize,
};

pub const Pool = struct {
    backing: Allocator,
    /// Source of class-sized blocks; `backing` serves the requests that
    /// bypass the pool.
    mint: Allocator,
    classes: [class_count]Class = [_]Class{.{}} ** class_count,
    retained_bytes: std.atomic.Value(usize) = .{ .raw = 0 },
    cap_bytes: usize,
    clock: std.atomic.Value(u64) = .{ .raw = 0 },
    hits: std.atomic.Value(usize) = .{ .raw = 0 },
    misses: std.atomic.Value(usize) = .{ .raw = 0 },
    minted_bytes: std.atomic.Value(usize) = .{ .raw = 0 },
    evictions: std.atomic.Value(usize) = .{ .raw = 0 },
    evicted_bytes: std.atomic.Value(usize) = .{ .raw = 0 },

    pub fn init(backing: Allocator, cap_bytes: usize) Pool {
        return .{ .backing = backing, .mint = backing, .cap_bytes = cap_bytes };
    }

    /// Drain every retained block back to the mint allocator. For shutdown
    /// and tests; never on the query path.
    pub fn drain(self: *Pool) void {
        for (0..class_count) |idx| {
            const chain = self.takeIdle(idx, std.math.maxInt(usize));
            self.releaseChain(chain, classSize(idx));
        }
    }

    /// Unlink up to `limit` idle blocks of class `idx`; the caller releases
    /// them with `releaseChain` outside the class lock.
    fn takeIdle(self: *Pool, idx: usize, limit: usize) IdleChain {
        const cls = &self.classes[idx];
        cls.lock.lock();
        const chain = cls.head;
        var taken: usize = 0;
        var last: ?*FreeNode = null;
        var node = chain;
        while (node) |n| {
            if (taken == limit) break;
            last = n;
            node = n.next;
            taken += 1;
        }
        cls.head = node;
        cls.count -= taken;
        cls.lock.unlock();
        if (last) |l| l.next = null;
        _ = self.retained_bytes.fetchSub(taken * classSize(idx), .monotonic);
        return .{ .head = if (taken == 0) null else chain, .taken = taken };
    }

    fn releaseChain(self: *Pool, chain: IdleChain, sz: usize) void {
        var freeing = chain.head;
        var left = chain.taken;
        while (left > 0) : (left -= 1) {
            const n = freeing.?;
            freeing = n.next;
            const buf: [*]u8 = @ptrCast(n);
            self.mint.rawFree(buf[0..sz], .fromByteUnits(@as(usize, 1) << max_align_log2), @returnAddress());
        }
    }

    /// Hand idle blocks back to the mint allocator until `want` bytes are
    /// gone or the free lists are empty; returns the bytes released. Largest
    /// classes go first: they are the blocks a general allocator maps one by
    /// one, so freeing them returns memory to the OS, where a small block
    /// would only move to the allocator's own free list. The frees run
    /// outside the class locks, so workers keep allocating meanwhile.
    pub fn reclaim(self: *Pool, want: usize) usize {
        var released: usize = 0;
        var idx: usize = class_count;
        while (idx > 0 and released < want) {
            idx -= 1;
            released += self.evictIdle(idx, want - released);
        }
        return released;
    }

    fn evictIdle(self: *Pool, idx: usize, want: usize) usize {
        const sz = classSize(idx);
        const chain = self.takeIdle(idx, (want +| (sz - 1)) / sz);
        _ = self.evictions.fetchAdd(chain.taken, .monotonic);
        _ = self.evicted_bytes.fetchAdd(chain.taken * sz, .monotonic);
        self.releaseChain(chain, sz);
        return chain.taken * sz;
    }

    /// Frees `want` idle bytes from classes allocated from less recently than
    /// class `keep`, least recent first, so a pool at its cap follows the
    /// classes the current workload uses. False when those classes hold too
    /// little; the caller then evicts the block it was freeing.
    fn evictStaleClasses(self: *Pool, keep: usize, want: usize) bool {
        const keep_use = self.classes[keep].last_use.load(.monotonic);
        var tried = std.StaticBitSet(class_count).initEmpty();
        var released: usize = 0;
        while (released < want) {
            var victim: ?usize = null;
            var oldest: u64 = keep_use;
            for (&self.classes, 0..) |*cls, idx| {
                if (idx == keep or tried.isSet(idx)) continue;
                const use = cls.last_use.load(.monotonic);
                if (use < oldest) {
                    oldest = use;
                    victim = idx;
                }
            }
            const idx = victim orelse return false;
            tried.set(idx);
            released += self.evictIdle(idx, want - released);
        }
        return true;
    }

    pub fn allocator(self: *Pool) Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    /// Profiling: cumulative traffic counters plus the retained blocks per
    /// class (a class that never drains marks a client whose blocks nobody
    /// else can reuse).
    pub fn dumpStats(self: *Pool) void {
        const mib = 1024 * 1024;
        std.debug.print("[hprof] bufpool retained={d} MiB cap={d} MiB hits={d} misses={d} minted={d} MiB evictions={d} evicted={d} MiB  classes:", .{
            self.retained_bytes.load(.monotonic) / mib,
            self.cap_bytes / mib,
            self.hits.load(.monotonic),
            self.misses.load(.monotonic),
            self.minted_bytes.load(.monotonic) / mib,
            self.evictions.load(.monotonic),
            self.evicted_bytes.load(.monotonic) / mib,
        });
        for (&self.classes, 0..) |*cls, i| {
            cls.lock.lock();
            const n = cls.count;
            cls.lock.unlock();
            if (n > 0) std.debug.print(" {d}K×{d}", .{ classSize(i) / 1024, n });
        }
        std.debug.print("\n", .{});
    }

    const vtable: Allocator.VTable = .{
        .alloc = allocImpl,
        .resize = resizeImpl,
        .remap = remapImpl,
        .free = freeImpl,
    };

    /// Size class for a (len, alignment) request, or null if it bypasses the
    /// pool. Identical on alloc and free so a block is classified the same way
    /// both directions.
    fn classOf(len: usize, alignment: std.mem.Alignment) ?usize {
        if (len == 0) return null;
        if (@intFromEnum(alignment) > max_align_log2) return null;
        const need_log2 = std.math.log2_int_ceil(usize, len);
        if (need_log2 < min_log2) return null;
        if (need_log2 <= max_log2) return need_log2 - min_log2;
        if (need_log2 > max_large_log2) return null;
        const base_log2 = need_log2 - 1;
        const quarter = @as(usize, 1) << @intCast(base_log2 - 2);
        const steps = (len - (@as(usize, 1) << @intCast(base_log2)) + quarter - 1) / quarter;
        return pow2_class_count + (base_log2 - max_log2) * 4 + steps - 1;
    }

    fn classSize(idx: usize) usize {
        if (idx < pow2_class_count) return @as(usize, 1) << @intCast(min_log2 + idx);
        const large = idx - pow2_class_count;
        const base_log2 = max_log2 + large / 4;
        return (@as(usize, 1) << @intCast(base_log2)) + (large % 4 + 1) * (@as(usize, 1) << @intCast(base_log2 - 2));
    }

    fn allocImpl(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Pool = @ptrCast(@alignCast(ctx));
        const idx = classOf(len, alignment) orelse
            return self.backing.rawAlloc(len, alignment, ret_addr);
        const cls = &self.classes[idx];
        cls.last_use.store(self.clock.fetchAdd(1, .monotonic) + 1, .monotonic);
        cls.lock.lock();
        if (cls.head) |n| {
            cls.head = n.next;
            cls.count -= 1;
            cls.lock.unlock();
            _ = self.retained_bytes.fetchSub(classSize(idx), .monotonic);
            _ = self.hits.fetchAdd(1, .monotonic);
            return @ptrCast(n);
        }
        cls.lock.unlock();
        // Miss: mint a fresh, over-aligned, class-sized block.
        _ = self.misses.fetchAdd(1, .monotonic);
        _ = self.minted_bytes.fetchAdd(classSize(idx), .monotonic);
        return self.mint.rawAlloc(classSize(idx), .fromByteUnits(@as(usize, 1) << max_align_log2), ret_addr);
    }

    fn freeImpl(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *Pool = @ptrCast(@alignCast(ctx));
        const idx = classOf(buf.len, alignment) orelse
            return self.backing.rawFree(buf, alignment, ret_addr);
        const sz = classSize(idx);
        // Retain unless it would push us past the cap — then evict (the block was
        // minted class-sized + max-aligned, so free it exactly that way). A block
        // larger than the cap can never stay, so it evicts nothing else first.
        const prior = self.retained_bytes.load(.monotonic);
        if (sz > self.cap_bytes or prior + sz > self.cap_bytes and
            !self.evictStaleClasses(idx, prior + sz - self.cap_bytes))
        {
            _ = self.evictions.fetchAdd(1, .monotonic);
            _ = self.evicted_bytes.fetchAdd(sz, .monotonic);
            const base: [*]u8 = buf.ptr;
            return self.mint.rawFree(base[0..sz], .fromByteUnits(@as(usize, 1) << max_align_log2), ret_addr);
        }
        // Counted before it is listed: a block another thread pops the moment
        // it is listed is subtracted straight away, and a count that ran
        // behind the lists would wrap below zero.
        _ = self.retained_bytes.fetchAdd(sz, .monotonic);
        const node: *FreeNode = @ptrCast(@alignCast(buf.ptr));
        const cls = &self.classes[idx];
        cls.lock.lock();
        node.next = cls.head;
        cls.head = node;
        cls.count += 1;
        cls.lock.unlock();
    }

    fn resizeImpl(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *Pool = @ptrCast(@alignCast(ctx));
        // Invariant: a block's classification (pooled-class vs bypass) is FIXED
        // for its whole life, so `free` always routes it back the way it was
        // minted. A pooled block resizes in place only within its own class; a
        // bypass block resizes only while it stays bypass. Crossing the boundary
        // returns false → the caller reallocs (mint fresh + copy + free old), so
        // a backing-grown block can never be mistaken for a pool block.
        const old_cls = classOf(buf.len, alignment);
        const new_cls = classOf(new_len, alignment);
        if (old_cls) |idx| return new_cls == idx;
        if (new_cls != null) return false;
        return self.backing.rawResize(buf, alignment, new_len, ret_addr);
    }

    fn remapImpl(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *Pool = @ptrCast(@alignCast(ctx));
        const old_cls = classOf(buf.len, alignment);
        const new_cls = classOf(new_len, alignment);
        if (old_cls) |idx| {
            if (new_cls == idx) return buf.ptr;
            return null;
        }
        if (new_cls != null) return null;
        return self.backing.rawRemap(buf, alignment, new_len, ret_addr);
    }
};

// ---- Process-global singleton ---------------------------------------------

var g_pool: ?Pool = null;
var g_init = SpinLock{};
var g_ready = std.atomic.Value(bool).init(false);

/// Default retention cap: an eighth of the machine's (or container's) memory,
/// clamped to [64 MiB, 8 GiB]; 1 GiB when the memory size is unknown. The cap
/// bounds IDLE scratch, not a reservation — the pool only ever holds blocks a
/// query already touched — but it must cover a wide query's steady-state
/// scratch, because every block past it is minted and released at the OS on
/// each use. Measured on a 12-worker rollforward over 200K-3.4M-row stages:
/// ~3 GiB steady state; a 1 GiB cap minted and evicted ~6 GiB per query
/// (page faults + release ≈ 130 ms), 4 GiB and 8 GiB evicted nothing.
const default_cap_bytes: usize = 1 << 30;

/// Where the global pool mints its blocks. The Windows process heap serves
/// blocks this size from shared segments under one lock and decommits them on
/// free, so 12 workers minting and evicting 256 KiB blocks serialize on it:
/// ClickBench Q33 with its blocks evicted ran 2.6-3.6 s per run off the heap
/// against 1.0-1.2 s off VirtualAlloc. glibc serves the same sizes from
/// per-thread arenas.
const mint_allocator: Allocator = if (builtin.os.tag == .windows) std.heap.page_allocator else std.heap.c_allocator;
const min_cap_bytes: usize = 64 << 20;
const max_cap_bytes: usize = 8 << 30;

/// `THINDB_BUFPOOL_CAP_MB=<n>` pins the retention cap for an A/B without a
/// rebuild.
fn capFromEnv() ?usize {
    const v = getenv("THINDB_BUFPOOL_CAP_MB") orelse return null;
    const mb = std.fmt.parseInt(usize, std.mem.span(v), 10) catch return null;
    return mb << 20;
}

pub fn defaultCapFor(total_memory: ?u64) usize {
    const total = total_memory orelse return default_cap_bytes;
    const share: u64 = @min(total / 8, @as(u64, max_cap_bytes));
    const share_bytes: usize = @intCast(share);
    return @max(share_bytes, min_cap_bytes);
}

pub fn global() *Pool {
    if (!g_ready.load(.acquire)) {
        g_init.lock();
        defer g_init.unlock();
        if (g_pool == null) {
            g_pool = .{
                .backing = std.heap.c_allocator,
                .mint = mint_allocator,
                .cap_bytes = capFromEnv() orelse defaultCapFor(affinity.totalMemoryBytes()),
            };
            g_ready.store(true, .release);
        }
    }
    return &g_pool.?;
}

/// The shared pooled allocator. Hand this to parallel-scan workers as their
/// thread-safe `worker_alloc`.
pub fn allocator() Allocator {
    return global().allocator();
}

pub fn dumpGlobalStats() void {
    global().dumpStats();
}

/// Bytes a request on `alloc` really holds: the whole class-sized block when
/// `alloc` is a pool and the request is pooled (a recycled block is resident
/// in full), else `len`.
pub fn footprint(alloc: Allocator, len: usize, alignment: std.mem.Alignment) usize {
    if (alloc.vtable != &Pool.vtable) return len;
    const idx = Pool.classOf(len, alignment) orelse return len;
    return Pool.classSize(idx);
}

/// Idle bytes the process-global pool holds between queries; 0 before first use.
pub fn globalRetainedBytes() usize {
    if (!g_ready.load(.acquire)) return 0;
    return g_pool.?.retained_bytes.load(.monotonic);
}

/// `Pool.reclaim` on the process-global pool; 0 before first use.
pub fn globalReclaim(want: usize) usize {
    if (!g_ready.load(.acquire)) return 0;
    return g_pool.?.reclaim(want);
}

/// The allocator worker-side operators draw their large short-lived buffers
/// from (per-worker decode buffers, partition arenas): the shared pool in
/// production; `base` under `THINDB_NO_BUFPOOL=1` (an A/B without a
/// rebuild) and in tests, where leak detection must observe every free — the
/// pool retains blocks, which a leak detector would flag.
pub fn workerAllocator(base: Allocator) Allocator {
    if (builtin.is_test) return base;
    if (getenv("THINDB_NO_BUFPOOL")) |v| {
        if (v[0] == '1') return base;
    }
    return allocator();
}

// ---------------------------------------------------------------------------

test "pooled alloc/free round-trips and recycles the same block" {
    var pool = Pool.init(std.testing.allocator, default_cap_bytes);
    defer pool.drain();
    const a = pool.allocator();

    const b1 = try a.alloc(u8, 100 * 1024); // 100 KiB → 128 KiB class
    const p1 = b1.ptr;
    a.free(b1);
    // Next same-class alloc must hand back the retained block, no backing churn.
    const b2 = try a.alloc(u8, 90 * 1024);
    try std.testing.expectEqual(p1, b2.ptr);
    a.free(b2);
}

test "sub-threshold and over-size allocations bypass the pool cleanly" {
    var pool = Pool.init(std.testing.allocator, default_cap_bytes);
    defer pool.drain();
    const a = pool.allocator();

    const small = try a.alloc(u8, 1024); // < 64 KiB → backing
    a.free(small);
    try std.testing.expectEqual(@as(usize, 0), pool.retained_bytes.load(.monotonic));
    try std.testing.expectEqual(@as(?usize, null), Pool.classOf((1 << max_large_log2) + 1, .@"1"));
    try std.testing.expectEqual(@as(?usize, null), Pool.classOf(1 << 20, .fromByteUnits(128)));
}

test "past 64 MiB the classes step by a quarter of their power of two" {
    const mib: usize = 1 << 20;
    const cases = .{
        .{ .len = 64 * mib, .size = 64 * mib },
        .{ .len = 64 * mib + 1, .size = 80 * mib },
        .{ .len = 128 * mib, .size = 128 * mib },
        .{ .len = 128 * mib + 1, .size = 160 * mib },
        .{ .len = 711 * mib, .size = 768 * mib },
        .{ .len = 2048 * mib, .size = 2048 * mib },
    };
    inline for (cases) |c| {
        const idx = Pool.classOf(c.len, .@"1").?;
        try std.testing.expectEqual(@as(usize, c.size), Pool.classSize(idx));
    }
    try std.testing.expectEqual(class_count - 1, Pool.classOf(1 << max_large_log2, .@"1").?);
    var prev: usize = 0;
    for (0..class_count) |idx| {
        try std.testing.expect(Pool.classSize(idx) > prev);
        try std.testing.expectEqual(idx, Pool.classOf(Pool.classSize(idx), .@"1").?);
        prev = Pool.classSize(idx);
    }
}

test "a quarter-step block recycles like a power-of-two one" {
    var pool = Pool.init(std.testing.allocator, default_cap_bytes);
    defer pool.drain();
    const a = pool.allocator();

    const b1 = try a.alloc(u8, (64 << 20) + 4096);
    b1[b1.len - 1] = 1;
    const p1 = b1.ptr;
    a.free(b1);
    try std.testing.expectEqual(@as(usize, 80 << 20), pool.retained_bytes.load(.monotonic));
    const b2 = try a.alloc(u8, 79 << 20);
    try std.testing.expectEqual(p1, b2.ptr);
    a.free(b2);
}

test "a block larger than the cap goes back without evicting the idle blocks" {
    var pool = Pool.init(std.testing.allocator, 1 << 20);
    defer pool.drain();
    const a = pool.allocator();

    const idle = try a.alloc(u8, 256 << 10);
    a.free(idle);
    const big = try a.alloc(u8, 2 << 20);
    a.free(big);
    try std.testing.expectEqual(@as(usize, 256 << 10), pool.retained_bytes.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), pool.evictions.load(.monotonic));
}

test "retention cap evicts instead of growing without bound" {
    // Cap below one class block → every free must evict, nothing retained.
    var pool = Pool.init(std.testing.allocator, 32 * 1024);
    defer pool.drain();
    const a = pool.allocator();
    const b = try a.alloc(u8, 100 * 1024);
    a.free(b);
    try std.testing.expectEqual(@as(usize, 0), pool.retained_bytes.load(.monotonic));
}

test "a free at the cap evicts idle blocks of a staler class, never a more recent one" {
    const kib: usize = 1024;
    var pool = Pool.init(std.testing.allocator, 192 * kib);
    defer pool.drain();
    const a = pool.allocator();
    const large_idx = 17 - min_log2;
    const small_idx = 16 - min_log2;

    const stale = try a.alloc(u8, 128 * kib);
    const s1 = try a.alloc(u8, 64 * kib);
    const s2 = try a.alloc(u8, 64 * kib);
    a.free(stale);
    a.free(s1);
    a.free(s2);
    try std.testing.expectEqual(@as(usize, 0), pool.classes[large_idx].count);
    try std.testing.expectEqual(@as(usize, 2), pool.classes[small_idx].count);
    try std.testing.expectEqual(128 * kib, pool.retained_bytes.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), pool.evictions.load(.monotonic));

    const large = try a.alloc(u8, 128 * kib);
    const t1 = try a.alloc(u8, 64 * kib);
    const t2 = try a.alloc(u8, 64 * kib);
    a.free(t1);
    a.free(t2);
    a.free(large);
    try std.testing.expectEqual(@as(usize, 0), pool.classes[large_idx].count);
    try std.testing.expectEqual(@as(usize, 2), pool.classes[small_idx].count);
    try std.testing.expectEqual(128 * kib, pool.retained_bytes.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 2), pool.evictions.load(.monotonic));
}

test "reclaim releases the largest idle blocks first and stops once the request is covered" {
    var pool = Pool.init(std.testing.allocator, default_cap_bytes);
    defer pool.drain();
    const a = pool.allocator();
    const kib: usize = 1024;

    var small: [4][]u8 = undefined;
    for (&small) |*b| b.* = try a.alloc(u8, 64 * kib);
    var large: [3][]u8 = undefined;
    for (&large) |*b| b.* = try a.alloc(u8, 1024 * kib);
    for (small) |b| a.free(b);
    for (large) |b| a.free(b);
    try std.testing.expectEqual(4 * 64 * kib + 3 * 1024 * kib, pool.retained_bytes.load(.monotonic));

    try std.testing.expectEqual(2 * 1024 * kib, pool.reclaim(1500 * kib));
    try std.testing.expectEqual(4 * 64 * kib + 1024 * kib, pool.retained_bytes.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), pool.classes[20 - min_log2].count);
    try std.testing.expectEqual(@as(usize, 4), pool.classes[0].count);

    try std.testing.expectEqual(1024 * kib + 2 * 64 * kib, pool.reclaim(1100 * kib));
    try std.testing.expectEqual(2 * 64 * kib, pool.reclaim(std.math.maxInt(usize)));
    try std.testing.expectEqual(@as(usize, 0), pool.retained_bytes.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), pool.reclaim(1));

    const again = try a.alloc(u8, 1024 * kib);
    @memset(again, 0xCD);
    a.free(again);
}

test "reclaim alongside workers allocating and freeing keeps the free lists and the idle count in step" {
    var pool = Pool.init(std.testing.allocator, default_cap_bytes);
    defer pool.drain();
    const a = pool.allocator();

    const Worker = struct {
        fn run(alloc: Allocator) void {
            var i: usize = 0;
            while (i < 300) : (i += 1) {
                const n = 64 * 1024 + (i % 7) * 100 * 1024;
                const buf = alloc.alloc(u8, n) catch return;
                buf[0] = 1;
                buf[n - 1] = 2;
                alloc.free(buf);
            }
        }
    };
    const Reclaimer = struct {
        fn run(p: *Pool, stop: *std.atomic.Value(bool)) void {
            while (!stop.load(.acquire)) {
                _ = p.reclaim(300 * 1024);
                std.Thread.yield() catch {};
            }
        }
    };
    const Sampler = struct {
        fn run(p: *Pool, stop: *std.atomic.Value(bool), peak: *usize) void {
            while (!stop.load(.acquire)) peak.* = @max(peak.*, p.retained_bytes.load(.monotonic));
        }
    };
    const worker_count = 8;
    var peak: usize = 0;
    {
        var stop = std.atomic.Value(bool).init(false);
        const reclaimer = try std.Thread.spawn(.{}, Reclaimer.run, .{ &pool, &stop });
        defer reclaimer.join();
        defer stop.store(true, .release);
        const sampler = try std.Thread.spawn(.{}, Sampler.run, .{ &pool, &stop, &peak });
        defer sampler.join();
        defer stop.store(true, .release);
        var threads: [worker_count]std.Thread = undefined;
        var started: usize = 0;
        defer for (threads[0..started]) |t| t.join();
        for (&threads) |*t| {
            t.* = try std.Thread.spawn(.{}, Worker.run, .{a});
            started += 1;
        }
    }

    var listed: usize = 0;
    for (&pool.classes, 0..) |*cls, i| listed += cls.count * Pool.classSize(i);
    try std.testing.expectEqual(listed, pool.retained_bytes.load(.monotonic));
    // A count that ran behind the lists would have wrapped below zero for a
    // moment, which reads as nearly all of the address space being idle.
    try std.testing.expect(peak <= worker_count * Pool.classSize(class_count - 1));
}

test "ArrayList growth across the bypass/pool boundary stays consistent" {
    // The original integration crash: a small (bypass) buffer grown in place by
    // the backing across 64 KiB into pool range, then freed AS a pool block —
    // handing out an undersized block on the next alloc. The boundary-preserving
    // resize/remap must force a realloc at the crossing instead.
    var pool = Pool.init(std.testing.allocator, default_cap_bytes);
    defer pool.drain();
    const a = pool.allocator();

    var list: std.ArrayListUnmanaged(u8) = .empty;
    defer list.deinit(a);
    var i: usize = 0;
    while (i < 300 * 1024) : (i += 1) try list.append(a, @truncate(i));
    try std.testing.expectEqual(@as(usize, 300 * 1024), list.items.len);
    for (list.items, 0..) |b, j| try std.testing.expectEqual(@as(u8, @truncate(j)), b);

    // A fresh pool alloc in the class the grown buffer passed through must be a
    // full classSize block — writing all of it must not corrupt anything.
    const probe = try a.alloc(u8, 100 * 1024);
    @memset(probe, 0xAB);
    a.free(probe);
}

test "concurrent alloc/free across workers stays consistent and leak-free" {
    var pool = Pool.init(std.testing.allocator, default_cap_bytes);
    defer pool.drain();
    const a = pool.allocator();

    const Worker = struct {
        fn run(alloc: Allocator) void {
            var i: usize = 0;
            while (i < 200) : (i += 1) {
                const n = 64 * 1024 + (i % 7) * 100 * 1024;
                const buf = alloc.alloc(u8, n) catch return;
                buf[0] = 1;
                buf[n - 1] = 2;
                alloc.free(buf);
            }
        }
    };
    var threads: [8]std.Thread = undefined;
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, Worker.run, .{a});
    for (threads) |t| t.join();
    // drain (deferred) returns all retained blocks to testing.allocator — a leak
    // or double-free trips std.testing.allocator.
}

test "footprint is the class block for pooled requests and the length otherwise" {
    var pool = Pool.init(std.testing.allocator, default_cap_bytes);
    const a = pool.allocator();
    const byte: std.mem.Alignment = .@"1";
    try std.testing.expectEqual(@as(usize, 128 * 1024), footprint(a, 100 * 1024, byte));
    try std.testing.expectEqual(@as(usize, 1024), footprint(a, 1024, byte));
    try std.testing.expectEqual(@as(usize, 160 << 20), footprint(a, (1 << 27) + 7, byte));
    try std.testing.expectEqual(@as(usize, (3 << 30)), footprint(a, 3 << 30, byte));
    try std.testing.expectEqual(@as(usize, 100 * 1024), footprint(std.testing.allocator, 100 * 1024, byte));
}

test "defaultCapFor scales with the memory size and clamps both ends" {
    try std.testing.expectEqual(default_cap_bytes, defaultCapFor(null));
    try std.testing.expectEqual(@as(usize, 4 << 30), defaultCapFor(32 << 30));
    try std.testing.expectEqual(max_cap_bytes, defaultCapFor(64 << 30));
    try std.testing.expectEqual(max_cap_bytes, defaultCapFor(256 << 30));
    try std.testing.expectEqual(@as(usize, 512 << 20), defaultCapFor(4 << 30));
    try std.testing.expectEqual(min_cap_bytes, defaultCapFor(256 << 20));
}
