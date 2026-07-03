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
//! Scope: only allocations in `[min_class, max_class]` with alignment ≤
//! `max_align` are pooled. Smaller allocations pass straight through (the
//! backing allocator already pools those cheaply); larger ones are too rare and
//! big to retain. The free-list decision uses the same `(len, alignment)`
//! predicate on alloc and free, so a block is always classified identically both
//! ways. Retained bytes are capped; frees past the cap evict to the backing
//! allocator instead of retaining.
//!
//! Thread-safety: workers allocate concurrently, so each size class carries its
//! own spinlock (distinct sizes rarely contend). The pool is a process
//! singleton wrapping `c_allocator`, mirroring util/core_scheduler.zig and the
//! huge-page slab pool — a machine resource shared by every query.

const std = @import("std");
const Allocator = std.mem.Allocator;

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

/// Pooled allocations span `[1<<min_log2, 1<<max_log2]` bytes. 64 KiB covers the
/// mmap-threshold boundary (glibc/CRT route allocations past ~128 KiB to the
/// kernel; 64 KiB is a safe floor) and 64 MiB caps a single retained block.
const min_log2: u6 = 16; // 64 KiB
const max_log2: u6 = 26; // 64 MiB
const class_count: usize = max_log2 - min_log2 + 1;

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
};

pub const Pool = struct {
    backing: Allocator,
    classes: [class_count]Class = [_]Class{.{}} ** class_count,
    retained_bytes: std.atomic.Value(usize) = .{ .raw = 0 },
    cap_bytes: usize,

    pub fn init(backing: Allocator, cap_bytes: usize) Pool {
        return .{ .backing = backing, .cap_bytes = cap_bytes };
    }

    /// Drain every retained block back to the backing allocator. For shutdown
    /// and tests; never on the query path.
    pub fn drain(self: *Pool) void {
        for (&self.classes, 0..) |*cls, i| {
            const sz = @as(usize, 1) << @intCast(min_log2 + i);
            cls.lock.lock();
            var node = cls.head;
            while (node) |n| {
                const next = n.next;
                const buf: [*]u8 = @ptrCast(n);
                self.backing.rawFree(buf[0..sz], .fromByteUnits(@as(usize, 1) << max_align_log2), @returnAddress());
                node = next;
            }
            cls.head = null;
            cls.count = 0;
            cls.lock.unlock();
        }
        self.retained_bytes.store(0, .monotonic);
    }

    pub fn allocator(self: *Pool) Allocator {
        return .{ .ptr = self, .vtable = &vtable };
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
        if (need_log2 > max_log2) return null;
        return need_log2 - min_log2;
    }

    fn classSize(idx: usize) usize {
        return @as(usize, 1) << @intCast(min_log2 + idx);
    }

    fn allocImpl(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Pool = @ptrCast(@alignCast(ctx));
        const idx = classOf(len, alignment) orelse
            return self.backing.rawAlloc(len, alignment, ret_addr);
        const cls = &self.classes[idx];
        cls.lock.lock();
        if (cls.head) |n| {
            cls.head = n.next;
            cls.count -= 1;
            cls.lock.unlock();
            _ = self.retained_bytes.fetchSub(classSize(idx), .monotonic);
            return @ptrCast(n);
        }
        cls.lock.unlock();
        // Miss: mint a fresh, over-aligned, class-sized block from the backing.
        return self.backing.rawAlloc(classSize(idx), .fromByteUnits(@as(usize, 1) << max_align_log2), ret_addr);
    }

    fn freeImpl(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *Pool = @ptrCast(@alignCast(ctx));
        const idx = classOf(buf.len, alignment) orelse
            return self.backing.rawFree(buf, alignment, ret_addr);
        const sz = classSize(idx);
        // Retain unless it would push us past the cap — then evict (the block was
        // minted class-sized + max-aligned, so free it exactly that way).
        const prior = self.retained_bytes.load(.monotonic);
        if (prior + sz > self.cap_bytes) {
            const base: [*]u8 = buf.ptr;
            return self.backing.rawFree(base[0..sz], .fromByteUnits(@as(usize, 1) << max_align_log2), ret_addr);
        }
        const node: *FreeNode = @ptrCast(@alignCast(buf.ptr));
        const cls = &self.classes[idx];
        cls.lock.lock();
        node.next = cls.head;
        cls.head = node;
        cls.count += 1;
        cls.lock.unlock();
        _ = self.retained_bytes.fetchAdd(sz, .monotonic);
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

/// Default retention cap: bounded scratch recycling, deliberately small relative
/// to the block cache (which owns the ~35%-of-RAM working set). 1 GiB holds a
/// few full sets of 12-worker decode buffers — enough to recycle across
/// back-to-back queries without competing with the cache for RAM.
const default_cap_bytes: usize = 1 << 30;

pub fn global() *Pool {
    if (!g_ready.load(.acquire)) {
        g_init.lock();
        defer g_init.unlock();
        if (g_pool == null) {
            g_pool = Pool.init(std.heap.c_allocator, default_cap_bytes);
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
    const huge = try a.alloc(u8, (1 << 27) + 7); // > 64 MiB → backing
    a.free(huge);
    try std.testing.expectEqual(@as(usize, 0), pool.retained_bytes.load(.monotonic));
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
