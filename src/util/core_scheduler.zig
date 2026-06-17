//! CoreScheduler — process-global CPU-core lease engine. Every parallel/serial
//! query stage leases the cores it needs from here, pins its threads to them,
//! and releases when the stage ends. One physical core hosts up to
//! `hw_threads_per_core + 1` leased threads (the +1 is deliberate
//! oversubscription so a core stays busy across memory/IO stalls); past that, a
//! thread asking for a core BLOCKS until one frees. The (cores × capacity) total
//! is therefore also the implicit ceiling on concurrent in-flight query work —
//! the bucket doubles as admission control.
//!
//! Process-global by design: cores are a machine resource shared by every
//! Database/query in the process, so a single shared bucket is what keeps two
//! concurrent queries from both pinning to the same cores. (The one legitimate
//! process singleton; holds no per-Database logical state.)
//!
//! Deadlock-freedom rests on one invariant: **a thread that already holds a
//! core never blocks for another** — if it needs one and none is free it runs
//! unpinned. That makes hold-and-wait impossible to express, so the blocking
//! wait can never form a cycle. Only fresh, core-less work ever waits; anything
//! already holding a core only ever releases. No starvation deadlock: a freed
//! slot is always eventually seen by a waiter's re-poll.
//!
//! Sync primitives: this Zig fork stripped std.Thread.{Mutex,Condition,sleep};
//! Io.Mutex needs an Io + an Io task, which raw worker threads aren't. So we use
//! a CAS spinlock (tiny critical section) + an OS sleep for the block-wait —
//! the same no-Io approach as storage/cache.zig and net/conn_registry.zig.

const std = @import("std");
const builtin = @import("builtin");
const affinity = @import("affinity.zig");

/// Per-thread flag: does THIS thread already hold a real core slot? Read by
/// `acquire` to honor the no-hold-and-wait invariant (a holder never blocks).
threadlocal var thread_holds_core: bool = false;

/// CAS spinlock — see module note on why not Io.Mutex. The critical section is
/// only the bucket arithmetic (a scan over a handful of cores), held for
/// microseconds, so spinning never matters in practice.
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

/// Result of `acquire`. Always returned (acquire never fails) — `owns` says
/// whether it actually took a bucket slot that `release` must give back.
pub const Lease = struct {
    sched: *CoreScheduler,
    /// Index into `cores` of the slot taken, or -1 when this lease didn't take
    /// one (holder-reuse, or pinning unsupported on this platform).
    core_index: i32 = -1,
    /// True iff this call took a slot + set the holder flag (so `release` must
    /// undo both). False for the reuse/no-op path.
    owns: bool = false,

    pub fn release(self: *Lease) void {
        self.sched.release(self);
    }
};

const Slot = struct {
    mask: u64,
    capacity: u8,
    leased: u16 = 0,
};

pub const CoreScheduler = struct {
    lock: SpinLock = .{},
    cores: []Slot,
    all_mask: u64,
    /// Runtime kill-switch (env `THINDB_NO_PIN`): when set, `acquire` is a no-op
    /// so the engine runs with no pinning/throttling. A production escape hatch
    /// and the lever for pinned-vs-unpinned A/B without a rebuild.
    disabled: bool = false,
    /// Backing allocation for `cores`; freed in deinit. (The global singleton
    /// never deinits — it lives for the process.)
    allocator: std.mem.Allocator,

    /// Poll interval a blocked waiter sleeps between bucket re-checks. Short so
    /// a freed core is picked up promptly; only ever hit under oversubscription.
    const WAIT_POLL_MS: u32 = 1;

    pub fn init(allocator: std.mem.Allocator) !CoreScheduler {
        const topo = affinity.detect(allocator);
        defer topo.deinit(allocator);
        const cores = try allocator.alloc(Slot, topo.cores.len);
        for (cores, topo.cores) |*s, c| s.* = .{ .mask = c.mask, .capacity = c.capacity };
        return .{
            .cores = cores,
            .all_mask = topo.all_mask,
            .disabled = getenv("THINDB_NO_PIN") != null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *CoreScheduler) void {
        self.allocator.free(self.cores);
    }

    /// Total slots across all cores — the ceiling on simultaneously-pinned
    /// threads (and thus on concurrent in-flight stage work).
    pub fn capacity(self: *const CoreScheduler) usize {
        var total: usize = 0;
        for (self.cores) |s| total += s.capacity;
        return total;
    }

    /// Lease one core for the calling thread and pin to it. Blocks (sleep-poll)
    /// until a slot frees when the bucket is full — UNLESS this thread already
    /// holds a core, in which case it returns a non-owning lease immediately
    /// (the no-hold-and-wait invariant) and keeps its existing pin.
    pub fn acquire(self: *CoreScheduler) Lease {
        if (self.disabled or thread_holds_core or self.cores.len == 0) {
            return .{ .sched = self, .core_index = -1, .owns = false };
        }
        while (true) {
            self.lock.lock();
            if (self.leastLoaded()) |idx| {
                self.cores[idx].leased += 1;
                const mask = self.cores[idx].mask;
                self.lock.unlock();
                thread_holds_core = true;
                affinity.pinCurrentThread(mask);
                return .{ .sched = self, .core_index = @intCast(idx), .owns = true };
            }
            self.lock.unlock();
            sleepMs(WAIT_POLL_MS);
        }
    }

    /// Non-blocking `acquire`: take a core slot if one is free RIGHT NOW, else
    /// return a non-owning (unpinned) lease instead of waiting. For callers whose
    /// threads coordinate at intra-query barriers — a worker that blocked here
    /// waiting for a slot could deadlock against a peer barrier-waiting while
    /// holding the only free slots. Best-effort pinning, never an admission gate.
    pub fn tryAcquire(self: *CoreScheduler) Lease {
        if (self.disabled or thread_holds_core or self.cores.len == 0) {
            return .{ .sched = self, .core_index = -1, .owns = false };
        }
        self.lock.lock();
        if (self.leastLoaded()) |idx| {
            self.cores[idx].leased += 1;
            const mask = self.cores[idx].mask;
            self.lock.unlock();
            thread_holds_core = true;
            affinity.pinCurrentThread(mask);
            return .{ .sched = self, .core_index = @intCast(idx), .owns = true };
        }
        self.lock.unlock();
        return .{ .sched = self, .core_index = -1, .owns = false };
    }

    pub fn release(self: *CoreScheduler, lease: *Lease) void {
        if (!lease.owns) return;
        lease.owns = false;
        self.lock.lock();
        const idx: usize = @intCast(lease.core_index);
        if (self.cores[idx].leased > 0) self.cores[idx].leased -= 1;
        self.lock.unlock();
        thread_holds_core = false;
        affinity.unpinCurrentThread(self.all_mask);
    }

    /// Least-loaded core with a free slot (fills distinct cores before doubling
    /// up, so the first N leases land on N separate physical cores). Null when
    /// every core is at capacity. Caller holds `lock`.
    fn leastLoaded(self: *CoreScheduler) ?usize {
        var best: ?usize = null;
        var best_load: u16 = std.math.maxInt(u16);
        for (self.cores, 0..) |s, i| {
            if (s.leased >= s.capacity) continue;
            if (s.leased < best_load) {
                best_load = s.leased;
                best = i;
            }
        }
        return best;
    }
};

/// Best-effort sleep, no Io required (std.Thread.sleep is gone in this fork).
fn sleepMs(ms: u32) void {
    switch (builtin.os.tag) {
        .windows => Sleep(ms),
        .linux => {
            const ts = [2]isize{ @intCast(ms / 1000), @intCast((ms % 1000) * std.time.ns_per_ms) };
            _ = std.os.linux.syscall2(.nanosleep, @intFromPtr(&ts), 0);
        },
        else => {
            // No portable no-Io sleep; yield-spin a bounded burst instead.
            var i: usize = 0;
            while (i < 4096) : (i += 1) std.Thread.yield() catch std.atomic.spinLoopHint();
        },
    }
}

extern "kernel32" fn Sleep(ms: u32) callconv(.winapi) void;
extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

// ---- Process-global singleton ---------------------------------------------

var g_sched: ?CoreScheduler = null;
var g_init = SpinLock{};
var g_ready = std.atomic.Value(bool).init(false);

/// The shared process-wide scheduler. Lazily initialized on first call.
pub fn global() *CoreScheduler {
    if (!g_ready.load(.acquire)) {
        g_init.lock();
        defer g_init.unlock();
        if (g_sched == null) {
            g_sched = CoreScheduler.init(std.heap.page_allocator) catch CoreScheduler{
                .cores = &.{},
                .all_mask = 0,
                .allocator = std.heap.page_allocator,
            };
            g_ready.store(true, .release);
        }
    }
    return &g_sched.?;
}

// ---------------------------------------------------------------------------

test "least-loaded fills distinct cores before doubling up" {
    var s = try CoreScheduler.init(std.testing.allocator);
    defer s.deinit();
    if (s.cores.len < 2) return error.SkipZigTest;

    // First pick is core 0 (all empty). Simulate taking it, then the next pick
    // must be a DIFFERENT core, not core 0 again.
    const a = s.leastLoaded().?;
    s.cores[a].leased += 1;
    const b = s.leastLoaded().?;
    try std.testing.expect(a != b);
    try std.testing.expectEqual(@as(u16, 0), s.cores[b].leased);
    s.cores[a].leased = 0;
}

test "capacity full then a freed slot reopens" {
    var s = try CoreScheduler.init(std.testing.allocator);
    defer s.deinit();
    if (s.cores.len == 0) return error.SkipZigTest;

    for (s.cores) |*c| c.leased = c.capacity; // saturate
    try std.testing.expect(s.leastLoaded() == null);
    s.cores[0].leased -= 1; // free one slot on core 0
    try std.testing.expectEqual(@as(?usize, 0), s.leastLoaded());
    for (s.cores) |*c| c.leased = 0;
}

test "holder never takes a second slot (no hold-and-wait)" {
    var s = try CoreScheduler.init(std.testing.allocator);
    defer s.deinit();
    if (s.cores.len == 0) return error.SkipZigTest;

    var outer = s.acquire();
    try std.testing.expect(outer.owns);
    // A nested acquire on the same thread must NOT take a slot or block.
    var inner = s.acquire();
    try std.testing.expect(!inner.owns);
    try std.testing.expectEqual(@as(i32, -1), inner.core_index);
    inner.release(); // no-op
    outer.release();
    for (s.cores) |c| try std.testing.expectEqual(@as(u16, 0), c.leased);
}

test "blocked waiter resolves when a core frees" {
    var s = try CoreScheduler.init(std.testing.allocator);
    defer s.deinit();
    const total = s.capacity();
    if (total == 0) return error.SkipZigTest;

    // Fillers each take one slot and hold until `go` flips; main thread then
    // tries to acquire — every slot is taken so it must block, and succeed only
    // once the fillers release.
    const Filler = struct {
        fn run(sched: *CoreScheduler, go: *std.atomic.Value(bool), filled: *std.atomic.Value(usize)) void {
            var lease = sched.acquire();
            _ = filled.fetchAdd(1, .monotonic);
            while (!go.load(.acquire)) std.atomic.spinLoopHint();
            lease.release();
        }
    };
    const allocator = std.testing.allocator;
    const threads = try allocator.alloc(std.Thread, total);
    defer allocator.free(threads);
    var go = std.atomic.Value(bool).init(false);
    var filled = std.atomic.Value(usize).init(0);
    for (threads) |*t| t.* = try std.Thread.spawn(.{}, Filler.run, .{ &s, &go, &filled });
    while (filled.load(.monotonic) < total) std.atomic.spinLoopHint();

    go.store(true, .release); // let fillers release; the bucket drains
    for (threads) |t| t.join();
    var l = s.acquire(); // now succeeds
    try std.testing.expect(l.owns);
    l.release();
}
