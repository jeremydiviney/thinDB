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
//! Deadlock-freedom rests on one invariant: **work that already holds a core
//! never blocks for another** — if it needs one and none is free it runs
//! unpinned. "Work" is the statement, not the thread: a statement's worker
//! threads are joined by its calling thread, which holds the admission lease,
//! so they take `tryAcquire`, never `acquire`. Blocking `acquire` is for
//! admitting a fresh statement only. That makes hold-and-wait impossible to
//! express, so the blocking wait can never form a cycle. No starvation
//! deadlock: a freed slot is always eventually seen by a waiter's re-poll.
//!
//! Fan-out sizing rides the same bookkeeping: `reserveWorkers` grants a
//! fan-out its thread count from a machine-wide budget of hardware threads, so
//! concurrent statements split the machine instead of each spawning its full
//! DOP onto the same cores. Leases still gate admission; the budget only
//! sizes.
//!
//! Sync primitives: this Zig fork stripped std.Thread.{Mutex,Condition,sleep};
//! Io.Mutex needs an Io + an Io task, which raw worker threads aren't. So we use
//! a CAS spinlock (tiny critical section) + an OS sleep for the block-wait —
//! the same no-Io approach as storage/cache.zig and net/conn_registry.zig.

const std = @import("std");
const builtin = @import("builtin");
const affinity = @import("affinity.zig");

/// Whether a lease also PINS the calling thread to its core. Windows only.
///
/// Windows: SetThreadAffinityMask affects just the calling thread — threads it
/// later spawns start with the process-wide mask, so a pinned connection thread
/// still fans its exec workers across every core. Pinning there is a measured
/// ~40% scaling win (see util/affinity.zig).
///
/// Linux: a spawned thread INHERITS the spawner's affinity mask (clone
/// semantics). The admission lease in mysql/server.zig runs on the connection
/// thread, which spawns every exec worker for the statement — pinning it to
/// one core silently trapped all non-re-pinning workers on that core (whole
/// queries at ~1 core of CPU; 7-9× slower on heavy rollforwards). Measured
/// unpinned Linux runs at parity with pinned Windows per-clock, so leases on
/// Linux do accounting only. If Linux worker pinning is ever revisited, every
/// spawn under a narrowed mask needs a child-side affinity reset first.
///
/// Lease ACCOUNTING (admission control) is platform-independent and unaffected.
const PIN_THREADS = builtin.os.tag == .windows;

/// Per-thread flag: does THIS thread already hold a real core slot? Read by
/// `acquire` to honor the no-hold-and-wait invariant (a holder never blocks).
threadlocal var thread_holds_core: bool = false;

/// Per-thread flag: is THIS thread a statement's admitted thread (it holds an
/// `acquire` lease)? Its fan-outs get a fair share of the cores; a nested
/// fan-out from a worker thread gets only what is idle.
threadlocal var thread_admitted: bool = false;

/// Per-thread flag: is THIS thread's admission parked for a blocking send
/// (see `parkAdmission`)? Its statement is out of `statements` and `busy`.
threadlocal var thread_parked: bool = false;

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
    /// True iff this is a statement's admission lease (`acquire`), counted in
    /// `statements` and `busy`.
    admission: bool = false,

    pub fn release(self: *Lease) void {
        self.sched.release(self);
    }
};

/// An admission taken out of fan-out sizing by `parkAdmission`; `unpark`
/// counts it again. Empty when parking was a no-op.
pub const Parked = struct {
    sched: ?*CoreScheduler = null,

    pub fn unpark(self: *Parked) void {
        const sched = self.sched orelse return;
        self.sched = null;
        sched.unparkAdmission();
    }
};

/// A fan-out's claim on the machine: `count` threads, the calling thread
/// included. `release` hands the helpers back once they are joined.
pub const Workers = struct {
    sched: ?*CoreScheduler = null,
    count: usize = 1,
    /// Helper threads counted in `busy` (`count - 1` when budgeted, else 0).
    held: usize = 0,

    pub fn release(self: *Workers) void {
        const sched = self.sched orelse return;
        self.sched = null;
        sched.releaseWorkers(self.held);
        self.held = 0;
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
    /// Hardware threads this process can keep busy (affinity set and CPU
    /// quota): the fan-out budget. 0 = unknown, fan-outs run unbudgeted.
    hardware_threads: usize = 0,
    /// Threads running statement work: one per admitted statement plus every
    /// helper a `reserveWorkers` grant holds.
    busy: usize = 0,
    /// Statements holding an admission lease.
    statements: usize = 0,
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
            .hardware_threads = affinity.allowedHardwareThreadCount(),
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

    /// Size a fan-out that wants `wanted` threads (the calling thread
    /// included) and hold the helpers against the budget until
    /// `Workers.release`. A statement's thread gets the larger of an equal
    /// share of the hardware threads among admitted statements and what is
    /// idle; a worker thread's nested fan-out gets only what is idle. SMT
    /// siblings count: on an 8-core, 16-thread c6a, ClickBench ran 9.94 s
    /// over 16 threads against 10.91 s over the 8 cores. A snapshot for
    /// sizing, never a gate — the count can be anything from 1 up, so callers
    /// must be correct at any count.
    pub fn reserveWorkers(self: *CoreScheduler, wanted: usize) Workers {
        if (wanted <= 1 or self.disabled or self.hardware_threads == 0) return .{ .count = @max(wanted, 1) };
        self.lock.lock();
        defer self.lock.unlock();
        const n = @min(wanted, self.workerBudget(thread_admitted));
        self.busy += n - 1;
        return .{ .sched = self, .count = n, .held = n - 1 };
    }

    fn releaseWorkers(self: *CoreScheduler, held: usize) void {
        self.lock.lock();
        defer self.lock.unlock();
        self.busy -|= held;
    }

    /// Take the calling thread's statement out of fan-out sizing while it
    /// blocks sending results. A statement waiting on a slow client runs no
    /// work, yet counted it shrinks every other statement's share toward one
    /// thread on an idle machine (#805). Only the sizing counts change: the
    /// admission slot stays held, so admission control is unchanged. A no-op
    /// on a thread without an admission lease or already parked.
    pub fn parkAdmission(self: *CoreScheduler) Parked {
        if (!thread_admitted or thread_parked) return .{};
        self.lock.lock();
        defer self.lock.unlock();
        self.statements -|= 1;
        self.busy -|= 1;
        thread_parked = true;
        return .{ .sched = self };
    }

    fn unparkAdmission(self: *CoreScheduler) void {
        if (!thread_parked) return;
        self.lock.lock();
        defer self.lock.unlock();
        self.statements += 1;
        self.busy += 1;
        thread_parked = false;
    }

    /// Threads a fan-out may run right now. The +1 is the calling thread,
    /// already counted in `busy` (its statement's admission or its parent's
    /// grant). Caller holds `lock`.
    fn workerBudget(self: *const CoreScheduler, admitted: bool) usize {
        const threads = self.hardware_threads;
        const idle = threads -| self.busy;
        const share = if (admitted) std.math.divCeil(usize, threads, @max(self.statements, 1)) catch threads else 1;
        return @max(1, @min(threads, @max(share, idle + 1)));
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
                self.statements += 1;
                self.busy += 1;
                const mask = self.cores[idx].mask;
                self.lock.unlock();
                thread_holds_core = true;
                thread_admitted = true;
                if (PIN_THREADS) affinity.pinCurrentThread(mask);
                return .{ .sched = self, .core_index = @intCast(idx), .owns = true, .admission = true };
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
            if (PIN_THREADS) affinity.pinCurrentThread(mask);
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
        // A parked admission is already out of the counts; its `unpark` then
        // finds nothing to restore.
        if (lease.admission and !thread_parked) {
            self.statements -|= 1;
            self.busy -|= 1;
        }
        self.lock.unlock();
        thread_holds_core = false;
        if (lease.admission) {
            thread_admitted = false;
            thread_parked = false;
        }
        if (PIN_THREADS) affinity.unpinCurrentThread(self.all_mask);
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

/// `CoreScheduler.reserveWorkers` on the process-wide scheduler.
pub fn reserveWorkers(wanted: usize) Workers {
    return global().reserveWorkers(wanted);
}

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

test "worker budget: a lone statement gets every hardware thread, concurrent ones split them" {
    var s: CoreScheduler = .{ .cores = &.{}, .all_mask = 0, .allocator = std.testing.allocator, .hardware_threads = 16 };
    s.statements = 1;
    s.busy = 1;
    try std.testing.expectEqual(@as(usize, 16), s.workerBudget(true));
    // The first statement still runs 16 threads: the newcomer gets its share.
    s.statements = 2;
    s.busy = 17;
    try std.testing.expectEqual(@as(usize, 8), s.workerBudget(true));
    s.statements = 3;
    s.busy = 25;
    try std.testing.expectEqual(@as(usize, 6), s.workerBudget(true));
    // Idle threads beat the share.
    s.statements = 2;
    s.busy = 3;
    try std.testing.expectEqual(@as(usize, 14), s.workerBudget(true));
    // A worker thread's nested fan-out only gets what is idle.
    s.statements = 1;
    s.busy = 16;
    try std.testing.expectEqual(@as(usize, 1), s.workerBudget(false));
    s.busy = 10;
    try std.testing.expectEqual(@as(usize, 7), s.workerBudget(false));
}

test "reserved workers count against later fan-outs until released" {
    var s: CoreScheduler = .{ .cores = &.{}, .all_mask = 0, .allocator = std.testing.allocator, .hardware_threads = 8 };
    var a = s.reserveWorkers(32);
    try std.testing.expectEqual(@as(usize, 8), a.count);
    var b = s.reserveWorkers(32);
    try std.testing.expectEqual(@as(usize, 2), b.count);
    var c = s.reserveWorkers(1);
    try std.testing.expectEqual(@as(usize, 1), c.count);
    c.release();
    b.release();
    a.release();
    a.release();
    try std.testing.expectEqual(@as(usize, 0), s.busy);
    s.disabled = true;
    var d = s.reserveWorkers(100);
    try std.testing.expectEqual(@as(usize, 100), d.count);
    d.release();
    s.disabled = false;
    s.hardware_threads = 0;
    var e = s.reserveWorkers(100);
    try std.testing.expectEqual(@as(usize, 100), e.count);
    e.release();
}

test "admission leases count statements and their threads" {
    var s = try CoreScheduler.init(std.testing.allocator);
    defer s.deinit();
    if (s.cores.len == 0) return error.SkipZigTest;
    s.disabled = false;
    var lease = s.acquire();
    try std.testing.expect(lease.admission);
    try std.testing.expectEqual(@as(usize, 1), s.statements);
    try std.testing.expectEqual(@as(usize, 1), s.busy);
    var helper = s.tryAcquire();
    try std.testing.expect(!helper.admission);
    try std.testing.expectEqual(@as(usize, 1), s.busy);
    helper.release();
    lease.release();
    try std.testing.expectEqual(@as(usize, 0), s.statements);
    try std.testing.expectEqual(@as(usize, 0), s.busy);
}

test "a parked admission leaves fan-out sizing until it is unparked" {
    var s = try CoreScheduler.init(std.testing.allocator);
    defer s.deinit();
    if (s.cores.len == 0) return error.SkipZigTest;
    s.disabled = false;
    const unadmitted = s.parkAdmission();
    try std.testing.expect(unadmitted.sched == null);

    var lease = s.acquire();
    var parked = s.parkAdmission();
    try std.testing.expectEqual(@as(usize, 0), s.statements);
    try std.testing.expectEqual(@as(usize, 0), s.busy);
    const twice = s.parkAdmission();
    try std.testing.expect(twice.sched == null);
    try std.testing.expectEqual(@as(u16, 1), s.cores[@intCast(lease.core_index)].leased);
    parked.unpark();
    parked.unpark();
    try std.testing.expectEqual(@as(usize, 1), s.statements);
    try std.testing.expectEqual(@as(usize, 1), s.busy);

    var late = s.parkAdmission();
    lease.release();
    late.unpark();
    try std.testing.expectEqual(@as(usize, 0), s.statements);
    try std.testing.expectEqual(@as(usize, 0), s.busy);
    for (s.cores) |c| try std.testing.expectEqual(@as(u16, 0), c.leased);
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

test "linux: leases never narrow thread affinity (spawned workers must inherit the full mask)" {
    // Regression for the datacenter 7-9× slowdown: Linux child threads inherit
    // the spawner's affinity mask, so a pinned admission thread trapped every
    // exec worker it spawned on one CPU. Leases must be accounting-only here.
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var s = try CoreScheduler.init(std.testing.allocator);
    defer s.deinit();
    if (s.cores.len == 0) return error.SkipZigTest;

    const before = linuxCurrentAffinity();
    var lease = s.acquire();
    try std.testing.expect(lease.owns);
    try std.testing.expectEqual(before, linuxCurrentAffinity());
    lease.release();
    try std.testing.expectEqual(before, linuxCurrentAffinity());

    var tl = s.tryAcquire();
    try std.testing.expect(tl.owns);
    try std.testing.expectEqual(before, linuxCurrentAffinity());
    tl.release();
    try std.testing.expectEqual(before, linuxCurrentAffinity());
}

fn linuxCurrentAffinity() u64 {
    if (builtin.os.tag != .linux) return 0;
    var set: [128]u8 = @splat(0);
    _ = std.os.linux.syscall3(.sched_getaffinity, 0, set.len, @intFromPtr(&set));
    return std.mem.readInt(u64, set[0..8], .little);
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
