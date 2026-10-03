//! Per-query execution allocation accounting and cooperative cancellation.
//!
//! Physical operators and worker allocators share one thread-safe ledger. It
//! charges live requested capacities (a pooled scratch block at its whole size
//! class) to per-query/shared limits; planner row estimates do not enforce
//! those limits. Output stays charged until released. Retained pool storage
//! attaches to the current borrower; while idle it is charged to nobody, has
//! its own retention cap, and yields to live demand: live plus idle bytes
//! stay within the shared budget (`MemoryPool.yieldIdle`).
//! General-allocator metadata, rounding and freelists, database
//! metadata, parser/wire buffers and source cache are separate costs; the
//! watchdog (`MemoryAccountant.watch`) reports when they grow large.
//!
//! Query ownership can retire before asynchronous frees finish. The ledger and
//! its allocator wrappers survive until the final tracked allocation is freed.
//! A free whose charge must end when the owner lets go, while the memory goes
//! back in the background, collects its blocks in `DeferredFrees`: the bytes
//! leave the budget at once and the ledger stays alive until they are back.

const std = @import("std");
pub const BudgetAllocator = @import("util/budget_allocator.zig").BudgetAllocator;
pub const DeferredFrees = @import("util/budget_allocator.zig").DeferredFrees;
const affinity = @import("util/affinity.zig");
const buffer_pool = @import("util/buffer_pool.zig");
const huge_page = @import("util/huge_page.zig");
const block_cache = @import("storage/cache.zig");
const prof = @import("util/prof.zig");

pub const Error = error{
    /// A query's accumulated memory in a blocking operator would
    /// exceed `Config.query_memory_budget`. The operator returns this
    /// error from its `next` or `create` rather than allocate.
    MemoryBudgetExceeded,
};

/// Which blocking operator a reservation belongs to. Used only for the
/// failure-time breakdown — every operator passes its own tag so an
/// over-budget query can report where the memory went.
pub const Source = enum {
    sort,
    topn,
    hash_aggregate,
    materialize,
    join_build,
    nested_loop,
    range_sweep,
    sort_merge_join,
    window,
    subquery,
    execution,
};

const source_count = std.meta.fields(Source).len;

pub const accountantOf = BudgetAllocator.accountantOf;
pub const ownerOf = BudgetAllocator.ownerOf;

pub fn checkCancelled(allocator: std.mem.Allocator) error{QueryCancelled}!void {
    if (accountantOf(allocator)) |a| try a.checkCancelled();
}

pub fn sort(comptime T: type, items: []T, context: anytype, allocator: std.mem.Allocator, comptime less: fn (@TypeOf(context), T, T) bool) error{QueryCancelled}!void {
    const flag = if (accountantOf(allocator)) |a| a.cancel_flag else null;
    try @import("util/cancellable_sort.zig").pdq(T, items, context, flag, less);
}

pub fn executionAllocator(fallback: std.mem.Allocator, accountant: ?*MemoryAccountant) !std.mem.Allocator {
    if (accountant) |a| if (a.physical_tracking) return a.executionAllocator();
    return fallback;
}

pub fn trackedBackend(child: std.mem.Allocator, accountant: ?*MemoryAccountant) !std.mem.Allocator {
    if (accountant) |a| return a.wrapAllocator(child);
    return child;
}

/// Thread-safe backing for a query's large worker-side buffers: the
/// process-global retaining pool, or `fallback` in tests and under
/// THINDB_NO_BUFPOOL. Pooling recycles blocks between queries; it never takes
/// them out of the budget of the query that holds them, so every pooled block
/// in use is charged to `accountant`.
pub fn workerAllocator(accountant: ?*MemoryAccountant, fallback: std.mem.Allocator) !std.mem.Allocator {
    return trackedBackend(buffer_pool.workerAllocator(fallback), accountant);
}

/// `workerAllocator` for the buffers of an operator built on `allocator`.
/// An operator built on a retained pool's allocator outlives the query that
/// built it, so its buffers stay on that allocator, which the pool charges to
/// each query in turn. A pooled block would stay charged to the first query
/// and hold that query's accounting, and its shutdown lease, open for as
/// long as the operator lives.
pub fn workerAllocatorOf(allocator: std.mem.Allocator) !std.mem.Allocator {
    return workerBackingOf(buffer_pool.workerAllocator(allocator), allocator);
}

fn workerBackingOf(pooled: std.mem.Allocator, allocator: std.mem.Allocator) !std.mem.Allocator {
    if (BudgetAllocator.isRetained(allocator)) return allocator;
    return trackedBackend(pooled, accountantOf(allocator));
}

pub fn allocationError(accountant: ?*MemoryAccountant, err: anytype) (@TypeOf(err) || Error) {
    if (err == error.OutOfMemory) if (accountant) |a| {
        if (a.exceeded.load(.acquire)) return error.MemoryBudgetExceeded;
    };
    return err;
}

/// One reading of where the process's resident memory is. Everything outside
/// the cache, the idle scratch pools and the accounted query bytes is memory
/// no budget bounds.
pub const MemorySnapshot = struct {
    resident: u64,
    cache: u64,
    retained: u64,
    accounted: u64,

    pub fn unaccounted(self: MemorySnapshot) u64 {
        return self.resident -| self.cache -| self.retained -| self.accounted;
    }
};

fn cacheBytes() u64 {
    return @max(huge_page.g_slab_bytes.load(.monotonic), block_cache.g_cache_bytes.load(.monotonic));
}

/// Unaccounted memory that earns a watchdog line: 2 GiB, or a quarter of the
/// per-query budget when that is larger.
pub fn watchThreshold(budget: usize) usize {
    const floor: usize = 2 << 30;
    if (budget == std.math.maxInt(usize)) return floor;
    return @max(floor, budget / 4);
}

/// Accounted growth between watchdog samples. Statements that never reach one
/// step are never sampled, so small statements pay no probe.
fn watchStep(budget: usize) usize {
    const min_step: usize = 64 << 20;
    const max_step: usize = 1 << 30;
    if (budget == std.math.maxInt(usize)) return max_step;
    return std.math.clamp(budget / 16, min_step, max_step);
}

/// Memory a subsystem keeps warm between uses and gives back when asked: the
/// scratch pool's free lists, a cached region program's buffers. Idle bytes
/// are charged to no statement; a block a statement takes out of a source
/// leaves the idle count and enters that statement's charge. A source
/// registers with the `MemoryPool` whose budget its memory shares.
pub const IdleSource = struct {
    idle_bytes_fn: *const fn (source: *IdleSource) usize,
    /// Release up to `want` idle bytes, least valuable first, and return how
    /// many are on their way back to the allocator. Runs on whichever thread
    /// found the pool over its budget.
    reclaim_fn: *const fn (source: *IdleSource, want: usize) usize,
    next: ?*IdleSource = null,
};

fn scratchIdleBytes(_: *IdleSource) usize {
    return buffer_pool.globalRetainedBytes();
}

fn scratchReclaim(_: *IdleSource, want: usize) usize {
    return buffer_pool.globalReclaim(want);
}

/// What a reclaim releases beyond the excess: one sampling step of the shared
/// budget. Giving back only the excess would have the next step of growth
/// reclaim again; giving back everything would cool buffers nobody asked for.
fn idleSlack(budget: usize) usize {
    return watchStep(budget);
}

/// Process-shared memory pool: one budget every query's accountant draws
/// from, so CONCURRENT queries can't sum past the box even when each is
/// individually under its per-query ceiling. Owned by the Catalog (one per
/// server process / embedded Catalog); thread-safe — queries reserve from
/// their own connection threads.
///
/// The budget also bounds the idle memory of the sources registered here:
/// live bytes plus idle bytes stay within it, checked where a statement
/// starts, grows by a sampling step, or is refused (`yieldIdle`). Admission
/// compares live bytes only, so idle memory never refuses a reservation; it
/// is given back instead.
pub const MemoryPool = struct {
    budget: usize,
    used: std.atomic.Value(usize) = .init(0),
    /// The highest `used` since the last `takeChargePeak`.
    charge_peak: std.atomic.Value(usize) = .init(0),
    /// Numbers the statements whose accountants draw from this pool.
    statements: std.atomic.Value(u64) = .init(0),
    /// The process scratch pool's free lists, the first idle source of every
    /// pool. The scratch pool is one per process: two pools in one process
    /// each count it and each may reclaim from it.
    scratch: IdleSource = .{ .idle_bytes_fn = scratchIdleBytes, .reclaim_fn = scratchReclaim },
    /// Guards the registered sources, the list behind `scratch.next`.
    idle_lock: std.atomic.Mutex = .unlocked,
    /// Held by the one thread giving idle memory back.
    yield_lock: std.atomic.Mutex = .unlocked,

    pub fn init(budget: usize) MemoryPool {
        return .{ .budget = budget };
    }

    fn lockIdle(self: *MemoryPool) void {
        while (!self.idle_lock.tryLock()) std.atomic.spinLoopHint();
    }

    /// `source` must stay at its address until `unregisterIdle`.
    pub fn registerIdle(self: *MemoryPool, source: *IdleSource) void {
        self.lockIdle();
        defer self.idle_lock.unlock();
        source.next = self.scratch.next;
        self.scratch.next = source;
    }

    /// On return no reclaim is running on `source` and none will start.
    pub fn unregisterIdle(self: *MemoryPool, source: *IdleSource) void {
        self.lockIdle();
        defer self.idle_lock.unlock();
        var link = &self.scratch.next;
        while (link.*) |s| : (link = &s.next) {
            if (s != source) continue;
            link.* = source.next;
            source.next = null;
            return;
        }
    }

    pub fn idleBytes(self: *MemoryPool) usize {
        var total = self.scratch.idle_bytes_fn(&self.scratch);
        self.lockIdle();
        defer self.idle_lock.unlock();
        var source = self.scratch.next;
        while (source) |s| : (source = s.next) total +|= s.idle_bytes_fn(s);
        return total;
    }

    /// When live plus idle bytes have passed the budget, idle sources give
    /// back the excess plus `idleSlack`: the scratch pool first, whose blocks
    /// cost a page fault to mint again, then the registered sources. A thread
    /// that finds another one already reclaiming leaves it to that thread.
    /// The scratch pool's frees run outside `idle_lock` — its source is
    /// embedded and never unregisters — so a long release holds up neither a
    /// reading of `idleBytes` nor a closing database.
    pub fn yieldIdle(self: *MemoryPool) void {
        if (self.budget == std.math.maxInt(usize)) return;
        if (!self.yield_lock.tryLock()) return;
        defer self.yield_lock.unlock();
        const idle = self.idleBytes();
        const excess = (self.inUse() +| idle) -| self.budget;
        if (excess == 0) return;
        var want: usize = @min(idle, excess +| idleSlack(self.budget));
        want -|= self.scratch.reclaim_fn(&self.scratch, want);
        if (want == 0) return;
        self.lockIdle();
        defer self.idle_lock.unlock();
        var source = self.scratch.next;
        while (source) |s| : (source = s.next) {
            want -|= s.reclaim_fn(s, want);
            if (want == 0) return;
        }
    }

    /// Atomically grab `bytes` from the pool; false when the pool can't
    /// cover it (no partial state). CAS loop — contention is per blocking-
    /// operator allocation, not per row, so it's never hot.
    pub fn tryReserve(self: *MemoryPool, bytes: usize) bool {
        var cur = self.used.load(.monotonic);
        while (true) {
            if (bytes > self.budget - cur) return false;
            const new = cur + bytes;
            cur = self.used.cmpxchgWeak(cur, new, .monotonic, .monotonic) orelse {
                _ = self.charge_peak.fetchMax(new, .monotonic);
                return true;
            };
        }
    }

    /// The highest charge since the previous call, which starts the next
    /// interval at the current charge.
    pub fn takeChargePeak(self: *MemoryPool) usize {
        return self.charge_peak.swap(self.inUse(), .monotonic);
    }

    pub fn release(self: *MemoryPool, bytes: usize) void {
        const prev = self.used.fetchSub(bytes, .monotonic);
        std.debug.assert(prev >= bytes);
    }

    pub fn inUse(self: *const MemoryPool) usize {
        return self.used.load(.monotonic);
    }

    /// Hand the general allocator's free pages back to the system once the
    /// memory no budget accounts for has passed the watch threshold. Pages a
    /// finished statement freed otherwise stay resident inside the allocator
    /// until a later statement reuses them. Null when nothing was released.
    pub fn releaseFreeHeap(self: *MemoryPool) ?HeapRelease {
        const before = affinity.processResidentBytes() orelse return null;
        const snapshot: MemorySnapshot = .{ .resident = before, .cache = cacheBytes(), .retained = self.idleBytes(), .accounted = self.inUse() };
        if (snapshot.unaccounted() < watchThreshold(self.budget)) return null;
        if (!affinity.releaseFreeHeapPages()) return null;
        return .{ .before = before, .after = affinity.processResidentBytes() orelse before };
    }
};

/// Resident bytes around a `MemoryPool.releaseFreeHeap`.
pub const HeapRelease = struct {
    before: u64,
    after: u64,
};

/// When a server gives its general allocator's free pages back: once per idle
/// period, after a run of quiet checks. A check is quiet when the pool's
/// charge stayed under one sampling step since the check before. Re-faulting
/// released pages costs the next statement, so a release waits until large
/// statements have stopped; small ones, like a stream of replicated writes,
/// leave little behind and neither hold a release off nor start a new period.
pub const IdleRelease = struct {
    quiet_checks_needed: u32,
    quiet_checks: u32 = 0,
    released: bool = false,

    /// One check; true when the pool should release now.
    pub fn due(self: *IdleRelease, pool: *MemoryPool) bool {
        if (pool.takeChargePeak() >= watchStep(pool.budget)) {
            self.quiet_checks = 0;
            self.released = false;
            return false;
        }
        if (self.released) return false;
        self.quiet_checks += 1;
        if (self.quiet_checks < self.quiet_checks_needed) return false;
        self.released = true;
        return true;
    }
};

pub const MemoryAccountant = struct {
    budget: usize,
    current_bytes: usize = 0,
    /// Freed bytes still on their way back to the allocator (`DeferredFrees`).
    /// They no longer count against the budget, but the shared pool stays
    /// charged and a released owner stays alive until they are back.
    retired_bytes: usize = 0,
    /// Live bytes attributed to each `Source`, indexed by `@intFromEnum`.
    by_source: [source_count]usize = [_]usize{0} ** source_count,
    /// Shared cross-query pool this accountant draws from (null = per-query
    /// budget only). Every reserve must also fit the pool; every release
    /// hands the bytes back.
    pool: ?*MemoryPool = null,
    reservation_lock: std.atomic.Mutex = .unlocked,
    physical_tracking: bool = false,
    allocation_parent: ?std.mem.Allocator = null,
    owner_allocator: ?std.mem.Allocator = null,
    wrappers: std.ArrayListUnmanaged(*BudgetAllocator) = .empty,
    exceeded: std.atomic.Value(bool) = .init(false),
    peak_bytes: usize = 0,
    lifetime_lease: ?@import("util/statement_gate.zig").StatementGate.LifetimeLease = null,
    cancel_flag: ?*const std.atomic.Value(bool) = null,
    /// Names this statement in watchdog lines; unique among its pool's.
    statement_id: u64 = 0,
    /// The wire connection running the statement (the PROCESSLIST / KILL id).
    connection_id: ?u32 = null,
    /// Accounted level whose crossing takes the next watchdog sample.
    watch_next_bytes: usize = 0,
    watch_logged: std.atomic.Value(bool) = .init(false),
    /// Set by the first refused reservation's breakdown; guarded by
    /// `reservation_lock`.
    refusal_logged: bool = false,
    resident_peak: std.atomic.Value(u64) = .init(0),

    pub fn checkCancelled(self: *const MemoryAccountant) error{QueryCancelled}!void {
        if (self.cancel_flag) |flag| if (flag.load(.acquire)) return error.QueryCancelled;
    }

    pub fn trackAllocations(self: *MemoryAccountant, parent: std.mem.Allocator) void {
        self.physical_tracking = true;
        self.allocation_parent = parent;
    }

    pub fn retainGate(self: *MemoryAccountant, gate: ?*@import("util/statement_gate.zig").StatementGate) !void {
        if (gate) |g| self.lifetime_lease = try g.retainAllocator();
    }

    pub fn wrapAllocator(self: *MemoryAccountant, child: std.mem.Allocator) !std.mem.Allocator {
        if (!self.physical_tracking) return child;
        if (BudgetAllocator.accountantOf(child) == self) return child;
        self.lock();
        defer self.reservation_lock.unlock();
        for (self.wrappers.items) |wrapper| {
            if (wrapper.child.ptr == child.ptr and wrapper.child.vtable == child.vtable) return wrapper.allocator();
        }
        const parent = self.allocation_parent.?;
        const wrapper = try parent.create(BudgetAllocator);
        errdefer parent.destroy(wrapper);
        wrapper.* = BudgetAllocator.init(child);
        wrapper.active = self;
        wrapper.query_owned = true;
        try self.wrappers.append(parent, wrapper);
        return wrapper.allocator();
    }

    pub fn executionAllocator(self: *MemoryAccountant) !std.mem.Allocator {
        return self.wrapAllocator(self.allocation_parent.?);
    }

    fn lock(self: *MemoryAccountant) void {
        while (!self.reservation_lock.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn reserveAllocation(self: *MemoryAccountant, bytes: usize) Error!void {
        self.lock();
        const sample = self.reserveLocked(.execution, bytes) catch |err| {
            self.reservation_lock.unlock();
            self.exceeded.store(true, .release);
            self.balanceIdle();
            return err;
        };
        self.reservation_lock.unlock();
        if (sample) self.sampleGrowth();
    }

    /// The statement just grew by a sampling step: idle memory makes room for
    /// it before the watchdog reads the process.
    fn sampleGrowth(self: *MemoryAccountant) void {
        self.balanceIdle();
        self.watch("growth");
    }

    fn balanceIdle(self: *MemoryAccountant) void {
        if (self.pool) |p| p.yieldIdle();
    }

    pub fn releaseAllocation(self: *MemoryAccountant, bytes: usize) void {
        self.lock();
        self.releaseLocked(.execution, bytes);
        const destroy = self.current_bytes == 0 and self.retired_bytes == 0 and self.owner_allocator != null;
        self.reservation_lock.unlock();
        if (destroy) self.destroyReleasedOwner();
    }

    /// The allocation's charge ends now, while its memory is still held.
    pub fn retireAllocation(self: *MemoryAccountant, bytes: usize) void {
        self.lock();
        defer self.reservation_lock.unlock();
        std.debug.assert(self.current_bytes >= bytes);
        std.debug.assert(self.by_source[@intFromEnum(Source.execution)] >= bytes);
        self.current_bytes -= bytes;
        self.by_source[@intFromEnum(Source.execution)] -= bytes;
        self.retired_bytes += bytes;
        self.rearmWatch();
    }

    /// The next sample comes one step above the lowest level since the last
    /// one, so memory a statement gives up and grows back is sampled like
    /// its first growth. Without this a statement could fall far below its
    /// sampled level and mint that much again unseen. Caller holds
    /// `reservation_lock`.
    fn rearmWatch(self: *MemoryAccountant) void {
        self.watch_next_bytes = @min(self.watch_next_bytes, self.current_bytes +| watchStep(self.budget));
    }

    /// Retired memory is back with its allocator.
    pub fn finishRetired(self: *MemoryAccountant, bytes: usize) void {
        self.lock();
        std.debug.assert(self.retired_bytes >= bytes);
        self.retired_bytes -= bytes;
        if (self.pool) |p| p.release(bytes);
        const destroy = self.current_bytes == 0 and self.retired_bytes == 0 and self.owner_allocator != null;
        self.reservation_lock.unlock();
        if (destroy) self.destroyReleasedOwner();
    }

    pub fn releaseOwner(self: *MemoryAccountant, allocator: std.mem.Allocator) void {
        if (!self.physical_tracking) {
            const lease = self.lifetime_lease;
            self.drainToPool();
            allocator.destroy(self);
            if (lease) |l| l.release();
            return;
        }
        self.lock();
        std.debug.assert(self.owner_allocator == null);
        self.owner_allocator = allocator;
        const destroy = self.current_bytes == 0 and self.retired_bytes == 0;
        self.reservation_lock.unlock();
        if (destroy) self.destroyReleasedOwner();
    }

    fn destroyReleasedOwner(self: *MemoryAccountant) void {
        const lease = self.lifetime_lease;
        const parent = self.allocation_parent.?;
        for (self.wrappers.items) |wrapper| {
            std.debug.assert(wrapper.live_bytes.load(.monotonic) == 0);
            parent.destroy(wrapper);
        }
        self.wrappers.deinit(parent);
        self.owner_allocator.?.destroy(self);
        if (lease) |l| l.release();
    }

    pub fn init(budget: usize) MemoryAccountant {
        return .{ .budget = budget, .watch_next_bytes = watchStep(budget) };
    }

    /// Per-query accountant drawing from a shared pool. `budget` of 0 means
    /// "no per-query ceiling" (pool-constrained only). A statement starts
    /// with the pool in balance: idle memory left over the budget by
    /// statements too small to reach a sampling step is given back here.
    pub fn initWithPool(budget: usize, pool: ?*MemoryPool) MemoryAccountant {
        var account = init(if (budget == 0) std.math.maxInt(usize) else budget);
        account.pool = pool;
        if (pool) |p| {
            account.statement_id = p.statements.fetchAdd(1, .monotonic) + 1;
            p.yieldIdle();
        }
        return account;
    }

    /// Reserve `bytes` from the budget, attributing them to `source`.
    /// Returns `MemoryBudgetExceeded` when the reservation would exceed
    /// the per-query budget OR the shared pool; does NOT update state in
    /// that case (no partial state). The statement's first refusal dumps a
    /// per-source breakdown to stderr so the failure is auditable; the
    /// refusals that follow it (parallel workers each reaching the ceiling
    /// before the error unwinds) stay quiet.
    pub fn reserve(self: *MemoryAccountant, source: Source, bytes: usize) Error!void {
        if (self.physical_tracking) return;
        self.lock();
        const sample = self.reserveLocked(source, bytes) catch |err| {
            self.reservation_lock.unlock();
            self.balanceIdle();
            return err;
        };
        self.reservation_lock.unlock();
        if (sample) self.sampleGrowth();
    }

    /// True when the reservation crossed the next watchdog sampling level.
    fn reserveLocked(self: *MemoryAccountant, source: Source, bytes: usize) Error!bool {
        if (bytes > self.budget - self.current_bytes) {
            self.logRefusal(source, bytes, null);
            return Error.MemoryBudgetExceeded;
        }
        if (self.pool) |p| {
            if (!p.tryReserve(bytes)) {
                self.logRefusal(source, bytes, p);
                return Error.MemoryBudgetExceeded;
            }
        }
        self.current_bytes += bytes;
        self.peak_bytes = @max(self.peak_bytes, self.current_bytes);
        self.by_source[@intFromEnum(source)] += bytes;
        if (self.current_bytes < self.watch_next_bytes) return false;
        self.watch_next_bytes = self.current_bytes +| watchStep(self.budget);
        return true;
    }

    /// Compare process memory against everything accounted and log once per
    /// statement when the difference passes `watchThreshold` — a gap in the
    /// accounting shows up before it grows into an out-of-memory kill. Callers
    /// sample at stage boundaries and accounted growth steps, never per row.
    pub fn watch(self: *MemoryAccountant, site: []const u8) void {
        const resident = affinity.processResidentBytes() orelse return;
        _ = self.observe(self.reading(resident), site);
    }

    fn reading(self: *MemoryAccountant, resident: u64) MemorySnapshot {
        return .{
            .resident = resident,
            .cache = cacheBytes(),
            .retained = if (self.pool) |p| p.idleBytes() else buffer_pool.globalRetainedBytes(),
            .accounted = self.accountedEverywhere(),
        };
    }

    /// Final sample for a statement whose accounting ever reached a sampling
    /// step, plus its peak report under `--profile-ops`.
    pub fn finishStatement(self: *MemoryAccountant) void {
        self.lock();
        const peak = self.peak_bytes;
        self.reservation_lock.unlock();
        if (peak >= watchStep(self.budget)) self.watch("statement end");
        if (!prof.enabled) return;
        const mib = 1024 * 1024;
        std.debug.print("[mem] stmt={d} accounted_peak={d} MiB sampled_resident_peak={d} MiB\n", .{
            self.statement_id, peak / mib, self.resident_peak.load(.monotonic) / mib,
        });
    }

    fn accountedEverywhere(self: *MemoryAccountant) u64 {
        if (self.pool) |p| return p.inUse();
        self.lock();
        defer self.reservation_lock.unlock();
        return self.current_bytes + self.retired_bytes;
    }

    /// True when this reading produced the statement's watchdog line.
    fn observe(self: *MemoryAccountant, snapshot: MemorySnapshot, site: []const u8) bool {
        _ = self.resident_peak.fetchMax(snapshot.resident, .monotonic);
        const gap = snapshot.unaccounted();
        const threshold = watchThreshold(self.budget);
        if (gap < threshold) return false;
        if (self.watch_logged.swap(true, .monotonic)) return false;
        const mib = 1024 * 1024;
        std.debug.print(
            "[mem-watch] stmt={d} conn={?d}: {d} MiB of process memory is unaccounted (threshold {d} MiB) at {s}: resident {d} MiB, accounted {d} MiB, block cache {d} MiB, idle pools {d} MiB\n",
            .{
                self.statement_id,        self.connection_id,   gap / mib,
                threshold / mib,          site,                 snapshot.resident / mib,
                snapshot.accounted / mib, snapshot.cache / mib, snapshot.retained / mib,
            },
        );
        return true;
    }

    /// Release `bytes` previously reserved under `source`. Asserts the
    /// balance never goes negative — operators must call `release` exactly
    /// once per matching `reserve` on their deinit/eviction path.
    pub fn release(self: *MemoryAccountant, source: Source, bytes: usize) void {
        if (self.physical_tracking) return;
        self.lock();
        defer self.reservation_lock.unlock();
        self.releaseLocked(source, bytes);
    }

    fn releaseLocked(self: *MemoryAccountant, source: Source, bytes: usize) void {
        std.debug.assert(self.current_bytes >= bytes);
        std.debug.assert(self.by_source[@intFromEnum(source)] >= bytes);
        self.current_bytes -= bytes;
        self.by_source[@intFromEnum(source)] -= bytes;
        self.rearmWatch();
        if (self.pool) |p| p.release(bytes);
    }

    /// Hand every byte this accountant still holds back to the shared pool and
    /// zero its local counters. Backstop invoked once at query teardown: a
    /// blocking operator that releases only on eviction (e.g. a materialized
    /// CTE never fully drained because of a LIMIT) — or any operator unwinding
    /// an error mid-query — would otherwise leave its reservation stranded in
    /// the cross-query pool forever, eroding the budget until later queries
    /// spuriously fail `MemoryBudgetExceeded`. Idempotent.
    pub fn drainToPool(self: *MemoryAccountant) void {
        self.lock();
        defer self.reservation_lock.unlock();
        std.debug.assert(!self.physical_tracking or self.current_bytes == 0);
        if (self.pool) |p| p.release(self.current_bytes);
        self.current_bytes = 0;
        self.by_source = [_]usize{0} ** source_count;
    }

    /// Bytes still available for additional reservations.
    pub fn available(self: *MemoryAccountant) usize {
        self.lock();
        defer self.reservation_lock.unlock();
        return self.budget - self.current_bytes;
    }

    /// Bytes this statement can still reserve: what its own budget has left,
    /// capped by what the shared pool has free.
    pub fn headroom(self: *MemoryAccountant) usize {
        self.lock();
        defer self.reservation_lock.unlock();
        const own = self.budget - self.current_bytes;
        const pool = self.pool orelse return own;
        return @min(own, pool.budget -| pool.inUse());
    }

    fn logRefusal(self: *MemoryAccountant, failing: Source, want: usize, pool: ?*MemoryPool) void {
        if (self.refusal_logged) return;
        self.refusal_logged = true;
        const mib = 1024 * 1024;
        std.debug.print(
            "[mem-audit] MemoryBudgetExceeded: +{d} MiB for '{s}' would exceed budget {d} MiB (in use {d} MiB)\n",
            .{ want / mib, @tagName(failing), self.budget / mib, self.current_bytes / mib },
        );
        inline for (std.meta.fields(Source)) |f| {
            const v = self.by_source[@intFromEnum(@field(Source, f.name))];
            if (v > 0) std.debug.print("[mem-audit]   {s:<16} {d:>6} MiB\n", .{ f.name, v / mib });
        }
        if (pool) |p| std.debug.print(
            "[mem-audit]   shared pool: {d} MiB in use of {d} MiB (other queries hold the rest)\n",
            .{ p.inUse() / mib, p.budget / mib },
        );
    }
};

const types = @import("types.zig");

/// Approximate per-row bytes for a schema. Fixed-width types are exact.
/// Variable-width (string/varchar/char) uses a 32-byte conservative
/// estimate — actual size varies with data. Hash table and bucket
/// overhead are NOT included; callers add their own factor.
pub fn estimateRowBytes(schema: []const types.Column) usize {
    var total: usize = 0;
    for (schema) |col| {
        total += estimateColumnBytes(col.type);
        // Validity bit, rounded up to a full byte for simplicity.
        if (col.nullable) total += 1;
    }
    return total;
}

pub fn estimateColumnBytes(t: types.Type) usize {
    return switch (t) {
        .boolean, .tinyint => 1,
        .smallint => 2,
        .int, .date, .float => 4,
        .bigint, .datetime, .decimal64, .double => 8,
        .largeint, .decimal128, .uuid => 16,
        .varchar, .string, .char, .json => 32,
    };
}

test "memory: shared pool constrains accountants across queries" {
    var pool = MemoryPool.init(1000);
    var q1 = MemoryAccountant.initWithPool(0, &pool);
    var q2 = MemoryAccountant.initWithPool(0, &pool);
    // Each query alone is unconstrained (no per-query ceiling), but the
    // pool caps their SUM.
    try q1.reserve(.sort, 600);
    try std.testing.expectError(Error.MemoryBudgetExceeded, q2.reserve(.hash_aggregate, 600));
    try q2.reserve(.hash_aggregate, 400);
    try std.testing.expectEqual(@as(usize, 1000), pool.inUse());
    q1.release(.sort, 600);
    try std.testing.expectEqual(@as(usize, 400), pool.inUse());
    try q2.reserve(.hash_aggregate, 600);
    q2.release(.hash_aggregate, 1000);
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());
}

test "memory: headroom is the budget left, capped by the pool's free bytes" {
    var pool = MemoryPool.init(1000);
    var q1 = MemoryAccountant.initWithPool(700, &pool);
    var q2 = MemoryAccountant.initWithPool(0, &pool);
    try q1.reserve(.sort, 200);
    try std.testing.expectEqual(@as(usize, 500), q1.headroom());
    try q2.reserve(.hash_aggregate, 600);
    try std.testing.expectEqual(@as(usize, 200), q1.headroom());
    try std.testing.expectEqual(@as(usize, 200), q2.headroom());
    q1.release(.sort, 200);
    q2.release(.hash_aggregate, 600);
    var alone = MemoryAccountant.init(300);
    try std.testing.expectEqual(@as(usize, 300), alone.headroom());
}

test "memory: per-query ceiling trips before the pool and reserves nothing" {
    var pool = MemoryPool.init(1000);
    var q = MemoryAccountant.initWithPool(100, &pool);
    try std.testing.expectError(Error.MemoryBudgetExceeded, q.reserve(.sort, 200));
    // The failed per-query check must not leak a pool reservation.
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());
}

test "memory: reserve then release returns budget" {
    var a = MemoryAccountant.init(1024);
    try a.reserve(.sort, 512);
    try std.testing.expectEqual(@as(usize, 512), a.current_bytes);
    try std.testing.expectEqual(@as(usize, 512), a.available());
    a.release(.sort, 512);
    try std.testing.expectEqual(@as(usize, 0), a.current_bytes);
    try std.testing.expectEqual(@as(usize, 1024), a.available());
}

test "memory: reserve fails when budget would be exceeded" {
    var a = MemoryAccountant.init(1024);
    try a.reserve(.sort, 1024);
    try std.testing.expectError(Error.MemoryBudgetExceeded, a.reserve(.sort, 1));
    // The failed reservation does not consume any budget.
    try std.testing.expectEqual(@as(usize, 1024), a.current_bytes);
}

test "memory: drainToPool hands a stranded reservation back to the pool" {
    var pool = MemoryPool.init(1000);
    // Query 1 reserves but (simulating a non-evicted materialize / error
    // unwind) never calls release before the accountant is torn down.
    var q1 = MemoryAccountant.initWithPool(0, &pool);
    try q1.reserve(.materialize, 600);
    try std.testing.expectEqual(@as(usize, 600), pool.inUse());
    q1.drainToPool(); // teardown backstop
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());
    try std.testing.expectEqual(@as(usize, 0), q1.current_bytes);

    // Query 2 now sees the full pool again — no permanent erosion.
    var q2 = MemoryAccountant.initWithPool(0, &pool);
    try q2.reserve(.sort, 1000);
    q2.release(.sort, 1000);
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());
}

test "memory: multiple operators sharing one accountant" {
    var a = MemoryAccountant.init(1024);
    // Operator A (sort) reserves 400
    try a.reserve(.sort, 400);
    // Operator B (hash aggregate) reserves 500
    try a.reserve(.hash_aggregate, 500);
    try std.testing.expectEqual(@as(usize, 900), a.current_bytes);
    // Operator C would push us over
    try std.testing.expectError(Error.MemoryBudgetExceeded, a.reserve(.materialize, 200));
    // Operator A releases — now operator C can fit
    a.release(.sort, 400);
    try a.reserve(.materialize, 200);
    try std.testing.expectEqual(@as(usize, 700), a.current_bytes);
    // Per-source attribution is tracked independently.
    try std.testing.expectEqual(@as(usize, 500), a.by_source[@intFromEnum(Source.hash_aggregate)]);
    try std.testing.expectEqual(@as(usize, 200), a.by_source[@intFromEnum(Source.materialize)]);
}

test "memory: tracked allocations charge capacities and refund failed growth" {
    const a = std.testing.allocator;
    var pool = MemoryPool.init(1024);
    const account = try a.create(MemoryAccountant);
    account.* = MemoryAccountant.initWithPool(512, &pool);
    account.trackAllocations(a);
    defer account.releaseOwner(a);
    const alloc = try account.executionAllocator();
    const bytes = try alloc.alloc(u8, 400);
    defer alloc.free(bytes);
    try std.testing.expectEqual(@as(usize, 400), pool.inUse());
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 200));
    try std.testing.expectEqual(@as(usize, 400), pool.inUse());
    try std.testing.expectEqual(@as(usize, 400), account.current_bytes);
}

test "memory: retained allocator charges each borrower and retains no query pointer" {
    const a = std.testing.allocator;
    var retained = BudgetAllocator.init(a);
    const alloc = retained.allocator();
    const bytes = try alloc.alloc(u8, 300);
    defer alloc.free(bytes);
    var first = MemoryAccountant.initWithPool(400, null);
    var second = MemoryAccountant.initWithPool(200, null);
    try retained.attach(&first);
    try std.testing.expectEqual(@as(usize, 300), first.current_bytes);
    retained.detach();
    try std.testing.expectEqual(@as(usize, 0), first.current_bytes);
    try std.testing.expectError(error.MemoryBudgetExceeded, retained.attach(&second));
    try std.testing.expect(retained.active == null);
    try std.testing.expectEqual(@as(usize, 0), second.current_bytes);
}

test "memory: a retained allocator has no owning query and backs its own worker buffers" {
    const a = std.testing.allocator;
    const account = try a.create(MemoryAccountant);
    account.* = MemoryAccountant.init(1 << 20);
    account.trackAllocations(a);
    defer account.releaseOwner(a);
    const owned = try account.executionAllocator();
    try std.testing.expectEqual(@as(?*MemoryAccountant, account), ownerOf(owned));
    try std.testing.expect(!BudgetAllocator.isRetained(owned));
    try std.testing.expect(!BudgetAllocator.isRetained(a));

    var retained = BudgetAllocator.init(a);
    const alloc = retained.allocator();
    try std.testing.expect(BudgetAllocator.isRetained(alloc));
    try retained.attach(account);
    try std.testing.expectEqual(@as(?*MemoryAccountant, account), accountantOf(alloc));
    try std.testing.expectEqual(@as(?*MemoryAccountant, null), ownerOf(alloc));

    // The pool is not the fallback in production; tests see the fallback.
    var pool_buffer: [256]u8 = undefined;
    var pool = std.heap.FixedBufferAllocator.init(&pool_buffer);
    const backing = try workerBackingOf(pool.allocator(), alloc);
    try std.testing.expectEqual(alloc.ptr, backing.ptr);
    try std.testing.expectEqual(alloc.vtable, backing.vtable);
    const bytes = try backing.alloc(u8, 64);
    retained.detach();
    try std.testing.expectEqual(@as(usize, 0), account.current_bytes);
    backing.free(bytes);

    const pooled = try workerBackingOf(pool.allocator(), owned);
    try std.testing.expectEqual(@as(?*MemoryAccountant, account), ownerOf(pooled));
    try std.testing.expect(pooled.ptr != owned.ptr);
}

test "memory: retiring an owner keeps its allocator alive through the last free" {
    const a = std.testing.allocator;
    var pool = MemoryPool.init(1024);
    const account = try a.create(MemoryAccountant);
    account.* = MemoryAccountant.initWithPool(512, &pool);
    account.trackAllocations(a);
    const alloc = try account.executionAllocator();
    const bytes = try alloc.alloc(u8, 400);
    account.releaseOwner(a);
    try std.testing.expectEqual(@as(usize, 400), pool.inUse());
    alloc.free(bytes);
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());
}

test "memory: collected frees stop counting against the budget at once and leave the pool on release" {
    const a = std.testing.allocator;
    var pool = MemoryPool.init(1 << 20);
    const account = try a.create(MemoryAccountant);
    account.* = MemoryAccountant.initWithPool(1 << 20, &pool);
    account.trackAllocations(a);
    defer account.releaseOwner(a);
    const alloc = try account.executionAllocator();
    const large = try alloc.alloc(u64, 1000);
    const elsewhere = try alloc.alloc(u64, 1000);
    const untracked = try a.alloc(u64, 1000);
    const Other = struct {
        fn free(allocator: std.mem.Allocator, bytes: []u64) void {
            allocator.free(bytes);
        }
    };

    var frees: DeferredFrees = .{};
    frees.collect();
    const other = try std.Thread.spawn(.{}, Other.free, .{ alloc, elsewhere });
    other.join();
    a.free(untracked);
    try std.testing.expect(frees.isEmpty());
    alloc.free(large);
    frees.stop();
    try std.testing.expect(!frees.isEmpty());
    try std.testing.expectEqual(@as(usize, 0), account.current_bytes);
    try std.testing.expectEqual(large.len * @sizeOf(u64), pool.inUse());
    const background = try std.Thread.spawn(.{}, DeferredFrees.release, .{frees});
    background.join();
    try std.testing.expectEqual(@as(usize, 0), account.retired_bytes);
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());
}

test "memory: memory returned on detached threads keeps its released owner and gate lease until it is back" {
    const a = std.testing.allocator;
    var gate = @import("util/statement_gate.zig").StatementGate.init(a, std.testing.io);
    defer gate.deinit();
    var pool = MemoryPool.init(1 << 20);
    const account = try a.create(MemoryAccountant);
    account.* = MemoryAccountant.initWithPool(1 << 20, &pool);
    account.trackAllocations(a);
    try account.retainGate(&gate);
    const alloc = try account.executionAllocator();
    var blocks: [8][]u64 = undefined;
    for (&blocks, 0..) |*block, i| block.* = try alloc.alloc(u64, 500 * (i + 1));

    var frees = DeferredFrees.init(3);
    frees.collect();
    for (blocks) |block| alloc.free(block);
    frees.stop();
    try std.testing.expectEqual(@as(usize, 0), account.current_bytes);
    try std.testing.expectEqual(@as(usize, 36 * 500 * @sizeOf(u64)), pool.inUse());
    account.releaseOwner(a);
    frees.releaseDetached();
    gate.beginClose();
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());
}

test "memory: worker allocator charges the query once, even over an already-tracked fallback" {
    const a = std.testing.allocator;
    var pool = MemoryPool.init(1024);
    const account = try a.create(MemoryAccountant);
    account.* = MemoryAccountant.initWithPool(512, &pool);
    account.trackAllocations(a);
    defer account.releaseOwner(a);
    const worker = try workerAllocator(account, a);
    const bytes = try worker.alloc(u8, 300);
    try std.testing.expectEqual(@as(usize, 300), account.current_bytes);
    const over_tracked = try workerAllocator(account, try account.executionAllocator());
    const more = try over_tracked.alloc(u8, 100);
    try std.testing.expectEqual(@as(usize, 400), account.current_bytes);
    try std.testing.expectError(error.OutOfMemory, worker.alloc(u8, 200));
    try std.testing.expectEqual(error.MemoryBudgetExceeded, allocationError(account, error.OutOfMemory));
    over_tracked.free(more);
    worker.free(bytes);
    try std.testing.expectEqual(@as(usize, 0), account.current_bytes);
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());
    try std.testing.expectEqual(a, try workerAllocator(null, a));
}

test "memory: a pooled block is charged at its size class, not the request" {
    const a = std.testing.allocator;
    var scratch = buffer_pool.Pool.init(a, 1 << 20);
    defer scratch.drain();
    var pool = MemoryPool.init(1 << 20);
    const account = try a.create(MemoryAccountant);
    account.* = MemoryAccountant.initWithPool(1 << 20, &pool);
    account.trackAllocations(a);
    defer account.releaseOwner(a);
    const pooled = try account.wrapAllocator(scratch.allocator());
    var bytes = try pooled.alloc(u8, 100 * 1024);
    try std.testing.expectEqual(@as(usize, 128 * 1024), account.current_bytes);
    bytes = try pooled.realloc(bytes, 300 * 1024);
    try std.testing.expectEqual(@as(usize, 512 * 1024), account.current_bytes);
    pooled.free(bytes);
    try std.testing.expectEqual(@as(usize, 0), account.current_bytes);
}

test "memory: watchdog logs one line per statement once unaccounted memory passes the threshold" {
    const gib: u64 = 1 << 30;
    var pool = MemoryPool.init(64 * gib);
    var account = MemoryAccountant.initWithPool(4 * gib, &pool);
    try std.testing.expectEqual(2 * gib, watchThreshold(account.budget));
    const within: MemorySnapshot = .{ .resident = 10 * gib, .cache = 4 * gib, .retained = gib, .accounted = 4 * gib };
    try std.testing.expectEqual(gib, within.unaccounted());
    try std.testing.expect(!account.observe(within, "test"));
    const beyond: MemorySnapshot = .{ .resident = 12 * gib, .cache = 4 * gib, .retained = gib, .accounted = 4 * gib };
    try std.testing.expect(account.observe(beyond, "test"));
    try std.testing.expect(!account.observe(beyond, "test"));
    try std.testing.expectEqual(12 * gib, account.resident_peak.load(.monotonic));
    const cache_only: MemorySnapshot = .{ .resident = gib, .cache = 3 * gib, .retained = 0, .accounted = 0 };
    try std.testing.expectEqual(@as(u64, 0), cache_only.unaccounted());
}

const TestIdle = struct {
    source: IdleSource = .{ .idle_bytes_fn = idleBytes, .reclaim_fn = reclaim },
    idle: usize,
    reclaims: usize = 0,

    fn idleBytes(source: *IdleSource) usize {
        const self: *TestIdle = @fieldParentPtr("source", source);
        return self.idle;
    }

    fn reclaim(source: *IdleSource, want: usize) usize {
        const self: *TestIdle = @fieldParentPtr("source", source);
        const given = @min(want, self.idle);
        self.idle -= given;
        self.reclaims += 1;
        return given;
    }
};

test "memory: a statement that fits the budget only once idle memory is given back succeeds" {
    const gib: usize = 1 << 30;
    var pool = MemoryPool.init(8 * gib);
    var warm = TestIdle{ .idle = 6 * gib };
    pool.registerIdle(&warm.source);
    defer pool.unregisterIdle(&warm.source);
    var statement = MemoryAccountant.initWithPool(8 * gib, &pool);
    try std.testing.expectEqual(gib / 2, idleSlack(pool.budget));

    try statement.reserve(.sort, 2 * gib);
    try std.testing.expectEqual(6 * gib, warm.idle);
    try std.testing.expectEqual(@as(usize, 0), warm.reclaims);

    try statement.reserve(.sort, gib);
    try std.testing.expectEqual(4 * gib + gib / 2, warm.idle);
    for (0..2) |_| {
        try statement.reserve(.sort, gib);
        try std.testing.expect(pool.inUse() + pool.idleBytes() <= pool.budget);
    }
    try std.testing.expectEqual(5 * gib, statement.current_bytes);
    try std.testing.expectEqual(2 * gib + gib / 2, warm.idle);
    try std.testing.expectEqual(@as(usize, 3), warm.reclaims);
    statement.release(.sort, 5 * gib);
}

test "memory: a statement that does not fit the budget is refused, and idle memory gives way first" {
    const gib: usize = 1 << 30;
    const mib: usize = 1 << 20;
    var pool = MemoryPool.init(8 * gib);
    var warm = TestIdle{ .idle = 6 * gib };
    pool.registerIdle(&warm.source);
    defer pool.unregisterIdle(&warm.source);
    var small: [3]MemoryAccountant = undefined;
    for (&small) |*s| {
        s.* = MemoryAccountant.initWithPool(0, &pool);
        try s.reserve(.sort, 900 * mib);
    }
    try std.testing.expectEqual(6 * gib, warm.idle);
    try std.testing.expect(pool.inUse() + pool.idleBytes() > pool.budget);

    try std.testing.expectError(Error.MemoryBudgetExceeded, small[0].reserve(.sort, 6 * gib));
    try std.testing.expectEqual(900 * mib, small[0].current_bytes);
    try std.testing.expectEqual(2700 * mib, pool.inUse());
    try std.testing.expectEqual(8 * gib - 2700 * mib - gib / 2, warm.idle);
    try std.testing.expectEqual(@as(usize, 1), warm.reclaims);
    for (&small) |*s| s.release(.sort, 900 * mib);
}

test "memory: a new statement starts with live plus idle bytes inside the budget" {
    const gib: usize = 1 << 30;
    var pool = MemoryPool.init(8 * gib);
    var older = TestIdle{ .idle = 3 * gib };
    var newer = TestIdle{ .idle = 7 * gib };
    pool.registerIdle(&older.source);
    pool.registerIdle(&newer.source);
    try std.testing.expectEqual(10 * gib, pool.idleBytes());

    _ = MemoryAccountant.initWithPool(0, &pool);
    try std.testing.expectEqual(8 * gib - gib / 2, pool.idleBytes());
    try std.testing.expectEqual(@as(usize, 1), older.reclaims + newer.reclaims);

    _ = MemoryAccountant.initWithPool(0, &pool);
    try std.testing.expectEqual(@as(usize, 1), older.reclaims + newer.reclaims);

    pool.unregisterIdle(&newer.source);
    try std.testing.expectEqual(older.idle, pool.idleBytes());
    pool.unregisterIdle(&older.source);
    try std.testing.expectEqual(@as(usize, 0), pool.idleBytes());
    older.idle = 20 * gib;
    pool.yieldIdle();
    try std.testing.expectEqual(20 * gib, older.idle);
}

test "memory: two running statements keep live plus idle bytes within the budget plus one sampling step each" {
    const gib: usize = 1 << 30;
    const mib: usize = 1 << 20;
    var pool = MemoryPool.init(8 * gib);
    var warm = TestIdle{ .idle = 8 * gib };
    pool.registerIdle(&warm.source);
    defer pool.unregisterIdle(&warm.source);
    var first = MemoryAccountant.initWithPool(4 * gib, &pool);
    var second = MemoryAccountant.initWithPool(4 * gib, &pool);
    const step = watchStep(first.budget);
    try std.testing.expectEqual(256 * mib, step);

    try first.reserve(.sort, step - mib);
    try second.reserve(.sort, step - mib);
    try std.testing.expectEqual(@as(usize, 0), warm.reclaims);
    try std.testing.expectEqual(pool.budget + 2 * step - 2 * mib, pool.inUse() + pool.idleBytes());

    try first.reserve(.sort, mib);
    try std.testing.expectEqual(@as(usize, 1), warm.reclaims);
    try std.testing.expect(pool.inUse() + pool.idleBytes() <= pool.budget);

    // The statements now grow in turn, in pieces that line up with no
    // sampling level. Every eleventh turn one of them frees most of what it
    // holds into the warm pool and then grows back with fresh memory.
    const statements = [2]*MemoryAccountant{ &first, &second };
    var held = [2]usize{ step, step - mib };
    var above_budget: usize = 0;
    for (0..600) |turn| {
        const s = turn % 2;
        const piece = (37 + 61 * (turn % 7)) * mib;
        if (turn % 11 == 10) {
            const freed = held[s] - held[s] / 4;
            statements[s].release(.sort, freed);
            held[s] -= freed;
            warm.idle += freed;
        } else if (held[s] + piece <= 3 * gib) {
            try statements[s].reserve(.sort, piece);
            held[s] += piece;
        }
        const sum = pool.inUse() + pool.idleBytes();
        try std.testing.expect(sum < pool.budget + 2 * step);
        if (sum > pool.budget) above_budget += 1;
    }
    try std.testing.expect(above_budget > 0);
    try std.testing.expect(warm.reclaims > 2);
    first.release(.sort, held[0]);
    second.release(.sort, held[1]);
}

test "memory: an unlimited pool never asks for idle memory back" {
    const gib: usize = 1 << 30;
    var pool = MemoryPool.init(std.math.maxInt(usize));
    var warm = TestIdle{ .idle = 6 * gib };
    pool.registerIdle(&warm.source);
    defer pool.unregisterIdle(&warm.source);
    var statement = MemoryAccountant.initWithPool(0, &pool);
    try statement.reserve(.sort, 4 * gib);
    statement.release(.sort, 4 * gib);
    try std.testing.expectEqual(@as(usize, 0), warm.reclaims);
}

test "memory: tracked allocations past the budget fail as MemoryBudgetExceeded after idle memory gave way" {
    const a = std.testing.allocator;
    const mib: usize = 1 << 20;
    var pool = MemoryPool.init(128 * mib);
    var warm = TestIdle{ .idle = 100 * mib };
    pool.registerIdle(&warm.source);
    defer pool.unregisterIdle(&warm.source);
    const account = try a.create(MemoryAccountant);
    account.* = MemoryAccountant.initWithPool(128 * mib, &pool);
    account.trackAllocations(a);
    defer account.releaseOwner(a);
    const alloc = try account.executionAllocator();
    const fits = try alloc.alloc(u8, 70 * mib);
    defer alloc.free(fits);
    try std.testing.expectEqual(@as(usize, 0), warm.idle);
    try std.testing.expect(pool.inUse() + pool.idleBytes() <= pool.budget);
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 70 * mib));
    try std.testing.expectEqual(error.MemoryBudgetExceeded, allocationError(account, error.OutOfMemory));
    try std.testing.expectEqual(70 * mib, pool.inUse());
}

test "memory: the watchdog counts every registered idle source" {
    const gib: u64 = 1 << 30;
    var pool = MemoryPool.init(64 * gib);
    var region_cache = TestIdle{ .idle = 5 * gib };
    pool.registerIdle(&region_cache.source);
    defer pool.unregisterIdle(&region_cache.source);
    var account = MemoryAccountant.initWithPool(4 * gib, &pool);
    try account.reserve(.sort, gib);
    defer account.release(.sort, gib);
    const sample = account.reading(20 * gib);
    try std.testing.expectEqual(5 * gib, sample.retained);
    try std.testing.expectEqual(gib, sample.accounted);
}

test "memory: watchdog threshold is 2 GiB or a quarter of the budget" {
    const gib: usize = 1 << 30;
    try std.testing.expectEqual(2 * gib, watchThreshold(512 << 20));
    try std.testing.expectEqual(4 * gib, watchThreshold(16 * gib));
    try std.testing.expectEqual(2 * gib, watchThreshold(std.math.maxInt(usize)));
}

test "memory: accounted growth schedules watchdog samples a step apart" {
    const mib: usize = 1 << 20;
    var account = MemoryAccountant.init(1024 * mib);
    try std.testing.expectEqual(64 * mib, account.watch_next_bytes);
    try account.reserve(.sort, 32 * mib);
    try std.testing.expectEqual(64 * mib, account.watch_next_bytes);
    try account.reserve(.sort, 40 * mib);
    try std.testing.expectEqual(136 * mib, account.watch_next_bytes);
    account.release(.sort, 72 * mib);
}

test "memory: statements drawing from one pool get distinct ids" {
    var pool = MemoryPool.init(1024);
    const first = MemoryAccountant.initWithPool(0, &pool);
    const second = MemoryAccountant.initWithPool(0, &pool);
    try std.testing.expectEqual(@as(u64, 1), first.statement_id);
    try std.testing.expectEqual(@as(u64, 2), second.statement_id);
}

test "memory: an idle release waits for quiet checks and fires once per idle period" {
    var pool = MemoryPool.init(1 << 30);
    const step = watchStep(pool.budget);
    var idle: IdleRelease = .{ .quiet_checks_needed = 2 };
    try std.testing.expect(!idle.due(&pool));
    try std.testing.expect(idle.due(&pool));
    try std.testing.expect(!idle.due(&pool));
    // A charge of a sampling step since the last check begins a new idle
    // period, even after it was released.
    try std.testing.expect(pool.tryReserve(step));
    pool.release(step);
    try std.testing.expect(!idle.due(&pool));
    try std.testing.expect(!idle.due(&pool));
    try std.testing.expect(idle.due(&pool));
    // One still held holds the release off until the check after it ends.
    try std.testing.expect(pool.tryReserve(step));
    try std.testing.expect(!idle.due(&pool));
    pool.release(step);
    try std.testing.expect(!idle.due(&pool));
    try std.testing.expect(!idle.due(&pool));
    try std.testing.expect(idle.due(&pool));
    // A smaller charge starts no new period.
    try std.testing.expect(pool.tryReserve(step - 1));
    pool.release(step - 1);
    try std.testing.expect(!idle.due(&pool));
}

test "memory: shared reservation rejects integer overflow" {
    var pool = MemoryPool.init(std.math.maxInt(usize));
    try std.testing.expect(pool.tryReserve(std.math.maxInt(usize) - 1));
    try std.testing.expect(!pool.tryReserve(8));
    pool.release(std.math.maxInt(usize) - 1);
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());
}
