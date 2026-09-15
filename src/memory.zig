//! Per-query execution allocation accounting and cooperative cancellation.
//!
//! Physical operators and worker allocators share one thread-safe ledger. It
//! charges live requested capacities to per-query/shared limits; planner row
//! estimates do not enforce those limits. Output stays charged until released.
//! Retained pool storage attaches to the current borrower and has a separate
//! retention cap while idle. Allocator metadata, internal rounding/freelists,
//! database metadata, parser/wire buffers and source cache are separate costs.
//!
//! Query ownership can retire before asynchronous frees finish. The ledger and
//! its allocator wrappers survive until the final tracked allocation is freed.

const std = @import("std");
pub const BudgetAllocator = @import("util/budget_allocator.zig").BudgetAllocator;

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

pub fn allocationError(accountant: ?*MemoryAccountant, err: anytype) (@TypeOf(err) || Error) {
    if (err == error.OutOfMemory) if (accountant) |a| {
        if (a.exceeded.load(.acquire)) return error.MemoryBudgetExceeded;
    };
    return err;
}

/// Process-shared memory pool: one budget every query's accountant draws
/// from, so CONCURRENT queries can't sum past the box even when each is
/// individually under its per-query ceiling. Owned by the Catalog (one per
/// server process / embedded Catalog); thread-safe — queries reserve from
/// their own connection threads.
pub const MemoryPool = struct {
    budget: usize,
    used: std.atomic.Value(usize) = .init(0),

    pub fn init(budget: usize) MemoryPool {
        return .{ .budget = budget };
    }

    /// Atomically grab `bytes` from the pool; false when the pool can't
    /// cover it (no partial state). CAS loop — contention is per blocking-
    /// operator allocation, not per row, so it's never hot.
    pub fn tryReserve(self: *MemoryPool, bytes: usize) bool {
        var cur = self.used.load(.monotonic);
        while (true) {
            if (bytes > self.budget - cur) return false;
            const new = cur + bytes;
            cur = self.used.cmpxchgWeak(cur, new, .monotonic, .monotonic) orelse return true;
        }
    }

    pub fn release(self: *MemoryPool, bytes: usize) void {
        const prev = self.used.fetchSub(bytes, .monotonic);
        std.debug.assert(prev >= bytes);
    }

    pub fn inUse(self: *const MemoryPool) usize {
        return self.used.load(.monotonic);
    }
};

pub const MemoryAccountant = struct {
    budget: usize,
    current_bytes: usize = 0,
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
        defer self.reservation_lock.unlock();
        self.reserveLocked(.execution, bytes) catch |err| {
            self.exceeded.store(true, .release);
            return err;
        };
    }

    pub fn releaseAllocation(self: *MemoryAccountant, bytes: usize) void {
        self.lock();
        self.releaseLocked(.execution, bytes);
        const destroy = self.current_bytes == 0 and self.owner_allocator != null;
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
        const destroy = self.current_bytes == 0;
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
        return .{ .budget = budget };
    }

    /// Per-query accountant drawing from a shared pool. `budget` of 0 means
    /// "no per-query ceiling" (pool-constrained only).
    pub fn initWithPool(budget: usize, pool: ?*MemoryPool) MemoryAccountant {
        return .{ .budget = if (budget == 0) std.math.maxInt(usize) else budget, .pool = pool };
    }

    /// Reserve `bytes` from the budget, attributing them to `source`.
    /// Returns `MemoryBudgetExceeded` when the reservation would exceed
    /// the per-query budget OR the shared pool; does NOT update state in
    /// that case (no partial state), but dumps a per-source breakdown to
    /// stderr first so the failure is auditable. A failed reservation
    /// propagates terminally (no operator retries it), so this fires at
    /// most once per over-budget query.
    pub fn reserve(self: *MemoryAccountant, source: Source, bytes: usize) Error!void {
        if (self.physical_tracking) return;
        self.lock();
        defer self.reservation_lock.unlock();
        return self.reserveLocked(source, bytes);
    }

    fn reserveLocked(self: *MemoryAccountant, source: Source, bytes: usize) Error!void {
        if (bytes > self.budget - self.current_bytes) {
            self.dumpBreakdown(source, bytes);
            return Error.MemoryBudgetExceeded;
        }
        if (self.pool) |p| {
            if (!p.tryReserve(bytes)) {
                self.dumpBreakdown(source, bytes);
                std.debug.print(
                    "[mem-audit]   shared pool: {d} MiB in use of {d} MiB (other queries hold the rest)\n",
                    .{ p.inUse() / (1024 * 1024), p.budget / (1024 * 1024) },
                );
                return Error.MemoryBudgetExceeded;
            }
        }
        self.current_bytes += bytes;
        self.peak_bytes = @max(self.peak_bytes, self.current_bytes);
        self.by_source[@intFromEnum(source)] += bytes;
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

    fn dumpBreakdown(self: *const MemoryAccountant, failing: Source, want: usize) void {
        const mib = 1024 * 1024;
        std.debug.print(
            "[mem-audit] MemoryBudgetExceeded: +{d} MiB for '{s}' would exceed budget {d} MiB (in use {d} MiB)\n",
            .{ want / mib, @tagName(failing), self.budget / mib, self.current_bytes / mib },
        );
        inline for (std.meta.fields(Source)) |f| {
            const v = self.by_source[@intFromEnum(@field(Source, f.name))];
            if (v > 0) std.debug.print("[mem-audit]   {s:<16} {d:>6} MiB\n", .{ f.name, v / mib });
        }
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

test "memory: shared reservation rejects integer overflow" {
    var pool = MemoryPool.init(std.math.maxInt(usize));
    try std.testing.expect(pool.tryReserve(std.math.maxInt(usize) - 1));
    try std.testing.expect(!pool.tryReserve(8));
    pool.release(std.math.maxInt(usize) - 1);
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());
}
