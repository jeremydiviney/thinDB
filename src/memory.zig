//! Memory accounting for blocking operators.
//!
//! Some operators (Sort, hash GroupBy, hash Join build, SMJ sort buffers,
//! materialize-with-stats) accumulate input rows in memory before
//! emitting output. Without a budget, a query that hits an unexpectedly
//! large input can OOM-kill the process. With a budget, those operators
//! check before each allocation; if the budget would be exceeded, the
//! query fails fast with `MemoryBudgetExceeded` instead.
//!
//! Scope: per-query. On the SQL compile path (`net/local.zig`'s
//! `CompileCtx`) one accountant is owned by the query root and injected
//! into every Scan, so the budget is a true whole-query limit shared
//! across all operators, materialized buffers, and subquery drains.
//! Blocking operators reserve as they accumulate and `release()` (and free
//! the backing memory) the moment their result is no longer a downstream
//! dependency — so `current_bytes` tracks the live concurrent peak, not
//! the sum of sequential phases. Spilling to disk is a future enhancement;
//! today an over-budget query simply fails with `MemoryBudgetExceeded`.
//!
//! Two layers:
//!   - `MemoryPool` — process-shared (Catalog-owned), thread-safe. One
//!     budget all queries draw from, so concurrent queries can't sum past
//!     the box even when each is individually under its per-query ceiling.
//!   - `MemoryAccountant` — per-query, single-threaded by construction
//!     (our pull-based execution drives one operator at a time within a
//!     query; parallel-scan workers never touch it). Carries the per-query
//!     ceiling and the per-source attribution, and forwards every
//!     reserve/release to the pool when one is attached.

const std = @import("std");

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
};

const source_count = std.meta.fields(Source).len;

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
            const new = cur + bytes;
            if (new > self.budget) return false;
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
        const new_total = self.current_bytes + bytes;
        if (new_total > self.budget) {
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
        self.current_bytes = new_total;
        self.by_source[@intFromEnum(source)] += bytes;
    }

    /// Release `bytes` previously reserved under `source`. Asserts the
    /// balance never goes negative — operators must call `release` exactly
    /// once per matching `reserve` on their deinit/eviction path.
    pub fn release(self: *MemoryAccountant, source: Source, bytes: usize) void {
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
        if (self.pool) |p| p.release(self.current_bytes);
        self.current_bytes = 0;
        self.by_source = [_]usize{0} ** source_count;
    }

    /// Bytes still available for additional reservations.
    pub fn available(self: MemoryAccountant) usize {
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
        .varchar, .string, .char => 32,
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
