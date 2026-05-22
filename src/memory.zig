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
//! Future: a process-global memory pool. Each query would acquire its
//! per-query budget from the pool at admission (today the grant == the
//! configured per-query max, always granted) and not start until the
//! reservation is available. The single seam for that is where the query
//! root creates the accountant (acquire) and frees it (release) — keep
//! budget acquisition centralized there rather than scattered.
//!
//! Threading: single-threaded by construction (our pull-based execution
//! drives one operator at a time within a query). No mutex.

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

pub const MemoryAccountant = struct {
    budget: usize,
    current_bytes: usize = 0,
    /// Live bytes attributed to each `Source`, indexed by `@intFromEnum`.
    by_source: [source_count]usize = [_]usize{0} ** source_count,

    pub fn init(budget: usize) MemoryAccountant {
        return .{ .budget = budget };
    }

    /// Reserve `bytes` from the budget, attributing them to `source`.
    /// Returns `MemoryBudgetExceeded` when the reservation would exceed
    /// the budget; does NOT update state in that case (no partial state),
    /// but dumps a per-source breakdown to stderr first so the failure is
    /// auditable. A failed reservation propagates terminally (no operator
    /// retries it), so this fires at most once per over-budget query.
    pub fn reserve(self: *MemoryAccountant, source: Source, bytes: usize) Error!void {
        const new_total = self.current_bytes + bytes;
        if (new_total > self.budget) {
            self.dumpBreakdown(source, bytes);
            return Error.MemoryBudgetExceeded;
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
