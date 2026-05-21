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

pub const MemoryAccountant = struct {
    budget: usize,
    current_bytes: usize = 0,

    pub fn init(budget: usize) MemoryAccountant {
        return .{ .budget = budget };
    }

    /// Reserve `bytes` from the budget. Returns `MemoryBudgetExceeded`
    /// when the reservation would exceed the budget; does NOT update
    /// `current_bytes` in that case (no partial state).
    pub fn reserve(self: *MemoryAccountant, bytes: usize) Error!void {
        const new_total = self.current_bytes + bytes;
        if (new_total > self.budget) return Error.MemoryBudgetExceeded;
        self.current_bytes = new_total;
    }

    /// Release `bytes` previously reserved. Asserts the balance never
    /// goes negative — operators must call `release` exactly once per
    /// matching `reserve` on their deinit path.
    pub fn release(self: *MemoryAccountant, bytes: usize) void {
        std.debug.assert(self.current_bytes >= bytes);
        self.current_bytes -= bytes;
    }

    /// Bytes still available for additional reservations.
    pub fn available(self: MemoryAccountant) usize {
        return self.budget - self.current_bytes;
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
    try a.reserve(512);
    try std.testing.expectEqual(@as(usize, 512), a.current_bytes);
    try std.testing.expectEqual(@as(usize, 512), a.available());
    a.release(512);
    try std.testing.expectEqual(@as(usize, 0), a.current_bytes);
    try std.testing.expectEqual(@as(usize, 1024), a.available());
}

test "memory: reserve fails when budget would be exceeded" {
    var a = MemoryAccountant.init(1024);
    try a.reserve(1024);
    try std.testing.expectError(Error.MemoryBudgetExceeded, a.reserve(1));
    // The failed reservation does not consume any budget.
    try std.testing.expectEqual(@as(usize, 1024), a.current_bytes);
}

test "memory: multiple operators sharing one accountant" {
    var a = MemoryAccountant.init(1024);
    // Operator A reserves 400
    try a.reserve(400);
    // Operator B reserves 500
    try a.reserve(500);
    try std.testing.expectEqual(@as(usize, 900), a.current_bytes);
    // Operator C would push us over
    try std.testing.expectError(Error.MemoryBudgetExceeded, a.reserve(200));
    // Operator A releases — now operator C can fit
    a.release(400);
    try a.reserve(200);
    try std.testing.expectEqual(@as(usize, 700), a.current_bytes);
}
