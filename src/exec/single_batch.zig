//! Single-Batch source operator — yields one pre-built Batch on the
//! first `next()` call, then `null`. Used by the streaming UPDATE
//! engine method to feed in-memory batches (decoded segment row
//! groups, memtable slices) through the standard Filter / Compute
//! pipeline without going through a real Scan.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

pub const SingleBatchSource = struct {
    allocator: Allocator,
    /// The batch handed in at create-time. Borrowed — caller owns the
    /// schema/views memory and keeps it alive until this source is
    /// `deinit`'d.
    batch: Batch,
    emitted: bool,

    pub fn create(allocator: Allocator, batch: Batch) !Query {
        const self = try allocator.create(SingleBatchSource);
        errdefer allocator.destroy(self);
        self.* = .{ .allocator = allocator, .batch = batch, .emitted = false };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *SingleBatchSource) void {
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *SingleBatchSource) []const Column {
        return self.batch.schema;
    }

    pub fn addPrune(self: *SingleBatchSource, pred: Predicate) !void {
        // No segments to prune — single-batch source has nothing to
        // skip. Silently accept hints from downstream operators.
        _ = self;
        _ = pred;
    }

    pub fn stats(self: *SingleBatchSource) exec.PipelineStats {
        return .{
            .upper_rows = self.batch.row_count,
            .sort_state = .{ .keys = &.{}, .global = false },
        };
    }

    pub fn accountant(self: *SingleBatchSource) ?*exec.memory.MemoryAccountant {
        _ = self;
        return null;
    }

    pub fn next(self: *SingleBatchSource) !?Batch {
        if (self.emitted) return null;
        self.emitted = true;
        return self.batch;
    }
};
