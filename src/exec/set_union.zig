//! UNION ALL — concatenate the row streams from two upstream Queries.
//!
//! v1 supports UNION ALL only. UNION (distinct) would land as a hash-
//! dedup operator on top of this; deferred.
//!
//! Schema unification: both sides must declare the same number of
//! columns with matching type tags at each position. Column *names*
//! may differ — the output adopts the left side's names. Nullability
//! is the union (nullable if either side is).

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const Error = exec.Error;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

pub const SetUnion = struct {
    allocator: Allocator,
    left: Query,
    right: Query,
    /// Set to false once the left side runs dry; subsequent next()
    /// calls drain from the right.
    left_exhausted: bool,
    output_schema: []Column,
    /// True for UNION ALL; reserved for the future distinct variant.
    /// v1 only constructs `all = true`.
    all: bool,

    pub fn create(allocator: Allocator, left: Query, right: Query, all: bool) !Query {
        const left_schema = left.outputSchema();
        const right_schema = right.outputSchema();
        if (left_schema.len != right_schema.len) return Error.TypeMismatch;
        for (left_schema, right_schema) |l, r| {
            if (std.meta.activeTag(l.type) != std.meta.activeTag(r.type)) return Error.TypeMismatch;
        }

        const out_schema = try allocator.alloc(Column, left_schema.len);
        errdefer allocator.free(out_schema);
        for (left_schema, right_schema, out_schema) |l, r, *o| {
            o.* = .{ .name = l.name, .type = l.type, .nullable = l.nullable or r.nullable };
        }

        const self = try allocator.create(SetUnion);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .left = left,
            .right = right,
            .left_exhausted = false,
            .output_schema = out_schema,
            .all = all,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *SetUnion) void {
        var l = self.left;
        l.deinit();
        var r = self.right;
        r.deinit();
        self.allocator.free(self.output_schema);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *SetUnion) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(_: *SetUnion, _: Predicate) !void {
        // Pushdown across UNION would need to clone the predicate down
        // each side; not worth the complication for v1.
    }

    pub fn stats(self: *SetUnion) exec.PipelineStats {
        const l = self.left.stats();
        const r = self.right.stats();
        return .{
            .upper_rows = l.upper_rows +| r.upper_rows,
            .sort_state = .{ .keys = &.{}, .global = false },
        };
    }

    pub fn accountant(self: *SetUnion) ?*exec.memory.MemoryAccountant {
        return self.left.accountant() orelse self.right.accountant();
    }

    pub fn next(self: *SetUnion) !?Batch {
        if (!self.left_exhausted) {
            if (try self.left.next()) |b| {
                return rebatched(self, b);
            }
            self.left_exhausted = true;
        }
        if (try self.right.next()) |b| {
            return rebatched(self, b);
        }
        return null;
    }

    fn rebatched(self: *SetUnion, src: Batch) Batch {
        return .{
            .schema = self.output_schema,
            .values = src.values,
            .row_count = src.row_count,
        };
    }
};
