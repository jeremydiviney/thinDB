//! Operator pipeline — front door.
//!
//! Type-erased `Query` value backed by a small vtable. Each operator
//! (Scan, Filter, Project, Limit, Sort, Aggregate) lives in its own file
//! and exposes `next()`, `deinit()`, `outputSchema()`, `addPrune()`. The
//! vtable wires them together.
//!
//! Public re-exports of types/functions defined in sibling files appear at
//! the bottom of this file so callers can keep importing `exec.*` without
//! caring how the operators are split internally.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const api = @import("../api/api.zig");
const Table = api.Table;

pub const Error = error{
    ColumnNotFound,
    TypeMismatch,
    PredicateTypeMismatch,
    UnsupportedOperatorForType,
    SortNoKeys,
    AggregateNoSpecs,
    AggregateColumnRequired,
    AggregateUnsupportedType,
    ArithmeticOverflow,
};

// ---------------------------------------------------------------------------
// Batch — the unit of data flowing between operators
// ---------------------------------------------------------------------------

pub const Batch = struct {
    /// Schema metadata for each output column (name + type), in column order.
    schema: []const Column,
    /// Borrowed column views — pointing into operator-owned buffers. Valid
    /// only until the next `Query.next()` call.
    values: []const ColumnView,
    row_count: usize,

    pub fn columnIndex(self: Batch, name: []const u8) ?usize {
        for (self.schema, 0..) |c, i| {
            if (std.mem.eql(u8, c.name, name)) return i;
        }
        return null;
    }

    pub fn columnView(self: Batch, name: []const u8) ?ColumnView {
        const idx = self.columnIndex(name) orelse return null;
        return self.values[idx];
    }
};

// ---------------------------------------------------------------------------
// Query — type-erased operator handle
// ---------------------------------------------------------------------------

pub const VTable = struct {
    next: *const fn (ptr: *anyopaque) anyerror!?Batch,
    deinit: *const fn (ptr: *anyopaque) void,
    outputSchema: *const fn (ptr: *anyopaque) []const Column,
    /// Operators that can act on hints (e.g. Scan) use them to skip row
    /// groups; others (Filter, Project, Limit) simply forward to upstream.
    addPrune: *const fn (ptr: *anyopaque, pred: predicate.Predicate) anyerror!void,
};

pub const Query = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    allocator: Allocator,

    pub fn next(self: *Query) !?Batch {
        return self.vtable.next(self.ptr);
    }

    pub fn deinit(self: *Query) void {
        self.vtable.deinit(self.ptr);
        self.* = undefined;
    }

    pub fn outputSchema(self: Query) []const Column {
        return self.vtable.outputSchema(self.ptr);
    }

    pub fn addPrune(self: *Query, pred: predicate.Predicate) !void {
        return self.vtable.addPrune(self.ptr, pred);
    }

    // ----- Combinators -----

    pub fn filter(self: Query, expr: predicate.PredicateExpr) !Query {
        return @import("filter.zig").Filter.create(self.allocator, self, expr);
    }

    pub fn project(self: Query, columns: []const []const u8) !Query {
        return @import("project_limit.zig").Project.create(self.allocator, self, columns);
    }

    pub fn limit(self: Query, n: usize) !Query {
        return @import("project_limit.zig").Limit.create(self.allocator, self, n);
    }

    /// Aggregate over the entire upstream (no grouping).
    pub fn aggregate(self: Query, aggs: []const AggSpec) !Query {
        return @import("aggregate.zig").Aggregate.create(self.allocator, self, &.{}, aggs);
    }

    /// Hash-grouped aggregation. `group_cols` lists the upstream columns to
    /// group by; one output row is emitted per distinct group.
    pub fn groupBy(self: Query, group_cols: []const []const u8, aggs: []const AggSpec) !Query {
        return @import("aggregate.zig").Aggregate.create(self.allocator, self, group_cols, aggs);
    }

    /// Sort upstream rows by `sort_specs` (multi-column, ASC/DESC per key).
    /// Blocking — materializes all upstream rows before emitting any output.
    pub fn orderBy(self: Query, sort_specs: []const SortSpec) !Query {
        return @import("sort.zig").Sort.create(self.allocator, self, sort_specs);
    }

    /// `f` is either a function taking `Query` and returning `!Query`, or a
    /// function returning `Query` (we accept both by being generic).
    pub fn pipe(self: Query, f: anytype) !Query {
        return f(self);
    }
};

/// Lift an operator pointer into a Query. The operator type must define
/// `next()`, `deinit()`, `outputSchema()`, and `addPrune()` methods.
pub fn makeQuery(allocator: Allocator, op: anytype) Query {
    const OpPtr = @TypeOf(op);
    const Op = comptime blk: {
        const info = @typeInfo(OpPtr);
        if (info != .pointer) @compileError("makeQuery: expected pointer to operator");
        break :blk info.pointer.child;
    };

    const Wrapper = struct {
        fn nextWrap(ptr: *anyopaque) anyerror!?Batch {
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.next();
        }
        fn deinitWrap(ptr: *anyopaque) void {
            const o: *Op = @ptrCast(@alignCast(ptr));
            o.deinit();
        }
        fn outputSchemaWrap(ptr: *anyopaque) []const Column {
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.outputSchema();
        }
        fn addPruneWrap(ptr: *anyopaque, pred: predicate.Predicate) anyerror!void {
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.addPrune(pred);
        }

        const vt: VTable = .{
            .next = nextWrap,
            .deinit = deinitWrap,
            .outputSchema = outputSchemaWrap,
            .addPrune = addPruneWrap,
        };
    };

    return .{ .ptr = op, .vtable = &Wrapper.vt, .allocator = allocator };
}

/// Top-level entry point: build a scan query against a Table.
pub fn scan(allocator: Allocator, table: *Table) !Query {
    return @import("scan.zig").Scan.create(allocator, table);
}

// ---------------------------------------------------------------------------
// Re-exports — callers @import("exec.zig") for everything operator-related
// ---------------------------------------------------------------------------

pub const predicate = @import("predicate.zig");
pub const Predicate = predicate.Predicate;
pub const PredicateOp = predicate.PredicateOp;
pub const PredicateExpr = predicate.PredicateExpr;
pub const leafExpr = predicate.leafExpr;
pub const isNullExpr = predicate.isNullExpr;
pub const isNotNullExpr = predicate.isNotNullExpr;
pub const statsOverlapPredicate = predicate.statsOverlapPredicate;

pub const Scan = @import("scan.zig").Scan;
pub const Filter = @import("filter.zig").Filter;
pub const Project = @import("project_limit.zig").Project;
pub const Limit = @import("project_limit.zig").Limit;

pub const sort_op = @import("sort.zig");
pub const Sort = sort_op.Sort;
pub const SortSpec = sort_op.SortSpec;

pub const aggregate_op = @import("aggregate.zig");
pub const Aggregate = aggregate_op.Aggregate;
pub const AggFunc = aggregate_op.AggFunc;
pub const AggSpec = aggregate_op.AggSpec;

test {
    _ = predicate;
    _ = Scan;
    _ = Filter;
    _ = Project;
    _ = Limit;
    _ = Sort;
    _ = Aggregate;
    _ = @import("exec_test.zig");
}
