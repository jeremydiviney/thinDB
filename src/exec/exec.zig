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

pub const memory = @import("../memory.zig");

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
    /// Compute operator: no derived columns provided.
    ComputeNoColumns,
    /// Compute operator: derived column name collides with an upstream
    /// column or another derived column.
    ComputeNameCollision,
    /// Compute operator: an expression shape not yet supported in v1
    /// (nested calls, literal-only derived).
    ComputeUnsupportedExpr,
    /// Compute operator: no scalar function overload matches the call's
    /// `(name, arg_types)`.
    ComputeNoSuchOverload,
    /// Compute operator: kernel call arity exceeds the internal fixed
    /// buffer (currently 16). Wider arities need a heap-allocated arg
    /// view buffer.
    ComputeTooManyArgs,
    /// Join operator: the requested join_type isn't implemented yet.
    /// v1 supports inner only; outer/semi/anti land in follow-ups.
    JoinUnsupportedType,
    /// Join operator: the `on` clause has no key pairs.
    JoinEmptyOnClause,
    /// Join operator: a key-pair's column types don't match (e.g.
    /// joining bigint to string).
    JoinKeyTypeMismatch,
    /// Join operator: left and right outputs have a colliding column
    /// name. v1 doesn't auto-alias; user must rename via .compute()
    /// or .exclude() before the join.
    JoinColumnNameCollision,
    /// A blocking operator (Sort, Aggregate, Join build, SMJ, NLJ)
    /// would exceed `Config.query_memory_budget` if it kept
    /// materializing. Aborts mid-build with a clear error rather
    /// than letting the underlying allocator OOM the process.
    MemoryBudgetExceeded,
    /// Hash join's build phase observed a key whose frequency
    /// exceeds `Spec.skew_threshold × build_rows`. Retry with
    /// `.algorithm = .sort_merge` for cache-friendlier bucket walks.
    JoinHeavySkew,
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
    /// Pre-execution statistics on this operator's OUTPUT: upper bound
    /// on rows, sort state. Cheap — computed from manifest + operator
    /// definitions, no data read required. Used by downstream planners
    /// (Join especially) to make algorithm decisions.
    stats: *const fn (ptr: *anyopaque) PipelineStats,
    /// Per-query memory accountant. Returns the same pointer
    /// throughout the query pipeline (operators inherit from their
    /// upstream). Null = no budget tracking (default; common in tests).
    accountant: *const fn (ptr: *anyopaque) ?*memory.MemoryAccountant,
};

/// Sort property of an operator's output stream.
pub const SortState = struct {
    /// Columns this stream is sorted by, in lexicographic order. Empty
    /// slice = not sorted on any known prefix. A join planner can
    /// check whether its join key matches a leading prefix of these.
    keys: []const []const u8 = &.{},
    /// `true` = sorted across the whole stream (globally). `false` =
    /// sorted only within each emitted batch (e.g., scan of an
    /// uncompacted table where each row group is sorted but segments
    /// can overlap). Joins exploit `global=true` for the SMJ-merge-only
    /// fast path.
    global: bool = false,
};

/// Pre-execution statistics about an operator's output.
pub const PipelineStats = struct {
    /// Upper bound on the number of rows this operator will emit.
    /// Never null — for operators with selectivity (Filter), this is
    /// the conservative upper bound (input row count). Refined to
    /// `exact_rows` only after the operator's input has been drained.
    upper_rows: u64,
    /// Sort property of the output stream. See `SortState`.
    sort_state: SortState = .{},
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

    /// Pre-execution stats on this operator's output. Cheap; no data
    /// scanned. See `PipelineStats`.
    pub fn stats(self: Query) PipelineStats {
        return self.vtable.stats(self.ptr);
    }

    /// Per-query memory accountant. Set up by the bottom-most Scan
    /// when Table.query_memory_budget > 0. Combinators upstream
    /// inherit by calling this method on their input.
    pub fn accountant(self: Query) ?*memory.MemoryAccountant {
        return self.vtable.accountant(self.ptr);
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

    /// Add derived columns via scalar function calls. Each `Derived`
    /// names the new column and supplies an `Expr` that resolves to a
    /// function on upstream columns (v1: no nesting). Output schema
    /// extends the upstream schema with these new columns appended.
    pub fn compute(self: Query, derived: []const @import("compute.zig").Derived) !Query {
        return @import("compute.zig").Compute.create(self.allocator, self, derived);
    }

    /// Inner equi-join with `other`. Output schema is this side's
    /// columns followed by `other`'s columns; column names must not
    /// collide (rename one side via `.compute()` if needed). Algorithm
    /// is hash join in v1 — build side is whichever has the smaller
    /// upper-bound row count.
    pub fn join(self: Query, other: Query, spec: @import("join.zig").Spec) !Query {
        return @import("join.zig").Join.create(self.allocator, self, other, spec);
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
        fn statsWrap(ptr: *anyopaque) PipelineStats {
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.stats();
        }
        fn accountantWrap(ptr: *anyopaque) ?*memory.MemoryAccountant {
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.accountant();
        }

        const vt: VTable = .{
            .next = nextWrap,
            .deinit = deinitWrap,
            .outputSchema = outputSchemaWrap,
            .addPrune = addPruneWrap,
            .stats = statsWrap,
            .accountant = accountantWrap,
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

pub const expr_mod = @import("expr.zig");
pub const Expr = expr_mod.Expr;
pub const scalar_fn = @import("scalar_fn.zig");
pub const ScalarFn = scalar_fn.ScalarFn;

pub const compute_op = @import("compute.zig");
pub const Compute = compute_op.Compute;
pub const Derived = compute_op.Derived;

pub const join_op = @import("join.zig");
pub const Join = join_op.Join;
pub const JoinSpec = join_op.Spec;
pub const JoinType = join_op.JoinType;
pub const KeyPair = join_op.KeyPair;

// PipelineStats / SortState are defined above; re-exported for clarity.

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
