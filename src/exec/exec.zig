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

pub const prof = @import("../util/prof.zig");

pub const Error = error{
    ColumnNotFound,
    TypeMismatch,
    PredicateTypeMismatch,
    UnsupportedOperatorForType,
    SortNoKeys,
    AggregateNoSpecs,
    AggregateColumnRequired,
    AggregateUnsupportedType,
    AggregateInvalidParam,
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
    /// Window operator: shape unsupported in the current implementation
    /// (string-typed window output, args other than column refs for
    /// aggregates, etc.). Tier 1 ships a deliberately narrow subset.
    WindowUnsupported,
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
            if (@import("../types.zig").columnNameEql(c.name, name)) return i;
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
    /// Render this operator's physical plan line(s) into `out` at the given
    /// indentation `depth`, then recurse into upstream(s) at `depth + 1`.
    /// Shows the chosen physical operator + decisions (hash vs sort group-by,
    /// join algorithm, pre-sorted/sort-elided), so the tree shape reveals
    /// what the compiler picked.
    explain: *const fn (ptr: *anyopaque, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) anyerror!void,
};

/// Write `depth` levels of indentation then a complete label line.
pub fn explainLine(out: *std.ArrayList(u8), allocator: Allocator, depth: usize, text: []const u8) !void {
    try explainIndent(out, allocator, depth);
    try out.appendSlice(allocator, text);
    try out.append(allocator, '\n');
}

/// Write `depth` levels of indentation (no newline). For operators that
/// build a dynamic label line (column lists, names) directly into `out`.
pub fn explainIndent(out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
    var i: usize = 0;
    while (i < depth) : (i += 1) try out.appendSlice(allocator, "  ");
}

/// Sort property of an operator's output stream.
pub const SortState = struct {
    /// Columns this stream is sorted by, in lexicographic order. Empty
    /// slice = not sorted on any known prefix. A join planner can
    /// check whether its join key matches a leading prefix of these.
    keys: []const []const u8 = &.{},
    /// Per-key sort direction. Either empty (⟺ every key ascending — the
    /// common case) or the same length as `keys`, where `descs[i] == true`
    /// means `keys[i]` is sorted descending. Grouping is direction-
    /// agnostic (equal values are adjacent in either order), but a
    /// sort-merge join's ascending merge must reject a descending prefix —
    /// see `joinKeysCovered`.
    descs: []const bool = &.{},
    /// `true` = sorted across the whole stream (globally). `false` =
    /// sorted only within each emitted batch (e.g., scan of an
    /// uncompacted table where each row group is sorted but segments
    /// can overlap). Joins exploit `global=true` for the SMJ-merge-only
    /// fast path.
    global: bool = false,

    /// Whether key `i` is ascending (the empty-`descs` convention means
    /// all ascending).
    pub fn ascendingAt(self: SortState, i: usize) bool {
        return self.descs.len == 0 or !self.descs[i];
    }
};

/// Per-column distinct-value cardinality as it flows through the pipeline.
/// `exact: n` means a proven upper bound of `n` distinct values (filters
/// only shrink distinct counts, so an upstream bound stays valid). `unknown`
/// means no proven bound (also used for the on-disk "big" marker). The
/// GROUP BY planner multiplies the group keys' bounds: all `exact` and the
/// product under the limit ⇒ hash fits; any `unknown` ⇒ sort.
pub const ColCard = union(enum) {
    exact: u32,
    unknown,
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
    /// Per-output-column distinct-value bound, indexed by output schema
    /// column. Empty ⇒ no information (all columns unknown).
    column_cards: []const ColCard = &.{},
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

    pub fn explain(self: Query, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        return self.vtable.explain(self.ptr, out, allocator, depth);
    }

    /// Render the whole compiled operator tree as an indented physical plan.
    /// Caller owns the returned bytes.
    pub fn explainPlan(self: Query, allocator: Allocator) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(allocator);
        try self.explain(&out, allocator, 0);
        return out.toOwnedSlice(allocator);
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

    pub fn limitOffset(self: Query, n: usize, offset: usize) !Query {
        return @import("project_limit.zig").Limit.createOffset(self.allocator, self, n, offset);
    }

    /// Aggregate over the entire upstream (no grouping).
    pub fn aggregate(self: Query, aggs: []const AggSpec) !Query {
        return @import("aggregate.zig").Aggregate.create(self.allocator, self, &.{}, aggs, null);
    }

    /// Hash-grouped aggregation. `group_cols` lists the upstream columns to
    /// group by; one output row is emitted per distinct group.
    pub fn groupBy(self: Query, group_cols: []const []const u8, aggs: []const AggSpec) !Query {
        return @import("aggregate.zig").Aggregate.create(self.allocator, self, group_cols, aggs, null);
    }

    /// Hash GROUP BY with an optional top-k hint (set when this aggregate is
    /// directly under `ORDER BY <keys> LIMIT k`). When every order key resolves
    /// to a numeric aggregate output, the aggregate emits only the top-k groups.
    pub fn groupByTopK(self: Query, group_cols: []const []const u8, aggs: []const AggSpec, top_k: ?@import("../ir/ir.zig").Op.TopK) !Query {
        const agg = @import("aggregate.zig");
        const t = top_k orelse
            return agg.Aggregate.create(self.allocator, self, group_cols, aggs, null);
        // The hint's keys are resolved (to agg indices) synchronously inside
        // create, so this temporary translation array need only outlive the call.
        const keys = try self.allocator.alloc(agg.TopKKey, t.keys.len);
        defer self.allocator.free(keys);
        for (t.keys, keys) |src, *dst| dst.* = .{ .col = src.col, .desc = src.desc };
        return agg.Aggregate.create(self.allocator, self, group_cols, aggs, agg.TopKHint{ .k = t.k, .keys = keys });
    }

    /// Streaming sort-based grouped aggregation. Requires the input to be
    /// sorted such that equal group keys are adjacent. Holds only one
    /// group's state at a time (O(1) in cardinality). Caller (planner)
    /// must verify the sortedness precondition via `stats().sort_state`.
    pub fn streamGroupBy(self: Query, group_cols: []const []const u8, aggs: []const AggSpec) !Query {
        return @import("aggregate.zig").SortedAggregate.create(self.allocator, self, group_cols, aggs);
    }

    /// Sort upstream rows by `sort_specs` (multi-column, ASC/DESC per key).
    /// Blocking — materializes all upstream rows before emitting any output.
    pub fn orderBy(self: Query, sort_specs: []const SortSpec) !Query {
        return @import("sort.zig").Sort.create(self.allocator, self, sort_specs);
    }

    /// Bounded `ORDER BY ... LIMIT limit OFFSET offset` — keeps only the
    /// `limit + offset` rows it might emit instead of materializing the
    /// whole input. The planner fuses `Limit(OrderBy(X))` into this.
    pub fn topN(self: Query, sort_specs: []const SortSpec, n: usize, offset: usize) !Query {
        return @import("topn.zig").TopN.create(self.allocator, self, sort_specs, n, offset);
    }

    /// Add derived columns via scalar function calls. Each `Derived`
    /// names the new column and supplies an `Expr` that resolves to a
    /// function on upstream columns (v1: no nesting). Output schema
    /// extends the upstream schema with these new columns appended.
    pub fn compute(self: Query, derived: []const @import("compute.zig").Derived) !Query {
        return @import("compute.zig").Compute.create(self.allocator, self, derived);
    }

    /// Window function step. `specs` is the list of unique window
    /// specifications referenced by `calls`; `calls` carry a `spec_idx`
    /// into `specs`. Operator sorts the input once per spec and
    /// evaluates all calls sharing that spec in a single sweep.
    pub fn window(
        self: Query,
        specs: []const @import("../ir/ir.zig").WindowSpec,
        calls: []const @import("../ir/ir.zig").WindowCall,
    ) !Query {
        return @import("window.zig").Window.create(self.allocator, self, specs, calls);
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
            if (!prof.enabled) return o.next();
            const t0 = prof.nowTicks();
            const r = o.next();
            const d = prof.nowTicks() - t0;
            prof.add(@typeName(Op), if (d > 0) @intCast(d) else 0);
            return r;
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
        fn explainWrap(ptr: *anyopaque, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) anyerror!void {
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.explain(out, alloc, depth);
        }

        const vt: VTable = .{
            .next = nextWrap,
            .deinit = deinitWrap,
            .outputSchema = outputSchemaWrap,
            .addPrune = addPruneWrap,
            .stats = statsWrap,
            .accountant = accountantWrap,
            .explain = explainWrap,
        };
    };

    return .{ .ptr = op, .vtable = &Wrapper.vt, .allocator = allocator };
}

/// Top-level entry point: build a scan query against a Table.
pub fn scan(allocator: Allocator, table: *Table) !Query {
    return @import("scan.zig").Scan.create(allocator, table);
}

/// Scan that uses a query-scoped accountant owned by the caller (the
/// SQL compile path's `CompileCtx`). Pass `null` to fall back to the
/// self-minting behaviour of `scan`.
pub fn scanWithAccountant(
    allocator: Allocator,
    table: *Table,
    accountant_ptr: ?*memory.MemoryAccountant,
) !Query {
    return @import("scan.zig").Scan.createWithAccountant(allocator, table, accountant_ptr);
}

/// Like `scanWithAccountant`, but `needed` (when non-null) restricts the
/// scan to those columns by name — projection pushdown. `null` reads all.
pub fn scanWithProjection(
    allocator: Allocator,
    table: *Table,
    accountant_ptr: ?*memory.MemoryAccountant,
    needed: ?[]const []const u8,
) !Query {
    return @import("scan.zig").Scan.createWithProjection(allocator, table, accountant_ptr, needed);
}

/// Build a join's output `column_cards` by concatenating the left columns'
/// bounds with the kept right columns' bounds (the join output schema is
/// `left ⧺ right-where-kept`). A join can't grow a column's distinct count,
/// so each side's bound stays a valid upper bound. Returns `&.{}` when
/// neither side carries cardinality info. Shared by all join operators.
/// `right_kept_mask` is null when every right column is kept (range joins
/// that drop no equi key).
pub fn concatJoinCards(
    allocator: Allocator,
    left: Query,
    right: Query,
    left_col_count: usize,
    right_kept_mask: ?[]const bool,
    output_len: usize,
) ![]const ColCard {
    const lc = left.stats().column_cards;
    const rc = right.stats().column_cards;
    if (lc.len == 0 and rc.len == 0) return &.{};
    const cc = try allocator.alloc(ColCard, output_len);
    for (cc[0..left_col_count], 0..) |*out, i| out.* = if (i < lc.len) lc[i] else .unknown;
    var oi: usize = left_col_count;
    if (right_kept_mask) |mask| {
        for (mask, 0..) |keep, ri| {
            if (!keep) continue;
            cc[oi] = if (ri < rc.len) rc[ri] else .unknown;
            oi += 1;
        }
    } else {
        var ri: usize = 0;
        while (oi < output_len) : (oi += 1) {
            cc[oi] = if (ri < rc.len) rc[ri] else .unknown;
            ri += 1;
        }
    }
    return cc;
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

pub const SetUnion = @import("set_union.zig").SetUnion;

pub const AliasRename = @import("alias_rename.zig").AliasRename;

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
    _ = @import("scalar_fn_test.zig");
    _ = @import("cast.zig");
}
