//! Compute operator — adds derived columns to a batch using scalar
//! functions.
//!
//! v1 scope (incremental):
//!   - Each derived column's `Expr` is either:
//!       * a `.col_ref` (rename) — copies an upstream column under a
//!         new name without invoking any function.
//!       * a `.call` whose args are themselves `.col_ref`s (no nested
//!         calls in v1; nested fall back to multiple Compute layers).
//!   - Output schema = upstream schema + derived columns appended in
//!     the order given. Downstream operators see both.
//!   - Null handling: per `NullStrategy` of the resolved function.
//!     `.propagates` → if ANY arg is null at row i, output is null.
//!     `.absorbs` → kernel handles nulls itself.
//!
//! Future (post-v1): nested calls (`upper(lower(x))`), literal args,
//! constant folding, expression compile to a flat op list with shared
//! intermediates.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const store = @import("../engine/store.zig");
const ColumnStore = store.ColumnStore;

const expr_mod = @import("expr.zig");
const Expr = expr_mod.Expr;
const predicate_mod = @import("predicate.zig");
const PredicateExpr = predicate_mod.PredicateExpr;
const scalar_fn = @import("scalar_fn.zig");
const ScalarFn = scalar_fn.ScalarFn;
const simd = @import("../util/simd.zig");
const NullStrategy = scalar_fn.NullStrategy;

const cast = @import("cast.zig");
const CastKernel = cast.CastKernel;

const exec = @import("exec.zig");
const Batch = exec.Batch;
const Query = exec.Query;
const Predicate = exec.Predicate;
const Error = exec.Error;
const makeQuery = exec.makeQuery;

/// One derived column on a Compute operator.
pub const Derived = struct {
    name: []const u8,
    expr: Expr,
};

/// One argument to a function call inside the resolved expression
/// tree. Args can be: an upstream column reference, a literal (which
/// materializes as a per-batch replicated constant column), or a
/// nested function call (which evaluates recursively).
const ArgPlan = union(enum) {
    col: usize,
    lit: *LitSlot,
    call: *CallPlan,
};

/// Per-literal scratch — typed ColumnStore refilled each batch with
/// `row_count` copies of `value`.
const LitSlot = struct {
    value: types.Value,
    buf: ColumnStore,
};

/// Resolved call node: ScalarFn + per-arg evaluation plan + optional
/// coercion machinery + output buffer. Roots of derived columns
/// alias `Compute.derived_cols[i]` as their output (avoids one copy);
/// internal nodes own their scratch.
const CallPlan = struct {
    func: ScalarFn,
    args: []ArgPlan,
    arg_casts: ?[]const ?CastKernel,
    cast_buffers: ?[]?ColumnStore,
    /// Where this call writes its result. Aliased to a parent slot
    /// (derived_cols[i] at the root) OR owned scratch (internal nodes).
    /// `output_owned = true` means Compute.deinit will free it.
    output: *ColumnStore,
    output_owned: bool,
    output_type: Type,
};

/// Fused `col <op> const` (or `const <op> col`) for +/-/* — evaluated in one
/// SIMD pass that widens the source column straight to the output type, so we
/// skip both the smallint→int cast column and the replicated-literal column the
/// generic call path would materialize. Only built when the column is
/// non-nullable and column+output are the same kind (int→int or float→float).
const FusedScalar = struct {
    src_idx: usize,
    src_type: Type,
    out_type: Type, // .int, .bigint, or .double
    op: simd.BinOp,
    col_left: bool,
    scalar_i: i64,
    scalar_f: f64,
};

/// Plan-time classification of a derived column's expression for PROVABLE
/// stats propagation. Computed at resolve from the expression shape alone
/// (no upstream stats needed); `stats()` combines it with the live upstream
/// `ColStat` of the referenced source column(s). Every rule yields an
/// UPPER/inclusive bound — never an estimate beyond what the algebra proves.
const StatClass = union(enum) {
    /// No provable bound (multi-arg non-arithmetic call, opaque fn, string
    /// op, etc.): ndv unknown, min/max null.
    none,
    /// Literal constant column: ndv 1, and an integer-family value carries
    /// min == max == value (null for non-int literals).
    literal: ?i128,
    /// Single-column function `f(src)`: ndv ≤ NDV(src) (pigeonhole). When
    /// `affine` is set, `value = scale·src + offset` exactly — min/max flow
    /// through the interval map. A null `affine` keeps min/max null.
    unary: struct { src_idx: usize, affine: ?Affine },
    /// Two-column arithmetic `src1 <op> src2`: ndv ≤ NDV(src1)·NDV(src2).
    /// `op` is add/sub (min/max via interval); other ops leave min/max null.
    binary: struct { src1: usize, src2: usize, op: ?simd.BinOp },
};

/// `value = scale·col + offset`, all i128. Captures the affine transforms the
/// interval map can flow a range through: `col±c`, `c±col`, `c·col`, `col·c`.
const Affine = struct { scale: i128, offset: i128 };

/// Resolved per-derived plan: rename, literal-only (constant column),
/// a function-call tree, a searched-CASE expression, or a fused col-op-scalar.
const ResolvedDerived = struct {
    name: []const u8,
    output_type: Type,
    /// Plan-time provable-stats descriptor (see `StatClass`). Read only by
    /// `stats()`; has no effect on evaluation.
    stat_class: StatClass,
    kind: union(enum) {
        rename: struct { src_idx: usize },
        lit_only: *LitSlot,
        call: *CallPlan,
        case: *CasePlan,
        fused_scalar: FusedScalar,
    },
};

/// One branch of a resolved CASE. `cond` is evaluated as a row mask;
/// `then_src` produces the per-row value when this branch wins.
const CaseBranch = struct {
    cond: PredicateExpr,
    then_src: BranchSrc,
};

/// Materialization source for a CASE branch's THEN (and ELSE) clause.
/// CASE's branches don't support nested CASE in v1 — keeps the resolve
/// + free trees finite without an extra dimension.
const BranchSrc = union(enum) {
    col: usize,
    lit: *LitSlot,
    call: *CallPlan,
};

const CasePlan = struct {
    branches: []const CaseBranch,
    else_src: ?BranchSrc,
    output: *ColumnStore,
    output_owned: bool,
    output_type: Type,
    /// Upstream schema captured at resolve so the per-batch predicate
    /// evaluator can resolve column refs in branch conditions.
    upstream_schema: []const Column,
    /// True when any branch may produce a NULL (else-less form, or
    /// any then_src is a nullable column). Used by Compute to decide
    /// whether the output column needs a validity bitmap.
    may_produce_null: bool,
};

pub const Compute = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    upstream: Query,

    derived: []ResolvedDerived,
    /// One ColumnStore per derived column. Cleared + refilled each
    /// `next()` from the upstream batch.
    derived_cols: []ColumnStore,

    /// Combined output schema: upstream schema followed by derived
    /// columns. Allocated once at create.
    output_schema: []Column,
    /// Reusable views slice (upstream views + derived views), sized at
    /// create. Rewired per batch.
    views: []ColumnView,

    pub fn create(
        allocator: Allocator,
        upstream: Query,
        derived: []const Derived,
    ) !Query {
        if (derived.len == 0) return Error.ComputeNoColumns;
        const up_schema = upstream.outputSchema();

        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const aa = arena.allocator();

        const resolved = try aa.alloc(ResolvedDerived, derived.len);
        for (derived, resolved) |d, *r| r.* = try resolveDerived(allocator, aa, d, up_schema);

        // Validate no duplicate derived names AND no collision with
        // upstream column names (downstream wouldn't be able to
        // disambiguate).
        for (resolved, 0..) |r, i| {
            for (up_schema) |uc| {
                if (@import("../types.zig").columnNameEql(uc.name, r.name)) return Error.ComputeNameCollision;
            }
            for (resolved[0..i]) |prior| {
                if (@import("../types.zig").columnNameEql(prior.name, r.name)) return Error.ComputeNameCollision;
            }
        }

        // Output schema = upstream + derived
        const output_schema = try allocator.alloc(Column, up_schema.len + resolved.len);
        errdefer allocator.free(output_schema);
        for (up_schema, 0..) |c, i| output_schema[i] = c;
        for (resolved, up_schema.len..) |r, i| {
            // Nullable: if propagates AND any input column is nullable
            // → derived is nullable. If absorbs → also nullable (the
            // function can still produce null if all inputs are null).
            // Renames inherit nullability from the source.
            const nullable = derivedNullable(r, up_schema);
            output_schema[i] = .{ .name = r.name, .type = r.output_type, .nullable = nullable };
        }

        // One ColumnStore per derived column. Re-initialized each batch
        // is overkill; instead clearRetainingCapacity at the top of
        // next(). We still rebuild the validity bitmap fresh per batch.
        const derived_cols = try allocator.alloc(ColumnStore, resolved.len);
        errdefer allocator.free(derived_cols);
        var inited: usize = 0;
        errdefer for (derived_cols[0..inited]) |*c| c.deinit(allocator);
        for (resolved, derived_cols, 0..) |r, *col, idx| {
            const out_col = output_schema[up_schema.len + idx];
            col.* = try ColumnStore.init(allocator, r.output_type, out_col.nullable);
            inited += 1;
        }

        const views = try allocator.alloc(ColumnView, output_schema.len);
        errdefer allocator.free(views);

        const self = try allocator.create(Compute);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .arena = arena,
            .upstream = upstream,
            .derived = resolved,
            .derived_cols = derived_cols,
            .output_schema = output_schema,
            .views = views,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Compute) void {
        var up = self.upstream;
        up.deinit();
        for (self.derived_cols) |*c| c.deinit(self.allocator);
        self.allocator.free(self.derived_cols);
        for (self.derived) |r| {
            switch (r.kind) {
                .call => |plan| freeCallPlan(self.allocator, plan),
                .lit_only => |slot| slot.buf.deinit(self.allocator),
                .case => |plan| freeCasePlan(self.allocator, plan),
                .rename => {},
                .fused_scalar => {},
            }
        }
        self.allocator.free(self.output_schema);
        self.allocator.free(self.views);
        self.arena.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Compute) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(self: *Compute, pred: Predicate) !void {
        // Pushdown is safe only for predicates referencing upstream
        // columns (not derived ones). For v1 simplicity, just forward
        // — the Filter operator above us validates predicate columns
        // against our output schema, which includes both upstream and
        // derived, so it'll only push down what's pushable through
        // its own check. Anything Filter pushes here we forward to
        // upstream blindly; upstream's addPrune does its own column
        // check.
        return self.upstream.addPrune(pred);
    }

    /// Compute preserves row count (adds columns, doesn't drop rows).
    /// Sort state preserved as long as the sort-state columns aren't
    /// derived (derived columns can't be in upstream's sort_state, so
    /// they can't appear in it; thus the upstream's claim is still
    /// fully valid in our output schema).
    ///
    /// Extends `column_stats` to cover the derived columns so downstream
    /// routing (notably GROUP BY hash-vs-sort, which products per-key NDV)
    /// can reason about them. Each derived column's bound is derived by
    /// plan-time algebra (`StatClass`) over the source column's live stats:
    ///   - rename → pass-through (values unchanged, so ndv + min/max carry).
    ///   - literal → ndv 1, min == max == the integer value (null for non-int).
    ///   - single-column `f(col)` → ndv ≤ NDV(col) (pigeonhole); min/max via
    ///     interval arithmetic when `f` is affine, else null.
    ///   - two-column `col1 <op> col2` → ndv ≤ NDV1·NDV2 (saturating); min/max
    ///     for +/- via interval arithmetic.
    /// Every i128 step is overflow-checked → null bound on overflow, never a
    /// wrong one. Every ndv is finally capped at `upper_rows` (unchanged by
    /// Compute). Without this, e.g. `GROUP BY <const>, key` reads the const
    /// column as unknown and is forced onto the sort path even when `key`
    /// alone fits the budget.
    pub fn stats(self: *Compute) exec.PipelineStats {
        var up = self.upstream.stats();
        const up_n = self.upstream.outputSchema().len;
        const out_stats = self.arena.allocator().alloc(exec.ColStat, up_n + self.derived.len) catch return up;
        // Align to the OUTPUT schema: copy the upstream stats we have, padding
        // any the upstream didn't report (a short/empty array) with unknown so
        // indices line up. Then extend with the derived columns. Bailing here
        // instead would leave column_stats shorter than the schema, so a
        // derived GROUP BY key reads out-of-bounds → unknown → the router
        // mis-sizes the hash table (the Q28 regex-key sort regression).
        for (out_stats[0..up_n], 0..) |*s, i| {
            s.* = if (i < up.column_stats.len) up.column_stats[i] else .{ .ndv = .unknown };
        }
        for (self.derived, 0..) |d, i| {
            out_stats[up_n + i] = derivedColStat(d, out_stats[0..up_n]);
        }
        exec.capColStats(out_stats, up.upper_rows);
        up.column_stats = out_stats;
        return up;
    }

    pub fn accountant(self: *Compute) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *Compute, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "Compute");
        try self.upstream.explain(out, allocator, depth + 1);
    }

    pub fn next(self: *Compute) !?Batch {
        const in = (try self.upstream.next()) orelse return null;
        const n = in.row_count;

        for (self.derived, self.derived_cols) |r, *out_col| {
            out_col.clear();
            switch (r.kind) {
                .rename => |rn| try appendCopiedColumn(self.allocator, out_col, in.values[rn.src_idx], n),
                .lit_only => |slot| {
                    slot.buf.clear();
                    try fillLiteralColumn(self.allocator, &slot.buf, slot.value, n);
                    try appendCopiedColumn(self.allocator, out_col, slot.buf.view(), n);
                },
                .call => |plan| {
                    try self.evalCall(plan, in.values, n);
                    // Root's owned output → derived_cols slot. Copies
                    // both data + validity (transform.appendAllColumn).
                    try appendCopiedColumn(self.allocator, out_col, plan.output.view(), n);
                },
                .case => |plan| {
                    try self.evalCase(plan, in.values, n);
                    try appendCopiedColumn(self.allocator, out_col, plan.output.view(), n);
                },
                .fused_scalar => |fs| try self.evalFusedScalar(fs, in.values, out_col, n),
            }
        }

        for (in.values, 0..) |v, i| self.views[i] = v;
        for (self.derived_cols, in.values.len..) |c, i| self.views[i] = c.view();

        return Batch{
            .schema = self.output_schema,
            .values = self.views,
            .row_count = n,
        };
    }

    /// Recursively evaluate one CallPlan into its `output` ColumnStore.
    /// Post-order: literals refill, sub-calls evaluate first; then args
    /// are coerced and the kernel runs. Null bookkeeping fires at every
    /// level so `length(upper(tag))` with a NULL tag produces NULL.
    fn evalCall(self: *Compute, plan: *CallPlan, in_values: []const ColumnView, n: usize) !void {
        // Output buffer is cleared by the caller for the root; for
        // internal nodes we clear before refilling here.
        if (plan.output_owned) plan.output.clear();

        // 1. Evaluate each arg (post-order). Build per-arg ColumnViews.
        var arg_views_buf: [16]ColumnView = undefined;
        if (plan.args.len > arg_views_buf.len) return Error.ComputeTooManyArgs;
        const arg_views = arg_views_buf[0..plan.args.len];
        for (plan.args, arg_views) |arg, *view| {
            switch (arg) {
                .col => |idx| view.* = in_values[idx],
                .lit => |slot| {
                    slot.buf.clear();
                    try fillLiteralColumn(self.allocator, &slot.buf, slot.value, n);
                    view.* = slot.buf.view();
                },
                .call => |sub| {
                    try self.evalCall(sub, in_values, n);
                    view.* = sub.output.view();
                },
            }
        }

        // 2. Apply implicit casts.
        if (plan.arg_casts) |casts| {
            const buffers = plan.cast_buffers.?;
            var one_cast_view: [1]ColumnView = undefined;
            for (casts, buffers, 0..) |kfn, *buf_slot, arg_i| {
                const k = kfn orelse continue;
                const buf = &buf_slot.*.?;
                buf.clear();
                one_cast_view[0] = arg_views[arg_i];
                try k(self.allocator, &one_cast_view, buf, n);
                arg_views[arg_i] = buf.view();
            }
        }

        // 3. Run the kernel.
        try plan.func.kernel(self.allocator, arg_views, plan.output, n);

        // 4. Null bookkeeping. Internal calls always have a nullable
        // output (we allocated it that way) so the parent's null-check
        // sees correct validity; root calls only write when their
        // declared schema column is nullable.
        if (plan.output.nulls != null) {
            switch (plan.func.null_strategy) {
                .propagates => try writePropagatedNulls(self.allocator, plan.output, arg_views, n),
                .absorbs => try writeAbsorbedNulls(self.allocator, plan.output, arg_views, n),
                .kernel_managed => {}, // kernel already wrote the bitmap
            }
        }
    }

    /// Evaluate a CASE expression over a single batch. Strategy:
    ///   1. Materialize every branch's THEN (and the ELSE) into a
    ///      per-batch ColumnView. Cheap for col_ref / lit; runs the
    ///      sub-CallPlan for call-typed branches.
    ///   2. Evaluate each branch's condition into a row mask. First
    ///      true mask wins per row; record the winner in `winners`.
    ///   3. Walk rows in order, copying the winner's cell into the
    ///      output ColumnStore (or appending NULL when no branch
    ///      matches and there's no ELSE).
    fn evalCase(self: *Compute, plan: *CasePlan, in_values: []const ColumnView, n: usize) !void {
        plan.output.clear();

        var branch_views_buf: [16]ColumnView = undefined;
        if (plan.branches.len > branch_views_buf.len) return Error.ComputeTooManyArgs;
        const branch_views = branch_views_buf[0..plan.branches.len];
        for (plan.branches, branch_views) |br, *bv| {
            bv.* = try self.materializeBranchSrc(br.then_src, in_values, n);
        }
        var else_view: ?ColumnView = null;
        if (plan.else_src) |es| else_view = try self.materializeBranchSrc(es, in_values, n);

        const cond_buf = try self.allocator.alloc(bool, n);
        defer self.allocator.free(cond_buf);
        const winners = try self.allocator.alloc(i32, n);
        defer self.allocator.free(winners);
        @memset(winners, -1);

        const fake_batch: Batch = .{
            .schema = plan.upstream_schema,
            .values = in_values,
            .row_count = n,
        };
        for (plan.branches, 0..) |br, bi| {
            @memset(cond_buf, false);
            try predicate_mod.evaluatePredicate(self.allocator, br.cond, plan.upstream_schema, fake_batch, cond_buf);
            for (cond_buf, winners) |c, *w| {
                if (c and w.* == -1) w.* = @intCast(bi);
            }
        }

        var i: usize = 0;
        while (i < n) : (i += 1) {
            const w = winners[i];
            if (w == -1) {
                if (else_view) |ev| {
                    try appendCellFromView(self.allocator, plan.output, ev, i);
                } else {
                    try plan.output.data.appendNullPlaceholder(self.allocator);
                    if (plan.output.nulls != null) {
                        const row = plan.output.data.rowCount() - 1;
                        try plan.output.appendValidBit(self.allocator, row, false);
                    }
                }
            } else {
                try appendCellFromView(self.allocator, plan.output, branch_views[@intCast(w)], i);
            }
        }
    }

    fn materializeBranchSrc(self: *Compute, s: BranchSrc, in_values: []const ColumnView, n: usize) !ColumnView {
        return switch (s) {
            .col => |idx| in_values[idx],
            .lit => |slot| blk: {
                slot.buf.clear();
                try fillLiteralColumn(self.allocator, &slot.buf, slot.value, n);
                break :blk slot.buf.view();
            },
            .call => |sub| blk: {
                try self.evalCall(sub, in_values, n);
                break :blk sub.output.view();
            },
        };
    }

    /// Evaluate a fused `col <op> const` directly into `out_col` in one
    /// widening SIMD pass — no cast column, no replicated-literal column.
    /// Only built for non-nullable int/float source columns (see tryFuseScalar),
    /// so there is no validity bitmap to propagate.
    fn evalFusedScalar(self: *Compute, fs: FusedScalar, in_values: []const ColumnView, out_col: *ColumnStore, n: usize) !void {
        const src = in_values[fs.src_idx];
        switch (fs.out_type) {
            .int => {
                try out_col.data.int.ensureUnusedCapacity(self.allocator, n);
                out_col.data.int.items.len = n;
                const dst = out_col.data.int.items[0..n];
                const s: i32 = @intCast(fs.scalar_i);
                switch (fs.src_type) {
                    .tinyint => runScalar(i8, i32, fs.op, fs.col_left, src.data.tinyint[0..n], s, dst),
                    .smallint => runScalar(i16, i32, fs.op, fs.col_left, src.data.smallint[0..n], s, dst),
                    .int => runScalar(i32, i32, fs.op, fs.col_left, src.data.int[0..n], s, dst),
                    .boolean => runScalar(u8, i32, fs.op, fs.col_left, src.data.boolean[0..n], s, dst),
                    else => unreachable,
                }
            },
            .bigint => {
                try out_col.data.bigint.ensureUnusedCapacity(self.allocator, n);
                out_col.data.bigint.items.len = n;
                const dst = out_col.data.bigint.items[0..n];
                const s: i64 = fs.scalar_i;
                switch (fs.src_type) {
                    .tinyint => runScalar(i8, i64, fs.op, fs.col_left, src.data.tinyint[0..n], s, dst),
                    .smallint => runScalar(i16, i64, fs.op, fs.col_left, src.data.smallint[0..n], s, dst),
                    .int => runScalar(i32, i64, fs.op, fs.col_left, src.data.int[0..n], s, dst),
                    .bigint => runScalar(i64, i64, fs.op, fs.col_left, src.data.bigint[0..n], s, dst),
                    .boolean => runScalar(u8, i64, fs.op, fs.col_left, src.data.boolean[0..n], s, dst),
                    else => unreachable,
                }
            },
            .double => {
                try out_col.data.double.ensureUnusedCapacity(self.allocator, n);
                out_col.data.double.items.len = n;
                const dst = out_col.data.double.items[0..n];
                const s: f64 = fs.scalar_f;
                switch (fs.src_type) {
                    .float => runScalar(f32, f64, fs.op, fs.col_left, src.data.float[0..n], s, dst),
                    .double => runScalar(f64, f64, fs.op, fs.col_left, src.data.double[0..n], s, dst),
                    else => unreachable,
                }
            },
            else => unreachable,
        }
    }
};

/// Bridge the runtime op/direction to the comptime-specialized SIMD kernel.
fn runScalar(comptime Tsrc: type, comptime Tout: type, op: simd.BinOp, col_left: bool, src: []const Tsrc, scalar: Tout, dst: []Tout) void {
    switch (op) {
        inline else => |o| switch (col_left) {
            inline else => |cl| simd.scalarOp(Tsrc, Tout, o, cl, src, scalar, dst),
        },
    }
}

fn fusableSrc(t: Type) bool {
    return switch (t) {
        .tinyint, .smallint, .int, .bigint, .boolean, .float, .double => true,
        else => false,
    };
}

fn isIntType(t: Type) bool {
    return switch (t) {
        .tinyint, .smallint, .int, .bigint, .boolean => true,
        else => false,
    };
}

fn valueToI64(v: types.Value) i64 {
    return switch (v) {
        .int => |x| x,
        .bigint => |x| x,
        .smallint => |x| x,
        .tinyint => |x| x,
        .boolean => |x| @intFromBool(x),
        .date => |x| x,
        .datetime => |x| x,
        .decimal64 => |x| x,
        else => 0,
    };
}

fn valueToF64(v: types.Value) f64 {
    return switch (v) {
        .double => |x| x,
        .float => |x| x,
        .int => |x| @floatFromInt(x),
        .bigint => |x| @floatFromInt(x),
        .smallint => |x| @floatFromInt(x),
        .tinyint => |x| @floatFromInt(x),
        else => 0,
    };
}

/// Recognize `col +/-/* const` (either operand order) where the column is a
/// non-nullable int/float and column+result are the same kind, so it can be
/// fused into one widening SIMD pass. Returns null to fall back to the generic
/// call path.
fn tryFuseScalar(aa: Allocator, expr: Expr, up_schema: []const Column) !?FusedScalar {
    const c = switch (expr) {
        .call => |x| x,
        else => return null,
    };
    const op: simd.BinOp = if (std.mem.eql(u8, c.fn_name, "add"))
        .add
    else if (std.mem.eql(u8, c.fn_name, "sub"))
        .sub
    else if (std.mem.eql(u8, c.fn_name, "mul"))
        .mul
    else
        return null;
    if (c.args.len != 2) return null;

    var col_idx: usize = undefined;
    var lit_v: types.Value = undefined;
    var col_left: bool = undefined;
    switch (c.args[0]) {
        .col_ref => |name| switch (c.args[1]) {
            .lit => |v| {
                col_idx = columnIndex(up_schema, name) orelse return null;
                lit_v = v;
                col_left = true;
            },
            else => return null,
        },
        .lit => |v| switch (c.args[1]) {
            .col_ref => |name| {
                col_idx = columnIndex(up_schema, name) orelse return null;
                lit_v = v;
                col_left = false;
            },
            else => return null,
        },
        else => return null,
    }

    const src_type = up_schema[col_idx].type;
    if (up_schema[col_idx].nullable or !fusableSrc(src_type)) return null;

    // Canonical output type from the real overload resolution, so the derived
    // column's type matches what the rest of the plan expects.
    var arg_types: [2]Type = undefined;
    arg_types[0] = if (col_left) src_type else literalType(lit_v);
    arg_types[1] = if (col_left) literalType(lit_v) else src_type;
    const r = (try scalar_fn.resolve(aa, c.fn_name, &arg_types)) orelse return null;
    const out_type = r.func.return_type;

    const src_int = isIntType(src_type);
    switch (out_type) {
        .int, .bigint => if (!src_int) return null,
        .double => if (src_int) return null,
        else => return null,
    }

    return FusedScalar{
        .src_idx = col_idx,
        .src_type = src_type,
        .out_type = out_type,
        .op = op,
        .col_left = col_left,
        .scalar_i = valueToI64(lit_v),
        .scalar_f = valueToF64(lit_v),
    };
}

// ---------------------------------------------------------------------------
// Provable-stats classification (plan-time; consumed by stats())
// ---------------------------------------------------------------------------

/// i128 value of an integer-family literal, else null (float/string/decimal:
/// no usable i128 range, matching ColStat.min/max's int-only contract).
fn intFamilyValueI128(v: types.Value) ?i128 {
    return switch (v) {
        .tinyint => |x| x,
        .smallint => |x| x,
        .int => |x| x,
        .bigint => |x| x,
        .largeint => |x| x,
        .boolean => |x| @intFromBool(x),
        .date => |x| x,
        .datetime => |x| x,
        else => null,
    };
}

/// Decompose `e` into `scale·col + offset` over a single column index, for the
/// affine shapes the interval map flows ranges through (`col±c`, `c±col`,
/// `c·col`, `col·c`, `-col` via `0-col`). Returns the src index plus Affine, or
/// null if `e` isn't a single-column affine call. Integer-family literals only.
fn affineUnary(e: Expr, up_schema: []const Column) ?struct { src_idx: usize, affine: Affine } {
    const c = switch (e) {
        .call => |x| x,
        else => return null,
    };
    if (c.args.len != 2) return null;
    const is_add = std.mem.eql(u8, c.fn_name, "add");
    const is_sub = std.mem.eql(u8, c.fn_name, "sub");
    const is_mul = std.mem.eql(u8, c.fn_name, "mul");
    if (!(is_add or is_sub or is_mul)) return null;

    var col_name: []const u8 = undefined;
    var lit_v: types.Value = undefined;
    var col_left: bool = undefined;
    switch (c.args[0]) {
        .col_ref => |n| switch (c.args[1]) {
            .lit => |v| {
                col_name = n;
                lit_v = v;
                col_left = true;
            },
            else => return null,
        },
        .lit => |v| switch (c.args[1]) {
            .col_ref => |n| {
                col_name = n;
                lit_v = v;
                col_left = false;
            },
            else => return null,
        },
        else => return null,
    }
    const k = intFamilyValueI128(lit_v) orelse return null;
    const idx = columnIndex(up_schema, col_name) orelse return null;

    var scale: i128 = undefined;
    var offset: i128 = undefined;
    if (is_add) {
        scale = 1;
        offset = k;
    } else if (is_sub) {
        if (col_left) {
            scale = 1;
            offset = -k; // col - k
        } else {
            scale = -1;
            offset = k; // k - col
        }
    } else { // mul
        scale = k;
        offset = 0;
    }
    return .{ .src_idx = idx, .affine = .{ .scale = scale, .offset = offset } };
}

/// Classify a `.call` expression for provable stats. Single-column calls bound
/// ndv ≤ NDV(src) (pigeonhole) and flow min/max when affine; two-column
/// arithmetic bounds ndv ≤ NDV·NDV with min/max for add/sub. Anything else is
/// `.none`.
fn classifyExpr(e: Expr, up_schema: []const Column) StatClass {
    const c = switch (e) {
        .call => |x| x,
        else => return .none,
    };

    // Two-column arithmetic `col1 <op> col2`.
    if (c.args.len == 2 and c.args[0] == .col_ref and c.args[1] == .col_ref) {
        const idx1 = columnIndex(up_schema, c.args[0].col_ref) orelse return .none;
        const idx2 = columnIndex(up_schema, c.args[1].col_ref) orelse return .none;
        const op: ?simd.BinOp = if (std.mem.eql(u8, c.fn_name, "add"))
            .add
        else if (std.mem.eql(u8, c.fn_name, "sub"))
            .sub
        else
            null; // mul/div two-col: ndv bound still holds, min/max left null
        return .{ .binary = .{ .src1 = idx1, .src2 = idx2, .op = op } };
    }

    // Single-column call. Affine shapes carry an interval map; any other
    // single-column function still gets ndv ≤ NDV(src) by pigeonhole.
    if (affineUnary(e, up_schema)) |aff| {
        return .{ .unary = .{ .src_idx = aff.src_idx, .affine = aff.affine } };
    }
    if (singleColIndex(c, up_schema)) |idx| {
        return .{ .unary = .{ .src_idx = idx, .affine = null } };
    }
    return .none;
}

/// If a call references exactly one distinct upstream column (and only column
/// refs + literals as args), return its index; else null. A function over one
/// column can't manufacture distinct outputs from equal inputs (pigeonhole),
/// so ndv ≤ NDV(that column).
fn singleColIndex(c: Expr.Call, up_schema: []const Column) ?usize {
    var found: ?usize = null;
    for (c.args) |arg| switch (arg) {
        .col_ref => |name| {
            const idx = columnIndex(up_schema, name) orelse return null;
            if (found) |f| {
                if (f != idx) return null; // two distinct columns
            } else found = idx;
        },
        .lit => {},
        else => return null, // nested call / case / subquery: can't prove single-col
    };
    return found;
}

test "NDV chains through deterministic functions (pigeonhole, never grows)" {
    const up_schema = [_]Column{
        .{ .name = "Referer", .type = .{ .varchar = 255 } },
        .{ .name = "n", .type = .int },
    };
    const up_stats = [_]exec.ColStat{
        .{ .ndv = .{ .exact = 19_700_000 } }, // NDV(Referer)
        .{ .ndv = .{ .exact = 1000 } },
    };

    // REGEXP_REPLACE(Referer, '<pat>', '<rep>') — one column arg + two literals.
    // Must classify as single-column ⇒ a deterministic function can't
    // manufacture distinct outputs from equal inputs: ndv ≤ NDV(Referer).
    const regex_args = [_]Expr{
        .{ .col_ref = "Referer" },
        .{ .lit = .{ .int = 0 } }, // pattern literal (value irrelevant to the bound)
        .{ .lit = .{ .int = 0 } }, // replacement literal
    };
    const cls = classifyExpr(.{ .call = .{ .fn_name = "regexp_replace", .args = &regex_args } }, &up_schema);
    try std.testing.expect(cls == .unary);
    try std.testing.expectEqual(@as(usize, 0), cls.unary.src_idx);

    const k = ResolvedDerived{
        .name = "k",
        .output_type = .{ .varchar = 255 },
        .stat_class = cls,
        .kind = .{ .call = undefined }, // derivedColStat only reads the tag for non-rename
    };
    try std.testing.expectEqual(exec.ColCard{ .exact = 19_700_000 }, derivedColStat(k, &up_stats).ndv);

    // Two-column arithmetic ⇒ ndv ≤ NDV1·NDV2, SATURATING at u32 max (never
    // wraps — a wrapped bound could read smaller than reality and mis-route).
    const add_args = [_]Expr{ .{ .col_ref = "Referer" }, .{ .col_ref = "n" } };
    const cls2 = classifyExpr(.{ .call = .{ .fn_name = "add", .args = &add_args } }, &up_schema);
    try std.testing.expect(cls2 == .binary);
    const p = ResolvedDerived{ .name = "p", .output_type = .int, .stat_class = cls2, .kind = .{ .call = undefined } };
    try std.testing.expectEqual(exec.ColCard{ .exact = std.math.maxInt(u32) }, derivedColStat(p, &up_stats).ndv);

    // No provable shape (opaque) ⇒ unknown — never a fabricated number.
    const o = ResolvedDerived{ .name = "o", .output_type = .int, .stat_class = .none, .kind = .{ .call = undefined } };
    try std.testing.expectEqual(exec.ColCard.unknown, derivedColStat(o, &up_stats).ndv);
}

/// Checked i128 add — null on overflow so a derived bound is never wrong.
fn addChecked(a: i128, b: i128) ?i128 {
    return std.math.add(i128, a, b) catch null;
}

/// Checked i128 subtract — null on overflow.
fn subChecked(a: i128, b: i128) ?i128 {
    return std.math.sub(i128, a, b) catch null;
}

/// Checked i128 mul — null on overflow.
fn mulChecked(a: i128, b: i128) ?i128 {
    return std.math.mul(i128, a, b) catch null;
}

/// Apply `scale·x + offset` to one interval endpoint, null on overflow.
fn affineApply(aff: Affine, x: i128) ?i128 {
    const s = mulChecked(aff.scale, x) orelse return null;
    return addChecked(s, aff.offset);
}

/// Flow a `[lo, hi]` range through an affine map. `scale` direction sets which
/// endpoint becomes the new min vs max; scale 0 collapses to `[offset, offset]`.
/// Returns null min/max on any overflow.
fn affineRange(aff: Affine, lo: ?i128, hi: ?i128) struct { min: ?i128, max: ?i128 } {
    if (aff.scale == 0) return .{ .min = aff.offset, .max = aff.offset };
    const l = lo orelse return .{ .min = null, .max = null };
    const h = hi orelse return .{ .min = null, .max = null };
    const a = affineApply(aff, l);
    const b = affineApply(aff, h);
    if (a == null or b == null) return .{ .min = null, .max = null };
    return .{ .min = @min(a.?, b.?), .max = @max(a.?, b.?) };
}

/// Inclusive i128 range of an int-family type, or null for float/string/uuid.
fn typeRangeI128(t: Type) ?struct { lo: i128, hi: i128 } {
    return switch (t) {
        .tinyint => .{ .lo = std.math.minInt(i8), .hi = std.math.maxInt(i8) },
        .smallint => .{ .lo = std.math.minInt(i16), .hi = std.math.maxInt(i16) },
        .int => .{ .lo = std.math.minInt(i32), .hi = std.math.maxInt(i32) },
        .bigint => .{ .lo = std.math.minInt(i64), .hi = std.math.maxInt(i64) },
        .largeint => .{ .lo = std.math.minInt(i128), .hi = std.math.maxInt(i128) },
        .boolean => .{ .lo = 0, .hi = 1 },
        .date => .{ .lo = std.math.minInt(i32), .hi = std.math.maxInt(i32) },
        .datetime => .{ .lo = std.math.minInt(i64), .hi = std.math.maxInt(i64) },
        .decimal64 => .{ .lo = std.math.minInt(i64), .hi = std.math.maxInt(i64) },
        .decimal128 => .{ .lo = std.math.minInt(i128), .hi = std.math.maxInt(i128) },
        else => null,
    };
}

/// A derived [min,max] is only PROVABLE if the runtime arithmetic can't wrap:
/// the integer kernels evaluate at the output type's width with wrapping
/// (`+%`/`*%`), so a computed bound that escapes that width would be a lie.
/// Returns the range only when both endpoints fit `out_type`; null otherwise.
fn provableRange(out_type: Type, min: ?i128, max: ?i128) struct { min: ?i128, max: ?i128 } {
    const lo = min orelse return .{ .min = null, .max = null };
    const hi = max orelse return .{ .min = null, .max = null };
    const rng = typeRangeI128(out_type) orelse return .{ .min = null, .max = null };
    if (lo < rng.lo or hi > rng.hi) return .{ .min = null, .max = null };
    return .{ .min = lo, .max = hi };
}

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

fn resolveDerived(
    runtime_allocator: Allocator,
    aa: Allocator,
    d: Derived,
    up_schema: []const Column,
) !ResolvedDerived {
    switch (d.expr) {
        .col_ref => |name| {
            const idx = columnIndex(up_schema, name) orelse return Error.ColumnNotFound;
            return .{
                .name = try aa.dupe(u8, d.name),
                .output_type = up_schema[idx].type,
                .stat_class = .none, // rename is handled directly by stats() (pass-through)
                .kind = .{ .rename = .{ .src_idx = idx } },
            };
        },
        .lit => |v| {
            const slot = try aa.create(LitSlot);
            slot.* = .{
                .value = v,
                .buf = try ColumnStore.init(runtime_allocator, literalType(v), false),
            };
            return .{
                .name = try aa.dupe(u8, d.name),
                .output_type = literalType(v),
                .stat_class = .{ .literal = intFamilyValueI128(v) },
                .kind = .{ .lit_only = slot },
            };
        },
        .call => {
            const stat_class = classifyExpr(d.expr, up_schema);
            // Fast path: `col +/-/* const` collapses to one widening SIMD pass.
            if (try tryFuseScalar(aa, d.expr, up_schema)) |fs| {
                return .{
                    .name = try aa.dupe(u8, d.name),
                    .output_type = fs.out_type,
                    .stat_class = stat_class,
                    .kind = .{ .fused_scalar = fs },
                };
            }
            const plan = try buildCallPlan(runtime_allocator, aa, d.expr, up_schema);
            return .{
                .name = try aa.dupe(u8, d.name),
                .output_type = plan.output_type,
                .stat_class = stat_class,
                .kind = .{ .call = plan },
            };
        },
        .case => {
            const plan = try buildCasePlan(runtime_allocator, aa, d.expr.case, up_schema);
            return .{
                .name = try aa.dupe(u8, d.name),
                .output_type = plan.output_type,
                .stat_class = .none,
                .kind = .{ .case = plan },
            };
        },
        // Subqueries and var_refs must be resolved (rewritten to `.lit`)
        // by the pre-compile pass before this resolver runs.
        .scalar_subquery, .exists_subquery, .var_ref => return Error.ComputeUnsupportedExpr,
    }
}

/// Resolve a parsed CASE expression to a CasePlan. All branches' THEN
/// (and ELSE) results must unify to a single output type — v1 picks
/// the first branch's type and rejects mismatches. Nested CASE in a
/// branch's THEN is also rejected.
fn buildCasePlan(
    runtime_allocator: Allocator,
    aa: Allocator,
    cs: Expr.Case,
    up_schema: []const Column,
) !*CasePlan {
    if (cs.branches.len == 0) return Error.ComputeUnsupportedExpr;

    const branches = try aa.alloc(CaseBranch, cs.branches.len);
    var built: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < built) : (i += 1) freeBranchSrc(runtime_allocator, branches[i].then_src);
    }

    var inferred_type: ?Type = null;
    var may_null = cs.else_branch == null;
    for (cs.branches, branches) |src, *dst| {
        const then_src = try buildBranchSrc(runtime_allocator, aa, src.then, up_schema);
        const t = branchSrcType(then_src, up_schema);
        if (inferred_type) |it| {
            if (std.meta.activeTag(it) != std.meta.activeTag(t)) {
                freeBranchSrc(runtime_allocator, then_src);
                return Error.ComputeUnsupportedExpr;
            }
        } else inferred_type = t;
        if (branchSrcNullable(then_src, up_schema)) may_null = true;
        dst.* = .{ .cond = src.cond, .then_src = then_src };
        built += 1;
        // Coerce the branch condition's leaf literals to their column types
        // (e.g. `= 0` against a SMALLINT column). The Filter operator does
        // this for WHERE predicates via validateExpr; a CASE condition is
        // evaluated directly in evalCase and needs the same pass, or
        // evaluateMaskWithPred reads the wrong Value union field and panics.
        try predicate_mod.validateExpr(&dst.cond, up_schema);
    }

    var else_src: ?BranchSrc = null;
    if (cs.else_branch) |eb| {
        const es = try buildBranchSrc(runtime_allocator, aa, eb.*, up_schema);
        errdefer freeBranchSrc(runtime_allocator, es);
        const t = branchSrcType(es, up_schema);
        if (inferred_type) |it| {
            if (std.meta.activeTag(it) != std.meta.activeTag(t)) return Error.ComputeUnsupportedExpr;
        }
        if (branchSrcNullable(es, up_schema)) may_null = true;
        else_src = es;
    }

    const out_type = inferred_type orelse return Error.ComputeUnsupportedExpr;

    const out_buf = try runtime_allocator.create(ColumnStore);
    errdefer runtime_allocator.destroy(out_buf);
    out_buf.* = try ColumnStore.init(runtime_allocator, out_type, may_null);

    const plan = try aa.create(CasePlan);
    plan.* = .{
        .branches = branches,
        .else_src = else_src,
        .output = out_buf,
        .output_owned = true,
        .output_type = out_type,
        .upstream_schema = up_schema,
        .may_produce_null = may_null,
    };
    return plan;
}

fn buildBranchSrc(
    runtime_allocator: Allocator,
    aa: Allocator,
    e: Expr,
    up_schema: []const Column,
) !BranchSrc {
    return switch (e) {
        .col_ref => |name| blk: {
            const idx = columnIndex(up_schema, name) orelse return Error.ColumnNotFound;
            break :blk BranchSrc{ .col = idx };
        },
        .lit => |v| blk: {
            const slot = try aa.create(LitSlot);
            slot.* = .{
                .value = v,
                .buf = try ColumnStore.init(runtime_allocator, literalType(v), false),
            };
            break :blk BranchSrc{ .lit = slot };
        },
        .call => blk: {
            const sub = try buildCallPlan(runtime_allocator, aa, e, up_schema);
            break :blk BranchSrc{ .call = sub };
        },
        .case => return Error.ComputeUnsupportedExpr,
        .scalar_subquery, .exists_subquery, .var_ref => return Error.ComputeUnsupportedExpr,
    };
}

fn branchSrcType(s: BranchSrc, up_schema: []const Column) Type {
    return switch (s) {
        .col => |idx| up_schema[idx].type,
        .lit => |slot| literalType(slot.value),
        .call => |plan| plan.output_type,
    };
}

fn branchSrcNullable(s: BranchSrc, up_schema: []const Column) bool {
    return switch (s) {
        .col => |idx| up_schema[idx].nullable,
        .lit => false,
        .call => |plan| callPlanNullable(plan, up_schema),
    };
}

fn freeBranchSrc(allocator: Allocator, s: BranchSrc) void {
    switch (s) {
        .col => {},
        .lit => |slot| slot.buf.deinit(allocator),
        .call => |sub| freeCallPlan(allocator, sub),
    }
}

fn freeCasePlan(allocator: Allocator, plan: *CasePlan) void {
    for (plan.branches) |br| freeBranchSrc(allocator, br.then_src);
    if (plan.else_src) |es| freeBranchSrc(allocator, es);
    if (plan.output_owned) {
        plan.output.deinit(allocator);
        allocator.destroy(plan.output);
    }
}

/// Recursively resolve an Expr into a CallPlan. The Expr must be a
/// `.call` at the entry point; nested args may themselves be calls,
/// literals, or column refs.
///
/// Every CallPlan — root or internal — owns its output ColumnStore.
/// At eval time the operator memcpy's the root's output into the
/// derived_cols slot. Keeps the resolver shape simple at the cost of
/// one bulk copy per derived column per batch (cheap relative to
/// kernel work).
fn buildCallPlan(
    runtime_allocator: Allocator,
    aa: Allocator,
    expr: Expr,
    up_schema: []const Column,
) !*CallPlan {
    const c = switch (expr) {
        .call => |x| x,
        else => return Error.ComputeUnsupportedExpr,
    };

    const arg_plans = try aa.alloc(ArgPlan, c.args.len);
    const arg_types = try aa.alloc(Type, c.args.len);
    for (c.args, 0..) |arg, i| {
        switch (arg) {
            .col_ref => |name| {
                const idx = columnIndex(up_schema, name) orelse return Error.ComputeUnsupportedExpr;
                arg_plans[i] = .{ .col = idx };
                arg_types[i] = up_schema[idx].type;
            },
            .lit => |v| {
                const slot = try aa.create(LitSlot);
                slot.* = .{
                    .value = v,
                    .buf = try ColumnStore.init(runtime_allocator, literalType(v), false),
                };
                arg_plans[i] = .{ .lit = slot };
                arg_types[i] = literalType(v);
            },
            .call => {
                const sub = try buildCallPlan(runtime_allocator, aa, arg, up_schema);
                arg_plans[i] = .{ .call = sub };
                arg_types[i] = sub.output_type;
            },
            .case => return Error.ComputeUnsupportedExpr,
            .scalar_subquery, .exists_subquery, .var_ref => return Error.ComputeUnsupportedExpr,
        }
    }

    const r = (try scalar_fn.resolve(aa, c.fn_name, arg_types)) orelse return Error.ComputeNoSuchOverload;

    // Cast scratch buffers (one per coerced arg).
    var cast_buffers: ?[]?ColumnStore = null;
    if (r.arg_casts) |casts| {
        const buffers = try runtime_allocator.alloc(?ColumnStore, casts.len);
        for (casts, r.func.arg_types, arg_plans, buffers) |k, declared, ap, *slot| {
            if (k == null) {
                slot.* = null;
                continue;
            }
            const src_nullable = switch (ap) {
                .col => |idx| up_schema[idx].nullable,
                .lit => false,
                // Sub-call outputs are nullable (allocated below).
                .call => true,
            };
            slot.* = try ColumnStore.init(runtime_allocator, declared, src_nullable);
        }
        cast_buffers = buffers;
    }

    // Own a nullable output ColumnStore so the next level up's null
    // propagation can see the correct validity bits.
    const output_buf = try runtime_allocator.create(ColumnStore);
    output_buf.* = try ColumnStore.init(runtime_allocator, r.func.return_type, true);

    const plan = try aa.create(CallPlan);
    plan.* = .{
        .func = r.func,
        .args = arg_plans,
        .arg_casts = r.arg_casts,
        .cast_buffers = cast_buffers,
        .output = output_buf,
        .output_owned = true,
        .output_type = r.func.return_type,
    };
    return plan;
}

/// Walk a CallPlan and release every runtime-allocated buffer
/// (cast scratches, owned outputs, recursive sub-calls' buffers).
/// Called from Compute.deinit. The CallPlan struct itself lives in
/// the arena and is freed there.
fn freeCallPlan(runtime_allocator: Allocator, plan: *CallPlan) void {
    for (plan.args) |arg| switch (arg) {
        .col => {},
        .lit => |slot| slot.buf.deinit(runtime_allocator),
        .call => |sub| freeCallPlan(runtime_allocator, sub),
    };
    if (plan.cast_buffers) |buffers| {
        for (buffers) |*slot| if (slot.*) |*cs| cs.deinit(runtime_allocator);
        runtime_allocator.free(buffers);
    }
    if (plan.output_owned) {
        plan.output.deinit(runtime_allocator);
        runtime_allocator.destroy(plan.output);
    }
}

/// Infer the ColumnStore-compatible Type for a literal Value. Mirrors
/// the active union tag — int literals stay int (not promoted to bigint);
/// promotion happens via the existing implicit-cast machinery if the
/// resolved overload requires it.
fn literalType(v: types.Value) Type {
    return switch (v) {
        .int => .int,
        .bigint => .bigint,
        .smallint => .smallint,
        .tinyint => .tinyint,
        .largeint => .largeint,
        .float => .float,
        .double => .double,
        .boolean => .boolean,
        .text => .string,
        .date => .date,
        .datetime => .datetime,
        // Decimal Values carry just the raw int payload (no precision/
        // scale). Compute can't materialize a typed decimal column
        // without those — caller must explicitly cast / project.
        .decimal64, .decimal128 => @panic("Compute: decimal literal args not supported"),
        .uuid => .uuid,
    };
}

fn columnIndex(schema: []const Column, name: []const u8) ?usize {
    return types.findColumn(schema, name);
}

/// Provable output stat for one derived column, combining its plan-time
/// `StatClass` with the live upstream per-column stats. Renames pass the
/// source stat through unchanged (the kind, not the class, carries the index).
fn derivedColStat(d: ResolvedDerived, up_stats: []const exec.ColStat) exec.ColStat {
    if (d.kind == .rename) {
        const rn = d.kind.rename;
        return if (rn.src_idx < up_stats.len) up_stats[rn.src_idx] else .{ .ndv = .unknown };
    }
    return switch (d.stat_class) {
        .none => .{ .ndv = .unknown },
        .literal => |v| .{ .ndv = .{ .exact = 1 }, .min = v, .max = v },
        .unary => |u| blk: {
            if (u.src_idx >= up_stats.len) break :blk .{ .ndv = .unknown };
            const src = up_stats[u.src_idx];
            // f(col): ndv ≤ NDV(col) (pigeonhole). Affine ⇒ flow the range,
            // then clamp to the output width (wrapping kernels make an
            // escaping bound unprovable).
            if (u.affine) |aff| {
                const r = affineRange(aff, src.min, src.max);
                const p = provableRange(d.output_type, r.min, r.max);
                break :blk .{ .ndv = src.ndv, .min = p.min, .max = p.max };
            }
            break :blk .{ .ndv = src.ndv };
        },
        .binary => |bn| blk: {
            if (bn.src1 >= up_stats.len or bn.src2 >= up_stats.len) break :blk .{ .ndv = .unknown };
            const s1 = up_stats[bn.src1];
            const s2 = up_stats[bn.src2];
            // ndv ≤ NDV1·NDV2 (saturating product; unknown if either is).
            const ndv: exec.ColCard = switch (s1.ndv) {
                .unknown => .unknown,
                .exact => |n1| switch (s2.ndv) {
                    .unknown => .unknown,
                    .exact => |n2| .{ .exact = n1 *| n2 },
                },
            };
            // min/max for + and - via interval arithmetic, clamped to the
            // output width (same wrapping-kernel caveat as the affine case).
            var min: ?i128 = null;
            var max: ?i128 = null;
            if (bn.op) |op| {
                if (s1.min != null and s1.max != null and s2.min != null and s2.max != null) {
                    switch (op) {
                        .add => {
                            min = addChecked(s1.min.?, s2.min.?);
                            max = addChecked(s1.max.?, s2.max.?);
                        },
                        .sub => {
                            min = subChecked(s1.min.?, s2.max.?);
                            max = subChecked(s1.max.?, s2.min.?);
                        },
                        .mul => {},
                    }
                }
            }
            const p = provableRange(d.output_type, min, max);
            break :blk .{ .ndv = ndv, .min = p.min, .max = p.max };
        },
    };
}

fn derivedNullable(r: ResolvedDerived, up_schema: []const Column) bool {
    return switch (r.kind) {
        .rename => |rn| up_schema[rn.src_idx].nullable,
        .lit_only => false, // literal-only derived: constant column, never null
        .call => |plan| callPlanNullable(plan, up_schema),
        .case => |plan| plan.may_produce_null,
        .fused_scalar => false, // only built for non-nullable col + const
    };
}

/// Walks a CallPlan tree and reports whether the result column is
/// nullable. Conservative: any null-producing path makes the column
/// nullable. Mirrors the eval-time null bookkeeping decision.
fn callPlanNullable(plan: *CallPlan, up_schema: []const Column) bool {
    switch (plan.func.null_strategy) {
        .absorbs, .kernel_managed => return true,
        .propagates => {},
    }
    for (plan.args) |arg| switch (arg) {
        .col => |idx| if (up_schema[idx].nullable) return true,
        .lit => {},
        .call => |sub| if (callPlanNullable(sub, up_schema)) return true,
    };
    return false;
}

// ---------------------------------------------------------------------------
// Null bookkeeping
// ---------------------------------------------------------------------------

fn writePropagatedNulls(
    allocator: Allocator,
    out: *ColumnStore,
    arg_views: []const ColumnView,
    n: usize,
) !void {
    // Output row i valid iff ALL arg rows i are valid.
    const base = out.data.rowCount() - n;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var valid = true;
        for (arg_views) |v| {
            if (!v.isValid(i)) {
                valid = false;
                break;
            }
        }
        try out.appendValidBit(allocator, base + i, valid);
    }
}

fn writeAbsorbedNulls(
    allocator: Allocator,
    out: *ColumnStore,
    arg_views: []const ColumnView,
    n: usize,
) !void {
    // Default rule for absorbs functions: output null iff ALL args
    // are null. (Matches coalesce semantics — first non-null wins.)
    const base = out.data.rowCount() - n;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var any_valid = false;
        for (arg_views) |v| {
            if (v.isValid(i)) {
                any_valid = true;
                break;
            }
        }
        try out.appendValidBit(allocator, base + i, any_valid);
    }
}

// ---------------------------------------------------------------------------
// Rename helper
// ---------------------------------------------------------------------------

fn appendCopiedColumn(
    allocator: Allocator,
    out: *ColumnStore,
    src: ColumnView,
    n: usize,
) !void {
    // Bulk-copy via existing memtable.transform helper. Same op as
    // appendAllColumn used by Sort/Filter for output staging.
    try @import("../engine/transform.zig").appendAllColumn(allocator, src, out);
    _ = n;
}

/// Append the single cell at `src_idx` from `src` into `dst`. Used by
/// the CASE evaluator to assemble the per-row winning value. `src`
/// and `dst` must share the same primitive type (validated at resolve
/// time for CASE branches).
fn appendCellFromView(
    allocator: Allocator,
    dst: *ColumnStore,
    src: ColumnView,
    src_idx: usize,
) !void {
    switch (dst.data) {
        .int => |*l| try l.append(allocator, src.data.int[src_idx]),
        .bigint => |*l| try l.append(allocator, src.data.bigint[src_idx]),
        .smallint => |*l| try l.append(allocator, src.data.smallint[src_idx]),
        .tinyint => |*l| try l.append(allocator, src.data.tinyint[src_idx]),
        .largeint => |*l| try l.append(allocator, src.data.largeint[src_idx]),
        .float => |*l| try l.append(allocator, src.data.float[src_idx]),
        .double => |*l| try l.append(allocator, src.data.double[src_idx]),
        .boolean => |*l| try l.append(allocator, src.data.boolean[src_idx]),
        .date => |*l| try l.append(allocator, src.data.date[src_idx]),
        .datetime => |*l| try l.append(allocator, src.data.datetime[src_idx]),
        .decimal64 => |*l| try l.append(allocator, src.data.decimal64[src_idx]),
        .decimal128 => |*l| try l.append(allocator, src.data.decimal128[src_idx]),
        .uuid => |*l| try l.append(allocator, src.data.uuid[src_idx]),
        .varchar => |*s| {
            const bytes = switch (src.data) {
                .varchar => |sv| sv.rowBytes(src_idx),
                .string => |sv| sv.rowBytes(src_idx),
                .char => |sv| sv.rowBytes(src_idx),
                else => unreachable,
            };
            try s.appendValue(allocator, bytes);
        },
        .string => |*s| {
            const bytes = switch (src.data) {
                .varchar => |sv| sv.rowBytes(src_idx),
                .string => |sv| sv.rowBytes(src_idx),
                .char => |sv| sv.rowBytes(src_idx),
                else => unreachable,
            };
            try s.appendValue(allocator, bytes);
        },
        .char => |*s| {
            const bytes = switch (src.data) {
                .varchar => |sv| sv.rowBytes(src_idx),
                .string => |sv| sv.rowBytes(src_idx),
                .char => |sv| sv.rowBytes(src_idx),
                else => unreachable,
            };
            try s.appendValue(allocator, bytes);
        },
    }
    if (dst.nulls != null) {
        const row = dst.data.rowCount() - 1;
        try dst.appendValidBit(allocator, row, src.isValid(src_idx));
    }
}

/// Append `n` copies of `v` into `buf`. Used by Compute's call path to
/// materialize a constant-valued column matching the current batch's
/// row count, so scalar kernels see uniform-width arg slices.
fn fillLiteralColumn(allocator: Allocator, buf: *ColumnStore, v: types.Value, n: usize) !void {
    var i: usize = 0;
    switch (v) {
        .int => |x| while (i < n) : (i += 1) try buf.data.int.append(allocator, x),
        .bigint => |x| while (i < n) : (i += 1) try buf.data.bigint.append(allocator, x),
        .smallint => |x| while (i < n) : (i += 1) try buf.data.smallint.append(allocator, x),
        .tinyint => |x| while (i < n) : (i += 1) try buf.data.tinyint.append(allocator, x),
        .largeint => |x| while (i < n) : (i += 1) try buf.data.largeint.append(allocator, x),
        .float => |x| while (i < n) : (i += 1) try buf.data.float.append(allocator, x),
        .double => |x| while (i < n) : (i += 1) try buf.data.double.append(allocator, x),
        .boolean => |x| while (i < n) : (i += 1) try buf.data.boolean.append(allocator, @intFromBool(x)),
        .text => |s| while (i < n) : (i += 1) try buf.data.string.appendValue(allocator, s),
        .date => |x| while (i < n) : (i += 1) try buf.data.date.append(allocator, x),
        .datetime => |x| while (i < n) : (i += 1) try buf.data.datetime.append(allocator, x),
        .decimal64, .decimal128 => unreachable, // literalType() panics before we get here
        .uuid => |x| while (i < n) : (i += 1) try buf.data.uuid.append(allocator, x),
    }
}
