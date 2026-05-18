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
const scalar_fn = @import("scalar_fn.zig");
const ScalarFn = scalar_fn.ScalarFn;
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

/// Resolved per-derived plan: rename, literal-only (constant column),
/// or a function-call tree.
const ResolvedDerived = struct {
    name: []const u8,
    output_type: Type,
    kind: union(enum) {
        rename: struct { src_idx: usize },
        lit_only: *LitSlot,
        call: *CallPlan,
    },
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
                if (std.mem.eql(u8, uc.name, r.name)) return Error.ComputeNameCollision;
            }
            for (resolved[0..i]) |prior| {
                if (std.mem.eql(u8, prior.name, r.name)) return Error.ComputeNameCollision;
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
                .rename => {},
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
    pub fn stats(self: *Compute) exec.PipelineStats {
        return self.upstream.stats();
    }

    pub fn accountant(self: *Compute) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
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
};

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
                .kind = .{ .lit_only = slot },
            };
        },
        .call => {
            const plan = try buildCallPlan(runtime_allocator, aa, d.expr, up_schema);
            return .{
                .name = try aa.dupe(u8, d.name),
                .output_type = plan.output_type,
                .kind = .{ .call = plan },
            };
        },
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
    for (schema, 0..) |c, i| {
        if (std.mem.eql(u8, c.name, name)) return i;
    }
    return null;
}

fn derivedNullable(r: ResolvedDerived, up_schema: []const Column) bool {
    return switch (r.kind) {
        .rename => |rn| up_schema[rn.src_idx].nullable,
        .lit_only => false, // literal-only derived: constant column, never null
        .call => |plan| callPlanNullable(plan, up_schema),
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
