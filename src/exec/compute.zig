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
//!     `.zero_divisor` → `.propagates`, and also null where the divisor is 0.
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
const scalar_common = @import("scalar_fn_common.zig");
const ScalarFn = scalar_fn.ScalarFn;
const simd = @import("../util/simd.zig");
const udf_mod = @import("../udf.zig");

const cast = @import("cast.zig");
const CastKernel = cast.CastKernel;

const exec = @import("exec.zig");
const getenv_cp = @extern(*const fn (name: [*:0]const u8) callconv(.c) ?[*:0]const u8, .{ .name = "getenv", .library_name = "c" });
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

fn appendUniqueName(allocator: Allocator, out: *std.ArrayListUnmanaged([]const u8), name: []const u8) !void {
    if (name.len == 0) return;
    for (out.items) |existing| if (types.columnNameEql(existing, name)) return;
    try out.append(allocator, name);
}

/// Append every base column name referenced by `e` into `out`, deduplicated.
/// Recurses through call arguments and CASE branches (including branch
/// predicates). Used to compute the scan projection that must back a set of
/// derived columns: a derived expression can only be evaluated if every column
/// it reads is projected by the upstream scan.
pub fn collectColumnRefs(allocator: Allocator, out: *std.ArrayListUnmanaged([]const u8), e: Expr) !void {
    switch (e) {
        .col_ref => |nm| try appendUniqueName(allocator, out, nm),
        .call => |c| for (c.args) |arg| try collectColumnRefs(allocator, out, arg),
        .case => |cs| {
            for (cs.branches) |b| {
                try collectPredicateColumnRefs(allocator, out, b.cond);
                try collectColumnRefs(allocator, out, b.then);
            }
            if (cs.else_branch) |eb| try collectColumnRefs(allocator, out, eb.*);
        },
        else => {},
    }
}

/// Companion to `collectColumnRefs` for the predicate trees inside CASE
/// conditions (and any other predicate whose columns must be projected).
pub fn collectPredicateColumnRefs(allocator: Allocator, out: *std.ArrayListUnmanaged([]const u8), p: PredicateExpr) !void {
    switch (p) {
        .leaf => |l| try appendUniqueName(allocator, out, l.col),
        .day_leaf => |l| try appendUniqueName(allocator, out, l.col),
        .leaf_col_col => |c| {
            try appendUniqueName(allocator, out, c.left);
            try appendUniqueName(allocator, out, c.right);
        },
        .is_null, .is_not_null => |nm| try appendUniqueName(allocator, out, nm),
        .like => |lk| try appendUniqueName(allocator, out, lk.col),
        .in_set => |s| try appendUniqueName(allocator, out, s.col),
        .@"and", .@"or" => |kids| for (kids) |k| try collectPredicateColumnRefs(allocator, out, k),
        .not => |k| try collectPredicateColumnRefs(allocator, out, k.*),
        else => {},
    }
}

/// One argument to a function call inside the resolved expression
/// tree. Args can be: an upstream column reference, a literal (which
/// materializes as a per-batch replicated constant column), or a
/// nested function call (which evaluates recursively).
const ArgPlan = union(enum) {
    col: usize,
    lit: *LitSlot,
    null_lit: *NullSlot,
    call: *CallPlan,
    case: *CasePlan,
};

/// Per-literal scratch — typed ColumnStore refilled each batch with
/// `row_count` copies of `value`.
const LitSlot = struct {
    value: types.Value,
    /// Kept beside the value because decimal Values carry only the raw
    /// scaled payload — literalType() can't recover precision/scale after
    /// a plan-time literal coercion (CASE `ELSE 0` against a decimal arm).
    ty: Type,
    buf: ColumnStore,
};

/// Per-typed-NULL scratch, refilled each batch with invalid placeholders.
const NullSlot = struct {
    ty: Type,
    buf: ColumnStore,
};

const PlanError = Allocator.Error || Error;

/// Resolved call node: ScalarFn + per-arg evaluation plan + optional
/// coercion machinery + output buffer. Roots of derived columns
/// alias `Compute.derived_cols[i]` as their output (avoids one copy);
/// internal nodes own their scratch.
const CallPlan = struct {
    func: ScalarFn,
    args: []ArgPlan,
    /// The runtime arg `Type`s (with `DecimalSpec`), captured at resolve. Only
    /// read by the `typed_kernel` path — decimal kernels need the operand
    /// scales the bare `ColumnView`s don't carry.
    arg_runtime_types: []const Type,
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
        null_only: Type,
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
    cast_kernel: ?CastKernel = null,
    cast_buf: ?*ColumnStore = null,
};

/// Materialization source for a CASE branch's THEN (and ELSE) clause.
/// CASE's branches don't support nested CASE in v1 — keeps the resolve
/// + free trees finite without an extra dimension.
const BranchSrc = union(enum) {
    col: usize,
    lit: *LitSlot,
    null_lit: *NullSlot,
    call: *CallPlan,
    case: *CasePlan,
};

const CasePlan = struct {
    branches: []const CaseBranch,
    else_src: ?BranchSrc,
    else_cast_kernel: ?CastKernel = null,
    else_cast_buf: ?*ColumnStore = null,
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

const MAX_CASE_BRANCHES: usize = 16;

/// One CASE branch's THEN (or the ELSE) value for the current batch. A
/// scalar source holds a single row that every row it wins reads.
const CaseSrc = struct {
    view: ColumnView,
    scalar: bool,

    fn row(self: CaseSrc, i: usize) usize {
        return if (self.scalar) 0 else i;
    }
};

/// Schema-only upstream for detached per-chunk Compute clones inside a probe
/// pipeline (evalBatch is the only entry used; next() is never pulled).
const SchemaStub = struct {
    schema: []const Column,
    pub fn next(_: *SchemaStub) !?Batch {
        return null;
    }
    pub fn deinit(_: *SchemaStub) void {}
    pub fn outputSchema(self: *SchemaStub) []const Column {
        return self.schema;
    }
    pub fn addPrune(_: *SchemaStub, _: Predicate) !void {}
    pub fn stats(_: *SchemaStub) exec.PipelineStats {
        return .{ .upper_rows = std.math.maxInt(u64) };
    }
    pub fn accountant(_: *SchemaStub) ?*exec.memory.MemoryAccountant {
        return null;
    }
    pub fn explain(_: *SchemaStub, _: *std.ArrayList(u8), _: std.mem.Allocator, _: usize) !void {}
};

/// Probe-offer forwarding state: the wrapped sink a chained Compute hands
/// its upstream. Each chunk evaluates through its own detached clone (own
/// arena, plans, scratch — expression IR is shared read-only, matching the
/// thread-safety contract fused computes already rely on) before the inner
/// (join) sink processes the batch.
const ChainForward = struct {
    src: *Compute,
    /// The sink above (a join's), or null for a TERMINAL push: a compute
    /// with nothing above it in the pipeline evaluates per chunk and emits
    /// the derived batch directly. A later upper join UPGRADES a terminal
    /// push by rechaining its sink through this Compute (tryFuseProbe).
    inner: ?exec.ProbeSink,
    /// Upstream schema snapshotted at offer time — the scan re-types its
    /// out_schema on accept, so it can't be re-read at bind time.
    in_schema: []const Column,
    per_chunk: []Query = &.{},
    stubs: []*SchemaStub = &.{},
    /// Per-chunk remap scratch when the inner sink carries a probe_map (a
    /// Project narrowed this Compute's output before the join compiled
    /// against it): the derived batch remaps into these views post-eval.
    map_views: [][]ColumnView = &.{},
    bind_alloc: Allocator = undefined,

    fn bindHook(ctx: *anyopaque, n_chunks: usize, alloc: Allocator) anyerror!void {
        const cf: *ChainForward = @ptrCast(@alignCast(ctx));
        // Grow-only (see Join.sinkBind): a SetUnion forwards one sink to both
        // arms, so this can bind twice at compile time. Keep the larger clone
        // set; still forward to the inner sink (itself grow-only).
        if (cf.per_chunk.len >= n_chunks) {
            if (cf.inner) |inner| try inner.bind(inner.ctx, n_chunks, alloc);
            return;
        }
        if (cf.per_chunk.len > 0) {
            for (cf.per_chunk, cf.stubs) |*q, st| {
                q.deinit();
                cf.bind_alloc.destroy(st);
            }
            cf.bind_alloc.free(cf.per_chunk);
            cf.bind_alloc.free(cf.stubs);
            cf.per_chunk = &.{};
            cf.stubs = &.{};
            for (cf.map_views) |v| cf.bind_alloc.free(v);
            if (cf.map_views.len > 0) cf.bind_alloc.free(cf.map_views);
            cf.map_views = &.{};
        }
        cf.bind_alloc = alloc;
        const t_bind = exec.prof.nowTicks();
        defer exec.prof.addPhase("compute.chain.bind_clones", @intCast(exec.prof.nowTicks() - t_bind));
        const qs = try alloc.alloc(Query, n_chunks);
        errdefer alloc.free(qs);
        const stubs = try alloc.alloc(*SchemaStub, n_chunks);
        errdefer alloc.free(stubs);
        var built: usize = 0;
        errdefer for (qs[0..built], stubs[0..built]) |*q, st| {
            q.deinit();
            alloc.destroy(st);
        };
        for (qs, stubs) |*q, *st| {
            const stub = try alloc.create(SchemaStub);
            errdefer alloc.destroy(stub);
            stub.* = .{ .schema = cf.in_schema };
            q.* = try Compute.createWithRegistry(alloc, makeQuery(alloc, stub), cf.src.derived_ir, cf.src.udf_registry);
            st.* = stub;
            built += 1;
        }
        cf.per_chunk = qs;
        cf.stubs = stubs;
        if (if (cf.inner) |inner| inner.probe_map else null) |m| {
            const mv = try alloc.alloc([]ColumnView, n_chunks);
            var mbuilt: usize = 0;
            errdefer {
                for (mv[0..mbuilt]) |v| alloc.free(v);
                alloc.free(mv);
            }
            for (mv) |*v| {
                v.* = try alloc.alloc(ColumnView, m.len);
                mbuilt += 1;
            }
            cf.map_views = mv;
        }
        if (cf.inner) |inner| try inner.bind(inner.ctx, n_chunks, alloc);
    }

    fn processHook(ctx: *anyopaque, chunk: usize, batch: Batch) anyerror!?Batch {
        const cf: *ChainForward = @ptrCast(@alignCast(ctx));
        const c = exec.queryAs(Compute, cf.per_chunk[chunk]).?;
        var out = try c.evalBatch(batch);
        const inner = cf.inner orelse return out;
        if (inner.probe_map) |m| {
            const vs = cf.map_views[chunk];
            for (m, vs) |src, *v| v.* = out.values[src];
            out = .{ .schema = out.schema, .values = vs, .row_count = out.row_count };
        }
        return try inner.process(inner.ctx, chunk, out);
    }

    fn deinitAll(cf: *ChainForward, owner_alloc: Allocator) void {
        const t_free = exec.prof.nowTicks();
        defer exec.prof.addPhase("compute.chain.deinit_clones", @intCast(exec.prof.nowTicks() - t_free));
        for (cf.per_chunk, cf.stubs) |*q, st| {
            q.deinit();
            cf.bind_alloc.destroy(st);
        }
        if (cf.per_chunk.len > 0) {
            cf.bind_alloc.free(cf.per_chunk);
            cf.bind_alloc.free(cf.stubs);
        }
        for (cf.map_views) |v| cf.bind_alloc.free(v);
        if (cf.map_views.len > 0) cf.bind_alloc.free(cf.map_views);
        owner_alloc.destroy(cf);
    }
};

pub const Compute = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    upstream: Query,

    derived: []ResolvedDerived,
    /// One ColumnStore per derived column. Cleared + refilled each
    /// `next()` from the upstream batch.
    derived_cols: []ColumnStore,
    /// Per-derived flag set each evalBatch: the output view aliases the
    /// producer's buffer directly (renamed upstream column, literal slot,
    /// call/case root output) instead of a copy into `derived_cols`. Same
    /// lifetime either way — valid until the next call on this instance.
    derived_direct: []bool,
    /// For each derived column, the output slot it writes into (append
    /// position for fresh names, the matched upstream index for renames).
    derived_output_indices: []usize,

    /// Combined output schema. Fresh derived names append; derived names
    /// matching an upstream column replace that upstream output slot.
    output_schema: []Column,
    /// Width of the upstream schema the derived columns were resolved
    /// against. The live upstream width can differ later: probe fusion
    /// re-types the pipeline below, and a chained operator then reports the
    /// joined output schema instead of the one this Compute was built on.
    in_width: usize,
    /// Reusable views slice (upstream views + derived views), sized at
    /// create. Rewired per batch.
    views: []ColumnView,
    /// Raw derived IR + registry, retained so tryFuseProbe can build
    /// detached per-chunk clones for a probe pipeline and the join stage
    /// peel can recognise in-place casts. Deep-copied into the arena:
    /// callers build `derived` in scratch that dies once their own create
    /// returns (a join's key-coercion casts), so nothing here may alias it.
    derived_ir: []const Derived,
    udf_registry: ?*const udf_mod.UdfRegistry,
    /// Set when this Compute forwarded a probe offer downward: evaluation
    /// happens in per-chunk clones inside the scan workers, and this
    /// operator passes the final joined batches through untouched.
    chain: ?*ChainForward = null,
    /// The upstream is the Compute for the earlier entries of the list this
    /// one was created from, so `evalBatch` runs it first.
    layered_on_upstream: bool = false,

    pub fn create(
        allocator: Allocator,
        upstream: Query,
        derived: []const Derived,
    ) !Query {
        return createWithRegistry(allocator, upstream, derived, null);
    }

    /// An entry of `derived` may read a column an earlier entry defines
    /// (`WHERE (CASE WHEN a + 1 > 2 THEN ...) = 1` anchors `a + 1` and the
    /// CASE that compares it in one list). Each entry resolves against its
    /// input only, so the list evaluates as a stack of Computes, one per
    /// layer of those reads.
    pub fn createWithRegistry(
        allocator: Allocator,
        upstream: Query,
        derived: []const Derived,
        udf_registry: ?*const udf_mod.UdfRegistry,
    ) !Query {
        return createLayer(allocator, upstream, derived, udf_registry, false);
    }

    fn createLayer(
        allocator: Allocator,
        upstream: Query,
        derived: []const Derived,
        udf_registry: ?*const udf_mod.UdfRegistry,
        layered_on_upstream: bool,
    ) !Query {
        if (derived.len == 0) return Error.ComputeNoColumns;
        const up_schema = upstream.outputSchema();
        if (try siblingReadIndex(allocator, derived, up_schema)) |split| {
            const lower = try createLayer(allocator, upstream, derived[0..split], udf_registry, layered_on_upstream);
            // On failure the caller still owns `upstream`.
            errdefer exec.queryAs(Compute, lower).?.deinitLayer();
            return try createLayer(allocator, lower, derived[split..], udf_registry, true);
        }

        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const aa = arena.allocator();
        const derived_ir = try aa.alloc(Derived, derived.len);
        for (derived, derived_ir) |d, *owned| {
            owned.* = .{ .name = try aa.dupe(u8, d.name), .expr = try expr_mod.deepClone(aa, d.expr) };
        }

        const resolved = try aa.alloc(ResolvedDerived, derived.len);
        var resolved_count: usize = 0;
        errdefer for (resolved[0..resolved_count]) |r| freeResolvedDerived(allocator, r);
        for (derived, resolved) |d, *r| {
            r.* = try resolveDerived(allocator, aa, d, up_schema, udf_registry);
            resolved_count += 1;
        }

        // Validate no duplicate derived names. Matching an upstream column
        // name is allowed and means "replace that output slot".
        for (resolved, 0..) |r, i| {
            for (resolved[0..i]) |prior| {
                if (@import("../types.zig").columnNameEql(prior.name, r.name)) return Error.ComputeNameCollision;
            }
        }

        // Map each derived column to its output slot: a name matching an
        // upstream column replaces that slot; a fresh name appends.
        const derived_output_indices = try allocator.alloc(usize, resolved.len);
        errdefer allocator.free(derived_output_indices);
        var append_count: usize = 0;
        for (resolved, derived_output_indices) |r, *out_idx| {
            if (columnIndex(up_schema, r.name)) |idx| {
                out_idx.* = idx;
            } else {
                out_idx.* = up_schema.len + append_count;
                append_count += 1;
            }
        }

        const output_schema = try allocator.alloc(Column, up_schema.len + append_count);
        errdefer allocator.free(output_schema);
        for (up_schema, 0..) |c, i| output_schema[i] = c;
        for (resolved, derived_output_indices) |r, out_idx| {
            // Nullable: if propagates AND any input column is nullable
            // → derived is nullable. If absorbs → also nullable (the
            // function can still produce null if all inputs are null).
            // Renames inherit nullability from the source.
            const nullable = derivedNullable(r, up_schema);
            output_schema[out_idx] = .{ .name = r.name, .type = r.output_type, .nullable = nullable };
        }

        // One ColumnStore per derived column. Re-initialized each batch
        // is overkill; instead clearRetainingCapacity at the top of
        // next(). We still rebuild the validity bitmap fresh per batch.
        const derived_cols = try allocator.alloc(ColumnStore, resolved.len);
        errdefer allocator.free(derived_cols);
        var inited: usize = 0;
        errdefer for (derived_cols[0..inited]) |*c| c.deinit(allocator);
        for (resolved, derived_cols, 0..) |r, *col, idx| {
            const out_col = output_schema[derived_output_indices[idx]];
            col.* = try ColumnStore.init(allocator, r.output_type, out_col.nullable);
            inited += 1;
        }

        const views = try allocator.alloc(ColumnView, output_schema.len);
        errdefer allocator.free(views);
        const derived_direct = try allocator.alloc(bool, resolved.len);
        errdefer allocator.free(derived_direct);
        @memset(derived_direct, false);

        const self = try allocator.create(Compute);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .arena = arena,
            .upstream = upstream,
            .derived = resolved,
            .derived_cols = derived_cols,
            .derived_direct = derived_direct,
            .derived_output_indices = derived_output_indices,
            .output_schema = output_schema,
            .in_width = up_schema.len,
            .views = views,
            .derived_ir = derived_ir,
            .udf_registry = udf_registry,
            .layered_on_upstream = layered_on_upstream,
        };
        return makeQuery(allocator, self);
    }

    /// Where `derived` splits into layers: the first entry that reads a
    /// column its input lacks but an earlier entry defines. None past an
    /// entry that replaces an input column, whose later readers must see
    /// the input's value.
    fn siblingReadIndex(allocator: Allocator, derived: []const Derived, up_schema: []const Column) !?usize {
        var refs: std.ArrayListUnmanaged([]const u8) = .empty;
        defer refs.deinit(allocator);
        for (derived, 0..) |d, i| {
            refs.clearRetainingCapacity();
            try collectColumnRefs(allocator, &refs, d.expr);
            for (refs.items) |r| {
                if (columnIndex(up_schema, r) != null) continue;
                for (derived[0..i]) |prior| {
                    if (types.columnNameEql(prior.name, r)) return i;
                }
            }
            if (columnIndex(up_schema, d.name) != null) return null;
        }
        return null;
    }

    pub fn deinit(self: *Compute) void {
        if (self.chain) |cf| cf.deinitAll(self.allocator);
        var up = self.upstream;
        up.deinit();
        self.deinitLayer();
    }

    /// Frees this operator but not its upstream.
    fn deinitLayer(self: *Compute) void {
        for (self.derived_cols) |*c| c.deinit(self.allocator);
        self.allocator.free(self.derived_cols);
        self.allocator.free(self.derived_direct);
        self.allocator.free(self.derived_output_indices);
        for (self.derived) |r| freeResolvedDerived(self.allocator, r);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.views);
        self.arena.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Compute) []const Column {
        // Chained: batches passing through are the probe pipeline's final
        // joined output — report the live schema from below.
        if (self.chain != null) return self.upstream.outputSchema();
        return self.output_schema;
    }

    pub fn addPrune(self: *Compute, pred: Predicate) !void {
        if (self.chain != null) return;
        const idx = types.findColumn(self.output_schema, pred.col) orelse return Error.ColumnNotFound;
        var src_idx = idx;
        for (self.derived, self.derived_output_indices) |derived, out_idx| {
            if (out_idx != idx) continue;
            switch (derived.kind) {
                .rename => |rename| src_idx = rename.src_idx,
                else => return,
            }
            break;
        }
        var rewritten = pred;
        rewritten.col = self.upstream.outputSchema()[src_idx].name;
        return self.upstream.addPrune(rewritten);
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
        // Join-chained: batches arriving here are the UPPER join's output —
        // per-derived stat extension would mis-index against that schema.
        // Terminal-chained (inner == null at push time): the pipeline was
        // re-typed to OUR output schema, so the normal extension below is
        // exactly right — dropping it starves downstream GROUP BY routing
        // of derived-key NDVs.
        if (self.chain) |cf| {
            if (cf.inner != null) return self.upstream.stats();
        }
        var up = self.upstream.stats();
        const in_width = self.in_width;
        // The upstream's per-column stats index ITS live output schema. When
        // that schema is no longer the one this Compute was resolved against
        // (the pipeline below was re-typed by probe fusion after this
        // operator was built), those stats describe other columns: treat
        // them as unknown instead of copying them into the wrong slots — or
        // past the end of `out_stats` when the live schema is wider.
        const aligned: []const exec.ColStat =
            if (self.upstream.outputSchema().len == in_width) up.column_stats else &.{};
        const out_stats = self.arena.allocator().alloc(exec.ColStat, self.output_schema.len) catch return up;
        // Align to the OUTPUT schema: copy the upstream stats we have, padding
        // any the upstream didn't report (a short/empty array) with unknown so
        // indices line up. Then extend with the derived columns. Bailing here
        // instead would leave column_stats shorter than the schema, so a
        // derived GROUP BY key reads out-of-bounds → unknown → the router
        // mis-sizes the hash table (the Q28 regex-key sort regression).
        for (out_stats[0..in_width], 0..) |*s, i| {
            s.* = if (i < aligned.len) aligned[i] else .{ .ndv = .unknown };
        }
        for (out_stats[in_width..]) |*s| s.* = .{ .ndv = .unknown };
        for (self.derived, self.derived_output_indices) |d, out_idx| {
            out_stats[out_idx] = derivedColStat(d, out_stats[0..in_width]);
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

    /// A join above this Compute offers its probe sink. The derived columns
    /// belong BETWEEN the source's batches and the join, so forward a
    /// WRAPPED sink downward: each chunk evaluates through a detached clone
    /// of this Compute before the join's sink processes it. The whole
    /// chain then runs inside the scan's stripe workers, and this operator
    /// becomes a pass-through for the final joined batches.
    pub fn tryFuseProbe(self: *Compute, sink: exec.ProbeSink) !bool {
        const trace_jf = getenv_cp("THINDB_TRACE_JOINFUSE") != null;
        if (self.chain) |cf| {
            // Upgrade a TERMINAL push: the wrapper is already bound below;
            // rechain the upper sink to the bottom scan (bind + re-type),
            // then adopt it so the per-chunk evaluation feeds it.
            if (cf.inner != null) {
                if (trace_jf) std.debug.print("[jf]   compute decline: chain already has inner sink\n", .{});
                return false;
            }
            if (!(try self.upstream.rechainProbeSink(sink))) {
                if (trace_jf) std.debug.print("[jf]   compute decline: rechain refused below terminal push\n", .{});
                return false;
            }
            if (sink.probe_map) |m| {
                const mv = try cf.bind_alloc.alloc([]ColumnView, cf.per_chunk.len);
                errdefer cf.bind_alloc.free(mv);
                var mbuilt: usize = 0;
                errdefer for (mv[0..mbuilt]) |v| cf.bind_alloc.free(v);
                for (mv) |*v| {
                    v.* = try cf.bind_alloc.alloc(ColumnView, m.len);
                    mbuilt += 1;
                }
                cf.map_views = mv;
            }
            cf.inner = sink;
            return true;
        }
        const chain = try self.allocator.create(ChainForward);
        errdefer self.allocator.destroy(chain);
        chain.* = .{
            .src = self,
            .inner = sink,
            .in_schema = self.upstream.outputSchema(),
        };
        const ok = self.upstream.tryFuseProbe(.{
            .ctx = chain,
            .out_schema = sink.out_schema,
            .bind = ChainForward.bindHook,
            .process = ChainForward.processHook,
        }) catch false;
        if (!ok) {
            if (trace_jf) {
                var buf: std.ArrayList(u8) = .empty;
                defer buf.deinit(self.allocator);
                self.upstream.explain(&buf, self.allocator, 0) catch {};
                var it = std.mem.splitScalar(u8, buf.items, '\n');
                std.debug.print("[jf]   compute decline: upstream refused wrapped sink; subtree:\n", .{});
                var n: usize = 0;
                while (it.next()) |line| : (n += 1) {
                    if (n >= 12) break;
                    if (line.len > 0) std.debug.print("[jf]     {s}\n", .{line});
                }
            }
            self.allocator.destroy(chain);
            return false;
        }
        self.chain = chain;
        return true;
    }

    pub fn probeFusionReachable(self: *const Compute) bool {
        return self.upstream.probeFusionReachable();
    }

    pub fn rechainProbeSink(self: *Compute, sink: exec.ProbeSink) !bool {
        // Only a FULLY chained compute (inner sink adopted, or terminal at
        // the very tail being extended by the caller above) passes through.
        if (self.chain == null) return false;
        return self.upstream.rechainProbeSink(sink);
    }

    /// A chained compute is a pass-through — the pipeline below already
    /// emits the computed batches, so a partial-aggregate offer composes
    /// against those and belongs to the scan.
    pub fn tryFuseAggregate(self: *Compute, group_cols: []const []const u8, aggs: []const exec.AggSpec) !bool {
        if (self.chain == null) return false;
        return self.upstream.tryFuseAggregate(group_cols, aggs);
    }

    /// A filter that touches no derived column commutes with the compute —
    /// forward it below so the scan can evaluate (and prune on) it BEFORE
    /// the derivation work runs. Renames block the forward too: the source
    /// column still exists below, but under a different name than the
    /// predicate uses.
    pub fn tryFuseFilter(self: *Compute, expr: exec.predicate.PredicateExpr) !bool {
        if (self.chain != null) return false;
        for (self.derived_output_indices) |idx| {
            if (exec.predicate.touches_resolved_column(expr, self.output_schema, idx)) return false;
        }
        return self.upstream.tryFuseFilter(expr);
    }

    /// Terminal self-push: nothing above this Compute offers a probe sink,
    /// but the pipeline below may be a probe-fused parallel chain — push
    /// the derived evaluation into it as a terminal chained sink, so the
    /// stripe workers emit already-computed batches and this operator
    /// becomes a pass-through. A join created later upgrades the terminal
    /// wrapper via tryFuseProbe. Safe no-op when the upstream declines.
    pub fn tryFuseSelf(self: *Compute) bool {
        if (self.chain != null) return false;
        const chain = self.allocator.create(ChainForward) catch return false;
        chain.* = .{
            .src = self,
            .inner = null,
            .in_schema = self.upstream.outputSchema(),
        };
        const ok = self.upstream.tryFuseProbe(.{
            .ctx = chain,
            .out_schema = self.output_schema,
            .bind = ChainForward.bindHook,
            .process = ChainForward.processHook,
        }) catch false;
        if (!ok) {
            self.allocator.destroy(chain);
            return false;
        }
        self.chain = chain;
        return true;
    }

    pub fn next(self: *Compute) !?Batch {
        const in = (try self.upstream.next()) orelse return null;
        if (self.chain != null) return in;
        return try self.evalLayer(in);
    }

    /// Push-model entry for probe-pipeline chunk wrappers: evaluate the
    /// derived columns against a caller-supplied batch. The returned views
    /// live until the next call on this instance.
    pub fn evalBatch(self: *Compute, in: Batch) !Batch {
        if (!self.layered_on_upstream) return try self.evalLayer(in);
        return try self.evalLayer(try exec.queryAs(Compute, self.upstream).?.evalBatch(in));
    }

    fn evalLayer(self: *Compute, in: Batch) !Batch {
        const n = in.row_count;

        for (self.derived, self.derived_cols, self.derived_direct) |r, *out_col, *direct| {
            out_col.clear();
            direct.* = false;
            switch (r.kind) {
                .rename => |rn| {
                    // Pass the upstream view through untouched when its
                    // physical type matches the declared slot (always today;
                    // the guard keeps a future presentation change safe).
                    if (std.meta.activeTag(in.values[rn.src_idx].data) == std.meta.activeTag(out_col.data)) {
                        direct.* = true;
                    } else {
                        try appendCopiedColumn(self.allocator, out_col, in.values[rn.src_idx], n);
                    }
                },
                .null_only => try fillNullColumn(self.allocator, out_col, n),
                .lit_only => |slot| {
                    slot.buf.clear();
                    try fillLiteralColumn(self.allocator, &slot.buf, slot.value, n);
                    if (std.meta.activeTag(slot.buf.data) == std.meta.activeTag(out_col.data)) {
                        direct.* = true;
                    } else {
                        try appendCopiedColumn(self.allocator, out_col, slot.buf.view(), n);
                    }
                },
                .call => |plan| {
                    try self.evalCall(plan, in.values, n);
                    // The root's output store IS the derived value; hand its
                    // view out directly instead of copying into the
                    // derived_cols slot (same lifetime — both refill on the
                    // next call). Copy only on a physical-type mismatch or a
                    // short fill, where the old path's presentation applies.
                    if (std.meta.activeTag(plan.output.data) == std.meta.activeTag(out_col.data) and
                        plan.output.rowCount() == n)
                    {
                        direct.* = true;
                    } else {
                        try appendCopiedColumn(self.allocator, out_col, plan.output.view(), n);
                    }
                },
                .case => |plan| {
                    try self.evalCase(plan, in.values, n);
                    if (std.meta.activeTag(plan.output.data) == std.meta.activeTag(out_col.data) and
                        plan.output.rowCount() == n)
                    {
                        direct.* = true;
                    } else {
                        try appendCopiedColumn(self.allocator, out_col, plan.output.view(), n);
                    }
                },
                .fused_scalar => |fs| try self.evalFusedScalar(fs, in.values, out_col, n),
            }
        }

        for (in.values, 0..) |v, i| self.views[i] = v;
        for (self.derived, self.derived_cols, self.derived_output_indices, self.derived_direct) |r, *c, out_idx, direct| {
            self.views[out_idx] = if (!direct) c.view() else switch (r.kind) {
                .rename => |rn| in.values[rn.src_idx],
                .lit_only => |slot| slot.buf.view(),
                .call => |plan| plan.output.view(),
                .case => |plan| plan.output.view(),
                // Only the four kinds above ever set `direct`.
                .null_only, .fused_scalar => unreachable,
            };
        }

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
    fn evalCall(self: *Compute, plan: *CallPlan, in_values: []const ColumnView, n: usize) anyerror!void {
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
                .null_lit => |slot| {
                    slot.buf.clear();
                    try fillNullColumn(self.allocator, &slot.buf, n);
                    view.* = slot.buf.view();
                },
                .call => |sub| {
                    try self.evalCall(sub, in_values, n);
                    view.* = sub.output.view();
                },
                .case => |sub| {
                    try self.evalCase(sub, in_values, n);
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

        // 3. Run the kernel. Decimal (typed) kernels take precedence and get
        // the arg/out Types so they can align scales.
        if (plan.func.typed_kernel) |tk| {
            try tk(self.allocator, plan.arg_runtime_types, plan.output_type, arg_views, plan.output, n);
        } else if (plan.func.kernel) |k| {
            try k(self.allocator, arg_views, plan.output, n);
        } else if (plan.func.udf_kernel) |k| {
            const ctx: udf_mod.ScalarContext = .{ .allocator = self.allocator, .user_data = plan.func.user_data };
            try k(&ctx, arg_views, plan.output, n);
        } else {
            return Error.ComputeNoSuchOverload;
        }

        // 4. Null bookkeeping. Internal calls always have a nullable
        // output (we allocated it that way) so the parent's null-check
        // sees correct validity; root calls only write when their
        // declared schema column is nullable.
        if (plan.output.nulls != null) {
            switch (plan.func.null_strategy) {
                .propagates => try writePropagatedNulls(self.allocator, plan.output, arg_views, n),
                .absorbs => try writeAbsorbedNulls(self.allocator, plan.output, arg_views, n),
                .zero_divisor => try writeZeroDivisorNulls(self.allocator, plan.output, arg_views, n),
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
    fn evalCase(self: *Compute, plan: *CasePlan, in_values: []const ColumnView, n: usize) anyerror!void {
        plan.output.clear();
        if (plan.branches.len > MAX_CASE_BRANCHES) return Error.ComputeTooManyArgs;

        var srcs_buf: [MAX_CASE_BRANCHES + 1]CaseSrc = undefined;
        for (plan.branches, 0..) |br, bi| {
            srcs_buf[bi] = try self.caseSrc(br.then_src, br.cast_kernel, br.cast_buf, in_values, n);
        }
        var src_count = plan.branches.len;
        if (plan.else_src) |es| {
            srcs_buf[src_count] = try self.caseSrc(es, plan.else_cast_kernel, plan.else_cast_buf, in_values, n);
            src_count += 1;
        }
        const srcs = srcs_buf[0..src_count];

        // winners[i] indexes `srcs`: the first branch whose condition holds,
        // else the ELSE slot; with no ELSE, `srcs.len` marks an unmatched
        // (NULL) row.
        const winners = try self.allocator.alloc(u8, n);
        defer self.allocator.free(winners);
        @memset(winners, @as(u8, @intCast(plan.branches.len)));
        const cond_buf = try self.allocator.alloc(bool, n);
        defer self.allocator.free(cond_buf);
        const open = try self.allocator.alloc(bool, n);
        defer self.allocator.free(open);
        @memset(open, true);

        const fake_batch: Batch = .{
            .schema = plan.upstream_schema,
            .values = in_values,
            .row_count = n,
        };
        for (plan.branches, 0..) |br, bi| {
            // Rows an earlier branch won are inactive: the guided evaluator
            // may skip them, and their mask bits are ignored here.
            @memset(cond_buf, false);
            try predicate_mod.evaluateExprGuided(self.allocator, br.cond, plan.upstream_schema, fake_batch, cond_buf, open);
            for (cond_buf, open, winners) |c, *o, *w| {
                if (o.* and c) {
                    w.* = @intCast(bi);
                    o.* = false;
                }
            }
        }

        switch (plan.output.data) {
            .varchar, .string, .char, .json => |*ss| try assembleCaseStrings(self.allocator, ss, plan.output, srcs, winners),
            inline else => |*list, tag| try assembleCaseFixed(self.allocator, list, tag, plan.output, srcs, winners),
        }
    }

    /// The batch value of one THEN/ELSE clause. Literals and bare NULLs
    /// materialize once as a single row (marked scalar); columns, calls and
    /// nested CASEs yield a row per input row. A branch cast runs over
    /// whichever it produced.
    fn caseSrc(
        self: *Compute,
        s: BranchSrc,
        cast_kernel: ?CastKernel,
        cast_buf: ?*ColumnStore,
        in_values: []const ColumnView,
        n: usize,
    ) anyerror!CaseSrc {
        const scalar = s == .lit or s == .null_lit;
        const raw: ColumnView = switch (s) {
            .col => |idx| in_values[idx],
            .lit => |slot| blk: {
                if (slot.buf.rowCount() != 1) {
                    slot.buf.clear();
                    try fillLiteralColumn(self.allocator, &slot.buf, slot.value, 1);
                }
                break :blk slot.buf.view();
            },
            .null_lit => |slot| blk: {
                if (slot.buf.rowCount() != 1) {
                    slot.buf.clear();
                    try fillNullColumn(self.allocator, &slot.buf, 1);
                }
                break :blk slot.buf.view();
            },
            .call => |sub| blk: {
                try self.evalCall(sub, in_values, n);
                break :blk sub.output.view();
            },
            .case => |sub| blk: {
                try self.evalCase(sub, in_values, n);
                break :blk sub.output.view();
            },
        };
        const k = cast_kernel orelse return .{ .view = raw, .scalar = scalar };
        const buf = cast_buf.?;
        buf.clear();
        var one_arg = [_]ColumnView{raw};
        try k(self.allocator, &one_arg, buf, if (scalar) 1 else n);
        return .{ .view = buf.view(), .scalar = scalar };
    }

    /// Evaluate a fused `col <op> const` directly into `out_col` in one
    /// widening SIMD pass — no cast column, no replicated-literal column.
    /// Only built for non-nullable int/float source columns (see tryFuseScalar),
    /// so there is no validity bitmap to propagate. Integer lanes wrap at the
    /// output width, exactly as the generic integer kernels do.
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
    const lit_idx: usize = if (col_left) 1 else 0;
    arg_types[1 - lit_idx] = src_type;
    arg_types[lit_idx] = try literalType(lit_v);
    arg_types[lit_idx] = try literalType(scalar_fn.arithOperandLiteral(c.fn_name, &arg_types, lit_v));
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
pub fn mayVary(e: Expr, registry: ?*const udf_mod.UdfRegistry) bool {
    switch (e) {
        .call => |call| {
            inline for (scalar_fn.builtins) |f| {
                if (comptime f.volatility == .@"volatile") {
                    if (std.ascii.eqlIgnoreCase(f.name, call.fn_name)) return true;
                }
            }
            if (registry) |r| for (r.scalarEntries()) |entry| {
                if (entry.volatility == .@"volatile" and std.ascii.eqlIgnoreCase(entry.name, call.fn_name)) return true;
            };
            for (call.args) |arg| if (mayVary(arg, registry)) return true;
            return false;
        },
        .case => |c| {
            for (c.branches) |b| if (mayVary(b.then, registry)) return true;
            if (c.else_branch) |e2| return mayVary(e2.*, registry);
            return false;
        },
        .col_ref, .lit, .null_lit => return false,
        .scalar_subquery, .exists_subquery, .var_ref => return true,
    }
}

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
        .null_lit => {},
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

/// A derived [min,max] is only kept when both endpoints fit `out_type`: the
/// integer kernels wrap any value outside it, so a computed bound that
/// escapes that width bounds nothing. Null otherwise.
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
    udf_registry: ?*const udf_mod.UdfRegistry,
) !ResolvedDerived {
    const name = try aa.dupe(u8, d.name);
    switch (d.expr) {
        .col_ref => |src_name| {
            const idx = columnIndex(up_schema, src_name) orelse return Error.ColumnNotFound;
            return .{
                .name = name,
                .output_type = up_schema[idx].type,
                .stat_class = .none, // rename is handled directly by stats() (pass-through)
                .kind = .{ .rename = .{ .src_idx = idx } },
            };
        },
        .lit => |v| {
            const slot = try aa.create(LitSlot);
            slot.* = .{
                .value = v,
                .ty = try literalType(v),
                .buf = try ColumnStore.init(runtime_allocator, try literalType(v), false),
            };
            return .{
                .name = name,
                .output_type = try literalType(v),
                .stat_class = .{ .literal = intFamilyValueI128(v) },
                .kind = .{ .lit_only = slot },
            };
        },
        .null_lit => |ty| {
            return .{
                .name = name,
                .output_type = ty,
                .stat_class = .none,
                .kind = .{ .null_only = ty },
            };
        },
        .call => {
            const stat_class: StatClass = if (mayVary(d.expr, udf_registry)) .none else classifyExpr(d.expr, up_schema);
            // Fast path: `col +/-/* const` collapses to one widening SIMD pass.
            if (try tryFuseScalar(aa, d.expr, up_schema)) |fs| {
                return .{
                    .name = name,
                    .output_type = fs.out_type,
                    .stat_class = stat_class,
                    .kind = .{ .fused_scalar = fs },
                };
            }
            const plan = try buildCallPlan(runtime_allocator, aa, d.expr, up_schema, udf_registry);
            return .{
                .name = name,
                .output_type = plan.output_type,
                .stat_class = stat_class,
                .kind = .{ .call = plan },
            };
        },
        .case => {
            const plan = try buildCasePlan(runtime_allocator, aa, d.expr.case, up_schema, udf_registry);
            return .{
                .name = name,
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

/// CASE result branches take one output type by the result-type rule
/// (`cast.commonType`). Branches that never share a type are rejected.
const CaseUnifyState = struct {
    /// True while every integer-typed contribution so far was a literal —
    /// only then may a later decimal branch take over the unified type.
    int_contribs_all_lits: bool = true,
    /// True while every branch so far was a bare NULL (whose parser
    /// placeholder type must not pin the CASE's type).
    only_null_so_far: bool = true,
};

/// `cast.commonType` plus two plan-time-only rules: an integer LITERAL
/// branch (`ELSE 0`) takes a decimal branch's type rather than widening its
/// precision (the literal carries BIGINT's 19 digits, not its own), and a
/// bare NULL branch (`ELSE NULL`) is typeless and adopts whatever the typed
/// branches unify to. normalizeBranchSrc rewrites the affected slots once
/// the final type is known.
fn unifyCaseType(current: ?Type, next: Type, next_src: BranchSrc, st: *CaseUnifyState) ?Type {
    if (next_src == .null_lit) return current orelse next;
    defer st.only_null_so_far = false;
    if (next.decimalSpec() == null and next.isInteger() and next_src != .lit) {
        st.int_contribs_all_lits = false;
    }
    if (st.only_null_so_far and current != null) return next;
    const cur = current orelse return next;
    if (cur.isDecimal() and next.isInteger() and next_src == .lit) return cur;
    if (next.isDecimal() and cur.isInteger() and st.int_contribs_all_lits) return next;
    return cast.commonType(cur, next);
}

/// Post-unification slot repair: bare-NULL branches take the unified type,
/// and integer literals against a decimal result scale into it.
fn normalizeBranchSrc(runtime_allocator: Allocator, src: BranchSrc, out_type: Type) !void {
    switch (src) {
        .lit => try coerceLitBranch(runtime_allocator, src, out_type),
        .null_lit => |slot| {
            if (std.meta.activeTag(slot.ty) == std.meta.activeTag(out_type)) return;
            replaceBuf(runtime_allocator, &slot.buf, try ColumnStore.init(runtime_allocator, out_type, true));
            slot.ty = out_type;
        },
        else => {},
    }
}

/// Rewrite a literal branch to the unified type when the value is
/// losslessly representable there (predicate.coerceValue — the shared
/// literal-coercion entry). A non-coercible literal is left alone for
/// attachCaseCast's kernel lattice to convert or reject.
fn coerceLitBranch(runtime_allocator: Allocator, src: BranchSrc, out_type: Type) !void {
    if (src != .lit) return;
    const slot = src.lit;
    // A decimal Value carries no scale to rescale from; retypeBranch casts it.
    if (cast.sameRepresentation(slot.ty, out_type) or slot.ty.isDecimal()) return;
    var v = slot.value;
    predicate_mod.coerceValueRounded(&v, out_type) catch return;
    replaceBuf(runtime_allocator, &slot.buf, try ColumnStore.init(runtime_allocator, out_type, false));
    slot.value = v;
    slot.ty = out_type;
}

fn replaceBuf(allocator: Allocator, buf: *ColumnStore, replacement: ColumnStore) void {
    buf.deinit(allocator);
    buf.* = replacement;
}

/// Whether a branch of type `src` reaches the CASE's type through a
/// tag-level widening kernel (attachCaseCast). Other conversions change
/// the value's representation (a decimal at another scale, a number as
/// text) and need retypeBranch's typed cast.
fn widensByKernel(src: Type, out_type: Type) bool {
    if (cast.sameRepresentation(src, out_type)) return true;
    if (src.isDecimal() or out_type.isDecimal()) return false;
    return cast.kernelFor(@as(types.TypeTag, src), @as(types.TypeTag, out_type)) != null;
}

/// Rebuild a branch that can't widen by kernel as the typed cast
/// `CAST(branch AS out_type)` lowers to, so its values are converted rather
/// than reinterpreted.
fn retypeBranch(
    runtime_allocator: Allocator,
    aa: Allocator,
    src: *BranchSrc,
    e: Expr,
    up_schema: []const Column,
    out_type: Type,
    udf_registry: ?*const udf_mod.UdfRegistry,
) PlanError!void {
    if (src.* == .null_lit or widensByKernel(branchSrcType(src.*, up_schema), out_type)) return;
    const name = try scalar_fn.castFnName(aa, out_type) orelse return Error.ComputeUnsupportedExpr;
    const rebuilt = try buildBranchSrc(runtime_allocator, aa, .{ .call = .{ .fn_name = name, .args = try aa.dupe(Expr, &.{e}) } }, up_schema, udf_registry);
    freeBranchSrc(runtime_allocator, src.*);
    src.* = rebuilt;
}

fn attachCaseCast(
    runtime_allocator: Allocator,
    src: BranchSrc,
    up_schema: []const Column,
    out_type: Type,
    cast_kernel: *?CastKernel,
    cast_buf: *?*ColumnStore,
) !void {
    const src_type = branchSrcType(src, up_schema);
    if (cast.sameRepresentation(src_type, out_type)) return;
    // A NULL branch keeps its placeholder buffer; no value to convert.
    if (src == .null_lit) return;
    if (!widensByKernel(src_type, out_type)) return Error.ComputeUnsupportedExpr;
    const k = cast.kernelFor(@as(types.TypeTag, src_type), @as(types.TypeTag, out_type)).?;
    const buf = try runtime_allocator.create(ColumnStore);
    errdefer runtime_allocator.destroy(buf);
    buf.* = try ColumnStore.init(runtime_allocator, out_type, branchSrcNullable(src, up_schema));
    cast_kernel.* = k;
    cast_buf.* = buf;
}

/// Resolve a parsed CASE expression to a CasePlan. All branches' THEN
/// (and ELSE) results unify to one output type by the result-type rule
/// (`unifyCaseType`); branches that never share a type are rejected.
fn buildCasePlan(
    runtime_allocator: Allocator,
    aa: Allocator,
    cs: Expr.Case,
    up_schema: []const Column,
    udf_registry: ?*const udf_mod.UdfRegistry,
) PlanError!*CasePlan {
    if (cs.branches.len == 0) return Error.ComputeUnsupportedExpr;

    const branches = try aa.alloc(CaseBranch, cs.branches.len);
    var built: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < built) : (i += 1) freeCaseBranch(runtime_allocator, branches[i]);
    }

    var inferred_type: ?Type = null;
    var unify_state: CaseUnifyState = .{};
    var may_null = cs.else_branch == null;
    for (cs.branches, branches) |src, *dst| {
        const then_src = try buildBranchSrc(runtime_allocator, aa, src.then, up_schema, udf_registry);
        const t = branchSrcType(then_src, up_schema);
        inferred_type = unifyCaseType(inferred_type, t, then_src, &unify_state) orelse {
            freeBranchSrc(runtime_allocator, then_src);
            return Error.ComputeUnsupportedExpr;
        };
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
    var else_cast_kernel: ?CastKernel = null;
    var else_cast_buf: ?*ColumnStore = null;
    errdefer {
        if (else_src) |es| freeBranchSrc(runtime_allocator, es);
        if (else_cast_buf) |buf| {
            buf.deinit(runtime_allocator);
            runtime_allocator.destroy(buf);
        }
    }
    if (cs.else_branch) |eb| {
        const es = try buildBranchSrc(runtime_allocator, aa, eb.*, up_schema, udf_registry);
        const t = branchSrcType(es, up_schema);
        inferred_type = unifyCaseType(inferred_type, t, es, &unify_state) orelse {
            freeBranchSrc(runtime_allocator, es);
            return Error.ComputeUnsupportedExpr;
        };
        if (branchSrcNullable(es, up_schema)) may_null = true;
        else_src = es;
    }

    const out_type = inferred_type orelse return Error.ComputeUnsupportedExpr;
    for (branches[0..built]) |*br| try normalizeBranchSrc(runtime_allocator, br.then_src, out_type);
    if (else_src) |es| try normalizeBranchSrc(runtime_allocator, es, out_type);
    for (branches[0..built], cs.branches) |*br, src| {
        try retypeBranch(runtime_allocator, aa, &br.then_src, src.then, up_schema, out_type, udf_registry);
    }
    if (else_src) |*es| try retypeBranch(runtime_allocator, aa, es, cs.else_branch.?.*, up_schema, out_type, udf_registry);
    for (branches[0..built]) |*br| {
        try attachCaseCast(runtime_allocator, br.then_src, up_schema, out_type, &br.cast_kernel, &br.cast_buf);
    }
    if (else_src) |es| {
        try attachCaseCast(runtime_allocator, es, up_schema, out_type, &else_cast_kernel, &else_cast_buf);
    }

    const out_buf = try runtime_allocator.create(ColumnStore);
    errdefer runtime_allocator.destroy(out_buf);
    out_buf.* = try ColumnStore.init(runtime_allocator, out_type, may_null);
    errdefer out_buf.deinit(runtime_allocator);

    const plan = try aa.create(CasePlan);
    plan.* = .{
        .branches = branches,
        .else_src = else_src,
        .else_cast_kernel = else_cast_kernel,
        .else_cast_buf = else_cast_buf,
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
    udf_registry: ?*const udf_mod.UdfRegistry,
) PlanError!BranchSrc {
    return switch (e) {
        .col_ref => |name| blk: {
            const idx = columnIndex(up_schema, name) orelse return Error.ColumnNotFound;
            break :blk BranchSrc{ .col = idx };
        },
        .lit => |v| blk: {
            const slot = try aa.create(LitSlot);
            slot.* = .{
                .value = v,
                .ty = try literalType(v),
                .buf = try ColumnStore.init(runtime_allocator, try literalType(v), false),
            };
            break :blk BranchSrc{ .lit = slot };
        },
        .null_lit => |ty| blk: {
            const slot = try aa.create(NullSlot);
            slot.* = .{
                .ty = ty,
                .buf = try ColumnStore.init(runtime_allocator, ty, true),
            };
            break :blk BranchSrc{ .null_lit = slot };
        },
        .call => blk: {
            const sub = try buildCallPlan(runtime_allocator, aa, e, up_schema, udf_registry);
            break :blk BranchSrc{ .call = sub };
        },
        .case => blk: {
            const sub = try buildCasePlan(runtime_allocator, aa, e.case, up_schema, udf_registry);
            break :blk BranchSrc{ .case = sub };
        },
        .scalar_subquery, .exists_subquery, .var_ref => return Error.ComputeUnsupportedExpr,
    };
}

fn branchSrcType(s: BranchSrc, up_schema: []const Column) Type {
    return switch (s) {
        .col => |idx| up_schema[idx].type,
        .lit => |slot| slot.ty,
        .null_lit => |slot| slot.ty,
        .call => |plan| plan.output_type,
        .case => |plan| plan.output_type,
    };
}

fn branchSrcNullable(s: BranchSrc, up_schema: []const Column) bool {
    return switch (s) {
        .col => |idx| up_schema[idx].nullable,
        .lit => false,
        .null_lit => true,
        .call => |plan| callPlanNullable(plan, up_schema),
        .case => |plan| plan.may_produce_null,
    };
}

fn freeBranchSrc(allocator: Allocator, s: BranchSrc) void {
    switch (s) {
        .col => {},
        .lit => |slot| slot.buf.deinit(allocator),
        .null_lit => |slot| slot.buf.deinit(allocator),
        .call => |sub| freeCallPlan(allocator, sub),
        .case => |sub| freeCasePlan(allocator, sub),
    }
}

fn freeCaseBranch(allocator: Allocator, branch: CaseBranch) void {
    freeBranchSrc(allocator, branch.then_src);
    if (branch.cast_buf) |buf| {
        buf.deinit(allocator);
        allocator.destroy(buf);
    }
}

fn freeCasePlan(allocator: Allocator, plan: *CasePlan) void {
    for (plan.branches) |br| freeCaseBranch(allocator, br);
    if (plan.else_src) |es| freeBranchSrc(allocator, es);
    if (plan.else_cast_buf) |buf| {
        buf.deinit(allocator);
        allocator.destroy(buf);
    }
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
    udf_registry: ?*const udf_mod.UdfRegistry,
) PlanError!*CallPlan {
    const c = switch (expr) {
        .call => |x| x,
        else => return Error.ComputeUnsupportedExpr,
    };

    const arg_plans = try aa.alloc(ArgPlan, c.args.len);
    const arg_types = try aa.alloc(Type, c.args.len);
    var built: usize = 0;
    errdefer for (arg_plans[0..built]) |ap| freeArgPlan(runtime_allocator, ap);
    for (c.args, 0..) |arg, i| {
        switch (arg) {
            .col_ref => |name| {
                const idx = columnIndex(up_schema, name) orelse return Error.ColumnNotFound;
                arg_plans[i] = .{ .col = idx };
                arg_types[i] = up_schema[idx].type;
            },
            .lit => |v| {
                const slot = try aa.create(LitSlot);
                slot.* = .{
                    .value = v,
                    .ty = try literalType(v),
                    .buf = try ColumnStore.init(runtime_allocator, try literalType(v), false),
                };
                arg_plans[i] = .{ .lit = slot };
                arg_types[i] = try literalType(v);
            },
            .null_lit => |ty| {
                const slot = try aa.create(NullSlot);
                slot.* = .{
                    .ty = ty,
                    .buf = try ColumnStore.init(runtime_allocator, ty, true),
                };
                arg_plans[i] = .{ .null_lit = slot };
                arg_types[i] = ty;
            },
            .call => {
                const sub = try buildCallPlan(runtime_allocator, aa, arg, up_schema, udf_registry);
                arg_plans[i] = .{ .call = sub };
                arg_types[i] = sub.output_type;
            },
            .case => {
                const sub = try buildCasePlan(runtime_allocator, aa, arg.case, up_schema, udf_registry);
                arg_plans[i] = .{ .case = sub };
                arg_types[i] = sub.output_type;
            },
            .scalar_subquery, .exists_subquery, .var_ref => {
                return Error.ComputeUnsupportedExpr;
            },
        }
        built += 1;
    }
    // Retyping a literal never changes whether the call is integer arithmetic,
    // so later literals still see the right decision.
    for (arg_plans, arg_types) |ap, *at| {
        if (ap != .lit) continue;
        const slot = ap.lit;
        const typed = scalar_fn.arithOperandLiteral(c.fn_name, arg_types, slot.value);
        if (std.meta.activeTag(typed) == std.meta.activeTag(slot.value)) continue;
        replaceBuf(runtime_allocator, &slot.buf, try ColumnStore.init(runtime_allocator, try literalType(typed), false));
        slot.value = typed;
        slot.ty = try literalType(typed);
        at.* = slot.ty;
    }

    var r = try scalar_fn.resolveWithRegistry(aa, udf_registry, c.fn_name, arg_types);
    const coerce_literals = if (r) |resolved| parsesTextToTemporal(resolved.func) else true;
    if (coerce_literals and try coerceTemporalStringLiterals(runtime_allocator, c.fn_name, arg_plans, arg_types)) {
        r = try scalar_fn.resolveWithRegistry(aa, udf_registry, c.fn_name, arg_types);
    }
    if (r == null) r = try resolveWithTypedNulls(runtime_allocator, aa, udf_registry, c.fn_name, arg_plans, arg_types);
    if (r == null) {
        if (try retypedCall(aa, c, arg_plans, arg_types)) |rewritten| {
            for (arg_plans[0..built]) |ap| freeArgPlan(runtime_allocator, ap);
            built = 0;
            return buildCallPlan(runtime_allocator, aa, rewritten, up_schema, udf_registry);
        }
    }
    const rr = r orelse return Error.ComputeNoSuchOverload;

    // Cast scratch buffers (one per coerced arg).
    var cast_buffers: ?[]?ColumnStore = null;
    errdefer if (cast_buffers) |buffers| freeCastBuffers(runtime_allocator, buffers);
    if (rr.arg_casts) |casts| {
        const buffers = try runtime_allocator.alloc(?ColumnStore, casts.len);
        @memset(buffers, null);
        cast_buffers = buffers;
        for (casts, rr.func.arg_types, arg_plans, buffers) |k, declared, ap, *slot| {
            if (k == null) {
                slot.* = null;
                continue;
            }
            const src_nullable = switch (ap) {
                .col => |idx| up_schema[idx].nullable,
                .lit => false,
                .null_lit => true,
                // Sub-call outputs are nullable (allocated below).
                .call => true,
                .case => |sub| sub.may_produce_null,
            };
            slot.* = try ColumnStore.init(runtime_allocator, declared, src_nullable);
        }
    }

    // Own a nullable output ColumnStore so the next level up's null
    // propagation can see the correct validity bits.
    const output_buf = try runtime_allocator.create(ColumnStore);
    errdefer runtime_allocator.destroy(output_buf);
    output_buf.* = try ColumnStore.init(runtime_allocator, rr.func.return_type, true);
    errdefer output_buf.deinit(runtime_allocator);

    var func = rr.func;
    if (try literalDivisorNonzero(aa, runtime_allocator, rr, arg_plans)) func.null_strategy = .propagates;
    const plan = try aa.create(CallPlan);
    plan.* = .{
        .func = func,
        .args = arg_plans,
        .arg_runtime_types = arg_types,
        .arg_casts = rr.arg_casts,
        .cast_buffers = cast_buffers,
        .output = output_buf,
        .output_owned = true,
        .output_type = rr.func.return_type,
    };
    return plan;
}

/// A call no overload accepts as written, with its arguments converted so
/// one does: the arguments a function returns (GREATEST, COALESCE, IF's
/// branches) take their common type by the result-type rule, and otherwise
/// a string parameter takes a number, decimal or date as its text
/// (`CONCAT('Q', quarter)`). Literals are converted in place; everything
/// else goes through the typed cast `CAST(x AS t)` lowers to. Null when
/// no conversion applies, so the rewritten call can't recurse again.
fn retypedCall(aa: Allocator, c: Expr.Call, arg_plans: []const ArgPlan, arg_types: []const Type) PlanError!?Expr {
    const args = try aa.alloc(Expr, c.args.len);
    for (args, c.args, arg_plans) |*a, orig, ap| a.* = if (ap == .lit) .{ .lit = ap.lit.value } else orig;
    if (scalar_fn.resultValueArgsStart(c.fn_name)) |start| if (start < args.len) {
        var typed: std.ArrayList(Type) = .empty;
        for (arg_plans[start..], arg_types[start..]) |ap, t| if (ap != .null_lit) try typed.append(aa, t);
        const target = cast.commonTypeOf(typed.items) orelse return null;
        var changed = false;
        for (args[start..], arg_plans[start..], arg_types[start..]) |*a, ap, t| {
            if (ap == .null_lit) {
                a.* = .{ .null_lit = target };
                continue;
            }
            if (cast.sameRepresentation(t, target)) continue;
            a.* = try convertedArg(aa, a.*, target) orelse return null;
            changed = true;
        }
        return if (changed) Expr{ .call = .{ .fn_name = c.fn_name, .args = args } } else null;
    };
    const wrap = try scalar_fn.stringifiedArgs(aa, c.fn_name, arg_types) orelse return null;
    for (args, wrap) |*a, w| {
        if (w) a.* = try convertedArg(aa, a.*, .string) orelse return null;
    }
    return Expr{ .call = .{ .fn_name = c.fn_name, .args = args } };
}

fn convertedArg(aa: Allocator, e: Expr, target: Type) !?Expr {
    if (e == .lit and !target.isString()) {
        var v = e.lit;
        if (predicate_mod.coerceValueRounded(&v, target)) |_| return Expr{ .lit = v } else |_| {}
    }
    const name = try scalar_fn.castFnName(aa, target) orelse return null;
    return Expr{ .call = .{ .fn_name = name, .args = try aa.dupe(Expr, &.{e}) } };
}

/// A `.zero_divisor` call whose divisor is a literal that is still nonzero as
/// the kernel sees it (after its implicit cast) never divides by zero, so the
/// result is exactly as nullable as its operands: `x / 100` or `x % 10` over a
/// NOT NULL column stays NOT NULL.
fn literalDivisorNonzero(aa: Allocator, runtime_allocator: Allocator, rr: scalar_fn.ResolvedOverload, args: []const ArgPlan) !bool {
    if (rr.func.null_strategy != .zero_divisor or args.len == 0) return false;
    const last = args.len - 1;
    const slot = switch (args[last]) {
        .lit => |s| s,
        else => return false,
    };
    slot.buf.clear();
    try fillLiteralColumn(runtime_allocator, &slot.buf, slot.value, 1);
    var divisor = slot.buf.view();
    if (rr.arg_casts) |casts| if (casts[last]) |k| {
        var converted = try ColumnStore.init(aa, rr.func.arg_types[last], false);
        const one = [_]ColumnView{divisor};
        k(aa, &one, &converted, 1) catch return false;
        divisor = converted.view();
    };
    return !zeroDivisorAt(divisor, 0);
}

/// MySQL/StarRocks coerce string LITERALS to temporal types in temporal
/// contexts (`DATE_ADD('2026-05-01', INTERVAL -1 MONTH)`, `LAST_DAY('2026-05-01')`).
/// The implicit cast ladder deliberately refuses string→date for COLUMNS — a
/// format-dependent footgun — so we special-case literals: when no overload
/// resolves, look for one reachable by reinterpreting a string-literal arg as
/// date/datetime, parse it, and rewrite the slot in place. Returns true when it
/// coerced at least one argument (caller re-resolves). The literal is validated
/// during the feasibility scan, so the commit pass never mutates partially.
///
/// Also called when the call resolved to an explicit text parse
/// (`CAST(text AS DATE)`): over a literal that parse is a constant, so doing
/// it here keeps the call a non-null constant rather than a per-row parse.
/// A NULL literal carries only a placeholder type, so a call no overload
/// accepts as written retries with its NULL arguments retyped to each
/// sibling argument's type in turn (`COALESCE(NULL, x)`, `x + NULL`).
fn resolveWithTypedNulls(
    runtime_allocator: Allocator,
    aa: Allocator,
    udf_registry: ?*const udf_mod.UdfRegistry,
    fn_name: []const u8,
    arg_plans: []ArgPlan,
    arg_types: []Type,
) !?scalar_fn.ResolvedOverload {
    var any_null = false;
    for (arg_plans) |ap| any_null = any_null or ap == .null_lit;
    if (!any_null) return null;
    const tried = try aa.alloc(Type, arg_types.len);
    var tried_len: usize = 0;
    candidates: for (arg_plans, arg_types) |ap, candidate| {
        if (ap == .null_lit) continue;
        for (tried[0..tried_len]) |t| if (std.meta.eql(t, candidate)) continue :candidates;
        tried[tried_len] = candidate;
        tried_len += 1;
        const retyped = try aa.dupe(Type, arg_types);
        for (arg_plans, retyped) |p, *t| {
            if (p == .null_lit) t.* = candidate;
        }
        const r = try scalar_fn.resolveWithRegistry(aa, udf_registry, fn_name, retyped) orelse continue;
        for (arg_plans, arg_types) |p, *t| {
            if (p != .null_lit) continue;
            replaceBuf(runtime_allocator, &p.null_lit.buf, try ColumnStore.init(runtime_allocator, candidate, true));
            p.null_lit.ty = candidate;
            t.* = candidate;
        }
        return r;
    }
    return null;
}

fn coerceTemporalStringLiterals(
    runtime_allocator: Allocator,
    fn_name: []const u8,
    arg_plans: []ArgPlan,
    arg_types: []Type,
) !bool {
    for (scalar_fn.overloadsOf(fn_name)) |f| {
        if (f.variadic_min_args != null or f.arg_types.len != arg_types.len) continue;
        var feasible = true;
        var any_coerce = false;
        for (arg_types, 0..) |given, i| {
            const declared = f.arg_types[i];
            if (foldStringTag(@as(types.TypeTag, declared)) == foldStringTag(@as(types.TypeTag, given))) continue;
            if (cast.castCost(@as(types.TypeTag, given), @as(types.TypeTag, declared)) != null) continue;
            // The only otherwise-unreachable mismatch we repair: a string
            // LITERAL where the overload wants a temporal type, and the literal
            // actually parses as that type.
            if ((declared == .date or declared == .datetime) and litTemporalValue(arg_plans[i], declared) != null) {
                any_coerce = true;
                continue;
            }
            feasible = false;
            break;
        }
        if (!feasible or !any_coerce) continue;

        for (arg_types, 0..) |*at, i| {
            const declared = f.arg_types[i];
            if (declared != .date and declared != .datetime) continue;
            const new_val = litTemporalValue(arg_plans[i], declared) orelse continue;
            const slot = arg_plans[i].lit;
            replaceBuf(runtime_allocator, &slot.buf, try ColumnStore.init(runtime_allocator, try literalType(new_val), false));
            slot.value = new_val;
            slot.ty = try literalType(new_val);
            at.* = try literalType(new_val);
        }
        return true;
    }
    return false;
}

/// Whether `f` reads text as a date or datetime, as `CAST(text AS DATE)` does.
fn parsesTextToTemporal(f: scalar_fn.ScalarFn) bool {
    return f.arg_types.len == 1 and f.arg_types[0].isString() and
        (f.return_type == .date or f.return_type == .datetime);
}

/// `.varchar`/`.char` share `.string`'s representation; fold them for matching.
fn foldStringTag(t: types.TypeTag) types.TypeTag {
    return switch (t) {
        .varchar, .char => .string,
        else => t,
    };
}

/// If `ap` is a string literal that parses as `target` (`.date`/`.datetime`),
/// return the coerced Value; otherwise null. It parses exactly as the per-row
/// text kernels do, so folding a literal never changes a result. Pure — used
/// both to test feasibility and to commit.
fn litTemporalValue(ap: ArgPlan, target: types.TypeTag) ?types.Value {
    const slot = switch (ap) {
        .lit => |s| s,
        else => return null,
    };
    const text = switch (slot.value) {
        .text => |s| s,
        else => return null,
    };
    return switch (target) {
        .date => .{ .date = scalar_common.textToDate(text) orelse return null },
        .datetime => .{ .datetime = scalar_common.textToDatetime(text) orelse return null },
        else => null,
    };
}

/// Walk a CallPlan and release every runtime-allocated buffer
/// (cast scratches, owned outputs, recursive sub-calls' buffers).
/// Called from Compute.deinit. The CallPlan struct itself lives in
/// the arena and is freed there.
fn freeCallPlan(runtime_allocator: Allocator, plan: *CallPlan) void {
    for (plan.args) |arg| freeArgPlan(runtime_allocator, arg);
    if (plan.cast_buffers) |buffers| freeCastBuffers(runtime_allocator, buffers);
    if (plan.output_owned) {
        plan.output.deinit(runtime_allocator);
        runtime_allocator.destroy(plan.output);
    }
}

fn freeArgPlan(runtime_allocator: Allocator, arg: ArgPlan) void {
    switch (arg) {
        .col => {},
        .lit => |slot| slot.buf.deinit(runtime_allocator),
        .null_lit => |slot| slot.buf.deinit(runtime_allocator),
        .call => |sub| freeCallPlan(runtime_allocator, sub),
        .case => |sub| freeCasePlan(runtime_allocator, sub),
    }
}

fn freeCastBuffers(runtime_allocator: Allocator, buffers: []?ColumnStore) void {
    for (buffers) |*slot| if (slot.*) |*cs| cs.deinit(runtime_allocator);
    runtime_allocator.free(buffers);
}

fn freeResolvedDerived(runtime_allocator: Allocator, r: ResolvedDerived) void {
    switch (r.kind) {
        .call => |plan| freeCallPlan(runtime_allocator, plan),
        .lit_only => |slot| slot.buf.deinit(runtime_allocator),
        .case => |plan| freeCasePlan(runtime_allocator, plan),
        .rename, .null_only, .fused_scalar => {},
    }
}

/// Infer the ColumnStore-compatible Type for a literal Value. Mirrors
/// the active union tag — int literals stay int (not promoted to bigint);
/// promotion happens via the existing implicit-cast machinery if the
/// resolved overload requires it.
fn literalType(v: types.Value) Error!Type {
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
        // without those — the producer must cast to the decimal type.
        .decimal64, .decimal128 => Error.TypeMismatch,
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
            // kept only when it fits the output width (see provableRange).
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
            // min/max for + and - via interval arithmetic, kept only when it
            // fits the output width (see provableRange).
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
        .null_only => true,
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
        .absorbs, .kernel_managed, .zero_divisor => return true,
        .propagates => {},
    }
    for (plan.args) |arg| switch (arg) {
        .col => |idx| if (up_schema[idx].nullable) return true,
        .lit => {},
        .null_lit => return true,
        .call => |sub| if (callPlanNullable(sub, up_schema)) return true,
        .case => |sub| if (sub.may_produce_null) return true,
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
    // Output row i valid iff ALL arg rows i are valid: start the rows all
    // valid, then AND each nullable argument's bitmap in.
    const base = out.data.rowCount() - n;
    try out.appendValidityRange(allocator, base, null, n);
    const bits = out.nulls.?.items;
    for (arg_views) |v| {
        const src = v.nulls orelse continue;
        if (base & 7 == 0) {
            // Bits past the n rows are 0 in `bits`, so the AND keeps them 0.
            const bytes = (n + 7) / 8;
            for (bits[base >> 3 ..][0..bytes], src[0..bytes]) |*d, s| d.* &= s;
        } else {
            for (0..n) |i| if (!storage.column.isValidBit(src, i)) clearBit(bits, base + i);
        }
    }
}

/// `.zero_divisor`: row i is valid iff every argument is valid and the divisor
/// (the last argument) is nonzero.
fn writeZeroDivisorNulls(
    allocator: Allocator,
    out: *ColumnStore,
    arg_views: []const ColumnView,
    n: usize,
) !void {
    try writePropagatedNulls(allocator, out, arg_views, n);
    const base = out.data.rowCount() - n;
    const bits = out.nulls.?.items;
    switch (arg_views[arg_views.len - 1].data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint, .boolean, .float, .double, .decimal64, .decimal128 => |divisor| {
            for (divisor[0..n], base..) |d, row| if (d == 0) clearBit(bits, row);
        },
        else => {},
    }
}

fn clearBit(bits: []u8, row: usize) void {
    bits[row >> 3] &= ~(@as(u8, 1) << @intCast(row & 7));
}

/// Only numeric columns divide; a divisor of any other type is never zero.
fn zeroDivisorAt(divisor: ColumnView, row: usize) bool {
    return switch (divisor.data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint, .boolean, .float, .double, .decimal64, .decimal128 => |s| s[row] == 0,
        else => false,
    };
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

/// Fixed-width CASE output: size the column once, then one pass reads each
/// row from its winning source (a scalar source replicates through a zero
/// stride; an unmatched row reads a zero placeholder and is NULL).
fn assembleCaseFixed(
    allocator: Allocator,
    list: anytype,
    comptime tag: types.TypeTag,
    out: *ColumnStore,
    srcs: []const CaseSrc,
    winners: []const u8,
) !void {
    const T = std.meta.Child(@TypeOf(list.items));
    try list.resize(allocator, winners.len);
    var ptrs: [MAX_CASE_BRANCHES + 2][*]const T = undefined;
    var strides: [MAX_CASE_BRANCHES + 2]usize = undefined;
    for (srcs, 0..) |s, k| {
        ptrs[k] = @field(s.view.data, @tagName(tag)).ptr;
        strides[k] = @intFromBool(!s.scalar);
    }
    const placeholder: [1]T = .{std.mem.zeroes(T)};
    ptrs[srcs.len] = &placeholder;
    strides[srcs.len] = 0;
    for (list.items, winners, 0..) |*o, w, i| o.* = ptrs[w][i * strides[w]];
    try setCaseValidity(allocator, out, srcs, winners);
}

/// String CASE output: append each row's bytes straight from its winning
/// source; an unmatched row is an empty NULL placeholder.
fn assembleCaseStrings(
    allocator: Allocator,
    ss: *store.StringStore,
    out: *ColumnStore,
    srcs: []const CaseSrc,
    winners: []const u8,
) !void {
    const unmatched: u8 = @intCast(srcs.len);
    for (winners, 0..) |w, i| {
        if (w == unmatched) {
            try ss.appendValue(allocator, "");
            try out.appendValidBit(allocator, i, false);
            continue;
        }
        const s = srcs[w];
        const r = s.row(i);
        try ss.appendValue(allocator, stringRowBytes(s.view, r));
        if (out.nulls != null) try out.appendValidBit(allocator, i, s.view.isValid(r));
    }
}

/// Validity of an assembled fixed-width CASE column: a row is NULL when it
/// matched no branch of an ELSE-less CASE or its winning source is NULL
/// there. Outputs proven non-null at plan time carry no bitmap.
fn setCaseValidity(allocator: Allocator, out: *ColumnStore, srcs: []const CaseSrc, winners: []const u8) !void {
    const bits: *std.ArrayList(u8) = if (out.nulls) |*b| b else return;
    try bits.resize(allocator, (winners.len + 7) >> 3);
    @memset(bits.items, 0);
    const unmatched: u8 = @intCast(srcs.len);
    for (winners, 0..) |w, i| {
        if (w == unmatched) continue;
        if (!srcs[w].view.isValid(srcs[w].row(i))) continue;
        bits.items[i >> 3] |= @as(u8, 1) << @as(u3, @intCast(i & 7));
    }
}

fn stringRowBytes(view: ColumnView, row: usize) []const u8 {
    return switch (view.data) {
        .varchar, .string, .char, .json => |sv| sv.rowBytes(row),
        // CASE branches unify to one type family at plan time.
        else => unreachable,
    };
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
        .decimal64 => |x| while (i < n) : (i += 1) try buf.data.decimal64.append(allocator, x),
        .decimal128 => |x| while (i < n) : (i += 1) try buf.data.decimal128.append(allocator, x),
        .uuid => |x| while (i < n) : (i += 1) try buf.data.uuid.append(allocator, x),
    }
}

fn fillNullColumn(allocator: Allocator, buf: *ColumnStore, n: usize) !void {
    var i: usize = 0;
    while (i < n) : (i += 1) {
        try buf.data.appendNullPlaceholder(allocator);
        try buf.appendValidBit(allocator, buf.data.rowCount() - 1, false);
    }
}

test "stats aligns to the create-time input width when the upstream is re-typed" {
    // Probe fusion re-types the pipeline below a Compute after it was built:
    // a chained operator then reports the joined output schema, wider than
    // the schema the derived columns were resolved against. The upstream's
    // per-column stats index that wider schema, so they must be dropped, not
    // copied into the wrong slots or past the end of the output buffer.
    const ReTyped = struct {
        schema: []const Column,
        col_stats: []const exec.ColStat,

        pub fn next(_: *@This()) anyerror!?Batch {
            return null;
        }
        pub fn deinit(_: *@This()) void {}
        pub fn outputSchema(self: *@This()) []const Column {
            return self.schema;
        }
        pub fn addPrune(_: *@This(), _: Predicate) anyerror!void {}
        pub fn stats(self: *@This()) exec.PipelineStats {
            return .{ .upper_rows = 10, .column_stats = self.col_stats };
        }
        pub fn accountant(_: *@This()) ?*exec.memory.MemoryAccountant {
            return null;
        }
        pub fn explain(_: *@This(), _: *std.ArrayList(u8), _: Allocator, _: usize) anyerror!void {}
    };
    const narrow = [_]Column{ .{ .name = "a", .type = .int }, .{ .name = "b", .type = .int } };
    const narrow_stats = [_]exec.ColStat{ .{ .ndv = .{ .exact = 3 } }, .{ .ndv = .{ .exact = 7 } } };
    const wide = [_]Column{
        .{ .name = "a", .type = .int }, .{ .name = "b", .type = .int }, .{ .name = "c", .type = .int },
        .{ .name = "d", .type = .int }, .{ .name = "e", .type = .int },
    };
    const wide_stats = [_]exec.ColStat{
        .{ .ndv = .{ .exact = 11 } }, .{ .ndv = .{ .exact = 12 } }, .{ .ndv = .{ .exact = 13 } },
        .{ .ndv = .{ .exact = 14 } }, .{ .ndv = .{ .exact = 15 } },
    };
    var src = ReTyped{ .schema = &narrow, .col_stats = &narrow_stats };
    const derived = [_]Derived{.{ .name = "k", .expr = .{ .col_ref = "a" } }};
    var q = try Compute.create(std.testing.allocator, exec.makeQuery(std.testing.allocator, &src), &derived);
    defer q.deinit();

    const aligned = q.stats();
    try std.testing.expectEqual(@as(usize, 3), aligned.column_stats.len);
    try std.testing.expectEqual(exec.ColCard{ .exact = 7 }, aligned.column_stats[1].ndv);
    try std.testing.expectEqual(exec.ColCard{ .exact = 3 }, aligned.column_stats[2].ndv);

    src.schema = &wide;
    src.col_stats = &wide_stats;
    const retyped = q.stats();
    try std.testing.expectEqual(@as(usize, 3), retyped.column_stats.len);
    for (retyped.column_stats) |s| try std.testing.expectEqual(exec.ColCard.unknown, s.ndv);
}

test "retains its own copy of the derived IR after the caller's scratch dies" {
    // Join.create builds its key-coercion casts (`to_*(col)` named after the
    // column) in an arena that dies when create returns. The IR is read
    // again long after that: chain clones re-resolve it and the join's
    // shared-stage peel inspects the call shape. A shallow copy aliased the
    // dead arena and segfaulted the ReleaseFast server under the wayroll
    // report suites. Scribbling the caller's bytes after create must not
    // change what the operator sees.
    const Source = struct {
        pub fn next(_: *@This()) anyerror!?Batch {
            return null;
        }
        pub fn deinit(_: *@This()) void {}
        pub fn outputSchema(_: *@This()) []const Column {
            return &.{.{ .name = "a", .type = .int }};
        }
        pub fn addPrune(_: *@This(), _: Predicate) anyerror!void {}
        pub fn stats(_: *@This()) exec.PipelineStats {
            return .{ .upper_rows = 0, .column_stats = &.{} };
        }
        pub fn accountant(_: *@This()) ?*exec.memory.MemoryAccountant {
            return null;
        }
        pub fn explain(_: *@This(), _: *std.ArrayList(u8), _: Allocator, _: usize) anyerror!void {}
    };
    var name_buf = "a".*;
    var fn_buf = "to_bigint".*;
    var arg_buf = "a".*;
    var args = [_]Expr{.{ .col_ref = &arg_buf }};
    var derived = [_]Derived{.{ .name = &name_buf, .expr = .{ .call = .{ .fn_name = &fn_buf, .args = &args } } }};
    var src = Source{};
    var q = try Compute.create(std.testing.allocator, exec.makeQuery(std.testing.allocator, &src), &derived);
    defer q.deinit();

    name_buf[0] = '?';
    @memset(&fn_buf, '?');
    arg_buf[0] = '?';
    args[0] = .{ .lit = .{ .int = 0 } };
    derived[0] = .{ .name = "gone", .expr = .{ .col_ref = "gone" } };

    const c = exec.queryAs(Compute, q).?;
    try std.testing.expectEqualStrings("a", c.derived_ir[0].name);
    try std.testing.expectEqualStrings("to_bigint", c.derived_ir[0].expr.call.fn_name);
    try std.testing.expectEqualStrings("a", c.derived_ir[0].expr.call.args[0].col_ref);
}

/// Upstream for the construction-failure tests: `a INT NULL, s VARCHAR NULL`, no rows.
const EmptyTestSource = struct {
    pub fn next(_: *@This()) anyerror!?Batch {
        return null;
    }
    pub fn deinit(_: *@This()) void {}
    pub fn outputSchema(_: *@This()) []const Column {
        return &.{
            .{ .name = "a", .type = .int, .nullable = true },
            .{ .name = "s", .type = .string, .nullable = true },
        };
    }
    pub fn addPrune(_: *@This(), _: Predicate) anyerror!void {}
    pub fn stats(_: *@This()) exec.PipelineStats {
        return .{ .upper_rows = 0, .column_stats = &.{} };
    }
    pub fn accountant(_: *@This()) ?*exec.memory.MemoryAccountant {
        return null;
    }
    pub fn explain(_: *@This(), _: *std.ArrayList(u8), _: Allocator, _: usize) anyerror!void {}
};

test "a create that fails frees every buffer it built" {
    // A statement's accountant, and the statement-gate lease Database.close
    // waits on, live until every tracked byte is returned: one leaked
    // literal buffer hung close for good (#63).
    const x = [_]Expr{.{ .lit = .{ .text = "x" } }};
    const sqrt_x: Expr = .{ .call = .{ .fn_name = "sqrt", .args = &x } };
    const lit_then_bad = [_]Expr{ .{ .lit = .{ .text = "a" } }, sqrt_x };
    const cases = .{
        .{ Error.ComputeNoSuchOverload, &[_]Derived{.{ .name = "v", .expr = sqrt_x }} },
        .{ Error.ComputeNoSuchOverload, &[_]Derived{
            .{ .name = "u", .expr = .{ .call = .{ .fn_name = "upper", .args = &x } } },
            .{ .name = "v", .expr = sqrt_x },
        } },
        .{ Error.ComputeNoSuchOverload, &[_]Derived{.{ .name = "v", .expr = .{ .call = .{ .fn_name = "concat", .args = &lit_then_bad } } }} },
        .{ Error.ComputeNoSuchOverload, &[_]Derived{.{ .name = "v", .expr = .{ .case = .{
            .branches = &.{.{ .cond = .{ .is_not_null = "a" }, .then = .{ .lit = .{ .text = "y" } } }},
            .else_branch = &sqrt_x,
        } } }} },
        .{ Error.ComputeNameCollision, &[_]Derived{
            .{ .name = "k", .expr = .{ .lit = .{ .text = "p" } } },
            .{ .name = "k", .expr = .{ .lit = .{ .text = "q" } } },
        } },
    };
    inline for (cases) |c| {
        var src = EmptyTestSource{};
        try std.testing.expectError(c[0], Compute.create(std.testing.allocator, exec.makeQuery(std.testing.allocator, &src), c[1]));
    }
}

fn createAndRelease(allocator: Allocator) !void {
    const date_text = [_]Expr{.{ .lit = .{ .text = "2024-05-01" } }};
    const a = [_]Expr{.{ .col_ref = "a" }};
    const s_x = [_]Expr{ .{ .col_ref = "s" }, .{ .lit = .{ .text = "x" } } };
    const lit_s = [_]Expr{ .{ .lit = .{ .text = "a" } }, .{ .col_ref = "s" } };
    const concat_lit_s = [_]Expr{.{ .call = .{ .fn_name = "concat", .args = &lit_s } }};
    const zero: Expr = .{ .lit = .{ .int = 0 } };
    const string_null: Expr = .{ .null_lit = .string };
    const s_a = [_]Expr{ .{ .col_ref = "s" }, .{ .col_ref = "a" } };
    const s_col: Expr = .{ .col_ref = "s" };
    const derived = [_]Derived{
        .{ .name = "month_end", .expr = .{ .call = .{ .fn_name = "last_day", .args = &date_text } } },
        .{ .name = "root", .expr = .{ .call = .{ .fn_name = "sqrt", .args = &a } } },
        .{ .name = "suffixed", .expr = .{ .call = .{ .fn_name = "concat", .args = &s_x } } },
        .{ .name = "shout", .expr = .{ .call = .{ .fn_name = "upper", .args = &concat_lit_s } } },
        .{ .name = "label", .expr = .{ .lit = .{ .text = "k" } } },
        .{ .name = "widened", .expr = .{ .case = .{
            .branches = &.{
                .{ .cond = .{ .is_not_null = "s" }, .then = .{ .lit = .{ .double = 1.5 } } },
                .{ .cond = .{ .is_not_null = "a" }, .then = .{ .col_ref = "a" } },
            },
            .else_branch = &zero,
        } } },
        .{ .name = "or_null", .expr = .{ .case = .{
            .branches = &.{.{ .cond = .{ .is_not_null = "a" }, .then = .{ .col_ref = "a" } }},
            .else_branch = &string_null,
        } } },
        .{ .name = "tagged", .expr = .{ .call = .{ .fn_name = "concat", .args = &s_a } } },
        .{ .name = "top", .expr = .{ .call = .{ .fn_name = "greatest", .args = &s_a } } },
        .{ .name = "as_text", .expr = .{ .case = .{
            .branches = &.{.{ .cond = .{ .is_not_null = "a" }, .then = .{ .col_ref = "a" } }},
            .else_branch = &s_col,
        } } },
    };
    var src = EmptyTestSource{};
    var q = try Compute.create(allocator, exec.makeQuery(allocator, &src), &derived);
    q.deinit();
}

test "create frees what it built when any allocation fails" {
    // Covers literal slots re-typed after they were built (a text literal
    // read as a date, CASE literals and NULLs unified to the result type):
    // a failed replacement must leave the old buffer freeable exactly once.
    try std.testing.checkAllAllocationFailures(std.testing.allocator, createAndRelease, .{});
}
